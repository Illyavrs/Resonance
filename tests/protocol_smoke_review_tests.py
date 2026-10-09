"""Offline CAS runner checks; extract helpers without importing the HTTP runner."""
import ast
from pathlib import Path
import unittest


SOURCE = Path(__file__).with_name("protocol_smoke.py")
PARSED = ast.parse(SOURCE.read_text(encoding="utf-8"), filename=str(SOURCE))
HELPERS = {"score_state", "score_review_state", "review_score", "reject_score_review"}
SELECTED = [item for item in PARSED.body if isinstance(item, ast.FunctionDef) and item.name in HELPERS]
assert {item.name for item in SELECTED} == HELPERS
NAMESPACE = {}
exec(compile(ast.Module(body=SELECTED, type_ignores=[]), str(SOURCE), "exec"), NAMESPACE)
review_score = NAMESPACE["review_score"]
reject_score_review = NAMESPACE["reject_score_review"]


class ScriptedClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def json(self, method, path, body=None, **options):
        self.calls.append((method, path, body, options))
        assert self.responses, "unexpected request or CAS retry"
        expected_method, expected_path, value = self.responses.pop(0)
        assert (method, path) == (expected_method, expected_path)
        if isinstance(value, Exception):
            raise value
        return value


def current(status="pending", revision=27):
    return {"id": 7, "verification": status, "moderation_revision": revision}


class ScoreReviewRunnerTests(unittest.TestCase):
    def test_one_transition_uses_actual_nonzero_revision(self):
        response = dict(current("verified", 28), ok=True)
        client = ScriptedClient([
            ("GET", "/api/v1/scores/7", current()),
            ("PATCH", "/api/v1/admin/scores/7", response),
        ])
        self.assertEqual(review_score(client, 7, "verified", from_status="pending"), response)
        self.assertEqual(client.calls[1][2], {"verification": "verified", "expected_revision": 27})
        self.assertEqual(len(client.calls), 2)

    def test_missing_or_invalid_revision_never_sends_mutation(self):
        samples = [current(revision=value) for value in (None, True, -1, 27.0, "27")]
        samples.append({"id": 7, "verification": "pending"})
        for sample in samples:
            with self.subTest(sample=sample):
                client = ScriptedClient([("GET", "/api/v1/scores/7", sample)])
                with self.assertRaises((AssertionError, KeyError)):
                    review_score(client, 7, "verified", from_status="pending")
                self.assertEqual(len(client.calls), 1)

    def test_fixture_state_drift_and_noop_never_silently_skip_or_mutate(self):
        for observed, target, expected_from in [
            ("verified", "verified", "pending"),
            ("pending", "pending", "pending"),
        ]:
            with self.subTest(observed=observed, target=target):
                client = ScriptedClient([("GET", "/api/v1/scores/7", current(observed))])
                with self.assertRaises(AssertionError):
                    review_score(client, 7, target, from_status=expected_from)
                self.assertEqual(len(client.calls), 1)

    def test_conflict_propagates_without_refreshing_or_retrying_cas(self):
        conflict = RuntimeError("fixture HTTP 409")
        client = ScriptedClient([
            ("GET", "/api/v1/scores/7", current()),
            ("PATCH", "/api/v1/admin/scores/7", conflict),
        ])
        with self.assertRaises(RuntimeError) as result:
            review_score(client, 7, "verified", from_status="pending")
        self.assertIs(result.exception, conflict)
        self.assertEqual(len(client.calls), 2)

    def test_returned_revision_drift_fails_without_another_mutation(self):
        client = ScriptedClient([
            ("GET", "/api/v1/scores/7", current()),
            ("PATCH", "/api/v1/admin/scores/7", dict(current("verified", 27), ok=True)),
        ])
        with self.assertRaises(AssertionError):
            review_score(client, 7, "verified", from_status="pending")
        self.assertEqual(len(client.calls), 2)

    def test_stale_probe_sends_captured_revision_and_checks_state_and_audit(self):
        audit = {"total": 12, "items": [{"id": 12, "action": "scores.verify"}]}
        client = ScriptedClient([
            ("GET", "/api/v1/scores/7", current(revision=29)),
            ("GET", "/api/v1/admin/audit", audit),
            ("PATCH", "/api/v1/admin/scores/7", {"error": {"code": "score_moderation_conflict"}}),
            ("GET", "/api/v1/scores/7", current(revision=29)),
            ("GET", "/api/v1/admin/audit", audit),
        ])
        reject_score_review(client, 7, "verified", 27, status=409, code="score_moderation_conflict")
        self.assertEqual(client.calls[2][2]["expected_revision"], 27)
        self.assertEqual(client.calls[2][3], {"expected": 409})
        self.assertEqual(len(client.calls), 5)

    def test_failed_decision_cannot_hide_an_audit_mutation(self):
        client = ScriptedClient([
            ("GET", "/api/v1/scores/7", current()),
            ("GET", "/api/v1/admin/audit", {"total": 12, "items": []}),
            ("PATCH", "/api/v1/admin/scores/7", {"error": {"code": "score_verification_unchanged"}}),
            ("GET", "/api/v1/scores/7", current()),
            ("GET", "/api/v1/admin/audit", {"total": 13, "items": []}),
        ])
        with self.assertRaises(AssertionError):
            reject_score_review(client, 7, "pending", 27, status=400, code="score_verification_unchanged")

    def test_all_score_patch_bodies_include_revision_and_explicit_calls_name_source_status(self):
        score_patches = []
        reviews = []
        for node in ast.walk(PARSED):
            if not isinstance(node, ast.Call):
                continue
            if isinstance(node.func, ast.Name) and node.func.id == "review_score":
                reviews.append(node)
                self.assertTrue(any(keyword.arg == "from_status" for keyword in node.keywords))
            if (isinstance(node.func, ast.Attribute) and node.func.attr == "json"
                    and len(node.args) >= 3 and isinstance(node.args[0], ast.Constant)
                    and node.args[0].value == "PATCH" and isinstance(node.args[1], ast.JoinedStr)
                    and any(isinstance(part, ast.Constant) and "/api/v1/admin/scores/" in part.value
                            for part in node.args[1].values)):
                score_patches.append(node)
                self.assertIsInstance(node.args[2], ast.Dict)
                keys = {key.value for key in node.args[2].keys if isinstance(key, ast.Constant)}
                self.assertIn("expected_revision", keys)
        self.assertEqual(len(score_patches), 2, "only the transition and intentional rejection helpers send score PATCH")
        self.assertEqual(len(reviews), 10, "every intended score review remains an explicit call site")


if __name__ == "__main__":
    unittest.main()
