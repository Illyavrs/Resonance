"""Offline checks for stored difficulty CAS; never import the live HTTP runner."""
import ast
from pathlib import Path
import unittest


SOURCE = Path(__file__).with_name("protocol_smoke.py")
PARSED = ast.parse(SOURCE.read_text(encoding="utf-8"), filename=str(SOURCE))
HELPERS = {"map_status_state", "review_map_status"}
SELECTED = [item for item in PARSED.body if isinstance(item, ast.FunctionDef) and item.name in HELPERS]
assert {item.name for item in SELECTED} == HELPERS
NAMESPACE = {}
exec(compile(ast.Module(body=SELECTED, type_ignores=[]), str(SOURCE), "exec"), NAMESPACE)
review_map_status = NAMESPACE["review_map_status"]


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


def current(status="graveyard", map_revision=29, override_revision=17):
    return {"id": 7, "native_id": 2_000_000_000, "native_set_id": 2_000_000_001,
            "status": status, "status_revision": map_revision,
            "status_override_revision": override_revision}


def transition(initial, returned):
    return ScriptedClient([
        ("GET", "/api/v1/beatmaps/7", initial),
        ("PATCH", "/api/v1/admin/beatmaps/7", returned),
    ])


class DifficultyReviewRunnerTests(unittest.TestCase):
    def test_both_fresh_nonzero_revisions_bound_to_the_one_difficulty(self):
        returned = current("ranked", 30, 18)
        client = transition(current(), returned)
        self.assertEqual(review_map_status(client, 7, "ranked", from_status="graveyard"), returned)
        self.assertEqual(client.calls[1][2], {
            "status": "ranked", "expected_revision": 17, "expected_status_revision": 29,
        })
        self.assertEqual(len(client.calls), 2)

    def test_explicit_zero_revisions_are_accepted_without_inventing_defaults(self):
        client = transition(current(map_revision=0, override_revision=0), current("ranked", 1, 1))
        review_map_status(client, 7, "ranked", from_status="graveyard")
        self.assertEqual(client.calls[1][2]["expected_revision"], 0)
        self.assertEqual(client.calls[1][2]["expected_status_revision"], 0)

    def test_missing_invalid_nullable_or_boolean_revision_never_mutates(self):
        for field in ("status_revision", "status_override_revision"):
            samples = [dict(current(), **{field: value}) for value in (None, True, False, -1, 17.0, "17")]
            missing = current()
            del missing[field]
            samples.append(missing)
            for initial in samples:
                with self.subTest(field=field, initial=initial):
                    client = ScriptedClient([("GET", "/api/v1/beatmaps/7", initial)])
                    with self.assertRaises((AssertionError, KeyError)):
                        review_map_status(client, 7, "ranked", from_status="graveyard")
                    self.assertEqual(len(client.calls), 1)

    def test_status_drift_noop_and_unknown_target_never_silently_skip_or_mutate(self):
        for observed, desired, prior in [
            ("wip", "ranked", "graveyard"),
            ("ranked", "ranked", "ranked"),
            ("graveyard", "invented", "graveyard"),
        ]:
            with self.subTest(observed=observed, desired=desired):
                client = ScriptedClient([("GET", "/api/v1/beatmaps/7", current(observed))])
                with self.assertRaises(AssertionError):
                    review_map_status(client, 7, desired, from_status=prior)
                self.assertEqual(len(client.calls), 1)

    def test_conflict_propagates_without_refetch_or_retry(self):
        conflict = RuntimeError("fixture HTTP 409 beatmap_status_conflict")
        client = transition(current(), conflict)
        with self.assertRaises(RuntimeError) as result:
            review_map_status(client, 7, "ranked", from_status="graveyard")
        self.assertIs(result.exception, conflict)
        self.assertEqual(len(client.calls), 2)
        self.assertEqual(client.responses, [])

    def test_wrong_resource_or_nonmatching_revision_response_is_fatal(self):
        variants = []
        for field in ("id", "native_id", "native_set_id", "status_revision", "status_override_revision"):
            malformed = current("ranked", 30, 18)
            malformed[field] += 1
            variants.append(malformed)
        variants.append(current("graveyard", 30, 18))
        for returned in variants:
            with self.subTest(returned=returned):
                client = transition(current(), returned)
                with self.assertRaises(AssertionError):
                    review_map_status(client, 7, "ranked", from_status="graveyard")
                self.assertEqual(len(client.calls), 2)

    def test_missing_response_revision_cannot_be_filled_from_the_request(self):
        returned = current("ranked", 30, 18)
        del returned["status_override_revision"]
        client = transition(current(), returned)
        with self.assertRaises(KeyError):
            review_map_status(client, 7, "ranked", from_status="graveyard")
        self.assertEqual(len(client.calls), 2)

    def test_every_legacy_status_patch_uses_the_dual_revision_helper(self):
        patches, reviews = [], []
        for node in ast.walk(PARSED):
            if not isinstance(node, ast.Call):
                continue
            if isinstance(node.func, ast.Name) and node.func.id == "review_map_status":
                reviews.append(node)
                keywords = {item.arg: item.value for item in node.keywords}
                self.assertEqual(keywords["from_status"].value, "graveyard")
            if (isinstance(node.func, ast.Attribute) and node.func.attr == "json"
                    and len(node.args) >= 3 and isinstance(node.args[0], ast.Constant)
                    and node.args[0].value == "PATCH" and isinstance(node.args[1], ast.JoinedStr)
                    and any(isinstance(part, ast.Constant) and "/api/v1/admin/beatmaps/" in part.value
                            for part in node.args[1].values)):
                patches.append(node)
                self.assertIsInstance(node.args[2], ast.Dict)
                keys = {key.value for key in node.args[2].keys if isinstance(key, ast.Constant)}
                self.assertEqual(keys, {"status", "expected_revision", "expected_status_revision"})
        self.assertEqual(len(patches), 1, "only the fresh dual-revision helper sends legacy map PATCH")
        self.assertEqual(len(reviews), 2, "both intentional fixture ranking transitions remain explicit")


if __name__ == "__main__":
    unittest.main()
