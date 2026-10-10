//! Original fixtures for the explicit native server catalogue adapter.
use super::search_tests::{Backend, Fixture, KEY};
use super::*;
use axum::extract::State;
use futures_util::FutureExt;
use std::time::Duration;

fn result() -> Value {
    json!({
        "beatmapsets":[{
            "id":820001,
            "title":"搜尋 Server Fixture",
            "status":"pending",
            "ranked":0,
            "metadata_only":true,
            "download_disabled":true,
            "availability":{"download_disabled":true},
            "beatmaps":[
                {"id":720001,"beatmapset_id":820001,"version":"Hard","mode_int":0,"difficulty_rating":4.2,"status":"ranked","ranked":1},
                {"id":720002,"beatmapset_id":820001,"version":"Easy","mode_int":0,"difficulty_rating":1.2,"status":"graveyard","ranked":-2}
            ]
        }],
        "total":51,
        "catalogue_revision":0,
        "cursor":{"illya":"opaque-server-cursor+/=&%"},
        "cursor_string":"opaque-server-cursor+/=&%"
    })
}

fn routed(raw: &str) -> (SearchSource, HashMap<String, String>) {
    let (source, encoded) = search_route(search_query(Some(raw.into())).unwrap()).unwrap();
    (source, serde_urlencoded::from_str(&encoded).unwrap())
}

#[test]
fn native_server_routing_is_explicit_and_official_selection_escapes_prefix() {
    for raw in [
        "",
        "q=ordinary",
        "q=Server%3Aordinary",
        "q=server",
        "q=x+server%3Aordinary",
    ] {
        assert_eq!(routed(raw).0, SearchSource::Official, "{raw}");
    }
    for raw in [
        "q=server%3Aordinary",
        "q=++server%3A+ordinary++",
        "source=local&q=ordinary",
        "source=local&q=server%3Aordinary",
    ] {
        let (source, query) = routed(raw);
        assert_eq!(source, SearchSource::Server, "{raw}");
        assert_eq!(query["q"], "ordinary", "{raw}");
        assert_eq!(
            query.get("source").map(String::as_str),
            raw.starts_with("source=local").then_some("local")
        );
    }
    assert_eq!(routed("q=server%3A").1["q"], "");
    let (source, query) = routed("source=official&q=++server%3A+ordinary++");
    assert_eq!(source, SearchSource::Official);
    assert_eq!(query["q"], "  server: ordinary  ");
    assert!(!query.contains_key("source"));
}

#[test]
fn native_server_query_encoding_preserves_unicode_filters_and_opaque_cursors() {
    let raw = "q=server%3A+%E6%98%9F+%26+100%25&m=3&s=ranked&sort=ranked_desc&nsfw=false&g=0&l=0&c=&e=&cursor%5Billya%5D=opaque+server+cursor+%E6%98%9F%2B%2F%3D%26%25";
    let (source, query) = routed(raw);
    assert_eq!(source, SearchSource::Server);
    assert_eq!(query["q"], "星 & 100%");
    assert_eq!(query["cursor[illya]"], "opaque server cursor 星+/=&%");
    for (key, expected) in [
        ("m", "3"),
        ("s", "ranked"),
        ("sort", "ranked_desc"),
        ("nsfw", "false"),
        ("g", "0"),
        ("l", "0"),
        ("c", ""),
        ("e", ""),
    ] {
        assert_eq!(query[key], expected);
    }
    let raw = serde_urlencoded::to_string([
        ("source", "local"),
        ("cursor_string", "arbitrary future token 雨+/%=&"),
    ])
    .unwrap();
    assert_eq!(
        routed(&raw).1["cursor_string"],
        "arbitrary future token 雨+/%=&"
    );
}

#[test]
fn native_server_validation_accepts_canonical_metadata_and_local_wire_ids() {
    assert!(validate_server_search(&result()).is_ok());
    let mut local = result();
    local["beatmapsets"][0]["id"] = json!(i32::MAX);
    local["beatmapsets"][0]["metadata_only"] = json!(false);
    local["beatmapsets"][0]["download_disabled"] = json!(false);
    local["beatmapsets"][0]["availability"]["download_disabled"] = json!(false);
    for (index, map) in local["beatmapsets"][0]["beatmaps"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .enumerate()
    {
        map["id"] = json!(2_000_000_000 + index as i64);
        map["beatmapset_id"] = json!(i32::MAX);
    }
    assert!(validate_server_search(&local).is_ok());
    assert!(validate_official_search(&local).is_err());
    for (status, rank) in [
        ("graveyard", -2),
        ("wip", -1),
        ("pending", 0),
        ("ranked", 1),
        ("approved", 2),
        ("qualified", 3),
        ("loved", 4),
    ] {
        let mut page = result();
        page["beatmapsets"][0]["status"] = json!(status);
        page["beatmapsets"][0]["ranked"] = json!(rank);
        page["beatmapsets"][0]["beatmaps"][0]["status"] = json!(status);
        page["beatmapsets"][0]["beatmaps"][0]["ranked"] = json!(rank);
        assert!(validate_server_search(&page).is_ok(), "{status}");
    }
    assert!(validate_server_search(&json!({"beatmapsets":[],"total":0,"catalogue_revision":0,"cursor":null,"cursor_string":null})).is_ok());
}

#[test]
fn native_server_validation_rejects_inconsistent_cursor_status_and_availability() {
    for (path, bad) in [
        ("/beatmapsets", json!({})),
        ("/total", json!(0)),
        ("/total", json!(-1)),
        ("/total", json!(2147483648i64)),
        ("/catalogue_revision", json!(-1)),
        ("/catalogue_revision", json!("0")),
        ("/cursor", json!(null)),
        ("/cursor", json!({"illya":"wrong"})),
        (
            "/cursor",
            json!({"illya":"opaque-server-cursor+/=&%","id":42}),
        ),
        ("/cursor", json!({"id":42})),
        ("/cursor", json!({"illya":42})),
        ("/cursor_string", json!(null)),
        ("/cursor_string", json!(42)),
        ("/beatmapsets/0/id", json!(0)),
        ("/beatmapsets/0/id", json!(2147483648i64)),
        ("/beatmapsets/0/metadata_only", json!(null)),
        ("/beatmapsets/0/download_disabled", json!(false)),
        ("/beatmapsets/0/availability/download_disabled", json!(null)),
        ("/beatmapsets/0/status", json!("unranked")),
        ("/beatmapsets/0/ranked", json!(1)),
        ("/beatmapsets/0/beatmaps", json!([])),
        ("/beatmapsets/0/beatmaps/0/id", json!(0)),
        ("/beatmapsets/0/beatmaps/0/id", json!(2147483648i64)),
        ("/beatmapsets/0/beatmaps/0/beatmapset_id", json!(820002)),
        ("/beatmapsets/0/beatmaps/0/status", json!("unknown")),
        ("/beatmapsets/0/beatmaps/0/status", json!("unranked")),
        ("/beatmapsets/0/beatmaps/0/ranked", json!(0)),
    ] {
        let mut page = result();
        *page.pointer_mut(path).unwrap() = bad;
        assert!(validate_server_search(&page).is_err(), "{path}: {page}");
    }
    let mut page = result();
    page.as_object_mut().unwrap().remove("cursor_string");
    assert!(validate_server_search(&page).is_err());
    for token in [String::new(), "x".repeat(2049), "星".into()] {
        let mut page = result();
        page["cursor"] = json!({"illya":token});
        page["cursor_string"] = json!(token);
        assert!(validate_server_search(&page).is_err());
    }
    let mut page = result();
    page["beatmapsets"][0]["download_disabled"] = json!(false);
    page["beatmapsets"][0]["availability"]["download_disabled"] = json!(false);
    assert!(validate_server_search(&page).is_err());
}

#[test]
fn native_server_validation_bounds_membership_and_rejects_duplicate_identities() {
    let mut page = result();
    let duplicate = page["beatmapsets"][0].clone();
    page["beatmapsets"].as_array_mut().unwrap().push(duplicate);
    assert!(validate_server_search(&page).is_err());
    let mut page = result();
    page["beatmapsets"][0]["beatmaps"][1]["id"] = json!(720001);
    assert!(validate_server_search(&page).is_err());
    let mut page = result();
    let mut duplicate_map_set = page["beatmapsets"][0].clone();
    duplicate_map_set["id"] = json!(820002);
    for map in duplicate_map_set["beatmaps"].as_array_mut().unwrap() {
        map["beatmapset_id"] = json!(820002);
    }
    page["beatmapsets"]
        .as_array_mut()
        .unwrap()
        .push(duplicate_map_set);
    assert!(validate_server_search(&page).is_err());
    let mut page = result();
    let original = page["beatmapsets"][0].clone();
    page["beatmapsets"] = json!(
        (0..51)
            .map(|index| {
                let mut set = original.clone();
                set["id"] = json!(820001 + index);
                let set_id = set["id"].clone();
                for (map_index, map) in set["beatmaps"]
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .enumerate()
                {
                    map["id"] = json!(720001 + index * 2 + map_index as i32);
                    map["beatmapset_id"] = set_id.clone();
                }
                set
            })
            .collect::<Vec<_>>()
    );
    assert!(validate_server_search(&page).is_err());
    let mut page = result();
    let original = page["beatmapsets"][0]["beatmaps"][0].clone();
    page["beatmapsets"][0]["beatmaps"] = json!(
        (0..513)
            .map(|index| {
                let mut map = original.clone();
                map["id"] = json!(720001 + index);
                map
            })
            .collect::<Vec<_>>()
    );
    assert!(validate_server_search(&page).is_err());
}

pub(super) async fn catalogue_backend(
    State(state): State<Backend>,
    headers: HeaderMap,
    Json(input): Json<Value>,
) -> Response {
    assert_eq!(headers["x-illya-service-key"], KEY);
    assert!(headers.get("authorization").is_none());
    assert_eq!(input.as_object().unwrap().len(), 2);
    assert!(input["user_id"].as_i64().unwrap() > 0);
    state.server_calls.lock().await.push(input);
    if state.pause.load(std::sync::atomic::Ordering::SeqCst) {
        state.entered.notify_one();
        tokio::time::timeout(Duration::from_secs(3), state.release.notified())
            .await
            .unwrap();
    }
    let (status, bytes) = state.server_reply.lock().await.clone();
    (status, bytes).into_response()
}

pub(super) async fn with_revision(f: &Fixture, mut page: Value) -> Value {
    page["catalogue_revision"] = json!(
        sqlx::query_scalar::<_, i64>("SELECT epoch FROM server_catalogue_revision WHERE singleton")
            .fetch_one(&f.state.pool)
            .await
            .unwrap()
    );
    page
}

#[tokio::test]
#[ignore = "requires an owned disposable PostgreSQL 17 database"]
async fn native_server_search_routes_unicode_metadata_and_cursor_without_imports() {
    let f = Fixture::new().await;
    let check = std::panic::AssertUnwindSafe(async {
        *f.backend.server_reply.lock().await = (StatusCode::OK, result().to_string().into_bytes());
        // The API preserves the complete catalogue payload while replacing
        // public media with resources on this deployment's own origin.
        let configured = std::env::var("PUBLIC_ORIGIN")
            .unwrap_or_else(|_| "http://localhost:3000".into());
        let origin = reqwest::Url::parse(&configured).unwrap().origin().ascii_serialization();
        let mut expected = result();
        expected["beatmapsets"][0]["covers"] = json!({
            "cover":format!("{origin}/api/v2/beatmapsets/820001/media/cover"),
            "cover@2x":format!("{origin}/api/v2/beatmapsets/820001/media/cover@2x"),
            "card":format!("{origin}/api/v2/beatmapsets/820001/media/card"),
            "card@2x":format!("{origin}/api/v2/beatmapsets/820001/media/card@2x"),
            "list":format!("{origin}/api/v2/beatmapsets/820001/media/list"),
            "list@2x":format!("{origin}/api/v2/beatmapsets/820001/media/list@2x"),
            "slimcover":format!("{origin}/api/v2/beatmapsets/820001/media/slimcover"),
            "slimcover@2x":format!("{origin}/api/v2/beatmapsets/820001/media/slimcover@2x")
        });
        expected["beatmapsets"][0]["preview_url"] =
            json!(format!("{origin}/api/v2/beatmapsets/820001/preview"));
        assert_eq!(f.get("q=server%3A", false).await.0, StatusCode::UNAUTHORIZED);
        assert!(f.backend.server_calls.lock().await.is_empty());
        let query = "q=++server%3A+%E6%98%9F+%26+100%25++&m=3&s=ranked&sort=ranked_desc&nsfw=false&g=0&l=0&c=&e=";
        assert_eq!(f.get(query, true).await, (StatusCode::OK, expected.clone()));
        let calls = f.backend.server_calls.lock().await;
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["user_id"], f.user);
        let forwarded: HashMap<String, String> = serde_urlencoded::from_str(calls[0]["query"].as_str().unwrap()).unwrap();
        assert_eq!(forwarded["q"], "星 & 100%");
        assert_eq!(forwarded["sort"], "ranked_desc");
        assert!(!forwarded.contains_key("source"));
        drop(calls);
        for cursor_key in ["cursor_string", "cursor[illya]"] {
            let query = serde_urlencoded::to_string([
                ("source", "local"),
                ("q", "server: 搜尋"),
                (cursor_key, result()["cursor_string"].as_str().unwrap()),
            ]).unwrap();
            assert_eq!(f.get(&query, true).await, (StatusCode::OK, expected.clone()));
            let calls = f.backend.server_calls.lock().await;
            let forwarded: HashMap<String, String> = serde_urlencoded::from_str(calls.last().unwrap()["query"].as_str().unwrap()).unwrap();
            assert_eq!(forwarded["q"], "搜尋");
            assert_eq!(forwarded["source"], "local");
            assert_eq!(forwarded[cursor_key], result()["cursor_string"].as_str().unwrap());
        }
        assert_eq!(f.get("source=official&q=server%3A%E6%98%9F", true).await.0, StatusCode::OK);
        assert_eq!(f.get("q=ordinary", true).await.0, StatusCode::OK);
        let calls = f.backend.calls.lock().await;
        assert_eq!(calls.len(), 2);
        let forwarded: HashMap<String, String> = serde_urlencoded::from_str(calls[0]["query"].as_str().unwrap()).unwrap();
        assert_eq!(forwarded["q"], "server:星");
        assert!(!forwarded.contains_key("source"));
        drop(calls);
        assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM beatmaps").fetch_one(&f.state.pool).await.unwrap(), 0);
        assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM audit_events").fetch_one(&f.state.pool).await.unwrap(), 0);
    }).catch_unwind().await;
    f.cleanup().await;
    if let Err(error) = check {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires an owned disposable PostgreSQL 17 database"]
async fn native_server_search_errors_and_invalid_payloads_never_use_official_fallback() {
    let f = Fixture::new().await;
    let check = std::panic::AssertUnwindSafe(async {
        for (backend, expected) in [
            (StatusCode::BAD_REQUEST, StatusCode::BAD_REQUEST),
            (StatusCode::UNAUTHORIZED, StatusCode::UNAUTHORIZED),
            (StatusCode::CONFLICT, StatusCode::CONFLICT),
            (StatusCode::TOO_MANY_REQUESTS, StatusCode::TOO_MANY_REQUESTS),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (StatusCode::INTERNAL_SERVER_ERROR, StatusCode::BAD_GATEWAY),
        ] {
            *f.backend.server_reply.lock().await =
                (backend, b"private synthetic catalogue failure".to_vec());
            let response = f.get("q=server%3AUnknown", true).await;
            assert_eq!(response.0, expected);
            assert!(response.1.get("beatmapsets").is_none());
            assert!(!response.1.to_string().contains("synthetic catalogue"));
        }
        *f.backend.server_reply.lock().await = (StatusCode::OK, vec![b'x'; 4 * 1024 * 1024 + 1]);
        assert_eq!(f.get("source=local", true).await.0, StatusCode::BAD_GATEWAY);
        let mut invalid = result();
        invalid["cursor"]["illya"] = json!("mismatched token");
        *f.backend.server_reply.lock().await = (StatusCode::OK, invalid.to_string().into_bytes());
        assert_eq!(f.get("source=local", true).await.0, StatusCode::BAD_GATEWAY);
        assert!(f.backend.calls.lock().await.is_empty());
        assert_eq!(f.backend.server_calls.lock().await.len(), 8);
    })
    .catch_unwind()
    .await;
    f.cleanup().await;
    if let Err(error) = check {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires an owned disposable PostgreSQL 17 database"]
async fn native_server_search_rechecks_bearer_after_success_and_backend_failure() {
    let f = Fixture::new().await;
    let check = std::panic::AssertUnwindSafe(async {
        for backend in [StatusCode::OK, StatusCode::TOO_MANY_REQUESTS] {
            sqlx::query("UPDATE lazer_sessions SET access_expires_at=now()+interval '1 hour'")
                .execute(&f.state.pool)
                .await
                .unwrap();
            *f.backend.server_reply.lock().await = (backend, result().to_string().into_bytes());
            f.backend
                .pause
                .store(true, std::sync::atomic::Ordering::SeqCst);
            let get = f.get("q=server%3A", true);
            let revoke = async {
                tokio::time::timeout(Duration::from_secs(2), f.backend.entered.notified())
                    .await
                    .unwrap();
                sqlx::query(
                    "UPDATE lazer_sessions SET access_expires_at=now()-interval '1 second'",
                )
                .execute(&f.state.pool)
                .await
                .unwrap();
                f.backend.release.notify_one();
            };
            let (response, ()) = tokio::join!(get, revoke);
            assert_eq!(response.0, StatusCode::UNAUTHORIZED);
            assert!(response.1.get("beatmapsets").is_none());
        }
        assert!(f.backend.calls.lock().await.is_empty());
    })
    .catch_unwind()
    .await;
    f.cleanup().await;
    if let Err(error) = check {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires an owned disposable PostgreSQL 17 database"]
async fn native_server_search_rejects_page_withdrawn_during_backend_wait() {
    let f = Fixture::new().await;
    let check = std::panic::AssertUnwindSafe(async {
        *f.backend.server_reply.lock().await = (StatusCode::OK, result().to_string().into_bytes());
        f.backend.pause.store(true, std::sync::atomic::Ordering::SeqCst);
        let get = f.get("q=server%3A", true);
        let withdraw = async {
            tokio::time::timeout(Duration::from_secs(2), f.backend.entered.notified()).await.unwrap();
            sqlx::query("INSERT INTO beatmapset_availability(native_set_id,withdrawn,revision,reason,actor_id) VALUES(820001,true,1,'Original server catalogue fixture',$1)")
                .bind(f.user).execute(&f.state.pool).await.unwrap();
            f.backend.release.notify_one();
        };
        let (response, ()) = tokio::join!(get, withdraw);
        assert_eq!(response.0, StatusCode::CONFLICT);
        assert_eq!(response.1["error"], "server_catalogue_changed");
        assert!(response.1.get("beatmapsets").is_none());
        f.backend.pause.store(false, std::sync::atomic::Ordering::SeqCst);
        sqlx::query("UPDATE beatmapset_availability SET withdrawn=false,revision=revision+1 WHERE native_set_id=820001").execute(&f.state.pool).await.unwrap();
        let page = with_revision(&f, result()).await;
        *f.backend.server_reply.lock().await = (StatusCode::OK, page.to_string().into_bytes());
        assert_eq!(f.get("source=local", true).await, (StatusCode::OK, page));
    }).catch_unwind().await;
    f.cleanup().await;
    if let Err(error) = check {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires an owned disposable PostgreSQL 17 database"]
async fn native_server_search_rejects_difficulty_demoted_during_backend_wait() {
    let f = Fixture::new().await;
    let check = std::panic::AssertUnwindSafe(async {
        sqlx::query("INSERT INTO beatmap_status_overrides(native_id,native_set_id,status,actor_id,reason) VALUES(720001,820001,'ranked',$1,'Original server status fixture')")
            .bind(f.user).execute(&f.state.pool).await.unwrap();
        let page = with_revision(&f, result()).await;
        *f.backend.server_reply.lock().await = (StatusCode::OK, page.to_string().into_bytes());
        assert_eq!(f.get("q=server%3A&s=ranked", true).await.0, StatusCode::OK);
        f.backend.pause.store(true, std::sync::atomic::Ordering::SeqCst);
        let get = f.get("q=server%3A&s=ranked", true);
        let demote = async {
            tokio::time::timeout(Duration::from_secs(2), f.backend.entered.notified()).await.unwrap();
            sqlx::query("UPDATE beatmap_status_overrides SET status='unranked' WHERE native_id=720001").execute(&f.state.pool).await.unwrap();
            f.backend.release.notify_one();
        };
        let (response, ()) = tokio::join!(get, demote);
        assert_eq!(response.0, StatusCode::CONFLICT);
        assert_eq!(response.1["error"], "server_catalogue_changed");
        f.backend.pause.store(false, std::sync::atomic::Ordering::SeqCst);
        let stale_status = with_revision(&f, result()).await;
        *f.backend.server_reply.lock().await = (StatusCode::OK, stale_status.to_string().into_bytes());
        assert_eq!(f.get("source=local", true).await.0, StatusCode::CONFLICT);
        let mut page = result();
        page["beatmapsets"][0]["beatmaps"][0]["status"] = json!("pending");
        page["beatmapsets"][0]["beatmaps"][0]["ranked"] = json!(0);
        let page = with_revision(&f, page).await;
        *f.backend.server_reply.lock().await = (StatusCode::OK, page.to_string().into_bytes());
        assert_eq!(f.get("source=local", true).await, (StatusCode::OK, page));
    }).catch_unwind().await;
    f.cleanup().await;
    if let Err(error) = check {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires an owned disposable PostgreSQL 17 database"]
async fn native_server_search_rejects_empty_page_if_membership_changes_during_wait() {
    let f = Fixture::new().await;
    let check = std::panic::AssertUnwindSafe(async {
        f.backend.pause.store(true, std::sync::atomic::Ordering::SeqCst);
        let get = f.get("source=local&s=ranked", true);
        let add_member = async {
            tokio::time::timeout(Duration::from_secs(2), f.backend.entered.notified()).await.unwrap();
            sqlx::query("INSERT INTO beatmap_status_overrides(native_id,native_set_id,status,actor_id,reason) VALUES(720001,820001,'ranked',$1,'Original empty page membership fixture')")
                .bind(f.user).execute(&f.state.pool).await.unwrap();
            f.backend.release.notify_one();
        };
        let (response, ()) = tokio::join!(get, add_member);
        assert_eq!(response.0, StatusCode::CONFLICT);
        assert_eq!(response.1["error"], "server_catalogue_changed");
        assert!(response.1.get("beatmapsets").is_none());
        f.backend.pause.store(false, std::sync::atomic::Ordering::SeqCst);
        let page = with_revision(&f, result()).await;
        *f.backend.server_reply.lock().await = (StatusCode::OK, page.to_string().into_bytes());
        assert_eq!(f.get("source=local&s=ranked", true).await, (StatusCode::OK, page));
    }).catch_unwind().await;
    f.cleanup().await;
    if let Err(error) = check {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires an owned disposable PostgreSQL 17 database"]
async fn native_server_search_preserves_stored_local_ids_and_rechecks_stored_status() {
    let f = Fixture::new().await;
    let check = std::panic::AssertUnwindSafe(async {
        let internal: i64 = sqlx::query_scalar("INSERT INTO beatmaps(title,artist,creator,version,mode,status,source,stars,md5,osu_path) VALUES('Original stored catalogue','Fixture','Fixture','Hard',0,'ranked','local',1,'12121212121212121212121212121212','fixture.osu') RETURNING id")
            .fetch_one(&f.state.pool).await.unwrap();
        let map = crate::native_maps::map_json(&f.state, internal).await.unwrap();
        let native_set_id = map["beatmapset"]["id"].as_i64().unwrap();
        let mut set = local_set_json(&f.state, native_set_id).await.unwrap();
        set["metadata_only"] = json!(false);
        let page = with_revision(&f, json!({"beatmapsets":[set],"total":1,"cursor":null,"cursor_string":null})).await;
        *f.backend.server_reply.lock().await = (StatusCode::OK, page.to_string().into_bytes());
        let response = f.get("source=local&s=ranked", true).await;
        assert_eq!(response, (StatusCode::OK, page));
        assert!(response.1["beatmapsets"][0]["id"].as_i64().unwrap() >= crate::native_maps::LOCAL_ID_MIN);
        assert_eq!(response.1["beatmapsets"][0]["beatmaps"][0]["id"], map["id"]);
        assert_eq!(response.1["beatmapsets"][0]["availability"]["download_disabled"], true);
        sqlx::query("UPDATE beatmaps SET status='pending' WHERE id=$1").bind(internal).execute(&f.state.pool).await.unwrap();
        assert_eq!(f.get("source=local&s=ranked", true).await.0, StatusCode::CONFLICT);
        assert!(f.backend.calls.lock().await.is_empty());
    }).catch_unwind().await;
    f.cleanup().await;
    if let Err(error) = check {
        std::panic::resume_unwind(error);
    }
}
