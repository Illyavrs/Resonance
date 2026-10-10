//! Real stable room admission preserves the checksum of the downloaded chart.
//! Every row and packet belongs to the disposable parent fixture.
use super::checksum_tests::{DERIVED_HASH, DERIVED_SIBLING_HASH, publish_room_distribution};
use super::*;
use resonance_protocol::{multiplayer as room, stable as wire};

async fn stable_account(f: &Fixture) -> common::Account {
    sqlx::query("INSERT INTO bancho_sessions(id,token_hash,user_id,expires_at) VALUES($1,$2,$3,now()+interval '1 hour')")
        .bind(uuid::Uuid::new_v4())
        .bind(common::token_hash(&common::secret()))
        .bind(f.users[0])
        .execute(&f.state.pool)
        .await
        .unwrap();
    common::account(&f.state, f.users[0]).await.unwrap()
}

fn selected_match(user: &common::Account, native_id: i64, checksum: &str, mode: u8) -> room::Match {
    let mut selected = room::Match {
        id: 0,
        in_progress: false,
        mods: 0,
        name: "Original stable checksum fixture".into(),
        password: String::new(),
        map_name: "Original fixture".into(),
        map_id: i32::try_from(native_id).unwrap(),
        map_md5: checksum.into(),
        statuses: [room::OPEN; room::SLOTS],
        teams: [0; room::SLOTS],
        users: [None; room::SLOTS],
        host_id: i32::try_from(user.id).unwrap(),
        mode,
        win_condition: 0,
        team_type: 0,
        freemods: false,
        slot_mods: [0; room::SLOTS],
        seed: 7,
    };
    selected.statuses[0] = room::NOT_READY;
    selected.users[0] = Some(selected.host_id);
    selected
}

async fn handle(
    f: &Fixture,
    user: &common::Account,
    packet_id: u16,
    payload: &[u8],
) -> Result<Vec<u8>, common::ApiError> {
    let mut tx = f.state.pool.begin().await.unwrap();
    let mut out = Vec::new();
    match crate::multiplayer::handle(&f.state, &mut tx, user, packet_id, payload, &mut out).await {
        Ok(handled) => {
            assert!(handled);
            tx.commit().await.unwrap();
            Ok(out)
        }
        Err(error) => {
            assert!(
                out.is_empty(),
                "rejected admission must not emit a success packet"
            );
            tx.rollback().await.unwrap();
            Err(error)
        }
    }
}

async fn stored_match(f: &Fixture, user: i64) -> room::Match {
    let state: Value = sqlx::query_scalar("SELECT r.state FROM stable_rooms r JOIN stable_room_members m ON m.room_id=r.id WHERE m.user_id=$1")
        .bind(user)
        .fetch_one(&f.state.pool)
        .await
        .unwrap();
    serde_json::from_value(state).unwrap()
}

async fn room_snapshot(f: &Fixture) -> Value {
    sqlx::query_scalar("SELECT jsonb_build_object('rooms',(SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY r.id),'[]'::jsonb) FROM stable_rooms r),'members',(SELECT COALESCE(jsonb_agg(to_jsonb(m) ORDER BY m.user_id),'[]'::jsonb) FROM stable_room_members m),'queue',(SELECT COALESCE(jsonb_agg(to_jsonb(q) ORDER BY q.id),'[]'::jsonb) FROM bancho_packet_queue q))")
        .fetch_one(&f.state.pool)
        .await
        .unwrap()
}

fn assert_match_packet(bytes: &[u8], packet_id: u16, checksum: &str, native_id: i64) {
    let packets = wire::decode_packets(bytes).unwrap();
    let packet = packets
        .iter()
        .find(|packet| packet.id == packet_id)
        .unwrap();
    let selected = room::Match::decode(packet.payload).unwrap();
    assert_eq!(selected.map_md5, checksum);
    assert_eq!(i64::from(selected.map_id), native_id);
}

async fn assert_start_packet(f: &Fixture, user: i64, checksum: &str, native_id: i64) {
    let queued: Vec<Vec<u8>> =
        sqlx::query_scalar("SELECT payload FROM bancho_packet_queue WHERE user_id=$1 ORDER BY id")
            .bind(user)
            .fetch_all(&f.state.pool)
            .await
            .unwrap();
    let start = queued
        .iter()
        .find(|bytes| {
            wire::decode_packets(bytes)
                .unwrap()
                .iter()
                .any(|packet| packet.id == 46)
        })
        .expect("the real stable start handler queues MATCH_START");
    assert_match_packet(start, 46, checksum, native_id);
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL pointing at disposable local PostgreSQL"]
async fn stable_source_room_keeps_historical_checksum_after_distribution_publication() {
    let f = Fixture::new().await;
    let result = std::panic::AssertUnwindSafe(async {
        let user = stable_account(&f).await;
        let selected = selected_match(&user, f.native_map, HASH, 0);
        let created = handle(&f, &user, 31, &selected.encode(false).unwrap())
            .await
            .unwrap();
        assert_match_packet(&created, 36, HASH, f.native_map);
        assert_eq!(stored_match(&f, user.id).await.map_md5, HASH);

        // This room predates the new downloadable distribution. Publishing it
        // must neither reinterpret its retained source hash nor switch its ID.
        publish_room_distribution(&f).await;
        assert_eq!(stored_match(&f, user.id).await.map_md5, HASH);
        handle(&f, &user, 44, &[]).await.unwrap();
        let started = stored_match(&f, user.id).await;
        assert!(started.in_progress);
        assert_eq!(started.statuses[0], room::PLAYING);
        assert_eq!(started.map_md5, HASH);
        assert_eq!(i64::from(started.map_id), f.native_map);
        assert_start_packet(&f, user.id, HASH, f.native_map).await;
        let source: String = sqlx::query_scalar("SELECT md5 FROM beatmaps WHERE id=$1")
            .bind(f.map)
            .fetch_one(&f.state.pool)
            .await
            .unwrap();
        assert_eq!(source, HASH);
        let alias: String = sqlx::query_scalar(
            "SELECT derived_md5 FROM local_beatmap_distribution_members WHERE beatmap_id=$1",
        )
        .bind(f.map)
        .fetch_one(&f.state.pool)
        .await
        .unwrap();
        assert_eq!(alias, DERIVED_HASH);
    })
    .catch_unwind()
    .await;
    f.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL pointing at disposable local PostgreSQL"]
async fn stable_derived_room_checks_native_id_hash_and_mode_without_rewriting_selected_hash() {
    let f = Fixture::new().await;
    let result = std::panic::AssertUnwindSafe(async {
        let user = stable_account(&f).await;
        // The sibling is a native taiko chart, so standard-chart conversion
        // permission cannot accidentally mask the wrong-mode rejection.
        sqlx::query("UPDATE beatmaps SET mode=1 WHERE id=$1")
            .bind(f.sibling)
            .execute(&f.state.pool)
            .await
            .unwrap();
        publish_room_distribution(&f).await;
        for (native, hash, mode, reason) in [
            (
                f.native_map,
                "ffffffffffffffffffffffffffffffff",
                0,
                "beatmap_id_mismatch",
            ),
            (f.native_sibling, DERIVED_HASH, 0, "beatmap_id_mismatch"),
            (f.native_map, DERIVED_SIBLING_HASH, 0, "beatmap_id_mismatch"),
            (
                f.native_sibling,
                DERIVED_SIBLING_HASH,
                0,
                "unsupported_ruleset_conversion",
            ),
        ] {
            let before = room_snapshot(&f).await;
            let selected = selected_match(&user, native, hash, mode);
            let denied = handle(&f, &user, 31, &selected.encode(false).unwrap())
                .await
                .unwrap_err();
            assert_eq!(denied.0, StatusCode::BAD_REQUEST);
            assert_eq!(denied.1, reason);
            assert_eq!(room_snapshot(&f).await, before);
        }

        for (native, hash, mode) in [
            (f.native_map, DERIVED_HASH, 0),
            (f.native_sibling, DERIVED_SIBLING_HASH, 1),
        ] {
            let selected = selected_match(&user, native, hash, mode);
            let created = handle(&f, &user, 31, &selected.encode(false).unwrap())
                .await
                .unwrap();
            assert_match_packet(&created, 36, hash, native);
            let persisted = stored_match(&f, user.id).await;
            assert_eq!(persisted.map_md5, hash);
            assert_eq!(persisted.mode, mode);

            let before = room_snapshot(&f).await;
            let mut mismatched = selected.clone();
            mismatched.map_md5 = if hash == DERIVED_HASH {
                DERIVED_SIBLING_HASH
            } else {
                DERIVED_HASH
            }
            .into();
            let denied = handle(&f, &user, 41, &mismatched.encode(false).unwrap())
                .await
                .unwrap_err();
            assert_eq!(denied.0, StatusCode::BAD_REQUEST);
            assert_eq!(denied.1, "beatmap_id_mismatch");
            assert_eq!(room_snapshot(&f).await, before);

            // Start validates the persisted identity again. Model a corrupted
            // selection only inside this fixture, then prove no partial start.
            let mut invalid = persisted.clone();
            invalid.map_md5 = mismatched.map_md5;
            sqlx::query("UPDATE stable_rooms SET state=$2 WHERE id=$1")
                .bind(persisted.id as i16)
                .bind(json!(invalid))
                .execute(&f.state.pool)
                .await
                .unwrap();
            let before = room_snapshot(&f).await;
            let denied = handle(&f, &user, 44, &[]).await.unwrap_err();
            assert_eq!(denied.0, StatusCode::BAD_REQUEST);
            assert_eq!(denied.1, "beatmap_id_mismatch");
            assert_eq!(room_snapshot(&f).await, before);
            sqlx::query("UPDATE stable_rooms SET state=$2 WHERE id=$1")
                .bind(persisted.id as i16)
                .bind(json!(persisted))
                .execute(&f.state.pool)
                .await
                .unwrap();

            handle(&f, &user, 44, &[]).await.unwrap();
            let started = stored_match(&f, user.id).await;
            assert!(started.in_progress);
            assert_eq!(started.map_md5, hash);
            assert_eq!(i64::from(started.map_id), native);
            assert_eq!(started.mode, mode);
            assert_start_packet(&f, user.id, hash, native).await;
            handle(&f, &user, 33, &[]).await.unwrap();
            sqlx::query("DELETE FROM bancho_packet_queue")
                .execute(&f.state.pool)
                .await
                .unwrap();
        }
    })
    .catch_unwind()
    .await;
    f.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
