//! Room admission tests use complete synthetic distribution registrations.
//! They exercise frozen SQL/API identity, not archive materialization or PP.
use super::*;

pub(super) const DERIVED_HASH: &str = "4123456789abcdef0123456789abcdef";
pub(super) const DERIVED_SIBLING_HASH: &str = "5123456789abcdef0123456789abcdef";

pub(super) async fn publish_room_distribution(f: &Fixture) {
    let distribution = uuid::Uuid::new_v4();
    let mut tx = f.state.pool.begin().await.unwrap();
    sqlx::query("INSERT INTO local_beatmap_distributions(package_id,id,native_set_id,format_version,kind,original_sha256,original_byte_length,archive_path,sha256,byte_length) SELECT local_package_id,$2,native_set_id,1,'osz',$3,1,$4,$5,1 FROM beatmaps WHERE id=$1")
        .bind(f.map).bind(distribution).bind("2".repeat(64))
        .bind(format!("distribution-{distribution}/package.osz")).bind("3".repeat(64))
        .execute(&mut *tx).await.unwrap();
    for (map, checksum) in [(f.map, DERIVED_HASH), (f.sibling, DERIVED_SIBLING_HASH)] {
        sqlx::query("INSERT INTO local_beatmap_distribution_members(distribution_id,beatmap_id,original_md5,derived_md5,native_id,native_set_id,osu_path,sha256,byte_length) SELECT $2,id,md5,$3,native_id,native_set_id,$4,$5,1 FROM beatmaps WHERE id=$1")
            .bind(map).bind(distribution).bind(checksum)
            .bind(format!("distribution-{distribution}/{checksum}.osu"))
            .bind("6".repeat(64)).execute(&mut *tx).await.unwrap();
    }
    tx.commit().await.unwrap();
}

async fn token_for_checksum(
    f: &Fixture,
    room: i64,
    item: i64,
    native: i64,
    checksum: &str,
    expected: u16,
) -> Value {
    body(f.http.post(f.url(&format!("/api/v2/rooms/{room}/playlist/{item}/scores")))
        .bearer_auth(&f.tokens[0])
        .json(&json!({"beatmap_id":native,"beatmap_hash":checksum,"version_hash":VERSION,"ruleset_id":0}))
        .send().await.unwrap(), expected).await
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL pointing at disposable local PostgreSQL"]
async fn original_and_derived_room_rounds_keep_exact_token_and_retry_checksum() {
    let f = Fixture::new().await;
    let result = std::panic::AssertUnwindSafe(async {
        let room = f.room(&[0, 1]).await;
        let original_round = f.round(room, 1, &[0, 1]).await;
        let (old_token, old_score) = f.seed_processed((room, 1, original_round), 0, 600, true, 1.0).await;
        f.finish(original_round).await;
        publish_room_distribution(&f).await;
        // Publishing a derivative must not rewrite either the old frozen
        // roster or a receipt admitted before explicit client hashes existed.
        let old = sqlx::query("SELECT p.beatmap_checksum,t.client_beatmap_md5 token_hash,s.client_beatmap_md5 score_hash FROM lazer_room_round_participants p JOIN lazer_score_tokens t ON t.round_id=p.round_id AND t.user_id=p.user_id JOIN scores s ON s.id=t.score_id WHERE t.id=$1")
            .bind(old_token).fetch_one(&f.state.pool).await.unwrap();
        assert_eq!(old.get::<String, _>("beatmap_checksum"), HASH);
        assert_eq!(old.get::<Option<String>, _>("token_hash"), None);
        assert_eq!(old.get::<Option<String>, _>("score_hash"), None);
        assert_eq!(f.put(0, room, 1, old_token, &submission(600), 200).await["id"], old_score);
        assert_eq!(room_scores::processed_score_id(&f.state, f.users[0], old_token).await.unwrap(), Some(old_score));

        // A new derived playlist freezes one exact hash. Other registered
        // bytes for the same difficulty cannot borrow its issued token.
        sqlx::query("UPDATE lazer_room_members SET state=1 WHERE room_id=$1")
            .bind(room).execute(&f.state.pool).await.unwrap();
        let mut snapshot = f.playlist(2);
        snapshot["beatmapChecksum"] = json!(DERIVED_HASH);
        let mut tx = f.state.pool.begin().await.unwrap();
        let round = room_rounds::freeze_round(&mut tx, room, &snapshot, &f.users[..2]).await.unwrap();
        sqlx::query("UPDATE lazer_rooms SET state=1 WHERE id=$1").bind(room).execute(&mut *tx).await.unwrap();
        sqlx::query("UPDATE lazer_room_members SET state=2 WHERE room_id=$1").bind(room).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        let rejected = token_for_checksum(&f, room, 2, f.native_map, HASH, 400).await;
        assert_eq!(rejected["error"], "room_score_context_mismatch");
        for (map, hash) in [(f.native_map, DERIVED_SIBLING_HASH), (f.native_sibling, DERIVED_HASH), (f.native_map, "ffffffffffffffffffffffffffffffff")] {
            assert_eq!(token_for_checksum(&f, room, 2, map, hash, 400).await["error"], "room_score_context_mismatch");
        }
        let token = token_for_checksum(&f, room, 2, f.native_map, DERIVED_HASH, 200).await["id"].as_i64().unwrap();
        assert_eq!(token_for_checksum(&f, room, 2, f.native_map, DERIVED_HASH, 200).await["id"], token);
        let frozen = sqlx::query("SELECT p.beatmap_checksum,t.client_beatmap_md5,t.beatmap_id FROM lazer_score_tokens t JOIN lazer_room_round_participants p ON p.round_id=t.round_id AND p.user_id=t.user_id WHERE t.id=$1")
            .bind(token).fetch_one(&f.state.pool).await.unwrap();
        assert_eq!(frozen.get::<String, _>("beatmap_checksum"), DERIVED_HASH);
        assert_eq!(frozen.get::<Option<String>, _>("client_beatmap_md5").as_deref(), Some(DERIVED_HASH));
        assert_eq!(frozen.get::<i64, _>("beatmap_id"), f.map);
        f.finish(round).await;
        let mut score = submission(700);
        score["beatmap_id"] = json!(f.native_map);
        score["beatmap_hash"] = json!(DERIVED_HASH);
        let mut source_score = score.clone();
        source_score["beatmap_hash"] = json!(HASH);
        assert_eq!(f.put(0, room, 2, token, &source_score, 400).await["error"], "room_score_context_mismatch");
        let submitted = f.put(0, room, 2, token, &score, 200).await;
        let id = submitted["id"].as_i64().unwrap();
        let saved: (i64, Option<String>) = sqlx::query_as("SELECT beatmap_id,client_beatmap_md5 FROM scores WHERE id=$1")
            .bind(id).fetch_one(&f.state.pool).await.unwrap();
        assert_eq!(saved, (f.map, Some(DERIVED_HASH.to_owned())));
        assert_eq!(f.put(0, room, 2, token, &score, 200).await["id"], id);
        assert_eq!(f.put(0, room, 2, token, &source_score, 409).await["error"], "score_token_already_used");
        assert_eq!(room_scores::processed_score_id(&f.state, f.users[0], token).await.unwrap(), Some(id));
        assert_eq!(f.get(1, &format!("/api/v2/rooms/{room}/playlist/2/scores"), 200).await["scores"][0]["id"], id);
        // A valid checksum registered to another map remains invalid even for
        // an exact processed receipt. Reads use the same frozen binding.
        sqlx::query("UPDATE lazer_room_round_participants SET beatmap_checksum=$3 WHERE round_id=$1 AND user_id=$2")
            .bind(round).bind(f.users[0]).bind(DERIVED_SIBLING_HASH).execute(&f.state.pool).await.unwrap();
        assert_eq!(f.put(0, room, 2, token, &score, 400).await["error"], "room_score_context_mismatch");
        assert_eq!(room_scores::processed_score_id(&f.state, f.users[0], token).await.unwrap(), None);
        assert!(f.get(1, &format!("/api/v2/rooms/{room}/playlist/2/scores"), 200).await["scores"].as_array().unwrap().is_empty());
        assert_eq!(f.ingress.calls.load(Ordering::SeqCst), 1);
        assert_eq!(f.count("SELECT count(*) FROM scores").await, 2);
    }).catch_unwind().await;
    f.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL pointing at disposable local PostgreSQL"]
async fn new_signalr_playlists_and_freestyle_siblings_prefer_published_distribution() {
    let f = Fixture::new().await;
    let result = std::panic::AssertUnwindSafe(async {
        publish_room_distribution(&f).await;
        let mut host = Client::open(&f, 2, "multiplayer", false).await;
        let settings = json!({"name":"Derived identity fixture","password":"","playlistItemId":0,"matchType":1,"queueMode":0,"autoStartDuration":0,"autoSkip":false,"maxParticipants":16});
        let created = host.ok("CreateRoom", vec![json!({"settings":settings,"playlist":[f.playlist(0)]})]).await;
        assert_eq!(created["playlist"][0]["beatmapChecksum"], DERIVED_HASH);
        let room = created["roomID"].as_i64().unwrap();
        let stored: Value = sqlx::query_scalar("SELECT playlist FROM lazer_rooms WHERE id=$1")
            .bind(room).fetch_one(&f.state.pool).await.unwrap();
        assert_eq!(stored[0]["beatmapChecksum"], DERIVED_HASH);
        let mut foreign = f.playlist(0);
        foreign["beatmapChecksum"] = json!(DERIVED_SIBLING_HASH);
        let denied = host.call("AddPlaylistItem", vec![foreign]).await;
        assert_eq!(denied["error"], "beatmap_checksum_mismatch");
        host.ok("LeaveRoom", vec![]).await;
        host.socket.close(None).await.unwrap();

        let room = f.room(&[0, 1]).await;
        sqlx::query("UPDATE lazer_room_members SET beatmap_id=$2 WHERE room_id=$1 AND user_id=$3")
            .bind(room).bind(f.native_sibling).bind(f.users[1]).execute(&f.state.pool).await.unwrap();
        let mut snapshot = f.playlist(1);
        snapshot["freestyle"] = json!(true);
        let mut tx = f.state.pool.begin().await.unwrap();
        let round = room_rounds::freeze_round(&mut tx, room, &snapshot, &f.users[..2]).await.unwrap();
        tx.commit().await.unwrap();
        let hashes: Vec<(i64, String)> = sqlx::query_as("SELECT beatmap_id,beatmap_checksum FROM lazer_room_round_participants WHERE round_id=$1 ORDER BY user_id")
            .bind(round).fetch_all(&f.state.pool).await.unwrap();
        assert_eq!(hashes, vec![(f.map, HASH.into()), (f.sibling, DERIVED_SIBLING_HASH.into())]);
    }).catch_unwind().await;
    f.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
