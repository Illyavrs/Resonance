//! Real HTTP and PostgreSQL checks use the existing disposable refresh fixture.
use super::*;

fn logout_request(token: Option<&str>) -> Request<Body> {
    let mut request = Request::builder()
        .method("DELETE")
        .uri("/api/v2/oauth/tokens/current");
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    request.body(Body::empty()).unwrap()
}

async fn another_session(f: &Fixture, user: i64) -> Session {
    let session = Session {
        id: Uuid::new_v4(),
        user,
        access: common::secret(),
        refresh: common::secret(),
    };
    sqlx::query("INSERT INTO lazer_sessions(id,user_id,access_hash,refresh_hash,access_expires_at,refresh_expires_at) VALUES($1,$2,$3,$4,clock_timestamp()+interval '1 hour',clock_timestamp()+interval '1 day')")
        .bind(session.id).bind(user).bind(common::token_hash(&session.access)).bind(common::token_hash(&session.refresh))
        .execute(&f.pool).await.unwrap();
    session
}

async fn logout(f: &Fixture, token: Option<&str>) -> Response {
    f.router
        .clone()
        .oneshot(logout_request(token))
        .await
        .unwrap()
}

fixture_test!(
    native_logout_revokes_only_current_credentials_and_retains_receipts,
    f,
    {
        let current = f.seed("LogoutCurrent").await;
        let other_device = another_session(&f, current.user).await;
        let other_user = f.seed("LogoutOtherUser").await;
        let web = Uuid::new_v4();
        let stable = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO sessions(id,user_id,expires_at) VALUES($1,$2,now()+interval '1 day')",
        )
        .bind(web)
        .bind(current.user)
        .execute(&f.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO bancho_sessions(id,user_id,token_hash,expires_at) VALUES($1,$2,$3,now()+interval '1 day')")
        .bind(stable).bind(current.user).bind(common::secret()).execute(&f.pool).await.unwrap();
        let map: i64 = sqlx::query_scalar("INSERT INTO beatmaps(official_id,official_set_id,title,artist,creator,version,mode,source,stars,md5,osu_path) VALUES(12001,13001,'Session fixture','Fixture','Fixture','Normal',0,'official',1,$1,'fixture.osu') RETURNING id")
        .bind("a".repeat(32)).fetch_one(&f.pool).await.unwrap();
        let token: i64 = sqlx::query_scalar("INSERT INTO lazer_score_tokens(user_id,beatmap_id,ruleset_id,version_hash,expires_at,received_session_id) VALUES($1,$2,0,'fixture-version',now()+interval '1 hour',$3) RETURNING id")
        .bind(current.user).bind(map).bind(current.id).fetch_one(&f.pool).await.unwrap();
        sqlx::query("INSERT INTO lazer_replay_captures(token_id,user_id,connection_key,initial_state) VALUES($1,$2,'fixture-connection','{}')")
        .bind(token).bind(current.user).execute(&f.pool).await.unwrap();
        let before: Value =
            sqlx::query_scalar("SELECT to_jsonb(t) FROM lazer_score_tokens t WHERE id=$1")
                .bind(token)
                .fetch_one(&f.pool)
                .await
                .unwrap();

        assert_eq!(logout(&f, None).await.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            logout(&f, Some("unknown-fixture-token")).await.status(),
            StatusCode::UNAUTHORIZED
        );
        let response = logout(&f, Some(&current.access)).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(response.headers()["cache-control"], "private, no-store");
        assert!(
            to_bytes(response.into_body(), 1024)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            logout(&f, Some(&current.access)).await.status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            f.me(&current.access).await.status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            f.refresh(&current.refresh).await.status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(f.me(&other_device.access).await.status(), StatusCode::OK);
        assert_eq!(f.me(&other_user.access).await.status(), StatusCode::OK);
        let (_, other_refresh) = replacement(f.refresh(&other_device.refresh).await).await;
        assert!(!other_refresh.is_empty());
        let after: Value =
            sqlx::query_scalar("SELECT to_jsonb(t) FROM lazer_score_tokens t WHERE id=$1")
                .bind(token)
                .fetch_one(&f.pool)
                .await
                .unwrap();
        assert_eq!(after, before, "logout must retain accepted token history");
        let history: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM sessions WHERE id=$1),(SELECT count(*) FROM bancho_sessions WHERE id=$2),(SELECT count(*) FROM lazer_replay_captures WHERE token_id=$3)")
        .bind(web).bind(stable).bind(token).fetch_one(&f.pool).await.unwrap();
        assert_eq!(history, (1, 1, 1));
    }
);

fixture_test!(
    native_logout_and_refresh_serialize_without_reviving_credentials,
    f,
    {
        let current = f.seed("LogoutRefreshRace").await;
        let other_device = another_session(&f, current.user).await;
        let blocker = f.lock_user(current.user).await;
        let router = f.router.clone();
        let request = logout_request(Some(&current.access));
        let logging_out = tokio::spawn(async move { router.oneshot(request).await.unwrap() });
        f.wait_for_user_locks(1).await;
        let router = f.router.clone();
        let request = refresh_request(&current.refresh);
        let refreshing = tokio::spawn(async move { router.oneshot(request).await.unwrap() });
        f.wait_for_user_locks(2).await;
        blocker.commit().await.unwrap();
        let (logged_out, refreshed) = tokio::time::timeout(Duration::from_secs(5), async {
            (logging_out.await.unwrap(), refreshing.await.unwrap())
        })
        .await
        .unwrap();
        match logged_out.status() {
            StatusCode::NO_CONTENT => {
                assert_eq!(refreshed.status(), StatusCode::UNAUTHORIZED);
                let remaining: i64 =
                    sqlx::query_scalar("SELECT count(*) FROM lazer_sessions WHERE id=$1")
                        .bind(current.id)
                        .fetch_one(&f.pool)
                        .await
                        .unwrap();
                assert_eq!(remaining, 0);
            }
            StatusCode::UNAUTHORIZED => {
                let (access, refresh) = replacement(refreshed).await;
                let id: Uuid =
                    sqlx::query_scalar("SELECT id FROM lazer_sessions WHERE access_hash=$1")
                        .bind(common::token_hash(&access))
                        .fetch_one(&f.pool)
                        .await
                        .unwrap();
                assert_eq!(id, current.id);
                assert_eq!(f.me(&access).await.status(), StatusCode::OK);
                assert_eq!(
                    logout(&f, Some(&current.access)).await.status(),
                    StatusCode::UNAUTHORIZED
                );
                assert_eq!(
                    logout(&f, Some(&access)).await.status(),
                    StatusCode::NO_CONTENT
                );
                assert_eq!(f.refresh(&refresh).await.status(), StatusCode::UNAUTHORIZED);
            }
            status => panic!("unexpected logout status {status}"),
        }
        assert_eq!(f.me(&other_device.access).await.status(), StatusCode::OK);
        assert_eq!(
            f.refresh(&current.refresh).await.status(),
            StatusCode::UNAUTHORIZED
        );
    }
);

fixture_test!(
    native_logout_rechecks_expiry_and_accepts_restricted_account_cleanup,
    f,
    {
        let current = f.seed("LogoutExpiry").await;
        let blocker = f.lock_user(current.user).await;
        let router = f.router.clone();
        let request = logout_request(Some(&current.access));
        let logging_out = tokio::spawn(async move { router.oneshot(request).await.unwrap() });
        f.wait_for_user_locks(1).await;
        sqlx::query("UPDATE lazer_sessions SET access_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1")
        .bind(current.id).execute(&f.pool).await.unwrap();
        blocker.commit().await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), logging_out)
                .await
                .unwrap()
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let unchanged: i64 = sqlx::query_scalar("SELECT count(*) FROM lazer_sessions WHERE id=$1")
            .bind(current.id)
            .fetch_one(&f.pool)
            .await
            .unwrap();
        assert_eq!(
            unchanged, 1,
            "expired access cannot revoke a still-refreshable login"
        );
        let (access, _) = replacement(f.refresh(&current.refresh).await).await;
        sqlx::query("UPDATE users SET is_restricted=true WHERE id=$1")
            .bind(current.user)
            .execute(&f.pool)
            .await
            .unwrap();
        assert_eq!(
            logout(&f, Some(&access)).await.status(),
            StatusCode::NO_CONTENT
        );
    }
);

fixture_test!(
    native_expired_session_cleanup_is_bounded_and_preserves_refreshable_devices,
    f,
    {
        let current = f.seed("LogoutCleanup").await;
        let other_user = f.seed("LogoutCleanupOther").await;
        // Expired access alone does not expire a remembered login.
        sqlx::query("UPDATE lazer_sessions SET access_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1")
        .bind(current.id).execute(&f.pool).await.unwrap();
        for _ in 0..101 {
            sqlx::query("INSERT INTO lazer_sessions(id,user_id,access_hash,refresh_hash,access_expires_at,refresh_expires_at) VALUES($1,$2,$3,$4,clock_timestamp()-interval '2 days',clock_timestamp()-interval '1 day')")
            .bind(Uuid::new_v4()).bind(current.user).bind(common::secret()).bind(common::secret())
            .execute(&f.pool).await.unwrap();
        }
        let mut tx = f.lock_user(current.user).await;
        lazer::sessions::cleanup_expired(&mut tx, current.user)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let remaining: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM lazer_sessions WHERE user_id=$1 AND refresh_expires_at<=clock_timestamp()),(SELECT count(*) FROM lazer_sessions WHERE id=$2),(SELECT count(*) FROM lazer_sessions WHERE id=$3)")
        .bind(current.user).bind(current.id).bind(other_user.id).fetch_one(&f.pool).await.unwrap();
        assert_eq!(remaining, (1, 1, 1));
        let (_, replacement_refresh) = replacement(f.refresh(&current.refresh).await).await;
        assert!(!replacement_refresh.is_empty());
        let expired: i64 = sqlx::query_scalar("SELECT count(*) FROM lazer_sessions WHERE user_id=$1 AND refresh_expires_at<=clock_timestamp()")
        .bind(current.user).fetch_one(&f.pool).await.unwrap();
        assert_eq!(expired, 0);
        assert_eq!(f.me(&other_user.access).await.status(), StatusCode::OK);
    }
);
