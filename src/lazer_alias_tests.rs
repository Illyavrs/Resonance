//! Empty optional native rulesets use a trailing slash, without a redirect.
use super::*;
use axum::{
    body::{Body, to_bytes},
    http::{Method, Request},
};
use futures_util::FutureExt;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::{str::FromStr, sync::Arc};
use tower::ServiceExt;

fn state(pool: sqlx::PgPool) -> AppState {
    AppState {
        pool,
        storage: Arc::new(std::env::temp_dir()),
        sanctuary_url: Arc::new("http://127.0.0.1:1".into()),
        service_key: Arc::new(String::new()),
        http: reqwest::Client::new(),
        limits: Default::default(),
        trusted_proxies: Default::default(),
        signalr: Arc::new(crate::signalr::Runtime::new()),
        lazer_chat: Arc::new(crate::lazer_chat::Runtime::new()),
    }
}

#[tokio::test]
async fn empty_ruleset_user_aliases_keep_authentication_without_redirects() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
        .unwrap();
    let app = router().with_state(state(pool));
    for path in [
        "/api/v2/me",
        "/api/v2/me/",
        "/api/v2/users/1",
        "/api/v2/users/1/",
    ] {
        for method in [Method::GET, Method::HEAD] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method.clone())
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "{method} {path}"
            );
            assert!(response.headers().get("location").is_none());
        }
    }
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL pointing at disposable local PostgreSQL"]
async fn empty_ruleset_user_aliases_return_identical_authenticated_api_models() {
    let url = std::env::var("ILLYA_TEST_DATABASE_URL").expect("disposable PostgreSQL URL");
    assert!(matches!(
        reqwest::Url::parse(&url).unwrap().host_str(),
        Some("127.0.0.1" | "localhost" | "[::1]")
    ));
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .unwrap();
    let schema = format!("native_user_alias_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await
        .unwrap();
    let options = PgConnectOptions::from_str(&url)
        .unwrap()
        .options([("search_path", schema.as_str())]);
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
    let app = router().with_state(state(pool.clone()));
    let result=std::panic::AssertUnwindSafe(async {
        let id:i64=sqlx::query_scalar("INSERT INTO users(username,email,password_hash,password_md5_hash) VALUES('UserAlias','useralias@invalid.test','unused','unused') RETURNING id").fetch_one(&pool).await.unwrap();
        let token=common::secret();
        sqlx::query("INSERT INTO lazer_sessions(id,user_id,access_hash,refresh_hash,access_expires_at,refresh_expires_at) VALUES($1,$2,$3,$4,clock_timestamp()+interval '1 hour',clock_timestamp()+interval '1 day')")
            .bind(uuid::Uuid::new_v4()).bind(id).bind(common::token_hash(&token)).bind(common::secret()).execute(&pool).await.unwrap();
        for path in ["/api/v2/me".to_owned(),format!("/api/v2/users/{id}"),"/api/v2/users/UserAlias".to_owned()] {
            for query in ["","?variant=relax","?key=username"] {
                let mut models=Vec::new();
                for suffix in ["","/"] {
                    let response=app.clone().oneshot(Request::builder().uri(format!("{path}{suffix}{query}")).header("authorization",format!("Bearer {token}")).body(Body::empty()).unwrap()).await.unwrap();
                    assert_eq!(response.status(),StatusCode::OK,"{path}{suffix}{query}");
                    assert!(response.headers().get("location").is_none());
                    let value:Value=serde_json::from_slice(&to_bytes(response.into_body(),65536).await.unwrap()).unwrap();
                    assert_eq!(value["id"],id); assert_eq!(value["username"],"UserAlias");
                    assert_eq!(value["playmode"],"osu"); assert!(value["statistics"].is_object());
                    models.push(value);
                    let response=app.clone().oneshot(Request::builder().method(Method::HEAD).uri(format!("{path}{suffix}{query}")).header("authorization",format!("Bearer {token}")).body(Body::empty()).unwrap()).await.unwrap();
                    assert_eq!(response.status(),StatusCode::OK); assert!(response.headers().get("location").is_none());
                    assert!(to_bytes(response.into_body(),65536).await.unwrap().is_empty());
                }
                assert_eq!(models[0],models[1],"same handlers serialize identical models");
            }
        }
        let response=app.clone().oneshot(Request::builder().uri("/api/v2/me/invalid").header("authorization",format!("Bearer {token}")).body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(),StatusCode::BAD_REQUEST,"a nonempty invalid ruleset still fails");
    }).catch_unwind().await;
    drop(app);
    pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
