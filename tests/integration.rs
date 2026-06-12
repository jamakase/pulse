use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use http_body_util::BodyExt;
use pulse::{AppState, compactor, config::Config, query::QueryEngine, wal::Wal};
use serde_json::{Value, json};
use tower::ServiceExt;

const KEY: &str = "test-key-0123456789abcdef";

fn test_config(dir: &std::path::Path) -> Config {
    Config {
        port: 0,
        data_dir: dir.to_path_buf(),
        api_key: KEY.to_string(),
        allowed_origins: vec!["https://app.example.com".to_string()],
        compact_interval_secs: 3600,
        ttl_days: 730,
        property_denylist: vec!["email".to_string(), "phone".to_string()],
    }
}

struct Harness {
    state: AppState,
    lock: Arc<tokio::sync::RwLock<()>>,
    _tmp: tempfile::TempDir,
}

fn harness() -> Harness {
    let tmp = tempfile::tempdir().unwrap();
    let config = Arc::new(test_config(tmp.path()));
    std::fs::create_dir_all(config.wal_dir()).unwrap();
    std::fs::create_dir_all(config.events_dir()).unwrap();
    let lock = Arc::new(tokio::sync::RwLock::new(()));
    let wal = Arc::new(Wal::new(config.wal_dir()).unwrap());
    let engine = Arc::new(QueryEngine::new(
        config.events_dir(),
        config.wal_dir(),
        lock.clone(),
    ));
    Harness {
        state: AppState {
            config,
            wal,
            engine,
        },
        lock,
        _tmp: tmp,
    }
}

fn post_events(body: Value, key: Option<&str>, origin: Option<&str>) -> Request<Body> {
    let mut req = Request::builder()
        .method("POST")
        .uri("/v1/events")
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(k) = key {
        req = req.header(header::AUTHORIZATION, format!("Bearer {k}"));
    }
    if let Some(o) = origin {
        req = req.header(header::ORIGIN, o);
    }
    req.body(Body::from(body.to_string())).unwrap()
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn health_is_public() {
    let h = harness();
    let app = pulse::build_router(h.state.clone());
    let resp = app
        .oneshot(Request::get("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn ingest_requires_key_and_allowed_origin() {
    let h = harness();
    let events = json!([{"product": "demo", "event": "x"}]);

    // No key → 401.
    let app = pulse::build_router(h.state.clone());
    let resp = app
        .oneshot(post_events(events.clone(), None, None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Wrong key → 401.
    let app = pulse::build_router(h.state.clone());
    let resp = app
        .oneshot(post_events(
            events.clone(),
            Some("nope-nope-nope-nope"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Valid key but foreign Origin → 403.
    let app = pulse::build_router(h.state.clone());
    let resp = app
        .oneshot(post_events(
            events.clone(),
            Some(KEY),
            Some("https://evil.example.com"),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // Valid key + allowed Origin → 202.
    let app = pulse::build_router(h.state.clone());
    let resp = app
        .oneshot(post_events(
            events,
            Some(KEY),
            Some("https://app.example.com"),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
}

#[tokio::test]
async fn ingest_validates_and_strips_pii() {
    let h = harness();
    let app = pulse::build_router(h.state.clone());
    let batch = json!({"events": [
        {"product": "demo", "event": "signup", "user_id": "u1",
         "properties": {"email": "a@b.c", "plan": "pro"}},
        {"event": "missing-product"}
    ]});
    let resp = app
        .oneshot(post_events(batch, Some(KEY), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let body = body_json(resp).await;
    assert_eq!(body["accepted"], 1);
    assert_eq!(body["rejected"].as_array().unwrap().len(), 1);
    assert_eq!(body["rejected"][0]["index"], 1);

    // The WAL line must not contain the denylisted email but keep plan.
    let wal_content =
        std::fs::read_to_string(h.state.config.wal_dir().join("current.ndjson")).unwrap();
    assert!(!wal_content.contains("a@b.c"));
    assert!(wal_content.contains(r#"\"plan\":\"pro\""#));
}

#[tokio::test]
async fn full_cycle_ingest_compact_query() {
    let h = harness();

    // Ingest two batches: one stays in WAL, one gets compacted to Parquet.
    let app = pulse::build_router(h.state.clone());
    let batch1 = json!([
        {"product": "demo", "event": "signup", "user_id": "u1",
         "occurred_at": "2026-06-01T10:00:00Z"},
        {"product": "demo", "event": "estimate_computed", "user_id": "u1",
         "occurred_at": "2026-06-01T10:05:00Z", "properties": {"total": 100}},
    ]);
    let resp = app
        .oneshot(post_events(batch1, Some(KEY), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    let n = compactor::compact_once(&h.state.config, &h.state.wal, &h.lock)
        .await
        .unwrap();
    assert_eq!(n, 1);

    // Parquet partition dirs exist, sealed WAL is gone.
    let demo_dir = h.state.config.events_dir().join("product=demo");
    assert!(demo_dir.join("date=2026-06-01").is_dir());
    assert!(h.state.wal.rotate_and_list_sealed().unwrap().is_empty());

    // Second batch stays in the WAL tail.
    let app = pulse::build_router(h.state.clone());
    let batch2 = json!([
        {"product": "demo", "event": "checkout_started", "user_id": "u1",
         "occurred_at": "2026-06-02T09:00:00Z"},
    ]);
    let resp = app
        .oneshot(post_events(batch2, Some(KEY), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    // Query must see Parquet + WAL union.
    let rows = h
        .state
        .engine
        .query(
            "SELECT event, user_id, occurred_at FROM events ORDER BY occurred_at",
            100,
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["event"], "signup");
    assert_eq!(rows[2]["event"], "checkout_started");

    // Aggregation across both storage tiers.
    let rows = h
        .state
        .engine
        .query(
            "SELECT count(*) AS n, count(DISTINCT user_id) AS users FROM events WHERE product = 'demo'",
            100,
        )
        .await
        .unwrap();
    assert_eq!(rows[0]["n"], 3);
    assert_eq!(rows[0]["users"], 1);
}

#[tokio::test]
async fn query_rejects_writes() {
    let h = harness();
    for sql in [
        "INSERT INTO events VALUES (1)",
        "CREATE TABLE x (a int)",
        "DROP TABLE events",
        "SET datafusion.execution.batch_size = 1",
    ] {
        let err = h.state.engine.query(sql, 10).await;
        assert!(err.is_err(), "expected rejection for: {sql}");
    }
}

#[tokio::test]
async fn ttl_drops_old_partitions() {
    let h = harness();
    let old = h
        .state
        .config
        .events_dir()
        .join("product=demo/date=2020-01-01");
    let fresh = h
        .state
        .config
        .events_dir()
        .join("product=demo/date=2099-01-01");
    std::fs::create_dir_all(&old).unwrap();
    std::fs::create_dir_all(&fresh).unwrap();
    compactor::enforce_ttl(&h.state.config.events_dir(), 730).unwrap();
    assert!(!old.exists());
    assert!(fresh.exists());
}
