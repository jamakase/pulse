use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use http_body_util::BodyExt;
use pulse::{AppState, compactor, config::Config, query::QueryEngine, wal::Wal};
use serde_json::{Value, json};
use tower::ServiceExt;

const KEY: &str = "test-key-0123456789abcdef"; // admin: MCP + erasure only
const SERVER_KEY: &str = "ps_demo_0123456789abcdef"; // secret, append, source='server' allowed
const CLIENT_KEY: &str = "pc_demo_0123456789abcdef"; // public, append, source forced 'client'

fn test_config(dir: &std::path::Path) -> Config {
    Config {
        port: 0,
        data_dir: dir.to_path_buf(),
        admin_key: KEY.to_string(),
        server_keys: vec![pulse::config::ProductKey {
            product: "demo".to_string(),
            key: SERVER_KEY.to_string(),
        }],
        client_keys: vec![pulse::config::ProductKey {
            product: "demo".to_string(),
            key: CLIENT_KEY.to_string(),
        }],
        allowed_origins: vec!["https://app.example.com".to_string()],
        allowed_hosts: vec![],
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
            compaction_lock: lock.clone(),
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
            Some(SERVER_KEY),
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
            Some(SERVER_KEY),
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
    // `product` is omitted on purpose: it always comes from the key now.
    // The second event has no name and must be rejected.
    let batch = json!({"events": [
        {"event": "signup", "user_id": "u1",
         "properties": {"email": "a@b.c", "plan": "pro"}},
        {"user_id": "u2"}
    ]});
    let resp = app
        .oneshot(post_events(batch, Some(SERVER_KEY), None))
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
        .oneshot(post_events(batch1, Some(SERVER_KEY), None))
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
        .oneshot(post_events(batch2, Some(SERVER_KEY), None))
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

#[tokio::test]
async fn funnel_counts_users_per_step() {
    let h = harness();
    let app = pulse::build_router(h.state.clone());
    // u1 completes signup→estimate→checkout, u2 stops after estimate,
    // u3 only signs up, u4 does steps out of order.
    let batch = json!([
        {"product":"demo","event":"signup","user_id":"u1","occurred_at":"2026-06-01T10:00:00Z"},
        {"product":"demo","event":"estimate","user_id":"u1","occurred_at":"2026-06-01T11:00:00Z"},
        {"product":"demo","event":"checkout","user_id":"u1","occurred_at":"2026-06-01T12:00:00Z"},
        {"product":"demo","event":"signup","user_id":"u2","occurred_at":"2026-06-01T10:00:00Z"},
        {"product":"demo","event":"estimate","user_id":"u2","occurred_at":"2026-06-02T10:00:00Z"},
        {"product":"demo","event":"signup","user_id":"u3","occurred_at":"2026-06-01T10:00:00Z"},
        {"product":"demo","event":"checkout","user_id":"u4","occurred_at":"2026-06-01T10:00:00Z"},
        {"product":"demo","event":"signup","user_id":"u4","occurred_at":"2026-06-01T11:00:00Z"}
    ]);
    let resp = app
        .oneshot(post_events(batch, Some(SERVER_KEY), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    let steps = vec![
        "signup".to_string(),
        "estimate".to_string(),
        "checkout".to_string(),
    ];
    let counts = pulse::funnel::compute(
        &h.state.engine,
        "demo",
        &steps,
        std::time::Duration::from_secs(7 * 86400),
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(counts, vec![4, 2, 1]);

    // Tight window: u2's estimate came a day later and falls out.
    let counts = pulse::funnel::compute(
        &h.state.engine,
        "demo",
        &steps,
        std::time::Duration::from_secs(3 * 3600),
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(counts, vec![4, 1, 1]);
}

#[tokio::test]
async fn identity_links_stitch_anonymous_history() {
    let h = harness();
    let app = pulse::build_router(h.state.clone());
    let batch = json!([
        {"product":"demo","event":"page_view","anonymous_id":"a1",
         "occurred_at":"2026-06-01T10:00:00Z","source":"client"},
        {"product":"demo","event":"$identify","anonymous_id":"a1","user_id":"u1",
         "occurred_at":"2026-06-01T10:05:00Z"},
        {"product":"demo","event":"purchase","user_id":"u1",
         "occurred_at":"2026-06-01T10:30:00Z"}
    ]);
    let resp = app
        .oneshot(post_events(batch, Some(SERVER_KEY), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    let links = h
        .state
        .engine
        .query("SELECT * FROM identity_links", 10)
        .await
        .unwrap();
    assert_eq!(links.len(), 1);
    assert_eq!(links[0]["anonymous_id"], "a1");
    assert_eq!(links[0]["user_id"], "u1");

    // The timeline-style join finds the pre-signup pageview too.
    let rows = h
        .state
        .engine
        .query(
            "SELECT event FROM events WHERE product = 'demo' AND (user_id = 'u1' OR \
             anonymous_id IN (SELECT anonymous_id FROM identity_links WHERE user_id = 'u1' \
             AND product = 'demo')) ORDER BY occurred_at",
            10,
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["event"], "page_view");
}

#[tokio::test]
async fn erase_user_endpoint_removes_data() {
    let h = harness();
    let app = pulse::build_router(h.state.clone());
    let batch = json!([
        {"product":"demo","event":"signup","user_id":"gone","occurred_at":"2026-06-01T10:00:00Z"},
        {"product":"demo","event":"purchase","user_id":"gone","occurred_at":"2026-06-02T10:00:00Z"},
        {"product":"demo","event":"signup","user_id":"stays","occurred_at":"2026-06-01T11:00:00Z"}
    ]);
    let resp = app
        .oneshot(post_events(batch, Some(SERVER_KEY), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    // Without key → 401.
    let app = pulse::build_router(h.state.clone());
    let resp = app
        .oneshot(
            Request::delete("/v1/users/gone?product=demo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Authorized erase → both events of 'gone' deleted (incl. WAL via compact).
    let app = pulse::build_router(h.state.clone());
    let resp = app
        .oneshot(
            Request::delete("/v1/users/gone?product=demo")
                .header(header::AUTHORIZATION, format!("Bearer {KEY}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["deleted"], 2);

    let rows = h
        .state
        .engine
        .query("SELECT user_id FROM events", 10)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["user_id"], "stays");
}

#[tokio::test]
async fn write_key_forces_product_and_cannot_read() {
    let h = harness();

    // Write key in the Authorization header: body claims another product and
    // a trusted source, but the key pins product to 'demo' and demotes the
    // event to source='client'.
    let app = pulse::build_router(h.state.clone());
    let resp = app
        .oneshot(post_events(
            json!([{"product": "spoofed", "event": "signup", "user_id": "u1",
                    "source": "server"}]),
            Some(CLIENT_KEY),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    // Write key as ?key= (the sendBeacon path), allowed browser Origin.
    let app = pulse::build_router(h.state.clone());
    let req = Request::builder()
        .method("POST")
        .uri(format!("/v1/events?key={CLIENT_KEY}"))
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ORIGIN, "https://app.example.com")
        .body(Body::from(json!([{"event": "page_view"}]).to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    // Both events landed under product=demo regardless of body claims, and
    // none of them could smuggle source='server' through a public key.
    let rows = h
        .state
        .engine
        .query(
            "SELECT product, source, count(*) AS n FROM events GROUP BY product, source",
            10,
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["product"], "demo");
    assert_eq!(rows[0]["source"], "client");
    assert_eq!(rows[0]["n"], 2);

    // The public write key must NOT open the admin surface (MCP, erasure).
    let app = pulse::build_router(h.state.clone());
    let resp = app
        .oneshot(
            Request::delete("/v1/users/u1?product=demo")
                .header(header::AUTHORIZATION, format!("Bearer {CLIENT_KEY}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let app = pulse::build_router(h.state.clone());
    let resp = app
        .oneshot(
            Request::post("/mcp")
                .header(header::AUTHORIZATION, format!("Bearer {CLIENT_KEY}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn key_tiers_enforce_trust_levels() {
    let h = harness();

    // The admin key deliberately CANNOT ingest — it must never live in an app.
    let app = pulse::build_router(h.state.clone());
    let resp = app
        .oneshot(post_events(json!([{"event": "x"}]), Some(KEY), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Server key: append-only but trusted — source='server' (the default)
    // survives.
    let app = pulse::build_router(h.state.clone());
    let resp = app
        .oneshot(post_events(
            json!([{"event": "payment_completed", "user_id": "u1"}]),
            Some(SERVER_KEY),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    let rows = h
        .state
        .engine
        .query("SELECT event, source FROM events", 10)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["source"], "server");

    // Server key still can't read or erase.
    let app = pulse::build_router(h.state.clone());
    let resp = app
        .oneshot(
            Request::post("/mcp")
                .header(header::AUTHORIZATION, format!("Bearer {SERVER_KEY}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn mcp_accepts_public_host_header() {
    // Behind a reverse proxy the Host header is the public domain; rmcp's
    // loopback-only default must be disabled (empty allowed_hosts).
    let h = harness();
    let app = pulse::build_router(h.state.clone());
    let resp = app
        .oneshot(
            Request::post("/mcp")
                .header(header::HOST, "events.example.com")
                .header(header::AUTHORIZATION, format!("Bearer {KEY}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::ACCEPT, "application/json, text/event-stream")
                .body(Body::from(
                    r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}
