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
        public_url: None,
        oauth_redirect_uris: vec![],
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
            oauth: None,
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

#[tokio::test]
async fn dashboard_summarizes_a_product() {
    let h = harness();
    let today = chrono::Utc::now().date_naive();
    let day = |n: u64| format!("{}T12:00:00Z", today - chrono::Days::new(n));
    let app = pulse::build_router(h.state.clone());
    let resp = app
        .oneshot(post_events(
            json!([
                {"event": "page_view", "anonymous_id": "a1", "occurred_at": day(0),
                 "context": {"utm_source": "telegram"}},
                {"event": "$identify", "anonymous_id": "a1", "user_id": "u1", "occurred_at": day(0)},
                {"event": "signup", "user_id": "u1", "session_id": "s1", "occurred_at": day(0),
                 "context": {"utm_source": "telegram"}},
                {"event": "page_view", "anonymous_id": "a9", "occurred_at": day(1)},
                {"event": "page_view", "user_id": "u2", "session_id": "s2", "occurred_at": day(2)},
                {"event": "page_view", "user_id": "u3", "occurred_at": day(9)},
                {"event": "page_view", "user_id": "u4", "occurred_at": day(40)},
            ]),
            Some(SERVER_KEY),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    let d = pulse::overview::dashboard(&h.state.engine, None, 7, today)
        .await
        .unwrap();
    assert_eq!(d["view"], "dashboard");
    assert_eq!(d["product"], "demo");
    assert_eq!(d["daily"].as_array().unwrap().len(), 7, "zero-filled days");
    assert_eq!(d["totals"]["events"], 5);
    // a1 was $identify-linked to u1 → one person; plus a9 and u2.
    assert_eq!(d["totals"]["users"], 3);
    assert_eq!(d["totals"]["sessions"], 2);
    assert_eq!(d["previous"]["events"], 1); // day 9 only; day 40 is out
    assert_eq!(d["top_events"][0]["event"], "page_view");
    assert!(
        d["top_events"]
            .as_array()
            .unwrap()
            .iter()
            .all(|e| e["event"] != "$identify"),
        "internal events are not 'top events'"
    );
    let sources = d["top_sources"].as_array().unwrap();
    assert_eq!(sources[0]["source"], "(direct)"); // a9, u2
    assert_eq!(sources[0]["users"], 2);
    assert!(
        sources
            .iter()
            .any(|s| s["source"] == "telegram" && s["users"] == 1)
    );
    let last = d["daily"].as_array().unwrap().last().unwrap().clone();
    assert_eq!(last["date"], today.to_string());
    assert_eq!(last["events"], 3);
    assert_eq!(last["users"], 1); // a1 + u1 stitched

    // Empty instance: no product to pick, no error.
    let empty = harness();
    let d = pulse::overview::dashboard(&empty.state.engine, None, 30, today)
        .await
        .unwrap();
    assert_eq!(d["product"], Value::Null);
}

/// One JSON-RPC exchange over streamable HTTP; returns (session id, result).
async fn mcp_call(
    app: &axum::Router,
    session: Option<&str>,
    body: Value,
) -> (Option<String>, Value) {
    let mut req = Request::post("/mcp")
        .header(header::HOST, "events.example.com")
        .header(header::AUTHORIZATION, format!("Bearer {KEY}"))
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "application/json, text/event-stream");
    if let Some(s) = session {
        req = req.header("mcp-session-id", s);
    }
    let resp = app
        .clone()
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    if !status.is_success() {
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        panic!("{} on {body}: {}", status, String::from_utf8_lossy(&bytes));
    }
    let sid = resp
        .headers()
        .get("mcp-session-id")
        .map(|v| v.to_str().unwrap().to_string());
    if body.get("id").is_none() {
        return (sid, Value::Null); // notification: 202, no body
    }
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    let msg = text
        .lines()
        .filter_map(|l| l.strip_prefix("data:"))
        .filter_map(|l| serde_json::from_str::<Value>(l.trim()).ok())
        .find(|m| m.get("id").is_some())
        .unwrap_or_else(|| panic!("no JSON-RPC response in {text}"));
    (sid, msg)
}

#[tokio::test]
async fn mcp_serves_the_app_view() {
    let h = harness();
    // One router = one MCP session manager, as in production.
    let app = pulse::build_router(h.state.clone());
    let (sid, init) = mcp_call(
        &app,
        None,
        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
            "protocolVersion":"2025-06-18","capabilities":{},
            "clientInfo":{"name":"t","version":"0"}}}),
    )
    .await;
    let sid = sid.expect("stateful session");
    let caps = &init["result"]["capabilities"];
    assert_eq!(
        caps["extensions"]["io.modelcontextprotocol/ui"]["mimeTypes"][0],
        "text/html;profile=mcp-app"
    );
    assert!(caps["resources"].is_object());
    mcp_call(
        &app,
        Some(&sid),
        json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
    )
    .await;

    let (_, tools) = mcp_call(
        &app,
        Some(&sid),
        json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
    )
    .await;
    let tools = tools["result"]["tools"].as_array().unwrap().clone();
    for name in ["dashboard", "funnel", "query_events", "user_timeline"] {
        let t = tools.iter().find(|t| t["name"] == name).unwrap();
        assert_eq!(
            t["_meta"]["ui"]["resourceUri"], "ui://pulse/app.html",
            "{name}"
        );
    }
    // Entry point for hosts that hide tools with required args (AgentArea /apps).
    let dash = tools.iter().find(|t| t["name"] == "dashboard").unwrap();
    assert!(
        dash["inputSchema"]["required"]
            .as_array()
            .is_none_or(|r| r.is_empty())
    );
    let schema = tools.iter().find(|t| t["name"] == "get_schema").unwrap();
    assert!(schema.get("_meta").is_none());

    let (_, res) = mcp_call(
        &app,
        Some(&sid),
        json!({"jsonrpc":"2.0","id":3,"method":"resources/read",
               "params":{"uri":"ui://pulse/app.html"}}),
    )
    .await;
    let content = &res["result"]["contents"][0];
    assert_eq!(content["mimeType"], "text/html;profile=mcp-app");
    assert!(content["text"].as_str().unwrap().contains("ui/initialize"));

    let (_, call) = mcp_call(
        &app,
        Some(&sid),
        json!({"jsonrpc":"2.0","id":4,"method":"tools/call",
               "params":{"name":"dashboard","arguments":{}}}),
    )
    .await;
    assert_eq!(call["result"]["structuredContent"]["view"], "dashboard");
    assert!(call["result"]["content"][0]["text"].is_string());

    // Execution failures are tool results with isError, not protocol errors:
    // the model can read them and a view gets something to render.
    let (_, bad) = mcp_call(
        &app,
        Some(&sid),
        json!({"jsonrpc":"2.0","id":5,"method":"tools/call",
               "params":{"name":"query_events","arguments":{"sql":"SELEC nope"}}}),
    )
    .await;
    assert!(bad.get("error").is_none(), "{bad}");
    assert_eq!(bad["result"]["isError"], true);
    assert!(
        bad["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .starts_with("query failed")
    );
}

#[tokio::test]
async fn funnel_stitches_anonymous_entry_steps() {
    // The natural top-of-funnel step (page_view) happens before signup, under
    // an anonymous_id. Without stitching such a funnel is always empty.
    let h = harness();
    let app = pulse::build_router(h.state.clone());
    let batch = json!([
        {"event":"page_view","anonymous_id":"a1","occurred_at":"2026-06-01T10:00:00Z"},
        {"event":"$identify","anonymous_id":"a1","user_id":"u1","occurred_at":"2026-06-01T10:05:00Z"},
        {"event":"signup","user_id":"u1","occurred_at":"2026-06-01T10:05:00Z"},
        {"event":"page_view","anonymous_id":"a2","occurred_at":"2026-06-01T11:00:00Z"}
    ]);
    let resp = app
        .oneshot(post_events(batch, Some(SERVER_KEY), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let counts = pulse::funnel::compute(
        &h.state.engine,
        "demo",
        &["page_view".to_string(), "signup".to_string()],
        std::time::Duration::from_secs(86400),
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(counts, vec![2, 1]); // a1→u1 converts, a2 bounces
}

#[tokio::test]
async fn funnel_orders_simultaneous_steps_deterministically() {
    // A backend often emits consecutive steps in one batch with the same
    // timestamp; they must count in funnel order, every time.
    let h = harness();
    let app = pulse::build_router(h.state.clone());
    let batch = json!([
        {"event":"estimate","user_id":"u1","occurred_at":"2026-06-01T10:00:00Z"},
        {"event":"signup","user_id":"u1","occurred_at":"2026-06-01T10:00:00Z"}
    ]);
    let resp = app
        .oneshot(post_events(batch, Some(SERVER_KEY), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let counts = pulse::funnel::compute(
        &h.state.engine,
        "demo",
        &["signup".to_string(), "estimate".to_string()],
        std::time::Duration::from_secs(3600),
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(counts, vec![1, 1]);
}

fn oauth_harness() -> Harness {
    let mut h = harness();
    let o = pulse::oauth::OAuth::new(
        "https://pulse.test",
        KEY,
        &[],
        h.state.config.data_dir.as_path(),
    )
    .unwrap();
    h.state.oauth = Some(Arc::new(o));
    h
}

async fn send(
    app: &axum::Router,
    req: Request<Body>,
) -> (StatusCode, axum::http::HeaderMap, String) {
    let resp = app.clone().oneshot(req).await.unwrap();
    let (status, headers) = (resp.status(), resp.headers().clone());
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, headers, String::from_utf8_lossy(&bytes).to_string())
}

fn form(uri: &str, pairs: &[(&str, &str)]) -> Request<Body> {
    let mut s = form_urlencoded::Serializer::new(String::new());
    for (k, v) in pairs {
        s.append_pair(k, v);
    }
    Request::post(uri)
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(Body::from(s.finish()))
        .unwrap()
}

#[tokio::test]
async fn oauth_flow_lets_a_connector_reach_mcp() {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64};
    use sha2::{Digest, Sha256};

    let h = oauth_harness();
    let app = pulse::build_router(h.state.clone());
    const CB: &str = "https://claude.ai/api/mcp/auth_callback";
    let init = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}"#;
    let mcp_req = |token: &str| {
        Request::post("/mcp")
            .header(header::HOST, "pulse.test")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ACCEPT, "application/json, text/event-stream")
            .body(Body::from(init))
            .unwrap()
    };

    // 1. Unauthenticated MCP → 401 pointing at the resource metadata.
    let (status, headers, _) = send(&app, mcp_req("")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        headers[header::WWW_AUTHENTICATE],
        r#"Bearer resource_metadata="https://pulse.test/.well-known/oauth-protected-resource""#
    );

    // 2. Discovery.
    let (_, _, prm) = send(
        &app,
        Request::get("/.well-known/oauth-protected-resource")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let prm: Value = serde_json::from_str(&prm).unwrap();
    assert_eq!(prm["resource"], "https://pulse.test/mcp");
    assert_eq!(prm["authorization_servers"][0], "https://pulse.test");
    let (_, _, asm) = send(
        &app,
        Request::get("/.well-known/oauth-authorization-server")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let asm: Value = serde_json::from_str(&asm).unwrap();
    assert_eq!(asm["code_challenge_methods_supported"][0], "S256");
    assert_eq!(
        asm["registration_endpoint"],
        "https://pulse.test/oauth/register"
    );

    // 3. Dynamic client registration; unknown redirect hosts are refused.
    let reg = |uris: Value| {
        Request::post("/oauth/register")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                json!({"redirect_uris": uris, "client_name": "Claude <b>"}).to_string(),
            ))
            .unwrap()
    };
    let (status, _, _) = send(&app, reg(json!(["https://evil.example/cb"]))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _, body) = send(&app, reg(json!([CB]))).await;
    assert_eq!(status, StatusCode::CREATED);
    let client_id = serde_json::from_str::<Value>(&body).unwrap()["client_id"]
        .as_str()
        .unwrap()
        .to_string();

    // 4. Consent page (client name escaped), then sign-in with the admin key.
    let verifier = "v".repeat(64);
    let challenge = B64.encode(Sha256::digest(verifier.as_bytes()));
    let mut q = form_urlencoded::Serializer::new(String::new());
    let params = [
        ("response_type", "code"),
        ("client_id", client_id.as_str()),
        ("redirect_uri", CB),
        ("code_challenge", challenge.as_str()),
        ("code_challenge_method", "S256"),
        ("state", "xyz"),
        ("resource", "https://pulse.test/mcp"),
    ];
    for (k, v) in params {
        q.append_pair(k, v);
    }
    let (status, headers, page) = send(
        &app,
        Request::get(format!("/oauth/authorize?{}", q.finish()))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(page.contains("Claude &lt;b&gt;") && page.contains("claude.ai"));
    assert_eq!(headers[header::X_FRAME_OPTIONS], "DENY");
    // Chromium enforces form-action on the post-submit redirect.
    assert!(
        headers[header::CONTENT_SECURITY_POLICY]
            .to_str()
            .unwrap()
            .contains("form-action 'self' https://claude.ai;")
    );

    let mut wrong = params.to_vec();
    wrong.push(("admin_key", "not-the-key-0000000000"));
    let (status, _, page) = send(&app, form("/oauth/authorize", &wrong)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(page.contains("Wrong key"));

    let mut right = params.to_vec();
    right.push(("admin_key", KEY));
    let (status, headers, _) = send(&app, form("/oauth/authorize", &right)).await;
    assert!(status.is_redirection());
    let location = headers[header::LOCATION].to_str().unwrap().to_string();
    assert!(location.starts_with(&format!("{CB}?code=")));
    assert!(location.contains("state=xyz") && location.contains("iss=https%3A%2F%2Fpulse.test"));
    let code = form_urlencoded::parse(location.split_once('?').unwrap().1.as_bytes())
        .find(|(k, _)| k == "code")
        .unwrap()
        .1
        .to_string();

    // 5. Code exchange: PKCE enforced, codes single-use.
    let exchange = |verifier: &str| {
        form(
            "/oauth/token",
            &[
                ("grant_type", "authorization_code"),
                ("code", &code),
                ("redirect_uri", CB),
                ("client_id", &client_id),
                ("code_verifier", verifier),
            ],
        )
    };
    let (status, _, body) = send(&app, exchange(&"w".repeat(64))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("invalid_grant"));
    let (status, headers, body) = send(&app, exchange(&verifier)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    let tokens: Value = serde_json::from_str(&body).unwrap();
    let access = tokens["access_token"].as_str().unwrap().to_string();
    let refresh = tokens["refresh_token"].as_str().unwrap().to_string();

    // 6. The token opens MCP — and only MCP: erasure stays admin-key only.
    let (status, _, _) = send(&app, mcp_req(&access)).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, _) = send(
        &app,
        Request::delete("/v1/users/u1?product=demo")
            .header(header::AUTHORIZATION, format!("Bearer {access}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    // A refresh token is not an access token.
    let (status, headers, _) = send(&app, mcp_req(&refresh)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(
        headers[header::WWW_AUTHENTICATE]
            .to_str()
            .unwrap()
            .contains(r#"error="invalid_token""#)
    );

    // 7. Refresh rotates: the new pair works, the old refresh token is spent.
    let refresh_req = |t: &str| {
        form(
            "/oauth/token",
            &[
                ("grant_type", "refresh_token"),
                ("refresh_token", t),
                ("client_id", &client_id),
            ],
        )
    };
    let (status, _, body) = send(&app, refresh_req(&refresh)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rotated: Value = serde_json::from_str(&body).unwrap();
    assert_ne!(rotated["refresh_token"], tokens["refresh_token"]);
    let (status, _, _) = send(&app, mcp_req(rotated["access_token"].as_str().unwrap())).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, body) = send(&app, refresh_req(&refresh)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("invalid_grant"));
    // That replay means the refresh token leaked: the whole family is
    // revoked, including the pair the legitimate rotation just produced.
    let (status, _, _) = send(&app, mcp_req(rotated["access_token"].as_str().unwrap())).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _, _) = send(
        &app,
        refresh_req(rotated["refresh_token"].as_str().unwrap()),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Malformed token requests get OAuth-shaped errors, not framework 4xx text.
    let (status, _, body) = send(
        &app,
        Request::post("/oauth/token")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from("{}"))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("invalid_request"));

    // 8. A replayed code fails AND revokes what its first exchange issued.
    let (_, headers, _) = send(&app, form("/oauth/authorize", &right)).await;
    let location = headers[header::LOCATION].to_str().unwrap().to_string();
    let code2 = form_urlencoded::parse(location.split_once('?').unwrap().1.as_bytes())
        .find(|(k, _)| k == "code")
        .unwrap()
        .1
        .to_string();
    let exchange2 = || {
        form(
            "/oauth/token",
            &[
                ("grant_type", "authorization_code"),
                ("code", &code2),
                ("redirect_uri", CB),
                ("client_id", &client_id),
                ("code_verifier", &verifier),
            ],
        )
    };
    let (status, _, body) = send(&app, exchange2()).await;
    assert_eq!(status, StatusCode::OK);
    let access2 = serde_json::from_str::<Value>(&body).unwrap()["access_token"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, _, _) = send(&app, mcp_req(&access2)).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, _) = send(&app, exchange2()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "code replay must fail");
    let (status, _, _) = send(&app, mcp_req(&access2)).await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "replay revokes the family"
    );

    // The spent-list survives a restart.
    let reloaded = pulse::oauth::OAuth::new(
        "https://pulse.test",
        KEY,
        &[],
        h.state.config.data_dir.as_path(),
    )
    .unwrap();
    let mut state = h.state.clone();
    state.oauth = Some(Arc::new(reloaded));
    let app2 = pulse::build_router(state);
    let (status, _, _) = send(&app2, refresh_req(&refresh)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn oauth_is_off_without_public_url() {
    let h = harness();
    let app = pulse::build_router(h.state.clone());
    let (status, _, _) = send(
        &app,
        Request::get("/.well-known/oauth-protected-resource")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, headers, _) =
        send(&app, Request::post("/mcp").body(Body::from("{}")).unwrap()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(headers[header::WWW_AUTHENTICATE], "Bearer");
}
