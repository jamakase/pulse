pub mod auth;
pub mod compactor;
pub mod config;
pub mod erase;
pub mod event;
pub mod funnel;
pub mod ingest;
pub mod mcp;
pub mod oauth;
pub mod overview;
pub mod query;
pub mod ui;
pub mod wal;

use std::sync::Arc;

use axum::{
    Json, Router,
    routing::{get, post},
};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<config::Config>,
    pub wal: Arc<wal::Wal>,
    pub engine: Arc<query::QueryEngine>,
    pub compaction_lock: Arc<tokio::sync::RwLock<()>>,
    /// Built-in OAuth server; `None` unless PULSE_PUBLIC_URL is set.
    pub oauth: Option<Arc<oauth::OAuth>>,
}

/// Full application router: /health is public, /v1/events and /mcp sit behind
/// bearer auth + origin allowlist.
pub fn build_router(state: AppState) -> Router {
    let mcp_service = mcp::service(state.engine.clone(), &state.config);

    // Erasure: the operator-only admin key, nothing else.
    let admin = Router::new()
        .route(
            "/v1/users/{user_id}",
            axum::routing::delete(erase::erase_user),
        )
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::require_admin,
        ));
    // MCP: the admin key, or an OAuth access token minted with it.
    let mcp = Router::new().nest_service("/mcp", mcp_service).layer(
        axum::middleware::from_fn_with_state(state.clone(), auth::require_mcp),
    );

    // Ingest authenticates inside the handler: public per-product write keys
    // (browser-safe, gated by the origin allowlist) or the admin key.
    let protected = Router::new()
        .route("/v1/events", post(ingest::ingest))
        .merge(admin)
        .merge(mcp)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::check_origin,
        ))
        .layer(auth::cors_layer(&state.config));

    // OAuth endpoints sit outside the origin check: the consent form posts
    // from pulse's own origin, and discovery/token calls come from the
    // connector's backend. They 404 unless PULSE_PUBLIC_URL is set.
    let oauth = Router::new()
        .route(
            "/.well-known/oauth-protected-resource",
            get(oauth::protected_resource),
        )
        .route(
            "/.well-known/oauth-protected-resource/mcp",
            get(oauth::protected_resource),
        )
        .route(
            "/.well-known/oauth-authorization-server",
            get(oauth::authorization_server),
        )
        .route("/oauth/register", post(oauth::register))
        .route(
            "/oauth/authorize",
            get(oauth::authorize_page).post(oauth::authorize_submit),
        )
        .route("/oauth/token", post(oauth::token));

    Router::new()
        .route("/health", get(health))
        .merge(oauth)
        .merge(protected)
        .with_state(state)
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "service": "pulse",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}
