pub mod auth;
pub mod compactor;
pub mod config;
pub mod erase;
pub mod event;
pub mod funnel;
pub mod ingest;
pub mod mcp;
pub mod query;
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
}

/// Full application router: /health is public, /v1/events and /mcp sit behind
/// bearer auth + origin allowlist.
pub fn build_router(state: AppState) -> Router {
    let mcp_service = mcp::service(state.engine.clone(), &state.config);

    // Admin surface: private PULSE_API_KEY only.
    let admin = Router::new()
        .route(
            "/v1/users/{user_id}",
            axum::routing::delete(erase::erase_user),
        )
        .nest_service("/mcp", mcp_service)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::require_admin,
        ));

    // Ingest authenticates inside the handler: public per-product write keys
    // (browser-safe, gated by the origin allowlist) or the admin key.
    let protected = Router::new()
        .route("/v1/events", post(ingest::ingest))
        .merge(admin)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::check_origin,
        ))
        .layer(auth::cors_layer(&state.config));

    Router::new()
        .route("/health", get(health))
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
