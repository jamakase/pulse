pub mod auth;
pub mod compactor;
pub mod config;
pub mod event;
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
}

/// Full application router: /health is public, /v1/events and /mcp sit behind
/// bearer auth + origin allowlist.
pub fn build_router(state: AppState) -> Router {
    let mcp_service = mcp::service(state.engine.clone());

    let protected = Router::new()
        .route("/v1/events", post(ingest::ingest))
        .nest_service("/mcp", mcp_service)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::require_auth,
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
