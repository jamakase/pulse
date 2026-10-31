use axum::{
    extract::{Request, State},
    http::{HeaderValue, Method, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use subtle::ConstantTimeEq;
use tower_http::cors::{AllowOrigin, CorsLayer};

use crate::AppState;

/// Origin allowlist (PostHog model): browser-originated requests must come
/// from a known Origin; server-to-server requests carry no Origin and pass.
/// Applied to everything behind the API (ingest, MCP, erasure).
pub async fn check_origin(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    if let Some(origin) = req.headers().get(header::ORIGIN) {
        let origin = origin.to_str().unwrap_or("").to_lowercase();
        if !state.config.allowed_origins.contains(&origin) {
            return Err(StatusCode::FORBIDDEN);
        }
    }
    Ok(next.run(req).await)
}

/// Admin surface (MCP, erasure): the operator-only `PULSE_ADMIN_KEY`.
/// Product keys — even the secret server ones — get 401 here.
pub async fn require_admin(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let provided = bearer(req.headers());
    if !key_matches(provided, &state.config.admin_key) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(next.run(req).await)
}

/// MCP surface: the admin key, or a live OAuth access token (claude.ai /
/// ChatGPT connectors). A 401 carries the RFC 9728 `resource_metadata`
/// pointer that starts the connector's OAuth flow.
pub async fn require_mcp(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let provided = bearer(req.headers());
    let ok = key_matches(provided, &state.config.admin_key)
        || state
            .oauth
            .as_deref()
            .is_some_and(|o| !provided.is_empty() && o.verify_access(provided));
    if ok {
        return next.run(req).await;
    }
    let challenge = crate::oauth::challenge(&state, req.headers());
    let mut resp = StatusCode::UNAUTHORIZED.into_response();
    resp.headers_mut()
        .insert(header::WWW_AUTHENTICATE, challenge);
    resp
}

pub fn bearer(headers: &axum::http::HeaderMap) -> &str {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("")
}

pub fn key_matches(provided: &str, expected: &str) -> bool {
    let (p, e) = (provided.as_bytes(), expected.as_bytes());
    // Length is the only thing leaked; comparison itself is constant-time.
    p.len() == e.len() && bool::from(p.ct_eq(e))
}

pub fn cors_layer(config: &crate::config::Config) -> CorsLayer {
    let origins: Vec<HeaderValue> = config
        .allowed_origins
        .iter()
        .filter_map(|o| o.parse().ok())
        .collect();
    CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_methods([Method::POST, Method::GET, Method::DELETE])
        .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION])
}

#[cfg(test)]
mod tests {
    use super::key_matches;

    #[test]
    fn key_comparison() {
        assert!(key_matches("secret-key-12345", "secret-key-12345"));
        assert!(!key_matches("secret-key-12346", "secret-key-12345"));
        assert!(!key_matches("", "secret-key-12345"));
        assert!(!key_matches("short", "secret-key-12345"));
    }
}
