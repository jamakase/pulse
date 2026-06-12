use axum::{
    extract::{Request, State},
    http::{HeaderValue, Method, StatusCode, header},
    middleware::Next,
    response::Response,
};
use subtle::ConstantTimeEq;
use tower_http::cors::{AllowOrigin, CorsLayer};

use crate::AppState;

/// Bearer-key auth + Origin allowlist (PostHog model: the key is shared, but
/// browser-originated requests must come from an allowed Origin).
pub async fn require_auth(
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

    let provided = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    if !key_matches(provided, &state.config.api_key) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    Ok(next.run(req).await)
}

fn key_matches(provided: &str, expected: &str) -> bool {
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
