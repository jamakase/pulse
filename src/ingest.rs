use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{AppState, auth, event};

pub const MAX_BATCH: usize = 500;

#[derive(Deserialize)]
#[serde(untagged)]
pub enum Batch {
    Wrapped { events: Vec<event::IncomingEvent> },
    Bare(Vec<event::IncomingEvent>),
}

#[derive(Deserialize)]
pub struct IngestQuery {
    /// Alternative to the Authorization header — sendBeacon can't set
    /// headers, so browsers pass the (public) write key as ?key=…
    pub key: Option<String>,
}

/// Admin key → trust `product` from the body. Write key → the product is
/// derived from the key; whatever the body claims is overwritten.
fn authenticate(
    state: &AppState,
    headers: &HeaderMap,
    query_key: Option<&str>,
) -> Result<Option<String>, StatusCode> {
    let provided = match auth::bearer(headers) {
        "" => query_key.unwrap_or(""),
        bearer => bearer,
    };
    if provided.is_empty() {
        return Err(StatusCode::UNAUTHORIZED);
    }
    if auth::key_matches(provided, &state.config.api_key) {
        return Ok(None);
    }
    for wk in &state.config.write_keys {
        if auth::key_matches(provided, &wk.key) {
            return Ok(Some(wk.product.clone()));
        }
    }
    Err(StatusCode::UNAUTHORIZED)
}

/// POST /v1/events — accepts a JSON array of events or {"events": [...]}.
/// Returns 202 only after the batch is fsynced into the WAL.
pub async fn ingest(
    State(state): State<AppState>,
    Query(query): Query<IngestQuery>,
    headers: HeaderMap,
    Json(batch): Json<Batch>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    let forced_product = authenticate(&state, &headers, query.key.as_deref())
        .map_err(|code| bad(code, "invalid or missing API key"))?;

    let mut events = match batch {
        Batch::Wrapped { events } => events,
        Batch::Bare(events) => events,
    };
    if let Some(product) = &forced_product {
        for ev in &mut events {
            ev.product = Some(product.clone());
            // Write keys are public (they ship in browser JS), so nothing
            // arriving on one can be trusted as server-originated. Only the
            // private admin key may claim source='server' — which makes
            // `WHERE source = 'server'` an integrity guarantee in queries.
            ev.source = Some("client".to_string());
        }
    }
    if events.is_empty() {
        return Err(bad(StatusCode::BAD_REQUEST, "empty batch"));
    }
    if events.len() > MAX_BATCH {
        return Err(bad(
            StatusCode::PAYLOAD_TOO_LARGE,
            &format!("batch too large: max {MAX_BATCH} events"),
        ));
    }

    let now = Utc::now();
    let mut lines = Vec::with_capacity(events.len());
    let mut rejected = Vec::new();
    for (i, ev) in events.into_iter().enumerate() {
        match event::normalize(ev, now, &state.config.property_denylist) {
            Ok(stored) => match serde_json::to_string(&stored) {
                Ok(line) => lines.push(line),
                Err(e) => rejected.push(json!({"index": i, "reason": e.to_string()})),
            },
            Err(reason) => rejected.push(json!({"index": i, "reason": reason})),
        }
    }

    let accepted = lines.len();
    if accepted > 0 {
        let wal = state.wal.clone();
        tokio::task::spawn_blocking(move || wal.append(&lines))
            .await
            .map_err(|e| internal(&e.to_string()))?
            .map_err(|e| internal(&e.to_string()))?;
    }

    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"accepted": accepted, "rejected": rejected})),
    ))
}

fn bad(code: StatusCode, msg: &str) -> (StatusCode, Json<Value>) {
    (code, Json(json!({"error": msg})))
}

fn internal(msg: &str) -> (StatusCode, Json<Value>) {
    tracing::error!(error = msg, "ingest failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error": "internal error"})),
    )
}
