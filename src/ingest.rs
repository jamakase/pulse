use axum::{Json, extract::State, http::StatusCode};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{AppState, event};

pub const MAX_BATCH: usize = 500;

#[derive(Deserialize)]
#[serde(untagged)]
pub enum Batch {
    Wrapped { events: Vec<event::IncomingEvent> },
    Bare(Vec<event::IncomingEvent>),
}

/// POST /v1/events — accepts a JSON array of events or {"events": [...]}.
/// Returns 202 only after the batch is fsynced into the WAL.
pub async fn ingest(
    State(state): State<AppState>,
    Json(batch): Json<Batch>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    let events = match batch {
        Batch::Wrapped { events } => events,
        Batch::Bare(events) => events,
    };
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
