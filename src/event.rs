use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Deserialize)]
pub struct IncomingEvent {
    pub product: Option<String>,
    pub event: Option<String>,
    /// RFC3339; defaults to server time when omitted.
    pub occurred_at: Option<String>,
    pub anonymous_id: Option<String>,
    pub user_id: Option<String>,
    pub session_id: Option<String>,
    /// "client" | "server"; defaults to "server".
    pub source: Option<String>,
    pub properties: Option<Map<String, Value>>,
    pub context: Option<Map<String, Value>>,
}

/// One WAL line. All columns are strings: timestamps are RFC3339 UTC with
/// millisecond precision (lexicographically sortable), properties/context are
/// serialized JSON. `date` is the Hive partition key derived from occurred_at.
#[derive(Debug, Serialize)]
pub struct StoredEvent {
    pub product: String,
    pub event: String,
    pub occurred_at: String,
    pub received_at: String,
    pub anonymous_id: String,
    pub user_id: String,
    pub session_id: String,
    pub source: String,
    pub properties: String,
    pub context: String,
    pub date: String,
}

pub fn normalize(
    ev: IncomingEvent,
    now: DateTime<Utc>,
    denylist: &[String],
) -> Result<StoredEvent, String> {
    let product = ev.product.unwrap_or_default().trim().to_string();
    if product.is_empty()
        || product.len() > 64
        || !product
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err("invalid product: [a-zA-Z0-9_-]{1,64} required".to_string());
    }

    let event = ev.event.unwrap_or_default().trim().to_string();
    if event.is_empty() || event.len() > 200 {
        return Err("invalid event name: 1..200 chars required".to_string());
    }

    let occurred_at = match ev.occurred_at.as_deref() {
        Some(s) => DateTime::parse_from_rfc3339(s)
            .map_err(|_| "invalid occurred_at: RFC3339 required".to_string())?
            .with_timezone(&Utc),
        None => now,
    };

    let source = match ev.source.as_deref() {
        None | Some("server") => "server",
        Some("client") => "client",
        Some(other) => return Err(format!("invalid source '{other}': client|server")),
    };

    let properties = sanitize(ev.properties.unwrap_or_default(), denylist);
    let context = sanitize(ev.context.unwrap_or_default(), denylist);

    Ok(StoredEvent {
        date: occurred_at.format("%Y-%m-%d").to_string(),
        occurred_at: occurred_at.to_rfc3339_opts(SecondsFormat::Millis, true),
        received_at: now.to_rfc3339_opts(SecondsFormat::Millis, true),
        anonymous_id: ev.anonymous_id.unwrap_or_default(),
        user_id: ev.user_id.unwrap_or_default(),
        session_id: ev.session_id.unwrap_or_default(),
        source: source.to_string(),
        properties: Value::Object(properties).to_string(),
        context: Value::Object(context).to_string(),
        product,
        event,
    })
}

/// PII guard: drop denylisted keys (case-insensitive, top level).
fn sanitize(mut m: Map<String, Value>, denylist: &[String]) -> Map<String, Value> {
    m.retain(|k, _| !denylist.contains(&k.to_lowercase()));
    m
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn incoming(v: Value) -> IncomingEvent {
        serde_json::from_value(v).unwrap()
    }

    fn denylist() -> Vec<String> {
        vec!["email".to_string(), "phone".to_string()]
    }

    #[test]
    fn normalizes_minimal_event() {
        let now = Utc::now();
        let ev = incoming(json!({"product": "constractio", "event": "signup"}));
        let se = normalize(ev, now, &denylist()).unwrap();
        assert_eq!(se.product, "constractio");
        assert_eq!(se.event, "signup");
        assert_eq!(se.source, "server");
        assert_eq!(se.occurred_at, se.received_at);
        assert_eq!(se.properties, "{}");
        assert_eq!(se.date, now.format("%Y-%m-%d").to_string());
    }

    #[test]
    fn strips_denylisted_properties() {
        let ev = incoming(json!({
            "product": "constractio",
            "event": "signup",
            "properties": {"Email": "a@b.c", "plan": "pro"},
            "context": {"phone": "+7900", "url": "/x"}
        }));
        let se = normalize(ev, Utc::now(), &denylist()).unwrap();
        assert_eq!(se.properties, r#"{"plan":"pro"}"#);
        assert_eq!(se.context, r#"{"url":"/x"}"#);
    }

    #[test]
    fn rejects_bad_input() {
        let now = Utc::now();
        for (v, frag) in [
            (json!({"event": "x"}), "product"),
            (json!({"product": "a b", "event": "x"}), "product"),
            (json!({"product": "a", "event": ""}), "event"),
            (
                json!({"product": "a", "event": "x", "occurred_at": "yesterday"}),
                "occurred_at",
            ),
            (
                json!({"product": "a", "event": "x", "source": "satellite"}),
                "source",
            ),
        ] {
            let err = normalize(incoming(v), now, &denylist()).unwrap_err();
            assert!(err.contains(frag), "expected '{frag}' in '{err}'");
        }
    }

    #[test]
    fn respects_occurred_at_for_partition_date() {
        let ev = incoming(json!({
            "product": "a", "event": "x",
            "occurred_at": "2026-01-15T10:30:00+03:00"
        }));
        let se = normalize(ev, Utc::now(), &denylist()).unwrap();
        assert_eq!(se.occurred_at, "2026-01-15T07:30:00.000Z");
        assert_eq!(se.date, "2026-01-15");
    }
}
