use chrono::{Days, NaiveDate};
use serde_json::{Value, json};

use crate::query::{MAX_ROWS, QueryEngine, sql_quote, stitched_events};

/// One-call product dashboard: daily volume, unique users, top events and
/// traffic sources over the last `days` days (today inclusive, UTC), plus the
/// same totals for the preceding period so callers can show deltas.
/// `product = None` picks the product with the most events.
pub async fn dashboard(
    engine: &QueryEngine,
    product: Option<&str>,
    days: u32,
    today: NaiveDate,
) -> anyhow::Result<Value> {
    anyhow::ensure!((1..=365).contains(&days), "days must be 1..=365");

    let products = engine
        .query(
            "SELECT product, count(*) AS events, max(occurred_at) AS last_seen FROM events \
             GROUP BY product ORDER BY events DESC",
            MAX_ROWS,
        )
        .await?;
    let product = match product.filter(|p| !p.is_empty()) {
        Some(p) => p.to_string(),
        None => match products.first().and_then(|p| p["product"].as_str()) {
            Some(p) => p.to_string(),
            None => return Ok(json!({"view": "dashboard", "products": [], "product": null})),
        },
    };

    let since = today - Days::new(u64::from(days) - 1);
    let prev_since = since - Days::new(u64::from(days));
    // `uid` is identity-stitched, so a visitor who later signs up counts once.
    let (src, s, ps, t) = (
        stitched_events(&product),
        sql_quote(&since.to_string()),
        sql_quote(&prev_since.to_string()),
        sql_quote(&today.to_string()),
    );

    let daily_rows = engine
        .query(
            &format!(
                "SELECT date, count(*) AS events, count(DISTINCT uid) AS users FROM {src} ev \
                 WHERE date >= {s} AND date <= {t} GROUP BY date"
            ),
            MAX_ROWS,
        )
        .await?;
    let totals = engine
        .query(
            &format!(
                "SELECT CASE WHEN date >= {s} THEN 'current' ELSE 'previous' END AS period, \
                 count(*) AS events, count(DISTINCT uid) AS users, \
                 count(DISTINCT CASE WHEN session_id <> '' THEN session_id END) AS sessions \
                 FROM {src} ev WHERE date >= {ps} AND date <= {t} GROUP BY 1"
            ),
            10,
        )
        .await?;
    let top_events = engine
        .query(
            &format!(
                "SELECT event, count(*) AS events, count(DISTINCT uid) AS users FROM {src} ev \
                 WHERE date >= {s} AND date <= {t} AND event NOT LIKE '$%' \
                 GROUP BY event ORDER BY events DESC, event LIMIT 10"
            ),
            10,
        )
        .await?;
    // context is a compact JSON string written by serde_json, so the key/value
    // shape is stable enough for a regex — no JSON UDF needed. Internal `$`
    // events carry no attribution and would all land in '(direct)'.
    let top_sources = engine
        .query(
            &format!(
                "SELECT coalesce(regexp_match(context, '\"utm_source\":\"([^\"]*)\"')[1], \
                 '(direct)') AS source, count(DISTINCT uid) AS users FROM {src} ev \
                 WHERE date >= {s} AND date <= {t} AND event NOT LIKE '$%' \
                 GROUP BY 1 ORDER BY users DESC, source LIMIT 10"
            ),
            10,
        )
        .await?;

    // Zero-fill so charts get one point per day.
    let mut daily = Vec::with_capacity(days as usize);
    let mut day = since;
    while day <= today {
        let d = day.to_string();
        let row = daily_rows.iter().find(|r| r["date"] == d.as_str());
        daily.push(json!({
            "date": d,
            "events": row.map_or(0, |r| r["events"].as_u64().unwrap_or(0)),
            "users": row.map_or(0, |r| r["users"].as_u64().unwrap_or(0)),
        }));
        day = day + Days::new(1);
    }
    let dau_sum: u64 = daily.iter().map(|d| d["users"].as_u64().unwrap_or(0)).sum();

    let period = |name: &str| {
        let row = totals.iter().find(|r| r["period"] == name);
        let get = |k: &str| row.map_or(0, |r| r[k].as_u64().unwrap_or(0));
        json!({"events": get("events"), "users": get("users"), "sessions": get("sessions")})
    };
    let mut current = period("current");
    current["avg_dau"] = json!((dau_sum as f64 / f64::from(days) * 10.0).round() / 10.0);

    Ok(json!({
        "view": "dashboard",
        "product": product,
        "products": products,
        "days": days,
        "since": since.to_string(),
        "until": today.to_string(),
        "totals": current,
        "previous": period("previous"),
        "daily": daily,
        "top_events": top_events,
        "top_sources": top_sources,
    }))
}
