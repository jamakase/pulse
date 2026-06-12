use std::collections::HashMap;
use std::time::Duration;

use datafusion::arrow::array::StringArray;

use crate::query::{QueryEngine, sql_quote};

/// Per-step unique user counts for an ordered funnel, ClickHouse
/// `windowFunnel` semantics: a user reaches step k if events matching steps
/// 1..k occur in order within `window` of the chain's first step. The DP
/// keeps the *latest* viable chain start per level, which maximizes the
/// remaining window — same trick ClickHouse uses.
pub async fn compute(
    engine: &QueryEngine,
    product: &str,
    steps: &[String],
    window: Duration,
    since: Option<&str>,
    until: Option<&str>,
) -> anyhow::Result<Vec<u64>> {
    anyhow::ensure!(
        (2..=10).contains(&steps.len()),
        "funnel needs 2..=10 steps, got {}",
        steps.len()
    );
    let mut step_idx: HashMap<&str, usize> = HashMap::new();
    for (i, s) in steps.iter().enumerate() {
        anyhow::ensure!(!s.trim().is_empty(), "step {} is empty", i + 1);
        anyhow::ensure!(
            step_idx.insert(s.as_str(), i).is_none(),
            "duplicate step '{s}': funnel steps must be distinct events"
        );
    }

    let in_list = steps
        .iter()
        .map(|s| sql_quote(s))
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = format!(
        "SELECT user_id, occurred_at, event FROM events \
         WHERE product = {} AND user_id <> '' AND event IN ({in_list})",
        sql_quote(product)
    );
    // RFC3339-UTC strings compare lexicographically, so bare dates like
    // '2026-06-01' work as prefixes too.
    if let Some(since) = since {
        sql.push_str(&format!(" AND occurred_at >= {}", sql_quote(since)));
    }
    if let Some(until) = until {
        sql.push_str(&format!(" AND occurred_at < {}", sql_quote(until)));
    }
    sql.push_str(" ORDER BY user_id, occurred_at");

    let batches = engine.collect(&sql).await?;
    let window_ms = i64::try_from(window.as_millis()).unwrap_or(i64::MAX);

    let step_idx = &step_idx;
    let rows = batches.iter().flat_map(|batch| {
        let users = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("user_id is Utf8");
        let times = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("occurred_at is Utf8");
        let events = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("event is Utf8");
        (0..batch.num_rows()).filter_map(move |i| {
            let ts = chrono::DateTime::parse_from_rfc3339(times.value(i))
                .ok()?
                .timestamp_millis();
            let step = *step_idx.get(events.value(i))?;
            Some((users.value(i).to_string(), ts, step))
        })
    });

    Ok(fold(rows, steps.len(), window_ms))
}

/// Rows MUST be sorted by (user, ts). Returns unique-user counts per level.
pub fn fold(
    rows: impl Iterator<Item = (String, i64, usize)>,
    n_steps: usize,
    window_ms: i64,
) -> Vec<u64> {
    let mut counts = vec![0u64; n_steps];
    let mut cur_user: Option<String> = None;
    let mut starts: Vec<Option<i64>> = vec![None; n_steps];
    let mut level = 0usize;

    for (user, ts, step) in rows {
        if cur_user.as_deref() != Some(user.as_str()) {
            count_user(&mut counts, level);
            cur_user = Some(user);
            starts.fill(None);
            level = 0;
        }
        if step == 0 {
            // Later start = more window headroom for the rest of the chain.
            starts[0] = Some(starts[0].map_or(ts, |s| s.max(ts)));
            level = level.max(1);
        } else if let Some(start) = starts[step - 1]
            && ts - start <= window_ms
        {
            starts[step] = Some(starts[step].map_or(start, |s| s.max(start)));
            level = level.max(step + 1);
        }
    }
    count_user(&mut counts, level);
    counts
}

fn count_user(counts: &mut [u64], level: usize) {
    for c in counts.iter_mut().take(level) {
        *c += 1;
    }
}

/// "7d" / "24h" / "90m" / "3600s" → Duration.
pub fn parse_window(s: &str) -> anyhow::Result<Duration> {
    let s = s.trim();
    let (num, unit) = s.split_at(s.len().saturating_sub(1));
    let n: u64 = num
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid window '{s}': use e.g. 7d, 24h, 90m, 3600s"))?;
    let secs = match unit {
        "d" => n * 86_400,
        "h" => n * 3_600,
        "m" => n * 60,
        "s" => n,
        _ => anyhow::bail!("invalid window unit '{unit}': use d/h/m/s"),
    };
    anyhow::ensure!(secs > 0, "window must be positive");
    Ok(Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(user: &str, ts: i64, step: usize) -> (String, i64, usize) {
        (user.to_string(), ts, step)
    }

    #[test]
    fn counts_levels_in_order() {
        // u1 completes all 3, u2 reaches 2, u3 only entered.
        let rows = vec![
            r("u1", 0, 0),
            r("u1", 10, 1),
            r("u1", 20, 2),
            r("u2", 0, 0),
            r("u2", 5, 1),
            r("u3", 0, 0),
        ];
        assert_eq!(fold(rows.into_iter(), 3, 1000), vec![3, 2, 1]);
    }

    #[test]
    fn out_of_order_steps_do_not_count() {
        // Step 2 before step 1: never advances past level 1.
        let rows = vec![r("u1", 0, 1), r("u1", 10, 0), r("u1", 5, 2)];
        assert_eq!(fold(rows.into_iter(), 3, 1000), vec![1, 0, 0]);
    }

    #[test]
    fn window_cuts_off_slow_chains() {
        let rows = vec![r("u1", 0, 0), r("u1", 2000, 1)];
        assert_eq!(fold(rows.into_iter(), 2, 1000), vec![1, 0]);
        let rows = vec![r("u1", 0, 0), r("u1", 1000, 1)];
        assert_eq!(fold(rows.into_iter(), 2, 1000), vec![1, 1]);
    }

    #[test]
    fn restart_uses_latest_entry() {
        // First entry too old, but the user re-enters and completes in time.
        let rows = vec![r("u1", 0, 0), r("u1", 5000, 0), r("u1", 5500, 1)];
        assert_eq!(fold(rows.into_iter(), 2, 1000), vec![1, 1]);
    }

    #[test]
    fn parses_windows() {
        assert_eq!(parse_window("7d").unwrap(), Duration::from_secs(604_800));
        assert_eq!(parse_window("24h").unwrap(), Duration::from_secs(86_400));
        assert_eq!(parse_window("90m").unwrap(), Duration::from_secs(5_400));
        assert!(parse_window("7w").is_err());
        assert!(parse_window("d").is_err());
    }
}
