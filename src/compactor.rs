use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::*;
use tokio::sync::RwLock;

use crate::config::Config;
use crate::query;
use crate::wal::Wal;

pub async fn run_loop(config: Arc<Config>, wal: Arc<Wal>, lock: Arc<RwLock<()>>) {
    let mut interval = tokio::time::interval(Duration::from_secs(config.compact_interval_secs));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        match compact_once(&config, &wal, &lock).await {
            Ok(0) => {}
            Ok(n) => tracing::info!(sealed_files = n, "compacted WAL into Parquet"),
            Err(e) => tracing::error!(error = %e, "compaction failed"),
        }
        if let Err(e) = enforce_ttl(&config.events_dir(), config.ttl_days) {
            tracing::error!(error = %e, "TTL cleanup failed");
        }
    }
}

/// Seal the current WAL, rewrite all sealed files into Hive-partitioned
/// Parquet (product=…/date=…), then delete them. The write lock is held for
/// the parquet-write + delete window so queries never see an event in both
/// the WAL and Parquet at once.
pub async fn compact_once(config: &Config, wal: &Wal, lock: &RwLock<()>) -> anyhow::Result<usize> {
    let sealed = wal.rotate_and_list_sealed()?;
    if sealed.is_empty() {
        return Ok(0);
    }

    let ctx = SessionContext::new();
    let paths: Vec<String> = sealed
        .iter()
        .map(|p| p.to_string_lossy().into_owned())
        .collect();
    let schema = query::wal_schema();
    let df = ctx
        .read_json(
            paths,
            JsonReadOptions::default()
                .schema(&schema)
                .file_extension(".ndjson"),
        )
        .await?;

    let events_dir = config.events_dir();
    let _guard = lock.write().await;
    df.write_parquet(
        &events_dir.to_string_lossy(),
        DataFrameWriteOptions::new()
            .with_partition_by(vec!["product".to_string(), "date".to_string()]),
        None,
    )
    .await?;
    for path in &sealed {
        std::fs::remove_file(path)?;
    }
    Ok(sealed.len())
}

/// Drop date partitions older than the TTL. Partitions are immutable, so this
/// is just directory removal.
pub fn enforce_ttl(events_dir: &Path, ttl_days: i64) -> anyhow::Result<()> {
    if ttl_days <= 0 || !events_dir.exists() {
        return Ok(());
    }
    let cutoff = (chrono::Utc::now() - chrono::Duration::days(ttl_days))
        .format("%Y-%m-%d")
        .to_string();
    for product in std::fs::read_dir(events_dir)?.filter_map(|e| e.ok()) {
        if !product.path().is_dir() {
            continue;
        }
        for part in std::fs::read_dir(product.path())?.filter_map(|e| e.ok()) {
            let path = part.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if let Some(date) = name.strip_prefix("date=")
                && path.is_dir()
                && date < cutoff.as_str()
            {
                tracing::info!(partition = name, "dropping expired partition");
                std::fs::remove_dir_all(&path)?;
            }
        }
    }
    Ok(())
}
