use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::json::ArrayWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::execution::context::SQLOptions;
use datafusion::prelude::*;
use tokio::sync::RwLock;

pub const QUERY_TIMEOUT: Duration = Duration::from_secs(30);
pub const MAX_ROWS: usize = 10_000;

pub const COLUMNS: [&str; 11] = [
    "product",
    "event",
    "occurred_at",
    "received_at",
    "anonymous_id",
    "user_id",
    "session_id",
    "source",
    "properties",
    "context",
    "date",
];

fn data_fields() -> Vec<Field> {
    [
        "event",
        "occurred_at",
        "received_at",
        "anonymous_id",
        "user_id",
        "session_id",
        "source",
        "properties",
        "context",
    ]
    .iter()
    .map(|n| Field::new(*n, DataType::Utf8, true))
    .collect()
}

/// Schema of Parquet data files (partition columns live in the dir layout).
pub fn parquet_file_schema() -> SchemaRef {
    Arc::new(Schema::new(data_fields()))
}

/// Schema of WAL NDJSON lines (all 11 columns inline).
pub fn wal_schema() -> SchemaRef {
    let mut fields = vec![Field::new("product", DataType::Utf8, true)];
    fields.extend(data_fields());
    fields.push(Field::new("date", DataType::Utf8, true));
    Arc::new(Schema::new(fields))
}

/// Read-only SQL over Parquet partitions + the live WAL tail, exposed as a
/// single `events` view. A fresh SessionContext per query keeps the catalog
/// in sync with whatever files exist right now — negligible overhead at our
/// query rates.
pub struct QueryEngine {
    events_dir: PathBuf,
    wal_dir: PathBuf,
    /// Held for read during queries; the compactor takes write while swapping
    /// WAL files for Parquet so a query never sees the same event twice.
    compaction_lock: Arc<RwLock<()>>,
}

impl QueryEngine {
    pub fn new(events_dir: PathBuf, wal_dir: PathBuf, compaction_lock: Arc<RwLock<()>>) -> Self {
        Self {
            events_dir,
            wal_dir,
            compaction_lock,
        }
    }

    pub async fn query(&self, sql: &str, limit: usize) -> anyhow::Result<Vec<serde_json::Value>> {
        let _guard = self.compaction_lock.read().await;
        let ctx = self.build_ctx().await?;
        let opts = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let df = ctx.sql_with_options(sql, opts).await?;
        let df = df.limit(0, Some(limit.min(MAX_ROWS)))?;
        let batches = tokio::time::timeout(QUERY_TIMEOUT, df.collect())
            .await
            .map_err(|_| anyhow::anyhow!("query timed out after {QUERY_TIMEOUT:?}"))??;
        batches_to_json(&batches)
    }

    async fn build_ctx(&self) -> anyhow::Result<SessionContext> {
        let ctx = SessionContext::new();
        let mut selects = Vec::new();
        let cols = COLUMNS.join(", ");

        if has_files_with_ext(&self.events_dir, "parquet") {
            let opts = ListingOptions::new(Arc::new(ParquetFormat::default()))
                .with_file_extension(".parquet")
                .with_table_partition_cols(vec![
                    ("product".to_string(), DataType::Utf8),
                    ("date".to_string(), DataType::Utf8),
                ]);
            ctx.register_listing_table(
                "events_parquet",
                dir_url(&self.events_dir),
                opts,
                Some(parquet_file_schema()),
                None,
            )
            .await?;
            selects.push(format!("SELECT {cols} FROM events_parquet"));
        }

        if has_files_with_ext(&self.wal_dir, "ndjson") {
            let opts =
                ListingOptions::new(Arc::new(JsonFormat::default())).with_file_extension(".ndjson");
            ctx.register_listing_table(
                "events_wal",
                dir_url(&self.wal_dir),
                opts,
                Some(wal_schema()),
                None,
            )
            .await?;
            selects.push(format!("SELECT {cols} FROM events_wal"));
        }

        if selects.is_empty() {
            ctx.register_table("events", Arc::new(MemTable::try_new(wal_schema(), vec![])?))?;
        } else {
            ctx.sql(&format!(
                "CREATE VIEW events AS {}",
                selects.join(" UNION ALL ")
            ))
            .await?;
        }
        Ok(ctx)
    }
}

fn dir_url(path: &std::path::Path) -> String {
    format!("file://{}/", path.display())
}

/// Non-empty files only: the WAL's current.ndjson is recreated empty after
/// every rotation, and arrow's JSON reader has nothing to do with it anyway.
fn has_files_with_ext(dir: &std::path::Path, ext: &str) -> bool {
    fn walk(dir: &std::path::Path, ext: &str) -> bool {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return false;
        };
        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.is_dir() {
                if walk(&path, ext) {
                    return true;
                }
            } else if path.extension().and_then(|e| e.to_str()) == Some(ext)
                && entry.metadata().map(|m| m.len() > 0).unwrap_or(false)
            {
                return true;
            }
        }
        false
    }
    walk(dir, ext)
}

fn batches_to_json(batches: &[RecordBatch]) -> anyhow::Result<Vec<serde_json::Value>> {
    let mut writer = ArrayWriter::new(Vec::new());
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    let data = writer.into_inner();
    if data.is_empty() {
        return Ok(Vec::new());
    }
    Ok(serde_json::from_slice(&data)?)
}
