use std::path::Path;

use axum::{
    Json,
    extract::{Path as UrlPath, Query, State},
    http::StatusCode,
};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::*;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::RwLock;

use crate::{AppState, compactor};

#[derive(Deserialize)]
pub struct EraseParams {
    pub product: String,
}

/// DELETE /v1/users/{user_id}?product=… — GDPR Art. 17. Flushes the WAL into
/// Parquet, then rewrites every partition of the product without the user's
/// rows. Erases everything ingested before the call; events arriving
/// concurrently land after the sweep and would need a second call.
pub async fn erase_user(
    State(state): State<AppState>,
    UrlPath(user_id): UrlPath<String>,
    Query(params): Query<EraseParams>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if user_id.trim().is_empty() || params.product.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "user_id and product are required"})),
        ));
    }

    compactor::compact_once(&state.config, &state.wal, &state.compaction_lock)
        .await
        .map_err(internal)?;
    let deleted = erase_user_data(
        &state.config.events_dir(),
        &state.compaction_lock,
        &params.product,
        &user_id,
    )
    .await
    .map_err(internal)?;

    tracing::info!(product = %params.product, deleted, "user erasure completed");
    Ok(Json(json!({"deleted": deleted})))
}

fn internal(e: anyhow::Error) -> (StatusCode, Json<Value>) {
    tracing::error!(error = %e, "erasure failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error": "internal error"})),
    )
}

pub async fn erase_user_data(
    events_dir: &Path,
    lock: &RwLock<()>,
    product: &str,
    user_id: &str,
) -> anyhow::Result<u64> {
    let product_dir = events_dir.join(format!("product={product}"));
    if !product_dir.exists() {
        return Ok(0);
    }
    let _guard = lock.write().await;
    let mut deleted = 0u64;
    for entry in std::fs::read_dir(&product_dir)?.filter_map(|e| e.ok()) {
        let path = entry.path();
        let is_partition = path.is_dir()
            && path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("date="));
        if is_partition {
            deleted += rewrite_partition(&path, user_id).await?;
        }
    }
    Ok(deleted)
}

/// Partitions are immutable, so erasure = read → filter → write aside → swap.
async fn rewrite_partition(dir: &Path, user_id: &str) -> anyhow::Result<u64> {
    let ctx = SessionContext::new();
    let df = ctx
        .read_parquet(
            dir.to_string_lossy().as_ref(),
            ParquetReadOptions::default(),
        )
        .await?;
    let total = df.clone().count().await? as u64;
    let kept_df = df.filter(col("user_id").not_eq(lit(user_id)))?;
    let kept = kept_df.clone().count().await? as u64;
    if kept == total {
        return Ok(0);
    }

    let tmp = dir.with_extension("rewrite");
    if tmp.exists() {
        std::fs::remove_dir_all(&tmp)?;
    }
    if kept > 0 {
        std::fs::create_dir_all(&tmp)?;
        kept_df
            .write_parquet(
                tmp.to_string_lossy().as_ref(),
                DataFrameWriteOptions::new(),
                None,
            )
            .await?;
    }

    for f in parquet_files(dir)? {
        std::fs::remove_file(f)?;
    }
    if kept > 0 {
        for f in parquet_files(&tmp)? {
            let name = f.file_name().expect("file has a name");
            std::fs::rename(&f, dir.join(name))?;
        }
        std::fs::remove_dir_all(&tmp)?;
    } else {
        std::fs::remove_dir_all(dir)?;
    }
    Ok(total - kept)
}

fn parquet_files(dir: &Path) -> anyhow::Result<Vec<std::path::PathBuf>> {
    Ok(std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("parquet"))
        .collect())
}
