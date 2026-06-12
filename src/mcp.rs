use std::sync::Arc;

use rmcp::{
    ErrorData as McpError, ServerHandler,
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Content, Implementation, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};
use serde_json::json;

use crate::query::{COLUMNS, MAX_ROWS, QueryEngine};

const SCHEMA_DOC: &str = "Table `events` — one row per analytics event. Columns (all Utf8): \
product (which app sent it), event (snake_case name), occurred_at / received_at (RFC3339 UTC, \
lexicographically sortable; cast via occurred_at::timestamp for date math), anonymous_id, \
user_id, session_id, source ('client'|'server'), properties (JSON string), context (JSON \
string: utm, url, referrer), date (YYYY-MM-DD partition key — filter on it for fast scans).";

#[derive(Clone)]
pub struct PulseMcp {
    engine: Arc<QueryEngine>,
}

#[derive(serde::Deserialize, schemars::JsonSchema)]
pub struct QueryEventsParams {
    /// SQL SELECT (DataFusion dialect) against the `events` view.
    pub sql: String,
    /// Max rows to return (default 1000, hard cap 10000).
    pub limit: Option<usize>,
}

#[derive(serde::Deserialize, schemars::JsonSchema)]
pub struct UserTimelineParams {
    /// Product the user belongs to.
    pub product: String,
    /// Match against user_id; provide this or anonymous_id.
    pub user_id: Option<String>,
    /// Match against anonymous_id (pre-signup visitors).
    pub anonymous_id: Option<String>,
    /// Only events at/after this RFC3339 instant.
    pub since: Option<String>,
    /// Max events (default 200).
    pub limit: Option<usize>,
}

#[tool_router]
impl PulseMcp {
    pub fn new(engine: Arc<QueryEngine>) -> Self {
        Self { engine }
    }

    #[tool(
        name = "query_events",
        description = "Run a read-only SQL SELECT (DataFusion dialect) over the `events` table. \
        Use get_schema first to see products, event names and volumes. Times are RFC3339 UTC \
        strings; filter on `date` (YYYY-MM-DD) to prune partitions. properties/context are JSON \
        strings — filter with LIKE or parse the returned rows yourself."
    )]
    async fn query_events(
        &self,
        Parameters(p): Parameters<QueryEventsParams>,
    ) -> Result<CallToolResult, McpError> {
        let limit = p.limit.unwrap_or(1000).min(MAX_ROWS);
        let rows = self
            .engine
            .query(&p.sql, limit)
            .await
            .map_err(|e| McpError::invalid_params(format!("query failed: {e}"), None))?;
        text_result(json!({"row_count": rows.len(), "rows": rows}))
    }

    #[tool(
        name = "get_schema",
        description = "Discover what data exists: the `events` table schema plus a per \
        (product, event) summary with counts and first/last seen timestamps."
    )]
    async fn get_schema(&self) -> Result<CallToolResult, McpError> {
        let summary = self
            .engine
            .query(
                "SELECT product, event, count(*) AS events, min(occurred_at) AS first_seen, \
                 max(occurred_at) AS last_seen FROM events GROUP BY product, event \
                 ORDER BY product, events DESC",
                MAX_ROWS,
            )
            .await
            .map_err(|e| McpError::internal_error(format!("schema query failed: {e}"), None))?;
        text_result(json!({
            "table": "events",
            "columns": COLUMNS,
            "doc": SCHEMA_DOC,
            "events": summary,
        }))
    }

    #[tool(
        name = "user_timeline",
        description = "Chronological event timeline for a single user of a product — the \
        'what happened to this user' debugging view. Provide user_id or anonymous_id."
    )]
    async fn user_timeline(
        &self,
        Parameters(p): Parameters<UserTimelineParams>,
    ) -> Result<CallToolResult, McpError> {
        let id_filter = match (p.user_id.as_deref(), p.anonymous_id.as_deref()) {
            (Some(u), _) if !u.is_empty() => format!("user_id = {}", sql_quote(u)),
            (_, Some(a)) if !a.is_empty() => format!("anonymous_id = {}", sql_quote(a)),
            _ => {
                return Err(McpError::invalid_params(
                    "provide user_id or anonymous_id",
                    None,
                ));
            }
        };
        let mut sql = format!(
            "SELECT occurred_at, event, source, session_id, properties, context FROM events \
             WHERE product = {} AND {id_filter}",
            sql_quote(&p.product),
        );
        if let Some(since) = p.since.as_deref() {
            sql.push_str(&format!(" AND occurred_at >= {}", sql_quote(since)));
        }
        sql.push_str(" ORDER BY occurred_at");
        let rows = self
            .engine
            .query(&sql, p.limit.unwrap_or(200).min(MAX_ROWS))
            .await
            .map_err(|e| McpError::internal_error(format!("timeline failed: {e}"), None))?;
        text_result(json!({"row_count": rows.len(), "rows": rows}))
    }
}

fn text_result(v: serde_json::Value) -> Result<CallToolResult, McpError> {
    let s = serde_json::to_string(&v)
        .map_err(|e| McpError::internal_error(format!("serialize failed: {e}"), None))?;
    Ok(CallToolResult::success(vec![Content::text(s)]))
}

fn sql_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

#[tool_handler]
impl ServerHandler for PulseMcp {
    fn get_info(&self) -> ServerInfo {
        let mut server_info = Implementation::default();
        server_info.name = "pulse".to_string();
        server_info.version = env!("CARGO_PKG_VERSION").to_string();

        let mut info = ServerInfo::default();
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info.server_info = server_info;
        info.instructions = Some(
            "Pulse is a multi-product event analytics store queried via SQL. \
             Start with get_schema to discover products and events, then use \
             query_events for funnels/aggregations and user_timeline to debug \
             a single user's journey."
                .to_string(),
        );
        info
    }
}

pub fn service(engine: Arc<QueryEngine>) -> StreamableHttpService<PulseMcp, LocalSessionManager> {
    StreamableHttpService::new(
        move || Ok(PulseMcp::new(engine.clone())),
        LocalSessionManager::default().into(),
        StreamableHttpServerConfig::default(),
    )
}

#[cfg(test)]
mod tests {
    use super::sql_quote;

    #[test]
    fn quotes_sql_literals() {
        assert_eq!(sql_quote("plain"), "'plain'");
        assert_eq!(sql_quote("o'brien"), "'o''brien'");
        assert_eq!(
            sql_quote("'; DROP TABLE events; --"),
            "'''; DROP TABLE events; --'"
        );
    }
}
