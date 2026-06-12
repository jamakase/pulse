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

use crate::funnel;
use crate::query::{COLUMNS, MAX_ROWS, QueryEngine, sql_quote};

const SCHEMA_DOC: &str = "Table `events` — one row per analytics event. Columns (all Utf8): \
product (which app sent it), event (snake_case name), occurred_at / received_at (RFC3339 UTC, \
lexicographically sortable; cast via occurred_at::timestamp for date math), anonymous_id, \
user_id, session_id, source ('client'|'server'), properties (JSON string), context (JSON \
string: utm, url, referrer), date (YYYY-MM-DD partition key — filter on it for fast scans). \
There is also a view `identity_links(product, anonymous_id, user_id, linked_at)` built from \
`$identify` events — join through it to stitch pre-signup anonymous activity to users.";

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
pub struct FunnelParams {
    /// Product to compute the funnel for.
    pub product: String,
    /// Ordered, distinct event names (2..=10) forming the funnel.
    pub steps: Vec<String>,
    /// Conversion window: e.g. "7d", "24h", "90m". Default "7d".
    pub window: Option<String>,
    /// Only events at/after this RFC3339 instant or YYYY-MM-DD date.
    pub since: Option<String>,
    /// Only events strictly before this instant/date.
    pub until: Option<String>,
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
            // For a known user also pull their pre-signup anonymous events
            // via identity_links.
            (Some(u), _) if !u.is_empty() => format!(
                "(user_id = {0} OR anonymous_id IN (SELECT anonymous_id FROM identity_links \
                 WHERE user_id = {0} AND product = {1}))",
                sql_quote(u),
                sql_quote(&p.product)
            ),
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

    #[tool(
        name = "funnel",
        description = "Ordered conversion funnel computed natively in Rust (ClickHouse \
        windowFunnel semantics): unique users reaching each step in order within the window, \
        counted via identity-aware user_id. steps = 2..10 distinct event names in funnel order; \
        window like '7d'/'24h'/'90m' (default '7d'); since/until are RFC3339 or YYYY-MM-DD."
    )]
    async fn funnel(
        &self,
        Parameters(p): Parameters<FunnelParams>,
    ) -> Result<CallToolResult, McpError> {
        let window = funnel::parse_window(p.window.as_deref().unwrap_or("7d"))
            .map_err(|e| McpError::invalid_params(e.to_string(), None))?;
        let counts = funnel::compute(
            &self.engine,
            &p.product,
            &p.steps,
            window,
            p.since.as_deref(),
            p.until.as_deref(),
        )
        .await
        .map_err(|e| McpError::invalid_params(format!("funnel failed: {e}"), None))?;

        let entered = counts.first().copied().unwrap_or(0);
        let steps: Vec<serde_json::Value> = p
            .steps
            .iter()
            .zip(&counts)
            .enumerate()
            .map(|(i, (event, &users))| {
                let prev = if i == 0 { entered } else { counts[i - 1] };
                json!({
                    "step": i + 1,
                    "event": event,
                    "users": users,
                    "conversion_from_first": ratio(users, entered),
                    "conversion_from_prev": ratio(users, prev),
                })
            })
            .collect();
        text_result(json!({
            "product": p.product,
            "window": p.window.as_deref().unwrap_or("7d"),
            "entered": entered,
            "steps": steps,
        }))
    }
}

fn ratio(num: u64, den: u64) -> serde_json::Value {
    if den == 0 {
        serde_json::Value::Null
    } else {
        json!((num as f64 / den as f64 * 1000.0).round() / 1000.0)
    }
}

fn text_result(v: serde_json::Value) -> Result<CallToolResult, McpError> {
    let s = serde_json::to_string(&v)
        .map_err(|e| McpError::internal_error(format!("serialize failed: {e}"), None))?;
    Ok(CallToolResult::success(vec![Content::text(s)]))
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
             Start with get_schema to discover products and events; use funnel \
             for ordered conversion funnels, query_events for ad-hoc SQL, and \
             user_timeline to debug a single user's journey."
                .to_string(),
        );
        info
    }
}

pub fn service(
    engine: Arc<QueryEngine>,
    config: &crate::config::Config,
) -> StreamableHttpService<PulseMcp, LocalSessionManager> {
    // rmcp's default Host validation only admits loopback (DNS-rebinding
    // guard for local servers). pulse is bearer-authenticated behind a
    // reverse proxy, so we accept any Host unless PULSE_ALLOWED_HOSTS pins it.
    let http_config = if config.allowed_hosts.is_empty() {
        StreamableHttpServerConfig::default().disable_allowed_hosts()
    } else {
        StreamableHttpServerConfig::default().with_allowed_hosts(config.allowed_hosts.clone())
    };
    StreamableHttpService::new(
        move || Ok(PulseMcp::new(engine.clone())),
        LocalSessionManager::default().into(),
        http_config,
    )
}

#[cfg(test)]
mod tests {
    use crate::query::sql_quote;

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
