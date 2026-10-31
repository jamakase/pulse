//! MCP Apps (SEP-1865, `io.modelcontextprotocol/ui`): one self-contained
//! HTML view, compiled into the binary, that hosts like Claude, ChatGPT or
//! AgentArea render in a sandboxed iframe next to tool results. No CDN, no
//! build step — vanilla JS + inline SVG, so the CSP stays empty.

use rmcp::model::{
    AnnotateAble, ExtensionCapabilities, Meta, RawResource, ReadResourceResult, Resource,
    ResourceContents,
};
use serde_json::json;

pub const EXTENSION: &str = "io.modelcontextprotocol/ui";
pub const MIME_TYPE: &str = "text/html;profile=mcp-app";
pub const APP_URI: &str = "ui://pulse/app.html";
const APP_HTML: &str = include_str!("ui/app.html");

/// `_meta` for every tool the view can render. Visibility is spelled out
/// (it defaults to the same) because some hosts gate view-initiated
/// `tools/call` on an explicit `"app"`.
pub fn tool_meta() -> Meta {
    let mut meta = Meta::new();
    meta.0.insert(
        "ui".into(),
        json!({"resourceUri": APP_URI, "visibility": ["model", "app"]}),
    );
    // Flat key from the pre-stable draft; still read by older hosts.
    meta.0.insert("ui/resourceUri".into(), json!(APP_URI));
    meta
}

pub fn extensions() -> ExtensionCapabilities {
    let mut ext = ExtensionCapabilities::new();
    let settings = json!({"mimeTypes": [MIME_TYPE]});
    ext.insert(
        EXTENSION.into(),
        settings.as_object().cloned().unwrap_or_default(),
    );
    ext
}

pub fn resources() -> Vec<Resource> {
    let mut r = RawResource::new(APP_URI, "pulse");
    r.title = Some("pulse analytics".into());
    r.description = Some("Interactive dashboard, funnel, SQL chart and user timeline".into());
    r.mime_type = Some(MIME_TYPE.into());
    vec![r.no_annotation()]
}

pub fn read(uri: &str) -> Option<ReadResourceResult> {
    (uri == APP_URI).then(|| {
        let mut meta = Meta::new();
        meta.0.insert("ui".into(), json!({"prefersBorder": true}));
        ReadResourceResult::new(vec![ResourceContents::TextResourceContents {
            uri: APP_URI.into(),
            mime_type: Some(MIME_TYPE.into()),
            text: APP_HTML.into(),
            meta: Some(meta),
        }])
    })
}
