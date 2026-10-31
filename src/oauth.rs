//! Built-in OAuth 2.1 authorization server, so claude.ai / ChatGPT custom
//! connectors (which don't take static bearer headers) can reach /mcp.
//!
//! Single-operator by design: "signing in" means pasting PULSE_ADMIN_KEY on
//! the consent page. Everything is stateless HMAC-signed tokens — client ids
//! (DCR), codes, access and refresh tokens — keyed off the admin key, so
//! rotating the key revokes every grant. The only state is a small spent-list
//! (persisted in the data dir) that makes codes and refresh tokens single-use
//! and revokes a whole token family when one of them is replayed — so this
//! is a single-node design.

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;

use axum::{
    Form, Json,
    extract::{
        Query, State,
        rejection::{FormRejection, JsonRejection},
    },
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{Html, IntoResponse, Redirect, Response},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64};
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::AppState;
use crate::auth::key_matches;

const CODE_TTL: i64 = 300;
const ACCESS_TTL: i64 = 3600;
const REFRESH_TTL: i64 = 30 * 86400;
const SCOPE: &str = "pulse";

/// Redirect targets accepted at registration without configuration: Claude's
/// hosted apps, ChatGPT's per-connector and legacy callbacks. Loopback
/// (Claude Code, local tools) is matched separately, port-agnostic.
const DEFAULT_REDIRECTS: [&str; 4] = [
    "https://claude.ai/api/mcp/auth_callback",
    "https://claude.com/api/mcp/auth_callback",
    "https://chatgpt.com/connector/oauth/*",
    "https://chatgpt.com/connector_platform_oauth_redirect",
];

type HmacSha256 = Hmac<Sha256>;

pub struct OAuth {
    /// Issuer, e.g. `https://events.example.com` (no trailing slash).
    issuer: String,
    key: Vec<u8>,
    /// Exact URIs, or prefixes when they end in `*`.
    redirects: Vec<String>,
    spent: Mutex<HashMap<String, i64>>,
    spent_path: PathBuf,
    /// Serializes failed admin-key attempts so parallel guesses still wait.
    fail_gate: tokio::sync::Mutex<()>,
}

/// Outcome of marking a one-time credential used.
#[derive(PartialEq)]
enum Spend {
    Fresh,
    Replayed,
    /// Could not persist — fail closed rather than allow a later replay.
    Failed,
}

#[derive(Serialize, Deserialize)]
struct ClientClaims {
    typ: String,
    redirect_uris: Vec<String>,
    name: String,
}

#[derive(Serialize, Deserialize)]
struct CodeClaims {
    typ: String,
    jti: String,
    exp: i64,
    client: String,
    redirect_uri: String,
    challenge: String,
}

#[derive(Serialize, Deserialize)]
struct TokenClaims {
    typ: String,
    jti: String,
    exp: i64,
    /// Fingerprint of the client the grant belongs to.
    client: String,
    /// Audience: the protected resource (`<issuer>/mcp`).
    aud: String,
    /// Token family: the code a chain of refreshes descends from. Replaying
    /// any spent member revokes the family.
    fam: String,
}

impl OAuth {
    pub fn new(
        public_url: &str,
        admin_key: &str,
        extra_redirects: &[String],
        data_dir: &std::path::Path,
    ) -> anyhow::Result<Self> {
        let issuer = public_url.trim_end_matches('/').to_string();
        anyhow::ensure!(
            issuer.starts_with("https://") || is_loopback_url(&issuer),
            "PULSE_PUBLIC_URL must be https:// (or http://localhost for development)"
        );
        let mut mac = HmacSha256::new_from_slice(admin_key.as_bytes())?;
        mac.update(b"pulse-oauth-v1");
        let key = mac.finalize().into_bytes().to_vec();

        let dir = data_dir.join("oauth");
        std::fs::create_dir_all(&dir)?;
        let spent_path = dir.join("spent");
        let now = now();
        let spent: HashMap<String, i64> = std::fs::read_to_string(&spent_path)
            .unwrap_or_default()
            .lines()
            .filter_map(|l| {
                let (exp, jti) = l.split_once(' ')?;
                Some((jti.to_string(), exp.parse().ok()?))
            })
            .filter(|(_, exp)| *exp > now)
            .collect();
        // Rewrite without expired entries.
        let compacted: String = spent.iter().map(|(j, e)| format!("{e} {j}\n")).collect();
        std::fs::write(&spent_path, compacted)?;

        let redirects = DEFAULT_REDIRECTS
            .iter()
            .map(|s| s.to_string())
            .chain(extra_redirects.iter().cloned())
            .collect();
        Ok(Self {
            issuer,
            key,
            redirects,
            spent: Mutex::new(spent),
            spent_path,
            fail_gate: tokio::sync::Mutex::new(()),
        })
    }

    pub fn resource(&self) -> String {
        format!("{}/mcp", self.issuer)
    }

    pub fn resource_metadata_url(&self) -> String {
        format!("{}/.well-known/oauth-protected-resource", self.issuer)
    }

    /// True for a live access token minted for this resource.
    pub fn verify_access(&self, token: &str) -> bool {
        self.verify::<TokenClaims>(token, "access")
            .is_some_and(|c| c.aud == self.resource() && !self.revoked(&c.fam))
    }

    fn sign<T: Serialize>(&self, claims: &T) -> String {
        let body = B64.encode(serde_json::to_vec(claims).expect("claims serialize"));
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("any key size");
        mac.update(body.as_bytes());
        format!("{body}.{}", B64.encode(mac.finalize().into_bytes()))
    }

    fn verify<T: DeserializeOwned + Typed>(&self, token: &str, typ: &str) -> Option<T> {
        let (body, sig) = token.split_once('.')?;
        let mut mac = HmacSha256::new_from_slice(&self.key).ok()?;
        mac.update(body.as_bytes());
        mac.verify_slice(&B64.decode(sig).ok()?).ok()?;
        let claims: T = serde_json::from_slice(&B64.decode(body).ok()?).ok()?;
        (claims.typ() == typ && claims.exp().is_none_or(|e| e > now())).then_some(claims)
    }

    /// Marks a one-time credential used.
    fn spend(&self, jti: &str, exp: i64) -> Spend {
        let mut spent = self.spent.lock().expect("spent lock");
        if spent.contains_key(jti) {
            return Spend::Replayed;
        }
        let line = format!("{exp} {jti}\n");
        let persisted = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.spent_path)
            .and_then(|mut f| f.write_all(line.as_bytes()));
        if let Err(e) = persisted {
            tracing::error!(error = %e, "cannot persist OAuth spent-list");
            return Spend::Failed;
        }
        spent.insert(jti.to_string(), exp);
        Spend::Fresh
    }

    /// Revokes a token family until its longest-lived member has expired.
    fn revoke(&self, fam: &str) {
        let _ = self.spend(&format!("fam:{fam}"), now() + REFRESH_TTL);
    }

    fn revoked(&self, fam: &str) -> bool {
        self.spent
            .lock()
            .expect("spent lock")
            .contains_key(&format!("fam:{fam}"))
    }

    fn redirect_allowed(&self, uri: &str) -> bool {
        is_loopback_url(uri)
            || self.redirects.iter().any(|r| match r.strip_suffix('*') {
                Some(prefix) => uri.starts_with(prefix) && uri.len() > prefix.len(),
                None => uri == r,
            })
    }

    fn issue_tokens(&self, client: &str, fam: &str) -> serde_json::Value {
        let now = now();
        let access = TokenClaims {
            typ: "access".into(),
            jti: random_id(),
            exp: now + ACCESS_TTL,
            client: client.into(),
            aud: self.resource(),
            fam: fam.into(),
        };
        let refresh = TokenClaims {
            typ: "refresh".into(),
            jti: random_id(),
            exp: now + REFRESH_TTL,
            client: client.into(),
            aud: self.resource(),
            fam: fam.into(),
        };
        json!({
            "access_token": self.sign(&access),
            "token_type": "Bearer",
            "expires_in": ACCESS_TTL,
            "refresh_token": self.sign(&refresh),
            "scope": SCOPE,
        })
    }
}

trait Typed {
    fn typ(&self) -> &str;
    fn exp(&self) -> Option<i64>;
}
impl Typed for ClientClaims {
    fn typ(&self) -> &str {
        &self.typ
    }
    fn exp(&self) -> Option<i64> {
        None
    }
}
impl Typed for CodeClaims {
    fn typ(&self) -> &str {
        &self.typ
    }
    fn exp(&self) -> Option<i64> {
        Some(self.exp)
    }
}
impl Typed for TokenClaims {
    fn typ(&self) -> &str {
        &self.typ
    }
    fn exp(&self) -> Option<i64> {
        Some(self.exp)
    }
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

fn random_id() -> String {
    let mut b = [0u8; 16];
    getrandom::fill(&mut b).expect("OS randomness");
    B64.encode(b)
}

/// Short stable fingerprint binding codes/tokens to a client id.
fn fingerprint(client_id: &str) -> String {
    B64.encode(&Sha256::digest(client_id.as_bytes())[..12])
}

/// `http://localhost[:port]/…` or `http://127.0.0.1[:port]/…` (RFC 8252).
fn is_loopback_url(uri: &str) -> bool {
    let Some(rest) = uri.strip_prefix("http://") else {
        return false;
    };
    let host_port = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host = match host_port.rsplit_once(':') {
        Some((h, port)) if port.chars().all(|c| c.is_ascii_digit()) => h,
        _ => host_port,
    };
    matches!(host, "localhost" | "127.0.0.1" | "[::1]")
}

/// Loopback redirects match ignoring the port (RFC 8252 §7.3); others exactly.
fn redirect_matches(registered: &str, requested: &str) -> bool {
    if registered == requested {
        return true;
    }
    if !(is_loopback_url(registered) && is_loopback_url(requested)) {
        return false;
    }
    let strip = |u: &str| {
        let rest = &u["http://".len()..];
        let (hp, path) = rest.split_at(rest.find('/').unwrap_or(rest.len()));
        let host = hp.rsplit_once(':').map_or(hp, |(h, _)| h).to_string();
        (host, path.to_string())
    };
    strip(registered) == strip(requested)
}

fn not_found() -> Response {
    StatusCode::NOT_FOUND.into_response()
}

fn no_store(mut resp: Response) -> Response {
    resp.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    resp
}

fn oauth_error(status: StatusCode, error: &str, description: &str) -> Response {
    no_store(
        (
            status,
            Json(json!({"error": error, "error_description": description})),
        )
            .into_response(),
    )
}

// ---------------------------------------------------------------- discovery

/// RFC 9728 protected resource metadata.
pub async fn protected_resource(State(state): State<AppState>) -> Response {
    let Some(o) = state.oauth.as_deref() else {
        return not_found();
    };
    Json(json!({
        "resource": o.resource(),
        "authorization_servers": [o.issuer],
        "bearer_methods_supported": ["header"],
        "scopes_supported": [SCOPE],
        "resource_name": "pulse analytics",
    }))
    .into_response()
}

/// RFC 8414 authorization server metadata.
pub async fn authorization_server(State(state): State<AppState>) -> Response {
    let Some(o) = state.oauth.as_deref() else {
        return not_found();
    };
    Json(json!({
        "issuer": o.issuer,
        "authorization_endpoint": format!("{}/oauth/authorize", o.issuer),
        "token_endpoint": format!("{}/oauth/token", o.issuer),
        "registration_endpoint": format!("{}/oauth/register", o.issuer),
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "code_challenge_methods_supported": ["S256"],
        "token_endpoint_auth_methods_supported": ["none"],
        "scopes_supported": [SCOPE, "offline_access"],
        "authorization_response_iss_parameter_supported": true,
    }))
    .into_response()
}

// ------------------------------------------------------------- registration

#[derive(Deserialize)]
pub struct RegisterRequest {
    #[serde(default)]
    redirect_uris: Vec<String>,
    client_name: Option<String>,
}

/// RFC 7591 dynamic client registration. The client id *is* the signed
/// registration, so nothing is stored.
pub async fn register(
    State(state): State<AppState>,
    req: Result<Json<RegisterRequest>, JsonRejection>,
) -> Response {
    let Some(o) = state.oauth.as_deref() else {
        return not_found();
    };
    let Ok(Json(req)) = req else {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_client_metadata",
            "expected a JSON registration request",
        );
    };
    if req.redirect_uris.is_empty() || req.redirect_uris.len() > 10 {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_redirect_uri",
            "1..10 redirect_uris required",
        );
    }
    if let Some(bad) = req.redirect_uris.iter().find(|u| !o.redirect_allowed(u)) {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_redirect_uri",
            &format!("redirect_uri not allowed: {bad} (see PULSE_OAUTH_REDIRECT_URIS)"),
        );
    }
    let name: String = req
        .client_name
        .unwrap_or_else(|| "MCP client".into())
        .chars()
        .take(80)
        .collect();
    let claims = ClientClaims {
        typ: "client".into(),
        redirect_uris: req.redirect_uris.clone(),
        name: name.clone(),
    };
    no_store(
        (
            StatusCode::CREATED,
            Json(json!({
                "client_id": o.sign(&claims),
                "client_id_issued_at": now(),
                "client_name": name,
                "redirect_uris": req.redirect_uris,
                "grant_types": ["authorization_code", "refresh_token"],
                "response_types": ["code"],
                "token_endpoint_auth_method": "none",
            })),
        )
            .into_response(),
    )
}

// ---------------------------------------------------------------- authorize

#[derive(Deserialize, Clone, Default)]
pub struct AuthorizeParams {
    #[serde(default)]
    response_type: String,
    #[serde(default)]
    client_id: String,
    #[serde(default)]
    redirect_uri: String,
    #[serde(default)]
    code_challenge: String,
    #[serde(default)]
    code_challenge_method: String,
    state: Option<String>,
    scope: Option<String>,
    resource: Option<String>,
    /// Only on the consent form POST.
    admin_key: Option<String>,
}

enum Checked {
    Ok(ClientClaims),
    /// Untrusted client/redirect: show an error page, never redirect.
    Page(String),
    /// Trusted redirect, bad request otherwise: bounce the error back.
    Bounce(&'static str, String),
}

fn check(o: &OAuth, p: &AuthorizeParams) -> Checked {
    let Some(client) = o.verify::<ClientClaims>(&p.client_id, "client") else {
        return Checked::Page("Unknown client. Re-add the connector to register again.".into());
    };
    if !client
        .redirect_uris
        .iter()
        .any(|r| redirect_matches(r, &p.redirect_uri))
    {
        return Checked::Page("redirect_uri does not match the registered client.".into());
    }
    if p.response_type != "code" {
        return Checked::Bounce(
            "unsupported_response_type",
            "only response_type=code".into(),
        );
    }
    if p.code_challenge.is_empty() || p.code_challenge_method != "S256" {
        return Checked::Bounce("invalid_request", "PKCE with S256 is required".into());
    }
    if let Some(res) = p.resource.as_deref()
        && res.trim_end_matches('/') != o.resource()
    {
        return Checked::Bounce("invalid_target", format!("unknown resource {res}"));
    }
    Checked::Ok(client)
}

fn bounce(o: &OAuth, p: &AuthorizeParams, params: &[(&str, &str)]) -> Response {
    let mut url = p.redirect_uri.clone();
    url.push(if url.contains('?') { '&' } else { '?' });
    let mut q = form_urlencoded::Serializer::new(String::new());
    for (k, v) in params {
        q.append_pair(k, v);
    }
    if let Some(s) = &p.state {
        q.append_pair("state", s);
    }
    q.append_pair("iss", &o.issuer);
    url.push_str(&q.finish());
    no_store(Redirect::to(&url).into_response())
}

pub async fn authorize_page(
    State(state): State<AppState>,
    Query(p): Query<AuthorizeParams>,
) -> Response {
    let Some(o) = state.oauth.as_deref() else {
        return not_found();
    };
    match check(o, &p) {
        Checked::Ok(client) => consent_page(&p, &client, None),
        Checked::Page(msg) => error_page(&msg),
        Checked::Bounce(err, desc) => {
            bounce(o, &p, &[("error", err), ("error_description", &desc)])
        }
    }
}

pub async fn authorize_submit(
    State(state): State<AppState>,
    Form(p): Form<AuthorizeParams>,
) -> Response {
    let Some(o) = state.oauth.as_deref() else {
        return not_found();
    };
    let client = match check(o, &p) {
        Checked::Ok(c) => c,
        Checked::Page(msg) => return error_page(&msg),
        Checked::Bounce(err, desc) => {
            return bounce(o, &p, &[("error", err), ("error_description", &desc)]);
        }
    };
    let provided = p.admin_key.as_deref().unwrap_or("").trim();
    if !key_matches(provided, &state.config.admin_key) {
        // Slow down guessing — globally, so parallel requests queue up.
        let _gate = o.fail_gate.lock().await;
        tokio::time::sleep(std::time::Duration::from_millis(800)).await;
        return consent_page(&p, &client, Some("Wrong key."));
    }
    let code = CodeClaims {
        typ: "code".into(),
        jti: random_id(),
        exp: now() + CODE_TTL,
        client: fingerprint(&p.client_id),
        redirect_uri: p.redirect_uri.clone(),
        challenge: p.code_challenge.clone(),
    };
    bounce(o, &p, &[("code", &o.sign(&code))])
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

/// `form_origin`: where the consent form's redirect lands. Chromium applies
/// `form-action` to redirects after a form submit, so it must be allowed.
fn page(body: &str, form_origin: Option<&str>) -> Response {
    let html = format!(
        r#"<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>pulse · connect</title>
<style>
:root{{color-scheme:light dark;--bg:light-dark(#f5f4f1,#161514);--card:light-dark(#fff,#1f1e1d);--text:light-dark(#1f1e1b,#ecebe7);--muted:light-dark(#6b6862,#a19e96);--border:light-dark(#e3e0d9,#3d3b37);--accent:light-dark(#4f5fd6,#8b97ff);--danger:light-dark(#b8382c,#ef7b6e)}}
*{{box-sizing:border-box}}body{{margin:0;min-height:100vh;display:grid;place-items:center;background:var(--bg);color:var(--text);font:14px/1.5 ui-sans-serif,system-ui,-apple-system,sans-serif;padding:16px}}
.card{{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:24px;width:100%;max-width:400px}}
h1{{font-size:17px;margin:0 0 6px}}p{{margin:0 0 12px;color:var(--muted)}}b{{color:var(--text)}}
label{{display:block;font-size:12.5px;margin:14px 0 6px;color:var(--muted)}}
input[type=password]{{width:100%;padding:9px 11px;border:1px solid var(--border);border-radius:8px;background:var(--bg);color:var(--text);font:inherit}}
button{{margin-top:14px;width:100%;padding:10px;border:0;border-radius:8px;background:var(--accent);color:light-dark(#fff,#14131a);font:inherit;font-weight:600;cursor:pointer}}
.err{{color:var(--danger);font-size:13px;margin-top:10px}}.warn{{color:var(--danger);font-size:12.5px}}code{{font-size:12.5px}}
</style></head><body><div class="card">{body}</div></body></html>"#
    );
    let mut resp = Html(html).into_response();
    let h = resp.headers_mut();
    h.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    let csp = format!(
        "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'{}; \
         frame-ancestors 'none'",
        form_origin.map(|o| format!(" {o}")).unwrap_or_default()
    );
    if let Ok(v) = HeaderValue::from_str(&csp) {
        h.insert(header::CONTENT_SECURITY_POLICY, v);
    }
    h.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    no_store(resp)
}

fn consent_page(p: &AuthorizeParams, client: &ClientClaims, error: Option<&str>) -> Response {
    let (scheme, rest) = p.redirect_uri.split_once("://").unwrap_or(("", ""));
    let host = rest.split(['/', '?', '#']).next().unwrap_or("");
    let origin = format!("{scheme}://{host}");
    // Anyone can start a flow from their own Claude/ChatGPT account and send
    // you the link, so the warning shows on every flow.
    let warning = if is_loopback_url(&p.redirect_uri) {
        "This returns to a program on your own computer. Continue only if you just started the connection from it yourself."
    } else {
        "Continue only if you just clicked Connect yourself. A link someone sent you would connect pulse to their account."
    };
    let hidden: String = [
        ("response_type", Some(p.response_type.as_str())),
        ("client_id", Some(p.client_id.as_str())),
        ("redirect_uri", Some(p.redirect_uri.as_str())),
        ("code_challenge", Some(p.code_challenge.as_str())),
        (
            "code_challenge_method",
            Some(p.code_challenge_method.as_str()),
        ),
        ("state", p.state.as_deref()),
        ("scope", p.scope.as_deref()),
        ("resource", p.resource.as_deref()),
    ]
    .iter()
    .filter_map(|(k, v)| {
        v.map(|v| format!(r#"<input type="hidden" name="{k}" value="{}">"#, esc(v)))
    })
    .collect();
    page(
        &format!(
            r#"<h1>Connect to pulse</h1>
<p>An app that calls itself <b>{name}</b> asks for read access to all analytics on this pulse instance. You'll be sent back to <b>{host}</b>.</p>
<p class="warn">{warning}</p>
<form method="post" action="/oauth/authorize">{hidden}
<label for="k">Admin key (PULSE_ADMIN_KEY)</label>
<input id="k" type="password" name="admin_key" autocomplete="current-password" required autofocus>
{err}<button type="submit">Allow access</button></form>"#,
            name = esc(&client.name),
            host = esc(host),
            err = error
                .map(|e| format!(r#"<div class="err">{}</div>"#, esc(e)))
                .unwrap_or_default(),
        ),
        Some(&origin),
    )
}

fn error_page(msg: &str) -> Response {
    let mut resp = page(&format!("<h1>Can't connect</h1><p>{}</p>", esc(msg)), None);
    *resp.status_mut() = StatusCode::BAD_REQUEST;
    resp
}

// -------------------------------------------------------------------- token

#[derive(Deserialize)]
pub struct TokenRequest {
    grant_type: String,
    code: Option<String>,
    redirect_uri: Option<String>,
    client_id: Option<String>,
    code_verifier: Option<String>,
    refresh_token: Option<String>,
}

pub async fn token(
    State(state): State<AppState>,
    req: Result<Form<TokenRequest>, FormRejection>,
) -> Response {
    let Some(o) = state.oauth.as_deref() else {
        return not_found();
    };
    let Ok(Form(req)) = req else {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "expected an application/x-www-form-urlencoded body with grant_type",
        );
    };
    let invalid_grant = |d: &str| oauth_error(StatusCode::BAD_REQUEST, "invalid_grant", d);
    match req.grant_type.as_str() {
        "authorization_code" => {
            let Some(code) = req
                .code
                .as_deref()
                .and_then(|c| o.verify::<CodeClaims>(c, "code"))
            else {
                return invalid_grant("code is invalid or expired");
            };
            let client_id = req.client_id.as_deref().unwrap_or("");
            if fingerprint(client_id) != code.client {
                return invalid_grant("code was issued to another client");
            }
            if req.redirect_uri.as_deref() != Some(code.redirect_uri.as_str()) {
                return invalid_grant("redirect_uri mismatch");
            }
            let verifier = req.code_verifier.as_deref().unwrap_or("");
            if !(43..=128).contains(&verifier.len())
                || B64.encode(Sha256::digest(verifier.as_bytes())) != code.challenge
            {
                return invalid_grant("PKCE verification failed");
            }
            // The code's id seeds the token family: a replayed code revokes
            // whatever the first exchange issued (RFC 9700 §4.5).
            match o.spend(&code.jti, code.exp) {
                Spend::Fresh => {}
                Spend::Replayed => {
                    o.revoke(&code.jti);
                    return invalid_grant("code already used");
                }
                Spend::Failed => return invalid_grant("try again"),
            }
            no_store(Json(o.issue_tokens(&code.client, &code.jti)).into_response())
        }
        "refresh_token" => {
            let Some(old) = req
                .refresh_token
                .as_deref()
                .and_then(|t| o.verify::<TokenClaims>(t, "refresh"))
            else {
                return invalid_grant("refresh_token is invalid or expired");
            };
            if let Some(cid) = req.client_id.as_deref()
                && fingerprint(cid) != old.client
            {
                return invalid_grant("refresh_token was issued to another client");
            }
            if o.revoked(&old.fam) {
                return invalid_grant("refresh_token was revoked");
            }
            // Rotation: each refresh token works once. A replay means it
            // leaked — revoke the whole family, attacker's copies included.
            match o.spend(&old.jti, old.exp) {
                Spend::Fresh => {}
                Spend::Replayed => {
                    o.revoke(&old.fam);
                    return invalid_grant("refresh_token already used");
                }
                Spend::Failed => return invalid_grant("try again"),
            }
            no_store(Json(o.issue_tokens(&old.client, &old.fam)).into_response())
        }
        _ => oauth_error(
            StatusCode::BAD_REQUEST,
            "unsupported_grant_type",
            "use authorization_code or refresh_token",
        ),
    }
}

/// `WWW-Authenticate` for a 401 on the protected resource.
pub fn challenge(state: &AppState, headers: &HeaderMap) -> HeaderValue {
    let had_token = !crate::auth::bearer(headers).is_empty();
    let value = match state.oauth.as_deref() {
        Some(o) if had_token => format!(
            r#"Bearer error="invalid_token", resource_metadata="{}""#,
            o.resource_metadata_url()
        ),
        Some(o) => format!(
            r#"Bearer resource_metadata="{}""#,
            o.resource_metadata_url()
        ),
        None => "Bearer".into(),
    };
    HeaderValue::from_str(&value).unwrap_or(HeaderValue::from_static("Bearer"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_detection_and_matching() {
        assert!(is_loopback_url("http://localhost:3118/callback"));
        assert!(is_loopback_url("http://127.0.0.1/callback"));
        assert!(!is_loopback_url("https://localhost/callback"));
        assert!(!is_loopback_url("http://localhost.evil.com/callback"));
        assert!(!is_loopback_url("http://127.0.0.1.nip.io/cb"));
        assert!(redirect_matches(
            "http://localhost/callback",
            "http://localhost:5555/callback"
        ));
        assert!(!redirect_matches(
            "http://localhost/callback",
            "http://localhost:5555/other"
        ));
        assert!(!redirect_matches(
            "https://claude.ai/api/mcp/auth_callback",
            "https://claude.ai/api/mcp/auth_callback/x"
        ));
    }

    #[test]
    fn tokens_are_typed_and_tamper_evident() {
        let dir = tempfile::tempdir().unwrap();
        let o = OAuth::new(
            "https://pulse.test",
            "admin-key-0123456789",
            &[],
            dir.path(),
        )
        .unwrap();
        let t = TokenClaims {
            typ: "access".into(),
            jti: "j".into(),
            exp: now() + 60,
            client: "c".into(),
            aud: o.resource(),
            fam: "f".into(),
        };
        let signed = o.sign(&t);
        assert!(o.verify_access(&signed));
        // A refresh token is not an access token.
        let r = TokenClaims {
            typ: "refresh".into(),
            ..t
        };
        assert!(!o.verify_access(&o.sign(&r)));
        // Flip one payload byte.
        let mut forged = signed.clone().into_bytes();
        forged[3] ^= 1;
        assert!(!o.verify_access(&String::from_utf8_lossy(&forged)));
        // Another admin key = another signing key.
        let other = OAuth::new(
            "https://pulse.test",
            "other-key-0123456789",
            &[],
            dir.path(),
        )
        .unwrap();
        assert!(!other.verify_access(&signed));
    }

    #[test]
    fn redirect_allowlist() {
        let dir = tempfile::tempdir().unwrap();
        let o = OAuth::new(
            "https://pulse.test",
            "admin-key-0123456789",
            &["https://app.example.com/cb".into()],
            dir.path(),
        )
        .unwrap();
        assert!(o.redirect_allowed("https://claude.ai/api/mcp/auth_callback"));
        assert!(o.redirect_allowed("https://chatgpt.com/connector/oauth/abc123"));
        assert!(!o.redirect_allowed("https://chatgpt.com/connector/oauth/"));
        assert!(o.redirect_allowed("https://app.example.com/cb"));
        assert!(o.redirect_allowed("http://localhost:4000/callback"));
        assert!(!o.redirect_allowed("https://evil.example/cb"));
        assert!(!o.redirect_allowed("https://claude.ai.evil.example/api/mcp/auth_callback"));
    }
}
