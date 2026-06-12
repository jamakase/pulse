# PRD: Pulse — event analytics without a UI, agent-first (working title)

**Status:** draft v1 · 2026-06-12
**Repo:** `jamakase/pulse` (this one)
**Owner:** Artem

## 1. Why

Product analytics whose interface is an LLM agent, not a dashboard.
One Rust binary (= one Docker container) ingests events from any of our
products over HTTP and answers analytical questions through MCP. No UI, no
external database, no Kafka.

Trigger: in our SaaS the signup → estimate → payment funnel dies at checkout,
and analytics was turned off. Self-hosted PostHog wants 16 GB RAM and 7
services for free-tier functionality; its cloud means a foreign jurisdiction
and ad-blockers. Every "lightweight" alternative (OpenPanel, Umami, Plausible)
sells a UI we don't need and brings its own infra.

**Product thesis:** if the consumer of analytics is an agent, the entire
product is (1) fast ingest, (2) columnar storage, (3) SQL over MCP. That fits
in one binary.

## 2. Users

| Who | What they do |
|---|---|
| Our products (webapp, future ones) | send events server-side and from the browser (through their own first-party proxy) |
| An agent (Claude Code, in-product copilot, any MCP client) | asks questions: funnels, retention, user timelines, ad-hoc SQL |
| The founder | plugs MCP into a chat and asks in plain language; never writes SQL by hand |

## 3. Goals / non-goals

**v1 goals:**
- G1. One statically linked binary / scratch-ish image < 100 MB; RAM < 512 MB idle.
- G2. Ingest: sustained ≥ 10k events/s on 2 vCPU with batched writes (real
  traffic is 4–5 orders of magnitude lower; the headroom = "all products, forever").
- G3. Queries: a typical funnel over 100M events < 1 s.
- G4. Multi-product: isolation by `product`, separate write keys.
- G5. Analytics is reachable **only** through MCP (+ a narrow REST surface for ingest/health).
- G6. Durability: an accepted event survives a process restart (WAL + fsync policy).

**v1 non-goals (deliberate):**
- No web UI (an optional `/status` page is a v2 candidate, off by default).
- Session replay, feature flags, A/B, surveys — no.
- Distribution/clustering — no; one node, scale vertically.
- Realtime streaming subscriptions — no; queries are pull-only.
- Cookie-consent UI — the product's responsibility; pulse supports a
  cookieless mode (events without identifiers until consent).

## 4. Architecture

```
products ──HTTP POST /v1/events──┐
                                 ▼
                 ┌────────────────────────────────┐
                 │  pulse (1 Rust binary, axum)   │
                 │                                │
                 │  ingest → WAL (append-only)    │
                 │     └─ compactor → Parquet     │
                 │         (product/day partitions)│
                 │                                │
                 │  DataFusion (SQL, read-only)   │
                 │     └─ MCP (streamable HTTP)   │
                 └────────────────────────────────┘
                                 ▲
agent (Claude / copilot) ── /mcp ┘      data: one volume (WAL + Parquet)
```

**Stack (all pure Rust):**
- HTTP + MCP: `axum` + `rmcp` (official Rust MCP SDK), one port, MCP
  transport — streamable HTTP at `/mcp`.
- Storage: our own WAL (NDJSON, append-only) → a background compactor folds it
  into Parquet, partitioned `product=…/date=…`, zstd.
- Queries: Apache `datafusion` over Parquet + the WAL tail (fresh events are
  visible to queries immediately — a union of the hot tail and cold partitions).
- "Processing speed in Rust": the funnel is a **native Rust fold** over Arrow
  batches inside the `funnel` tool (ClickHouse `windowFunnel()` semantics,
  latest-start DP). M3 decision: a fold in the tool instead of a UDAF — same
  results and speed, less code and risk; we'll add a UDAF if funnels are ever
  needed inside arbitrary SQL. Retention is expressible in plain SQL via
  `query_events`.
- TTL: drop partitions older than N days (config, per product later).

**Why not the embedded alternatives:** DuckDB drags a C++ toolchain into the
build and limits us to its SQL; chDB is a ~0.5 GB binary; SQLite has weak
analytical SQL and row storage. DataFusion+Parquet is native Rust, columnar,
and read-only by construction (worst case for SQL injection is reading
events — there is no DDL/DML surface at all).

## 5. Data model

One logical table `events`:

| field | type | notes |
|---|---|---|
| `product` | string (dict) | required; in v1 comes from the request body, later derived from the write key |
| `event` | string (dict) | snake_case; catalog — see §7 schema registry |
| `occurred_at` | timestamp(ms, UTC) | client time; the server also records `received_at` |
| `received_at` | timestamp(ms, UTC) | server time, guards against broken client clocks |
| `anonymous_id` | string | first-touch cookie (set by the product itself) |
| `user_id` | string | after login; empty for anonymous |
| `session_id` | string | optional |
| `source` | enum: client/server | |
| `properties` | JSON (string) | event payload |
| `context` | JSON (string) | utm_*, referrer, url, user_agent, ip-derived geo (country) |

Identity stitching: a `$identify {anonymous_id, user_id}` event plus the
`identity_links(product, anonymous_id, user_id, linked_at)` view for joining
"anonymous ad-click visitor → paying user".

Parquet sort order: `(event, occurred_at)` inside a `product/date` partition.

## 6. API (REST — ingest only)

```
POST /v1/events            batch ≤500 events, NDJSON or JSON array
  Authorization: Bearer <product write key>
  → 202 {accepted: N, rejected: [{index, reason}]}
DELETE /v1/users/{id}?product=…   GDPR erasure (see §6.2)
GET  /health               liveness/readiness
GET  /v1/schema            event catalog (same as MCP get_schema) — for product CI
```

- Browser events go through the product's first-party proxy (e.g. `/api/t` in
  the webapp): ad-blockers, CORS and cookies are the product's problem; pulse
  stays server-to-server. Direct CORS ingest — v2.
- Backpressure: 429 when the WAL queue overflows; client wrappers retry with jitter.

### 6.1 Authorization

Two kinds of static bearer keys with asymmetric rights; config — env or a
mounted `keys.toml`; rotation by restart. No OAuth/UI in v1.

| | write key (`pw_<product>_…`) | read key (`pr_…`) |
|---|---|---|
| grants | only `POST /v1/events` | only `/mcp` (all analytics) |
| scope | one product — **`product` is derived from the key**, never accepted from the body | all products (per-product read scopes — v2) |
| lives | server-side in the product only; never in the browser (browser → the product's first-party proxy) | with MCP clients (Claude Code `--header`, copilot) |
| leak = | garbage in one product's data | telemetry reads; no write/DDL exists structurally |

Hygiene: no valid key → 401 for everything including the MCP handshake;
constant-time comparison; only key prefixes in logs; TLS terminates at
Traefik, the binary listens on plain HTTP behind the proxy; rate limit per
write key.

> Implemented 2026-06-12 (revised twice same day, final model — three tiers):
> public per-product client keys (`PULSE_CLIENT_KEYS`, source forced to
> 'client', `?key=` for sendBeacon), secret per-product server keys
> (`PULSE_SERVER_KEYS`, append-only but may claim source='server' — the
> integrity channel), and the operator-only `PULSE_ADMIN_KEY` (MCP + erasure,
> deliberately cannot ingest so it never ends up in an app's env). Origin
> allowlist + CORS gate browser requests.

### 6.2 Privacy (GDPR)

Self-hosting covers the big one: no data goes to third parties and no
cross-border transfer; data residency follows your server. Mechanisms in pulse:

- **Minimization**: raw IP is never persisted (only a derived country if geo
  is enabled); a configurable denylist of `properties` keys (`email`, `phone`,
  `name`, …) is scrubbed on ingest; identifiers are pseudonymous (`user_id` is
  an internal ID, `anonymous_id` a random cookie).
- **Erasure (Art. 17)**: `DELETE /v1/users/{id}?product=…` — compacts the WAL,
  then rewrites the product's partitions without that user's rows (immutable
  partitions → rewrite + swap under the write lock).
- **Access/portability (Art. 15/20)**: `user_timeline` → JSON export.
- **Storage limitation**: TTL per product.
- **Cookieless mode**: until cookie consent the product sends events without
  identifiers; consent and lawful basis are the product's responsibility.

## 7. MCP tools

| tool | input | output | notes |
|---|---|---|---|
| `query_events` | SQL (SELECT-only), limit | table (JSON) | the main tool; 30 s statement timeout, 10k row cap |
| `get_schema` | — | event catalog: per (product, event) counts and first/last seen + column docs | registry fills lazily from observed events |
| `funnel` | product, steps[], window, since/until | per-step unique users + conversion rates | native Rust fold, `windowFunnel` semantics |
| `user_timeline` | product, user_id \| anonymous_id, since? | the user's chronological events incl. pre-signup anonymous ones | the "what happened to this user" debug view |

Read-only is guaranteed constructively (DataFusion without DML) + SELECT-only
options. MCP auth: bearer key.

## 8. Deployment & operations

- One Docker container, one volume (`/data`: WAL + Parquet). Config via env.
- CI: GitHub Actions → `ghcr.io/jamakase/pulse` (`latest`, commit SHA, semver).
- Behind an existing Traefik via `deploy/docker-compose.yml`. Not a
  critical-path service: degradation = lost telemetry, not customer data.
- Backup: rclone/cron sync of Parquet partitions to S3 — partitions are
  immutable, the sync is trivial.
- Observability v1: `/health` + structured JSON logs. Prometheus metrics — v2.

## 9. First integration (our webapp)

1. `src/lib/analytics.ts`: posthog-node → HTTP batcher into pulse (the
   `trackEvent()` signature stays, call sites untouched).
2. `/api/t`: first-party proxy for browser events + `anonymous_id` cookie +
   `$identify` on login.
3. Remove posthog-js/posthog-node and the provider outright (pre-launch, no shims).
4. Instrument checkout finely: `paywall_shown → checkout_started →
   payment_redirect → payment_webhook{status, error_code}` + `client_error` —
   this answers the original "where do users die" question.
5. Plug the pulse MCP into Claude Code (and later the in-product copilot).

## 10. Milestones

- **M0** — skeleton: cargo project, axum, config, /health, Dockerfile, CI. ✅ 2026-06-12
- **M1** — ingest: /v1/events, keys, WAL + fsync, Parquet compactor, TTL. ✅ 2026-06-12
- **M2** — queries: DataFusion over Parquet+WAL, MCP `query_events` + `get_schema`. ✅ 2026-06-12
- **M3** — Rust analytics: `funnel` tool (native fold), identity stitching
  (`$identify` → `identity_links` view, auto-join in `user_timeline`), GDPR
  erasure (`DELETE /v1/users/{id}`). ✅ 2026-06-12
- **M4** — production: deploy behind Traefik, webapp integration (§9), S3 backup.
- v2 candidates: status page, CORS ingest, Prometheus, per-product read
  scopes, separate write/read keys.

## 11. Risks

| risk | mitigation |
|---|---|
| DataFusion SQL is poorer than ClickHouse (no built-in funnel functions) | native Rust fold in the `funnel` tool (done in M3); UDAF if ever needed in raw SQL |
| Event loss between accept and fsync | WAL with grouped fsync ≤ 50 ms; 202 only after the WAL write |
| One disk = single point of failure for data | immutable partitions → cheap S3 sync; it's telemetry, not customer data |
| Scope creep into a PostHog clone | re-read non-goals (§3) before every milestone |
| `rmcp`/DataFusion API churn | pin versions; our surface is narrow (4 tools) |

## 12. Open questions

1. Product/binary name (pulse is taken by many; candidates: `evrs`, `tracer`).
2. WAL format: NDJSON (easier debugging) vs Arrow IPC (faster compaction) — v1 shipped NDJSON.
3. Geo from IP (country) in v1 or v2? (pulls a maxmind DB into the image).
4. License if ever open-sourced (Apache 2.0 by default).
