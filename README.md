# pulse

**Agent-first product analytics. One binary. No UI — your LLM is the dashboard.**

[![ci](https://github.com/jamakase/pulse/actions/workflows/ci.yml/badge.svg)](https://github.com/jamakase/pulse/actions/workflows/ci.yml)
[![release](https://github.com/jamakase/pulse/actions/workflows/release.yml/badge.svg)](https://github.com/jamakase/pulse/actions/workflows/release.yml)
[![image](https://img.shields.io/badge/ghcr.io-jamakase%2Fpulse-blue?logo=docker)](https://github.com/jamakase/pulse/pkgs/container/pulse)
[![license](https://img.shields.io/badge/license-Apache--2.0-green)](LICENSE)

pulse ingests product events over HTTP and answers analytical questions
through [MCP](https://modelcontextprotocol.io) — funnels, retention, user
timelines, ad-hoc SQL — straight from Claude, Cursor, or any MCP client.
There is no web UI to build, host, or learn. If the consumer of analytics is
an agent, the whole product collapses into: fast ingest, columnar storage,
SQL over MCP. That fits in one binary.

```
products ──POST /v1/events──▶  pulse  ──WAL──▶ Parquet (product=…/date=…)
                                 │                      ▲
agents (Claude, copilot, …) ◀──MCP /mcp (SQL, funnels)──┘
```

## Why

Self-hosted PostHog wants 16 GB RAM and seven services (ClickHouse, Kafka,
Zookeeper, Redis, …) to run its free tier. Lightweight alternatives sell a
dashboard and stop at page views. pulse takes the opposite bet:

- **One process, one volume.** Embedded storage: append-only WAL → immutable
  Parquet partitions, queried by [Apache DataFusion](https://datafusion.apache.org)
  in-process. No external database, no broker. Idles under 512 MB.
- **MCP instead of a UI.** `get_schema` → `query_events` / `funnel` /
  `user_timeline`. "Show me this week's checkout funnel by source" is a chat
  message, not a dashboard session.
- **Multi-product by design.** One pulse instance collects events from all
  your apps; everything is partitioned by `product`.
- **Boring durability.** 202 only after fsync; queries see the WAL tail
  instantly; partitions are immutable files you can rsync to S3.

## Quick start

```bash
docker run -p 8080:8080 -v pulse-data:/data \
  -e PULSE_API_KEY=$(openssl rand -hex 24) \
  ghcr.io/jamakase/pulse:latest
```

Send an event:

```bash
curl -s -X POST localhost:8080/v1/events \
  -H "Authorization: Bearer $PULSE_API_KEY" -H 'Content-Type: application/json' \
  -d '[{"product":"myapp","event":"signup","user_id":"u1",
        "properties":{"plan":"pro"},"context":{"utm_source":"x"}}]'
# → 202 {"accepted":1,"rejected":[]}
```

Plug it into Claude Code:

```bash
claude mcp add pulse http://localhost:8080/mcp \
  -t http -H "Authorization: Bearer $PULSE_API_KEY"
```

…and ask: *"what's the signup → estimate → payment funnel for myapp over the
last 7 days?"*

## MCP tools

| tool | what it does |
|---|---|
| `get_schema` | discover what exists: products, event names, volumes, time ranges |
| `query_events` | read-only SQL (DataFusion dialect) over the `events` table |
| `funnel` | ordered conversion funnel computed natively in Rust — ClickHouse `windowFunnel` semantics: unique users per step within a time window |
| `user_timeline` | chronological history of one user, including pre-signup anonymous activity |

Identity stitching: send a `$identify {anonymous_id, user_id}` event when a
visitor logs in; the `identity_links` view joins their anonymous history to
the account, and `funnel`/`user_timeline` use it automatically.

## HTTP API

```
POST   /v1/events                 ingest, batch ≤500 (array or {"events":[…]})
DELETE /v1/users/{id}?product=…   GDPR Art. 17 erasure (partition rewrite)
GET    /health                    liveness
```

Event fields: `product` (required, `[a-zA-Z0-9_-]`), `event` (required),
`occurred_at` (RFC3339, default = server time), `anonymous_id`, `user_id`,
`session_id`, `source` (`client`|`server`), `properties`, `context`.

Browser events should go through your app's first-party proxy (a 30-line
route: set an httpOnly anonymous-id cookie, attach the session user_id,
forward server-to-server). Ad-blockers can't cut it and the write key never
reaches the browser.

## Configuration

| env | default | purpose |
|---|---|---|
| `PULSE_API_KEY` | — (required, ≥16 chars) | bearer key for ingest and MCP |
| `PULSE_PORT` | `8080` | HTTP port |
| `PULSE_DATA_DIR` | `./data` | WAL + Parquet directory (mount a volume) |
| `PULSE_ALLOWED_ORIGINS` | empty | CSV of exact Origins allowed for browser requests; empty = any request with an Origin header is rejected |
| `PULSE_COMPACT_INTERVAL_SECS` | `60` | WAL → Parquet compaction period |
| `PULSE_TTL_DAYS` | `730` | drop partitions older than N days |
| `PULSE_PROPERTY_DENYLIST` | `email,phone,name,…` | PII keys stripped from properties/context on ingest |

## Privacy

Self-hosted means no third parties and no cross-border transfer. On top of
that: raw IPs are never persisted, a configurable PII denylist scrubs
properties on ingest, TTL bounds retention, and `DELETE /v1/users/{id}`
implements the right to erasure by rewriting the immutable partitions without
that user's rows.

## How it stores data

```
/data
├── wal/current.ndjson            fsync'd on every accepted batch
└── events/
    └── product=myapp/
        └── date=2026-06-12/*.parquet   (zstd, immutable)
```

A background compactor seals the WAL and folds it into Hive-partitioned
Parquet. Queries union the cold partitions with the hot WAL tail, so events
are queryable the moment they're accepted. A single RwLock orders compaction
against queries — an event is never visible twice. Backup is `rclone sync` of
immutable files; scaling out later means pointing ClickHouse at the same
Parquet — the storage format outlives the engine choice.

## Deploying

`deploy/docker-compose.yml` runs the image behind an existing Traefik with
TLS. CI publishes `ghcr.io/jamakase/pulse` (`latest`, commit SHA, and semver
tags on `v*`).

## Development

```bash
cargo test                                     # unit + integration
cargo fmt && cargo clippy --all-targets -- -D warnings
```

Stack: [axum](https://github.com/tokio-rs/axum) ·
[rmcp](https://github.com/modelcontextprotocol/rust-sdk) ·
[DataFusion](https://datafusion.apache.org) · Parquet. Pure Rust, no C++
toolchain needed.

## Roadmap

- [ ] Separate write/read keys with per-product scopes
- [ ] Direct CORS ingest (skip the first-party proxy)
- [ ] Prometheus metrics
- [ ] Optional `/status` page (single static HTML)
- [ ] S3-native partition sync
- [ ] IP → country enrichment (optional maxmind)

See [PRD.md](PRD.md) for the full design rationale and non-goals.

## License

[Apache 2.0](LICENSE)
