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
  -e PULSE_ADMIN_KEY=$(openssl rand -hex 24) \
  -e PULSE_SERVER_KEYS="myapp:$(openssl rand -hex 24)" \
  ghcr.io/jamakase/pulse:latest
```

Send an event (server key — `product` comes from the key):

```bash
curl -s -X POST localhost:8080/v1/events \
  -H "Authorization: Bearer $MYAPP_SERVER_KEY" -H 'Content-Type: application/json' \
  -d '[{"event":"signup","user_id":"u1",
        "properties":{"plan":"pro"},"context":{"utm_source":"x"}}]'
# → 202 {"accepted":1,"rejected":[]}
```

Plug it into Claude Code (admin key):

```bash
claude mcp add pulse http://localhost:8080/mcp \
  -t http -H "Authorization: Bearer $PULSE_ADMIN_KEY"
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

### Three tiers of keys

| | client key (`PULSE_CLIENT_KEYS`) | server key (`PULSE_SERVER_KEYS`) | admin key (`PULSE_ADMIN_KEY`) |
|---|---|---|---|
| grants | append only | append only | MCP + erasure — **cannot even ingest** |
| product | pinned to the key | pinned to the key | — |
| source | forced to `client` | may claim `server` (default) | — |
| lives | browser JS — public by design | your backends / SSG pipelines | with the operator only; never ships to any app |
| leak = | garbage events in one product | *forged* trusted events in one product (rotate it) | full telemetry read |

Per-product keys are append-only; the key that can read or delete never
leaves your machine. And because only server keys can write
`source='server'`, a `WHERE source = 'server'` filter in queries is an
integrity guarantee, not a convention: those events were produced by your
backend, full stop. Send decision-grade events (payments, subscriptions)
server-side.

### Sending from the browser

Two options:

1. **Direct** (PostHog model): set `PULSE_ALLOWED_ORIGINS` to your app's
   origins and send with the public client key. `?key=` exists because
   `sendBeacon` can't set headers:

   ```js
   navigator.sendBeacon(
     'https://events.example.com/v1/events?key=' + PULSE_CLIENT_KEY,
     new Blob([JSON.stringify([{event: 'page_view'}])], {type: 'application/json'}),
   );
   ```

2. **First-party proxy** (recommended when you have a backend): a 30-line
   route in your app sets an httpOnly anonymous-id cookie, attaches the
   session user_id, and forwards server-to-server. Ad-blockers can't cut it
   and no key ships to the browser at all.

Integrity rule of thumb: decision-grade events (payments, subscriptions)
should always be sent server-side — browser events are spoofable by design,
in every analytics system.

## Configuration

| env | default | purpose |
|---|---|---|
| `PULSE_ADMIN_KEY` | — (required, ≥16 chars) | operator-only key: MCP + erasure (cannot ingest) |
| `PULSE_SERVER_KEYS` | empty | per-product secret append keys: `myapp:ps_…,other:ps_…` |
| `PULSE_CLIENT_KEYS` | empty | per-product public append keys: `myapp:pc_…` |
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

- [ ] Prometheus metrics
- [ ] Optional `/status` page (single static HTML)
- [ ] S3-native partition sync
- [ ] IP → country enrichment (optional maxmind)

See [PRD.md](PRD.md) for the full design rationale and non-goals.

## License

[Apache 2.0](LICENSE)
