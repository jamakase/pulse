# pulse

Event analytics without a UI: a single Rust binary that ingests events from
any of your products over HTTP and answers analytical questions through MCP —
the interface is an LLM agent, not a dashboard. Storage is embedded: WAL →
Parquet on local disk, SQL via Apache DataFusion. Design decisions live in
[PRD.md](PRD.md).

```
products ──POST /v1/events──▶ pulse ──WAL──▶ Parquet (product=…/date=…)
agents   ◀──MCP /mcp (SQL)──┘
```

## Running

```bash
PULSE_API_KEY=$(openssl rand -hex 24) cargo run
# or
docker run -p 8080:8080 -v pulse-data:/data \
  -e PULSE_API_KEY=... ghcr.io/jamakase/pulse:latest
```

### Configuration (env)

| variable | default | purpose |
|---|---|---|
| `PULSE_API_KEY` | — (required, ≥16 chars) | bearer key for ingest and MCP |
| `PULSE_PORT` | `8080` | HTTP port |
| `PULSE_DATA_DIR` | `./data` | WAL + Parquet directory (mount a volume) |
| `PULSE_ALLOWED_ORIGINS` | empty | CSV of exact Origins allowed for browser requests; empty = requests carrying an Origin header are rejected |
| `PULSE_COMPACT_INTERVAL_SECS` | `60` | WAL → Parquet compaction period |
| `PULSE_TTL_DAYS` | `730` | drop partitions older than N days |
| `PULSE_PROPERTY_DENYLIST` | `email,phone,name,…` | PII keys stripped from properties/context on ingest |

## API

```bash
# ingest (array or {"events": [...]}, ≤500 per batch)
curl -s -X POST localhost:8080/v1/events \
  -H "Authorization: Bearer $PULSE_API_KEY" -H 'Content-Type: application/json' \
  -d '[{"product":"myapp","event":"signup","user_id":"u1",
        "properties":{"plan":"pro"},"context":{"utm_source":"x"}}]'
# → 202 {"accepted":1,"rejected":[]}

# GDPR erasure (rewrites partitions without the user's rows)
curl -s -X DELETE "localhost:8080/v1/users/u1?product=myapp" \
  -H "Authorization: Bearer $PULSE_API_KEY"

curl -s localhost:8080/health
```

Event fields: `product` (required, `[a-zA-Z0-9_-]`), `event` (required),
`occurred_at` (RFC3339, defaults to server time), `anonymous_id`, `user_id`,
`session_id`, `source` (`client`|`server`), `properties`, `context` (objects).
Send a `$identify` event carrying both `anonymous_id` and `user_id` to stitch
pre-signup anonymous activity to an account.

## MCP

Streamable HTTP at `/mcp`, same bearer key:

```bash
claude mcp add pulse http://localhost:8080/mcp \
  -t http -H "Authorization: Bearer $PULSE_API_KEY"
```

Tools:

- `get_schema` — what data exists: products, event names, volumes, time ranges
- `query_events` — read-only SQL over the `events` table (plus the
  `identity_links` view for anonymous↔user joins)
- `funnel` — ordered conversion funnel computed natively in Rust (ClickHouse
  `windowFunnel` semantics): unique users per step within a time window
- `user_timeline` — chronological history of one user, including their
  pre-signup anonymous events

All columns are strings; timestamps are RFC3339 UTC (lexicographically
sortable), `properties`/`context` are JSON strings, `date` (YYYY-MM-DD) is the
partition key — filter on it for fast scans.

## Guarantees and limits (v1)

- 202 is returned only after the batch is fsynced into the WAL; a process
  restart loses nothing.
- Queries see the WAL tail immediately — no waiting for compaction.
- Read-only by construction: the query engine has no DDL/DML surface.
- Raw IPs are never persisted; PII keys are stripped on ingest.
- Single node. Backup = sync the immutable Parquet partitions to S3
  (rclone/cron), outside the binary's scope.

## Deploying

`deploy/docker-compose.yml` runs the GHCR image behind an existing Traefik.
CI builds and pushes `ghcr.io/jamakase/pulse` (`latest`, commit SHA, semver
tags) on every push to main.

## Development

```bash
cargo test          # unit + integration (ingest → compact → query)
cargo fmt && cargo clippy --all-targets -- -D warnings
```
