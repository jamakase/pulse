# pulse

Event-аналитика без UI: один Rust-бинарь, который принимает события от любых
наших продуктов по HTTP и отвечает на аналитические вопросы через MCP
(интерфейс — LLM-агент, а не дашборд). Хранилище встроено: WAL → Parquet на
локальном диске, SQL — Apache DataFusion. Подробности и решения — в [PRD.md](PRD.md).

```
продукты ──POST /v1/events──▶ pulse ──WAL──▶ Parquet (product=…/date=…)
агенты   ◀──MCP /mcp (SQL)──┘
```

## Запуск

```bash
PULSE_API_KEY=$(openssl rand -hex 24) cargo run
# или
docker build -t pulse . && docker run -p 8080:8080 -v pulse-data:/data \
  -e PULSE_API_KEY=... pulse
```

### Env-конфиг

| переменная | default | что делает |
|---|---|---|
| `PULSE_API_KEY` | — (обязателен, ≥16 симв.) | bearer-ключ для ingest и MCP |
| `PULSE_PORT` | `8080` | порт HTTP |
| `PULSE_DATA_DIR` | `./data` | каталог WAL + Parquet (volume) |
| `PULSE_ALLOWED_ORIGINS` | пусто | CSV точных Origin для браузерных запросов; пусто = запросы с Origin отклоняются |
| `PULSE_COMPACT_INTERVAL_SECS` | `60` | период компакции WAL → Parquet |
| `PULSE_TTL_DAYS` | `730` | удалять партиции старше N дней |
| `PULSE_PROPERTY_DENYLIST` | `email,phone,name,…` | PII-ключи, вырезаемые из properties/context на входе |

## API

```bash
# приём событий (массив или {"events": [...]}, ≤500 за батч)
curl -s -X POST localhost:8080/v1/events \
  -H "Authorization: Bearer $PULSE_API_KEY" -H 'Content-Type: application/json' \
  -d '[{"product":"constractio","event":"signup","user_id":"u1",
        "properties":{"plan":"pro"},"context":{"utm_source":"vk"}}]'
# → 202 {"accepted":1,"rejected":[]}

curl -s localhost:8080/health
```

Поля события: `product` (обязательно, `[a-zA-Z0-9_-]`), `event` (обязательно),
`occurred_at` (RFC3339, default — серверное время), `anonymous_id`, `user_id`,
`session_id`, `source` (`client`|`server`), `properties`, `context` (объекты).

## MCP

Streamable HTTP на `/mcp`, тот же bearer-ключ:

```bash
claude mcp add pulse http://localhost:8080/mcp \
  -t http -H "Authorization: Bearer $PULSE_API_KEY"
```

Тулы: `get_schema` (что вообще есть: продукты, события, объёмы),
`query_events` (read-only SQL по таблице `events`), `user_timeline`
(хронология одного пользователя).

Все колонки — строки; времена RFC3339 UTC (сортируются лексикографически),
`properties`/`context` — JSON-строки, `date` (YYYY-MM-DD) — ключ партиции,
фильтр по нему ускоряет сканы.

## Гарантии и границы (v1)

- 202 возвращается после fsync в WAL; рестарт процесса события не теряет.
- Запросы видят WAL-хвост сразу, без ожидания компакции.
- Read-only конструктивно: у движка запросов нет DDL/DML поверхности.
- Один узел, телеметрия (не данные клиентов): бэкап = синк иммутабельных
  Parquet-партиций в S3 (rclone/cron), вне скоупа бинаря.
- Сырой IP не персистится; PII-ключи режутся denylist'ом на входе.

## Разработка

```bash
cargo test          # юниты + интеграционные (ingest → compact → query)
cargo fmt && cargo clippy --all-targets -- -D warnings
```
