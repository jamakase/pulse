# PRD: Pulse — event-аналитика без UI, agent-first (рабочее название)

**Статус:** draft v1 · 2026-06-12
**Репо:** `constractio/analytics` (этот)
**Владелец:** Artem

## 1. Зачем

Продуктовая аналитика, у которой интерфейсом является LLM-агент, а не дашборд.
Один Rust-бинарь (= один Docker-контейнер) принимает события от любых наших
продуктов по HTTP и отвечает на аналитические вопросы через MCP. Никакого UI,
никакой внешней БД, никакой Kafka.

Триггер: в constractio воронка регистрация → смета → оплата падает на чекауте,
а аналитика выключена. PostHog self-hosted требует 16 GB RAM и 7 сервисов ради
free-tier функциональности; облако — чужая юрисдикция и адблокеры. Все готовые
«лёгкие» альтернативы (OpenPanel, Umami, Plausible) продают UI, который нам не
нужен, и тянут свою инфру.

**Тезис продукта:** если потребитель аналитики — агент, то весь продукт это
(1) быстрый ingest, (2) колоночное хранилище, (3) SQL через MCP. Это помещается
в один бинарь.

## 2. Пользователи

| Кто | Что делает |
|---|---|
| Наши продукты (constractio webapp, будущие) | шлют события server-side и из браузера (через свой first-party прокси) |
| Агент (Claude Code, копилот в продукте, любой MCP-клиент) | задаёт вопросы: воронки, retention, таймлайны пользователей, ad-hoc SQL |
| Основатель | подключает MCP в чат и спрашивает словами; не пишет SQL руками |

## 3. Цели / не-цели

**Цели v1:**
- G1. Один статически слинкованный бинарь / scratch-образ < 100 MB; RAM < 512 MB idle.
- G2. Ingest: устойчиво ≥ 10k событий/с на 2 vCPU при батчевой записи (наш реальный трафик — на 4–5 порядков ниже; запас = «на все продукты навсегда»).
- G3. Запросы: типовая воронка по 100M событий < 1 с.
- G4. Мульти-продуктовость: изоляция по `product`, отдельные write-ключи.
- G5. Аналитика доступна **только** через MCP (+ узкий REST для ingest/health).
- G6. Durability: принятое событие переживает рестарт процесса (WAL + fsync-политика).

**Не-цели v1 (осознанно):**
- Никакого web-UI (опциональная статус-страница `/status` — кандидат на v2, по умолчанию выключена).
- Session replay, feature flags, A/B, surveys — нет.
- Распределённость/кластер — нет; одна нода, вертикальный рост.
- Реалтайм-стриминг подписки — нет; запросы pull-only.
- Cookie-consent UI — ответственность продукта; pulse поддерживает cookieless-режим (события без идентификаторов до согласия).

## 4. Архитектура

```
продукты ──HTTP POST /v1/events──┐
                                 ▼
                 ┌────────────────────────────────┐
                 │  pulse (1 Rust-бинарь, axum)   │
                 │                                │
                 │  ingest → WAL (append-only)    │
                 │     └─ compactor → Parquet     │
                 │         (партиции product/день)│
                 │                                │
                 │  DataFusion (SQL, read-only)   │
                 │     ├─ UDAF: window_funnel,    │
                 │     │        retention, seq    │
                 │     └─ MCP (streamable HTTP)   │
                 └────────────────────────────────┘
                                 ▲
агент (Claude / копилот) ── /mcp ┘        данные: один volume (WAL + Parquet)
```

**Стек (всё pure Rust):**
- HTTP + MCP: `axum` + `rmcp` (официальный Rust MCP SDK), один порт, MCP transport — streamable HTTP на `/mcp`.
- Хранилище: собственный WAL (NDJSON или Arrow IPC, append-only) → фоновый компактор сворачивает в Parquet, партиционирование `product=…/date=…`, zstd.
- Запросы: Apache `datafusion` поверх Parquet + WAL-хвоста (свежие события видны в запросах сразу — union горячего хвоста и холодных партиций).
- Скорость «обработки на Rust»: воронки/retention реализуем как **нативные UDAF** в DataFusion (аналог `windowFunnel()`/`retention()` из ClickHouse) — это и есть наши «тулы на расте, чтобы обрабатывать максимально быстро», а не пост-обработка результатов SQL на клиенте.
- TTL: удаление партиций старше N дней (конфиг per-product).

**Почему не embedded-альтернативы:** DuckDB — тянет C++ toolchain в сборку и
ограничивает нас его SQL; chDB — бинарь ~0.5 GB; SQLite — слабый аналитический
SQL и строковое хранение. DataFusion+Parquet — родной Rust, колоночный формат,
и read-only по построению (SQL-инъекция в худшем случае прочитает события —
DDL/DML поверхности нет вообще).

## 5. Модель данных

Одна логическая таблица `events`:

| поле | тип | примечание |
|---|---|---|
| `product` | string (dict) | 'constractio', … — обязателен, берётся из API-ключа, клиент не может подменить |
| `event` | string (dict) | snake_case, каталог — см. §7 schema registry |
| `occurred_at` | timestamp(ms, UTC) | клиентское время; сервер пишет также `received_at` |
| `received_at` | timestamp(ms, UTC) | серверное, защита от кривых клиентских часов |
| `anonymous_id` | string | кука первого касания (ставит продукт у себя) |
| `user_id` | string | после логина; пустой для анонимов |
| `session_id` | string | опционально |
| `source` | enum: client/server | |
| `properties` | JSON (string) | полезная нагрузка события |
| `context` | JSON (string) | utm_*, referrer, url, user_agent, ip-derived geo (страна) |

Identity stitching: событие `$identify {anonymous_id, user_id}` + материализуемая
при компакции таблица `identity_links(product, anonymous_id, user_id, linked_at)`
для join'ов «аноним с рекламы → оплативший пользователь».

Сортировка в Parquet: `(event, occurred_at)` внутри партиции `product/date`.

## 6. API (REST — только ingest)

```
POST /v1/events            батч до 500 событий, NDJSON или JSON-массив
  Authorization: Bearer <write-key продукта>
  → 202 {accepted: N, rejected: [{index, reason}]}
GET  /health               liveness/readiness
GET  /v1/schema            каталог событий (тот же, что MCP get_schema) — для CI продуктов
```

- Браузерные события идут через first-party прокси продукта (`/api/t` в webapp): адблокеры, CORS и куки — проблема продукта, pulse остаётся server-to-server. Прямой CORS-приём — v2.
- Backpressure: при переполнении WAL-очереди — 429; клиентская обвязка ретраит с джиттером.

### 6.1 Авторизация

Два типа статических bearer-ключей, асимметричные права, конфиг — env или
примонтированный `keys.toml`; ротация перезапуском. Никакого OAuth/UI в v1.

| | write-ключ (`pw_<product>_…`) | read-ключ (`pr_…`) |
|---|---|---|
| даёт | только `POST /v1/events` | только `/mcp` (вся аналитика) |
| скоуп | один product — **поле `product` берётся из ключа**, из тела не принимается | все products (per-product read-скоупы — v2) |
| где живёт | только server-side у продукта; в браузер не попадает (браузер → first-party прокси продукта) | у MCP-клиентов (Claude Code `--header`, копилот) |
| утечка = | мусор в данных одного продукта | чтение телеметрии; записи/DDL нет конструктивно |

Гигиена: без валидного ключа — 401 на всё, включая MCP handshake; сравнение
constant-time; в логах только префикс ключа; TLS терминирует Traefik, бинарь
слушает plain HTTP за прокси; rate limit на write-ключ.

> Упрощение для v1 (решение 2026-06-12): один общий API-ключ из env
> (`PULSE_API_KEY`) на ingest и MCP + PostHog-модель для браузерных источников —
> allowlist разрешённых Origin (`PULSE_ALLOWED_ORIGINS`): запросы с Origin вне
> списка отклоняются, server-to-server (без Origin) проходят по ключу.
> Раздельные write/read-ключи — следующая итерация.

### 6.2 Приватность (GDPR / 152-ФЗ)

Self-hosted закрывает главное: нет передачи данных третьим лицам и
трансграничного трансфера; для РФ-продуктов данные физически в РФ (152-ФЗ).
Механизмы в pulse:

- **Минимизация**: сырой IP не персистится никогда (только производная страна,
  если geo включён); конфигурируемый denylist ключей `properties`
  (`email`, `phone`, `name`, …) — чистится на ingest; идентификаторы
  псевдонимные (`user_id` — внутренний ID, `anonymous_id` — случайная кука).
- **Erasure (Art. 17)**: админ-джоба «переписать партиции без user_id X»
  (`DELETE /v1/users/{id}`, асинхронно; партиции иммутабельны → rewrite+swap).
- **Access/portability (Art. 15/20)**: `user_timeline` → JSON-экспорт.
- **Storage limitation**: TTL per-product.
- **Cookieless-режим**: до cookie-согласия продукт шлёт события без
  идентификаторов; согласие/lawful basis — зона ответственности продукта.

## 7. MCP-тулы

| тул | вход | выход | примечание |
|---|---|---|---|
| `query_events` | SQL (SELECT-only), product?, limit | таблица (JSON) | главный тул; statement timeout 30 c, cap 10k строк |
| `get_schema` | product? | каталог событий: имя, описание, properties, когда появилось | schema registry наполняется лениво: ingest аккумулирует наблюдаемые события/ключи, описания докидываем файлом `schema/*.md` |
| `funnel` | product, steps[], window, since/until, breakdown? | конверсия по шагам + uniques | сахар над UDAF `window_funnel`; для агентов, которым лень писать SQL |
| `user_timeline` | product, user_id \| anonymous_id, since? | хронология событий пользователя | «что случилось у этого юзера» — главный дебаг-вопрос |

Read-only гарантируется конструктивно (DataFusion без DML) + allowlist на SELECT.
Auth MCP: отдельный read-ключ (Bearer). Один read-ключ видит все продукты (мы
один пользователь); per-product read-скоупы — v2.

## 8. Деплой и эксплуатация

- Один Docker-контейнер, один volume (`/data`: WAL + Parquet). Конфиг через env: ключи, TTL, лимиты.
- Первый запуск: на staging VM Timeweb (186.246.28.126, 2 vCPU/4 GB — pulse займёт < 0.5 GB) за существующим Traefik, хост `pulse.constract.io` (или events.). Это не critical-path сервис: деградация = потеря телеметрии, не данных клиентов.
- Бэкап: `rclone`/cron синк Parquet-партиций в S3 (Yandex Object Storage уже есть) — партиции иммутабельны, синк тривиален.
- Наблюдаемость v1: `/health` + счётчики в логах (structured, JSON). Метрики Prometheus — v2.

## 9. Интеграция с constractio webapp (первый клиент)

1. `src/lib/analytics.ts`: posthog-node → HTTP-батчер в pulse (сигнатура `trackEvent()` не меняется, колл-сайты в `actions/*` не трогаем).
2. Новый `/api/t`: first-party прокси браузерных событий + кука `anonymous_id` + `$identify` при логине.
3. Выпилить posthog-js/posthog-node и провайдер (pre-launch, шимов не оставляем).
4. Доинструментировать чекаут мелко: `paywall_shown → checkout_started → yookassa_redirect → payment_webhook{status, error_code}` + `client_error` — это закрывает исходную задачу «где умирают 48 человек».
5. MCP pulse подключить в Claude Code (и позже — копилоту как client-side MCP).

## 10. Вехи

- **M0** — скелет: cargo workspace, axum, конфиг, /health, Docker scratch-образ, CI (fmt/clippy/test). ~день.
- **M1** — ingest: /v1/events, ключи, WAL + fsync, компактор в Parquet, TTL. Бенч ingest. ~2–3 дня.
- **M2** — запросы: DataFusion над Parquet+WAL, MCP `query_events` + `get_schema`. С этого момента продукт уже полезен. ~2–3 дня.
- **M3** — Rust-аналитика: UDAF `window_funnel`/`retention`, тулы `funnel`, `user_timeline`, identity stitching. ~2–3 дня.
- **M4** — прод: деплой на staging VM, интеграция webapp (§9), бэкап в S3. ~1–2 дня.
- v2-кандидаты: статус-страница, CORS-ingest, Prometheus, GDPR-delete, per-product read-скоупы.

## 11. Риски

| риск | митигация |
|---|---|
| SQL DataFusion беднее ClickHouse (нет родных funnel-функций) | свои UDAF (M3); до M3 воронки выражаются оконными функциями |
| Потеря событий при падении между accept и fsync | WAL с групповым fsync ≤ 50 мс; 202 отдаём после записи в WAL |
| Один диск = единая точка отказа данных | иммутабельные партиции → дешёвый синк в S3; телеметрия, не клиентские данные |
| Scope creep в «PostHog-клон» | не-цели §3 ревьюим перед каждой вехой |
| `rmcp`/DataFusion API нестабильны | пиновать версии; surface у нас узкий (4 тула) |

## 12. Открытые вопросы

1. Имя продукта/бинаря (pulse — занято многими; варианты: `evrs`, `tracer`, `пульс`).
2. Формат WAL: NDJSON (проще дебаг) vs Arrow IPC (быстрее компакция) — решить бенчем в M1.
3. Geo из IP (страна) в v1 или v2? (тянет maxmind-базу в образ).
4. Лицензия репо, если когда-нибудь опенсорсить (Apache 2.0 по умолчанию).
