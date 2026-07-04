# Notifier — Event-Driven Notification System

A scalable notification service in Go: notifications are accepted over a
REST API, queued in RabbitMQ, and delivered asynchronously to SMS, Email,
and Push channels with per-channel rate limiting, tiered retries, and
real-time status tracking.

Built for the Insider One Software Engineer Assessment.

## Quickstart

```bash
docker compose up --build
```

That one command starts PostgreSQL, RabbitMQ, the API, and the worker;
database migrations apply automatically at boot. Then:

- **Testing dashboard**: http://localhost:8081/dashboard — send
  notifications, run one-click behavior scenarios, pause/resume the
  worker, watch queues drain and messages land on simulated devices.
- **API docs (Swagger UI)**: http://localhost:8081/docs
- **RabbitMQ management**: http://localhost:15672 (notifier/notifier)

By default the worker delivers to a built-in mock provider so everything
works offline. To deliver to a real webhook.site URL instead:

```bash
PROVIDER_URL=https://webhook.site/<your-uuid> docker compose up --build
```

## Architecture

```
                      ┌──────────────────────── RabbitMQ ────────────────────────┐
 API consumer ──REST──► api ──publish (confirms)──► notifications.direct         │
      ▲                 │                             │ per-channel priority     │
      │ /ws live        │ persist                     ▼ queues (sms/email/push)  │
      └─────────────────┤                           worker ──POST──► provider
                        ▼                             │ claim / rate limit
                   PostgreSQL ◄──── status writes ────┘ retry tiers 5s/30s/120s
                   (source of truth)                    exhausted → DLQ
```

Editable diagrams live in `docs/architecture.drawio` and
`docs/er-diagram.drawio` (open with diagrams.net).

- `cmd/notifier` — single binary; `ROLE=api|worker|all` selects components.
- `internal/domain` — pure types, the status state machine, validation.
- `internal/service` — orchestration: create/batch/cancel/list, template
  rendering, idempotent replay.
- `internal/storage/postgres` — pgx repositories and embedded migrations.
- `internal/queue/rabbit` — topology, confirmed publishing, consuming.
- `internal/worker` — claims deliveries, rate limits, applies retry policy.
- `internal/scheduler` — fires scheduled notifications; sweeps lost publishes.
- `internal/delivery` — provider senders (webhook + logging simulator).
- `internal/api` — chi handlers, WebSocket hub, testing dashboard.

**Status lifecycle**

```
pending ──► queued ──► processing ──► sent
scheduled ─┘   ▲            │
               └─ retrying ◄┘──► failed
pending | scheduled | queued ──► cancelled
```

Every transition is enforced twice: in `internal/domain`'s state machine
and in SQL via guarded conditional UPDATEs — concurrent workers can never
double-deliver or resurrect a cancelled notification.

## API examples

Create a notification:

```bash
curl -X POST localhost:8081/api/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"recipient":"+905551234567","channel":"sms","content":"Hello!","priority":"high"}'
```

Idempotent create (retrying the same request returns the original with 200):

```bash
curl -X POST localhost:8081/api/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"recipient":"+905551234567","channel":"sms","content":"Hi","idempotency_key":"order-42"}'
```

Schedule for later:

```bash
curl -X POST localhost:8081/api/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"recipient":"+905551234567","channel":"sms","content":"Reminder!","scheduled_at":"2026-07-05T09:00:00Z"}'
```

Templates (render at enqueue time; `{{.var}}` placeholders, missing
variables are rejected):

```bash
curl -X POST localhost:8081/api/v1/templates \
  -H 'Content-Type: application/json' \
  -d '{"name":"otp","channel":"sms","body":"Your code is {{.code}}."}'

curl -X POST localhost:8081/api/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"recipient":"+905551234567","channel":"sms","template":{"name":"otp","vars":{"code":"421337"}}}'
```

Batch (≤1000 per request, partial success with per-index results):

```bash
curl -X POST localhost:8081/api/v1/notifications/batch \
  -H 'Content-Type: application/json' \
  -d '{"notifications":[{"recipient":"+905551111111","channel":"sms","content":"one"},
                        {"recipient":"+905552222222","channel":"sms","content":"two"}]}'
```

Query, list, cancel:

```bash
curl localhost:8081/api/v1/notifications/<id>
curl 'localhost:8081/api/v1/notifications?status=sent&channel=sms&limit=20'
curl -X POST localhost:8081/api/v1/notifications/<id>/cancel
```

Live status stream (WebSocket): connect to `ws://localhost:8081/ws`
(optionally `?id=<uuid>` to follow one notification).

Observability: `GET /metrics` (Prometheus), `GET /healthz` (liveness),
`GET /readyz` (dependency readiness with per-dependency detail).

The full contract is in `api/openapi.yaml`, served at
`/api/v1/openapi.yaml` and rendered at `/docs`.

## Delivery & retry design

- **At-least-once made effectively-once.** The queue carries only the
  notification ID; workers atomically claim rows via
  `UPDATE ... WHERE status IN (...) RETURNING` — a redelivered or
  duplicate message finds the row already claimed/sent/cancelled and is
  dropped. Workers ack only after the outcome is persisted.
- **Error classification.** Provider failures are typed: network errors,
  timeouts, HTTP 5xx and 429 are retryable; other 4xx are permanent;
  unknown errors default to retryable (safe under the claim guard).
- **Tiered backoff without plugins.** Retryable failures republish to
  fixed-TTL queues (5s → 30s → 120s) whose dead-letter exchange routes
  back to the original work queue, preserving the routing key. Fixed
  per-tier TTLs avoid the head-of-line blocking of per-message TTLs.
  Attempts cap at 4 (`MAX_DELIVERY_ATTEMPTS`); exhausted or permanent
  failures persist `failed` + `last_error` and are dead-lettered to
  `notifications.dlq` for inspection.
- **Rate limiting.** A shared token bucket per channel
  (`golang.org/x/time/rate`) caps deliveries at 100/s per channel across
  that channel's concurrent handlers.
- **Priority.** Work queues are declared with `x-max-priority`; high/
  normal/low map to AMQP priorities 9/5/1, so a high-priority message
  jumps any backlog.
- **Scheduling & self-healing.** A 1s poller claims due scheduled rows
  with `FOR UPDATE SKIP LOCKED` (safe under replicas). The same sweep
  rescues rows whose queue publish was lost (e.g. broker outage at create
  time) — recovery without a full outbox implementation.
- **Correlation IDs** flow from `X-Request-ID` through AMQP headers into
  worker logs: one grep follows a notification end to end.

## Honest limitations (deliberate scope decisions)

- **Rate limit is per-process.** N worker replicas ⇒ N×100/s. Mitigate by
  setting `RATE_LIMIT_PER_CHANNEL=100/N`, or move to a shared (Redis)
  bucket for true distributed limiting.
- **No AMQP auto-reconnect.** If RabbitMQ restarts, `/readyz` flips to
  503 and consumers exit so the orchestrator restarts the process (compose
  is configured with `restart: unless-stopped`). Production would add a
  reconnecting connection wrapper.
- **Publisher confirms serialize on one channel** (~hundreds of
  publishes/s). Fine for this scale; a channel pool would lift it.
- **No jitter on retry tiers** — fixed-TTL queues can't vary per message;
  tier delays already spread load adequately at this scale.
- **Prometheus counters reset on restart** (standard behavior); the
  dashboard therefore shows session metrics alongside restart-proof
  lifetime totals derived from the database.
- **Distributed tracing** was skipped in favor of correlation IDs, which
  cover the debugging story at this scale.

## Configuration

Everything is environment-driven (defaults in parentheses):

| Variable | Purpose |
|---|---|
| `ROLE` | `api`, `worker`, or `all` (all) |
| `HTTP_PORT` | API / ops port (8081) |
| `DATABASE_URL` | Postgres DSN |
| `RABBITMQ_URL` | AMQP URL |
| `PROVIDER_URL` | Delivery endpoint; empty = log-simulated sends |
| `PROVIDER_TIMEOUT` | Outbound send timeout (10s) |
| `MAX_DELIVERY_ATTEMPTS` | First try + retries (4) |
| `RATE_LIMIT_PER_CHANNEL` | Deliveries/sec per channel (100) |
| `WORKER_CONCURRENCY` | Handlers per channel queue (10) |
| `WORKER_PREFETCH` | AMQP prefetch per consumer (50) |
| `SCHEDULER_POLL_INTERVAL` | Due-row poll cadence (1s) |
| `STALE_PENDING_AFTER` | Lost-publish sweep cutoff (1m) |
| `DASHBOARD_ENABLED` | Mount dashboard + mock provider (compose: true) |
| `WORKER_METRICS_URL` | Merge worker metrics into dashboard summary |
| `SHUTDOWN_TIMEOUT` | Graceful drain budget (15s) |
| `LOG_LEVEL` | debug/info/warn/error (info) |

## Testing

```bash
make test          # unit tests with -race (CI runs the same)
./scripts/test.sh  # full matrix: vet, unit, coverage, race, live e2e checks
```

Tests are table-driven with hand-written fakes (no mock frameworks) and
cover the domain state machine, validation, retry policy and error
classification, rate limiter behavior, idempotent replay, batch partial
success, cursor pagination, the WebSocket hub, and every handler's
status-code mapping. The dashboard's scenario runner doubles as an
interactive acceptance suite: idempotency, retry, dead-letter, cancel,
priority ordering, rate limiting, scheduling, templates, and batch — each
one button, asserting against the live stack.

CI (GitHub Actions) runs build, vet, golangci-lint, and `go test -race`
on every push.

## Project layout & development

```bash
make up      # start the stack
make run     # run api+worker locally against it
make lint    # gofmt + vet + golangci-lint
make deploy  # build image + roll local stack (scripts/deploy.sh)
```

Deploy/test scripts have Windows twins (`scripts/*.bat`). The dashboard,
OpenAPI spec, and migrations are embedded in the binary — the compose
image is self-contained.
