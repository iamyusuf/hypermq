# HyperMQ Roadmap

Phases are ordered by dependency. A phase is done when its exit criteria pass; do not start Task kinds beyond HTTP before Phase 6.

## Phase 0: Foundation

- Add core dependencies: tokio, serde, tracing, an HTTP server framework, an error-handling crate.
- Define the configuration format and loader (plain-text YAML values, including the RabbitMQ URL; no variable interpolation).
- Set up logging, a typed error model, and a `/health` endpoint.
- Replace the placeholder in `src/main.rs` with the real entry point.

Exit: `cargo build`, `cargo clippy --all-targets -- -D warnings` and `cargo test` pass; the service starts and answers `/health`.

## Phase 1: Publish over HTTP

- Add a RabbitMQ client module (connection, channel management, reconnect on failure).
- Add a publish endpoint targeting an exchange or queue with routing key, headers, and body.
- Validate and bound inputs (payload size, names, routing keys) and use publisher confirms.
- Return stable error responses for broker failures.

Exit: a client can publish via HTTP and the message lands in RabbitMQ; invalid input is rejected without panics. Unit tests cover validation; RabbitMQ integration tests are kept separate.

## Phase 2: Topology management

- Declare exchanges, queues, and bindings from configuration at startup.
- Optionally expose management endpoints for the same operations (not implemented; revisit if runtime changes are needed, see Phase 7).
- Support dead-letter exchanges and queue arguments needed by the retry policy (`dead_letter_exchange`, `dead_letter_routing_key`, `message_ttl`).

Exit: topology in configuration is created idempotently; conflicting declarations fail with a clear error.

## Phase 3: Task abstraction and consumer engine

- Define the `Task` trait, the message type (body, headers, routing metadata, stable message id), and the outcomes `Success`, `Retryable`, `Permanent`.
- Build the task registry/factory driven by the `type` tag; unknown kinds fail at config load.
- Build the consumer engine: consume from a configured queue, run the Task, map outcomes to ack/nack, apply bounded retries, then dead-letter.
- Enforce a per-Task timeout and a concurrency limit (prefetch) per consumer.

Exit: the engine is tested only with a fake Task, covering each outcome, retry exhaustion, timeouts, and dead-lettering. The engine references no concrete Task kind.

## Phase 4: HTTP Task (first Task kind)

- Implement `tasks/http` with an HTTP client: method, URL, headers, and body from configuration.
- Map responses to outcomes (for example 2xx to `Success`, 429/5xx/network errors to `Retryable`, other 4xx to `Permanent`).
- Send the message id as an idempotency header.
- Restrict destinations (allowlist or blocked ranges) to prevent SSRF; keep secrets out of logs.

Exit: an end-to-end flow works: HTTP publish, RabbitMQ, consumer, HTTP call to a downstream test service, including retry and dead-letter paths.

## Phase 5: Hardening and operations

- Authentication for the HTTP API.
- Graceful shutdown (stop consuming, finish in-flight Tasks, then ack/nack).
- Consumer reconnect and recovery after broker restarts.
- Metrics and structured logs (published, delivered, retried, dead-lettered, Task latency).
- Container image, CI pipeline, and integration tests against a real RabbitMQ.
- Write `docs/` for the API and configuration reference.

Exit: the service survives broker restarts and shutdown without losing or double-acking messages beyond at-least-once guarantees; CI runs unit and integration suites.

## Phase 6: Additional Task kinds

Add one kind at a time, each in its own module behind a Cargo feature, with isolated tests and no change to the consumer engine:

- Publish to another RabbitMQ broker or exchange.
- Email (SMTP).
- SMS (provider abstraction).
- Kafka.

Exit per kind: configuration schema documented, outcome mapping defined, timeout and secret handling in place.

## Phase 7: Runtime management and advanced consumers

- Add, change, and remove consumers at runtime (if the open design question is decided in favor).
- Template Task inputs from message content.
- Multiple Tasks per consumer (sequence or fan-out) with a defined partial-failure policy.

Exit: these behaviors are covered by tests and documented, and the open design questions in [AGENTS.md](../../AGENTS.md) are resolved.

## Phase 8: Publish outbox

Until this phase, publishing is fail-fast: if RabbitMQ is unreachable or a confirm times out, the API returns `503` with `Retry-After` and the client retries. Clients may send an idempotency key or message id so retries do not create duplicates.

This phase adds a persistent outbox as a fallback. A message is stored in the database only when its direct publish to RabbitMQ fails with a connection, channel, or confirm failure. Otherwise it is published directly, as before. A relay worker publishes stored messages later, so the API can accept them while RabbitMQ is down.

Decisions:

- Database: PostgreSQL (development and the first production target) and MySQL 8 or later, through one ORM that supports both. SeaORM with `sea-orm-migration` is the choice. Confirm it in a short spike before the phase starts; sqlx is the fallback if the spike shows the ORM gets in the way.
- Only the `src/outbox/` module imports the ORM. Each database backend sits behind its own Cargo feature (`outbox-postgres`, `outbox-mysql`), and no SQL outside that module depends on one backend.

Steps:

- Keep direct publishing as the first path. If it fails with `broker_unavailable` (connection, channel, or confirm failure), insert the message into the `outbox` table and respond `202 Accepted`. Direct success keeps `200 OK`. A broker nack (`publish_rejected`) stays `502` and is not stored.
- Enable the fallback by setting `outbox.database_url`. Without it, publishing stays fail-fast.
- The `outbox` table holds the message id (primary key), exchange, routing key, headers, body, attempt count, next attempt time, creation time, and last error.
- Run one relay worker per process in the `api` and `all` roles, for the life of the process. Each cycle claims due rows with `SELECT ... FOR UPDATE SKIP LOCKED` (supported by PostgreSQL and MySQL 8), publishes them with publisher confirms, deletes each row after its confirm, and then waits a cooldown before the next cycle. The cooldown is 5 seconds by default and configurable; it also bounds delivery latency.
- Retry with exponential backoff and a cap while the broker is down. A failed row gets a later `next_attempt_at`; the worker keeps running and reconnects when the broker returns.
- Define ordering: within the outbox, a row is published only after earlier rows with the same exchange and routing key. A failing row blocks only its own key. A message published directly can overtake earlier outbox rows with the same key, and replicas do not guarantee per-key order. The docs must say so.
- Bound the outbox by row count and age (`outbox.max_messages`, `outbox.max_age`). At the row limit the API returns `503` with `Retry-After`. Rows older than `max_age` move to the dead-letter table.
- Dead-letter rows that can never be published (the broker rejects the exchange or message, or the message stays unroutable after the maximum attempts). They move to an `outbox_dead` table with the last error and stay until an operator removes them.
- Expose outbox depth, oldest message age, dead-letter count, and relay failures as metrics. Report `degraded` in `/health` when depth keeps growing across cycles or the oldest message exceeds a threshold.
- Run migrations as an explicit `hypermq migrate` step before replicas start, not on every startup.
- Configure the database with `outbox.database_url`. The URL scheme (`postgres://` or `mysql://`) selects the backend. Treat it as a secret: read it from the environment or config and never log it.
- Deployment: all replicas share one database; there is no per-replica outbox. The database needs durable storage (the Compose `postgres-data` volume in development, managed storage in production).
- Development: the Compose `postgres` service (`postgres:17`, host port `5433`) provides the database.

Not planned: an in-memory-only buffer, because it loses data on restart.

Exit: messages that fail to publish during a RabbitMQ outage are stored and published after it recovers, with at-least-once delivery and no loss across an app restart. Messages published while RabbitMQ is healthy never write an outbox row. Tests cover the outage, restart, full outbox, and permanent-failure paths on both PostgreSQL and MySQL (MySQL as a CI service container), and two relays never hold the same row at once.
