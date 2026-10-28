# HyperMQ Roadmap

Phases are ordered by dependency. A phase is done when its exit criteria pass; do not start Task kinds beyond HTTP before Phase 6.

## Phase 0: Foundation

- Add core dependencies: tokio, serde, tracing, an HTTP server framework, an error-handling crate.
- Define the configuration format and loader (settings, RabbitMQ URL from environment).
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
- Optionally expose management endpoints for the same operations.
- Support dead-letter exchanges and queue arguments needed by the retry policy.

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
