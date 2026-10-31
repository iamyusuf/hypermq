# HyperMQ

HyperMQ is an HTTP wrapper around RabbitMQ. It also lets operators configure consumers that receive messages from RabbitMQ and run a configurable **Task** for each message (an HTTP call, SMS, email, publish to another broker, and so on).

## Status

Phase 0 (foundation) is implemented: YAML config loader (plain-text values, no variable interpolation), `--role` flag, tracing, typed errors, and `GET /health`. Phase 1 (publish over HTTP) is implemented: `POST /publish` with a lapin-based `Publisher` (lazy reconnect, publisher confirms, mandatory publishes). Phase 2 (topology) is implemented: exchanges, queues, and bindings declared from the `topology` config section at startup. Phase 3 (Task abstraction and consumer engine) is implemented: RabbitMQ consume side with ack, bounded retry through a per-queue retry queue, and dead-lettering. Phase 4 (HTTP Task) is implemented: `type: http` with `allowed_hosts`, outcome mapping, and an end-to-end test through `POST /publish`. Phase 5 (hardening) is partly implemented: optional bearer-token auth for `POST /publish` (`http_api.bearer_token`), graceful shutdown on Ctrl+C or SIGTERM (consumers get a 30-second grace period), and consumer reconnect with backoff after broker failures. Not yet implemented: metrics and structured operational logs, container image and CI, topology management endpoints, and Task kinds beyond HTTP. The rest describes the intended design, so update this file as it is implemented.

## Concept

- **Publish over HTTP**: clients send messages to HyperMQ via HTTP, and HyperMQ publishes them to RabbitMQ exchanges or queues. Clients do not need an AMQP library.
- **Consume via configuration**: consumers are declared in configuration (queue, task, retry/ack policy) rather than written as code. HyperMQ consumes from RabbitMQ and executes the consumer's Task for each message.
- **Task abstraction**: a Task is the unit of work performed for a consumed message. The consumer engine knows only the Task interface, never a concrete kind. The first and only initial Task is an HTTP call to a downstream service; other kinds must be addable without changing the consumer engine.
- **Topology management**: exchanges, queues, and bindings can be declared through the service or its configuration.

```mermaid
flowchart LR
    Client -->|HTTP publish| HyperMQ
    HyperMQ -->|AMQP| RabbitMQ
    RabbitMQ -->|AMQP deliver| Consumer[Consumer engine]
    Consumer -->|execute| Task{{Task}}
    Task --> Http[HTTP call - initial]
    Task -.-> Sms[SMS]
    Task -.-> Email[Email]
    Task -.-> Amqp[Publish to RabbitMQ]
    Task -.-> Kafka[Publish to Kafka]
```

Solid arrows are implemented first; dashed ones are future Task kinds.

## Task design

- A Task receives the message (body, headers, routing metadata) plus its own configuration and returns a typed outcome, not just success/failure.
- Outcomes the consumer engine maps to ack/nack: `Success` (ack), `Retryable` (bounded retry, then dead-letter), `Permanent` (reject/dead-letter without retry).
- Task kinds are selected by a `type` tag in consumer configuration (e.g. `type: http`), with kind-specific settings under it. Unknown kinds fail at config load, not at message time.
- Likely shape: an async trait (e.g. `Task`) with one implementation per kind, constructed from config through a registry/factory. Keep each kind in its own module so heavy dependencies (Kafka, SMTP, SMS providers) can sit behind Cargo features.
- Tasks must be idempotency-aware: delivery is at-least-once, so a Task may run twice for the same message. Expose a stable message id so Tasks can pass it downstream (for example as an HTTP idempotency header).
- Every Task needs a timeout; it must not block a consumer indefinitely.
- Task-specific secrets (API keys, SMTP credentials, Kafka brokers) come from environment or config and are never logged.

## Tech stack

- Rust, edition 2024 (see [Cargo.toml](Cargo.toml)).
- Planned dependencies (not yet added): an async runtime (tokio), an HTTP server framework, an AMQP client (e.g. lapin), an HTTP client for the HTTP Task (e.g. reqwest), serde for config and payloads, and a logging crate (tracing). Dependencies for other Task kinds (Kafka, email, SMS) are added only when that kind is implemented.

## Project layout

- [src/main.rs](src/main.rs): entry point.
- [src/api/](src/api/mod.rs): HTTP routes (`/health`, `/publish`), request validation, stable `ApiError` responses.
- [src/broker/](src/broker/mod.rs): `Publisher` trait and types; [src/broker/topology.rs](src/broker/topology.rs): topology types and `TopologyManager`; [src/broker/rabbitmq/](src/broker/rabbitmq/mod.rs) is the only code that imports lapin.
- [tests/rabbitmq.rs](tests/rabbitmq.rs): integration tests that need RabbitMQ (ignored by default).
- [src/consumers/](src/consumers/mod.rs): consumer config, retry settings, and the consumer engine (`Consumer`). It depends only on the `Broker`, `Task`, and `RetryPolicy` abstractions.
- [src/tasks/](src/tasks/mod.rs): the `Task` trait, `TaskFactory`, and `TaskRegistry`; [src/tasks/http/](src/tasks/http/mod.rs) is the first kind (behind the `task-http` feature).
- [docs/api/publish.md](docs/api/publish.md): publish endpoint reference.
- [docs/](docs/): design and API documentation.
- [docs/roadmap/roadmap.md](docs/roadmap/roadmap.md): phase-by-phase plan; follow its order and exit criteria.
- [docs/traits/](docs/traits/README.md): high-level trait contracts (`Task`, `Broker`, `Publisher`, `TopologyManager`, `RetryPolicy`).
- [docs/core/consumer-manager.md](docs/core/consumer-manager.md): consumer manager and consumer loop design.
- [docs/core/deployment.md](docs/core/deployment.md): one binary with `all`/`api`/`consumers` roles; single container first, split later.
- [docs/core/project-structure.md](docs/core/project-structure.md): planned module layout and dependency rules.
- [docs/dev/developer-guide.md](docs/dev/developer-guide.md): setup, Docker development environment (RabbitMQ in Compose is for development only), build, run, test, code map, and how to add endpoints, config options, and broker operations.
- `target/`: build output; never edit or commit.

## Commands

- Build: `cargo build`
- Run: `cargo run`
- Test: `cargo test`
- Integration tests (need RabbitMQ): `HYPERMQ_TEST_RABBITMQ_URL=amqp://user:pass@localhost:5672 cargo test --test rabbitmq -- --ignored`; they create and delete uniquely named queues and exchanges
- Lint: `cargo clippy --all-targets -- -D warnings`
- Format: `cargo fmt`

## Conventions for agents

- Keep changes minimal and scoped to the request; do not add features or dependencies that were not asked for.
- Use idiomatic Rust: `Result` with typed errors, no `unwrap()`/`expect()` in code paths that handle requests or messages.
- Never panic on bad input from HTTP or RabbitMQ; return an error response or reject/nack the message.
- Keep HTTP handlers thin; put RabbitMQ and consumer logic in separate modules so they can be tested without a running server.
- Never reference a concrete Task kind from the consumer engine; depend only on the `Task` abstraction. Implement only the HTTP Task until asked otherwise, but do not hardcode HTTP assumptions into shared types (messages, outcomes, consumer config).
- Test the consumer engine with a fake Task; test each Task kind in isolation.
- Read credentials (RabbitMQ URL, tokens) from environment or config; never hardcode or log them.
- Validate and bound inputs on the HTTP API (payload size, exchange/queue names, routing keys).
- Consumer processing must define ack/nack behavior explicitly: ack only after the Task succeeds, and apply bounded retries with a dead-letter or reject path rather than infinite requeue.
- Tasks make outbound requests (HTTP URLs, SMTP/SMS endpoints, broker addresses); validate or restrict destinations from configuration to avoid SSRF if they can be set at runtime.
- Add tests for new behavior; keep integration tests that need RabbitMQ separate from unit tests.
- Document new endpoints and configuration options in `docs/` when they are added.

## Open design questions

- Configuration format and location (file, environment, or runtime API).
- Authentication for the HTTP API.
- Consumer processing semantics (RabbitMQ is at-least-once by default) and retry/dead-letter policy.
- Exact `Task` trait shape, and whether Task kinds are compiled in (Cargo features) or loaded as plugins.
- Whether one consumer can run multiple Tasks (sequence or fan-out) and how partial failure is handled.
- Templating of Task inputs from message content (for example mapping fields into an HTTP body or email subject).
- Whether consumers can be added or changed at runtime or only at startup.