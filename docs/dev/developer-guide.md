# Developer guide

How to set up, build, run, test, and extend HyperMQ, including the Docker development environment. For the design see [AGENTS.md](../../AGENTS.md), the [roadmap](../roadmap/roadmap.md), and [project-structure.md](../core/project-structure.md).

## Prerequisites

- A recent stable Rust toolchain (edition 2024).
- Docker, for a local RabbitMQ and for the integration tests.

## Quick start

```sh
docker compose up -d rabbitmq          # RabbitMQ on host port 5673
cargo run                              # reads ./config.yaml
curl localhost:8080/health
curl -X POST localhost:8080/publish -H 'content-type: application/json' \
  -d '{"queue":"jobs","payload":{"hello":"world"}}'
```

Set `broker.url` in `config.yaml` to `amqp://hypermq:change-me@localhost:5673` to match the Compose broker.

The publish example returns `422 unroutable` until a queue named `jobs` exists; topology is not managed by HyperMQ yet (roadmap Phase 2), so declare it yourself, for example in the management UI at `http://localhost:15673` (user `hypermq`, password `change-me`).

## Development environment

The Compose file provides a local RabbitMQ for development only; production uses a dedicated RabbitMQ that HyperMQ reaches through configuration. Only the `rabbitmq` service exists in [docker-compose.yml](../../docker-compose.yml) today; the HyperMQ image and service are planned.

### Principles

- The HyperMQ image is the same artifact in development and production. Only configuration differs.
- The Compose RabbitMQ is a development convenience. Nothing in the code may assume the broker is local or managed by Compose.
- The broker is selected only by `broker.url` in the config file. Topology is declared by HyperMQ (see [TopologyManager](../traits/topology.md)), so it works against any empty RabbitMQ.
- Development credentials are for local use only and are never reused elsewhere.

### Services

| Service | Image | Purpose | Host ports | Status |
|---|---|---|---|---|
| `rabbitmq` | `rabbitmq:4-management` | Local broker with management UI | `5673` (AMQP), `15673` (UI) | In `docker-compose.yml` |
| `hypermq` | built from `Dockerfile` | The application | `8080` (HTTP API) | Planned |

The host ports `5673` and `15673` avoid clashing with a RabbitMQ already bound to `5672`/`15672`. Inside the Compose network the broker still listens on `5672`.

The default user is `hypermq` with password `change-me`; override with `RABBITMQ_USER` and `RABBITMQ_PASS` in a local `.env` file (read by Compose for the RabbitMQ container only).

```mermaid
flowchart LR
    Dev[Developer] -->|HTTP :8080| H[hypermq]
    Dev -->|UI :15673| R[(rabbitmq container)]
    H -->|AMQP :5673 from host, rabbitmq:5672 in Compose| R
```

### Two ways to work

1. **App on the host, RabbitMQ in Docker** (fast iteration; available now). Start only the broker, then `cargo run`; the app connects to `localhost:5673`.
2. **Everything in Docker** (closest to production; planned). Compose builds the image and runs both services; the app connects to `rabbitmq:5672` over the Compose network.

### Planned `hypermq` service (sketch)

```yaml
  hypermq:
    build: .
    depends_on:
      rabbitmq:
        condition: service_healthy
    ports:
      - "8080:8080"
    volumes:
      - ./config.yaml:/etc/hypermq/config.yaml:ro
    command: ["--role", "all", "--config", "/etc/hypermq/config.yaml"]
```

- In this setup `broker.url` in `config.yaml` must use host `rabbitmq` and port `5672`.
- The healthcheck on `rabbitmq` lets `hypermq` wait for the broker instead of failing at startup.
- The default `guest` user only works from localhost inside the container, so the Compose broker uses an explicit user.

### Planned Dockerfile (sketch)

- Multi-stage build: a Rust builder stage compiles `cargo build --release`, and a slim runtime stage copies only the binary.
- Cache dependencies in a separate layer so source changes do not rebuild them.
- Run as a non-root user and do not bake config or secrets into the image.
- Task features (for example `task-http`) are chosen at build time through build arguments.

### Production differences

| | Development | Production |
|---|---|---|
| RabbitMQ | Compose service, throwaway data volume | Dedicated, managed separately |
| Connection | `amqp://` to the Compose broker | `amqps://` (TLS) to the dedicated host |
| Credentials | Local values | Secret store or orchestrator secrets mounted as the config file |
| Compose file | Used | Not used; the image is deployed by the production orchestrator |
| Management UI | Exposed on `15673` | Not exposed publicly |

### Docker commands

```sh
docker compose up -d rabbitmq      # broker only, run the app with cargo
docker compose down                # stop, keep data
docker compose down -v             # stop and delete the RabbitMQ data volume
```

## Configuration

- The config file is YAML with plain-text values. HyperMQ does not expand environment variables.
- `config.yaml` in the project root is gitignored; copy values from [docs/examples/config.yaml](../examples/config.yaml). It holds credentials, so never commit it; in production provide it from a secret mount.
- Only `broker.url` (required, `amqp://` or `amqps://`) and `http_api` (`bind`, `max_body_bytes`) are read today. Other sections in the example are ignored until their phase is implemented.
- `http_api` rejects unknown keys, so typos there fail at startup.
- Never log the broker URL or credentials. `broker.url` is held as a secret and redacted in `Debug` output.

CLI options (each also has an environment variable):

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `--config` | `HYPERMQ_CONFIG` | `config.yaml` | Path to the config file |
| `--role` | `HYPERMQ_ROLE` | `all` | `all`, `api`, or `consumers` (see [deployment.md](../core/deployment.md)) |

Logging uses `RUST_LOG` (default `info`), for example `RUST_LOG=hypermq=debug,lapin=warn cargo run`.

## Commands

| Task | Command |
|---|---|
| Build | `cargo build` |
| Run | `cargo run` |
| Unit tests | `cargo test` |
| Integration tests | `HYPERMQ_TEST_RABBITMQ_URL=amqp://hypermq:change-me@localhost:5673 cargo test --test rabbitmq -- --ignored` |
| Lint | `cargo clippy --all-targets -- -D warnings` |
| Format | `cargo fmt` |

Run format, lint, and unit tests before committing.

## Code map

| Path | Responsibility |
|---|---|
| `src/main.rs` | Parses CLI, loads config, wires components, starts the HTTP server. The only place that picks concrete types and the role. |
| `src/lib.rs` | Exposes the modules so tests and the binary share code. |
| `src/config/` | Config model, YAML loader, validation, `Role`. |
| `src/error.rs` | Shared `Error` and `ConfigError`. No dependency on other project modules. |
| `src/api/` | Axum router, handlers, request validation, `ApiError`, JSON extractor. |
| `src/broker/mod.rs` | `Publisher` trait and its request/error types. |
| `src/broker/rabbitmq/` | `RabbitPublisher`. The only code that imports `lapin`. |
| `tests/rabbitmq.rs` | Integration tests that need RabbitMQ. |

Dependency rules:
- `api` talks to the broker only through the `Publisher` trait.
- Only `broker/rabbitmq` imports the AMQP client.
- The consumer engine (later phases) depends on traits only, never on RabbitMQ or a concrete Task kind.

## Request flow for `POST /publish`

```mermaid
sequenceDiagram
    participant C as Client
    participant H as api::publish
    participant P as Publisher (RabbitPublisher)
    participant R as RabbitMQ
    C->>H: POST /publish (JSON)
    H->>H: ApiJson parse, PublishBody::into_request (validate, bound)
    H->>P: publish(PublishRequest)
    P->>R: basic_publish (mandatory, persistent)
    R-->>P: confirm (ack, ack+return, nack)
    P-->>H: Ok or PublishError
    H-->>C: 200 or stable JSON error
```

## How to add things

### A new HTTP endpoint

1. Add a handler module under `src/api/` and register the route in `api::router`.
2. Parse the body with `ApiJson<T>` so failures use the stable error shape.
3. Validate and bound every field; return `ApiError::InvalidRequest` for bad input. Never `unwrap()` request data.
4. Add a variant to `ApiError` (status and `code`) for any new failure, and map domain errors with `From`.
5. Test with the router and a fake dependency (see `FakePublisher` in `src/api/mod.rs`).
6. Document it in `docs/api/`.

### A new config option

1. Add the field to the types in `src/config/mod.rs` with a `#[serde(default)]` where a default makes sense.
2. Validate it in `Config::validate` if it has constraints.
3. Add a unit test next to the existing ones.
4. Update [docs/examples/config.yaml](../examples/config.yaml) and the configuration section above.

### A new broker operation

1. Add the method and types to the trait in `src/broker/mod.rs`.
2. Implement it in `src/broker/rabbitmq/`.
3. Add a fake implementation in the consumer or API tests, and an `#[ignore]`d integration test in `tests/rabbitmq.rs`.

## Testing

- **Unit tests** sit next to the code in `#[cfg(test)]` modules and do not need Docker. API tests use `tower::ServiceExt::oneshot` with a fake `Publisher`.
- **Integration tests** in `tests/` are `#[ignore]`d so a plain `cargo test` never needs RabbitMQ. They run against the Compose broker, create uniquely named queues and exchanges, and delete them afterwards.
- RabbitMQ 4 rejects transient non-exclusive queues, so declare test queues as durable and delete them at the end.
- For a throwaway broker instead of Compose: `docker run -d --rm --name hypermq-test-rabbit -p 5673:5672 rabbitmq:4`.
- CI will start RabbitMQ as a service container or through Compose before the integration suite (roadmap Phase 5).

## Conventions

- Keep changes minimal; do not add features or dependencies that were not requested.
- Return typed errors; no `unwrap()`/`expect()` on code paths that handle requests or messages.
- Keep HTTP handlers thin; put broker logic in `broker/`.
- Add tests with new behavior and keep integration tests separate.
- Document new endpoints and options in `docs/`.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `ACCESS_REFUSED - Login was refused` at startup | The broker rejects the credentials in `broker.url`. Fix the user and password, or use the Compose broker. |
| `POST /publish` returns `503 broker_unavailable` | RabbitMQ is down or unreachable; the client should retry. HyperMQ reconnects on the next request. |
| `POST /publish` returns `422 unroutable` | No queue received the message: the queue does not exist, or the exchange has no matching binding. |
| `POST /publish` returns `404 target_not_found` | The exchange does not exist. |
| `cannot read config file` | No `config.yaml` in the working directory; pass `--config` or create one. |
| Integration test fails with `transient_nonexcl_queues` | Declare the queue as durable. |

## Open questions

- Whether to add a small local downstream service to Compose for testing the HTTP Task.
- Whether to pin the RabbitMQ image to an exact version to match production.
- Whether a pre-provisioned definitions file should load users, vhosts, or queues, or whether HyperMQ topology declaration alone is enough.
