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

The publish example returns `422 unroutable` until a queue named `jobs` exists. Declare it in `config.yaml`; HyperMQ creates it at startup:

```yaml
topology:
  queues:
    - name: jobs
```

You can also create it by hand in the management UI at `http://localhost:15673` (user `hypermq`, password `change-me`).

## Development environment

The Compose file provides a local RabbitMQ and PostgreSQL for development only; production uses a dedicated broker and database that HyperMQ reaches through configuration. The `rabbitmq` and `postgres` services exist in [docker-compose.yml](../../docker-compose.yml) today; the HyperMQ image and service are planned.

### Principles

- The HyperMQ image is the same artifact in development and production. Only configuration differs.
- The Compose RabbitMQ is a development convenience. Nothing in the code may assume the broker is local or managed by Compose.
- The broker is selected only by `broker.url` in the config file. Topology is declared by HyperMQ (see [TopologyManager](../traits/topology.md)), so it works against any empty RabbitMQ.
- Development credentials are for local use only and are never reused elsewhere.

### Services

| Service | Image | Purpose | Host ports | Status |
|---|---|---|---|---|
| `rabbitmq` | `rabbitmq:4-management` | Local broker with management UI | `5673` (AMQP), `15673` (UI) | In `docker-compose.yml` |
| `postgres` | `postgres:17` | Local database for the publish outbox (Phase 8) | `5433` | In `docker-compose.yml`; used when `outbox.database_url` is set |
| `hypermq` | built from `Dockerfile` | The application | `8080` (HTTP API) | Planned |

The host ports `5673` and `15673` avoid clashing with a RabbitMQ already bound to `5672`/`15672`. Inside the Compose network the broker still listens on `5672`.

The default user is `hypermq` with password `change-me`; override with `RABBITMQ_USER` and `RABBITMQ_PASS` in a local `.env` file (read by Compose for the RabbitMQ container only).

The `postgres` service creates database `hypermq` for user `hypermq` with password `change-me`. Override with `POSTGRES_DB`, `POSTGRES_USER`, and `POSTGRES_PASSWORD` in `.env`.

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
- `broker.url` (required, `amqp://` or `amqps://`), `http_api` (`bind`, `max_body_bytes`, optional `bearer_token`), `topology`, and the optional `outbox` section are read today. Other sections in the example are ignored until their phase is implemented.
- `http_api` and every `topology` entry reject unknown keys, so typos fail at startup.
- Never log the broker URL or credentials. `broker.url` is held as a secret and redacted in `Debug` output.

### Topology

`topology` lists exchanges, queues, and bindings that HyperMQ declares at startup, in that order, in every role. See [docs/examples/config.yaml](../examples/config.yaml) and [topology.md](../traits/topology.md).

| Entity | Fields |
|---|---|
| `exchanges[]` | `name`, `kind` (`direct`, `fanout`, `topic`), `durable` (default `true`) |
| `queues[]` | `name`, `durable` (default `true`), `dead_letter_exchange`, `dead_letter_routing_key`, `message_ttl` (for example `30s`) |
| `bindings[]` | `queue`, `exchange`, `routing_key` (default empty) |

- Names use letters, digits, `-`, `_`, `.`, `:` (at most 255 bytes). Declared names cannot start with `amq.`. Duplicates and invalid values fail config load, before any connection is made.
- Declarations are idempotent: re-declaring identical settings succeeds. If an exchange or queue already exists with different settings, startup fails with the broker's reason; nothing is altered or deleted.
- A binding fails if its queue or exchange does not exist. Bindings may target exchanges not listed in the config (for example `amq.topic`).
- If `topology` is empty no connection is made at startup. If it is set and RabbitMQ is unreachable, startup fails, unlike publishing, which tolerates an unreachable broker.
- RabbitMQ 4 rejects non-durable, non-exclusive queues, so keep queues durable there.

### Outbox

Optional. When set, publishes that fail with `broker_unavailable` are stored in this database and relayed later (see [publish.md](../api/publish.md#outbox-fallback)). Without it, publishing is fail-fast. Unknown keys fail at startup.

| Key | Default | Notes |
|---|---|---|
| `database_url` | required | `postgres://` or `postgresql://` for PostgreSQL, `mysql://` for MySQL 8. Selects the backend. A secret: never logged. |
| `max_messages` | `100000` | At least 1. At this many stored rows, publishes return `503`. |
| `max_age` | `24h` | Non-zero. Older rows move to `outbox_dead`. |
| `cooldown` | `5s` | Non-zero. Pause between relay cycles. |
| `max_attempts` | `10` | At least 1. Attempts for an unroutable message before it is dead-lettered. |
| `retry_base_delay` | `1s` | Non-zero. First backoff delay for an unroutable message. |
| `retry_max_delay` | `5m` | At least `retry_base_delay`. |
| `degraded_after` | `5m` | Non-zero. `/health` reports `degraded` when the oldest row is older than this. |

Run `hypermq migrate` once before the first start.

CLI options (each also has an environment variable):

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `--config` | `HYPERMQ_CONFIG` | `config.yaml` | Path to the config file |
| `--role` | `HYPERMQ_ROLE` | `all` | `all`, `api`, or `consumers` (see [deployment.md](../core/deployment.md)) |

Subcommand:

| Command | Meaning |
|---|---|
| `hypermq migrate` | Creates or updates the outbox tables in `outbox.database_url`, then exits. Run it before the first start and after upgrades that add migrations. Logs and exits without changes when `outbox` is not set. |

Logging uses `RUST_LOG` (default `info`), for example `RUST_LOG=hypermq=debug,lapin=warn cargo run`.

## Commands

| Task | Command |
|---|---|
| Build | `cargo build` |
| Run | `cargo run` |
| Unit tests | `cargo test` |
| Integration tests | `HYPERMQ_TEST_RABBITMQ_URL=amqp://hypermq:change-me@localhost:5673 cargo test --test rabbitmq -- --ignored` |
| Outbox integration tests | `HYPERMQ_TEST_POSTGRES_URL=postgres://hypermq:change-me@localhost:5433/hypermq HYPERMQ_TEST_MYSQL_URL=mysql://hypermq:change-me@localhost:3306/hypermq cargo test --test outbox -- --ignored` |
| Migrate outbox schema | `cargo run -- migrate` |
| Build without the outbox | `cargo build --no-default-features` |
| Lint | `cargo clippy --all-targets -- -D warnings` |
| Format | `cargo fmt` |

Run format, lint, and unit tests before committing.

### Cargo features

| Feature | Default | Enables |
|---|---|---|
| `task-http` | yes | HTTP Task kind |
| `task-amqp` | yes | Publish-to-broker Task kind |
| `outbox` | via backends | Outbox store, relay, and `hypermq migrate` (SeaORM) |
| `outbox-postgres` | yes | PostgreSQL backend (implies `outbox`) |
| `outbox-mysql` | yes | MySQL backend (implies `outbox`) |

A build without the outbox (`--no-default-features`) fails at startup if the config has an `outbox` section.

## Code map

| Path | Responsibility |
|---|---|
| `src/main.rs` | Parses CLI, loads config, wires components, starts the HTTP server. The only place that picks concrete types and the role. |
| `src/lib.rs` | Exposes the modules so tests and the binary share code. |
| `src/config/` | Config model, YAML loader, validation, `Role`. |
| `src/error.rs` | Shared `Error` and `ConfigError`. No dependency on other project modules. |
| `src/api/` | Axum router, handlers, request validation, `ApiError`, JSON extractor. |
| `src/broker/mod.rs` | `Publisher` trait, its request/error types, and shared name validation. |
| `src/broker/topology.rs` | Topology config types, validation, `TopologyManager` trait, and `apply`. |
| `src/broker/rabbitmq/` | `RabbitPublisher` and `RabbitTopology`. The only code that imports `lapin`. |
| `src/outbox/` | Publish fallback: `OutboxStore` trait, `OutboxPublisher`, relay loop, `RelayPolicy`. No ORM imports. |
| `src/outbox/sea/` | SeaORM store, entities, and migrations for PostgreSQL and MySQL. The only code that imports the ORM. |
| `tests/rabbitmq.rs` | Integration tests that need RabbitMQ. |
| `tests/outbox.rs` | Integration tests that need PostgreSQL and MySQL. |

Dependency rules:
- `api` talks to the broker only through the `Publisher` trait.
- Only `broker/rabbitmq` imports the AMQP client.
- The consumer engine (later phases) depends on traits only, never on RabbitMQ or a concrete Task kind.
- Only `outbox/sea` imports the ORM. `outbox` uses the `Publisher` trait and never names RabbitMQ.

## Request flow for `POST /publish`

```mermaid
sequenceDiagram
    participant C as Client
    participant H as api::publish
    participant O as OutboxPublisher
    participant P as Publisher (RabbitPublisher)
    participant R as RabbitMQ
    participant D as Outbox database
    C->>H: POST /publish (JSON)
    H->>H: ApiJson parse, PublishBody::into_request (validate, bound)
    H->>O: publish(PublishRequest)
    O->>P: publish(PublishRequest)
    P->>R: basic_publish (mandatory, persistent)
    R-->>P: confirm (ack, ack+return, nack)
    P-->>O: Ok or PublishError
    alt broker unavailable and outbox configured
        O->>D: insert row
        O-->>H: Queued (202) or Unavailable (503)
    else published, or any other error
        O-->>H: Published (200) or PublishError
    end
    H-->>C: 200, 202, or stable JSON error
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
- Outbox tests (`tests/outbox.rs`) run one round trip per backend and need `HYPERMQ_TEST_POSTGRES_URL` and `HYPERMQ_TEST_MYSQL_URL`. MySQL is not in Compose; for a throwaway MySQL 8: `docker run -d --rm --name hypermq-test-mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=root-change-me -e MYSQL_USER=hypermq -e MYSQL_PASSWORD=change-me -e MYSQL_DATABASE=hypermq mysql:8`. CI runs both in the `outbox` job.

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
| `POST /publish` returns `503 broker_unavailable` | RabbitMQ is down and no outbox is configured, or the outbox is full or its database is unreachable. Check `hypermq_outbox_depth` and `outbox.database_url`. Without an outbox, the client should retry. |
| Startup fails with `outbox database is not ready; run hypermq migrate first` | The schema is missing or the database is unreachable. Run `hypermq migrate` with the same config, and check `database_url`. |
| Startup fails when `outbox` is set but the binary has no backend feature | Build with `outbox-postgres` or `outbox-mysql` (both are default). |
| `/health` reports `degraded` | The outbox is growing, its oldest row is older than `degraded_after`, or a database call failed. Check `hypermq_outbox_depth` and the relay logs. |
| Rows accumulate in `outbox_dead` | Read `reason` and `last_error`, fix the exchange or routing key, then delete the rows. |
| `POST /publish` returns `422 unroutable` | No queue received the message: the queue does not exist, or the exchange has no matching binding. |
| `POST /publish` returns `404 target_not_found` | The exchange does not exist. Declare it under `topology`. |
| Startup fails with `topology: ... conflicts with an existing declaration` | The exchange or queue exists with different settings (for example another `message_ttl`). Match the existing settings, or delete the entity on the broker if it is disposable. |
| Startup fails with `topology: ... refers to something that does not exist` | A binding names a queue or exchange that is neither in `topology` nor on the broker. |
| Startup fails with `topology: broker unavailable` | RabbitMQ is unreachable. Start it or fix `broker.url`. |
| `cannot read config file` | No `config.yaml` in the working directory; pass `--config` or create one. |
| Integration test fails with `transient_nonexcl_queues` | Declare the queue as durable. |

## Open questions

- Whether to add a small local downstream service to Compose for testing the HTTP Task.
- Whether to pin the RabbitMQ image to an exact version to match production.
- Whether a pre-provisioned definitions file should load users, vhosts, or queues, or whether HyperMQ topology declaration alone is enough.
