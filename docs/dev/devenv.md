# Development environment

The project is containerized. A Docker Compose file provides a local RabbitMQ for development only; production is expected to use a dedicated RabbitMQ that HyperMQ reaches through configuration. These files are planned, not created yet.

## Principles

- The HyperMQ image is the same artifact in development and production. Only configuration differs.
- The Compose RabbitMQ is a development convenience. Nothing in the code may assume the broker is local or managed by Compose.
- The broker is selected only by `RABBITMQ_URL`. Topology is declared by HyperMQ (see [TopologyManager](../traits/topology.md)), so it works against any empty RabbitMQ.
- Development credentials are for local use only and are never reused elsewhere.

## Services

| Service | Image | Purpose | Ports |
|---|---|---|---|
| `rabbitmq` | `rabbitmq:4-management` | Local broker with management UI | `5672` (AMQP), `15672` (UI) |
| `hypermq` | built from `Dockerfile` | The application | `8080` (HTTP API) |

```mermaid
flowchart LR
    Dev[Developer] -->|HTTP :8080| H[hypermq container]
    Dev -->|UI :15672| R[(rabbitmq container)]
    H -->|AMQP rabbitmq:5672| R
```

## Two ways to work

1. **App on the host, RabbitMQ in Docker** (fast iteration). Start only the broker, then run the app with `cargo run`; it connects to `localhost:5672`.
2. **Everything in Docker** (closest to production). Compose builds the image and runs both services; the app connects to `rabbitmq:5672` over the Compose network.

## Compose file (sketch)

```yaml
services:
  rabbitmq:
    image: rabbitmq:4-management
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      RABBITMQ_DEFAULT_USER: ${RABBITMQ_USER}
      RABBITMQ_DEFAULT_PASS: ${RABBITMQ_PASS}
    volumes:
      - rabbitmq-data:/var/lib/rabbitmq
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 10s
      timeout: 5s
      retries: 10

  hypermq:
    build: .
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      RABBITMQ_URL: amqp://${RABBITMQ_USER}:${RABBITMQ_PASS}@rabbitmq:5672
    ports:
      - "8080:8080"
    volumes:
      - ./docs/examples/config.yaml:/etc/hypermq/config.yaml:ro
    command: ["--role", "all", "--config", "/etc/hypermq/config.yaml"]

volumes:
  rabbitmq-data:
```

- The default `guest` user only works from localhost inside the container, so set an explicit user and password for the app to connect over the Compose network.
- The healthcheck lets `hypermq` wait until the broker accepts connections instead of failing at startup.
- The `--role` and `--config` flags are the planned CLI (see [deployment.md](../core/deployment.md)); adjust to the real options when implemented.

## Dockerfile (sketch)

- Multi-stage build: a Rust builder stage compiles `cargo build --release`, and a slim runtime stage copies only the binary.
- Cache dependencies in a separate layer so source changes do not rebuild them.
- Run as a non-root user and do not bake config or secrets into the image.
- Task features (for example `task-http`) are chosen at build time through build arguments.

## Configuration and secrets

- Compose reads `RABBITMQ_USER` and `RABBITMQ_PASS` from a local `.env` file.
- Commit a `.env.example` with placeholder values; `.env` is gitignored.
- The app reads `RABBITMQ_URL` and other secrets from the environment, following the `${VAR}` rule in the [example config](../examples/config.yaml).
- Never log the broker URL or credentials.

## Production differences

| | Development | Production |
|---|---|---|
| RabbitMQ | Compose service, throwaway data volume | Dedicated, managed separately |
| Connection | `amqp://` to `rabbitmq:5672` | `amqps://` (TLS) to the dedicated host |
| Credentials | Local `.env` values | Secret store or orchestrator secrets |
| Compose file | Used | Not used; the image is deployed by the production orchestrator |
| Management UI | Exposed on `15672` | Not exposed publicly |

## Common commands

```sh
docker compose up -d rabbitmq      # broker only, run the app with cargo
docker compose up --build          # broker and app in containers
docker compose logs -f hypermq     # follow app logs
docker compose down                # stop, keep data
docker compose down -v             # stop and delete the RabbitMQ data volume
```

## Testing against the broker

- Unit tests do not need Docker.
- Integration tests that need RabbitMQ run against the Compose broker and are kept separate from unit tests (see [project-structure.md](../core/project-structure.md)).
- CI starts RabbitMQ as a service container or through Compose before running the integration suite (roadmap Phase 5).

## Roadmap placement

- Phase 0: `Dockerfile`, Compose file with RabbitMQ, `.env.example`.
- Phase 1 onward: the Compose broker is used to verify publishing and consuming end to end.
- Phase 5: production image hardening, CI integration tests, and documented production deployment.

## Open questions

- Whether to add a small local downstream service to Compose for testing the HTTP Task.
- Whether to pin the RabbitMQ image to an exact version to match production.
- Whether a pre-provisioned definitions file should load users, vhosts, or queues, or whether HyperMQ topology declaration alone is enough.
