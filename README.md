# HyperMQ

HyperMQ is an HTTP wrapper around RabbitMQ. Clients publish messages over HTTP, and operators configure consumers that run a **Task** for each message received from RabbitMQ (an HTTP call to a downstream service first; SMS, email, publishing to another broker, and Kafka later).

> **Status:** early scaffold. The design is documented in [docs/](docs/); the code is not implemented yet.

## Features (planned)

- **Publish over HTTP:** no AMQP library needed on the client side.
- **Consumers from configuration:** declare a queue, a Task, and a retry policy instead of writing code.
- **Pluggable Tasks:** the consumer engine only knows the `Task` interface, so new kinds can be added without changing it. The first Task kind is an HTTP call.
- **Topology management:** exchanges, queues, and bindings declared from configuration.
- **Bounded retries and dead-lettering:** ack only after the Task succeeds; no infinite requeue.
- **One binary, three roles:** run `all`, `api`, or `consumers` in one container or split them later.

```mermaid
flowchart LR
    Client -->|HTTP publish| HyperMQ
    HyperMQ -->|AMQP| RabbitMQ
    RabbitMQ -->|AMQP deliver| Consumer[Consumer engine]
    Consumer -->|execute| Task{{Task}}
    Task --> Http[HTTP call]
```

## Configuration

Consumers, topology, and Task settings are declared in YAML. Secrets are referenced as `${VAR}` and read from the environment, not stored in the file. The format is not final; see the [example config](docs/examples/config.yaml).

## Getting started

Requires a recent stable Rust toolchain (edition 2024).

```sh
cargo build
cargo run
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt
```

A running RabbitMQ will be required once publishing and consuming are implemented.

## Documentation

- [Roadmap](docs/roadmap/roadmap.md): phase-by-phase plan.
- [Traits](docs/traits/README.md): `Task`, `Broker`, `Publisher`, `TopologyManager`, `RetryPolicy`.
- [Consumer manager](docs/core/consumer-manager.md): consumer lifecycle and loop.
- [Deployment](docs/core/deployment.md): roles and container layout.
- [Project structure](docs/core/project-structure.md): planned modules and dependency rules.
- [Development environment](docs/dev/devenv.md): Docker and Compose setup with a local RabbitMQ.
- [AGENTS.md](AGENTS.md): conventions for AI coding agents and open design questions.
