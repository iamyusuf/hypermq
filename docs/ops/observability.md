# Observability

HyperMQ exposes Prometheus-format metrics and structured logs. Both are local to the process: no exporter or collector is bundled.

## Metrics

`GET /metrics` returns Prometheus text exposition format (version 0.0.4). It is served by every role. When `http_api.bearer_token` is set, the endpoint requires the same `Authorization: Bearer <token>` header as `POST /publish`. `/health` stays open.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `hypermq_messages_published_total` | counter | `result` = `accepted`, `failed` | Publish attempts that reached the broker. Requests rejected by validation are not counted. |
| `hypermq_deliveries_total` | counter | `consumer` | Deliveries received from the queue. |
| `hypermq_task_outcomes_total` | counter | `consumer`, `outcome` = `success`, `retryable`, `permanent` | Outcomes returned by `Task::execute`. Timeouts count as `retryable`. |
| `hypermq_settlements_total` | counter | `consumer`, `action` = `ack`, `retry`, `dead_letter` | Settlements the broker confirmed. A failed settlement is not counted. |
| `hypermq_task_duration_seconds` | histogram | `consumer` | Time spent in `Task::execute`, including timeouts. Buckets run from 5 ms to 30 s. |

Consumers appear once they have received a delivery or at startup, depending on the series. Every family is declared even when no series exists yet.

Example scrape configuration:

```yaml
scrape_configs:
  - job_name: hypermq
    metrics_path: /metrics
    authorization:
      credentials: <bearer token>
    static_configs:
      - targets: ["hypermq:8080"]
```

Omit `authorization` when no bearer token is configured.

## Logs

Logs are written to stderr. Two options control the output:

| Setting | Values | Default | Notes |
|---|---|---|---|
| `--log-format` / `HYPERMQ_LOG_FORMAT` | `text`, `json` | `text` | The container image sets `json`. |
| `RUST_LOG` | a `tracing` filter, for example `hypermq=debug` | `info` | |

Delivery settlements are logged as one event, `delivery settled`, with these fields:

- `consumer`: the consumer name from configuration.
- `message_id`: the stable id assigned at publish time. Use it to correlate retries and Task calls.
- `attempt`: the delivery attempt, starting at 1.
- `outcome`: `success`, `retryable`, or `permanent`.
- `action`: `ack`, `retry`, or `dead_letter`.
- `duration_ms`: time spent in `Task::execute`.
- `reason`: the Task's reason, for non-success outcomes.

Acks log at `debug`. Retries and dead-letters log at `info`. A settlement the broker rejected logs at `error` as `failed to settle delivery`.

Message bodies, headers, and credentials are never logged.
