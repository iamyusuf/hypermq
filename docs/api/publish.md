# Publish API

`POST /publish` publishes one message to RabbitMQ and waits for the broker confirm before answering. It is served by the `all` and `api` roles; the `consumers` role does not serve it.

## Request

`Content-Type: application/json`

```json
{
  "exchange": "orders",
  "routing_key": "order.created",
  "headers": { "source": "web" },
  "message_id": "order-42-created",
  "payload": { "id": 42 }
}
```

| Field | Required | Rules |
|---|---|---|
| `exchange` | one of `exchange` / `queue` | 1-255 characters of letters, digits, `-`, `_`, `.`, `:`. The exchange must already exist. |
| `queue` | one of `exchange` / `queue` | Same rules. Published through the default exchange, so the queue must already exist. |
| `routing_key` | no | Up to 255 bytes, no control characters. Not allowed together with `queue`. |
| `headers` | no | String map, at most 32 entries; names 1-128 visible ASCII characters, values up to 1024 bytes. |
| `message_id` | no | 1-255 visible ASCII characters. Generated (UUID v4) when omitted. Becomes the AMQP `message_id`, the stable id consumers use for idempotency. |
| `payload` | yes | Any JSON value. Published as compact JSON with `content_type: application/json`. |

Unknown fields are rejected. The whole request body is limited by `http_api.max_body_bytes` (default 1 MiB).

Messages are published persistent (`delivery_mode: 2`) with `mandatory` set, so a message that no queue receives is reported instead of silently dropped.

## Responses

Success: `200 OK`

```json
{ "message_id": "order-42-created" }
```

Errors share one shape:

```json
{ "error": { "code": "unroutable", "message": "message was not routed to any queue" } }
```

| Status | `code` | Meaning |
|---|---|---|
| 400 | `invalid_request` | Malformed JSON or a field rule above was violated. |
| 404 | `target_not_found` | The exchange does not exist. |
| 413 | `payload_too_large` | Body exceeds `max_body_bytes`. |
| 422 | `unroutable` | The broker accepted the message but no queue matched (no binding, or the queue does not exist). |
| 502 | `publish_rejected` | The broker negatively acknowledged the message. |
| 503 | `broker_unavailable` | Connection, channel, or confirm failure. Includes `Retry-After: 5`; the client should retry. |

Publishing is fail-fast: HyperMQ does not buffer messages while RabbitMQ is down (see the outbox in roadmap Phase 8). Retries are safe when the client sends the same `message_id`, because consumers can deduplicate on it; delivery is at-least-once.

## Broker connection

- One connection and one confirm-mode channel are shared by all requests.
- The connection is opened at startup when the role serves the API. If RabbitMQ is unreachable then, the service still starts and logs a warning.
- After a failure the next request reconnects, or reopens only the channel when the broker closed just the channel (for example after publishing to a missing exchange).
- Connect and confirm timeouts are fixed at 5 seconds for now.
- The broker URL and credentials are never logged.
