# Publish API

`POST /publish` publishes one message to RabbitMQ and waits for the broker confirm before answering. It is served by the `all` and `api` roles; the `consumers` role does not serve it.

## Request

`Content-Type: application/json`

When `http_api.bearer_token` is set, the request must also send `Authorization: Bearer <token>`. The scheme is case-insensitive. `GET /health` stays open either way. When no token is configured, `/publish` accepts any client and a warning is logged at startup.

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

Success: `200 OK` when RabbitMQ confirmed the message, or `202 Accepted` when it was stored in the outbox (see below). Both return the same body:

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
| 401 | `unauthorized` | A bearer token is configured and the `Authorization` header is missing or wrong. Includes `WWW-Authenticate: Bearer`. |
| 404 | `target_not_found` | The exchange does not exist. |
| 413 | `payload_too_large` | Body exceeds `max_body_bytes`. |
| 422 | `unroutable` | The broker accepted the message but no queue matched (no binding, or the queue does not exist). |
| 502 | `publish_rejected` | The broker negatively acknowledged the message. |
| 503 | `broker_unavailable` | Connection, channel, or confirm failure, and no outbox is configured. With an outbox, the response is `202` instead. Also returned when the outbox is full or its database cannot be used. Includes `Retry-After: 5`; the client should retry. |

## Outbox fallback

When `outbox.database_url` is set, a publish that fails with `broker_unavailable` is stored in the outbox database and answered `202 Accepted`. A relay worker in the `api` and `all` roles publishes stored messages once RabbitMQ is back. It runs every `outbox.cooldown` (5 seconds by default). Only failed publishes are stored; a publish that RabbitMQ confirms never writes to the database. Without `outbox.database_url`, a publish fails with `503` and nothing is buffered.

- Errors other than `broker_unavailable` (`400`, `404`, `422`, `502`) are not stored.
- Delivery is at-least-once. A stored message can be published more than once, and a client that retries after a `202` creates another copy with the same `message_id`. Consumers must deduplicate on `message_id`.
- Ordering: a stored message is published only after earlier stored messages with the same exchange and routing key. A row that keeps failing blocks only its own key. A message published directly while RabbitMQ is healthy can overtake stored messages with the same key. With several HyperMQ replicas, per-key order is not guaranteed.
- Limits: at `outbox.max_messages` stored rows, the API returns `503` with `Retry-After`. Rows older than `outbox.max_age` move to the dead-letter table, as do rows the broker can never accept (`target_not_found`, `publish_rejected`, or an unroutable message after `outbox.max_attempts`). Dead-letter rows (`outbox_dead`) stay until an operator removes them.

## Broker connection

- One connection and one confirm-mode channel are shared by all requests.
- The connection is opened at startup when the role serves the API. If RabbitMQ is unreachable then, the service still starts and logs a warning.
- After a failure the next request reconnects, or reopens only the channel when the broker closed just the channel (for example after publishing to a missing exchange).
- Connect and confirm timeouts are fixed at 5 seconds for now.
- The broker URL and credentials are never logged.
