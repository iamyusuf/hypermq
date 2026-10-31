# Task and TaskFactory

A `Task` is the unit of work performed for one consumed message (HTTP call, SMS, email, publish to another broker, ...). The consumer engine knows only this trait.

```rust
#[async_trait]
pub trait Task: Send + Sync {
    /// Runs the work for one message. Must be idempotency-aware: it may run twice for the same message.
    async fn execute(&self, message: &Message) -> TaskOutcome;
}

pub enum TaskOutcome {
    /// Work done; the engine acks.
    Success,
    /// Transient failure; the engine retries within the policy, then dead-letters.
    Retryable { reason: String, retry_after: Option<Duration> },
    /// Will never succeed; the engine rejects or dead-letters without retry.
    Permanent { reason: String },
}
```

## Construction

Task kinds are selected by the `type` tag in consumer configuration and built through a registry.

```rust
pub trait TaskFactory: Send + Sync {
    /// The `type` tag this factory handles, for example "http".
    fn kind(&self) -> &'static str;

    /// Validates kind-specific settings and builds the Task. Called at config load, never at message time.
    fn build(&self, settings: serde_json::Value) -> Result<Arc<dyn Task>, TaskConfigError>;
}

pub struct TaskRegistry { /* kind -> Arc<dyn TaskFactory> */ }

impl TaskRegistry {
    pub fn register(&mut self, factory: Arc<dyn TaskFactory>);
    /// Fails with `TaskConfigError::UnknownKind` for an unregistered `type`.
    pub fn build(&self, kind: &str, settings: serde_json::Value) -> Result<Arc<dyn Task>, TaskConfigError>;
}
```

## Contract

- The engine enforces the timeout around `execute`; a timeout counts as `Retryable`. A Task may also apply its own tighter timeouts.
- Tasks never ack/nack and never touch the consumed message's channel; they only return an outcome. A Task that publishes to a broker uses the `Publisher` trait and receives its concrete publisher from `main`.
- Secrets come from configuration or environment and are never included in `reason` or logs.
- Outbound destinations are validated at `build` time where possible (SSRF).
- Each kind lives in its own module under `tasks/`, optionally behind a Cargo feature. The current kinds are `tasks/http`, `tasks/amqp`, and `tasks/email`.

## HTTP Task (`type: http`)

Behind the `task-http` Cargo feature, which is on by default. Settings:

| Key | Required | Default | Notes |
| --- | --- | --- | --- |
| `url` | yes | | `http` or `https`; no credentials in the URL; host must be in `allowed_hosts`. |
| `allowed_hosts` | yes | | Non-empty list of hosts the URL may target. Matched case-insensitively. This is the SSRF control. |
| `method` | no | `POST` | Any valid HTTP method. |
| `headers` | no | empty | Static headers, for example `Authorization`. Values are never logged or shown in Debug output. |
| `idempotency_header` | no | none | Header set to the stable message id on every attempt. |

The message body is sent as the request body. The message `content-type` header is forwarded when present. Unknown keys fail at config load.

Outcome mapping:

- 2xx: `Success`.
- 429 and 5xx: `Retryable`.
- Other statuses, including 3xx: `Permanent`. Redirects are not followed.
- Timeout or connection failure: `Retryable`. Other transport errors are also `Retryable`.
- Invalid header value built from the message (for example a message id that is not a valid header value): `Permanent`.

Failure reasons never include the URL.

## AMQP Task (`type: amqp`)

Behind the `task-amqp` Cargo feature, which is on by default. Publishes the message to an exchange or queue on another RabbitMQ broker, using publisher confirms and mandatory publishing. Settings:

| Key | Required | Default | Notes |
| --- | --- | --- | --- |
| `url` | yes | | `amqp` or `amqps`. Credentials are allowed. The host must be in `allowed_hosts`. |
| `allowed_hosts` | yes | | Non-empty list of hosts the URL may target. Matched case-insensitively. This is the SSRF control. |
| `exchange` | one of | | Publish to this exchange with `routing_key`. |
| `queue` | one of | | Publish to this queue through the default exchange. |
| `routing_key` | no | empty | Exchange targets only. Setting it on a `queue` target fails at config load. |

Set exactly one of `exchange` or `queue`. Names and routing keys use the same validation as `POST /publish`. Unknown keys fail at config load.

The message body is the publish body. The `content-type` header is sent as the message property, defaulting to `application/octet-stream`. The message id is sent as the `message_id` property. Other message headers are forwarded, except the internal `x-hypermq-attempt` header.

Outcome mapping:

- Published and confirmed: `Success`.
- Connection, channel, or confirm failure: `Retryable`.
- Unroutable (no queue received the message) or rejected by the broker: `Retryable`.
- Target exchange or queue does not exist: `Permanent`.

Failure reasons are fixed strings. They never include the URL or broker error text, because that text can contain the target address.

## Email Task (`type: email`)

Behind the `task-email` Cargo feature, which is on by default. Sends the message as one email through an SMTP server (lettre, with rustls). Settings:

| Key | Required | Default | Notes |
| --- | --- | --- | --- |
| `host` | yes | | SMTP server host name. No scheme, port, or path. Must be in `allowed_hosts`. |
| `allowed_hosts` | yes | | Non-empty list of hosts the SMTP server may be. Matched case-insensitively. This is the SSRF control. |
| `port` | no | `587`, `465`, or `25` | Depends on `tls`: `starttls` uses 587, `tls` uses 465, `plaintext` uses 25. |
| `tls` | no | `starttls` | `starttls` requires an upgrade before any credentials or message are sent. `tls` is implicit TLS (SMTPS). `plaintext` has no TLS and is for trusted networks only. |
| `username` | no | none | Must be set together with `password`. Refused with `tls: plaintext`. |
| `password` | no | none | Secret. Never logged or shown in Debug output. |
| `from` | yes | | Sender mailbox, for example `HyperMQ <orders@example.com>`. |
| `to` | yes | | Non-empty list of recipient mailboxes. Every message goes to all of them. |
| `subject` | yes | | Static subject. Non-empty, no control characters. |

Set the settings at config load: invalid mailboxes, an empty `to`, an invalid subject, a host outside `allowed_hosts`, and a username without a password all fail there. Unknown keys fail at config load.

Message mapping:

- The message body is the email body. It must be valid UTF-8.
- The message `content-type` header selects `text/plain` (default) or `text/html`. Any other value is `Permanent`.
- The message id becomes `Message-ID: <id@domain>`, where `domain` is the domain of `from`. The id may contain only ASCII letters, digits, `.`, `-`, and `_`; any other id is `Permanent`. Downstream systems can use this header to deduplicate.

Outcome mapping:

- Accepted by the SMTP server: `Success`.
- SMTP 5xx reply or authentication refused: `Permanent`.
- SMTP 4xx reply: `Retryable`.
- Timeout, connection failure, TLS failure, or other transport error: `Retryable`.

Failure reasons are fixed strings. They never include the host, addresses, or the SMTP reply text, because the reply can name a recipient.
