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
- Tasks never ack/nack and never touch the broker client; they only return an outcome.
- Secrets come from configuration or environment and are never included in `reason` or logs.
- Outbound destinations are validated at `build` time where possible (SSRF).
- Each kind lives in its own module under `tasks/`, optionally behind a Cargo feature. `tasks/http` is the only initial kind.

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
