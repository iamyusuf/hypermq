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
