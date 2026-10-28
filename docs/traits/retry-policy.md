# RetryPolicy

Turns a `TaskOutcome` and the delivery attempt into an action for the consumer engine. Keeps ack/nack rules out of both the engine loop and the Tasks.

```rust
pub trait RetryPolicy: Send + Sync {
    fn decide(&self, outcome: &TaskOutcome, attempt: u32) -> Decision;
}

pub enum Decision {
    Ack,                          // Success
    Retry { delay: Duration },    // Retryable within the attempt limit
    DeadLetter,                   // Retryable but attempts exhausted, or Permanent
}
```

## Default behavior

| Outcome | Condition | Decision |
|---|---|---|
| `Success` | always | `Ack` |
| `Retryable` | `attempt < max_attempts` | `Retry` with backoff, or the Task's `retry_after` if given |
| `Retryable` | attempts exhausted | `DeadLetter` |
| `Permanent` | always | `DeadLetter` |

## Contract

- Pure and synchronous: no I/O, easy to unit test.
- Always bounded: `max_attempts` is required; there is no infinite requeue.
- Configurable per consumer (max attempts, base delay, backoff, cap).
