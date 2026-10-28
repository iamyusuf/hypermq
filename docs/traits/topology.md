# TopologyManager

Declares broker topology from configuration at startup and, optionally, through management endpoints.

```rust
#[async_trait]
pub trait TopologyManager: Send + Sync {
    async fn declare_exchange(&self, spec: &ExchangeSpec) -> Result<(), TopologyError>;
    async fn declare_queue(&self, spec: &QueueSpec) -> Result<(), TopologyError>;
    async fn bind(&self, spec: &BindingSpec) -> Result<(), TopologyError>;
}

pub struct ExchangeSpec { pub name: String, pub kind: ExchangeKind, pub durable: bool }

pub struct QueueSpec {
    pub name: String,
    pub durable: bool,
    pub dead_letter_exchange: Option<String>,   // target for rejected and exhausted messages
    pub message_ttl: Option<Duration>,          // used for delayed-retry queues
}

pub struct BindingSpec { pub queue: String, pub exchange: String, pub routing_key: String }
```

## Contract

- Declarations are idempotent; re-declaring an identical spec succeeds, a conflicting one fails with a clear error.
- Names and routing keys are validated before reaching the broker.
- Dead-letter and retry queues needed by `RetryPolicy` are declared here, not by the consumer engine.
