# TopologyManager

Declares broker topology from configuration at startup. Management endpoints for the same operations are not implemented.

```rust
#[async_trait]
pub trait TopologyManager: Send + Sync {
    async fn declare_exchange(&self, spec: &ExchangeSpec) -> Result<(), TopologyError>;
    async fn declare_queue(&self, spec: &QueueSpec) -> Result<(), TopologyError>;
    async fn bind(&self, spec: &BindingSpec) -> Result<(), TopologyError>;
}

pub struct ExchangeSpec { pub name: String, pub kind: ExchangeKind, pub durable: bool } // Direct | Fanout | Topic

pub struct QueueSpec {
    pub name: String,
    pub durable: bool,
    pub dead_letter_exchange: Option<String>,      // target for rejected and exhausted messages
    pub dead_letter_routing_key: Option<String>,   // requires dead_letter_exchange
    pub message_ttl: Option<Duration>,             // used for delayed-retry queues
}

pub struct BindingSpec { pub queue: String, pub exchange: String, pub routing_key: String }

pub enum TopologyError { Unavailable(String), Conflict { .. }, NotFound { .. }, Refused { .. } }
```

The specs deserialize directly from the `topology` config section (see the [developer guide](../dev/developer-guide.md#topology)). `topology::apply` declares exchanges, then queues, then bindings and stops at the first error. `RabbitTopology` implements the trait over its own connection and a fresh channel per declaration, because the broker closes the channel when a declaration fails.

## Contract

- Declarations are idempotent; re-declaring an identical spec succeeds, a conflicting one fails with a clear error (`Conflict`), and a binding to something missing fails with `NotFound`.
- Names, routing keys, TTLs, and duplicates are validated when the config loads, before anything reaches the broker.
- Dead-letter and retry queues needed by `RetryPolicy` are declared here, not by the consumer engine.
- Existing entities are never altered or deleted.
