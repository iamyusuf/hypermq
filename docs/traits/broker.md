# Broker, Delivery and Publisher

These traits hide the AMQP client (for example lapin) so the HTTP API and consumer engine can be tested without RabbitMQ.

## Publish side

```rust
#[async_trait]
pub trait Publisher: Send + Sync {
    /// Publishes and waits for the broker confirm.
    async fn publish(&self, request: PublishRequest) -> Result<(), PublishError>;
}

pub struct PublishRequest {
    pub target: PublishTarget,   // Exchange(name) or Queue(name)
    pub routing_key: String,
    pub message_id: String,      // stable id for consumer-side idempotency
    pub content_type: String,
    pub headers: BTreeMap<String, String>,
    pub body: Bytes,
}

pub enum PublishError { Unavailable(String), Unroutable, TargetNotFound, Rejected }
```

Implemented in `src/broker/mod.rs`; the RabbitMQ implementation is `RabbitPublisher`. See [the publish API](../api/publish.md).

`PublishRequest` is validated and size-bounded before it reaches the `Publisher`; the HTTP handler stays thin.

## Consume side

```rust
#[async_trait]
pub trait Broker: Send + Sync {
    /// Starts consuming a queue with a prefetch limit; yields deliveries until cancelled or the connection drops.
    async fn consume(&self, queue: &str, prefetch: u16) -> Result<BoxStream<'static, Result<Box<dyn Delivery>, BrokerError>>, BrokerError>;
}

#[async_trait]
pub trait Delivery: Send {
    fn message(&self) -> &Message;

    /// Positive acknowledgement; call only after the Task succeeded.
    async fn ack(self: Box<Self>) -> Result<(), BrokerError>;

    /// Reject without requeue so the broker dead-letters the message.
    async fn reject(self: Box<Self>) -> Result<(), BrokerError>;

    /// Republish for a delayed retry with an incremented attempt count, then ack the original.
    async fn retry(self: Box<Self>, delay: Duration) -> Result<(), BrokerError>;
}
```

## Contract

- Ack/nack happens only through `Delivery`, and only from the consumer engine.
- Redelivery must never loop forever: the attempt count travels with the message and `RetryPolicy` bounds it.
- Reconnect handling lives in the implementation; the stream yields an error and the engine resubscribes.
- Credentials and the broker URL are never logged.
- The RabbitMQ implementation is the only code that imports the AMQP client.
