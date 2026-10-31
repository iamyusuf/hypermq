//! Publisher that stores a message in the outbox when the broker is unavailable.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;

use super::OutboxStore;
use crate::broker::{Accepted, PublishError, PublishRequest, Publisher};
use crate::metrics::Metrics;

/// Tries the broker first. Stores the message only when the broker is unavailable, so a healthy
/// broker never sees the outbox. Other broker errors pass through unchanged.
pub struct OutboxPublisher {
    broker: Arc<dyn Publisher>,
    store: Arc<dyn OutboxStore>,
    max_messages: u64,
    metrics: Arc<Metrics>,
}

impl OutboxPublisher {
    pub fn new(
        broker: Arc<dyn Publisher>,
        store: Arc<dyn OutboxStore>,
        max_messages: u64,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            broker,
            store,
            max_messages,
            metrics,
        }
    }

    async fn store(&self, request: &PublishRequest) -> Result<Accepted, PublishError> {
        match self
            .store
            .insert(request, Utc::now(), self.max_messages)
            .await
        {
            Ok(true) => {
                self.metrics.outbox.record_queued();
                Ok(Accepted::Queued)
            }
            Ok(false) => {
                tracing::warn!(message_id = %request.message_id, "outbox is full; rejecting publish");
                Err(PublishError::Unavailable("outbox is full".to_owned()))
            }
            Err(err) => {
                tracing::error!(message_id = %request.message_id, %err, "failed to store message in outbox");
                self.metrics.outbox.record_store_error();
                Err(PublishError::Unavailable("outbox unavailable".to_owned()))
            }
        }
    }
}

#[async_trait]
impl Publisher for OutboxPublisher {
    async fn publish(&self, request: PublishRequest) -> Result<Accepted, PublishError> {
        match self.broker.publish(request.clone()).await {
            Err(PublishError::Unavailable(reason)) => {
                tracing::warn!(message_id = %request.message_id, %reason, "broker unavailable; storing message in outbox");
                self.store(&request).await
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::PublishTarget;
    use crate::outbox::testing::{MemoryStore, ScriptedPublisher, request};

    fn queue_request(id: &str) -> PublishRequest {
        request(id, PublishTarget::Queue("orders".to_owned()), "")
    }

    fn fixture(
        script: Vec<Result<Accepted, PublishError>>,
        max_messages: u64,
    ) -> (OutboxPublisher, Arc<MemoryStore>, Arc<Metrics>) {
        let store = Arc::new(MemoryStore::default());
        let metrics = Arc::new(Metrics::default());
        let publisher = OutboxPublisher::new(
            Arc::new(ScriptedPublisher::new(script)),
            store.clone(),
            max_messages,
            metrics.clone(),
        );
        (publisher, store, metrics)
    }

    #[tokio::test]
    async fn healthy_broker_publishes_without_storing() {
        let (publisher, store, _) = fixture(vec![], 10);

        let accepted = publisher.publish(queue_request("m1")).await;

        assert_eq!(accepted.unwrap(), Accepted::Published);
        assert_eq!(store.depth(), 0);
    }

    #[tokio::test]
    async fn unavailable_broker_stores_the_message() {
        let (publisher, store, metrics) =
            fixture(vec![Err(PublishError::Unavailable("down".to_owned()))], 10);

        let accepted = publisher.publish(queue_request("m1")).await;

        assert_eq!(accepted.unwrap(), Accepted::Queued);
        assert_eq!(store.depth(), 1);
        assert!(metrics.render().contains("hypermq_outbox_queued_total 1"));
    }

    #[tokio::test]
    async fn rejected_message_is_not_stored() {
        let (publisher, store, _) = fixture(vec![Err(PublishError::Rejected)], 10);

        let result = publisher.publish(queue_request("m1")).await;

        assert!(matches!(result, Err(PublishError::Rejected)));
        assert_eq!(store.depth(), 0);
    }

    #[tokio::test]
    async fn full_outbox_returns_unavailable() {
        let (publisher, store, _) =
            fixture(vec![Err(PublishError::Unavailable("down".to_owned()))], 0);

        let result = publisher.publish(queue_request("m1")).await;

        assert!(matches!(result, Err(PublishError::Unavailable(_))));
        assert_eq!(store.depth(), 0);
    }

    #[tokio::test]
    async fn store_failure_returns_unavailable_and_degrades() {
        let (publisher, store, metrics) =
            fixture(vec![Err(PublishError::Unavailable("down".to_owned()))], 10);
        store.fail.store(true, std::sync::atomic::Ordering::Relaxed);

        let result = publisher.publish(queue_request("m1")).await;

        assert!(matches!(result, Err(PublishError::Unavailable(_))));
        assert!(metrics.outbox.is_degraded());
    }
}
