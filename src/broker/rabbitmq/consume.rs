//! Consuming side of the RabbitMQ broker: subscriptions, deliveries, and delayed retries.

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_lite::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions, BasicRejectOptions,
    ConfirmSelectOptions, QueueDeclareOptions,
};
use lapin::protocol::AMQPErrorKind;
use lapin::types::{AMQPValue, FieldTable};
use lapin::{BasicProperties, Channel, Confirmation, Connection, ErrorKind};
use secrecy::SecretString;

use super::{CONFIRM_TIMEOUT, PERSISTENT, close, connect};
use crate::broker::{ATTEMPT_HEADER, Broker, BrokerError, Delivery, MAX_NAME_LEN, Subscription};
use crate::message::{Message, MessageId, RoutingInfo};

const RETRY_SUFFIX: &str = ".retry";
/// Largest per-message TTL sent for a retry (`i32::MAX` milliseconds, about 24 days).
const MAX_EXPIRATION_MS: u128 = i32::MAX as u128;

/// Opens a consumer connection per subscription, separate from the publisher's.
pub struct RabbitBroker {
    url: SecretString,
}

impl RabbitBroker {
    pub fn new(url: SecretString) -> Self {
        Self { url }
    }
}

#[async_trait]
impl Broker for RabbitBroker {
    async fn consume(
        &self,
        queue: &str,
        prefetch: u16,
    ) -> Result<Box<dyn Subscription>, BrokerError> {
        let retry_queue = retry_queue_name(queue)?;
        let connection = connect(&self.url).await.map_err(BrokerError::Unavailable)?;

        match open(&connection, queue, &retry_queue, prefetch).await {
            Ok((channel, consumer)) => Ok(Box::new(RabbitSubscription {
                connection: Some(connection),
                channel,
                consumer,
                queue: queue.to_owned(),
                retry_queue,
            })),
            Err(err) => {
                close(connection);
                Err(err)
            }
        }
    }
}

/// Name of the per-queue delay queue used for retries.
fn retry_queue_name(queue: &str) -> Result<String, BrokerError> {
    let name = format!("{queue}{RETRY_SUFFIX}");
    if name.len() > MAX_NAME_LEN {
        return Err(BrokerError::Operation(format!(
            "queue name is too long for its `{RETRY_SUFFIX}` queue"
        )));
    }
    Ok(name)
}

async fn open(
    connection: &Connection,
    queue: &str,
    retry_queue: &str,
    prefetch: u16,
) -> Result<(Channel, lapin::Consumer), BrokerError> {
    declare_retry_queue(connection, queue, retry_queue).await?;

    let channel = connection.create_channel().await.map_err(broker_error)?;
    // Confirms are needed so a retry is durable before the original is acked.
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .map_err(broker_error)?;
    channel
        .basic_qos(prefetch, BasicQosOptions::default())
        .await
        .map_err(broker_error)?;
    // An empty tag lets the broker assign one.
    let consumer = channel
        .basic_consume(
            queue.into(),
            "".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .map_err(broker_error)?;
    Ok((channel, consumer))
}

/// Durable delay queue: messages expire after their per-message TTL and dead-letter back to
/// the work queue through the default exchange.
async fn declare_retry_queue(
    connection: &Connection,
    queue: &str,
    retry_queue: &str,
) -> Result<(), BrokerError> {
    // A failed declaration closes its channel, so it gets a channel of its own.
    let channel = connection.create_channel().await.map_err(broker_error)?;
    let mut arguments = FieldTable::default();
    arguments.insert(
        "x-dead-letter-exchange".into(),
        AMQPValue::LongString("".to_owned().into()),
    );
    arguments.insert(
        "x-dead-letter-routing-key".into(),
        AMQPValue::LongString(queue.to_owned().into()),
    );
    let result = channel
        .queue_declare(
            retry_queue.into(),
            QueueDeclareOptions::durable(),
            arguments,
        )
        .await;
    let _ = channel.close(200, "retry queue declared".into()).await;
    result.map(|_| ()).map_err(broker_error)
}

struct RabbitSubscription {
    // Option so `Drop` can move the connection out to close it.
    connection: Option<Connection>,
    channel: Channel,
    consumer: lapin::Consumer,
    queue: String,
    retry_queue: String,
}

#[async_trait]
impl Subscription for RabbitSubscription {
    async fn next(&mut self) -> Option<Result<Box<dyn Delivery>, BrokerError>> {
        loop {
            let mut delivery = match self.consumer.next().await? {
                Ok(delivery) => delivery,
                Err(err) => return Some(Err(BrokerError::Unavailable(err.to_string()))),
            };

            let body = std::mem::take(&mut delivery.data);
            match to_message(
                &delivery.properties,
                delivery.exchange.as_str(),
                delivery.routing_key.as_str(),
                &self.queue,
                body,
            ) {
                Some(message) => {
                    return Some(Ok(Box::new(RabbitDelivery {
                        acker: delivery.acker,
                        channel: self.channel.clone(),
                        retry_queue: self.retry_queue.clone(),
                        message,
                    })));
                }
                None => {
                    // Without a message id the message cannot be deduplicated, so it is dead-lettered.
                    tracing::warn!(queue = %self.queue, "rejecting delivery without a message id");
                    if let Err(err) = delivery
                        .acker
                        .reject(BasicRejectOptions { requeue: false })
                        .await
                    {
                        return Some(Err(broker_error(err)));
                    }
                }
            }
        }
    }
}

impl Drop for RabbitSubscription {
    fn drop(&mut self) {
        // Closing the connection requeues any delivery that was never settled.
        if let Some(connection) = self.connection.take() {
            close(connection);
        }
    }
}

struct RabbitDelivery {
    acker: lapin::Acker,
    channel: Channel,
    retry_queue: String,
    message: Message,
}

#[async_trait]
impl Delivery for RabbitDelivery {
    fn message(&self) -> &Message {
        &self.message
    }

    async fn ack(self: Box<Self>) -> Result<(), BrokerError> {
        self.acker
            .ack(BasicAckOptions::default())
            .await
            .map(|_| ())
            .map_err(broker_error)
    }

    async fn reject(self: Box<Self>) -> Result<(), BrokerError> {
        self.acker
            .reject(BasicRejectOptions { requeue: false })
            .await
            .map(|_| ())
            .map_err(broker_error)
    }

    async fn retry(self: Box<Self>, delay: Duration) -> Result<(), BrokerError> {
        let expiration = delay.as_millis().min(MAX_EXPIRATION_MS).to_string();

        let mut headers = FieldTable::default();
        for (key, value) in &self.message.headers {
            headers.insert(
                key.as_str().into(),
                AMQPValue::LongString(value.clone().into()),
            );
        }
        headers.insert(
            ATTEMPT_HEADER.into(),
            AMQPValue::LongString((self.message.attempt + 1).to_string().into()),
        );

        let mut properties = BasicProperties::default()
            .with_message_id(self.message.id.as_str().into())
            .with_delivery_mode(PERSISTENT)
            .with_expiration(expiration.into())
            .with_headers(headers);
        if let Some(content_type) = self.message.headers.get("content-type") {
            properties = properties.with_content_type(content_type.as_str().into());
        }

        // `mandatory` turns a missing retry queue into an error instead of a silent drop.
        let options = BasicPublishOptions {
            mandatory: true,
            ..Default::default()
        };
        let confirmation = tokio::time::timeout(CONFIRM_TIMEOUT, async {
            self.channel
                .basic_publish(
                    "".into(),
                    self.retry_queue.as_str().into(),
                    options,
                    &self.message.body,
                    properties,
                )
                .await?
                .await
        })
        .await;

        match confirmation {
            Err(_) => {
                return Err(BrokerError::Operation(
                    "retry was not confirmed in time".into(),
                ));
            }
            Ok(Err(err)) => return Err(broker_error(err)),
            Ok(Ok(Confirmation::Ack(None))) => {}
            Ok(Ok(_)) => {
                return Err(BrokerError::Operation(
                    "retry queue did not accept the message".into(),
                ));
            }
        }

        // The original is acked only after the retry is confirmed, so a failure cannot lose it.
        self.acker
            .ack(BasicAckOptions::default())
            .await
            .map(|_| ())
            .map_err(broker_error)
    }
}

/// Maps a delivery to a broker-agnostic message. Returns `None` when there is no message id.
fn to_message(
    properties: &BasicProperties,
    exchange: &str,
    routing_key: &str,
    queue: &str,
    body: Vec<u8>,
) -> Option<Message> {
    let id = properties.message_id().as_ref()?.as_str();

    let mut headers = BTreeMap::new();
    if let Some(table) = properties.headers().as_ref() {
        for (key, value) in table.inner() {
            if let Some(text) = header_text(value) {
                headers.insert(key.as_str().to_owned(), text);
            }
        }
    }
    if let Some(content_type) = properties.content_type().as_ref() {
        headers.insert("content-type".to_owned(), content_type.as_str().to_owned());
    }

    let attempt = headers
        .get(ATTEMPT_HEADER)
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|attempt| *attempt >= 1)
        .unwrap_or(1);

    Some(Message {
        id: MessageId::new(id),
        body: Bytes::from(body),
        headers,
        routing: RoutingInfo {
            exchange: exchange.to_owned(),
            routing_key: routing_key.to_owned(),
            queue: queue.to_owned(),
        },
        attempt,
    })
}

/// Only string-valued headers reach the Task; other AMQP types are dropped.
fn header_text(value: &AMQPValue) -> Option<String> {
    match value {
        AMQPValue::LongString(text) => Some(text.to_string()),
        AMQPValue::ShortString(text) => Some(text.as_str().to_owned()),
        _ => None,
    }
}

fn broker_error(err: lapin::Error) -> BrokerError {
    match err.kind() {
        // Soft protocol errors (for example a missing queue) will not fix themselves on retry.
        ErrorKind::ProtocolError(amqp) if matches!(amqp.kind(), AMQPErrorKind::Soft(_)) => {
            BrokerError::Operation(err.to_string())
        }
        _ => BrokerError::Unavailable(err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn properties_with(attempt: Option<&str>) -> BasicProperties {
        let mut headers = FieldTable::default();
        headers.insert(
            "source".into(),
            AMQPValue::LongString("web".to_owned().into()),
        );
        if let Some(attempt) = attempt {
            headers.insert(
                ATTEMPT_HEADER.into(),
                AMQPValue::LongString(attempt.to_owned().into()),
            );
        }
        BasicProperties::default()
            .with_message_id("m-1".into())
            .with_content_type("application/json".into())
            .with_headers(headers)
    }

    #[test]
    fn delivery_maps_to_message() {
        let message = to_message(
            &properties_with(Some("3")),
            "orders",
            "order.created",
            "jobs",
            b"{}".to_vec(),
        )
        .unwrap();

        assert_eq!(message.id.as_str(), "m-1");
        assert_eq!(message.attempt, 3);
        assert_eq!(message.headers["source"], "web");
        assert_eq!(message.headers["content-type"], "application/json");
        assert_eq!(message.routing.exchange, "orders");
        assert_eq!(message.routing.routing_key, "order.created");
        assert_eq!(message.routing.queue, "jobs");
        assert_eq!(message.body.as_ref(), br"{}");
    }

    #[test]
    fn attempt_defaults_to_one_when_missing_or_invalid() {
        for attempt in [None, Some("0"), Some("-2"), Some("many")] {
            let message =
                to_message(&properties_with(attempt), "", "", "jobs", Vec::new()).unwrap();
            assert_eq!(message.attempt, 1, "{attempt:?}");
        }
    }

    #[test]
    fn delivery_without_message_id_is_not_mapped() {
        let properties = BasicProperties::default();
        assert!(to_message(&properties, "", "", "jobs", Vec::new()).is_none());
    }

    #[test]
    fn retry_queue_name_is_suffixed_and_length_checked() {
        assert_eq!(retry_queue_name("jobs").unwrap(), "jobs.retry");
        assert!(retry_queue_name(&"q".repeat(MAX_NAME_LEN)).is_err());
        assert!(retry_queue_name(&"q".repeat(MAX_NAME_LEN - RETRY_SUFFIX.len())).is_ok());
    }
}
