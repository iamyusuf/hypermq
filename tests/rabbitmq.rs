//! Needs a running RabbitMQ; run with `cargo test --test rabbitmq -- --ignored`.
//! Set `HYPERMQ_TEST_RABBITMQ_URL` to override the default local broker.

use std::collections::BTreeMap;

use bytes::Bytes;
use hypermq::broker::rabbitmq::{RabbitPublisher, RabbitTopology};
use hypermq::broker::topology::{self, Topology};
use hypermq::broker::{PublishError, PublishRequest, PublishTarget, Publisher};
use hypermq::error::TopologyError;
use lapin::options::{
    BasicGetOptions, ExchangeDeclareOptions, ExchangeDeleteOptions, QueueBindOptions,
    QueueDeclareOptions, QueueDeleteOptions,
};
use lapin::types::FieldTable;
use lapin::{Channel, Connection, ConnectionProperties, ExchangeKind};
use secrecy::SecretString;
use uuid::Uuid;

fn url() -> String {
    std::env::var("HYPERMQ_TEST_RABBITMQ_URL")
        .unwrap_or_else(|_| "amqp://guest:guest@localhost:5672".to_owned())
}

fn publisher() -> RabbitPublisher {
    RabbitPublisher::new(SecretString::from(url()))
}

async fn admin_channel() -> Channel {
    let connection = Connection::connect(&url(), ConnectionProperties::default())
        .await
        .expect("RabbitMQ must be running for this test");
    connection.create_channel().await.unwrap()
}

fn unique(prefix: &str) -> String {
    format!("hypermq-test-{prefix}-{}", Uuid::new_v4())
}

fn request(target: PublishTarget, routing_key: &str) -> PublishRequest {
    PublishRequest {
        target,
        routing_key: routing_key.to_owned(),
        message_id: Uuid::new_v4().to_string(),
        content_type: "application/json".into(),
        headers: BTreeMap::from([("source".to_owned(), "test".to_owned())]),
        body: Bytes::from_static(br#"{"hello":"world"}"#),
    }
}

async fn declare_queue(channel: &Channel, name: &str) {
    channel
        .queue_declare(
            name.into(),
            QueueDeclareOptions::durable(),
            FieldTable::default(),
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "needs RabbitMQ"]
async fn publishes_to_a_queue_with_properties() {
    let channel = admin_channel().await;
    let queue = unique("queue");
    declare_queue(&channel, &queue).await;

    let req = request(PublishTarget::Queue(queue.clone()), "");
    let message_id = req.message_id.clone();
    publisher().publish(req).await.unwrap();

    let message = channel
        .basic_get(queue.as_str().into(), BasicGetOptions::default())
        .await
        .unwrap()
        .expect("message should be queued");
    assert_eq!(message.data, br#"{"hello":"world"}"#);
    let props = &message.properties;
    assert_eq!(props.message_id().as_ref().unwrap().as_str(), message_id);
    assert_eq!(
        props.content_type().as_ref().unwrap().as_str(),
        "application/json"
    );
    assert_eq!(*props.delivery_mode(), Some(2));
    assert!(
        props
            .headers()
            .as_ref()
            .unwrap()
            .inner()
            .contains_key("source")
    );

    channel
        .queue_delete(queue.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "needs RabbitMQ"]
async fn publishes_through_an_exchange_and_reports_unroutable() {
    let channel = admin_channel().await;
    let exchange = unique("exchange");
    let queue = unique("queue");
    channel
        .exchange_declare(
            exchange.as_str().into(),
            ExchangeKind::Direct,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    declare_queue(&channel, &queue).await;
    channel
        .queue_bind(
            queue.as_str().into(),
            exchange.as_str().into(),
            "key".into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    let publisher = publisher();
    publisher
        .publish(request(PublishTarget::Exchange(exchange.clone()), "key"))
        .await
        .unwrap();
    assert!(
        channel
            .basic_get(queue.as_str().into(), BasicGetOptions::default())
            .await
            .unwrap()
            .is_some()
    );

    let err = publisher
        .publish(request(PublishTarget::Exchange(exchange.clone()), "other"))
        .await
        .unwrap_err();
    assert!(matches!(err, PublishError::Unroutable), "{err:?}");

    let err = publisher
        .publish(request(PublishTarget::Queue(unique("missing")), ""))
        .await
        .unwrap_err();
    assert!(matches!(err, PublishError::Unroutable), "{err:?}");

    channel
        .queue_delete(queue.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
    channel
        .exchange_delete(exchange.as_str().into(), ExchangeDeleteOptions::default())
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "needs RabbitMQ"]
async fn missing_exchange_is_reported_and_publisher_recovers() {
    let channel = admin_channel().await;
    let queue = unique("queue");
    declare_queue(&channel, &queue).await;

    let publisher = publisher();
    let err = publisher
        .publish(request(PublishTarget::Exchange(unique("missing")), "key"))
        .await
        .unwrap_err();
    assert!(matches!(err, PublishError::TargetNotFound), "{err:?}");

    // The broker closed the channel; the next publish must open a new one.
    publisher
        .publish(request(PublishTarget::Queue(queue.clone()), ""))
        .await
        .unwrap();

    channel
        .queue_delete(queue.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "needs RabbitMQ"]
async fn unreachable_broker_reports_unavailable() {
    let publisher = RabbitPublisher::new(SecretString::from("amqp://guest:guest@127.0.0.1:1"));
    let err = publisher
        .publish(request(PublishTarget::Queue("q".into()), ""))
        .await
        .unwrap_err();
    assert!(matches!(err, PublishError::Unavailable(_)), "{err:?}");
}

fn topology_from(yaml: &str) -> Topology {
    serde_yaml_ng::from_str(yaml).unwrap()
}

async fn topology_manager() -> RabbitTopology {
    RabbitTopology::connect(&SecretString::from(url()))
        .await
        .expect("RabbitMQ must be running for this test")
}

async fn delete_all(channel: &Channel, queues: &[&str], exchanges: &[&str]) {
    for queue in queues {
        let _ = channel
            .queue_delete((*queue).into(), QueueDeleteOptions::default())
            .await;
    }
    for exchange in exchanges {
        let _ = channel
            .exchange_delete((*exchange).into(), ExchangeDeleteOptions::default())
            .await;
    }
}

#[tokio::test]
#[ignore = "needs RabbitMQ"]
async fn declared_topology_is_idempotent_and_routes_messages() {
    let (exchange, dlx, queue) = (unique("ex"), unique("dlx"), unique("queue"));
    let topology = topology_from(&format!(
        "
exchanges:
  - {{ name: {exchange}, kind: direct }}
  - {{ name: {dlx}, kind: fanout }}
queues:
  - name: {queue}
    dead_letter_exchange: {dlx}
    message_ttl: 1h
bindings:
  - {{ queue: {queue}, exchange: {exchange}, routing_key: key }}
"
    ));

    let manager = topology_manager().await;
    topology::apply(&manager, &topology).await.unwrap();
    topology::apply(&manager, &topology).await.unwrap();
    manager.close().await;

    publisher()
        .publish(request(PublishTarget::Exchange(exchange.clone()), "key"))
        .await
        .unwrap();

    let channel = admin_channel().await;
    let message = channel
        .basic_get(queue.as_str().into(), BasicGetOptions::default())
        .await
        .unwrap();
    assert!(message.is_some(), "message should be routed to the queue");

    delete_all(&channel, &[&queue], &[&exchange, &dlx]).await;
}

#[tokio::test]
#[ignore = "needs RabbitMQ"]
async fn conflicting_declarations_fail_with_a_clear_error() {
    let (exchange, queue) = (unique("ex"), unique("queue"));
    let original = topology_from(&format!(
        "
exchanges: [{{ name: {exchange}, kind: direct }}]
queues: [{{ name: {queue} }}]
"
    ));
    let manager = topology_manager().await;
    topology::apply(&manager, &original).await.unwrap();

    let different_queue = topology_from(&format!("queues: [{{ name: {queue}, message_ttl: 5s }}]"));
    let err = topology::apply(&manager, &different_queue)
        .await
        .unwrap_err();
    assert!(matches!(err, TopologyError::Conflict { .. }), "{err:?}");
    assert!(err.to_string().contains(&queue), "{err}");

    let different_exchange = topology_from(&format!(
        "exchanges: [{{ name: {exchange}, kind: fanout }}]"
    ));
    let err = topology::apply(&manager, &different_exchange)
        .await
        .unwrap_err();
    assert!(matches!(err, TopologyError::Conflict { .. }), "{err:?}");

    // A failed declaration must not poison later ones.
    topology::apply(&manager, &original).await.unwrap();
    manager.close().await;

    delete_all(&admin_channel().await, &[&queue], &[&exchange]).await;
}

#[tokio::test]
#[ignore = "needs RabbitMQ"]
async fn binding_a_missing_queue_fails_with_not_found() {
    let exchange = unique("ex");
    let topology = topology_from(&format!(
        "
exchanges: [{{ name: {exchange}, kind: direct }}]
bindings: [{{ queue: {missing}, exchange: {exchange} }}]
",
        missing = unique("missing")
    ));
    let manager = topology_manager().await;
    let err = topology::apply(&manager, &topology).await.unwrap_err();
    assert!(matches!(err, TopologyError::NotFound { .. }), "{err:?}");
    manager.close().await;

    delete_all(&admin_channel().await, &[], &[&exchange]).await;
}

#[tokio::test]
#[ignore = "needs RabbitMQ"]
async fn topology_reports_an_unreachable_broker() {
    let result =
        RabbitTopology::connect(&SecretString::from("amqp://guest:guest@127.0.0.1:1")).await;
    assert!(matches!(result, Err(TopologyError::Unavailable(_))));
}
