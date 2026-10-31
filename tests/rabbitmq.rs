//! Needs a running RabbitMQ; run with `cargo test --test rabbitmq -- --ignored`.
//! Set `HYPERMQ_TEST_RABBITMQ_URL` to override the default local broker.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use hypermq::broker::rabbitmq::{RabbitBroker, RabbitPublisher, RabbitTopology};
use hypermq::broker::topology::{self, Topology};
use hypermq::broker::{BrokerError, PublishError, PublishRequest, PublishTarget, Publisher};
use hypermq::consumers::consumer::Consumer;
use hypermq::consumers::{self, ConsumerConfig};
use hypermq::error::TopologyError;
use hypermq::message::Message;
use hypermq::tasks::{Task, TaskConfigError, TaskFactory, TaskOutcome, TaskRegistry};
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

const WAIT: Duration = Duration::from_secs(10);
const POLL: Duration = Duration::from_millis(50);
/// Time for in-flight settlements to finish before the consumer is stopped.
const GRACE: Duration = Duration::from_millis(300);

type Seen = Arc<Mutex<Vec<(String, u32)>>>;

/// Records each attempt and returns the scripted outcome for that attempt, then `Success`.
struct FakeTask {
    script: Vec<TaskOutcome>,
    seen: Seen,
}

#[async_trait]
impl Task for FakeTask {
    async fn execute(&self, message: &Message) -> TaskOutcome {
        self.seen
            .lock()
            .unwrap()
            .push((message.id.as_str().to_owned(), message.attempt));
        let index = usize::try_from(message.attempt.saturating_sub(1)).unwrap_or(usize::MAX);
        self.script
            .get(index)
            .cloned()
            .unwrap_or(TaskOutcome::Success)
    }
}

struct FakeFactory {
    script: Vec<TaskOutcome>,
    seen: Seen,
}

impl TaskFactory for FakeFactory {
    fn kind(&self) -> &'static str {
        "fake"
    }

    fn build(&self, _settings: serde_json::Value) -> Result<Arc<dyn Task>, TaskConfigError> {
        Ok(Arc::new(FakeTask {
            script: self.script.clone(),
            seen: Arc::clone(&self.seen),
        }))
    }
}

fn fake_registry(script: Vec<TaskOutcome>) -> (TaskRegistry, Seen) {
    let seen: Seen = Arc::default();
    let mut registry = TaskRegistry::default();
    registry.register(Arc::new(FakeFactory {
        script,
        seen: Arc::clone(&seen),
    }));
    (registry, seen)
}

fn retryable() -> TaskOutcome {
    TaskOutcome::Retryable {
        reason: "scripted".into(),
        retry_after: None,
    }
}

fn permanent() -> TaskOutcome {
    TaskOutcome::Permanent {
        reason: "scripted".into(),
    }
}

fn fake_consumer_yaml(queue: &str, max_attempts: u32) -> String {
    format!(
        "
- name: test
  queue: {queue}
  prefetch: 1
  timeout: 5s
  retry: {{ max_attempts: {max_attempts}, base_delay: 50ms, backoff: fixed }}
  task: {{ type: fake }}
"
    )
}

fn single_consumer(yaml: &str, registry: &TaskRegistry) -> Consumer {
    let configs: Vec<ConsumerConfig> = serde_yaml_ng::from_str(yaml).unwrap();
    consumers::build(&configs, registry).unwrap().remove(0)
}

/// Declares a work queue whose rejected messages dead-letter to `dlq` through a fanout exchange.
async fn declare_work_topology(work: &str, dlx: &str, dlq: &str) {
    let topology = topology_from(&format!(
        "
exchanges: [{{ name: {dlx}, kind: fanout }}]
queues:
  - {{ name: {work}, dead_letter_exchange: {dlx} }}
  - {{ name: {dlq} }}
bindings: [{{ queue: {dlq}, exchange: {dlx} }}]
"
    ));
    let manager = topology_manager().await;
    topology::apply(&manager, &topology).await.unwrap();
    manager.close().await;
}

async fn depth(channel: &Channel, queue: &str) -> u32 {
    channel
        .queue_declare(
            queue.into(),
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap()
        .message_count()
}

async fn wait_for_depth(channel: &Channel, queue: &str, expected: u32) {
    let deadline = Instant::now() + WAIT;
    loop {
        let current = depth(channel, queue).await;
        if current == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{queue} has {current} messages, expected {expected}"
        );
        tokio::time::sleep(POLL).await;
    }
}

async fn wait_for_attempts(seen: &Seen, expected: usize) {
    let deadline = Instant::now() + WAIT;
    loop {
        let count = seen.lock().unwrap().len();
        if count >= expected {
            return;
        }
        assert!(Instant::now() < deadline, "only {count} attempts ran");
        tokio::time::sleep(POLL).await;
    }
}

fn start(consumer: Consumer) -> tokio::task::JoinHandle<Result<(), BrokerError>> {
    let broker = RabbitBroker::new(SecretString::from(url()));
    // The sender is dropped here, so shutdown never fires; `stop` aborts the task instead.
    let (_, shutdown) = tokio::sync::watch::channel(false);
    tokio::spawn(async move { consumer.run(&broker, &shutdown).await })
}

/// Lets in-flight settlements finish, then stops the consumer and lets the broker requeue anything unsettled.
async fn stop(handle: tokio::task::JoinHandle<Result<(), BrokerError>>) {
    tokio::time::sleep(GRACE).await;
    handle.abort();
    let _ = handle.await;
    tokio::time::sleep(GRACE).await;
}

#[tokio::test]
#[ignore = "needs RabbitMQ"]
async fn successful_delivery_is_acked_and_not_requeued() {
    let (work, dlx, dlq) = (unique("work"), unique("dlx"), unique("dlq"));
    let retry = format!("{work}.retry");
    declare_work_topology(&work, &dlx, &dlq).await;

    let (registry, seen) = fake_registry(Vec::new());
    let consumer = single_consumer(&fake_consumer_yaml(&work, 3), &registry);
    let req = request(PublishTarget::Queue(work.clone()), "");
    let id = req.message_id.clone();
    publisher().publish(req).await.unwrap();

    let handle = start(consumer);
    wait_for_attempts(&seen, 1).await;
    stop(handle).await;

    let channel = admin_channel().await;
    assert_eq!(seen.lock().unwrap().clone(), vec![(id, 1)]);
    assert_eq!(
        depth(&channel, &work).await,
        0,
        "acked message was requeued"
    );
    assert_eq!(depth(&channel, &retry).await, 0);
    assert_eq!(depth(&channel, &dlq).await, 0);

    delete_all(&channel, &[&work, &dlq, &retry], &[&dlx]).await;
}

#[tokio::test]
#[ignore = "needs RabbitMQ"]
async fn retryable_outcome_is_retried_then_acked() {
    let (work, dlx, dlq) = (unique("work"), unique("dlx"), unique("dlq"));
    let retry = format!("{work}.retry");
    declare_work_topology(&work, &dlx, &dlq).await;

    let (registry, seen) = fake_registry(vec![retryable(), TaskOutcome::Success]);
    let consumer = single_consumer(&fake_consumer_yaml(&work, 3), &registry);
    let req = request(PublishTarget::Queue(work.clone()), "");
    let id = req.message_id.clone();
    publisher().publish(req).await.unwrap();

    let handle = start(consumer);
    wait_for_attempts(&seen, 2).await;
    stop(handle).await;

    let channel = admin_channel().await;
    assert_eq!(seen.lock().unwrap().clone(), vec![(id.clone(), 1), (id, 2)]);
    assert_eq!(depth(&channel, &work).await, 0);
    assert_eq!(depth(&channel, &retry).await, 0);
    assert_eq!(depth(&channel, &dlq).await, 0);

    delete_all(&channel, &[&work, &dlq, &retry], &[&dlx]).await;
}

#[tokio::test]
#[ignore = "needs RabbitMQ"]
async fn exhausted_retries_are_dead_lettered() {
    let (work, dlx, dlq) = (unique("work"), unique("dlx"), unique("dlq"));
    let retry = format!("{work}.retry");
    declare_work_topology(&work, &dlx, &dlq).await;

    let (registry, seen) = fake_registry(vec![retryable(), retryable(), retryable()]);
    let consumer = single_consumer(&fake_consumer_yaml(&work, 3), &registry);
    let req = request(PublishTarget::Queue(work.clone()), "");
    let id = req.message_id.clone();
    publisher().publish(req).await.unwrap();

    let handle = start(consumer);
    let channel = admin_channel().await;
    wait_for_depth(&channel, &dlq, 1).await;
    stop(handle).await;

    let attempts: Vec<u32> = seen.lock().unwrap().iter().map(|(_, a)| *a).collect();
    assert_eq!(attempts, vec![1, 2, 3]);
    assert_eq!(depth(&channel, &work).await, 0);
    assert_eq!(depth(&channel, &retry).await, 0);

    let dead = channel
        .basic_get(dlq.as_str().into(), BasicGetOptions::default())
        .await
        .unwrap()
        .expect("message should be dead-lettered");
    assert_eq!(dead.properties.message_id().as_ref().unwrap().as_str(), id);

    delete_all(&channel, &[&work, &dlq, &retry], &[&dlx]).await;
}

#[tokio::test]
#[ignore = "needs RabbitMQ"]
async fn permanent_outcome_is_dead_lettered_without_retry() {
    let (work, dlx, dlq) = (unique("work"), unique("dlx"), unique("dlq"));
    let retry = format!("{work}.retry");
    declare_work_topology(&work, &dlx, &dlq).await;

    let (registry, seen) = fake_registry(vec![permanent()]);
    let consumer = single_consumer(&fake_consumer_yaml(&work, 3), &registry);
    publisher()
        .publish(request(PublishTarget::Queue(work.clone()), ""))
        .await
        .unwrap();

    let handle = start(consumer);
    let channel = admin_channel().await;
    wait_for_depth(&channel, &dlq, 1).await;
    stop(handle).await;

    assert_eq!(
        seen.lock().unwrap().len(),
        1,
        "permanent outcome must not retry"
    );
    assert_eq!(depth(&channel, &retry).await, 0);

    delete_all(&channel, &[&work, &dlq, &retry], &[&dlx]).await;
}

#[cfg(feature = "task-http")]
mod http_end_to_end {
    use std::collections::{HashMap, VecDeque};

    use axum::Router;
    use axum::body::Body;
    use axum::extract::State;
    use axum::http::{HeaderMap, Request, StatusCode};
    use axum::routing::post;
    use hypermq::api::{self, AppState};
    use hypermq::config::Role;
    use hypermq::tasks::http::HttpTaskFactory;
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use super::*;

    /// Statuses returned per case, one per call; 200 once a case's list is empty.
    #[derive(Default)]
    struct Downstream {
        statuses: Mutex<HashMap<&'static str, VecDeque<StatusCode>>>,
        calls: Mutex<Vec<(&'static str, Option<String>)>>,
    }

    async fn hook(
        State(downstream): State<Arc<Downstream>>,
        headers: HeaderMap,
        body: String,
    ) -> StatusCode {
        let case = if body.contains("reject") {
            "reject"
        } else {
            "retry"
        };
        let key = headers
            .get("idempotency-key")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        downstream.calls.lock().unwrap().push((case, key));
        downstream
            .statuses
            .lock()
            .unwrap()
            .get_mut(case)
            .and_then(VecDeque::pop_front)
            .unwrap_or(StatusCode::OK)
    }

    /// Publishes through the HTTP API and returns the message id it reports.
    async fn publish_via_api(queue: &str, payload: Value) -> String {
        let app = api::router(AppState {
            role: Role::Api,
            max_body_bytes: 64 * 1024,
            publisher: Arc::new(publisher()),
            auth: None,
        });
        let body = json!({ "queue": queue, "payload": payload }).to_string();
        let request = Request::builder()
            .method("POST")
            .uri("/publish")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert!(response.status().is_success(), "{}", response.status());
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let out: Value = serde_json::from_slice(&bytes).unwrap();
        out["message_id"].as_str().unwrap().to_owned()
    }

    async fn wait_for_calls(downstream: &Downstream, expected: usize) {
        let deadline = Instant::now() + WAIT;
        loop {
            let count = downstream.calls.lock().unwrap().len();
            if count >= expected {
                return;
            }
            assert!(Instant::now() < deadline, "downstream got {count} calls");
            tokio::time::sleep(POLL).await;
        }
    }

    fn keys_for(downstream: &Downstream, case: &str) -> Vec<Option<String>> {
        downstream
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(seen_case, _)| *seen_case == case)
            .map(|(_, key)| key.clone())
            .collect()
    }

    #[tokio::test]
    #[ignore = "needs RabbitMQ"]
    async fn http_task_retries_and_dead_letters_through_the_api() {
        let (work, dlx, dlq) = (unique("work"), unique("dlx"), unique("dlq"));
        let retry = format!("{work}.retry");
        declare_work_topology(&work, &dlx, &dlq).await;

        let downstream = Arc::new(Downstream {
            statuses: Mutex::new(HashMap::from([
                ("retry", VecDeque::from([StatusCode::SERVICE_UNAVAILABLE])),
                ("reject", VecDeque::from([StatusCode::BAD_REQUEST])),
            ])),
            ..Default::default()
        });
        let app = Router::new()
            .route("/hook", post(hook))
            .with_state(Arc::clone(&downstream));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move { axum::serve(listener, app).await });

        let yaml = format!(
            "
- name: e2e
  queue: {work}
  prefetch: 1
  timeout: 5s
  retry: {{ max_attempts: 3, base_delay: 100ms, backoff: fixed }}
  task:
    type: http
    url: http://127.0.0.1:{port}/hook
    allowed_hosts: [127.0.0.1]
    idempotency_header: Idempotency-Key
"
        );
        let mut registry = TaskRegistry::default();
        registry.register(Arc::new(HttpTaskFactory));
        let consumer = single_consumer(&yaml, &registry);

        let accepted = publish_via_api(&work, json!({ "case": "retry" })).await;
        let rejected = publish_via_api(&work, json!({ "case": "reject" })).await;

        let handle = start(consumer);
        let channel = admin_channel().await;
        wait_for_calls(&downstream, 3).await;
        wait_for_depth(&channel, &dlq, 1).await;
        stop(handle).await;

        // The retried message keeps the same idempotency key on both attempts.
        assert_eq!(
            keys_for(&downstream, "retry"),
            vec![Some(accepted.clone()), Some(accepted)]
        );
        assert_eq!(
            keys_for(&downstream, "reject"),
            vec![Some(rejected.clone())]
        );
        assert_eq!(depth(&channel, &work).await, 0);
        assert_eq!(depth(&channel, &retry).await, 0);

        let dead = channel
            .basic_get(dlq.as_str().into(), BasicGetOptions::default())
            .await
            .unwrap()
            .expect("rejected message should be dead-lettered");
        assert_eq!(
            dead.properties.message_id().as_ref().unwrap().as_str(),
            rejected
        );

        delete_all(&channel, &[&work, &dlq, &retry], &[&dlx]).await;
    }
}
