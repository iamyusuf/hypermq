//! AMQP Task: publishes the message to an exchange or queue on another RabbitMQ broker.
//!
//! Broker access goes through the `Publisher` trait. `main` supplies the concrete publisher through a
//! `PublisherFactory`, so this module never imports `broker::rabbitmq` or the AMQP client.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use url::Url;

use crate::broker::{
    ATTEMPT_HEADER, PublishError, PublishRequest, PublishTarget, Publisher, is_valid_name,
    is_valid_routing_key,
};
use crate::message::Message;
use crate::tasks::{Task, TaskConfigError, TaskFactory, TaskOutcome};

const KIND: &str = "amqp";
const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AmqpSettings {
    /// `amqp://` or `amqps://` URL of the target broker, with credentials if it needs them.
    pub url: SecretString,
    /// Hosts the broker URL may target. Required and matched exactly (case-insensitive).
    pub allowed_hosts: Vec<String>,
    /// Publish to this exchange. Set exactly one of `exchange` or `queue`.
    #[serde(default)]
    pub exchange: Option<String>,
    /// Publish through the default exchange to this queue.
    #[serde(default)]
    pub queue: Option<String>,
    /// Only for `exchange` targets; defaults to empty.
    #[serde(default)]
    pub routing_key: Option<String>,
}

pub struct AmqpTask {
    publisher: Arc<dyn Publisher>,
    target: PublishTarget,
    routing_key: String,
}

impl AmqpTask {
    fn new(publisher: Arc<dyn Publisher>, target: PublishTarget, routing_key: String) -> Self {
        Self {
            publisher,
            target,
            routing_key,
        }
    }
}

#[async_trait]
impl Task for AmqpTask {
    async fn execute(&self, message: &Message) -> TaskOutcome {
        let request = PublishRequest {
            target: self.target.clone(),
            routing_key: self.routing_key.clone(),
            message_id: message.id.as_str().to_owned(),
            content_type: message
                .headers
                .get("content-type")
                .cloned()
                .unwrap_or_else(|| DEFAULT_CONTENT_TYPE.to_owned()),
            headers: forwarded_headers(&message.headers),
            body: message.body.clone(),
        };

        match self.publisher.publish(request).await {
            Ok(_) => TaskOutcome::Success,
            Err(err) => outcome_for_error(&err),
        }
    }
}

/// Drops the retry counter, which is internal to HyperMQ, and content-type, which is sent as a property.
fn forwarded_headers(headers: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter(|(name, _)| {
            !name.eq_ignore_ascii_case(ATTEMPT_HEADER) && !name.eq_ignore_ascii_case("content-type")
        })
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

/// Reasons are fixed strings because broker error text can include the target URL.
fn outcome_for_error(err: &PublishError) -> TaskOutcome {
    match err {
        PublishError::TargetNotFound => TaskOutcome::Permanent {
            reason: "target exchange or queue does not exist".into(),
        },
        PublishError::Unavailable(_) => retryable("broker unavailable"),
        PublishError::Unroutable => retryable("message was not routed to any queue"),
        PublishError::Rejected => retryable("broker rejected the message"),
    }
}

fn retryable(reason: &str) -> TaskOutcome {
    TaskOutcome::Retryable {
        reason: reason.into(),
        retry_after: None,
    }
}

fn invalid(message: String) -> TaskConfigError {
    TaskConfigError::InvalidSettings {
        kind: KIND.into(),
        message,
    }
}

/// Checks the scheme and that the host is in `allowed_hosts`. Errors never echo the URL.
fn validate_url(url: &str, allowed_hosts: &[String]) -> Result<(), String> {
    let parsed = Url::parse(url).map_err(|_| "url is not a valid URL".to_owned())?;
    if !matches!(parsed.scheme(), "amqp" | "amqps") {
        return Err("url must use amqp or amqps".into());
    }
    let host = parsed
        .host_str()
        .ok_or("url must include a host")?
        .to_ascii_lowercase();
    if allowed_hosts.is_empty() {
        return Err("allowed_hosts must list at least one host".into());
    }
    if !allowed_hosts
        .iter()
        .any(|allowed| allowed.eq_ignore_ascii_case(&host))
    {
        return Err(format!("host `{host}` is not in allowed_hosts"));
    }
    Ok(())
}

fn target_from(settings: &AmqpSettings) -> Result<(PublishTarget, String), String> {
    match (&settings.exchange, &settings.queue) {
        (Some(exchange), None) => {
            if !is_valid_name(exchange) {
                return Err("exchange is not a valid name".into());
            }
            let routing_key = settings.routing_key.clone().unwrap_or_default();
            if !is_valid_routing_key(&routing_key) {
                return Err("routing_key is not valid".into());
            }
            Ok((PublishTarget::Exchange(exchange.clone()), routing_key))
        }
        (None, Some(queue)) => {
            if !is_valid_name(queue) {
                return Err("queue is not a valid name".into());
            }
            if settings.routing_key.is_some() {
                return Err("routing_key applies only to exchange targets".into());
            }
            Ok((PublishTarget::Queue(queue.clone()), String::new()))
        }
        _ => Err("set exactly one of exchange or queue".into()),
    }
}

/// Builds the broker publisher for a target URL. `main` supplies the concrete implementation.
pub type PublisherFactory = Arc<dyn Fn(SecretString) -> Arc<dyn Publisher> + Send + Sync>;

pub struct AmqpTaskFactory {
    publishers: PublisherFactory,
}

impl AmqpTaskFactory {
    pub fn new(publishers: PublisherFactory) -> Self {
        Self { publishers }
    }
}

impl TaskFactory for AmqpTaskFactory {
    fn kind(&self) -> &'static str {
        KIND
    }

    fn build(&self, settings: serde_json::Value) -> Result<Arc<dyn Task>, TaskConfigError> {
        let settings: AmqpSettings =
            serde_json::from_value(settings).map_err(|err| invalid(err.to_string()))?;
        validate_url(settings.url.expose_secret(), &settings.allowed_hosts).map_err(invalid)?;
        let (target, routing_key) = target_from(&settings).map_err(invalid)?;
        let publisher = (self.publishers)(settings.url);
        Ok(Arc::new(AmqpTask::new(publisher, target, routing_key)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use bytes::Bytes;
    use serde_json::json;

    use super::*;
    use crate::broker::Accepted;
    use crate::message::{MessageId, RoutingInfo};

    #[derive(Default)]
    struct FakePublisher {
        failure: Mutex<Option<PublishError>>,
        requests: Mutex<Vec<PublishRequest>>,
    }

    #[async_trait]
    impl Publisher for FakePublisher {
        async fn publish(&self, request: PublishRequest) -> Result<Accepted, PublishError> {
            self.requests.lock().unwrap().push(request);
            match self.failure.lock().unwrap().take() {
                Some(err) => Err(err),
                None => Ok(Accepted::Published),
            }
        }
    }

    fn fake(failure: Option<PublishError>) -> Arc<FakePublisher> {
        Arc::new(FakePublisher {
            failure: Mutex::new(failure),
            requests: Mutex::new(Vec::new()),
        })
    }

    fn build(settings: serde_json::Value) -> Result<Arc<dyn Task>, TaskConfigError> {
        let publishers: PublisherFactory =
            Arc::new(|_url: SecretString| -> Arc<dyn Publisher> { fake(None) });
        AmqpTaskFactory::new(publishers).build(settings)
    }

    fn message() -> Message {
        let mut headers = BTreeMap::new();
        headers.insert("content-type".to_owned(), "application/json".to_owned());
        headers.insert(ATTEMPT_HEADER.to_owned(), "2".to_owned());
        headers.insert("x-trace".to_owned(), "abc".to_owned());
        Message {
            id: MessageId::new("m-7"),
            body: Bytes::from_static(br#"{"order":1}"#),
            headers,
            routing: RoutingInfo {
                exchange: "orders".into(),
                routing_key: "order.created".into(),
                queue: "orders.created".into(),
            },
            attempt: 2,
        }
    }

    fn valid() -> serde_json::Value {
        json!({
            "url": "amqp://hypermq:secret-pw@broker.internal:5672/orders",
            "allowed_hosts": ["broker.internal"],
            "exchange": "orders",
            "routing_key": "order.created",
        })
    }

    fn with(key: &str, value: serde_json::Value) -> serde_json::Value {
        let mut settings = valid();
        settings[key] = value;
        settings
    }

    fn without(key: &str) -> serde_json::Value {
        let mut settings = valid();
        settings.as_object_mut().unwrap().remove(key);
        settings
    }

    #[tokio::test]
    async fn publishes_body_message_id_and_forwarded_headers() {
        let publisher = fake(None);
        let task = AmqpTask::new(
            publisher.clone(),
            PublishTarget::Exchange("orders".into()),
            "order.created".into(),
        );

        assert_eq!(task.execute(&message()).await, TaskOutcome::Success);

        let requests = publisher.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert_eq!(request.target, PublishTarget::Exchange("orders".into()));
        assert_eq!(request.routing_key, "order.created");
        assert_eq!(request.message_id, "m-7");
        assert_eq!(request.content_type, "application/json");
        assert_eq!(request.body.as_ref(), br#"{"order":1}"#);
        assert_eq!(
            request.headers,
            BTreeMap::from([("x-trace".to_owned(), "abc".to_owned())])
        );
    }

    #[tokio::test]
    async fn queue_target_uses_default_content_type_when_none_is_set() {
        let publisher = fake(None);
        let task = AmqpTask::new(
            publisher.clone(),
            PublishTarget::Queue("jobs".into()),
            String::new(),
        );
        let mut message = message();
        message.headers.remove("content-type");

        assert_eq!(task.execute(&message).await, TaskOutcome::Success);

        let requests = publisher.requests.lock().unwrap();
        assert_eq!(requests[0].target, PublishTarget::Queue("jobs".into()));
        assert_eq!(requests[0].content_type, DEFAULT_CONTENT_TYPE);
    }

    #[tokio::test]
    async fn publish_errors_map_to_outcomes_without_broker_text() {
        let cases = [
            (PublishError::Unavailable("connect refused".into()), false),
            (PublishError::Unroutable, false),
            (PublishError::Rejected, false),
            (PublishError::TargetNotFound, true),
        ];
        for (error, permanent) in cases {
            let task = AmqpTask::new(
                fake(Some(error)),
                PublishTarget::Exchange("orders".into()),
                "order.created".into(),
            );
            let outcome = task.execute(&message()).await;
            assert_eq!(
                matches!(outcome, TaskOutcome::Permanent { .. }),
                permanent,
                "unexpected outcome {outcome:?}"
            );
            assert!(!format!("{outcome:?}").contains("connect refused"));
        }
    }

    #[test]
    fn build_hands_the_url_to_the_publisher_factory() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorded = seen.clone();
        let tasks =
            AmqpTaskFactory::new(Arc::new(move |url: SecretString| -> Arc<dyn Publisher> {
                recorded
                    .lock()
                    .unwrap()
                    .push(url.expose_secret().to_owned());
                fake(None)
            }));

        assert!(tasks.build(valid()).is_ok());
        assert_eq!(
            *seen.lock().unwrap(),
            vec!["amqp://hypermq:secret-pw@broker.internal:5672/orders".to_owned()]
        );
    }

    #[test]
    fn accepts_exchange_and_queue_targets() {
        assert!(build(valid()).is_ok());
        assert!(build(with("routing_key", json!(""))).is_ok());

        let queue = json!({
            "url": "amqps://broker.internal",
            "allowed_hosts": ["broker.internal"],
            "queue": "jobs",
        });
        assert!(build(queue).is_ok());
    }

    #[test]
    fn host_is_matched_case_insensitively() {
        let value = with(
            "url",
            json!("amqp://hypermq:secret-pw@Broker.Internal:5672/orders"),
        );
        assert!(build(value).is_ok());
    }

    #[test]
    fn rejects_invalid_settings() {
        let mut queue_with_exchange_key = without("exchange");
        queue_with_exchange_key["queue"] = json!("jobs");
        let cases = [
            with("bogus", json!(1)),
            with("queue", json!("jobs")),
            without("exchange"),
            with("exchange", json!("bad name")),
            with("routing_key", json!("a\nb")),
            with("url", json!("http://broker.internal")),
            with("url", json!("not a url")),
            with("allowed_hosts", json!(["other.internal"])),
            with("allowed_hosts", json!([])),
            without("url"),
            queue_with_exchange_key,
        ];
        for value in cases {
            assert!(
                matches!(
                    build(value.clone()),
                    Err(TaskConfigError::InvalidSettings { .. })
                ),
                "accepted {value}"
            );
        }
    }

    #[test]
    fn errors_do_not_echo_the_url_credentials() {
        let value = with("allowed_hosts", json!(["other.internal"]));
        let Err(TaskConfigError::InvalidSettings { message, .. }) = build(value) else {
            panic!("expected the host to be rejected");
        };
        assert!(!message.contains("secret-pw"));
    }
}
