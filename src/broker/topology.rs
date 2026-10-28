use std::collections::HashSet;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;

use super::{MAX_NAME_LEN, MAX_ROUTING_KEY_LEN, is_valid_name, is_valid_routing_key};
use crate::error::TopologyError;

const RESERVED_PREFIX: &str = "amq.";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExchangeKind {
    Direct,
    Fanout,
    Topic,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExchangeSpec {
    pub name: String,
    pub kind: ExchangeKind,
    #[serde(default = "default_true")]
    pub durable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueSpec {
    pub name: String,
    #[serde(default = "default_true")]
    pub durable: bool,
    /// Target for rejected and expired messages.
    pub dead_letter_exchange: Option<String>,
    pub dead_letter_routing_key: Option<String>,
    /// Per-message TTL; used for delayed-retry queues.
    #[serde(default, with = "humantime_serde")]
    pub message_ttl: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingSpec {
    pub queue: String,
    pub exchange: String,
    #[serde(default)]
    pub routing_key: String,
}

/// Exchanges, queues, and bindings to declare at startup.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Topology {
    #[serde(default)]
    pub exchanges: Vec<ExchangeSpec>,
    #[serde(default)]
    pub queues: Vec<QueueSpec>,
    #[serde(default)]
    pub bindings: Vec<BindingSpec>,
}

fn default_true() -> bool {
    true
}

#[async_trait]
pub trait TopologyManager: Send + Sync {
    /// Idempotent: an identical existing exchange succeeds, a conflicting one fails.
    async fn declare_exchange(&self, spec: &ExchangeSpec) -> Result<(), TopologyError>;
    async fn declare_queue(&self, spec: &QueueSpec) -> Result<(), TopologyError>;
    async fn bind(&self, spec: &BindingSpec) -> Result<(), TopologyError>;
}

/// Declares exchanges, then queues, then bindings, stopping at the first failure.
pub async fn apply(
    manager: &dyn TopologyManager,
    topology: &Topology,
) -> Result<(), TopologyError> {
    for spec in &topology.exchanges {
        manager.declare_exchange(spec).await?;
        tracing::info!(exchange = %spec.name, "declared exchange");
    }
    for spec in &topology.queues {
        manager.declare_queue(spec).await?;
        tracing::info!(queue = %spec.name, "declared queue");
    }
    for spec in &topology.bindings {
        manager.bind(spec).await?;
        tracing::info!(queue = %spec.queue, exchange = %spec.exchange, routing_key = %spec.routing_key, "declared binding");
    }
    Ok(())
}

impl Topology {
    pub fn is_empty(&self) -> bool {
        self.exchanges.is_empty() && self.queues.is_empty() && self.bindings.is_empty()
    }

    /// Checks names, bounds, and duplicates so nothing invalid reaches the broker.
    pub fn validate(&self) -> Result<(), String> {
        let mut exchanges = HashSet::new();
        for (i, exchange) in self.exchanges.iter().enumerate() {
            check_declared_name(&format!("topology.exchanges[{i}].name"), &exchange.name)?;
            if !exchanges.insert(exchange.name.as_str()) {
                return Err(format!("exchange `{}` is declared twice", exchange.name));
            }
        }

        let mut queues = HashSet::new();
        for (i, queue) in self.queues.iter().enumerate() {
            let field = format!("topology.queues[{i}]");
            check_declared_name(&format!("{field}.name"), &queue.name)?;
            if !queues.insert(queue.name.as_str()) {
                return Err(format!("queue `{}` is declared twice", queue.name));
            }
            if let Some(dlx) = &queue.dead_letter_exchange {
                check_name(&format!("{field}.dead_letter_exchange"), dlx)?;
            }
            if let Some(key) = &queue.dead_letter_routing_key {
                check_routing_key(&format!("{field}.dead_letter_routing_key"), key)?;
                if queue.dead_letter_exchange.is_none() {
                    return Err(format!(
                        "{field}.dead_letter_routing_key requires dead_letter_exchange"
                    ));
                }
            }
            if let Some(ttl) = queue.message_ttl {
                let ms = ttl.as_millis();
                if ms == 0 || ms > u128::from(u32::MAX) {
                    return Err(format!(
                        "{field}.message_ttl must be between 1ms and {}ms",
                        u32::MAX
                    ));
                }
            }
        }

        for (i, binding) in self.bindings.iter().enumerate() {
            let field = format!("topology.bindings[{i}]");
            check_name(&format!("{field}.queue"), &binding.queue)?;
            check_name(&format!("{field}.exchange"), &binding.exchange)?;
            check_routing_key(&format!("{field}.routing_key"), &binding.routing_key)?;
        }
        Ok(())
    }
}

fn check_name(field: &str, name: &str) -> Result<(), String> {
    if is_valid_name(name) {
        Ok(())
    } else {
        Err(format!(
            "{field} must be 1-{MAX_NAME_LEN} characters of letters, digits, '-', '_', '.' or ':'"
        ))
    }
}

/// Names being declared must not use the broker's reserved `amq.` prefix.
fn check_declared_name(field: &str, name: &str) -> Result<(), String> {
    check_name(field, name)?;
    if name.starts_with(RESERVED_PREFIX) {
        return Err(format!(
            "{field} must not start with the reserved prefix `{RESERVED_PREFIX}`"
        ));
    }
    Ok(())
}

fn check_routing_key(field: &str, key: &str) -> Result<(), String> {
    if is_valid_routing_key(key) {
        Ok(())
    } else {
        Err(format!(
            "{field} must be at most {MAX_ROUTING_KEY_LEN} bytes without control characters"
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    fn parse(yaml: &str) -> Topology {
        serde_yaml_ng::from_str(yaml).unwrap()
    }

    fn error(yaml: &str) -> String {
        parse(yaml).validate().unwrap_err()
    }

    #[test]
    fn parses_full_topology_with_defaults() {
        let topology = parse(
            "
exchanges:
  - { name: orders, kind: topic }
queues:
  - name: orders.created
    dead_letter_exchange: orders.dlx
    dead_letter_routing_key: dead
    message_ttl: 30s
bindings:
  - { queue: orders.created, exchange: orders, routing_key: order.created }
  - { queue: orders.created, exchange: orders }
",
        );
        topology.validate().unwrap();
        assert!(topology.exchanges[0].durable);
        assert_eq!(topology.exchanges[0].kind, ExchangeKind::Topic);
        assert!(topology.queues[0].durable);
        assert_eq!(
            topology.queues[0].message_ttl,
            Some(Duration::from_secs(30))
        );
        assert_eq!(topology.bindings[1].routing_key, "");
        assert!(!topology.is_empty());
    }

    #[test]
    fn empty_topology_is_valid() {
        let topology = parse("{}");
        assert!(topology.is_empty());
        topology.validate().unwrap();
    }

    #[test]
    fn unknown_fields_and_kinds_fail_to_parse() {
        assert!(
            serde_yaml_ng::from_str::<Topology>("exchanges: [{name: a, kind: topic, x: 1}]")
                .is_err()
        );
        assert!(
            serde_yaml_ng::from_str::<Topology>("exchanges: [{name: a, kind: headers}]").is_err()
        );
        assert!(serde_yaml_ng::from_str::<Topology>("exchange: []").is_err());
    }

    #[test]
    fn invalid_and_reserved_names_are_rejected() {
        assert!(error("queues: [{name: 'has space'}]").contains("queues[0].name"));
        assert!(error("exchanges: [{name: '', kind: direct}]").contains("exchanges[0].name"));
        assert!(error("exchanges: [{name: amq.direct, kind: direct}]").contains("reserved"));
        assert!(error("queues: [{name: amq.jobs}]").contains("reserved"));
    }

    #[test]
    fn duplicates_are_rejected() {
        assert!(
            error("exchanges: [{name: a, kind: direct}, {name: a, kind: fanout}]")
                .contains("twice")
        );
        assert!(error("queues: [{name: q}, {name: q}]").contains("twice"));
    }

    #[test]
    fn dead_letter_and_ttl_rules() {
        assert!(
            error("queues: [{name: q, dead_letter_exchange: 'a b'}]")
                .contains("dead_letter_exchange")
        );
        assert!(
            error("queues: [{name: q, dead_letter_routing_key: k}]")
                .contains("requires dead_letter_exchange")
        );
        assert!(error("queues: [{name: q, message_ttl: 0s}]").contains("message_ttl"));
        assert!(error("queues: [{name: q, message_ttl: 100days}]").contains("message_ttl"));
    }

    #[test]
    fn bindings_may_target_broker_exchanges_but_are_validated() {
        parse("bindings: [{queue: q, exchange: amq.topic, routing_key: 'a.#'}]")
            .validate()
            .unwrap();
        assert!(error("bindings: [{queue: '', exchange: e}]").contains("bindings[0].queue"));
        assert!(
            error("bindings: [{queue: q, exchange: e, routing_key: \"a\\nb\"}]")
                .contains("routing_key")
        );
    }

    #[derive(Default)]
    struct Recorder {
        calls: Mutex<Vec<String>>,
        fail_on: Option<&'static str>,
    }

    impl Recorder {
        fn record(&self, call: String) -> Result<(), TopologyError> {
            self.calls.lock().unwrap().push(call.clone());
            if self.fail_on == Some(call.as_str()) {
                return Err(TopologyError::Unavailable("boom".into()));
            }
            Ok(())
        }
    }

    #[async_trait]
    impl TopologyManager for Recorder {
        async fn declare_exchange(&self, spec: &ExchangeSpec) -> Result<(), TopologyError> {
            self.record(format!("exchange:{}", spec.name))
        }
        async fn declare_queue(&self, spec: &QueueSpec) -> Result<(), TopologyError> {
            self.record(format!("queue:{}", spec.name))
        }
        async fn bind(&self, spec: &BindingSpec) -> Result<(), TopologyError> {
            self.record(format!("bind:{}:{}", spec.queue, spec.exchange))
        }
    }

    const SAMPLE: &str = "
bindings: [{queue: q1, exchange: e1}]
queues: [{name: q1}, {name: q2}]
exchanges: [{name: e1, kind: direct}]
";

    #[tokio::test]
    async fn apply_declares_exchanges_then_queues_then_bindings() {
        let recorder = Recorder::default();
        apply(&recorder, &parse(SAMPLE)).await.unwrap();
        assert_eq!(
            *recorder.calls.lock().unwrap(),
            ["exchange:e1", "queue:q1", "queue:q2", "bind:q1:e1"]
        );
    }

    #[tokio::test]
    async fn apply_stops_at_the_first_failure() {
        let recorder = Recorder {
            fail_on: Some("queue:q1"),
            ..Default::default()
        };
        let err = apply(&recorder, &parse(SAMPLE)).await.unwrap_err();
        assert!(matches!(err, TopologyError::Unavailable(_)));
        assert_eq!(*recorder.calls.lock().unwrap(), ["exchange:e1", "queue:q1"]);
    }
}
