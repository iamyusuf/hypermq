//! Consumer configuration and the consumer engine.
//!
//! The engine depends only on `Broker`, `Task`, and `RetryPolicy`; it never names a concrete Task kind.

pub mod consumer;
pub mod retry;

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;

use crate::broker::{MAX_NAME_LEN, is_valid_name};
use crate::error::ConfigError;
use crate::tasks::TaskRegistry;
use consumer::Consumer;
use retry::RetryConfig;

pub const DEFAULT_PREFETCH: u16 = 10;
pub const MAX_PREFETCH: u16 = 1000;
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerConfig {
    /// Unique label used in logs.
    pub name: String,
    pub queue: String,
    #[serde(default = "default_prefetch")]
    pub prefetch: u16,
    /// Per-message limit on `Task::execute`; a timeout counts as retryable.
    #[serde(default = "default_timeout", with = "humantime_serde")]
    pub timeout: Duration,
    pub retry: RetryConfig,
    pub task: TaskConfig,
}

fn default_prefetch() -> u16 {
    DEFAULT_PREFETCH
}

fn default_timeout() -> Duration {
    DEFAULT_TIMEOUT
}

/// The `type` tag plus kind-specific settings, which are opaque here and passed to the TaskFactory.
#[derive(Clone, Deserialize)]
pub struct TaskConfig {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(flatten)]
    pub settings: serde_json::Map<String, serde_json::Value>,
}

impl fmt::Debug for TaskConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Settings often hold credentials, so they are never printed.
        f.debug_struct("TaskConfig")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

/// Checks names, bounds, and duplicates. Unknown Task kinds are checked by [`build`].
pub fn validate(consumers: &[ConsumerConfig]) -> Result<(), String> {
    let mut names = HashSet::new();
    for (i, consumer) in consumers.iter().enumerate() {
        let field = format!("consumers[{i}]");
        if !is_valid_name(&consumer.name) {
            return Err(format!(
                "{field}.name must be 1 to {MAX_NAME_LEN} letters, digits, or -_.: characters"
            ));
        }
        if !names.insert(consumer.name.as_str()) {
            return Err(format!("consumer `{}` is declared twice", consumer.name));
        }
        if !is_valid_name(&consumer.queue) {
            return Err(format!("{field}.queue is not a valid queue name"));
        }
        if consumer.prefetch == 0 || consumer.prefetch > MAX_PREFETCH {
            return Err(format!(
                "{field}.prefetch must be between 1 and {MAX_PREFETCH}"
            ));
        }
        if consumer.timeout.is_zero() {
            return Err(format!("{field}.timeout must be greater than zero"));
        }
        if consumer.retry.max_attempts == 0 {
            return Err(format!("{field}.retry.max_attempts must be at least 1"));
        }
        if consumer.retry.base_delay > consumer.retry.max_delay {
            return Err(format!(
                "{field}.retry.base_delay must not exceed retry.max_delay"
            ));
        }
    }
    Ok(())
}

/// Validates the consumers and builds each Task through the registry.
/// Unknown `type` tags and invalid settings fail here, at config load.
pub fn build(
    consumers: &[ConsumerConfig],
    registry: &TaskRegistry,
) -> Result<Vec<Consumer>, ConfigError> {
    validate(consumers).map_err(ConfigError::Invalid)?;
    consumers
        .iter()
        .map(|config| {
            let settings = serde_json::Value::Object(config.task.settings.clone());
            let task = registry.build(&config.task.kind, settings).map_err(|err| {
                ConfigError::Invalid(format!("consumer `{}`: {err}", config.name))
            })?;
            Ok(Consumer::new(
                config.name.clone(),
                config.queue.clone(),
                config.prefetch,
                config.timeout,
                task,
                Arc::new(config.retry.clone()),
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Message;
    use crate::tasks::TaskOutcome;
    use crate::tasks::{Task, TaskConfigError, TaskFactory};
    use async_trait::async_trait;

    const VALID: &str = r#"
name: notify
queue: orders.created
prefetch: 4
timeout: 5s
retry:
  max_attempts: 3
  base_delay: 1s
task:
  type: fake
  url: https://example.internal/hook
  headers:
    Authorization: Bearer secret-token
"#;

    fn parse(yaml: &str) -> ConsumerConfig {
        serde_yaml_ng::from_str(yaml).unwrap()
    }

    struct NoopTask;

    #[async_trait]
    impl Task for NoopTask {
        async fn execute(&self, _message: &Message) -> TaskOutcome {
            TaskOutcome::Success
        }
    }

    struct FakeFactory;

    impl TaskFactory for FakeFactory {
        fn kind(&self) -> &'static str {
            "fake"
        }

        fn build(&self, settings: serde_json::Value) -> Result<Arc<dyn Task>, TaskConfigError> {
            if settings.get("url").is_none() {
                return Err(TaskConfigError::InvalidSettings {
                    kind: "fake".into(),
                    message: "url is required".into(),
                });
            }
            Ok(Arc::new(NoopTask))
        }
    }

    fn registry() -> TaskRegistry {
        let mut registry = TaskRegistry::default();
        registry.register(Arc::new(FakeFactory));
        registry
    }

    #[test]
    fn parses_settings_and_defaults() {
        let minimal = parse(
            "name: a\nqueue: q\nretry:\n  max_attempts: 2\n  base_delay: 1s\ntask:\n  type: fake\n",
        );
        assert_eq!(minimal.prefetch, DEFAULT_PREFETCH);
        assert_eq!(minimal.timeout, DEFAULT_TIMEOUT);
        assert_eq!(minimal.task.kind, "fake");
        assert!(minimal.task.settings.is_empty());

        let full = parse(VALID);
        assert_eq!(full.prefetch, 4);
        assert_eq!(full.timeout, Duration::from_secs(5));
        assert!(full.task.settings.contains_key("url"));
        assert!(validate(&[full]).is_ok());
    }

    #[test]
    fn rejects_unknown_consumer_keys() {
        let yaml = "name: a\nqueue: q\nretry:\n  max_attempts: 2\n  base_delay: 1s\ntask:\n  type: fake\nqueu: typo\n";
        assert!(serde_yaml_ng::from_str::<ConsumerConfig>(yaml).is_err());
    }

    #[test]
    fn debug_output_hides_task_settings() {
        let debug = format!("{:?}", parse(VALID));
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("example.internal"));
    }

    #[test]
    fn duplicate_names_are_rejected() {
        let a = parse(VALID);
        let err = validate(&[a.clone(), a]).unwrap_err();
        assert!(err.contains("declared twice"));
    }

    #[test]
    fn invalid_queue_name_is_rejected() {
        let mut config = parse(VALID);
        config.queue = "bad name".into();
        assert!(validate(&[config]).unwrap_err().contains("queue"));
    }

    #[test]
    fn prefetch_bounds_are_enforced() {
        let mut config = parse(VALID);
        config.prefetch = 0;
        assert!(
            validate(std::slice::from_ref(&config))
                .unwrap_err()
                .contains("prefetch")
        );
        config.prefetch = MAX_PREFETCH + 1;
        assert!(validate(&[config]).unwrap_err().contains("prefetch"));
    }

    #[test]
    fn zero_timeout_is_rejected() {
        let mut config = parse(VALID);
        config.timeout = Duration::ZERO;
        assert!(validate(&[config]).unwrap_err().contains("timeout"));
    }

    #[test]
    fn retry_must_allow_one_attempt_and_consistent_delays() {
        let mut config = parse(VALID);
        config.retry.max_attempts = 0;
        assert!(
            validate(std::slice::from_ref(&config))
                .unwrap_err()
                .contains("max_attempts")
        );

        config.retry.max_attempts = 3;
        config.retry.base_delay = Duration::from_secs(600);
        config.retry.max_delay = Duration::from_secs(60);
        assert!(validate(&[config]).unwrap_err().contains("base_delay"));
    }

    #[test]
    fn build_creates_consumers_through_the_registry() {
        let consumers = build(&[parse(VALID)], &registry()).unwrap();
        assert_eq!(consumers.len(), 1);
    }

    #[test]
    fn build_rejects_unknown_task_type() {
        let yaml = VALID.replace("type: fake", "type: sms");
        let err = build(&[parse(&yaml)], &registry()).err().unwrap();
        assert!(
            matches!(err, ConfigError::Invalid(msg) if msg.contains("unknown task type `sms`"))
        );
    }

    #[test]
    fn build_reports_invalid_task_settings() {
        let yaml = VALID.replace("  url: https://example.internal/hook\n", "");
        let err = build(&[parse(&yaml)], &registry()).err().unwrap();
        assert!(matches!(err, ConfigError::Invalid(msg) if msg.contains("url is required")));
    }
}
