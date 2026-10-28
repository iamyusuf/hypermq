//! The `Task` abstraction: the unit of work run for one consumed message.
//!
//! The consumer engine depends only on this module. Concrete kinds live in submodules
//! and are registered in a [`TaskRegistry`] by their `type` tag.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::message::Message;

#[cfg(feature = "task-http")]
pub mod http;

#[async_trait]
pub trait Task: Send + Sync {
    /// Runs the work for one message. It may run twice for the same message, so it must be idempotency-aware.
    async fn execute(&self, message: &Message) -> TaskOutcome;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskOutcome {
    /// Work done; the engine acks.
    Success,
    /// Transient failure; the retry policy decides between retry and dead-letter.
    Retryable {
        reason: String,
        /// Overrides the policy's backoff delay (capped by the policy's maximum).
        retry_after: Option<Duration>,
    },
    /// Will never succeed; the engine dead-letters without retry.
    Permanent { reason: String },
}

#[derive(Debug, thiserror::Error)]
pub enum TaskConfigError {
    #[error("unknown task type `{0}`")]
    UnknownKind(String),
    #[error("invalid settings for task type `{kind}`: {message}")]
    InvalidSettings { kind: String, message: String },
}

/// Builds a [`Task`] of one kind from its settings. Called at config load, never at message time.
pub trait TaskFactory: Send + Sync {
    /// The `type` tag this factory handles, for example `"http"`.
    fn kind(&self) -> &'static str;

    /// Validates kind-specific settings and builds the Task.
    fn build(&self, settings: serde_json::Value) -> Result<Arc<dyn Task>, TaskConfigError>;
}

#[derive(Default)]
pub struct TaskRegistry {
    factories: HashMap<&'static str, Arc<dyn TaskFactory>>,
}

impl TaskRegistry {
    /// Registers a factory under its kind; a later registration for the same kind replaces the earlier one.
    pub fn register(&mut self, factory: Arc<dyn TaskFactory>) {
        self.factories.insert(factory.kind(), factory);
    }

    pub fn build(
        &self,
        kind: &str,
        settings: serde_json::Value,
    ) -> Result<Arc<dyn Task>, TaskConfigError> {
        let factory = self
            .factories
            .get(kind)
            .ok_or_else(|| TaskConfigError::UnknownKind(kind.to_owned()))?;
        factory.build(settings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{MessageId, RoutingInfo};
    use bytes::Bytes;
    use std::collections::BTreeMap;

    struct EchoTask;

    #[async_trait]
    impl Task for EchoTask {
        async fn execute(&self, _message: &Message) -> TaskOutcome {
            TaskOutcome::Success
        }
    }

    struct EchoFactory;

    impl TaskFactory for EchoFactory {
        fn kind(&self) -> &'static str {
            "echo"
        }

        fn build(&self, settings: serde_json::Value) -> Result<Arc<dyn Task>, TaskConfigError> {
            if settings.get("fail").is_some() {
                return Err(TaskConfigError::InvalidSettings {
                    kind: "echo".into(),
                    message: "fail is not allowed".into(),
                });
            }
            Ok(Arc::new(EchoTask))
        }
    }

    fn message() -> Message {
        Message {
            id: MessageId::new("m-1"),
            body: Bytes::from_static(b"{}"),
            headers: BTreeMap::new(),
            routing: RoutingInfo {
                exchange: String::new(),
                routing_key: "jobs".into(),
                queue: "jobs".into(),
            },
            attempt: 1,
        }
    }

    fn registry() -> TaskRegistry {
        let mut registry = TaskRegistry::default();
        registry.register(Arc::new(EchoFactory));
        registry
    }

    #[tokio::test]
    async fn registered_kind_builds_a_task() {
        let task = registry().build("echo", serde_json::json!({})).unwrap();
        assert_eq!(task.execute(&message()).await, TaskOutcome::Success);
    }

    #[test]
    fn unknown_kind_fails() {
        let err = registry()
            .build("sms", serde_json::json!({}))
            .err()
            .unwrap();
        assert!(matches!(err, TaskConfigError::UnknownKind(kind) if kind == "sms"));
    }

    #[test]
    fn invalid_settings_are_reported_by_the_factory() {
        let err = registry()
            .build("echo", serde_json::json!({"fail": true}))
            .err()
            .unwrap();
        assert!(matches!(err, TaskConfigError::InvalidSettings { .. }));
    }
}
