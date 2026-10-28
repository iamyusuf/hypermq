pub mod rabbitmq;
pub mod topology;

use std::collections::BTreeMap;

use async_trait::async_trait;
use bytes::Bytes;

pub const MAX_NAME_LEN: usize = 255;
pub const MAX_ROUTING_KEY_LEN: usize = 255;

/// Exchange and queue names: letters, digits, and `-_.:` only, at most 255 bytes.
pub fn is_valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME_LEN
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
}

/// Routing keys and binding patterns: at most 255 bytes, no control characters; may be empty.
pub fn is_valid_routing_key(key: &str) -> bool {
    key.len() <= MAX_ROUTING_KEY_LEN && !key.chars().any(char::is_control)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishTarget {
    Exchange(String),
    /// Published through the default exchange using the queue name as the routing key.
    Queue(String),
}

/// A validated, size-bounded message ready to publish.
#[derive(Debug, Clone)]
pub struct PublishRequest {
    pub target: PublishTarget,
    /// Ignored for `PublishTarget::Queue`.
    pub routing_key: String,
    /// Stable id that travels with the message so consumers can deduplicate.
    pub message_id: String,
    pub content_type: String,
    pub headers: BTreeMap<String, String>,
    pub body: Bytes,
}

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    /// Connection, channel, or confirm failure; the caller may retry.
    #[error("broker unavailable: {0}")]
    Unavailable(String),
    /// The broker accepted the message but no queue received it.
    #[error("message was not routed to any queue")]
    Unroutable,
    /// The target exchange does not exist.
    #[error("target not found")]
    TargetNotFound,
    /// The broker negatively acknowledged the message.
    #[error("broker rejected the message")]
    Rejected,
}

#[async_trait]
pub trait Publisher: Send + Sync {
    /// Publishes and waits for the broker confirm.
    async fn publish(&self, request: PublishRequest) -> Result<(), PublishError>;
}
