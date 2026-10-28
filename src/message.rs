use std::collections::BTreeMap;
use std::fmt;

use bytes::Bytes;

/// Stable across redeliveries of the same message; Tasks pass it downstream for idempotency.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MessageId(String);

impl MessageId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingInfo {
    pub exchange: String,
    pub routing_key: String,
    pub queue: String,
}

/// A message as seen by a Task; independent of the broker client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub id: MessageId,
    pub body: Bytes,
    pub headers: BTreeMap<String, String>,
    pub routing: RoutingInfo,
    /// Delivery attempt, starting at 1.
    pub attempt: u32,
}
