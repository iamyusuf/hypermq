//! Persistent fallback for publishes that RabbitMQ cannot take.
//!
//! [`OutboxPublisher`] tries the broker first and stores the message only when the broker is
//! unavailable. The relay in [`relay`] publishes stored rows later, in order per ordering key.
//! Database code lives behind [`OutboxStore`]; the SeaORM implementation is in [`sea`].

pub mod publisher;
pub mod relay;
#[cfg(feature = "outbox")]
pub mod sea;

use std::sync::Arc;
#[cfg(any(feature = "outbox", test))]
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

#[cfg(any(feature = "outbox", test))]
use crate::broker::PublishTarget;
use crate::broker::{PublishRequest, Publisher};

pub use publisher::OutboxPublisher;
pub use relay::{RelayPolicy, Settled};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// A database call failed. Never contains the connection URL.
    #[error("outbox database error: {0}")]
    Database(String),
    /// The binary was built without the `outbox` feature.
    #[error("{0}")]
    Unsupported(&'static str),
}

/// Counts and ages reported to the gauges and `/health`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub depth: u64,
    pub oldest_age_seconds: u64,
    pub dead_letters: u64,
}

/// Persistence for stored publishes. Implementations must keep each ordering key in order.
#[async_trait]
pub trait OutboxStore: Send + Sync {
    /// Stores the request unless `max_messages` rows already wait. Returns `Ok(false)` when full.
    async fn insert(
        &self,
        request: &PublishRequest,
        now: DateTime<Utc>,
        max_messages: u64,
    ) -> Result<bool, StoreError>;

    /// Moves rows older than `policy.max_age` to the dead-letter store. Returns how many moved.
    async fn expire(&self, policy: &RelayPolicy, now: DateTime<Utc>) -> Result<u64, StoreError>;

    /// Settles the oldest ready row that is first for its ordering key.
    /// Returns `None` when no row is ready.
    async fn relay_one(
        &self,
        publisher: &dyn Publisher,
        policy: &RelayPolicy,
        now: DateTime<Utc>,
    ) -> Result<Option<Settled>, StoreError>;

    async fn snapshot(&self, now: DateTime<Utc>) -> Result<Snapshot, StoreError>;
}

/// Connects to the outbox database and returns its store.
#[cfg(feature = "outbox")]
pub async fn connect(url: &str) -> Result<Arc<dyn OutboxStore>, StoreError> {
    Ok(Arc::new(sea::SeaStore::connect(url).await?))
}

/// Connects to the outbox database and returns its store.
#[cfg(not(feature = "outbox"))]
pub async fn connect(_url: &str) -> Result<Arc<dyn OutboxStore>, StoreError> {
    Err(StoreError::Unsupported(
        "outbox is configured but this build has no outbox support (enable the `outbox` feature)",
    ))
}

/// Creates or upgrades the outbox tables.
#[cfg(feature = "outbox")]
pub async fn migrate(url: &str) -> Result<(), StoreError> {
    sea::SeaStore::migrate(url).await
}

/// Creates or upgrades the outbox tables.
#[cfg(not(feature = "outbox"))]
pub async fn migrate(_url: &str) -> Result<(), StoreError> {
    Err(StoreError::Unsupported(
        "this build has no outbox support (enable the `outbox` feature)",
    ))
}

/// Routing key used for ordering. Queue targets have no routing key, so they use the empty string.
#[cfg(any(feature = "outbox", test))]
pub(crate) fn ordering_routing_key(request: &PublishRequest) -> &str {
    match request.target {
        PublishTarget::Queue(_) => "",
        PublishTarget::Exchange(_) => &request.routing_key,
    }
}

/// True when two requests must be relayed in order (same target and routing key).
#[cfg(test)]
pub(crate) fn same_ordering_key(a: &PublishRequest, b: &PublishRequest) -> bool {
    a.target == b.target && ordering_routing_key(a) == ordering_routing_key(b)
}

/// Span used when a duration is too large to add. Far enough to act as "never", and still inside
/// every database's timestamp range (PostgreSQL cannot store chrono's extreme bounds).
#[cfg(any(feature = "outbox", test))]
const SATURATION_DAYS: i64 = 36_500;

/// The instant `delay` after `now`, saturating at about 100 years out.
#[cfg(any(feature = "outbox", test))]
pub(crate) fn after(now: DateTime<Utc>, delay: Duration) -> DateTime<Utc> {
    chrono::Duration::from_std(delay)
        .ok()
        .and_then(|delay| now.checked_add_signed(delay))
        .unwrap_or(now + chrono::Duration::days(SATURATION_DAYS))
}

/// Rows created before this instant have outlived `max_age`. Saturates at about 100 years ago.
#[cfg(any(feature = "outbox", test))]
pub(crate) fn expiry_cutoff(now: DateTime<Utc>, max_age: Duration) -> DateTime<Utc> {
    chrono::Duration::from_std(max_age)
        .ok()
        .and_then(|age| now.checked_sub_signed(age))
        .unwrap_or(now - chrono::Duration::days(SATURATION_DAYS))
}

/// Whole seconds from `since` to `now`, never negative.
#[cfg(any(feature = "outbox", test))]
pub(crate) fn age_seconds(now: DateTime<Utc>, since: DateTime<Utc>) -> u64 {
    u64::try_from((now - since).num_seconds()).unwrap_or(0)
}

#[cfg(test)]
pub(crate) mod testing {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;
    use bytes::Bytes;
    use chrono::{DateTime, Utc};

    use super::*;
    use crate::broker::{Accepted, PublishError, PublishRequest, PublishTarget, Publisher};

    /// Builds a request with a fixed content type and body.
    pub(crate) fn request(
        message_id: &str,
        target: PublishTarget,
        routing_key: &str,
    ) -> PublishRequest {
        PublishRequest {
            target,
            routing_key: routing_key.to_owned(),
            message_id: message_id.to_owned(),
            content_type: "application/json".to_owned(),
            headers: Default::default(),
            body: Bytes::from_static(b"{}"),
        }
    }

    /// Broker fake that replays scripted results, then accepts everything.
    /// Records the message ids it accepted, in order.
    #[derive(Default)]
    pub(crate) struct ScriptedPublisher {
        script: Mutex<VecDeque<Result<Accepted, PublishError>>>,
        pub(crate) accepted: Mutex<Vec<String>>,
    }

    impl ScriptedPublisher {
        pub(crate) fn new(script: Vec<Result<Accepted, PublishError>>) -> Self {
            Self {
                script: Mutex::new(script.into()),
                accepted: Mutex::default(),
            }
        }
    }

    #[async_trait]
    impl Publisher for ScriptedPublisher {
        async fn publish(&self, request: PublishRequest) -> Result<Accepted, PublishError> {
            let next = self
                .script
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(Accepted::Published));
            if next.is_ok() {
                self.accepted.lock().unwrap().push(request.message_id);
            }
            next
        }
    }

    struct Row {
        request: PublishRequest,
        attempts: u32,
        created_at: DateTime<Utc>,
        next_attempt_at: DateTime<Utc>,
    }

    /// In-memory store with the same rules as the database store.
    #[derive(Default)]
    pub(crate) struct MemoryStore {
        rows: Mutex<Vec<Row>>,
        pub(crate) dead: Mutex<Vec<&'static str>>,
        pub(crate) fail: AtomicBool,
    }

    impl MemoryStore {
        pub(crate) fn depth(&self) -> usize {
            self.rows.lock().unwrap().len()
        }

        fn check(&self) -> Result<(), StoreError> {
            if self.fail.load(Ordering::Relaxed) {
                Err(StoreError::Database("injected failure".to_owned()))
            } else {
                Ok(())
            }
        }
    }

    #[async_trait]
    impl OutboxStore for MemoryStore {
        async fn insert(
            &self,
            request: &PublishRequest,
            now: DateTime<Utc>,
            max_messages: u64,
        ) -> Result<bool, StoreError> {
            self.check()?;
            let mut rows = self.rows.lock().unwrap();
            if u64::try_from(rows.len()).unwrap_or(u64::MAX) >= max_messages {
                return Ok(false);
            }
            rows.push(Row {
                request: request.clone(),
                attempts: 0,
                created_at: now,
                next_attempt_at: now,
            });
            Ok(true)
        }

        async fn expire(
            &self,
            policy: &RelayPolicy,
            now: DateTime<Utc>,
        ) -> Result<u64, StoreError> {
            self.check()?;
            let cutoff = expiry_cutoff(now, policy.max_age);
            let mut rows = self.rows.lock().unwrap();
            let mut dead = self.dead.lock().unwrap();
            let before = rows.len();
            rows.retain(|row| {
                let keep = row.created_at >= cutoff;
                if !keep {
                    dead.push("expired");
                }
                keep
            });
            Ok(u64::try_from(before - rows.len()).unwrap_or(0))
        }

        async fn relay_one(
            &self,
            publisher: &dyn Publisher,
            policy: &RelayPolicy,
            now: DateTime<Utc>,
        ) -> Result<Option<Settled>, StoreError> {
            self.check()?;
            let picked = {
                let rows = self.rows.lock().unwrap();
                rows.iter()
                    .enumerate()
                    .find(|(index, row)| {
                        row.next_attempt_at <= now
                            && !rows[..*index]
                                .iter()
                                .any(|earlier| same_ordering_key(&earlier.request, &row.request))
                    })
                    .map(|(_, row)| (row.request.clone(), row.attempts))
            };
            let Some((request, attempts)) = picked else {
                return Ok(None);
            };
            let message_id = request.message_id.clone();

            let result = publisher.publish(request).await;
            let settled = relay::decide(&result, attempts, policy);

            let mut rows = self.rows.lock().unwrap();
            let Some(index) = rows
                .iter()
                .position(|row| row.request.message_id == message_id)
            else {
                return Ok(Some(settled));
            };
            match &settled {
                Settled::Delivered => {
                    rows.remove(index);
                }
                Settled::Dead { reason } => {
                    rows.remove(index);
                    self.dead.lock().unwrap().push(*reason);
                }
                Settled::Retry {
                    attempts,
                    delay,
                    last_error: _,
                } => {
                    rows[index].attempts = *attempts;
                    rows[index].next_attempt_at = after(now, *delay);
                }
                Settled::BrokerDown => {}
            }
            Ok(Some(settled))
        }

        async fn snapshot(&self, now: DateTime<Utc>) -> Result<Snapshot, StoreError> {
            self.check()?;
            let rows = self.rows.lock().unwrap();
            let oldest = rows.iter().map(|row| row.created_at).min();
            Ok(Snapshot {
                depth: u64::try_from(rows.len()).unwrap_or(u64::MAX),
                oldest_age_seconds: oldest.map_or(0, |created| age_seconds(now, created)),
                dead_letters: u64::try_from(self.dead.lock().unwrap().len()).unwrap_or(u64::MAX),
            })
        }
    }
}
