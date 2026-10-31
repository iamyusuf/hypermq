//! Relay loop: publishes stored rows and decides what happens to each one.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;

use super::{OutboxStore, Snapshot};
use crate::broker::{Accepted, PublishError, Publisher};
use crate::metrics::Metrics;
use crate::shutdown;

/// Rows settled per cycle before the relay pauses for the cooldown.
pub const BATCH_SIZE: usize = 100;

/// Largest exponent used for backoff. Keeps the multiplier inside `u32`.
const MAX_BACKOFF_EXPONENT: u32 = 20;

/// Limits that decide how the relay settles and reports rows.
#[derive(Debug, Clone)]
pub struct RelayPolicy {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub max_age: Duration,
    pub cooldown: Duration,
    pub degraded_after: Duration,
}

/// What the relay did with one row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settled {
    /// Published. The row is deleted.
    Delivered,
    /// Will never succeed. The row moves to the dead-letter store.
    Dead { reason: &'static str },
    /// Failed for now. The row waits `delay` before the next attempt.
    Retry {
        attempts: u32,
        delay: Duration,
        last_error: &'static str,
    },
    /// The broker is unavailable. The row is unchanged and the cycle stops.
    BrokerDown,
}

/// Maps a broker result to the outcome for one row.
///
/// `attempts` is the count before this try. Broker outages never count as attempts, so a row is
/// never dead-lettered just because the broker was down.
pub fn decide(
    result: &Result<Accepted, PublishError>,
    attempts: u32,
    policy: &RelayPolicy,
) -> Settled {
    match result {
        Ok(_) => Settled::Delivered,
        Err(PublishError::Unavailable(_)) => Settled::BrokerDown,
        Err(PublishError::TargetNotFound) => Settled::Dead {
            reason: "target_not_found",
        },
        Err(PublishError::Rejected) => Settled::Dead {
            reason: "publish_rejected",
        },
        Err(PublishError::Unroutable) => {
            let attempts = attempts.saturating_add(1);
            if attempts >= policy.max_attempts {
                Settled::Dead {
                    reason: "max_attempts",
                }
            } else {
                Settled::Retry {
                    attempts,
                    delay: backoff(attempts, policy),
                    last_error: "unroutable",
                }
            }
        }
    }
}

/// Delay before the next attempt: `base * 2^(attempts - 1)`, capped at `max_delay`.
pub fn backoff(attempts: u32, policy: &RelayPolicy) -> Duration {
    let exponent = attempts.saturating_sub(1).min(MAX_BACKOFF_EXPONENT);
    policy
        .base_delay
        .saturating_mul(1u32 << exponent)
        .min(policy.max_delay)
}

/// True when the outbox grew since the last cycle or its oldest row is older than `degraded_after`.
pub fn is_degraded(previous_depth: u64, snapshot: &Snapshot, policy: &RelayPolicy) -> bool {
    snapshot.depth > previous_depth || snapshot.oldest_age_seconds > policy.degraded_after.as_secs()
}

/// Runs one relay cycle: expire old rows, then settle up to `BATCH_SIZE` ready rows.
/// Returns the depth seen at the end, which the next cycle uses to detect growth.
pub async fn cycle(
    store: &dyn OutboxStore,
    broker: &dyn Publisher,
    policy: &RelayPolicy,
    metrics: &Metrics,
    previous_depth: u64,
) -> u64 {
    match store.expire(policy, Utc::now()).await {
        Ok(expired) => metrics.outbox.record_dead_lettered(expired),
        Err(err) => {
            tracing::error!(%err, "outbox expiry failed");
            metrics.outbox.record_store_error();
        }
    }

    for _ in 0..BATCH_SIZE {
        match store.relay_one(broker, policy, Utc::now()).await {
            Ok(None) => break,
            Ok(Some(Settled::Delivered)) => metrics.outbox.record_relayed(),
            Ok(Some(Settled::Dead { reason })) => {
                tracing::warn!(reason, "outbox message moved to dead letters");
                metrics.outbox.record_dead_lettered(1);
            }
            Ok(Some(Settled::Retry { .. })) => metrics.outbox.record_relay_failure(),
            Ok(Some(Settled::BrokerDown)) => {
                metrics.outbox.record_relay_failure();
                break;
            }
            Err(err) => {
                tracing::error!(%err, "outbox relay failed");
                metrics.outbox.record_store_error();
                break;
            }
        }
    }

    match store.snapshot(Utc::now()).await {
        Ok(snapshot) => {
            let degraded = is_degraded(previous_depth, &snapshot, policy);
            metrics.outbox.set_state(
                snapshot.depth,
                snapshot.oldest_age_seconds,
                snapshot.dead_letters,
                degraded,
            );
            snapshot.depth
        }
        Err(err) => {
            tracing::error!(%err, "outbox snapshot failed");
            metrics.outbox.record_store_error();
            previous_depth
        }
    }
}

/// Runs cycles forever, pausing `policy.cooldown` between them, until shutdown is requested.
/// Rows left in the store stay there for the next start.
pub async fn run(
    store: Arc<dyn OutboxStore>,
    broker: Arc<dyn Publisher>,
    policy: RelayPolicy,
    metrics: Arc<Metrics>,
    mut shutdown: shutdown::Receiver,
) {
    let mut depth = 0;
    loop {
        depth = cycle(store.as_ref(), broker.as_ref(), &policy, &metrics, depth).await;
        tokio::select! {
            () = tokio::time::sleep(policy.cooldown) => {}
            () = shutdown::requested(&mut shutdown) => break,
        }
    }
    tracing::info!("outbox relay stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::PublishTarget;
    use crate::outbox::testing::{MemoryStore, ScriptedPublisher, request};

    fn policy() -> RelayPolicy {
        RelayPolicy {
            max_attempts: 3,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(10),
            max_age: Duration::from_secs(3600),
            cooldown: Duration::from_secs(5),
            degraded_after: Duration::from_secs(300),
        }
    }

    fn queue(id: &str) -> crate::broker::PublishRequest {
        request(id, PublishTarget::Queue("orders".to_owned()), "")
    }

    #[test]
    fn decide_maps_broker_results() {
        let policy = policy();

        assert_eq!(
            decide(&Ok(Accepted::Published), 0, &policy),
            Settled::Delivered
        );
        assert_eq!(
            decide(&Err(PublishError::Unavailable("down".into())), 0, &policy),
            Settled::BrokerDown
        );
        assert_eq!(
            decide(&Err(PublishError::TargetNotFound), 0, &policy),
            Settled::Dead {
                reason: "target_not_found"
            }
        );
        assert_eq!(
            decide(&Err(PublishError::Rejected), 0, &policy),
            Settled::Dead {
                reason: "publish_rejected"
            }
        );
    }

    #[test]
    fn unroutable_retries_then_dead_letters() {
        let policy = policy();

        assert_eq!(
            decide(&Err(PublishError::Unroutable), 0, &policy),
            Settled::Retry {
                attempts: 1,
                delay: Duration::from_secs(1),
                last_error: "unroutable",
            }
        );
        assert_eq!(
            decide(&Err(PublishError::Unroutable), 2, &policy),
            Settled::Dead {
                reason: "max_attempts"
            }
        );
    }

    #[test]
    fn backoff_doubles_and_is_capped() {
        let policy = policy();

        assert_eq!(backoff(1, &policy), Duration::from_secs(1));
        assert_eq!(backoff(2, &policy), Duration::from_secs(2));
        assert_eq!(backoff(3, &policy), Duration::from_secs(4));
        assert_eq!(backoff(6, &policy), Duration::from_secs(10));
        assert_eq!(backoff(u32::MAX, &policy), Duration::from_secs(10));
    }

    #[test]
    fn degraded_when_growing_or_oldest_row_is_too_old() {
        let policy = policy();
        let snapshot = |depth, oldest_age_seconds| Snapshot {
            depth,
            oldest_age_seconds,
            dead_letters: 0,
        };

        assert!(is_degraded(3, &snapshot(5, 0), &policy));
        assert!(!is_degraded(3, &snapshot(3, 0), &policy));
        assert!(!is_degraded(3, &snapshot(3, 300), &policy));
        assert!(is_degraded(3, &snapshot(3, 301), &policy));
    }

    #[tokio::test]
    async fn cycle_drains_in_order_after_broker_recovers() {
        let store = MemoryStore::default();
        let metrics = Metrics::default();
        let now = Utc::now();
        for id in ["a", "b", "c"] {
            store.insert(&queue(id), now, 10).await.unwrap();
        }

        let down = ScriptedPublisher::new(vec![Err(PublishError::Unavailable("down".into()))]);
        let depth = cycle(&store, &down, &policy(), &metrics, 0).await;
        assert_eq!(depth, 3);
        assert!(down.accepted.lock().unwrap().is_empty());

        let up = ScriptedPublisher::new(vec![]);
        let depth = cycle(&store, &up, &policy(), &metrics, depth).await;
        assert_eq!(depth, 0);
        assert_eq!(*up.accepted.lock().unwrap(), vec!["a", "b", "c"]);
        assert!(metrics.render().contains("hypermq_outbox_relayed_total 3"));
    }

    #[tokio::test]
    async fn failing_row_blocks_only_its_ordering_key() {
        let store = MemoryStore::default();
        let metrics = Metrics::default();
        let now = Utc::now();
        let exchange =
            |id: &str, key: &str| request(id, PublishTarget::Exchange("events".to_owned()), key);
        store.insert(&exchange("a", "k"), now, 10).await.unwrap();
        store.insert(&exchange("b", "k"), now, 10).await.unwrap();
        store
            .insert(&exchange("c", "other"), now, 10)
            .await
            .unwrap();

        let broker =
            ScriptedPublisher::new(vec![Err(PublishError::Unroutable), Ok(Accepted::Published)]);
        let depth = cycle(&store, &broker, &policy(), &metrics, 0).await;

        assert_eq!(depth, 2);
        assert_eq!(*broker.accepted.lock().unwrap(), vec!["c"]);
    }

    #[tokio::test]
    async fn dead_lettered_rows_leave_the_outbox() {
        let store = MemoryStore::default();
        let metrics = Metrics::default();
        store.insert(&queue("a"), Utc::now(), 10).await.unwrap();

        let broker = ScriptedPublisher::new(vec![Err(PublishError::TargetNotFound)]);
        let depth = cycle(&store, &broker, &policy(), &metrics, 0).await;

        assert_eq!(depth, 0);
        assert_eq!(*store.dead.lock().unwrap(), vec!["target_not_found"]);
    }

    #[tokio::test]
    async fn expired_rows_are_dead_lettered() {
        let store = MemoryStore::default();
        let metrics = Metrics::default();
        let two_hours_ago = Utc::now() - chrono::Duration::hours(2);
        store.insert(&queue("a"), two_hours_ago, 10).await.unwrap();

        let broker = ScriptedPublisher::new(vec![]);
        let depth = cycle(&store, &broker, &policy(), &metrics, 0).await;

        assert_eq!(depth, 0);
        assert!(broker.accepted.lock().unwrap().is_empty());
        assert_eq!(*store.dead.lock().unwrap(), vec!["expired"]);
    }

    #[tokio::test]
    async fn store_failure_marks_outbox_degraded() {
        let store = MemoryStore::default();
        let metrics = Metrics::default();
        store.fail.store(true, std::sync::atomic::Ordering::Relaxed);

        let broker = ScriptedPublisher::new(vec![]);
        let depth = cycle(&store, &broker, &policy(), &metrics, 7).await;

        assert_eq!(depth, 7);
        assert!(metrics.outbox.is_degraded());
    }
}
