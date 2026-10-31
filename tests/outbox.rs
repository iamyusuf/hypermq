//! Needs running databases. Run with:
//! `HYPERMQ_TEST_POSTGRES_URL=postgres://... HYPERMQ_TEST_MYSQL_URL=mysql://... cargo test --test outbox -- --ignored`
//! Use a dedicated database: each test expects rows it inserts to be the only ones it relays.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use hypermq::broker::{Accepted, PublishError, PublishRequest, PublishTarget, Publisher};
use hypermq::outbox::{OutboxStore, RelayPolicy, Settled, connect, migrate};
use uuid::Uuid;

const MAX_MESSAGES: u64 = 100;

fn policy() -> RelayPolicy {
    RelayPolicy {
        max_attempts: 5,
        base_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(60),
        max_age: Duration::from_secs(24 * 60 * 60),
        cooldown: Duration::from_secs(5),
        degraded_after: Duration::from_secs(300),
    }
}

/// A whole-second instant in the past. MySQL `DATETIME` stores whole seconds, so this value
/// round-trips unchanged and the "ready now" check does not depend on rounding.
fn fixed_now() -> DateTime<Utc> {
    DateTime::from_timestamp(Utc::now().timestamp() - 60, 0).expect("valid timestamp")
}

fn request(queue: &str) -> PublishRequest {
    PublishRequest {
        target: PublishTarget::Queue(queue.to_owned()),
        routing_key: String::new(),
        message_id: Uuid::new_v4().to_string(),
        content_type: "application/json".into(),
        headers: BTreeMap::from([("source".to_owned(), "outbox-test".to_owned())]),
        body: Bytes::from_static(br#"{"hello":"world"}"#),
    }
}

/// Accepts every publish and records the message ids in order.
#[derive(Default)]
struct Recorder {
    ids: Mutex<Vec<String>>,
}

#[async_trait]
impl Publisher for Recorder {
    async fn publish(&self, request: PublishRequest) -> Result<Accepted, PublishError> {
        self.ids
            .lock()
            .expect("recorder lock")
            .push(request.message_id);
        Ok(Accepted::Published)
    }
}

/// Simulates a broker that is down.
struct Down;

#[async_trait]
impl Publisher for Down {
    async fn publish(&self, _request: PublishRequest) -> Result<Accepted, PublishError> {
        Err(PublishError::Unavailable("broker down".into()))
    }
}

/// Relays every ready row, so the table is empty afterwards.
async fn drain(store: &dyn OutboxStore, now: DateTime<Utc>) {
    let recorder = Recorder::default();
    while store
        .relay_one(&recorder, &policy(), now)
        .await
        .expect("relay")
        .is_some()
    {}
}

fn env_url(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("set {name} to run this test"))
}

async fn exercise(url: &str) {
    migrate(url).await.expect("migrate");
    // Running migrations again must be a no-op.
    migrate(url).await.expect("second migrate");

    let store = connect(url).await.expect("connect");
    let now = fixed_now();
    drain(&*store, now).await;

    // Capacity: a second insert is refused while one row waits.
    assert!(
        store
            .insert(&request("capacity"), now, 1)
            .await
            .expect("insert")
    );
    assert!(
        !store
            .insert(&request("capacity"), now, 1)
            .await
            .expect("insert")
    );
    drain(&*store, now).await;
    assert_eq!(store.snapshot(now).await.expect("snapshot").depth, 0);

    // Order: rows with the same key relay first-in, first-out. The later row waits for the head.
    let first = request("orders");
    let second = request("orders");
    let other = request("invoices");
    for row in [&first, &second, &other] {
        assert!(store.insert(row, now, MAX_MESSAGES).await.expect("insert"));
    }
    let recorder = Recorder::default();
    for _ in 0..3 {
        assert!(matches!(
            store
                .relay_one(&recorder, &policy(), now)
                .await
                .expect("relay"),
            Some(Settled::Delivered)
        ));
    }
    assert!(
        store
            .relay_one(&recorder, &policy(), now)
            .await
            .expect("relay")
            .is_none()
    );
    let order = recorder.ids.lock().expect("recorder lock").clone();
    assert_eq!(
        order,
        vec![
            first.message_id.clone(),
            second.message_id.clone(),
            other.message_id.clone(),
        ]
    );

    // A broker outage leaves the row in place for a later cycle.
    assert!(
        store
            .insert(&request("held"), now, MAX_MESSAGES)
            .await
            .expect("insert")
    );
    assert!(matches!(
        store.relay_one(&Down, &policy(), now).await.expect("relay"),
        Some(Settled::BrokerDown)
    ));
    assert_eq!(store.snapshot(now).await.expect("snapshot").depth, 1);
    assert!(matches!(
        store
            .relay_one(&Recorder::default(), &policy(), now)
            .await
            .expect("relay"),
        Some(Settled::Delivered)
    ));
    assert_eq!(store.snapshot(now).await.expect("snapshot").depth, 0);

    // Rows older than max_age move to the dead-letter table.
    let dead_before = store.snapshot(now).await.expect("snapshot").dead_letters;
    assert!(
        store
            .insert(&request("aged"), now, MAX_MESSAGES)
            .await
            .expect("insert")
    );
    let later = now + ChronoDuration::days(2);
    assert_eq!(store.expire(&policy(), later).await.expect("expire"), 1);
    let after = store.snapshot(later).await.expect("snapshot");
    assert_eq!(after.depth, 0);
    assert_eq!(after.dead_letters, dead_before + 1);
}

#[tokio::test]
#[ignore = "needs PostgreSQL"]
async fn postgres_outbox_round_trip() {
    exercise(&env_url("HYPERMQ_TEST_POSTGRES_URL")).await;
}

#[tokio::test]
#[ignore = "needs MySQL"]
async fn mysql_outbox_round_trip() {
    exercise(&env_url("HYPERMQ_TEST_MYSQL_URL")).await;
}
