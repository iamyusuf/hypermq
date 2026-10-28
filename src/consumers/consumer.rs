use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use super::retry::{Decision, RetryPolicy};
use crate::broker::{Broker, BrokerError, Delivery, Subscription};
use crate::message::Message;
use crate::tasks::{Task, TaskOutcome};

/// Consumes one queue and settles each delivery from its Task outcome and the retry policy.
pub struct Consumer {
    name: String,
    queue: String,
    prefetch: u16,
    timeout: Duration,
    task: Arc<dyn Task>,
    policy: Arc<dyn RetryPolicy>,
}

impl Consumer {
    pub fn new(
        name: String,
        queue: String,
        prefetch: u16,
        timeout: Duration,
        task: Arc<dyn Task>,
        policy: Arc<dyn RetryPolicy>,
    ) -> Self {
        Self {
            name,
            queue,
            prefetch,
            timeout,
            task,
            policy,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Consumes until the subscription ends or fails, then waits for in-flight deliveries to settle.
    /// An error means the caller decides whether to resubscribe.
    pub async fn run(&self, broker: &dyn Broker) -> Result<(), BrokerError> {
        let mut subscription = broker.consume(&self.queue, self.prefetch).await?;
        let slots = Arc::new(Semaphore::new(usize::from(self.prefetch)));
        let result = self.pump(subscription.as_mut(), &slots).await;
        // Taking every slot waits until no delivery is still unsettled.
        let _ = slots.acquire_many(u32::from(self.prefetch)).await;
        result
    }

    async fn pump(
        &self,
        subscription: &mut dyn Subscription,
        slots: &Arc<Semaphore>,
    ) -> Result<(), BrokerError> {
        loop {
            // A slot is taken before pulling, so unsettled deliveries never exceed the prefetch.
            let slot = Arc::clone(slots)
                .acquire_owned()
                .await
                .map_err(|_| BrokerError::Operation("consumer slots were closed".into()))?;
            let Some(next) = subscription.next().await else {
                return Ok(());
            };
            let delivery = next?;

            let task = Arc::clone(&self.task);
            let policy = Arc::clone(&self.policy);
            let timeout = self.timeout;
            let name = self.name.clone();
            tokio::spawn(async move {
                settle(delivery, task, policy, timeout, &name).await;
                // The slot is released when this future ends.
                drop(slot);
            });
        }
    }
}

async fn settle(
    delivery: Box<dyn Delivery>,
    task: Arc<dyn Task>,
    policy: Arc<dyn RetryPolicy>,
    timeout: Duration,
    consumer: &str,
) {
    let message = delivery.message().clone();
    let message_id = message.id.clone();
    let attempt = message.attempt;

    let outcome = run_task(task, message, timeout).await;
    if outcome != TaskOutcome::Success {
        tracing::warn!(consumer, message_id = %message_id, attempt, outcome = ?outcome, "task did not succeed");
    }

    let decision = policy.decide(&outcome, attempt);
    let result = match decision {
        Decision::Ack => delivery.ack().await,
        Decision::Retry { delay } => delivery.retry(delay).await,
        Decision::DeadLetter => delivery.reject().await,
    };
    match result {
        Ok(()) => {
            tracing::debug!(consumer, message_id = %message_id, decision = ?decision, "delivery settled");
        }
        Err(err) => {
            tracing::error!(consumer, message_id = %message_id, %err, "failed to settle delivery");
        }
    }
}

/// Runs the Task on its own task so a panic is contained and a timeout can abort it.
async fn run_task(task: Arc<dyn Task>, message: Message, timeout: Duration) -> TaskOutcome {
    let mut handle = tokio::spawn(async move { task.execute(&message).await });
    match tokio::time::timeout(timeout, &mut handle).await {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(err)) => TaskOutcome::Retryable {
            reason: if err.is_panic() {
                "task panicked".into()
            } else {
                "task was cancelled".into()
            },
            retry_after: None,
        },
        Err(_) => {
            handle.abort();
            TaskOutcome::Retryable {
                reason: format!("task timed out after {timeout:?}"),
                retry_after: None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use bytes::Bytes;

    use super::*;
    use crate::consumers::retry::{Backoff, RetryConfig};
    use crate::message::{MessageId, RoutingInfo};

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Action {
        Ack,
        Reject,
        Retry(Duration),
    }

    type Log = Arc<Mutex<Vec<(String, Action)>>>;

    fn message(id: &str, attempt: u32) -> Message {
        Message {
            id: MessageId::new(id),
            body: Bytes::from_static(b"{}"),
            headers: BTreeMap::new(),
            routing: RoutingInfo {
                exchange: String::new(),
                routing_key: "jobs".into(),
                queue: "jobs".into(),
            },
            attempt,
        }
    }

    struct FakeDelivery {
        message: Message,
        log: Log,
    }

    impl FakeDelivery {
        fn record(&self, action: Action) {
            let id = self.message.id.as_str().to_owned();
            self.log.lock().unwrap().push((id, action));
        }
    }

    #[async_trait]
    impl Delivery for FakeDelivery {
        fn message(&self) -> &Message {
            &self.message
        }

        async fn ack(self: Box<Self>) -> Result<(), BrokerError> {
            self.record(Action::Ack);
            Ok(())
        }

        async fn reject(self: Box<Self>) -> Result<(), BrokerError> {
            self.record(Action::Reject);
            Ok(())
        }

        async fn retry(self: Box<Self>, delay: Duration) -> Result<(), BrokerError> {
            self.record(Action::Retry(delay));
            Ok(())
        }
    }

    struct FakeSubscription {
        messages: VecDeque<Message>,
        log: Log,
    }

    #[async_trait]
    impl Subscription for FakeSubscription {
        async fn next(&mut self) -> Option<Result<Box<dyn Delivery>, BrokerError>> {
            self.messages.pop_front().map(|message| {
                Ok(Box::new(FakeDelivery {
                    message,
                    log: Arc::clone(&self.log),
                }) as Box<dyn Delivery>)
            })
        }
    }

    struct FakeBroker {
        messages: Vec<Message>,
        log: Log,
        fail_consume: bool,
    }

    #[async_trait]
    impl Broker for FakeBroker {
        async fn consume(
            &self,
            _queue: &str,
            _prefetch: u16,
        ) -> Result<Box<dyn Subscription>, BrokerError> {
            if self.fail_consume {
                return Err(BrokerError::Unavailable("connection lost".into()));
            }
            Ok(Box::new(FakeSubscription {
                messages: VecDeque::from(self.messages.clone()),
                log: Arc::clone(&self.log),
            }))
        }
    }

    /// Returns the outcome chosen by a closure over the message.
    struct ClosureTask<F>(F);

    #[async_trait]
    impl<F> Task for ClosureTask<F>
    where
        F: Fn(&Message) -> TaskOutcome + Send + Sync,
    {
        async fn execute(&self, message: &Message) -> TaskOutcome {
            (self.0)(message)
        }
    }

    struct SleepingTask(Duration);

    #[async_trait]
    impl Task for SleepingTask {
        async fn execute(&self, _message: &Message) -> TaskOutcome {
            tokio::time::sleep(self.0).await;
            TaskOutcome::Success
        }
    }

    struct PanickingTask;

    #[async_trait]
    impl Task for PanickingTask {
        async fn execute(&self, _message: &Message) -> TaskOutcome {
            panic!("boom");
        }
    }

    /// Tracks how many executions overlap.
    struct TrackingTask {
        active: AtomicUsize,
        peak: AtomicUsize,
    }

    #[async_trait]
    impl Task for TrackingTask {
        async fn execute(&self, _message: &Message) -> TaskOutcome {
            let now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            TaskOutcome::Success
        }
    }

    fn retry_config(max_attempts: u32) -> Arc<dyn RetryPolicy> {
        Arc::new(RetryConfig {
            max_attempts,
            base_delay: Duration::from_secs(1),
            backoff: Backoff::Fixed,
            max_delay: Duration::from_secs(60),
        })
    }

    fn consumer(
        task: Arc<dyn Task>,
        max_attempts: u32,
        prefetch: u16,
        timeout: Duration,
    ) -> Consumer {
        Consumer::new(
            "test".into(),
            "jobs".into(),
            prefetch,
            timeout,
            task,
            retry_config(max_attempts),
        )
    }

    async fn run(consumer: &Consumer, messages: Vec<Message>) -> Vec<(String, Action)> {
        let log = Log::default();
        let broker = FakeBroker {
            messages,
            log: Arc::clone(&log),
            fail_consume: false,
        };
        consumer.run(&broker).await.unwrap();
        log.lock().unwrap().clone()
    }

    fn outcome_task(f: impl Fn(&Message) -> TaskOutcome + Send + Sync + 'static) -> Arc<dyn Task> {
        Arc::new(ClosureTask(f))
    }

    fn retryable() -> TaskOutcome {
        TaskOutcome::Retryable {
            reason: "upstream 503".into(),
            retry_after: None,
        }
    }

    #[tokio::test]
    async fn success_acks() {
        let c = consumer(
            outcome_task(|_| TaskOutcome::Success),
            3,
            1,
            Duration::from_secs(5),
        );
        assert_eq!(
            run(&c, vec![message("m1", 1)]).await,
            vec![("m1".to_owned(), Action::Ack)]
        );
    }

    #[tokio::test]
    async fn permanent_failure_rejects_without_retry() {
        let c = consumer(
            outcome_task(|_| TaskOutcome::Permanent {
                reason: "bad input".into(),
            }),
            3,
            1,
            Duration::from_secs(5),
        );
        assert_eq!(
            run(&c, vec![message("m1", 1)]).await,
            vec![("m1".to_owned(), Action::Reject)]
        );
    }

    #[tokio::test]
    async fn retryable_failure_retries_with_delay_while_attempts_remain() {
        let c = consumer(
            outcome_task(move |_| retryable()),
            3,
            1,
            Duration::from_secs(5),
        );
        assert_eq!(
            run(&c, vec![message("m1", 1)]).await,
            vec![("m1".to_owned(), Action::Retry(Duration::from_secs(1)))]
        );
    }

    #[tokio::test]
    async fn retryable_failure_dead_letters_when_attempts_are_exhausted() {
        let c = consumer(
            outcome_task(move |_| retryable()),
            3,
            1,
            Duration::from_secs(5),
        );
        assert_eq!(
            run(&c, vec![message("m1", 3)]).await,
            vec![("m1".to_owned(), Action::Reject)]
        );
    }

    #[tokio::test]
    async fn timeout_counts_as_retryable() {
        let c = consumer(
            Arc::new(SleepingTask(Duration::from_secs(3600))),
            3,
            1,
            Duration::from_millis(20),
        );
        assert_eq!(
            run(&c, vec![message("m1", 1)]).await,
            vec![("m1".to_owned(), Action::Retry(Duration::from_secs(1)))]
        );
    }

    #[tokio::test]
    async fn panic_counts_as_retryable() {
        let c = consumer(Arc::new(PanickingTask), 2, 1, Duration::from_secs(5));
        assert_eq!(
            run(&c, vec![message("m1", 1)]).await,
            vec![("m1".to_owned(), Action::Retry(Duration::from_secs(1)))]
        );
    }

    #[tokio::test]
    async fn each_delivery_is_settled_exactly_once() {
        let c = consumer(
            outcome_task(|m| match m.id.as_str() {
                "ok" => TaskOutcome::Success,
                "bad" => TaskOutcome::Permanent {
                    reason: "no".into(),
                },
                _ => retryable(),
            }),
            3,
            4,
            Duration::from_secs(5),
        );
        let mut entries = run(
            &c,
            vec![message("ok", 1), message("bad", 1), message("flaky", 1)],
        )
        .await;
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            entries,
            vec![
                ("bad".to_owned(), Action::Reject),
                ("flaky".to_owned(), Action::Retry(Duration::from_secs(1))),
                ("ok".to_owned(), Action::Ack),
            ]
        );
    }

    #[tokio::test]
    async fn in_flight_deliveries_never_exceed_prefetch() {
        let task = Arc::new(TrackingTask {
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        });
        let c = consumer(task.clone(), 3, 2, Duration::from_secs(5));
        let messages = (0..6).map(|i| message(&format!("m{i}"), 1)).collect();

        let entries = run(&c, messages).await;

        assert_eq!(entries.len(), 6);
        assert!(entries.iter().all(|(_, action)| *action == Action::Ack));
        assert!(task.peak.load(Ordering::SeqCst) <= 2);
    }

    #[tokio::test]
    async fn consume_failure_is_returned_to_the_caller() {
        let c = consumer(
            outcome_task(|_| TaskOutcome::Success),
            3,
            1,
            Duration::from_secs(5),
        );
        let broker = FakeBroker {
            messages: vec![],
            log: Log::default(),
            fail_consume: true,
        };
        assert!(matches!(
            c.run(&broker).await,
            Err(BrokerError::Unavailable(_))
        ));
    }

    #[tokio::test]
    async fn empty_queue_returns_ok() {
        let c = consumer(
            outcome_task(|_| TaskOutcome::Success),
            3,
            1,
            Duration::from_secs(5),
        );
        assert!(run(&c, vec![]).await.is_empty());
    }
}
