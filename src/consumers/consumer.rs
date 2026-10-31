use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Semaphore;

use super::retry::{Decision, RetryPolicy};
use crate::broker::{Broker, BrokerError, Delivery, Subscription};
use crate::message::Message;
use crate::metrics::{ConsumerMetrics, Settlement};
use crate::shutdown;
use crate::tasks::{Task, TaskOutcome};

const RECONNECT_BASE: Duration = Duration::from_secs(1);
const RECONNECT_MAX: Duration = Duration::from_secs(30);

/// Consumes one queue and settles each delivery from its Task outcome and the retry policy.
pub struct Consumer {
    name: String,
    queue: String,
    prefetch: u16,
    timeout: Duration,
    task: Arc<dyn Task>,
    policy: Arc<dyn RetryPolicy>,
    metrics: Arc<ConsumerMetrics>,
}

impl Consumer {
    pub fn new(
        name: String,
        queue: String,
        prefetch: u16,
        timeout: Duration,
        task: Arc<dyn Task>,
        policy: Arc<dyn RetryPolicy>,
        metrics: Arc<ConsumerMetrics>,
    ) -> Self {
        Self {
            name,
            queue,
            prefetch,
            timeout,
            task,
            policy,
            metrics,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Consumes until shutdown or a failure. On shutdown it stops taking deliveries and waits for
    /// in-flight ones to settle. An error means the caller decides whether to resubscribe.
    pub async fn run(
        &self,
        broker: &dyn Broker,
        shutdown: &shutdown::Receiver,
    ) -> Result<(), BrokerError> {
        let mut subscription = broker.consume(&self.queue, self.prefetch).await?;
        let slots = Arc::new(Semaphore::new(usize::from(self.prefetch)));
        let mut stop = shutdown.clone();
        let result = self.pump(subscription.as_mut(), &slots, &mut stop).await;
        // Taking every slot waits until no delivery is still unsettled.
        let _ = slots.acquire_many(u32::from(self.prefetch)).await;
        result
    }

    /// Runs the consumer and resubscribes with backoff after broker failures, until shutdown.
    /// A failure that is not a connection problem stops the consumer.
    pub async fn supervise(
        &self,
        broker: &dyn Broker,
        shutdown: &shutdown::Receiver,
    ) -> Result<(), BrokerError> {
        self.supervise_with(broker, shutdown, RECONNECT_BASE, RECONNECT_MAX)
            .await
    }

    async fn supervise_with(
        &self,
        broker: &dyn Broker,
        shutdown: &shutdown::Receiver,
        base: Duration,
        max: Duration,
    ) -> Result<(), BrokerError> {
        let mut stop = shutdown.clone();
        let mut failures: u32 = 0;
        loop {
            let started = Instant::now();
            let err = match self.run(broker, shutdown).await {
                Ok(()) => return Ok(()),
                Err(err @ BrokerError::Operation(_)) => return Err(err),
                Err(err) => err,
            };
            // A session that lasted at least `max` was healthy, so the backoff starts over.
            if started.elapsed() >= max {
                failures = 0;
            }
            failures = failures.saturating_add(1);
            let delay = backoff_delay(base, max, failures);
            tracing::warn!(consumer = %self.name, %err, delay = ?delay, "reconnecting after broker failure");
            tokio::select! {
                biased;
                () = shutdown::requested(&mut stop) => return Ok(()),
                () = tokio::time::sleep(delay) => {}
            }
        }
    }

    async fn pump(
        &self,
        subscription: &mut dyn Subscription,
        slots: &Arc<Semaphore>,
        stop: &mut shutdown::Receiver,
    ) -> Result<(), BrokerError> {
        loop {
            // A slot is taken before pulling, so unsettled deliveries never exceed the prefetch.
            let slot = tokio::select! {
                biased;
                () = shutdown::requested(stop) => return Ok(()),
                slot = Arc::clone(slots).acquire_owned() => {
                    slot.map_err(|_| BrokerError::Operation("consumer slots were closed".into()))?
                }
            };
            let next = tokio::select! {
                biased;
                () = shutdown::requested(stop) => return Ok(()),
                next = subscription.next() => next,
            };
            let Some(next) = next else {
                return Ok(());
            };
            let delivery = next?;

            let task = Arc::clone(&self.task);
            let policy = Arc::clone(&self.policy);
            let timeout = self.timeout;
            let name = self.name.clone();
            let metrics = Arc::clone(&self.metrics);
            tokio::spawn(async move {
                settle(delivery, task, policy, timeout, &name, &metrics).await;
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
    metrics: &ConsumerMetrics,
) {
    let message = delivery.message().clone();
    let message_id = message.id.clone();
    let attempt = message.attempt;
    metrics.record_delivery();

    let started = Instant::now();
    let outcome = run_task(task, message, timeout).await;
    let duration = started.elapsed();
    metrics.record_task(&outcome, duration);

    let decision = policy.decide(&outcome, attempt);
    let (settlement, result) = match decision {
        Decision::Ack => (Settlement::Ack, delivery.ack().await),
        Decision::Retry { delay } => (Settlement::Retry, delivery.retry(delay).await),
        Decision::DeadLetter => (Settlement::DeadLetter, delivery.reject().await),
    };
    let duration_ms = duration.as_secs_f64() * 1000.0;
    let reason = match &outcome {
        TaskOutcome::Success => "",
        TaskOutcome::Retryable { reason, .. } | TaskOutcome::Permanent { reason } => {
            reason.as_str()
        }
    };
    match result {
        Ok(()) => {
            metrics.record_settlement(settlement);
            if settlement == Settlement::Ack {
                tracing::debug!(consumer, message_id = %message_id, attempt, outcome = outcome.label(), action = settlement.label(), duration_ms, "delivery settled");
            } else {
                tracing::info!(consumer, message_id = %message_id, attempt, outcome = outcome.label(), action = settlement.label(), duration_ms, reason, "delivery settled");
            }
        }
        Err(err) => {
            tracing::error!(consumer, message_id = %message_id, attempt, action = settlement.label(), %err, "failed to settle delivery");
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

/// Delay before reconnect attempt `failures` (1-based): `base` doubled per failure, capped at `max`.
fn backoff_delay(base: Duration, max: Duration, failures: u32) -> Duration {
    let factor = 2u32.saturating_pow(failures.saturating_sub(1).min(31));
    base.saturating_mul(factor).min(max)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use bytes::Bytes;
    use tokio::sync::watch;

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
        /// When set, an exhausted subscription waits instead of ending.
        hang: bool,
    }

    #[async_trait]
    impl Subscription for FakeSubscription {
        async fn next(&mut self) -> Option<Result<Box<dyn Delivery>, BrokerError>> {
            if let Some(message) = self.messages.pop_front() {
                return Some(Ok(Box::new(FakeDelivery {
                    message,
                    log: Arc::clone(&self.log),
                }) as Box<dyn Delivery>));
            }
            if self.hang {
                std::future::pending::<()>().await;
            }
            None
        }
    }

    /// Why a scripted `consume` call fails.
    #[derive(Clone, Copy)]
    enum Failure {
        Unavailable,
        Operation,
    }

    struct FakeBroker {
        messages: Vec<Message>,
        log: Log,
        /// Failures for the first `consume` calls, in order; later calls succeed.
        script: Mutex<VecDeque<Failure>>,
        hang_when_empty: bool,
    }

    fn fake_broker(
        messages: Vec<Message>,
        log: &Log,
        script: Vec<Failure>,
        hang_when_empty: bool,
    ) -> FakeBroker {
        FakeBroker {
            messages,
            log: Arc::clone(log),
            script: Mutex::new(VecDeque::from(script)),
            hang_when_empty,
        }
    }

    #[async_trait]
    impl Broker for FakeBroker {
        async fn consume(
            &self,
            _queue: &str,
            _prefetch: u16,
        ) -> Result<Box<dyn Subscription>, BrokerError> {
            let failure = self.script.lock().unwrap().pop_front();
            match failure {
                Some(Failure::Unavailable) => {
                    return Err(BrokerError::Unavailable("connection lost".into()));
                }
                Some(Failure::Operation) => {
                    return Err(BrokerError::Operation("queue is missing".into()));
                }
                None => {}
            }
            Ok(Box::new(FakeSubscription {
                messages: VecDeque::from(self.messages.clone()),
                log: Arc::clone(&self.log),
                hang: self.hang_when_empty,
            }))
        }
    }

    /// Shutdown receiver whose sender is dropped, so shutdown never fires.
    fn no_shutdown() -> shutdown::Receiver {
        watch::channel(false).1
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
            Arc::default(),
        )
    }

    async fn run(consumer: &Consumer, messages: Vec<Message>) -> Vec<(String, Action)> {
        let log = Log::default();
        let broker = fake_broker(messages, &log, vec![], false);
        consumer.run(&broker, &no_shutdown()).await.unwrap();
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
        let broker = fake_broker(vec![], &Log::default(), vec![Failure::Unavailable], false);
        assert!(matches!(
            c.run(&broker, &no_shutdown()).await,
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

    #[test]
    fn backoff_doubles_and_caps_at_max() {
        let base = Duration::from_secs(1);
        let max = Duration::from_secs(30);
        assert_eq!(backoff_delay(base, max, 1), Duration::from_secs(1));
        assert_eq!(backoff_delay(base, max, 2), Duration::from_secs(2));
        assert_eq!(backoff_delay(base, max, 5), Duration::from_secs(16));
        assert_eq!(backoff_delay(base, max, 6), max);
        assert_eq!(backoff_delay(base, max, u32::MAX), max);
    }

    #[tokio::test]
    async fn reconnects_after_broker_failures() {
        let c = consumer(
            outcome_task(|_| TaskOutcome::Success),
            3,
            1,
            Duration::from_secs(5),
        );
        let log = Log::default();
        let broker = fake_broker(
            vec![message("m1", 1)],
            &log,
            vec![Failure::Unavailable, Failure::Unavailable],
            false,
        );
        c.supervise_with(
            &broker,
            &no_shutdown(),
            Duration::from_millis(1),
            Duration::from_millis(5),
        )
        .await
        .unwrap();
        assert_eq!(
            log.lock().unwrap().clone(),
            vec![("m1".to_owned(), Action::Ack)]
        );
    }

    #[tokio::test]
    async fn operation_failure_stops_without_reconnecting() {
        let c = consumer(
            outcome_task(|_| TaskOutcome::Success),
            3,
            1,
            Duration::from_secs(5),
        );
        let log = Log::default();
        let broker = fake_broker(
            vec![message("m1", 1)],
            &log,
            vec![Failure::Operation],
            false,
        );
        let result = c
            .supervise_with(
                &broker,
                &no_shutdown(),
                Duration::from_millis(1),
                Duration::from_millis(5),
            )
            .await;
        assert!(matches!(result, Err(BrokerError::Operation(_))));
        // A reconnect would have acked m1, so an empty log shows no second attempt.
        assert!(log.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn shutdown_stops_an_idle_consumer() {
        let c = consumer(
            outcome_task(|_| TaskOutcome::Success),
            3,
            1,
            Duration::from_secs(5),
        );
        let broker = fake_broker(vec![], &Log::default(), vec![], true);
        let (stop, shutdown) = watch::channel(false);
        let stopper = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let _ = stop.send(true);
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(c.run(&broker, &shutdown), stopper)
        })
        .await
        .expect("consumer did not stop on shutdown");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn shutdown_lets_in_flight_work_settle() {
        let c = consumer(
            Arc::new(SleepingTask(Duration::from_millis(50))),
            3,
            1,
            Duration::from_secs(5),
        );
        let log = Log::default();
        let broker = fake_broker(vec![message("m1", 1)], &log, vec![], true);
        let (stop, shutdown) = watch::channel(false);
        let stopper = async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let _ = stop.send(true);
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(c.run(&broker, &shutdown), stopper)
        })
        .await
        .expect("consumer did not stop on shutdown");
        assert!(result.is_ok());
        assert_eq!(
            log.lock().unwrap().clone(),
            vec![("m1".to_owned(), Action::Ack)]
        );
    }

    #[tokio::test]
    async fn shutdown_interrupts_reconnect_backoff() {
        let c = consumer(
            outcome_task(|_| TaskOutcome::Success),
            3,
            1,
            Duration::from_secs(5),
        );
        let broker = fake_broker(vec![], &Log::default(), vec![Failure::Unavailable], false);
        let (stop, shutdown) = watch::channel(false);
        let stopper = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let _ = stop.send(true);
        };
        let hour = Duration::from_secs(3600);
        let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(c.supervise_with(&broker, &shutdown, hour, hour), stopper)
        })
        .await
        .expect("backoff ignored shutdown");
        assert!(result.is_ok());
    }
}
