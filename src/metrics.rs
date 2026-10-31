//! In-process counters and a Task latency histogram, rendered in Prometheus text format.
//!
//! Recording uses atomics, so it never blocks a delivery. Consumer names are validated on load
//! (letters, digits, and `-_.:`), so they can be used as label values without escaping.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crate::tasks::TaskOutcome;

/// Upper bounds, in seconds, of the Task latency histogram buckets.
const LATENCY_BUCKETS: [f64; 12] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];
const OUTCOME_LABELS: [&str; 3] = ["success", "retryable", "permanent"];
const SETTLEMENT_LABELS: [&str; 3] = ["ack", "retry", "dead_letter"];

/// How a delivery was settled after its Task ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settlement {
    Ack,
    Retry,
    DeadLetter,
}

impl Settlement {
    /// Short name for logs and metrics: `ack`, `retry`, or `dead_letter`.
    pub fn label(self) -> &'static str {
        SETTLEMENT_LABELS[self.index()]
    }

    fn index(self) -> usize {
        match self {
            Self::Ack => 0,
            Self::Retry => 1,
            Self::DeadLetter => 2,
        }
    }
}

fn outcome_index(outcome: &TaskOutcome) -> usize {
    match outcome {
        TaskOutcome::Success => 0,
        TaskOutcome::Retryable { .. } => 1,
        TaskOutcome::Permanent { .. } => 2,
    }
}

/// Process-wide metrics. Shared by the API and every consumer through `Arc`.
#[derive(Default)]
pub struct Metrics {
    accepted: AtomicU64,
    failed: AtomicU64,
    consumers: Mutex<BTreeMap<String, Arc<ConsumerMetrics>>>,
}

impl Metrics {
    /// Records one publish attempt against the broker. Requests rejected by validation are not counted.
    pub fn record_publish(&self, accepted: bool) {
        let counter = if accepted {
            &self.accepted
        } else {
            &self.failed
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the handle for one consumer, creating it on first use.
    pub fn consumer(&self, name: &str) -> Arc<ConsumerMetrics> {
        let mut consumers = self
            .consumers
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        Arc::clone(consumers.entry(name.to_owned()).or_default())
    }

    /// Renders every metric in Prometheus text exposition format (version 0.0.4).
    pub fn render(&self) -> String {
        let consumers: Vec<(String, Arc<ConsumerMetrics>)> = self
            .consumers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(name, metrics)| (name.clone(), Arc::clone(metrics)))
            .collect();

        let mut out = String::new();
        family(
            &mut out,
            "hypermq_messages_published_total",
            "counter",
            "Publish attempts against the broker, by result.",
        );
        let _ = writeln!(
            out,
            "hypermq_messages_published_total{{result=\"accepted\"}} {}",
            self.accepted.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "hypermq_messages_published_total{{result=\"failed\"}} {}",
            self.failed.load(Ordering::Relaxed)
        );

        family(
            &mut out,
            "hypermq_deliveries_total",
            "counter",
            "Deliveries received, by consumer.",
        );
        for (name, metrics) in &consumers {
            let _ = writeln!(
                out,
                "hypermq_deliveries_total{{consumer=\"{name}\"}} {}",
                metrics.deliveries.load(Ordering::Relaxed)
            );
        }

        family(
            &mut out,
            "hypermq_task_outcomes_total",
            "counter",
            "Task outcomes, by consumer.",
        );
        for (name, metrics) in &consumers {
            for (index, label) in OUTCOME_LABELS.iter().enumerate() {
                let _ = writeln!(
                    out,
                    "hypermq_task_outcomes_total{{consumer=\"{name}\",outcome=\"{label}\"}} {}",
                    metrics.outcomes[index].load(Ordering::Relaxed)
                );
            }
        }

        family(
            &mut out,
            "hypermq_settlements_total",
            "counter",
            "Deliveries settled on the broker, by consumer and action.",
        );
        for (name, metrics) in &consumers {
            for (index, label) in SETTLEMENT_LABELS.iter().enumerate() {
                let _ = writeln!(
                    out,
                    "hypermq_settlements_total{{consumer=\"{name}\",action=\"{label}\"}} {}",
                    metrics.settlements[index].load(Ordering::Relaxed)
                );
            }
        }

        family(
            &mut out,
            "hypermq_task_duration_seconds",
            "histogram",
            "Time spent in Task::execute, including timeouts, by consumer.",
        );
        for (name, metrics) in &consumers {
            let histogram = &metrics.task_duration;
            let count = histogram.count.load(Ordering::Relaxed);
            let mut cumulative = 0;
            for (index, bound) in LATENCY_BUCKETS.iter().enumerate() {
                cumulative += histogram.buckets[index].load(Ordering::Relaxed);
                let _ = writeln!(
                    out,
                    "hypermq_task_duration_seconds_bucket{{consumer=\"{name}\",le=\"{bound}\"}} {cumulative}"
                );
            }
            let _ = writeln!(
                out,
                "hypermq_task_duration_seconds_bucket{{consumer=\"{name}\",le=\"+Inf\"}} {count}"
            );
            let sum_seconds = histogram.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0;
            let _ = writeln!(
                out,
                "hypermq_task_duration_seconds_sum{{consumer=\"{name}\"}} {sum_seconds}"
            );
            let _ = writeln!(
                out,
                "hypermq_task_duration_seconds_count{{consumer=\"{name}\"}} {count}"
            );
        }
        out
    }
}

fn family(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

/// Counters for one consumer. Obtained from [`Metrics::consumer`].
#[derive(Default)]
pub struct ConsumerMetrics {
    deliveries: AtomicU64,
    outcomes: [AtomicU64; OUTCOME_LABELS.len()],
    settlements: [AtomicU64; SETTLEMENT_LABELS.len()],
    task_duration: Histogram,
}

impl ConsumerMetrics {
    pub fn record_delivery(&self) {
        self.deliveries.fetch_add(1, Ordering::Relaxed);
    }

    /// Records the Task outcome and how long `Task::execute` ran.
    pub fn record_task(&self, outcome: &TaskOutcome, duration: Duration) {
        self.outcomes[outcome_index(outcome)].fetch_add(1, Ordering::Relaxed);
        self.task_duration.observe(duration);
    }

    /// Records a settlement that the broker confirmed.
    pub fn record_settlement(&self, settlement: Settlement) {
        self.settlements[settlement.index()].fetch_add(1, Ordering::Relaxed);
    }
}

/// Fixed-bucket histogram. Bucket counts are stored per bucket and made cumulative on render.
#[derive(Default)]
struct Histogram {
    buckets: [AtomicU64; LATENCY_BUCKETS.len()],
    count: AtomicU64,
    sum_micros: AtomicU64,
}

impl Histogram {
    fn observe(&self, duration: Duration) {
        let seconds = duration.as_secs_f64();
        if let Some(index) = LATENCY_BUCKETS.iter().position(|bound| seconds <= *bound) {
            self.buckets[index].fetch_add(1, Ordering::Relaxed);
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        let micros = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
        self.sum_micros.fetch_add(micros, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumer_handle_is_shared_by_name() {
        let metrics = Metrics::default();
        assert!(Arc::ptr_eq(
            &metrics.consumer("notify"),
            &metrics.consumer("notify")
        ));
        assert!(!Arc::ptr_eq(
            &metrics.consumer("notify"),
            &metrics.consumer("archive")
        ));
    }

    #[test]
    fn render_reports_counters_and_cumulative_histogram() {
        let metrics = Metrics::default();
        metrics.record_publish(true);
        metrics.record_publish(true);
        metrics.record_publish(false);

        let notify = metrics.consumer("notify");
        notify.record_delivery();
        notify.record_task(&TaskOutcome::Success, Duration::from_millis(3));
        notify.record_settlement(Settlement::Ack);
        notify.record_delivery();
        notify.record_task(
            &TaskOutcome::Retryable {
                reason: "down".into(),
                retry_after: None,
            },
            Duration::from_secs(40),
        );
        notify.record_settlement(Settlement::Retry);

        let text = metrics.render();
        for line in [
            "hypermq_messages_published_total{result=\"accepted\"} 2",
            "hypermq_messages_published_total{result=\"failed\"} 1",
            "hypermq_deliveries_total{consumer=\"notify\"} 2",
            "hypermq_task_outcomes_total{consumer=\"notify\",outcome=\"success\"} 1",
            "hypermq_task_outcomes_total{consumer=\"notify\",outcome=\"retryable\"} 1",
            "hypermq_settlements_total{consumer=\"notify\",action=\"ack\"} 1",
            "hypermq_settlements_total{consumer=\"notify\",action=\"retry\"} 1",
            "hypermq_settlements_total{consumer=\"notify\",action=\"dead_letter\"} 0",
            "hypermq_task_duration_seconds_bucket{consumer=\"notify\",le=\"0.005\"} 1",
            "hypermq_task_duration_seconds_bucket{consumer=\"notify\",le=\"0.01\"} 1",
            "hypermq_task_duration_seconds_bucket{consumer=\"notify\",le=\"30\"} 1",
            "hypermq_task_duration_seconds_bucket{consumer=\"notify\",le=\"+Inf\"} 2",
            "hypermq_task_duration_seconds_count{consumer=\"notify\"} 2",
        ] {
            assert!(
                text.lines().any(|l| l == line),
                "missing `{line}` in:\n{text}"
            );
        }
    }

    #[test]
    fn render_without_consumers_still_declares_families() {
        let text = Metrics::default().render();
        assert!(text.contains("# TYPE hypermq_task_duration_seconds histogram"));
        assert!(text.contains("hypermq_messages_published_total{result=\"accepted\"} 0"));
    }
}
