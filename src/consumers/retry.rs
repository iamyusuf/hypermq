use std::time::Duration;

use serde::Deserialize;

use crate::tasks::TaskOutcome;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Ack,
    Retry { delay: Duration },
    DeadLetter,
}

/// Maps a Task outcome and delivery attempt to a settlement decision. Pure and synchronous.
pub trait RetryPolicy: Send + Sync {
    fn decide(&self, outcome: &TaskOutcome, attempt: u32) -> Decision;
}

pub const DEFAULT_MAX_DELAY: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Backoff {
    Fixed,
    #[default]
    Exponential,
}

/// Bounded retry settings for one consumer. `max_attempts` counts total deliveries, including the first.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    pub max_attempts: u32,
    #[serde(with = "humantime_serde")]
    pub base_delay: Duration,
    #[serde(default)]
    pub backoff: Backoff,
    #[serde(default = "default_max_delay", with = "humantime_serde")]
    pub max_delay: Duration,
}

fn default_max_delay() -> Duration {
    DEFAULT_MAX_DELAY
}

impl RetryConfig {
    /// Delay before the next attempt, after `attempt` failed. Never exceeds `max_delay`.
    fn backoff_delay(&self, attempt: u32) -> Duration {
        let delay = match self.backoff {
            Backoff::Fixed => self.base_delay,
            Backoff::Exponential => {
                let factor = 2u32
                    .checked_pow(attempt.saturating_sub(1))
                    .unwrap_or(u32::MAX);
                self.base_delay
                    .checked_mul(factor)
                    .unwrap_or(self.max_delay)
            }
        };
        delay.min(self.max_delay)
    }
}

impl RetryPolicy for RetryConfig {
    fn decide(&self, outcome: &TaskOutcome, attempt: u32) -> Decision {
        match outcome {
            TaskOutcome::Success => Decision::Ack,
            TaskOutcome::Permanent { .. } => Decision::DeadLetter,
            TaskOutcome::Retryable { retry_after, .. } => {
                if attempt >= self.max_attempts {
                    return Decision::DeadLetter;
                }
                let delay = retry_after
                    .unwrap_or_else(|| self.backoff_delay(attempt))
                    .min(self.max_delay);
                Decision::Retry { delay }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(backoff: Backoff) -> RetryConfig {
        RetryConfig {
            max_attempts: 5,
            base_delay: Duration::from_secs(2),
            backoff,
            max_delay: Duration::from_secs(10),
        }
    }

    fn retryable(retry_after: Option<Duration>) -> TaskOutcome {
        TaskOutcome::Retryable {
            reason: "down".into(),
            retry_after,
        }
    }

    #[test]
    fn success_acks() {
        assert_eq!(
            config(Backoff::Fixed).decide(&TaskOutcome::Success, 1),
            Decision::Ack
        );
    }

    #[test]
    fn permanent_dead_letters_without_retry() {
        let outcome = TaskOutcome::Permanent {
            reason: "bad input".into(),
        };
        assert_eq!(
            config(Backoff::Fixed).decide(&outcome, 1),
            Decision::DeadLetter
        );
    }

    #[test]
    fn retryable_dead_letters_when_attempts_are_exhausted() {
        assert_eq!(
            config(Backoff::Fixed).decide(&retryable(None), 5),
            Decision::DeadLetter
        );
    }

    #[test]
    fn fixed_backoff_uses_base_delay() {
        assert_eq!(
            config(Backoff::Fixed).decide(&retryable(None), 3),
            Decision::Retry {
                delay: Duration::from_secs(2)
            }
        );
    }

    #[test]
    fn exponential_backoff_doubles_and_caps_at_max_delay() {
        let policy = config(Backoff::Exponential);
        let delay = |attempt| match policy.decide(&retryable(None), attempt) {
            Decision::Retry { delay } => delay,
            other => panic!("expected retry, got {other:?}"),
        };
        assert_eq!(delay(1), Duration::from_secs(2));
        assert_eq!(delay(2), Duration::from_secs(4));
        assert_eq!(delay(3), Duration::from_secs(8));
        assert_eq!(delay(4), Duration::from_secs(10));
    }

    #[test]
    fn huge_attempt_numbers_do_not_overflow() {
        let policy = RetryConfig {
            max_attempts: u32::MAX,
            ..config(Backoff::Exponential)
        };
        assert_eq!(
            policy.decide(&retryable(None), u32::MAX - 1),
            Decision::Retry {
                delay: Duration::from_secs(10)
            }
        );
    }

    #[test]
    fn task_retry_after_is_used_and_capped() {
        let policy = config(Backoff::Exponential);
        assert_eq!(
            policy.decide(&retryable(Some(Duration::from_secs(3))), 1),
            Decision::Retry {
                delay: Duration::from_secs(3)
            }
        );
        assert_eq!(
            policy.decide(&retryable(Some(Duration::from_secs(60))), 1),
            Decision::Retry {
                delay: Duration::from_secs(10)
            }
        );
    }

    #[test]
    fn single_attempt_dead_letters_immediately() {
        let policy = RetryConfig {
            max_attempts: 1,
            ..config(Backoff::Fixed)
        };
        assert_eq!(policy.decide(&retryable(None), 1), Decision::DeadLetter);
    }
}
