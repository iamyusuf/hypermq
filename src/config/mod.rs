use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use clap::ValueEnum;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;

use crate::broker::topology::Topology;
use crate::consumers::{self, ConsumerConfig};
use crate::error::ConfigError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Role {
    All,
    Api,
    Consumers,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::All => "all",
            Role::Api => "api",
            Role::Consumers => "consumers",
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub broker: BrokerConfig,
    #[serde(default)]
    pub http_api: HttpApiConfig,
    #[serde(default)]
    pub topology: Topology,
    #[serde(default)]
    pub consumers: Vec<ConsumerConfig>,
    /// When set, publishes that RabbitMQ cannot take are stored in this database and relayed later.
    #[serde(default)]
    pub outbox: Option<OutboxConfig>,
}

#[derive(Debug, Deserialize)]
pub struct BrokerConfig {
    pub url: SecretString,
}

/// Persistent fallback for publishes made while RabbitMQ is unavailable. See `docs/roadmap/roadmap.md`, Phase 8.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxConfig {
    /// `postgres://`, `postgresql://`, or `mysql://`. Treated as a secret.
    pub database_url: SecretString,
    /// Rows held before publishes return `503`.
    #[serde(default = "default_outbox_max_messages")]
    pub max_messages: u64,
    /// Rows older than this move to the dead-letter table.
    #[serde(default = "default_outbox_max_age", with = "humantime_serde")]
    pub max_age: Duration,
    /// Pause between relay cycles.
    #[serde(default = "default_outbox_cooldown", with = "humantime_serde")]
    pub cooldown: Duration,
    /// Failed publishes of an unroutable message before it is dead-lettered.
    #[serde(default = "default_outbox_max_attempts")]
    pub max_attempts: u32,
    /// Delay before the first retry; doubles on each failure.
    #[serde(default = "default_outbox_retry_base_delay", with = "humantime_serde")]
    pub retry_base_delay: Duration,
    /// Upper bound for the retry delay.
    #[serde(default = "default_outbox_retry_max_delay", with = "humantime_serde")]
    pub retry_max_delay: Duration,
    /// `/health` reports `degraded` when the oldest row is older than this.
    #[serde(default = "default_outbox_degraded_after", with = "humantime_serde")]
    pub degraded_after: Duration,
}

fn default_outbox_max_messages() -> u64 {
    100_000
}

fn default_outbox_max_age() -> Duration {
    Duration::from_secs(24 * 60 * 60)
}

fn default_outbox_cooldown() -> Duration {
    Duration::from_secs(5)
}

fn default_outbox_max_attempts() -> u32 {
    10
}

fn default_outbox_retry_base_delay() -> Duration {
    Duration::from_secs(1)
}

fn default_outbox_retry_max_delay() -> Duration {
    Duration::from_secs(5 * 60)
}

fn default_outbox_degraded_after() -> Duration {
    Duration::from_secs(5 * 60)
}

impl OutboxConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        let url = self.database_url.expose_secret();
        if !(url.starts_with("postgres://")
            || url.starts_with("postgresql://")
            || url.starts_with("mysql://"))
        {
            return Err(ConfigError::Invalid(
                "outbox.database_url must start with postgres://, postgresql://, or mysql://"
                    .into(),
            ));
        }
        let positive_durations = [
            ("max_age", self.max_age),
            ("cooldown", self.cooldown),
            ("retry_base_delay", self.retry_base_delay),
            ("degraded_after", self.degraded_after),
        ];
        for (name, value) in positive_durations {
            if value.is_zero() {
                return Err(ConfigError::Invalid(format!(
                    "outbox.{name} must be greater than zero"
                )));
            }
        }
        if self.max_messages == 0 {
            return Err(ConfigError::Invalid(
                "outbox.max_messages must be greater than zero".into(),
            ));
        }
        if self.max_attempts == 0 {
            return Err(ConfigError::Invalid(
                "outbox.max_attempts must be at least 1".into(),
            ));
        }
        if self.retry_max_delay < self.retry_base_delay {
            return Err(ConfigError::Invalid(
                "outbox.retry_max_delay must be at least outbox.retry_base_delay".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpApiConfig {
    #[serde(default = "default_bind")]
    pub bind: SocketAddr,
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: usize,
    /// When set, `POST /publish` requires `Authorization: Bearer <token>`. `/health` stays open.
    #[serde(default)]
    pub bearer_token: Option<SecretString>,
}

impl Default for HttpApiConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            max_body_bytes: default_max_body_bytes(),
            bearer_token: None,
        }
    }
}

fn default_bind() -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], 8080))
}

fn default_max_body_bytes() -> usize {
    1024 * 1024
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        Self::from_yaml(&text)
    }

    pub fn from_yaml(text: &str) -> Result<Self, ConfigError> {
        let config: Config = serde_yaml_ng::from_str(text)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let url = self.broker.url.expose_secret();
        if !(url.starts_with("amqp://") || url.starts_with("amqps://")) {
            return Err(ConfigError::Invalid(
                "broker.url must start with amqp:// or amqps://".into(),
            ));
        }
        self.topology.validate().map_err(ConfigError::Invalid)?;
        consumers::validate(&self.consumers).map_err(ConfigError::Invalid)?;
        if let Some(token) = &self.http_api.bearer_token
            && token.expose_secret().is_empty()
        {
            return Err(ConfigError::Invalid(
                "http_api.bearer_token must not be empty".into(),
            ));
        }
        if let Some(outbox) = &self.outbox {
            outbox.validate()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = "broker:\n  url: amqp://user:pw@localhost:5672\n";

    #[test]
    fn loads_minimal_config_with_defaults() {
        let cfg = Config::from_yaml(MINIMAL).unwrap();
        assert_eq!(
            cfg.broker.url.expose_secret(),
            "amqp://user:pw@localhost:5672"
        );
        assert_eq!(cfg.http_api.bind, default_bind());
        assert_eq!(cfg.http_api.max_body_bytes, 1024 * 1024);
    }

    #[test]
    fn accepts_empty_sections() {
        let yaml = format!("{MINIMAL}consumers: []\ntopology: {{}}\n");
        assert!(Config::from_yaml(&yaml).unwrap().consumers.is_empty());
    }

    #[test]
    fn invalid_consumer_fails_config_load() {
        let yaml = format!(
            "{MINIMAL}consumers:\n  - name: a\n    queue: 'bad name'\n    retry:\n      max_attempts: 2\n      base_delay: 1s\n    task:\n      type: http\n"
        );
        let err = Config::from_yaml(&yaml).unwrap_err();
        assert!(matches!(err, ConfigError::Invalid(msg) if msg.contains("queue")));
    }

    #[test]
    fn does_not_expand_variable_syntax() {
        let err = Config::from_yaml("broker:\n  url: ${RABBITMQ_URL}\n").unwrap_err();
        assert!(matches!(err, ConfigError::Invalid(_)));
    }

    #[test]
    fn rejects_non_amqp_url() {
        let err = Config::from_yaml("broker:\n  url: http://x\n").unwrap_err();
        assert!(matches!(err, ConfigError::Invalid(_)));
    }

    #[test]
    fn rejects_unknown_http_api_key() {
        let yaml = "broker:\n  url: amqp://x\nhttp_api:\n  bnd: 0.0.0.0:1\n";
        assert!(Config::from_yaml(yaml).is_err());
    }

    #[test]
    fn example_config_is_valid() {
        let cfg = Config::from_yaml(include_str!("../../docs/examples/config.yaml")).unwrap();
        assert!(!cfg.topology.is_empty());
    }

    #[test]
    fn invalid_topology_fails_config_load() {
        let yaml = format!("{MINIMAL}topology:\n  queues: [{{name: 'bad name'}}]\n");
        let err = Config::from_yaml(&yaml).unwrap_err();
        assert!(matches!(err, ConfigError::Invalid(msg) if msg.contains("queues[0].name")));
    }

    #[test]
    fn topology_defaults_to_empty() {
        assert!(Config::from_yaml(MINIMAL).unwrap().topology.is_empty());
    }

    #[test]
    fn debug_output_hides_the_url() {
        let cfg = Config::from_yaml(MINIMAL).unwrap();
        assert!(!format!("{cfg:?}").contains("pw@"));
    }

    #[test]
    fn bearer_token_is_optional_and_hidden_in_debug_output() {
        assert!(
            Config::from_yaml(MINIMAL)
                .unwrap()
                .http_api
                .bearer_token
                .is_none()
        );
        let yaml = format!("{MINIMAL}http_api:\n  bearer_token: hunter2-token\n");
        let cfg = Config::from_yaml(&yaml).unwrap();
        assert_eq!(
            cfg.http_api
                .bearer_token
                .as_ref()
                .map(|t| t.expose_secret()),
            Some("hunter2-token")
        );
        assert!(!format!("{cfg:?}").contains("hunter2-token"));
    }

    #[test]
    fn rejects_empty_bearer_token() {
        let yaml = format!("{MINIMAL}http_api:\n  bearer_token: ''\n");
        let err = Config::from_yaml(&yaml).unwrap_err();
        assert!(matches!(err, ConfigError::Invalid(msg) if msg.contains("bearer_token")));
    }

    fn outbox_yaml(extra: &str) -> String {
        format!(
            "{MINIMAL}outbox:\n  database_url: postgres://hypermq:pw@localhost:5433/hypermq\n{extra}"
        )
    }

    #[test]
    fn outbox_is_optional_and_defaults_apply() {
        assert!(Config::from_yaml(MINIMAL).unwrap().outbox.is_none());
        let cfg = Config::from_yaml(&outbox_yaml("")).unwrap();
        let outbox = cfg.outbox.unwrap();
        assert_eq!(outbox.max_messages, 100_000);
        assert_eq!(outbox.max_age, Duration::from_secs(24 * 60 * 60));
        assert_eq!(outbox.cooldown, Duration::from_secs(5));
        assert_eq!(outbox.max_attempts, 10);
        assert_eq!(outbox.retry_base_delay, Duration::from_secs(1));
        assert_eq!(outbox.retry_max_delay, Duration::from_secs(5 * 60));
        assert_eq!(outbox.degraded_after, Duration::from_secs(5 * 60));
    }

    #[test]
    fn accepts_postgres_and_mysql_urls() {
        for url in [
            "postgres://u:p@localhost/db",
            "postgresql://u:p@localhost/db",
            "mysql://u:p@localhost/db",
        ] {
            let yaml = format!("{MINIMAL}outbox:\n  database_url: {url}\n");
            assert!(Config::from_yaml(&yaml).is_ok(), "{url} should be accepted");
        }
    }

    #[test]
    fn rejects_outbox_url_with_other_scheme() {
        let yaml = format!("{MINIMAL}outbox:\n  database_url: sqlite://local.db\n");
        let err = Config::from_yaml(&yaml).unwrap_err();
        assert!(matches!(err, ConfigError::Invalid(msg) if msg.contains("database_url")));
    }

    #[test]
    fn rejects_zero_outbox_durations() {
        for key in ["max_age", "cooldown", "retry_base_delay", "degraded_after"] {
            let err = Config::from_yaml(&outbox_yaml(&format!("  {key}: 0s\n"))).unwrap_err();
            assert!(
                matches!(&err, ConfigError::Invalid(msg) if msg.contains(key)),
                "{key}: {err:?}"
            );
        }
    }

    #[test]
    fn rejects_zero_outbox_counts() {
        let err = Config::from_yaml(&outbox_yaml("  max_messages: 0\n")).unwrap_err();
        assert!(matches!(err, ConfigError::Invalid(msg) if msg.contains("max_messages")));
        let err = Config::from_yaml(&outbox_yaml("  max_attempts: 0\n")).unwrap_err();
        assert!(matches!(err, ConfigError::Invalid(msg) if msg.contains("max_attempts")));
    }

    #[test]
    fn rejects_retry_max_below_base() {
        let yaml = outbox_yaml("  retry_base_delay: 10s\n  retry_max_delay: 5s\n");
        let err = Config::from_yaml(&yaml).unwrap_err();
        assert!(matches!(err, ConfigError::Invalid(msg) if msg.contains("retry_max_delay")));
    }

    #[test]
    fn rejects_unknown_outbox_key() {
        assert!(Config::from_yaml(&outbox_yaml("  cooldwn: 5s\n")).is_err());
    }

    #[test]
    fn outbox_database_url_is_hidden_in_debug_output() {
        let cfg = Config::from_yaml(&outbox_yaml("")).unwrap();
        assert!(!format!("{cfg:?}").contains("pw@"));
    }
}
