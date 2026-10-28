use std::net::SocketAddr;
use std::path::Path;

use clap::ValueEnum;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;

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
}

#[derive(Debug, Deserialize)]
pub struct BrokerConfig {
    pub url: SecretString,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpApiConfig {
    #[serde(default = "default_bind")]
    pub bind: SocketAddr,
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: usize,
}

impl Default for HttpApiConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            max_body_bytes: default_max_body_bytes(),
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

    /// Sections for later phases (topology, consumers) are ignored until they are implemented.
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
    fn ignores_sections_for_later_phases() {
        let yaml = format!("{MINIMAL}consumers: []\ntopology: {{}}\n");
        assert!(Config::from_yaml(&yaml).is_ok());
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
    fn debug_output_hides_the_url() {
        let cfg = Config::from_yaml(MINIMAL).unwrap();
        assert!(!format!("{cfg:?}").contains("pw@"));
    }
}
