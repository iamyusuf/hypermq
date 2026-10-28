use std::io;
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read config file {}: {source}", path.display())]
    Read { path: PathBuf, source: io::Error },
    #[error("cannot parse config: {0}")]
    Parse(#[from] serde_yaml_ng::Error),
    #[error("invalid config: {0}")]
    Invalid(String),
}

#[derive(Debug, thiserror::Error)]
pub enum TopologyError {
    #[error("broker unavailable: {0}")]
    Unavailable(String),
    /// Re-declaring with different settings than the existing entity.
    #[error("{what} conflicts with an existing declaration: {message}")]
    Conflict { what: String, message: String },
    /// A binding or dead-letter target that does not exist.
    #[error("{what} refers to something that does not exist: {message}")]
    NotFound { what: String, message: String },
    #[error("broker refused {what}: {message}")]
    Refused { what: String, message: String },
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("topology: {0}")]
    Topology(#[from] TopologyError),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}
