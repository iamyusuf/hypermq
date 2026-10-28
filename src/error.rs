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
pub enum Error {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}
