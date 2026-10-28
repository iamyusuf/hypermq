use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use hypermq::api::{self, AppState};
use hypermq::config::{Config, Role};
use hypermq::error::Error;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    version,
    about = "HTTP wrapper around RabbitMQ with configurable consumers"
)]
struct Cli {
    /// Path to the YAML configuration file.
    #[arg(long, env = "HYPERMQ_CONFIG", default_value = "config.yaml")]
    config: PathBuf,

    /// Which components this instance runs.
    #[arg(long, env = "HYPERMQ_ROLE", value_enum, default_value = "all")]
    role: Role,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(%err, "fatal error");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), Error> {
    let config = Config::load(&cli.config)?;

    let listener = tokio::net::TcpListener::bind(config.http_api.bind).await?;
    tracing::info!(role = cli.role.as_str(), bind = %config.http_api.bind, "starting");

    axum::serve(
        listener,
        api::router(AppState {
            role: cli.role,
            max_body_bytes: config.http_api.max_body_bytes,
        }),
    )
    .await?;
    Ok(())
}
