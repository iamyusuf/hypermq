use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use hypermq::api::{self, AppState};
use hypermq::broker::rabbitmq::{RabbitBroker, RabbitPublisher, RabbitTopology};
use hypermq::broker::topology;
use hypermq::config::{Config, Role};
use hypermq::consumers;
use hypermq::error::Error;
use hypermq::tasks::TaskRegistry;
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

/// Task kinds available in this build. Config referencing any other kind fails at load.
fn task_registry() -> TaskRegistry {
    #[allow(unused_mut)]
    let mut registry = TaskRegistry::default();
    #[cfg(feature = "task-http")]
    registry.register(Arc::new(hypermq::tasks::http::HttpTaskFactory));
    registry
}

async fn run(cli: Cli) -> Result<(), Error> {
    let config = Config::load(&cli.config)?;
    // Unknown task kinds and invalid task settings fail here, before any connection is made.
    let consumers = consumers::build(&config.consumers, &task_registry())?;

    // Fails startup when declared topology cannot be created or conflicts with the broker.
    if !config.topology.is_empty() {
        let manager = RabbitTopology::connect(&config.broker.url).await?;
        let result = topology::apply(&manager, &config.topology).await;
        manager.close().await;
        result?;
    }

    if cli.role != Role::Api {
        let broker = Arc::new(RabbitBroker::new(config.broker.url.clone()));
        for consumer in consumers {
            let broker = Arc::clone(&broker);
            tokio::spawn(async move {
                let name = consumer.name().to_owned();
                match consumer.run(broker.as_ref()).await {
                    Ok(()) => tracing::info!(consumer = %name, "consumer stopped"),
                    // Reconnecting after broker failures is part of Phase 5.
                    Err(err) => tracing::error!(consumer = %name, %err, "consumer stopped"),
                }
            });
        }
    }

    let publisher = Arc::new(RabbitPublisher::new(config.broker.url));
    if cli.role != Role::Consumers
        && let Err(err) = publisher.connect().await
    {
        tracing::warn!(%err, "RabbitMQ is unreachable at startup; publishes fail until it recovers");
    }

    let listener = tokio::net::TcpListener::bind(config.http_api.bind).await?;
    tracing::info!(role = cli.role.as_str(), bind = %config.http_api.bind, "starting");

    axum::serve(
        listener,
        api::router(AppState {
            role: cli.role,
            max_body_bytes: config.http_api.max_body_bytes,
            publisher,
        }),
    )
    .await?;
    Ok(())
}
