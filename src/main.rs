use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use hypermq::api::{self, AppState};
use hypermq::broker::rabbitmq::{RabbitBroker, RabbitPublisher, RabbitTopology};
use hypermq::broker::topology;
use hypermq::config::{Config, Role};
use hypermq::consumers;
use hypermq::error::Error;
use hypermq::tasks::TaskRegistry;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing_subscriber::EnvFilter;

/// How long consumers may finish in-flight deliveries after shutdown starts.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

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
    #[cfg(feature = "task-amqp")]
    {
        let publishers: hypermq::tasks::amqp::PublisherFactory = Arc::new(
            |url: secrecy::SecretString| -> Arc<dyn hypermq::broker::Publisher> {
                Arc::new(hypermq::broker::rabbitmq::RabbitPublisher::new(url))
            },
        );
        registry.register(Arc::new(hypermq::tasks::amqp::AmqpTaskFactory::new(
            publishers,
        )));
    }
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

    let (stop, shutdown) = watch::channel(false);
    let stop = Arc::new(stop);
    let mut consumer_tasks = JoinSet::new();
    if cli.role != Role::Api {
        let broker = Arc::new(RabbitBroker::new(config.broker.url.clone()));
        for consumer in consumers {
            let broker = Arc::clone(&broker);
            let shutdown = shutdown.clone();
            consumer_tasks.spawn(async move {
                let name = consumer.name().to_owned();
                match consumer.supervise(broker.as_ref(), &shutdown).await {
                    Ok(()) => tracing::info!(consumer = %name, "consumer stopped"),
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

    if cli.role != Role::Consumers && config.http_api.bearer_token.is_none() {
        tracing::warn!("http_api.bearer_token is not set; POST /publish accepts any client");
    }
    let listener = tokio::net::TcpListener::bind(config.http_api.bind).await?;
    tracing::info!(role = cli.role.as_str(), bind = %config.http_api.bind, "starting");

    let signal_stop = Arc::clone(&stop);
    let server = axum::serve(
        listener,
        api::router(AppState {
            role: cli.role,
            max_body_bytes: config.http_api.max_body_bytes,
            publisher,
            auth: config.http_api.bearer_token.map(Arc::new),
        }),
    )
    .with_graceful_shutdown(async move {
        shutdown_signal().await;
        tracing::info!("shutdown requested; stopping consumers and draining connections");
        let _ = signal_stop.send(true);
    });
    let served = server.await;

    // Also runs when the server fails, so consumers do not keep running without the API.
    let _ = stop.send(true);
    stop_consumers(&mut consumer_tasks).await;
    served?;
    Ok(())
}

/// Waits for the consumers to finish, aborting any still running after the grace period.
async fn stop_consumers(tasks: &mut JoinSet<()>) {
    let drained = tokio::time::timeout(SHUTDOWN_GRACE, async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
    if drained.is_err() {
        tracing::warn!(grace = ?SHUTDOWN_GRACE, "consumers did not stop within the grace period; aborting them");
        tasks.abort_all();
    }
}

/// Resolves on Ctrl+C, or on SIGTERM on unix. A listener that cannot be installed never fires.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            tracing::error!(%err, "failed to listen for Ctrl+C");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(err) => {
                tracing::error!(%err, "failed to listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}
