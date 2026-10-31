use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use clap::{Parser, Subcommand, ValueEnum};
use hypermq::api::{self, AppState};
use hypermq::broker::Publisher;
use hypermq::broker::rabbitmq::{RabbitBroker, RabbitPublisher, RabbitTopology};
use hypermq::broker::topology;
use hypermq::config::{Config, OutboxConfig, Role};
use hypermq::consumers;
use hypermq::error::{ConfigError, Error};
use hypermq::metrics::Metrics;
use hypermq::outbox::{self, OutboxPublisher, relay};
use hypermq::tasks::TaskRegistry;
use secrecy::ExposeSecret;
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
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

    /// Log output format. Filter with `RUST_LOG` (default `info`).
    #[arg(long, env = "HYPERMQ_LOG_FORMAT", value_enum, default_value = "text")]
    log_format: LogFormat,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Creates or upgrades the outbox tables, then exits.
    Migrate,
}

#[derive(Clone, Copy, ValueEnum)]
enum LogFormat {
    Text,
    Json,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    init_logging(cli.log_format);

    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(%err, "fatal error");
            ExitCode::FAILURE
        }
    }
}

fn init_logging(format: LogFormat) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    match format {
        LogFormat::Text => tracing_subscriber::fmt().with_env_filter(filter).init(),
        LogFormat::Json => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init(),
    }
}

/// Task kinds available in this build. Config referencing any other kind fails at load.
fn task_registry() -> TaskRegistry {
    #[allow(unused_mut)]
    let mut registry = TaskRegistry::default();
    #[cfg(feature = "task-http")]
    registry.register(Arc::new(hypermq::tasks::http::HttpTaskFactory));
    #[cfg(feature = "task-email")]
    registry.register(Arc::new(hypermq::tasks::email::EmailTaskFactory));
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
    if matches!(cli.command, Some(Command::Migrate)) {
        return migrate(&config).await;
    }
    // Unknown task kinds and invalid task settings fail here, before any connection is made.
    let metrics = Arc::new(Metrics::default());
    let consumers = consumers::build(&config.consumers, &task_registry(), &metrics)?;

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

    let broker = Arc::new(RabbitPublisher::new(config.broker.url));
    if cli.role != Role::Consumers
        && let Err(err) = broker.connect().await
    {
        tracing::warn!(%err, "RabbitMQ is unreachable at startup; publishes fail until it recovers");
    }
    let broker: Arc<dyn Publisher> = broker;

    // Publishes fall back to the outbox when RabbitMQ is unavailable. The relay drains it later.
    let mut relay_task: Option<JoinHandle<()>> = None;
    let publisher: Arc<dyn Publisher> = match &config.outbox {
        Some(settings) if cli.role != Role::Consumers => {
            let store = outbox::connect(settings.database_url.expose_secret()).await?;
            if let Err(err) = store.snapshot(Utc::now()).await {
                tracing::error!(%err, "outbox database is not ready; run `hypermq migrate` first");
                return Err(err.into());
            }
            relay_task = Some(tokio::spawn(relay::run(
                Arc::clone(&store),
                Arc::clone(&broker),
                relay_policy(settings),
                Arc::clone(&metrics),
                shutdown.clone(),
            )));
            Arc::new(OutboxPublisher::new(
                Arc::clone(&broker),
                store,
                settings.max_messages,
                Arc::clone(&metrics),
            ))
        }
        _ => broker,
    };

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
            metrics: Arc::clone(&metrics),
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
    if let Some(mut relay) = relay_task {
        stop_relay(&mut relay).await;
    }
    served?;
    Ok(())
}

/// Creates or upgrades the outbox tables.
async fn migrate(config: &Config) -> Result<(), Error> {
    let Some(outbox) = &config.outbox else {
        return Err(Error::Config(ConfigError::Invalid(
            "no `outbox` section in the config; nothing to migrate".to_owned(),
        )));
    };
    outbox::migrate(outbox.database_url.expose_secret()).await?;
    tracing::info!("outbox migrations applied");
    Ok(())
}

fn relay_policy(settings: &OutboxConfig) -> relay::RelayPolicy {
    relay::RelayPolicy {
        max_attempts: settings.max_attempts,
        base_delay: settings.retry_base_delay,
        max_delay: settings.retry_max_delay,
        max_age: settings.max_age,
        cooldown: settings.cooldown,
        degraded_after: settings.degraded_after,
    }
}

/// Waits for the relay to finish its current cycle, aborting it after the grace period.
async fn stop_relay(relay: &mut JoinHandle<()>) {
    match tokio::time::timeout(SHUTDOWN_GRACE, &mut *relay).await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "outbox relay task failed"),
        Err(_) => {
            tracing::warn!(grace = ?SHUTDOWN_GRACE, "outbox relay did not stop within the grace period; aborting it");
            relay.abort();
        }
    }
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
