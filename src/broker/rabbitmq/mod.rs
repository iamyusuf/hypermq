use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use lapin::options::{BasicPublishOptions, ConfirmSelectOptions};
use lapin::protocol::{AMQPErrorKind, AMQPSoftError};
use lapin::types::{AMQPValue, FieldTable};
use lapin::{BasicProperties, Channel, Confirmation, Connection, ConnectionProperties, ErrorKind};
use secrecy::{ExposeSecret, SecretString};
use tokio::sync::Mutex;

use super::{PublishError, PublishRequest, PublishTarget, Publisher};

mod consume;
mod topology;

pub use consume::RabbitBroker;
pub use topology::RabbitTopology;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(5);
const PERSISTENT: u8 = 2;

struct Session {
    connection: Connection,
    channel: Channel,
}

/// Publishes through one confirm-mode channel and reconnects lazily after a failure.
pub struct RabbitPublisher {
    url: SecretString,
    session: Mutex<Option<Session>>,
}

impl RabbitPublisher {
    pub fn new(url: SecretString) -> Self {
        Self {
            url,
            session: Mutex::new(None),
        }
    }

    /// Opens the connection ahead of the first publish.
    pub async fn connect(&self) -> Result<(), PublishError> {
        self.channel().await.map(|_| ())
    }

    async fn channel(&self) -> Result<Channel, PublishError> {
        let mut guard = self.session.lock().await;

        if let Some(session) = guard.as_mut() {
            if session.channel.status().connected() {
                return Ok(session.channel.clone());
            }
            // The broker closes a channel on some errors; the connection can still be fine.
            if session.connection.status().connected() {
                match open_channel(&session.connection).await {
                    Ok(channel) => {
                        session.channel = channel.clone();
                        return Ok(channel);
                    }
                    Err(err) => tracing::warn!(%err, "cannot reopen channel; reconnecting"),
                }
            }
        }

        if let Some(stale) = guard.take() {
            close(stale.connection);
        }

        let connection = connect(&self.url)
            .await
            .map_err(PublishError::Unavailable)?;

        let channel = match open_channel(&connection).await {
            Ok(channel) => channel,
            Err(err) => {
                close(connection);
                return Err(err);
            }
        };

        tracing::info!("connected to RabbitMQ");
        *guard = Some(Session {
            connection,
            channel: channel.clone(),
        });
        Ok(channel)
    }

    /// Drops the session after a timeout, when the channel state is unknown.
    async fn discard(&self) {
        if let Some(session) = self.session.lock().await.take() {
            close(session.connection);
        }
    }
}

#[async_trait]
impl Publisher for RabbitPublisher {
    async fn publish(&self, request: PublishRequest) -> Result<(), PublishError> {
        let channel = self.channel().await?;

        let (exchange, routing_key) = match request.target {
            PublishTarget::Exchange(name) => (name, request.routing_key),
            PublishTarget::Queue(name) => (String::new(), name),
        };

        let mut headers = FieldTable::default();
        for (key, value) in request.headers {
            headers.insert(key.into(), AMQPValue::LongString(value.into()));
        }
        let properties = BasicProperties::default()
            .with_message_id(request.message_id.into())
            .with_content_type(request.content_type.into())
            .with_delivery_mode(PERSISTENT)
            .with_timestamp(unix_seconds())
            .with_headers(headers);

        // `mandatory` makes the broker return unroutable messages instead of dropping them.
        let options = BasicPublishOptions {
            mandatory: true,
            ..Default::default()
        };

        let confirmation = tokio::time::timeout(CONFIRM_TIMEOUT, async {
            channel
                .basic_publish(
                    exchange.into(),
                    routing_key.into(),
                    options,
                    &request.body,
                    properties,
                )
                .await?
                .await
        })
        .await;

        match confirmation {
            Err(_) => {
                self.discard().await;
                Err(PublishError::Unavailable("confirm timed out".into()))
            }
            Ok(Err(err)) => Err(classify(err)),
            Ok(Ok(Confirmation::Ack(None))) => Ok(()),
            Ok(Ok(Confirmation::Ack(Some(_)) | Confirmation::Nack(Some(_)))) => {
                Err(PublishError::Unroutable)
            }
            Ok(Ok(Confirmation::Nack(None))) => Err(PublishError::Rejected),
            Ok(Ok(Confirmation::NotRequested)) => {
                Err(PublishError::Unavailable("confirms not enabled".into()))
            }
        }
    }
}

/// Opens a connection; the error text never contains the URL.
async fn connect(url: &SecretString) -> Result<Connection, String> {
    tokio::time::timeout(
        CONNECT_TIMEOUT,
        Connection::connect(
            url.expose_secret(),
            ConnectionProperties::default().with_connection_name("hypermq".into()),
        ),
    )
    .await
    .map_err(|_| "connect timed out".to_owned())?
    .map_err(|err| err.to_string())
}

async fn open_channel(connection: &Connection) -> Result<Channel, PublishError> {
    let setup = async {
        let channel = connection.create_channel().await?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await?;
        Ok(channel)
    };
    tokio::time::timeout(CONNECT_TIMEOUT, setup)
        .await
        .map_err(|_| PublishError::Unavailable("opening channel timed out".into()))?
        .map_err(unavailable)
}

/// Closes in the background; the connection may already be dead.
fn close(connection: Connection) {
    tokio::spawn(async move {
        let _ = connection.close(200, "closing".into()).await;
    });
}

fn unavailable(err: lapin::Error) -> PublishError {
    PublishError::Unavailable(err.to_string())
}

fn classify(err: lapin::Error) -> PublishError {
    match err.kind() {
        ErrorKind::ProtocolError(amqp)
            if matches!(amqp.kind(), AMQPErrorKind::Soft(AMQPSoftError::NOTFOUND)) =>
        {
            PublishError::TargetNotFound
        }
        _ => unavailable(err),
    }
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}
