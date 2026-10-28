use std::future::Future;
use std::time::Duration;

use async_trait::async_trait;
use lapin::options::{ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions};
use lapin::protocol::{AMQPErrorKind, AMQPSoftError};
use lapin::types::{AMQPValue, FieldTable};
use lapin::{Channel, Connection, ErrorKind};
use secrecy::SecretString;

use super::connect;
use crate::broker::topology::{
    BindingSpec, ExchangeKind, ExchangeSpec, QueueSpec, TopologyManager,
};
use crate::error::TopologyError;

const OPERATION_TIMEOUT: Duration = Duration::from_secs(10);

/// Declares topology over its own connection, using a fresh channel per declaration
/// because the broker closes a channel when a declaration fails.
pub struct RabbitTopology {
    connection: Connection,
}

impl RabbitTopology {
    pub async fn connect(url: &SecretString) -> Result<Self, TopologyError> {
        let connection = connect(url).await.map_err(TopologyError::Unavailable)?;
        Ok(Self { connection })
    }

    /// Closes the connection once declarations are done.
    pub async fn close(self) {
        let _ = self.connection.close(200, "topology applied".into()).await;
    }

    async fn run<F, Fut>(&self, what: String, operation: F) -> Result<(), TopologyError>
    where
        F: FnOnce(Channel) -> Fut,
        Fut: Future<Output = lapin::Result<()>>,
    {
        let channel = tokio::time::timeout(OPERATION_TIMEOUT, self.connection.create_channel())
            .await
            .map_err(|_| TopologyError::Unavailable("opening channel timed out".into()))?
            .map_err(|err| TopologyError::Unavailable(err.to_string()))?;

        let result = tokio::time::timeout(OPERATION_TIMEOUT, operation(channel.clone())).await;
        // Already closed by the broker after a failed declaration.
        let _ = channel.close(200, "done".into()).await;

        match result {
            Err(_) => Err(TopologyError::Unavailable(format!("{what} timed out"))),
            Ok(result) => result.map_err(|err| classify(what, err)),
        }
    }
}

#[async_trait]
impl TopologyManager for RabbitTopology {
    async fn declare_exchange(&self, spec: &ExchangeSpec) -> Result<(), TopologyError> {
        let kind = match spec.kind {
            ExchangeKind::Direct => lapin::ExchangeKind::Direct,
            ExchangeKind::Fanout => lapin::ExchangeKind::Fanout,
            ExchangeKind::Topic => lapin::ExchangeKind::Topic,
        };
        let options = ExchangeDeclareOptions {
            durable: spec.durable,
            ..Default::default()
        };
        self.run(format!("exchange `{}`", spec.name), |channel| async move {
            channel
                .exchange_declare(
                    spec.name.as_str().into(),
                    kind,
                    options,
                    FieldTable::default(),
                )
                .await
        })
        .await
    }

    async fn declare_queue(&self, spec: &QueueSpec) -> Result<(), TopologyError> {
        let options = QueueDeclareOptions {
            durable: spec.durable,
            ..Default::default()
        };
        let mut arguments = FieldTable::default();
        if let Some(exchange) = &spec.dead_letter_exchange {
            arguments.insert(
                "x-dead-letter-exchange".into(),
                AMQPValue::LongString(exchange.as_str().into()),
            );
        }
        if let Some(key) = &spec.dead_letter_routing_key {
            arguments.insert(
                "x-dead-letter-routing-key".into(),
                AMQPValue::LongString(key.as_str().into()),
            );
        }
        if let Some(ttl) = spec.message_ttl {
            // Validated to fit in u32 milliseconds when the config is loaded.
            let millis = u32::try_from(ttl.as_millis()).unwrap_or(u32::MAX);
            arguments.insert("x-message-ttl".into(), AMQPValue::LongUInt(millis));
        }
        self.run(format!("queue `{}`", spec.name), |channel| async move {
            channel
                .queue_declare(spec.name.as_str().into(), options, arguments)
                .await
                .map(|_| ())
        })
        .await
    }

    async fn bind(&self, spec: &BindingSpec) -> Result<(), TopologyError> {
        let what = format!(
            "binding of queue `{}` to exchange `{}`",
            spec.queue, spec.exchange
        );
        self.run(what, |channel| async move {
            channel
                .queue_bind(
                    spec.queue.as_str().into(),
                    spec.exchange.as_str().into(),
                    spec.routing_key.as_str().into(),
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await
        })
        .await
    }
}

fn classify(what: String, err: lapin::Error) -> TopologyError {
    let ErrorKind::ProtocolError(amqp) = err.kind() else {
        return TopologyError::Unavailable(format!("{what}: {err}"));
    };
    let message = amqp.get_message().to_string();
    match amqp.kind() {
        AMQPErrorKind::Soft(AMQPSoftError::PRECONDITIONFAILED) => {
            TopologyError::Conflict { what, message }
        }
        AMQPErrorKind::Soft(AMQPSoftError::NOTFOUND) => TopologyError::NotFound { what, message },
        _ => TopologyError::Refused { what, message },
    }
}
