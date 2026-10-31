//! Email Task: sends the message body as an email through an SMTP server and maps the reply to an outcome.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use lettre::message::header::ContentType;
use lettre::message::{Mailbox, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Tokio1Executor};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;

use crate::message::Message;
use crate::tasks::{Task, TaskConfigError, TaskFactory, TaskOutcome};

const KIND: &str = "email";

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TlsMode {
    /// STARTTLS is required before any credentials or message are sent.
    #[default]
    Starttls,
    /// Implicit TLS from the first byte (SMTPS).
    Tls,
    /// No TLS. Only for trusted networks; credentials are refused in this mode.
    Plaintext,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmailSettings {
    pub host: String,
    /// Defaults to 587 for `starttls`, 465 for `tls`, and 25 for `plaintext`.
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub tls: TlsMode,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<SecretString>,
    pub from: String,
    /// Recipients of every message. The message body is the email body.
    pub to: Vec<String>,
    pub subject: String,
    /// Hosts the SMTP server may be. Required and matched exactly (case-insensitive).
    pub allowed_hosts: Vec<String>,
}

pub struct EmailTask {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    from: Mailbox,
    to: Vec<Mailbox>,
    subject: String,
}

impl EmailTask {
    fn new(settings: EmailSettings) -> Result<Self, String> {
        let EmailSettings {
            host,
            port,
            tls,
            username,
            password,
            from,
            to,
            subject,
            allowed_hosts,
        } = settings;

        if host.is_empty()
            || host
                .chars()
                .any(|c| c.is_whitespace() || matches!(c, '/' | '@' | ':'))
        {
            return Err("host is not a valid hostname".into());
        }
        if allowed_hosts.is_empty() {
            return Err("allowed_hosts must list at least one host".into());
        }
        if !allowed_hosts
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(&host))
        {
            return Err(format!("host `{host}` is not in allowed_hosts"));
        }
        if port == Some(0) {
            return Err("port must be between 1 and 65535".into());
        }

        let mut builder = match tls {
            TlsMode::Starttls => AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&host)
                .map_err(|_| "could not configure STARTTLS".to_owned())?,
            TlsMode::Tls => AsyncSmtpTransport::<Tokio1Executor>::relay(&host)
                .map_err(|_| "could not configure implicit TLS".to_owned())?,
            TlsMode::Plaintext => AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&host),
        };
        if let Some(port) = port {
            builder = builder.port(port);
        }
        match (username, password) {
            (None, None) => {}
            (Some(username), Some(password)) => {
                if tls == TlsMode::Plaintext {
                    return Err("credentials are not allowed with tls `plaintext`".into());
                }
                let password = password.expose_secret().to_owned();
                builder = builder.credentials(Credentials::new(username, password));
            }
            _ => return Err("username and password must be set together".into()),
        }

        let from = from
            .parse::<Mailbox>()
            .map_err(|_| "from is not a valid mailbox".to_owned())?;
        if to.is_empty() {
            return Err("to must list at least one address".into());
        }
        let to = to
            .iter()
            .map(|address| address.parse::<Mailbox>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| "to contains an invalid mailbox".to_owned())?;
        if subject.is_empty() || subject.chars().any(char::is_control) {
            return Err("subject must be non-empty and contain no control characters".into());
        }

        Ok(Self {
            transport: builder.build(),
            from,
            to,
            subject,
        })
    }

    /// Builds the email for one message. Errors are fixed strings that name the field, never its value.
    fn compose(&self, message: &Message) -> Result<lettre::message::Message, &'static str> {
        let body =
            std::str::from_utf8(&message.body).map_err(|_| "message body is not valid UTF-8")?;
        let content_type =
            content_type_for(message.headers.get("content-type").map(String::as_str))?;
        let message_id = message_id_for(message.id.as_str(), self.from.email.domain())?;

        let mut builder = lettre::message::Message::builder().from(self.from.clone());
        for recipient in &self.to {
            builder = builder.to(recipient.clone());
        }
        builder
            .subject(self.subject.clone())
            .message_id(Some(message_id))
            .singlepart(
                SinglePart::builder()
                    .header(content_type)
                    .body(body.to_owned()),
            )
            .map_err(|_| "message could not be built")
    }
}

impl fmt::Debug for EmailTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Host, credentials, and addresses are omitted; the transport holds the credentials.
        f.debug_struct("EmailTask")
            .field("recipients", &self.to.len())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Task for EmailTask {
    async fn execute(&self, message: &Message) -> TaskOutcome {
        let email = match self.compose(message) {
            Ok(email) => email,
            Err(reason) => {
                return TaskOutcome::Permanent {
                    reason: reason.into(),
                };
            }
        };
        match self.transport.send(email).await {
            Ok(_) => TaskOutcome::Success,
            Err(err) => outcome_for_error(&err),
        }
    }
}

/// Only `text/plain` and `text/html` are sent; a missing header means `text/plain`.
fn content_type_for(value: Option<&str>) -> Result<ContentType, &'static str> {
    let Some(value) = value else {
        return Ok(ContentType::TEXT_PLAIN);
    };
    let media_type = value.split(';').next().unwrap_or_default().trim();
    if media_type.eq_ignore_ascii_case("text/plain") {
        Ok(ContentType::TEXT_PLAIN)
    } else if media_type.eq_ignore_ascii_case("text/html") {
        Ok(ContentType::TEXT_HTML)
    } else {
        Err("content-type must be text/plain or text/html")
    }
}

/// The stable message id becomes the Message-ID header so downstream systems can deduplicate.
fn message_id_for(id: &str, domain: &str) -> Result<String, &'static str> {
    let valid = !id.is_empty()
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'));
    if valid {
        Ok(format!("<{id}@{domain}>"))
    } else {
        Err("message id is not a valid Message-ID value")
    }
}

/// Permanent for 5xx replies; transient for 4xx replies and transport failures.
/// Reasons are fixed strings because SMTP reply text can name recipients.
fn outcome_for_error(err: &lettre::transport::smtp::Error) -> TaskOutcome {
    let retryable = |reason: &str| TaskOutcome::Retryable {
        reason: reason.into(),
        retry_after: None,
    };
    if err.is_permanent() {
        TaskOutcome::Permanent {
            reason: "smtp server rejected the message".into(),
        }
    } else if err.is_transient() {
        retryable("smtp server deferred the message")
    } else if err.is_timeout() {
        retryable("smtp request timed out")
    } else if err.is_tls() {
        retryable("tls negotiation failed")
    } else {
        retryable("smtp delivery failed")
    }
}

pub struct EmailTaskFactory;

impl TaskFactory for EmailTaskFactory {
    fn kind(&self) -> &'static str {
        KIND
    }

    fn build(&self, settings: serde_json::Value) -> Result<Arc<dyn Task>, TaskConfigError> {
        let settings: EmailSettings =
            serde_json::from_value(settings).map_err(|err| TaskConfigError::InvalidSettings {
                kind: KIND.into(),
                message: err.to_string(),
            })?;
        let task =
            EmailTask::new(settings).map_err(|message| TaskConfigError::InvalidSettings {
                kind: KIND.into(),
                message,
            })?;
        Ok(Arc::new(task))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};
    use std::thread;

    use bytes::Bytes;
    use serde_json::json;

    use super::*;
    use crate::message::{MessageId, RoutingInfo};

    fn settings(port: u16) -> serde_json::Value {
        json!({
            "host": "127.0.0.1",
            "port": port,
            "tls": "plaintext",
            "from": "HyperMQ <noreply@example.com>",
            "to": ["ops@example.com"],
            "subject": "Order created",
            "allowed_hosts": ["127.0.0.1"],
        })
    }

    fn with(key: &str, value: serde_json::Value) -> serde_json::Value {
        let mut settings = settings(2525);
        settings[key] = value;
        settings
    }

    fn build_error(settings: serde_json::Value) -> String {
        match EmailTaskFactory.build(settings) {
            Ok(_) => panic!("settings should be rejected"),
            Err(err) => err.to_string(),
        }
    }

    fn message(id: &str, body: &'static [u8], content_type: Option<&str>) -> Message {
        let mut headers = BTreeMap::new();
        if let Some(content_type) = content_type {
            headers.insert("content-type".to_owned(), content_type.to_owned());
        }
        Message {
            id: MessageId::new(id),
            body: Bytes::from_static(body),
            headers,
            routing: RoutingInfo {
                exchange: String::new(),
                routing_key: "orders".into(),
                queue: "orders".into(),
            },
            attempt: 1,
        }
    }

    fn task(settings: serde_json::Value) -> EmailTask {
        let settings: EmailSettings = serde_json::from_value(settings).unwrap();
        EmailTask::new(settings).unwrap()
    }

    /// Minimal SMTP server for one session per connection. Records each DATA payload.
    struct FakeSmtp {
        port: u16,
        received: Arc<Mutex<Vec<String>>>,
    }

    fn fake_smtp(rcpt_reply: &'static str, data_reply: &'static str) -> FakeSmtp {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let received = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&received);
        thread::spawn(move || {
            let Ok((stream, _)) = listener.accept() else {
                return;
            };
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);
            let _ = writer.write_all(b"220 fake ready\r\n");
            let mut line = String::new();
            loop {
                line.clear();
                if reader.read_line(&mut line).unwrap_or(0) == 0 {
                    break;
                }
                let command = line.trim_end().to_ascii_uppercase();
                let reply = if command.starts_with("EHLO") || command.starts_with("HELO") {
                    "250 fake"
                } else if command.starts_with("MAIL FROM") || command == "RSET" {
                    "250 ok"
                } else if command.starts_with("RCPT TO") {
                    rcpt_reply
                } else if command == "DATA" {
                    let _ = writer.write_all(b"354 go ahead\r\n");
                    let mut payload = String::new();
                    loop {
                        line.clear();
                        if reader.read_line(&mut line).unwrap_or(0) == 0 || line == ".\r\n" {
                            break;
                        }
                        payload.push_str(&line);
                    }
                    sink.lock().unwrap().push(payload);
                    data_reply
                } else if command == "QUIT" {
                    let _ = writer.write_all(b"221 bye\r\n");
                    break;
                } else {
                    "502 not implemented"
                };
                let _ = writer.write_all(format!("{reply}\r\n").as_bytes());
            }
        });
        FakeSmtp { port, received }
    }

    #[test]
    fn settings_build_a_task() {
        assert!(EmailTaskFactory.build(settings(2525)).is_ok());
        assert_eq!(EmailTaskFactory.kind(), "email");
    }

    #[test]
    fn settings_require_host_in_allowed_hosts() {
        let mut settings = settings(2525);
        settings["allowed_hosts"] = json!(["smtp.example.com"]);
        assert!(build_error(settings).contains("allowed_hosts"));
        assert!(build_error(with("allowed_hosts", json!([]))).contains("at least one"));
    }

    #[test]
    fn settings_reject_unknown_keys() {
        assert!(build_error(with("bcc", json!("x@example.com"))).contains("unknown field"));
    }

    #[test]
    fn settings_reject_invalid_addresses_and_empty_recipients() {
        assert!(build_error(with("from", json!("not-an-address"))).contains("from"));
        assert!(build_error(with("to", json!(["bad address"]))).contains("to"));
        assert!(build_error(with("to", json!([]))).contains("at least one"));
    }

    #[test]
    fn settings_reject_control_characters_in_subject() {
        assert!(
            build_error(with("subject", json!("Hi\r\nBcc: x@example.com"))).contains("subject")
        );
        assert!(build_error(with("subject", json!(""))).contains("subject"));
    }

    #[test]
    fn settings_require_username_and_password_together() {
        let only_user = with("username", json!("mailer"));
        assert!(build_error(only_user).contains("together"));
    }

    #[test]
    fn settings_refuse_credentials_over_plaintext() {
        let mut settings = settings(2525);
        settings["username"] = json!("mailer");
        settings["password"] = json!("s3cret-pass");
        assert!(build_error(settings).contains("plaintext"));
    }

    #[test]
    fn settings_reject_port_zero_and_bad_host() {
        assert!(build_error(with("port", json!(0))).contains("port"));
        assert!(build_error(with("host", json!("smtp.example.com:25"))).contains("host"));
    }

    #[test]
    fn debug_output_hides_password_and_addresses() {
        let mut settings = settings(2525);
        settings["tls"] = json!("starttls");
        settings["username"] = json!("mailer");
        settings["password"] = json!("s3cret-pass");
        let debug = format!("{:?}", task(settings));
        assert!(!debug.contains("s3cret-pass"));
        assert!(!debug.contains("ops@example.com"));
        assert!(!debug.contains("127.0.0.1"));
    }

    #[test]
    fn content_type_defaults_to_plain_and_accepts_html() {
        assert_eq!(content_type_for(None), Ok(ContentType::TEXT_PLAIN));
        assert_eq!(
            content_type_for(Some("Text/HTML; charset=utf-8")),
            Ok(ContentType::TEXT_HTML)
        );
        assert!(content_type_for(Some("application/json")).is_err());
    }

    #[test]
    fn message_id_must_be_a_safe_header_token() {
        assert_eq!(
            message_id_for("order-1", "example.com").unwrap(),
            "<order-1@example.com>"
        );
        assert!(message_id_for("bad id\r\nBcc: x", "example.com").is_err());
        assert!(message_id_for("", "example.com").is_err());
    }

    #[test]
    fn compose_rejects_non_utf8_body() {
        let task = task(settings(2525));
        let bad = message("order-1", b"\xff\xfe", None);
        assert!(task.compose(&bad).is_err());
    }

    #[tokio::test]
    async fn delivers_message_and_reports_success() {
        let smtp = fake_smtp("250 ok", "250 queued");
        let task = task(settings(smtp.port));
        let outcome = task
            .execute(&message("order-1", b"Order 42 was created", None))
            .await;
        assert_eq!(outcome, TaskOutcome::Success);

        let received = smtp.received.lock().unwrap();
        assert_eq!(received.len(), 1);
        let payload = &received[0];
        assert!(payload.contains("Subject: Order created"));
        assert!(payload.contains("Message-ID: <order-1@example.com>"));
        assert!(payload.contains("To: ops@example.com"));
        assert!(payload.contains("Order 42 was created"));
        assert!(payload.contains("text/plain"));
    }

    #[tokio::test]
    async fn html_content_type_is_sent_as_html() {
        let smtp = fake_smtp("250 ok", "250 queued");
        let task = task(settings(smtp.port));
        let outcome = task
            .execute(&message("order-2", b"<p>Created</p>", Some("text/html")))
            .await;
        assert_eq!(outcome, TaskOutcome::Success);
        assert!(smtp.received.lock().unwrap()[0].contains("text/html"));
    }

    #[tokio::test]
    async fn unsupported_content_type_is_permanent_and_not_sent() {
        let smtp = fake_smtp("250 ok", "250 queued");
        let task = task(settings(smtp.port));
        let outcome = task
            .execute(&message("order-3", b"{}", Some("application/json")))
            .await;
        assert!(matches!(outcome, TaskOutcome::Permanent { .. }));
        assert!(smtp.received.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn invalid_message_id_is_permanent_and_not_sent() {
        let smtp = fake_smtp("250 ok", "250 queued");
        let task = task(settings(smtp.port));
        let outcome = task.execute(&message("bad id", b"hello", None)).await;
        assert!(matches!(outcome, TaskOutcome::Permanent { .. }));
        assert!(smtp.received.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn permanent_rejection_maps_to_permanent() {
        let smtp = fake_smtp("550 no such user", "250 queued");
        let task = task(settings(smtp.port));
        let outcome = task.execute(&message("order-4", b"hello", None)).await;
        assert!(matches!(outcome, TaskOutcome::Permanent { .. }));
    }

    #[tokio::test]
    async fn transient_reply_maps_to_retryable() {
        let smtp = fake_smtp("250 ok", "451 try again later");
        let task = task(settings(smtp.port));
        let outcome = task.execute(&message("order-5", b"hello", None)).await;
        assert!(matches!(outcome, TaskOutcome::Retryable { .. }));
    }

    #[tokio::test]
    async fn connection_failure_maps_to_retryable() {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let task = task(settings(port));
        let outcome = task.execute(&message("order-6", b"hello", None)).await;
        match outcome {
            TaskOutcome::Retryable { reason, .. } => {
                assert!(!reason.contains("127.0.0.1"));
            }
            other => panic!("expected retryable, got {other:?}"),
        }
    }
}
