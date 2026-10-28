//! HTTP Task: sends the message body to a downstream service and maps the response to an outcome.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use reqwest::redirect::Policy;
use reqwest::{Client, Method, StatusCode, Url};
use serde::Deserialize;

use crate::message::Message;
use crate::tasks::{Task, TaskConfigError, TaskFactory, TaskOutcome};

const KIND: &str = "http";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpSettings {
    #[serde(default = "default_method")]
    pub method: String,
    pub url: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub idempotency_header: Option<String>,
    /// Hosts the URL may target. Required and matched exactly (case-insensitive).
    pub allowed_hosts: Vec<String>,
}

fn default_method() -> String {
    "POST".to_owned()
}

pub struct HttpTask {
    client: Client,
    method: Method,
    url: Url,
    headers: HeaderMap,
    idempotency_header: Option<HeaderName>,
}

impl HttpTask {
    fn new(settings: HttpSettings) -> Result<Self, String> {
        let url = Url::parse(&settings.url).map_err(|_| "url is not a valid URL".to_owned())?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err("url must use http or https".into());
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err("url must not contain credentials; use headers instead".into());
        }
        let host = url
            .host_str()
            .ok_or("url must include a host")?
            .to_ascii_lowercase();
        if settings.allowed_hosts.is_empty() {
            return Err("allowed_hosts must list at least one host".into());
        }
        if !settings
            .allowed_hosts
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(&host))
        {
            return Err(format!("host `{host}` is not in allowed_hosts"));
        }

        let method = Method::from_bytes(settings.method.as_bytes())
            .map_err(|_| "method is not a valid HTTP method".to_owned())?;

        let mut headers = HeaderMap::new();
        for (name, value) in &settings.headers {
            let header_name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| format!("header name `{name}` is not valid"))?;
            // The value is not echoed because it may hold a credential.
            let header_value = HeaderValue::from_str(value)
                .map_err(|_| format!("value for header `{name}` is not valid"))?;
            headers.insert(header_name, header_value);
        }

        let idempotency_header = settings
            .idempotency_header
            .map(|name| {
                HeaderName::from_bytes(name.as_bytes())
                    .map_err(|_| format!("idempotency_header `{name}` is not valid"))
            })
            .transpose()?;

        // Redirects are not followed, so a downstream cannot move the request to another host.
        let client = Client::builder()
            .redirect(Policy::none())
            .build()
            .map_err(|_| "could not create the HTTP client".to_owned())?;

        Ok(Self {
            client,
            method,
            url,
            headers,
            idempotency_header,
        })
    }
}

impl fmt::Debug for HttpTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Headers are omitted because they often hold credentials.
        f.debug_struct("HttpTask")
            .field("method", &self.method)
            .field("host", &self.url.host_str())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Task for HttpTask {
    async fn execute(&self, message: &Message) -> TaskOutcome {
        let mut headers = self.headers.clone();
        if let Some(content_type) = message
            .headers
            .get("content-type")
            .and_then(|value| HeaderValue::from_str(value).ok())
        {
            headers.insert(CONTENT_TYPE, content_type);
        }
        if let Some(name) = &self.idempotency_header {
            match HeaderValue::from_str(message.id.as_str()) {
                Ok(value) => {
                    headers.insert(name.clone(), value);
                }
                Err(_) => {
                    return TaskOutcome::Permanent {
                        reason: "message id is not a valid header value".into(),
                    };
                }
            }
        }

        let request = self
            .client
            .request(self.method.clone(), self.url.clone())
            .headers(headers)
            .body(message.body.clone());

        match request.send().await {
            Ok(response) => outcome_for_status(response.status()),
            Err(err) => outcome_for_error(&err),
        }
    }
}

/// 2xx is success, 429 and 5xx are retryable, and any other status is permanent.
fn outcome_for_status(status: StatusCode) -> TaskOutcome {
    let code = status.as_u16();
    if status.is_success() {
        TaskOutcome::Success
    } else if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        TaskOutcome::Retryable {
            reason: format!("downstream returned {code}"),
            retry_after: None,
        }
    } else {
        TaskOutcome::Permanent {
            reason: format!("downstream returned {code}"),
        }
    }
}

/// Reasons are generic because the error text can include the URL.
fn outcome_for_error(err: &reqwest::Error) -> TaskOutcome {
    if err.is_builder() {
        return TaskOutcome::Permanent {
            reason: "request could not be built".into(),
        };
    }
    let reason = if err.is_timeout() {
        "request timed out"
    } else if err.is_connect() {
        "connection failed"
    } else {
        "request failed"
    };
    TaskOutcome::Retryable {
        reason: reason.into(),
        retry_after: None,
    }
}

pub struct HttpTaskFactory;

impl TaskFactory for HttpTaskFactory {
    fn kind(&self) -> &'static str {
        KIND
    }

    fn build(&self, settings: serde_json::Value) -> Result<Arc<dyn Task>, TaskConfigError> {
        let settings: HttpSettings =
            serde_json::from_value(settings).map_err(|err| TaskConfigError::InvalidSettings {
                kind: KIND.into(),
                message: err.to_string(),
            })?;
        let task = HttpTask::new(settings).map_err(|message| TaskConfigError::InvalidSettings {
            kind: KIND.into(),
            message,
        })?;
        Ok(Arc::new(task))
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener as StdListener;
    use std::sync::Mutex;

    use axum::Router;
    use axum::body::Bytes as BodyBytes;
    use axum::extract::State;
    use axum::http::header::LOCATION;
    use axum::routing::any;
    use serde_json::json;

    use super::*;
    use crate::message::{MessageId, RoutingInfo};

    struct Recording {
        status: StatusCode,
        seen: Mutex<Vec<Seen>>,
    }

    #[derive(Clone)]
    struct Seen {
        method: axum::http::Method,
        headers: axum::http::HeaderMap,
        body: BodyBytes,
    }

    async fn handle(
        State(recording): State<Arc<Recording>>,
        method: axum::http::Method,
        headers: axum::http::HeaderMap,
        body: BodyBytes,
    ) -> impl axum::response::IntoResponse {
        recording.seen.lock().unwrap().push(Seen {
            method,
            headers,
            body,
        });
        (
            recording.status,
            [(LOCATION, "http://127.0.0.1:9/elsewhere")],
        )
    }

    /// Starts a local downstream that always answers with `status`.
    async fn downstream(status: StatusCode) -> (String, Arc<Recording>) {
        let recording = Arc::new(Recording {
            status,
            seen: Mutex::new(Vec::new()),
        });
        let app = Router::new()
            .route("/hook", any(handle))
            .with_state(Arc::clone(&recording));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://127.0.0.1:{port}/hook"), recording)
    }

    /// A URL on a port with nothing listening.
    fn closed_port_url() -> String {
        let listener = StdListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        format!("http://127.0.0.1:{port}/hook")
    }

    fn settings(url: &str) -> serde_json::Value {
        json!({
            "url": url,
            "allowed_hosts": ["127.0.0.1"],
            "headers": {"Authorization": "Bearer secret-token"},
            "idempotency_header": "Idempotency-Key",
        })
    }

    fn build(settings: serde_json::Value) -> Result<Arc<dyn Task>, TaskConfigError> {
        HttpTaskFactory.build(settings)
    }

    fn build_error(settings: serde_json::Value) -> String {
        match build(settings) {
            Err(TaskConfigError::InvalidSettings { message, .. }) => message,
            Err(other) => panic!("unexpected error: {other}"),
            Ok(_) => panic!("expected the settings to be rejected"),
        }
    }

    fn message(id: &str) -> Message {
        let mut headers = BTreeMap::new();
        headers.insert("content-type".to_owned(), "application/json".to_owned());
        Message {
            id: MessageId::new(id),
            body: BodyBytes::from_static(br#"{"order":1}"#),
            headers,
            routing: RoutingInfo {
                exchange: String::new(),
                routing_key: "orders.created".into(),
                queue: "orders.created".into(),
            },
            attempt: 1,
        }
    }

    #[tokio::test]
    async fn success_sends_method_body_headers_and_idempotency_key() {
        let (url, recording) = downstream(StatusCode::OK).await;
        let task = build(settings(&url)).unwrap();

        let outcome = task.execute(&message("m-42")).await;

        assert_eq!(outcome, TaskOutcome::Success);
        let seen = recording.seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 1);
        let request = &seen[0];
        assert_eq!(request.method, axum::http::Method::POST);
        assert_eq!(request.body.as_ref(), br#"{"order":1}"#);
        assert_eq!(request.headers["authorization"], "Bearer secret-token");
        assert_eq!(request.headers["idempotency-key"], "m-42");
        assert_eq!(request.headers["content-type"], "application/json");
    }

    #[tokio::test]
    async fn any_2xx_is_success() {
        for status in [StatusCode::OK, StatusCode::ACCEPTED, StatusCode::NO_CONTENT] {
            let (url, _) = downstream(status).await;
            let task = build(settings(&url)).unwrap();
            assert_eq!(task.execute(&message("m")).await, TaskOutcome::Success);
        }
    }

    #[tokio::test]
    async fn rate_limits_and_server_errors_are_retryable() {
        for status in [
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            let (url, _) = downstream(status).await;
            let task = build(settings(&url)).unwrap();
            assert!(matches!(
                task.execute(&message("m")).await,
                TaskOutcome::Retryable { .. }
            ));
        }
    }

    #[tokio::test]
    async fn other_client_errors_are_permanent() {
        for status in [
            StatusCode::BAD_REQUEST,
            StatusCode::UNAUTHORIZED,
            StatusCode::NOT_FOUND,
        ] {
            let (url, _) = downstream(status).await;
            let task = build(settings(&url)).unwrap();
            assert!(matches!(
                task.execute(&message("m")).await,
                TaskOutcome::Permanent { .. }
            ));
        }
    }

    #[tokio::test]
    async fn redirects_are_not_followed() {
        let (url, _) = downstream(StatusCode::FOUND).await;
        let task = build(settings(&url)).unwrap();
        assert!(matches!(
            task.execute(&message("m")).await,
            TaskOutcome::Permanent { .. }
        ));
    }

    #[tokio::test]
    async fn connection_failure_is_retryable() {
        let task = build(settings(&closed_port_url())).unwrap();
        assert_eq!(
            task.execute(&message("m")).await,
            TaskOutcome::Retryable {
                reason: "connection failed".into(),
                retry_after: None,
            }
        );
    }

    #[test]
    fn rejects_hosts_outside_the_allowlist() {
        let mut value = settings("http://example.com/hook");
        value["allowed_hosts"] = json!(["billing.internal.example.com"]);
        assert!(build_error(value).contains("not in allowed_hosts"));
    }

    #[test]
    fn rejects_an_empty_allowlist() {
        let mut value = settings("http://127.0.0.1/hook");
        value["allowed_hosts"] = json!([]);
        assert!(build_error(value).contains("at least one host"));
    }

    #[test]
    fn host_matching_ignores_case() {
        let mut value = settings("http://Billing.Example.com/hook");
        value["allowed_hosts"] = json!(["billing.example.com"]);
        assert!(build(value).is_ok());
    }

    #[test]
    fn rejects_non_http_schemes() {
        assert!(build_error(settings("ftp://127.0.0.1/hook")).contains("http or https"));
    }

    #[test]
    fn rejects_urls_with_credentials() {
        let message = build_error(settings("http://user:pw@127.0.0.1/hook"));
        assert!(message.contains("credentials"));
        assert!(!message.contains("pw"));
    }

    #[test]
    fn rejects_invalid_header_names() {
        let mut value = settings("http://127.0.0.1/hook");
        value["headers"] = json!({"bad header": "x"});
        assert!(build_error(value).contains("header name"));
    }

    #[test]
    fn rejects_unknown_settings() {
        let mut value = settings("http://127.0.0.1/hook");
        value["timeout"] = json!("10s");
        assert!(build_error(value).contains("unknown field"));
    }

    #[test]
    fn debug_output_hides_headers() {
        let task =
            HttpTask::new(serde_json::from_value(settings("http://127.0.0.1/hook")).unwrap())
                .unwrap();
        let debug = format!("{task:?}");
        assert!(!debug.contains("secret-token"));
        assert!(debug.contains("127.0.0.1"));
    }
}
