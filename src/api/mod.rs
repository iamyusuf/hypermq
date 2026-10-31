mod auth;
mod error;
mod json;
mod publish;

use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, State};
use axum::http::header;
use axum::middleware::from_fn_with_state;
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, get, post};
use axum::{Json, Router};
use secrecy::SecretString;
use serde_json::{Value, json};

pub use error::ApiError;

use crate::broker::Publisher;
use crate::config::Role;
use crate::metrics::Metrics;

#[derive(Clone)]
pub struct AppState {
    pub role: Role,
    pub max_body_bytes: usize,
    pub publisher: Arc<dyn Publisher>,
    pub metrics: Arc<Metrics>,
    /// When set, `/publish` and `/metrics` require this bearer token. `/health` is never protected.
    pub auth: Option<Arc<SecretString>>,
}

pub fn router(state: AppState) -> Router {
    let mut router = Router::new()
        .route("/health", get(health))
        .route("/metrics", guarded(get(metrics), &state.auth));
    if state.role != Role::Consumers {
        router = router.route("/publish", guarded(post(publish::publish), &state.auth));
    }
    router
        .fallback(|| async { ApiError::NotFound })
        .layer(DefaultBodyLimit::max(state.max_body_bytes))
        .with_state(state)
}

/// Applies the bearer-token check to a route when a token is configured.
fn guarded(
    route: MethodRouter<AppState>,
    auth: &Option<Arc<SecretString>>,
) -> MethodRouter<AppState> {
    match auth {
        Some(token) => {
            route.route_layer(from_fn_with_state(Arc::clone(token), auth::require_bearer))
        }
        None => route,
    }
}

async fn health(State(state): State<AppState>) -> Json<Value> {
    Json(json!({ "status": "ok", "role": state.role.as_str() }))
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.metrics.render(),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;
    use crate::broker::{PublishError, PublishRequest, PublishTarget};

    #[derive(Default)]
    struct FakePublisher {
        published: Mutex<Vec<PublishRequest>>,
        fail_with: Mutex<Option<PublishError>>,
    }

    #[async_trait]
    impl Publisher for FakePublisher {
        async fn publish(&self, request: PublishRequest) -> Result<(), PublishError> {
            if let Some(err) = self.fail_with.lock().unwrap().take() {
                return Err(err);
            }
            self.published.lock().unwrap().push(request);
            Ok(())
        }
    }

    fn app(role: Role, publisher: Arc<FakePublisher>) -> Router {
        router(AppState {
            role,
            max_body_bytes: 1024,
            publisher,
            metrics: Arc::default(),
            auth: None,
        })
    }

    fn authed_app(publisher: Arc<FakePublisher>) -> Router {
        router(AppState {
            role: Role::Api,
            max_body_bytes: 1024,
            publisher,
            metrics: Arc::default(),
            auth: Some(Arc::new(SecretString::new("s3cret".into()))),
        })
    }

    async fn send(
        app: Router,
        request: Request<Body>,
    ) -> (StatusCode, axum::http::HeaderMap, Value) {
        let res = app.oneshot(request).await.unwrap();
        let status = res.status();
        let headers = res.headers().clone();
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        (status, headers, serde_json::from_slice(&bytes).unwrap())
    }

    fn post_publish(body: impl Into<Body>) -> Request<Body> {
        Request::post("/publish")
            .header(header::CONTENT_TYPE, "application/json")
            .body(body.into())
            .unwrap()
    }

    async fn get_json(path: &str) -> (StatusCode, Value) {
        let app = app(Role::Api, Arc::default());
        let (status, _, body) = send(app, Request::get(path).body(Body::empty()).unwrap()).await;
        (status, body)
    }

    #[tokio::test]
    async fn health_reports_ok_and_role() {
        let (status, body) = get_json("/health").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, json!({ "status": "ok", "role": "api" }));
    }

    #[tokio::test]
    async fn unknown_route_returns_stable_error() {
        let (status, body) = get_json("/nope").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], "not_found");
    }

    #[tokio::test]
    async fn publish_returns_message_id_and_reaches_publisher() {
        let publisher = Arc::new(FakePublisher::default());
        let body =
            json!({ "exchange": "orders", "routing_key": "order.created", "payload": { "id": 1 } });
        let (status, _, out) = send(
            app(Role::All, publisher.clone()),
            post_publish(body.to_string()),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        let published = publisher.published.lock().unwrap();
        assert_eq!(published.len(), 1);
        assert_eq!(
            published[0].target,
            PublishTarget::Exchange("orders".into())
        );
        assert_eq!(out["message_id"], published[0].message_id.as_str());
    }

    #[tokio::test]
    async fn invalid_requests_are_rejected_without_publishing() {
        let publisher = Arc::new(FakePublisher::default());
        let cases = [
            json!({ "payload": 1 }).to_string(),
            json!({ "queue": "a b", "payload": 1 }).to_string(),
            json!({ "queue": "q" }).to_string(),
            "not json".to_owned(),
        ];
        for case in cases {
            let (status, _, out) = send(
                app(Role::Api, publisher.clone()),
                post_publish(case.clone()),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{case}");
            assert_eq!(out["error"]["code"], "invalid_request", "{case}");
        }
        assert!(publisher.published.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn oversized_body_returns_stable_413() {
        let big = json!({ "queue": "q", "payload": "x".repeat(2048) }).to_string();
        let (status, _, out) = send(app(Role::Api, Arc::default()), post_publish(big)).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(out["error"]["code"], "payload_too_large");
    }

    #[tokio::test]
    async fn publisher_errors_map_to_stable_responses() {
        let cases = [
            (
                PublishError::Unavailable("down".into()),
                StatusCode::SERVICE_UNAVAILABLE,
                "broker_unavailable",
            ),
            (
                PublishError::Unroutable,
                StatusCode::UNPROCESSABLE_ENTITY,
                "unroutable",
            ),
            (
                PublishError::TargetNotFound,
                StatusCode::NOT_FOUND,
                "target_not_found",
            ),
            (
                PublishError::Rejected,
                StatusCode::BAD_GATEWAY,
                "publish_rejected",
            ),
        ];
        for (err, expected_status, code) in cases {
            let publisher = Arc::new(FakePublisher::default());
            *publisher.fail_with.lock().unwrap() = Some(err);
            let body = json!({ "queue": "q", "payload": 1 }).to_string();
            let (status, headers, out) = send(app(Role::Api, publisher), post_publish(body)).await;

            assert_eq!(status, expected_status);
            assert_eq!(out["error"]["code"], code);
            assert_eq!(
                headers.contains_key(header::RETRY_AFTER),
                code == "broker_unavailable"
            );
        }
    }

    #[tokio::test]
    async fn consumers_role_does_not_serve_publish() {
        let body = json!({ "queue": "q", "payload": 1 }).to_string();
        let (status, _, _) = send(app(Role::Consumers, Arc::default()), post_publish(body)).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    fn publish_with_authorization(value: Option<&str>) -> Request<Body> {
        let mut builder =
            Request::post("/publish").header(header::CONTENT_TYPE, "application/json");
        if let Some(value) = value {
            builder = builder.header(header::AUTHORIZATION, value);
        }
        builder
            .body(Body::from(
                json!({ "queue": "q", "payload": 1 }).to_string(),
            ))
            .unwrap()
    }

    #[tokio::test]
    async fn publish_requires_the_bearer_token_when_configured() {
        let cases = [
            (None, StatusCode::UNAUTHORIZED),
            (Some("Bearer wrong"), StatusCode::UNAUTHORIZED),
            (Some("Bearer s3cret-extra"), StatusCode::UNAUTHORIZED),
            (Some("Basic czNjcmV0"), StatusCode::UNAUTHORIZED),
            (Some("s3cret"), StatusCode::UNAUTHORIZED),
            (Some("bearer s3cret"), StatusCode::OK),
            (Some("Bearer s3cret"), StatusCode::OK),
        ];
        for (value, expected) in cases {
            let publisher = Arc::new(FakePublisher::default());
            let (status, headers, out) = send(
                authed_app(publisher.clone()),
                publish_with_authorization(value),
            )
            .await;

            assert_eq!(status, expected, "{value:?}");
            let published = publisher.published.lock().unwrap().len();
            if expected == StatusCode::UNAUTHORIZED {
                assert_eq!(out["error"]["code"], "unauthorized", "{value:?}");
                assert_eq!(
                    headers[header::WWW_AUTHENTICATE].to_str().unwrap(),
                    "Bearer"
                );
                assert_eq!(published, 0, "{value:?}");
            } else {
                assert_eq!(published, 1, "{value:?}");
            }
        }
    }

    #[tokio::test]
    async fn metrics_reports_publish_results() {
        let publisher = Arc::new(FakePublisher::default());
        let app = app(Role::Api, publisher.clone());
        send(
            app.clone(),
            post_publish(json!({ "queue": "q", "payload": 1 }).to_string()),
        )
        .await;
        *publisher.fail_with.lock().unwrap() = Some(PublishError::Rejected);
        send(
            app.clone(),
            post_publish(json!({ "queue": "q", "payload": 1 }).to_string()),
        )
        .await;

        let res = app
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers()[header::CONTENT_TYPE],
            "text/plain; version=0.0.4; charset=utf-8"
        );
        let text = String::from_utf8(res.into_body().collect().await.unwrap().to_bytes().to_vec())
            .unwrap();
        assert!(text.contains("hypermq_messages_published_total{result=\"accepted\"} 1"));
        assert!(text.contains("hypermq_messages_published_total{result=\"failed\"} 1"));
    }

    #[tokio::test]
    async fn metrics_requires_the_bearer_token_when_configured() {
        let res = authed_app(Arc::default())
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

        let res = authed_app(Arc::default())
            .oneshot(
                Request::get("/metrics")
                    .header(header::AUTHORIZATION, "Bearer s3cret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn health_stays_open_when_auth_is_configured() {
        let (status, _, body) = send(
            authed_app(Arc::default()),
            Request::get("/health").body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ok");
    }

    #[test]
    fn constant_time_eq_compares_bytes() {
        assert!(auth::constant_time_eq(b"abc", b"abc"));
        assert!(!auth::constant_time_eq(b"abc", b"abd"));
        assert!(!auth::constant_time_eq(b"abc", b"ab"));
        assert!(!auth::constant_time_eq(b"", b"x"));
    }
}
