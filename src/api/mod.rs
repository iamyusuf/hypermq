mod error;
mod json;
mod publish;

use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};

pub use error::ApiError;

use crate::broker::Publisher;
use crate::config::Role;

#[derive(Clone)]
pub struct AppState {
    pub role: Role,
    pub max_body_bytes: usize,
    pub publisher: Arc<dyn Publisher>,
}

pub fn router(state: AppState) -> Router {
    let mut router = Router::new().route("/health", get(health));
    if state.role != Role::Consumers {
        router = router.route("/publish", post(publish::publish));
    }
    router
        .fallback(|| async { ApiError::NotFound })
        .layer(DefaultBodyLimit::max(state.max_body_bytes))
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Json<Value> {
    Json(json!({ "status": "ok", "role": state.role.as_str() }))
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
}
