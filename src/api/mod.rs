mod error;

use axum::extract::{DefaultBodyLimit, State};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Value, json};

pub use error::ApiError;

use crate::config::Role;

#[derive(Clone)]
pub struct AppState {
    pub role: Role,
    pub max_body_bytes: usize,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .fallback(|| async { ApiError::NotFound })
        .layer(DefaultBodyLimit::max(state.max_body_bytes))
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Json<Value> {
    Json(json!({ "status": "ok", "role": state.role.as_str() }))
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;

    async fn get_json(path: &str) -> (StatusCode, Value) {
        let app = router(AppState {
            role: Role::Api,
            max_body_bytes: 1024,
        });
        let res = app
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = res.status();
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap())
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
}
