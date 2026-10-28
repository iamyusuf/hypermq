use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

/// Errors returned to HTTP clients; each variant maps to a stable status and error code.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("route not found")]
    NotFound,
}

impl ApiError {
    fn status(&self) -> StatusCode {
        match self {
            ApiError::NotFound => StatusCode::NOT_FOUND,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            ApiError::NotFound => "not_found",
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = json!({ "error": { "code": self.code(), "message": self.to_string() } });
        (self.status(), Json(body)).into_response()
    }
}
