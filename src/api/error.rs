use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::broker::PublishError;

const RETRY_AFTER_SECS: &str = "5";

/// Errors returned to HTTP clients; each variant maps to a stable status and error code.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("route not found")]
    NotFound,
    #[error("{0}")]
    InvalidRequest(String),
    #[error("request body is too large")]
    PayloadTooLarge,
    #[error("exchange does not exist")]
    TargetNotFound,
    #[error("message was not routed to any queue")]
    Unroutable,
    #[error("message broker is unavailable")]
    BrokerUnavailable,
    #[error("message broker rejected the message")]
    PublishRejected,
}

impl ApiError {
    fn status(&self) -> StatusCode {
        match self {
            ApiError::NotFound | ApiError::TargetNotFound => StatusCode::NOT_FOUND,
            ApiError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            ApiError::Unroutable => StatusCode::UNPROCESSABLE_ENTITY,
            ApiError::BrokerUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            ApiError::PublishRejected => StatusCode::BAD_GATEWAY,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            ApiError::NotFound => "not_found",
            ApiError::InvalidRequest(_) => "invalid_request",
            ApiError::PayloadTooLarge => "payload_too_large",
            ApiError::TargetNotFound => "target_not_found",
            ApiError::Unroutable => "unroutable",
            ApiError::BrokerUnavailable => "broker_unavailable",
            ApiError::PublishRejected => "publish_rejected",
        }
    }
}

impl From<PublishError> for ApiError {
    fn from(err: PublishError) -> Self {
        match err {
            PublishError::Unavailable(_) => ApiError::BrokerUnavailable,
            PublishError::Unroutable => ApiError::Unroutable,
            PublishError::TargetNotFound => ApiError::TargetNotFound,
            PublishError::Rejected => ApiError::PublishRejected,
        }
    }
}

impl From<JsonRejection> for ApiError {
    fn from(rejection: JsonRejection) -> Self {
        if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
            ApiError::PayloadTooLarge
        } else {
            ApiError::InvalidRequest(rejection.body_text())
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = json!({ "error": { "code": self.code(), "message": self.to_string() } });
        let mut response = (self.status(), Json(body)).into_response();
        if matches!(self, ApiError::BrokerUnavailable) {
            response.headers_mut().insert(
                header::RETRY_AFTER,
                HeaderValue::from_static(RETRY_AFTER_SECS),
            );
        }
        response
    }
}
