//! Bearer-token check for endpoints that are protected when a token is configured.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::header;
use axum::middleware::Next;
use axum::response::Response;
use secrecy::{ExposeSecret, SecretString};

use super::ApiError;

/// Passes the request on only if its `Authorization` header carries the expected bearer token.
pub async fn require_bearer(
    State(expected): State<Arc<SecretString>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let presented = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' '))
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
        .map(|(_, token)| token.trim());

    match presented {
        Some(token) if constant_time_eq(token.as_bytes(), expected.expose_secret().as_bytes()) => {
            Ok(next.run(request).await)
        }
        _ => Err(ApiError::Unauthorized),
    }
}

/// Compares without stopping at the first mismatch. Unequal lengths return early, which only
/// reveals the token length.
pub(super) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}
