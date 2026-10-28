use axum::Json;
use axum::extract::{FromRequest, Request};
use serde::de::DeserializeOwned;

use super::ApiError;

/// `Json` that reports failures through the stable `ApiError` body.
pub struct ApiJson<T>(pub T);

impl<S, T> FromRequest<S> for ApiJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<Self, ApiError> {
        let Json(value) = Json::<T>::from_request(req, state).await?;
        Ok(ApiJson(value))
    }
}
