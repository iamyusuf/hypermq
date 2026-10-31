use std::collections::BTreeMap;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use bytes::Bytes;
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use super::json::ApiJson;
use super::{ApiError, AppState};
use crate::broker::{
    ATTEMPT_HEADER, Accepted, MAX_NAME_LEN, MAX_ROUTING_KEY_LEN, PublishRequest, PublishTarget,
    is_valid_name, is_valid_routing_key,
};

const MAX_MESSAGE_ID_LEN: usize = 255;
const MAX_HEADERS: usize = 32;
const MAX_HEADER_KEY_LEN: usize = 128;
const MAX_HEADER_VALUE_LEN: usize = 1024;

/// Request body of `POST /publish`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishBody {
    exchange: Option<String>,
    queue: Option<String>,
    #[serde(default)]
    routing_key: String,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    message_id: Option<String>,
    payload: Value,
}

/// `200 OK` when the broker confirmed the message, `202 Accepted` when it was stored in the outbox.
pub async fn publish(
    State(state): State<AppState>,
    ApiJson(body): ApiJson<PublishBody>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let request = body.into_request()?;
    let message_id = request.message_id.clone();

    let result = state.publisher.publish(request).await;
    state.metrics.record_publish(result.is_ok());
    let accepted = result.map_err(|err| {
        tracing::warn!(%err, %message_id, "publish failed");
        ApiError::from(err)
    })?;

    let status = match accepted {
        Accepted::Published => StatusCode::OK,
        Accepted::Queued => StatusCode::ACCEPTED,
    };
    Ok((status, Json(json!({ "message_id": message_id }))))
}

impl PublishBody {
    pub fn into_request(self) -> Result<PublishRequest, ApiError> {
        let target = match (self.exchange, self.queue) {
            (Some(name), None) => {
                check_name("exchange", &name)?;
                check_routing_key(&self.routing_key)?;
                PublishTarget::Exchange(name)
            }
            (None, Some(name)) => {
                check_name("queue", &name)?;
                if !self.routing_key.is_empty() {
                    return invalid("`routing_key` is not allowed with `queue`");
                }
                PublishTarget::Queue(name)
            }
            _ => return invalid("exactly one of `exchange` or `queue` is required"),
        };

        check_headers(&self.headers)?;

        let message_id = match self.message_id {
            Some(id) => {
                check_message_id(&id)?;
                id
            }
            None => Uuid::new_v4().to_string(),
        };

        let body = serde_json::to_vec(&self.payload)
            .map_err(|_| ApiError::InvalidRequest("`payload` cannot be serialized".into()))?;

        Ok(PublishRequest {
            target,
            routing_key: self.routing_key,
            message_id,
            content_type: "application/json".into(),
            headers: self.headers,
            body: Bytes::from(body),
        })
    }
}

fn invalid<T>(message: &str) -> Result<T, ApiError> {
    Err(ApiError::InvalidRequest(message.into()))
}

/// Exchange and queue names: see `broker::is_valid_name`.
fn check_name(field: &str, name: &str) -> Result<(), ApiError> {
    if is_valid_name(name) {
        Ok(())
    } else {
        Err(ApiError::InvalidRequest(format!(
            "`{field}` must be 1-{MAX_NAME_LEN} characters of letters, digits, '-', '_', '.' or ':'"
        )))
    }
}

fn check_routing_key(key: &str) -> Result<(), ApiError> {
    if is_valid_routing_key(key) {
        Ok(())
    } else {
        Err(ApiError::InvalidRequest(format!(
            "`routing_key` must be at most {MAX_ROUTING_KEY_LEN} bytes without control characters"
        )))
    }
}

fn check_message_id(id: &str) -> Result<(), ApiError> {
    if !id.is_empty() && id.len() <= MAX_MESSAGE_ID_LEN && id.bytes().all(|b| b.is_ascii_graphic())
    {
        Ok(())
    } else {
        Err(ApiError::InvalidRequest(format!(
            "`message_id` must be 1-{MAX_MESSAGE_ID_LEN} visible ASCII characters"
        )))
    }
}

fn check_headers(headers: &BTreeMap<String, String>) -> Result<(), ApiError> {
    if headers.len() > MAX_HEADERS {
        return Err(ApiError::InvalidRequest(format!(
            "at most {MAX_HEADERS} headers are allowed"
        )));
    }
    for (key, value) in headers {
        let key_ok = !key.is_empty()
            && key.len() <= MAX_HEADER_KEY_LEN
            && key.bytes().all(|b| b.is_ascii_graphic());
        if !key_ok {
            return Err(ApiError::InvalidRequest(format!(
                "header names must be 1-{MAX_HEADER_KEY_LEN} visible ASCII characters"
            )));
        }
        if key.eq_ignore_ascii_case(ATTEMPT_HEADER) {
            return Err(ApiError::InvalidRequest(format!(
                "header `{ATTEMPT_HEADER}` is reserved"
            )));
        }
        if value.len() > MAX_HEADER_VALUE_LEN {
            return Err(ApiError::InvalidRequest(format!(
                "header values must be at most {MAX_HEADER_VALUE_LEN} bytes"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(json: Value) -> PublishBody {
        serde_json::from_value(json).unwrap()
    }

    fn invalid_message(json: Value) -> String {
        match body(json).into_request().unwrap_err() {
            ApiError::InvalidRequest(msg) => msg,
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn exchange_request_is_accepted() {
        let req = body(json!({
            "exchange": "orders",
            "routing_key": "order.created",
            "headers": { "source": "web" },
            "message_id": "abc-1",
            "payload": { "id": 7 }
        }))
        .into_request()
        .unwrap();

        assert_eq!(req.target, PublishTarget::Exchange("orders".into()));
        assert_eq!(req.routing_key, "order.created");
        assert_eq!(req.message_id, "abc-1");
        assert_eq!(req.headers["source"], "web");
        assert_eq!(req.body.as_ref(), br#"{"id":7}"#);
    }

    #[test]
    fn queue_request_generates_message_id() {
        let req = body(json!({ "queue": "jobs", "payload": null }))
            .into_request()
            .unwrap();
        assert_eq!(req.target, PublishTarget::Queue("jobs".into()));
        assert!(Uuid::parse_str(&req.message_id).is_ok());
    }

    #[test]
    fn exactly_one_target_is_required() {
        assert!(invalid_message(json!({ "payload": 1 })).contains("exactly one"));
        let both = json!({ "exchange": "a", "queue": "b", "payload": 1 });
        assert!(invalid_message(both).contains("exactly one"));
    }

    #[test]
    fn routing_key_is_rejected_with_queue() {
        let msg = invalid_message(json!({ "queue": "q", "routing_key": "x", "payload": 1 }));
        assert!(msg.contains("routing_key"));
    }

    #[test]
    fn bad_names_are_rejected() {
        for name in ["", "has space", "a/b", "ünï", &"x".repeat(256)] {
            let msg = invalid_message(json!({ "exchange": name, "payload": 1 }));
            assert!(msg.contains("`exchange`"), "{name:?}");
        }
    }

    #[test]
    fn routing_key_bounds() {
        let long = "k".repeat(256);
        assert!(
            invalid_message(json!({ "exchange": "e", "routing_key": long, "payload": 1 }))
                .contains("routing_key")
        );
        assert!(
            invalid_message(json!({ "exchange": "e", "routing_key": "a\nb", "payload": 1 }))
                .contains("routing_key")
        );
    }

    #[test]
    fn message_id_bounds() {
        for id in ["", "has space", &"i".repeat(256)] {
            let msg = invalid_message(json!({ "queue": "q", "message_id": id, "payload": 1 }));
            assert!(msg.contains("message_id"), "{id:?}");
        }
    }

    #[test]
    fn header_bounds() {
        let too_many: BTreeMap<String, String> =
            (0..33).map(|i| (format!("h{i}"), "v".to_owned())).collect();
        let msg = invalid_message(json!({ "queue": "q", "headers": too_many, "payload": 1 }));
        assert!(msg.contains("headers"));

        let long_value = "v".repeat(1025);
        let msg =
            invalid_message(json!({ "queue": "q", "headers": { "k": long_value }, "payload": 1 }));
        assert!(msg.contains("header values"));

        let msg = invalid_message(json!({ "queue": "q", "headers": { "": "v" }, "payload": 1 }));
        assert!(msg.contains("header names"));
    }

    #[test]
    fn reserved_attempt_header_is_rejected() {
        let msg = invalid_message(
            json!({ "queue": "q", "headers": { "X-HyperMQ-Attempt": "9" }, "payload": 1 }),
        );
        assert!(msg.contains("reserved"));
    }

    #[test]
    fn unknown_fields_and_missing_payload_fail_to_deserialize() {
        assert!(
            serde_json::from_value::<PublishBody>(json!({ "queue": "q", "payload": 1, "x": 1 }))
                .is_err()
        );
        assert!(serde_json::from_value::<PublishBody>(json!({ "queue": "q" })).is_err());
    }
}
