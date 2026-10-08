//! Bounded asynchronous client for Forge command HTTP requests.
//!
//! Each call is one loopback-only HTTP/1 exchange. The client owns no catalog,
//! connection pool, retry loop, redirect policy, or background task.

use std::{fmt, net::SocketAddr, sync::Arc, time::Duration};

use forge_backend::ops::CommandOp;
use forge_backend::{ForgeHttpClientConfigError, ForgeHttpClientError, ForgeHttpServiceCode};
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Bytes;
use hyper::header::{
    AUTHORIZATION, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HOST, HeaderMap, HeaderValue,
};
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::Semaphore;
use uuid::Uuid;

const COMMANDS_PATH_PREFIX: &str = "/api/v1/commands/";
const MAX_HEADER_BYTES: usize = 8 * 1024;
const MAX_HEADER_FIELDS: usize = 16;
const MAX_REQUEST_BYTES: usize = 64 * 1024;
const MAX_RESPONSE_BYTES: usize = forge_backend::MAX_BODY_BYTES;
const TRANSPORT_CONCURRENCY: usize = 4;

/// One bounded one-shot Forge command transport. The bearer is always marked
/// sensitive and omitted from Debug output.
#[derive(Clone)]
pub struct ForgeCommandHttpClient {
    authority: SocketAddr,
    service_bearer: HeaderValue,
    timeout: Duration,
    slots: Arc<Semaphore>,
}

impl fmt::Debug for ForgeCommandHttpClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ForgeCommandHttpClient")
            .field("authority", &self.authority)
            .field("timeout", &self.timeout)
            .field("service_bearer_configured", &true)
            .finish_non_exhaustive()
    }
}

impl ForgeCommandHttpClient {
    /// Build a loopback-only command client.
    ///
    /// # Errors
    /// Rejects non-loopback or zero-port authorities and empty or invalid
    /// bearer credentials.
    pub fn new(
        authority: SocketAddr,
        service_secret: &str,
        timeout: Duration,
    ) -> Result<Self, ForgeHttpClientConfigError> {
        if !authority.ip().is_loopback() {
            return Err(ForgeHttpClientConfigError::NonLoopbackAuthority);
        }
        if authority.port() == 0 {
            return Err(ForgeHttpClientConfigError::MissingAuthorityPort);
        }
        if service_secret.is_empty() {
            return Err(ForgeHttpClientConfigError::EmptyServiceBearer);
        }
        let mut service_bearer = HeaderValue::from_str(&format!("Bearer {service_secret}"))
            .map_err(|_| ForgeHttpClientConfigError::InvalidServiceBearer)?;
        service_bearer.set_sensitive(true);
        Ok(Self {
            authority,
            service_bearer,
            timeout,
            slots: Arc::new(Semaphore::new(TRANSPORT_CONCURRENCY)),
        })
    }

    /// Send one bounded Forge command envelope and return its readback.
    /// Request identity, protocol version, and framing are checked before the
    /// response payload is decoded. No command is retried automatically.
    ///
    /// # Errors
    /// Returns a typed error for request/admission failures, transport or
    /// deadline failures, invalid responses, authentication failures, and
    /// Forge service rejections.
    pub async fn command(
        &self,
        op: CommandOp,
        payload: Value,
    ) -> Result<Value, ForgeHttpClientError> {
        let _permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| ForgeHttpClientError::Capacity)?;
        let request_id = Uuid::now_v7();
        let envelope = json!({
            "forge_api": forge_backend::FORGE_API_VERSION,
            "request_id": request_id,
            "op": op.as_str(),
            "payload": payload,
        });
        let body = serde_json::to_vec(&envelope).map_err(|_| ForgeHttpClientError::Request)?;
        if body.len() > MAX_REQUEST_BYTES {
            return Err(ForgeHttpClientError::RequestTooLarge);
        }
        let (status, _headers, body) = tokio::time::timeout(self.timeout, self.exchange(op, body))
            .await
            .map_err(|_| ForgeHttpClientError::Timeout)??;
        decode_reply(request_id, status, &body, op)
    }

    /// Send one bounded Forge command envelope and return its readback with
    /// the failure's bounded wire facts preserved (peer text is never kept).
    /// The caller classifies control categories that the collapsed
    /// `ForgeHttpServiceCode` vocabulary cannot distinguish — for example an
    /// absent `diff.read` subject arriving as `FORGE_UNAVAILABLE` with the
    /// `evidence` category.
    ///
    /// # Errors
    /// Identical admission/transport/deadline/protocol behavior to
    /// [`Self::command`]; failures return [`ForgeCommandFailure`].
    pub async fn command_classified(
        &self,
        op: CommandOp,
        payload: Value,
    ) -> Result<Value, ForgeCommandFailure> {
        let _permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| ForgeCommandFailure::capacity())?;
        let request_id = Uuid::now_v7();
        let envelope = json!({
            "forge_api": forge_backend::FORGE_API_VERSION,
            "request_id": request_id,
            "op": op.as_str(),
            "payload": payload,
        });
        let body = serde_json::to_vec(&envelope).map_err(|_| ForgeCommandFailure::unavailable())?;
        if body.len() > MAX_REQUEST_BYTES {
            return Err(ForgeCommandFailure::unavailable());
        }
        let (status, _headers, body) = tokio::time::timeout(self.timeout, self.exchange(op, body))
            .await
            .map_err(|_| ForgeCommandFailure::unavailable())?
            .map_err(ForgeCommandFailure::from_exchange)?;
        decode_classified(request_id, status, &body)
    }

    async fn exchange(
        &self,
        op: CommandOp,
        body: Vec<u8>,
    ) -> Result<(StatusCode, HeaderMap, Bytes), ForgeHttpClientError> {
        let stream = tokio::net::TcpStream::connect(self.authority)
            .await
            .map_err(|_| ForgeHttpClientError::Transport)?;
        let mut builder = hyper::client::conn::http1::Builder::new();
        builder
            .max_headers(MAX_HEADER_FIELDS)
            .max_buf_size(MAX_HEADER_BYTES);
        let (mut sender, connection) = builder
            .handshake::<_, Full<Bytes>>(TokioIo::new(stream))
            .await
            .map_err(|_| ForgeHttpClientError::Transport)?;
        let request = Request::builder()
            .method(Method::POST)
            .uri(format!("{COMMANDS_PATH_PREFIX}{}", op.as_str()))
            .header(HOST, self.authority.to_string())
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, body.len().to_string())
            .header(CONNECTION, "close")
            .header(AUTHORIZATION, self.service_bearer.clone())
            .body(Full::new(Bytes::from(body)))
            .map_err(|_| ForgeHttpClientError::Request)?;
        if request.headers().len() > MAX_HEADER_FIELDS {
            return Err(ForgeHttpClientError::Request);
        }
        let exchange = async move {
            let response = sender
                .send_request(request)
                .await
                .map_err(|_| ForgeHttpClientError::Transport)?;
            let status = response.status();
            let headers = response.headers();
            if headers.len() > MAX_HEADER_FIELDS
                || header_bytes(headers) > MAX_HEADER_BYTES
                || !json_content_type(headers)
            {
                return Err(ForgeHttpClientError::Protocol);
            }
            let headers = headers.clone();
            let body = Limited::new(response.into_body(), MAX_RESPONSE_BYTES)
                .collect()
                .await
                .map_err(|_| ForgeHttpClientError::ResponseTooLarge)?
                .to_bytes();
            Ok::<_, ForgeHttpClientError>((status, headers, body))
        };
        let mut connection = Box::pin(connection);
        let mut exchange = Box::pin(exchange);
        tokio::select! {
            result = &mut exchange => result,
            _connection_result = &mut connection => exchange.await,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SuccessReply {
    forge_api: u32,
    request_id: Uuid,
    ok: bool,
    result: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FailureError {
    code: String,
    #[serde(rename = "message")]
    _message: String,
    category: String,
    retryable: bool,
    recovery_action: String,
    #[serde(rename = "recovery_hint")]
    _recovery_hint: String,
    #[serde(
        rename = "readback",
        deserialize_with = "deserialize_required_readback"
    )]
    readback: Option<Value>,
}

fn deserialize_required_readback<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<Value>::deserialize(deserializer)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FailureReply {
    forge_api: u32,
    request_id: Uuid,
    ok: bool,
    error: FailureError,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum CommandReply {
    Success(SuccessReply),
    Failure(FailureReply),
}

/// Bounded wire facts of one Forge command failure. Peer text is never
/// retained; the collapsed wire code and the control category are kept so
/// source classifiers can separate families the wire vocabulary collapses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeCommandFailure {
    pub status: u16,
    pub wire_code: ForgeHttpServiceCode,
    pub category: String,
    pub retryable: bool,
}

impl ForgeCommandFailure {
    fn unavailable() -> Self {
        Self {
            status: 503,
            wire_code: ForgeHttpServiceCode::ForgeUnavailable,
            category: String::new(),
            retryable: true,
        }
    }

    fn capacity() -> Self {
        Self {
            status: 503,
            wire_code: ForgeHttpServiceCode::CapacityExceeded,
            category: String::new(),
            retryable: true,
        }
    }

    fn from_exchange(error: ForgeHttpClientError) -> Self {
        match error {
            ForgeHttpClientError::Capacity => Self::capacity(),
            ForgeHttpClientError::Timeout | ForgeHttpClientError::Transport => Self::unavailable(),
            ForgeHttpClientError::Request
            | ForgeHttpClientError::RequestTooLarge
            | ForgeHttpClientError::Protocol
            | ForgeHttpClientError::ResponseTooLarge
            | ForgeHttpClientError::ResponseHeadersTooLarge => Self::protocol(),
            // Admission/auth rejections are re-raised through the typed
            // surface below; the exchange only produces transport-class
            // errors otherwise.
            other => Self {
                status: 503,
                wire_code: match other {
                    ForgeHttpClientError::Service { code, .. } => code,
                    _ => ForgeHttpServiceCode::ForgeUnavailable,
                },
                category: String::new(),
                retryable: true,
            },
        }
    }

    fn protocol() -> Self {
        Self {
            status: 502,
            wire_code: ForgeHttpServiceCode::Unknown,
            category: String::new(),
            retryable: false,
        }
    }
}

fn decode_classified(
    request_id: Uuid,
    status: StatusCode,
    body: &[u8],
) -> Result<Value, ForgeCommandFailure> {
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        return Err(ForgeCommandFailure {
            status: status.as_u16(),
            wire_code: ForgeHttpServiceCode::AuthFailed,
            category: String::new(),
            retryable: false,
        });
    }
    let reply: CommandReply =
        serde_json::from_slice(body).map_err(|_| ForgeCommandFailure::protocol())?;
    match reply {
        CommandReply::Success(reply) => {
            if status != StatusCode::OK
                || reply.forge_api != forge_backend::FORGE_API_VERSION
                || reply.request_id != request_id
                || !reply.ok
            {
                return Err(ForgeCommandFailure::protocol());
            }
            Ok(reply.result)
        }
        CommandReply::Failure(reply) => {
            if status == StatusCode::OK
                || reply.forge_api != forge_backend::FORGE_API_VERSION
                || reply.request_id != request_id
                || reply.ok
            {
                return Err(ForgeCommandFailure::protocol());
            }
            let wire_code = ForgeHttpServiceCode::from_wire_code(reply.error.code.as_str());
            if status == StatusCode::UNAUTHORIZED
                || status == StatusCode::FORBIDDEN
                || matches!(wire_code, ForgeHttpServiceCode::AuthFailed)
            {
                return Err(ForgeCommandFailure {
                    status: status.as_u16(),
                    wire_code: ForgeHttpServiceCode::AuthFailed,
                    category: String::new(),
                    retryable: false,
                });
            }
            Err(ForgeCommandFailure {
                status: status.as_u16(),
                wire_code,
                category: reply.error.category.clone(),
                retryable: reply.error.retryable,
            })
        }
    }
}

fn decode_reply(
    request_id: Uuid,
    status: StatusCode,
    body: &[u8],
    op: CommandOp,
) -> Result<Value, ForgeHttpClientError> {
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        return Err(ForgeHttpClientError::Authentication);
    }
    let reply: CommandReply =
        serde_json::from_slice(body).map_err(|_| ForgeHttpClientError::Protocol)?;
    match reply {
        CommandReply::Success(reply) => {
            if status != StatusCode::OK
                || reply.forge_api != forge_backend::FORGE_API_VERSION
                || reply.request_id != request_id
                || !reply.ok
            {
                return Err(ForgeHttpClientError::Protocol);
            }
            Ok(reply.result)
        }
        CommandReply::Failure(reply) => {
            if status == StatusCode::OK
                || reply.forge_api != forge_backend::FORGE_API_VERSION
                || reply.request_id != request_id
                || reply.ok
            {
                return Err(ForgeHttpClientError::Protocol);
            }
            let mut code = ForgeHttpServiceCode::from_wire_code(reply.error.code.as_str());
            if status == StatusCode::UNAUTHORIZED
                || status == StatusCode::FORBIDDEN
                || matches!(code, ForgeHttpServiceCode::AuthFailed)
            {
                return Err(ForgeHttpClientError::Authentication);
            }
            // Forge intentionally folds Agent conflicts into its unavailable
            // bucket, but retains the typed control category. Interpret that
            // known rejection only for departure; never collapse readback or
            // retryable uncertainty, and retain the actual peer HTTP status.
            if matches!(
                op,
                CommandOp::AgentDepartureReconcile | CommandOp::AgentDepartureReconciliationGet
            ) && code == ForgeHttpServiceCode::ForgeUnavailable
                && reply.error.category == "revision_conflict"
                && !reply.error.retryable
                && reply.error.recovery_action != "read_back"
                && reply.error.readback.is_none()
            {
                code = ForgeHttpServiceCode::StaleRevision;
            }
            Err(ForgeHttpClientError::Service {
                status: status.as_u16(),
                code,
            })
        }
    }
}

fn header_bytes(headers: &HeaderMap) -> usize {
    headers.iter().fold(0_usize, |total, (name, value)| {
        total
            .saturating_add(name.as_str().len())
            .saturating_add(value.as_bytes().len())
            .saturating_add(4)
    })
}

fn json_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
}

#[cfg(test)]
mod tests;
