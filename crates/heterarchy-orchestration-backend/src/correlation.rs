//! Per-request correlation identifiers for the HTTP surface.
//!
//! One identifier is minted at the outermost transport layer and travels
//! three ways: request extensions (handlers), the `tracing` request span
//! (structured logs), and the `X-Request-Id` response header (callers).
//! Externally supplied identifiers are never trusted for identity — the
//! minted identifier is the only correlation authority.

use std::time::Instant;

use axum::extract::Request;
use axum::http::{HeaderName, HeaderValue};
use axum::middleware::Next;
use axum::response::Response;
use tracing::Instrument;
use uuid::Uuid;

/// Response header carrying the minted request correlation identifier.
pub(crate) const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

/// Request-scoped correlation identifier minted per accepted request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RequestId(Uuid);

impl RequestId {
    fn mint() -> Self {
        Self(Uuid::now_v7())
    }
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Mint one identifier, admit the request into its correlation span, and
/// stamp the identifier on the response.
pub(crate) async fn middleware(request: Request, next: Next) -> Response {
    let request_id = RequestId::mint();
    let started = Instant::now();
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let mut request = request;
    request.extensions_mut().insert(request_id);
    let span = tracing::info_span!(
        "http_request",
        request_id = %request_id,
        http_method = %method,
        http_path = %path,
    );
    let response = next.run(request).instrument(span.clone()).await;
    let status = response.status().as_u16();
    tracing::info!(
        parent: &span,
        http_status = status,
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "request completed"
    );
    // Uuid Display output is visible ASCII, so this conversion cannot fail
    // for any identifier this module mints; the invariant does not depend on
    // request data.
    #[allow(clippy::expect_used)]
    let value = HeaderValue::from_str(&request_id.to_string())
        .expect("Uuid Display is always a valid header value");
    let mut response = response;
    response.headers_mut().insert(REQUEST_ID_HEADER, value);
    response
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::Extension;
    use axum::http::StatusCode;
    use axum::routing::get;
    use axum::{Json, Router};
    use serde_json::Value;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    use std::future::IntoFuture as _;

    /// The production stack ordering under test: the correlation layer wraps
    /// every route, including routes that fail (401) before any handler work.
    fn probe_router() -> Router {
        Router::new()
            .route("/probe", get(probe))
            .route("/deny", get(denied))
            .layer(axum::middleware::from_fn(middleware))
    }

    async fn probe(Extension(request_id): Extension<RequestId>) -> Json<Value> {
        Json(serde_json::json!({ "requestId": request_id.to_string() }))
    }

    async fn denied() -> StatusCode {
        StatusCode::UNAUTHORIZED
    }

    fn loopback_network_is_unavailable() -> bool {
        std::env::var(forge_control::RUST_CI_SANDBOX_MARKER_ENV).as_deref()
            == Ok(forge_control::RUST_CI_SANDBOX_MARKER_VALUE)
    }

    async fn serve_probe() -> Result<std::net::SocketAddr, Box<dyn std::error::Error>> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(axum::serve(listener, probe_router()).into_future());
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(address).await.is_ok() {
                return Ok(address);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Err("probe listener never became ready")?
    }

    async fn raw_request(
        address: std::net::SocketAddr,
        target: &str,
    ) -> Result<(u16, String), Box<dyn std::error::Error>> {
        let mut stream = tokio::net::TcpStream::connect(address).await?;
        stream
            .write_all(
                format!("GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await?;
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await?;
        let text = String::from_utf8_lossy(&raw).into_owned();
        let status: u16 = text
            .split_whitespace()
            .nth(1)
            .ok_or("probe response has no status line")?
            .parse()
            .map_err(|error| format!("probe status line is malformed: {error}"))?;
        Ok((status, text))
    }

    fn response_header(raw: &str, name: &str) -> Option<String> {
        raw.lines()
            .find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.trim()
                    .eq_ignore_ascii_case(name)
                    .then(|| value.trim().to_owned())
            })
            .filter(|value| !value.is_empty())
    }

    #[tokio::test]
    async fn every_response_carries_a_distinct_request_identifier_matching_the_extension()
    -> Result<(), Box<dyn std::error::Error>> {
        if loopback_network_is_unavailable() {
            return Ok(());
        }
        let address = serve_probe().await?;
        let (first_status, first_raw) = raw_request(address, "/probe").await?;
        let (second_status, second_raw) = raw_request(address, "/probe").await?;
        assert_eq!(first_status, 200);
        assert_eq!(second_status, 200);
        let first_id = response_header(&first_raw, "x-request-id")
            .ok_or("first response is missing x-request-id")?;
        let second_id = response_header(&second_raw, "x-request-id")
            .ok_or("second response is missing x-request-id")?;
        assert!(!first_id.is_empty());
        assert_ne!(first_id, second_id, "identifiers must be per-request");
        let body: Value = serde_json::from_str(
            first_raw
                .split_once("\r\n\r\n")
                .map(|(_, body)| body)
                .ok_or("probe response has no body")?,
        )?;
        assert_eq!(
            body.get("requestId").and_then(Value::as_str),
            Some(first_id.as_str()),
            "extension identifier and header identifier must agree"
        );
        Ok(())
    }

    #[tokio::test]
    async fn failed_responses_are_stamped_by_the_outermost_correlation_layer()
    -> Result<(), Box<dyn std::error::Error>> {
        if loopback_network_is_unavailable() {
            return Ok(());
        }
        let address = serve_probe().await?;
        let (status, raw) = raw_request(address, "/deny").await?;
        assert_eq!(status, 401);
        let id = response_header(&raw, "x-request-id")
            .ok_or("denied response is missing x-request-id")?;
        assert!(!id.is_empty());
        Ok(())
    }
}
