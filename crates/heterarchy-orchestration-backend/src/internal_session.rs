//! Loopback-only internal Agent-session resolution server.
//!
//! This transport exposes only the Orchestration-owned session authority over
//! the shared Forge session-resolution wire. It has no public MCP, OAuth,
//! origin, or fallback-catalog route.

use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::Response;
use axum::routing::post;
use forge_backend::IssuerAuth;
use forge_backend::session_protocol::{RemoteSessionResolveReply, RemoteSessionResolveRequest};
use forge_control::AgentSessionAuthority;
use forge_domain::RegisteredAgentSession;
use heterarchy_orchestration_sessions::SessionCatalogAuthority;
use thiserror::Error;
use tokio::sync::{Mutex, Semaphore, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// The only route owned by this server.
pub const INTERNAL_SESSION_RESOLVE_PATH: &str = "/internal/v1/sessions/resolve";
/// Bound for one request body and one serialized reply.
pub const INTERNAL_SESSION_MAX_BODY_BYTES: usize = 4 * 1024;
/// Maximum number of synchronous catalog reads admitted at once.
pub const INTERNAL_SESSION_QUERY_CONCURRENCY: usize = 4;

const JSON_CONTENT_TYPE: &str = "application/json";
const STATIC_UNAUTHORIZED: &str = "unauthorized";
const STATIC_ORIGIN_REJECTED: &str = "origin is not accepted";
const STATIC_INVALID_REQUEST: &str = "invalid session request";
const STATIC_UNAVAILABLE: &str = "session authority unavailable";
const STATIC_CAPACITY: &str = "session resolver unavailable";
const STATIC_INTERNAL: &str = "session resolver response unavailable";

/// Configuration for one loopback internal session listener.
#[must_use]
#[derive(Clone)]
pub struct InternalSessionServerConfig {
    address: SocketAddr,
    issuer: IssuerAuth,
}

impl fmt::Debug for InternalSessionServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InternalSessionServerConfig")
            .field("address", &self.address)
            .field("secret_configured", &true)
            .finish_non_exhaustive()
    }
}

impl InternalSessionServerConfig {
    /// Bind one loopback address and retain only the issuer's secret digest.
    ///
    /// # Errors
    /// Returns an error for non-loopback addresses or an empty bearer secret.
    pub fn new(
        address: SocketAddr,
        bearer_secret: impl AsRef<str>,
    ) -> Result<Self, InternalSessionServerConfigError> {
        if !address.ip().is_loopback() {
            return Err(InternalSessionServerConfigError::NonLoopbackAddress);
        }
        let bearer_secret = bearer_secret.as_ref();
        if bearer_secret.is_empty() {
            return Err(InternalSessionServerConfigError::EmptyBearerSecret);
        }
        Ok(Self {
            address,
            issuer: IssuerAuth::new(bearer_secret),
        })
    }

    #[must_use]
    pub const fn address(&self) -> SocketAddr {
        self.address
    }
}

/// Configuration failures are redacted and deterministic.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum InternalSessionServerConfigError {
    #[error("internal session server address must be loopback")]
    NonLoopbackAddress,
    #[error("internal session server bearer secret must not be empty")]
    EmptyBearerSecret,
}

/// Listener join state retained across repeated shutdown attempts.
#[must_use]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum InternalSessionServerJoinState {
    /// The listener task has not been committed to the handle.
    #[default]
    NotStarted,
    /// The listener task exited cleanly.
    Joined,
    /// The listener or one of its owned tasks is still retained for retry.
    Pending,
    /// The listener task exited with an error or join failure.
    Failed,
}

/// Bounded shutdown observation. A pending report keeps the server handle's
/// join task available for a later shutdown call.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InternalSessionServerShutdownReport {
    pending_queries: usize,
    server: InternalSessionServerJoinState,
}

impl InternalSessionServerShutdownReport {
    #[must_use]
    pub const fn pending_queries(self) -> usize {
        self.pending_queries
    }

    pub const fn server(self) -> InternalSessionServerJoinState {
        self.server
    }

    #[must_use]
    pub fn is_complete(self) -> bool {
        self.pending_queries == 0 && self.server != InternalSessionServerJoinState::Pending
    }

    #[must_use]
    pub fn is_successful(self) -> bool {
        self.pending_queries == 0 && self.server == InternalSessionServerJoinState::Joined
    }
}

/// Errors that prevent the listener from starting.
#[derive(Debug, Error)]
pub enum InternalSessionServerError {
    #[error("failed to bind internal session listener")]
    Bind(#[source] io::Error),
}

struct InternalSessionServerState {
    authority: Arc<SessionCatalogAuthority>,
    issuer: IssuerAuth,
    admission: Mutex<()>,
    accepting: AtomicBool,
    queries: TaskTracker,
    query_slots: Arc<Semaphore>,
    cancellation: CancellationToken,
}

impl fmt::Debug for InternalSessionServerState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InternalSessionServerState")
            .field("accepting", &self.accepting.load(Ordering::Acquire))
            .field("pending_queries", &self.queries.len())
            .finish_non_exhaustive()
    }
}

impl InternalSessionServerState {
    async fn stop_admission(&self) {
        let _admission = self.admission.lock().await;
        self.accepting.store(false, Ordering::Release);
        self.cancellation.cancel();
        self.queries.close();
    }
}

/// Owned loopback listener and all query tasks admitted through it.
#[must_use]
pub struct InternalSessionServerHandle {
    address: SocketAddr,
    state: Arc<InternalSessionServerState>,
    join: Option<JoinHandle<Result<(), io::Error>>>,
    server: InternalSessionServerJoinState,
}

impl fmt::Debug for InternalSessionServerHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InternalSessionServerHandle")
            .field("address", &self.address)
            .field("server", &self.server)
            .field("pending_queries", &self.state.queries.len())
            .finish_non_exhaustive()
    }
}

impl InternalSessionServerHandle {
    /// Start a dedicated loopback listener over the selected session catalog.
    ///
    /// The listener binds before returning. Query work is admitted later under
    /// the bounded semaphore and is executed through `spawn_blocking` because
    /// the catalog authority is synchronous SQLite-backed code.
    ///
    /// # Errors
    /// Returns an error when the loopback listener cannot bind or report its
    /// bound address.
    pub async fn start(
        authority: Arc<SessionCatalogAuthority>,
        config: InternalSessionServerConfig,
    ) -> Result<Self, InternalSessionServerError> {
        let listener = tokio::net::TcpListener::bind(config.address())
            .await
            .map_err(InternalSessionServerError::Bind)?;
        let address = listener
            .local_addr()
            .map_err(InternalSessionServerError::Bind)?;
        let cancellation = CancellationToken::new();
        let state = Arc::new(InternalSessionServerState {
            authority,
            issuer: config.issuer,
            admission: Mutex::new(()),
            accepting: AtomicBool::new(true),
            queries: TaskTracker::new(),
            query_slots: Arc::new(Semaphore::new(INTERNAL_SESSION_QUERY_CONCURRENCY)),
            cancellation: cancellation.clone(),
        });
        let router = Router::new()
            .route(INTERNAL_SESSION_RESOLVE_PATH, post(resolve_session))
            .with_state(Arc::clone(&state));
        let join = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(cancellation.cancelled_owned())
                .await
        });
        Ok(Self {
            address,
            state,
            join: Some(join),
            server: InternalSessionServerJoinState::Pending,
        })
    }

    #[must_use]
    pub const fn address(&self) -> SocketAddr {
        self.address
    }

    /// Observe listener termination while retaining ownership if the wait is cancelled.
    pub async fn wait_for_listener(&mut self) -> InternalSessionServerJoinState {
        if let Some(join) = self.join.as_mut() {
            let result = (&mut *join).await;
            self.record_listener_exit(&result);
        }
        self.server
    }

    /// Stop admission, close the tracker, and join as much as the supplied
    /// budget permits. A pending listener join remains owned for retry.
    pub async fn shutdown(&mut self, timeout: Duration) -> InternalSessionServerShutdownReport {
        let started = Instant::now();
        self.state.stop_admission().await;
        let remaining = timeout.saturating_sub(started.elapsed());
        let _ = tokio::time::timeout(remaining, self.state.queries.wait()).await;
        let pending_queries = self.state.queries.len();
        let remaining = timeout.saturating_sub(started.elapsed());
        self.join_server(remaining).await;
        InternalSessionServerShutdownReport {
            pending_queries,
            server: self.server,
        }
    }

    async fn join_server(&mut self, timeout: Duration) {
        let Some(join) = self.join.as_mut() else {
            return;
        };
        let result = if join.is_finished() {
            Some((&mut *join).await)
        } else {
            tokio::time::timeout(timeout, &mut *join).await.ok()
        };
        if let Some(result) = result {
            self.record_listener_exit(&result);
        } else {
            self.server = InternalSessionServerJoinState::Pending;
        }
    }

    fn record_listener_exit(
        &mut self,
        result: &Result<Result<(), io::Error>, tokio::task::JoinError>,
    ) {
        self.join.take();
        self.server = if matches!(result, Ok(Ok(()))) {
            InternalSessionServerJoinState::Joined
        } else {
            InternalSessionServerJoinState::Failed
        };
    }
}

impl Drop for InternalSessionServerHandle {
    fn drop(&mut self) {
        self.state.accepting.store(false, Ordering::Release);
        self.state.cancellation.cancel();
        self.state.queries.close();
    }
}

async fn resolve_session(
    State(state): State<Arc<InternalSessionServerState>>,
    request: Request,
) -> Response {
    if !authorized(&state.issuer, request.headers()) {
        return response_text(StatusCode::UNAUTHORIZED, STATIC_UNAUTHORIZED);
    }
    if request.headers().contains_key(header::ORIGIN) {
        return response_text(StatusCode::FORBIDDEN, STATIC_ORIGIN_REJECTED);
    }
    let body = tokio::select! {
        () = state.cancellation.cancelled() => return response_text(StatusCode::SERVICE_UNAVAILABLE, STATIC_UNAVAILABLE),
        body = to_bytes(request.into_body(), INTERNAL_SESSION_MAX_BODY_BYTES) => body,
    };
    let Ok(body) = body else {
        return response_text(StatusCode::PAYLOAD_TOO_LARGE, STATIC_INVALID_REQUEST);
    };
    let wire_request: RemoteSessionResolveRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => return response_text(StatusCode::BAD_REQUEST, STATIC_INVALID_REQUEST),
    };
    let Ok(request) = wire_request.into_request() else {
        return response_text(StatusCode::BAD_REQUEST, STATIC_INVALID_REQUEST);
    };

    let receiver = {
        let _admission = state.admission.lock().await;
        if !state.accepting.load(Ordering::Acquire) {
            return response_text(StatusCode::SERVICE_UNAVAILABLE, STATIC_CAPACITY);
        }
        let Ok(permit) = Arc::clone(&state.query_slots).try_acquire_owned() else {
            return response_text(StatusCode::SERVICE_UNAVAILABLE, STATIC_CAPACITY);
        };
        let (sender, receiver) = oneshot::channel();
        let authority = Arc::clone(&state.authority);
        let tracker_token = state.queries.token();
        tokio::task::spawn_blocking(move || {
            let result = authority.resolve(request).map_err(|_| ());
            drop(permit);
            let _ = sender.send(result);
            drop(tracker_token);
        });
        receiver
    };

    match receiver.await {
        Ok(Ok(session)) => session_response(session.as_ref()),
        Ok(Err(())) | Err(_) => response_text(StatusCode::SERVICE_UNAVAILABLE, STATIC_UNAVAILABLE),
    }
}

fn authorized(issuer: &IssuerAuth, headers: &HeaderMap) -> bool {
    issuer.verify_bearer(
        headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
    )
}

fn session_response(session: Option<&RegisteredAgentSession>) -> Response {
    let Ok(reply) = RemoteSessionResolveReply::from_session(session) else {
        return response_text(StatusCode::INTERNAL_SERVER_ERROR, STATIC_INTERNAL);
    };
    let body = match serde_json::to_vec(&reply) {
        Ok(body) if body.len() <= INTERNAL_SESSION_MAX_BODY_BYTES => body,
        Ok(_) | Err(_) => return response_text(StatusCode::INTERNAL_SERVER_ERROR, STATIC_INTERNAL),
    };
    response_json(StatusCode::OK, body)
}

fn response_json(status: StatusCode, body: Vec<u8>) -> Response {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(JSON_CONTENT_TYPE),
    );
    response
}

fn response_text(status: StatusCode, body: &'static str) -> Response {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    response
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use forge_backend::remote_authority::RemoteAgentSessionAuthority;
    use forge_backend::session_protocol::RemoteSessionResolveRequest;
    use forge_control::{
        AgentSessionResolveRequest as ControlSessionResolveRequest, ControlErrorCode, ForgeControl,
        ForgeControlClient, ForgeControlWorker,
    };
    use forge_domain::{
        AgentSessionId, AgentSessionOpenRequestId, AuthenticatedPrincipal, OpenAgentSession,
        TimestampMillis,
    };
    use forge_persistence::GlobalRegistry;
    use forge_workspace::LocationService;
    use heterarchy_orchestration_sessions::{AgentSessionCatalog, SessionCatalogAuthority};
    use serde_json::Value;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use uuid::Uuid;

    use super::*;

    const SERVICE_SECRET: &str = "internal-test-secret";
    const HTTP_TIMEOUT: Duration = Duration::from_secs(5);
    const MAX_HTTP_RESPONSE_BYTES: usize = 4 * 1024;

    struct SessionFixture {
        root: PathBuf,
        registry_path: PathBuf,
        session: OpenAgentSession,
        principal: AuthenticatedPrincipal,
    }

    fn fixture() -> Result<SessionFixture, Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!(
            "orchestration-internal-session-integration-{}",
            Uuid::now_v7()
        ));
        fs::create_dir(&root)?;
        let registry_path = root.join("registry.sqlite3");
        let principal = AuthenticatedPrincipal::TrustedStdio;
        let session = OpenAgentSession::new(
            AgentSessionId::from_uuid(Uuid::from_u128(0xD101)),
            AgentSessionOpenRequestId::from_uuid(Uuid::from_u128(0xD102)),
            principal,
            TimestampMillis::new(10)?,
        );
        Ok(SessionFixture {
            root,
            registry_path,
            session,
            principal,
        })
    }

    fn seed_orchestration_closed(
        path: &std::path::Path,
        request: &OpenAgentSession,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut catalog = AgentSessionCatalog::open(path)?;
        let open = catalog.open_session(request)?;
        let _closed = catalog.close(
            open.session().id(),
            request.principal(),
            open.session().revision(),
            TimestampMillis::new(11)?,
        )?;
        Ok(())
    }

    async fn resolve_blocking(
        client: ForgeControlClient,
        request: ControlSessionResolveRequest,
    ) -> Result<
        Result<forge_domain::RegisteredAgentSession, forge_control::ControlClientError>,
        Box<dyn std::error::Error>,
    > {
        Ok(tokio::task::spawn_blocking(move || client.agent_session_resolve(request)).await?)
    }

    async fn post_json(
        address: SocketAddr,
        body: &[u8],
        authorization: Option<&str>,
        origin: Option<&str>,
    ) -> Result<(u16, Vec<u8>), Box<dyn std::error::Error>> {
        let exchange = async {
            let mut stream = tokio::net::TcpStream::connect(address).await?;
            let mut request = format!(
                "POST {INTERNAL_SESSION_RESOLVE_PATH} HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
                body.len()
            );
            if let Some(authorization) = authorization {
                request.push_str("Authorization: ");
                request.push_str(authorization);
                request.push_str("\r\n");
            }
            if let Some(origin) = origin {
                request.push_str("Origin: ");
                request.push_str(origin);
                request.push_str("\r\n");
            }
            request.push_str("\r\n");
            stream.write_all(request.as_bytes()).await?;
            stream.write_all(body).await?;
            let mut response = Vec::new();
            let mut chunk = [0_u8; 1024];
            loop {
                let read = stream.read(&mut chunk).await?;
                if read == 0 {
                    return Err("HTTP response ended before its complete frame".into());
                }
                if response.len().saturating_add(read) > MAX_HTTP_RESPONSE_BYTES {
                    return Err("internal session HTTP response exceeded its bound".into());
                }
                response.extend_from_slice(&chunk[..read]);
                // Rejected requests may close/reset with unread input. A
                // complete HTTP frame is the response, not the peer's EOF.
                if let Some(end) = response.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    let headers = std::str::from_utf8(&response[..end])?;
                    let length = headers
                        .lines()
                        .filter_map(|line| line.split_once(':'))
                        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                        .ok_or("HTTP response Content-Length missing")?
                        .1
                        .trim()
                        .parse::<usize>()?;
                    let expected = end
                        .checked_add(4)
                        .and_then(|head| head.checked_add(length))
                        .ok_or("HTTP response length overflow")?;
                    if expected > MAX_HTTP_RESPONSE_BYTES {
                        return Err("internal session HTTP response exceeded its bound".into());
                    }
                    if response.len() >= expected {
                        if response.len() != expected {
                            return Err("HTTP response has trailing bytes".into());
                        }
                        break;
                    }
                }
            }
            let text = String::from_utf8(response)?;
            let status = text
                .split_whitespace()
                .nth(1)
                .ok_or("HTTP status missing")?
                .parse::<u16>()?;
            let body = text
                .split_once("\r\n\r\n")
                .ok_or("HTTP response body missing")?
                .1
                .as_bytes()
                .to_vec();
            Ok::<_, Box<dyn std::error::Error>>((status, body))
        };
        tokio::time::timeout(HTTP_TIMEOUT, exchange).await?
    }

    fn request_body(session: &OpenAgentSession) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let wire = RemoteSessionResolveRequest::from_request(&ControlSessionResolveRequest {
            session_id: session.proposed_id(),
            principal: session.principal(),
        })?;
        Ok(serde_json::to_vec(&wire)?)
    }

    fn assert_error_code(
        result: Result<forge_domain::RegisteredAgentSession, forge_control::ControlClientError>,
        expected: ControlErrorCode,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let error = result
            .err()
            .ok_or("session resolve unexpectedly succeeded")?;
        if error.code() != expected {
            return Err(format!("expected {expected:?}, got {:?}", error.code()).into());
        }
        Ok(())
    }

    struct RunningRemote {
        server: InternalSessionServerHandle,
        client: ForgeControlClient,
        worker: ForgeControlWorker,
    }

    async fn start_default_control(
        fixture: &SessionFixture,
    ) -> Result<(ForgeControlClient, ForgeControlWorker), Box<dyn std::error::Error>> {
        let local_control = ForgeControl::new(GlobalRegistry::open(&fixture.registry_path)?);
        let (client, worker) = ForgeControlWorker::start(local_control)?;
        let outcome = tokio::task::spawn_blocking({
            let client = client.clone();
            let request = fixture.session;
            move || client.agent_session_open(request)
        })
        .await??;
        if outcome.session().state() != forge_domain::AgentSessionState::Open {
            return Err("default Forge catalog did not retain an open local session".into());
        }
        Ok((client, worker))
    }

    async fn start_remote_control(
        fixture: &SessionFixture,
    ) -> Result<RunningRemote, Box<dyn std::error::Error>> {
        let orchestration_path = fixture.root.join("orchestration-sessions.sqlite3");
        seed_orchestration_closed(&orchestration_path, &fixture.session)?;
        let authority = Arc::new(SessionCatalogAuthority::new(orchestration_path));
        let config = InternalSessionServerConfig::new(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            SERVICE_SECRET,
        )?;
        let diagnostics = format!("{config:?}");
        if !diagnostics.contains("secret_configured: true") || diagnostics.contains(SERVICE_SECRET)
        {
            return Err("internal server diagnostics exposed or omitted secret state".into());
        }
        let server = InternalSessionServerHandle::start(authority, config).await?;
        let remote = RemoteAgentSessionAuthority::new(
            tokio::runtime::Handle::current(),
            server.address(),
            SERVICE_SECRET,
            Duration::from_secs(2),
        )?;
        let remote_control = ForgeControl::with_session_authority(
            LocationService::new(GlobalRegistry::open(&fixture.registry_path)?),
            Box::new(remote),
        );
        let (client, worker) = ForgeControlWorker::start(remote_control)?;
        Ok(RunningRemote {
            server,
            client,
            worker,
        })
    }

    async fn assert_raw_http_contract(
        address: SocketAddr,
        body: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (status, response_body) =
            post_json(address, body, Some("Bearer internal-test-secret"), None).await?;
        if status != 200 {
            return Err(format!("raw remote session request returned HTTP {status}").into());
        }
        let response: Value = serde_json::from_slice(&response_body)?;
        if response["outcome"]["kind"] != "found"
            || response["outcome"]["session"]["state"] != "closed"
        {
            return Err("raw remote session response did not preserve closed metadata".into());
        }
        let (status, _) = post_json(address, body, None, None).await?;
        if status != 401 {
            return Err(format!("missing Authorization returned HTTP {status}").into());
        }
        let (status, _) = post_json(
            address,
            body,
            Some("Bearer internal-test-secret"),
            Some("https://unexpected.example"),
        )
        .await?;
        if status != 403 {
            return Err(format!("Origin request returned HTTP {status}").into());
        }
        Ok(())
    }

    #[tokio::test]
    async fn partial_request_body_is_cancelled_before_listener_join()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = fixture()?;
        let authority = Arc::new(SessionCatalogAuthority::new(
            fixture.root.join("sessions.sqlite3"),
        ));
        let config = InternalSessionServerConfig::new(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            SERVICE_SECRET,
        )?;
        let mut server = InternalSessionServerHandle::start(authority, config).await?;
        let mut stream = tokio::net::TcpStream::connect(server.address()).await?;
        let header = format!(
            "POST {INTERNAL_SESSION_RESOLVE_PATH} HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {SERVICE_SECRET}\r\nContent-Length: 100\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n",
            server.address()
        );
        stream.write_all(header.as_bytes()).await?;
        let mut continued = [0_u8; 25];
        tokio::time::timeout(Duration::from_secs(1), stream.read_exact(&mut continued)).await??;
        assert_eq!(&continued, b"HTTP/1.1 100 Continue\r\n\r\n");
        let report = server.shutdown(Duration::from_secs(1)).await;
        assert!(
            report.is_successful(),
            "partial body prevented shutdown: {report:?}"
        );
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), stream.read_to_end(&mut response)).await??;
        assert!(std::str::from_utf8(&response)?.starts_with("HTTP/1.1 503"));
        fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[tokio::test]
    async fn failed_listener_is_observed_and_terminal_join_is_retained()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = fixture()?;
        let authority = Arc::new(SessionCatalogAuthority::new(
            fixture.root.join("sessions.sqlite3"),
        ));
        let config = InternalSessionServerConfig::new(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            SERVICE_SECRET,
        )?;
        let mut server = InternalSessionServerHandle::start(authority, config).await?;
        server.join.as_ref().ok_or("missing listener join")?.abort();
        assert_eq!(
            server.wait_for_listener().await,
            InternalSessionServerJoinState::Failed
        );
        let report = server.shutdown(Duration::from_secs(1)).await;
        assert!(report.is_complete());
        assert!(!report.is_successful());
        assert_eq!(
            server.wait_for_listener().await,
            InternalSessionServerJoinState::Failed
        );
        fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[tokio::test]
    async fn remote_authority_is_used_for_forge_session_resolution_and_no_fallback()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = fixture()?;
        let (local_client, mut local_worker) = start_default_control(&fixture).await?;
        let mut remote = start_remote_control(&fixture).await?;
        let request = ControlSessionResolveRequest {
            session_id: fixture.session.proposed_id(),
            principal: fixture.principal,
        };
        let first = resolve_blocking(remote.client.clone(), request).await?;
        assert_error_code(first, ControlErrorCode::AgentSessionClosed)?;
        let body = request_body(&fixture.session)?;
        assert_raw_http_contract(remote.server.address(), &body).await?;
        let report = remote.server.shutdown(Duration::from_secs(1)).await;
        if !report.is_successful() {
            return Err(format!("internal session server did not shut down: {report:?}").into());
        }
        let second = resolve_blocking(remote.client.clone(), request).await?;
        assert_error_code(second, ControlErrorCode::AgentSessionAuthorityUnavailable)?;
        drop(remote.client);
        drop(local_client);
        remote.worker.shutdown()?;
        local_worker.shutdown()?;
        fs::remove_dir_all(fixture.root)?;
        Ok(())
    }
}
