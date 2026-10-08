//! Bearer authentication adapter over the app-global token authority.
//!
//! This reuses the exact Orchestration token authority the MCP transport
//! uses (same Keychain pepper, same `mcp-tokens.sqlite3` catalog, same
//! bounded worker design); it never creates a second secret store. Every
//! `/internal/v1/**` route sits behind [`require_bearer`]; `/health` and
//! `/ready` stay open for supervision. The authenticated
//! `AuthenticatedPrincipal` is request-scoped and is the only identity the
//! domain lanes may trust — body fields like `agent_id` or `session_id` are
//! never promoted to identity.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use forge_domain::{AuthenticatedPrincipal, DirectTokenId};
use heterarchy_orchestration_auth::{
    MAX_MCP_TOKEN_BEARER_BYTES, McpTokenClient, McpTokenClientError, McpTokenServiceError,
};
use std::sync::atomic::{AtomicBool, Ordering};
use thiserror::Error;
use tokio::sync::{Semaphore, oneshot};
use tokio_util::task::TaskTracker;

/// Synchronous token-authentication jobs admitted at once.
///
/// Deliberately below the token worker queue capacity: a request arriving
/// while all slots are occupied receives `503` instead of growing an
/// unbounded blocking-pool backlog.
const AUTH_BLOCKING_CAPACITY: usize = 4;

const _: () =
    assert!(AUTH_BLOCKING_CAPACITY <= heterarchy_orchestration_auth::MCP_TOKEN_QUEUE_CAPACITY);

/// Shared, clonable bearer authenticator bound into the router layer.
#[derive(Debug, Clone)]
pub(crate) struct BearerAuthorizer {
    token_client: McpTokenClient,
    auth_blocking_slots: Arc<Semaphore>,
    admission: Arc<std::sync::Mutex<()>>,
    jobs: TaskTracker,
    failed: Arc<AtomicBool>,
}

impl BearerAuthorizer {
    pub(crate) fn new(token_client: McpTokenClient) -> Self {
        Self {
            token_client,
            auth_blocking_slots: Arc::new(Semaphore::new(AUTH_BLOCKING_CAPACITY)),
            admission: Arc::new(std::sync::Mutex::new(())),
            jobs: TaskTracker::new(),
            failed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Authenticate one `Bearer <token>` header value off the async path.
    ///
    /// The auth worker is a blocking OS-thread service; the bounded slot
    /// keeps the Tokio blocking pool bounded under authentication load.
    async fn authenticate(
        &self,
        authorization: String,
    ) -> Result<Option<AuthenticatedPrincipal>, AuthFailure> {
        self.run_job(move |client| match client.authenticate(authorization) {
            Ok(Some(token)) => Ok(Some(AuthenticatedPrincipal::DirectToken(
                DirectTokenId::from_uuid(token.token_id().into_uuid()),
            ))),
            Ok(None) => Ok(None),
            Err(error) => Err(classify_auth_error(&error)),
        })
        .await
    }

    pub(crate) async fn revalidate(
        &self,
        credential: &CheckedCredential,
    ) -> Result<bool, AuthFailure> {
        self.authenticate(credential.authorization.clone())
            .await
            .map(|principal| principal == Some(credential.principal))
    }

    async fn run_job<T, F>(&self, operation: F) -> Result<T, AuthFailure>
    where
        T: Send + 'static,
        F: FnOnce(McpTokenClient) -> Result<T, AuthFailure> + Send + 'static,
    {
        let receiver = {
            let _gate = self
                .admission
                .lock()
                .map_err(|_| AuthFailure::Unavailable)?;
            let permit = Arc::clone(&self.auth_blocking_slots)
                .try_acquire_owned()
                .map_err(|_| AuthFailure::Unavailable)?;
            let client = self.token_client.clone();
            let failed = Arc::clone(&self.failed);
            let (sender, receiver) = oneshot::channel();
            let _owned = self.jobs.spawn(async move {
                let result = tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    operation(client)
                })
                .await;
                let result = match result {
                    Ok(result) => result,
                    Err(error) => {
                        failed.store(true, Ordering::Release);
                        tracing::warn!(?error, "owned token query failed to join");
                        Err(AuthFailure::Unavailable)
                    }
                };
                let _disconnected = sender.send(result);
            });
            receiver
        };
        receiver.await.map_err(|_| AuthFailure::Unavailable)?
    }

    pub(crate) fn stop_admission(&self) {
        let _gate = self
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.auth_blocking_slots.close();
    }

    pub(crate) async fn drain(&self) -> Result<(), AuthFailure> {
        self.jobs.close();
        self.jobs.wait().await;
        if self.failed.load(Ordering::Acquire) {
            Err(AuthFailure::Unavailable)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone)]
pub(crate) struct CheckedCredential {
    authorization: String,
    principal: AuthenticatedPrincipal,
}

impl std::fmt::Debug for CheckedCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CheckedCredential")
            .field("authorization", &"<redacted>")
            .finish_non_exhaustive()
    }
}

fn classify_auth_error(error: &McpTokenClientError) -> AuthFailure {
    match error {
        McpTokenClientError::Service(
            McpTokenServiceError::MalformedBearer
            | McpTokenServiceError::Inactive
            | McpTokenServiceError::NotFound,
        ) => AuthFailure::Invalid,
        McpTokenClientError::QueueFull
        | McpTokenClientError::WorkerClosed
        | McpTokenClientError::WorkerShuttingDown
        | McpTokenClientError::ResponseDropped
        | McpTokenClientError::Service(_) => AuthFailure::Unavailable,
    }
}

/// Why a request was not admitted past the bearer layer.
#[derive(Debug, Error)]
pub(crate) enum AuthFailure {
    #[error("bearer authentication failed")]
    Invalid,
    #[error("token authority is unavailable")]
    Unavailable,
}

/// Route layer: resolve the `Authorization` header into a request-scoped
/// authenticated principal or reject the request before any handler runs.
pub(crate) async fn require_bearer(
    State(authorizer): State<BearerAuthorizer>,
    mut request: Request,
    next: Next,
) -> Response {
    if let Some(header) = request.headers_mut().get_mut(header::AUTHORIZATION) {
        header.set_sensitive(true);
    }
    let authorization = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .filter(|value| value.len() <= MAX_MCP_TOKEN_BEARER_BYTES)
        .map(str::to_owned);
    let Some(authorization) = authorization else {
        return AuthFailure::Invalid.into_response();
    };
    match authorizer.authenticate(authorization.clone()).await {
        Ok(Some(principal)) => {
            request.extensions_mut().insert(principal);
            request.extensions_mut().insert(CheckedCredential {
                authorization,
                principal,
            });
            request.extensions_mut().insert(authorizer);
            next.run(request).await
        }
        Ok(None) | Err(AuthFailure::Invalid) => AuthFailure::Invalid.into_response(),
        Err(AuthFailure::Unavailable) => AuthFailure::Unavailable.into_response(),
    }
}

impl IntoResponse for AuthFailure {
    fn into_response(self) -> Response {
        match self {
            Self::Invalid => (
                StatusCode::UNAUTHORIZED,
                Json(crate::ErrorResponse {
                    code: "UNAUTHENTICATED",
                    message: "A valid bearer token is required.",
                    retryable: false,
                }),
            )
                .into_response(),
            Self::Unavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(crate::ErrorResponse {
                    code: "TOKEN_AUTHORITY_UNAVAILABLE",
                    message: "The token authority is temporarily unavailable.",
                    retryable: true,
                }),
            )
                .into_response(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use forge_domain::TimestampMillis;
    use heterarchy_orchestration_auth::{
        McpTokenId, McpTokenIssueRequest, McpTokenLabel, McpTokenRuntime, McpTokenService,
        McpTokenWorker,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::*;
    /// Started token authority over an isolated temp catalog; the worker is
    /// kept alive by the returned guard.
    struct TempAuthority {
        authorizer: BearerAuthorizer,
        runtime: McpTokenRuntime,
        issued_bearer: String,
    }

    fn start_temp_authority(label: &str) -> Result<TempAuthority, Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!(
            "backend-auth-{label}-{}-{}",
            std::process::id(),
            Uuid::now_v7()
        ));
        std::fs::create_dir_all(&root)?;
        let catalog =
            heterarchy_orchestration_auth::McpTokenCatalog::open(root.join("mcp-tokens.sqlite3"))?;
        let (client, worker) = McpTokenWorker::start(McpTokenService::new([0x5a; 32], catalog))?;
        let issued = client.issue(McpTokenIssueRequest::new(
            McpTokenLabel::new(label.to_owned())?,
            TimestampMillis::new(1)?,
        ))?;
        let issued_bearer = format!("Bearer {}", issued.reveal().as_str());
        let authorizer = BearerAuthorizer::new(client.clone());
        let runtime = McpTokenRuntime::from_parts(client, worker);
        Ok(TempAuthority {
            authorizer,
            runtime,
            issued_bearer,
        })
    }

    #[tokio::test]
    async fn authenticate_rejects_malformed_and_unknown_bearer_values()
    -> Result<(), Box<dyn std::error::Error>> {
        let authority = start_temp_authority("authorizer")?;
        assert!(matches!(
            authority
                .authorizer
                .authenticate("not-a-bearer".to_owned())
                .await,
            Err(AuthFailure::Invalid)
        ));
        assert!(matches!(
            authority
                .authorizer
                .authenticate(format!("Bearer {}", "A".repeat(43)))
                .await,
            Ok(None)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn auth_queries_are_owned_after_disconnect_and_joined_before_shutdown()
    -> Result<(), Box<dyn std::error::Error>> {
        let authority = start_temp_authority("auth-drain")?;
        let (started, started_rx) = oneshot::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        let authorizer = authority.authorizer.clone();
        let caller = tokio::spawn(async move {
            authorizer
                .run_job(move |_client| {
                    let _observed = started.send(());
                    release_rx.recv().map_err(|_| AuthFailure::Unavailable)?;
                    Ok(())
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), started_rx).await??;
        caller.abort();
        assert!(caller.await.is_err());
        authority.authorizer.stop_admission();
        let authorizer = authority.authorizer.clone();
        let drain = tokio::spawn(async move { authorizer.drain().await });
        tokio::task::yield_now().await;
        assert!(!drain.is_finished());
        release.send(())?;
        tokio::time::timeout(Duration::from_secs(2), drain).await???;
        assert!(authority.authorizer.jobs.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn served_stream_closes_after_its_token_is_revoked()
    -> Result<(), Box<dyn std::error::Error>> {
        if loopback_network_is_unavailable() {
            return Ok(());
        }
        let authority = start_temp_authority("stream-revoke")?;
        let claim = authority
            .authorizer
            .authenticate(authority.issued_bearer.clone())
            .await?
            .ok_or("operator principal missing")?;
        let token = authority
            .authorizer
            .token_client
            .get(McpTokenId::from_uuid(
                claim
                    .direct_token_id()
                    .ok_or("direct token principal missing")?
                    .into_uuid(),
            ))?
            .ok_or("operator token metadata missing")?;
        let reservation = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = reservation.local_addr()?;
        drop(reservation);
        let root = std::env::temp_dir().join(format!("stream-revoke-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let config = crate::BackendConfig::new(address, root.clone())?
            .with_operator_policy(
                heterarchy_orchestration_runtime::access::OperatorPolicy::new(vec![
                    AuthenticatedPrincipal::DirectToken(
                        claim
                            .direct_token_id()
                            .ok_or("direct token principal missing")?,
                    ),
                ])?,
            )
            .with_token_authority(crate::TokenAuthorityBootstrap::Runtime(authority.runtime));
        let cancellation = CancellationToken::new();
        let server = tokio::spawn(crate::serve(config, cancellation.clone()));
        wait_for_listener(address).await;

        let readiness = raw_request(
            address,
            "GET /ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await?;
        assert!(readiness.starts_with("HTTP/1.1 200"));
        assert!(readiness.contains("\"readinessScope\":\"local_runtime\""));
        assert!(readiness.contains("\"coordinatorActorProof\":\"unavailable\""));
        let mut stream = tokio::net::TcpStream::connect(address).await?;
        stream.write_all(format!(
            "GET /internal/v1/events HTTP/1.1\r\nHost: localhost\r\nAuthorization: {}\r\nConnection: close\r\n\r\n", authority.issued_bearer
        ).as_bytes()).await?;
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !response.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                response.push(stream.read_u8().await?);
                if response.len() > 4096 {
                    return Err(std::io::Error::other("header bound exceeded"));
                }
            }
            Ok::<_, std::io::Error>(())
        })
        .await??;
        assert!(response.starts_with(b"HTTP/1.1 200"));
        let _revoked = authority.authorizer.token_client.revoke(
            token.id(),
            token.revision(),
            TimestampMillis::new(2)?,
        )?;
        tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut response)).await??;
        let response = std::str::from_utf8(&response)?;
        assert!(response.contains("access_revoked"));
        assert!(!response.contains("state_changed"));
        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(2), server).await???;
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn served_backend_rejects_unauthenticated_internal_requests()
    -> Result<(), Box<dyn std::error::Error>> {
        if loopback_network_is_unavailable() {
            return Ok(());
        }
        let authority = start_temp_authority("integration")?;
        // Reserve an ephemeral port so the test knows the served address;
        // the reservation socket is dropped before the server binds it.
        let reservation = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = reservation.local_addr()?;
        drop(reservation);

        let root = std::env::temp_dir().join(format!(
            "backend-auth-server-{}-{}",
            std::process::id(),
            Uuid::now_v7()
        ));
        std::fs::create_dir_all(&root)?;
        let claim = authority
            .authorizer
            .authenticate(authority.issued_bearer.clone())
            .await?
            .ok_or("issued principal missing")?;
        let principal = AuthenticatedPrincipal::DirectToken(
            claim
                .direct_token_id()
                .ok_or("direct token principal missing")?,
        );
        let seed = heterarchy_orchestration_runtime::OrchestrationRuntime::open(&root)?;
        let owned_session = seed.open_session(Uuid::now_v7(), principal, 1)?.session;
        let closed = seed.open_session(Uuid::now_v7(), principal, 1)?.session;
        seed.close_session(closed.session_id, principal, closed.revision, 2)?;
        drop(seed);
        let foreign = authority.runtime.client().issue(McpTokenIssueRequest::new(
            McpTokenLabel::new("foreign-session-principal".to_owned())?,
            TimestampMillis::new(1)?,
        ))?;
        let foreign_bearer = format!("Bearer {}", foreign.reveal().as_str());
        let foreign_claim = authority
            .authorizer
            .authenticate(foreign_bearer.clone())
            .await?
            .ok_or("operator token principal missing")?;
        let config = crate::BackendConfig::new(address, root.clone())?
            .with_operator_policy(
                heterarchy_orchestration_runtime::access::OperatorPolicy::new(vec![
                    AuthenticatedPrincipal::DirectToken(
                        foreign_claim
                            .direct_token_id()
                            .ok_or("direct token principal missing")?,
                    ),
                ])?,
            )
            .with_token_authority(crate::TokenAuthorityBootstrap::Runtime(authority.runtime));
        let cancellation = CancellationToken::new();
        let server = tokio::spawn(crate::serve(config, cancellation.clone()));
        wait_for_listener(address).await;

        let unauthenticated = raw_request(
            address,
            "GET /internal/v1/agent-profiles HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await?;
        assert!(
            unauthenticated.starts_with("HTTP/1.1 401"),
            "expected 401 for the unauthenticated internal request"
        );

        let authorized = raw_request(
            address,
            &format!(
                "GET /internal/v1/agent-profiles HTTP/1.1\r\nHost: localhost\r\nAuthorization: {}\r\nConnection: close\r\n\r\n",
                authority.issued_bearer
            ),
        )
        .await?;
        let status = authorized.lines().next().unwrap_or("<empty>").to_owned();
        assert!(
            !status.starts_with("HTTP/1.1 401") && !status.starts_with("HTTP/1.1 5"),
            "authenticated request must pass the bearer layer, got {status}"
        );
        assert_operator_profile_read(address, &foreign_bearer).await?;

        assert_session_access_http(
            address,
            owned_session.session_id,
            closed.session_id,
            &authority.issued_bearer,
            &foreign_bearer,
            &root,
        )
        .await?;
        assert_no_operator_http(address, &authority.issued_bearer).await?;

        let (mut events, mut event_response) =
            open_stream_headers(address, &foreign_bearer).await?;
        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(2), server).await???;
        tokio::time::timeout(
            Duration::from_secs(2),
            events.read_to_end(&mut event_response),
        )
        .await??;
        let response = std::str::from_utf8(&event_response)?;
        assert!(response.contains("resync_required"));
        assert!(response.contains("server_shutdown"));
        Ok(())
    }

    async fn assert_operator_profile_read(
        address: std::net::SocketAddr,
        bearer: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let response = raw_request(
            address,
            &format!("GET /internal/v1/agent-profiles HTTP/1.1\r\nHost: localhost\r\nAuthorization: {bearer}\r\nConnection: close\r\n\r\n"),
        ).await?;
        assert!(response.starts_with("HTTP/1.1 200"));
        Ok(())
    }

    async fn open_stream_headers(
        address: std::net::SocketAddr,
        bearer: &str,
    ) -> Result<(tokio::net::TcpStream, Vec<u8>), Box<dyn std::error::Error>> {
        let mut stream = tokio::net::TcpStream::connect(address).await?;
        stream.write_all(format!("GET /internal/v1/events HTTP/1.1\r\nHost: localhost\r\nAuthorization: {bearer}\r\nConnection: close\r\n\r\n").as_bytes()).await?;
        let mut headers = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !headers.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                headers.push(stream.read_u8().await?);
                if headers.len() > 4096 {
                    return Err(std::io::Error::other("SSE header bound exceeded"));
                }
            }
            Ok::<_, std::io::Error>(())
        })
        .await??;
        assert!(headers.starts_with(b"HTTP/1.1 200"));
        Ok((stream, headers))
    }

    async fn assert_session_access_http(
        address: std::net::SocketAddr,
        owned_session_id: Uuid,
        closed_session_id: Uuid,
        owned_bearer: &str,
        foreign_bearer: &str,
        root: &std::path::Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let owned = raw_request(address, &format!(
            "GET /internal/v1/sessions/{owned_session_id} HTTP/1.1\r\nHost: localhost\r\nAuthorization: {owned_bearer}\r\nConnection: close\r\n\r\n"
        )).await?;
        assert!(owned.starts_with("HTTP/1.1 200"));
        let foreign_read = raw_request(address, &format!(
            "GET /internal/v1/sessions/{owned_session_id} HTTP/1.1\r\nHost: localhost\r\nAuthorization: {foreign_bearer}\r\nConnection: close\r\n\r\n"
        )).await?;
        assert!(foreign_read.starts_with("HTTP/1.1 403"));
        let workspace_id = Uuid::now_v7();
        let team_id = Uuid::now_v7();
        let body = serde_json::json!({
            "scope":{"workspaceId":workspace_id,"locationId":Uuid::now_v7(),"locationRevision":1,"workspaceRevision":1,"locator":"workspace"},
            "teamId":team_id,"requestId": Uuid::now_v7(), "activityId": Uuid::now_v7(),
            "sessionId": owned_session_id, "expectedTeamRevision": 1,
            "role": "participant"
        })
        .to_string();
        let forged_join = raw_request(address, &format!(
            "POST /internal/v1/workspaces/{}/teams/{}/join HTTP/1.1\r\nHost: localhost\r\nAuthorization: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            workspace_id, team_id, foreign_bearer, body.len(), body
        )).await?;
        assert!(forged_join.starts_with("HTTP/1.1 403"));
        assert!(!root.join("broker-command-session.json").exists());
        let closed_read = raw_request(address, &format!(
            "GET /internal/v1/sessions/{closed_session_id} HTTP/1.1\r\nHost: localhost\r\nAuthorization: {owned_bearer}\r\nConnection: close\r\n\r\n"
        )).await?;
        assert!(closed_read.starts_with("HTTP/1.1 200"));
        assert!(closed_read.contains("closed"));
        let workspace = Uuid::now_v7();
        let outside_membership = raw_request(address, &format!(
            "GET /internal/v1/workspaces/{workspace}/teams HTTP/1.1\r\nHost: localhost\r\nAuthorization: {owned_bearer}\r\nX-Orchestration-Session-Id: {owned_session_id}\r\nConnection: close\r\n\r\n",
        )).await?;
        assert!(outside_membership.starts_with("HTTP/1.1 403"));

        Ok(())
    }

    async fn assert_no_operator_http(
        address: std::net::SocketAddr,
        bearer: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let body = serde_json::json!({
            "decision": "approve", "sessionId": Uuid::now_v7(),
            "scope": {"workspaceId": Uuid::now_v7(), "locationId": Uuid::now_v7(),
                "locationRevision": 1, "workspaceRevision": 1, "locator": "/tmp/denied"}
        })
        .to_string();
        let response = raw_request(address, &format!(
            "POST /internal/v1/approval-requests/{}/decision HTTP/1.1\r\nHost: localhost\r\nAuthorization: {bearer}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            Uuid::now_v7(), body.len(),
        )).await?;
        assert!(response.starts_with("HTTP/1.1 403"));
        for path in [
            "/internal/v1/provider-worker/provision/dispatch",
            "/internal/v1/provider-worker/messages/dispatch",
            "/internal/v1/agent-profile-provider-deletions/next",
        ] {
            let response = raw_request(address, &format!(
                "POST {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: {bearer}\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}"
            )).await?;
            assert!(
                response.starts_with("HTTP/1.1 403"),
                "worker privilege denied for {path}"
            );
        }
        Ok(())
    }

    fn loopback_network_is_unavailable() -> bool {
        std::env::var(forge_control::RUST_CI_SANDBOX_MARKER_ENV).as_deref()
            == Ok(forge_control::RUST_CI_SANDBOX_MARKER_VALUE)
    }

    async fn wait_for_listener(address: std::net::SocketAddr) {
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(address).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            tokio::net::TcpStream::connect(address).await.is_ok(),
            "backend listener did not come up"
        );
    }

    async fn raw_request(
        address: std::net::SocketAddr,
        request: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let mut stream = tokio::net::TcpStream::connect(address).await?;
        stream.write_all(request.as_bytes()).await?;
        let mut response = Vec::new();
        let _ignored =
            tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response)).await;
        Ok(String::from_utf8_lossy(&response).to_string())
    }
}
