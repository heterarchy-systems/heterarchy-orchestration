//! Official bounded HTTP client. Each call presents its original caller
//! credential; this client owns no catalog, fallback, retry or detached task.

use std::{fmt, net::SocketAddr, sync::Arc, time::Duration};

use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Bytes;
use hyper::header::{
    AUTHORIZATION, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HOST, HeaderMap, HeaderValue,
};
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Semaphore;
use uuid::Uuid;

use crate::wire::{
    AgentProfileCreateRequest, AgentProfileCreateResponse, AgentProfileResponse,
    AgentSessionCloseRequest, AgentSessionOpenReadbackResponse, AgentSessionOpenRequest,
    AgentSessionOpenResponse, AgentSessionResponse,
};

mod agent_request_admission;
mod agent_requests;
mod approval;
mod coordination;
mod departure;
mod forge_reads;
mod handoff;
mod leadership;
mod leadership_ballots;
mod leadership_lifecycle;
mod leadership_recalls;
mod leadership_reviews;
mod leadership_statements;
mod member_list;
mod membership;
mod node_chat;
mod node_team;
mod operations_import;
mod participation;
mod profile_deletion;
mod profile_sessions;
mod profiles;
mod provider_worker;
mod provisioning_operations;
mod provisioning_participation;
mod team_commands;
mod teams;
#[cfg(test)]
mod tests;
mod work_credit;

const CLIENT_CONCURRENCY: usize = 4;
const MAX_HEADERS: usize = 16;
const MAX_HEADER_BYTES: usize = 8 * 1024;
const MAX_REQUEST_BYTES: usize = 64 * 1024;
const MAX_RESPONSE_BYTES: usize = 256 * 1024;
// Review pages retain up to 64 statements of 16 KiB plus identities and an
// incumbent response. This also bounds worst-case JSON string escaping.
const MAX_REVIEW_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_TIMEOUT: Duration = Duration::from_secs(5);

/// Exact durable identity to resolve after an uncertain mutation response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadbackTarget {
    /// Recover the exact immutable Profile deletion intent without resuming it.
    ProfileDeletionRequest { request_id: Uuid },
    /// Observe current provider deletion selection; not original-request proof.
    ProfileDeletionSelection,
    /// Read the exact original provider fence and optional durable receipt.
    ProfileDeletionProvider(
        Box<crate::wire::profile_deletion::AgentProfileProviderDeletionFenceWire>,
    ),
    /// Bounded current selection observation, never original-request proof or retry permission.
    ProviderProvisionDispatch,
    /// Bounded current pending/unknown outboxes, without request attribution or retry permission.
    ProviderMessageDispatch,
    /// Pure operator readback for one exact released operation.
    ProviderProvisionOperation { operation_id: Uuid },
    /// Pure operator readback for one exact message, without an Inspect transition.
    ProviderMessageOutbox { message_id: Uuid },
    /// Original incoming intent; a pure turn lookup, never replaying the writer.
    ProviderIncomingTurn(Box<crate::wire::provider_worker::ProviderIncomingMessage>),
    /// Exact original capability-request intent, without retry authorization.
    AgentRequestAdmission(Box<crate::wire::agent_request_admission::AgentRequestAdmissionRequest>),
    /// Recover the immutable actor, body, context and target witness of this
    /// message. An absent receipt does not authorize a blind retry.
    NodeChat(Box<crate::wire::node_chat::NodeChatSendRequest>),
    /// Observe the exact Node-Team scope after an uncertain CAS. This query
    /// cannot prove mutation attribution and does not authorize a retry.
    NodeTeam(Box<crate::wire::node_team::NodeTeamQueryRequest>),
    /// Complete caller-controlled handoff intent; current state cannot replace its fences.
    MemberHandoff(Box<crate::wire::handoff::HandoffRequest>),
    /// Read the exact logical departure; an absent local receipt is not source retry authority.
    MemberDeparture {
        workspace_id: Uuid,
        activity_id: Uuid,
        agent_id: Uuid,
        session_id: Uuid,
        request_id: Uuid,
        expected_agent_revision: i64,
    },
    /// Read the caller-owned confirmed participation after an uncertain stage.
    AgentRequestParticipation {
        operation_id: Uuid,
        session_id: Option<Uuid>,
    },
    /// Read the exact logical-member registration after an uncertain source or commit.
    MemberRegistration {
        workspace_id: Uuid,
        activity_id: Uuid,
        session_id: Uuid,
        request_id: Uuid,
    },
    /// Observe the exact leadership scope after an uncertain advance.
    LeadershipScope {
        workspace_id: Uuid,
        activity_id: Option<Uuid>,
        team_id: Option<Uuid>,
    },
    /// Read the durable confirm intent after an uncertain fence/commit/recovery.
    LeadershipConfirmation {
        workspace_id: Uuid,
        activity_id: Option<Uuid>,
        team_id: Option<Uuid>,
        request_id: Uuid,
    },

    /// Read the full frozen Team mutation by its exact request identity.
    TeamMutation {
        workspace_id: Uuid,
        team_id: Uuid,
        request_id: Uuid,
    },

    /// Observe presence after an uncertain CAS; the revision is not attribution proof.
    Presence {
        workspace_id: Uuid,
        activity_id: Uuid,
        agent_id: Uuid,
        session_id: Uuid,
        expected_revision: i64,
    },
    /// Observe the exact recipient/event receipt after an uncertain ACK.
    InboxReceipt {
        workspace_id: Uuid,
        activity_id: Uuid,
        agent_id: Uuid,
        session_id: Uuid,
        event_id: Uuid,
        expected_revision: i64,
    },

    /// Read the exact persisted review intent; current phase/revision are observations.
    LeadershipReview {
        workspace_id: Uuid,
        request_id: Uuid,
    },
    /// Read the immutable statement journal after an uncertain append reply.
    LeadershipStatement {
        workspace_id: Uuid,
        message_id: Uuid,
    },
    /// Read the exact frozen subject ballot; a mutation is never retried automatically.
    LeadershipBallot {
        workspace_id: Uuid,
        activity_id: Uuid,
        session_id: Uuid,
        election_id: Uuid,
    },
    /// Read the Profile receipt by the original creation/mutation nonce.
    ProfileRequest(Uuid),
    /// Re-present the exact Session open nonce to recover its server-issued ID.
    SessionRequest(Uuid),
    /// Read the named Session and its terminal revision.
    Session(Uuid),
    /// Resolve the immutable Profile/Session correspondence, including when
    /// the requested nonce aliases an existing link's original receipt.
    ProfileSession {
        request_id: Uuid,
        profile_id: Uuid,
        session_id: Uuid,
    },
    /// Read the exact provision operation after uncertain cancellation. No receipt nonce is implied.
    ProvisionOperation {
        workspace_id: Uuid,
        operation_id: Uuid,
    },
    /// Read current policy and compare the intended result revision/configuration.
    /// A policy CAS has no replay nonce; revision alone cannot attribute a commit.
    WorkspacePolicy {
        workspace_id: Uuid,
        expected_revision: u64,
    },
    /// Read the exact approval request after an uncertain submission or
    /// decision exchange.
    ApprovalRequest {
        workspace_id: Uuid,
        request_id: Uuid,
    },
    /// Read the exact import progress after an uncertain operator mutation.
    OperationsImport(Box<OperationsImportReadback>),
}

/// Original operator import mutation intent retained for durable readback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationsImportReadback {
    Prepare(Box<crate::wire::operations_import::ImportPrepareRequest>),
    ImportNextPage {
        migration_id: Uuid,
        request: Box<crate::wire::operations_import::ImportStepRequest>,
    },
    VerifyNextPage {
        migration_id: Uuid,
        request: Box<crate::wire::operations_import::ImportStepRequest>,
    },
    Abort {
        migration_id: Uuid,
        request: Box<crate::wire::operations_import::ImportStepRequest>,
    },
}

/// Closed, redacted service classifications. Peer messages are never exposed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceCode {
    /// Unauthenticated caller.
    Authentication,
    /// Authenticated caller lacks permission.
    Forbidden,
    /// Invalid identity or bounded request input.
    InvalidInput,
    /// Missing resource.
    NotFound,
    /// Reused nonce, terminal state or stale revision.
    Conflict,
    /// Refresh the canonical snapshot.
    ResyncRequired,
    /// The requested Workspace scope is unavailable from the current authority.
    ScopeUnavailable,
    /// Unrecognized peer classification; do not infer retry safety.
    Unknown,
}

impl ServiceCode {
    /// Stable recovery classification without reflecting untrusted peer text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Authentication => "UNAUTHENTICATED",
            Self::Forbidden => "FORBIDDEN",
            Self::InvalidInput => "INVALID_INPUT",
            Self::NotFound => "NOT_FOUND",
            Self::Conflict => "ORCHESTRATION_CONFLICT",
            Self::ResyncRequired => "RESYNC_REQUIRED",
            Self::ScopeUnavailable => "WORKSPACE_SCOPE_UNAVAILABLE",
            Self::Unknown => "ORCHESTRATION_REJECTED",
        }
    }
}

/// Client failures retain recovery identity without retaining credentials or
/// untrusted response text. No variant triggers an automatic retry.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ClientError {
    #[error("invalid Orchestration client configuration")]
    Configuration,
    #[error("invalid Orchestration client request")]
    InvalidRequest,
    #[error("Orchestration client admission is full")]
    Capacity,
    #[error("Orchestration query transport failed")]
    Unavailable,
    #[error("Orchestration response violates its bounded protocol")]
    Protocol,
    #[error("Orchestration mutation outcome is unknown; resolve its durable identity")]
    UnknownOutcome { readback: ReadbackTarget },
    #[error("Orchestration mutation requires canonical readback")]
    ReadbackRequired { readback: ReadbackTarget },
    #[error("Orchestration rejected the request")]
    Rejected { status: u16, code: ServiceCode },
}

/// A loopback authority with four simultaneous bounded one-shot exchanges.
/// Credentials are supplied per call, never selected from a host service token.
#[derive(Clone)]
pub struct OrchestrationHttpClient {
    authority: SocketAddr,
    timeout: Duration,
    slots: Arc<Semaphore>,
}

impl fmt::Debug for OrchestrationHttpClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OrchestrationHttpClient")
            .field("authority", &self.authority)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl OrchestrationHttpClient {
    /// Select one trusted loopback endpoint and a deadline capped at five seconds.
    ///
    /// # Errors
    /// Rejects a remote/zero-port endpoint or a zero deadline.
    pub fn new(authority: SocketAddr, timeout: Duration) -> Result<Self, ClientError> {
        if !authority.ip().is_loopback() || authority.port() == 0 || timeout.is_zero() {
            return Err(ClientError::Configuration);
        }
        Ok(Self {
            authority,
            timeout: timeout.min(MAX_TIMEOUT),
            slots: Arc::new(Semaphore::new(CLIENT_CONCURRENCY)),
        })
    }

    /// Create/replay one Profile using the caller's existing credential.
    ///
    /// # Errors
    /// Returns bounded admission, typed rejection or a durable readback identity.
    pub async fn create_profile(
        &self,
        authorization: &HeaderValue,
        request: &AgentProfileCreateRequest,
    ) -> Result<AgentProfileCreateResponse, ClientError> {
        let nonce = canonical_identity(&request.request_id)?;
        let readback = ReadbackTarget::ProfileRequest(nonce);
        let response: AgentProfileCreateResponse = self
            .call(
                authorization,
                Method::POST,
                "/internal/v1/agent-profiles".to_owned(),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if response.profile.request_id != nonce || !valid_profile(&response.profile) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read an exact Profile ID.
    ///
    /// # Errors
    /// Returns typed rejection, bounded transport or response identity failure.
    pub async fn profile(
        &self,
        authorization: &HeaderValue,
        profile_id: Uuid,
    ) -> Result<AgentProfileResponse, ClientError> {
        require_identity(profile_id)?;
        let response: AgentProfileResponse = self
            .call::<_, ()>(
                authorization,
                Method::GET,
                format!("/internal/v1/agent-profiles/{profile_id}"),
                None,
                None,
            )
            .await?;
        if response.profile_id != profile_id || !valid_profile(&response) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Open/replay one caller-bound logical Session.
    ///
    /// # Errors
    /// Returns bounded admission, typed rejection or the exact nonce for readback.
    pub async fn open_session(
        &self,
        authorization: &HeaderValue,
        request: &AgentSessionOpenRequest,
    ) -> Result<AgentSessionOpenResponse, ClientError> {
        let nonce = canonical_identity(&request.request_id)?;
        let readback = ReadbackTarget::SessionRequest(nonce);
        let response: AgentSessionOpenResponse = self
            .call(
                authorization,
                Method::POST,
                "/internal/v1/sessions".to_owned(),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if response.session.request_id != nonce || !valid_session(&response.session) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read an exact caller-owned Session, including terminal state.
    ///
    /// # Errors
    /// Returns typed rejection, bounded transport or response identity failure.
    pub async fn session(
        &self,
        authorization: &HeaderValue,
        session_id: Uuid,
    ) -> Result<AgentSessionResponse, ClientError> {
        require_identity(session_id)?;
        let response: AgentSessionResponse = self
            .call::<_, ()>(
                authorization,
                Method::GET,
                format!("/internal/v1/sessions/{session_id}"),
                None,
                None,
            )
            .await?;
        if response.session_id != session_id || !valid_session(&response) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Read the exact caller-owned Session-open nonce without replaying creation.
    ///
    /// # Errors
    /// Returns invalid identity, bounded transport, typed rejection, or response
    /// correlation/protocol failures.
    pub async fn session_open_readback(
        &self,
        authorization: &HeaderValue,
        request_id: Uuid,
    ) -> Result<Option<AgentSessionResponse>, ClientError> {
        require_identity(request_id)?;
        let response: AgentSessionOpenReadbackResponse = self
            .call::<_, ()>(
                authorization,
                Method::GET,
                format!("/internal/v1/session-open-requests/{request_id}"),
                None,
                None,
            )
            .await?;
        if response.request_id != request_id
            || response
                .session
                .as_ref()
                .is_some_and(|session| session.request_id != request_id || !valid_session(session))
        {
            return Err(ClientError::Protocol);
        }
        Ok(response.session)
    }

    /// Close one Session at its exact revision. A lost reply requires readback.
    ///
    /// # Errors
    /// Returns typed rejection or an uncertain outcome carrying the Session ID.
    pub async fn close_session(
        &self,
        authorization: &HeaderValue,
        session_id: Uuid,
        request: &AgentSessionCloseRequest,
    ) -> Result<AgentSessionResponse, ClientError> {
        require_identity(session_id)?;
        if request.expected_revision <= 0 {
            return Err(ClientError::InvalidRequest);
        }
        let closed_revision = request
            .expected_revision
            .checked_add(1)
            .ok_or(ClientError::InvalidRequest)?;
        let readback = ReadbackTarget::Session(session_id);
        let response: AgentSessionResponse = self
            .call(
                authorization,
                Method::POST,
                format!("/internal/v1/sessions/{session_id}/close"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if response.session_id != session_id
            || !valid_session(&response)
            || response.state != "closed"
            || response.revision != closed_revision
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    async fn call<T: DeserializeOwned, B: Serialize>(
        &self,
        authorization: &HeaderValue,
        method: Method,
        path: String,
        body: Option<&B>,
        readback: Option<ReadbackTarget>,
    ) -> Result<T, ClientError> {
        self.call_with_response_limit(
            authorization,
            method,
            path,
            body,
            readback,
            MAX_RESPONSE_BYTES,
        )
        .await
    }

    async fn call_with_response_limit<T: DeserializeOwned, B: Serialize>(
        &self,
        authorization: &HeaderValue,
        method: Method,
        path: String,
        body: Option<&B>,
        readback: Option<ReadbackTarget>,
        max_response_bytes: usize,
    ) -> Result<T, ClientError> {
        if max_response_bytes == 0 || max_response_bytes > MAX_REVIEW_RESPONSE_BYTES {
            return Err(ClientError::InvalidRequest);
        }
        let value = authorization
            .to_str()
            .map_err(|_| ClientError::InvalidRequest)?;
        if value.len() > heterarchy_orchestration_auth::MAX_MCP_TOKEN_BEARER_BYTES
            || !value.starts_with("Bearer ")
        {
            return Err(ClientError::InvalidRequest);
        }
        let _permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| ClientError::Capacity)?;
        let body = body.map(bounded_json).transpose()?.unwrap_or_default();
        let uncertain = || {
            readback
                .clone()
                .map_or(ClientError::Unavailable, |readback| {
                    ClientError::UnknownOutcome { readback }
                })
        };
        let result = tokio::time::timeout(
            self.timeout,
            self.exchange(authorization, method, path, body, max_response_bytes),
        )
        .await
        .map_err(|_| uncertain())?;
        let (status, body) = result.map_err(|error| {
            if readback.is_some() {
                uncertain()
            } else {
                error
            }
        })?;
        if !status.is_success() {
            return Err(service_error(status, &body, readback));
        }
        let expected_status = status == StatusCode::OK
            || (status == StatusCode::CREATED
                && matches!(
                    readback,
                    Some(
                        ReadbackTarget::ProfileRequest(_)
                            | ReadbackTarget::SessionRequest(_)
                            | ReadbackTarget::ProfileSession { .. }
                    )
                ));
        if !expected_status {
            return Err(readback.clone().map_or(ClientError::Protocol, |readback| {
                ClientError::UnknownOutcome { readback }
            }));
        }
        serde_json::from_slice(&body).map_err(|_| {
            readback.clone().map_or(ClientError::Protocol, |readback| {
                ClientError::UnknownOutcome { readback }
            })
        })
    }

    async fn exchange(
        &self,
        authorization: &HeaderValue,
        method: Method,
        path: String,
        body: Vec<u8>,
        max_response_bytes: usize,
    ) -> Result<(StatusCode, Bytes), ClientError> {
        let socket = tokio::net::TcpStream::connect(self.authority)
            .await
            .map_err(|_| ClientError::Unavailable)?;
        let mut builder = hyper::client::conn::http1::Builder::new();
        builder
            .max_headers(MAX_HEADERS)
            .max_buf_size(MAX_HEADER_BYTES);
        let (mut sender, connection) = builder
            .handshake::<_, Full<Bytes>>(TokioIo::new(socket))
            .await
            .map_err(|_| ClientError::Unavailable)?;
        let mut credential = authorization.clone();
        credential.set_sensitive(true);
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header(HOST, self.authority.to_string())
            .header(AUTHORIZATION, credential)
            .header(CONNECTION, "close")
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, body.len())
            .body(Full::new(Bytes::from(body)))
            .map_err(|_| ClientError::InvalidRequest)?;
        let exchange = async move {
            let response = sender
                .send_request(request)
                .await
                .map_err(|_| ClientError::Unavailable)?;
            if !valid_headers(response.headers()) {
                return Err(ClientError::Protocol);
            }
            let status = response.status();
            let body = Limited::new(response.into_body(), max_response_bytes)
                .collect()
                .await
                .map_err(|_| ClientError::Protocol)?
                .to_bytes();
            Ok((status, body))
        };
        tokio::pin!(connection, exchange);
        // Drive the connection inline: timeout/drop cannot leave an owned
        // transport task behind. No redirects, pool or detached driver exists.
        tokio::select! { result = &mut exchange => result, _connection_result = &mut connection => exchange.await }
    }
}

fn canonical_identity(value: &str) -> Result<Uuid, ClientError> {
    let id = Uuid::parse_str(value).map_err(|_| ClientError::InvalidRequest)?;
    require_identity(id)?;
    if id.to_string() != value {
        return Err(ClientError::InvalidRequest);
    }
    Ok(id)
}

fn bounded_json<T: Serialize>(value: &T) -> Result<Vec<u8>, ClientError> {
    struct BoundedBody(Vec<u8>);
    impl std::io::Write for BoundedBody {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > MAX_REQUEST_BYTES.saturating_sub(self.0.len()) {
                return Err(std::io::Error::other("bounded request exceeded"));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut body = BoundedBody(Vec::new());
    serde_json::to_writer(&mut body, value).map_err(|_| ClientError::InvalidRequest)?;
    Ok(body.0)
}

fn require_identity(value: Uuid) -> Result<(), ClientError> {
    if value.is_nil() {
        Err(ClientError::InvalidRequest)
    } else {
        Ok(())
    }
}

fn valid_profile(value: &AgentProfileResponse) -> bool {
    !value.profile_id.is_nil()
        && !value.request_id.is_nil()
        && value.revision > 0
        && value.provider_family == "chatgpt"
        && value.created_at_ms >= 0
        && value.updated_at_ms >= value.created_at_ms
        && value.last_used_at_ms.is_none_or(|timestamp| timestamp >= 0)
        && matches!(
            value.creation_origin.as_str(),
            "user_manual" | "agent_request" | "imported" | "system_bootstrap"
        )
        && matches!(
            value.lifecycle_state.as_str(),
            "active" | "disabled" | "retired"
        )
}

fn valid_session(value: &AgentSessionResponse) -> bool {
    !value.session_id.is_nil()
        && !value.request_id.is_nil()
        && value.revision > 0
        && matches!(value.state.as_str(), "open" | "closed")
}

fn valid_headers(headers: &HeaderMap) -> bool {
    headers.len() <= MAX_HEADERS
        && headers
            .iter()
            .map(|(name, value)| name.as_str().len() + value.as_bytes().len() + 4)
            .sum::<usize>()
            <= MAX_HEADER_BYTES
        && headers
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                value
                    .split(';')
                    .next()
                    .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("application/json"))
            })
}

#[derive(Deserialize)]
struct FailureBody {
    code: String,
}

fn service_error(status: StatusCode, body: &[u8], readback: Option<ReadbackTarget>) -> ClientError {
    if status == StatusCode::UNAUTHORIZED {
        return ClientError::Rejected {
            status: status.as_u16(),
            code: ServiceCode::Authentication,
        };
    }
    if status == StatusCode::FORBIDDEN {
        return ClientError::Rejected {
            status: status.as_u16(),
            code: ServiceCode::Forbidden,
        };
    }
    let body = serde_json::from_slice::<FailureBody>(body).ok();
    let code = body.as_ref().map(|body| body.code.as_str());
    if code == Some("ORCHESTRATION_CAPACITY") && status == StatusCode::SERVICE_UNAVAILABLE {
        return ClientError::Capacity;
    }
    // This closed code is emitted only by the source proof before the
    // scoped mutation reaches its durable operation. Preserve known rejection.
    if code == Some("WORKSPACE_SCOPE_UNAVAILABLE") && status == StatusCode::SERVICE_UNAVAILABLE {
        return ClientError::Rejected {
            status: status.as_u16(),
            code: ServiceCode::ScopeUnavailable,
        };
    }
    if let Some(readback) = &readback {
        if code == Some("COMMITTED_BUT_UNCONFIRMED") {
            return ClientError::ReadbackRequired {
                readback: readback.clone(),
            };
        }
        if status.is_server_error() || body.is_none() || !status.is_client_error() {
            return ClientError::UnknownOutcome {
                readback: readback.clone(),
            };
        }
    }
    let code = match code {
        Some("INVALID_INPUT" | "INVALID_IDENTITY") => ServiceCode::InvalidInput,
        Some("NOT_FOUND") => ServiceCode::NotFound,
        Some("ORCHESTRATION_CONFLICT" | "STALE_REVISION") => ServiceCode::Conflict,
        Some("RESYNC_REQUIRED") => ServiceCode::ResyncRequired,
        Some("WORKSPACE_SCOPE_UNAVAILABLE") => ServiceCode::ScopeUnavailable,
        _ => ServiceCode::Unknown,
    };
    if code == ServiceCode::Unknown
        && let Some(readback) = readback
    {
        return ClientError::UnknownOutcome { readback };
    }
    ClientError::Rejected {
        status: status.as_u16(),
        code,
    }
}
