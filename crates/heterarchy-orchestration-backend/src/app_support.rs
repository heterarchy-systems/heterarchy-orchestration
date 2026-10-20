//! Shared HTTP adapter support for the authority lanes (leadership,
//! provisioning).
//!
//! Owns the one transport error contract those adapters project onto: the
//! backend `ErrorResponse` wire shape with semantic codes, retryability, and
//! the committed-but-unconfirmed distinction the durable authorities use for
//! uncertain outcomes. Also owns the adapter-side clock, matching the MCP
//! handlers' `current_timestamp_millis` role.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use heterarchy_orchestration_operations::OperationsError;
use heterarchy_orchestration_runtime::RuntimeError;
use uuid::Uuid;

/// A session header is a selector, never an authenticated identity.
pub(crate) fn session_selector(
    headers: &axum::http::HeaderMap,
) -> Result<Option<Uuid>, crate::ApiError> {
    headers
        .get("x-orchestration-session-id")
        .map(|header| {
            header
                .to_str()
                .map_err(|_| crate::ApiError::InvalidIdentity)
                .and_then(crate::canonical_uuid)
        })
        .transpose()
}

/// Optional exact Workspace scope for resource-addressed queries.
pub(crate) fn workspace_selector(
    headers: &axum::http::HeaderMap,
) -> Result<Option<Uuid>, crate::ApiError> {
    headers
        .get("x-orchestration-workspace-id")
        .map(|header| {
            header
                .to_str()
                .map_err(|_| crate::ApiError::InvalidIdentity)
                .and_then(crate::canonical_uuid)
        })
        .transpose()
}

/// Validate routing/selector syntax only. These fields never grant permission.
pub(crate) fn validate_workspace_reference(
    scope: &crate::wire::WorkspaceQueryScope,
    session: Option<Uuid>,
) -> Result<(), crate::ApiError> {
    if scope.workspace_id.is_nil()
        || scope.location_id.is_nil()
        || scope.location_revision < 1
        || scope.workspace_revision < 1
        || scope.locator.is_empty()
        || scope.locator.len() > 4096
        || scope.locator.chars().any(char::is_control)
        || session.is_some_and(|id| id.is_nil())
    {
        return Err(crate::ApiError::InvalidIdentity);
    }
    Ok(())
}

/// Resolve the adapter wall clock once per operation.
///
/// # Errors
/// Returns `None`-style failure as a typed unavailable error when the
/// process clock cannot produce a monotonic-enough timestamp.
pub fn now_millis() -> Result<i64, LaneError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .map_err(|_| LaneError::ClockUnavailable)
}

/// Transport error for the authority lane adapters.
#[derive(Debug)]
pub enum LaneError {
    /// Canonical-UUID or enum-text validation failed.
    InvalidIdentity,
    /// The backend's bounded catalog admission is saturated.
    Capacity,
    /// Adapter clock unavailable.
    ClockUnavailable,
    /// Exact Forge Coordinator proof is unavailable before request admission.
    CoordinatorUnavailable,
    /// Exact Forge source scope cannot be revalidated.
    WorkspaceScopeUnavailable,
    /// A source binding outcome requires exact durable readback before resume.
    SourceBindingUnconfirmed,
    /// The runtime/catalog operation failed with its typed error.
    Runtime(RuntimeError),
}

impl From<RuntimeError> for LaneError {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}

impl From<crate::ApiError> for LaneError {
    fn from(error: crate::ApiError) -> Self {
        match error {
            crate::ApiError::InvalidIdentity => Self::InvalidIdentity,
            crate::ApiError::NotFound => Self::Runtime(RuntimeError::NotFound),
            crate::ApiError::Capacity => Self::Capacity,
            crate::ApiError::Unavailable => Self::Runtime(RuntimeError::CatalogOwnerPoisoned),
            crate::ApiError::CoordinatorUnavailable => Self::CoordinatorUnavailable,
            crate::ApiError::WorkspaceScopeUnavailable => Self::WorkspaceScopeUnavailable,
            crate::ApiError::SourceBindingUnconfirmed => Self::SourceBindingUnconfirmed,
            crate::ApiError::Runtime(inner) => Self::Runtime(inner),
        }
    }
}

impl LaneError {
    const INVALID_IDENTITY: &'static str = "INVALID_IDENTITY";
    const INVALID_INPUT: &'static str = "INVALID_INPUT";
    const NOT_FOUND: &'static str = "NOT_FOUND";
    const CONFLICT: &'static str = "ORCHESTRATION_CONFLICT";
    const RESYNC_REQUIRED: &'static str = "RESYNC_REQUIRED";
    const OWNER_ALREADY_ACTIVE: &'static str = "OWNER_ALREADY_ACTIVE";
    const AUTHORITY_CONFLICT: &'static str = "AUTHORITY_CONFLICT";
    const BROKER_UNAVAILABLE: &'static str = "BROKER_UNAVAILABLE";
    const COMMITTED_BUT_UNCONFIRMED: &'static str = "COMMITTED_BUT_UNCONFIRMED";
    const ORCHESTRATION_CAPACITY: &'static str = "ORCHESTRATION_CAPACITY";
    const ORCHESTRATION_UNAVAILABLE: &'static str = "ORCHESTRATION_UNAVAILABLE";

    fn classification(&self) -> (StatusCode, &'static str, &'static str, bool) {
        match self {
            Self::CoordinatorUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "COORDINATOR_AUTHORITY_UNAVAILABLE",
                "The exact Forge Coordinator authority is unavailable; no AgentRequest was admitted.",
                true,
            ),
            Self::WorkspaceScopeUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "WORKSPACE_SCOPE_UNAVAILABLE",
                "The exact Forge Workspace scope authority is unavailable.",
                true,
            ),
            Self::InvalidIdentity => (
                StatusCode::BAD_REQUEST,
                Self::INVALID_IDENTITY,
                "The supplied identity is invalid.",
                false,
            ),
            Self::Capacity => (
                StatusCode::SERVICE_UNAVAILABLE,
                Self::ORCHESTRATION_CAPACITY,
                "The Orchestration backend has reached its bounded work capacity.",
                true,
            ),
            Self::ClockUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                Self::ORCHESTRATION_UNAVAILABLE,
                "The Orchestration adapter clock is unavailable.",
                true,
            ),
            Self::SourceBindingUnconfirmed => (
                StatusCode::SERVICE_UNAVAILABLE,
                Self::COMMITTED_BUT_UNCONFIRMED,
                "Read the exact durable subject and source receipt before resuming.",
                false,
            ),
            Self::Runtime(error) => runtime_classification(error),
        }
    }
}

/// Shared Runtime HTTP error projection for all Backend adapters.
pub(crate) fn runtime_error_response(error: &RuntimeError) -> (StatusCode, crate::ErrorResponse) {
    let (status, code, message, retryable) = runtime_classification(error);
    (
        status,
        crate::ErrorResponse {
            code,
            message,
            retryable,
        },
    )
}

/// Preserve the durable authorities' semantic distinctions instead of
/// collapsing them into one 500.
fn runtime_classification(error: &RuntimeError) -> (StatusCode, &'static str, &'static str, bool) {
    match error {
        RuntimeError::Chat(error) => chat_classification(error),
        RuntimeError::ProfileCatalog(heterarchy_orchestration_sessions::AgentProfileError::ProfileMutationCommitUnconfirmed { .. }) => (
            StatusCode::CONFLICT, LaneError::COMMITTED_BUT_UNCONFIRMED,
            "Read the same Profile mutation receipt before retrying.", false,
        ),
        RuntimeError::SourceAuthorityUnavailable => (StatusCode::SERVICE_UNAVAILABLE,"WORKSPACE_SCOPE_UNAVAILABLE","The exact source scope authority is unavailable.",true),
        RuntimeError::Forbidden => (
            StatusCode::FORBIDDEN,
            "FORBIDDEN",
            "The caller is not authorized for this operation.",
            false,
        ),
        RuntimeError::InvalidInput => (
            StatusCode::BAD_REQUEST,
            LaneError::INVALID_INPUT,
            "The request exceeds a bounded Orchestration contract.",
            false,
        ),
        RuntimeError::NotFound => (
            StatusCode::NOT_FOUND,
            LaneError::NOT_FOUND,
            "The requested Orchestration resource was not found.",
            false,
        ),
        RuntimeError::Conflict => (
            StatusCode::CONFLICT,
            LaneError::CONFLICT,
            "The request conflicts with current Orchestration state.",
            false,
        ),
        RuntimeError::ResyncRequired => (
            StatusCode::CONFLICT,
            LaneError::RESYNC_REQUIRED,
            "The caller must refresh canonical Orchestration state.",
            true,
        ),
        RuntimeError::BindingRejectionTargetUnsupported => (
            StatusCode::CONFLICT,
            "UNSUPPORTED_BINDING_REJECTION_TARGET",
            "Only normal committed registrations support terminal binding rejection settlement.",
            false,
        ),
        RuntimeError::OwnerAlreadyActive(_) => (
            StatusCode::CONFLICT,
            LaneError::OWNER_ALREADY_ACTIVE,
            "Another mutable Orchestration runtime already owns this data-root.",
            false,
        ),
        RuntimeError::BrokerOwner(source) => broker_owner_classification(source),
        RuntimeError::Persistence(source) => {
            if operations_commit_unconfirmed(source) {
                (
                    StatusCode::CONFLICT,
                    LaneError::COMMITTED_BUT_UNCONFIRMED,
                    "The commit outcome is unconfirmed; read back the same identity before retrying.",
                    false,
                )
            } else {
                match source.as_ref() {
                    OperationsError::Conflict(_) => runtime_classification(&RuntimeError::Conflict),
                    OperationsError::Bounds(_) => runtime_classification(&RuntimeError::InvalidInput),
                    OperationsError::ResyncRequired(_) => runtime_classification(&RuntimeError::ResyncRequired),
                    _ => persistence_unavailable(),
                }
            }
        }
        RuntimeError::InvalidDataRoot
        | RuntimeError::CorruptDestinationIdentity
        | RuntimeError::DestinationIdentityConflict
        | RuntimeError::CatalogOwnerPoisoned
        | RuntimeError::ProfileCatalog(_)
        | RuntimeError::SessionCatalog(_)
        | RuntimeError::Io(_) => persistence_unavailable(),
    }
}

fn chat_classification(
    error: &heterarchy_orchestration_runtime::chat::ChatError,
) -> (StatusCode, &'static str, &'static str, bool) {
    use heterarchy_orchestration_runtime::chat::ChatError;
    match error {
        ChatError::Identity
        | ChatError::BodyLimit
        | ChatError::AdmissionRejected(OperationsError::Bounds(_)) => {
            runtime_classification(&RuntimeError::InvalidInput)
        }
        ChatError::TargetChanged
        | ChatError::ReplayConflict
        | ChatError::AdmissionRejected(OperationsError::Conflict(_)) => {
            runtime_classification(&RuntimeError::Conflict)
        }
        ChatError::Runtime(error) => runtime_classification(error),
        ChatError::Operations(error) if operations_commit_unconfirmed(error) => (
            StatusCode::CONFLICT,
            LaneError::COMMITTED_BUT_UNCONFIRMED,
            "Read the exact immutable message intent before resuming.",
            false,
        ),
        ChatError::AdmissionRejected(_)
        | ChatError::Operations(_)
        | ChatError::Profile(_)
        | ChatError::CatalogLock => persistence_unavailable(),
    }
}

fn broker_owner_classification(
    source: &heterarchy_orchestration_operations::BrokerOwnerAuthorityError,
) -> (StatusCode, &'static str, &'static str, bool) {
    use heterarchy_orchestration_operations::BrokerOwnerAuthorityError as Owner;
    match source {
        Owner::Stale | Owner::Rejected | Owner::LocalOwnerBusy => (
            StatusCode::CONFLICT,
            LaneError::AUTHORITY_CONFLICT,
            "The Broker coordination authority rejected the mutation for this owner.",
            false,
        ),
        Owner::Unconfirmed | Owner::InvalidConfiguration => (
            StatusCode::SERVICE_UNAVAILABLE,
            LaneError::BROKER_UNAVAILABLE,
            "The Broker coordination authority is unavailable.",
            true,
        ),
        Owner::LocalState(_) => persistence_unavailable(),
    }
}

fn persistence_unavailable() -> (StatusCode, &'static str, &'static str, bool) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        LaneError::ORCHESTRATION_UNAVAILABLE,
        "Orchestration durable authority is unavailable.",
        true,
    )
}

/// The typed uncertain-commit family both durable authorities surface.
fn operations_commit_unconfirmed(source: &OperationsError) -> bool {
    matches!(
        source,
        OperationsError::CommitUnconfirmed { .. }
            | OperationsError::OperationCommitUnconfirmed { .. }
            | OperationsError::NodeTeamBindingCommitUnconfirmed { .. }
            | OperationsError::PresenceCommitUnconfirmed { .. }
            | OperationsError::InboxCommitUnconfirmed { .. }
            | OperationsError::LeadershipCommitUnconfirmed { .. }
            | OperationsError::LeadershipFenceUnconfirmed { .. }
            | OperationsError::AgentRequestCommitUnconfirmed { .. }
            | OperationsError::AgentProvisionCommitUnconfirmed { .. }
            | OperationsError::BindingRejectionCommitUnconfirmed { .. }
    )
}

impl IntoResponse for LaneError {
    fn into_response(self) -> Response {
        let (status, code, message, retryable) = self.classification();
        (
            status,
            Json(crate::ErrorResponse {
                code,
                message,
                retryable,
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use heterarchy_orchestration_operations::OperationsCatalog;
    use http_body_util::BodyExt;

    #[test]
    fn chat_lookup_failure_is_not_definite_append_rejection() {
        use heterarchy_orchestration_runtime::chat::ChatError;
        let lookup = RuntimeError::Chat(Box::new(ChatError::Operations(
            OperationsError::Conflict("receipt lookup"),
        )));
        let admission = RuntimeError::Chat(Box::new(ChatError::AdmissionRejected(
            OperationsError::Conflict("append"),
        )));
        let (status, body) = runtime_error_response(&lookup);
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body.code, "ORCHESTRATION_UNAVAILABLE");
        let (status, body) = runtime_error_response(&admission);
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body.code, "ORCHESTRATION_CONFLICT");
        assert!(!body.retryable);
    }

    #[test]
    fn replacement_binding_rejection_target_is_explicitly_unsupported() {
        let (status, body) =
            runtime_error_response(&RuntimeError::BindingRejectionTargetUnsupported);
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body.code, "UNSUPPORTED_BINDING_REJECTION_TARGET");
        assert!(!body.retryable);
    }

    fn corrupt_catalog_error() -> Result<OperationsError, Box<dyn std::error::Error>> {
        let root =
            std::env::temp_dir().join(format!("participation-commit-error-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let path = root.join("invalid.sqlite3");
        std::fs::write(&path, b"invalid sqlite header")?;
        let error = OperationsCatalog::open(&path, Uuid::now_v7())
            .err()
            .ok_or("invalid catalog unexpectedly opened")?;
        std::fs::remove_dir_all(root)?;
        Ok(error)
    }

    #[tokio::test]
    async fn uncertain_operations_commit_never_advertises_blind_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        for kind in 0..6 {
            let OperationsError::Sqlite(source) = corrupt_catalog_error()? else {
                return Err("expected SQLite failure fixture".into());
            };
            let failure = match kind {
                0 => OperationsError::CommitUnconfirmed {
                    migration_id: Uuid::from_u128(4),
                    source,
                },
                1 => OperationsError::PresenceCommitUnconfirmed {
                    workspace_id: Uuid::from_u128(1),
                    participant_id: Uuid::from_u128(2),
                    source,
                },
                2 => OperationsError::InboxCommitUnconfirmed {
                    workspace_id: Uuid::from_u128(1),
                    participant_id: Uuid::from_u128(2),
                    event_id: Uuid::from_u128(3),
                    source,
                },
                3 => OperationsError::OperationCommitUnconfirmed {
                    workspace_id: forge_domain::WorkspaceId::from_uuid(Uuid::from_u128(1)),
                    request_id: Uuid::from_u128(3),
                    source,
                },
                4 => OperationsError::BindingRejectionCommitUnconfirmed {
                    request_id: Uuid::from_u128(5),
                    source,
                },
                _ => OperationsError::NodeTeamBindingCommitUnconfirmed {
                    scope: Box::new(
                        heterarchy_orchestration_operations::NodeTeamBindingScope::new(
                            forge_domain::WorkspaceId::from_uuid(Uuid::from_u128(1)),
                            forge_domain::ActivityId::from_uuid(Uuid::from_u128(2)),
                            forge_domain::GoalId::from_uuid(Uuid::from_u128(3)),
                            forge_domain::TimestampMillis::new(1)?,
                            forge_domain::DagRevisionNumber::new(1)?,
                            forge_domain::GoalRevision::new(1)?,
                            forge_domain::DagNodeId::from_uuid(Uuid::from_u128(4)),
                        )?,
                    ),
                    source,
                },
            };
            let error = RuntimeError::persistence(failure);
            let (status, body) = crate::runtime_error_response(&error);
            assert_eq!(status, StatusCode::CONFLICT);
            assert_eq!(body.code, "COMMITTED_BUT_UNCONFIRMED");
            assert!(!body.retryable);
            let response = LaneError::Runtime(error).into_response();
            assert_eq!(response.status(), StatusCode::CONFLICT);
            let bytes = response.into_body().collect().await?.to_bytes();
            let value: serde_json::Value = serde_json::from_slice(&bytes)?;
            assert_eq!(value["code"], "COMMITTED_BUT_UNCONFIRMED");
            assert_eq!(value["retryable"], false);
        }
        Ok(())
    }
}
