//! Fail-closed HTTP response and error mappings for Swarm and `SwarmRuntimeDispatcher` operations.

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use heterarchy_orchestration_operations::{SnapshotValidationError, SwarmError};
use heterarchy_orchestration_runtime::execution_supervisor::ExecutionSupervisorError;
use heterarchy_orchestration_runtime::{RuntimeError, SwarmDispatchError};
use serde::Serialize;

use crate::ApiError;
use crate::wire::swarm::SwarmErrorResponse;

pub(super) enum SwarmReply<T> {
    Success(StatusCode, T),
    DomainRejected(StatusCode, SwarmErrorResponse),
}

impl<T: Serialize> IntoResponse for SwarmReply<T> {
    fn into_response(self) -> Response {
        match self {
            Self::Success(status, payload) => (status, Json(payload)).into_response(),
            Self::DomainRejected(status, err) => (status, Json(err)).into_response(),
        }
    }
}

pub(super) fn map_swarm_error<T>(error: SwarmError) -> Result<SwarmReply<T>, ApiError> {
    let detail = error.to_string();
    let (status, code) = match error {
        SwarmError::WorkerVerifierSpawnForbidden => {
            (StatusCode::FORBIDDEN, "WORKER_VERIFIER_SPAWN_FORBIDDEN")
        }
        SwarmError::WorkerAllMentionForbidden => {
            (StatusCode::FORBIDDEN, "WORKER_ALL_MENTION_FORBIDDEN")
        }
        SwarmError::SybilLineageViolation { .. } => {
            (StatusCode::CONFLICT, "SYBIL_LINEAGE_VIOLATION")
        }
        SwarmError::VerifierMustInherit => (StatusCode::CONFLICT, "VERIFIER_MUST_INHERIT"),
        SwarmError::SpawnDepthExceeded { .. } => (StatusCode::CONFLICT, "SPAWN_DEPTH_EXCEEDED"),
        SwarmError::MaxMembersExceeded { .. } => (StatusCode::CONFLICT, "MAX_MEMBERS_EXCEEDED"),
        SwarmError::TurnBudgetExceeded { .. } => (StatusCode::CONFLICT, "TURN_BUDGET_EXCEEDED"),
        SwarmError::DagNodeHybridCannotSettleDirectly => (
            StatusCode::CONFLICT,
            "DAG_NODE_HYBRID_CANNOT_SETTLE_DIRECTLY",
        ),
        SwarmError::VerdictDiffDigestMismatch => {
            (StatusCode::CONFLICT, "VERDICT_DIFF_DIGEST_MISMATCH")
        }
        SwarmError::ReviewNotAccepted => (StatusCode::CONFLICT, "REVIEW_NOT_ACCEPTED"),
        SwarmError::InvalidStateTransition(_) => (StatusCode::CONFLICT, "INVALID_STATE_TRANSITION"),
        SwarmError::NotFound(_) => return Err(ApiError::NotFound),
        SwarmError::Operations(ops_err) => {
            return Err(ApiError::Runtime(RuntimeError::from(ops_err)));
        }
    };
    Ok(SwarmReply::DomainRejected(
        status,
        SwarmErrorResponse::permanent(code, detail),
    ))
}

pub(super) fn map_swarm_dispatch_error<T>(
    error: SwarmDispatchError,
) -> Result<SwarmReply<T>, ApiError> {
    match error {
        SwarmDispatchError::Swarm(swarm_err) => map_swarm_error(swarm_err),
        SwarmDispatchError::Operations(ops_err) => {
            Err(ApiError::Runtime(RuntimeError::from(ops_err)))
        }
        SwarmDispatchError::ProfileCatalog(profile_err) => {
            Err(ApiError::Runtime(RuntimeError::from(profile_err)))
        }
        SwarmDispatchError::ProfileMissing(_) => Err(ApiError::NotFound),
        SwarmDispatchError::ProfileInactive(_) => Ok(SwarmReply::DomainRejected(
            StatusCode::CONFLICT,
            SwarmErrorResponse::permanent("PROFILE_INACTIVE", error.to_string()),
        )),
        SwarmDispatchError::MemberAlreadyInFlight { .. } => Ok(SwarmReply::DomainRejected(
            StatusCode::CONFLICT,
            SwarmErrorResponse::permanent("MEMBER_ALREADY_IN_FLIGHT", error.to_string()),
        )),
        SwarmDispatchError::MemberNotWakeQueued { .. } => Ok(SwarmReply::DomainRejected(
            StatusCode::CONFLICT,
            SwarmErrorResponse::permanent("MEMBER_NOT_WAKE_QUEUED", error.to_string()),
        )),
        SwarmDispatchError::SnapshotValidation(validation_err) => {
            map_dispatch_snapshot_validation_error(&validation_err)
        }
        SwarmDispatchError::Supervisor(sup_err) => map_dispatch_supervisor_error(sup_err),
        SwarmDispatchError::InvalidSlotConfiguration { .. } => {
            Err(ApiError::Runtime(RuntimeError::InvalidInput))
        }
        SwarmDispatchError::OperationsCatalogPoisoned
        | SwarmDispatchError::ProfileCatalogPoisoned
        | SwarmDispatchError::InflightGuardPoisoned
        | SwarmDispatchError::SemaphoreClosed => Err(ApiError::Unavailable),
        SwarmDispatchError::UnsupportedOutboxFactKind(_)
        | SwarmDispatchError::OutboxDestinationMismatch { .. }
        | SwarmDispatchError::OutboxSequenceRegression { .. }
        | SwarmDispatchError::OutboxDigestMismatch => Ok(SwarmReply::DomainRejected(
            StatusCode::CONFLICT,
            SwarmErrorResponse::permanent("ORCHESTRATION_CONFLICT", error.to_string()),
        )),
    }
}

fn map_dispatch_snapshot_validation_error<T>(
    error: &SnapshotValidationError,
) -> Result<SwarmReply<T>, ApiError> {
    let code = match error {
        SnapshotValidationError::MissingExecutionPreference => "MISSING_EXECUTION_PREFERENCE",
        SnapshotValidationError::ProfileRevisionMismatch => "PROFILE_REVISION_MISMATCH",
        SnapshotValidationError::EngineMismatch => "ENGINE_MISMATCH",
        SnapshotValidationError::ModelMismatch => "MODEL_MISMATCH",
        SnapshotValidationError::ReasoningMismatch => "REASONING_MISMATCH",
        SnapshotValidationError::RolePromptMismatch => "ROLE_PROMPT_MISMATCH",
        SnapshotValidationError::IncompatibleEngineModel => "INCOMPATIBLE_ENGINE_MODEL",
        SnapshotValidationError::ForbiddenSsdBuildWorktree => "FORBIDDEN_SSD_BUILD_WORKTREE",
        SnapshotValidationError::InvalidWorktreePath(_) => "INVALID_WORKTREE_PATH",
        SnapshotValidationError::InvalidIdentity(_)
        | SnapshotValidationError::InvalidGrantWindow
        | SnapshotValidationError::InvalidTimeout(_) => {
            return Err(ApiError::Runtime(RuntimeError::InvalidInput));
        }
    };
    Ok(SwarmReply::DomainRejected(
        StatusCode::CONFLICT,
        SwarmErrorResponse::permanent(code, error.to_string()),
    ))
}

fn map_dispatch_supervisor_error<T>(
    error: ExecutionSupervisorError,
) -> Result<SwarmReply<T>, ApiError> {
    match error {
        ExecutionSupervisorError::ForbiddenSsdBuildWorktree(_) => Ok(SwarmReply::DomainRejected(
            StatusCode::CONFLICT,
            SwarmErrorResponse::permanent(
                "FORBIDDEN_SSD_BUILD_WORKTREE",
                "Swarm worktrees under /Volumes/256.SSD/RustBuild are strictly forbidden",
            ),
        )),
        ExecutionSupervisorError::InvalidWorktreePath(_)
        | ExecutionSupervisorError::MissingWorktreeDirectory(_) => Ok(SwarmReply::DomainRejected(
            StatusCode::CONFLICT,
            SwarmErrorResponse::permanent(
                "INVALID_WORKTREE_PATH",
                "Swarm member worktree path is invalid or missing on disk",
            ),
        )),
        ExecutionSupervisorError::InvalidExecutablePath(_)
        | ExecutionSupervisorError::ForbiddenSsdBuildExecutable(_) => {
            Ok(SwarmReply::DomainRejected(
                StatusCode::CONFLICT,
                SwarmErrorResponse::permanent(
                    "INVALID_CLI_EXECUTABLE",
                    "Configured CLI executable path violates launch hygiene rules",
                ),
            ))
        }
        ExecutionSupervisorError::AttemptNotFound(_) => Err(ApiError::NotFound),
        ExecutionSupervisorError::AttemptNotPrepared(_) => Ok(SwarmReply::DomainRejected(
            StatusCode::CONFLICT,
            SwarmErrorResponse::permanent(
                "INVALID_STATE_TRANSITION",
                "Execution attempt is not in Prepared state",
            ),
        )),
        ExecutionSupervisorError::InvalidCliArgument { .. }
        | ExecutionSupervisorError::InvalidExecutionContext(_)
        | ExecutionSupervisorError::InvalidConcurrencyLimit
        | ExecutionSupervisorError::InvalidOwnershipIdentity(_) => {
            Err(ApiError::Runtime(RuntimeError::InvalidInput))
        }
        ExecutionSupervisorError::Operations(ops_err) => {
            Err(ApiError::Runtime(RuntimeError::from(*ops_err)))
        }
        ExecutionSupervisorError::CatalogPoisoned
        | ExecutionSupervisorError::ConcurrencyBridgeClosed
        | ExecutionSupervisorError::BlockingTaskJoinFailed
        | ExecutionSupervisorError::ClockBeforeUnixEpoch
        | ExecutionSupervisorError::BootIdUnavailable(_)
        | ExecutionSupervisorError::ProcessGroupNotIsolated { .. }
        | ExecutionSupervisorError::ProcessProbeFailed(_)
        | ExecutionSupervisorError::ProcessProbeParse(_)
        | ExecutionSupervisorError::OwnershipMismatch { .. }
        | ExecutionSupervisorError::KillCommandFailed { .. }
        | ExecutionSupervisorError::GroupTerminationUnconfirmed { .. }
        | ExecutionSupervisorError::Io(_) => Err(ApiError::Unavailable),
    }
}
