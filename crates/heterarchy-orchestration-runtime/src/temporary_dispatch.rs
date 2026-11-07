//! Runtime adapter for Team temporary-dispatch request admission.
//!
//! This boundary records an admission request only. It does not grant consent,
//! reserve capacity, activate work, or authorize execution.

use std::time::{SystemTime, UNIX_EPOCH};

use forge_domain::{TimestampMillis, WorkspaceId};
use heterarchy_orchestration_operations::{
    OperationalActor, OperationsError, TemporaryDispatchAdmissionRequest,
    TemporaryDispatchAdmissionResult, TemporaryDispatchRequestRecord,
    TemporaryDispatchRequestedScope,
};
use uuid::Uuid;

use crate::{OrchestrationRuntime, RuntimeError, teams::participant_actor};

/// Exact original intent for one new temporary-dispatch admission request.
///
/// Agent identities, roles, source evidence and admission timestamps are never
/// supplied here; Runtime derives the authenticated actor's Agent binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporaryDispatchAdmissionSpec {
    /// Exact request identity used for idempotency and recovery.
    pub request_id: Uuid,
    /// Exact Workspace scope.
    pub workspace_id: Uuid,
    /// Receiving Team whose quota and leadership fence apply.
    pub requesting_team_id: Uuid,
    /// Current authenticated actor Activity.
    pub activity_id: Uuid,
    /// Current authenticated actor Session.
    pub session_id: Uuid,
    /// Expected current Team leadership revision.
    pub expected_leadership_revision: u64,
    /// Stable logical participant requested for temporary assistance.
    pub target_participant_id: Uuid,
    /// Bounded request intent, not source-work evidence.
    pub requested_scope: String,
    /// Requested absolute end time; the admission time is server-owned.
    pub requested_end_at_ms: i64,
}

/// Readback retains the original intent while authenticating a current Session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporaryDispatchReadbackSpec {
    /// Exact original request whose receipt is being recovered.
    pub intent: TemporaryDispatchAdmissionSpec,
    /// Current Activity for the same logical actor after a possible rebind.
    pub current_activity_id: Uuid,
    /// Current open Session used for authorization and server-side Agent lookup.
    pub current_session_id: Uuid,
}

impl OrchestrationRuntime {
    /// Admit a new request through the Operations quota and immutable receipt.
    ///
    /// The Operations owner invokes `trusted_now` once inside its IMMEDIATE
    /// transaction after actor, membership, Team authority and payload checks.
    ///
    /// # Errors
    /// Rejects invalid scope/identity, foreign or closed Sessions, stale Team
    /// leadership, quota-state corruption, clock failure, or persistence errors.
    pub fn admit_temporary_dispatch(
        &self,
        spec: &TemporaryDispatchAdmissionSpec,
        principal: forge_domain::AuthenticatedPrincipal,
    ) -> Result<TemporaryDispatchAdmissionResult, RuntimeError> {
        let workspace = WorkspaceId::from_uuid(spec.workspace_id);
        let (actor, participation) =
            participant_actor(workspace, spec.activity_id, spec.session_id)?;
        self.require_participation(participation, principal)?;
        let request = admission_request(spec, actor)?;
        let mut catalog = self.catalog()?;
        catalog
            .admit_temporary_dispatch(&request, trusted_server_time)
            .map_err(RuntimeError::persistence)
    }

    /// Read the exact original receipt as the same logical actor under a
    /// separately authorized current Activity and Session. Current Team
    /// leadership is intentionally not required for receipt recovery.
    ///
    /// # Errors
    /// Rejects an invalid current binding, foreign or closed Session, changed
    /// logical actor, corrupt receipt, or persistence errors.
    pub fn read_temporary_dispatch_request(
        &self,
        spec: &TemporaryDispatchReadbackSpec,
        principal: forge_domain::AuthenticatedPrincipal,
    ) -> Result<Option<TemporaryDispatchRequestRecord>, RuntimeError> {
        let workspace = WorkspaceId::from_uuid(spec.intent.workspace_id);
        let (current_actor, participation) =
            participant_actor(workspace, spec.current_activity_id, spec.current_session_id)?;
        self.require_participation(participation, principal)?;
        let request = admission_request(&spec.intent, current_actor)?;
        let mut catalog = self.catalog()?;
        catalog
            .read_temporary_dispatch_request(&request)
            .map_err(RuntimeError::persistence)
    }
}

fn admission_request(
    spec: &TemporaryDispatchAdmissionSpec,
    actor: OperationalActor,
) -> Result<TemporaryDispatchAdmissionRequest, RuntimeError> {
    let workspace = WorkspaceId::from_uuid(spec.workspace_id);
    let requested_scope = TemporaryDispatchRequestedScope::new(spec.requested_scope.clone())
        .map_err(RuntimeError::persistence)?;
    let requested_end_at =
        TimestampMillis::new(spec.requested_end_at_ms).map_err(|_| RuntimeError::InvalidInput)?;
    TemporaryDispatchAdmissionRequest::new(
        spec.request_id,
        workspace,
        spec.requesting_team_id,
        spec.expected_leadership_revision,
        actor,
        spec.target_participant_id,
        requested_scope,
        requested_end_at,
    )
    .map_err(RuntimeError::persistence)
}

fn trusted_server_time() -> Result<TimestampMillis, OperationsError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| OperationsError::Bounds("temporary dispatch trusted clock"))?;
    let millis = i64::try_from(elapsed.as_millis())
        .map_err(|_| OperationsError::Bounds("temporary dispatch trusted clock"))?;
    TimestampMillis::new(millis)
        .map_err(|_| OperationsError::Bounds("temporary dispatch trusted clock"))
}
