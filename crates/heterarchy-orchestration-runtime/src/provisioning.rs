//! Transport-neutral durable `AgentRequest` intents and historical resolution
//! readback. Provider resolution and effect orchestration have been retired;
//! stored resolution and operation records remain readable for history.

mod admission;
pub use admission::AgentRequestAdmissionSnapshot;

pub mod operations;
pub mod requests;

use uuid::Uuid;

use forge_domain::TimestampMillis;
use heterarchy_orchestration_operations::{
    AgentProvisionOperation, AgentProvisionState, AgentRequestCommand, AgentRequestEffectScope,
    AgentRequestIndependence, AgentRequestIntent, AgentRequestRecord, AgentRequestRequester,
    AgentRequestResolution, AgentRequestResolutionKind, AgentRequestState, OperationsError,
};
use heterarchy_orchestration_sessions::{
    AgentProfileError, AgentProfileId, AgentProfileRuntimeSessionId,
};
use thiserror::Error;

use crate::{OrchestrationRuntime, RuntimeError};

/// Why a durable `AgentRequest` intent submission was refused.
///
/// The backend adapter maps each variant to its own HTTP classification;
/// nothing here collapses to a generic failure.
#[derive(Debug, Error)]
pub enum ProvisioningError {
    /// The command violates a bounded contract (fan-out, text bounds,
    /// identities).
    #[error("provisioning command violates a bounded contract")]
    InvalidInput,
    /// The referenced request/claim/runtime does not exist.
    #[error("provisioning target was not found")]
    NotFound,
    /// Stale coordinator/request revision or a changed exact replay.
    #[error("provisioning state conflicts with the requested transition")]
    Conflict,
    /// The durable observation is incomplete and must be re-read.
    #[error("provisioning observation is incomplete; re-read canonical state")]
    ResyncRequired,
    /// The profile catalog rejected the operation.
    #[error("Agent Profile catalog rejected the provisioning operation: {0}")]
    Profile(#[from] AgentProfileError),
    /// The operations catalog rejected the operation.
    #[error("operations catalog rejected the provisioning operation: {0}")]
    Operations(#[from] OperationsError),
}

impl From<ProvisioningError> for RuntimeError {
    fn from(error: ProvisioningError) -> Self {
        match error {
            ProvisioningError::InvalidInput => Self::InvalidInput,
            ProvisioningError::NotFound => Self::NotFound,
            ProvisioningError::Conflict => Self::Conflict,
            ProvisioningError::ResyncRequired => Self::ResyncRequired,
            ProvisioningError::Profile(inner) => crate::profile_error_to_runtime(inner),
            ProvisioningError::Operations(inner) => Self::from(inner),
        }
    }
}

/// Durable `AgentRequest` submit intent supplied by the requesting coordinator.
#[derive(Debug, Clone)]
pub struct AgentRequestSubmit {
    /// Exact Workspace scope.
    pub workspace_id: Uuid,
    /// Exact Activity scope.
    pub activity_id: Uuid,
    /// Requesting Agent identity.
    pub requester_agent_id: Uuid,
    /// Requesting logical Session.
    pub requester_session_id: Uuid,
    /// Coordinator revision the requester observed (CAS fence).
    pub coordinator_revision: u64,
    /// Exact replay identity of the request.
    pub request_id: Uuid,
    /// Objective text (1..=4096 bytes, domain-validated).
    pub objective: String,
    /// Requested capability (domain-validated).
    pub capability: String,
    /// Requested Agent count (1..=4, domain-validated).
    pub count: u8,
    /// Whether the requested work may write sources.
    pub effect_scope: AgentRequestEffectScope,
    /// Writer-independence requirement.
    pub independence: AgentRequestIndependence,
    /// Reason text (1..=2048 bytes, domain-validated).
    pub reason: String,
    /// Caller-supplied wall-clock milliseconds.
    pub created_at_ms: i64,
}

/// Projection of one durable `AgentRequest`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRequestSubmitSummary {
    /// Exact request identity.
    pub request_id: Uuid,
    /// Workspace scope.
    pub workspace_id: Uuid,
    /// Activity scope.
    pub activity_id: Uuid,
    /// Requested capability.
    pub capability: String,
    /// Requested Agent count.
    pub count: u8,
    /// Durable lifecycle state label.
    pub state: &'static str,
    /// Exact durable request revision, including historical fulfillment.
    pub revision: u64,
    /// True when this call created the request; false on exact replay.
    pub created: bool,
}

/// One resolved slot of a reserved request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolutionSlotSummary {
    /// Slot ordinal (contiguous, 0-based).
    pub slot_ordinal: u8,
    /// Domain resolution kind label (snake case).
    pub kind: &'static str,
    /// Claim identity, when the kind is Profile-backed.
    pub claim_id: Option<Uuid>,
    /// Claimed Profile, when the kind is Profile-backed.
    pub profile_id: Option<Uuid>,
    /// Claimed `RuntimeSession`, when the kind carries one.
    pub runtime_session_id: Option<Uuid>,
    /// `RuntimeSession` revision the resolver observed.
    pub runtime_session_revision: Option<i64>,
}

/// Projection of one durable resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRequestResolutionSummary {
    /// Exact request identity.
    pub request_id: Uuid,
    /// Request Workspace scope.
    pub workspace_id: Uuid,
    /// Request lifecycle state label after this resolution.
    pub request_state: &'static str,
    /// Request revision observed by the committed resolution.
    pub request_revision: u64,
    /// Always false for pure historical readback.
    pub created: bool,
    /// Per-slot selections.
    pub slots: Vec<ResolutionSlotSummary>,
}

/// One historical provision operation. This projection never dispatches work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvisionOperationSummary {
    /// Deterministic operation identity.
    pub operation_id: Uuid,
    /// Slot ordinal.
    pub slot_ordinal: u8,
    /// Durable historical operation state label.
    pub state: &'static str,
    /// Bound historical Profile identity, if one was recorded.
    pub profile_id: Option<Uuid>,
    /// Bound historical `RuntimeSession` identity, if one was recorded.
    pub runtime_session_id: Option<Uuid>,
    /// Always false for this read-only projection.
    pub created: bool,
    /// Durable historical operation revision.
    pub revision: u64,
    /// Always false for this read-only projection.
    pub changed: bool,
}

impl OrchestrationRuntime {
    /// Submit one durable `AgentRequest` intent.
    ///
    /// Exact replays return the original request with `created == false`;
    /// any replay drift conflicts at the durable authority.
    ///
    /// # Errors
    /// Returns [`ProvisioningError`] while preserving whether the failure came
    /// from the Profile catalog or the Operations catalog.
    pub fn submit_agent_request(
        &self,
        command: AgentRequestSubmit,
    ) -> Result<AgentRequestSubmitSummary, ProvisioningError> {
        let agent_request = admission::domain_command(command)?;
        let mut catalog = self.catalog().map_err(|_| {
            ProvisioningError::Operations(OperationsError::Corrupt("operations catalog lock"))
        })?;
        let outcome = catalog.submit_agent_request(&agent_request)?;
        Ok(AgentRequestSubmitSummary::from_record(
            outcome.request(),
            outcome.created(),
        ))
    }

    /// Read the durable resolution of one request, when reserved.
    ///
    /// # Errors
    /// Returns durable catalog failures.
    pub fn agent_request_resolution(
        &self,
        request_id: Uuid,
    ) -> Result<Option<AgentRequestResolutionSummary>, RuntimeError> {
        let catalog = self.catalog()?;
        Ok(into_runtime(catalog.agent_request_resolution(request_id))?
            .as_ref()
            .map(|resolution| AgentRequestResolutionSummary::from_resolution(resolution, false)))
    }

    /// Read at most the request's bounded slot count from historical resolution
    /// rows. This never creates or dispatches provider work.
    ///
    /// # Errors
    /// Preserves durable request/resolution/operation decoding failures.
    pub fn agent_request_provision_operations(
        &self,
        request_id: Uuid,
    ) -> Result<Vec<ProvisionOperationSummary>, RuntimeError> {
        if request_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        let catalog = self.catalog()?;
        let Some(resolution) = into_runtime(catalog.agent_request_resolution(request_id))? else {
            return Ok(Vec::new());
        };
        let mut operations = Vec::with_capacity(resolution.slots().len());
        for slot in resolution.slots() {
            let ordinal = slot.selection().slot_ordinal();
            if let Some(operation) = into_runtime(
                catalog.agent_provision_operation_for_request_slot(request_id, ordinal),
            )? {
                operations.push(ProvisionOperationSummary::from_operation(&operation));
            }
        }
        Ok(operations)
    }
}

/// Convert a domain result into the runtime error vocabulary.
fn into_runtime<T, E: Into<RuntimeError>>(result: Result<T, E>) -> Result<T, RuntimeError> {
    result.map_err(Into::into)
}

fn workspace(value: Uuid) -> Result<forge_domain::WorkspaceId, ProvisioningError> {
    if value.is_nil() {
        return Err(ProvisioningError::InvalidInput);
    }
    Ok(forge_domain::WorkspaceId::from_uuid(value))
}

fn activity(value: Uuid) -> Result<forge_domain::ActivityId, ProvisioningError> {
    if value.is_nil() {
        return Err(ProvisioningError::InvalidInput);
    }
    Ok(forge_domain::ActivityId::from_uuid(value))
}

fn agent(value: Uuid) -> Result<forge_domain::AgentInstanceId, ProvisioningError> {
    if value.is_nil() {
        return Err(ProvisioningError::InvalidInput);
    }
    Ok(forge_domain::AgentInstanceId::from_uuid(value))
}

fn session(value: Uuid) -> Result<forge_domain::AgentSessionId, ProvisioningError> {
    if value.is_nil() {
        return Err(ProvisioningError::InvalidInput);
    }
    Ok(forge_domain::AgentSessionId::from_uuid(value))
}

fn timestamp(value: i64) -> Result<TimestampMillis, ProvisioningError> {
    TimestampMillis::new(value).map_err(|_| ProvisioningError::InvalidInput)
}

impl AgentRequestSubmitSummary {
    fn from_record(record: &AgentRequestRecord, created: bool) -> Self {
        Self {
            request_id: record.request_id(),
            workspace_id: record.workspace_id().into_uuid(),
            activity_id: record.activity_id().into_uuid(),
            capability: record.capability().to_owned(),
            count: record.count(),
            state: request_state_label(record.state()),
            revision: record.revision(),
            created,
        }
    }
}

impl AgentRequestResolutionSummary {
    fn from_resolution(resolution: &AgentRequestResolution, created: bool) -> Self {
        Self {
            request_id: resolution.request().request_id(),
            workspace_id: resolution.request().workspace_id().into_uuid(),
            request_state: request_state_label(resolution.request().state()),
            request_revision: resolution.request().revision(),
            created,
            slots: resolution
                .slots()
                .iter()
                .copied()
                .map(|slot| {
                    let selection = slot.selection();
                    ResolutionSlotSummary {
                        slot_ordinal: selection.slot_ordinal(),
                        kind: resolution_kind_label(selection.kind()),
                        claim_id: selection.claim_id(),
                        profile_id: selection.profile_id().map(AgentProfileId::into_uuid),
                        runtime_session_id: selection
                            .runtime_session_id()
                            .map(AgentProfileRuntimeSessionId::into_uuid),
                        runtime_session_revision: selection.runtime_session_revision(),
                    }
                })
                .collect(),
        }
    }
}

impl ProvisionOperationSummary {
    fn from_operation(operation: &AgentProvisionOperation) -> Self {
        Self {
            operation_id: operation.operation_id(),
            slot_ordinal: operation.slot_ordinal(),
            state: provision_state_label(operation.state()),
            profile_id: operation.profile_id().map(AgentProfileId::into_uuid),
            runtime_session_id: operation
                .runtime_session_id()
                .map(AgentProfileRuntimeSessionId::into_uuid),
            created: false,
            revision: operation.revision(),
            changed: false,
        }
    }
}

/// One bounded page of durable `AgentRequests`, newest first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRequestPageSummary {
    /// Page items.
    pub items: Vec<crate::AgentRequestSummary>,
    /// True when more rows exist beyond the requested limit.
    pub truncated: bool,
}

impl OrchestrationRuntime {
    /// Read one bounded, newest-first page of the Workspace's durable
    /// `AgentRequests`.
    ///
    /// # Errors
    /// Returns out-of-bounds limits, foreign workspaces, or durable catalog
    /// failures.
    pub fn agent_requests(
        &self,
        workspace_id: Uuid,
        limit: u32,
    ) -> Result<AgentRequestPageSummary, RuntimeError> {
        if workspace_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        let catalog = self.catalog()?;
        let page =
            catalog.agent_requests(forge_domain::WorkspaceId::from_uuid(workspace_id), limit)?;
        Ok(AgentRequestPageSummary {
            items: page.items().iter().map(agent_request_summary_for).collect(),
            truncated: page.truncated(),
        })
    }
}

fn agent_request_summary_for(record: &AgentRequestRecord) -> crate::AgentRequestSummary {
    crate::AgentRequestSummary {
        request_id: record.request_id(),
        workspace_id: record.workspace_id().to_string(),
        activity_id: record.activity_id().to_string(),
        objective: record.objective().to_owned(),
        capability: record.capability().to_owned(),
        requested_count: record.count(),
        reason: record.reason().to_owned(),
        state: request_state_label(record.state()),
        created_at_ms: record.created_at().get(),
        revision: record.revision(),
    }
}

/// Deterministic request state labels shared by every transport projection.
#[must_use]
pub const fn request_state_label(state: AgentRequestState) -> &'static str {
    match state {
        AgentRequestState::ResolutionPending => "resolution_pending",
        AgentRequestState::ResolutionReserved => "resolution_reserved",
        AgentRequestState::Fulfilled => "fulfilled",
    }
}

/// Deterministic resolution kind labels shared by every transport projection.
#[must_use]
pub const fn resolution_kind_label(kind: AgentRequestResolutionKind) -> &'static str {
    match kind {
        AgentRequestResolutionKind::ReuseCurrentScope => "reuse_current_scope",
        AgentRequestResolutionKind::ReuseReadyRuntime => "reuse_ready_runtime",
        AgentRequestResolutionKind::WakeExistingRuntime => "wake_existing_runtime",
        AgentRequestResolutionKind::WakeNewRuntime => "wake_new_runtime",
        AgentRequestResolutionKind::ProvisionNew => "provision_new",
    }
}

/// Deterministic provision state labels shared by every transport projection.
#[must_use]
pub const fn provision_state_label(state: AgentProvisionState) -> &'static str {
    match state {
        AgentProvisionState::Prepared => "prepared",
        AgentProvisionState::EffectPending => "effect_pending",
        AgentProvisionState::EffectUnknown => "effect_unknown",
        AgentProvisionState::Observed => "observed",
        AgentProvisionState::Confirmed => "confirmed",
        AgentProvisionState::Failed => "failed",
        AgentProvisionState::Cancelled => "cancelled",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn historical_state_labels_remain_stable() {
        assert_eq!(
            request_state_label(AgentRequestState::ResolutionPending),
            "resolution_pending"
        );
        assert_eq!(
            resolution_kind_label(AgentRequestResolutionKind::ProvisionNew),
            "provision_new"
        );
        assert_eq!(
            provision_state_label(AgentProvisionState::EffectUnknown),
            "effect_unknown"
        );
    }

    #[test]
    fn submit_preserves_intent_without_resolving_or_preparing_provider_work()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("agent-request-intent-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        let workspace_id = Uuid::from_u128(0xA101);
        let request_id = Uuid::from_u128(0xA105);
        let command = AgentRequestSubmit {
            workspace_id,
            activity_id: Uuid::from_u128(0xA102),
            requester_agent_id: Uuid::from_u128(0xA103),
            requester_session_id: Uuid::from_u128(0xA104),
            coordinator_revision: 1,
            request_id,
            objective: "retain the durable AgentRequest intent".to_owned(),
            capability: "verification".to_owned(),
            count: 1,
            effect_scope: AgentRequestEffectScope::ReadOnly,
            independence: AgentRequestIndependence::None,
            reason: "intent-only submit".to_owned(),
            created_at_ms: 1,
        };

        let submitted = runtime.submit_agent_request(command.clone())?;
        assert!(submitted.created);
        assert_eq!(submitted.state, "resolution_pending");
        assert_eq!(submitted.revision, 1);
        let replay = runtime.submit_agent_request(command)?;
        assert!(!replay.created);
        assert_eq!(replay.state, "resolution_pending");
        assert_eq!(replay.revision, submitted.revision);

        let request = runtime
            .agent_request_in_workspace(workspace_id, request_id)?
            .ok_or("submitted request readback missing")?;
        assert_eq!(request.state, "resolution_pending");
        assert_eq!(request.revision, 1);
        assert!(runtime.agent_request_resolution(request_id)?.is_none());
        assert!(
            runtime
                .agent_request_provision_operations(request_id)?
                .is_empty()
        );
        assert!(
            runtime
                .provision_operation(workspace_id, Uuid::from_u128(0xA106))?
                .is_none()
        );

        drop(runtime);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
