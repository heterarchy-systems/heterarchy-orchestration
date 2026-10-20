//! Transport-neutral `AgentRequest` → resolution → provision-target
//! application operations.
//!
//! The MCP `agent_request` tool used to own this orchestration across the
//! Operations and Agent Profile catalogs. This module moves that application
//! logic next to the runtime that owns both catalogs, with the durable
//! semantics preserved exactly:
//!
//! * `AgentRequestState` stays `ResolutionPending` → `ResolutionReserved` →
//!   `Fulfilled`; this layer never invents a transport state machine.
//! * Resolution kinds stay the domain five (`ReuseCurrentScope`,
//!   `ReuseReadyRuntime`, `WakeExistingRuntime`, `WakeNewRuntime`,
//!   `ProvisionNew`) and are only projected to labels.
//! * Claim/profile/runtime identities are derived with the exact MCP blake3
//!   domains, so both transports produce identical durable rows and replays.
//! * External provider effects stay behind the durable journal: this module
//!   only prepares and binds intent (`Prepared`); release (`EffectPending`)
//!   remains the exclusive grant of `next_agent_provision_dispatch`, and
//!   success is only ever recorded by the confirmed readback path.
//!
//! Confirmed participation composes the canonical logical Session and member
//! owners through the participation application operations.

mod admission;
pub use admission::{AgentRequestAdmissionSnapshot, AgentRequestAdmissionStage};

pub mod operations;
pub mod participation;
pub mod requests;

use uuid::Uuid;

use forge_domain::TimestampMillis;
use heterarchy_orchestration_operations::{
    AgentProvisionState, AgentRequestCommand, AgentRequestEffectScope, AgentRequestIndependence,
    AgentRequestIntent, AgentRequestRecord, AgentRequestRequester, AgentRequestResolution,
    AgentRequestResolutionKind, AgentRequestResolutionSelection, AgentRequestState,
    CommitAgentRequestResolution, OperationsCatalog, OperationsError, WorkspaceProjectReference,
};
use heterarchy_orchestration_policy::{
    AgentResolverActionKind, AgentResolverAvailability, AgentResolverCandidate,
    evaluate_agent_resolver,
};
use heterarchy_orchestration_sessions::{
    AgentProfileCatalog, AgentProfileCreationOrigin, AgentProfileError, AgentProfileId,
    AgentProfileProviderFamily, AgentProfileResolutionClaimCommand,
    AgentProfileResolutionClaimKind, AgentProfileResolutionClaimTarget,
    AgentProfileResolverAvailability, AgentProfileRuntimeSessionId,
    AgentProfileRuntimeSessionState, CreateAgentProfile, CreateAgentProfileRuntimeSession,
};
use thiserror::Error;

use crate::{OrchestrationRuntime, RuntimeError};

/// Policy revision this resolver generation commits under; bump only with a
/// reviewed semantics change.
pub const AGENT_REQUEST_RESOLUTION_POLICY_REVISION: u64 = 1;

const AGENT_REQUEST_RESOLUTION_CLAIM_DOMAIN: &[u8] =
    b"heterarchy.orchestration.agent-request.resolution-claim.v1\0";
const AGENT_PROVISION_PROFILE_ID_DOMAIN: &[u8] =
    b"heterarchy.orchestration.agent-provision.profile-id.v1\0";
const AGENT_PROVISION_PROFILE_REQUEST_DOMAIN: &[u8] =
    b"heterarchy.orchestration.agent-provision.profile-request.v1\0";
const AGENT_PROVISION_RUNTIME_ID_DOMAIN: &[u8] =
    b"heterarchy.orchestration.agent-provision.runtime-id.v1\0";
const AGENT_PROVISION_RUNTIME_REQUEST_DOMAIN: &[u8] =
    b"heterarchy.orchestration.agent-provision.runtime-request.v1\0";

/// Deterministic claim identity for one request slot, byte-identical to the
/// MCP resolver so both transports reserve the same durable claim rows.
#[must_use]
pub fn derived_resolution_claim_id(request_id: Uuid, slot_ordinal: u8) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(AGENT_REQUEST_RESOLUTION_CLAIM_DOMAIN);
    hasher.update(request_id.as_bytes());
    hasher.update(&[slot_ordinal]);
    stamped_uuid(hasher.finalize().as_bytes())
}

fn derived_provision_uuid(domain: &[u8], operation_id: Uuid) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(operation_id.as_bytes());
    stamped_uuid(hasher.finalize().as_bytes())
}

fn stamped_uuid(digest: &[u8]) -> Uuid {
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

/// Why a provisioning application operation was refused.
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
    /// Exact request revision (fulfillment moves it to 3).
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
    /// True when this call committed the resolution; false on replay.
    pub created: bool,
    /// Per-slot selections.
    pub slots: Vec<ResolutionSlotSummary>,
}

/// One prepared (intent-only) provision operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvisionOperationSummary {
    /// Deterministic operation identity.
    pub operation_id: Uuid,
    /// Slot ordinal.
    pub slot_ordinal: u8,
    /// Durable operation state label (`prepared` until dispatch releases it).
    pub state: &'static str,
    /// Bound Profile, when the target has been prepared.
    pub profile_id: Option<Uuid>,
    /// Bound `RuntimeSession`, when the target has been prepared.
    pub runtime_session_id: Option<Uuid>,
    /// True when this call created the durable provision operation.
    pub created: bool,
    /// Durable operation revision after target preparation.
    pub revision: u64,
    /// True when this call changed durable state (creation or target binding).
    pub changed: bool,
    /// Frozen provider Project route; legacy operations retain no provenance.
    pub project: Option<WorkspaceProjectReference>,
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
        let mut catalog = self.provisioning_catalog()?;
        let outcome = catalog.submit_agent_request(&agent_request)?;
        Ok(AgentRequestSubmitSummary::from_record(
            outcome.request(),
            outcome.created(),
        ))
    }

    /// Reserve the resolution for one Pending request: observe current-scope
    /// Profiles, evaluate the deterministic resolver policy, reserve Profile
    /// claims (all-or-nothing), and commit the exact slot selections.
    ///
    /// Already-reserved or fulfilled requests return their durable resolution
    /// unchanged, so repeated calls are safe.
    ///
    /// # Errors
    /// Returns [`ProvisioningError`] while preserving Profile/Operations
    /// origin, including incomplete-observation resynchronization.
    pub fn reserve_agent_request_resolution(
        &self,
        request_id: Uuid,
        claimed_at_ms: i64,
        reserved_at_ms: i64,
    ) -> Result<AgentRequestResolutionSummary, ProvisioningError> {
        let claimed_at = timestamp(claimed_at_ms)?;
        let reserved_at = timestamp(reserved_at_ms)?;
        let (request, current_scope) = {
            let catalog = self.provisioning_catalog()?;
            let request = catalog
                .agent_request(request_id)?
                .ok_or(ProvisioningError::NotFound)?;
            if request.state() != AgentRequestState::ResolutionPending {
                let existing = catalog.agent_request_resolution(request_id)?.ok_or(
                    OperationsError::Corrupt("reserved AgentRequest is missing resolver rows"),
                )?;
                return Ok(AgentRequestResolutionSummary::from_resolution(
                    &existing, false,
                ));
            }
            let current_scope = catalog.agent_request_current_scope_profiles(&request)?;
            if current_scope.truncated() {
                // The observation this decision would be based on is incomplete;
                // the caller must re-read canonical state instead of reserving
                // against a truncated candidate set.
                return Err(ProvisioningError::ResyncRequired);
            }
            (request, current_scope)
        };
        let selections = {
            let mut profiles = self.provisioning_profile_catalog()?;
            resolve_and_claim_profiles(
                &mut profiles,
                request_id,
                request.count(),
                request.capability(),
                current_scope.profiles(),
                claimed_at,
            )?
        };
        let commit = CommitAgentRequestResolution::new(
            request_id,
            request.revision(),
            AGENT_REQUEST_RESOLUTION_POLICY_REVISION,
            selections,
            reserved_at,
        )?;
        let outcome = self
            .provisioning_catalog()?
            .commit_agent_request_resolution(&commit)?;
        Ok(AgentRequestResolutionSummary::from_resolution(
            outcome.resolution(),
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

    /// Read at most the request's bounded slot count, including partially
    /// prepared operations. This never prepares or dispatches provider work.
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
                operations.push(ProvisionOperationSummary::from_plan(&ProvisionTargetPlan {
                    prepared_revision: operation.revision(),
                    operation,
                    expected_runtime_revision: None,
                    created: false,
                }));
            }
        }
        Ok(operations)
    }

    /// Prepare the external-effect intent for every slot whose resolution
    /// kind carries a provider effect, and bind each Profile/RuntimeSession
    /// target. This only commits durable `Prepared` intent and target
    /// bindings; releasing the effect stays with
    /// `next_agent_provision_dispatch`, and confirmation stays with the
    /// confirmed-readback path.
    ///
    /// # Errors
    /// Returns [`ProvisioningError`] while preserving Profile/Operations
    /// origin, including partial-commit readback and stale target fences.
    pub fn prepare_agent_request_effect_targets(
        &self,
        request_id: Uuid,
        prepared_at_ms: i64,
        bound_at_ms: i64,
    ) -> Result<Vec<ProvisionOperationSummary>, ProvisioningError> {
        let prepared_at = timestamp(prepared_at_ms)?;
        let bound_at = timestamp(bound_at_ms)?;
        let (capability, effect_slots) = {
            let catalog = self.provisioning_catalog()?;
            let resolution = catalog
                .agent_request_resolution(request_id)?
                .ok_or(ProvisioningError::NotFound)?;
            let capability = resolution.request().capability().to_owned();
            let effect_slots = resolution
                .slots()
                .iter()
                .copied()
                .filter(|slot| {
                    matches!(
                        slot.selection().kind(),
                        AgentRequestResolutionKind::ReuseReadyRuntime
                            | AgentRequestResolutionKind::WakeExistingRuntime
                            | AgentRequestResolutionKind::WakeNewRuntime
                            | AgentRequestResolutionKind::ProvisionNew
                    )
                })
                .map(|slot| {
                    (
                        slot.selection().slot_ordinal(),
                        slot.selection().runtime_session_revision(),
                    )
                })
                .collect::<Vec<_>>();
            (capability, effect_slots)
        };
        if effect_slots.is_empty() {
            return Ok(Vec::new());
        }
        let mut plans = Vec::with_capacity(effect_slots.len());
        {
            let mut catalog = self.provisioning_catalog()?;
            for (slot_ordinal, expected_runtime_revision) in effect_slots {
                let outcome = catalog.prepare_agent_provision_operation(
                    request_id,
                    slot_ordinal,
                    prepared_at,
                )?;
                plans.push(ProvisionTargetPlan {
                    operation: outcome.operation().clone(),
                    expected_runtime_revision,
                    created: outcome.created(),
                    prepared_revision: outcome.operation().revision(),
                });
            }
        }
        let bindings = {
            let mut profiles = self.provisioning_profile_catalog()?;
            prepare_provision_targets(&mut profiles, &plans, &capability)?
        };
        {
            let mut catalog = self.provisioning_catalog()?;
            for binding in bindings {
                let operation = catalog.bind_agent_provision_target(
                    binding.operation_id,
                    binding.operation_revision,
                    binding.profile_id,
                    binding.runtime_session_id,
                    bound_at,
                )?;
                if let Some(plan) = plans
                    .iter_mut()
                    .find(|plan| plan.operation.operation_id() == binding.operation_id)
                {
                    plan.operation = operation;
                }
            }
        }
        let summaries = plans
            .iter()
            .map(ProvisionOperationSummary::from_plan)
            .collect();
        Ok(summaries)
    }

    fn provisioning_catalog(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, OperationsCatalog>, ProvisioningError> {
        self.catalog().map_err(|_| {
            ProvisioningError::Operations(OperationsError::Corrupt("operations catalog lock"))
        })
    }

    fn provisioning_profile_catalog(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, AgentProfileCatalog>, ProvisioningError> {
        self.profile_catalog_guard().map_err(|_| {
            ProvisioningError::Profile(AgentProfileError::Corrupt(
                "global Agent Profile catalog lock",
            ))
        })
    }
}

/// One planned provision intent between the durable operation and its
/// Profile-catalog target.
struct ProvisionTargetPlan {
    operation: heterarchy_orchestration_operations::AgentProvisionOperation,
    expected_runtime_revision: Option<i64>,
    created: bool,
    prepared_revision: u64,
}

struct ProvisionTargetBinding {
    operation_id: Uuid,
    operation_revision: u64,
    profile_id: AgentProfileId,
    runtime_session_id: AgentProfileRuntimeSessionId,
}

fn prepare_provision_targets(
    catalog: &mut AgentProfileCatalog,
    plans: &[ProvisionTargetPlan],
    capability: &str,
) -> Result<Vec<ProvisionTargetBinding>, ProvisioningError> {
    let mut bindings = Vec::new();
    for plan in plans {
        if plan.operation.state() != AgentProvisionState::Prepared {
            continue;
        }
        match plan.operation.effect_kind() {
            heterarchy_orchestration_operations::AgentProvisionEffectKind::ReuseReadyRuntime => {
                validate_ready_runtime_reuse_target(catalog, plan)?;
            }
            heterarchy_orchestration_operations::AgentProvisionEffectKind::WakeExistingRuntime => {
                validate_existing_runtime_wake_target(catalog, plan)?;
            }
            heterarchy_orchestration_operations::AgentProvisionEffectKind::WakeNewRuntime => {
                let profile_id = plan
                    .operation
                    .profile_id()
                    .ok_or(AgentProfileError::Corrupt(
                        "wake-new Profile target missing",
                    ))?;
                let runtime_session_id =
                    prepare_provision_runtime_session(catalog, &plan.operation, profile_id)?;
                bindings.push(target_binding(plan, profile_id, runtime_session_id));
            }
            heterarchy_orchestration_operations::AgentProvisionEffectKind::ProvisionNew => {
                let (profile_id, runtime_session_id) =
                    prepare_new_provision_target(catalog, &plan.operation, capability)?;
                bindings.push(target_binding(plan, profile_id, runtime_session_id));
            }
        }
    }
    Ok(bindings)
}

fn validate_ready_runtime_reuse_target(
    catalog: &mut AgentProfileCatalog,
    plan: &ProvisionTargetPlan,
) -> Result<(), ProvisioningError> {
    let profile_id = plan
        .operation
        .profile_id()
        .ok_or(AgentProfileError::Corrupt(
            "reuse-ready Profile target missing",
        ))?;
    let runtime_session_id =
        plan.operation
            .runtime_session_id()
            .ok_or(AgentProfileError::Corrupt(
                "reuse-ready RuntimeSession target missing",
            ))?;
    let current = catalog
        .runtime_session(runtime_session_id)?
        .ok_or(AgentProfileError::RuntimeSessionMissing(runtime_session_id))?;
    let expected_revision = plan
        .expected_runtime_revision
        .ok_or(AgentProfileError::Corrupt(
            "reuse-ready resolver revision missing",
        ))?;
    if current.profile_id() != profile_id
        || current.state() != AgentProfileRuntimeSessionState::Ready
        || current.revision().get() != expected_revision
        || current.provider_conversation_id().is_none()
    {
        return Err(AgentProfileError::Corrupt(
            "reuse-ready RuntimeSession resolver fence changed",
        )
        .into());
    }
    Ok(())
}

fn validate_existing_runtime_wake_target(
    catalog: &mut AgentProfileCatalog,
    plan: &ProvisionTargetPlan,
) -> Result<(), ProvisioningError> {
    let profile_id = plan
        .operation
        .profile_id()
        .ok_or(AgentProfileError::Corrupt(
            "wake-existing Profile target missing",
        ))?;
    let runtime_session_id =
        plan.operation
            .runtime_session_id()
            .ok_or(AgentProfileError::Corrupt(
                "wake-existing RuntimeSession target missing",
            ))?;
    let current = catalog
        .runtime_session(runtime_session_id)?
        .ok_or(AgentProfileError::RuntimeSessionMissing(runtime_session_id))?;
    if current.profile_id() != profile_id {
        return Err(
            AgentProfileError::Corrupt("wake-existing RuntimeSession Profile mismatch").into(),
        );
    }
    let expected_revision = plan
        .expected_runtime_revision
        .ok_or(AgentProfileError::Corrupt(
            "wake-existing resolver revision missing",
        ))?;
    if current.state() != AgentProfileRuntimeSessionState::Offline
        || current.revision().get() != expected_revision
    {
        return Err(AgentProfileError::Corrupt(
            "wake-existing RuntimeSession resolver fence changed",
        )
        .into());
    }
    Ok(())
}

fn prepare_new_profile(
    catalog: &mut AgentProfileCatalog,
    operation: &heterarchy_orchestration_operations::AgentProvisionOperation,
    capability: &str,
) -> Result<AgentProfileId, AgentProfileError> {
    let operation_id = operation.operation_id();
    let profile_id = AgentProfileId::from_uuid(derived_provision_uuid(
        AGENT_PROVISION_PROFILE_ID_DOMAIN,
        operation_id,
    ))?;
    let stable_name = format!("agent-request-{operation_id}");
    let command = CreateAgentProfile::new(
        profile_id,
        derived_provision_uuid(AGENT_PROVISION_PROFILE_REQUEST_DOMAIN, operation_id),
        stable_name.clone(),
        stable_name,
        AgentProfileProviderFamily::ChatGpt,
        None,
        None,
        vec![capability.to_owned()],
        AgentProfileCreationOrigin::AgentRequest,
        operation.prepared_at(),
    )?;
    Ok(catalog.create_profile(&command)?.profile().id())
}

/// Create the deterministic `RuntimeSession` target for one prepared provision.
/// The operation identity preserves exact replay and the existing provider boundary.
///
/// # Errors
/// Preserves invalid target identity, Profile/runtime replay conflicts, catalog
/// failures, and a replayed runtime no longer in the provisioning state.
pub fn prepare_provision_runtime_session(
    catalog: &mut AgentProfileCatalog,
    operation: &heterarchy_orchestration_operations::AgentProvisionOperation,
    profile_id: AgentProfileId,
) -> Result<AgentProfileRuntimeSessionId, AgentProfileError> {
    let operation_id = operation.operation_id();
    let runtime_session_id = AgentProfileRuntimeSessionId::from_uuid(derived_provision_uuid(
        AGENT_PROVISION_RUNTIME_ID_DOMAIN,
        operation_id,
    ))?;
    let command = CreateAgentProfileRuntimeSession::new(
        runtime_session_id,
        derived_provision_uuid(AGENT_PROVISION_RUNTIME_REQUEST_DOMAIN, operation_id),
        profile_id,
        AgentProfileProviderFamily::ChatGpt,
        operation.prepared_at(),
    )?;
    let outcome = catalog.create_runtime_session(&command)?;
    if outcome.session().state() != AgentProfileRuntimeSessionState::Provisioning {
        return Err(AgentProfileError::Corrupt(
            "new provider RuntimeSession is not PROVISIONING",
        ));
    }
    Ok(outcome.session().id())
}

/// Create the deterministic Profile and `RuntimeSession` targets for a new
/// provider provision operation. This is the Runtime-owned target path used by
/// the `ProvisionNew` resolution kind; callers with an existing Profile use
/// [`prepare_provision_runtime_session`] directly.
///
/// # Errors
/// Preserves Profile/runtime validation, replay conflicts and uncertain catalog
/// commits. An earlier Profile commit may survive a later runtime failure.
pub fn prepare_new_provision_target(
    catalog: &mut AgentProfileCatalog,
    operation: &heterarchy_orchestration_operations::AgentProvisionOperation,
    capability: &str,
) -> Result<(AgentProfileId, AgentProfileRuntimeSessionId), AgentProfileError> {
    let profile_id = prepare_new_profile(catalog, operation, capability)?;
    let runtime_session_id = prepare_provision_runtime_session(catalog, operation, profile_id)?;
    Ok((profile_id, runtime_session_id))
}

fn target_binding(
    plan: &ProvisionTargetPlan,
    profile_id: AgentProfileId,
    runtime_session_id: AgentProfileRuntimeSessionId,
) -> ProvisionTargetBinding {
    ProvisionTargetBinding {
        operation_id: plan.operation.operation_id(),
        operation_revision: plan.operation.revision(),
        profile_id,
        runtime_session_id,
    }
}

/// Convert a domain result into the runtime error vocabulary.
fn into_runtime<T, E: Into<RuntimeError>>(result: Result<T, E>) -> Result<T, RuntimeError> {
    result.map_err(Into::into)
}

/// Resolver orchestration over the Profile catalog, identical to the MCP
/// resolver: reuse any existing claims for the request first, then evaluate
/// the deterministic policy once over the bounded candidate observation and
/// reserve the missing claims all-or-nothing.
fn resolve_and_claim_profiles(
    catalog: &mut heterarchy_orchestration_sessions::AgentProfileCatalog,
    request_id: Uuid,
    requested_count: u8,
    capability: &str,
    current_scope_profiles: &[AgentProfileId],
    claimed_at: TimestampMillis,
) -> Result<Vec<AgentRequestResolutionSelection>, ProvisioningError> {
    let (mut selections, missing_slots) =
        existing_claim_selections(catalog, request_id, requested_count)?;
    if missing_slots.is_empty() {
        return collect_complete_selections(selections);
    }
    let candidates = catalog.resolver_candidates(capability)?;
    if candidates.truncated() {
        return Err(
            AgentProfileError::Corrupt("Agent resolver candidate observation truncated").into(),
        );
    }
    let in_current_scope = current_scope_profiles.to_vec();
    let policy_candidates = candidates
        .candidates()
        .iter()
        .copied()
        .map(|candidate| {
            AgentResolverCandidate::new(
                candidate.profile_id().into_uuid().into_bytes(),
                in_current_scope.contains(&candidate.profile_id()),
                policy_availability(candidate.availability()),
            )
        })
        .collect::<Vec<_>>();
    let missing_count = u8::try_from(missing_slots.len())
        .map_err(|_| AgentProfileError::Corrupt("Agent resolver missing slot count"))?;
    let heterarchy_orchestration_policy::AgentResolverDecision::Resolved(decisions) =
        evaluate_agent_resolver(missing_count, &policy_candidates, false, false)
    else {
        return Err(AgentProfileError::Corrupt("Agent resolver policy indeterminate").into());
    };
    let mut claim_commands = Vec::new();
    for decision in decisions {
        let actual_slot = missing_slots[usize::from(decision.slot_ordinal())];
        if decision.action() == AgentResolverActionKind::ProvisionNew {
            selections[usize::from(actual_slot)] = Some(
                AgentRequestResolutionSelection::provision_new(actual_slot).map_err(|_| {
                    AgentProfileError::Corrupt("Agent resolver provision projection")
                })?,
            );
            continue;
        }
        let candidate_index = decision
            .candidate_index()
            .ok_or(AgentProfileError::Corrupt("Agent resolver candidate index"))?;
        let candidate = candidates
            .candidates()
            .get(candidate_index)
            .copied()
            .ok_or(AgentProfileError::Corrupt(
                "Agent resolver candidate bounds",
            ))?;
        let kind = claim_kind(decision.action())?;
        let runtime = match kind {
            AgentProfileResolutionClaimKind::ReuseReadyRuntime
            | AgentProfileResolutionClaimKind::WakeExistingRuntime => Some((
                candidate
                    .runtime_session_id()
                    .ok_or(AgentProfileError::Corrupt("Agent resolver Runtime Session"))?,
                candidate
                    .runtime_session_revision()
                    .ok_or(AgentProfileError::Corrupt(
                        "Agent resolver Runtime revision",
                    ))?,
            )),
            AgentProfileResolutionClaimKind::ReuseCurrentScope
            | AgentProfileResolutionClaimKind::WakeNewRuntime => None,
        };
        let target = AgentProfileResolutionClaimTarget::new(
            kind,
            candidate.profile_id(),
            candidate.profile_revision(),
            runtime,
        )?;
        claim_commands.push(AgentProfileResolutionClaimCommand::new(
            derived_resolution_claim_id(request_id, actual_slot),
            request_id,
            actual_slot,
            target,
            claimed_at,
        )?);
    }
    let claims = catalog.reserve_resolution_claims(&claim_commands)?;
    for claim in claims {
        let slot = claim.slot_ordinal();
        selections[usize::from(slot)] = Some(
            AgentRequestResolutionSelection::from_claim(slot, claim)
                .map_err(|_| AgentProfileError::Corrupt("Agent resolver claim projection"))?,
        );
    }
    collect_complete_selections(selections)
}

fn existing_claim_selections(
    catalog: &mut heterarchy_orchestration_sessions::AgentProfileCatalog,
    request_id: Uuid,
    requested_count: u8,
) -> Result<(Vec<Option<AgentRequestResolutionSelection>>, Vec<u8>), ProvisioningError> {
    let mut selections = vec![None; usize::from(requested_count)];
    let mut missing_slots = Vec::new();
    for slot in 0..requested_count {
        if let Some(claim) = catalog.resolution_claim(request_id, slot)? {
            selections[usize::from(slot)] = Some(
                AgentRequestResolutionSelection::from_claim(slot, claim)
                    .map_err(|_| AgentProfileError::Corrupt("Agent resolver claim projection"))?,
            );
        } else {
            missing_slots.push(slot);
        }
    }
    Ok((selections, missing_slots))
}

fn collect_complete_selections(
    selections: Vec<Option<AgentRequestResolutionSelection>>,
) -> Result<Vec<AgentRequestResolutionSelection>, ProvisioningError> {
    selections
        .into_iter()
        .map(|selection| {
            selection
                .ok_or(AgentProfileError::Corrupt("Agent resolver incomplete selection").into())
        })
        .collect()
}

const fn policy_availability(value: AgentProfileResolverAvailability) -> AgentResolverAvailability {
    match value {
        AgentProfileResolverAvailability::Ready => AgentResolverAvailability::Ready,
        AgentProfileResolverAvailability::OfflineResumable => {
            AgentResolverAvailability::OfflineResumable
        }
        AgentProfileResolverAvailability::OfflineNeedsSession => {
            AgentResolverAvailability::OfflineNeedsSession
        }
        AgentProfileResolverAvailability::Busy => AgentResolverAvailability::Busy,
        AgentProfileResolverAvailability::Transitioning => AgentResolverAvailability::Transitioning,
        AgentProfileResolverAvailability::Error => AgentResolverAvailability::Error,
    }
}

fn claim_kind(
    action: AgentResolverActionKind,
) -> Result<AgentProfileResolutionClaimKind, ProvisioningError> {
    match action {
        AgentResolverActionKind::ReuseCurrentScope => {
            Ok(AgentProfileResolutionClaimKind::ReuseCurrentScope)
        }
        AgentResolverActionKind::ReuseReadyRuntime => {
            Ok(AgentProfileResolutionClaimKind::ReuseReadyRuntime)
        }
        AgentResolverActionKind::WakeExistingRuntime => {
            Ok(AgentProfileResolutionClaimKind::WakeExistingRuntime)
        }
        AgentResolverActionKind::WakeNewRuntime => {
            Ok(AgentProfileResolutionClaimKind::WakeNewRuntime)
        }
        AgentResolverActionKind::ProvisionNew => Err(AgentProfileError::Corrupt(
            "provision-new action cannot create a Profile claim",
        )
        .into()),
    }
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
    fn from_plan(plan: &ProvisionTargetPlan) -> Self {
        let operation = &plan.operation;
        Self {
            operation_id: operation.operation_id(),
            slot_ordinal: operation.slot_ordinal(),
            state: provision_state_label(operation.state()),
            profile_id: operation.profile_id().map(AgentProfileId::into_uuid),
            runtime_session_id: operation
                .runtime_session_id()
                .map(AgentProfileRuntimeSessionId::into_uuid),
            created: plan.created,
            revision: operation.revision(),
            changed: plan.created || operation.revision() != plan.prepared_revision,
            project: operation.project(),
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

    /// Re-read one request's fulfillment readiness and durably promote it to
    /// `Fulfilled` when every resolved slot has a Ready registration.
    ///
    /// Pure observation: this never creates participation and is an
    /// idempotent no-op on an already-fulfilled request.
    ///
    /// # Errors
    /// Returns [`ProvisioningError`] while preserving Operations origin for
    /// missing requests, state conflicts, and durable failures.
    pub fn reconcile_agent_request_fulfillment(
        &self,
        request_id: Uuid,
        observed_at_ms: i64,
    ) -> Result<AgentRequestSubmitSummary, ProvisioningError> {
        if request_id.is_nil() {
            return Err(ProvisioningError::InvalidInput);
        }
        let observed_at = timestamp(observed_at_ms)?;
        let mut catalog = self.provisioning_catalog()?;
        let record = catalog.reconcile_agent_request_fulfillment(request_id, observed_at)?;
        Ok(AgentRequestSubmitSummary::from_record(&record, false))
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
    fn deterministic_identities_match_the_canonical_domains() {
        // Pinned against the MCP resolver derivations. Any drift would make
        // the backend and MCP reserve different durable rows for the same
        // request/slot, splitting replay identity between transports.
        let request = Uuid::from_u128(0xB100);
        let claim = derived_resolution_claim_id(request, 1);
        let expected = {
            let mut hasher = blake3::Hasher::new();
            hasher.update(b"heterarchy.orchestration.agent-request.resolution-claim.v1\0");
            hasher.update(request.as_bytes());
            hasher.update(&[1]);
            let mut bytes = [0_u8; 16];
            bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
            bytes[6] = (bytes[6] & 0x0f) | 0x80;
            bytes[8] = (bytes[8] & 0x3f) | 0x80;
            Uuid::from_bytes(bytes)
        };
        assert_eq!(claim, expected);
        // Slot ordinal participates: different slots derive different claims.
        assert_ne!(derived_resolution_claim_id(request, 0), claim);
        // Provision identities use their own domains per operation.
        let operation = Uuid::from_u128(0xB200);
        let profile = derived_provision_uuid(
            b"heterarchy.orchestration.agent-provision.profile-id.v1\0",
            operation,
        );
        let runtime = derived_provision_uuid(
            b"heterarchy.orchestration.agent-provision.runtime-id.v1\0",
            operation,
        );
        assert_ne!(profile, runtime);
    }

    #[test]
    fn provisioning_state_labels_cover_the_closed_vocabularies() {
        // These labels are transport projection contracts; they must cover
        // every domain variant and stay stable.
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
    fn provisioning_replays_preserve_creation_and_revision_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("provisioning-evidence-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        crate::test_support::ensure_workspace_project_binding(
            &runtime,
            Uuid::from_u128(0xA101),
            1,
        )?;
        let command = AgentRequestSubmit {
            workspace_id: Uuid::from_u128(0xA101),
            activity_id: Uuid::from_u128(0xA102),
            requester_agent_id: Uuid::from_u128(0xA103),
            requester_session_id: Uuid::from_u128(0xA104),
            coordinator_revision: 1,
            request_id: Uuid::from_u128(0xA105),
            objective: "prepare one provider target".to_owned(),
            capability: "chat".to_owned(),
            count: 1,
            effect_scope: AgentRequestEffectScope::SourceWrite,
            independence: AgentRequestIndependence::None,
            reason: "evidence test".to_owned(),
            created_at_ms: 1,
        };
        let submitted = runtime.submit_agent_request(command.clone())?;
        assert!(submitted.created);
        assert_eq!(submitted.revision, 1);
        let submitted_replay = runtime.submit_agent_request(command)?;
        assert!(!submitted_replay.created);
        assert_eq!(submitted_replay.revision, submitted.revision);

        let resolution = runtime.reserve_agent_request_resolution(Uuid::from_u128(0xA105), 2, 3)?;
        assert!(resolution.created);
        assert_eq!(resolution.workspace_id, Uuid::from_u128(0xA101));
        assert_eq!(resolution.request_revision, 2);
        let resolution_replay =
            runtime.reserve_agent_request_resolution(Uuid::from_u128(0xA105), 4, 5)?;
        assert!(!resolution_replay.created);
        assert_eq!(
            resolution_replay.request_revision,
            resolution.request_revision
        );
        // Readback must not prepare a provider operation as a side effect.
        assert!(
            runtime
                .agent_request_provision_operations(Uuid::from_u128(0xA105))?
                .is_empty()
        );

        let prepared =
            runtime.prepare_agent_request_effect_targets(Uuid::from_u128(0xA105), 6, 7)?;
        assert_eq!(prepared.len(), 1);
        assert!(prepared[0].created);
        assert!(prepared[0].changed);
        assert!(prepared[0].revision > 0);
        let observed = runtime.agent_request_provision_operations(Uuid::from_u128(0xA105))?;
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].operation_id, prepared[0].operation_id);
        assert_eq!(observed[0].revision, prepared[0].revision);
        assert!(!observed[0].created && !observed[0].changed);
        let prepared_replay =
            runtime.prepare_agent_request_effect_targets(Uuid::from_u128(0xA105), 8, 9)?;
        assert_eq!(prepared_replay.len(), 1);
        assert!(!prepared_replay[0].created);
        assert!(!prepared_replay[0].changed);
        assert_eq!(prepared_replay[0].revision, prepared[0].revision);
        assert_eq!(prepared_replay[0].operation_id, prepared[0].operation_id);

        drop(runtime);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn partial_target_failure_preserves_exact_operation_readback()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("partial-provision-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        let workspace_id = Uuid::now_v7();
        crate::test_support::ensure_workspace_project_binding(&runtime, workspace_id, 1)?;
        let request_id = Uuid::now_v7();
        runtime.submit_agent_request(AgentRequestSubmit {
            workspace_id,
            activity_id: Uuid::now_v7(),
            requester_agent_id: Uuid::now_v7(),
            requester_session_id: Uuid::now_v7(),
            coordinator_revision: 1,
            request_id,
            objective: "partial intent readback".to_owned(),
            capability: "chat".to_owned(),
            count: 2,
            effect_scope: AgentRequestEffectScope::SourceWrite,
            independence: AgentRequestIndependence::None,
            reason: "later slot fails".to_owned(),
            created_at_ms: 1,
        })?;
        runtime.reserve_agent_request_resolution(request_id, 2, 3)?;
        let second = runtime
            .catalog()?
            .prepare_agent_provision_operation(request_id, 1, timestamp(4)?)?
            .operation()
            .clone();
        let operation_id = second.operation_id();
        let profile_id = AgentProfileId::from_uuid(derived_provision_uuid(
            AGENT_PROVISION_PROFILE_ID_DOMAIN,
            operation_id,
        ))?;
        let profile_nonce =
            derived_provision_uuid(AGENT_PROVISION_PROFILE_REQUEST_DOMAIN, operation_id);
        // A conflicting canonical request belongs to the later slot. Earlier
        // intent commits remain durable; the batch must never be called atomic.
        runtime
            .profile_catalog_guard()?
            .create_profile(&CreateAgentProfile::new(
                profile_id,
                profile_nonce,
                "conflicting-slot".to_owned(),
                "Conflicting slot".to_owned(),
                AgentProfileProviderFamily::ChatGpt,
                None,
                None,
                Vec::new(),
                AgentProfileCreationOrigin::UserManual,
                timestamp(4)?,
            )?)?;
        assert!(
            runtime
                .prepare_agent_request_effect_targets(request_id, 5, 6)
                .is_err()
        );
        let observed = runtime.agent_request_provision_operations(request_id)?;
        assert_eq!(observed.len(), 2);
        assert_eq!(observed[1].operation_id, operation_id);
        assert!(
            observed.iter().all(|operation| operation.revision > 0
                && !operation.changed
                && !operation.created)
        );
        assert!(observed[0].profile_id.is_none());
        drop(runtime);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
