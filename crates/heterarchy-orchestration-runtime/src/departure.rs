//! Caller-owned application stages for Orchestration logical detach.

use forge_domain::{
    ActivityId, AgentInstanceId, AgentInstanceRevision, AgentSessionId, AuthenticatedPrincipal,
    TimestampMillis, WorkspaceId,
};
use heterarchy_orchestration_operations::{
    AgentDepartureKind, DepartureObservation, LogicalDepartureKind, LogicalDepartureReceipt,
    LogicalDetachCommand,
};
use uuid::Uuid;

pub mod handoff;

use crate::{OrchestrationRuntime, RuntimeError};

const DEPARTURE_DIGEST_DOMAIN: &[u8] = b"heterarchy-orchestration/agent-departure-identity/v1\0";
const DETACH_REQUEST_DOMAIN: &[u8] = b"heterarchy-orchestration/agent-detach-request/v1\0";

/// Exact caller-selected scope and revision fence for a logical detach.
///
/// Construction validates all identities and the positive Agent revision.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DepartureTarget {
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    expected_agent_revision: AgentInstanceRevision,
}

impl DepartureTarget {
    /// Construct a checked detach target from wire-level identities.
    ///
    /// # Errors
    /// Rejects nil identities or a non-positive Agent revision.
    pub fn new(
        workspace_id: Uuid,
        activity_id: Uuid,
        agent_id: Uuid,
        session_id: Uuid,
        expected_agent_revision: i64,
    ) -> Result<Self, RuntimeError> {
        if workspace_id.is_nil() || activity_id.is_nil() || agent_id.is_nil() || session_id.is_nil()
        {
            return Err(RuntimeError::InvalidInput);
        }
        let expected_agent_revision = AgentInstanceRevision::new(expected_agent_revision)
            .map_err(|_| RuntimeError::InvalidInput)?;
        Ok(Self {
            workspace_id: WorkspaceId::from_uuid(workspace_id),
            activity_id: ActivityId::from_uuid(activity_id),
            agent_id: AgentInstanceId::from_uuid(agent_id),
            session_id: AgentSessionId::from_uuid(session_id),
            expected_agent_revision,
        })
    }

    fn request_id(self) -> Uuid {
        detach_request_id(
            self.workspace_id.into_uuid(),
            self.activity_id.into_uuid(),
            self.agent_id.into_uuid(),
            self.expected_agent_revision.get(),
        )
    }
}

/// Read-only preparation result for one exact detach request.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetachPreparation {
    /// Existing canonical logical participant for the caller's Workspace Session.
    pub participant_id: Uuid,
    /// Stable request identity derived from the exact Workspace, Activity, Agent and revision.
    pub request_id: Uuid,
    /// Matching journal receipt when this exact caller already completed the detach.
    pub receipt: Option<LogicalDepartureReceipt>,
}

/// Stable replay identity for a detach request, preserving the existing MCP byte contract.
#[must_use]
pub fn detach_request_id(
    workspace_id: Uuid,
    activity_id: Uuid,
    agent_id: Uuid,
    expected_agent_revision: i64,
) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DETACH_REQUEST_DOMAIN);
    hasher.update(workspace_id.as_bytes());
    hasher.update(activity_id.as_bytes());
    hasher.update(agent_id.as_bytes());
    hasher.update(expected_agent_revision.to_be_bytes().as_slice());
    derived_departure_uuid(&hasher)
}

/// Compute the existing Forge departure identity digest for the exact caller and request.
#[must_use]
pub fn departure_identity_digest(
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    principal: AuthenticatedPrincipal,
    request_id: Uuid,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DEPARTURE_DIGEST_DOMAIN);
    hasher.update(workspace_id.into_uuid().as_bytes());
    hasher.update(activity_id.into_uuid().as_bytes());
    hasher.update(agent_id.into_uuid().as_bytes());
    hasher.update(session_id.into_uuid().as_bytes());
    match principal {
        AuthenticatedPrincipal::TrustedStdio => {
            hasher.update(&[0_u8][..]);
        }
        AuthenticatedPrincipal::DirectToken(id) => {
            hasher.update(&[1_u8][..]);
            hasher.update(id.into_uuid().as_bytes());
        }
        AuthenticatedPrincipal::OAuthGrant(id) => {
            hasher.update(&[2_u8][..]);
            hasher.update(id.into_uuid().as_bytes());
        }
    }
    hasher.update(request_id.as_bytes());
    *hasher.finalize().as_bytes()
}

impl OrchestrationRuntime {
    /// Resolve the exact caller-owned Session and any existing detach receipt without writing state.
    ///
    /// A receipt is returned only when its full detach identity matches the original caller's
    /// canonical participant. A changed Session or journal kind is a conflict.
    ///
    /// # Errors
    /// Rejects foreign or closed Sessions, absent logical membership, conflicting journal state,
    /// and catalog failures.
    pub fn prepare_detach(
        &self,
        target: DepartureTarget,
        principal: AuthenticatedPrincipal,
    ) -> Result<DetachPreparation, RuntimeError> {
        self.require_open_session(target.session_id.into_uuid(), principal)?;

        let request_id = target.request_id();
        let catalog = self.catalog()?;
        if let Some(receipt) = catalog.logical_departure(request_id)? {
            if !receipt_matches_target(&receipt, target) {
                return Err(RuntimeError::Conflict);
            }
            return Ok(DetachPreparation {
                participant_id: receipt.participant_id(),
                request_id,
                receipt: Some(receipt),
            });
        }
        let participant_id = catalog
            .logical_identity_for_session(target.workspace_id, target.session_id)?
            .map(|identity| identity.participant_id())
            .ok_or(RuntimeError::Forbidden)?;
        Ok(DetachPreparation {
            participant_id,
            request_id,
            receipt: None,
        })
    }

    /// Read one exact detach receipt for the owning Session, including terminal Session state.
    ///
    /// This uses the existing Session ownership read and Operations journal only; it does not
    /// reconstruct current participation rights or create missing state.
    ///
    /// # Errors
    /// Rejects missing or foreign Sessions, conflicting journal state, and catalog failures.
    pub fn detach_readback(
        &self,
        target: DepartureTarget,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<LogicalDepartureReceipt>, RuntimeError> {
        let _session = self.resolve_owned_session(target.session_id.into_uuid(), principal)?;

        let catalog = self.catalog()?;
        let receipt = catalog.logical_departure(target.request_id())?;
        if let Some(receipt) = &receipt
            && !receipt_matches_target(receipt, target)
        {
            return Err(RuntimeError::Conflict);
        }
        Ok(receipt)
    }

    /// Commit the Orchestration detach stage from a checked source observation.
    ///
    /// The observation is source data, not a Forge capability. Operations remains the sole
    /// owner of Ready-membership validation, replay and journal writes.
    ///
    /// # Errors
    /// Rejects source observations that do not match the exact caller, target, verified import or
    /// detach shape; invalid timestamps; closed/foreign Sessions; and Operations commit failures.
    pub fn commit_detach(
        &self,
        target: DepartureTarget,
        principal: AuthenticatedPrincipal,
        source: &DepartureObservation,
        observed_at_ms: i64,
        committed_at_ms: i64,
    ) -> Result<LogicalDepartureReceipt, RuntimeError> {
        let request_id = target.request_id();
        let expected_digest = departure_identity_digest(
            target.workspace_id,
            target.activity_id,
            target.agent_id,
            target.session_id,
            principal,
            request_id,
        );
        if source.request_id() != request_id
            || source.identity_digest() != expected_digest
            || source.workspace_id() != target.workspace_id
            || source.activity_id() != target.activity_id
            || source.agent_id() != target.agent_id
            || source.session_id() != target.session_id
            || source.expected_agent_revision() != target.expected_agent_revision
            || source.kind() != AgentDepartureKind::Detach
            || source.target_agent_id().is_some()
            || source.target_session_id().is_some()
            || source.result_coordinator_revision().is_some()
        {
            return Err(RuntimeError::Conflict);
        }
        let observed_at =
            TimestampMillis::new(observed_at_ms).map_err(|_| RuntimeError::InvalidInput)?;
        let committed_at =
            TimestampMillis::new(committed_at_ms).map_err(|_| RuntimeError::InvalidInput)?;

        // Revalidate the original caller immediately before resolving and committing the logical
        // membership transition. No lock crosses an async boundary in this synchronous stage.
        self.require_open_session(target.session_id.into_uuid(), principal)?;
        let mut catalog = self.catalog()?;
        let import = catalog
            .verified_workspace_import(target.workspace_id)?
            .ok_or(RuntimeError::SourceAuthorityUnavailable)?;
        if source.migration_id() != import.migration_id()
            || source.destination_id() != import.destination_id()
        {
            return Err(RuntimeError::Conflict);
        }
        let participant_id = catalog
            .logical_identity_for_session(target.workspace_id, target.session_id)?
            .map(|identity| identity.participant_id())
            .ok_or(RuntimeError::Forbidden)?;
        let command = LogicalDetachCommand::new(
            target.workspace_id,
            target.activity_id,
            participant_id,
            target.agent_id,
            target.session_id,
            principal,
            observed_at,
        );
        catalog
            .commit_logical_detach_observation(command, source, committed_at)
            .map_err(RuntimeError::from)
    }
}

fn receipt_matches_target(receipt: &LogicalDepartureReceipt, target: DepartureTarget) -> bool {
    receipt.request_id() == target.request_id()
        && receipt.workspace_id() == target.workspace_id
        && receipt.activity_id() == target.activity_id
        && receipt.agent_id() == target.agent_id
        && receipt.session_id() == target.session_id
        && receipt.expected_agent_revision() == target.expected_agent_revision
        && receipt.kind() == LogicalDepartureKind::Detach
        && receipt.target_participant_id().is_none()
        && receipt.target_agent_id().is_none()
        && receipt.target_session_id().is_none()
        && receipt.result_coordinator_revision().is_none()
}

fn derived_departure_uuid(hasher: &blake3::Hasher) -> Uuid {
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}
