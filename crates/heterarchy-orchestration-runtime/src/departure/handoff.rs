//! Full request equivalence for the existing Forge handoff boundary.

use forge_domain::{
    ActivityCoordinatorRevision, AgentInstanceId, AgentInstanceRevision, AgentSessionId,
    AuthenticatedPrincipal, CoordinationSnapshot,
};
use heterarchy_orchestration_operations::{
    AgentDepartureKind, DepartureObservation, LogicalDepartureKind, LogicalDepartureReceipt,
    LogicalDetachCommand, LogicalHandoffCommand,
};
use uuid::Uuid;

use super::{DepartureTarget, departure_identity_digest};
use crate::{OrchestrationRuntime, RuntimeError};

/// Checked immutable caller intent; observation time is deliberately not replay identity.
#[must_use]
#[derive(Debug, Clone, Copy)]
pub struct HandoffTarget {
    pub(super) source: DepartureTarget,
    pub(super) target_agent_id: AgentInstanceId,
    pub(super) target_session_id: AgentSessionId,
    pub(super) expected_target_revision: AgentInstanceRevision,
    pub(super) expected_coordinator_revision: ActivityCoordinatorRevision,
    pub(super) expected_plan: CoordinationSnapshot,
    pub(super) request_id: Uuid,
}

impl HandoffTarget {
    /// Build the exact Forge command after the source port resolves the target principal.
    ///
    /// # Errors
    /// Rejects invalid observation time or a source command construction failure.
    pub fn source_request(
        self,
        principal: AuthenticatedPrincipal,
        target_principal: AuthenticatedPrincipal,
        observed_at_ms: i64,
    ) -> Result<
        heterarchy_orchestration_operations::AgentDepartureReconciliationRequest,
        RuntimeError,
    > {
        let actor = forge_domain::ActivityCoordinatorActor::new(
            self.source.workspace_id,
            self.source.activity_id,
            self.source.agent_id,
            self.expected_coordinator_revision,
            principal,
            self.source.session_id,
        );
        let command = forge_domain::AgentHandoff::new(
            actor,
            self.source.expected_agent_revision,
            self.target_agent_id,
            self.target_session_id,
            target_principal,
            self.expected_target_revision,
            self.expected_plan,
            forge_domain::AgentEventId::from_uuid(self.request_id),
            forge_domain::TimestampMillis::new(observed_at_ms)
                .map_err(|_| RuntimeError::InvalidInput)?,
        );
        heterarchy_orchestration_operations::AgentDepartureReconciliationRequest::new(
            self.request_id,
            heterarchy_orchestration_operations::AgentDepartureCommand::Handoff(Box::new(command)),
            self.source.session_id,
            principal,
            self.identity_digest(principal),
        )
        .map_err(|_| RuntimeError::InvalidInput)
    }

    /// Validate the exact target and revision fences of a handoff.
    ///
    /// # Errors
    /// Rejects nil identities, identical source/target bindings and invalid revisions.
    pub fn new(
        source: DepartureTarget,
        target_agent_id: Uuid,
        target_session_id: Uuid,
        expected_target_revision: i64,
        expected_coordinator_revision: i64,
        expected_plan: CoordinationSnapshot,
        request_id: Uuid,
    ) -> Result<Self, RuntimeError> {
        if target_agent_id.is_nil()
            || target_session_id.is_nil()
            || request_id.is_nil()
            || source.agent_id.into_uuid() == target_agent_id
            || source.session_id.into_uuid() == target_session_id
            || expected_plan
                .goal()
                .is_some_and(|goal| goal.goal_id().into_uuid().is_nil())
            || expected_plan
                .node()
                .is_some_and(|node| node.node_id().into_uuid().is_nil())
        {
            return Err(RuntimeError::InvalidInput);
        }
        let expected_target_revision = AgentInstanceRevision::new(expected_target_revision)
            .map_err(|_| RuntimeError::InvalidInput)?;
        expected_target_revision
            .checked_next()
            .map_err(|_| RuntimeError::InvalidInput)?;
        source
            .expected_agent_revision
            .checked_next()
            .map_err(|_| RuntimeError::InvalidInput)?;
        let expected_coordinator_revision =
            ActivityCoordinatorRevision::new(expected_coordinator_revision)
                .map_err(|_| RuntimeError::InvalidInput)?;
        expected_coordinator_revision
            .checked_next()
            .map_err(|_| RuntimeError::InvalidInput)?;
        Ok(Self {
            source,
            target_agent_id: AgentInstanceId::from_uuid(target_agent_id),
            target_session_id: AgentSessionId::from_uuid(target_session_id),
            expected_target_revision,
            expected_coordinator_revision,
            expected_plan,
            request_id,
        })
    }

    /// Bind every caller-controlled handoff fence to Forge's durable opaque digest.
    #[must_use]
    pub fn identity_digest(self, principal: AuthenticatedPrincipal) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"heterarchy-orchestration/agent-handoff-intent/v1\0");
        hasher.update(&departure_identity_digest(
            self.source.workspace_id,
            self.source.activity_id,
            self.source.agent_id,
            self.source.session_id,
            principal,
            self.request_id,
        ));
        hasher.update(&self.source.expected_agent_revision.get().to_be_bytes());
        hasher.update(self.target_agent_id.into_uuid().as_bytes());
        hasher.update(self.target_session_id.into_uuid().as_bytes());
        hasher.update(&self.expected_target_revision.get().to_be_bytes());
        hasher.update(&self.expected_coordinator_revision.get().to_be_bytes());
        if let Some(goal) = self.expected_plan.goal() {
            hasher.update(&[1]);
            hasher.update(goal.goal_id().into_uuid().as_bytes());
            hasher.update(&goal.expected_revision().get().to_be_bytes());
        } else {
            hasher.update(&[0]);
        }
        for revision in [
            self.expected_plan
                .dag_revision()
                .map(forge_domain::DagRevisionNumber::get),
            self.expected_plan
                .dag_head_revision()
                .map(forge_domain::DagHeadRevision::get),
        ] {
            if let Some(value) = revision {
                hasher.update(&[1]);
                hasher.update(&value.to_be_bytes());
            } else {
                hasher.update(&[0]);
            }
        }
        if let Some(node) = self.expected_plan.node() {
            hasher.update(&[1]);
            hasher.update(node.node_id().into_uuid().as_bytes());
            hasher.update(&node.revision().get().to_be_bytes());
        } else {
            hasher.update(&[0]);
        }
        *hasher.finalize().as_bytes()
    }

    /// Prove source receipt equivalence before any local replay or commit.
    ///
    /// # Errors
    /// Rejects a changed handoff tuple, unrelated or historical source-only digest.
    pub fn validate_observation(
        self,
        principal: AuthenticatedPrincipal,
        receipt: &DepartureObservation,
    ) -> Result<(), RuntimeError> {
        if receipt.kind() != AgentDepartureKind::Handoff
            || receipt.request_id() != self.request_id
            || receipt.workspace_id() != self.source.workspace_id
            || receipt.activity_id() != self.source.activity_id
            || receipt.agent_id() != self.source.agent_id
            || receipt.session_id() != self.source.session_id
            || receipt.expected_agent_revision() != self.source.expected_agent_revision
            || receipt.target_agent_id() != Some(self.target_agent_id)
            || receipt.target_session_id() != Some(self.target_session_id)
            || receipt.result_coordinator_revision()
                != self.expected_coordinator_revision.checked_next().ok()
            || receipt.identity_digest() != self.identity_digest(principal)
        {
            return Err(RuntimeError::Conflict);
        }
        Ok(())
    }
}

impl OrchestrationRuntime {
    /// Observe whether the caller's exact handoff ID has local durable state.
    /// This is inconsistency detection only, never replay equivalence or permission.
    ///
    /// # Errors
    /// Rejects foreign Sessions or journal failures.
    pub fn handoff_journal_present(
        &self,
        target: HandoffTarget,
        principal: AuthenticatedPrincipal,
    ) -> Result<bool, RuntimeError> {
        let _session =
            self.resolve_owned_session(target.source.session_id.into_uuid(), principal)?;
        Ok(self
            .catalog()?
            .logical_departure(target.request_id)?
            .is_some())
    }

    /// Read the local handoff receipt only after exact full-intent source proof.
    /// Terminal owned Sessions remain eligible for immutable audit.
    ///
    /// # Errors
    /// Rejects foreign Sessions, unrelated source proof or conflicting local journal state.
    pub fn handoff_readback(
        &self,
        target: HandoffTarget,
        principal: AuthenticatedPrincipal,
        source: &DepartureObservation,
    ) -> Result<Option<LogicalDepartureReceipt>, RuntimeError> {
        let _session =
            self.resolve_owned_session(target.source.session_id.into_uuid(), principal)?;
        target.validate_observation(principal, source)?;
        let catalog = self.catalog()?;
        let receipt = catalog.logical_departure(target.request_id)?;
        if let Some(receipt) = &receipt
            && (receipt.kind() != LogicalDepartureKind::Handoff
                || receipt.workspace_id() != source.workspace_id()
                || receipt.activity_id() != source.activity_id()
                || receipt.agent_id() != source.agent_id()
                || receipt.session_id() != source.session_id()
                || receipt.expected_agent_revision() != source.expected_agent_revision()
                || receipt.result_agent_revision() != source.result_agent_revision()
                || receipt.target_agent_id() != source.target_agent_id()
                || receipt.target_session_id() != source.target_session_id()
                || receipt.result_coordinator_revision() != source.result_coordinator_revision()
                || receipt.source_reconciled_at() != source.reconciled_at())
        {
            return Err(RuntimeError::Conflict);
        }
        Ok(receipt)
    }

    /// Commit through the sole Operations handoff transaction after fresh caller/source proof.
    ///
    /// # Errors
    /// Rejects closed/foreign Sessions, mismatched intent/import/bindings or uncertain commits.
    pub fn commit_handoff(
        &self,
        target: HandoffTarget,
        principal: AuthenticatedPrincipal,
        source: &DepartureObservation,
        observed_at_ms: i64,
        committed_at_ms: i64,
    ) -> Result<LogicalDepartureReceipt, RuntimeError> {
        target.validate_observation(principal, source)?;
        self.require_open_session(target.source.session_id.into_uuid(), principal)?;
        let observed_at = forge_domain::TimestampMillis::new(observed_at_ms)
            .map_err(|_| RuntimeError::InvalidInput)?;
        let committed_at = forge_domain::TimestampMillis::new(committed_at_ms)
            .map_err(|_| RuntimeError::InvalidInput)?;
        let mut catalog = self.catalog()?;
        let import = catalog
            .verified_workspace_import(target.source.workspace_id)?
            .ok_or(RuntimeError::SourceAuthorityUnavailable)?;
        if source.migration_id() != import.migration_id()
            || source.destination_id() != import.destination_id()
        {
            return Err(RuntimeError::Conflict);
        }
        let source_participant = catalog
            .logical_identity_for_session(target.source.workspace_id, target.source.session_id)?
            .ok_or(RuntimeError::Forbidden)?
            .participant_id();
        let target_participant = catalog
            .logical_identity_for_session(target.source.workspace_id, target.target_session_id)?
            .ok_or(RuntimeError::Forbidden)?
            .participant_id();
        let command = LogicalHandoffCommand::new(
            LogicalDetachCommand::new(
                target.source.workspace_id,
                target.source.activity_id,
                source_participant,
                target.source.agent_id,
                target.source.session_id,
                principal,
                observed_at,
            ),
            target_participant,
            target.target_agent_id,
            target.target_session_id,
        );
        catalog
            .commit_logical_handoff_observation(command, source, committed_at)
            .map_err(RuntimeError::from)
    }
}

#[cfg(test)]
mod tests;
