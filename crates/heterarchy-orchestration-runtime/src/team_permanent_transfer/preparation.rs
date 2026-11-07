//! Runtime boundary for an immutable permanent-transfer preparation.

use forge_domain::{AuthenticatedPrincipal, WorkspaceId};
use heterarchy_orchestration_operations::{
    TeamPermanentTransferPreparationCommand, TeamPermanentTransferPreparationReceipt,
};
use uuid::Uuid;

use super::{OrchestrationRuntime, RuntimeError, TeamPermanentTransferActorTarget};

/// Exact Orchestrator authorization and prior consent receipts for preparation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamPermanentTransferPreparationSpec {
    pub actor: TeamPermanentTransferActorTarget,
    pub preparation_id: Uuid,
    pub transfer_id: Uuid,
    pub expected_generation: u64,
    pub expected_workspace_leadership_revision: i64,
    pub source_leader_consent_id: Uuid,
    pub target_acceptance_id: Uuid,
}

/// Independent current reader for an immutable preparation receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamPermanentTransferPreparationReadSpec {
    pub workspace_id: Uuid,
    pub preparation_id: Uuid,
    pub actor: TeamPermanentTransferActorTarget,
}

impl OrchestrationRuntime {
    /// Commit target identity capture and the preparation barrier in Operations.
    /// This records no Forge work fence, settlement or Home membership change.
    /// # Errors
    /// Rejects invalid actor authority, changed generation or consents,
    /// incomplete Workspace identity, and persistence or clock failures.
    pub fn prepare_team_permanent_transfer(
        &self,
        spec: TeamPermanentTransferPreparationSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<TeamPermanentTransferPreparationReceipt, RuntimeError> {
        let workspace = WorkspaceId::from_uuid(spec.actor.workspace_id);
        let (actor, participation) = crate::teams::participant_actor(
            workspace,
            spec.actor.activity_id,
            spec.actor.session_id,
        )?;
        self.require_participation(participation, principal)?;
        let command = TeamPermanentTransferPreparationCommand::new(
            spec.preparation_id,
            workspace,
            spec.transfer_id,
            spec.expected_generation,
            spec.expected_workspace_leadership_revision,
            spec.source_leader_consent_id,
            spec.target_acceptance_id,
            actor,
        )
        .map_err(RuntimeError::persistence)?;
        self.catalog()?
            .prepare_team_permanent_transfer(&command, super::trusted_server_time)
            .map_err(RuntimeError::persistence)
    }

    /// Read historical target identity capture using its author or current O.
    /// # Errors
    /// Rejects invalid current actor binding and corrupt authorized history
    /// or persistence failures. An unrelated reader receives no record.
    pub fn read_team_permanent_transfer_preparation(
        &self,
        spec: TeamPermanentTransferPreparationReadSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<TeamPermanentTransferPreparationReceipt>, RuntimeError> {
        if spec.workspace_id != spec.actor.workspace_id {
            return Err(RuntimeError::InvalidInput);
        }
        let workspace = WorkspaceId::from_uuid(spec.workspace_id);
        let (actor, participation) = crate::teams::participant_actor(
            workspace,
            spec.actor.activity_id,
            spec.actor.session_id,
        )?;
        self.require_participation(participation, principal)?;
        let catalog = self.catalog()?;
        catalog
            .read_team_permanent_transfer_preparation(workspace, spec.preparation_id, actor)
            .map_err(RuntimeError::persistence)
    }
}
