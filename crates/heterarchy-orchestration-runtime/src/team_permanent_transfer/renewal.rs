//! Runtime operations for renewing and observing transfer generations.

use forge_domain::AuthenticatedPrincipal;
use forge_domain::WorkspaceId;
use heterarchy_orchestration_operations::{
    TeamPermanentTransferDestinationSlot, TeamPermanentTransferGenerationSnapshot,
    TeamPermanentTransferIntentCommand, TeamPermanentTransferRenewalCommand,
    TeamPermanentTransferRevisions, TeamPermanentTransferScope, WorkspaceParticipationId,
};
use uuid::Uuid;

use super::{
    OrchestrationRuntime, RuntimeError, TeamPermanentTransferActorTarget,
    TeamPermanentTransferIntentSpec,
};

/// Exact stale-Leader proposal replacement and expected active generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamPermanentTransferRenewalSpec {
    /// Exact immutable proposal for the new generation.
    pub proposal: TeamPermanentTransferIntentSpec,
    /// Current slot head that the renewal expects to supersede.
    pub predecessor_transfer_id: Uuid,
    /// Generation number of that exact predecessor.
    pub expected_generation: u64,
}

impl OrchestrationRuntime {
    /// Renew one exact transfer slot proposal after its source Leader changed.
    /// Operations revalidates the current Orchestrator, exact active head,
    /// destination approval, current source membership, and changed Leader in
    /// one transaction. This records intent only; it performs no source work.
    ///
    /// # Errors
    /// Rejects a foreign or closed actor Session, stale or invalid proposal,
    /// non-head predecessor, unchanged source Leader, or catalog failure.
    pub fn renew_team_permanent_transfer(
        &self,
        spec: TeamPermanentTransferRenewalSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<TeamPermanentTransferGenerationSnapshot, RuntimeError> {
        let proposal = spec.proposal;
        let workspace = WorkspaceId::from_uuid(proposal.actor.workspace_id);
        let (actor, participation) = crate::teams::participant_actor(
            workspace,
            proposal.actor.activity_id,
            proposal.actor.session_id,
        )?;
        self.require_participation(participation, principal)?;

        let target_participation_id =
            WorkspaceParticipationId::from_uuid(proposal.target_participation_id)
                .map_err(RuntimeError::persistence)?;
        let scope = TeamPermanentTransferScope::new(
            workspace,
            proposal.source_team_id,
            proposal.destination_team_id,
            target_participation_id,
        );
        let destination = TeamPermanentTransferDestinationSlot::new(
            proposal.destination_request_id,
            proposal.destination_decision_id,
            proposal.destination_ordinal,
        );
        let revisions = TeamPermanentTransferRevisions::new(
            proposal.expected_destination_team_revision,
            proposal.expected_destination_capacity_revision,
            proposal.expected_workspace_leadership_revision,
            proposal.expected_source_membership_revision,
            proposal.expected_source_leadership_revision,
        );
        let proposal = TeamPermanentTransferIntentCommand::new(
            proposal.transfer_id,
            scope,
            destination,
            revisions,
            actor,
        )
        .map_err(RuntimeError::persistence)?;
        let command = TeamPermanentTransferRenewalCommand::new(
            spec.predecessor_transfer_id,
            spec.expected_generation,
            proposal,
        )
        .map_err(RuntimeError::persistence)?;

        self.catalog()?
            .renew_team_permanent_transfer(&command, super::trusted_server_time)
            .map_err(RuntimeError::persistence)
    }

    /// Read one immutable generation using the original proposer or current
    /// Workspace Orchestrator authorization enforced by Operations.
    ///
    /// # Errors
    /// Rejects a foreign or closed reader Session, malformed transfer identity,
    /// or corrupt generation history.
    pub fn read_team_permanent_transfer_generation(
        &self,
        actor: TeamPermanentTransferActorTarget,
        transfer_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<TeamPermanentTransferGenerationSnapshot>, RuntimeError> {
        let workspace = WorkspaceId::from_uuid(actor.workspace_id);
        let (reader, participation) =
            crate::teams::participant_actor(workspace, actor.activity_id, actor.session_id)?;
        self.require_participation(participation, principal)?;
        self.catalog()?
            .read_team_permanent_transfer_generation(workspace, transfer_id, reader)
            .map_err(RuntimeError::persistence)
    }
}
