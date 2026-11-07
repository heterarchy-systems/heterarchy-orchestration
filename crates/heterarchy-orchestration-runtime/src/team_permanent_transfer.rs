//! Runtime boundary for a permanent Home-Team transfer proposal and its two
//! point-in-time consent statements.
//!
//! These operations do not change Team membership, assign the held slot, or
//! establish work/lease settlement or handoff. Reusing an authenticated
//! principal across Sessions does not prove that different human Agents acted.

use forge_domain::{AuthenticatedPrincipal, WorkspaceId};
use heterarchy_orchestration_operations::{
    TeamPermanentTransferConsentCommand, TeamPermanentTransferConsentKind,
    TeamPermanentTransferConsentReceipt, TeamPermanentTransferDestinationSlot,
    TeamPermanentTransferIntentCommand, TeamPermanentTransferIntentReceipt,
    TeamPermanentTransferRevisions, TeamPermanentTransferScope, TeamPermanentTransferSnapshot,
    WorkspaceParticipationId,
};
use uuid::Uuid;

use crate::{OrchestrationRuntime, RuntimeError};

mod preparation;
mod renewal;
mod source;
pub use preparation::{
    TeamPermanentTransferPreparationReadSpec, TeamPermanentTransferPreparationSpec,
};
pub use renewal::TeamPermanentTransferRenewalSpec;
pub use source::TeamPermanentTransferSourceDispatchSpec;

/// Authenticated Workspace, Activity and Session selector for one transfer action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamPermanentTransferActorTarget {
    /// Workspace containing the source and destination Teams.
    pub workspace_id: Uuid,
    /// Current Activity of the acting Profile Session.
    pub activity_id: Uuid,
    /// Open Session authenticated by the request principal.
    pub session_id: Uuid,
}

/// Exact Orchestrator proposal against one held destination slot and one
/// current source Home membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamPermanentTransferIntentSpec {
    /// Current Workspace Orchestrator.
    pub actor: TeamPermanentTransferActorTarget,
    /// Exact proposal identity and source/destination Teams.
    pub transfer_id: Uuid,
    pub source_team_id: Uuid,
    pub destination_team_id: Uuid,
    /// Exact target Home membership already present in the Workspace.
    pub target_participation_id: Uuid,
    /// Exact approved destination slot identity.
    pub destination_request_id: Uuid,
    pub destination_decision_id: Uuid,
    pub destination_ordinal: u8,
    /// Revisions observed by the current Orchestrator and source Team Leader.
    pub expected_destination_team_revision: u64,
    pub expected_destination_capacity_revision: u64,
    pub expected_workspace_leadership_revision: i64,
    pub expected_source_membership_revision: u64,
    pub expected_source_leadership_revision: i64,
}

/// One authenticated point-in-time source Leader consent or target acceptance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamPermanentTransferConsentSpec {
    /// Current consentor Session. Operations checks the exact role/Profile.
    pub actor: TeamPermanentTransferActorTarget,
    /// Immutable consent receipt and proposal identities.
    pub receipt_id: Uuid,
    pub transfer_id: Uuid,
    /// Source Team Leader or target Profile consent category.
    pub kind: TeamPermanentTransferConsentKind,
}

/// Exact original consent selector and an independently authenticated reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamPermanentTransferConsentReadSpec {
    /// Immutable original consent intent retained for exact response validation.
    pub intent: TeamPermanentTransferConsentSpec,
    /// Current consentor rebind or current Workspace Orchestrator reader.
    pub reader: TeamPermanentTransferActorTarget,
}

impl OrchestrationRuntime {
    /// Record one current Workspace Orchestrator proposal. This creates only
    /// transfer intent and source/destination snapshots; no membership or work
    /// authority changes here.
    ///
    /// # Errors
    /// Rejects a foreign or closed actor Session, malformed exact IDs/revisions,
    /// stale Orchestrator/source membership/leadership, or catalog failure.
    pub fn propose_team_permanent_transfer(
        &self,
        spec: TeamPermanentTransferIntentSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<TeamPermanentTransferIntentReceipt, RuntimeError> {
        let workspace = WorkspaceId::from_uuid(spec.actor.workspace_id);
        let (actor, participation) = crate::teams::participant_actor(
            workspace,
            spec.actor.activity_id,
            spec.actor.session_id,
        )?;
        self.require_participation(participation, principal)?;
        let target_participation_id =
            WorkspaceParticipationId::from_uuid(spec.target_participation_id)
                .map_err(RuntimeError::persistence)?;
        let scope = TeamPermanentTransferScope::new(
            workspace,
            spec.source_team_id,
            spec.destination_team_id,
            target_participation_id,
        );
        let destination = TeamPermanentTransferDestinationSlot::new(
            spec.destination_request_id,
            spec.destination_decision_id,
            spec.destination_ordinal,
        );
        let revisions = TeamPermanentTransferRevisions::new(
            spec.expected_destination_team_revision,
            spec.expected_destination_capacity_revision,
            spec.expected_workspace_leadership_revision,
            spec.expected_source_membership_revision,
            spec.expected_source_leadership_revision,
        );
        let command = TeamPermanentTransferIntentCommand::new(
            spec.transfer_id,
            scope,
            destination,
            revisions,
            actor,
        )
        .map_err(RuntimeError::persistence)?;
        self.catalog()?
            .propose_team_permanent_transfer(&command, trusted_server_time)
            .map_err(RuntimeError::persistence)
    }

    /// Record one exact source Home Leader consent or target Profile acceptance.
    /// This only records a point-in-time statement; it does not perform transfer
    /// cutover or prove independent principals across authenticated Sessions.
    ///
    /// # Errors
    /// Rejects a foreign or closed actor Session, stale authority, a Session
    /// outside the exact consent role, duplicate consent, or catalog failure.
    pub fn record_team_permanent_transfer_consent(
        &self,
        spec: TeamPermanentTransferConsentSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<TeamPermanentTransferConsentReceipt, RuntimeError> {
        let workspace = WorkspaceId::from_uuid(spec.actor.workspace_id);
        let (actor, participation) = crate::teams::participant_actor(
            workspace,
            spec.actor.activity_id,
            spec.actor.session_id,
        )?;
        self.require_participation(participation, principal)?;
        let command = TeamPermanentTransferConsentCommand::new(
            spec.receipt_id,
            workspace,
            spec.transfer_id,
            spec.kind,
            actor,
        )
        .map_err(RuntimeError::persistence)?;
        self.catalog()?
            .record_team_permanent_transfer_consent(&command, trusted_server_time)
            .map_err(RuntimeError::persistence)
    }

    /// Read one exact consent receipt by its original transfer and receipt IDs.
    /// The original intent selector remains separate from the current reader;
    /// Operations permits the stable original consentor or current Workspace O.
    ///
    /// # Errors
    /// Rejects a cross-Workspace reader, foreign or closed reader Session,
    /// malformed IDs, or corrupt authorized receipt history.
    pub fn read_team_permanent_transfer_consent(
        &self,
        spec: TeamPermanentTransferConsentReadSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<TeamPermanentTransferConsentReceipt>, RuntimeError> {
        let workspace = WorkspaceId::from_uuid(spec.intent.actor.workspace_id);
        if spec.reader.workspace_id != workspace.into_uuid() {
            return Err(RuntimeError::InvalidInput);
        }
        let (reader, participation) = crate::teams::participant_actor(
            workspace,
            spec.reader.activity_id,
            spec.reader.session_id,
        )?;
        self.require_participation(participation, principal)?;
        let catalog = self.catalog()?;
        catalog
            .read_team_permanent_transfer_consent(
                workspace,
                spec.intent.transfer_id,
                spec.intent.receipt_id,
                reader,
            )
            .map_err(RuntimeError::persistence)
    }

    /// Read the immutable proposal and its source Leader/target Agent consent
    /// statements as the original proposer or current Workspace Orchestrator.
    /// Unauthorized, absent and cross-Workspace IDs remain `None` at Operations.
    ///
    /// # Errors
    /// Rejects a foreign or closed reader Session, malformed transfer identity,
    /// or corrupt durable proposal/consent history.
    pub fn read_team_permanent_transfer_snapshot(
        &self,
        actor: TeamPermanentTransferActorTarget,
        transfer_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<TeamPermanentTransferSnapshot>, RuntimeError> {
        let workspace = WorkspaceId::from_uuid(actor.workspace_id);
        let (reader, participation) =
            crate::teams::participant_actor(workspace, actor.activity_id, actor.session_id)?;
        self.require_participation(participation, principal)?;
        let mut catalog = self.catalog()?;
        catalog
            .read_team_permanent_transfer_snapshot(workspace, transfer_id, reader)
            .map_err(RuntimeError::persistence)
    }
}

fn trusted_server_time()
-> Result<forge_domain::TimestampMillis, heterarchy_orchestration_operations::OperationsError> {
    use std::time::{SystemTime, UNIX_EPOCH};

    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| {
        heterarchy_orchestration_operations::OperationsError::Bounds(
            "permanent Team transfer trusted clock",
        )
    })?;
    let millis = i64::try_from(elapsed.as_millis()).map_err(|_| {
        heterarchy_orchestration_operations::OperationsError::Bounds(
            "permanent Team transfer trusted clock",
        )
    })?;
    forge_domain::TimestampMillis::new(millis).map_err(|_| {
        heterarchy_orchestration_operations::OperationsError::Bounds(
            "permanent Team transfer trusted clock",
        )
    })
}
