//! Authenticated first-attempt admission and historical Source evidence storage.
use std::net::SocketAddr;

use forge_domain::{
    AuthenticatedPrincipal, ForgeBackendId, HomeTransferSourceGrant, HomeTransferSourceRequest,
    OrchestrationIssuerId, WorkspaceId,
};
use heterarchy_orchestration_operations::{
    TeamPermanentTransferSourceDispatchCommand, TeamPermanentTransferSourceDispatchOutcome,
    TeamPermanentTransferSourceDispatchReadback, TeamPermanentTransferSourceReceiptCommand,
    TeamPermanentTransferSourceReceiptOutcome, TeamPermanentTransferSourceWireIdentity,
    WorkspaceProjectScope,
};
use uuid::Uuid;

use super::{
    OrchestrationRuntime, RuntimeError, TeamPermanentTransferActorTarget,
    TeamPermanentTransferPreparationReadSpec,
};

/// Backend-built preflight data. It carries no bearer or Source send capability.
#[derive(Debug, Clone)]
pub struct TeamPermanentTransferSourceDispatchSpec {
    pub actor: TeamPermanentTransferActorTarget,
    pub preparation_id: Uuid,
    pub transfer_id: Uuid,
    pub generation: u64,
    pub expected_workspace_leadership_revision: i64,
    pub request_id: Uuid,
    pub source_route: SocketAddr,
    pub source_scope: WorkspaceProjectScope,
    pub backend_id: ForgeBackendId,
    pub issuer_id: OrchestrationIssuerId,
    pub service_destination_id: Uuid,
    pub source_request: HomeTransferSourceRequest,
    pub grant: HomeTransferSourceGrant,
    pub wire_identity: TeamPermanentTransferSourceWireIdentity,
}

impl OrchestrationRuntime {
    /// Commit one first-send authorization under current Operations authority.
    /// The caller sends only on Created, after this call has released its lock.
    /// # Errors
    /// Rejects foreign actors, stale preparation/authority, bounds and storage failures.
    pub fn commit_team_permanent_transfer_source_dispatch(
        &self,
        spec: TeamPermanentTransferSourceDispatchSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<TeamPermanentTransferSourceDispatchOutcome, RuntimeError> {
        let workspace = WorkspaceId::from_uuid(spec.actor.workspace_id);
        let (actor, participation) = crate::teams::participant_actor(
            workspace,
            spec.actor.activity_id,
            spec.actor.session_id,
        )?;
        self.require_participation(participation, principal)?;
        let command = TeamPermanentTransferSourceDispatchCommand::new(
            workspace,
            spec.preparation_id,
            spec.transfer_id,
            spec.generation,
            spec.expected_workspace_leadership_revision,
            spec.request_id,
            actor,
            spec.source_route,
            spec.source_scope,
            spec.backend_id,
            spec.issuer_id,
            spec.service_destination_id,
            spec.source_request,
            spec.grant,
            spec.wire_identity,
        )
        .map_err(RuntimeError::persistence)?;
        self.catalog()?
            .commit_team_permanent_transfer_source_dispatch(&command, super::trusted_server_time)
            .map_err(RuntimeError::persistence)
    }

    /// Read immutable dispatch/Source history through stable authorship or current O.
    /// # Errors
    /// Rejects foreign actors and corrupt evidence; unrelated readers receive None.
    pub fn read_team_permanent_transfer_source_dispatch(
        &self,
        spec: TeamPermanentTransferPreparationReadSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<TeamPermanentTransferSourceDispatchReadback>, RuntimeError> {
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
        self.catalog()?
            .read_team_permanent_transfer_source_dispatch(workspace, spec.preparation_id, actor)
            .map_err(RuntimeError::persistence)
    }

    /// Trusted Backend ingestion after authenticated Source fetch and pin checks.
    /// No public proof-upload route may call this with user-supplied evidence.
    /// Current business leadership is not re-authorized for this historical fact.
    /// # Errors
    /// Rejects a changed envelope, malformed or contradictory Source evidence, and storage failures.
    pub fn record_team_permanent_transfer_source_receipt(
        &self,
        command: &TeamPermanentTransferSourceReceiptCommand,
    ) -> Result<TeamPermanentTransferSourceReceiptOutcome, RuntimeError> {
        self.catalog()?
            .record_team_permanent_transfer_source_receipt(command)
            .map_err(RuntimeError::persistence)
    }
}
