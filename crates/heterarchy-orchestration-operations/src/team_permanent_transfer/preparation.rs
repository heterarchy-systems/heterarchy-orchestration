//! Operations-owned preparation before any permanent-transfer source dispatch.
//!
//! The immutable receipt and target admission barrier commit together. This
//! records neither a Forge fence nor settlement nor a Home membership change.

pub(super) mod admission;
mod identity;
mod pending_registration;
pub(crate) mod schema;
mod store;

use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, TimestampMillis, WorkspaceId};
use rusqlite::{Connection, TransactionBehavior};
use uuid::Uuid;

use super::{
    ActorSnapshot, actor_input_is_valid, authorize_current_orchestrator,
    consent_reader_has_orchestrator_access, load_intent_in_workspace,
};
use crate::{
    OperationalActor, OperationsCatalog, OperationsError, WorkspaceParticipationId, teams,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamPermanentTransferPreparationCommand {
    preparation_id: Uuid,
    workspace_id: WorkspaceId,
    transfer_id: Uuid,
    expected_generation: u64,
    expected_workspace_leadership_revision: i64,
    source_leader_consent_id: Uuid,
    target_acceptance_id: Uuid,
    actor: OperationalActor,
}

impl TeamPermanentTransferPreparationCommand {
    /// Pin the exact generation, consents and current Orchestrator authority.
    ///
    /// # Errors
    /// Rejects nil identities, invalid revisions and non-Agent authors.
    #[allow(
        clippy::too_many_arguments,
        reason = "one exact preparation authority tuple"
    )]
    pub fn new(
        preparation_id: Uuid,
        workspace_id: WorkspaceId,
        transfer_id: Uuid,
        expected_generation: u64,
        expected_workspace_leadership_revision: i64,
        source_leader_consent_id: Uuid,
        target_acceptance_id: Uuid,
        actor: OperationalActor,
    ) -> Result<Self, OperationsError> {
        if [
            preparation_id,
            workspace_id.into_uuid(),
            transfer_id,
            source_leader_consent_id,
            target_acceptance_id,
        ]
        .iter()
        .any(Uuid::is_nil)
            || source_leader_consent_id == target_acceptance_id
            || expected_generation == 0
            || i64::try_from(expected_generation).is_err()
            || expected_workspace_leadership_revision < 1
            || !actor_input_is_valid(actor)
        {
            return Err(OperationsError::Bounds(
                "permanent transfer preparation identity",
            ));
        }
        Ok(Self {
            preparation_id,
            workspace_id,
            transfer_id,
            expected_generation,
            expected_workspace_leadership_revision,
            source_leader_consent_id,
            target_acceptance_id,
            actor,
        })
    }

    #[must_use]
    pub const fn preparation_id(&self) -> Uuid {
        self.preparation_id
    }
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }
    #[must_use]
    pub const fn transfer_id(&self) -> Uuid {
        self.transfer_id
    }
    #[must_use]
    pub const fn expected_generation(&self) -> u64 {
        self.expected_generation
    }
    #[must_use]
    pub const fn expected_workspace_leadership_revision(&self) -> i64 {
        self.expected_workspace_leadership_revision
    }
    #[must_use]
    pub const fn source_leader_consent_id(&self) -> Uuid {
        self.source_leader_consent_id
    }
    #[must_use]
    pub const fn target_acceptance_id(&self) -> Uuid {
        self.target_acceptance_id
    }
    #[must_use]
    pub const fn actor(&self) -> OperationalActor {
        self.actor
    }
}

/// Historical Operations preparation, not permission to execute or change Home.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamPermanentTransferPreparationReceipt {
    payload: store::PreparationPayload,
    digest: [u8; 32],
    recorded_at: TimestampMillis,
    target_participation: WorkspaceParticipationId,
    author_participation: WorkspaceParticipationId,
    replayed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeamPermanentTransferBindingOrigin {
    Registration(Uuid),
    LegacyAdoption(Uuid),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamPermanentTransferPreparationBinding(identity::CapturedBinding);

impl TeamPermanentTransferPreparationBinding {
    #[must_use]
    pub const fn participant_id(self) -> Uuid {
        Uuid::from_bytes(self.0.participant_id)
    }
    #[must_use]
    pub const fn activity_id(self) -> ActivityId {
        ActivityId::from_uuid(Uuid::from_bytes(self.0.activity_id))
    }
    #[must_use]
    pub const fn agent_id(self) -> AgentInstanceId {
        AgentInstanceId::from_uuid(Uuid::from_bytes(self.0.agent_id))
    }
    pub const fn session_id(self) -> AgentSessionId {
        AgentSessionId::from_uuid(Uuid::from_bytes(self.0.session_id))
    }
    #[must_use]
    pub const fn source_request_id(self) -> Uuid {
        Uuid::from_bytes(self.0.source_request_id)
    }
    /// # Errors
    /// Rejects malformed historical principal fields.
    pub fn principal(self) -> Result<forge_domain::AuthenticatedPrincipal, OperationsError> {
        crate::registration::decode_principal_fields(
            self.0.principal_kind,
            self.0.principal_id.as_ref().map(<[u8; 16]>::as_slice),
        )
    }
    #[must_use]
    pub fn origin(self) -> TeamPermanentTransferBindingOrigin {
        if self.0.source_kind == 0 {
            TeamPermanentTransferBindingOrigin::Registration(self.source_request_id())
        } else {
            TeamPermanentTransferBindingOrigin::LegacyAdoption(self.source_request_id())
        }
    }
}

impl TeamPermanentTransferPreparationReceipt {
    #[must_use]
    pub const fn preparation_id(&self) -> Uuid {
        Uuid::from_bytes(self.payload.preparation_id)
    }
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        WorkspaceId::from_uuid(Uuid::from_bytes(self.payload.workspace_id))
    }
    #[must_use]
    pub const fn operations_destination_id(&self) -> Uuid {
        self.payload.destination_id()
    }
    #[must_use]
    pub const fn transfer_id(&self) -> Uuid {
        Uuid::from_bytes(self.payload.transfer_id)
    }
    #[must_use]
    pub const fn source_team_id(&self) -> Uuid {
        Uuid::from_bytes(self.payload.source_team)
    }
    #[must_use]
    pub const fn destination_team_id(&self) -> Uuid {
        Uuid::from_bytes(self.payload.destination_team)
    }
    #[must_use]
    pub const fn intent_digest(&self) -> [u8; 32] {
        self.payload.identity.intent_digest
    }
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.payload.generation
    }
    #[must_use]
    pub const fn workspace_leadership_revision(&self) -> i64 {
        self.payload.orchestrator_revision
    }
    #[must_use]
    pub const fn source_leader_consent_id(&self) -> Uuid {
        Uuid::from_bytes(self.payload.source_leader_consent_id)
    }
    #[must_use]
    pub const fn target_acceptance_id(&self) -> Uuid {
        Uuid::from_bytes(self.payload.target_acceptance_id)
    }
    #[must_use]
    pub const fn target_profile_id(&self) -> Uuid {
        Uuid::from_bytes(self.payload.identity.profile_id)
    }
    #[must_use]
    pub const fn target_participation_id(&self) -> WorkspaceParticipationId {
        self.target_participation
    }
    #[must_use]
    pub const fn actor(&self) -> OperationalActor {
        OperationalActor::Participant {
            activity_id: ActivityId::from_uuid(Uuid::from_bytes(self.payload.actor.activity)),
            agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(self.payload.actor.agent)),
            session_id: AgentSessionId::from_uuid(Uuid::from_bytes(self.payload.actor.session)),
        }
    }
    #[must_use]
    pub const fn actor_participation_id(&self) -> WorkspaceParticipationId {
        self.author_participation
    }
    #[must_use]
    pub const fn actor_profile_id(&self) -> Uuid {
        Uuid::from_bytes(self.payload.actor.profile)
    }
    #[must_use]
    pub const fn actor_participant_id(&self) -> Uuid {
        Uuid::from_bytes(self.payload.actor.participant)
    }
    #[must_use]
    pub fn actor_display_name(&self) -> &str {
        &self.payload.actor.display_name
    }
    #[must_use]
    pub fn participant_ids(&self) -> impl ExactSizeIterator<Item = Uuid> + '_ {
        self.payload.identity.participant_ids()
    }
    #[must_use]
    pub const fn participation_revision(&self) -> u64 {
        self.payload.identity.participation_revision
    }
    #[must_use]
    pub const fn source_membership_revision(&self) -> u64 {
        self.payload.identity.source_membership_revision
    }
    #[must_use]
    pub fn sessions(&self) -> impl ExactSizeIterator<Item = AgentSessionId> + '_ {
        self.payload.identity.session_ids()
    }
    pub fn bindings(
        &self,
    ) -> impl ExactSizeIterator<Item = TeamPermanentTransferPreparationBinding> + '_ {
        self.payload
            .identity
            .bindings
            .iter()
            .copied()
            .map(TeamPermanentTransferPreparationBinding)
    }
    #[must_use]
    pub const fn recorded_at(&self) -> TimestampMillis {
        self.recorded_at
    }
    #[must_use]
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
    #[must_use]
    pub const fn session_count(&self) -> usize {
        self.payload.identity.session_count()
    }
    #[must_use]
    pub const fn binding_count(&self) -> usize {
        self.payload.identity.binding_count()
    }
    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

impl OperationsCatalog {
    /// Freeze target identity and the transfer generation in one owner commit.
    /// Same-ID mutation replay keeps the original physical actor tuple; a
    /// rebound/alias author uses the separate stable-author historical query.
    ///
    /// # Errors
    /// Rejects changed heads/consents/authority, unresolved identity, incompatible
    /// custody and malformed source history. Unconfirmed commit requires readback.
    pub fn prepare_team_permanent_transfer(
        &mut self,
        command: &TeamPermanentTransferPreparationCommand,
        trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
    ) -> Result<TeamPermanentTransferPreparationReceipt, OperationsError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        teams::ensure_verified_workspace(&tx, command.workspace_id)?;
        let actor = ActorSnapshot::resolve(&tx, command.workspace_id, command.actor)?;
        if let Some(original) = store::author(&tx, command.workspace_id, command.preparation_id)?
            && original != (actor.participation_id.into_uuid(), actor.profile_id)
        {
            return Err(OperationsError::Conflict(
                "permanent transfer preparation author",
            ));
        }
        if let Some(mut existing) = store::load(&tx, command.workspace_id, command.preparation_id)?
        {
            if !store::matches_command(&existing, command, &actor) {
                return Err(OperationsError::Conflict(
                    "permanent transfer preparation replay",
                ));
            }
            existing.replayed = true;
            tx.commit()?;
            return Ok(existing);
        }
        let current = authorize_current_orchestrator(&tx, command.workspace_id, &actor)?;
        if current != command.expected_workspace_leadership_revision {
            return Err(OperationsError::Conflict(
                "permanent transfer preparation Orchestrator revision",
            ));
        }
        let intent = load_intent_in_workspace(&tx, command.workspace_id, command.transfer_id)?
            .ok_or(OperationsError::Conflict(
                "permanent transfer preparation intent missing",
            ))?;
        let high_water = admission::validate(&tx, command, &intent)?;
        let captured = identity::capture(&tx, &intent)?;
        let now = trusted_now()?;
        if now.get() < high_water.max(captured.observed_at_ms_high_water()) {
            return Err(OperationsError::Conflict(
                "permanent transfer preparation time regression",
            ));
        }
        let payload = store::PreparationPayload::new(
            command,
            self.destination,
            &actor,
            captured,
            &intent,
            now,
        );
        store::insert(&tx, &payload, &intent)?;
        let result = store::load(&tx, command.workspace_id, command.preparation_id)?.ok_or(
            OperationsError::Corrupt("permanent transfer preparation readback"),
        )?;
        match tx.commit() {
            Ok(()) => Ok(result),
            Err(source) => match store::load(
                &self.connection,
                command.workspace_id,
                command.preparation_id,
            ) {
                Ok(Some(mut existing)) if store::matches_command(&existing, command, &actor) => {
                    existing.replayed = true;
                    Ok(existing)
                }
                Ok(_) | Err(_) => Err(OperationsError::OperationCommitUnconfirmed {
                    workspace_id: command.workspace_id,
                    request_id: command.preparation_id,
                    source,
                }),
            },
        }
    }

    /// Recover one immutable preparation as its stable Workspace Participation
    /// and Profile author (including aliases) or the current Orchestrator.
    ///
    /// # Errors
    /// Rejects malformed/current actor bindings and corrupt authorized evidence.
    /// Unrelated readers and missing records receive None.
    pub fn read_team_permanent_transfer_preparation(
        &self,
        workspace: WorkspaceId,
        preparation: Uuid,
        actor_input: OperationalActor,
    ) -> Result<Option<TeamPermanentTransferPreparationReceipt>, OperationsError> {
        if workspace.into_uuid().is_nil()
            || preparation.is_nil()
            || !actor_input_is_valid(actor_input)
        {
            return Err(OperationsError::Bounds(
                "permanent transfer preparation read identity",
            ));
        }
        let tx = self.connection.unchecked_transaction()?;
        teams::ensure_verified_workspace(&tx, workspace)?;
        let actor = ActorSnapshot::resolve(&tx, workspace, actor_input)?;
        let Some((participation, profile)) = store::author(&tx, workspace, preparation)? else {
            return Ok(None);
        };
        if (participation, profile) != (actor.participation_id.into_uuid(), actor.profile_id)
            && !consent_reader_has_orchestrator_access(authorize_current_orchestrator(
                &tx, workspace, &actor,
            ))?
        {
            return Ok(None);
        }
        let result = store::load(&tx, workspace, preparation)?;
        tx.commit()?;
        Ok(result)
    }
}

pub(crate) use admission::{
    ensure_profile_open, ensure_session_open, ensure_team_open, ensure_unscoped_open,
};

pub(super) fn is_preparing(
    connection: &Connection,
    workspace: WorkspaceId,
    transfer: Uuid,
) -> Result<bool, OperationsError> {
    store::contains_transfer(connection, workspace, transfer)
}

/// Read and validate one current preparation within a caller-owned transaction.
/// Source dispatch uses this only after taking its IMMEDIATE admission lock.
pub(super) fn load_in_transaction(
    connection: &Connection,
    workspace: WorkspaceId,
    preparation: Uuid,
) -> Result<Option<TeamPermanentTransferPreparationReceipt>, OperationsError> {
    store::load(connection, workspace, preparation)
}

pub(super) fn ensure_unprepared(
    connection: &Connection,
    workspace: WorkspaceId,
    transfer: Uuid,
) -> Result<(), OperationsError> {
    if is_preparing(connection, workspace, transfer)? {
        return Err(OperationsError::Conflict(
            "permanent transfer preparation already started",
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn registration_census_for_test(
    connection: &Connection,
    workspace: WorkspaceId,
    profile: Uuid,
    participation: Uuid,
) -> Result<i64, OperationsError> {
    pending_registration::ensure_settled(connection, workspace, profile, participation)
        .map(|census| census.observed_high_water_ms())
}
