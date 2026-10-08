//! Phase-D additive registration journal for a new Runtime Session generation.
//!
//! The legacy registration/membership tables remain immutable evidence for the
//! first JOINED identity. A replacement registration reserves a new Forge
//! Agent binding, reuses the existing WorkspaceParticipation/participant, and
//! records its own Pending -> Reserved -> `MembershipCommitted` -> Ready state.
//! It never creates a second JOINED event or logical member.

use crate::{
    OperationsCatalog, OperationsError, WorkspaceParticipationSessionExpectation,
    WorkspaceParticipationSessionRebindReceipt,
    binding_observation::{BindingActivationObservation, BindingReservationObservation},
    effect_binding, membership, registration, wire,
};
use forge_domain::{
    AgentSessionId, AgentSessionRevision, AgentSessionState, RegisteredAgentSession,
    TimestampMillis, WorkspaceId,
};
use forge_persistence::{
    AgentBindingActivation, AgentBindingReservation, AgentBindingReservationRequest,
};
use heterarchy_orchestration_sessions::{
    AgentProfileId, AgentProfileLogicalSessionLink, AgentProfileRevision,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const REPLACEMENT_COMMIT_DIGEST_DOMAIN: &[u8] =
    b"heterarchy.workspace-participation.replacement-membership.v1\0";

const REPLACEMENT_REGISTRATIONS_SQL: &str = "CREATE TABLE ops_workspace_participation_replacement_registrations (request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), migration_id BLOB NOT NULL CHECK(length(migration_id)=16), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), destination_id BLOB NOT NULL CHECK(length(destination_id)=16), participation_id BLOB NOT NULL CHECK(length(participation_id)=16 AND participation_id != zeroblob(16)), agent_profile_id BLOB NOT NULL CHECK(length(agent_profile_id)=16 AND agent_profile_id != zeroblob(16)), profile_revision INTEGER NOT NULL CHECK(profile_revision>0), profile_mapping_request_id BLOB NOT NULL CHECK(length(profile_mapping_request_id)=16 AND profile_mapping_request_id != zeroblob(16)), previous_session_id BLOB NOT NULL CHECK(length(previous_session_id)=16 AND previous_session_id != zeroblob(16)), new_session_id BLOB NOT NULL CHECK(length(new_session_id)=16 AND new_session_id != zeroblob(16) AND new_session_id != previous_session_id), new_session_revision INTEGER NOT NULL CHECK(new_session_revision>0), participant_id BLOB NOT NULL CHECK(length(participant_id)=16 AND participant_id != zeroblob(16)), participant_membership_revision INTEGER NOT NULL CHECK(participant_membership_revision>0), expected_participation_revision INTEGER NOT NULL CHECK(expected_participation_revision>0), activity_id BLOB NOT NULL CHECK(length(activity_id)=16), agent_id BLOB NOT NULL CHECK(length(agent_id)=16 AND agent_id != zeroblob(16)), request_wire BLOB NOT NULL CHECK(length(request_wire) BETWEEN 1 AND 4096), phase INTEGER NOT NULL CHECK(phase IN(0,1,2,3)), freeze_revision INTEGER CHECK(freeze_revision IS NULL OR freeze_revision>0), reserved_at_ms INTEGER CHECK(reserved_at_ms IS NULL OR reserved_at_ms>=0), commit_digest BLOB CHECK(commit_digest IS NULL OR (length(commit_digest)=32 AND commit_digest != zeroblob(32))), committed_at_ms INTEGER CHECK(committed_at_ms IS NULL OR committed_at_ms>=0), joined_event_id BLOB CHECK(joined_event_id IS NULL OR (length(joined_event_id)=16 AND joined_event_id != zeroblob(16))), activated_at_ms INTEGER CHECK(activated_at_ms IS NULL OR activated_at_ms>=0), UNIQUE(workspace_id,new_session_id), UNIQUE(workspace_id,agent_id), FOREIGN KEY(migration_id) REFERENCES ops_imports(migration_id), FOREIGN KEY(destination_id) REFERENCES ops_catalog_identity(destination_id), FOREIGN KEY(workspace_id,agent_profile_id) REFERENCES ops_workspace_participations(workspace_id,agent_profile_id), FOREIGN KEY(workspace_id,participant_id) REFERENCES ops_logical_members(workspace_id,participant_id), CHECK((phase=0 AND freeze_revision IS NULL AND reserved_at_ms IS NULL AND commit_digest IS NULL AND committed_at_ms IS NULL AND joined_event_id IS NULL AND activated_at_ms IS NULL) OR (phase=1 AND freeze_revision IS NOT NULL AND reserved_at_ms IS NOT NULL AND commit_digest IS NULL AND committed_at_ms IS NULL AND joined_event_id IS NULL AND activated_at_ms IS NULL) OR (phase=2 AND freeze_revision IS NOT NULL AND reserved_at_ms IS NOT NULL AND commit_digest IS NOT NULL AND committed_at_ms IS NOT NULL AND joined_event_id IS NOT NULL AND activated_at_ms IS NULL) OR (phase=3 AND freeze_revision IS NOT NULL AND reserved_at_ms IS NOT NULL AND commit_digest IS NOT NULL AND committed_at_ms IS NOT NULL AND joined_event_id IS NOT NULL AND activated_at_ms IS NOT NULL)))";

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![(
        "table",
        "ops_workspace_participation_replacement_registrations",
        REPLACEMENT_REGISTRATIONS_SQL,
    )]
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    connection.execute_batch(REPLACEMENT_REGISTRATIONS_SQL)?;
    Ok(())
}

#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceParticipationReplacementProgress {
    request_id: Uuid,
    migration_id: Uuid,
    workspace_id: WorkspaceId,
    participation_id: crate::WorkspaceParticipationId,
    profile_id: AgentProfileId,
    profile_revision: AgentProfileRevision,
    profile_mapping_request_id: Uuid,
    previous_session_id: AgentSessionId,
    new_session_revision: AgentSessionRevision,
    participant_id: Uuid,
    participant_membership_revision: u64,
    expected_participation_revision: u64,
    request: registration::RegistrationRequest,
    phase: registration::RegistrationPhase,
    freeze_revision: Option<u64>,
    reserved_at: Option<TimestampMillis>,
    commit_digest: Option<[u8; 32]>,
    committed_at: Option<TimestampMillis>,
    joined_event_id: Option<Uuid>,
    activated_at: Option<TimestampMillis>,
}

impl WorkspaceParticipationReplacementProgress {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn migration_id(&self) -> Uuid {
        self.migration_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn participation_id(&self) -> crate::WorkspaceParticipationId {
        self.participation_id
    }

    #[must_use]
    pub const fn profile_id(&self) -> AgentProfileId {
        self.profile_id
    }

    #[must_use]
    pub const fn profile_revision(&self) -> AgentProfileRevision {
        self.profile_revision
    }

    #[must_use]
    pub const fn profile_mapping_request_id(&self) -> Uuid {
        self.profile_mapping_request_id
    }

    pub const fn previous_session_id(&self) -> AgentSessionId {
        self.previous_session_id
    }

    pub const fn new_session_revision(&self) -> AgentSessionRevision {
        self.new_session_revision
    }

    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }

    #[must_use]
    pub const fn participant_membership_revision(&self) -> u64 {
        self.participant_membership_revision
    }

    #[must_use]
    pub const fn expected_participation_revision(&self) -> u64 {
        self.expected_participation_revision
    }

    pub const fn request(&self) -> &registration::RegistrationRequest {
        &self.request
    }

    pub const fn phase(&self) -> registration::RegistrationPhase {
        self.phase
    }

    #[must_use]
    pub const fn freeze_revision(&self) -> Option<u64> {
        self.freeze_revision
    }

    #[must_use]
    pub const fn reserved_at(&self) -> Option<TimestampMillis> {
        self.reserved_at
    }

    #[must_use]
    pub const fn commit_digest(&self) -> Option<[u8; 32]> {
        self.commit_digest
    }

    #[must_use]
    pub const fn committed_at(&self) -> Option<TimestampMillis> {
        self.committed_at
    }

    #[must_use]
    pub const fn joined_event_id(&self) -> Option<Uuid> {
        self.joined_event_id
    }

    #[must_use]
    pub const fn activated_at(&self) -> Option<TimestampMillis> {
        self.activated_at
    }
}

#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceParticipationReplacementCommitReceipt {
    request_id: Uuid,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
    binding: AgentBindingReservationRequest,
    commit_digest: [u8; 32],
    joined_event_id: Uuid,
    committed_at: TimestampMillis,
}

impl WorkspaceParticipationReplacementCommitReceipt {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }

    pub const fn binding(&self) -> &AgentBindingReservationRequest {
        &self.binding
    }

    #[must_use]
    pub const fn commit_digest(&self) -> [u8; 32] {
        self.commit_digest
    }

    #[must_use]
    pub const fn joined_event_id(&self) -> Uuid {
        self.joined_event_id
    }

    #[must_use]
    pub const fn committed_at(&self) -> TimestampMillis {
        self.committed_at
    }
}

#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceParticipationReplacementActivationReceipt {
    request_id: Uuid,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
    binding: AgentBindingReservationRequest,
    membership_digest: [u8; 32],
    activated_at: TimestampMillis,
}

impl WorkspaceParticipationReplacementActivationReceipt {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }

    pub const fn binding(&self) -> &AgentBindingReservationRequest {
        &self.binding
    }

    #[must_use]
    pub const fn membership_digest(&self) -> [u8; 32] {
        self.membership_digest
    }

    #[must_use]
    pub const fn activated_at(&self) -> TimestampMillis {
        self.activated_at
    }
}

#[derive(Debug)]
struct RawReplacement {
    request_id: Vec<u8>,
    migration_id: Vec<u8>,
    workspace_id: Vec<u8>,
    destination_id: Vec<u8>,
    participation_id: Vec<u8>,
    profile_id: Vec<u8>,
    profile_revision: i64,
    profile_mapping_request_id: Vec<u8>,
    previous_session_id: Vec<u8>,
    new_session_id: Vec<u8>,
    new_session_revision: i64,
    participant_id: Vec<u8>,
    participant_membership_revision: i64,
    expected_participation_revision: i64,
    request_wire: Vec<u8>,
    phase: i64,
    freeze_revision: Option<i64>,
    reserved_at_ms: Option<i64>,
    commit_digest: Option<Vec<u8>>,
    committed_at_ms: Option<i64>,
    joined_event_id: Option<Vec<u8>>,
    activated_at_ms: Option<i64>,
}

impl OperationsCatalog {
    /// Persist one replacement registration intent without creating membership
    /// or Forge effects.
    ///
    /// # Errors
    /// Rejects stale Profile/Session/participation observations, conflicting
    /// request replays, occupied identities, or unverified source delegation.
    pub fn prepare_workspace_participation_replacement(
        &mut self,
        migration_id: Uuid,
        target: WorkspaceParticipationSessionExpectation,
        profile_link: AgentProfileLogicalSessionLink,
        verified_new_session: &RegisteredAgentSession,
        request: &registration::RegistrationRequest,
    ) -> Result<WorkspaceParticipationReplacementProgress, OperationsError> {
        validate_prepare_inputs(target, profile_link, verified_new_session, request)?;
        let request_id = request.binding().request_id();
        let encoded = request.encode()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let import = registration::verified_import(&transaction, migration_id, self.destination)?;
        if import.workspace_id() != target.workspace_id() {
            return Err(OperationsError::Conflict("replacement import workspace"));
        }
        if transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_registration_requests WHERE request_id=?1)",
            params![request_id.as_bytes()],
            |row| row.get::<_, bool>(0),
        )? {
            return Err(OperationsError::Conflict("replacement request identity"));
        }
        if let Some(existing) = load_replacement(&transaction, request_id)? {
            let progress = decode_replacement(existing)?;
            validate_prepare_replay(
                &progress,
                migration_id,
                target,
                profile_link,
                verified_new_session,
                request,
            )?;
            commit_replacement(transaction, self.destination, request_id)?;
            return Ok(progress);
        }
        crate::leadership::work_roster::ensure_profile_admission_open(
            &transaction,
            target.workspace_id(),
            target.profile_id(),
        )?;
        crate::leadership::work_roster::ensure_session_admission_open(
            &transaction,
            target.workspace_id(),
            target.current_session_id(),
        )?;
        crate::leadership::work_roster::ensure_session_admission_open(
            &transaction,
            target.workspace_id(),
            verified_new_session.id(),
        )?;
        validate_current_target(&transaction, target, request)?;
        let occupied: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_workspace_participation_replacement_registrations WHERE workspace_id=?1 AND (new_session_id=?2 OR agent_id=?3) UNION ALL SELECT 1 FROM ops_registration_requests WHERE workspace_id=?1 AND (session_id=?2 OR agent_id=?3) UNION ALL SELECT 1 FROM ops_current_effect_bindings WHERE workspace_id=?1 AND (session_id=?2 OR agent_id=?3) UNION ALL SELECT 1 FROM ops_logical_members WHERE workspace_id=?1 AND session_id=?2 UNION ALL SELECT 1 FROM ops_workspace_participation_session_rebinds WHERE workspace_id=?1 AND new_session_id=?2)",
            params![
                target.workspace_id().into_uuid().as_bytes(),
                request.binding().session_id().into_uuid().as_bytes(),
                request.binding().agent_id().into_uuid().as_bytes(),
            ],
            |row| row.get(0),
        )?;
        if occupied {
            return Err(OperationsError::Conflict("replacement binding identity"));
        }
        transaction.execute(
            "INSERT INTO ops_workspace_participation_replacement_registrations(request_id,migration_id,workspace_id,destination_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,previous_session_id,new_session_id,new_session_revision,participant_id,participant_membership_revision,expected_participation_revision,activity_id,agent_id,request_wire,phase) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,0)",
            params![
                request_id.as_bytes(),
                migration_id.as_bytes(),
                target.workspace_id().into_uuid().as_bytes(),
                self.destination.as_bytes(),
                target.participation_id().into_uuid().as_bytes(),
                target.profile_id().into_uuid().as_bytes(),
                profile_link.profile_revision().get(),
                profile_link.request_id().as_bytes(),
                target.current_session_id().into_uuid().as_bytes(),
                verified_new_session.id().into_uuid().as_bytes(),
                verified_new_session.revision().get(),
                target.participant_id().as_bytes(),
                crate::catalog::signed(target.participant_membership_revision())?,
                crate::catalog::signed(target.participation_revision())?,
                request.binding().activity_id().into_uuid().as_bytes(),
                request.binding().agent_id().into_uuid().as_bytes(),
                encoded.as_slice(),
            ],
        )?;
        let progress = load_replacement(&transaction, request_id)?
            .ok_or(OperationsError::Corrupt("replacement registration insert"))
            .and_then(decode_replacement)?;
        commit_replacement(transaction, self.destination, request_id)?;
        Ok(progress)
    }

    /// Record Forge's exact reservation for a replacement registration.
    ///
    /// # Errors
    /// Rejects changed reservations, stale source delegation, or unknown
    /// replacement request identities.
    pub fn record_workspace_participation_replacement_reservation(
        &mut self,
        reservation: &AgentBindingReservation,
    ) -> Result<WorkspaceParticipationReplacementProgress, OperationsError> {
        let observation = BindingReservationObservation::from_native(reservation)?;
        self.record_workspace_participation_replacement_reservation_observation(&observation)
    }

    /// Record one checked Forge reservation readback for a replacement
    /// registration through the same validation and transaction as the native
    /// receipt path.
    ///
    /// # Errors
    /// Rejects changed reservations, stale source delegation, or unknown
    /// replacement request identities.
    pub fn record_workspace_participation_replacement_reservation_observation(
        &mut self,
        reservation: &BindingReservationObservation,
    ) -> Result<WorkspaceParticipationReplacementProgress, OperationsError> {
        let request_id = reservation.request().request_id();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let progress = load_progress(&transaction, request_id)?.ok_or(
            OperationsError::Conflict("replacement registration missing"),
        )?;
        let import =
            registration::verified_import(&transaction, progress.migration_id(), self.destination)?;
        validate_reservation(&progress, &import, reservation, self.destination)?;
        if progress.phase() == registration::RegistrationPhase::Pending {
            transaction.execute(
                "UPDATE ops_workspace_participation_replacement_registrations SET phase=1,freeze_revision=?1,reserved_at_ms=?2 WHERE request_id=?3 AND phase=0",
                params![
                    crate::catalog::signed(reservation.freeze_revision())?,
                    reservation.reserved_at().get(),
                    request_id.as_bytes(),
                ],
            )?;
        }
        let readback = load_progress(&transaction, request_id)?
            .ok_or(OperationsError::Corrupt("replacement reservation readback"))?;
        if readback.freeze_revision() != Some(reservation.freeze_revision())
            || readback.reserved_at() != Some(reservation.reserved_at())
        {
            return Err(OperationsError::Conflict("replacement reservation replay"));
        }
        commit_replacement(transaction, self.destination, request_id)?;
        Ok(readback)
    }

    /// Commit the replacement membership phase after the exact v22 Session
    /// rebind has durably succeeded. No JOINED event or inbox receipt is
    /// created; the original deterministic JOINED identity is reused.
    ///
    /// # Errors
    /// Rejects non-empty JOINED recipients, stale rebind evidence, changed
    /// replays, or source-delegation drift.
    pub fn commit_workspace_participation_replacement(
        &mut self,
        request_id: Uuid,
        rebind: WorkspaceParticipationSessionRebindReceipt,
        recipients: &[Uuid],
        committed_at: TimestampMillis,
    ) -> Result<WorkspaceParticipationReplacementCommitReceipt, OperationsError> {
        if !recipients.is_empty() {
            return Err(OperationsError::Conflict("replacement JOINED recipients"));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let progress = load_progress(&transaction, request_id)?.ok_or(
            OperationsError::Conflict("replacement registration missing"),
        )?;
        if progress.phase() != registration::RegistrationPhase::Reserved
            && progress.phase() != registration::RegistrationPhase::MembershipCommitted
            && progress.phase() != registration::RegistrationPhase::Ready
        {
            return Err(OperationsError::Conflict(
                "replacement registration not reserved",
            ));
        }
        validate_rebind_evidence(&progress, rebind)?;
        let import =
            registration::verified_import(&transaction, progress.migration_id(), self.destination)?;
        let freeze_revision = progress
            .freeze_revision()
            .ok_or(OperationsError::Corrupt("replacement freeze revision"))?;
        if freeze_revision != import.source_revision() {
            return Err(OperationsError::Conflict("replacement source revision"));
        }
        let digest = replacement_commit_digest(&progress, freeze_revision)?;
        let joined_event_id =
            membership::joined_event_id(progress.workspace_id(), progress.participant_id());
        ensure_existing_joined_event(
            &transaction,
            progress.workspace_id(),
            progress.participant_id(),
            joined_event_id,
        )?;
        if progress.phase() == registration::RegistrationPhase::Reserved {
            transaction.execute(
                "UPDATE ops_workspace_participation_replacement_registrations SET phase=2,commit_digest=?1,committed_at_ms=?2,joined_event_id=?3 WHERE request_id=?4 AND phase=1",
                params![
                    digest.as_slice(),
                    committed_at.get(),
                    joined_event_id.as_bytes(),
                    request_id.as_bytes(),
                ],
            )?;
        }
        let readback = load_progress(&transaction, request_id)?
            .ok_or(OperationsError::Corrupt("replacement commit readback"))?;
        let receipt = replacement_commit_receipt(&readback)?;
        if receipt.commit_digest() != digest
            || receipt.joined_event_id() != joined_event_id
            || receipt.committed_at() != committed_at
        {
            return Err(OperationsError::Conflict("replacement membership replay"));
        }
        commit_replacement(transaction, self.destination, request_id)?;
        Ok(receipt)
    }

    /// Record the real Forge activation for one replacement registration and
    /// publish its current effect binding under the already-existing logical
    /// participant.
    ///
    /// # Errors
    /// Rejects changed activation identity, source drift, missing commit, or
    /// effect-binding conflicts.
    pub fn record_workspace_participation_replacement_activation(
        &mut self,
        activation: &AgentBindingActivation,
    ) -> Result<WorkspaceParticipationReplacementActivationReceipt, OperationsError> {
        let observation = BindingActivationObservation::from_native(activation)?;
        self.record_workspace_participation_replacement_activation_observation(&observation)
    }

    /// Record one checked Forge activation readback for a replacement
    /// registration through the same validation and transaction as the native
    /// receipt path.
    ///
    /// # Errors
    /// Rejects changed activation identity, source drift, missing commit, or
    /// effect-binding conflicts.
    pub fn record_workspace_participation_replacement_activation_observation(
        &mut self,
        activation: &BindingActivationObservation,
    ) -> Result<WorkspaceParticipationReplacementActivationReceipt, OperationsError> {
        let request_id = activation.request_id();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let progress = load_progress(&transaction, request_id)?.ok_or(
            OperationsError::Conflict("replacement registration missing"),
        )?;
        if progress.phase() != registration::RegistrationPhase::MembershipCommitted
            && progress.phase() != registration::RegistrationPhase::Ready
        {
            return Err(OperationsError::Conflict(
                "replacement membership not committed",
            ));
        }
        let import =
            registration::verified_import(&transaction, progress.migration_id(), self.destination)?;
        validate_activation(&progress, &import, activation, self.destination)?;
        ensure_rebind_persisted(&transaction, &progress)?;
        if progress.phase() == registration::RegistrationPhase::MembershipCommitted {
            let binding = progress.request().binding();
            let _current = effect_binding::record_registration(
                &transaction,
                progress.workspace_id(),
                progress.participant_id(),
                binding.activity_id(),
                binding.session_id(),
                binding.agent_id(),
                request_id,
            )?;
            transaction.execute(
                "UPDATE ops_workspace_participation_replacement_registrations SET phase=3,activated_at_ms=?1 WHERE request_id=?2 AND phase=2",
                params![activation.activated_at().get(), request_id.as_bytes()],
            )?;
        }
        let readback = load_progress(&transaction, request_id)?
            .ok_or(OperationsError::Corrupt("replacement activation readback"))?;
        let receipt = replacement_activation_receipt(&readback)?;
        if receipt.membership_digest() != activation.membership_digest()
            || receipt.activated_at() != activation.activated_at()
        {
            return Err(OperationsError::Conflict("replacement activation replay"));
        }
        commit_replacement(transaction, self.destination, request_id)?;
        Ok(receipt)
    }

    /// Read one replacement registration by exact request identity.
    ///
    /// # Errors
    /// Returns storage or corruption failures.
    pub fn workspace_participation_replacement_progress(
        &self,
        request_id: Uuid,
    ) -> Result<Option<WorkspaceParticipationReplacementProgress>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let result = load_progress(&transaction, request_id);
        match result {
            Ok(value) => {
                crate::catalog::commit_read(transaction, self.destination)?;
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }

    /// Read the exact durable replacement membership commit receipt, if the
    /// replacement has reached `MembershipCommitted` or Ready.
    ///
    /// # Errors
    /// Returns storage or corruption failures.
    pub fn workspace_participation_replacement_commit(
        &self,
        request_id: Uuid,
    ) -> Result<Option<WorkspaceParticipationReplacementCommitReceipt>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let result = load_progress(&transaction, request_id)?.map(|progress| {
            if matches!(
                progress.phase(),
                registration::RegistrationPhase::MembershipCommitted
                    | registration::RegistrationPhase::Ready
            ) {
                replacement_commit_receipt(&progress).map(Some)
            } else {
                Ok(None)
            }
        });
        let value = result.transpose()?.flatten();
        crate::catalog::commit_read(transaction, self.destination)?;
        Ok(value)
    }

    /// Read the exact durable replacement activation receipt, if Ready.
    ///
    /// # Errors
    /// Returns storage or corruption failures.
    pub fn workspace_participation_replacement_activation(
        &self,
        request_id: Uuid,
    ) -> Result<Option<WorkspaceParticipationReplacementActivationReceipt>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let value = match load_progress(&transaction, request_id)? {
            Some(progress) if progress.phase() == registration::RegistrationPhase::Ready => {
                Some(replacement_activation_receipt(&progress)?)
            }
            Some(_) | None => None,
        };
        crate::catalog::commit_read(transaction, self.destination)?;
        Ok(value)
    }
}

fn validate_current_target(
    connection: &Connection,
    target: WorkspaceParticipationSessionExpectation,
    request: &registration::RegistrationRequest,
) -> Result<(), OperationsError> {
    let participation = crate::workspace_participation::load_participation(
        connection,
        target.workspace_id(),
        target.profile_id(),
    )?
    .ok_or(OperationsError::Conflict(
        "replacement participation missing",
    ))?;
    if participation.participation_id() != target.participation_id()
        || participation.revision() != target.participation_revision()
    {
        return Err(OperationsError::Conflict(
            "replacement participation revision",
        ));
    }
    let member = connection
        .query_row(
            "SELECT substr(session_id,1,17),principal_kind,substr(principal_id,1,17),substr(CAST(display_name AS BLOB),1,129),substr(CAST(declared_role AS BLOB),1,129),transport_kind,membership_revision FROM ops_logical_members WHERE workspace_id=?1 AND participant_id=?2 LIMIT 1",
            params![
                target.workspace_id().into_uuid().as_bytes(),
                target.participant_id().as_bytes(),
            ],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<Vec<u8>>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            },
        )
        .optional()?
        .ok_or(OperationsError::Conflict("replacement participant missing"))?;
    let session_id = AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(member.0)?));
    let principal = registration::decode_principal_fields(member.1, member.2.as_deref())?;
    let display_name = String::from_utf8(member.3)
        .map_err(|_| OperationsError::Corrupt("replacement member display"))?;
    let declared_role = String::from_utf8(member.4)
        .map_err(|_| OperationsError::Corrupt("replacement member role"))?;
    let transport = registration::decode_transport(member.5)?;
    let membership_revision = u64::try_from(member.6)
        .map_err(|_| OperationsError::Corrupt("replacement member revision"))?;
    if session_id != target.current_session_id()
        || principal != request.binding().principal()
        || display_name != request.display_name().as_str()
        || declared_role != request.declared_role().as_str()
        || transport != request.transport()
        || membership_revision != target.participant_membership_revision()
    {
        return Err(OperationsError::Conflict(
            "replacement participant snapshot",
        ));
    }
    Ok(())
}

fn ensure_rebind_persisted(
    connection: &Connection,
    progress: &WorkspaceParticipationReplacementProgress,
) -> Result<(), OperationsError> {
    let rebind = crate::session_replacement::load_rebind_by_session(
        connection,
        progress.workspace_id(),
        progress.request().binding().session_id(),
    )?
    .ok_or(OperationsError::Conflict(
        "replacement Session rebind missing",
    ))?;
    if rebind.participation_id() != progress.participation_id()
        || rebind.profile_id() != progress.profile_id()
        || rebind.profile_revision() != progress.profile_revision()
        || rebind.profile_mapping_request_id() != progress.profile_mapping_request_id()
        || rebind.previous_session_id() != progress.previous_session_id()
        || rebind.new_session_revision() != progress.new_session_revision()
        || rebind.participant_id() != progress.participant_id()
        || rebind.participant_membership_revision() != progress.participant_membership_revision()
        || rebind.expected_participation_revision() != progress.expected_participation_revision()
    {
        return Err(OperationsError::Conflict(
            "replacement Session rebind identity",
        ));
    }
    Ok(())
}

fn validate_prepare_inputs(
    target: WorkspaceParticipationSessionExpectation,
    profile_link: AgentProfileLogicalSessionLink,
    session: &RegisteredAgentSession,
    request: &registration::RegistrationRequest,
) -> Result<(), OperationsError> {
    let binding = request.binding();
    if session.state() != AgentSessionState::Open
        || target.current_session_id() == session.id()
        || target.profile_id() != profile_link.profile_id()
        || profile_link.session_id() != session.id()
        || profile_link.session_revision() != session.revision()
        || request.proposed_participant_id() != target.participant_id()
        || binding.session_id() != session.id()
        || binding.principal() != session.principal()
    {
        return Err(OperationsError::Conflict(
            "replacement registration identity",
        ));
    }
    Ok(())
}

fn validate_prepare_replay(
    progress: &WorkspaceParticipationReplacementProgress,
    migration_id: Uuid,
    target: WorkspaceParticipationSessionExpectation,
    profile_link: AgentProfileLogicalSessionLink,
    session: &RegisteredAgentSession,
    request: &registration::RegistrationRequest,
) -> Result<(), OperationsError> {
    if progress.migration_id() != migration_id
        || progress.workspace_id() != target.workspace_id()
        || progress.participation_id() != target.participation_id()
        || progress.profile_id() != target.profile_id()
        || progress.profile_revision() != profile_link.profile_revision()
        || progress.profile_mapping_request_id() != profile_link.request_id()
        || progress.previous_session_id() != target.current_session_id()
        || progress.new_session_revision() != session.revision()
        || progress.participant_id() != target.participant_id()
        || progress.participant_membership_revision() != target.participant_membership_revision()
        || progress.expected_participation_revision() != target.participation_revision()
        || progress.request() != request
    {
        return Err(OperationsError::Conflict("replacement registration replay"));
    }
    Ok(())
}

fn validate_reservation(
    progress: &WorkspaceParticipationReplacementProgress,
    import: &crate::ImportProgress,
    reservation: &BindingReservationObservation,
    destination: Uuid,
) -> Result<(), OperationsError> {
    if import.workspace_id() != progress.workspace_id()
        || import.destination_id() != destination
        || reservation.migration_id() != progress.migration_id()
        || reservation.destination_id() != destination
        || reservation.workspace_id() != progress.workspace_id()
        || reservation.freeze_revision() != import.source_revision()
        || reservation.request() != progress.request().binding()
    {
        return Err(OperationsError::Conflict(
            "replacement reservation identity",
        ));
    }
    if let Some(freeze) = progress.freeze_revision()
        && freeze != reservation.freeze_revision()
    {
        return Err(OperationsError::Conflict("replacement reservation replay"));
    }
    if let Some(reserved_at) = progress.reserved_at()
        && reserved_at != reservation.reserved_at()
    {
        return Err(OperationsError::Conflict("replacement reservation replay"));
    }
    Ok(())
}

fn validate_rebind_evidence(
    progress: &WorkspaceParticipationReplacementProgress,
    rebind: WorkspaceParticipationSessionRebindReceipt,
) -> Result<(), OperationsError> {
    if rebind.workspace_id() != progress.workspace_id()
        || rebind.participation_id() != progress.participation_id()
        || rebind.profile_id() != progress.profile_id()
        || rebind.profile_revision() != progress.profile_revision()
        || rebind.profile_mapping_request_id() != progress.profile_mapping_request_id()
        || rebind.previous_session_id() != progress.previous_session_id()
        || rebind.new_session_id() != progress.request().binding().session_id()
        || rebind.new_session_revision() != progress.new_session_revision()
        || rebind.participant_id() != progress.participant_id()
        || rebind.participant_membership_revision() != progress.participant_membership_revision()
        || rebind.expected_participation_revision() != progress.expected_participation_revision()
    {
        return Err(OperationsError::Conflict("replacement rebind evidence"));
    }
    Ok(())
}

fn validate_activation(
    progress: &WorkspaceParticipationReplacementProgress,
    import: &crate::ImportProgress,
    activation: &BindingActivationObservation,
    destination: Uuid,
) -> Result<(), OperationsError> {
    let reservation = activation.reservation();
    let digest = progress
        .commit_digest()
        .ok_or(OperationsError::Corrupt("replacement commit digest"))?;
    if import.workspace_id() != progress.workspace_id()
        || import.destination_id() != destination
        || reservation.migration_id() != progress.migration_id()
        || reservation.destination_id() != destination
        || reservation.workspace_id() != progress.workspace_id()
        || reservation.freeze_revision()
            != progress
                .freeze_revision()
                .ok_or(OperationsError::Corrupt("replacement freeze revision"))?
        || reservation.reserved_at()
            != progress.reserved_at().ok_or(OperationsError::Corrupt(
                "replacement reservation timestamp",
            ))?
        || reservation.request() != progress.request().binding()
        || activation.membership_digest() != digest
        || activation.display_name() != progress.request().display_name()
        || activation.declared_role() != progress.request().declared_role()
        || activation.transport() != progress.request().transport()
    {
        return Err(OperationsError::Conflict("replacement activation identity"));
    }
    Ok(())
}

fn ensure_existing_joined_event(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
    event_id: Uuid,
) -> Result<(), OperationsError> {
    let exists: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_logical_events WHERE workspace_id=?1 AND participant_id=?2 AND event_id=?3)",
        params![
            workspace_id.into_uuid().as_bytes(),
            participant_id.as_bytes(),
            event_id.as_bytes(),
        ],
        |row| row.get(0),
    )?;
    if !exists {
        return Err(OperationsError::Conflict("replacement JOINED identity"));
    }
    Ok(())
}

fn replacement_commit_digest(
    progress: &WorkspaceParticipationReplacementProgress,
    freeze_revision: u64,
) -> Result<[u8; 32], OperationsError> {
    let request_wire = progress.request().encode()?;
    let mut digest = Sha256::new();
    digest.update(REPLACEMENT_COMMIT_DIGEST_DOMAIN);
    digest.update(
        u32::try_from(request_wire.len())
            .map_err(|_| OperationsError::Bounds("replacement digest request"))?
            .to_be_bytes(),
    );
    digest.update(request_wire);
    digest.update(progress.participation_id().into_uuid().as_bytes());
    digest.update(progress.participant_id().as_bytes());
    digest.update(progress.previous_session_id().into_uuid().as_bytes());
    digest.update(
        progress
            .request()
            .binding()
            .session_id()
            .into_uuid()
            .as_bytes(),
    );
    digest.update(freeze_revision.to_be_bytes());
    Ok(digest.finalize().into())
}

fn replacement_commit_receipt(
    progress: &WorkspaceParticipationReplacementProgress,
) -> Result<WorkspaceParticipationReplacementCommitReceipt, OperationsError> {
    Ok(WorkspaceParticipationReplacementCommitReceipt {
        request_id: progress.request_id(),
        workspace_id: progress.workspace_id(),
        participant_id: progress.participant_id(),
        binding: progress.request().binding().clone(),
        commit_digest: progress
            .commit_digest()
            .ok_or(OperationsError::Corrupt("replacement commit digest"))?,
        joined_event_id: progress
            .joined_event_id()
            .ok_or(OperationsError::Corrupt("replacement JOINED identity"))?,
        committed_at: progress
            .committed_at()
            .ok_or(OperationsError::Corrupt("replacement commit timestamp"))?,
    })
}

fn replacement_activation_receipt(
    progress: &WorkspaceParticipationReplacementProgress,
) -> Result<WorkspaceParticipationReplacementActivationReceipt, OperationsError> {
    Ok(WorkspaceParticipationReplacementActivationReceipt {
        request_id: progress.request_id(),
        workspace_id: progress.workspace_id(),
        participant_id: progress.participant_id(),
        binding: progress.request().binding().clone(),
        membership_digest: progress
            .commit_digest()
            .ok_or(OperationsError::Corrupt("replacement commit digest"))?,
        activated_at: progress
            .activated_at()
            .ok_or(OperationsError::Corrupt("replacement activation timestamp"))?,
    })
}

fn load_progress(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<WorkspaceParticipationReplacementProgress>, OperationsError> {
    load_replacement(connection, request_id)?
        .map(decode_replacement)
        .transpose()
}

fn load_replacement(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<RawReplacement>, OperationsError> {
    connection
        .query_row(
            "SELECT substr(request_id,1,17),substr(migration_id,1,17),substr(workspace_id,1,17),substr(destination_id,1,17),substr(participation_id,1,17),substr(agent_profile_id,1,17),profile_revision,substr(profile_mapping_request_id,1,17),substr(previous_session_id,1,17),substr(new_session_id,1,17),new_session_revision,substr(participant_id,1,17),participant_membership_revision,expected_participation_revision,substr(request_wire,1,4097),phase,freeze_revision,reserved_at_ms,substr(commit_digest,1,33),committed_at_ms,substr(joined_event_id,1,17),activated_at_ms FROM ops_workspace_participation_replacement_registrations WHERE request_id=?1 LIMIT 1",
            params![request_id.as_bytes()],
            |row| {
                Ok(RawReplacement {
                    request_id: row.get(0)?,
                    migration_id: row.get(1)?,
                    workspace_id: row.get(2)?,
                    destination_id: row.get(3)?,
                    participation_id: row.get(4)?,
                    profile_id: row.get(5)?,
                    profile_revision: row.get(6)?,
                    profile_mapping_request_id: row.get(7)?,
                    previous_session_id: row.get(8)?,
                    new_session_id: row.get(9)?,
                    new_session_revision: row.get(10)?,
                    participant_id: row.get(11)?,
                    participant_membership_revision: row.get(12)?,
                    expected_participation_revision: row.get(13)?,
                    request_wire: row.get(14)?,
                    phase: row.get(15)?,
                    freeze_revision: row.get(16)?,
                    reserved_at_ms: row.get(17)?,
                    commit_digest: row.get(18)?,
                    committed_at_ms: row.get(19)?,
                    joined_event_id: row.get(20)?,
                    activated_at_ms: row.get(21)?,
                })
            },
        )
        .optional()
        .map_err(OperationsError::from)
}

fn decode_replacement(
    raw: RawReplacement,
) -> Result<WorkspaceParticipationReplacementProgress, OperationsError> {
    let destination_id = Uuid::from_bytes(wire::id(raw.destination_id)?);
    if destination_id.is_nil() {
        return Err(OperationsError::Corrupt("replacement destination identity"));
    }
    let profile_revision = raw.profile_revision;
    let new_session_revision = raw.new_session_revision;
    let membership_revision = u64::try_from(raw.participant_membership_revision)
        .map_err(|_| OperationsError::Corrupt("replacement membership revision"))?;
    let participation_revision = u64::try_from(raw.expected_participation_revision)
        .map_err(|_| OperationsError::Corrupt("replacement participation revision"))?;
    let freeze_revision = raw
        .freeze_revision
        .map(|value| {
            u64::try_from(value)
                .map_err(|_| OperationsError::Corrupt("replacement freeze revision"))
        })
        .transpose()?;
    let commit_digest = raw
        .commit_digest
        .map(|value| {
            let digest: [u8; 32] = value
                .try_into()
                .map_err(|_| OperationsError::Corrupt("replacement commit digest"))?;
            if digest == [0; 32] {
                return Err(OperationsError::Corrupt("replacement commit digest"));
            }
            Ok(digest)
        })
        .transpose()?;
    let request = registration::RegistrationRequest::decode(&raw.request_wire)?;
    let session_id = AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(raw.new_session_id)?));
    if request.binding().session_id() != session_id {
        return Err(OperationsError::Corrupt("replacement request Session"));
    }
    Ok(WorkspaceParticipationReplacementProgress {
        request_id: Uuid::from_bytes(wire::id(raw.request_id)?),
        migration_id: Uuid::from_bytes(wire::id(raw.migration_id)?),
        workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(raw.workspace_id)?)),
        participation_id: crate::WorkspaceParticipationId::from_uuid(Uuid::from_bytes(wire::id(
            raw.participation_id,
        )?))?,
        profile_id: AgentProfileId::from_uuid(Uuid::from_bytes(wire::id(raw.profile_id)?))
            .map_err(|_| OperationsError::Corrupt("replacement Profile identity"))?,
        profile_revision: AgentProfileRevision::new(profile_revision)
            .map_err(|_| OperationsError::Corrupt("replacement Profile revision"))?,
        profile_mapping_request_id: Uuid::from_bytes(wire::id(raw.profile_mapping_request_id)?),
        previous_session_id: AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(
            raw.previous_session_id,
        )?)),
        new_session_revision: AgentSessionRevision::new(new_session_revision)
            .map_err(|_| OperationsError::Corrupt("replacement Session revision"))?,
        participant_id: Uuid::from_bytes(wire::id(raw.participant_id)?),
        participant_membership_revision: membership_revision,
        expected_participation_revision: participation_revision,
        request,
        phase: decode_phase(raw.phase)?,
        freeze_revision,
        reserved_at: raw
            .reserved_at_ms
            .map(|value| {
                TimestampMillis::new(value)
                    .map_err(|_| OperationsError::Corrupt("replacement reservation timestamp"))
            })
            .transpose()?,
        commit_digest,
        committed_at: raw
            .committed_at_ms
            .map(|value| {
                TimestampMillis::new(value)
                    .map_err(|_| OperationsError::Corrupt("replacement commit timestamp"))
            })
            .transpose()?,
        joined_event_id: raw
            .joined_event_id
            .map(|value| wire::id(value).map(Uuid::from_bytes))
            .transpose()?,
        activated_at: raw
            .activated_at_ms
            .map(|value| {
                TimestampMillis::new(value)
                    .map_err(|_| OperationsError::Corrupt("replacement activation timestamp"))
            })
            .transpose()?,
    })
}

fn decode_phase(value: i64) -> Result<registration::RegistrationPhase, OperationsError> {
    match value {
        0 => Ok(registration::RegistrationPhase::Pending),
        1 => Ok(registration::RegistrationPhase::Reserved),
        2 => Ok(registration::RegistrationPhase::MembershipCommitted),
        3 => Ok(registration::RegistrationPhase::Ready),
        _ => Err(OperationsError::Corrupt("replacement registration phase")),
    }
}

fn commit_replacement(
    transaction: Transaction<'_>,
    destination_id: Uuid,
    request_id: Uuid,
) -> Result<(), OperationsError> {
    transaction.commit().map_err(|source| {
        OperationsError::ReplacementRegistrationCommitUnconfirmed {
            destination_id,
            request_id,
            source,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    fn table_sql(connection: &Connection, name: &str) -> Result<String, Box<dyn Error>> {
        Ok(connection.query_row(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?1",
            params![name],
            |row| row.get(0),
        )?)
    }

    #[test]
    fn v22_to_v23_adds_only_the_replacement_registration_journal() -> Result<(), Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!(
            "orchestration-replacement-v22-v23-{}",
            Uuid::now_v7()
        ));
        std::fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xEE01);
        let catalog = OperationsCatalog::open(&path, destination)?;
        drop(catalog);

        let connection = Connection::open(&path)?;
        let member_before = table_sql(&connection, "ops_logical_members")?;
        let leadership_before = table_sql(&connection, "ops_leadership")?;
        let credit_before = table_sql(&connection, "ops_work_credit_entries")?;
        let rebind_before = table_sql(&connection, "ops_workspace_participation_session_rebinds")?;
        crate::election_subject::drop_schema_for_legacy_fixture(&connection)?;
        connection.execute(
            "DROP TABLE ops_workspace_participation_replacement_registrations",
            [],
        )?;
        connection.pragma_update(None, "user_version", 22_i64)?;
        drop(connection);

        let migrated = OperationsCatalog::open(&path, destination)?;
        let version: i64 = migrated
            .connection
            .pragma_query_value(None, "user_version", |row| row.get(0))?;
        assert_eq!(version, crate::catalog::VERSION);
        assert_eq!(
            table_sql(&migrated.connection, "ops_logical_members")?,
            member_before
        );
        crate::catalog::assert_fixture_table_schema(
            &migrated.connection,
            "ops_leadership",
            &leadership_before,
        )?;
        assert_eq!(
            table_sql(&migrated.connection, "ops_work_credit_entries")?,
            credit_before
        );
        assert_eq!(
            table_sql(
                &migrated.connection,
                "ops_workspace_participation_session_rebinds"
            )?,
            rebind_before
        );
        assert_eq!(
            migrated.connection.query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name='ops_workspace_participation_replacement_registrations'",
                [],
                |row| row.get::<_, i64>(0),
            )?,
            1
        );

        drop(migrated);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
