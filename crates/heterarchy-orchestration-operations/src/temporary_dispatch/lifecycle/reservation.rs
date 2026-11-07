use forge_domain::{TimestampMillis, WorkspaceId};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use uuid::Uuid;

use crate::{
    CurrentLeadershipAuthority, LeadershipScope, OperationsCatalog, OperationsError, catalog,
    operational_actor::resolve_actor, teams,
};

use super::{
    TemporaryDispatchAdmissionBinding, TemporaryDispatchConsentKind,
    TemporaryDispatchReservationReceipt, TemporaryDispatchReservationState,
    TemporaryDispatchReserveCommand, same_actor,
};

use super::super::resolve_actor_provenance;
use super::consent::{
    TargetMembershipAuthority, current_consent, current_logical_actor, load_bound_request,
    participant_identity, target_membership_authority,
};

impl OperationsCatalog {
    /// Atomically revalidate the admission, three current consents, actor roles,
    /// home membership, target availability, and receiving-Team capacity before
    /// writing the inbound reservation receipt.
    ///
    /// Same-subject overlap across receiving Teams is durably recorded as
    /// `PendingConflict`; it consumes no Team seat and requires a future explicit
    /// product decision. A `Reserved` receipt holds the seat only.
    ///
    /// # Errors
    /// Rejects stale or missing authority, changed home membership, capacity
    /// pressure, invalid same-ID replay, corruption, clock errors, and SQLite
    /// failures.
    pub fn reserve_temporary_dispatch(
        &mut self,
        command: &TemporaryDispatchReserveCommand,
        trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
    ) -> Result<TemporaryDispatchReservationReceipt, OperationsError> {
        if command.expected_revision != 0 {
            return Err(OperationsError::Conflict(
                "temporary dispatch reservation revision",
            ));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        teams::ensure_verified_workspace(&transaction, command.binding.workspace_id)?;
        let resolved_actor =
            resolve_actor(&transaction, command.binding.workspace_id, command.actor)?;
        let actor = resolve_actor_provenance(command.actor, &resolved_actor)?;
        let admitted = load_bound_request(&transaction, &command.binding)?;

        if let Some(existing) = load_reservation_receipt(&transaction, command.binding.request_id)?
        {
            if !reservation_matches_command(&existing, command, &actor) {
                return Err(OperationsError::Conflict(
                    "temporary dispatch reservation replay",
                ));
            }
            transaction.commit()?;
            return Ok(with_reservation_replay(existing));
        }
        if load_reservation_state(&transaction, command.binding.request_id)?.is_some() {
            return Err(OperationsError::Corrupt(
                "temporary dispatch reservation without receipt",
            ));
        }
        if super::consent::lifecycle_receipt_id_exists(&transaction, command.binding.request_id)? {
            return Err(OperationsError::Conflict(
                "temporary dispatch lifecycle receipt identity",
            ));
        }

        let authorities = validate_reservation_authority(&transaction, command, &actor)?;

        crate::team_permanent_transfer::preparation::ensure_profile_open(
            &transaction,
            command.binding.workspace_id,
            authorities.target_membership.profile_id,
        )?;

        if crate::team_expansion::has_unconsumed_expansion_slot_for_target(
            &transaction,
            command.binding.workspace_id,
            command.binding.requesting_team_id,
            authorities.target_membership.participation_id,
            authorities.target_membership.profile_id,
        )? {
            return Err(OperationsError::Conflict(
                "temporary dispatch target already has a Team expansion slot",
            ));
        }

        let state = if has_cross_team_subject_conflict(
            &transaction,
            command.binding.workspace_id,
            authorities.target_membership.participation_id,
            command.binding.requesting_team_id,
        )? {
            TemporaryDispatchReservationState::PendingConflict
        } else {
            teams::ensure_team_member_capacity(
                &transaction,
                command.binding.workspace_id,
                command.binding.requesting_team_id,
            )?;
            TemporaryDispatchReservationState::Reserved
        };
        let recorded_at = trusted_now()?;
        let receipt = TemporaryDispatchReservationReceipt {
            binding: command.binding.clone(),
            state,
            revision: 1,
            target_participation_id: authorities.target_membership.participation_id,
            target_profile_id: authorities.target_membership.profile_id,
            target_membership_revision: authorities.target_membership.revision,
            requesting_leadership_revision: positive_revision(
                authorities.requesting_authority.leadership_revision(),
                "temporary dispatch requesting leadership revision",
            )?,
            home_leadership_revision: authorities.home_consent.authority_revision,
            workspace_leadership_revision: workspace_authority_revision(
                &authorities.workspace_authority,
            )?,
            target_consent_receipt_id: authorities.target_consent.receipt_id,
            home_leader_consent_receipt_id: authorities.home_consent.receipt_id,
            orchestrator_consent_receipt_id: authorities.orchestrator_consent.receipt_id,
            actor: actor.clone(),
            recorded_at,
            replayed: false,
        };
        insert_reservation_receipt(&transaction, &receipt)?;
        insert_reservation_state(&transaction, &receipt)?;
        let commit_result = transaction.commit();
        commit_reservation_write(
            commit_result,
            &self.connection,
            receipt,
            command,
            &admitted,
            &actor,
        )
    }

    /// Read an exact reservation as its original requesting Leader or the
    /// current authenticated Workspace Orchestrator.
    ///
    /// # Errors
    /// Rejects invalid identity, foreign admission binding, unauthorized actor,
    /// inconsistent state/receipt pairs, or corrupt data.
    pub fn read_temporary_dispatch_reservation(
        &mut self,
        binding: &TemporaryDispatchAdmissionBinding,
        actor: crate::OperationalActor,
    ) -> Result<Option<TemporaryDispatchReservationReceipt>, OperationsError> {
        if !super::actor_input_is_valid(actor) {
            return Err(OperationsError::Conflict(
                "temporary dispatch reservation readback identity",
            ));
        }
        let transaction = self.connection.unchecked_transaction()?;
        teams::ensure_verified_workspace(&transaction, binding.workspace_id)?;
        let resolved = resolve_actor(&transaction, binding.workspace_id, actor)?;
        let reader = resolve_actor_provenance(actor, &resolved)?;
        let admitted = load_bound_request(&transaction, binding)?;
        let is_requesting_actor = same_logical_actor(binding.actor(), &reader);
        let is_current_orchestrator = if is_requesting_actor {
            false
        } else {
            match current_authority(&transaction, binding.workspace_id, None)? {
                Some(authority) => {
                    let (participation, profile) = super::consent::participant_identity(
                        &transaction,
                        binding.workspace_id,
                        authority.participant_id(),
                    )?;
                    reader.participation_id().into_uuid() == participation
                        && reader.profile_id() == profile
                }
                None => false,
            }
        };
        if !is_current_orchestrator && !is_requesting_actor {
            return Err(OperationsError::Conflict(
                "temporary dispatch reservation readback actor",
            ));
        }
        let state = load_reservation_state(&transaction, binding.request_id)?;
        let receipt = load_reservation_receipt(&transaction, binding.request_id)?;
        let Some(receipt) = receipt else {
            if state.is_some() {
                return Err(OperationsError::Corrupt(
                    "temporary dispatch reservation state without receipt",
                ));
            }
            catalog::commit_read(transaction, self.destination)?;
            return Ok(None);
        };
        if state != Some((receipt.state, receipt.revision)) {
            return Err(OperationsError::Corrupt(
                "temporary dispatch reservation receipt and state differ",
            ));
        }
        if receipt.binding != *binding || receipt.request_id() != admitted.request_id() {
            return Err(OperationsError::Corrupt(
                "temporary dispatch reservation readback binding",
            ));
        }
        catalog::commit_read(transaction, self.destination)?;
        Ok(Some(receipt))
    }
}

struct ReservationAuthoritySnapshot {
    target_consent: super::TemporaryDispatchConsentReceipt,
    home_consent: super::TemporaryDispatchConsentReceipt,
    orchestrator_consent: super::TemporaryDispatchConsentReceipt,
    workspace_authority: CurrentLeadershipAuthority,
    requesting_authority: CurrentLeadershipAuthority,
    target_membership: TargetMembershipAuthority,
}

fn validate_reservation_authority(
    transaction: &Transaction<'_>,
    command: &TemporaryDispatchReserveCommand,
    actor: &super::TemporaryDispatchActorProvenance,
) -> Result<ReservationAuthoritySnapshot, OperationsError> {
    let binding = &command.binding;
    let target_consent = current_consent(
        transaction,
        binding,
        TemporaryDispatchConsentKind::TargetAcceptance,
    )?;
    let home_consent = current_consent(
        transaction,
        binding,
        TemporaryDispatchConsentKind::HomeLeaderConsent,
    )?;
    let orchestrator_consent = current_consent(
        transaction,
        binding,
        TemporaryDispatchConsentKind::WorkspaceOrchestratorApproval,
    )?;
    let workspace_authority = current_authority(transaction, binding.workspace_id, None)?.ok_or(
        OperationsError::Conflict("temporary dispatch Workspace Orchestrator missing"),
    )?;
    let (orchestrator_participation, orchestrator_profile) = participant_identity(
        transaction,
        binding.workspace_id,
        workspace_authority.participant_id(),
    )?;
    if actor.participation_id().into_uuid() != orchestrator_participation
        || actor.profile_id() != orchestrator_profile
        || !same_logical_actor(actor, &orchestrator_consent.actor)
        || positive_revision(
            workspace_authority.leadership_revision(),
            "temporary dispatch Workspace leadership revision",
        )? != orchestrator_consent.authority_revision
    {
        return Err(OperationsError::Conflict(
            "temporary dispatch reservation actor authority",
        ));
    }
    let requesting_authority = validate_requesting_leader(transaction, binding)?;
    let target_membership = validate_target_home_membership(transaction, binding, &target_consent)?;
    Ok(ReservationAuthoritySnapshot {
        target_consent,
        home_consent,
        orchestrator_consent,
        workspace_authority,
        requesting_authority,
        target_membership,
    })
}

fn validate_requesting_leader(
    transaction: &Transaction<'_>,
    binding: &TemporaryDispatchAdmissionBinding,
) -> Result<CurrentLeadershipAuthority, OperationsError> {
    let requester = current_logical_actor(
        transaction,
        binding.workspace_id,
        binding.actor().profile_id(),
        binding.actor().participation_id().into_uuid(),
    )?;
    if !same_logical_actor(binding.actor(), &requester) {
        return Err(OperationsError::Conflict(
            "temporary dispatch requesting Leader logical actor changed",
        ));
    }
    let authority = current_authority(
        transaction,
        binding.workspace_id,
        Some(binding.requesting_team_id),
    )?
    .ok_or(OperationsError::Conflict(
        "temporary dispatch requesting Team has no current Leader",
    ))?;
    let (participation, profile) = participant_identity(
        transaction,
        binding.workspace_id,
        authority.participant_id(),
    )?;
    if participation != binding.actor().participation_id().into_uuid()
        || profile != binding.actor().profile_id()
        || positive_revision(
            authority.leadership_revision(),
            "temporary dispatch requesting leadership revision",
        )? != binding.leadership_revision
    {
        return Err(OperationsError::Conflict(
            "temporary dispatch requesting Team authority changed",
        ));
    }
    teams::ensure_team_formed(
        transaction,
        binding.workspace_id,
        binding.requesting_team_id,
    )?;
    Ok(authority)
}

fn validate_target_home_membership(
    transaction: &Transaction<'_>,
    binding: &TemporaryDispatchAdmissionBinding,
    target_consent: &super::TemporaryDispatchConsentReceipt,
) -> Result<TargetMembershipAuthority, OperationsError> {
    let target_home = super::super::resolve_target_home_team(
        transaction,
        binding.workspace_id,
        binding.requesting_team_id,
        binding.target_participant_id,
    )?;
    if target_home != binding.target_home_team_id {
        return Err(OperationsError::Conflict(
            "temporary dispatch target home Team changed",
        ));
    }
    let membership = target_membership_authority(transaction, binding)?;
    if membership.revision != target_consent.authority_revision
        || membership.participation_id != target_consent.actor.participation_id().into_uuid()
        || membership.profile_id != target_consent.actor.profile_id()
    {
        return Err(OperationsError::Conflict(
            "temporary dispatch target membership changed",
        ));
    }
    Ok(membership)
}

fn current_authority(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    team_id: Option<Uuid>,
) -> Result<Option<CurrentLeadershipAuthority>, OperationsError> {
    let scope = team_id.map_or_else(
        || LeadershipScope::workspace(workspace_id),
        |team| LeadershipScope::team(workspace_id, team),
    );
    crate::leadership::current_leadership_authority_in_transaction(transaction, scope)
}

fn has_cross_team_subject_conflict(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    target_participation_id: Uuid,
    receiving_team_id: Uuid,
) -> Result<bool, OperationsError> {
    transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_temporary_dispatch_reservations WHERE workspace_id=?1 AND target_participation_id=?2 AND receiving_team_id<>?3 AND state IN('PENDING_CONFLICT','RESERVED','SOURCE_READY','ACTIVE','DRAINING'))",
            params![
                workspace_id.into_uuid().as_bytes(),
                target_participation_id.as_bytes(),
                receiving_team_id.as_bytes(),
            ],
            |row| row.get(0),
        )
        .map_err(OperationsError::from)
}

fn load_reservation_state(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<(TemporaryDispatchReservationState, u64)>, OperationsError> {
    connection
        .query_row(
            "SELECT state,revision FROM ops_temporary_dispatch_reservations WHERE request_id=?1",
            params![request_id.as_bytes()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?
        .map(|(state, revision)| {
            Ok((
                TemporaryDispatchReservationState::decode(&state)?,
                positive_revision(revision, "temporary dispatch reservation revision")?,
            ))
        })
        .transpose()
}

struct RawReservationReceipt {
    receipt_id: Vec<u8>,
    revision: i64,
    state: String,
    intent_digest: Vec<u8>,
    target_participation: Vec<u8>,
    target_profile: Vec<u8>,
    target_membership_revision: i64,
    requesting_leadership_revision: i64,
    home_leadership_revision: i64,
    workspace_leadership_revision: i64,
    target_consent: Vec<u8>,
    home_consent: Vec<u8>,
    orchestrator_consent: Vec<u8>,
    activity: Vec<u8>,
    agent: Vec<u8>,
    session: Vec<u8>,
    participation: Vec<u8>,
    profile: Vec<u8>,
    participant: Vec<u8>,
    display_name: String,
    recorded_at: i64,
}

fn load_raw_reservation_receipt(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<RawReservationReceipt>, OperationsError> {
    connection
        .query_row(
            "SELECT receipt_id,revision,state,intent_digest,target_participation_id,target_profile_id,target_membership_revision,requesting_leadership_revision,home_leadership_revision,workspace_leadership_revision,target_consent_receipt_id,home_leader_consent_receipt_id,orchestrator_consent_receipt_id,actor_activity_id,actor_agent_id,actor_session_id,actor_participation_id,actor_profile_id,actor_participant_id,actor_display_name,recorded_at_ms FROM ops_temporary_dispatch_reservation_receipts WHERE request_id=?1",
            params![request_id.as_bytes()],
            |row| {
                Ok(RawReservationReceipt {
                    receipt_id: row.get(0)?,
                    revision: row.get(1)?,
                    state: row.get(2)?,
                    intent_digest: row.get(3)?,
                    target_participation: row.get(4)?,
                    target_profile: row.get(5)?,
                    target_membership_revision: row.get(6)?,
                    requesting_leadership_revision: row.get(7)?,
                    home_leadership_revision: row.get(8)?,
                    workspace_leadership_revision: row.get(9)?,
                    target_consent: row.get(10)?,
                    home_consent: row.get(11)?,
                    orchestrator_consent: row.get(12)?,
                    activity: row.get(13)?,
                    agent: row.get(14)?,
                    session: row.get(15)?,
                    participation: row.get(16)?,
                    profile: row.get(17)?,
                    participant: row.get(18)?,
                    display_name: row.get(19)?,
                    recorded_at: row.get(20)?,
                })
            },
        )
        .optional()
        .map_err(OperationsError::from)
}

fn load_reservation_receipt(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<TemporaryDispatchReservationReceipt>, OperationsError> {
    let Some(raw) = load_raw_reservation_receipt(connection, request_id)? else {
        return Ok(None);
    };
    let request = super::super::load_request(connection, request_id)?.ok_or(
        OperationsError::Corrupt("temporary dispatch reservation admission receipt"),
    )?;
    let binding = TemporaryDispatchAdmissionBinding::from_record(&request);
    let receipt_id = uuid_from_bytes(&raw.receipt_id, "temporary dispatch reservation receipt")?;
    if receipt_id != request_id || raw.intent_digest.as_slice() != binding.intent_digest.as_slice()
    {
        return Err(OperationsError::Corrupt(
            "temporary dispatch reservation identity or digest",
        ));
    }
    let receipt = TemporaryDispatchReservationReceipt {
        binding,
        state: TemporaryDispatchReservationState::decode(&raw.state)?,
        revision: positive_revision(raw.revision, "temporary dispatch reservation revision")?,
        target_participation_id: uuid_from_bytes(
            &raw.target_participation,
            "temporary dispatch reservation target participation",
        )?,
        target_profile_id: uuid_from_bytes(
            &raw.target_profile,
            "temporary dispatch reservation target Profile",
        )?,
        target_membership_revision: positive_revision(
            raw.target_membership_revision,
            "temporary dispatch target membership revision",
        )?,
        requesting_leadership_revision: positive_revision(
            raw.requesting_leadership_revision,
            "temporary dispatch requesting leadership revision",
        )?,
        home_leadership_revision: positive_revision(
            raw.home_leadership_revision,
            "temporary dispatch home leadership revision",
        )?,
        workspace_leadership_revision: positive_revision(
            raw.workspace_leadership_revision,
            "temporary dispatch Workspace leadership revision",
        )?,
        target_consent_receipt_id: uuid_from_bytes(
            &raw.target_consent,
            "temporary dispatch target consent receipt",
        )?,
        home_leader_consent_receipt_id: uuid_from_bytes(
            &raw.home_consent,
            "temporary dispatch home Leader consent receipt",
        )?,
        orchestrator_consent_receipt_id: uuid_from_bytes(
            &raw.orchestrator_consent,
            "temporary dispatch Orchestrator consent receipt",
        )?,
        actor: super::consent::decode_actor(
            &raw.activity,
            &raw.agent,
            &raw.session,
            &raw.participation,
            &raw.profile,
            &raw.participant,
            raw.display_name,
        )?,
        recorded_at: decode_timestamp(raw.recorded_at, "temporary dispatch reservation time")?,
        replayed: false,
    };
    validate_linked_consents(connection, &receipt)?;
    let Some((state, current_revision)) = load_reservation_state(connection, request_id)? else {
        return Err(OperationsError::Corrupt(
            "temporary dispatch reservation current state missing",
        ));
    };
    if state != receipt.state || current_revision != receipt.revision {
        return Err(OperationsError::Corrupt(
            "temporary dispatch reservation receipt and state differ",
        ));
    }
    validate_reservation_state(connection, &receipt)?;
    Ok(Some(receipt))
}

fn validate_linked_consents(
    connection: &Connection,
    receipt: &TemporaryDispatchReservationReceipt,
) -> Result<(), OperationsError> {
    let target =
        super::consent::load_consent_receipt(connection, receipt.target_consent_receipt_id)?
            .ok_or(OperationsError::Corrupt(
                "temporary dispatch reservation target consent",
            ))?;
    let home =
        super::consent::load_consent_receipt(connection, receipt.home_leader_consent_receipt_id)?
            .ok_or(OperationsError::Corrupt(
            "temporary dispatch reservation home consent",
        ))?;
    let orchestrator =
        super::consent::load_consent_receipt(connection, receipt.orchestrator_consent_receipt_id)?
            .ok_or(OperationsError::Corrupt(
                "temporary dispatch reservation Orchestrator consent",
            ))?;
    if target.request_id() != receipt.request_id()
        || target.kind() != TemporaryDispatchConsentKind::TargetAcceptance
        || target.intent_digest() != receipt.binding.intent_digest
        || target.authority_revision != receipt.target_membership_revision
        || target.actor.participation_id().into_uuid() != receipt.target_participation_id
        || target.actor.profile_id() != receipt.target_profile_id
        || home.request_id() != receipt.request_id()
        || home.kind() != TemporaryDispatchConsentKind::HomeLeaderConsent
        || home.intent_digest() != receipt.binding.intent_digest
        || home.authority_revision != receipt.home_leadership_revision
        || orchestrator.request_id() != receipt.request_id()
        || orchestrator.kind() != TemporaryDispatchConsentKind::WorkspaceOrchestratorApproval
        || orchestrator.intent_digest() != receipt.binding.intent_digest
        || orchestrator.authority_revision != receipt.workspace_leadership_revision
        || !same_logical_actor(&orchestrator.actor, &receipt.actor)
    {
        return Err(OperationsError::Corrupt(
            "temporary dispatch reservation consent bindings",
        ));
    }
    Ok(())
}

fn validate_reservation_state(
    connection: &Connection,
    receipt: &TemporaryDispatchReservationReceipt,
) -> Result<(), OperationsError> {
    let raw = connection.query_row(
        "SELECT workspace_id,receiving_team_id,home_team_id,target_participant_id,target_participation_id,target_profile_id,requested_scope,requested_end_at_ms,intent_digest,recorded_at_ms FROM ops_temporary_dispatch_reservations WHERE request_id=?1",
        params![receipt.binding.request_id.as_bytes()],
        |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, Vec<u8>>(3)?,
                row.get::<_, Vec<u8>>(4)?,
                row.get::<_, Vec<u8>>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, Vec<u8>>(8)?,
                row.get::<_, i64>(9)?,
            ))
        },
    )?;
    if raw.0.as_slice() != receipt.binding.workspace_id.into_uuid().as_bytes()
        || raw.1.as_slice() != receipt.binding.requesting_team_id.as_bytes()
        || raw.2.as_slice() != receipt.binding.target_home_team_id.as_bytes()
        || raw.3.as_slice() != receipt.binding.target_participant_id.as_bytes()
        || raw.4.as_slice() != receipt.target_participation_id.as_bytes()
        || raw.5.as_slice() != receipt.target_profile_id.as_bytes()
        || raw.6 != receipt.binding.requested_scope.as_str()
        || raw.7 != receipt.binding.requested_end_at.get()
        || raw.8.as_slice() != receipt.binding.intent_digest.as_slice()
        || raw.9 != receipt.recorded_at.get()
    {
        return Err(OperationsError::Corrupt(
            "temporary dispatch reservation current payload",
        ));
    }
    Ok(())
}

fn insert_reservation_receipt(
    transaction: &Transaction<'_>,
    receipt: &TemporaryDispatchReservationReceipt,
) -> Result<(), OperationsError> {
    let actor = &receipt.actor;
    let request_id = receipt.binding.request_id;
    transaction.execute(
        "INSERT INTO ops_temporary_dispatch_reservation_receipts(receipt_id,request_id,revision,state,intent_digest,target_participation_id,target_profile_id,target_membership_revision,requesting_leadership_revision,home_leadership_revision,workspace_leadership_revision,target_consent_receipt_id,home_leader_consent_receipt_id,orchestrator_consent_receipt_id,actor_activity_id,actor_agent_id,actor_session_id,actor_participation_id,actor_profile_id,actor_participant_id,actor_display_name,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22)",
        params![
            request_id.as_bytes(),
            request_id.as_bytes(),
            to_i64(receipt.revision, "temporary dispatch reservation revision")?,
            receipt.state.as_str(),
            receipt.binding.intent_digest.as_slice(),
            receipt.target_participation_id.as_bytes(),
            receipt.target_profile_id.as_bytes(),
            to_i64(receipt.target_membership_revision, "temporary dispatch target membership revision")?,
            to_i64(receipt.requesting_leadership_revision, "temporary dispatch requesting leadership revision")?,
            to_i64(receipt.home_leadership_revision, "temporary dispatch home leadership revision")?,
            to_i64(receipt.workspace_leadership_revision, "temporary dispatch Workspace leadership revision")?,
            receipt.target_consent_receipt_id.as_bytes(),
            receipt.home_leader_consent_receipt_id.as_bytes(),
            receipt.orchestrator_consent_receipt_id.as_bytes(),
            actor.activity_id().into_uuid().as_bytes(),
            actor.agent_id().into_uuid().as_bytes(),
            actor.session_id().into_uuid().as_bytes(),
            actor.participation_id().into_uuid().as_bytes(),
            actor.profile_id().as_bytes(),
            actor.participant_id().as_bytes(),
            actor.display_name(),
            receipt.recorded_at.get(),
        ],
    )?;
    Ok(())
}

fn insert_reservation_state(
    transaction: &Transaction<'_>,
    receipt: &TemporaryDispatchReservationReceipt,
) -> Result<(), OperationsError> {
    let binding = &receipt.binding;
    transaction.execute(
        "INSERT INTO ops_temporary_dispatch_reservations(request_id,workspace_id,receiving_team_id,home_team_id,target_participant_id,target_participation_id,target_profile_id,requested_scope,requested_end_at_ms,intent_digest,state,revision,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
        params![
            binding.request_id.as_bytes(),
            binding.workspace_id.into_uuid().as_bytes(),
            binding.requesting_team_id.as_bytes(),
            binding.target_home_team_id.as_bytes(),
            binding.target_participant_id.as_bytes(),
            receipt.target_participation_id.as_bytes(),
            receipt.target_profile_id.as_bytes(),
            binding.requested_scope.as_str(),
            binding.requested_end_at.get(),
            binding.intent_digest.as_slice(),
            receipt.state.as_str(),
            to_i64(receipt.revision, "temporary dispatch reservation revision")?,
            receipt.recorded_at.get(),
        ],
    )?;
    Ok(())
}

fn commit_reservation_write(
    commit_result: Result<(), rusqlite::Error>,
    connection: &Connection,
    receipt: TemporaryDispatchReservationReceipt,
    command: &TemporaryDispatchReserveCommand,
    admitted: &super::TemporaryDispatchRequestRecord,
    actor: &super::TemporaryDispatchActorProvenance,
) -> Result<TemporaryDispatchReservationReceipt, OperationsError> {
    match commit_result {
        Ok(()) => Ok(receipt),
        Err(source) => match load_reservation_receipt(connection, command.binding.request_id) {
            Ok(Some(existing)) if reservation_matches_command(&existing, command, actor) => {
                Ok(with_reservation_replay(existing))
            }
            Ok(Some(_)) => Err(OperationsError::Conflict(
                "temporary dispatch reservation replay after uncertain commit",
            )),
            Ok(None) | Err(_) => Err(OperationsError::OperationCommitUnconfirmed {
                workspace_id: admitted.workspace_id(),
                request_id: command.binding.request_id,
                source,
            }),
        },
    }
}

fn reservation_matches_command(
    receipt: &TemporaryDispatchReservationReceipt,
    command: &TemporaryDispatchReserveCommand,
    actor: &super::TemporaryDispatchActorProvenance,
) -> bool {
    receipt.binding == command.binding
        && receipt.revision == 1
        && command.expected_revision == 0
        && same_actor(&receipt.actor, actor)
}

fn with_reservation_replay(
    mut receipt: TemporaryDispatchReservationReceipt,
) -> TemporaryDispatchReservationReceipt {
    receipt.replayed = true;
    receipt
}

fn positive_revision(value: i64, label: &'static str) -> Result<u64, OperationsError> {
    u64::try_from(value)
        .ok()
        .filter(|revision| *revision > 0)
        .ok_or(OperationsError::Corrupt(label))
}

fn to_i64(value: u64, label: &'static str) -> Result<i64, OperationsError> {
    i64::try_from(value).map_err(|_| OperationsError::Bounds(label))
}

fn decode_timestamp(value: i64, label: &'static str) -> Result<TimestampMillis, OperationsError> {
    TimestampMillis::new(value).map_err(|_| OperationsError::Corrupt(label))
}

fn uuid_from_bytes(value: &[u8], label: &'static str) -> Result<Uuid, OperationsError> {
    Uuid::from_slice(value).map_err(|_| OperationsError::Corrupt(label))
}

fn same_logical_actor(
    left: &super::TemporaryDispatchActorProvenance,
    right: &super::TemporaryDispatchActorProvenance,
) -> bool {
    left.participation_id() == right.participation_id() && left.profile_id() == right.profile_id()
}

fn workspace_authority_revision(
    authority: &CurrentLeadershipAuthority,
) -> Result<u64, OperationsError> {
    positive_revision(
        authority.leadership_revision(),
        "temporary dispatch Workspace leadership revision",
    )
}
