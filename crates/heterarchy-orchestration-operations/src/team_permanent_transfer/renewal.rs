//! Re-propose an unstarted transfer after its source Leader changes.
//!
//! These generations are intent-only. Before any source dispatcher is added,
//! it must durably enter a non-replaceable preparation state in this owner.
//! No source outcome or absence of an external effect is inferred here.

use forge_domain::{TimestampMillis, WorkspaceId};
use rusqlite::{
    Connection, OptionalExtension, Transaction, TransactionBehavior, params, types::Value,
};
use uuid::Uuid;

use super::{
    ActorSnapshot, ResolvedIntentSource, TeamPermanentTransferIntentCommand,
    TeamPermanentTransferIntentReceipt, TransferSlotSource, actor_input_is_valid,
    authorize_current_orchestrator, build_intent_receipt, consent_reader_has_orchestrator_access,
    consent_receipt_id_exists, ensure_transfer_proposal_time_not_regressed, generation,
    insert_intent, intent_matches_command, load_intent_in_workspace, resolve_intent_source,
    validate_live_transfer_intent,
};
use crate::{OperationalActor, OperationsCatalog, OperationsError, teams};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeamPermanentTransferGenerationState {
    IntentOnly,
    Preparing,
    Superseded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamPermanentTransferGeneration {
    number: u64,
    predecessor_transfer_id: Option<Uuid>,
    state: TeamPermanentTransferGenerationState,
    recorded_at: TimestampMillis,
    head_transfer_id: Uuid,
    head_generation: u64,
}

impl TeamPermanentTransferGeneration {
    #[must_use]
    pub const fn number(&self) -> u64 {
        self.number
    }
    #[must_use]
    pub const fn predecessor_transfer_id(&self) -> Option<Uuid> {
        self.predecessor_transfer_id
    }
    #[must_use]
    pub const fn state(&self) -> TeamPermanentTransferGenerationState {
        self.state
    }
    #[must_use]
    pub const fn recorded_at(&self) -> TimestampMillis {
        self.recorded_at
    }
    #[must_use]
    pub const fn head_transfer_id(&self) -> Uuid {
        self.head_transfer_id
    }
    #[must_use]
    pub const fn head_generation(&self) -> u64 {
        self.head_generation
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamPermanentTransferGenerationSnapshot {
    intent: TeamPermanentTransferIntentReceipt,
    generation: TeamPermanentTransferGeneration,
}

impl TeamPermanentTransferGenerationSnapshot {
    #[must_use]
    pub const fn intent(&self) -> &TeamPermanentTransferIntentReceipt {
        &self.intent
    }
    #[must_use]
    pub const fn generation(&self) -> &TeamPermanentTransferGeneration {
        &self.generation
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamPermanentTransferRenewalCommand {
    predecessor_transfer_id: Uuid,
    expected_generation: u64,
    proposal: TeamPermanentTransferIntentCommand,
}

impl TeamPermanentTransferRenewalCommand {
    /// Pin the exact prior generation; the owner validates current authority.
    ///
    /// # Errors
    /// Rejects nil/self ancestry and non-representable generation revisions.
    pub fn new(
        predecessor_transfer_id: Uuid,
        expected_generation: u64,
        proposal: TeamPermanentTransferIntentCommand,
    ) -> Result<Self, OperationsError> {
        if predecessor_transfer_id.is_nil()
            || predecessor_transfer_id == proposal.transfer_id
            || expected_generation == 0
            || i64::try_from(expected_generation)
                .ok()
                .and_then(|number| number.checked_add(1))
                .is_none()
        {
            return Err(OperationsError::Bounds(
                "permanent transfer renewal identity",
            ));
        }
        Ok(Self {
            predecessor_transfer_id,
            expected_generation,
            proposal,
        })
    }
}

impl OperationsCatalog {
    /// Replace an intent-only generation after its source Leader advances.
    /// Approved capacity, cooldown, source membership and history are retained.
    ///
    /// # Errors
    /// Rejects stale heads, unchanged Leaders, changed source/slot/target,
    /// unauthorized actors, regressing clocks, or uncertain commits.
    pub fn renew_team_permanent_transfer(
        &mut self,
        command: &TeamPermanentTransferRenewalCommand,
        trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
    ) -> Result<TeamPermanentTransferGenerationSnapshot, OperationsError> {
        let proposal = &command.proposal;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        teams::ensure_verified_workspace(&tx, proposal.workspace_id)?;
        let actor = ActorSnapshot::resolve(&tx, proposal.workspace_id, proposal.actor)?;
        if let Some(existing) = load_matching_renewal(&tx, command, &actor, true)? {
            tx.commit()?;
            return Ok(existing);
        }
        if consent_receipt_id_exists(&tx, proposal.workspace_id, proposal.transfer_id)? {
            return Err(OperationsError::Conflict(
                "permanent transfer renewal identity",
            ));
        }
        authorize_current_orchestrator(&tx, proposal.workspace_id, &actor)?;
        let previous =
            load_intent_in_workspace(&tx, proposal.workspace_id, command.predecessor_transfer_id)?
                .ok_or(OperationsError::Conflict(
                    "permanent transfer predecessor missing",
                ))?;
        let prior_generation = checked_generation(&tx, &previous)?;
        if !prior_generation.active()
            || prior_generation.generation() != command.expected_generation
        {
            return Err(OperationsError::Conflict(
                "permanent transfer generation changed",
            ));
        }
        super::preparation::ensure_unprepared(&tx, proposal.workspace_id, previous.transfer_id)?;
        let source = resolve_intent_source(&tx, proposal, &actor)?;
        validate_renewal_source(&previous, proposal, &source)?;
        let recorded_at = trusted_now()?;
        ensure_transfer_proposal_time_not_regressed(
            &tx,
            proposal,
            source.target_profile_id,
            recorded_at,
        )?;
        validate_renewal_time(&tx, &previous, recorded_at)?;
        let intent = build_intent_receipt(proposal, actor.clone(), &source, recorded_at)?;
        insert_intent(&tx, &intent)?;
        let ledger = generation::replace_head(
            &tx,
            proposal.workspace_id,
            proposal.transfer_id,
            command.predecessor_transfer_id,
            command.expected_generation,
        )?;
        validate_live_transfer_intent(&tx, &intent)?;
        let result = snapshot(&tx, intent, ledger)?;
        match tx.commit() {
            Ok(()) => Ok(result),
            Err(source) => match recover_renewal_after_commit(&self.connection, command, &actor) {
                Ok(Some(existing)) => Ok(existing),
                Ok(None) | Err(_) => Err(OperationsError::OperationCommitUnconfirmed {
                    workspace_id: proposal.workspace_id,
                    request_id: proposal.transfer_id,
                    source,
                }),
            },
        }
    }

    /// Read exact generation history and its current slot head as the original
    /// proposer or current Orchestrator. This observation grants no execution.
    ///
    /// # Errors
    /// Rejects malformed identities, unauthorized Session binding and corrupt
    /// authorized history. Unrelated readers and absent records return None.
    pub fn read_team_permanent_transfer_generation(
        &self,
        workspace_id: WorkspaceId,
        transfer_id: Uuid,
        actor_input: OperationalActor,
    ) -> Result<Option<TeamPermanentTransferGenerationSnapshot>, OperationsError> {
        if workspace_id.into_uuid().is_nil()
            || transfer_id.is_nil()
            || !actor_input_is_valid(actor_input)
        {
            return Err(OperationsError::Bounds(
                "permanent transfer generation read identity",
            ));
        }
        let tx = self.connection.unchecked_transaction()?;
        teams::ensure_verified_workspace(&tx, workspace_id)?;
        let actor = ActorSnapshot::resolve(&tx, workspace_id, actor_input)?;
        if !authorized_generation_reader(&tx, workspace_id, transfer_id, &actor)? {
            return Ok(None);
        }
        let intent = load_intent_in_workspace(&tx, workspace_id, transfer_id)?.ok_or(
            OperationsError::Corrupt("permanent transfer generation intent missing"),
        )?;
        let ledger = checked_generation(&tx, &intent)?;
        let result = snapshot(&tx, intent, ledger)?;
        tx.commit()?;
        Ok(Some(result))
    }
}

fn authorized_generation_reader(
    connection: &Transaction<'_>,
    workspace: WorkspaceId,
    transfer: Uuid,
    actor: &ActorSnapshot,
) -> Result<bool, OperationsError> {
    let author: Option<(Value, Value)> = connection.query_row(
        "SELECT workspace_actor_participation_id,workspace_actor_profile_id FROM ops_team_permanent_transfer_intents WHERE workspace_id=?1 AND transfer_id=?2",
        params![workspace.into_uuid().as_bytes(), transfer.as_bytes()],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ).optional()?;
    let Some((participation, profile)) = author else {
        return Ok(false);
    };
    if matches!((participation, profile), (Value::Blob(p), Value::Blob(f))
        if p.as_slice() == actor.participation_id.into_uuid().as_bytes() && f.as_slice() == actor.profile_id.as_bytes())
    {
        return Ok(true);
    }
    consent_reader_has_orchestrator_access(authorize_current_orchestrator(
        connection, workspace, actor,
    ))
}

fn validate_renewal_source(
    previous: &TeamPermanentTransferIntentReceipt,
    proposal: &TeamPermanentTransferIntentCommand,
    source: &ResolvedIntentSource,
) -> Result<(), OperationsError> {
    if TransferSlotSource::from_intent(previous)
        != TransferSlotSource::from_command(proposal, source.target_profile_id)
    {
        return Err(OperationsError::Conflict(
            "permanent transfer renewal source changed",
        ));
    }
    if source.source_leadership_revision <= previous.source_leadership_revision {
        return Err(OperationsError::Conflict(
            "permanent transfer source Leader has not advanced",
        ));
    }
    Ok(())
}

fn validate_renewal_time(
    connection: &Connection,
    previous: &TeamPermanentTransferIntentReceipt,
    now: TimestampMillis,
) -> Result<(), OperationsError> {
    let latest_consent: Option<i64> = connection.query_row(
        "SELECT MAX(recorded_at_ms) FROM ops_team_permanent_transfer_consents WHERE workspace_id=?1 AND transfer_id=?2",
        params![previous.workspace_id.into_uuid().as_bytes(), previous.transfer_id.as_bytes()],
        |row| row.get(0),
    )?;
    let authority_time: Option<i64> = connection.query_row(
        "SELECT MAX(updated_at_ms) FROM ops_leadership WHERE workspace_id=?1 AND ((scope_kind=0 AND team_id IS NULL AND activity_id IS NULL) OR (scope_kind=2 AND team_id=?2 AND activity_id IS NULL))",
        params![previous.workspace_id.into_uuid().as_bytes(), previous.source_team_id.as_bytes()],
        |row| row.get(0),
    )?;
    let latest = previous
        .recorded_at
        .get()
        .max(latest_consent.unwrap_or(0))
        .max(authority_time.unwrap_or(0));
    if now.get() < latest {
        return Err(OperationsError::Conflict(
            "permanent transfer renewal time regression",
        ));
    }
    Ok(())
}

fn checked_generation(
    connection: &Connection,
    intent: &TeamPermanentTransferIntentReceipt,
) -> Result<generation::LedgerGeneration, OperationsError> {
    let record = generation::read_generation(connection, intent.workspace_id, intent.transfer_id)?
        .ok_or(OperationsError::Corrupt(
            "permanent transfer generation missing",
        ))?;
    validate_generation_identity(intent, &record)?;
    validate_predecessor_history(connection, intent, record.predecessor_transfer_id())?;
    Ok(record)
}

fn validate_predecessor_history(
    connection: &Connection,
    intent: &TeamPermanentTransferIntentReceipt,
    predecessor_id: Option<Uuid>,
) -> Result<(), OperationsError> {
    let Some(predecessor_id) = predecessor_id else {
        return Ok(());
    };
    let predecessor = load_intent_in_workspace(connection, intent.workspace_id, predecessor_id)?
        .ok_or(OperationsError::Corrupt(
            "permanent transfer predecessor history missing",
        ))?;
    let latest_consent: Option<i64> = connection.query_row(
        "SELECT MAX(recorded_at_ms) FROM ops_team_permanent_transfer_consents WHERE workspace_id=?1 AND transfer_id=?2",
        params![intent.workspace_id.into_uuid().as_bytes(), predecessor_id.as_bytes()],
        |row| row.get(0),
    )?;
    if TransferSlotSource::from_intent(intent) != TransferSlotSource::from_intent(&predecessor)
        || intent.source_leadership_revision <= predecessor.source_leadership_revision
        || intent.recorded_at.get()
            < predecessor
                .recorded_at
                .get()
                .max(latest_consent.unwrap_or(0))
    {
        return Err(OperationsError::Corrupt(
            "permanent transfer predecessor source history",
        ));
    }
    Ok(())
}

fn validate_generation_identity(
    intent: &TeamPermanentTransferIntentReceipt,
    record: &generation::LedgerGeneration,
) -> Result<(), OperationsError> {
    if record.workspace_id() != intent.workspace_id
        || record.transfer_id() != intent.transfer_id
        || record.decision_id() != intent.destination_decision_id
        || record.ordinal() != intent.destination_ordinal
        || record.recorded_at() != intent.recorded_at
    {
        return Err(OperationsError::Corrupt(
            "permanent transfer generation identity",
        ));
    }
    Ok(())
}

fn snapshot(
    connection: &Connection,
    intent: TeamPermanentTransferIntentReceipt,
    record: generation::LedgerGeneration,
) -> Result<TeamPermanentTransferGenerationSnapshot, OperationsError> {
    validate_generation_identity(&intent, &record)?;
    let state = if record.superseded() {
        TeamPermanentTransferGenerationState::Superseded
    } else if super::preparation::is_preparing(connection, intent.workspace_id, intent.transfer_id)?
    {
        TeamPermanentTransferGenerationState::Preparing
    } else {
        TeamPermanentTransferGenerationState::IntentOnly
    };
    Ok(TeamPermanentTransferGenerationSnapshot {
        intent,
        generation: TeamPermanentTransferGeneration {
            number: record.generation(),
            predecessor_transfer_id: record.predecessor_transfer_id(),
            state,
            recorded_at: record.recorded_at(),
            head_transfer_id: record.head_transfer_id(),
            head_generation: record.head_generation(),
        },
    })
}

pub(super) fn require_root_generation(
    connection: &Connection,
    intent: &TeamPermanentTransferIntentReceipt,
) -> Result<(), OperationsError> {
    let record = checked_generation(connection, intent)?;
    if record.predecessor_transfer_id().is_some() || record.generation() != 1 {
        return Err(OperationsError::Conflict(
            "permanent transfer root versus renewal",
        ));
    }
    Ok(())
}

pub(super) fn require_active_generation(
    connection: &Connection,
    intent: &TeamPermanentTransferIntentReceipt,
) -> Result<(), OperationsError> {
    if !checked_generation(connection, intent)?.active() {
        return Err(OperationsError::Conflict(
            "permanent transfer generation is superseded",
        ));
    }
    Ok(())
}

fn load_matching_renewal(
    connection: &Connection,
    command: &TeamPermanentTransferRenewalCommand,
    actor: &ActorSnapshot,
    replayed: bool,
) -> Result<Option<TeamPermanentTransferGenerationSnapshot>, OperationsError> {
    let proposal = &command.proposal;
    let Some(mut intent) =
        load_intent_in_workspace(connection, proposal.workspace_id, proposal.transfer_id)?
    else {
        return Ok(None);
    };
    if !intent_matches_command(&intent, proposal, actor) {
        return Err(OperationsError::Conflict(
            "permanent transfer renewal replay",
        ));
    }
    let record = checked_generation(connection, &intent)?;
    if record.predecessor_transfer_id() != Some(command.predecessor_transfer_id)
        || record.generation()
            != command
                .expected_generation
                .checked_add(1)
                .ok_or(OperationsError::Bounds("permanent transfer generation"))?
    {
        return Err(OperationsError::Conflict(
            "permanent transfer renewal replay",
        ));
    }
    intent.replayed = replayed;
    snapshot(connection, intent, record).map(Some)
}

fn recover_renewal_after_commit(
    connection: &Connection,
    command: &TeamPermanentTransferRenewalCommand,
    actor: &ActorSnapshot,
) -> Result<Option<TeamPermanentTransferGenerationSnapshot>, OperationsError> {
    let tx = connection.unchecked_transaction()?;
    let result = load_matching_renewal(&tx, command, actor, true)?;
    tx.commit()?;
    Ok(result)
}

pub(super) fn recover_root_after_commit(
    connection: &Connection,
    command: &TeamPermanentTransferIntentCommand,
    actor: &ActorSnapshot,
) -> Result<Option<TeamPermanentTransferIntentReceipt>, OperationsError> {
    let tx = connection.unchecked_transaction()?;
    let result = load_intent_in_workspace(&tx, command.workspace_id, command.transfer_id)?;
    if let Some(intent) = &result {
        require_root_generation(&tx, intent)?;
        if !intent_matches_command(intent, command, actor) {
            return Err(OperationsError::Conflict(
                "permanent transfer root recovery",
            ));
        }
    }
    tx.commit()?;
    Ok(result)
}
