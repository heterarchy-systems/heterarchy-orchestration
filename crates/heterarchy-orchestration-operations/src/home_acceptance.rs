//! Native Operations owner for Source-confirmed Home acceptance.
//!
//! Cross-catalog records passed to this owner are observations. The trusted
//! Runtime must fetch Source, Session, and Profile records from their configured
//! owners before calling these APIs; DTO construction does not authenticate an
//! owner read.

mod model;
mod store;

pub mod schema;

#[cfg(test)]
pub(crate) mod tests;

use forge_domain::{
    AgentAssignmentHomeHandoffOrigin, AgentAssignmentId, AgentAssignmentOrigin,
    AgentAssignmentState, AgentSessionId, AgentSessionRevision,
    HomeHandoffAffinityAdmissionReferenceDigest, HomeHandoffHomeTeamAuthority,
    HomeHandoffNodeBindingDigest, HomeHandoffTakeRequest, HomeHandoffWorkAuthority,
    RegisteredAgentAssignment, TimestampMillis, WorkspaceId,
};
use forge_persistence::{
    HomeHandoffAcceptanceOutcomeKind, HomeHandoffAcceptanceOutcomeRecord,
    HomeHandoffAssignmentLineage, HomeHandoffProposalRecord,
};
use heterarchy_orchestration_sessions::{
    AgentProfileId, AgentProfileRevision, HomeAcceptanceProfilePinRecord,
    HomeAcceptanceProfileState, HomeAcceptanceSessionRecord, HomeAcceptanceSessionState,
};
use rusqlite::{Connection, Transaction, TransactionBehavior, params};
use uuid::Uuid;

use crate::{
    NodeTeamBindingScope, OperationalActor, OperationsCatalog, OperationsError,
    WorkspaceParticipationId, presence, team_assignment, workspace_participation,
};

pub use model::*;

/// Reject a Node binding mutation while a Home acceptance or its successor
/// still reserves this exact Activity/Goal/DAG/Node scope.
pub(crate) fn ensure_node_admission_open(
    connection: &Connection,
    scope: &NodeTeamBindingScope,
) -> Result<(), OperationsError> {
    let held: bool = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM ops_home_acceptance_work_reservations
            WHERE workspace_id=?1 AND activity_id=?2 AND goal_id=?3
              AND goal_created_at_ms=?4 AND dag_revision=?5
              AND dag_base_goal_revision=?6 AND node_id=?7 AND state IN(0,1)
        )",
        params![
            scope.workspace_id().into_uuid().as_bytes(),
            scope.activity_id().into_uuid().as_bytes(),
            scope.goal_id().into_uuid().as_bytes(),
            scope.goal_created_at().get(),
            scope.dag_revision().get(),
            scope.dag_base_goal_revision().get(),
            scope.node_id().into_uuid().as_bytes(),
        ],
        |row| row.get(0),
    )?;
    if held {
        return Err(OperationsError::Conflict(
            "Node has a pending or active Home acceptance work reservation",
        ));
    }
    Ok(())
}

/// Reject mutation of a recipient Profile while its acceptance qualification
/// is unresolved. Applied work reservations do not extend this temporary hold.
pub(crate) fn ensure_profile_mutation_open(
    connection: &Connection,
    workspace_id: WorkspaceId,
    profile_id: Uuid,
) -> Result<(), OperationsError> {
    let held: bool = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM ops_home_acceptance_admissions a
            LEFT JOIN ops_home_acceptance_resolutions r USING(acceptance_id)
            WHERE a.workspace_id=?1 AND a.recipient_profile_id=?2
              AND r.acceptance_id IS NULL
        )",
        params![workspace_id.into_uuid().as_bytes(), profile_id.as_bytes()],
        |row| row.get(0),
    )?;
    if held {
        return Err(OperationsError::Conflict(
            "Profile has an unresolved Home acceptance",
        ));
    }
    Ok(())
}

/// Reject logical Session rebind while its Home acceptance qualification is
/// unresolved. Source terminal readback releases this gate through resolution.
pub(crate) fn ensure_session_rebind_open(
    connection: &Connection,
    workspace_id: WorkspaceId,
    session_id: AgentSessionId,
) -> Result<(), OperationsError> {
    let held: bool = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM ops_home_acceptance_admissions a
            LEFT JOIN ops_home_acceptance_resolutions r USING(acceptance_id)
            WHERE a.workspace_id=?1 AND a.recipient_session_id=?2
              AND r.acceptance_id IS NULL
        )",
        params![
            workspace_id.into_uuid().as_bytes(),
            session_id.into_uuid().as_bytes(),
        ],
        |row| row.get(0),
    )?;
    if held {
        return Err(OperationsError::Conflict(
            "Session has an unresolved Home acceptance",
        ));
    }
    Ok(())
}

/// Reject Team retirement or membership mutation while a Team-bound Home
/// acceptance is awaiting an exact Source terminal.
pub(crate) fn ensure_team_mutation_open(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<(), OperationsError> {
    let held: bool = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM ops_home_acceptance_admissions a
            LEFT JOIN ops_home_acceptance_resolutions r USING(acceptance_id)
            WHERE a.workspace_id=?1 AND a.home_team_id=?2
              AND r.acceptance_id IS NULL
        )",
        params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()],
        |row| row.get(0),
    )?;
    if held {
        return Err(OperationsError::Conflict(
            "Team has an unresolved Home acceptance",
        ));
    }
    Ok(())
}

impl OperationsCatalog {
    /// Stage one exact Source take request and proposal before acquiring owner
    /// holds. `source_proposal` and an optional Home predecessor terminal must
    /// be fetched through the Runtime's configured authenticated Source owner.
    /// A Home-origin predecessor requires the exact prior Applied receipt and
    /// its still-Active Operations work reservation; root identity alone is not
    /// enough to admit a chained handoff.
    ///
    /// # Errors
    /// Rejects mismatched configured scope, absent Prepared Source evidence,
    /// stale recipient qualification, missing predecessor proof, replay
    /// substitution, and uncertain commit.
    pub fn stage_home_acceptance<C>(
        &mut self,
        identity: HomeAcceptanceSourceIdentity,
        request: HomeHandoffTakeRequest,
        source_proposal: HomeHandoffProposalRecord,
        predecessor_source: Option<&HomeHandoffAcceptanceOutcomeRecord>,
        sample_clock: C,
    ) -> Result<HomeAcceptanceIntentReceipt, OperationsError>
    where
        C: FnOnce() -> Result<TimestampMillis, OperationsError>,
    {
        ensure_configured_identity(identity, self.destination_id(), request)?;
        let proposal_snapshot = source_proposal
            .to_snapshot_bytes()
            .map_err(|_| OperationsError::Conflict("Home Source proposal snapshot"))?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;

        if let Some(existing) = store::load_intent(&transaction, request.acceptance_id())? {
            if existing.identity != identity
                || existing.request != request
                || existing.proposal_snapshot.as_deref() != Some(proposal_snapshot.as_slice())
                || existing.proposal_digest != Some(*source_proposal.payload_digest())
            {
                return Err(OperationsError::Conflict("Home acceptance stage replay"));
            }
            let prior = prior_source_pin(predecessor_source);
            if prior
                != existing
                    .predecessor_acceptance_id
                    .zip(existing.predecessor_source_digest)
            {
                return Err(OperationsError::Conflict(
                    "Home predecessor terminal replay changed",
                ));
            }
            if existing.proposal.is_none()
                || store::load_resolution(&transaction, &existing, false)?.is_some()
            {
                return Err(OperationsError::Conflict(
                    "Home acceptance already has an absorbing Source outcome",
                ));
            }
            transaction.commit()?;
            return Ok(existing.receipt(true));
        }

        validate_proposal_request(request, &source_proposal)?;
        let prepared =
            load_and_validate_preparation(&transaction, identity, request, &source_proposal)?;
        let recipient = resolve_recipient(&transaction, request, &source_proposal)?;
        if prepared.moving_profile_id == recipient.profile_id.into_uuid() {
            return Err(OperationsError::Conflict(
                "Home recipient and moving Profiles must differ",
            ));
        }
        let predecessor = validate_prior_home_predecessor(
            &transaction,
            identity,
            request,
            &source_proposal,
            predecessor_source,
        )?;
        let recorded_at = sample_clock()?;
        let insert = store::IntentInsert {
            identity,
            request,
            proposal: Some(&source_proposal),
            proposal_snapshot: Some(&proposal_snapshot),
            predecessor_acceptance_id: predecessor.map(|value| value.acceptance_id),
            predecessor_source_digest: predecessor.map(|value| value.source_digest),
            recorded_at,
        };
        let row = store::insert_intent(&transaction, &insert)?;
        transaction.commit().map_err(|source| {
            OperationsError::HomeAcceptanceCommitUnconfirmed {
                acceptance_id: request.acceptance_id(),
                source,
            }
        })?;
        Ok(row.receipt(false))
    }

    /// Revalidate current Operations facts, consume the exact actual owner
    /// holds, and persist admission plus the successor's Pending-work
    /// reservation before returning a Source grant. The window callback is a
    /// pure configured-policy function; it runs inside the first admission
    /// transaction only and is skipped on exact replay.
    ///
    /// # Errors
    /// Rejects changed intent/proposal, non-Held owner records, stale current
    /// bindings, unrelated competing reservations, unavailable Temporary
    /// Dispatch activation, invalid configured grant window, and uncertain
    /// commit. A terminal Aborted resolution absorbs any delayed admission.
    #[allow(
        clippy::too_many_arguments,
        reason = "the Source proposal, predecessor, two actual owner observations, local clock, and configured grant policy are independent admission inputs"
    )]
    pub fn admit_home_acceptance<C, W>(
        &mut self,
        identity: HomeAcceptanceSourceIdentity,
        acceptance_id: Uuid,
        source_proposal: HomeHandoffProposalRecord,
        predecessor_source: Option<&HomeHandoffAcceptanceOutcomeRecord>,
        session_record: &HomeAcceptanceSessionRecord,
        profile_record: &HomeAcceptanceProfilePinRecord,
        sample_clock: C,
        configured_grant_window: W,
    ) -> Result<HomeAcceptanceAdmissionOutcome, OperationsError>
    where
        C: FnOnce() -> Result<TimestampMillis, OperationsError>,
        W: FnOnce(
            TimestampMillis,
            Option<TimestampMillis>,
        ) -> Result<(TimestampMillis, TimestampMillis), OperationsError>,
    {
        if acceptance_id.is_nil() || identity.destination() != self.destination_id() {
            return Err(OperationsError::Conflict(
                "Home admission configured identity",
            ));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let intent = store::load_intent(&transaction, acceptance_id)?.ok_or(
            OperationsError::Conflict("Home acceptance intent is missing"),
        )?;
        ensure_request_identity(identity, intent.request)?;
        if intent.identity != identity {
            return Err(OperationsError::Conflict("Home admission Source identity"));
        }
        if let Some(replay) = replay_home_admission(
            &transaction,
            identity,
            &intent,
            &source_proposal,
            session_record,
            profile_record,
        )? {
            transaction.commit()?;
            return Ok(replay);
        }

        let predecessor = validate_admission_predecessor(
            &transaction,
            identity,
            &intent,
            &source_proposal,
            predecessor_source,
        )?;

        let prepared = load_and_validate_preparation(
            &transaction,
            identity,
            intent.request,
            &source_proposal,
        )?;
        let recipient = resolve_recipient(&transaction, intent.request, &source_proposal)?;
        if prepared.moving_profile_id == recipient.profile_id.into_uuid() {
            return Err(OperationsError::Conflict(
                "Home recipient and moving Profiles must differ",
            ));
        }
        let (session_hold, profile_pin) = validate_owner_holds(
            identity,
            intent.request,
            &recipient,
            session_record,
            profile_record,
        )?;
        let work = resolve_work_authority(
            &transaction,
            intent.request.scope().workspace_id(),
            &source_proposal,
            &recipient,
        )?;
        ensure_admission_work_scope_available(
            &transaction,
            &work,
            source_proposal.source().assignment_id(),
            predecessor.map(|value| value.acceptance_id),
        )?;

        let now = sample_clock()?;
        let (issued_at, expires_at) = configured_grant_window(now, None)?;
        if issued_at > now || now >= expires_at || expires_at <= issued_at {
            return Err(OperationsError::Conflict("configured Home grant window"));
        }
        let admission_facts = store::AdmissionFacts {
            moving_profile_id: prepared.moving_profile_id,
            recipient_profile_id: recipient.profile_id,
            recipient_participant_id: recipient.participant_id,
            recipient_session_id: recipient.session_id,
            recipient_participation_id: recipient.participation_id,
            recipient_participation_revision: recipient.participation_revision,
            recipient_membership_revision: recipient.membership_revision,
            session_hold,
            profile_pin,
            node_scope: work.node_scope,
            root_assignment_id: work.root_assignment_id,
            successor_assignment_id: work.successor_assignment_id,
            authority: work.authority,
            affinity_digest: work.affinity_digest,
            node_binding_revision: work.node_binding_revision,
            node_binding_digest: work.node_binding_digest,
            team_membership_revision: work.team_membership_revision,
            issued_at,
            expires_at,
            admitted_at: now,
        };
        persist_home_admission(transaction, acceptance_id, &intent, &admission_facts)
    }

    /// Persist the absorbing Operations resolution for one exact Source
    /// terminal. Actual Session/Profile records are required when admission
    /// consumed their holds; an Aborted terminal with no admission takes neither
    /// record and does not fabricate holder evidence.
    ///
    /// If Source Aborted before this Operations catalog ever staged the take,
    /// this call writes a proposal-less tombstone and resolution atomically.
    /// A later stage/admit replay cannot reopen it.
    ///
    /// # Errors
    /// Rejects changed Source or Operations identities, Applied without
    /// admission, mismatched/absent actual owner records, stale replay, and
    /// uncertain commit.
    pub fn resolve_home_acceptance<C>(
        &mut self,
        identity: HomeAcceptanceSourceIdentity,
        source_terminal: &HomeHandoffAcceptanceOutcomeRecord,
        session_record: Option<&HomeAcceptanceSessionRecord>,
        profile_record: Option<&HomeAcceptanceProfilePinRecord>,
        sample_clock: C,
    ) -> Result<HomeAcceptanceResolutionReceipt, OperationsError>
    where
        C: FnOnce() -> Result<TimestampMillis, OperationsError>,
    {
        if identity.destination() != self.destination_id() {
            return Err(OperationsError::Conflict("Home resolution destination"));
        }
        ensure_source_request_identity(identity, source_terminal.request())?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(intent) = store::load_intent(&transaction, source_terminal.acceptance_id())?
        else {
            if source_terminal.outcome_kind() != HomeHandoffAcceptanceOutcomeKind::Aborted
                || session_record.is_some()
                || profile_record.is_some()
            {
                return Err(OperationsError::Conflict(
                    "Home Applied terminal requires its staged Operations admission",
                ));
            }
            let resolved_at = sample_clock()?;
            let receipt =
                resolve_unstaged_abort(&transaction, identity, source_terminal, resolved_at)?;
            transaction.commit().map_err(|source| {
                OperationsError::HomeAcceptanceCommitUnconfirmed {
                    acceptance_id: source_terminal.acceptance_id(),
                    source,
                }
            })?;
            return Ok(receipt);
        };
        if intent.identity != identity || intent.request != source_terminal.request() {
            return Err(OperationsError::Conflict("Home resolution intent identity"));
        }
        validate_source_terminal(identity, &intent, source_terminal)?;

        if let Some(receipt) = replay_home_resolution(
            &transaction,
            &intent,
            source_terminal,
            session_record,
            profile_record,
        )? {
            transaction.commit()?;
            return Ok(receipt);
        }

        let admission = store::load_admission(&transaction, &intent, false)?;
        if source_terminal.outcome_kind() == HomeHandoffAcceptanceOutcomeKind::Applied {
            let admission = admission.as_ref().ok_or(OperationsError::Conflict(
                "Home Applied Source terminal has no Operations admission",
            ))?;
            validate_applied_source_matches_admission(&intent, source_terminal, admission)?;
        }
        validate_resolution_owner_records(
            &intent,
            admission.as_ref().map(|value| &value.receipt),
            source_terminal,
            session_record,
            profile_record,
            None,
            false,
        )?;
        let resolved_at = sample_clock()?;
        validate_resolution_clock(resolved_at, admission.as_ref().map(|value| &value.receipt))?;
        persist_home_resolution(
            transaction,
            &intent,
            source_terminal,
            admission.as_ref(),
            resolved_at,
        )
    }

    /// End one Applied successor reservation only after the configured Source
    /// returned that exact current assignment in a terminal state. Submitted
    /// work remains Active. This method reconciles Home reservation history; it
    /// does not rewrite the original Assignment affinity.
    ///
    /// # Errors
    /// Rejects missing or foreign Source/Operations history, changed successor
    /// lineage/scope, nonterminal assignment observations, and uncertain commit.
    pub fn reconcile_home_acceptance_work<C>(
        &mut self,
        identity: HomeAcceptanceSourceIdentity,
        source_terminal: &HomeHandoffAcceptanceOutcomeRecord,
        current_successor: &RegisteredAgentAssignment,
        sample_clock: C,
    ) -> Result<HomeAcceptanceWorkReservationReceipt, OperationsError>
    where
        C: FnOnce() -> Result<TimestampMillis, OperationsError>,
    {
        if identity.destination() != self.destination_id() {
            return Err(OperationsError::Conflict(
                "Home work reconciliation destination",
            ));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let intent = store::load_intent(&transaction, source_terminal.acceptance_id())?.ok_or(
            OperationsError::Conflict("Home reconciliation intent is missing"),
        )?;
        if intent.identity != identity {
            return Err(OperationsError::Conflict(
                "Home work reconciliation Source identity",
            ));
        }
        validate_source_terminal(identity, &intent, source_terminal)?;
        if source_terminal.outcome_kind() != HomeHandoffAcceptanceOutcomeKind::Applied {
            return Err(OperationsError::Conflict(
                "Aborted Home acceptance has no active successor work",
            ));
        }
        let resolution = store::load_resolution(&transaction, &intent, false)?.ok_or(
            OperationsError::Conflict("Home acceptance resolution is missing"),
        )?;
        if resolution.receipt.outcome() != HomeHandoffAcceptanceOutcomeKind::Applied
            || resolution.receipt.source_payload_digest() != *source_terminal.payload_digest()
            || resolution.receipt.admission().is_none()
        {
            return Err(OperationsError::Conflict("Home successor resolution pins"));
        }
        verify_terminal_successor(&intent, source_terminal, current_successor)?;
        let terminal_observation = store::terminal_assignment_observation(
            source_terminal.acceptance_id(),
            *source_terminal.payload_digest(),
            current_successor,
        )?;
        let work =
            store::load_work_reservation(&transaction, source_terminal.acceptance_id(), false)?
                .ok_or(OperationsError::Conflict(
                    "Home successor reservation is missing",
                ))?;
        let admission = resolution
            .receipt
            .admission()
            .ok_or(OperationsError::Corrupt(
                "Applied Home work admission is missing",
            ))?;
        store::verify_work_reservation_source(&intent, &admission, &work, source_terminal)?;
        if work.state() == HomeAcceptanceWorkReservationState::Terminal {
            if work.terminal_source_digest() == Some(*source_terminal.payload_digest())
                && work.terminal_assignment_state() == Some(terminal_observation.state)
                && work.terminal_assignment_revision() == Some(terminal_observation.revision)
                && work.terminal_assignment_cancellation_requested()
                    == Some(terminal_observation.cancellation_requested)
                && work.terminal_assignment_digest() == Some(terminal_observation.digest)
                && work.terminal_assignment_updated_at() == Some(terminal_observation.updated_at)
            {
                transaction.commit()?;
                return store::load_work_reservation(
                    &self.connection,
                    source_terminal.acceptance_id(),
                    true,
                )?
                .ok_or(OperationsError::Corrupt(
                    "Home terminal reservation disappeared",
                ));
            }
            return Err(OperationsError::Conflict(
                "Home terminal Source digest changed",
            ));
        }
        if work.state() != HomeAcceptanceWorkReservationState::Active {
            return Err(OperationsError::Conflict(
                "Home successor reservation is not Active",
            ));
        }
        let updated_at = sample_clock()?;
        if updated_at < work.updated_at() {
            return Err(OperationsError::Conflict(
                "Home work reconciliation clock regressed",
            ));
        }
        let receipt = store::transition_work_reservation(
            &transaction,
            source_terminal.acceptance_id(),
            HomeAcceptanceWorkReservationState::Active,
            HomeAcceptanceWorkReservationState::Terminal,
            Some(*source_terminal.payload_digest()),
            Some(terminal_observation),
            updated_at,
        )?;
        transaction.commit().map_err(|source| {
            OperationsError::HomeAcceptanceCommitUnconfirmed {
                acceptance_id: source_terminal.acceptance_id(),
                source,
            }
        })?;
        Ok(receipt)
    }

    /// Read one exact Home intent and its Operations-owned durable outcomes.
    /// Session/Profile owners remain separately queried by Runtime.
    ///
    /// # Errors
    /// Returns an error when the configured Source identity is wrong, SQLite
    /// access fails, or persisted Home evidence is internally inconsistent.
    pub fn home_acceptance(
        &self,
        identity: HomeAcceptanceSourceIdentity,
        acceptance_id: Uuid,
    ) -> Result<Option<HomeAcceptanceRecord>, OperationsError> {
        if identity.destination() != self.destination_id() || acceptance_id.is_nil() {
            return Err(OperationsError::Conflict("Home readback identity"));
        }
        let transaction = self.connection.unchecked_transaction()?;
        let Some(intent) = store::load_intent(&transaction, acceptance_id)? else {
            transaction.commit()?;
            return Ok(None);
        };
        if intent.identity != identity {
            return Err(OperationsError::Conflict("Home readback Source identity"));
        }
        let record = read_record_in_transaction(&transaction, &intent)?;
        transaction.commit()?;
        Ok(Some(record))
    }

    /// Read a bounded, stable page of unresolved Home admissions/intents for
    /// restart recovery. The caller then reads exact Source and owner records.
    ///
    /// # Errors
    /// Returns an error for an invalid identity/page, failed SQLite access, or
    /// inconsistent persisted Home evidence.
    pub fn pending_home_acceptances(
        &self,
        identity: HomeAcceptanceSourceIdentity,
        workspace_id: WorkspaceId,
        after: Option<Uuid>,
        limit: u16,
    ) -> Result<HomeAcceptancePendingPage, OperationsError> {
        if identity.destination() != self.destination_id() || workspace_id.into_uuid().is_nil() {
            return Err(OperationsError::Conflict("Home recovery page identity"));
        }
        let transaction = self.connection.unchecked_transaction()?;
        let (ids, next_after) =
            store::pending_acceptance_ids(&transaction, workspace_id, after, limit)?;
        let mut records = Vec::with_capacity(ids.len());
        for acceptance_id in ids {
            let intent = store::load_intent(&transaction, acceptance_id)?
                .ok_or(OperationsError::Corrupt("Home recovery intent disappeared"))?;
            if intent.identity != identity {
                return Err(OperationsError::Corrupt("Home recovery Source identity"));
            }
            records.push(read_record_in_transaction(&transaction, &intent)?);
        }
        transaction.commit()?;
        Ok(HomeAcceptancePendingPage::from_parts(records, next_after))
    }
}

fn resolve_unstaged_abort(
    transaction: &Transaction<'_>,
    identity: HomeAcceptanceSourceIdentity,
    source_terminal: &HomeHandoffAcceptanceOutcomeRecord,
    resolved_at: TimestampMillis,
) -> Result<HomeAcceptanceResolutionReceipt, OperationsError> {
    validate_resolution_clock(resolved_at, None)?;
    let insert = store::IntentInsert {
        identity,
        request: source_terminal.request(),
        proposal: None,
        proposal_snapshot: None,
        predecessor_acceptance_id: None,
        predecessor_source_digest: None,
        recorded_at: resolved_at,
    };
    let intent = store::insert_intent(transaction, &insert)?;
    validate_source_terminal(identity, &intent, source_terminal)?;
    let resolution =
        store::insert_resolution(transaction, &intent, source_terminal, None, resolved_at)?;
    Ok(resolution.receipt)
}

fn persist_home_admission(
    transaction: Transaction<'_>,
    acceptance_id: Uuid,
    intent: &store::IntentRow,
    facts: &store::AdmissionFacts,
) -> Result<HomeAcceptanceAdmissionOutcome, OperationsError> {
    let admission = store::insert_admission(&transaction, intent, facts)?;
    let _pending_work =
        store::insert_work_reservation(&transaction, &admission.receipt, facts.admitted_at)?;
    transaction
        .commit()
        .map_err(|source| OperationsError::HomeAcceptanceCommitUnconfirmed {
            acceptance_id,
            source,
        })?;
    Ok(HomeAcceptanceAdmissionOutcome::Admitted(admission.receipt))
}

fn persist_home_resolution(
    transaction: Transaction<'_>,
    intent: &store::IntentRow,
    source_terminal: &HomeHandoffAcceptanceOutcomeRecord,
    admission: Option<&store::AdmissionRow>,
    resolved_at: TimestampMillis,
) -> Result<HomeAcceptanceResolutionReceipt, OperationsError> {
    let resolution = store::insert_resolution(
        &transaction,
        intent,
        source_terminal,
        admission,
        resolved_at,
    )?;
    match source_terminal.outcome_kind() {
        HomeHandoffAcceptanceOutcomeKind::Applied => {
            let _active_work = store::transition_work_reservation(
                &transaction,
                source_terminal.acceptance_id(),
                HomeAcceptanceWorkReservationState::Pending,
                HomeAcceptanceWorkReservationState::Active,
                None,
                None,
                resolved_at,
            )?;
        }
        HomeHandoffAcceptanceOutcomeKind::Aborted if admission.is_some() => {
            let _aborted_work = store::transition_work_reservation(
                &transaction,
                source_terminal.acceptance_id(),
                HomeAcceptanceWorkReservationState::Pending,
                HomeAcceptanceWorkReservationState::Aborted,
                Some(*source_terminal.payload_digest()),
                None,
                resolved_at,
            )?;
        }
        HomeHandoffAcceptanceOutcomeKind::Aborted => {}
    }
    transaction
        .commit()
        .map_err(|source| OperationsError::HomeAcceptanceCommitUnconfirmed {
            acceptance_id: source_terminal.acceptance_id(),
            source,
        })?;
    Ok(resolution.receipt)
}

fn replay_home_resolution(
    transaction: &Transaction<'_>,
    intent: &store::IntentRow,
    source_terminal: &HomeHandoffAcceptanceOutcomeRecord,
    session_record: Option<&HomeAcceptanceSessionRecord>,
    profile_record: Option<&HomeAcceptanceProfilePinRecord>,
) -> Result<Option<HomeAcceptanceResolutionReceipt>, OperationsError> {
    let Some(existing) = store::load_resolution(transaction, intent, true)? else {
        return Ok(None);
    };
    if existing.source.payload_digest() != source_terminal.payload_digest()
        || existing.source_snapshot
            != source_terminal
                .to_snapshot_bytes()
                .map_err(|_| OperationsError::Conflict("Home Source terminal snapshot"))?
    {
        return Err(OperationsError::Conflict(
            "Home Source terminal replay changed",
        ));
    }
    let admission = existing.receipt.admission();
    validate_resolution_owner_records(
        intent,
        admission.as_ref(),
        source_terminal,
        session_record,
        profile_record,
        Some(&existing.receipt),
        true,
    )?;
    Ok(Some(existing.receipt))
}

#[derive(Debug, Clone, Copy)]
struct PreparedSourceFacts {
    moving_profile_id: Uuid,
}

#[derive(Debug, Clone, Copy)]
struct RecipientFacts {
    profile_id: AgentProfileId,
    participant_id: Uuid,
    session_id: AgentSessionId,
    participation_id: WorkspaceParticipationId,
    participation_revision: u64,
    membership_revision: u64,
    mapping_request_id: Uuid,
    mapping_profile_revision: AgentProfileRevision,
    mapping_session_revision: AgentSessionRevision,
}

#[derive(Debug, Clone, Copy)]
struct WorkAuthorityFacts {
    node_scope: NodeTeamBindingScope,
    root_assignment_id: AgentAssignmentId,
    successor_assignment_id: AgentAssignmentId,
    authority: HomeHandoffWorkAuthority,
    affinity_digest: [u8; 32],
    node_binding_revision: Option<u64>,
    node_binding_digest: [u8; 32],
    team_membership_revision: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PriorHomeAcceptance {
    acceptance_id: Uuid,
    source_digest: [u8; 32],
}

fn replay_home_admission(
    transaction: &Transaction<'_>,
    identity: HomeAcceptanceSourceIdentity,
    intent: &store::IntentRow,
    source_proposal: &HomeHandoffProposalRecord,
    session_record: &HomeAcceptanceSessionRecord,
    profile_record: &HomeAcceptanceProfilePinRecord,
) -> Result<Option<HomeAcceptanceAdmissionOutcome>, OperationsError> {
    if let Some(resolution) = store::load_resolution(transaction, intent, true)? {
        return Ok(Some(HomeAcceptanceAdmissionOutcome::Resolved(
            resolution.receipt,
        )));
    }
    let Some(staged_proposal) = intent.proposal else {
        return Err(OperationsError::Corrupt(
            "unresolved Home acceptance has no staged proposal",
        ));
    };
    let proposal_snapshot = source_proposal
        .to_snapshot_bytes()
        .map_err(|_| OperationsError::Conflict("Home Source proposal snapshot"))?;
    if proposal_snapshot.as_slice() != intent.proposal_snapshot.as_deref().unwrap_or_default()
        || source_proposal.payload_digest() != &intent.proposal_digest.unwrap_or([0; 32])
        || source_proposal != &staged_proposal
    {
        return Err(OperationsError::Conflict("Home admission proposal changed"));
    }
    let Some(existing) = store::load_admission(transaction, intent, true)? else {
        return Ok(None);
    };
    validate_held_owner_records(
        identity,
        intent.request,
        &existing.receipt,
        session_record,
        profile_record,
        None,
        false,
    )?;
    Ok(Some(HomeAcceptanceAdmissionOutcome::Admitted(
        existing.receipt,
    )))
}

fn validate_admission_predecessor(
    transaction: &Transaction<'_>,
    identity: HomeAcceptanceSourceIdentity,
    intent: &store::IntentRow,
    source_proposal: &HomeHandoffProposalRecord,
    predecessor_source: Option<&HomeHandoffAcceptanceOutcomeRecord>,
) -> Result<Option<PriorHomeAcceptance>, OperationsError> {
    let predecessor = validate_prior_home_predecessor(
        transaction,
        identity,
        intent.request,
        source_proposal,
        predecessor_source,
    )?;
    if predecessor.map(|value| (value.acceptance_id, value.source_digest))
        != intent
            .predecessor_acceptance_id
            .zip(intent.predecessor_source_digest)
    {
        return Err(OperationsError::Conflict(
            "Home predecessor Applied receipt changed",
        ));
    }
    Ok(predecessor)
}

fn ensure_admission_work_scope_available(
    transaction: &Transaction<'_>,
    work: &WorkAuthorityFacts,
    predecessor_assignment_id: AgentAssignmentId,
    predecessor_acceptance_id: Option<Uuid>,
) -> Result<(), OperationsError> {
    let expected_team_id = match work.authority {
        HomeHandoffWorkAuthority::WorkspaceWork { .. } => None,
        HomeHandoffWorkAuthority::HomeTeam(value) => Some(value.team_id()),
        HomeHandoffWorkAuthority::TemporaryDispatch(_) => {
            return Err(OperationsError::Conflict(
                "Home acceptance Temporary Dispatch authority is unavailable",
            ));
        }
    };
    ensure_no_competing_home_reservation(
        transaction,
        work.node_scope,
        work.root_assignment_id,
        predecessor_assignment_id,
        predecessor_acceptance_id,
        expected_team_id,
    )
}

fn ensure_configured_identity(
    identity: HomeAcceptanceSourceIdentity,
    catalog_destination: Uuid,
    request: HomeHandoffTakeRequest,
) -> Result<(), OperationsError> {
    if identity.destination() != catalog_destination
        || request.scope().workspace_id().into_uuid().is_nil()
        || request.scope().backend_id() != identity.backend()
        || request.scope().destination_id() != identity.destination()
    {
        return Err(OperationsError::Conflict(
            "Home request does not match configured Source scope",
        ));
    }
    Ok(())
}

fn ensure_request_identity(
    identity: HomeAcceptanceSourceIdentity,
    request: HomeHandoffTakeRequest,
) -> Result<(), OperationsError> {
    if identity.backend() != request.scope().backend_id()
        || identity.destination() != request.scope().destination_id()
        || request.scope().workspace_id().into_uuid().is_nil()
    {
        return Err(OperationsError::Conflict("Home Source request scope"));
    }
    Ok(())
}

fn ensure_source_request_identity(
    identity: HomeAcceptanceSourceIdentity,
    request: HomeHandoffTakeRequest,
) -> Result<(), OperationsError> {
    ensure_request_identity(identity, request)
}

fn validate_proposal_request(
    request: HomeHandoffTakeRequest,
    proposal: &HomeHandoffProposalRecord,
) -> Result<(), OperationsError> {
    let proposal_request = proposal.request();
    let source = proposal.source();
    if proposal_request.proposal_id() != request.proposal_id()
        || proposal_request.handoff_id() != request.handoff_id()
        || proposal_request.request_digest() != request.proposal_digest()
        || proposal.case_revision() != request.expected_case_revision()
        || proposal_request.preparation_id().is_nil()
        || !proposal_request
            .recipient()
            .has_same_identity(request.recipient())
        || source.workspace_id() != request.scope().workspace_id()
        || source.assignment_id() != proposal_request.predecessor_id()
        || source.activity_id() != proposal_request.coordinator().activity_id()
        || source.lineage().root_assignment_id(source.assignment_id())
            != source.root_assignment_id()
    {
        return Err(OperationsError::Conflict(
            "Home take request differs from exact Source proposal",
        ));
    }
    Ok(())
}

fn load_and_validate_preparation(
    transaction: &Transaction<'_>,
    identity: HomeAcceptanceSourceIdentity,
    request: HomeHandoffTakeRequest,
    proposal: &HomeHandoffProposalRecord,
) -> Result<PreparedSourceFacts, OperationsError> {
    validate_proposal_request(request, proposal)?;
    let preparation_id = proposal.request().preparation_id();
    let context =
        crate::team_permanent_transfer::source::load_home_preparation_for_handoff_in_transaction(
            transaction,
            request.scope().workspace_id(),
            preparation_id,
            identity.destination(),
        )?
        .ok_or(OperationsError::Conflict(
            "Home acceptance requires an exact Operations preparation and confirmed Source receipt",
        ))?;
    let preparation = context.preparation();
    let source_record = context.source().source_record();
    let source_request = source_record.request();
    let source_subject = source_request.subject();
    let source_pin = proposal.source();
    if preparation.preparation_id() != preparation_id
        || preparation.workspace_id() != request.scope().workspace_id()
        || preparation.operations_destination_id() != identity.destination()
        || source_record.issuer() != identity.issuer()
        || source_record.state() != forge_persistence::HomeTransferSourceState::Prepared
        || source_request.scope() != request.scope()
        || source_request.witness().preparation_id() != preparation_id
        || source_subject.target_profile_id() != preparation.target_profile_id()
        || proposal.prepared_outcome_digest() != source_record.outcome_digest()
        || source_pin.workspace_id() != request.scope().workspace_id()
    {
        return Err(OperationsError::Conflict(
            "Home Source preparation differs from its exact prepared receipt",
        ));
    }
    Ok(PreparedSourceFacts {
        moving_profile_id: preparation.target_profile_id(),
    })
}

fn resolve_recipient(
    connection: &Connection,
    request: HomeHandoffTakeRequest,
    proposal: &HomeHandoffProposalRecord,
) -> Result<RecipientFacts, OperationsError> {
    let workspace_id = request.scope().workspace_id();
    let target = request.recipient();
    let activity_id = proposal.request().coordinator().activity_id();
    let ready = presence::resolve_ready_logical_binding(
        connection,
        workspace_id,
        activity_id,
        target.agent_id(),
        target.session_id(),
        target.principal(),
    )?;
    let actor = OperationalActor::Participant {
        activity_id,
        agent_id: target.agent_id(),
        session_id: target.session_id(),
    };
    let resolved = crate::operational_actor::resolve_actor(connection, workspace_id, actor)?;
    if !resolved.is_profile_participant()
        || resolved.participant_id() != Some(ready.participant_id)
        || resolved.session_id() != Some(target.session_id())
    {
        return Err(OperationsError::Conflict(
            "Home recipient Agent, Session, principal, and participant disagree",
        ));
    }
    let profile_id = AgentProfileId::from_uuid(
        resolved
            .profile_id()
            .ok_or(OperationsError::Conflict("Home recipient has no Profile"))?,
    )
    .map_err(|_| OperationsError::Conflict("Home recipient Profile identity"))?;
    let participation_id = resolved
        .participation_id()
        .ok_or(OperationsError::Conflict(
            "Home recipient has no participation",
        ))?;
    let participation =
        workspace_participation::load_participation(connection, workspace_id, profile_id)?.ok_or(
            OperationsError::Conflict("Home recipient has no current Workspace participation"),
        )?;
    if participation.workspace_id() != workspace_id
        || participation.profile_id() != profile_id
        || participation.participation_id() != participation_id
    {
        return Err(OperationsError::Conflict(
            "Home recipient participation does not match its Profile",
        ));
    }
    let mut matched = participation.session_mappings().iter().filter(|mapping| {
        mapping.participation_id() == participation_id
            && mapping.profile_id() == profile_id
            && mapping.session_id() == target.session_id()
            && mapping.participant_id() == ready.participant_id
            && mapping.participant_membership_revision() == ready.membership_revision
    });
    let mapping = matched.next().ok_or(OperationsError::Conflict(
        "Home recipient Session is not mapped to its current participation",
    ))?;
    if matched.next().is_some() {
        return Err(OperationsError::Corrupt(
            "duplicate Home recipient participation mapping",
        ));
    }
    Ok(RecipientFacts {
        profile_id,
        participant_id: ready.participant_id,
        session_id: target.session_id(),
        participation_id,
        participation_revision: participation.revision(),
        membership_revision: ready.membership_revision,
        mapping_request_id: mapping.profile_mapping_request_id(),
        mapping_profile_revision: mapping.profile_revision(),
        mapping_session_revision: mapping.session_revision(),
    })
}

fn validate_owner_holds(
    identity: HomeAcceptanceSourceIdentity,
    request: HomeHandoffTakeRequest,
    recipient: &RecipientFacts,
    session_record: &HomeAcceptanceSessionRecord,
    profile_record: &HomeAcceptanceProfilePinRecord,
) -> Result<
    (
        HomeAcceptanceSessionHoldObservation,
        HomeAcceptanceProfilePinObservation,
    ),
    OperationsError,
> {
    if session_record.intent().request() != request
        || session_record.intent().issuer() != identity.issuer()
        || !matches!(
            session_record.state(),
            HomeAcceptanceSessionState::Held { .. }
        )
        || profile_record.request() != request
        || profile_record.issuer() != identity.issuer()
        || profile_record.state() != HomeAcceptanceProfileState::Held
    {
        return Err(OperationsError::Conflict(
            "Home admission requires the exact currently Held Session and Profile records",
        ));
    }
    let session_reference = session_record
        .held_reference()
        .ok_or(OperationsError::Conflict("Home Session hold is not Held"))?;
    let profile_reference = profile_record
        .profile_pin()
        .ok_or(OperationsError::Conflict("Home Profile pin is not Held"))?;
    if session_reference.acceptance_id() != request.acceptance_id()
        || session_reference.request_digest() != *request.request_digest().as_bytes()
        || session_reference.session_id() != recipient.session_id
        || session_reference.session_id() != request.recipient().session_id()
        || session_reference.principal() != request.recipient().principal()
        || profile_reference.profile_id() != recipient.profile_id
        || profile_reference.logical_session_request_id() != recipient.mapping_request_id
        || profile_reference.linked_profile_revision() != recipient.mapping_profile_revision
        || profile_reference.linked_session_revision() != recipient.mapping_session_revision
        || profile_reference.linked_session_revision() != session_reference.session_revision()
    {
        return Err(OperationsError::Conflict(
            "Home Session and Profile hold references differ from current recipient participation",
        ));
    }
    Ok((
        HomeAcceptanceSessionHoldObservation::from_owner(session_reference),
        HomeAcceptanceProfilePinObservation::from_owner(profile_reference),
    ))
}

fn validate_prior_home_predecessor(
    connection: &Connection,
    identity: HomeAcceptanceSourceIdentity,
    request: HomeHandoffTakeRequest,
    proposal: &HomeHandoffProposalRecord,
    prior_source: Option<&HomeHandoffAcceptanceOutcomeRecord>,
) -> Result<Option<PriorHomeAcceptance>, OperationsError> {
    let source_pin = proposal.source();
    match source_pin.lineage() {
        HomeHandoffAssignmentLineage::CoordinatorOffer => {
            if source_pin.root_assignment_id() != source_pin.assignment_id()
                || prior_source.is_some()
            {
                return Err(OperationsError::Conflict(
                    "Coordinator Home predecessor lineage is not a root offer",
                ));
            }
            Ok(None)
        }
        HomeHandoffAssignmentLineage::HomeHandoff(origin) => {
            let prior = prior_source.ok_or(OperationsError::Conflict(
                "Home-origin predecessor requires its exact prior Applied Source receipt",
            ))?;
            let applied = prior.applied().ok_or(OperationsError::Conflict(
                "Home-origin predecessor receipt is not Applied",
            ))?;
            let predecessor = applied.successor();
            if prior.issuer() != identity.issuer()
                || prior.request().scope() != request.scope()
                || prior.handoff_id() != origin.handoff_id()
                || prior.preparation_id() != origin.preparation_id()
                || prior.predecessor_id() != origin.predecessor_id()
                || prior.root_assignment_id() != origin.root_assignment_id()
                || prior.workspace_id() != request.scope().workspace_id()
                || predecessor.assignment_id() != source_pin.assignment_id()
                || predecessor.origin() != origin
                || predecessor.workspace_id() != source_pin.workspace_id()
                || predecessor.activity_id() != source_pin.activity_id()
                || predecessor.goal_id() != source_pin.goal_id()
                || predecessor.goal_created_at() != source_pin.goal_created_at()
                || predecessor.goal_revision() != source_pin.goal_revision()
                || predecessor.dag_revision() != source_pin.dag_revision()
                || predecessor.dag_head_revision() != source_pin.dag_head_revision()
                || predecessor.node_id() != source_pin.node_id()
                || predecessor.node_revision() != source_pin.node_revision()
                || predecessor.coordinator() != source_pin.coordinator()
                || predecessor.target() != source_pin.target()
                || predecessor.credit_budget() != source_pin.credit_budget()
                || predecessor.created_at() != source_pin.created_at()
                || source_pin.root_assignment_id() != origin.root_assignment_id()
            {
                return Err(OperationsError::Conflict(
                    "Home predecessor Source receipt does not prove exact lineage and scope",
                ));
            }
            let prior_intent = store::load_intent(connection, prior.acceptance_id())?.ok_or(
                OperationsError::Conflict(
                    "Home predecessor has no local immutable Operations intent",
                ),
            )?;
            if prior_intent.identity != identity || prior_intent.request != prior.request() {
                return Err(OperationsError::Conflict(
                    "Home predecessor Operations intent differs from Source",
                ));
            }
            let prior_resolution = store::load_resolution(connection, &prior_intent, false)?
                .ok_or(OperationsError::Conflict(
                    "Home predecessor has no local Applied Operations resolution",
                ))?;
            if prior_resolution.receipt.outcome() != HomeHandoffAcceptanceOutcomeKind::Applied
                || prior_resolution.receipt.source_payload_digest() != *prior.payload_digest()
            {
                return Err(OperationsError::Conflict(
                    "Home predecessor Operations resolution is not the exact Applied receipt",
                ));
            }
            let prior_admission = store::load_admission(connection, &prior_intent, false)?.ok_or(
                OperationsError::Corrupt("Applied Home predecessor has no admission"),
            )?;
            validate_applied_source_matches_admission(&prior_intent, prior, &prior_admission)?;
            let prior_work =
                store::load_work_reservation(connection, prior.acceptance_id(), false)?.ok_or(
                    OperationsError::Conflict("Home predecessor has no active work reservation"),
                )?;
            store::verify_work_reservation_source(
                &prior_intent,
                &prior_admission.receipt,
                &prior_work,
                prior,
            )?;
            if prior_work.state() != HomeAcceptanceWorkReservationState::Active
                || prior_work.successor_assignment_id() != source_pin.assignment_id()
                || prior_work.root_assignment_id() != source_pin.root_assignment_id()
                || prior_work.node_scope() != store::scope_from_proposal(proposal)?
            {
                return Err(OperationsError::Conflict(
                    "Home predecessor reservation does not match its exact Applied lineage",
                ));
            }
            Ok(Some(PriorHomeAcceptance {
                acceptance_id: prior.acceptance_id(),
                source_digest: *prior.payload_digest(),
            }))
        }
    }
}

fn validate_root_affinity_scope(
    root_affinity: &team_assignment::AssignmentAffinityReceipt,
    workspace_id: WorkspaceId,
    root_assignment_id: AgentAssignmentId,
    node_scope: NodeTeamBindingScope,
    lineage: &HomeHandoffAssignmentLineage,
) -> Result<(), OperationsError> {
    let disposition_is_live = matches!(
        root_affinity.disposition(),
        crate::AssignmentAffinityDisposition::Pending
            | crate::AssignmentAffinityDisposition::Active
    );
    let disposition_is_historical_home =
        matches!(lineage, HomeHandoffAssignmentLineage::HomeHandoff(_))
            && root_affinity.disposition() == crate::AssignmentAffinityDisposition::Terminal;
    if root_affinity.workspace_id() != workspace_id
        || root_affinity.assignment_id() != root_assignment_id
        || root_affinity.activity_id() != node_scope.activity_id()
        || root_affinity.node_scope() != Some(node_scope)
        || !(disposition_is_live || disposition_is_historical_home)
    {
        return Err(OperationsError::Conflict(
            "Home root Assignment affinity does not match exact Node scope",
        ));
    }
    Ok(())
}

struct NodeWorkAuthorityFacts {
    authority: HomeHandoffWorkAuthority,
    node_revision: Option<u64>,
    node_digest: [u8; 32],
    team_membership_revision: Option<u64>,
}

fn resolve_workspace_work_authority(
    root_affinity: &team_assignment::AssignmentAffinityReceipt,
    node_scope: NodeTeamBindingScope,
    current_binding: Option<&crate::node_team::NodeTeamBinding>,
) -> Result<NodeWorkAuthorityFacts, OperationsError> {
    let (node_revision, node_digest) = match current_binding {
        None => {
            if root_affinity.binding_revision().is_some() {
                return Err(OperationsError::Conflict(
                    "absent Workspace Node binding differs from root affinity revision",
                ));
            }
            (None, store::node_binding_digest(node_scope, None))
        }
        Some(binding)
            if binding.scope() == Some(node_scope)
                && binding.team_id().is_none()
                && root_affinity.binding_revision() == Some(binding.revision()) =>
        {
            (
                Some(binding.revision()),
                store::node_binding_digest(node_scope, Some(binding)),
            )
        }
        Some(_) => {
            return Err(OperationsError::Conflict(
                "Workspace work requires an exact unscoped current Node binding",
            ));
        }
    };
    let affinity_digest =
        store::affinity_reference_digest(root_affinity, node_scope, Some(node_digest), None);
    let affinity = HomeHandoffAffinityAdmissionReferenceDigest::from_bytes(affinity_digest)
        .map_err(|_| OperationsError::Corrupt("Home affinity reference digest"))?;
    Ok(NodeWorkAuthorityFacts {
        authority: HomeHandoffWorkAuthority::WorkspaceWork {
            affinity_admission_reference_digest: affinity,
        },
        node_revision,
        node_digest,
        team_membership_revision: None,
    })
}

fn resolve_work_authority(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    proposal: &HomeHandoffProposalRecord,
    recipient: &RecipientFacts,
) -> Result<WorkAuthorityFacts, OperationsError> {
    let source_pin = proposal.source();
    let node_scope = store::scope_from_proposal(proposal)?;
    let root_assignment_id = source_pin.root_assignment_id();
    let root_affinity = team_assignment::read_assignment_affinity_in_transaction(
        transaction,
        workspace_id,
        root_assignment_id,
    )?
    .ok_or(OperationsError::Conflict(
        "Home root Assignment has no explicit Operations affinity",
    ))?;
    validate_root_affinity_scope(
        &root_affinity,
        workspace_id,
        root_assignment_id,
        node_scope,
        &source_pin.lineage(),
    )?;
    if crate::node_team::has_legacy_node_binding(transaction, node_scope)? {
        return Err(OperationsError::Conflict(
            "Home Node has an unreconciled legacy Team binding",
        ));
    }
    let current_binding =
        crate::node_team::read_node_team_binding_in_transaction(transaction, node_scope)?;
    let root_team = root_affinity.team_id();
    reject_unbound_temporary_dispatch(transaction, workspace_id, recipient.participant_id)?;
    let NodeWorkAuthorityFacts {
        authority,
        node_revision,
        node_digest,
        team_membership_revision,
    } = match root_team {
        None => {
            resolve_workspace_work_authority(&root_affinity, node_scope, current_binding.as_ref())?
        }
        Some(team_id) => {
            let binding = current_binding.as_ref().ok_or(OperationsError::Conflict(
                "Home Team work requires a current exact Node-Team binding",
            ))?;
            if binding.scope() != Some(node_scope) || binding.team_id() != Some(team_id) {
                return Err(OperationsError::Conflict(
                    "Home Team affinity and current Node binding disagree",
                ));
            }
            if root_affinity.binding_revision() != Some(binding.revision()) {
                return Err(OperationsError::Conflict(
                    "Home root affinity binding revision differs from current Node-Team binding",
                ));
            }
            let membership_revision = team_assignment::verify_team_membership(
                transaction,
                workspace_id,
                team_id,
                recipient.participation_id,
            )?;
            let node_digest = store::node_binding_digest(node_scope, Some(binding));
            let affinity_digest = store::affinity_reference_digest(
                &root_affinity,
                node_scope,
                Some(node_digest),
                Some(membership_revision),
            );
            let affinity = HomeHandoffAffinityAdmissionReferenceDigest::from_bytes(affinity_digest)
                .map_err(|_| OperationsError::Corrupt("Home affinity reference digest"))?;
            let node_binding_digest = HomeHandoffNodeBindingDigest::from_bytes(node_digest)
                .map_err(|_| OperationsError::Corrupt("Home Node binding digest"))?;
            let authority = HomeHandoffHomeTeamAuthority::new(
                affinity,
                team_id,
                membership_revision,
                binding.revision(),
                node_binding_digest,
            )
            .map_err(|_| OperationsError::Conflict("invalid Home Team authority"))?;
            NodeWorkAuthorityFacts {
                authority: HomeHandoffWorkAuthority::HomeTeam(authority),
                node_revision: Some(binding.revision()),
                node_digest,
                team_membership_revision: Some(membership_revision),
            }
        }
    };
    let affinity_digest = *authority.affinity_admission_reference_digest().as_bytes();
    Ok(WorkAuthorityFacts {
        node_scope,
        root_assignment_id,
        successor_assignment_id: proposal.request().successor_id(),
        authority,
        affinity_digest,
        node_binding_revision: node_revision,
        node_binding_digest: node_digest,
        team_membership_revision,
    })
}

fn reject_unbound_temporary_dispatch(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
) -> Result<(), OperationsError> {
    let reservation: bool = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM ops_temporary_dispatch_reservations
            WHERE workspace_id=?1 AND target_participant_id=?2
              AND state IN('SOURCE_READY','ACTIVE','DRAINING')
        )",
        params![
            workspace_id.into_uuid().as_bytes(),
            participant_id.as_bytes()
        ],
        |row| row.get(0),
    )?;
    if reservation {
        return Err(OperationsError::Conflict(
            "Temporary Dispatch has no exact active Source-work binding for Home acceptance",
        ));
    }
    Ok(())
}

fn ensure_no_competing_home_reservation(
    connection: &Connection,
    scope: NodeTeamBindingScope,
    root_assignment_id: AgentAssignmentId,
    predecessor_assignment_id: AgentAssignmentId,
    prior_acceptance_id: Option<Uuid>,
    expected_team_id: Option<Uuid>,
) -> Result<(), OperationsError> {
    let mut statement = connection.prepare(
        "SELECT substr(acceptance_id,1,17),substr(root_assignment_id,1,17),
                substr(successor_assignment_id,1,17),team_id,state
         FROM ops_home_acceptance_work_reservations
         WHERE workspace_id=?1 AND activity_id=?2 AND goal_id=?3
           AND goal_created_at_ms=?4 AND dag_revision=?5
           AND dag_base_goal_revision=?6 AND node_id=?7 AND state IN(0,1)",
    )?;
    let mut rows = statement.query(params![
        scope.workspace_id().into_uuid().as_bytes(),
        scope.activity_id().into_uuid().as_bytes(),
        scope.goal_id().into_uuid().as_bytes(),
        scope.goal_created_at().get(),
        scope.dag_revision().get(),
        scope.dag_base_goal_revision().get(),
        scope.node_id().into_uuid().as_bytes(),
    ])?;
    while let Some(row) = rows.next()? {
        let acceptance_id = Uuid::from_bytes(crate::wire::id(row.get::<_, Vec<u8>>(0)?)?);
        let root = AgentAssignmentId::from_uuid(Uuid::from_bytes(crate::wire::id(
            row.get::<_, Vec<u8>>(1)?,
        )?));
        let successor = AgentAssignmentId::from_uuid(Uuid::from_bytes(crate::wire::id(
            row.get::<_, Vec<u8>>(2)?,
        )?));
        let team_id = row
            .get::<_, Option<Vec<u8>>>(3)?
            .map(|value| crate::wire::id(value).map(Uuid::from_bytes))
            .transpose()?;
        let state: i64 = row.get(4)?;
        let exact_predecessor = prior_acceptance_id == Some(acceptance_id)
            && state == HomeAcceptanceWorkReservationState::Active.encode()
            && root == root_assignment_id
            && successor == predecessor_assignment_id
            && team_id == expected_team_id;
        if !exact_predecessor {
            return Err(OperationsError::Conflict(
                "Node has an unrelated pending or active Home reservation",
            ));
        }
    }
    Ok(())
}

fn prior_source_pin(
    source: Option<&HomeHandoffAcceptanceOutcomeRecord>,
) -> Option<(Uuid, [u8; 32])> {
    source.map(|value| (value.acceptance_id(), *value.payload_digest()))
}

fn validate_source_terminal(
    identity: HomeAcceptanceSourceIdentity,
    intent: &store::IntentRow,
    source: &HomeHandoffAcceptanceOutcomeRecord,
) -> Result<(), OperationsError> {
    ensure_source_request_identity(identity, source.request())?;
    if source.issuer() != identity.issuer()
        || source.acceptance_id() != intent.request.acceptance_id()
        || source.request() != intent.request
        || source.workspace_id() != intent.request.scope().workspace_id()
        || source.payload_digest() == &[0; 32]
    {
        return Err(OperationsError::Conflict("Home Source terminal identity"));
    }
    match intent.proposal {
        Some(proposal) => {
            validate_proposal_request(intent.request, &proposal)?;
            let source_pin = proposal.source();
            if source.preparation_id() != proposal.request().preparation_id()
                || source.handoff_id() != proposal.request().handoff_id()
                || source.proposal_id() != proposal.request().proposal_id()
                || source.case_revision() != proposal.case_revision()
                || source.predecessor_id() != source_pin.assignment_id()
                || source.root_assignment_id() != source_pin.root_assignment_id()
            {
                return Err(OperationsError::Conflict(
                    "Home Source terminal differs from staged proposal lineage",
                ));
            }
        }
        None if source.outcome_kind() == HomeHandoffAcceptanceOutcomeKind::Aborted => {}
        None => {
            return Err(OperationsError::Conflict(
                "Home Applied terminal has no staged Source proposal",
            ));
        }
    }
    Ok(())
}

fn validate_applied_source_matches_admission(
    intent: &store::IntentRow,
    source: &HomeHandoffAcceptanceOutcomeRecord,
    admission: &store::AdmissionRow,
) -> Result<(), OperationsError> {
    let proposal = intent.proposal.ok_or(OperationsError::Conflict(
        "Applied Home Source lacks staged proposal",
    ))?;
    let applied = source.applied().ok_or(OperationsError::Conflict(
        "Applied Home Source payload is missing",
    ))?;
    let grant = applied.grant();
    let admitted = admission.receipt.grant();
    let source_pin = proposal.source();
    let expected_origin = AgentAssignmentHomeHandoffOrigin::new(
        proposal.request().preparation_id(),
        proposal.request().handoff_id(),
        proposal.request().predecessor_id(),
        source_pin.root_assignment_id(),
    )
    .map_err(|_| OperationsError::Corrupt("Home successor origin"))?;
    let successor = applied.successor();
    if grant != admitted
        || grant.issuer() != intent.identity.issuer()
        || grant.take_request_digest() != intent.request.request_digest()
        || grant.acceptance_receipt_id() != admission.receipt.receipt_id()
        || grant.acceptance_receipt_digest().as_bytes() != &admission.receipt.receipt_digest()
        || grant.moving_profile_id() != admission.receipt.moving_profile_id()
        || grant.recipient_profile_id() != admission.receipt.recipient_profile_id().into_uuid()
        || grant.recipient_participation().id()
            != admission.receipt.recipient_participation_id().into_uuid()
        || grant.recipient_participation().revision()
            != admission.receipt.recipient_participation_revision()
        || grant.work_authority() != admission.receipt.work_authority()
        || successor.assignment_id() != proposal.request().successor_id()
        || successor.origin() != expected_origin
        || successor.workspace_id() != source_pin.workspace_id()
        || successor.activity_id() != source_pin.activity_id()
        || successor.goal_id() != source_pin.goal_id()
        || successor.goal_created_at() != source_pin.goal_created_at()
        || successor.goal_revision() != source_pin.goal_revision()
        || successor.dag_revision() != source_pin.dag_revision()
        || successor.dag_head_revision() != source_pin.dag_head_revision()
        || successor.node_id() != source_pin.node_id()
        || successor.node_revision().get() < source_pin.node_revision().get()
        || successor.coordinator()
            != forge_domain::AgentAssignmentCoordinator::from_actor(
                proposal.request().coordinator(),
            )
        || successor.target() != intent.request.recipient()
        || successor.credit_budget() != source_pin.credit_budget()
        || successor.created_at() != source.source_at()
    {
        return Err(OperationsError::Conflict(
            "Home Applied Source grant or successor differs from exact Operations admission",
        ));
    }
    Ok(())
}

fn validate_held_owner_records(
    identity: HomeAcceptanceSourceIdentity,
    request: HomeHandoffTakeRequest,
    admission: &HomeAcceptanceAdmissionReceipt,
    session_record: &HomeAcceptanceSessionRecord,
    profile_record: &HomeAcceptanceProfilePinRecord,
    resolution: Option<&HomeAcceptanceResolutionReceipt>,
    allow_resolved: bool,
) -> Result<(), OperationsError> {
    if admission.intent().identity() != identity
        || admission.intent().request() != request
        || session_record.intent().issuer() != identity.issuer()
        || session_record.intent().request() != request
        || profile_record.issuer() != identity.issuer()
        || profile_record.request() != request
    {
        return Err(OperationsError::Conflict(
            "Home actual owner intent identity",
        ));
    }
    let session_reference = match session_record.state() {
        HomeAcceptanceSessionState::Held { .. } => session_record.held_reference(),
        HomeAcceptanceSessionState::Resolved {
            resolution: owner_resolution,
            ..
        } if allow_resolved => {
            let stored = owner_resolution.operations();
            if resolution.is_none_or(|expected| {
                stored.acceptance_id() != expected.acceptance_id()
                    || stored.outcome() != expected.outcome()
                    || stored.source_digest() != expected.source_payload_digest()
                    || stored.resolution_digest() != expected.resolution_digest()
            }) {
                return Err(OperationsError::Conflict(
                    "resolved Session record differs from exact Operations resolution",
                ));
            }
            stored.admission().map(
                heterarchy_orchestration_sessions::HomeAcceptanceOperationsAdmission::session_hold,
            )
        }
        HomeAcceptanceSessionState::Resolved { .. } => {
            return Err(OperationsError::Conflict(
                "Session was resolved before Operations recorded its terminal",
            ));
        }
    }
    .ok_or(OperationsError::Conflict(
        "Home Session hold reference is missing",
    ))?;
    let profile_pin = profile_record
        .profile_pin()
        .ok_or(OperationsError::Conflict(
            "Home Profile pin reference is missing",
        ))?;
    if !allow_resolved && profile_record.state() != HomeAcceptanceProfileState::Held {
        return Err(OperationsError::Conflict(
            "Profile was resolved before Operations recorded its terminal",
        ));
    }
    if let Some(expected) = resolution
        && matches!(
            profile_record.state(),
            HomeAcceptanceProfileState::ResolvedApplied
                | HomeAcceptanceProfileState::ResolvedAborted
        )
    {
        let profile_resolution =
            profile_record
                .operations_resolution()
                .ok_or(OperationsError::Conflict(
                    "resolved Profile record has no Operations reference",
                ))?;
        if profile_resolution.resolution_id() != expected.resolution_id()
            || profile_resolution.resolution_digest() != expected.resolution_digest()
            || profile_resolution.source_payload_digest() != expected.source_payload_digest()
            || profile_resolution.outcome() != expected.outcome()
        {
            return Err(OperationsError::Conflict(
                "resolved Profile record differs from exact Operations resolution",
            ));
        }
    }
    if HomeAcceptanceSessionHoldObservation::from_owner(session_reference)
        != admission.session_hold()
        || HomeAcceptanceProfilePinObservation::from_owner(profile_pin) != admission.profile_pin()
    {
        return Err(OperationsError::Conflict(
            "actual Session/Profile owner pins differ from admitted references",
        ));
    }
    Ok(())
}

fn validate_resolution_owner_records(
    intent: &store::IntentRow,
    admission: Option<&HomeAcceptanceAdmissionReceipt>,
    source: &HomeHandoffAcceptanceOutcomeRecord,
    session_record: Option<&HomeAcceptanceSessionRecord>,
    profile_record: Option<&HomeAcceptanceProfilePinRecord>,
    resolution: Option<&HomeAcceptanceResolutionReceipt>,
    allow_resolved: bool,
) -> Result<(), OperationsError> {
    if let Some(admission) = admission {
        let session = session_record.ok_or(OperationsError::Conflict(
            "Operations admission requires its actual Session record",
        ))?;
        let profile = profile_record.ok_or(OperationsError::Conflict(
            "Operations admission requires its actual Profile record",
        ))?;
        validate_held_owner_records(
            intent.identity,
            intent.request,
            admission,
            session,
            profile,
            resolution,
            allow_resolved,
        )?;
        return Ok(());
    }
    if source.outcome_kind() != HomeHandoffAcceptanceOutcomeKind::Aborted {
        return Err(OperationsError::Conflict(
            "Applied Home Source requires admitted owner pins",
        ));
    }
    if session_record.is_none() && profile_record.is_none() {
        return Ok(());
    }
    if let Some(session) = session_record {
        if session.intent().request() != intent.request
            || session.intent().issuer() != intent.identity.issuer()
        {
            return Err(OperationsError::Conflict(
                "Aborted Home Session record differs from exact request",
            ));
        }
        if let HomeAcceptanceSessionState::Resolved {
            resolution: owner, ..
        } = session.state()
        {
            if !allow_resolved
                || owner.outcome() != source.outcome_kind()
                || owner.source_digest() != *source.payload_digest()
                || resolution.is_none_or(|expected| {
                    owner.operations().resolution_digest() != expected.resolution_digest()
                })
            {
                return Err(OperationsError::Conflict(
                    "Aborted Home Session outcome differs from Source terminal",
                ));
            }
            if owner.operations().admission().is_some() {
                return Err(OperationsError::Conflict(
                    "Aborted Home Session has an unrecorded Operations admission",
                ));
            }
        }
    }
    if let Some(profile) = profile_record {
        if profile.request() != intent.request || profile.issuer() != intent.identity.issuer() {
            return Err(OperationsError::Conflict(
                "Aborted Home Profile record differs from exact request",
            ));
        }
        if matches!(
            profile.state(),
            HomeAcceptanceProfileState::ResolvedApplied
                | HomeAcceptanceProfileState::ResolvedAborted
        ) {
            let owner = profile
                .operations_resolution()
                .ok_or(OperationsError::Conflict(
                    "Aborted Home Profile resolution is missing",
                ))?;
            if !allow_resolved
                || owner.outcome() != source.outcome_kind()
                || owner.source_payload_digest() != *source.payload_digest()
                || resolution.is_none_or(|expected| {
                    owner.resolution_id() != expected.resolution_id()
                        || owner.resolution_digest() != expected.resolution_digest()
                })
                || owner.admission().is_some()
            {
                return Err(OperationsError::Conflict(
                    "Aborted Home Profile outcome differs from Source terminal",
                ));
            }
        }
    }
    Ok(())
}

fn validate_resolution_clock(
    resolved_at: TimestampMillis,
    admission: Option<&HomeAcceptanceAdmissionReceipt>,
) -> Result<(), OperationsError> {
    if admission.is_some_and(|value| resolved_at < value.admitted_at()) {
        return Err(OperationsError::Conflict("Home resolution clock regressed"));
    }
    Ok(())
}

fn verify_terminal_successor(
    intent: &store::IntentRow,
    source: &HomeHandoffAcceptanceOutcomeRecord,
    current: &RegisteredAgentAssignment,
) -> Result<(), OperationsError> {
    let applied = source
        .applied()
        .ok_or(OperationsError::Conflict("Home Source is not Applied"))?;
    let initial = applied.successor();
    let current_origin = match current.origin() {
        AgentAssignmentOrigin::HomeHandoff(origin) => origin,
        AgentAssignmentOrigin::CoordinatorOffer | AgentAssignmentOrigin::CutContinuation(_) => {
            return Err(OperationsError::Conflict("Home successor origin changed"));
        }
    };
    let state_is_terminal = match current.state() {
        AgentAssignmentState::Accepted | AgentAssignmentState::Rejected => {
            !current.cancellation_requested()
        }
        AgentAssignmentState::Cancelled | AgentAssignmentState::Interrupted => {
            current.cancellation_requested()
        }
        AgentAssignmentState::Offered
        | AgentAssignmentState::Running
        | AgentAssignmentState::Submitted => false,
    };
    if !state_is_terminal
        || current.id() != initial.assignment_id()
        || current_origin != initial.origin()
        || current.workspace_id() != initial.workspace_id()
        || current.activity_id() != initial.activity_id()
        || current.goal_id() != initial.goal_id()
        || current.goal_created_at() != initial.goal_created_at()
        || current.goal_revision() != initial.goal_revision()
        || current.dag_revision() != initial.dag_revision()
        || current.dag_head_revision() != initial.dag_head_revision()
        || current.node_id() != initial.node_id()
        || current.node_revision() != initial.node_revision()
        || current.coordinator() != initial.coordinator()
        || current.target() != initial.target()
        || current.credit_budget() != initial.credit_budget()
        || current.created_at() != initial.created_at()
        || current.updated_at() < current.created_at()
    {
        return Err(OperationsError::Conflict(
            "Home successor Source assignment is absent, active, or has changed lineage/scope",
        ));
    }
    if let Some(proposal) = intent.proposal
        && (current.id() != proposal.request().successor_id()
            || current.workspace_id() != proposal.source().workspace_id())
    {
        return Err(OperationsError::Conflict("Home successor intent scope"));
    }
    Ok(())
}

fn read_record_in_transaction(
    connection: &Connection,
    intent: &store::IntentRow,
) -> Result<HomeAcceptanceRecord, OperationsError> {
    let admission = store::load_admission(connection, intent, false)?;
    let resolution = store::load_resolution(connection, intent, false)?;
    let work = store::load_work_reservation(connection, intent.request.acceptance_id(), false)?;
    match (&admission, &work, &resolution) {
        (None, Some(_), _) => {
            return Err(OperationsError::Corrupt(
                "Home work reservation has no admission",
            ));
        }
        (Some(_), None, None) => {
            return Err(OperationsError::Corrupt(
                "Home admission has no Pending work reservation",
            ));
        }
        (Some(_), Some(work), None)
            if work.state() != HomeAcceptanceWorkReservationState::Pending =>
        {
            return Err(OperationsError::Corrupt(
                "unresolved Home admission is not Pending",
            ));
        }
        (Some(admission), Some(work), Some(resolution)) => {
            store::verify_work_reservation_source(
                intent,
                &admission.receipt,
                work,
                &resolution.source,
            )?;
        }
        (None, None, Some(resolution))
            if resolution.receipt.outcome() != HomeHandoffAcceptanceOutcomeKind::Aborted =>
        {
            return Err(OperationsError::Corrupt(
                "Applied Home resolution has no admission or work reservation",
            ));
        }
        (Some(_), None, Some(_)) => {
            return Err(OperationsError::Corrupt(
                "resolved Home admission has no work reservation history",
            ));
        }
        _ => {}
    }
    let intent_receipt = intent.receipt(false);
    Ok(HomeAcceptanceRecord::from_parts(
        &intent_receipt,
        admission.as_ref().map(|value| &value.receipt),
        resolution.as_ref().map(|value| &value.receipt),
        work.as_ref(),
    ))
}
