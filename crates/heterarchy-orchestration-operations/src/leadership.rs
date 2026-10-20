//! Durable leadership state machine for the Workspace Orchestrator and Team
//! Leader (per-Activity) scopes.
//!
//! Leadership is bootstrapped from first work in two explicit steps: a
//! consumed kind-3 assignment offer whose target Agent resolves to a Ready
//! logical participant seeds the matching scope(s) into `BOOTSTRAPPING` with
//! the offer recorded as pending evidence, and `confirm_bootstrap_evidence`
//! durably reads the offer projection back before the scope enters
//! `STABILIZING` for a fixed stabilization window. A failed evidence readback
//! returns the scope to `LEADERLESS` — authority is never fabricated from
//! lost evidence. After the window elapses an explicit `mark_stable` promotes
//! the leader to `ACTIVE`. A recall election may start only from `ACTIVE` (or
//! from `FAILOVER_REQUIRED` after a failover); it freezes a participation-aware
//! voter-subject electorate, gates ballots by that frozen roster, and is decided by a strict
//! majority of the FULL electorate (abstentions and non-responses count
//! against the replacement). A passed election parks the scope in
//! `AUTHORITY_PENDING` with the incumbent still leading until an explicit
//! confirmation starts a fresh stabilization window.
//!
//! Work/session handoff stays a Forge-side effect plus an Orchestration logical
//! departure. If the departing participant is the current leader, the same
//! Orchestration transaction removes that stale authority and moves the scope
//! to `FAILOVER_REQUIRED`; the receiving participant is never promoted merely
//! because work was handed off. A successor becomes authoritative only through
//! the election and Broker-fenced confirmation path. When a commit or readback
//! outcome is unknown,
//! `mark_recovery_required` parks the scope in `RECOVERY_REQUIRED` with every
//! recorded fact preserved, and `reconcile_recovery` resolves it from the
//! durable confirm-intent readback alone: committed intents restore the
//! confirmed leadership, terminal intents resolve per the recorded facts, and
//! a still-live intent keeps the scope parked with an appended observation —
//! never a competing takeover.
//!
//! Every mutation happens in one IMMEDIATE transaction and is fenced by an
//! exact compare-and-set on `leadership_revision`; the receipts table is
//! append-only and no history row is ever updated. The current leadership
//! row is the single mutable current-state row per scope. Reading is lazy:
//! `leadership_view` never mutates state and deadline enforcement happens
//! only in explicit commands.
//!
//! Confirming one elected leader is gated on a real Broker authority fence
//! (`authority.record`) and split into three phases so no SQLite transaction
//! or catalog lock is ever held across the network await:
//!
//! 1. `prepare_leadership_confirm` (IMMEDIATE writer) re-validates the
//!    leadership preconditions exactly as before and durably inserts one
//!    `ops_leadership_confirm_intents` row (`pending`).
//! 2. The caller performs the fence outside any transaction through the
//!    injected adapter (`broker_fence::BrokerAuthorityFence` or a test
//!    double), producing a `LeadershipFenceOutcome`.
//! 3. `commit_leadership_confirm` (new IMMEDIATE writer) re-validates the
//!    row, compares-and-sets `leadership_revision` R -> R+1 into
//!    `STABILIZING`, appends the `CONFIRMED` receipt, and marks the intent
//!    `committed` with the Broker-recorded revision in the SAME transaction,
//!    so a lost commit response leaves durable committed state that a
//!    restart readback observes. A definitive Broker rejection is recorded
//!    with exact-request exclusion proof. Generic conflicts stay live;
//!    noncommitted terminal intents never restore an earlier active leader.
//!
//! The durable Broker owner identity is the catalog's own destination id
//! (sealed once in `ops_catalog_identity`), so the fence identity survives
//! process restarts and lost-response readbacks.
//!
//! Qualification is exact integer math: a candidate qualifies when
//! `own * mean_subject_count * 20 >= (mean * mean_subject_count + remainder) * 7`
//! (35% of the exact scoped-subject window mean). A zero mean means no
//! verified qualification exists, so nobody qualifies (a zero mean must not
//! manufacture unverified qualification).
//!
//! Receipt `activity_id` values mirror the leadership scope for activity
//! scopes and record the bootstrap provenance activity for bootstrap
//! receipts; workspace-scope receipts without provenance store the schema's
//! `zeroblob(16)` no-activity marker, mirroring the leadership row's NULL
//! activity under its unique index. Receipt ordinals are allocated per scope
//! inside the serialized IMMEDIATE writer, so replays cannot duplicate a
//! receipt for one revision transition.

pub(crate) mod ballots;
pub(crate) mod candidates;
pub(crate) mod confirm_saga;
pub(crate) mod governance;
pub(crate) mod work_exchange;
pub(crate) mod work_roster;

use crate::{
    OperationsCatalog, OperationsError, WorkspaceParticipationId,
    activation::RegistrationActivationReceipt, catalog, effect_binding, election_subject, wire,
    work_credit, work_credit_subject,
};
use forge_domain::{ActivityId, AgentAssignmentId, AgentInstanceId, WorkspaceId};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Stabilization window length: 20 minutes in milliseconds. The window is
/// anchored by the caller-supplied clock; the operations layer owns no clock.
pub const STABILIZATION_WINDOW_MS: i64 = 1_200_000;

/// A leader heartbeat older than half the stabilization window is treated as
/// unresponsive by the deterministic leadership lifecycle advance. Missing
/// presence is not proof of staleness and never triggers failover.
pub const LEADER_UNRESPONSIVE_AFTER_MS: i64 = STABILIZATION_WINDOW_MS / 2;

/// Ballot window length: 2 minutes in milliseconds.
pub const BALLOT_WINDOW_MS: i64 = 120_000;
/// Spec 2026-09-22 #9-1: after any leadership change, re-election requests
/// stay locked for 30 minutes - identical for Team and Orchestration scopes.
pub const LEADER_CHANGE_COOLDOWN_MS: i64 = 1_800_000;
/// Spec 2026-09-22 #6: newly joined team members cannot vote for 30 minutes.
pub const NEW_MEMBER_PROBATION_MS: i64 = 1_800_000;
/// Spec 2026-09-22 #7: a member whose task fulfillment rate is at or below
/// 30% of their evaluated assignments cannot vote.
pub const VOTER_FULFILLMENT_INELIGIBLE_NUM: i64 = 3;
pub const VOTER_FULFILLMENT_INELIGIBLE_DEN: i64 = 10;

/// Exact qualification ratio 35% expressed as 7/20.
const QUALIFICATION_NUMERATOR: i64 = 7;
const QUALIFICATION_DENOMINATOR: i64 = 20;

const ELECTION_ID_DOMAIN: &[u8] = b"heterarchy-orchestration-leadership-election-id-v1";
const NO_ACTIVITY_MARKER: &[u8] = &[0_u8; 16];

const LEADERSHIP_SQL_V19: &str = "CREATE TABLE ops_leadership (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), scope_kind INTEGER NOT NULL CHECK(scope_kind IN(0,1)), activity_id BLOB CHECK(activity_id IS NULL OR length(activity_id)=16), state TEXT NOT NULL CHECK(state IN('LEADERLESS','BOOTSTRAPPING','STABILIZING','ACTIVE','ELECTING','AUTHORITY_PENDING','HANDOFF_PENDING','FAILOVER_REQUIRED','VACANT','RECOVERY_REQUIRED')), leader_participant_id BLOB CHECK(leader_participant_id IS NULL OR length(leader_participant_id)=16), leadership_revision INTEGER NOT NULL CHECK(leadership_revision>=1), stabilization_until_ms INTEGER CHECK(stabilization_until_ms IS NULL OR stabilization_until_ms>=0), current_election_id BLOB CHECK(current_election_id IS NULL OR length(current_election_id)=16), updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms>=0), CHECK((scope_kind=0 AND activity_id IS NULL) OR (scope_kind=1 AND activity_id IS NOT NULL)))";
const LEADERSHIP_SQL_V31: &str = "CREATE TABLE ops_leadership (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), scope_kind INTEGER NOT NULL CHECK(scope_kind IN(0,1)), activity_id BLOB CHECK(activity_id IS NULL OR length(activity_id)=16), state TEXT NOT NULL CHECK(state IN('LEADERLESS','BOOTSTRAPPING','STABILIZING','ACTIVE','ELECTING','AUTHORITY_PENDING','FAILOVER_REQUIRED','VACANT','RECOVERY_REQUIRED')), leader_participant_id BLOB CHECK(leader_participant_id IS NULL OR length(leader_participant_id)=16), leadership_revision INTEGER NOT NULL CHECK(leadership_revision>=1), stabilization_until_ms INTEGER CHECK(stabilization_until_ms IS NULL OR stabilization_until_ms>=0), current_election_id BLOB CHECK(current_election_id IS NULL OR length(current_election_id)=16), updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms>=0), CHECK((scope_kind=0 AND activity_id IS NULL) OR (scope_kind=1 AND activity_id IS NOT NULL)))";
const LEADERSHIP_SQL: &str = "CREATE TABLE ops_leadership (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), scope_kind INTEGER NOT NULL CHECK(scope_kind IN(0,1,2)), activity_id BLOB CHECK(activity_id IS NULL OR length(activity_id)=16), team_id BLOB CHECK(team_id IS NULL OR length(team_id)=16), state TEXT NOT NULL CHECK(state IN('LEADERLESS','BOOTSTRAPPING','STABILIZING','ACTIVE','ELECTING','AUTHORITY_PENDING','FAILOVER_REQUIRED','VACANT','RECOVERY_REQUIRED')), leader_participant_id BLOB CHECK(leader_participant_id IS NULL OR length(leader_participant_id)=16), leadership_revision INTEGER NOT NULL CHECK(leadership_revision>=1), stabilization_until_ms INTEGER CHECK(stabilization_until_ms IS NULL OR stabilization_until_ms>=0), current_election_id BLOB CHECK(current_election_id IS NULL OR length(current_election_id)=16), updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms>=0), CHECK((scope_kind=0 AND activity_id IS NULL AND team_id IS NULL) OR (scope_kind=1 AND activity_id IS NOT NULL AND team_id IS NULL) OR (scope_kind=2 AND activity_id IS NULL AND team_id IS NOT NULL)))";
const LEADERSHIP_SCOPE_INDEX_V31: &str = "CREATE UNIQUE INDEX ops_leadership_scope_idx ON ops_leadership(workspace_id, scope_kind, COALESCE(activity_id, zeroblob(16)))";
const LEADERSHIP_SCOPE_INDEX: &str = "CREATE UNIQUE INDEX ops_leadership_scope_idx ON ops_leadership(workspace_id, scope_kind, COALESCE(activity_id, zeroblob(16)), COALESCE(team_id, zeroblob(16)))";
const ELECTIONS_SQL_V31: &str = "CREATE TABLE ops_leadership_elections (election_id BLOB PRIMARY KEY NOT NULL CHECK(length(election_id)=16), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), scope_kind INTEGER NOT NULL CHECK(scope_kind IN(0,1)), activity_id BLOB CHECK(activity_id IS NULL OR length(activity_id)=16), kind TEXT NOT NULL CHECK(kind IN('RECALL')), state TEXT NOT NULL CHECK(state IN('ELECTING','CLOSED_ELECTED','CLOSED_NO_MAJORITY','CANCELLED')), incumbent_participant_id BLOB CHECK(incumbent_participant_id IS NULL OR length(incumbent_participant_id)=16), candidate_participant_id BLOB NOT NULL CHECK(length(candidate_participant_id)=16), ballot_deadline_ms INTEGER NOT NULL CHECK(ballot_deadline_ms>=0), elected_participant_id BLOB CHECK(elected_participant_id IS NULL OR length(elected_participant_id)=16), tally_yes INTEGER CHECK(tally_yes IS NULL OR tally_yes>=0), tally_no INTEGER CHECK(tally_no IS NULL OR tally_no>=0), tally_abstain INTEGER CHECK(tally_abstain IS NULL OR tally_abstain>=0), created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0), closed_at_ms INTEGER CHECK(closed_at_ms IS NULL OR closed_at_ms>=0), CHECK((scope_kind=0 AND activity_id IS NULL) OR (scope_kind=1 AND activity_id IS NOT NULL)), CHECK(state!='ELECTING' OR (closed_at_ms IS NULL AND elected_participant_id IS NULL AND tally_yes IS NULL AND tally_no IS NULL AND tally_abstain IS NULL)), CHECK(state!='CLOSED_ELECTED' OR (elected_participant_id IS NOT NULL AND tally_yes IS NOT NULL AND tally_no IS NOT NULL AND tally_abstain IS NOT NULL)), CHECK(state!='CLOSED_NO_MAJORITY' OR (elected_participant_id IS NULL AND tally_yes IS NOT NULL AND tally_no IS NOT NULL AND tally_abstain IS NOT NULL)))";
const ELECTIONS_SQL: &str = "CREATE TABLE ops_leadership_elections (election_id BLOB PRIMARY KEY NOT NULL CHECK(length(election_id)=16), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), scope_kind INTEGER NOT NULL CHECK(scope_kind IN(0,1,2)), activity_id BLOB CHECK(activity_id IS NULL OR length(activity_id)=16), team_id BLOB CHECK(team_id IS NULL OR length(team_id)=16), kind TEXT NOT NULL CHECK(kind IN('RECALL')), state TEXT NOT NULL CHECK(state IN('ELECTING','CLOSED_ELECTED','CLOSED_NO_MAJORITY','CANCELLED')), incumbent_participant_id BLOB CHECK(incumbent_participant_id IS NULL OR length(incumbent_participant_id)=16), candidate_participant_id BLOB NOT NULL CHECK(length(candidate_participant_id)=16), ballot_deadline_ms INTEGER NOT NULL CHECK(ballot_deadline_ms>=0), elected_participant_id BLOB CHECK(elected_participant_id IS NULL OR length(elected_participant_id)=16), tally_yes INTEGER CHECK(tally_yes IS NULL OR tally_yes>=0), tally_no INTEGER CHECK(tally_no IS NULL OR tally_no>=0), tally_abstain INTEGER CHECK(tally_abstain IS NULL OR tally_abstain>=0), created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0), closed_at_ms INTEGER CHECK(closed_at_ms IS NULL OR closed_at_ms>=0), CHECK((scope_kind=0 AND activity_id IS NULL AND team_id IS NULL) OR (scope_kind=1 AND activity_id IS NOT NULL AND team_id IS NULL) OR (scope_kind=2 AND activity_id IS NULL AND team_id IS NOT NULL)), CHECK(state!='ELECTING' OR (closed_at_ms IS NULL AND elected_participant_id IS NULL AND tally_yes IS NULL AND tally_no IS NULL AND tally_abstain IS NULL)), CHECK(state!='CLOSED_ELECTED' OR (elected_participant_id IS NOT NULL AND tally_yes IS NOT NULL AND tally_no IS NOT NULL AND tally_abstain IS NOT NULL)), CHECK(state!='CLOSED_NO_MAJORITY' OR (elected_participant_id IS NULL AND tally_yes IS NOT NULL AND tally_no IS NOT NULL AND tally_abstain IS NOT NULL)))";
const ELECTORATE_SQL: &str = "CREATE TABLE ops_election_electorate (election_id BLOB NOT NULL REFERENCES ops_leadership_elections(election_id) CHECK(length(election_id)=16), participant_id BLOB NOT NULL CHECK(length(participant_id)=16 AND participant_id != zeroblob(16)), PRIMARY KEY(election_id,participant_id))";
const BALLOTS_SQL: &str = "CREATE TABLE ops_ballots (election_id BLOB NOT NULL REFERENCES ops_leadership_elections(election_id) CHECK(length(election_id)=16), voter_participant_id BLOB NOT NULL CHECK(length(voter_participant_id)=16 AND voter_participant_id != zeroblob(16)), choice TEXT NOT NULL CHECK(choice IN('YES','NO','ABSTAIN')), ballot_revision INTEGER NOT NULL CHECK(ballot_revision>=1), cast_at_ms INTEGER NOT NULL CHECK(cast_at_ms>=0), PRIMARY KEY(election_id,voter_participant_id))";
/// Receipt vocabulary as it existed from schema version 14 through 18, before
/// the handoff and recovery receipts were added.
const RECEIPTS_SQL_V18: &str = "CREATE TABLE ops_leadership_receipts (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), scope_kind INTEGER NOT NULL CHECK(scope_kind IN(0,1)), activity_id BLOB NOT NULL CHECK(length(activity_id)=16), receipt_ordinal INTEGER NOT NULL CHECK(receipt_ordinal>=1), receipt_kind TEXT NOT NULL CHECK(receipt_kind IN('BOOTSTRAP','ELECTED','CONFIRMED','VACATED','FAILOVER')), work_assignment_id BLOB CHECK(work_assignment_id IS NULL OR length(work_assignment_id)=16), election_id BLOB CHECK(election_id IS NULL OR length(election_id)=16), participant_id BLOB CHECK(participant_id IS NULL OR length(participant_id)=16), leadership_revision INTEGER NOT NULL CHECK(leadership_revision>=1), recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0), PRIMARY KEY(workspace_id,scope_kind,activity_id,receipt_ordinal))";
const RECEIPTS_SQL_V31: &str = "CREATE TABLE ops_leadership_receipts (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), scope_kind INTEGER NOT NULL CHECK(scope_kind IN(0,1)), activity_id BLOB NOT NULL CHECK(length(activity_id)=16), receipt_ordinal INTEGER NOT NULL CHECK(receipt_ordinal>=1), receipt_kind TEXT NOT NULL CHECK(receipt_kind IN('BOOTSTRAP','ELECTED','CONFIRMED','VACATED','FAILOVER','HANDOFF','RECOVERY')), work_assignment_id BLOB CHECK(work_assignment_id IS NULL OR length(work_assignment_id)=16), election_id BLOB CHECK(election_id IS NULL OR length(election_id)=16), participant_id BLOB CHECK(participant_id IS NULL OR length(participant_id)=16), leadership_revision INTEGER NOT NULL CHECK(leadership_revision>=1), recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0), PRIMARY KEY(workspace_id,scope_kind,activity_id,receipt_ordinal))";
const RECEIPTS_SQL: &str = "CREATE TABLE ops_leadership_receipts (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), scope_kind INTEGER NOT NULL CHECK(scope_kind IN(0,1,2)), activity_id BLOB NOT NULL CHECK(length(activity_id)=16), team_id BLOB NOT NULL CHECK(length(team_id)=16), receipt_ordinal INTEGER NOT NULL CHECK(receipt_ordinal>=1), receipt_kind TEXT NOT NULL CHECK(receipt_kind IN('BOOTSTRAP','ELECTED','CONFIRMED','VACATED','FAILOVER','HANDOFF','RECOVERY')), work_assignment_id BLOB CHECK(work_assignment_id IS NULL OR length(work_assignment_id)=16), election_id BLOB CHECK(election_id IS NULL OR length(election_id)=16), participant_id BLOB CHECK(participant_id IS NULL OR length(participant_id)=16), leadership_revision INTEGER NOT NULL CHECK(leadership_revision>=1), recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms>=0), PRIMARY KEY(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal))";
const CONFIRM_INTENTS_SQL_V31: &str = "CREATE TABLE ops_leadership_confirm_intents (request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16), kind TEXT NOT NULL CHECK(kind='election_confirm'), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), scope_kind INTEGER NOT NULL CHECK(scope_kind IN(0,1)), activity_id BLOB CHECK(activity_id IS NULL OR length(activity_id)=16), election_id BLOB NOT NULL CHECK(length(election_id)=16), elected_participant_id BLOB NOT NULL CHECK(length(elected_participant_id)=16), expected_local_revision INTEGER NOT NULL CHECK(expected_local_revision>=1), requested_fence_revision INTEGER NOT NULL CHECK(requested_fence_revision=expected_local_revision), owner_instance_id BLOB NOT NULL CHECK(length(owner_instance_id)=16), remote_recorded_revision INTEGER CHECK(remote_recorded_revision IS NULL OR remote_recorded_revision>=1), confirmed_at_ms INTEGER CHECK(confirmed_at_ms IS NULL OR confirmed_at_ms>=0), state TEXT NOT NULL CHECK(state IN('pending','remote_recorded','committed','superseded','rejected')), created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0), resolved_at_ms INTEGER CHECK(resolved_at_ms IS NULL OR resolved_at_ms>=0), CHECK((scope_kind=0 AND activity_id IS NULL) OR (scope_kind=1 AND activity_id IS NOT NULL)))";
const CONFIRM_INTENTS_SQL: &str = "CREATE TABLE ops_leadership_confirm_intents (request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16), kind TEXT NOT NULL CHECK(kind='election_confirm'), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), scope_kind INTEGER NOT NULL CHECK(scope_kind IN(0,1,2)), activity_id BLOB CHECK(activity_id IS NULL OR length(activity_id)=16), team_id BLOB CHECK(team_id IS NULL OR length(team_id)=16), election_id BLOB NOT NULL CHECK(length(election_id)=16), elected_participant_id BLOB NOT NULL CHECK(length(elected_participant_id)=16), expected_local_revision INTEGER NOT NULL CHECK(expected_local_revision>=1), requested_fence_revision INTEGER NOT NULL CHECK(requested_fence_revision=expected_local_revision), owner_instance_id BLOB NOT NULL CHECK(length(owner_instance_id)=16), remote_recorded_revision INTEGER CHECK(remote_recorded_revision IS NULL OR remote_recorded_revision>=1), confirmed_at_ms INTEGER CHECK(confirmed_at_ms IS NULL OR confirmed_at_ms>=0), state TEXT NOT NULL CHECK(state IN('pending','remote_recorded','committed','superseded','rejected')), created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0), resolved_at_ms INTEGER CHECK(resolved_at_ms IS NULL OR resolved_at_ms>=0), CHECK((scope_kind=0 AND activity_id IS NULL AND team_id IS NULL) OR (scope_kind=1 AND activity_id IS NOT NULL AND team_id IS NULL) OR (scope_kind=2 AND activity_id IS NULL AND team_id IS NOT NULL)))";

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("table", "ops_leadership", LEADERSHIP_SQL),
        ("index", "ops_leadership_scope_idx", LEADERSHIP_SCOPE_INDEX),
        ("table", "ops_leadership_elections", ELECTIONS_SQL),
        ("table", "ops_election_electorate", ELECTORATE_SQL),
        ("table", "ops_ballots", BALLOTS_SQL),
        ("table", "ops_leadership_receipts", RECEIPTS_SQL),
        (
            "table",
            "ops_leadership_confirm_intents",
            CONFIRM_INTENTS_SQL,
        ),
    ]
}

/// Exact leadership schema objects at catalog version 31.  Historical
/// validators and the v31 -> v32 migration use this immutable DDL; it must not
/// drift when the current Team-aware schema evolves.
pub(crate) fn schema_objects_v31() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("table", "ops_leadership", LEADERSHIP_SQL_V31),
        (
            "index",
            "ops_leadership_scope_idx",
            LEADERSHIP_SCOPE_INDEX_V31,
        ),
        ("table", "ops_leadership_elections", ELECTIONS_SQL_V31),
        ("table", "ops_election_electorate", ELECTORATE_SQL),
        ("table", "ops_ballots", BALLOTS_SQL),
        ("table", "ops_leadership_receipts", RECEIPTS_SQL_V31),
        (
            "table",
            "ops_leadership_confirm_intents",
            CONFIRM_INTENTS_SQL_V31,
        ),
    ]
}

/// Leadership schema objects as they existed at catalog version 14, before
/// leadership confirmation gained its durable intent journal.
pub(crate) fn schema_objects_v14() -> Vec<(&'static str, &'static str, String)> {
    vec![
        ("table", "ops_leadership", LEADERSHIP_SQL_V19.to_owned()),
        (
            "index",
            "ops_leadership_scope_idx",
            LEADERSHIP_SCOPE_INDEX_V31.to_owned(),
        ),
        (
            "table",
            "ops_leadership_elections",
            ELECTIONS_SQL_V31.to_owned(),
        ),
        (
            "table",
            "ops_election_electorate",
            ELECTORATE_SQL.to_owned(),
        ),
        ("table", "ops_ballots", BALLOTS_SQL.to_owned()),
        (
            "table",
            "ops_leadership_receipts",
            RECEIPTS_SQL_V18.to_owned(),
        ),
    ]
}

/// Leadership schema objects as they existed at catalog version 18: the
/// confirm-intent journal was present, but the receipt vocabulary still
/// predated the handoff and recovery receipts.
pub(crate) fn schema_objects_v18() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = schema_objects_v14();
    objects.push((
        "table",
        "ops_leadership_confirm_intents",
        CONFIRM_INTENTS_SQL_V31.to_owned(),
    ));
    objects
}

/// Exact leadership schema at catalog version 19, before the unused
/// `HANDOFF_PENDING` state was retired. Receipt history already used the
/// extended vocabulary at this version.
pub(crate) fn schema_objects_v19() -> Vec<(&'static str, &'static str, String)> {
    vec![
        ("table", "ops_leadership", LEADERSHIP_SQL_V19.to_owned()),
        (
            "index",
            "ops_leadership_scope_idx",
            LEADERSHIP_SCOPE_INDEX_V31.to_owned(),
        ),
        (
            "table",
            "ops_leadership_elections",
            ELECTIONS_SQL_V31.to_owned(),
        ),
        (
            "table",
            "ops_election_electorate",
            ELECTORATE_SQL.to_owned(),
        ),
        ("table", "ops_ballots", BALLOTS_SQL.to_owned()),
        (
            "table",
            "ops_leadership_receipts",
            RECEIPTS_SQL_V31.to_owned(),
        ),
        (
            "table",
            "ops_leadership_confirm_intents",
            CONFIRM_INTENTS_SQL_V31.to_owned(),
        ),
    ]
}

/// Additive v18 -> v19 upgrade: rebuild the receipts table with the extended
/// receipt vocabulary, preserving every existing history row. No other table
/// references the receipts table, so the rebuild is side-effect free.
pub(crate) fn rebuild_receipts_v19(tx: &Transaction<'_>) -> Result<(), OperationsError> {
    let upgraded = RECEIPTS_SQL_V31.replacen(
        "CREATE TABLE ops_leadership_receipts ",
        "CREATE TABLE ops_leadership_receipts_v19 ",
        1,
    );
    tx.execute_batch(&upgraded)?;
    tx.execute(
        "INSERT INTO ops_leadership_receipts_v19 SELECT * FROM ops_leadership_receipts",
        [],
    )?;
    tx.execute("DROP TABLE ops_leadership_receipts", [])?;
    tx.execute_batch("ALTER TABLE ops_leadership_receipts_v19 RENAME TO ops_leadership_receipts")?;
    Ok(())
}

/// v19 -> v20 retirement of the unused leadership handoff phase. Historical
/// `HANDOFF` receipts remain readable evidence, but a live `HANDOFF_PENDING` row
/// is fail-closed into `FAILOVER_REQUIRED` with no current leader. The revision
/// advances and an explicit FAILOVER receipt records the migration so stale
/// callers cannot retain pre-migration authority.
pub(crate) fn rebuild_leadership_v20(tx: &Transaction<'_>) -> Result<(), OperationsError> {
    let overflow: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_leadership WHERE state='HANDOFF_PENDING' AND leadership_revision>=?1)",
        params![i64::MAX],
        |row| row.get(0),
    )?;
    if overflow {
        return Err(OperationsError::Bounds("leadership migration revision"));
    }
    tx.execute(
        "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,receipt_ordinal,receipt_kind,work_assignment_id,election_id,participant_id,leadership_revision,recorded_at_ms) SELECT l.workspace_id,l.scope_kind,COALESCE(l.activity_id,zeroblob(16)),(SELECT COALESCE(MAX(r.receipt_ordinal),0)+1 FROM ops_leadership_receipts r WHERE r.workspace_id=l.workspace_id AND r.scope_kind=l.scope_kind AND r.activity_id IS COALESCE(l.activity_id,zeroblob(16))),'FAILOVER',NULL,NULL,l.leader_participant_id,l.leadership_revision+1,l.updated_at_ms FROM ops_leadership l WHERE l.state='HANDOFF_PENDING'",
        [],
    )?;
    let upgraded = LEADERSHIP_SQL_V31.replacen(
        "CREATE TABLE ops_leadership ",
        "CREATE TABLE ops_leadership_v20 ",
        1,
    );
    tx.execute_batch(&upgraded)?;
    tx.execute(
        "INSERT INTO ops_leadership_v20(workspace_id,scope_kind,activity_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) SELECT workspace_id,scope_kind,activity_id,CASE WHEN state='HANDOFF_PENDING' THEN 'FAILOVER_REQUIRED' ELSE state END,CASE WHEN state='HANDOFF_PENDING' THEN NULL ELSE leader_participant_id END,leadership_revision+CASE WHEN state='HANDOFF_PENDING' THEN 1 ELSE 0 END,CASE WHEN state='HANDOFF_PENDING' THEN NULL ELSE stabilization_until_ms END,CASE WHEN state='HANDOFF_PENDING' THEN NULL ELSE current_election_id END,updated_at_ms FROM ops_leadership",
        [],
    )?;
    tx.execute("DROP TABLE ops_leadership", [])?;
    tx.execute_batch(LEADERSHIP_SQL_V31)?;
    tx.execute(
        "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) SELECT workspace_id,scope_kind,activity_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms FROM ops_leadership_v20",
        [],
    )?;
    tx.execute("DROP TABLE ops_leadership_v20", [])?;
    tx.execute_batch(LEADERSHIP_SCOPE_INDEX_V31)?;
    Ok(())
}

/// Reconcile scope-local elections before the v35 catalog cutover. The
/// caller owns the surrounding IMMEDIATE migration transaction.
///
/// # Errors
/// Rejects corrupt election/scope links, committed intents that still point
/// at an unresolved election, revision overflow, and storage failures.
pub(crate) fn reconcile_legacy_leadership_elections_for_v35(
    tx: &Transaction<'_>,
) -> Result<(), OperationsError> {
    const PAGE_LIMIT: i64 = 64;
    let mut after: Option<Vec<u8>> = None;
    loop {
        let candidates = legacy_election_page_v35(tx, after.as_deref(), PAGE_LIMIT)?;
        let Some((last_id, _, _)) = candidates.last() else {
            return Ok(());
        };
        after = Some(last_id.clone());
        for (raw_id, created_at_ms, closed_at_ms) in candidates {
            let election_id = Uuid::from_bytes(wire::id(raw_id)?);
            reconcile_legacy_election_v35(tx, election_id, created_at_ms, closed_at_ms)?;
        }
    }
}

type LegacyElectionMigrationRow = (Vec<u8>, i64, Option<i64>);

fn legacy_election_page_v35(
    tx: &Transaction<'_>,
    after: Option<&[u8]>,
    limit: i64,
) -> Result<Vec<LegacyElectionMigrationRow>, OperationsError> {
    let mut query = tx.prepare(
        "SELECT substr(e.election_id,1,17),e.created_at_ms,e.closed_at_ms
         FROM ops_leadership_elections e
         WHERE e.scope_kind IN(0,2) AND (?1 IS NULL OR e.election_id>?1)
           AND (e.state='ELECTING' OR (e.state='CLOSED_ELECTED' AND EXISTS(
             SELECT 1 FROM ops_leadership l
             WHERE l.workspace_id=e.workspace_id AND l.scope_kind=e.scope_kind
               AND l.activity_id IS e.activity_id AND l.team_id IS e.team_id
               AND l.current_election_id=e.election_id
               AND l.state IN('AUTHORITY_PENDING','RECOVERY_REQUIRED')
           )))
         ORDER BY e.election_id LIMIT ?2",
    )?;
    query
        .query_map(params![after, limit], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Option<i64>>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(OperationsError::from)
}

fn reconcile_legacy_election_v35(
    tx: &Transaction<'_>,
    election_id: Uuid,
    created_at_ms: i64,
    closed_at_ms: Option<i64>,
) -> Result<(), OperationsError> {
    let election = load_election(tx, election_id)?
        .ok_or(OperationsError::Corrupt("leadership migration election"))?;
    let row = load_leadership(tx, election.scope)?;
    let pointer_owns_election = row
        .as_ref()
        .is_some_and(|row| row.current_election_id == Some(election_id));
    let recoverable_pointer = row.as_ref().is_some_and(|row| {
        pointer_owns_election
            && matches!(
                row.state,
                LeadershipState::AuthorityPending | LeadershipState::RecoveryRequired
            )
    });

    match election.state {
        ElectionState::Electing => {}
        ElectionState::ClosedElected if recoverable_pointer => {}
        ElectionState::ClosedElected
        | ElectionState::ClosedNoMajority
        | ElectionState::Cancelled => return Ok(()),
    }

    let transition_at_ms = row.as_ref().map_or(created_at_ms, |row| {
        if election.state == ElectionState::Electing {
            created_at_ms.max(row.updated_at_ms)
        } else {
            row.updated_at_ms.max(closed_at_ms.unwrap_or(created_at_ms))
        }
    });
    let cancelled_closed_at_ms =
        (election.state == ElectionState::Electing).then_some(transition_at_ms);

    if pointer_owns_election && committed_confirm_intent_exists(tx, election.scope, election_id)? {
        return Err(OperationsError::Corrupt(
            "committed leadership intent has unresolved election pointer",
        ));
    }

    cancel_legacy_election_v35(tx, election_id, election.state, cancelled_closed_at_ms)?;
    if pointer_owns_election {
        let row = row
            .as_ref()
            .ok_or(OperationsError::Corrupt("leadership migration pointer"))?;
        reconcile_cancelled_election_pointer_v35(
            tx,
            election.scope,
            election_id,
            row,
            transition_at_ms,
        )?;
    }
    Ok(())
}

fn cancel_legacy_election_v35(
    tx: &Transaction<'_>,
    election_id: Uuid,
    state: ElectionState,
    closed_at_ms: Option<i64>,
) -> Result<(), OperationsError> {
    let changed = match state {
        ElectionState::Electing => tx.execute(
            "UPDATE ops_leadership_elections SET state='CANCELLED',closed_at_ms=?2 WHERE election_id=?1 AND state='ELECTING'",
            params![election_id.as_bytes(), closed_at_ms],
        )?,
        ElectionState::ClosedElected => tx.execute(
            "UPDATE ops_leadership_elections SET state='CANCELLED' WHERE election_id=?1 AND state='CLOSED_ELECTED'",
            params![election_id.as_bytes()],
        )?,
        ElectionState::ClosedNoMajority | ElectionState::Cancelled => return Ok(()),
    };
    if changed != 1 {
        return Err(OperationsError::Corrupt(
            "leadership migration election state",
        ));
    }
    Ok(())
}

fn reconcile_cancelled_election_pointer_v35(
    tx: &Transaction<'_>,
    scope: LeadershipScope,
    election_id: Uuid,
    row: &LeadershipRow,
    now_ms: i64,
) -> Result<(), OperationsError> {
    let has_live_intent = has_live_confirm_intent(tx, scope, None)?;
    let (state, current_election_id, receipt_kind) = if has_live_intent {
        (
            LeadershipState::RecoveryRequired,
            Some(election_id),
            ReceiptKind::Recovery,
        )
    } else if row.leader_participant_id.is_some() {
        (
            LeadershipState::FailoverRequired,
            None,
            ReceiptKind::Failover,
        )
    } else {
        (LeadershipState::Vacant, None, ReceiptKind::Vacated)
    };
    let updated = cas_leadership(
        tx,
        scope,
        row.leadership_revision,
        LeadershipUpdate {
            state,
            leader_participant_id: row.leader_participant_id,
            stabilization_until_ms: None,
            current_election_id,
            now_ms,
        },
    )?
    .ok_or(OperationsError::Conflict("leadership migration revision"))?;
    append_receipt(
        tx,
        scope,
        &ReceiptRow {
            kind: receipt_kind,
            provenance_activity: None,
            work_assignment_id: None,
            election_id: Some(election_id),
            participant_id: row.leader_participant_id,
            leadership_revision: updated.leadership_revision,
            recorded_at_ms: now_ms,
        },
    )
}

fn committed_confirm_intent_exists(
    tx: &Transaction<'_>,
    scope: LeadershipScope,
    election_id: Uuid,
) -> Result<bool, OperationsError> {
    tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_leadership_confirm_intents
         WHERE workspace_id=?1 AND scope_kind=?2 AND activity_id IS ?3 AND team_id IS ?4
           AND election_id=?5 AND state='committed')",
        params![
            scope.workspace_id().into_uuid().as_bytes(),
            scope.scope_kind(),
            election_activity_param(scope),
            election_team_param(scope),
            election_id.as_bytes(),
        ],
        |row| row.get(0),
    )
    .map_err(OperationsError::from)
}
/// Rebuild the Team-aware leadership tables from catalog v31. The catalog
/// migration owner temporarily disables foreign-key enforcement outside its
/// transaction while this helper replaces the parent election table; it runs
/// `foreign_key_check` before committing and restores enforcement on every
/// path. This helper itself never toggles that connection-wide pragma.
pub(crate) fn migrate_v31_to_v32(tx: &Transaction<'_>) -> Result<(), OperationsError> {
    if !table_has_column(tx, "ops_leadership", "team_id")? {
        rebuild_scope_table(
            tx,
            LEADERSHIP_SQL,
            "ops_leadership",
            "workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms",
            "workspace_id,scope_kind,activity_id,NULL,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms",
        )?;
    }
    if !table_has_column(tx, "ops_leadership_elections", "team_id")? {
        rebuild_scope_table(
            tx,
            ELECTIONS_SQL,
            "ops_leadership_elections",
            "election_id,workspace_id,scope_kind,activity_id,team_id,kind,state,incumbent_participant_id,candidate_participant_id,ballot_deadline_ms,elected_participant_id,tally_yes,tally_no,tally_abstain,created_at_ms,closed_at_ms",
            "election_id,workspace_id,scope_kind,activity_id,NULL,kind,state,incumbent_participant_id,candidate_participant_id,ballot_deadline_ms,elected_participant_id,tally_yes,tally_no,tally_abstain,created_at_ms,closed_at_ms",
        )?;
    }
    if !table_has_column(tx, "ops_leadership_receipts", "team_id")? {
        rebuild_scope_table(
            tx,
            RECEIPTS_SQL,
            "ops_leadership_receipts",
            "workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,work_assignment_id,election_id,participant_id,leadership_revision,recorded_at_ms",
            "workspace_id,scope_kind,activity_id,zeroblob(16),receipt_ordinal,receipt_kind,work_assignment_id,election_id,participant_id,leadership_revision,recorded_at_ms",
        )?;
    }
    if !table_has_column(tx, "ops_leadership_confirm_intents", "team_id")? {
        rebuild_scope_table(
            tx,
            CONFIRM_INTENTS_SQL,
            "ops_leadership_confirm_intents",
            "request_id,kind,workspace_id,scope_kind,activity_id,team_id,election_id,elected_participant_id,expected_local_revision,requested_fence_revision,owner_instance_id,remote_recorded_revision,confirmed_at_ms,state,created_at_ms,resolved_at_ms",
            "request_id,kind,workspace_id,scope_kind,activity_id,NULL,election_id,elected_participant_id,expected_local_revision,requested_fence_revision,owner_instance_id,remote_recorded_revision,confirmed_at_ms,state,created_at_ms,resolved_at_ms",
        )?;
    }
    tx.execute_batch("DROP INDEX IF EXISTS ops_leadership_scope_idx")?;
    tx.execute_batch(LEADERSHIP_SCOPE_INDEX)?;
    Ok(())
}

fn rebuild_scope_table(
    tx: &Transaction<'_>,
    schema: &str,
    table: &str,
    target_columns: &str,
    source_columns: &str,
) -> Result<(), OperationsError> {
    tx.execute_batch(&schema.replacen(
        &format!("CREATE TABLE {table} "),
        &format!("CREATE TABLE {table}_v32 "),
        1,
    ))?;
    tx.execute(
        &format!("INSERT INTO {table}_v32({target_columns}) SELECT {source_columns} FROM {table}"),
        [],
    )?;
    if table == "ops_leadership" {
        tx.execute_batch("DROP INDEX IF EXISTS ops_leadership_scope_idx")?;
    }
    tx.execute(&format!("DROP TABLE {table}"), [])?;
    tx.execute_batch(&format!("ALTER TABLE {table}_v32 RENAME TO {table}"))?;
    Ok(())
}

fn table_has_column(
    connection: &Connection,
    table: &str,
    column: &str,
) -> Result<bool, OperationsError> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        if name == column {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in schema_objects() {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

/// Additive v14 -> v15 upgrade: create only the confirm-intent journal.
pub(crate) fn create_confirm_intents_schema(
    connection: &Connection,
) -> Result<(), OperationsError> {
    connection.execute_batch(CONFIRM_INTENTS_SQL_V31)?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeadershipScopeTarget {
    Workspace,
    Activity(ActivityId),
    Team(Uuid),
}

/// Durable leadership scope: the Workspace Orchestrator (`workspace`), one
/// exact Activity (`activity`), or one exact Team (`team`). The private target
/// enum keeps the mutually exclusive identity columns explicit at the type
/// boundary; a Team UUID is never represented as an Activity UUID.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeadershipScope {
    workspace_id: WorkspaceId,
    target: LeadershipScopeTarget,
}

impl LeadershipScope {
    pub const fn workspace(workspace_id: WorkspaceId) -> Self {
        Self {
            workspace_id,
            target: LeadershipScopeTarget::Workspace,
        }
    }

    pub const fn activity(workspace_id: WorkspaceId, activity_id: ActivityId) -> Self {
        Self {
            workspace_id,
            target: LeadershipScopeTarget::Activity(activity_id),
        }
    }

    pub const fn team(workspace_id: WorkspaceId, team_id: Uuid) -> Self {
        Self {
            workspace_id,
            target: LeadershipScopeTarget::Team(team_id),
        }
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn activity_id(&self) -> Option<ActivityId> {
        match self.target {
            LeadershipScopeTarget::Activity(activity_id) => Some(activity_id),
            LeadershipScopeTarget::Workspace | LeadershipScopeTarget::Team(_) => None,
        }
    }

    #[must_use]
    pub const fn team_id(&self) -> Option<Uuid> {
        match self.target {
            LeadershipScopeTarget::Team(team_id) => Some(team_id),
            LeadershipScopeTarget::Workspace | LeadershipScopeTarget::Activity(_) => None,
        }
    }

    const fn scope_kind(self) -> i64 {
        match self.target {
            LeadershipScopeTarget::Workspace => 0,
            LeadershipScopeTarget::Activity(_) => 1,
            LeadershipScopeTarget::Team(_) => 2,
        }
    }
}

/// Closed leadership lifecycle states. Every state is written by a real
/// transition: `BOOTSTRAPPING` parks a seeded scope until its offer evidence
/// is durably confirmed, and `RECOVERY_REQUIRED` parks a scope whose
/// commit/readback outcome must be reconciled from durable facts
/// before any further authority mutation.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeadershipState {
    Leaderless,
    Bootstrapping,
    Stabilizing,
    Active,
    Electing,
    AuthorityPending,
    FailoverRequired,
    Vacant,
    RecoveryRequired,
}

impl LeadershipState {
    const fn encode(self) -> &'static str {
        match self {
            Self::Leaderless => "LEADERLESS",
            Self::Bootstrapping => "BOOTSTRAPPING",
            Self::Stabilizing => "STABILIZING",
            Self::Active => "ACTIVE",
            Self::Electing => "ELECTING",
            Self::AuthorityPending => "AUTHORITY_PENDING",
            Self::FailoverRequired => "FAILOVER_REQUIRED",
            Self::Vacant => "VACANT",
            Self::RecoveryRequired => "RECOVERY_REQUIRED",
        }
    }

    fn decode(value: &str) -> Result<Self, OperationsError> {
        match value {
            "LEADERLESS" => Ok(Self::Leaderless),
            "BOOTSTRAPPING" => Ok(Self::Bootstrapping),
            "STABILIZING" => Ok(Self::Stabilizing),
            "ACTIVE" => Ok(Self::Active),
            "ELECTING" => Ok(Self::Electing),
            "AUTHORITY_PENDING" => Ok(Self::AuthorityPending),
            "FAILOVER_REQUIRED" => Ok(Self::FailoverRequired),
            "VACANT" => Ok(Self::Vacant),
            "RECOVERY_REQUIRED" => Ok(Self::RecoveryRequired),
            _ => Err(OperationsError::Corrupt("leadership state")),
        }
    }
}

/// One recall ballot choice.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BallotChoice {
    Yes,
    No,
    Abstain,
}

impl BallotChoice {
    const fn encode(self) -> &'static str {
        match self {
            Self::Yes => "YES",
            Self::No => "NO",
            Self::Abstain => "ABSTAIN",
        }
    }

    fn decode(value: &str) -> Result<Self, OperationsError> {
        match value {
            "YES" => Ok(Self::Yes),
            "NO" => Ok(Self::No),
            "ABSTAIN" => Ok(Self::Abstain),
            _ => Err(OperationsError::Corrupt("leadership ballot choice")),
        }
    }
}

/// Election lifecycle state.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionState {
    Electing,
    ClosedElected,
    ClosedNoMajority,
    Cancelled,
}

impl ElectionState {
    const fn encode(self) -> &'static str {
        match self {
            Self::Electing => "ELECTING",
            Self::ClosedElected => "CLOSED_ELECTED",
            Self::ClosedNoMajority => "CLOSED_NO_MAJORITY",
            Self::Cancelled => "CANCELLED",
        }
    }

    fn decode(value: &str) -> Result<Self, OperationsError> {
        match value {
            "ELECTING" => Ok(Self::Electing),
            "CLOSED_ELECTED" => Ok(Self::ClosedElected),
            "CLOSED_NO_MAJORITY" => Ok(Self::ClosedNoMajority),
            "CANCELLED" => Ok(Self::Cancelled),
            _ => Err(OperationsError::Corrupt("leadership election state")),
        }
    }
}

/// Bounded failover reasons. The reason is validated at the boundary; the
/// bounded receipt vocabulary records the transition itself.
#[cfg(test)]
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeadershipFailoverReason {
    LeaderUnresponsive,
    IncumbentDeparted,
}

/// Explicit deferred bootstrap of one scope from one already-durable,
/// not-yet-consumed-for-bootstrap assignment offer projection.
#[cfg(test)]
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BootstrapLeadershipCommand {
    pub scope: LeadershipScope,
    pub assignment_id: AgentAssignmentId,
    pub now_ms: i64,
}

/// Promote one stabilizing scope to `ACTIVE` once its window has elapsed.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarkStableCommand {
    pub scope: LeadershipScope,
    pub expected_revision: i64,
    pub now_ms: i64,
}

/// Start one recall election for one scope.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecallElectionCommand {
    pub scope: LeadershipScope,
    pub expected_revision: i64,
    pub candidate_participant_id: Uuid,
    pub now_ms: i64,
}

/// Cast one ballot in one open election.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CastBallotCommand {
    pub election_id: Uuid,
    pub voter_participant_id: Uuid,
    pub choice: BallotChoice,
    pub now_ms: i64,
}

/// Close one election whose ballot deadline has passed.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CloseElectionCommand {
    pub election_id: Uuid,
    pub now_ms: i64,
}

/// Prepare one elected-candidate confirmation behind a Broker authority
/// fence. The command carries the caller-chosen correlation identity; the
/// durable Broker owner identity is derived from the catalog destination.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrepareLeadershipConfirmCommand {
    pub scope: LeadershipScope,
    pub election_id: Uuid,
    pub expected_revision: i64,
    pub request_id: Uuid,
    pub now_ms: i64,
}

/// Closed confirm-intent lifecycle. `pending` and `remote_recorded` are
/// resumable and are never pruned by this slice; `committed`, `superseded`
/// and `rejected` are terminal evidence states.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeadershipConfirmIntentState {
    Pending,
    RemoteRecorded,
    Committed,
    Superseded,
    Rejected,
}

impl LeadershipConfirmIntentState {
    const fn encode(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::RemoteRecorded => "remote_recorded",
            Self::Committed => "committed",
            Self::Superseded => "superseded",
            Self::Rejected => "rejected",
        }
    }

    fn decode(value: &str) -> Result<Self, OperationsError> {
        match value {
            "pending" => Ok(Self::Pending),
            "remote_recorded" => Ok(Self::RemoteRecorded),
            "committed" => Ok(Self::Committed),
            "superseded" => Ok(Self::Superseded),
            "rejected" => Ok(Self::Rejected),
            _ => Err(OperationsError::Corrupt("leadership confirm intent state")),
        }
    }
}

/// One durable `election_confirm` intent journal row.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipConfirmIntent {
    request_id: Uuid,
    scope: LeadershipScope,
    election_id: Uuid,
    elected_participant_id: Uuid,
    expected_local_revision: i64,
    requested_fence_revision: i64,
    owner_instance_id: Uuid,
    remote_recorded_revision: Option<i64>,
    confirmed_at_ms: Option<i64>,
    state: LeadershipConfirmIntentState,
    created_at_ms: i64,
    resolved_at_ms: Option<i64>,
}

impl LeadershipConfirmIntent {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }
    pub const fn scope(&self) -> LeadershipScope {
        self.scope
    }
    #[must_use]
    pub const fn election_id(&self) -> Uuid {
        self.election_id
    }
    #[must_use]
    pub const fn elected_participant_id(&self) -> Uuid {
        self.elected_participant_id
    }
    #[must_use]
    pub const fn expected_local_revision(&self) -> i64 {
        self.expected_local_revision
    }
    #[must_use]
    pub const fn requested_fence_revision(&self) -> i64 {
        self.requested_fence_revision
    }
    #[must_use]
    pub const fn owner_instance_id(&self) -> Uuid {
        self.owner_instance_id
    }
    #[must_use]
    pub const fn remote_recorded_revision(&self) -> Option<i64> {
        self.remote_recorded_revision
    }
    #[must_use]
    pub const fn confirmed_at_ms(&self) -> Option<i64> {
        self.confirmed_at_ms
    }
    pub const fn state(&self) -> LeadershipConfirmIntentState {
        self.state
    }
    #[must_use]
    pub const fn created_at_ms(&self) -> i64 {
        self.created_at_ms
    }
    #[must_use]
    pub const fn resolved_at_ms(&self) -> Option<i64> {
        self.resolved_at_ms
    }
}

/// The exact Broker `authority.record` request one prepared confirm must
/// fence before `commit_leadership_confirm` may run. Pure data: the caller
/// executes it outside any SQLite transaction.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipFenceRequest {
    request_id: Uuid,
    key: String,
    revision: i64,
    authority_id: Uuid,
    owner_instance_id: String,
}

impl LeadershipFenceRequest {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }
    #[must_use]
    pub const fn revision(&self) -> i64 {
        self.revision
    }
    #[must_use]
    pub const fn authority_id(&self) -> Uuid {
        self.authority_id
    }
    #[must_use]
    pub fn owner_instance_id(&self) -> &str {
        &self.owner_instance_id
    }

    fn new(
        request_id: Uuid,
        key: String,
        revision: i64,
        authority_id: Uuid,
        owner_instance_id: String,
    ) -> Self {
        Self {
            request_id,
            key,
            revision,
            authority_id,
            owner_instance_id,
        }
    }
}

/// Broker evidence that one exact authority request is excluded by `STALE_FENCE`.
/// This proves only that this retry cannot land now; it does not prove that the
/// request never landed before a later high-water revision.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipFenceExclusionProof {
    request: LeadershipFenceRequest,
}

impl LeadershipFenceExclusionProof {
    pub(crate) fn stale_for(request: &LeadershipFenceRequest) -> Self {
        Self {
            request: request.clone(),
        }
    }

    fn matches_request(&self, request: &LeadershipFenceRequest) -> bool {
        self.request == *request
    }
}

/// Broker evidence for one performed authority fence: the exact recorded
/// request identity and recorded revision the Broker returned. The identity
/// is copied from the prepared request by the broker adapter; callers cannot
/// construct an outcome for a different request payload.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipFenceOutcome {
    request_id: Uuid,
    key: String,
    revision: i64,
    authority_id: Uuid,
    owner_instance_id: String,
    recorded_revision: i64,
}

impl LeadershipFenceOutcome {
    pub(crate) fn from_request(request: &LeadershipFenceRequest, recorded_revision: i64) -> Self {
        Self {
            request_id: request.request_id,
            key: request.key.clone(),
            revision: request.revision,
            authority_id: request.authority_id,
            owner_instance_id: request.owner_instance_id.clone(),
            recorded_revision,
        }
    }

    #[must_use]
    pub const fn recorded_revision(&self) -> i64 {
        self.recorded_revision
    }

    fn matches_request(&self, request: &LeadershipFenceRequest) -> bool {
        self.request_id == request.request_id
            && self.key == request.key
            && self.revision == request.revision
            && self.authority_id == request.authority_id
            && self.owner_instance_id == request.owner_instance_id
    }
}

/// One prepared, not-yet-fenced confirm: the durable intent readback plus
/// the exact fence request the caller must perform.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedLeadershipConfirm {
    intent: LeadershipConfirmIntent,
    fence_request: LeadershipFenceRequest,
}

impl PreparedLeadershipConfirm {
    pub const fn intent(&self) -> &LeadershipConfirmIntent {
        &self.intent
    }
    pub const fn fence_request(&self) -> &LeadershipFenceRequest {
        &self.fence_request
    }
}

/// Outcome of one prepare: either a live (fresh or resumed) pending intent
/// that must now be fenced, or an honest resolution readback for an intent
/// whose outcome is already durably decided.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrepareLeadershipConfirmOutcome {
    Prepared(PreparedLeadershipConfirm),
    Resolved(LeadershipConfirmReadback),
}

/// Honest readback of one resolved confirm: the recorded intent state plus a
/// snapshot of the current leadership row.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipConfirmReadback {
    intent_state: LeadershipConfirmIntentState,
    scope: LeadershipScope,
    state: LeadershipState,
    leader_participant_id: Option<Uuid>,
    leadership_revision: i64,
    stabilization_until_ms: Option<i64>,
    updated_at_ms: i64,
}

impl LeadershipConfirmReadback {
    pub const fn intent_state(&self) -> LeadershipConfirmIntentState {
        self.intent_state
    }
    pub const fn scope(&self) -> LeadershipScope {
        self.scope
    }
    pub const fn state(&self) -> LeadershipState {
        self.state
    }
    #[must_use]
    pub const fn leader_participant_id(&self) -> Option<Uuid> {
        self.leader_participant_id
    }
    #[must_use]
    pub const fn leadership_revision(&self) -> i64 {
        self.leadership_revision
    }
    #[must_use]
    pub const fn stabilization_until_ms(&self) -> Option<i64> {
        self.stabilization_until_ms
    }
    #[must_use]
    pub const fn updated_at_ms(&self) -> i64 {
        self.updated_at_ms
    }
}

/// Resign one currently leading participant.
#[cfg(test)]
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResignLeadershipCommand {
    pub scope: LeadershipScope,
    pub expected_leader: Uuid,
    pub expected_revision: i64,
    pub now_ms: i64,
}

/// Demand failover for one stabilizing or active scope.
#[cfg(test)]
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MarkFailoverCommand {
    pub scope: LeadershipScope,
    pub expected_revision: i64,
    pub reason: LeadershipFailoverReason,
    pub now_ms: i64,
}

/// Deterministically fail over one leader only when its durable logical
/// presence is older than the bounded unresponsive threshold.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarkUnresponsiveFailoverCommand {
    pub scope: LeadershipScope,
    pub expected_revision: i64,
    pub now_ms: i64,
}

/// Mark one failed-over scope vacant when no successor exists.
#[cfg(test)]
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MarkVacantCommand {
    pub scope: LeadershipScope,
    pub expected_revision: i64,
    pub now_ms: i64,
}

/// Confirm one bootstrapped scope's offer evidence: the durable readback that
/// promotes `BOOTSTRAPPING` to `STABILIZING` under the recorded leader.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfirmBootstrapEvidenceCommand {
    pub scope: LeadershipScope,
    pub assignment_id: AgentAssignmentId,
    pub expected_revision: i64,
    pub now_ms: i64,
}

/// Confirm the exact pending bootstrap whose assignment identity is recovered
/// from the durable BOOTSTRAP receipt for this scope/revision.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfirmPendingBootstrapCommand {
    pub scope: LeadershipScope,
    pub expected_revision: i64,
    pub now_ms: i64,
}

/// Bounded recovery-entry reasons. The reason is validated at the boundary;
/// the recovery receipt records the transition itself.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeadershipRecoveryReason {
    /// A leadership commit was attempted and its outcome is unknown.
    CommitUnknown,
    /// A durable readback after a mutation could not be reconciled.
    ReadbackUnknown,
    /// An `AUTHORITY_PENDING` row and its election pointer cannot be
    /// reconciled against the recorded confirm intent.
    PointerUnreconciled,
}

/// Park one leading (or authority-pending) scope in `RECOVERY_REQUIRED` until
/// a same-identity durable readback reconciles the recorded facts.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarkRecoveryRequiredCommand {
    pub scope: LeadershipScope,
    pub expected_revision: i64,
    pub reason: LeadershipRecoveryReason,
    pub now_ms: i64,
}

/// Reconcile one recovery from the durable confirm-intent readback.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconcileRecoveryCommand {
    pub scope: LeadershipScope,
    pub intent_request_id: Uuid,
    pub expected_revision: i64,
    pub now_ms: i64,
}

/// Resolution of one recovery reconcile, read from durable facts only.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeadershipRecoveryResolution {
    /// The intent shows the fenced confirm committed: the scope returns to
    /// `STABILIZING` under the elected leader with the committed window.
    ConfirmCompleted(LeadershipMutationReceipt),
    /// The intent is terminal non-commit evidence (`superseded`/`rejected`):
    /// the scope resolved per recorded facts, back to its preserved previous
    /// phase or to `VACANT` when no leader remains.
    Resolved(LeadershipMutationReceipt),
    /// The intent is still live (`pending`/`remote_recorded`): the scope
    /// stays `RECOVERY_REQUIRED` and the observation was appended to the
    /// receipt history. Never a competing takeover.
    Unresolved(LeadershipMutationReceipt),
}

/// Read-back of one successful leadership mutation.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeadershipMutationReceipt {
    scope: LeadershipScope,
    state: LeadershipState,
    leader_participant_id: Option<Uuid>,
    leadership_revision: i64,
    stabilization_until_ms: Option<i64>,
    updated_at_ms: i64,
}

impl LeadershipMutationReceipt {
    pub const fn scope(&self) -> LeadershipScope {
        self.scope
    }
    pub const fn state(&self) -> LeadershipState {
        self.state
    }
    #[must_use]
    pub const fn leader_participant_id(&self) -> Option<Uuid> {
        self.leader_participant_id
    }
    #[must_use]
    pub const fn leadership_revision(&self) -> i64 {
        self.leadership_revision
    }
    #[must_use]
    pub const fn stabilization_until_ms(&self) -> Option<i64> {
        self.stabilization_until_ms
    }
    #[must_use]
    pub const fn updated_at_ms(&self) -> i64 {
        self.updated_at_ms
    }
}

/// Read-back of one successfully started recall election.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecallElectionReceipt {
    election_id: Uuid,
    ballot_deadline_ms: i64,
    electorate_size: i64,
}

impl RecallElectionReceipt {
    #[must_use]
    pub const fn election_id(&self) -> Uuid {
        self.election_id
    }
    #[must_use]
    pub const fn ballot_deadline_ms(&self) -> i64 {
        self.ballot_deadline_ms
    }
    #[must_use]
    pub const fn electorate_size(&self) -> i64 {
        self.electorate_size
    }
}

/// Read-back of one accepted or replayed ballot.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BallotReceipt {
    election_id: Uuid,
    voter_participant_id: Uuid,
    voter_participation_id: Option<WorkspaceParticipationId>,
    choice: BallotChoice,
    ballot_revision: i64,
    replay: bool,
}

impl BallotReceipt {
    #[must_use]
    pub const fn election_id(&self) -> Uuid {
        self.election_id
    }
    #[must_use]
    pub const fn voter_participant_id(&self) -> Uuid {
        self.voter_participant_id
    }
    #[must_use]
    pub const fn voter_participation_id(&self) -> Option<WorkspaceParticipationId> {
        self.voter_participation_id
    }
    pub const fn choice(&self) -> BallotChoice {
        self.choice
    }
    #[must_use]
    pub const fn ballot_revision(&self) -> i64 {
        self.ballot_revision
    }
    #[must_use]
    pub const fn replay(&self) -> bool {
        self.replay
    }
}

/// Final tally of one closed election.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ElectionTally {
    election_id: Uuid,
    state: ElectionState,
    elected_participant_id: Option<Uuid>,
    tally_yes: i64,
    tally_no: i64,
    tally_abstain: i64,
    electorate_size: i64,
}

impl ElectionTally {
    #[must_use]
    pub const fn election_id(&self) -> Uuid {
        self.election_id
    }
    pub const fn state(&self) -> ElectionState {
        self.state
    }
    #[must_use]
    pub const fn elected_participant_id(&self) -> Option<Uuid> {
        self.elected_participant_id
    }
    #[must_use]
    pub const fn tally_yes(&self) -> i64 {
        self.tally_yes
    }
    #[must_use]
    pub const fn tally_no(&self) -> i64 {
        self.tally_no
    }
    #[must_use]
    pub const fn tally_abstain(&self) -> i64 {
        self.tally_abstain
    }
    #[must_use]
    pub const fn electorate_size(&self) -> i64 {
        self.electorate_size
    }
}

/// Live summary of the scope's current election, including cast counts.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeadershipElectionSummary {
    election_id: Uuid,
    state: ElectionState,
    ballot_deadline_ms: i64,
    deadline_passed: bool,
    electorate_size: i64,
    cast_yes: i64,
    cast_no: i64,
    cast_abstain: i64,
}

impl LeadershipElectionSummary {
    #[must_use]
    pub const fn election_id(&self) -> Uuid {
        self.election_id
    }
    pub const fn state(&self) -> ElectionState {
        self.state
    }
    #[must_use]
    pub const fn ballot_deadline_ms(&self) -> i64 {
        self.ballot_deadline_ms
    }
    #[must_use]
    pub const fn deadline_passed(&self) -> bool {
        self.deadline_passed
    }
    #[must_use]
    pub const fn electorate_size(&self) -> i64 {
        self.electorate_size
    }
    #[must_use]
    pub const fn cast_yes(&self) -> i64 {
        self.cast_yes
    }
    #[must_use]
    pub const fn cast_no(&self) -> i64 {
        self.cast_no
    }
    #[must_use]
    pub const fn cast_abstain(&self) -> i64 {
        self.cast_abstain
    }
}

/// Exact leader authority returned only when the shared leadership predicate
/// proves a current Ready Session binding and a valid confirmation path.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CurrentLeadershipAuthority {
    scope: LeadershipScope,
    participant_id: Uuid,
    leadership_revision: i64,
}

impl CurrentLeadershipAuthority {
    pub const fn scope(self) -> LeadershipScope {
        self.scope
    }

    #[must_use]
    pub const fn participant_id(self) -> Uuid {
        self.participant_id
    }

    #[must_use]
    pub const fn leadership_revision(self) -> i64 {
        self.leadership_revision
    }
}

/// Read-only leadership observation for one scope. Reading never mutates
/// state: deadline enforcement happens only in explicit commands.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipView {
    workspace_id: WorkspaceId,
    activity_id: Option<ActivityId>,
    team_id: Option<Uuid>,
    state: LeadershipState,
    leader_participant_id: Option<Uuid>,
    leader_display_name: Option<String>,
    leader_participation_id: Option<WorkspaceParticipationId>,
    leader_profile_id: Option<Uuid>,
    leadership_revision: i64,
    stabilization_until_ms: Option<i64>,
    current_election: Option<LeadershipElectionSummary>,
    updated_at_ms: i64,
}

impl LeadershipView {
    #[must_use]
    pub const fn leader_participation_id(&self) -> Option<WorkspaceParticipationId> {
        self.leader_participation_id
    }
    #[must_use]
    pub const fn leader_profile_id(&self) -> Option<Uuid> {
        self.leader_profile_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }
    #[must_use]
    pub const fn activity_id(&self) -> Option<ActivityId> {
        self.activity_id
    }
    #[must_use]
    pub const fn team_id(&self) -> Option<Uuid> {
        self.team_id
    }
    pub const fn state(&self) -> LeadershipState {
        self.state
    }
    #[must_use]
    pub const fn leader_participant_id(&self) -> Option<Uuid> {
        self.leader_participant_id
    }
    #[must_use]
    pub fn leader_display_name(&self) -> Option<&str> {
        self.leader_display_name.as_deref()
    }
    #[must_use]
    pub const fn leadership_revision(&self) -> i64 {
        self.leadership_revision
    }
    #[must_use]
    pub const fn stabilization_until_ms(&self) -> Option<i64> {
        self.stabilization_until_ms
    }
    #[must_use]
    pub const fn current_election(&self) -> Option<&LeadershipElectionSummary> {
        self.current_election.as_ref()
    }
    #[must_use]
    pub const fn updated_at_ms(&self) -> i64 {
        self.updated_at_ms
    }
}

/// The one assignment-offer observation consumed by the auto-bootstrap hook
/// inside the effect-fact page transaction.
#[derive(Debug, Clone, Copy)]
pub(crate) struct OfferBootstrapObservation {
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) activity_id: ActivityId,
    pub(crate) assignment_id: AgentAssignmentId,
    pub(crate) target_agent_id: AgentInstanceId,
    pub(crate) offered_at_ms: i64,
}

#[derive(Debug, Clone, Copy)]
struct LeadershipRow {
    scope: LeadershipScope,
    state: LeadershipState,
    leader_participant_id: Option<Uuid>,
    leadership_revision: i64,
    stabilization_until_ms: Option<i64>,
    current_election_id: Option<Uuid>,
    updated_at_ms: i64,
}

/// Single SQL authority gate shared by exact leadership reads and election
/// source queries. It intentionally references the enclosing `l` row alias.
pub(crate) const CURRENT_LEADERSHIP_AUTHORITY_PREDICATE: &str =
    "l.leader_participant_id IS NOT NULL
    AND EXISTS(SELECT 1
      FROM ops_current_effect_bindings AS b
      JOIN ops_logical_members AS lm
        ON lm.workspace_id=b.workspace_id AND lm.participant_id=b.participant_id
           AND lm.session_id=b.session_id
      WHERE b.workspace_id=l.workspace_id AND b.participant_id=l.leader_participant_id
        AND (l.scope_kind!=1 OR b.activity_id=l.activity_id))
    AND EXISTS(SELECT 1 FROM ops_leadership_receipts AS r
      WHERE r.workspace_id=l.workspace_id AND r.scope_kind=l.scope_kind
        AND r.team_id=COALESCE(l.team_id,zeroblob(16))
        AND (l.scope_kind!=1 OR r.activity_id IS l.activity_id)
        AND r.receipt_kind='CONFIRMED' AND r.participant_id=l.leader_participant_id
        AND r.leadership_revision<=l.leadership_revision)
    AND NOT EXISTS(SELECT 1 FROM ops_leadership_confirm_intents AS i
      WHERE i.workspace_id=l.workspace_id AND i.scope_kind=l.scope_kind
        AND i.activity_id IS l.activity_id AND i.team_id IS l.team_id
        AND i.state IN('pending','remote_recorded'))
    AND ((l.state IN('ACTIVE','STABILIZING') AND l.current_election_id IS NULL)
      OR EXISTS(SELECT 1 FROM ops_leadership_elections AS e
        JOIN ops_election_subject_cutovers AS c ON c.election_id=e.election_id
        WHERE e.election_id=l.current_election_id
          AND e.workspace_id=l.workspace_id AND e.scope_kind=l.scope_kind
          AND e.activity_id IS l.activity_id AND e.team_id IS l.team_id
          AND c.workspace_id=l.workspace_id AND c.scope_kind=l.scope_kind
          AND c.activity_id IS l.activity_id AND c.team_id IS l.team_id
          AND ((l.state='ELECTING' AND e.state='ELECTING')
            OR (l.state='AUTHORITY_PENDING' AND e.state='CLOSED_ELECTED'))
          AND NOT EXISTS(SELECT 1 FROM ops_leadership_receipts AS fr
            WHERE fr.workspace_id=l.workspace_id AND fr.scope_kind=l.scope_kind
              AND fr.team_id=COALESCE(l.team_id,zeroblob(16))
              AND (l.scope_kind!=1 OR fr.activity_id IS l.activity_id)
              AND fr.receipt_kind='FAILOVER'
              AND fr.leadership_revision=c.source_leadership_revision)))";

#[derive(Debug, Clone, Copy)]
struct ElectionRow {
    scope: LeadershipScope,
    state: ElectionState,
    candidate_participant_id: Uuid,
    ballot_deadline_ms: i64,
    elected_participant_id: Option<Uuid>,
    tally_yes: Option<i64>,
    tally_no: Option<i64>,
    tally_abstain: Option<i64>,
}

/// One appended receipt row.
struct ReceiptRow {
    kind: ReceiptKind,
    provenance_activity: Option<ActivityId>,
    work_assignment_id: Option<AgentAssignmentId>,
    election_id: Option<Uuid>,
    participant_id: Option<Uuid>,
    leadership_revision: i64,
    recorded_at_ms: i64,
}

/// Append-only receipt vocabulary.
#[derive(Debug, Clone, Copy)]
enum ReceiptKind {
    Bootstrap,
    Elected,
    Confirmed,
    Vacated,
    Failover,
    Recovery,
}

impl ReceiptKind {
    const fn encode(self) -> &'static str {
        match self {
            Self::Bootstrap => "BOOTSTRAP",
            Self::Elected => "ELECTED",
            Self::Confirmed => "CONFIRMED",
            Self::Vacated => "VACATED",
            Self::Failover => "FAILOVER",
            Self::Recovery => "RECOVERY",
        }
    }
}

impl OperationsCatalog {
    /// Explicitly bootstrap one leadership scope from one already-durable
    /// assignment-offer projection whose target participant is resolvable.
    /// This covers offers that deferred during page consumption.
    ///
    /// # Errors
    /// Rejects unknown offers, unresolvable participants, already-bootstrapped
    /// offers, already-seeded scopes, and uncertain commits.
    #[cfg(test)]
    fn bootstrap_leadership(
        &mut self,
        command: BootstrapLeadershipCommand,
    ) -> Result<LeadershipMutationReceipt, OperationsError> {
        self.leadership_transaction(move |tx| {
            let offer =
                load_offer_projection(tx, command.scope.workspace_id(), command.assignment_id)?
                    .ok_or(OperationsError::Conflict("leadership bootstrap offer"))?;
            let participant = resolve_offer_participant(
                tx,
                command.scope.workspace_id(),
                offer.activity_id,
                offer.target_agent_id,
            )?
            .ok_or(OperationsError::Conflict(
                "leadership bootstrap participant",
            ))?;
            if bootstrap_receipt_exists(tx, command.scope, command.assignment_id)? {
                return Err(OperationsError::Conflict("leadership bootstrap replay"));
            }
            let receipt = bootstrap_scope(
                tx,
                command.scope,
                participant,
                command.assignment_id,
                Some(offer.activity_id),
                command.now_ms,
            )?
            .ok_or(OperationsError::Conflict("leadership already seeded"))?;
            Ok((command.scope.workspace_id(), receipt))
        })
    }

    /// Promote one `STABILIZING` scope to `ACTIVE` once its stabilization
    /// window has elapsed. Before the window the command fails closed.
    ///
    /// # Errors
    /// Rejects unknown scopes, non-stabilizing states, stale revisions, the
    /// unelapsed window, and uncertain commits.
    pub fn mark_stable(
        &mut self,
        command: MarkStableCommand,
    ) -> Result<LeadershipMutationReceipt, OperationsError> {
        self.leadership_transaction(move |tx| {
            let row = load_leadership(tx, command.scope)?
                .ok_or(OperationsError::Conflict("leadership scope missing"))?;
            require_revision(&row, command.expected_revision)?;
            if row.state != LeadershipState::Stabilizing {
                return Err(OperationsError::Conflict("leadership not stabilizing"));
            }
            let until = row
                .stabilization_until_ms
                .ok_or(OperationsError::Corrupt("leadership stabilization window"))?;
            if command.now_ms < until {
                return Err(OperationsError::Conflict("leadership stabilization window"));
            }
            let updated = cas_leadership(
                tx,
                row.scope,
                row.leadership_revision,
                LeadershipUpdate {
                    state: LeadershipState::Active,
                    leader_participant_id: row.leader_participant_id,
                    stabilization_until_ms: None,
                    current_election_id: row.current_election_id,
                    now_ms: command.now_ms,
                },
            )?
            .ok_or(OperationsError::Conflict("leadership revision"))?;
            append_receipt(
                tx,
                row.scope,
                &ReceiptRow {
                    kind: ReceiptKind::Confirmed,
                    provenance_activity: None,
                    work_assignment_id: None,
                    election_id: None,
                    participant_id: row.leader_participant_id,
                    leadership_revision: updated.leadership_revision,
                    recorded_at_ms: command.now_ms,
                },
            )?;
            Ok((row.scope.workspace_id(), updated))
        })
    }

    /// Start one recall election from `ACTIVE`. The candidate must be a Ready
    /// member of the frozen electorate and must meet the exact 35% credit
    /// qualification; the stabilization lock rejects earlier starts.
    ///
    /// # Errors
    /// Rejects unknown scopes, non-active states, stale revisions,
    /// unqualified or non-member candidates, and uncertain commits.
    #[cfg(test)]
    fn start_recall_election(
        &mut self,
        command: RecallElectionCommand,
    ) -> Result<RecallElectionReceipt, OperationsError> {
        self.start_election_internal(command, LeadershipState::Active)
    }

    /// Start one recall election from `FAILOVER_REQUIRED`. Qualification and
    /// electorate membership are still enforced, so a departed incumbent is
    /// never electable; no arbitrary winner is manufactured.
    ///
    /// # Errors
    /// Rejects unknown scopes, non-failed-over states, stale revisions,
    /// unqualified or non-member candidates, and uncertain commits.
    pub fn start_failover_recall_election(
        &mut self,
        command: RecallElectionCommand,
    ) -> Result<RecallElectionReceipt, OperationsError> {
        self.start_election_internal(command, LeadershipState::FailoverRequired)
    }

    /// Cast one ballot in one open election. The voter must be in the frozen
    /// electorate; an identical repeat ballot is a harmless no-op and a
    /// changed choice conflicts. One participant holds exactly one vote.
    ///
    /// # Errors
    /// Rejects unknown elections, closed elections, elapsed deadlines,
    /// non-electorate voters, changed repeat ballots, and uncertain commits.
    pub fn cast_ballot(
        &mut self,
        command: CastBallotCommand,
    ) -> Result<BallotReceipt, OperationsError> {
        self.leadership_transaction(move |tx| {
            let election = load_election(tx, command.election_id)?
                .ok_or(OperationsError::Conflict("leadership election missing"))?;
            if election.state != ElectionState::Electing {
                return Err(OperationsError::Conflict("leadership election closed"));
            }
            Self::ensure_ballot_open(tx, command.election_id, command.now_ms)?;
            if command.now_ms >= election.ballot_deadline_ms {
                return Err(OperationsError::Conflict("leadership ballot deadline"));
            }
            if !election_subject::has_cutover(tx, command.election_id)? {
                return Err(OperationsError::Conflict(
                    "legacy leadership election is read-only",
                ));
            }
            let outcome = election_subject::cast_subject_ballot(
                tx,
                command.election_id,
                command.voter_participant_id,
                command.choice.encode(),
                command.now_ms,
            )?;
            let stored_choice = BallotChoice::decode(outcome.stored_choice())?;
            Ok((
                election.scope.workspace_id(),
                BallotReceipt {
                    election_id: command.election_id,
                    voter_participant_id: command.voter_participant_id,
                    voter_participation_id: outcome.voter_participation_id(),
                    choice: stored_choice,
                    ballot_revision: 1,
                    replay: outcome.replay(),
                },
            ))
        })
    }

    /// Close one election whose ballot deadline has passed and decide it by a
    /// strict majority of the FULL frozen electorate (`yes > size / 2`;
    /// abstentions and non-responses count against the replacement). A pass
    /// moves the scope to `AUTHORITY_PENDING` with the incumbent still
    /// leading. A failed normal recall returns the incumbent to `ACTIVE`; a
    /// failed failover recall remains `FAILOVER_REQUIRED`, never reviving the
    /// failed incumbent. Without an incumbent the scope becomes `VACANT`.
    ///
    /// # Errors
    /// Rejects unknown elections, unelapsed deadlines, non-electing states,
    /// stale leadership revisions, and uncertain commits.
    pub fn close_election(
        &mut self,
        command: CloseElectionCommand,
    ) -> Result<ElectionTally, OperationsError> {
        self.leadership_transaction(move |tx| {
            let election = load_election(tx, command.election_id)?
                .ok_or(OperationsError::Conflict("leadership election missing"))?;
            if election.state != ElectionState::Electing {
                return Err(OperationsError::Conflict("leadership election closed"));
            }
            if command.now_ms < election.ballot_deadline_ms {
                return Err(OperationsError::Conflict("leadership ballot deadline"));
            }
            let electorate_size = election_subject::electorate_size(tx, command.election_id)?;
            let (tally_yes, tally_no, tally_abstain) =
                election_subject::cast_tallies(tx, command.election_id)?;
            let passed = tally_yes > electorate_size / 2;
            let state = if passed {
                ElectionState::ClosedElected
            } else {
                ElectionState::ClosedNoMajority
            };
            let elected = if passed {
                Some(election.candidate_participant_id)
            } else {
                None
            };
            tx.execute(
                "UPDATE ops_leadership_elections SET state=?2,elected_participant_id=?3,tally_yes=?4,tally_no=?5,tally_abstain=?6,closed_at_ms=?7 WHERE election_id=?1 AND state='ELECTING'",
                params![
                    command.election_id.as_bytes(),
                    state.encode(),
                    elected.map(|value| value.as_bytes().to_vec()),
                    tally_yes,
                    tally_no,
                    tally_abstain,
                    command.now_ms,
                ],
            )?;
            let row = load_leadership(tx, election.scope)?
                .ok_or(OperationsError::Corrupt("leadership election scope"))?;
            if row.state!=LeadershipState::Electing || row.current_election_id!=Some(command.election_id) {
                return Err(OperationsError::Conflict("leadership election no longer current"));
            }
            let next_state = if passed {
                LeadershipState::AuthorityPending
            } else if election_originated_from_failover(tx, &row)? {
                LeadershipState::FailoverRequired
            } else if row.leader_participant_id.is_some() {
                LeadershipState::Active
            } else {
                LeadershipState::Vacant
            };
            let kept_election = if passed {
                row.current_election_id
            } else {
                None
            };
            let updated = cas_leadership(
                tx,
                election.scope,
                row.leadership_revision,
                LeadershipUpdate {
                    state: next_state,
                    leader_participant_id: row.leader_participant_id,
                    stabilization_until_ms: None,
                    current_election_id: kept_election,
                    now_ms: command.now_ms,
                },
            )?
            .ok_or(OperationsError::Conflict("leadership revision"))?;
            if let Some(elected) = elected {
                append_receipt(
                    tx,
                    election.scope,
                    &ReceiptRow {
                        kind: ReceiptKind::Elected,
                        provenance_activity: None,
                        work_assignment_id: None,
                        election_id: Some(command.election_id),
                        participant_id: Some(elected),
                        leadership_revision: updated.leadership_revision,
                        recorded_at_ms: command.now_ms,
                    },
                )?;
            }
            Ok((
                election.scope.workspace_id(),
                ElectionTally {
                    election_id: command.election_id,
                    state,
                    elected_participant_id: elected,
                    tally_yes,
                    tally_no,
                    tally_abstain,
                    electorate_size,
                },
            ))
        })
    }

    /// Cancel an unactivated election whose winner is no longer a Ready scope
    /// member. Existing ballots remain public evidence; no invalid winner is
    /// activated and no unresolved intent can later revive the cancelled result.
    ///
    /// # Errors
    /// Rejects stale revisions, corrupt election links and uncertain commits.
    pub fn reconcile_ineligible_elected_candidate(
        &mut self,
        scope: LeadershipScope,
        expected_revision: i64,
        now_ms: i64,
    ) -> Result<Option<LeadershipMutationReceipt>, OperationsError> {
        self.leadership_transaction(|tx| {
            let row=load_leadership(tx,scope)?.ok_or(OperationsError::Conflict("leadership scope missing"))?;
            require_revision(&row,expected_revision)?;
            if row.state!=LeadershipState::AuthorityPending { return Ok((scope.workspace_id(),None)); }
            let election_id=row.current_election_id.ok_or(OperationsError::Corrupt("leadership election pointer"))?;
            let election=load_election(tx,election_id)?.ok_or(OperationsError::Corrupt("leadership election missing"))?;
            let elected=election.elected_participant_id.ok_or(OperationsError::Corrupt("leadership election winner"))?;
            let ready=election_subject::snapshot_ready_subjects_for_scope(tx,scope)?;
            if ready.contains_participant(elected) { return Ok((scope.workspace_id(),None)); }
            // An in-flight Broker operation may already have recorded authority.
            // Wait for exact recovery rather than manufacturing a terminal result.
            if scope_has_live_confirm_intent(tx,scope)? { return Ok((scope.workspace_id(),None)); }
            tx.execute("UPDATE ops_leadership_elections SET state='CANCELLED',closed_at_ms=?2 WHERE election_id=?1 AND state='CLOSED_ELECTED'",params![election_id.as_bytes(),now_ms])?;
            let leader=row.leader_participant_id.filter(|id|ready.contains_participant(*id));
            let updated=cas_leadership(tx,scope,row.leadership_revision,LeadershipUpdate {
                state:if leader.is_some(){LeadershipState::Active}else{LeadershipState::Vacant},
                leader_participant_id:leader,stabilization_until_ms:None,current_election_id:None,now_ms,
            })?.ok_or(OperationsError::Conflict("leadership revision"))?;
            append_receipt(tx,scope,&ReceiptRow {kind:ReceiptKind::Vacated,provenance_activity:None,work_assignment_id:None,election_id:Some(election_id),participant_id:Some(elected),leadership_revision:updated.leadership_revision,recorded_at_ms:now_ms})?;
            invalidate_workspace_election_for_team_change(tx, scope, now_ms)?;
            Ok((scope.workspace_id(),Some(updated)))
        })
    }

    /// Advance deterministic lifecycle edges; elected authority still requires
    /// the separate Broker-fenced confirmation lifecycle.
    ///
    /// # Errors
    /// Rejects stale revisions, invalid clocks, missing evidence and uncertain commits.
    pub fn advance_leadership(
        &mut self,
        scope: LeadershipScope,
        now_ms: i64,
    ) -> Result<(bool, Option<LeadershipView>), OperationsError> {
        if now_ms < 0 {
            return Err(OperationsError::Bounds("leadership clock"));
        }
        let view = self.leadership_scope_view(scope, now_ms)?;
        let Some(view) = view else {
            return Ok((false, None));
        };
        if view.state() == LeadershipState::Bootstrapping {
            let _ = self.confirm_pending_bootstrap(ConfirmPendingBootstrapCommand {
                scope,
                expected_revision: view.leadership_revision(),
                now_ms,
            })?;
            let updated = self.leadership_scope_view(scope, now_ms)?;
            return Ok((true, updated));
        }
        if matches!(
            view.state(),
            LeadershipState::Stabilizing | LeadershipState::Active
        ) && self
            .mark_failover_if_unresponsive(MarkUnresponsiveFailoverCommand {
                scope,
                expected_revision: view.leadership_revision(),
                now_ms,
            })?
            .is_some()
        {
            let updated = self.leadership_scope_view(scope, now_ms)?;
            return Ok((true, updated));
        }
        match view.state() {
            LeadershipState::AuthorityPending => {
                let changed = self
                    .reconcile_ineligible_elected_candidate(
                        scope,
                        view.leadership_revision(),
                        now_ms,
                    )?
                    .is_some();
                let updated = self.leadership_scope_view(scope, now_ms)?;
                return Ok((changed, updated));
            }
            LeadershipState::Electing => {
                let election = view
                    .current_election()
                    .ok_or(OperationsError::Corrupt("leadership election missing"))?;
                if !election.deadline_passed() {
                    return Ok((false, Some(view)));
                }
                let _ = self.close_election(CloseElectionCommand {
                    election_id: election.election_id(),
                    now_ms,
                })?;
            }
            LeadershipState::Stabilizing => {
                let until = view
                    .stabilization_until_ms()
                    .ok_or(OperationsError::Corrupt("leadership stabilization window"))?;
                if now_ms < until {
                    return Ok((false, Some(view)));
                }
                let _ = self.mark_stable(MarkStableCommand {
                    scope,
                    expected_revision: view.leadership_revision(),
                    now_ms,
                })?;
            }
            _ => return Ok((false, Some(view))),
        }
        let updated = self.leadership_scope_view(scope, now_ms)?;
        Ok((true, updated))
    }

    /// The durable Broker owner instance identity for authority fences: the
    /// catalog's own destination id, sealed once beside the catalog. It is
    /// derived here (not chosen per call) so a lost fence response replays
    /// with the exact same owner across process restarts.
    ///
    /// The wire form is `orch-{destination_id}`; the journal stores the raw
    /// 16-byte id.
    #[must_use]
    pub fn broker_owner_instance_id(&self) -> String {
        format!("orch-{}", self.destination)
    }

    /// Prepare one elected-candidate confirmation: re-validate the exact
    /// leadership preconditions (scope present, `AUTHORITY_PENDING` at the
    /// expected revision, election pointer, `CLOSED_ELECTED` winner) and
    /// durably record the confirm intent in one IMMEDIATE transaction. The
    /// returned fence request must be performed against the Broker outside
    /// any transaction before `commit_leadership_confirm` runs; no SQLite
    /// transaction or catalog lock is held while the network await happens.
    ///
    /// Reconciliation is exact-identity: re-preparing with the same
    /// `request_id` and identical payload on a still-`AUTHORITY_PENDING` row
    /// resumes the same intent (a re-fence is an idempotent Broker readback);
    /// an unresolved intent whose local row already moved on is marked
    /// `superseded` and reported honestly; a terminal intent returns the
    /// durable resolution without re-execution; a reused `request_id` with a
    /// different payload conflicts.
    ///
    /// # Errors
    /// Rejects unknown scopes or elections, non-`AUTHORITY_PENDING` states,
    /// mismatched election pointers, unelected elections, stale revisions,
    /// nil request identities, identity payload conflicts, and uncertain
    /// commits.
    pub fn prepare_leadership_confirm(
        &mut self,
        command: PrepareLeadershipConfirmCommand,
    ) -> Result<PrepareLeadershipConfirmOutcome, OperationsError> {
        if command.request_id.is_nil() {
            return Err(OperationsError::Conflict("leadership confirm request id"));
        }
        let destination = self.destination;
        self.leadership_transaction(move |tx| {
            let existing = load_confirm_intent(tx, command.request_id)?;
            if let Some(existing) = &existing {
                if !confirm_intent_matches_command(existing, &command) {
                    return Err(OperationsError::Conflict("leadership confirm identity"));
                }
                if !is_live_confirm_state(existing.state) {
                    let readback = confirm_readback(tx, existing, existing.state)?;
                    return Ok((
                        command.scope.workspace_id(),
                        PrepareLeadershipConfirmOutcome::Resolved(readback),
                    ));
                }
                let fence_request = confirm_fence_request(existing);
                return Ok((
                    command.scope.workspace_id(),
                    PrepareLeadershipConfirmOutcome::Prepared(PreparedLeadershipConfirm {
                        intent: existing.clone(),
                        fence_request,
                    }),
                ));
            }

            let row = load_leadership(tx, command.scope)?
                .ok_or(OperationsError::Conflict("leadership scope missing"))?;
            let elected = validate_confirm_preconditions(tx, &row, &command)?;
            let intent = insert_confirm_intent(tx, &command, elected, destination)?;
            invalidate_workspace_election_for_team_change(tx, command.scope, command.now_ms)?;
            let fence_request = confirm_fence_request(&intent);
            Ok((
                command.scope.workspace_id(),
                PrepareLeadershipConfirmOutcome::Prepared(PreparedLeadershipConfirm {
                    intent,
                    fence_request,
                }),
            ))
        })
    }

    /// Commit one fenced confirm in a single IMMEDIATE transaction: the
    /// leadership row is re-validated, compared-and-set from the expected
    /// revision into `STABILIZING` under the elected leader with a fresh
    /// stabilization window, the `CONFIRMED` receipt is appended, and the
    /// intent is marked `committed` with the Broker-recorded revision in the
    /// SAME transaction — so a lost commit response leaves durable committed
    /// state that a restart readback observes.
    ///
    /// # Errors
    /// Rejects an unknown or already-resolved intent, fence evidence that
    /// does not match the recorded intent revision, non-`AUTHORITY_PENDING`
    /// or moved-on local rows (the intent is superseded atomically), stale
    /// revisions, and uncertain commits.
    pub fn commit_leadership_confirm(
        &mut self,
        prepared: &PreparedLeadershipConfirm,
        fence: LeadershipFenceOutcome,
        now_ms: i64,
    ) -> Result<LeadershipMutationReceipt, OperationsError> {
        let internal = self.commit_leadership_confirm_transaction(prepared, fence, now_ms)?;
        match internal {
            InternalConfirmCommit::Committed(receipt) => Ok(receipt),
            // The superseded evidence is already durably committed; the stale
            // precondition is the honest failure for the caller.
            InternalConfirmCommit::Superseded(error) => Err(error),
        }
    }

    fn commit_leadership_confirm_transaction(
        &mut self,
        prepared: &PreparedLeadershipConfirm,
        fence: LeadershipFenceOutcome,
        now_ms: i64,
    ) -> Result<InternalConfirmCommit, OperationsError> {
        self.leadership_transaction(move |tx| {
            let stored = load_confirm_intent(tx, prepared.intent.request_id)?.ok_or(
                OperationsError::Conflict("leadership confirm intent unknown"),
            )?;
            if !is_live_confirm_state(stored.state) {
                // The response of an earlier resolution was lost: the durable
                // outcome is authoritative and is never re-executed.
                return Err(OperationsError::Conflict(
                    "leadership confirm already resolved",
                ));
            }
            if !confirm_intents_agree(&stored, &prepared.intent) {
                return Err(OperationsError::Conflict("leadership confirm identity"));
            }
            let stored_fence_request = confirm_fence_request(&stored);
            if !fence.matches_request(prepared.fence_request())
                || !fence.matches_request(&stored_fence_request)
            {
                return Err(OperationsError::Conflict("leadership fence identity"));
            }
            if fence.recorded_revision() != stored.requested_fence_revision {
                return Err(OperationsError::Conflict("leadership fence revision"));
            }
            let command = PrepareLeadershipConfirmCommand {
                scope: stored.scope,
                election_id: stored.election_id,
                expected_revision: stored.expected_local_revision,
                request_id: stored.request_id,
                now_ms,
            };
            let row = load_leadership(tx, stored.scope)?
                .ok_or(OperationsError::Conflict("leadership scope missing"))?;
            let elected = match validate_confirm_preconditions(tx, &row, &command) {
                Ok(elected) => elected,
                // The local CAS precondition vanished while the fence was in
                // flight: the remote record stays as the next succession's
                // high-water, the intent resolves atomically as superseded
                // evidence inside THIS transaction, and the stale precondition
                // is surfaced only after the commit so the evidence survives.
                Err(error) => {
                    supersede_confirm_intent(tx, &stored, Some(fence.recorded_revision()), now_ms)?;
                    let _ = terminalize_noncommitted_leadership(
                        tx,
                        &row,
                        Some(stored.election_id),
                        now_ms,
                    )?;
                    return Ok((
                        stored.scope.workspace_id(),
                        InternalConfirmCommit::Superseded(error),
                    ));
                }
            };
            let until = now_ms
                .checked_add(STABILIZATION_WINDOW_MS)
                .ok_or(OperationsError::Bounds("leadership stabilization window"))?;
            let updated = cas_leadership(
                tx,
                stored.scope,
                row.leadership_revision,
                LeadershipUpdate {
                    state: LeadershipState::Stabilizing,
                    leader_participant_id: Some(elected),
                    stabilization_until_ms: Some(until),
                    current_election_id: None,
                    now_ms: command.now_ms,
                },
            )?
            .ok_or(OperationsError::Conflict("leadership revision"))?;
            append_receipt(
                tx,
                stored.scope,
                &ReceiptRow {
                    kind: ReceiptKind::Confirmed,
                    provenance_activity: None,
                    work_assignment_id: None,
                    election_id: Some(stored.election_id),
                    participant_id: Some(elected),
                    leadership_revision: updated.leadership_revision,
                    recorded_at_ms: command.now_ms,
                },
            )?;
            work_exchange::append_confirmed(tx, &stored, updated.leadership_revision, now_ms)?;
            mark_confirm_intent(
                tx,
                stored.request_id,
                LeadershipConfirmIntentState::Committed,
                Some(fence.recorded_revision()),
                now_ms,
            )?;
            invalidate_workspace_election_for_team_change(tx, stored.scope, now_ms)?;
            Ok((
                stored.scope.workspace_id(),
                InternalConfirmCommit::Committed(updated),
            ))
        })
    }

    /// Resolve one live confirm only from an exact-request `STALE_FENCE` proof.
    /// The Broker rejection excludes this retry now; it does not establish that
    /// the same request never landed before a later high-water revision.
    ///
    /// # Errors
    /// Rejects unknown/mismatched intents, a proof for another request, and
    /// uncertain commits. A same-request terminal race returns durable readback.
    pub fn exclude_leadership_confirm(
        &mut self,
        prepared: &PreparedLeadershipConfirm,
        proof: LeadershipFenceExclusionProof,
        now_ms: i64,
    ) -> Result<LeadershipConfirmReadback, OperationsError> {
        self.leadership_transaction(move |tx| {
            let stored = load_confirm_intent(tx, prepared.intent.request_id)?.ok_or(
                OperationsError::Conflict("leadership confirm intent unknown"),
            )?;
            if !confirm_intents_agree(&stored, &prepared.intent) {
                return Err(OperationsError::Conflict("leadership confirm identity"));
            }
            let stored_request = confirm_fence_request(&stored);
            if !proof.matches_request(prepared.fence_request())
                || !proof.matches_request(&stored_request)
            {
                return Err(OperationsError::Conflict(
                    "leadership fence exclusion identity",
                ));
            }
            if !is_live_confirm_state(stored.state) {
                let readback = confirm_readback(tx, &stored, stored.state)?;
                return Ok((readback.scope.workspace_id(), readback));
            }
            let row = load_leadership(tx, stored.scope)?
                .ok_or(OperationsError::Conflict("leadership scope missing"))?;
            supersede_confirm_intent(tx, &stored, None, now_ms)?;
            let _ =
                terminalize_noncommitted_leadership(tx, &row, Some(stored.election_id), now_ms)?;
            let readback = confirm_readback(tx, &stored, LeadershipConfirmIntentState::Superseded)?;
            Ok((readback.scope.workspace_id(), readback))
        })
    }

    /// Read one confirm intent by correlation identity. Observation only.
    ///
    /// # Errors
    /// Rejects corrupt persisted state and database read failures.
    pub fn leadership_confirm_intent(
        &self,
        request_id: Uuid,
    ) -> Result<Option<LeadershipConfirmIntent>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let intent = load_confirm_intent(&transaction, request_id)?;
        catalog::commit_read(transaction, self.destination)?;
        Ok(intent)
    }

    /// Read a bounded UUID-keyset page of live confirms and reconstruct each
    /// exact durable Broker request without revalidating mutable leadership.
    ///
    /// # Errors
    /// Rejects invalid cursors/limits, corrupt intents, and storage failures.
    pub fn pending_leadership_confirms(
        &self,
        after_request_id: Option<Uuid>,
        limit: u32,
    ) -> Result<Vec<PreparedLeadershipConfirm>, OperationsError> {
        const MAX_PAGE: u32 = 8;
        if !(1..=MAX_PAGE).contains(&limit) {
            return Err(OperationsError::Bounds("leadership confirm page"));
        }
        if after_request_id.is_some_and(|id| id.is_nil()) {
            return Err(OperationsError::Conflict("leadership confirm cursor"));
        }
        let tx = self.connection.unchecked_transaction()?;
        let after = after_request_id.map(|id| id.into_bytes().to_vec());
        let mut query = tx.prepare(
            "SELECT substr(c.request_id,1,17) FROM ops_leadership_confirm_intents c WHERE c.state IN('pending','remote_recorded') AND (?1 IS NULL OR c.request_id>?1) AND EXISTS(SELECT 1 FROM ops_imports i WHERE i.workspace_id=c.workspace_id AND i.destination_id=?2 AND i.phase=2) ORDER BY c.request_id LIMIT ?3",
        )?;
        let ids = query
            .query_map(
                params![after, self.destination.as_bytes(), i64::from(limit)],
                |row| row.get::<_, Vec<u8>>(0),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        drop(query);
        let mut prepared = Vec::with_capacity(ids.len());
        for raw_id in ids {
            let request_id = Uuid::from_bytes(wire::id(raw_id)?);
            let intent = load_confirm_intent(&tx, request_id)?
                .ok_or(OperationsError::Corrupt("leadership confirm page"))?;
            if !is_live_confirm_state(intent.state) {
                return Err(OperationsError::Corrupt("leadership confirm page state"));
            }
            let fence_request = confirm_fence_request(&intent);
            prepared.push(PreparedLeadershipConfirm {
                intent,
                fence_request,
            });
        }
        catalog::commit_read(tx, self.destination)?;
        Ok(prepared)
    }

    /// Read one intent and its current scope from the same SQLite snapshot.
    ///
    /// # Errors
    /// Rejects corrupt intents, unavailable leadership rows, and storage errors.
    pub fn leadership_confirmation_snapshot(
        &self,
        request_id: Uuid,
    ) -> Result<Option<(LeadershipConfirmIntent, LeadershipConfirmReadback)>, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        let intent = load_confirm_intent(&tx, request_id)?;
        let snapshot = intent
            .map(|intent| {
                let readback = confirm_readback(&tx, &intent, intent.state)?;
                Ok::<_, OperationsError>((intent, readback))
            })
            .transpose()?;
        catalog::commit_read(tx, self.destination)?;
        Ok(snapshot)
    }

    /// Resolve orphaned pending/recovery rows from exact durable evidence.
    /// Live aliases and rows owned by a newer election are left untouched.
    ///
    /// # Errors
    /// Rejects missing/corrupt scope facts and uncertain database commits.
    pub fn recover_terminal_leadership(
        &mut self,
        scope: LeadershipScope,
        now_ms: i64,
    ) -> Result<(), OperationsError> {
        self.leadership_transaction(move |tx| {
            let row = load_leadership(tx, scope)?
                .ok_or(OperationsError::Conflict("leadership scope missing"))?;
            match row.state {
                LeadershipState::AuthorityPending => {
                    let Some(election_id) = row.current_election_id else {
                        return Ok((scope.workspace_id(), ()));
                    };
                    let election = load_election(tx, election_id)?
                        .ok_or(OperationsError::Corrupt("leadership recovery election"))?;
                    if election.scope != scope
                        || election.state == ElectionState::Electing
                        || has_live_confirm_intent(tx, scope, Some(election_id))?
                        || !terminal_confirm_intent_exists(tx, scope, election_id)?
                    {
                        return Ok((scope.workspace_id(), ()));
                    }
                    if let Some(intent) = unique_committed_recovery_intent(tx, &row)? {
                        let _ = reconcile_committed_recovery(tx, &row, &intent, now_ms)?;
                    } else {
                        let _ = terminalize_noncommitted_leadership(
                            tx,
                            &row,
                            Some(election_id),
                            now_ms,
                        )?;
                    }
                }
                LeadershipState::RecoveryRequired => {
                    if has_live_confirm_intent(tx, scope, None)? {
                        return Ok((scope.workspace_id(), ()));
                    }
                    if let Some(intent) = unique_committed_recovery_intent(tx, &row)? {
                        let _ = reconcile_committed_recovery(tx, &row, &intent, now_ms)?;
                    } else {
                        let _ = terminalize_noncommitted_leadership(
                            tx,
                            &row,
                            row.current_election_id,
                            now_ms,
                        )?;
                    }
                }
                _ => {}
            }
            Ok((scope.workspace_id(), ()))
        })
    }

    /// Resign one currently leading participant; the scope becomes `VACANT`.
    ///
    /// # Errors
    /// Rejects unknown scopes, non-leading states, a mismatched expected
    /// leader, stale revisions, and uncertain commits.
    #[cfg(test)]
    fn resign_leadership(
        &mut self,
        command: ResignLeadershipCommand,
    ) -> Result<LeadershipMutationReceipt, OperationsError> {
        self.leadership_transaction(move |tx| {
            let row = load_leadership(tx, command.scope)?
                .ok_or(OperationsError::Conflict("leadership scope missing"))?;
            require_revision(&row, command.expected_revision)?;
            if !matches!(
                row.state,
                LeadershipState::Stabilizing | LeadershipState::Active
            ) {
                return Err(OperationsError::Conflict("leadership not resignable"));
            }
            if row.leader_participant_id != Some(command.expected_leader) {
                return Err(OperationsError::Conflict("leadership expected leader"));
            }
            let updated = cas_leadership(
                tx,
                command.scope,
                row.leadership_revision,
                LeadershipUpdate {
                    state: LeadershipState::Vacant,
                    leader_participant_id: None,
                    stabilization_until_ms: None,
                    current_election_id: None,
                    now_ms: command.now_ms,
                },
            )?
            .ok_or(OperationsError::Conflict("leadership revision"))?;
            append_receipt(
                tx,
                command.scope,
                &ReceiptRow {
                    kind: ReceiptKind::Vacated,
                    provenance_activity: None,
                    work_assignment_id: None,
                    election_id: None,
                    participant_id: Some(command.expected_leader),
                    leadership_revision: updated.leadership_revision,
                    recorded_at_ms: command.now_ms,
                },
            )?;
            Ok((command.scope.workspace_id(), updated))
        })
    }

    /// Demand failover for one stabilizing, active, or handoff-coordinating
    /// scope. This bypasses the stabilization lock (and the handoff
    /// coordination gate); no arbitrary winner is produced and the incumbent
    /// stays recorded for reference.
    ///
    /// # Errors
    /// Rejects unknown scopes, non-failable states, stale revisions, and
    /// uncertain commits.
    #[cfg(test)]
    fn mark_failover(
        &mut self,
        command: MarkFailoverCommand,
    ) -> Result<LeadershipMutationReceipt, OperationsError> {
        self.leadership_transaction(move |tx| {
            let row = load_leadership(tx, command.scope)?
                .ok_or(OperationsError::Conflict("leadership scope missing"))?;
            require_revision(&row, command.expected_revision)?;
            require_failable(&row)?;
            let updated = transition_to_failover(tx, &row, command.now_ms)?;
            Ok((command.scope.workspace_id(), updated))
        })
    }

    /// Fail over only when the incumbent's durable authenticated presence is
    /// older than `LEADER_UNRESPONSIVE_AFTER_MS`. Missing presence is not
    /// affirmative stale evidence, so it is a deterministic no-op.
    ///
    /// # Errors
    /// Rejects unknown scopes, non-failable states, stale revisions, corrupt
    /// leader identity, persistence failures, and uncertain commits.
    pub fn mark_failover_if_unresponsive(
        &mut self,
        command: MarkUnresponsiveFailoverCommand,
    ) -> Result<Option<LeadershipMutationReceipt>, OperationsError> {
        self.leadership_transaction(move |tx| {
            let row = load_leadership(tx, command.scope)?
                .ok_or(OperationsError::Conflict("leadership scope missing"))?;
            require_revision(&row, command.expected_revision)?;
            require_failable(&row)?;
            let leader = row
                .leader_participant_id
                .ok_or(OperationsError::Corrupt("leadership leader missing"))?;
            let last_authenticated_at_ms: Option<i64> = tx
                .query_row(
                    "SELECT last_authenticated_at_ms FROM ops_logical_presence WHERE workspace_id=?1 AND participant_id=?2",
                    params![command.scope.workspace_id().into_uuid().as_bytes(), leader.as_bytes()],
                    |row| row.get(0),
                )
                .optional()?;
            let Some(last_authenticated_at_ms) = last_authenticated_at_ms else {
                return Ok((command.scope.workspace_id(), None));
            };
            if command.now_ms.saturating_sub(last_authenticated_at_ms)
                <= LEADER_UNRESPONSIVE_AFTER_MS
            {
                return Ok((command.scope.workspace_id(), None));
            }
            let updated = transition_to_failover(tx, &row, command.now_ms)?;
            Ok((command.scope.workspace_id(), Some(updated)))
        })
    }

    /// Mark one failed-over scope vacant when no successor exists.
    ///
    /// # Errors
    /// Rejects unknown scopes, non-failed-over states, stale revisions, and
    /// uncertain commits.
    #[cfg(test)]
    fn mark_vacant(
        &mut self,
        command: MarkVacantCommand,
    ) -> Result<LeadershipMutationReceipt, OperationsError> {
        self.leadership_transaction(move |tx| {
            let row = load_leadership(tx, command.scope)?
                .ok_or(OperationsError::Conflict("leadership scope missing"))?;
            require_revision(&row, command.expected_revision)?;
            if row.state != LeadershipState::FailoverRequired {
                return Err(OperationsError::Conflict("leadership not failed over"));
            }
            let updated = cas_leadership(
                tx,
                command.scope,
                row.leadership_revision,
                LeadershipUpdate {
                    state: LeadershipState::Vacant,
                    leader_participant_id: None,
                    stabilization_until_ms: None,
                    current_election_id: None,
                    now_ms: command.now_ms,
                },
            )?
            .ok_or(OperationsError::Conflict("leadership revision"))?;
            append_receipt(
                tx,
                command.scope,
                &ReceiptRow {
                    kind: ReceiptKind::Vacated,
                    provenance_activity: None,
                    work_assignment_id: None,
                    election_id: None,
                    participant_id: row.leader_participant_id,
                    leadership_revision: updated.leadership_revision,
                    recorded_at_ms: command.now_ms,
                },
            )?;
            Ok((command.scope.workspace_id(), updated))
        })
    }

    /// Retry one initially deferred leadership bootstrap when a trusted Ready
    /// activation makes the exact target Agent resolvable. Only absent scopes
    /// are seeded; historical offers never revive an existing LEADERLESS,
    /// VACANT, or otherwise-established leadership row. The offer is bounded
    /// to the activation's exact migration/workspace/activity/Agent and the
    /// earliest source sequence wins, matching normal effect-fact ordering.
    ///
    /// # Errors
    /// Rejects corrupt offer identities, a Ready participant mismatch,
    /// persistence failures, and uncertain leadership commits.
    pub fn bootstrap_deferred_after_activation(
        &mut self,
        activation: &RegistrationActivationReceipt,
    ) -> Result<Vec<LeadershipMutationReceipt>, OperationsError> {
        let workspace = activation.workspace_id();
        let activity = activation.binding().activity_id();
        let agent = activation.binding().agent_id();
        let migration = activation.migration_id();
        let participant = activation.participant_id();
        self.leadership_transaction(move |tx| {
            let activity_scope = LeadershipScope::activity(workspace, activity);
            let workspace_scope = LeadershipScope::workspace(workspace);
            let activity_missing = load_leadership(tx, activity_scope)?.is_none();
            let workspace_missing = load_leadership(tx, workspace_scope)?.is_none();
            let offer: Option<(Vec<u8>, i64)> = tx
                .query_row(
                    "SELECT substr(assignment_id,1,17),offered_at_ms FROM ops_assignment_offer_projection WHERE migration_id=?1 AND workspace_id=?2 AND activity_id=?3 AND target_agent_id=?4 ORDER BY source_sequence ASC LIMIT 1",
                    params![
                        migration.as_bytes(),
                        workspace.into_uuid().as_bytes(),
                        activity.into_uuid().as_bytes(),
                        agent.into_uuid().as_bytes(),
                    ],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            let Some((assignment_raw, offered_at_ms)) = offer else {
                return Ok((workspace, Vec::new()));
            };
            let assignment = Uuid::from_bytes(wire::id(assignment_raw)?);
            if assignment.is_nil() {
                return Err(OperationsError::Corrupt("leadership deferred assignment"));
            }
            let resolved = resolve_offer_participant(tx, workspace, activity, agent)?
                .ok_or(OperationsError::Conflict("leadership deferred participant"))?;
            if resolved != participant {
                return Err(OperationsError::Conflict("leadership deferred participant"));
            }
            let assignment_id = AgentAssignmentId::from_uuid(assignment);
            let mut receipts = Vec::with_capacity(2);
            if activity_missing
                && let Some(receipt) = bootstrap_scope(
                    tx,
                    activity_scope,
                    participant,
                    assignment_id,
                    Some(activity),
                    offered_at_ms,
                )?
            {
                receipts.push(receipt);
            }
            if workspace_missing
                && let Some(receipt) = bootstrap_scope(
                    tx,
                    workspace_scope,
                    participant,
                    assignment_id,
                    Some(activity),
                    offered_at_ms,
                )?
            {
                receipts.push(receipt);
            }
            let team_offers = {
                let mut query = tx.prepare("SELECT substr(o.assignment_id,1,17),o.offered_at_ms FROM ops_assignment_offer_projection o JOIN ops_assignment_team_affinities a ON a.workspace_id=o.workspace_id AND a.assignment_id=o.assignment_id AND a.activity_id=o.activity_id AND a.target_agent_id=o.target_agent_id AND a.target_session_id=o.target_session_id WHERE o.migration_id=?1 AND o.workspace_id=?2 AND o.activity_id=?3 AND o.target_agent_id=?4 AND a.team_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM ops_leadership l WHERE l.workspace_id=a.workspace_id AND l.scope_kind=2 AND l.team_id=a.team_id) AND o.source_sequence=(SELECT MIN(o2.source_sequence) FROM ops_assignment_offer_projection o2 JOIN ops_assignment_team_affinities a2 ON a2.workspace_id=o2.workspace_id AND a2.assignment_id=o2.assignment_id WHERE o2.migration_id=o.migration_id AND o2.activity_id=o.activity_id AND o2.target_agent_id=o.target_agent_id AND a2.team_id=a.team_id) ORDER BY o.source_sequence LIMIT 65")?;
                query.query_map(params![migration.as_bytes(),workspace.into_uuid().as_bytes(),activity.into_uuid().as_bytes(),agent.into_uuid().as_bytes()], |row| Ok((row.get::<_,Vec<u8>>(0)?,row.get::<_,i64>(1)?)))?.collect::<Result<Vec<_>,_>>()?
            };
            if team_offers.len()>64 { return Err(OperationsError::Bounds("deferred Team leadership scopes")); }
            for (assignment, offered_at_ms) in team_offers {
                let assignment_id = AgentAssignmentId::from_uuid(Uuid::from_bytes(wire::id(assignment)?));
                let observation = OfferBootstrapObservation { workspace_id:workspace, activity_id:activity, assignment_id, target_agent_id:agent, offered_at_ms };
                if let Some(team) = admitted_team_for_offer(tx,observation,participant)?
                    && let Some(receipt) = bootstrap_scope(tx,LeadershipScope::team(workspace,team),participant,assignment_id,Some(activity),offered_at_ms)? {
                    receipts.push(receipt);
                }
            }
            Ok((workspace, receipts))
        })
    }

    /// Confirm the pending bootstrap using only the durable BOOTSTRAP receipt
    /// for this exact scope/revision. The caller does not supply an assignment
    /// identity, so a stale or foreign offer cannot be substituted.
    ///
    /// # Errors
    /// Rejects unknown/non-bootstrapping scopes, stale revisions, missing or
    /// corrupt bootstrap provenance, lost offer evidence, and uncertain commits.
    pub fn confirm_pending_bootstrap(
        &mut self,
        command: ConfirmPendingBootstrapCommand,
    ) -> Result<LeadershipMutationReceipt, OperationsError> {
        let assignment_id = {
            let tx = self.connection.unchecked_transaction()?;
            let row = load_leadership(&tx, command.scope)?
                .ok_or(OperationsError::Conflict("leadership scope missing"))?;
            require_revision(&row, command.expected_revision)?;
            if row.state != LeadershipState::Bootstrapping {
                return Err(OperationsError::Conflict("leadership not bootstrapping"));
            }
            let assignment_id = pending_bootstrap_assignment(&tx, &row)?
                .ok_or(OperationsError::Corrupt("leadership bootstrap receipt"))?;
            catalog::commit_read(tx, self.destination)?;
            assignment_id
        };
        self.confirm_bootstrap_evidence(ConfirmBootstrapEvidenceCommand {
            scope: command.scope,
            assignment_id,
            expected_revision: command.expected_revision,
            now_ms: command.now_ms,
        })
    }

    /// Confirm one bootstrapped scope's offer evidence by durable readback:
    /// the assignment-offer projection must still exist, its target must
    /// still resolve to the recorded leader, and the scope must be
    /// `BOOTSTRAPPING` at the expected revision. Success promotes the scope
    /// to `STABILIZING` with a fresh stabilization window anchored at the
    /// confirmation time. A failed readback returns the scope to `LEADERLESS`
    /// — no authority is ever fabricated from lost evidence — and surfaces
    /// the typed error after the evidence is durably recorded.
    ///
    /// # Errors
    /// Rejects unknown scopes, non-bootstrapping states, stale revisions, a
    /// lost or retargeted offer projection, and uncertain commits.
    pub fn confirm_bootstrap_evidence(
        &mut self,
        command: ConfirmBootstrapEvidenceCommand,
    ) -> Result<LeadershipMutationReceipt, OperationsError> {
        let internal = self.confirm_bootstrap_evidence_transaction(command)?;
        match internal {
            InternalBootstrapConfirm::Confirmed(receipt) => Ok(receipt),
            // The LEADERLESS fallback and its receipt are already durably
            // committed; the lost evidence is the honest failure surface.
            InternalBootstrapConfirm::EvidenceLost(error) => Err(error),
        }
    }

    fn confirm_bootstrap_evidence_transaction(
        &mut self,
        command: ConfirmBootstrapEvidenceCommand,
    ) -> Result<InternalBootstrapConfirm, OperationsError> {
        self.leadership_transaction(move |tx| {
            let row = load_leadership(tx, command.scope)?
                .ok_or(OperationsError::Conflict("leadership scope missing"))?;
            require_revision(&row, command.expected_revision)?;
            if row.state != LeadershipState::Bootstrapping {
                return Err(OperationsError::Conflict("leadership not bootstrapping"));
            }
            let evidence_failure = match load_offer_projection(
                tx,
                command.scope.workspace_id(),
                command.assignment_id,
            ) {
                Err(error) => Some(error),
                Ok(None) => Some(OperationsError::Conflict("leadership bootstrap evidence")),
                Ok(Some(offer)) => {
                    (!bootstrap_offer_matches_scope(tx, command, offer, row.leader_participant_id)?)
                        .then_some(OperationsError::Conflict("leadership bootstrap evidence"))
                }
            };
            if let Some(error) = evidence_failure {
                let updated = cas_leadership(
                    tx,
                    command.scope,
                    row.leadership_revision,
                    LeadershipUpdate {
                        state: LeadershipState::Leaderless,
                        leader_participant_id: None,
                        stabilization_until_ms: None,
                        current_election_id: None,
                        now_ms: command.now_ms,
                    },
                )?
                .ok_or(OperationsError::Conflict("leadership revision"))?;
                append_receipt(
                    tx,
                    command.scope,
                    &ReceiptRow {
                        kind: ReceiptKind::Vacated,
                        provenance_activity: None,
                        work_assignment_id: Some(command.assignment_id),
                        election_id: None,
                        participant_id: row.leader_participant_id,
                        leadership_revision: updated.leadership_revision,
                        recorded_at_ms: command.now_ms,
                    },
                )?;
                return Ok((
                    command.scope.workspace_id(),
                    InternalBootstrapConfirm::EvidenceLost(error),
                ));
            }
            let until = command
                .now_ms
                .checked_add(STABILIZATION_WINDOW_MS)
                .ok_or(OperationsError::Bounds("leadership stabilization window"))?;
            let updated = cas_leadership(
                tx,
                command.scope,
                row.leadership_revision,
                LeadershipUpdate {
                    state: LeadershipState::Stabilizing,
                    leader_participant_id: row.leader_participant_id,
                    stabilization_until_ms: Some(until),
                    current_election_id: None,
                    now_ms: command.now_ms,
                },
            )?
            .ok_or(OperationsError::Conflict("leadership revision"))?;
            append_receipt(
                tx,
                command.scope,
                &ReceiptRow {
                    kind: ReceiptKind::Confirmed,
                    provenance_activity: None,
                    work_assignment_id: None,
                    election_id: None,
                    participant_id: row.leader_participant_id,
                    leadership_revision: updated.leadership_revision,
                    recorded_at_ms: command.now_ms,
                },
            )?;
            Ok((
                command.scope.workspace_id(),
                InternalBootstrapConfirm::Confirmed(updated),
            ))
        })
    }

    /// Park one leading or authority-pending scope in `RECOVERY_REQUIRED`
    /// after an unknown commit/readback outcome or an irreconcilable
    /// `AUTHORITY_PENDING` pointer. Every recorded fact is preserved: the
    /// leader, the stabilization window and the election pointer stay
    /// exactly as they were so the reconcile can resolve per recorded facts.
    ///
    /// # Errors
    /// Rejects unknown scopes, non-recoverable states, stale revisions, and
    /// uncertain commits.
    pub fn mark_recovery_required(
        &mut self,
        command: MarkRecoveryRequiredCommand,
    ) -> Result<LeadershipMutationReceipt, OperationsError> {
        self.leadership_transaction(move |tx| {
            let row = load_leadership(tx, command.scope)?
                .ok_or(OperationsError::Conflict("leadership scope missing"))?;
            require_revision(&row, command.expected_revision)?;
            if !matches!(
                row.state,
                LeadershipState::AuthorityPending
                    | LeadershipState::Stabilizing
                    | LeadershipState::Active
            ) {
                return Err(OperationsError::Conflict("leadership not recoverable"));
            }
            let updated = cas_leadership(
                tx,
                command.scope,
                row.leadership_revision,
                LeadershipUpdate {
                    state: LeadershipState::RecoveryRequired,
                    leader_participant_id: row.leader_participant_id,
                    stabilization_until_ms: row.stabilization_until_ms,
                    current_election_id: row.current_election_id,
                    now_ms: command.now_ms,
                },
            )?
            .ok_or(OperationsError::Conflict("leadership revision"))?;
            append_receipt(
                tx,
                command.scope,
                &ReceiptRow {
                    kind: ReceiptKind::Recovery,
                    provenance_activity: None,
                    work_assignment_id: None,
                    election_id: row.current_election_id,
                    participant_id: row.leader_participant_id,
                    leadership_revision: updated.leadership_revision,
                    recorded_at_ms: command.now_ms,
                },
            )?;
            invalidate_workspace_election_for_team_change(tx, command.scope, command.now_ms)?;
            Ok((command.scope.workspace_id(), updated))
        })
    }

    /// Reconcile one recovery from the same-identity durable readback: the
    /// recorded confirm intent plus the preserved leadership row. A
    /// `committed` intent proves the fenced confirm landed, so the scope
    /// returns to `STABILIZING` under the elected leader with the committed
    /// window. A terminal `superseded`/`rejected` intent resolves per the
    /// recorded facts: a still-related scope requires failover, or becomes
    /// `VACANT` when no leader remains. A still-live intent
    /// (`pending`/`remote_recorded`) leaves the scope in
    /// `RECOVERY_REQUIRED` with the observation appended to the receipt
    /// history — never a competing takeover.
    ///
    /// # Errors
    /// Rejects unknown scopes, non-recovery states, stale revisions, an
    /// unknown or foreign intent, fact corruption, and uncertain commits.
    pub fn reconcile_recovery(
        &mut self,
        command: ReconcileRecoveryCommand,
    ) -> Result<LeadershipRecoveryResolution, OperationsError> {
        self.leadership_transaction(move |tx| {
            let row = load_leadership(tx, command.scope)?
                .ok_or(OperationsError::Conflict("leadership scope missing"))?;
            require_revision(&row, command.expected_revision)?;
            if row.state != LeadershipState::RecoveryRequired {
                return Err(OperationsError::Conflict("leadership not recovering"));
            }
            let intent = load_confirm_intent(tx, command.intent_request_id)?
                .ok_or(OperationsError::Conflict("leadership recovery intent"))?;
            if intent.scope != command.scope {
                return Err(OperationsError::Conflict("leadership recovery intent"));
            }
            let resolution = match intent.state {
                LeadershipConfirmIntentState::Committed => {
                    let proof = unique_committed_recovery_intent(tx, &row)?
                        .ok_or(OperationsError::Conflict("leadership recovery facts"))?;
                    if proof.request_id != intent.request_id {
                        return Err(OperationsError::Conflict("leadership recovery facts"));
                    }
                    LeadershipRecoveryResolution::ConfirmCompleted(reconcile_committed_recovery(
                        tx,
                        &row,
                        &intent,
                        command.now_ms,
                    )?)
                }
                LeadershipConfirmIntentState::Superseded
                | LeadershipConfirmIntentState::Rejected => LeadershipRecoveryResolution::Resolved(
                    reconcile_terminal_recovery(tx, &row, &intent, command.now_ms)?,
                ),
                LeadershipConfirmIntentState::Pending
                | LeadershipConfirmIntentState::RemoteRecorded => {
                    LeadershipRecoveryResolution::Unresolved(observe_live_recovery(
                        tx,
                        &row,
                        command.now_ms,
                    )?)
                }
            };
            Ok((command.scope.workspace_id(), resolution))
        })
    }

    /// Read one scope's leadership view. Reading never mutates state and a
    /// scope without any leadership row reads as `None`.
    ///
    /// # Errors
    /// Rejects corrupt persisted state and database read failures.
    pub fn leadership_view(
        &self,
        workspace_id: WorkspaceId,
        activity_id: Option<ActivityId>,
        now_ms: i64,
    ) -> Result<Option<LeadershipView>, OperationsError> {
        let scope = match activity_id {
            Some(activity_id) => LeadershipScope::activity(workspace_id, activity_id),
            None => LeadershipScope::workspace(workspace_id),
        };
        self.leadership_scope_view(scope, now_ms)
    }

    /// Read one exact leadership scope, including Team scopes. Reading never
    /// mutates state and a scope without any leadership row reads as `None`.
    ///
    /// # Errors
    /// Rejects corrupt persisted state and database read failures.
    pub fn leadership_scope_view(
        &self,
        scope: LeadershipScope,
        now_ms: i64,
    ) -> Result<Option<LeadershipView>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let row = load_leadership(&transaction, scope)?;
        let view = match row {
            None => None,
            Some(row) => {
                let current_election = match row.current_election_id {
                    None => None,
                    Some(election_id) => Some(
                        election_summary(&transaction, election_id, now_ms)?
                            .ok_or(OperationsError::Corrupt("leadership election pointer"))?,
                    ),
                };
                let leader_display_name = match row.leader_participant_id {
                    None => None,
                    Some(leader) => {
                        member_display_name(&transaction, scope.workspace_id(), leader)?
                    }
                };
                let identity = match row.leader_participant_id {
                    None => None,
                    Some(leader) => transaction.query_row(
                        "SELECT substr(m.participation_id,1,17),substr(m.agent_profile_id,1,17) FROM ops_workspace_participation_mappings m JOIN ops_workspace_participations p ON p.workspace_id=m.workspace_id AND p.participation_id=m.participation_id AND p.agent_profile_id=m.agent_profile_id WHERE m.workspace_id=?1 AND m.participant_id=?2",
                        params![scope.workspace_id().into_uuid().as_bytes(),leader.as_bytes()],
                        |record| Ok((record.get::<_,Vec<u8>>(0)?,record.get::<_,Vec<u8>>(1)?)),
                    ).optional()?.map(|(participation,profile)| Ok::<_,OperationsError>((WorkspaceParticipationId::from_uuid(Uuid::from_bytes(wire::id(participation)?))?,Uuid::from_bytes(wire::id(profile)?)))).transpose()?,
                };
                Some((row, leader_display_name, current_election, identity))
            }
        };
        catalog::commit_read(transaction, self.destination)?;
        Ok(view.map(
            |(row, leader_display_name, current_election, identity)| LeadershipView {
                workspace_id: row.scope.workspace_id(),
                activity_id: row.scope.activity_id(),
                team_id: row.scope.team_id(),
                state: row.state,
                leader_participant_id: row.leader_participant_id,
                leader_display_name,
                leader_participation_id: identity.map(|pair| pair.0),
                leader_profile_id: identity.map(|pair| pair.1),
                leadership_revision: row.leadership_revision,
                stabilization_until_ms: row.stabilization_until_ms,
                current_election,
                updated_at_ms: row.updated_at_ms,
            },
        ))
    }

    /// Read the exact current leader authority for one Workspace, Activity, or
    /// Team scope. The returned participant is absent when the shared authority
    /// predicate fails, even if the leadership row still names a historical incumbent.
    ///
    /// # Errors
    /// Rejects corrupt leadership rows and database read failures.
    pub fn current_leadership_authority(
        &self,
        scope: LeadershipScope,
    ) -> Result<Option<CurrentLeadershipAuthority>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let authority = current_leadership_authority_in_transaction(&transaction, scope)?;
        catalog::commit_read(transaction, self.destination)?;
        Ok(authority)
    }

    fn start_election_internal(
        &mut self,
        command: RecallElectionCommand,
        required: LeadershipState,
    ) -> Result<RecallElectionReceipt, OperationsError> {
        self.leadership_transaction(move |tx| {
            let receipt = start_election_transaction(tx, command, required)?;
            Ok((command.scope.workspace_id(), receipt))
        })
    }

    /// Run one leadership mutation in one IMMEDIATE transaction; the closure
    /// resolves the owning workspace so an uncertain commit reports the exact
    /// scope identity.
    fn leadership_transaction<T, F>(&mut self, operation: F) -> Result<T, OperationsError>
    where
        F: FnOnce(&Transaction<'_>) -> Result<(WorkspaceId, T), OperationsError>,
    {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (workspace_id, result) = operation(&tx)?;
        tx.commit()
            .map_err(|source| OperationsError::LeadershipCommitUnconfirmed {
                workspace_id: workspace_id.into_uuid(),
                source,
            })?;
        Ok(result)
    }
}

fn start_election_transaction(
    tx: &Transaction<'_>,
    command: RecallElectionCommand,
    required: LeadershipState,
) -> Result<RecallElectionReceipt, OperationsError> {
    if let Some(team_id) = command.scope.team_id() {
        crate::teams::ensure_team_formed(tx, command.scope.workspace_id(), team_id)?;
    }
    let row = load_leadership(tx, command.scope)?
        .ok_or(OperationsError::Conflict("leadership scope missing"))?;
    require_revision(&row, command.expected_revision)?;
    if row.state != required {
        return Err(OperationsError::Conflict("leadership not electable"));
    }
    if row.current_election_id.is_some() {
        return Err(OperationsError::Corrupt("leadership election pointer"));
    }
    let candidate_pool = election_subject::snapshot_ready_subjects_for_scope(tx, command.scope)?;
    if !candidate_pool.contains_participant(command.candidate_participant_id) {
        return Err(OperationsError::Conflict("leadership candidate member"));
    }
    let electorate =
        election_subject::snapshot_voting_subjects_for_scope(tx, command.scope, command.now_ms)?;
    let qualification = candidate_work_credit_qualification(
        tx,
        command.scope,
        command.candidate_participant_id,
        command.now_ms,
    )?;
    let next_revision = row
        .leadership_revision
        .checked_add(1)
        .ok_or(OperationsError::Bounds("leadership revision"))?;
    let election_id = election_identity(command.scope, next_revision);
    let deadline = command
        .now_ms
        .checked_add(BALLOT_WINDOW_MS)
        .ok_or(OperationsError::Bounds("leadership ballot deadline"))?;
    tx.execute(
                "INSERT INTO ops_leadership_elections(election_id,workspace_id,scope_kind,activity_id,team_id,kind,state,incumbent_participant_id,candidate_participant_id,ballot_deadline_ms,elected_participant_id,tally_yes,tally_no,tally_abstain,created_at_ms,closed_at_ms) VALUES(?1,?2,?3,?4,?5,'RECALL','ELECTING',?6,?7,?8,NULL,NULL,NULL,NULL,?9,NULL)",
                params![
                    election_id.as_bytes(),
                    command.scope.workspace_id().into_uuid().as_bytes(),
                    command.scope.scope_kind(),
                    election_activity_param(command.scope),
                    election_team_param(command.scope),
                    row.leader_participant_id.map(|value| value.as_bytes().to_vec()),
                    command.candidate_participant_id.as_bytes(),
                    deadline,
                    command.now_ms,
                ],
            )?;
    let _work_credit_snapshot = work_credit_subject::persist_election_snapshot(
        tx,
        work_credit_subject::WorkCreditElectionSnapshotCommand {
            election_id,
            candidate_participant_id: command.candidate_participant_id,
            candidate_subject: qualification.subject,
            candidate_source_participant_count: qualification.source_participant_count,
            candidate_total_credits: qualification.total_credits,
            qualified: true,
            evaluated_at_ms: command.now_ms,
        },
        &qualification.snapshot,
    )?;
    let source_leadership_revision = u64::try_from(row.leadership_revision)
        .map_err(|_| OperationsError::Corrupt("leadership revision"))?;
    let result_leadership_revision =
        u64::try_from(next_revision).map_err(|_| OperationsError::Bounds("leadership revision"))?;
    let subject_cutover = election_subject::persist_cutover(
        tx,
        election_subject::ElectionSubjectCutoverCommand {
            election_id,
            workspace_id: command.scope.workspace_id(),
            activity_id: command.scope.activity_id(),
            team_id: command.scope.team_id(),
            source_leadership_revision,
            result_leadership_revision,
            created_at_ms: command.now_ms,
        },
        &electorate,
    )?;
    let _ = cas_leadership(
        tx,
        command.scope,
        row.leadership_revision,
        LeadershipUpdate {
            state: LeadershipState::Electing,
            leader_participant_id: row.leader_participant_id,
            stabilization_until_ms: None,
            current_election_id: Some(election_id),
            now_ms: command.now_ms,
        },
    )?
    .ok_or(OperationsError::Conflict("leadership revision"))?;
    Ok(RecallElectionReceipt {
        election_id,
        ballot_deadline_ms: deadline,
        electorate_size: i64::try_from(subject_cutover.voter_subject_count())
            .map_err(|_| OperationsError::Bounds("leadership electorate"))?,
    })
}

/// Remove current leadership authority owned by one logical participant as
/// part of the same transaction that commits its departure. This never picks
/// a successor: elections and Broker-fenced confirmation remain the only
/// replacement authority path. Pending elections/confirmations may continue,
/// but the departed incumbent is no longer represented as current leader.
pub(crate) fn reconcile_departing_leader(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    participant_id: Uuid,
    now_ms: i64,
) -> Result<(), OperationsError> {
    let mut scopes = vec![
        LeadershipScope::activity(workspace_id, activity_id),
        LeadershipScope::workspace(workspace_id),
    ];
    let teams = {
        let mut query = tx.prepare("SELECT substr(team_id,1,17) FROM ops_leadership WHERE workspace_id=?1 AND scope_kind=2 AND leader_participant_id=?2 ORDER BY team_id LIMIT 65")?;
        query
            .query_map(
                params![
                    workspace_id.into_uuid().as_bytes(),
                    participant_id.as_bytes()
                ],
                |row| row.get::<_, Vec<u8>>(0),
            )?
            .collect::<Result<Vec<_>, _>>()?
    };
    if teams.len() > 64 {
        return Err(OperationsError::Bounds(
            "participant Team leadership scopes",
        ));
    }
    for team in teams {
        scopes.push(LeadershipScope::team(
            workspace_id,
            Uuid::from_bytes(wire::id(team)?),
        ));
    }
    for scope in scopes {
        let Some(row) = load_leadership(tx, scope)? else {
            continue;
        };
        if row.leader_participant_id == Some(participant_id) {
            vacate_departing_scope(tx, &row, Some(activity_id), now_ms)?;
        }
    }
    Ok(())
}

/// Preserve the frozen denominator, but cancel an open Workspace decision
/// when a voter no longer has any confirmed Team leadership. Call only inside
/// the same IMMEDIATE transaction as the Team authority change.
fn invalidate_workspace_election_for_team_change(
    tx: &Transaction<'_>,
    changed_scope: LeadershipScope,
    now_ms: i64,
) -> Result<(), OperationsError> {
    if changed_scope.team_id().is_none() {
        return Ok(());
    }
    let scope = LeadershipScope::workspace(changed_scope.workspace_id());
    let Some(row) = load_leadership(tx, scope)? else {
        return Ok(());
    };
    if row.state != LeadershipState::Electing {
        return Ok(());
    }
    let election_id = row
        .current_election_id
        .ok_or(OperationsError::Corrupt("Workspace election pointer"))?;
    let election = load_election(tx, election_id)?
        .ok_or(OperationsError::Corrupt("Workspace election missing"))?;
    if election.scope != scope || election.state != ElectionState::Electing {
        return Err(OperationsError::Corrupt("Workspace election authority"));
    }
    if !election_subject::workspace_electorate_lost_team_leadership(
        tx,
        scope.workspace_id(),
        election_id,
    )? {
        return Ok(());
    }
    let incumbent = match row.leader_participant_id {
        Some(participant) => {
            let ready: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM ops_current_effect_bindings b
                 JOIN ops_logical_members m ON m.workspace_id=b.workspace_id
                     AND m.participant_id=b.participant_id AND m.session_id=b.session_id
                 WHERE b.workspace_id=?1 AND b.participant_id=?2)",
                params![
                    scope.workspace_id().into_uuid().as_bytes(),
                    participant.as_bytes()
                ],
                |record| record.get(0),
            )?;
            ready.then_some(participant)
        }
        None => None,
    };
    let active = incumbent.is_some() && !election_originated_from_failover(tx, &row)?;
    let (state, leader, receipt_kind) = if active {
        (LeadershipState::Active, incumbent, ReceiptKind::Recovery)
    } else {
        (
            LeadershipState::FailoverRequired,
            incumbent,
            ReceiptKind::Failover,
        )
    };
    let revision = row
        .leadership_revision
        .checked_add(1)
        .ok_or(OperationsError::Bounds("leadership revision"))?;
    let changed = tx.execute(
        "UPDATE ops_leadership SET state=?1,leader_participant_id=?2,leadership_revision=?3,
             stabilization_until_ms=NULL,current_election_id=NULL,updated_at_ms=?4
         WHERE workspace_id=?5 AND scope_kind=0 AND activity_id IS NULL AND team_id IS NULL
             AND state='ELECTING' AND current_election_id=?6 AND leadership_revision=?7",
        params![
            state.encode(),
            leader.map(|id| id.as_bytes().to_vec()),
            revision,
            now_ms,
            scope.workspace_id().into_uuid().as_bytes(),
            election_id.as_bytes(),
            row.leadership_revision
        ],
    )?;
    if changed != 1 {
        return Err(OperationsError::Conflict("Workspace election revision"));
    }
    let changed = tx.execute("UPDATE ops_leadership_elections SET state='CANCELLED',closed_at_ms=?2 WHERE election_id=?1 AND state='ELECTING'",
        params![election_id.as_bytes(),now_ms])?;
    if changed != 1 {
        return Err(OperationsError::Conflict("Workspace election state"));
    }
    append_receipt(
        tx,
        scope,
        &ReceiptRow {
            kind: receipt_kind,
            provenance_activity: None,
            work_assignment_id: None,
            election_id: Some(election_id),
            participant_id: leader,
            leadership_revision: revision,
            recorded_at_ms: now_ms,
        },
    )?;
    Ok(())
}

fn vacate_departing_scope(
    tx: &Transaction<'_>,
    row: &LeadershipRow,
    provenance_activity: Option<ActivityId>,
    now_ms: i64,
) -> Result<(), OperationsError> {
    let scope = row.scope;
    let (state, current_election_id, receipt_kind) = match row.state {
        LeadershipState::Bootstrapping => (LeadershipState::Leaderless, None, ReceiptKind::Vacated),
        LeadershipState::Stabilizing | LeadershipState::Active => (
            LeadershipState::FailoverRequired,
            None,
            ReceiptKind::Failover,
        ),
        LeadershipState::Electing | LeadershipState::AuthorityPending => {
            (row.state, row.current_election_id, ReceiptKind::Vacated)
        }
        LeadershipState::FailoverRequired => (
            LeadershipState::FailoverRequired,
            row.current_election_id,
            ReceiptKind::Vacated,
        ),
        LeadershipState::Leaderless
        | LeadershipState::Vacant
        | LeadershipState::RecoveryRequired => return Ok(()),
    };
    let updated = cas_leadership(
        tx,
        scope,
        row.leadership_revision,
        LeadershipUpdate {
            state,
            leader_participant_id: None,
            stabilization_until_ms: None,
            current_election_id,
            now_ms,
        },
    )?
    .ok_or(OperationsError::Conflict("leadership revision"))?;
    append_receipt(
        tx,
        scope,
        &ReceiptRow {
            kind: receipt_kind,
            provenance_activity,
            work_assignment_id: None,
            election_id: row.current_election_id,
            participant_id: row.leader_participant_id,
            leadership_revision: updated.leadership_revision,
            recorded_at_ms: now_ms,
        },
    )?;
    invalidate_workspace_election_for_team_change(tx, scope, now_ms)?;
    Ok(())
}

/// Removing a Team membership revokes only its leadership role. Work and
/// leases stay with Forge and are never moved to another Agent implicitly.
pub(crate) fn reconcile_team_departure(
    tx: &Transaction<'_>,
    workspace: WorkspaceId,
    team: Uuid,
    participation: WorkspaceParticipationId,
    now_ms: i64,
) -> Result<(), OperationsError> {
    let Some(row) = load_leadership(tx, LeadershipScope::team(workspace, team))? else {
        return Ok(());
    };
    let Some(leader) = row.leader_participant_id else {
        return Ok(());
    };
    let departing: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM ops_workspace_participation_mappings WHERE workspace_id=?1 AND participation_id=?2 AND participant_id=?3)",params![workspace.into_uuid().as_bytes(),participation.into_uuid().as_bytes(),leader.as_bytes()],|row|row.get(0))?;
    if departing {
        vacate_departing_scope(tx, &row, None, now_ms)?;
    }
    Ok(())
}

fn scope_has_live_confirm_intent(
    tx: &Transaction<'_>,
    scope: LeadershipScope,
) -> Result<bool, OperationsError> {
    Ok(tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_leadership_confirm_intents
         WHERE workspace_id=?1 AND scope_kind=?2 AND activity_id IS ?3 AND team_id IS ?4
             AND state IN('pending','remote_recorded'))",
        params![
            scope.workspace_id().into_uuid().as_bytes(),
            scope.scope_kind(),
            election_activity_param(scope),
            election_team_param(scope)
        ],
        |row| row.get(0),
    )?)
}

pub(crate) fn retire_team_leadership(
    tx: &Transaction<'_>,
    workspace: WorkspaceId,
    team: Uuid,
    now_ms: i64,
) -> Result<(), OperationsError> {
    let scope = LeadershipScope::team(workspace, team);
    if scope_has_live_confirm_intent(tx, scope)? {
        return Err(OperationsError::Conflict(
            "Team retirement requires resolved leadership intent",
        ));
    }
    let Some(row) = load_leadership(tx, scope)? else {
        return Ok(());
    };
    if let Some(election_id) = row.current_election_id {
        let election = load_election(tx, election_id)?
            .ok_or(OperationsError::Corrupt("retired Team election"))?;
        if election.scope != scope {
            return Err(OperationsError::Corrupt("retired Team election scope"));
        }
        tx.execute("UPDATE ops_leadership_elections SET state='CANCELLED',closed_at_ms=?2 WHERE election_id=?1 AND state IN('ELECTING','CLOSED_ELECTED')",params![election_id.as_bytes(),now_ms])?;
    }
    let updated = cas_leadership(
        tx,
        scope,
        row.leadership_revision,
        LeadershipUpdate {
            state: LeadershipState::Vacant,
            leader_participant_id: None,
            stabilization_until_ms: None,
            current_election_id: None,
            now_ms,
        },
    )?
    .ok_or(OperationsError::Conflict("leadership revision"))?;
    append_receipt(
        tx,
        scope,
        &ReceiptRow {
            kind: ReceiptKind::Vacated,
            provenance_activity: None,
            work_assignment_id: None,
            election_id: row.current_election_id,
            participant_id: row.leader_participant_id,
            leadership_revision: updated.leadership_revision,
            recorded_at_ms: now_ms,
        },
    )?;
    invalidate_workspace_election_for_team_change(tx, scope, now_ms)?;
    Ok(())
}

/// Auto-bootstrap hook consumed inside the effect-fact page transaction: the
/// first offer by source sequence seeds each leaderless scope (the activity
/// scope for the offered activity, and the workspace orchestrator scope once
/// per workspace) into `BOOTSTRAPPING` with the resolved Ready participant as
/// the pending-evidence leader. Unresolvable participants defer the
/// bootstrap; the explicit command covers that case later. Concurrent offers
/// in one page: the first wins, later ones no-op. Evidence confirmation is
/// the separate `confirm_bootstrap_evidence` step.
pub(crate) fn bootstrap_from_offer(
    tx: &Transaction<'_>,
    observation: OfferBootstrapObservation,
) -> Result<(), OperationsError> {
    let Some(participant) = resolve_offer_participant(
        tx,
        observation.workspace_id,
        observation.activity_id,
        observation.target_agent_id,
    )?
    else {
        return Ok(());
    };
    let activity_scope =
        LeadershipScope::activity(observation.workspace_id, observation.activity_id);
    let _ = bootstrap_scope(
        tx,
        activity_scope,
        participant,
        observation.assignment_id,
        Some(observation.activity_id),
        observation.offered_at_ms,
    )?;
    let workspace_scope = LeadershipScope::workspace(observation.workspace_id);
    let _ = bootstrap_scope(
        tx,
        workspace_scope,
        participant,
        observation.assignment_id,
        Some(observation.activity_id),
        observation.offered_at_ms,
    )?;
    if let Some(team) = admitted_team_for_offer(tx, observation, participant)? {
        let _ = bootstrap_scope(
            tx,
            LeadershipScope::team(observation.workspace_id, team),
            participant,
            observation.assignment_id,
            Some(observation.activity_id),
            observation.offered_at_ms,
        )?;
    }
    Ok(())
}

fn bootstrap_offer_matches_scope(
    tx: &Transaction<'_>,
    command: ConfirmBootstrapEvidenceCommand,
    offer: OfferProjection,
    expected_leader: Option<Uuid>,
) -> Result<bool, OperationsError> {
    let Some(participant) = resolve_offer_participant(
        tx,
        command.scope.workspace_id(),
        offer.activity_id,
        offer.target_agent_id,
    )?
    else {
        return Ok(false);
    };
    if Some(participant) != expected_leader {
        return Ok(false);
    }
    if command
        .scope
        .activity_id()
        .is_some_and(|activity| activity != offer.activity_id)
    {
        return Ok(false);
    }
    let Some(team) = command.scope.team_id() else {
        return Ok(true);
    };
    let observed = OfferBootstrapObservation {
        workspace_id: command.scope.workspace_id(),
        activity_id: offer.activity_id,
        assignment_id: command.assignment_id,
        target_agent_id: offer.target_agent_id,
        offered_at_ms: command.now_ms,
    };
    Ok(admitted_team_for_offer(tx, observed, participant)? == Some(team))
}

/// An admission row alone is never work evidence. Match it to the actual
/// imported Forge offer, the immutable target and current logical Team member.
fn admitted_team_for_offer(
    tx: &Transaction<'_>,
    observation: OfferBootstrapObservation,
    participant: Uuid,
) -> Result<Option<Uuid>, OperationsError> {
    let Some(team) = crate::team_assignment::admitted_assignment_team(
        tx,
        observation.workspace_id,
        observation.assignment_id,
    )?
    else {
        return Ok(None);
    };
    let valid: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_assignment_team_affinities a JOIN ops_assignment_offer_projection o ON o.workspace_id=a.workspace_id AND o.assignment_id=a.assignment_id AND o.activity_id=a.activity_id AND o.target_agent_id=a.target_agent_id AND o.target_session_id=a.target_session_id JOIN ops_teams t ON t.workspace_id=a.workspace_id AND t.team_id=a.team_id AND t.state='ACTIVE' JOIN ops_team_members tm ON tm.workspace_id=a.workspace_id AND tm.team_id=a.team_id AND tm.participation_id=a.target_participation_id AND tm.state='JOINED' JOIN ops_workspace_participation_mappings m ON m.workspace_id=a.workspace_id AND m.participation_id=a.target_participation_id AND m.participant_id=?5 WHERE a.workspace_id=?1 AND a.assignment_id=?2 AND a.activity_id=?3 AND a.target_agent_id=?4 AND a.team_id=?6)",
        params![observation.workspace_id.into_uuid().as_bytes(),observation.assignment_id.into_uuid().as_bytes(),observation.activity_id.into_uuid().as_bytes(),observation.target_agent_id.into_uuid().as_bytes(),participant.as_bytes(),team.as_bytes()],
        |row| row.get(0),
    )?;
    Ok(valid.then_some(team))
}

/// Seed or re-seed one scope into `BOOTSTRAPPING` with the recorded offer as
/// pending evidence. Absent, `LEADERLESS` and `VACANT` scopes bootstrap; an
/// already-seeded scope is a concurrent-offer no-op (`Ok(None)`). The
/// stabilization window anchors only when `confirm_bootstrap_evidence`
/// durably confirms the offer projection, so the recorded leader never
/// stabilizes on fabricated authority.
fn bootstrap_scope(
    tx: &Transaction<'_>,
    scope: LeadershipScope,
    participant: Uuid,
    assignment_id: AgentAssignmentId,
    provenance_activity: Option<ActivityId>,
    anchor_ms: i64,
) -> Result<Option<LeadershipMutationReceipt>, OperationsError> {
    let updated = match load_leadership(tx, scope)? {
        None => {
            let row = LeadershipRow {
                scope,
                state: LeadershipState::Bootstrapping,
                leader_participant_id: Some(participant),
                leadership_revision: 1,
                stabilization_until_ms: None,
                current_election_id: None,
                updated_at_ms: anchor_ms,
            };
            insert_leadership(tx, &row)?;
            LeadershipMutationReceipt {
                scope,
                state: row.state,
                leader_participant_id: row.leader_participant_id,
                leadership_revision: row.leadership_revision,
                stabilization_until_ms: row.stabilization_until_ms,
                updated_at_ms: row.updated_at_ms,
            }
        }
        Some(row)
            if matches!(
                row.state,
                LeadershipState::Leaderless | LeadershipState::Vacant
            ) =>
        {
            cas_leadership(
                tx,
                scope,
                row.leadership_revision,
                LeadershipUpdate {
                    state: LeadershipState::Bootstrapping,
                    leader_participant_id: Some(participant),
                    stabilization_until_ms: None,
                    current_election_id: None,
                    now_ms: anchor_ms,
                },
            )?
            .ok_or(OperationsError::Conflict("leadership revision"))?
        }
        Some(_) => return Ok(None),
    };
    append_receipt(
        tx,
        scope,
        &ReceiptRow {
            kind: ReceiptKind::Bootstrap,
            provenance_activity,
            work_assignment_id: Some(assignment_id),
            election_id: None,
            participant_id: Some(participant),
            leadership_revision: updated.leadership_revision,
            recorded_at_ms: anchor_ms,
        },
    )?;
    Ok(Some(updated))
}

fn pending_bootstrap_assignment(
    tx: &Transaction<'_>,
    row: &LeadershipRow,
) -> Result<Option<AgentAssignmentId>, OperationsError> {
    let raw: Option<Option<Vec<u8>>> = match (row.scope.activity_id(), row.scope.team_id()) {
        (Some(activity_id), None) => tx
            .query_row(
                "SELECT work_assignment_id FROM ops_leadership_receipts WHERE workspace_id=?1 AND scope_kind=?2 AND activity_id=?3 AND team_id=?4 AND receipt_kind='BOOTSTRAP' AND leadership_revision=?5 ORDER BY receipt_ordinal DESC LIMIT 1",
                params![
                    row.scope.workspace_id().into_uuid().as_bytes(),
                    row.scope.scope_kind(),
                    activity_id.into_uuid().as_bytes(),
                    NO_ACTIVITY_MARKER,
                    row.leadership_revision,
                ],
                |sql_row| sql_row.get(0),
            )
            .optional()?,
        (None, Some(team_id)) => tx
            .query_row(
                "SELECT work_assignment_id FROM ops_leadership_receipts WHERE workspace_id=?1 AND scope_kind=?2 AND team_id=?3 AND receipt_kind='BOOTSTRAP' AND leadership_revision=?4 ORDER BY receipt_ordinal DESC LIMIT 1",
                params![
                    row.scope.workspace_id().into_uuid().as_bytes(),
                    row.scope.scope_kind(),
                    team_id.as_bytes(),
                    row.leadership_revision,
                ],
                |sql_row| sql_row.get(0),
            )
            .optional()?,
        (None, None) => tx
            .query_row(
                "SELECT work_assignment_id FROM ops_leadership_receipts WHERE workspace_id=?1 AND scope_kind=?2 AND team_id=?3 AND receipt_kind='BOOTSTRAP' AND leadership_revision=?4 ORDER BY receipt_ordinal DESC LIMIT 1",
                params![
                    row.scope.workspace_id().into_uuid().as_bytes(),
                    row.scope.scope_kind(),
                    NO_ACTIVITY_MARKER,
                    row.leadership_revision,
                ],
                |sql_row| sql_row.get(0),
            )
            .optional()?,
        (Some(_), Some(_)) => return Err(OperationsError::Corrupt("leadership scope target")),
    };
    match raw {
        None => Ok(None),
        Some(None) => Err(OperationsError::Corrupt("leadership bootstrap assignment")),
        Some(Some(value)) => {
            let assignment = Uuid::from_bytes(wire::id(value)?);
            if assignment.is_nil() {
                return Err(OperationsError::Corrupt("leadership bootstrap assignment"));
            }
            Ok(Some(AgentAssignmentId::from_uuid(assignment)))
        }
    }
}

fn require_revision(row: &LeadershipRow, expected_revision: i64) -> Result<(), OperationsError> {
    if expected_revision < 1 || row.leadership_revision != expected_revision {
        return Err(OperationsError::Conflict("leadership revision"));
    }
    Ok(())
}

fn require_failable(row: &LeadershipRow) -> Result<(), OperationsError> {
    if !matches!(
        row.state,
        LeadershipState::Stabilizing | LeadershipState::Active
    ) {
        return Err(OperationsError::Conflict("leadership not failable"));
    }
    Ok(())
}

fn transition_to_failover(
    tx: &Transaction<'_>,
    row: &LeadershipRow,
    now_ms: i64,
) -> Result<LeadershipMutationReceipt, OperationsError> {
    let updated = cas_leadership(
        tx,
        row.scope,
        row.leadership_revision,
        LeadershipUpdate {
            state: LeadershipState::FailoverRequired,
            leader_participant_id: row.leader_participant_id,
            stabilization_until_ms: None,
            current_election_id: row.current_election_id,
            now_ms,
        },
    )?
    .ok_or(OperationsError::Conflict("leadership revision"))?;
    append_receipt(
        tx,
        row.scope,
        &ReceiptRow {
            kind: ReceiptKind::Failover,
            provenance_activity: None,
            work_assignment_id: None,
            election_id: None,
            participant_id: row.leader_participant_id,
            leadership_revision: updated.leadership_revision,
            recorded_at_ms: now_ms,
        },
    )?;
    invalidate_workspace_election_for_team_change(tx, row.scope, now_ms)?;
    Ok(updated)
}

fn load_leadership(
    tx: &Transaction<'_>,
    scope: LeadershipScope,
) -> Result<Option<LeadershipRow>, OperationsError> {
    let activity_param = election_activity_param(scope);
    let raw = tx
        .query_row(
            "SELECT state,substr(leader_participant_id,1,17),leadership_revision,stabilization_until_ms,substr(current_election_id,1,17),updated_at_ms FROM ops_leadership WHERE workspace_id=?1 AND scope_kind=?2 AND activity_id IS ?3 AND team_id IS ?4",
            params![
                scope.workspace_id().into_uuid().as_bytes(),
                scope.scope_kind(),
                activity_param,
                election_team_param(scope),
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<Vec<u8>>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, Option<Vec<u8>>>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            },
        )
        .optional()?;
    raw.map(
        |(state, leader, revision, until, election, updated_at_ms)| {
            Ok(LeadershipRow {
                scope,
                state: LeadershipState::decode(&state)?,
                leader_participant_id: leader
                    .map(|value| Ok::<_, OperationsError>(Uuid::from_bytes(wire::id(value)?)))
                    .transpose()?,
                leadership_revision: revision,
                stabilization_until_ms: until,
                current_election_id: election
                    .map(|value| Ok::<_, OperationsError>(Uuid::from_bytes(wire::id(value)?)))
                    .transpose()?,
                updated_at_ms,
            })
        },
    )
    .transpose()
}

/// Connection-level form for callers already holding the exact read/write
/// transaction, including message enqueue effects. Never opens a nested transaction.
pub(crate) fn current_leadership_authority_in_transaction(
    tx: &Transaction<'_>,
    scope: LeadershipScope,
) -> Result<Option<CurrentLeadershipAuthority>, OperationsError> {
    let Some(row) = load_leadership(tx, scope)? else {
        return Ok(None);
    };
    let eligible = tx.query_row(
        &format!(
            "SELECT EXISTS(SELECT 1 FROM ops_leadership AS l
             WHERE l.workspace_id=?1 AND l.scope_kind=?2
               AND l.activity_id IS ?3 AND l.team_id IS ?4
               AND ({CURRENT_LEADERSHIP_AUTHORITY_PREDICATE}))"
        ),
        params![
            scope.workspace_id().into_uuid().as_bytes(),
            scope.scope_kind(),
            election_activity_param(scope),
            election_team_param(scope),
        ],
        |record| record.get::<_, bool>(0),
    )?;
    if !eligible {
        return Ok(None);
    }
    let participant_id = row
        .leader_participant_id
        .ok_or(OperationsError::Corrupt("leadership authority leader"))?;
    Ok(Some(CurrentLeadershipAuthority {
        scope,
        participant_id,
        leadership_revision: row.leadership_revision,
    }))
}

fn insert_leadership(tx: &Transaction<'_>, row: &LeadershipRow) -> Result<(), OperationsError> {
    tx.execute(
        "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        params![
            row.scope.workspace_id().into_uuid().as_bytes(),
            row.scope.scope_kind(),
            election_activity_param(row.scope),
            election_team_param(row.scope),
            row.state.encode(),
            row.leader_participant_id.map(|value| value.as_bytes().to_vec()),
            row.leadership_revision,
            row.stabilization_until_ms,
            row.current_election_id.map(|value| value.as_bytes().to_vec()),
            row.updated_at_ms,
        ],
    )?;
    Ok(())
}

/// One proposed current-state leadership row update.
#[derive(Debug, Clone, Copy)]
struct LeadershipUpdate {
    state: LeadershipState,
    leader_participant_id: Option<Uuid>,
    stabilization_until_ms: Option<i64>,
    current_election_id: Option<Uuid>,
    now_ms: i64,
}

/// Compare-and-set the current-state leadership row on the exact revision.
/// `Ok(None)` means the revision moved underneath the caller.
fn cas_leadership(
    tx: &Transaction<'_>,
    scope: LeadershipScope,
    expected_revision: i64,
    update: LeadershipUpdate,
) -> Result<Option<LeadershipMutationReceipt>, OperationsError> {
    let next_revision = expected_revision
        .checked_add(1)
        .ok_or(OperationsError::Bounds("leadership revision"))?;
    let changed = tx.execute(
        "UPDATE ops_leadership SET state=?5,leader_participant_id=?6,leadership_revision=?7,stabilization_until_ms=?8,current_election_id=?9,updated_at_ms=?10 WHERE workspace_id=?1 AND scope_kind=?2 AND activity_id IS ?3 AND team_id IS ?4 AND leadership_revision=?11",
        params![
            scope.workspace_id().into_uuid().as_bytes(),
            scope.scope_kind(),
            election_activity_param(scope),
            election_team_param(scope),
            update.state.encode(),
            update.leader_participant_id.map(|value| value.as_bytes().to_vec()),
            next_revision,
            update.stabilization_until_ms,
            update.current_election_id.map(|value| value.as_bytes().to_vec()),
            update.now_ms,
            expected_revision,
        ],
    )?;
    if changed != 1 {
        return Ok(None);
    }
    Ok(Some(LeadershipMutationReceipt {
        scope,
        state: update.state,
        leader_participant_id: update.leader_participant_id,
        leadership_revision: next_revision,
        stabilization_until_ms: update.stabilization_until_ms,
        updated_at_ms: update.now_ms,
    }))
}

/// Append one receipt inside the serialized IMMEDIATE writer.
fn election_originated_from_failover(
    tx: &Transaction<'_>,
    row: &LeadershipRow,
) -> Result<bool, OperationsError> {
    let source_revision = row.current_election_id.map(|id| {
        tx.query_row("SELECT source_leadership_revision FROM ops_election_subject_cutovers WHERE election_id=?1",
            params![id.as_bytes()],|record|record.get::<_,i64>(0)).optional()
    }).transpose()?.flatten();
    // Membership changes can advance the live leadership revision during a
    // ballot. New elections retain their immutable pre-start revision here.
    let previous_revision = match source_revision {
        Some(revision) => revision,
        None => row
            .leadership_revision
            .checked_sub(1)
            .ok_or(OperationsError::Corrupt("leadership election revision"))?,
    };
    match (row.scope.activity_id(), row.scope.team_id()) {
        (Some(activity), None) => Ok(tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_leadership_receipts WHERE workspace_id=?1 AND scope_kind=?2 AND activity_id=?3 AND team_id=?4 AND receipt_kind='FAILOVER' AND leadership_revision=?5)",
            params![
                row.scope.workspace_id().into_uuid().as_bytes(),
                row.scope.scope_kind(),
                activity.into_uuid().as_bytes(),
                NO_ACTIVITY_MARKER,
                previous_revision,
            ],
            |sql_row| sql_row.get(0),
        )?),
        (None, Some(team)) => Ok(tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_leadership_receipts WHERE workspace_id=?1 AND scope_kind=?2 AND team_id=?3 AND receipt_kind='FAILOVER' AND leadership_revision=?4)",
            params![
                row.scope.workspace_id().into_uuid().as_bytes(),
                row.scope.scope_kind(),
                team.as_bytes(),
                previous_revision,
            ],
            |sql_row| sql_row.get(0),
        )?),
        (None, None) => Ok(tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_leadership_receipts WHERE workspace_id=?1 AND scope_kind=?2 AND team_id=?3 AND receipt_kind='FAILOVER' AND leadership_revision=?4)",
            params![
                row.scope.workspace_id().into_uuid().as_bytes(),
                row.scope.scope_kind(),
                NO_ACTIVITY_MARKER,
                previous_revision,
            ],
            |sql_row| sql_row.get(0),
        )?),
        (Some(_), Some(_)) => Err(OperationsError::Corrupt("leadership scope target")),
    }
}

fn append_receipt(
    tx: &Transaction<'_>,
    scope: LeadershipScope,
    row: &ReceiptRow,
) -> Result<(), OperationsError> {
    let activity_bytes: Vec<u8> = match scope.activity_id().or(row.provenance_activity) {
        Some(activity) => activity.into_uuid().as_bytes().to_vec(),
        None => NO_ACTIVITY_MARKER.to_vec(),
    };
    let team_bytes = scope.team_id().map_or_else(
        || NO_ACTIVITY_MARKER.to_vec(),
        |team| team.as_bytes().to_vec(),
    );
    let ordinal: i64 = tx.query_row(
        "SELECT COUNT(*) FROM ops_leadership_receipts WHERE workspace_id=?1 AND scope_kind=?2 AND team_id=?3",
        params![
            scope.workspace_id().into_uuid().as_bytes(),
            scope.scope_kind(),
            team_bytes.clone(),
        ],
        |row| row.get(0),
    )?;
    let ordinal = ordinal
        .checked_add(1)
        .ok_or(OperationsError::Bounds("leadership receipt ordinal"))?;
    tx.execute(
        "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,work_assignment_id,election_id,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
        params![
            scope.workspace_id().into_uuid().as_bytes(),
            scope.scope_kind(),
            activity_bytes,
            team_bytes,
            ordinal,
            row.kind.encode(),
            row.work_assignment_id.map(|value| value.into_uuid().as_bytes().to_vec()),
            row.election_id.map(|value| value.as_bytes().to_vec()),
            row.participant_id.map(|value| value.as_bytes().to_vec()),
            row.leadership_revision,
            row.recorded_at_ms,
        ],
    )?;
    Ok(())
}

#[cfg(test)]
fn bootstrap_receipt_exists(
    tx: &Transaction<'_>,
    scope: LeadershipScope,
    assignment_id: AgentAssignmentId,
) -> Result<bool, OperationsError> {
    match (scope.activity_id(), scope.team_id()) {
        (Some(activity), None) => Ok(tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_leadership_receipts WHERE workspace_id=?1 AND scope_kind=?2 AND activity_id=?3 AND team_id=?4 AND receipt_kind='BOOTSTRAP' AND work_assignment_id=?5)",
            params![
                scope.workspace_id().into_uuid().as_bytes(),
                scope.scope_kind(),
                activity.into_uuid().as_bytes(),
                NO_ACTIVITY_MARKER,
                assignment_id.into_uuid().as_bytes(),
            ],
            |row| row.get(0),
        )?),
        (None, Some(team)) => Ok(tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_leadership_receipts WHERE workspace_id=?1 AND scope_kind=?2 AND team_id=?3 AND receipt_kind='BOOTSTRAP' AND work_assignment_id=?4)",
            params![
                scope.workspace_id().into_uuid().as_bytes(),
                scope.scope_kind(),
                team.as_bytes(),
                assignment_id.into_uuid().as_bytes(),
            ],
            |row| row.get(0),
        )?),
        (None, None) => Ok(tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_leadership_receipts WHERE workspace_id=?1 AND scope_kind=?2 AND team_id=?3 AND receipt_kind='BOOTSTRAP' AND work_assignment_id=?4)",
            params![
                scope.workspace_id().into_uuid().as_bytes(),
                scope.scope_kind(),
                NO_ACTIVITY_MARKER,
                assignment_id.into_uuid().as_bytes(),
            ],
            |row| row.get(0),
        )?),
        (Some(_), Some(_)) => Err(OperationsError::Corrupt("leadership scope target")),
    }
}

/// Internal outcome of one fenced commit transaction. The superseded arm
/// exists because the superseded evidence MUST survive the transaction: the
/// precondition failure it carries is surfaced only after the commit.
enum InternalConfirmCommit {
    Committed(LeadershipMutationReceipt),
    Superseded(OperationsError),
}

/// Internal outcome of one bootstrap-evidence confirmation. The evidence-lost
/// arm exists because the LEADERLESS fallback MUST survive the transaction:
/// the failed readback is surfaced only after the scope returned to
/// `LEADERLESS` (never fabricated authority).
enum InternalBootstrapConfirm {
    Confirmed(LeadershipMutationReceipt),
    EvidenceLost(OperationsError),
}

/// Whether an intent may still advance: only `pending` and `remote_recorded`
/// are resumable; terminal states are durable evidence, never re-executed.
fn is_live_confirm_state(state: LeadershipConfirmIntentState) -> bool {
    matches!(
        state,
        LeadershipConfirmIntentState::Pending | LeadershipConfirmIntentState::RemoteRecorded
    )
}

/// Exact command payload binding for a reused `request_id`.
fn confirm_intent_matches_command(
    intent: &LeadershipConfirmIntent,
    command: &PrepareLeadershipConfirmCommand,
) -> bool {
    intent.scope == command.scope
        && intent.election_id == command.election_id
        && intent.expected_local_revision == command.expected_revision
}

/// Exact payload agreement between the stored intent and the caller's
/// prepared readback at commit time.
fn confirm_intents_agree(
    stored: &LeadershipConfirmIntent,
    prepared: &LeadershipConfirmIntent,
) -> bool {
    stored.scope == prepared.scope
        && stored.election_id == prepared.election_id
        && stored.elected_participant_id == prepared.elected_participant_id
        && stored.expected_local_revision == prepared.expected_local_revision
        && stored.requested_fence_revision == prepared.requested_fence_revision
        && stored.owner_instance_id == prepared.owner_instance_id
}

/// The exact confirm preconditions, identical to the pre-fence checks this
/// state machine has always enforced: the row must be `AUTHORITY_PENDING`
/// at the expected revision with the election pointer resolved, the election
/// must belong to the scope, be `CLOSED_ELECTED`, and carry the winner.
fn validate_confirm_preconditions(
    tx: &Transaction<'_>,
    row: &LeadershipRow,
    command: &PrepareLeadershipConfirmCommand,
) -> Result<Uuid, OperationsError> {
    require_revision(row, command.expected_revision)?;
    if row.state != LeadershipState::AuthorityPending {
        return Err(OperationsError::Conflict(
            "leadership not authority pending",
        ));
    }
    if row.current_election_id != Some(command.election_id) {
        return Err(OperationsError::Conflict("leadership election pointer"));
    }
    let election = load_election(tx, command.election_id)?
        .ok_or(OperationsError::Conflict("leadership election missing"))?;
    if election.scope != command.scope {
        return Err(OperationsError::Conflict("leadership election scope"));
    }
    if election.state != ElectionState::ClosedElected {
        return Err(OperationsError::Conflict("leadership election not elected"));
    }
    let elected = election
        .elected_participant_id
        .ok_or(OperationsError::Corrupt("leadership election winner"))?;
    if !election_subject::snapshot_ready_subjects_for_scope(tx, command.scope)?
        .contains_participant(elected)
    {
        return Err(OperationsError::Conflict(
            "elected leader is no longer a Ready scope member",
        ));
    }
    Ok(elected)
}

/// Pure fence key encoding over the scope target:
/// `lead-ws-{workspace}-s{scope_kind}-{target}`. Deterministic so
/// retries and restarts replay the exact same Broker key.
fn leadership_fence_key(scope: LeadershipScope) -> String {
    match scope.target {
        LeadershipScopeTarget::Activity(activity) => format!(
            "lead-ws-{}-s{}-act-{}",
            scope.workspace_id().into_uuid(),
            scope.scope_kind(),
            activity.into_uuid()
        ),
        LeadershipScopeTarget::Team(team_id) => format!(
            "lead-ws-{}-s{}-team-{}",
            scope.workspace_id().into_uuid(),
            scope.scope_kind(),
            team_id
        ),
        LeadershipScopeTarget::Workspace => format!(
            "lead-ws-{}-s{}-act-nil",
            scope.workspace_id().into_uuid(),
            scope.scope_kind()
        ),
    }
}

fn confirm_fence_request(intent: &LeadershipConfirmIntent) -> LeadershipFenceRequest {
    LeadershipFenceRequest::new(
        intent.request_id,
        leadership_fence_key(intent.scope),
        intent.requested_fence_revision,
        intent.election_id,
        format!("orch-{}", intent.owner_instance_id),
    )
}

fn insert_confirm_intent(
    tx: &Transaction<'_>,
    command: &PrepareLeadershipConfirmCommand,
    elected: Uuid,
    owner_instance_id: Uuid,
) -> Result<LeadershipConfirmIntent, OperationsError> {
    tx.execute(
        "INSERT INTO ops_leadership_confirm_intents(request_id,kind,workspace_id,scope_kind,activity_id,team_id,election_id,elected_participant_id,expected_local_revision,requested_fence_revision,owner_instance_id,remote_recorded_revision,confirmed_at_ms,state,created_at_ms,resolved_at_ms) VALUES(?1,'election_confirm',?2,?3,?4,?5,?6,?7,?8,?8,?9,NULL,NULL,'pending',?10,NULL)",
        params![
            command.request_id.as_bytes(),
            command.scope.workspace_id().into_uuid().as_bytes(),
            command.scope.scope_kind(),
            election_activity_param(command.scope),
            election_team_param(command.scope),
            command.election_id.as_bytes(),
            elected.as_bytes(),
            command.expected_revision,
            owner_instance_id.as_bytes(),
            command.now_ms,
        ],
    )?;
    Ok(LeadershipConfirmIntent {
        request_id: command.request_id,
        scope: command.scope,
        election_id: command.election_id,
        elected_participant_id: elected,
        expected_local_revision: command.expected_revision,
        requested_fence_revision: command.expected_revision,
        owner_instance_id,
        remote_recorded_revision: None,
        confirmed_at_ms: None,
        state: LeadershipConfirmIntentState::Pending,
        created_at_ms: command.now_ms,
        resolved_at_ms: None,
    })
}

fn load_confirm_intent(
    tx: &Transaction<'_>,
    request_id: Uuid,
) -> Result<Option<LeadershipConfirmIntent>, OperationsError> {
    let raw = tx
        .query_row(
            "SELECT substr(workspace_id,1,17),scope_kind,substr(activity_id,1,17),substr(team_id,1,17),substr(election_id,1,17),substr(elected_participant_id,1,17),expected_local_revision,requested_fence_revision,substr(owner_instance_id,1,17),remote_recorded_revision,confirmed_at_ms,state,created_at_ms,resolved_at_ms FROM ops_leadership_confirm_intents WHERE request_id=?1",
            params![request_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<Vec<u8>>>(2)?,
                    row.get::<_, Option<Vec<u8>>>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, Vec<u8>>(8)?,
                    row.get::<_, Option<i64>>(9)?,
                    row.get::<_, Option<i64>>(10)?,
                    row.get::<_, String>(11)?,
                    row.get::<_, i64>(12)?,
                    row.get::<_, Option<i64>>(13)?,
                ))
            },
        )
        .optional()?;
    raw.map(
        |(
            workspace,
            scope_kind,
            activity,
            team,
            election,
            elected,
            expected_local_revision,
            requested_fence_revision,
            owner,
            remote_recorded_revision,
            confirmed_at_ms,
            state,
            created_at_ms,
            resolved_at_ms,
        )| {
            let workspace_id = WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(workspace)?));
            let activity_id = activity
                .map(|value| {
                    Ok::<_, OperationsError>(ActivityId::from_uuid(Uuid::from_bytes(wire::id(
                        value,
                    )?)))
                })
                .transpose()?;
            let team_id = team
                .map(|value| Ok::<_, OperationsError>(Uuid::from_bytes(wire::id(value)?)))
                .transpose()?;
            let scope = match (scope_kind, activity_id, team_id) {
                (0, None, None) => LeadershipScope::workspace(workspace_id),
                (1, Some(activity_id), None) => {
                    LeadershipScope::activity(workspace_id, activity_id)
                }
                (2, None, Some(team_id)) => LeadershipScope::team(workspace_id, team_id),
                _ => return Err(OperationsError::Corrupt("leadership confirm intent scope")),
            };
            Ok(LeadershipConfirmIntent {
                request_id,
                scope,
                election_id: Uuid::from_bytes(wire::id(election)?),
                elected_participant_id: Uuid::from_bytes(wire::id(elected)?),
                expected_local_revision,
                requested_fence_revision,
                owner_instance_id: Uuid::from_bytes(wire::id(owner)?),
                remote_recorded_revision,
                confirmed_at_ms,
                state: LeadershipConfirmIntentState::decode(&state)?,
                created_at_ms,
                resolved_at_ms,
            })
        },
    )
    .transpose()
}

/// Resolve one live intent into the target state. `remote_recorded_revision`
/// and `confirmed_at_ms` are write-once: a `None` argument never erases
/// recorded evidence. The resolved rows stay in the journal (terminal states
/// are prunable later; `pending`/`remote_recorded` are never pruned).
fn mark_confirm_intent(
    tx: &Transaction<'_>,
    request_id: Uuid,
    state: LeadershipConfirmIntentState,
    remote_recorded_revision: Option<i64>,
    now_ms: i64,
) -> Result<(), OperationsError> {
    let changed = tx.execute(
        "UPDATE ops_leadership_confirm_intents SET state=?2,remote_recorded_revision=COALESCE(?3,remote_recorded_revision),confirmed_at_ms=CASE WHEN ?2='committed' THEN COALESCE(confirmed_at_ms,?4) ELSE confirmed_at_ms END,resolved_at_ms=CASE WHEN ?2 IN('committed','superseded','rejected') THEN COALESCE(resolved_at_ms,?4) ELSE resolved_at_ms END WHERE request_id=?1 AND state IN('pending','remote_recorded')",
        params![
            request_id.as_bytes(),
            state.encode(),
            remote_recorded_revision,
            now_ms
        ],
    )?;
    if changed != 1 {
        return Err(OperationsError::Conflict("leadership confirm intent state"));
    }
    Ok(())
}

/// Resolve one live intent as `superseded`; the local record stays untouched
/// (the remote authority record is retained as the next succession's
/// high-water).
fn supersede_confirm_intent(
    tx: &Transaction<'_>,
    intent: &LeadershipConfirmIntent,
    remote_recorded_revision: Option<i64>,
    now_ms: i64,
) -> Result<(), OperationsError> {
    mark_confirm_intent(
        tx,
        intent.request_id,
        LeadershipConfirmIntentState::Superseded,
        remote_recorded_revision,
        now_ms,
    )
}

fn confirm_readback(
    tx: &Transaction<'_>,
    intent: &LeadershipConfirmIntent,
    reported_state: LeadershipConfirmIntentState,
) -> Result<LeadershipConfirmReadback, OperationsError> {
    let row = load_leadership(tx, intent.scope)?
        .ok_or(OperationsError::Corrupt("leadership confirm scope"))?;
    Ok(LeadershipConfirmReadback {
        intent_state: reported_state,
        scope: row.scope,
        state: row.state,
        leader_participant_id: row.leader_participant_id,
        leadership_revision: row.leadership_revision,
        stabilization_until_ms: row.stabilization_until_ms,
        updated_at_ms: row.updated_at_ms,
    })
}

/// Reconcile one recovery against a `committed` intent: the fenced confirm
/// durably landed, so the scope returns to `STABILIZING` under the elected
/// leader with the committed stabilization window. The preserved row facts
/// must agree with the intent evidence.
fn reconcile_committed_recovery(
    tx: &Transaction<'_>,
    row: &LeadershipRow,
    intent: &LeadershipConfirmIntent,
    now_ms: i64,
) -> Result<LeadershipMutationReceipt, OperationsError> {
    if row.leader_participant_id != Some(intent.elected_participant_id)
        || row
            .current_election_id
            .is_some_and(|id| id != intent.election_id)
        || unique_committed_recovery_intent(tx, row)?
            .is_none_or(|proof| proof.request_id != intent.request_id)
    {
        return Err(OperationsError::Conflict("leadership recovery facts"));
    }
    let confirmed_at = intent
        .confirmed_at_ms
        .ok_or(OperationsError::Corrupt("leadership recovery facts"))?;
    let until = confirmed_at
        .checked_add(STABILIZATION_WINDOW_MS)
        .ok_or(OperationsError::Bounds("leadership stabilization window"))?;
    let updated = cas_leadership(
        tx,
        row.scope,
        row.leadership_revision,
        LeadershipUpdate {
            state: LeadershipState::Stabilizing,
            leader_participant_id: Some(intent.elected_participant_id),
            stabilization_until_ms: Some(until),
            current_election_id: None,
            now_ms,
        },
    )?
    .ok_or(OperationsError::Conflict("leadership revision"))?;
    append_receipt(
        tx,
        row.scope,
        &ReceiptRow {
            kind: ReceiptKind::Confirmed,
            provenance_activity: None,
            work_assignment_id: None,
            election_id: None,
            participant_id: Some(intent.elected_participant_id),
            leadership_revision: updated.leadership_revision,
            recorded_at_ms: now_ms,
        },
    )?;
    Ok(updated)
}

/// Resolve one noncommitted confirm only while the current row still belongs
/// to that recovery/election. Preserve the incumbent as history and never
/// restore an earlier `ACTIVE` or `AUTHORITY_PENDING` phase.
fn terminalize_noncommitted_leadership(
    tx: &Transaction<'_>,
    row: &LeadershipRow,
    expected_election_id: Option<Uuid>,
    now_ms: i64,
) -> Result<Option<LeadershipMutationReceipt>, OperationsError> {
    if !matches!(
        row.state,
        LeadershipState::AuthorityPending | LeadershipState::RecoveryRequired
    ) || expected_election_id.is_some_and(|expected| row.current_election_id != Some(expected))
        || has_live_confirm_intent(tx, row.scope, expected_election_id)?
    {
        return Ok(None);
    }
    let (state, kind) = if row.leader_participant_id.is_some() {
        (LeadershipState::FailoverRequired, ReceiptKind::Failover)
    } else {
        (LeadershipState::Vacant, ReceiptKind::Vacated)
    };
    let updated = cas_leadership(
        tx,
        row.scope,
        row.leadership_revision,
        LeadershipUpdate {
            state,
            leader_participant_id: row.leader_participant_id,
            stabilization_until_ms: None,
            current_election_id: None,
            now_ms,
        },
    )?
    .ok_or(OperationsError::Conflict("leadership revision"))?;
    append_receipt(
        tx,
        row.scope,
        &ReceiptRow {
            kind,
            provenance_activity: None,
            work_assignment_id: None,
            election_id: expected_election_id.or(row.current_election_id),
            participant_id: row.leader_participant_id,
            leadership_revision: updated.leadership_revision,
            recorded_at_ms: now_ms,
        },
    )?;
    invalidate_workspace_election_for_team_change(tx, row.scope, now_ms)?;
    Ok(Some(updated))
}

fn reconcile_terminal_recovery(
    tx: &Transaction<'_>,
    row: &LeadershipRow,
    intent: &LeadershipConfirmIntent,
    now_ms: i64,
) -> Result<LeadershipMutationReceipt, OperationsError> {
    terminalize_noncommitted_leadership(tx, row, Some(intent.election_id), now_ms)?
        .ok_or(OperationsError::Conflict("leadership recovery election"))
}

fn terminal_confirm_intent_exists(
    tx: &Transaction<'_>,
    scope: LeadershipScope,
    election_id: Uuid,
) -> Result<bool, OperationsError> {
    tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_leadership_confirm_intents WHERE workspace_id=?1 AND scope_kind=?2 AND activity_id IS ?3 AND team_id IS ?4 AND election_id=?5 AND state IN('committed','superseded','rejected'))",
        params![
            scope.workspace_id().into_uuid().as_bytes(),
            scope.scope_kind(),
            election_activity_param(scope),
            election_team_param(scope),
            election_id.as_bytes(),
        ],
        |row| row.get(0),
    )
    .map_err(OperationsError::from)
}

fn has_live_confirm_intent(
    tx: &Transaction<'_>,
    scope: LeadershipScope,
    election_id: Option<Uuid>,
) -> Result<bool, OperationsError> {
    let election = election_id.map(|id| id.as_bytes().to_vec());
    tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_leadership_confirm_intents WHERE workspace_id=?1 AND scope_kind=?2 AND activity_id IS ?3 AND team_id IS ?4 AND (?5 IS NULL OR election_id=?5) AND state IN('pending','remote_recorded'))",
        params![
            scope.workspace_id().into_uuid().as_bytes(),
            scope.scope_kind(),
            election_activity_param(scope),
            election_team_param(scope),
            election,
        ],
        |row| row.get(0),
    )
    .map_err(OperationsError::from)
}

/// Return a committed intent only when its exact confirmation is the latest
/// non-recovery authority receipt for this scope. This prevents an older
/// successful intent from reviving a later leadership incident.
fn unique_committed_recovery_intent(
    tx: &Transaction<'_>,
    row: &LeadershipRow,
) -> Result<Option<LeadershipConfirmIntent>, OperationsError> {
    let Some(leader) = row.leader_participant_id else {
        return Ok(None);
    };
    let latest = tx
        .query_row(
            "SELECT receipt_kind,substr(election_id,1,17),substr(participant_id,1,17),leadership_revision,recorded_at_ms FROM ops_leadership_receipts WHERE workspace_id=?1 AND scope_kind=?2 AND activity_id=COALESCE(?3,zeroblob(16)) AND team_id=COALESCE(?4,zeroblob(16)) AND receipt_kind!='RECOVERY' AND NOT(receipt_kind='CONFIRMED' AND election_id IS NULL) ORDER BY receipt_ordinal DESC LIMIT 1",
            params![
                row.scope.workspace_id().into_uuid().as_bytes(),
                row.scope.scope_kind(),
                election_activity_param(row.scope),
                election_team_param(row.scope),
            ],
            |sql_row| {
                Ok((
                    sql_row.get::<_, String>(0)?,
                    sql_row.get::<_, Option<Vec<u8>>>(1)?,
                    sql_row.get::<_, Option<Vec<u8>>>(2)?,
                    sql_row.get::<_, i64>(3)?,
                    sql_row.get::<_, i64>(4)?,
                ))
            },
        )
        .optional()?;
    let Some((kind, election, participant, receipt_revision, confirmed_at)) = latest else {
        return Ok(None);
    };
    if kind != "CONFIRMED" {
        return Ok(None);
    }
    let Some(election_id) = election
        .map(|bytes| wire::id(bytes).map(Uuid::from_bytes))
        .transpose()?
    else {
        return Ok(None);
    };
    let Some(receipt_participant) = participant
        .map(|bytes| wire::id(bytes).map(Uuid::from_bytes))
        .transpose()?
    else {
        return Ok(None);
    };
    if receipt_participant != leader
        || row
            .current_election_id
            .is_some_and(|current| current != election_id)
    {
        return Ok(None);
    }
    let mut query = tx.prepare(
        "SELECT substr(request_id,1,17) FROM ops_leadership_confirm_intents WHERE workspace_id=?1 AND scope_kind=?2 AND activity_id IS ?3 AND team_id IS ?4 AND election_id=?5 AND elected_participant_id=?6 AND state='committed' AND remote_recorded_revision IS NOT NULL AND confirmed_at_ms=?7 ORDER BY request_id LIMIT 2",
    )?;
    let ids = query
        .query_map(
            params![
                row.scope.workspace_id().into_uuid().as_bytes(),
                row.scope.scope_kind(),
                election_activity_param(row.scope),
                election_team_param(row.scope),
                election_id.as_bytes(),
                leader.as_bytes(),
                confirmed_at,
            ],
            |sql_row| sql_row.get::<_, Vec<u8>>(0),
        )?
        .collect::<Result<Vec<_>, _>>()?;
    drop(query);
    if ids.len() != 1 {
        return Ok(None);
    }
    let request_id = Uuid::from_bytes(wire::id(
        ids.into_iter()
            .next()
            .ok_or(OperationsError::Corrupt("leadership recovery intent"))?,
    )?);
    let intent = load_confirm_intent(tx, request_id)?
        .ok_or(OperationsError::Corrupt("leadership recovery intent"))?;
    if intent.requested_fence_revision.checked_add(1) != Some(receipt_revision)
        || intent.remote_recorded_revision != Some(intent.requested_fence_revision)
        || intent.confirmed_at_ms != Some(confirmed_at)
    {
        return Ok(None);
    }
    Ok(Some(intent))
}

fn observe_live_recovery(
    tx: &Transaction<'_>,
    row: &LeadershipRow,
    now_ms: i64,
) -> Result<LeadershipMutationReceipt, OperationsError> {
    let updated = cas_leadership(
        tx,
        row.scope,
        row.leadership_revision,
        LeadershipUpdate {
            state: LeadershipState::RecoveryRequired,
            leader_participant_id: row.leader_participant_id,
            stabilization_until_ms: row.stabilization_until_ms,
            current_election_id: row.current_election_id,
            now_ms,
        },
    )?
    .ok_or(OperationsError::Conflict("leadership revision"))?;
    append_receipt(
        tx,
        row.scope,
        &ReceiptRow {
            kind: ReceiptKind::Recovery,
            provenance_activity: None,
            work_assignment_id: None,
            election_id: row.current_election_id,
            participant_id: row.leader_participant_id,
            leadership_revision: updated.leadership_revision,
            recorded_at_ms: now_ms,
        },
    )?;
    Ok(updated)
}

#[derive(Debug, Clone, Copy)]
struct OfferProjection {
    activity_id: ActivityId,
    target_agent_id: AgentInstanceId,
}

fn load_offer_projection(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    assignment_id: AgentAssignmentId,
) -> Result<Option<OfferProjection>, OperationsError> {
    let raw = tx
        .query_row(
            "SELECT substr(activity_id,1,17),substr(target_agent_id,1,17) FROM ops_assignment_offer_projection WHERE workspace_id=?1 AND assignment_id=?2",
            params![
                workspace_id.into_uuid().as_bytes(),
                assignment_id.into_uuid().as_bytes()
            ],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()?;
    raw.map(|(activity, agent)| {
        Ok(OfferProjection {
            activity_id: ActivityId::from_uuid(Uuid::from_bytes(wire::id(activity)?)),
            target_agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(agent)?)),
        })
    })
    .transpose()
}

fn resolve_offer_participant(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    agent_id: AgentInstanceId,
) -> Result<Option<Uuid>, OperationsError> {
    Ok(effect_binding::by_agent(tx, workspace_id, agent_id)?
        .filter(|binding| binding.activity_id() == activity_id)
        .map(effect_binding::CurrentEffectBinding::participant_id))
}

struct CandidateWorkCreditQualification {
    snapshot: work_credit::WorkCreditScoreSnapshot,
    subject: work_credit_subject::WorkCreditSubjectIdentity,
    source_participant_count: u64,
    total_credits: i64,
}

fn candidate_work_credit_qualification(
    tx: &Transaction<'_>,
    scope: LeadershipScope,
    candidate_participant_id: Uuid,
    now_ms: i64,
) -> Result<CandidateWorkCreditQualification, OperationsError> {
    let workspace_id = scope.workspace_id();
    let snapshot = match scope.team_id() {
        Some(team) => work_credit::team_work_credit_scores_at(tx, workspace_id, team, now_ms)?,
        None => work_credit::work_credit_scores_at(tx, workspace_id, now_ms)?,
    };
    let (subject, source_participant_count, total_credits) =
        work_credit_subject::subject_score_for_participant(
            tx,
            workspace_id,
            &snapshot,
            candidate_participant_id,
        )?;
    if !candidate_qualifies(&snapshot, total_credits)? {
        return Err(OperationsError::Conflict("leadership candidate score"));
    }
    Ok(CandidateWorkCreditQualification {
        snapshot,
        subject,
        source_participant_count,
        total_credits,
    })
}

/// Exact 35%-of-subject-mean qualification. A zero mean (no verified credits
/// in the window) qualifies nobody.
fn candidate_qualifies(
    snapshot: &work_credit::WorkCreditScoreSnapshot,
    own_score: i64,
) -> Result<bool, OperationsError> {
    let count = snapshot.mean_subject_count();
    if count == 0 {
        return Ok(false);
    }
    let verified_total = snapshot
        .subject_arithmetic_mean()
        .checked_mul(count)
        .and_then(|value| value.checked_add(snapshot.subject_mean_remainder_numerator()))
        .ok_or(OperationsError::Bounds("leadership qualification mean"))?;
    if verified_total == 0 {
        return Ok(false);
    }
    let own = own_score
        .checked_mul(count)
        .and_then(|value| value.checked_mul(QUALIFICATION_DENOMINATOR))
        .ok_or(OperationsError::Bounds("leadership qualification score"))?;
    let threshold = verified_total
        .checked_mul(QUALIFICATION_NUMERATOR)
        .ok_or(OperationsError::Bounds("leadership qualification mean"))?;
    Ok(own >= threshold)
}

/// Deterministic election identity: domain-separated digest over the scope
/// and the exact post-start leadership revision, so an interrupted start
/// converges on the same election identity.
fn election_identity(scope: LeadershipScope, leadership_revision: i64) -> Uuid {
    let mut digest = Sha256::new();
    digest.update(ELECTION_ID_DOMAIN);
    digest.update((16_u32).to_be_bytes());
    digest.update(scope.workspace_id().into_uuid().as_bytes());
    let kind = u8::try_from(scope.scope_kind()).unwrap_or(0);
    digest.update([kind]);
    match scope.target {
        LeadershipScopeTarget::Activity(activity) => {
            digest.update([1]);
            digest.update(activity.into_uuid().as_bytes());
        }
        LeadershipScopeTarget::Workspace => digest.update([0]),
        LeadershipScopeTarget::Team(team_id) => {
            digest.update([2]);
            digest.update(team_id.as_bytes());
        }
    }
    digest.update(leadership_revision.to_be_bytes());
    let digest = digest.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

fn election_activity_param(scope: LeadershipScope) -> Option<Vec<u8>> {
    scope
        .activity_id()
        .map(|activity| activity.into_uuid().as_bytes().to_vec())
}

fn election_team_param(scope: LeadershipScope) -> Option<Vec<u8>> {
    scope.team_id().map(|team| team.as_bytes().to_vec())
}

fn load_election(
    tx: &Transaction<'_>,
    election_id: Uuid,
) -> Result<Option<ElectionRow>, OperationsError> {
    let raw = tx
        .query_row(
            "SELECT substr(workspace_id,1,17),scope_kind,substr(activity_id,1,17),substr(team_id,1,17),state,substr(candidate_participant_id,1,17),ballot_deadline_ms,substr(elected_participant_id,1,17),tally_yes,tally_no,tally_abstain FROM ops_leadership_elections WHERE election_id=?1",
            params![election_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<Vec<u8>>>(2)?,
                    row.get::<_, Option<Vec<u8>>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, Option<Vec<u8>>>(7)?,
                    row.get::<_, Option<i64>>(8)?,
                    row.get::<_, Option<i64>>(9)?,
                    row.get::<_, Option<i64>>(10)?,
                ))
            },
        )
        .optional()?;
    raw.map(
        |(
            workspace,
            scope_kind,
            activity,
            team,
            state,
            candidate,
            deadline,
            elected,
            tally_yes,
            tally_no,
            tally_abstain,
        )| {
            let workspace_id = WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(workspace)?));
            let activity_id = activity
                .map(|value| {
                    Ok::<_, OperationsError>(ActivityId::from_uuid(Uuid::from_bytes(wire::id(
                        value,
                    )?)))
                })
                .transpose()?;
            let team_id = team
                .map(|value| Ok::<_, OperationsError>(Uuid::from_bytes(wire::id(value)?)))
                .transpose()?;
            let scope = match (scope_kind, activity_id, team_id) {
                (0, None, None) => LeadershipScope::workspace(workspace_id),
                (1, Some(activity_id), None) => {
                    LeadershipScope::activity(workspace_id, activity_id)
                }
                (2, None, Some(team_id)) => LeadershipScope::team(workspace_id, team_id),
                _ => return Err(OperationsError::Corrupt("leadership election scope")),
            };
            Ok(ElectionRow {
                scope,
                state: ElectionState::decode(&state)?,
                candidate_participant_id: Uuid::from_bytes(wire::id(candidate)?),
                ballot_deadline_ms: deadline,
                elected_participant_id: elected
                    .map(|value| Ok::<_, OperationsError>(Uuid::from_bytes(wire::id(value)?)))
                    .transpose()?,
                tally_yes,
                tally_no,
                tally_abstain,
            })
        },
    )
    .transpose()
}

fn election_summary(
    tx: &Transaction<'_>,
    election_id: Uuid,
    now_ms: i64,
) -> Result<Option<LeadershipElectionSummary>, OperationsError> {
    let Some(election) = load_election(tx, election_id)? else {
        return Ok(None);
    };
    let electorate_size = election_subject::electorate_size(tx, election_id)?;
    let (cast_yes, cast_no, cast_abstain) = match election.state {
        ElectionState::Electing => election_subject::cast_tallies(tx, election_id)?,
        _ => (
            election.tally_yes.unwrap_or(0),
            election.tally_no.unwrap_or(0),
            election.tally_abstain.unwrap_or(0),
        ),
    };
    Ok(Some(LeadershipElectionSummary {
        election_id,
        state: election.state,
        ballot_deadline_ms: election.ballot_deadline_ms,
        deadline_passed: now_ms >= election.ballot_deadline_ms,
        electorate_size,
        cast_yes,
        cast_no,
        cast_abstain,
    }))
}

fn member_display_name(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
) -> Result<Option<String>, OperationsError> {
    let raw: Option<Vec<u8>> = tx
        .query_row(
            "SELECT substr(CAST(display_name AS BLOB),1,129) FROM ops_logical_members WHERE workspace_id=?1 AND participant_id=?2",
            params![
                workspace_id.into_uuid().as_bytes(),
                participant_id.as_bytes()
            ],
            |row| row.get(0),
        )
        .optional()?;
    raw.map(|value| {
        String::from_utf8(value).map_err(|_| OperationsError::Corrupt("leadership member display"))
    })
    .transpose()
}

#[cfg(test)]
mod tests {
    mod work_roster_team;

    use super::*;
    use crate::{LeadershipWorkExchangeReason, LeadershipWorkExchangeState};
    use forge_domain::ActivityId;
    use std::path::PathBuf;

    struct Fixture {
        root: PathBuf,
        catalog: OperationsCatalog,
        destination: Uuid,
        migration: Uuid,
        workspace: WorkspaceId,
        activity: ActivityId,
    }

    fn fixture(name: &str, seed: u128) -> Result<Fixture, Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!(
            "orchestration-leadership-{name}-{}",
            Uuid::now_v7()
        ));
        std::fs::create_dir(&root)?;
        let destination = Uuid::from_u128(seed);
        let migration = Uuid::from_u128(seed + 1);
        let workspace = WorkspaceId::from_uuid(Uuid::from_u128(seed + 3));
        let activity = ActivityId::from_uuid(Uuid::from_u128(seed + 4));
        let catalog = OperationsCatalog::open(&root.join("catalog.sqlite3"), destination)?;
        catalog.connection.execute(
            "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
            params![migration.as_bytes(), workspace.into_uuid().as_bytes(), destination.as_bytes(), [0u8;32].as_slice()],
        )?;
        catalog.connection.execute(
            "INSERT INTO ops_effect_fact_consumers(migration_id,workspace_id,destination_id,last_sequence,applied_count) VALUES(?1,?2,?3,0,0)",
            params![migration.as_bytes(), workspace.into_uuid().as_bytes(), destination.as_bytes()],
        )?;
        Ok(Fixture {
            root,
            catalog,
            destination,
            migration,
            workspace,
            activity,
        })
    }

    fn insert_member(
        fixture: &Fixture,
        activity: ActivityId,
        participant: Uuid,
        session_seed: u128,
        agent_seed: u128,
    ) -> Result<(), Box<dyn std::error::Error>> {
        fixture.catalog.connection.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,?4,?5,1,1,1)",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                participant.as_bytes(),
                Uuid::from_u128(session_seed).as_bytes(),
                format!("member {session_seed:x}"),
                "worker",
            ],
        )?;
        fixture.catalog.connection.execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                participant.as_bytes(),
                activity.into_uuid().as_bytes(),
                Uuid::from_u128(session_seed).as_bytes(),
                Uuid::from_u128(agent_seed).as_bytes(),
                Uuid::from_u128(agent_seed + 0x30).as_bytes(),
            ],
        )?;
        Ok(())
    }

    fn insert_credit(
        fixture: &Fixture,
        assignment_seed: u128,
        participant: Uuid,
        agent_seed: u128,
        budget: i64,
        awarded_at_ms: i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        fixture.catalog.connection.execute(
            "INSERT INTO ops_work_credit_entries(credit_id,workspace_id,assignment_id,participant_id,agent_id,activity_id,goal_id,node_id,budget,award_source_sequence,evidence_execution_id,awarded_at_ms,created_digest) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,1,?10,?11,?12)",
            params![
                Uuid::from_u128(assignment_seed).as_bytes(),
                fixture.workspace.into_uuid().as_bytes(),
                Uuid::from_u128(assignment_seed).as_bytes(),
                participant.as_bytes(),
                Uuid::from_u128(agent_seed).as_bytes(),
                fixture.activity.into_uuid().as_bytes(),
                Uuid::from_u128(assignment_seed + 1).as_bytes(),
                Uuid::from_u128(assignment_seed + 2).as_bytes(),
                budget,
                Uuid::from_u128(assignment_seed + 3).as_bytes(),
                awarded_at_ms,
                [9_u8; 32].as_slice(),
            ],
        )?;
        Ok(())
    }

    fn insert_offer(
        fixture: &Fixture,
        sequence: i64,
        activity: ActivityId,
        assignment_seed: u128,
        agent_seed: u128,
        offered_at_ms: i64,
    ) -> Result<AgentAssignmentId, Box<dyn std::error::Error>> {
        let assignment_id = AgentAssignmentId::from_uuid(Uuid::from_u128(assignment_seed));
        fixture.catalog.connection.execute(
            "INSERT INTO ops_assignment_offer_projection(migration_id,source_sequence,fact_digest,assignment_id,workspace_id,activity_id,goal_id,node_id,target_agent_id,target_session_id,credit_budget,offered_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,NULL,NULL,?10)",
            params![
                fixture.migration.as_bytes(),
                sequence,
                [7_u8; 32].as_slice(),
                assignment_id.into_uuid().as_bytes(),
                fixture.workspace.into_uuid().as_bytes(),
                activity.into_uuid().as_bytes(),
                Uuid::from_u128(assignment_seed + 1).as_bytes(),
                Uuid::from_u128(assignment_seed + 2).as_bytes(),
                Uuid::from_u128(agent_seed).as_bytes(),
                offered_at_ms,
            ],
        )?;
        Ok(assignment_id)
    }

    fn receipt_kinds(
        fixture: &Fixture,
        scope_kind: i64,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut statement = fixture.catalog.connection.prepare(
            "SELECT receipt_kind FROM ops_leadership_receipts WHERE workspace_id=?1 AND scope_kind=?2 ORDER BY receipt_ordinal",
        )?;
        let rows = statement
            .query_map(
                params![fixture.workspace.into_uuid().as_bytes(), scope_kind],
                |row| row.get::<_, String>(0),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// The two-step bootstrap: seed the scope into `BOOTSTRAPPING` from one
    /// durable offer, then confirm the offer evidence durably so the scope
    /// stabilizes.
    fn bootstrap_and_confirm(
        fixture: &mut Fixture,
        scope: LeadershipScope,
        assignment_id: AgentAssignmentId,
        bootstrapped_at_ms: i64,
        confirmed_at_ms: i64,
    ) -> Result<LeadershipMutationReceipt, Box<dyn std::error::Error>> {
        let bootstrapped = fixture
            .catalog
            .bootstrap_leadership(BootstrapLeadershipCommand {
                scope,
                assignment_id,
                now_ms: bootstrapped_at_ms,
            })?;
        assert_eq!(bootstrapped.state(), LeadershipState::Bootstrapping);
        assert_eq!(bootstrapped.stabilization_until_ms(), None);
        let confirmed =
            fixture
                .catalog
                .confirm_bootstrap_evidence(ConfirmBootstrapEvidenceCommand {
                    scope,
                    assignment_id,
                    expected_revision: bootstrapped.leadership_revision(),
                    now_ms: confirmed_at_ms,
                })?;
        assert_eq!(confirmed.state(), LeadershipState::Stabilizing);
        Ok(confirmed)
    }

    /// Seed current, confirmed Team leaders for Workspace election fixtures.
    /// Every Team has at least two distinct joined Workspace Participations;
    /// additional Workspace members can belong to a Team without becoming its
    /// current leader or a Workspace voter.
    #[allow(
        clippy::too_many_lines,
        reason = "Keeps the multi-table electorate fixture and its authority receipts together."
    )]
    fn seed_workspace_team_electorate(
        fixture: &mut Fixture,
        leaders: &[Uuid],
        additional_members: &[Uuid],
        seed: u128,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if leaders.len() < 2 {
            return Err("Workspace electorate fixture needs at least two Team leaders".into());
        }

        let mut participants = leaders.to_vec();
        for participant in additional_members {
            if !participants.contains(participant) {
                participants.push(*participant);
            }
        }
        for (ordinal, participant) in participants.iter().enumerate() {
            let session_id: Vec<u8> = fixture.catalog.connection.query_row(
                "SELECT session_id FROM ops_logical_members WHERE workspace_id=?1 AND participant_id=?2",
                params![fixture.workspace.into_uuid().as_bytes(), participant.as_bytes()],
                |row| row.get(0),
            )?;
            let ordinal = u128::try_from(ordinal)?;
            let profile_id = Uuid::from_u128(seed + 0x6100 + ordinal);
            let participation_id = Uuid::from_u128(seed + 0x6200 + ordinal);
            fixture.catalog.connection.execute(
                "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,1,1)",
                params![
                    fixture.workspace.into_uuid().as_bytes(),
                    profile_id.as_bytes(),
                    participation_id.as_bytes(),
                ],
            )?;
            fixture.catalog.connection.execute(
                "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,1)",
                params![
                    Uuid::from_u128(seed + 0x6300 + ordinal).as_bytes(),
                    fixture.workspace.into_uuid().as_bytes(),
                    participation_id.as_bytes(),
                    profile_id.as_bytes(),
                    Uuid::from_u128(seed + 0x6400 + ordinal).as_bytes(),
                    session_id,
                    participant.as_bytes(),
                ],
            )?;
        }

        for (ordinal, leader) in leaders.iter().enumerate() {
            let ordinal = u128::try_from(ordinal)?;
            let team_id = Uuid::from_u128(seed + 0x6500 + ordinal);
            fixture.catalog.connection.execute(
                "INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_at_ms,updated_at_ms,formed_at_ms) VALUES(?1,?2,?3,'election electorate','ACTIVE',1,'test','Test',1,1,1)",
                params![
                    team_id.as_bytes(),
                    fixture.workspace.into_uuid().as_bytes(),
                    format!("election electorate {ordinal}"),
                ],
            )?;

            let mut team_members = vec![
                *leader,
                leaders[(usize::try_from(ordinal)? + 1) % leaders.len()],
            ];
            if ordinal == 0 {
                team_members.extend_from_slice(additional_members);
            }
            for participant in team_members {
                let participation_id: Vec<u8> = fixture.catalog.connection.query_row(
                    "SELECT participation_id FROM ops_workspace_participation_mappings WHERE workspace_id=?1 AND participant_id=?2",
                    params![fixture.workspace.into_uuid().as_bytes(), participant.as_bytes()],
                    |row| row.get(0),
                )?;
                let exists: bool = fixture.catalog.connection.query_row(
                    "SELECT EXISTS(SELECT 1 FROM ops_team_members WHERE workspace_id=?1 AND team_id=?2 AND participation_id=?3)",
                    params![fixture.workspace.into_uuid().as_bytes(), team_id.as_bytes(), participation_id.as_slice()],
                    |row| row.get(0),
                )?;
                if !exists {
                    fixture.catalog.connection.execute(
                        "INSERT INTO ops_team_members(workspace_id,team_id,participation_id,state,revision,joined_at_ms,left_at_ms,role) VALUES(?1,?2,?3,'JOINED',1,1,NULL,'verification')",
                        params![
                            fixture.workspace.into_uuid().as_bytes(),
                            team_id.as_bytes(),
                            participation_id,
                        ],
                    )?;
                }
            }

            let agent_id: Vec<u8> = fixture.catalog.connection.query_row(
                "SELECT agent_id FROM ops_current_effect_bindings WHERE workspace_id=?1 AND participant_id=?2 AND activity_id=?3",
                params![
                    fixture.workspace.into_uuid().as_bytes(),
                    leader.as_bytes(),
                    fixture.activity.into_uuid().as_bytes(),
                ],
                |row| row.get(0),
            )?;
            let agent_id = Uuid::from_slice(&agent_id)?;
            let session_id: Vec<u8> = fixture.catalog.connection.query_row(
                "SELECT session_id FROM ops_current_effect_bindings WHERE workspace_id=?1 AND agent_id=?2",
                params![fixture.workspace.into_uuid().as_bytes(), agent_id.as_bytes()],
                |row| row.get(0),
            )?;
            let assignment_seed = seed + 0x6600 + ordinal;
            let node_scope = crate::NodeTeamBindingScope::new(
                fixture.workspace,
                fixture.activity,
                forge_domain::GoalId::from_uuid(Uuid::from_u128(assignment_seed + 1)),
                forge_domain::TimestampMillis::new(1)?,
                forge_domain::DagRevisionNumber::INITIAL,
                forge_domain::GoalRevision::INITIAL,
                forge_domain::DagNodeId::from_uuid(Uuid::from_u128(assignment_seed + 2)),
            )?;
            let _binding =
                fixture
                    .catalog
                    .set_node_team_binding(crate::SetNodeTeamBindingCommand {
                        scope: node_scope,
                        expected_revision: None,
                        team_id: Some(team_id),
                        actor: crate::OperationalActor::LocalOperator,
                        expected_sender_leadership_revision: None,
                        observed_at: forge_domain::TimestampMillis::new(T0)?,
                    })?;
            let _affinity =
                fixture
                    .catalog
                    .record_assignment_affinity(&crate::AssignmentAffinityCommand {
                        workspace_id: fixture.workspace,
                        assignment_id: AgentAssignmentId::from_uuid(Uuid::from_u128(
                            assignment_seed,
                        )),
                        activity_id: fixture.activity,
                        target_agent_id: AgentInstanceId::from_uuid(agent_id),
                        target_session_id: forge_domain::AgentSessionId::from_uuid(
                            Uuid::from_slice(&session_id)?,
                        ),
                        expected_target_principal:
                            forge_domain::AuthenticatedPrincipal::TrustedStdio,
                        requester: crate::OperationalActor::LocalOperator,
                        team_id: Some(team_id),
                        node_scope,
                        expected_binding_revision: Some(1),
                        assignment_payload_fingerprint: [7_u8; 32],
                        original_offer_payload: b"fixture-offer".to_vec(),
                        observed_at: forge_domain::TimestampMillis::new(T0)?,
                    })?;
            let assignment = insert_offer(
                fixture,
                100 + i64::try_from(ordinal)?,
                fixture.activity,
                seed + 0x6600 + ordinal,
                u128::from_be_bytes(*agent_id.as_bytes()),
                T0,
            )?;
            let scope = LeadershipScope::team(fixture.workspace, team_id);
            fixture.catalog.connection.execute(
                "UPDATE ops_assignment_offer_projection SET target_session_id=?1 WHERE workspace_id=?2 AND assignment_id=?3",
                params![session_id, fixture.workspace.into_uuid().as_bytes(), assignment.into_uuid().as_bytes()],
            )?;
            let confirmed = bootstrap_and_confirm(fixture, scope, assignment, T0, T0)?;
            let active = fixture.catalog.mark_stable(MarkStableCommand {
                scope,
                expected_revision: confirmed.leadership_revision(),
                now_ms: T0 + STABILIZATION_WINDOW_MS,
            })?;
            assert_eq!(active.state(), LeadershipState::Active);
            assert_eq!(active.leader_participant_id(), Some(*leader));
        }
        Ok(())
    }

    const T0: i64 = 10_000_000;

    #[test]
    fn catalog_v11_migrates_additively_without_fabricating_leadership()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = fixture("v11-migration", 0x1E00)?;
        let path = fixture.root.join("catalog.sqlite3");
        let destination = fixture.destination;
        let workspace = fixture.workspace;
        drop(fixture.catalog);
        std::fs::remove_file(&path)?;

        // Rebuild at v12 then downgrade by dropping the newest objects only.
        let catalog = OperationsCatalog::open(&path, destination)?;
        drop(catalog);
        let connection = Connection::open(&path)?;
        crate::workspace_participation::drop_schema_for_legacy_fixture(&connection)?;
        // The downgraded catalog must keep the exact v11 change-set shape:
        // SQLite's ADD COLUMN rewrite places the producer columns before the
        // table constraints, so a v14-created table text already carries them.
        connection.execute_batch(&format!(
            "DROP TABLE ops_admission_records; DROP TABLE ops_approval_requests; DROP TABLE ops_approval_policy; DROP TABLE ops_issuer_config; DROP TABLE ops_leadership_confirm_intents; DROP TABLE ops_ballots; DROP TABLE ops_election_electorate; DROP TABLE ops_leadership_elections; DROP TABLE ops_leadership_receipts; DROP TABLE ops_leadership; DROP TABLE ops_assignment_submit_source_refs; DROP TABLE ops_assignment_submit_projection; DROP TABLE ops_assignment_offer_projection; DROP TABLE ops_change_set_projection; {}; PRAGMA user_version=11;",
            crate::effect_fact::change_sets_sql_v13()
        ))?;
        drop(connection);

        let migrated = OperationsCatalog::open(&path, destination)?;
        let version: i64 = migrated
            .connection
            .pragma_query_value(None, "user_version", |row| row.get(0))?;
        assert_eq!(version, crate::catalog::VERSION);
        assert!(
            migrated.leadership_view(workspace, None, T0)?.is_none(),
            "v11 -> v12 must not fabricate a workspace leadership row"
        );
        drop(migrated);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn v19_handoff_pending_migrates_fail_closed_to_failover()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = fixture("v19-handoff-retirement", 0x1E08)?;
        let path = fixture.root.join("catalog.sqlite3");
        let destination = fixture.destination;
        let workspace = fixture.workspace;
        let leader = Uuid::from_u128(0x1E09);
        drop(fixture.catalog);

        let connection = Connection::open(&path)?;
        crate::workspace_participation::drop_schema_for_legacy_fixture(&connection)?;
        connection.execute("DROP TABLE ops_leadership", [])?;
        connection.execute_batch(LEADERSHIP_SQL_V19)?;
        connection.execute_batch(LEADERSHIP_SCOPE_INDEX_V31)?;
        connection.execute(
            "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,0,NULL,'HANDOFF_PENDING',?2,7,NULL,NULL,?3)",
            params![workspace.into_uuid().as_bytes(), leader.as_bytes(), T0],
        )?;
        connection.pragma_update(None, "user_version", 19_i64)?;
        drop(connection);

        let migrated = OperationsCatalog::open(&path, destination)?;
        let version: i64 = migrated
            .connection
            .pragma_query_value(None, "user_version", |row| row.get(0))?;
        assert_eq!(version, crate::catalog::VERSION);
        let view = migrated
            .leadership_view(workspace, None, T0)?
            .ok_or("migrated leadership missing")?;
        assert_eq!(view.state(), LeadershipState::FailoverRequired);
        assert_eq!(view.leader_participant_id(), None);
        assert_eq!(view.leadership_revision(), 8);
        let schema: String = migrated.connection.query_row(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name='ops_leadership'",
            [],
            |row| row.get(0),
        )?;
        assert!(!schema.contains("HANDOFF_PENDING"));
        let failover_receipts: i64 = migrated.connection.query_row(
            "SELECT COUNT(*) FROM ops_leadership_receipts WHERE workspace_id=?1 AND scope_kind=0 AND receipt_kind='FAILOVER' AND leadership_revision=8",
            params![workspace.into_uuid().as_bytes()],
            |row| row.get(0),
        )?;
        assert_eq!(failover_receipts, 1);
        drop(migrated);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn explicit_bootstrap_seeds_both_scopes_and_cas_guards_revisions()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = fixture("bootstrap", 0x1E10)?;
        let participant = Uuid::from_u128(0x1E11);
        insert_member(&fixture, fixture.activity, participant, 0x1E12, 0x1E13)?;
        let assignment = insert_offer(&fixture, 1, fixture.activity, 0x1E14, 0x1E13, T0)?;

        let activity_scope = LeadershipScope::activity(fixture.workspace, fixture.activity);
        // Step one: the offer seeds BOOTSTRAPPING with the pending evidence,
        // no stabilization window and no confirmed authority yet.
        let receipt = fixture
            .catalog
            .bootstrap_leadership(BootstrapLeadershipCommand {
                scope: activity_scope,
                assignment_id: assignment,
                now_ms: T0,
            })?;
        assert_eq!(receipt.state(), LeadershipState::Bootstrapping);
        assert_eq!(receipt.leader_participant_id(), Some(participant));
        assert_eq!(receipt.leadership_revision(), 1);
        assert_eq!(receipt.stabilization_until_ms(), None);
        assert!(matches!(
            fixture
                .catalog
                .start_recall_election(RecallElectionCommand {
                    scope: activity_scope,
                    expected_revision: 1,
                    candidate_participant_id: participant,
                    now_ms: T0 + 1,
                }),
            Err(OperationsError::Conflict("leadership not electable"))
        ));

        let view = fixture
            .catalog
            .leadership_view(fixture.workspace, Some(fixture.activity), T0)?
            .ok_or("activity leadership view missing")?;
        assert_eq!(view.state(), LeadershipState::Bootstrapping);
        assert_eq!(view.leader_display_name(), Some("member 1e12"));
        assert!(view.current_election().is_none());
        assert_eq!(view.stabilization_until_ms(), None);

        // Step two: the durable evidence readback anchors the window.
        let confirmed =
            fixture
                .catalog
                .confirm_bootstrap_evidence(ConfirmBootstrapEvidenceCommand {
                    scope: activity_scope,
                    assignment_id: assignment,
                    expected_revision: 1,
                    now_ms: T0 + 5,
                })?;
        assert_eq!(confirmed.state(), LeadershipState::Stabilizing);
        assert_eq!(confirmed.leader_participant_id(), Some(participant));
        assert_eq!(confirmed.leadership_revision(), 2);
        assert_eq!(
            confirmed.stabilization_until_ms(),
            Some(T0 + 5 + STABILIZATION_WINDOW_MS)
        );

        // Deferred bootstrap must match the automatic offer path: the same
        // assignment may seed the distinct Workspace scope exactly once.
        let workspace_scope = LeadershipScope::workspace(fixture.workspace);
        let workspace_receipt =
            bootstrap_and_confirm(&mut fixture, workspace_scope, assignment, T0 + 10, T0 + 10)?;
        assert_eq!(workspace_receipt.leadership_revision(), 2);
        assert_eq!(
            workspace_receipt.stabilization_until_ms(),
            Some(T0 + 10 + STABILIZATION_WINDOW_MS)
        );
        assert_eq!(
            receipt_kinds(&fixture, 1)?,
            vec!["BOOTSTRAP".to_owned(), "CONFIRMED".to_owned()]
        );
        assert_eq!(
            receipt_kinds(&fixture, 0)?,
            vec!["BOOTSTRAP".to_owned(), "CONFIRMED".to_owned()]
        );
        let bootstrap_rows: i64 = fixture.catalog.connection.query_row(
            "SELECT COUNT(*) FROM ops_leadership_receipts WHERE receipt_kind='BOOTSTRAP' AND work_assignment_id=?1",
            params![assignment.into_uuid().as_bytes()],
            |row| row.get(0),
        )?;
        assert_eq!(
            bootstrap_rows, 2,
            "one bootstrap provenance per leadership scope"
        );
        assert!(matches!(
            fixture
                .catalog
                .bootstrap_leadership(BootstrapLeadershipCommand {
                    scope: workspace_scope,
                    assignment_id: assignment,
                    now_ms: T0 + 11,
                }),
            Err(OperationsError::Conflict("leadership bootstrap replay"))
        ));

        // A second bootstrap of a seeded scope conflicts; stale revisions and
        // unknown offers fail closed.
        let later_assignment =
            insert_offer(&fixture, 3, fixture.activity, 0x1E30, 0x1E13, T0 + 20)?;
        assert!(matches!(
            fixture
                .catalog
                .bootstrap_leadership(BootstrapLeadershipCommand {
                    scope: activity_scope,
                    assignment_id: later_assignment,
                    now_ms: T0 + 20,
                }),
            Err(OperationsError::Conflict("leadership already seeded"))
        ));
        assert!(matches!(
            fixture
                .catalog
                .bootstrap_leadership(BootstrapLeadershipCommand {
                    scope: activity_scope,
                    assignment_id: AgentAssignmentId::from_uuid(Uuid::from_u128(0x1E99)),
                    now_ms: T0,
                }),
            Err(OperationsError::Conflict("leadership bootstrap offer"))
        ));
        assert!(matches!(
            fixture.catalog.mark_stable(MarkStableCommand {
                scope: activity_scope,
                expected_revision: 9,
                now_ms: T0 + STABILIZATION_WINDOW_MS + 1,
            }),
            Err(OperationsError::Conflict("leadership revision"))
        ));
        drop(fixture.catalog);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn stabilization_lock_gates_recall_and_mark_stable_until_the_window_elapses()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = fixture("stabilization", 0x1E40)?;
        let participant = Uuid::from_u128(0x1E41);
        insert_member(&fixture, fixture.activity, participant, 0x1E42, 0x1E43)?;
        let assignment = insert_offer(&fixture, 1, fixture.activity, 0x1E44, 0x1E43, T0)?;
        let scope = LeadershipScope::activity(fixture.workspace, fixture.activity);
        let _ = bootstrap_and_confirm(&mut fixture, scope, assignment, T0, T0)?;

        // Recall during stabilization is rejected by the stabilization lock.
        assert!(matches!(
            fixture
                .catalog
                .start_recall_election(RecallElectionCommand {
                    scope,
                    expected_revision: 2,
                    candidate_participant_id: participant,
                    now_ms: T0 + 1,
                }),
            Err(OperationsError::Conflict("leadership not electable"))
        ));
        // mark_stable before the window fails closed; at/after it succeeds.
        assert!(matches!(
            fixture.catalog.mark_stable(MarkStableCommand {
                scope,
                expected_revision: 2,
                now_ms: T0 + STABILIZATION_WINDOW_MS - 1,
            }),
            Err(OperationsError::Conflict("leadership stabilization window"))
        ));
        let stable = fixture.catalog.mark_stable(MarkStableCommand {
            scope,
            expected_revision: 2,
            now_ms: T0 + STABILIZATION_WINDOW_MS,
        })?;
        assert_eq!(stable.state(), LeadershipState::Active);
        assert_eq!(stable.leadership_revision(), 3);
        assert_eq!(stable.stabilization_until_ms(), None);
        assert!(matches!(
            fixture.catalog.mark_stable(MarkStableCommand {
                scope,
                expected_revision: 3,
                now_ms: T0 + STABILIZATION_WINDOW_MS + 1,
            }),
            Err(OperationsError::Conflict("leadership not stabilizing"))
        ));
        assert_eq!(
            receipt_kinds(&fixture, 1)?,
            vec![
                "BOOTSTRAP".to_owned(),
                "CONFIRMED".to_owned(),
                "CONFIRMED".to_owned()
            ]
        );

        // Failover bypasses the stabilization lock from ACTIVE...
        let failed_over = fixture.catalog.mark_failover(MarkFailoverCommand {
            scope,
            expected_revision: 3,
            reason: LeadershipFailoverReason::LeaderUnresponsive,
            now_ms: T0 + STABILIZATION_WINDOW_MS + 2,
        })?;
        assert_eq!(failed_over.state(), LeadershipState::FailoverRequired);
        // ...and a failover recall election may then start with the same
        // exact qualification and membership rules.
        insert_credit(&fixture, 0x1E50, participant, 0x1E43, 5, T0)?;
        insert_credit(&fixture, 0x1E51, participant, 0x1E43, 5, T0)?;
        let election = fixture
            .catalog
            .start_failover_recall_election(RecallElectionCommand {
                scope,
                expected_revision: 4,
                candidate_participant_id: participant,
                now_ms: T0 + STABILIZATION_WINDOW_MS + 3,
            })?;
        assert_eq!(election.electorate_size(), 1);
        let closed = fixture.catalog.close_election(CloseElectionCommand {
            election_id: election.election_id(),
            now_ms: election.ballot_deadline_ms(),
        })?;
        assert_eq!(closed.state(), ElectionState::ClosedNoMajority);
        let view = fixture
            .catalog
            .leadership_view(fixture.workspace, Some(fixture.activity), T0)?
            .ok_or("activity view missing")?;
        assert_eq!(view.state(), LeadershipState::FailoverRequired);
        drop(fixture.catalog);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn recall_election_freezes_electorate_replays_ballots_and_decides_majority()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = fixture("election", 0x1E60)?;
        let activity = fixture.activity;
        let leader = Uuid::from_u128(0x1E61);
        let candidate = Uuid::from_u128(0x1E62);
        let voter_c = Uuid::from_u128(0x1E63);
        let voter_d = Uuid::from_u128(0x1E64);
        let voter_e = Uuid::from_u128(0x1E65);
        let voter_f = Uuid::from_u128(0x1E66);
        let participants = [leader, candidate, voter_c, voter_d, voter_e, voter_f];
        for (ordinal, participant) in participants.iter().enumerate() {
            insert_member(
                &fixture,
                activity,
                *participant,
                0x1E70 + u128::try_from(ordinal)?,
                0x1E80 + u128::try_from(ordinal)?,
            )?;
        }
        seed_workspace_team_electorate(
            &mut fixture,
            &[leader, voter_c, voter_d, voter_e, voter_f],
            &[candidate],
            0x1E60,
        )?;
        let assignment = insert_offer(&fixture, 1, activity, 0x1E90, 0x1E80, T0)?;
        let scope = LeadershipScope::workspace(fixture.workspace);
        let _ = bootstrap_and_confirm(&mut fixture, scope, assignment, T0, T0)?;
        let _ = fixture.catalog.mark_stable(MarkStableCommand {
            scope,
            expected_revision: 2,
            now_ms: T0 + STABILIZATION_WINDOW_MS,
        })?;
        // Candidate must hold at least 35% of the exact mean: 5 of a mean of 1.
        insert_credit(&fixture, 0x1EA0, candidate, 0x1E81, 5, T0)?;
        for (ordinal, participant) in participants.iter().take(5).enumerate() {
            insert_credit(
                &fixture,
                0x1EB0 + u128::try_from(ordinal)?,
                *participant,
                0x1E80 + u128::try_from(ordinal)?,
                1,
                T0,
            )?;
        }
        let start = T0 + STABILIZATION_WINDOW_MS + 10;
        let election = fixture
            .catalog
            .start_recall_election(RecallElectionCommand {
                scope,
                expected_revision: 3,
                candidate_participant_id: candidate,
                now_ms: start,
            })?;
        assert_eq!(election.electorate_size(), 5);
        assert_eq!(election.ballot_deadline_ms(), start + BALLOT_WINDOW_MS);

        // The electorate is frozen: a member that joins after the freeze
        // cannot vote in this election.
        let late = Uuid::from_u128(0x1EC0);
        insert_member(&fixture, activity, late, 0x1EC1, 0x1EC2)?;
        assert!(matches!(
            fixture.catalog.cast_ballot(CastBallotCommand {
                election_id: election.election_id(),
                voter_participant_id: late,
                choice: BallotChoice::Yes,
                now_ms: start + 1,
            }),
            Err(OperationsError::Conflict("leadership voter electorate"))
        ));

        // Casting at or after the deadline fails closed.
        assert!(matches!(
            fixture.catalog.cast_ballot(CastBallotCommand {
                election_id: election.election_id(),
                voter_participant_id: leader,
                choice: BallotChoice::Yes,
                now_ms: election.ballot_deadline_ms(),
            }),
            Err(OperationsError::Conflict("leadership ballot deadline"))
        ));

        let vote = |fixture: &mut Fixture, voter: Uuid, choice: BallotChoice, at: i64| {
            fixture.catalog.cast_ballot(CastBallotCommand {
                election_id: election.election_id(),
                voter_participant_id: voter,
                choice,
                now_ms: at,
            })
        };
        assert!(matches!(
            vote(&mut fixture, candidate, BallotChoice::Yes, start + 1),
            Err(OperationsError::Conflict("leadership voter electorate"))
        ));
        let first = vote(&mut fixture, leader, BallotChoice::Yes, start + 1)?;
        assert!(!first.replay());
        assert_eq!(first.ballot_revision(), 1);
        let replay = vote(&mut fixture, leader, BallotChoice::Yes, start + 2)?;
        assert!(replay.replay(), "an identical repeat ballot is a no-op");
        assert!(matches!(
            vote(&mut fixture, leader, BallotChoice::No, start + 3),
            Err(OperationsError::Conflict("leadership ballot choice"))
        ));
        let _ = vote(&mut fixture, voter_c, BallotChoice::Yes, start + 4)?;
        // A changed choice on any voter conflicts.
        assert!(matches!(
            vote(&mut fixture, voter_c, BallotChoice::No, start + 5),
            Err(OperationsError::Conflict("leadership ballot choice"))
        ));
        let _ = vote(&mut fixture, voter_d, BallotChoice::Yes, start + 6)?;
        let _ = vote(&mut fixture, voter_e, BallotChoice::Abstain, start + 7)?;

        let live_view = fixture
            .catalog
            .leadership_view(fixture.workspace, None, start + 8)?
            .ok_or("workspace view missing")?;
        let live = live_view
            .current_election()
            .ok_or("live election missing")?;
        assert_eq!(live.state(), ElectionState::Electing);
        assert!(!live.deadline_passed());
        assert_eq!(
            (live.cast_yes(), live.cast_no(), live.cast_abstain()),
            (3, 0, 1)
        );

        // Closing before the deadline is rejected.
        assert!(matches!(
            fixture.catalog.close_election(CloseElectionCommand {
                election_id: election.election_id(),
                now_ms: election.ballot_deadline_ms() - 1,
            }),
            Err(OperationsError::Conflict("leadership ballot deadline"))
        ));

        // 3 of 5 Yes passes: yes > electorate/2, the abstention counts against.
        let tally = fixture.catalog.close_election(CloseElectionCommand {
            election_id: election.election_id(),
            now_ms: election.ballot_deadline_ms(),
        })?;
        assert_eq!(tally.state(), ElectionState::ClosedElected);
        assert_eq!(tally.elected_participant_id(), Some(candidate));
        assert_eq!(
            (tally.tally_yes(), tally.tally_no(), tally.tally_abstain()),
            (3, 0, 1)
        );
        assert_eq!(tally.electorate_size(), 5);

        let pending_view = fixture
            .catalog
            .leadership_view(fixture.workspace, None, election.ballot_deadline_ms())?
            .ok_or("workspace view missing")?;
        assert_eq!(pending_view.state(), LeadershipState::AuthorityPending);
        assert_eq!(pending_view.leader_participant_id(), Some(leader));
        let closed_summary = pending_view
            .current_election()
            .ok_or("closed election still referenced")?;
        assert_eq!(closed_summary.state(), ElectionState::ClosedElected);

        // Confirm starts a fresh stabilization window under the new leader,
        // gated behind the prepared Broker authority fence: the durable
        // intent, the exact fence request, then the fenced commit.
        let request_id = Uuid::from_u128(0x1ED0);
        let prepared =
            match fixture
                .catalog
                .prepare_leadership_confirm(PrepareLeadershipConfirmCommand {
                    scope,
                    election_id: election.election_id(),
                    expected_revision: pending_view.leadership_revision(),
                    request_id,
                    now_ms: election.ballot_deadline_ms() + 1,
                })? {
                PrepareLeadershipConfirmOutcome::Prepared(prepared) => prepared,
                PrepareLeadershipConfirmOutcome::Resolved(_) => {
                    return Err("a fresh confirm must prepare".into());
                }
            };
        assert_eq!(
            prepared.fence_request().revision(),
            pending_view.leadership_revision()
        );
        assert_eq!(
            prepared.intent().state(),
            LeadershipConfirmIntentState::Pending
        );
        let confirmed = fixture.catalog.commit_leadership_confirm(
            &prepared,
            LeadershipFenceOutcome::from_request(
                prepared.fence_request(),
                pending_view.leadership_revision(),
            ),
            election.ballot_deadline_ms() + 1,
        )?;
        assert_eq!(confirmed.state(), LeadershipState::Stabilizing);
        assert_eq!(confirmed.leader_participant_id(), Some(candidate));
        assert_eq!(
            confirmed.stabilization_until_ms(),
            Some(election.ballot_deadline_ms() + 1 + STABILIZATION_WINDOW_MS)
        );
        let final_view = fixture
            .catalog
            .leadership_view(fixture.workspace, None, election.ballot_deadline_ms() + 1)?
            .ok_or("workspace view missing")?;
        assert!(final_view.current_election().is_none());
        assert_eq!(
            receipt_kinds(&fixture, 0)?,
            vec![
                "BOOTSTRAP".to_owned(),
                "CONFIRMED".to_owned(),
                "CONFIRMED".to_owned(),
                "ELECTED".to_owned(),
                "CONFIRMED".to_owned()
            ]
        );

        // Reopen: leadership, elections and ballots survive.
        let path = fixture.root.join("catalog.sqlite3");
        drop(fixture.catalog);
        let reopened = OperationsCatalog::open(&path, fixture.destination)?;
        let reopened_view = reopened
            .leadership_view(fixture.workspace, None, election.ballot_deadline_ms() + 1)?
            .ok_or("workspace view missing after reopen")?;
        assert_eq!(reopened_view.state(), LeadershipState::Stabilizing);
        assert_eq!(reopened_view.leader_participant_id(), Some(candidate));
        assert_eq!(
            reopened_view.leadership_revision(),
            confirmed.leadership_revision()
        );
        let legacy_ballots: i64 =
            reopened
                .connection
                .query_row("SELECT COUNT(*) FROM ops_ballots", [], |row| row.get(0))?;
        let subject_ballots: i64 = reopened.connection.query_row(
            "SELECT COUNT(*) FROM ops_election_subject_ballots",
            [],
            |row| row.get(0),
        )?;
        let legacy_electorate: i64 = reopened.connection.query_row(
            "SELECT COUNT(*) FROM ops_election_electorate",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(
            legacy_ballots, 0,
            "new elections never dual-write legacy ballots"
        );
        assert_eq!(
            legacy_electorate, 0,
            "new elections never dual-write the legacy electorate"
        );
        assert_eq!(subject_ballots, 4, "subject ballot rows survive the reopen");
        let cutover = reopened
            .election_subject_cutover(election.election_id())?
            .ok_or("election subject cutover missing after reopen")?;
        let team_leader_sources: (i64, i64, i64) = reopened.connection.query_row(
            "SELECT COUNT(*),COUNT(DISTINCT team_id),SUM(CASE WHEN leadership_revision=3 AND team_revision=1 AND team_membership_revision=1 THEN 1 ELSE 0 END) FROM ops_election_team_leader_sources WHERE election_id=?1",
            params![election.election_id().as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        assert_eq!(team_leader_sources, (5, 5, 5));
        let candidate_is_voter: bool = reopened.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_election_voter_sources WHERE election_id=?1 AND participant_id=?2)",
            params![election.election_id().as_bytes(), candidate.as_bytes()],
            |row| row.get(0),
        )?;
        assert!(
            !candidate_is_voter,
            "a qualified Workspace candidate may be a non-voter"
        );
        assert_eq!(cutover.source_participant_count(), 5);
        assert_eq!(cutover.voter_subject_count(), 5);
        assert_eq!(cutover.source_leadership_revision(), 3);
        assert_eq!(cutover.result_leadership_revision(), 4);
        assert_ne!(cutover.electorate_digest(), [0; 32]);
        drop(reopened);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn no_majority_keeps_the_incumbent_and_qualification_fails_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = fixture("no-majority", 0x1F00)?;
        let activity = fixture.activity;
        let leader = Uuid::from_u128(0x1F01);
        let candidate = Uuid::from_u128(0x1F02);
        let voter_c = Uuid::from_u128(0x1F03);
        let voter_d = Uuid::from_u128(0x1F04);
        let voter_e = Uuid::from_u128(0x1F05);
        let voter_f = Uuid::from_u128(0x1F06);
        let participants = [leader, candidate, voter_c, voter_d, voter_e, voter_f];
        for (ordinal, participant) in participants.iter().enumerate() {
            insert_member(
                &fixture,
                activity,
                *participant,
                0x1F10 + u128::try_from(ordinal)?,
                0x1F20 + u128::try_from(ordinal)?,
            )?;
        }
        seed_workspace_team_electorate(
            &mut fixture,
            &[leader, voter_c, voter_d, voter_e, voter_f],
            &[candidate],
            0x1F00,
        )?;
        let assignment = insert_offer(&fixture, 1, activity, 0x1F30, 0x1F20, T0)?;
        let scope = LeadershipScope::workspace(fixture.workspace);
        let _ = bootstrap_and_confirm(&mut fixture, scope, assignment, T0, T0)?;
        let _ = fixture.catalog.mark_stable(MarkStableCommand {
            scope,
            expected_revision: 2,
            now_ms: T0 + STABILIZATION_WINDOW_MS,
        })?;

        // Zero-mean freeze: with no verified credits in the window, nobody
        // qualifies, so the candidate is rejected before any election exists.
        assert!(matches!(
            fixture
                .catalog
                .start_recall_election(RecallElectionCommand {
                    scope,
                    expected_revision: 3,
                    candidate_participant_id: candidate,
                    now_ms: T0 + STABILIZATION_WINDOW_MS + 1,
                }),
            Err(OperationsError::Conflict("leadership candidate score"))
        ));

        // Below the exact 35% mean: the incumbent holds 0 of a mean of 5.
        for (ordinal, participant) in participants.iter().skip(1).take(4).enumerate() {
            insert_credit(
                &fixture,
                0x1F40 + u128::try_from(ordinal)?,
                *participant,
                0x1F21 + u128::try_from(ordinal)?,
                5,
                T0,
            )?;
        }
        assert!(matches!(
            fixture
                .catalog
                .start_recall_election(RecallElectionCommand {
                    scope,
                    expected_revision: 3,
                    candidate_participant_id: leader,
                    now_ms: T0 + STABILIZATION_WINDOW_MS + 2,
                }),
            Err(OperationsError::Conflict("leadership candidate score"))
        ));

        let start = T0 + STABILIZATION_WINDOW_MS + 3;
        let election = fixture
            .catalog
            .start_recall_election(RecallElectionCommand {
                scope,
                expected_revision: 3,
                candidate_participant_id: candidate,
                now_ms: start,
            })?;
        assert_eq!(election.electorate_size(), 5);
        let vote = |fixture: &mut Fixture, voter: Uuid, choice: BallotChoice, at: i64| {
            fixture.catalog.cast_ballot(CastBallotCommand {
                election_id: election.election_id(),
                voter_participant_id: voter,
                choice,
                now_ms: at,
            })
        };
        let _ = vote(&mut fixture, leader, BallotChoice::Yes, start + 1)?;
        let _ = vote(&mut fixture, voter_c, BallotChoice::Yes, start + 2)?;
        assert!(matches!(
            vote(&mut fixture, candidate, BallotChoice::No, start + 3),
            Err(OperationsError::Conflict("leadership voter electorate"))
        ));
        let _ = vote(&mut fixture, voter_f, BallotChoice::No, start + 3)?;
        let _ = vote(&mut fixture, voter_d, BallotChoice::Abstain, start + 4)?;
        // 2 of 5 Yes fails: yes > electorate/2 is false (abstentions and
        // non-responses count against the replacement).
        let tally = fixture.catalog.close_election(CloseElectionCommand {
            election_id: election.election_id(),
            now_ms: election.ballot_deadline_ms(),
        })?;
        assert_eq!(tally.state(), ElectionState::ClosedNoMajority);
        assert_eq!(tally.elected_participant_id(), None);
        let view = fixture
            .catalog
            .leadership_view(fixture.workspace, None, election.ballot_deadline_ms())?
            .ok_or("workspace view missing")?;
        assert_eq!(view.state(), LeadershipState::Active);
        assert_eq!(view.leader_participant_id(), Some(leader));
        assert!(view.current_election().is_none());
        assert_eq!(
            receipt_kinds(&fixture, 0)?,
            vec![
                "BOOTSTRAP".to_owned(),
                "CONFIRMED".to_owned(),
                "CONFIRMED".to_owned()
            ]
        );

        // Resign fences on the expected leader and vacates the scope; a new
        // offer may then re-seed the vacant scope.
        assert!(matches!(
            fixture.catalog.resign_leadership(ResignLeadershipCommand {
                scope,
                expected_leader: candidate,
                expected_revision: view.leadership_revision(),
                now_ms: election.ballot_deadline_ms() + 2,
            }),
            Err(OperationsError::Conflict("leadership expected leader"))
        ));
        let resigned = fixture.catalog.resign_leadership(ResignLeadershipCommand {
            scope,
            expected_leader: leader,
            expected_revision: view.leadership_revision(),
            now_ms: election.ballot_deadline_ms() + 2,
        })?;
        assert_eq!(resigned.state(), LeadershipState::Vacant);
        let vacancy_assignment = insert_offer(&fixture, 2, activity, 0x1F50, 0x1F20, T0)?;
        let reseeded = bootstrap_and_confirm(
            &mut fixture,
            scope,
            vacancy_assignment,
            election.ballot_deadline_ms() + 3,
            election.ballot_deadline_ms() + 4,
        )?;
        assert_eq!(
            reseeded.leadership_revision(),
            resigned.leadership_revision() + 2
        );
        drop(fixture.catalog);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn failover_required_can_mark_vacant_and_never_manufactures_a_winner()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = fixture("failover-vacant", 0x1F60)?;
        let participant = Uuid::from_u128(0x1F61);
        insert_member(&fixture, fixture.activity, participant, 0x1F62, 0x1F63)?;
        let assignment = insert_offer(&fixture, 1, fixture.activity, 0x1F64, 0x1F63, T0)?;
        let scope = LeadershipScope::activity(fixture.workspace, fixture.activity);
        let _ = bootstrap_and_confirm(&mut fixture, scope, assignment, T0, T0)?;
        let failed_over = fixture.catalog.mark_failover(MarkFailoverCommand {
            scope,
            expected_revision: 2,
            reason: LeadershipFailoverReason::IncumbentDeparted,
            now_ms: T0 + 1,
        })?;
        assert_eq!(failed_over.state(), LeadershipState::FailoverRequired);
        assert!(matches!(
            fixture.catalog.mark_stable(MarkStableCommand {
                scope,
                expected_revision: 3,
                now_ms: T0 + STABILIZATION_WINDOW_MS + 1,
            }),
            Err(OperationsError::Conflict("leadership not stabilizing"))
        ));
        let vacated = fixture.catalog.mark_vacant(MarkVacantCommand {
            scope,
            expected_revision: 3,
            now_ms: T0 + 2,
        })?;
        assert_eq!(vacated.state(), LeadershipState::Vacant);
        assert_eq!(vacated.leader_participant_id(), None);
        assert_eq!(
            receipt_kinds(&fixture, 1)?,
            vec![
                "BOOTSTRAP".to_owned(),
                "CONFIRMED".to_owned(),
                "FAILOVER".to_owned(),
                "VACATED".to_owned()
            ]
        );
        drop(fixture.catalog);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    // ---- Broker-fenced confirm helpers ---------------------------------

    fn current_revision(
        fixture: &Fixture,
        workspace: WorkspaceId,
    ) -> Result<i64, Box<dyn std::error::Error>> {
        let view = fixture
            .catalog
            .leadership_view(workspace, None, T0)?
            .ok_or("workspace leadership row missing")?;
        Ok(view.leadership_revision())
    }

    /// Workspace-scope fixture: leader bootstrapped and stable, candidate
    /// qualified (5 credits of an exact mean of 3), scope `ACTIVE`.
    fn seeded_workspace_fixture(
        name: &str,
        seed: u128,
    ) -> Result<(Fixture, LeadershipScope, Uuid, Uuid), Box<dyn std::error::Error>> {
        let mut fixture = fixture(name, seed)?;
        let leader = Uuid::from_u128(seed + 0x100);
        let candidate = Uuid::from_u128(seed + 0x200);
        insert_member(
            &fixture,
            fixture.activity,
            leader,
            seed + 0x101,
            seed + 0x102,
        )?;
        insert_member(
            &fixture,
            fixture.activity,
            candidate,
            seed + 0x201,
            seed + 0x202,
        )?;
        let assignment = insert_offer(
            &fixture,
            1,
            fixture.activity,
            seed + 0x300,
            seed + 0x102,
            T0,
        )?;
        let scope = LeadershipScope::workspace(fixture.workspace);
        let seeded = bootstrap_and_confirm(&mut fixture, scope, assignment, T0, T0 + 1)?;
        assert_eq!(seeded.leader_participant_id(), Some(leader));
        let active = fixture.catalog.mark_stable(MarkStableCommand {
            scope,
            expected_revision: 2,
            now_ms: T0 + 1 + STABILIZATION_WINDOW_MS,
        })?;
        assert_eq!(active.state(), LeadershipState::Active);
        insert_credit(&fixture, seed + 0x400, candidate, seed + 0x202, 5, T0)?;
        insert_credit(&fixture, seed + 0x410, leader, seed + 0x102, 1, T0)?;
        seed_workspace_team_electorate(&mut fixture, &[leader, candidate], &[], seed)?;
        Ok((fixture, scope, leader, candidate))
    }

    fn run_majority_election(
        fixture: &mut Fixture,
        scope: LeadershipScope,
        candidate: Uuid,
        start: i64,
    ) -> Result<(Uuid, i64), Box<dyn std::error::Error>> {
        let election = fixture
            .catalog
            .start_recall_election(RecallElectionCommand {
                scope,
                expected_revision: current_revision(fixture, scope.workspace_id())?,
                candidate_participant_id: candidate,
                now_ms: start,
            })?;
        let election_id = election.election_id();
        let voters =
            election_subject::frozen_source_participants(&fixture.catalog.connection, election_id)?;
        for voter in &voters {
            let ballot = fixture.catalog.cast_ballot(CastBallotCommand {
                election_id,
                voter_participant_id: *voter,
                choice: BallotChoice::Yes,
                now_ms: start + 1,
            })?;
            assert!(!ballot.replay());
        }
        let tally = fixture.catalog.close_election(CloseElectionCommand {
            election_id,
            now_ms: election.ballot_deadline_ms(),
        })?;
        assert_eq!(tally.state(), ElectionState::ClosedElected);
        Ok((election_id, election.ballot_deadline_ms()))
    }

    fn prepare_confirm(
        fixture: &mut Fixture,
        scope: LeadershipScope,
        election_id: Uuid,
        request_id: Uuid,
        now_ms: i64,
    ) -> Result<PreparedLeadershipConfirm, Box<dyn std::error::Error>> {
        match fixture
            .catalog
            .prepare_leadership_confirm(PrepareLeadershipConfirmCommand {
                scope,
                election_id,
                expected_revision: current_revision(fixture, scope.workspace_id())?,
                request_id,
                now_ms,
            })? {
            PrepareLeadershipConfirmOutcome::Prepared(prepared) => Ok(prepared),
            PrepareLeadershipConfirmOutcome::Resolved(_) => {
                Err("expected a live prepared confirm".into())
            }
        }
    }

    /// The evidence a healthy Broker returns for the exact prepared request.
    fn ok_fence(prepared: &PreparedLeadershipConfirm) -> LeadershipFenceOutcome {
        LeadershipFenceOutcome::from_request(
            prepared.fence_request(),
            prepared.fence_request().revision(),
        )
    }

    const FENCED_ELECTION_REVISION: i64 = 5;

    /// Scenario 1: the first confirm happens at a non-1 local revision; the
    /// fence request and the durable evidence carry the EXACT revision (no
    /// bootstrap-at-1 requirement, gaps allowed).
    #[test]
    fn confirm_fences_the_exact_non_one_local_revision() -> Result<(), Box<dyn std::error::Error>> {
        let (mut fixture, scope, _leader, candidate) =
            seeded_workspace_fixture("fence-exact-revision", 0x5000)?;
        let (election_id, deadline) = run_majority_election(
            &mut fixture,
            scope,
            candidate,
            T0 + STABILIZATION_WINDOW_MS + 1,
        )?;
        let request_id = Uuid::from_u128(0x5901);

        let prepared = prepare_confirm(&mut fixture, scope, election_id, request_id, deadline + 1)?;
        assert_eq!(
            prepared.fence_request().revision(),
            FENCED_ELECTION_REVISION
        );
        assert_eq!(
            prepared.fence_request().key(),
            format!("lead-ws-{}-s0-act-nil", fixture.workspace.into_uuid())
        );
        assert_eq!(prepared.fence_request().authority_id(), election_id);
        assert_eq!(
            prepared.fence_request().owner_instance_id(),
            format!("orch-{}", fixture.destination)
        );
        let intent = fixture
            .catalog
            .leadership_confirm_intent(request_id)?
            .ok_or("intent row missing")?;
        assert_eq!(intent.state(), LeadershipConfirmIntentState::Pending);
        assert_eq!(intent.owner_instance_id(), fixture.destination);
        assert_eq!(intent.remote_recorded_revision(), None);

        let committed = fixture.catalog.commit_leadership_confirm(
            &prepared,
            ok_fence(&prepared),
            deadline + 2,
        )?;
        assert_eq!(committed.state(), LeadershipState::Stabilizing);
        assert_eq!(committed.leader_participant_id(), Some(candidate));
        assert_eq!(
            committed.leadership_revision(),
            FENCED_ELECTION_REVISION + 1
        );

        let intent = fixture
            .catalog
            .leadership_confirm_intent(request_id)?
            .ok_or("intent row missing after commit")?;
        assert_eq!(intent.state(), LeadershipConfirmIntentState::Committed);
        assert_eq!(
            intent.remote_recorded_revision(),
            Some(FENCED_ELECTION_REVISION)
        );
        assert_eq!(intent.confirmed_at_ms(), Some(deadline + 2));
        assert_eq!(intent.resolved_at_ms(), Some(deadline + 2));

        // A replayed commit is a resolution rejection, never a re-execution.
        assert!(matches!(
            fixture
                .catalog
                .commit_leadership_confirm(&prepared, ok_fence(&prepared), deadline + 3),
            Err(OperationsError::Conflict(
                "leadership confirm already resolved"
            ))
        ));
        drop(fixture.catalog);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn confirm_rejects_outcome_bound_to_a_different_prepared_request()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut fixture, scope, _leader, candidate) =
            seeded_workspace_fixture("fence-outcome-identity", 0x5A00)?;
        let (election_id, deadline) = run_majority_election(
            &mut fixture,
            scope,
            candidate,
            T0 + STABILIZATION_WINDOW_MS + 1,
        )?;
        let prepared = prepare_confirm(
            &mut fixture,
            scope,
            election_id,
            Uuid::from_u128(0x5A01),
            deadline + 1,
        )?;
        let rogue_request = LeadershipFenceRequest::new(
            Uuid::from_u128(0x5AFF),
            prepared.fence_request().key().to_owned(),
            prepared.fence_request().revision(),
            prepared.fence_request().authority_id(),
            prepared.fence_request().owner_instance_id().to_owned(),
        );
        assert!(matches!(
            fixture.catalog.commit_leadership_confirm(
                &prepared,
                LeadershipFenceOutcome::from_request(
                    &rogue_request,
                    prepared.fence_request().revision(),
                ),
                deadline + 2,
            ),
            Err(OperationsError::Conflict("leadership fence identity"))
        ));
        let intent = fixture
            .catalog
            .leadership_confirm_intent(Uuid::from_u128(0x5A01))?
            .ok_or("intent row missing")?;
        assert_eq!(intent.state(), LeadershipConfirmIntentState::Pending);
        drop(fixture.catalog);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    /// Scenario 2: same-host leader replacement. E1 confirms at revision 5,
    /// E2 confirms at revision 10 (the succession records over the earlier
    /// authority), and a late E1 retry resolves honestly without
    /// re-execution.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn same_host_leader_replacement_succeeds_and_late_retry_resolves()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut fixture, scope, _leader, candidate) =
            seeded_workspace_fixture("fence-succession", 0x5100)?;
        let first = T0 + STABILIZATION_WINDOW_MS;
        let (election_1, deadline_1) =
            run_majority_election(&mut fixture, scope, candidate, first + 1)?;
        let prepared_1 = prepare_confirm(
            &mut fixture,
            scope,
            election_1,
            Uuid::from_u128(0x5911),
            deadline_1 + 1,
        )?;
        let committed_1 = fixture.catalog.commit_leadership_confirm(
            &prepared_1,
            ok_fence(&prepared_1),
            deadline_1 + 2,
        )?;
        assert_eq!(committed_1.leadership_revision(), 6);

        // Second election for the same host: confirm lands at revision 10.
        let second = deadline_1 + 2 + STABILIZATION_WINDOW_MS;
        let reactivated = fixture.catalog.mark_stable(MarkStableCommand {
            scope,
            expected_revision: 6,
            now_ms: second,
        })?;
        assert_eq!(reactivated.leadership_revision(), 7);
        let (election_2, deadline_2) =
            run_majority_election(&mut fixture, scope, candidate, second + 1)?;
        assert_eq!(current_revision(&fixture, fixture.workspace)?, 9);
        let prepared_2 = prepare_confirm(
            &mut fixture,
            scope,
            election_2,
            Uuid::from_u128(0x5912),
            deadline_2 + 1,
        )?;
        let committed_2 = fixture.catalog.commit_leadership_confirm(
            &prepared_2,
            ok_fence(&prepared_2),
            deadline_2 + 2,
        )?;
        assert_eq!(committed_2.leadership_revision(), 10);

        // The late E1 retry with the same identity resolves honestly: the
        // committed intent plus the CURRENT leadership row, never re-executed.
        match fixture
            .catalog
            .prepare_leadership_confirm(PrepareLeadershipConfirmCommand {
                scope,
                election_id: election_1,
                expected_revision: 5,
                request_id: Uuid::from_u128(0x5911),
                now_ms: deadline_2 + 3,
            })? {
            PrepareLeadershipConfirmOutcome::Resolved(readback) => {
                assert_eq!(
                    readback.intent_state(),
                    LeadershipConfirmIntentState::Committed
                );
                assert_eq!(readback.state(), LeadershipState::Stabilizing);
                assert_eq!(readback.leadership_revision(), 10);
                assert_eq!(readback.leader_participant_id(), Some(candidate));
            }
            PrepareLeadershipConfirmOutcome::Prepared(_) => {
                return Err("a committed intent must not re-prepare".into());
            }
        }
        // The same participant may lead later elections. An old committed
        // request is not evidence for this newer recovery incident.
        let recovery = fixture
            .catalog
            .mark_recovery_required(MarkRecoveryRequiredCommand {
                scope,
                expected_revision: committed_2.leadership_revision(),
                reason: LeadershipRecoveryReason::CommitUnknown,
                now_ms: deadline_2 + 4,
            })?;
        assert!(matches!(
            fixture
                .catalog
                .reconcile_recovery(ReconcileRecoveryCommand {
                    scope,
                    intent_request_id: prepared_1.intent().request_id(),
                    expected_revision: recovery.leadership_revision(),
                    now_ms: deadline_2 + 5,
                }),
            Err(OperationsError::Conflict("leadership recovery facts"))
        ));
        let restored = fixture
            .catalog
            .reconcile_recovery(ReconcileRecoveryCommand {
                scope,
                intent_request_id: prepared_2.intent().request_id(),
                expected_revision: recovery.leadership_revision(),
                now_ms: deadline_2 + 5,
            })?;
        assert!(matches!(
            restored,
            LeadershipRecoveryResolution::ConfirmCompleted(_)
        ));
        drop(fixture.catalog);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    /// Scenario 3: a delayed remote reply whose local CAS precondition
    /// vanished resolves the intent as superseded atomically with the failed
    /// commit, and the reconcile re-prepare reports the current state.
    #[test]
    fn delayed_remote_reply_after_local_cas_change_supersedes_the_intent()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut fixture, scope, _leader, candidate) =
            seeded_workspace_fixture("fence-superseded", 0x5200)?;
        let (election_id, deadline) = run_majority_election(
            &mut fixture,
            scope,
            candidate,
            T0 + STABILIZATION_WINDOW_MS + 1,
        )?;
        let prepared_first = prepare_confirm(
            &mut fixture,
            scope,
            election_id,
            Uuid::from_u128(0x5921),
            deadline + 1,
        )?;
        // A second operator prepares the same election with a distinct
        // correlation identity; both intents are live and fencible.
        let prepared_second = prepare_confirm(
            &mut fixture,
            scope,
            election_id,
            Uuid::from_u128(0x5922),
            deadline + 1,
        )?;

        let committed = fixture.catalog.commit_leadership_confirm(
            &prepared_first,
            ok_fence(&prepared_first),
            deadline + 2,
        )?;
        assert_eq!(committed.leadership_revision(), 6);

        // The delayed reply for the second identity can no longer commit.
        assert!(matches!(
            fixture.catalog.commit_leadership_confirm(
                &prepared_second,
                ok_fence(&prepared_second),
                deadline + 3
            ),
            Err(OperationsError::Conflict("leadership revision"))
        ));
        let intent = fixture
            .catalog
            .leadership_confirm_intent(Uuid::from_u128(0x5922))?
            .ok_or("second intent row missing")?;
        assert_eq!(intent.state(), LeadershipConfirmIntentState::Superseded);
        assert_eq!(intent.resolved_at_ms(), Some(deadline + 3));
        assert_eq!(intent.remote_recorded_revision(), Some(5));
        assert_eq!(intent.confirmed_at_ms(), None);
        assert_eq!(current_revision(&fixture, fixture.workspace)?, 6);

        // The reconcile re-prepare reports the durable resolution honestly.
        match fixture
            .catalog
            .prepare_leadership_confirm(PrepareLeadershipConfirmCommand {
                scope,
                election_id,
                expected_revision: 5,
                request_id: Uuid::from_u128(0x5922),
                now_ms: deadline + 4,
            })? {
            PrepareLeadershipConfirmOutcome::Resolved(readback) => {
                assert_eq!(
                    readback.intent_state(),
                    LeadershipConfirmIntentState::Superseded
                );
                assert_eq!(readback.state(), LeadershipState::Stabilizing);
                assert_eq!(readback.leadership_revision(), 6);
            }
            PrepareLeadershipConfirmOutcome::Prepared(_) => {
                return Err("a superseded intent must not re-prepare".into());
            }
        }
        drop(fixture.catalog);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    /// Scenario 4: crash after prepare, restart, then same-identity resume:
    /// the pending intent survives, the fence is replayed with the SAME
    /// identity (an idempotent Broker readback), and the fenced commit
    /// completes atomically.
    #[test]
    fn crash_restart_resume_completes_the_fenced_confirm() -> Result<(), Box<dyn std::error::Error>>
    {
        let (mut fixture, scope, _leader, candidate) =
            seeded_workspace_fixture("fence-restart", 0x5300)?;
        let (election_id, deadline) = run_majority_election(
            &mut fixture,
            scope,
            candidate,
            T0 + STABILIZATION_WINDOW_MS + 1,
        )?;
        let request_id = Uuid::from_u128(0x5931);
        let prepared = prepare_confirm(&mut fixture, scope, election_id, request_id, deadline + 1)?;
        let path = fixture.root.join("catalog.sqlite3");
        let destination = fixture.destination;

        // Crash: drop the catalog mid-confirm, before the fence landed.
        drop(fixture.catalog);
        let mut reopened = OperationsCatalog::open(&path, destination)?;
        let intent = reopened
            .leadership_confirm_intent(request_id)?
            .ok_or("pending intent lost across restart")?;
        assert_eq!(intent.state(), LeadershipConfirmIntentState::Pending);
        let view = reopened
            .leadership_view(fixture.workspace, None, deadline + 1)?
            .ok_or("workspace view missing")?;
        assert_eq!(view.state(), LeadershipState::AuthorityPending);
        assert_eq!(view.leadership_revision(), FENCED_ELECTION_REVISION);

        // Same-identity resume: identical fence request, same owner.
        match reopened.prepare_leadership_confirm(PrepareLeadershipConfirmCommand {
            scope,
            election_id,
            expected_revision: FENCED_ELECTION_REVISION,
            request_id,
            now_ms: deadline + 2,
        })? {
            PrepareLeadershipConfirmOutcome::Prepared(resumed) => {
                assert_eq!(resumed.fence_request(), prepared.fence_request());
            }
            PrepareLeadershipConfirmOutcome::Resolved(_) => {
                return Err("a pending intent must resume, not resolve".into());
            }
        }
        let committed =
            reopened.commit_leadership_confirm(&prepared, ok_fence(&prepared), deadline + 3)?;
        assert_eq!(
            committed.leadership_revision(),
            FENCED_ELECTION_REVISION + 1
        );
        assert_eq!(committed.leader_participant_id(), Some(candidate));
        // A request_id reused with a DIFFERENT payload conflicts.
        assert!(matches!(
            reopened.prepare_leadership_confirm(PrepareLeadershipConfirmCommand {
                scope,
                election_id,
                expected_revision: FENCED_ELECTION_REVISION + 1,
                request_id,
                now_ms: deadline + 4,
            }),
            Err(OperationsError::Conflict("leadership confirm identity"))
        ));
        drop(reopened);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    /// Scenario 5: absent Broker (connection refused). The fence outcome is
    /// unknown, so the typed unconfirmed error is the honest surface, the
    /// leadership row stays `AUTHORITY_PENDING`, and the intent stays
    /// resumable.
    #[tokio::test]
    async fn broker_absent_preserves_authority_pending_and_surfaces_unconfirmed()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::net::{Ipv4Addr, SocketAddr};
        use std::time::Duration;

        use agent_broker_client::StaticClusterNode;

        let (mut fixture, scope, _leader, candidate) =
            seeded_workspace_fixture("fence-absent-broker", 0x5400)?;
        let (election_id, deadline) = run_majority_election(
            &mut fixture,
            scope,
            candidate,
            T0 + STABILIZATION_WINDOW_MS + 1,
        )?;
        let request_id = Uuid::from_u128(0x5941);
        let prepared = prepare_confirm(&mut fixture, scope, election_id, request_id, deadline + 1)?;

        // No server listens on these reserved discard endpoints. The production
        // fixed-three-node router must fail closed before sending a mutation.
        let localhost = Ipv4Addr::LOCALHOST.into();
        let fence = crate::BrokerAuthorityFence::new(crate::BrokerFenceConfig {
            nodes: [
                StaticClusterNode {
                    node_id: 1,
                    broker_address: SocketAddr::new(localhost, 9),
                    operations_address: SocketAddr::new(localhost, 19),
                },
                StaticClusterNode {
                    node_id: 2,
                    broker_address: SocketAddr::new(localhost, 10),
                    operations_address: SocketAddr::new(localhost, 20),
                },
                StaticClusterNode {
                    node_id: 3,
                    broker_address: SocketAddr::new(localhost, 11),
                    operations_address: SocketAddr::new(localhost, 21),
                },
            ],
            timeout: Duration::from_secs(1),
        })?;
        let recorded = fence.record(prepared.fence_request()).await;
        assert_eq!(recorded, Err(crate::LeadershipFenceError::Unconfirmed));
        // The typed fail-closed surface a host projects for this outcome.
        let typed = OperationsError::LeadershipFenceUnconfirmed {
            workspace_id: fixture.workspace.into_uuid(),
            election_id,
            request_id,
        };
        assert!(matches!(
            typed,
            OperationsError::LeadershipFenceUnconfirmed { .. }
        ));

        let view = fixture
            .catalog
            .leadership_view(fixture.workspace, None, deadline + 2)?
            .ok_or("workspace view missing")?;
        assert_eq!(view.state(), LeadershipState::AuthorityPending);
        assert_eq!(view.leadership_revision(), FENCED_ELECTION_REVISION);
        let intent = fixture
            .catalog
            .leadership_confirm_intent(request_id)?
            .ok_or("intent row missing")?;
        assert_eq!(intent.state(), LeadershipConfirmIntentState::Pending);
        // The same identity resumes cleanly once the Broker is reachable.
        let resumed = prepare_confirm(&mut fixture, scope, election_id, request_id, deadline + 3)?;
        assert_eq!(resumed.fence_request(), prepared.fence_request());
        drop(fixture.catalog);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    /// An exact exclusion permits failover but cannot fabricate a confirmation.
    #[test]
    fn stale_identity_excludes_the_intent_and_requires_failover()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut fixture, scope, leader, candidate) =
            seeded_workspace_fixture("fence-stale-reject", 0x5500)?;
        let (election_id, deadline) = run_majority_election(
            &mut fixture,
            scope,
            candidate,
            T0 + STABILIZATION_WINDOW_MS + 1,
        )?;
        let request_id = Uuid::from_u128(0x5951);
        let prepared = prepare_confirm(&mut fixture, scope, election_id, request_id, deadline + 1)?;
        let proof = LeadershipFenceExclusionProof::stale_for(prepared.fence_request());
        let readback =
            fixture
                .catalog
                .exclude_leadership_confirm(&prepared, proof.clone(), deadline + 2)?;
        assert_eq!(
            readback.intent_state(),
            LeadershipConfirmIntentState::Superseded
        );
        assert_eq!(readback.state(), LeadershipState::FailoverRequired);
        assert_eq!(readback.leader_participant_id(), Some(leader));
        assert_eq!(readback.leadership_revision(), FENCED_ELECTION_REVISION + 1);
        let intent = fixture
            .catalog
            .leadership_confirm_intent(request_id)?
            .ok_or("intent missing")?;
        assert_eq!(intent.confirmed_at_ms(), None);
        assert_eq!(intent.remote_recorded_revision(), None);
        assert_eq!(intent.resolved_at_ms(), Some(deadline + 2));
        let replay = fixture
            .catalog
            .exclude_leadership_confirm(&prepared, proof, deadline + 3)?;
        assert_eq!(replay, readback);
        assert!(matches!(
            fixture
                .catalog
                .prepare_leadership_confirm(PrepareLeadershipConfirmCommand {
                    scope,
                    election_id,
                    expected_revision: FENCED_ELECTION_REVISION,
                    request_id,
                    now_ms: deadline + 4,
                })?,
            PrepareLeadershipConfirmOutcome::Resolved(_)
        ));
        assert!(matches!(
            fixture
                .catalog
                .commit_leadership_confirm(&prepared, ok_fence(&prepared), deadline + 5),
            Err(OperationsError::Conflict(
                "leadership confirm already resolved"
            ))
        ));
        assert!(
            fixture
                .catalog
                .prepare_leadership_confirm(PrepareLeadershipConfirmCommand {
                    scope,
                    election_id,
                    expected_revision: FENCED_ELECTION_REVISION + 1,
                    request_id: Uuid::from_u128(0x5952),
                    now_ms: deadline + 6,
                })
                .is_err()
        );
        drop(fixture.catalog);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    /// The bootstrap two-step: evidence confirmation anchors stabilization,
    /// and a lost or retargeted offer projection returns the scope to
    /// `LEADERLESS` — no authority is ever fabricated from lost evidence.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn bootstrap_two_step_confirms_evidence_and_fails_closed_to_leaderless()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = fixture("bootstrap-evidence", 0x2A00)?;
        let participant = Uuid::from_u128(0x2A01);
        insert_member(&fixture, fixture.activity, participant, 0x2A02, 0x2A03)?;
        let assignment = insert_offer(&fixture, 1, fixture.activity, 0x2A04, 0x2A03, T0)?;
        let scope = LeadershipScope::activity(fixture.workspace, fixture.activity);

        let bootstrapped = fixture
            .catalog
            .bootstrap_leadership(BootstrapLeadershipCommand {
                scope,
                assignment_id: assignment,
                now_ms: T0,
            })?;
        assert_eq!(bootstrapped.state(), LeadershipState::Bootstrapping);
        assert_eq!(bootstrapped.leadership_revision(), 1);

        // Stale revisions fail closed before any evidence is read.
        assert!(matches!(
            fixture
                .catalog
                .confirm_bootstrap_evidence(ConfirmBootstrapEvidenceCommand {
                    scope,
                    assignment_id: assignment,
                    expected_revision: 2,
                    now_ms: T0 + 1,
                }),
            Err(OperationsError::Conflict("leadership revision"))
        ));

        // The durable readback promotes `BOOTSTRAPPING` to `STABILIZING`.
        let confirmed =
            fixture
                .catalog
                .confirm_bootstrap_evidence(ConfirmBootstrapEvidenceCommand {
                    scope,
                    assignment_id: assignment,
                    expected_revision: 1,
                    now_ms: T0 + 1,
                })?;
        assert_eq!(confirmed.state(), LeadershipState::Stabilizing);
        assert_eq!(confirmed.leader_participant_id(), Some(participant));
        assert_eq!(confirmed.leadership_revision(), 2);
        assert_eq!(
            confirmed.stabilization_until_ms(),
            Some(T0 + 1 + STABILIZATION_WINDOW_MS)
        );
        assert!(matches!(
            fixture.catalog.mark_stable(MarkStableCommand {
                scope,
                expected_revision: 2,
                now_ms: T0 + STABILIZATION_WINDOW_MS,
            }),
            Err(OperationsError::Conflict("leadership stabilization window"))
        ));

        // Failure path A: the offer projection vanished after seeding. The
        // typed evidence error surfaces AFTER the scope durably returned to
        // `LEADERLESS` with a recorded `VACATED` receipt.
        let lost_assignment = insert_offer(&fixture, 2, fixture.activity, 0x2A10, 0x2A03, T0 + 2)?;
        let workspace_scope = LeadershipScope::workspace(fixture.workspace);
        let _ = fixture
            .catalog
            .bootstrap_leadership(BootstrapLeadershipCommand {
                scope: workspace_scope,
                assignment_id: lost_assignment,
                now_ms: T0 + 2,
            })?;
        fixture.catalog.connection.execute(
            "DELETE FROM ops_assignment_offer_projection WHERE workspace_id=?1 AND assignment_id=?2",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                lost_assignment.into_uuid().as_bytes()
            ],
        )?;
        assert!(matches!(
            fixture
                .catalog
                .confirm_bootstrap_evidence(ConfirmBootstrapEvidenceCommand {
                    scope: workspace_scope,
                    assignment_id: lost_assignment,
                    expected_revision: 1,
                    now_ms: T0 + 3,
                }),
            Err(OperationsError::Conflict("leadership bootstrap evidence"))
        ));
        let view = fixture
            .catalog
            .leadership_view(fixture.workspace, None, T0 + 3)?
            .ok_or("workspace leadership view missing")?;
        assert_eq!(view.state(), LeadershipState::Leaderless);
        assert_eq!(view.leader_participant_id(), None);

        // Failure path B: the offer still exists but its target Agent no
        // longer resolves to a Ready binding (let alone the recorded
        // leader). The scope again returns to `LEADERLESS`.
        let orphaned_assignment =
            insert_offer(&fixture, 3, fixture.activity, 0x2A20, 0x2A03, T0 + 4)?;
        let _ = fixture
            .catalog
            .bootstrap_leadership(BootstrapLeadershipCommand {
                scope: workspace_scope,
                assignment_id: orphaned_assignment,
                now_ms: T0 + 4,
            })?;
        fixture.catalog.connection.execute(
            "DELETE FROM ops_current_effect_bindings WHERE workspace_id=?1 AND agent_id=?2",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                Uuid::from_u128(0x2A03).as_bytes()
            ],
        )?;
        assert!(matches!(
            fixture
                .catalog
                .confirm_bootstrap_evidence(ConfirmBootstrapEvidenceCommand {
                    scope: workspace_scope,
                    assignment_id: orphaned_assignment,
                    expected_revision: 3,
                    now_ms: T0 + 5,
                }),
            Err(OperationsError::Conflict("leadership bootstrap evidence"))
        ));
        let view = fixture
            .catalog
            .leadership_view(fixture.workspace, None, T0 + 5)?
            .ok_or("workspace leadership view missing")?;
        assert_eq!(view.state(), LeadershipState::Leaderless);
        assert_eq!(view.leader_participant_id(), None);
        let vacated_observations: i64 = fixture.catalog.connection.query_row(
            "SELECT COUNT(*) FROM ops_leadership_receipts WHERE workspace_id=?1 AND scope_kind=0 AND receipt_kind='VACATED'",
            params![fixture.workspace.into_uuid().as_bytes()],
            |row| row.get(0),
        )?;
        assert_eq!(vacated_observations, 2, "both evidence losses recorded");
        drop(fixture.catalog);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    /// `RECOVERY_REQUIRED` resolves only from durable facts: a committed
    /// intent proves the confirm landed and restores the confirmed
    /// leadership; a superseded intent resolves per the recorded facts; a
    /// live intent keeps the scope parked with an appended observation.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn recovery_required_reconciles_from_durable_facts() -> Result<(), Box<dyn std::error::Error>> {
        // Part 1: a commit whose outcome was reported unknown after the fact.
        // The durable intent readback shows `committed`, so the reconcile
        // restores the confirmed leadership exactly.
        let (mut fixture, scope, _leader, candidate) =
            seeded_workspace_fixture("recovery-committed", 0x2C00)?;
        let (election_id, deadline) = run_majority_election(
            &mut fixture,
            scope,
            candidate,
            T0 + STABILIZATION_WINDOW_MS + 1,
        )?;
        let request_id = Uuid::from_u128(0x2C01);
        let prepared = prepare_confirm(&mut fixture, scope, election_id, request_id, deadline + 1)?;
        let committed = fixture.catalog.commit_leadership_confirm(
            &prepared,
            ok_fence(&prepared),
            deadline + 2,
        )?;
        assert_eq!(
            committed.leadership_revision(),
            FENCED_ELECTION_REVISION + 1
        );
        let recovery = fixture
            .catalog
            .mark_recovery_required(MarkRecoveryRequiredCommand {
                scope,
                expected_revision: committed.leadership_revision(),
                reason: LeadershipRecoveryReason::CommitUnknown,
                now_ms: deadline + 3,
            })?;
        assert_eq!(recovery.state(), LeadershipState::RecoveryRequired);
        assert_eq!(recovery.leader_participant_id(), Some(candidate));
        assert_eq!(
            recovery.stabilization_until_ms(),
            committed.stabilization_until_ms()
        );
        // A foreign or unknown intent never resolves the recovery.
        assert!(matches!(
            fixture
                .catalog
                .reconcile_recovery(ReconcileRecoveryCommand {
                    scope,
                    intent_request_id: Uuid::from_u128(0x2CFF),
                    expected_revision: recovery.leadership_revision(),
                    now_ms: deadline + 4,
                }),
            Err(OperationsError::Conflict("leadership recovery intent"))
        ));
        let resolution = fixture
            .catalog
            .reconcile_recovery(ReconcileRecoveryCommand {
                scope,
                intent_request_id: request_id,
                expected_revision: recovery.leadership_revision(),
                now_ms: deadline + 4,
            })?;
        match resolution {
            LeadershipRecoveryResolution::ConfirmCompleted(receipt) => {
                assert_eq!(receipt.state(), LeadershipState::Stabilizing);
                assert_eq!(receipt.leader_participant_id(), Some(candidate));
                assert_eq!(
                    receipt.stabilization_until_ms(),
                    committed.stabilization_until_ms()
                );
            }
            _ => return Err("a committed intent must resolve confirm-completed".into()),
        }
        drop(fixture.catalog);
        std::fs::remove_dir_all(fixture.root)?;

        // Part 2: an unresolved pointer entered from `AUTHORITY_PENDING`
        // with two live intents. The live intent keeps the scope parked with
        // an observation; once the intent resolves as superseded (its commit
        // precondition vanished), the recorded facts restore the preserved
        // `AUTHORITY_PENDING` phase with the incumbent and election pointer.
        let (mut fixture, scope, leader, candidate) =
            seeded_workspace_fixture("recovery-superseded", 0x2D00)?;
        let (election_id, deadline) = run_majority_election(
            &mut fixture,
            scope,
            candidate,
            T0 + STABILIZATION_WINDOW_MS + 1,
        )?;
        let first = prepare_confirm(
            &mut fixture,
            scope,
            election_id,
            Uuid::from_u128(0x2D01),
            deadline + 1,
        )?;
        let second = prepare_confirm(
            &mut fixture,
            scope,
            election_id,
            Uuid::from_u128(0x2D02),
            deadline + 1,
        )?;
        let recovery = fixture
            .catalog
            .mark_recovery_required(MarkRecoveryRequiredCommand {
                scope,
                expected_revision: FENCED_ELECTION_REVISION,
                reason: LeadershipRecoveryReason::PointerUnreconciled,
                now_ms: deadline + 2,
            })?;
        assert_eq!(recovery.state(), LeadershipState::RecoveryRequired);
        assert_eq!(recovery.leader_participant_id(), Some(leader));
        assert_eq!(recovery.leadership_revision(), FENCED_ELECTION_REVISION + 1);

        // The live intent is unresolved: the scope stays in recovery and the
        // observation is appended (revision fenced, never a takeover).
        let unresolved = fixture
            .catalog
            .reconcile_recovery(ReconcileRecoveryCommand {
                scope,
                intent_request_id: Uuid::from_u128(0x2D02),
                expected_revision: recovery.leadership_revision(),
                now_ms: deadline + 3,
            })?;
        match unresolved {
            LeadershipRecoveryResolution::Unresolved(receipt) => {
                assert_eq!(receipt.state(), LeadershipState::RecoveryRequired);
                assert_eq!(
                    receipt.leadership_revision(),
                    recovery.leadership_revision() + 1
                );
            }
            _ => return Err("a live intent must stay unresolved".into()),
        }

        // The delayed commit for the superseded intent fails its vanished
        // precondition and resolves the intent as superseded evidence.
        assert!(matches!(
            fixture
                .catalog
                .commit_leadership_confirm(&second, ok_fence(&second), deadline + 4),
            Err(OperationsError::Conflict("leadership revision"))
        ));

        // A remaining live alias keeps recovery parked; it is discovered even
        // though the review no longer has an active election pointer.
        let pending = fixture.catalog.pending_leadership_confirms(None, 1)?;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].fence_request(), first.fence_request());
        assert!(
            fixture
                .catalog
                .pending_leadership_confirms(Some(first.intent().request_id()), 8)?
                .is_empty()
        );
        assert!(
            fixture
                .catalog
                .pending_leadership_confirms(None, 9)
                .is_err()
        );
        let resumed =
            fixture
                .catalog
                .prepare_leadership_confirm(PrepareLeadershipConfirmCommand {
                    scope,
                    election_id,
                    expected_revision: first.intent().expected_local_revision(),
                    request_id: first.intent().request_id(),
                    now_ms: deadline + 5,
                })?;
        let PrepareLeadershipConfirmOutcome::Prepared(resumed) = resumed else {
            return Err("live intent must replay after local drift".into());
        };
        assert_eq!(resumed.fence_request(), first.fence_request());
        fixture
            .catalog
            .recover_terminal_leadership(scope, deadline + 5)?;
        assert_eq!(
            fixture
                .catalog
                .leadership_scope_view(scope, deadline + 5)?
                .ok_or("scope missing")?
                .state(),
            LeadershipState::RecoveryRequired
        );
        assert!(
            fixture
                .catalog
                .commit_leadership_confirm(&first, ok_fence(&first), deadline + 6)
                .is_err()
        );
        let view = fixture
            .catalog
            .leadership_scope_view(scope, deadline + 6)?
            .ok_or("scope missing")?;
        assert_eq!(view.state(), LeadershipState::FailoverRequired);
        assert_eq!(view.leader_participant_id(), Some(leader));
        assert!(view.current_election().is_none());
        assert!(
            fixture
                .catalog
                .pending_leadership_confirms(None, 8)?
                .is_empty()
        );
        assert_eq!(
            receipt_kinds(&fixture, 0)?.last().map(String::as_str),
            Some("FAILOVER")
        );
        drop(fixture.catalog);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    /// Heartbeat-stale failover, full path: the incumbent's live presence
    /// stopped being touched during the still-running stabilization window.
    /// The operations catalog owns the bounded staleness predicate; failover
    /// bypasses the stabilization lock, the failover recall elects the
    /// replacement, and the fenced confirm installs it.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn heartbeat_stale_failover_bypasses_stabilization_and_replaces_the_incumbent()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = fixture("heartbeat-failover", 0x2E00)?;
        let leader = Uuid::from_u128(0x2E01);
        let candidate = Uuid::from_u128(0x2E02);
        insert_member(&fixture, fixture.activity, leader, 0x2E03, 0x2E04)?;
        insert_member(&fixture, fixture.activity, candidate, 0x2E05, 0x2E06)?;
        let assignment = insert_offer(&fixture, 1, fixture.activity, 0x2E07, 0x2E04, T0)?;
        let scope = LeadershipScope::activity(fixture.workspace, fixture.activity);
        let _ = bootstrap_and_confirm(&mut fixture, scope, assignment, T0, T0)?;

        // The incumbent's last heartbeat is older than the catalog-owned
        // unresponsive threshold while the stabilization window still runs.
        fixture.catalog.connection.execute(
            "INSERT INTO ops_logical_presence(workspace_id,participant_id,session_id,last_authenticated_at_ms,presence_revision) VALUES(?1,?2,?3,?4,2)",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                leader.as_bytes(),
                Uuid::from_u128(0x2E03).as_bytes(),
                T0.saturating_sub(LEADER_UNRESPONSIVE_AFTER_MS + 1),
            ],
        )?;
        let now = T0 + 1_000;

        // During the still-running window a plain recall is locked...
        assert!(matches!(
            fixture
                .catalog
                .start_recall_election(RecallElectionCommand {
                    scope,
                    expected_revision: 2,
                    candidate_participant_id: candidate,
                    now_ms: now,
                }),
            Err(OperationsError::Conflict("leadership not electable"))
        ));
        // ...but the catalog-owned heartbeat predicate bypasses the lock.
        let failed_over = fixture
            .catalog
            .mark_failover_if_unresponsive(MarkUnresponsiveFailoverCommand {
                scope,
                expected_revision: 2,
                now_ms: now,
            })?
            .ok_or("stale leader must fail over")?;
        assert_eq!(failed_over.state(), LeadershipState::FailoverRequired);

        // The failover recall election elects the replacement by majority.
        insert_credit(&fixture, 0x2E10, candidate, 0x2E06, 5, T0)?;
        let election = fixture
            .catalog
            .start_failover_recall_election(RecallElectionCommand {
                scope,
                expected_revision: 3,
                candidate_participant_id: candidate,
                now_ms: now + 1,
            })?;
        assert_eq!(election.electorate_size(), 2);
        for voter in [leader, candidate] {
            let ballot = fixture.catalog.cast_ballot(CastBallotCommand {
                election_id: election.election_id(),
                voter_participant_id: voter,
                choice: BallotChoice::Yes,
                now_ms: now + 2,
            })?;
            assert!(!ballot.replay());
        }
        let tally = fixture.catalog.close_election(CloseElectionCommand {
            election_id: election.election_id(),
            now_ms: election.ballot_deadline_ms(),
        })?;
        assert_eq!(tally.state(), ElectionState::ClosedElected);
        assert_eq!(tally.elected_participant_id(), Some(candidate));

        // The fenced confirm installs the replacement.
        let pending_view = fixture
            .catalog
            .leadership_view(fixture.workspace, Some(fixture.activity), now)?
            .ok_or("activity leadership view missing")?;
        let prepared =
            match fixture
                .catalog
                .prepare_leadership_confirm(PrepareLeadershipConfirmCommand {
                    scope,
                    election_id: election.election_id(),
                    expected_revision: pending_view.leadership_revision(),
                    request_id: Uuid::from_u128(0x2E20),
                    now_ms: election.ballot_deadline_ms() + 1,
                })? {
                PrepareLeadershipConfirmOutcome::Prepared(prepared) => prepared,
                PrepareLeadershipConfirmOutcome::Resolved(_) => {
                    return Err("a fresh confirm must prepare".into());
                }
            };
        let confirmed = fixture.catalog.commit_leadership_confirm(
            &prepared,
            ok_fence(&prepared),
            election.ballot_deadline_ms() + 2,
        )?;
        assert_eq!(confirmed.state(), LeadershipState::Stabilizing);
        assert_eq!(confirmed.leader_participant_id(), Some(candidate));
        assert_eq!(
            receipt_kinds(&fixture, 1)?,
            vec![
                "BOOTSTRAP".to_owned(),
                "CONFIRMED".to_owned(),
                "FAILOVER".to_owned(),
                "ELECTED".to_owned(),
                "CONFIRMED".to_owned()
            ]
        );
        drop(fixture.catalog);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    // ---- Real Broker process evidence ----------------------------------

    struct RunningBroker {
        child: std::process::Child,
        address: std::net::SocketAddr,
    }

    type ReadinessThread = std::thread::JoinHandle<std::io::Result<()>>;
    type ProductionFenceFixture = (crate::BrokerAuthorityFence, Vec<ReadinessThread>);

    impl Drop for RunningBroker {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    fn unused_loopback_address() -> std::io::Result<std::net::SocketAddr> {
        let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
        let address = listener.local_addr()?;
        drop(listener);
        Ok(address)
    }

    fn spawn_readiness_endpoint(
        node_id: u64,
        write_ready: bool,
        request_count: usize,
    ) -> std::io::Result<(
        std::net::SocketAddr,
        std::thread::JoinHandle<std::io::Result<()>>,
    )> {
        use std::io::{BufRead, Write};

        let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
        let address = listener.local_addr()?;
        let thread = std::thread::spawn(move || -> std::io::Result<()> {
            for _ in 0..request_count {
                let (mut stream, _) = listener.accept()?;
                stream.set_read_timeout(Some(std::time::Duration::from_secs(2)))?;
                stream.set_write_timeout(Some(std::time::Duration::from_secs(2)))?;
                let mut reader = std::io::BufReader::new(stream.try_clone()?);
                let mut request = String::new();
                reader.read_line(&mut request)?;
                if request != "{\"schema_version\":1,\"operation\":\"readiness\"}\n" {
                    return Err(std::io::Error::other(
                        "unexpected Broker operations readiness request",
                    ));
                }
                let consensus = if write_ready {
                    serde_json::json!({
                        "status": "ready",
                        "write_ready": true,
                        "progress": {"node_id": node_id, "current_leader": node_id}
                    })
                } else {
                    serde_json::json!({
                        "status": "follower",
                        "write_ready": false,
                        "progress": null
                    })
                };
                let response = serde_json::json!({
                    "schema_version": 1,
                    "operation": "readiness",
                    "live": true,
                    "write_ready": write_ready,
                    "reason": if write_ready { "ready" } else { "follower" },
                    "maintenance_authority": write_ready,
                    "consensus": consensus
                });
                serde_json::to_writer(&mut stream, &response).map_err(std::io::Error::other)?;
                stream.write_all(b"\n")?;
            }
            Ok(())
        });
        Ok((address, thread))
    }

    fn production_fence_for_real_broker(
        broker_address: std::net::SocketAddr,
        request_count: usize,
    ) -> Result<ProductionFenceFixture, Box<dyn std::error::Error>> {
        let (leader_ops, leader_thread) = spawn_readiness_endpoint(1, true, request_count)?;
        let (follower_two_ops, follower_two_thread) =
            spawn_readiness_endpoint(2, false, request_count)?;
        let (follower_three_ops, follower_three_thread) =
            spawn_readiness_endpoint(3, false, request_count)?;
        let config = crate::BrokerClusterConfig {
            nodes: [
                agent_broker_client::StaticClusterNode {
                    node_id: 1,
                    broker_address,
                    operations_address: leader_ops,
                },
                agent_broker_client::StaticClusterNode {
                    node_id: 2,
                    broker_address: unused_loopback_address()?,
                    operations_address: follower_two_ops,
                },
                agent_broker_client::StaticClusterNode {
                    node_id: 3,
                    broker_address: unused_loopback_address()?,
                    operations_address: follower_three_ops,
                },
            ],
            timeout: std::time::Duration::from_secs(5),
        };
        let fence = crate::BrokerAuthorityFence::new(config)?;
        Ok((
            fence,
            vec![leader_thread, follower_two_thread, follower_three_thread],
        ))
    }

    fn join_readiness_endpoints(
        threads: Vec<ReadinessThread>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for thread in threads {
            match thread.join() {
                Ok(result) => result?,
                Err(_) => return Err("Broker readiness fixture thread panicked".into()),
            }
        }
        Ok(())
    }

    /// Spawn the REAL standalone `agentbrokerd` binary (protocol-v3
    /// `authority.record` extension included) and read its `broker_ready`
    /// line for the bound loopback address.
    fn spawn_real_broker(
        state_path: &std::path::Path,
    ) -> Result<RunningBroker, Box<dyn std::error::Error>> {
        use std::io::BufRead;
        use std::process::{Command, Stdio};

        let binary = std::env::var("LANE_D_AGENTBROKERD").map_err(|_| {
            "LANE_D_AGENTBROKERD must name the built agentbrokerd binary".to_owned()
        })?;
        let mut child = Command::new(binary)
            .args([
                "serve",
                "--host",
                "127.0.0.1",
                "--port",
                "0",
                "--operations-port",
                "0",
                "--state-path",
            ])
            .arg(state_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let stdout = child
            .stdout
            .take()
            .ok_or("agentbrokerd stdout was not piped")?;
        let mut reader = std::io::BufReader::new(stdout);
        let mut ready_line = String::new();
        if reader.read_line(&mut ready_line)? == 0 {
            let _ = child.kill();
            let _ = child.wait();
            return Err("agentbrokerd exited before emitting broker_ready".into());
        }
        let ready: serde_json::Value = serde_json::from_str(ready_line.trim())?;
        if ready.get("event").and_then(serde_json::Value::as_str) != Some("broker_ready") {
            return Err(format!("unexpected agentbrokerd ready event: {ready_line}").into());
        }
        let host: std::net::IpAddr = ready
            .get("host")
            .and_then(serde_json::Value::as_str)
            .ok_or("broker_ready host missing")?
            .parse()?;
        let port = u16::try_from(
            ready
                .get("port")
                .and_then(serde_json::Value::as_u64)
                .ok_or("broker_ready port missing")?,
        )?;
        Ok(RunningBroker {
            child,
            address: std::net::SocketAddr::new(host, port),
        })
    }

    /// Scenario 7: the real Broker process through the production fixed-three-
    /// node readiness router. The fenced confirm records the EXACT gapped
    /// revision, succession over the same key records the later authority, a
    /// rogue authority at or below the recorded revision is answered
    /// `STALE_FENCE` (and the intent is resolved rejected), and the authority
    /// journal survives a full process restart (idempotent readback of the
    /// recorded revision). Requires the env var
    /// `LANE_D_AGENTBROKERD` to name the built `agentbrokerd` binary; the
    /// default suite passes without it.
    #[tokio::test]
    #[ignore = "requires LANE_D_AGENTBROKERD pointing at the built agentbrokerd binary from the Broker workspace"]
    async fn real_broker_process_fences_confirm_succession_and_restart()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut fixture, scope, _leader, candidate) =
            seeded_workspace_fixture("fence-real-broker", 0x5600)?;

        // E1 at AUTHORITY_PENDING@4.
        let (election_1, deadline_1) = run_majority_election(
            &mut fixture,
            scope,
            candidate,
            T0 + STABILIZATION_WINDOW_MS + 1,
        )?;
        let prepared_1 = prepare_confirm(
            &mut fixture,
            scope,
            election_1,
            Uuid::from_u128(0x5961),
            deadline_1 + 1,
        )?;

        let directory = fixture.root.join("broker-state");
        std::fs::create_dir(&directory)?;
        let state_path = directory.join("broker-state.json");
        let broker = spawn_real_broker(&state_path)?;
        // Standalone Broker rejections carry UNKNOWN disposition, so the
        // production router rediscovery policy resends the exact stale frame
        // three times before surfacing the stable STALE_FENCE code. Account
        // for initial record + successor record + three old-fence retries.
        let (fence, readiness_threads) = production_fence_for_real_broker(broker.address, 5)?;

        // Exact gapped revision recorded by the real Broker.
        let recorded = fence.record(prepared_1.fence_request()).await;
        assert_eq!(recorded, Ok(ok_fence(&prepared_1)));
        let committed_1 = fixture.catalog.commit_leadership_confirm(
            &prepared_1,
            ok_fence(&prepared_1),
            deadline_1 + 2,
        )?;
        assert_eq!(committed_1.leadership_revision(), 6);

        // E2: succession confirm at revision 10 over the SAME key.
        let second = deadline_1 + 2 + STABILIZATION_WINDOW_MS;
        let reactivated = fixture.catalog.mark_stable(MarkStableCommand {
            scope,
            expected_revision: 6,
            now_ms: second,
        })?;
        assert_eq!(reactivated.leadership_revision(), 7);
        let (election_2, deadline_2) =
            run_majority_election(&mut fixture, scope, candidate, second + 1)?;
        let prepared_2 = prepare_confirm(
            &mut fixture,
            scope,
            election_2,
            Uuid::from_u128(0x5962),
            deadline_2 + 1,
        )?;
        let prepared_3 = prepared_2;
        let recorded_3 = fence.record(prepared_3.fence_request()).await;
        assert_eq!(recorded_3, Ok(ok_fence(&prepared_3)));
        let committed_2 = fixture.catalog.commit_leadership_confirm(
            &prepared_3,
            ok_fence(&prepared_3),
            deadline_2 + 4,
        )?;
        assert_eq!(committed_2.leadership_revision(), 10);
        let Err(crate::LeadershipFenceError::Stale(proof)) =
            fence.record(prepared_1.fence_request()).await
        else {
            return Err("the old exact fence must be excluded after succession".into());
        };
        assert!(
            fixture
                .catalog
                .exclude_leadership_confirm(&prepared_3, proof.clone(), deadline_2 + 5)
                .is_err(),
            "proof for a different exact request cannot resolve this intent"
        );
        let historical =
            fixture
                .catalog
                .exclude_leadership_confirm(&prepared_1, proof, deadline_2 + 5)?;
        assert_eq!(
            historical.intent_state(),
            LeadershipConfirmIntentState::Committed
        );
        assert_eq!(historical.state(), LeadershipState::Stabilizing);
        assert_eq!(historical.leadership_revision(), 10);
        join_readiness_endpoints(readiness_threads)?;

        // Restart the Broker process on the SAME journal: the recorded
        // revision readback stays idempotent and the record survives.
        drop(broker);
        let restarted = spawn_real_broker(&state_path)?;
        let (restarted_fence, restarted_readiness_threads) =
            production_fence_for_real_broker(restarted.address, 1)?;
        let replayed = restarted_fence.record(prepared_3.fence_request()).await;
        assert_eq!(replayed, Ok(ok_fence(&prepared_3)));
        join_readiness_endpoints(restarted_readiness_threads)?;
        drop(restarted);

        drop(fixture.catalog);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn workspace_activity_and_team_scope_ids_do_not_collide() {
        let workspace = WorkspaceId::from_uuid(Uuid::from_u128(0xF100));
        let shared = Uuid::from_u128(0xF101);
        let activity = LeadershipScope::activity(workspace, ActivityId::from_uuid(shared));
        let team = LeadershipScope::team(workspace, shared);
        assert_ne!(activity, team);
        assert_eq!(activity.activity_id(), Some(ActivityId::from_uuid(shared)));
        assert_eq!(activity.team_id(), None);
        assert_eq!(team.activity_id(), None);
        assert_eq!(team.team_id(), Some(shared));
        assert_ne!(activity.scope_kind(), team.scope_kind());
    }

    // Append these focused unit tests inside `leadership.rs`'s existing `mod tests`.
    // They intentionally reuse its Fixture and confirm/election helpers.

    #[test]
    fn confirmed_work_exchange_is_scoped_durable_and_replayable()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut fixture, scope, leader, candidate) =
            seeded_workspace_fixture("work-exchange-reopen", 0x8A00)?;
        let (election_id, deadline) = run_majority_election(
            &mut fixture,
            scope,
            candidate,
            T0 + STABILIZATION_WINDOW_MS + 1,
        )?;
        let request_id = Uuid::from_u128(0x8A01);
        let prepared = prepare_confirm(&mut fixture, scope, election_id, request_id, deadline + 1)?;
        let fence_revision = prepared.fence_request().revision();
        let committed = fixture.catalog.commit_leadership_confirm(
            &prepared,
            ok_fence(&prepared),
            deadline + 2,
        )?;
        assert_eq!(committed.leader_participant_id(), Some(candidate));

        let record = fixture
            .catalog
            .leadership_work_exchange(scope, request_id)?
            .ok_or("committed leadership work-exchange record missing")?;
        assert_eq!(record.request_id(), request_id);
        assert_eq!(record.scope(), scope);
        assert_eq!(record.election_id(), election_id);
        assert_eq!(record.incumbent_participant_id(), Some(leader));
        assert_eq!(record.elected_participant_id(), candidate);
        let incumbent_participation = record
            .incumbent_participation_id()
            .ok_or("frozen incumbent participation missing")?;
        let elected_participation = record
            .elected_participation_id()
            .ok_or("frozen elected participation missing")?;
        assert_ne!(incumbent_participation, elected_participation);
        assert_eq!(
            record.leadership_revision(),
            committed.leadership_revision()
        );
        assert_eq!(record.fence_revision(), fence_revision);
        assert_eq!(record.confirmed_at_ms(), deadline + 2);
        assert_eq!(
            record.state(),
            LeadershipWorkExchangeState::AwaitingInventory
        );
        assert_eq!(
            record.reason(),
            LeadershipWorkExchangeReason::DistinctParticipation
        );

        assert_pending_work_exchange_page(&fixture, scope, request_id, record.state())?;

        let path = fixture.root.join("catalog.sqlite3");
        drop(fixture.catalog);
        let mut reopened = OperationsCatalog::open(&path, fixture.destination)?;
        let replay = reopened.prepare_leadership_confirm(PrepareLeadershipConfirmCommand {
            scope,
            election_id,
            expected_revision: FENCED_ELECTION_REVISION,
            request_id,
            now_ms: deadline + 3,
        })?;
        assert!(matches!(
            replay,
            PrepareLeadershipConfirmOutcome::Resolved(_)
        ));
        let reopened_record = reopened
            .leadership_work_exchange(scope, request_id)?
            .ok_or("work-exchange readback missing after reopen")?;
        assert_eq!(reopened_record.election_id(), election_id);
        assert_eq!(
            reopened_record.leadership_revision(),
            committed.leadership_revision()
        );
        assert_eq!(reopened_record.fence_revision(), fence_revision);
        assert_eq!(reopened_record.confirmed_at_ms(), deadline + 2);
        assert_eq!(
            reopened_record.state(),
            LeadershipWorkExchangeState::AwaitingInventory
        );
        let rows: i64 = reopened.connection.query_row(
            "SELECT COUNT(*) FROM ops_leadership_work_exchanges WHERE request_id=?1",
            params![request_id.as_bytes()],
            |row| row.get(0),
        )?;
        assert_eq!(rows, 1);

        drop(reopened);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    fn assert_pending_work_exchange_page(
        fixture: &Fixture,
        scope: LeadershipScope,
        request_id: Uuid,
        state: LeadershipWorkExchangeState,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let page =
            fixture
                .catalog
                .leadership_work_exchange_pending_page(fixture.workspace, None, 1)?;
        assert_eq!(page.records().len(), 1);
        assert_eq!(page.records()[0].request_id(), request_id);
        assert_eq!(page.records()[0].scope(), scope);
        assert_eq!(page.records()[0].state(), state);
        assert_eq!(page.next_cursor(), None);

        let after = fixture.catalog.leadership_work_exchange_pending_page(
            fixture.workspace,
            Some(request_id),
            1,
        )?;
        assert!(after.records().is_empty());
        assert!(after.next_cursor().is_none());
        assert!(
            fixture
                .catalog
                .leadership_work_exchange_pending_page(fixture.workspace, None, 0)
                .is_err()
        );
        assert!(
            fixture
                .catalog
                .leadership_work_exchange_pending_page(fixture.workspace, None, 33)
                .is_err()
        );
        assert!(
            fixture
                .catalog
                .leadership_work_exchange_pending_page(
                    WorkspaceId::from_uuid(Uuid::from_u128(0x8AFF)),
                    Some(request_id),
                    1,
                )
                .is_err()
        );
        assert!(
            fixture
                .catalog
                .leadership_work_exchange(
                    LeadershipScope::activity(fixture.workspace, fixture.activity),
                    request_id,
                )
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn v39_reopen_adds_empty_work_exchange_journal_without_backfill()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut fixture, scope, _, candidate) =
            seeded_workspace_fixture("work-exchange-v39-migration", 0x8C00)?;
        let (election_id, deadline) = run_majority_election(
            &mut fixture,
            scope,
            candidate,
            T0 + STABILIZATION_WINDOW_MS + 1,
        )?;
        let request_id = Uuid::from_u128(0x8C01);
        let prepared = prepare_confirm(&mut fixture, scope, election_id, request_id, deadline + 1)?;
        let _committed = fixture.catalog.commit_leadership_confirm(
            &prepared,
            ok_fence(&prepared),
            deadline + 2,
        )?;
        assert!(
            fixture
                .catalog
                .leadership_work_exchange(scope, request_id)?
                .is_some()
        );

        let path = fixture.root.join("catalog.sqlite3");
        crate::binding_rejection::drop_schema_for_legacy_fixture(&fixture.catalog.connection)?;
        work_roster::drop_schema_for_legacy_fixture(&fixture.catalog.connection)?;
        fixture.catalog.connection.execute_batch(
            "DROP TABLE ops_agent_provision_uncertainty_retirements;
             DROP TABLE ops_registration_profile_scopes;
             DROP INDEX ops_leadership_work_exchanges_pending;
             DROP TABLE ops_leadership_work_exchanges;",
        )?;
        crate::catalog::restore_v46_message_schema_for_legacy_fixture(&fixture.catalog.connection)?;
        crate::catalog::restore_v47_assignment_schema_for_legacy_fixture(
            &fixture.catalog.connection,
        )?;
        fixture
            .catalog
            .connection
            .pragma_update(None, "user_version", 39_i64)?;
        drop(fixture.catalog);

        let migrated = OperationsCatalog::open(&path, fixture.destination)?;
        let version: i64 = migrated
            .connection
            .pragma_query_value(None, "user_version", |row| row.get(0))?;
        assert_eq!(version, crate::catalog::VERSION);
        let rows: i64 = migrated.connection.query_row(
            "SELECT COUNT(*) FROM ops_leadership_work_exchanges",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(rows, 0);
        assert!(
            migrated
                .leadership_work_exchange(scope, request_id)?
                .is_none()
        );

        drop(migrated);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn failed_work_exchange_insert_rolls_back_the_confirm_and_can_be_retried()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut fixture, scope, leader, candidate) =
            seeded_workspace_fixture("work-exchange-atomicity", 0x8B00)?;
        let (election_id, deadline) = run_majority_election(
            &mut fixture,
            scope,
            candidate,
            T0 + STABILIZATION_WINDOW_MS + 1,
        )?;
        let request_id = Uuid::from_u128(0x8B01);
        let prepared = prepare_confirm(&mut fixture, scope, election_id, request_id, deadline + 1)?;
        fixture.catalog.connection.execute_batch(
            "CREATE TRIGGER reject_leadership_work_exchange_insert
             BEFORE INSERT ON ops_leadership_work_exchanges
             BEGIN SELECT RAISE(ABORT, 'injected work-exchange insert failure'); END;",
        )?;

        assert!(
            fixture
                .catalog
                .commit_leadership_confirm(&prepared, ok_fence(&prepared), deadline + 2)
                .is_err()
        );
        let unchanged = fixture
            .catalog
            .leadership_scope_view(scope, deadline + 2)?
            .ok_or("leadership view missing after rolled-back confirm")?;
        assert_eq!(unchanged.state(), LeadershipState::AuthorityPending);
        assert_eq!(unchanged.leader_participant_id(), Some(leader));
        assert_eq!(unchanged.leadership_revision(), FENCED_ELECTION_REVISION);
        assert_eq!(
            unchanged
                .current_election()
                .map(LeadershipElectionSummary::election_id),
            Some(election_id)
        );
        assert_eq!(
            fixture
                .catalog
                .leadership_confirm_intent(request_id)?
                .ok_or("confirm intent missing after rollback")?
                .state(),
            LeadershipConfirmIntentState::Pending
        );
        let confirmed_receipts: i64 = fixture.catalog.connection.query_row(
            "SELECT COUNT(*) FROM ops_leadership_receipts
             WHERE workspace_id=?1 AND scope_kind=?2 AND election_id=?3 AND receipt_kind='CONFIRMED'",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                scope.scope_kind(),
                election_id.as_bytes(),
            ],
            |row| row.get(0),
        )?;
        assert_eq!(confirmed_receipts, 0);
        let obligations: i64 = fixture.catalog.connection.query_row(
            "SELECT COUNT(*) FROM ops_leadership_work_exchanges WHERE request_id=?1",
            params![request_id.as_bytes()],
            |row| row.get(0),
        )?;
        assert_eq!(obligations, 0);

        fixture
            .catalog
            .connection
            .execute_batch("DROP TRIGGER reject_leadership_work_exchange_insert")?;
        let committed = fixture.catalog.commit_leadership_confirm(
            &prepared,
            ok_fence(&prepared),
            deadline + 3,
        )?;
        assert_eq!(committed.state(), LeadershipState::Stabilizing);
        assert_eq!(
            fixture
                .catalog
                .leadership_work_exchange(scope, request_id)?
                .ok_or("retried work-exchange record missing")?
                .state(),
            LeadershipWorkExchangeState::AwaitingInventory
        );

        drop(fixture.catalog);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    // Append this test and helper inside the existing `leadership.rs` `mod tests`.
    // The leadership fixture does not own Profile/Session catalogs, so it seeds
    // the existing Operations Profile/Session mapping rows directly. The real
    // election path then freezes that mapping. The missing-witness half deletes a
    // frozen row after election creation as a historical gap fixture; neither half
    // claims end-to-end coverage of the public Profile mapping API.

    fn seed_profile_alias_for_leader(
        fixture: &Fixture,
        leader: Uuid,
        alias: Uuid,
        seed: u128,
    ) -> Result<Uuid, Box<dyn std::error::Error>> {
        let (profile, participation): (Vec<u8>, Vec<u8>) = fixture.catalog.connection.query_row(
            "SELECT agent_profile_id,participation_id
             FROM ops_workspace_participation_mappings
             WHERE workspace_id=?1 AND participant_id=?2",
            params![fixture.workspace.into_uuid().as_bytes(), leader.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        insert_member(fixture, fixture.activity, alias, seed + 0x501, seed + 0x502)?;
        fixture.catalog.connection.execute(
            "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,1)",
            params![
                Uuid::from_u128(seed + 0x503).as_bytes(),
                fixture.workspace.into_uuid().as_bytes(),
                participation.as_slice(),
                profile.as_slice(),
                Uuid::from_u128(seed + 0x504).as_bytes(),
                Uuid::from_u128(seed + 0x501).as_bytes(),
                alias.as_bytes(),
            ],
        )?;
        let mapping_count: i64 = fixture.catalog.connection.query_row(
            "SELECT COUNT(*) FROM ops_workspace_participation_mappings
             WHERE workspace_id=?1 AND agent_profile_id=?2 AND participation_id=?3",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                profile.as_slice(),
                participation.as_slice(),
            ],
            |row| row.get(0),
        )?;
        assert_eq!(mapping_count, 2);
        Ok(Uuid::from_slice(&participation)?)
    }

    #[test]
    fn work_exchange_frozen_alias_and_missing_identity_remain_distinct()
    -> Result<(), Box<dyn std::error::Error>> {
        for (seed, missing, expected_state, expected_reason) in [
            (
                0x8D00,
                false,
                LeadershipWorkExchangeState::NoExchangeRequired,
                LeadershipWorkExchangeReason::SameParticipation,
            ),
            (
                0x8E00,
                true,
                LeadershipWorkExchangeState::AwaitingIdentity,
                LeadershipWorkExchangeReason::MissingFrozenIdentity,
            ),
        ] {
            let (mut fixture, scope, leader, _) =
                seeded_workspace_fixture("work-exchange-identity", seed)?;
            let alias = Uuid::from_u128(seed + 0x50);
            let shared = seed_profile_alias_for_leader(&fixture, leader, alias, seed)?;
            insert_credit(&fixture, seed + 0x60, alias, seed + 0x502, 5, T0)?;
            let (election_id, deadline) = run_majority_election(
                &mut fixture,
                scope,
                alias,
                T0 + STABILIZATION_WINDOW_MS + 1,
            )?;
            if missing {
                // Historical evidence gap only: do not substitute current mappings.
                fixture.catalog.connection.execute(
                    "DELETE FROM ops_election_work_credit_snapshots WHERE election_id=?1",
                    params![election_id.as_bytes()],
                )?;
            }
            let request_id = Uuid::from_u128(seed + 0x70);
            let prepared =
                prepare_confirm(&mut fixture, scope, election_id, request_id, deadline + 1)?;
            let committed = fixture.catalog.commit_leadership_confirm(
                &prepared,
                ok_fence(&prepared),
                deadline + 2,
            )?;
            assert_eq!(committed.leader_participant_id(), Some(alias));
            let record = fixture
                .catalog
                .leadership_work_exchange(scope, request_id)?
                .ok_or("identity obligation missing")?;
            assert_eq!(record.state(), expected_state);
            assert_eq!(record.reason(), expected_reason);
            assert_eq!(record.incumbent_participant_id(), Some(leader));
            assert_eq!(record.elected_participant_id(), alias);
            assert_eq!(record.incumbent_participation_id(), Some(shared));
            assert_eq!(
                record.elected_participation_id(),
                if missing { None } else { Some(shared) }
            );
            let pending = fixture.catalog.leadership_work_exchange_pending_page(
                fixture.workspace,
                None,
                32,
            )?;
            assert_eq!(pending.records().len(), usize::from(missing));
            drop(fixture.catalog);
            std::fs::remove_dir_all(fixture.root)?;
        }
        Ok(())
    }

    fn prepare_test_roster(
        fixture: &mut Fixture,
        scope: LeadershipScope,
        election_id: Uuid,
        cut_id: Uuid,
        now_ms: i64,
    ) -> Result<work_roster::LeadershipWorkRoster, Box<dyn std::error::Error>> {
        let expected_revision = current_revision(fixture, scope.workspace_id())?;
        match fixture.catalog.prepare_leadership_work_roster(
            scope,
            election_id,
            expected_revision,
            cut_id,
            now_ms,
        )? {
            work_roster::LeadershipWorkRosterPrepareOutcome::Recorded(roster) => Ok(*roster),
            other => Err(format!("expected a complete roster, received {other:?}").into()),
        }
    }

    fn work_roster_barrier_count(fixture: &Fixture) -> Result<i64, Box<dyn std::error::Error>> {
        Ok(fixture.catalog.connection.query_row(
            "SELECT COUNT(*) FROM ops_leadership_work_roster_barriers WHERE workspace_id=?1",
            params![fixture.workspace.into_uuid().as_bytes()],
            |row| row.get(0),
        )?)
    }

    fn assert_roster_admission_closed(
        fixture: &Fixture,
        profile_id: Uuid,
        session_id: forge_domain::AgentSessionId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let profile = heterarchy_orchestration_sessions::AgentProfileId::from_uuid(profile_id)?;
        assert!(
            work_roster::ensure_profile_admission_open(
                &fixture.catalog.connection,
                fixture.workspace,
                profile,
            )
            .is_err()
        );
        assert!(
            work_roster::ensure_session_admission_open(
                &fixture.catalog.connection,
                fixture.workspace,
                session_id,
            )
            .is_err()
        );
        assert!(
            work_roster::ensure_unscoped_admission_open(
                &fixture.catalog.connection,
                fixture.workspace,
            )
            .is_err()
        );
        Ok(())
    }

    fn assert_roster_admission_open(
        fixture: &Fixture,
        profile_id: Uuid,
        session_id: forge_domain::AgentSessionId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let profile = heterarchy_orchestration_sessions::AgentProfileId::from_uuid(profile_id)?;
        work_roster::ensure_profile_admission_open(
            &fixture.catalog.connection,
            fixture.workspace,
            profile,
        )?;
        work_roster::ensure_session_admission_open(
            &fixture.catalog.connection,
            fixture.workspace,
            session_id,
        )?;
        work_roster::ensure_unscoped_admission_open(
            &fixture.catalog.connection,
            fixture.workspace,
        )?;
        Ok(())
    }

    fn reopen_and_release_roster(
        mut fixture: Fixture,
        scope: LeadershipScope,
        election_id: Uuid,
        cut_id: Uuid,
        deadline: i64,
        seed: u128,
        digest: [u8; 32],
    ) -> Result<Fixture, Box<dyn std::error::Error>> {
        let destination = fixture.destination;
        drop(fixture.catalog);
        fixture.catalog =
            OperationsCatalog::open(&fixture.root.join("catalog.sqlite3"), destination)?;
        let replay = prepare_test_roster(&mut fixture, scope, election_id, cut_id, deadline + 1)?;
        assert_eq!(replay.digest(), digest);
        assert_eq!(
            fixture
                .catalog
                .leadership_work_roster(scope, cut_id)?
                .map(|row| row.digest()),
            Some(digest),
        );
        let incumbent_session =
            forge_domain::AgentSessionId::from_uuid(Uuid::from_u128(seed + 0x101));
        let elected_session =
            forge_domain::AgentSessionId::from_uuid(Uuid::from_u128(seed + 0x201));
        assert_roster_admission_closed(
            &fixture,
            Uuid::from_u128(seed + 0x6100),
            incumbent_session,
        )?;
        assert_roster_admission_closed(&fixture, Uuid::from_u128(seed + 0x6101), elected_session)?;

        let released = fixture.catalog.release_unbound_leadership_work_roster(
            scope,
            cut_id,
            1,
            deadline + 2,
        )?;
        assert_eq!(
            released.state(),
            work_roster::LeadershipWorkRosterState::Released
        );
        assert_eq!(released.digest(), digest);
        assert_eq!(work_roster_barrier_count(&fixture)?, 0);
        assert_roster_admission_open(&fixture, Uuid::from_u128(seed + 0x6100), incumbent_session)?;
        assert_roster_admission_open(&fixture, Uuid::from_u128(seed + 0x6101), elected_session)?;
        let released_replay = fixture.catalog.release_unbound_leadership_work_roster(
            scope,
            cut_id,
            1,
            deadline + 2,
        )?;
        assert_eq!(released_replay, released);
        Ok(fixture)
    }

    fn assert_stale_current_session_rejected(
        fixture: &mut Fixture,
        scope: LeadershipScope,
        election_id: Uuid,
        participant_id: Uuid,
        stale_cut: Uuid,
        now_ms: i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Raw fixture corruption: current logical Session diverges from its
        // still-current effect binding. The cut must reject rather than omit it.
        fixture.catalog.connection.execute(
            "UPDATE ops_logical_members SET session_id=?1 WHERE workspace_id=?2 AND participant_id=?3",
            params![
                Uuid::from_u128(stale_cut.as_u128() + 0x100).as_bytes(),
                fixture.workspace.into_uuid().as_bytes(),
                participant_id.as_bytes(),
            ],
        )?;
        let expected_revision = current_revision(fixture, scope.workspace_id())?;
        assert!(matches!(
            fixture.catalog.prepare_leadership_work_roster(
                scope,
                election_id,
                expected_revision,
                stale_cut,
                now_ms
            ),
            Err(crate::OperationsError::Conflict(
                "work roster stale current binding Session"
            ))
        ));
        assert_eq!(work_roster_barrier_count(fixture)?, 0);
        assert!(
            fixture
                .catalog
                .leadership_work_roster(scope, stale_cut)?
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn prepared_roster_replays_reopens_blocks_scoped_admission_and_releases_atomically()
    -> Result<(), Box<dyn std::error::Error>> {
        let seed = 0x8D00;
        let (mut fixture, scope, incumbent, elected) =
            seeded_workspace_fixture("work-roster-reopen", seed)?;
        let (election_id, deadline) = run_majority_election(
            &mut fixture,
            scope,
            elected,
            T0 + STABILIZATION_WINDOW_MS + 2,
        )?;
        let cut_id = Uuid::from_u128(seed + 0x800);
        let roster = prepare_test_roster(&mut fixture, scope, election_id, cut_id, deadline + 1)?;
        assert_eq!(
            roster.state(),
            work_roster::LeadershipWorkRosterState::Prepared
        );
        for (side, offset) in [
            (work_roster::LeadershipWorkRosterSide::Incumbent, 0),
            (work_roster::LeadershipWorkRosterSide::Elected, 1),
        ] {
            assert_eq!(
                roster.participation_id(side),
                Uuid::from_u128(seed + 0x6200 + offset)
            );
            assert_eq!(
                roster.profile_id(side),
                Uuid::from_u128(seed + 0x6100 + offset)
            );
        }
        assert_eq!(roster.sessions().len(), 2);
        assert_eq!(roster.bindings().len(), 2);
        assert_eq!(work_roster_barrier_count(&fixture)?, 2);

        let incumbent_session =
            forge_domain::AgentSessionId::from_uuid(Uuid::from_u128(seed + 0x101));
        let elected_session =
            forge_domain::AgentSessionId::from_uuid(Uuid::from_u128(seed + 0x201));
        assert_roster_admission_closed(
            &fixture,
            Uuid::from_u128(seed + 0x6100),
            incumbent_session,
        )?;
        assert_roster_admission_closed(&fixture, Uuid::from_u128(seed + 0x6101), elected_session)?;
        assert!(
            roster
                .sessions()
                .any(|item| item.session_id() == elected_session)
        );
        let fresh_session = forge_domain::AgentSessionId::from_uuid(Uuid::from_u128(seed + 0x900));
        assert!(
            work_roster::ensure_identity_admission_open(
                &fixture.catalog.connection,
                fixture.workspace,
                fresh_session,
                Some(
                    heterarchy_orchestration_sessions::AgentProfileId::from_uuid(
                        roster.profile_id(work_roster::LeadershipWorkRosterSide::Elected)
                    )?
                ),
            )
            .is_err()
        );
        work_roster::ensure_identity_admission_open(
            &fixture.catalog.connection,
            fixture.workspace,
            fresh_session,
            Some(
                heterarchy_orchestration_sessions::AgentProfileId::from_uuid(Uuid::from_u128(
                    seed + 0x61FF,
                ))?,
            ),
        )?;

        fixture = reopen_and_release_roster(
            fixture,
            scope,
            election_id,
            cut_id,
            deadline,
            seed,
            roster.digest(),
        )?;

        let stale_cut = Uuid::from_u128(seed + 0x801);
        assert_stale_current_session_rejected(
            &mut fixture,
            scope,
            election_id,
            incumbent,
            stale_cut,
            deadline + 3,
        )?;

        drop(fixture.catalog);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn roster_dispatch_is_once_and_readback_survives_confirm_and_import_changes()
    -> Result<(), Box<dyn std::error::Error>> {
        let seed = 0x8E00;
        let (mut fixture, scope, _incumbent, elected) =
            seeded_workspace_fixture("work-roster-dispatch", seed)?;
        let (election_id, deadline) = run_majority_election(
            &mut fixture,
            scope,
            elected,
            T0 + STABILIZATION_WINDOW_MS + 2,
        )?;
        let cut_id = Uuid::from_u128(seed + 0x800);
        let roster = prepare_test_roster(&mut fixture, scope, election_id, cut_id, deadline + 1)?;
        let dispatch =
            fixture
                .catalog
                .dispatch_leadership_work_roster(scope, cut_id, 1, deadline + 2)?;
        let dispatched = match dispatch {
            work_roster::LeadershipWorkRosterDispatch::Execute(value) => value,
            other @ work_roster::LeadershipWorkRosterDispatch::Readback(_) => {
                return Err(format!("first dispatch must execute once: {other:?}").into());
            }
        };
        assert_eq!(
            dispatched.state(),
            work_roster::LeadershipWorkRosterState::Dispatched
        );
        assert_eq!(dispatched.digest(), roster.digest());
        let retry =
            fixture
                .catalog
                .dispatch_leadership_work_roster(scope, cut_id, 1, deadline + 3)?;
        let readback = match retry {
            work_roster::LeadershipWorkRosterDispatch::Readback(value) => value,
            other @ work_roster::LeadershipWorkRosterDispatch::Execute(_) => {
                return Err(format!("dispatch retry must only read back: {other:?}").into());
            }
        };
        assert_eq!(readback.digest(), roster.digest());
        assert!(
            fixture
                .catalog
                .release_unbound_leadership_work_roster(scope, cut_id, 1, deadline + 3,)
                .is_err(),
            "a dispatched roster cannot be released without Forge proof"
        );

        let expected_revision = current_revision(&fixture, scope.workspace_id())?;
        let prepared = prepare_confirm(
            &mut fixture,
            scope,
            election_id,
            Uuid::from_u128(seed + 0x810),
            deadline + 4,
        )?;
        assert_eq!(
            prepared.intent().expected_local_revision(),
            expected_revision
        );
        let barrier_count_before = work_roster_barrier_count(&fixture)?;
        assert!(matches!(
            fixture.catalog.prepare_leadership_work_roster(
                scope,
                election_id,
                expected_revision,
                Uuid::from_u128(seed + 0x811),
                deadline + 5,
            ),
            Err(crate::OperationsError::Conflict(
                "work roster must precede confirmation"
            ))
        ));
        assert_eq!(work_roster_barrier_count(&fixture)?, barrier_count_before);

        let _confirmed = fixture.catalog.commit_leadership_confirm(
            &prepared,
            ok_fence(&prepared),
            deadline + 6,
        )?;
        fixture.catalog.connection.execute(
            "UPDATE ops_imports SET phase=3 WHERE migration_id=?1 AND phase=2",
            params![fixture.migration.as_bytes()],
        )?;
        let durable = fixture
            .catalog
            .leadership_work_roster(scope, cut_id)?
            .ok_or("immutable roster readback missing after source changes")?;
        assert_eq!(
            durable.state(),
            work_roster::LeadershipWorkRosterState::Dispatched
        );
        assert_eq!(durable.digest(), roster.digest());

        drop(fixture.catalog);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }
    #[test]
    fn prepared_roster_cannot_release_after_confirmation_starts()
    -> Result<(), Box<dyn std::error::Error>> {
        let seed = 0x8F00;
        let (mut fixture, scope, _incumbent, elected) =
            seeded_workspace_fixture("work-roster-confirm-release", seed)?;
        let (election_id, deadline) = run_majority_election(
            &mut fixture,
            scope,
            elected,
            T0 + STABILIZATION_WINDOW_MS + 2,
        )?;
        let cut_id = Uuid::from_u128(seed + 0x800);
        let roster = prepare_test_roster(&mut fixture, scope, election_id, cut_id, deadline + 1)?;
        // The host confirmation-to-roster gate is not integrated yet. Even if a
        // confirmation starts, the prepared cut must retain its identity barriers.
        let _intent = prepare_confirm(
            &mut fixture,
            scope,
            election_id,
            Uuid::from_u128(seed + 0x810),
            deadline + 2,
        )?;
        assert!(matches!(
            fixture
                .catalog
                .release_unbound_leadership_work_roster(scope, cut_id, 1, deadline + 3),
            Err(crate::OperationsError::Conflict(
                "work roster must precede confirmation"
            ))
        ));
        assert_eq!(work_roster_barrier_count(&fixture)?, 2);
        assert_eq!(
            fixture.catalog.leadership_work_roster(scope, cut_id)?,
            Some(roster)
        );
        drop(fixture.catalog);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }
}
