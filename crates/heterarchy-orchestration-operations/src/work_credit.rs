//! Deterministic append-only work-credit ledger derived from consumed
//! assignment-review and execution-terminal effect facts.
//!
//! One credit belongs to exactly one Workspace assignment. It is derived only
//! when both durable witnesses agree inside one consumer transaction: an
//! `Accepted` assignment review with a non-NULL credit budget and one
//! verification-`Accepted` execution terminal projection for the same
//! Workspace, Agent, Goal and DAG node. Rejected reviews and credit-ineligible
//! assignments never award. The ledger has no UPDATE or DELETE path anywhere;
//! replays of already-derived credits are exact no-ops and conflicting rows
//! fail closed.

use crate::{
    MAX_WORKSPACE_PARTICIPATIONS_PER_READ, OperationsCatalog, OperationsError,
    catalog::commit_read, effect_binding, wire,
};
use forge_domain::{
    ActivityId, AgentAssignmentId, AgentInstanceId, AgentSessionId, ChangeSetId, DagNodeId, DiffId,
    ExecutionId, GoalId, WorkCreditBudget, WorkspaceId,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Score window length: 30 days in milliseconds. The window is applied by the
/// caller-supplied `now_ms` parameter; the operations layer owns no clock.
pub const WORK_CREDIT_WINDOW_MS: i64 = 30 * 24 * 3600 * 1000;

/// Upper bound on entries returned by one bounded ledger read.
const LEDGER_READ_LIMIT: usize = 64;

/// Upper bound on rows returned by one bounded provenance join read.
const PROVENANCE_READ_LIMIT: usize = 64;

/// Bound each score/fulfillment source projection to the 256-row Workspace roster budget.
const WORK_CREDIT_SCORE_SOURCE_READ_LIMIT: usize = MAX_WORKSPACE_PARTICIPATIONS_PER_READ;

const CREDIT_ID_DOMAIN: &[u8] = b"heterarchy-orchestration-work-credit-id-v1";
const CREATED_DIGEST_DOMAIN: &[u8] = b"heterarchy.orchestration.work-credit.v1\0";

const LEDGER_SQL: &str = "CREATE TABLE ops_work_credit_entries (credit_id BLOB PRIMARY KEY NOT NULL CHECK(length(credit_id)=16), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), assignment_id BLOB NOT NULL CHECK(length(assignment_id)=16), participant_id BLOB CHECK(participant_id IS NULL OR length(participant_id)=16), agent_id BLOB NOT NULL CHECK(length(agent_id)=16), activity_id BLOB NOT NULL CHECK(length(activity_id)=16), goal_id BLOB NOT NULL CHECK(length(goal_id)=16), node_id BLOB NOT NULL CHECK(length(node_id)=16), budget INTEGER NOT NULL CHECK(budget IN(1,3,5)), award_source_sequence INTEGER NOT NULL CHECK(award_source_sequence>=1), evidence_execution_id BLOB NOT NULL CHECK(length(evidence_execution_id)=16), awarded_at_ms INTEGER NOT NULL CHECK(awarded_at_ms>=0), created_digest BLOB NOT NULL CHECK(length(created_digest)=32), UNIQUE(workspace_id,assignment_id))";

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![("table", "ops_work_credit_entries", LEDGER_SQL)]
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    connection.execute_batch(LEDGER_SQL)?;
    Ok(())
}

/// One durable work-credit ledger row read back from the catalog.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkCreditEntryRow {
    credit_id: Uuid,
    workspace_id: WorkspaceId,
    assignment_id: AgentAssignmentId,
    participant_id: Option<Uuid>,
    agent_id: AgentInstanceId,
    activity_id: ActivityId,
    goal_id: GoalId,
    node_id: DagNodeId,
    budget: WorkCreditBudget,
    award_source_sequence: u64,
    evidence_execution_id: ExecutionId,
    awarded_at_ms: i64,
    created_digest: [u8; 32],
}
impl WorkCreditEntryRow {
    #[must_use]
    pub const fn credit_id(self) -> Uuid {
        self.credit_id
    }
    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        self.workspace_id
    }
    #[must_use]
    pub const fn assignment_id(self) -> AgentAssignmentId {
        self.assignment_id
    }
    #[must_use]
    pub const fn participant_id(self) -> Option<Uuid> {
        self.participant_id
    }
    #[must_use]
    pub const fn agent_id(self) -> AgentInstanceId {
        self.agent_id
    }
    #[must_use]
    pub const fn activity_id(self) -> ActivityId {
        self.activity_id
    }
    #[must_use]
    pub const fn goal_id(self) -> GoalId {
        self.goal_id
    }
    #[must_use]
    pub const fn node_id(self) -> DagNodeId {
        self.node_id
    }
    #[must_use]
    pub const fn budget(self) -> WorkCreditBudget {
        self.budget
    }
    #[must_use]
    pub const fn award_source_sequence(self) -> u64 {
        self.award_source_sequence
    }
    #[must_use]
    pub const fn evidence_execution_id(self) -> ExecutionId {
        self.evidence_execution_id
    }
    #[must_use]
    pub const fn awarded_at_ms(self) -> i64 {
        self.awarded_at_ms
    }
    #[must_use]
    pub const fn created_digest(self) -> [u8; 32] {
        self.created_digest
    }
}

/// Per-participant credit totals inside one score window.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WorkCreditParticipantScore {
    participant_id: Uuid,
    total_credits: i64,
    entry_count: i64,
}
impl WorkCreditParticipantScore {
    #[must_use]
    pub(crate) const fn participant_id(self) -> Uuid {
        self.participant_id
    }
    #[must_use]
    pub(crate) const fn total_credits(self) -> i64 {
        self.total_credits
    }
    #[must_use]
    pub(crate) const fn entry_count(self) -> i64 {
        self.entry_count
    }
}

/// Deterministic credit-score snapshot over one Workspace.
///
/// The canonical arithmetic mean is computed across scoped logical `WorkCredit`
/// subjects with exact integer math and a separate remainder numerator.
/// Historical participant identity is used only as provenance while building
/// the subject projection and is not retained as a parallel score authority.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkCreditScoreSnapshot {
    workspace_id: WorkspaceId,
    team_id: Option<Uuid>,
    window_ms: i64,
    per_subject: Vec<crate::work_credit_subject::WorkCreditSubjectScore>,
    total_credits: i64,
    subject_arithmetic_mean: i64,
    subject_mean_remainder_numerator: i64,
    unattributed_count: i64,
    fulfillment_completed: i64,
    fulfillment_evaluated: i64,
    fulfillment_per_subject: Vec<crate::work_credit_subject::WorkCreditSubjectFulfillment>,
    provenance_submitted: i64,
    provenance_matched: i64,
}
impl WorkCreditScoreSnapshot {
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }
    #[must_use]
    pub const fn team_id(&self) -> Option<Uuid> {
        self.team_id
    }
    #[must_use]
    pub const fn window_ms(&self) -> i64 {
        self.window_ms
    }
    /// Canonical Phase-F score rows keyed by scoped logical `WorkCredit` subject.
    pub fn per_subject(&self) -> &[crate::work_credit_subject::WorkCreditSubjectScore] {
        &self.per_subject
    }
    #[must_use]
    pub const fn total_credits(&self) -> i64 {
        self.total_credits
    }
    /// Canonical Phase-F arithmetic mean across `WorkCredit` subjects.
    #[must_use]
    pub const fn subject_arithmetic_mean(&self) -> i64 {
        self.subject_arithmetic_mean
    }
    #[must_use]
    pub const fn subject_mean_remainder_numerator(&self) -> i64 {
        self.subject_mean_remainder_numerator
    }
    #[must_use]
    pub fn mean_subject_count(&self) -> i64 {
        i64::try_from(self.per_subject.len()).unwrap_or(i64::MAX)
    }
    #[must_use]
    pub const fn subject_total_credits(&self) -> i64 {
        self.total_credits
    }
    #[must_use]
    pub const fn unattributed_count(&self) -> i64 {
        self.unattributed_count
    }
    /// Workspace-wide `completed`: distinct reviewed assignments carrying a
    /// durable work-credit ledger row. Integer only, never a ratio.
    #[must_use]
    pub const fn fulfillment_completed(&self) -> i64 {
        self.fulfillment_completed
    }
    /// Workspace-wide denominator: distinct assignments with a kind-2 terminal
    /// review (`Accepted`/`Rejected`). Running, cancelled and reassigned
    /// assignments are absent from this projection by construction.
    #[must_use]
    pub const fn fulfillment_evaluated(&self) -> i64 {
        self.fulfillment_evaluated
    }
    /// Canonical Phase-F fulfillment counts aggregated by logical subject.
    pub fn fulfillment_per_subject(
        &self,
    ) -> &[crate::work_credit_subject::WorkCreditSubjectFulfillment] {
        &self.fulfillment_per_subject
    }
    /// Workspace-wide kind-4 submissions observed in the durable projection.
    #[must_use]
    pub const fn provenance_submitted(&self) -> i64 {
        self.provenance_submitted
    }
    /// Of those submissions, how many join to exactly-attributed `Applied`
    /// kind-1 artifact evidence. Pre-`v47` unattributed facts never match.
    #[must_use]
    pub const fn provenance_matched(&self) -> i64 {
        self.provenance_matched
    }
}

/// The evidence dimension that matched in one exact provenance join.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProvenanceDimension {
    /// An explicit submitted Goal evidence reference resolved to an Applied
    /// kind-1 `ChangeSet` with the target Agent and session as its producer.
    ExactSubmittedSourceReference,
}

/// The exactly-attributed artifact evidence joined to one kind-4 submission.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkCreditProvenanceMatch {
    change_set_id: ChangeSetId,
    diff_id: DiffId,
    producer_agent_id: AgentInstanceId,
    producer_session_id: Option<AgentSessionId>,
    dimension: ProvenanceDimension,
}
impl WorkCreditProvenanceMatch {
    #[must_use]
    pub const fn change_set_id(self) -> ChangeSetId {
        self.change_set_id
    }
    #[must_use]
    pub const fn diff_id(self) -> DiffId {
        self.diff_id
    }
    #[must_use]
    pub const fn producer_agent_id(self) -> AgentInstanceId {
        self.producer_agent_id
    }
    #[must_use]
    pub const fn producer_session_id(self) -> Option<AgentSessionId> {
        self.producer_session_id
    }
    pub const fn dimension(self) -> ProvenanceDimension {
        self.dimension
    }
}

/// One kind-4 assignment submission with its exact kind-1 artifact provenance.
/// `match` is `None` when no sufficient evidence exists: unattributed
/// (pre-`v47`) kind-1 rows, failed `ChangeSets`, and non-matching scopes all
/// fail closed to `None` instead of an inferred producer.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkCreditSubmitProvenanceRow {
    assignment_id: AgentAssignmentId,
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    goal_id: GoalId,
    node_id: DagNodeId,
    target_agent_id: AgentInstanceId,
    provenance: Option<WorkCreditProvenanceMatch>,
}
impl WorkCreditSubmitProvenanceRow {
    #[must_use]
    pub const fn assignment_id(self) -> AgentAssignmentId {
        self.assignment_id
    }
    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        self.workspace_id
    }
    #[must_use]
    pub const fn activity_id(self) -> ActivityId {
        self.activity_id
    }
    #[must_use]
    pub const fn goal_id(self) -> GoalId {
        self.goal_id
    }
    #[must_use]
    pub const fn node_id(self) -> DagNodeId {
        self.node_id
    }
    #[must_use]
    pub const fn target_agent_id(self) -> AgentInstanceId {
        self.target_agent_id
    }
    #[must_use]
    pub const fn provenance(self) -> Option<WorkCreditProvenanceMatch> {
        self.provenance
    }
}

/// Per-participant fulfillment counts over terminal (kind-2) assignment
/// reviews. Both fields are exact integers; no ratio or float is derived.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WorkCreditParticipantFulfillment {
    pub(crate) participant_id: Uuid,
    completed: i64,
    evaluated: i64,
}

impl WorkCreditParticipantFulfillment {
    #[must_use]
    pub(crate) const fn participant_id(self) -> Uuid {
        self.participant_id
    }
    /// Distinct terminally reviewed assignments with a durable credit row.
    /// Attribution includes a stored participant and all exact live or
    /// retained review-Agent witnesses.
    #[must_use]
    pub(crate) const fn completed(self) -> i64 {
        self.completed
    }
    /// Distinct assignments with a terminal `Accepted` or `Rejected` review.
    /// Attribution matches `completed`; unreviewed credits and repeated
    /// imports cannot inflate either side of the fulfillment ratio.
    #[must_use]
    pub(crate) const fn evaluated(self) -> i64 {
        self.evaluated
    }
}

/// The assignment-review side of one credit derivation, extracted from either
/// the just-stored kind-2 fact or its already-durable projection row.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReviewAwardObservation {
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) assignment_id: AgentAssignmentId,
    pub(crate) activity_id: ActivityId,
    pub(crate) goal_id: GoalId,
    pub(crate) node_id: DagNodeId,
    pub(crate) target_agent_id: AgentInstanceId,
    pub(crate) reviewer_agent_id: AgentInstanceId,
    pub(crate) budget: Option<WorkCreditBudget>,
    pub(crate) award_source_sequence: u64,
    pub(crate) awarded_at_ms: i64,
}

/// Consume one kind-2 assignment-review fact: only an `Accepted` outcome with
/// a non-NULL credit budget may award, and only against an already-stored
/// verification-`Accepted` execution projection row in the same transaction.
pub(crate) fn award_for_review(
    transaction: &Transaction<'_>,
    observation: ReviewAwardObservation,
    outcome: forge_persistence::AssignmentReviewedOutcome,
) -> Result<(), OperationsError> {
    if observation.target_agent_id == observation.reviewer_agent_id {
        return Ok(());
    }
    let eligible_budget = match outcome {
        forge_persistence::AssignmentReviewedOutcome::Accepted => observation.budget,
        forge_persistence::AssignmentReviewedOutcome::Rejected => None,
    };
    let Some(budget) = eligible_budget else {
        return Ok(());
    };
    let Some(evidence) = earliest_verified_execution(
        transaction,
        observation.workspace_id,
        observation.target_agent_id,
        observation.goal_id,
        observation.node_id,
    )?
    else {
        return Ok(());
    };
    let participant = resolve_participant(
        transaction,
        observation.workspace_id,
        observation.target_agent_id,
    )?;
    record_credit(
        transaction,
        credit_row(observation, budget, evidence),
        participant,
    )
}

/// Consume one kind-0 execution terminal fact with a verification-`Accepted`
/// disposition: award only when a durable `Accepted`, credit-eligible review
/// projection row exists for the same Workspace, Agent, Goal and DAG node.
pub(crate) fn award_for_verified_execution(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    agent_id: AgentInstanceId,
    goal_id: GoalId,
    node_id: DagNodeId,
) -> Result<(), OperationsError> {
    let review = transaction
        .query_row(
            "SELECT substr(assignment_id,1,17),substr(activity_id,1,17),substr(target_agent_id,1,17),substr(reviewer_agent_id,1,17),credit_budget,source_sequence,observed_at_ms FROM ops_assignment_review_projection WHERE workspace_id=?1 AND target_agent_id=?2 AND goal_id=?3 AND node_id=?4 AND target_agent_id<>reviewer_agent_id AND outcome='Accepted' AND credit_budget IS NOT NULL ORDER BY source_sequence ASC LIMIT 1",
            params![
                workspace_id.into_uuid().as_bytes(),
                agent_id.into_uuid().as_bytes(),
                goal_id.into_uuid().as_bytes(),
                node_id.into_uuid().as_bytes(),
            ],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            },
        )
        .optional()?;
    let Some((
        assignment,
        activity,
        target_agent,
        reviewer_agent,
        budget,
        sequence,
        observed_at_ms,
    )) = review
    else {
        return Ok(());
    };
    let Some(evidence) =
        earliest_verified_execution(transaction, workspace_id, agent_id, goal_id, node_id)?
    else {
        return Ok(());
    };
    let budget = crate::effect_fact::decode_credit_budget(budget)?;
    let observation = ReviewAwardObservation {
        workspace_id,
        assignment_id: AgentAssignmentId::from_uuid(Uuid::from_bytes(wire::id(assignment)?)),
        activity_id: ActivityId::from_uuid(Uuid::from_bytes(wire::id(activity)?)),
        goal_id,
        node_id,
        target_agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(target_agent)?)),
        reviewer_agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(reviewer_agent)?)),
        budget: Some(budget),
        award_source_sequence: u64::try_from(sequence)
            .map_err(|_| OperationsError::Corrupt("work credit source sequence"))?,
        awarded_at_ms: observed_at_ms,
    };
    let participant = resolve_participant(transaction, workspace_id, agent_id)?;
    record_credit(
        transaction,
        credit_row(observation, budget, evidence),
        participant,
    )
}

/// Deterministic evidence choice: the earliest stored verification-`Accepted`
/// execution projection row for the exact Workspace/Agent/Goal/node scope.
/// Every trigger path derives the identical evidence, so both consumption
/// orders of a matching pair converge on one ledger row.
fn earliest_verified_execution(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    agent_id: AgentInstanceId,
    goal_id: GoalId,
    node_id: DagNodeId,
) -> Result<Option<ExecutionId>, OperationsError> {
    let raw = transaction
        .query_row(
            "SELECT substr(execution_id,1,17) FROM ops_execution_terminal_projection WHERE workspace_id=?1 AND agent_id=?2 AND goal_id=?3 AND node_id=?4 AND verification_disposition=0 ORDER BY source_sequence ASC LIMIT 1",
            params![
                workspace_id.into_uuid().as_bytes(),
                agent_id.into_uuid().as_bytes(),
                goal_id.into_uuid().as_bytes(),
                node_id.into_uuid().as_bytes(),
            ],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?;
    raw.map(|value| {
        Ok::<_, OperationsError>(ExecutionId::from_uuid(Uuid::from_bytes(wire::id(value)?)))
    })
    .transpose()
}

fn resolve_participant(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    agent_id: AgentInstanceId,
) -> Result<Option<Uuid>, OperationsError> {
    Ok(
        effect_binding::by_agent(transaction, workspace_id, agent_id)?
            .map(effect_binding::CurrentEffectBinding::participant_id),
    )
}

#[derive(Debug, Clone, Copy)]
struct CreditLedgerRow {
    credit_id: Uuid,
    workspace_id: WorkspaceId,
    assignment_id: AgentAssignmentId,
    agent_id: AgentInstanceId,
    activity_id: ActivityId,
    goal_id: GoalId,
    node_id: DagNodeId,
    budget: WorkCreditBudget,
    award_source_sequence: u64,
    evidence_execution_id: ExecutionId,
    awarded_at_ms: i64,
    created_digest: [u8; 32],
}

fn credit_row(
    observation: ReviewAwardObservation,
    budget: WorkCreditBudget,
    evidence_execution_id: ExecutionId,
) -> CreditLedgerRow {
    let award_source_sequence = observation.award_source_sequence;
    let awarded_at_ms = observation.awarded_at_ms;
    let created_digest = created_digest(&observation, budget, evidence_execution_id);
    CreditLedgerRow {
        credit_id: credit_id(observation.workspace_id, observation.assignment_id),
        workspace_id: observation.workspace_id,
        assignment_id: observation.assignment_id,
        agent_id: observation.target_agent_id,
        activity_id: observation.activity_id,
        goal_id: observation.goal_id,
        node_id: observation.node_id,
        budget,
        award_source_sequence,
        evidence_execution_id,
        awarded_at_ms,
        created_digest,
    }
}

/// Insert the derived credit, or accept an exact replay of an identical row
/// as a no-op. A differing row for the same Workspace assignment conflicts.
fn record_credit(
    transaction: &Transaction<'_>,
    credit: CreditLedgerRow,
    participant_id: Option<Uuid>,
) -> Result<(), OperationsError> {
    let stored: Option<Vec<u8>> = transaction
        .query_row(
            "SELECT substr(created_digest,1,33) FROM ops_work_credit_entries WHERE workspace_id=?1 AND assignment_id=?2",
            params![
                credit.workspace_id.into_uuid().as_bytes(),
                credit.assignment_id.into_uuid().as_bytes(),
            ],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(stored) = stored {
        if stored.as_slice() == credit.created_digest.as_slice() {
            return Ok(());
        }
        return Err(OperationsError::Conflict("work credit identity"));
    }
    transaction.execute(
        "INSERT INTO ops_work_credit_entries(credit_id,workspace_id,assignment_id,participant_id,agent_id,activity_id,goal_id,node_id,budget,award_source_sequence,evidence_execution_id,awarded_at_ms,created_digest) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
        params![
            credit.credit_id.as_bytes(),
            credit.workspace_id.into_uuid().as_bytes(),
            credit.assignment_id.into_uuid().as_bytes(),
            participant_id.map(|value| value.as_bytes().to_vec()),
            credit.agent_id.into_uuid().as_bytes(),
            credit.activity_id.into_uuid().as_bytes(),
            credit.goal_id.into_uuid().as_bytes(),
            credit.node_id.into_uuid().as_bytes(),
            credit.budget.value(),
            crate::catalog::signed(credit.award_source_sequence)?,
            credit.evidence_execution_id.into_uuid().as_bytes(),
            credit.awarded_at_ms,
            credit.created_digest.as_slice(),
        ],
    )?;
    Ok(())
}

/// Deterministic credit identity following the `joined_event_id` SHA-256
/// derivation pattern: domain-separated digest truncated to 16 bytes.
fn credit_id(workspace_id: WorkspaceId, assignment_id: AgentAssignmentId) -> Uuid {
    let mut digest = Sha256::new();
    digest.update(CREDIT_ID_DOMAIN);
    digest.update((16_u32).to_be_bytes());
    digest.update(workspace_id.into_uuid().as_bytes());
    digest.update((16_u32).to_be_bytes());
    digest.update(assignment_id.into_uuid().as_bytes());
    let digest = digest.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

fn created_digest(
    observation: &ReviewAwardObservation,
    budget: WorkCreditBudget,
    evidence_execution_id: ExecutionId,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(CREATED_DIGEST_DOMAIN);
    digest.update(observation.workspace_id.into_uuid().as_bytes());
    digest.update(observation.assignment_id.into_uuid().as_bytes());
    digest.update(observation.target_agent_id.into_uuid().as_bytes());
    digest.update(observation.activity_id.into_uuid().as_bytes());
    digest.update(observation.goal_id.into_uuid().as_bytes());
    digest.update(observation.node_id.into_uuid().as_bytes());
    digest.update(budget.value().to_be_bytes());
    digest.update(observation.award_source_sequence.to_be_bytes());
    digest.update(evidence_execution_id.into_uuid().as_bytes());
    digest.update(observation.awarded_at_ms.to_be_bytes());
    digest.finalize().into()
}

fn decode_budget(value: i64) -> Result<WorkCreditBudget, OperationsError> {
    match value {
        1 => Ok(WorkCreditBudget::One),
        3 => Ok(WorkCreditBudget::Three),
        5 => Ok(WorkCreditBudget::Five),
        _ => Err(OperationsError::Corrupt("work credit budget")),
    }
}

#[derive(Debug)]
struct RawLedgerRow {
    credit_id: Vec<u8>,
    workspace: Vec<u8>,
    assignment: Vec<u8>,
    participant: Option<Vec<u8>>,
    agent: Vec<u8>,
    activity: Vec<u8>,
    goal: Vec<u8>,
    node: Vec<u8>,
    budget: i64,
    sequence: i64,
    evidence: Vec<u8>,
    awarded_at_ms: i64,
    digest: Vec<u8>,
}

fn decode_ledger_row(raw: RawLedgerRow) -> Result<WorkCreditEntryRow, OperationsError> {
    Ok(WorkCreditEntryRow {
        credit_id: Uuid::from_bytes(wire::id(raw.credit_id)?),
        workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(raw.workspace)?)),
        assignment_id: AgentAssignmentId::from_uuid(Uuid::from_bytes(wire::id(raw.assignment)?)),
        participant_id: raw
            .participant
            .map(|value| Ok::<_, OperationsError>(Uuid::from_bytes(wire::id(value)?)))
            .transpose()?,
        agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(raw.agent)?)),
        activity_id: ActivityId::from_uuid(Uuid::from_bytes(wire::id(raw.activity)?)),
        goal_id: GoalId::from_uuid(Uuid::from_bytes(wire::id(raw.goal)?)),
        node_id: DagNodeId::from_uuid(Uuid::from_bytes(wire::id(raw.node)?)),
        budget: decode_budget(raw.budget)?,
        award_source_sequence: u64::try_from(raw.sequence)
            .map_err(|_| OperationsError::Corrupt("work credit source sequence"))?,
        evidence_execution_id: ExecutionId::from_uuid(Uuid::from_bytes(wire::id(raw.evidence)?)),
        awarded_at_ms: raw.awarded_at_ms,
        created_digest: raw
            .digest
            .try_into()
            .map_err(|_| OperationsError::Corrupt("work credit digest"))?,
    })
}

#[derive(Debug)]
struct RawSubmitProvenanceRow {
    assignment: Vec<u8>,
    workspace: Vec<u8>,
    activity: Vec<u8>,
    goal: Vec<u8>,
    node: Vec<u8>,
    target_agent: Vec<u8>,
    change_set: Option<Vec<u8>>,
    diff: Option<Vec<u8>>,
    producer_agent: Option<Vec<u8>>,
    producer_session: Option<Vec<u8>>,
}

fn decode_submit_provenance_row(
    raw: RawSubmitProvenanceRow,
) -> Result<WorkCreditSubmitProvenanceRow, OperationsError> {
    // All-or-nothing evidence: a partial match row is corruption, not
    // partial attribution.
    let provenance = match (
        raw.change_set,
        raw.diff,
        raw.producer_agent,
        raw.producer_session,
    ) {
        (None, None, None, None) => None,
        (Some(change_set), Some(diff), Some(producer_agent), producer_session) => {
            Some(WorkCreditProvenanceMatch {
                change_set_id: ChangeSetId::from_uuid(Uuid::from_bytes(wire::id(change_set)?)),
                diff_id: DiffId::from_uuid(Uuid::from_bytes(wire::id(diff)?)),
                producer_agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(
                    producer_agent,
                )?)),
                producer_session_id: producer_session
                    .map(wire::id)
                    .transpose()?
                    .map(|id| AgentSessionId::from_uuid(Uuid::from_bytes(id))),
                dimension: ProvenanceDimension::ExactSubmittedSourceReference,
            })
        }
        _ => return Err(OperationsError::Corrupt("work credit provenance match")),
    };
    Ok(WorkCreditSubmitProvenanceRow {
        assignment_id: AgentAssignmentId::from_uuid(Uuid::from_bytes(wire::id(raw.assignment)?)),
        workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(raw.workspace)?)),
        activity_id: ActivityId::from_uuid(Uuid::from_bytes(wire::id(raw.activity)?)),
        goal_id: GoalId::from_uuid(Uuid::from_bytes(wire::id(raw.goal)?)),
        node_id: DagNodeId::from_uuid(Uuid::from_bytes(wire::id(raw.node)?)),
        target_agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(raw.target_agent)?)),
        provenance,
    })
}

impl OperationsCatalog {
    /// Read at most 64 durable work-credit entries for one Workspace, newest
    /// first. This never mutates state.
    ///
    /// # Errors
    /// Rejects malformed persisted ledger rows and database read failures.
    pub fn work_credit_entries(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Vec<WorkCreditEntryRow>, OperationsError> {
        let limit = i64::try_from(LEDGER_READ_LIMIT)
            .map_err(|_| OperationsError::Bounds("work credit read limit"))?;
        let mut statement = self.connection.prepare(
            "SELECT substr(credit_id,1,17),substr(workspace_id,1,17),substr(assignment_id,1,17),substr(participant_id,1,17),substr(agent_id,1,17),substr(activity_id,1,17),substr(goal_id,1,17),substr(node_id,1,17),budget,award_source_sequence,substr(evidence_execution_id,1,17),awarded_at_ms,substr(created_digest,1,33) FROM ops_work_credit_entries WHERE workspace_id=?1 ORDER BY awarded_at_ms DESC, award_source_sequence DESC, credit_id DESC LIMIT ?2",
        )?;
        let raw = statement
            .query_map(params![workspace_id.into_uuid().as_bytes(), limit], |row| {
                Ok(RawLedgerRow {
                    credit_id: row.get(0)?,
                    workspace: row.get(1)?,
                    assignment: row.get(2)?,
                    participant: row.get(3)?,
                    agent: row.get(4)?,
                    activity: row.get(5)?,
                    goal: row.get(6)?,
                    node: row.get(7)?,
                    budget: row.get(8)?,
                    sequence: row.get(9)?,
                    evidence: row.get(10)?,
                    awarded_at_ms: row.get(11)?,
                    digest: row.get(12)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        raw.into_iter().map(decode_ledger_row).collect()
    }

    /// Read `(completed, evaluated)` for reviewed assignments in one Workspace.
    /// `completed` counts distinct reviewed assignments with a matching durable
    /// work-credit ledger row; `evaluated` counts distinct Accepted/Rejected
    /// review assignments. Non-review assignment states are absent from this
    /// projection and therefore do not contribute. A Workspace with no reviews
    /// returns `(0, 0)`.
    ///
    /// # Errors
    /// Returns database failures, catalog read-commit failures, or corrupt
    /// negative aggregate counts.
    pub fn work_credit_fulfillment(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<(u64, u64), OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let (completed, evaluated) = work_credit_fulfillment_at(&transaction, workspace_id)?;
        let completed = u64::try_from(completed)
            .map_err(|_| OperationsError::Corrupt("work credit completed count"))?;
        let evaluated = u64::try_from(evaluated)
            .map_err(|_| OperationsError::Corrupt("work credit evaluated count"))?;
        commit_read(transaction, self.destination)?;
        Ok((completed, evaluated))
    }

    /// Read the exact provenance join between kind-4 assignment submissions
    /// and kind-1 `ChangeSet` terminal facts for one Workspace, oldest
    /// submission first, bounded at 64 rows.
    ///
    /// Join contract: the first submitted source ordinal that explicitly names
    /// a kind-1 `ChangeSet` is resolved by exact `ChangeSet` ID within the same
    /// migration, Workspace, and Activity. Only `Applied` rows whose producer
    /// Agent and session equal the submission target are evidence. A missing
    /// target session never claims producer causality. Rows with no sufficient
    /// evidence — legacy/unavailable refs, failed change sets, foreign
    /// producers, and foreign scopes — carry `provenance == None`.
    ///
    /// # Errors
    /// Rejects malformed persisted projections and database read failures.
    pub fn work_credit_submit_provenance(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Vec<WorkCreditSubmitProvenanceRow>, OperationsError> {
        let limit = i64::try_from(PROVENANCE_READ_LIMIT)
            .map_err(|_| OperationsError::Bounds("work credit provenance read limit"))?;
        let mut statement = self.connection.prepare(
            "SELECT substr(s.assignment_id,1,17),substr(s.workspace_id,1,17),substr(s.activity_id,1,17),substr(s.goal_id,1,17),substr(s.node_id,1,17),substr(s.target_agent_id,1,17),substr(c.change_set_id,1,17),substr(c.diff_id,1,17),substr(c.producer_agent_id,1,17),substr(c.producer_session_id,1,17) FROM ops_assignment_submit_projection AS s LEFT JOIN ops_change_set_projection AS c ON c.migration_id=s.migration_id AND c.workspace_id=s.workspace_id AND c.activity_id=s.activity_id AND c.change_set_id=(SELECT c2.change_set_id FROM ops_assignment_submit_source_refs AS r2 JOIN ops_change_set_projection AS c2 ON c2.migration_id=r2.migration_id AND c2.workspace_id=s.workspace_id AND c2.activity_id=s.activity_id AND c2.change_set_id=r2.evidence_id WHERE s.source_refs_recorded=1 AND r2.migration_id=s.migration_id AND r2.source_sequence=s.source_sequence AND r2.evidence_kind=0 AND c2.state='Applied' AND s.target_session_id IS NOT NULL AND c2.producer_agent_id=s.target_agent_id AND c2.producer_session_id=s.target_session_id ORDER BY r2.ordinal ASC LIMIT 1) WHERE s.workspace_id=?1 ORDER BY s.source_sequence ASC LIMIT ?2",
        )?;
        let raw = statement
            .query_map(params![workspace_id.into_uuid().as_bytes(), limit], |row| {
                Ok(RawSubmitProvenanceRow {
                    assignment: row.get(0)?,
                    workspace: row.get(1)?,
                    activity: row.get(2)?,
                    goal: row.get(3)?,
                    node: row.get(4)?,
                    target_agent: row.get(5)?,
                    change_set: row.get(6)?,
                    diff: row.get(7)?,
                    producer_agent: row.get(8)?,
                    producer_session: row.get(9)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        raw.into_iter().map(decode_submit_provenance_row).collect()
    }

    /// Compute the deterministic per-participant credit score snapshot over
    /// the fixed 30-day window ending at `now_ms`. The caller owns the clock;
    /// this read never mutates state.
    ///
    /// # Errors
    /// Rejects malformed persisted ledger rows, bindings, and database read
    /// failures.
    pub fn work_credit_scores(
        &self,
        workspace_id: WorkspaceId,
        now_ms: i64,
    ) -> Result<WorkCreditScoreSnapshot, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let result = work_credit_scores_at(&transaction, workspace_id, now_ms);
        match result {
            Ok(value) => {
                commit_read(transaction, self.destination)?;
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }

    /// Read a full score snapshot after checking its score and fulfillment
    /// source groups against the fixed Workspace roster read budget.
    ///
    /// The preflight counts and projection share one read transaction. An
    /// oversized observation returns `ResyncRequired`; it is never clipped.
    /// Pagination/recovery for larger snapshots remains a separate contract.
    ///
    /// # Errors
    /// Returns invalid clocks, catalog failures, malformed rows, or a typed
    /// resynchronization request when either source projection exceeds budget.
    pub fn work_credit_scores_bounded(
        &self,
        workspace_id: WorkspaceId,
        now_ms: i64,
    ) -> Result<WorkCreditScoreSnapshot, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let result = work_credit_scores_bounded_at(&transaction, workspace_id, now_ms);
        match result {
            Ok(value) => {
                commit_read(transaction, self.destination)?;
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }

    /// Compute a score snapshot scoped to the immutable Team affinity journal.
    /// Current Team membership is deliberately not consulted: only a stored
    /// affinity joined to the actual assignment/target/activity evidence can
    /// contribute.
    ///
    /// # Errors
    /// Rejects missing or foreign Teams, invalid clocks and unavailable credit evidence.
    pub fn team_work_credit_scores(
        &self,
        workspace_id: WorkspaceId,
        team_id: Uuid,
        now_ms: i64,
    ) -> Result<WorkCreditScoreSnapshot, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let result = team_work_credit_scores_at(&transaction, workspace_id, team_id, now_ms);
        match result {
            Ok(value) => {
                commit_read(transaction, self.destination)?;
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }
}

pub(crate) fn work_credit_scores_at(
    connection: &Connection,
    workspace_id: WorkspaceId,
    now_ms: i64,
) -> Result<WorkCreditScoreSnapshot, OperationsError> {
    let window_floor = now_ms.saturating_sub(WORK_CREDIT_WINDOW_MS);
    let grouped_rows_sql =
        format!("{WORK_CREDIT_SCORE_GROUPS_SQL} ORDER BY pid IS NULL ASC, pid ASC");
    let mut statement = connection.prepare(&grouped_rows_sql)?;
    let raw = statement
        .query_map(
            params![workspace_id.into_uuid().as_bytes(), window_floor,],
            |row| {
                Ok((
                    row.get::<_, Option<Vec<u8>>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;
    let mut per_participant = Vec::new();
    let mut total_credits = 0_i64;
    let mut unattributed_count = 0_i64;
    for (participant, credits, entries) in raw {
        match participant {
            Some(participant) => {
                let score = WorkCreditParticipantScore {
                    participant_id: Uuid::from_bytes(wire::id(participant)?),
                    total_credits: credits,
                    entry_count: entries,
                };
                total_credits = total_credits.saturating_add(score.total_credits());
                per_participant.push(score);
            }
            None => unattributed_count = unattributed_count.saturating_add(entries),
        }
    }
    let per_subject =
        crate::work_credit_subject::aggregate_scores(connection, workspace_id, &per_participant)?;
    let mean_subject_count = i64::try_from(per_subject.len())
        .map_err(|_| OperationsError::Bounds("WorkCredit subjects"))?;
    let (subject_arithmetic_mean, subject_mean_remainder_numerator) = if mean_subject_count == 0 {
        (0, 0)
    } else {
        (
            total_credits / mean_subject_count,
            total_credits % mean_subject_count,
        )
    };
    let (fulfillment_completed, fulfillment_evaluated) =
        work_credit_fulfillment_at(connection, workspace_id)?;
    let fulfillment_per_participant = fulfillment_per_participant_at(connection, workspace_id)?;
    let fulfillment_per_subject = crate::work_credit_subject::aggregate_fulfillment(
        connection,
        workspace_id,
        &fulfillment_per_participant,
    )?;
    let (provenance_submitted, provenance_matched) =
        provenance_totals_at(connection, workspace_id)?;
    Ok(WorkCreditScoreSnapshot {
        workspace_id,
        team_id: None,
        window_ms: WORK_CREDIT_WINDOW_MS,
        per_subject,
        total_credits,
        subject_arithmetic_mean,
        subject_mean_remainder_numerator,
        unattributed_count,
        fulfillment_completed,
        fulfillment_evaluated,
        fulfillment_per_subject,
        provenance_submitted,
        provenance_matched,
    })
}

fn work_credit_scores_bounded_at(
    connection: &Connection,
    workspace_id: WorkspaceId,
    now_ms: i64,
) -> Result<WorkCreditScoreSnapshot, OperationsError> {
    ensure_bounded_score_groups(connection, workspace_id, now_ms)?;
    ensure_bounded_fulfillment_sources(connection, workspace_id)?;
    work_credit_scores_at(connection, workspace_id, now_ms)
}

/// The exact source groups materialized by `work_credit_scores_at`. The NULL
/// attribution group is counted too because it occupies a raw result row.
const WORK_CREDIT_SCORE_GROUPS_SQL: &str = "SELECT CASE WHEN e.participant_id IS NOT NULL THEN e.participant_id ELSE b.participant_id END AS pid, SUM(e.budget), COUNT(*) FROM ops_work_credit_entries e LEFT JOIN ops_current_effect_bindings b ON b.workspace_id=e.workspace_id AND b.agent_id=e.agent_id WHERE e.workspace_id=?1 AND e.awarded_at_ms>=?2 AND (e.assignment_id,e.award_source_sequence) NOT IN (SELECT assignment_id,source_sequence FROM ops_assignment_review_projection WHERE workspace_id=?1 AND target_agent_id=reviewer_agent_id) GROUP BY pid";

const WORK_CREDIT_READ_BUDGET_REJECTION: &str = "WorkCredit observation exceeds read budget";

fn ensure_bounded_score_groups(
    connection: &Connection,
    workspace_id: WorkspaceId,
    now_ms: i64,
) -> Result<(), OperationsError> {
    let count_sql = format!("SELECT COUNT(*) FROM ({WORK_CREDIT_SCORE_GROUPS_SQL})");
    let group_count: i64 = connection.query_row(
        &count_sql,
        params![
            workspace_id.into_uuid().as_bytes(),
            now_ms.saturating_sub(WORK_CREDIT_WINDOW_MS),
        ],
        |row| row.get(0),
    )?;
    require_work_credit_source_budget(group_count)
}

fn ensure_bounded_fulfillment_sources(
    connection: &Connection,
    workspace_id: WorkspaceId,
) -> Result<(), OperationsError> {
    let count_sql =
        format!("SELECT COUNT(*) FROM ({FULFILLMENT_ATTRIBUTION_SQL}) WHERE has_conflict=0");
    let source_count: i64 = connection.query_row(
        &count_sql,
        params![workspace_id.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    require_work_credit_source_budget(source_count)
}

fn require_work_credit_source_budget(count: i64) -> Result<(), OperationsError> {
    let count = usize::try_from(count)
        .map_err(|_| OperationsError::Corrupt("WorkCredit source group count"))?;
    if count > WORK_CREDIT_SCORE_SOURCE_READ_LIMIT {
        return Err(OperationsError::ResyncRequired(
            WORK_CREDIT_READ_BUDGET_REJECTION,
        ));
    }
    Ok(())
}

/// Compute Team-scoped `WorkCredit` scores from immutable assignment affinities.
pub(crate) fn team_work_credit_scores_at(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    now_ms: i64,
) -> Result<WorkCreditScoreSnapshot, OperationsError> {
    if team_id.is_nil() {
        return Err(OperationsError::Conflict("WorkCredit Team identity"));
    }
    let known_team: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_teams WHERE workspace_id=?1 AND team_id=?2)",
        params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()],
        |row| row.get(0),
    )?;
    if !known_team {
        return Err(OperationsError::Conflict("WorkCredit Team workspace scope"));
    }
    let window_floor = now_ms.saturating_sub(WORK_CREDIT_WINDOW_MS);
    let per_participant =
        team_work_credit_participants_at(connection, workspace_id, team_id, window_floor)?;
    let total_credits = per_participant.iter().fold(0_i64, |total, score| {
        total.saturating_add(score.total_credits())
    });
    let per_subject =
        crate::work_credit_subject::aggregate_scores(connection, workspace_id, &per_participant)?;
    let mean_subject_count = i64::try_from(per_subject.len())
        .map_err(|_| OperationsError::Bounds("WorkCredit Team subjects"))?;
    let (subject_arithmetic_mean, subject_mean_remainder_numerator) = if mean_subject_count == 0 {
        (0, 0)
    } else {
        (
            total_credits / mean_subject_count,
            total_credits % mean_subject_count,
        )
    };
    let (fulfillment_completed, fulfillment_evaluated) =
        team_work_credit_fulfillment_at(connection, workspace_id, team_id)?;
    let fulfillment_per_participant =
        team_fulfillment_per_participant_at(connection, workspace_id, team_id)?;
    let fulfillment_per_subject = crate::work_credit_subject::aggregate_fulfillment(
        connection,
        workspace_id,
        &fulfillment_per_participant,
    )?;
    let (provenance_submitted, provenance_matched) =
        team_provenance_totals_at(connection, workspace_id, team_id)?;
    Ok(WorkCreditScoreSnapshot {
        workspace_id,
        team_id: Some(team_id),
        window_ms: WORK_CREDIT_WINDOW_MS,
        per_subject,
        total_credits,
        subject_arithmetic_mean,
        subject_mean_remainder_numerator,
        unattributed_count: 0,
        fulfillment_completed,
        fulfillment_evaluated,
        fulfillment_per_subject,
        provenance_submitted,
        provenance_matched,
    })
}

fn team_work_credit_participants_at(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    window_floor: i64,
) -> Result<Vec<WorkCreditParticipantScore>, OperationsError> {
    let mut statement = connection.prepare(
		"SELECT a.target_participant_id,SUM(e.budget),COUNT(*) FROM ops_assignment_team_affinities a JOIN ops_work_credit_entries e ON e.workspace_id=a.workspace_id AND e.assignment_id=a.assignment_id AND e.activity_id=a.activity_id AND e.agent_id=a.target_agent_id WHERE a.workspace_id=?1 AND a.team_id=?2 AND e.awarded_at_ms>=?3 AND (e.assignment_id,e.award_source_sequence) NOT IN (SELECT assignment_id,source_sequence FROM ops_assignment_review_projection WHERE workspace_id=?1 AND target_agent_id=reviewer_agent_id) AND EXISTS (SELECT 1 FROM ops_assignment_offer_projection o WHERE o.workspace_id=a.workspace_id AND o.assignment_id=a.assignment_id AND o.activity_id=a.activity_id AND o.target_agent_id=a.target_agent_id) GROUP BY a.target_participant_id ORDER BY a.target_participant_id ASC",
	)?;
    let rows = statement
        .query_map(
            params![
                workspace_id.into_uuid().as_bytes(),
                team_id.as_bytes(),
                window_floor
            ],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;
    rows.into_iter()
        .map(|(participant, total_credits, entry_count)| {
            Ok(WorkCreditParticipantScore {
                participant_id: Uuid::from_bytes(wire::id(participant)?),
                total_credits,
                entry_count,
            })
        })
        .collect()
}

fn team_work_credit_fulfillment_at(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<(i64, i64), OperationsError> {
    connection
		.query_row(
        "SELECT COUNT(DISTINCT CASE WHEN EXISTS (SELECT 1 FROM ops_work_credit_entries e WHERE e.workspace_id=a.workspace_id AND e.assignment_id=a.assignment_id AND e.activity_id=a.activity_id AND e.agent_id=a.target_agent_id AND (e.assignment_id,e.award_source_sequence) NOT IN (SELECT assignment_id,source_sequence FROM ops_assignment_review_projection WHERE workspace_id=?1 AND target_agent_id=reviewer_agent_id)) THEN a.assignment_id END), COUNT(DISTINCT r.assignment_id) FROM ops_assignment_team_affinities a LEFT JOIN ops_assignment_review_projection r ON r.workspace_id=a.workspace_id AND r.assignment_id=a.assignment_id AND r.activity_id=a.activity_id AND r.target_agent_id=a.target_agent_id AND r.outcome IN ('Accepted','Rejected') AND r.target_agent_id<>r.reviewer_agent_id WHERE a.workspace_id=?1 AND a.team_id=?2 AND EXISTS (SELECT 1 FROM ops_assignment_offer_projection o WHERE o.workspace_id=a.workspace_id AND o.assignment_id=a.assignment_id AND o.activity_id=a.activity_id AND o.target_agent_id=a.target_agent_id)",
			params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()],
			|row| Ok((row.get(0)?, row.get(1)?)),
		)
		.map_err(OperationsError::from)
}

fn team_fulfillment_per_participant_at(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<Vec<WorkCreditParticipantFulfillment>, OperationsError> {
    let mut statement = connection.prepare(
		"SELECT pid,SUM(completed),SUM(evaluated) FROM (SELECT a.target_participant_id AS pid,1 AS completed,0 AS evaluated FROM ops_assignment_team_affinities a JOIN ops_work_credit_entries e ON e.workspace_id=a.workspace_id AND e.assignment_id=a.assignment_id AND e.activity_id=a.activity_id AND e.agent_id=a.target_agent_id WHERE a.workspace_id=?1 AND a.team_id=?2 AND (e.assignment_id,e.award_source_sequence) NOT IN (SELECT assignment_id,source_sequence FROM ops_assignment_review_projection WHERE workspace_id=?1 AND target_agent_id=reviewer_agent_id) AND EXISTS (SELECT 1 FROM ops_assignment_offer_projection o WHERE o.workspace_id=a.workspace_id AND o.assignment_id=a.assignment_id AND o.activity_id=a.activity_id AND o.target_agent_id=a.target_agent_id) UNION ALL SELECT a.target_participant_id AS pid,0 AS completed,1 AS evaluated FROM ops_assignment_team_affinities a JOIN ops_assignment_review_projection r ON r.workspace_id=a.workspace_id AND r.assignment_id=a.assignment_id AND r.activity_id=a.activity_id AND r.target_agent_id=a.target_agent_id WHERE a.workspace_id=?1 AND a.team_id=?2 AND r.outcome IN ('Accepted','Rejected') AND r.target_agent_id<>r.reviewer_agent_id AND EXISTS (SELECT 1 FROM ops_assignment_offer_projection o WHERE o.workspace_id=a.workspace_id AND o.assignment_id=a.assignment_id AND o.activity_id=a.activity_id AND o.target_agent_id=a.target_agent_id)) GROUP BY pid ORDER BY pid ASC",
	)?;
    let rows = statement
        .query_map(
            params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;
    rows.into_iter()
        .map(|(participant, completed, evaluated)| {
            Ok(WorkCreditParticipantFulfillment {
                participant_id: Uuid::from_bytes(wire::id(participant)?),
                completed,
                evaluated,
            })
        })
        .collect()
}

fn team_provenance_totals_at(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<(i64, i64), OperationsError> {
    connection
		.query_row(
            "SELECT COUNT(DISTINCT s.assignment_id), COUNT(DISTINCT CASE WHEN EXISTS (SELECT 1 FROM ops_assignment_submit_source_refs AS r JOIN ops_change_set_projection AS c ON c.migration_id=r.migration_id AND c.workspace_id=s.workspace_id AND c.activity_id=s.activity_id AND c.change_set_id=r.evidence_id WHERE s.source_refs_recorded=1 AND r.migration_id=s.migration_id AND r.source_sequence=s.source_sequence AND r.evidence_kind=0 AND c.state='Applied' AND s.target_session_id IS NOT NULL AND c.producer_agent_id=s.target_agent_id AND c.producer_session_id=s.target_session_id) THEN s.assignment_id END) FROM ops_assignment_submit_projection s WHERE s.workspace_id=?1 AND EXISTS (SELECT 1 FROM ops_assignment_team_affinities a WHERE a.workspace_id=s.workspace_id AND a.assignment_id=s.assignment_id AND a.activity_id=s.activity_id AND a.target_agent_id=s.target_agent_id AND a.team_id=?2) AND EXISTS (SELECT 1 FROM ops_assignment_offer_projection o WHERE o.workspace_id=s.workspace_id AND o.assignment_id=s.assignment_id AND o.activity_id=s.activity_id AND o.target_agent_id=s.target_agent_id)",
			params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()],
			|row| Ok((row.get(0)?, row.get(1)?)),
		)
		.map_err(OperationsError::from)
}

/// Exact workspace-wide provenance join totals: distinct kind-4 submissions
/// and how many of them carry exactly-attributed `Applied` kind-1 evidence.
fn provenance_totals_at(
    connection: &Connection,
    workspace_id: WorkspaceId,
) -> Result<(i64, i64), OperationsError> {
    let (submitted, matched): (i64, i64) = connection.query_row(
        "SELECT COUNT(DISTINCT s.assignment_id), COUNT(DISTINCT CASE WHEN EXISTS (SELECT 1 FROM ops_assignment_submit_source_refs AS r JOIN ops_change_set_projection AS c ON c.migration_id=r.migration_id AND c.workspace_id=s.workspace_id AND c.activity_id=s.activity_id AND c.change_set_id=r.evidence_id WHERE s.source_refs_recorded=1 AND r.migration_id=s.migration_id AND r.source_sequence=s.source_sequence AND r.evidence_kind=0 AND c.state='Applied' AND s.target_session_id IS NOT NULL AND c.producer_agent_id=s.target_agent_id AND c.producer_session_id=s.target_session_id) THEN s.assignment_id END) FROM ops_assignment_submit_projection AS s WHERE s.workspace_id=?1",
        params![workspace_id.into_uuid().as_bytes()],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok((submitted, matched))
}

/// Workspace-wide fulfillment counts: `completed` counts distinct reviewed
/// assignments with a matching durable work-credit ledger row; `evaluated`
/// counts distinct kind-2 terminal (`Accepted`/`Rejected`) review assignments.
/// Running, cancelled and reassigned assignments never reach kind-2 and are
/// excluded by construction. A Workspace with no reviews returns `(0, 0)`.
fn work_credit_fulfillment_at(
    connection: &Connection,
    workspace_id: WorkspaceId,
) -> Result<(i64, i64), OperationsError> {
    let (completed, evaluated): (i64, i64) = connection.query_row(
        "SELECT COUNT(DISTINCT CASE WHEN ledger.assignment_id IS NOT NULL AND (ledger.assignment_id,ledger.award_source_sequence) NOT IN (SELECT assignment_id,source_sequence FROM ops_assignment_review_projection WHERE workspace_id=?1 AND target_agent_id=reviewer_agent_id) THEN review.assignment_id END), COUNT(DISTINCT review.assignment_id) FROM ops_assignment_review_projection AS review LEFT JOIN ops_work_credit_entries AS ledger ON ledger.workspace_id=review.workspace_id AND ledger.assignment_id=review.assignment_id WHERE review.workspace_id=?1 AND review.outcome IN ('Accepted','Rejected') AND review.target_agent_id<>review.reviewer_agent_id",
        params![workspace_id.into_uuid().as_bytes()],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok((completed, evaluated))
}

/// Per-participant fulfillment over distinct terminally reviewed assignments.
/// Attribution combines the credit ledger, the live binding, and retained
/// binding journals. Unresolved participants remain in workspace totals only;
/// conflicting durable identities fail closed.
pub(crate) fn participant_fulfillment_rows(
    connection: &Connection,
    workspace_id: WorkspaceId,
) -> Result<Vec<WorkCreditParticipantFulfillment>, OperationsError> {
    fulfillment_per_participant_at(connection, workspace_id)
}

const FULFILLMENT_ATTRIBUTION_SQL: &str = "WITH
         self_reviewed_awards AS (
           SELECT workspace_id,assignment_id,source_sequence
           FROM ops_assignment_review_projection
           WHERE workspace_id=?1 AND target_agent_id=reviewer_agent_id
         ),
         terminal_reviews AS (
           SELECT DISTINCT workspace_id,assignment_id,activity_id,target_agent_id,target_session_id
           FROM ops_assignment_review_projection
           WHERE workspace_id=?1 AND outcome IN ('Accepted','Rejected')
             AND target_agent_id<>reviewer_agent_id
         ),
         attribution_witnesses AS (
           SELECT workspace_id,activity_id,agent_id,session_id,participant_id
           FROM ops_current_effect_bindings WHERE workspace_id=?1
           UNION ALL
           SELECT binding.workspace_id,binding.activity_id,binding.agent_id,binding.session_id,binding.participant_id
           FROM ops_logical_bindings AS binding
           JOIN ops_binding_activations AS activation ON activation.request_id=binding.request_id
           WHERE binding.workspace_id=?1
           UNION ALL
           SELECT workspace_id,activity_id,agent_id,new_session_id,participant_id
           FROM ops_workspace_participation_replacement_registrations
           WHERE workspace_id=?1 AND phase=3
           UNION ALL
           SELECT workspace_id,activity_id,agent_id,session_id,participant_id
           FROM ops_legacy_adoptions WHERE workspace_id=?1
           UNION ALL
           SELECT workspace_id,activity_id,agent_id,session_id,participant_id
           FROM ops_logical_departures WHERE workspace_id=?1
         ),
         ledger AS (
           SELECT workspace_id,assignment_id,activity_id,agent_id,participant_id,award_source_sequence
           FROM ops_work_credit_entries WHERE workspace_id=?1
         ),
         review_shape_conflicts AS (
           SELECT assignment_id
           FROM terminal_reviews
           GROUP BY assignment_id
           HAVING COUNT(*)>1
         ),
         ledger_identity_conflicts AS (
           SELECT DISTINCT review.assignment_id
           FROM terminal_reviews AS review
           JOIN ledger
             ON ledger.workspace_id=review.workspace_id
            AND ledger.assignment_id=review.assignment_id
           WHERE ledger.activity_id<>review.activity_id
              OR ledger.agent_id<>review.target_agent_id
         ),
         witness_session_conflicts AS (
           SELECT DISTINCT review.assignment_id
           FROM terminal_reviews AS review
           JOIN attribution_witnesses AS witness
             ON witness.workspace_id=review.workspace_id
            AND witness.activity_id=review.activity_id
            AND witness.agent_id=review.target_agent_id
           WHERE review.target_session_id IS NOT NULL
             AND witness.session_id<>review.target_session_id
         ),
         matched_witnesses AS (
           SELECT review.assignment_id,witness.participant_id,witness.session_id
           FROM terminal_reviews AS review
           JOIN attribution_witnesses AS witness
             ON witness.workspace_id=review.workspace_id
            AND witness.activity_id=review.activity_id
            AND witness.agent_id=review.target_agent_id
            AND (review.target_session_id IS NULL OR witness.session_id=review.target_session_id)
           UNION ALL
           SELECT review.assignment_id,ledger.participant_id,NULL
           FROM terminal_reviews AS review
           JOIN ledger
             ON ledger.workspace_id=review.workspace_id
            AND ledger.assignment_id=review.assignment_id
           WHERE ledger.participant_id IS NOT NULL
             AND (ledger.assignment_id,ledger.award_source_sequence) NOT IN (
                   SELECT assignment_id,source_sequence FROM self_reviewed_awards
             )
         ),
         assignment_witness_stats AS (
           SELECT assignment_id,
                  COUNT(DISTINCT participant_id) AS participant_count,
                  COUNT(DISTINCT session_id) AS session_count,
                  MIN(participant_id) AS participant_id
           FROM matched_witnesses
           GROUP BY assignment_id
         ),
         attribution_conflicts AS (
           SELECT assignment_id FROM review_shape_conflicts
           UNION
           SELECT assignment_id FROM ledger_identity_conflicts
           UNION
           SELECT assignment_id FROM witness_session_conflicts
           UNION
           SELECT stats.assignment_id
           FROM assignment_witness_stats AS stats
           WHERE stats.participant_count>1
              OR (stats.session_count>1 AND EXISTS (
                    SELECT 1 FROM terminal_reviews AS review
                    WHERE review.assignment_id=stats.assignment_id
                      AND review.target_session_id IS NULL
              ))
         ),
         conflict_row AS (
           SELECT 1 AS has_conflict,CAST(NULL AS BLOB) AS participant_id,0 AS completed,0 AS evaluated
           FROM attribution_conflicts LIMIT 1
         ),
         resolved_assignments AS (
           SELECT review.assignment_id,stats.participant_id,
                  CASE WHEN ledger.assignment_id IS NOT NULL
                         AND (ledger.assignment_id,ledger.award_source_sequence) NOT IN (
                           SELECT assignment_id,source_sequence FROM self_reviewed_awards
                         )
                       THEN 1 ELSE 0 END AS has_credit
           FROM terminal_reviews AS review
           JOIN assignment_witness_stats AS stats
             ON stats.assignment_id=review.assignment_id
           LEFT JOIN ledger
             ON ledger.workspace_id=review.workspace_id
            AND ledger.assignment_id=review.assignment_id
           WHERE stats.participant_count=1
           GROUP BY review.assignment_id,stats.participant_id,ledger.assignment_id
         ),
         participant_rows AS (
           SELECT participant_id,
                  COUNT(DISTINCT CASE WHEN has_credit=1 THEN assignment_id END) AS completed,
                  COUNT(DISTINCT assignment_id) AS evaluated
           FROM resolved_assignments
           GROUP BY participant_id
         ),
         result_rows AS (
           SELECT has_conflict,participant_id,completed,evaluated FROM conflict_row
           UNION ALL
           SELECT 0,participant_id,completed,evaluated FROM participant_rows
         )
         SELECT has_conflict,participant_id,completed,evaluated
         FROM result_rows
         ORDER BY has_conflict DESC,participant_id ASC";

fn fulfillment_per_participant_at(
    connection: &Connection,
    workspace_id: WorkspaceId,
) -> Result<Vec<WorkCreditParticipantFulfillment>, OperationsError> {
    let mut statement = connection.prepare(FULFILLMENT_ATTRIBUTION_SQL)?;
    let raw = statement
        .query_map(params![workspace_id.into_uuid().as_bytes()], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<Vec<u8>>>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    if raw.iter().any(|(conflict, _, _, _)| *conflict != 0) {
        return Err(OperationsError::Corrupt("work credit attribution"));
    }
    raw.into_iter()
        .map(|(_, participant, completed, evaluated)| {
            let participant = participant.ok_or(OperationsError::Corrupt(
                "work credit participant attribution",
            ))?;
            Ok(WorkCreditParticipantFulfillment {
                participant_id: Uuid::from_bytes(wire::id(participant)?),
                completed,
                evaluated,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WorkspaceParticipationId;
    use std::error::Error;

    fn catalog_fixture(
        name: &str,
    ) -> Result<
        (
            std::path::PathBuf,
            crate::OperationsCatalog,
            Uuid,
            WorkspaceId,
        ),
        Box<dyn std::error::Error>,
    > {
        let root = std::env::temp_dir().join(format!(
            "orchestration-work-credit-{name}-{}",
            Uuid::now_v7()
        ));
        std::fs::create_dir(&root)?;
        let destination = Uuid::from_u128(0xE001);
        let migration = Uuid::from_u128(0xE002);
        let workspace = WorkspaceId::from_uuid(Uuid::from_u128(0xE003));
        let catalog = crate::OperationsCatalog::open(&root.join("catalog.sqlite3"), destination)?;
        catalog.connection.execute(
            "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
            params![migration.as_bytes(), workspace.into_uuid().as_bytes(), destination.as_bytes(), [0u8;32].as_slice()],
        )?;
        Ok((root, catalog, migration, workspace))
    }

    fn insert_credit(
        connection: &Connection,
        workspace: WorkspaceId,
        assignment: u128,
        participant: Option<Uuid>,
        agent: u128,
        budget: WorkCreditBudget,
        awarded_at_ms: i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let assignment_id = AgentAssignmentId::from_uuid(Uuid::from_u128(assignment));
        let observation = ReviewAwardObservation {
            workspace_id: workspace,
            assignment_id,
            activity_id: forge_domain::ActivityId::from_uuid(Uuid::from_u128(assignment + 1)),
            goal_id: GoalId::from_uuid(Uuid::from_u128(assignment + 2)),
            node_id: DagNodeId::from_uuid(Uuid::from_u128(assignment + 3)),
            target_agent_id: AgentInstanceId::from_uuid(Uuid::from_u128(agent)),
            reviewer_agent_id: AgentInstanceId::from_uuid(Uuid::from_u128(agent + 0x1000)),
            budget: Some(budget),
            award_source_sequence: 1,
            awarded_at_ms,
        };
        let credit = CreditLedgerRow {
            credit_id: credit_id(workspace, assignment_id),
            workspace_id: workspace,
            assignment_id,
            agent_id: observation.target_agent_id,
            activity_id: observation.activity_id,
            goal_id: observation.goal_id,
            node_id: observation.node_id,
            budget,
            award_source_sequence: 1,
            evidence_execution_id: ExecutionId::from_uuid(Uuid::from_u128(assignment + 4)),
            awarded_at_ms,
            created_digest: created_digest(
                &observation,
                budget,
                ExecutionId::from_uuid(Uuid::from_u128(assignment + 4)),
            ),
        };
        connection.execute(
            "INSERT INTO ops_work_credit_entries(credit_id,workspace_id,assignment_id,participant_id,agent_id,activity_id,goal_id,node_id,budget,award_source_sequence,evidence_execution_id,awarded_at_ms,created_digest) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
            params![
                credit.credit_id.as_bytes(),
                credit.workspace_id.into_uuid().as_bytes(),
                credit.assignment_id.into_uuid().as_bytes(),
                participant.map(|value| value.as_bytes().to_vec()),
                credit.agent_id.into_uuid().as_bytes(),
                credit.activity_id.into_uuid().as_bytes(),
                credit.goal_id.into_uuid().as_bytes(),
                credit.node_id.into_uuid().as_bytes(),
                credit.budget.value(),
                1_i64,
                credit.evidence_execution_id.into_uuid().as_bytes(),
                credit.awarded_at_ms,
                credit.created_digest.as_slice(),
            ],
        )?;
        Ok(())
    }

    fn insert_member_and_binding(
        connection: &Connection,
        workspace: WorkspaceId,
        participant: Uuid,
        session: u128,
        agent: u128,
    ) -> Result<(), Box<dyn std::error::Error>> {
        connection.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,?4,?5,1,1,1)",
            params![
                workspace.into_uuid().as_bytes(),
                participant.as_bytes(),
                Uuid::from_u128(session).as_bytes(),
                "score participant",
                "worker",
            ],
        )?;
        connection.execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
            params![
                workspace.into_uuid().as_bytes(),
                participant.as_bytes(),
                Uuid::from_u128(session + 0x10).as_bytes(),
                Uuid::from_u128(session).as_bytes(),
                Uuid::from_u128(agent).as_bytes(),
                Uuid::from_u128(session + 0x20).as_bytes(),
            ],
        )?;
        Ok(())
    }

    fn insert_import_and_consumer(
        connection: &Connection,
        migration: Uuid,
        workspace: WorkspaceId,
        destination: Uuid,
        phase: i64,
        page_count: i64,
        verified_pages: i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        connection.execute(
            "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,?4,NULL,?5,0,?6,?7,?6)",
            params![
                migration.as_bytes(),
                workspace.into_uuid().as_bytes(),
                destination.as_bytes(),
                phase,
                page_count,
                [0_u8; 32].as_slice(),
                verified_pages,
            ],
        )?;
        connection.execute(
            "INSERT INTO ops_effect_fact_consumers(migration_id,workspace_id,destination_id,last_sequence,applied_count) VALUES(?1,?2,?3,0,0)",
            params![
                migration.as_bytes(),
                workspace.into_uuid().as_bytes(),
                destination.as_bytes(),
            ],
        )?;
        Ok(())
    }

    fn insert_review_projection(
        connection: &Connection,
        migration: Uuid,
        sequence: i64,
        workspace: WorkspaceId,
        assignment: u128,
        outcome: &str,
        credit_budget: Option<WorkCreditBudget>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut fact_digest = [0_u8; 32];
        fact_digest[24..].copy_from_slice(&sequence.to_be_bytes());
        connection.execute(
            "INSERT INTO ops_assignment_review_projection(migration_id,source_sequence,fact_digest,assignment_id,workspace_id,activity_id,goal_id,node_id,target_agent_id,target_session_id,outcome,credit_budget,reviewer_agent_id,result_digest,observed_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
            params![
                migration.as_bytes(),
                sequence,
                fact_digest.as_slice(),
                Uuid::from_u128(assignment).as_bytes(),
                workspace.into_uuid().as_bytes(),
                Uuid::from_u128(assignment + 1).as_bytes(),
                Uuid::from_u128(assignment + 2).as_bytes(),
                Uuid::from_u128(assignment + 3).as_bytes(),
                Uuid::from_u128(assignment + 4).as_bytes(),
                Option::<Vec<u8>>::None,
                outcome,
                credit_budget.map(WorkCreditBudget::value),
                Uuid::from_u128(assignment + 5).as_bytes(),
                Option::<Vec<u8>>::None,
                100_i64 + sequence,
            ],
        )?;
        Ok(())
    }

    fn insert_offer_projection(
        connection: &Connection,
        migration: Uuid,
        sequence: i64,
        workspace: WorkspaceId,
        assignment: u128,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let digest_byte = u8::try_from(sequence)?;
        connection.execute(
            "INSERT INTO ops_assignment_offer_projection(migration_id,source_sequence,fact_digest,assignment_id,workspace_id,activity_id,goal_id,node_id,target_agent_id,target_session_id,credit_budget,offered_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            params![
                migration.as_bytes(),
                sequence,
                [digest_byte; 32].as_slice(),
                Uuid::from_u128(assignment).as_bytes(),
                workspace.into_uuid().as_bytes(),
                Uuid::from_u128(assignment + 1).as_bytes(),
                Uuid::from_u128(assignment + 2).as_bytes(),
                Uuid::from_u128(assignment + 3).as_bytes(),
                Uuid::from_u128(assignment + 4).as_bytes(),
                Option::<Vec<u8>>::None,
                3_i64,
                200_i64,
            ],
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_submit_row(
        connection: &Connection,
        migration: Uuid,
        sequence: i64,
        workspace: WorkspaceId,
        assignment: u128,
        activity: u128,
        agent: u128,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let digest_byte = u8::try_from(sequence)?;
        connection.execute(
            "INSERT INTO ops_assignment_submit_projection(migration_id,source_sequence,fact_digest,assignment_id,workspace_id,activity_id,goal_id,node_id,target_agent_id,target_session_id,source_count,source_digests,source_kinds,result_digest,submitted_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,0,NULL,NULL,NULL,100)",
            params![
                migration.as_bytes(),
                sequence,
                [digest_byte; 32].as_slice(),
                Uuid::from_u128(assignment).as_bytes(),
                workspace.into_uuid().as_bytes(),
                Uuid::from_u128(activity).as_bytes(),
                Uuid::from_u128(assignment + 1).as_bytes(),
                Uuid::from_u128(assignment + 2).as_bytes(),
                Uuid::from_u128(agent).as_bytes(),
                Some(Uuid::from_u128(agent + 1).as_bytes().to_vec()),
            ],
        )?;
        Ok(())
    }

    #[derive(Debug, Clone, Copy)]
    struct SubmitSourceRefCase {
        sequence: i64,
        assignment: u128,
        activity: u128,
        agent: u128,
        change_set: u128,
    }

    fn insert_submit_row_with_change_set_ref(
        connection: &Connection,
        migration: Uuid,
        workspace: WorkspaceId,
        case: SubmitSourceRefCase,
    ) -> Result<(), Box<dyn std::error::Error>> {
        insert_submit_projection_case(
            connection,
            migration,
            workspace,
            case.sequence,
            1,
            Some(&[0xAA; 32]),
            Some(&[1]),
        )?;
        connection.execute(
            "UPDATE ops_assignment_submit_projection SET assignment_id=?1,activity_id=?2,target_agent_id=?3,target_session_id=?4,source_refs_recorded=1 WHERE migration_id=?5 AND source_sequence=?6",
            params![
                Uuid::from_u128(case.assignment).as_bytes(),
                Uuid::from_u128(case.activity).as_bytes(),
                Uuid::from_u128(case.agent).as_bytes(),
                Uuid::from_u128(case.agent + 1).as_bytes(),
                migration.as_bytes(),
                case.sequence,
            ],
        )?;
        connection.execute(
            "INSERT INTO ops_assignment_submit_source_refs(migration_id,source_sequence,ordinal,evidence_kind,evidence_id) VALUES(?1,?2,0,0,?3)",
            params![
                migration.as_bytes(),
                case.sequence,
                Uuid::from_u128(case.change_set).as_bytes()
            ],
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_change_set_row(
        connection: &Connection,
        migration: Uuid,
        sequence: i64,
        workspace: WorkspaceId,
        change_set: u128,
        activity: u128,
        producer_agent: Option<u128>,
        state: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let digest_byte = u8::try_from(sequence)?;
        let failure_code: Option<String> = if state == "Failed" {
            Some("IoFailure".to_owned())
        } else {
            None
        };
        connection.execute(
            "INSERT INTO ops_change_set_projection(migration_id,source_sequence,fact_digest,change_set_id,workspace_id,activity_id,change_set_revision,state,failure_code,diff_id,diff_state,diff_revision,observed_at_ms,producer_agent_id,producer_session_id) VALUES(?1,?2,?3,?4,?5,?6,1,?7,?8,?9,'Ready',1,100,?10,?11)",
            params![
                migration.as_bytes(),
                sequence,
                [digest_byte; 32].as_slice(),
                Uuid::from_u128(change_set).as_bytes(),
                workspace.into_uuid().as_bytes(),
                Uuid::from_u128(activity).as_bytes(),
                state,
                failure_code,
                Uuid::from_u128(change_set + 0x50).as_bytes(),
                producer_agent.map(|agent| Uuid::from_u128(agent).as_bytes().to_vec()),
                producer_agent.map(|agent| Uuid::from_u128(agent + 1).as_bytes().to_vec()),
            ],
        )?;
        Ok(())
    }

    fn insert_submit_projection_case(
        connection: &Connection,
        migration: Uuid,
        workspace: WorkspaceId,
        sequence: i64,
        source_count: i64,
        source_digests: Option<&[u8]>,
        source_kinds: Option<&[u8]>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let seed = 0xD000_u128 + u128::try_from(sequence)?;
        let digest_byte = u8::try_from(sequence)?;
        connection.execute(
            "INSERT INTO ops_assignment_submit_projection(migration_id,source_sequence,fact_digest,assignment_id,workspace_id,activity_id,goal_id,node_id,target_agent_id,target_session_id,source_count,source_digests,source_kinds,result_digest,submitted_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
            params![
                migration.as_bytes(),
                sequence,
                [digest_byte; 32].as_slice(),
                Uuid::from_u128(seed).as_bytes(),
                workspace.into_uuid().as_bytes(),
                Uuid::from_u128(seed + 1).as_bytes(),
                Uuid::from_u128(seed + 2).as_bytes(),
                Uuid::from_u128(seed + 3).as_bytes(),
                Uuid::from_u128(seed + 4).as_bytes(),
                Option::<Vec<u8>>::None,
                source_count,
                source_digests,
                source_kinds,
                Option::<Vec<u8>>::None,
                100_i64,
            ],
        )?;
        Ok(())
    }

    fn assert_corrupt_submit_projection(
        catalog: &crate::OperationsCatalog,
        migration: Uuid,
        workspace: WorkspaceId,
        source_count: i64,
        source_digests: Option<&[u8]>,
        source_kinds: Option<&[u8]>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        catalog.connection.execute(
            "DELETE FROM ops_assignment_submit_projection WHERE migration_id=?1",
            params![migration.as_bytes()],
        )?;
        insert_submit_projection_case(
            &catalog.connection,
            migration,
            workspace,
            1,
            source_count,
            source_digests,
            source_kinds,
        )?;
        assert!(matches!(
            catalog.assignment_submit_projection(migration),
            Err(OperationsError::Corrupt(_))
        ));
        Ok(())
    }

    #[test]
    fn catalog_v10_migrates_additively_without_fabricating_work_credits()
    -> Result<(), Box<dyn std::error::Error>> {
        let (root, catalog, migration, workspace) = catalog_fixture("v10-migration")?;
        let destination = catalog.destination_id();
        let path = root.join("catalog.sqlite3");
        drop(catalog);

        let connection = Connection::open(&path)?;
        crate::workspace_participation::drop_schema_for_legacy_fixture(&connection)?;
        // The downgraded catalog must keep the exact v10 change-set shape:
        // SQLite's ADD COLUMN rewrite places the producer columns before the
        // table constraints, so a v14-created table text already carries them.
        connection.execute_batch(&format!(
            "DROP TABLE ops_admission_records; DROP TABLE ops_approval_requests; DROP TABLE ops_approval_policy; DROP TABLE ops_issuer_config; DROP TABLE ops_leadership_confirm_intents; DROP TABLE ops_leadership_receipts; DROP TABLE ops_leadership_elections; DROP TABLE ops_election_electorate; DROP TABLE ops_ballots; DROP TABLE ops_leadership; DROP TABLE ops_work_credit_entries; DROP TABLE ops_assignment_submit_source_refs; DROP TABLE ops_assignment_submit_projection; DROP TABLE ops_assignment_offer_projection; DROP TABLE ops_assignment_review_projection; DROP TABLE ops_change_set_projection; {};",
            crate::effect_fact::change_sets_sql_v13()
        ))?;
        connection.pragma_update(None, "user_version", 10_i64)?;
        drop(connection);

        let migrated = crate::OperationsCatalog::open(&path, destination)?;
        let version: i64 = migrated
            .connection
            .pragma_query_value(None, "user_version", |row| row.get(0))?;
        assert_eq!(version, crate::catalog::VERSION);
        let import = migrated
            .progress(migration)?
            .ok_or("verified import disappeared")?;
        assert_eq!(import.workspace_id(), workspace);
        assert_eq!(import.phase(), crate::ImportPhase::Verified);
        assert!(
            migrated.work_credit_entries(workspace)?.is_empty(),
            "legacy migration must not fabricate work credits"
        );
        assert!(
            migrated.assignment_review_projection(migration)?.is_empty(),
            "legacy migration must recreate the review projection additively"
        );
        drop(migrated);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn work_credit_fulfillment_counts_credits_over_distinct_reviews()
    -> Result<(), Box<dyn std::error::Error>> {
        let (root, catalog, migration, workspace) = catalog_fixture("fulfillment")?;
        let destination = catalog.destination_id();
        catalog.connection.execute(
            "INSERT INTO ops_effect_fact_consumers(migration_id,workspace_id,destination_id,last_sequence,applied_count) VALUES(?1,?2,?3,0,0)",
            params![
                migration.as_bytes(),
                workspace.into_uuid().as_bytes(),
                destination.as_bytes(),
            ],
        )?;
        assert_eq!(catalog.work_credit_fulfillment(workspace)?, (0, 0));
        for (sequence, assignment, outcome, budget) in [
            (1, 0xC301, "Accepted", Some(WorkCreditBudget::Three)),
            (2, 0xC302, "Accepted", Some(WorkCreditBudget::Five)),
            (3, 0xC303, "Rejected", Some(WorkCreditBudget::One)),
        ] {
            insert_review_projection(
                &catalog.connection,
                migration,
                sequence,
                workspace,
                assignment,
                outcome,
                budget,
            )?;
        }
        insert_credit(
            &catalog.connection,
            workspace,
            0xC301,
            None,
            0xC3A1,
            WorkCreditBudget::Three,
            101,
        )?;
        insert_offer_projection(&catalog.connection, migration, 4, workspace, 0xC304)?;

        let duplicate_migration = Uuid::from_u128(0xE004);
        insert_import_and_consumer(
            &catalog.connection,
            duplicate_migration,
            workspace,
            destination,
            3,
            0,
            0,
        )?;
        insert_review_projection(
            &catalog.connection,
            duplicate_migration,
            1,
            workspace,
            0xC301,
            "Accepted",
            Some(WorkCreditBudget::Three),
        )?;

        let foreign_workspace = WorkspaceId::from_uuid(Uuid::from_u128(0xF003));
        let foreign_migration = Uuid::from_u128(0xE005);
        insert_import_and_consumer(
            &catalog.connection,
            foreign_migration,
            foreign_workspace,
            destination,
            2,
            5,
            5,
        )?;
        insert_review_projection(
            &catalog.connection,
            foreign_migration,
            1,
            foreign_workspace,
            0xF301,
            "Accepted",
            Some(WorkCreditBudget::Five),
        )?;
        insert_credit(
            &catalog.connection,
            foreign_workspace,
            0xF301,
            None,
            0xF3A1,
            WorkCreditBudget::Five,
            101,
        )?;

        assert_eq!(catalog.work_credit_fulfillment(workspace)?, (1, 3));
        drop(catalog);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn submit_provenance_join_matches_only_exact_attribution()
    -> Result<(), Box<dyn std::error::Error>> {
        let (root, catalog, migration, workspace) = catalog_fixture("provenance-join")?;
        let destination = catalog.destination_id();
        catalog.connection.execute(
            "INSERT INTO ops_effect_fact_consumers(migration_id,workspace_id,destination_id,last_sequence,applied_count) VALUES(?1,?2,?3,0,0)",
            params![
                migration.as_bytes(),
                workspace.into_uuid().as_bytes(),
                destination.as_bytes(),
            ],
        )?;
        // Exact match: same activity, producer == target agent, Applied.
        insert_submit_row_with_change_set_ref(
            &catalog.connection,
            migration,
            workspace,
            SubmitSourceRefCase {
                sequence: 1,
                assignment: 0xB101,
                activity: 0x1111,
                agent: 0x2222,
                change_set: 0xC105,
            },
        )?;
        insert_change_set_row(
            &catalog.connection,
            migration,
            2,
            workspace,
            0xC101,
            0x1111,
            Some(0x2222),
            "Applied",
        )?;
        insert_change_set_row(
            &catalog.connection,
            migration,
            10,
            workspace,
            0xC105,
            0x1111,
            Some(0x2222),
            "Applied",
        )?;
        // Failed change set: no artifact evidence.
        insert_submit_row(
            &catalog.connection,
            migration,
            3,
            workspace,
            0xB102,
            0x1111,
            0x3333,
        )?;
        insert_change_set_row(
            &catalog.connection,
            migration,
            4,
            workspace,
            0xC102,
            0x1111,
            Some(0x3333),
            "Failed",
        )?;
        // Foreign producer/session/scope: an explicit source reference still
        // cannot claim an artifact owned by a different actor.
        insert_submit_row_with_change_set_ref(
            &catalog.connection,
            migration,
            workspace,
            SubmitSourceRefCase {
                sequence: 5,
                assignment: 0xB103,
                activity: 0x4444,
                agent: 0x5555,
                change_set: 0xC103,
            },
        )?;
        insert_change_set_row(
            &catalog.connection,
            migration,
            6,
            workspace,
            0xC103,
            0x4445,
            Some(0x5555),
            "Applied",
        )?;
        // Pre-v47 unattributed fact: never matches (fail closed).
        insert_submit_row(
            &catalog.connection,
            migration,
            7,
            workspace,
            0xB104,
            0x6666,
            0x7777,
        )?;
        insert_change_set_row(
            &catalog.connection,
            migration,
            8,
            workspace,
            0xC104,
            0x6666,
            None,
            "Applied",
        )?;
        // Submission without any change set at all.
        insert_submit_row(
            &catalog.connection,
            migration,
            9,
            workspace,
            0xB105,
            0x8888,
            0x9999,
        )?;

        let rows = catalog.work_credit_submit_provenance(workspace)?;
        assert_eq!(rows.len(), 5, "all kind-4 submissions are returned");
        let matched: Vec<u128> = rows
            .iter()
            .filter_map(|row| row.provenance())
            .map(|match_| match_.producer_agent_id().into_uuid().as_u128())
            .collect();
        assert_eq!(matched, vec![0x2222], "only the exact match has evidence");
        let first = &rows[0];
        assert_eq!(
            first.assignment_id().into_uuid().as_u128(),
            0xB101,
            "rows read oldest-submission first"
        );
        let provenance = first
            .provenance()
            .ok_or("exact match must carry evidence")?;
        assert_eq!(
            provenance.dimension(),
            ProvenanceDimension::ExactSubmittedSourceReference
        );
        assert_eq!(
            provenance.change_set_id().into_uuid().as_u128(),
            0xC105,
            "the explicit source reference wins over an unrelated earlier change set"
        );
        assert_eq!(provenance.diff_id().into_uuid().as_u128(), 0xC155);
        assert_eq!(
            provenance
                .producer_session_id()
                .map(|session| session.into_uuid().as_u128()),
            Some(0x2223)
        );
        assert!(
            rows[1..].iter().all(|row| row.provenance().is_none()),
            "insufficient evidence must stay None"
        );

        let snapshot = catalog.work_credit_scores(workspace, 1_000)?;
        assert_eq!(snapshot.provenance_submitted(), 5);
        assert_eq!(snapshot.provenance_matched(), 1);
        drop(catalog);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn legacy_submit_rows_do_not_fabricate_source_references()
    -> Result<(), Box<dyn std::error::Error>> {
        let (root, catalog, migration, workspace) = catalog_fixture("legacy-submit-refs")?;
        let destination = catalog.destination_id();
        catalog.connection.execute(
            "INSERT INTO ops_effect_fact_consumers(migration_id,workspace_id,destination_id,last_sequence,applied_count) VALUES(?1,?2,?3,0,0)",
            params![
                migration.as_bytes(),
                workspace.into_uuid().as_bytes(),
                destination.as_bytes(),
            ],
        )?;
        insert_submit_row(
            &catalog.connection,
            migration,
            1,
            workspace,
            0xB201,
            0x1212,
            0x3434,
        )?;
        insert_change_set_row(
            &catalog.connection,
            migration,
            2,
            workspace,
            0xC201,
            0x1212,
            Some(0x3434),
            "Applied",
        )?;
        let rows = catalog.assignment_submit_projection(migration)?;
        assert_eq!(rows.len(), 1);
        assert!(!rows[0].source_refs_recorded());
        assert!(
            catalog.work_credit_submit_provenance(workspace)?[0]
                .provenance()
                .is_none()
        );
        drop(catalog);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn fulfillment_denominator_counts_only_terminal_reviews()
    -> Result<(), Box<dyn std::error::Error>> {
        let (root, catalog, migration, workspace) = catalog_fixture("fulfillment-denominator")?;
        let destination = catalog.destination_id();
        catalog.connection.execute(
            "INSERT INTO ops_effect_fact_consumers(migration_id,workspace_id,destination_id,last_sequence,applied_count) VALUES(?1,?2,?3,0,0)",
            params![
                migration.as_bytes(),
                workspace.into_uuid().as_bytes(),
                destination.as_bytes(),
            ],
        )?;
        let participant = Uuid::from_u128(0xD901);
        // Review 0xB201 has activity 0xB202 / Agent 0xB205. Match the durable
        // binding to that exact review identity rather than relying on Agent
        // identity alone.
        insert_member_and_binding(&catalog.connection, workspace, participant, 0xD911, 0xB205)?;
        catalog.connection.execute(
            "UPDATE ops_current_effect_bindings SET activity_id=?1 WHERE workspace_id=?2 AND agent_id=?3",
            params![
                Uuid::from_u128(0xB202).as_bytes(),
                workspace.into_uuid().as_bytes(),
                Uuid::from_u128(0xB205).as_bytes(),
            ],
        )?;
        // Review 0xB202 has activity 0xB203 / Agent 0xB206.
        catalog.connection.execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
            params![
                workspace.into_uuid().as_bytes(),
                participant.as_bytes(),
                Uuid::from_u128(0xB203).as_bytes(),
                Uuid::from_u128(0xD912).as_bytes(),
                Uuid::from_u128(0xB206).as_bytes(),
                Uuid::from_u128(0xD932).as_bytes(),
            ],
        )?;
        // Terminal reviews for exact Agent identities 0xB205 and 0xB206.
        insert_review_projection(
            &catalog.connection,
            migration,
            1,
            workspace,
            0xB201,
            "Accepted",
            Some(WorkCreditBudget::Three),
        )?;
        insert_review_projection(
            &catalog.connection,
            migration,
            2,
            workspace,
            0xB202,
            "Rejected",
            Some(WorkCreditBudget::One),
        )?;
        // A retained departure is the immutable attribution witness after the
        // live effect binding is retired. Store the event, departure, and
        // projection deletion atomically as production does.
        let departure_request = Uuid::from_u128(0xD940);
        let departed_event = Uuid::from_u128(0xD941);
        {
            let transaction = catalog.connection.unchecked_transaction()?;
            transaction.execute(
                "INSERT INTO ops_logical_departure_events(workspace_id,event_id,participant_id,activity_id,event_kind,membership_revision,request_id,observed_at_ms) VALUES(?1,?2,?3,?4,1,2,?5,104)",
                params![
                    workspace.into_uuid().as_bytes(),
                    departed_event.as_bytes(),
                    participant.as_bytes(),
                    Uuid::from_u128(0xB203).as_bytes(),
                    departure_request.as_bytes(),
                ],
            )?;
            transaction.execute(
                "INSERT INTO ops_logical_departures(request_id,workspace_id,participant_id,activity_id,agent_id,session_id,kind,expected_agent_revision,result_agent_revision,target_participant_id,target_agent_id,target_session_id,result_coordinator_revision,departed_event_id,started_event_id,completed_event_id,membership_revision,presence_cleared,source_reconciled_at_ms,committed_at_ms) VALUES(?1,?2,?3,?4,?5,?6,0,1,2,NULL,NULL,NULL,NULL,?7,NULL,NULL,2,0,104,105)",
                params![
                    departure_request.as_bytes(),
                    workspace.into_uuid().as_bytes(),
                    participant.as_bytes(),
                    Uuid::from_u128(0xB203).as_bytes(),
                    Uuid::from_u128(0xB206).as_bytes(),
                    Uuid::from_u128(0xD912).as_bytes(),
                    departed_event.as_bytes(),
                ],
            )?;
            transaction.execute(
                "DELETE FROM ops_current_effect_bindings WHERE workspace_id=?1 AND agent_id=?2",
                params![
                    workspace.into_uuid().as_bytes(),
                    Uuid::from_u128(0xB206).as_bytes(),
                ],
            )?;
            transaction.commit()?;
        }
        // Terminal review for an unbound agent: attributed to nobody.
        insert_review_projection(
            &catalog.connection,
            migration,
            3,
            workspace,
            0xB203,
            "Accepted",
            None,
        )?;
        // Running assignment: a kind-4 submission without any kind-2 review.
        insert_submit_row(
            &catalog.connection,
            migration,
            4,
            workspace,
            0xB204,
            0xD931,
            0xD941,
        )?;
        // Credit for the accepted assignment, attributed through the binding.
        insert_credit(
            &catalog.connection,
            workspace,
            0xB201,
            None,
            0xB205,
            WorkCreditBudget::Three,
            101,
        )?;

        // Workspace totals: completed = credited assignments, evaluated = all
        // terminal reviews (accepted AND rejected). The running submission and
        // the offer projection contribute nothing.
        assert_eq!(catalog.work_credit_fulfillment(workspace)?, (1, 3));

        let snapshot = catalog.work_credit_scores(workspace, 1_000)?;
        assert_eq!(snapshot.fulfillment_completed(), 1);
        assert_eq!(snapshot.fulfillment_evaluated(), 3);
        let fulfillment = snapshot.fulfillment_per_subject();
        assert_eq!(
            fulfillment.len(),
            1,
            "unbound work stays in the totals only"
        );
        assert_eq!(
            fulfillment[0].subject().legacy_participant_id(),
            Some(participant)
        );
        assert_eq!(fulfillment[0].completed(), 1);
        assert_eq!(fulfillment[0].evaluated(), 2);
        // A ledger entry without a terminal review must not inflate completion.
        insert_credit(
            &catalog.connection,
            workspace,
            0xB205,
            Some(participant),
            0xB205,
            WorkCreditBudget::One,
            102,
        )?;
        let duplicate_migration = Uuid::from_u128(0xD950);
        insert_import_and_consumer(
            &catalog.connection,
            duplicate_migration,
            workspace,
            destination,
            3,
            0,
            0,
        )?;
        // Reimporting the same review must not inflate the evaluated denominator.
        insert_review_projection(
            &catalog.connection,
            duplicate_migration,
            1,
            workspace,
            0xB201,
            "Accepted",
            Some(WorkCreditBudget::Three),
        )?;
        let rows = participant_fulfillment_rows(&catalog.connection, workspace)?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].completed(), 1);
        assert_eq!(rows[0].evaluated(), 2);
        assert_eq!(catalog.work_credit_fulfillment(workspace)?, (1, 3));

        // A second exact Agent witness with a different Session is ambiguous
        // when the immutable review has no target Session.
        let conflicting_participant = Uuid::from_u128(0xD902);
        catalog.connection.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,'conflict participant','worker',1,1,1)",
            params![
                workspace.into_uuid().as_bytes(),
                conflicting_participant.as_bytes(),
                Uuid::from_u128(0xB1F3).as_bytes(),
            ],
        )?;
        catalog.connection.execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
            params![
                workspace.into_uuid().as_bytes(),
                participant.as_bytes(),
                Uuid::from_u128(0xB203).as_bytes(),
                Uuid::from_u128(0xB1F3).as_bytes(),
                Uuid::from_u128(0xB206).as_bytes(),
                Uuid::from_u128(0xB1F4).as_bytes(),
            ],
        )?;
        assert!(matches!(
            participant_fulfillment_rows(&catalog.connection, workspace),
            Err(OperationsError::Corrupt("work credit attribution"))
        ));

        // A known review Session is also an exact fence, not a reason to
        // silently ignore a contradictory witness for the same Agent.
        catalog.connection.execute(
            "UPDATE ops_assignment_review_projection SET target_session_id=?1 WHERE workspace_id=?2 AND assignment_id=?3",
            params![Uuid::from_u128(0xD912).as_bytes(), workspace.into_uuid().as_bytes(), Uuid::from_u128(0xB202).as_bytes()],
        )?;
        assert!(matches!(
            participant_fulfillment_rows(&catalog.connection, workspace),
            Err(OperationsError::Corrupt("work credit attribution"))
        ));

        // Keep the Session equal to the departure witness while changing only
        // the current binding's participant, proving participant disagreement
        // is independently rejected.
        catalog.connection.execute(
            "UPDATE ops_current_effect_bindings SET participant_id=?1,session_id=?2 WHERE workspace_id=?3 AND agent_id=?4",
            params![
                conflicting_participant.as_bytes(),
                Uuid::from_u128(0xD912).as_bytes(),
                workspace.into_uuid().as_bytes(),
                Uuid::from_u128(0xB206).as_bytes(),
            ],
        )?;
        assert!(matches!(
            participant_fulfillment_rows(&catalog.connection, workspace),
            Err(OperationsError::Corrupt("work credit attribution"))
        ));
        drop(catalog);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn assignment_submit_projection_reader_rejects_corrupt_source_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        let (root, catalog, migration, workspace) = catalog_fixture("submit-read-corruption")?;
        let destination = catalog.destination_id();
        catalog.connection.execute(
            "INSERT INTO ops_effect_fact_consumers(migration_id,workspace_id,destination_id,last_sequence,applied_count) VALUES(?1,?2,?3,0,0)",
            params![
                migration.as_bytes(),
                workspace.into_uuid().as_bytes(),
                destination.as_bytes(),
            ],
        )?;
        catalog
            .connection
            .pragma_update(None, "ignore_check_constraints", true)?;

        catalog.connection.execute(
            "DELETE FROM ops_assignment_submit_projection WHERE migration_id=?1",
            params![migration.as_bytes()],
        )?;
        insert_submit_projection_case(&catalog.connection, migration, workspace, 1, 0, None, None)?;
        let valid_empty = catalog.assignment_submit_projection(migration)?;
        assert_eq!(valid_empty.len(), 1);
        assert_eq!(valid_empty[0].source_count(), 0);
        assert_eq!(valid_empty[0].source_digests(), None);
        assert_eq!(valid_empty[0].source_kinds(), None);
        assert!(!valid_empty[0].source_refs_recorded());
        catalog.connection.execute(
            "UPDATE ops_assignment_submit_projection SET source_refs_recorded=1 WHERE migration_id=?1",
            params![migration.as_bytes()],
        )?;
        let known_empty = catalog.assignment_submit_projection(migration)?;
        assert!(known_empty[0].source_refs_recorded());

        catalog.connection.execute(
            "DELETE FROM ops_assignment_submit_projection WHERE migration_id=?1",
            params![migration.as_bytes()],
        )?;
        let max_kinds: Vec<u8> = (0..32).map(|index| index % 2).collect();
        insert_submit_projection_case(
            &catalog.connection,
            migration,
            workspace,
            1,
            32,
            Some(&[0xAA; 32]),
            Some(max_kinds.as_slice()),
        )?;
        let valid_max = catalog.assignment_submit_projection(migration)?;
        assert_eq!(valid_max[0].source_count(), 32);
        assert_eq!(valid_max[0].source_kinds(), Some(max_kinds.as_slice()));

        assert_corrupt_submit_projection(
            &catalog,
            migration,
            workspace,
            2,
            Some(&[0xAA; 32]),
            Some(&[0]),
        )?;
        assert_corrupt_submit_projection(
            &catalog,
            migration,
            workspace,
            2,
            Some(&[0xAA; 32]),
            Some(&[0; 33]),
        )?;
        assert_corrupt_submit_projection(
            &catalog,
            migration,
            workspace,
            33,
            Some(&[0xAA; 32]),
            Some(&[0; 33]),
        )?;
        assert_corrupt_submit_projection(
            &catalog,
            migration,
            workspace,
            0,
            Some(&[0xAA; 32]),
            None,
        )?;
        assert_corrupt_submit_projection(
            &catalog,
            migration,
            workspace,
            1,
            Some(&[0xAA; 32]),
            Some(&[2]),
        )?;
        assert_corrupt_submit_projection(
            &catalog,
            migration,
            workspace,
            1,
            Some(&[0xAA; 31]),
            Some(&[0]),
        )?;

        drop(catalog);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    fn subject_totals(snapshot: &WorkCreditScoreSnapshot) -> Vec<(Uuid, i64, i64)> {
        snapshot
            .per_subject()
            .iter()
            .filter_map(|score| {
                score
                    .subject()
                    .legacy_participant_id()
                    .map(|participant| (participant, score.total_credits(), score.entry_count()))
            })
            .collect()
    }

    #[test]
    fn score_window_mean_and_unattributed_math_is_exact() -> Result<(), Box<dyn std::error::Error>>
    {
        let (root, catalog, _migration, workspace) = catalog_fixture("scores")?;
        let now_ms = 10_000_000_000_i64;
        let participant_a = Uuid::from_u128(0xC0A1);
        let participant_b = Uuid::from_u128(0xC0B1);
        insert_member_and_binding(
            &catalog.connection,
            workspace,
            participant_a,
            0xC1A0,
            0xC1A5,
        )?;
        insert_member_and_binding(
            &catalog.connection,
            workspace,
            participant_b,
            0xC1B0,
            0xC1B5,
        )?;

        insert_credit(
            &catalog.connection,
            workspace,
            0xC201,
            Some(participant_a),
            0xC1A5,
            WorkCreditBudget::Three,
            now_ms - 1_000,
        )?;
        insert_credit(
            &catalog.connection,
            workspace,
            0xC202,
            Some(participant_b),
            0xC1B5,
            WorkCreditBudget::Five,
            now_ms - 2_000,
        )?;
        insert_credit(
            &catalog.connection,
            workspace,
            0xC203,
            Some(participant_b),
            0xC1B5,
            WorkCreditBudget::One,
            now_ms - 3_000,
        )?;
        // Unattributed: stored participant is NULL and no current binding exists.
        insert_credit(
            &catalog.connection,
            workspace,
            0xC204,
            None,
            0xC1FF,
            WorkCreditBudget::Three,
            now_ms - 4_000,
        )?;
        // Outside the window: must not contribute to any total.
        insert_credit(
            &catalog.connection,
            workspace,
            0xC205,
            Some(participant_a),
            0xC1A5,
            WorkCreditBudget::Five,
            now_ms - WORK_CREDIT_WINDOW_MS - 1,
        )?;

        let snapshot = catalog.work_credit_scores(workspace, now_ms)?;
        assert_eq!(snapshot.workspace_id(), workspace);
        assert_eq!(snapshot.window_ms(), WORK_CREDIT_WINDOW_MS);
        assert_eq!(snapshot.per_subject().len(), 2);
        let totals = subject_totals(&snapshot);
        assert!(
            totals.contains(&(participant_a, 3, 1)),
            "participant A total must be 3: {totals:?}"
        );
        assert!(
            totals.contains(&(participant_b, 6, 2)),
            "participant B total must be 6: {totals:?}"
        );
        assert_eq!(snapshot.total_credits(), 9);
        assert_eq!(snapshot.mean_subject_count(), 2);
        assert_eq!(snapshot.subject_arithmetic_mean(), 4);
        assert_eq!(snapshot.subject_mean_remainder_numerator(), 1);
        assert_eq!(snapshot.unattributed_count(), 1);

        // The exact window boundary is inclusive: an entry awarded exactly at
        // now_ms - window stays inside the snapshot.
        let boundary = catalog.work_credit_scores(workspace, now_ms - WORK_CREDIT_WINDOW_MS)?;
        assert_eq!(boundary.total_credits(), 14);

        // Newest-first bounded ledger read.
        let entries = catalog.work_credit_entries(workspace)?;
        assert_eq!(entries.len(), 5);
        let awarded: Vec<i64> = entries.iter().map(|entry| entry.awarded_at_ms()).collect();
        let mut sorted = awarded.clone();
        sorted.sort_unstable_by(|a, b| b.cmp(a));
        assert_eq!(awarded, sorted, "entries must read newest first");
        drop(catalog);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "One lifecycle fixture verifies durable credit through bootstrap, departure and retirement."
    )]
    fn team_scores_use_immutable_affinity_after_current_membership_leaves()
    -> Result<(), Box<dyn Error>> {
        let root =
            std::env::temp_dir().join(format!("orchestration-team-work-credit-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let destination = Uuid::from_u128(0xD001);
        let workspace = WorkspaceId::from_uuid(Uuid::from_u128(0xD002));
        let team = Uuid::from_u128(0xD003);
        let profile = Uuid::from_u128(0xD004);
        let participant = Uuid::from_u128(0xD005);
        let participation = WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xD006))?;
        let session = AgentSessionId::from_uuid(Uuid::from_u128(0xD007));
        let activity = ActivityId::from_uuid(Uuid::from_u128(0xD008));
        let agent = AgentInstanceId::from_uuid(Uuid::from_u128(0xD009));
        let assignment = AgentAssignmentId::from_uuid(Uuid::from_u128(0xD00A));
        let migration = Uuid::from_u128(0xD00B);
        let mut catalog =
            crate::OperationsCatalog::open(&root.join("operations.sqlite3"), destination)?;
        let peer_participation = WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xDF03))?;
        for (
            participant,
            profile,
            participation,
            session,
            agent,
            registration,
            mapping,
            profile_mapping,
        ) in [
            (
                participant,
                profile,
                participation,
                session,
                agent,
                Uuid::from_u128(0xD00C),
                Uuid::from_u128(0xD00D),
                Uuid::from_u128(0xD00E),
            ),
            (
                Uuid::from_u128(0xDF01),
                Uuid::from_u128(0xDF02),
                peer_participation,
                AgentSessionId::from_uuid(Uuid::from_u128(0xDF04)),
                AgentInstanceId::from_uuid(Uuid::from_u128(0xDF05)),
                Uuid::from_u128(0xDF06),
                Uuid::from_u128(0xDF07),
                Uuid::from_u128(0xDF08),
            ),
        ] {
            catalog.connection.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,'Team scorer','worker',1,1,1)",
            params![workspace.into_uuid().as_bytes(), participant.as_bytes(), session.into_uuid().as_bytes()],
        )?;
            catalog.connection.execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
            params![workspace.into_uuid().as_bytes(), participant.as_bytes(), activity.into_uuid().as_bytes(), session.into_uuid().as_bytes(), agent.into_uuid().as_bytes(), registration.as_bytes()],
        )?;
            catalog.connection.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,1,1)",
            params![workspace.into_uuid().as_bytes(), profile.as_bytes(), participation.into_uuid().as_bytes()],
        )?;
            catalog.connection.execute(
            "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,1)",
            params![mapping.as_bytes(), workspace.into_uuid().as_bytes(), participation.into_uuid().as_bytes(), profile.as_bytes(), profile_mapping.as_bytes(), session.into_uuid().as_bytes(), participant.as_bytes()],
        )?;
        }
        catalog.connection.execute(
            "INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_by_participation_id,created_by_profile_id,created_by_participant_id,created_by_session_id,created_at_ms,updated_at_ms,formed_at_ms) VALUES(?1,?2,'Team','scope','ACTIVE',1,'operator','Local operator',NULL,NULL,NULL,NULL,1,1,1)",
            params![team.as_bytes(), workspace.into_uuid().as_bytes()],
        )?;
        catalog.connection.execute(
            "INSERT INTO ops_team_members(workspace_id,team_id,participation_id,state,revision,joined_at_ms,left_at_ms,role) VALUES(?1,?2,?3,'JOINED',1,1,NULL,'implementation'),(?1,?2,?4,'JOINED',1,1,NULL,'verification')",
            params![workspace.into_uuid().as_bytes(), team.as_bytes(), participation.into_uuid().as_bytes(),peer_participation.into_uuid().as_bytes()],
        )?;
        let node_scope = crate::NodeTeamBindingScope::new(
            workspace,
            activity,
            forge_domain::GoalId::from_uuid(Uuid::from_u128(0xD00F)),
            forge_domain::TimestampMillis::new(50)?,
            forge_domain::DagRevisionNumber::INITIAL,
            forge_domain::GoalRevision::INITIAL,
            forge_domain::DagNodeId::from_uuid(Uuid::from_u128(0xD010)),
        )?;
        let _binding = catalog.set_node_team_binding(crate::SetNodeTeamBindingCommand {
            scope: node_scope,
            expected_revision: None,
            team_id: Some(team),
            actor: crate::OperationalActor::LocalOperator,
            expected_sender_leadership_revision: None,
            observed_at: forge_domain::TimestampMillis::new(80)?,
        })?;
        let affinity = crate::team_assignment::AssignmentAffinityCommand {
            workspace_id: workspace,
            assignment_id: assignment,
            activity_id: activity,
            target_agent_id: agent,
            target_session_id: session,
            requester: crate::OperationalActor::LocalOperator,
            team_id: Some(team),
            node_scope,
            expected_binding_revision: Some(1),
            assignment_payload_fingerprint: [0xD0; 32],
            original_offer_payload: b"fixture-offer".to_vec(),
            observed_at: forge_domain::TimestampMillis::new(100)?,
        };
        catalog.connection.execute(
            "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
            params![migration.as_bytes(), workspace.into_uuid().as_bytes(), destination.as_bytes(), [0u8;32].as_slice()],
        )?;
        let _receipt = catalog.record_assignment_affinity(&affinity)?;
        catalog.connection.execute(
            "INSERT INTO ops_effect_fact_consumers(migration_id,workspace_id,destination_id,last_sequence,applied_count) VALUES(?1,?2,?3,0,0)",
            params![migration.as_bytes(), workspace.into_uuid().as_bytes(), destination.as_bytes()],
        )?;
        catalog.connection.execute(
            "INSERT INTO ops_assignment_offer_projection(migration_id,source_sequence,fact_digest,assignment_id,workspace_id,activity_id,goal_id,node_id,target_agent_id,target_session_id,credit_budget,offered_at_ms) VALUES(?1,1,?2,?3,?4,?5,?6,?7,?8,?9,3,100)",
            params![migration.as_bytes(), [1u8;32].as_slice(), assignment.into_uuid().as_bytes(), workspace.into_uuid().as_bytes(), activity.into_uuid().as_bytes(), Uuid::from_u128(0xD00F).as_bytes(), Uuid::from_u128(0xD010).as_bytes(), agent.into_uuid().as_bytes(), session.into_uuid().as_bytes()],
        )?;
        let scope = crate::LeadershipScope::team(workspace, team);
        assert!(catalog.leadership_scope_view(scope, 100)?.is_none());
        {
            let tx = catalog.connection.transaction()?;
            crate::leadership::bootstrap_from_offer(
                &tx,
                crate::leadership::OfferBootstrapObservation {
                    workspace_id: workspace,
                    activity_id: activity,
                    assignment_id: assignment,
                    target_agent_id: agent,
                    offered_at_ms: 100,
                },
            )?;
            tx.commit()?;
        }
        let confirmed =
            catalog.confirm_pending_bootstrap(crate::ConfirmPendingBootstrapCommand {
                scope,
                expected_revision: 1,
                now_ms: 101,
            })?;
        assert_eq!(confirmed.leader_participant_id(), Some(participant));
        // Seed a historical row with this Team's exact work attribution. The
        // generic fixture derives different Activity/Goal/node IDs and cannot
        // stand in for this affinity. This bypasses only the new award gate.
        {
            let tx = catalog.connection.transaction()?;
            let observation = ReviewAwardObservation {
                workspace_id: workspace,
                assignment_id: assignment,
                activity_id: activity,
                goal_id: GoalId::from_uuid(Uuid::from_u128(0xD00F)),
                node_id: DagNodeId::from_uuid(Uuid::from_u128(0xD010)),
                target_agent_id: agent,
                reviewer_agent_id: agent,
                budget: Some(WorkCreditBudget::Three),
                award_source_sequence: 1,
                awarded_at_ms: 100,
            };
            record_credit(
                &tx,
                credit_row(
                    observation,
                    WorkCreditBudget::Three,
                    ExecutionId::from_uuid(Uuid::from_u128(0xD012)),
                ),
                None,
            )?;
            tx.commit()?;
        }
        let ledger_before = catalog.work_credit_entries(workspace)?;
        let _left = catalog.leave_team(crate::TeamLeaveCommand {
            request_id: Uuid::from_u128(0xD016),
            workspace_id: workspace,
            team_id: team,
            expected_team_revision: 1,
            actor: crate::OperationalActor::Participant {
                activity_id: activity,
                agent_id: agent,
                session_id: session,
            },
            observed_at: forge_domain::TimestampMillis::new(200)?,
        })?;
        let departed = catalog
            .leadership_scope_view(scope, 200)?
            .ok_or("missing Team leader history")?;
        assert_eq!(departed.state(), crate::LeadershipState::FailoverRequired);
        assert_eq!(departed.leader_participant_id(), None);
        let _retired = catalog.retire_team(crate::TeamRetireCommand {
            request_id: Uuid::from_u128(0xD017),
            workspace_id: workspace,
            team_id: team,
            expected_team_revision: 2,
            actor: crate::OperationalActor::LocalOperator,
            observed_at: forge_domain::TimestampMillis::new(201)?,
        })?;
        assert_eq!(
            catalog
                .leadership_scope_view(scope, 201)?
                .ok_or("missing retired Team history")?
                .state(),
            crate::LeadershipState::Vacant
        );
        let snapshot = team_work_credit_scores_at(&catalog.connection, workspace, team, 100)?;
        assert_eq!(snapshot.team_id(), Some(team));
        assert_eq!(snapshot.total_credits(), 3);
        assert_eq!(snapshot.per_subject().len(), 1);
        assert_eq!(snapshot.per_subject()[0].total_credits(), 3);

        // This models a legacy self-reviewed credit already present in the
        // append-only ledger. Keep both source and ledger rows, but exclude the
        // matched review from every score and fulfillment projection.
        insert_review_projection(
            &catalog.connection,
            migration,
            1,
            workspace,
            0xD00A,
            "Accepted",
            Some(WorkCreditBudget::Three),
        )?;
        catalog.connection.execute(
            "UPDATE ops_assignment_review_projection SET activity_id=?1,goal_id=?2,node_id=?3,target_agent_id=?4,target_session_id=?5,reviewer_agent_id=?4 WHERE migration_id=?6 AND source_sequence=1",
            params![
                activity.into_uuid().as_bytes(),
                Uuid::from_u128(0xD00F).as_bytes(),
                Uuid::from_u128(0xD010).as_bytes(),
                agent.into_uuid().as_bytes(),
                session.into_uuid().as_bytes(),
                migration.as_bytes(),
            ],
        )?;
        assert_eq!(catalog.work_credit_entries(workspace)?.len(), 1);
        let reviews_before = catalog.assignment_review_projection(migration)?;
        assert_eq!(catalog.work_credit_fulfillment(workspace)?, (0, 0));
        let workspace_snapshot = catalog.work_credit_scores(workspace, 100)?;
        assert_eq!(workspace_snapshot.total_credits(), 0);
        assert!(workspace_snapshot.per_subject().is_empty());
        assert_eq!(workspace_snapshot.fulfillment_completed(), 0);
        assert_eq!(workspace_snapshot.fulfillment_evaluated(), 0);
        assert!(workspace_snapshot.fulfillment_per_subject().is_empty());
        let team_snapshot = team_work_credit_scores_at(&catalog.connection, workspace, team, 100)?;
        assert_eq!(team_snapshot.total_credits(), 0);
        assert!(team_snapshot.per_subject().is_empty());
        assert_eq!(team_snapshot.fulfillment_completed(), 0);
        assert_eq!(team_snapshot.fulfillment_evaluated(), 0);
        assert!(team_snapshot.fulfillment_per_subject().is_empty());
        assert_eq!(catalog.work_credit_entries(workspace)?, ledger_before);
        assert_eq!(
            catalog.assignment_review_projection(migration)?,
            reviews_before
        );

        let foreign_workspace = WorkspaceId::from_uuid(Uuid::from_u128(0xD013));
        let foreign_team = Uuid::from_u128(0xD014);
        catalog.connection.execute(
            "INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_by_participation_id,created_by_profile_id,created_by_participant_id,created_by_session_id,created_at_ms,updated_at_ms) VALUES(?1,?2,'Foreign','scope','ACTIVE',1,'operator','Local operator',NULL,NULL,NULL,NULL,1,1)",
            params![foreign_team.as_bytes(), foreign_workspace.into_uuid().as_bytes()],
        )?;
        assert!(
            team_work_credit_scores_at(&catalog.connection, workspace, foreign_team, 100).is_err()
        );
        assert!(
            team_work_credit_scores_at(
                &catalog.connection,
                workspace,
                Uuid::from_u128(0xD015),
                100
            )
            .is_err()
        );
        drop(catalog);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn bounded_score_read_matches_existing_full_snapshot_within_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        let (root, catalog, _migration, workspace) = catalog_fixture("bounded-small")?;
        let participant = Uuid::from_u128(0xF100);
        insert_credit(
            &catalog.connection,
            workspace,
            0xF101,
            Some(participant),
            0xF102,
            WorkCreditBudget::Three,
            101,
        )?;

        let legacy = catalog.work_credit_scores(workspace, 1_000)?;
        let bounded = catalog.work_credit_scores_bounded(workspace, 1_000)?;
        assert_eq!(bounded, legacy);
        assert_eq!(bounded.total_credits(), 3);
        assert_eq!(bounded.per_subject().len(), 1);
        assert_eq!(bounded.per_subject()[0].entry_count(), 1);

        drop(catalog);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn bounded_score_read_rejects_score_group_limit_plus_one_without_mutating_ledger()
    -> Result<(), Box<dyn std::error::Error>> {
        let (root, catalog, _migration, workspace) = catalog_fixture("bounded-score-limit")?;
        let base = 0x1_0000_0000_u128;
        for index in 0..=WORK_CREDIT_SCORE_SOURCE_READ_LIMIT {
            let offset = u128::try_from(index)?;
            insert_credit(
                &catalog.connection,
                workspace,
                base + offset,
                Some(Uuid::from_u128(base + 0x1_000 + offset)),
                base + 0x2_000 + offset,
                WorkCreditBudget::One,
                100,
            )?;
        }

        let ledger_before = work_credit_ledger_count(&catalog.connection, workspace)?;
        assert_eq!(ledger_before, 257);
        assert!(matches!(
            catalog.work_credit_scores_bounded(workspace, 1_000),
            Err(OperationsError::ResyncRequired(
                WORK_CREDIT_READ_BUDGET_REJECTION
            ))
        ));
        assert_eq!(
            work_credit_ledger_count(&catalog.connection, workspace)?,
            ledger_before
        );

        drop(catalog);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn bounded_score_read_rejects_fulfillment_source_limit_plus_one_without_mutating_ledger()
    -> Result<(), Box<dyn std::error::Error>> {
        let (root, catalog, migration, workspace) = catalog_fixture("bounded-fulfillment-limit")?;
        let destination = catalog.destination_id();
        catalog.connection.execute(
            "INSERT INTO ops_effect_fact_consumers(migration_id,workspace_id,destination_id,last_sequence,applied_count) VALUES(?1,?2,?3,0,0)",
            params![
                migration.as_bytes(),
                workspace.into_uuid().as_bytes(),
                destination.as_bytes(),
            ],
        )?;
        let base = 0x2_0000_0000_u128;
        for index in 0..=WORK_CREDIT_SCORE_SOURCE_READ_LIMIT {
            let offset = u128::try_from(index)?;
            let assignment = base + offset;
            let participant = Uuid::from_u128(base + 0x1_000 + offset);
            let session = assignment + 1 - 0x10;
            insert_member_and_binding(
                &catalog.connection,
                workspace,
                participant,
                session,
                assignment + 4,
            )?;
            insert_review_projection(
                &catalog.connection,
                migration,
                i64::try_from(index + 1)?,
                workspace,
                assignment,
                "Accepted",
                None,
            )?;
        }

        let ledger_before = work_credit_ledger_count(&catalog.connection, workspace)?;
        assert_eq!(ledger_before, 0);
        assert!(matches!(
            catalog.work_credit_scores_bounded(workspace, 1_000),
            Err(OperationsError::ResyncRequired(
                WORK_CREDIT_READ_BUDGET_REJECTION
            ))
        ));
        assert_eq!(
            work_credit_ledger_count(&catalog.connection, workspace)?,
            ledger_before
        );

        drop(catalog);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    fn work_credit_ledger_count(
        connection: &Connection,
        workspace: WorkspaceId,
    ) -> Result<i64, rusqlite::Error> {
        connection.query_row(
            "SELECT COUNT(*) FROM ops_work_credit_entries WHERE workspace_id=?1",
            params![workspace.into_uuid().as_bytes()],
            |row| row.get(0),
        )
    }
}
