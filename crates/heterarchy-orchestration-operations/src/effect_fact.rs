//! Durable consumer cursor and read-only projection for Forge-owned effect facts.
mod input_validation;

use crate::{OperationsError, catalog, wire, work_credit};
use forge_domain::{
    ActivityId, AgentAssignmentId, AgentInstanceId, AgentSessionId, ChangeSetId, ChangeSetState,
    DagNodeId, DiffEvidenceState, DiffId, ExecutionId, ExecutionState, FileMutationFailureCode,
    GoalId, RuntimeInstanceId, WorkCreditBudget, WorkspaceId,
};
use forge_persistence::{
    AssignmentReviewedOutcome, AssignmentSubmittedSourceRef, EffectFact, EffectFactOutboxEntry,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const MAX_PAGE_SIZE: usize = 64;
const CONSUMERS_SQL: &str = "CREATE TABLE ops_effect_fact_consumers (migration_id BLOB PRIMARY KEY NOT NULL REFERENCES ops_imports(migration_id) CHECK(length(migration_id)=16), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), destination_id BLOB NOT NULL REFERENCES ops_catalog_identity(destination_id) CHECK(length(destination_id)=16), last_sequence INTEGER NOT NULL CHECK(last_sequence>=0), applied_count INTEGER NOT NULL CHECK(applied_count>=0))";
const EXECUTIONS_SQL: &str = "CREATE TABLE ops_execution_terminal_projection (migration_id BLOB NOT NULL REFERENCES ops_effect_fact_consumers(migration_id) CHECK(length(migration_id)=16), source_sequence INTEGER NOT NULL CHECK(source_sequence>=1), fact_digest BLOB NOT NULL CHECK(length(fact_digest)=32), execution_id BLOB NOT NULL CHECK(length(execution_id)=16), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), activity_id BLOB NOT NULL CHECK(length(activity_id)=16), execution_revision INTEGER NOT NULL CHECK(execution_revision>=1), execution_state INTEGER NOT NULL CHECK(execution_state IN(2,3,4,5,6)), observed_at_ms INTEGER NOT NULL CHECK(observed_at_ms>=0), agent_id BLOB NOT NULL CHECK(length(agent_id)=16), agent_revision INTEGER NOT NULL CHECK(agent_revision>=1), session_id BLOB CHECK(session_id IS NULL OR length(session_id)=16), goal_id BLOB NOT NULL CHECK(length(goal_id)=16), goal_revision INTEGER NOT NULL CHECK(goal_revision>=1), dag_revision INTEGER NOT NULL CHECK(dag_revision>=1), node_id BLOB NOT NULL CHECK(length(node_id)=16), node_revision INTEGER NOT NULL CHECK(node_revision>=1), verification_input_id BLOB CHECK(verification_input_id IS NULL OR length(verification_input_id)=16), verification_input_fingerprint BLOB CHECK(verification_input_fingerprint IS NULL OR length(verification_input_fingerprint)=32), verification_disposition INTEGER CHECK(verification_disposition IS NULL OR verification_disposition BETWEEN 0 AND 3), verification_output_integrity INTEGER CHECK(verification_output_integrity IS NULL OR verification_output_integrity BETWEEN 0 AND 3), verification_output_fingerprint BLOB CHECK(verification_output_fingerprint IS NULL OR length(verification_output_fingerprint)=32), PRIMARY KEY(migration_id,source_sequence), UNIQUE(migration_id,execution_id), CHECK((verification_input_id IS NULL AND verification_input_fingerprint IS NULL AND verification_disposition IS NULL AND verification_output_integrity IS NULL AND verification_output_fingerprint IS NULL) OR (verification_input_id IS NOT NULL AND verification_input_fingerprint IS NOT NULL AND verification_disposition IS NOT NULL AND ((verification_output_integrity IS NULL AND verification_output_fingerprint IS NULL) OR (verification_output_integrity IS NOT NULL AND verification_output_fingerprint IS NOT NULL)))))";
const CHANGE_SETS_SQL: &str = "CREATE TABLE ops_change_set_projection (migration_id BLOB NOT NULL REFERENCES ops_effect_fact_consumers(migration_id) CHECK(length(migration_id)=16), source_sequence INTEGER NOT NULL CHECK(source_sequence>=1), fact_digest BLOB NOT NULL CHECK(length(fact_digest)=32), change_set_id BLOB NOT NULL CHECK(length(change_set_id)=16), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), activity_id BLOB NOT NULL CHECK(length(activity_id)=16), change_set_revision INTEGER NOT NULL CHECK(change_set_revision>=1), state TEXT NOT NULL CHECK(state IN('Applied','Failed')), failure_code TEXT CHECK(failure_code IS NULL OR failure_code IN('InvalidPath','ProtectedPath','PathConflict','StaleWrite','CapacityExceeded','IoFailure')), diff_id BLOB NOT NULL CHECK(length(diff_id)=16), diff_state TEXT NOT NULL CHECK(diff_state IN('Ready','SummaryOnly','Abandoned')), diff_revision INTEGER NOT NULL CHECK(diff_revision>=1), observed_at_ms INTEGER NOT NULL CHECK(observed_at_ms>=0), producer_agent_id BLOB CHECK(producer_agent_id IS NULL OR length(producer_agent_id)=16), producer_session_id BLOB CHECK(producer_session_id IS NULL OR length(producer_session_id)=16), digest_version INTEGER NOT NULL DEFAULT 0 CHECK(digest_version IN(0,2)), PRIMARY KEY(migration_id,source_sequence), UNIQUE(migration_id,change_set_id), CHECK((state='Applied' AND failure_code IS NULL) OR (state='Failed' AND failure_code IS NOT NULL)))";
/// Additive `v13` to `v14` producer-attribution columns. `SQLite` rewrites the
/// stored schema text by appending these column definitions in order, which is
/// exactly their position in [`CHANGE_SETS_SQL`], so migrated and freshly
/// created tables share one normalized shape. The columns stay nullable so
/// the additive `ALTER` can run over legacy rows; a `NULL` producer means the
/// source fact predates attribution and is excluded from provenance joins.
const CHANGE_SET_PRODUCER_AGENT_COLUMN_SQL: &str =
    "producer_agent_id BLOB CHECK(producer_agent_id IS NULL OR length(producer_agent_id)=16)";
const CHANGE_SET_PRODUCER_SESSION_COLUMN_SQL: &str =
    "producer_session_id BLOB CHECK(producer_session_id IS NULL OR length(producer_session_id)=16)";
const CHANGE_SET_DIGEST_VERSION_COLUMN_SQL: &str =
    "digest_version INTEGER NOT NULL DEFAULT 0 CHECK(digest_version IN(0,2))";

/// Exact `v13` shape of the change-set projection, derived from the current
/// canonical definition so there is only one hand-written authority for the
/// table contract.
pub(crate) fn change_sets_sql_v13() -> String {
    change_sets_sql_v17()
        .replace(&format!(", {CHANGE_SET_PRODUCER_AGENT_COLUMN_SQL}"), "")
        .replace(&format!(", {CHANGE_SET_PRODUCER_SESSION_COLUMN_SQL}"), "")
}

pub(crate) fn change_sets_sql_v17() -> String {
    CHANGE_SETS_SQL.replace(&format!(", {CHANGE_SET_DIGEST_VERSION_COLUMN_SQL}"), "")
}

/// One additive `v13` -> `v14` in-place upgrade of an existing change-set
/// projection. No durable row is rewritten or fabricated.
pub(crate) fn add_change_set_producer_columns(
    connection: &Connection,
) -> Result<(), OperationsError> {
    connection.execute_batch(
        "ALTER TABLE ops_change_set_projection ADD COLUMN producer_agent_id BLOB CHECK(producer_agent_id IS NULL OR length(producer_agent_id)=16)",
    )?;
    connection.execute_batch(
        "ALTER TABLE ops_change_set_projection ADD COLUMN producer_session_id BLOB CHECK(producer_session_id IS NULL OR length(producer_session_id)=16)",
    )?;
    Ok(())
}

pub(crate) fn add_change_set_digest_version_column(
    connection: &Connection,
) -> Result<(), OperationsError> {
    connection.execute_batch(
        "ALTER TABLE ops_change_set_projection ADD COLUMN digest_version INTEGER NOT NULL DEFAULT 0 CHECK(digest_version IN(0,2))",
    )?;
    Ok(())
}
const ASSIGNMENT_REVIEWS_SQL: &str = "CREATE TABLE ops_assignment_review_projection (migration_id BLOB NOT NULL REFERENCES ops_effect_fact_consumers(migration_id) CHECK(length(migration_id)=16), source_sequence INTEGER NOT NULL CHECK(source_sequence>=1), fact_digest BLOB NOT NULL CHECK(length(fact_digest)=32), assignment_id BLOB NOT NULL CHECK(length(assignment_id)=16), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), activity_id BLOB NOT NULL CHECK(length(activity_id)=16), goal_id BLOB NOT NULL CHECK(length(goal_id)=16), node_id BLOB NOT NULL CHECK(length(node_id)=16), target_agent_id BLOB NOT NULL CHECK(length(target_agent_id)=16), target_session_id BLOB CHECK(target_session_id IS NULL OR length(target_session_id)=16), outcome TEXT NOT NULL CHECK(outcome IN('Accepted','Rejected')), credit_budget INTEGER CHECK(credit_budget IS NULL OR credit_budget IN(1,3,5)), reviewer_agent_id BLOB NOT NULL CHECK(length(reviewer_agent_id)=16), result_digest BLOB CHECK(result_digest IS NULL OR length(result_digest)=32), observed_at_ms INTEGER NOT NULL CHECK(observed_at_ms>=0), PRIMARY KEY(migration_id,source_sequence), UNIQUE(migration_id,assignment_id))";
const ASSIGNMENT_OFFERS_SQL: &str = "CREATE TABLE ops_assignment_offer_projection (migration_id BLOB NOT NULL REFERENCES ops_effect_fact_consumers(migration_id) CHECK(length(migration_id)=16), source_sequence INTEGER NOT NULL CHECK(source_sequence>=1), fact_digest BLOB NOT NULL CHECK(length(fact_digest)=32), assignment_id BLOB NOT NULL CHECK(length(assignment_id)=16), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), activity_id BLOB NOT NULL CHECK(length(activity_id)=16), goal_id BLOB NOT NULL CHECK(length(goal_id)=16), node_id BLOB NOT NULL CHECK(length(node_id)=16), target_agent_id BLOB NOT NULL CHECK(length(target_agent_id)=16), target_session_id BLOB CHECK(target_session_id IS NULL OR length(target_session_id)=16), credit_budget INTEGER CHECK(credit_budget IS NULL OR credit_budget IN(1,3,5)), offered_at_ms INTEGER NOT NULL CHECK(offered_at_ms>=0), PRIMARY KEY(migration_id,source_sequence), UNIQUE(migration_id,assignment_id))";

const SOURCE_REFS_RECORDED_COLUMN_SQL: &str =
    "source_refs_recorded INTEGER NOT NULL DEFAULT 0 CHECK(source_refs_recorded IN(0,1))";
const ASSIGNMENT_SUBMITS_SQL: &str = "CREATE TABLE ops_assignment_submit_projection (migration_id BLOB NOT NULL REFERENCES ops_effect_fact_consumers(migration_id) CHECK(length(migration_id)=16), source_sequence INTEGER NOT NULL CHECK(source_sequence>=1), fact_digest BLOB NOT NULL CHECK(length(fact_digest)=32), assignment_id BLOB NOT NULL CHECK(length(assignment_id)=16), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), activity_id BLOB NOT NULL CHECK(length(activity_id)=16), goal_id BLOB NOT NULL CHECK(length(goal_id)=16), node_id BLOB NOT NULL CHECK(length(node_id)=16), target_agent_id BLOB NOT NULL CHECK(length(target_agent_id)=16), target_session_id BLOB CHECK(target_session_id IS NULL OR length(target_session_id)=16), source_count INTEGER NOT NULL CHECK(source_count>=0), source_digests BLOB CHECK(source_digests IS NULL OR length(source_digests)=32), source_kinds BLOB, result_digest BLOB CHECK(result_digest IS NULL OR length(result_digest)=32), submitted_at_ms INTEGER NOT NULL CHECK(submitted_at_ms>=0), source_refs_recorded INTEGER NOT NULL DEFAULT 0 CHECK(source_refs_recorded IN(0,1)), PRIMARY KEY(migration_id,source_sequence), UNIQUE(migration_id,assignment_id))";
const ASSIGNMENT_SUBMIT_SOURCE_REFS_SQL: &str = "CREATE TABLE ops_assignment_submit_source_refs (migration_id BLOB NOT NULL REFERENCES ops_effect_fact_consumers(migration_id) CHECK(length(migration_id)=16), source_sequence INTEGER NOT NULL CHECK(source_sequence>=1), ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 31), evidence_kind INTEGER NOT NULL CHECK(evidence_kind BETWEEN 0 AND 3), evidence_id BLOB NOT NULL CHECK(length(evidence_id)=16), PRIMARY KEY(migration_id,source_sequence,ordinal), FOREIGN KEY(migration_id,source_sequence) REFERENCES ops_assignment_submit_projection(migration_id,source_sequence))";

pub(crate) fn assignment_submits_sql_v16() -> String {
    ASSIGNMENT_SUBMITS_SQL.replace(&format!(", {SOURCE_REFS_RECORDED_COLUMN_SQL}"), "")
}

#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffectFactConsumerProgress {
    migration_id: Uuid,
    workspace_id: WorkspaceId,
    destination_id: Uuid,
    last_sequence: u64,
    applied_count: u64,
}
impl EffectFactConsumerProgress {
    #[must_use]
    pub const fn migration_id(self) -> Uuid {
        self.migration_id
    }
    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        self.workspace_id
    }
    #[must_use]
    pub const fn destination_id(self) -> Uuid {
        self.destination_id
    }
    #[must_use]
    pub const fn last_sequence(self) -> u64 {
        self.last_sequence
    }
    #[must_use]
    pub const fn applied_count(self) -> u64 {
        self.applied_count
    }
}

/// One durable execution-terminal projection row read back from the catalog.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionTerminalProjectionRow {
    migration_id: Uuid,
    source_sequence: u64,
    fact_digest: [u8; 32],
    execution_id: ExecutionId,
    execution_state: ExecutionState,
    observed_at_ms: i64,
}
impl ExecutionTerminalProjectionRow {
    #[must_use]
    pub const fn migration_id(self) -> Uuid {
        self.migration_id
    }
    #[must_use]
    pub const fn source_sequence(self) -> u64 {
        self.source_sequence
    }
    #[must_use]
    pub const fn fact_digest(self) -> [u8; 32] {
        self.fact_digest
    }
    #[must_use]
    pub const fn execution_id(self) -> ExecutionId {
        self.execution_id
    }
    #[must_use]
    pub const fn execution_state(self) -> ExecutionState {
        self.execution_state
    }
    #[must_use]
    pub const fn observed_at_ms(self) -> i64 {
        self.observed_at_ms
    }
}

/// One durable `ChangeSet` terminal projection row read back from the catalog.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangeSetProjectionRow {
    migration_id: Uuid,
    source_sequence: u64,
    fact_digest: [u8; 32],
    change_set_id: ChangeSetId,
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    change_set_revision: i64,
    state: ChangeSetState,
    failure_code: Option<FileMutationFailureCode>,
    diff_id: DiffId,
    diff_state: DiffEvidenceState,
    diff_revision: i64,
    observed_at_ms: i64,
    producer_agent_id: Option<AgentInstanceId>,
    producer_session_id: Option<AgentSessionId>,
}
impl ChangeSetProjectionRow {
    #[must_use]
    pub const fn migration_id(self) -> Uuid {
        self.migration_id
    }
    #[must_use]
    pub const fn source_sequence(self) -> u64 {
        self.source_sequence
    }
    #[must_use]
    pub const fn fact_digest(self) -> [u8; 32] {
        self.fact_digest
    }
    #[must_use]
    pub const fn change_set_id(self) -> ChangeSetId {
        self.change_set_id
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
    pub const fn change_set_revision(self) -> i64 {
        self.change_set_revision
    }
    #[must_use]
    pub const fn state(self) -> ChangeSetState {
        self.state
    }
    #[must_use]
    pub const fn failure_code(self) -> Option<FileMutationFailureCode> {
        self.failure_code
    }
    #[must_use]
    pub const fn diff_id(self) -> DiffId {
        self.diff_id
    }
    #[must_use]
    pub const fn diff_state(self) -> DiffEvidenceState {
        self.diff_state
    }
    #[must_use]
    pub const fn diff_revision(self) -> i64 {
        self.diff_revision
    }
    #[must_use]
    pub const fn observed_at_ms(self) -> i64 {
        self.observed_at_ms
    }
    /// The exact producer attribution carried by the source kind-1 fact.
    /// `None` means the row was consumed before the v47/v14 attribution
    /// contract existed and it must be excluded from provenance joins.
    #[must_use]
    pub const fn producer_agent_id(self) -> Option<AgentInstanceId> {
        self.producer_agent_id
    }
    #[must_use]
    pub const fn producer_session_id(self) -> Option<AgentSessionId> {
        self.producer_session_id
    }
}

/// One durable assignment-review projection row read back from the catalog.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssignmentReviewProjectionRow {
    migration_id: Uuid,
    source_sequence: u64,
    fact_digest: [u8; 32],
    assignment_id: AgentAssignmentId,
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    goal_id: GoalId,
    node_id: DagNodeId,
    target_agent_id: AgentInstanceId,
    target_session_id: Option<AgentSessionId>,
    outcome: AssignmentReviewedOutcome,
    credit_budget: Option<WorkCreditBudget>,
    reviewer_agent_id: AgentInstanceId,
    result_digest: Option<[u8; 32]>,
    observed_at_ms: i64,
}
impl AssignmentReviewProjectionRow {
    #[must_use]
    pub const fn migration_id(self) -> Uuid {
        self.migration_id
    }
    #[must_use]
    pub const fn source_sequence(self) -> u64 {
        self.source_sequence
    }
    #[must_use]
    pub const fn fact_digest(self) -> [u8; 32] {
        self.fact_digest
    }
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
    pub const fn target_session_id(self) -> Option<AgentSessionId> {
        self.target_session_id
    }
    pub const fn outcome(self) -> AssignmentReviewedOutcome {
        self.outcome
    }
    #[must_use]
    pub const fn credit_budget(self) -> Option<WorkCreditBudget> {
        self.credit_budget
    }
    #[must_use]
    pub const fn reviewer_agent_id(self) -> AgentInstanceId {
        self.reviewer_agent_id
    }
    #[must_use]
    pub const fn result_digest(self) -> Option<[u8; 32]> {
        self.result_digest
    }
    #[must_use]
    pub const fn observed_at_ms(self) -> i64 {
        self.observed_at_ms
    }
}

/// Durable assignment-submission projection row read back from the catalog.
#[must_use]
pub struct AssignmentSubmitProjectionRow {
    migration_id: Uuid,
    source_sequence: u64,
    fact_digest: [u8; 32],
    assignment_id: AgentAssignmentId,
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    goal_id: GoalId,
    node_id: DagNodeId,
    target_agent_id: AgentInstanceId,
    target_session_id: Option<AgentSessionId>,
    source_count: u8,
    source_digests: Option<[u8; 32]>,
    source_kinds: Option<[u8; 32]>,
    source_refs_recorded: bool,
    result_digest: Option<[u8; 32]>,
    submitted_at_ms: i64,
}

impl AssignmentSubmitProjectionRow {
    #[must_use]
    pub const fn migration_id(&self) -> Uuid {
        self.migration_id
    }

    #[must_use]
    pub const fn source_sequence(&self) -> u64 {
        self.source_sequence
    }

    #[must_use]
    pub const fn fact_digest(&self) -> [u8; 32] {
        self.fact_digest
    }

    #[must_use]
    pub const fn assignment_id(&self) -> AgentAssignmentId {
        self.assignment_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity_id
    }

    #[must_use]
    pub const fn goal_id(&self) -> GoalId {
        self.goal_id
    }

    #[must_use]
    pub const fn node_id(&self) -> DagNodeId {
        self.node_id
    }

    #[must_use]
    pub const fn target_agent_id(&self) -> AgentInstanceId {
        self.target_agent_id
    }

    #[must_use]
    pub const fn target_session_id(&self) -> Option<AgentSessionId> {
        self.target_session_id
    }

    #[must_use]
    pub const fn source_count(&self) -> u8 {
        self.source_count
    }

    #[must_use]
    pub const fn source_digests(&self) -> Option<[u8; 32]> {
        self.source_digests
    }

    #[must_use]
    pub fn source_kinds(&self) -> Option<&[u8]> {
        self.source_kinds
            .as_ref()
            .map(|kinds| &kinds[..self.source_count as usize])
    }

    /// Whether the source-reference child projection was known and consumed.
    /// `false` preserves legacy/unavailable `None`; `true` may still mean an
    /// explicitly known empty source-reference list.
    #[must_use]
    pub const fn source_refs_recorded(&self) -> bool {
        self.source_refs_recorded
    }

    #[must_use]
    pub const fn result_digest(&self) -> Option<[u8; 32]> {
        self.result_digest
    }

    #[must_use]
    pub const fn submitted_at_ms(&self) -> i64 {
        self.submitted_at_ms
    }
}

/// One durable assignment-offer projection row read back from the catalog.
/// This is the first-work bootstrap identity Orchestration leads from.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssignmentOfferProjectionRow {
    migration_id: Uuid,
    source_sequence: u64,
    fact_digest: [u8; 32],
    assignment_id: AgentAssignmentId,
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    goal_id: GoalId,
    node_id: DagNodeId,
    target_agent_id: AgentInstanceId,
    target_session_id: Option<AgentSessionId>,
    credit_budget: Option<WorkCreditBudget>,
    offered_at_ms: i64,
}
impl AssignmentOfferProjectionRow {
    #[must_use]
    pub const fn migration_id(self) -> Uuid {
        self.migration_id
    }
    #[must_use]
    pub const fn source_sequence(self) -> u64 {
        self.source_sequence
    }
    #[must_use]
    pub const fn fact_digest(self) -> [u8; 32] {
        self.fact_digest
    }
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
    pub const fn target_session_id(self) -> Option<AgentSessionId> {
        self.target_session_id
    }
    #[must_use]
    pub const fn credit_budget(self) -> Option<WorkCreditBudget> {
        self.credit_budget
    }
    #[must_use]
    pub const fn offered_at_ms(self) -> i64 {
        self.offered_at_ms
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Raw effect candidates, validated by the catalog before any projection write.
pub enum TerminalFact<'a> {
    Execution(ExecutionTerminalFact),
    ChangeSet(ChangeSetTerminalFact),
    AssignmentReviewed(AssignmentReviewedFact),
    AssignmentOffered(AssignmentOfferedFact),
    AssignmentSubmitted(AssignmentSubmittedFact<'a>),
}
/// Transport-neutral application input; this value alone is not source authority.
pub type EffectFactInput<'a> = TerminalFact<'a>;

impl TerminalFact<'_> {
    const fn sequence(self) -> u64 {
        match self {
            Self::Execution(fact) => fact.sequence,
            Self::ChangeSet(fact) => fact.sequence,
            Self::AssignmentReviewed(fact) => fact.sequence,
            Self::AssignmentOffered(fact) => fact.sequence,
            Self::AssignmentSubmitted(fact) => fact.sequence,
        }
    }
    const fn migration_id(self) -> Uuid {
        match self {
            Self::Execution(fact) => fact.migration_id,
            Self::ChangeSet(fact) => fact.migration_id,
            Self::AssignmentReviewed(fact) => fact.migration_id,
            Self::AssignmentOffered(fact) => fact.migration_id,
            Self::AssignmentSubmitted(fact) => fact.migration_id,
        }
    }
    const fn destination_id(self) -> Uuid {
        match self {
            Self::Execution(fact) => fact.destination_id,
            Self::ChangeSet(fact) => fact.destination_id,
            Self::AssignmentReviewed(fact) => fact.destination_id,
            Self::AssignmentOffered(fact) => fact.destination_id,
            Self::AssignmentSubmitted(fact) => fact.destination_id,
        }
    }
    const fn workspace_id(self) -> WorkspaceId {
        match self {
            Self::Execution(fact) => fact.workspace_id,
            Self::ChangeSet(fact) => fact.workspace_id,
            Self::AssignmentReviewed(fact) => fact.workspace_id,
            Self::AssignmentOffered(fact) => fact.workspace_id,
            Self::AssignmentSubmitted(fact) => fact.workspace_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionTerminalFact {
    pub sequence: u64,
    pub migration_id: Uuid,
    pub destination_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub execution_id: ExecutionId,
    pub activity_id: ActivityId,
    pub execution_revision: i64,
    pub execution_state: ExecutionState,
    pub observed_at_ms: i64,
    pub agent_id: AgentInstanceId,
    pub agent_revision: i64,
    pub session_id: Option<AgentSessionId>,
    pub goal_id: GoalId,
    pub goal_revision: i64,
    pub dag_revision: i64,
    pub node_id: DagNodeId,
    pub node_revision: i64,
    pub verification_input_id: Option<Uuid>,
    pub verification_input_fingerprint: Option<[u8; 32]>,
    pub verification_disposition: Option<i64>,
    pub verification_output_integrity: Option<i64>,
    pub verification_output_fingerprint: Option<[u8; 32]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangeSetTerminalFact {
    pub sequence: u64,
    pub migration_id: Uuid,
    pub destination_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub change_set_id: ChangeSetId,
    pub workspace_revision: i64,
    pub activity_id: ActivityId,
    pub runtime_instance_id: RuntimeInstanceId,
    pub activity_lease_revision: i64,
    pub change_set_revision: i64,
    pub state: ChangeSetState,
    pub failure_code: Option<FileMutationFailureCode>,
    pub diff_id: DiffId,
    pub diff_state: DiffEvidenceState,
    pub diff_revision: i64,
    pub observed_at_ms: i64,
    pub producer_agent_id: Option<AgentInstanceId>,
    pub producer_session_id: Option<AgentSessionId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssignmentReviewedFact {
    pub sequence: u64,
    pub migration_id: Uuid,
    pub destination_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub assignment_id: AgentAssignmentId,
    pub activity_id: ActivityId,
    pub goal_id: GoalId,
    pub node_id: DagNodeId,
    pub target_agent_id: AgentInstanceId,
    pub target_session_id: Option<AgentSessionId>,
    pub outcome: AssignmentReviewedOutcome,
    pub credit_budget: Option<WorkCreditBudget>,
    pub reviewer_agent_id: AgentInstanceId,
    pub reviewer_session_id: Option<AgentSessionId>,
    pub result_digest: Option<[u8; 32]>,
    pub observed_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssignmentOfferedFact {
    pub sequence: u64,
    pub migration_id: Uuid,
    pub destination_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub assignment_id: AgentAssignmentId,
    pub activity_id: ActivityId,
    pub goal_id: GoalId,
    pub node_id: DagNodeId,
    pub target_agent_id: AgentInstanceId,
    pub target_session_id: Option<AgentSessionId>,
    pub credit_budget: Option<WorkCreditBudget>,
    pub offered_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssignmentSubmittedFact<'a> {
    pub sequence: u64,
    pub migration_id: Uuid,
    pub destination_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub assignment_id: AgentAssignmentId,
    pub activity_id: ActivityId,
    pub goal_id: GoalId,
    pub node_id: DagNodeId,
    pub target_agent_id: AgentInstanceId,
    pub target_session_id: Option<AgentSessionId>,
    pub source_count: u8,
    pub source_digests: Option<[u8; 32]>,
    pub source_kinds: Option<[u8; 32]>,
    pub source_refs: Option<&'a [AssignmentSubmittedSourceRef]>,
    pub result_digest: Option<[u8; 32]>,
    pub submitted_at_ms: i64,
}

fn decode_assignment_submitted<'a>(
    entry: &'a EffectFactOutboxEntry,
    fact: &forge_persistence::AssignmentSubmittedEffectFact,
) -> AssignmentSubmittedFact<'a> {
    AssignmentSubmittedFact {
        sequence: entry.sequence(),
        migration_id: entry.migration_id(),
        destination_id: entry.destination_id(),
        workspace_id: entry.workspace_id(),
        assignment_id: fact.assignment_id(),
        activity_id: fact.activity_id(),
        goal_id: fact.goal_id(),
        node_id: fact.node_id(),
        target_agent_id: fact.target_agent_id(),
        target_session_id: fact.target_session_id(),
        source_count: fact.source_count(),
        source_digests: fact.source_digests(),
        source_kinds: fact.source_kinds().map(|kinds| {
            let mut padded = [0u8; 32];
            padded[..kinds.len()].copy_from_slice(kinds);
            padded
        }),
        source_refs: entry.submitted_source_refs(),
        result_digest: fact.result_digest(),
        submitted_at_ms: fact.submitted_at().get(),
    }
}

impl<'a> TryFrom<&'a EffectFactOutboxEntry> for TerminalFact<'a> {
    type Error = OperationsError;
    fn try_from(entry: &'a EffectFactOutboxEntry) -> Result<Self, Self::Error> {
        match entry.fact() {
            EffectFact::ExecutionTerminal(fact) => {
                let verification = fact.verification();
                Ok(Self::Execution(ExecutionTerminalFact {
                    sequence: entry.sequence(),
                    migration_id: entry.migration_id(),
                    destination_id: entry.destination_id(),
                    workspace_id: entry.workspace_id(),
                    execution_id: fact.execution_id(),
                    activity_id: fact.activity_id(),
                    execution_revision: fact.execution_revision().get(),
                    execution_state: fact.state(),
                    observed_at_ms: fact.observed_at().get(),
                    agent_id: fact.agent_id(),
                    agent_revision: fact.agent_revision().get(),
                    session_id: fact.session_id(),
                    goal_id: fact.goal_id(),
                    goal_revision: fact.goal_revision().get(),
                    dag_revision: fact.dag_revision().get(),
                    node_id: fact.node_id(),
                    node_revision: fact.node_revision().get(),
                    verification_input_id: verification.map(|value| value.input_id().into_uuid()),
                    verification_input_fingerprint: verification
                        .map(|value| *value.input_fingerprint().as_bytes()),
                    verification_disposition: verification
                        .map(|value| encode_disposition(value.disposition())),
                    verification_output_integrity: verification
                        .and_then(|value| value.output_integrity().map(encode_integrity)),
                    verification_output_fingerprint: verification.and_then(|value| {
                        value
                            .output_fingerprint()
                            .map(|fingerprint| *fingerprint.as_bytes())
                    }),
                }))
            }
            EffectFact::ChangeSetTerminal(fact) => Ok(Self::ChangeSet(ChangeSetTerminalFact {
                sequence: entry.sequence(),
                migration_id: entry.migration_id(),
                destination_id: entry.destination_id(),
                workspace_id: entry.workspace_id(),
                change_set_id: fact.change_set_id(),
                workspace_revision: fact.workspace_revision().get(),
                activity_id: fact.activity_id(),
                runtime_instance_id: fact.runtime_instance_id(),
                activity_lease_revision: fact.activity_lease_revision().get(),
                change_set_revision: fact.change_set_revision().get(),
                state: fact.state(),
                failure_code: fact.failure_code(),
                diff_id: fact.diff_id(),
                diff_state: fact.diff_state(),
                diff_revision: fact.diff_revision().get(),
                observed_at_ms: fact.observed_at().get(),
                producer_agent_id: fact.producer_agent_id(),
                producer_session_id: fact.producer_session_id(),
            })),
            EffectFact::AssignmentReviewed(fact) => {
                Ok(Self::AssignmentReviewed(AssignmentReviewedFact {
                    sequence: entry.sequence(),
                    migration_id: entry.migration_id(),
                    destination_id: entry.destination_id(),
                    workspace_id: entry.workspace_id(),
                    assignment_id: fact.assignment_id(),
                    activity_id: fact.activity_id(),
                    goal_id: fact.goal_id(),
                    node_id: fact.node_id(),
                    target_agent_id: fact.target_agent_id(),
                    target_session_id: fact.target_session_id(),
                    outcome: fact.outcome(),
                    credit_budget: fact.credit_budget(),
                    reviewer_agent_id: fact.reviewer_agent_id(),
                    reviewer_session_id: fact.reviewer_session_id(),
                    result_digest: fact.result_digest(),
                    observed_at_ms: fact.observed_at().get(),
                }))
            }
            EffectFact::AssignmentOffered(fact) => {
                Ok(Self::AssignmentOffered(AssignmentOfferedFact {
                    sequence: entry.sequence(),
                    migration_id: entry.migration_id(),
                    destination_id: entry.destination_id(),
                    workspace_id: entry.workspace_id(),
                    assignment_id: fact.assignment_id(),
                    activity_id: fact.activity_id(),
                    goal_id: fact.goal_id(),
                    node_id: fact.node_id(),
                    target_agent_id: fact.target_agent_id(),
                    target_session_id: fact.target_session_id(),
                    credit_budget: fact.credit_budget(),
                    offered_at_ms: fact.offered_at().get(),
                }))
            }
            EffectFact::AssignmentSubmitted(fact) => Ok(Self::AssignmentSubmitted(
                decode_assignment_submitted(entry, fact),
            )),
        }
    }
}

impl crate::OperationsCatalog {
    /// Resolve the exact next source sequence for a verified delegated identity.
    /// This is read-only and never acknowledges Forge.
    ///
    /// # Errors
    /// Rejects unknown or unverified imports, identity mismatches, corrupt consumer
    /// state, and database read failures.
    pub fn effect_fact_read_cursor(
        &self,
        migration_id: Uuid,
        workspace_id: WorkspaceId,
        destination_id: Uuid,
    ) -> Result<u64, OperationsError> {
        let import = catalog::load_progress(&self.connection, migration_id)?
            .ok_or(OperationsError::Conflict("effect fact import missing"))?;
        if import.phase() != catalog::ImportPhase::Verified
            || import.workspace_id() != workspace_id
            || import.destination_id() != destination_id
            || self.destination != destination_id
        {
            return Err(OperationsError::Conflict("effect fact read identity"));
        }
        let progress = load_progress(&self.connection, migration_id)?;
        if progress.is_some_and(|value| {
            value.workspace_id() != workspace_id || value.destination_id() != destination_id
        }) {
            return Err(OperationsError::Corrupt("effect fact consumer identity"));
        }
        Ok(progress.map_or(0, EffectFactConsumerProgress::last_sequence))
    }

    /// Apply a bounded source page and advance its durable consumer cursor atomically.
    /// Exact replay is a no-op; gaps and identity mismatches fail closed.
    ///
    /// # Errors
    /// Rejects oversized pages, malformed facts, source gaps, identity conflicts,
    /// corrupt projections, and database commit failures.
    pub fn apply_effect_fact_page(
        &mut self,
        migration_id: Uuid,
        entries: &[EffectFactOutboxEntry],
    ) -> Result<EffectFactConsumerProgress, OperationsError> {
        if entries.len() > MAX_PAGE_SIZE {
            return Err(OperationsError::Bounds("effect fact page"));
        }
        let facts = entries
            .iter()
            .map(TerminalFact::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        self.apply_effect_fact_inputs(migration_id, &facts)
    }

    /// Validate bounded source candidates and atomically apply the existing projection protocol.
    ///
    /// # Errors
    /// Rejects invalid candidates, import identity conflicts, gaps, divergent replay and storage failures.
    pub fn apply_effect_fact_inputs(
        &mut self,
        migration_id: Uuid,
        facts: &[EffectFactInput<'_>],
    ) -> Result<EffectFactConsumerProgress, OperationsError> {
        if migration_id.is_nil() {
            return Err(OperationsError::Bounds("effect fact migration id"));
        }
        input_validation::validate_effect_fact_inputs(facts)?;
        apply_page(&mut self.connection, self.destination, migration_id, facts)
    }

    /// Resolve the locally verified source and its current consumer cursor in one read snapshot.
    /// Missing non-aborted imports produce no source; incomplete imports fail explicitly.
    ///
    /// # Errors
    /// Rejects damaged identities, unverified imports, mismatched consumers and database failures.
    pub fn effect_fact_source_cursor(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<EffectFactConsumerProgress>, OperationsError> {
        if workspace_id.into_uuid().is_nil() {
            return Err(OperationsError::Bounds("effect source workspace"));
        }
        let tx = self.connection.unchecked_transaction()?;
        let raw: Option<Vec<u8>> = tx
            .query_row(
                "SELECT migration_id FROM ops_imports WHERE workspace_id=?1 AND phase!=3",
                params![workspace_id.into_uuid().as_bytes()],
                |row| row.get(0),
            )
            .optional()?;
        let result = if let Some(raw) = raw {
            let migration_id = Uuid::from_slice(&raw)
                .map_err(|_| OperationsError::Corrupt("effect source migration"))?;
            let import = catalog::load_progress(&tx, migration_id)?
                .ok_or(OperationsError::Corrupt("effect source import"))?;
            if migration_id.is_nil()
                || import.phase() != catalog::ImportPhase::Verified
                || import.workspace_id() != workspace_id
                || import.destination_id() != self.destination
            {
                return Err(OperationsError::Conflict(
                    "effect source import not verified",
                ));
            }
            let progress =
                load_progress(&tx, migration_id)?.unwrap_or(EffectFactConsumerProgress {
                    migration_id,
                    workspace_id,
                    destination_id: self.destination,
                    last_sequence: 0,
                    applied_count: 0,
                });
            if progress.workspace_id() != workspace_id
                || progress.destination_id() != self.destination
            {
                return Err(OperationsError::Corrupt("effect source consumer identity"));
            }
            Some(progress)
        } else {
            None
        };
        tx.commit()?;
        Ok(result)
    }

    /// Read the durable source cursor without acknowledging or mutating Forge.
    ///
    /// # Errors
    /// Rejects malformed persisted consumer state and database read failures.
    pub fn effect_fact_progress(
        &self,
        migration_id: Uuid,
    ) -> Result<Option<EffectFactConsumerProgress>, OperationsError> {
        load_progress(&self.connection, migration_id)
    }

    /// Read one bounded page of durable execution-terminal projections ordered by
    /// source sequence. This never mutates state or acknowledges Forge.
    ///
    /// # Errors
    /// Rejects malformed persisted projections and database read failures.
    pub fn execution_terminal_projection(
        &self,
        migration_id: Uuid,
    ) -> Result<Vec<ExecutionTerminalProjectionRow>, OperationsError> {
        load_execution_projection(&self.connection, migration_id)
    }

    /// Read one bounded page of durable `ChangeSet` terminal projections ordered by
    /// source sequence. This never mutates state or acknowledges Forge.
    ///
    /// # Errors
    /// Rejects malformed persisted projections and database read failures.
    pub fn change_set_projection(
        &self,
        migration_id: Uuid,
    ) -> Result<Vec<ChangeSetProjectionRow>, OperationsError> {
        load_change_set_projection(&self.connection, migration_id)
    }

    /// Read one bounded page of durable assignment-review projections ordered by
    /// source sequence. This never mutates state or acknowledges Forge.
    ///
    /// # Errors
    /// Rejects malformed persisted projections and database read failures.
    pub fn assignment_review_projection(
        &self,
        migration_id: Uuid,
    ) -> Result<Vec<AssignmentReviewProjectionRow>, OperationsError> {
        load_assignment_review_projection(&self.connection, migration_id)
    }

    /// Read one bounded page of durable assignment-offer projections ordered by
    /// source sequence. This never mutates state or acknowledges Forge.
    ///
    /// # Errors
    /// Rejects malformed persisted projections and database read failures.
    pub fn assignment_offer_projection(
        &self,
        migration_id: Uuid,
    ) -> Result<Vec<AssignmentOfferProjectionRow>, OperationsError> {
        load_assignment_offer_projection(&self.connection, migration_id)
    }

    /// Read one bounded page of durable assignment-submission projections
    /// ordered by source sequence. This never mutates state or acknowledges
    /// Forge.
    ///
    /// # Errors
    /// Rejects malformed persisted projections and database read failures.
    pub fn assignment_submit_projection(
        &self,
        migration_id: Uuid,
    ) -> Result<Vec<AssignmentSubmitProjectionRow>, OperationsError> {
        load_assignment_submit_projection(&self.connection, migration_id)
    }
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    connection.execute_batch(CONSUMERS_SQL)?;
    connection.execute_batch(EXECUTIONS_SQL)?;
    create_change_set_schema(connection)?;
    create_assignment_review_schema(connection)?;
    create_assignment_offer_schema(connection)?;
    create_assignment_submit_schema(connection)?;
    connection.execute_batch(ASSIGNMENT_SUBMIT_SOURCE_REFS_SQL)?;
    Ok(())
}

pub(crate) fn create_change_set_schema(connection: &Connection) -> Result<(), OperationsError> {
    connection.execute_batch(CHANGE_SETS_SQL)?;
    Ok(())
}

pub(crate) fn create_assignment_review_schema(
    connection: &Connection,
) -> Result<(), OperationsError> {
    connection.execute_batch(ASSIGNMENT_REVIEWS_SQL)?;
    Ok(())
}

pub(crate) fn create_assignment_offer_schema(
    connection: &Connection,
) -> Result<(), OperationsError> {
    connection.execute_batch(ASSIGNMENT_OFFERS_SQL)?;
    Ok(())
}

pub(crate) fn create_assignment_submit_schema(
    connection: &Connection,
) -> Result<(), OperationsError> {
    connection.execute_batch(ASSIGNMENT_SUBMITS_SQL)?;
    Ok(())
}

pub(crate) fn create_assignment_submit_source_refs_schema(
    connection: &Connection,
) -> Result<(), OperationsError> {
    connection.execute_batch(&format!(
        "ALTER TABLE ops_assignment_submit_projection ADD COLUMN {SOURCE_REFS_RECORDED_COLUMN_SQL}"
    ))?;
    connection.execute_batch(ASSIGNMENT_SUBMIT_SOURCE_REFS_SQL)?;
    Ok(())
}

pub(crate) fn create_assignment_submit_source_refs_table(
    connection: &Connection,
) -> Result<(), OperationsError> {
    connection.execute_batch(ASSIGNMENT_SUBMIT_SOURCE_REFS_SQL)?;
    Ok(())
}

/// Current-shape schema objects with the change-set projection text
/// normalized to the shared owned form used by catalog validation.
pub(crate) fn schema_objects_owned() -> Vec<(&'static str, &'static str, String)> {
    schema_objects()
        .into_iter()
        .map(|(kind, name, sql)| (kind, name, sql.to_owned()))
        .collect()
}

pub(crate) fn schema_objects() -> [(&'static str, &'static str, &'static str); 7] {
    [
        ("table", "ops_effect_fact_consumers", CONSUMERS_SQL),
        ("table", "ops_execution_terminal_projection", EXECUTIONS_SQL),
        ("table", "ops_change_set_projection", CHANGE_SETS_SQL),
        (
            "table",
            "ops_assignment_review_projection",
            ASSIGNMENT_REVIEWS_SQL,
        ),
        (
            "table",
            "ops_assignment_offer_projection",
            ASSIGNMENT_OFFERS_SQL,
        ),
        (
            "table",
            "ops_assignment_submit_projection",
            ASSIGNMENT_SUBMITS_SQL,
        ),
        (
            "table",
            "ops_assignment_submit_source_refs",
            ASSIGNMENT_SUBMIT_SOURCE_REFS_SQL,
        ),
    ]
}

pub(crate) fn schema_objects_v16() -> Vec<(&'static str, &'static str, String)> {
    schema_objects()
        .into_iter()
        .filter(|(_, name, _)| *name != "ops_assignment_submit_source_refs")
        .map(|(kind, name, sql)| {
            (
                kind,
                name,
                if name == "ops_assignment_submit_projection" {
                    assignment_submits_sql_v16()
                } else if name == "ops_change_set_projection" {
                    change_sets_sql_v17()
                } else {
                    sql.to_owned()
                },
            )
        })
        .collect()
}

pub(crate) fn schema_objects_v17() -> Vec<(&'static str, &'static str, String)> {
    schema_objects()
        .into_iter()
        .map(|(kind, name, sql)| {
            (
                kind,
                name,
                if name == "ops_change_set_projection" {
                    change_sets_sql_v17()
                } else {
                    sql.to_owned()
                },
            )
        })
        .collect()
}

/// Schema objects as they existed at catalog version 9, before `ChangeSet`
/// terminal facts gained a durable projection.
pub(crate) fn schema_objects_v9() -> Vec<(&'static str, &'static str, String)> {
    vec![
        (
            "table",
            "ops_effect_fact_consumers",
            CONSUMERS_SQL.to_owned(),
        ),
        (
            "table",
            "ops_execution_terminal_projection",
            EXECUTIONS_SQL.to_owned(),
        ),
    ]
}

/// Schema objects as they existed at catalog version 10, before assignment
/// reviews gained a durable projection and work credits gained a ledger. The
/// change-set projection still predates the producer columns.
pub(crate) fn schema_objects_v10() -> Vec<(&'static str, &'static str, String)> {
    vec![
        (
            "table",
            "ops_effect_fact_consumers",
            CONSUMERS_SQL.to_owned(),
        ),
        (
            "table",
            "ops_execution_terminal_projection",
            EXECUTIONS_SQL.to_owned(),
        ),
        ("table", "ops_change_set_projection", change_sets_sql_v13()),
    ]
}

/// Schema objects as they existed at catalog version 11, before assignment
/// offers gained a durable projection and leadership gained its journal.
pub(crate) fn schema_objects_v11() -> Vec<(&'static str, &'static str, String)> {
    vec![
        (
            "table",
            "ops_effect_fact_consumers",
            CONSUMERS_SQL.to_owned(),
        ),
        (
            "table",
            "ops_execution_terminal_projection",
            EXECUTIONS_SQL.to_owned(),
        ),
        ("table", "ops_change_set_projection", change_sets_sql_v13()),
        (
            "table",
            "ops_assignment_review_projection",
            ASSIGNMENT_REVIEWS_SQL.to_owned(),
        ),
    ]
}

/// Schema objects as they existed at catalog version 12, before assignment
/// submissions gained a durable projection.
pub(crate) fn schema_objects_v12() -> Vec<(&'static str, &'static str, String)> {
    vec![
        (
            "table",
            "ops_effect_fact_consumers",
            CONSUMERS_SQL.to_owned(),
        ),
        (
            "table",
            "ops_execution_terminal_projection",
            EXECUTIONS_SQL.to_owned(),
        ),
        ("table", "ops_change_set_projection", change_sets_sql_v13()),
        (
            "table",
            "ops_assignment_review_projection",
            ASSIGNMENT_REVIEWS_SQL.to_owned(),
        ),
        (
            "table",
            "ops_assignment_offer_projection",
            ASSIGNMENT_OFFERS_SQL.to_owned(),
        ),
    ]
}

/// Schema objects as they existed at catalog version 13, before the
/// change-set projection gained producer attribution.
pub(crate) fn schema_objects_v13() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = schema_objects_v12();
    objects.push((
        "table",
        "ops_assignment_submit_projection",
        assignment_submits_sql_v16(),
    ));
    objects
}

fn apply_page(
    connection: &mut Connection,
    destination_id: Uuid,
    migration_id: Uuid,
    facts: &[TerminalFact<'_>],
) -> Result<EffectFactConsumerProgress, OperationsError> {
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let import = catalog::load_progress(&tx, migration_id)?
        .ok_or(OperationsError::Conflict("effect fact import missing"))?;
    if import.phase() != catalog::ImportPhase::Verified || import.destination_id() != destination_id
    {
        return Err(OperationsError::Conflict("effect fact import not verified"));
    }
    let mut progress = load_progress(&tx, migration_id)?.unwrap_or(EffectFactConsumerProgress {
        migration_id,
        workspace_id: import.workspace_id(),
        destination_id,
        last_sequence: 0,
        applied_count: 0,
    });
    if progress.workspace_id != import.workspace_id() || progress.destination_id != destination_id {
        return Err(OperationsError::Corrupt("effect fact consumer identity"));
    }
    if facts.is_empty() {
        tx.commit()
            .map_err(|source| OperationsError::CommitUnconfirmed {
                migration_id,
                source,
            })?;
        return Ok(progress);
    }
    if load_progress(&tx, migration_id)?.is_none() {
        tx.execute(
            "INSERT INTO ops_effect_fact_consumers(migration_id,workspace_id,destination_id,last_sequence,applied_count) VALUES(?1,?2,?3,0,0)",
            params![migration_id.as_bytes(), import.workspace_id().into_uuid().as_bytes(), destination_id.as_bytes()],
        )?;
    }
    let initial = progress;
    for fact in facts {
        validate_identity(fact, &import)?;
        if fact.sequence() <= progress.last_sequence {
            if !replay_matches(&tx, fact)? {
                return Err(OperationsError::Conflict("effect fact replay"));
            }
            continue;
        }
        let expected = progress
            .last_sequence
            .checked_add(1)
            .ok_or(OperationsError::Bounds("effect fact sequence"))?;
        if fact.sequence() != expected {
            return Err(OperationsError::ResyncRequired("effect fact sequence gap"));
        }
        insert_projection(&tx, fact)?;
        derive_work_credit(&tx, fact)?;
        progress.last_sequence = fact.sequence();
        progress.applied_count = progress
            .applied_count
            .checked_add(1)
            .ok_or(OperationsError::Bounds("effect fact applied count"))?;
    }
    if progress != initial {
        let changed = tx.execute(
            "UPDATE ops_effect_fact_consumers SET last_sequence=?1,applied_count=?2 WHERE migration_id=?3 AND last_sequence=?4 AND applied_count=?5",
            params![signed(progress.last_sequence)?, signed(progress.applied_count)?, migration_id.as_bytes(), signed(initial.last_sequence)?, signed(initial.applied_count)?],
        )?;
        if changed != 1 {
            return Err(OperationsError::Conflict("effect fact consumer revision"));
        }
    }
    tx.commit()
        .map_err(|source| OperationsError::CommitUnconfirmed {
            migration_id,
            source,
        })?;
    Ok(progress)
}

fn validate_identity(
    fact: &TerminalFact<'_>,
    import: &catalog::ImportProgress,
) -> Result<(), OperationsError> {
    if fact.sequence() == 0
        || fact.migration_id() != import.migration_id()
        || fact.workspace_id() != import.workspace_id()
        || fact.destination_id() != import.destination_id()
    {
        return Err(OperationsError::Conflict("effect fact source identity"));
    }
    if let TerminalFact::ChangeSet(change) = fact
        && change.producer_agent_id.is_none()
        && change.producer_session_id.is_some()
    {
        return Err(OperationsError::Conflict("effect fact producer pair"));
    }
    Ok(())
}

fn replay_matches(
    transaction: &Transaction<'_>,
    fact: &TerminalFact<'_>,
) -> Result<bool, OperationsError> {
    if let TerminalFact::ChangeSet(fact) = fact {
        let stored = transaction
            .query_row(
                "SELECT substr(fact_digest,1,33),digest_version,substr(producer_agent_id,1,17),substr(producer_session_id,1,17) FROM ops_change_set_projection WHERE migration_id=?1 AND source_sequence=?2",
                params![fact.migration_id.as_bytes(), signed(fact.sequence)?],
                |row| {
                    Ok(StoredChangeSetReplay {
                        digest: row.get(0)?,
                        digest_version: row.get(1)?,
                        producer_agent: row.get(2)?,
                        producer_session: row.get(3)?,
                    })
                },
            )
            .optional()?;
        return change_set_replay_matches(transaction, fact, stored);
    }
    let stored: Option<Vec<u8>> = match fact {
        TerminalFact::Execution(fact) => transaction
            .query_row(
                "SELECT substr(fact_digest,1,33) FROM ops_execution_terminal_projection WHERE migration_id=?1 AND source_sequence=?2",
                params![fact.migration_id.as_bytes(), signed(fact.sequence)?],
                |row| row.get(0),
            )
            .optional()?,
        TerminalFact::ChangeSet(fact) => transaction
            .query_row(
                "SELECT substr(fact_digest,1,33) FROM ops_change_set_projection WHERE migration_id=?1 AND source_sequence=?2",
                params![fact.migration_id.as_bytes(), signed(fact.sequence)?],
                |row| row.get(0),
            )
            .optional()?,
        TerminalFact::AssignmentReviewed(fact) => transaction
            .query_row(
                "SELECT substr(fact_digest,1,33) FROM ops_assignment_review_projection WHERE migration_id=?1 AND source_sequence=?2",
                params![fact.migration_id.as_bytes(), signed(fact.sequence)?],
                |row| row.get(0),
            )
            .optional()?,
        TerminalFact::AssignmentOffered(fact) => transaction
            .query_row(
                "SELECT substr(fact_digest,1,33) FROM ops_assignment_offer_projection WHERE migration_id=?1 AND source_sequence=?2",
                params![fact.migration_id.as_bytes(), signed(fact.sequence)?],
                |row| row.get(0),
            )
            .optional()?,
        TerminalFact::AssignmentSubmitted(fact) => transaction
            .query_row(
                "SELECT substr(fact_digest,1,33) FROM ops_assignment_submit_projection WHERE migration_id=?1 AND source_sequence=?2",
                params![fact.migration_id.as_bytes(), signed(fact.sequence)?],
                |row| row.get(0),
            )
            .optional()?,
    };
    if stored.as_deref() != Some(fact_digest(fact).as_slice()) {
        return Ok(false);
    }
    match fact {
        TerminalFact::AssignmentSubmitted(fact) => assignment_source_refs_match(transaction, fact),
        _ => Ok(true),
    }
}

#[derive(Debug)]
struct StoredChangeSetReplay {
    digest: Vec<u8>,
    digest_version: i64,
    producer_agent: Option<Vec<u8>>,
    producer_session: Option<Vec<u8>>,
}

fn change_set_replay_matches(
    transaction: &Transaction<'_>,
    fact: &ChangeSetTerminalFact,
    stored: Option<StoredChangeSetReplay>,
) -> Result<bool, OperationsError> {
    let Some(stored) = stored else {
        return Ok(false);
    };
    let stored_digest: [u8; 32] = stored
        .digest
        .as_slice()
        .try_into()
        .map_err(|_| OperationsError::Corrupt("effect fact change set digest"))?;
    validate_replay_producer(
        stored.producer_agent.as_deref(),
        "effect fact producer agent",
    )?;
    validate_replay_producer(
        stored.producer_session.as_deref(),
        "effect fact producer session",
    )?;
    if stored.producer_agent.is_none() && stored.producer_session.is_some() {
        return Err(OperationsError::Corrupt("effect fact producer pair"));
    }
    let incoming_producer_agent = fact
        .producer_agent_id
        .map(|id| id.into_uuid().as_bytes().to_vec());
    let incoming_producer_session = fact
        .producer_session_id
        .map(|id| id.into_uuid().as_bytes().to_vec());
    let producer_matches = stored.producer_agent.as_ref() == incoming_producer_agent.as_ref()
        && stored.producer_session.as_ref() == incoming_producer_session.as_ref();
    let v2 = change_set_digest(fact);
    let matches_v2 = producer_matches && stored_digest == v2;
    let digest_version = match stored.digest_version {
        0 | 2 => stored.digest_version,
        _ => {
            return Err(OperationsError::Corrupt(
                "effect fact change set digest version",
            ));
        }
    };
    let matches_v1 = digest_version == 0
        && stored.producer_agent.is_none()
        && stored.producer_session.is_none()
        && fact.producer_agent_id.is_none()
        && fact.producer_session_id.is_none()
        && stored_digest == change_set_digest_v1(fact);
    if digest_version == 2 {
        return Ok(matches_v2);
    }
    if !matches_v2 && !matches_v1 {
        return Ok(false);
    }
    transaction.execute(
        "UPDATE ops_change_set_projection SET fact_digest=?1,digest_version=2 WHERE migration_id=?2 AND source_sequence=?3 AND digest_version=0",
        params![v2.as_slice(), fact.migration_id.as_bytes(), signed(fact.sequence)?],
    )?;
    Ok(true)
}

fn validate_replay_producer(
    value: Option<&[u8]>,
    label: &'static str,
) -> Result<(), OperationsError> {
    if value.is_some_and(|value| value.len() != 16) {
        return Err(OperationsError::Corrupt(label));
    }
    Ok(())
}

fn assignment_source_refs_match(
    transaction: &Transaction<'_>,
    fact: &AssignmentSubmittedFact<'_>,
) -> Result<bool, OperationsError> {
    let recorded: i64 = transaction.query_row(
        "SELECT source_refs_recorded FROM ops_assignment_submit_projection WHERE migration_id=?1 AND source_sequence=?2",
        params![fact.migration_id.as_bytes(), signed(fact.sequence)?],
        |row| row.get(0),
    )?;
    let recorded = match recorded {
        0 => false,
        1 => true,
        _ => return Err(OperationsError::Corrupt("effect fact source refs marker")),
    };
    if recorded != fact.source_refs.is_some() {
        return Ok(false);
    }
    let mut statement = transaction.prepare(
        "SELECT ordinal,evidence_kind,substr(evidence_id,1,17) FROM ops_assignment_submit_source_refs WHERE migration_id=?1 AND source_sequence=?2 ORDER BY ordinal ASC LIMIT 33",
    )?;
    let stored = statement
        .query_map(
            params![fact.migration_id.as_bytes(), signed(fact.sequence)?],
            |row| {
                Ok(RawAssignmentSourceRefRow {
                    ordinal: row.get(0)?,
                    evidence_kind: row.get(1)?,
                    evidence_id: row.get(2)?,
                })
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;
    if stored.len() > 32 {
        return Err(OperationsError::Corrupt("effect fact source refs"));
    }
    let mut previous_ordinal = None;
    for row in &stored {
        if !(0..=31).contains(&row.ordinal)
            || !(0..=3).contains(&row.evidence_kind)
            || row.evidence_id.len() != 16
            || previous_ordinal.is_some_and(|previous| row.ordinal <= previous)
        {
            return Err(OperationsError::Corrupt("effect fact source refs"));
        }
        previous_ordinal = Some(row.ordinal);
    }
    let Some(source_refs) = fact.source_refs else {
        return Ok(stored.is_empty());
    };
    if stored.len() != source_refs.len() {
        return Ok(false);
    }
    for (stored, expected) in stored.iter().zip(source_refs) {
        let (kind, id) = encode_source_ref(expected.evidence());
        if stored.ordinal != i64::from(expected.ordinal())
            || stored.evidence_kind != kind
            || stored.evidence_id.as_slice() != id.as_slice()
        {
            return Ok(false);
        }
    }
    Ok(true)
}

#[derive(Debug)]
struct RawAssignmentSourceRefRow {
    ordinal: i64,
    evidence_kind: i64,
    evidence_id: Vec<u8>,
}

fn insert_projection(
    transaction: &Transaction<'_>,
    fact: &TerminalFact<'_>,
) -> Result<(), OperationsError> {
    match fact {
        TerminalFact::Execution(fact) => insert_execution_projection(transaction, fact),
        TerminalFact::ChangeSet(fact) => insert_change_set_projection(transaction, fact),
        TerminalFact::AssignmentReviewed(fact) => {
            insert_assignment_review_projection(transaction, fact)
        }
        TerminalFact::AssignmentOffered(fact) => {
            insert_assignment_offer_projection(transaction, fact)
        }
        TerminalFact::AssignmentSubmitted(fact) => {
            insert_assignment_submit_projection(transaction, fact)
        }
    }
}

/// Derive one deterministic work credit from the just-stored projection row.
/// The derivation is order-independent: both consumption orders of a matching
/// execution/review pair converge on the identical ledger row, so the losing
/// trigger observes an exact replay no-op.
fn derive_work_credit(
    transaction: &Transaction<'_>,
    fact: &TerminalFact<'_>,
) -> Result<(), OperationsError> {
    match fact {
        TerminalFact::Execution(fact) => {
            if fact.verification_disposition != Some(ACCEPTED_DISPOSITION) {
                return Ok(());
            }
            work_credit::award_for_verified_execution(
                transaction,
                fact.workspace_id,
                fact.agent_id,
                fact.goal_id,
                fact.node_id,
            )
        }
        TerminalFact::AssignmentReviewed(fact) => work_credit::award_for_review(
            transaction,
            work_credit::ReviewAwardObservation {
                workspace_id: fact.workspace_id,
                assignment_id: fact.assignment_id,
                activity_id: fact.activity_id,
                goal_id: fact.goal_id,
                node_id: fact.node_id,
                target_agent_id: fact.target_agent_id,
                reviewer_agent_id: fact.reviewer_agent_id,
                budget: fact.credit_budget,
                award_source_sequence: fact.sequence,
                awarded_at_ms: fact.observed_at_ms,
            },
            fact.outcome,
        ),
        TerminalFact::ChangeSet(_) | TerminalFact::AssignmentSubmitted(_) => Ok(()),
        TerminalFact::AssignmentOffered(fact) => crate::leadership::bootstrap_from_offer(
            transaction,
            crate::leadership::OfferBootstrapObservation {
                workspace_id: fact.workspace_id,
                activity_id: fact.activity_id,
                assignment_id: fact.assignment_id,
                target_agent_id: fact.target_agent_id,
                offered_at_ms: fact.offered_at_ms,
            },
        ),
    }
}

fn insert_execution_projection(
    transaction: &Transaction<'_>,
    fact: &ExecutionTerminalFact,
) -> Result<(), OperationsError> {
    if transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_execution_terminal_projection WHERE migration_id=?1 AND execution_id=?2)",
        params![fact.migration_id.as_bytes(), fact.execution_id.into_uuid().as_bytes()],
        |row| row.get::<_, bool>(0),
    )? {
        return Err(OperationsError::Conflict("effect fact execution identity"));
    }
    transaction.execute(
        "INSERT INTO ops_execution_terminal_projection(migration_id,source_sequence,fact_digest,execution_id,workspace_id,activity_id,execution_revision,execution_state,observed_at_ms,agent_id,agent_revision,session_id,goal_id,goal_revision,dag_revision,node_id,node_revision,verification_input_id,verification_input_fingerprint,verification_disposition,verification_output_integrity,verification_output_fingerprint) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22)",
        params![
            fact.migration_id.as_bytes(), signed(fact.sequence)?, execution_digest(fact).as_slice(), fact.execution_id.into_uuid().as_bytes(),
            fact.workspace_id.into_uuid().as_bytes(), fact.activity_id.into_uuid().as_bytes(), fact.execution_revision, encode_state(fact.execution_state), fact.observed_at_ms,
            fact.agent_id.into_uuid().as_bytes(), fact.agent_revision, fact.session_id.map(|id| id.into_uuid().as_bytes().to_vec()), fact.goal_id.into_uuid().as_bytes(),
            fact.goal_revision, fact.dag_revision, fact.node_id.into_uuid().as_bytes(), fact.node_revision, fact.verification_input_id.map(|id| id.as_bytes().to_vec()),
            fact.verification_input_fingerprint.map(|value| value.to_vec()), fact.verification_disposition, fact.verification_output_integrity, fact.verification_output_fingerprint.map(|value| value.to_vec()),
        ],
    )?;
    Ok(())
}

fn insert_change_set_projection(
    transaction: &Transaction<'_>,
    fact: &ChangeSetTerminalFact,
) -> Result<(), OperationsError> {
    if transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_change_set_projection WHERE migration_id=?1 AND change_set_id=?2)",
        params![fact.migration_id.as_bytes(), fact.change_set_id.into_uuid().as_bytes()],
        |row| row.get::<_, bool>(0),
    )? {
        return Err(OperationsError::Conflict("effect fact change set identity"));
    }
    transaction.execute(
        "INSERT INTO ops_change_set_projection(migration_id,source_sequence,fact_digest,change_set_id,workspace_id,activity_id,change_set_revision,state,failure_code,diff_id,diff_state,diff_revision,observed_at_ms,producer_agent_id,producer_session_id,digest_version) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
        params![
            fact.migration_id.as_bytes(), signed(fact.sequence)?, change_set_digest(fact).as_slice(),
            fact.change_set_id.into_uuid().as_bytes(), fact.workspace_id.into_uuid().as_bytes(),
            fact.activity_id.into_uuid().as_bytes(), fact.change_set_revision,
            encode_change_set_state(fact.state), encode_failure_code(fact.failure_code),
            fact.diff_id.into_uuid().as_bytes(), encode_diff_state(fact.diff_state),
            fact.diff_revision, fact.observed_at_ms,
            fact.producer_agent_id.map(|id| id.into_uuid().as_bytes().to_vec()),
            fact.producer_session_id.map(|id| id.into_uuid().as_bytes().to_vec()),
            2_i64,
        ],
    )?;
    Ok(())
}

fn insert_assignment_review_projection(
    transaction: &Transaction<'_>,
    fact: &AssignmentReviewedFact,
) -> Result<(), OperationsError> {
    if transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_assignment_review_projection WHERE migration_id=?1 AND assignment_id=?2)",
        params![fact.migration_id.as_bytes(), fact.assignment_id.into_uuid().as_bytes()],
        |row| row.get::<_, bool>(0),
    )? {
        return Err(OperationsError::Conflict("effect fact assignment identity"));
    }
    transaction.execute(
        "INSERT INTO ops_assignment_review_projection(migration_id,source_sequence,fact_digest,assignment_id,workspace_id,activity_id,goal_id,node_id,target_agent_id,target_session_id,outcome,credit_budget,reviewer_agent_id,result_digest,observed_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
        params![
            fact.migration_id.as_bytes(), signed(fact.sequence)?, assignment_review_digest(fact).as_slice(),
            fact.assignment_id.into_uuid().as_bytes(), fact.workspace_id.into_uuid().as_bytes(),
            fact.activity_id.into_uuid().as_bytes(), fact.goal_id.into_uuid().as_bytes(),
            fact.node_id.into_uuid().as_bytes(), fact.target_agent_id.into_uuid().as_bytes(),
            fact.target_session_id.map(|id| id.into_uuid().as_bytes().to_vec()),
            encode_review_outcome(fact.outcome), fact.credit_budget.map(WorkCreditBudget::value),
            fact.reviewer_agent_id.into_uuid().as_bytes(),
            fact.result_digest.map(|value| value.to_vec()), fact.observed_at_ms,
        ],
    )?;
    Ok(())
}

fn insert_assignment_offer_projection(
    transaction: &Transaction<'_>,
    fact: &AssignmentOfferedFact,
) -> Result<(), OperationsError> {
    if transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_assignment_offer_projection WHERE migration_id=?1 AND assignment_id=?2)",
        params![fact.migration_id.as_bytes(), fact.assignment_id.into_uuid().as_bytes()],
        |row| row.get::<_, bool>(0),
    )? {
        return Err(OperationsError::Conflict("effect fact assignment identity"));
    }
    transaction.execute(
        "INSERT INTO ops_assignment_offer_projection(migration_id,source_sequence,fact_digest,assignment_id,workspace_id,activity_id,goal_id,node_id,target_agent_id,target_session_id,credit_budget,offered_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
        params![
            fact.migration_id.as_bytes(), signed(fact.sequence)?, assignment_offer_digest(fact).as_slice(),
            fact.assignment_id.into_uuid().as_bytes(), fact.workspace_id.into_uuid().as_bytes(),
            fact.activity_id.into_uuid().as_bytes(), fact.goal_id.into_uuid().as_bytes(),
            fact.node_id.into_uuid().as_bytes(), fact.target_agent_id.into_uuid().as_bytes(),
            fact.target_session_id.map(|id| id.into_uuid().as_bytes().to_vec()),
            fact.credit_budget.map(WorkCreditBudget::value), fact.offered_at_ms,
        ],
    )?;
    Ok(())
}

fn insert_assignment_submit_projection(
    transaction: &Transaction<'_>,
    fact: &AssignmentSubmittedFact<'_>,
) -> Result<(), OperationsError> {
    validate_assignment_source_refs(fact)?;
    if transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_assignment_submit_projection WHERE migration_id=?1 AND assignment_id=?2)",
        params![fact.migration_id.as_bytes(), fact.assignment_id.into_uuid().as_bytes()],
        |row| row.get::<_, bool>(0),
    )? {
        return Err(OperationsError::Conflict("effect fact assignment identity"));
    }
    transaction.execute(
        "INSERT INTO ops_assignment_submit_projection(migration_id,source_sequence,fact_digest,assignment_id,workspace_id,activity_id,goal_id,node_id,target_agent_id,target_session_id,source_count,source_digests,source_kinds,result_digest,submitted_at_ms,source_refs_recorded) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
        params![
            fact.migration_id.as_bytes(), signed(fact.sequence)?, assignment_submit_digest(fact).as_slice(),
            fact.assignment_id.into_uuid().as_bytes(), fact.workspace_id.into_uuid().as_bytes(),
            fact.activity_id.into_uuid().as_bytes(), fact.goal_id.into_uuid().as_bytes(),
            fact.node_id.into_uuid().as_bytes(), fact.target_agent_id.into_uuid().as_bytes(),
            fact.target_session_id.map(|id| id.into_uuid().as_bytes().to_vec()),
            i64::from(fact.source_count),
            fact.source_digests.map(|digest| digest.to_vec()),
            fact.source_kinds
                .map(|kinds| kinds[..fact.source_count as usize].to_vec()),
            fact.result_digest.map(|digest| digest.to_vec()),
            fact.submitted_at_ms,
            i64::from(fact.source_refs.is_some()),
        ],
    )?;
    if let Some(source_refs) = fact.source_refs {
        for source_ref in source_refs {
            let (evidence_kind, evidence_id) = encode_source_ref(source_ref.evidence());
            transaction.execute(
                "INSERT INTO ops_assignment_submit_source_refs(migration_id,source_sequence,ordinal,evidence_kind,evidence_id) VALUES(?1,?2,?3,?4,?5)",
                params![
                    fact.migration_id.as_bytes(),
                    signed(fact.sequence)?,
                    i64::from(source_ref.ordinal()),
                    evidence_kind,
                    evidence_id,
                ],
            )?;
        }
    }
    Ok(())
}

fn validate_assignment_source_refs(
    fact: &AssignmentSubmittedFact<'_>,
) -> Result<(), OperationsError> {
    let Some(source_refs) = fact.source_refs else {
        return Ok(());
    };
    let source_count = usize::from(fact.source_count);
    if source_count > 32 {
        return Err(OperationsError::Corrupt("effect fact source refs"));
    }
    let Some(source_kinds) = fact.source_kinds.as_ref() else {
        return if source_refs.is_empty() && source_count == 0 {
            Ok(())
        } else {
            Err(OperationsError::Corrupt("effect fact source refs"))
        };
    };
    let source_kinds = &source_kinds[..source_count];
    let expected_count = source_kinds
        .iter()
        .try_fold(0_usize, |count, kind| match *kind {
            0 => Ok(count),
            1 => count
                .checked_add(1)
                .ok_or(OperationsError::Bounds("effect fact source refs")),
            _ => Err(OperationsError::Corrupt("effect fact source kinds")),
        })?;
    if source_refs.len() != expected_count {
        return Err(OperationsError::Corrupt("effect fact source refs"));
    }
    let mut ref_index = 0;
    for (ordinal, kind) in source_kinds.iter().enumerate() {
        if *kind != 1 {
            continue;
        }
        let source_ref = source_refs
            .get(ref_index)
            .ok_or(OperationsError::Corrupt("effect fact source refs"))?;
        if usize::from(source_ref.ordinal()) != ordinal {
            return Err(OperationsError::Corrupt("effect fact source refs"));
        }
        ref_index += 1;
    }
    if ref_index != source_refs.len() {
        return Err(OperationsError::Corrupt("effect fact source refs"));
    }
    Ok(())
}

fn encode_source_ref(evidence: forge_domain::GoalEvidenceRef) -> (i64, Vec<u8>) {
    match evidence {
        forge_domain::GoalEvidenceRef::ChangeSet(id) => (0, id.into_uuid().into_bytes().to_vec()),
        forge_domain::GoalEvidenceRef::Execution(id) => (1, id.into_uuid().into_bytes().to_vec()),
        forge_domain::GoalEvidenceRef::Approval(id) => (2, id.into_uuid().into_bytes().to_vec()),
        forge_domain::GoalEvidenceRef::Checkpoint(id) => (3, id.into_uuid().into_bytes().to_vec()),
    }
}

fn load_assignment_submit_projection(
    connection: &Connection,
    migration_id: Uuid,
) -> Result<Vec<AssignmentSubmitProjectionRow>, OperationsError> {
    let limit = page_limit()?;
    let mut statement = connection.prepare(
        "SELECT substr(migration_id,1,17),source_sequence,substr(fact_digest,1,33),substr(assignment_id,1,17),substr(workspace_id,1,17),substr(activity_id,1,17),substr(goal_id,1,17),substr(node_id,1,17),substr(target_agent_id,1,17),substr(target_session_id,1,17),source_count,substr(source_digests,1,33),substr(source_kinds,1,33),substr(result_digest,1,33),submitted_at_ms,source_refs_recorded FROM ops_assignment_submit_projection WHERE migration_id=?1 ORDER BY source_sequence ASC LIMIT ?2",
    )?;
    let raw = statement
        .query_map(params![migration_id.as_bytes(), limit], |row| {
            Ok(RawAssignmentSubmitRow {
                migration: row.get(0)?,
                sequence: row.get(1)?,
                digest: row.get(2)?,
                assignment: row.get(3)?,
                workspace: row.get(4)?,
                activity: row.get(5)?,
                goal: row.get(6)?,
                node: row.get(7)?,
                target_agent: row.get(8)?,
                target_session: row.get(9)?,
                source_count: row.get(10)?,
                source_digests: row.get(11)?,
                source_kinds: row.get(12)?,
                result_digest: row.get(13)?,
                submitted_at_ms: row.get(14)?,
                source_refs_recorded: row.get(15)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    raw.into_iter().map(decode_assignment_submit_row).collect()
}

fn decode_assignment_submit_row(
    row: RawAssignmentSubmitRow,
) -> Result<AssignmentSubmitProjectionRow, OperationsError> {
    let RawAssignmentSubmitRow {
        migration,
        sequence,
        digest,
        assignment,
        workspace,
        activity,
        goal,
        node,
        target_agent,
        target_session,
        source_count,
        source_digests,
        source_kinds,
        result_digest,
        submitted_at_ms,
        source_refs_recorded,
    } = row;
    let source_count = match source_count {
        0 => 0,
        1..=32 => u8::try_from(source_count)
            .map_err(|_| OperationsError::Corrupt("effect fact source count"))?,
        _ => return Err(OperationsError::Corrupt("effect fact source count")),
    };
    let source_refs_recorded = match source_refs_recorded {
        0 => false,
        1 => true,
        _ => return Err(OperationsError::Corrupt("effect fact source refs marker")),
    };
    let (source_digests, source_kinds) = match (source_count, source_digests, source_kinds) {
        (0, None, None) => (None, None),
        (1..=32, Some(digests), Some(kinds)) => {
            let source_digests: [u8; 32] = digests
                .try_into()
                .map_err(|_| OperationsError::Corrupt("effect fact source digests"))?;
            if kinds.len() != usize::from(source_count) || kinds.iter().any(|kind| *kind > 1) {
                return Err(OperationsError::Corrupt("effect fact source kinds"));
            }
            let mut padded = [0_u8; 32];
            padded[..kinds.len()].copy_from_slice(&kinds);
            (Some(source_digests), Some(padded))
        }
        _ => return Err(OperationsError::Corrupt("effect fact source pair")),
    };
    Ok(AssignmentSubmitProjectionRow {
        migration_id: Uuid::from_bytes(wire::id(migration)?),
        source_sequence: u64::try_from(sequence)
            .map_err(|_| OperationsError::Corrupt("effect fact projection sequence"))?,
        fact_digest: digest
            .try_into()
            .map_err(|_| OperationsError::Corrupt("effect fact projection digest"))?,
        assignment_id: AgentAssignmentId::from_uuid(Uuid::from_bytes(wire::id(assignment)?)),
        workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(workspace)?)),
        activity_id: ActivityId::from_uuid(Uuid::from_bytes(wire::id(activity)?)),
        goal_id: GoalId::from_uuid(Uuid::from_bytes(wire::id(goal)?)),
        node_id: DagNodeId::from_uuid(Uuid::from_bytes(wire::id(node)?)),
        target_agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(target_agent)?)),
        target_session_id: target_session
            .map(wire::id)
            .transpose()?
            .map(|id| forge_domain::AgentSessionId::from_uuid(Uuid::from_bytes(id))),
        source_count,
        source_digests,
        source_kinds,
        source_refs_recorded,
        result_digest: result_digest
            .map(TryInto::try_into)
            .transpose()
            .map_err(|_| OperationsError::Corrupt("effect fact result digest"))?,
        submitted_at_ms,
    })
}

fn load_progress(
    connection: &Connection,
    migration_id: Uuid,
) -> Result<Option<EffectFactConsumerProgress>, OperationsError> {
    let raw: Option<(Vec<u8>, Vec<u8>, i64, i64)> = connection.query_row(
        "SELECT substr(workspace_id,1,17),substr(destination_id,1,17),last_sequence,applied_count FROM ops_effect_fact_consumers WHERE migration_id=?1",
        params![migration_id.as_bytes()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    ).optional()?;
    raw.map(|(workspace, destination, last, count)| {
        Ok(EffectFactConsumerProgress {
            migration_id,
            workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(workspace)?)),
            destination_id: Uuid::from_bytes(wire::id(destination)?),
            last_sequence: u64::try_from(last)
                .map_err(|_| OperationsError::Corrupt("effect fact last sequence"))?,
            applied_count: u64::try_from(count)
                .map_err(|_| OperationsError::Corrupt("effect fact applied count"))?,
        })
    })
    .transpose()
}

fn fact_digest(fact: &TerminalFact<'_>) -> [u8; 32] {
    match fact {
        TerminalFact::Execution(fact) => execution_digest(fact),
        TerminalFact::ChangeSet(fact) => change_set_digest(fact),
        TerminalFact::AssignmentReviewed(fact) => assignment_review_digest(fact),
        TerminalFact::AssignmentOffered(fact) => assignment_offer_digest(fact),
        TerminalFact::AssignmentSubmitted(fact) => assignment_submit_digest(fact),
    }
}

/// The projection encoding of one accepted verification run, shared by the
/// execution projection and the work-credit evidence match.
pub(crate) const ACCEPTED_DISPOSITION: i64 = 0;

fn execution_digest(fact: &ExecutionTerminalFact) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"heterarchy.orchestration.effect-fact.execution-terminal.v1\0");
    digest.update(fact.sequence.to_be_bytes());
    digest.update(fact.migration_id.as_bytes());
    digest.update(fact.destination_id.as_bytes());
    digest.update(fact.workspace_id.into_uuid().as_bytes());
    digest.update(fact.execution_id.into_uuid().as_bytes());
    digest.update(fact.activity_id.into_uuid().as_bytes());
    digest.update(fact.execution_revision.to_be_bytes());
    digest.update(encode_state(fact.execution_state).to_be_bytes());
    digest.update(fact.observed_at_ms.to_be_bytes());
    digest.update(fact.agent_id.into_uuid().as_bytes());
    digest.update(fact.agent_revision.to_be_bytes());
    update_optional_uuid(
        &mut digest,
        fact.session_id.map(forge_domain::AgentSessionId::into_uuid),
    );
    digest.update(fact.goal_id.into_uuid().as_bytes());
    digest.update(fact.goal_revision.to_be_bytes());
    digest.update(fact.dag_revision.to_be_bytes());
    digest.update(fact.node_id.into_uuid().as_bytes());
    digest.update(fact.node_revision.to_be_bytes());
    update_optional_uuid(&mut digest, fact.verification_input_id);
    update_optional_digest(&mut digest, fact.verification_input_fingerprint);
    update_optional_i64(&mut digest, fact.verification_disposition);
    update_optional_i64(&mut digest, fact.verification_output_integrity);
    update_optional_digest(&mut digest, fact.verification_output_fingerprint);
    digest.finalize().into()
}

fn change_set_digest_v1(fact: &ChangeSetTerminalFact) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"heterarchy.orchestration.effect-fact.change-set.v1\0");
    update_change_set_digest_body(&mut digest, fact);
    digest.finalize().into()
}

fn change_set_digest(fact: &ChangeSetTerminalFact) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"heterarchy.orchestration.effect-fact.change-set.v2\0");
    update_change_set_digest_body(&mut digest, fact);
    update_optional_uuid(
        &mut digest,
        fact.producer_agent_id
            .map(forge_domain::AgentInstanceId::into_uuid),
    );
    update_optional_uuid(
        &mut digest,
        fact.producer_session_id
            .map(forge_domain::AgentSessionId::into_uuid),
    );
    digest.finalize().into()
}

fn update_change_set_digest_body(digest: &mut Sha256, fact: &ChangeSetTerminalFact) {
    digest.update(fact.sequence.to_be_bytes());
    digest.update(fact.migration_id.as_bytes());
    digest.update(fact.destination_id.as_bytes());
    digest.update(fact.workspace_id.into_uuid().as_bytes());
    digest.update(fact.change_set_id.into_uuid().as_bytes());
    digest.update(fact.workspace_revision.to_be_bytes());
    digest.update(fact.activity_id.into_uuid().as_bytes());
    digest.update(fact.runtime_instance_id.into_uuid().as_bytes());
    digest.update(fact.activity_lease_revision.to_be_bytes());
    digest.update(fact.change_set_revision.to_be_bytes());
    digest.update(encode_change_set_state(fact.state).as_bytes());
    update_optional_text(digest, encode_failure_code(fact.failure_code));
    digest.update(fact.diff_id.into_uuid().as_bytes());
    digest.update(encode_diff_state(fact.diff_state).as_bytes());
    digest.update(fact.diff_revision.to_be_bytes());
    digest.update(fact.observed_at_ms.to_be_bytes());
}

fn assignment_review_digest(fact: &AssignmentReviewedFact) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"heterarchy.orchestration.effect-fact.assignment-review.v1\0");
    digest.update(fact.sequence.to_be_bytes());
    digest.update(fact.migration_id.as_bytes());
    digest.update(fact.destination_id.as_bytes());
    digest.update(fact.workspace_id.into_uuid().as_bytes());
    digest.update(fact.assignment_id.into_uuid().as_bytes());
    digest.update(fact.activity_id.into_uuid().as_bytes());
    digest.update(fact.goal_id.into_uuid().as_bytes());
    digest.update(fact.node_id.into_uuid().as_bytes());
    digest.update(fact.target_agent_id.into_uuid().as_bytes());
    update_optional_uuid(
        &mut digest,
        fact.target_session_id
            .map(forge_domain::AgentSessionId::into_uuid),
    );
    digest.update(encode_review_outcome(fact.outcome).as_bytes());
    update_optional_i64(&mut digest, fact.credit_budget.map(WorkCreditBudget::value));
    digest.update(fact.reviewer_agent_id.into_uuid().as_bytes());
    update_optional_uuid(
        &mut digest,
        fact.reviewer_session_id
            .map(forge_domain::AgentSessionId::into_uuid),
    );
    update_optional_digest(&mut digest, fact.result_digest);
    digest.update(fact.observed_at_ms.to_be_bytes());
    digest.finalize().into()
}

fn assignment_offer_digest(fact: &AssignmentOfferedFact) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"heterarchy.orchestration.effect-fact.assignment-offer.v1\0");
    digest.update(fact.sequence.to_be_bytes());
    digest.update(fact.migration_id.as_bytes());
    digest.update(fact.destination_id.as_bytes());
    digest.update(fact.workspace_id.into_uuid().as_bytes());
    digest.update(fact.assignment_id.into_uuid().as_bytes());
    digest.update(fact.activity_id.into_uuid().as_bytes());
    digest.update(fact.goal_id.into_uuid().as_bytes());
    digest.update(fact.node_id.into_uuid().as_bytes());
    digest.update(fact.target_agent_id.into_uuid().as_bytes());
    update_optional_uuid(
        &mut digest,
        fact.target_session_id
            .map(forge_domain::AgentSessionId::into_uuid),
    );
    update_optional_i64(&mut digest, fact.credit_budget.map(WorkCreditBudget::value));
    digest.update(fact.offered_at_ms.to_be_bytes());
    digest.finalize().into()
}

fn assignment_submit_digest(fact: &AssignmentSubmittedFact<'_>) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"heterarchy.orchestration.effect-fact.assignment-submitted.v1\0");
    digest.update(fact.sequence.to_be_bytes());
    digest.update(fact.migration_id.as_bytes());
    digest.update(fact.destination_id.as_bytes());
    digest.update(fact.workspace_id.into_uuid().as_bytes());
    digest.update(fact.assignment_id.into_uuid().as_bytes());
    digest.update(fact.activity_id.into_uuid().as_bytes());
    digest.update(fact.goal_id.into_uuid().as_bytes());
    digest.update(fact.node_id.into_uuid().as_bytes());
    digest.update(fact.target_agent_id.into_uuid().as_bytes());
    update_optional_uuid(
        &mut digest,
        fact.target_session_id
            .map(forge_domain::AgentSessionId::into_uuid),
    );
    digest.update(i64::from(fact.source_count).to_be_bytes());
    update_optional_digest(&mut digest, fact.source_digests);
    if let Some(kinds) = &fact.source_kinds {
        digest.update(&kinds[..fact.source_count as usize]);
    }
    update_optional_digest(&mut digest, fact.result_digest);
    digest.update(fact.submitted_at_ms.to_be_bytes());
    digest.finalize().into()
}

fn update_optional_uuid(digest: &mut Sha256, value: Option<Uuid>) {
    match value {
        Some(value) => {
            digest.update([1]);
            digest.update(value.as_bytes());
        }
        None => digest.update([0]),
    }
}
fn update_optional_digest(digest: &mut Sha256, value: Option<[u8; 32]>) {
    match value {
        Some(value) => {
            digest.update([1]);
            digest.update(value);
        }
        None => digest.update([0]),
    }
}
fn update_optional_i64(digest: &mut Sha256, value: Option<i64>) {
    match value {
        Some(value) => {
            digest.update([1]);
            digest.update(value.to_be_bytes());
        }
        None => digest.update([0]),
    }
}
fn update_optional_text(digest: &mut Sha256, value: Option<&'static str>) {
    match value {
        Some(value) => {
            digest.update([1]);
            digest.update(value.as_bytes());
            digest.update([0]);
        }
        None => digest.update([0]),
    }
}
fn signed(value: u64) -> Result<i64, OperationsError> {
    i64::try_from(value).map_err(|_| OperationsError::Bounds("effect fact counter"))
}
const fn encode_state(value: ExecutionState) -> i64 {
    match value {
        ExecutionState::Completed => 2,
        ExecutionState::Failed => 3,
        ExecutionState::TimedOut => 4,
        ExecutionState::Cancelled => 5,
        ExecutionState::Interrupted => 6,
        ExecutionState::Starting | ExecutionState::Running => -1,
    }
}
const fn encode_disposition(value: forge_domain::VerificationRunDisposition) -> i64 {
    match value {
        forge_domain::VerificationRunDisposition::Accepted => 0,
        forge_domain::VerificationRunDisposition::Rejected => 1,
        forge_domain::VerificationRunDisposition::Incomplete => 2,
        forge_domain::VerificationRunDisposition::RecoveryRequired => 3,
    }
}
const fn encode_integrity(value: forge_domain::VerificationOutputIntegrity) -> i64 {
    match value {
        forge_domain::VerificationOutputIntegrity::Complete => 0,
        forge_domain::VerificationOutputIntegrity::Gapped => 1,
        forge_domain::VerificationOutputIntegrity::Truncated => 2,
        forge_domain::VerificationOutputIntegrity::GappedAndTruncated => 3,
    }
}
const fn encode_change_set_state(value: ChangeSetState) -> &'static str {
    match value {
        ChangeSetState::Applied => "Applied",
        ChangeSetState::Failed => "Failed",
        ChangeSetState::Starting => "Starting",
    }
}
const fn encode_review_outcome(value: AssignmentReviewedOutcome) -> &'static str {
    match value {
        AssignmentReviewedOutcome::Accepted => "Accepted",
        AssignmentReviewedOutcome::Rejected => "Rejected",
    }
}
const fn encode_failure_code(value: Option<FileMutationFailureCode>) -> Option<&'static str> {
    match value {
        None => None,
        Some(code) => Some(match code {
            FileMutationFailureCode::InvalidPath => "InvalidPath",
            FileMutationFailureCode::ProtectedPath => "ProtectedPath",
            FileMutationFailureCode::PathConflict => "PathConflict",
            FileMutationFailureCode::StaleWrite => "StaleWrite",
            FileMutationFailureCode::CapacityExceeded => "CapacityExceeded",
            FileMutationFailureCode::IoFailure => "IoFailure",
        }),
    }
}
const fn encode_diff_state(value: DiffEvidenceState) -> &'static str {
    match value {
        DiffEvidenceState::Ready => "Ready",
        DiffEvidenceState::SummaryOnly => "SummaryOnly",
        DiffEvidenceState::Abandoned => "Abandoned",
        DiffEvidenceState::Intent => "Intent",
        DiffEvidenceState::PurgePending => "PurgePending",
        DiffEvidenceState::Tombstoned => "Tombstoned",
    }
}
const fn decode_execution_state_text(value: i64) -> Result<ExecutionState, OperationsError> {
    match value {
        2 => Ok(ExecutionState::Completed),
        3 => Ok(ExecutionState::Failed),
        4 => Ok(ExecutionState::TimedOut),
        5 => Ok(ExecutionState::Cancelled),
        6 => Ok(ExecutionState::Interrupted),
        _ => Err(OperationsError::Corrupt("effect fact execution state")),
    }
}
fn decode_change_set_state_text(value: &str) -> Result<ChangeSetState, OperationsError> {
    match value {
        "Applied" => Ok(ChangeSetState::Applied),
        "Failed" => Ok(ChangeSetState::Failed),
        _ => Err(OperationsError::Corrupt("effect fact change set state")),
    }
}
fn decode_failure_code_text(value: &str) -> Result<FileMutationFailureCode, OperationsError> {
    match value {
        "InvalidPath" => Ok(FileMutationFailureCode::InvalidPath),
        "ProtectedPath" => Ok(FileMutationFailureCode::ProtectedPath),
        "PathConflict" => Ok(FileMutationFailureCode::PathConflict),
        "StaleWrite" => Ok(FileMutationFailureCode::StaleWrite),
        "CapacityExceeded" => Ok(FileMutationFailureCode::CapacityExceeded),
        "IoFailure" => Ok(FileMutationFailureCode::IoFailure),
        _ => Err(OperationsError::Corrupt("effect fact failure code")),
    }
}
fn decode_diff_state_text(value: &str) -> Result<DiffEvidenceState, OperationsError> {
    match value {
        "Ready" => Ok(DiffEvidenceState::Ready),
        "SummaryOnly" => Ok(DiffEvidenceState::SummaryOnly),
        "Abandoned" => Ok(DiffEvidenceState::Abandoned),
        _ => Err(OperationsError::Corrupt("effect fact diff state")),
    }
}
fn decode_review_outcome_text(value: &str) -> Result<AssignmentReviewedOutcome, OperationsError> {
    match value {
        "Accepted" => Ok(AssignmentReviewedOutcome::Accepted),
        "Rejected" => Ok(AssignmentReviewedOutcome::Rejected),
        _ => Err(OperationsError::Corrupt("effect fact review outcome")),
    }
}
pub(crate) fn decode_credit_budget(value: i64) -> Result<WorkCreditBudget, OperationsError> {
    match value {
        1 => Ok(WorkCreditBudget::One),
        3 => Ok(WorkCreditBudget::Three),
        5 => Ok(WorkCreditBudget::Five),
        _ => Err(OperationsError::Corrupt("effect fact credit budget")),
    }
}

#[derive(Debug)]
struct RawExecutionProjectionRow {
    migration: Vec<u8>,
    sequence: i64,
    digest: Vec<u8>,
    execution: Vec<u8>,
    state: i64,
    observed_at_ms: i64,
}

fn load_execution_projection(
    connection: &Connection,
    migration_id: Uuid,
) -> Result<Vec<ExecutionTerminalProjectionRow>, OperationsError> {
    let limit = page_limit()?;
    let mut statement = connection.prepare(
        "SELECT substr(migration_id,1,17),source_sequence,substr(fact_digest,1,33),substr(execution_id,1,17),execution_state,observed_at_ms FROM ops_execution_terminal_projection WHERE migration_id=?1 ORDER BY source_sequence ASC LIMIT ?2",
    )?;
    let raw = statement
        .query_map(params![migration_id.as_bytes(), limit], |row| {
            Ok(RawExecutionProjectionRow {
                migration: row.get(0)?,
                sequence: row.get(1)?,
                digest: row.get(2)?,
                execution: row.get(3)?,
                state: row.get(4)?,
                observed_at_ms: row.get(5)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    raw.into_iter()
        .map(|row| {
            let RawExecutionProjectionRow {
                migration,
                sequence,
                digest,
                execution,
                state,
                observed_at_ms,
            } = row;
            Ok(ExecutionTerminalProjectionRow {
                migration_id: Uuid::from_bytes(wire::id(migration)?),
                source_sequence: u64::try_from(sequence)
                    .map_err(|_| OperationsError::Corrupt("effect fact projection sequence"))?,
                fact_digest: digest
                    .try_into()
                    .map_err(|_| OperationsError::Corrupt("effect fact projection digest"))?,
                execution_id: ExecutionId::from_uuid(Uuid::from_bytes(wire::id(execution)?)),
                execution_state: decode_execution_state_text(state)?,
                observed_at_ms,
            })
        })
        .collect()
}

#[derive(Debug)]
struct RawChangeSetProjectionRow {
    migration: Vec<u8>,
    sequence: i64,
    digest: Vec<u8>,
    change_set: Vec<u8>,
    workspace: Vec<u8>,
    activity: Vec<u8>,
    change_set_revision: i64,
    state: String,
    failure_code: Option<String>,
    diff: Vec<u8>,
    diff_state: String,
    diff_revision: i64,
    observed_at_ms: i64,
    producer_agent: Option<Vec<u8>>,
    producer_session: Option<Vec<u8>>,
    digest_version: i64,
}

fn load_change_set_projection(
    connection: &Connection,
    migration_id: Uuid,
) -> Result<Vec<ChangeSetProjectionRow>, OperationsError> {
    let limit = page_limit()?;
    let mut statement = connection.prepare(
        "SELECT substr(migration_id,1,17),source_sequence,substr(fact_digest,1,33),substr(change_set_id,1,17),substr(workspace_id,1,17),substr(activity_id,1,17),change_set_revision,state,failure_code,substr(diff_id,1,17),diff_state,diff_revision,observed_at_ms,substr(producer_agent_id,1,17),substr(producer_session_id,1,17),digest_version FROM ops_change_set_projection WHERE migration_id=?1 ORDER BY source_sequence ASC LIMIT ?2",
    )?;
    let raw = statement
        .query_map(params![migration_id.as_bytes(), limit], |row| {
            Ok(RawChangeSetProjectionRow {
                migration: row.get(0)?,
                sequence: row.get(1)?,
                digest: row.get(2)?,
                change_set: row.get(3)?,
                workspace: row.get(4)?,
                activity: row.get(5)?,
                change_set_revision: row.get(6)?,
                state: row.get(7)?,
                failure_code: row.get(8)?,
                diff: row.get(9)?,
                diff_state: row.get(10)?,
                diff_revision: row.get(11)?,
                observed_at_ms: row.get(12)?,
                producer_agent: row.get(13)?,
                producer_session: row.get(14)?,
                digest_version: row.get(15)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    raw.into_iter()
        .map(|row| {
            let RawChangeSetProjectionRow {
                migration,
                sequence,
                digest,
                change_set,
                workspace,
                activity,
                change_set_revision,
                state,
                failure_code,
                diff,
                diff_state,
                diff_revision,
                observed_at_ms,
                producer_agent,
                producer_session,
                digest_version,
            } = row;
            if digest_version != 0 && digest_version != 2 {
                return Err(OperationsError::Corrupt(
                    "effect fact change set digest version",
                ));
            }
            if producer_agent.is_none() && producer_session.is_some() {
                return Err(OperationsError::Corrupt("effect fact producer pair"));
            }
            Ok(ChangeSetProjectionRow {
                migration_id: Uuid::from_bytes(wire::id(migration)?),
                source_sequence: u64::try_from(sequence)
                    .map_err(|_| OperationsError::Corrupt("effect fact projection sequence"))?,
                fact_digest: digest
                    .try_into()
                    .map_err(|_| OperationsError::Corrupt("effect fact projection digest"))?,
                change_set_id: ChangeSetId::from_uuid(Uuid::from_bytes(wire::id(change_set)?)),
                workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(workspace)?)),
                activity_id: ActivityId::from_uuid(Uuid::from_bytes(wire::id(activity)?)),
                change_set_revision,
                state: decode_change_set_state_text(&state)?,
                failure_code: failure_code
                    .map(|code| decode_failure_code_text(&code))
                    .transpose()?,
                diff_id: DiffId::from_uuid(Uuid::from_bytes(wire::id(diff)?)),
                diff_state: decode_diff_state_text(&diff_state)?,
                diff_revision,
                observed_at_ms,
                producer_agent_id: producer_agent
                    .map(wire::id)
                    .transpose()?
                    .map(|id| AgentInstanceId::from_uuid(Uuid::from_bytes(id))),
                producer_session_id: producer_session
                    .map(wire::id)
                    .transpose()?
                    .map(|id| AgentSessionId::from_uuid(Uuid::from_bytes(id))),
            })
        })
        .collect()
}

fn page_limit() -> Result<i64, OperationsError> {
    i64::try_from(MAX_PAGE_SIZE).map_err(|_| OperationsError::Bounds("effect fact page"))
}

#[derive(Debug)]
struct RawAssignmentSubmitRow {
    migration: Vec<u8>,
    sequence: i64,
    digest: Vec<u8>,
    assignment: Vec<u8>,
    workspace: Vec<u8>,
    activity: Vec<u8>,
    goal: Vec<u8>,
    node: Vec<u8>,
    target_agent: Vec<u8>,
    target_session: Option<Vec<u8>>,
    source_count: i64,
    source_digests: Option<Vec<u8>>,
    source_kinds: Option<Vec<u8>>,
    result_digest: Option<Vec<u8>>,
    submitted_at_ms: i64,
    source_refs_recorded: i64,
}

struct RawAssignmentOfferRow {
    migration: Vec<u8>,
    sequence: i64,
    digest: Vec<u8>,
    assignment: Vec<u8>,
    workspace: Vec<u8>,
    activity: Vec<u8>,
    goal: Vec<u8>,
    node: Vec<u8>,
    target_agent: Vec<u8>,
    target_session: Option<Vec<u8>>,
    credit_budget: Option<i64>,
    offered_at_ms: i64,
}

fn load_assignment_offer_projection(
    connection: &Connection,
    migration_id: Uuid,
) -> Result<Vec<AssignmentOfferProjectionRow>, OperationsError> {
    let limit = page_limit()?;
    let mut statement = connection.prepare(
        "SELECT substr(migration_id,1,17),source_sequence,substr(fact_digest,1,33),substr(assignment_id,1,17),substr(workspace_id,1,17),substr(activity_id,1,17),substr(goal_id,1,17),substr(node_id,1,17),substr(target_agent_id,1,17),substr(target_session_id,1,17),credit_budget,offered_at_ms FROM ops_assignment_offer_projection WHERE migration_id=?1 ORDER BY source_sequence ASC LIMIT ?2",
    )?;
    let raw = statement
        .query_map(params![migration_id.as_bytes(), limit], |row| {
            Ok(RawAssignmentOfferRow {
                migration: row.get(0)?,
                sequence: row.get(1)?,
                digest: row.get(2)?,
                assignment: row.get(3)?,
                workspace: row.get(4)?,
                activity: row.get(5)?,
                goal: row.get(6)?,
                node: row.get(7)?,
                target_agent: row.get(8)?,
                target_session: row.get(9)?,
                credit_budget: row.get(10)?,
                offered_at_ms: row.get(11)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    raw.into_iter()
        .map(|row| {
            let RawAssignmentOfferRow {
                migration,
                sequence,
                digest,
                assignment,
                workspace,
                activity,
                goal,
                node,
                target_agent,
                target_session,
                credit_budget,
                offered_at_ms,
            } = row;
            Ok(AssignmentOfferProjectionRow {
                migration_id: Uuid::from_bytes(wire::id(migration)?),
                source_sequence: u64::try_from(sequence)
                    .map_err(|_| OperationsError::Corrupt("effect fact projection sequence"))?,
                fact_digest: digest
                    .try_into()
                    .map_err(|_| OperationsError::Corrupt("effect fact projection digest"))?,
                assignment_id: AgentAssignmentId::from_uuid(Uuid::from_bytes(wire::id(
                    assignment,
                )?)),
                workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(workspace)?)),
                activity_id: ActivityId::from_uuid(Uuid::from_bytes(wire::id(activity)?)),
                goal_id: GoalId::from_uuid(Uuid::from_bytes(wire::id(goal)?)),
                node_id: DagNodeId::from_uuid(Uuid::from_bytes(wire::id(node)?)),
                target_agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(
                    target_agent,
                )?)),
                target_session_id: target_session
                    .map(|value| {
                        Ok::<_, OperationsError>(AgentSessionId::from_uuid(Uuid::from_bytes(
                            wire::id(value)?,
                        )))
                    })
                    .transpose()?,
                credit_budget: credit_budget.map(decode_credit_budget).transpose()?,
                offered_at_ms,
            })
        })
        .collect()
}

#[derive(Debug)]
struct RawAssignmentReviewRow {
    migration: Vec<u8>,
    sequence: i64,
    digest: Vec<u8>,
    assignment: Vec<u8>,
    workspace: Vec<u8>,
    activity: Vec<u8>,
    goal: Vec<u8>,
    node: Vec<u8>,
    target_agent: Vec<u8>,
    target_session: Option<Vec<u8>>,
    outcome: String,
    credit_budget: Option<i64>,
    reviewer_agent: Vec<u8>,
    result_digest: Option<Vec<u8>>,
    observed_at_ms: i64,
}

fn load_assignment_review_projection(
    connection: &Connection,
    migration_id: Uuid,
) -> Result<Vec<AssignmentReviewProjectionRow>, OperationsError> {
    let limit = page_limit()?;
    let mut statement = connection.prepare(
        "SELECT substr(migration_id,1,17),source_sequence,substr(fact_digest,1,33),substr(assignment_id,1,17),substr(workspace_id,1,17),substr(activity_id,1,17),substr(goal_id,1,17),substr(node_id,1,17),substr(target_agent_id,1,17),substr(target_session_id,1,17),outcome,credit_budget,substr(reviewer_agent_id,1,17),substr(result_digest,1,33),observed_at_ms FROM ops_assignment_review_projection WHERE migration_id=?1 ORDER BY source_sequence ASC LIMIT ?2",
    )?;
    let raw = statement
        .query_map(params![migration_id.as_bytes(), limit], |row| {
            Ok(RawAssignmentReviewRow {
                migration: row.get(0)?,
                sequence: row.get(1)?,
                digest: row.get(2)?,
                assignment: row.get(3)?,
                workspace: row.get(4)?,
                activity: row.get(5)?,
                goal: row.get(6)?,
                node: row.get(7)?,
                target_agent: row.get(8)?,
                target_session: row.get(9)?,
                outcome: row.get(10)?,
                credit_budget: row.get(11)?,
                reviewer_agent: row.get(12)?,
                result_digest: row.get(13)?,
                observed_at_ms: row.get(14)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    raw.into_iter()
        .map(|row| {
            let RawAssignmentReviewRow {
                migration,
                sequence,
                digest,
                assignment,
                workspace,
                activity,
                goal,
                node,
                target_agent,
                target_session,
                outcome,
                credit_budget,
                reviewer_agent,
                result_digest,
                observed_at_ms,
            } = row;
            Ok(AssignmentReviewProjectionRow {
                migration_id: Uuid::from_bytes(wire::id(migration)?),
                source_sequence: u64::try_from(sequence)
                    .map_err(|_| OperationsError::Corrupt("effect fact projection sequence"))?,
                fact_digest: digest
                    .try_into()
                    .map_err(|_| OperationsError::Corrupt("effect fact projection digest"))?,
                assignment_id: AgentAssignmentId::from_uuid(Uuid::from_bytes(wire::id(
                    assignment,
                )?)),
                workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(workspace)?)),
                activity_id: ActivityId::from_uuid(Uuid::from_bytes(wire::id(activity)?)),
                goal_id: GoalId::from_uuid(Uuid::from_bytes(wire::id(goal)?)),
                node_id: DagNodeId::from_uuid(Uuid::from_bytes(wire::id(node)?)),
                target_agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(
                    target_agent,
                )?)),
                target_session_id: target_session
                    .map(|value| {
                        Ok::<_, OperationsError>(AgentSessionId::from_uuid(Uuid::from_bytes(
                            wire::id(value)?,
                        )))
                    })
                    .transpose()?,
                outcome: decode_review_outcome_text(&outcome)?,
                credit_budget: credit_budget.map(decode_credit_budget).transpose()?,
                reviewer_agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(
                    reviewer_agent,
                )?)),
                result_digest: result_digest
                    .map(|value| {
                        value
                            .try_into()
                            .map_err(|_| OperationsError::Corrupt("effect fact projection digest"))
                    })
                    .transpose()?,
                observed_at_ms,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DigestFixture {
        root: std::path::PathBuf,
        catalog: crate::OperationsCatalog,
        migration: Uuid,
        workspace: WorkspaceId,
        first: TerminalFact<'static>,
        second: TerminalFact<'static>,
    }

    fn fact(
        sequence: u64,
        migration: Uuid,
        workspace: WorkspaceId,
        destination: Uuid,
        execution: u128,
    ) -> TerminalFact<'static> {
        TerminalFact::Execution(ExecutionTerminalFact {
            sequence,
            migration_id: migration,
            destination_id: destination,
            workspace_id: workspace,
            execution_id: ExecutionId::from_uuid(Uuid::from_u128(execution)),
            activity_id: ActivityId::from_uuid(Uuid::from_u128(0xE101)),
            execution_revision: 3,
            execution_state: ExecutionState::Completed,
            observed_at_ms: 42,
            agent_id: AgentInstanceId::from_uuid(Uuid::from_u128(0xE102)),
            agent_revision: 2,
            session_id: Some(AgentSessionId::from_uuid(Uuid::from_u128(0xE103))),
            goal_id: GoalId::from_uuid(Uuid::from_u128(0xE104)),
            goal_revision: 1,
            dag_revision: 1,
            node_id: DagNodeId::from_uuid(Uuid::from_u128(0xE105)),
            node_revision: 2,
            verification_input_id: None,
            verification_input_fingerprint: None,
            verification_disposition: None,
            verification_output_integrity: None,
            verification_output_fingerprint: None,
        })
    }

    fn change_set_fact(
        sequence: u64,
        migration: Uuid,
        workspace: WorkspaceId,
        destination: Uuid,
        change_set: u128,
    ) -> TerminalFact<'static> {
        change_set_fact_with(
            sequence,
            migration,
            workspace,
            destination,
            change_set,
            ChangeSetState::Applied,
            None,
        )
    }

    fn no_producer_change_set_fact(
        sequence: u64,
        migration: Uuid,
        workspace: WorkspaceId,
        destination: Uuid,
        change_set: u128,
    ) -> Result<TerminalFact<'static>, Box<dyn std::error::Error>> {
        let TerminalFact::ChangeSet(mut fact) =
            change_set_fact(sequence, migration, workspace, destination, change_set)
        else {
            return Err("expected change-set fact".into());
        };
        fact.producer_agent_id = None;
        fact.producer_session_id = None;
        Ok(TerminalFact::ChangeSet(fact))
    }

    fn failed_change_set_fact(
        sequence: u64,
        migration: Uuid,
        workspace: WorkspaceId,
        destination: Uuid,
        change_set: u128,
    ) -> TerminalFact<'static> {
        change_set_fact_with(
            sequence,
            migration,
            workspace,
            destination,
            change_set,
            ChangeSetState::Failed,
            Some(FileMutationFailureCode::IoFailure),
        )
    }

    fn change_set_fact_with(
        sequence: u64,
        migration: Uuid,
        workspace: WorkspaceId,
        destination: Uuid,
        change_set: u128,
        state: ChangeSetState,
        failure_code: Option<FileMutationFailureCode>,
    ) -> TerminalFact<'static> {
        TerminalFact::ChangeSet(ChangeSetTerminalFact {
            sequence,
            migration_id: migration,
            destination_id: destination,
            workspace_id: workspace,
            change_set_id: ChangeSetId::from_uuid(Uuid::from_u128(change_set)),
            workspace_revision: 4,
            activity_id: ActivityId::from_uuid(Uuid::from_u128(0xE101)),
            runtime_instance_id: RuntimeInstanceId::from_uuid(Uuid::from_u128(0xE106)),
            activity_lease_revision: 5,
            change_set_revision: 2,
            state,
            failure_code,
            diff_id: DiffId::from_uuid(Uuid::from_u128(change_set + 0x100)),
            diff_state: DiffEvidenceState::Ready,
            diff_revision: 1,
            observed_at_ms: 43,
            producer_agent_id: Some(AgentInstanceId::from_uuid(Uuid::from_u128(
                change_set + 0x200,
            ))),
            producer_session_id: Some(AgentSessionId::from_uuid(Uuid::from_u128(
                change_set + 0x201,
            ))),
        })
    }

    fn execution_fact_with_verification(
        sequence: u64,
        migration: Uuid,
        workspace: WorkspaceId,
        destination: Uuid,
        execution: u128,
        verified: bool,
    ) -> TerminalFact<'static> {
        let (input_id, input_fingerprint, disposition, integrity, output_fingerprint) = if verified
        {
            (
                Some(Uuid::from_u128(execution + 0x1000)),
                Some([7_u8; 32]),
                Some(ACCEPTED_DISPOSITION),
                Some(0_i64),
                Some([8_u8; 32]),
            )
        } else {
            (None, None, None, None, None)
        };
        TerminalFact::Execution(ExecutionTerminalFact {
            sequence,
            migration_id: migration,
            destination_id: destination,
            workspace_id: workspace,
            execution_id: ExecutionId::from_uuid(Uuid::from_u128(execution)),
            activity_id: ActivityId::from_uuid(Uuid::from_u128(0xE101)),
            execution_revision: 3,
            execution_state: ExecutionState::Completed,
            observed_at_ms: 42,
            agent_id: AgentInstanceId::from_uuid(Uuid::from_u128(0xE102)),
            agent_revision: 2,
            session_id: Some(AgentSessionId::from_uuid(Uuid::from_u128(0xE103))),
            goal_id: GoalId::from_uuid(Uuid::from_u128(0xE104)),
            goal_revision: 1,
            dag_revision: 1,
            node_id: DagNodeId::from_uuid(Uuid::from_u128(0xE105)),
            node_revision: 2,
            verification_input_id: input_id,
            verification_input_fingerprint: input_fingerprint,
            verification_disposition: disposition,
            verification_output_integrity: integrity,
            verification_output_fingerprint: output_fingerprint,
        })
    }

    #[derive(Debug, Clone, Copy)]
    struct OfferShape {
        assignment: u128,
        activity: u128,
        agent: u128,
        offered_at_ms: i64,
    }

    fn offer_fact(
        sequence: u64,
        migration: Uuid,
        workspace: WorkspaceId,
        destination: Uuid,
        shape: OfferShape,
    ) -> TerminalFact<'static> {
        TerminalFact::AssignmentOffered(AssignmentOfferedFact {
            sequence,
            migration_id: migration,
            destination_id: destination,
            workspace_id: workspace,
            assignment_id: AgentAssignmentId::from_uuid(Uuid::from_u128(shape.assignment)),
            activity_id: ActivityId::from_uuid(Uuid::from_u128(shape.activity)),
            goal_id: GoalId::from_uuid(Uuid::from_u128(shape.assignment + 1)),
            node_id: DagNodeId::from_uuid(Uuid::from_u128(shape.assignment + 2)),
            target_agent_id: AgentInstanceId::from_uuid(Uuid::from_u128(shape.agent)),
            target_session_id: Some(AgentSessionId::from_uuid(Uuid::from_u128(shape.agent + 1))),
            credit_budget: Some(WorkCreditBudget::Three),
            offered_at_ms: shape.offered_at_ms,
        })
    }

    #[derive(Debug, Clone, Copy)]
    struct SubmitShape {
        assignment: u128,
        agent: u128,
        submitted_at_ms: i64,
    }

    fn submit_fact(
        sequence: u64,
        migration: Uuid,
        workspace: WorkspaceId,
        destination: Uuid,
        shape: SubmitShape,
    ) -> TerminalFact<'static> {
        TerminalFact::AssignmentSubmitted(AssignmentSubmittedFact {
            sequence,
            migration_id: migration,
            destination_id: destination,
            workspace_id: workspace,
            assignment_id: AgentAssignmentId::from_uuid(Uuid::from_u128(shape.assignment)),
            activity_id: ActivityId::from_uuid(Uuid::from_u128(0xE101)),
            goal_id: GoalId::from_uuid(Uuid::from_u128(shape.assignment + 1)),
            node_id: DagNodeId::from_uuid(Uuid::from_u128(shape.assignment + 2)),
            target_agent_id: AgentInstanceId::from_uuid(Uuid::from_u128(shape.agent)),
            target_session_id: Some(AgentSessionId::from_uuid(Uuid::from_u128(shape.agent + 1))),
            source_count: 2,
            source_digests: Some([0xAA; 32]),
            source_kinds: {
                let mut kinds = [0u8; 32];
                kinds[..2].copy_from_slice(&[1, 1]);
                Some(kinds)
            },
            source_refs: None,
            result_digest: Some([0xBB; 32]),
            submitted_at_ms: shape.submitted_at_ms,
        })
    }

    fn review_fact(
        sequence: u64,
        migration: Uuid,
        workspace: WorkspaceId,
        destination: Uuid,
        assignment: u128,
        outcome: AssignmentReviewedOutcome,
        budget: Option<WorkCreditBudget>,
    ) -> TerminalFact<'static> {
        review_fact_with_reviewer_agent(
            sequence,
            migration,
            workspace,
            destination,
            assignment,
            outcome,
            budget,
            AgentInstanceId::from_uuid(Uuid::from_u128(0xE107)),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn review_fact_with_reviewer_agent(
        sequence: u64,
        migration: Uuid,
        workspace: WorkspaceId,
        destination: Uuid,
        assignment: u128,
        outcome: AssignmentReviewedOutcome,
        budget: Option<WorkCreditBudget>,
        reviewer_agent_id: AgentInstanceId,
    ) -> TerminalFact<'static> {
        TerminalFact::AssignmentReviewed(AssignmentReviewedFact {
            sequence,
            migration_id: migration,
            destination_id: destination,
            workspace_id: workspace,
            assignment_id: AgentAssignmentId::from_uuid(Uuid::from_u128(assignment)),
            activity_id: ActivityId::from_uuid(Uuid::from_u128(0xE101)),
            goal_id: GoalId::from_uuid(Uuid::from_u128(0xE104)),
            node_id: DagNodeId::from_uuid(Uuid::from_u128(0xE105)),
            target_agent_id: AgentInstanceId::from_uuid(Uuid::from_u128(0xE102)),
            target_session_id: Some(AgentSessionId::from_uuid(Uuid::from_u128(0xE103))),
            outcome,
            credit_budget: budget,
            reviewer_agent_id,
            reviewer_session_id: Some(AgentSessionId::from_uuid(Uuid::from_u128(0xE108))),
            result_digest: None,
            observed_at_ms: 44,
        })
    }

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
            "orchestration-effect-fact-{name}-{}",
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

    fn insert_legacy_change_set_projection(
        connection: &Connection,
        fact: &ChangeSetTerminalFact,
        digest: [u8; 32],
    ) -> Result<(), Box<dyn std::error::Error>> {
        connection.execute(
            "INSERT INTO ops_change_set_projection(migration_id,source_sequence,fact_digest,change_set_id,workspace_id,activity_id,change_set_revision,state,failure_code,diff_id,diff_state,diff_revision,observed_at_ms,producer_agent_id,producer_session_id) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
            params![
                fact.migration_id.as_bytes(),
                signed(fact.sequence)?,
                digest.as_slice(),
                fact.change_set_id.into_uuid().as_bytes(),
                fact.workspace_id.into_uuid().as_bytes(),
                fact.activity_id.into_uuid().as_bytes(),
                fact.change_set_revision,
                encode_change_set_state(fact.state),
                encode_failure_code(fact.failure_code),
                fact.diff_id.into_uuid().as_bytes(),
                encode_diff_state(fact.diff_state),
                fact.diff_revision,
                fact.observed_at_ms,
                Option::<Vec<u8>>::None,
                Option::<Vec<u8>>::None,
            ],
        )?;
        Ok(())
    }

    // Captured from the pre-attribution `change_set_digest` implementation
    // in the historical W2 source snapshot; this anchors the v1 wire hash
    // independently of the v2 producer fields.
    const HISTORICAL_V1_FIRST_DIGEST: [u8; 32] = [
        0x8c, 0xb5, 0xd6, 0xe8, 0xb3, 0x17, 0xb5, 0xe8, 0x74, 0xf5, 0xfc, 0x88, 0x83, 0xae, 0x18,
        0x55, 0xc4, 0xce, 0xd8, 0xc1, 0xa4, 0x69, 0xf9, 0xfe, 0xf5, 0xd4, 0x33, 0x54, 0x5e, 0x8d,
        0x37, 0xcc,
    ];

    fn reopen_v17_digest_fixture(name: &str) -> Result<DigestFixture, Box<dyn std::error::Error>> {
        let (root, catalog, migration, workspace) = catalog_fixture(name)?;
        let destination = catalog.destination_id();
        let path = root.join("catalog.sqlite3");
        drop(catalog);
        let connection = Connection::open(&path)?;
        crate::workspace_participation::drop_schema_for_legacy_fixture(&connection)?;
        connection.execute_batch("DROP TABLE ops_change_set_projection;")?;
        connection.execute_batch(&crate::effect_fact::change_sets_sql_v17())?;
        connection.execute(
            "INSERT INTO ops_effect_fact_consumers(migration_id,workspace_id,destination_id,last_sequence,applied_count) VALUES(?1,?2,?3,2,2)",
            params![
                migration.as_bytes(),
                workspace.into_uuid().as_bytes(),
                destination.as_bytes(),
            ],
        )?;
        let first = no_producer_change_set_fact(1, migration, workspace, destination, 0xE901)?;
        let second = no_producer_change_set_fact(2, migration, workspace, destination, 0xE902)?;
        let TerminalFact::ChangeSet(first_row) = &first else {
            return Err("expected first change-set fact".into());
        };
        let TerminalFact::ChangeSet(second_row) = &second else {
            return Err("expected second change-set fact".into());
        };
        assert_eq!(change_set_digest_v1(first_row), HISTORICAL_V1_FIRST_DIGEST);
        insert_legacy_change_set_projection(
            &connection,
            first_row,
            change_set_digest_v1(first_row),
        )?;
        insert_legacy_change_set_projection(
            &connection,
            second_row,
            change_set_digest(second_row),
        )?;
        connection.pragma_update(None, "user_version", 17_i64)?;
        drop(connection);
        let catalog = crate::OperationsCatalog::open(&path, destination)?;
        Ok(DigestFixture {
            root,
            catalog,
            migration,
            workspace,
            first,
            second,
        })
    }

    fn assert_change_set_replay_conflict(
        catalog: &mut crate::OperationsCatalog,
        destination: Uuid,
        migration: Uuid,
        fact: &TerminalFact<'_>,
    ) {
        assert!(matches!(
            apply_page(
                &mut catalog.connection,
                destination,
                migration,
                std::slice::from_ref(fact),
            ),
            Err(OperationsError::Conflict("effect fact replay"))
        ));
    }

    #[test]
    fn catalog_v9_migrates_additively_without_fabricating_effect_progress()
    -> Result<(), Box<dyn std::error::Error>> {
        let (root, catalog, migration, workspace) = catalog_fixture("v9-migration")?;
        let destination = catalog.destination_id();
        let path = root.join("catalog.sqlite3");
        assert_eq!(
            catalog
                .progress(migration)?
                .map(|progress| progress.workspace_id()),
            Some(workspace)
        );
        drop(catalog);

        let connection = Connection::open(&path)?;
        connection.pragma_update(None, "foreign_keys", true)?;
        crate::workspace_participation::drop_schema_for_legacy_fixture(&connection)?;
        connection.execute_batch("DROP TABLE ops_admission_records; DROP TABLE ops_approval_requests; DROP TABLE ops_approval_policy; DROP TABLE ops_issuer_config; DROP TABLE ops_leadership_confirm_intents; DROP TABLE ops_ballots; DROP TABLE ops_election_electorate; DROP TABLE ops_leadership_elections; DROP TABLE ops_leadership_receipts; DROP TABLE ops_leadership; DROP TABLE ops_work_credit_entries; DROP TABLE ops_assignment_submit_source_refs; DROP TABLE ops_assignment_submit_projection; DROP TABLE ops_assignment_offer_projection; DROP TABLE ops_assignment_review_projection; DROP TABLE ops_change_set_projection;")?;
        connection.pragma_update(None, "user_version", 9_i64)?;
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
        assert_eq!(import.destination_id(), destination);
        assert_eq!(import.phase(), crate::ImportPhase::Verified);
        assert!(
            migrated.effect_fact_progress(migration)?.is_none(),
            "legacy migration must not fabricate consumer acknowledgement"
        );
        assert!(
            migrated.change_set_projection(migration)?.is_empty(),
            "legacy migration must recreate the change set projection additively"
        );
        let source_ref_count: i64 = migrated.connection.query_row(
            "SELECT COUNT(*) FROM ops_assignment_submit_source_refs",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(
            source_ref_count, 0,
            "legacy migrations fabricate no source refs"
        );
        drop(migrated);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    fn reject_partial_producer(
        fixture: &mut DigestFixture,
    ) -> Result<(), Box<dyn std::error::Error>> {
        fixture.catalog.connection.execute(
            "UPDATE ops_change_set_projection SET producer_agent_id=NULL,producer_session_id=?1 WHERE migration_id=?2 AND source_sequence=2",
            params![Uuid::from_u128(0xF002).as_bytes(), fixture.migration.as_bytes()],
        )?;
        assert!(matches!(
            load_change_set_projection(&fixture.catalog.connection, fixture.migration),
            Err(OperationsError::Corrupt("effect fact producer pair"))
        ));
        let destination = fixture.catalog.destination_id();
        assert!(matches!(
            apply_page(
                &mut fixture.catalog.connection,
                destination,
                fixture.migration,
                std::slice::from_ref(&fixture.second)
            ),
            Err(OperationsError::Corrupt("effect fact producer pair"))
        ));
        let TerminalFact::ChangeSet(mut partial) = fixture.second else {
            return Err("expected change-set fixture".into());
        };
        partial.producer_session_id = Some(AgentSessionId::from_uuid(Uuid::from_u128(0xF002)));
        assert!(matches!(
            apply_page(
                &mut fixture.catalog.connection,
                destination,
                fixture.migration,
                &[TerminalFact::ChangeSet(partial)]
            ),
            Err(OperationsError::Conflict("effect fact producer pair"))
        ));
        fixture.catalog.connection.execute(
            "UPDATE ops_change_set_projection SET producer_session_id=NULL WHERE migration_id=?1 AND source_sequence=2",
            params![fixture.migration.as_bytes()],
        )?;
        Ok(())
    }

    #[test]
    fn change_set_digest_marker_upgrades_only_from_source_witnessed_replay()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = reopen_v17_digest_fixture("change-set-digest-marker")?;
        let destination = fixture.catalog.destination_id();
        let _ = apply_page(
            &mut fixture.catalog.connection,
            destination,
            fixture.migration,
            std::slice::from_ref(&fixture.first),
        )?;
        let progress = apply_page(
            &mut fixture.catalog.connection,
            destination,
            fixture.migration,
            std::slice::from_ref(&fixture.second),
        )?;
        assert_eq!(progress.last_sequence(), 2);
        for sequence in [1_i64, 2_i64] {
            let stored = fixture.catalog.connection.query_row(
                "SELECT substr(fact_digest,1,33),digest_version,producer_agent_id,producer_session_id FROM ops_change_set_projection WHERE migration_id=?1 AND source_sequence=?2",
                params![fixture.migration.as_bytes(), sequence],
                |row| Ok(StoredChangeSetReplay { digest: row.get(0)?, digest_version: row.get(1)?, producer_agent: row.get(2)?, producer_session: row.get(3)? }),
            )?;
            assert_eq!(stored.digest_version, 2);
            assert_eq!(stored.digest.len(), 32);
            assert_eq!(stored.producer_agent, None);
            assert_eq!(stored.producer_session, None);
        }
        let TerminalFact::ChangeSet(mut changed_source) = fixture.first else {
            return Err("expected first change-set fact".into());
        };
        changed_source.diff_revision += 1;
        assert_change_set_replay_conflict(
            &mut fixture.catalog,
            destination,
            fixture.migration,
            &TerminalFact::ChangeSet(changed_source),
        );
        let attributed =
            change_set_fact(1, fixture.migration, fixture.workspace, destination, 0xE901);
        assert_change_set_replay_conflict(
            &mut fixture.catalog,
            destination,
            fixture.migration,
            &attributed,
        );

        fixture.catalog.connection.execute(
            "UPDATE ops_change_set_projection SET producer_agent_id=?1,digest_version=0 WHERE migration_id=?2 AND source_sequence=2",
            params![
                Uuid::from_u128(0xF001).as_bytes(),
                fixture.migration.as_bytes(),
            ],
        )?;
        assert_change_set_replay_conflict(
            &mut fixture.catalog,
            destination,
            fixture.migration,
            &fixture.second,
        );
        let (version, producer_agent): (i64, Option<Vec<u8>>) =
            fixture.catalog.connection.query_row(
                "SELECT digest_version,producer_agent_id FROM ops_change_set_projection WHERE migration_id=?1 AND source_sequence=2",
                params![fixture.migration.as_bytes()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
        assert_eq!(version, 0);
        assert_eq!(
            producer_agent,
            Some(Uuid::from_u128(0xF001).as_bytes().to_vec())
        );
        reject_partial_producer(&mut fixture)?;
        fixture.catalog.connection.execute(
            "UPDATE ops_change_set_projection SET producer_agent_id=NULL WHERE migration_id=?1 AND source_sequence=2",
            params![fixture.migration.as_bytes()],
        )?;
        fixture.catalog.connection.execute(
            "UPDATE ops_change_set_projection SET fact_digest=?1,digest_version=0 WHERE migration_id=?2 AND source_sequence=2",
            params![[0xA5_u8; 32].as_slice(), fixture.migration.as_bytes()],
        )?;
        assert_change_set_replay_conflict(
            &mut fixture.catalog,
            destination,
            fixture.migration,
            &fixture.second,
        );
        let (digest, version): (Vec<u8>, i64) = fixture.catalog.connection.query_row(
            "SELECT substr(fact_digest,1,33),digest_version FROM ops_change_set_projection WHERE migration_id=?1 AND source_sequence=2",
            params![fixture.migration.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(digest, vec![0xA5; 32]);
        assert_eq!(version, 0);
        drop(fixture.catalog);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn page_apply_is_atomic_replay_safe_and_gap_fenced() -> Result<(), Box<dyn std::error::Error>> {
        let (root, mut catalog, migration, workspace) = catalog_fixture("apply")?;
        let destination = catalog.destination_id();
        let first = fact(1, migration, workspace, destination, 0xE201);
        let TerminalFact::Execution(valid) = first else {
            return Err("expected execution candidate".into());
        };
        let mut nil_identity = valid;
        nil_identity.execution_id = ExecutionId::from_uuid(Uuid::nil());
        let mut missing_output = valid;
        missing_output.verification_input_id = Some(Uuid::from_u128(0xE299));
        missing_output.verification_input_fingerprint = Some([9; 32]);
        missing_output.verification_disposition = Some(0);
        for invalid in [nil_identity, missing_output] {
            assert!(
                catalog
                    .apply_effect_fact_inputs(migration, &[TerminalFact::Execution(invalid)])
                    .is_err()
            );
            assert_eq!(catalog.effect_fact_progress(migration)?, None);
            assert!(catalog.execution_terminal_projection(migration)?.is_empty());
        }
        assert!(catalog.apply_effect_fact_inputs(Uuid::nil(), &[]).is_err());
        assert_eq!(
            catalog
                .effect_fact_source_cursor(workspace)?
                .map(EffectFactConsumerProgress::last_sequence),
            Some(0)
        );
        let progress = catalog.apply_effect_fact_inputs(migration, std::slice::from_ref(&first))?;
        assert_eq!(progress.last_sequence(), 1);
        assert_eq!(progress.applied_count(), 1);
        let replay = catalog.apply_effect_fact_inputs(migration, std::slice::from_ref(&first))?;
        assert_eq!(replay, progress);

        let second = fact(2, migration, workspace, destination, 0xE202);
        let fourth = fact(4, migration, workspace, destination, 0xE204);
        assert!(matches!(
            catalog.apply_effect_fact_inputs(migration, &[second, fourth]),
            Err(OperationsError::ResyncRequired("effect fact sequence gap"))
        ));
        assert_eq!(catalog.effect_fact_progress(migration)?, Some(progress));
        let count: i64 = catalog.connection.query_row(
            "SELECT COUNT(*) FROM ops_execution_terminal_projection WHERE migration_id=?1",
            params![migration.as_bytes()],
            |row| row.get(0),
        )?;
        assert_eq!(
            count, 1,
            "gap failure must roll back earlier page rows and cursor"
        );

        let next = catalog.apply_effect_fact_inputs(migration, &[second])?;
        assert_eq!(catalog.effect_fact_source_cursor(workspace)?, Some(next));
        assert_eq!(next.last_sequence(), 2);
        assert_eq!(next.applied_count(), 2);
        drop(catalog);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn mixed_kind_page_applies_in_order_and_replays_exactly()
    -> Result<(), Box<dyn std::error::Error>> {
        let (root, mut catalog, migration, workspace) = catalog_fixture("mixed")?;
        let destination = catalog.destination_id();
        let execution_first = fact(1, migration, workspace, destination, 0xE401);
        let change_set_second = change_set_fact(2, migration, workspace, destination, 0xE402);
        let execution_third = fact(3, migration, workspace, destination, 0xE403);
        let page = [execution_first, change_set_second, execution_third];
        let progress = apply_page(&mut catalog.connection, destination, migration, &page)?;
        assert_eq!(progress.last_sequence(), 3);
        assert_eq!(progress.applied_count(), 3);

        let replay = apply_page(&mut catalog.connection, destination, migration, &page)?;
        assert_eq!(replay, progress, "exact mixed replay must be a no-op");

        let conflicting = change_set_fact(2, migration, workspace, destination, 0xE412);
        assert!(matches!(
            apply_page(
                &mut catalog.connection,
                destination,
                migration,
                std::slice::from_ref(&conflicting)
            ),
            Err(OperationsError::Conflict("effect fact replay"))
        ));

        let next_change_set = change_set_fact(4, migration, workspace, destination, 0xE405);
        let gapped = fact(6, migration, workspace, destination, 0xE406);
        assert!(matches!(
            apply_page(
                &mut catalog.connection,
                destination,
                migration,
                &[next_change_set, gapped]
            ),
            Err(OperationsError::ResyncRequired("effect fact sequence gap"))
        ));
        assert_eq!(catalog.effect_fact_progress(migration)?, Some(progress));
        let executions = catalog.execution_terminal_projection(migration)?;
        assert_eq!(executions.len(), 2, "gap must roll back the whole page");
        let change_sets = catalog.change_set_projection(migration)?;
        assert_eq!(
            change_sets.len(),
            1,
            "mid-page gap must roll back the earlier kind-1 row with the cursor"
        );
        assert_eq!(change_sets[0].source_sequence(), 2);
        assert_eq!(
            change_sets[0].change_set_id().into_uuid(),
            Uuid::from_u128(0xE402)
        );
        assert_eq!(
            change_sets[0].fact_digest(),
            fact_digest(&change_set_second)
        );

        let resumed = apply_page(
            &mut catalog.connection,
            destination,
            migration,
            std::slice::from_ref(&next_change_set),
        )?;
        assert_eq!(resumed.last_sequence(), 4);
        assert_eq!(resumed.applied_count(), 4);
        drop(catalog);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn change_set_replay_and_foreign_identity_fail_closed() -> Result<(), Box<dyn std::error::Error>>
    {
        let (root, mut catalog, migration, workspace) = catalog_fixture("change-set-identity")?;
        let destination = catalog.destination_id();
        let first = change_set_fact(1, migration, workspace, destination, 0xE501);
        let progress = apply_page(
            &mut catalog.connection,
            destination,
            migration,
            std::slice::from_ref(&first),
        )?;
        assert_eq!(progress.last_sequence(), 1);
        let conflicting = change_set_fact(1, migration, workspace, destination, 0xE502);
        assert!(matches!(
            apply_page(
                &mut catalog.connection,
                destination,
                migration,
                std::slice::from_ref(&conflicting)
            ),
            Err(OperationsError::Conflict("effect fact replay"))
        ));
        let foreign = change_set_fact(2, migration, workspace, Uuid::from_u128(0xFFFF), 0xE503);
        assert!(matches!(
            apply_page(&mut catalog.connection, destination, migration, &[foreign]),
            Err(OperationsError::Conflict("effect fact source identity"))
        ));
        let duplicate = change_set_fact(2, migration, workspace, destination, 0xE501);
        assert!(matches!(
            apply_page(
                &mut catalog.connection,
                destination,
                migration,
                &[duplicate]
            ),
            Err(OperationsError::Conflict("effect fact change set identity"))
        ));
        let progress = catalog
            .effect_fact_progress(migration)?
            .ok_or("consumer progress missing")?;
        assert_eq!(progress.last_sequence(), 1);
        drop(catalog);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn projections_and_cursor_survive_catalog_reopen() -> Result<(), Box<dyn std::error::Error>> {
        let (root, mut catalog, migration, workspace) = catalog_fixture("reopen")?;
        let destination = catalog.destination_id();
        let path = root.join("catalog.sqlite3");
        let execution = fact(1, migration, workspace, destination, 0xE601);
        let applied = change_set_fact(2, migration, workspace, destination, 0xE602);
        let failed = failed_change_set_fact(3, migration, workspace, destination, 0xE603);
        let progress = apply_page(
            &mut catalog.connection,
            destination,
            migration,
            &[execution, applied, failed],
        )?;
        assert_eq!(progress.last_sequence(), 3);
        let change_sets_before = catalog.change_set_projection(migration)?;
        drop(catalog);

        let reopened = crate::OperationsCatalog::open(&path, destination)?;
        assert_eq!(
            reopened.effect_fact_progress(migration)?,
            Some(progress),
            "consumer cursor must survive reopen"
        );
        let executions = reopened.execution_terminal_projection(migration)?;
        assert_eq!(executions.len(), 1);
        assert_eq!(executions[0].source_sequence(), 1);
        assert_eq!(
            executions[0].execution_id().into_uuid(),
            Uuid::from_u128(0xE601)
        );
        assert_eq!(executions[0].fact_digest(), fact_digest(&execution));
        let change_sets = reopened.change_set_projection(migration)?;
        assert_eq!(
            change_sets, change_sets_before,
            "projections must survive reopen"
        );
        assert_eq!(change_sets.len(), 2);
        assert_eq!(change_sets[0].state(), ChangeSetState::Applied);
        assert_eq!(change_sets[0].failure_code(), None);
        assert_eq!(change_sets[1].state(), ChangeSetState::Failed);
        assert_eq!(
            change_sets[1].failure_code(),
            Some(FileMutationFailureCode::IoFailure)
        );
        drop(reopened);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    fn credit_count(catalog: &crate::OperationsCatalog, workspace: WorkspaceId) -> usize {
        catalog
            .work_credit_entries(workspace)
            .map_or(usize::MAX, |entries| entries.len())
    }

    #[test]
    fn assignment_review_replay_and_foreign_identity_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let (root, mut catalog, migration, workspace) = catalog_fixture("review-identity")?;
        let destination = catalog.destination_id();
        let first = review_fact(
            1,
            migration,
            workspace,
            destination,
            0xE501,
            AssignmentReviewedOutcome::Accepted,
            Some(WorkCreditBudget::Three),
        );
        let progress = apply_page(
            &mut catalog.connection,
            destination,
            migration,
            std::slice::from_ref(&first),
        )?;
        assert_eq!(progress.last_sequence(), 1);
        let conflicting = review_fact(
            1,
            migration,
            workspace,
            destination,
            0xE512,
            AssignmentReviewedOutcome::Accepted,
            Some(WorkCreditBudget::Three),
        );
        assert!(matches!(
            apply_page(
                &mut catalog.connection,
                destination,
                migration,
                std::slice::from_ref(&conflicting)
            ),
            Err(OperationsError::Conflict("effect fact replay"))
        ));
        let foreign = review_fact(
            2,
            migration,
            workspace,
            Uuid::from_u128(0xFFFF),
            0xE513,
            AssignmentReviewedOutcome::Accepted,
            Some(WorkCreditBudget::Three),
        );
        assert!(matches!(
            apply_page(&mut catalog.connection, destination, migration, &[foreign]),
            Err(OperationsError::Conflict("effect fact source identity"))
        ));
        let duplicate_assignment = review_fact(
            2,
            migration,
            workspace,
            destination,
            0xE501,
            AssignmentReviewedOutcome::Rejected,
            None,
        );
        assert!(matches!(
            apply_page(
                &mut catalog.connection,
                destination,
                migration,
                std::slice::from_ref(&duplicate_assignment)
            ),
            Err(OperationsError::Conflict("effect fact assignment identity"))
        ));
        assert_eq!(
            catalog
                .effect_fact_progress(migration)?
                .ok_or("consumer progress missing")?
                .last_sequence(),
            1
        );
        drop(catalog);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn work_credit_awards_once_in_either_order_and_replays_exactly()
    -> Result<(), Box<dyn std::error::Error>> {
        // Order A: the verified execution arrives before the review.
        let (root, mut catalog, migration, workspace) = catalog_fixture("award-exec-first")?;
        let destination = catalog.destination_id();
        let execution =
            execution_fact_with_verification(1, migration, workspace, destination, 0xE601, true);
        let review = review_fact(
            2,
            migration,
            workspace,
            destination,
            0xE602,
            AssignmentReviewedOutcome::Accepted,
            Some(WorkCreditBudget::Three),
        );
        let progress = apply_page(
            &mut catalog.connection,
            destination,
            migration,
            &[execution, review],
        )?;
        assert_eq!(progress.last_sequence(), 2);
        assert_single_credit(&catalog, workspace, 0xE601, 2)?;
        let replay = apply_page(
            &mut catalog.connection,
            destination,
            migration,
            &[execution, review],
        )?;
        assert_eq!(replay, progress, "exact replay must be a no-op");
        assert_eq!(credit_count(&catalog, workspace), 1);
        drop(catalog);
        std::fs::remove_dir_all(root)?;

        // Order B: the review arrives before the verified execution.
        let (root, mut catalog, migration, workspace) = catalog_fixture("award-review-first")?;
        let destination = catalog.destination_id();
        let review = review_fact(
            1,
            migration,
            workspace,
            destination,
            0xE602,
            AssignmentReviewedOutcome::Accepted,
            Some(WorkCreditBudget::Three),
        );
        let progress = apply_page(
            &mut catalog.connection,
            destination,
            migration,
            std::slice::from_ref(&review),
        )?;
        assert_eq!(progress.last_sequence(), 1);
        assert_eq!(
            credit_count(&catalog, workspace),
            0,
            "a review without stored evidence never awards"
        );
        let execution =
            execution_fact_with_verification(2, migration, workspace, destination, 0xE601, true);
        let progress = apply_page(
            &mut catalog.connection,
            destination,
            migration,
            std::slice::from_ref(&execution),
        )?;
        assert_eq!(progress.last_sequence(), 2);
        assert_single_credit(&catalog, workspace, 0xE601, 1)?;
        let replay = apply_page(
            &mut catalog.connection,
            destination,
            migration,
            &[review, execution],
        )?;
        assert_eq!(replay, progress, "exact replay must be a no-op");
        assert_eq!(credit_count(&catalog, workspace), 1);
        drop(catalog);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn self_review_is_consumed_without_credit_in_either_fact_order()
    -> Result<(), Box<dyn std::error::Error>> {
        for (label, execution_first) in [
            ("self-review-execution-first", true),
            ("self-review-review-first", false),
        ] {
            let (root, mut catalog, migration, workspace) = catalog_fixture(label)?;
            let destination = catalog.destination_id();
            let target_agent = AgentInstanceId::from_uuid(Uuid::from_u128(0xE102));
            let execution = execution_fact_with_verification(
                if execution_first { 1 } else { 2 },
                migration,
                workspace,
                destination,
                0xE601,
                true,
            );
            let review = review_fact_with_reviewer_agent(
                if execution_first { 2 } else { 1 },
                migration,
                workspace,
                destination,
                0xE602,
                AssignmentReviewedOutcome::Accepted,
                Some(WorkCreditBudget::Three),
                target_agent,
            );
            let facts = if execution_first {
                [execution, review]
            } else {
                [review, execution]
            };
            let progress = apply_page(&mut catalog.connection, destination, migration, &facts)?;
            assert_eq!(progress.last_sequence(), 2);
            assert_eq!(
                catalog.effect_fact_progress(migration)?,
                Some(progress),
                "self-review must consume and persist the complete effect cursor"
            );
            assert_eq!(
                credit_count(&catalog, workspace),
                0,
                "a target Agent cannot award itself credit"
            );
            let reviews = catalog.assignment_review_projection(migration)?;
            assert_eq!(reviews.len(), 1);
            assert_eq!(reviews[0].target_agent_id(), target_agent);
            assert_eq!(reviews[0].reviewer_agent_id(), target_agent);
            let snapshot = catalog.work_credit_scores(workspace, 100)?;
            assert_eq!(snapshot.fulfillment_completed(), 0);
            assert_eq!(snapshot.fulfillment_evaluated(), 0);
            drop(catalog);
            std::fs::remove_dir_all(root)?;
        }
        Ok(())
    }

    /// Both consumption orders must converge on the identical single credit:
    /// one row, earliest matching execution as evidence, the review's sequence
    /// and observation time, and NULL participant without a current binding.
    fn assert_single_credit(
        catalog: &crate::OperationsCatalog,
        workspace: WorkspaceId,
        evidence_execution: u128,
        award_sequence: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let entries = catalog.work_credit_entries(workspace)?;
        assert_eq!(
            entries.len(),
            1,
            "one matching pair awards exactly one credit"
        );
        assert_eq!(
            entries[0].evidence_execution_id().into_uuid(),
            Uuid::from_u128(evidence_execution)
        );
        assert_eq!(entries[0].award_source_sequence(), award_sequence);
        assert_eq!(entries[0].awarded_at_ms(), 44);
        assert_eq!(entries[0].budget(), WorkCreditBudget::Three);
        assert_eq!(
            entries[0].participant_id(),
            None,
            "no binding resolves to NULL"
        );
        Ok(())
    }

    #[test]
    fn work_credit_requires_accepted_budgeted_review_and_verified_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        // Rejected outcome: projection row is stored, no credit is awarded.
        let (root, mut catalog, migration, workspace) = catalog_fixture("no-award-rejected")?;
        let destination = catalog.destination_id();
        let execution =
            execution_fact_with_verification(1, migration, workspace, destination, 0xE701, true);
        let rejected = review_fact(
            2,
            migration,
            workspace,
            destination,
            0xE702,
            AssignmentReviewedOutcome::Rejected,
            Some(WorkCreditBudget::Five),
        );
        let _ = apply_page(
            &mut catalog.connection,
            destination,
            migration,
            &[execution, rejected],
        )?;
        assert_eq!(credit_count(&catalog, workspace), 0);
        let reviews = catalog.assignment_review_projection(migration)?;
        assert_eq!(reviews.len(), 1, "rejected review is still projected");
        assert_eq!(reviews[0].outcome(), AssignmentReviewedOutcome::Rejected);
        drop(catalog);
        std::fs::remove_dir_all(root)?;

        // Credit-ineligible assignment (budget NULL): no credit even on acceptance.
        let (root, mut catalog, migration, workspace) = catalog_fixture("no-award-null-budget")?;
        let destination = catalog.destination_id();
        let execution =
            execution_fact_with_verification(1, migration, workspace, destination, 0xE701, true);
        let ineligible = review_fact(
            2,
            migration,
            workspace,
            destination,
            0xE702,
            AssignmentReviewedOutcome::Accepted,
            None,
        );
        let _ = apply_page(
            &mut catalog.connection,
            destination,
            migration,
            &[execution, ineligible],
        )?;
        assert_eq!(credit_count(&catalog, workspace), 0);
        let reviews = catalog.assignment_review_projection(migration)?;
        assert_eq!(reviews.len(), 1);
        assert_eq!(reviews[0].credit_budget(), None);
        drop(catalog);
        std::fs::remove_dir_all(root)?;

        // No verification-Accepted execution: the review stores but never awards.
        let (root, mut catalog, migration, workspace) = catalog_fixture("no-award-no-evidence")?;
        let destination = catalog.destination_id();
        let unverified =
            execution_fact_with_verification(1, migration, workspace, destination, 0xE701, false);
        let accepted = review_fact(
            2,
            migration,
            workspace,
            destination,
            0xE702,
            AssignmentReviewedOutcome::Accepted,
            Some(WorkCreditBudget::Three),
        );
        let _ = apply_page(
            &mut catalog.connection,
            destination,
            migration,
            &[unverified, accepted],
        )?;
        assert_eq!(credit_count(&catalog, workspace), 0);
        assert_eq!(
            catalog.execution_terminal_projection(migration)?.len(),
            1,
            "unverified execution projection is stored without awarding"
        );
        drop(catalog);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn mixed_kind_page_with_assignment_review_applies_in_order_and_gap_is_fenced()
    -> Result<(), Box<dyn std::error::Error>> {
        let (root, mut catalog, migration, workspace) = catalog_fixture("mixed-review")?;
        let destination = catalog.destination_id();
        let execution =
            execution_fact_with_verification(1, migration, workspace, destination, 0xE801, false);
        let change_set = change_set_fact(2, migration, workspace, destination, 0xE802);
        let review = review_fact(
            3,
            migration,
            workspace,
            destination,
            0xE803,
            AssignmentReviewedOutcome::Accepted,
            Some(WorkCreditBudget::Five),
        );
        let verified =
            execution_fact_with_verification(4, migration, workspace, destination, 0xE804, true);
        let progress = apply_page(
            &mut catalog.connection,
            destination,
            migration,
            &[execution, change_set, review, verified],
        )?;
        assert_eq!(progress.last_sequence(), 4);
        assert_eq!(progress.applied_count(), 4);
        assert_eq!(
            credit_count(&catalog, workspace),
            1,
            "the mid-page review awards only once the verified execution lands"
        );
        let entries = catalog.work_credit_entries(workspace)?;
        assert_eq!(entries[0].award_source_sequence(), 3);
        assert_eq!(
            entries[0].evidence_execution_id().into_uuid(),
            Uuid::from_u128(0xE804)
        );

        let replay = apply_page(
            &mut catalog.connection,
            destination,
            migration,
            &[execution, change_set, review, verified],
        )?;
        assert_eq!(replay, progress, "exact mixed replay must be a no-op");
        assert_eq!(credit_count(&catalog, workspace), 1);

        let next_change_set = change_set_fact(5, migration, workspace, destination, 0xE805);
        let gapped = fact(7, migration, workspace, destination, 0xE806);
        assert!(matches!(
            apply_page(
                &mut catalog.connection,
                destination,
                migration,
                &[next_change_set, gapped]
            ),
            Err(OperationsError::ResyncRequired("effect fact sequence gap"))
        ));
        assert_eq!(catalog.effect_fact_progress(migration)?, Some(progress));
        assert_eq!(
            credit_count(&catalog, workspace),
            1,
            "gap must roll back the page"
        );
        let reviews = catalog.assignment_review_projection(migration)?;
        assert_eq!(
            reviews.len(),
            1,
            "gap must preserve the committed review row"
        );
        assert_eq!(reviews[0].source_sequence(), 3);
        assert_eq!(reviews[0].fact_digest(), fact_digest(&review));
        let change_sets = catalog.change_set_projection(migration)?;
        assert_eq!(
            change_sets.len(),
            1,
            "mid-page gap must roll back the earlier kind-1 row"
        );

        let resumed = apply_page(
            &mut catalog.connection,
            destination,
            migration,
            std::slice::from_ref(&next_change_set),
        )?;
        assert_eq!(resumed.last_sequence(), 5);
        assert_eq!(resumed.applied_count(), 5);
        drop(catalog);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn work_credits_and_projections_survive_catalog_reopen()
    -> Result<(), Box<dyn std::error::Error>> {
        let (root, mut catalog, migration, workspace) = catalog_fixture("credit-reopen")?;
        let destination = catalog.destination_id();
        let path = root.join("catalog.sqlite3");
        let execution =
            execution_fact_with_verification(1, migration, workspace, destination, 0xE901, true);
        let review = review_fact(
            2,
            migration,
            workspace,
            destination,
            0xE902,
            AssignmentReviewedOutcome::Accepted,
            Some(WorkCreditBudget::Three),
        );
        let progress = apply_page(
            &mut catalog.connection,
            destination,
            migration,
            &[execution, review],
        )?;
        assert_eq!(progress.last_sequence(), 2);
        let credits_before = catalog.work_credit_entries(workspace)?;
        let reviews_before = catalog.assignment_review_projection(migration)?;
        drop(catalog);

        let reopened = crate::OperationsCatalog::open(&path, destination)?;
        assert_eq!(
            reopened.effect_fact_progress(migration)?,
            Some(progress),
            "consumer cursor must survive reopen"
        );
        assert_eq!(
            reopened.work_credit_entries(workspace)?,
            credits_before,
            "work-credit ledger must survive reopen"
        );
        assert_eq!(credits_before.len(), 1);
        assert_eq!(
            reopened.assignment_review_projection(migration)?,
            reviews_before,
            "review projections must survive reopen"
        );
        drop(reopened);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    fn insert_member_and_binding(
        catalog: &crate::OperationsCatalog,
        workspace: WorkspaceId,
        activity: ActivityId,
        participant: Uuid,
        session: u128,
        agent: u128,
    ) -> Result<(), Box<dyn std::error::Error>> {
        catalog.connection.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,?4,?5,1,1,1)",
            params![
                workspace.into_uuid().as_bytes(),
                participant.as_bytes(),
                Uuid::from_u128(session).as_bytes(),
                "offer worker",
                "worker",
            ],
        )?;
        catalog.connection.execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
            params![
                workspace.into_uuid().as_bytes(),
                participant.as_bytes(),
                activity.into_uuid().as_bytes(),
                Uuid::from_u128(session).as_bytes(),
                Uuid::from_u128(agent).as_bytes(),
                Uuid::from_u128(session + 0x40).as_bytes(),
            ],
        )?;
        Ok(())
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn assignment_submit_replay_projection_and_foreign_identity_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let (root, mut catalog, migration, workspace) = catalog_fixture("submit-identity")?;
        let destination = catalog.destination_id();
        let first = submit_fact(
            1,
            migration,
            workspace,
            destination,
            SubmitShape {
                assignment: 0xEE11,
                agent: 0xE102,
                submitted_at_ms: 2000,
            },
        );
        let progress = apply_page(
            &mut catalog.connection,
            destination,
            migration,
            std::slice::from_ref(&first),
        )?;
        assert_eq!(progress.last_sequence(), 1);
        let rows = catalog.assignment_submit_projection(migration)?;
        assert_eq!(rows.len(), 1, "submit fact is projected");
        assert_eq!(rows[0].assignment_id().into_uuid(), Uuid::from_u128(0xEE11));
        assert_eq!(rows[0].source_count(), 2);
        assert_eq!(
            rows[0].source_kinds(),
            Some(&[1u8, 1][..]),
            "ordered provenance source kinds are stored exactly"
        );

        // Identical replay at the same sequence is a harmless no-op.
        let replay = submit_fact(
            1,
            migration,
            workspace,
            destination,
            SubmitShape {
                assignment: 0xEE11,
                agent: 0xE102,
                submitted_at_ms: 2000,
            },
        );
        let progress = apply_page(
            &mut catalog.connection,
            destination,
            migration,
            std::slice::from_ref(&replay),
        )?;
        assert_eq!(progress.last_sequence(), 1);
        assert_eq!(catalog.assignment_submit_projection(migration)?.len(), 1);

        // Changed payload at the same sequence fails closed.
        let conflicting = submit_fact(
            1,
            migration,
            workspace,
            destination,
            SubmitShape {
                assignment: 0xEE12,
                agent: 0xE102,
                submitted_at_ms: 2000,
            },
        );
        assert!(matches!(
            apply_page(
                &mut catalog.connection,
                destination,
                migration,
                std::slice::from_ref(&conflicting)
            ),
            Err(OperationsError::Conflict("effect fact replay"))
        ));

        // Foreign destination is fenced.
        let foreign = submit_fact(
            2,
            migration,
            workspace,
            Uuid::from_u128(0xFFFF),
            SubmitShape {
                assignment: 0xEE13,
                agent: 0xE102,
                submitted_at_ms: 2000,
            },
        );
        assert!(matches!(
            apply_page(
                &mut catalog.connection,
                Uuid::from_u128(0xFFFF),
                migration,
                std::slice::from_ref(&foreign)
            ),
            Err(OperationsError::Conflict("effect fact import not verified"))
        ));

        // Sequence gap is fenced without consuming.
        let gapped = submit_fact(
            3,
            migration,
            workspace,
            destination,
            SubmitShape {
                assignment: 0xEE14,
                agent: 0xE102,
                submitted_at_ms: 2000,
            },
        );
        assert!(matches!(
            apply_page(
                &mut catalog.connection,
                destination,
                migration,
                std::slice::from_ref(&gapped)
            ),
            Err(OperationsError::ResyncRequired(_))
        ));
        assert_eq!(catalog.assignment_submit_projection(migration)?.len(), 1);

        drop(catalog);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn assignment_offer_replay_and_foreign_identity_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let (root, mut catalog, migration, workspace) = catalog_fixture("offer-identity")?;
        let destination = catalog.destination_id();
        let first = offer_fact(
            1,
            migration,
            workspace,
            destination,
            OfferShape {
                assignment: 0xEE01,
                activity: 0xE101,
                agent: 0xE102,
                offered_at_ms: 1000,
            },
        );
        let progress = apply_page(
            &mut catalog.connection,
            destination,
            migration,
            std::slice::from_ref(&first),
        )?;
        assert_eq!(progress.last_sequence(), 1);
        let conflicting = offer_fact(
            1,
            migration,
            workspace,
            destination,
            OfferShape {
                assignment: 0xEE02,
                activity: 0xE101,
                agent: 0xE102,
                offered_at_ms: 1000,
            },
        );
        assert!(matches!(
            apply_page(
                &mut catalog.connection,
                destination,
                migration,
                std::slice::from_ref(&conflicting)
            ),
            Err(OperationsError::Conflict("effect fact replay"))
        ));
        let foreign = offer_fact(
            2,
            migration,
            workspace,
            Uuid::from_u128(0xFFFF),
            OfferShape {
                assignment: 0xEE03,
                activity: 0xE101,
                agent: 0xE102,
                offered_at_ms: 1000,
            },
        );
        assert!(matches!(
            apply_page(&mut catalog.connection, destination, migration, &[foreign]),
            Err(OperationsError::Conflict("effect fact source identity"))
        ));
        let duplicate_assignment = offer_fact(
            2,
            migration,
            workspace,
            destination,
            OfferShape {
                assignment: 0xEE01,
                activity: 0xE101,
                agent: 0xE102,
                offered_at_ms: 1001,
            },
        );
        assert!(matches!(
            apply_page(
                &mut catalog.connection,
                destination,
                migration,
                std::slice::from_ref(&duplicate_assignment)
            ),
            Err(OperationsError::Conflict("effect fact assignment identity"))
        ));
        assert_eq!(
            catalog
                .effect_fact_progress(migration)?
                .ok_or("consumer progress missing")?
                .last_sequence(),
            1
        );
        let offers = catalog.assignment_offer_projection(migration)?;
        assert_eq!(offers.len(), 1);
        assert_eq!(offers[0].fact_digest(), fact_digest(&first));
        assert_eq!(offers[0].offered_at_ms(), 1000);
        drop(catalog);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn assignment_offer_auto_bootstraps_first_offer_and_defers_unresolved()
    -> Result<(), Box<dyn std::error::Error>> {
        let (root, mut catalog, migration, workspace) = catalog_fixture("offer-bootstrap")?;
        let destination = catalog.destination_id();
        let path = root.join("catalog.sqlite3");
        let activity = ActivityId::from_uuid(Uuid::from_u128(0xE101));
        let participant = Uuid::from_u128(0xEE10);
        insert_member_and_binding(&catalog, workspace, activity, participant, 0xEE11, 0xE102)?;
        let page = [
            offer_fact(
                1,
                migration,
                workspace,
                destination,
                OfferShape {
                    assignment: 0xEE01,
                    activity: 0xE101,
                    agent: 0xE102,
                    offered_at_ms: 1000,
                },
            ),
            // An offer whose target Agent has no Ready binding defers quietly.
            offer_fact(
                2,
                migration,
                workspace,
                destination,
                OfferShape {
                    assignment: 0xEE02,
                    activity: 0xE101,
                    agent: 0xEE99,
                    offered_at_ms: 1001,
                },
            ),
            // A later offer for the already-stabilizing scopes is a no-op.
            offer_fact(
                3,
                migration,
                workspace,
                destination,
                OfferShape {
                    assignment: 0xEE03,
                    activity: 0xE101,
                    agent: 0xE102,
                    offered_at_ms: 1002,
                },
            ),
        ];
        let progress = apply_page(&mut catalog.connection, destination, migration, &page)?;
        assert_eq!(progress.last_sequence(), 3);
        assert_eq!(progress.applied_count(), 3);
        assert_eq!(catalog.assignment_offer_projection(migration)?.len(), 3);

        // The auto path parks each scope in BOOTSTRAPPING with the offer as
        // pending evidence; a later offer for the already-parked scope is a
        // no-op until the evidence is confirmed.
        let view = catalog
            .leadership_view(workspace, Some(activity), 1000)?
            .ok_or("activity leadership row missing")?;
        assert_eq!(
            view.state(),
            crate::leadership::LeadershipState::Bootstrapping
        );
        assert_eq!(view.leader_participant_id(), Some(participant));
        assert_eq!(view.stabilization_until_ms(), None);
        let workspace_view = catalog
            .leadership_view(workspace, None, 1000)?
            .ok_or("workspace leadership row missing")?;
        assert_eq!(
            workspace_view.state(),
            crate::leadership::LeadershipState::Bootstrapping
        );
        assert_eq!(workspace_view.leader_participant_id(), Some(participant));

        // Evidence confirmation anchors the stabilization window per scope.
        for scope in [
            crate::leadership::LeadershipScope::activity(workspace, activity),
            crate::leadership::LeadershipScope::workspace(workspace),
        ] {
            let confirmed = catalog.confirm_pending_bootstrap(
                crate::leadership::ConfirmPendingBootstrapCommand {
                    scope,
                    expected_revision: 1,
                    now_ms: 1001,
                },
            )?;
            assert_eq!(
                confirmed.state(),
                crate::leadership::LeadershipState::Stabilizing
            );
            assert_eq!(
                confirmed.stabilization_until_ms(),
                Some(1001 + crate::leadership::STABILIZATION_WINDOW_MS)
            );
        }
        let stabilized = catalog
            .leadership_view(workspace, Some(activity), 1000)?
            .ok_or("activity leadership row missing")?;
        assert_eq!(
            stabilized.state(),
            crate::leadership::LeadershipState::Stabilizing
        );
        assert_eq!(stabilized.leader_participant_id(), Some(participant));
        let workspace_stabilized = catalog
            .leadership_view(workspace, None, 1000)?
            .ok_or("workspace leadership row missing")?;
        assert_eq!(
            workspace_stabilized.state(),
            crate::leadership::LeadershipState::Stabilizing
        );
        assert_eq!(
            workspace_stabilized.leader_participant_id(),
            Some(participant)
        );

        let bootstrap_rows: i64 = catalog.connection.query_row(
            "SELECT COUNT(*) FROM ops_leadership_receipts WHERE receipt_kind='BOOTSTRAP' AND work_assignment_id=?1",
            params![Uuid::from_u128(0xEE01).as_bytes()],
            |row| row.get(0),
        )?;
        assert_eq!(
            bootstrap_rows, 2,
            "both scopes record one bootstrap receipt"
        );
        let deferred_bootstrap: i64 = catalog.connection.query_row(
            "SELECT COUNT(*) FROM ops_leadership_receipts WHERE receipt_kind='BOOTSTRAP' AND work_assignment_id=?1",
            params![Uuid::from_u128(0xEE02).as_bytes()],
            |row| row.get(0),
        )?;
        assert_eq!(
            deferred_bootstrap, 0,
            "an unresolved offer must not bootstrap"
        );

        drop(catalog);
        let reopened = crate::OperationsCatalog::open(&path, destination)?;
        let reopened_view = reopened
            .leadership_view(workspace, Some(activity), 1000)?
            .ok_or("activity leadership row missing after reopen")?;
        assert_eq!(reopened_view.leader_participant_id(), Some(participant));
        assert_eq!(reopened.assignment_offer_projection(migration)?.len(), 3);
        drop(reopened);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
