//! Durable independent reviewer selection, self-review prohibition, and diff-digest-bound verdict gating.
//!
//! Enforces strict separation across `AgentProfileId`, `participant_id`, and `AuthenticatedPrincipal`
//! against both the execution author and the assigning `Team Leader`, verifies durable reviewer
//! participation interest and active qualification-rule generations, and binds node completion to
//! an `Accepted` verdict whose `handoff_diff_digest` matches the live worktree diff digest.

pub(crate) mod schema;
mod sql;

use std::collections::BTreeMap;

use forge_domain::{
    AgentAssignmentId, AuthenticatedPrincipal, DagNodeId, DagRevisionNumber, GoalId, WorkspaceId,
};
use heterarchy_orchestration_sessions::AgentProfileId;
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::reviewer_qualification_rule::QualificationDomain;
use crate::sqlite_codecs::{
    decode_non_nil_uuid, decode_non_zero_digest, decode_positive_u64, decode_principal,
    derive_deterministic_uuid, encode_principal, finalize_operation_commit, hash_str, len_u64,
    principal_is_valid,
};
use crate::{OperationsCatalog, OperationsError, WorkspaceParticipationId};

/// Maximum candidate entries accepted in one [`ReviewerAssignmentCommand`].
pub const MAX_REVIEWER_CANDIDATE_POOL: usize = 1_024;
/// Maximum UTF-8 byte length of a [`RecordedReviewVerdict`] rationale.
pub const MAX_REVIEW_VERDICT_RATIONALE_BYTES: usize = 8_192;

const ASSIGNMENT_RECORD_ID_DOMAIN: &[u8] =
    b"heterarchy.orchestration.reviewer_assignment.record_id.v1\0";
const ASSIGNMENT_DIGEST_DOMAIN: &[u8] = b"heterarchy.orchestration.reviewer_assignment.assign.v1\0";
const VERDICT_DIGEST_DOMAIN: &[u8] = b"heterarchy.orchestration.reviewer_assignment.verdict.v1\0";

use sql::{
    INSERT_REVIEWER_ASSIGNMENT_SQL, INSERT_REVIEWER_VERDICT_RECEIPT_SQL,
    SELECT_ACTIVE_RULE_GENERATION_SQL, SELECT_ASSIGNMENT_BY_CORRELATION_SQL,
    SELECT_ASSIGNMENT_BY_RECORD_ID_SQL, SELECT_ASSIGNMENT_BY_REQUEST_ID_SQL,
    SELECT_REVIEWER_PARTICIPATION_HEAD_SQL, SELECT_REVIEWER_SESSION_BINDING_SQL,
    SELECT_VERDICT_RECEIPT_BY_REQUEST_ID_SQL, UPDATE_REVIEWER_ASSIGNMENT_VERDICT_SQL,
};

/// Exact 6-part correlation key identifying one node-assignment attempt under review.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkCorrelationKey {
    workspace_id: WorkspaceId,
    goal_id: GoalId,
    dag_revision: DagRevisionNumber,
    node_id: DagNodeId,
    assignment_id: AgentAssignmentId,
    attempt: u32,
}

impl WorkCorrelationKey {
    /// Construct a validated [`WorkCorrelationKey`].
    ///
    /// # Errors
    /// Rejects nil UUIDs, invalid `dag_revision`, or `attempt == 0`.
    pub fn new(
        workspace_id: WorkspaceId,
        goal_id: GoalId,
        dag_revision: DagRevisionNumber,
        node_id: DagNodeId,
        assignment_id: AgentAssignmentId,
        attempt: u32,
    ) -> Result<Self, OperationsError> {
        let key = Self {
            workspace_id,
            goal_id,
            dag_revision,
            node_id,
            assignment_id,
            attempt,
        };
        key.validate()?;
        Ok(key)
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn goal_id(&self) -> GoalId {
        self.goal_id
    }

    #[must_use]
    pub const fn dag_revision(&self) -> DagRevisionNumber {
        self.dag_revision
    }

    #[must_use]
    pub const fn node_id(&self) -> DagNodeId {
        self.node_id
    }

    #[must_use]
    pub const fn assignment_id(&self) -> AgentAssignmentId {
        self.assignment_id
    }

    #[must_use]
    pub const fn attempt(&self) -> u32 {
        self.attempt
    }

    fn validate(&self) -> Result<(), OperationsError> {
        if self.workspace_id.into_uuid().is_nil()
            || self.goal_id.into_uuid().is_nil()
            || self.node_id.into_uuid().is_nil()
            || self.assignment_id.into_uuid().is_nil()
            || self.dag_revision.get() < 1
            || self.attempt == 0
        {
            return Err(OperationsError::Bounds("reviewer work correlation key"));
        }
        Ok(())
    }
}

/// Complete three-axis identity of an executor or assigning `Team Leader`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionAuthorIdentity {
    profile_id: AgentProfileId,
    participant_id: Uuid,
    principal: AuthenticatedPrincipal,
}

impl ExecutionAuthorIdentity {
    /// Construct a validated [`ExecutionAuthorIdentity`].
    ///
    /// # Errors
    /// Rejects nil `profile_id`, nil `participant_id`, or nil token/grant principals.
    pub fn new(
        profile_id: AgentProfileId,
        participant_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<Self, OperationsError> {
        let author = Self {
            profile_id,
            participant_id,
            principal,
        };
        author.validate()?;
        Ok(author)
    }

    #[must_use]
    pub const fn profile_id(&self) -> AgentProfileId {
        self.profile_id
    }

    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }

    pub const fn principal(&self) -> AuthenticatedPrincipal {
        self.principal
    }

    fn validate(&self) -> Result<(), OperationsError> {
        if self.profile_id.into_uuid().is_nil()
            || self.participant_id.is_nil()
            || !principal_is_valid(self.principal)
        {
            return Err(OperationsError::Bounds("execution author identity"));
        }
        Ok(())
    }
}

/// Resolved reviewer candidate identity backed by a `WorkspaceParticipationId`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReviewerCandidateIdentity {
    participation_id: WorkspaceParticipationId,
    profile_id: AgentProfileId,
    participant_id: Uuid,
    principal: AuthenticatedPrincipal,
}

impl ReviewerCandidateIdentity {
    /// Construct a validated [`ReviewerCandidateIdentity`].
    ///
    /// # Errors
    /// Rejects nil participation/profile/participant UUIDs or nil token/grant principals.
    pub fn new(
        participation_id: WorkspaceParticipationId,
        profile_id: AgentProfileId,
        participant_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<Self, OperationsError> {
        let candidate = Self {
            participation_id,
            profile_id,
            participant_id,
            principal,
        };
        candidate.validate()?;
        Ok(candidate)
    }

    #[must_use]
    pub const fn participation_id(&self) -> WorkspaceParticipationId {
        self.participation_id
    }

    #[must_use]
    pub const fn profile_id(&self) -> AgentProfileId {
        self.profile_id
    }

    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }

    pub const fn principal(&self) -> AuthenticatedPrincipal {
        self.principal
    }

    fn validate(&self) -> Result<(), OperationsError> {
        if self.participation_id.into_uuid().is_nil()
            || self.profile_id.into_uuid().is_nil()
            || self.participant_id.is_nil()
            || !principal_is_valid(self.principal)
        {
            return Err(OperationsError::Bounds("reviewer candidate identity"));
        }
        Ok(())
    }
}

/// Domain and storage errors returned by reviewer assignment, verdict recording, and completion gating.
#[derive(Debug, thiserror::Error)]
pub enum ReviewerAssignmentError {
    #[error("self-review prohibited: {reason}")]
    SelfReview { reason: &'static str },
    #[error("no interested, qualified, independent reviewer candidate is available")]
    NoQualifiedReviewer,
    #[error("active reviewer qualification rule is missing for requested domain")]
    QualificationMissing,
    #[error("worktree diff digest does not match accepted review verdict digest")]
    VerdictDiffDigestMismatch,
    #[error("independent review has not recorded an Accepted verdict")]
    ReviewNotAccepted,
    #[error("reviewer assignment or verdict revision is stale")]
    StaleRevision,
    #[error(transparent)]
    Operations(#[from] OperationsError),
}

/// Outcome recorded by an assigned independent reviewer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ReviewVerdictOutcome {
    Accepted,
    Rejected,
    NeedsChanges,
}

impl ReviewVerdictOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "ACCEPTED",
            Self::Rejected => "REJECTED",
            Self::NeedsChanges => "NEEDS_CHANGES",
        }
    }

    fn parse(raw: &str) -> Result<Self, OperationsError> {
        match raw {
            "ACCEPTED" => Ok(Self::Accepted),
            "REJECTED" => Ok(Self::Rejected),
            "NEEDS_CHANGES" => Ok(Self::NeedsChanges),
            _ => Err(OperationsError::Corrupt("review verdict outcome")),
        }
    }
}

/// Command requesting deterministic independent reviewer assignment for a work attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewerAssignmentCommand {
    pub request_id: Uuid,
    pub correlation: WorkCorrelationKey,
    pub assigning_leader: ExecutionAuthorIdentity,
    pub executor: ExecutionAuthorIdentity,
    pub candidates: Vec<ReviewerCandidateIdentity>,
    pub qualification_domain: Option<QualificationDomain>,
    pub assigned_at_ms: i64,
}

impl ReviewerAssignmentCommand {
    fn validate(&self) -> Result<(), OperationsError> {
        if self.request_id.is_nil() || self.assigned_at_ms < 0 {
            return Err(OperationsError::Bounds("reviewer assignment command"));
        }
        if self.candidates.len() > MAX_REVIEWER_CANDIDATE_POOL {
            return Err(OperationsError::Bounds("reviewer candidate pool size"));
        }
        self.correlation.validate()?;
        self.assigning_leader.validate()?;
        self.executor.validate()?;
        if let Some(domain) = &self.qualification_domain {
            domain.validate()?;
        }
        for candidate in &self.candidates {
            candidate.validate()?;
        }
        Ok(())
    }
}

/// Durable diff-digest-bound verdict recorded by the assigned independent reviewer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedReviewVerdict {
    request_id: Uuid,
    outcome: ReviewVerdictOutcome,
    handoff_diff_digest: [u8; 32],
    rationale: String,
    recorded_at_ms: i64,
}

impl RecordedReviewVerdict {
    /// Construct a validated [`RecordedReviewVerdict`].
    ///
    /// # Errors
    /// Rejects nil `request_id`, all-zero `handoff_diff_digest`, blank/oversized/NUL rationale,
    /// and negative `recorded_at_ms`.
    pub fn new(
        request_id: Uuid,
        outcome: ReviewVerdictOutcome,
        handoff_diff_digest: [u8; 32],
        rationale: String,
        recorded_at_ms: i64,
    ) -> Result<Self, OperationsError> {
        validate_verdict_fields(request_id, handoff_diff_digest, &rationale, recorded_at_ms)?;
        Ok(Self {
            request_id,
            outcome,
            handoff_diff_digest,
            rationale,
            recorded_at_ms,
        })
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn outcome(&self) -> ReviewVerdictOutcome {
        self.outcome
    }

    #[must_use]
    pub const fn handoff_diff_digest(&self) -> [u8; 32] {
        self.handoff_diff_digest
    }

    #[must_use]
    pub fn rationale(&self) -> &str {
        &self.rationale
    }

    #[must_use]
    pub const fn recorded_at_ms(&self) -> i64 {
        self.recorded_at_ms
    }
}

/// Versioned durable record of an independent reviewer assignment and its latest verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewerAssignmentRecord {
    assignment_record_id: Uuid,
    correlation: WorkCorrelationKey,
    reviewer: ReviewerCandidateIdentity,
    executor: ExecutionAuthorIdentity,
    assigning_leader: ExecutionAuthorIdentity,
    qualification_domain: Option<QualificationDomain>,
    rule_generation: Option<u64>,
    revision: u64,
    assigned_at_ms: i64,
    verdict: Option<RecordedReviewVerdict>,
    replayed: bool,
}

impl ReviewerAssignmentRecord {
    #[must_use]
    pub const fn assignment_record_id(&self) -> Uuid {
        self.assignment_record_id
    }

    #[must_use]
    pub const fn correlation(&self) -> WorkCorrelationKey {
        self.correlation
    }

    #[must_use]
    pub const fn reviewer(&self) -> ReviewerCandidateIdentity {
        self.reviewer
    }

    #[must_use]
    pub const fn executor(&self) -> ExecutionAuthorIdentity {
        self.executor
    }

    #[must_use]
    pub const fn assigning_leader(&self) -> ExecutionAuthorIdentity {
        self.assigning_leader
    }

    #[must_use]
    pub const fn qualification_domain(&self) -> Option<&QualificationDomain> {
        self.qualification_domain.as_ref()
    }

    #[must_use]
    pub const fn rule_generation(&self) -> Option<u64> {
        self.rule_generation
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub const fn assigned_at_ms(&self) -> i64 {
        self.assigned_at_ms
    }

    #[must_use]
    pub const fn verdict(&self) -> Option<&RecordedReviewVerdict> {
        self.verdict.as_ref()
    }

    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Compare-and-swap command to record a diff-digest-bound reviewer verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewVerdictCommand {
    pub request_id: Uuid,
    pub correlation: WorkCorrelationKey,
    pub expected_revision: u64,
    pub reviewer_principal: AuthenticatedPrincipal,
    pub reviewer_profile_id: AgentProfileId,
    pub reviewer_participant_id: Uuid,
    pub outcome: ReviewVerdictOutcome,
    pub handoff_diff_digest: [u8; 32],
    pub rationale: String,
    pub recorded_at_ms: i64,
}

impl ReviewVerdictCommand {
    fn validate(&self) -> Result<(), OperationsError> {
        self.correlation.validate()?;
        if self.expected_revision == 0 {
            return Err(OperationsError::Bounds("review verdict expected_revision"));
        }
        crate::catalog::signed(self.expected_revision)?;
        if self.reviewer_profile_id.into_uuid().is_nil()
            || self.reviewer_participant_id.is_nil()
            || !principal_is_valid(self.reviewer_principal)
        {
            return Err(OperationsError::Bounds("review verdict reviewer identity"));
        }
        validate_verdict_fields(
            self.request_id,
            self.handoff_diff_digest,
            &self.rationale,
            self.recorded_at_ms,
        )
    }
}

impl OperationsCatalog {
    /// Deterministically select and persist an independent, interested, qualified reviewer.
    ///
    /// # Errors
    /// Returns [`ReviewerAssignmentError::SelfReview`] if only self-reviewing candidates are
    /// supplied, [`ReviewerAssignmentError::QualificationMissing`] if no active qualification
    /// rule exists for `qualification_domain`, [`ReviewerAssignmentError::NoQualifiedReviewer`]
    /// if no independent `Interested` candidate remains, [`ReviewerAssignmentError::StaleRevision`]
    /// if `correlation` is already assigned under another `request_id`, or [`OperationsError`]
    /// on validation, `request_id` payload conflict, or SQLite failures.
    pub fn assign_independent_reviewer(
        &mut self,
        command: &ReviewerAssignmentCommand,
    ) -> Result<ReviewerAssignmentRecord, ReviewerAssignmentError> {
        command.validate()?;
        let digest = assignment_digest(command);
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(OperationsError::from)?;
        if let Some(replayed) = load_assignment_replay(&tx, command.request_id, digest)? {
            tx.commit().map_err(OperationsError::from)?;
            return Ok(replayed);
        }
        if load_assignment_by_correlation(&tx, &command.correlation)?.is_some() {
            return Err(ReviewerAssignmentError::StaleRevision);
        }
        let (selected, rule_generation) = select_independent_candidate(&tx, command)?;
        let record_id = derive_deterministic_uuid(
            ASSIGNMENT_RECORD_ID_DOMAIN,
            self.destination,
            command.request_id,
        );
        let fresh = ReviewerAssignmentRecord {
            assignment_record_id: record_id,
            correlation: command.correlation,
            reviewer: selected,
            executor: command.executor,
            assigning_leader: command.assigning_leader,
            qualification_domain: command.qualification_domain.clone(),
            rule_generation,
            revision: 1,
            assigned_at_ms: command.assigned_at_ms,
            verdict: None,
            replayed: false,
        };
        insert_assignment_row(&tx, &fresh, command.request_id, digest)?;
        finalize_operation_commit(
            tx.commit(),
            fresh,
            command.correlation.workspace_id(),
            command.request_id,
            || load_assignment_replay(&self.connection, command.request_id, digest),
        )
        .map_err(ReviewerAssignmentError::from)
    }

    /// Record a diff-digest-bound reviewer verdict using compare-and-swap on `expected_revision`.
    ///
    /// # Errors
    /// Returns [`ReviewerAssignmentError::SelfReview`] if the caller violates independence,
    /// [`ReviewerAssignmentError::StaleRevision`] on CAS mismatch, or [`OperationsError`] on
    /// invalid inputs, unassigned correlation, mismatched reviewer identity, or storage failure.
    pub fn record_review_verdict(
        &mut self,
        command: &ReviewVerdictCommand,
    ) -> Result<ReviewerAssignmentRecord, ReviewerAssignmentError> {
        command.validate()?;
        let digest = verdict_digest(command);
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(OperationsError::from)?;
        if let Some(replayed) = load_verdict_replay(&tx, command.request_id, digest)? {
            tx.commit().map_err(OperationsError::from)?;
            return Ok(replayed);
        }
        let current = load_assignment_by_correlation(&tx, &command.correlation)?.ok_or(
            OperationsError::Conflict("reviewer assignment missing for correlation"),
        )?;
        verify_verdict_caller(&current, command)?;
        if current.revision != command.expected_revision {
            return Err(ReviewerAssignmentError::StaleRevision);
        }
        let next_revision = current
            .revision
            .checked_add(1)
            .ok_or(OperationsError::Bounds("reviewer assignment revision"))?;
        let verdict = RecordedReviewVerdict::new(
            command.request_id,
            command.outcome,
            command.handoff_diff_digest,
            command.rationale.clone(),
            command.recorded_at_ms,
        )?;
        persist_verdict_transition(
            &tx,
            &current,
            &verdict,
            command.expected_revision,
            next_revision,
            digest,
        )?;
        let fresh = ReviewerAssignmentRecord {
            revision: next_revision,
            verdict: Some(verdict),
            replayed: false,
            ..current
        };
        finalize_operation_commit(
            tx.commit(),
            fresh,
            command.correlation.workspace_id(),
            command.request_id,
            || load_verdict_replay(&self.connection, command.request_id, digest),
        )
        .map_err(ReviewerAssignmentError::from)
    }

    /// Verify that `correlation` holds an `Accepted` independent review matching the live worktree diff.
    ///
    /// # Errors
    /// Fails closed with [`ReviewerAssignmentError::ReviewNotAccepted`] when unassigned or not
    /// `Accepted`, [`ReviewerAssignmentError::VerdictDiffDigestMismatch`] when `current_worktree_diff_digest`
    /// is zero or differs from `handoff_diff_digest`, or [`ReviewerAssignmentError::SelfReview`]
    /// if stored identities violate independence.
    pub fn verify_node_completion_review_gate(
        &self,
        correlation: &WorkCorrelationKey,
        current_worktree_diff_digest: [u8; 32],
    ) -> Result<ReviewerAssignmentRecord, ReviewerAssignmentError> {
        correlation.validate()?;
        let Some(record) = self.get_reviewer_assignment(correlation)? else {
            return Err(ReviewerAssignmentError::ReviewNotAccepted);
        };
        check_independence(
            record.reviewer.profile_id,
            record.reviewer.participant_id,
            record.reviewer.principal,
            &record.executor,
            &record.assigning_leader,
        )?;
        let Some(verdict) = &record.verdict else {
            return Err(ReviewerAssignmentError::ReviewNotAccepted);
        };
        if verdict.outcome != ReviewVerdictOutcome::Accepted {
            return Err(ReviewerAssignmentError::ReviewNotAccepted);
        }
        if current_worktree_diff_digest == [0_u8; 32]
            || verdict.handoff_diff_digest != current_worktree_diff_digest
        {
            return Err(ReviewerAssignmentError::VerdictDiffDigestMismatch);
        }
        Ok(record)
    }

    /// Read the current durable [`ReviewerAssignmentRecord`] for `correlation`, if assigned.
    ///
    /// # Errors
    /// Rejects invalid `correlation` keys, corrupt persisted rows, and SQLite read failures.
    pub fn get_reviewer_assignment(
        &self,
        correlation: &WorkCorrelationKey,
    ) -> Result<Option<ReviewerAssignmentRecord>, OperationsError> {
        correlation.validate()?;
        let tx = self.connection.unchecked_transaction()?;
        let record = load_assignment_by_correlation(&tx, correlation)?;
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(record)
    }

    /// Verify that `(agent_id, session_id)` matches the active `'INTERESTED'` reviewer
    /// participation head receipt for `(workspace_id, participation_id)`.
    ///
    /// # Errors
    /// Returns `ReviewerAssignmentError::Operations(OperationsError::Conflict("review verdict caller does not match assigned reviewer"))`
    /// when the participation head is missing, withdrawn, or bound to a different `(agent_id, session_id)`,
    /// or corrupt/database errors on malformed persisted state or read failure.
    pub fn verify_reviewer_session_binding(
        &self,
        workspace_id: WorkspaceId,
        participation_id: WorkspaceParticipationId,
        agent_id: Uuid,
        session_id: Uuid,
    ) -> Result<(), ReviewerAssignmentError> {
        if workspace_id.into_uuid().is_nil()
            || participation_id.into_uuid().is_nil()
            || agent_id.is_nil()
            || session_id.is_nil()
        {
            return Err(OperationsError::Conflict(
                "review verdict caller does not match assigned reviewer",
            )
            .into());
        }
        let tx = self
            .connection
            .unchecked_transaction()
            .map_err(OperationsError::from)?;
        let head: Option<(String, Vec<u8>, Vec<u8>)> = tx
            .query_row(
                SELECT_REVIEWER_SESSION_BINDING_SQL,
                params![
                    workspace_id.into_uuid().as_bytes(),
                    participation_id.into_uuid().as_bytes(),
                ],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(OperationsError::from)?;
        crate::catalog::commit_read(tx, self.destination)?;
        let Some((statement, raw_agent, raw_session)) = head else {
            return Err(OperationsError::Conflict(
                "review verdict caller does not match assigned reviewer",
            )
            .into());
        };
        let stored_agent = decode_non_nil_uuid(&raw_agent, "reviewer participation agent_id")?;
        let stored_session =
            decode_non_nil_uuid(&raw_session, "reviewer participation session_id")?;
        match statement.as_str() {
            "INTERESTED" if stored_agent == agent_id && stored_session == session_id => Ok(()),
            "INTERESTED" | "WITHDRAW_INTEREST" => Err(OperationsError::Conflict(
                "review verdict caller does not match assigned reviewer",
            )
            .into()),
            _ => Err(OperationsError::Corrupt("reviewer participation statement").into()),
        }
    }
}

fn select_independent_candidate(
    tx: &Transaction<'_>,
    command: &ReviewerAssignmentCommand,
) -> Result<(ReviewerCandidateIdentity, Option<u64>), ReviewerAssignmentError> {
    let deduped = deduplicate_candidates(&command.candidates)?;
    let mut independent = Vec::with_capacity(deduped.len());
    let mut first_self_reason: Option<&'static str> = None;
    for candidate in deduped {
        match check_independence(
            candidate.profile_id,
            candidate.participant_id,
            candidate.principal,
            &command.executor,
            &command.assigning_leader,
        ) {
            Ok(()) => independent.push(candidate),
            Err(ReviewerAssignmentError::SelfReview { reason }) => {
                first_self_reason.get_or_insert(reason);
            }
            Err(other) => return Err(other),
        }
    }
    if independent.is_empty()
        && let Some(reason) = first_self_reason
    {
        return Err(ReviewerAssignmentError::SelfReview { reason });
    }
    let rule_generation = match &command.qualification_domain {
        Some(domain) => Some(resolve_active_rule_generation(
            tx,
            command.correlation.workspace_id(),
            domain,
        )?),
        None => None,
    };
    if independent.is_empty() {
        return Err(ReviewerAssignmentError::NoQualifiedReviewer);
    }
    let mut eligible =
        filter_interested_candidates(tx, command.correlation.workspace_id(), &independent)?;
    eligible.sort_by_key(|candidate| {
        (
            candidate.participation_id.into_uuid(),
            candidate.profile_id.into_uuid(),
            candidate.participant_id,
            principal_sort_key(candidate.principal),
        )
    });
    let selected = eligible
        .first()
        .copied()
        .ok_or(ReviewerAssignmentError::NoQualifiedReviewer)?;
    Ok((selected, rule_generation))
}

fn deduplicate_candidates(
    candidates: &[ReviewerCandidateIdentity],
) -> Result<Vec<ReviewerCandidateIdentity>, OperationsError> {
    let mut by_participation: BTreeMap<Uuid, ReviewerCandidateIdentity> = BTreeMap::new();
    let mut by_profile: BTreeMap<Uuid, ReviewerCandidateIdentity> = BTreeMap::new();
    for &candidate in candidates {
        let participation_key = candidate.participation_id.into_uuid();
        if let Some(existing) = by_participation.get(&participation_key)
            && *existing != candidate
        {
            return Err(OperationsError::Conflict(
                "conflicting duplicate reviewer candidate participation",
            ));
        }
        let profile_key = candidate.profile_id.into_uuid();
        if let Some(existing) = by_profile.get(&profile_key)
            && *existing != candidate
        {
            return Err(OperationsError::Conflict(
                "conflicting duplicate reviewer candidate profile",
            ));
        }
        by_participation.insert(participation_key, candidate);
        by_profile.insert(profile_key, candidate);
    }
    Ok(by_participation.into_values().collect())
}

fn check_independence(
    profile_id: AgentProfileId,
    participant_id: Uuid,
    principal: AuthenticatedPrincipal,
    executor: &ExecutionAuthorIdentity,
    leader: &ExecutionAuthorIdentity,
) -> Result<(), ReviewerAssignmentError> {
    if profile_id == executor.profile_id {
        return Err(ReviewerAssignmentError::SelfReview {
            reason: "reviewer shares AgentProfileId with executor",
        });
    }
    if participant_id == executor.participant_id {
        return Err(ReviewerAssignmentError::SelfReview {
            reason: "reviewer shares participant_id with executor",
        });
    }
    if principal == executor.principal {
        return Err(ReviewerAssignmentError::SelfReview {
            reason: "reviewer shares AuthenticatedPrincipal with executor",
        });
    }
    if profile_id == leader.profile_id {
        return Err(ReviewerAssignmentError::SelfReview {
            reason: "reviewer shares AgentProfileId with assigning Team Leader",
        });
    }
    if participant_id == leader.participant_id {
        return Err(ReviewerAssignmentError::SelfReview {
            reason: "reviewer shares participant_id with assigning Team Leader",
        });
    }
    if principal == leader.principal {
        return Err(ReviewerAssignmentError::SelfReview {
            reason: "reviewer shares AuthenticatedPrincipal with assigning Team Leader",
        });
    }
    Ok(())
}

fn verify_verdict_caller(
    current: &ReviewerAssignmentRecord,
    command: &ReviewVerdictCommand,
) -> Result<(), ReviewerAssignmentError> {
    check_independence(
        command.reviewer_profile_id,
        command.reviewer_participant_id,
        command.reviewer_principal,
        &current.executor,
        &current.assigning_leader,
    )?;
    if command.reviewer_profile_id != current.reviewer.profile_id
        || command.reviewer_participant_id != current.reviewer.participant_id
        || command.reviewer_principal != current.reviewer.principal
    {
        return Err(OperationsError::Conflict(
            "review verdict caller does not match assigned reviewer",
        )
        .into());
    }
    Ok(())
}

fn resolve_active_rule_generation(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    domain: &QualificationDomain,
) -> Result<u64, ReviewerAssignmentError> {
    let raw_generation: Option<i64> = tx
        .query_row(
            SELECT_ACTIVE_RULE_GENERATION_SQL,
            params![workspace_id.into_uuid().as_bytes(), domain.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(OperationsError::from)?;
    let Some(generation) = raw_generation else {
        return Err(ReviewerAssignmentError::QualificationMissing);
    };
    decode_positive_u64(generation, "qualification rule generation")
        .map_err(ReviewerAssignmentError::from)
}

fn filter_interested_candidates(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    candidates: &[ReviewerCandidateIdentity],
) -> Result<Vec<ReviewerCandidateIdentity>, OperationsError> {
    let mut statement = tx.prepare(SELECT_REVIEWER_PARTICIPATION_HEAD_SQL)?;
    let mut eligible = Vec::with_capacity(candidates.len());
    for &candidate in candidates {
        let head: Option<(Vec<u8>, String)> = statement
            .query_row(
                params![
                    workspace_id.into_uuid().as_bytes(),
                    candidate.participation_id.into_uuid().as_bytes(),
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((profile_bytes, status)) = head else {
            continue;
        };
        let stored_profile = decode_non_nil_uuid(&profile_bytes, "reviewer participation profile")?;
        if stored_profile != candidate.profile_id.into_uuid() {
            return Err(OperationsError::Conflict(
                "reviewer candidate profile does not match durable participation",
            ));
        }
        match status.as_str() {
            "INTERESTED" => eligible.push(candidate),
            "WITHDRAW_INTEREST" => {}
            _ => return Err(OperationsError::Corrupt("reviewer participation statement")),
        }
    }
    Ok(eligible)
}

fn insert_assignment_row(
    tx: &Transaction<'_>,
    record: &ReviewerAssignmentRecord,
    request_id: Uuid,
    payload_digest: [u8; 32],
) -> Result<(), OperationsError> {
    let (rev_kind, rev_id) = encode_principal(record.reviewer.principal);
    let (exec_kind, exec_id) = encode_principal(record.executor.principal);
    let (lead_kind, lead_id) = encode_principal(record.assigning_leader.principal);
    let rule_gen_i64 = record
        .rule_generation
        .map(crate::catalog::signed)
        .transpose()?;
    tx.execute(
        INSERT_REVIEWER_ASSIGNMENT_SQL,
        params![
            record.assignment_record_id.as_bytes(),
            request_id.as_bytes(),
            payload_digest.as_slice(),
            record.correlation.workspace_id.into_uuid().as_bytes(),
            record.correlation.goal_id.into_uuid().as_bytes(),
            record.correlation.dag_revision.get(),
            record.correlation.node_id.into_uuid().as_bytes(),
            record.correlation.assignment_id.into_uuid().as_bytes(),
            i64::from(record.correlation.attempt),
            record.reviewer.participation_id.into_uuid().as_bytes(),
            record.reviewer.profile_id.into_uuid().as_bytes(),
            record.reviewer.participant_id.as_bytes(),
            rev_kind,
            rev_id.as_ref().map(Uuid::as_bytes),
            record.executor.profile_id.into_uuid().as_bytes(),
            record.executor.participant_id.as_bytes(),
            exec_kind,
            exec_id.as_ref().map(Uuid::as_bytes),
            record.assigning_leader.profile_id.into_uuid().as_bytes(),
            record.assigning_leader.participant_id.as_bytes(),
            lead_kind,
            lead_id.as_ref().map(Uuid::as_bytes),
            record
                .qualification_domain
                .as_ref()
                .map(QualificationDomain::as_str),
            rule_gen_i64,
            record.assigned_at_ms,
        ],
    )?;
    Ok(())
}

fn persist_verdict_transition(
    tx: &Transaction<'_>,
    current: &ReviewerAssignmentRecord,
    verdict: &RecordedReviewVerdict,
    expected_revision: u64,
    next_revision: u64,
    payload_digest: [u8; 32],
) -> Result<(), ReviewerAssignmentError> {
    let expected_i64 = crate::catalog::signed(expected_revision)?;
    let next_i64 = crate::catalog::signed(next_revision)?;
    tx.execute(
        INSERT_REVIEWER_VERDICT_RECEIPT_SQL,
        params![
            verdict.request_id.as_bytes(),
            current.assignment_record_id.as_bytes(),
            expected_i64,
            next_i64,
            verdict.outcome.as_str(),
            verdict.handoff_diff_digest.as_slice(),
            verdict.rationale.as_str(),
            payload_digest.as_slice(),
            verdict.recorded_at_ms,
        ],
    )
    .map_err(OperationsError::from)?;
    let updated = tx
        .execute(
            UPDATE_REVIEWER_ASSIGNMENT_VERDICT_SQL,
            params![
                current.assignment_record_id.as_bytes(),
                next_i64,
                verdict.request_id.as_bytes(),
                expected_i64,
            ],
        )
        .map_err(OperationsError::from)?;
    if updated != 1 {
        return Err(ReviewerAssignmentError::StaleRevision);
    }
    Ok(())
}

struct RawAssignmentRow {
    assignment_record_id: Vec<u8>,
    payload_digest: Vec<u8>,
    workspace_id: Vec<u8>,
    goal_id: Vec<u8>,
    dag_revision: i64,
    node_id: Vec<u8>,
    assignment_id: Vec<u8>,
    attempt: i64,
    reviewer_participation_id: Vec<u8>,
    reviewer_profile_id: Vec<u8>,
    reviewer_participant_id: Vec<u8>,
    reviewer_principal_kind: i64,
    reviewer_principal_id: Option<Vec<u8>>,
    executor_profile_id: Vec<u8>,
    executor_participant_id: Vec<u8>,
    executor_principal_kind: i64,
    executor_principal_id: Option<Vec<u8>>,
    leader_profile_id: Vec<u8>,
    leader_participant_id: Vec<u8>,
    leader_principal_kind: i64,
    leader_principal_id: Option<Vec<u8>>,
    qualification_domain: Option<String>,
    rule_generation: Option<i64>,
    revision: i64,
    assigned_at_ms: i64,
    current_verdict_request_id: Option<Vec<u8>>,
    verdict_request_id: Option<Vec<u8>>,
    verdict_outcome: Option<String>,
    verdict_diff_digest: Option<Vec<u8>>,
    verdict_rationale: Option<String>,
    verdict_recorded_at_ms: Option<i64>,
}

fn read_raw_assignment_row(row: &Row<'_>) -> rusqlite::Result<RawAssignmentRow> {
    Ok(RawAssignmentRow {
        assignment_record_id: row.get(0)?,
        payload_digest: row.get(1)?,
        workspace_id: row.get(2)?,
        goal_id: row.get(3)?,
        dag_revision: row.get(4)?,
        node_id: row.get(5)?,
        assignment_id: row.get(6)?,
        attempt: row.get(7)?,
        reviewer_participation_id: row.get(8)?,
        reviewer_profile_id: row.get(9)?,
        reviewer_participant_id: row.get(10)?,
        reviewer_principal_kind: row.get(11)?,
        reviewer_principal_id: row.get(12)?,
        executor_profile_id: row.get(13)?,
        executor_participant_id: row.get(14)?,
        executor_principal_kind: row.get(15)?,
        executor_principal_id: row.get(16)?,
        leader_profile_id: row.get(17)?,
        leader_participant_id: row.get(18)?,
        leader_principal_kind: row.get(19)?,
        leader_principal_id: row.get(20)?,
        qualification_domain: row.get(21)?,
        rule_generation: row.get(22)?,
        revision: row.get(23)?,
        assigned_at_ms: row.get(24)?,
        current_verdict_request_id: row.get(25)?,
        verdict_request_id: row.get(26)?,
        verdict_outcome: row.get(27)?,
        verdict_diff_digest: row.get(28)?,
        verdict_rationale: row.get(29)?,
        verdict_recorded_at_ms: row.get(30)?,
    })
}

fn decode_assignment_row(
    raw: RawAssignmentRow,
) -> Result<(ReviewerAssignmentRecord, [u8; 32]), OperationsError> {
    let correlation = decode_correlation(&raw)?;
    let reviewer = decode_reviewer_candidate(&raw)?;
    let record_id = decode_non_nil_uuid(&raw.assignment_record_id, "assignment_record_id")?;
    let digest = decode_non_zero_digest(&raw.payload_digest, "assignment payload_digest")?;
    let executor = decode_author(
        &raw.executor_profile_id,
        &raw.executor_participant_id,
        raw.executor_principal_kind,
        raw.executor_principal_id.as_deref(),
        "executor identity",
    )?;
    let assigning_leader = decode_author(
        &raw.leader_profile_id,
        &raw.leader_participant_id,
        raw.leader_principal_kind,
        raw.leader_principal_id.as_deref(),
        "leader identity",
    )?;
    let (qualification_domain, rule_generation) =
        decode_qualification(raw.qualification_domain, raw.rule_generation)?;
    let revision = decode_positive_u64(raw.revision, "assignment revision")?;
    if raw.assigned_at_ms < 0 {
        return Err(OperationsError::Corrupt("assignment assigned_at_ms"));
    }
    let verdict = decode_attached_verdict(
        revision,
        raw.current_verdict_request_id.as_deref(),
        raw.verdict_request_id.as_deref(),
        raw.verdict_outcome.as_deref(),
        raw.verdict_diff_digest.as_deref(),
        raw.verdict_rationale,
        raw.verdict_recorded_at_ms,
    )?;
    Ok((
        ReviewerAssignmentRecord {
            assignment_record_id: record_id,
            correlation,
            reviewer,
            executor,
            assigning_leader,
            qualification_domain,
            rule_generation,
            revision,
            assigned_at_ms: raw.assigned_at_ms,
            verdict,
            replayed: false,
        },
        digest,
    ))
}

fn decode_correlation(raw: &RawAssignmentRow) -> Result<WorkCorrelationKey, OperationsError> {
    let workspace_id = WorkspaceId::from_uuid(decode_non_nil_uuid(
        &raw.workspace_id,
        "assignment workspace_id",
    )?);
    let goal_id = GoalId::from_uuid(decode_non_nil_uuid(&raw.goal_id, "assignment goal_id")?);
    let dag_revision = DagRevisionNumber::new(raw.dag_revision)
        .map_err(|_| OperationsError::Corrupt("assignment dag_revision"))?;
    let node_id = DagNodeId::from_uuid(decode_non_nil_uuid(&raw.node_id, "assignment node_id")?);
    let assignment_id = AgentAssignmentId::from_uuid(decode_non_nil_uuid(
        &raw.assignment_id,
        "assignment assignment_id",
    )?);
    let attempt = u32::try_from(raw.attempt)
        .ok()
        .filter(|value| *value >= 1)
        .ok_or(OperationsError::Corrupt("assignment attempt"))?;
    WorkCorrelationKey::new(
        workspace_id,
        goal_id,
        dag_revision,
        node_id,
        assignment_id,
        attempt,
    )
    .map_err(|_| OperationsError::Corrupt("assignment correlation"))
}

fn decode_reviewer_candidate(
    raw: &RawAssignmentRow,
) -> Result<ReviewerCandidateIdentity, OperationsError> {
    let participation_uuid =
        decode_non_nil_uuid(&raw.reviewer_participation_id, "reviewer_participation_id")?;
    let participation_id = WorkspaceParticipationId::from_uuid(participation_uuid)
        .map_err(|_| OperationsError::Corrupt("reviewer_participation_id"))?;
    let profile_uuid = decode_non_nil_uuid(&raw.reviewer_profile_id, "reviewer_profile_id")?;
    let profile_id = AgentProfileId::from_uuid(profile_uuid)
        .map_err(|_| OperationsError::Corrupt("reviewer_profile_id"))?;
    let participant_id =
        decode_non_nil_uuid(&raw.reviewer_participant_id, "reviewer_participant_id")?;
    let principal = decode_principal(
        raw.reviewer_principal_kind,
        raw.reviewer_principal_id.as_deref(),
        "reviewer principal",
    )?;
    ReviewerCandidateIdentity::new(participation_id, profile_id, participant_id, principal)
        .map_err(|_| OperationsError::Corrupt("reviewer candidate"))
}

fn decode_author(
    profile_bytes: &[u8],
    participant_bytes: &[u8],
    principal_kind: i64,
    principal_bytes: Option<&[u8]>,
    label: &'static str,
) -> Result<ExecutionAuthorIdentity, OperationsError> {
    let profile_uuid = decode_non_nil_uuid(profile_bytes, label)?;
    let profile_id =
        AgentProfileId::from_uuid(profile_uuid).map_err(|_| OperationsError::Corrupt(label))?;
    let participant_id = decode_non_nil_uuid(participant_bytes, label)?;
    let principal = decode_principal(principal_kind, principal_bytes, label)?;
    ExecutionAuthorIdentity::new(profile_id, participant_id, principal)
        .map_err(|_| OperationsError::Corrupt(label))
}

fn decode_qualification(
    domain_raw: Option<String>,
    generation_raw: Option<i64>,
) -> Result<(Option<QualificationDomain>, Option<u64>), OperationsError> {
    match (domain_raw, generation_raw) {
        (None, None) => Ok((None, None)),
        (Some(text), Some(generation)) => {
            let domain = QualificationDomain::new(text)
                .map_err(|_| OperationsError::Corrupt("assignment qualification_domain"))?;
            let parsed_gen = decode_positive_u64(generation, "assignment rule_generation")?;
            Ok((Some(domain), Some(parsed_gen)))
        }
        _ => Err(OperationsError::Corrupt(
            "assignment qualification_domain/rule_generation pair",
        )),
    }
}

fn decode_attached_verdict(
    revision: u64,
    pointer_id: Option<&[u8]>,
    verdict_request_id: Option<&[u8]>,
    outcome_raw: Option<&str>,
    diff_raw: Option<&[u8]>,
    rationale_raw: Option<String>,
    recorded_at_ms: Option<i64>,
) -> Result<Option<RecordedReviewVerdict>, OperationsError> {
    if revision == 1 {
        if pointer_id.is_some() || verdict_request_id.is_some() {
            return Err(OperationsError::Corrupt("unexpected verdict at revision 1"));
        }
        return Ok(None);
    }
    let pointer_uuid = decode_non_nil_uuid(
        pointer_id.ok_or(OperationsError::Corrupt(
            "missing current_verdict_request_id",
        ))?,
        "current_verdict_request_id",
    )?;
    let receipt_uuid = decode_non_nil_uuid(
        verdict_request_id.ok_or(OperationsError::Corrupt("missing joined verdict receipt"))?,
        "verdict request_id",
    )?;
    if pointer_uuid != receipt_uuid {
        return Err(OperationsError::Corrupt("verdict receipt pointer mismatch"));
    }
    let outcome = ReviewVerdictOutcome::parse(
        outcome_raw.ok_or(OperationsError::Corrupt("verdict outcome"))?,
    )?;
    let digest = decode_non_zero_digest(
        diff_raw.ok_or(OperationsError::Corrupt("verdict handoff_diff_digest"))?,
        "verdict handoff_diff_digest",
    )?;
    let rationale = rationale_raw.ok_or(OperationsError::Corrupt("verdict rationale"))?;
    let recorded = recorded_at_ms.ok_or(OperationsError::Corrupt("verdict recorded_at_ms"))?;
    RecordedReviewVerdict::new(receipt_uuid, outcome, digest, rationale, recorded)
        .map(Some)
        .map_err(|_| OperationsError::Corrupt("stored review verdict"))
}

fn load_assignment_by_correlation(
    connection: &Connection,
    correlation: &WorkCorrelationKey,
) -> Result<Option<ReviewerAssignmentRecord>, OperationsError> {
    let raw = connection
        .query_row(
            SELECT_ASSIGNMENT_BY_CORRELATION_SQL,
            params![
                correlation.workspace_id.into_uuid().as_bytes(),
                correlation.goal_id.into_uuid().as_bytes(),
                correlation.dag_revision.get(),
                correlation.node_id.into_uuid().as_bytes(),
                correlation.assignment_id.into_uuid().as_bytes(),
                i64::from(correlation.attempt),
            ],
            read_raw_assignment_row,
        )
        .optional()?;
    raw.map(|row| decode_assignment_row(row).map(|(record, _)| record))
        .transpose()
}

fn load_assignment_by_record_id(
    connection: &Connection,
    assignment_record_id: Uuid,
) -> Result<Option<ReviewerAssignmentRecord>, OperationsError> {
    let raw = connection
        .query_row(
            SELECT_ASSIGNMENT_BY_RECORD_ID_SQL,
            params![assignment_record_id.as_bytes()],
            read_raw_assignment_row,
        )
        .optional()?;
    raw.map(|row| decode_assignment_row(row).map(|(record, _)| record))
        .transpose()
}

fn load_assignment_replay(
    connection: &Connection,
    request_id: Uuid,
    expected_digest: [u8; 32],
) -> Result<Option<ReviewerAssignmentRecord>, OperationsError> {
    let raw = connection
        .query_row(
            SELECT_ASSIGNMENT_BY_REQUEST_ID_SQL,
            params![request_id.as_bytes()],
            read_raw_assignment_row,
        )
        .optional()?;
    let Some(raw_row) = raw else {
        return Ok(None);
    };
    let (mut record, stored_digest) = decode_assignment_row(raw_row)?;
    if stored_digest != expected_digest {
        return Err(OperationsError::Conflict(
            "reviewer assignment request_id reused with different payload",
        ));
    }
    record.replayed = true;
    Ok(Some(record))
}

fn load_verdict_replay(
    connection: &Connection,
    request_id: Uuid,
    expected_digest: [u8; 32],
) -> Result<Option<ReviewerAssignmentRecord>, OperationsError> {
    let raw = connection
        .query_row(
            SELECT_VERDICT_RECEIPT_BY_REQUEST_ID_SQL,
            params![request_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            },
        )
        .optional()?;
    let Some((
        record_bytes,
        resulting_rev,
        outcome_raw,
        diff_bytes,
        rationale,
        digest_bytes,
        recorded_at,
    )) = raw
    else {
        return Ok(None);
    };
    let stored_digest = decode_non_zero_digest(&digest_bytes, "verdict payload_digest")?;
    if stored_digest != expected_digest {
        return Err(OperationsError::Conflict(
            "review verdict request_id reused with different payload",
        ));
    }
    let record_id = decode_non_nil_uuid(&record_bytes, "verdict assignment_record_id")?;
    let mut assignment = load_assignment_by_record_id(connection, record_id)?.ok_or(
        OperationsError::Corrupt("assignment missing for verdict receipt"),
    )?;
    let verdict = RecordedReviewVerdict::new(
        request_id,
        ReviewVerdictOutcome::parse(&outcome_raw)?,
        decode_non_zero_digest(&diff_bytes, "verdict handoff_diff_digest")?,
        rationale,
        recorded_at,
    )
    .map_err(|_| OperationsError::Corrupt("stored verdict receipt"))?;
    assignment.revision = decode_positive_u64(resulting_rev, "verdict resulting_revision")?;
    assignment.verdict = Some(verdict);
    assignment.replayed = true;
    Ok(Some(assignment))
}

fn validate_verdict_fields(
    request_id: Uuid,
    handoff_diff_digest: [u8; 32],
    rationale: &str,
    recorded_at_ms: i64,
) -> Result<(), OperationsError> {
    if request_id.is_nil() || recorded_at_ms < 0 {
        return Err(OperationsError::Bounds("review verdict metadata"));
    }
    if handoff_diff_digest == [0_u8; 32] {
        return Err(OperationsError::Bounds("review verdict zero diff digest"));
    }
    if rationale.trim().is_empty()
        || rationale.len() > MAX_REVIEW_VERDICT_RATIONALE_BYTES
        || rationale.contains('\0')
    {
        return Err(OperationsError::Bounds("review verdict rationale"));
    }
    Ok(())
}

fn principal_sort_key(principal: AuthenticatedPrincipal) -> (u8, Uuid) {
    match principal {
        AuthenticatedPrincipal::TrustedStdio => (0, Uuid::nil()),
        AuthenticatedPrincipal::DirectToken(id) => (1, id.into_uuid()),
        AuthenticatedPrincipal::OAuthGrant(id) => (2, id.into_uuid()),
    }
}

fn assignment_digest(command: &ReviewerAssignmentCommand) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(ASSIGNMENT_DIGEST_DOMAIN);
    hash_correlation(&mut hasher, &command.correlation);
    hash_author(&mut hasher, &command.assigning_leader);
    hash_author(&mut hasher, &command.executor);
    hasher.update(len_u64(command.candidates.len()));
    for candidate in &command.candidates {
        hasher.update(candidate.participation_id.into_uuid().as_bytes());
        hasher.update(candidate.profile_id.into_uuid().as_bytes());
        hasher.update(candidate.participant_id.as_bytes());
        hash_principal(&mut hasher, candidate.principal);
    }
    match &command.qualification_domain {
        Some(domain) => {
            hasher.update([1_u8]);
            hash_str(&mut hasher, domain.as_str());
        }
        None => hasher.update([0_u8]),
    }
    hasher.update(command.assigned_at_ms.to_le_bytes());
    hasher.finalize().into()
}

fn verdict_digest(command: &ReviewVerdictCommand) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(VERDICT_DIGEST_DOMAIN);
    hash_correlation(&mut hasher, &command.correlation);
    hasher.update(command.expected_revision.to_le_bytes());
    hash_principal(&mut hasher, command.reviewer_principal);
    hasher.update(command.reviewer_profile_id.into_uuid().as_bytes());
    hasher.update(command.reviewer_participant_id.as_bytes());
    hasher.update(command.outcome.as_str().as_bytes());
    hasher.update([0_u8]);
    hasher.update(command.handoff_diff_digest);
    hash_str(&mut hasher, &command.rationale);
    hasher.update(command.recorded_at_ms.to_le_bytes());
    hasher.finalize().into()
}

fn hash_correlation(hasher: &mut Sha256, correlation: &WorkCorrelationKey) {
    hasher.update(correlation.workspace_id.into_uuid().as_bytes());
    hasher.update(correlation.goal_id.into_uuid().as_bytes());
    hasher.update(correlation.dag_revision.get().to_le_bytes());
    hasher.update(correlation.node_id.into_uuid().as_bytes());
    hasher.update(correlation.assignment_id.into_uuid().as_bytes());
    hasher.update(correlation.attempt.to_le_bytes());
}

fn hash_author(hasher: &mut Sha256, author: &ExecutionAuthorIdentity) {
    hasher.update(author.profile_id.into_uuid().as_bytes());
    hasher.update(author.participant_id.as_bytes());
    hash_principal(hasher, author.principal);
}

fn hash_principal(hasher: &mut Sha256, principal: AuthenticatedPrincipal) {
    let (tag, id) = principal_sort_key(principal);
    hasher.update([tag]);
    hasher.update(id.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_domain::{DirectTokenId, OAuthGrantId};
    use std::error::Error;
    use std::path::PathBuf;

    type TestResult = Result<(), Box<dyn Error>>;

    const DESTINATION_ID: Uuid = Uuid::from_u128(0x7100);
    const WORKSPACE_UUID: Uuid = Uuid::from_u128(0x7101);
    const GOAL_UUID: Uuid = Uuid::from_u128(0x7102);
    const NODE_UUID: Uuid = Uuid::from_u128(0x7103);
    const ASSIGNMENT_UUID: Uuid = Uuid::from_u128(0x7104);

    struct TempCatalog {
        dir: PathBuf,
    }

    impl TempCatalog {
        fn open() -> Result<(Self, OperationsCatalog), Box<dyn Error>> {
            let dir =
                std::env::temp_dir().join(format!("reviewer-assignment-test-{}", Uuid::now_v7()));
            std::fs::create_dir_all(&dir)?;
            let guard = Self { dir };
            let catalog =
                OperationsCatalog::open(&guard.dir.join("operations.sqlite3"), DESTINATION_ID)?;
            Ok((guard, catalog))
        }
    }

    impl Drop for TempCatalog {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    struct AssignmentFixture {
        correlation: WorkCorrelationKey,
        leader: ExecutionAuthorIdentity,
        executor: ExecutionAuthorIdentity,
    }

    impl AssignmentFixture {
        fn standard() -> Result<Self, Box<dyn Error>> {
            let correlation = WorkCorrelationKey::new(
                WorkspaceId::from_uuid(WORKSPACE_UUID),
                GoalId::from_uuid(GOAL_UUID),
                DagRevisionNumber::INITIAL,
                DagNodeId::from_uuid(NODE_UUID),
                AgentAssignmentId::from_uuid(ASSIGNMENT_UUID),
                1,
            )?;
            let leader = ExecutionAuthorIdentity::new(
                AgentProfileId::from_uuid(Uuid::from_u128(0x7201))?,
                Uuid::from_u128(0x7301),
                AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(
                    0x7401,
                ))),
            )?;
            let executor = ExecutionAuthorIdentity::new(
                AgentProfileId::from_uuid(Uuid::from_u128(0x7202))?,
                Uuid::from_u128(0x7302),
                AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(
                    0x7402,
                ))),
            )?;
            Ok(Self {
                correlation,
                leader,
                executor,
            })
        }
    }

    fn seed_participation_statement(
        catalog: &OperationsCatalog,
        candidate: &ReviewerCandidateIdentity,
        interested: bool,
        seed_tag: u128,
    ) -> TestResult {
        catalog.connection.execute(
            "INSERT OR IGNORE INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,1,1)",
            params![
                WORKSPACE_UUID.as_bytes(),
                candidate.profile_id().into_uuid().as_bytes(),
                candidate.participation_id().into_uuid().as_bytes(),
            ],
        )?;
        let req_one = Uuid::from_u128(0x8000_0000 + seed_tag * 2);
        let activity_id = Uuid::from_u128(0x8100_0000 + seed_tag);
        let agent_id = Uuid::from_u128(0x8200_0000 + seed_tag);
        let session_id = Uuid::from_u128(0x8300_0000 + seed_tag);
        catalog.connection.execute(
            "INSERT INTO ops_reviewer_participation_receipts(workspace_id,participation_id,profile_id,participant_id,request_id,revision,expected_revision,statement,withdraws_request_id,withdraws_revision,activity_id,agent_id,session_id,display_name,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,1,0,'INTERESTED',NULL,NULL,?6,?7,?8,'Reviewer',10)",
            params![
                WORKSPACE_UUID.as_bytes(),
                candidate.participation_id().into_uuid().as_bytes(),
                candidate.profile_id().into_uuid().as_bytes(),
                candidate.participant_id().as_bytes(),
                req_one.as_bytes(),
                activity_id.as_bytes(),
                agent_id.as_bytes(),
                session_id.as_bytes(),
            ],
        )?;
        let (head_revision, head_request_id) = if interested {
            (1_i64, req_one)
        } else {
            let req_two = Uuid::from_u128(0x8000_0001 + seed_tag * 2);
            catalog.connection.execute(
                "INSERT INTO ops_reviewer_participation_receipts(workspace_id,participation_id,profile_id,participant_id,request_id,revision,expected_revision,statement,withdraws_request_id,withdraws_revision,activity_id,agent_id,session_id,display_name,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,2,1,'WITHDRAW_INTEREST',?6,1,?7,?8,?9,'Reviewer',20)",
                params![
                    WORKSPACE_UUID.as_bytes(),
                    candidate.participation_id().into_uuid().as_bytes(),
                    candidate.profile_id().into_uuid().as_bytes(),
                    candidate.participant_id().as_bytes(),
                    req_two.as_bytes(),
                    req_one.as_bytes(),
                    activity_id.as_bytes(),
                    agent_id.as_bytes(),
                    session_id.as_bytes(),
                ],
            )?;
            (2_i64, req_two)
        };
        catalog.connection.execute(
            "INSERT INTO ops_reviewer_participation_heads(workspace_id,participation_id,current_revision,request_id) VALUES(?1,?2,?3,?4)",
            params![
                WORKSPACE_UUID.as_bytes(),
                candidate.participation_id().into_uuid().as_bytes(),
                head_revision,
                head_request_id.as_bytes(),
            ],
        )?;
        Ok(())
    }

    fn seed_qualification_rule(
        catalog: &OperationsCatalog,
        domain: &str,
        generation: i64,
        active: bool,
        seed_tag: u128,
    ) -> TestResult {
        let rule_id = Uuid::from_u128(0x8500_0000 + seed_tag);
        for current_gen in 1..=generation {
            let expected_gen = (current_gen > 1).then_some(current_gen - 1);
            let state = i64::from(current_gen == generation && !active);
            let req_id =
                Uuid::from_u128(0x8600_0000 + seed_tag * 16 + u128::try_from(current_gen)?);
            catalog.connection.execute(
                "INSERT INTO ops_reviewer_qualification_rule_generations(workspace_id,rule_id,generation,expected_generation,request_id,requester_session_id,actor_direct_token_id,state,domain,permitted_scope,policy_json,change_json,created_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,'workspace','{}','{}',100)",
                params![
                    WORKSPACE_UUID.as_bytes(),
                    rule_id.as_bytes(),
                    current_gen,
                    expected_gen,
                    req_id.as_bytes(),
                    Uuid::from_u128(0x8701).as_bytes(),
                    Uuid::from_u128(0x8702).as_bytes(),
                    state,
                    domain,
                ],
            )?;
        }
        catalog.connection.execute(
            "INSERT INTO ops_reviewer_qualification_rule_heads(workspace_id,rule_id,current_generation) VALUES(?1,?2,?3)",
            params![WORKSPACE_UUID.as_bytes(), rule_id.as_bytes(), generation],
        )?;
        Ok(())
    }

    #[test]
    fn self_review_dictionary_rejects_all_executor_and_leader_identity_overlaps() -> TestResult {
        let (_guard, mut catalog) = TempCatalog::open()?;
        let fixture = AssignmentFixture::standard()?;
        let fresh_profile = AgentProfileId::from_uuid(Uuid::from_u128(0x7500))?;
        let fresh_participant = Uuid::from_u128(0x7600);
        let fresh_principal =
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(0x7700)));

        let cases = [
            (
                "executor_profile",
                fixture.executor.profile_id(),
                fresh_participant,
                fresh_principal,
            ),
            (
                "executor_participant",
                fresh_profile,
                fixture.executor.participant_id(),
                fresh_principal,
            ),
            (
                "executor_principal",
                fresh_profile,
                fresh_participant,
                fixture.executor.principal(),
            ),
            (
                "leader_profile",
                fixture.leader.profile_id(),
                fresh_participant,
                fresh_principal,
            ),
            (
                "leader_participant",
                fresh_profile,
                fixture.leader.participant_id(),
                fresh_principal,
            ),
            (
                "leader_principal",
                fresh_profile,
                fresh_participant,
                fixture.leader.principal(),
            ),
        ];

        for (index, (label, profile_id, participant_id, principal)) in cases.into_iter().enumerate()
        {
            let tag = u128::try_from(index + 1)?;
            let candidate = ReviewerCandidateIdentity::new(
                WorkspaceParticipationId::from_uuid(Uuid::from_u128(0x7800 + tag))?,
                profile_id,
                participant_id,
                principal,
            )?;
            let command = ReviewerAssignmentCommand {
                request_id: Uuid::from_u128(0x7900 + tag),
                correlation: fixture.correlation,
                assigning_leader: fixture.leader,
                executor: fixture.executor,
                candidates: vec![candidate],
                qualification_domain: None,
                assigned_at_ms: 500,
            };
            let result = catalog.assign_independent_reviewer(&command);
            assert!(
                matches!(result, Err(ReviewerAssignmentError::SelfReview { .. })),
                "case {label} did not fail with SelfReview: {result:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn pool_filters_self_and_withdrawn_candidates_and_fails_closed_when_unqualified() -> TestResult
    {
        let (_guard, mut catalog) = TempCatalog::open()?;
        let fixture = AssignmentFixture::standard()?;
        let self_candidate = ReviewerCandidateIdentity::new(
            WorkspaceParticipationId::from_uuid(Uuid::from_u128(0x7A01))?,
            fixture.executor.profile_id(),
            Uuid::from_u128(0x7B01),
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(0x7C01))),
        )?;
        let withdrawn_candidate = ReviewerCandidateIdentity::new(
            WorkspaceParticipationId::from_uuid(Uuid::from_u128(0x7A02))?,
            AgentProfileId::from_uuid(Uuid::from_u128(0x7D02))?,
            Uuid::from_u128(0x7B02),
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(0x7C02))),
        )?;
        let eligible_second = ReviewerCandidateIdentity::new(
            WorkspaceParticipationId::from_uuid(Uuid::from_u128(0x7A04))?,
            AgentProfileId::from_uuid(Uuid::from_u128(0x7D04))?,
            Uuid::from_u128(0x7B04),
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(0x7C04))),
        )?;
        let eligible_first = ReviewerCandidateIdentity::new(
            WorkspaceParticipationId::from_uuid(Uuid::from_u128(0x7A03))?,
            AgentProfileId::from_uuid(Uuid::from_u128(0x7D03))?,
            Uuid::from_u128(0x7B03),
            AuthenticatedPrincipal::OAuthGrant(OAuthGrantId::from_uuid(Uuid::from_u128(0x7C03))),
        )?;

        seed_participation_statement(&catalog, &self_candidate, true, 1)?;
        seed_participation_statement(&catalog, &withdrawn_candidate, false, 2)?;

        let no_qualified_cmd = ReviewerAssignmentCommand {
            request_id: Uuid::from_u128(0x7E01),
            correlation: fixture.correlation,
            assigning_leader: fixture.leader,
            executor: fixture.executor,
            candidates: vec![self_candidate, withdrawn_candidate],
            qualification_domain: None,
            assigned_at_ms: 600,
        };
        assert!(matches!(
            catalog.assign_independent_reviewer(&no_qualified_cmd),
            Err(ReviewerAssignmentError::NoQualifiedReviewer)
        ));

        seed_participation_statement(&catalog, &eligible_second, true, 3)?;
        seed_participation_statement(&catalog, &eligible_first, true, 4)?;

        let domain = QualificationDomain::new("rust-operations".to_owned())?;
        let missing_rule_cmd = ReviewerAssignmentCommand {
            request_id: Uuid::from_u128(0x7E02),
            correlation: fixture.correlation,
            assigning_leader: fixture.leader,
            executor: fixture.executor,
            candidates: vec![eligible_second, eligible_first],
            qualification_domain: Some(domain.clone()),
            assigned_at_ms: 601,
        };
        assert!(matches!(
            catalog.assign_independent_reviewer(&missing_rule_cmd),
            Err(ReviewerAssignmentError::QualificationMissing)
        ));

        seed_qualification_rule(&catalog, "rust-operations", 2, true, 1)?;
        let assigned = catalog.assign_independent_reviewer(&ReviewerAssignmentCommand {
            request_id: Uuid::from_u128(0x7E03),
            candidates: vec![
                self_candidate,
                withdrawn_candidate,
                eligible_second,
                eligible_first,
            ],
            ..missing_rule_cmd
        })?;
        assert_eq!(assigned.reviewer(), eligible_first);
        assert_eq!(assigned.rule_generation(), Some(2));
        assert_eq!((assigned.revision(), assigned.replayed()), (1, false));
        Ok(())
    }

    #[test]
    fn completion_gate_enforces_accepted_verdict_and_exact_diff_digest() -> TestResult {
        let (_guard, mut catalog) = TempCatalog::open()?;
        let fixture = AssignmentFixture::standard()?;
        let reviewer = ReviewerCandidateIdentity::new(
            WorkspaceParticipationId::from_uuid(Uuid::from_u128(0x7F01))?,
            AgentProfileId::from_uuid(Uuid::from_u128(0x7F02))?,
            Uuid::from_u128(0x7F03),
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(0x7F04))),
        )?;
        seed_participation_statement(&catalog, &reviewer, true, 10)?;

        let diff_one = [0xAA_u8; 32];
        let diff_two = [0xBB_u8; 32];
        assert!(matches!(
            catalog.verify_node_completion_review_gate(&fixture.correlation, diff_one),
            Err(ReviewerAssignmentError::ReviewNotAccepted)
        ));

        let assign_cmd = ReviewerAssignmentCommand {
            request_id: Uuid::from_u128(0x7F10),
            correlation: fixture.correlation,
            assigning_leader: fixture.leader,
            executor: fixture.executor,
            candidates: vec![reviewer],
            qualification_domain: None,
            assigned_at_ms: 1_000,
        };
        catalog.assign_independent_reviewer(&assign_cmd)?;
        let replayed_assign = catalog.assign_independent_reviewer(&assign_cmd)?;
        assert!(replayed_assign.replayed());
        assert!(matches!(
            catalog.verify_node_completion_review_gate(&fixture.correlation, diff_one),
            Err(ReviewerAssignmentError::ReviewNotAccepted)
        ));

        let needs_changes = catalog.record_review_verdict(&ReviewVerdictCommand {
            request_id: Uuid::from_u128(0x7F11),
            correlation: fixture.correlation,
            expected_revision: 1,
            reviewer_principal: reviewer.principal(),
            reviewer_profile_id: reviewer.profile_id(),
            reviewer_participant_id: reviewer.participant_id(),
            outcome: ReviewVerdictOutcome::NeedsChanges,
            handoff_diff_digest: diff_one,
            rationale: "add missing edge-case test".to_owned(),
            recorded_at_ms: 1_100,
        })?;
        assert_eq!(needs_changes.revision(), 2);
        assert!(matches!(
            catalog.verify_node_completion_review_gate(&fixture.correlation, diff_one),
            Err(ReviewerAssignmentError::ReviewNotAccepted)
        ));

        let accept_cmd = ReviewVerdictCommand {
            request_id: Uuid::from_u128(0x7F12),
            correlation: fixture.correlation,
            expected_revision: 2,
            reviewer_principal: reviewer.principal(),
            reviewer_profile_id: reviewer.profile_id(),
            reviewer_participant_id: reviewer.participant_id(),
            outcome: ReviewVerdictOutcome::Accepted,
            handoff_diff_digest: diff_two,
            rationale: "verified independent review against diff_two".to_owned(),
            recorded_at_ms: 1_200,
        };
        let accepted = catalog.record_review_verdict(&accept_cmd)?;
        assert_eq!(accepted.revision(), 3);
        assert!(catalog.record_review_verdict(&accept_cmd)?.replayed());

        let post_accept_modified_diff = [0xCC_u8; 32];
        assert!(matches!(
            catalog.verify_node_completion_review_gate(
                &fixture.correlation,
                post_accept_modified_diff
            ),
            Err(ReviewerAssignmentError::VerdictDiffDigestMismatch)
        ));
        let verified =
            catalog.verify_node_completion_review_gate(&fixture.correlation, diff_two)?;
        assert_eq!(verified.revision(), 3);
        Ok(())
    }

    #[test]
    fn verify_reviewer_session_binding_enforces_active_interested_head_agent_and_session()
    -> TestResult {
        let (_guard, catalog) = TempCatalog::open()?;
        let workspace = WorkspaceId::from_uuid(WORKSPACE_UUID);
        let interested = ReviewerCandidateIdentity::new(
            WorkspaceParticipationId::from_uuid(Uuid::from_u128(0x9501))?,
            AgentProfileId::from_uuid(Uuid::from_u128(0x9601))?,
            Uuid::from_u128(0x9701),
            AuthenticatedPrincipal::TrustedStdio,
        )?;
        let withdrawn = ReviewerCandidateIdentity::new(
            WorkspaceParticipationId::from_uuid(Uuid::from_u128(0x9502))?,
            AgentProfileId::from_uuid(Uuid::from_u128(0x9602))?,
            Uuid::from_u128(0x9702),
            AuthenticatedPrincipal::TrustedStdio,
        )?;
        seed_participation_statement(&catalog, &interested, true, 51)?;
        seed_participation_statement(&catalog, &withdrawn, false, 52)?;

        let expected_agent = Uuid::from_u128(0x8200_0000 + 51);
        let expected_session = Uuid::from_u128(0x8300_0000 + 51);
        catalog.verify_reviewer_session_binding(
            workspace,
            interested.participation_id(),
            expected_agent,
            expected_session,
        )?;

        for bad_call in [
            catalog.verify_reviewer_session_binding(
                workspace,
                interested.participation_id(),
                Uuid::from_u128(0xDEAD_BEEF),
                expected_session,
            ),
            catalog.verify_reviewer_session_binding(
                workspace,
                interested.participation_id(),
                expected_agent,
                Uuid::from_u128(0xDEAD_CAFE),
            ),
            catalog.verify_reviewer_session_binding(
                workspace,
                withdrawn.participation_id(),
                Uuid::from_u128(0x8200_0000 + 52),
                Uuid::from_u128(0x8300_0000 + 52),
            ),
            catalog.verify_reviewer_session_binding(
                workspace,
                WorkspaceParticipationId::from_uuid(Uuid::from_u128(0x9599))?,
                expected_agent,
                expected_session,
            ),
        ] {
            assert!(matches!(
                bad_call,
                Err(ReviewerAssignmentError::Operations(
                    OperationsError::Conflict(
                        "review verdict caller does not match assigned reviewer"
                    )
                ))
            ));
        }
        Ok(())
    }
}
