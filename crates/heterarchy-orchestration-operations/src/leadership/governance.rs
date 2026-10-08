//! Durable leadership-review governance layered over the existing election.
//!
//! This child owns only review/statement records and their phase gates. The
//! leadership row, election, frozen electorate and ballot authority remain in
//! the parent leadership/election-subject modules.

#![allow(clippy::must_use_candidate)]

use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, WorkspaceId};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use uuid::Uuid;

use crate::{
    LeadershipScope, OperationalActor, OperationsCatalog, OperationsError,
    WorkspaceParticipationId,
    operational_actor::{ResolvedOperationalActor, resolve_actor},
};

const MAX_REVIEW_REASON_BYTES: usize = 4 * 1024;
const MAX_STATEMENT_BODY_BYTES: usize = 16 * 1024;
const MAX_STATEMENTS_PER_ELECTION: i64 = 256;
const MAX_REVIEW_PAGE: u32 = 64;
const DEFENSE_WINDOW_MS: i64 = 60_000;
const DISCUSSION_WINDOW_MS: i64 = 30_000;

pub(crate) mod statement_readback;
pub use statement_readback::LeadershipStatementReadback;

const REVIEWS_SQL: &str = "CREATE TABLE ops_leadership_reviews (request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), election_id BLOB NOT NULL UNIQUE REFERENCES ops_leadership_elections(election_id) CHECK(length(election_id)=16), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), scope_kind INTEGER NOT NULL CHECK(scope_kind IN(0,1,2)), activity_id BLOB CHECK(activity_id IS NULL OR length(activity_id)=16), team_id BLOB CHECK(team_id IS NULL OR length(team_id)=16), expected_leadership_revision INTEGER NOT NULL CHECK(expected_leadership_revision>=1), proposer_kind INTEGER NOT NULL CHECK(proposer_kind IN(0,1)), proposer_activity_id BLOB CHECK(proposer_activity_id IS NULL OR length(proposer_activity_id)=16), proposer_agent_id BLOB CHECK(proposer_agent_id IS NULL OR length(proposer_agent_id)=16), proposer_session_id BLOB CHECK(proposer_session_id IS NULL OR length(proposer_session_id)=16), proposer_participation_id BLOB CHECK(proposer_participation_id IS NULL OR length(proposer_participation_id)=16), proposer_profile_id BLOB CHECK(proposer_profile_id IS NULL OR length(proposer_profile_id)=16), proposer_participant_id BLOB CHECK(proposer_participant_id IS NULL OR length(proposer_participant_id)=16), proposer_display_name TEXT NOT NULL CHECK(length(CAST(proposer_display_name AS BLOB)) BETWEEN 1 AND 128), incumbent_participant_id BLOB CHECK(incumbent_participant_id IS NULL OR length(incumbent_participant_id)=16), incumbent_participation_id BLOB CHECK(incumbent_participation_id IS NULL OR length(incumbent_participation_id)=16), incumbent_profile_id BLOB CHECK(incumbent_profile_id IS NULL OR length(incumbent_profile_id)=16), incumbent_session_id BLOB CHECK(incumbent_session_id IS NULL OR length(incumbent_session_id)=16), incumbent_display_name TEXT CHECK(incumbent_display_name IS NULL OR length(CAST(incumbent_display_name AS BLOB)) BETWEEN 1 AND 128), reason TEXT NOT NULL CHECK(length(CAST(reason AS BLOB)) BETWEEN 1 AND 4096), defense_deadline_ms INTEGER NOT NULL CHECK(defense_deadline_ms>=0), discussion_deadline_ms INTEGER NOT NULL CHECK(discussion_deadline_ms>=defense_deadline_ms), ballot_deadline_ms INTEGER NOT NULL CHECK(ballot_deadline_ms>=discussion_deadline_ms), defense_closed_at_ms INTEGER CHECK(defense_closed_at_ms IS NULL OR defense_closed_at_ms>=0), incumbent_statement_id BLOB CHECK(incumbent_statement_id IS NULL OR length(incumbent_statement_id)=16), created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0), CHECK((scope_kind=0 AND activity_id IS NULL AND team_id IS NULL) OR (scope_kind=1 AND activity_id IS NOT NULL AND team_id IS NULL) OR (scope_kind=2 AND activity_id IS NULL AND team_id IS NOT NULL)), CHECK((proposer_kind=0 AND proposer_activity_id IS NULL AND proposer_agent_id IS NULL AND proposer_session_id IS NULL AND proposer_participation_id IS NULL AND proposer_profile_id IS NULL AND proposer_participant_id IS NULL) OR (proposer_kind=1 AND proposer_activity_id IS NOT NULL AND proposer_agent_id IS NOT NULL AND proposer_session_id IS NOT NULL AND proposer_participation_id IS NOT NULL AND proposer_profile_id IS NOT NULL AND proposer_participant_id IS NOT NULL)))";
const STATEMENTS_SQL: &str = "CREATE TABLE ops_leadership_review_statements (statement_id BLOB PRIMARY KEY NOT NULL CHECK(length(statement_id)=16 AND statement_id != zeroblob(16)), request_id BLOB NOT NULL REFERENCES ops_leadership_reviews(request_id) CHECK(length(request_id)=16), election_id BLOB NOT NULL REFERENCES ops_leadership_elections(election_id) CHECK(length(election_id)=16), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), statement_kind TEXT NOT NULL CHECK(statement_kind IN('DEFENSE','DISCUSSION')), author_kind INTEGER NOT NULL CHECK(author_kind=1), author_activity_id BLOB NOT NULL CHECK(length(author_activity_id)=16), author_agent_id BLOB NOT NULL CHECK(length(author_agent_id)=16), author_session_id BLOB NOT NULL CHECK(length(author_session_id)=16), author_participation_id BLOB NOT NULL CHECK(length(author_participation_id)=16), author_profile_id BLOB NOT NULL CHECK(length(author_profile_id)=16), author_participant_id BLOB NOT NULL CHECK(length(author_participant_id)=16), author_display_name TEXT NOT NULL CHECK(length(CAST(author_display_name AS BLOB)) BETWEEN 1 AND 128), body TEXT NOT NULL CHECK(length(CAST(body AS BLOB)) BETWEEN 1 AND 16384), sequence INTEGER NOT NULL CHECK(sequence>=1 AND sequence<=256), submitted_at_ms INTEGER NOT NULL CHECK(submitted_at_ms>=0), incumbent_final INTEGER NOT NULL CHECK(incumbent_final IN(0,1)), UNIQUE(election_id,sequence), CHECK((incumbent_final=0) OR statement_kind='DEFENSE'))";

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("table", "ops_leadership_reviews", REVIEWS_SQL),
        ("table", "ops_leadership_review_statements", STATEMENTS_SQL),
    ]
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IdentitySnapshot {
    kind: i64,
    activity_id: Option<ActivityId>,
    agent_id: Option<forge_domain::AgentInstanceId>,
    session_id: Option<AgentSessionId>,
    participation_id: Option<WorkspaceParticipationId>,
    profile_id: Option<Uuid>,
    participant_id: Option<Uuid>,
    display_name: String,
}

impl IdentitySnapshot {
    fn from_actor(actor: OperationalActor, resolved: &ResolvedOperationalActor) -> Self {
        let (kind, activity_id, agent_id) = match actor {
            OperationalActor::LocalOperator => (0, None, None),
            OperationalActor::Participant {
                activity_id,
                agent_id,
                ..
            } => (1, Some(activity_id), Some(agent_id)),
        };
        Self {
            kind,
            activity_id,
            agent_id,
            session_id: resolved.session_id,
            participation_id: resolved.participation_id,
            profile_id: resolved.profile_id,
            participant_id: resolved.participant_id,
            display_name: resolved.display_name.clone(),
        }
    }

    fn into_public(self) -> LeadershipIdentitySnapshot {
        LeadershipIdentitySnapshot {
            participant_id: self.participant_id,
            participation_id: self.participation_id,
            profile_id: self.profile_id,
            session_id: self.session_id,
            display_name: self.display_name,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReviewRow {
    request_id: Uuid,
    election_id: Uuid,
    scope: super::LeadershipScope,
    expected_revision: i64,
    proposer: IdentitySnapshot,
    incumbent: Option<IdentitySnapshot>,
    reason: String,
    defense_deadline_ms: i64,
    discussion_deadline_ms: i64,
    ballot_deadline_ms: i64,
    defense_closed_at_ms: Option<i64>,
    incumbent_statement_id: Option<Uuid>,
    created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StatementRow {
    statement_id: Uuid,
    request_id: Uuid,
    election_id: Uuid,
    workspace_id: WorkspaceId,
    kind: LeadershipStatementKind,
    author: IdentitySnapshot,
    body: String,
    sequence: u64,
    submitted_at_ms: i64,
    incumbent_final: bool,
}

impl StatementRow {
    fn into_public(self) -> LeadershipStatementRecord {
        LeadershipStatementRecord {
            statement_id: self.statement_id,
            election_id: self.election_id,
            kind: self.kind,
            author: self.author.into_public(),
            body: self.body,
            sequence: self.sequence,
            submitted_at_ms: self.submitted_at_ms,
            incumbent_final: self.incumbent_final,
        }
    }
}

pub(crate) fn create_schema(connection: &rusqlite::Connection) -> Result<(), OperationsError> {
    connection.execute_batch(REVIEWS_SQL)?;
    connection.execute_batch(STATEMENTS_SQL)?;
    Ok(())
}

#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeadershipReviewPhase {
    Defense,
    Discussion,
    Ballot,
    AwaitingDecision,
    Resolved,
}

#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeadershipStatementKind {
    Defense,
    Discussion,
}

impl LeadershipStatementKind {
    const fn encode(self) -> &'static str {
        match self {
            Self::Defense => "DEFENSE",
            Self::Discussion => "DISCUSSION",
        }
    }

    fn decode(value: &str) -> Result<Self, OperationsError> {
        match value {
            "DEFENSE" => Ok(Self::Defense),
            "DISCUSSION" => Ok(Self::Discussion),
            _ => Err(OperationsError::Corrupt("leadership statement kind")),
        }
    }
}

#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipReviewCommand {
    pub request_id: Uuid,
    pub scope: super::LeadershipScope,
    pub expected_revision: i64,
    pub actor: OperationalActor,
    pub candidate_participant_id: Uuid,
    pub reason: String,
    pub now_ms: i64,
}

#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipReviewReceipt {
    request_id: Uuid,
    election_id: Uuid,
    scope: super::LeadershipScope,
    /// Leadership revision observed when this receipt was produced.
    leadership_revision: i64,
    /// Review phase observed at receipt time; a replay can report later progress.
    phase: LeadershipReviewPhase,
    /// Persisted review deadlines. Failover-required reviews may close Defense immediately.
    defense_deadline_ms: i64,
    discussion_deadline_ms: i64,
    ballot_deadline_ms: i64,
    replay: bool,
    electorate_size: i64,
}

impl LeadershipReviewReceipt {
    #[must_use]
    pub const fn electorate_size(&self) -> i64 {
        self.electorate_size
    }

    /// Return the leadership revision observed when this receipt was produced.
    /// A replay may therefore report a later revision than the initial request.
    #[must_use]
    pub const fn leadership_revision(&self) -> i64 {
        self.leadership_revision
    }

    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }
    pub const fn election_id(&self) -> Uuid {
        self.election_id
    }
    pub const fn scope(&self) -> super::LeadershipScope {
        self.scope
    }
    /// Return the phase observed at receipt time; an exact replay may return a later phase.
    pub const fn phase(&self) -> LeadershipReviewPhase {
        self.phase
    }
    /// Return the persisted Defense deadline, which may be immediate for failover reviews.
    pub const fn defense_deadline_ms(&self) -> i64 {
        self.defense_deadline_ms
    }
    pub const fn discussion_deadline_ms(&self) -> i64 {
        self.discussion_deadline_ms
    }
    pub const fn ballot_deadline_ms(&self) -> i64 {
        self.ballot_deadline_ms
    }
    pub const fn replay(&self) -> bool {
        self.replay
    }
}

#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipStatementCommand {
    pub statement_id: Uuid,
    pub request_id: Uuid,
    pub election_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub actor: OperationalActor,
    pub kind: LeadershipStatementKind,
    pub body: String,
    pub now_ms: i64,
}

#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipStatementReceipt {
    statement_id: Uuid,
    election_id: Uuid,
    sequence: u64,
    phase: LeadershipReviewPhase,
    replay: bool,
}

impl LeadershipStatementReceipt {
    pub const fn statement_id(&self) -> Uuid {
        self.statement_id
    }
    pub const fn election_id(&self) -> Uuid {
        self.election_id
    }
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }
    pub const fn phase(&self) -> LeadershipReviewPhase {
        self.phase
    }
    pub const fn replay(&self) -> bool {
        self.replay
    }
}

#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeadershipReviewQuery {
    pub request_id: Option<Uuid>,
    pub election_id: Option<Uuid>,
}

impl LeadershipReviewQuery {
    pub const fn request(request_id: Uuid) -> Self {
        Self {
            request_id: Some(request_id),
            election_id: None,
        }
    }
    pub const fn election(election_id: Uuid) -> Self {
        Self {
            request_id: None,
            election_id: Some(election_id),
        }
    }
}

#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipIdentitySnapshot {
    participant_id: Option<Uuid>,
    participation_id: Option<WorkspaceParticipationId>,
    profile_id: Option<Uuid>,
    session_id: Option<AgentSessionId>,
    display_name: String,
}

impl LeadershipIdentitySnapshot {
    pub const fn participant_id(&self) -> Option<Uuid> {
        self.participant_id
    }
    pub const fn profile_id(&self) -> Option<Uuid> {
        self.profile_id
    }
    pub const fn participation_id(&self) -> Option<WorkspaceParticipationId> {
        self.participation_id
    }
    pub const fn session_id(&self) -> Option<AgentSessionId> {
        self.session_id
    }
    pub fn display_name(&self) -> &str {
        &self.display_name
    }
}

#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipStatementRecord {
    statement_id: Uuid,
    election_id: Uuid,
    kind: LeadershipStatementKind,
    author: LeadershipIdentitySnapshot,
    body: String,
    sequence: u64,
    submitted_at_ms: i64,
    incumbent_final: bool,
}

impl LeadershipStatementRecord {
    pub const fn statement_id(&self) -> Uuid {
        self.statement_id
    }
    pub const fn election_id(&self) -> Uuid {
        self.election_id
    }
    pub const fn kind(&self) -> LeadershipStatementKind {
        self.kind
    }
    pub fn author(&self) -> &LeadershipIdentitySnapshot {
        &self.author
    }
    pub fn body(&self) -> &str {
        &self.body
    }
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }
    pub const fn submitted_at_ms(&self) -> i64 {
        self.submitted_at_ms
    }
    pub const fn incumbent_final(&self) -> bool {
        self.incumbent_final
    }
}

#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipStatementPage {
    statements: Vec<LeadershipStatementRecord>,
    next_cursor: Option<u64>,
    has_more: bool,
}

impl LeadershipStatementPage {
    pub fn statements(&self) -> &[LeadershipStatementRecord] {
        &self.statements
    }
    pub const fn next_cursor(&self) -> Option<u64> {
        self.next_cursor
    }
    pub const fn has_more(&self) -> bool {
        self.has_more
    }
}

#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipVoterSnapshot {
    participant_id: Uuid,
    subject_id: Uuid,
    participation_id: Option<WorkspaceParticipationId>,
    profile_id: Option<Uuid>,
    session_id: Option<AgentSessionId>,
    display_name: Option<String>,
    choice: Option<super::BallotChoice>,
    cast_at_ms: Option<i64>,
}

impl LeadershipVoterSnapshot {
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }
    pub const fn subject_id(&self) -> Uuid {
        self.subject_id
    }
    pub const fn participation_id(&self) -> Option<WorkspaceParticipationId> {
        self.participation_id
    }
    pub const fn profile_id(&self) -> Option<Uuid> {
        self.profile_id
    }
    pub const fn session_id(&self) -> Option<AgentSessionId> {
        self.session_id
    }
    pub fn display_name(&self) -> Option<&str> {
        self.display_name.as_deref()
    }
    pub const fn choice(&self) -> Option<super::BallotChoice> {
        self.choice
    }
    pub const fn cast_at_ms(&self) -> Option<i64> {
        self.cast_at_ms
    }
}

#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipReviewSnapshot {
    request_id: Uuid,
    election_id: Uuid,
    scope: super::LeadershipScope,
    expected_leadership_revision: i64,
    proposer_activity_id: Option<ActivityId>,
    proposer_agent_id: Option<AgentInstanceId>,
    candidate_participant_id: Uuid,
    candidate_subject_id: Uuid,
    election_state: super::ElectionState,
    elected_participant_id: Option<Uuid>,
    authority_confirmed: bool,
    proposer: LeadershipIdentitySnapshot,
    incumbent: Option<LeadershipIdentitySnapshot>,
    reason: String,
    phase: LeadershipReviewPhase,
    defense_deadline_ms: i64,
    discussion_deadline_ms: i64,
    ballot_deadline_ms: i64,
    created_at_ms: i64,
    defense_closed_at_ms: Option<i64>,
    incumbent_response: Option<LeadershipStatementRecord>,
    electorate: Vec<LeadershipVoterSnapshot>,
    electorate_truncated: bool,
    statements: LeadershipStatementPage,
}

impl LeadershipReviewSnapshot {
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }
    pub const fn election_id(&self) -> Uuid {
        self.election_id
    }
    pub const fn scope(&self) -> super::LeadershipScope {
        self.scope
    }
    /// Return the original leadership revision supplied with this request.
    pub const fn expected_leadership_revision(&self) -> i64 {
        self.expected_leadership_revision
    }
    /// Return the proposer's Activity stored with the original request.
    pub const fn proposer_activity_id(&self) -> Option<ActivityId> {
        self.proposer_activity_id
    }
    /// Return the proposer's member Agent stored with the original request.
    pub const fn proposer_agent_id(&self) -> Option<AgentInstanceId> {
        self.proposer_agent_id
    }
    pub const fn candidate_participant_id(&self) -> Uuid {
        self.candidate_participant_id
    }
    pub const fn candidate_subject_id(&self) -> Uuid {
        self.candidate_subject_id
    }
    pub const fn election_state(&self) -> super::ElectionState {
        self.election_state
    }
    pub const fn elected_participant_id(&self) -> Option<Uuid> {
        self.elected_participant_id
    }
    pub const fn authority_confirmed(&self) -> bool {
        self.authority_confirmed
    }
    pub fn proposer(&self) -> &LeadershipIdentitySnapshot {
        &self.proposer
    }
    pub fn incumbent(&self) -> Option<&LeadershipIdentitySnapshot> {
        self.incumbent.as_ref()
    }
    pub fn reason(&self) -> &str {
        &self.reason
    }
    pub const fn phase(&self) -> LeadershipReviewPhase {
        self.phase
    }
    pub const fn defense_deadline_ms(&self) -> i64 {
        self.defense_deadline_ms
    }
    pub const fn discussion_deadline_ms(&self) -> i64 {
        self.discussion_deadline_ms
    }
    pub const fn ballot_deadline_ms(&self) -> i64 {
        self.ballot_deadline_ms
    }
    pub const fn created_at_ms(&self) -> i64 {
        self.created_at_ms
    }
    pub const fn defense_closed_at_ms(&self) -> Option<i64> {
        self.defense_closed_at_ms
    }
    pub fn incumbent_response(&self) -> Option<&LeadershipStatementRecord> {
        self.incumbent_response.as_ref()
    }
    pub fn electorate(&self) -> &[LeadershipVoterSnapshot] {
        &self.electorate
    }
    pub const fn electorate_truncated(&self) -> bool {
        self.electorate_truncated
    }
    pub fn statements(&self) -> &LeadershipStatementPage {
        &self.statements
    }
}

fn read_review_snapshot_in_tx(
    tx: &Transaction<'_>,
    review: &ReviewRow,
    now_ms: i64,
    statement_after: Option<u64>,
    statement_limit: u32,
) -> Result<LeadershipReviewSnapshot, OperationsError> {
    let election = load_review_election(tx, review)?;
    let leadership = super::load_leadership(tx, review.scope)?
        .ok_or(OperationsError::Corrupt("leadership review scope"))?;
    let candidate_subject_id = frozen_source_subjects(tx, review.election_id)?
        .into_iter()
        .find(|source| source.participant_id == election.candidate_participant_id)
        .map(|source| source.subject_id)
        .ok_or(OperationsError::Corrupt("leadership candidate subject"))?;
    let authority_confirmed = leadership_receipt_confirmed(tx, review.scope, review.election_id)?;
    let electorate = load_voters(tx, review.election_id)?;
    let statements = load_statement_page(tx, review.election_id, statement_after, statement_limit)?;
    let incumbent_response = match review.incumbent_statement_id {
        Some(statement_id) => {
            let statement = load_statement(tx, statement_id)?
                .ok_or(OperationsError::Corrupt("leadership incumbent statement"))?;
            if statement.request_id != review.request_id
                || statement.election_id != review.election_id
                || statement.workspace_id != review.scope.workspace_id()
                || statement.kind != LeadershipStatementKind::Defense
                || !statement.incumbent_final
            {
                return Err(OperationsError::Corrupt(
                    "leadership incumbent statement context",
                ));
            }
            Some(statement)
        }
        None => None,
    };
    Ok(LeadershipReviewSnapshot {
        request_id: review.request_id,
        election_id: review.election_id,
        scope: review.scope,
        expected_leadership_revision: review.expected_revision,
        proposer_activity_id: review.proposer.activity_id,
        proposer_agent_id: review.proposer.agent_id,
        candidate_participant_id: election.candidate_participant_id,
        candidate_subject_id,
        election_state: election.state,
        elected_participant_id: election.elected_participant_id,
        authority_confirmed,
        proposer: review.proposer.clone().into_public(),
        incumbent: review.incumbent.clone().map(IdentitySnapshot::into_public),
        reason: review.reason.clone(),
        phase: review_phase_with_leadership(review, &election, &leadership, now_ms),
        defense_deadline_ms: review.defense_deadline_ms,
        discussion_deadline_ms: review.discussion_deadline_ms,
        ballot_deadline_ms: review.ballot_deadline_ms,
        created_at_ms: review.created_at_ms,
        defense_closed_at_ms: review.defense_closed_at_ms,
        incumbent_response: incumbent_response.map(StatementRow::into_public),
        electorate: electorate.0,
        electorate_truncated: electorate.1,
        statements,
    })
}

impl OperationsCatalog {
    /// Request one durable leadership review and start its existing election
    /// in the same immediate transaction.
    ///
    /// # Errors
    /// Returns authority, replay, validation, storage, or commit errors.
    #[allow(clippy::needless_pass_by_value)]
    pub fn request_leadership_review(
        &mut self,
        command: LeadershipReviewCommand,
    ) -> Result<LeadershipReviewReceipt, OperationsError> {
        validate_review_command(&command)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        ensure_verified_workspace(&tx, command.scope.workspace_id(), self.destination)?;
        if let Some(existing) = load_review_by_request(&tx, command.request_id)? {
            let election = load_review_election(&tx, &existing)?;
            ensure_review_replay(&tx, &existing, &command, &election)?;
            let receipt = review_receipt(&tx, &existing, &election, command.now_ms, true)?;
            tx.commit()?;
            return Ok(receipt);
        }
        // Spec 2026-09-22 #9-1: once leadership changes, re-election requests
        // stay locked for 30 minutes - identical for Team and Orchestration
        // scopes. Replays of an in-flight request remain exempt.
        if let Some(last) = last_leader_change_ms(&tx, &command.scope)?
            && command.now_ms.saturating_sub(last) < super::LEADER_CHANGE_COOLDOWN_MS
        {
            return Err(OperationsError::Conflict(
                "leadership re-election cooldown active",
            ));
        }
        let current = super::load_leadership(&tx, command.scope)?
            .ok_or(OperationsError::Conflict("leadership scope missing"))?;
        let required = match current.state {
            super::LeadershipState::Active => super::LeadershipState::Active,
            super::LeadershipState::FailoverRequired => super::LeadershipState::FailoverRequired,
            _ => return Err(OperationsError::Conflict("leadership review not electable")),
        };
        let incumbent = match current.leader_participant_id {
            Some(participant) => Some(
                load_identity(&tx, command.scope.workspace_id(), participant)?
                    .ok_or(OperationsError::Corrupt("leadership incumbent identity"))?,
            ),
            None => None,
        };
        let resolved = resolve_actor(&tx, command.scope.workspace_id(), command.actor)?;
        let participant = resolved.participant_id.ok_or(OperationsError::Conflict(
            "leadership proposer requires participant",
        ))?;
        let election = super::start_election_transaction(
            &tx,
            super::RecallElectionCommand {
                scope: command.scope,
                expected_revision: command.expected_revision,
                candidate_participant_id: command.candidate_participant_id,
                now_ms: command.now_ms,
            },
            required,
        )?;
        let sources = frozen_source_subjects(&tx, election.election_id())?;
        if !actor_in_frozen_electorate(&resolved, participant, &sources) {
            return Err(OperationsError::Conflict(
                "leadership proposer is not frozen electorate",
            ));
        }
        let defense_deadline = if required == super::LeadershipState::FailoverRequired {
            command.now_ms
        } else {
            command
                .now_ms
                .checked_add(DEFENSE_WINDOW_MS)
                .ok_or(OperationsError::Bounds("leadership defense deadline"))?
        };
        let defense_closed_at =
            (required == super::LeadershipState::FailoverRequired).then_some(command.now_ms);
        let discussion_deadline = defense_deadline
            .checked_add(DISCUSSION_WINDOW_MS)
            .ok_or(OperationsError::Bounds("leadership discussion deadline"))?;
        let ballot_deadline = discussion_deadline
            .checked_add(super::BALLOT_WINDOW_MS)
            .ok_or(OperationsError::Bounds("leadership ballot deadline"))?;
        tx.execute(
            "UPDATE ops_leadership_elections SET ballot_deadline_ms=?2 WHERE election_id=?1 AND state='ELECTING'",
            params![election.election_id().as_bytes(), ballot_deadline],
        )?;
        let proposer = IdentitySnapshot::from_actor(command.actor, &resolved);
        insert_review(
            &tx,
            &command,
            election.election_id(),
            &proposer,
            incumbent.as_ref(),
            defense_deadline,
            discussion_deadline,
            ballot_deadline,
            defense_closed_at,
        )?;
        let stored = load_review_by_request(&tx, command.request_id)?
            .ok_or(OperationsError::Corrupt("leadership review insert"))?;
        let election_row = super::load_election(&tx, election.election_id())?.ok_or(
            OperationsError::Corrupt("leadership review election readback"),
        )?;
        let receipt = review_receipt(&tx, &stored, &election_row, command.now_ms, false)?;
        tx.commit()
            .map_err(|source| OperationsError::LeadershipCommitUnconfirmed {
                workspace_id: command.scope.workspace_id().into_uuid(),
                source,
            })?;
        Ok(receipt)
    }

    /// Append one bounded review statement with stable message-id replay.
    ///
    /// # Errors
    /// Returns authority, replay, phase, validation, storage, or commit errors.
    #[allow(clippy::needless_pass_by_value)]
    #[allow(clippy::too_many_lines)]
    pub fn append_leadership_statement(
        &mut self,
        command: LeadershipStatementCommand,
    ) -> Result<LeadershipStatementReceipt, OperationsError> {
        validate_statement_command(&command)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        ensure_verified_workspace(&tx, command.workspace_id, self.destination)?;
        if let Some(existing) = load_statement(&tx, command.statement_id)? {
            if existing.workspace_id != command.workspace_id {
                return Err(OperationsError::Conflict("leadership statement Workspace"));
            }
            ensure_statement_replay(&tx, &existing, &command)?;
            let review = load_review_by_request(&tx, command.request_id)?
                .ok_or(OperationsError::Corrupt("leadership statement review"))?;
            let election = super::load_election(&tx, command.election_id)?
                .ok_or(OperationsError::Corrupt("leadership statement election"))?;
            let leadership = super::load_leadership(&tx, review.scope)?
                .ok_or(OperationsError::Corrupt("leadership review scope"))?;
            let phase =
                review_phase_with_leadership(&review, &election, &leadership, command.now_ms);
            tx.commit()?;
            return Ok(LeadershipStatementReceipt {
                statement_id: command.statement_id,
                election_id: command.election_id,
                sequence: existing.sequence,
                phase,
                replay: true,
            });
        }
        let review = load_review_by_request(&tx, command.request_id)?
            .ok_or(OperationsError::Conflict("leadership review missing"))?;
        if review.scope.workspace_id() != command.workspace_id {
            return Err(OperationsError::Conflict("leadership statement Workspace"));
        }
        if review.election_id != command.election_id {
            return Err(OperationsError::Conflict("leadership statement election"));
        }
        let election = super::load_election(&tx, command.election_id)?
            .ok_or(OperationsError::Conflict("leadership election missing"))?;
        if election.state != super::ElectionState::Electing {
            return Err(OperationsError::Conflict("leadership review resolved"));
        }
        let leadership = super::load_leadership(&tx, review.scope)?
            .ok_or(OperationsError::Conflict("leadership review scope missing"))?;
        if leadership.state != super::LeadershipState::Electing
            || leadership.current_election_id != Some(review.election_id)
        {
            return Err(OperationsError::Conflict("leadership review resolved"));
        }
        let resolved = resolve_actor(&tx, review.scope.workspace_id(), command.actor)?;
        let participant = resolved.participant_id.ok_or(OperationsError::Conflict(
            "leadership statements require participant",
        ))?;
        let sources = frozen_source_subjects(&tx, command.election_id)?;
        let is_incumbent = review
            .incumbent
            .as_ref()
            .and_then(|identity| identity.participant_id)
            .is_some_and(|incumbent| incumbent == participant);
        if command.kind == LeadershipStatementKind::Defense && !is_incumbent {
            return Err(OperationsError::Conflict("leadership defense incumbent"));
        }
        if command.kind == LeadershipStatementKind::Discussion
            && !actor_in_frozen_electorate(&resolved, participant, &sources)
        {
            return Err(OperationsError::Conflict("leadership statement electorate"));
        }
        let now = command.now_ms;
        let incumbent_final = command.kind == LeadershipStatementKind::Defense && is_incumbent;
        if command.kind == LeadershipStatementKind::Defense
            && (now >= review.defense_deadline_ms || review.defense_closed_at_ms.is_some())
        {
            return Err(OperationsError::Conflict("leadership defense deadline"));
        }
        if command.kind == LeadershipStatementKind::Discussion
            && (now
                < review
                    .defense_closed_at_ms
                    .unwrap_or(review.defense_deadline_ms)
                || now >= review.discussion_deadline_ms)
        {
            return Err(OperationsError::Conflict("leadership discussion deadline"));
        }
        if incumbent_final && review.incumbent_statement_id.is_some() {
            return Err(OperationsError::Conflict(
                "leadership incumbent defense replay",
            ));
        }
        let sequence = next_statement_sequence(&tx, command.election_id)?;
        insert_statement(&tx, &command, &review, &resolved, sequence, incumbent_final)?;
        let (discussion_deadline, ballot_deadline) = if incumbent_final {
            // The shortened discussion still may not end before the defense
            // window it replaces (ops_leadership_reviews CHECK).
            let discussion = (now
                .checked_add(DISCUSSION_WINDOW_MS)
                .ok_or(OperationsError::Bounds("leadership discussion deadline"))?)
            .max(review.defense_deadline_ms);
            let ballot = discussion
                .checked_add(super::BALLOT_WINDOW_MS)
                .ok_or(OperationsError::Bounds("leadership ballot deadline"))?;
            tx.execute(
                "UPDATE ops_leadership_reviews SET defense_closed_at_ms=?2,incumbent_statement_id=?3,discussion_deadline_ms=?4,ballot_deadline_ms=?5 WHERE request_id=?1",
                params![
                    command.request_id.as_bytes(),
                    now,
                    command.statement_id.as_bytes(),
                    discussion,
                    ballot,
                ],
            )?;
            tx.execute(
                "UPDATE ops_leadership_elections SET ballot_deadline_ms=?2 WHERE election_id=?1 AND state='ELECTING'",
                params![command.election_id.as_bytes(), ballot],
            )?;
            (discussion, ballot)
        } else {
            (review.discussion_deadline_ms, review.ballot_deadline_ms)
        };
        let phase = if incumbent_final {
            LeadershipReviewPhase::Discussion
        } else {
            review_phase_with_deadlines(
                &review,
                &election,
                now,
                Some((discussion_deadline, ballot_deadline)),
            )
        };
        tx.commit()
            .map_err(|source| OperationsError::LeadershipCommitUnconfirmed {
                workspace_id: review.scope.workspace_id().into_uuid(),
                source,
            })?;
        Ok(LeadershipStatementReceipt {
            statement_id: command.statement_id,
            election_id: command.election_id,
            sequence,
            phase,
            replay: false,
        })
    }

    /// Read one bounded review by request or election identity.
    /// # Errors
    /// Returns storage, scope, import-authority, or bound errors.
    pub fn read_leadership_review(
        &self,
        workspace_id: WorkspaceId,
        query: LeadershipReviewQuery,
        now_ms: i64,
        statement_after: Option<u64>,
        statement_limit: u32,
    ) -> Result<Option<LeadershipReviewSnapshot>, OperationsError> {
        if workspace_id.into_uuid().is_nil() {
            return Err(OperationsError::Conflict("leadership review Workspace"));
        }
        validate_statement_limit(statement_limit)?;
        let tx = self.connection.unchecked_transaction()?;
        ensure_verified_workspace(&tx, workspace_id, self.destination)?;
        let Some(review) = load_review_query(&tx, workspace_id, query)? else {
            tx.commit()?;
            return Ok(None);
        };
        let snapshot =
            read_review_snapshot_in_tx(&tx, &review, now_ms, statement_after, statement_limit)?;
        tx.commit()?;
        Ok(Some(snapshot))
    }

    /// # Errors
    /// Returns storage, scope, import-authority, or bound errors.
    pub fn read_leadership_statements(
        &self,
        workspace_id: WorkspaceId,
        election_id: Uuid,
        after_sequence: Option<u64>,
        limit: u32,
    ) -> Result<LeadershipStatementPage, OperationsError> {
        validate_statement_limit(limit)?;
        let tx = self.connection.unchecked_transaction()?;
        ensure_verified_workspace(&tx, workspace_id, self.destination)?;
        let review = load_review_by_election(&tx, election_id)?
            .ok_or(OperationsError::Conflict("leadership review missing"))?;
        if review.scope.workspace_id() != workspace_id {
            return Err(OperationsError::Conflict("leadership review Workspace"));
        }
        let page = load_statement_page(&tx, election_id, after_sequence, limit)?;
        tx.commit()?;
        Ok(page)
    }

    /// Recover the candidate identity for an exact review retry without
    /// requiring the caller's Activity or current effect binding to remain
    /// open. The returned candidate is journaled election data; new request
    /// IDs still use the live mutation path.
    ///
    /// # Errors
    /// Returns scope, actor-proof, import-authority, storage, or corruption
    /// errors.
    pub fn leadership_review_replay_candidate(
        &self,
        workspace_id: WorkspaceId,
        scope: super::LeadershipScope,
        request_id: Uuid,
        actor: OperationalActor,
    ) -> Result<Option<Uuid>, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        ensure_verified_workspace(&tx, workspace_id, self.destination)?;
        let Some(review) = load_review_by_request(&tx, request_id)? else {
            tx.commit()?;
            return Ok(None);
        };
        if review.scope != scope {
            return Err(OperationsError::Conflict(
                "leadership review replay changed",
            ));
        }
        let election = load_review_election(&tx, &review)?;
        if !replay_actor_matches_identity(&tx, workspace_id, actor, &review.proposer)? {
            return Err(OperationsError::Conflict(
                "leadership review replay changed",
            ));
        }
        tx.commit()?;
        Ok(Some(election.candidate_participant_id))
    }

    /// Check an exact statement retry from the durable statement journal.
    /// Existing IDs are validated against the stored actor tuple or a current
    /// actor with the same logical participation/profile; missing IDs return
    /// `false` so the caller can perform live Activity admission.
    ///
    /// # Errors
    /// Returns validation, actor-proof, import-authority, storage, or replay
    /// errors.
    pub fn leadership_statement_replay_exists(
        &self,
        command: &LeadershipStatementCommand,
    ) -> Result<bool, OperationsError> {
        validate_statement_command(command)?;
        let tx = self.connection.unchecked_transaction()?;
        ensure_verified_workspace(&tx, command.workspace_id, self.destination)?;
        let Some(existing) = load_statement(&tx, command.statement_id)? else {
            tx.commit()?;
            return Ok(false);
        };
        if existing.workspace_id != command.workspace_id {
            return Err(OperationsError::Conflict("leadership statement Workspace"));
        }
        ensure_statement_replay(&tx, &existing, command)?;
        tx.commit()?;
        Ok(true)
    }
}

impl OperationsCatalog {
    pub(super) fn ensure_ballot_open(
        tx: &Transaction<'_>,
        election_id: Uuid,
        now_ms: i64,
    ) -> Result<(), OperationsError> {
        ensure_ballot_open(tx, election_id, now_ms)
    }
}

pub(super) fn ensure_ballot_open(
    tx: &Transaction<'_>,
    election_id: Uuid,
    now_ms: i64,
) -> Result<(), OperationsError> {
    let Some(review) = load_review_by_election(tx, election_id)? else {
        return Ok(());
    };
    let leadership = super::load_leadership(tx, review.scope)?
        .ok_or(OperationsError::Conflict("leadership review scope missing"))?;
    if leadership.state != super::LeadershipState::Electing
        || leadership.current_election_id != Some(election_id)
    {
        return Err(OperationsError::Conflict("leadership review resolved"));
    }
    if now_ms < review.discussion_deadline_ms {
        return Err(OperationsError::Conflict(
            "leadership review discussion phase",
        ));
    }
    Ok(())
}

fn validate_review_command(command: &LeadershipReviewCommand) -> Result<(), OperationsError> {
    if command.request_id.is_nil()
        || command.candidate_participant_id.is_nil()
        || command.expected_revision < 1
        || command.now_ms < 0
    {
        return Err(OperationsError::Conflict("leadership review identity"));
    }
    if command.reason.trim().is_empty() || command.reason.len() > MAX_REVIEW_REASON_BYTES {
        return Err(OperationsError::Bounds("leadership review reason"));
    }
    Ok(())
}

fn validate_statement_command(command: &LeadershipStatementCommand) -> Result<(), OperationsError> {
    if command.statement_id.is_nil()
        || command.request_id.is_nil()
        || command.election_id.is_nil()
        || command.now_ms < 0
    {
        return Err(OperationsError::Conflict("leadership statement identity"));
    }
    if command.body.trim().is_empty() || command.body.len() > MAX_STATEMENT_BODY_BYTES {
        return Err(OperationsError::Bounds("leadership statement body"));
    }
    Ok(())
}

fn validate_statement_limit(limit: u32) -> Result<(), OperationsError> {
    if (1..=MAX_REVIEW_PAGE).contains(&limit) {
        Ok(())
    } else {
        Err(OperationsError::Bounds("leadership statement page"))
    }
}

pub(super) fn ensure_verified_workspace(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    destination: Uuid,
) -> Result<(), OperationsError> {
    let verified: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_imports WHERE workspace_id=?1 AND destination_id=?2 AND phase=2)",
        params![workspace_id.into_uuid().as_bytes(), destination.as_bytes()],
        |row| row.get(0),
    )?;
    if verified {
        Ok(())
    } else {
        Err(OperationsError::Conflict(
            "Workspace import is not Verified",
        ))
    }
}

fn leadership_receipt_confirmed(
    tx: &Transaction<'_>,
    scope: super::LeadershipScope,
    election_id: Uuid,
) -> Result<bool, OperationsError> {
    let activity = scope.activity_id().map_or_else(
        || super::NO_ACTIVITY_MARKER.to_vec(),
        |id| id.into_uuid().as_bytes().to_vec(),
    );
    let team = scope.team_id().map_or_else(
        || super::NO_ACTIVITY_MARKER.to_vec(),
        |id| id.as_bytes().to_vec(),
    );
    Ok(tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_leadership_receipts WHERE workspace_id=?1 AND scope_kind=?2 AND activity_id=?3 AND team_id=?4 AND election_id=?5 AND receipt_kind='CONFIRMED')",
        params![
            scope.workspace_id().into_uuid().as_bytes(),
            scope.scope_kind(),
            activity,
            team,
            election_id.as_bytes(),
        ],
        |row| row.get(0),
    )?)
}

fn scope_values(scope: super::LeadershipScope) -> (i64, Option<Vec<u8>>, Option<Vec<u8>>) {
    (
        scope.scope_kind(),
        scope
            .activity_id()
            .map(|id| id.into_uuid().into_bytes().to_vec()),
        scope.team_id().map(|id| id.into_bytes().to_vec()),
    )
}

fn decode_scope(
    workspace_id: WorkspaceId,
    scope_kind: i64,
    activity_id: Option<Vec<u8>>,
    team_id: Option<Vec<u8>>,
) -> Result<super::LeadershipScope, OperationsError> {
    match scope_kind {
        0 if activity_id.is_none() && team_id.is_none() => {
            Ok(super::LeadershipScope::workspace(workspace_id))
        }
        1 => {
            let bytes =
                activity_id.ok_or(OperationsError::Corrupt("leadership review activity"))?;
            Ok(super::LeadershipScope::activity(
                workspace_id,
                ActivityId::from_uuid(Uuid::from_bytes(crate::wire::id(bytes)?)),
            ))
        }
        2 => {
            let bytes = team_id.ok_or(OperationsError::Corrupt("leadership review team"))?;
            Ok(super::LeadershipScope::team(
                workspace_id,
                Uuid::from_bytes(crate::wire::id(bytes)?),
            ))
        }
        _ => Err(OperationsError::Corrupt("leadership review scope")),
    }
}

fn scope_matches(left: super::LeadershipScope, right: super::LeadershipScope) -> bool {
    left.workspace_id() == right.workspace_id()
        && left.activity_id() == right.activity_id()
        && left.team_id() == right.team_id()
}

fn encode_optional_uuid(value: Option<Uuid>) -> Option<Vec<u8>> {
    value.map(|id| id.into_bytes().to_vec())
}

fn encode_optional_session(value: Option<AgentSessionId>) -> Option<Vec<u8>> {
    value.map(|id| id.into_uuid().into_bytes().to_vec())
}

fn encode_optional_participation(value: Option<WorkspaceParticipationId>) -> Option<Vec<u8>> {
    value.map(|id| id.into_uuid().into_bytes().to_vec())
}

#[allow(clippy::too_many_arguments)]
fn insert_review(
    tx: &Transaction<'_>,
    command: &LeadershipReviewCommand,
    election_id: Uuid,
    proposer: &IdentitySnapshot,
    incumbent: Option<&IdentitySnapshot>,
    defense_deadline_ms: i64,
    discussion_deadline_ms: i64,
    ballot_deadline_ms: i64,
    defense_closed_at_ms: Option<i64>,
) -> Result<(), OperationsError> {
    let (scope_kind, activity_id, team_id) = scope_values(command.scope);
    tx.execute(
        "INSERT INTO ops_leadership_reviews(request_id,election_id,workspace_id,scope_kind,activity_id,team_id,expected_leadership_revision,proposer_kind,proposer_activity_id,proposer_agent_id,proposer_session_id,proposer_participation_id,proposer_profile_id,proposer_participant_id,proposer_display_name,incumbent_participant_id,incumbent_participation_id,incumbent_profile_id,incumbent_session_id,incumbent_display_name,reason,defense_deadline_ms,discussion_deadline_ms,ballot_deadline_ms,defense_closed_at_ms,incumbent_statement_id,created_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,NULL,?26)",
        params![
            command.request_id.as_bytes(),
            election_id.as_bytes(),
            command.scope.workspace_id().into_uuid().as_bytes(),
            scope_kind,
            activity_id,
            team_id,
            command.expected_revision,
            proposer.kind,
            proposer.activity_id.map(|id| id.into_uuid().into_bytes().to_vec()),
            proposer.agent_id.map(|id| id.into_uuid().into_bytes().to_vec()),
            encode_optional_session(proposer.session_id),
            encode_optional_participation(proposer.participation_id),
            encode_optional_uuid(proposer.profile_id),
            encode_optional_uuid(proposer.participant_id),
            proposer.display_name,
            incumbent.and_then(|x| encode_optional_uuid(x.participant_id)),
            incumbent.and_then(|x| encode_optional_participation(x.participation_id)),
            incumbent.and_then(|x| encode_optional_uuid(x.profile_id)),
            incumbent.and_then(|x| encode_optional_session(x.session_id)),
            incumbent.map(|x| x.display_name.clone()),
            command.reason,
            defense_deadline_ms,
            discussion_deadline_ms,
            ballot_deadline_ms,
            defense_closed_at_ms,
            command.now_ms,
        ],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn decode_identity(
    kind: i64,
    activity: Option<Vec<u8>>,
    agent: Option<Vec<u8>>,
    session: Option<Vec<u8>>,
    participation: Option<Vec<u8>>,
    profile: Option<Vec<u8>>,
    participant: Option<Vec<u8>>,
    display_name: String,
) -> Result<IdentitySnapshot, OperationsError> {
    let decode = |value: Option<Vec<u8>>, field| {
        value
            .map(|bytes| decode_uuid_bytes(bytes, field))
            .transpose()
    };
    let activity_id =
        decode(activity, "leadership review proposer activity")?.map(ActivityId::from_uuid);
    let agent_id = decode(agent, "leadership review proposer agent")?
        .map(forge_domain::AgentInstanceId::from_uuid);
    let session_id =
        decode(session, "leadership review proposer session")?.map(AgentSessionId::from_uuid);
    let participation_id = decode(participation, "leadership review proposer participation")?
        .map(WorkspaceParticipationId::from_uuid)
        .transpose()
        .map_err(|_| OperationsError::Corrupt("leadership review proposer participation"))?;
    let profile_id = decode(profile, "leadership review proposer profile")?;
    let participant_id = decode(participant, "leadership review proposer participant")?;
    Ok(IdentitySnapshot {
        kind,
        activity_id,
        agent_id,
        session_id,
        participation_id,
        profile_id,
        participant_id,
        display_name,
    })
}

fn decode_uuid_bytes(bytes: Vec<u8>, field: &'static str) -> Result<Uuid, OperationsError> {
    Ok(Uuid::from_bytes(
        crate::wire::id(bytes).map_err(|_| OperationsError::Corrupt(field))?,
    ))
}

fn decode_string(bytes: Vec<u8>, field: &'static str) -> Result<String, OperationsError> {
    String::from_utf8(bytes).map_err(|_| OperationsError::Corrupt(field))
}

fn to_sql_error(error: OperationsError) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}

fn decode_uuid_sql(bytes: Vec<u8>, field: &'static str) -> rusqlite::Result<Uuid> {
    decode_uuid_bytes(bytes, field).map_err(to_sql_error)
}

fn decode_string_sql(bytes: Vec<u8>, field: &'static str) -> rusqlite::Result<String> {
    decode_string(bytes, field).map_err(to_sql_error)
}

fn load_review_by_request(
    tx: &Transaction<'_>,
    request_id: Uuid,
) -> Result<Option<ReviewRow>, OperationsError> {
    load_review(tx, "WHERE request_id=?1", params![request_id.as_bytes()])
}

fn load_review_election(
    tx: &Transaction<'_>,
    review: &ReviewRow,
) -> Result<super::ElectionRow, OperationsError> {
    let election = super::load_election(tx, review.election_id)?
        .ok_or(OperationsError::Corrupt("leadership review election"))?;
    if election.scope != review.scope {
        return Err(OperationsError::Corrupt("leadership review election scope"));
    }
    Ok(election)
}

fn load_review_by_election(
    tx: &Transaction<'_>,
    election_id: Uuid,
) -> Result<Option<ReviewRow>, OperationsError> {
    load_review(tx, "WHERE election_id=?1", params![election_id.as_bytes()])
}

fn load_review_query(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    query: LeadershipReviewQuery,
) -> Result<Option<ReviewRow>, OperationsError> {
    match (query.request_id, query.election_id) {
        (Some(request_id), None) => {
            let review = load_review_by_request(tx, request_id)?;
            if review
                .as_ref()
                .is_some_and(|review| review.scope.workspace_id() != workspace_id)
            {
                return Err(OperationsError::Conflict("leadership review Workspace"));
            }
            Ok(review)
        }
        (None, Some(election_id)) => {
            let review = load_review_by_election(tx, election_id)?;
            if review
                .as_ref()
                .is_some_and(|review| review.scope.workspace_id() != workspace_id)
            {
                return Err(OperationsError::Conflict("leadership review Workspace"));
            }
            Ok(review)
        }
        _ => Err(OperationsError::Conflict("leadership review query")),
    }
}

fn load_review(
    tx: &Transaction<'_>,
    where_sql: &str,
    params: impl rusqlite::Params,
) -> Result<Option<ReviewRow>, OperationsError> {
    let sql = format!(
        "SELECT substr(request_id,1,17),substr(election_id,1,17),substr(workspace_id,1,17),scope_kind,substr(activity_id,1,17),substr(team_id,1,17),expected_leadership_revision,proposer_kind,substr(proposer_activity_id,1,17),substr(proposer_agent_id,1,17),substr(proposer_session_id,1,17),substr(proposer_participation_id,1,17),substr(proposer_profile_id,1,17),substr(proposer_participant_id,1,17),substr(CAST(proposer_display_name AS BLOB),1,129),substr(incumbent_participant_id,1,17),substr(incumbent_participation_id,1,17),substr(incumbent_profile_id,1,17),substr(incumbent_session_id,1,17),substr(CAST(incumbent_display_name AS BLOB),1,129),substr(CAST(reason AS BLOB),1,4097),defense_deadline_ms,discussion_deadline_ms,ballot_deadline_ms,defense_closed_at_ms,substr(incumbent_statement_id,1,17),created_at_ms FROM ops_leadership_reviews {where_sql}"
    );
    tx.query_row(&sql, params, |row| {
        let request_id = decode_uuid_sql(row.get(0)?, "leadership review request")?;
        let election_id = decode_uuid_sql(row.get(1)?, "leadership review election")?;
        let workspace_id =
            WorkspaceId::from_uuid(decode_uuid_sql(row.get(2)?, "leadership review Workspace")?);
        let scope = decode_scope(workspace_id, row.get(3)?, row.get(4)?, row.get(5)?)
            .map_err(to_sql_error)?;
        let proposer = decode_identity(
            row.get(7)?,
            row.get(8)?,
            row.get(9)?,
            row.get(10)?,
            row.get(11)?,
            row.get(12)?,
            row.get(13)?,
            decode_string_sql(row.get(14)?, "leadership proposer display")?,
        )
        .map_err(to_sql_error)?;
        let incumbent_participant = row
            .get::<_, Option<Vec<u8>>>(15)?
            .map(|value| decode_uuid_sql(value, "leadership incumbent participant"))
            .transpose()?;
        let incumbent = if let Some(participant_id) = incumbent_participant {
            let session_id = row
                .get::<_, Option<Vec<u8>>>(18)?
                .map(|value| {
                    decode_uuid_sql(value, "leadership incumbent session")
                        .map(AgentSessionId::from_uuid)
                })
                .transpose()?;
            let profile_id = row
                .get::<_, Option<Vec<u8>>>(17)?
                .map(|value| decode_uuid_sql(value, "leadership incumbent profile"))
                .transpose()?;
            let display_name = row
                .get::<_, Option<Vec<u8>>>(19)?
                .map(|value| decode_string_sql(value, "leadership incumbent display"))
                .transpose()?
                .ok_or_else(|| {
                    to_sql_error(OperationsError::Corrupt("leadership incumbent display"))
                })?;
            Some(IdentitySnapshot {
                kind: 1,
                activity_id: None,
                agent_id: None,
                session_id,
                participation_id: row
                    .get::<_, Option<Vec<u8>>>(16)?
                    .map(|value| {
                        decode_uuid_sql(value, "leadership incumbent participation").and_then(
                            |id| WorkspaceParticipationId::from_uuid(id).map_err(to_sql_error),
                        )
                    })
                    .transpose()?,
                profile_id,
                participant_id: Some(participant_id),
                display_name,
            })
        } else {
            None
        };
        Ok(ReviewRow {
            request_id,
            election_id,
            scope,
            expected_revision: row.get(6)?,
            proposer,
            incumbent,
            reason: decode_string_sql(row.get(20)?, "leadership review reason")?,
            defense_deadline_ms: row.get(21)?,
            discussion_deadline_ms: row.get(22)?,
            ballot_deadline_ms: row.get(23)?,
            defense_closed_at_ms: row.get(24)?,
            incumbent_statement_id: row
                .get::<_, Option<Vec<u8>>>(25)?
                .map(|value| decode_uuid_sql(value, "leadership incumbent statement"))
                .transpose()?,
            created_at_ms: row.get(26)?,
        })
    })
    .optional()
    .map_err(OperationsError::from)
}

fn next_statement_sequence(
    tx: &Transaction<'_>,
    election_id: Uuid,
) -> Result<u64, OperationsError> {
    let (count, max): (i64, Option<i64>) = tx.query_row(
        "SELECT COUNT(*),MAX(sequence) FROM ops_leadership_review_statements WHERE election_id=?1",
        params![election_id.as_bytes()],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if count >= MAX_STATEMENTS_PER_ELECTION {
        return Err(OperationsError::Bounds("leadership review statements"));
    }
    u64::try_from(
        max.unwrap_or(0)
            .checked_add(1)
            .ok_or(OperationsError::Bounds("leadership statement sequence"))?,
    )
    .map_err(|_| OperationsError::Bounds("leadership statement sequence"))
}

fn insert_statement(
    tx: &Transaction<'_>,
    command: &LeadershipStatementCommand,
    review: &ReviewRow,
    actor: &ResolvedOperationalActor,
    sequence: u64,
    incumbent_final: bool,
) -> Result<(), OperationsError> {
    let (activity_id, agent_id) = match command.actor {
        OperationalActor::Participant {
            activity_id,
            agent_id,
            ..
        } => (activity_id, agent_id),
        OperationalActor::LocalOperator => {
            return Err(OperationsError::Conflict(
                "leadership statements require participant",
            ));
        }
    };
    let session_id = actor.session_id.ok_or(OperationsError::Conflict(
        "leadership statement actor Session",
    ))?;
    let participation_id = actor.participation_id.ok_or(OperationsError::Conflict(
        "leadership statement participation",
    ))?;
    let profile_id = actor
        .profile_id
        .ok_or(OperationsError::Conflict("leadership statement profile"))?;
    let participant_id = actor.participant_id.ok_or(OperationsError::Conflict(
        "leadership statement participant",
    ))?;
    tx.execute(
        "INSERT INTO ops_leadership_review_statements(statement_id,request_id,election_id,workspace_id,statement_kind,author_kind,author_activity_id,author_agent_id,author_session_id,author_participation_id,author_profile_id,author_participant_id,author_display_name,body,sequence,submitted_at_ms,incumbent_final) VALUES(?1,?2,?3,?4,?5,1,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
        params![
            command.statement_id.as_bytes(),
            command.request_id.as_bytes(),
            command.election_id.as_bytes(),
            command.workspace_id.into_uuid().as_bytes(),
            command.kind.encode(),
            activity_id.into_uuid().as_bytes(),
            agent_id.into_uuid().as_bytes(),
            session_id.into_uuid().as_bytes(),
            participation_id.into_uuid().as_bytes(),
            profile_id.into_bytes(),
            participant_id.as_bytes(),
            actor.display_name,
            command.body,
            i64::try_from(sequence).map_err(|_| OperationsError::Bounds("leadership statement sequence"))?,
            command.now_ms,
            i64::from(incumbent_final),
        ],
    )?;
    if review.scope.workspace_id() != command.workspace_id {
        return Err(OperationsError::Conflict("leadership statement Workspace"));
    }
    Ok(())
}

fn load_statement(
    tx: &Transaction<'_>,
    statement_id: Uuid,
) -> Result<Option<StatementRow>, OperationsError> {
    tx.query_row(
        "SELECT substr(statement_id,1,17),substr(request_id,1,17),substr(election_id,1,17),substr(workspace_id,1,17),statement_kind,substr(author_activity_id,1,17),substr(author_agent_id,1,17),substr(author_session_id,1,17),substr(author_participation_id,1,17),substr(author_profile_id,1,17),substr(author_participant_id,1,17),substr(CAST(author_display_name AS BLOB),1,129),substr(CAST(body AS BLOB),1,16385),sequence,submitted_at_ms,incumbent_final FROM ops_leadership_review_statements WHERE statement_id=?1",
        params![statement_id.as_bytes()],
        |row| {
            let workspace_id = WorkspaceId::from_uuid(decode_uuid_sql(
                row.get(3)?,
                "leadership statement Workspace",
            )?);
            let author = IdentitySnapshot {
                kind: 1,
                activity_id: Some(ActivityId::from_uuid(decode_uuid_sql(
                    row.get(5)?,
                    "leadership statement activity",
                )?)),
                agent_id: Some(forge_domain::AgentInstanceId::from_uuid(decode_uuid_sql(
                    row.get(6)?,
                    "leadership statement agent",
                )?)),
                session_id: Some(AgentSessionId::from_uuid(decode_uuid_sql(
                    row.get(7)?,
                    "leadership statement session",
                )?)),
                participation_id: Some(
                    WorkspaceParticipationId::from_uuid(decode_uuid_sql(
                        row.get(8)?,
                        "leadership statement participation",
                    )?)
                    .map_err(to_sql_error)?,
                ),
                profile_id: Some(decode_uuid_sql(row.get(9)?, "leadership statement profile")?),
                participant_id: Some(decode_uuid_sql(
                    row.get(10)?,
                    "leadership statement participant",
                )?),
                display_name: decode_string_sql(row.get(11)?, "leadership statement author")?,
            };
            Ok(StatementRow {
                statement_id: decode_uuid_sql(row.get(0)?, "leadership statement id")?,
                request_id: decode_uuid_sql(row.get(1)?, "leadership statement request")?,
                election_id: decode_uuid_sql(row.get(2)?, "leadership statement election")?,
                workspace_id,
                kind: LeadershipStatementKind::decode(row.get::<_, String>(4)?.as_str())
                    .map_err(to_sql_error)?,
                author,
                body: decode_string_sql(row.get(12)?, "leadership statement body")?,
                sequence: u64::try_from(row.get::<_, i64>(13)?)
                    .map_err(|_| to_sql_error(OperationsError::Corrupt("leadership statement sequence")))?,
                submitted_at_ms: row.get(14)?,
                incumbent_final: row.get::<_, i64>(15)? != 0,
            })
        },
    )
    .optional()
    .map_err(OperationsError::from)
}

fn ensure_statement_replay(
    tx: &Transaction<'_>,
    existing: &StatementRow,
    command: &LeadershipStatementCommand,
) -> Result<(), OperationsError> {
    let same_actor =
        replay_actor_matches_identity(tx, existing.workspace_id, command.actor, &existing.author)?;
    if existing.request_id != command.request_id
        || existing.election_id != command.election_id
        || existing.workspace_id != command.workspace_id
        || existing.kind != command.kind
        || existing.body != command.body
        || !same_actor
    {
        return Err(OperationsError::Conflict("leadership statement replay"));
    }
    Ok(())
}

fn review_receipt(
    tx: &Transaction<'_>,
    review: &ReviewRow,
    election: &super::ElectionRow,
    now_ms: i64,
    replay: bool,
) -> Result<LeadershipReviewReceipt, OperationsError> {
    let leadership = super::load_leadership(tx, review.scope)?
        .ok_or(OperationsError::Corrupt("leadership review scope"))?;
    Ok(LeadershipReviewReceipt {
        request_id: review.request_id,
        election_id: review.election_id,
        scope: review.scope,
        leadership_revision: leadership.leadership_revision,
        phase: review_phase_with_leadership(review, election, &leadership, now_ms),
        defense_deadline_ms: review.defense_deadline_ms,
        discussion_deadline_ms: review.discussion_deadline_ms,
        ballot_deadline_ms: review.ballot_deadline_ms,
        replay,
        electorate_size: crate::election_subject::electorate_size(tx, review.election_id)?,
    })
}

fn review_phase(
    review: &ReviewRow,
    election: &super::ElectionRow,
    now_ms: i64,
) -> LeadershipReviewPhase {
    review_phase_with_deadlines(review, election, now_ms, None)
}

fn review_phase_with_deadlines(
    review: &ReviewRow,
    election: &super::ElectionRow,
    now_ms: i64,
    override_deadlines: Option<(i64, i64)>,
) -> LeadershipReviewPhase {
    match election.state {
        super::ElectionState::ClosedElected => return LeadershipReviewPhase::AwaitingDecision,
        super::ElectionState::ClosedNoMajority | super::ElectionState::Cancelled => {
            return LeadershipReviewPhase::Resolved;
        }
        super::ElectionState::Electing => {}
    }
    let discussion_deadline =
        override_deadlines.map_or(review.discussion_deadline_ms, |value| value.0);
    let ballot_deadline = override_deadlines.map_or(review.ballot_deadline_ms, |value| value.1);
    if review.defense_closed_at_ms.is_none() && now_ms < review.defense_deadline_ms {
        LeadershipReviewPhase::Defense
    } else if now_ms < discussion_deadline {
        LeadershipReviewPhase::Discussion
    } else if now_ms < ballot_deadline {
        LeadershipReviewPhase::Ballot
    } else {
        LeadershipReviewPhase::AwaitingDecision
    }
}

fn review_phase_with_leadership(
    review: &ReviewRow,
    election: &super::ElectionRow,
    leadership: &super::LeadershipRow,
    now_ms: i64,
) -> LeadershipReviewPhase {
    if election.state == super::ElectionState::Electing {
        return review_phase(review, election, now_ms);
    }
    match leadership.state {
        super::LeadershipState::AuthorityPending => LeadershipReviewPhase::AwaitingDecision,
        _ => LeadershipReviewPhase::Resolved,
    }
}

fn load_statement_page(
    tx: &Transaction<'_>,
    election_id: Uuid,
    after_sequence: Option<u64>,
    limit: u32,
) -> Result<LeadershipStatementPage, OperationsError> {
    let after = i64::try_from(after_sequence.unwrap_or(0))
        .map_err(|_| OperationsError::Bounds("leadership statement cursor"))?;
    let sql_limit = i64::from(limit) + 1;
    let mut statement = tx.prepare(
        "SELECT substr(statement_id,1,17),substr(request_id,1,17),substr(election_id,1,17),substr(workspace_id,1,17),statement_kind,substr(author_activity_id,1,17),substr(author_agent_id,1,17),substr(author_session_id,1,17),substr(author_participation_id,1,17),substr(author_profile_id,1,17),substr(author_participant_id,1,17),substr(CAST(author_display_name AS BLOB),1,129),substr(CAST(body AS BLOB),1,16385),sequence,submitted_at_ms,incumbent_final FROM ops_leadership_review_statements WHERE election_id=?1 AND sequence>?2 ORDER BY sequence ASC LIMIT ?3",
    )?;
    let rows = statement.query_map(params![election_id.as_bytes(), after, sql_limit], |row| {
        decode_statement_row(row)
    })?;
    let mut values = Vec::with_capacity(limit as usize + 1);
    for row in rows {
        values.push(row?.into_public());
    }
    let has_more = values.len() > limit as usize;
    if has_more {
        values.pop();
    }
    let next_cursor = has_more
        .then(|| values.last().map(LeadershipStatementRecord::sequence))
        .flatten();
    Ok(LeadershipStatementPage {
        statements: values,
        next_cursor,
        has_more,
    })
}

fn decode_statement_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StatementRow> {
    let workspace_id = WorkspaceId::from_uuid(decode_uuid_sql(
        row.get(3)?,
        "leadership statement Workspace",
    )?);
    let participation_id = WorkspaceParticipationId::from_uuid(decode_uuid_sql(
        row.get(8)?,
        "leadership statement participation",
    )?)
    .map_err(to_sql_error)?;
    Ok(StatementRow {
        statement_id: decode_uuid_sql(row.get(0)?, "leadership statement id")?,
        request_id: decode_uuid_sql(row.get(1)?, "leadership statement request")?,
        election_id: decode_uuid_sql(row.get(2)?, "leadership statement election")?,
        workspace_id,
        kind: LeadershipStatementKind::decode(row.get::<_, String>(4)?.as_str())
            .map_err(to_sql_error)?,
        author: IdentitySnapshot {
            kind: 1,
            activity_id: Some(ActivityId::from_uuid(decode_uuid_sql(
                row.get(5)?,
                "leadership statement activity",
            )?)),
            agent_id: Some(forge_domain::AgentInstanceId::from_uuid(decode_uuid_sql(
                row.get(6)?,
                "leadership statement agent",
            )?)),
            session_id: Some(AgentSessionId::from_uuid(decode_uuid_sql(
                row.get(7)?,
                "leadership statement session",
            )?)),
            participation_id: Some(participation_id),
            profile_id: Some(decode_uuid_sql(
                row.get(9)?,
                "leadership statement profile",
            )?),
            participant_id: Some(decode_uuid_sql(
                row.get(10)?,
                "leadership statement participant",
            )?),
            display_name: decode_string_sql(row.get(11)?, "leadership statement author")?,
        },
        body: decode_string_sql(row.get(12)?, "leadership statement body")?,
        sequence: u64::try_from(row.get::<_, i64>(13)?)
            .map_err(|_| to_sql_error(OperationsError::Corrupt("leadership statement sequence")))?,
        submitted_at_ms: row.get(14)?,
        incumbent_final: row.get::<_, i64>(15)? != 0,
    })
}

fn load_voters(
    tx: &Transaction<'_>,
    election_id: Uuid,
) -> Result<(Vec<LeadershipVoterSnapshot>, bool), OperationsError> {
    let cutover: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_election_subject_cutovers WHERE election_id=?1)",
        params![election_id.as_bytes()],
        |row| row.get(0),
    )?;
    let mut voters = if cutover {
        let mut statement = tx.prepare(
            "SELECT substr(COALESCE(b.cast_by_participant_id,s.participant_id),1,17),subj.subject_kind,substr(subj.subject_id,1,17),substr(subj.agent_profile_id,1,17),substr(m.session_id,1,17),substr(CAST(m.display_name AS BLOB),1,129),b.choice,b.cast_at_ms FROM ops_election_voter_subjects subj LEFT JOIN (SELECT election_id,subject_kind,subject_id,MIN(participant_id) AS participant_id FROM ops_election_voter_sources GROUP BY election_id,subject_kind,subject_id) s ON s.election_id=subj.election_id AND s.subject_kind=subj.subject_kind AND s.subject_id=subj.subject_id LEFT JOIN ops_election_subject_ballots b ON b.election_id=subj.election_id AND b.subject_kind=subj.subject_kind AND b.subject_id=subj.subject_id LEFT JOIN ops_logical_members m ON m.workspace_id=(SELECT workspace_id FROM ops_leadership_elections WHERE election_id=?1) AND m.participant_id=COALESCE(b.cast_by_participant_id,s.participant_id) WHERE subj.election_id=?1 ORDER BY subj.subject_kind,subj.subject_id LIMIT ?2",
        )?;
        let rows = statement.query_map(params![election_id.as_bytes(), 65_i64], |row| {
            decode_voter_row(row)
        })?;
        rows.collect::<Result<Vec<_>, _>>()?
    } else {
        let mut statement = tx.prepare(
            "SELECT substr(e.participant_id,1,17),1,substr(e.participant_id,1,17),NULL,substr(m.session_id,1,17),substr(CAST(m.display_name AS BLOB),1,129),b.choice,b.cast_at_ms FROM ops_election_electorate e LEFT JOIN ops_logical_members m ON m.workspace_id=(SELECT workspace_id FROM ops_leadership_elections WHERE election_id=?1) AND m.participant_id=e.participant_id LEFT JOIN ops_ballots b ON b.election_id=e.election_id AND b.voter_participant_id=e.participant_id WHERE e.election_id=?1 ORDER BY e.participant_id LIMIT ?2",
        )?;
        let rows = statement.query_map(params![election_id.as_bytes(), 65_i64], |row| {
            decode_voter_row(row)
        })?;
        rows.collect::<Result<Vec<_>, _>>()?
    };
    let truncated = voters.len() > 64;
    if truncated {
        voters.pop();
    }
    Ok((voters, truncated))
}

fn decode_voter_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<LeadershipVoterSnapshot> {
    let participant_id = decode_uuid_sql(row.get(0)?, "leadership voter participant")?;
    let subject_kind: i64 = row.get(1)?;
    let subject_id = decode_uuid_sql(row.get(2)?, "leadership voter subject")?;
    let participation_id = if subject_kind == 0 {
        Some(WorkspaceParticipationId::from_uuid(subject_id).map_err(to_sql_error)?)
    } else {
        None
    };
    let profile_id = row
        .get::<_, Option<Vec<u8>>>(3)?
        .map(|value| decode_uuid_sql(value, "leadership voter profile"))
        .transpose()?;
    let session_id = row
        .get::<_, Option<Vec<u8>>>(4)?
        .map(|value| {
            decode_uuid_sql(value, "leadership voter session").map(AgentSessionId::from_uuid)
        })
        .transpose()?;
    let display_name = row
        .get::<_, Option<Vec<u8>>>(5)?
        .map(|value| decode_string_sql(value, "leadership voter display"))
        .transpose()?;
    let choice = row
        .get::<_, Option<String>>(6)?
        .map(|value| super::BallotChoice::decode(&value))
        .transpose()
        .map_err(to_sql_error)?;
    Ok(LeadershipVoterSnapshot {
        participant_id,
        subject_id,
        participation_id,
        profile_id,
        session_id,
        display_name,
        choice,
        cast_at_ms: row.get(7)?,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FrozenSource {
    participant_id: Uuid,
    subject_kind: i64,
    subject_id: Uuid,
}

fn frozen_source_subjects(
    tx: &Transaction<'_>,
    election_id: Uuid,
) -> Result<Vec<FrozenSource>, OperationsError> {
    let cutover: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_election_subject_cutovers WHERE election_id=?1)",
        params![election_id.as_bytes()],
        |row| row.get(0),
    )?;
    let sql = if cutover {
        "SELECT substr(participant_id,1,17),subject_kind,substr(subject_id,1,17) FROM ops_election_voter_sources WHERE election_id=?1 ORDER BY participant_id"
    } else {
        "SELECT substr(participant_id,1,17),1,substr(participant_id,1,17) FROM ops_election_electorate WHERE election_id=?1 ORDER BY participant_id"
    };
    let mut statement = tx.prepare(sql)?;
    let rows = statement.query_map(params![election_id.as_bytes()], |row| {
        Ok(FrozenSource {
            participant_id: decode_uuid_sql(row.get(0)?, "leadership voter participant")?,
            subject_kind: row.get(1)?,
            subject_id: decode_uuid_sql(row.get(2)?, "leadership voter subject")?,
        })
    })?;
    let mut values = Vec::new();
    for row in rows {
        values.push(row?);
        if values.len() > 64 {
            return Err(OperationsError::Bounds("leadership electorate"));
        }
    }
    if values.is_empty() {
        return Err(OperationsError::Corrupt("leadership electorate"));
    }
    Ok(values)
}

fn actor_in_frozen_electorate(
    resolved: &ResolvedOperationalActor,
    participant_id: Uuid,
    sources: &[FrozenSource],
) -> bool {
    sources.iter().any(|source| {
        (source.subject_kind == 0
            && resolved
                .participation_id
                .is_some_and(|participation| participation.into_uuid() == source.subject_id))
            || (source.subject_kind == 1 && source.participant_id == participant_id)
    })
}

fn ensure_review_replay(
    tx: &Transaction<'_>,
    existing: &ReviewRow,
    command: &LeadershipReviewCommand,
    election: &super::ElectionRow,
) -> Result<(), OperationsError> {
    if !scope_matches(existing.scope, command.scope)
        || existing.expected_revision != command.expected_revision
        || election.candidate_participant_id != command.candidate_participant_id
        || existing.reason != command.reason
        || !replay_actor_matches_identity(
            tx,
            existing.scope.workspace_id(),
            command.actor,
            &existing.proposer,
        )?
    {
        return Err(OperationsError::Conflict(
            "leadership review replay changed",
        ));
    }
    Ok(())
}

fn resolved_actor_matches_identity(
    resolved: &ResolvedOperationalActor,
    identity: &IdentitySnapshot,
) -> bool {
    match resolved.participant_id {
        Some(participant_id) => {
            identity.kind == 1
                && identity.participant_id == Some(participant_id)
                && identity.participation_id == resolved.participation_id
                && identity.profile_id == resolved.profile_id
        }
        None => identity.kind == 0,
    }
}

fn actor_tuple_matches_identity(actor: OperationalActor, identity: &IdentitySnapshot) -> bool {
    match actor {
        OperationalActor::LocalOperator => identity.kind == 0,
        OperationalActor::Participant {
            activity_id,
            agent_id,
            session_id,
        } => {
            identity.kind == 1
                && identity.activity_id == Some(activity_id)
                && identity.agent_id == Some(agent_id)
                && identity.session_id == Some(session_id)
        }
    }
}

fn replay_actor_matches_identity(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    actor: OperationalActor,
    identity: &IdentitySnapshot,
) -> Result<bool, OperationsError> {
    if actor_tuple_matches_identity(actor, identity) {
        return Ok(true);
    }
    match resolve_actor(tx, workspace_id, actor) {
        Ok(resolved) => Ok(resolved_actor_matches_identity(&resolved, identity)),
        Err(OperationsError::Conflict(_)) => Ok(false),
        Err(error) => Err(error),
    }
}

fn load_identity(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
) -> Result<Option<IdentitySnapshot>, OperationsError> {
    tx.query_row(
        "SELECT substr(p.participation_id,1,17),substr(p.agent_profile_id,1,17),substr(l.session_id,1,17),substr(CAST(l.display_name AS BLOB),1,129) FROM ops_logical_members AS l JOIN ops_workspace_participation_mappings AS m ON m.workspace_id=l.workspace_id AND m.participant_id=l.participant_id JOIN ops_workspace_participations AS p ON p.workspace_id=m.workspace_id AND p.participation_id=m.participation_id WHERE l.workspace_id=?1 AND l.participant_id=?2",
        params![workspace_id.into_uuid().as_bytes(), participant_id.as_bytes()],
        |row| {
            let participation = decode_uuid_sql(
                row.get(0)?,
                "leadership incumbent participation",
            )?;
            let profile = decode_uuid_sql(row.get(1)?, "leadership incumbent profile")?;
            let session = decode_uuid_sql(row.get(2)?, "leadership incumbent session")?;
            let display_name = decode_string_sql(row.get(3)?, "leadership incumbent display")?;
            Ok(IdentitySnapshot {
                kind: 1,
                activity_id: None,
                agent_id: None,
                session_id: Some(AgentSessionId::from_uuid(session)),
                participation_id: Some(
                    WorkspaceParticipationId::from_uuid(participation).map_err(to_sql_error)?,
                ),
                profile_id: Some(profile),
                participant_id: Some(participant_id),
                display_name,
            })
        },
    )
    .optional()
    .map_err(OperationsError::from)
}

/// A bounded history entry. Full public discussion is read by request ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipReviewHistoryEntry {
    request_id: Uuid,
    election_id: Uuid,
    created_at_ms: i64,
    reason_preview: String,
    phase: LeadershipReviewPhase,
}
impl LeadershipReviewHistoryEntry {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }
    #[must_use]
    pub const fn election_id(&self) -> Uuid {
        self.election_id
    }
    #[must_use]
    pub const fn created_at_ms(&self) -> i64 {
        self.created_at_ms
    }
    #[must_use]
    pub fn reason_preview(&self) -> &str {
        &self.reason_preview
    }
    pub const fn phase(&self) -> LeadershipReviewPhase {
        self.phase
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipReviewHistory {
    entries: Vec<LeadershipReviewHistoryEntry>,
    next_cursor: Option<Uuid>,
}
impl LeadershipReviewHistory {
    #[must_use]
    pub fn entries(&self) -> &[LeadershipReviewHistoryEntry] {
        &self.entries
    }
    #[must_use]
    pub const fn next_cursor(&self) -> Option<Uuid> {
        self.next_cursor
    }
}
impl OperationsCatalog {
    /// Discover review history even after the active election pointer is cleared.
    ///
    /// # Errors
    /// Rejects foreign cursors, invalid limits and corrupt or unavailable history.
    pub fn leadership_review_history(
        &self,
        scope: super::LeadershipScope,
        after: Option<Uuid>,
        limit: u32,
        now_ms: i64,
    ) -> Result<LeadershipReviewHistory, OperationsError> {
        validate_statement_limit(limit)?;
        let tx = self.connection.unchecked_transaction()?;
        ensure_verified_workspace(&tx, scope.workspace_id(), self.destination)?;
        let cursor = after
            .map(|id| {
                let row = load_review_by_request(&tx, id)?
                    .ok_or(OperationsError::Conflict("leadership history cursor"))?;
                if row.scope != scope {
                    return Err(OperationsError::Conflict("leadership history scope"));
                }
                Ok(row.created_at_ms)
            })
            .transpose()?;
        let requests = {
            let mut query=tx.prepare("SELECT substr(request_id,1,17) FROM ops_leadership_reviews WHERE workspace_id=?1 AND scope_kind=?2 AND activity_id IS ?3 AND team_id IS ?4 AND (?5 IS NULL OR created_at_ms<?5 OR (created_at_ms=?5 AND request_id<?6)) ORDER BY created_at_ms DESC,request_id DESC LIMIT ?7")?;
            query
                .query_map(
                    params![
                        scope.workspace_id().into_uuid().as_bytes(),
                        scope.scope_kind(),
                        scope
                            .activity_id()
                            .map(|id| id.into_uuid().as_bytes().to_vec()),
                        scope.team_id().map(|id| id.as_bytes().to_vec()),
                        cursor,
                        after.map(|id| id.as_bytes().to_vec()),
                        i64::from(limit) + 1
                    ],
                    |row| row.get::<_, Vec<u8>>(0),
                )?
                .collect::<Result<Vec<_>, _>>()?
        };
        let more = requests.len()
            > usize::try_from(limit)
                .map_err(|_| OperationsError::Bounds("leadership history limit"))?;
        let leadership = super::load_leadership(&tx, scope)?;
        let mut entries = Vec::new();
        for id in requests.into_iter().take(
            usize::try_from(limit)
                .map_err(|_| OperationsError::Bounds("leadership history limit"))?,
        ) {
            let request_id = Uuid::from_bytes(crate::wire::id(id)?);
            let row = load_review_by_request(&tx, request_id)?
                .ok_or(OperationsError::Corrupt("leadership review history"))?;
            let election = super::load_election(&tx, row.election_id)?
                .ok_or(OperationsError::Corrupt("leadership review election"))?;
            let leadership = leadership
                .as_ref()
                .ok_or(OperationsError::Corrupt("leadership review scope"))?;
            entries.push(LeadershipReviewHistoryEntry {
                request_id,
                election_id: row.election_id,
                created_at_ms: row.created_at_ms,
                reason_preview: row.reason.chars().take(160).collect(),
                phase: review_phase_with_leadership(&row, &election, leadership, now_ms),
            });
        }
        let next_cursor = if more {
            entries.last().map(|entry| entry.request_id)
        } else {
            None
        };
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(LeadershipReviewHistory {
            entries,
            next_cursor,
        })
    }
}

mod driver;

#[cfg(test)]
pub(crate) mod tests;

/// Latest confirmed election timestamp that changed the leader for one scope.
/// Unconfirmed ELECTED receipts, bootstrap/mark-stable confirmations and
/// confirmed re-elections of the incumbent do not start the cooldown.
/// Different source participants also preserve the leader when immutable election
/// witnesses identify the same participation. Current Profile mappings cannot
/// rewrite that historical identity after confirmation.
pub(crate) fn last_leader_change_ms(
    tx: &Connection,
    scope: &LeadershipScope,
) -> Result<Option<i64>, OperationsError> {
    let activity_bytes: Vec<u8> = scope.activity_id().map_or_else(
        || super::NO_ACTIVITY_MARKER.to_vec(),
        |activity| activity.into_uuid().as_bytes().to_vec(),
    );
    let team_bytes = scope.team_id().map_or_else(
        || super::NO_ACTIVITY_MARKER.to_vec(),
        |team| team.as_bytes().to_vec(),
    );
    let last: Option<i64> = tx
        .query_row(
            "SELECT MAX(r.recorded_at_ms) \
             FROM ops_leadership_receipts r \
             JOIN ops_leadership_elections e ON e.election_id=r.election_id \
                 AND e.workspace_id=r.workspace_id \
                 AND e.scope_kind=r.scope_kind \
                 AND COALESCE(e.activity_id,zeroblob(16))=r.activity_id \
                 AND COALESCE(e.team_id,zeroblob(16))=r.team_id \
             WHERE r.workspace_id=?1 AND r.scope_kind=?2 \
                 AND r.activity_id=?3 AND r.team_id=?4 \
                 AND r.receipt_kind='CONFIRMED' \
                 AND r.participant_id=e.elected_participant_id \
                 AND r.participant_id IS NOT e.incumbent_participant_id \
                 AND NOT EXISTS ( \
                     SELECT 1 \
                     FROM ops_election_work_credit_snapshots candidate \
                     WHERE candidate.election_id=e.election_id \
                         AND candidate.workspace_id=e.workspace_id \
                         AND candidate.workspace_id=r.workspace_id \
                         AND candidate.candidate_participant_id=e.elected_participant_id \
                         AND candidate.subject_kind=0 \
                         AND ( \
                             EXISTS ( \
                                 SELECT 1 \
                                 FROM ops_leadership_reviews review \
                                 WHERE review.election_id=e.election_id \
                                     AND review.workspace_id=r.workspace_id \
                                     AND review.scope_kind=r.scope_kind \
                                     AND COALESCE(review.activity_id,zeroblob(16))=r.activity_id \
                                     AND COALESCE(review.team_id,zeroblob(16))=r.team_id \
                                     AND review.incumbent_participant_id=e.incumbent_participant_id \
                                     AND review.incumbent_participation_id=candidate.subject_id \
                             ) \
                             OR EXISTS ( \
                                 SELECT 1 \
                                 FROM ops_election_voter_sources incumbent_source \
                                 WHERE incumbent_source.election_id=e.election_id \
                                     AND incumbent_source.participant_id=e.incumbent_participant_id \
                                     AND incumbent_source.subject_kind=0 \
                                     AND incumbent_source.subject_id=candidate.subject_id \
                             ) \
                         ) \
                 )",
            params![
                scope.workspace_id().into_uuid().as_bytes(),
                scope.scope_kind(),
                activity_bytes,
                team_bytes,
            ],
            |row| row.get(0),
        )
        .map_err(OperationsError::Sqlite)?;
    Ok(last)
}
