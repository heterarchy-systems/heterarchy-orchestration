//! Transport-neutral Leadership/Election application operations.
//!
//! The durable election/governance authority (state machine, exact revision
//! CAS, quorum, electorate freeze, replay/exclusion semantics) lives in
//! `heterarchy-orchestration-operations`. This module is the application
//! layer between that authority and any transport adapter (Axum, MCP): it
//! resolves the calling session to its canonical member Agent identity,
//! performs bounded admission checks, and owns Broker-fenced confirmation and
//! recovery stages. It never talks to a transport and never opens Forge
//! activities.
//!
//! # Authority contracts preserved
//!
//! * Every mutation stays behind the caller-performed Broker owner gate —
//!   revalidate `OrchestrationRuntime::ensure_broker_owner` immediately
//!   before calling the mutating methods here, exactly like
//!   `create_agent_profile`.
//! * Recall requests keep the exact `request_id` replay identity, the
//!   `expected_revision` CAS fence, and the frozen-electorate candidate
//!   admission of `request_leadership_review`.
//! * Ballots stay bound to the Ready participant of the calling session;
//!   repeated identical ballots replay, changed ballots conflict.
//! * Deterministic commands carry caller-supplied clocks. Runtime owns the
//!   current timestamps sampled for durable confirmation and recovery stages.
//! * Source-authority checks are supplied by trusted composition and re-run
//!   before intent preparation, after Broker awaits, and before recovery
//!   reconciliation. No catalog lock is held across those awaits.

use forge_domain::{
    ActivityId, AgentInstanceId, AgentSessionId, AuthenticatedPrincipal, TimestampMillis,
    WorkspaceId,
};
use heterarchy_orchestration_operations::{
    BallotChoice, CastBallotCommand, LeadershipConfirmIntent, LeadershipConfirmIntentState,
    LeadershipConfirmReadback, LeadershipElectionSummary, LeadershipRecoveryResolution,
    LeadershipReviewCommand, LeadershipReviewHistoryEntry, LeadershipReviewPhase, LeadershipScope,
    LeadershipState, LeadershipStatementKind, LeadershipView, OperationsError,
    PrepareLeadershipConfirmCommand, PrepareLeadershipConfirmOutcome, ReconcileRecoveryCommand,
};
use uuid::Uuid;

use crate::{OrchestrationRuntime, RuntimeError};

pub mod ballots;
pub mod recalls;
pub mod reviews;
pub mod statements;

pub use heterarchy_orchestration_operations::{
    FencedConfirmCommand, FencedConfirmOutcome, LeadershipFenceExecutor, ProductionFence,
};

/// Canonical member binding Agent identity for one logical Session's exact
/// Workspace/Activity participation, byte-identical to the MCP member
/// derivation (`MEMBER_AGENT_DOMAIN` = `member-binding-agent/v1`).
#[must_use]
pub fn member_agent_id(workspace: Uuid, activity: Uuid, session: Uuid) -> AgentInstanceId {
    AgentInstanceId::from_uuid(derived_participation_uuid(
        b"heterarchy-orchestration/member-binding-agent/v1\0",
        workspace,
        Some(activity),
        session,
    ))
}

/// blake3-derived, version/variant-stamped identity shared with the MCP
/// member path. The digest input order (domain, workspace, optional activity,
/// session) and the version/variant bit stamps must not change: durable rows
/// in both paths resolve the same identities through them.
fn derived_participation_uuid(
    domain: &[u8],
    workspace: Uuid,
    activity: Option<Uuid>,
    session: Uuid,
) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(workspace.as_bytes());
    if let Some(activity) = activity {
        hasher.update(activity.as_bytes());
    }
    hasher.update(session.as_bytes());
    stamped_uuid(&hasher.finalize().as_bytes()[..16])
}

fn stamped_uuid(digest_prefix: &[u8]) -> Uuid {
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(digest_prefix);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

/// Which leadership scope a recall request targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeadershipTarget {
    /// The Workspace Orchestrator scope.
    Workspace,
    /// The caller's own Activity scope.
    Activity,
    /// One exact Team scope.
    Team(Uuid),
}

/// Start one recall election with the calling Ready member as candidate.
#[derive(Debug, Clone)]
pub struct RecallReviewCommand {
    /// Exact Workspace scope.
    pub workspace_id: Uuid,
    /// The caller's Activity (membership provenance).
    pub activity_id: Uuid,
    /// The caller's logical Session.
    pub session_id: Uuid,
    /// Scope the election targets.
    pub target: LeadershipTarget,
    /// Exact leadership revision the caller observed (>= 1).
    pub expected_revision: i64,
    /// Exact replay identity of the review request.
    pub request_id: Uuid,
    /// Human-readable recall reason (1..=4096 bytes, domain-validated).
    pub reason: String,
    /// Caller-supplied wall-clock milliseconds.
    pub now_ms: i64,
}

/// Append one Defense or Discussion statement to one open election.
#[derive(Debug, Clone)]
pub struct StatementCommand {
    /// Exact Workspace scope.
    pub workspace_id: Uuid,
    /// The caller's Activity (membership provenance).
    pub activity_id: Uuid,
    /// The caller's logical Session.
    pub session_id: Uuid,
    /// Open election the statement belongs to.
    pub election_id: Uuid,
    /// Exact replay identity of the statement.
    pub message_id: Uuid,
    /// Exact replay identity of the originating review request.
    pub request_id: Uuid,
    /// Defense (incumbent only) or Discussion (frozen electorate only).
    pub kind: LeadershipStatementKind,
    /// Statement body (1..=16384 bytes, domain-validated).
    pub body: String,
    /// Caller-supplied wall-clock milliseconds.
    pub now_ms: i64,
}

/// Cast the calling session's ballot in one open recall election.
#[derive(Debug, Clone, Copy)]
pub struct BallotCommand {
    /// Exact Workspace scope.
    pub workspace_id: Uuid,
    /// The caller's Activity (membership provenance).
    pub activity_id: Uuid,
    /// The caller's logical Session.
    pub session_id: Uuid,
    /// Open election to vote in.
    pub election_id: Uuid,
    /// Yes / No / Abstain.
    pub choice: BallotChoice,
    /// Caller-supplied wall-clock milliseconds.
    pub now_ms: i64,
}

/// Advance only deterministic server-owned lifecycle edges.
#[derive(Debug, Clone, Copy)]
pub struct AdvanceCommand {
    /// Exact Workspace scope.
    pub workspace_id: Uuid,
    /// Narrows the scope to one Activity's leader.
    pub activity_id: Option<Uuid>,
    /// Narrows the scope to one Team.
    pub team_id: Option<Uuid>,
    /// Caller-supplied wall-clock milliseconds.
    pub now_ms: i64,
}

/// Bounded projection of one scope's durable leadership view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipViewSummary {
    /// Workspace scope.
    pub workspace_id: Uuid,
    /// Activity scope, when narrowed.
    pub activity_id: Option<Uuid>,
    /// Team scope, when narrowed.
    pub team_id: Option<Uuid>,
    /// Durable leadership state label (lowercase snake).
    pub state: &'static str,
    /// Exact leadership revision (the CAS fence).
    pub leadership_revision: i64,
    /// Leader participation identity, when present.
    pub leader_participation_id: Option<Uuid>,
    /// Leader profile identity, when present.
    pub leader_profile_id: Option<Uuid>,
    /// Current leader participant, when present.
    pub leader_participant_id: Option<Uuid>,
    /// Current leader display name, when present.
    pub leader_display_name: Option<String>,
    /// Stabilization window end, when stabilizing.
    pub stabilization_until_ms: Option<i64>,
    /// Durable view update timestamp.
    pub updated_at_ms: i64,
    /// Open or recently closed election summary, when one is attached.
    pub current_election: Option<LeadershipElectionSummaryProjection>,
}

/// Bounded projection of the election attached to a leadership view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipElectionSummaryProjection {
    /// Election identity.
    pub election_id: Uuid,
    /// Election lifecycle state label.
    pub state: &'static str,
    /// Ballot deadline.
    pub ballot_deadline_ms: i64,
    /// Whether the ballot deadline has passed at read time.
    pub deadline_passed: bool,
    /// Frozen electorate size.
    pub electorate_size: i64,
    /// Tallies observed so far.
    pub cast_yes: i64,
    pub cast_no: i64,
    pub cast_abstain: i64,
}

/// One page of discoverable review history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipReviewHistorySummary {
    /// Entries in creation order.
    pub entries: Vec<LeadershipReviewHistoryEntrySummary>,
    /// Cursor for the next page.
    pub next_cursor: Option<Uuid>,
}

/// One discoverable review history row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipReviewHistoryEntrySummary {
    /// Exact review request identity.
    pub request_id: Uuid,
    /// Election identity.
    pub election_id: Uuid,
    /// Creation timestamp.
    pub created_at_ms: i64,
    /// Bounded reason preview.
    pub reason_preview: String,
    /// Review phase label.
    pub phase: &'static str,
}

/// Start-of-election receipt projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipReviewReceiptSummary {
    /// Exact review request identity.
    pub request_id: Uuid,
    /// Election identity.
    pub election_id: Uuid,
    /// Leadership revision observed when this receipt was produced.
    pub leadership_revision: i64,
    /// Frozen electorate size.
    pub electorate_size: i64,
    /// Phase observed at receipt time; an exact replay can report later progress.
    pub phase: &'static str,
    /// Persisted review deadlines; failover-required reviews may close Defense immediately.
    pub defense_deadline_ms: i64,
    pub discussion_deadline_ms: i64,
    pub ballot_deadline_ms: i64,
    /// True when the request was an exact replay.
    pub replay: bool,
}

/// Statement receipt projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipStatementReceiptSummary {
    /// Exact statement identity.
    pub message_id: Uuid,
    /// Election identity.
    pub election_id: Uuid,
    /// Election-bounded sequence.
    pub sequence: u64,
    /// Phase at append time.
    pub phase: &'static str,
    /// True when the statement was an exact replay.
    pub replay: bool,
}

/// Ballot receipt projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BallotReceiptSummary {
    /// Election identity.
    pub election_id: Uuid,
    /// Resolved voter participant.
    pub voter_participant_id: Uuid,
    /// Canonical participation subject for the frozen electorate, when present.
    pub voter_participation_id: Option<Uuid>,
    /// Recorded choice.
    pub choice: BallotChoice,
    /// Ballot revision.
    pub ballot_revision: i64,
    /// True when the ballot was an exact replay.
    pub replay: bool,
}

/// Advance outcome projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipAdvanceSummary {
    /// Whether any deterministic edge advanced.
    pub advanced: bool,
    /// Post-advance durable view, when one exists.
    pub leadership: Option<LeadershipView>,
}

fn saga_now() -> Result<i64, RuntimeError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .map_err(|_| RuntimeError::InvalidInput)
}

/// Deterministic confirm-intent-state labels shared by every transport
/// projection.
#[must_use]
pub const fn confirm_intent_state_label(
    state: heterarchy_orchestration_operations::LeadershipConfirmIntentState,
) -> &'static str {
    use heterarchy_orchestration_operations::LeadershipConfirmIntentState as S;
    match state {
        S::Pending => "pending",
        S::RemoteRecorded => "remote_recorded",
        S::Committed => "committed",
        S::Superseded => "superseded",
        S::Rejected => "rejected",
    }
}

impl OrchestrationRuntime {
    /// Read the durable leadership view for one exact scope, optionally
    /// followed by the discoverable review history page.
    ///
    /// # Errors
    /// Returns invalid input, durable catalog failures, or a poisoned-owner
    /// error.
    pub fn leadership_view(
        &self,
        workspace_id: Uuid,
        activity_id: Option<Uuid>,
        team_id: Option<Uuid>,
        history_after: Option<Uuid>,
        history_limit: u32,
        now_ms: i64,
    ) -> Result<
        (
            Option<LeadershipViewSummary>,
            LeadershipReviewHistorySummary,
        ),
        RuntimeError,
    > {
        let scope = leadership_scope(workspace_id, activity_id, team_id)?;
        let now = timestamp(now_ms)?;
        let catalog = self.catalog()?;
        let view = catalog.leadership_scope_view(scope, now)?;
        let history =
            catalog.leadership_review_history(scope, history_after, history_limit, now)?;
        Ok((
            view.as_ref().map(LeadershipViewSummary::from_view),
            LeadershipReviewHistorySummary::from_history(&history),
        ))
    }

    /// Read one exact confirmation intent and its current durable scope view.
    ///
    /// The caller must hold the configured operator grant and name an owned
    /// open Session with Workspace read access. This query is side-effect-free.
    ///
    /// # Errors
    /// Returns invalid input, authorization or scope conflicts, or durable
    /// catalog failures.
    pub fn leadership_confirmation(
        &self,
        scope: LeadershipScope,
        request_id: Uuid,
        session_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<(LeadershipConfirmIntent, LeadershipConfirmReadback)>, RuntimeError> {
        self.require_operator(principal)?;
        let session_id = require_session(session_id)?.into_uuid();
        let scope = leadership_scope(
            scope.workspace_id().into_uuid(),
            scope.activity_id().map(ActivityId::into_uuid),
            scope.team_id(),
        )?;
        self.require_open_session(session_id, principal)?;
        self.require_workspace_read(
            scope.workspace_id().into_uuid(),
            Some(session_id),
            principal,
        )?;
        let request_id = require_non_nil(request_id)?;
        let snapshot = self
            .catalog()?
            .leadership_confirmation_snapshot(request_id)?;
        match snapshot {
            Some((intent, readback)) if intent.scope() == scope && readback.scope() == scope => {
                Ok(Some((intent, readback)))
            }
            Some(_) => Err(RuntimeError::Conflict),
            None => Ok(None),
        }
    }

    /// Advance only deterministic server-owned lifecycle edges for one scope.
    ///
    /// The caller must revalidate the Broker owner authority immediately
    /// before this call. Authority confirmation remains a separate
    /// Broker-fenced step and is intentionally not part of this operation.
    ///
    /// # Errors
    /// Returns invalid input, durable catalog failures, or a poisoned-owner
    /// error.
    pub fn advance_leadership(
        &self,
        command: AdvanceCommand,
    ) -> Result<LeadershipAdvanceSummary, RuntimeError> {
        let scope = leadership_scope(command.workspace_id, command.activity_id, command.team_id)?;
        let now = timestamp(command.now_ms)?;
        let mut catalog = self.catalog()?;
        let (advanced, view) = catalog.advance_leadership(scope, now)?;
        Ok(LeadershipAdvanceSummary {
            advanced,
            leadership: view,
        })
    }

    /// Start one recall election with the calling Ready member as candidate.
    ///
    /// The caller must revalidate the Broker owner authority immediately
    /// before this call, and the caller's Activity must already be open; this
    /// layer never opens Forge activities. Exact `request_id` replays return
    /// the same durable election with a newly observed phase/revision. The
    /// durable authority rejects replay drift and owns the exact
    /// `expected_revision` CAS fence plus frozen-electorate admission.
    ///
    /// # Errors
    /// Returns invalid input, stale revision conflict, non-Ready or
    /// unqualified candidate conflict, durable catalog failures, or a
    /// poisoned-owner error.
    pub fn request_leadership_review(
        &self,
        command: RecallReviewCommand,
    ) -> Result<LeadershipReviewReceiptSummary, RuntimeError> {
        let context = recalls::validated_context(&command)?;
        let mut catalog = self.catalog()?;
        // Replay first: an exact repeat returns the original election without
        // requiring readiness again. Any new candidate must be the caller's
        // Ready participant, resolved to the frozen-electorate subject by the
        // durable authority itself.
        let candidate = match catalog.leadership_review_replay_candidate(
            context.workspace,
            context.scope,
            command.request_id,
            context.actor,
        )? {
            Some(candidate) => candidate,
            None => catalog
                .ready_participant_for_agent(context.workspace, context.activity, context.agent_id)?
                .ok_or(OperationsError::Conflict("leadership candidate not ready"))?,
        };
        let receipt = catalog.request_leadership_review(LeadershipReviewCommand {
            request_id: command.request_id,
            scope: context.scope,
            expected_revision: command.expected_revision,
            actor: context.actor,
            candidate_participant_id: candidate,
            reason: command.reason,
            now_ms: context.now_ms,
        })?;
        Ok(LeadershipReviewReceiptSummary::from_receipt(&receipt))
    }

    /// Append one Defense or Discussion statement to one open election.
    ///
    /// The caller must revalidate the Broker owner authority immediately
    /// before this call and must own an open Activity. Exact statement
    /// replays return the original receipt.
    ///
    /// # Errors
    /// Returns invalid input, phase/authority conflicts, durable catalog
    /// failures, or a poisoned-owner error.
    pub fn append_leadership_statement(
        &self,
        command: StatementCommand,
    ) -> Result<LeadershipStatementReceiptSummary, RuntimeError> {
        let replay_probe = statements::operations_command(command)?;
        let mut catalog = self.catalog()?;
        let receipt = catalog.append_leadership_statement(replay_probe)?;
        Ok(LeadershipStatementReceiptSummary::from_receipt(&receipt))
    }

    /// Cast the calling session's ballot in one open recall election.
    ///
    /// The caller must revalidate the Broker owner authority immediately
    /// before this call. The Ready participant of the calling session is the
    /// runtime provenance; the durable authority resolves it to the frozen
    /// voter subject.
    ///
    /// # Errors
    /// Returns invalid input, unready-voter or closed-election conflict,
    /// durable catalog failures, or a poisoned-owner error.
    pub fn cast_leadership_ballot(
        &self,
        command: BallotCommand,
    ) -> Result<BallotReceiptSummary, RuntimeError> {
        let workspace = require_workspace(command.workspace_id)?;
        let activity = ActivityId::from_uuid(require_activity(command.activity_id)?);
        let now = timestamp(command.now_ms)?;
        // Session identity only participates in the member Agent derivation;
        // the durable authority binds ballots to the Ready participant.
        if command.session_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        let agent_id = member_agent_id(
            command.workspace_id,
            command.activity_id,
            command.session_id,
        );
        let mut catalog = self.catalog()?;
        let participant = catalog
            .ready_participant_for_agent(workspace, activity, agent_id)?
            .ok_or(OperationsError::Conflict("leadership voter not ready"))?;
        // The frozen election owns its Workspace and scope; reject a foreign
        // election before the ballot mutation can write its subject row.
        let _ = catalog.leadership_ballot(workspace, command.election_id, participant)?;
        let receipt = catalog.cast_ballot(CastBallotCommand {
            election_id: command.election_id,
            voter_participant_id: participant,
            choice: command.choice,
            now_ms: now,
        })?;
        Ok(BallotReceiptSummary::from_receipt(&receipt))
    }
}

fn leadership_scope(
    workspace_id: Uuid,
    activity_id: Option<Uuid>,
    team_id: Option<Uuid>,
) -> Result<LeadershipScope, RuntimeError> {
    let workspace = require_workspace(workspace_id)?;
    match (activity_id, team_id) {
        (None, None) => Ok(LeadershipScope::workspace(workspace)),
        (Some(activity), None) => Ok(LeadershipScope::activity(
            workspace,
            ActivityId::from_uuid(require_activity(activity)?),
        )),
        (None, Some(team)) => Ok(LeadershipScope::team(workspace, require_non_nil(team)?)),
        (Some(_), Some(_)) => Err(RuntimeError::InvalidInput),
    }
}

fn require_workspace(value: Uuid) -> Result<WorkspaceId, RuntimeError> {
    if value.is_nil() {
        return Err(RuntimeError::InvalidInput);
    }
    Ok(WorkspaceId::from_uuid(value))
}

fn require_activity(value: Uuid) -> Result<Uuid, RuntimeError> {
    if value.is_nil() {
        return Err(RuntimeError::InvalidInput);
    }
    Ok(value)
}

fn require_session(value: Uuid) -> Result<AgentSessionId, RuntimeError> {
    if value.is_nil() {
        return Err(RuntimeError::InvalidInput);
    }
    Ok(AgentSessionId::from_uuid(value))
}

fn require_non_nil(value: Uuid) -> Result<Uuid, RuntimeError> {
    if value.is_nil() {
        return Err(RuntimeError::InvalidInput);
    }
    Ok(value)
}

fn timestamp(value: i64) -> Result<i64, RuntimeError> {
    TimestampMillis::new(value)
        .map(TimestampMillis::get)
        .map_err(|_| RuntimeError::InvalidInput)
}

impl LeadershipViewSummary {
    /// Project one full durable view into the existing bounded transport summary.
    #[must_use]
    pub fn from_view(view: &LeadershipView) -> Self {
        Self {
            workspace_id: view.workspace_id().into_uuid(),
            activity_id: view.activity_id().map(ActivityId::into_uuid),
            team_id: view.team_id(),
            state: leadership_state_label(view.state()),
            leadership_revision: view.leadership_revision(),
            leader_participation_id: view
                .leader_participation_id()
                .map(heterarchy_orchestration_operations::WorkspaceParticipationId::into_uuid),
            leader_profile_id: view.leader_profile_id(),
            leader_participant_id: view.leader_participant_id(),
            leader_display_name: view.leader_display_name().map(str::to_owned),
            stabilization_until_ms: view.stabilization_until_ms(),
            updated_at_ms: view.updated_at_ms(),
            current_election: view
                .current_election()
                .map(LeadershipElectionSummaryProjection::from_summary),
        }
    }
}

impl LeadershipElectionSummaryProjection {
    fn from_summary(election: &LeadershipElectionSummary) -> Self {
        Self {
            election_id: election.election_id(),
            state: election_state_label(election.state()),
            ballot_deadline_ms: election.ballot_deadline_ms(),
            deadline_passed: election.deadline_passed(),
            electorate_size: election.electorate_size(),
            cast_yes: election.cast_yes(),
            cast_no: election.cast_no(),
            cast_abstain: election.cast_abstain(),
        }
    }
}

impl LeadershipReviewHistorySummary {
    fn from_history(
        history: &heterarchy_orchestration_operations::LeadershipReviewHistory,
    ) -> Self {
        Self {
            entries: history
                .entries()
                .iter()
                .map(
                    |entry: &LeadershipReviewHistoryEntry| LeadershipReviewHistoryEntrySummary {
                        request_id: entry.request_id(),
                        election_id: entry.election_id(),
                        created_at_ms: entry.created_at_ms(),
                        reason_preview: entry.reason_preview().to_owned(),
                        phase: review_phase_label(entry.phase()),
                    },
                )
                .collect(),
            next_cursor: history.next_cursor(),
        }
    }
}

impl LeadershipReviewReceiptSummary {
    fn from_receipt(
        receipt: &heterarchy_orchestration_operations::LeadershipReviewReceipt,
    ) -> Self {
        Self {
            request_id: receipt.request_id(),
            election_id: receipt.election_id(),
            leadership_revision: receipt.leadership_revision(),
            electorate_size: receipt.electorate_size(),
            phase: review_phase_label(receipt.phase()),
            defense_deadline_ms: receipt.defense_deadline_ms(),
            discussion_deadline_ms: receipt.discussion_deadline_ms(),
            ballot_deadline_ms: receipt.ballot_deadline_ms(),
            replay: receipt.replay(),
        }
    }
}

impl LeadershipStatementReceiptSummary {
    fn from_receipt(
        receipt: &heterarchy_orchestration_operations::LeadershipStatementReceipt,
    ) -> Self {
        Self {
            message_id: receipt.statement_id(),
            election_id: receipt.election_id(),
            sequence: receipt.sequence(),
            phase: review_phase_label(receipt.phase()),
            replay: receipt.replay(),
        }
    }
}

impl BallotReceiptSummary {
    fn from_receipt(receipt: &heterarchy_orchestration_operations::BallotReceipt) -> Self {
        Self {
            election_id: receipt.election_id(),
            voter_participant_id: receipt.voter_participant_id(),
            voter_participation_id: receipt
                .voter_participation_id()
                .map(heterarchy_orchestration_operations::WorkspaceParticipationId::into_uuid),
            choice: receipt.choice(),
            ballot_revision: receipt.ballot_revision(),
            replay: receipt.replay(),
        }
    }
}

/// Deterministic state labels shared by every transport projection.
#[must_use]
pub const fn leadership_state_label(state: LeadershipState) -> &'static str {
    match state {
        LeadershipState::Leaderless => "leaderless",
        LeadershipState::Bootstrapping => "bootstrapping",
        LeadershipState::Stabilizing => "stabilizing",
        LeadershipState::Active => "active",
        LeadershipState::Electing => "electing",
        LeadershipState::AuthorityPending => "authority_pending",
        LeadershipState::FailoverRequired => "failover_required",
        LeadershipState::Vacant => "vacant",
        LeadershipState::RecoveryRequired => "recovery_required",
    }
}

/// Deterministic election state labels shared by every transport projection.
#[must_use]
pub const fn election_state_label(
    state: heterarchy_orchestration_operations::ElectionState,
) -> &'static str {
    match state {
        heterarchy_orchestration_operations::ElectionState::Electing => "electing",
        heterarchy_orchestration_operations::ElectionState::ClosedElected => "closed_elected",
        heterarchy_orchestration_operations::ElectionState::ClosedNoMajority => {
            "closed_no_majority"
        }
        heterarchy_orchestration_operations::ElectionState::Cancelled => "cancelled",
    }
}

/// Deterministic review phase labels shared by every transport projection.
#[must_use]
pub const fn review_phase_label(phase: LeadershipReviewPhase) -> &'static str {
    match phase {
        LeadershipReviewPhase::Defense => "defense",
        LeadershipReviewPhase::Discussion => "discussion",
        LeadershipReviewPhase::Ballot => "ballot",
        LeadershipReviewPhase::AwaitingDecision => "awaiting_decision",
        LeadershipReviewPhase::Resolved => "resolved",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fenced_operations_can_run_as_owned_send_tasks() -> Result<(), Box<dyn std::error::Error>> {
        fn require_send<T>(future: impl std::future::Future<Output = T> + Send) {
            drop(future);
        }
        let root = std::env::temp_dir().join(format!("leadership-send-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        require_send(runtime.confirm_elected_leader(
            FencedConfirmCommand {
                scope: LeadershipScope::workspace(WorkspaceId::from_uuid(Uuid::now_v7())),
                election_id: Uuid::now_v7(),
                expected_revision: 1,
                request_id: Uuid::now_v7(),
            },
            || std::future::ready(Ok(())),
        ));
        require_send(runtime.reconcile_leadership_recovery(
            LeadershipScope::workspace(WorkspaceId::from_uuid(Uuid::now_v7())),
            Uuid::now_v7(),
            1,
            || std::future::ready(Ok(())),
        ));
        drop(runtime);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn source_check_denial_before_prepare_leaves_no_confirm_intent()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("leadership-source-check-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        let request_id = Uuid::now_v7();
        let executor = tokio::runtime::Builder::new_current_thread().build()?;
        let result = executor.block_on(runtime.confirm_elected_leader(
            FencedConfirmCommand {
                scope: LeadershipScope::workspace(WorkspaceId::from_uuid(Uuid::now_v7())),
                election_id: Uuid::now_v7(),
                expected_revision: 1,
                request_id,
            },
            || std::future::ready(Err(RuntimeError::SourceAuthorityUnavailable)),
        ));
        assert!(matches!(
            result,
            Err(RuntimeError::SourceAuthorityUnavailable)
        ));

        let operations = runtime.operations_catalog();
        let catalog = operations
            .lock()
            .map_err(|_| RuntimeError::CatalogOwnerPoisoned)?;
        assert_eq!(catalog.leadership_confirm_intent(request_id)?, None);
        drop(catalog);
        drop(operations);
        drop(runtime);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn member_agent_id_matches_the_canonical_member_binding() {
        // Pinned against the MCP member derivation
        // (blake3 over the domain, workspace, activity, session in order,
        // stamped to UUIDv8-compatible version/variant bits). Any drift here
        // would split durable membership identity between transports.
        let workspace = Uuid::from_u128(0xA100);
        let activity = Uuid::from_u128(0xA200);
        let session = Uuid::from_u128(0xA300);
        let agent = member_agent_id(workspace, activity, session);
        let expected = derived_participation_uuid(
            b"heterarchy-orchestration/member-binding-agent/v1\0",
            workspace,
            Some(activity),
            session,
        );
        assert_eq!(agent.into_uuid(), expected);
        // Stamp bits: version 8, RFC 4122 variant.
        let bytes = *expected.as_bytes();
        assert_eq!(bytes[6] >> 4, 0x8);
        assert_eq!(bytes[8] >> 6, 0b10);
        // A different session must derive a different binding.
        assert_ne!(
            member_agent_id(workspace, activity, Uuid::from_u128(0xA301)).into_uuid(),
            agent.into_uuid()
        );
    }

    #[test]
    fn leadership_input_validation_fails_closed() {
        // Scope/timestamp validation precedes any durable access.
        assert!(leadership_scope(Uuid::nil(), None, None).is_err());
        assert!(
            leadership_scope(Uuid::now_v7(), Some(Uuid::now_v7()), Some(Uuid::now_v7())).is_err()
        );
        assert!(leadership_scope(Uuid::now_v7(), None, Some(Uuid::nil())).is_err());
        // The domain clock contract rejects only negative timestamps.
        assert!(timestamp(-1).is_err());
        assert!(timestamp(0).is_ok());
    }
}

/// Outcome of one leadership recovery reconciliation run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeadershipRecoveryOutcome {
    /// The fence replay committed, and its full durable confirmation readback
    /// shows that the requested recovery revision has already moved.
    Replayed {
        /// Durable intent after the replay.
        intent: Box<LeadershipConfirmIntent>,
        /// Current leadership readback for the intent's exact scope.
        readback: Box<LeadershipConfirmReadback>,
    },
    /// Full Operations resolution of the recorded recovery evidence.
    Reconciled(LeadershipRecoveryResolution),
}

impl LeadershipRecoveryOutcome {
    /// Resolve the existing transport label from the full durable outcome.
    ///
    /// # Errors
    /// Returns a conflict when a replayed intent remains pending or remotely
    /// recorded, because neither state has terminal recovery evidence.
    pub fn resolution_label(&self) -> Result<&'static str, RuntimeError> {
        match self {
            Self::Replayed { intent, readback } => match intent.state() {
                LeadershipConfirmIntentState::Committed
                    if readback.leader_participant_id()
                        == Some(intent.elected_participant_id())
                        && intent.remote_recorded_revision()
                            == Some(intent.requested_fence_revision())
                        && intent.confirmed_at_ms() == Some(readback.updated_at_ms())
                        && intent.expected_local_revision().checked_add(1)
                            == Some(readback.leadership_revision())
                        && matches!(
                            readback.state(),
                            LeadershipState::Stabilizing | LeadershipState::Active
                        ) =>
                {
                    Ok("confirm_completed")
                }
                LeadershipConfirmIntentState::Committed
                | LeadershipConfirmIntentState::Superseded
                | LeadershipConfirmIntentState::Rejected => {
                    if matches!(
                        readback.state(),
                        LeadershipState::RecoveryRequired | LeadershipState::AuthorityPending
                    ) {
                        Ok("unresolved")
                    } else {
                        Ok("resolved")
                    }
                }
                LeadershipConfirmIntentState::Pending
                | LeadershipConfirmIntentState::RemoteRecorded => Err(RuntimeError::Conflict),
            },
            Self::Reconciled(resolution) => Ok(match resolution {
                LeadershipRecoveryResolution::ConfirmCompleted(_) => "confirm_completed",
                LeadershipRecoveryResolution::Resolved(_) => "resolved",
                LeadershipRecoveryResolution::Unresolved(_) => "unresolved",
            }),
        }
    }
}

impl OrchestrationRuntime {
    /// Run the full Broker-fenced confirm saga for one elected leader:
    /// prepare the intent, record the exact authority fence on the Broker
    /// (no catalog lock held across the await), then commit on the recorded
    /// outcome or exclude on a stale-fence proof, resuming once through
    /// re-prepare when the local commit was uncertain. A
    /// `Conflict`/`Unconfirmed` fence leaves the intent resumable and
    /// surfaces the typed unconfirmed error.
    ///
    /// The caller must hold Broker owner authority for the run. `check`
    /// independently revalidates operator, Session and source authority before
    /// the intent is prepared and after the Broker await. A failed late check
    /// leaves the prepared intent durable for exact Broker replay.
    ///
    /// # Errors
    /// Returns invalid input, fence storage failures, or the typed
    /// unconfirmed error; never collapses them.
    pub async fn confirm_elected_leader<F, Fut>(
        &self,
        command: FencedConfirmCommand,
        check: F,
    ) -> Result<FencedConfirmOutcome, RuntimeError>
    where
        F: Fn() -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<(), RuntimeError>> + Send,
    {
        let fence =
            ProductionFence::new(self.broker_cluster_config()).map_err(RuntimeError::from)?;
        let scope = leadership_scope(
            command.scope.workspace_id().into_uuid(),
            command.scope.activity_id().map(ActivityId::into_uuid),
            command.scope.team_id(),
        )?;
        check().await?;

        // Step 1 (short lock): prepare the durable confirm intent. The guard
        // is dropped before any await — the fence await must never hold the
        // catalog lock.
        let prepared = self
            .run_catalog_stage(move |catalog| {
                catalog
                    .prepare_leadership_confirm(PrepareLeadershipConfirmCommand {
                        scope,
                        election_id: command.election_id,
                        expected_revision: command.expected_revision,
                        request_id: command.request_id,
                        now_ms: saga_now()?,
                    })
                    .map_err(RuntimeError::from)
            })
            .await?;
        let prepared = match prepared {
            PrepareLeadershipConfirmOutcome::Prepared(prepared) => prepared,
            PrepareLeadershipConfirmOutcome::Resolved(readback) => {
                return Ok(FencedConfirmOutcome::Resolved(Box::new(readback)));
            }
        };
        let outcome = fence.record(prepared.fence_request()).await;
        check().await?;
        self.run_catalog_stage(move |catalog| {
            heterarchy_orchestration_operations::complete_fenced_confirm(
                catalog, &prepared, outcome,
            )
            .map_err(RuntimeError::from)
        })
        .await
    }

    /// Reconcile one parked leadership recovery from the exact durable
    /// confirm intent: re-prepare the fence, replay it through the Broker,
    /// and reconcile the recorded outcome. The caller's expected revision is
    /// validated before any effect and never re-applied after the replay
    /// moved the row.
    ///
    /// The caller must hold Broker owner authority for the run. `check`
    /// independently revalidates operator, Session and source authority before
    /// preparation, after the Broker await, and before final reconciliation. A
    /// failed post-fence check leaves the prepared intent available for exact
    /// Broker replay.
    ///
    /// # Errors
    /// Returns invalid input, stale revision/foreign-intent conflict, or
    /// durable/fence failures (unconfirmed fence surfaces the typed
    /// unconfirmed error).
    pub async fn reconcile_leadership_recovery<F, Fut>(
        &self,
        scope: LeadershipScope,
        request_id: Uuid,
        expected_revision: i64,
        check: F,
    ) -> Result<LeadershipRecoveryOutcome, RuntimeError>
    where
        F: Fn() -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<(), RuntimeError>> + Send,
    {
        let fence =
            ProductionFence::new(self.broker_cluster_config()).map_err(RuntimeError::from)?;
        let scope = leadership_scope(
            scope.workspace_id().into_uuid(),
            scope.activity_id().map(ActivityId::into_uuid),
            scope.team_id(),
        )?;
        if expected_revision < 0 {
            return Err(RuntimeError::InvalidInput);
        }
        check().await?;

        // Step 1 (short lock): validate the parked recovery scope and intent,
        // and re-prepare only a live confirm. Terminal intents go directly to
        // Operations reconciliation below. The guard is dropped before the
        // fence await — the Broker replay must never hold the catalog lock.
        let prepared = self
            .run_catalog_stage(move |catalog| {
                let row = catalog
                    .leadership_scope_view(scope, saga_now()?)?
                    .ok_or(OperationsError::Conflict("leadership scope missing"))?;
                if row.leadership_revision() != expected_revision
                    || row.state() != LeadershipState::RecoveryRequired
                {
                    return Err(RuntimeError::Conflict);
                }
                let intent = catalog
                    .leadership_confirm_intent(request_id)?
                    .ok_or(OperationsError::Conflict("leadership recovery intent"))?;
                if intent.scope() != scope {
                    return Err(RuntimeError::Conflict);
                }
                let prepared = match intent.state() {
                    LeadershipConfirmIntentState::Pending
                    | LeadershipConfirmIntentState::RemoteRecorded => {
                        match catalog.prepare_leadership_confirm(
                            PrepareLeadershipConfirmCommand {
                                scope,
                                election_id: intent.election_id(),
                                expected_revision: intent.expected_local_revision(),
                                request_id,
                                now_ms: saga_now()?,
                            },
                        )? {
                            PrepareLeadershipConfirmOutcome::Prepared(prepared) => Some(prepared),
                            PrepareLeadershipConfirmOutcome::Resolved(_) => None,
                        }
                    }
                    LeadershipConfirmIntentState::Committed
                    | LeadershipConfirmIntentState::Superseded
                    | LeadershipConfirmIntentState::Rejected => None,
                };
                Ok(prepared)
            })
            .await?;

        // Record the exact fence with no catalog lock held, then apply its
        // outcome through the same durable completion as ordinary confirms.
        let had_prepared = prepared.is_some();
        if let Some(prepared) = prepared {
            let outcome = fence.record(prepared.fence_request()).await;
            check().await?;
            self.run_catalog_stage(move |catalog| {
                heterarchy_orchestration_operations::complete_fenced_confirm(
                    catalog, &prepared, outcome,
                )
                .map_err(RuntimeError::from)
            })
            .await?;
        }

        check().await?;
        // Step 3 (short lock): durable readback gate, then reconcile to the
        // recorded outcome.
        self.run_catalog_stage(move |catalog| {
            if had_prepared {
                let snapshot = catalog
                    .leadership_confirmation_snapshot(request_id)?
                    .ok_or(OperationsError::Conflict("leadership recovery intent"))?;
                if snapshot.0.scope() != scope {
                    return Err(RuntimeError::Conflict);
                }
                if snapshot.1.scope() != scope {
                    return Err(RuntimeError::Conflict);
                }
                if snapshot.1.leadership_revision() != expected_revision {
                    return Ok(LeadershipRecoveryOutcome::Replayed {
                        intent: Box::new(snapshot.0),
                        readback: Box::new(snapshot.1),
                    });
                }
            }

            let resolution = catalog.reconcile_recovery(ReconcileRecoveryCommand {
                scope,
                intent_request_id: request_id,
                expected_revision,
                now_ms: saga_now()?,
            })?;
            Ok(LeadershipRecoveryOutcome::Reconciled(resolution))
        })
        .await
    }
}
