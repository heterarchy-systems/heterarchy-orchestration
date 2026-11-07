//! Durable Team capacity settings and Orchestrator-approved expansion slots.
//!
//! This module owns the Orchestration decision and its local capacity hold.
//! Profile provisioning remains a separate owner operation. A decision does
//! not itself create a Profile or assign a Team membership; a target-bound
//! slot must be consumed in the same Operations transaction as the join.

mod decision;
mod existing_home_transfer;
mod schema;
#[cfg(test)]
mod tests;
pub(crate) mod workspace_hold;

use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, TimestampMillis, WorkspaceId};
use rusqlite::{
    Connection, OptionalExtension, Transaction, TransactionBehavior, params, types::Value,
};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::{
    LeadershipScope, OperationsCatalog, OperationsError, TeamCreateCommand, TeamMutationRecord,
    WorkspaceParticipationId,
    leadership::current_leadership_authority_in_transaction,
    operational_actor::{OperationalActor, ResolvedOperationalActor, resolve_actor},
    teams, wire,
};

const GLOBAL_TEAM_MEMBER_CAP: u8 = 8;
const TEAM_EXPANSION_COOLDOWN_MS: i64 = 60 * 60 * 1_000;
const MAX_EXPANSION_REASON_BYTES: usize = 2_048;

/// A Team-specific registered-member cap, bounded by the user-confirmed hard
/// system cap. Zero is representable so the configured value is not silently
/// rewritten; a new Team's existing minimum initial roster makes zero
/// impossible at creation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TeamMemberCapacity(u8);

impl TeamMemberCapacity {
    /// Construct an explicit Team limit in the user-confirmed range `0..=8`.
    ///
    /// # Errors
    /// Rejects any value above the global Team hard cap.
    pub fn new(value: u8) -> Result<Self, OperationsError> {
        if value > GLOBAL_TEAM_MEMBER_CAP {
            return Err(OperationsError::Bounds("Team member capacity"));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

const TEAM_CREATE_ROSTER_DIGEST_DOMAIN: &[u8] = b"heterarchy.team-create-roster-source/v1\0";

/// One source selector and role used by the shared Team create roster digest.
/// The Agent ID is intentionally omitted: the source contract binds Activity,
/// Session, and exact role bytes.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamRosterDigestInput<'a> {
    activity_id: ActivityId,
    session_id: AgentSessionId,
    role: &'a str,
}

impl<'a> TeamRosterDigestInput<'a> {
    pub const fn new(activity_id: ActivityId, session_id: AgentSessionId, role: &'a str) -> Self {
        Self {
            activity_id,
            session_id,
            role,
        }
    }
}

/// Versioned SHA-256 identity for one canonical Team create roster.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TeamCreateRosterDigest([u8; 32]);

impl TeamCreateRosterDigest {
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }

    #[must_use]
    pub fn to_hex(self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(64);
        for byte in self.0 {
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        encoded
    }
}

/// Compute the canonical version-1 digest shared by Operations and Backend.
/// Roster order does not affect the digest; each exact source tuple is sorted
/// by Activity bytes, Session bytes, and role UTF-8 bytes before hashing.
///
/// # Errors
/// Rejects out-of-range rosters, invalid IDs or roles, and duplicate
/// Activity/Session selectors.
pub fn team_create_roster_digest(
    members: &[TeamRosterDigestInput<'_>],
) -> Result<TeamCreateRosterDigest, OperationsError> {
    if !(teams::MIN_FORMING_TEAM_MEMBERS..=teams::MAX_INITIAL_TEAM_MEMBERS).contains(&members.len())
    {
        return Err(OperationsError::Bounds("team initial roster"));
    }
    let mut canonical = members.to_vec();
    for member in &canonical {
        if member.activity_id.into_uuid().is_nil() || member.session_id.into_uuid().is_nil() {
            return Err(OperationsError::Conflict("team initial roster identity"));
        }
        teams::validate_team_role(member.role)?;
    }
    canonical.sort_unstable_by(|left, right| {
        left.activity_id
            .into_uuid()
            .as_bytes()
            .cmp(right.activity_id.into_uuid().as_bytes())
            .then_with(|| {
                left.session_id
                    .into_uuid()
                    .as_bytes()
                    .cmp(right.session_id.into_uuid().as_bytes())
            })
            .then_with(|| left.role.as_bytes().cmp(right.role.as_bytes()))
    });
    if canonical.windows(2).any(|pair| {
        pair[0].activity_id == pair[1].activity_id && pair[0].session_id == pair[1].session_id
    }) {
        return Err(OperationsError::Conflict("duplicate team roster actor"));
    }

    let mut digest = Sha256::new();
    digest.update(TEAM_CREATE_ROSTER_DIGEST_DOMAIN);
    digest.update(
        u32::try_from(canonical.len())
            .map_err(|_| OperationsError::Bounds("team roster digest count"))?
            .to_be_bytes(),
    );
    for member in canonical {
        let role = member.role.as_bytes();
        digest.update(member.activity_id.into_uuid().as_bytes());
        digest.update(member.session_id.into_uuid().as_bytes());
        digest.update(
            u32::try_from(role.len())
                .map_err(|_| OperationsError::Bounds("team roster digest role"))?
                .to_be_bytes(),
        );
        digest.update(role);
    }
    Ok(TeamCreateRosterDigest(digest.finalize().into()))
}

/// Explicit capacity paired with an existing Team creation intent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamCreateWithCapacityCommand {
    create: TeamCreateCommand,
    max_members: TeamMemberCapacity,
}

impl TeamCreateWithCapacityCommand {
    #[must_use]
    pub const fn new(create: TeamCreateCommand, max_members: TeamMemberCapacity) -> Self {
        Self {
            create,
            max_members,
        }
    }

    #[must_use]
    pub const fn create(&self) -> &TeamCreateCommand {
        &self.create
    }

    #[must_use]
    pub const fn max_members(&self) -> TeamMemberCapacity {
        self.max_members
    }
}

/// Current or legacy-unknown capacity observation for a Team.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamCapacitySnapshot {
    workspace_id: WorkspaceId,
    team_id: Uuid,
    max_members: TeamMemberCapacity,
    revision: u64,
    leadership_revision: i64,
    recorded_at: TimestampMillis,
    actor: TeamExpansionActor,
}

impl TeamCapacitySnapshot {
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn team_id(&self) -> Uuid {
        self.team_id
    }

    #[must_use]
    pub const fn max_members(&self) -> TeamMemberCapacity {
        self.max_members
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub const fn leadership_revision(&self) -> i64 {
        self.leadership_revision
    }

    #[must_use]
    pub const fn recorded_at(&self) -> TimestampMillis {
        self.recorded_at
    }

    #[must_use]
    pub const fn actor(&self) -> &TeamExpansionActor {
        &self.actor
    }
}

/// Public immutable actor attribution retained by Team capacity and expansion
/// receipts. It records an authenticated Profile Session, not a host-supplied
/// target identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamExpansionActor {
    activity_id: ActivityId,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    participation_id: WorkspaceParticipationId,
    profile_id: Uuid,
    participant_id: Uuid,
    display_name: String,
}

impl TeamExpansionActor {
    fn from_resolved(
        input: OperationalActor,
        actor: &ResolvedOperationalActor,
    ) -> Result<Self, OperationsError> {
        let OperationalActor::Participant {
            activity_id,
            agent_id,
            session_id,
        } = input
        else {
            return Err(OperationsError::Conflict(
                "Team expansion requires a Profile participant",
            ));
        };
        Ok(Self {
            activity_id,
            agent_id,
            session_id,
            participation_id: actor.participation_id().ok_or(OperationsError::Conflict(
                "Team expansion requires a Profile participation",
            ))?,
            profile_id: actor.profile_id().ok_or(OperationsError::Conflict(
                "Team expansion requires a Profile participant",
            ))?,
            participant_id: actor.participant_id().ok_or(OperationsError::Conflict(
                "Team expansion requires a Profile participant",
            ))?,
            display_name: actor.display_name().to_owned(),
        })
    }

    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity_id
    }
    #[must_use]
    pub const fn agent_id(&self) -> AgentInstanceId {
        self.agent_id
    }
    pub const fn session_id(&self) -> AgentSessionId {
        self.session_id
    }
    #[must_use]
    pub const fn participation_id(&self) -> WorkspaceParticipationId {
        self.participation_id
    }
    #[must_use]
    pub const fn profile_id(&self) -> Uuid {
        self.profile_id
    }
    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }
    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }
}

/// Exact request from the current Team Leader to add Team members.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamExpansionScope {
    workspace_id: WorkspaceId,
    team_id: Uuid,
}

impl TeamExpansionScope {
    pub const fn new(workspace_id: WorkspaceId, team_id: Uuid) -> Self {
        Self {
            workspace_id,
            team_id,
        }
    }

    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn team_id(self) -> Uuid {
        self.team_id
    }
}

/// Team, capacity, and caller-leadership revisions fenced by an Ops mutation.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamExpansionRevisions {
    team: u64,
    capacity: u64,
    leadership: i64,
}

impl TeamExpansionRevisions {
    pub const fn new(
        expected_team_revision: u64,
        expected_capacity_revision: u64,
        expected_leadership_revision: i64,
    ) -> Self {
        Self {
            team: expected_team_revision,
            capacity: expected_capacity_revision,
            leadership: expected_leadership_revision,
        }
    }

    #[must_use]
    pub const fn expected_team_revision(self) -> u64 {
        self.team
    }

    #[must_use]
    pub const fn expected_capacity_revision(self) -> u64 {
        self.capacity
    }

    #[must_use]
    pub const fn expected_leadership_revision(self) -> i64 {
        self.leadership
    }
}

/// Exact request from the current Team Leader to add Team members.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamExpansionRequestCommand {
    request_id: Uuid,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    expected_team_revision: u64,
    expected_capacity_revision: u64,
    expected_leadership_revision: i64,
    requested_count: u8,
    reason: String,
    actor: OperationalActor,
}

impl TeamExpansionRequestCommand {
    /// Construct a request; current Leader and capacity revisions are checked
    /// inside the owner's IMMEDIATE write transaction.
    #[must_use]
    pub const fn new(
        request_id: Uuid,
        scope: TeamExpansionScope,
        revisions: TeamExpansionRevisions,
        requested_count: u8,
        reason: String,
        actor: OperationalActor,
    ) -> Self {
        Self {
            request_id,
            workspace_id: scope.workspace_id,
            team_id: scope.team_id,
            expected_team_revision: revisions.team,
            expected_capacity_revision: revisions.capacity,
            expected_leadership_revision: revisions.leadership,
            requested_count,
            reason,
            actor,
        }
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }
}

/// One immutable Team Leader request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamExpansionRequestReceipt {
    request_id: Uuid,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    expected_team_revision: u64,
    capacity_revision: u64,
    requester_leadership_revision: i64,
    requested_count: u8,
    reason: String,
    actor: TeamExpansionActor,
    recorded_at: TimestampMillis,
    replayed: bool,
}

impl TeamExpansionRequestReceipt {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }
    #[must_use]
    pub const fn team_id(&self) -> Uuid {
        self.team_id
    }
    #[must_use]
    pub const fn expected_team_revision(&self) -> u64 {
        self.expected_team_revision
    }
    #[must_use]
    pub const fn capacity_revision(&self) -> u64 {
        self.capacity_revision
    }
    #[must_use]
    pub const fn requester_leadership_revision(&self) -> i64 {
        self.requester_leadership_revision
    }
    #[must_use]
    pub const fn requested_count(&self) -> u8 {
        self.requested_count
    }
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }
    #[must_use]
    pub const fn actor(&self) -> &TeamExpansionActor {
        &self.actor
    }
    #[must_use]
    pub const fn recorded_at(&self) -> TimestampMillis {
        self.recorded_at
    }
    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Orchestrator decision for one exact Team expansion request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamExpansionDecisionCommand {
    decision_id: Uuid,
    request_id: Uuid,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    expected_team_revision: u64,
    expected_capacity_revision: u64,
    expected_leadership_revision: i64,
    expected_requester_leadership_revision: i64,
    decision: TeamExpansionDecision,
    actor: OperationalActor,
}

impl TeamExpansionDecisionCommand {
    #[must_use]
    pub const fn new(
        decision_id: Uuid,
        request_id: Uuid,
        scope: TeamExpansionScope,
        revisions: TeamExpansionRevisions,
        expected_requester_leadership_revision: i64,
        decision: TeamExpansionDecision,
        actor: OperationalActor,
    ) -> Self {
        Self {
            decision_id,
            request_id,
            workspace_id: scope.workspace_id,
            team_id: scope.team_id,
            expected_team_revision: revisions.team,
            expected_capacity_revision: revisions.capacity,
            expected_leadership_revision: revisions.leadership,
            expected_requester_leadership_revision,
            decision,
            actor,
        }
    }
}

/// One Workspace-capacity target frozen for an approved Team slot.
/// `NewProfile` reserves capacity only and grants no downstream authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeamExpansionWorkspaceTarget {
    ExistingWorkspaceProfile {
        profile_id: Uuid,
        participation_id: WorkspaceParticipationId,
    },
    NewProfile,
}

/// Ordered target for one zero-based approval slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamExpansionApprovalSlotPlan {
    ordinal: u8,
    target: TeamExpansionWorkspaceTarget,
}

impl TeamExpansionApprovalSlotPlan {
    #[must_use]
    pub const fn new(ordinal: u8, target: TeamExpansionWorkspaceTarget) -> Self {
        Self { ordinal, target }
    }
}

/// V2 decision contract whose approval includes every Workspace hold target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamExpansionDecisionV2Command {
    decision: TeamExpansionDecisionCommand,
    slot_plan: Vec<TeamExpansionApprovalSlotPlan>,
}

/// One Workspace target frozen by the V3 approval contract.
///
/// `ExistingHomeTransfer` records only the current source membership observed
/// by the Workspace Orchestrator. It does not represent source-Leader consent,
/// Agent acceptance, work/lease handoff, or a completed Team transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeamExpansionWorkspaceTargetV3 {
    ExistingWorkspaceProfile {
        profile_id: Uuid,
        participation_id: WorkspaceParticipationId,
    },
    ExistingHomeTransfer {
        profile_id: Uuid,
        participation_id: WorkspaceParticipationId,
        source_team_id: Uuid,
        source_membership_revision: u64,
    },
    NewProfile,
}

/// Ordered V3 target for one zero-based approval slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamExpansionApprovalSlotPlanV3 {
    ordinal: u8,
    target: TeamExpansionWorkspaceTargetV3,
}

impl TeamExpansionApprovalSlotPlanV3 {
    #[must_use]
    pub const fn new(ordinal: u8, target: TeamExpansionWorkspaceTargetV3) -> Self {
        Self { ordinal, target }
    }

    #[must_use]
    pub const fn ordinal(self) -> u8 {
        self.ordinal
    }

    #[must_use]
    pub const fn target(self) -> TeamExpansionWorkspaceTargetV3 {
        self.target
    }
}

/// V3 decision contract with an explicit source-membership receipt for every
/// existing-Home transfer target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamExpansionDecisionV3Command {
    decision: TeamExpansionDecisionCommand,
    slot_plan: Vec<TeamExpansionApprovalSlotPlanV3>,
}

impl TeamExpansionDecisionV3Command {
    #[must_use]
    pub fn new(
        decision: TeamExpansionDecisionCommand,
        slot_plan: Vec<TeamExpansionApprovalSlotPlanV3>,
    ) -> Self {
        Self {
            decision,
            slot_plan,
        }
    }

    #[must_use]
    pub const fn decision(&self) -> &TeamExpansionDecisionCommand {
        &self.decision
    }

    #[must_use]
    pub fn slot_plan(&self) -> &[TeamExpansionApprovalSlotPlanV3] {
        &self.slot_plan
    }
}

/// Authorized read-only reconstruction of one immutable V3 approval source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamExpansionDecisionV3Projection {
    decision: TeamExpansionDecisionReceipt,
    slot_plan: Vec<TeamExpansionApprovalSlotPlanV3>,
}

impl TeamExpansionDecisionV3Projection {
    #[must_use]
    pub const fn decision(&self) -> &TeamExpansionDecisionReceipt {
        &self.decision
    }

    #[must_use]
    pub fn slot_plan(&self) -> &[TeamExpansionApprovalSlotPlanV3] {
        &self.slot_plan
    }

    #[must_use]
    pub const fn original_issuer(&self) -> &TeamExpansionActor {
        self.decision.actor()
    }
}

impl TeamExpansionDecisionV2Command {
    #[must_use]
    pub fn new(
        decision: TeamExpansionDecisionCommand,
        slot_plan: Vec<TeamExpansionApprovalSlotPlan>,
    ) -> Self {
        Self {
            decision,
            slot_plan,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LegacySlotRecoveryIntent {
    request_id: Uuid,
    slot: TeamExpansionSlotRef,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    expected_team_revision: u64,
    expected_capacity_revision: u64,
    target_profile_id: Uuid,
    target_participation_id: WorkspaceParticipationId,
}

impl LegacySlotRecoveryIntent {
    #[must_use]
    pub const fn new(
        request_id: Uuid,
        slot: TeamExpansionSlotRef,
        scope: TeamExpansionScope,
        revisions: TeamExpansionRevisions,
        target_profile_id: Uuid,
        target_participation_id: WorkspaceParticipationId,
    ) -> Self {
        Self::from_recovery_identity(
            request_id,
            slot,
            scope,
            revisions.team,
            revisions.capacity,
            target_profile_id,
            target_participation_id,
        )
    }

    /// Construct the exact persisted recovery identity without inventing an
    /// approval-time or current leadership revision.
    #[must_use]
    pub const fn from_recovery_identity(
        request_id: Uuid,
        slot: TeamExpansionSlotRef,
        scope: TeamExpansionScope,
        expected_team_revision: u64,
        expected_capacity_revision: u64,
        target_profile_id: Uuid,
        target_participation_id: WorkspaceParticipationId,
    ) -> Self {
        Self {
            request_id,
            slot,
            workspace_id: scope.workspace_id,
            team_id: scope.team_id,
            expected_team_revision,
            expected_capacity_revision,
            target_profile_id,
            target_participation_id,
        }
    }
    #[must_use]
    pub const fn request_id(self) -> Uuid {
        self.request_id
    }
    #[must_use]
    pub const fn slot(self) -> TeamExpansionSlotRef {
        self.slot
    }
    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        self.workspace_id
    }
    #[must_use]
    pub const fn team_id(self) -> Uuid {
        self.team_id
    }
    #[must_use]
    pub const fn expected_team_revision(self) -> u64 {
        self.expected_team_revision
    }
    #[must_use]
    pub const fn expected_capacity_revision(self) -> u64 {
        self.expected_capacity_revision
    }
    #[must_use]
    pub const fn target_profile_id(self) -> Uuid {
        self.target_profile_id
    }
    #[must_use]
    pub const fn target_participation_id(self) -> WorkspaceParticipationId {
        self.target_participation_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LegacySlotRecoveryCommand {
    intent: LegacySlotRecoveryIntent,
    expected_leadership_revision: i64,
    actor: OperationalActor,
}

impl LegacySlotRecoveryCommand {
    #[must_use]
    pub const fn new(
        intent: LegacySlotRecoveryIntent,
        expected_leadership_revision: i64,
        actor: OperationalActor,
    ) -> Self {
        Self {
            intent,
            expected_leadership_revision,
            actor,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacySlotRecoveryReceipt {
    intent: LegacySlotRecoveryIntent,
    approval_request_id: Uuid,
    leadership_revision: i64,
    actor: TeamExpansionActor,
    recorded_at: TimestampMillis,
    replayed: bool,
}

impl LegacySlotRecoveryReceipt {
    #[must_use]
    pub const fn intent(&self) -> LegacySlotRecoveryIntent {
        self.intent
    }
    #[must_use]
    pub const fn approval_request_id(&self) -> Uuid {
        self.approval_request_id
    }
    #[must_use]
    pub const fn leadership_revision(&self) -> i64 {
        self.leadership_revision
    }
    #[must_use]
    pub const fn actor(&self) -> &TeamExpansionActor {
        &self.actor
    }
    #[must_use]
    pub const fn recorded_at(&self) -> TimestampMillis {
        self.recorded_at
    }
    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// The only allowed decision result. Rejection carries no cooldown; approval
/// names the amount the Orchestrator can actually authorize now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeamExpansionDecision {
    Approve { approved_count: u8 },
    Reject,
}

/// Immutable decision and cooldown anchor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamExpansionDecisionReceipt {
    decision_id: Uuid,
    request_id: Uuid,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    state: TeamExpansionDecisionState,
    approved_count: u8,
    cooldown_started_at: Option<TimestampMillis>,
    cooldown_until: Option<TimestampMillis>,
    expected_team_revision: u64,
    capacity_revision: u64,
    leadership_revision: i64,
    actor: TeamExpansionActor,
    finalized_by: Option<TeamExpansionActor>,
    finalized_by_leadership_revision: Option<i64>,
    recorded_at: TimestampMillis,
    replayed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeamExpansionDecisionState {
    /// Approval and capacity holds are durable; the post-commit cooldown
    /// anchor is not yet confirmed, so new requests remain blocked.
    ApprovedPendingAnchor,
    /// Approval plus its separately recorded cooldown anchor are durable.
    Approved,
    Rejected,
}

impl TeamExpansionDecisionReceipt {
    #[must_use]
    pub const fn decision_id(&self) -> Uuid {
        self.decision_id
    }
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }
    #[must_use]
    pub const fn team_id(&self) -> Uuid {
        self.team_id
    }
    #[must_use]
    pub const fn state(&self) -> TeamExpansionDecisionState {
        self.state
    }
    #[must_use]
    pub const fn approved_count(&self) -> u8 {
        self.approved_count
    }
    #[must_use]
    pub const fn cooldown_started_at(&self) -> Option<TimestampMillis> {
        self.cooldown_started_at
    }
    #[must_use]
    pub const fn cooldown_until(&self) -> Option<TimestampMillis> {
        self.cooldown_until
    }
    #[must_use]
    pub const fn expected_team_revision(&self) -> u64 {
        self.expected_team_revision
    }
    #[must_use]
    pub const fn capacity_revision(&self) -> u64 {
        self.capacity_revision
    }
    #[must_use]
    pub const fn leadership_revision(&self) -> i64 {
        self.leadership_revision
    }
    #[must_use]
    pub const fn actor(&self) -> &TeamExpansionActor {
        &self.actor
    }
    #[must_use]
    pub const fn finalized_by(&self) -> Option<&TeamExpansionActor> {
        self.finalized_by.as_ref()
    }
    #[must_use]
    pub const fn finalized_by_leadership_revision(&self) -> Option<i64> {
        self.finalized_by_leadership_revision
    }
    #[must_use]
    pub const fn recorded_at(&self) -> TimestampMillis {
        self.recorded_at
    }
    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Exact O-owned slot selected for a single membership operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TeamExpansionSlotRef {
    approval_id: Uuid,
    ordinal: u8,
}

impl TeamExpansionSlotRef {
    #[must_use]
    pub const fn new(approval_id: Uuid, ordinal: u8) -> Self {
        Self {
            approval_id,
            ordinal,
        }
    }
    #[must_use]
    pub const fn approval_id(self) -> Uuid {
        self.approval_id
    }
    #[must_use]
    pub const fn ordinal(self) -> u8 {
        self.ordinal
    }
}

/// Original participant actor and approval slot recorded by a v2 slot-backed
/// JOIN request. Historical JOINs without source actor evidence remain absent.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamExpansionJoinSource {
    actor: OperationalActor,
    slot: TeamExpansionSlotRef,
    role: String,
}

impl TeamExpansionJoinSource {
    pub(crate) fn new(actor: OperationalActor, slot: TeamExpansionSlotRef, role: String) -> Self {
        Self { actor, slot, role }
    }

    #[must_use]
    pub const fn actor(&self) -> OperationalActor {
        self.actor
    }

    #[must_use]
    pub const fn slot(&self) -> TeamExpansionSlotRef {
        self.slot
    }

    #[must_use]
    pub fn role(&self) -> &str {
        &self.role
    }
}

/// Source evidence returned only by the owner-scoped Team mutation readback.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TeamMutationSourceEvidence {
    CappedCreate {
        capacity: TeamCapacitySnapshot,
        roster_digest: TeamCreateRosterDigest,
    },
    SlotBackedJoin(TeamExpansionJoinSource),
}

/// One Team mutation and its optional owner-scoped source proof.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamMutationOwnerReadback {
    mutation: TeamMutationRecord,
    source: Option<TeamMutationSourceEvidence>,
}

impl TeamMutationOwnerReadback {
    pub(crate) fn new(
        mutation: TeamMutationRecord,
        source: Option<TeamMutationSourceEvidence>,
    ) -> Self {
        Self { mutation, source }
    }

    #[must_use]
    pub const fn mutation(&self) -> &TeamMutationRecord {
        &self.mutation
    }

    #[must_use]
    pub const fn source(&self) -> Option<&TeamMutationSourceEvidence> {
        self.source.as_ref()
    }
}

/// Workspace Orchestrator assigns one approved capacity slot to a Ready
/// Workspace Profile. Provisioning effects remain owned by the existing
/// provisioning subsystem and use this exact approval identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamExpansionSlotAssignmentCommand {
    request_id: Uuid,
    slot: TeamExpansionSlotRef,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    expected_team_revision: u64,
    expected_capacity_revision: u64,
    expected_leadership_revision: i64,
    target: OperationalActor,
    actor: OperationalActor,
}

impl TeamExpansionSlotAssignmentCommand {
    #[must_use]
    pub const fn new(
        request_id: Uuid,
        slot: TeamExpansionSlotRef,
        scope: TeamExpansionScope,
        revisions: TeamExpansionRevisions,
        target: OperationalActor,
        actor: OperationalActor,
    ) -> Self {
        Self {
            request_id,
            slot,
            workspace_id: scope.workspace_id,
            team_id: scope.team_id,
            expected_team_revision: revisions.team,
            expected_capacity_revision: revisions.capacity,
            expected_leadership_revision: revisions.leadership,
            target,
            actor,
        }
    }
}

/// Selected target bound to one unconsumed approved slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamExpansionSlotAssignmentReceipt {
    request_id: Uuid,
    slot: TeamExpansionSlotRef,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    participation_id: WorkspaceParticipationId,
    profile_id: Uuid,
    participant_id: Uuid,
    expected_team_revision: u64,
    capacity_revision: u64,
    leadership_revision: i64,
    target: TeamExpansionActor,
    actor: TeamExpansionActor,
    recorded_at: TimestampMillis,
    replayed: bool,
}

impl TeamExpansionSlotAssignmentReceipt {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }
    #[must_use]
    pub const fn slot(&self) -> TeamExpansionSlotRef {
        self.slot
    }
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }
    #[must_use]
    pub const fn team_id(&self) -> Uuid {
        self.team_id
    }
    #[must_use]
    pub const fn participation_id(&self) -> WorkspaceParticipationId {
        self.participation_id
    }
    #[must_use]
    pub const fn profile_id(&self) -> Uuid {
        self.profile_id
    }
    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }
    #[must_use]
    pub const fn expected_team_revision(&self) -> u64 {
        self.expected_team_revision
    }
    #[must_use]
    pub const fn capacity_revision(&self) -> u64 {
        self.capacity_revision
    }
    #[must_use]
    pub const fn leadership_revision(&self) -> i64 {
        self.leadership_revision
    }
    #[must_use]
    pub const fn target(&self) -> &TeamExpansionActor {
        &self.target
    }
    #[must_use]
    pub const fn actor(&self) -> &TeamExpansionActor {
        &self.actor
    }
    #[must_use]
    pub const fn recorded_at(&self) -> TimestampMillis {
        self.recorded_at
    }
    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamCapacitySetCommand {
    request_id: Uuid,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    expected_team_revision: u64,
    expected_capacity_revision: u64,
    expected_leadership_revision: i64,
    capacity: TeamMemberCapacity,
    actor: OperationalActor,
}

impl TeamCapacitySetCommand {
    #[must_use]
    pub const fn new(
        request_id: Uuid,
        scope: TeamExpansionScope,
        revisions: TeamExpansionRevisions,
        capacity: TeamMemberCapacity,
        actor: OperationalActor,
    ) -> Self {
        Self {
            request_id,
            workspace_id: scope.workspace_id,
            team_id: scope.team_id,
            expected_team_revision: revisions.team,
            expected_capacity_revision: revisions.capacity,
            expected_leadership_revision: revisions.leadership,
            capacity,
            actor,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamCapacityReceipt {
    request_id: Uuid,
    snapshot: TeamCapacitySnapshot,
    expected_team_revision: u64,
    expected_capacity_revision: u64,
    result_team_revision: u64,
    leadership_revision: i64,
    actor: TeamExpansionActor,
    replayed: bool,
}

impl TeamCapacityReceipt {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }
    #[must_use]
    pub const fn snapshot(&self) -> &TeamCapacitySnapshot {
        &self.snapshot
    }
    #[must_use]
    pub const fn expected_team_revision(&self) -> u64 {
        self.expected_team_revision
    }
    #[must_use]
    pub const fn expected_capacity_revision(&self) -> u64 {
        self.expected_capacity_revision
    }
    #[must_use]
    pub const fn result_team_revision(&self) -> u64 {
        self.result_team_revision
    }
    #[must_use]
    pub const fn leadership_revision(&self) -> i64 {
        self.leadership_revision
    }
    #[must_use]
    pub const fn actor(&self) -> &TeamExpansionActor {
        &self.actor
    }
    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

impl OperationsCatalog {
    /// Create a Team with an Orchestrator-supplied Team-specific capacity in
    /// the same Operations transaction as the Team and initial roster.
    ///
    /// # Errors
    /// Rejects capless new creates, an unauthorised creator, roster larger than
    /// the configured cap, stale Workspace scope and capacity overflow.
    pub fn create_team_with_capacity(
        &mut self,
        create: TeamCreateCommand,
        max_members: TeamMemberCapacity,
    ) -> Result<TeamMutationRecord, OperationsError> {
        teams::create_team_with_capacity(&mut self.connection, create, max_members)
    }

    /// Read the explicit Team-specific cap. `None` means an old Team has no
    /// capacity receipt yet; no hard-cap default is inferred for that Team.
    ///
    /// # Errors
    /// Rejects corrupt capacity history or database failures.
    pub fn team_capacity(
        &self,
        workspace_id: WorkspaceId,
        team_id: Uuid,
    ) -> Result<Option<TeamCapacitySnapshot>, OperationsError> {
        read_capacity_in_transaction(&self.connection, workspace_id, team_id)
    }

    /// Read the original cap-setting receipt, not the Team's latest cap.
    /// Current Orchestrator or the same stable Profile participation may read it.
    ///
    /// # Errors
    /// Returns `None` when the request is absent or outside the caller's scope;
    /// reports corrupt durable history to an authorized reader.
    pub fn team_capacity_set_receipt(
        &self,
        workspace_id: WorkspaceId,
        request_id: Uuid,
        actor: OperationalActor,
    ) -> Result<Option<TeamCapacityReceipt>, OperationsError> {
        read_capacity_receipt(&self.connection, workspace_id, request_id, actor)
    }

    /// Change one Team's explicit registered-member maximum. The approval
    /// cooldown and expansion grants are independent records.
    ///
    /// # Errors
    /// Rejects a non-Orchestrator, stale Team/capacity/leadership revisions,
    /// a new cap below committed or held occupancy, and an unconfirmed write.
    pub fn set_team_capacity(
        &mut self,
        command: &TeamCapacitySetCommand,
        trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
    ) -> Result<TeamCapacityReceipt, OperationsError> {
        set_team_capacity(&mut self.connection, command, trusted_now)
    }

    /// Submit a new member request from the exact current Team Leader.
    ///
    /// # Errors
    /// Rejects a stale Leader/Team/capacity, missing legacy Team cap, active
    /// Team cooldown, invalid request and uncertain commit.
    pub fn request_team_expansion(
        &mut self,
        command: &TeamExpansionRequestCommand,
        trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
    ) -> Result<TeamExpansionRequestReceipt, OperationsError> {
        request_team_expansion(&mut self.connection, command, trusted_now)
    }

    /// Read an exact request as its original Leader or the current
    /// Workspace Orchestrator.
    ///
    /// # Errors
    /// Returns `None` when the request is absent or outside the caller's scope;
    /// reports corrupt durable history to an authorized reader.
    pub fn team_expansion_request(
        &self,
        workspace_id: WorkspaceId,
        request_id: Uuid,
        actor: OperationalActor,
    ) -> Result<Option<TeamExpansionRequestReceipt>, OperationsError> {
        read_team_expansion_request(&self.connection, workspace_id, request_id, actor)
    }

    /// Commit one Orchestrator decision. Approved slots are held immediately;
    /// the operation records `APPROVED_PENDING_ANCHOR` before it confirms the
    /// cooldown in a later durable transaction.
    ///
    /// # Errors
    /// Rejects a non-Orchestrator, stale revisions, over-capacity approval,
    /// duplicate/changed decisions and unconfirmed commit outcomes.
    pub fn decide_team_expansion(
        &mut self,
        command: &TeamExpansionDecisionCommand,
        trusted_now: impl FnMut() -> Result<TimestampMillis, OperationsError>,
    ) -> Result<TeamExpansionDecisionReceipt, OperationsError> {
        decision::decide_team_expansion(&mut self.connection, command, trusted_now)
    }

    /// Commit a V2 decision and its complete Workspace hold plan atomically.
    ///
    /// # Errors
    /// Rejects stale authority/revisions, incomplete or conflicting plans,
    /// capacity overflow, replay drift, and unconfirmed commits.
    pub fn decide_team_expansion_v2(
        &mut self,
        command: &TeamExpansionDecisionV2Command,
        trusted_now: impl FnMut() -> Result<TimestampMillis, OperationsError>,
    ) -> Result<TeamExpansionDecisionReceipt, OperationsError> {
        decision::decide_team_expansion_v2(&mut self.connection, command, trusted_now)
    }

    /// Commit a V3 approval with exact existing-Home source membership intents.
    ///
    /// This records a destination hold and immutable transfer intent only. It
    /// does not change the source membership or authorize assignment/JOIN.
    ///
    /// # Errors
    /// Rejects stale authority/revisions, a nonexact or multiply-homed source,
    /// duplicate active holds, replay drift, capacity overflow, and unknown
    /// commit outcomes.
    pub fn decide_team_expansion_v3(
        &mut self,
        command: &TeamExpansionDecisionV3Command,
        trusted_now: impl FnMut() -> Result<TimestampMillis, OperationsError>,
    ) -> Result<TeamExpansionDecisionReceipt, OperationsError> {
        decision::decide_team_expansion_v3(&mut self.connection, command, trusted_now)
    }

    /// Complete the cooldown anchor for a committed pending approval using a
    /// currently verified Orchestrator. This never changes its approved count.
    ///
    /// # Errors
    /// Rejects a foreign Workspace, invalid/missing pending approval, stale
    /// Orchestrator authority, regressed time and unconfirmed anchor commit.
    pub fn finalize_team_expansion_cooldown(
        &mut self,
        workspace_id: WorkspaceId,
        request_id: Uuid,
        decision_id: Uuid,
        expected_leadership_revision: i64,
        actor: OperationalActor,
        trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
    ) -> Result<TeamExpansionDecisionReceipt, OperationsError> {
        decision::finalize_team_expansion_cooldown(
            &mut self.connection,
            workspace_id,
            request_id,
            decision_id,
            expected_leadership_revision,
            actor,
            trusted_now,
        )
    }

    /// Read the original decision, including its exact pending-anchor or
    /// anchored outcome, as the requester or current Workspace Orchestrator.
    ///
    /// # Errors
    /// Rejects a foreign Workspace or corrupt decision/anchor history.
    pub fn team_expansion_decision(
        &self,
        workspace_id: WorkspaceId,
        decision_id: Uuid,
        actor: OperationalActor,
    ) -> Result<Option<TeamExpansionDecisionReceipt>, OperationsError> {
        decision::read_team_expansion_decision(&self.connection, workspace_id, decision_id, actor)
    }

    /// Read and validate one exact V2 decision intent, including its ordered
    /// Workspace hold plan. This is the recovery boundary after a lost result.
    ///
    /// # Errors
    /// Rejects changed authorized intent, corrupt source history, or invalid
    /// reader identity. Unauthorized readers receive `None`.
    pub fn team_expansion_decision_v2_readback(
        &self,
        intent: &TeamExpansionDecisionV2Command,
        reader: OperationalActor,
    ) -> Result<Option<TeamExpansionDecisionReceipt>, OperationsError> {
        decision::read_team_expansion_decision_v2(&self.connection, intent, reader)
    }

    /// Read one exact V3 decision and its original source-membership intents.
    ///
    /// # Errors
    /// Rejects changed authorized intent or corrupt source history.
    /// Unauthorized readers receive `None`.
    pub fn team_expansion_decision_v3_readback(
        &self,
        intent: &TeamExpansionDecisionV3Command,
        reader: OperationalActor,
    ) -> Result<Option<TeamExpansionDecisionReceipt>, OperationsError> {
        decision::read_team_expansion_decision_v3(&self.connection, intent, reader)
    }

    /// Reconstruct one original V3 decision source without requiring the
    /// caller to already possess its ordered plan.
    ///
    /// # Errors
    /// Authorized readers receive corruption errors for inconsistent durable
    /// source. Unauthorized, absent, cross-Workspace, legacy, and V2 records
    /// all project as `None`.
    pub fn team_expansion_decision_v3_projection(
        &self,
        workspace_id: WorkspaceId,
        decision_id: Uuid,
        reader: OperationalActor,
    ) -> Result<Option<TeamExpansionDecisionV3Projection>, OperationsError> {
        decision::read_team_expansion_decision_v3_projection(
            &self.connection,
            workspace_id,
            decision_id,
            reader,
        )
    }

    /// Reserve one current Workspace target for an unused `PRE_HOLD` approval slot.
    ///
    /// # Errors
    /// Rejects non-Orchestrators, nonlegacy/used slots, stale revisions,
    /// ineligible targets, capacity conflicts, replay drift, and unknown commits.
    pub fn recover_legacy_team_expansion_slot(
        &mut self,
        command: &LegacySlotRecoveryCommand,
        trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
    ) -> Result<LegacySlotRecoveryReceipt, OperationsError> {
        workspace_hold::recover_legacy_slot(&mut self.connection, command, trusted_now)
    }

    /// Read one exact legacy-slot recovery intent as its original issuer or the
    /// current Workspace Orchestrator.
    ///
    /// # Errors
    /// Rejects changed authorized intent and corrupt source history.
    pub fn legacy_team_expansion_slot_recovery(
        &self,
        intent: &LegacySlotRecoveryIntent,
        reader: OperationalActor,
    ) -> Result<Option<LegacySlotRecoveryReceipt>, OperationsError> {
        workspace_hold::read_legacy_slot_recovery(&self.connection, intent, reader)
    }

    /// Bind one approved held slot to an exact Ready Profile selected by the
    /// current Workspace Orchestrator. This creates no registration or
    /// membership itself.
    ///
    /// # Errors
    /// Rejects a non-Orchestrator, stale revisions, unknown/used slot, foreign
    /// or already-home Profile, and unconfirmed commit outcomes.
    pub fn assign_team_expansion_slot(
        &mut self,
        command: &TeamExpansionSlotAssignmentCommand,
        trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
    ) -> Result<TeamExpansionSlotAssignmentReceipt, OperationsError> {
        assign_team_expansion_slot(&mut self.connection, command, trusted_now)
    }

    /// Read one exact assigned slot as its selected Profile or the current
    /// Workspace Orchestrator.
    ///
    /// # Errors
    /// Returns `None` when the slot is absent or outside the caller's scope;
    /// reports corrupt durable history to an authorized reader.
    pub fn team_expansion_slot_assignment(
        &self,
        workspace_id: WorkspaceId,
        slot: TeamExpansionSlotRef,
        actor: OperationalActor,
    ) -> Result<Option<TeamExpansionSlotAssignmentReceipt>, OperationsError> {
        read_expansion_slot_assignment(&self.connection, workspace_id, slot, actor)
    }
}

/// Return exact schema objects for Main's schema-58 additive migration.
pub(crate) const fn schema_objects() -> &'static [(&'static str, &'static str, &'static str)] {
    schema::schema_objects()
}

pub(crate) fn create_schema(transaction: &Transaction<'_>) -> Result<(), OperationsError> {
    schema::create_schema(transaction)
}

pub(crate) fn create_workspace_hold_schema(
    transaction: &Transaction<'_>,
) -> Result<(), OperationsError> {
    workspace_hold::create_workspace_hold_schema(transaction)
}

pub(crate) const fn workspace_hold_schema_objects()
-> &'static [(&'static str, &'static str, &'static str)] {
    workspace_hold::workspace_hold_schema_objects()
}

pub(crate) fn classify_pre_hold_decisions(
    transaction: &Transaction<'_>,
) -> Result<(), OperationsError> {
    transaction.execute(workspace_hold::PRE_HOLD_DECISION_BACKFILL_SQL, [])?;
    Ok(())
}

pub(crate) fn create_legacy_slot_recovery_schema(
    transaction: &Transaction<'_>,
) -> Result<(), OperationsError> {
    workspace_hold::create_recovery_schema(transaction)
}

pub(crate) const fn legacy_slot_recovery_schema_objects()
-> &'static [(&'static str, &'static str, &'static str)] {
    workspace_hold::recovery_schema_objects()
}

pub(crate) fn create_existing_home_transfer_schema(
    transaction: &Transaction<'_>,
) -> Result<(), OperationsError> {
    existing_home_transfer::create_schema(transaction)
}

pub(crate) const fn existing_home_transfer_schema_objects()
-> &'static [(&'static str, &'static str, &'static str)] {
    existing_home_transfer::schema_objects()
}

pub(crate) fn record_team_created_capacity(
    transaction: &Transaction<'_>,
    command: &TeamCreateCommand,
    capacity: TeamMemberCapacity,
    resolved_actor: &ResolvedOperationalActor,
    recorded_at: TimestampMillis,
) -> Result<TeamCapacitySnapshot, OperationsError> {
    let actor = TeamExpansionActor::from_resolved(command.actor, resolved_actor)?;
    let authority = current_leadership_authority_in_transaction(
        transaction,
        LeadershipScope::workspace(command.workspace_id),
    )?
    .ok_or(OperationsError::Conflict(
        "Team capacity creation requires current Workspace Orchestrator",
    ))?;
    if actor.participant_id != authority.participant_id()
        || command.expected_sender_leadership_revision != Some(authority.leadership_revision())
    {
        return Err(OperationsError::Conflict(
            "Team capacity creation authority changed",
        ));
    }
    let leadership_revision =
        command
            .expected_sender_leadership_revision
            .ok_or(OperationsError::Conflict(
                "Team capacity creation requires Workspace Orchestrator revision",
            ))?;
    write_capacity_snapshot(
        transaction,
        CapacitySnapshotWrite {
            request_id: command.request_id,
            workspace_id: command.workspace_id,
            team_id: command.team_id,
            expected_capacity_revision: 0,
            capacity_revision: 1,
            expected_team_revision: 0,
            result_team_revision: 1,
            leadership_revision,
            capacity,
            actor: &actor,
            recorded_at,
            operation: "CREATE",
        },
    )
}

pub(crate) fn capacity_receipt_snapshot_for_create(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    request_id: Uuid,
) -> Result<Option<TeamCapacitySnapshot>, OperationsError> {
    let Some(raw) = load_capacity_receipt_raw(connection, workspace_id, request_id)? else {
        if capacity_request_id_exists(connection, request_id)? {
            return Err(OperationsError::Corrupt(
                "Team create capacity request Workspace scope",
            ));
        }
        return Ok(None);
    };
    if raw.operation != "CREATE" {
        return Err(OperationsError::Corrupt("Team create capacity operation"));
    }
    let stored_team_id = Uuid::from_bytes(wire::id(raw.team_id.clone())?);
    if stored_team_id != team_id
        || raw.expected_capacity_revision != 0
        || raw.expected_team_revision != 0
        || raw.result_team_revision != 1
    {
        return Err(OperationsError::Corrupt("Team create capacity receipt"));
    }
    let receipt = raw.into_receipt(workspace_id, team_id, true)?;
    if receipt.snapshot.revision != 1 {
        return Err(OperationsError::Corrupt("Team create capacity generation"));
    }
    Ok(Some(receipt.snapshot))
}

fn request_team_expansion(
    connection: &mut Connection,
    command: &TeamExpansionRequestCommand,
    trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
) -> Result<TeamExpansionRequestReceipt, OperationsError> {
    validate_team_expansion_request_intent(command)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    teams::ensure_verified_workspace(&transaction, command.workspace_id)?;
    let (actor, _) = validate_actor(&transaction, command.workspace_id, command.actor)?;

    if let Some(raw) =
        load_team_expansion_request_raw(&transaction, command.workspace_id, command.request_id)?
    {
        let existing = raw.into_receipt(true)?;
        if request_matches_command(&existing, command, &actor) {
            transaction.commit()?;
            return Ok(existing);
        }
        return Err(OperationsError::Conflict("Team expansion request replay"));
    }
    if team_expansion_request_id_exists(&transaction, command.request_id)? {
        return Err(OperationsError::Conflict(
            "Team expansion request Workspace scope",
        ));
    }
    validate_team_expansion_request_authority(&transaction, command, &actor)?;

    let recorded_at = trusted_now()?;
    ensure_source_time_not_regressed(
        &transaction,
        command.workspace_id,
        command.team_id,
        recorded_at,
    )?;
    ensure_expansion_request_unlocked(
        &transaction,
        command.workspace_id,
        command.team_id,
        recorded_at,
    )?;
    let receipt = TeamExpansionRequestReceipt {
        request_id: command.request_id,
        workspace_id: command.workspace_id,
        team_id: command.team_id,
        expected_team_revision: command.expected_team_revision,
        capacity_revision: command.expected_capacity_revision,
        requester_leadership_revision: command.expected_leadership_revision,
        requested_count: command.requested_count,
        reason: command.reason.clone(),
        actor: actor.clone(),
        recorded_at,
        replayed: false,
    };
    insert_team_expansion_request(&transaction, &receipt)?;
    let commit_result = transaction.commit();
    match commit_result {
        Ok(()) => Ok(receipt),
        Err(source) => match load_team_expansion_request_raw(
            connection,
            command.workspace_id,
            command.request_id,
        ) {
            Ok(Some(raw)) => {
                let existing = raw.into_receipt(true)?;
                if request_matches_command(&existing, command, &actor) {
                    Ok(existing)
                } else {
                    Err(OperationsError::Conflict(
                        "Team expansion request replay after unconfirmed commit",
                    ))
                }
            }
            Ok(None) | Err(_) => Err(OperationsError::OperationCommitUnconfirmed {
                workspace_id: command.workspace_id,
                request_id: command.request_id,
                source,
            }),
        },
    }
}

fn validate_team_expansion_request_intent(
    command: &TeamExpansionRequestCommand,
) -> Result<(), OperationsError> {
    validate_non_nil(command.request_id, "Team expansion request identity")?;
    validate_non_nil(command.team_id, "Team expansion Team identity")?;
    if command.expected_team_revision == 0
        || command.expected_capacity_revision == 0
        || command.expected_leadership_revision < 1
        || command.requested_count == 0
        || command.requested_count > GLOBAL_TEAM_MEMBER_CAP
        || i64::try_from(command.expected_team_revision).is_err()
        || i64::try_from(command.expected_capacity_revision).is_err()
        || command.reason.trim().is_empty()
        || command.reason.len() > MAX_EXPANSION_REASON_BYTES
    {
        return Err(OperationsError::Bounds("Team expansion request"));
    }
    Ok(())
}

fn validate_team_expansion_request_authority(
    transaction: &Transaction<'_>,
    command: &TeamExpansionRequestCommand,
    actor: &TeamExpansionActor,
) -> Result<(), OperationsError> {
    let team_authority = current_leadership_authority_in_transaction(
        transaction,
        LeadershipScope::team(command.workspace_id, command.team_id),
    )?
    .ok_or(OperationsError::Conflict(
        "Team expansion request requires current Team Leader",
    ))?;
    if team_authority.participant_id() != actor.participant_id
        || team_authority.leadership_revision() != command.expected_leadership_revision
    {
        return Err(OperationsError::Conflict(
            "Team expansion request Leader changed",
        ));
    }
    let (team_revision, active) =
        team_revision_and_active(transaction, command.workspace_id, command.team_id)?;
    if !active || team_revision != command.expected_team_revision {
        return Err(OperationsError::Conflict(
            "Team expansion request Team revision",
        ));
    }
    let capacity = load_current_capacity(transaction, command.workspace_id, command.team_id)?
        .ok_or(OperationsError::Conflict("Team capacity is not configured"))?;
    if capacity.revision != command.expected_capacity_revision {
        return Err(OperationsError::Conflict(
            "Team expansion request capacity revision",
        ));
    }
    Ok(())
}

fn read_team_expansion_request(
    connection: &Connection,
    workspace_id: WorkspaceId,
    request_id: Uuid,
    actor_input: OperationalActor,
) -> Result<Option<TeamExpansionRequestReceipt>, OperationsError> {
    validate_non_nil(request_id, "Team expansion request identity")?;
    let transaction = connection.unchecked_transaction()?;
    teams::ensure_verified_workspace(&transaction, workspace_id)?;
    let (reader, _) = validate_actor(&transaction, workspace_id, actor_input)?;
    let is_current_orchestrator = current_leadership_authority_in_transaction(
        &transaction,
        LeadershipScope::workspace(workspace_id),
    )?
    .is_some_and(|authority| authority.participant_id() == reader.participant_id);
    let authorized = is_current_orchestrator
        || request_actor_matches_reader(&transaction, workspace_id, request_id, &reader)?;
    if !authorized {
        transaction.commit()?;
        return Ok(None);
    }
    let Some(raw) = load_team_expansion_request_raw(&transaction, workspace_id, request_id)? else {
        transaction.commit()?;
        return Ok(None);
    };
    let receipt = raw.into_receipt(false)?;
    transaction.commit()?;
    Ok(Some(receipt))
}

fn request_actor_matches_reader(
    connection: &Connection,
    workspace_id: WorkspaceId,
    request_id: Uuid,
    reader: &TeamExpansionActor,
) -> Result<bool, OperationsError> {
    let actor: Option<(Value, Value)> = connection
        .query_row(
            "SELECT actor_participation_id,actor_profile_id FROM ops_team_expansion_requests WHERE workspace_id=?1 AND request_id=?2",
            params![workspace_id.into_uuid().as_bytes(), request_id.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    Ok(actor.is_some_and(|(participation, profile)| {
        stored_profile_matches_reader(participation, profile, reader)
    }))
}

fn assign_team_expansion_slot(
    connection: &mut Connection,
    command: &TeamExpansionSlotAssignmentCommand,
    trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
) -> Result<TeamExpansionSlotAssignmentReceipt, OperationsError> {
    validate_slot_assignment_intent(command)?;

    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    teams::ensure_verified_workspace(&transaction, command.workspace_id)?;
    let (actor, _) = validate_actor(&transaction, command.workspace_id, command.actor)?;
    let (target, _) = validate_actor(&transaction, command.workspace_id, command.target)?;

    if let Some(raw) =
        load_slot_assignment_by_request(&transaction, command.workspace_id, command.request_id)?
    {
        let existing = raw.into_receipt(true)?;
        validate_slot_assignment_history(&transaction, &existing)?;
        if slot_assignment_matches_command(&existing, command, &actor, &target) {
            transaction.commit()?;
            return Ok(existing);
        }
        return Err(OperationsError::Conflict(
            "Team expansion slot assignment replay",
        ));
    }
    if slot_assignment_request_id_exists(&transaction, command.request_id)? {
        return Err(OperationsError::Conflict(
            "Team expansion slot assignment Workspace scope",
        ));
    }

    validate_new_slot_assignment(&transaction, command, &actor, &target)?;

    let recorded_at = trusted_now()?;
    ensure_source_time_not_regressed(
        &transaction,
        command.workspace_id,
        command.team_id,
        recorded_at,
    )?;
    let receipt = record_slot_assignment(&transaction, command, &actor, &target, recorded_at)?;
    let commit_result = transaction.commit();
    finish_slot_assignment_commit(commit_result, connection, command, &actor, &target, receipt)
}

fn validate_slot_assignment_intent(
    command: &TeamExpansionSlotAssignmentCommand,
) -> Result<(), OperationsError> {
    validate_non_nil(
        command.request_id,
        "Team expansion slot assignment identity",
    )?;
    validate_non_nil(command.team_id, "Team expansion Team identity")?;
    validate_non_nil(command.slot.approval_id, "Team expansion approval identity")?;
    if command.slot.ordinal > 7
        || command.expected_team_revision == 0
        || command.expected_capacity_revision == 0
        || command.expected_leadership_revision < 1
        || i64::try_from(command.expected_team_revision).is_err()
        || i64::try_from(command.expected_capacity_revision).is_err()
    {
        return Err(OperationsError::Bounds("Team expansion slot assignment"));
    }
    Ok(())
}

fn validate_new_slot_assignment(
    transaction: &Transaction<'_>,
    command: &TeamExpansionSlotAssignmentCommand,
    actor: &TeamExpansionActor,
    target: &TeamExpansionActor,
) -> Result<(), OperationsError> {
    let authority = current_leadership_authority_in_transaction(
        transaction,
        LeadershipScope::workspace(command.workspace_id),
    )?
    .ok_or(OperationsError::Conflict(
        "Team expansion slot assignment requires current Workspace Orchestrator",
    ))?;
    if authority.participant_id() != actor.participant_id
        || authority.leadership_revision() != command.expected_leadership_revision
    {
        return Err(OperationsError::Conflict(
            "Team expansion slot assignment Workspace authority changed",
        ));
    }
    let (team_revision, active) =
        team_revision_and_active(transaction, command.workspace_id, command.team_id)?;
    if !active || team_revision != command.expected_team_revision {
        return Err(OperationsError::Conflict(
            "Team expansion slot assignment Team revision",
        ));
    }
    let capacity = load_current_capacity(transaction, command.workspace_id, command.team_id)?
        .ok_or(OperationsError::Conflict("Team capacity is not configured"))?;
    if capacity.revision != command.expected_capacity_revision {
        return Err(OperationsError::Conflict(
            "Team expansion slot assignment capacity revision",
        ));
    }
    let _request_id = ensure_approved_slot_available(
        transaction,
        command.workspace_id,
        command.team_id,
        command.slot,
    )?;
    existing_home_transfer::ensure_generic_slot_admission(
        transaction,
        command.workspace_id,
        command.team_id,
        command.slot,
    )?;
    workspace_hold::validate_slot_assignment_target(
        transaction,
        command.workspace_id,
        command.slot,
        target.profile_id,
        target.participation_id.into_uuid(),
    )?;
    ensure_slot_target_eligible(transaction, command.workspace_id, command.team_id, target)
}

fn record_slot_assignment(
    transaction: &Transaction<'_>,
    command: &TeamExpansionSlotAssignmentCommand,
    actor: &TeamExpansionActor,
    target: &TeamExpansionActor,
    recorded_at: TimestampMillis,
) -> Result<TeamExpansionSlotAssignmentReceipt, OperationsError> {
    transaction.execute(
        "INSERT INTO ops_team_expansion_slot_assignments(request_id,decision_id,ordinal,workspace_id,team_id,target_participation_id,target_profile_id,target_participant_id,target_activity_id,target_agent_id,target_session_id,target_display_name,actor_participation_id,actor_profile_id,actor_participant_id,actor_activity_id,actor_agent_id,actor_session_id,actor_display_name,expected_team_revision,capacity_revision,leadership_revision,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23)",
        params![
            command.request_id.as_bytes(),
            command.slot.approval_id.as_bytes(),
            i64::from(command.slot.ordinal),
            command.workspace_id.into_uuid().as_bytes(),
            command.team_id.as_bytes(),
            target.participation_id.into_uuid().as_bytes(),
            target.profile_id.as_bytes(),
            target.participant_id.as_bytes(),
            target.activity_id.into_uuid().as_bytes(),
            target.agent_id.into_uuid().as_bytes(),
            target.session_id.into_uuid().as_bytes(),
            target.display_name,
            actor.participation_id.into_uuid().as_bytes(),
            actor.profile_id.as_bytes(),
            actor.participant_id.as_bytes(),
            actor.activity_id.into_uuid().as_bytes(),
            actor.agent_id.into_uuid().as_bytes(),
            actor.session_id.into_uuid().as_bytes(),
            actor.display_name,
            i64::try_from(command.expected_team_revision)
                .map_err(|_| OperationsError::Bounds("Team expansion Team revision"))?,
            i64::try_from(command.expected_capacity_revision)
                .map_err(|_| OperationsError::Bounds("Team expansion capacity revision"))?,
            command.expected_leadership_revision,
            recorded_at.get(),
        ],
    )?;
    let receipt =
        load_slot_assignment_by_request(transaction, command.workspace_id, command.request_id)?
            .ok_or(OperationsError::Corrupt(
                "Team expansion slot assignment readback",
            ))?
            .into_receipt(false)?;
    validate_slot_assignment_history(transaction, &receipt)?;
    Ok(receipt)
}

fn finish_slot_assignment_commit(
    commit_result: rusqlite::Result<()>,
    connection: &Connection,
    command: &TeamExpansionSlotAssignmentCommand,
    actor: &TeamExpansionActor,
    target: &TeamExpansionActor,
    receipt: TeamExpansionSlotAssignmentReceipt,
) -> Result<TeamExpansionSlotAssignmentReceipt, OperationsError> {
    match commit_result {
        Ok(()) => Ok(receipt),
        Err(source) => match load_slot_assignment_by_request(
            connection,
            command.workspace_id,
            command.request_id,
        ) {
            Ok(Some(raw)) => {
                let existing = raw.into_receipt(true)?;
                validate_slot_assignment_history(connection, &existing)?;
                if slot_assignment_matches_command(&existing, command, actor, target) {
                    Ok(existing)
                } else {
                    Err(OperationsError::Conflict(
                        "Team expansion slot assignment replay after unconfirmed commit",
                    ))
                }
            }
            Ok(None) | Err(_) => Err(OperationsError::OperationCommitUnconfirmed {
                workspace_id: command.workspace_id,
                request_id: command.request_id,
                source,
            }),
        },
    }
}

fn read_expansion_slot_assignment(
    connection: &Connection,
    workspace_id: WorkspaceId,
    slot: TeamExpansionSlotRef,
    actor_input: OperationalActor,
) -> Result<Option<TeamExpansionSlotAssignmentReceipt>, OperationsError> {
    validate_non_nil(slot.approval_id, "Team expansion approval identity")?;
    if slot.ordinal > 7 {
        return Err(OperationsError::Bounds("Team expansion slot ordinal"));
    }
    let transaction = connection.unchecked_transaction()?;
    teams::ensure_verified_workspace(&transaction, workspace_id)?;
    let (reader, _) = validate_actor(&transaction, workspace_id, actor_input)?;
    let is_current_orchestrator = current_leadership_authority_in_transaction(
        &transaction,
        LeadershipScope::workspace(workspace_id),
    )?
    .is_some_and(|authority| authority.participant_id() == reader.participant_id);
    let authorized = is_current_orchestrator
        || slot_assignment_target_matches_reader(&transaction, workspace_id, slot, &reader)?;
    if !authorized {
        transaction.commit()?;
        return Ok(None);
    }
    let Some(raw) = load_slot_assignment(&transaction, workspace_id, slot)? else {
        transaction.commit()?;
        return Ok(None);
    };
    let receipt = raw.into_receipt(false)?;
    validate_slot_assignment_history(&transaction, &receipt)?;
    transaction.commit()?;
    Ok(Some(receipt))
}

fn slot_assignment_target_matches_reader(
    connection: &Connection,
    workspace_id: WorkspaceId,
    slot: TeamExpansionSlotRef,
    reader: &TeamExpansionActor,
) -> Result<bool, OperationsError> {
    let target: Option<(Value, Value)> = connection
        .query_row(
            "SELECT target_participation_id,target_profile_id FROM ops_team_expansion_slot_assignments WHERE workspace_id=?1 AND decision_id=?2 AND ordinal=?3",
            params![
                workspace_id.into_uuid().as_bytes(),
                slot.approval_id.as_bytes(),
                i64::from(slot.ordinal),
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    Ok(target.is_some_and(|(participation, profile)| {
        stored_profile_matches_reader(participation, profile, reader)
    }))
}

pub(super) fn stored_profile_matches_reader(
    participation: Value,
    profile: Value,
    reader: &TeamExpansionActor,
) -> bool {
    matches!(
        (participation, profile),
        (Value::Blob(participation), Value::Blob(profile))
            if participation.as_slice() == reader.participation_id.into_uuid().as_bytes()
                && profile.as_slice() == reader.profile_id.as_bytes()
    )
}

fn ensure_approved_slot_available(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    slot: TeamExpansionSlotRef,
) -> Result<Uuid, OperationsError> {
    let result = connection
        .query_row(
            "SELECT slot.request_id,decision.state,decision.approved_count,anchor.request_id,assignment.request_id FROM ops_team_expansion_slots AS slot JOIN ops_team_expansion_decisions AS decision ON decision.workspace_id=slot.workspace_id AND decision.team_id=slot.team_id AND decision.decision_id=slot.decision_id AND decision.request_id=slot.request_id LEFT JOIN ops_team_expansion_cooldown_anchors AS anchor ON anchor.workspace_id=decision.workspace_id AND anchor.team_id=decision.team_id AND anchor.decision_id=decision.decision_id AND anchor.request_id=decision.request_id LEFT JOIN ops_team_expansion_slot_assignments AS assignment ON assignment.workspace_id=slot.workspace_id AND assignment.team_id=slot.team_id AND assignment.decision_id=slot.decision_id AND assignment.ordinal=slot.ordinal WHERE slot.workspace_id=?1 AND slot.team_id=?2 AND slot.decision_id=?3 AND slot.ordinal=?4",
            params![
                workspace_id.into_uuid().as_bytes(),
                team_id.as_bytes(),
                slot.approval_id.as_bytes(),
                i64::from(slot.ordinal),
            ],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<Vec<u8>>>(3)?,
                    row.get::<_, Option<Vec<u8>>>(4)?,
                ))
            },
        )
        .optional()?;
    let Some((request_id, state, approved_count, anchor, assignment)) = result else {
        return Err(OperationsError::Conflict(
            "Team expansion slot is not approved",
        ));
    };
    let approved_count = u8::try_from(approved_count)
        .map_err(|_| OperationsError::Corrupt("Team expansion approved count"))?;
    if state != "APPROVED"
        || slot.ordinal >= approved_count
        || anchor.is_none()
        || assignment.is_some()
    {
        return Err(OperationsError::Conflict(
            "Team expansion slot is not available",
        ));
    }
    Ok(Uuid::from_bytes(wire::id(request_id)?))
}

fn ensure_slot_target_eligible(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    target: &TeamExpansionActor,
) -> Result<(), OperationsError> {
    let joined_any_team: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_team_members AS member LEFT JOIN ops_workspace_participations AS participation ON participation.workspace_id=member.workspace_id AND participation.participation_id=member.participation_id WHERE member.workspace_id=?1 AND member.state='JOINED' AND (member.participation_id=?2 OR participation.agent_profile_id=?3))",
        params![workspace_id.into_uuid().as_bytes(), target.participation_id.into_uuid().as_bytes(), target.profile_id.as_bytes()],
        |row| row.get(0),
    )?;
    if joined_any_team {
        return Err(OperationsError::Conflict(
            "Team expansion target already has a home Team",
        ));
    }
    let inbound_dispatch: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_temporary_dispatch_reservations WHERE workspace_id=?1 AND receiving_team_id=?2 AND (target_participation_id=?3 OR target_profile_id=?4) AND state IN('RESERVED','SOURCE_READY','ACTIVE','DRAINING'))",
        params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes(), target.participation_id.into_uuid().as_bytes(), target.profile_id.as_bytes()],
        |row| row.get(0),
    )?;
    if inbound_dispatch {
        return Err(OperationsError::Conflict(
            "Team expansion target has inbound Team dispatch",
        ));
    }
    let active_assignment: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_team_expansion_slot_assignments AS assignment LEFT JOIN ops_team_expansion_slot_uses AS used ON used.decision_id=assignment.decision_id AND used.ordinal=assignment.ordinal WHERE assignment.workspace_id=?1 AND (assignment.target_participation_id=?2 OR assignment.target_profile_id=?3) AND used.decision_id IS NULL)",
        params![workspace_id.into_uuid().as_bytes(), target.participation_id.into_uuid().as_bytes(), target.profile_id.as_bytes()],
        |row| row.get(0),
    )?;
    if active_assignment {
        return Err(OperationsError::Conflict(
            "Team expansion target already has an outstanding Workspace slot",
        ));
    }
    Ok(())
}

fn slot_assignment_matches_command(
    receipt: &TeamExpansionSlotAssignmentReceipt,
    command: &TeamExpansionSlotAssignmentCommand,
    actor: &TeamExpansionActor,
    target: &TeamExpansionActor,
) -> bool {
    receipt.request_id == command.request_id
        && receipt.slot == command.slot
        && receipt.workspace_id == command.workspace_id
        && receipt.team_id == command.team_id
        && receipt.expected_team_revision == command.expected_team_revision
        && receipt.capacity_revision == command.expected_capacity_revision
        && receipt.leadership_revision == command.expected_leadership_revision
        && actor_invocation_same(&receipt.actor, actor)
        && actor_invocation_same(&receipt.target, target)
}

fn validate_slot_assignment_history(
    connection: &Connection,
    receipt: &TeamExpansionSlotAssignmentReceipt,
) -> Result<(), OperationsError> {
    let row = connection
        .query_row(
            "SELECT slot.request_id,decision.request_id,decision.state,decision.approved_count,anchor.request_id,slot.workspace_id,slot.team_id,assignment.target_participation_id,assignment.target_profile_id FROM ops_team_expansion_slot_assignments AS assignment JOIN ops_team_expansion_slots AS slot ON slot.workspace_id=assignment.workspace_id AND slot.team_id=assignment.team_id AND slot.decision_id=assignment.decision_id AND slot.ordinal=assignment.ordinal JOIN ops_team_expansion_decisions AS decision ON decision.workspace_id=slot.workspace_id AND decision.team_id=slot.team_id AND decision.decision_id=slot.decision_id AND decision.request_id=slot.request_id LEFT JOIN ops_team_expansion_cooldown_anchors AS anchor ON anchor.workspace_id=decision.workspace_id AND anchor.team_id=decision.team_id AND anchor.decision_id=decision.decision_id AND anchor.request_id=decision.request_id WHERE assignment.request_id=?1 AND assignment.workspace_id=?2 AND assignment.team_id=?3 AND assignment.decision_id=?4 AND assignment.ordinal=?5",
            params![
                receipt.request_id.as_bytes(),
                receipt.workspace_id.into_uuid().as_bytes(),
                receipt.team_id.as_bytes(),
                receipt.slot.approval_id.as_bytes(),
                i64::from(receipt.slot.ordinal),
            ],
            |row| {
                Ok(SlotAssignmentHistoryRaw {
                    slot_request_id: row.get(0)?,
                    decision_request_id: row.get(1)?,
                    decision_state: row.get(2)?,
                    approved_count: row.get(3)?,
                    cooldown_anchor: row.get(4)?,
                    workspace_id: row.get(5)?,
                    team_id: row.get(6)?,
                    target_participation_id: row.get(7)?,
                    target_profile_id: row.get(8)?,
                })
            },
        )
        .optional()?;
    let Some(row) = row else {
        return Err(OperationsError::Corrupt(
            "Team expansion slot assignment relation",
        ));
    };
    let approved_count = u8::try_from(row.approved_count)
        .map_err(|_| OperationsError::Corrupt("Team expansion approved count"))?;
    if Uuid::from_bytes(wire::id(row.slot_request_id)?)
        != Uuid::from_bytes(wire::id(row.decision_request_id)?)
        || row.decision_state != "APPROVED"
        || receipt.slot.ordinal >= approved_count
        || row.cooldown_anchor.is_none()
        || WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(row.workspace_id)?))
            != receipt.workspace_id
        || Uuid::from_bytes(wire::id(row.team_id)?) != receipt.team_id
        || WorkspaceParticipationId::from_uuid(Uuid::from_bytes(wire::id(
            row.target_participation_id,
        )?))?
            != receipt.participation_id
        || Uuid::from_bytes(wire::id(row.target_profile_id)?) != receipt.profile_id
    {
        return Err(OperationsError::Corrupt(
            "Team expansion slot assignment evidence",
        ));
    }
    Ok(())
}

struct SlotAssignmentHistoryRaw {
    slot_request_id: Vec<u8>,
    decision_request_id: Vec<u8>,
    decision_state: String,
    approved_count: i64,
    cooldown_anchor: Option<Vec<u8>>,
    workspace_id: Vec<u8>,
    team_id: Vec<u8>,
    target_participation_id: Vec<u8>,
    target_profile_id: Vec<u8>,
}

fn load_slot_assignment_by_request(
    connection: &Connection,
    workspace_id: WorkspaceId,
    request_id: Uuid,
) -> Result<Option<SlotAssignmentRaw>, OperationsError> {
    connection
        .query_row(
            "SELECT request_id,decision_id,ordinal,workspace_id,team_id,target_participation_id,target_profile_id,target_participant_id,target_activity_id,target_agent_id,target_session_id,target_display_name,actor_participation_id,actor_profile_id,actor_participant_id,actor_activity_id,actor_agent_id,actor_session_id,actor_display_name,expected_team_revision,capacity_revision,leadership_revision,recorded_at_ms FROM ops_team_expansion_slot_assignments WHERE request_id=?1 AND workspace_id=?2",
            params![request_id.as_bytes(), workspace_id.into_uuid().as_bytes()],
            decode_slot_assignment_raw,
        )
        .optional()
        .map_err(Into::into)
}

fn slot_assignment_request_id_exists(
    connection: &Connection,
    request_id: Uuid,
) -> Result<bool, OperationsError> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_team_expansion_slot_assignments WHERE request_id=?1)",
            params![request_id.as_bytes()],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

fn load_slot_assignment(
    connection: &Connection,
    workspace_id: WorkspaceId,
    slot: TeamExpansionSlotRef,
) -> Result<Option<SlotAssignmentRaw>, OperationsError> {
    connection
        .query_row(
            "SELECT assignment.request_id,assignment.decision_id,assignment.ordinal,assignment.workspace_id,assignment.team_id,assignment.target_participation_id,assignment.target_profile_id,assignment.target_participant_id,assignment.target_activity_id,assignment.target_agent_id,assignment.target_session_id,assignment.target_display_name,assignment.actor_participation_id,assignment.actor_profile_id,assignment.actor_participant_id,assignment.actor_activity_id,assignment.actor_agent_id,assignment.actor_session_id,assignment.actor_display_name,assignment.expected_team_revision,assignment.capacity_revision,assignment.leadership_revision,assignment.recorded_at_ms FROM ops_team_expansion_slot_assignments AS assignment JOIN ops_team_expansion_slots AS slot ON slot.workspace_id=assignment.workspace_id AND slot.team_id=assignment.team_id AND slot.decision_id=assignment.decision_id AND slot.ordinal=assignment.ordinal WHERE assignment.workspace_id=?1 AND assignment.decision_id=?2 AND assignment.ordinal=?3",
            params![workspace_id.into_uuid().as_bytes(), slot.approval_id.as_bytes(), i64::from(slot.ordinal)],
            decode_slot_assignment_raw,
        )
        .optional()
        .map_err(Into::into)
}

struct SlotAssignmentRaw {
    request_id: Vec<u8>,
    decision_id: Vec<u8>,
    ordinal: i64,
    workspace_id: Vec<u8>,
    team_id: Vec<u8>,
    target: TeamExpansionActorRaw,
    actor: TeamExpansionActorRaw,
    expected_team_revision: i64,
    capacity_revision: i64,
    leadership_revision: i64,
    recorded_at_ms: i64,
}

fn decode_slot_assignment_raw(row: &rusqlite::Row<'_>) -> rusqlite::Result<SlotAssignmentRaw> {
    Ok(SlotAssignmentRaw {
        request_id: row.get(0)?,
        decision_id: row.get(1)?,
        ordinal: row.get(2)?,
        workspace_id: row.get(3)?,
        team_id: row.get(4)?,
        target: TeamExpansionActorRaw {
            participation_id: row.get(5)?,
            profile_id: row.get(6)?,
            participant_id: row.get(7)?,
            activity_id: row.get(8)?,
            agent_id: row.get(9)?,
            session_id: row.get(10)?,
            display_name: row.get(11)?,
        },
        actor: TeamExpansionActorRaw {
            participation_id: row.get(12)?,
            profile_id: row.get(13)?,
            participant_id: row.get(14)?,
            activity_id: row.get(15)?,
            agent_id: row.get(16)?,
            session_id: row.get(17)?,
            display_name: row.get(18)?,
        },
        expected_team_revision: row.get(19)?,
        capacity_revision: row.get(20)?,
        leadership_revision: row.get(21)?,
        recorded_at_ms: row.get(22)?,
    })
}

impl SlotAssignmentRaw {
    fn into_receipt(
        self,
        replayed: bool,
    ) -> Result<TeamExpansionSlotAssignmentReceipt, OperationsError> {
        let ordinal = u8::try_from(self.ordinal)
            .map_err(|_| OperationsError::Corrupt("Team expansion slot ordinal"))?;
        let expected_team_revision = u64::try_from(self.expected_team_revision)
            .map_err(|_| OperationsError::Corrupt("Team expansion Team revision"))?;
        let capacity_revision = u64::try_from(self.capacity_revision)
            .map_err(|_| OperationsError::Corrupt("Team expansion capacity revision"))?;
        if ordinal > 7
            || expected_team_revision == 0
            || capacity_revision == 0
            || self.leadership_revision < 1
        {
            return Err(OperationsError::Corrupt(
                "Team expansion slot assignment values",
            ));
        }
        let target = self.target.into_actor()?;
        let actor = self.actor.into_actor()?;
        Ok(TeamExpansionSlotAssignmentReceipt {
            request_id: Uuid::from_bytes(wire::id(self.request_id)?),
            slot: TeamExpansionSlotRef::new(Uuid::from_bytes(wire::id(self.decision_id)?), ordinal),
            workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(self.workspace_id)?)),
            team_id: Uuid::from_bytes(wire::id(self.team_id)?),
            participation_id: target.participation_id,
            profile_id: target.profile_id,
            participant_id: target.participant_id,
            expected_team_revision,
            capacity_revision,
            leadership_revision: self.leadership_revision,
            target,
            actor,
            recorded_at: TimestampMillis::new(self.recorded_at_ms).map_err(|_| {
                OperationsError::Corrupt("Team expansion slot assignment timestamp")
            })?,
            replayed,
        })
    }
}

fn insert_team_expansion_request(
    transaction: &Transaction<'_>,
    receipt: &TeamExpansionRequestReceipt,
) -> Result<(), OperationsError> {
    transaction.execute(
        "INSERT INTO ops_team_expansion_requests(request_id,workspace_id,team_id,expected_team_revision,capacity_revision,requester_leadership_revision,requested_count,reason,actor_activity_id,actor_agent_id,actor_session_id,actor_participation_id,actor_profile_id,actor_participant_id,actor_display_name,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
        params![
            receipt.request_id.as_bytes(),
            receipt.workspace_id.into_uuid().as_bytes(),
            receipt.team_id.as_bytes(),
            i64::try_from(receipt.expected_team_revision)
                .map_err(|_| OperationsError::Bounds("Team expansion Team revision"))?,
            i64::try_from(receipt.capacity_revision)
                .map_err(|_| OperationsError::Bounds("Team expansion capacity revision"))?,
            receipt.requester_leadership_revision,
            i64::from(receipt.requested_count),
            receipt.reason,
            receipt.actor.activity_id.into_uuid().as_bytes(),
            receipt.actor.agent_id.into_uuid().as_bytes(),
            receipt.actor.session_id.into_uuid().as_bytes(),
            receipt.actor.participation_id.into_uuid().as_bytes(),
            receipt.actor.profile_id.as_bytes(),
            receipt.actor.participant_id.as_bytes(),
            receipt.actor.display_name,
            receipt.recorded_at.get(),
        ],
    )?;
    Ok(())
}

fn load_team_expansion_request_raw(
    connection: &Connection,
    workspace_id: WorkspaceId,
    request_id: Uuid,
) -> Result<Option<TeamExpansionRequestRaw>, OperationsError> {
    connection
        .query_row(
            "SELECT request_id,workspace_id,team_id,expected_team_revision,capacity_revision,requester_leadership_revision,requested_count,reason,actor_activity_id,actor_agent_id,actor_session_id,actor_participation_id,actor_profile_id,actor_participant_id,actor_display_name,recorded_at_ms FROM ops_team_expansion_requests WHERE request_id=?1 AND workspace_id=?2",
            params![request_id.as_bytes(), workspace_id.into_uuid().as_bytes()],
            decode_team_expansion_request_raw,
        )
        .optional()
        .map_err(Into::into)
}

fn team_expansion_request_id_exists(
    connection: &Connection,
    request_id: Uuid,
) -> Result<bool, OperationsError> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_team_expansion_requests WHERE request_id=?1)",
            params![request_id.as_bytes()],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

fn request_matches_command(
    receipt: &TeamExpansionRequestReceipt,
    command: &TeamExpansionRequestCommand,
    actor: &TeamExpansionActor,
) -> bool {
    receipt.request_id == command.request_id
        && receipt.workspace_id == command.workspace_id
        && receipt.team_id == command.team_id
        && receipt.expected_team_revision == command.expected_team_revision
        && receipt.capacity_revision == command.expected_capacity_revision
        && receipt.requester_leadership_revision == command.expected_leadership_revision
        && receipt.requested_count == command.requested_count
        && receipt.reason == command.reason
        && actor_invocation_same(&receipt.actor, actor)
}

fn ensure_expansion_request_unlocked(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    now: TimestampMillis,
) -> Result<(), OperationsError> {
    let pending_anchor: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_team_expansion_decisions AS decision LEFT JOIN ops_team_expansion_cooldown_anchors AS anchor ON anchor.workspace_id=decision.workspace_id AND anchor.team_id=decision.team_id AND anchor.request_id=decision.request_id AND anchor.decision_id=decision.decision_id WHERE decision.workspace_id=?1 AND decision.team_id=?2 AND decision.state='APPROVED' AND anchor.request_id IS NULL)",
        params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()],
        |row| row.get(0),
    )?;
    if pending_anchor {
        return Err(OperationsError::Conflict(
            "Team expansion cooldown anchor pending",
        ));
    }
    let latest: Option<(i64, i64)> = connection
        .query_row(
            "SELECT anchor_at_ms,cooldown_until_ms FROM ops_team_expansion_cooldown_anchors WHERE workspace_id=?1 AND team_id=?2 ORDER BY anchor_at_ms DESC LIMIT 1",
            params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((started_at, until)) = latest {
        if now.get() < started_at {
            return Err(OperationsError::Conflict(
                "Team expansion trusted clock regressed",
            ));
        }
        if now.get() < until {
            return Err(OperationsError::Conflict("Team expansion cooldown active"));
        }
    }
    Ok(())
}

fn ensure_source_time_not_regressed(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    observed_at: TimestampMillis,
) -> Result<(), OperationsError> {
    let latest: Option<i64> = connection.query_row(
        "SELECT MAX(value) FROM (SELECT updated_at_ms AS value FROM ops_teams WHERE workspace_id=?1 AND team_id=?2 UNION ALL SELECT MAX(recorded_at_ms) FROM ops_team_capacity_events WHERE workspace_id=?1 AND team_id=?2 UNION ALL SELECT MAX(recorded_at_ms) FROM ops_team_expansion_requests WHERE workspace_id=?1 AND team_id=?2 UNION ALL SELECT MAX(decided_at_ms) FROM ops_team_expansion_decisions WHERE workspace_id=?1 AND team_id=?2 UNION ALL SELECT MAX(anchor_at_ms) FROM ops_team_expansion_cooldown_anchors WHERE workspace_id=?1 AND team_id=?2 UNION ALL SELECT MAX(recorded_at_ms) FROM ops_team_expansion_slot_assignments WHERE workspace_id=?1 AND team_id=?2)",
        params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()],
        |row| row.get(0),
    )?;
    let recovery_latest: Option<i64> = connection.query_row(
        "SELECT MAX(recorded_at_ms) FROM ops_team_expansion_legacy_slot_recoveries WHERE workspace_id=?1 AND team_id=?2",
        params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()],
        |row| row.get(0),
    )?;
    let latest = latest.into_iter().chain(recovery_latest).max();
    if latest.is_some_and(|latest| observed_at.get() < latest) {
        return Err(OperationsError::Conflict(
            "Team expansion trusted clock regressed",
        ));
    }
    Ok(())
}

struct TeamExpansionRequestRaw {
    request_id: Vec<u8>,
    workspace_id: Vec<u8>,
    team_id: Vec<u8>,
    expected_team_revision: i64,
    capacity_revision: i64,
    requester_leadership_revision: i64,
    requested_count: i64,
    reason: String,
    actor: TeamExpansionActorRaw,
    recorded_at_ms: i64,
}

struct TeamExpansionActorRaw {
    activity_id: Vec<u8>,
    agent_id: Vec<u8>,
    session_id: Vec<u8>,
    participation_id: Vec<u8>,
    profile_id: Vec<u8>,
    participant_id: Vec<u8>,
    display_name: String,
}

fn decode_team_expansion_request_raw(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<TeamExpansionRequestRaw> {
    Ok(TeamExpansionRequestRaw {
        request_id: row.get(0)?,
        workspace_id: row.get(1)?,
        team_id: row.get(2)?,
        expected_team_revision: row.get(3)?,
        capacity_revision: row.get(4)?,
        requester_leadership_revision: row.get(5)?,
        requested_count: row.get(6)?,
        reason: row.get(7)?,
        actor: TeamExpansionActorRaw {
            activity_id: row.get(8)?,
            agent_id: row.get(9)?,
            session_id: row.get(10)?,
            participation_id: row.get(11)?,
            profile_id: row.get(12)?,
            participant_id: row.get(13)?,
            display_name: row.get(14)?,
        },
        recorded_at_ms: row.get(15)?,
    })
}

impl TeamExpansionRequestRaw {
    fn into_receipt(self, replayed: bool) -> Result<TeamExpansionRequestReceipt, OperationsError> {
        let expected_team_revision = u64::try_from(self.expected_team_revision)
            .map_err(|_| OperationsError::Corrupt("Team expansion Team revision"))?;
        let capacity_revision = u64::try_from(self.capacity_revision)
            .map_err(|_| OperationsError::Corrupt("Team expansion capacity revision"))?;
        let requested_count = u8::try_from(self.requested_count)
            .map_err(|_| OperationsError::Corrupt("Team expansion requested count"))?;
        if expected_team_revision == 0
            || capacity_revision == 0
            || self.requester_leadership_revision < 1
            || !(1..=GLOBAL_TEAM_MEMBER_CAP).contains(&requested_count)
            || self.reason.trim().is_empty()
            || self.reason.len() > MAX_EXPANSION_REASON_BYTES
        {
            return Err(OperationsError::Corrupt("Team expansion request values"));
        }
        Ok(TeamExpansionRequestReceipt {
            request_id: Uuid::from_bytes(wire::id(self.request_id)?),
            workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(self.workspace_id)?)),
            team_id: Uuid::from_bytes(wire::id(self.team_id)?),
            expected_team_revision,
            capacity_revision,
            requester_leadership_revision: self.requester_leadership_revision,
            requested_count,
            reason: self.reason,
            actor: self.actor.into_actor()?,
            recorded_at: TimestampMillis::new(self.recorded_at_ms)
                .map_err(|_| OperationsError::Corrupt("Team expansion request timestamp"))?,
            replayed,
        })
    }
}

impl TeamExpansionActorRaw {
    fn into_actor(self) -> Result<TeamExpansionActor, OperationsError> {
        Ok(TeamExpansionActor {
            activity_id: ActivityId::from_uuid(Uuid::from_bytes(wire::id(self.activity_id)?)),
            agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(self.agent_id)?)),
            session_id: AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(self.session_id)?)),
            participation_id: WorkspaceParticipationId::from_uuid(Uuid::from_bytes(wire::id(
                self.participation_id,
            )?))?,
            profile_id: Uuid::from_bytes(wire::id(self.profile_id)?),
            participant_id: Uuid::from_bytes(wire::id(self.participant_id)?),
            display_name: self.display_name,
        })
    }
}

fn load_capacity_receipt_raw(
    connection: &Connection,
    workspace_id: WorkspaceId,
    request_id: Uuid,
) -> Result<Option<CapacityReceiptRaw>, OperationsError> {
    connection.query_row(
        "SELECT request_id,team_id,expected_capacity_revision,expected_team_revision,result_team_revision,capacity_revision,max_members,recorded_at_ms,actor_activity_id,actor_agent_id,actor_session_id,actor_participation_id,actor_profile_id,actor_participant_id,actor_display_name,operation,leadership_revision FROM ops_team_capacity_events WHERE request_id=?1 AND workspace_id=?2",
        params![request_id.as_bytes(), workspace_id.into_uuid().as_bytes()],
        decode_capacity_receipt_raw,
    ).optional().map_err(Into::into)
}

fn capacity_request_id_exists(
    connection: &Connection,
    request_id: Uuid,
) -> Result<bool, OperationsError> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_team_capacity_events WHERE request_id=?1)",
            params![request_id.as_bytes()],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

fn team_revision_and_active(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<(u64, bool), OperationsError> {
    let row = connection
        .query_row(
            "SELECT revision,state FROM ops_teams WHERE workspace_id=?1 AND team_id=?2",
            params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?
        .ok_or(OperationsError::Conflict("team workspace scope"))?;
    let revision = u64::try_from(row.0).map_err(|_| OperationsError::Corrupt("team revision"))?;
    let active = match row.1.as_str() {
        "ACTIVE" => true,
        "RETIRED" => false,
        _ => return Err(OperationsError::Corrupt("team state")),
    };
    Ok((revision, active))
}

pub(crate) fn read_capacity_in_transaction(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<Option<TeamCapacitySnapshot>, OperationsError> {
    let transaction = connection.unchecked_transaction()?;
    teams::ensure_verified_workspace(&transaction, workspace_id)?;
    team_revision_and_active(&transaction, workspace_id, team_id)?;
    let result = load_current_capacity(&transaction, workspace_id, team_id)?;
    transaction.commit()?;
    Ok(result)
}

pub(crate) fn ensure_team_capacity_for_addition(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    consuming_slot: Option<TeamExpansionSlotRef>,
    participation_id: Option<WorkspaceParticipationId>,
    profile_id: Option<Uuid>,
) -> Result<(), OperationsError> {
    let capacity = load_current_capacity(transaction, workspace_id, team_id)?
        .ok_or(OperationsError::Conflict("team capacity is not configured"))?;
    let joins_and_tf: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM (SELECT participation_id AS id FROM ops_team_members WHERE workspace_id=?1 AND team_id=?2 AND state='JOINED' UNION SELECT target_participation_id AS id FROM ops_temporary_dispatch_reservations WHERE workspace_id=?1 AND receiving_team_id=?2 AND state IN('RESERVED','SOURCE_READY','ACTIVE','DRAINING'))",
        params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()],
        |row| row.get(0),
    )?;
    let mut held_slots = outstanding_slots(transaction, workspace_id, team_id)?;
    if let Some(slot) = consuming_slot {
        validate_consumable_slot(
            transaction,
            workspace_id,
            team_id,
            slot,
            participation_id.ok_or(OperationsError::Conflict("Team expansion slot participant"))?,
            profile_id.ok_or(OperationsError::Conflict("Team expansion slot Profile"))?,
        )?;
        held_slots = held_slots
            .checked_sub(1)
            .ok_or(OperationsError::Corrupt("team expansion capacity hold"))?;
    }
    // Keep conversions and sums checked at the persisted integer boundary.
    let occupied = usize::try_from(joins_and_tf)
        .map_err(|_| OperationsError::Corrupt("team capacity count"))?;
    let held_slots = usize::try_from(held_slots)
        .map_err(|_| OperationsError::Corrupt("team expansion capacity hold"))?;
    let next_occupied = occupied
        .checked_add(held_slots)
        .ok_or(OperationsError::Bounds("team member capacity"))?;
    let cap = usize::from(capacity.max_members.get());
    if next_occupied >= cap {
        return Err(OperationsError::Bounds("team member capacity"));
    }
    Ok(())
}

pub(crate) fn has_unconsumed_expansion_slot_for_target(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    participation_id: Uuid,
    profile_id: Uuid,
) -> Result<bool, OperationsError> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_team_expansion_slot_assignments AS assignment LEFT JOIN ops_team_expansion_slot_uses AS used ON used.decision_id=assignment.decision_id AND used.ordinal=assignment.ordinal WHERE assignment.workspace_id=?1 AND assignment.team_id=?2 AND assignment.target_participation_id=?3 AND assignment.target_profile_id=?4 AND used.decision_id IS NULL)",
            params![
                workspace_id.into_uuid().as_bytes(),
                team_id.as_bytes(),
                participation_id.as_bytes(),
                profile_id.as_bytes(),
            ],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

pub(crate) fn validate_consumable_slot(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    slot: TeamExpansionSlotRef,
    participation_id: WorkspaceParticipationId,
    profile_id: Uuid,
) -> Result<(), OperationsError> {
    if slot.approval_id.is_nil() || slot.ordinal > 7 {
        return Err(OperationsError::Conflict("Team expansion slot identity"));
    }
    let row = connection
        .query_row(
            "SELECT slot.request_id,decision.state,decision.approved_count,anchor.request_id,used.decision_id FROM ops_team_expansion_slots AS slot JOIN ops_team_expansion_decisions AS decision ON decision.workspace_id=slot.workspace_id AND decision.team_id=slot.team_id AND decision.decision_id=slot.decision_id AND decision.request_id=slot.request_id JOIN ops_team_expansion_slot_assignments AS assignment ON assignment.workspace_id=slot.workspace_id AND assignment.team_id=slot.team_id AND assignment.decision_id=slot.decision_id AND assignment.ordinal=slot.ordinal LEFT JOIN ops_team_expansion_cooldown_anchors AS anchor ON anchor.workspace_id=decision.workspace_id AND anchor.team_id=decision.team_id AND anchor.decision_id=decision.decision_id AND anchor.request_id=decision.request_id LEFT JOIN ops_team_expansion_slot_uses AS used ON used.decision_id=slot.decision_id AND used.ordinal=slot.ordinal WHERE slot.workspace_id=?1 AND slot.team_id=?2 AND slot.decision_id=?3 AND slot.ordinal=?4 AND assignment.target_participation_id=?5 AND assignment.target_profile_id=?6",
            params![
                workspace_id.into_uuid().as_bytes(),
                team_id.as_bytes(),
                slot.approval_id.as_bytes(),
                i64::from(slot.ordinal),
                participation_id.into_uuid().as_bytes(),
                profile_id.as_bytes(),
            ],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<Vec<u8>>>(3)?,
                    row.get::<_, Option<Vec<u8>>>(4)?,
                ))
            },
        )
        .optional()?;
    let Some((request_id, state, approved_count, anchor, used)) = row else {
        return Err(OperationsError::Conflict(
            "Team expansion slot is not assigned to this Profile",
        ));
    };
    let approved_count = u8::try_from(approved_count)
        .map_err(|_| OperationsError::Corrupt("Team expansion approved count"))?;
    if state != "APPROVED" || slot.ordinal >= approved_count || anchor.is_none() || used.is_some() {
        return Err(OperationsError::Conflict(
            "Team expansion slot is not consumable",
        ));
    }
    let _source_request_id = Uuid::from_bytes(wire::id(request_id)?);
    existing_home_transfer::ensure_generic_slot_admission(connection, workspace_id, team_id, slot)?;
    let inbound_dispatch: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_temporary_dispatch_reservations WHERE workspace_id=?1 AND receiving_team_id=?2 AND target_participation_id=?3 AND state IN('RESERVED','SOURCE_READY','ACTIVE','DRAINING'))",
        params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes(), participation_id.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    if inbound_dispatch {
        return Err(OperationsError::Conflict(
            "Team expansion target has inbound Team dispatch",
        ));
    }
    let other_workspace_assignment: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_team_expansion_slot_assignments AS assignment LEFT JOIN ops_team_expansion_slot_uses AS used ON used.decision_id=assignment.decision_id AND used.ordinal=assignment.ordinal WHERE assignment.workspace_id=?1 AND (assignment.target_participation_id=?2 OR assignment.target_profile_id=?3) AND used.decision_id IS NULL AND (assignment.decision_id<>?4 OR assignment.ordinal<>?5))",
        params![
            workspace_id.into_uuid().as_bytes(),
            participation_id.into_uuid().as_bytes(),
            profile_id.as_bytes(),
            slot.approval_id.as_bytes(),
            i64::from(slot.ordinal),
        ],
        |row| row.get(0),
    )?;
    if other_workspace_assignment {
        return Err(OperationsError::Conflict(
            "Team expansion target has another outstanding Workspace slot",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TeamExpansionSlotUseIntent {
    pub(crate) join_request_id: Uuid,
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) team_id: Uuid,
    pub(crate) slot: TeamExpansionSlotRef,
    pub(crate) participation_id: WorkspaceParticipationId,
    pub(crate) profile_id: Uuid,
    pub(crate) membership_revision: u64,
    pub(crate) recorded_at: TimestampMillis,
}

pub(crate) fn record_team_expansion_slot_use(
    transaction: &Transaction<'_>,
    intent: TeamExpansionSlotUseIntent,
) -> Result<(), OperationsError> {
    validate_non_nil(intent.join_request_id, "Team join request identity")?;
    validate_consumable_slot(
        transaction,
        intent.workspace_id,
        intent.team_id,
        intent.slot,
        intent.participation_id,
        intent.profile_id,
    )?;
    transaction.execute(
        "INSERT INTO ops_team_expansion_slot_uses(decision_id,ordinal,join_request_id,workspace_id,team_id,participation_id,profile_id,membership_revision,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
        params![
            intent.slot.approval_id.as_bytes(),
            i64::from(intent.slot.ordinal),
            intent.join_request_id.as_bytes(),
            intent.workspace_id.into_uuid().as_bytes(),
            intent.team_id.as_bytes(),
            intent.participation_id.into_uuid().as_bytes(),
            intent.profile_id.as_bytes(),
            i64::try_from(intent.membership_revision)
                .map_err(|_| OperationsError::Bounds("Team membership revision"))?,
            intent.recorded_at.get(),
        ],
    )?;
    validate_slot_use_history(transaction, &intent)
}

pub(crate) fn slot_ref_for_join_request(
    connection: &Connection,
    join_request_id: Uuid,
) -> Result<Option<TeamExpansionSlotRef>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT decision_id,ordinal,workspace_id,team_id,participation_id,profile_id,membership_revision,recorded_at_ms FROM ops_team_expansion_slot_uses WHERE join_request_id=?1",
            params![join_request_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                ))
            },
        )
        .optional()?;
    let Some((
        decision_id,
        ordinal,
        workspace_id,
        team_id,
        participation_id,
        profile_id,
        revision,
        recorded_at,
    )) = raw
    else {
        return Ok(None);
    };
    let slot = TeamExpansionSlotRef::new(
        Uuid::from_bytes(wire::id(decision_id)?),
        u8::try_from(ordinal)
            .map_err(|_| OperationsError::Corrupt("Team expansion slot ordinal"))?,
    );
    let intent = TeamExpansionSlotUseIntent {
        join_request_id,
        workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(workspace_id)?)),
        team_id: Uuid::from_bytes(wire::id(team_id)?),
        slot,
        participation_id: WorkspaceParticipationId::from_uuid(Uuid::from_bytes(wire::id(
            participation_id,
        )?))?,
        profile_id: Uuid::from_bytes(wire::id(profile_id)?),
        membership_revision: u64::try_from(revision)
            .map_err(|_| OperationsError::Corrupt("Team membership revision"))?,
        recorded_at: TimestampMillis::new(recorded_at)
            .map_err(|_| OperationsError::Corrupt("Team expansion slot use timestamp"))?,
    };
    validate_slot_use_history(connection, &intent)?;
    Ok(Some(slot))
}

pub(crate) fn validate_legacy_join_slot_history(
    connection: &Connection,
    join_request_id: Uuid,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    observed_at: TimestampMillis,
    slot: Option<TeamExpansionSlotRef>,
) -> Result<(), OperationsError> {
    let event: Option<(Vec<u8>, Vec<u8>, String, i64)> = connection
        .query_row(
            "SELECT workspace_id,team_id,state,observed_at_ms FROM ops_team_member_events WHERE request_id=?1",
            params![join_request_id.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((event_workspace, event_team, state, event_at)) = event else {
        return Err(OperationsError::Corrupt(
            "Team join membership event missing",
        ));
    };
    if Uuid::from_bytes(wire::id(event_workspace)?) != workspace_id.into_uuid()
        || Uuid::from_bytes(wire::id(event_team)?) != team_id
        || state != "JOINED"
        || event_at != observed_at.get()
    {
        return Err(OperationsError::Corrupt(
            "Team join membership event mismatch",
        ));
    }
    let stored_slot = slot_ref_for_join_request(connection, join_request_id)?;
    match (slot, stored_slot) {
        (None, None) => Ok(()),
        (Some(expected), Some(actual)) if expected == actual => Ok(()),
        (None | Some(_), Some(_)) | (Some(_), None) => {
            Err(OperationsError::Corrupt("Team join slot history mismatch"))
        }
    }
}

struct SlotUseHistoryRaw {
    source_request_id: Vec<u8>,
    decision_state: String,
    approved_count: i64,
    anchor_request_id: Option<Vec<u8>>,
    target_participation_id: Vec<u8>,
    target_profile_id: Vec<u8>,
    event_membership_revision: i64,
    event_observed_at_ms: i64,
    event_state: String,
    event_workspace_id: Vec<u8>,
    event_team_id: Vec<u8>,
    event_participation_id: Vec<u8>,
    event_profile_id: Option<Vec<u8>>,
    request_operation: String,
    request_workspace_id: Vec<u8>,
    request_team_id: Vec<u8>,
    request_membership_state: String,
    request_participation_id: Option<Vec<u8>>,
    request_membership_revision: Option<i64>,
    request_observed_at_ms: i64,
    event_actor_session_id: Option<Vec<u8>>,
    event_role: Option<String>,
    request_actor_kind: Option<String>,
    request_actor_activity_id: Option<Vec<u8>>,
    request_actor_agent_id: Option<Vec<u8>>,
    request_actor_participation_id: Option<Vec<u8>>,
    request_actor_profile_id: Option<Vec<u8>>,
    request_actor_session_id: Option<Vec<u8>>,
    request_role: Option<String>,
    request_result_membership_role: Option<String>,
}

fn slot_use_history_raw(
    connection: &Connection,
    intent: &TeamExpansionSlotUseIntent,
) -> Result<SlotUseHistoryRaw, OperationsError> {
    connection
        .query_row(
            "SELECT slot.request_id,decision.state,decision.approved_count,anchor.request_id,assignment.target_participation_id,assignment.target_profile_id,event.membership_revision,event.observed_at_ms,event.state,event.workspace_id,event.team_id,event.participation_id,event.actor_profile_id,request.operation,request.workspace_id,request.team_id,request.result_membership_state,request.result_membership_participation_id,request.result_membership_revision,request.observed_at_ms,event.actor_session_id,event.role,request.request_actor_kind,request.request_actor_activity_id,request.request_actor_agent_id,request.actor_participation_id,request.actor_profile_id,request.actor_session_id,request.role,request.result_membership_role FROM ops_team_expansion_slot_uses AS used JOIN ops_team_expansion_slot_assignments AS assignment ON assignment.workspace_id=used.workspace_id AND assignment.team_id=used.team_id AND assignment.decision_id=used.decision_id AND assignment.ordinal=used.ordinal JOIN ops_team_expansion_slots AS slot ON slot.workspace_id=assignment.workspace_id AND slot.team_id=assignment.team_id AND slot.decision_id=assignment.decision_id AND slot.ordinal=assignment.ordinal JOIN ops_team_expansion_decisions AS decision ON decision.workspace_id=slot.workspace_id AND decision.team_id=slot.team_id AND decision.decision_id=slot.decision_id AND decision.request_id=slot.request_id LEFT JOIN ops_team_expansion_cooldown_anchors AS anchor ON anchor.workspace_id=decision.workspace_id AND anchor.team_id=decision.team_id AND anchor.decision_id=decision.decision_id AND anchor.request_id=decision.request_id JOIN ops_team_member_events AS event ON event.request_id=used.join_request_id AND event.workspace_id=used.workspace_id AND event.team_id=used.team_id AND event.participation_id=used.participation_id JOIN ops_team_requests AS request ON request.request_id=used.join_request_id WHERE used.join_request_id=?1 AND used.workspace_id=?2 AND used.team_id=?3 AND used.decision_id=?4 AND used.ordinal=?5 AND used.participation_id=?6 AND used.profile_id=?7 AND used.membership_revision=?8 AND used.recorded_at_ms=?9",
            params![
                intent.join_request_id.as_bytes(),
                intent.workspace_id.into_uuid().as_bytes(),
                intent.team_id.as_bytes(),
                intent.slot.approval_id.as_bytes(),
                i64::from(intent.slot.ordinal),
                intent.participation_id.into_uuid().as_bytes(),
                intent.profile_id.as_bytes(),
                i64::try_from(intent.membership_revision)
                    .map_err(|_| OperationsError::Corrupt("Team membership revision"))?,
                intent.recorded_at.get(),
            ],
            |row| {
                Ok(SlotUseHistoryRaw {
                    source_request_id: row.get(0)?,
                    decision_state: row.get(1)?,
                    approved_count: row.get(2)?,
                    anchor_request_id: row.get(3)?,
                    target_participation_id: row.get(4)?,
                    target_profile_id: row.get(5)?,
                    event_membership_revision: row.get(6)?,
                    event_observed_at_ms: row.get(7)?,
                    event_state: row.get(8)?,
                    event_workspace_id: row.get(9)?,
                    event_team_id: row.get(10)?,
                    event_participation_id: row.get(11)?,
                    event_profile_id: row.get(12)?,
                    request_operation: row.get(13)?,
                    request_workspace_id: row.get(14)?,
                    request_team_id: row.get(15)?,
                    request_membership_state: row.get(16)?,
                    request_participation_id: row.get(17)?,
                    request_membership_revision: row.get(18)?,
                    request_observed_at_ms: row.get(19)?,
                    event_actor_session_id: row.get(20)?,
                    event_role: row.get(21)?,
                    request_actor_kind: row.get(22)?,
                    request_actor_activity_id: row.get(23)?,
                    request_actor_agent_id: row.get(24)?,
                    request_actor_participation_id: row.get(25)?,
                    request_actor_profile_id: row.get(26)?,
                    request_actor_session_id: row.get(27)?,
                    request_role: row.get(28)?,
                    request_result_membership_role: row.get(29)?,
                })
            },
        )
        .optional()?
        .ok_or(OperationsError::Corrupt("Team expansion slot use relation"))
}

fn validate_slot_use_history(
    connection: &Connection,
    intent: &TeamExpansionSlotUseIntent,
) -> Result<(), OperationsError> {
    let raw = slot_use_history_raw(connection, intent)?;
    validate_slot_use_history_evidence(intent, raw)
}

fn validate_slot_use_request_source(
    raw: &SlotUseHistoryRaw,
    intent: &TeamExpansionSlotUseIntent,
) -> Result<(), OperationsError> {
    match raw.request_actor_kind.as_deref() {
        None => {
            if raw.request_actor_activity_id.is_some() || raw.request_actor_agent_id.is_some() {
                return Err(OperationsError::Corrupt(
                    "legacy Team slot Join actor source shape",
                ));
            }
            Ok(())
        }
        Some("PARTICIPANT") => {
            let activity_id = Uuid::from_bytes(wire::id(
                raw.request_actor_activity_id
                    .clone()
                    .ok_or(OperationsError::Corrupt("Team slot Join source Activity"))?,
            )?);
            let agent_id = Uuid::from_bytes(wire::id(
                raw.request_actor_agent_id
                    .clone()
                    .ok_or(OperationsError::Corrupt("Team slot Join source Agent"))?,
            )?);
            let request_participation = raw
                .request_actor_participation_id
                .clone()
                .map(|value| {
                    wire::id(value)
                        .map(Uuid::from_bytes)
                        .and_then(WorkspaceParticipationId::from_uuid)
                })
                .transpose()?;
            let request_profile = raw
                .request_actor_profile_id
                .clone()
                .map(|value| wire::id(value).map(Uuid::from_bytes))
                .transpose()?;
            let request_session = raw
                .request_actor_session_id
                .clone()
                .map(|value| {
                    wire::id(value).map(|uuid| AgentSessionId::from_uuid(Uuid::from_bytes(uuid)))
                })
                .transpose()?;
            let event_session = raw
                .event_actor_session_id
                .clone()
                .map(|value| {
                    wire::id(value).map(|uuid| AgentSessionId::from_uuid(Uuid::from_bytes(uuid)))
                })
                .transpose()?;
            let role_matches = raw.request_role.as_deref().is_some_and(|role| {
                teams::validate_team_role(role).is_ok()
                    && raw.request_result_membership_role.as_deref() == Some(role)
                    && raw.event_role.as_deref() == Some(role)
            });
            if activity_id.is_nil()
                || agent_id.is_nil()
                || request_participation != Some(intent.participation_id)
                || request_profile != Some(intent.profile_id)
                || request_session.is_none()
                || request_session != event_session
                || !role_matches
            {
                return Err(OperationsError::Corrupt("Team slot Join source evidence"));
            }
            Ok(())
        }
        Some(_) => Err(OperationsError::Corrupt("Team slot Join actor kind")),
    }
}

fn validate_slot_use_history_evidence(
    intent: &TeamExpansionSlotUseIntent,
    raw: SlotUseHistoryRaw,
) -> Result<(), OperationsError> {
    validate_slot_use_request_source(&raw, intent)?;
    let approved_count = u8::try_from(raw.approved_count)
        .map_err(|_| OperationsError::Corrupt("Team expansion approved count"))?;
    let event_profile_id = raw
        .event_profile_id
        .map(|value| wire::id(value).map(Uuid::from_bytes))
        .transpose()?;
    let request_participation_id = raw
        .request_participation_id
        .map(|value| {
            wire::id(value)
                .map(Uuid::from_bytes)
                .and_then(WorkspaceParticipationId::from_uuid)
        })
        .transpose()?;
    if Uuid::from_bytes(wire::id(raw.source_request_id)?) == Uuid::nil()
        || raw.decision_state != "APPROVED"
        || intent.slot.ordinal >= approved_count
        || raw.anchor_request_id.is_none()
        || WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(raw.event_workspace_id)?))
            != intent.workspace_id
        || Uuid::from_bytes(wire::id(raw.event_team_id)?) != intent.team_id
        || WorkspaceParticipationId::from_uuid(Uuid::from_bytes(wire::id(
            raw.event_participation_id,
        )?))?
            != intent.participation_id
        || event_profile_id != Some(intent.profile_id)
        || raw.request_operation != "JOIN"
        || WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(raw.request_workspace_id)?))
            != intent.workspace_id
        || Uuid::from_bytes(wire::id(raw.request_team_id)?) != intent.team_id
        || raw.request_membership_state != "JOINED"
        || request_participation_id != Some(intent.participation_id)
        || raw
            .request_membership_revision
            .and_then(|value| u64::try_from(value).ok())
            != Some(intent.membership_revision)
        || raw.request_observed_at_ms != raw.event_observed_at_ms
        || raw.request_observed_at_ms != intent.recorded_at.get()
        || Uuid::from_bytes(wire::id(raw.target_participation_id)?)
            != intent.participation_id.into_uuid()
        || Uuid::from_bytes(wire::id(raw.target_profile_id)?) != intent.profile_id
        || u64::try_from(raw.event_membership_revision).ok() != Some(intent.membership_revision)
        || raw.event_observed_at_ms != intent.recorded_at.get()
        || raw.event_state != "JOINED"
    {
        return Err(OperationsError::Corrupt("Team expansion slot use evidence"));
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct CapacitySnapshotWrite<'a> {
    request_id: Uuid,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    expected_capacity_revision: u64,
    capacity_revision: u64,
    expected_team_revision: u64,
    result_team_revision: u64,
    leadership_revision: i64,
    capacity: TeamMemberCapacity,
    actor: &'a TeamExpansionActor,
    recorded_at: TimestampMillis,
    operation: &'static str,
}

fn write_capacity_snapshot(
    transaction: &Transaction<'_>,
    write: CapacitySnapshotWrite<'_>,
) -> Result<TeamCapacitySnapshot, OperationsError> {
    let CapacitySnapshotWrite {
        request_id,
        workspace_id,
        team_id,
        expected_capacity_revision,
        capacity_revision,
        expected_team_revision,
        result_team_revision,
        leadership_revision,
        capacity,
        actor,
        recorded_at,
        operation,
    } = write;
    if request_id.is_nil() || team_id.is_nil() {
        return Err(OperationsError::Conflict("team capacity identity"));
    }
    let revision = i64::try_from(capacity_revision)
        .map_err(|_| OperationsError::Bounds("team capacity revision"))?;
    let expected_revision = i64::try_from(expected_capacity_revision)
        .map_err(|_| OperationsError::Bounds("team capacity revision"))?;
    let expected_team_revision = i64::try_from(expected_team_revision)
        .map_err(|_| OperationsError::Bounds("team revision"))?;
    let result_team_revision = i64::try_from(result_team_revision)
        .map_err(|_| OperationsError::Bounds("team revision"))?;
    transaction.execute(
        "INSERT INTO ops_team_capacity_events(request_id,workspace_id,team_id,capacity_revision,expected_capacity_revision,expected_team_revision,result_team_revision,max_members,operation,actor_activity_id,actor_agent_id,actor_session_id,actor_participation_id,actor_profile_id,actor_participant_id,actor_display_name,recorded_at_ms,leadership_revision) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)",
        params![
            request_id.as_bytes(), workspace_id.into_uuid().as_bytes(), team_id.as_bytes(),
            revision, expected_revision, expected_team_revision, result_team_revision, i64::from(capacity.get()), operation,
            actor.activity_id.into_uuid().as_bytes(), actor.agent_id.into_uuid().as_bytes(),
            actor.session_id.into_uuid().as_bytes(), actor.participation_id.into_uuid().as_bytes(),
            actor.profile_id.as_bytes(), actor.participant_id.as_bytes(), actor.display_name,
            recorded_at.get(), leadership_revision,
        ],
    )?;
    Ok(TeamCapacitySnapshot {
        workspace_id,
        team_id,
        max_members: capacity,
        revision: capacity_revision,
        leadership_revision,
        recorded_at,
        actor: actor.clone(),
    })
}

fn set_team_capacity(
    connection: &mut Connection,
    command: &TeamCapacitySetCommand,
    trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
) -> Result<TeamCapacityReceipt, OperationsError> {
    validate_non_nil(command.request_id, "team capacity request identity")?;
    validate_non_nil(command.team_id, "team capacity Team identity")?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    teams::ensure_verified_workspace(&transaction, command.workspace_id)?;
    let resolved = resolve_actor(&transaction, command.workspace_id, command.actor)?;
    let actor = TeamExpansionActor::from_resolved(command.actor, &resolved)?;

    if let Some(raw) =
        load_capacity_receipt_raw(&transaction, command.workspace_id, command.request_id)?
    {
        let existing = raw.into_receipt(command.workspace_id, command.team_id, true)?;
        if capacity_receipt_matches(&existing, command, &actor) {
            transaction.commit()?;
            return Ok(existing);
        }
        return Err(OperationsError::Conflict("team capacity request identity"));
    }
    if capacity_request_id_exists(&transaction, command.request_id)? {
        return Err(OperationsError::Conflict("team capacity request identity"));
    }

    let receipt = apply_team_capacity_set(&transaction, command, &actor, trusted_now)?;
    transaction
        .commit()
        .map_err(|source| OperationsError::OperationCommitUnconfirmed {
            workspace_id: command.workspace_id,
            request_id: command.request_id,
            source,
        })?;
    Ok(receipt)
}

fn apply_team_capacity_set(
    transaction: &Transaction<'_>,
    command: &TeamCapacitySetCommand,
    actor: &TeamExpansionActor,
    trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
) -> Result<TeamCapacityReceipt, OperationsError> {
    let authority = current_leadership_authority_in_transaction(
        transaction,
        LeadershipScope::workspace(command.workspace_id),
    )?
    .ok_or(OperationsError::Conflict(
        "team capacity requires current Workspace Orchestrator",
    ))?;
    if authority.participant_id() != actor.participant_id
        || authority.leadership_revision() != command.expected_leadership_revision
    {
        return Err(OperationsError::Conflict(
            "team capacity Workspace authority changed",
        ));
    }

    let (team_revision, active) =
        team_revision_and_active(transaction, command.workspace_id, command.team_id)?;
    if !active || team_revision != command.expected_team_revision {
        return Err(OperationsError::Conflict("team capacity Team revision"));
    }
    let current = load_current_capacity(transaction, command.workspace_id, command.team_id)?;
    let capacity_revision = current.as_ref().map_or(0, |snapshot| snapshot.revision);
    if capacity_revision != command.expected_capacity_revision {
        return Err(OperationsError::Conflict("team capacity revision"));
    }
    let occupied = full_team_occupancy(transaction, command.workspace_id, command.team_id)?;
    if usize::from(command.capacity.get()) < occupied {
        return Err(OperationsError::Bounds(
            "team capacity below committed and held occupancy",
        ));
    }
    let recorded_at = trusted_now()?;
    ensure_source_time_not_regressed(
        transaction,
        command.workspace_id,
        command.team_id,
        recorded_at,
    )?;
    if current.is_some_and(|snapshot| recorded_at < snapshot.recorded_at) {
        return Err(OperationsError::Conflict("team capacity clock regression"));
    }
    let next_capacity_revision = capacity_revision
        .checked_add(1)
        .ok_or(OperationsError::Bounds("team capacity revision"))?;
    let next_team_revision = team_revision
        .checked_add(1)
        .ok_or(OperationsError::Bounds("team revision"))?;
    let updated = transaction.execute(
        "UPDATE ops_teams SET revision=?1,updated_at_ms=?2 WHERE workspace_id=?3 AND team_id=?4 AND revision=?5 AND state='ACTIVE'",
        params![
            i64::try_from(next_team_revision).map_err(|_| OperationsError::Bounds("team revision"))?,
            recorded_at.get(),
            command.workspace_id.into_uuid().as_bytes(),
            command.team_id.as_bytes(),
            i64::try_from(team_revision).map_err(|_| OperationsError::Bounds("team revision"))?,
        ],
    )?;
    if updated != 1 {
        return Err(OperationsError::Conflict("team capacity Team revision"));
    }
    let snapshot = write_capacity_snapshot(
        transaction,
        CapacitySnapshotWrite {
            request_id: command.request_id,
            workspace_id: command.workspace_id,
            team_id: command.team_id,
            expected_capacity_revision: capacity_revision,
            capacity_revision: next_capacity_revision,
            expected_team_revision: team_revision,
            result_team_revision: next_team_revision,
            leadership_revision: command.expected_leadership_revision,
            capacity: command.capacity,
            actor,
            recorded_at,
            operation: "SET",
        },
    )?;
    let receipt = TeamCapacityReceipt {
        request_id: command.request_id,
        snapshot,
        expected_team_revision: team_revision,
        expected_capacity_revision: capacity_revision,
        result_team_revision: next_team_revision,
        leadership_revision: command.expected_leadership_revision,
        actor: actor.clone(),
        replayed: false,
    };
    Ok(receipt)
}

fn capacity_receipt_matches(
    receipt: &TeamCapacityReceipt,
    command: &TeamCapacitySetCommand,
    actor: &TeamExpansionActor,
) -> bool {
    receipt.request_id == command.request_id
        && receipt.snapshot.workspace_id == command.workspace_id
        && receipt.snapshot.team_id == command.team_id
        && receipt.expected_team_revision == command.expected_team_revision
        && receipt.expected_capacity_revision == command.expected_capacity_revision
        && receipt.leadership_revision() == command.expected_leadership_revision
        && receipt.snapshot.max_members == command.capacity
        && actor_invocation_same(&receipt.snapshot.actor, actor)
}

fn read_capacity_receipt(
    connection: &Connection,
    workspace_id: WorkspaceId,
    request_id: Uuid,
    actor_input: OperationalActor,
) -> Result<Option<TeamCapacityReceipt>, OperationsError> {
    let transaction = connection.unchecked_transaction()?;
    teams::ensure_verified_workspace(&transaction, workspace_id)?;
    let resolved = resolve_actor(&transaction, workspace_id, actor_input)?;
    let reader = TeamExpansionActor::from_resolved(actor_input, &resolved)?;
    let is_current_orchestrator = current_leadership_authority_in_transaction(
        &transaction,
        LeadershipScope::workspace(workspace_id),
    )?
    .is_some_and(|authority| authority.participant_id() == reader.participant_id);
    let authorized = is_current_orchestrator
        || capacity_receipt_actor_matches_reader(&transaction, workspace_id, request_id, &reader)?;
    if !authorized {
        transaction.commit()?;
        return Ok(None);
    }
    let Some(raw) = load_capacity_receipt_raw(&transaction, workspace_id, request_id)? else {
        transaction.commit()?;
        return Ok(None);
    };
    let team_id = Uuid::from_bytes(wire::id(raw.team_id.clone())?);
    let receipt = raw.into_receipt(workspace_id, team_id, false)?;
    transaction.commit()?;
    Ok(Some(receipt))
}

fn capacity_receipt_actor_matches_reader(
    connection: &Connection,
    workspace_id: WorkspaceId,
    request_id: Uuid,
    reader: &TeamExpansionActor,
) -> Result<bool, OperationsError> {
    let actor: Option<(Value, Value)> = connection
        .query_row(
            "SELECT actor_participation_id,actor_profile_id FROM ops_team_capacity_events WHERE workspace_id=?1 AND request_id=?2",
            params![workspace_id.into_uuid().as_bytes(), request_id.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    Ok(actor.is_some_and(|(participation, profile)| {
        stored_profile_matches_reader(participation, profile, reader)
    }))
}

fn validate_non_nil(value: Uuid, label: &'static str) -> Result<(), OperationsError> {
    if value.is_nil() {
        Err(OperationsError::Conflict(label))
    } else {
        Ok(())
    }
}

fn load_current_capacity(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<Option<TeamCapacitySnapshot>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT capacity_revision,max_members,recorded_at_ms,actor_activity_id,actor_agent_id,actor_session_id,actor_participation_id,actor_profile_id,actor_participant_id,actor_display_name,leadership_revision FROM ops_team_capacity_events WHERE workspace_id=?1 AND team_id=?2 ORDER BY capacity_revision DESC LIMIT 1",
            params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()],
            decode_capacity_raw,
        )
        .optional()?;
    raw.map(|raw| raw.into_snapshot(workspace_id, team_id))
        .transpose()
}

struct CapacityRaw {
    revision: i64,
    leadership_revision: i64,
    max_members: i64,
    recorded_at_ms: i64,
    actor_activity_id: Vec<u8>,
    actor_agent_id: Vec<u8>,
    actor_session_id: Vec<u8>,
    actor_participation_id: Vec<u8>,
    actor_profile_id: Vec<u8>,
    actor_participant_id: Vec<u8>,
    actor_display_name: String,
}

fn decode_capacity_raw(row: &rusqlite::Row<'_>) -> rusqlite::Result<CapacityRaw> {
    Ok(CapacityRaw {
        revision: row.get(0)?,
        max_members: row.get(1)?,
        recorded_at_ms: row.get(2)?,
        actor_activity_id: row.get(3)?,
        actor_agent_id: row.get(4)?,
        actor_session_id: row.get(5)?,
        actor_participation_id: row.get(6)?,
        actor_profile_id: row.get(7)?,
        actor_participant_id: row.get(8)?,
        actor_display_name: row.get(9)?,
        leadership_revision: row.get(10)?,
    })
}

struct CapacityReceiptRaw {
    request_id: Vec<u8>,
    team_id: Vec<u8>,
    expected_capacity_revision: i64,
    expected_team_revision: i64,
    result_team_revision: i64,
    capacity: CapacityRaw,
    operation: String,
}

fn decode_capacity_receipt_raw(row: &rusqlite::Row<'_>) -> rusqlite::Result<CapacityReceiptRaw> {
    Ok(CapacityReceiptRaw {
        request_id: row.get(0)?,
        team_id: row.get(1)?,
        expected_capacity_revision: row.get(2)?,
        expected_team_revision: row.get(3)?,
        result_team_revision: row.get(4)?,
        capacity: CapacityRaw {
            revision: row.get(5)?,
            max_members: row.get(6)?,
            recorded_at_ms: row.get(7)?,
            actor_activity_id: row.get(8)?,
            actor_agent_id: row.get(9)?,
            actor_session_id: row.get(10)?,
            actor_participation_id: row.get(11)?,
            actor_profile_id: row.get(12)?,
            actor_participant_id: row.get(13)?,
            actor_display_name: row.get(14)?,
            leadership_revision: row.get(16)?,
        },
        operation: row.get(15)?,
    })
}

impl CapacityReceiptRaw {
    fn into_receipt(
        self,
        workspace_id: WorkspaceId,
        team_id: Uuid,
        replayed: bool,
    ) -> Result<TeamCapacityReceipt, OperationsError> {
        if self.operation != "CREATE" && self.operation != "SET" {
            return Err(OperationsError::Corrupt("team capacity operation"));
        }
        let request_id = Uuid::from_bytes(wire::id(self.request_id)?);
        let expected_capacity_revision = u64::try_from(self.expected_capacity_revision)
            .map_err(|_| OperationsError::Corrupt("team capacity revision"))?;
        let expected_capacity_next = expected_capacity_revision
            .checked_add(1)
            .ok_or(OperationsError::Corrupt("team capacity revision chain"))?;
        let actor = self.capacity.actor()?;
        let snapshot = self.capacity.into_snapshot(workspace_id, team_id)?;
        if snapshot.revision != expected_capacity_next {
            return Err(OperationsError::Corrupt("team capacity revision chain"));
        }
        let expected_team_revision = u64::try_from(self.expected_team_revision)
            .map_err(|_| OperationsError::Corrupt("team capacity Team revision"))?;
        let expected_team_next =
            expected_team_revision
                .checked_add(1)
                .ok_or(OperationsError::Corrupt(
                    "team capacity Team revision chain",
                ))?;
        let result_team_revision = u64::try_from(self.result_team_revision)
            .map_err(|_| OperationsError::Corrupt("team capacity Team revision"))?;
        if result_team_revision != expected_team_next
            || snapshot.leadership_revision() < 1
            || (self.operation == "CREATE"
                && (expected_capacity_revision != 0
                    || expected_team_revision != 0
                    || snapshot.revision != 1
                    || result_team_revision != 1))
            || (self.operation == "SET" && expected_team_revision == 0)
        {
            return Err(OperationsError::Corrupt("team capacity receipt revision"));
        }
        let leadership_revision = snapshot.leadership_revision();
        Ok(TeamCapacityReceipt {
            request_id,
            snapshot,
            expected_team_revision,
            expected_capacity_revision,
            result_team_revision,
            leadership_revision,
            actor,
            replayed,
        })
    }
}

impl CapacityRaw {
    fn actor(&self) -> Result<TeamExpansionActor, OperationsError> {
        Ok(TeamExpansionActor {
            activity_id: ActivityId::from_uuid(Uuid::from_bytes(wire::id(
                self.actor_activity_id.clone(),
            )?)),
            agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(
                self.actor_agent_id.clone(),
            )?)),
            session_id: AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(
                self.actor_session_id.clone(),
            )?)),
            participation_id: WorkspaceParticipationId::from_uuid(Uuid::from_bytes(wire::id(
                self.actor_participation_id.clone(),
            )?))?,
            profile_id: Uuid::from_bytes(wire::id(self.actor_profile_id.clone())?),
            participant_id: Uuid::from_bytes(wire::id(self.actor_participant_id.clone())?),
            display_name: self.actor_display_name.clone(),
        })
    }

    fn into_snapshot(
        self,
        workspace_id: WorkspaceId,
        team_id: Uuid,
    ) -> Result<TeamCapacitySnapshot, OperationsError> {
        if self.leadership_revision < 1 {
            return Err(OperationsError::Corrupt(
                "team capacity leadership revision",
            ));
        }
        let actor = self.actor()?;
        Ok(TeamCapacitySnapshot {
            workspace_id,
            team_id,
            max_members: TeamMemberCapacity::new(
                u8::try_from(self.max_members)
                    .map_err(|_| OperationsError::Corrupt("team capacity value"))?,
            )?,
            revision: u64::try_from(self.revision)
                .map_err(|_| OperationsError::Corrupt("team capacity revision"))?,
            leadership_revision: self.leadership_revision,
            recorded_at: TimestampMillis::new(self.recorded_at_ms)
                .map_err(|_| OperationsError::Corrupt("team capacity recorded time"))?,
            actor,
        })
    }
}

fn outstanding_slots(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<i64, OperationsError> {
    transaction
        .query_row(
            "SELECT COUNT(*) FROM ops_team_expansion_slots AS slot LEFT JOIN ops_team_expansion_slot_uses AS used ON used.decision_id=slot.decision_id AND used.ordinal=slot.ordinal WHERE slot.workspace_id=?1 AND slot.team_id=?2 AND used.decision_id IS NULL",
            params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

fn joined_and_inbound_occupancy(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<usize, OperationsError> {
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM (SELECT participation_id AS subject_id FROM ops_team_members WHERE workspace_id=?1 AND team_id=?2 AND state='JOINED' UNION SELECT target_participation_id AS subject_id FROM ops_temporary_dispatch_reservations WHERE workspace_id=?1 AND receiving_team_id=?2 AND state IN('RESERVED','SOURCE_READY','ACTIVE','DRAINING'))",
        params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()],
        |row| row.get(0),
    )?;
    usize::try_from(count).map_err(|_| OperationsError::Corrupt("team capacity count"))
}

fn full_team_occupancy(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<usize, OperationsError> {
    joined_and_inbound_occupancy(connection, workspace_id, team_id)?
        .checked_add(
            usize::try_from(outstanding_slots_sql(connection, workspace_id, team_id)?)
                .map_err(|_| OperationsError::Corrupt("team expansion capacity hold"))?,
        )
        .ok_or(OperationsError::Bounds("team capacity count"))
}

fn outstanding_slots_sql(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<i64, OperationsError> {
    connection
        .query_row(
            "SELECT COUNT(*) FROM ops_team_expansion_slots AS slot LEFT JOIN ops_team_expansion_slot_uses AS used ON used.decision_id=slot.decision_id AND used.ordinal=slot.ordinal WHERE slot.workspace_id=?1 AND slot.team_id=?2 AND used.decision_id IS NULL",
            params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

fn actor_same(left: &TeamExpansionActor, right: &TeamExpansionActor) -> bool {
    left.participation_id == right.participation_id && left.profile_id == right.profile_id
}

fn actor_invocation_same(left: &TeamExpansionActor, right: &TeamExpansionActor) -> bool {
    actor_same(left, right)
        && left.activity_id == right.activity_id
        && left.agent_id == right.agent_id
        && left.session_id == right.session_id
        && left.participant_id == right.participant_id
}

fn validate_actor(
    connection: &Connection,
    workspace_id: WorkspaceId,
    input: OperationalActor,
) -> Result<(TeamExpansionActor, ResolvedOperationalActor), OperationsError> {
    let resolved = resolve_actor(connection, workspace_id, input)?;
    let actor = TeamExpansionActor::from_resolved(input, &resolved)?;
    Ok((actor, resolved))
}
