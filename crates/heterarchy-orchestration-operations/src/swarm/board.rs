//! Swarm runs, area channels, member rosters, two-phase watermarks,
//! single-flight coalesced doorbells, and workspace-global message overlay.

mod codecs;
mod convergence;
mod members_and_turns;
mod messages_and_doorbells;
mod runs_and_channels;

pub(super) use codecs::{
    load_members_by_run, load_run_by_id, require_member_by_id, require_run_by_id,
};

use forge_domain::{DagNodeId, DagRevisionNumber, GoalId, WorkspaceId};
use heterarchy_orchestration_sessions::AgentProfileId;
use uuid::Uuid;

use super::{
    MAX_SWARM_CHANNEL_PURPOSE_BYTES, MAX_SWARM_MEMBERS_PER_RUN, MAX_SWARM_SPAWN_DEPTH,
    MAX_SWARM_TOTAL_TURNS, MAX_SWARM_VERIFIER_QUORUM, SwarmCommitOid, SwarmMemberIdentity,
    check_timestamp_non_negative, validate_channel_name, validate_non_nil_uuid,
    validate_non_zero_digest, validate_roster_name, validate_title, validate_workspace_id,
};
use crate::{ForgeWorktreeRef, MessageContext, OperationsError, validate_chat_text};

const SWARM_RUN_ID_DOMAIN: &[u8] = b"heterarchy.orchestration.swarm.run_id.v1\0";
const SWARM_MAIN_CHANNEL_ID_DOMAIN: &[u8] = b"heterarchy.orchestration.swarm.main_channel_id.v1\0";
const SWARM_RUN_DIGEST_DOMAIN: &[u8] = b"heterarchy.orchestration.swarm.run_create.v1\0";
const MAX_DOORBELL_READ_LIMIT: i64 = 4_096;
const MAX_SUBSCRIPTION_READ_LIMIT: i64 = 4_096;

/// Execution mode of a Swarm run: standalone workspace swarm or scoped DAG-node micro-swarm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwarmRunMode {
    Standalone,
    DagNodeHybrid {
        goal_id: GoalId,
        dag_revision: DagRevisionNumber,
        node_id: DagNodeId,
        declared_write_manifest_blake3: [u8; 32],
    },
}

impl SwarmRunMode {
    const fn mode_kind(self) -> i64 {
        match self {
            Self::Standalone => 0,
            Self::DagNodeHybrid { .. } => 1,
        }
    }

    fn validate(&self) -> Result<(), OperationsError> {
        if let Self::DagNodeHybrid {
            goal_id,
            dag_revision,
            node_id,
            declared_write_manifest_blake3,
        } = self
        {
            validate_non_nil_uuid(goal_id.into_uuid(), "swarm goal_id")?;
            validate_non_nil_uuid(node_id.into_uuid(), "swarm node_id")?;
            if dag_revision.get() < 1 {
                return Err(OperationsError::Bounds("swarm dag_revision"));
            }
            validate_non_zero_digest(
                *declared_write_manifest_blake3,
                "swarm declared_write_manifest_blake3",
            )?;
        }
        Ok(())
    }
}

/// Durable lifecycle state of a Swarm run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SwarmRunState {
    Active,
    Converging,
    CandidateReady,
    Settled,
    Aborted,
}

impl SwarmRunState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::Converging => "CONVERGING",
            Self::CandidateReady => "CANDIDATE_READY",
            Self::Settled => "SETTLED",
            Self::Aborted => "ABORTED",
        }
    }

    /// Parse a canonical uppercase state label.
    ///
    /// # Errors
    /// Rejects unknown state strings.
    pub fn parse(raw: &str) -> Result<Self, OperationsError> {
        match raw {
            "ACTIVE" => Ok(Self::Active),
            "CONVERGING" => Ok(Self::Converging),
            "CANDIDATE_READY" => Ok(Self::CandidateReady),
            "SETTLED" => Ok(Self::Settled),
            "ABORTED" => Ok(Self::Aborted),
            _ => Err(OperationsError::Corrupt("swarm run state")),
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Settled | Self::Aborted)
    }
}

/// Authority role of a member in a Swarm roster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SwarmRoleKind {
    Coordinator,
    Worker,
    Helper,
    Verifier,
    Monitor,
}

impl SwarmRoleKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Coordinator => "COORDINATOR",
            Self::Worker => "WORKER",
            Self::Helper => "HELPER",
            Self::Verifier => "VERIFIER",
            Self::Monitor => "MONITOR",
        }
    }

    /// Parse a canonical uppercase role label.
    ///
    /// # Errors
    /// Rejects unknown role strings.
    pub fn parse(raw: &str) -> Result<Self, OperationsError> {
        match raw {
            "COORDINATOR" => Ok(Self::Coordinator),
            "WORKER" => Ok(Self::Worker),
            "HELPER" => Ok(Self::Helper),
            "VERIFIER" => Ok(Self::Verifier),
            "MONITOR" => Ok(Self::Monitor),
            _ => Err(OperationsError::Corrupt("swarm role_kind")),
        }
    }
}

/// Worktree isolation mode assigned to a Swarm member.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SwarmWorkspaceMode {
    Inherit,
    Share,
    Branch,
}

impl SwarmWorkspaceMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Inherit => "INHERIT",
            Self::Share => "SHARE",
            Self::Branch => "BRANCH",
        }
    }

    /// Parse a canonical uppercase workspace-mode label.
    ///
    /// # Errors
    /// Rejects unknown workspace-mode strings.
    pub fn parse(raw: &str) -> Result<Self, OperationsError> {
        match raw {
            "INHERIT" => Ok(Self::Inherit),
            "SHARE" => Ok(Self::Share),
            "BRANCH" => Ok(Self::Branch),
            _ => Err(OperationsError::Corrupt("swarm workspace_mode")),
        }
    }
}

/// Durable execution/wakeup state of a Swarm member.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SwarmMemberState {
    Waiting,
    WakeQueued,
    Working,
    SuspendedQuota,
    Stopped,
    Killed,
}

impl SwarmMemberState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Waiting => "WAITING",
            Self::WakeQueued => "WAKE_QUEUED",
            Self::Working => "WORKING",
            Self::SuspendedQuota => "SUSPENDED_QUOTA",
            Self::Stopped => "STOPPED",
            Self::Killed => "KILLED",
        }
    }

    /// Parse a canonical uppercase member state label.
    ///
    /// # Errors
    /// Rejects unknown state strings.
    pub fn parse(raw: &str) -> Result<Self, OperationsError> {
        match raw {
            "WAITING" => Ok(Self::Waiting),
            "WAKE_QUEUED" => Ok(Self::WakeQueued),
            "WORKING" => Ok(Self::Working),
            "SUSPENDED_QUOTA" => Ok(Self::SuspendedQuota),
            "STOPPED" => Ok(Self::Stopped),
            "KILLED" => Ok(Self::Killed),
            _ => Err(OperationsError::Corrupt("swarm member_state")),
        }
    }
}

/// Cause recorded for a durable Swarm wakeup doorbell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SwarmDoorbellReason {
    DirectMention,
    ChannelMention,
    AllMention,
    SystemStall,
    SystemLivelock,
    ManualWake,
}

impl SwarmDoorbellReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DirectMention => "DIRECT_MENTION",
            Self::ChannelMention => "CHANNEL_MENTION",
            Self::AllMention => "ALL_MENTION",
            Self::SystemStall => "SYSTEM_STALL",
            Self::SystemLivelock => "SYSTEM_LIVELOCK",
            Self::ManualWake => "MANUAL_WAKE",
        }
    }

    /// Parse a canonical uppercase doorbell reason label.
    ///
    /// # Errors
    /// Rejects unknown reason strings.
    pub fn parse(raw: &str) -> Result<Self, OperationsError> {
        match raw {
            "DIRECT_MENTION" => Ok(Self::DirectMention),
            "CHANNEL_MENTION" => Ok(Self::ChannelMention),
            "ALL_MENTION" => Ok(Self::AllMention),
            "SYSTEM_STALL" => Ok(Self::SystemStall),
            "SYSTEM_LIVELOCK" => Ok(Self::SystemLivelock),
            "MANUAL_WAKE" => Ok(Self::ManualWake),
            _ => Err(OperationsError::Corrupt("swarm doorbell reason")),
        }
    }

    const fn precedence(self) -> u8 {
        match self {
            Self::DirectMention => 5,
            Self::SystemLivelock => 4,
            Self::SystemStall => 3,
            Self::AllMention => 2,
            Self::ChannelMention => 1,
            Self::ManualWake => 0,
        }
    }
}

/// Durable Swarm run record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmRunRecord {
    swarm_run_id: Uuid,
    workspace_id: WorkspaceId,
    title: String,
    mode: SwarmRunMode,
    state: SwarmRunState,
    max_members: u32,
    max_spawn_depth: u8,
    max_total_turns: u64,
    total_turns_used: u64,
    required_verifier_quorum: u32,
    allow_worker_all_mention: bool,
    baseline_commit_oid: SwarmCommitOid,
    baseline_tree_blake3: [u8; 32],
    converged_commit_oid: Option<SwarmCommitOid>,
    converged_tree_blake3: Option<[u8; 32]>,
    converged_diff_digest: Option<[u8; 32]>,
    stall_nudge_count: u32,
    settlement_request_id: Option<Uuid>,
    revision: u64,
    created_at_ms: i64,
    updated_at_ms: i64,
    replayed: bool,
}

impl SwarmRunRecord {
    #[must_use]
    pub const fn swarm_run_id(&self) -> Uuid {
        self.swarm_run_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub fn title(&self) -> &str {
        &self.title
    }

    #[must_use]
    pub const fn mode(&self) -> SwarmRunMode {
        self.mode
    }

    #[must_use]
    pub const fn state(&self) -> SwarmRunState {
        self.state
    }

    #[must_use]
    pub const fn max_members(&self) -> u32 {
        self.max_members
    }

    #[must_use]
    pub const fn max_spawn_depth(&self) -> u8 {
        self.max_spawn_depth
    }

    #[must_use]
    pub const fn max_total_turns(&self) -> u64 {
        self.max_total_turns
    }

    #[must_use]
    pub const fn total_turns_used(&self) -> u64 {
        self.total_turns_used
    }

    #[must_use]
    pub const fn required_verifier_quorum(&self) -> u32 {
        self.required_verifier_quorum
    }

    #[must_use]
    pub const fn allow_worker_all_mention(&self) -> bool {
        self.allow_worker_all_mention
    }

    #[must_use]
    pub const fn baseline_commit_oid(&self) -> &SwarmCommitOid {
        &self.baseline_commit_oid
    }

    #[must_use]
    pub const fn baseline_tree_blake3(&self) -> [u8; 32] {
        self.baseline_tree_blake3
    }

    #[must_use]
    pub const fn converged_commit_oid(&self) -> Option<&SwarmCommitOid> {
        self.converged_commit_oid.as_ref()
    }

    #[must_use]
    pub const fn converged_tree_blake3(&self) -> Option<[u8; 32]> {
        self.converged_tree_blake3
    }

    #[must_use]
    pub const fn converged_diff_digest(&self) -> Option<[u8; 32]> {
        self.converged_diff_digest
    }

    #[must_use]
    pub const fn stall_nudge_count(&self) -> u32 {
        self.stall_nudge_count
    }

    #[must_use]
    pub const fn settlement_request_id(&self) -> Option<Uuid> {
        self.settlement_request_id
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub const fn created_at_ms(&self) -> i64 {
        self.created_at_ms
    }

    #[must_use]
    pub const fn updated_at_ms(&self) -> i64 {
        self.updated_at_ms
    }

    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Durable Swarm channel record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmChannelRecord {
    channel_id: Uuid,
    swarm_run_id: Uuid,
    workspace_id: WorkspaceId,
    name: String,
    purpose: Option<String>,
    created_by_member_id: Option<Uuid>,
    created_at_ms: i64,
}

impl SwarmChannelRecord {
    #[must_use]
    pub const fn channel_id(&self) -> Uuid {
        self.channel_id
    }

    #[must_use]
    pub const fn swarm_run_id(&self) -> Uuid {
        self.swarm_run_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn purpose(&self) -> Option<&str> {
        self.purpose.as_deref()
    }

    #[must_use]
    pub const fn created_by_member_id(&self) -> Option<Uuid> {
        self.created_by_member_id
    }

    #[must_use]
    pub const fn created_at_ms(&self) -> i64 {
        self.created_at_ms
    }
}

/// Durable Swarm member record including two-phase watermarks and output receipts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmMemberRecord {
    member_id: Uuid,
    swarm_run_id: Uuid,
    workspace_id: WorkspaceId,
    roster_name: String,
    role_kind: SwarmRoleKind,
    parent_member_id: Option<Uuid>,
    spawn_depth: u8,
    workspace_mode: SwarmWorkspaceMode,
    worktree_path: Option<ForgeWorktreeRef>,
    member_state: SwarmMemberState,
    identity: SwarmMemberIdentity,
    committed_seen_seq: u64,
    inflight_target_seq: u64,
    active_snapshot_id: Option<Uuid>,
    attempt_epoch: u64,
    turns_used: u64,
    ping_pong_peer_id: Option<Uuid>,
    ping_pong_streak: u32,
    last_output_commit_oid: Option<SwarmCommitOid>,
    last_output_tree_blake3: Option<[u8; 32]>,
    created_at_ms: i64,
    updated_at_ms: i64,
}

impl SwarmMemberRecord {
    #[must_use]
    pub const fn member_id(&self) -> Uuid {
        self.member_id
    }

    #[must_use]
    pub const fn swarm_run_id(&self) -> Uuid {
        self.swarm_run_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub fn roster_name(&self) -> &str {
        &self.roster_name
    }

    #[must_use]
    pub const fn role_kind(&self) -> SwarmRoleKind {
        self.role_kind
    }

    #[must_use]
    pub const fn parent_member_id(&self) -> Option<Uuid> {
        self.parent_member_id
    }

    #[must_use]
    pub const fn spawn_depth(&self) -> u8 {
        self.spawn_depth
    }

    #[must_use]
    pub const fn workspace_mode(&self) -> SwarmWorkspaceMode {
        self.workspace_mode
    }

    #[must_use]
    pub const fn worktree_path(&self) -> Option<&ForgeWorktreeRef> {
        self.worktree_path.as_ref()
    }

    #[must_use]
    pub const fn member_state(&self) -> SwarmMemberState {
        self.member_state
    }

    #[must_use]
    pub const fn identity(&self) -> SwarmMemberIdentity {
        self.identity
    }

    #[must_use]
    pub const fn committed_seen_seq(&self) -> u64 {
        self.committed_seen_seq
    }

    #[must_use]
    pub const fn inflight_target_seq(&self) -> u64 {
        self.inflight_target_seq
    }

    #[must_use]
    pub const fn active_snapshot_id(&self) -> Option<Uuid> {
        self.active_snapshot_id
    }

    #[must_use]
    pub const fn attempt_epoch(&self) -> u64 {
        self.attempt_epoch
    }

    #[must_use]
    pub const fn turns_used(&self) -> u64 {
        self.turns_used
    }

    #[must_use]
    pub const fn ping_pong_peer_id(&self) -> Option<Uuid> {
        self.ping_pong_peer_id
    }

    #[must_use]
    pub const fn ping_pong_streak(&self) -> u32 {
        self.ping_pong_streak
    }

    #[must_use]
    pub const fn last_output_commit_oid(&self) -> Option<&SwarmCommitOid> {
        self.last_output_commit_oid.as_ref()
    }

    #[must_use]
    pub const fn last_output_tree_blake3(&self) -> Option<[u8; 32]> {
        self.last_output_tree_blake3
    }

    #[must_use]
    pub const fn created_at_ms(&self) -> i64 {
        self.created_at_ms
    }

    #[must_use]
    pub const fn updated_at_ms(&self) -> i64 {
        self.updated_at_ms
    }
}

/// Durable pending doorbell entry targeting a Swarm member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwarmDoorbellRecord {
    swarm_run_id: Uuid,
    target_member_id: Uuid,
    sequence: u64,
    reason: SwarmDoorbellReason,
    enqueued_at_ms: i64,
}

impl SwarmDoorbellRecord {
    #[must_use]
    pub const fn swarm_run_id(&self) -> Uuid {
        self.swarm_run_id
    }

    #[must_use]
    pub const fn target_member_id(&self) -> Uuid {
        self.target_member_id
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub const fn reason(&self) -> SwarmDoorbellReason {
        self.reason
    }

    #[must_use]
    pub const fn enqueued_at_ms(&self) -> i64 {
        self.enqueued_at_ms
    }
}

/// Consistent read-snapshot of a Swarm run, its channels, members, subscriptions, and pending doorbells.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmRunSnapshot {
    run: SwarmRunRecord,
    channels: Vec<SwarmChannelRecord>,
    members: Vec<SwarmMemberRecord>,
    subscriptions: Vec<(Uuid, Uuid)>,
    pending_doorbells: Vec<SwarmDoorbellRecord>,
}

impl SwarmRunSnapshot {
    #[must_use]
    pub const fn run(&self) -> &SwarmRunRecord {
        &self.run
    }

    #[must_use]
    pub fn channels(&self) -> &[SwarmChannelRecord] {
        &self.channels
    }

    #[must_use]
    pub fn members(&self) -> &[SwarmMemberRecord] {
        &self.members
    }

    #[must_use]
    pub fn subscriptions(&self) -> &[(Uuid, Uuid)] {
        &self.subscriptions
    }

    #[must_use]
    pub fn pending_doorbells(&self) -> &[SwarmDoorbellRecord] {
        &self.pending_doorbells
    }
}

/// One Swarm board message overlaid onto `ops_workspace_messages(workspace_id, sequence)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmBoardMessage {
    message_id: Uuid,
    workspace_id: WorkspaceId,
    sequence: u64,
    swarm_run_id: Uuid,
    channel_id: Uuid,
    channel_name: String,
    author_member_id: Option<Uuid>,
    author_roster_name: String,
    author_epoch: Option<u64>,
    body: String,
    submitted_at_ms: i64,
}

impl SwarmBoardMessage {
    #[must_use]
    pub const fn message_id(&self) -> Uuid {
        self.message_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub const fn swarm_run_id(&self) -> Uuid {
        self.swarm_run_id
    }

    #[must_use]
    pub const fn channel_id(&self) -> Uuid {
        self.channel_id
    }

    #[must_use]
    pub fn channel_name(&self) -> &str {
        &self.channel_name
    }

    #[must_use]
    pub const fn author_member_id(&self) -> Option<Uuid> {
        self.author_member_id
    }

    #[must_use]
    pub fn author_roster_name(&self) -> &str {
        &self.author_roster_name
    }

    #[must_use]
    pub const fn author_epoch(&self) -> Option<u64> {
        self.author_epoch
    }

    #[must_use]
    pub fn body(&self) -> &str {
        &self.body
    }

    #[must_use]
    pub const fn submitted_at_ms(&self) -> i64 {
        self.submitted_at_ms
    }
}

/// Outcome of posting a Swarm board message and firing mention doorbells.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmPostMessageOutcome {
    message: SwarmBoardMessage,
    woken_member_ids: Vec<Uuid>,
    coalesced_doorbell_member_ids: Vec<Uuid>,
    replayed: bool,
}

impl SwarmPostMessageOutcome {
    #[must_use]
    pub const fn message(&self) -> &SwarmBoardMessage {
        &self.message
    }

    #[must_use]
    pub fn woken_member_ids(&self) -> &[Uuid] {
        &self.woken_member_ids
    }

    #[must_use]
    pub fn coalesced_doorbell_member_ids(&self) -> &[Uuid] {
        &self.coalesced_doorbell_member_ids
    }

    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Bounded incremental page of Swarm board messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmBoardPage {
    messages: Vec<SwarmBoardMessage>,
    next_after_sequence: Option<u64>,
    has_more: bool,
}

impl SwarmBoardPage {
    #[must_use]
    pub fn messages(&self) -> &[SwarmBoardMessage] {
        &self.messages
    }

    #[must_use]
    pub const fn next_after_sequence(&self) -> Option<u64> {
        self.next_after_sequence
    }

    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }
}

/// Coalesced wakeup message batch claimed under an in-flight snapshot fence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmWakeupBatch {
    swarm_run_id: Uuid,
    member: SwarmMemberRecord,
    snapshot_id: Uuid,
    committed_seen_seq: u64,
    inflight_target_seq: u64,
    messages: Vec<SwarmBoardMessage>,
    total_body_bytes: usize,
}

impl SwarmWakeupBatch {
    #[must_use]
    pub const fn swarm_run_id(&self) -> Uuid {
        self.swarm_run_id
    }

    #[must_use]
    pub const fn member(&self) -> &SwarmMemberRecord {
        &self.member
    }

    #[must_use]
    pub const fn snapshot_id(&self) -> Uuid {
        self.snapshot_id
    }

    #[must_use]
    pub const fn committed_seen_seq(&self) -> u64 {
        self.committed_seen_seq
    }

    #[must_use]
    pub const fn inflight_target_seq(&self) -> u64 {
        self.inflight_target_seq
    }

    #[must_use]
    pub fn messages(&self) -> &[SwarmBoardMessage] {
        &self.messages
    }

    #[must_use]
    pub const fn total_body_bytes(&self) -> usize {
        self.total_body_bytes
    }
}

/// Output commit/tree receipt captured on clean completion of a `Share` or `Branch` member turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmTurnOutput {
    pub commit_oid: SwarmCommitOid,
    pub tree_blake3: [u8; 32],
}

/// Non-clean turn disposition preserving `committed_seen_seq` without losing unread messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwarmTurnFailureDisposition {
    TransientRetry,
    QuotaExceeded,
    Stopped,
}

/// Author of a Swarm board message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwarmMessageAuthor {
    Member(Uuid),
    System,
}

/// Command to create or idempotently replay a Swarm run and its default `main` channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSwarmRunCommand {
    pub request_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub title: String,
    pub mode: SwarmRunMode,
    pub max_members: u32,
    pub max_spawn_depth: u8,
    pub max_total_turns: u64,
    pub required_verifier_quorum: u32,
    pub allow_worker_all_mention: bool,
    pub baseline_commit_oid: SwarmCommitOid,
    pub baseline_tree_blake3: [u8; 32],
    pub created_at_ms: i64,
}

impl CreateSwarmRunCommand {
    fn validate(&self) -> Result<(), OperationsError> {
        validate_non_nil_uuid(self.request_id, "swarm request_id")?;
        validate_workspace_id(self.workspace_id)?;
        validate_title(&self.title)?;
        self.mode.validate()?;
        if !(1..=u32::try_from(MAX_SWARM_MEMBERS_PER_RUN).unwrap_or(256))
            .contains(&self.max_members)
        {
            return Err(OperationsError::Bounds("swarm max_members"));
        }
        if self.max_spawn_depth > MAX_SWARM_SPAWN_DEPTH {
            return Err(OperationsError::Bounds("swarm max_spawn_depth"));
        }
        if !(1..=MAX_SWARM_TOTAL_TURNS).contains(&self.max_total_turns) {
            return Err(OperationsError::Bounds("swarm max_total_turns"));
        }
        if !(1..=MAX_SWARM_VERIFIER_QUORUM).contains(&self.required_verifier_quorum)
            || self.required_verifier_quorum > self.max_members
        {
            return Err(OperationsError::Bounds("swarm required_verifier_quorum"));
        }
        validate_non_zero_digest(self.baseline_tree_blake3, "swarm baseline_tree_blake3")?;
        check_timestamp_non_negative(self.created_at_ms, "swarm created_at_ms")?;
        Ok(())
    }
}

/// Command to add a member to an active Swarm roster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddSwarmMemberCommand {
    pub member_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub swarm_run_id: Uuid,
    pub roster_name: String,
    pub role_kind: SwarmRoleKind,
    pub parent_member_id: Option<Uuid>,
    pub workspace_mode: SwarmWorkspaceMode,
    pub worktree_path: Option<ForgeWorktreeRef>,
    pub identity: SwarmMemberIdentity,
    pub initial_channels: Vec<String>,
    pub added_at_ms: i64,
}

impl AddSwarmMemberCommand {
    fn validate(&self) -> Result<(), OperationsError> {
        validate_non_nil_uuid(self.member_id, "swarm member_id")?;
        validate_workspace_id(self.workspace_id)?;
        validate_non_nil_uuid(self.swarm_run_id, "swarm_run_id")?;
        validate_roster_name(&self.roster_name)?;
        if let Some(parent_id) = self.parent_member_id {
            validate_non_nil_uuid(parent_id, "swarm parent_member_id")?;
            if parent_id == self.member_id {
                return Err(OperationsError::Conflict("member cannot be its own parent"));
            }
        }
        self.identity.validate()?;
        for channel in &self.initial_channels {
            validate_channel_name(channel)?;
        }
        check_timestamp_non_negative(self.added_at_ms, "swarm added_at_ms")?;
        Ok(())
    }
}

/// Command to kill a member and cascade-cancel all descendant helpers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KillSwarmMemberCommand {
    pub workspace_id: WorkspaceId,
    pub swarm_run_id: Uuid,
    pub member_id: Uuid,
    pub killed_at_ms: i64,
}

/// Command to create an area channel inside a Swarm run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSwarmChannelCommand {
    pub channel_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub swarm_run_id: Uuid,
    pub name: String,
    pub purpose: Option<String>,
    pub created_by_member_id: Option<Uuid>,
    pub created_at_ms: i64,
}

impl CreateSwarmChannelCommand {
    fn validate(&self) -> Result<(), OperationsError> {
        validate_non_nil_uuid(self.channel_id, "swarm channel_id")?;
        validate_workspace_id(self.workspace_id)?;
        validate_non_nil_uuid(self.swarm_run_id, "swarm_run_id")?;
        validate_channel_name(&self.name)?;
        if let Some(purpose) = &self.purpose
            && (purpose.trim().is_empty()
                || purpose.len() > MAX_SWARM_CHANNEL_PURPOSE_BYTES
                || purpose.contains('\0'))
        {
            return Err(OperationsError::Bounds("swarm channel purpose"));
        }
        if let Some(creator) = self.created_by_member_id {
            validate_non_nil_uuid(creator, "swarm channel created_by_member_id")?;
        }
        check_timestamp_non_negative(self.created_at_ms, "swarm channel created_at_ms")?;
        Ok(())
    }
}

/// Command to subscribe an active Swarm member to an area channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscribeSwarmChannelCommand {
    pub workspace_id: WorkspaceId,
    pub swarm_run_id: Uuid,
    pub channel_id: Uuid,
    pub member_id: Uuid,
    pub subscribed_at_ms: i64,
}

/// Command to append a Swarm board message, allocate a workspace-global sequence, and trigger doorbells.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostSwarmMessageCommand {
    pub message_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub swarm_run_id: Uuid,
    pub channel_id: Uuid,
    pub author: SwarmMessageAuthor,
    pub body: String,
    pub context: MessageContext,
    pub submitted_at_ms: i64,
}

impl PostSwarmMessageCommand {
    fn validate(&self) -> Result<(), OperationsError> {
        validate_non_nil_uuid(self.message_id, "swarm message_id")?;
        validate_workspace_id(self.workspace_id)?;
        validate_non_nil_uuid(self.swarm_run_id, "swarm_run_id")?;
        validate_non_nil_uuid(self.channel_id, "swarm channel_id")?;
        if let SwarmMessageAuthor::Member(member_id) = self.author {
            validate_non_nil_uuid(member_id, "swarm author_member_id")?;
        }
        validate_chat_text(&self.body)?;
        check_timestamp_non_negative(self.submitted_at_ms, "swarm submitted_at_ms")?;
        Ok(())
    }
}

/// Incremental board read query scoped to a Swarm run and optional channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadSwarmBoardQuery {
    pub workspace_id: WorkspaceId,
    pub swarm_run_id: Uuid,
    pub channel_id: Option<Uuid>,
    pub after_sequence: Option<u64>,
    pub limit: usize,
}

/// Command to claim a bounded wakeup batch and transition `WakeQueued -> Working`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimSwarmWakeupCommand {
    pub workspace_id: WorkspaceId,
    pub swarm_run_id: Uuid,
    pub member_id: Uuid,
    pub snapshot_id: Uuid,
    pub claimed_at_ms: i64,
}

/// Command to commit `committed_seen_seq = inflight_target_seq` upon clean turn completion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteSwarmTurnCommand {
    pub workspace_id: WorkspaceId,
    pub swarm_run_id: Uuid,
    pub member_id: Uuid,
    pub expected_snapshot_id: Uuid,
    pub output: Option<SwarmTurnOutput>,
    pub completed_at_ms: i64,
}

/// Command to release an in-flight turn on failure, timeout, quota exhaustion, or stop while preserving `committed_seen_seq`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FailOrSuspendSwarmTurnCommand {
    pub workspace_id: WorkspaceId,
    pub swarm_run_id: Uuid,
    pub member_id: Uuid,
    pub expected_snapshot_id: Uuid,
    pub disposition: SwarmTurnFailureDisposition,
    pub consume_turn: bool,
    pub observed_at_ms: i64,
}

/// Command to rebind a `SuspendedQuota` member to a new `AgentProfileId` and requeue it cleanly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwitchSwarmMemberProfileCommand {
    pub workspace_id: WorkspaceId,
    pub swarm_run_id: Uuid,
    pub member_id: Uuid,
    pub new_profile_id: AgentProfileId,
    pub updated_at_ms: i64,
}

/// Command for deterministic zero-token stall/livelock nudge or escalation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordSwarmStallNudgeCommand {
    pub message_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub swarm_run_id: Uuid,
    pub channel_id: Uuid,
    pub target_member_id: Uuid,
    pub reason: SwarmDoorbellReason,
    pub body: String,
    pub max_stall_nudges: u32,
    pub observed_at_ms: i64,
}

/// Result of a deterministic stall/livelock nudge check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmStallNudgeOutcome {
    run: SwarmRunRecord,
    message: Option<SwarmBoardMessage>,
    escalated_to_aborted: bool,
}

impl SwarmStallNudgeOutcome {
    #[must_use]
    pub const fn run(&self) -> &SwarmRunRecord {
        &self.run
    }

    #[must_use]
    pub const fn message(&self) -> Option<&SwarmBoardMessage> {
        self.message.as_ref()
    }

    #[must_use]
    pub const fn escalated_to_aborted(&self) -> bool {
        self.escalated_to_aborted
    }
}

/// Command to stop looping Swarm members (`Waiting`/`WakeQueued`/`SuspendedQuota` -> `Stopped`)
/// and escalate a `[livelock]` doorbell to the run's `Coordinator`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BreakSwarmLivelockCommand {
    pub message_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub swarm_run_id: Uuid,
    pub channel_id: Uuid,
    pub looping_member_ids: Vec<Uuid>,
    pub coordinator_member_id: Uuid,
    pub body: String,
    pub observed_at_ms: i64,
}

/// Outcome of breaking a Swarm ping-pong livelock and escalating to `Coordinator`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BreakSwarmLivelockOutcome {
    stopped_members: Vec<SwarmMemberRecord>,
    escalation_message: SwarmBoardMessage,
}

impl BreakSwarmLivelockOutcome {
    #[must_use]
    pub fn stopped_members(&self) -> &[SwarmMemberRecord] {
        &self.stopped_members
    }

    #[must_use]
    pub const fn escalation_message(&self) -> &SwarmBoardMessage {
        &self.escalation_message
    }
}

/// Command to record a converged candidate commit/tree/diff digest and transition to `CandidateReady`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordSwarmConvergenceCommand {
    pub workspace_id: WorkspaceId,
    pub swarm_run_id: Uuid,
    pub converged_commit_oid: SwarmCommitOid,
    pub converged_tree_blake3: [u8; 32],
    pub converged_diff_digest: [u8; 32],
    pub recorded_at_ms: i64,
}

/// Command to settle a `Standalone` Swarm run after verifying the adversarial verifier quorum gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SettleStandaloneSwarmCommand {
    pub request_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub swarm_run_id: Uuid,
    pub settled_at_ms: i64,
}
