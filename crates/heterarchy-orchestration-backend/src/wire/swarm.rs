//! Strongly-typed HTTP wire contracts for Native Swarm coordination, two-phase
//! watermarks, single-flight doorbell coalescing, adversarial verifier panels,
//! convergence gating, and deterministic stall/livelock inspection.
//!
//! Every request, query, and response struct enforces `#[serde(rename_all = "camelCase",
//! deny_unknown_fields)]` so caller typos and extraneous or secret-bearing fields
//! fail closed at the transport boundary before reaching runtime or SQLite catalogs.

use forge_domain::{DagNodeId, DagRevisionNumber, GoalId};
use heterarchy_orchestration_operations::{
    ForgeWorktreeRef, SwarmBoardMessage, SwarmBoardPage, SwarmChannelRecord, SwarmCommitOid,
    SwarmConvergenceGateOutcome, SwarmDoorbellRecord, SwarmMemberIdentity, SwarmMemberRecord,
    SwarmPanelSeatRecord, SwarmPostMessageOutcome, SwarmRoleKind, SwarmRunMode, SwarmRunRecord,
    SwarmRunSnapshot, SwarmTurnFailureDisposition, SwarmTurnOutput, SwarmVerdictRecord,
    SwarmVerificationPanelRecord, SwarmWakeupBatch, SwarmWorkspaceMode,
};
use heterarchy_orchestration_runtime::{
    SwarmBottleneckReason, SwarmLivelockReason, SwarmModelSwitchOutcome, SwarmModelSwitchTarget,
    SwarmMonitorActionReport,
};
use heterarchy_orchestration_sessions::{AgentProfileId, AgentProfileRevision};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::wire::autonomy_loop::{
    ExecutionAuthorWire, ExecutionPreferenceWire, PrincipalIdentityWire, encode_sha256_hex,
    parse_sha256_hex,
};
use crate::{ApiError, RuntimeError, canonical_uuid};

mod dispatch;

pub use dispatch::{
    CommittedExecutionSnapshotWire, DispatchSwarmTurnRequest, DispatchSwarmTurnResponse,
    DispatchedTurnDispositionWire, DispatchedTurnSupervisionWire,
};

/// Maximum JSON request body size accepted by Swarm HTTP endpoints (`128 KiB`).
pub const MAX_SWARM_HTTP_BODY_BYTES: usize = 131_072;

/// Uniform transport error envelope returned for Swarm domain rejections.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwarmErrorResponse {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

impl SwarmErrorResponse {
    #[must_use]
    pub fn permanent(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_owned(),
            message: message.into(),
            retryable: false,
        }
    }
}

/// Coordinates binding a `DagNodeHybrid` micro-swarm to its owning Forge DAG node.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DagNodeHybridTargetWire {
    pub goal_id: String,
    pub dag_revision: i64,
    pub node_id: String,
    pub declared_write_manifest_blake3_hex: String,
}

/// Discriminated Swarm execution mode (`STANDALONE` vs `DAG_NODE_HYBRID`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwarmRunModeWire {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hybrid_target: Option<DagNodeHybridTargetWire>,
}

impl SwarmRunModeWire {
    pub(crate) fn to_domain(&self) -> Result<SwarmRunMode, ApiError> {
        match self.kind.as_str() {
            "standalone" | "STANDALONE" => {
                if self.hybrid_target.is_some() {
                    return Err(ApiError::Runtime(RuntimeError::InvalidInput));
                }
                Ok(SwarmRunMode::Standalone)
            }
            "dag_node_hybrid" | "dagNodeHybrid" | "DAG_NODE_HYBRID" => {
                let target = self
                    .hybrid_target
                    .as_ref()
                    .ok_or(ApiError::Runtime(RuntimeError::InvalidInput))?;
                let goal_id = GoalId::from_uuid(canonical_uuid(&target.goal_id)?);
                let dag_revision = DagRevisionNumber::new(target.dag_revision)
                    .map_err(|_| ApiError::Runtime(RuntimeError::InvalidInput))?;
                let node_id = DagNodeId::from_uuid(canonical_uuid(&target.node_id)?);
                let declared_write_manifest_blake3 =
                    parse_non_zero_digest_hex(&target.declared_write_manifest_blake3_hex)?;
                Ok(SwarmRunMode::DagNodeHybrid {
                    goal_id,
                    dag_revision,
                    node_id,
                    declared_write_manifest_blake3,
                })
            }
            _ => Err(ApiError::Runtime(RuntimeError::InvalidInput)),
        }
    }

    #[must_use]
    pub fn from_domain(mode: SwarmRunMode) -> Self {
        match mode {
            SwarmRunMode::Standalone => Self {
                kind: "STANDALONE".to_owned(),
                hybrid_target: None,
            },
            SwarmRunMode::DagNodeHybrid {
                goal_id,
                dag_revision,
                node_id,
                declared_write_manifest_blake3,
            } => Self {
                kind: "DAG_NODE_HYBRID".to_owned(),
                hybrid_target: Some(DagNodeHybridTargetWire {
                    goal_id: goal_id.into_uuid().to_string(),
                    dag_revision: dag_revision.get(),
                    node_id: node_id.into_uuid().to_string(),
                    declared_write_manifest_blake3_hex: encode_sha256_hex(
                        declared_write_manifest_blake3,
                    ),
                }),
            },
        }
    }
}

/// Request to create or idempotently replay a `Standalone` or `DagNodeHybrid` Swarm run.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateSwarmRunRequest {
    pub request_id: String,
    pub title: String,
    pub mode: SwarmRunModeWire,
    pub max_members: u32,
    pub max_spawn_depth: u8,
    pub max_total_turns: u64,
    pub required_verifier_quorum: u32,
    #[serde(default)]
    pub allow_worker_all_mention: bool,
    pub baseline_commit_oid: String,
    pub baseline_tree_blake3_hex: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<crate::wire::WorkspaceQueryScope>,
}

/// Optional pagination limit when listing Swarm runs in a Workspace.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ListSwarmRunsQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

/// Canonical wire readback of a durable [`SwarmRunRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwarmRunWire {
    pub swarm_run_id: Uuid,
    pub workspace_id: Uuid,
    pub title: String,
    pub mode: SwarmRunModeWire,
    pub state: String,
    pub max_members: u32,
    pub max_spawn_depth: u8,
    pub max_total_turns: u64,
    pub total_turns_used: u64,
    pub required_verifier_quorum: u32,
    pub allow_worker_all_mention: bool,
    pub baseline_commit_oid: String,
    pub baseline_tree_blake3_hex: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub converged_commit_oid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub converged_tree_blake3_hex: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub converged_diff_digest_hex: Option<String>,
    pub stall_nudge_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settlement_request_id: Option<Uuid>,
    pub revision: u64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub replayed: bool,
}

impl From<&SwarmRunRecord> for SwarmRunWire {
    fn from(run: &SwarmRunRecord) -> Self {
        Self {
            swarm_run_id: run.swarm_run_id(),
            workspace_id: run.workspace_id().into_uuid(),
            title: run.title().to_owned(),
            mode: SwarmRunModeWire::from_domain(run.mode()),
            state: run.state().as_str().to_owned(),
            max_members: run.max_members(),
            max_spawn_depth: run.max_spawn_depth(),
            max_total_turns: run.max_total_turns(),
            total_turns_used: run.total_turns_used(),
            required_verifier_quorum: run.required_verifier_quorum(),
            allow_worker_all_mention: run.allow_worker_all_mention(),
            baseline_commit_oid: run.baseline_commit_oid().as_str().to_owned(),
            baseline_tree_blake3_hex: encode_sha256_hex(run.baseline_tree_blake3()),
            converged_commit_oid: run
                .converged_commit_oid()
                .map(|oid| oid.as_str().to_owned()),
            converged_tree_blake3_hex: run.converged_tree_blake3().map(encode_sha256_hex),
            converged_diff_digest_hex: run.converged_diff_digest().map(encode_sha256_hex),
            stall_nudge_count: run.stall_nudge_count(),
            settlement_request_id: run.settlement_request_id(),
            revision: run.revision(),
            created_at_ms: run.created_at_ms(),
            updated_at_ms: run.updated_at_ms(),
            replayed: run.replayed(),
        }
    }
}

/// Response returned when creating or replaying a Swarm run, including its default `main` channel.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateSwarmRunResponse {
    pub run: SwarmRunWire,
    pub main_channel: SwarmChannelWire,
}

/// Bounded list of Swarm runs within a Workspace.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwarmRunListResponse {
    pub runs: Vec<SwarmRunWire>,
}

/// Request to create an isolated area channel inside an active Swarm run.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateSwarmChannelRequest {
    pub channel_id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_member_id: Option<String>,
}

/// Wire projection of a [`SwarmChannelRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwarmChannelWire {
    pub channel_id: Uuid,
    pub swarm_run_id: Uuid,
    pub workspace_id: Uuid,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_member_id: Option<Uuid>,
    pub created_at_ms: i64,
}

impl From<&SwarmChannelRecord> for SwarmChannelWire {
    fn from(channel: &SwarmChannelRecord) -> Self {
        Self {
            channel_id: channel.channel_id(),
            swarm_run_id: channel.swarm_run_id(),
            workspace_id: channel.workspace_id().into_uuid(),
            name: channel.name().to_owned(),
            purpose: channel.purpose().map(str::to_owned),
            created_by_member_id: channel.created_by_member_id(),
            created_at_ms: channel.created_at_ms(),
        }
    }
}

/// Request to subscribe an active Swarm member to an area channel.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SubscribeSwarmChannelRequest {
    pub member_id: String,
}

/// Response returned after subscribing a Swarm member to an area channel.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SubscribeSwarmChannelResponse {
    pub swarm_run_id: Uuid,
    pub channel_id: Uuid,
    pub member_id: Uuid,
    pub newly_subscribed: bool,
}

/// Subscription edge pairing `(channel_id, member_id)` in a Swarm run snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwarmChannelSubscriptionWire {
    pub channel_id: Uuid,
    pub member_id: Uuid,
}

/// Pending doorbell entry awaiting single-flight wakeup claim.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwarmDoorbellWire {
    pub swarm_run_id: Uuid,
    pub target_member_id: Uuid,
    pub sequence: u64,
    pub reason: String,
    pub enqueued_at_ms: i64,
}

impl From<&SwarmDoorbellRecord> for SwarmDoorbellWire {
    fn from(record: &SwarmDoorbellRecord) -> Self {
        Self {
            swarm_run_id: record.swarm_run_id(),
            target_member_id: record.target_member_id(),
            sequence: record.sequence(),
            reason: record.reason().as_str().to_owned(),
            enqueued_at_ms: record.enqueued_at_ms(),
        }
    }
}

/// Three-axis identity projection of a Swarm member.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwarmMemberIdentityWire {
    pub profile_id: Uuid,
    pub participant_id: Uuid,
    pub principal: PrincipalIdentityWire,
}

impl From<SwarmMemberIdentity> for SwarmMemberIdentityWire {
    fn from(identity: SwarmMemberIdentity) -> Self {
        Self {
            profile_id: identity.profile_id().into_uuid(),
            participant_id: identity.participant_id(),
            principal: PrincipalIdentityWire::from_principal(identity.principal()),
        }
    }
}

/// Request to add a `Coordinator`, `Worker`, `Helper`, `Verifier`, or `Monitor` to a Swarm run.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AddSwarmMemberRequest {
    pub member_id: String,
    pub roster_name: String,
    pub role_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_member_id: Option<String>,
    pub workspace_mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree_path: Option<String>,
    pub identity: ExecutionAuthorWire,
    #[serde(default)]
    pub initial_channels: Vec<String>,
}

/// Canonical wire readback of a [`SwarmMemberRecord`], including two-phase watermarks.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwarmMemberWire {
    pub member_id: Uuid,
    pub swarm_run_id: Uuid,
    pub workspace_id: Uuid,
    pub roster_name: String,
    pub role_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_member_id: Option<Uuid>,
    pub spawn_depth: u8,
    pub workspace_mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree_path: Option<String>,
    pub member_state: String,
    pub identity: SwarmMemberIdentityWire,
    pub committed_seen_seq: u64,
    pub inflight_target_seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_snapshot_id: Option<Uuid>,
    pub attempt_epoch: u64,
    pub turns_used: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ping_pong_peer_id: Option<Uuid>,
    pub ping_pong_streak: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_output_commit_oid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_output_tree_blake3_hex: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl From<&SwarmMemberRecord> for SwarmMemberWire {
    fn from(member: &SwarmMemberRecord) -> Self {
        Self {
            member_id: member.member_id(),
            swarm_run_id: member.swarm_run_id(),
            workspace_id: member.workspace_id().into_uuid(),
            roster_name: member.roster_name().to_owned(),
            role_kind: member.role_kind().as_str().to_owned(),
            parent_member_id: member.parent_member_id(),
            spawn_depth: member.spawn_depth(),
            workspace_mode: member.workspace_mode().as_str().to_owned(),
            worktree_path: member.worktree_path().map(|path| path.as_str().to_owned()),
            member_state: member.member_state().as_str().to_owned(),
            identity: SwarmMemberIdentityWire::from(member.identity()),
            committed_seen_seq: member.committed_seen_seq(),
            inflight_target_seq: member.inflight_target_seq(),
            active_snapshot_id: member.active_snapshot_id(),
            attempt_epoch: member.attempt_epoch(),
            turns_used: member.turns_used(),
            ping_pong_peer_id: member.ping_pong_peer_id(),
            ping_pong_streak: member.ping_pong_streak(),
            last_output_commit_oid: member
                .last_output_commit_oid()
                .map(|oid| oid.as_str().to_owned()),
            last_output_tree_blake3_hex: member.last_output_tree_blake3().map(encode_sha256_hex),
            created_at_ms: member.created_at_ms(),
            updated_at_ms: member.updated_at_ms(),
        }
    }
}

/// Explicit empty request body for killing a Swarm member and cascade-cancelling its helpers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KillSwarmMemberRequest {}

/// Response listing the killed target member and any cascade-cancelled descendant helpers.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KillSwarmMemberResponse {
    pub killed_members: Vec<SwarmMemberWire>,
}

/// Request to append a `#seq` message to a Swarm channel and fire `@mention` doorbells.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PostSwarmMessageRequest {
    pub message_id: String,
    /// Optional channel ID; defaults to the run's `main` channel when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_id: Option<String>,
    /// Optional author member ID; `None` posts as `System`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author_member_id: Option<String>,
    pub body: String,
}

/// Query parameters for incremental Swarm board reads (`GET .../messages?sinceSequence=...&channelId=...`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReadSwarmMessagesQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since_sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

/// Canonical wire projection of one [`SwarmBoardMessage`].
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwarmBoardMessageWire {
    pub message_id: Uuid,
    pub workspace_id: Uuid,
    pub sequence: u64,
    pub swarm_run_id: Uuid,
    pub channel_id: Uuid,
    pub channel_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author_member_id: Option<Uuid>,
    pub author_roster_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author_epoch: Option<u64>,
    pub body: String,
    pub submitted_at_ms: i64,
}

impl From<&SwarmBoardMessage> for SwarmBoardMessageWire {
    fn from(msg: &SwarmBoardMessage) -> Self {
        Self {
            message_id: msg.message_id(),
            workspace_id: msg.workspace_id().into_uuid(),
            sequence: msg.sequence(),
            swarm_run_id: msg.swarm_run_id(),
            channel_id: msg.channel_id(),
            channel_name: msg.channel_name().to_owned(),
            author_member_id: msg.author_member_id(),
            author_roster_name: msg.author_roster_name().to_owned(),
            author_epoch: msg.author_epoch(),
            body: msg.body().to_owned(),
            submitted_at_ms: msg.submitted_at_ms(),
        }
    }
}

/// Response returned after appending a Swarm board message and firing `@mention` doorbells.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PostSwarmMessageResponse {
    pub message: SwarmBoardMessageWire,
    pub woken_member_ids: Vec<Uuid>,
    pub coalesced_doorbell_member_ids: Vec<Uuid>,
    pub replayed: bool,
}

impl From<&SwarmPostMessageOutcome> for PostSwarmMessageResponse {
    fn from(outcome: &SwarmPostMessageOutcome) -> Self {
        Self {
            message: SwarmBoardMessageWire::from(outcome.message()),
            woken_member_ids: outcome.woken_member_ids().to_vec(),
            coalesced_doorbell_member_ids: outcome.coalesced_doorbell_member_ids().to_vec(),
            replayed: outcome.replayed(),
        }
    }
}

/// Incremental page of Swarm board messages ordered by monotonic `sequence`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwarmBoardPageResponse {
    pub messages: Vec<SwarmBoardMessageWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_after_sequence: Option<u64>,
    pub has_more: bool,
}

impl From<&SwarmBoardPage> for SwarmBoardPageResponse {
    fn from(page: &SwarmBoardPage) -> Self {
        Self {
            messages: page
                .messages()
                .iter()
                .map(SwarmBoardMessageWire::from)
                .collect(),
            next_after_sequence: page.next_after_sequence(),
            has_more: page.has_more(),
        }
    }
}

/// Request to claim a single-flight coalesced wakeup batch (`<= 32` msgs, `<= 64 KiB`)
/// and transition a `WakeQueued` member into `Working`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClaimSwarmWakeupRequest {
    pub snapshot_id: String,
}

/// Coalesced single-flight wakeup batch bound to `(committed_seen_seq, inflight_target_seq]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwarmWakeupBatchWire {
    pub swarm_run_id: Uuid,
    pub member: SwarmMemberWire,
    pub snapshot_id: Uuid,
    pub committed_seen_seq: u64,
    pub inflight_target_seq: u64,
    pub messages: Vec<SwarmBoardMessageWire>,
    pub total_body_bytes: usize,
}

impl From<&SwarmWakeupBatch> for SwarmWakeupBatchWire {
    fn from(batch: &SwarmWakeupBatch) -> Self {
        Self {
            swarm_run_id: batch.swarm_run_id(),
            member: SwarmMemberWire::from(batch.member()),
            snapshot_id: batch.snapshot_id(),
            committed_seen_seq: batch.committed_seen_seq(),
            inflight_target_seq: batch.inflight_target_seq(),
            messages: batch
                .messages()
                .iter()
                .map(SwarmBoardMessageWire::from)
                .collect(),
            total_body_bytes: batch.total_body_bytes(),
        }
    }
}

/// Request to cleanly complete an in-flight `Working` member turn and commit `committed_seen_seq`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompleteSwarmTurnRequest {
    pub expected_snapshot_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_commit_oid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tree_blake3_hex: Option<String>,
}

impl CompleteSwarmTurnRequest {
    pub(crate) fn to_turn_output(&self) -> Result<Option<SwarmTurnOutput>, ApiError> {
        match (&self.output_commit_oid, &self.output_tree_blake3_hex) {
            (None, None) => Ok(None),
            (Some(commit_raw), Some(tree_hex)) => Ok(Some(SwarmTurnOutput {
                commit_oid: parse_swarm_commit_oid(commit_raw)?,
                tree_blake3: parse_non_zero_digest_hex(tree_hex)?,
            })),
            _ => Err(ApiError::Runtime(RuntimeError::InvalidInput)),
        }
    }
}

/// Request to release an in-flight `Working` member turn on crash, timeout, quota exhaustion,
/// or fatal stop while preserving `committed_seen_seq` unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FailSwarmTurnRequest {
    pub expected_snapshot_id: String,
    pub disposition: String,
    #[serde(default)]
    pub consume_turn: bool,
}

/// Request to switch a `SuspendedQuota` member's model or profile at an attempt boundary
/// without losing unread board messages.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwitchSwarmMemberModelRequest {
    /// Rebind to an existing active `AgentProfileId` when supplied without `new_preference`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_profile_id: Option<String>,
    /// Mutate `profile_id` at `expected_profile_revision` with `new_preference`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_profile_revision: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_preference: Option<ExecutionPreferenceWire>,
}

impl SwitchSwarmMemberModelRequest {
    pub(crate) fn to_target(&self) -> Result<SwarmModelSwitchTarget, ApiError> {
        if let Some(new_profile_raw) = &self.new_profile_id {
            if self.request_id.is_some()
                || self.profile_id.is_some()
                || self.expected_profile_revision.is_some()
                || self.new_preference.is_some()
            {
                return Err(ApiError::Runtime(RuntimeError::InvalidInput));
            }
            let new_profile_id = AgentProfileId::from_uuid(canonical_uuid(new_profile_raw)?)
                .map_err(|_| ApiError::InvalidIdentity)?;
            return Ok(SwarmModelSwitchTarget::RebindActiveProfile { new_profile_id });
        }

        let request_id = canonical_uuid(
            self.request_id
                .as_deref()
                .ok_or(ApiError::Runtime(RuntimeError::InvalidInput))?,
        )?;
        let profile_uuid = canonical_uuid(
            self.profile_id
                .as_deref()
                .ok_or(ApiError::Runtime(RuntimeError::InvalidInput))?,
        )?;
        let profile_id =
            AgentProfileId::from_uuid(profile_uuid).map_err(|_| ApiError::InvalidIdentity)?;
        let expected_rev_raw = self
            .expected_profile_revision
            .ok_or(ApiError::Runtime(RuntimeError::InvalidInput))?;
        let expected_profile_revision = AgentProfileRevision::new(expected_rev_raw)
            .map_err(|_| ApiError::Runtime(RuntimeError::InvalidInput))?;
        let new_preference = self
            .new_preference
            .as_ref()
            .ok_or(ApiError::Runtime(RuntimeError::InvalidInput))?
            .to_execution_preference()
            .map_err(RuntimeError::from)
            .map_err(ApiError::Runtime)?;
        Ok(SwarmModelSwitchTarget::UpdateProfilePreference {
            request_id,
            profile_id,
            expected_profile_revision,
            new_preference,
        })
    }
}

/// Receipt returned after switching a `SuspendedQuota` member's model and requeuing it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwarmModelSwitchResponse {
    pub member: SwarmMemberWire,
    pub profile_id: Uuid,
    pub profile_revision: i64,
    pub resolved_preference: ExecutionPreferenceWire,
}

impl From<&SwarmModelSwitchOutcome> for SwarmModelSwitchResponse {
    fn from(outcome: &SwarmModelSwitchOutcome) -> Self {
        Self {
            member: SwarmMemberWire::from(&outcome.member),
            profile_id: outcome.profile_id.into_uuid(),
            profile_revision: outcome.profile_revision.get(),
            resolved_preference: ExecutionPreferenceWire::from_execution_preference(
                &outcome.resolved_preference,
            ),
        }
    }
}

/// Request to create an explicit lineage-separated Swarm verification panel.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateSwarmVerificationPanelRequest {
    pub request_id: String,
    pub author_member_ids: Vec<String>,
    pub verifier_member_ids: Vec<String>,
    pub required_quorum: u32,
}

/// Seat projection inside a [`SwarmVerificationPanelWire`].
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwarmPanelSeatWire {
    pub member_id: Uuid,
    pub seat_kind: String,
    pub assigned_at_ms: i64,
}

impl From<&SwarmPanelSeatRecord> for SwarmPanelSeatWire {
    fn from(seat: &SwarmPanelSeatRecord) -> Self {
        Self {
            member_id: seat.member_id(),
            seat_kind: seat.seat_kind().as_str().to_owned(),
            assigned_at_ms: seat.assigned_at_ms(),
        }
    }
}

/// Wire projection of a [`SwarmVerificationPanelRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwarmVerificationPanelWire {
    pub panel_id: Uuid,
    pub swarm_run_id: Uuid,
    pub workspace_id: Uuid,
    pub required_quorum: u32,
    pub auto_created: bool,
    pub seats: Vec<SwarmPanelSeatWire>,
    pub created_at_ms: i64,
    pub replayed: bool,
}

impl From<&SwarmVerificationPanelRecord> for SwarmVerificationPanelWire {
    fn from(panel: &SwarmVerificationPanelRecord) -> Self {
        Self {
            panel_id: panel.panel_id(),
            swarm_run_id: panel.swarm_run_id(),
            workspace_id: panel.workspace_id().into_uuid(),
            required_quorum: panel.required_quorum(),
            auto_created: panel.auto_created(),
            seats: panel.seats().iter().map(SwarmPanelSeatWire::from).collect(),
            created_at_ms: panel.created_at_ms(),
            replayed: panel.replayed(),
        }
    }
}

/// Request to record an immutable diff-digest-bound verdict from a lineage-separated `Verifier`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RecordSwarmVerdictRequest {
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub panel_id: Option<String>,
    pub verifier_member_id: String,
    pub outcome: String,
    pub reviewed_diff_digest_hex: String,
    pub rationale: String,
}

/// Wire projection of an immutable [`SwarmVerdictRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwarmVerdictWire {
    pub verdict_id: Uuid,
    pub request_id: Uuid,
    pub panel_id: Uuid,
    pub swarm_run_id: Uuid,
    pub verifier_member_id: Uuid,
    pub outcome: String,
    pub reviewed_diff_digest_hex: String,
    pub rationale: String,
    pub recorded_at_ms: i64,
    pub replayed: bool,
}

impl From<&SwarmVerdictRecord> for SwarmVerdictWire {
    fn from(verdict: &SwarmVerdictRecord) -> Self {
        Self {
            verdict_id: verdict.verdict_id(),
            request_id: verdict.request_id(),
            panel_id: verdict.panel_id(),
            swarm_run_id: verdict.swarm_run_id(),
            verifier_member_id: verdict.verifier_member_id(),
            outcome: verdict.outcome().as_str().to_owned(),
            reviewed_diff_digest_hex: encode_sha256_hex(verdict.reviewed_diff_digest()),
            rationale: verdict.rationale().to_owned(),
            recorded_at_ms: verdict.recorded_at_ms(),
            replayed: verdict.replayed(),
        }
    }
}

/// Consistent full inspection response for `GET /v1/workspaces/{workspace_id}/swarms/{swarm_run_id}`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwarmRunInspectionResponse {
    pub run: SwarmRunWire,
    pub channels: Vec<SwarmChannelWire>,
    pub members: Vec<SwarmMemberWire>,
    pub subscriptions: Vec<SwarmChannelSubscriptionWire>,
    pub pending_doorbells: Vec<SwarmDoorbellWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_panel: Option<SwarmVerificationPanelWire>,
    pub verdicts: Vec<SwarmVerdictWire>,
}

impl SwarmRunInspectionResponse {
    #[must_use]
    pub fn from_parts(
        snapshot: &SwarmRunSnapshot,
        panel: Option<&SwarmVerificationPanelRecord>,
        verdicts: &[SwarmVerdictRecord],
    ) -> Self {
        Self {
            run: SwarmRunWire::from(snapshot.run()),
            channels: snapshot
                .channels()
                .iter()
                .map(SwarmChannelWire::from)
                .collect(),
            members: snapshot
                .members()
                .iter()
                .map(SwarmMemberWire::from)
                .collect(),
            subscriptions: snapshot
                .subscriptions()
                .iter()
                .map(|&(channel_id, member_id)| SwarmChannelSubscriptionWire {
                    channel_id,
                    member_id,
                })
                .collect(),
            pending_doorbells: snapshot
                .pending_doorbells()
                .iter()
                .map(SwarmDoorbellWire::from)
                .collect(),
            verification_panel: panel.map(SwarmVerificationPanelWire::from),
            verdicts: verdicts.iter().map(SwarmVerdictWire::from).collect(),
        }
    }
}

/// Explicit convergence target requested by a caller on `POST .../converge`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwarmConvergenceTarget {
    CandidateReady,
    Settled,
}

/// Request to record converged candidate digests, verify the adversarial verifier quorum gate,
/// and transition `Standalone -> Settled` or `DagNodeHybrid -> CandidateReady`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConvergeSwarmRequest {
    pub request_id: String,
    pub converged_commit_oid: String,
    pub converged_tree_blake3_hex: String,
    pub converged_diff_digest_hex: String,
    /// Optional explicit target (`"CANDIDATE_READY"` or `"SETTLED"`).
    /// Setting `"SETTLED"` on a `DagNodeHybrid` run fails closed with `409 Conflict`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_state: Option<String>,
    /// When `true`, stages the candidate digest (`CANDIDATE_READY`) before verifier verdicts.
    #[serde(default)]
    pub stage_candidate_only: bool,
}

/// Explicit request to settle an already-staged `CandidateReady` Swarm run.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SettleSwarmRequest {
    pub request_id: String,
}

/// Explicit empty request body for aborting an active or candidate Swarm run.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AbortSwarmRunRequest {}

/// Verified quorum proof attached to a converged or settled Swarm run response.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwarmConvergenceGateWire {
    pub panel_id: Uuid,
    pub swarm_run_id: Uuid,
    pub converged_diff_digest_hex: String,
    pub required_quorum: u32,
    pub accepted_verdicts: Vec<SwarmVerdictWire>,
}

impl From<&SwarmConvergenceGateOutcome> for SwarmConvergenceGateWire {
    fn from(gate: &SwarmConvergenceGateOutcome) -> Self {
        Self {
            panel_id: gate.panel_id(),
            swarm_run_id: gate.swarm_run_id(),
            converged_diff_digest_hex: encode_sha256_hex(gate.converged_diff_digest()),
            required_quorum: gate.required_quorum(),
            accepted_verdicts: gate
                .accepted_verdicts()
                .iter()
                .map(SwarmVerdictWire::from)
                .collect(),
        }
    }
}

/// Response returned from `POST .../converge` or `POST .../settle`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConvergeSwarmResponse {
    pub run: SwarmRunWire,
    pub settled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate: Option<SwarmConvergenceGateWire>,
}

/// Request to evaluate and remediate deterministic pure-Rust Stall & Livelock conditions (`0` LLM tokens).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwarmStallCheckRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stall_grace_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_stall_nudges: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_ping_pong_streak: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_member_turns: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at_ms: Option<i64>,
}

/// Deterministic remediation report returned by `POST .../stall-check`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwarmStallCheckResponse {
    pub action: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_members: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_member_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bottleneck_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub livelock_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_ping_pong_streak: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_member_turns: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<SwarmRunWire>,
    #[serde(default)]
    pub stopped_members: Vec<SwarmMemberWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub emitted_message: Option<SwarmBoardMessageWire>,
}

impl From<&SwarmMonitorActionReport> for SwarmStallCheckResponse {
    fn from(report: &SwarmMonitorActionReport) -> Self {
        match report {
            SwarmMonitorActionReport::NoAction {
                run_state,
                working_members,
            } => Self {
                action: "NO_ACTION".to_owned(),
                run_state: Some(run_state.as_str().to_owned()),
                working_members: Some(*working_members),
                target_member_id: None,
                bottleneck_reason: None,
                livelock_reason: None,
                observed_ping_pong_streak: None,
                observed_member_turns: None,
                run: None,
                stopped_members: Vec::new(),
                emitted_message: None,
            },
            SwarmMonitorActionReport::StallNudged {
                target_member_id,
                reason,
                outcome,
            } => Self {
                action: "STALL_NUDGED".to_owned(),
                run_state: Some(outcome.run().state().as_str().to_owned()),
                working_members: Some(0),
                target_member_id: Some(*target_member_id),
                bottleneck_reason: Some(bottleneck_reason_str(*reason).to_owned()),
                livelock_reason: None,
                observed_ping_pong_streak: None,
                observed_member_turns: None,
                run: Some(SwarmRunWire::from(outcome.run())),
                stopped_members: Vec::new(),
                emitted_message: outcome.message().map(SwarmBoardMessageWire::from),
            },
            SwarmMonitorActionReport::StallAborted {
                target_member_id,
                reason,
                outcome,
            } => Self {
                action: "STALL_ABORTED".to_owned(),
                run_state: Some(outcome.run().state().as_str().to_owned()),
                working_members: Some(0),
                target_member_id: Some(*target_member_id),
                bottleneck_reason: Some(bottleneck_reason_str(*reason).to_owned()),
                livelock_reason: None,
                observed_ping_pong_streak: None,
                observed_member_turns: None,
                run: Some(SwarmRunWire::from(outcome.run())),
                stopped_members: Vec::new(),
                emitted_message: outcome.message().map(SwarmBoardMessageWire::from),
            },
            SwarmMonitorActionReport::LivelockBroken { reason, outcome } => {
                let (reason_code, streak, turns) = classify_livelock_reason(*reason);
                Self {
                    action: "LIVELOCK_BROKEN".to_owned(),
                    run_state: None,
                    working_members: None,
                    target_member_id: None,
                    bottleneck_reason: None,
                    livelock_reason: Some(reason_code.to_owned()),
                    observed_ping_pong_streak: streak,
                    observed_member_turns: turns,
                    run: None,
                    stopped_members: outcome
                        .stopped_members()
                        .iter()
                        .map(SwarmMemberWire::from)
                        .collect(),
                    emitted_message: Some(SwarmBoardMessageWire::from(
                        outcome.escalation_message(),
                    )),
                }
            }
        }
    }
}

const fn bottleneck_reason_str(reason: SwarmBottleneckReason) -> &'static str {
    match reason {
        SwarmBottleneckReason::WakeQueuedVerifier => "WAKE_QUEUED_VERIFIER",
        SwarmBottleneckReason::WakeQueuedWorker => "WAKE_QUEUED_WORKER",
        SwarmBottleneckReason::UnreadPendingDoorbell => "UNREAD_PENDING_DOORBELL",
        SwarmBottleneckReason::AllMembersWaitingCoordinatorFallback => {
            "ALL_MEMBERS_WAITING_COORDINATOR_FALLBACK"
        }
    }
}

const fn classify_livelock_reason(
    reason: SwarmLivelockReason,
) -> (&'static str, Option<u32>, Option<u64>) {
    match reason {
        SwarmLivelockReason::PingPongStreakExceeded { observed_streak } => {
            ("PING_PONG_STREAK_EXCEEDED", Some(observed_streak), None)
        }
        SwarmLivelockReason::MemberTurnCapExceeded { turns_used } => {
            ("MEMBER_TURN_CAP_EXCEEDED", None, Some(turns_used))
        }
    }
}

pub(crate) fn parse_swarm_role_kind(raw: &str) -> Result<SwarmRoleKind, ApiError> {
    match raw {
        "coordinator" | "COORDINATOR" => Ok(SwarmRoleKind::Coordinator),
        "worker" | "WORKER" => Ok(SwarmRoleKind::Worker),
        "helper" | "HELPER" => Ok(SwarmRoleKind::Helper),
        "verifier" | "VERIFIER" => Ok(SwarmRoleKind::Verifier),
        "monitor" | "MONITOR" => Ok(SwarmRoleKind::Monitor),
        _ => Err(ApiError::Runtime(RuntimeError::InvalidInput)),
    }
}

pub(crate) fn parse_swarm_workspace_mode(raw: &str) -> Result<SwarmWorkspaceMode, ApiError> {
    match raw {
        "inherit" | "INHERIT" => Ok(SwarmWorkspaceMode::Inherit),
        "share" | "SHARE" => Ok(SwarmWorkspaceMode::Share),
        "branch" | "BRANCH" => Ok(SwarmWorkspaceMode::Branch),
        _ => Err(ApiError::Runtime(RuntimeError::InvalidInput)),
    }
}

pub(crate) fn parse_swarm_turn_failure_disposition(
    raw: &str,
) -> Result<SwarmTurnFailureDisposition, ApiError> {
    match raw {
        "transient_retry" | "transientRetry" | "TRANSIENT_RETRY" => {
            Ok(SwarmTurnFailureDisposition::TransientRetry)
        }
        "quota_exceeded" | "quotaExceeded" | "QUOTA_EXCEEDED" => {
            Ok(SwarmTurnFailureDisposition::QuotaExceeded)
        }
        "stopped" | "STOPPED" => Ok(SwarmTurnFailureDisposition::Stopped),
        _ => Err(ApiError::Runtime(RuntimeError::InvalidInput)),
    }
}

pub(crate) fn parse_swarm_convergence_target(
    raw: &str,
) -> Result<SwarmConvergenceTarget, ApiError> {
    match raw {
        "candidate_ready" | "candidateReady" | "CANDIDATE_READY" => {
            Ok(SwarmConvergenceTarget::CandidateReady)
        }
        "settled" | "SETTLED" => Ok(SwarmConvergenceTarget::Settled),
        _ => Err(ApiError::Runtime(RuntimeError::InvalidInput)),
    }
}

pub(crate) fn parse_swarm_commit_oid(raw: &str) -> Result<SwarmCommitOid, ApiError> {
    SwarmCommitOid::new(raw).map_err(|_| ApiError::Runtime(RuntimeError::InvalidInput))
}

pub(crate) fn parse_non_zero_digest_hex(raw: &str) -> Result<[u8; 32], ApiError> {
    let digest = parse_sha256_hex(raw)?;
    if digest == [0_u8; 32] {
        return Err(ApiError::Runtime(RuntimeError::InvalidInput));
    }
    Ok(digest)
}

pub(crate) fn parse_optional_worktree(
    raw: Option<&str>,
) -> Result<Result<Option<ForgeWorktreeRef>, SwarmErrorResponse>, ApiError> {
    let Some(path_str) = raw else {
        return Ok(Ok(None));
    };
    match ForgeWorktreeRef::new(std::path::PathBuf::from(path_str)) {
        Ok(valid) => Ok(Ok(Some(valid))),
        Err(
            heterarchy_orchestration_operations::SnapshotValidationError::ForbiddenSsdBuildWorktree,
        ) => Ok(Err(SwarmErrorResponse::permanent(
            "FORBIDDEN_SSD_BUILD_WORKTREE",
            "Swarm worktrees under /Volumes/256.SSD/RustBuild are strictly forbidden",
        ))),
        Err(_) => Err(ApiError::Runtime(RuntimeError::InvalidInput)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn swarm_wire_requests_reject_unknown_fields() {
        let unknown_in_run = r#"{
            "requestId": "0196ed00-0000-7000-8000-000000000001",
            "title": "Swarm Run",
            "mode": { "kind": "STANDALONE" },
            "maxMembers": 8,
            "maxSpawnDepth": 2,
            "maxTotalTurns": 64,
            "requiredVerifierQuorum": 1,
            "baselineCommitOid": "0123456789abcdef0123456789abcdef01234567",
            "baselineTreeBlake3Hex": "1111111111111111111111111111111111111111111111111111111111111111",
            "unexpectedSecret": "leaked"
        }"#;
        assert!(serde_json::from_str::<CreateSwarmRunRequest>(unknown_in_run).is_err());

        let unknown_in_verdict = r#"{
            "requestId": "0196ed00-0000-7000-8000-000000000002",
            "verifierMemberId": "0196ed00-0000-7000-8000-000000000003",
            "outcome": "ACCEPTED",
            "reviewedDiffDigestHex": "2222222222222222222222222222222222222222222222222222222222222222",
            "rationale": "verified",
            "extraField": true
        }"#;
        assert!(serde_json::from_str::<RecordSwarmVerdictRequest>(unknown_in_verdict).is_err());
    }

    #[test]
    fn swarm_mode_wire_roundtrips_standalone_and_hybrid() -> Result<(), Box<dyn std::error::Error>>
    {
        let standalone = SwarmRunModeWire::from_domain(SwarmRunMode::Standalone);
        assert_eq!(
            standalone
                .to_domain()
                .map_err(|e| format!("standalone decode: {e:?}"))?,
            SwarmRunMode::Standalone
        );

        let manifest = [0x4a; 32];
        let hybrid_domain = SwarmRunMode::DagNodeHybrid {
            goal_id: GoalId::from_uuid(Uuid::from_u128(0x101)),
            dag_revision: DagRevisionNumber::INITIAL,
            node_id: DagNodeId::from_uuid(Uuid::from_u128(0x102)),
            declared_write_manifest_blake3: manifest,
        };
        let hybrid_wire = SwarmRunModeWire::from_domain(hybrid_domain);
        assert_eq!(
            hybrid_wire
                .to_domain()
                .map_err(|e| format!("hybrid decode: {e:?}"))?,
            hybrid_domain
        );

        let mismatched = SwarmRunModeWire {
            kind: "STANDALONE".to_owned(),
            hybrid_target: hybrid_wire.hybrid_target,
        };
        assert!(mismatched.to_domain().is_err());
        Ok(())
    }

    #[test]
    fn dispatch_swarm_turn_request_roundtrips_domain_and_rejects_ssd_worktree()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::wire::autonomy_loop::{ExecutionPermissionsWire, ExecutionTimeoutsWire};
        use forge_domain::WorkspaceId;

        let ws = WorkspaceId::from_uuid(Uuid::from_u128(0x501));
        let run_id = Uuid::from_u128(0x502);
        let member_id = Uuid::from_u128(0x503);
        let wire = DispatchSwarmTurnRequest {
            snapshot_request_id: Uuid::from_u128(0x504).to_string(),
            snapshot_id: Uuid::from_u128(0x505).to_string(),
            activity_id: Uuid::from_u128(0x506).to_string(),
            default_worktree_path: "/var/tmp/forge-swarm/dispatch-worker".to_owned(),
            lease_id: Uuid::from_u128(0x507).to_string(),
            lease_revision: 2,
            lease_expires_at_ms: 90_000,
            mcp_grant: None,
            permissions: ExecutionPermissionsWire {
                allow_workspace_write: true,
                allow_local_shell: true,
                allow_network: false,
            },
            timeouts: ExecutionTimeoutsWire {
                startup_timeout_ms: 2_000,
                execution_timeout_ms: 10_000,
                termination_grace_ms: 1_000,
            },
            dispatched_at_ms: Some(12_345),
            clean_exit_output_commit_oid: Some("a".repeat(40)),
            clean_exit_output_tree_blake3_hex: Some("b2".repeat(32)),
        };
        let domain = wire
            .to_domain(ws, run_id, member_id, 9_999)
            .map_err(|e| format!("unexpected api error: {e:?}"))?
            .map_err(|rej| format!("unexpected domain rejection: {rej:?}"))?;
        assert_eq!(DispatchSwarmTurnRequest::from(&domain), wire);

        let mut ssd_wire = wire;
        ssd_wire.default_worktree_path = "/Volumes/256.SSD/RustBuild/bad-worktree".to_owned();
        let Err(rejected) = ssd_wire
            .to_domain(ws, run_id, member_id, 9_999)
            .map_err(|e| format!("unexpected api error: {e:?}"))?
        else {
            return Err("expected SSD worktree rejection".into());
        };
        assert_eq!(rejected.code, "FORBIDDEN_SSD_BUILD_WORKTREE");
        Ok(())
    }
}
