//! Wire DTOs and bidirectional conversions for single-flight Swarm member turn dispatch.

use std::path::PathBuf;

use forge_domain::{ActivityId, DirectTokenId, WorkspaceId};
use heterarchy_orchestration_operations::{
    AssignmentExecutionSnapshot, ExecutionPermissions, ExecutionTimeouts, ForgeWorktreeRef,
    LeaseRef, McpGrant, SnapshotValidationError, SwarmTurnOutput,
};
use heterarchy_orchestration_runtime::execution_supervisor::{
    SupervisionOutcome, TerminationDisposition,
};
use heterarchy_orchestration_runtime::{
    SwarmDispatchTurnRequest, SwarmDispatchedTurnOutcome, SwarmTurnDisposition,
};
use heterarchy_orchestration_sessions::RolePromptDigest;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    SwarmErrorResponse, SwarmMemberWire, SwarmWakeupBatchWire, parse_non_zero_digest_hex,
    parse_swarm_commit_oid,
};
use crate::wire::autonomy_loop::{
    ExecutionPermissionsWire, ExecutionTimeoutsWire, LaunchLedgerRecordWire, McpGrantWire,
    encode_sha256_hex,
};
use crate::{ApiError, RuntimeError, canonical_uuid};

/// Request to dispatch and supervise one single-flight `WakeQueued` Swarm member turn.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DispatchSwarmTurnRequest {
    pub snapshot_request_id: String,
    pub snapshot_id: String,
    pub activity_id: String,
    pub default_worktree_path: String,
    pub lease_id: String,
    pub lease_revision: u64,
    pub lease_expires_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_grant: Option<McpGrantWire>,
    pub permissions: ExecutionPermissionsWire,
    pub timeouts: ExecutionTimeoutsWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatched_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clean_exit_output_commit_oid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clean_exit_output_tree_blake3_hex: Option<String>,
}

impl DispatchSwarmTurnRequest {
    /// Validate and convert this wire request into a domain [`SwarmDispatchTurnRequest`].
    ///
    /// # Errors
    /// Returns [`ApiError`] for malformed identities, grant windows, or timeouts, and
    /// `Ok(Err(SwarmErrorResponse))` when `default_worktree_path` targets `/Volumes/256.SSD/RustBuild`.
    pub(crate) fn to_domain(
        &self,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
        member_id: Uuid,
        fallback_dispatched_at_ms: i64,
    ) -> Result<Result<SwarmDispatchTurnRequest, SwarmErrorResponse>, ApiError> {
        if workspace_id.into_uuid().is_nil() || swarm_run_id.is_nil() || member_id.is_nil() {
            return Err(ApiError::InvalidIdentity);
        }
        let snapshot_request_id = canonical_uuid(&self.snapshot_request_id)?;
        let snapshot_id = canonical_uuid(&self.snapshot_id)?;
        let activity_id = ActivityId::from_uuid(canonical_uuid(&self.activity_id)?);
        let lease = LeaseRef::new(
            canonical_uuid(&self.lease_id)?,
            self.lease_revision,
            self.lease_expires_at_ms,
        )
        .map_err(|_| ApiError::Runtime(RuntimeError::InvalidInput))?;
        let mcp_grant = parse_optional_mcp_grant(self.mcp_grant.as_ref())?;
        let permissions = ExecutionPermissions::new(
            self.permissions.allow_workspace_write,
            self.permissions.allow_local_shell,
            self.permissions.allow_network,
        );
        let timeouts = ExecutionTimeouts::new(
            self.timeouts.startup_timeout_ms,
            self.timeouts.execution_timeout_ms,
            self.timeouts.termination_grace_ms,
        )
        .map_err(|_| ApiError::Runtime(RuntimeError::InvalidInput))?;
        let dispatched_at_ms = match self.dispatched_at_ms {
            Some(ts) if ts < 0 => return Err(ApiError::Runtime(RuntimeError::InvalidInput)),
            Some(ts) => ts,
            None => fallback_dispatched_at_ms,
        };
        let clean_exit_output = parse_clean_exit_output(
            self.clean_exit_output_commit_oid.as_deref(),
            self.clean_exit_output_tree_blake3_hex.as_deref(),
        )?;
        let default_worktree =
            match ForgeWorktreeRef::new(PathBuf::from(&self.default_worktree_path)) {
                Ok(valid) => valid,
                Err(SnapshotValidationError::ForbiddenSsdBuildWorktree) => {
                    return Ok(Err(SwarmErrorResponse::permanent(
                        "FORBIDDEN_SSD_BUILD_WORKTREE",
                        "Swarm worktrees under /Volumes/256.SSD/RustBuild are strictly forbidden",
                    )));
                }
                Err(_) => return Err(ApiError::Runtime(RuntimeError::InvalidInput)),
            };

        Ok(Ok(SwarmDispatchTurnRequest {
            snapshot_request_id,
            snapshot_id,
            workspace_id,
            swarm_run_id,
            member_id,
            activity_id,
            default_worktree,
            lease,
            mcp_grant,
            permissions,
            timeouts,
            dispatched_at_ms,
            clean_exit_output,
        }))
    }
}

impl From<&SwarmDispatchTurnRequest> for DispatchSwarmTurnRequest {
    fn from(req: &SwarmDispatchTurnRequest) -> Self {
        let (clean_exit_output_commit_oid, clean_exit_output_tree_blake3_hex) =
            match &req.clean_exit_output {
                Some(output) => (
                    Some(output.commit_oid.as_str().to_owned()),
                    Some(encode_sha256_hex(output.tree_blake3)),
                ),
                None => (None, None),
            };
        Self {
            snapshot_request_id: req.snapshot_request_id.to_string(),
            snapshot_id: req.snapshot_id.to_string(),
            activity_id: req.activity_id.into_uuid().to_string(),
            default_worktree_path: req.default_worktree.as_str().to_owned(),
            lease_id: req.lease.lease_id().to_string(),
            lease_revision: req.lease.lease_revision(),
            lease_expires_at_ms: req.lease.expires_at_ms(),
            mcp_grant: req.mcp_grant.map(|grant| McpGrantWire {
                token_id: grant.token_id().into_uuid().to_string(),
                issued_at_ms: grant.issued_at_ms(),
                expires_at_ms: grant.expires_at_ms(),
            }),
            permissions: ExecutionPermissionsWire {
                allow_workspace_write: req.permissions.allow_workspace_write(),
                allow_local_shell: req.permissions.allow_local_shell(),
                allow_network: req.permissions.allow_network(),
            },
            timeouts: ExecutionTimeoutsWire {
                startup_timeout_ms: req.timeouts.startup_timeout_ms(),
                execution_timeout_ms: req.timeouts.execution_timeout_ms(),
                termination_grace_ms: req.timeouts.termination_grace_ms(),
            },
            dispatched_at_ms: Some(req.dispatched_at_ms),
            clean_exit_output_commit_oid,
            clean_exit_output_tree_blake3_hex,
        }
    }
}

impl From<SwarmDispatchTurnRequest> for DispatchSwarmTurnRequest {
    fn from(req: SwarmDispatchTurnRequest) -> Self {
        Self::from(&req)
    }
}

/// Wire projection of the immutable [`AssignmentExecutionSnapshot`] sealed immediately before spawn.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CommittedExecutionSnapshotWire {
    pub snapshot_id: Uuid,
    pub workspace_id: Uuid,
    pub goal_id: Uuid,
    pub dag_revision: i64,
    pub node_id: Uuid,
    pub assignment_id: Uuid,
    pub attempt: u32,
    pub activity_id: Uuid,
    pub profile_id: Uuid,
    pub profile_revision: i64,
    pub resolved_engine: String,
    pub resolved_model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_reasoning: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_prompt_digest_hex: Option<String>,
    pub worktree_path: String,
    pub lease_id: Uuid,
    pub lease_revision: u64,
    pub lease_expires_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_grant: Option<McpGrantWire>,
    pub permissions: ExecutionPermissionsWire,
    pub timeouts: ExecutionTimeoutsWire,
    pub committed_at_ms: i64,
}

impl From<&AssignmentExecutionSnapshot> for CommittedExecutionSnapshotWire {
    fn from(snapshot: &AssignmentExecutionSnapshot) -> Self {
        let attempt = snapshot.attempt_key();
        Self {
            snapshot_id: snapshot.snapshot_id(),
            workspace_id: attempt.workspace_id().into_uuid(),
            goal_id: attempt.goal_id().into_uuid(),
            dag_revision: attempt.dag_revision().get(),
            node_id: attempt.node_id().into_uuid(),
            assignment_id: attempt.assignment_id().into_uuid(),
            attempt: attempt.attempt(),
            activity_id: snapshot.activity_id().into_uuid(),
            profile_id: snapshot.profile_id().into_uuid(),
            profile_revision: snapshot.profile_revision().get(),
            resolved_engine: snapshot.resolved_engine().as_str().to_owned(),
            resolved_model: snapshot.resolved_model().to_owned(),
            resolved_reasoning: snapshot.resolved_reasoning().map(str::to_owned),
            role_prompt_digest_hex: snapshot
                .role_prompt_digest()
                .as_ref()
                .map(RolePromptDigest::to_hex),
            worktree_path: snapshot.worktree().as_str().to_owned(),
            lease_id: snapshot.lease().lease_id(),
            lease_revision: snapshot.lease().lease_revision(),
            lease_expires_at_ms: snapshot.lease().expires_at_ms(),
            mcp_grant: snapshot.mcp_grant().map(|grant| McpGrantWire {
                token_id: grant.token_id().into_uuid().to_string(),
                issued_at_ms: grant.issued_at_ms(),
                expires_at_ms: grant.expires_at_ms(),
            }),
            permissions: ExecutionPermissionsWire {
                allow_workspace_write: snapshot.permissions().allow_workspace_write(),
                allow_local_shell: snapshot.permissions().allow_local_shell(),
                allow_network: snapshot.permissions().allow_network(),
            },
            timeouts: ExecutionTimeoutsWire {
                startup_timeout_ms: snapshot.timeouts().startup_timeout_ms(),
                execution_timeout_ms: snapshot.timeouts().execution_timeout_ms(),
                termination_grace_ms: snapshot.timeouts().termination_grace_ms(),
            },
            committed_at_ms: snapshot.committed_at_ms(),
        }
    }
}

/// Wire projection of the [`SupervisionOutcome`] recorded in the Launch Ledger.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DispatchedTurnSupervisionWire {
    pub status: String,
    pub ledger: LaunchLedgerRecordWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub termination: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unknown_reason: Option<String>,
}

impl From<&SupervisionOutcome> for DispatchedTurnSupervisionWire {
    fn from(outcome: &SupervisionOutcome) -> Self {
        match outcome {
            SupervisionOutcome::Exited { ledger, exit_code } => Self {
                status: "EXITED".to_owned(),
                ledger: LaunchLedgerRecordWire::from(ledger),
                exit_code: Some(*exit_code),
                termination: None,
                unknown_reason: None,
            },
            SupervisionOutcome::TimedOut {
                ledger,
                termination,
            } => Self {
                status: "TIMED_OUT".to_owned(),
                ledger: LaunchLedgerRecordWire::from(ledger),
                exit_code: None,
                termination: Some(termination_disposition_str(*termination).to_owned()),
                unknown_reason: None,
            },
            SupervisionOutcome::Unknown { ledger, reason } => Self {
                status: "UNKNOWN".to_owned(),
                ledger: LaunchLedgerRecordWire::from(ledger),
                exit_code: None,
                termination: None,
                unknown_reason: Some(reason.as_str().to_owned()),
            },
        }
    }
}

const fn termination_disposition_str(disposition: TerminationDisposition) -> &'static str {
    match disposition {
        TerminationDisposition::AlreadyExited => "ALREADY_EXITED",
        TerminationDisposition::TerminatedAfterTerm => "TERMINATED_AFTER_TERM",
        TerminationDisposition::TerminatedAfterKill => "TERMINATED_AFTER_KILL",
        TerminationDisposition::BootMismatchSkipped => "BOOT_MISMATCH_SKIPPED",
    }
}

/// Wire projection of the post-turn [`SwarmTurnDisposition`] applied to `ops_swarm_members`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DispatchedTurnDispositionWire {
    pub kind: String,
    pub member: SwarmMemberWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub timed_out: bool,
}

impl From<&SwarmTurnDisposition> for DispatchedTurnDispositionWire {
    fn from(disposition: &SwarmTurnDisposition) -> Self {
        match disposition {
            SwarmTurnDisposition::Completed { member, exit_code } => Self {
                kind: "COMPLETED".to_owned(),
                member: SwarmMemberWire::from(member),
                exit_code: Some(*exit_code),
                timed_out: false,
            },
            SwarmTurnDisposition::SuspendedQuota { member, exit_code } => Self {
                kind: "SUSPENDED_QUOTA".to_owned(),
                member: SwarmMemberWire::from(member),
                exit_code: Some(*exit_code),
                timed_out: false,
            },
            SwarmTurnDisposition::TransientFailed {
                member,
                exit_code,
                timed_out,
            } => Self {
                kind: "TRANSIENT_FAILED".to_owned(),
                member: SwarmMemberWire::from(member),
                exit_code: *exit_code,
                timed_out: *timed_out,
            },
            SwarmTurnDisposition::Stopped { member, exit_code } => Self {
                kind: "STOPPED".to_owned(),
                member: SwarmMemberWire::from(member),
                exit_code: *exit_code,
                timed_out: false,
            },
            SwarmTurnDisposition::CancelledWhileWorking { member } => Self {
                kind: "CANCELLED_WHILE_WORKING".to_owned(),
                member: SwarmMemberWire::from(member),
                exit_code: None,
                timed_out: false,
            },
        }
    }
}

/// Complete receipt returned by `POST /v1/workspaces/{workspace_id}/swarms/{swarm_run_id}/members/{member_id}/turns/dispatch`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DispatchSwarmTurnResponse {
    pub wakeup_batch: SwarmWakeupBatchWire,
    pub committed_snapshot: CommittedExecutionSnapshotWire,
    pub supervision: DispatchedTurnSupervisionWire,
    pub disposition: DispatchedTurnDispositionWire,
    pub used_parent_spawner_slot: bool,
}

impl From<&SwarmDispatchedTurnOutcome> for DispatchSwarmTurnResponse {
    fn from(outcome: &SwarmDispatchedTurnOutcome) -> Self {
        Self {
            wakeup_batch: SwarmWakeupBatchWire::from(&outcome.wakeup_batch),
            committed_snapshot: CommittedExecutionSnapshotWire::from(&outcome.committed_snapshot),
            supervision: DispatchedTurnSupervisionWire::from(&outcome.supervision),
            disposition: DispatchedTurnDispositionWire::from(&outcome.disposition),
            used_parent_spawner_slot: outcome.used_parent_spawner_slot,
        }
    }
}

impl From<SwarmDispatchedTurnOutcome> for DispatchSwarmTurnResponse {
    fn from(outcome: SwarmDispatchedTurnOutcome) -> Self {
        Self::from(&outcome)
    }
}

fn parse_optional_mcp_grant(raw: Option<&McpGrantWire>) -> Result<Option<McpGrant>, ApiError> {
    raw.map(|grant| {
        let token_uuid = canonical_uuid(&grant.token_id)?;
        McpGrant::new(
            DirectTokenId::from_uuid(token_uuid),
            grant.issued_at_ms,
            grant.expires_at_ms,
        )
        .map_err(|_| ApiError::Runtime(RuntimeError::InvalidInput))
    })
    .transpose()
}

fn parse_clean_exit_output(
    commit_raw: Option<&str>,
    tree_hex_raw: Option<&str>,
) -> Result<Option<SwarmTurnOutput>, ApiError> {
    match (commit_raw, tree_hex_raw) {
        (None, None) => Ok(None),
        (Some(commit_str), Some(tree_hex)) => Ok(Some(SwarmTurnOutput {
            commit_oid: parse_swarm_commit_oid(commit_str)?,
            tree_blake3: parse_non_zero_digest_hex(tree_hex)?,
        })),
        _ => Err(ApiError::Runtime(RuntimeError::InvalidInput)),
    }
}
