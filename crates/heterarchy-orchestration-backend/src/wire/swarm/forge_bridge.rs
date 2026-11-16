//! Additive East-West `Orchestration <-> Forge` Swarm convergence, settlement,
//! and autonomy outbox synchronization wire contracts.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{SwarmConvergenceGateWire, SwarmRunWire};

/// Request to converge captured Swarm member candidates via Forge 3-way merge
/// and transition the Swarm run to `CandidateReady`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ForgeConvergeSwarmRequest {
    pub request_id: String,
    pub session_id: String,
    #[serde(default)]
    pub candidate_member_ids: Vec<String>,
    #[serde(default)]
    pub stage_candidate_only: bool,
}

/// Response returned after Forge converges Swarm member candidates and Orchestration
/// records the `CandidateReady` digest.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ForgeConvergeSwarmResponse {
    pub run: SwarmRunWire,
    pub changed_files: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate: Option<SwarmConvergenceGateWire>,
}

/// Request to verify the adversarial `Verifier` quorum gate, atomically settle a
/// `CandidateReady` Swarm run on Forge, and optionally consume `SWARM_SETTLED_KIND = 6`
/// and purge ephemeral Swarm worktrees.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ForgeSettleSwarmRequest {
    pub request_id: String,
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity_id: Option<String>,
    pub coordinator_agent_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outbox_migration_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outbox_after_sequence: Option<u64>,
    #[serde(default)]
    pub purge_ephemeral: bool,
}

/// Response returned after settling a `Standalone` Swarm run across Forge and Orchestration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ForgeSettleSwarmResponse {
    pub run: SwarmRunWire,
    pub gate: SwarmConvergenceGateWire,
    pub settlement_id: Uuid,
    pub settled_file_count: usize,
    pub settled_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outbox_sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outbox_sync: Option<SyncAutonomyOutboxResponse>,
    pub purged_ephemeral: bool,
}

/// Request to read one bounded page from Forge's autonomy outbox and reconcile
/// `DAG_SETTLED_KIND = 5` and `SWARM_SETTLED_KIND = 6` facts in Orchestration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SyncAutonomyOutboxRequest {
    pub migration_id: String,
    #[serde(default)]
    pub after_sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// Reconciliation summary returned by `POST /internal/v1/workspaces/{workspace_id}/autonomy-outbox/sync`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SyncAutonomyOutboxResponse {
    pub applied_count: usize,
    pub dag_settled_count: usize,
    pub swarm_settled_count: usize,
    pub last_sequence: u64,
}
