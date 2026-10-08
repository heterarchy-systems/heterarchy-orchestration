//! Wire contracts for the Goal/DAG source reads: `goal.list`, `goal.read`,
//! and `dag.read_current`.
//!
//! Responses are bounded source observations; no goal or DAG row grants
//! mutation rights, Writer Lease ownership, or scope membership.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::super::leadership::WorkspaceQueryScope;

/// `POST /internal/v1/workspaces/{workspace_id}/forge/goal-list` request.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GoalListQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub activity_id: Uuid,
    pub limit: u32,
}

/// `POST /internal/v1/workspaces/{workspace_id}/forge/goal-list` response.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GoalListQueryResponse {
    pub goals: Vec<GoalWire>,
}

/// `POST /internal/v1/workspaces/{workspace_id}/forge/goal-get` request.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GoalGetQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub activity_id: Uuid,
    pub goal_id: Uuid,
}

/// `POST /internal/v1/workspaces/{workspace_id}/forge/goal-get` response.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GoalGetQueryResponse {
    pub goal: Option<GoalWire>,
}

/// `POST /internal/v1/workspaces/{workspace_id}/forge/dag-get-current`
/// request.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DagCurrentQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub activity_id: Uuid,
    pub goal_id: Uuid,
}

/// `POST /internal/v1/workspaces/{workspace_id}/forge/dag-get-current`
/// response.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DagCurrentQueryResponse {
    pub dag: Option<DagCurrentWire>,
}

/// One Goal row.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GoalWire {
    pub goal_id: String,
    pub workspace_id: String,
    pub revision: i64,
    pub status: GoalStatusWire,
    pub objective: String,
    pub success_criteria: Vec<String>,
    pub constraints: Vec<String>,
    pub out_of_scope: Vec<String>,
    pub verification_requirements: Vec<GoalVerificationWire>,
    pub stop_conditions: Vec<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// One Goal status with its optional reason.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GoalStatusWire {
    pub kind: GoalStatusKindWire,
    pub reason: Option<String>,
}

/// The closed Goal status vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalStatusKindWire {
    Active,
    Paused,
    Blocked,
    Completed,
    Failed,
    Cancelled,
}

/// One verification requirement.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GoalVerificationWire {
    pub kind: GoalVerificationKindWire,
    pub description: String,
}

/// The closed verification-kind vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalVerificationKindWire {
    ExecutionSucceeded,
    FilePresent,
    ChangeSetSealed,
    CheckpointReady,
}

/// The current DAG revision for one Goal.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DagCurrentWire {
    pub goal_id: String,
    pub revision_id: String,
    pub base_goal_revision: i64,
    pub dag_revision: i64,
    pub parent_revision: Option<i64>,
    pub created_at_ms: i64,
    pub head_revision: i64,
    pub head_updated_at_ms: i64,
    pub nodes: Vec<DagNodeWire>,
    pub edges: Vec<DagEdgeWire>,
}

/// One DAG node.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DagNodeWire {
    pub node_id: String,
    pub ordinal: u16,
    pub title: String,
    pub objective: String,
    pub acceptance: Vec<String>,
    pub lifecycle: DagLifecycleWire,
    pub status: DagStatusWire,
    pub waiting_on: Vec<String>,
    pub revision: i64,
    pub evidence_count: u16,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// The closed DAG node lifecycle vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DagLifecycleWire {
    Pending,
    Active,
    Blocked,
    Completed,
    Failed,
    Skipped,
    Cancelled,
}

/// The closed DAG node status vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DagStatusWire {
    Pending,
    Ready,
    Active,
    Blocked,
    Completed,
    Failed,
    Skipped,
    Cancelled,
}

/// One DAG edge.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DagEdgeWire {
    pub upstream: String,
    pub downstream: String,
}
