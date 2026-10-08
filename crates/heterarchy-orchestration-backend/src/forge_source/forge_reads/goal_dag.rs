//! Source read observations for the Goal/DAG family: `goal.list`,
//! `goal.read`, and `dag.read_current`.
//!
//! The decoders mirror the established native/HTTP acceptance class:
//! closed status/lifecycle vocabularies, bounded ordinals and evidence
//! counts, and nullable fields that must be present. Readbacks are
//! observations only — they never grant mutation rights, scope membership,
//! or DAG authority.

use std::collections::BTreeSet;

use forge_backend::ops::{CommandOp, ScopeDto};
use forge_domain::{
    DagHeadRevision, DagNodeId, DagNodeOrdinal, DagNodeRevision, DagRevisionId, DagRevisionNumber,
    GoalId, GoalListLimit, GoalRevision, MAX_DAG_EDGES, MAX_DAG_NODES, TimestampMillis,
};
use serde_json::{Value, json};
use uuid::Uuid;

use super::super::{ForgeSourceError, ForgeSourceHttpClient, canonical_uuid, map_command_error};

/// One Goal row observed from Forge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeGoal {
    goal_id: String,
    workspace_id: String,
    revision: i64,
    status: ForgeGoalStatus,
    objective: String,
    success_criteria: Vec<String>,
    constraints: Vec<String>,
    out_of_scope: Vec<String>,
    verification_requirements: Vec<ForgeGoalVerification>,
    stop_conditions: Vec<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
}

impl ForgeGoal {
    /// The exact Goal identity string returned by Forge.
    #[must_use]
    pub fn goal_id(&self) -> &str {
        &self.goal_id
    }

    /// The exact Workspace identity string returned by Forge.
    #[must_use]
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }

    /// The exact Goal revision returned by Forge.
    #[must_use]
    pub const fn revision(&self) -> i64 {
        self.revision
    }

    /// The Goal lifecycle status.
    #[must_use]
    pub const fn status(&self) -> &ForgeGoalStatus {
        &self.status
    }

    /// The objective text.
    #[must_use]
    pub fn objective(&self) -> &str {
        &self.objective
    }

    /// The success criteria list.
    #[must_use]
    pub fn success_criteria(&self) -> &[String] {
        &self.success_criteria
    }

    /// The constraints list.
    #[must_use]
    pub fn constraints(&self) -> &[String] {
        &self.constraints
    }

    /// The out-of-scope list.
    #[must_use]
    pub fn out_of_scope(&self) -> &[String] {
        &self.out_of_scope
    }

    /// The verification requirements.
    #[must_use]
    pub fn verification_requirements(&self) -> &[ForgeGoalVerification] {
        &self.verification_requirements
    }

    /// The stop conditions list.
    #[must_use]
    pub fn stop_conditions(&self) -> &[String] {
        &self.stop_conditions
    }

    /// Creation time in epoch milliseconds.
    #[must_use]
    pub const fn created_at_ms(&self) -> i64 {
        self.created_at_ms
    }

    /// Last update time in epoch milliseconds.
    #[must_use]
    pub const fn updated_at_ms(&self) -> i64 {
        self.updated_at_ms
    }
}

/// One Goal lifecycle status with its optional reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeGoalStatus {
    kind: ForgeGoalStatusKind,
    reason: Option<String>,
}

impl ForgeGoalStatus {
    /// The closed status kind.
    #[must_use]
    pub const fn kind(&self) -> ForgeGoalStatusKind {
        self.kind
    }

    /// The optional status reason.
    #[must_use]
    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }
}

/// The closed Goal status vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgeGoalStatusKind {
    Active,
    Paused,
    Blocked,
    Completed,
    Failed,
    Cancelled,
}

impl ForgeGoalStatusKind {
    /// The exact wire vocabulary string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Blocked => "blocked",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

/// One Goal verification requirement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeGoalVerification {
    kind: ForgeVerificationKind,
    description: String,
}

impl ForgeGoalVerification {
    /// The closed verification kind.
    #[must_use]
    pub const fn kind(&self) -> ForgeVerificationKind {
        self.kind
    }

    /// The requirement description.
    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }
}

/// The closed verification-kind vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgeVerificationKind {
    ExecutionSucceeded,
    FilePresent,
    ChangeSetSealed,
    CheckpointReady,
}

/// The current DAG revision observed for one Goal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeDagCurrent {
    goal_id: String,
    revision_id: String,
    base_goal_revision: i64,
    dag_revision: i64,
    parent_revision: Option<i64>,
    created_at_ms: i64,
    head_revision: i64,
    head_updated_at_ms: i64,
    nodes: Vec<ForgeDagNode>,
    edges: Vec<ForgeDagEdge>,
}

impl ForgeDagCurrent {
    /// The exact Goal identity string.
    #[must_use]
    pub fn goal_id(&self) -> &str {
        &self.goal_id
    }

    /// The exact DAG revision identity string.
    #[must_use]
    pub fn revision_id(&self) -> &str {
        &self.revision_id
    }

    /// The Goal revision the DAG was based on.
    #[must_use]
    pub const fn base_goal_revision(&self) -> i64 {
        self.base_goal_revision
    }

    /// The observed DAG revision number.
    #[must_use]
    pub const fn dag_revision(&self) -> i64 {
        self.dag_revision
    }

    /// The parent DAG revision, when this revision has one.
    #[must_use]
    pub const fn parent_revision(&self) -> Option<i64> {
        self.parent_revision
    }

    /// Creation time in epoch milliseconds.
    #[must_use]
    pub const fn created_at_ms(&self) -> i64 {
        self.created_at_ms
    }

    /// The current DAG head revision.
    #[must_use]
    pub const fn head_revision(&self) -> i64 {
        self.head_revision
    }

    /// The head revision's last update time in epoch milliseconds.
    #[must_use]
    pub const fn head_updated_at_ms(&self) -> i64 {
        self.head_updated_at_ms
    }

    /// The bounded node list.
    #[must_use]
    pub fn nodes(&self) -> &[ForgeDagNode] {
        &self.nodes
    }

    /// The edge list.
    #[must_use]
    pub fn edges(&self) -> &[ForgeDagEdge] {
        &self.edges
    }
}

/// One DAG node observed in the current revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeDagNode {
    node_id: String,
    ordinal: u16,
    title: String,
    objective: String,
    acceptance: Vec<String>,
    lifecycle: ForgeDagLifecycle,
    status: ForgeDagNodeStatus,
    waiting_on: Vec<String>,
    revision: i64,
    evidence_count: u16,
    created_at_ms: i64,
    updated_at_ms: i64,
}

impl ForgeDagNode {
    /// The exact node identity string.
    #[must_use]
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// The bounded node ordinal.
    #[must_use]
    pub const fn ordinal(&self) -> u16 {
        self.ordinal
    }

    /// The node title.
    #[must_use]
    pub fn title(&self) -> &str {
        &self.title
    }

    /// The node objective.
    #[must_use]
    pub fn objective(&self) -> &str {
        &self.objective
    }

    /// The acceptance criteria list.
    #[must_use]
    pub fn acceptance(&self) -> &[String] {
        &self.acceptance
    }

    /// The closed node lifecycle.
    #[must_use]
    pub const fn lifecycle(&self) -> ForgeDagLifecycle {
        self.lifecycle
    }

    /// The closed node status.
    #[must_use]
    pub const fn status(&self) -> ForgeDagNodeStatus {
        self.status
    }

    /// The waiting-on list.
    #[must_use]
    pub fn waiting_on(&self) -> &[String] {
        &self.waiting_on
    }

    /// The exact node revision.
    #[must_use]
    pub const fn revision(&self) -> i64 {
        self.revision
    }

    /// The bounded evidence count.
    #[must_use]
    pub const fn evidence_count(&self) -> u16 {
        self.evidence_count
    }

    /// Creation time in epoch milliseconds.
    #[must_use]
    pub const fn created_at_ms(&self) -> i64 {
        self.created_at_ms
    }

    /// Last update time in epoch milliseconds.
    #[must_use]
    pub const fn updated_at_ms(&self) -> i64 {
        self.updated_at_ms
    }
}

/// The closed DAG node lifecycle vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgeDagLifecycle {
    Pending,
    Active,
    Blocked,
    Completed,
    Failed,
    Skipped,
    Cancelled,
}

/// The closed DAG node status vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgeDagNodeStatus {
    Pending,
    Ready,
    Active,
    Blocked,
    Completed,
    Failed,
    Skipped,
    Cancelled,
}

/// One DAG edge between two node identity strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeDagEdge {
    upstream: String,
    downstream: String,
}

impl ForgeDagEdge {
    /// The upstream node identity string.
    #[must_use]
    pub fn upstream(&self) -> &str {
        &self.upstream
    }

    /// The downstream node identity string.
    #[must_use]
    pub fn downstream(&self) -> &str {
        &self.downstream
    }
}

impl ForgeSourceHttpClient {
    /// Observe the bounded Goal list for one exact Activity
    /// (`goal.list`).
    ///
    /// # Errors
    /// Returns a typed invalid-scope, denied, unavailable, stale, or protocol
    /// error.
    pub async fn goal_list(
        &self,
        scope: &ScopeDto,
        activity_id: Uuid,
        limit: GoalListLimit,
    ) -> Result<Vec<ForgeGoal>, ForgeSourceError> {
        if activity_id.is_nil() {
            return Err(ForgeSourceError::InvalidScope);
        }
        let readback = self
            .commands()
            .command(
                CommandOp::GoalList,
                json!({
                    "scope": scope_json(scope),
                    "activity_id": activity_id,
                    "limit": limit.get(),
                }),
            )
            .await
            .map_err(map_command_error)?;
        let goals = readback
            .get("goals")
            .and_then(Value::as_array)
            .ok_or(ForgeSourceError::Protocol)?;
        if goals.len() > usize::try_from(limit.get()).map_err(|_| ForgeSourceError::Protocol)? {
            return Err(ForgeSourceError::Protocol);
        }
        goals
            .iter()
            .map(|row| {
                let goal = goal_row_from_wire(row)?;
                if goal.workspace_id != scope.workspace_id.to_string() {
                    return Err(ForgeSourceError::Protocol);
                }
                Ok(goal)
            })
            .collect()
    }

    /// Observe one exact Goal row (`goal.read`). A missing row is
    /// `Ok(None)`.
    ///
    /// # Errors
    /// Returns a typed invalid-scope, denied, unavailable, stale, or protocol
    /// error.
    pub async fn goal_get(
        &self,
        scope: &ScopeDto,
        activity_id: Uuid,
        goal_id: Uuid,
    ) -> Result<Option<ForgeGoal>, ForgeSourceError> {
        if activity_id.is_nil() || goal_id.is_nil() {
            return Err(ForgeSourceError::InvalidScope);
        }
        let readback = self
            .commands()
            .command(
                CommandOp::GoalRead,
                json!({
                    "scope": scope_json(scope),
                    "activity_id": activity_id,
                    "goal_id": goal_id,
                }),
            )
            .await
            .map_err(map_command_error)?;
        if readback.get("found").and_then(Value::as_bool) == Some(false) {
            return Ok(None);
        }
        let goal = goal_row_from_wire(&readback)?;
        if goal.workspace_id != scope.workspace_id.to_string()
            || goal.goal_id != goal_id.to_string()
        {
            return Err(ForgeSourceError::Protocol);
        }
        Ok(Some(goal))
    }

    /// Observe the current DAG revision for one Goal
    /// (`dag.read_current`). A missing DAG is `Ok(None)`.
    ///
    /// # Errors
    /// Returns a typed invalid-scope, denied, unavailable, stale, or protocol
    /// error.
    pub async fn dag_get_current(
        &self,
        scope: &ScopeDto,
        goal_id: Uuid,
    ) -> Result<Option<ForgeDagCurrent>, ForgeSourceError> {
        if goal_id.is_nil() {
            return Err(ForgeSourceError::InvalidScope);
        }
        let readback = self
            .commands()
            .command(
                CommandOp::DagReadCurrent,
                json!({
                    "scope": scope_json(scope),
                    "goal_id": goal_id,
                }),
            )
            .await
            .map_err(map_command_error)?;
        if readback.get("found").and_then(Value::as_bool) == Some(false) {
            return Ok(None);
        }
        let dag = dag_current_from_wire(&readback)?;
        if dag.goal_id != goal_id.to_string() {
            return Err(ForgeSourceError::Protocol);
        }
        Ok(Some(dag))
    }
}

fn scope_json(scope: &ScopeDto) -> Value {
    json!({
        "workspace_id": scope.workspace_id,
        "workspace_revision": scope.workspace_revision,
        "locator": {
            "location_id": scope.locator.location_id,
            "expected_revision": scope.locator.expected_revision,
            "relative_root": scope.locator.relative_root,
        },
    })
}

/// Decode one found Goal row with the established acceptance class.
///
/// # Errors
/// Returns `Protocol` for any missing, malformed, or unknown vocabulary
/// field.
pub fn goal_row_from_wire(readback: &Value) -> Result<ForgeGoal, ForgeSourceError> {
    let goal_id = required_string(readback.get("goal_id"))?;
    let workspace_id = required_string(readback.get("workspace_id"))?;
    canonical_uuid(&goal_id)?;
    canonical_uuid(&workspace_id)?;
    let revision = required_i64(readback.get("revision"))?;
    if revision <= 0 {
        return Err(ForgeSourceError::Protocol);
    }
    let status = readback.get("status").ok_or(ForgeSourceError::Protocol)?;
    let status = ForgeGoalStatus {
        kind: match status
            .get("kind")
            .and_then(Value::as_str)
            .ok_or(ForgeSourceError::Protocol)?
        {
            "active" => ForgeGoalStatusKind::Active,
            "paused" => ForgeGoalStatusKind::Paused,
            "blocked" => ForgeGoalStatusKind::Blocked,
            "completed" => ForgeGoalStatusKind::Completed,
            "failed" => ForgeGoalStatusKind::Failed,
            "cancelled" => ForgeGoalStatusKind::Cancelled,
            _ => return Err(ForgeSourceError::Protocol),
        },
        reason: optional_string(status.get("reason"))?,
    };
    let verification_requirements = readback
        .get("verification_requirements")
        .and_then(Value::as_array)
        .ok_or(ForgeSourceError::Protocol)?
        .iter()
        .map(|requirement| {
            Ok(ForgeGoalVerification {
                kind: match requirement
                    .get("kind")
                    .and_then(Value::as_str)
                    .ok_or(ForgeSourceError::Protocol)?
                {
                    "execution_succeeded" => ForgeVerificationKind::ExecutionSucceeded,
                    "file_present" => ForgeVerificationKind::FilePresent,
                    "change_set_sealed" => ForgeVerificationKind::ChangeSetSealed,
                    "checkpoint_ready" => ForgeVerificationKind::CheckpointReady,
                    _ => return Err(ForgeSourceError::Protocol),
                },
                description: required_string(requirement.get("description"))?,
            })
        })
        .collect::<Result<Vec<_>, ForgeSourceError>>()?;
    Ok(ForgeGoal {
        goal_id,
        workspace_id,
        revision,
        status,
        objective: required_string(readback.get("objective"))?,
        success_criteria: string_list(readback.get("success_criteria"))?,
        constraints: string_list(readback.get("constraints"))?,
        out_of_scope: string_list(readback.get("out_of_scope"))?,
        verification_requirements,
        stop_conditions: string_list(readback.get("stop_conditions"))?,
        created_at_ms: required_i64(readback.get("created_at_ms"))?,
        updated_at_ms: required_i64(readback.get("updated_at_ms"))?,
    })
}

/// Decode one found `dag.read_current` row.
///
/// # Errors
/// Returns `Protocol` for any missing, malformed, or out-of-bounds field.
pub fn dag_current_from_wire(readback: &Value) -> Result<ForgeDagCurrent, ForgeSourceError> {
    let goal_id = required_string(readback.get("goal_id"))?;
    let goal_id = GoalId::from_uuid(canonical_uuid(&goal_id)?).to_string();
    let revision_id = required_string(readback.get("revision_id"))?;
    let revision_id = DagRevisionId::from_uuid(canonical_uuid(&revision_id)?).to_string();
    let base_goal_revision = GoalRevision::new(required_i64(readback.get("base_goal_revision"))?)
        .map_err(|_| ForgeSourceError::Protocol)?;
    let dag_revision = DagRevisionNumber::new(required_i64(readback.get("dag_revision"))?)
        .map_err(|_| ForgeSourceError::Protocol)?;
    let parent_revision = optional_i64(readback.get("parent_revision"))?
        .map(DagRevisionNumber::new)
        .transpose()
        .map_err(|_| ForgeSourceError::Protocol)?;
    let parent_is_valid = match parent_revision {
        None => dag_revision == DagRevisionNumber::INITIAL,
        Some(parent_revision) => parent_revision
            .checked_next()
            .is_ok_and(|next| next == dag_revision),
    };
    if !parent_is_valid {
        return Err(ForgeSourceError::Protocol);
    }
    let created_at_ms = TimestampMillis::new(required_i64(readback.get("created_at_ms"))?)
        .map_err(|_| ForgeSourceError::Protocol)?;
    let head_revision = DagHeadRevision::new(required_i64(readback.get("head_revision"))?)
        .map_err(|_| ForgeSourceError::Protocol)?;
    let head_updated_at_ms =
        TimestampMillis::new(required_i64(readback.get("head_updated_at_ms"))?)
            .map_err(|_| ForgeSourceError::Protocol)?;

    let node_rows = readback
        .get("nodes")
        .and_then(Value::as_array)
        .ok_or(ForgeSourceError::Protocol)?;
    if !(1..=MAX_DAG_NODES).contains(&node_rows.len()) {
        return Err(ForgeSourceError::Protocol);
    }
    let nodes = node_rows
        .iter()
        .map(dag_node_from_wire)
        .collect::<Result<Vec<_>, ForgeSourceError>>()?;

    let node_ids = nodes
        .iter()
        .map(|node| canonical_uuid(&node.node_id).map(DagNodeId::from_uuid))
        .collect::<Result<BTreeSet<_>, ForgeSourceError>>()?;
    for node in &nodes {
        for waiting_on in &node.waiting_on {
            let waiting_on = DagNodeId::from_uuid(canonical_uuid(waiting_on)?);
            if !node_ids.contains(&waiting_on) {
                return Err(ForgeSourceError::Protocol);
            }
        }
    }

    let edge_rows = readback
        .get("edges")
        .and_then(Value::as_array)
        .ok_or(ForgeSourceError::Protocol)?;
    if edge_rows.len() > MAX_DAG_EDGES {
        return Err(ForgeSourceError::Protocol);
    }
    let edges = edge_rows
        .iter()
        .map(|edge| {
            let upstream = required_string(edge.get("upstream"))?;
            let upstream = DagNodeId::from_uuid(canonical_uuid(&upstream)?);
            let downstream = required_string(edge.get("downstream"))?;
            let downstream = DagNodeId::from_uuid(canonical_uuid(&downstream)?);
            if !node_ids.contains(&upstream) || !node_ids.contains(&downstream) {
                return Err(ForgeSourceError::Protocol);
            }
            Ok(ForgeDagEdge {
                upstream: upstream.to_string(),
                downstream: downstream.to_string(),
            })
        })
        .collect::<Result<Vec<_>, ForgeSourceError>>()?;
    Ok(ForgeDagCurrent {
        goal_id,
        revision_id,
        base_goal_revision: base_goal_revision.get(),
        dag_revision: dag_revision.get(),
        parent_revision: parent_revision.map(DagRevisionNumber::get),
        created_at_ms: created_at_ms.get(),
        head_revision: head_revision.get(),
        head_updated_at_ms: head_updated_at_ms.get(),
        nodes,
        edges,
    })
}

fn dag_node_from_wire(node: &Value) -> Result<ForgeDagNode, ForgeSourceError> {
    let node_id = required_string(node.get("node_id"))?;
    let node_id = DagNodeId::from_uuid(canonical_uuid(&node_id)?);
    let ordinal = u16::try_from(required_i64(node.get("ordinal"))?)
        .map_err(|_| ForgeSourceError::Protocol)?;
    let ordinal = DagNodeOrdinal::new(ordinal).map_err(|_| ForgeSourceError::Protocol)?;
    let revision = DagNodeRevision::new(required_i64(node.get("revision"))?)
        .map_err(|_| ForgeSourceError::Protocol)?;
    let created_at_ms = TimestampMillis::new(required_i64(node.get("created_at_ms"))?)
        .map_err(|_| ForgeSourceError::Protocol)?;
    let updated_at_ms = TimestampMillis::new(required_i64(node.get("updated_at_ms"))?)
        .map_err(|_| ForgeSourceError::Protocol)?;
    if updated_at_ms.get() < created_at_ms.get() {
        return Err(ForgeSourceError::Protocol);
    }
    Ok(ForgeDagNode {
        node_id: node_id.to_string(),
        ordinal: ordinal.get(),
        title: required_string(node.get("title"))?,
        objective: required_string(node.get("objective"))?,
        acceptance: string_list(node.get("acceptance"))?,
        lifecycle: match node
            .get("lifecycle")
            .and_then(Value::as_str)
            .ok_or(ForgeSourceError::Protocol)?
        {
            "pending" => ForgeDagLifecycle::Pending,
            "active" => ForgeDagLifecycle::Active,
            "blocked" => ForgeDagLifecycle::Blocked,
            "completed" => ForgeDagLifecycle::Completed,
            "failed" => ForgeDagLifecycle::Failed,
            "skipped" => ForgeDagLifecycle::Skipped,
            "cancelled" => ForgeDagLifecycle::Cancelled,
            _ => return Err(ForgeSourceError::Protocol),
        },
        status: match node
            .get("status")
            .and_then(Value::as_str)
            .ok_or(ForgeSourceError::Protocol)?
        {
            "pending" => ForgeDagNodeStatus::Pending,
            "ready" => ForgeDagNodeStatus::Ready,
            "active" => ForgeDagNodeStatus::Active,
            "blocked" => ForgeDagNodeStatus::Blocked,
            "completed" => ForgeDagNodeStatus::Completed,
            "failed" => ForgeDagNodeStatus::Failed,
            "skipped" => ForgeDagNodeStatus::Skipped,
            "cancelled" => ForgeDagNodeStatus::Cancelled,
            _ => return Err(ForgeSourceError::Protocol),
        },
        waiting_on: string_list(node.get("waiting_on"))?,
        revision: revision.get(),
        evidence_count: u16::try_from(required_i64(node.get("evidence_count"))?)
            .map_err(|_| ForgeSourceError::Protocol)?,
        created_at_ms: created_at_ms.get(),
        updated_at_ms: updated_at_ms.get(),
    })
}

fn string_list(value: Option<&Value>) -> Result<Vec<String>, ForgeSourceError> {
    let list = value
        .and_then(Value::as_array)
        .ok_or(ForgeSourceError::Protocol)?;
    list.iter()
        .map(|entry| required_string(Some(entry)))
        .collect()
}

fn required_string(value: Option<&Value>) -> Result<String, ForgeSourceError> {
    value
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or(ForgeSourceError::Protocol)
}

fn required_i64(value: Option<&Value>) -> Result<i64, ForgeSourceError> {
    value
        .and_then(Value::as_i64)
        .ok_or(ForgeSourceError::Protocol)
}

fn optional_string(value: Option<&Value>) -> Result<Option<String>, ForgeSourceError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.clone())),
        Some(_) => Err(ForgeSourceError::Protocol),
    }
}

fn optional_i64(value: Option<&Value>) -> Result<Option<i64>, ForgeSourceError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value.as_i64().map(Some).ok_or(ForgeSourceError::Protocol),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn goal_readback() -> Value {
        json!({
            "goal_id": "00000000-0000-0000-0000-00000000a701",
            "workspace_id": "00000000-0000-0000-0000-00000000a702",
            "revision": 3,
            "status": {"kind": "active", "reason": null},
            "objective": "Ship the slice",
            "success_criteria": ["tests pass"],
            "constraints": [],
            "out_of_scope": ["docs"],
            "verification_requirements": [
                {"kind": "execution_succeeded", "description": "ci green"}
            ],
            "stop_conditions": ["budget exhausted"],
            "created_at_ms": 10,
            "updated_at_ms": 20,
        })
    }

    #[test]
    fn goal_row_preserves_the_exact_projection() -> TestResult {
        let goal = goal_row_from_wire(&goal_readback())?;
        assert_eq!(goal.goal_id(), "00000000-0000-0000-0000-00000000a701");
        assert_eq!(goal.revision(), 3);
        assert_eq!(goal.status().kind(), ForgeGoalStatusKind::Active);
        assert_eq!(goal.status().reason(), None);
        assert_eq!(goal.verification_requirements().len(), 1);
        assert_eq!(
            goal.verification_requirements()[0].kind(),
            ForgeVerificationKind::ExecutionSucceeded
        );
        assert_eq!(goal.stop_conditions(), ["budget exhausted"]);

        let mut unknown_kind = goal_readback();
        unknown_kind["status"]["kind"] = json!("dreaming");
        assert_eq!(
            goal_row_from_wire(&unknown_kind),
            Err(ForgeSourceError::Protocol)
        );

        // The status reason is an optional field in the established class:
        // an absent reason decodes as None rather than a protocol violation.
        let mut absent_reason = goal_readback();
        absent_reason["status"]
            .as_object_mut()
            .ok_or("status object")?
            .remove("reason");
        assert_eq!(goal_row_from_wire(&absent_reason)?.status().reason(), None);
        Ok(())
    }

    #[test]
    fn dag_current_preserves_nodes_edges_and_generations() -> TestResult {
        let readback = json!({
            "goal_id": "00000000-0000-0000-0000-00000000a701",
            "revision_id": "00000000-0000-0000-0000-00000000a703",
            "base_goal_revision": 2,
            "dag_revision": 1,
            "parent_revision": null,
            "created_at_ms": 30,
            "head_revision": 6,
            "head_updated_at_ms": 40,
            "nodes": [
                {
                    "node_id": "00000000-0000-0000-0000-00000000a704",
                    "ordinal": 0,
                    "title": "Implement",
                    "objective": "Write it",
                    "acceptance": ["done"],
                    "lifecycle": "active",
                    "status": "active",
                    "waiting_on": [],
                    "revision": 2,
                    "evidence_count": 1,
                    "created_at_ms": 31,
                    "updated_at_ms": 32,
                },
                {
                    "node_id": "00000000-0000-0000-0000-00000000a705",
                    "ordinal": 1,
                    "title": "Verify",
                    "objective": "Check it",
                    "acceptance": ["verified"],
                    "lifecycle": "pending",
                    "status": "pending",
                    "waiting_on": ["00000000-0000-0000-0000-00000000a704"],
                    "revision": 1,
                    "evidence_count": 0,
                    "created_at_ms": 33,
                    "updated_at_ms": 33
                }
            ],
            "edges": [
                {
                    "upstream": "00000000-0000-0000-0000-00000000a704",
                    "downstream": "00000000-0000-0000-0000-00000000a705"
                }
            ],
        });
        let dag = dag_current_from_wire(&readback)?;
        assert_eq!(dag.dag_revision(), 1);
        assert_eq!(dag.parent_revision(), None);
        assert_eq!(dag.head_revision(), 6);
        assert_eq!(dag.head_updated_at_ms(), 40);
        assert_eq!(dag.nodes().len(), 2);
        let node = &dag.nodes()[0];
        assert_eq!(node.ordinal(), 0);
        assert_eq!(node.lifecycle(), ForgeDagLifecycle::Active);
        assert_eq!(node.status(), ForgeDagNodeStatus::Active);
        assert_eq!(node.evidence_count(), 1);
        assert_eq!(dag.edges().len(), 1);
        assert_eq!(
            dag.edges()[0].upstream(),
            "00000000-0000-0000-0000-00000000a704"
        );

        let mut oversized_ordinal = readback.clone();
        oversized_ordinal["nodes"][0]["ordinal"] = json!(128);
        assert_eq!(
            dag_current_from_wire(&oversized_ordinal),
            Err(ForgeSourceError::Protocol)
        );

        let mut unknown_status = readback;
        unknown_status["nodes"][0]["status"] = json!("vibing");
        assert_eq!(
            dag_current_from_wire(&unknown_status),
            Err(ForgeSourceError::Protocol)
        );
        Ok(())
    }
}
