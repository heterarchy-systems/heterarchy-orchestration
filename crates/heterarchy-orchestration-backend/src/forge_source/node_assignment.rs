//! Typed Forge observations for exact Goal/DAG/node assignment reads.
//!
//! These values preserve the requested historical node identity alongside
//! Forge's current DAG head. They carry no grant or repository-effect
//! authority.

use forge_backend::ops::CommandOp;
use forge_control::ResolvedWorkspaceScope;
use forge_domain::{
    ActivityId, AgentAssignmentId, AgentAssignmentRevision, AgentAssignmentState, AgentInstanceId,
    AgentSessionId, DagHeadRevision, DagNodeId, DagNodeRevision, DagRevisionNumber, GoalId,
    GoalRevision, TimestampMillis, WorkspaceId,
};
use heterarchy_orchestration_operations::{DagNodeAssignmentSnapshot, DagNodeEvidenceScope};
use heterarchy_orchestration_runtime::node_team::NodeTeamSourceObservation;
use serde_json::{Map, Value, json};
use uuid::Uuid;

use super::{ForgeSourceError, ForgeSourceHttpClient, map_command_error, validated_scope};

/// The current head information returned alongside the requested historical
/// node. `current_dag_revision` may differ from the scope's DAG revision.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeAssignmentHead {
    current_dag_revision: DagRevisionNumber,
    head_revision: DagHeadRevision,
}

impl NodeAssignmentHead {
    /// The current DAG revision observed with the requested node.
    #[must_use]
    pub const fn current_dag_revision(self) -> DagRevisionNumber {
        self.current_dag_revision
    }
}

/// Narrow assignment metadata available from Forge's node readback.
///
/// It intentionally omits assignment fields absent from the wire projection
/// rather than fabricating timestamps, result counts, or full persistence rows.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeAssignmentSummary {
    id: AgentAssignmentId,
    revision: AgentAssignmentRevision,
    target_agent_id: AgentInstanceId,
    target_session_id: AgentSessionId,
    goal_revision: GoalRevision,
    dag_head_revision: DagHeadRevision,
    node_revision: DagNodeRevision,
    state: AgentAssignmentState,
    cancellation_requested: bool,
}

impl NodeAssignmentSummary {
    /// The exact assignment identity returned by Forge.
    #[must_use]
    pub const fn id(self) -> AgentAssignmentId {
        self.id
    }

    /// The exact assignment revision returned by Forge.
    #[must_use]
    pub const fn revision(self) -> AgentAssignmentRevision {
        self.revision
    }

    /// The target Forge Agent instance.
    #[must_use]
    pub const fn target_agent_id(self) -> AgentInstanceId {
        self.target_agent_id
    }

    /// The target logical Agent session.
    pub const fn target_session_id(self) -> AgentSessionId {
        self.target_session_id
    }

    /// The active lifecycle state returned by Forge.
    #[must_use]
    pub const fn state(self) -> AgentAssignmentState {
        self.state
    }

    /// Whether Forge has requested cancellation for this assignment.
    #[must_use]
    pub const fn cancellation_requested(self) -> bool {
        self.cancellation_requested
    }
}

/// Exact typed observation needed by node readers over either Forge transport.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeAssignmentObservation {
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    goal_created_at: TimestampMillis,
    dag_base_goal_revision: GoalRevision,
    goal_revision: GoalRevision,
    scope: DagNodeEvidenceScope,
    head: NodeAssignmentHead,
    node_revision: DagNodeRevision,
    assignment: Option<NodeAssignmentSummary>,
}

impl NodeAssignmentObservation {
    /// The exact Workspace identity returned by Forge.
    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        self.workspace_id
    }

    /// The exact Activity identity returned by Forge.
    #[must_use]
    pub const fn activity_id(self) -> ActivityId {
        self.activity_id
    }

    /// The exact Goal creation timestamp returned by Forge.
    #[must_use]
    pub const fn goal_created_at(self) -> TimestampMillis {
        self.goal_created_at
    }

    /// The Goal revision from which the observed DAG was based.
    #[must_use]
    pub const fn dag_base_goal_revision(self) -> GoalRevision {
        self.dag_base_goal_revision
    }

    /// The current Goal revision returned by Forge.
    #[must_use]
    pub const fn goal_revision(self) -> GoalRevision {
        self.goal_revision
    }

    /// The requested historical Goal/DAG/node identity.
    pub const fn scope(self) -> DagNodeEvidenceScope {
        self.scope
    }

    /// The current DAG head observed alongside the requested node.
    pub const fn head(self) -> NodeAssignmentHead {
        self.head
    }

    /// The exact node revision returned by Forge.
    #[must_use]
    pub const fn node_revision(self) -> DagNodeRevision {
        self.node_revision
    }

    /// The active assignment, if Forge observed one for this node.
    #[must_use]
    pub const fn assignment(self) -> Option<NodeAssignmentSummary> {
        self.assignment
    }

    /// Convert this Forge observation into the exact Operations node-team source evidence.
    ///
    /// The requested DAG generation remains the binding scope even when the
    /// current DAG head has advanced. The current head revision is returned as
    /// separate source evidence.
    ///
    /// # Errors
    /// Returns `Protocol` if the checked identity cannot form an Operations scope.
    pub fn node_team_evidence(self) -> Result<NodeTeamSourceObservation, ForgeSourceError> {
        let scope = heterarchy_orchestration_operations::NodeTeamBindingScope::new(
            self.workspace_id,
            self.activity_id,
            self.scope.goal_id(),
            self.goal_created_at,
            self.scope.dag_revision(),
            self.dag_base_goal_revision,
            self.scope.node_id(),
        )
        .map_err(|_| ForgeSourceError::Protocol)?;

        Ok(NodeTeamSourceObservation {
            scope,
            current_dag_revision: self.head.current_dag_revision,
            assignment_present: self.assignment.is_some(),
        })
    }

    /// Supply the complete immutable node route observation to Runtime.
    /// This conversion does not grant message or repository-effect authority.
    ///
    /// # Errors
    /// Rejects an invalid exact node generation in the Source observation.
    pub fn chat_evidence(
        self,
    ) -> Result<heterarchy_orchestration_runtime::chat::ChatSourceObservation, ForgeSourceError>
    {
        let node = self.node_team_evidence()?;
        Ok(
            heterarchy_orchestration_runtime::chat::ChatSourceObservation {
                scope: node.scope,
                current_dag_revision: node.current_dag_revision,
                assignment: self.assignment.map(|assignment| {
                    heterarchy_orchestration_runtime::chat::ChatSourceAssignment {
                        id: assignment.id(),
                        revision: assignment.revision(),
                        target_agent_id: assignment.target_agent_id(),
                        target_session_id: assignment.target_session_id(),
                        state: assignment.state(),
                        cancellation_requested: assignment.cancellation_requested(),
                    }
                }),
            },
        )
    }
}

impl ForgeSourceHttpClient {
    /// Read the exact Goal/DAG/node assignment observation from configured Forge.
    ///
    /// # Errors
    /// Returns an invalid-scope, stale, denied, unavailable, or protocol error.
    pub async fn node_assignment(
        &self,
        expected: &ResolvedWorkspaceScope,
        node_scope: DagNodeEvidenceScope,
    ) -> Result<Option<NodeAssignmentObservation>, ForgeSourceError> {
        let payload = node_assignment_read_wire(expected, node_scope)?;
        let readback = self
            .commands()
            .command(CommandOp::NodeAssignmentRead, payload)
            .await
            .map_err(map_command_error)?;
        node_assignment_read_output_from_wire(&readback, expected, node_scope)
    }
}

/// Normalize the in-process Forge snapshot to the same typed observation used
/// by the configured HTTP source.
pub fn node_assignment_observation_from_snapshot(
    snapshot: &DagNodeAssignmentSnapshot,
) -> NodeAssignmentObservation {
    let head = snapshot.head();
    NodeAssignmentObservation {
        workspace_id: snapshot.workspace_id(),
        activity_id: snapshot.activity_id(),
        goal_created_at: snapshot.goal_created_at(),
        dag_base_goal_revision: snapshot.dag_base_goal_revision(),
        goal_revision: snapshot.goal_revision(),
        scope: snapshot.scope(),
        head: NodeAssignmentHead {
            current_dag_revision: head.dag_revision(),
            head_revision: head.revision(),
        },
        node_revision: snapshot.node_revision(),
        assignment: snapshot
            .assignment()
            .map(NodeAssignmentSummary::from_summary),
    }
}

/// Build the bounded Forge command payload for one exact node-assignment read.
///
/// # Errors
/// Returns `InvalidScope` if the trusted Workspace scope cannot be represented
/// by the Forge source protocol.
pub fn node_assignment_read_wire(
    expected: &ResolvedWorkspaceScope,
    node_scope: DagNodeEvidenceScope,
) -> Result<Value, ForgeSourceError> {
    let (scope, _) = validated_scope(expected)?;
    Ok(json!({
        "scope": scope,
        "goal_id": node_scope.goal_id().into_uuid(),
        "dag_revision": node_scope.dag_revision().get(),
        "node_id": node_scope.node_id().into_uuid(),
    }))
}

/// Decode the strict Forge projection, requiring the returned identity to match
/// the exact Workspace and requested historical Goal/DAG/node scope.
///
/// # Errors
/// Returns `Protocol` if a required field is missing, unknown, malformed, or
/// inconsistent with the requested identity.
pub fn node_assignment_read_output_from_wire(
    readback: &Value,
    expected_workspace: &ResolvedWorkspaceScope,
    requested_scope: DagNodeEvidenceScope,
) -> Result<Option<NodeAssignmentObservation>, ForgeSourceError> {
    let Some(found) = readback.get("found").and_then(Value::as_bool) else {
        return Err(ForgeSourceError::Protocol);
    };
    if !found {
        require_exact_fields(readback, &["found"])?;
        return Ok(None);
    }
    require_exact_fields(
        readback,
        &[
            "found",
            "workspace_id",
            "activity_id",
            "goal_id",
            "goal_revision",
            "goal_created_at_ms",
            "dag_base_goal_revision",
            "dag_revision",
            "head_revision",
            "current_dag_revision",
            "node_id",
            "node_revision",
            "assignment",
        ],
    )?;

    let workspace_id = WorkspaceId::from_uuid(required_uuid(readback.get("workspace_id"))?);
    let activity_id = ActivityId::from_uuid(required_uuid(readback.get("activity_id"))?);
    let goal_id = GoalId::from_uuid(required_uuid(readback.get("goal_id"))?);
    let dag_revision =
        DagRevisionNumber::new(required_positive_revision(readback.get("dag_revision"))?)
            .map_err(|_| ForgeSourceError::Protocol)?;
    let node_id = DagNodeId::from_uuid(required_uuid(readback.get("node_id"))?);
    if workspace_id != expected_workspace.workspace_id()
        || goal_id != requested_scope.goal_id()
        || dag_revision != requested_scope.dag_revision()
        || node_id != requested_scope.node_id()
    {
        return Err(ForgeSourceError::Protocol);
    }

    let goal_revision =
        GoalRevision::new(required_positive_revision(readback.get("goal_revision"))?)
            .map_err(|_| ForgeSourceError::Protocol)?;
    let goal_created_at = TimestampMillis::new(
        readback
            .get("goal_created_at_ms")
            .and_then(Value::as_i64)
            .ok_or(ForgeSourceError::Protocol)?,
    )
    .map_err(|_| ForgeSourceError::Protocol)?;
    let dag_base_goal_revision = GoalRevision::new(required_positive_revision(
        readback.get("dag_base_goal_revision"),
    )?)
    .map_err(|_| ForgeSourceError::Protocol)?;
    let current_dag_revision = DagRevisionNumber::new(required_positive_revision(
        readback.get("current_dag_revision"),
    )?)
    .map_err(|_| ForgeSourceError::Protocol)?;
    let head_revision =
        DagHeadRevision::new(required_positive_revision(readback.get("head_revision"))?)
            .map_err(|_| ForgeSourceError::Protocol)?;
    let node_revision =
        DagNodeRevision::new(required_positive_revision(readback.get("node_revision"))?)
            .map_err(|_| ForgeSourceError::Protocol)?;
    let assignment = decode_assignment(readback.get("assignment"))?;

    Ok(Some(NodeAssignmentObservation {
        workspace_id,
        activity_id,
        goal_created_at,
        dag_base_goal_revision,
        goal_revision,
        scope: requested_scope,
        head: NodeAssignmentHead {
            current_dag_revision,
            head_revision,
        },
        node_revision,
        assignment,
    }))
}

impl NodeAssignmentSummary {
    fn from_summary(summary: forge_domain::AgentAssignmentSummary) -> Self {
        Self {
            id: summary.id(),
            revision: summary.revision(),
            target_agent_id: summary.target_agent_id(),
            target_session_id: summary.target_session_id(),
            goal_revision: summary.goal_revision(),
            dag_head_revision: summary.dag_head_revision(),
            node_revision: summary.node_revision(),
            state: summary.state(),
            cancellation_requested: summary.cancellation_requested(),
        }
    }
}

fn decode_assignment(
    value: Option<&Value>,
) -> Result<Option<NodeAssignmentSummary>, ForgeSourceError> {
    let value = value.ok_or(ForgeSourceError::Protocol)?;
    if value.is_null() {
        return Ok(None);
    }
    require_exact_fields(
        value,
        &[
            "assignment_id",
            "assignment_revision",
            "target_agent_id",
            "target_session_id",
            "goal_revision",
            "dag_head_revision",
            "node_revision",
            "state",
            "cancellation_requested",
        ],
    )?;
    let cancellation_requested = value
        .get("cancellation_requested")
        .and_then(Value::as_bool)
        .ok_or(ForgeSourceError::Protocol)?;
    let assignment = NodeAssignmentSummary {
        id: AgentAssignmentId::from_uuid(required_uuid(value.get("assignment_id"))?),
        revision: AgentAssignmentRevision::new(required_positive_revision(
            value.get("assignment_revision"),
        )?)
        .map_err(|_| ForgeSourceError::Protocol)?,
        target_agent_id: AgentInstanceId::from_uuid(required_uuid(value.get("target_agent_id"))?),
        target_session_id: AgentSessionId::from_uuid(required_uuid(
            value.get("target_session_id"),
        )?),
        goal_revision: GoalRevision::new(required_positive_revision(value.get("goal_revision"))?)
            .map_err(|_| ForgeSourceError::Protocol)?,
        dag_head_revision: DagHeadRevision::new(required_positive_revision(
            value.get("dag_head_revision"),
        )?)
        .map_err(|_| ForgeSourceError::Protocol)?,
        node_revision: DagNodeRevision::new(required_positive_revision(
            value.get("node_revision"),
        )?)
        .map_err(|_| ForgeSourceError::Protocol)?,
        state: decode_assignment_state(value.get("state"))?,
        cancellation_requested,
    };
    Ok(Some(assignment))
}

fn decode_assignment_state(
    value: Option<&Value>,
) -> Result<AgentAssignmentState, ForgeSourceError> {
    let state = match value
        .and_then(Value::as_str)
        .ok_or(ForgeSourceError::Protocol)?
    {
        "offered" => AgentAssignmentState::Offered,
        "running" => AgentAssignmentState::Running,
        "submitted" => AgentAssignmentState::Submitted,
        "accepted" => AgentAssignmentState::Accepted,
        "rejected" => AgentAssignmentState::Rejected,
        "cancelled" => AgentAssignmentState::Cancelled,
        "interrupted" => AgentAssignmentState::Interrupted,
        _ => return Err(ForgeSourceError::Protocol),
    };
    if state.is_active() {
        Ok(state)
    } else {
        Err(ForgeSourceError::Protocol)
    }
}

fn required_uuid(value: Option<&Value>) -> Result<Uuid, ForgeSourceError> {
    let raw = value
        .and_then(Value::as_str)
        .ok_or(ForgeSourceError::Protocol)?;
    let id = Uuid::parse_str(raw).map_err(|_| ForgeSourceError::Protocol)?;
    if id.is_nil() || id.to_string() != raw {
        return Err(ForgeSourceError::Protocol);
    }
    Ok(id)
}

fn required_positive_revision(value: Option<&Value>) -> Result<i64, ForgeSourceError> {
    let revision = value
        .and_then(Value::as_i64)
        .ok_or(ForgeSourceError::Protocol)?;
    if revision < 1 {
        return Err(ForgeSourceError::Protocol);
    }
    Ok(revision)
}

fn require_exact_fields(value: &Value, fields: &[&str]) -> Result<(), ForgeSourceError> {
    let object: &Map<String, Value> = value.as_object().ok_or(ForgeSourceError::Protocol)?;
    if object.len() != fields.len() || fields.iter().any(|field| !object.contains_key(*field)) {
        return Err(ForgeSourceError::Protocol);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_domain::{LocationId, LocationRevision, WorkspaceRevision};
    use forge_workspace::WorkspaceLocator;

    fn workspace_scope() -> Result<ResolvedWorkspaceScope, Box<dyn std::error::Error>> {
        let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(0x301));
        let location_id = LocationId::from_uuid(Uuid::from_u128(0x302));
        let locator = WorkspaceLocator::new(location_id, LocationRevision::new(5)?, ".")?;
        Ok(ResolvedWorkspaceScope::from_observation(
            workspace_id,
            WorkspaceRevision::new(8)?,
            locator,
        ))
    }

    fn requested_scope() -> Result<DagNodeEvidenceScope, Box<dyn std::error::Error>> {
        Ok(DagNodeEvidenceScope::new(
            GoalId::from_uuid(Uuid::from_u128(0x303)),
            DagRevisionNumber::new(1)?,
            DagNodeId::from_uuid(Uuid::from_u128(0x304)),
        ))
    }

    fn active_assignment() -> Value {
        json!({
            "assignment_id": Uuid::from_u128(0x408),
            "assignment_revision": 13,
            "target_agent_id": Uuid::from_u128(0x409),
            "target_session_id": Uuid::from_u128(0x40a),
            "goal_revision": 8,
            "dag_head_revision": 9,
            "node_revision": 3,
            "state": "running",
            "cancellation_requested": false,
        })
    }

    fn found_readback(
        workspace: &ResolvedWorkspaceScope,
        requested: DagNodeEvidenceScope,
        assignment: &Value,
    ) -> Value {
        json!({
            "found": true,
            "workspace_id": workspace.workspace_id().into_uuid(),
            "activity_id": Uuid::from_u128(0x305),
            "goal_id": requested.goal_id().into_uuid(),
            "goal_revision": 8,
            "goal_created_at_ms": 42,
            "dag_base_goal_revision": 5,
            "dag_revision": requested.dag_revision().get(),
            "head_revision": 9,
            "current_dag_revision": 2,
            "node_id": requested.node_id().into_uuid(),
            "node_revision": 3,
            "assignment": assignment,
        })
    }

    #[test]
    fn full_assignment_readback_preserves_historical_scope_and_current_head()
    -> Result<(), Box<dyn std::error::Error>> {
        let workspace = workspace_scope()?;
        let requested = requested_scope()?;
        let readback = found_readback(&workspace, requested, &active_assignment());

        let observation = node_assignment_read_output_from_wire(&readback, &workspace, requested)?
            .ok_or("expected a found node assignment observation")?;
        assert_eq!(observation.workspace_id(), workspace.workspace_id());
        assert_eq!(
            observation.activity_id(),
            ActivityId::from_uuid(Uuid::from_u128(0x305))
        );
        assert_eq!(observation.goal_created_at(), TimestampMillis::new(42)?);
        assert_eq!(observation.dag_base_goal_revision(), GoalRevision::new(5)?);
        assert_eq!(observation.goal_revision(), GoalRevision::new(8)?);
        assert_eq!(observation.scope(), requested);
        assert_eq!(
            observation.head().current_dag_revision(),
            DagRevisionNumber::new(2)?
        );
        assert_eq!(observation.node_revision(), DagNodeRevision::new(3)?);

        let assignment = observation
            .assignment()
            .ok_or("expected the complete active assignment")?;
        assert_eq!(
            assignment.id(),
            AgentAssignmentId::from_uuid(Uuid::from_u128(0x408))
        );
        assert_eq!(assignment.revision(), AgentAssignmentRevision::new(13)?);
        assert_eq!(
            assignment.target_agent_id(),
            AgentInstanceId::from_uuid(Uuid::from_u128(0x409))
        );
        assert_eq!(
            assignment.target_session_id(),
            AgentSessionId::from_uuid(Uuid::from_u128(0x40a))
        );
        assert_eq!(assignment.state(), AgentAssignmentState::Running);
        assert!(!assignment.cancellation_requested());

        let evidence = observation.node_team_evidence()?;
        assert_eq!(evidence.scope.workspace_id(), workspace.workspace_id());
        assert_eq!(evidence.scope.activity_id(), observation.activity_id());
        assert_eq!(evidence.scope.goal_id(), requested.goal_id());
        assert_eq!(
            evidence.scope.goal_created_at(),
            observation.goal_created_at()
        );
        assert_eq!(evidence.scope.dag_revision(), requested.dag_revision());
        assert_eq!(
            evidence.scope.dag_base_goal_revision(),
            observation.dag_base_goal_revision()
        );
        assert_eq!(evidence.scope.node_id(), requested.node_id());
        assert_eq!(evidence.current_dag_revision, DagRevisionNumber::new(2)?);
        assert!(evidence.assignment_present);
        Ok(())
    }

    #[test]
    fn decoder_requires_exact_identity_and_explicit_nullable_assignment()
    -> Result<(), Box<dyn std::error::Error>> {
        let workspace = workspace_scope()?;
        let requested = requested_scope()?;
        assert_eq!(
            node_assignment_read_output_from_wire(
                &json!({ "found": false }),
                &workspace,
                requested
            )?,
            None
        );

        let unknown_not_found = json!({ "found": false, "extra": true });
        assert_eq!(
            node_assignment_read_output_from_wire(&unknown_not_found, &workspace, requested),
            Err(ForgeSourceError::Protocol)
        );

        let valid = found_readback(&workspace, requested, &Value::Null);
        assert_eq!(
            node_assignment_read_output_from_wire(&valid, &workspace, requested)?
                .and_then(NodeAssignmentObservation::assignment),
            None
        );

        let mut missing_assignment = valid.clone();
        missing_assignment
            .as_object_mut()
            .ok_or("object")?
            .remove("assignment");
        assert_eq!(
            node_assignment_read_output_from_wire(&missing_assignment, &workspace, requested),
            Err(ForgeSourceError::Protocol)
        );

        let mut unknown_top_level = valid.clone();
        unknown_top_level["extra"] = json!(true);
        assert_eq!(
            node_assignment_read_output_from_wire(&unknown_top_level, &workspace, requested),
            Err(ForgeSourceError::Protocol)
        );

        let mut unknown_assignment = found_readback(&workspace, requested, &active_assignment());
        unknown_assignment["assignment"]["extra"] = json!(true);
        assert_eq!(
            node_assignment_read_output_from_wire(&unknown_assignment, &workspace, requested),
            Err(ForgeSourceError::Protocol)
        );

        let mut terminal_assignment = found_readback(&workspace, requested, &active_assignment());
        terminal_assignment["assignment"]["state"] = json!("accepted");
        assert_eq!(
            node_assignment_read_output_from_wire(&terminal_assignment, &workspace, requested),
            Err(ForgeSourceError::Protocol)
        );

        let mut wrong_generation = valid.clone();
        wrong_generation["dag_revision"] = json!(2);
        assert_eq!(
            node_assignment_read_output_from_wire(&wrong_generation, &workspace, requested),
            Err(ForgeSourceError::Protocol)
        );

        let mut noncanonical_activity = valid;
        noncanonical_activity["activity_id"] = json!(
            Uuid::from_u128(0x00ab_cdef)
                .to_string()
                .to_ascii_uppercase()
        );
        assert_eq!(
            node_assignment_read_output_from_wire(&noncanonical_activity, &workspace, requested),
            Err(ForgeSourceError::Protocol)
        );
        Ok(())
    }
}
