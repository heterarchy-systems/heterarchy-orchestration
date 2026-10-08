//! Exact generation Node-Team queries through Runtime and Source scope proof.

use crate::{
    ApiError, BackendState, RuntimeError,
    wire::node_team::{
        NodeTeamQueryRequest, NodeTeamQueryResponse, NodeTeamScopeWire, NodeTeamSetRequest,
        NodeTeamSetResponse, NodeTeamSnapshotWire,
    },
};
use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    routing::post,
};
use forge_domain::{
    ActivityId, AuthenticatedPrincipal, DagNodeId, DagRevisionNumber, GoalId, GoalRevision,
    TimestampMillis, WorkspaceId,
};
use heterarchy_orchestration_operations::{
    AssignmentAffinityDisposition, AssignmentAffinityReceipt, NodeTeamBinding, NodeTeamBindingScope,
};
use heterarchy_orchestration_runtime::participation::ParticipationTarget;
mod projection;

pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/node-teams/query",
            post(query),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/node-teams/set",
            post(set),
        )
}

async fn set(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<NodeTeamSetRequest>,
) -> Result<Json<NodeTeamSetResponse>, ApiError> {
    crate::app_support::validate_workspace_reference(&body.scope, Some(body.session_id))?;
    if crate::canonical_uuid(&path)? != body.scope.workspace_id
        || body.sender_activity_id.is_nil()
        || body.sender_leadership_revision <= 0
        || body
            .expected_revision
            .is_some_and(|revision| revision == 0 || revision >= i64::MAX as u64)
        || body.team_id.is_some_and(|id| id.is_nil())
    {
        return Err(ApiError::InvalidIdentity);
    }
    let scope = node_scope(body.scope.workspace_id, &body.node_scope)?;
    let target = ParticipationTarget {
        workspace_id: body.scope.workspace_id,
        activity_id: body.sender_activity_id,
        agent_id: heterarchy_orchestration_runtime::leadership::member_agent_id(
            body.scope.workspace_id,
            body.sender_activity_id,
            body.session_id,
        )
        .into_uuid(),
        session_id: body.session_id,
    };
    let owner = state.clone();
    state
        .run_owned(async move {
            owner
                .catalog_job(move |runtime| runtime.require_participation(target, principal))
                .await?;
            owner.workspace_scope_proof(body.scope.clone()).await?;
            let authority = owner
                .coordinator_authority
                .as_ref()
                .ok_or(ApiError::WorkspaceScopeUnavailable)?;
            let permit = owner
                .forge_query_slots
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| ApiError::Unavailable)?;
            let observation = authority
                .node_assignment_observation(body.scope.clone(), scope, permit)
                .await
                .map_err(|error| match error {
                    crate::CoordinatorAuthorityError::Denied => {
                        ApiError::Runtime(RuntimeError::Forbidden)
                    }
                    crate::CoordinatorAuthorityError::Stale => {
                        ApiError::Runtime(RuntimeError::Conflict)
                    }
                    crate::CoordinatorAuthorityError::Unavailable => {
                        ApiError::WorkspaceScopeUnavailable
                    }
                })?
                .ok_or(ApiError::Runtime(RuntimeError::Conflict))?;
            let source = observation
                .node_team_evidence()
                .map_err(|_| ApiError::WorkspaceScopeUnavailable)?;
            let observed_at = TimestampMillis::new(
                crate::app_support::now_millis().map_err(|_| ApiError::Unavailable)?,
            )
            .map_err(|_| ApiError::Unavailable)?;
            let command = heterarchy_orchestration_runtime::node_team::NodeTeamSet {
                scope,
                expected_revision: body.expected_revision,
                team_id: body.team_id,
                expected_sender_leadership_revision: Some(body.sender_leadership_revision),
                observed_at,
            };
            let binding = owner
                .catalog_job(move |runtime| {
                    runtime.set_node_team(target, principal, command, source)
                })
                .await?;
            Ok(Json(NodeTeamSetResponse {
                request: body,
                binding: project_binding(&binding),
            }))
        })
        .await
}

async fn query(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<NodeTeamQueryRequest>,
) -> Result<Json<NodeTeamQueryResponse>, ApiError> {
    crate::app_support::validate_workspace_reference(&body.scope, Some(body.session_id))?;
    if crate::canonical_uuid(&path)? != body.scope.workspace_id || body.sender_activity_id.is_nil()
    {
        return Err(ApiError::InvalidIdentity);
    }
    let scope = node_scope(body.scope.workspace_id, &body.node_scope)?;
    let target = ParticipationTarget {
        workspace_id: body.scope.workspace_id,
        activity_id: body.sender_activity_id,
        agent_id: heterarchy_orchestration_runtime::leadership::member_agent_id(
            body.scope.workspace_id,
            body.sender_activity_id,
            body.session_id,
        )
        .into_uuid(),
        session_id: body.session_id,
    };
    let after = body
        .after_assignment_id
        .map(forge_domain::AgentAssignmentId::from_uuid);
    let owner = state.clone();
    state
        .run_owned(async move {
            owner
                .catalog_job(move |runtime| runtime.require_participation(target, principal))
                .await?;
            owner.workspace_scope_proof(body.scope.clone()).await?;
            let read = owner
                .catalog_job(move |runtime| runtime.read_node_team(target, principal, scope, after))
                .await?;
            let snapshot = project_read(&read, scope, body.node_scope, body.after_assignment_id)
                .map_err(ApiError::Runtime)?;
            Ok(Json(NodeTeamQueryResponse {
                request: body,
                snapshot,
            }))
        })
        .await
}

pub use projection::{binding as project_binding, retirement as project_retirement};

pub(crate) fn node_scope(
    workspace: uuid::Uuid,
    value: &NodeTeamScopeWire,
) -> Result<NodeTeamBindingScope, ApiError> {
    NodeTeamBindingScope::new(
        WorkspaceId::from_uuid(workspace),
        ActivityId::from_uuid(value.activity_id),
        GoalId::from_uuid(value.goal_id),
        TimestampMillis::new(value.goal_created_at_ms).map_err(|_| ApiError::InvalidIdentity)?,
        DagRevisionNumber::new(value.dag_revision).map_err(|_| ApiError::InvalidIdentity)?,
        GoalRevision::new(value.dag_base_goal_revision).map_err(|_| ApiError::InvalidIdentity)?,
        DagNodeId::from_uuid(value.node_id),
    )
    .map_err(|_| ApiError::InvalidIdentity)
}

/// Preserve the full existing Node-Team read projection from canonical owner data.
///
/// # Errors
/// Rejects mismatched generation coordinates or malformed owner output.
pub fn project_read(
    read: &heterarchy_orchestration_runtime::node_team::NodeTeamRead,
    scope: NodeTeamBindingScope,
    node_scope: NodeTeamScopeWire,
    after_assignment_id: Option<uuid::Uuid>,
) -> Result<NodeTeamSnapshotWire, RuntimeError> {
    if node_scope.activity_id != scope.activity_id().into_uuid()
        || node_scope.goal_id != scope.goal_id().into_uuid()
        || node_scope.goal_created_at_ms != scope.goal_created_at().get()
        || node_scope.dag_revision != scope.dag_revision().get()
        || node_scope.dag_base_goal_revision != scope.dag_base_goal_revision().get()
        || node_scope.node_id != scope.node_id().into_uuid()
    {
        return Err(RuntimeError::InvalidInput);
    }
    projection::snapshot(read, scope, node_scope, after_assignment_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn exact_node_generation_rejects_missing_identity_and_invalid_revision() {
        let value = NodeTeamScopeWire {
            activity_id: Uuid::from_u128(2),
            goal_id: Uuid::from_u128(3),
            goal_created_at_ms: 1,
            dag_revision: 1,
            dag_base_goal_revision: 1,
            node_id: Uuid::from_u128(4),
        };
        assert!(node_scope(Uuid::from_u128(1), &value).is_ok());
        let mut changed = value;
        changed.goal_created_at_ms = -1;
        assert!(node_scope(Uuid::from_u128(1), &changed).is_err());
        changed = value;
        changed.node_id = Uuid::nil();
        assert!(node_scope(Uuid::from_u128(1), &changed).is_err());
        changed = value;
        changed.dag_base_goal_revision = 0;
        assert!(node_scope(Uuid::from_u128(1), &changed).is_err());
    }
}
