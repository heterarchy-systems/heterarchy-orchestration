//! Source-fenced Workspace member-list HTTP query.

use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    routing::post,
};
use forge_domain::{AgentTransportKind, AuthenticatedPrincipal};
use heterarchy_orchestration_operations::VisibleLogicalMember;

use crate::wire::member_list::{LogicalMemberWire, MemberListRequest, MemberListResponse};
use crate::wire::membership::MemberTransportWire;
use crate::{ApiError, BackendState, app_support::validate_workspace_reference};

pub(crate) fn routes() -> Router<BackendState> {
    Router::new().route(
        "/internal/v1/workspaces/{workspace_id}/members/list",
        post(list),
    )
}

async fn list(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<MemberListRequest>,
) -> Result<Json<MemberListResponse>, ApiError> {
    validate_workspace_reference(&body.scope, body.session_id)?;
    if crate::canonical_uuid(&path)? != body.scope.workspace_id || !(1..=64).contains(&body.limit) {
        return Err(ApiError::InvalidIdentity);
    }
    let workspace_id = body.scope.workspace_id;
    let session_id = body.session_id;
    let limit = body.limit;
    let members = state
        .run_workspace_reference_query(
            body.scope.clone(),
            session_id,
            principal,
            move |runtime, _| runtime.list_members(workspace_id, session_id, principal, limit),
        )
        .await?;
    Ok(Json(MemberListResponse {
        scope: body.scope,
        session_id,
        limit,
        members: members.iter().map(member_wire).collect(),
    }))
}

/// Project one canonical logical member without adding authority or transport state.
#[must_use]
pub fn member_wire(member: &VisibleLogicalMember) -> LogicalMemberWire {
    LogicalMemberWire {
        workspace_id: member.workspace_id().into_uuid(),
        participant_id: member.participant_id(),
        session_id: member.session_id().into_uuid(),
        display_name: member.display_name().as_str().to_owned(),
        declared_role: member.declared_role().as_str().to_owned(),
        transport: match member.transport() {
            AgentTransportKind::Retired => MemberTransportWire::Mcp,
            AgentTransportKind::Stdio => MemberTransportWire::Stdio,
            AgentTransportKind::StreamableHttp => MemberTransportWire::StreamableHttp,
            AgentTransportKind::Other => MemberTransportWire::Other,
        },
        membership_revision: member.membership_revision(),
    }
}
