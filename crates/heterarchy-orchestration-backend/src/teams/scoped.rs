//! Source-fenced, read-only Team query routes.

use std::collections::{HashMap, HashSet};

use axum::extract::{Path, State};
use axum::routing::post;
use axum::{Extension, Json, Router};
use forge_domain::{AgentSessionId, AuthenticatedPrincipal, TimestampMillis};
use heterarchy_orchestration_operations::{
    TeamCreator, TeamMemberRecord, TeamMutationRecord, TeamRecord, TeamState,
    WorkspaceParticipationId,
};
use heterarchy_orchestration_runtime::{OrchestrationRuntime, RuntimeError};
use uuid::Uuid;

use crate::app_support::validate_workspace_reference;
use crate::wire::teams::{
    TeamCreatorWire, TeamGetQueryRequest, TeamGetQueryResponse, TeamListQueryRequest,
    TeamListQueryResponse, TeamMemberWire, TeamMutationWire, TeamWire,
};
use crate::{ApiError, BackendState};

const TEAM_LIST_LIMIT: u32 = 64;
const TEAM_MEMBER_LIMIT: u32 = 64;

/// Register the new source-scoped read routes. Mutation routes remain owned by
/// the parent Team adapter.
pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/query",
            post(list_query),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/{team_id}/query",
            post(get_query),
        )
}

async fn list_query(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<TeamListQueryRequest>,
) -> Result<Json<TeamListQueryResponse>, ApiError> {
    validate_workspace_reference(&request.scope, request.session_id)?;
    if crate::canonical_uuid(&workspace_path)? != request.scope.workspace_id {
        return Err(ApiError::InvalidIdentity);
    }
    if !(1..=TEAM_LIST_LIMIT).contains(&request.limit) {
        return Err(ApiError::Runtime(RuntimeError::InvalidInput));
    }

    let scope = request.scope.clone();
    let session_id = request.session_id;
    let workspace_id = scope.workspace_id;
    let limit = request.limit;
    let page = state
        .run_workspace_reference_query(scope.clone(), session_id, principal, move |runtime, _| {
            runtime.team_records(workspace_id, limit)
        })
        .await?;

    Ok(Json(TeamListQueryResponse {
        scope,
        session_id,
        limit,
        teams: page.items().iter().map(team_wire).collect(),
        truncated: page.truncated(),
    }))
}

async fn get_query(
    State(state): State<BackendState>,
    Path((workspace_path, team_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<TeamGetQueryRequest>,
) -> Result<Json<TeamGetQueryResponse>, ApiError> {
    validate_workspace_reference(&request.scope, request.session_id)?;
    if crate::canonical_uuid(&workspace_path)? != request.scope.workspace_id
        || crate::canonical_uuid(&team_path)? != request.team_id
    {
        return Err(ApiError::InvalidIdentity);
    }

    let scope = request.scope.clone();
    let session_id = request.session_id;
    let workspace_id = scope.workspace_id;
    let team_id = request.team_id;
    let request_id = request.request_id;
    let (snapshot, profile_names) = state
        .run_workspace_reference_query(scope.clone(), session_id, principal, move |runtime, _| {
            let first = runtime.team_query_snapshot(
                workspace_id,
                team_id,
                TEAM_MEMBER_LIMIT,
                request_id,
            )?;
            let profile_names = resolve_member_profile_names(runtime, first.members.items())?;
            let confirmed = runtime.team_query_snapshot(
                workspace_id,
                team_id,
                TEAM_MEMBER_LIMIT,
                request_id,
            )?;
            if first != confirmed {
                return Err(RuntimeError::ResyncRequired);
            }
            Ok((first, profile_names))
        })
        .await?;

    let members = snapshot
        .members
        .items()
        .iter()
        .map(|member| {
            team_member_wire(
                member,
                member
                    .profile_id()
                    .and_then(|profile_id| profile_names.get(&profile_id))
                    .map(String::as_str),
            )
        })
        .collect();
    Ok(Json(TeamGetQueryResponse {
        scope,
        session_id,
        team_id,
        request_id,
        team: snapshot.team.as_ref().map(team_wire),
        members,
        members_truncated: snapshot.members.truncated(),
        mutation: snapshot.mutation.as_ref().map(team_mutation_wire),
    }))
}

fn resolve_member_profile_names(
    runtime: &OrchestrationRuntime,
    members: &[TeamMemberRecord],
) -> Result<HashMap<Uuid, String>, RuntimeError> {
    let mut names = HashMap::new();
    let mut resolved = HashSet::new();
    for profile_id in members.iter().filter_map(TeamMemberRecord::profile_id) {
        if !resolved.insert(profile_id) {
            continue;
        }
        if let Some(profile) = runtime.agent_profile(profile_id)? {
            names.insert(profile_id, profile.display_name);
        }
    }
    Ok(names)
}

fn team_wire(team: &TeamRecord) -> TeamWire {
    TeamWire {
        team_id: team.team_id(),
        workspace_id: team.workspace_id().into_uuid(),
        name: team.name().to_owned(),
        purpose: team.purpose().to_owned(),
        state: match team.state() {
            TeamState::Active => "active",
            TeamState::Retired => "retired",
        }
        .to_owned(),
        revision: team.revision(),
        created_by: creator_wire(team.created_by()),
        created_at_ms: team.created_at().get(),
        updated_at_ms: team.updated_at().get(),
        formed_at_ms: team.formed_at().map(TimestampMillis::get),
    }
}

fn creator_wire(creator: &TeamCreator) -> TeamCreatorWire {
    TeamCreatorWire {
        key: creator.key().to_owned(),
        display_name: creator.display_name().to_owned(),
        participation_id: creator
            .participation_id()
            .map(WorkspaceParticipationId::into_uuid),
        profile_id: creator.profile_id(),
        participant_id: creator.participant_id(),
        session_id: creator.session_id().map(AgentSessionId::into_uuid),
    }
}

fn team_member_wire(member: &TeamMemberRecord, display_name: Option<&str>) -> TeamMemberWire {
    TeamMemberWire {
        workspace_id: member.workspace_id().into_uuid(),
        team_id: member.team_id(),
        participation_id: member.participation_id().into_uuid(),
        profile_id: member.profile_id(),
        display_name: display_name.map(str::to_owned),
        role: member.role().map(str::to_owned),
        state: heterarchy_orchestration_runtime::teams::membership_state_label(member.state())
            .to_owned(),
        revision: member.revision(),
        joined_at_ms: member.joined_at().get(),
        left_at_ms: member.left_at().map(TimestampMillis::get),
    }
}

pub(super) fn team_mutation_wire(record: &TeamMutationRecord) -> TeamMutationWire {
    TeamMutationWire {
        team: team_wire(record.team()),
        membership: record
            .membership()
            .map(|member| team_member_wire(member, None)),
        initial_members: record
            .initial_members()
            .iter()
            .map(|member| team_member_wire(member, None))
            .collect(),
        created: record.created(),
        replayed: record.replayed(),
    }
}
