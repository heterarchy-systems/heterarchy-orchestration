//! Source-fenced Team commands returning complete immutable owner receipts.
use crate::{
    ApiError, BackendState, RealtimeEventKind,
    app_support::{LaneError, validate_workspace_reference},
    wire::{
        WorkspaceQueryScope,
        team_commands::{
            TeamCreateRequest, TeamJoinRequest, TeamLeaveRequest, TeamMutationResponse,
            TeamRetireRequest,
        },
    },
};
use axum::{
    Json, Router,
    extract::{Extension, Path, State},
    routing::post,
};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_operations::TeamMutationRecord;
use heterarchy_orchestration_runtime::{
    OrchestrationRuntime, RuntimeError,
    teams::{TeamCreateSpec, TeamJoinSpec, TeamLeaveSpec, TeamRosterMemberSpec},
};
use uuid::Uuid;
pub(super) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/create",
            post(create),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/{team_id}/join",
            post(join),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/{team_id}/leave",
            post(leave),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/{team_id}/retire",
            post(retire),
        )
}
fn validate(
    workspace: &str,
    team: Option<&str>,
    scope: &WorkspaceQueryScope,
    team_id: Uuid,
    request: Uuid,
    session: Option<Uuid>,
) -> Result<(), LaneError> {
    validate_workspace_reference(scope, session)?;
    if crate::canonical_uuid(workspace)? != scope.workspace_id
        || team_id.is_nil()
        || request.is_nil()
    {
        return Err(LaneError::InvalidIdentity);
    }
    if let Some(team) = team
        && crate::canonical_uuid(team)? != team_id
    {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(())
}
#[derive(Clone, Copy)]
enum TeamAuthority {
    Participant { session: Uuid },
    Operator,
}
async fn create(
    State(state): State<BackendState>,
    Path(workspace): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<TeamCreateRequest>,
) -> Result<Json<TeamMutationResponse>, LaneError> {
    validate(
        &workspace,
        None,
        &body.scope,
        body.team_id,
        body.request_id,
        Some(body.session_id),
    )?;
    if body.activity_id.is_nil()
        || body.expected_sender_leadership_revision < 1
        || !(2..=64).contains(&body.initial_members.len())
    {
        return Err(LaneError::InvalidIdentity);
    }
    let mut activities = vec![body.activity_id];
    for member in &body.initial_members {
        if member.activity_id.is_nil() || member.session_id.is_nil() {
            return Err(LaneError::InvalidIdentity);
        }
        activities.push(member.activity_id);
    }
    let scope = body.scope;
    let request = body.request_id;
    let team = body.team_id;
    let actor = TeamAuthority::Participant {
        session: body.session_id,
    };
    let mut spec = TeamCreateSpec {
        request_id: request,
        workspace_id: scope.workspace_id,
        team_id: team,
        activity_id: body.activity_id,
        session_id: body.session_id,
        expected_sender_leadership_revision: Some(body.expected_sender_leadership_revision),
        name: body.name,
        purpose: body.purpose,
        initial_members: body
            .initial_members
            .into_iter()
            .map(|m| TeamRosterMemberSpec {
                activity_id: m.activity_id,
                session_id: m.session_id,
                role: m.role,
            })
            .collect(),
        observed_at_ms: 0,
    };
    let record = state
        .run_scoped_team(
            scope.clone(),
            actor,
            principal,
            activities,
            move |runtime, now| {
                spec.observed_at_ms = now;
                runtime.create_team(spec, principal)
            },
        )
        .await?;
    Ok(Json(response(scope, request, team, &record)))
}
async fn join(
    State(state): State<BackendState>,
    Path((workspace, team_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<TeamJoinRequest>,
) -> Result<Json<TeamMutationResponse>, LaneError> {
    validate(
        &workspace,
        Some(&team_path),
        &body.scope,
        body.team_id,
        body.request_id,
        Some(body.session_id),
    )?;
    if body.activity_id.is_nil()
        || body.expected_team_revision < 1
        || body.expected_team_revision > i64::MAX as u64
    {
        return Err(LaneError::InvalidIdentity);
    }
    let scope = body.scope;
    let request = body.request_id;
    let team = body.team_id;
    let actor = TeamAuthority::Participant {
        session: body.session_id,
    };
    let mut spec = TeamJoinSpec {
        request_id: request,
        workspace_id: scope.workspace_id,
        team_id: team,
        activity_id: body.activity_id,
        session_id: body.session_id,
        expected_team_revision: body.expected_team_revision,
        role: body.role,
        observed_at_ms: 0,
    };
    let record = state
        .run_scoped_team(
            scope.clone(),
            actor,
            principal,
            vec![body.activity_id],
            move |runtime, now| {
                spec.observed_at_ms = now;
                runtime.join_team(spec, principal)
            },
        )
        .await?;
    Ok(Json(response(scope, request, team, &record)))
}
async fn leave(
    State(state): State<BackendState>,
    Path((workspace, team_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<TeamLeaveRequest>,
) -> Result<Json<TeamMutationResponse>, LaneError> {
    validate(
        &workspace,
        Some(&team_path),
        &body.scope,
        body.team_id,
        body.request_id,
        Some(body.session_id),
    )?;
    if body.activity_id.is_nil()
        || body.expected_team_revision < 1
        || body.expected_team_revision > i64::MAX as u64
    {
        return Err(LaneError::InvalidIdentity);
    }
    let scope = body.scope;
    let request = body.request_id;
    let team = body.team_id;
    let actor = TeamAuthority::Participant {
        session: body.session_id,
    };
    let mut spec = TeamLeaveSpec {
        request_id: request,
        workspace_id: scope.workspace_id,
        team_id: team,
        activity_id: body.activity_id,
        session_id: body.session_id,
        expected_team_revision: body.expected_team_revision,
        observed_at_ms: 0,
    };
    let record = state
        .run_scoped_team(
            scope.clone(),
            actor,
            principal,
            vec![body.activity_id],
            move |runtime, now| {
                spec.observed_at_ms = now;
                runtime.leave_team(spec, principal)
            },
        )
        .await?;
    Ok(Json(response(scope, request, team, &record)))
}
async fn retire(
    State(state): State<BackendState>,
    Path((workspace, team_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<TeamRetireRequest>,
) -> Result<Json<TeamMutationResponse>, LaneError> {
    validate(
        &workspace,
        Some(&team_path),
        &body.scope,
        body.team_id,
        body.request_id,
        None,
    )?;
    if body.expected_team_revision < 1 || body.expected_team_revision > i64::MAX as u64 {
        return Err(LaneError::InvalidIdentity);
    }
    let scope = body.scope;
    let request = body.request_id;
    let team = body.team_id;
    let workspace_id = scope.workspace_id;
    let revision = body.expected_team_revision;
    let record = state
        .run_scoped_team(
            scope.clone(),
            TeamAuthority::Operator,
            principal,
            Vec::new(),
            move |runtime, now| {
                runtime.retire_team(request, workspace_id, team, revision, now, principal)
            },
        )
        .await?;
    Ok(Json(response(scope, request, team, &record)))
}
fn response(
    scope: WorkspaceQueryScope,
    request_id: Uuid,
    team_id: Uuid,
    record: &TeamMutationRecord,
) -> TeamMutationResponse {
    TeamMutationResponse {
        scope,
        request_id,
        team_id,
        mutation: super::scoped::team_mutation_wire(record),
    }
}
impl TeamAuthority {
    fn authorize(
        self,
        runtime: &OrchestrationRuntime,
        workspace: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<(), RuntimeError> {
        match self {
            Self::Operator => runtime.require_operator(principal),
            Self::Participant { session } => {
                runtime.require_open_session(session, principal)?;
                runtime.require_workspace_read(workspace, Some(session), principal)
            }
        }
    }
}
impl BackendState {
    async fn prove_team_source(
        &self,
        scope: &WorkspaceQueryScope,
        activities: &[Uuid],
    ) -> Result<(), ApiError> {
        if activities.is_empty() {
            self.workspace_scope_proof(scope.clone()).await?;
        }
        for activity in activities {
            self.activity_scope_proof(scope.clone(), *activity).await?;
        }
        Ok(())
    }
    async fn run_scoped_team<F>(
        &self,
        scope: WorkspaceQueryScope,
        actor: TeamAuthority,
        principal: AuthenticatedPrincipal,
        mut activities: Vec<Uuid>,
        operation: F,
    ) -> Result<TeamMutationRecord, ApiError>
    where
        F: FnOnce(&OrchestrationRuntime, i64) -> Result<TeamMutationRecord, RuntimeError>
            + Send
            + 'static,
    {
        activities.sort_unstable();
        activities.dedup();
        let state = self.clone();
        self.run_owned(async move {
            let workspace = scope.workspace_id;
            state
                .catalog_job(move |runtime| actor.authorize(runtime, workspace, principal))
                .await?;
            state.prove_team_source(&scope, &activities).await?;
            state
                .runtime
                .ensure_broker_owner()
                .await
                .map_err(RuntimeError::BrokerOwner)
                .map_err(ApiError::Runtime)?;
            state.prove_team_source(&scope, &activities).await?;
            let publisher = state.clone();
            state
                .catalog_job(move |runtime| {
                    actor.authorize(runtime, workspace, principal)?;
                    let now = crate::current_timestamp_millis()
                        .map_err(|_| RuntimeError::InvalidInput)?;
                    let record = operation(runtime, now)?;
                    if !record.replayed() {
                        publisher.publish_state_changed(
                            RealtimeEventKind::TeamChanged,
                            record.team().team_id(),
                            record.team().revision(),
                            Some(workspace),
                        );
                    }
                    Ok(record)
                })
                .await
        })
        .await
    }
}
