//! Source-fenced Team commands returning complete immutable owner receipts.
use crate::{
    ApiError, BackendState, RealtimeEventKind,
    app_support::{LaneError, validate_workspace_reference},
    wire::{
        WorkspaceQueryScope,
        team_commands::{
            TeamCreateRequest, TeamCreateV2Request, TeamJoinRequest, TeamJoinV2Request,
            TeamLeaveRequest, TeamMutationResponse, TeamMutationV2Response, TeamRetireRequest,
        },
    },
};
use axum::{
    Json, Router,
    extract::{Extension, Path, State},
    routing::post,
};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_operations::{
    TeamMutationRecord, TeamMutationSourceEvidence, team_expansion::TeamExpansionSlotRef,
};
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
            "/internal/v2/workspaces/{workspace_id}/teams/create",
            post(create_v2),
        )
        .route(
            "/internal/v2/workspaces/{workspace_id}/teams/{team_id}/join",
            post(join_v2),
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
    let scope = body.scope.clone();
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
        max_members: None,
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
        expansion_slot: None,
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

async fn create_v2(
    State(state): State<BackendState>,
    Path(workspace): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<TeamCreateV2Request>,
) -> Result<Json<TeamMutationV2Response>, LaneError> {
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
        || body.max_members > 8
        || usize::from(body.max_members) < body.initial_members.len()
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
            .map(|member| TeamRosterMemberSpec {
                activity_id: member.activity_id,
                session_id: member.session_id,
                role: member.role,
            })
            .collect(),
        max_members: Some(body.max_members),
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
    Ok(Json(response_v2(scope, request, team, &record, None)?))
}

async fn join_v2(
    State(state): State<BackendState>,
    Path((workspace, team_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<TeamJoinV2Request>,
) -> Result<Json<TeamMutationV2Response>, LaneError> {
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
        || body.expansion_slot.decision_id.is_nil()
        || body.expansion_slot.ordinal >= 8
    {
        return Err(LaneError::InvalidIdentity);
    }
    let scope = body.scope.clone();
    let request = body.request_id;
    let team = body.team_id;
    let reader_activity_id = body.activity_id;
    let reader_session_id = body.session_id;
    let slot =
        TeamExpansionSlotRef::new(body.expansion_slot.decision_id, body.expansion_slot.ordinal);
    let role = body.role.clone();
    let actor = TeamAuthority::Participant {
        session: reader_session_id,
    };
    let mut spec = TeamJoinSpec {
        request_id: request,
        workspace_id: scope.workspace_id,
        team_id: team,
        activity_id: reader_activity_id,
        session_id: reader_session_id,
        expected_team_revision: body.expected_team_revision,
        role: role.clone(),
        expansion_slot: Some(slot),
        observed_at_ms: 0,
    };
    let record = state
        .run_scoped_team(
            scope.clone(),
            actor,
            principal,
            vec![reader_activity_id],
            move |runtime, now| {
                spec.observed_at_ms = now;
                runtime.join_team(spec, principal)
            },
        )
        .await?;
    let source = join_source_after_commit(&state, &body, &record, principal).await?;
    Ok(Json(response_v2(
        scope,
        request,
        team,
        &record,
        Some(source),
    )?))
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

fn response_v2(
    scope: WorkspaceQueryScope,
    request_id: Uuid,
    team_id: Uuid,
    record: &TeamMutationRecord,
    expansion_join_source: Option<crate::wire::teams::TeamExpansionJoinSourceV2Wire>,
) -> Result<TeamMutationV2Response, LaneError> {
    let mutation = super::scoped::team_mutation_v2_wire(record, expansion_join_source);
    let is_team_create = record.membership().is_none() && !record.initial_members().is_empty();
    let missing_source = (is_team_create
        && (mutation.capacity.is_none() || mutation.initial_roster_digest_v1.is_none()))
        || (record.expansion_slot().is_some() && mutation.expansion_join_source.is_none());
    if missing_source {
        return Err(LaneError::SourceBindingUnconfirmed);
    }
    Ok(TeamMutationV2Response {
        scope,
        request_id,
        team_id,
        mutation,
    })
}

async fn join_source_after_commit(
    state: &BackendState,
    request: &TeamJoinV2Request,
    committed: &TeamMutationRecord,
    principal: AuthenticatedPrincipal,
) -> Result<crate::wire::teams::TeamExpansionJoinSourceV2Wire, LaneError> {
    let scope = request.scope.clone();
    let team_id = request.team_id;
    let request_id = request.request_id;
    let activity_id = request.activity_id;
    let session_id = request.session_id;
    let slot = TeamExpansionSlotRef::new(
        request.expansion_slot.decision_id,
        request.expansion_slot.ordinal,
    );
    let role = request.role.as_str();
    if state
        .activity_scope_proof(scope.clone(), activity_id)
        .await
        .is_err()
    {
        return Err(unconfirmed_join_source("activity_scope_proof"));
    }
    let workspace_id = scope.workspace_id;
    let readback = match state
        .run_workspace_reference_query(scope, Some(session_id), principal, move |runtime, _| {
            runtime.team_mutation_owner_readback(
                workspace_id,
                team_id,
                request_id,
                activity_id,
                session_id,
                principal,
            )
        })
        .await
    {
        Ok(Some(readback)) => readback,
        Ok(None) => return Err(unconfirmed_join_source("owner_readback_missing")),
        Err(error) => {
            tracing::warn!(
                error_class = owner_readback_error_class(&error),
                "v2 Team JOIN owner readback failed after commit"
            );
            return Err(unconfirmed_join_source("owner_readback_error"));
        }
    };
    let Some(TeamMutationSourceEvidence::SlotBackedJoin(source)) = readback.source() else {
        return Err(unconfirmed_join_source("slot_source_missing"));
    };
    let source_slot = source.slot();
    let mutation = readback.mutation();
    if !join_source_actor_matches_request(source, request) {
        return Err(unconfirmed_join_source("actor_mismatch"));
    }
    if source_slot != slot
        || committed.expansion_slot() != Some(slot)
        || mutation.expansion_slot() != committed.expansion_slot()
        || mutation.expansion_slot() != Some(slot)
    {
        return Err(unconfirmed_join_source("slot_mismatch"));
    }
    if source.role() != role
        || mutation.membership().is_none_or(|membership| {
            membership.state() != heterarchy_orchestration_operations::TeamMembershipState::Joined
                || membership.role() != Some(role)
        })
    {
        return Err(unconfirmed_join_source("role_or_membership_mismatch"));
    }
    if mutation.team().revision() != committed.team().revision()
        || mutation.membership() != committed.membership()
        || mutation.team().team_id() != team_id
    {
        return Err(unconfirmed_join_source("committed_record_mismatch"));
    }
    Ok(crate::wire::teams::TeamExpansionJoinSourceV2Wire {
        activity_id,
        agent_id: heterarchy_orchestration_runtime::leadership::member_agent_id(
            workspace_id,
            activity_id,
            session_id,
        )
        .into_uuid(),
        session_id,
        decision_id: source_slot.approval_id(),
        ordinal: source_slot.ordinal(),
        role: source.role().to_owned(),
    })
}

fn join_source_actor_matches_request(
    source: &heterarchy_orchestration_operations::team_expansion::TeamExpansionJoinSource,
    request: &TeamJoinV2Request,
) -> bool {
    let workspace_id = request.scope.workspace_id;
    match source.actor() {
        heterarchy_orchestration_operations::OperationalActor::Participant {
            activity_id,
            agent_id,
            session_id,
        } => {
            activity_id.into_uuid() == request.activity_id
                && session_id.into_uuid() == request.session_id
                && agent_id.into_uuid()
                    == heterarchy_orchestration_runtime::leadership::member_agent_id(
                        workspace_id,
                        request.activity_id,
                        request.session_id,
                    )
                    .into_uuid()
        }
        heterarchy_orchestration_operations::OperationalActor::LocalOperator => false,
    }
}

fn unconfirmed_join_source(stage: &'static str) -> LaneError {
    tracing::warn!(
        stage,
        "v2 Team JOIN post-commit source proof is unconfirmed"
    );
    LaneError::SourceBindingUnconfirmed
}

fn owner_readback_error_class(error: &ApiError) -> &'static str {
    match error {
        ApiError::Runtime(RuntimeError::Persistence(source)) => match source.as_ref() {
            heterarchy_orchestration_operations::OperationsError::Corrupt(reason)
            | heterarchy_orchestration_operations::OperationsError::Conflict(reason)
            | heterarchy_orchestration_operations::OperationsError::ResyncRequired(reason)
            | heterarchy_orchestration_operations::OperationsError::Bounds(reason) => reason,
            _ => "operations_error",
        },
        ApiError::Runtime(RuntimeError::Forbidden) => "runtime_forbidden",
        ApiError::Runtime(RuntimeError::Conflict) => "runtime_conflict",
        ApiError::WorkspaceScopeUnavailable => "workspace_scope_unavailable",
        ApiError::Unavailable => "backend_unavailable",
        ApiError::InvalidIdentity => "invalid_identity",
        _ => "backend_error",
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
