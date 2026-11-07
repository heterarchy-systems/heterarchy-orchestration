//! Source-scoped HTTP adapter for Team capacity and expansion decisions.
//!
//! This adapter records the Orchestrator's explicit cap, the Team Leader's
//! request, the Orchestrator's decision, and a target-bound slot assignment.
//! It does not provision Profiles or create Team membership.

use axum::extract::{DefaultBodyLimit, Extension, Path, State};
use axum::routing::post;
use axum::{Json, Router};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_operations::TeamExpansionSlotRef;
use heterarchy_orchestration_runtime::{
    OrchestrationRuntime, RuntimeError,
    leadership::member_agent_id,
    participation::ParticipationTarget,
    team_expansion::{
        TeamCapacitySetSpec, TeamExpansionActorTarget, TeamExpansionDecideSpec,
        TeamExpansionDecisionSpec, TeamExpansionFinalizeAnchorSpec, TeamExpansionRequestSpec,
        TeamExpansionSlotAssignSpec, TeamExpansionTarget,
    },
};
use uuid::Uuid;

use crate::app_support::{LaneError, validate_workspace_reference};
use crate::wire::{WorkspaceQueryScope, team_expansion as wire};
use crate::{ApiError, BackendState};

pub(super) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/capacity/query",
            post(read_capacity),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/capacity/set",
            post(set_capacity),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/expansion",
            post(mutate_expansion),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/expansion/readback",
            post(readback_expansion),
        )
        .layer(DefaultBodyLimit::max(wire::MAX_TEAM_EXPANSION_BODY_BYTES))
}

fn actor_target(actor: &wire::TeamExpansionActorWire) -> TeamExpansionActorTarget {
    TeamExpansionActorTarget {
        workspace_id: actor.scope.workspace_id,
        activity_id: actor.activity_id,
        session_id: actor.session_id,
    }
}

fn validate_actor_path(
    path_workspace_id: &str,
    actor: &wire::TeamExpansionActorWire,
) -> Result<(), LaneError> {
    validate_workspace_reference(&actor.scope, Some(actor.session_id))?;
    if crate::canonical_uuid(path_workspace_id)? != actor.scope.workspace_id
        || actor.activity_id.is_nil()
        || actor.session_id.is_nil()
    {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(())
}

fn validate_readback_path(
    path_workspace_id: &str,
    request: &wire::TeamExpansionReadbackRequest,
) -> Result<(), LaneError> {
    validate_actor_path(path_workspace_id, &request.current_actor)?;
    if !wire::valid_readback_request(request) {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(())
}

fn validate_action_path(
    path_workspace_id: &str,
    request: &wire::TeamExpansionMutationRequest,
) -> Result<(), LaneError> {
    validate_actor_path(path_workspace_id, &request.actor)?;
    if !wire::valid_mutation_request(request) {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(())
}

async fn read_capacity(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::TeamCapacityQueryRequest>,
) -> Result<Json<wire::TeamCapacityQueryResponse>, LaneError> {
    validate_actor_path(&workspace_path, &request.actor)?;
    if !wire::capacity_query_request_is_valid(&request) {
        return Err(LaneError::InvalidIdentity);
    }
    let actor = actor_target(&request.actor);
    let team_id = request.team_id;
    let snapshot = run_expansion_query(
        &state,
        request.actor.scope.clone(),
        actor,
        principal,
        move |runtime| runtime.team_capacity(actor, team_id, principal),
    )
    .await?;
    let response = wire::TeamCapacityQueryResponse {
        actor: request.actor.clone(),
        team_id: request.team_id,
        snapshot: snapshot.as_ref().map(wire::TeamCapacitySnapshotWire::from),
    };
    if !wire::capacity_query_response_is_valid(&response, &request) {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(Json(response))
}

async fn set_capacity(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::TeamExpansionMutationRequest>,
) -> Result<Json<wire::TeamExpansionMutationResponse>, LaneError> {
    validate_action_path(&workspace_path, &request)?;
    let wire::TeamExpansionMutationActionWire::SetCapacity { command } = request.action.clone()
    else {
        return Err(ApiError::InvalidIdentity.into());
    };
    let scope = request.actor.scope.clone();
    let actor = actor_target(&request.actor);
    let team_id = command.team_id;
    let spec = TeamCapacitySetSpec {
        actor,
        team_id,
        request_id: command.request_id,
        expected_team_revision: command.expected_team_revision,
        expected_capacity_revision: command.expected_capacity_revision,
        expected_leadership_revision: command.expected_leadership_revision,
        max_members: command.max_members,
    };
    let original = request.clone();
    let receipt = run_expansion_mutation(
        &state,
        scope,
        actor,
        Vec::new(),
        principal,
        move |runtime| runtime.set_team_capacity(spec, principal),
    )
    .await?;
    let response = wire::TeamExpansionMutationResponse {
        request: original,
        result: wire::TeamExpansionMutationResultWire::CapacitySet {
            receipt: Box::new(wire::TeamCapacityReceiptWire::from(&receipt)),
        },
    };
    if !wire::valid_mutation_response(&response, &response.request) {
        return Err(ApiError::InvalidIdentity.into());
    }
    Ok(Json(response))
}

async fn mutate_expansion(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::TeamExpansionMutationRequest>,
) -> Result<Json<wire::TeamExpansionMutationResponse>, LaneError> {
    validate_action_path(&workspace_path, &request)?;
    let scope = request.actor.scope.clone();
    let actor = actor_target(&request.actor);
    let original = request.clone();
    let result = match request.action {
        wire::TeamExpansionMutationActionWire::Request { command } => {
            request_expansion(&state, scope, actor, principal, command).await?
        }
        wire::TeamExpansionMutationActionWire::Decide { command } => {
            decide_expansion(&state, scope, actor, principal, command).await?
        }
        wire::TeamExpansionMutationActionWire::FinalizeAnchor { command } => {
            finalize_expansion_anchor(&state, scope, actor, principal, command).await?
        }
        wire::TeamExpansionMutationActionWire::AssignSlot { command } => {
            assign_expansion_slot(&state, scope, actor, principal, command).await?
        }
        wire::TeamExpansionMutationActionWire::SetCapacity { .. } => {
            return Err(ApiError::InvalidIdentity.into());
        }
    };
    let response = wire::TeamExpansionMutationResponse {
        request: original,
        result,
    };
    if !wire::valid_mutation_response(&response, &response.request) {
        return Err(ApiError::InvalidIdentity.into());
    }
    Ok(Json(response))
}

async fn request_expansion(
    state: &BackendState,
    scope: WorkspaceQueryScope,
    actor: TeamExpansionActorTarget,
    principal: AuthenticatedPrincipal,
    command: wire::TeamExpansionRequestAction,
) -> Result<wire::TeamExpansionMutationResultWire, ApiError> {
    let spec = TeamExpansionRequestSpec {
        actor,
        team_id: command.team_id,
        request_id: command.request_id,
        expected_team_revision: command.expected_team_revision,
        expected_capacity_revision: command.expected_capacity_revision,
        expected_leadership_revision: command.expected_leadership_revision,
        requested_count: command.requested_count,
        reason: command.reason,
    };
    let receipt =
        run_expansion_mutation(state, scope, actor, Vec::new(), principal, move |runtime| {
            runtime.request_team_expansion(spec, principal)
        })
        .await?;
    Ok(wire::TeamExpansionMutationResultWire::Request {
        receipt: Box::new((&receipt).into()),
    })
}

async fn decide_expansion(
    state: &BackendState,
    scope: WorkspaceQueryScope,
    actor: TeamExpansionActorTarget,
    principal: AuthenticatedPrincipal,
    command: wire::TeamExpansionDecisionAction,
) -> Result<wire::TeamExpansionMutationResultWire, ApiError> {
    let decision = match command.decision {
        wire::TeamExpansionDecisionWire::Approve { approved_count } => {
            TeamExpansionDecisionSpec::Approve { approved_count }
        }
        wire::TeamExpansionDecisionWire::Reject => TeamExpansionDecisionSpec::Reject,
    };
    let spec = TeamExpansionDecideSpec {
        actor,
        team_id: command.team_id,
        request_id: command.request_id,
        decision_id: command.decision_id,
        expected_team_revision: command.expected_team_revision,
        expected_capacity_revision: command.expected_capacity_revision,
        expected_leadership_revision: command.expected_leadership_revision,
        expected_requester_leadership_revision: command.expected_requester_leadership_revision,
        decision,
    };
    let receipt =
        run_expansion_mutation(state, scope, actor, Vec::new(), principal, move |runtime| {
            runtime.decide_team_expansion(spec, principal)
        })
        .await?;
    Ok(wire::TeamExpansionMutationResultWire::Decision {
        receipt: Box::new((&receipt).into()),
    })
}

async fn finalize_expansion_anchor(
    state: &BackendState,
    scope: WorkspaceQueryScope,
    actor: TeamExpansionActorTarget,
    principal: AuthenticatedPrincipal,
    command: wire::TeamExpansionFinalizeAnchorAction,
) -> Result<wire::TeamExpansionMutationResultWire, ApiError> {
    let spec = TeamExpansionFinalizeAnchorSpec {
        actor,
        request_id: command.request_id,
        decision_id: command.decision_id,
        expected_leadership_revision: command.expected_leadership_revision,
    };
    let receipt =
        run_expansion_mutation(state, scope, actor, Vec::new(), principal, move |runtime| {
            runtime.finalize_team_expansion_anchor(spec, principal)
        })
        .await?;
    Ok(wire::TeamExpansionMutationResultWire::Decision {
        receipt: Box::new((&receipt).into()),
    })
}

async fn assign_expansion_slot(
    state: &BackendState,
    scope: WorkspaceQueryScope,
    actor: TeamExpansionActorTarget,
    principal: AuthenticatedPrincipal,
    command: wire::TeamExpansionSlotAssignmentAction,
) -> Result<wire::TeamExpansionMutationResultWire, ApiError> {
    let target_activity = command.target.activity_id;
    let spec = TeamExpansionSlotAssignSpec {
        actor,
        slot: TeamExpansionSlotRef::new(command.approval_id, command.ordinal),
        request_id: command.request_id,
        team_id: command.team_id,
        expected_team_revision: command.expected_team_revision,
        expected_capacity_revision: command.expected_capacity_revision,
        expected_leadership_revision: command.expected_leadership_revision,
        target: TeamExpansionTarget {
            activity_id: target_activity,
            session_id: command.target.session_id,
        },
    };
    let receipt = run_expansion_mutation(
        state,
        scope,
        actor,
        vec![target_activity],
        principal,
        move |runtime| runtime.assign_team_expansion_slot(spec, principal),
    )
    .await?;
    Ok(wire::TeamExpansionMutationResultWire::SlotAssignment {
        receipt: Box::new((&receipt).into()),
    })
}

async fn readback_expansion(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::TeamExpansionReadbackRequest>,
) -> Result<Json<wire::TeamExpansionReadbackResponse>, LaneError> {
    validate_readback_path(&workspace_path, &request)?;
    let scope = request.current_actor.scope.clone();
    let actor = actor_target(&request.current_actor);
    let intent = request.intent.clone();
    let response = run_expansion_query(
        &state,
        scope,
        actor,
        principal,
        move |runtime| match intent.action {
            wire::TeamExpansionMutationActionWire::SetCapacity { command } => runtime
                .team_capacity_set_receipt(actor, command.request_id, principal)
                .map(|receipt| {
                    receipt.map(
                        |receipt| wire::TeamExpansionMutationResultWire::CapacitySet {
                            receipt: Box::new((&receipt).into()),
                        },
                    )
                }),
            wire::TeamExpansionMutationActionWire::Request { command } => runtime
                .team_expansion_request(actor, command.request_id, principal)
                .map(|receipt| {
                    receipt.map(|receipt| wire::TeamExpansionMutationResultWire::Request {
                        receipt: Box::new((&receipt).into()),
                    })
                }),
            wire::TeamExpansionMutationActionWire::Decide { command } => runtime
                .team_expansion_decision(actor, command.decision_id, principal)
                .map(|receipt| {
                    receipt.map(|receipt| wire::TeamExpansionMutationResultWire::Decision {
                        receipt: Box::new((&receipt).into()),
                    })
                }),
            wire::TeamExpansionMutationActionWire::FinalizeAnchor { command } => runtime
                .team_expansion_decision(actor, command.decision_id, principal)
                .map(|receipt| {
                    receipt.map(|receipt| wire::TeamExpansionMutationResultWire::Decision {
                        receipt: Box::new((&receipt).into()),
                    })
                }),
            wire::TeamExpansionMutationActionWire::AssignSlot { command } => runtime
                .team_expansion_slot_assignment(
                    actor,
                    TeamExpansionSlotRef::new(command.approval_id, command.ordinal),
                    principal,
                )
                .map(|receipt| {
                    receipt.map(
                        |receipt| wire::TeamExpansionMutationResultWire::SlotAssignment {
                            receipt: Box::new((&receipt).into()),
                        },
                    )
                }),
        },
    )
    .await?;
    let response = wire::TeamExpansionReadbackResponse {
        request,
        result: response,
    };
    if !wire::valid_readback_response(&response, &response.request) {
        return Err(ApiError::InvalidIdentity.into());
    }
    Ok(Json(response))
}

async fn run_expansion_mutation<T, F>(
    state: &BackendState,
    scope: WorkspaceQueryScope,
    actor: TeamExpansionActorTarget,
    target_activities: Vec<Uuid>,
    principal: AuthenticatedPrincipal,
    operation: F,
) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce(&OrchestrationRuntime) -> Result<T, RuntimeError> + Send + 'static,
{
    let owned_state = state.clone();
    let mut activities = target_activities;
    activities.push(actor.activity_id);
    activities.sort_unstable();
    activities.dedup();
    state
        .run_owned(async move {
            owned_state
                .catalog_job(move |runtime| {
                    runtime.require_participation(participation_target(actor), principal)
                })
                .await?;
            // The participant selector already authenticates each Session below;
            // prove every relevant Forge Activity before and after acquiring Broker ownership.
            for activity_id in &activities {
                owned_state
                    .activity_scope_proof(scope.clone(), *activity_id)
                    .await?;
            }
            owned_state
                .runtime
                .ensure_broker_owner()
                .await
                .map_err(RuntimeError::BrokerOwner)
                .map_err(ApiError::Runtime)?;
            for activity_id in &activities {
                owned_state
                    .activity_scope_proof(scope.clone(), *activity_id)
                    .await?;
            }
            owned_state
                .catalog_job(move |runtime| {
                    runtime.require_participation(participation_target(actor), principal)?;
                    // The command API re-derives the actor and checks the exact role
                    // revision inside its Operations write transaction.
                    operation(runtime)
                })
                .await
        })
        .await
}

async fn run_expansion_query<T, F>(
    state: &BackendState,
    scope: WorkspaceQueryScope,
    actor: TeamExpansionActorTarget,
    principal: AuthenticatedPrincipal,
    operation: F,
) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce(&OrchestrationRuntime) -> Result<T, RuntimeError> + Send + 'static,
{
    let owned_state = state.clone();
    state
        .run_owned(async move {
            owned_state
                .catalog_job(move |runtime| {
                    runtime.require_participation(participation_target(actor), principal)
                })
                .await?;
            owned_state
                .activity_scope_proof(scope.clone(), actor.activity_id)
                .await?;
            owned_state
                .catalog_job(move |runtime| {
                    runtime.require_participation(participation_target(actor), principal)?;
                    operation(runtime)
                })
                .await
        })
        .await
}

fn participation_target(actor: TeamExpansionActorTarget) -> ParticipationTarget {
    ParticipationTarget {
        workspace_id: actor.workspace_id,
        activity_id: actor.activity_id,
        agent_id: member_agent_id(actor.workspace_id, actor.activity_id, actor.session_id)
            .into_uuid(),
        session_id: actor.session_id,
    }
}
