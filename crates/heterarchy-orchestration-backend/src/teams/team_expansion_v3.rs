//! V3 Team-expansion approval adapter with exact source Home evidence.
//!
//! A transfer approval reserves destination Workspace capacity and records
//! the source Team membership revision. It does not record consent or
//! acceptance, assign a slot, change membership, or perform work handoff.
//! This is not a complete/releasable Home-transfer workflow until those
//! separately authorized consent, acceptance, assignment, and cutover steps
//! are available and connected.

use axum::extract::{DefaultBodyLimit, Extension, Path, State};
use axum::routing::post;
use axum::{Json, Router};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_operations::{
    TeamExpansionActor, TeamExpansionDecisionV3Projection, TeamExpansionWorkspaceTargetV3,
};
use heterarchy_orchestration_runtime::{
    OrchestrationRuntime, RuntimeError,
    leadership::member_agent_id,
    participation::ParticipationTarget,
    team_expansion::{
        TeamExpansionActorTarget, TeamExpansionDecideSpec, TeamExpansionDecideV3Spec,
        TeamExpansionDecisionSpec, TeamExpansionDecisionV3SlotSpec,
        TeamExpansionDecisionV3TargetSpec,
    },
};

use crate::app_support::{LaneError, validate_workspace_reference};
use crate::wire::{WorkspaceQueryScope, team_expansion as base_wire, team_expansion_v3 as wire};
use crate::{ApiError, BackendState};

pub(super) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v3/workspaces/{workspace_id}/teams/expansion/decide",
            post(decide_expansion_v3),
        )
        .route(
            "/internal/v3/workspaces/{workspace_id}/teams/expansion/readback",
            post(readback_expansion_v3),
        )
        .route(
            "/internal/v3/workspaces/{workspace_id}/teams/expansion/projection",
            post(project_expansion_v3),
        )
        .layer(DefaultBodyLimit::max(
            base_wire::MAX_TEAM_EXPANSION_BODY_BYTES,
        ))
}

fn actor_target(actor: &base_wire::TeamExpansionActorWire) -> TeamExpansionActorTarget {
    TeamExpansionActorTarget {
        workspace_id: actor.scope.workspace_id,
        activity_id: actor.activity_id,
        session_id: actor.session_id,
    }
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

fn validate_actor_path(
    path_workspace_id: &str,
    actor: &base_wire::TeamExpansionActorWire,
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

fn runtime_spec(request: &wire::TeamExpansionDecisionV3Request) -> TeamExpansionDecideV3Spec {
    let decision = match request.command.decision {
        base_wire::TeamExpansionDecisionWire::Approve { approved_count } => {
            TeamExpansionDecisionSpec::Approve { approved_count }
        }
        base_wire::TeamExpansionDecisionWire::Reject => TeamExpansionDecisionSpec::Reject,
    };
    TeamExpansionDecideV3Spec {
        decision: TeamExpansionDecideSpec {
            actor: actor_target(&request.actor),
            team_id: request.command.team_id,
            request_id: request.command.request_id,
            decision_id: request.command.decision_id,
            expected_team_revision: request.command.expected_team_revision,
            expected_capacity_revision: request.command.expected_capacity_revision,
            expected_leadership_revision: request.command.expected_leadership_revision,
            expected_requester_leadership_revision: request
                .command
                .expected_requester_leadership_revision,
            decision,
        },
        slot_plan: request
            .slot_plan
            .iter()
            .map(|slot| TeamExpansionDecisionV3SlotSpec {
                ordinal: slot.ordinal,
                target: match slot.target {
                    wire::TeamExpansionApprovalTargetV3Wire::ExistingWorkspaceProfile {
                        profile_id,
                        participation_id,
                    } => TeamExpansionDecisionV3TargetSpec::ExistingWorkspaceProfile {
                        profile_id,
                        participation_id,
                    },
                    wire::TeamExpansionApprovalTargetV3Wire::ExistingHomeTransfer {
                        profile_id,
                        participation_id,
                        source_team_id,
                        source_membership_revision,
                    } => TeamExpansionDecisionV3TargetSpec::ExistingHomeTransfer {
                        profile_id,
                        participation_id,
                        source_team_id,
                        source_membership_revision,
                    },
                },
            })
            .collect(),
    }
}

async fn decide_expansion_v3(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::TeamExpansionDecisionV3Request>,
) -> Result<Json<wire::TeamExpansionDecisionV3Response>, LaneError> {
    validate_actor_path(&workspace_path, &request.actor)?;
    if !wire::valid_decision_v3_request(&request) {
        return Err(LaneError::InvalidIdentity);
    }
    let actor = actor_target(&request.actor);
    let scope = request.actor.scope.clone();
    let spec = runtime_spec(&request);
    let receipt = run_v3_mutation(&state, scope, actor, principal, move |runtime| {
        runtime.decide_team_expansion_v3(spec, principal)
    })
    .await?;
    let response = wire::TeamExpansionDecisionV3Response {
        request: request.clone(),
        receipt: wire::TeamExpansionDecisionV3ReceiptWire {
            decision: (&receipt).into(),
            slot_plan: request.slot_plan.clone(),
        },
    };
    if !wire::valid_decision_v3_response(&response, &request) {
        return Err(ApiError::SourceBindingUnconfirmed.into());
    }
    Ok(Json(response))
}

async fn readback_expansion_v3(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::TeamExpansionDecisionV3ReadbackRequest>,
) -> Result<Json<wire::TeamExpansionDecisionV3ReadbackResponse>, LaneError> {
    validate_actor_path(&workspace_path, &request.current_actor)?;
    validate_actor_path(&workspace_path, &request.intent.actor)?;
    if !wire::valid_decision_v3_readback_request(&request) {
        return Err(LaneError::InvalidIdentity);
    }
    let reader = actor_target(&request.current_actor);
    let scope = request.current_actor.scope.clone();
    let intent = runtime_spec(&request.intent);
    let result = run_v3_query(&state, scope, reader, principal, move |runtime| {
        runtime.team_expansion_decision_v3_readback(reader, &intent, principal)
    })
    .await?;
    let response = wire::TeamExpansionDecisionV3ReadbackResponse {
        request: request.clone(),
        result: result.map(|receipt| wire::TeamExpansionDecisionV3ReceiptWire {
            decision: (&receipt).into(),
            slot_plan: request.intent.slot_plan.clone(),
        }),
    };
    if !wire::valid_decision_v3_readback_response(&response, &request) {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(Json(response))
}

async fn project_expansion_v3(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::TeamExpansionDecisionV3ProjectionRequest>,
) -> Result<Json<wire::TeamExpansionDecisionV3ProjectionResponse>, LaneError> {
    validate_actor_path(&workspace_path, &request.current_actor)?;
    if crate::canonical_uuid(&workspace_path)? != request.workspace_id
        || !wire::valid_decision_v3_projection_request(&request)
    {
        return Err(LaneError::InvalidIdentity);
    }
    let reader = actor_target(&request.current_actor);
    let scope = request.current_actor.scope.clone();
    let workspace_id = request.workspace_id;
    let decision_id = request.decision_id;
    let result = run_v3_query(&state, scope, reader, principal, move |runtime| {
        runtime.team_expansion_decision_v3_projection(workspace_id, decision_id, reader, principal)
    })
    .await?
    .map(|projection| projection_wire(&projection))
    .transpose()?;
    let response = wire::TeamExpansionDecisionV3ProjectionResponse {
        request: request.clone(),
        result,
    };
    if !wire::valid_decision_v3_projection_response(&response, &request) {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(Json(response))
}

fn projection_wire(
    projection: &TeamExpansionDecisionV3Projection,
) -> Result<wire::TeamExpansionDecisionV3ProjectionWire, LaneError> {
    // Operations has already matched this reconstruction to the immutable
    // manifest and source rows; never replace it with current membership data.
    let decision: base_wire::TeamExpansionDecisionReceiptWire = projection.decision().into();
    let original_issuer = actor_receipt(projection.original_issuer());
    if decision.actor != original_issuer {
        return Err(ApiError::SourceBindingUnconfirmed.into());
    }
    let slot_plan = projection
        .slot_plan()
        .iter()
        .map(|slot| {
            let target = match slot.target() {
                TeamExpansionWorkspaceTargetV3::ExistingWorkspaceProfile {
                    profile_id,
                    participation_id,
                } => wire::TeamExpansionApprovalTargetV3Wire::ExistingWorkspaceProfile {
                    profile_id,
                    participation_id: participation_id.into_uuid(),
                },
                TeamExpansionWorkspaceTargetV3::ExistingHomeTransfer {
                    profile_id,
                    participation_id,
                    source_team_id,
                    source_membership_revision,
                } => wire::TeamExpansionApprovalTargetV3Wire::ExistingHomeTransfer {
                    profile_id,
                    participation_id: participation_id.into_uuid(),
                    source_team_id,
                    source_membership_revision,
                },
                TeamExpansionWorkspaceTargetV3::NewProfile => {
                    return Err(ApiError::SourceBindingUnconfirmed.into());
                }
            };
            Ok(wire::TeamExpansionApprovalSlotV3Wire {
                ordinal: slot.ordinal(),
                target,
            })
        })
        .collect::<Result<Vec<_>, LaneError>>()?;
    Ok(wire::TeamExpansionDecisionV3ProjectionWire {
        decision,
        original_issuer,
        slot_plan,
    })
}

fn actor_receipt(actor: &TeamExpansionActor) -> base_wire::TeamExpansionActorReceiptWire {
    base_wire::TeamExpansionActorReceiptWire {
        activity_id: actor.activity_id().into_uuid(),
        agent_id: actor.agent_id().into_uuid(),
        session_id: actor.session_id().into_uuid(),
        participation_id: actor.participation_id().into_uuid(),
        profile_id: actor.profile_id(),
        participant_id: actor.participant_id(),
        display_name: actor.display_name().to_owned(),
    }
}

async fn run_v3_mutation<T, F>(
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
                .runtime
                .ensure_broker_owner()
                .await
                .map_err(RuntimeError::BrokerOwner)
                .map_err(ApiError::Runtime)?;
            owned_state
                .activity_scope_proof(scope, actor.activity_id)
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

async fn run_v3_query<T, F>(
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
                .activity_scope_proof(scope, actor.activity_id)
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
