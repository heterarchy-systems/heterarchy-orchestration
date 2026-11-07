//! V2 approval adapter. Each approved Team ordinal names an existing
//! Workspace Profile whose Workspace occupancy is held in the same Operations
//! transaction as the approval.

use axum::extract::{DefaultBodyLimit, Extension, Path, State};
use axum::routing::post;
use axum::{Json, Router};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_runtime::{
    OrchestrationRuntime, RuntimeError,
    leadership::member_agent_id,
    participation::ParticipationTarget,
    team_expansion::{
        LegacyTeamExpansionSlotRecoveryIntentSpec, RecoverLegacyTeamExpansionSlotSpec,
        TeamExpansionActorTarget, TeamExpansionDecideSpec, TeamExpansionDecideV2Spec,
        TeamExpansionDecisionSpec, TeamExpansionDecisionV2SlotSpec,
        TeamExpansionDecisionV2TargetSpec,
    },
};

use crate::app_support::{LaneError, validate_workspace_reference};
use crate::wire::{
    WorkspaceQueryScope, legacy_slot_recovery as recovery_wire, team_expansion_v2 as wire,
};
use crate::{ApiError, BackendState};

pub(super) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v2/workspaces/{workspace_id}/teams/expansion/decide",
            post(decide_expansion_v2),
        )
        .route(
            "/internal/v2/workspaces/{workspace_id}/teams/expansion/readback",
            post(readback_expansion_v2),
        )
        .route(
            "/internal/v2/workspaces/{workspace_id}/teams/expansion/legacy-slot-recovery",
            post(recover_legacy_slot_v2),
        )
        .route(
            "/internal/v2/workspaces/{workspace_id}/teams/expansion/legacy-slot-recovery/readback",
            post(readback_legacy_slot_v2),
        )
        .layer(DefaultBodyLimit::max(
            crate::wire::team_expansion::MAX_TEAM_EXPANSION_BODY_BYTES,
        ))
}

fn actor_target(
    actor: &crate::wire::team_expansion::TeamExpansionActorWire,
) -> TeamExpansionActorTarget {
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
    actor: &crate::wire::team_expansion::TeamExpansionActorWire,
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

fn runtime_spec(request: &wire::TeamExpansionDecisionV2Request) -> TeamExpansionDecideV2Spec {
    let decision = match request.command.decision {
        crate::wire::team_expansion::TeamExpansionDecisionWire::Approve { approved_count } => {
            TeamExpansionDecisionSpec::Approve { approved_count }
        }
        crate::wire::team_expansion::TeamExpansionDecisionWire::Reject => {
            TeamExpansionDecisionSpec::Reject
        }
    };
    TeamExpansionDecideV2Spec {
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
            .map(|slot| TeamExpansionDecisionV2SlotSpec {
                ordinal: slot.ordinal,
                target: TeamExpansionDecisionV2TargetSpec {
                    profile_id: slot.target.profile_id,
                    participation_id: slot.target.participation_id,
                },
            })
            .collect(),
    }
}

async fn decide_expansion_v2(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::TeamExpansionDecisionV2Request>,
) -> Result<Json<wire::TeamExpansionDecisionV2Response>, LaneError> {
    validate_actor_path(&workspace_path, &request.actor)?;
    if !wire::valid_decision_v2_request(&request) {
        return Err(LaneError::InvalidIdentity);
    }
    let actor = actor_target(&request.actor);
    let scope = request.actor.scope.clone();
    let spec = runtime_spec(&request);
    let receipt = run_v2_mutation(&state, scope, actor, principal, move |runtime| {
        runtime.decide_team_expansion_v2(spec, principal)
    })
    .await?;
    let response = wire::TeamExpansionDecisionV2Response {
        request: request.clone(),
        receipt: wire::TeamExpansionDecisionV2ReceiptWire {
            decision: (&receipt).into(),
            slot_plan: request.slot_plan.clone(),
        },
    };
    if !wire::valid_decision_v2_response(&response, &request) {
        return Err(ApiError::SourceBindingUnconfirmed.into());
    }
    Ok(Json(response))
}

async fn readback_expansion_v2(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::TeamExpansionDecisionV2ReadbackRequest>,
) -> Result<Json<wire::TeamExpansionDecisionV2ReadbackResponse>, LaneError> {
    validate_actor_path(&workspace_path, &request.current_actor)?;
    validate_actor_path(&workspace_path, &request.intent.actor)?;
    if !wire::valid_decision_v2_readback_request(&request) {
        return Err(LaneError::InvalidIdentity);
    }
    let reader = actor_target(&request.current_actor);
    let scope = request.current_actor.scope.clone();
    let intent = runtime_spec(&request.intent);
    let result = run_v2_query(&state, scope, reader, principal, move |runtime| {
        runtime.team_expansion_decision_v2_readback(reader, &intent, principal)
    })
    .await?;
    let response = wire::TeamExpansionDecisionV2ReadbackResponse {
        request: request.clone(),
        result: result.map(|receipt| wire::TeamExpansionDecisionV2ReceiptWire {
            decision: (&receipt).into(),
            slot_plan: request.intent.slot_plan.clone(),
        }),
    };
    if !wire::valid_decision_v2_readback_response(&response, &request) {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(Json(response))
}

fn recovery_intent(
    intent: recovery_wire::LegacySlotRecoveryIntentWire,
) -> LegacyTeamExpansionSlotRecoveryIntentSpec {
    LegacyTeamExpansionSlotRecoveryIntentSpec {
        recovery_request_id: intent.recovery_request_id,
        approval_id: intent.approval_id,
        ordinal: intent.ordinal,
        workspace_id: intent.workspace_id,
        team_id: intent.team_id,
        expected_team_revision: intent.expected_team_revision,
        expected_capacity_revision: intent.expected_capacity_revision,
        target_profile_id: intent.target_profile_id,
        target_participation_id: intent.target_participation_id,
    }
}

fn recovery_receipt(
    receipt: &heterarchy_orchestration_operations::LegacySlotRecoveryReceipt,
) -> recovery_wire::LegacySlotRecoveryReceiptWire {
    let actor = receipt.actor();
    let intent = receipt.intent();
    recovery_wire::LegacySlotRecoveryReceiptWire {
        intent: recovery_wire::LegacySlotRecoveryIntentWire {
            workspace_id: intent.workspace_id().into_uuid(),
            recovery_request_id: intent.request_id(),
            approval_id: intent.slot().approval_id(),
            ordinal: intent.slot().ordinal(),
            team_id: intent.team_id(),
            expected_team_revision: intent.expected_team_revision(),
            expected_capacity_revision: intent.expected_capacity_revision(),
            target_profile_id: intent.target_profile_id(),
            target_participation_id: intent.target_participation_id().into_uuid(),
        },
        approval_request_id: receipt.approval_request_id(),
        leadership_revision: receipt.leadership_revision(),
        actor: crate::wire::team_expansion::TeamExpansionActorReceiptWire {
            activity_id: actor.activity_id().into_uuid(),
            agent_id: actor.agent_id().into_uuid(),
            session_id: actor.session_id().into_uuid(),
            participation_id: actor.participation_id().into_uuid(),
            profile_id: actor.profile_id(),
            participant_id: actor.participant_id(),
            display_name: actor.display_name().to_owned(),
        },
        recorded_at_ms: receipt.recorded_at().get(),
        replayed: receipt.replayed(),
    }
}

async fn recover_legacy_slot_v2(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<recovery_wire::LegacySlotRecoveryRequest>,
) -> Result<Json<recovery_wire::LegacySlotRecoveryResponse>, LaneError> {
    validate_actor_path(&workspace_path, &request.actor)?;
    if !recovery_wire::valid_legacy_slot_recovery_request(&request) {
        return Err(LaneError::InvalidIdentity);
    }
    let actor = actor_target(&request.actor);
    let scope = request.actor.scope.clone();
    let spec = RecoverLegacyTeamExpansionSlotSpec {
        actor,
        intent: recovery_intent(request.intent),
        expected_leadership_revision: request.expected_leadership_revision,
    };
    let receipt = run_v2_mutation(&state, scope, actor, principal, move |runtime| {
        runtime.recover_legacy_team_expansion_slot(spec, principal)
    })
    .await?;
    let response = recovery_wire::LegacySlotRecoveryResponse {
        request: request.clone(),
        receipt: recovery_receipt(&receipt),
    };
    if !recovery_wire::valid_legacy_slot_recovery_response(&response, &request) {
        return Err(ApiError::SourceBindingUnconfirmed.into());
    }
    Ok(Json(response))
}

async fn readback_legacy_slot_v2(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<recovery_wire::LegacySlotRecoveryReadbackRequest>,
) -> Result<Json<recovery_wire::LegacySlotRecoveryReadbackResponse>, LaneError> {
    validate_actor_path(&workspace_path, &request.current_actor)?;
    if !recovery_wire::valid_legacy_slot_recovery_readback_request(&request) {
        return Err(LaneError::InvalidIdentity);
    }
    let reader = actor_target(&request.current_actor);
    let scope = request.current_actor.scope.clone();
    let intent = recovery_intent(request.intent);
    let result = run_v2_query(&state, scope, reader, principal, move |runtime| {
        runtime.legacy_team_expansion_slot_recovery(reader, intent, principal)
    })
    .await?;
    let response = recovery_wire::LegacySlotRecoveryReadbackResponse {
        request: request.clone(),
        result: result.as_ref().map(recovery_receipt),
    };
    if !recovery_wire::valid_legacy_slot_recovery_readback_response(&response, &request) {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(Json(response))
}

async fn run_v2_mutation<T, F>(
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

async fn run_v2_query<T, F>(
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
