//! HTTP adapters for Leader-change transfer renewal and generation readback.

use axum::extract::{Extension, Path, State};
use axum::routing::post;
use axum::{Json, Router};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_operations::{
    TeamPermanentTransferGenerationSnapshot, TeamPermanentTransferGenerationState,
};
use heterarchy_orchestration_runtime::team_permanent_transfer::TeamPermanentTransferRenewalSpec;

use crate::app_support::LaneError;
use crate::wire::team_permanent_transfer as wire;
use crate::{ApiError, BackendState};

use super::{actor_target, run_transfer_mutation, run_transfer_query, validate_actor_path};

pub(super) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/permanent-transfers/renew",
            post(renew_transfer),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/permanent-transfers/generation/readback",
            post(readback_generation),
        )
}

async fn renew_transfer(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::TeamPermanentTransferRenewalRequest>,
) -> Result<Json<wire::TeamPermanentTransferRenewalResponse>, LaneError> {
    validate_actor_path(&workspace_path, &request.proposal.actor)?;
    if !wire::valid_renewal_request(&request) {
        return Err(LaneError::InvalidIdentity);
    }

    let actor = actor_target(&request.proposal.actor);
    let scope = request.proposal.actor.scope.clone();
    let spec = TeamPermanentTransferRenewalSpec {
        proposal: super::intent_spec(&request.proposal),
        predecessor_transfer_id: request.predecessor_transfer_id,
        expected_generation: request.expected_generation,
    };
    let receipt = run_transfer_mutation(&state, scope, actor, principal, move |runtime| {
        runtime.renew_team_permanent_transfer(spec, principal)
    })
    .await?;
    let response = wire::TeamPermanentTransferRenewalResponse {
        request: request.clone(),
        receipt: generation_snapshot_wire(&receipt),
    };
    if !wire::valid_renewal_response(&response, &request) {
        return Err(ApiError::SourceBindingUnconfirmed.into());
    }
    Ok(Json(response))
}

async fn readback_generation(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::TeamPermanentTransferReadbackRequest>,
) -> Result<Json<wire::TeamPermanentTransferGenerationReadbackResponse>, LaneError> {
    validate_actor_path(&workspace_path, &request.current_actor)?;
    if crate::canonical_uuid(&workspace_path)? != request.workspace_id
        || !wire::valid_generation_readback_request(&request)
    {
        return Err(LaneError::InvalidIdentity);
    }
    let reader = actor_target(&request.current_actor);
    let scope = request.current_actor.scope.clone();
    let transfer_id = request.transfer_id;
    let result = run_transfer_query(&state, scope, reader, principal, move |runtime| {
        runtime.read_team_permanent_transfer_generation(reader, transfer_id, principal)
    })
    .await?
    .map(|snapshot| generation_snapshot_wire(&snapshot));
    let response = wire::TeamPermanentTransferGenerationReadbackResponse {
        request: request.clone(),
        result,
    };
    if !wire::valid_generation_readback_response(&response, &request) {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(Json(response))
}

fn generation_snapshot_wire(
    snapshot: &TeamPermanentTransferGenerationSnapshot,
) -> wire::TeamPermanentTransferGenerationSnapshotWire {
    let generation = snapshot.generation();
    wire::TeamPermanentTransferGenerationSnapshotWire {
        intent: super::intent_receipt_wire(snapshot.intent()),
        generation: wire::TeamPermanentTransferGenerationWire {
            number: generation.number(),
            predecessor_transfer_id: generation.predecessor_transfer_id(),
            state: generation_state_wire(generation.state()),
            recorded_at_ms: generation.recorded_at().get(),
            head_transfer_id: generation.head_transfer_id(),
            head_generation: generation.head_generation(),
        },
    }
}

fn generation_state_wire(
    state: TeamPermanentTransferGenerationState,
) -> wire::TeamPermanentTransferGenerationStateWire {
    match state {
        TeamPermanentTransferGenerationState::IntentOnly => {
            wire::TeamPermanentTransferGenerationStateWire::IntentOnly
        }
        TeamPermanentTransferGenerationState::Preparing => {
            wire::TeamPermanentTransferGenerationStateWire::Preparing
        }
        TeamPermanentTransferGenerationState::Superseded => {
            wire::TeamPermanentTransferGenerationStateWire::Superseded
        }
    }
}
