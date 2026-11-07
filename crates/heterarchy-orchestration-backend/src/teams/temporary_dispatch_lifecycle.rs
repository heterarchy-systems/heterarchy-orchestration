//! Source-fenced HTTP adapter for temporary-dispatch consent and reservation.
//!
//! These endpoints call Operations lifecycle owners only. They do not issue
//! source work, activate a dispatch, transfer a Broker claim, or free a seat.

use axum::extract::{Path, State};
use axum::routing::post;
use axum::{Extension, Json, Router, extract::DefaultBodyLimit};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_operations::temporary_dispatch::{
    TemporaryDispatchActorProvenance, TemporaryDispatchConsentKind,
    TemporaryDispatchConsentReceipt, TemporaryDispatchConsentWithdrawalReceipt,
    TemporaryDispatchReservationReceipt, TemporaryDispatchReservationState,
};
use heterarchy_orchestration_runtime::temporary_dispatch_lifecycle::{
    TemporaryDispatchConsentReadSpec, TemporaryDispatchConsentSpec,
    TemporaryDispatchConsentWithdrawalReadSpec, TemporaryDispatchConsentWithdrawalSpec,
    TemporaryDispatchLifecycleActor, TemporaryDispatchReservationSpec,
};
use uuid::Uuid;

use super::temporary_dispatch::run_scoped_temporary_dispatch;
use crate::{ApiError, BackendState, wire::temporary_dispatch_lifecycle as wire};

pub(super) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/temporary-dispatch/lifecycle",
            post(mutate),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/temporary-dispatch/lifecycle/readback",
            post(readback),
        )
        .layer(DefaultBodyLimit::max(
            wire::MAX_TEMPORARY_DISPATCH_LIFECYCLE_BODY_BYTES,
        ))
}

async fn mutate(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::TemporaryDispatchLifecycleMutationRequest>,
) -> Result<Json<wire::TemporaryDispatchLifecycleMutationResponse>, ApiError> {
    validate_mutation_path(&workspace_path, &request)?;
    let original = request.clone();
    let actor = request.actor.clone();
    let request_id = request.request_id;
    let action = request.action;
    let result = run_scoped_temporary_dispatch(
        &state,
        actor.scope.clone(),
        actor.activity_id,
        actor.session_id,
        principal,
        move |runtime| {
            let actor = lifecycle_actor(&actor, request_id);
            match action {
                wire::TemporaryDispatchLifecycleActionWire::Consent {
                    receipt_id,
                    kind,
                    expected_revision,
                } => runtime
                    .record_temporary_dispatch_consent(
                        TemporaryDispatchConsentSpec {
                            actor,
                            receipt_id,
                            kind: consent_kind(kind),
                            expected_revision,
                        },
                        principal,
                    )
                    .map(
                        |receipt| wire::TemporaryDispatchLifecycleResultWire::Consent {
                            receipt: Box::new(consent_receipt(&receipt)),
                        },
                    ),
                wire::TemporaryDispatchLifecycleActionWire::WithdrawConsent {
                    receipt_id,
                    consent_receipt_id,
                    kind,
                    expected_revision,
                } => runtime
                    .withdraw_temporary_dispatch_consent(
                        TemporaryDispatchConsentWithdrawalSpec {
                            actor,
                            receipt_id,
                            consent_receipt_id,
                            kind: consent_kind(kind),
                            expected_revision,
                        },
                        principal,
                    )
                    .map(
                        |receipt| wire::TemporaryDispatchLifecycleResultWire::WithdrawConsent {
                            receipt: Box::new(withdrawal_receipt(&receipt)),
                        },
                    ),
                wire::TemporaryDispatchLifecycleActionWire::Reserve { expected_revision } => {
                    runtime
                        .reserve_temporary_dispatch(
                            TemporaryDispatchReservationSpec {
                                actor,
                                expected_revision,
                            },
                            principal,
                        )
                        .map(
                            |receipt| wire::TemporaryDispatchLifecycleResultWire::Reservation {
                                receipt: Box::new(reservation_receipt(&receipt)),
                            },
                        )
                }
            }
        },
    )
    .await?;
    let response = wire::TemporaryDispatchLifecycleMutationResponse {
        request: original.clone(),
        result,
    };
    if !wire::valid_mutation_response(&response, &original) {
        return Err(ApiError::Unavailable);
    }
    Ok(Json(response))
}

async fn readback(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::TemporaryDispatchLifecycleReadbackRequest>,
) -> Result<Json<wire::TemporaryDispatchLifecycleReadbackResponse>, ApiError> {
    validate_readback_path(&workspace_path, &request)?;
    let original = request.clone();
    let intent = request.intent.clone();
    let current_actor = request.current_actor.clone();
    let request_id = intent.request_id;
    let action = intent.action;
    let result = run_scoped_temporary_dispatch_read(
        &state,
        current_actor.scope.clone(),
        current_actor.activity_id,
        current_actor.session_id,
        principal,
        move |runtime| {
            let actor = lifecycle_actor(&current_actor, request_id);
            match action {
                wire::TemporaryDispatchLifecycleActionWire::Consent {
                    receipt_id, kind, ..
                } => runtime
                    .read_temporary_dispatch_consent(
                        TemporaryDispatchConsentReadSpec {
                            actor,
                            receipt_id,
                            kind: consent_kind(kind),
                        },
                        principal,
                    )
                    .map(|receipt| {
                        receipt.map(
                            |receipt| wire::TemporaryDispatchLifecycleResultWire::Consent {
                                receipt: Box::new(consent_receipt(&receipt)),
                            },
                        )
                    }),
                wire::TemporaryDispatchLifecycleActionWire::WithdrawConsent {
                    receipt_id,
                    consent_receipt_id,
                    kind,
                    ..
                } => runtime
                    .read_temporary_dispatch_consent_withdrawal(
                        TemporaryDispatchConsentWithdrawalReadSpec {
                            actor,
                            receipt_id,
                            consent_receipt_id,
                            kind: consent_kind(kind),
                        },
                        principal,
                    )
                    .map(|receipt| {
                        receipt.map(|receipt| {
                            wire::TemporaryDispatchLifecycleResultWire::WithdrawConsent {
                                receipt: Box::new(withdrawal_receipt(&receipt)),
                            }
                        })
                    }),
                wire::TemporaryDispatchLifecycleActionWire::Reserve { .. } => runtime
                    .read_temporary_dispatch_reservation(actor, principal)
                    .map(|receipt| {
                        receipt.map(|receipt| {
                            wire::TemporaryDispatchLifecycleResultWire::Reservation {
                                receipt: Box::new(reservation_receipt(&receipt)),
                            }
                        })
                    }),
            }
        },
    )
    .await?;
    let response = wire::TemporaryDispatchLifecycleReadbackResponse {
        request: original.clone(),
        result,
    };
    if !wire::valid_readback_response(&response, &original) {
        return Err(ApiError::Unavailable);
    }
    Ok(Json(response))
}

/// Read-only lifecycle recovery keeps Session and Forge scope checks but does
/// not require the Broker mutation owner to be available after commit.
async fn run_scoped_temporary_dispatch_read<T, F>(
    state: &BackendState,
    scope: crate::wire::WorkspaceQueryScope,
    activity_id: Uuid,
    session_id: Uuid,
    principal: AuthenticatedPrincipal,
    operation: F,
) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce(
            &heterarchy_orchestration_runtime::OrchestrationRuntime,
        ) -> Result<T, heterarchy_orchestration_runtime::RuntimeError>
        + Send
        + 'static,
{
    let workspace_id = scope.workspace_id;
    let owned_state = state.clone();
    owned_state
        .clone()
        .run_owned(async move {
            owned_state
                .catalog_job(move |runtime| {
                    runtime.require_open_session(session_id, principal)?;
                    runtime.require_workspace_read(workspace_id, Some(session_id), principal)
                })
                .await?;
            owned_state.activity_scope_proof(scope, activity_id).await?;
            owned_state
                .catalog_job(move |runtime| {
                    runtime.require_open_session(session_id, principal)?;
                    runtime.require_workspace_read(workspace_id, Some(session_id), principal)?;
                    operation(runtime)
                })
                .await
        })
        .await
}

fn validate_mutation_path(
    workspace_path: &str,
    request: &wire::TemporaryDispatchLifecycleMutationRequest,
) -> Result<(), ApiError> {
    let workspace = crate::canonical_uuid(workspace_path)?;
    if !wire::valid_mutation_request(request) || request.actor.scope.workspace_id != workspace {
        return Err(ApiError::InvalidIdentity);
    }
    crate::app_support::validate_workspace_reference(
        &request.actor.scope,
        Some(request.actor.session_id),
    )?;
    Ok(())
}

fn validate_readback_path(
    workspace_path: &str,
    request: &wire::TemporaryDispatchLifecycleReadbackRequest,
) -> Result<(), ApiError> {
    let workspace = crate::canonical_uuid(workspace_path)?;
    if !wire::valid_readback_request(request)
        || request.intent.actor.scope.workspace_id != workspace
        || request.current_actor.scope.workspace_id != workspace
    {
        return Err(ApiError::InvalidIdentity);
    }
    crate::app_support::validate_workspace_reference(
        &request.current_actor.scope,
        Some(request.current_actor.session_id),
    )?;
    Ok(())
}

fn lifecycle_actor(
    actor: &wire::TemporaryDispatchLifecycleActorWire,
    request_id: Uuid,
) -> TemporaryDispatchLifecycleActor {
    TemporaryDispatchLifecycleActor {
        workspace_id: actor.scope.workspace_id,
        request_id,
        activity_id: actor.activity_id,
        session_id: actor.session_id,
    }
}

fn consent_kind(kind: wire::TemporaryDispatchConsentKindWire) -> TemporaryDispatchConsentKind {
    match kind {
        wire::TemporaryDispatchConsentKindWire::TargetAcceptance => {
            TemporaryDispatchConsentKind::TargetAcceptance
        }
        wire::TemporaryDispatchConsentKindWire::HomeLeaderConsent => {
            TemporaryDispatchConsentKind::HomeLeaderConsent
        }
        wire::TemporaryDispatchConsentKindWire::WorkspaceOrchestratorApproval => {
            TemporaryDispatchConsentKind::WorkspaceOrchestratorApproval
        }
    }
}

fn consent_actor_wire(
    actor: &TemporaryDispatchActorProvenance,
) -> wire::TemporaryDispatchLifecycleActorReceiptWire {
    wire::TemporaryDispatchLifecycleActorReceiptWire {
        activity_id: actor.activity_id().into_uuid(),
        agent_id: actor.agent_id().into_uuid(),
        session_id: actor.session_id().into_uuid(),
        participation_id: actor.participation_id().into_uuid(),
        profile_id: actor.profile_id(),
        participant_id: actor.participant_id(),
        display_name: actor.display_name().to_owned(),
    }
}

fn consent_receipt(
    receipt: &TemporaryDispatchConsentReceipt,
) -> wire::TemporaryDispatchConsentReceiptWire {
    wire::TemporaryDispatchConsentReceiptWire {
        request_id: receipt.request_id(),
        receipt_id: receipt.receipt_id(),
        kind: consent_kind_wire(receipt.kind()),
        revision: receipt.revision(),
        authority_revision: receipt.authority_revision(),
        actor: consent_actor_wire(receipt.actor()),
        recorded_at_ms: receipt.recorded_at().get(),
        replayed: receipt.replayed(),
    }
}

fn withdrawal_receipt(
    receipt: &TemporaryDispatchConsentWithdrawalReceipt,
) -> wire::TemporaryDispatchConsentWithdrawalReceiptWire {
    wire::TemporaryDispatchConsentWithdrawalReceiptWire {
        request_id: receipt.request_id(),
        receipt_id: receipt.receipt_id(),
        consent_receipt_id: receipt.consent_receipt_id(),
        kind: consent_kind_wire(receipt.kind()),
        revision: receipt.revision(),
        authority_revision: receipt.authority_revision(),
        actor: consent_actor_wire(receipt.actor()),
        recorded_at_ms: receipt.recorded_at().get(),
        replayed: receipt.replayed(),
    }
}

fn reservation_receipt(
    receipt: &TemporaryDispatchReservationReceipt,
) -> wire::TemporaryDispatchReservationReceiptWire {
    wire::TemporaryDispatchReservationReceiptWire {
        request_id: receipt.request_id(),
        state: match receipt.state() {
            TemporaryDispatchReservationState::PendingConflict => {
                wire::TemporaryDispatchReservationStateWire::PendingConflict
            }
            TemporaryDispatchReservationState::Reserved => {
                wire::TemporaryDispatchReservationStateWire::Reserved
            }
        },
        revision: receipt.revision(),
        target_participation_id: receipt.target_participation_id(),
        target_profile_id: receipt.target_profile_id(),
        target_membership_revision: receipt.target_membership_revision(),
        requesting_leadership_revision: receipt.requesting_leadership_revision(),
        home_leadership_revision: receipt.home_leadership_revision(),
        workspace_leadership_revision: receipt.workspace_leadership_revision(),
        target_consent_receipt_id: receipt.target_consent_receipt_id(),
        home_leader_consent_receipt_id: receipt.home_leader_consent_receipt_id(),
        orchestrator_consent_receipt_id: receipt.orchestrator_consent_receipt_id(),
        actor: consent_actor_wire(receipt.actor()),
        recorded_at_ms: receipt.recorded_at().get(),
        replayed: receipt.replayed(),
    }
}

fn consent_kind_wire(kind: TemporaryDispatchConsentKind) -> wire::TemporaryDispatchConsentKindWire {
    match kind {
        TemporaryDispatchConsentKind::TargetAcceptance => {
            wire::TemporaryDispatchConsentKindWire::TargetAcceptance
        }
        TemporaryDispatchConsentKind::HomeLeaderConsent => {
            wire::TemporaryDispatchConsentKindWire::HomeLeaderConsent
        }
        TemporaryDispatchConsentKind::WorkspaceOrchestratorApproval => {
            wire::TemporaryDispatchConsentKindWire::WorkspaceOrchestratorApproval
        }
    }
}
