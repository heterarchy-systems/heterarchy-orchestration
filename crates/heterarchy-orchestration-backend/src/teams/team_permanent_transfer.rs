//! HTTP adapter for immutable permanent Home-Team transfer intent and consent.
//!
//! The proposal, current source Home Leader consent, and target Profile
//! acceptance are separate point-in-time records. These routes perform no
//! membership cutover, slot assignment, work settlement, or handoff.

use axum::extract::{DefaultBodyLimit, Extension, Path, State};
use axum::routing::post;
use axum::{Json, Router};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_operations::{
    ActorSnapshot, TeamPermanentTransferConsentReceipt, TeamPermanentTransferIntentReceipt,
    TeamPermanentTransferSnapshot,
};
use heterarchy_orchestration_runtime::{
    OrchestrationRuntime, RuntimeError,
    leadership::member_agent_id,
    participation::ParticipationTarget,
    team_permanent_transfer::{
        TeamPermanentTransferActorTarget, TeamPermanentTransferConsentReadSpec,
        TeamPermanentTransferConsentSpec, TeamPermanentTransferIntentSpec,
        TeamPermanentTransferPreparationReadSpec, TeamPermanentTransferPreparationSpec,
    },
};

use crate::app_support::{LaneError, validate_workspace_reference};
use crate::wire::{
    WorkspaceQueryScope, team_expansion as base_wire, team_permanent_transfer as wire,
};
use crate::{ApiError, BackendState};

mod renewal;
mod source;

pub(super) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/permanent-transfers/propose",
            post(propose_transfer),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/permanent-transfers/consent",
            post(record_consent),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/permanent-transfers/readback",
            post(readback_transfer),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/permanent-transfers/consent/readback",
            post(readback_consent),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/permanent-transfers/prepare",
            post(prepare_transfer),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/permanent-transfers/preparation/readback",
            post(readback_preparation),
        )
        .merge(renewal::routes())
        .merge(source::routes())
        .layer(DefaultBodyLimit::max(
            base_wire::MAX_TEAM_EXPANSION_BODY_BYTES,
        ))
}

fn preparation_spec(
    request: &wire::TeamPermanentTransferPreparationRequest,
) -> TeamPermanentTransferPreparationSpec {
    TeamPermanentTransferPreparationSpec {
        actor: actor_target(&request.actor),
        preparation_id: request.command.preparation_id,
        transfer_id: request.command.transfer_id,
        expected_generation: request.command.expected_generation,
        expected_workspace_leadership_revision: request
            .command
            .expected_workspace_leadership_revision,
        source_leader_consent_id: request.command.source_leader_consent_id,
        target_acceptance_id: request.command.target_acceptance_id,
    }
}

async fn prepare_transfer(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::TeamPermanentTransferPreparationRequest>,
) -> Result<Json<wire::TeamPermanentTransferPreparationResponse>, LaneError> {
    validate_actor_path(&workspace_path, &request.actor)?;
    if !wire::valid_preparation_request(&request) {
        return Err(LaneError::InvalidIdentity);
    }
    let actor = TeamPermanentTransferActorTarget {
        workspace_id: request.actor.scope.workspace_id,
        activity_id: request.actor.activity_id,
        session_id: request.actor.session_id,
    };
    let scope = request.actor.scope.clone();
    let spec = preparation_spec(&request);
    let receipt = run_transfer_mutation(&state, scope, actor, principal, move |runtime| {
        let preparation_id = spec.preparation_id;
        let transfer_id = spec.transfer_id;
        runtime
            .prepare_team_permanent_transfer(spec, principal)
            .inspect_err(|error| {
                tracing::debug!(?error, %preparation_id, %transfer_id,
                "Home transfer preparation was not confirmed");
            })
    })
    .await?;
    let response = wire::TeamPermanentTransferPreparationResponse {
        request: request.clone(),
        receipt: preparation_receipt_wire(&receipt)?,
    };
    if !wire::valid_preparation_response(&response, &request) {
        return Err(ApiError::SourceBindingUnconfirmed.into());
    }
    Ok(Json(response))
}

async fn readback_preparation(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::TeamPermanentTransferPreparationReadbackRequest>,
) -> Result<Json<wire::TeamPermanentTransferPreparationReadbackResponse>, LaneError> {
    validate_actor_path(&workspace_path, &request.current_actor)?;
    if crate::canonical_uuid(&workspace_path)? != request.workspace_id
        || !wire::valid_preparation_readback_request(&request)
    {
        return Err(LaneError::InvalidIdentity);
    }
    let actor = TeamPermanentTransferActorTarget {
        workspace_id: request.current_actor.scope.workspace_id,
        activity_id: request.current_actor.activity_id,
        session_id: request.current_actor.session_id,
    };
    let scope = request.current_actor.scope.clone();
    let spec = TeamPermanentTransferPreparationReadSpec {
        workspace_id: request.workspace_id,
        preparation_id: request.preparation_id,
        actor: actor_target(&request.current_actor),
    };
    let result = run_transfer_query(&state, scope, actor, principal, move |runtime| {
        runtime.read_team_permanent_transfer_preparation(spec, principal)
    })
    .await?
    .as_ref()
    .map(preparation_receipt_wire)
    .transpose()?;
    let response = wire::TeamPermanentTransferPreparationReadbackResponse {
        request: request.clone(),
        result,
    };
    if !wire::valid_preparation_readback_response(&response, &request) {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(Json(response))
}

fn actor_target(actor: &base_wire::TeamExpansionActorWire) -> TeamPermanentTransferActorTarget {
    TeamPermanentTransferActorTarget {
        workspace_id: actor.scope.workspace_id,
        activity_id: actor.activity_id,
        session_id: actor.session_id,
    }
}

fn participation_target(actor: TeamPermanentTransferActorTarget) -> ParticipationTarget {
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

fn intent_spec(
    request: &wire::TeamPermanentTransferIntentRequest,
) -> TeamPermanentTransferIntentSpec {
    let command = request.command;
    TeamPermanentTransferIntentSpec {
        actor: actor_target(&request.actor),
        transfer_id: command.transfer_id,
        source_team_id: command.source_team_id,
        destination_team_id: command.destination_team_id,
        target_participation_id: command.target_participation_id,
        destination_request_id: command.destination_request_id,
        destination_decision_id: command.destination_decision_id,
        destination_ordinal: command.destination_ordinal,
        expected_destination_team_revision: command.expected_destination_team_revision,
        expected_destination_capacity_revision: command.expected_destination_capacity_revision,
        expected_workspace_leadership_revision: command.expected_workspace_leadership_revision,
        expected_source_membership_revision: command.expected_source_membership_revision,
        expected_source_leadership_revision: command.expected_source_leadership_revision,
    }
}

async fn propose_transfer(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::TeamPermanentTransferIntentRequest>,
) -> Result<Json<wire::TeamPermanentTransferIntentResponse>, LaneError> {
    validate_actor_path(&workspace_path, &request.actor)?;
    if !wire::valid_intent_request(&request) {
        return Err(LaneError::InvalidIdentity);
    }
    let actor = actor_target(&request.actor);
    let scope = request.actor.scope.clone();
    let spec = intent_spec(&request);
    let receipt = run_transfer_mutation(&state, scope, actor, principal, move |runtime| {
        runtime.propose_team_permanent_transfer(spec, principal)
    })
    .await?;
    let response = wire::TeamPermanentTransferIntentResponse {
        request: request.clone(),
        receipt: intent_receipt_wire(&receipt),
    };
    if !wire::valid_intent_response(&response, &request) {
        return Err(ApiError::SourceBindingUnconfirmed.into());
    }
    Ok(Json(response))
}

async fn record_consent(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::TeamPermanentTransferConsentRequest>,
) -> Result<Json<wire::TeamPermanentTransferConsentResponse>, LaneError> {
    validate_actor_path(&workspace_path, &request.actor)?;
    if !wire::valid_consent_request(&request) {
        return Err(LaneError::InvalidIdentity);
    }
    let actor = actor_target(&request.actor);
    let scope = request.actor.scope.clone();
    let kind = match request.command.kind {
        wire::TeamPermanentTransferConsentKindWire::SourceHomeLeader => {
            heterarchy_orchestration_operations::TeamPermanentTransferConsentKind::SourceHomeLeader
        }
        wire::TeamPermanentTransferConsentKindWire::TargetAgentAcceptance => {
            heterarchy_orchestration_operations::TeamPermanentTransferConsentKind::TargetAgentAcceptance
        }
    };
    let spec = TeamPermanentTransferConsentSpec {
        actor,
        receipt_id: request.command.receipt_id,
        transfer_id: request.command.transfer_id,
        kind,
    };
    let receipt = run_transfer_mutation(&state, scope, actor, principal, move |runtime| {
        runtime.record_team_permanent_transfer_consent(spec, principal)
    })
    .await?;
    let response = wire::TeamPermanentTransferConsentResponse {
        request: request.clone(),
        receipt: consent_receipt_wire(&receipt),
    };
    if !wire::valid_consent_response(&response, &request) {
        return Err(ApiError::SourceBindingUnconfirmed.into());
    }
    Ok(Json(response))
}

async fn readback_transfer(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::TeamPermanentTransferReadbackRequest>,
) -> Result<Json<wire::TeamPermanentTransferReadbackResponse>, LaneError> {
    validate_actor_path(&workspace_path, &request.current_actor)?;
    if crate::canonical_uuid(&workspace_path)? != request.workspace_id
        || !wire::valid_readback_request(&request)
    {
        return Err(LaneError::InvalidIdentity);
    }
    let reader = actor_target(&request.current_actor);
    let scope = request.current_actor.scope.clone();
    let transfer_id = request.transfer_id;
    let result = run_transfer_query(&state, scope, reader, principal, move |runtime| {
        runtime.read_team_permanent_transfer_snapshot(reader, transfer_id, principal)
    })
    .await?
    .map(|snapshot| snapshot_wire(&snapshot));
    let response = wire::TeamPermanentTransferReadbackResponse {
        request: request.clone(),
        result,
    };
    if !wire::valid_readback_response(&response, &request) {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(Json(response))
}

async fn readback_consent(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::TeamPermanentTransferConsentReadbackRequest>,
) -> Result<Json<wire::TeamPermanentTransferConsentReadbackResponse>, LaneError> {
    validate_actor_path(&workspace_path, &request.current_actor)?;
    if crate::canonical_uuid(&workspace_path)? != request.intent.actor.scope.workspace_id
        || !wire::valid_consent_readback_request(&request)
    {
        return Err(LaneError::InvalidIdentity);
    }
    let reader = actor_target(&request.current_actor);
    let scope = request.current_actor.scope.clone();
    let kind = match request.intent.command.kind {
        wire::TeamPermanentTransferConsentKindWire::SourceHomeLeader => {
            heterarchy_orchestration_operations::TeamPermanentTransferConsentKind::SourceHomeLeader
        }
        wire::TeamPermanentTransferConsentKindWire::TargetAgentAcceptance => {
            heterarchy_orchestration_operations::TeamPermanentTransferConsentKind::TargetAgentAcceptance
        }
    };
    let spec = TeamPermanentTransferConsentReadSpec {
        intent: TeamPermanentTransferConsentSpec {
            actor: actor_target(&request.intent.actor),
            receipt_id: request.intent.command.receipt_id,
            transfer_id: request.intent.command.transfer_id,
            kind,
        },
        reader,
    };
    let result = run_transfer_query(&state, scope, reader, principal, move |runtime| {
        runtime.read_team_permanent_transfer_consent(spec, principal)
    })
    .await?;
    let response = wire::TeamPermanentTransferConsentReadbackResponse {
        request: request.clone(),
        result: result.as_ref().map(consent_receipt_wire),
    };
    if !wire::valid_consent_readback_response(&response, &request) {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(Json(response))
}

fn intent_receipt_wire(
    receipt: &TeamPermanentTransferIntentReceipt,
) -> wire::TeamPermanentTransferIntentReceiptWire {
    wire::TeamPermanentTransferIntentReceiptWire {
        transfer_id: receipt.transfer_id(),
        workspace_id: receipt.workspace_id().into_uuid(),
        source_team_id: receipt.source_team_id(),
        destination_team_id: receipt.destination_team_id(),
        target_participation_id: receipt.target_participation_id().into_uuid(),
        target_profile_id: receipt.target_profile_id(),
        source_membership_revision: receipt.source_membership_revision(),
        source_leadership_revision: receipt.source_leadership_revision(),
        source_leader_participation_id: receipt.source_leader_participation_id().into_uuid(),
        source_leader_profile_id: receipt.source_leader_profile_id(),
        destination_team_revision: receipt.destination_team_revision(),
        destination_capacity_revision: receipt.destination_capacity_revision(),
        destination_workspace_leadership_revision: receipt
            .destination_workspace_leadership_revision(),
        destination_request_id: receipt.destination_request_id(),
        destination_decision_id: receipt.destination_decision_id(),
        destination_ordinal: receipt.destination_ordinal(),
        workspace_actor: actor_snapshot_wire(receipt.workspace_actor()),
        recorded_at_ms: receipt.recorded_at().get(),
        replayed: receipt.replayed(),
    }
}

fn preparation_receipt_wire(
    receipt: &heterarchy_orchestration_operations::TeamPermanentTransferPreparationReceipt,
) -> Result<wire::TeamPermanentTransferPreparationReceiptWire, RuntimeError> {
    let heterarchy_orchestration_operations::OperationalActor::Participant {
        activity_id,
        agent_id,
        session_id,
    } = receipt.actor()
    else {
        return Err(RuntimeError::persistence(
            heterarchy_orchestration_operations::OperationsError::Corrupt(
                "transfer preparation author",
            ),
        ));
    };
    let payload_digest = receipt.digest();
    let bindings = receipt
        .bindings()
        .map(preparation_binding_wire)
        .collect::<Result<Vec<_>, RuntimeError>>()?;
    Ok(wire::TeamPermanentTransferPreparationReceiptWire {
        preparation_id: receipt.preparation_id(),
        workspace_id: receipt.workspace_id().into_uuid(),
        transfer_id: receipt.transfer_id(),
        source_team_id: receipt.source_team_id(),
        destination_team_id: receipt.destination_team_id(),
        generation: receipt.generation(),
        workspace_leadership_revision: receipt.workspace_leadership_revision(),
        source_leader_consent_id: receipt.source_leader_consent_id(),
        target_acceptance_id: receipt.target_acceptance_id(),
        target_participation_id: receipt.target_participation_id().into_uuid(),
        target_profile_id: receipt.target_profile_id(),
        target_participant_ids: receipt.participant_ids().collect(),
        identity_version: 1,
        participation_revision: receipt.participation_revision(),
        source_membership_revision: receipt.source_membership_revision(),
        intent_digest: digest_hex(&receipt.intent_digest()),
        sessions: receipt
            .sessions()
            .map(forge_domain::AgentSessionId::into_uuid)
            .collect(),
        bindings,
        actor: base_wire::TeamExpansionActorReceiptWire {
            activity_id: activity_id.into_uuid(),
            agent_id: agent_id.into_uuid(),
            session_id: session_id.into_uuid(),
            participation_id: receipt.actor_participation_id().into_uuid(),
            profile_id: receipt.actor_profile_id(),
            participant_id: receipt.actor_participant_id(),
            display_name: receipt.actor_display_name().to_owned(),
        },
        recorded_at_ms: receipt.recorded_at().get(),
        payload_digest: digest_hex(&payload_digest),
        session_count: u16::try_from(receipt.session_count()).map_err(|_| {
            RuntimeError::persistence(
                heterarchy_orchestration_operations::OperationsError::Corrupt(
                    "transfer preparation Session count",
                ),
            )
        })?,
        binding_count: u16::try_from(receipt.binding_count()).map_err(|_| {
            RuntimeError::persistence(
                heterarchy_orchestration_operations::OperationsError::Corrupt(
                    "transfer preparation binding count",
                ),
            )
        })?,
        state: wire::TeamPermanentTransferPreparationStateWire::Preparing,
        replayed: receipt.replayed(),
    })
}

fn preparation_binding_wire(
    binding: heterarchy_orchestration_operations::TeamPermanentTransferPreparationBinding,
) -> Result<wire::TeamPermanentTransferBindingWire, RuntimeError> {
    let principal = match binding.principal().map_err(RuntimeError::persistence)? {
        forge_domain::AuthenticatedPrincipal::TrustedStdio => {
            wire::TeamPermanentTransferCapturedPrincipalWire::TrustedStdio
        }
        forge_domain::AuthenticatedPrincipal::DirectToken(id) => {
            wire::TeamPermanentTransferCapturedPrincipalWire::DirectToken {
                token_id: id.into_uuid(),
            }
        }
        forge_domain::AuthenticatedPrincipal::OAuthGrant(id) => {
            wire::TeamPermanentTransferCapturedPrincipalWire::OAuthGrant {
                grant_id: id.into_uuid(),
            }
        }
    };
    let origin = match binding.origin() {
        heterarchy_orchestration_operations::TeamPermanentTransferBindingOrigin::Registration(
            _,
        ) => wire::TeamPermanentTransferBindingOriginWire::Registration,
        heterarchy_orchestration_operations::TeamPermanentTransferBindingOrigin::LegacyAdoption(
            _,
        ) => wire::TeamPermanentTransferBindingOriginWire::LegacyAdoption,
    };
    Ok(wire::TeamPermanentTransferBindingWire {
        participant_id: binding.participant_id(),
        activity_id: binding.activity_id().into_uuid(),
        agent_id: binding.agent_id().into_uuid(),
        session_id: binding.session_id().into_uuid(),
        source_request_id: binding.source_request_id(),
        origin,
        principal,
    })
}

fn digest_hex(digest: &[u8; 32]) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut encoded, "{byte:02x}");
    }
    encoded
}

fn consent_receipt_wire(
    receipt: &TeamPermanentTransferConsentReceipt,
) -> wire::TeamPermanentTransferConsentReceiptWire {
    wire::TeamPermanentTransferConsentReceiptWire {
        receipt_id: receipt.receipt_id(),
        transfer_id: receipt.transfer_id(),
        kind: consent_kind_wire(receipt.kind()),
        authority_revision: receipt.authority_revision(),
        membership_revision: receipt.membership_revision(),
        actor: actor_snapshot_wire(receipt.actor()),
        recorded_at_ms: receipt.recorded_at().get(),
        replayed: receipt.replayed(),
    }
}

fn consent_kind_wire(
    kind: heterarchy_orchestration_operations::TeamPermanentTransferConsentKind,
) -> wire::TeamPermanentTransferConsentKindWire {
    match kind {
        heterarchy_orchestration_operations::TeamPermanentTransferConsentKind::SourceHomeLeader => {
            wire::TeamPermanentTransferConsentKindWire::SourceHomeLeader
        }
        heterarchy_orchestration_operations::TeamPermanentTransferConsentKind::TargetAgentAcceptance => {
            wire::TeamPermanentTransferConsentKindWire::TargetAgentAcceptance
        }
    }
}

fn actor_snapshot_wire(actor: &ActorSnapshot) -> base_wire::TeamExpansionActorReceiptWire {
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

fn snapshot_wire(
    snapshot: &TeamPermanentTransferSnapshot,
) -> wire::TeamPermanentTransferSnapshotWire {
    wire::TeamPermanentTransferSnapshotWire {
        intent: intent_receipt_wire(snapshot.intent()),
        source_home_leader_consent: snapshot
            .source_home_leader_consent()
            .map(consent_receipt_wire),
        target_agent_acceptance: snapshot.target_agent_acceptance().map(consent_receipt_wire),
    }
}

async fn run_transfer_mutation<T, F>(
    state: &BackendState,
    scope: WorkspaceQueryScope,
    actor: TeamPermanentTransferActorTarget,
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

async fn run_transfer_query<T, F>(
    state: &BackendState,
    scope: WorkspaceQueryScope,
    actor: TeamPermanentTransferActorTarget,
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
