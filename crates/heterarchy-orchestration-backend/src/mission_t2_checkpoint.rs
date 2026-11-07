//! HTTP adapters for source-backed T2 deferral and exact readback.
//!
//! The only mutation is a defer statement. No endpoint here decides a final
//! score, selects a Leader, or interprets the defer as independent evidence.

use axum::extract::{Path, State};
use axum::routing::post;
use axum::{Extension, Json, Router};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_operations::MissionT2DeferredCheckpoint;
use heterarchy_orchestration_runtime::RuntimeError;
use heterarchy_orchestration_runtime::mission_evaluation::MissionEvaluationParticipantSpec;
use heterarchy_orchestration_runtime::mission_t2_checkpoint::{
    MissionT2DeferContentSpec, MissionT2DeferSpec,
};
use uuid::Uuid;

use crate::app_support::validate_workspace_reference;
use crate::catalog_execution::CatalogLane;
use crate::wire::mission_evaluation::MissionEvaluationMutationContextWire;
use crate::wire::mission_t2_checkpoint as wire;
use crate::{ApiError, BackendState, ScopedMutationAuthority};

/// Register the T2 defer and request-readback paths.
pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/missions/{mission_id}/evaluation-plans/{plan_id}/t2/defer",
            post(defer_checkpoint),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/missions/{mission_id}/evaluation-plans/{plan_id}/t2/readback",
            post(readback_checkpoint),
        )
}

async fn defer_checkpoint(
    State(state): State<BackendState>,
    Path((workspace_path, mission_path, plan_path)): Path<(String, String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::MissionT2DeferRequest>,
) -> Result<Json<wire::MissionT2DeferResponse>, ApiError> {
    validate_mutation_context(&workspace_path, &request.context)?;
    validate_plan_path(
        &workspace_path,
        &mission_path,
        &plan_path,
        request.mission_id,
        request.plan_id,
    )?;
    if !wire::valid_defer_request(&request) {
        return Err(ApiError::InvalidIdentity);
    }
    let scope = request.context.scope.clone();
    let spec = defer_spec(&request);
    let outcome = state
        .run_workspace_reference_mutation(
            scope.clone(),
            request.context.session_id,
            principal,
            ScopedMutationAuthority::WorkspaceMember,
            CatalogLane::Ordinary,
            move |runtime, _| {
                runtime.defer_mission_t2_checkpoint(&spec, principal, || {
                    crate::current_timestamp_millis().map_err(|_| RuntimeError::InvalidInput)
                })
            },
        )
        .await?;
    Ok(Json(wire::MissionT2DeferResponse {
        scope,
        request,
        checkpoint: checkpoint_wire(outcome.checkpoint())?,
        replayed: outcome.replayed(),
    }))
}

async fn readback_checkpoint(
    State(state): State<BackendState>,
    Path((workspace_path, mission_path, plan_path)): Path<(String, String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<wire::MissionT2DeferReadbackRequest>,
) -> Result<Json<wire::MissionT2DeferReadbackResponse>, ApiError> {
    if !wire::valid_readback_request(&request) {
        return Err(ApiError::InvalidIdentity);
    }
    validate_workspace_reference(&request.reader.scope, Some(request.reader.session_id))?;
    if crate::canonical_uuid(&workspace_path)? != request.reader.scope.workspace_id {
        return Err(ApiError::InvalidIdentity);
    }
    validate_plan_path(
        &workspace_path,
        &mission_path,
        &plan_path,
        request.intent.mission_id,
        request.intent.plan_id,
    )?;
    let scope = request.reader.scope.clone();
    let reader_session_id = request.reader.session_id;
    let intent = defer_content_spec(&request.intent);
    let reader = MissionEvaluationParticipantSpec {
        activity_id: request.reader.sender_activity_id,
        session_id: reader_session_id,
        expected_workspace_leadership_revision: request
            .reader
            .expected_workspace_leadership_revision,
    };
    let checkpoint = state
        .run_workspace_reference_query(
            scope,
            Some(reader_session_id),
            principal,
            move |runtime, _| {
                runtime.mission_t2_deferred_checkpoint_for_request(&intent, reader, principal)
            },
        )
        .await?;
    Ok(Json(wire::MissionT2DeferReadbackResponse {
        request,
        checkpoint: checkpoint.as_ref().map(checkpoint_wire).transpose()?,
    }))
}

fn defer_spec(request: &wire::MissionT2DeferRequest) -> MissionT2DeferSpec {
    MissionT2DeferSpec {
        request_id: request.request_id,
        checkpoint_id: request.checkpoint_id,
        workspace_id: request.context.scope.workspace_id,
        mission_id: request.mission_id,
        plan_id: request.plan_id,
        expected_revision: request.expected_revision,
        expected_previous_checkpoint_id: request.expected_previous_checkpoint_id,
        reason: request.reason.clone(),
        next_check_condition: request.next_check_condition.clone(),
        next_check_at_ms: request.next_check_at_ms,
        participant: participant_spec(&request.context),
    }
}

fn defer_content_spec(request: &wire::MissionT2DeferContentWire) -> MissionT2DeferContentSpec {
    MissionT2DeferContentSpec {
        request_id: request.request_id,
        checkpoint_id: request.checkpoint_id,
        workspace_id: request.workspace_id,
        mission_id: request.mission_id,
        plan_id: request.plan_id,
        expected_revision: request.expected_revision,
        expected_previous_checkpoint_id: request.expected_previous_checkpoint_id,
        reason: request.reason.clone(),
        next_check_condition: request.next_check_condition.clone(),
        next_check_at_ms: request.next_check_at_ms,
    }
}

fn participant_spec(
    context: &MissionEvaluationMutationContextWire,
) -> MissionEvaluationParticipantSpec {
    MissionEvaluationParticipantSpec {
        activity_id: context.sender_activity_id,
        session_id: context.session_id,
        expected_workspace_leadership_revision: context.expected_workspace_leadership_revision,
    }
}

fn checkpoint_wire(
    record: &MissionT2DeferredCheckpoint,
) -> Result<wire::MissionT2DeferredCheckpointWire, ApiError> {
    let owner = record.accountable_owner();
    let (kind, participation_id, profile_id) = match (owner.participation_id(), owner.profile_id())
    {
        (Some(participation_id), Some(profile_id)) => (
            wire::MissionT2AccountableOwnerKindWire::ProfileParticipant,
            Some(participation_id.into_uuid()),
            Some(profile_id),
        ),
        (None, None) => (
            wire::MissionT2AccountableOwnerKindWire::NativeParticipant,
            None,
            None,
        ),
        _ => return Err(ApiError::InvalidIdentity),
    };
    Ok(wire::MissionT2DeferredCheckpointWire {
        request_id: record.request_id(),
        checkpoint_id: record.checkpoint_id().into_uuid(),
        workspace_id: record.workspace_id().into_uuid(),
        mission_id: record.mission_id().into_uuid(),
        plan_id: record.plan_id().into_uuid(),
        revision: record.revision(),
        sequence: record.sequence(),
        previous_checkpoint_id: record
            .previous_checkpoint_id()
            .map(heterarchy_orchestration_operations::MissionT2CheckpointId::into_uuid),
        reason: record.reason().to_owned(),
        next_check_condition: record.next_check_condition().to_owned(),
        next_check_at_ms: record.next_check_at_ms(),
        recorded_at_ms: record.recorded_at_ms(),
        accountable_owner: wire::MissionT2AccountableOwnerWire {
            activity_id: owner.activity_id().into_uuid(),
            agent_id: owner.agent_id().into_uuid(),
            session_id: owner.session_id().into_uuid(),
            kind,
            participation_id,
            profile_id,
            participant_id: owner.participant_id(),
            display_name: owner.display_name().to_owned(),
            leadership_revision: owner.leadership_revision(),
        },
    })
}

fn validate_mutation_context(
    workspace_path: &str,
    context: &MissionEvaluationMutationContextWire,
) -> Result<(), ApiError> {
    validate_workspace_reference(&context.scope, Some(context.session_id))?;
    if crate::canonical_uuid(workspace_path)? != context.scope.workspace_id
        || context.sender_activity_id.is_nil()
        || context.expected_workspace_leadership_revision < 1
    {
        return Err(ApiError::InvalidIdentity);
    }
    Ok(())
}

fn validate_plan_path(
    workspace_path: &str,
    mission_path: &str,
    plan_path: &str,
    mission_id: Uuid,
    plan_id: Uuid,
) -> Result<(), ApiError> {
    if crate::canonical_uuid(workspace_path)?.is_nil()
        || crate::canonical_uuid(mission_path)? != mission_id
        || crate::canonical_uuid(plan_path)? != plan_id
    {
        return Err(ApiError::InvalidIdentity);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> wire::MissionT2DeferRequest {
        wire::MissionT2DeferRequest {
            context: MissionEvaluationMutationContextWire {
                scope: crate::wire::WorkspaceQueryScope {
                    workspace_id: Uuid::from_u128(1),
                    location_id: Uuid::from_u128(2),
                    location_revision: 3,
                    workspace_revision: 4,
                    locator: "workspace".to_owned(),
                },
                session_id: Uuid::from_u128(5),
                sender_activity_id: Uuid::from_u128(6),
                expected_workspace_leadership_revision: 7,
            },
            request_id: Uuid::from_u128(8),
            checkpoint_id: Uuid::from_u128(9),
            mission_id: Uuid::from_u128(10),
            plan_id: Uuid::from_u128(11),
            expected_revision: 12,
            expected_previous_checkpoint_id: None,
            reason: "review pending".to_owned(),
            next_check_condition: "read the reviewer receipt".to_owned(),
            next_check_at_ms: 100,
        }
    }

    #[test]
    fn defer_spec_preserves_request_intent_without_a_timestamp() {
        let request = request();
        let spec = defer_spec(&request);
        assert_eq!(spec.request_id, request.request_id);
        assert_eq!(spec.checkpoint_id, request.checkpoint_id);
        assert_eq!(spec.workspace_id, request.context.scope.workspace_id);
        assert_eq!(spec.mission_id, request.mission_id);
        assert_eq!(spec.plan_id, request.plan_id);
        assert_eq!(spec.expected_revision, request.expected_revision);
        assert_eq!(
            spec.expected_previous_checkpoint_id,
            request.expected_previous_checkpoint_id
        );
        assert_eq!(spec.reason, request.reason);
        assert_eq!(spec.next_check_condition, request.next_check_condition);
        assert_eq!(spec.next_check_at_ms, request.next_check_at_ms);
    }

    #[test]
    fn readback_spec_does_not_require_or_infer_original_actor() {
        let request = request();
        let content = wire::MissionT2DeferContentWire {
            request_id: request.request_id,
            checkpoint_id: request.checkpoint_id,
            workspace_id: request.context.scope.workspace_id,
            mission_id: request.mission_id,
            plan_id: request.plan_id,
            expected_revision: request.expected_revision,
            expected_previous_checkpoint_id: request.expected_previous_checkpoint_id,
            reason: request.reason.clone(),
            next_check_condition: request.next_check_condition.clone(),
            next_check_at_ms: request.next_check_at_ms,
        };
        let spec = defer_content_spec(&content);
        assert_eq!(spec.workspace_id, content.workspace_id);
        assert_eq!(spec.request_id, content.request_id);
        assert_eq!(spec.reason, content.reason);
    }

    #[test]
    fn route_selectors_are_canonical_and_exact() {
        assert!(
            validate_plan_path(
                "00000000-0000-0000-0000-000000000001",
                "00000000-0000-0000-0000-00000000000a",
                "00000000-0000-0000-0000-00000000000b",
                Uuid::from_u128(10),
                Uuid::from_u128(11),
            )
            .is_ok()
        );
        assert!(
            validate_plan_path(
                "00000000-0000-0000-0000-000000000001",
                "00000000-0000-0000-0000-000000000063",
                "00000000-0000-0000-0000-00000000000b",
                Uuid::from_u128(10),
                Uuid::from_u128(11),
            )
            .is_err()
        );
    }
}
