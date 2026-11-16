//! Exact readback for historical `AgentRequest` admission evidence.

use crate::wire::agent_request_admission::{
    AdmissionEffectScopeWire, AdmissionIndependenceWire, AdmissionRequestStateWire,
    AdmissionResolutionKindWire, AdmissionResolutionSlotWire, AdmissionResolutionWire,
    AgentRequestAdmissionReceiptWire, AgentRequestAdmissionRecoverResponse,
    AgentRequestAdmissionRequest, AgentRequestAdmissionSnapshotWire,
};
use crate::{ApiError, BackendState};
use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    routing::post,
};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_runtime::provisioning::{
    AgentRequestAdmissionSnapshot, AgentRequestResolutionSummary, AgentRequestSubmit,
    AgentRequestSubmitSummary,
};

pub(super) fn routes() -> Router<BackendState> {
    Router::new().route(
        "/internal/v1/workspaces/{workspace_id}/agent-requests/admit/recover",
        post(recover),
    )
}

fn validate(path: &str, body: &AgentRequestAdmissionRequest) -> Result<(), ApiError> {
    crate::app_support::validate_workspace_reference(
        &body.scope,
        Some(body.coordinator_session_id),
    )?;
    if crate::canonical_uuid(path)? != body.scope.workspace_id
        || body.activity_id.is_nil()
        || body.coordinator_agent_id.is_nil()
        || body.request_id.is_nil()
        || body.coordinator_revision <= 0
    {
        return Err(ApiError::InvalidIdentity);
    }
    Ok(())
}

fn command(body: &AgentRequestAdmissionRequest) -> Result<AgentRequestSubmit, ApiError> {
    Ok(AgentRequestSubmit {
        workspace_id: body.scope.workspace_id,
        activity_id: body.activity_id,
        requester_agent_id: body.coordinator_agent_id,
        requester_session_id: body.coordinator_session_id,
        coordinator_revision: u64::try_from(body.coordinator_revision)
            .map_err(|_| ApiError::InvalidIdentity)?,
        request_id: body.request_id,
        objective: body.objective.clone(),
        capability: body.capability.clone(),
        count: body.count,
        reason: body.reason.clone(),
        created_at_ms: 0,
        effect_scope: match body.effect_scope {
            AdmissionEffectScopeWire::ReadOnly => {
                heterarchy_orchestration_operations::AgentRequestEffectScope::ReadOnly
            }
            AdmissionEffectScopeWire::SourceWrite => {
                heterarchy_orchestration_operations::AgentRequestEffectScope::SourceWrite
            }
        },
        independence: match body.independence {
            AdmissionIndependenceWire::None => {
                heterarchy_orchestration_operations::AgentRequestIndependence::None
            }
            AdmissionIndependenceWire::IndependentFromCurrentWriter => {
                heterarchy_orchestration_operations::AgentRequestIndependence::IndependentFromCurrentWriter
            }
        },
    })
}

async fn recover(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<AgentRequestAdmissionRequest>,
) -> Result<Json<AgentRequestAdmissionRecoverResponse>, ApiError> {
    validate(&path, &body)?;
    let input = command(&body)?;
    let snapshot = state
        .catalog_job(move |runtime| runtime.recover_agent_request_admission(input, principal))
        .await?;
    Ok(Json(AgentRequestAdmissionRecoverResponse {
        request: body,
        snapshot: snapshot.as_ref().map(project_snapshot).transpose()?,
    }))
}

fn state(value: &str) -> Result<AdmissionRequestStateWire, ApiError> {
    match value {
        "resolution_pending" => Ok(AdmissionRequestStateWire::ResolutionPending),
        "resolution_reserved" => Ok(AdmissionRequestStateWire::ResolutionReserved),
        "fulfilled" => Ok(AdmissionRequestStateWire::Fulfilled),
        _ => Err(ApiError::Unavailable),
    }
}

fn receipt(
    value: &AgentRequestSubmitSummary,
) -> Result<AgentRequestAdmissionReceiptWire, ApiError> {
    Ok(AgentRequestAdmissionReceiptWire {
        request_id: value.request_id,
        workspace_id: value.workspace_id,
        activity_id: value.activity_id,
        state: state(value.state)?,
        count: value.count,
        created: value.created,
        revision: value.revision,
    })
}

fn resolution(value: &AgentRequestResolutionSummary) -> Result<AdmissionResolutionWire, ApiError> {
    let slots = value
        .slots
        .iter()
        .map(|slot| {
            let kind = match slot.kind {
                "reuse_current_scope" => AdmissionResolutionKindWire::ReuseCurrentScope,
                "reuse_ready_runtime" => AdmissionResolutionKindWire::ReuseReadyRuntime,
                "wake_existing_runtime" => AdmissionResolutionKindWire::WakeExistingRuntime,
                "wake_new_runtime" => AdmissionResolutionKindWire::WakeNewRuntime,
                "provision_new" => AdmissionResolutionKindWire::ProvisionNew,
                _ => return Err(ApiError::Unavailable),
            };
            Ok(AdmissionResolutionSlotWire {
                slot_ordinal: slot.slot_ordinal,
                kind,
                claim_id: slot.claim_id,
                profile_id: slot.profile_id,
                runtime_session_id: slot.runtime_session_id,
                runtime_session_revision: slot.runtime_session_revision,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok(AdmissionResolutionWire {
        request_id: value.request_id,
        workspace_id: value.workspace_id,
        request_state: state(value.request_state)?,
        request_revision: value.request_revision,
        slots,
    })
}

fn project_snapshot(
    value: &AgentRequestAdmissionSnapshot,
) -> Result<AgentRequestAdmissionSnapshotWire, ApiError> {
    Ok(AgentRequestAdmissionSnapshotWire {
        receipt: receipt(&value.receipt)?,
        resolution: value.resolution.as_ref().map(resolution).transpose()?,
        operations: value
            .operations
            .iter()
            .map(super::operations::operation_wire)
            .collect(),
    })
}
