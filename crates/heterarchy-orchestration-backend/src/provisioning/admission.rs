//! Full scoped `AgentRequest` admission and immutable domain-intent recovery.

use crate::wire::agent_request_admission::{
    AdmissionEffectScopeWire, AdmissionIndependenceWire, AdmissionRequestStateWire,
    AdmissionResolutionKindWire, AdmissionResolutionSlotWire, AdmissionResolutionWire,
    AgentRequestAdmissionReceiptWire, AgentRequestAdmissionRecoverResponse,
    AgentRequestAdmissionRequest, AgentRequestAdmissionResponse, AgentRequestAdmissionSnapshotWire,
};
use crate::{ApiError, BackendState, ErrorResponse, RealtimeEventKind};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    routing::post,
};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_runtime::{
    RuntimeError,
    provisioning::{
        AgentRequestAdmissionSnapshot, AgentRequestAdmissionStage, AgentRequestResolutionSummary,
        AgentRequestSubmit, AgentRequestSubmitSummary,
    },
};

pub(super) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/agent-requests/admit",
            post(admit),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/agent-requests/admit/recover",
            post(recover),
        )
}

#[derive(Debug)]
enum AdmissionError {
    Api(ApiError),
    Incomplete(RuntimeError),
}
impl From<ApiError> for AdmissionError {
    fn from(value: ApiError) -> Self {
        Self::Api(value)
    }
}
impl IntoResponse for AdmissionError {
    fn into_response(self) -> Response {
        match self {
            Self::Api(error) => error.into_response(),
            Self::Incomplete(_cause) => (StatusCode::CONFLICT, Json(ErrorResponse {
                code: "COMMITTED_BUT_UNCONFIRMED",
                message: "Read the original AgentRequest intent, resolution and provision targets before resuming.",
                retryable: false,
            })).into_response(),
        }
    }
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

fn command(
    body: &AgentRequestAdmissionRequest,
    created_at_ms: i64,
) -> Result<AgentRequestSubmit, ApiError> {
    use heterarchy_orchestration_operations::{AgentRequestEffectScope, AgentRequestIndependence};
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
        created_at_ms,
        effect_scope: match body.effect_scope {
            AdmissionEffectScopeWire::ReadOnly => AgentRequestEffectScope::ReadOnly,
            AdmissionEffectScopeWire::SourceWrite => AgentRequestEffectScope::SourceWrite,
        },
        independence: match body.independence {
            AdmissionIndependenceWire::None => AgentRequestIndependence::None,
            AdmissionIndependenceWire::IndependentFromCurrentWriter => {
                AgentRequestIndependence::IndependentFromCurrentWriter
            }
        },
    })
}

fn clock(
    stage: AgentRequestAdmissionStage,
) -> Result<i64, (AgentRequestAdmissionStage, RuntimeError)> {
    let failure = || {
        (
            stage,
            RuntimeError::Io(std::io::Error::other("AgentRequest clock unavailable")),
        )
    };
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| failure())?;
    i64::try_from(duration.as_millis()).map_err(|_| failure())
}

async fn admit(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<AgentRequestAdmissionRequest>,
) -> Result<Json<AgentRequestAdmissionResponse>, AdmissionError> {
    validate(&path, &body)?;
    let input = command(&body, 0)?;
    let publisher = state.clone();
    let result = state
        .run_agent_request_mutation(
            Some(body.scope.clone()),
            input,
            principal,
            move |runtime, command| {
                let id = command.request_id;
                let workspace = command.workspace_id;
                let mut revision = runtime.agent_request(id)?.map(|value| value.revision);
                let mut operation_revisions = runtime
                    .agent_request_provision_operations(id)?
                    .into_iter()
                    .map(|value| (value.operation_id, value.revision))
                    .collect::<std::collections::BTreeMap<_, _>>();
                let mut publish = || {
                    publish_delta(
                        runtime,
                        &publisher,
                        id,
                        workspace,
                        &mut revision,
                        &mut operation_revisions,
                    )
                };
                let outcome = runtime.admit_agent_request(
                    command,
                    |stage| {
                        publish().map_err(|error| (stage, error))?;
                        clock(stage)
                    },
                    |stage, error| (stage, RuntimeError::from(error)),
                );
                // Publication follows durable readback, including partially committed
                // failures. It is never evidence of successful provider delivery.
                if let Err(error) = publish() {
                    return Ok(Err((AgentRequestAdmissionStage::Reconcile, error)));
                }
                Ok(outcome)
            },
        )
        .await?;
    let summary = result.map_err(|(stage, error)| {
        if stage == AgentRequestAdmissionStage::Submit {
            AdmissionError::Api(ApiError::Runtime(error))
        } else {
            AdmissionError::Incomplete(error)
        }
    })?;
    Ok(Json(AgentRequestAdmissionResponse {
        receipt: receipt(&summary)?,
        request: body,
    }))
}

async fn recover(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<AgentRequestAdmissionRequest>,
) -> Result<Json<AgentRequestAdmissionRecoverResponse>, ApiError> {
    validate(&path, &body)?;
    let input = command(&body, 0)?;
    let snapshot = state
        .catalog_job(move |runtime| runtime.recover_agent_request_admission(input, principal))
        .await?;
    Ok(Json(AgentRequestAdmissionRecoverResponse {
        request: body,
        snapshot: snapshot.as_ref().map(project_snapshot).transpose()?,
    }))
}

fn publish_delta(
    runtime: &heterarchy_orchestration_runtime::OrchestrationRuntime,
    publisher: &BackendState,
    request_id: uuid::Uuid,
    workspace: uuid::Uuid,
    revision: &mut Option<u64>,
    operations: &mut std::collections::BTreeMap<uuid::Uuid, u64>,
) -> Result<(), RuntimeError> {
    if let Some(request) = runtime.agent_request(request_id)?
        && Some(request.revision) != *revision
    {
        publisher.publish_state_changed(
            RealtimeEventKind::AgentRequestStateChanged,
            request_id,
            request.revision,
            Some(workspace),
        );
        *revision = Some(request.revision);
    }
    for operation in runtime.agent_request_provision_operations(request_id)? {
        if operations.get(&operation.operation_id) != Some(&operation.revision) {
            publisher.publish_state_changed(
                RealtimeEventKind::ProvisioningStateChanged,
                operation.operation_id,
                operation.revision,
                Some(workspace),
            );
            operations.insert(operation.operation_id, operation.revision);
        }
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    #[tokio::test]
    async fn refusal_after_submit_never_claims_that_nothing_was_admitted()
    -> Result<(), Box<dyn std::error::Error>> {
        let response = AdmissionError::Incomplete(RuntimeError::Conflict).into_response();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = response.into_body().collect().await?.to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(value["code"], "COMMITTED_BUT_UNCONFIRMED");
        assert_eq!(value["retryable"], false);
        let response =
            AdmissionError::Api(ApiError::Runtime(RuntimeError::Conflict)).into_response();
        let body = response.into_body().collect().await?.to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(value["code"], "ORCHESTRATION_CONFLICT");
        Ok(())
    }
}
