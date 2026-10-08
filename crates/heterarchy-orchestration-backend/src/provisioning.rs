//! Axum adapter for the transport-neutral AgentRequest/provisioning
//! application layer.
//!
//! Mirrors the MCP `agent_request` tool's application flow (submit, then
//! reserve when still pending, then prepare effect targets) over the runtime
//! operations. No provider effect is ever executed or confirmed here: the
//! dispatch journal keeps releasing effects exclusively through
//! `next_agent_provision_dispatch`, and confirmations stay on the
//! confirmed-readback path. Confirmed participation uses the canonical Session,
//! shared member stages and exact source binding witnesses.

mod admission;
mod operations;
mod participation;
mod requests;

use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_operations::{AgentRequestEffectScope, AgentRequestIndependence};
use heterarchy_orchestration_runtime::provisioning::AgentRequestSubmit;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::BackendState;
use crate::RealtimeEventKind;
use crate::app_support::{LaneError, now_millis};

/// Provisioning routes; merge before the router takes its state.
pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .merge(admission::routes())
        .merge(operations::routes())
        .merge(participation::routes())
        .merge(requests::routes())
        .route("/internal/v1/agent-requests", post(submit))
        .route(
            "/internal/v1/agent-requests/{request_id}/resolution",
            get(resolution).post(reserve),
        )
        .route(
            "/internal/v1/agent-requests/{request_id}/provision-targets",
            post(prepare_targets).get(prepared_targets),
        )
        .route(
            "/internal/v1/agent-requests/{request_id}/reconcile",
            post(reconcile_fulfillment),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/agent-requests",
            get(list_requests),
        )
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RequestListQuery {
    limit: Option<u32>,
}

async fn list_requests(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Query(query): Query<RequestListQuery>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: axum::http::HeaderMap,
) -> Result<Json<RequestPageWire>, LaneError> {
    let session = crate::app_support::session_selector(&headers)?;
    let workspace = canonical_uuid(Some(workspace_id.as_str()))?;
    let limit = query.limit.unwrap_or(DEFAULT_REQUEST_LIST_LIMIT);
    let page = state
        .run_workspace_query(workspace, session, principal, move |runtime| {
            runtime.agent_requests(workspace, limit)
        })
        .await?;
    Ok(Json(RequestPageWire {
        items: page
            .items
            .iter()
            .map(|summary| RequestWire {
                request_id: summary.request_id,
                workspace_id: summary.workspace_id.clone(),
                activity_id: summary.activity_id.clone(),
                capability: summary.capability.clone(),
                count: summary.requested_count,
                state: summary.state,
                revision: summary.revision,
                created: false,
            })
            .collect(),
        truncated: page.truncated,
    }))
}
/// Default bounded page size, mirroring the team read tool.
const DEFAULT_REQUEST_LIST_LIMIT: u32 = 64;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RequestPageWire {
    items: Vec<RequestWire>,
    truncated: bool,
}

async fn reconcile_fulfillment(
    State(state): State<BackendState>,
    Path(request_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<RequestWire>, LaneError> {
    let request_id = canonical_uuid(Some(request_id.as_str()))?;
    let observed_at = now_millis()?;
    let publisher = state.clone();
    let summary = state
        .run_operator_mutation(principal, move |runtime| {
            let before = runtime
                .agent_request(request_id)?
                .ok_or(crate::RuntimeError::NotFound)?;
            let summary = runtime.reconcile_agent_request_fulfillment(request_id, observed_at)?;
            if summary.revision != before.revision {
                publisher.publish_state_changed(
                    RealtimeEventKind::AgentRequestStateChanged,
                    summary.request_id,
                    summary.revision,
                    Some(summary.workspace_id),
                );
            }
            Ok(summary)
        })
        .await?;
    Ok(Json(RequestWire {
        request_id: summary.request_id,
        workspace_id: summary.workspace_id.to_string(),
        activity_id: summary.activity_id.to_string(),
        capability: summary.capability.clone(),
        count: summary.count,
        state: summary.state,
        revision: summary.revision,
        created: false,
    }))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SubmitBody {
    workspace_id: String,
    activity_id: String,
    requester_agent_id: String,
    requester_session_id: String,
    coordinator_revision: u64,
    request_id: String,
    objective: String,
    capability: String,
    count: u8,
    effect_scope: String,
    independence: String,
    reason: String,
}

async fn submit(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<SubmitBody>,
) -> Result<Json<SubmitResponse>, LaneError> {
    let command = AgentRequestSubmit {
        workspace_id: canonical_uuid(Some(body.workspace_id.as_str()))?,
        activity_id: canonical_uuid(Some(body.activity_id.as_str()))?,
        requester_agent_id: canonical_uuid(Some(body.requester_agent_id.as_str()))?,
        requester_session_id: canonical_uuid(Some(body.requester_session_id.as_str()))?,
        coordinator_revision: body.coordinator_revision,
        request_id: canonical_uuid(Some(body.request_id.as_str()))?,
        objective: body.objective,
        capability: body.capability,
        count: body.count,
        effect_scope: effect_scope(&body.effect_scope)?,
        independence: independence(&body.independence)?,
        reason: body.reason,
        created_at_ms: now_millis()?,
    };
    let publisher = state.clone();
    let (summary, resolution) = state
        .run_agent_request_mutation(None, command, principal, move |runtime, command| {
            let mut summary = runtime.submit_agent_request(command)?;
            if summary.created {
                publisher.publish_state_changed(
                    RealtimeEventKind::AgentRequestStateChanged,
                    summary.request_id,
                    summary.revision,
                    Some(summary.workspace_id),
                );
            }
            let resolution = if summary.state
                == heterarchy_orchestration_runtime::provisioning::request_state_label(
                    heterarchy_orchestration_operations::AgentRequestState::ResolutionPending,
                ) {
                let observed = crate::current_timestamp_millis()
                    .map_err(|_| crate::RuntimeError::InvalidInput)?;
                Some(runtime.reserve_agent_request_resolution(
                    summary.request_id,
                    observed,
                    observed,
                )?)
            } else {
                Some(
                    runtime
                        .agent_request_resolution(summary.request_id)?
                        .ok_or(crate::RuntimeError::Persistence(
                            heterarchy_orchestration_operations::OperationsError::Corrupt(
                                "reserved AgentRequest is missing resolver rows",
                            ),
                        ))?,
                )
            };
            if let Some(resolution) = &resolution {
                if resolution.created {
                    publisher.publish_state_changed(
                        RealtimeEventKind::AgentRequestStateChanged,
                        resolution.request_id,
                        resolution.request_revision,
                        Some(resolution.workspace_id),
                    );
                }
                summary.state = resolution.request_state;
                summary.revision = resolution.request_revision;
            }
            Ok((summary, resolution))
        })
        .await?;
    Ok(Json(SubmitResponse::from_parts(
        &summary,
        resolution.as_ref(),
    )))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SubmitResponse {
    request: RequestWire,
    resolution: Option<ResolutionWire>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RequestWire {
    request_id: Uuid,
    workspace_id: String,
    activity_id: String,
    capability: String,
    count: u8,
    state: &'static str,
    revision: u64,
    created: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ResolutionWire {
    request_id: Uuid,
    workspace_id: Uuid,
    request_state: &'static str,
    request_revision: u64,
    created: bool,
    slots: Vec<SlotWire>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SlotWire {
    slot_ordinal: u8,
    kind: &'static str,
    claim_id: Option<Uuid>,
    profile_id: Option<Uuid>,
    runtime_session_id: Option<Uuid>,
    runtime_session_revision: Option<i64>,
}

impl SubmitResponse {
    fn from_parts(
        summary: &heterarchy_orchestration_runtime::provisioning::AgentRequestSubmitSummary,
        resolution: Option<
            &heterarchy_orchestration_runtime::provisioning::AgentRequestResolutionSummary,
        >,
    ) -> Self {
        Self {
            request: RequestWire {
                request_id: summary.request_id,
                workspace_id: summary.workspace_id.to_string(),
                activity_id: summary.activity_id.to_string(),
                capability: summary.capability.clone(),
                count: summary.count,
                state: summary.state,
                revision: summary.revision,
                created: summary.created,
            },
            resolution: resolution.map(ResolutionWire::from_summary),
        }
    }
}

impl ResolutionWire {
    fn from_summary(
        summary: &heterarchy_orchestration_runtime::provisioning::AgentRequestResolutionSummary,
    ) -> Self {
        Self {
            request_id: summary.request_id,
            workspace_id: summary.workspace_id,
            request_state: summary.request_state,
            request_revision: summary.request_revision,
            created: summary.created,
            slots: summary
                .slots
                .iter()
                .map(|slot| SlotWire {
                    slot_ordinal: slot.slot_ordinal,
                    kind: slot.kind,
                    claim_id: slot.claim_id,
                    profile_id: slot.profile_id,
                    runtime_session_id: slot.runtime_session_id,
                    runtime_session_revision: slot.runtime_session_revision,
                })
                .collect(),
        }
    }
}

async fn resolution(
    State(state): State<BackendState>,
    Path(request_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Option<ResolutionWire>>, LaneError> {
    let session = crate::app_support::session_selector(&headers)?;
    let workspace = crate::app_support::workspace_selector(&headers)?;
    let request_id = canonical_uuid(Some(request_id.as_str()))?;
    let summary = state
        .run_resource_query(workspace, session, principal, move |runtime| {
            let Some(request) = runtime.agent_request(request_id)? else {
                return Ok(None);
            };
            let scope = Uuid::parse_str(&request.workspace_id).map_err(|_| {
                heterarchy_orchestration_runtime::RuntimeError::Persistence(
                    heterarchy_orchestration_operations::OperationsError::Corrupt(
                        "request Workspace identity",
                    ),
                )
            })?;
            Ok(runtime
                .agent_request_resolution(request_id)?
                .map(|resolution| (scope, resolution)))
        })
        .await?;
    Ok(Json(summary.as_ref().map(ResolutionWire::from_summary)))
}

async fn reserve(
    State(state): State<BackendState>,
    Path(request_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<ResolutionWire>, LaneError> {
    let request_id = canonical_uuid(Some(request_id.as_str()))?;
    let claimed_at = now_millis()?;
    let reserved_at = now_millis()?;
    let publisher = state.clone();
    let summary = state
        .run_operator_mutation(principal, move |runtime| {
            let summary =
                runtime.reserve_agent_request_resolution(request_id, claimed_at, reserved_at)?;
            if summary.created {
                publisher.publish_state_changed(
                    RealtimeEventKind::AgentRequestStateChanged,
                    summary.request_id,
                    summary.request_revision,
                    Some(summary.workspace_id),
                );
            }
            Ok(summary)
        })
        .await?;
    Ok(Json(ResolutionWire::from_summary(&summary)))
}

async fn prepare_targets(
    State(state): State<BackendState>,
    Path(request_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<Vec<ProvisionOperationWire>>, LaneError> {
    let request_id = canonical_uuid(Some(request_id.as_str()))?;
    let prepared_at = now_millis()?;
    let bound_at = now_millis()?;
    let publisher = state.clone();
    let operations = state
        .run_operator_mutation(principal, move |runtime| {
            let request = runtime
                .agent_request(request_id)?
                .ok_or(crate::RuntimeError::NotFound)?;
            let workspace_id = Uuid::parse_str(&request.workspace_id).map_err(|_| {
                crate::RuntimeError::Persistence(
                    heterarchy_orchestration_operations::OperationsError::Corrupt(
                        "request Workspace identity",
                    ),
                )
            })?;
            let before = runtime.agent_request_provision_operations(request_id)?;
            let result =
                runtime.prepare_agent_request_effect_targets(request_id, prepared_at, bound_at);
            // A later slot can fail after earlier commits. Read those exact
            // owner records and publish actual deltas before returning the error.
            let after = runtime.agent_request_provision_operations(request_id)?;
            for operation in &after {
                let previous = before
                    .iter()
                    .find(|old| old.operation_id == operation.operation_id);
                if previous.is_none_or(|old| old.revision != operation.revision) {
                    publisher.publish_state_changed(
                        RealtimeEventKind::ProvisioningStateChanged,
                        operation.operation_id,
                        operation.revision,
                        Some(workspace_id),
                    );
                }
            }
            result.map_err(crate::RuntimeError::from)
        })
        .await?;
    Ok(Json(
        operations
            .iter()
            .map(ProvisionOperationWire::from_summary)
            .collect(),
    ))
}

async fn prepared_targets(
    State(state): State<BackendState>,
    Path(request_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Vec<ProvisionOperationWire>>, LaneError> {
    let request_id = canonical_uuid(Some(request_id.as_str()))?;
    let workspace = crate::app_support::workspace_selector(&headers)?;
    let session = crate::app_support::session_selector(&headers)?;
    let operations = state
        .run_resource_query(workspace, session, principal, move |runtime| {
            let Some(request) = runtime.agent_request(request_id)? else {
                return Ok(None);
            };
            let scope = Uuid::parse_str(&request.workspace_id).map_err(|_| {
                crate::RuntimeError::Persistence(
                    heterarchy_orchestration_operations::OperationsError::Corrupt(
                        "request Workspace identity",
                    ),
                )
            })?;
            Ok(Some((
                scope,
                runtime.agent_request_provision_operations(request_id)?,
            )))
        })
        .await?
        .ok_or(crate::ApiError::NotFound)?;
    Ok(Json(
        operations
            .iter()
            .map(ProvisionOperationWire::from_summary)
            .collect(),
    ))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProvisionOperationWire {
    operation_id: Uuid,
    slot_ordinal: u8,
    state: &'static str,
    profile_id: Option<Uuid>,
    runtime_session_id: Option<Uuid>,
    created: bool,
    revision: u64,
    changed: bool,
}

impl ProvisionOperationWire {
    fn from_summary(
        operation: &heterarchy_orchestration_runtime::provisioning::ProvisionOperationSummary,
    ) -> Self {
        Self {
            operation_id: operation.operation_id,
            slot_ordinal: operation.slot_ordinal,
            state: operation.state,
            profile_id: operation.profile_id,
            runtime_session_id: operation.runtime_session_id,
            created: operation.created,
            revision: operation.revision,
            changed: operation.changed,
        }
    }
}

fn canonical_uuid(value: Option<&str>) -> Result<Uuid, LaneError> {
    let Some(value) = value else {
        return Err(LaneError::InvalidIdentity);
    };
    let parsed = Uuid::parse_str(value).map_err(|_| LaneError::InvalidIdentity)?;
    if parsed.is_nil() || parsed.as_hyphenated().to_string() != value {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(parsed)
}

fn effect_scope(value: &str) -> Result<AgentRequestEffectScope, LaneError> {
    match value {
        "read_only" => Ok(AgentRequestEffectScope::ReadOnly),
        "source_write" => Ok(AgentRequestEffectScope::SourceWrite),
        _ => Err(LaneError::InvalidIdentity),
    }
}

fn independence(value: &str) -> Result<AgentRequestIndependence, LaneError> {
    match value {
        "none" => Ok(AgentRequestIndependence::None),
        "independent_from_current_writer" => {
            Ok(AgentRequestIndependence::IndependentFromCurrentWriter)
        }
        _ => Err(LaneError::InvalidIdentity),
    }
}
