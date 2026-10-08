//! Scoped approval mutations and exact historical readback over existing owners.

use axum::extract::{Path, State};
use axum::routing::post;
use axum::{Extension, Json, Router};
use forge_domain::{ApprovalDecision, AuthenticatedPrincipal, ExecutionPermissions};
use heterarchy_orchestration_operations::{DecideApprovalCommand, SubmitApprovalCommand};
use heterarchy_orchestration_runtime::RuntimeError;
use heterarchy_orchestration_runtime::approval::{AdmissionSummary, ApprovalDecisionSummary};
use uuid::Uuid;

use crate::app_support::{LaneError, validate_workspace_reference};
use crate::catalog_execution::CatalogLane;
use crate::wire::WorkspaceQueryScope;
use crate::wire::approval::{
    ApprovalAdmissionWire, ApprovalDecideRequest, ApprovalDecideResponse,
    ApprovalRequestGetRequest, ApprovalRequestGetResponse, ApprovalSubmitRequest,
    ApprovalSubmitResponse,
};
use crate::{BackendState, RealtimeEventKind, ScopedMutationAuthority};

pub(super) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/approval-requests/submit",
            post(submit),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/approval-requests/decision",
            post(decide),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/approval-requests/get",
            post(get),
        )
}

async fn submit(
    State(state): State<BackendState>,
    Path(workspace): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ApprovalSubmitRequest>,
) -> Result<Json<ApprovalSubmitResponse>, LaneError> {
    super::scoped::validate_scope(&workspace, &body.scope, Some(body.session_id))?;
    if body.request_id.is_nil()
        || !canonical_digest(&body.request_digest)
        || !(1000..=86_400_000).contains(&body.ttl_ms)
    {
        return Err(LaneError::InvalidIdentity);
    }
    let action = super::action_class(&body.action)?;
    let digest = super::request_digest(&body.request_digest)?;
    let scope = body.scope.clone();
    let workspace_id = scope.workspace_id;
    let publisher = state.clone();
    let submission = state
        .run_workspace_reference_mutation(
            body.scope,
            body.session_id,
            principal,
            ScopedMutationAuthority::WorkspaceMember,
            CatalogLane::Ordinary,
            move |runtime, now| {
                let submission = runtime.submit_approval(SubmitApprovalCommand {
                    request_id: forge_domain::ApprovalRequestId::from_uuid(body.request_id),
                    workspace_id: forge_domain::WorkspaceId::from_uuid(workspace_id),
                    action,
                    request_digest: digest,
                    principal,
                    session_id: body.session_id,
                    requested_permissions: ExecutionPermissions::new(
                        body.requested_permissions.workspace_read,
                        body.requested_permissions.workspace_write,
                        body.requested_permissions.network,
                    ),
                    ttl_ms: body.ttl_ms,
                    now_ms: now,
                })?;
                if !submission.replay {
                    publisher.publish_state_changed(
                        RealtimeEventKind::ApprovalRequestChanged,
                        submission.request.request_id,
                        submission.request.revision,
                        Some(submission.request.workspace_id),
                    );
                }
                Ok(submission)
            },
        )
        .await?;
    Ok(Json(ApprovalSubmitResponse {
        scope,
        request: super::scoped::request_wire(&submission.request),
        admission: submission.admission.as_ref().map(admission_wire),
        replay: submission.replay,
    }))
}

async fn decide(
    State(state): State<BackendState>,
    Path(workspace): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ApprovalDecideRequest>,
) -> Result<Json<ApprovalDecideResponse>, LaneError> {
    super::scoped::validate_scope(&workspace, &body.scope, Some(body.session_id))?;
    let outcome = decide_in_scope(
        &state,
        body.scope.clone(),
        body.session_id,
        body.request_id,
        &body.decision,
        principal,
    )
    .await?;
    Ok(Json(ApprovalDecideResponse {
        scope: body.scope,
        request: super::scoped::request_wire(&outcome.request),
        admission: outcome.admission.as_ref().map(admission_wire),
    }))
}

pub(super) async fn decide_in_scope(
    state: &BackendState,
    scope: WorkspaceQueryScope,
    session: Uuid,
    request: Uuid,
    decision: &str,
    principal: AuthenticatedPrincipal,
) -> Result<ApprovalDecisionSummary, LaneError> {
    validate_workspace_reference(&scope, Some(session))?;
    if request.is_nil() {
        return Err(LaneError::InvalidIdentity);
    }
    let decision = match decision {
        "approve" => ApprovalDecision::Approve,
        "reject" => ApprovalDecision::Reject,
        "cancel" => ApprovalDecision::Cancel,
        _ => return Err(LaneError::InvalidIdentity),
    };
    let workspace = scope.workspace_id;
    let publisher = state.clone();
    Ok(state
        .run_workspace_reference_mutation(
            scope,
            session,
            principal,
            ScopedMutationAuthority::Operator,
            CatalogLane::Control,
            move |runtime, now| {
                let outcome = runtime.decide_approval(DecideApprovalCommand {
                    request_id: forge_domain::ApprovalRequestId::from_uuid(request),
                    workspace_id: forge_domain::WorkspaceId::from_uuid(workspace),
                    decision,
                    decided_by: session,
                    now_ms: now,
                })?;
                publisher.publish_state_changed(
                    RealtimeEventKind::ApprovalDecisionChanged,
                    outcome.request.request_id,
                    outcome.request.revision,
                    Some(outcome.request.workspace_id),
                );
                Ok(outcome)
            },
        )
        .await?)
}

async fn get(
    State(state): State<BackendState>,
    Path(workspace): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<ApprovalRequestGetRequest>,
) -> Result<Json<ApprovalRequestGetResponse>, LaneError> {
    super::scoped::validate_scope(&workspace, &body.scope, body.session_id)?;
    if body.request_id.is_nil() {
        return Err(LaneError::InvalidIdentity);
    }
    let workspace = body.scope.workspace_id;
    let snapshot = state
        .run_workspace_reference_query(
            body.scope.clone(),
            body.session_id,
            principal,
            move |runtime, _| {
                let snapshot = runtime.approval_request_snapshot(body.request_id)?;
                if snapshot
                    .as_ref()
                    .is_some_and(|value| value.request.workspace_id != workspace)
                {
                    return Err(RuntimeError::Forbidden);
                }
                Ok(snapshot)
            },
        )
        .await?;
    Ok(Json(ApprovalRequestGetResponse {
        scope: body.scope,
        request: snapshot
            .as_ref()
            .map(|value| super::scoped::request_wire(&value.request)),
        admission: snapshot
            .as_ref()
            .and_then(|value| value.admission.as_ref())
            .map(admission_wire),
    }))
}

fn admission_wire(value: &AdmissionSummary) -> ApprovalAdmissionWire {
    ApprovalAdmissionWire {
        admission_id: value.admission_id,
        request_id: value.request_id,
        workspace_id: value.workspace_id,
        issuer: value.issuer.clone(),
        backend_id: value.backend_id.clone(),
        issued_at_ms: value.issued_at_ms,
        expires_at_ms: value.expires_at_ms,
    }
}

fn canonical_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
