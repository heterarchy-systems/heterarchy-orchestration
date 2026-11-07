//! Authenticated Session-bound Reviewer participation statements.
//!
//! The routes persist and read statements through the caller's Profile-bound
//! Session. They do not prove an independent human Agent, publish qualification,
//! assign a case, or grant evidence access.

use axum::extract::{Path, State};
use axum::routing::post;
use axum::{Extension, Json, Router};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_runtime::{
    OrchestrationRuntime, RuntimeError, participation::ParticipationTarget,
    reviewer_participation::ReviewerParticipationTarget,
};
use uuid::Uuid;

use crate::wire::reviewer_participation::{
    ReviewerParticipationHistoryRequest, ReviewerParticipationHistoryResponse,
    ReviewerParticipationMutationRequest, ReviewerParticipationMutationResponse,
    ReviewerParticipationReceiptWire, ReviewerParticipationRequestReadRequest,
    ReviewerParticipationRequestReadResponse, ReviewerParticipationStatementWire,
    ReviewerParticipationTargetWire,
};
use crate::{ApiError, BackendState};

pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/reviewer-participation/statements",
            post(record_statement),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/reviewer-participation/request",
            post(read_request),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/reviewer-participation/history",
            post(read_history),
        )
}

async fn record_statement(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<ReviewerParticipationMutationRequest>,
) -> Result<Json<ReviewerParticipationMutationResponse>, ApiError> {
    let workspace = validate_target(&workspace_path, &request.target)?;
    if request.request_id.is_nil() {
        return Err(ApiError::InvalidIdentity);
    }
    let target = runtime_target(&request.target);
    let request_id = request.request_id;
    let expected_revision = request.expected_revision;
    let statement = runtime_statement(request.statement);
    let receipt =
        run_participant_mutation(state, request.target.clone(), principal, move |runtime| {
            runtime.record_reviewer_participation(
                target,
                request_id,
                expected_revision,
                statement,
                principal,
            )
        })
        .await?;
    if receipt.workspace_id().into_uuid() != workspace
        || receipt.request_id() != request.request_id
        || receipt.participant_id().is_nil()
        || receipt.profile_id().is_nil()
        || receipt.participation_id().into_uuid().is_nil()
    {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    Ok(Json(ReviewerParticipationMutationResponse {
        target: request.target,
        receipt: receipt_wire(&receipt),
    }))
}

async fn read_request(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<ReviewerParticipationRequestReadRequest>,
) -> Result<Json<ReviewerParticipationRequestReadResponse>, ApiError> {
    let workspace = validate_target(&workspace_path, &request.target)?;
    if request.request_id.is_nil() {
        return Err(ApiError::InvalidIdentity);
    }
    let target = runtime_target(&request.target);
    let request_id = request.request_id;
    let receipt = run_participant_query(state, request.target.clone(), principal, move |runtime| {
        runtime.reviewer_participation_request(target, request_id, principal)
    })
    .await?;
    let receipt = receipt
        .as_ref()
        .map(|receipt| {
            let wire = receipt_wire(receipt);
            if wire.workspace_id != workspace
                || wire.request_id != request.request_id
                || wire.profile_id.is_nil()
                || wire.participation_id.is_nil()
            {
                return Err(ApiError::Runtime(RuntimeError::Conflict));
            }
            Ok(wire)
        })
        .transpose()?;
    Ok(Json(ReviewerParticipationRequestReadResponse {
        target: request.target,
        receipt,
    }))
}

async fn read_history(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<ReviewerParticipationHistoryRequest>,
) -> Result<Json<ReviewerParticipationHistoryResponse>, ApiError> {
    validate_target(&workspace_path, &request.target)?;
    if request.limit == 0 || request.limit > 64 || request.after_revision == Some(0) {
        return Err(ApiError::InvalidIdentity);
    }
    let target = runtime_target(&request.target);
    let after_revision = request.after_revision;
    let limit = request.limit;
    let page = run_participant_query(state, request.target.clone(), principal, move |runtime| {
        runtime.reviewer_participation_history(target, after_revision, limit, principal)
    })
    .await?;
    let records = page.records().iter().map(receipt_wire).collect();
    Ok(Json(ReviewerParticipationHistoryResponse {
        target: request.target,
        records,
        next_after_revision: page.next_after_revision(),
    }))
}

async fn run_participant_mutation<T, F>(
    state: BackendState,
    target: ReviewerParticipationTargetWire,
    principal: AuthenticatedPrincipal,
    operation: F,
) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce(&OrchestrationRuntime) -> Result<T, RuntimeError> + Send + 'static,
{
    let participation_target = ParticipationTarget {
        workspace_id: target.scope.workspace_id,
        activity_id: target.activity_id,
        agent_id: target.agent_id,
        session_id: target.session_id,
    };
    let workspace = target.scope.workspace_id;
    let session = target.session_id;
    let scope = target.scope.clone();
    let activity = target.activity_id;
    state
        .clone()
        .run_owned(async move {
            state
                .catalog_job(move |runtime| {
                    runtime.require_participation(participation_target, principal)
                })
                .await?;
            state.activity_scope_proof(scope.clone(), activity).await?;
            state
                .runtime
                .ensure_broker_owner()
                .await
                .map_err(RuntimeError::BrokerOwner)
                .map_err(ApiError::Runtime)?;
            state.activity_scope_proof(scope, activity).await?;
            state
                .catalog_job(move |runtime| {
                    runtime.require_open_session(session, principal)?;
                    runtime.require_workspace_read(workspace, Some(session), principal)?;
                    operation(runtime)
                })
                .await
        })
        .await
}

async fn run_participant_query<T, F>(
    state: BackendState,
    target: ReviewerParticipationTargetWire,
    principal: AuthenticatedPrincipal,
    operation: F,
) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce(&OrchestrationRuntime) -> Result<T, RuntimeError> + Send + 'static,
{
    let participation_target = ParticipationTarget {
        workspace_id: target.scope.workspace_id,
        activity_id: target.activity_id,
        agent_id: target.agent_id,
        session_id: target.session_id,
    };
    let workspace = target.scope.workspace_id;
    let session = target.session_id;
    let scope = target.scope.clone();
    let activity = target.activity_id;
    state
        .clone()
        .run_owned(async move {
            state
                .catalog_job(move |runtime| {
                    runtime.require_participation(participation_target, principal)
                })
                .await?;
            state.activity_scope_proof(scope, activity).await?;
            state
                .catalog_job(move |runtime| {
                    runtime.require_open_session(session, principal)?;
                    runtime.require_workspace_read(workspace, Some(session), principal)?;
                    operation(runtime)
                })
                .await
        })
        .await
}

fn validate_target(path: &str, target: &ReviewerParticipationTargetWire) -> Result<Uuid, ApiError> {
    crate::app_support::validate_workspace_reference(&target.scope, Some(target.session_id))?;
    let path_workspace = Uuid::parse_str(path).map_err(|_| ApiError::InvalidIdentity)?;
    if path_workspace.is_nil()
        || path_workspace.as_hyphenated().to_string() != path
        || path_workspace != target.scope.workspace_id
        || target.activity_id.is_nil()
        || target.agent_id.is_nil()
    {
        return Err(ApiError::InvalidIdentity);
    }
    Ok(path_workspace)
}

fn runtime_target(target: &ReviewerParticipationTargetWire) -> ReviewerParticipationTarget {
    ReviewerParticipationTarget {
        workspace_id: target.scope.workspace_id,
        activity_id: target.activity_id,
        agent_id: target.agent_id,
        session_id: target.session_id,
    }
}

fn runtime_statement(
    statement: ReviewerParticipationStatementWire,
) -> heterarchy_orchestration_operations::reviewer_participation::ReviewerParticipationStatement {
    use heterarchy_orchestration_operations::reviewer_participation::ReviewerParticipationStatement;
    match statement {
        ReviewerParticipationStatementWire::Interested => {
            ReviewerParticipationStatement::Interested
        }
        ReviewerParticipationStatementWire::WithdrawInterest {
            withdraws_request_id,
            withdraws_revision,
        } => ReviewerParticipationStatement::WithdrawInterest {
            withdraws_request_id,
            withdraws_revision,
        },
    }
}

fn receipt_wire(
    receipt: &heterarchy_orchestration_operations::reviewer_participation::ReviewerParticipationReceipt,
) -> ReviewerParticipationReceiptWire {
    ReviewerParticipationReceiptWire {
        workspace_id: receipt.workspace_id().into_uuid(),
        participation_id: receipt.participation_id().into_uuid(),
        profile_id: receipt.profile_id(),
        participant_id: receipt.participant_id(),
        request_id: receipt.request_id(),
        revision: receipt.revision(),
        expected_revision: receipt.expected_revision(),
        statement: match receipt.statement() {
            heterarchy_orchestration_operations::reviewer_participation::ReviewerParticipationStatement::Interested => {
                ReviewerParticipationStatementWire::Interested
            }
            heterarchy_orchestration_operations::reviewer_participation::ReviewerParticipationStatement::WithdrawInterest {
                withdraws_request_id,
                withdraws_revision,
            } => ReviewerParticipationStatementWire::WithdrawInterest {
                withdraws_request_id,
                withdraws_revision,
            },
        },
        activity_id: receipt.activity_id().into_uuid(),
        agent_id: receipt.agent_id().into_uuid(),
        session_id: receipt.session_id().into_uuid(),
        display_name: receipt.display_name().to_owned(),
        recorded_at_ms: receipt.recorded_at().get(),
        replayed: receipt.replayed(),
    }
}
