//! Scoped statement commands retaining journal-first replay admission.

use axum::extract::{Extension, Path, State};
use axum::routing::post;
use axum::{Json, Router};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_operations::LeadershipStatementKind;
use heterarchy_orchestration_runtime::leadership::{
    LeadershipStatementReceiptSummary, StatementCommand,
};
use uuid::Uuid;

use crate::app_support::{LaneError, validate_workspace_reference};
use crate::wire::WorkspaceQueryScope;
use crate::wire::leadership_reviews::{
    LeadershipReviewPhaseWire, LeadershipReviewStatementKindWire,
};
use crate::wire::leadership_statements::{
    LeadershipStatementAppendRequest, LeadershipStatementAppendResponse,
    LeadershipStatementQueryRequest, LeadershipStatementQueryResponse,
    LeadershipStatementReadbackWire, LeadershipStatementReceiptWire,
};
use crate::{ApiError, BackendState, RuntimeError, ScopedMutationAuthority};

pub(super) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/leadership/statements/append",
            post(append),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/leadership/statements/query",
            post(query),
        )
}

pub(super) async fn append(
    State(state): State<BackendState>,
    Path(workspace): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<LeadershipStatementAppendRequest>,
) -> Result<Json<LeadershipStatementAppendResponse>, LaneError> {
    validate_scope(&workspace, &body.scope, Some(body.session_id))?;
    if [
        body.activity_id,
        body.election_id,
        body.request_id,
        body.message_id,
    ]
    .iter()
    .any(Uuid::is_nil)
        || body.body.trim().is_empty()
        || body.body.len() > 16 * 1024
    {
        return Err(LaneError::InvalidIdentity);
    }
    let scope = body.scope;
    let command = StatementCommand {
        workspace_id: scope.workspace_id,
        activity_id: body.activity_id,
        session_id: body.session_id,
        election_id: body.election_id,
        request_id: body.request_id,
        message_id: body.message_id,
        kind: match body.kind {
            LeadershipReviewStatementKindWire::Defense => LeadershipStatementKind::Defense,
            LeadershipReviewStatementKindWire::Discussion => LeadershipStatementKind::Discussion,
        },
        body: body.body,
        now_ms: crate::current_timestamp_millis()?,
    };
    let receipt = state
        .append_scoped_statement(scope.clone(), principal, command)
        .await?;
    Ok(Json(LeadershipStatementAppendResponse {
        scope,
        receipt: receipt_wire(&receipt)?,
    }))
}

async fn query(
    State(state): State<BackendState>,
    Path(workspace): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<LeadershipStatementQueryRequest>,
) -> Result<Json<LeadershipStatementQueryResponse>, LaneError> {
    validate_scope(&workspace, &body.scope, body.session_id)?;
    if body.message_id.is_nil() {
        return Err(LaneError::InvalidIdentity);
    }
    let workspace_id = body.scope.workspace_id;
    let message_id = body.message_id;
    let record = state
        .run_workspace_reference_query(
            body.scope.clone(),
            body.session_id,
            principal,
            move |runtime, _| runtime.leadership_statement(workspace_id, message_id),
        )
        .await?;
    Ok(Json(LeadershipStatementQueryResponse {
        scope: body.scope,
        record: record
            .as_ref()
            .map(|record| LeadershipStatementReadbackWire {
                workspace_id: record.workspace_id().into_uuid(),
                request_id: record.request_id(),
                statement: super::reviews::statement_wire(record.statement()),
            }),
    }))
}

impl BackendState {
    async fn append_scoped_statement(
        &self,
        scope: WorkspaceQueryScope,
        principal: AuthenticatedPrincipal,
        command: StatementCommand,
    ) -> Result<LeadershipStatementReceiptSummary, ApiError> {
        let state = self.clone();
        self.run_owned(async move {
            let workspace = scope.workspace_id;
            let session = command.session_id;
            state
                .catalog_job(move |runtime| {
                    ScopedMutationAuthority::WorkspaceMember
                        .authorize(runtime, workspace, session, principal)
                })
                .await?;
            state.workspace_scope_proof(scope.clone()).await?;
            let probe = command.clone();
            let replay = state
                .catalog_job(move |runtime| runtime.leadership_statement_replay_exists(&probe))
                .await?;
            if !replay {
                state
                    .activity_scope_proof(scope.clone(), command.activity_id)
                    .await?;
            }
            state
                .runtime
                .ensure_broker_owner()
                .await
                .map_err(RuntimeError::BrokerOwner)
                .map_err(ApiError::Runtime)?;
            state.workspace_scope_proof(scope.clone()).await?;
            if !replay {
                state
                    .activity_scope_proof(scope, command.activity_id)
                    .await?;
            }
            state
                .catalog_job(move |runtime| {
                    ScopedMutationAuthority::WorkspaceMember
                        .authorize(runtime, workspace, session, principal)?;
                    let mut current = command;
                    current.now_ms = crate::current_timestamp_millis()
                        .map_err(|_| RuntimeError::InvalidInput)?;
                    runtime.append_leadership_statement(current)
                })
                .await
        })
        .await
    }
}

fn validate_scope(
    path: &str,
    scope: &WorkspaceQueryScope,
    session: Option<Uuid>,
) -> Result<(), LaneError> {
    validate_workspace_reference(scope, session)?;
    let parsed = Uuid::parse_str(path).map_err(|_| LaneError::InvalidIdentity)?;
    if parsed.is_nil() || parsed.to_string() != path || parsed != scope.workspace_id {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(())
}

fn receipt_wire(
    receipt: &LeadershipStatementReceiptSummary,
) -> Result<LeadershipStatementReceiptWire, LaneError> {
    let phase = match receipt.phase {
        "defense" => LeadershipReviewPhaseWire::Defense,
        "discussion" => LeadershipReviewPhaseWire::Discussion,
        "ballot" => LeadershipReviewPhaseWire::Ballot,
        "awaiting_decision" => LeadershipReviewPhaseWire::AwaitingDecision,
        "resolved" => LeadershipReviewPhaseWire::Resolved,
        _ => {
            return Err(RuntimeError::Persistence(
                heterarchy_orchestration_operations::OperationsError::Corrupt(
                    "statement receipt phase",
                ),
            )
            .into());
        }
    };
    Ok(LeadershipStatementReceiptWire {
        message_id: receipt.message_id,
        election_id: receipt.election_id,
        sequence: receipt.sequence,
        phase,
        replay: receipt.replay,
    })
}
