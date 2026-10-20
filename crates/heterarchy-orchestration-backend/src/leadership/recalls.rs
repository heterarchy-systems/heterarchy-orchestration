//! Source-fenced recall requests over the existing governance owner.

use axum::extract::{Extension, Path, State};
use axum::routing::post;
use axum::{Json, Router};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_operations::OperationsError;
use heterarchy_orchestration_runtime::leadership::{
    LeadershipReviewReceiptSummary, LeadershipTarget, RecallReviewCommand,
};
use uuid::Uuid;

use crate::app_support::{LaneError, validate_workspace_reference};
use crate::wire::WorkspaceQueryScope;
use crate::wire::leadership_recalls::{
    LeadershipRecallReceiptWire, LeadershipRecallStartRequest, LeadershipRecallStartResponse,
    LeadershipRecallTargetWire,
};
use crate::wire::leadership_reviews::LeadershipReviewPhaseWire;
use crate::{ApiError, BackendState, RealtimeEventKind, RuntimeError, ScopedMutationAuthority};

pub(super) fn routes() -> Router<BackendState> {
    Router::new().route(
        "/internal/v1/workspaces/{workspace_id}/leadership/recalls/start",
        post(start),
    )
}

pub(super) async fn start(
    State(state): State<BackendState>,
    Path(workspace): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<LeadershipRecallStartRequest>,
) -> Result<Json<LeadershipRecallStartResponse>, LaneError> {
    validate_request(&workspace, &body)?;
    let target = recall_target(body.target, body.team_id)?;
    let scope = body.scope;
    let command = RecallReviewCommand {
        workspace_id: scope.workspace_id,
        activity_id: body.activity_id,
        session_id: body.session_id,
        target,
        expected_revision: body.expected_leadership_revision,
        request_id: body.request_id,
        reason: body.reason,
        now_ms: crate::current_timestamp_millis()?,
    };
    let receipt = state
        .start_scoped_recall(scope.clone(), principal, command)
        .await?;
    Ok(Json(LeadershipRecallStartResponse {
        scope,
        activity_id: body.activity_id,
        session_id: body.session_id,
        target: body.target,
        team_id: body.team_id,
        receipt: receipt_wire(&receipt)?,
    }))
}

impl BackendState {
    async fn start_scoped_recall(
        &self,
        scope: WorkspaceQueryScope,
        principal: AuthenticatedPrincipal,
        command: RecallReviewCommand,
    ) -> Result<LeadershipReviewReceiptSummary, ApiError> {
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
                .catalog_job(move |runtime| runtime.leadership_review_replay_exists(&probe))
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
            let publisher = state.clone();
            state
                .catalog_job(move |runtime| {
                    ScopedMutationAuthority::WorkspaceMember
                        .authorize(runtime, workspace, session, principal)?;
                    let mut current = command;
                    current.now_ms = crate::current_timestamp_millis()
                        .map_err(|_| RuntimeError::InvalidInput)?;
                    let receipt = runtime.request_leadership_review(current)?;
                    if !receipt.replay {
                        let revision = u64::try_from(receipt.leadership_revision)
                            .ok()
                            .filter(|revision| *revision > 0)
                            .ok_or(RuntimeError::persistence(OperationsError::Corrupt(
                                "recall leadership revision",
                            )))?;
                        publisher.publish_state_changed(
                            RealtimeEventKind::LeadershipChanged,
                            receipt.election_id,
                            revision,
                            Some(workspace),
                        );
                    }
                    Ok(receipt)
                })
                .await
        })
        .await
    }
}

fn validate_request(path: &str, body: &LeadershipRecallStartRequest) -> Result<(), LaneError> {
    validate_workspace_reference(&body.scope, Some(body.session_id))?;
    let workspace = Uuid::parse_str(path).map_err(|_| LaneError::InvalidIdentity)?;
    if workspace.is_nil()
        || workspace.to_string() != path
        || workspace != body.scope.workspace_id
        || body.activity_id.is_nil()
        || body.request_id.is_nil()
        || body.expected_leadership_revision < 1
        || body.reason.trim().is_empty()
        || body.reason.len() > 4_096
    {
        return Err(LaneError::InvalidIdentity);
    }
    recall_target(body.target, body.team_id)?;
    Ok(())
}

fn recall_target(
    target: LeadershipRecallTargetWire,
    team_id: Option<Uuid>,
) -> Result<LeadershipTarget, LaneError> {
    match (target, team_id) {
        (LeadershipRecallTargetWire::Workspace, None) => Ok(LeadershipTarget::Workspace),
        (LeadershipRecallTargetWire::Activity, None) => Ok(LeadershipTarget::Activity),
        (LeadershipRecallTargetWire::Team, Some(team)) if !team.is_nil() => {
            Ok(LeadershipTarget::Team(team))
        }
        _ => Err(LaneError::InvalidIdentity),
    }
}

fn receipt_wire(
    receipt: &LeadershipReviewReceiptSummary,
) -> Result<LeadershipRecallReceiptWire, LaneError> {
    let phase = match receipt.phase {
        "defense" => LeadershipReviewPhaseWire::Defense,
        "discussion" => LeadershipReviewPhaseWire::Discussion,
        "ballot" => LeadershipReviewPhaseWire::Ballot,
        "awaiting_decision" => LeadershipReviewPhaseWire::AwaitingDecision,
        "resolved" => LeadershipReviewPhaseWire::Resolved,
        _ => {
            return Err(RuntimeError::persistence(OperationsError::Corrupt(
                "recall receipt phase",
            ))
            .into());
        }
    };
    Ok(LeadershipRecallReceiptWire {
        request_id: receipt.request_id,
        election_id: receipt.election_id,
        leadership_revision: receipt.leadership_revision,
        electorate_size: receipt.electorate_size,
        phase,
        defense_deadline_ms: receipt.defense_deadline_ms,
        discussion_deadline_ms: receipt.discussion_deadline_ms,
        ballot_deadline_ms: receipt.ballot_deadline_ms,
        replay: receipt.replay,
        candidate_is_caller: true,
    })
}
