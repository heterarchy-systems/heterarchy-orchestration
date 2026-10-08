//! Source-fenced leadership ballot cast and observation routes.

use axum::extract::{Extension, Path, State};
use axum::routing::post;
use axum::{Json, Router};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_operations::{BallotChoice, LeadershipBallotSnapshot};
use heterarchy_orchestration_runtime::leadership::BallotCommand;
use uuid::Uuid;

use crate::app_support::{LaneError, validate_workspace_reference};
use crate::wire::WorkspaceQueryScope;
use crate::wire::leadership_ballots::{
    LeadershipBallotCastRequest, LeadershipBallotCastResponse, LeadershipBallotChoice,
    LeadershipBallotQueryRequest, LeadershipBallotQueryResponse, LeadershipBallotReceiptWire,
    LeadershipBallotWire,
};
use crate::{ApiError, BackendState, RuntimeError, ScopedMutationAuthority};

pub(super) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/leadership/ballots/cast",
            post(cast),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/leadership/ballots/query",
            post(query),
        )
}

async fn cast(
    State(state): State<BackendState>,
    Path(workspace): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<LeadershipBallotCastRequest>,
) -> Result<Json<LeadershipBallotCastResponse>, LaneError> {
    validate_scope(&workspace, &body.scope, Some(body.session_id))?;
    validate_ballot_ids(body.activity_id, body.election_id)?;
    let choice = ballot_choice(body.choice);
    let scope = body.scope.clone();
    let workspace_id = scope.workspace_id;
    let activity_id = body.activity_id;
    let session_id = body.session_id;
    let election_id = body.election_id;
    let receipt = state
        .cast_scoped_ballot(
            body.scope,
            principal,
            BallotCommand {
                workspace_id,
                activity_id,
                session_id,
                election_id,
                choice,
                now_ms: crate::current_timestamp_millis()?,
            },
        )
        .await?;
    Ok(Json(LeadershipBallotCastResponse {
        scope,
        activity_id,
        session_id,
        receipt: receipt_wire(&receipt),
    }))
}

impl BackendState {
    async fn cast_scoped_ballot(
        &self,
        scope: WorkspaceQueryScope,
        principal: AuthenticatedPrincipal,
        command: BallotCommand,
    ) -> Result<heterarchy_orchestration_runtime::leadership::BallotReceiptSummary, ApiError> {
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
            state
                .activity_scope_proof(scope.clone(), command.activity_id)
                .await?;
            state
                .runtime
                .ensure_broker_owner()
                .await
                .map_err(RuntimeError::BrokerOwner)
                .map_err(ApiError::Runtime)?;
            state.workspace_scope_proof(scope.clone()).await?;
            state
                .activity_scope_proof(scope, command.activity_id)
                .await?;
            state
                .catalog_job(move |runtime| {
                    ScopedMutationAuthority::WorkspaceMember
                        .authorize(runtime, workspace, session, principal)?;
                    let mut current = command;
                    current.now_ms = crate::current_timestamp_millis()
                        .map_err(|_| RuntimeError::InvalidInput)?;
                    runtime.cast_leadership_ballot(current)
                })
                .await
        })
        .await
    }
}

async fn query(
    State(state): State<BackendState>,
    Path(workspace): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<LeadershipBallotQueryRequest>,
) -> Result<Json<LeadershipBallotQueryResponse>, LaneError> {
    validate_scope(&workspace, &body.scope, Some(body.session_id))?;
    validate_ballot_ids(body.activity_id, body.election_id)?;
    let scope = body.scope.clone();
    let workspace_id = scope.workspace_id;
    let activity_id = body.activity_id;
    let session_id = body.session_id;
    let election_id = body.election_id;
    let ballot = state
        .run_workspace_reference_query(
            body.scope,
            Some(session_id),
            principal,
            move |runtime, _| {
                runtime.leadership_ballot(workspace_id, activity_id, session_id, election_id)
            },
        )
        .await?;
    Ok(Json(LeadershipBallotQueryResponse {
        scope,
        activity_id,
        session_id,
        ballot: ballot.as_ref().map(ballot_wire),
    }))
}

fn validate_scope(
    path: &str,
    scope: &WorkspaceQueryScope,
    session_id: Option<Uuid>,
) -> Result<(), LaneError> {
    validate_workspace_reference(scope, session_id)?;
    if canonical_uuid(path)? != scope.workspace_id {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(())
}

fn validate_ballot_ids(activity_id: Uuid, election_id: Uuid) -> Result<(), LaneError> {
    if activity_id.is_nil() || election_id.is_nil() {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(())
}

fn canonical_uuid(value: &str) -> Result<Uuid, LaneError> {
    let parsed = Uuid::parse_str(value).map_err(|_| LaneError::InvalidIdentity)?;
    if parsed.is_nil() || parsed.to_string() != value {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(parsed)
}

fn ballot_choice(choice: LeadershipBallotChoice) -> BallotChoice {
    match choice {
        LeadershipBallotChoice::Yes => BallotChoice::Yes,
        LeadershipBallotChoice::No => BallotChoice::No,
        LeadershipBallotChoice::Abstain => BallotChoice::Abstain,
    }
}

fn choice_wire(choice: BallotChoice) -> LeadershipBallotChoice {
    match choice {
        BallotChoice::Yes => LeadershipBallotChoice::Yes,
        BallotChoice::No => LeadershipBallotChoice::No,
        BallotChoice::Abstain => LeadershipBallotChoice::Abstain,
    }
}

fn receipt_wire(
    receipt: &heterarchy_orchestration_runtime::leadership::BallotReceiptSummary,
) -> LeadershipBallotReceiptWire {
    LeadershipBallotReceiptWire {
        election_id: receipt.election_id,
        voter_participant_id: receipt.voter_participant_id,
        voter_participation_id: receipt.voter_participation_id,
        choice: choice_wire(receipt.choice),
        ballot_revision: receipt.ballot_revision,
        replay: receipt.replay,
    }
}

fn ballot_wire(ballot: &LeadershipBallotSnapshot) -> LeadershipBallotWire {
    LeadershipBallotWire {
        election_id: ballot.election_id(),
        voter_participant_id: ballot.voter_participant_id(),
        voter_participation_id: ballot.voter_participation_id(),
        cast_by_participant_id: ballot.cast_by_participant_id(),
        choice: choice_wire(ballot.choice()),
        ballot_revision: ballot.ballot_revision(),
        cast_at_ms: ballot.cast_at_ms(),
    }
}
