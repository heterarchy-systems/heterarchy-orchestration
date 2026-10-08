//! Axum adapter for the transport-neutral leadership application layer.
//!
//! Thin by contract: canonical-identity extraction, Broker owner gating for
//! authoritative mutations, one runtime application call, and typed
//! projection. No election semantics live here; all fences (`request_id`
//! replay identity, `expected_revision` CAS, frozen-electorate admission,
//! Ready-participant provenance) stay with the runtime and durable
//! authority.

use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_operations::BallotChoice;
use heterarchy_orchestration_runtime::leadership::BallotCommand;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::BackendState;
mod ballots;
mod lifecycle;
mod recalls;
mod reviews;
mod statements;
use crate::app_support::{LaneError, now_millis};
use crate::wire::leadership::{
    ElectionWire, LeadershipQueryRequest, LeadershipQueryResponse, LeadershipViewResponse,
    LeadershipWire, ReviewHistoryEntryWire, ReviewHistoryWire,
};

/// Leadership routes; merge before the router takes its state.
pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .merge(ballots::routes())
        .merge(reviews::routes())
        .merge(recalls::routes())
        .merge(statements::routes())
        .merge(lifecycle::routes())
        .route(
            "/internal/v1/workspaces/{workspace_id}/leadership",
            get(leadership_view),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/leadership/query",
            post(scoped_leadership_view),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/leadership/reviews",
            post(recalls::start),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/leadership/statements",
            post(statements::append),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/leadership/ballots",
            post(cast_ballot),
        )
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LeadershipViewQuery {
    activity_id: Option<String>,
    team_id: Option<String>,
    review_after: Option<String>,
    limit: Option<u32>,
}

/// Default review-history page size, mirroring the MCP read tool.
const DEFAULT_HISTORY_LIMIT: u32 = 32;
/// History page ceiling, mirroring the durable authority's own validation.
const MAX_HISTORY_LIMIT: u32 = 64;

async fn scoped_leadership_view(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<LeadershipQueryRequest>,
) -> Result<Json<LeadershipQueryResponse>, LaneError> {
    let workspace = canonical_uuid(Some(workspace_id.as_str()))?;
    if workspace != body.scope.workspace_id
        || body.scope.location_id.is_nil()
        || body.scope.location_revision < 1
        || body.scope.workspace_revision < 1
        || body.scope.locator.is_empty()
        || body.scope.locator.len() > 4096
        || body.scope.locator.chars().any(char::is_control)
        || body.session_id.is_some_and(|id| id.is_nil())
        || body.activity_id.is_some_and(|id| id.is_nil())
        || body.team_id.is_some_and(|id| id.is_nil())
        || body.review_after.is_some_and(|id| id.is_nil())
        || (body.activity_id.is_some() && body.team_id.is_some())
    {
        return Err(LaneError::InvalidIdentity);
    }
    let limit = history_limit(Some(body.limit))?;
    let scope = body.scope.clone();
    let (view, history) = state
        .run_workspace_reference_query(
            body.scope,
            body.session_id,
            principal,
            move |runtime, now| {
                runtime.leadership_view(
                    workspace,
                    body.activity_id,
                    body.team_id,
                    body.review_after,
                    limit,
                    now,
                )
            },
        )
        .await?;
    Ok(Json(LeadershipQueryResponse {
        scope,
        activity_id: body.activity_id,
        team_id: body.team_id,
        view: LeadershipViewResponse::from_parts(view.as_ref(), &history),
    }))
}

fn history_limit(limit: Option<u32>) -> Result<u32, LaneError> {
    let limit = limit.unwrap_or(DEFAULT_HISTORY_LIMIT);
    if limit == 0 || limit > MAX_HISTORY_LIMIT {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(limit)
}

async fn leadership_view(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Query(query): Query<LeadershipViewQuery>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: axum::http::HeaderMap,
) -> Result<Json<LeadershipViewResponse>, LaneError> {
    let session = crate::app_support::session_selector(&headers)?;
    let workspace = canonical_uuid(Some(workspace_id.as_str()))?;
    let activity = optional_uuid(query.activity_id.as_deref())?;
    let team = optional_uuid(query.team_id.as_deref())?;
    let after = optional_uuid(query.review_after.as_deref())?;
    let limit = history_limit(query.limit)?;
    let now = now_millis()?;
    let (view, history) = state
        .run_workspace_query(workspace, session, principal, move |runtime| {
            runtime.leadership_view(workspace, activity, team, after, limit, now)
        })
        .await?;
    Ok(Json(LeadershipViewResponse::from_parts(
        view.as_ref(),
        &history,
    )))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BallotBody {
    activity_id: String,
    session_id: String,
    election_id: String,
    choice: String,
}

async fn cast_ballot(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<BallotBody>,
) -> Result<Json<BallotReceiptResponse>, LaneError> {
    let choice = match body.choice.as_str() {
        "yes" => BallotChoice::Yes,
        "no" => BallotChoice::No,
        "abstain" => BallotChoice::Abstain,
        _ => return Err(LaneError::InvalidIdentity),
    };
    let command = BallotCommand {
        workspace_id: canonical_uuid(Some(workspace_id.as_str()))?,
        activity_id: canonical_uuid(Some(body.activity_id.as_str()))?,
        session_id: canonical_uuid(Some(body.session_id.as_str()))?,
        election_id: canonical_uuid(Some(body.election_id.as_str()))?,
        choice,
        now_ms: now_millis()?,
    };
    let receipt = state
        .run_session_mutation(command.session_id, principal, move |runtime| {
            runtime.cast_leadership_ballot(command)
        })
        .await?;
    Ok(Json(BallotReceiptResponse::from_summary(&receipt)))
}

fn optional_uuid(value: Option<&str>) -> Result<Option<Uuid>, LaneError> {
    match value {
        Some(value) => Ok(Some(canonical_uuid(Some(value))?)),
        None => Ok(None),
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

// --- wire projections ---

impl LeadershipViewResponse {
    fn from_parts(
        view: Option<&heterarchy_orchestration_runtime::leadership::LeadershipViewSummary>,
        history: &heterarchy_orchestration_runtime::leadership::LeadershipReviewHistorySummary,
    ) -> Self {
        Self {
            leadership: view.map(|view| LeadershipWire {
                workspace_id: view.workspace_id,
                activity_id: view.activity_id,
                team_id: view.team_id,
                state: view.state.to_owned(),
                leadership_revision: view.leadership_revision,
                leader_participation_id: view.leader_participation_id,
                leader_profile_id: view.leader_profile_id,
                leader_participant_id: view.leader_participant_id,
                leader_display_name: view.leader_display_name.clone(),
                stabilization_until_ms: view.stabilization_until_ms,
                updated_at_ms: view.updated_at_ms,
                current_election: view.current_election.as_ref().map(|election| ElectionWire {
                    election_id: election.election_id,
                    state: election.state.to_owned(),
                    ballot_deadline_ms: election.ballot_deadline_ms,
                    deadline_passed: election.deadline_passed,
                    electorate_size: election.electorate_size,
                    cast_yes: election.cast_yes,
                    cast_no: election.cast_no,
                    cast_abstain: election.cast_abstain,
                }),
            }),
            review_history: ReviewHistoryWire {
                entries: history
                    .entries
                    .iter()
                    .map(|entry| ReviewHistoryEntryWire {
                        request_id: entry.request_id,
                        election_id: entry.election_id,
                        created_at_ms: entry.created_at_ms,
                        reason_preview: entry.reason_preview.clone(),
                        phase: entry.phase.to_owned(),
                    })
                    .collect(),
                next_cursor: history.next_cursor,
            },
        }
    }
}

fn empty_history() -> heterarchy_orchestration_runtime::leadership::LeadershipReviewHistorySummary {
    heterarchy_orchestration_runtime::leadership::LeadershipReviewHistorySummary {
        entries: Vec::new(),
        next_cursor: None,
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BallotReceiptResponse {
    election_id: Uuid,
    voter_participant_id: Uuid,
    choice: &'static str,
    ballot_revision: i64,
    replay: bool,
}

impl BallotReceiptResponse {
    fn from_summary(
        summary: &heterarchy_orchestration_runtime::leadership::BallotReceiptSummary,
    ) -> Self {
        Self {
            election_id: summary.election_id,
            voter_participant_id: summary.voter_participant_id,
            choice: match summary.choice {
                BallotChoice::Yes => "yes",
                BallotChoice::No => "no",
                BallotChoice::Abstain => "abstain",
            },
            ballot_revision: summary.ballot_revision,
            replay: summary.replay,
        }
    }
}
