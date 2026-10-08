//! Axum adapter for the Team / Membership application layer.
//!
//! Thin by contract: request bodies name sessions and activities; the
//! runtime derives every member Agent identity through the canonical member
//! binding, and retirement runs strictly as the local operator. Exact
//! `request_id` replay, `expected_team_revision` CAS and the initial-roster
//! atomicity stay with the durable authority.

mod commands;
pub(crate) mod scoped;

use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Extension, Json, Router};
use forge_domain::AuthenticatedPrincipal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::BackendState;
use crate::app_support::LaneError;

/// Team mutation routes; merge before the router takes its state.
pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .merge(scoped::routes())
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/{team_id}",
            get(read_team),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/teams/{team_id}/members",
            get(list_members),
        )
        .merge(commands::routes())
}

async fn read_team(
    State(state): State<BackendState>,
    Path((workspace_id, team_id)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Option<crate::TeamResponse>>, LaneError> {
    let session = crate::app_support::session_selector(&headers)?;
    let (workspace, team) = scope_ids(&workspace_id, &team_id)?;
    let summary = state
        .run_workspace_query(workspace, session, principal, move |runtime| {
            runtime.team(workspace, team)
        })
        .await?;
    Ok(Json(summary.map(crate::TeamResponse::from)))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MemberListQuery {
    limit: Option<u32>,
}

async fn list_members(
    State(state): State<BackendState>,
    Path((workspace_id, team_id)): Path<(String, String)>,
    Query(query): Query<MemberListQuery>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: axum::http::HeaderMap,
) -> Result<Json<MembershipPageWire>, LaneError> {
    let session = crate::app_support::session_selector(&headers)?;
    let (workspace, team) = scope_ids(&workspace_id, &team_id)?;
    let limit = query.limit.unwrap_or(64);
    let page = state
        .run_workspace_query(workspace, session, principal, move |runtime| {
            runtime.team_members(workspace, team, limit)
        })
        .await?;
    Ok(Json(MembershipPageWire {
        items: page
            .items
            .iter()
            .map(|member| MembershipWire {
                participation_id: member.participation_id,
                profile_id: member.profile_id,
                role: member.role.clone(),
                state: member.state,
                revision: member.revision,
                joined_at_ms: member.joined_at_ms,
                left_at_ms: member.left_at_ms,
            })
            .collect(),
        truncated: page.truncated,
    }))
}

fn scope_ids(workspace: &str, team: &str) -> Result<(Uuid, Uuid), LaneError> {
    Ok((
        canonical_uuid(Some(workspace))?,
        canonical_uuid(Some(team))?,
    ))
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

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MembershipWire {
    participation_id: Uuid,
    profile_id: Option<Uuid>,
    role: Option<String>,
    state: &'static str,
    revision: u64,
    joined_at_ms: i64,
    left_at_ms: Option<i64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MembershipPageWire {
    items: Vec<MembershipWire>,
    truncated: bool,
}
