//! Bounded HTTP leadership query client.

use std::convert::TryFrom;

use hyper::Method;
use hyper::header::HeaderValue;

use crate::wire::leadership::LeadershipQueryResponse;
use crate::wire::{ElectionWire, LeadershipQueryRequest, LeadershipViewResponse};

use super::{ClientError, OrchestrationHttpClient};

const MAX_LEADERSHIP_HISTORY: u32 = 64;
const MAX_LOCATOR_BYTES: usize = 4_096;

impl OrchestrationHttpClient {
    /// Read one bounded leadership projection using the caller's credential.
    ///
    /// The request is a pure query, so an uncertain exchange has no durable
    /// mutation identity and is never converted into a mutation readback.
    ///
    /// # Errors
    /// Rejects invalid scope selectors, bounded response violations, typed
    /// service rejection, and bounded transport failures.
    pub async fn leadership_view(
        &self,
        authorization: &HeaderValue,
        request: &LeadershipQueryRequest,
    ) -> Result<LeadershipViewResponse, ClientError> {
        validate_request(request)?;
        let path = format!(
            "/internal/v1/workspaces/{}/leadership/query",
            request.scope.workspace_id
        );
        let response: LeadershipQueryResponse = self
            .call(authorization, Method::POST, path, Some(request), None)
            .await?;
        if !valid_query_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response.view)
    }
}

fn validate_request(request: &LeadershipQueryRequest) -> Result<(), ClientError> {
    let scope = &request.scope;
    if scope.workspace_id.is_nil()
        || scope.location_id.is_nil()
        || scope.location_revision <= 0
        || scope.workspace_revision <= 0
        || scope.locator.is_empty()
        || scope.locator.len() > MAX_LOCATOR_BYTES
        || scope.locator.chars().any(char::is_control)
        || !(1..=MAX_LEADERSHIP_HISTORY).contains(&request.limit)
        || request.activity_id.is_some_and(|id| id.is_nil())
        || request.team_id.is_some_and(|id| id.is_nil())
        || request.review_after.is_some_and(|id| id.is_nil())
        || request.session_id.is_some_and(|id| id.is_nil())
        || (request.activity_id.is_some() && request.team_id.is_some())
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn valid_query_response(
    response: &LeadershipQueryResponse,
    request: &LeadershipQueryRequest,
) -> bool {
    response.scope == request.scope
        && response.activity_id == request.activity_id
        && response.team_id == request.team_id
        && valid_response(&response.view, request)
}

pub(super) fn valid_response(
    response: &LeadershipViewResponse,
    request: &LeadershipQueryRequest,
) -> bool {
    let Ok(history_limit) = usize::try_from(request.limit) else {
        return false;
    };
    if response.review_history.entries.len() > history_limit
        || response
            .review_history
            .next_cursor
            .is_some_and(|id| id.is_nil())
    {
        return false;
    }
    if response.review_history.entries.iter().any(|entry| {
        entry.request_id.is_nil()
            || entry.election_id.is_nil()
            || entry.created_at_ms < 0
            || !matches!(
                entry.phase.as_str(),
                "defense" | "discussion" | "ballot" | "awaiting_decision" | "resolved"
            )
    }) {
        return false;
    }

    let Some(leadership) = response.leadership.as_ref() else {
        return true;
    };
    if leadership.workspace_id != request.scope.workspace_id
        || leadership.activity_id != request.activity_id
        || leadership.team_id != request.team_id
        || !matches!(
            leadership.state.as_str(),
            "leaderless"
                | "bootstrapping"
                | "stabilizing"
                | "active"
                | "electing"
                | "authority_pending"
                | "failover_required"
                | "vacant"
                | "recovery_required"
        )
        || leadership.leadership_revision <= 0
        || leadership.updated_at_ms < 0
        || leadership
            .stabilization_until_ms
            .is_some_and(|value| value < 0)
        || leadership
            .leader_participation_id
            .is_some_and(|id| id.is_nil())
        || leadership.leader_profile_id.is_some_and(|id| id.is_nil())
        || leadership
            .leader_participant_id
            .is_some_and(|id| id.is_nil())
    {
        return false;
    }
    leadership
        .current_election
        .as_ref()
        .is_none_or(valid_election)
}

fn valid_election(election: &ElectionWire) -> bool {
    !election.election_id.is_nil()
        && matches!(
            election.state.as_str(),
            "electing" | "closed_elected" | "closed_no_majority" | "cancelled"
        )
        && election.ballot_deadline_ms >= 0
        && election.electorate_size >= 0
        && election.cast_yes >= 0
        && election.cast_no >= 0
        && election.cast_abstain >= 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{
        LeadershipWire, ReviewHistoryEntryWire, ReviewHistoryWire, WorkspaceQueryScope,
    };
    use uuid::Uuid;

    fn request() -> LeadershipQueryRequest {
        LeadershipQueryRequest {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 1,
                workspace_revision: 1,
                locator: "workspace".to_owned(),
            },
            session_id: Some(Uuid::from_u128(3)),
            activity_id: Some(Uuid::from_u128(4)),
            team_id: None,
            review_after: None,
            limit: 32,
        }
    }

    #[test]
    fn empty_view_still_requires_exact_query_context() {
        let request = request();
        let mut response = LeadershipQueryResponse {
            scope: request.scope.clone(),
            activity_id: request.activity_id,
            team_id: request.team_id,
            view: LeadershipViewResponse {
                leadership: None,
                review_history: ReviewHistoryWire {
                    entries: Vec::new(),
                    next_cursor: None,
                },
            },
        };
        assert!(valid_query_response(&response, &request));
        response.scope.workspace_revision += 1;
        assert!(!valid_query_response(&response, &request));
        response.scope = request.scope.clone();
        response.team_id = Some(Uuid::from_u128(8));
        assert!(!valid_query_response(&response, &request));
    }

    #[test]
    fn request_validation_keeps_scope_and_history_bounds_explicit() {
        let mut invalid = request();
        invalid.team_id = Some(Uuid::from_u128(5));
        assert_eq!(validate_request(&invalid), Err(ClientError::InvalidRequest));

        let mut invalid = request();
        invalid.scope.locator = "bad\nlocator".to_owned();
        assert_eq!(validate_request(&invalid), Err(ClientError::InvalidRequest));

        let mut invalid = request();
        invalid.limit = 65;
        assert_eq!(validate_request(&invalid), Err(ClientError::InvalidRequest));
    }

    #[test]
    fn response_validation_rejects_scope_mismatch_and_oversized_history() {
        let request = request();
        let response = LeadershipViewResponse {
            leadership: Some(LeadershipWire {
                workspace_id: Uuid::from_u128(9),
                activity_id: Some(Uuid::from_u128(4)),
                team_id: None,
                state: "active".to_owned(),
                leadership_revision: 1,
                leader_participation_id: None,
                leader_profile_id: None,
                leader_participant_id: None,
                leader_display_name: None,
                stabilization_until_ms: None,
                updated_at_ms: 1,
                current_election: None,
            }),
            review_history: ReviewHistoryWire {
                entries: Vec::new(),
                next_cursor: None,
            },
        };
        assert!(!valid_response(&response, &request));

        let mut response = response;
        response.leadership = None;
        response.review_history.entries = (0..33)
            .map(|_| ReviewHistoryEntryWire {
                request_id: Uuid::from_u128(6),
                election_id: Uuid::from_u128(7),
                created_at_ms: 1,
                reason_preview: "reason".to_owned(),
                phase: "defense".to_owned(),
            })
            .collect();
        assert!(!valid_response(&response, &request));
    }
}
