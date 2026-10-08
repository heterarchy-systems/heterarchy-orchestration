//! Source-scoped, read-only leadership review queries.

use axum::extract::{Extension, Path, State};
use axum::routing::post;
use axum::{Json, Router};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_operations::{
    BallotChoice, ElectionState, LeadershipReviewPhase, LeadershipReviewQuery,
    LeadershipReviewSnapshot, LeadershipStatementKind,
};
use uuid::Uuid;

use crate::BackendState;
use crate::app_support::{LaneError, validate_workspace_reference};
use crate::wire::leadership_ballots::LeadershipBallotChoice;
use crate::wire::leadership_reviews::{
    LeadershipReviewElectionStateWire, LeadershipReviewIdentityWire, LeadershipReviewPhaseWire,
    LeadershipReviewQueryRequest, LeadershipReviewQueryResponse, LeadershipReviewStatementKindWire,
    LeadershipReviewStatementWire, LeadershipReviewVoterWire, LeadershipReviewWire,
};

pub(super) fn routes() -> Router<BackendState> {
    Router::new().route(
        "/internal/v1/workspaces/{workspace_id}/leadership/reviews/query",
        post(query),
    )
}

async fn query(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<LeadershipReviewQueryRequest>,
) -> Result<Json<LeadershipReviewQueryResponse>, LaneError> {
    validate_request(&workspace_id, &body)?;
    let scope = body.scope.clone();
    let workspace = scope.workspace_id;
    let review_query = match (body.request_id, body.election_id) {
        (Some(request_id), None) => LeadershipReviewQuery::request(request_id),
        (None, Some(election_id)) => LeadershipReviewQuery::election(election_id),
        _ => return Err(LaneError::InvalidIdentity),
    };
    let after_sequence = body.after_sequence;
    let limit = body.limit;
    let review = state
        .run_workspace_reference_query(
            body.scope,
            body.session_id,
            principal,
            move |runtime, observed_at_ms| {
                runtime.leadership_review(
                    workspace,
                    review_query,
                    observed_at_ms,
                    Some(after_sequence),
                    limit,
                )
            },
        )
        .await?;
    Ok(Json(LeadershipReviewQueryResponse {
        scope,
        review: review.as_ref().map(leadership_review_wire),
    }))
}

fn validate_request(
    path_workspace_id: &str,
    request: &LeadershipReviewQueryRequest,
) -> Result<(), LaneError> {
    validate_workspace_reference(&request.scope, request.session_id)?;
    if canonical_uuid(path_workspace_id)? != request.scope.workspace_id
        || request.request_id.is_some_and(|id| id.is_nil())
        || request.election_id.is_some_and(|id| id.is_nil())
        || request.request_id.is_some() == request.election_id.is_some()
        || i64::try_from(request.after_sequence).is_err()
        || request.limit == 0
        || request.limit > crate::wire::leadership_reviews::MAX_LEADERSHIP_REVIEW_QUERY_LIMIT
    {
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

fn leadership_review_wire(value: &LeadershipReviewSnapshot) -> LeadershipReviewWire {
    LeadershipReviewWire {
        request_id: value.request_id(),
        election_id: value.election_id(),
        workspace_id: value.scope().workspace_id().into_uuid(),
        expected_leadership_revision: value.expected_leadership_revision(),
        proposer_activity_id: value
            .proposer_activity_id()
            .map(forge_domain::ActivityId::into_uuid),
        proposer_agent_id: value
            .proposer_agent_id()
            .map(forge_domain::AgentInstanceId::into_uuid),
        activity_id: value
            .scope()
            .activity_id()
            .map(forge_domain::ActivityId::into_uuid),
        team_id: value.scope().team_id(),
        candidate_participant_id: value.candidate_participant_id(),
        candidate_subject_id: value.candidate_subject_id(),
        election_state: election_state_wire(value.election_state()),
        elected_participant_id: value.elected_participant_id(),
        authority_confirmed: value.authority_confirmed(),
        proposer: identity_wire(value.proposer()),
        incumbent: value.incumbent().map(identity_wire),
        reason: value.reason().to_owned(),
        phase: review_phase_wire(value.phase()),
        defense_deadline_ms: value.defense_deadline_ms(),
        discussion_deadline_ms: value.discussion_deadline_ms(),
        ballot_deadline_ms: value.ballot_deadline_ms(),
        created_at_ms: value.created_at_ms(),
        defense_closed_at_ms: value.defense_closed_at_ms(),
        incumbent_response: value.incumbent_response().map(statement_wire),
        electorate: value.electorate().iter().map(voter_wire).collect(),
        electorate_truncated: value.electorate_truncated(),
        statements: value
            .statements()
            .statements()
            .iter()
            .map(statement_wire)
            .collect(),
        next_cursor: value.statements().next_cursor(),
        has_more: value.statements().has_more(),
    }
}

fn identity_wire(
    value: &heterarchy_orchestration_operations::LeadershipIdentitySnapshot,
) -> LeadershipReviewIdentityWire {
    LeadershipReviewIdentityWire {
        participant_id: value.participant_id(),
        participation_id: value
            .participation_id()
            .map(heterarchy_orchestration_operations::WorkspaceParticipationId::into_uuid),
        profile_id: value.profile_id(),
        session_id: value
            .session_id()
            .map(forge_domain::AgentSessionId::into_uuid),
        display_name: value.display_name().to_owned(),
    }
}

fn voter_wire(
    value: &heterarchy_orchestration_operations::LeadershipVoterSnapshot,
) -> LeadershipReviewVoterWire {
    LeadershipReviewVoterWire {
        participant_id: value.participant_id(),
        subject_id: value.subject_id(),
        participation_id: value
            .participation_id()
            .map(heterarchy_orchestration_operations::WorkspaceParticipationId::into_uuid),
        profile_id: value.profile_id(),
        session_id: value
            .session_id()
            .map(forge_domain::AgentSessionId::into_uuid),
        display_name: value.display_name().map(str::to_owned),
        choice: value.choice().map(ballot_choice_wire),
        cast_at_ms: value.cast_at_ms(),
    }
}

pub(super) fn statement_wire(
    value: &heterarchy_orchestration_operations::LeadershipStatementRecord,
) -> LeadershipReviewStatementWire {
    LeadershipReviewStatementWire {
        statement_id: value.statement_id(),
        election_id: value.election_id(),
        kind: statement_kind_wire(value.kind()),
        author: identity_wire(value.author()),
        body: value.body().to_owned(),
        sequence: value.sequence(),
        submitted_at_ms: value.submitted_at_ms(),
        incumbent_final: value.incumbent_final(),
    }
}

fn ballot_choice_wire(value: BallotChoice) -> LeadershipBallotChoice {
    match value {
        BallotChoice::Yes => LeadershipBallotChoice::Yes,
        BallotChoice::No => LeadershipBallotChoice::No,
        BallotChoice::Abstain => LeadershipBallotChoice::Abstain,
    }
}

fn election_state_wire(value: ElectionState) -> LeadershipReviewElectionStateWire {
    match value {
        ElectionState::Electing => LeadershipReviewElectionStateWire::Electing,
        ElectionState::ClosedElected => LeadershipReviewElectionStateWire::ClosedElected,
        ElectionState::ClosedNoMajority => LeadershipReviewElectionStateWire::ClosedNoMajority,
        ElectionState::Cancelled => LeadershipReviewElectionStateWire::Cancelled,
    }
}

fn review_phase_wire(value: LeadershipReviewPhase) -> LeadershipReviewPhaseWire {
    match value {
        LeadershipReviewPhase::Defense => LeadershipReviewPhaseWire::Defense,
        LeadershipReviewPhase::Discussion => LeadershipReviewPhaseWire::Discussion,
        LeadershipReviewPhase::Ballot => LeadershipReviewPhaseWire::Ballot,
        LeadershipReviewPhase::AwaitingDecision => LeadershipReviewPhaseWire::AwaitingDecision,
        LeadershipReviewPhase::Resolved => LeadershipReviewPhaseWire::Resolved,
    }
}

fn statement_kind_wire(value: LeadershipStatementKind) -> LeadershipReviewStatementKindWire {
    match value {
        LeadershipStatementKind::Defense => LeadershipReviewStatementKindWire::Defense,
        LeadershipStatementKind::Discussion => LeadershipReviewStatementKindWire::Discussion,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::WorkspaceQueryScope;
    use crate::wire::leadership_reviews::MAX_LEADERSHIP_REVIEW_QUERY_LIMIT;
    use std::error::Error;

    fn request() -> LeadershipReviewQueryRequest {
        LeadershipReviewQueryRequest {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            session_id: Some(Uuid::from_u128(5)),
            request_id: Some(Uuid::from_u128(6)),
            election_id: None,
            after_sequence: 0,
            limit: MAX_LEADERSHIP_REVIEW_QUERY_LIMIT,
        }
    }

    #[test]
    fn review_query_requires_exact_scope_one_selector_and_bounded_page_cursor() {
        let request = request();
        let path = request.scope.workspace_id.to_string();
        assert!(validate_request(&path, &request).is_ok());

        let mut election_selector = request.clone();
        election_selector.request_id = None;
        election_selector.election_id = Some(Uuid::from_u128(7));
        assert!(validate_request(&path, &election_selector).is_ok());

        let mut no_selector = request.clone();
        no_selector.request_id = None;
        assert!(matches!(
            validate_request(&path, &no_selector),
            Err(LaneError::InvalidIdentity)
        ));

        let mut two_selectors = request.clone();
        two_selectors.election_id = Some(Uuid::from_u128(7));
        assert!(matches!(
            validate_request(&path, &two_selectors),
            Err(LaneError::InvalidIdentity)
        ));

        let mut nil_selector = request.clone();
        nil_selector.request_id = Some(Uuid::nil());
        assert!(matches!(
            validate_request(&path, &nil_selector),
            Err(LaneError::InvalidIdentity)
        ));

        let mut path_mismatch = request.clone();
        path_mismatch.scope.workspace_id = Uuid::from_u128(8);
        assert!(matches!(
            validate_request(&path, &path_mismatch),
            Err(LaneError::InvalidIdentity)
        ));

        let mut oversized_cursor = request.clone();
        oversized_cursor.after_sequence = u64::MAX;
        assert!(matches!(
            validate_request(&path, &oversized_cursor),
            Err(LaneError::InvalidIdentity)
        ));

        let mut oversized_limit = request.clone();
        oversized_limit.limit = MAX_LEADERSHIP_REVIEW_QUERY_LIMIT + 1;
        assert!(matches!(
            validate_request(&path, &oversized_limit),
            Err(LaneError::InvalidIdentity)
        ));

        let mut empty_limit = request;
        empty_limit.limit = 0;
        assert!(matches!(
            validate_request(&path, &empty_limit),
            Err(LaneError::InvalidIdentity)
        ));
    }

    #[test]
    fn owner_review_enums_keep_the_screaming_snake_case_wire_labels() -> Result<(), Box<dyn Error>>
    {
        assert_eq!(
            serde_json::to_string(&review_phase_wire(LeadershipReviewPhase::AwaitingDecision))?,
            "\"AWAITING_DECISION\""
        );
        assert_eq!(
            serde_json::to_string(&election_state_wire(ElectionState::ClosedNoMajority))?,
            "\"CLOSED_NO_MAJORITY\""
        );
        assert_eq!(
            serde_json::to_string(&statement_kind_wire(LeadershipStatementKind::Defense))?,
            "\"DEFENSE\""
        );
        Ok(())
    }
}
