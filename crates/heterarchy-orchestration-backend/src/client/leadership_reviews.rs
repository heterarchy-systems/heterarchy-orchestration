//! Bounded client for source-scoped leadership review snapshots.

use std::collections::HashSet;

use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use crate::wire::leadership::WorkspaceQueryScope;
use crate::wire::leadership_reviews::{
    LeadershipReviewIdentityWire, LeadershipReviewQueryRequest, LeadershipReviewQueryResponse,
    LeadershipReviewStatementKindWire, LeadershipReviewStatementWire, LeadershipReviewVoterWire,
    LeadershipReviewWire, MAX_LEADERSHIP_REVIEW_QUERY_LIMIT,
};

use super::{ClientError, MAX_REVIEW_RESPONSE_BYTES, OrchestrationHttpClient};

const MAX_LOCATOR_BYTES: usize = 4_096;
const MAX_REASON_BYTES: usize = 4_096;
const MAX_DISPLAY_NAME_BYTES: usize = 128;
const MAX_STATEMENT_BODY_BYTES: usize = 16_384;
const MAX_ELECTORATE_SIZE: usize = 64;

impl OrchestrationHttpClient {
    /// Read one exact leadership review and bounded statement page.
    ///
    /// `session_id` is a caller-authorized selector only; the server does not
    /// use it to filter the current review snapshot. This read never uses
    /// mutation readback or retry semantics and accepts the review API's
    /// larger bounded response envelope for a maximum-size statement page.
    ///
    /// # Errors
    /// Returns invalid scope/selector input, typed service rejection, bounded
    /// transport failures, or a protocol error for a mismatched projection.
    pub async fn leadership_review(
        &self,
        authorization: &HeaderValue,
        request: &LeadershipReviewQueryRequest,
    ) -> Result<LeadershipReviewQueryResponse, ClientError> {
        validate_request(request)?;
        let response: LeadershipReviewQueryResponse = self
            .call_with_response_limit(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/leadership/reviews/query",
                    request.scope.workspace_id
                ),
                Some(request),
                None,
                MAX_REVIEW_RESPONSE_BYTES,
            )
            .await?;
        if !valid_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn validate_request(request: &LeadershipReviewQueryRequest) -> Result<(), ClientError> {
    let has_request = request.request_id.is_some();
    let has_election = request.election_id.is_some();
    if !valid_scope(&request.scope)
        || request.session_id.is_some_and(|id| id.is_nil())
        || has_request == has_election
        || request.request_id.is_some_and(|id| id.is_nil())
        || request.election_id.is_some_and(|id| id.is_nil())
        || i64::try_from(request.after_sequence).is_err()
        || !(1..=MAX_LEADERSHIP_REVIEW_QUERY_LIMIT).contains(&request.limit)
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn valid_scope(scope: &WorkspaceQueryScope) -> bool {
    !scope.workspace_id.is_nil()
        && !scope.location_id.is_nil()
        && scope.location_revision > 0
        && scope.workspace_revision > 0
        && !scope.locator.is_empty()
        && scope.locator.len() <= MAX_LOCATOR_BYTES
        && !scope.locator.chars().any(char::is_control)
}

fn valid_response(
    response: &LeadershipReviewQueryResponse,
    request: &LeadershipReviewQueryRequest,
) -> bool {
    response.scope == request.scope
        && response
            .review
            .as_ref()
            .is_none_or(|review| valid_review(review, request))
}

fn valid_review(review: &LeadershipReviewWire, request: &LeadershipReviewQueryRequest) -> bool {
    let selector_matches = match (request.request_id, request.election_id) {
        (Some(request_id), None) => review.request_id == request_id,
        (None, Some(election_id)) => review.election_id == election_id,
        _ => false,
    };
    selector_matches
        && !review.request_id.is_nil()
        && !review.election_id.is_nil()
        && review.workspace_id == request.scope.workspace_id
        && review.expected_leadership_revision > 0
        && review.proposer_activity_id.is_none_or(|id| !id.is_nil())
        && review.proposer_agent_id.is_none_or(|id| !id.is_nil())
        && valid_proposer_bindings(review)
        && review.activity_id.is_none_or(|id| !id.is_nil())
        && review.team_id.is_none_or(|id| !id.is_nil())
        && !(review.activity_id.is_some() && review.team_id.is_some())
        && !review.candidate_participant_id.is_nil()
        && !review.candidate_subject_id.is_nil()
        && review.elected_participant_id.is_none_or(|id| !id.is_nil())
        && valid_identity(&review.proposer)
        && review.incumbent.as_ref().is_none_or(valid_identity)
        && valid_reason(&review.reason)
        && valid_review_timestamps(review)
        && review.electorate.len() <= MAX_ELECTORATE_SIZE
        && (!review.electorate_truncated || review.electorate.len() == MAX_ELECTORATE_SIZE)
        && review.electorate.iter().all(valid_voter)
        && unique_voters(&review.electorate)
        && review.incumbent_response.as_ref().is_none_or(|statement| {
            valid_statement(statement, review.election_id, 0)
                && statement.kind == LeadershipReviewStatementKindWire::Defense
                && statement.incumbent_final
        })
        && valid_statement_page(review, request)
}

fn valid_proposer_bindings(review: &LeadershipReviewWire) -> bool {
    let member = review.proposer_activity_id.is_some();
    [
        review.proposer_agent_id.is_some(),
        review.proposer.session_id.is_some(),
        review.proposer.participation_id.is_some(),
        review.proposer.profile_id.is_some(),
        review.proposer.participant_id.is_some(),
    ]
    .into_iter()
    .all(|present| present == member)
}

fn valid_review_timestamps(review: &LeadershipReviewWire) -> bool {
    review.created_at_ms >= 0
        && review.defense_deadline_ms >= 0
        && review.discussion_deadline_ms >= review.defense_deadline_ms
        && review.ballot_deadline_ms >= review.discussion_deadline_ms
        && review
            .defense_closed_at_ms
            .is_none_or(|timestamp| timestamp >= 0)
}

fn valid_reason(reason: &str) -> bool {
    !reason.trim().is_empty() && reason.len() <= MAX_REASON_BYTES
}

fn valid_identity(identity: &LeadershipReviewIdentityWire) -> bool {
    identity.participant_id.is_none_or(|id| !id.is_nil())
        && identity.participation_id.is_none_or(|id| !id.is_nil())
        && identity.profile_id.is_none_or(|id| !id.is_nil())
        && identity.session_id.is_none_or(|id| !id.is_nil())
        && valid_display_name(&identity.display_name)
}

fn valid_display_name(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= MAX_DISPLAY_NAME_BYTES
}

fn valid_voter(voter: &LeadershipReviewVoterWire) -> bool {
    let subject_matches_source = voter
        .participation_id
        .map_or(voter.participant_id == voter.subject_id, |id| {
            id == voter.subject_id
        });
    !voter.participant_id.is_nil()
        && !voter.subject_id.is_nil()
        && voter.participation_id.is_none_or(|id| !id.is_nil())
        && voter.profile_id.is_none_or(|id| !id.is_nil())
        && voter.session_id.is_none_or(|id| !id.is_nil())
        && voter.display_name.as_deref().is_none_or(valid_display_name)
        && voter.choice.is_some() == voter.cast_at_ms.is_some()
        && voter.cast_at_ms.is_none_or(|timestamp| timestamp >= 0)
        && subject_matches_source
}

fn unique_voters(voters: &[LeadershipReviewVoterWire]) -> bool {
    let mut subjects = HashSet::with_capacity(voters.len());
    voters
        .iter()
        .all(|voter| subjects.insert((voter.participation_id.is_some(), voter.subject_id)))
}

fn valid_statement_page(
    review: &LeadershipReviewWire,
    request: &LeadershipReviewQueryRequest,
) -> bool {
    let Ok(limit) = usize::try_from(request.limit) else {
        return false;
    };
    let statements = &review.statements;
    if statements.len() > limit
        || !statements
            .iter()
            .all(|statement| valid_statement(statement, review.election_id, request.after_sequence))
        || !statements
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence)
        || !unique_statements(statements)
    {
        return false;
    }
    match (review.has_more, review.next_cursor, statements.last()) {
        (true, Some(cursor), Some(last)) => statements.len() == limit && cursor == last.sequence,
        (false, None, _) => true,
        _ => false,
    }
}

fn unique_statements(statements: &[LeadershipReviewStatementWire]) -> bool {
    let mut ids = HashSet::with_capacity(statements.len());
    statements
        .iter()
        .all(|statement| ids.insert(statement.statement_id))
}

fn valid_statement(
    statement: &LeadershipReviewStatementWire,
    election_id: Uuid,
    after_sequence: u64,
) -> bool {
    !statement.statement_id.is_nil()
        && statement.election_id == election_id
        && valid_identity(&statement.author)
        && !statement.body.trim().is_empty()
        && statement.body.len() <= MAX_STATEMENT_BODY_BYTES
        && statement.sequence > after_sequence
        && i64::try_from(statement.sequence).is_ok()
        && statement.submitted_at_ms >= 0
        && (!statement.incumbent_final
            || statement.kind == LeadershipReviewStatementKindWire::Defense)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use hyper::header::HeaderValue;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use uuid::Uuid;

    use crate::wire::leadership::WorkspaceQueryScope;
    use crate::wire::leadership_ballots::LeadershipBallotChoice;
    use crate::wire::leadership_reviews::{
        LeadershipReviewElectionStateWire, LeadershipReviewIdentityWire, LeadershipReviewPhaseWire,
        LeadershipReviewQueryRequest, LeadershipReviewQueryResponse,
        LeadershipReviewStatementKindWire, LeadershipReviewStatementWire,
        LeadershipReviewVoterWire, LeadershipReviewWire,
    };

    use super::*;

    fn scope() -> WorkspaceQueryScope {
        WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 3,
            workspace_revision: 4,
            locator: "workspace".to_owned(),
        }
    }

    fn request() -> LeadershipReviewQueryRequest {
        LeadershipReviewQueryRequest {
            scope: scope(),
            session_id: Some(Uuid::from_u128(3)),
            request_id: Some(Uuid::from_u128(4)),
            election_id: None,
            after_sequence: 0,
            limit: 64,
        }
    }

    fn identity(seed: u128) -> LeadershipReviewIdentityWire {
        LeadershipReviewIdentityWire {
            participant_id: Some(Uuid::from_u128(seed)),
            participation_id: Some(Uuid::from_u128(seed + 1)),
            profile_id: Some(Uuid::from_u128(seed + 2)),
            session_id: Some(Uuid::from_u128(seed + 3)),
            display_name: "Review participant".to_owned(),
        }
    }

    fn voter(seed: u128, cast: bool) -> LeadershipReviewVoterWire {
        let participation_id = Uuid::from_u128(seed + 1);
        LeadershipReviewVoterWire {
            participant_id: Uuid::from_u128(seed),
            subject_id: participation_id,
            participation_id: Some(participation_id),
            profile_id: Some(Uuid::from_u128(seed + 2)),
            session_id: Some(Uuid::from_u128(seed + 3)),
            display_name: None,
            choice: cast.then_some(LeadershipBallotChoice::Yes),
            cast_at_ms: cast.then_some(15),
        }
    }

    fn statement(election_id: Uuid, sequence: u64, body: String) -> LeadershipReviewStatementWire {
        LeadershipReviewStatementWire {
            statement_id: Uuid::from_u128(0x1000 + u128::from(sequence)),
            election_id,
            kind: LeadershipReviewStatementKindWire::Discussion,
            author: identity(0x2000 + u128::from(sequence) * 4),
            body,
            sequence,
            submitted_at_ms: 20,
            incumbent_final: false,
        }
    }

    fn review(
        request: &LeadershipReviewQueryRequest,
        statement_count: u64,
    ) -> LeadershipReviewWire {
        let election_id = request.election_id.unwrap_or(Uuid::from_u128(5));
        LeadershipReviewWire {
            request_id: request.request_id.unwrap_or(Uuid::from_u128(4)),
            election_id,
            workspace_id: request.scope.workspace_id,
            expected_leadership_revision: 2,
            proposer_activity_id: Some(Uuid::from_u128(8)),
            proposer_agent_id: Some(Uuid::from_u128(9)),
            activity_id: None,
            team_id: None,
            candidate_participant_id: Uuid::from_u128(6),
            candidate_subject_id: Uuid::from_u128(7),
            election_state: LeadershipReviewElectionStateWire::Electing,
            elected_participant_id: None,
            authority_confirmed: false,
            proposer: identity(10),
            incumbent: None,
            reason: "A bounded review fixture".to_owned(),
            phase: LeadershipReviewPhaseWire::Ballot,
            defense_deadline_ms: 100,
            discussion_deadline_ms: 200,
            ballot_deadline_ms: 300,
            created_at_ms: 10,
            defense_closed_at_ms: Some(100),
            incumbent_response: None,
            electorate: (0..64)
                .map(|index| voter(0x3000 + index * 8, index == 0))
                .collect(),
            electorate_truncated: true,
            statements: (1..=statement_count)
                .map(|sequence| statement(election_id, sequence, "A statement".to_owned()))
                .collect(),
            next_cursor: None,
            has_more: false,
        }
    }

    fn response(
        request: &LeadershipReviewQueryRequest,
        statements: u64,
    ) -> LeadershipReviewQueryResponse {
        LeadershipReviewQueryResponse {
            scope: request.scope.clone(),
            review: Some(review(request, statements)),
        }
    }

    #[test]
    fn request_requires_exact_selector_and_native_bounds() {
        let valid = request();
        assert!(validate_request(&valid).is_ok());

        let mut invalid = valid.clone();
        invalid.request_id = None;
        assert!(validate_request(&invalid).is_err());
        invalid.request_id = Some(Uuid::from_u128(4));
        invalid.election_id = Some(Uuid::from_u128(5));
        assert!(validate_request(&invalid).is_err());

        let mut invalid = valid.clone();
        invalid.request_id = Some(Uuid::nil());
        assert!(validate_request(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.session_id = Some(Uuid::nil());
        assert!(validate_request(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.after_sequence = u64::MAX;
        assert!(validate_request(&invalid).is_err());
        let mut invalid = valid.clone();
        invalid.limit = 0;
        assert!(validate_request(&invalid).is_err());
        invalid.limit = MAX_LEADERSHIP_REVIEW_QUERY_LIMIT + 1;
        assert!(validate_request(&invalid).is_err());
    }

    #[test]
    fn proposer_and_revision_preserve_persisted_intent() -> Result<(), Box<dyn std::error::Error>> {
        let query = request();
        let complete = response(&query, 1);
        assert!(valid_response(&complete, &query));
        let mut invalid_revision = complete.clone();
        invalid_revision
            .review
            .as_mut()
            .ok_or("review missing")?
            .expected_leadership_revision = 0;
        assert!(!valid_response(&invalid_revision, &query));

        let mut nil_proposer_activity = complete.clone();
        nil_proposer_activity
            .review
            .as_mut()
            .ok_or("review missing")?
            .proposer_activity_id = Some(Uuid::nil());
        assert!(!valid_response(&nil_proposer_activity, &query));

        let mut nil_proposer_agent = complete.clone();
        nil_proposer_agent
            .review
            .as_mut()
            .ok_or("review missing")?
            .proposer_agent_id = Some(Uuid::nil());
        assert!(!valid_response(&nil_proposer_agent, &query));

        for missing in 0..6 {
            let mut partial = complete.clone();
            let review = partial.review.as_mut().ok_or("review missing")?;
            match missing {
                0 => review.proposer_activity_id = None,
                1 => review.proposer_agent_id = None,
                2 => review.proposer.session_id = None,
                3 => review.proposer.participation_id = None,
                4 => review.proposer.profile_id = None,
                _ => review.proposer.participant_id = None,
            }
            assert!(!valid_response(&partial, &query));
        }
        let mut operator = complete;
        let review = operator.review.as_mut().ok_or("review missing")?;
        review.proposer_activity_id = None;
        review.proposer_agent_id = None;
        review.proposer.session_id = None;
        review.proposer.participation_id = None;
        review.proposer.profile_id = None;
        review.proposer.participant_id = None;
        assert!(valid_response(&operator, &query));
        Ok(())
    }

    #[test]
    fn response_preserves_optional_identity_fields_and_checks_snapshot_and_cursor()
    -> Result<(), Box<dyn std::error::Error>> {
        let query = request();
        let complete = response(&query, 2);
        assert!(valid_response(&complete, &query));

        let mut other_session = query.clone();
        other_session.session_id = Some(Uuid::from_u128(90));
        assert!(valid_response(&complete, &other_session));

        let mut foreign_scope = complete.clone();
        foreign_scope.scope.workspace_revision += 1;
        assert!(!valid_response(&foreign_scope, &query));

        let mut wrong_request = complete.clone();
        wrong_request
            .review
            .as_mut()
            .ok_or("review missing")?
            .request_id = Uuid::from_u128(55);
        assert!(!valid_response(&wrong_request, &query));

        let mut foreign_review = complete.clone();
        foreign_review
            .review
            .as_mut()
            .ok_or("review missing")?
            .workspace_id = Uuid::from_u128(56);
        assert!(!valid_response(&foreign_review, &query));

        let mut wrong_page = complete.clone();
        let page = wrong_page.review.as_mut().ok_or("review missing")?;
        page.statements.swap(0, 1);
        assert!(!valid_response(&wrong_page, &query));

        let mut after_query = query.clone();
        after_query.after_sequence = 1;
        assert!(!valid_response(&complete, &after_query));

        let mut wrong_cursor = complete.clone();
        let page = wrong_cursor.review.as_mut().ok_or("review missing")?;
        page.has_more = true;
        page.next_cursor = Some(1);
        assert!(!valid_response(&wrong_cursor, &query));

        let mut valid_continuation = complete.clone();
        let page = valid_continuation.review.as_mut().ok_or("review missing")?;
        page.statements = (1..=64)
            .map(|sequence| statement(page.election_id, sequence, "A statement".to_owned()))
            .collect();
        page.has_more = true;
        page.next_cursor = Some(64);
        assert!(valid_response(&valid_continuation, &query));

        let mut wrong_choice_pair = complete.clone();
        let voter = wrong_choice_pair
            .review
            .as_mut()
            .ok_or("review missing")?
            .electorate
            .get_mut(0)
            .ok_or("voter missing")?;
        voter.cast_at_ms = None;
        assert!(!valid_response(&wrong_choice_pair, &query));

        let mut duplicate_subject = complete.clone();
        let electorate = &mut duplicate_subject
            .review
            .as_mut()
            .ok_or("review missing")?
            .electorate;
        let mut second_source = electorate[0].clone();
        second_source.participant_id = Uuid::from_u128(91);
        electorate[1] = second_source;
        assert!(!valid_response(&duplicate_subject, &query));

        let mut duplicate_statement = complete.clone();
        let statements = &mut duplicate_statement
            .review
            .as_mut()
            .ok_or("review missing")?
            .statements;
        let mut repeated = statements[0].clone();
        repeated.sequence = 2;
        statements[1] = repeated;
        assert!(!valid_response(&duplicate_statement, &query));

        let mut wire = serde_json::to_value(&complete)?;
        let review = wire["review"].as_object_mut().ok_or("review object")?;
        let _omitted = review.remove("nextCursor");
        assert!(serde_json::from_value::<LeadershipReviewQueryResponse>(wire).is_err());

        let mut wire = serde_json::to_value(&complete)?;
        let review = wire["review"].as_object_mut().ok_or("review object")?;
        let _omitted = review.remove("incumbent");
        assert!(serde_json::from_value::<LeadershipReviewQueryResponse>(wire).is_err());

        let mut wire = serde_json::to_value(&complete)?;
        let review = wire["review"].as_object_mut().ok_or("review object")?;
        let _omitted = review.remove("proposerActivityId");
        assert!(serde_json::from_value::<LeadershipReviewQueryResponse>(wire).is_err());

        let mut wire = serde_json::to_value(&complete)?;
        let review = wire["review"].as_object_mut().ok_or("review object")?;
        let _omitted = review.remove("proposerAgentId");
        assert!(serde_json::from_value::<LeadershipReviewQueryResponse>(wire).is_err());

        let mut wire = serde_json::to_value(&complete)?;
        let review = wire["review"].as_object_mut().ok_or("review object")?;
        let _omitted = review.remove("expectedLeadershipRevision");
        assert!(serde_json::from_value::<LeadershipReviewQueryResponse>(wire).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn maximum_statement_page_uses_the_review_response_envelope()
    -> Result<(), Box<dyn std::error::Error>> {
        let query = request();
        let mut large = response(&query, 64);
        let page = large.review.as_mut().ok_or("review missing")?;
        let body = "x\t".repeat(8_192);
        for (index, item) in page.statements.iter_mut().enumerate() {
            item.body = body.clone();
            item.sequence = u64::try_from(index + 1)?;
        }
        let body = serde_json::to_vec(&large)?;
        assert!(body.len() > 256 * 1024);
        assert!(body.len() < MAX_REVIEW_RESPONSE_BYTES);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let client = OrchestrationHttpClient::new(listener.local_addr()?, Duration::from_secs(2))?;
        let expected_path = format!(
            "POST /internal/v1/workspaces/{}/leadership/reviews/query HTTP/1.1",
            query.scope.workspace_id
        );
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut request_bytes = [0_u8; 2_048];
            let count = socket.read(&mut request_bytes).await?;
            let headers = String::from_utf8_lossy(&request_bytes[..count]);
            assert!(headers.starts_with(&expected_path));
            write_response(&mut socket, &body).await?;
            Ok::<_, std::io::Error>(())
        });
        let large_request = query.clone();
        let result = client
            .leadership_review(&HeaderValue::from_static("Bearer fixture"), &large_request)
            .await?;
        assert!(valid_response(&result, &large_request));
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn oversize_review_body_fails_and_releases_client_admission()
    -> Result<(), Box<dyn std::error::Error>> {
        let query = request();
        let valid_body = serde_json::to_vec(&response(&query, 0))?;
        let oversized_body = vec![b'x'; MAX_REVIEW_RESPONSE_BYTES + 1];
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let client = OrchestrationHttpClient::new(listener.local_addr()?, Duration::from_secs(5))?;
        let workspace_id = query.scope.workspace_id;
        let server = tokio::spawn(async move {
            for body in [oversized_body, valid_body] {
                let (mut socket, _) = listener.accept().await?;
                let mut request_bytes = [0_u8; 2_048];
                let count = socket.read(&mut request_bytes).await?;
                let headers = String::from_utf8_lossy(&request_bytes[..count]);
                assert!(headers.starts_with(&format!(
                    "POST /internal/v1/workspaces/{workspace_id}/leadership/reviews/query HTTP/1.1"
                )));
                let _ = write_response(&mut socket, &body).await;
            }
            Ok::<_, std::io::Error>(())
        });

        assert!(matches!(
            client
                .leadership_review(&HeaderValue::from_static("Bearer fixture"), &query)
                .await,
            Err(ClientError::Protocol)
        ));
        let result = client
            .leadership_review(&HeaderValue::from_static("Bearer fixture"), &query)
            .await?;
        assert!(valid_response(&result, &query));
        server.await??;
        Ok(())
    }

    async fn write_response(
        socket: &mut tokio::net::TcpStream,
        body: &[u8],
    ) -> Result<(), std::io::Error> {
        let headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        socket.write_all(headers.as_bytes()).await?;
        socket.write_all(body).await?;
        Ok(())
    }
}
