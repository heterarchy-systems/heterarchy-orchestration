//! Bounded, read-only Team query client.

use std::collections::HashSet;

use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use crate::wire::teams::{
    TeamGetQueryRequest, TeamGetQueryResponse, TeamListQueryRequest, TeamListQueryResponse,
    TeamMemberWire, TeamMutationWire, TeamWire,
};

use super::{ClientError, OrchestrationHttpClient};

const MAX_TEAM_RESULTS: u32 = 64;
const MAX_TEAM_MEMBERS: usize = 64;
const MAX_LOCATOR_BYTES: usize = 4_096;

impl OrchestrationHttpClient {
    /// Read one bounded Team page through the caller's original credential.
    ///
    /// This is a pure query; an uncertain exchange has no mutation identity
    /// and is never converted into mutation readback.
    ///
    /// # Errors
    /// Rejects invalid scope/selectors, unbounded or malformed responses,
    /// typed service rejection, and bounded transport failures.
    pub async fn team_list(
        &self,
        authorization: &HeaderValue,
        request: &TeamListQueryRequest,
    ) -> Result<TeamListQueryResponse, ClientError> {
        validate_list_request(request)?;
        let path = format!(
            "/internal/v1/workspaces/{}/teams/query",
            request.scope.workspace_id
        );
        let response: TeamListQueryResponse = self
            .call(authorization, Method::POST, path, Some(request), None)
            .await?;
        if !valid_list_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Read one complete Team and bounded membership page, optionally reading
    /// back one exact historical mutation receipt.
    ///
    /// The receipt selector is read-only and does not make the request a
    /// mutation or enable uncertain-mutation recovery.
    ///
    /// # Errors
    /// Rejects invalid scope/identities, malformed snapshots or receipt
    /// bindings, typed service rejection, and bounded transport failures.
    pub async fn team_get(
        &self,
        authorization: &HeaderValue,
        request: &TeamGetQueryRequest,
    ) -> Result<TeamGetQueryResponse, ClientError> {
        validate_get_request(request)?;
        let path = format!(
            "/internal/v1/workspaces/{}/teams/{}/query",
            request.scope.workspace_id, request.team_id
        );
        let response: TeamGetQueryResponse = self
            .call(authorization, Method::POST, path, Some(request), None)
            .await?;
        if !valid_get_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn validate_list_request(request: &TeamListQueryRequest) -> Result<(), ClientError> {
    if !valid_scope(&request.scope)
        || request.session_id.is_some_and(|id| id.is_nil())
        || !(1..=MAX_TEAM_RESULTS).contains(&request.limit)
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_get_request(request: &TeamGetQueryRequest) -> Result<(), ClientError> {
    if !valid_scope(&request.scope)
        || request.session_id.is_some_and(|id| id.is_nil())
        || request.team_id.is_nil()
        || request.request_id.is_some_and(|id| id.is_nil())
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

pub(super) fn valid_scope(scope: &crate::wire::WorkspaceQueryScope) -> bool {
    !scope.workspace_id.is_nil()
        && !scope.location_id.is_nil()
        && scope.location_revision > 0
        && scope.workspace_revision > 0
        && !scope.locator.is_empty()
        && scope.locator.len() <= MAX_LOCATOR_BYTES
        && !scope.locator.chars().any(char::is_control)
}

fn valid_list_response(response: &TeamListQueryResponse, request: &TeamListQueryRequest) -> bool {
    let limit = usize::try_from(request.limit).ok();
    response.scope == request.scope
        && response.session_id == request.session_id
        && response.limit == request.limit
        && limit.is_some_and(|limit| response.teams.len() <= limit)
        && (!response.truncated || limit == Some(response.teams.len()))
        && response
            .teams
            .iter()
            .all(|team| valid_team(team, request.scope.workspace_id, None))
        && unique_team_ids(&response.teams)
        && response
            .teams
            .windows(2)
            .all(|pair| pair[0].team_id.as_bytes() < pair[1].team_id.as_bytes())
}

fn valid_get_response(response: &TeamGetQueryResponse, request: &TeamGetQueryRequest) -> bool {
    if response.scope != request.scope
        || response.session_id != request.session_id
        || response.team_id != request.team_id
        || response.request_id != request.request_id
        || response.members.len() > MAX_TEAM_MEMBERS
        || (response.members_truncated && response.members.len() != MAX_TEAM_MEMBERS)
        || response.team.as_ref().is_some_and(|team| {
            !valid_team(team, request.scope.workspace_id, Some(request.team_id))
        })
        || response
            .members
            .iter()
            .any(|member| !valid_member(member, request.scope.workspace_id, request.team_id))
        || !unique_member_ids(&response.members)
        || (response.team.is_none() && (!response.members.is_empty() || response.members_truncated))
        || (request.request_id.is_none() && response.mutation.is_some())
    {
        return false;
    }
    response.mutation.as_ref().is_none_or(|mutation| {
        valid_mutation(mutation, request.scope.workspace_id, request.team_id)
    })
}

fn valid_team(team: &TeamWire, workspace_id: Uuid, team_id: Option<Uuid>) -> bool {
    team_id.is_none_or(|expected| team.team_id == expected)
        && !team.team_id.is_nil()
        && team.workspace_id == workspace_id
        && (1..=128).contains(&team.name.len())
        && team.purpose.len() <= 2_048
        && matches!(team.state.as_str(), "active" | "retired")
        && team.revision > 0
        && team.created_at_ms >= 0
        && team.updated_at_ms >= team.created_at_ms
        && team.formed_at_ms.is_none_or(|formed_at| {
            formed_at >= team.created_at_ms && formed_at <= team.updated_at_ms
        })
        && valid_creator(&team.created_by)
}

fn valid_creator(creator: &crate::wire::teams::TeamCreatorWire) -> bool {
    (1..=256).contains(&creator.key.len())
        && (1..=128).contains(&creator.display_name.len())
        && creator.participation_id.is_none_or(|id| !id.is_nil())
        && creator.profile_id.is_none_or(|id| !id.is_nil())
        && creator.participant_id.is_none_or(|id| !id.is_nil())
        && creator.session_id.is_none_or(|id| !id.is_nil())
}

fn valid_member(member: &TeamMemberWire, workspace_id: Uuid, team_id: Uuid) -> bool {
    member.workspace_id == workspace_id
        && member.team_id == team_id
        && !member.participation_id.is_nil()
        && member.profile_id.is_none_or(|id| !id.is_nil())
        && member
            .display_name
            .as_ref()
            .is_none_or(|name| (1..=128).contains(&name.len()))
        && member
            .role
            .as_ref()
            .is_none_or(|role| (1..=256).contains(&role.len()))
        && matches!(member.state.as_str(), "joined" | "left")
        && member.revision > 0
        && member.joined_at_ms >= 0
        && member
            .left_at_ms
            .is_none_or(|left_at| left_at >= member.joined_at_ms)
}

pub(super) fn valid_mutation(
    mutation: &TeamMutationWire,
    workspace_id: Uuid,
    team_id: Uuid,
) -> bool {
    valid_team(&mutation.team, workspace_id, Some(team_id))
        && mutation
            .membership
            .as_ref()
            .is_none_or(|member| valid_member(member, workspace_id, team_id))
        && mutation.initial_members.len() <= MAX_TEAM_MEMBERS
        && mutation
            .initial_members
            .iter()
            .all(|member| valid_member(member, workspace_id, team_id))
        && unique_member_ids(&mutation.initial_members)
}

fn unique_team_ids(teams: &[TeamWire]) -> bool {
    let mut ids = HashSet::with_capacity(teams.len());
    teams.iter().all(|team| ids.insert(team.team_id))
}

fn unique_member_ids(members: &[TeamMemberWire]) -> bool {
    let mut ids = HashSet::with_capacity(members.len());
    members
        .iter()
        .all(|member| ids.insert(member.participation_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::leadership::WorkspaceQueryScope;
    use crate::wire::teams::{TeamCreatorWire, TeamMutationWire};

    fn scope() -> WorkspaceQueryScope {
        WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 3,
            workspace_revision: 4,
            locator: "workspace".to_owned(),
        }
    }

    fn team(team_id: Uuid) -> TeamWire {
        TeamWire {
            team_id,
            workspace_id: scope().workspace_id,
            name: "review".to_owned(),
            purpose: "review source".to_owned(),
            state: "active".to_owned(),
            revision: 7,
            created_by: TeamCreatorWire {
                key: "creator-key".to_owned(),
                display_name: "Creator".to_owned(),
                participation_id: Some(Uuid::from_u128(3)),
                profile_id: Some(Uuid::from_u128(4)),
                participant_id: Some(Uuid::from_u128(5)),
                session_id: Some(Uuid::from_u128(6)),
            },
            created_at_ms: 10,
            updated_at_ms: 12,
            formed_at_ms: Some(11),
        }
    }

    fn member(team_id: Uuid, participation_id: Uuid) -> TeamMemberWire {
        TeamMemberWire {
            workspace_id: scope().workspace_id,
            team_id,
            participation_id,
            profile_id: Some(Uuid::from_u128(7)),
            display_name: Some("Member".to_owned()),
            role: Some("reviewer".to_owned()),
            state: "joined".to_owned(),
            revision: 2,
            joined_at_ms: 9,
            left_at_ms: None,
        }
    }

    fn get_request() -> TeamGetQueryRequest {
        TeamGetQueryRequest {
            scope: scope(),
            session_id: Some(Uuid::from_u128(8)),
            team_id: Uuid::from_u128(9),
            request_id: Some(Uuid::from_u128(10)),
        }
    }

    fn get_response(request: &TeamGetQueryRequest) -> TeamGetQueryResponse {
        let current = team(request.team_id);
        let receipt = TeamMutationWire {
            team: current.clone(),
            membership: None,
            initial_members: vec![member(request.team_id, Uuid::from_u128(11))],
            created: true,
            replayed: true,
        };
        TeamGetQueryResponse {
            scope: request.scope.clone(),
            session_id: request.session_id,
            team_id: request.team_id,
            request_id: request.request_id,
            team: Some(current),
            members: vec![member(request.team_id, Uuid::from_u128(12))],
            members_truncated: false,
            mutation: Some(receipt),
        }
    }

    #[test]
    fn list_query_binds_scope_selector_and_bounded_ordered_team_records() {
        let request = TeamListQueryRequest {
            scope: scope(),
            session_id: Some(Uuid::from_u128(8)),
            limit: 2,
        };
        let response = TeamListQueryResponse {
            scope: request.scope.clone(),
            session_id: request.session_id,
            limit: request.limit,
            teams: vec![team(Uuid::from_u128(9)), team(Uuid::from_u128(10))],
            truncated: true,
        };
        assert!(validate_list_request(&request).is_ok());
        assert!(valid_list_response(&response, &request));

        let mut mismatch = response.clone();
        mismatch.scope.workspace_revision += 1;
        assert!(!valid_list_response(&mismatch, &request));
        let mut duplicate = response;
        duplicate.teams[1].team_id = duplicate.teams[0].team_id;
        assert!(!valid_list_response(&duplicate, &request));

        let mut invalid_request = request;
        invalid_request.limit = 65;
        assert_eq!(
            validate_list_request(&invalid_request),
            Err(ClientError::InvalidRequest)
        );
    }

    #[test]
    fn get_query_preserves_full_projection_and_exact_historical_receipt_binding()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = get_request();
        let response = get_response(&request);
        assert!(validate_get_request(&request).is_ok());
        assert!(valid_get_response(&response, &request));

        let mut wrong_team = response.clone();
        wrong_team.team_id = Uuid::from_u128(13);
        assert!(!valid_get_response(&wrong_team, &request));
        let mut wrong_receipt_context = response.clone();
        wrong_receipt_context.request_id = Some(Uuid::from_u128(14));
        assert!(!valid_get_response(&wrong_receipt_context, &request));

        let mut invalid_creator = response.clone();
        invalid_creator
            .team
            .as_mut()
            .ok_or_else(|| std::io::Error::other("team present"))?
            .created_by
            .session_id = Some(Uuid::nil());
        assert!(!valid_get_response(&invalid_creator, &request));

        let mut oversized_page = response.clone();
        oversized_page.members = (0..65)
            .map(|index| member(request.team_id, Uuid::from_u128(100 + index)))
            .collect();
        assert!(!valid_get_response(&oversized_page, &request));

        let mut wrong_roster_scope = response;
        let receipt = wrong_roster_scope
            .mutation
            .as_mut()
            .ok_or_else(|| std::io::Error::other("receipt present"))?;
        let roster_member = receipt
            .initial_members
            .first_mut()
            .ok_or_else(|| std::io::Error::other("initial roster member"))?;
        roster_member.team_id = Uuid::from_u128(15);
        assert!(!valid_get_response(&wrong_roster_scope, &request));
        Ok(())
    }

    #[test]
    fn get_query_keeps_absence_and_member_paging_semantics_explicit() {
        let request = get_request();
        let mut response = get_response(&request);
        response.team = None;
        response.members.clear();
        response.members_truncated = false;
        assert!(valid_get_response(&response, &request));

        response
            .members
            .push(member(request.team_id, Uuid::from_u128(12)));
        assert!(!valid_get_response(&response, &request));

        let mut full_page = get_response(&request);
        full_page.members = (0..64)
            .map(|index| member(request.team_id, Uuid::from_u128(100 + index)))
            .collect();
        full_page.members_truncated = true;
        assert!(valid_get_response(&full_page, &request));
        full_page.members_truncated = false;
        assert!(valid_get_response(&full_page, &request));

        let mut invalid_selector = request;
        invalid_selector.session_id = Some(Uuid::nil());
        assert_eq!(
            validate_get_request(&invalid_selector),
            Err(ClientError::InvalidRequest)
        );
    }
}
