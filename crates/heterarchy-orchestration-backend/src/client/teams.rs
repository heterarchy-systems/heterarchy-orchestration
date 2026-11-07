//! Bounded, read-only Team query client.

use std::collections::HashSet;

use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use crate::wire::team_commands::{
    TeamCreateRequest, TeamCreateV2Request, TeamJoinRequest, TeamJoinV2Request,
    TeamMutationReaderV2Wire, TeamMutationResponse, TeamMutationV2Intent,
    TeamMutationV2ReadbackRequest, TeamMutationV2ReadbackResponse, TeamMutationV2Response,
};
use crate::wire::teams::{
    TeamGetQueryRequest, TeamGetQueryResponse, TeamListQueryRequest, TeamListQueryResponse,
    TeamMemberWire, TeamMutationV2Wire, TeamMutationWire, TeamWire,
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

    /// Read back an uncertain v1 create by its original v1 roster intent.
    /// The read query can use a current Workspace Session after credential
    /// or Session recovery; the immutable mutation receipt is still checked
    /// against the original v1 create payload. This route cannot confirm a v2
    /// capacity-bearing create or its source roster digest.
    ///
    /// # Errors
    /// Rejects mismatched selectors or any receipt that differs from the
    /// original create intent.
    pub async fn team_create_mutation_readback(
        &self,
        authorization: &HeaderValue,
        query: &TeamGetQueryRequest,
        original: &TeamCreateRequest,
    ) -> Result<Option<TeamMutationResponse>, ClientError> {
        super::team_commands::validate_create_request(original)?;
        validate_create_readback_query(query, original)?;
        let response = self.team_get(authorization, query).await?;
        let Some(mutation) = response.mutation else {
            return Ok(None);
        };
        if !valid_mutation(&mutation, query.scope.workspace_id, query.team_id)
            || !super::team_commands::valid_create_mutation(&mutation, original)
        {
            return Err(ClientError::Protocol);
        }
        Ok(Some(TeamMutationResponse {
            scope: response.scope,
            request_id: original.request_id,
            team_id: original.team_id,
            mutation,
        }))
    }

    /// Read back an uncertain v1 join by its original join intent. This route
    /// cannot confirm a v2 decision-slot-backed join and never retries it.
    ///
    /// # Errors
    /// Rejects mismatched selectors or any receipt that differs from the
    /// original join and expansion-slot intent.
    pub async fn team_join_mutation_readback(
        &self,
        authorization: &HeaderValue,
        query: &TeamGetQueryRequest,
        original: &TeamJoinRequest,
    ) -> Result<Option<TeamMutationResponse>, ClientError> {
        super::team_commands::validate_join_request(original)?;
        validate_join_readback_query(query, original)?;
        let response = self.team_get(authorization, query).await?;
        let Some(mutation) = response.mutation else {
            return Ok(None);
        };
        if !valid_mutation(&mutation, query.scope.workspace_id, query.team_id)
            || !super::team_commands::valid_join_mutation(&mutation, original)
        {
            return Err(ClientError::Protocol);
        }
        Ok(Some(TeamMutationResponse {
            scope: response.scope,
            request_id: original.request_id,
            team_id: original.team_id,
            mutation,
        }))
    }

    /// Read an exact v2 create receipt through the owner-scoped source route.
    /// This never falls back to the general v1 Team query.
    ///
    /// # Errors
    /// Rejects malformed original intent, an invalid current reader selector,
    /// or a source receipt that differs from the original v2 create.
    pub async fn team_create_mutation_readback_v2(
        &self,
        authorization: &HeaderValue,
        original: &TeamCreateV2Request,
        reader: TeamMutationReaderV2Wire,
    ) -> Result<Option<TeamMutationV2Response>, ClientError> {
        super::team_commands::validate_create_v2_request(original)?;
        validate_v2_reader(reader)?;
        let request = TeamMutationV2ReadbackRequest {
            reader,
            intent: TeamMutationV2Intent::Create(original.clone()),
        };
        let response: TeamMutationV2ReadbackResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v2/workspaces/{}/teams/{}/receipts/query",
                    original.scope.workspace_id, original.team_id
                ),
                Some(&request),
                None,
            )
            .await?;
        if response.scope != original.scope
            || response.request_id != original.request_id
            || response.team_id != original.team_id
        {
            return Err(ClientError::Protocol);
        }
        let Some(mutation) = response.receipt else {
            return Ok(None);
        };
        if !valid_v2_mutation(&mutation, original.scope.workspace_id, original.team_id)
            || !super::team_commands::valid_create_v2_mutation(&mutation, original)
        {
            return Err(ClientError::Protocol);
        }
        Ok(Some(TeamMutationV2Response {
            scope: response.scope,
            request_id: response.request_id,
            team_id: response.team_id,
            mutation,
        }))
    }

    /// Read an exact v2 join receipt through the owner-scoped source route.
    /// This never falls back to the general v1 Team query.
    ///
    /// # Errors
    /// Rejects malformed original intent, an invalid current reader selector,
    /// or a source receipt that differs from the original slot-backed join.
    pub async fn team_join_mutation_readback_v2(
        &self,
        authorization: &HeaderValue,
        original: &TeamJoinV2Request,
        reader: TeamMutationReaderV2Wire,
    ) -> Result<Option<TeamMutationV2Response>, ClientError> {
        super::team_commands::validate_join_v2_request(original)?;
        validate_v2_reader(reader)?;
        let request = TeamMutationV2ReadbackRequest {
            reader,
            intent: TeamMutationV2Intent::Join(original.clone()),
        };
        let response: TeamMutationV2ReadbackResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v2/workspaces/{}/teams/{}/receipts/query",
                    original.scope.workspace_id, original.team_id
                ),
                Some(&request),
                None,
            )
            .await?;
        if response.scope != original.scope
            || response.request_id != original.request_id
            || response.team_id != original.team_id
        {
            return Err(ClientError::Protocol);
        }
        let Some(mutation) = response.receipt else {
            return Ok(None);
        };
        if !valid_v2_mutation(&mutation, original.scope.workspace_id, original.team_id)
            || !mutation
                .expansion_join_source
                .as_ref()
                .is_some_and(|source| super::team_commands::valid_join_v2_source(source, original))
            || !super::team_commands::valid_join_v2_mutation(&mutation, original)
        {
            return Err(ClientError::Protocol);
        }
        Ok(Some(TeamMutationV2Response {
            scope: response.scope,
            request_id: response.request_id,
            team_id: response.team_id,
            mutation,
        }))
    }
}

fn validate_v2_reader(reader: TeamMutationReaderV2Wire) -> Result<(), ClientError> {
    if reader.activity_id.is_nil() || reader.session_id.is_nil() {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_create_readback_query(
    query: &TeamGetQueryRequest,
    original: &TeamCreateRequest,
) -> Result<(), ClientError> {
    validate_get_request(query)?;
    if query.scope.workspace_id != original.scope.workspace_id
        || query.team_id != original.team_id
        || query.request_id != Some(original.request_id)
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_join_readback_query(
    query: &TeamGetQueryRequest,
    original: &TeamJoinRequest,
) -> Result<(), ClientError> {
    validate_get_request(query)?;
    if query.scope.workspace_id != original.scope.workspace_id
        || query.team_id != original.team_id
        || query.request_id != Some(original.request_id)
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
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

pub(super) fn valid_v2_mutation(
    mutation: &TeamMutationV2Wire,
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
        && mutation.capacity.as_ref().is_none_or(|capacity| {
            capacity.workspace_id == workspace_id
                && capacity.team_id == team_id
                && capacity.max_members <= 8
                && capacity.revision > 0
                && capacity.leadership_revision > 0
                && capacity.recorded_at_ms >= mutation.team.created_at_ms
                && capacity.recorded_at_ms <= mutation.team.updated_at_ms
                && valid_v2_capacity_actor(&capacity.actor)
        })
        && mutation
            .initial_roster_digest_v1
            .as_deref()
            .is_none_or(valid_v1_roster_digest)
        && mutation.expansion_slot.is_none_or(|slot| {
            !slot.decision_id.is_nil()
                && slot.ordinal < 8
                && mutation
                    .membership
                    .as_ref()
                    .is_some_and(|membership| membership.state == "joined")
        })
        && mutation
            .expansion_join_source
            .as_ref()
            .is_none_or(|source| {
                !source.activity_id.is_nil()
                    && !source.agent_id.is_nil()
                    && !source.session_id.is_nil()
                    && !source.decision_id.is_nil()
                    && source.ordinal < 8
                    && (1..=256).contains(&source.role.len())
                    && !source.role.chars().any(char::is_control)
                    && mutation.expansion_slot.is_some_and(|slot| {
                        slot.decision_id == source.decision_id && slot.ordinal == source.ordinal
                    })
                    && mutation
                        .membership
                        .as_ref()
                        .is_some_and(|membership| membership.state == "joined")
            })
        && matches!(
            (
                mutation.capacity.is_some(),
                mutation.initial_roster_digest_v1.is_some()
            ),
            (true, true) | (false, false)
        )
        && unique_member_ids(&mutation.initial_members)
}

fn valid_v2_capacity_actor(
    actor: &crate::wire::team_expansion::TeamExpansionActorReceiptWire,
) -> bool {
    !actor.activity_id.is_nil()
        && !actor.agent_id.is_nil()
        && !actor.session_id.is_nil()
        && !actor.participation_id.is_nil()
        && !actor.profile_id.is_nil()
        && !actor.participant_id.is_nil()
        && (1..=128).contains(&actor.display_name.len())
        && !actor.display_name.chars().any(char::is_control)
}

fn valid_v1_roster_digest(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
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
