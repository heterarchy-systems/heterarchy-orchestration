//! Bounded HTTP client for source-scoped Team mutations and full receipts.

use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use crate::wire::team_commands::{
    TeamCreateRequest, TeamCreateV2Request, TeamJoinRequest, TeamJoinV2Request, TeamLeaveRequest,
    TeamMutationResponse, TeamMutationV2Response, TeamRetireRequest, TeamRosterMemberRequest,
};
use crate::wire::teams::TeamMutationWire;

use super::teams::{valid_mutation, valid_scope, valid_v2_mutation};
use super::{ClientError, OrchestrationHttpClient, ReadbackTarget, require_identity};

const MAX_TEAM_NAME_BYTES: usize = 128;
const MAX_TEAM_PURPOSE_BYTES: usize = 2_048;
const MAX_TEAM_ROLE_BYTES: usize = 256;
const MIN_INITIAL_TEAM_MEMBERS: usize = 2;
const MAX_INITIAL_TEAM_MEMBERS: usize = 64;

impl OrchestrationHttpClient {
    /// Create or exactly replay one source-scoped Team and its frozen roster.
    ///
    /// An uncertain exchange carries the exact Workspace/Team/request receipt
    /// identity. The client never retries the mutation automatically.
    ///
    /// # Errors
    /// Rejects invalid scope, identity, text, leadership revision, or roster;
    /// returns typed service rejection, bounded transport failure, or an
    /// unknown outcome with exact readback identity.
    pub async fn create_team(
        &self,
        authorization: &HeaderValue,
        request: &TeamCreateRequest,
    ) -> Result<TeamMutationResponse, ClientError> {
        validate_create_request(request)?;
        let readback = mutation_readback(
            request.scope.workspace_id,
            request.team_id,
            request.request_id,
        );
        let response: TeamMutationResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/teams/create",
                    request.scope.workspace_id
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_create_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Create one Team under the explicit per-Team cap contract.
    ///
    /// An uncertain result retains the complete v2 intent for owner-scoped
    /// readback; this method never retries the mutation.
    ///
    /// # Errors
    /// Rejects invalid scope/identities/cap/roster, typed service rejection,
    /// bounded transport failure, or an unknown outcome with exact v2 intent.
    pub async fn create_team_v2(
        &self,
        authorization: &HeaderValue,
        request: &TeamCreateV2Request,
    ) -> Result<TeamMutationV2Response, ClientError> {
        validate_create_v2_request(request)?;
        let readback = ReadbackTarget::TeamCreateV2Mutation(Box::new(request.clone()));
        let response: TeamMutationV2Response = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v2/workspaces/{}/teams/create",
                    request.scope.workspace_id
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_create_v2_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Join or exactly replay one logical Session's Team membership mutation.
    ///
    /// An uncertain exchange carries the exact Workspace/Team/request receipt
    /// identity. The client never retries the mutation automatically.
    ///
    /// # Errors
    /// Rejects invalid scope, identity, revision, or role; returns typed service
    /// rejection, bounded transport failure, or an unknown outcome with exact
    /// readback identity.
    pub async fn join_team(
        &self,
        authorization: &HeaderValue,
        request: &TeamJoinRequest,
    ) -> Result<TeamMutationResponse, ClientError> {
        validate_join_request(request)?;
        let readback = mutation_readback(
            request.scope.workspace_id,
            request.team_id,
            request.request_id,
        );
        let response: TeamMutationResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/teams/{}/join",
                    request.scope.workspace_id, request.team_id
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_join_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Join one Team by consuming an exact Orchestrator decision slot.
    ///
    /// An uncertain result retains the complete v2 intent for owner-scoped
    /// readback; this method never retries or reassigns the slot.
    ///
    /// # Errors
    /// Rejects invalid scope/identities/revision/slot, typed service rejection,
    /// bounded transport failure, or an unknown outcome with exact v2 intent.
    pub async fn join_team_v2(
        &self,
        authorization: &HeaderValue,
        request: &TeamJoinV2Request,
    ) -> Result<TeamMutationV2Response, ClientError> {
        validate_join_v2_request(request)?;
        let readback = ReadbackTarget::TeamJoinV2Mutation(Box::new(request.clone()));
        let response: TeamMutationV2Response = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v2/workspaces/{}/teams/{}/join",
                    request.scope.workspace_id, request.team_id
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_join_v2_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Leave or exactly replay one logical Session's Team membership mutation.
    ///
    /// An uncertain exchange carries the exact Workspace/Team/request receipt
    /// identity. The client never retries the mutation automatically.
    ///
    /// # Errors
    /// Rejects invalid scope, identity, or revision; returns typed service
    /// rejection, bounded transport failure, or an unknown outcome with exact
    /// readback identity.
    pub async fn leave_team(
        &self,
        authorization: &HeaderValue,
        request: &TeamLeaveRequest,
    ) -> Result<TeamMutationResponse, ClientError> {
        validate_leave_request(request)?;
        let readback = mutation_readback(
            request.scope.workspace_id,
            request.team_id,
            request.request_id,
        );
        let response: TeamMutationResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/teams/{}/leave",
                    request.scope.workspace_id, request.team_id
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_leave_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Retire or exactly replay one Team mutation through the operator route.
    ///
    /// An uncertain exchange carries the exact Workspace/Team/request receipt
    /// identity. The client never retries the mutation automatically.
    ///
    /// # Errors
    /// Rejects invalid scope, identity, or revision; returns typed service
    /// rejection, bounded transport failure, or an unknown outcome with exact
    /// readback identity.
    pub async fn retire_team(
        &self,
        authorization: &HeaderValue,
        request: &TeamRetireRequest,
    ) -> Result<TeamMutationResponse, ClientError> {
        validate_retire_request(request)?;
        let readback = mutation_readback(
            request.scope.workspace_id,
            request.team_id,
            request.request_id,
        );
        let response: TeamMutationResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/teams/{}/retire",
                    request.scope.workspace_id, request.team_id
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_retire_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }
}

fn mutation_readback(workspace_id: Uuid, team_id: Uuid, request_id: Uuid) -> ReadbackTarget {
    ReadbackTarget::TeamMutation {
        workspace_id,
        team_id,
        request_id,
    }
}

pub(super) fn validate_create_request(request: &TeamCreateRequest) -> Result<(), ClientError> {
    validate_common_request(&request.scope, request.request_id, request.team_id)?;
    require_identity(request.activity_id)?;
    require_identity(request.session_id)?;
    if request.expected_sender_leadership_revision < 1
        || !valid_text(&request.name, MAX_TEAM_NAME_BYTES)
        || !valid_text(&request.purpose, MAX_TEAM_PURPOSE_BYTES)
        || !(MIN_INITIAL_TEAM_MEMBERS..=MAX_INITIAL_TEAM_MEMBERS)
            .contains(&request.initial_members.len())
        || !valid_initial_roster(&request.initial_members)
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

pub(super) fn validate_join_request(request: &TeamJoinRequest) -> Result<(), ClientError> {
    validate_common_request(&request.scope, request.request_id, request.team_id)?;
    require_identity(request.activity_id)?;
    require_identity(request.session_id)?;
    require_expected_team_revision(request.expected_team_revision)?;
    if !valid_text(&request.role, MAX_TEAM_ROLE_BYTES) {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

pub(super) fn validate_create_v2_request(request: &TeamCreateV2Request) -> Result<(), ClientError> {
    let v1_shape = TeamCreateRequest {
        scope: request.scope.clone(),
        request_id: request.request_id,
        team_id: request.team_id,
        activity_id: request.activity_id,
        session_id: request.session_id,
        expected_sender_leadership_revision: request.expected_sender_leadership_revision,
        name: request.name.clone(),
        purpose: request.purpose.clone(),
        initial_members: request.initial_members.clone(),
    };
    validate_create_request(&v1_shape)?;
    if request.max_members > 8 || usize::from(request.max_members) < request.initial_members.len() {
        return Err(ClientError::InvalidRequest);
    }
    expected_roster_digest_v1(request).map(|_| ())
}

pub(super) fn validate_join_v2_request(request: &TeamJoinV2Request) -> Result<(), ClientError> {
    validate_join_request(&TeamJoinRequest {
        scope: request.scope.clone(),
        request_id: request.request_id,
        team_id: request.team_id,
        activity_id: request.activity_id,
        session_id: request.session_id,
        expected_team_revision: request.expected_team_revision,
        role: request.role.clone(),
    })?;
    if request.expansion_slot.decision_id.is_nil() || request.expansion_slot.ordinal >= 8 {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

pub(super) fn expected_roster_digest_v1(
    request: &TeamCreateV2Request,
) -> Result<String, ClientError> {
    let inputs = request
        .initial_members
        .iter()
        .map(|member| {
            heterarchy_orchestration_operations::team_expansion::TeamRosterDigestInput::new(
                forge_domain::ActivityId::from_uuid(member.activity_id),
                forge_domain::AgentSessionId::from_uuid(member.session_id),
                member.role.as_str(),
            )
        })
        .collect::<Vec<_>>();
    heterarchy_orchestration_operations::team_expansion::team_create_roster_digest(&inputs)
        .map(heterarchy_orchestration_operations::team_expansion::TeamCreateRosterDigest::to_hex)
        .map_err(|_| ClientError::InvalidRequest)
}

fn validate_leave_request(request: &TeamLeaveRequest) -> Result<(), ClientError> {
    validate_common_request(&request.scope, request.request_id, request.team_id)?;
    require_identity(request.activity_id)?;
    require_identity(request.session_id)?;
    require_expected_team_revision(request.expected_team_revision)
}

fn validate_retire_request(request: &TeamRetireRequest) -> Result<(), ClientError> {
    validate_common_request(&request.scope, request.request_id, request.team_id)?;
    require_expected_team_revision(request.expected_team_revision)
}

fn validate_common_request(
    scope: &crate::wire::WorkspaceQueryScope,
    request_id: Uuid,
    team_id: Uuid,
) -> Result<(), ClientError> {
    if !valid_scope(scope) {
        return Err(ClientError::InvalidRequest);
    }
    require_identity(request_id)?;
    require_identity(team_id)
}

fn require_expected_team_revision(revision: u64) -> Result<(), ClientError> {
    if next_team_revision(revision).is_none() {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn next_team_revision(revision: u64) -> Option<u64> {
    if revision == 0 {
        return None;
    }
    let stored = i64::try_from(revision).ok()?;
    let next = stored.checked_add(1)?;
    u64::try_from(next).ok()
}

fn valid_initial_roster(members: &[TeamRosterMemberRequest]) -> bool {
    let mut identities = std::collections::HashSet::with_capacity(members.len());
    members.iter().all(|member| {
        !member.activity_id.is_nil()
            && !member.session_id.is_nil()
            && valid_text(&member.role, MAX_TEAM_ROLE_BYTES)
            && identities.insert((member.activity_id, member.session_id))
    })
}

fn valid_text(value: &str, max_bytes: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max_bytes
}

pub(super) fn valid_create_response(
    response: &TeamMutationResponse,
    request: &TeamCreateRequest,
) -> bool {
    valid_common_response(
        response,
        &request.scope,
        request.request_id,
        request.team_id,
    ) && valid_create_mutation(&response.mutation, request)
}

pub(super) fn valid_create_mutation(
    mutation: &TeamMutationWire,
    request: &TeamCreateRequest,
) -> bool {
    mutation.created
        && mutation.team.revision == 1
        && mutation.team.state == "active"
        && mutation.team.name == request.name
        && mutation.team.purpose == request.purpose
        && mutation.team.created_by.session_id == Some(request.session_id)
        && mutation.membership.is_none()
        && same_initial_roster_roles(&request.initial_members, &mutation.initial_members)
}

pub(super) fn valid_join_response(
    response: &TeamMutationResponse,
    request: &TeamJoinRequest,
) -> bool {
    valid_common_response(
        response,
        &request.scope,
        request.request_id,
        request.team_id,
    ) && valid_join_mutation(&response.mutation, request)
}

pub(super) fn valid_join_mutation(mutation: &TeamMutationWire, request: &TeamJoinRequest) -> bool {
    next_team_revision(request.expected_team_revision)
        .is_some_and(|revision| mutation.team.revision == revision)
        && mutation.team.team_id == request.team_id
        && mutation.team.state == "active"
        && !mutation.created
        && mutation.membership.as_ref().is_some_and(|membership| {
            membership.state == "joined"
                && membership.role.as_deref() == Some(request.role.as_str())
        })
        && mutation.initial_members.is_empty()
}

pub(super) fn valid_create_v2_response(
    response: &TeamMutationV2Response,
    request: &TeamCreateV2Request,
) -> bool {
    response.scope == request.scope
        && response.request_id == request.request_id
        && response.team_id == request.team_id
        && valid_create_v2_mutation(&response.mutation, request)
}

pub(super) fn valid_create_v2_mutation(
    mutation: &crate::wire::teams::TeamMutationV2Wire,
    request: &TeamCreateV2Request,
) -> bool {
    let Some(expected_digest) = expected_roster_digest_v1(request).ok() else {
        return false;
    };
    valid_v2_mutation(mutation, request.scope.workspace_id, request.team_id)
        && mutation.created
        && mutation.team.team_id == request.team_id
        && mutation.team.workspace_id == request.scope.workspace_id
        && mutation.team.revision == 1
        && mutation.team.state == "active"
        && mutation.team.name == request.name
        && mutation.team.purpose == request.purpose
        && mutation.team.created_by.session_id == Some(request.session_id)
        && mutation.capacity.as_ref().is_some_and(|capacity| {
            capacity.team_id == request.team_id
                && capacity.workspace_id == request.scope.workspace_id
                && capacity.max_members == request.max_members
                && capacity.revision == 1
                && capacity.recorded_at_ms >= mutation.team.created_at_ms
                && capacity.recorded_at_ms <= mutation.team.updated_at_ms
                && capacity.leadership_revision == request.expected_sender_leadership_revision
                && capacity.actor.activity_id == request.activity_id
                && capacity.actor.agent_id
                    == heterarchy_orchestration_runtime::leadership::member_agent_id(
                        request.scope.workspace_id,
                        request.activity_id,
                        request.session_id,
                    )
                    .into_uuid()
                && capacity.actor.session_id == request.session_id
        })
        && mutation.initial_roster_digest_v1.as_deref() == Some(expected_digest.as_str())
        && mutation.membership.is_none()
        && mutation.expansion_slot.is_none()
        && mutation.expansion_join_source.is_none()
        && same_initial_roster_roles(&request.initial_members, &mutation.initial_members)
}

pub(super) fn valid_join_v2_response(
    response: &TeamMutationV2Response,
    request: &TeamJoinV2Request,
) -> bool {
    response.scope == request.scope
        && response.request_id == request.request_id
        && response.team_id == request.team_id
        && valid_join_v2_mutation(&response.mutation, request)
}

pub(super) fn valid_join_v2_mutation(
    mutation: &crate::wire::teams::TeamMutationV2Wire,
    request: &TeamJoinV2Request,
) -> bool {
    valid_v2_mutation(mutation, request.scope.workspace_id, request.team_id)
        && next_team_revision(request.expected_team_revision)
            .is_some_and(|revision| mutation.team.revision == revision)
        && mutation.team.team_id == request.team_id
        && mutation.team.workspace_id == request.scope.workspace_id
        && mutation.team.state == "active"
        && mutation.membership.as_ref().is_some_and(|membership| {
            membership.state == "joined"
                && membership.role.as_deref() == Some(request.role.as_str())
                && crate::wire::teams::v2_join_created_flag_matches_membership_revision(
                    mutation.created,
                    membership.revision,
                )
        })
        && mutation.initial_members.is_empty()
        && mutation.capacity.is_none()
        && mutation.initial_roster_digest_v1.is_none()
        && mutation.expansion_slot.is_some_and(|slot| {
            slot.decision_id == request.expansion_slot.decision_id
                && slot.ordinal == request.expansion_slot.ordinal
        })
        && mutation
            .expansion_join_source
            .as_ref()
            .is_some_and(|source| valid_join_v2_source(source, request))
}

pub(super) fn valid_join_v2_source(
    source: &crate::wire::teams::TeamExpansionJoinSourceV2Wire,
    request: &TeamJoinV2Request,
) -> bool {
    source.activity_id == request.activity_id
        && source.session_id == request.session_id
        && source.agent_id
            == heterarchy_orchestration_runtime::leadership::member_agent_id(
                request.scope.workspace_id,
                request.activity_id,
                request.session_id,
            )
            .into_uuid()
        && source.decision_id == request.expansion_slot.decision_id
        && source.ordinal == request.expansion_slot.ordinal
        && source.role == request.role
}

fn valid_leave_response(response: &TeamMutationResponse, request: &TeamLeaveRequest) -> bool {
    valid_common_response(
        response,
        &request.scope,
        request.request_id,
        request.team_id,
    ) && next_team_revision(request.expected_team_revision)
        .is_some_and(|revision| response.mutation.team.revision == revision)
        && response.mutation.team.state == "active"
        && response
            .mutation
            .membership
            .as_ref()
            .is_some_and(|membership| membership.state == "left")
        && response.mutation.initial_members.is_empty()
        && !response.mutation.created
}

fn valid_retire_response(response: &TeamMutationResponse, request: &TeamRetireRequest) -> bool {
    valid_common_response(
        response,
        &request.scope,
        request.request_id,
        request.team_id,
    ) && next_team_revision(request.expected_team_revision)
        .is_some_and(|revision| response.mutation.team.revision == revision)
        && response.mutation.team.state == "retired"
        && response.mutation.membership.is_none()
        && response.mutation.initial_members.is_empty()
        && !response.mutation.created
}

fn valid_common_response(
    response: &TeamMutationResponse,
    scope: &crate::wire::WorkspaceQueryScope,
    request_id: Uuid,
    team_id: Uuid,
) -> bool {
    response.scope == *scope
        && response.request_id == request_id
        && response.team_id == team_id
        && valid_mutation(&response.mutation, scope.workspace_id, team_id)
}

fn same_initial_roster_roles(
    requested: &[TeamRosterMemberRequest],
    receipt: &[crate::wire::teams::TeamMemberWire],
) -> bool {
    if requested.len() != receipt.len() {
        return false;
    }
    let mut requested_roles = requested
        .iter()
        .map(|member| member.role.as_str())
        .collect::<Vec<_>>();
    let Some(mut receipt_roles) = receipt
        .iter()
        .map(|member| member.role.as_deref())
        .collect::<Option<Vec<_>>>()
    else {
        return false;
    };
    requested_roles.sort_unstable();
    receipt_roles.sort_unstable();
    requested_roles == receipt_roles
}

#[cfg(test)]
mod tests {
    use std::{io, time::Duration};

    use super::super::{MAX_HEADER_BYTES, MAX_REQUEST_BYTES};
    use super::*;
    use crate::wire::leadership::WorkspaceQueryScope;
    use crate::wire::team_commands::{TeamMutationReaderV2Wire, TeamMutationV2ReadbackResponse};
    use crate::wire::teams::{TeamCreatorWire, TeamMemberWire, TeamMutationWire, TeamWire};
    use hyper::header::HeaderValue;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const CREDENTIAL: &str = "Bearer team-command-fixture-secret";

    fn scope() -> WorkspaceQueryScope {
        WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 3,
            workspace_revision: 4,
            locator: "workspace".to_owned(),
        }
    }

    fn create_request() -> TeamCreateRequest {
        TeamCreateRequest {
            scope: scope(),
            request_id: Uuid::from_u128(3),
            team_id: Uuid::from_u128(4),
            activity_id: Uuid::from_u128(5),
            session_id: Uuid::from_u128(6),
            expected_sender_leadership_revision: 1,
            name: "review".to_owned(),
            purpose: "review source".to_owned(),
            initial_members: vec![
                TeamRosterMemberRequest {
                    activity_id: Uuid::from_u128(7),
                    session_id: Uuid::from_u128(8),
                    role: "reviewer".to_owned(),
                },
                TeamRosterMemberRequest {
                    activity_id: Uuid::from_u128(9),
                    session_id: Uuid::from_u128(10),
                    role: "observer".to_owned(),
                },
            ],
        }
    }

    fn create_v2_request() -> TeamCreateV2Request {
        TeamCreateV2Request {
            scope: scope(),
            request_id: Uuid::from_u128(3),
            team_id: Uuid::from_u128(4),
            activity_id: Uuid::from_u128(5),
            session_id: Uuid::from_u128(6),
            expected_sender_leadership_revision: 1,
            name: "review".to_owned(),
            purpose: "review source".to_owned(),
            initial_members: vec![
                TeamRosterMemberRequest {
                    activity_id: Uuid::from_u128(7),
                    session_id: Uuid::from_u128(8),
                    role: "reviewer".to_owned(),
                },
                TeamRosterMemberRequest {
                    activity_id: Uuid::from_u128(9),
                    session_id: Uuid::from_u128(10),
                    role: "reviewer".to_owned(),
                },
            ],
            max_members: 2,
        }
    }

    fn join_request() -> TeamJoinRequest {
        TeamJoinRequest {
            scope: scope(),
            request_id: Uuid::from_u128(11),
            team_id: Uuid::from_u128(12),
            activity_id: Uuid::from_u128(13),
            session_id: Uuid::from_u128(14),
            expected_team_revision: 5,
            role: "reviewer".to_owned(),
        }
    }

    fn join_v2_request() -> TeamJoinV2Request {
        TeamJoinV2Request {
            scope: scope(),
            request_id: Uuid::from_u128(11),
            team_id: Uuid::from_u128(12),
            activity_id: Uuid::from_u128(13),
            session_id: Uuid::from_u128(14),
            expected_team_revision: 5,
            role: "reviewer".to_owned(),
            expansion_slot: crate::wire::team_commands::TeamExpansionSlotV2Request {
                decision_id: Uuid::from_u128(15),
                ordinal: 0,
            },
        }
    }

    fn leave_request() -> TeamLeaveRequest {
        TeamLeaveRequest {
            scope: scope(),
            request_id: Uuid::from_u128(15),
            team_id: Uuid::from_u128(16),
            activity_id: Uuid::from_u128(17),
            session_id: Uuid::from_u128(18),
            expected_team_revision: 5,
        }
    }

    fn retire_request() -> TeamRetireRequest {
        TeamRetireRequest {
            scope: scope(),
            request_id: Uuid::from_u128(19),
            team_id: Uuid::from_u128(20),
            expected_team_revision: 5,
        }
    }

    fn team(
        scope: &WorkspaceQueryScope,
        team_id: Uuid,
        revision: u64,
        state: &str,
        creator_session_id: Uuid,
    ) -> TeamWire {
        TeamWire {
            team_id,
            workspace_id: scope.workspace_id,
            name: "review".to_owned(),
            purpose: "review source".to_owned(),
            state: state.to_owned(),
            revision,
            created_by: TeamCreatorWire {
                key: "session:creator".to_owned(),
                display_name: "Creator".to_owned(),
                participation_id: Some(Uuid::from_u128(21)),
                profile_id: Some(Uuid::from_u128(22)),
                participant_id: Some(Uuid::from_u128(23)),
                session_id: Some(creator_session_id),
            },
            created_at_ms: 10,
            updated_at_ms: 11,
            formed_at_ms: Some(10),
        }
    }

    fn member(
        scope: &WorkspaceQueryScope,
        team_id: Uuid,
        participation_id: Uuid,
        role: &str,
        state: &str,
        revision: u64,
        left_at_ms: Option<i64>,
    ) -> TeamMemberWire {
        TeamMemberWire {
            workspace_id: scope.workspace_id,
            team_id,
            participation_id,
            profile_id: Some(Uuid::from_u128(24)),
            display_name: Some("Member".to_owned()),
            role: Some(role.to_owned()),
            state: state.to_owned(),
            revision,
            joined_at_ms: 10,
            left_at_ms,
        }
    }

    struct MutationResponseSubject<'a> {
        scope: &'a WorkspaceQueryScope,
        request_id: Uuid,
        team_id: Uuid,
        creator_session_id: Uuid,
    }

    struct MutationResponseSnapshot {
        team_revision: u64,
        team_state: &'static str,
        membership: Option<TeamMemberWire>,
        initial_members: Vec<TeamMemberWire>,
        created: bool,
    }

    fn mutation_response(
        subject: &MutationResponseSubject<'_>,
        snapshot: MutationResponseSnapshot,
    ) -> TeamMutationResponse {
        TeamMutationResponse {
            scope: subject.scope.clone(),
            request_id: subject.request_id,
            team_id: subject.team_id,
            mutation: TeamMutationWire {
                team: team(
                    subject.scope,
                    subject.team_id,
                    snapshot.team_revision,
                    snapshot.team_state,
                    subject.creator_session_id,
                ),
                membership: snapshot.membership,
                initial_members: snapshot.initial_members,
                created: snapshot.created,
                replayed: false,
            },
        }
    }

    fn create_response(request: &TeamCreateRequest) -> TeamMutationResponse {
        mutation_response(
            &MutationResponseSubject {
                scope: &request.scope,
                request_id: request.request_id,
                team_id: request.team_id,
                creator_session_id: request.session_id,
            },
            MutationResponseSnapshot {
                team_revision: 1,
                team_state: "active",
                membership: None,
                initial_members: vec![
                    member(
                        &request.scope,
                        request.team_id,
                        Uuid::from_u128(25),
                        "reviewer",
                        "joined",
                        1,
                        None,
                    ),
                    member(
                        &request.scope,
                        request.team_id,
                        Uuid::from_u128(26),
                        "observer",
                        "joined",
                        1,
                        None,
                    ),
                ],
                created: true,
            },
        )
    }

    fn create_v2_response(
        request: &TeamCreateV2Request,
    ) -> Result<TeamMutationV2Response, ClientError> {
        let initial_roster_digest_v1 = expected_roster_digest_v1(request)?;
        Ok(TeamMutationV2Response {
            scope: request.scope.clone(),
            request_id: request.request_id,
            team_id: request.team_id,
            mutation: crate::wire::teams::TeamMutationV2Wire {
                team: team(
                    &request.scope,
                    request.team_id,
                    1,
                    "active",
                    request.session_id,
                ),
                membership: None,
                initial_members: vec![
                    member(
                        &request.scope,
                        request.team_id,
                        Uuid::from_u128(25),
                        "reviewer",
                        "joined",
                        1,
                        None,
                    ),
                    member(
                        &request.scope,
                        request.team_id,
                        Uuid::from_u128(26),
                        "reviewer",
                        "joined",
                        1,
                        None,
                    ),
                ],
                capacity: Some(crate::wire::teams::TeamCapacityReceiptV2Wire {
                    team_id: request.team_id,
                    workspace_id: request.scope.workspace_id,
                    max_members: request.max_members,
                    revision: 1,
                    recorded_at_ms: 10,
                    actor: crate::wire::team_expansion::TeamExpansionActorReceiptWire {
                        activity_id: request.activity_id,
                        agent_id: heterarchy_orchestration_runtime::leadership::member_agent_id(
                            request.scope.workspace_id,
                            request.activity_id,
                            request.session_id,
                        )
                        .into_uuid(),
                        session_id: request.session_id,
                        participation_id: Uuid::from_u128(33),
                        profile_id: Uuid::from_u128(34),
                        participant_id: Uuid::from_u128(35),
                        display_name: "Creator".to_owned(),
                    },
                    leadership_revision: request.expected_sender_leadership_revision,
                }),
                initial_roster_digest_v1: Some(initial_roster_digest_v1),
                expansion_slot: None,
                expansion_join_source: None,
                created: true,
                replayed: false,
            },
        })
    }

    fn join_response(request: &TeamJoinRequest) -> TeamMutationResponse {
        mutation_response(
            &MutationResponseSubject {
                scope: &request.scope,
                request_id: request.request_id,
                team_id: request.team_id,
                creator_session_id: Uuid::from_u128(27),
            },
            MutationResponseSnapshot {
                team_revision: 6,
                team_state: "active",
                membership: Some(member(
                    &request.scope,
                    request.team_id,
                    Uuid::from_u128(28),
                    &request.role,
                    "joined",
                    1,
                    None,
                )),
                initial_members: Vec::new(),
                created: false,
            },
        )
    }

    fn join_v2_response(request: &TeamJoinV2Request) -> TeamMutationV2Response {
        TeamMutationV2Response {
            scope: request.scope.clone(),
            request_id: request.request_id,
            team_id: request.team_id,
            mutation: crate::wire::teams::TeamMutationV2Wire {
                team: team(
                    &request.scope,
                    request.team_id,
                    6,
                    "active",
                    Uuid::from_u128(27),
                ),
                membership: Some(member(
                    &request.scope,
                    request.team_id,
                    Uuid::from_u128(28),
                    &request.role,
                    "joined",
                    1,
                    None,
                )),
                initial_members: Vec::new(),
                capacity: None,
                initial_roster_digest_v1: None,
                expansion_slot: Some(request.expansion_slot),
                expansion_join_source: Some(crate::wire::teams::TeamExpansionJoinSourceV2Wire {
                    activity_id: request.activity_id,
                    agent_id: heterarchy_orchestration_runtime::leadership::member_agent_id(
                        request.scope.workspace_id,
                        request.activity_id,
                        request.session_id,
                    )
                    .into_uuid(),
                    session_id: request.session_id,
                    decision_id: request.expansion_slot.decision_id,
                    ordinal: request.expansion_slot.ordinal,
                    role: request.role.clone(),
                }),
                created: true,
                replayed: false,
            },
        }
    }

    fn leave_response(request: &TeamLeaveRequest) -> TeamMutationResponse {
        mutation_response(
            &MutationResponseSubject {
                scope: &request.scope,
                request_id: request.request_id,
                team_id: request.team_id,
                creator_session_id: Uuid::from_u128(29),
            },
            MutationResponseSnapshot {
                team_revision: 6,
                team_state: "active",
                membership: Some(member(
                    &request.scope,
                    request.team_id,
                    Uuid::from_u128(30),
                    "reviewer",
                    "left",
                    2,
                    Some(11),
                )),
                initial_members: Vec::new(),
                created: false,
            },
        )
    }

    fn retire_response(request: &TeamRetireRequest) -> TeamMutationResponse {
        mutation_response(
            &MutationResponseSubject {
                scope: &request.scope,
                request_id: request.request_id,
                team_id: request.team_id,
                creator_session_id: Uuid::from_u128(31),
            },
            MutationResponseSnapshot {
                team_revision: 6,
                team_state: "retired",
                membership: None,
                initial_members: Vec::new(),
                created: false,
            },
        )
    }

    #[test]
    fn requests_match_operations_text_roster_and_revision_bounds() {
        let create = create_request();
        assert!(validate_create_request(&create).is_ok());
        assert!(validate_join_request(&join_request()).is_ok());
        assert!(validate_leave_request(&leave_request()).is_ok());
        assert!(validate_retire_request(&retire_request()).is_ok());

        let mut invalid = create.clone();
        invalid.purpose = " \t".to_owned();
        assert!(validate_create_request(&invalid).is_err());
        let mut invalid = create.clone();
        invalid.initial_members.pop();
        assert!(validate_create_request(&invalid).is_err());
        let mut invalid = create.clone();
        invalid.initial_members[1] = invalid.initial_members[0].clone();
        assert!(validate_create_request(&invalid).is_err());
        let mut invalid = create;
        invalid.initial_members[0].role = "r".repeat(MAX_TEAM_ROLE_BYTES + 1);
        assert!(validate_create_request(&invalid).is_err());

        let mut invalid_join = join_request();
        invalid_join.expected_team_revision = 0;
        assert!(validate_join_request(&invalid_join).is_err());
        let mut invalid_retire = retire_request();
        invalid_retire.expected_team_revision = u64::MAX;
        assert!(validate_retire_request(&invalid_retire).is_err());
    }

    #[test]
    fn v2_create_digest_rejects_swapped_same_role_sessions()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = create_v2_request();
        let response = create_v2_response(&request)?;
        assert!(valid_create_v2_response(&response, &request));

        let mut swapped_roster = request.clone();
        let first_session = swapped_roster.initial_members[0].session_id;
        swapped_roster.initial_members[0].session_id = swapped_roster.initial_members[1].session_id;
        swapped_roster.initial_members[1].session_id = first_session;
        let swapped_digest = expected_roster_digest_v1(&swapped_roster)?;
        let mut substituted = response;
        substituted.mutation.initial_roster_digest_v1 = Some(swapped_digest);

        assert!(!valid_create_v2_response(&substituted, &request));
        Ok(())
    }

    #[test]
    fn v2_join_receipt_correlates_the_exact_decision_slot() -> Result<(), Box<dyn std::error::Error>>
    {
        let request = join_v2_request();
        let response = join_v2_response(&request);
        assert!(valid_join_v2_response(&response, &request));

        let mut rejoin = response.clone();
        rejoin.mutation.created = false;
        let membership =
            rejoin.mutation.membership.as_mut().ok_or_else(|| {
                std::io::Error::other("joined membership missing in test receipt")
            })?;
        membership.revision = 2;
        assert!(valid_join_v2_response(&rejoin, &request));

        let mut inconsistent_first_join = response.clone();
        inconsistent_first_join.mutation.created = false;
        assert!(!valid_join_v2_response(&inconsistent_first_join, &request));

        let mut inconsistent_rejoin = rejoin;
        inconsistent_rejoin.mutation.created = true;
        assert!(!valid_join_v2_response(&inconsistent_rejoin, &request));

        let mut substituted = response;
        let slot = substituted
            .mutation
            .expansion_slot
            .as_mut()
            .ok_or_else(|| std::io::Error::other("v2 slot receipt missing in test response"))?;
        slot.decision_id = Uuid::from_u128(99);
        assert!(!valid_join_v2_response(&substituted, &request));
        Ok(())
    }

    #[tokio::test]
    async fn create_wrong_scope_success_preserves_exact_readback_without_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = create_request();
        let mut body = serde_json::to_value(create_response(&request))?;
        body["scope"]["workspaceId"] = serde_json::json!(Uuid::from_u128(99));
        let (client, server) = fixture(body).await?;

        assert_eq!(
            client
                .create_team(&HeaderValue::from_static(CREDENTIAL), &request)
                .await
                .err(),
            Some(ClientError::UnknownOutcome {
                readback: ReadbackTarget::TeamMutation {
                    workspace_id: request.scope.workspace_id,
                    team_id: request.team_id,
                    request_id: request.request_id,
                }
            })
        );
        let captured = String::from_utf8(server.await??)?;
        assert!(captured.contains("/teams/create HTTP/1.1"));
        assert!(!captured.contains("\"maxMembers\""));
        assert!(!captured.contains("\"agentId\""));
        Ok(())
    }

    #[tokio::test]
    async fn join_wrong_nonce_success_preserves_exact_readback_without_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = join_request();
        let mut body = serde_json::to_value(join_response(&request))?;
        body["requestId"] = serde_json::json!(Uuid::from_u128(99));
        let (client, server) = fixture(body).await?;

        assert_eq!(
            client
                .join_team(&HeaderValue::from_static(CREDENTIAL), &request)
                .await
                .err(),
            Some(ClientError::UnknownOutcome {
                readback: ReadbackTarget::TeamMutation {
                    workspace_id: request.scope.workspace_id,
                    team_id: request.team_id,
                    request_id: request.request_id,
                }
            })
        );
        let captured = String::from_utf8(server.await??)?;
        assert!(captured.contains(&format!("/teams/{}/join HTTP/1.1", request.team_id)));
        assert!(!captured.contains("\"expansionSlot\""));
        Ok(())
    }

    #[tokio::test]
    async fn v2_create_digest_mismatch_preserves_full_intent_without_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = create_v2_request();
        let mut swapped_roster = request.clone();
        let first_session = swapped_roster.initial_members[0].session_id;
        swapped_roster.initial_members[0].session_id = swapped_roster.initial_members[1].session_id;
        swapped_roster.initial_members[1].session_id = first_session;
        let swapped_digest = expected_roster_digest_v1(&swapped_roster)?;
        let mut body = serde_json::to_value(create_v2_response(&request)?)?;
        body["mutation"]["initialRosterDigestV1"] = serde_json::json!(swapped_digest);
        let (client, server) = fixture(body).await?;

        assert_eq!(
            client
                .create_team_v2(&HeaderValue::from_static(CREDENTIAL), &request)
                .await
                .err(),
            Some(ClientError::UnknownOutcome {
                readback: ReadbackTarget::TeamCreateV2Mutation(Box::new(request.clone()))
            })
        );
        let captured = String::from_utf8(server.await??)?;
        assert!(
            captured.contains("/internal/v2/workspaces/") && captured.contains("/teams/create")
        );
        assert!(captured.contains("\"maxMembers\":2"));
        Ok(())
    }

    #[tokio::test]
    async fn v2_join_wrong_slot_preserves_full_intent_without_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = join_v2_request();
        let mut body = serde_json::to_value(join_v2_response(&request))?;
        body["mutation"]["expansionSlot"]["decisionId"] = serde_json::json!(Uuid::from_u128(99));
        let (client, server) = fixture(body).await?;

        assert_eq!(
            client
                .join_team_v2(&HeaderValue::from_static(CREDENTIAL), &request)
                .await
                .err(),
            Some(ClientError::UnknownOutcome {
                readback: ReadbackTarget::TeamJoinV2Mutation(Box::new(request.clone()))
            })
        );
        let captured = String::from_utf8(server.await??)?;
        assert!(
            captured.contains("/internal/v2/workspaces/") && captured.contains("/join HTTP/1.1")
        );
        assert!(captured.contains("\"decisionId\""));
        Ok(())
    }

    #[tokio::test]
    async fn v2_join_missing_source_is_unknown_with_exact_intent()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = join_v2_request();
        let mut body = serde_json::to_value(join_v2_response(&request))?;
        body["mutation"]["expansionJoinSource"] = serde_json::Value::Null;
        let (client, server) = fixture(body).await?;

        assert_eq!(
            client
                .join_team_v2(&HeaderValue::from_static(CREDENTIAL), &request)
                .await
                .err(),
            Some(ClientError::UnknownOutcome {
                readback: ReadbackTarget::TeamJoinV2Mutation(Box::new(request.clone()))
            })
        );
        let captured = String::from_utf8(server.await??)?;
        assert!(captured.contains("/internal/v2/workspaces/"));
        assert!(captured.contains("/join HTTP/1.1"));
        Ok(())
    }

    #[tokio::test]
    async fn v2_create_readback_keeps_original_intent_separate_from_rebound_reader()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = create_v2_request();
        let direct = create_v2_response(&request)?;
        let body = serde_json::to_value(TeamMutationV2ReadbackResponse {
            scope: request.scope.clone(),
            request_id: request.request_id,
            team_id: request.team_id,
            receipt: Some(direct.mutation),
        })?;
        let (client, server) = fixture(body).await?;
        let reader = TeamMutationReaderV2Wire {
            activity_id: request.activity_id,
            session_id: Uuid::from_u128(90),
        };

        let readback = client
            .team_create_mutation_readback_v2(
                &HeaderValue::from_static(CREDENTIAL),
                &request,
                reader,
            )
            .await?;
        assert!(readback.is_some());

        let captured = String::from_utf8(server.await??)?;
        assert!(captured.contains("/internal/v2/workspaces/"));
        assert!(captured.contains("/receipts/query HTTP/1.1"));
        let request_json: serde_json::Value = serde_json::from_str(
            captured
                .split_once("\r\n\r\n")
                .ok_or("readback request body")?
                .1,
        )?;
        assert_eq!(
            request_json["reader"]["sessionId"],
            serde_json::json!(Uuid::from_u128(90))
        );
        assert_eq!(
            request_json["intent"]["request"]["sessionId"],
            serde_json::json!(request.session_id)
        );
        assert_eq!(
            request_json["intent"]["request"]["maxMembers"],
            serde_json::json!(request.max_members)
        );
        Ok(())
    }

    #[tokio::test]
    async fn v2_join_readback_checks_original_actor_and_decision_slot()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = join_v2_request();
        let mut direct = join_v2_response(&request);
        direct.mutation.expansion_join_source =
            Some(crate::wire::teams::TeamExpansionJoinSourceV2Wire {
                activity_id: request.activity_id,
                agent_id: heterarchy_orchestration_runtime::leadership::member_agent_id(
                    request.scope.workspace_id,
                    request.activity_id,
                    request.session_id,
                )
                .into_uuid(),
                session_id: request.session_id,
                decision_id: request.expansion_slot.decision_id,
                ordinal: request.expansion_slot.ordinal,
                role: request.role.clone(),
            });
        let body = serde_json::to_value(TeamMutationV2ReadbackResponse {
            scope: request.scope.clone(),
            request_id: request.request_id,
            team_id: request.team_id,
            receipt: Some(direct.mutation),
        })?;
        let (client, server) = fixture(body).await?;
        let readback = client
            .team_join_mutation_readback_v2(
                &HeaderValue::from_static(CREDENTIAL),
                &request,
                TeamMutationReaderV2Wire {
                    activity_id: Uuid::from_u128(90),
                    session_id: Uuid::from_u128(91),
                },
            )
            .await?;
        assert!(readback.is_some());

        let captured = String::from_utf8(server.await??)?;
        assert!(captured.contains("/internal/v2/workspaces/"));
        assert!(captured.contains("/receipts/query HTTP/1.1"));
        let request_json: serde_json::Value = serde_json::from_str(
            captured
                .split_once("\r\n\r\n")
                .ok_or("readback request body")?
                .1,
        )?;
        assert_eq!(
            request_json["reader"]["activityId"],
            serde_json::json!(Uuid::from_u128(90))
        );
        assert_eq!(
            request_json["reader"]["sessionId"],
            serde_json::json!(Uuid::from_u128(91))
        );
        assert_eq!(
            request_json["intent"]["request"]["expansionSlot"]["decisionId"],
            serde_json::json!(request.expansion_slot.decision_id)
        );
        assert_ne!(
            request_json["intent"]["request"]["expansionSlot"]["decisionId"],
            serde_json::json!(request.request_id)
        );
        Ok(())
    }

    #[tokio::test]
    async fn v2_readback_without_source_proof_stays_unresolved_without_v1_fallback()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = create_v2_request();
        let body = serde_json::to_value(TeamMutationV2ReadbackResponse {
            scope: request.scope.clone(),
            request_id: request.request_id,
            team_id: request.team_id,
            receipt: None,
        })?;
        let (client, server) = fixture(body).await?;

        assert!(
            client
                .team_create_mutation_readback_v2(
                    &HeaderValue::from_static(CREDENTIAL),
                    &request,
                    TeamMutationReaderV2Wire {
                        activity_id: request.activity_id,
                        session_id: request.session_id,
                    },
                )
                .await?
                .is_none()
        );
        let captured = String::from_utf8(server.await??)?;
        assert!(captured.contains("/internal/v2/workspaces/"));
        assert!(captured.contains("/receipts/query HTTP/1.1"));
        assert!(captured.contains("\"maxMembers\":2"));
        Ok(())
    }

    #[tokio::test]
    async fn leave_malformed_success_receipt_preserves_exact_readback_without_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = leave_request();
        let mut body = serde_json::to_value(leave_response(&request))?;
        body["mutation"]["membership"]["state"] = serde_json::json!("joined");
        let (client, server) = fixture(body).await?;

        assert_eq!(
            client
                .leave_team(&HeaderValue::from_static(CREDENTIAL), &request)
                .await
                .err(),
            Some(ClientError::UnknownOutcome {
                readback: ReadbackTarget::TeamMutation {
                    workspace_id: request.scope.workspace_id,
                    team_id: request.team_id,
                    request_id: request.request_id,
                }
            })
        );
        let captured = String::from_utf8(server.await??)?;
        assert!(captured.contains(&format!("/teams/{}/leave HTTP/1.1", request.team_id)));
        Ok(())
    }

    #[tokio::test]
    async fn retire_malformed_success_receipt_preserves_exact_readback_without_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = retire_request();
        let mut body = serde_json::to_value(retire_response(&request))?;
        body["mutation"]["team"]["revision"] = serde_json::json!(7);
        let (client, server) = fixture(body).await?;

        assert_eq!(
            client
                .retire_team(&HeaderValue::from_static(CREDENTIAL), &request)
                .await
                .err(),
            Some(ClientError::UnknownOutcome {
                readback: ReadbackTarget::TeamMutation {
                    workspace_id: request.scope.workspace_id,
                    team_id: request.team_id,
                    request_id: request.request_id,
                }
            })
        );
        let captured = String::from_utf8(server.await??)?;
        assert!(captured.contains(&format!("/teams/{}/retire HTTP/1.1", request.team_id)));
        Ok(())
    }

    async fn fixture(
        response: serde_json::Value,
    ) -> Result<
        (
            OrchestrationHttpClient,
            tokio::task::JoinHandle<Result<Vec<u8>, io::Error>>,
        ),
        Box<dyn std::error::Error>,
    > {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let client = OrchestrationHttpClient::new(listener.local_addr()?, Duration::from_secs(1))?;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                let count = socket.read(&mut buffer).await?;
                if count == 0 {
                    return Err(io::Error::other("client closed before completing request"));
                }
                request.extend_from_slice(&buffer[..count]);
                if request.len() > MAX_REQUEST_BYTES + MAX_HEADER_BYTES {
                    return Err(io::Error::other("fixture request exceeded bound"));
                }
                if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&request[..end]);
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap_or_default();
                    if request.len() >= end + 4 + length {
                        break;
                    }
                }
            }

            let body = response.to_string();
            let reply = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(reply.as_bytes()).await?;
            socket.shutdown().await?;
            drop(socket);

            match tokio::time::timeout(Duration::from_millis(100), listener.accept()).await {
                Ok(Ok((mut socket, _))) => {
                    let _ = socket.shutdown().await;
                    return Err(io::Error::other("client retried an uncertain mutation"));
                }
                Ok(Err(error)) => return Err(error),
                Err(_) => {}
            }
            Ok(request)
        });
        Ok((client, server))
    }
}
