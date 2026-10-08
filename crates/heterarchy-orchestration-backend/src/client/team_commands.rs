//! Bounded HTTP client for source-scoped Team mutations and full receipts.

use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use crate::wire::team_commands::{
    TeamCreateRequest, TeamJoinRequest, TeamLeaveRequest, TeamMutationResponse, TeamRetireRequest,
    TeamRosterMemberRequest,
};

use super::teams::{valid_mutation, valid_scope};
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

fn validate_create_request(request: &TeamCreateRequest) -> Result<(), ClientError> {
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

fn validate_join_request(request: &TeamJoinRequest) -> Result<(), ClientError> {
    validate_common_request(&request.scope, request.request_id, request.team_id)?;
    require_identity(request.activity_id)?;
    require_identity(request.session_id)?;
    require_expected_team_revision(request.expected_team_revision)?;
    if !valid_text(&request.role, MAX_TEAM_ROLE_BYTES) {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
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

fn valid_create_response(response: &TeamMutationResponse, request: &TeamCreateRequest) -> bool {
    valid_common_response(
        response,
        &request.scope,
        request.request_id,
        request.team_id,
    ) && response.mutation.created
        && response.mutation.team.revision == 1
        && response.mutation.team.state == "active"
        && response.mutation.team.name == request.name
        && response.mutation.team.purpose == request.purpose
        && response.mutation.team.created_by.session_id == Some(request.session_id)
        && response.mutation.membership.is_none()
        && same_initial_roster_roles(&request.initial_members, &response.mutation.initial_members)
}

fn valid_join_response(response: &TeamMutationResponse, request: &TeamJoinRequest) -> bool {
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
            .is_some_and(|membership| {
                membership.state == "joined"
                    && membership.role.as_deref() == Some(request.role.as_str())
            })
        && response.mutation.initial_members.is_empty()
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
                created: true,
            },
        )
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
