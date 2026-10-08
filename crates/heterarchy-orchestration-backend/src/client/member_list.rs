//! Bounded official client for caller-scoped logical Workspace member reads.

use std::collections::HashSet;

use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use crate::wire::member_list::{LogicalMemberWire, MemberListRequest, MemberListResponse};

use super::membership::validate_scope;
use super::{ClientError, OrchestrationHttpClient, require_identity};

const MAX_MEMBERS: u32 = 64;
const MAX_MEMBER_METADATA_BYTES: usize = 128;

impl OrchestrationHttpClient {
    /// Read the bounded logical-member projection through the selected caller Session.
    ///
    /// # Errors
    /// Returns invalid input, a typed service rejection, bounded transport
    /// failure, or a protocol error for a malformed member projection.
    pub async fn list_members(
        &self,
        authorization: &HeaderValue,
        request: &MemberListRequest,
    ) -> Result<MemberListResponse, ClientError> {
        validate_request(request)?;
        let response: MemberListResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/members/list",
                    request.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if !valid_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn validate_request(request: &MemberListRequest) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    if !(1..=MAX_MEMBERS).contains(&request.limit)
        || request
            .session_id
            .is_some_and(|session_id| session_id.is_nil())
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn valid_response(response: &MemberListResponse, request: &MemberListRequest) -> bool {
    let Ok(limit) = usize::try_from(request.limit) else {
        return false;
    };
    if response.scope != request.scope
        || response.session_id != request.session_id
        || response.limit != request.limit
        || response.members.len() > limit
    {
        return false;
    }

    let mut sessions = HashSet::with_capacity(response.members.len());
    let mut participants = HashSet::with_capacity(response.members.len());
    for member in &response.members {
        if !valid_member(member, request.scope.workspace_id)
            || !sessions.insert(member.session_id)
            || !participants.insert(member.participant_id)
        {
            return false;
        }
    }
    response
        .members
        .windows(2)
        .all(|pair| pair[0].participant_id.as_bytes() < pair[1].participant_id.as_bytes())
}

fn valid_member(member: &LogicalMemberWire, workspace_id: Uuid) -> bool {
    member.workspace_id == workspace_id
        && require_identity(member.participant_id).is_ok()
        && require_identity(member.session_id).is_ok()
        && valid_metadata(&member.display_name)
        && valid_metadata(&member.declared_role)
        && member.membership_revision > 0
}

fn valid_metadata(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_MEMBER_METADATA_BYTES
        && !value.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::WorkspaceQueryScope;
    use crate::wire::membership::MemberTransportWire;

    fn scope() -> WorkspaceQueryScope {
        WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 3,
            workspace_revision: 4,
            locator: "workspace".to_owned(),
        }
    }

    fn request(limit: u32) -> MemberListRequest {
        MemberListRequest {
            scope: scope(),
            session_id: Some(Uuid::from_u128(3)),
            limit,
        }
    }

    fn member(participant_id: Uuid, session_id: Uuid) -> LogicalMemberWire {
        LogicalMemberWire {
            workspace_id: scope().workspace_id,
            participant_id,
            session_id,
            display_name: "Ada".to_owned(),
            declared_role: "reviewer".to_owned(),
            transport: MemberTransportWire::Mcp,
            membership_revision: 1,
        }
    }

    #[test]
    fn request_rejects_invalid_scope_session_and_limit() {
        assert!(validate_request(&request(64)).is_ok());
        assert!(validate_request(&request(0)).is_err());
        assert!(validate_request(&request(65)).is_err());

        let mut nil_session = request(1);
        nil_session.session_id = Some(Uuid::nil());
        assert!(validate_request(&nil_session).is_err());

        let mut bad_scope = request(1);
        bad_scope.scope.workspace_revision = 0;
        assert!(validate_request(&bad_scope).is_err());
    }

    #[test]
    fn hostile_member_readbacks_reject_foreign_scope_bad_echoes_duplicates_and_bounds() {
        let request = request(2);
        let valid = MemberListResponse {
            scope: request.scope.clone(),
            session_id: request.session_id,
            limit: request.limit,
            members: vec![
                member(Uuid::from_u128(10), Uuid::from_u128(20)),
                member(Uuid::from_u128(11), Uuid::from_u128(21)),
            ],
        };
        assert!(valid_response(&valid, &request));

        let mut foreign_workspace = valid.clone();
        foreign_workspace.members[0].workspace_id = Uuid::from_u128(99);
        assert!(!valid_response(&foreign_workspace, &request));

        let mut wrong_scope = valid.clone();
        wrong_scope.scope.workspace_revision += 1;
        assert!(!valid_response(&wrong_scope, &request));

        let mut wrong_session = valid.clone();
        wrong_session.session_id = None;
        assert!(!valid_response(&wrong_session, &request));

        let mut wrong_limit = valid.clone();
        wrong_limit.limit = 1;
        assert!(!valid_response(&wrong_limit, &request));

        let mut duplicate = valid.clone();
        duplicate.members[1].participant_id = duplicate.members[0].participant_id;
        duplicate.members[1].session_id = duplicate.members[0].session_id;
        assert!(!valid_response(&duplicate, &request));

        let mut repeated_session = valid.clone();
        repeated_session.members[1].session_id = repeated_session.members[0].session_id;
        assert!(!valid_response(&repeated_session, &request));

        let mut out_of_order = valid.clone();
        out_of_order.members.swap(0, 1);
        assert!(!valid_response(&out_of_order, &request));

        let mut over_limit = valid;
        over_limit
            .members
            .push(member(Uuid::from_u128(12), Uuid::from_u128(22)));
        assert!(!valid_response(&over_limit, &request));
    }

    #[test]
    fn hostile_member_fields_reject_nil_identity_bad_metadata_and_revision() {
        let request = request(1);
        let response = MemberListResponse {
            scope: request.scope.clone(),
            session_id: request.session_id,
            limit: request.limit,
            members: vec![member(Uuid::from_u128(10), Uuid::from_u128(20))],
        };
        assert!(valid_response(&response, &request));

        let mut nil_participant = response.clone();
        nil_participant.members[0].participant_id = Uuid::nil();
        assert!(!valid_response(&nil_participant, &request));

        let mut nil_session = response.clone();
        nil_session.members[0].session_id = Uuid::nil();
        assert!(!valid_response(&nil_session, &request));

        let mut empty_name = response.clone();
        empty_name.members[0].display_name = " ".to_owned();
        assert!(!valid_response(&empty_name, &request));

        let mut overlong_role = response.clone();
        overlong_role.members[0].declared_role = "r".repeat(MAX_MEMBER_METADATA_BYTES + 1);
        assert!(!valid_response(&overlong_role, &request));

        let mut control_character = response.clone();
        control_character.members[0].display_name = "bad\nname".to_owned();
        assert!(!valid_response(&control_character, &request));

        let mut zero_revision = response;
        zero_revision.members[0].membership_revision = 0;
        assert!(!valid_response(&zero_revision, &request));
    }
}
