//! Bounded client for Agent-request join, activation, and participation reads.

use heterarchy_orchestration_operations::AgentRequestState;
use heterarchy_orchestration_runtime::{
    membership::member_registration_request_id,
    provisioning::{participation::agent_request_session_open_request_id, request_state_label},
};
use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use crate::wire::AgentSessionResponse;
use crate::wire::membership::{
    MemberRegistrationQueryRequest, MemberRegistrationQueryResponse, MemberTarget,
};
use crate::wire::provisioning_participation::{
    AgentRequestActivateRequest, AgentRequestJoinRequest, AgentRequestParticipationQueryRequest,
    AgentRequestParticipationQueryResponse, AgentRequestParticipationResponse,
    AgentRequestParticipationTargetWire,
};

use super::membership::{
    valid_activation, valid_query_response as valid_member_query_response, validate_member_target,
    validate_scope,
};
use super::{
    ClientError, OrchestrationHttpClient, ReadbackTarget, require_identity, valid_session,
};

impl OrchestrationHttpClient {
    /// Join one confirmed provision operation into the caller's exact Session.
    ///
    /// # Errors
    /// Returns invalid input, a typed service rejection, bounded transport
    /// failure, or an unknown outcome carrying the operation and Session IDs.
    pub async fn join_agent_request(
        &self,
        authorization: &HeaderValue,
        request: &AgentRequestJoinRequest,
    ) -> Result<AgentRequestParticipationResponse, ClientError> {
        validate_join_request(request)?;
        let readback = ReadbackTarget::AgentRequestParticipation {
            operation_id: request.operation_id,
            session_id: Some(request.session_id),
        };
        let response: AgentRequestParticipationResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/provision-operations/{}/join",
                    request.operation_id
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

    /// Open/replay the operation-derived Session and activate its membership.
    ///
    /// # Errors
    /// Returns invalid input, a typed service rejection, bounded transport
    /// failure, or an unknown outcome carrying the exact operation identity.
    pub async fn activate_agent_request(
        &self,
        authorization: &HeaderValue,
        request: &AgentRequestActivateRequest,
    ) -> Result<AgentRequestParticipationResponse, ClientError> {
        validate_activate_request(request)?;
        let readback = ReadbackTarget::AgentRequestParticipation {
            operation_id: request.operation_id,
            session_id: None,
        };
        let response: AgentRequestParticipationResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/provision-operations/{}/activate",
                    request.operation_id
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_activate_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read only the operation participation visible through the caller's Session.
    ///
    /// # Errors
    /// Returns invalid input, a typed service rejection, bounded transport
    /// failure, or a protocol error for malformed or incoherent readback.
    pub async fn agent_request_participation(
        &self,
        authorization: &HeaderValue,
        request: &AgentRequestParticipationQueryRequest,
    ) -> Result<AgentRequestParticipationQueryResponse, ClientError> {
        validate_query_request(request)?;
        let response: AgentRequestParticipationQueryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/provision-operations/{}/participation/query",
                    request.operation_id
                ),
                Some(request),
                None,
            )
            .await?;
        if !valid_participation_query_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn validate_join_request(request: &AgentRequestJoinRequest) -> Result<(), ClientError> {
    require_identity(request.operation_id)?;
    require_identity(request.session_id)
}

fn validate_activate_request(request: &AgentRequestActivateRequest) -> Result<(), ClientError> {
    require_identity(request.operation_id)
}

fn validate_query_request(
    request: &AgentRequestParticipationQueryRequest,
) -> Result<(), ClientError> {
    require_identity(request.operation_id)?;
    if request
        .session_id
        .is_some_and(|session_id| session_id.is_nil())
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn valid_join_response(
    response: &AgentRequestParticipationResponse,
    request: &AgentRequestJoinRequest,
) -> bool {
    response.target.session_id == request.session_id
        && response.session_created.is_none()
        && response.session_revision.is_none()
        && valid_mutation_response(response, request.operation_id)
}

fn valid_activate_response(
    response: &AgentRequestParticipationResponse,
    request: &AgentRequestActivateRequest,
) -> bool {
    response.session_created.is_some()
        && response
            .session_revision
            .is_some_and(|revision| revision > 0)
        && valid_mutation_response(response, request.operation_id)
}

fn valid_mutation_response(
    response: &AgentRequestParticipationResponse,
    operation_id: Uuid,
) -> bool {
    response.operation_id == operation_id
        && validate_member_target(&response.target).is_ok()
        && valid_agent_request_identity(
            response.agent_request_id,
            &response.agent_request_state,
            response.agent_request_revision,
        )
        && require_identity(response.profile_id).is_ok()
        && require_identity(response.runtime_session_id).is_ok()
        && valid_activation(
            &response.membership,
            &response.target,
            expected_member_request_id(&response.target),
            None,
            None,
        )
}

fn valid_participation_query_response(
    response: &AgentRequestParticipationQueryResponse,
    request: &AgentRequestParticipationQueryRequest,
) -> bool {
    if response.operation_id != request.operation_id {
        return false;
    }

    let Some(session) = response.session.as_ref() else {
        return response.target.is_none() && response.membership.is_none();
    };
    if !valid_query_session(session, request) {
        return false;
    }

    match session.state.as_str() {
        "closed" => response.target.is_none() && response.membership.is_none(),
        "open" => match response.target.as_ref() {
            None => response.membership.is_none(),
            Some(target) => response.membership.as_ref().is_some_and(|membership| {
                valid_target_wire(target)
                    && valid_member_query_for_session(membership, target, session.session_id)
            }),
        },
        _ => false,
    }
}

fn valid_query_session(
    session: &AgentSessionResponse,
    request: &AgentRequestParticipationQueryRequest,
) -> bool {
    valid_session(session)
        && session.created_at_ms >= 0
        && session.updated_at_ms >= session.created_at_ms
        && request
            .session_id
            .is_none_or(|session_id| session_id == session.session_id)
        && (request.session_id.is_some()
            || session.request_id == agent_request_session_open_request_id(request.operation_id))
}

fn valid_target_wire(target: &AgentRequestParticipationTargetWire) -> bool {
    validate_scope(&target.scope).is_ok()
        && require_identity(target.activity_id).is_ok()
        && valid_agent_request_identity(
            target.agent_request_id,
            &target.agent_request_state,
            target.agent_request_revision,
        )
        && require_identity(target.profile_id).is_ok()
        && require_identity(target.runtime_session_id).is_ok()
}

fn valid_member_query_for_session(
    response: &MemberRegistrationQueryResponse,
    target: &AgentRequestParticipationTargetWire,
    session_id: Uuid,
) -> bool {
    let member_target = MemberTarget {
        scope: target.scope.clone(),
        activity_id: target.activity_id,
        session_id,
    };
    if validate_member_target(&member_target).is_err() {
        return false;
    }

    let request = MemberRegistrationQueryRequest {
        request_id: expected_member_request_id(&member_target),
        target: member_target,
    };
    valid_member_query_response(response, &request)
}

fn valid_agent_request_identity(request_id: Uuid, state: &str, revision: u64) -> bool {
    !request_id.is_nil()
        && ((state == request_state_label(AgentRequestState::ResolutionPending) && revision == 1)
            || (state == request_state_label(AgentRequestState::ResolutionReserved)
                && revision == 2)
            || (state == request_state_label(AgentRequestState::Fulfilled) && revision == 3))
}

fn expected_member_request_id(target: &MemberTarget) -> Uuid {
    member_registration_request_id(
        target.scope.workspace_id,
        target.activity_id,
        target.session_id,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use heterarchy_orchestration_runtime::leadership::member_agent_id;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::wire::leadership::WorkspaceQueryScope;
    use crate::wire::membership::{MemberActivationWire, MemberPhaseWire};

    fn scope() -> WorkspaceQueryScope {
        WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 3,
            workspace_revision: 4,
            locator: "workspace".to_owned(),
        }
    }

    fn member_target(session_id: Uuid) -> MemberTarget {
        MemberTarget {
            scope: scope(),
            activity_id: Uuid::from_u128(5),
            session_id,
        }
    }

    fn target_wire() -> AgentRequestParticipationTargetWire {
        AgentRequestParticipationTargetWire {
            scope: scope(),
            activity_id: Uuid::from_u128(5),
            agent_request_id: Uuid::from_u128(7),
            agent_request_state: "fulfilled".to_owned(),
            agent_request_revision: 3,
            profile_id: Uuid::from_u128(8),
            runtime_session_id: Uuid::from_u128(9),
        }
    }

    fn session(session_id: Uuid, request_id: Uuid, state: &str) -> AgentSessionResponse {
        AgentSessionResponse {
            session_id,
            request_id,
            state: state.to_owned(),
            revision: 2,
            created_at_ms: 10,
            updated_at_ms: 11,
        }
    }

    fn member_query(session_id: Uuid) -> MemberRegistrationQueryResponse {
        let target = member_target(session_id);
        MemberRegistrationQueryResponse {
            request_id: expected_member_request_id(&target),
            target,
            progress: None,
            commit: None,
            activation: None,
        }
    }

    fn mutation_response(
        operation_id: Uuid,
        target: MemberTarget,
    ) -> AgentRequestParticipationResponse {
        AgentRequestParticipationResponse {
            operation_id,
            agent_request_id: Uuid::from_u128(7),
            agent_request_state: "fulfilled".to_owned(),
            agent_request_revision: 3,
            profile_id: Uuid::from_u128(8),
            runtime_session_id: Uuid::from_u128(9),
            membership: MemberActivationWire {
                request_id: expected_member_request_id(&target),
                participant_id: Uuid::from_u128(10),
                agent_id: member_agent_id(
                    target.scope.workspace_id,
                    target.activity_id,
                    target.session_id,
                )
                .into_uuid(),
                membership_digest: "ab".repeat(32),
                activated_at_ms: 12,
                phase: MemberPhaseWire::Ready,
            },
            target,
            session_created: None,
            session_revision: None,
        }
    }

    fn query_response(
        operation_id: Uuid,
        session: Option<AgentSessionResponse>,
        target: Option<AgentRequestParticipationTargetWire>,
        membership: Option<MemberRegistrationQueryResponse>,
    ) -> AgentRequestParticipationQueryResponse {
        AgentRequestParticipationQueryResponse {
            operation_id,
            session,
            target,
            membership,
        }
    }

    #[test]
    fn mutations_require_exact_ready_membership_and_receipt_shape() {
        let operation_id = Uuid::from_u128(6);
        let session_id = Uuid::from_u128(11);
        let request = AgentRequestJoinRequest {
            operation_id,
            session_id,
        };
        let response = mutation_response(operation_id, member_target(session_id));
        assert!(valid_join_response(&response, &request));

        let mut mismatched_session = response.clone();
        mismatched_session.target.session_id = Uuid::from_u128(12);
        assert!(!valid_join_response(&mismatched_session, &request));

        let mut unexpected_receipt = response.clone();
        unexpected_receipt.session_created = Some(false);
        assert!(!valid_join_response(&unexpected_receipt, &request));

        let mut bad_membership = response.clone();
        bad_membership.membership.phase = MemberPhaseWire::MembershipCommitted;
        assert!(!valid_join_response(&bad_membership, &request));

        let mut bad_state = response;
        bad_state.agent_request_state = "cancelled".to_owned();
        assert!(!valid_join_response(&bad_state, &request));

        let mut activated = mutation_response(operation_id, member_target(session_id));
        activated.session_created = Some(false);
        activated.session_revision = Some(1);
        let activation_request = AgentRequestActivateRequest { operation_id };
        assert!(valid_activate_response(&activated, &activation_request));
        activated.session_revision = Some(0);
        assert!(!valid_activate_response(&activated, &activation_request));
    }

    #[test]
    fn requests_reject_nil_operation_and_session_identities() {
        assert!(
            validate_join_request(&AgentRequestJoinRequest {
                operation_id: Uuid::from_u128(6),
                session_id: Uuid::from_u128(11),
            })
            .is_ok()
        );
        assert!(
            validate_join_request(&AgentRequestJoinRequest {
                operation_id: Uuid::nil(),
                session_id: Uuid::from_u128(11),
            })
            .is_err()
        );
        assert!(
            validate_join_request(&AgentRequestJoinRequest {
                operation_id: Uuid::from_u128(6),
                session_id: Uuid::nil(),
            })
            .is_err()
        );
        assert!(
            validate_activate_request(&AgentRequestActivateRequest {
                operation_id: Uuid::nil(),
            })
            .is_err()
        );
        assert!(
            validate_query_request(&AgentRequestParticipationQueryRequest {
                operation_id: Uuid::from_u128(6),
                session_id: Some(Uuid::nil()),
            })
            .is_err()
        );
    }

    #[test]
    fn query_requires_full_exact_membership_for_an_open_linked_session()
    -> Result<(), Box<dyn std::error::Error>> {
        let operation_id = Uuid::from_u128(6);
        let session_id = Uuid::from_u128(11);
        let explicit = AgentRequestParticipationQueryRequest {
            operation_id,
            session_id: Some(session_id),
        };
        let target = target_wire();
        let membership = member_query(session_id);
        let linked = query_response(
            operation_id,
            Some(session(session_id, Uuid::from_u128(12), "open")),
            Some(target.clone()),
            Some(membership.clone()),
        );
        assert!(valid_participation_query_response(&linked, &explicit));

        let mut mismatched_session = linked.clone();
        mismatched_session
            .session
            .as_mut()
            .ok_or("session missing")?
            .session_id = Uuid::from_u128(13);
        assert!(!valid_participation_query_response(
            &mismatched_session,
            &explicit
        ));

        let mut mismatched_member = linked.clone();
        mismatched_member
            .membership
            .as_mut()
            .ok_or("membership missing")?
            .target
            .scope
            .workspace_id = Uuid::from_u128(99);
        assert!(!valid_participation_query_response(
            &mismatched_member,
            &explicit
        ));

        let mut missing_membership = linked.clone();
        missing_membership.membership = None;
        assert!(!valid_participation_query_response(
            &missing_membership,
            &explicit
        ));
        let mut wrong_operation = linked;
        wrong_operation.operation_id = Uuid::from_u128(99);
        assert!(!valid_participation_query_response(
            &wrong_operation,
            &explicit
        ));
        Ok(())
    }

    #[test]
    fn query_withholds_operation_details_for_absent_closed_or_unlinked_sessions() {
        let operation_id = Uuid::from_u128(6);
        let session_id = Uuid::from_u128(11);
        let explicit = AgentRequestParticipationQueryRequest {
            operation_id,
            session_id: Some(session_id),
        };
        let target = target_wire();
        let membership = member_query(session_id);
        let unlinked = query_response(
            operation_id,
            Some(session(session_id, Uuid::from_u128(12), "open")),
            None,
            None,
        );
        assert!(valid_participation_query_response(&unlinked, &explicit));

        let closed = query_response(
            operation_id,
            Some(session(session_id, Uuid::from_u128(12), "closed")),
            None,
            None,
        );
        assert!(valid_participation_query_response(&closed, &explicit));
        let closed_with_details = query_response(
            operation_id,
            closed.session.clone(),
            Some(target),
            Some(membership),
        );
        assert!(!valid_participation_query_response(
            &closed_with_details,
            &explicit
        ));
    }

    #[test]
    fn default_selector_requires_the_operation_derived_session_receipt() {
        let operation_id = Uuid::from_u128(6);
        let session_id = Uuid::from_u128(11);
        let none_selector = AgentRequestParticipationQueryRequest {
            operation_id,
            session_id: None,
        };
        let target = target_wire();
        let membership = member_query(session_id);
        let receipt = agent_request_session_open_request_id(operation_id);
        let operation_session = query_response(
            operation_id,
            Some(session(session_id, receipt, "open")),
            Some(target),
            Some(membership),
        );
        assert!(valid_participation_query_response(
            &operation_session,
            &none_selector
        ));
        let wrong_receipt = query_response(
            operation_id,
            Some(session(session_id, Uuid::from_u128(12), "open")),
            None,
            None,
        );
        assert!(!valid_participation_query_response(
            &wrong_receipt,
            &none_selector
        ));
        let no_session = query_response(operation_id, None, None, None);
        assert!(valid_participation_query_response(
            &no_session,
            &none_selector
        ));
        let no_session_with_details = query_response(
            operation_id,
            None,
            Some(target_wire()),
            Some(member_query(session_id)),
        );
        assert!(!valid_participation_query_response(
            &no_session_with_details,
            &none_selector
        ));
    }

    #[tokio::test]
    async fn malformed_mutation_replies_retain_exact_operation_readback_selectors()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let client = OrchestrationHttpClient::new(
            listener.local_addr()?,
            std::time::Duration::from_secs(1),
        )?;
        let operation_id = Uuid::from_u128(6);
        let session_id = Uuid::from_u128(11);
        let mut invalid_join = mutation_response(operation_id, member_target(session_id));
        invalid_join.operation_id = Uuid::from_u128(99);
        let join_body = serde_json::to_vec(&invalid_join)?;
        let mut invalid_activation = mutation_response(operation_id, member_target(session_id));
        invalid_activation.operation_id = Uuid::from_u128(99);
        invalid_activation.session_created = Some(false);
        invalid_activation.session_revision = Some(1);
        let activate_body = serde_json::to_vec(&invalid_activation)?;
        let server = tokio::spawn(async move {
            for (stage, body) in [("join", join_body), ("activate", activate_body)] {
                let (mut socket, _) = listener.accept().await?;
                let mut request = Vec::new();
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let mut chunk = [0_u8; 1024];
                    let count = socket.read(&mut chunk).await?;
                    if count == 0 || request.len() + count > 8 * 1024 {
                        return Err(std::io::Error::other("incomplete bounded request headers"));
                    }
                    request.extend_from_slice(&chunk[..count]);
                }
                let request = String::from_utf8_lossy(&request);
                let expected_path = format!(
                    "POST /internal/v1/provision-operations/{operation_id}/{stage} HTTP/1.1\r\n"
                );
                if !request.starts_with(&expected_path) {
                    return Err(std::io::Error::other(
                        "unexpected Agent-request participation route",
                    ));
                }
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                socket.write_all(headers.as_bytes()).await?;
                socket.write_all(&body).await?;
            }
            Ok::<_, std::io::Error>(())
        });

        let authorization = HeaderValue::from_static("Bearer fixture");
        let join_result = client
            .join_agent_request(
                &authorization,
                &AgentRequestJoinRequest {
                    operation_id,
                    session_id,
                },
            )
            .await;
        assert!(matches!(
            join_result,
            Err(ClientError::UnknownOutcome {
                readback: ReadbackTarget::AgentRequestParticipation {
                    operation_id: observed_operation_id,
                    session_id: Some(observed_session_id),
                }
            }) if observed_operation_id == operation_id && observed_session_id == session_id
        ));

        let activate_result = client
            .activate_agent_request(
                &authorization,
                &AgentRequestActivateRequest { operation_id },
            )
            .await;
        assert!(matches!(
            activate_result,
            Err(ClientError::UnknownOutcome {
                readback: ReadbackTarget::AgentRequestParticipation {
                    operation_id: observed_operation_id,
                    session_id: None,
                }
            }) if observed_operation_id == operation_id
        ));
        server.await??;
        Ok(())
    }
}
