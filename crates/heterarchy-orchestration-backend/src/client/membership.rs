//! Official bounded client for logical Workspace membership registration.

use std::collections::HashSet;

use heterarchy_orchestration_runtime::{
    leadership::member_agent_id, membership::member_registration_request_id,
};
use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use crate::wire::WorkspaceQueryScope;
use crate::wire::membership::{
    MemberActivateRequest, MemberActivateResponse, MemberActivationWire, MemberCommitWire,
    MemberConfirmRequest, MemberConfirmResponse, MemberPhaseWire, MemberProgressWire,
    MemberRegisterRequest, MemberRegisterResponse, MemberRegistrationQueryRequest,
    MemberRegistrationQueryResponse, MemberTarget,
};

use super::{ClientError, OrchestrationHttpClient, ReadbackTarget, require_identity};

const MAX_LOCATOR_BYTES: usize = 4_096;
const MAX_MEMBER_METADATA_BYTES: usize = 128;
const MAX_MEMBER_RECIPIENTS: usize = 64;

impl OrchestrationHttpClient {
    /// Prepare or exactly replay one caller-owned Workspace membership.
    ///
    /// The server derives the replay request identity from the exact target.
    /// A lost or malformed reply carries that identity for canonical readback;
    /// this method never retries.
    ///
    /// # Errors
    /// Returns invalid input, typed service rejection, bounded transport
    /// failure, or an uncertain outcome carrying the exact registration key.
    pub async fn register_member(
        &self,
        authorization: &HeaderValue,
        request: &MemberRegisterRequest,
    ) -> Result<MemberRegisterResponse, ClientError> {
        validate_register_request(request)?;
        let request_id = expected_member_request_id(&request.target);
        let readback = member_readback(&request.target, request_id);
        let response: MemberRegisterResponse = self
            .call(
                authorization,
                Method::POST,
                member_path(&request.target, "register"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if response.target != request.target
            || !valid_progress(&response.progress, &request.target, request_id)
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Atomically confirm one exact registration and bounded notification set.
    ///
    /// # Errors
    /// Returns invalid input, typed service rejection, bounded transport
    /// failure, or an uncertain outcome carrying the exact registration key.
    pub async fn confirm_member(
        &self,
        authorization: &HeaderValue,
        request: &MemberConfirmRequest,
    ) -> Result<MemberConfirmResponse, ClientError> {
        validate_confirm_request(request)?;
        let readback = member_readback(&request.target, request.request_id);
        let response: MemberConfirmResponse = self
            .call(
                authorization,
                Method::POST,
                member_path(&request.target, "confirm"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if response.target != request.target
            || !valid_commit(
                &response.commit,
                &request.target,
                request.request_id,
                None,
                None,
            )
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Activate one exact committed registration using its source witness.
    ///
    /// # Errors
    /// Returns invalid input, typed service rejection, bounded transport
    /// failure, or an uncertain outcome carrying the exact registration key.
    pub async fn activate_member(
        &self,
        authorization: &HeaderValue,
        request: &MemberActivateRequest,
    ) -> Result<MemberActivateResponse, ClientError> {
        validate_registration_request(&request.target, request.request_id)?;
        let readback = member_readback(&request.target, request.request_id);
        let response: MemberActivateResponse = self
            .call(
                authorization,
                Method::POST,
                member_path(&request.target, "activate"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if response.target != request.target
            || !valid_activation(
                &response.activation,
                &request.target,
                request.request_id,
                None,
                None,
            )
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read the complete exact registration projection, including explicit
    /// absence for stages that have not been recorded.
    ///
    /// # Errors
    /// Returns invalid input, typed service rejection, bounded transport
    /// failure, or a protocol error for a malformed projection.
    pub async fn member_registration(
        &self,
        authorization: &HeaderValue,
        request: &MemberRegistrationQueryRequest,
    ) -> Result<MemberRegistrationQueryResponse, ClientError> {
        validate_registration_request(&request.target, request.request_id)?;
        let response: MemberRegistrationQueryResponse = self
            .call(
                authorization,
                Method::POST,
                member_path(&request.target, "query"),
                Some(request),
                None,
            )
            .await?;
        if !valid_query_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn member_path(target: &MemberTarget, stage: &str) -> String {
    format!(
        "/internal/v1/workspaces/{}/members/{stage}",
        target.scope.workspace_id
    )
}

fn member_readback(target: &MemberTarget, request_id: Uuid) -> ReadbackTarget {
    ReadbackTarget::MemberRegistration {
        workspace_id: target.scope.workspace_id,
        activity_id: target.activity_id,
        session_id: target.session_id,
        request_id,
    }
}

fn expected_member_request_id(target: &MemberTarget) -> Uuid {
    member_registration_request_id(
        target.scope.workspace_id,
        target.activity_id,
        target.session_id,
    )
}

fn expected_member_agent_id(target: &MemberTarget) -> Uuid {
    member_agent_id(
        target.scope.workspace_id,
        target.activity_id,
        target.session_id,
    )
    .into_uuid()
}

fn validate_register_request(request: &MemberRegisterRequest) -> Result<(), ClientError> {
    validate_member_target(&request.target)?;
    validate_member_metadata(&request.display_name)?;
    validate_member_metadata(&request.declared_role)
}

fn validate_confirm_request(request: &MemberConfirmRequest) -> Result<(), ClientError> {
    validate_registration_request(&request.target, request.request_id)?;
    if request.recipients.len() > MAX_MEMBER_RECIPIENTS {
        return Err(ClientError::InvalidRequest);
    }
    let mut recipients = HashSet::with_capacity(request.recipients.len());
    for recipient in &request.recipients {
        require_identity(*recipient)?;
        if !recipients.insert(*recipient) {
            return Err(ClientError::InvalidRequest);
        }
    }
    Ok(())
}

fn validate_registration_request(
    target: &MemberTarget,
    request_id: Uuid,
) -> Result<(), ClientError> {
    validate_member_target(target)?;
    require_identity(request_id)?;
    if request_id != expected_member_request_id(target) {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

pub(super) fn validate_member_target(target: &MemberTarget) -> Result<(), ClientError> {
    validate_scope(&target.scope)?;
    require_identity(target.activity_id)?;
    require_identity(target.session_id)
}

pub(super) fn validate_scope(scope: &WorkspaceQueryScope) -> Result<(), ClientError> {
    if scope.workspace_id.is_nil()
        || scope.location_id.is_nil()
        || scope.location_revision <= 0
        || scope.workspace_revision <= 0
        || scope.locator.is_empty()
        || scope.locator.len() > MAX_LOCATOR_BYTES
        || scope.locator.chars().any(char::is_control)
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_member_metadata(value: &str) -> Result<(), ClientError> {
    if value.trim().is_empty()
        || value.len() > MAX_MEMBER_METADATA_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn valid_progress(value: &MemberProgressWire, target: &MemberTarget, request_id: Uuid) -> bool {
    if value.workspace_id != target.scope.workspace_id
        || value.activity_id != target.activity_id
        || value.session_id != target.session_id
        || value.agent_id != expected_member_agent_id(target)
        || value.request_id != request_id
        || value.participant_id.is_nil()
        || value.migration_id.is_nil()
    {
        return false;
    }

    match value.phase {
        MemberPhaseWire::Pending => !value.reserved && value.reserved_at_ms.is_none(),
        MemberPhaseWire::Reserved
        | MemberPhaseWire::MembershipCommitted
        | MemberPhaseWire::Ready => {
            value.reserved && value.reserved_at_ms.is_some_and(|timestamp| timestamp >= 0)
        }
    }
}

fn valid_commit(
    value: &MemberCommitWire,
    target: &MemberTarget,
    request_id: Uuid,
    participant_id: Option<Uuid>,
    reserved_at_ms: Option<i64>,
) -> bool {
    value.request_id == request_id
        && request_id == expected_member_request_id(target)
        && !value.participant_id.is_nil()
        && participant_id.is_none_or(|id| id == value.participant_id)
        && valid_digest(&value.commit_digest)
        && !value.joined_event_id.is_nil()
        && value.committed_at_ms >= 0
        && reserved_at_ms.is_none_or(|timestamp| value.committed_at_ms >= timestamp)
}

pub(super) fn valid_activation(
    value: &MemberActivationWire,
    target: &MemberTarget,
    request_id: Uuid,
    participant_id: Option<Uuid>,
    committed_at_ms: Option<i64>,
) -> bool {
    value.request_id == request_id
        && request_id == expected_member_request_id(target)
        && !value.participant_id.is_nil()
        && participant_id.is_none_or(|id| id == value.participant_id)
        && value.agent_id == expected_member_agent_id(target)
        && valid_digest(&value.membership_digest)
        && value.activated_at_ms >= 0
        && committed_at_ms.is_none_or(|timestamp| value.activated_at_ms >= timestamp)
        && value.phase == MemberPhaseWire::Ready
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(super) fn valid_query_response(
    response: &MemberRegistrationQueryResponse,
    request: &MemberRegistrationQueryRequest,
) -> bool {
    if response.target != request.target || response.request_id != request.request_id {
        return false;
    }

    let Some(progress) = response.progress.as_ref() else {
        return response.commit.is_none() && response.activation.is_none();
    };
    if !valid_progress(progress, &request.target, request.request_id) {
        return false;
    }

    let participant_id = progress.participant_id;
    let reserved_at_ms = progress.reserved_at_ms;
    match progress.phase {
        MemberPhaseWire::Pending | MemberPhaseWire::Reserved => {
            response.commit.is_none() && response.activation.is_none()
        }
        MemberPhaseWire::MembershipCommitted => {
            response.activation.is_none()
                && response.commit.as_ref().is_some_and(|commit| {
                    valid_commit(
                        commit,
                        &request.target,
                        request.request_id,
                        Some(participant_id),
                        reserved_at_ms,
                    )
                })
        }
        MemberPhaseWire::Ready => {
            let Some(commit) = response.commit.as_ref() else {
                return false;
            };
            let Some(activation) = response.activation.as_ref() else {
                return false;
            };
            valid_commit(
                commit,
                &request.target,
                request.request_id,
                Some(participant_id),
                reserved_at_ms,
            ) && valid_activation(
                activation,
                &request.target,
                request.request_id,
                Some(participant_id),
                Some(commit.committed_at_ms),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::membership::MemberTransportWire;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn target() -> MemberTarget {
        MemberTarget {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            activity_id: Uuid::from_u128(5),
            session_id: Uuid::from_u128(6),
        }
    }

    fn register_request() -> MemberRegisterRequest {
        MemberRegisterRequest {
            target: target(),
            display_name: "Ada".to_owned(),
            declared_role: "reviewer".to_owned(),
            transport: MemberTransportWire::Mcp,
        }
    }

    fn progress(target: &MemberTarget, phase: MemberPhaseWire) -> MemberProgressWire {
        let reserved = phase != MemberPhaseWire::Pending;
        MemberProgressWire {
            workspace_id: target.scope.workspace_id,
            activity_id: target.activity_id,
            session_id: target.session_id,
            agent_id: expected_member_agent_id(target),
            request_id: expected_member_request_id(target),
            participant_id: Uuid::from_u128(7),
            migration_id: Uuid::from_u128(8),
            phase,
            reserved,
            reserved_at_ms: reserved.then_some(10),
        }
    }

    fn commit(request_id: Uuid, participant_id: Uuid, committed_at_ms: i64) -> MemberCommitWire {
        MemberCommitWire {
            request_id,
            participant_id,
            commit_digest: "ab".repeat(32),
            joined_event_id: Uuid::from_u128(9),
            committed_at_ms,
        }
    }

    fn activation(
        target: &MemberTarget,
        request_id: Uuid,
        participant_id: Uuid,
        activated_at_ms: i64,
    ) -> MemberActivationWire {
        MemberActivationWire {
            request_id,
            participant_id,
            agent_id: expected_member_agent_id(target),
            membership_digest: "cd".repeat(32),
            activated_at_ms,
            phase: MemberPhaseWire::Ready,
        }
    }

    #[test]
    fn member_requests_reject_invalid_scope_session_metadata_and_recipient_sets() {
        assert!(validate_register_request(&register_request()).is_ok());

        let mut invalid = register_request();
        invalid.target.scope.workspace_revision = 0;
        assert!(validate_register_request(&invalid).is_err());
        let mut invalid = register_request();
        invalid.target.session_id = Uuid::nil();
        assert!(validate_register_request(&invalid).is_err());
        let mut invalid = register_request();
        invalid.display_name = " ".to_owned();
        assert!(validate_register_request(&invalid).is_err());
        let mut invalid = register_request();
        invalid.declared_role = "x".repeat(MAX_MEMBER_METADATA_BYTES + 1);
        assert!(validate_register_request(&invalid).is_err());

        let mut confirm = MemberConfirmRequest {
            target: target(),
            request_id: expected_member_request_id(&target()),
            recipients: vec![Uuid::from_u128(10), Uuid::from_u128(11)],
        };
        assert!(validate_confirm_request(&confirm).is_ok());
        confirm.recipients[1] = confirm.recipients[0];
        assert!(validate_confirm_request(&confirm).is_err());
        confirm.recipients = vec![Uuid::nil()];
        assert!(validate_confirm_request(&confirm).is_err());
        // 65 is one above the public 64-recipient bound.
        confirm.recipients = (1_u128..=65).map(Uuid::from_u128).collect();
        assert!(validate_confirm_request(&confirm).is_err());
    }

    #[test]
    fn progress_commit_and_activation_validation_preserves_exact_stage_identity() {
        let target = target();
        let request_id = expected_member_request_id(&target);
        let participant_id = Uuid::from_u128(7);
        let pending = progress(&target, MemberPhaseWire::Pending);
        assert!(valid_progress(&pending, &target, request_id));

        let mut bad = pending.clone();
        bad.agent_id = Uuid::from_u128(99);
        assert!(!valid_progress(&bad, &target, request_id));
        let mut bad = pending.clone();
        bad.reserved_at_ms = Some(-1);
        assert!(!valid_progress(&bad, &target, request_id));
        let mut bad = pending;
        bad.reserved = true;
        assert!(!valid_progress(&bad, &target, request_id));

        let receipt = commit(request_id, participant_id, 12);
        assert!(valid_commit(
            &receipt,
            &target,
            request_id,
            Some(participant_id),
            Some(10)
        ));
        let mut malformed = receipt.clone();
        malformed.commit_digest = "AB".repeat(32);
        assert!(!valid_commit(
            &malformed,
            &target,
            request_id,
            Some(participant_id),
            Some(10)
        ));
        let mut malformed = receipt;
        malformed.committed_at_ms = 9;
        assert!(!valid_commit(
            &malformed,
            &target,
            request_id,
            Some(participant_id),
            Some(10)
        ));

        let ready = activation(&target, request_id, participant_id, 13);
        assert!(valid_activation(
            &ready,
            &target,
            request_id,
            Some(participant_id),
            Some(12)
        ));
    }

    #[test]
    fn registration_query_enforces_phase_receipt_consistency() {
        let target = target();
        let request_id = expected_member_request_id(&target);
        let request = MemberRegistrationQueryRequest {
            target: target.clone(),
            request_id,
        };
        let pending = MemberRegistrationQueryResponse {
            target: target.clone(),
            request_id,
            progress: Some(progress(&target, MemberPhaseWire::Pending)),
            commit: None,
            activation: None,
        };
        assert!(valid_query_response(&pending, &request));

        let ready_progress = progress(&target, MemberPhaseWire::Ready);
        let participant_id = ready_progress.participant_id;
        let mut ready = MemberRegistrationQueryResponse {
            target: target.clone(),
            request_id,
            progress: Some(ready_progress),
            commit: Some(commit(request_id, participant_id, 12)),
            activation: Some(activation(&target, request_id, participant_id, 13)),
        };
        assert!(valid_query_response(&ready, &request));
        ready.activation = None;
        assert!(!valid_query_response(&ready, &request));
        ready.progress = None;
        ready.commit = Some(commit(request_id, participant_id, 12));
        assert!(!valid_query_response(&ready, &request));
    }

    #[tokio::test]
    async fn malformed_register_reply_returns_exact_server_derived_readback()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let client = OrchestrationHttpClient::new(
            listener.local_addr()?,
            std::time::Duration::from_secs(1),
        )?;
        let server = tokio::spawn(async move {
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
            if !request.starts_with("POST /internal/v1/workspaces/00000000-0000-0000-0000-000000000001/members/register HTTP/1.1\r\n") {
                return Err(std::io::Error::other("unexpected membership route"));
            }
            let body = b"{}";
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(headers.as_bytes()).await?;
            socket.write_all(body).await?;
            Ok::<_, std::io::Error>(())
        });

        let request = register_request();
        let expected = target();
        let expected_request_id = expected_member_request_id(&expected);
        assert!(matches!(
            client
                .register_member(&HeaderValue::from_static("Bearer fixture"), &request)
                .await,
            Err(ClientError::UnknownOutcome {
                readback: ReadbackTarget::MemberRegistration {
                    workspace_id,
                    activity_id,
                    session_id,
                    request_id,
                }
            }) if workspace_id == expected.scope.workspace_id
                && activity_id == expected.activity_id
                && session_id == expected.session_id
                && request_id == expected_request_id
        ));
        server.await??;
        Ok(())
    }
}
