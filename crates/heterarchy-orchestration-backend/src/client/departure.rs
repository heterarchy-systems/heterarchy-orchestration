//! Exact detach mutation and durable departure readback client operations.

use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use crate::wire::departure::{
    DepartureQueryRequest, DepartureQueryResponse, DepartureReceiptWire, DepartureRequest,
    DepartureResponse, DepartureTarget,
};

use super::membership::validate_scope;
use super::{ClientError, OrchestrationHttpClient, ReadbackTarget, require_identity};

impl OrchestrationHttpClient {
    /// Detach one exact Agent through the caller's authenticated credential.
    ///
    /// # Errors
    /// Returns `UnknownOutcome` with exact durable readback identity when the
    /// response cannot prove the complete detach result.
    pub async fn detach_member(
        &self,
        authorization: &HeaderValue,
        request: &DepartureRequest,
    ) -> Result<DepartureResponse, ClientError> {
        validate_target(&request.target)?;
        let request_id = expected_detach_request_id(&request.target);
        let readback = departure_readback(&request.target, request_id);
        let response: DepartureResponse = self
            .call(
                authorization,
                Method::POST,
                departure_path(&request.target, "detach"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if response.target != request.target
            || response.request_id != request_id
            || !valid_receipt(&response.receipt, &request.target, request_id)
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read one exact historical departure receipt, including explicit absence.
    ///
    /// # Errors
    /// Returns `Protocol` for malformed or foreign readback; queries never
    /// manufacture mutation-unknown semantics.
    pub async fn member_departure(
        &self,
        authorization: &HeaderValue,
        request: &DepartureQueryRequest,
    ) -> Result<DepartureQueryResponse, ClientError> {
        validate_query_request(request)?;
        let response: DepartureQueryResponse = self
            .call(
                authorization,
                Method::POST,
                departure_path(&request.target, "departure/query"),
                Some(request),
                None,
            )
            .await?;
        if response.target != request.target
            || response.request_id != request.request_id
            || response
                .receipt
                .as_ref()
                .is_some_and(|receipt| !valid_receipt(receipt, &request.target, request.request_id))
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn departure_path(target: &DepartureTarget, suffix: &str) -> String {
    format!(
        "/internal/v1/workspaces/{}/members/{suffix}",
        target.scope.workspace_id
    )
}

fn departure_readback(target: &DepartureTarget, request_id: Uuid) -> ReadbackTarget {
    ReadbackTarget::MemberDeparture {
        workspace_id: target.scope.workspace_id,
        activity_id: target.activity_id,
        agent_id: target.agent_id,
        session_id: target.session_id,
        request_id,
        expected_agent_revision: target.expected_agent_revision,
    }
}

fn expected_detach_request_id(target: &DepartureTarget) -> Uuid {
    heterarchy_orchestration_runtime::departure::detach_request_id(
        target.scope.workspace_id,
        target.activity_id,
        target.agent_id,
        target.expected_agent_revision,
    )
}

fn validate_query_request(request: &DepartureQueryRequest) -> Result<(), ClientError> {
    validate_target(&request.target)?;
    require_identity(request.request_id)?;
    if request.request_id != expected_detach_request_id(&request.target) {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_target(target: &DepartureTarget) -> Result<(), ClientError> {
    validate_scope(&target.scope)?;
    require_identity(target.activity_id)?;
    require_identity(target.agent_id)?;
    require_identity(target.session_id)?;
    if target.expected_agent_revision <= 0
        || target.expected_agent_revision.checked_add(1).is_none()
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn valid_receipt(
    receipt: &DepartureReceiptWire,
    target: &DepartureTarget,
    request_id: Uuid,
) -> bool {
    let Some(result_revision) = target.expected_agent_revision.checked_add(1) else {
        return false;
    };
    receipt.request_id == request_id
        && receipt.workspace_id == target.scope.workspace_id
        && receipt.participant_id != Uuid::nil()
        && receipt.activity_id == target.activity_id
        && receipt.agent_id == target.agent_id
        && receipt.session_id == target.session_id
        && receipt.kind == crate::wire::departure::DepartureKindWire::Detach
        && receipt.expected_agent_revision == target.expected_agent_revision
        && receipt.result_agent_revision == result_revision
        && receipt.target_participant_id.is_none()
        && receipt.target_agent_id.is_none()
        && receipt.target_session_id.is_none()
        && receipt.result_coordinator_revision.is_none()
        && receipt.departed_event_id.is_some_and(|id| !id.is_nil())
        && receipt.handoff_started_event_id.is_none()
        && receipt.handoff_completed_event_id.is_none()
        && receipt.membership_revision > 0
        && receipt.source_reconciled_at_ms >= 0
        && receipt.committed_at_ms >= receipt.source_reconciled_at_ms
        && valid_metadata(&receipt.display_name)
        && valid_metadata(&receipt.declared_role)
}

fn valid_metadata(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 128 && !value.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::membership::MemberTransportWire;
    use crate::wire::{WorkspaceQueryScope, departure::DepartureKindWire};

    fn target() -> DepartureTarget {
        DepartureTarget {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            activity_id: Uuid::from_u128(5),
            agent_id: Uuid::from_u128(6),
            session_id: Uuid::from_u128(7),
            expected_agent_revision: 8,
        }
    }

    fn receipt(target: &DepartureTarget) -> DepartureReceiptWire {
        DepartureReceiptWire {
            request_id: expected_detach_request_id(target),
            workspace_id: target.scope.workspace_id,
            participant_id: Uuid::from_u128(10),
            activity_id: target.activity_id,
            agent_id: target.agent_id,
            session_id: target.session_id,
            kind: DepartureKindWire::Detach,
            expected_agent_revision: target.expected_agent_revision,
            result_agent_revision: target.expected_agent_revision + 1,
            target_participant_id: None,
            target_agent_id: None,
            target_session_id: None,
            result_coordinator_revision: None,
            departed_event_id: Some(Uuid::from_u128(11)),
            handoff_started_event_id: None,
            handoff_completed_event_id: None,
            membership_revision: 2,
            presence_cleared: true,
            source_reconciled_at_ms: 12,
            committed_at_ms: 13,
            display_name: "Ada".to_owned(),
            declared_role: "reviewer".to_owned(),
            transport: MemberTransportWire::Mcp,
        }
    }

    #[test]
    fn detach_request_requires_exact_non_nil_fences_and_safe_next_revision() {
        let mut value = target();
        assert!(validate_target(&value).is_ok());

        value.session_id = Uuid::nil();
        assert_eq!(validate_target(&value), Err(ClientError::InvalidRequest));

        value = target();
        value.expected_agent_revision = i64::MAX;
        assert_eq!(validate_target(&value), Err(ClientError::InvalidRequest));

        value = target();
        value.scope.locator = "bad\nlocator".to_owned();
        assert_eq!(validate_target(&value), Err(ClientError::InvalidRequest));
    }

    #[test]
    fn detach_receipt_rejects_foreign_fences_handoff_fields_and_bad_metadata() {
        let target = target();
        let request_id = expected_detach_request_id(&target);
        let valid = receipt(&target);
        assert!(valid_receipt(&valid, &target, request_id));

        let mut foreign = valid.clone();
        foreign.workspace_id = Uuid::from_u128(99);
        assert!(!valid_receipt(&foreign, &target, request_id));

        let mut wrong_revision = valid.clone();
        wrong_revision.result_agent_revision += 1;
        assert!(!valid_receipt(&wrong_revision, &target, request_id));

        let mut handoff = valid.clone();
        handoff.handoff_started_event_id = Some(Uuid::from_u128(12));
        assert!(!valid_receipt(&handoff, &target, request_id));

        let mut missing_event = valid.clone();
        missing_event.departed_event_id = None;
        assert!(!valid_receipt(&missing_event, &target, request_id));

        let mut nil_event = valid.clone();
        nil_event.departed_event_id = Some(Uuid::nil());
        assert!(!valid_receipt(&nil_event, &target, request_id));

        let mut bad_timestamp = valid.clone();
        bad_timestamp.committed_at_ms = bad_timestamp.source_reconciled_at_ms - 1;
        assert!(!valid_receipt(&bad_timestamp, &target, request_id));

        let mut bad_metadata = valid;
        bad_metadata.display_name = "Ada\n".to_owned();
        assert!(!valid_receipt(&bad_metadata, &target, request_id));
    }

    #[test]
    fn query_echo_and_optional_receipt_are_validated_as_readback_only() {
        let target = target();
        let request_id = expected_detach_request_id(&target);
        let request = DepartureQueryRequest {
            target: target.clone(),
            request_id,
        };
        assert!(validate_query_request(&request).is_ok());

        let mut foreign_nonce = request.clone();
        foreign_nonce.request_id = Uuid::from_u128(99);
        assert_eq!(
            validate_query_request(&foreign_nonce),
            Err(ClientError::InvalidRequest)
        );

        let receipt = receipt(&target);
        assert!(valid_receipt(&receipt, &target, request_id));
        let mut foreign_receipt = receipt;
        foreign_receipt.session_id = Uuid::from_u128(99);
        assert!(!valid_receipt(&foreign_receipt, &target, request_id));
    }
}
