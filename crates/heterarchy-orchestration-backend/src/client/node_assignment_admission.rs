//! Official no-retry client for pre-offer Assignment admission and pure recovery.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hyper::Method;
use hyper::header::HeaderValue;
use sha2::{Digest as _, Sha256};

use crate::wire::node_assignment_admission::{
    NodeAssignmentAdmissionDispositionWire, NodeAssignmentAdmissionReceiptWire,
    NodeAssignmentAdmissionRecoverResponse, NodeAssignmentAdmissionRequest,
    NodeAssignmentAdmissionResponse,
};

use super::membership::validate_scope;
use super::{ClientError, OrchestrationHttpClient, ReadbackTarget, require_identity};

const MAX_SOURCE_OFFER_BYTES: usize = 16_384;

impl OrchestrationHttpClient {
    /// Admit one exact pre-offer Assignment intent without automatic retry.
    ///
    /// # Errors
    /// Rejects malformed requests, typed service failures and malformed success.
    /// An uncertain outcome retains the complete immutable request for recovery.
    pub async fn admit_node_assignment(
        &self,
        authorization: &HeaderValue,
        request: &NodeAssignmentAdmissionRequest,
    ) -> Result<NodeAssignmentAdmissionResponse, ClientError> {
        validate_request(request)?;
        let readback = ReadbackTarget::NodeAssignmentAdmission(Box::new(request.clone()));
        let response: NodeAssignmentAdmissionResponse = self
            .call(
                authorization,
                Method::POST,
                admit_path(request),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if response.request != *request || !valid_receipt(&response.admission, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read the exact immutable admission outcome without retrying it.
    ///
    /// # Errors
    /// Rejects malformed requests, typed service failures and foreign or
    /// malformed recovery evidence.
    pub async fn recover_node_assignment_admission(
        &self,
        authorization: &HeaderValue,
        request: &NodeAssignmentAdmissionRequest,
    ) -> Result<NodeAssignmentAdmissionRecoverResponse, ClientError> {
        validate_request(request)?;
        let response: NodeAssignmentAdmissionRecoverResponse = self
            .call(
                authorization,
                Method::POST,
                recover_path(request),
                Some(request),
                None,
            )
            .await?;
        if response.request != *request
            || response.retry_authorized
            || response
                .admission
                .as_ref()
                .is_some_and(|receipt| !valid_receipt(receipt, request))
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn admit_path(request: &NodeAssignmentAdmissionRequest) -> String {
    format!(
        "/internal/v1/workspaces/{}/node-assignment-admissions/admit",
        request.scope.workspace_id
    )
}

fn recover_path(request: &NodeAssignmentAdmissionRequest) -> String {
    format!(
        "/internal/v1/workspaces/{}/node-assignment-admissions/admit/recover",
        request.scope.workspace_id
    )
}

fn validate_request(request: &NodeAssignmentAdmissionRequest) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    require_identity(request.activity_id)?;
    require_identity(request.coordinator_session_id)?;
    require_identity(request.coordinator_agent_id)?;
    require_identity(request.assignment_id)?;
    require_identity(request.source_fences.goal_id)?;
    require_identity(request.source_fences.node_id)?;
    require_identity(request.target.agent_id)?;
    require_identity(request.target.session_id)?;
    if request.coordinator_revision <= 0
        || request.source_fences.goal_created_at_ms < 0
        || request.source_fences.goal_revision <= 0
        || request.source_fences.dag_revision <= 0
        || request.source_fences.dag_base_goal_revision <= 0
        || request.source_fences.dag_head_revision <= 0
        || request.source_fences.node_revision <= 0
        || request.target.agent_revision <= 0
        || request.team_id.is_some_and(|id| id.is_nil())
        || request
            .expected_binding_revision
            .is_some_and(|revision| revision == 0 || revision >= i64::MAX.unsigned_abs())
    {
        return Err(ClientError::InvalidRequest);
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(&request.source_offer_bytes_base64)
        .map_err(|_| ClientError::InvalidRequest)?;
    if bytes.is_empty()
        || bytes.len() > MAX_SOURCE_OFFER_BYTES
        || URL_SAFE_NO_PAD.encode(&bytes) != request.source_offer_bytes_base64
    {
        return Err(ClientError::InvalidRequest);
    }
    let digest: [u8; 32] = Sha256::digest(&bytes).into();
    if encode_digest(&digest) != request.source_offer_sha256 {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn valid_receipt(
    receipt: &NodeAssignmentAdmissionReceiptWire,
    request: &NodeAssignmentAdmissionRequest,
) -> bool {
    let Ok(observed_at_ms) = offer_observed_at(request) else {
        return false;
    };
    receipt.workspace_id == request.scope.workspace_id
        && receipt.assignment_id == request.assignment_id
        && receipt.activity_id == request.activity_id
        && receipt.target_agent_id == request.target.agent_id
        && receipt.target_session_id == request.target.session_id
        && !receipt.target_participation_id.is_nil()
        && !receipt.target_participant_id.is_nil()
        && receipt.team_id == request.team_id
        && receipt.node_scope.activity_id == request.activity_id
        && receipt.node_scope.goal_id == request.source_fences.goal_id
        && receipt.node_scope.goal_created_at_ms == request.source_fences.goal_created_at_ms
        && receipt.node_scope.dag_revision == request.source_fences.dag_revision
        && receipt.node_scope.dag_base_goal_revision == request.source_fences.dag_base_goal_revision
        && receipt.node_scope.node_id == request.source_fences.node_id
        && receipt.binding_revision == request.expected_binding_revision
        && matches!(
            receipt.disposition,
            NodeAssignmentAdmissionDispositionWire::Pending
                | NodeAssignmentAdmissionDispositionWire::Active
        )
        && receipt.requester.activity_id == request.activity_id
        && receipt.requester.agent_id == request.coordinator_agent_id
        && receipt.requester.session_id == request.coordinator_session_id
        && match (
            receipt.requester.participation_id,
            receipt.requester.profile_id,
        ) {
            (Some(participation_id), Some(profile_id)) => {
                !participation_id.is_nil() && !profile_id.is_nil()
            }
            (None, None) => true,
            _ => false,
        }
        && !receipt.requester.participant_id.is_nil()
        && receipt.source_offer_sha256 == request.source_offer_sha256
        && receipt.observed_at_ms == observed_at_ms
        && match request.team_id {
            Some(_) => receipt
                .team_membership_revision
                .is_some_and(|revision| revision > 0),
            None => receipt.team_membership_revision.is_none(),
        }
}

fn offer_observed_at(request: &NodeAssignmentAdmissionRequest) -> Result<i64, ClientError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(&request.source_offer_bytes_base64)
        .map_err(|_| ClientError::InvalidRequest)?;
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| ClientError::InvalidRequest)?;
    value
        .get("observed_at_ms")
        .and_then(serde_json::Value::as_i64)
        .filter(|value| *value >= 0)
        .ok_or(ClientError::InvalidRequest)
}

fn encode_digest(digest: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::node_assignment_admission::{
        NodeAssignmentRequesterWire, NodeAssignmentScopeWire,
    };
    use serde_json::json;
    use std::error::Error;
    use uuid::Uuid;

    fn request() -> Result<NodeAssignmentAdmissionRequest, serde_json::Error> {
        let payload = serde_json::to_vec(&json!({ "observed_at_ms": 11 }))?;
        let digest: [u8; 32] = Sha256::digest(&payload).into();
        Ok(NodeAssignmentAdmissionRequest {
            scope: crate::wire::leadership::WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 1,
                workspace_revision: 1,
                locator: "hf1:test:1:Lg".to_owned(),
            },
            activity_id: Uuid::from_u128(3),
            coordinator_session_id: Uuid::from_u128(4),
            coordinator_agent_id: Uuid::from_u128(5),
            coordinator_revision: 1,
            assignment_id: Uuid::from_u128(6),
            source_fences: crate::wire::node_assignment_admission::NodeAssignmentSourceFencesWire {
                goal_id: Uuid::from_u128(7),
                goal_created_at_ms: 8,
                goal_revision: 1,
                dag_revision: 1,
                dag_base_goal_revision: 1,
                dag_head_revision: 1,
                node_id: Uuid::from_u128(9),
                node_revision: 1,
            },
            target: crate::wire::node_assignment_admission::NodeAssignmentTargetWire {
                agent_id: Uuid::from_u128(10),
                session_id: Uuid::from_u128(11),
                agent_revision: 1,
            },
            team_id: None,
            expected_binding_revision: Some(1),
            source_offer_bytes_base64: URL_SAFE_NO_PAD.encode(payload),
            source_offer_sha256: encode_digest(&digest),
        })
    }

    fn receipt(
        request: &NodeAssignmentAdmissionRequest,
        participation_id: Option<Uuid>,
        profile_id: Option<Uuid>,
    ) -> NodeAssignmentAdmissionReceiptWire {
        NodeAssignmentAdmissionReceiptWire {
            workspace_id: request.scope.workspace_id,
            assignment_id: request.assignment_id,
            activity_id: request.activity_id,
            target_agent_id: request.target.agent_id,
            target_session_id: request.target.session_id,
            target_participation_id: Uuid::from_u128(12),
            target_participant_id: Uuid::from_u128(13),
            team_id: None,
            team_membership_revision: None,
            node_scope: NodeAssignmentScopeWire {
                activity_id: request.activity_id,
                goal_id: request.source_fences.goal_id,
                goal_created_at_ms: request.source_fences.goal_created_at_ms,
                dag_revision: request.source_fences.dag_revision,
                dag_base_goal_revision: request.source_fences.dag_base_goal_revision,
                node_id: request.source_fences.node_id,
            },
            binding_revision: request.expected_binding_revision,
            disposition: NodeAssignmentAdmissionDispositionWire::Pending,
            requester: NodeAssignmentRequesterWire {
                activity_id: request.activity_id,
                agent_id: request.coordinator_agent_id,
                session_id: request.coordinator_session_id,
                participation_id,
                profile_id,
                participant_id: Uuid::from_u128(14),
            },
            source_offer_sha256: request.source_offer_sha256.clone(),
            observed_at_ms: 11,
        }
    }

    #[test]
    fn requester_identity_accepts_native_and_profile_pairs_but_rejects_mixed_or_nil()
    -> Result<(), Box<dyn Error>> {
        let request = request()?;
        assert!(valid_receipt(&receipt(&request, None, None), &request));
        assert!(valid_receipt(
            &receipt(
                &request,
                Some(Uuid::from_u128(15)),
                Some(Uuid::from_u128(16)),
            ),
            &request,
        ));
        assert!(!valid_receipt(
            &receipt(&request, Some(Uuid::from_u128(15)), None),
            &request,
        ));
        assert!(!valid_receipt(
            &receipt(&request, None, Some(Uuid::from_u128(16))),
            &request,
        ));

        let mut nil_participant = receipt(&request, None, None);
        nil_participant.requester.participant_id = Uuid::nil();
        assert!(!valid_receipt(&nil_participant, &request));
        Ok(())
    }
}
