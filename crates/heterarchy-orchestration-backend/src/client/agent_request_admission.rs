//! Bounded client for complete `AgentRequest` admission and immutable readback.

use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use crate::wire::agent_request_admission::{
    AdmissionRequestStateWire, AdmissionResolutionKindWire, AdmissionResolutionSlotWire,
    AdmissionResolutionWire, AgentRequestAdmissionReceiptWire,
    AgentRequestAdmissionRecoverResponse, AgentRequestAdmissionRequest,
    AgentRequestAdmissionResponse, AgentRequestAdmissionSnapshotWire,
};
use crate::wire::leadership::WorkspaceQueryScope;
use crate::wire::provisioning_operations::ProvisionOperationWire;
use crate::wire::workspace_project::valid_provider_project_reference;

use super::{ClientError, OrchestrationHttpClient, ReadbackTarget, require_identity};

const MAX_LOCATOR_BYTES: usize = 4_096;
const MAX_OBJECTIVE_BYTES: usize = 4_096;
const MAX_CAPABILITY_BYTES: usize = 128;
const MAX_REASON_BYTES: usize = 2_048;
const MAX_REQUESTED_SLOTS: u8 = 4;

impl OrchestrationHttpClient {
    /// Run complete scoped admission using the caller's original credential.
    ///
    /// The mutation is never retried automatically. Any uncertain or malformed
    /// successful reply carries the full immutable request for exact readback.
    ///
    /// # Errors
    /// Rejects invalid request values, returns typed service rejection, or
    /// retains exact request identity when the durable outcome is uncertain.
    pub async fn admit_agent_request(
        &self,
        authorization: &HeaderValue,
        request: &AgentRequestAdmissionRequest,
    ) -> Result<AgentRequestAdmissionResponse, ClientError> {
        validate_request(request)?;
        let readback = ReadbackTarget::AgentRequestAdmission(Box::new(request.clone()));
        let response: AgentRequestAdmissionResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/agent-requests/admit",
                    request.scope.workspace_id
                ),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_admission_response(&response, request) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read partial or complete evidence for the exact immutable request.
    ///
    /// Recovery is read-only and never grants permission to retry admission or
    /// release a provider effect.
    ///
    /// # Errors
    /// Rejects invalid request values, typed service failures, or malformed
    /// recovery evidence.
    pub async fn recover_agent_request_admission(
        &self,
        authorization: &HeaderValue,
        request: &AgentRequestAdmissionRequest,
    ) -> Result<AgentRequestAdmissionRecoverResponse, ClientError> {
        validate_request(request)?;
        let response: AgentRequestAdmissionRecoverResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/agent-requests/admit/recover",
                    request.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if !valid_recovery_response(&response, request) {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn validate_request(request: &AgentRequestAdmissionRequest) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    require_identity(request.activity_id)?;
    require_identity(request.coordinator_session_id)?;
    require_identity(request.coordinator_agent_id)?;
    require_identity(request.request_id)?;
    if request.coordinator_revision <= 0
        || request.objective.trim().is_empty()
        || request.objective.len() > MAX_OBJECTIVE_BYTES
        || request.capability.trim().is_empty()
        || request.capability.len() > MAX_CAPABILITY_BYTES
        || !(1..=MAX_REQUESTED_SLOTS).contains(&request.count)
        || request.reason.trim().is_empty()
        || request.reason.len() > MAX_REASON_BYTES
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_scope(scope: &WorkspaceQueryScope) -> Result<(), ClientError> {
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

fn valid_admission_response(
    response: &AgentRequestAdmissionResponse,
    request: &AgentRequestAdmissionRequest,
) -> bool {
    &response.request == request
        && valid_receipt(
            &response.receipt,
            request.request_id,
            request.scope.workspace_id,
            request.activity_id,
            request.count,
        )
        && matches!(
            response.receipt.state,
            AdmissionRequestStateWire::ResolutionReserved | AdmissionRequestStateWire::Fulfilled
        )
}

fn valid_recovery_response(
    response: &AgentRequestAdmissionRecoverResponse,
    request: &AgentRequestAdmissionRequest,
) -> bool {
    &response.request == request
        && response
            .snapshot
            .as_ref()
            .is_none_or(|snapshot| valid_snapshot(snapshot, request))
}

fn valid_snapshot(
    snapshot: &AgentRequestAdmissionSnapshotWire,
    request: &AgentRequestAdmissionRequest,
) -> bool {
    let receipt = &snapshot.receipt;
    if !valid_receipt(
        receipt,
        request.request_id,
        request.scope.workspace_id,
        request.activity_id,
        request.count,
    ) {
        return false;
    }

    match receipt.state {
        AdmissionRequestStateWire::ResolutionPending => {
            snapshot.resolution.is_none() && snapshot.operations.is_empty()
        }
        AdmissionRequestStateWire::ResolutionReserved | AdmissionRequestStateWire::Fulfilled => {
            let Some(resolution) = snapshot.resolution.as_ref() else {
                return false;
            };
            valid_resolution(resolution, receipt)
                && valid_operations(&snapshot.operations, resolution, receipt.request_id)
        }
    }
}

fn valid_receipt(
    receipt: &AgentRequestAdmissionReceiptWire,
    request_id: Uuid,
    workspace_id: Uuid,
    activity_id: Uuid,
    count: u8,
) -> bool {
    receipt.request_id == request_id
        && receipt.workspace_id == workspace_id
        && receipt.activity_id == activity_id
        && receipt.count == count
        && (1..=MAX_REQUESTED_SLOTS).contains(&receipt.count)
        && valid_state_revision(receipt.state, receipt.revision)
}

fn valid_state_revision(state: AdmissionRequestStateWire, revision: u64) -> bool {
    match state {
        AdmissionRequestStateWire::ResolutionPending => revision == 1,
        AdmissionRequestStateWire::ResolutionReserved => revision == 2,
        AdmissionRequestStateWire::Fulfilled => revision == 3,
    }
}

fn valid_resolution(
    resolution: &AdmissionResolutionWire,
    receipt: &AgentRequestAdmissionReceiptWire,
) -> bool {
    resolution.request_id == receipt.request_id
        && resolution.workspace_id == receipt.workspace_id
        && resolution.request_state == receipt.state
        && resolution.request_revision == receipt.revision
        && matches!(
            resolution.request_state,
            AdmissionRequestStateWire::ResolutionReserved | AdmissionRequestStateWire::Fulfilled
        )
        && resolution.slots.len() == usize::from(receipt.count)
        && resolution.slots.iter().enumerate().all(|(index, slot)| {
            u8::try_from(index)
                .is_ok_and(|ordinal| slot.slot_ordinal == ordinal && valid_resolution_slot(slot))
        })
}

fn valid_resolution_slot(slot: &AdmissionResolutionSlotWire) -> bool {
    if slot.slot_ordinal >= MAX_REQUESTED_SLOTS
        || slot.claim_id.is_some_and(|id| id.is_nil())
        || slot.profile_id.is_some_and(|id| id.is_nil())
        || slot.runtime_session_id.is_some_and(|id| id.is_nil())
        || slot
            .runtime_session_revision
            .is_some_and(|revision| revision <= 0)
    {
        return false;
    }
    match slot.kind {
        AdmissionResolutionKindWire::ReuseCurrentScope
        | AdmissionResolutionKindWire::WakeNewRuntime => {
            slot.claim_id.is_some()
                && slot.profile_id.is_some()
                && slot.runtime_session_id.is_none()
                && slot.runtime_session_revision.is_none()
        }
        AdmissionResolutionKindWire::ReuseReadyRuntime
        | AdmissionResolutionKindWire::WakeExistingRuntime => {
            slot.claim_id.is_some()
                && slot.profile_id.is_some()
                && slot.runtime_session_id.is_some()
                && slot.runtime_session_revision.is_some()
        }
        AdmissionResolutionKindWire::ProvisionNew => {
            slot.claim_id.is_none()
                && slot.profile_id.is_none()
                && slot.runtime_session_id.is_none()
                && slot.runtime_session_revision.is_none()
        }
    }
}

fn valid_operations(
    operations: &[ProvisionOperationWire],
    resolution: &AdmissionResolutionWire,
    request_id: Uuid,
) -> bool {
    if operations.len() > usize::from(MAX_REQUESTED_SLOTS) {
        return false;
    }
    let mut previous_slot = None;
    let mut operation_ids = std::collections::HashSet::with_capacity(operations.len());
    for operation in operations {
        if !valid_operation(operation)
            || operation.request_id != request_id
            || operation.operation_id
                != heterarchy_orchestration_operations::AgentProvisionOperation::identity_for_slot(
                    request_id,
                    operation.slot_ordinal,
                )
            || !operation_ids.insert(operation.operation_id)
            || previous_slot.is_some_and(|previous| previous >= operation.slot_ordinal)
        {
            return false;
        }
        previous_slot = Some(operation.slot_ordinal);
        let Some(slot) = resolution.slots.get(usize::from(operation.slot_ordinal)) else {
            return false;
        };
        if !valid_operation_for_slot(operation, slot) {
            return false;
        }
    }
    true
}

fn valid_operation_for_slot(
    operation: &ProvisionOperationWire,
    slot: &AdmissionResolutionSlotWire,
) -> bool {
    let expected_effect = match slot.kind {
        AdmissionResolutionKindWire::ReuseCurrentScope => return false,
        AdmissionResolutionKindWire::ReuseReadyRuntime => "reuse_ready_runtime",
        AdmissionResolutionKindWire::WakeExistingRuntime => "wake_existing_runtime",
        AdmissionResolutionKindWire::WakeNewRuntime => "wake_new_runtime",
        AdmissionResolutionKindWire::ProvisionNew => "provision_new",
    };
    if operation.slot_ordinal != slot.slot_ordinal || operation.effect_kind != expected_effect {
        return false;
    }
    match slot.kind {
        AdmissionResolutionKindWire::ReuseCurrentScope => false,
        AdmissionResolutionKindWire::ReuseReadyRuntime
        | AdmissionResolutionKindWire::WakeExistingRuntime => {
            operation.profile_id == slot.profile_id
                && operation.runtime_session_id == slot.runtime_session_id
        }
        AdmissionResolutionKindWire::WakeNewRuntime => {
            operation.profile_id == slot.profile_id
                && (operation.state == "prepared" || operation.runtime_session_id.is_some())
        }
        AdmissionResolutionKindWire::ProvisionNew => {
            operation.profile_id.is_some() == operation.runtime_session_id.is_some()
                && (operation.state == "prepared"
                    || (operation.profile_id.is_some() && operation.runtime_session_id.is_some()))
        }
    }
}

fn valid_operation(operation: &ProvisionOperationWire) -> bool {
    !operation.operation_id.is_nil()
        && !operation.request_id.is_nil()
        && operation.slot_ordinal < MAX_REQUESTED_SLOTS
        && matches!(
            operation.effect_kind.as_str(),
            "wake_existing_runtime" | "wake_new_runtime" | "provision_new" | "reuse_ready_runtime"
        )
        && operation.provider_family == "chatgpt"
        && operation.profile_id.is_none_or(|id| !id.is_nil())
        && operation.runtime_session_id.is_none_or(|id| !id.is_nil())
        && matches!(
            operation.state.as_str(),
            "prepared"
                | "effect_pending"
                | "effect_unknown"
                | "observed"
                | "confirmed"
                | "failed"
                | "cancelled"
        )
        && super::provisioning_operations::valid_conversation(operation)
        && super::provisioning_operations::valid_failure(operation)
        && operation.prepared_at_ms >= 0
        && operation.updated_at_ms >= operation.prepared_at_ms
        && operation.revision > 0
        && (operation.state == "prepared"
            || (operation.profile_id.is_some() && operation.runtime_session_id.is_some()))
        && operation
            .project
            .as_ref()
            .is_none_or(valid_provider_project_reference)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::agent_request_admission::{
        AdmissionEffectScopeWire, AdmissionIndependenceWire, AdmissionResolutionKindWire,
        AgentRequestAdmissionRequest,
    };
    use hyper::header::HeaderValue;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn scope() -> WorkspaceQueryScope {
        WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 3,
            workspace_revision: 4,
            locator: "workspace".to_owned(),
        }
    }

    fn request() -> AgentRequestAdmissionRequest {
        AgentRequestAdmissionRequest {
            scope: scope(),
            activity_id: Uuid::from_u128(5),
            coordinator_session_id: Uuid::from_u128(6),
            coordinator_agent_id: Uuid::from_u128(7),
            coordinator_revision: 8,
            request_id: Uuid::from_u128(9),
            objective: "verify an independent capability".to_owned(),
            capability: "verification".to_owned(),
            count: 1,
            effect_scope: AdmissionEffectScopeWire::ReadOnly,
            independence: AdmissionIndependenceWire::IndependentFromCurrentWriter,
            reason: "additional evidence is useful".to_owned(),
        }
    }

    fn receipt(
        state: AdmissionRequestStateWire,
        revision: u64,
    ) -> AgentRequestAdmissionReceiptWire {
        AgentRequestAdmissionReceiptWire {
            request_id: Uuid::from_u128(9),
            workspace_id: Uuid::from_u128(1),
            activity_id: Uuid::from_u128(5),
            state,
            count: 1,
            created: false,
            revision,
        }
    }

    #[test]
    fn successful_admission_requires_exact_echo_and_a_committed_stage() {
        let request = request();
        let reserved = AgentRequestAdmissionResponse {
            request: request.clone(),
            receipt: receipt(AdmissionRequestStateWire::ResolutionReserved, 2),
        };
        assert!(valid_admission_response(&reserved, &request));

        let pending = AgentRequestAdmissionResponse {
            request: request.clone(),
            receipt: receipt(AdmissionRequestStateWire::ResolutionPending, 1),
        };
        assert!(!valid_admission_response(&pending, &request));

        let mut changed_echo = reserved;
        changed_echo.request.reason.push('!');
        assert!(!valid_admission_response(&changed_echo, &request));
    }

    #[test]
    fn recovery_accepts_pending_and_prepared_unbound_partial_evidence() {
        let request = request();
        let pending = AgentRequestAdmissionRecoverResponse {
            request: request.clone(),
            snapshot: Some(AgentRequestAdmissionSnapshotWire {
                receipt: receipt(AdmissionRequestStateWire::ResolutionPending, 1),
                resolution: None,
                operations: Vec::new(),
            }),
        };
        assert!(valid_recovery_response(&pending, &request));

        let resolution = AdmissionResolutionWire {
            request_id: request.request_id,
            workspace_id: request.scope.workspace_id,
            request_state: AdmissionRequestStateWire::ResolutionReserved,
            request_revision: 2,
            slots: vec![AdmissionResolutionSlotWire {
                slot_ordinal: 0,
                kind: AdmissionResolutionKindWire::ProvisionNew,
                claim_id: None,
                profile_id: None,
                runtime_session_id: None,
                runtime_session_revision: None,
            }],
        };
        let partial_operation = ProvisionOperationWire {
            operation_id:
                heterarchy_orchestration_operations::AgentProvisionOperation::identity_for_slot(
                    request.request_id,
                    0,
                ),
            request_id: request.request_id,
            slot_ordinal: 0,
            effect_kind: "provision_new".to_owned(),
            provider_family: "chatgpt".to_owned(),
            profile_id: None,
            runtime_session_id: None,
            state: "prepared".to_owned(),
            provider_conversation_id: None,
            reported_failure_code: None,
            prepared_at_ms: 10,
            updated_at_ms: 10,
            revision: 1,
            uncertainty_retirement: None,
            project: None,
        };
        let partial = AgentRequestAdmissionRecoverResponse {
            request: request.clone(),
            snapshot: Some(AgentRequestAdmissionSnapshotWire {
                receipt: receipt(AdmissionRequestStateWire::ResolutionReserved, 2),
                resolution: Some(resolution.clone()),
                operations: vec![partial_operation.clone()],
            }),
        };
        assert!(valid_recovery_response(&partial, &request));

        let mut released_without_target = partial;
        if let Some(operation) = released_without_target
            .snapshot
            .as_mut()
            .and_then(|snapshot| snapshot.operations.first_mut())
        {
            operation.state = "effect_pending".to_owned();
        }
        assert!(!valid_recovery_response(&released_without_target, &request));

        let mut wrong_slot = partial_operation;
        wrong_slot.slot_ordinal = 1;
        assert!(!valid_operations(
            &[wrong_slot],
            &resolution,
            request.request_id
        ));
    }

    #[tokio::test]
    async fn malformed_admission_reply_keeps_exact_readback_and_original_authorization()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let client = OrchestrationHttpClient::new(
            listener.local_addr()?,
            std::time::Duration::from_secs(1),
        )?;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut request_bytes = [0_u8; 8_192];
            let count = socket.read(&mut request_bytes).await?;
            let request_text =
                String::from_utf8_lossy(&request_bytes[..count]).to_ascii_lowercase();
            let body = b"{}";
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(headers.as_bytes()).await?;
            socket.write_all(body).await?;
            Ok::<_, std::io::Error>(request_text)
        });
        let request = request();
        let result = client
            .admit_agent_request(
                &HeaderValue::from_static("Bearer original-caller"),
                &request,
            )
            .await;
        assert_eq!(
            result,
            Err(ClientError::UnknownOutcome {
                readback: ReadbackTarget::AgentRequestAdmission(Box::new(request.clone()))
            })
        );
        let received = server.await??;
        assert!(received.contains("post /internal/v1/workspaces/00000000-0000-0000-0000-000000000001/agent-requests/admit http/1.1"));
        assert!(received.contains("authorization: bearer original-caller"));
        Ok(())
    }
}
