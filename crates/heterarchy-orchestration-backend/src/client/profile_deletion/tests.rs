use hyper::header::HeaderValue;
use serde_json::{Value, json};
use uuid::Uuid;

use super::super::tests::{fixture, reply};
use super::*;
use crate::wire::profile_deletion::{
    AgentProfileProviderDeletionDeferredReasonWire, AgentProfileProviderDeletionFenceWire,
};

const CREDENTIAL: &str = "Bearer profile-deletion-fixture-secret";

fn ids() -> (Uuid, Uuid, Uuid, Uuid) {
    (
        Uuid::from_u128(10),
        Uuid::from_u128(11),
        Uuid::from_u128(12),
        Uuid::from_u128(13),
    )
}

fn profile(profile_id: Uuid) -> Value {
    json!({
        "profileId": profile_id,
        "requestId": Uuid::from_u128(20),
        "stableName": "operator",
        "displayName": "Operator",
        "providerFamily": "chatgpt",
        "preferredModel": null,
        "preferredReasoning": null,
        "baseCapabilities": ["backend", "rust"],
        "lifecycleState": "disabled",
        "creationOrigin": "user_manual",
        "createdAtMs": 1,
        "updatedAtMs": 2,
        "lastUsedAtMs": null,
        "revision": 2
    })
}

fn progress(request_id: Uuid) -> Value {
    json!({
        "requestId": request_id,
        "expectedProfileRevision": 1,
        "requestedAtMs": 3,
        "stage": "closing_admission",
        "pendingEffects": 0,
        "linkedConversationCount": null,
        "deletedConversationCount": 0,
        "deferredReason": null,
        "completedAtMs": null,
        "revision": null
    })
}

fn deletion_snapshot(profile_id: Uuid, request_id: Uuid) -> Value {
    json!({"profile": profile(profile_id), "deletion": progress(request_id)})
}

fn fence(action: &str) -> Value {
    let (operation_id, request_id, profile_id, runtime_session_id) = ids();
    json!({
        "operationId": operation_id,
        "requestId": request_id,
        "profileId": profile_id,
        "runtimeSessionId": runtime_session_id,
        "providerConversationId": "conversation-1",
        "expectedRevision": 7,
        "attemptNumber": 3,
        "action": action
    })
}

fn operation(state: &str, revision: u64, attempt_number: u64) -> Value {
    let (operation_id, request_id, profile_id, runtime_session_id) = ids();
    json!({
        "operationId": operation_id,
        "requestId": request_id,
        "profileId": profile_id,
        "runtimeSessionId": runtime_session_id,
        "providerConversationId": "conversation-1",
        "state": state,
        "revision": revision,
        "attemptNumber": attempt_number,
        "preparedAtMs": 4,
        "updatedAtMs": 8
    })
}

fn assert_request(captured: &[u8], method_path: &str) {
    let request = String::from_utf8_lossy(captured);
    assert!(
        request
            .lines()
            .next()
            .is_some_and(|line| line.contains(method_path))
    );
    assert!(
        request
            .to_ascii_lowercase()
            .contains("authorization: bearer profile-deletion-fixture-secret")
    );
}

#[tokio::test]
async fn profile_and_request_reads_use_exact_ids() -> Result<(), Box<dyn std::error::Error>> {
    let (profile_id, request_id, _, _) = ids();
    let response = reply("200 OK", &deletion_snapshot(profile_id, request_id));
    let (client, server) = fixture(Some(response)).await?;
    let result = client
        .profile_deletion_profile(&HeaderValue::from_static(CREDENTIAL), profile_id)
        .await?;
    assert_eq!(result.profile.profile_id, profile_id);
    assert_request(
        &server.await??,
        &format!("GET /internal/v1/agent-profiles/{profile_id}/deletion"),
    );

    let response = reply("200 OK", &deletion_snapshot(profile_id, request_id));
    let (client, server) = fixture(Some(response)).await?;
    let result = client
        .profile_deletion_request(&HeaderValue::from_static(CREDENTIAL), request_id)
        .await?;
    assert_eq!(
        result.deletion.as_ref().map(|value| value.request_id),
        Some(request_id)
    );
    assert_request(
        &server.await??,
        &format!("GET /internal/v1/agent-profile-deletions/{request_id}"),
    );

    let response = reply("200 OK", &deletion_snapshot(profile_id, request_id));
    let (client, server) = fixture(Some(response)).await?;
    let result = client
        .begin_profile_deletion(
            &HeaderValue::from_static(CREDENTIAL),
            profile_id,
            &AgentProfileDeletionRequest {
                request_id: request_id.to_string(),
                expected_profile_revision: 1,
            },
        )
        .await?;
    assert_eq!(
        result.deletion.as_ref().map(|value| value.request_id),
        Some(request_id)
    );
    assert_request(
        &server.await??,
        &format!("POST /internal/v1/agent-profiles/{profile_id}/deletion"),
    );
    Ok(())
}

#[tokio::test]
async fn lost_begin_returns_exact_readback_without_retrying_or_replaying()
-> Result<(), Box<dyn std::error::Error>> {
    let (profile_id, request_id, _, _) = ids();
    let (client, server) = fixture(None).await?;
    let result = client
        .begin_profile_deletion(
            &HeaderValue::from_static(CREDENTIAL),
            profile_id,
            &AgentProfileDeletionRequest {
                request_id: request_id.to_string(),
                expected_profile_revision: 1,
            },
        )
        .await;
    assert_eq!(
        result.err(),
        Some(ClientError::UnknownOutcome {
            readback: ReadbackTarget::ProfileDeletionRequest { request_id },
        })
    );
    let captured = server.await??;
    assert_request(
        &captured,
        &format!("POST /internal/v1/agent-profiles/{profile_id}/deletion"),
    );
    let request = String::from_utf8(captured)?;
    assert_eq!(request.matches("POST ").count(), 1);
    assert!(request.contains(&request_id.to_string()));
    Ok(())
}

#[tokio::test]
async fn begin_rejects_a_success_for_another_request_as_uncertain()
-> Result<(), Box<dyn std::error::Error>> {
    let (profile_id, request_id, _, _) = ids();
    let response = reply(
        "200 OK",
        &deletion_snapshot(profile_id, Uuid::from_u128(99)),
    );
    let (client, server) = fixture(Some(response)).await?;
    assert_eq!(
        client
            .begin_profile_deletion(
                &HeaderValue::from_static(CREDENTIAL),
                profile_id,
                &AgentProfileDeletionRequest {
                    request_id: request_id.to_string(),
                    expected_profile_revision: 1,
                },
            )
            .await
            .err(),
        Some(ClientError::UnknownOutcome {
            readback: ReadbackTarget::ProfileDeletionRequest { request_id },
        })
    );
    let _captured = server.await??;
    Ok(())
}

#[tokio::test]
async fn deletion_phase_requires_its_durable_revision_and_linked_count()
-> Result<(), Box<dyn std::error::Error>> {
    let (profile_id, request_id, _, _) = ids();
    let mut snapshot = deletion_snapshot(profile_id, request_id);
    snapshot["deletion"]["stage"] = json!("deleting_conversations");
    let response = reply("200 OK", &snapshot);
    let (client, server) = fixture(Some(response)).await?;
    assert_eq!(
        client
            .profile_deletion_profile(&HeaderValue::from_static(CREDENTIAL), profile_id)
            .await
            .err(),
        Some(ClientError::Protocol)
    );
    let _captured = server.await??;
    Ok(())
}

#[tokio::test]
async fn resume_uses_request_readback_and_checks_request_echo()
-> Result<(), Box<dyn std::error::Error>> {
    let (profile_id, request_id, _, _) = ids();
    let response = reply(
        "200 OK",
        &json!({"step":"waiting","snapshot":deletion_snapshot(profile_id, request_id)}),
    );
    let (client, server) = fixture(Some(response)).await?;
    let result = client
        .resume_profile_deletion(&HeaderValue::from_static(CREDENTIAL), request_id)
        .await?;
    assert_eq!(
        result
            .snapshot
            .deletion
            .as_ref()
            .map(|value| value.request_id),
        Some(request_id)
    );
    assert_request(
        &server.await??,
        &format!("POST /internal/v1/agent-profile-deletions/{request_id}/resume"),
    );
    Ok(())
}

#[tokio::test]
async fn provider_next_requires_a_closed_action_state_and_chatgpt_provider()
-> Result<(), Box<dyn std::error::Error>> {
    let response = reply("200 OK", &json!({"dispatch":null}));
    let (client, server) = fixture(Some(response)).await?;
    assert!(
        client
            .next_profile_deletion_provider_operation(&HeaderValue::from_static(CREDENTIAL))
            .await?
            .dispatch
            .is_none()
    );
    assert_request(
        &server.await??,
        "POST /internal/v1/agent-profile-provider-deletions/next",
    );

    let response = reply(
        "200 OK",
        &json!({"dispatch":{"fence":fence("execute"),"state":"effect_unknown","provider":"chatgpt"}}),
    );
    let (client, server) = fixture(Some(response)).await?;
    assert_eq!(
        client
            .next_profile_deletion_provider_operation(&HeaderValue::from_static(CREDENTIAL))
            .await
            .err(),
        Some(ClientError::UnknownOutcome {
            readback: ReadbackTarget::ProfileDeletionSelection,
        })
    );
    let _captured = server.await??;
    Ok(())
}

#[tokio::test]
async fn unknown_and_receipt_echo_exact_fences_and_receipt_action()
-> Result<(), Box<dyn std::error::Error>> {
    let (operation_id, request_id, profile_id, runtime_session_id) = ids();
    let unknown_fence = AgentProfileProviderDeletionFenceWire {
        operation_id,
        request_id,
        profile_id,
        runtime_session_id,
        provider_conversation_id: "conversation-1".to_owned(),
        expected_revision: 7,
        attempt_number: 3,
        action: AgentProfileProviderDeletionActionWire::Execute,
    };
    let response = reply(
        "200 OK",
        &json!({"fence":unknown_fence,"state":"effect_unknown","observedAtMs":10}),
    );
    let (client, server) = fixture(Some(response)).await?;
    client
        .mark_profile_deletion_provider_unknown(
            &HeaderValue::from_static(CREDENTIAL),
            &AgentProfileProviderDeletionUnknownRequest {
                fence: unknown_fence.clone(),
            },
        )
        .await?;
    assert_request(
        &server.await??,
        &format!("POST /internal/v1/agent-profile-provider-deletions/{operation_id}/unknown"),
    );

    let inspect_fence = AgentProfileProviderDeletionFenceWire {
        action: AgentProfileProviderDeletionActionWire::Inspect,
        expected_revision: 8,
        ..unknown_fence
    };
    let receipt = AgentProfileProviderDeletionReceiptWire::ProviderDeleteConfirmed {
        browser_session_id: "browser".to_owned(),
        tab_id: "tab".to_owned(),
        document_id: "document".to_owned(),
    };
    let response = reply(
        "200 OK",
        &json!({"fence":inspect_fence,"state":"deleted","observedAtMs":11}),
    );
    let (client, server) = fixture(Some(response)).await?;
    client
        .record_profile_deletion_provider_receipt(
            &HeaderValue::from_static(CREDENTIAL),
            &AgentProfileProviderDeletionReceiptRequest {
                fence: inspect_fence,
                receipt,
            },
        )
        .await?;
    assert_request(
        &server.await??,
        &format!("POST /internal/v1/agent-profile-provider-deletions/{operation_id}/receipt"),
    );
    Ok(())
}

#[tokio::test]
async fn lost_provider_receipt_retains_exact_fence_without_automatic_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let (operation_id, request_id, profile_id, runtime_session_id) = ids();
    let requested_fence = AgentProfileProviderDeletionFenceWire {
        operation_id,
        request_id,
        profile_id,
        runtime_session_id,
        provider_conversation_id: "conversation-1".to_owned(),
        expected_revision: 7,
        attempt_number: 3,
        action: AgentProfileProviderDeletionActionWire::Execute,
    };
    let (client, server) = fixture(None).await?;
    let result = client
        .record_profile_deletion_provider_receipt(
            &HeaderValue::from_static(CREDENTIAL),
            &AgentProfileProviderDeletionReceiptRequest {
                fence: requested_fence.clone(),
                receipt: AgentProfileProviderDeletionReceiptWire::NotStartedBeforeClick {
                    reason: AgentProfileProviderDeletionDeferredReasonWire::SurfaceUnavailable,
                    browser_session_id: None,
                    tab_id: None,
                    document_id: None,
                },
            },
        )
        .await;
    assert_eq!(
        result.err(),
        Some(ClientError::UnknownOutcome {
            readback: ReadbackTarget::ProfileDeletionProvider(Box::new(requested_fence)),
        })
    );
    let captured = server.await??;
    assert_request(
        &captured,
        &format!("POST /internal/v1/agent-profile-provider-deletions/{operation_id}/receipt"),
    );
    assert_eq!(String::from_utf8(captured)?.matches("POST ").count(), 1);
    Ok(())
}

#[tokio::test]
async fn selection_and_provider_query_are_observations_only()
-> Result<(), Box<dyn std::error::Error>> {
    let response = reply(
        "200 OK",
        &json!({"operation":null,"retryAuthorized":false,"requestEquivalenceProven":false}),
    );
    let (client, server) = fixture(Some(response)).await?;
    let selected = client
        .profile_deletion_provider_selection(&HeaderValue::from_static(CREDENTIAL))
        .await?;
    assert!(!selected.retry_authorized && !selected.request_equivalence_proven);
    assert_request(
        &server.await??,
        "GET /internal/v1/agent-profile-provider-deletions/in-flight",
    );

    let (operation_id, request_id, profile_id, runtime_session_id) = ids();
    let requested_fence = AgentProfileProviderDeletionFenceWire {
        operation_id,
        request_id,
        profile_id,
        runtime_session_id,
        provider_conversation_id: "conversation-1".to_owned(),
        expected_revision: 7,
        attempt_number: 3,
        action: AgentProfileProviderDeletionActionWire::Inspect,
    };
    let response = reply(
        "200 OK",
        &json!({
            "operation":operation("effect_unknown", 8, 3),
            "receipt":null,
            "retryAuthorized":false,
            "requestEquivalenceProven":false
        }),
    );
    let (client, server) = fixture(Some(response)).await?;
    let readback = client
        .profile_deletion_provider_readback(
            &HeaderValue::from_static(CREDENTIAL),
            &AgentProfileProviderDeletionUnknownRequest {
                fence: requested_fence,
            },
        )
        .await?;
    assert_eq!(readback.operation.operation_id, operation_id);
    assert!(!readback.retry_authorized && !readback.request_equivalence_proven);
    assert_request(
        &server.await??,
        &format!("POST /internal/v1/agent-profile-provider-deletions/{operation_id}/query"),
    );
    Ok(())
}

#[tokio::test]
async fn provider_readback_rejects_retry_claims_or_a_foreign_target()
-> Result<(), Box<dyn std::error::Error>> {
    let (operation_id, request_id, profile_id, runtime_session_id) = ids();
    let requested_fence = AgentProfileProviderDeletionFenceWire {
        operation_id,
        request_id,
        profile_id,
        runtime_session_id,
        provider_conversation_id: "conversation-1".to_owned(),
        expected_revision: 7,
        attempt_number: 3,
        action: AgentProfileProviderDeletionActionWire::Inspect,
    };
    let response = reply(
        "200 OK",
        &json!({
            "operation":operation("effect_unknown", 8, 3),
            "receipt":null,
            "retryAuthorized":true,
            "requestEquivalenceProven":false
        }),
    );
    let (client, server) = fixture(Some(response)).await?;
    assert_eq!(
        client
            .profile_deletion_provider_readback(
                &HeaderValue::from_static(CREDENTIAL),
                &AgentProfileProviderDeletionUnknownRequest {
                    fence: requested_fence.clone(),
                },
            )
            .await
            .err(),
        Some(ClientError::Protocol)
    );
    let _captured = server.await??;

    let response = reply(
        "200 OK",
        &json!({
            "operation":operation("deleted", 8, 3),
            "receipt":{
                "fence":fence("execute"),
                "receipt":{
                    "kind":"provider_delete_confirmed",
                    "browserSessionId":"browser",
                    "tabId":"tab",
                    "documentId":"document"
                },
                "observedAtMs":7,
                "retryAfterMs":null
            },
            "retryAuthorized":false,
            "requestEquivalenceProven":false
        }),
    );
    let (client, server) = fixture(Some(response)).await?;
    assert_eq!(
        client
            .profile_deletion_provider_readback(
                &HeaderValue::from_static(CREDENTIAL),
                &AgentProfileProviderDeletionUnknownRequest {
                    fence: requested_fence.clone(),
                },
            )
            .await
            .err(),
        Some(ClientError::Protocol)
    );
    let _captured = server.await??;

    let mut foreign_operation = operation("effect_unknown", 8, 3);
    foreign_operation["operationId"] = json!(Uuid::from_u128(99));
    let response = reply(
        "200 OK",
        &json!({
            "operation":foreign_operation,
            "receipt":null,
            "retryAuthorized":false,
            "requestEquivalenceProven":false
        }),
    );
    let (client, server) = fixture(Some(response)).await?;
    assert_eq!(
        client
            .profile_deletion_provider_readback(
                &HeaderValue::from_static(CREDENTIAL),
                &AgentProfileProviderDeletionUnknownRequest {
                    fence: requested_fence,
                },
            )
            .await
            .err(),
        Some(ClientError::Protocol)
    );
    let _captured = server.await??;
    Ok(())
}

#[test]
fn provider_receipt_action_is_checked_before_transport() {
    let not_started = AgentProfileProviderDeletionReceiptWire::NotStartedBeforeClick {
        reason: AgentProfileProviderDeletionDeferredReasonWire::SurfaceUnavailable,
        browser_session_id: None,
        tab_id: None,
        document_id: None,
    };
    assert!(
        validate_receipt(
            &not_started,
            AgentProfileProviderDeletionActionWire::Execute,
        )
        .is_ok()
    );
    assert_eq!(
        validate_receipt(
            &not_started,
            AgentProfileProviderDeletionActionWire::Inspect,
        ),
        Err(ClientError::InvalidRequest)
    );
}
