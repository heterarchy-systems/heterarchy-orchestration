use std::{error::Error, net::SocketAddr, time::Duration};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use heterarchy_orchestration_backend::{
    client::{ClientError, OrchestrationHttpClient, ReadbackTarget},
    wire::{
        leadership::WorkspaceQueryScope,
        node_assignment_admission::{
            NodeAssignmentAdmissionRequest, NodeAssignmentRequesterWire,
            NodeAssignmentSourceFencesWire, NodeAssignmentTargetWire,
        },
    },
};
use hyper::header::HeaderValue;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

fn request() -> Result<NodeAssignmentAdmissionRequest, Box<dyn Error>> {
    let workspace = Uuid::from_u128(1);
    let activity = Uuid::from_u128(2);
    let coordinator_session = Uuid::from_u128(3);
    let coordinator_agent = Uuid::from_u128(4);
    let assignment = Uuid::from_u128(5);
    let target_agent = Uuid::from_u128(6);
    let target_session = Uuid::from_u128(7);
    let goal = Uuid::from_u128(8);
    let node = Uuid::from_u128(9);
    let location = Uuid::from_u128(10);
    let token = Uuid::from_u128(11);
    let payload = serde_json::to_vec(&json!({
        "scope": {
            "workspace_id": workspace,
            "workspace_revision": 1,
            "locator": {
                "location_id": location,
                "expected_revision": 1,
                "relative_root": "."
            }
        },
        "activity_id": activity,
        "assignment_id": assignment,
        "observed_at_ms": 20,
        "action": {
            "kind": "offer",
            "coordinator": {
                "session_id": coordinator_session,
                "agent_id": coordinator_agent,
                "expected_revision": 1,
                "principal": {"kind": "direct_token", "token_id": token}
            },
            "target": {
                "agent_id": target_agent,
                "session_id": target_session,
                "expected_revision": 1,
                "principal": {"kind": "direct_token", "token_id": Uuid::from_u128(12)}
            },
            "goal_id": goal,
            "expected_goal_revision": 1,
            "expected_goal_created_at_ms": 12,
            "dag_revision": 1,
            "expected_head_revision": 1,
            "node_id": node,
            "expected_node_revision": 1,
            "credit_budget": null
        }
    }))?;
    let digest: [u8; 32] = Sha256::digest(&payload).into();
    Ok(NodeAssignmentAdmissionRequest {
        scope: WorkspaceQueryScope {
            workspace_id: workspace,
            location_id: location,
            location_revision: 1,
            workspace_revision: 1,
            locator: "hf1:test:1:Lg".to_owned(),
        },
        activity_id: activity,
        coordinator_session_id: coordinator_session,
        coordinator_agent_id: coordinator_agent,
        coordinator_revision: 1,
        assignment_id: assignment,
        source_fences: NodeAssignmentSourceFencesWire {
            goal_id: goal,
            goal_created_at_ms: 12,
            goal_revision: 1,
            dag_revision: 1,
            dag_base_goal_revision: 1,
            dag_head_revision: 1,
            node_id: node,
            node_revision: 1,
        },
        target: NodeAssignmentTargetWire {
            agent_id: target_agent,
            session_id: target_session,
            agent_revision: 1,
        },
        team_id: None,
        expected_binding_revision: None,
        source_offer_bytes_base64: URL_SAFE_NO_PAD.encode(payload),
        source_offer_sha256: digest_hex(&digest),
    })
}

fn digest_hex(digest: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    digest
        .iter()
        .fold(String::with_capacity(64), |mut value, byte| {
            let _ = write!(value, "{byte:02x}");
            value
        })
}

#[test]
fn strict_request_requires_nullable_keys_and_rejects_unknown_fields() -> Result<(), Box<dyn Error>>
{
    let request = request()?;
    let mut value = serde_json::to_value(&request)?;
    value
        .as_object_mut()
        .ok_or("request object")?
        .remove("teamId");
    assert!(serde_json::from_value::<NodeAssignmentAdmissionRequest>(value).is_err());

    let mut value = serde_json::to_value(&request)?;
    value
        .as_object_mut()
        .ok_or("request object")?
        .insert("unexpected".to_owned(), Value::Bool(true));
    assert!(serde_json::from_value::<NodeAssignmentAdmissionRequest>(value).is_err());
    Ok(())
}

#[test]
fn native_requester_requires_null_profile_participation_pair() -> Result<(), Box<dyn Error>> {
    let requester_value = json!({
        "activityId": Uuid::from_u128(20),
        "agentId": Uuid::from_u128(21),
        "sessionId": Uuid::from_u128(22),
        "participationId": null,
        "profileId": null,
        "participantId": Uuid::from_u128(23)
    });
    let requester: NodeAssignmentRequesterWire = serde_json::from_value(requester_value.clone())?;
    let encoded = serde_json::to_value(requester)?;
    assert!(encoded["participationId"].is_null());
    assert!(encoded["profileId"].is_null());

    let mut missing_participation = requester_value.clone();
    missing_participation
        .as_object_mut()
        .ok_or("requester object")?
        .remove("participationId");
    assert!(serde_json::from_value::<NodeAssignmentRequesterWire>(missing_participation).is_err());

    let mut missing_profile = requester_value;
    missing_profile
        .as_object_mut()
        .ok_or("requester object")?
        .remove("profileId");
    assert!(serde_json::from_value::<NodeAssignmentRequesterWire>(missing_profile).is_err());
    Ok(())
}

#[tokio::test]
async fn uncertain_admit_retains_the_exact_request_and_recovery_does_not()
-> Result<(), Box<dyn Error>> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address: SocketAddr = listener.local_addr()?;
    drop(listener);
    let client = OrchestrationHttpClient::new(address, Duration::from_millis(100))?;
    let authorization = HeaderValue::from_static("Bearer test-credential");
    let request = request()?;

    assert_eq!(
        client.admit_node_assignment(&authorization, &request).await,
        Err(ClientError::UnknownOutcome {
            readback: ReadbackTarget::NodeAssignmentAdmission(Box::new(request.clone()))
        })
    );
    assert_eq!(
        client
            .recover_node_assignment_admission(&authorization, &request)
            .await,
        Err(ClientError::Unavailable)
    );
    Ok(())
}

#[tokio::test]
async fn noncanonical_offer_encoding_rejects_before_transport() -> Result<(), Box<dyn Error>> {
    let client = OrchestrationHttpClient::new("127.0.0.1:9".parse()?, Duration::from_millis(100))?;
    let authorization = HeaderValue::from_static("Bearer test-credential");
    let mut request = request()?;
    request.source_offer_bytes_base64.push('=');
    assert_eq!(
        client.admit_node_assignment(&authorization, &request).await,
        Err(ClientError::InvalidRequest)
    );
    Ok(())
}
