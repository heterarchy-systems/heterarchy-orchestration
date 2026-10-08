use super::*;
use crate::client::tests::{fixture, reply};
use crate::wire::WorkspaceQueryScope;
use serde_json::{Value, json};
use uuid::Uuid;

type R = Result<(), Box<dyn std::error::Error>>;
fn request() -> CoordinatorGetRequest {
    CoordinatorGetRequest {
        scope: WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 1,
            workspace_revision: 2,
            locator: "known-locator".to_owned(),
        },
        session_id: None,
        activity_id: Uuid::from_u128(3),
    }
}
fn response(request: &CoordinatorGetRequest) -> Value {
    json!({"scope":request.scope,"activityId":request.activity_id,"coordinator":{"workspaceId":request.scope.workspace_id,"activityId":request.activity_id,"agentId":Uuid::from_u128(4),"revision":5,"updatedAtMs":6}})
}
#[tokio::test]
async fn exact_binding_echo_and_original_credential_are_preserved() -> R {
    let request = request();
    let (client, server) = fixture(Some(reply("200 OK", &response(&request)))).await?;
    let result = client
        .coordinator_get(
            &HeaderValue::from_static("Bearer test-exact-credential"),
            &request,
        )
        .await?;
    assert_eq!(
        result.coordinator.ok_or("missing binding")?.updated_at_ms,
        6
    );
    let captured = String::from_utf8(server.await??)?;
    assert!(captured.starts_with(&format!(
        "POST /internal/v1/workspaces/{}/forge/coordinator-get ",
        request.scope.workspace_id
    )));
    assert!(
        captured
            .to_ascii_lowercase()
            .contains("authorization: bearer test-exact-credential")
    );
    assert_eq!(captured.matches("POST ").count(), 1);
    Ok(())
}
#[tokio::test]
async fn absence_is_required_null_and_foreign_binding_is_rejected() -> R {
    let request = request();
    let mut null = response(&request);
    null["coordinator"] = Value::Null;
    let (client, server) = fixture(Some(reply("200 OK", &null))).await?;
    assert!(
        client
            .coordinator_get(&HeaderValue::from_static("Bearer fixture"), &request)
            .await?
            .coordinator
            .is_none()
    );
    server.await??;
    let mut omitted = null.clone();
    omitted
        .as_object_mut()
        .ok_or("not object")?
        .remove("coordinator");
    let mut foreign = response(&request);
    foreign["coordinator"]["workspaceId"] = json!(Uuid::from_u128(99));
    let mut stale_echo = response(&request);
    stale_echo["scope"]["workspaceRevision"] = json!(1);
    let mut bad_revision = response(&request);
    bad_revision["coordinator"]["revision"] = json!(0);
    for invalid in [omitted, foreign, stale_echo, bad_revision] {
        let (client, server) = fixture(Some(reply("200 OK", &invalid))).await?;
        assert!(matches!(
            client
                .coordinator_get(&HeaderValue::from_static("Bearer fixture"), &request)
                .await,
            Err(ClientError::Protocol)
        ));
        server.await??;
    }
    Ok(())
}
#[test]
fn session_selector_is_explicit_nullable_and_extra_authority_is_not_a_contract() -> R {
    let request = request();
    let mut value = serde_json::to_value(&request)?;
    value
        .as_object_mut()
        .ok_or("not object")?
        .remove("sessionId");
    assert!(serde_json::from_value::<CoordinatorGetRequest>(value).is_err());
    let mut value = response(&request);
    value["retryAuthorized"] = json!(true);
    assert!(serde_json::from_value::<CoordinatorGetResponse>(value).is_err());
    Ok(())
}

#[tokio::test]
async fn lost_read_is_unavailable_and_does_not_retry_or_grant_mutation_readback() -> R {
    let (client, server) = fixture(None).await?;
    assert!(matches!(
        client
            .coordinator_get(&HeaderValue::from_static("Bearer fixture"), &request())
            .await,
        Err(ClientError::Unavailable)
    ));
    let captured = String::from_utf8(server.await??)?;
    assert_eq!(captured.matches("POST ").count(), 1);
    Ok(())
}
