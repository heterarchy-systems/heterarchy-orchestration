use std::time::Duration;
#[cfg(unix)]
use std::{ffi::OsString, os::unix::ffi::OsStringExt, path::PathBuf};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use super::*;

#[test]
fn detailed_workspace_failure_retains_the_existing_source_class() {
    for error in [
        ForgeHttpClientError::Timeout,
        ForgeHttpClientError::Transport,
        ForgeHttpClientError::Capacity,
        ForgeHttpClientError::Protocol,
        ForgeHttpClientError::Service {
            status: 409,
            code: ForgeHttpServiceCode::StaleRevision,
        },
        ForgeHttpClientError::Service {
            status: 403,
            code: ForgeHttpServiceCode::WorkspaceConfinementDenied,
        },
    ] {
        assert_eq!(
            WorkspaceObservationError::Command(error).source_class(),
            map_command_error(error)
        );
    }
    assert_eq!(
        WorkspaceObservationError::Scope(ForgeSourceError::Stale).source_class(),
        ForgeSourceError::Stale
    );
}

const SERVICE_SECRET: &str = "forge-source-fixture-secret";

#[tokio::test]
async fn workspace_resolution_sends_the_exact_locator_and_checks_canonical_readback()
-> Result<(), Box<dyn std::error::Error>> {
    let scope = scope()?;
    let expected_workspace = scope.workspace_id().into_uuid();
    let result = json!({
        "workspace_id": expected_workspace,
        "workspace_revision": scope.workspace_revision().get(),
    });
    let (client, server) = fixture(FakeReply::Success(result)).await?;

    client.resolve_workspace(&scope).await?;

    let request = server.await??;
    assert_eq!(request.path, "/api/v1/commands/workspace.resolve");
    assert_eq!(request.body["op"], "workspace.resolve");
    assert_eq!(
        request.body["payload"],
        json!({
            "location_id": scope.locator().location_id().into_uuid(),
            "expected_revision": scope.locator().expected_revision().get(),
            "relative_root": ".",
        })
    );
    Ok(())
}

#[tokio::test]
async fn workspace_resolution_separates_stale_identity_from_malformed_readback()
-> Result<(), Box<dyn std::error::Error>> {
    let scope = scope()?;
    let other_workspace = Uuid::from_u128(0x999);
    assert_ne!(other_workspace, scope.workspace_id().into_uuid());
    let stale = json!({
        "workspace_id": other_workspace,
        "workspace_revision": scope.workspace_revision().get(),
    });
    let (client, server) = fixture(FakeReply::Success(stale)).await?;
    assert_eq!(
        client.resolve_workspace(&scope).await,
        Err(ForgeSourceError::Stale)
    );
    let _request = server.await??;

    let malformed = json!({
        "workspace_id": "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
        "workspace_revision": scope.workspace_revision().get(),
    });
    let (client, server) = fixture(FakeReply::Success(malformed)).await?;
    assert_eq!(
        client.resolve_workspace(&scope).await,
        Err(ForgeSourceError::Protocol)
    );
    let _request = server.await??;
    Ok(())
}

#[tokio::test]
async fn activity_query_returns_open_closed_and_missing_source_observations()
-> Result<(), Box<dyn std::error::Error>> {
    let scope = scope()?;
    let activity_id = Uuid::from_u128(0x302);
    for (state, expected_state) in [
        ("open", ActivityState::Open),
        ("closed", ActivityState::Closed),
        ("interrupted", ActivityState::Interrupted),
    ] {
        let row = activity_row(
            &scope,
            activity_id,
            state,
            Some(Uuid::from_u128(0x303)),
            Some(0),
        );
        let result = activity_readback(&scope, activity_id, Some(&row), true)?;
        let (client, server) = fixture(FakeReply::Success(result)).await?;

        let observation = client
            .activity(&scope, activity_id)
            .await?
            .ok_or("expected an Activity observation")?;

        assert_eq!(observation.activity_id().into_uuid(), activity_id);
        assert_eq!(
            observation.workspace_id().into_uuid(),
            scope.workspace_id().into_uuid()
        );
        assert_eq!(observation.state(), expected_state);
        let request = server.await??;
        assert_eq!(request.path, "/api/v1/commands/activity.get");
        assert_eq!(request.body["op"], "activity.get");
        assert_eq!(request.body["payload"]["activity_id"], json!(activity_id));
        assert_eq!(request.body["payload"]["scope"], scope_wire(&scope)?);
    }

    let result = activity_readback(&scope, activity_id, None, false)?;
    let (client, server) = fixture(FakeReply::Success(result)).await?;
    assert_eq!(client.activity(&scope, activity_id).await?, None);
    let _request = server.await??;
    Ok(())
}

#[tokio::test]
async fn activity_query_rejects_scope_identity_flag_and_required_nullable_mismatches()
-> Result<(), Box<dyn std::error::Error>> {
    let scope = scope()?;
    let activity_id = Uuid::from_u128(0x304);
    let base_row = activity_row(&scope, activity_id, "open", None, None);
    let base = activity_readback(&scope, activity_id, Some(&base_row), true)?;

    let mut bad_scope = base.clone();
    bad_scope["scope"]["workspace_revision"] = json!(scope.workspace_revision().get() + 1);

    let mut bad_top_level_activity = base.clone();
    bad_top_level_activity["activity_id"] = json!(Uuid::from_u128(0x305));

    let mut bad_row_activity = base.clone();
    bad_row_activity["activity"]["activity_id"] = json!(Uuid::from_u128(0x305));

    let mut bad_row_workspace = base.clone();
    bad_row_workspace["activity"]["workspace_id"] = json!(Uuid::from_u128(0x306));

    let mut retry_granted = base.clone();
    retry_granted["retry_authorized"] = json!(true);

    let mut equivalence_claimed = base.clone();
    equivalence_claimed["request_equivalence_proven"] = json!(true);

    let mut missing_owner_field = base.clone();
    missing_owner_field["activity"]
        .as_object_mut()
        .ok_or("activity result was not an object")?
        .remove("lease_owner_id");

    let mut missing_activity_field = base.clone();
    missing_activity_field["found"] = json!(false);
    missing_activity_field
        .as_object_mut()
        .ok_or("activity readback was not an object")?
        .remove("activity");

    let mut missing_both_lease_fields = base.clone();
    let row = missing_both_lease_fields["activity"]
        .as_object_mut()
        .ok_or("activity result was not an object")?;
    row.remove("lease_owner_id");
    row.remove("lease_until_ms");

    let mut partial_lease_pair = base.clone();
    partial_lease_pair["activity"]["lease_until_ms"] = json!(0);

    let mut found_disagrees_with_null = base.clone();
    found_disagrees_with_null["activity"] = Value::Null;

    for malformed in [
        bad_scope,
        bad_top_level_activity,
        bad_row_activity,
        bad_row_workspace,
        retry_granted,
        equivalence_claimed,
        missing_owner_field,
        missing_activity_field,
        missing_both_lease_fields,
        partial_lease_pair,
        found_disagrees_with_null,
    ] {
        let (client, server) = fixture(FakeReply::Success(malformed)).await?;
        assert_eq!(
            client.activity(&scope, activity_id).await,
            Err(ForgeSourceError::Protocol)
        );
        let _request = server.await??;
    }
    Ok(())
}

#[tokio::test]
async fn stale_service_errors_are_stale_and_peer_details_stay_redacted()
-> Result<(), Box<dyn std::error::Error>> {
    let scope = scope()?;
    let (client, server) = fixture(FakeReply::ServiceError {
        status: 409,
        code: "STALE_REVISION",
    })
    .await?;
    let error = client
        .resolve_workspace(&scope)
        .await
        .err()
        .ok_or("the fake peer returns a stale source error")?;
    assert_eq!(error, ForgeSourceError::Stale);
    assert!(!error.to_string().contains("peer-only detail"));
    let _request = server.await??;

    assert_eq!(
        map_command_error(ForgeHttpClientError::Authentication),
        ForgeSourceError::Unavailable
    );
    assert_eq!(
        map_command_error(ForgeHttpClientError::Service {
            status: 202,
            code: ForgeHttpServiceCode::UnknownOutcome,
        }),
        ForgeSourceError::Unavailable
    );
    assert_eq!(
        map_command_error(ForgeHttpClientError::Service {
            status: 409,
            code: ForgeHttpServiceCode::SourceConflict,
        }),
        ForgeSourceError::Unavailable
    );
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn non_utf8_locator_is_rejected_before_the_http_exchange()
-> Result<(), Box<dyn std::error::Error>> {
    let original = scope()?;
    let locator = forge_workspace::WorkspaceLocator::new(
        original.locator().location_id(),
        original.locator().expected_revision(),
        PathBuf::from(OsString::from_vec(vec![0xff])),
    )?;
    let invalid_scope = ResolvedWorkspaceScope::from_observation(
        original.workspace_id(),
        original.workspace_revision(),
        locator,
    );
    let commands = ForgeCommandHttpClient::new(
        "127.0.0.1:1".parse()?,
        SERVICE_SECRET,
        Duration::from_secs(1),
    )?;
    let client = ForgeSourceHttpClient::new(commands);

    assert_eq!(
        client.resolve_workspace(&invalid_scope).await,
        Err(ForgeSourceError::InvalidScope)
    );
    assert_eq!(
        client
            .activity(&invalid_scope, Uuid::from_u128(0x307))
            .await,
        Err(ForgeSourceError::InvalidScope)
    );
    Ok(())
}

#[tokio::test]
async fn activity_query_rejects_invalid_row_revisions_timestamps_and_lease_owner()
-> Result<(), Box<dyn std::error::Error>> {
    let scope = scope()?;
    let activity_id = Uuid::from_u128(0x308);
    let base_row = activity_row(&scope, activity_id, "open", None, None);
    for (field, invalid) in [
        ("revision", json!(0)),
        ("lease_revision", json!(0)),
        ("created_at_ms", json!(-1)),
        ("updated_at_ms", json!(-1)),
        ("lease_owner_id", json!(Uuid::nil())),
        ("lease_until_ms", json!(-1)),
    ] {
        let mut row = base_row.clone();
        row[field] = invalid;
        if field == "lease_until_ms" {
            row["lease_owner_id"] = json!(Uuid::from_u128(0x309));
        }
        if field == "lease_owner_id" {
            row["lease_until_ms"] = json!(0);
        }
        let result = activity_readback(&scope, activity_id, Some(&row), true)?;
        let (client, server) = fixture(FakeReply::Success(result)).await?;
        assert_eq!(
            client.activity(&scope, activity_id).await,
            Err(ForgeSourceError::Protocol)
        );
        let _request = server.await??;
    }
    Ok(())
}

fn scope() -> Result<ResolvedWorkspaceScope, Box<dyn std::error::Error>> {
    let workspace_id = forge_domain::WorkspaceId::from_uuid(Uuid::from_u128(0x301));
    let location_id = forge_domain::LocationId::from_uuid(Uuid::from_u128(0x302));
    let workspace_revision = forge_domain::WorkspaceRevision::new(8)?;
    let location_revision = forge_domain::LocationRevision::new(5)?;
    let locator = forge_workspace::WorkspaceLocator::new(location_id, location_revision, ".")?;
    Ok(ResolvedWorkspaceScope::from_observation(
        workspace_id,
        workspace_revision,
        locator,
    ))
}

fn scope_wire(scope: &ResolvedWorkspaceScope) -> Result<Value, ForgeSourceError> {
    let (scope, _) = validated_scope(scope)?;
    serde_json::to_value(scope).map_err(|_| ForgeSourceError::Protocol)
}

fn activity_row(
    scope: &ResolvedWorkspaceScope,
    activity_id: Uuid,
    state: &str,
    lease_owner_id: Option<Uuid>,
    lease_until_ms: Option<i64>,
) -> Value {
    json!({
        "activity_id": activity_id,
        "workspace_id": scope.workspace_id().into_uuid(),
        "state": state,
        "revision": 2,
        "created_at_ms": 0,
        "updated_at_ms": 1,
        "lease_owner_id": lease_owner_id,
        "lease_until_ms": lease_until_ms,
        "lease_revision": 1,
    })
}

fn activity_readback(
    scope: &ResolvedWorkspaceScope,
    activity_id: Uuid,
    activity: Option<&Value>,
    found: bool,
) -> Result<Value, ForgeSourceError> {
    Ok(json!({
        "scope": scope_wire(scope)?,
        "activity_id": activity_id,
        "found": found,
        "activity": activity,
        "retry_authorized": false,
        "request_equivalence_proven": false,
    }))
}

enum FakeReply {
    Success(Value),
    ServiceError { status: u16, code: &'static str },
}

struct CapturedRequest {
    path: String,
    body: Value,
}

async fn fixture(
    reply: FakeReply,
) -> Result<
    (
        ForgeSourceHttpClient,
        tokio::task::JoinHandle<Result<CapturedRequest, std::io::Error>>,
    ),
    Box<dyn std::error::Error>,
> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let commands = ForgeCommandHttpClient::new(
        listener.local_addr()?,
        SERVICE_SECRET,
        Duration::from_secs(1),
    )?;
    let client = ForgeSourceHttpClient::new(commands);
    let server = tokio::spawn(async move { serve_one(listener, reply).await });
    Ok((client, server))
}

async fn serve_one(
    listener: tokio::net::TcpListener,
    reply: FakeReply,
) -> Result<CapturedRequest, std::io::Error> {
    let (mut socket, _) = listener.accept().await?;
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 1024];
    let (header_end, body_start, body_length) = loop {
        let count = socket.read(&mut chunk).await?;
        if count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Forge command request ended before its body",
            ));
        }
        bytes.extend_from_slice(&chunk[..count]);
        if bytes.len() > 72 * 1024 {
            return Err(std::io::Error::other("fixture request exceeded its bound"));
        }
        let Some(header_end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") else {
            continue;
        };
        let header_text = String::from_utf8_lossy(&bytes[..header_end]);
        let content_length = header_text
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "missing content length")
            })?;
        let body_start = header_end + 4;
        if bytes.len() >= body_start + content_length {
            break (header_end, body_start, content_length);
        }
    };

    let request_line = String::from_utf8_lossy(&bytes[..header_end]);
    let path = request_line
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid request"))?
        .to_owned();
    let body = serde_json::from_slice::<Value>(&bytes[body_start..body_start + body_length])
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let (status, response) = match reply {
        FakeReply::Success(result) => (
            200,
            json!({
                "forge_api": body["forge_api"],
                "request_id": body["request_id"],
                "ok": true,
                "result": result,
            }),
        ),
        FakeReply::ServiceError { status, code } => (
            status,
            json!({
                "forge_api": body["forge_api"],
                "request_id": body["request_id"],
                "ok": false,
                "error": {
                    "code": code,
                    "message": "peer-only detail",
                    "category": "scope",
                    "retryable": false,
                    "recovery_action": "correct_input",
                    "recovery_hint": "",
                    "readback": null,
                },
            }),
        ),
    };
    let reason = match status {
        200 => "OK",
        409 => "Conflict",
        401 => "Unauthorized",
        _ => "Internal Server Error",
    };
    let response_body = response.to_string();
    let response_text = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
        response_body.len(),
    );
    socket.write_all(response_text.as_bytes()).await?;
    socket.shutdown().await?;
    Ok(CapturedRequest { path, body })
}
