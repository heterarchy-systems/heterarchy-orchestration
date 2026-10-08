//! Read-only Activity Coordinator observations from the configured Forge source.

use forge_backend::ops::CommandOp;
use forge_control::ResolvedWorkspaceScope;
use forge_domain::{
    ActivityCoordinatorActor, ActivityCoordinatorBinding, ActivityCoordinatorRevision, ActivityId,
    AgentInstanceId, AgentSessionId, AuthenticatedPrincipal, DirectTokenId, OAuthGrantId,
    TimestampMillis, WorkspaceId,
};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use super::{
    ForgeSourceError, ForgeSourceHttpClient, canonical_uuid, decode_result, map_command_error,
    validated_scope,
};

impl ForgeSourceHttpClient {
    /// Read the exact Coordinator binding for one Workspace and Activity.
    ///
    /// A found result is an opaque identity and revision observation. It does
    /// not contain a caller principal or session and cannot construct an actor.
    ///
    /// # Errors
    /// Rejects an invalid Activity selector or malformed/foreign source readback.
    /// Forge transport and service errors retain their typed source classes.
    pub async fn coordinator(
        &self,
        expected: &ResolvedWorkspaceScope,
        activity_id: Uuid,
    ) -> Result<Option<ActivityCoordinatorBinding>, ForgeSourceError> {
        if activity_id.is_nil() {
            return Err(ForgeSourceError::InvalidScope);
        }
        let payload = coordinator_payload(expected, activity_id)?;
        let result = self
            .commands
            .command(CommandOp::ActivityCoordinatorGet, payload)
            .await
            .map_err(map_command_error)?;
        parse_coordinator_readback(result, expected.workspace_id().into_uuid(), activity_id)
    }

    /// Ask Forge to resolve its current Coordinator into an owner-validated,
    /// principal-bound actor. The HTTP client never reconstructs authority from
    /// the binding-only observation.
    ///
    /// # Errors
    /// Rejects invalid scope, unavailable source transport, or malformed and
    /// foreign actor readback.
    pub async fn coordinator_actor(
        &self,
        expected: &ResolvedWorkspaceScope,
        activity_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<ActivityCoordinatorActor>, ForgeSourceError> {
        if activity_id.is_nil() {
            return Err(ForgeSourceError::InvalidScope);
        }
        let (scope, _) = validated_scope(expected)?;
        let result = self
            .commands
            .command(
                CommandOp::ActivityCoordinatorActorGet,
                json!({
                    "scope": scope,
                    "activity_id": activity_id,
                    "principal": principal_value(principal),
                }),
            )
            .await
            .map_err(map_command_error)?;
        parse_coordinator_actor_readback(
            result,
            expected.workspace_id().into_uuid(),
            activity_id,
            principal,
        )
    }
}

fn principal_value(principal: AuthenticatedPrincipal) -> Value {
    match principal {
        AuthenticatedPrincipal::TrustedStdio => json!({ "kind": "trusted_stdio" }),
        AuthenticatedPrincipal::DirectToken(id) => {
            json!({ "kind": "direct_token", "token_id": id.into_uuid() })
        }
        AuthenticatedPrincipal::OAuthGrant(id) => {
            json!({ "kind": "oauth_grant", "grant_id": id.into_uuid() })
        }
    }
}

fn coordinator_payload(
    expected: &ResolvedWorkspaceScope,
    activity_id: Uuid,
) -> Result<Value, ForgeSourceError> {
    if activity_id.is_nil() {
        return Err(ForgeSourceError::InvalidScope);
    }
    let (scope, _) = validated_scope(expected)?;
    Ok(json!({
        "scope": scope,
        "activity_id": activity_id,
    }))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CoordinatorNotFoundReadback {
    found: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CoordinatorFoundReadback {
    found: bool,
    coordinator: CoordinatorBindingReadback,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CoordinatorBindingReadback {
    workspace_id: String,
    activity_id: String,
    agent_id: String,
    revision: i64,
    updated_at_ms: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CoordinatorActorFoundReadback {
    found: bool,
    actor: CoordinatorActorReadback,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CoordinatorActorReadback {
    workspace_id: String,
    activity_id: String,
    agent_id: String,
    expected_coordinator_revision: i64,
    principal: PrincipalReadback,
    session_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrincipalReadback {
    kind: String,
    #[serde(default)]
    token_id: Option<String>,
    #[serde(default)]
    grant_id: Option<String>,
}

fn parse_coordinator_actor_readback(
    value: Value,
    expected_workspace_id: Uuid,
    expected_activity_id: Uuid,
    expected_principal: AuthenticatedPrincipal,
) -> Result<Option<ActivityCoordinatorActor>, ForgeSourceError> {
    let found = value
        .get("found")
        .and_then(Value::as_bool)
        .ok_or(ForgeSourceError::Protocol)?;
    if !found {
        let readback: CoordinatorNotFoundReadback = decode_result(value)?;
        return if readback.found {
            Err(ForgeSourceError::Protocol)
        } else {
            Ok(None)
        };
    }
    let readback: CoordinatorActorFoundReadback = decode_result(value)?;
    if !readback.found {
        return Err(ForgeSourceError::Protocol);
    }
    let actor = readback.actor;
    let workspace_id = canonical_uuid(&actor.workspace_id)?;
    let activity_id = canonical_uuid(&actor.activity_id)?;
    let agent_id = canonical_uuid(&actor.agent_id)?;
    let session_id = canonical_uuid(&actor.session_id)?;
    let principal = decode_principal(actor.principal)?;
    if workspace_id != expected_workspace_id
        || activity_id != expected_activity_id
        || principal != expected_principal
    {
        return Err(ForgeSourceError::Protocol);
    }
    let revision = ActivityCoordinatorRevision::new(actor.expected_coordinator_revision)
        .map_err(|_| ForgeSourceError::Protocol)?;
    Ok(Some(ActivityCoordinatorActor::new(
        WorkspaceId::from_uuid(workspace_id),
        ActivityId::from_uuid(activity_id),
        AgentInstanceId::from_uuid(agent_id),
        revision,
        principal,
        AgentSessionId::from_uuid(session_id),
    )))
}

fn decode_principal(value: PrincipalReadback) -> Result<AuthenticatedPrincipal, ForgeSourceError> {
    match (value.kind.as_str(), value.token_id, value.grant_id) {
        ("trusted_stdio", None, None) => Ok(AuthenticatedPrincipal::TrustedStdio),
        ("direct_token", Some(id), None) => Ok(AuthenticatedPrincipal::DirectToken(
            DirectTokenId::from_uuid(canonical_uuid(&id)?),
        )),
        ("oauth_grant", None, Some(id)) => Ok(AuthenticatedPrincipal::OAuthGrant(
            OAuthGrantId::from_uuid(canonical_uuid(&id)?),
        )),
        _ => Err(ForgeSourceError::Protocol),
    }
}

fn parse_coordinator_readback(
    value: Value,
    expected_workspace_id: Uuid,
    expected_activity_id: Uuid,
) -> Result<Option<ActivityCoordinatorBinding>, ForgeSourceError> {
    let found = value
        .get("found")
        .and_then(Value::as_bool)
        .ok_or(ForgeSourceError::Protocol)?;
    if !found {
        let readback: CoordinatorNotFoundReadback = decode_result(value)?;
        if readback.found {
            return Err(ForgeSourceError::Protocol);
        }
        return Ok(None);
    }

    let readback: CoordinatorFoundReadback = decode_result(value)?;
    if !readback.found {
        return Err(ForgeSourceError::Protocol);
    }
    let coordinator = readback.coordinator;
    let workspace_id = canonical_uuid(&coordinator.workspace_id)?;
    let activity_id = canonical_uuid(&coordinator.activity_id)?;
    let agent_id = canonical_uuid(&coordinator.agent_id)?;
    if workspace_id != expected_workspace_id || activity_id != expected_activity_id {
        return Err(ForgeSourceError::Protocol);
    }
    if coordinator.updated_at_ms < 0 {
        return Err(ForgeSourceError::Protocol);
    }
    let revision = ActivityCoordinatorRevision::new(coordinator.revision)
        .map_err(|_| ForgeSourceError::Protocol)?;
    let updated_at =
        TimestampMillis::new(coordinator.updated_at_ms).map_err(|_| ForgeSourceError::Protocol)?;
    Ok(Some(ActivityCoordinatorBinding::new(
        WorkspaceId::from_uuid(workspace_id),
        ActivityId::from_uuid(activity_id),
        AgentInstanceId::from_uuid(agent_id),
        revision,
        updated_at,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    use forge_domain::{LocationId, LocationRevision, WorkspaceRevision};
    use forge_workspace::WorkspaceLocator;

    fn scope() -> Result<ResolvedWorkspaceScope, Box<dyn Error>> {
        let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(0x301));
        let locator = WorkspaceLocator::new(
            LocationId::from_uuid(Uuid::from_u128(0x302)),
            LocationRevision::new(5)?,
            ".",
        )?;
        Ok(ResolvedWorkspaceScope::from_observation(
            workspace_id,
            WorkspaceRevision::new(8)?,
            locator,
        ))
    }

    fn coordinator_readback(scope: &ResolvedWorkspaceScope, activity_id: Uuid) -> Value {
        json!({
            "found": true,
            "coordinator": {
                "workspace_id": scope.workspace_id().into_uuid().to_string(),
                "activity_id": activity_id.to_string(),
                "agent_id": Uuid::from_u128(0x304).to_string(),
                "revision": 7,
                "updated_at_ms": 11,
            },
        })
    }

    #[test]
    fn coordinator_payload_matches_the_native_forge_parser() -> Result<(), Box<dyn Error>> {
        let scope = scope()?;
        let activity_id = Uuid::from_u128(0x303);
        let payload = coordinator_payload(&scope, activity_id)?;
        let (scope_dto, _) = validated_scope(&scope)?;
        assert_eq!(
            payload,
            json!({
                "scope": scope_dto,
                "activity_id": activity_id,
            })
        );
        assert!(
            forge_backend::ops::parse_command(
                CommandOp::ActivityCoordinatorGet,
                Uuid::from_u128(0x305),
                &payload,
            )
            .is_ok()
        );
        Ok(())
    }

    #[test]
    fn coordinator_actor_payload_and_readback_preserve_full_authority() -> Result<(), Box<dyn Error>>
    {
        let scope = scope()?;
        let activity_id = Uuid::from_u128(0x303);
        let principal =
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(0x306)));
        let (scope_dto, _) = validated_scope(&scope)?;
        let payload = json!({
            "scope": scope_dto,
            "activity_id": activity_id,
            "principal": principal_value(principal),
        });
        assert!(
            forge_backend::ops::parse_command(
                CommandOp::ActivityCoordinatorActorGet,
                Uuid::from_u128(0x307),
                &payload,
            )
            .is_ok()
        );
        let actor = parse_coordinator_actor_readback(
            json!({
                "found": true,
                "actor": {
                    "workspace_id": scope.workspace_id().into_uuid().to_string(),
                    "activity_id": activity_id.to_string(),
                    "agent_id": Uuid::from_u128(0x304).to_string(),
                    "expected_coordinator_revision": 7,
                    "principal": principal_value(principal),
                    "session_id": Uuid::from_u128(0x305).to_string(),
                }
            }),
            scope.workspace_id().into_uuid(),
            activity_id,
            principal,
        )?
        .ok_or("actor missing")?;
        assert_eq!(actor.principal(), principal);
        assert_eq!(
            actor.expected_revision(),
            ActivityCoordinatorRevision::new(7)?
        );
        assert_eq!(actor.session_id().into_uuid(), Uuid::from_u128(0x305));
        assert_eq!(
            parse_coordinator_actor_readback(
                json!({"found": false}),
                scope.workspace_id().into_uuid(),
                activity_id,
                principal,
            )?,
            None
        );
        Ok(())
    }

    #[test]
    fn coordinator_actor_readback_rejects_foreign_principal_and_identity()
    -> Result<(), Box<dyn Error>> {
        let scope = scope()?;
        let activity_id = Uuid::from_u128(0x303);
        let expected = AuthenticatedPrincipal::TrustedStdio;
        let base = json!({
            "found": true,
            "actor": {
                "workspace_id": scope.workspace_id().into_uuid().to_string(),
                "activity_id": activity_id.to_string(),
                "agent_id": Uuid::from_u128(0x304).to_string(),
                "expected_coordinator_revision": 7,
                "principal": principal_value(expected),
                "session_id": Uuid::from_u128(0x305).to_string(),
            }
        });
        let mut foreign_principal = base.clone();
        foreign_principal["actor"]["principal"] = principal_value(
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(9))),
        );
        let mut nil_session = base.clone();
        nil_session["actor"]["session_id"] = json!(Uuid::nil().to_string());
        let mut stale_revision = base;
        stale_revision["actor"]["expected_coordinator_revision"] = json!(0);
        for hostile in [foreign_principal, nil_session, stale_revision] {
            assert_eq!(
                parse_coordinator_actor_readback(
                    hostile,
                    scope.workspace_id().into_uuid(),
                    activity_id,
                    expected,
                ),
                Err(ForgeSourceError::Protocol)
            );
        }
        Ok(())
    }

    #[test]
    fn coordinator_readback_projects_data_only_and_accepts_exact_absence()
    -> Result<(), Box<dyn Error>> {
        let scope = scope()?;
        let activity_id = Uuid::from_u128(0x303);
        let binding = parse_coordinator_readback(
            coordinator_readback(&scope, activity_id),
            scope.workspace_id().into_uuid(),
            activity_id,
        )?
        .ok_or("expected a Coordinator binding")?;
        assert_eq!(binding.workspace_id(), scope.workspace_id());
        assert_eq!(binding.activity_id(), ActivityId::from_uuid(activity_id));
        assert_eq!(binding.agent_id().into_uuid(), Uuid::from_u128(0x304));
        assert_eq!(binding.revision(), ActivityCoordinatorRevision::new(7)?);
        assert_eq!(binding.updated_at(), TimestampMillis::new(11)?);

        assert_eq!(
            parse_coordinator_readback(
                json!({"found": false}),
                scope.workspace_id().into_uuid(),
                activity_id,
            )?,
            None
        );
        Ok(())
    }

    #[test]
    fn coordinator_readback_rejects_hostile_shapes_and_foreign_identity()
    -> Result<(), Box<dyn Error>> {
        let scope = scope()?;
        let activity_id = Uuid::from_u128(0x303);
        let base = coordinator_readback(&scope, activity_id);

        let absent_with_null = json!({"found": false, "coordinator": null});
        let absent_with_extra = json!({"found": false, "activity_id": activity_id});
        let found_without_coordinator = json!({"found": true});
        let found_with_null = json!({"found": true, "coordinator": null});
        let mut found_with_actor_fields = base.clone();
        found_with_actor_fields["coordinator"]["principal"] = json!("trusted_stdio");
        let mut foreign_workspace = base.clone();
        foreign_workspace["coordinator"]["workspace_id"] =
            json!(Uuid::from_u128(0x306).to_string());
        let mut foreign_activity = base.clone();
        foreign_activity["coordinator"]["activity_id"] = json!(Uuid::from_u128(0x307).to_string());
        let mut nil_agent = base.clone();
        nil_agent["coordinator"]["agent_id"] = json!(Uuid::nil().to_string());
        let mut noncanonical_agent = base.clone();
        noncanonical_agent["coordinator"]["agent_id"] =
            json!("00000000-0000-0000-0000-00000000030A");
        let mut wrong_revision = base.clone();
        wrong_revision["coordinator"]["revision"] = json!(0);
        let mut negative_updated_at = base.clone();
        negative_updated_at["coordinator"]["updated_at_ms"] = json!(-1);
        let mut unknown_binding_field = base;
        unknown_binding_field["coordinator"]["session_id"] =
            json!(Uuid::from_u128(0x308).to_string());

        for hostile in [
            absent_with_null,
            absent_with_extra,
            found_without_coordinator,
            found_with_null,
            found_with_actor_fields,
            foreign_workspace,
            foreign_activity,
            nil_agent,
            noncanonical_agent,
            wrong_revision,
            negative_updated_at,
            unknown_binding_field,
        ] {
            assert_eq!(
                parse_coordinator_readback(hostile, scope.workspace_id().into_uuid(), activity_id,),
                Err(ForgeSourceError::Protocol)
            );
        }
        Ok(())
    }
}
