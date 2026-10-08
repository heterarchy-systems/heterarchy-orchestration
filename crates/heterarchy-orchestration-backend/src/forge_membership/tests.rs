use std::error::Error;

use forge_domain::{
    ActivityId, AgentDeclaredRole, AgentDisplayName, AgentInstanceId, AgentSessionId,
    AgentTransportKind, AuthenticatedPrincipal, DirectTokenId, LocationId, LocationRevision,
    TimestampMillis, WorkspaceId, WorkspaceRevision,
};
use forge_workspace::WorkspaceLocator;
use heterarchy_orchestration_operations::{
    AgentBindingActivationRequest, AgentBindingReservationRequest,
};
use serde_json::{Value, json};
use uuid::Uuid;

use super::*;

fn scope() -> Result<ResolvedWorkspaceScope, Box<dyn Error>> {
    let locator = WorkspaceLocator::new(
        LocationId::from_uuid(Uuid::from_u128(0x302)),
        LocationRevision::new(5)?,
        ".",
    )?;
    Ok(ResolvedWorkspaceScope::from_observation(
        WorkspaceId::from_uuid(Uuid::from_u128(0x301)),
        WorkspaceRevision::new(8)?,
        locator,
    ))
}

fn reference() -> Result<MembershipSourceReference, ForgeSourceError> {
    MembershipSourceReference::new(
        Uuid::from_u128(0x303),
        Uuid::from_u128(0x304),
        Uuid::from_u128(0x305),
        7,
        [0xab; 32],
    )
}

fn request(requested_at_ms: i64) -> Result<AgentBindingReservationRequest, Box<dyn Error>> {
    Ok(AgentBindingReservationRequest::new(
        Uuid::from_u128(0x306),
        ActivityId::from_uuid(Uuid::from_u128(0x307)),
        AgentInstanceId::from_uuid(Uuid::from_u128(0x308)),
        AgentSessionId::from_uuid(Uuid::from_u128(0x309)),
        AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(0x30a))),
        TimestampMillis::new(requested_at_ms)?,
    )?)
}

fn base(
    scope: &ResolvedWorkspaceScope,
    reference: MembershipSourceReference,
    found: bool,
) -> Result<(ScopeDto, Value), Box<dyn Error>> {
    let (wire_scope, _) = forge_source::validated_scope(scope)?;
    let result = json!({
        "scope": &wire_scope,
        "destination_id": reference.destination_id,
        "found": found,
        "retry_authorized": false,
        "request_equivalence_proven": false,
    });
    Ok((wire_scope, result))
}

fn add_reservation(
    value: &mut Value,
    scope: &ResolvedWorkspaceScope,
    reference: MembershipSourceReference,
    request: &AgentBindingReservationRequest,
    reserved_at_ms: i64,
) {
    value["workspace_id"] = json!(scope.workspace_id().into_uuid());
    value["migration_id"] = json!(reference.migration_id);
    value["freeze_revision"] = json!(reference.freeze_revision);
    value["request_id"] = json!(request.request_id());
    value["activity_id"] = json!(request.activity_id().into_uuid());
    value["agent_id"] = json!(request.agent_id().into_uuid());
    value["session_id"] = json!(request.session_id().into_uuid());
    value["principal"] = principal_json(request.principal());
    value["requested_at_ms"] = json!(request.requested_at().get());
    value["reserved_at_ms"] = json!(reserved_at_ms);
}

#[test]
fn delegation_readback_rejects_scope_source_flags_and_missing_metadata()
-> Result<(), Box<dyn Error>> {
    let scope = scope()?;
    let reference = reference()?;
    let (wire_scope, mut valid) = base(&scope, reference, true)?;
    valid["workspace_id"] = json!(scope.workspace_id().into_uuid());
    valid["migration_id"] = json!(reference.migration_id);
    valid["freeze_destination_id"] = json!(reference.destination_id);
    valid["freeze_revision"] = json!(reference.freeze_revision);
    valid["freeze_state"] = json!("frozen");
    valid["freeze_created_at_ms"] = json!(10);
    valid["freeze_updated_at_ms"] = json!(11);
    valid["manifest_digest"] = json!(encode_digest(reference.manifest_digest));
    valid["delegated_at_ms"] = json!(12);

    let observation = parse_delegation_readback(valid.clone(), &wire_scope, reference)?
        .ok_or("expected a delegation observation")?;
    assert_eq!(
        observation.workspace_id().into_uuid(),
        scope.workspace_id().into_uuid()
    );
    assert_eq!(observation.freeze_revision(), reference.freeze_revision);
    assert_eq!(observation.freeze_created_at().get(), 10);
    assert_eq!(observation.delegated_at().get(), 12);

    let mut wrong_scope = valid.clone();
    wrong_scope["scope"]["workspace_revision"] = json!(9);
    let mut wrong_manifest = valid.clone();
    wrong_manifest["manifest_digest"] = json!(encode_digest([0xcd; 32]));
    let mut retry_claim = valid.clone();
    retry_claim["retry_authorized"] = json!(true);
    let mut missing_time = valid;
    missing_time
        .as_object_mut()
        .ok_or("delegation projection was not an object")?
        .remove("delegated_at_ms");

    for invalid in [wrong_scope, wrong_manifest, retry_claim, missing_time] {
        assert_eq!(
            parse_delegation_readback(invalid, &wire_scope, reference),
            Err(ForgeSourceError::Protocol)
        );
    }

    let (_, not_found) = base(&scope, reference, false)?;
    assert_eq!(
        parse_delegation_readback(not_found, &wire_scope, reference)?,
        None
    );
    Ok(())
}

#[test]
fn reservation_readback_checks_exact_request_principal_and_required_fields()
-> Result<(), Box<dyn Error>> {
    let scope = scope()?;
    let reference = reference()?;
    let request = request(100)?;
    let (wire_scope, mut valid) = base(&scope, reference, true)?;
    add_reservation(&mut valid, &scope, reference, &request, 20);

    let observation =
        parse_reservation_readback(valid.clone(), &wire_scope, reference, Some(&request))?
            .ok_or("expected a reservation observation")?;
    assert_eq!(observation.request(), &request);
    // Forge's reservation timestamp is ordered against its frozen source, not
    // against request metadata supplied by Operations.
    assert_eq!(observation.reserved_at().get(), 20);

    let mut wrong_request = valid.clone();
    wrong_request["agent_id"] = json!(Uuid::from_u128(0x30b));
    let mut wrong_principal = valid.clone();
    wrong_principal["principal"]["token_id"] = json!(Uuid::from_u128(0x30c));
    let mut missing_reserved_at = valid.clone();
    missing_reserved_at
        .as_object_mut()
        .ok_or("reservation projection was not an object")?
        .remove("reserved_at_ms");
    let mut equivalence_claim = valid;
    equivalence_claim["request_equivalence_proven"] = json!(true);

    for invalid in [
        wrong_request,
        wrong_principal,
        missing_reserved_at,
        equivalence_claim,
    ] {
        assert_eq!(
            parse_reservation_readback(invalid, &wire_scope, reference, Some(&request)),
            Err(ForgeSourceError::Protocol)
        );
    }
    Ok(())
}

#[test]
fn activation_readback_checks_reservation_principal_and_activation_payload()
-> Result<(), Box<dyn Error>> {
    let scope = scope()?;
    let reference = reference()?;
    let request = request(10)?;
    let activation = AgentBindingActivationRequest::new(
        request.request_id(),
        [0x55; 32],
        AgentDisplayName::new("Ada".to_owned())?,
        AgentDeclaredRole::new("reader".to_owned())?,
        AgentTransportKind::Retired,
    )?;
    let (wire_scope, mut valid) = base(&scope, reference, true)?;
    add_reservation(&mut valid, &scope, reference, &request, 20);
    valid["membership_digest"] = json!(encode_digest(activation.membership_digest()));
    valid["display_name"] = json!(activation.display_name().as_str());
    valid["declared_role"] = json!(activation.declared_role().as_str());
    valid["transport"] = json!("mcp");
    valid["activated_at_ms"] = json!(21);

    let projection = parse_activation_readback(valid.clone(), &wire_scope, reference)?
        .ok_or("expected an activation projection")?;
    validate_activation_mutation(
        &projection,
        &activation,
        request.session_id().into_uuid(),
        request.principal(),
    )?;
    validate_activation_query(&projection, &request)?;
    let observation = activation_observation(projection, reference, scope.workspace_id())?;
    assert_eq!(observation.request(), &request);
    assert_eq!(observation.activated_at().get(), 21);

    let mut wrong_principal = valid.clone();
    wrong_principal["principal"]["token_id"] = json!(Uuid::from_u128(0x30c));
    let wrong_activation = AgentBindingActivationRequest::new(
        activation.request_id(),
        [0x66; 32],
        activation.display_name().clone(),
        activation.declared_role().clone(),
        activation.transport(),
    )?;
    let mut missing_transport = valid;
    missing_transport
        .as_object_mut()
        .ok_or("activation projection was not an object")?
        .remove("transport");

    let wrong_principal = parse_activation_readback(wrong_principal, &wire_scope, reference)?
        .ok_or("expected an activation projection")?;
    assert_eq!(
        validate_activation_mutation(
            &wrong_principal,
            &activation,
            request.session_id().into_uuid(),
            request.principal(),
        ),
        Err(ForgeSourceError::Protocol)
    );
    let wrong_payload = parse_activation_readback(
        json!({
            "scope": wire_scope,
            "destination_id": reference.destination_id,
            "found": true,
            "retry_authorized": false,
            "request_equivalence_proven": false,
            "workspace_id": scope.workspace_id().into_uuid(),
            "migration_id": reference.migration_id,
            "freeze_revision": reference.freeze_revision,
            "request_id": request.request_id(),
            "activity_id": request.activity_id().into_uuid(),
            "agent_id": request.agent_id().into_uuid(),
            "session_id": request.session_id().into_uuid(),
            "principal": principal_json(request.principal()),
            "requested_at_ms": request.requested_at().get(),
            "reserved_at_ms": 20,
            "membership_digest": encode_digest(wrong_activation.membership_digest()),
            "display_name": wrong_activation.display_name().as_str(),
            "declared_role": wrong_activation.declared_role().as_str(),
            "transport": "mcp",
            "activated_at_ms": 21,
        }),
        &wire_scope,
        reference,
    )?
    .ok_or("expected an activation projection")?;
    assert_eq!(
        validate_activation_mutation(
            &wrong_payload,
            &activation,
            request.session_id().into_uuid(),
            request.principal(),
        ),
        Err(ForgeSourceError::Protocol)
    );
    assert_eq!(
        parse_activation_readback(missing_transport, &wire_scope, reference),
        Err(ForgeSourceError::Protocol)
    );
    Ok(())
}
