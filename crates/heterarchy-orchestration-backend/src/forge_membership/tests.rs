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

fn rejection_scope() -> Result<ResolvedWorkspaceScope, Box<dyn Error>> {
    let locator = WorkspaceLocator::new(
        LocationId::from_uuid(Uuid::from_u128(0x302)),
        LocationRevision::new(5)?,
        ".",
    )?;
    Ok(ResolvedWorkspaceScope::from_observation(
        WorkspaceId::from_uuid(Uuid::from_u128(0xEB10)),
        WorkspaceRevision::new(8)?,
        locator,
    ))
}

fn rejection_reference() -> Result<MembershipSourceReference, ForgeSourceError> {
    MembershipSourceReference::new(
        Uuid::from_u128(0xEB25),
        Uuid::from_u128(0xEB00),
        Uuid::from_u128(0x15B10),
        1,
        [
            0xa1, 0xff, 0x1c, 0xd8, 0xe5, 0xa9, 0x85, 0xfa, 0x9d, 0xc2, 0x9a, 0x4e, 0x3d, 0x84,
            0x26, 0xaf, 0xaf, 0x17, 0xe2, 0x64, 0xf6, 0xcd, 0xfc, 0x05, 0xf3, 0xc3, 0x70, 0x39,
            0x6f, 0xde, 0xc7, 0x72,
        ],
    )
}

fn rejection_binding() -> Result<AgentBindingReservationRequest, Box<dyn Error>> {
    Ok(AgentBindingReservationRequest::new(
        Uuid::from_u128(0xEB21),
        ActivityId::from_uuid(Uuid::from_u128(0xEB12)),
        AgentInstanceId::from_uuid(Uuid::from_u128(0xEB22)),
        AgentSessionId::from_uuid(Uuid::from_u128(0xEB23)),
        AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(0xEB20))),
        TimestampMillis::new(1_000)?,
    )?)
}

fn rejection_activation() -> Result<AgentBindingActivationRequest, Box<dyn Error>> {
    Ok(AgentBindingActivationRequest::new(
        Uuid::from_u128(0xEB21),
        [
            0xfa, 0xf0, 0x30, 0x48, 0x38, 0xa4, 0x20, 0x0b, 0x57, 0x72, 0x1e, 0x03, 0x63, 0xfb,
            0x54, 0xf6, 0xc3, 0x97, 0xe6, 0x44, 0x44, 0xe6, 0xc2, 0x81, 0x7d, 0xb1, 0x41, 0x1d,
            0xe9, 0x7f, 0xa5, 0x16,
        ],
        AgentDisplayName::new("terminal worker".to_owned())?,
        AgentDeclaredRole::new("reviewer".to_owned())?,
        AgentTransportKind::Other,
    )?)
}

fn rejection_receipt_json(_scope: &ScopeDto, reference: MembershipSourceReference) -> Value {
    json!({
        "request_id": Uuid::from_u128(0xEB21),
        "request_digest": "c2cc2ab70e4d16f7420f474d56042ca6ccfe18eb09a42940eae8bd90488086ee",
        "workspace_id": Uuid::from_u128(0xEB10),
        "activity_id": Uuid::from_u128(0xEB12),
        "agent_id": Uuid::from_u128(0xEB22),
        "session_id": Uuid::from_u128(0xEB23),
        "principal": {"kind":"direct_token","token_id":Uuid::from_u128(0xEB20),"grant_id":null},
        "migration_id": reference.migration_id,
        "destination_id": reference.destination_id,
        "expected_freeze_revision": reference.freeze_revision,
        "manifest_digest": encode_digest(reference.manifest_digest),
        "issuer_id": reference.issuer_id,
        "workspace_root_fingerprint": "31".repeat(16),
        "binding_digest": "32".repeat(32),
        "delegation_digest": "33".repeat(32),
        "membership_digest": "faf0304838a4200b57721e0363fb54f6c397e64444e6c2817db1411de97fa516",
        "display_name": "terminal worker",
        "declared_role": "reviewer",
        "transport": "other",
        "reason": "principal_transport_mismatch",
        "reserved_at_ms": 1_100,
        "requested_at_ms": 1_000,
        "observed_at_ms": 1_400,
        "rejected_at_ms": 1_401,
        "receipt_digest": "535951cdbe6a81226010b8ed2da2a365c03d0807ea232c4f44795174894fbf8f",
    })
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
fn rejection_mutation_and_get_validate_full_receipt_and_absence() -> Result<(), Box<dyn Error>> {
    let scope = rejection_scope()?;
    let reference = rejection_reference()?;
    let binding = rejection_binding()?;
    let activation = rejection_activation()?;
    let (wire_scope, _) = forge_source::validated_scope(&scope)?;
    let receipt = rejection_receipt_json(&wire_scope, reference);
    let mutation = json!({
        "scope": &wire_scope,
        "issuer_id": reference.issuer_id,
        "destination_id": reference.destination_id,
        "status": "recorded",
        "replayed": false,
        "receipt": receipt,
        "retry_authorized": false,
    });
    let observed = parse_rejection_mutation(
        mutation.clone(),
        &wire_scope,
        reference,
        &binding,
        &activation,
        TimestampMillis::new(1_400)?,
    )?;
    assert_eq!(observed.request_id(), binding.request_id());

    let mut mismatched_status = mutation.clone();
    mismatched_status["status"] = json!("replay");
    assert_eq!(
        parse_rejection_mutation(
            mismatched_status,
            &wire_scope,
            reference,
            &binding,
            &activation,
            TimestampMillis::new(1_400)?,
        ),
        Err(ForgeSourceError::Protocol)
    );

    let absent = json!({
        "scope": &wire_scope,
        "issuer_id": reference.issuer_id,
        "destination_id": reference.destination_id,
        "found": false,
        "status": "historical_receipt",
        "receipt": null,
        "retry_authorized": false,
        "request_equivalence_proven": false,
    });
    assert!(
        parse_rejection_readback(
            absent.clone(),
            &wire_scope,
            reference,
            &binding,
            observed.request_digest(),
        )?
        .is_none()
    );
    let mut retry_claim = absent.clone();
    retry_claim["retry_authorized"] = json!(true);
    let mut missing_receipt = absent;
    let _removed = missing_receipt
        .as_object_mut()
        .ok_or("readback object")?
        .remove("receipt");
    for invalid in [retry_claim, missing_receipt] {
        assert_eq!(
            parse_rejection_readback(
                invalid,
                &wire_scope,
                reference,
                &binding,
                observed.request_digest(),
            ),
            Err(ForgeSourceError::Protocol)
        );
    }
    Ok(())
}

#[test]
fn rejection_error_mapping_separates_determinate_conflict_from_unknown_outcome() {
    assert_eq!(
        map_rejection_command_error(ForgeHttpClientError::Service {
            status: 409,
            code: ForgeHttpServiceCode::ForgeUnavailable,
        }),
        ForgeSourceError::Stale
    );
    assert_eq!(
        map_rejection_command_error(ForgeHttpClientError::Service {
            status: 202,
            code: ForgeHttpServiceCode::UnknownOutcome,
        }),
        ForgeSourceError::Unavailable
    );
    assert_eq!(
        map_rejection_command_error(ForgeHttpClientError::Timeout),
        ForgeSourceError::Unavailable
    );
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
