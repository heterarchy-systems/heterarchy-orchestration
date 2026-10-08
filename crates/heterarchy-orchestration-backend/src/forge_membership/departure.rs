//! Typed Forge HTTP boundary for logical Agent departure reconciliation.

use forge_backend::ops::{CommandOp, ScopeDto};
use forge_control::ResolvedWorkspaceScope;
use forge_domain::{
    ActivityCoordinatorRevision, ActivityId, AgentHandoff, AgentInstanceId, AgentInstanceRevision,
    AgentSessionId, CoordinationSnapshot, DagHeadRevision, DagRevisionNumber, TimestampMillis,
    WorkspaceId,
};
use heterarchy_orchestration_operations::{
    AgentDepartureCommand, AgentDepartureKind, AgentDepartureReconciliationRequest,
    DepartureObservation, DepartureObservationParts,
};
use serde::{Deserialize, Deserializer};
use serde_json::{Value, json};

use crate::forge_source::{self, ForgeSourceError};

use super::{
    ForgeMembershipHttpClient, MembershipSourceReference, decode_digest, encode_digest, found_flag,
    principal_json, response_uuid, timestamp, valid_principal, validate_base,
    validated_request_scope,
};

impl ForgeMembershipHttpClient {
    /// Read a source receipt without constructing an effect command or target principal.
    ///
    /// # Errors
    /// Rejects malformed scope, caller identity or source response. Grants no retry permission.
    pub async fn departure_by_identity(
        &self,
        scope: &ResolvedWorkspaceScope,
        reference: &MembershipSourceReference,
        request_id: uuid::Uuid,
        session_id: AgentSessionId,
        principal: forge_domain::AuthenticatedPrincipal,
    ) -> Result<Option<DepartureObservation>, ForgeSourceError> {
        let (wire_scope, _) = validated_request_scope(scope, *reference)?;
        if request_id.is_nil() || session_id.into_uuid().is_nil() || !valid_principal(principal) {
            return Err(ForgeSourceError::InvalidScope);
        }
        let value = self
            .commands
            .command(
                CommandOp::AgentDepartureReconciliationGet,
                json!({
                    "scope": wire_scope, "issuer_id": reference.issuer_id(),
                    "destination_id": reference.destination_id(), "request_id": request_id,
                    "session_id": session_id.into_uuid(), "principal": principal_json(principal),
                }),
            )
            .await
            .map_err(forge_source::map_command_error)?;
        if !found_flag(&value)? {
            let absent: DepartureNotFoundReadback = forge_source::decode_result(value)?;
            validate_base(
                &absent.scope,
                &absent.destination_id,
                absent.retry_authorized,
                absent.request_equivalence_proven,
                &wire_scope,
                reference.destination_id(),
            )?;
            return Ok(None);
        }
        let receipt: DepartureFoundReadback = forge_source::decode_result(value)?;
        validate_base(
            &receipt.scope,
            &receipt.destination_id,
            receipt.retry_authorized,
            receipt.request_equivalence_proven,
            &wire_scope,
            reference.destination_id(),
        )?;
        if response_uuid(&receipt.workspace_id)? != wire_scope.workspace_id
            || response_uuid(&receipt.migration_id)? != reference.migration_id()
            || response_uuid(&receipt.request_id)? != request_id
            || response_uuid(&receipt.session_id)? != session_id.into_uuid()
        {
            return Err(ForgeSourceError::Protocol);
        }
        identity_observation(
            &receipt,
            scope.workspace_id(),
            *reference,
            request_id,
            session_id,
        )
    }

    /// Reconcile one exact detach or handoff through Forge's bounded command transport.
    ///
    /// # Errors
    /// Rejects invalid or unsupported requests and malformed source readback.
    /// Forge transport and service errors retain their typed source classes. An
    /// uncertain mutation is returned to the caller without an automatic read or retry.
    pub async fn reconcile_departure(
        &self,
        scope: &ResolvedWorkspaceScope,
        reference: &MembershipSourceReference,
        request: &AgentDepartureReconciliationRequest,
    ) -> Result<DepartureObservation, ForgeSourceError> {
        let (wire_scope, _) = validated_request_scope(scope, *reference)?;
        let (payload, observed_at) =
            departure_mutation_payload(&wire_scope, *reference, request, scope.workspace_id())?;
        let result = self
            .commands
            .command(CommandOp::AgentDepartureReconcile, payload)
            .await
            .map_err(forge_source::map_command_error)?;
        parse_departure_readback(result, &wire_scope, *reference, request, Some(observed_at))?
            .ok_or(ForgeSourceError::Protocol)
    }

    /// Read one immutable departure by its exact request and caller identity.
    ///
    /// The read returns Forge's original reconciliation timestamp. A caller may
    /// have constructed its current retry request with a later observation time;
    /// that time does not replace or invalidate the stored receipt.
    ///
    /// # Errors
    /// Rejects invalid or unsupported requests and malformed source readback.
    /// Forge transport and service errors retain their typed source classes.
    pub async fn departure_reconciliation(
        &self,
        scope: &ResolvedWorkspaceScope,
        reference: &MembershipSourceReference,
        request: &AgentDepartureReconciliationRequest,
    ) -> Result<Option<DepartureObservation>, ForgeSourceError> {
        let (wire_scope, _) = validated_request_scope(scope, *reference)?;
        validate_departure_request(request, scope.workspace_id())?;
        let payload = json!({
            "scope": &wire_scope,
            "issuer_id": reference.issuer_id(),
            "destination_id": reference.destination_id(),
            "request_id": request.request_id(),
            "session_id": request.session_id().into_uuid(),
            "principal": principal_json(request.principal()),
        });
        let result = self
            .commands
            .command(CommandOp::AgentDepartureReconciliationGet, payload)
            .await
            .map_err(forge_source::map_command_error)?;
        parse_departure_readback(result, &wire_scope, *reference, request, None)
    }
}

fn identity_observation(
    receipt: &DepartureFoundReadback,
    workspace_id: WorkspaceId,
    reference: MembershipSourceReference,
    request_id: uuid::Uuid,
    session_id: AgentSessionId,
) -> Result<Option<DepartureObservation>, ForgeSourceError> {
    DepartureObservation::new(DepartureObservationParts {
        request_id,
        migration_id: reference.migration_id(),
        destination_id: reference.destination_id(),
        workspace_id,
        activity_id: ActivityId::from_uuid(response_uuid(&receipt.activity_id)?),
        kind: match receipt.kind {
            DepartureKindReadback::Detach => AgentDepartureKind::Detach,
            DepartureKindReadback::Handoff => AgentDepartureKind::Handoff,
        },
        agent_id: AgentInstanceId::from_uuid(response_uuid(&receipt.agent_id)?),
        session_id,
        expected_agent_revision: AgentInstanceRevision::new(receipt.expected_agent_revision)
            .map_err(|_| ForgeSourceError::Protocol)?,
        result_agent_revision: AgentInstanceRevision::new(receipt.result_agent_revision)
            .map_err(|_| ForgeSourceError::Protocol)?,
        target_agent_id: receipt
            .target_agent_id
            .as_deref()
            .map(response_uuid)
            .transpose()?
            .map(AgentInstanceId::from_uuid),
        target_session_id: receipt
            .target_session_id
            .as_deref()
            .map(response_uuid)
            .transpose()?
            .map(AgentSessionId::from_uuid),
        result_coordinator_revision: receipt
            .result_coordinator_revision
            .map(ActivityCoordinatorRevision::new)
            .transpose()
            .map_err(|_| ForgeSourceError::Protocol)?,
        identity_digest: decode_digest(&receipt.identity_digest)?,
        reconciled_at: timestamp(receipt.reconciled_at_ms, ForgeSourceError::Protocol)?,
    })
    .map(Some)
    .map_err(|_| ForgeSourceError::Protocol)
}

fn departure_mutation_payload(
    scope: &ScopeDto,
    reference: MembershipSourceReference,
    request: &AgentDepartureReconciliationRequest,
    expected_workspace_id: WorkspaceId,
) -> Result<(Value, TimestampMillis), ForgeSourceError> {
    let (command, observed_at) = match request.command() {
        AgentDepartureCommand::Detach(_) => {
            let detach = validated_detach_request(request)?;
            (
                json!({
                    "kind": "detach",
                    "activity_id": detach.activity_id().into_uuid(),
                    "agent_id": detach.agent_id().into_uuid(),
                    "expected_agent_revision": detach.expected_revision().get(),
                    "observed_at_ms": detach.observed_at().get(),
                }),
                detach.observed_at(),
            )
        }
        AgentDepartureCommand::Handoff(_) => {
            let handoff = validated_handoff_request(request, expected_workspace_id)?;
            let actor = handoff.actor();
            (
                json!({
                    "kind": "handoff",
                    "activity_id": actor.activity_id().into_uuid(),
                    "agent_id": actor.agent_id().into_uuid(),
                    "expected_coordinator_revision": actor.expected_revision().get(),
                    "expected_source_agent_revision": handoff.expected_source_agent_revision().get(),
                    "target_agent_id": handoff.target_agent_id().into_uuid(),
                    "target_session_id": handoff.target_session_id().into_uuid(),
                    "target_principal": principal_json(handoff.target_principal()),
                    "expected_target_revision": handoff.expected_target_revision().get(),
                    "expected_coordination": coordination_snapshot_json(
                        handoff.expected_coordination(),
                    ),
                    "event_id": handoff.event_id().into_uuid(),
                    "observed_at_ms": handoff.observed_at().get(),
                }),
                handoff.observed_at(),
            )
        }
    };

    let payload = json!({
        "scope": scope,
        "issuer_id": reference.issuer_id(),
        "destination_id": reference.destination_id(),
        "reconciliation": {
            "request_id": request.request_id(),
            "session_id": request.session_id().into_uuid(),
            "principal": principal_json(request.principal()),
            "identity_digest": encode_digest(request.identity_digest()),
            "command": command,
        },
    });
    Ok((payload, observed_at))
}

fn coordination_snapshot_json(snapshot: CoordinationSnapshot) -> Value {
    let goal = snapshot.goal().map(|goal| {
        json!({
            "goal_id": goal.goal_id().into_uuid(),
            "expected_revision": goal.expected_revision().get(),
        })
    });
    let node = snapshot.node().map(|node| {
        json!({
            "node_id": node.node_id().into_uuid(),
            "expected_revision": node.revision().get(),
        })
    });
    json!({
        "goal": goal,
        "dag_revision": snapshot.dag_revision().map(DagRevisionNumber::get),
        "dag_head_revision": snapshot
            .dag_head_revision()
            .map(DagHeadRevision::get),
        "node": node,
    })
}

fn validate_departure_request(
    request: &AgentDepartureReconciliationRequest,
    expected_workspace_id: WorkspaceId,
) -> Result<(), ForgeSourceError> {
    match request.command() {
        AgentDepartureCommand::Detach(_) => validated_detach_request(request).map(|_| ()),
        AgentDepartureCommand::Handoff(_) => {
            validated_handoff_request(request, expected_workspace_id).map(|_| ())
        }
    }
}

fn validated_detach_request(
    request: &AgentDepartureReconciliationRequest,
) -> Result<&forge_domain::DetachAgent, ForgeSourceError> {
    if request.request_id().is_nil()
        || request.session_id().into_uuid().is_nil()
        || !valid_principal(request.principal())
        || request.identity_digest() == [0; 32]
        || request.command().kind() != AgentDepartureKind::Detach
    {
        return Err(ForgeSourceError::InvalidScope);
    }
    let AgentDepartureCommand::Detach(detach) = request.command() else {
        return Err(ForgeSourceError::InvalidScope);
    };
    if detach.activity_id().into_uuid().is_nil() || detach.agent_id().into_uuid().is_nil() {
        return Err(ForgeSourceError::InvalidScope);
    }
    Ok(detach)
}

fn validated_handoff_request(
    request: &AgentDepartureReconciliationRequest,
    expected_workspace_id: WorkspaceId,
) -> Result<&AgentHandoff, ForgeSourceError> {
    if request.request_id().is_nil()
        || request.session_id().into_uuid().is_nil()
        || !valid_principal(request.principal())
        || request.identity_digest() == [0; 32]
    {
        return Err(ForgeSourceError::InvalidScope);
    }
    let AgentDepartureCommand::Handoff(handoff) = request.command() else {
        return Err(ForgeSourceError::InvalidScope);
    };
    let actor = handoff.actor();
    let expected_coordination = handoff.expected_coordination();
    if actor.workspace_id() != expected_workspace_id
        || actor.activity_id().into_uuid().is_nil()
        || actor.agent_id().into_uuid().is_nil()
        || actor.session_id().into_uuid().is_nil()
        || actor.session_id() != request.session_id()
        || actor.principal() != request.principal()
        || !valid_principal(actor.principal())
        || handoff.target_agent_id().into_uuid().is_nil()
        || handoff.target_session_id().into_uuid().is_nil()
        || !valid_principal(handoff.target_principal())
        || handoff.target_agent_id() == actor.agent_id()
        || handoff.target_session_id() == actor.session_id()
        || handoff.event_id().into_uuid().is_nil()
        || handoff.expected_source_agent_revision().get() < 1
        || handoff.expected_target_revision().get() < 1
        || handoff.expected_target_revision().checked_next().is_err()
        || actor.expected_revision().checked_next().is_err()
        || !valid_coordination_snapshot(expected_coordination)
    {
        return Err(ForgeSourceError::InvalidScope);
    }
    Ok(handoff)
}

fn valid_coordination_snapshot(snapshot: CoordinationSnapshot) -> bool {
    let valid_goal = snapshot.goal().is_none_or(|goal| {
        !goal.goal_id().into_uuid().is_nil() && goal.expected_revision().get() > 0
    });
    let valid_dag_revision = snapshot
        .dag_revision()
        .is_none_or(|revision| revision.get() > 0);
    let valid_head_revision = snapshot
        .dag_head_revision()
        .is_none_or(|revision| revision.get() > 0);
    let valid_node = snapshot
        .node()
        .is_none_or(|node| !node.node_id().into_uuid().is_nil() && node.revision().get() > 0);
    let valid_shape = CoordinationSnapshot::new(
        snapshot.goal(),
        snapshot.dag_revision(),
        snapshot.dag_head_revision(),
        snapshot.node(),
    )
    .is_ok();
    valid_goal && valid_dag_revision && valid_head_revision && valid_node && valid_shape
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DepartureNotFoundReadback {
    scope: ScopeDto,
    destination_id: String,
    found: bool,
    retry_authorized: bool,
    request_equivalence_proven: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DepartureFoundReadback {
    scope: ScopeDto,
    destination_id: String,
    found: bool,
    retry_authorized: bool,
    request_equivalence_proven: bool,
    workspace_id: String,
    migration_id: String,
    request_id: String,
    activity_id: String,
    kind: DepartureKindReadback,
    agent_id: String,
    session_id: String,
    expected_agent_revision: i64,
    result_agent_revision: i64,
    #[serde(deserialize_with = "deserialize_required_option")]
    target_agent_id: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    target_session_id: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    result_coordinator_revision: Option<i64>,
    identity_digest: String,
    reconciled_at_ms: i64,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum DepartureKindReadback {
    Detach,
    Handoff,
}

fn parse_departure_readback(
    value: Value,
    expected_scope: &ScopeDto,
    reference: MembershipSourceReference,
    request: &AgentDepartureReconciliationRequest,
    expected_reconciled_at: Option<TimestampMillis>,
) -> Result<Option<DepartureObservation>, ForgeSourceError> {
    if found_flag(&value)? {
        let readback: DepartureFoundReadback = forge_source::decode_result(value)?;
        Ok(Some(departure_observation(
            &readback,
            expected_scope,
            reference,
            request,
            expected_reconciled_at,
        )?))
    } else {
        let readback: DepartureNotFoundReadback = forge_source::decode_result(value)?;
        if readback.found {
            return Err(ForgeSourceError::Protocol);
        }
        validate_base(
            &readback.scope,
            &readback.destination_id,
            readback.retry_authorized,
            readback.request_equivalence_proven,
            expected_scope,
            reference.destination_id(),
        )?;
        Ok(None)
    }
}

struct DepartureExpectation {
    kind: DepartureKindReadback,
    activity_id: uuid::Uuid,
    agent_id: uuid::Uuid,
    agent_revision: AgentInstanceRevision,
    target_agent: Option<AgentInstanceId>,
    target_session: Option<AgentSessionId>,
    coordinator_revision: Option<ActivityCoordinatorRevision>,
}

fn expected_departure(
    request: &AgentDepartureReconciliationRequest,
    expected_workspace_id: WorkspaceId,
) -> Result<DepartureExpectation, ForgeSourceError> {
    Ok(match request.command() {
        AgentDepartureCommand::Detach(_) => {
            let detach = validated_detach_request(request)?;
            DepartureExpectation {
                kind: DepartureKindReadback::Detach,
                activity_id: detach.activity_id().into_uuid(),
                agent_id: detach.agent_id().into_uuid(),
                agent_revision: detach.expected_revision(),
                target_agent: None,
                target_session: None,
                coordinator_revision: None,
            }
        }
        AgentDepartureCommand::Handoff(_) => {
            let handoff = validated_handoff_request(request, expected_workspace_id)?;
            let coordinator_revision = handoff
                .actor()
                .expected_revision()
                .checked_next()
                .map_err(|_| ForgeSourceError::Protocol)?;
            DepartureExpectation {
                kind: DepartureKindReadback::Handoff,
                activity_id: handoff.actor().activity_id().into_uuid(),
                agent_id: handoff.actor().agent_id().into_uuid(),
                agent_revision: handoff.expected_source_agent_revision(),
                target_agent: Some(handoff.target_agent_id()),
                target_session: Some(handoff.target_session_id()),
                coordinator_revision: Some(coordinator_revision),
            }
        }
    })
}

fn departure_observation(
    readback: &DepartureFoundReadback,
    expected_scope: &ScopeDto,
    reference: MembershipSourceReference,
    request: &AgentDepartureReconciliationRequest,
    expected_reconciled_at: Option<TimestampMillis>,
) -> Result<DepartureObservation, ForgeSourceError> {
    validate_base(
        &readback.scope,
        &readback.destination_id,
        readback.retry_authorized,
        readback.request_equivalence_proven,
        expected_scope,
        reference.destination_id(),
    )?;

    let DepartureExpectation {
        kind: expected_kind,
        activity_id: expected_activity_id,
        agent_id: expected_agent_id,
        agent_revision: expected_agent_revision,
        target_agent: expected_target_agent_id,
        target_session: expected_target_session_id,
        coordinator_revision: expected_result_coordinator_revision,
    } = expected_departure(request, WorkspaceId::from_uuid(expected_scope.workspace_id))?;
    if !readback.found || readback.kind != expected_kind {
        return Err(ForgeSourceError::Protocol);
    }
    let workspace_id = response_uuid(&readback.workspace_id)?;
    let migration_id = response_uuid(&readback.migration_id)?;
    let destination_id = response_uuid(&readback.destination_id)?;
    let request_id = response_uuid(&readback.request_id)?;
    let activity_id = response_uuid(&readback.activity_id)?;
    let agent_id = response_uuid(&readback.agent_id)?;
    let session_id = response_uuid(&readback.session_id)?;
    let identity_digest = decode_digest(&readback.identity_digest)?;
    let target_agent_id = readback
        .target_agent_id
        .as_deref()
        .map(response_uuid)
        .transpose()?
        .map(AgentInstanceId::from_uuid);
    let target_session_id = readback
        .target_session_id
        .as_deref()
        .map(response_uuid)
        .transpose()?
        .map(AgentSessionId::from_uuid);
    let result_coordinator_revision = readback
        .result_coordinator_revision
        .map(ActivityCoordinatorRevision::new)
        .transpose()
        .map_err(|_| ForgeSourceError::Protocol)?;
    let result_agent_revision = AgentInstanceRevision::new(readback.result_agent_revision)
        .map_err(|_| ForgeSourceError::Protocol)?;
    let reconciled_at = timestamp(readback.reconciled_at_ms, ForgeSourceError::Protocol)?;

    if workspace_id != expected_scope.workspace_id
        || migration_id != reference.migration_id()
        || destination_id != reference.destination_id()
        || request_id != request.request_id()
        || activity_id != expected_activity_id
        || agent_id != expected_agent_id
        || session_id != request.session_id().into_uuid()
        || readback.expected_agent_revision != expected_agent_revision.get()
        || target_agent_id != expected_target_agent_id
        || target_session_id != expected_target_session_id
        || result_coordinator_revision != expected_result_coordinator_revision
        || identity_digest != request.identity_digest()
        || expected_reconciled_at.is_some_and(|expected| reconciled_at != expected)
    {
        return Err(ForgeSourceError::Protocol);
    }

    let parts = DepartureObservationParts {
        request_id,
        migration_id,
        destination_id,
        workspace_id: WorkspaceId::from_uuid(workspace_id),
        activity_id: ActivityId::from_uuid(activity_id),
        kind: request.command().kind(),
        agent_id: AgentInstanceId::from_uuid(agent_id),
        session_id: AgentSessionId::from_uuid(session_id),
        expected_agent_revision,
        result_agent_revision,
        target_agent_id,
        target_session_id,
        result_coordinator_revision,
        identity_digest,
        reconciled_at,
    };
    DepartureObservation::new(parts).map_err(|_| ForgeSourceError::Protocol)
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    use forge_backend::ops::LocatorDto;
    use forge_domain::{
        ActivityCoordinatorActor, AgentEventId, AgentFocusNode, AuthenticatedPrincipal,
        DagHeadRevision, DagNodeId, DagNodeRevision, DagRevisionNumber, DetachAgent, DirectTokenId,
        GoalAttribution, GoalId, GoalRevision,
    };
    use serde_json::json;
    use std::error::Error;

    const WORKSPACE_ID: u128 = 0xA001;
    const ACTIVITY_ID: u128 = 0xA002;
    const AGENT_ID: u128 = 0xA003;
    const SESSION_ID: u128 = 0xA004;
    const REQUEST_ID: u128 = 0xA005;
    const DESTINATION_ID: u128 = 0xA006;
    const MIGRATION_ID: u128 = 0xA007;
    const TARGET_AGENT_ID: u128 = 0xA010;
    const TARGET_SESSION_ID: u128 = 0xA011;
    const TARGET_PRINCIPAL_ID: u128 = 0xA012;
    const HANDOFF_EVENT_ID: u128 = 0xA013;

    fn scope() -> ScopeDto {
        ScopeDto {
            workspace_id: Uuid::from_u128(WORKSPACE_ID),
            workspace_revision: 5,
            locator: LocatorDto {
                location_id: Uuid::from_u128(0xA008),
                expected_revision: 4,
                relative_root: "workspace".to_owned(),
            },
        }
    }

    fn source_reference() -> Result<MembershipSourceReference, ForgeSourceError> {
        MembershipSourceReference::new(
            Uuid::from_u128(0xA009),
            Uuid::from_u128(DESTINATION_ID),
            Uuid::from_u128(MIGRATION_ID),
            3,
            [0x33; 32],
        )
    }

    fn request(observed_at: i64) -> Result<AgentDepartureReconciliationRequest, Box<dyn Error>> {
        Ok(AgentDepartureReconciliationRequest::new(
            Uuid::from_u128(REQUEST_ID),
            AgentDepartureCommand::Detach(DetachAgent::new(
                AgentInstanceId::from_uuid(Uuid::from_u128(AGENT_ID)),
                ActivityId::from_uuid(Uuid::from_u128(ACTIVITY_ID)),
                AgentInstanceRevision::new(7)?,
                TimestampMillis::new(observed_at)?,
            )),
            AgentSessionId::from_uuid(Uuid::from_u128(SESSION_ID)),
            AuthenticatedPrincipal::TrustedStdio,
            [0xAB; 32],
        )?)
    }

    fn handoff_request(
        observed_at: i64,
    ) -> Result<AgentDepartureReconciliationRequest, Box<dyn Error>> {
        let source_principal = AuthenticatedPrincipal::TrustedStdio;
        let target_principal = AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(
            Uuid::from_u128(TARGET_PRINCIPAL_ID),
        ));
        let expected_coordination = CoordinationSnapshot::new(
            Some(GoalAttribution::new(
                GoalId::from_uuid(Uuid::from_u128(0xA014)),
                GoalRevision::new(2)?,
            )),
            Some(DagRevisionNumber::new(3)?),
            Some(DagHeadRevision::new(4)?),
            Some(AgentFocusNode::new(
                DagNodeId::from_uuid(Uuid::from_u128(0xA015)),
                DagNodeRevision::new(5)?,
            )),
        )?;
        let command = AgentHandoff::new(
            ActivityCoordinatorActor::new(
                WorkspaceId::from_uuid(Uuid::from_u128(WORKSPACE_ID)),
                ActivityId::from_uuid(Uuid::from_u128(ACTIVITY_ID)),
                AgentInstanceId::from_uuid(Uuid::from_u128(AGENT_ID)),
                ActivityCoordinatorRevision::new(11)?,
                source_principal,
                AgentSessionId::from_uuid(Uuid::from_u128(SESSION_ID)),
            ),
            AgentInstanceRevision::new(9)?,
            AgentInstanceId::from_uuid(Uuid::from_u128(TARGET_AGENT_ID)),
            AgentSessionId::from_uuid(Uuid::from_u128(TARGET_SESSION_ID)),
            target_principal,
            AgentInstanceRevision::new(6)?,
            expected_coordination,
            AgentEventId::from_uuid(Uuid::from_u128(HANDOFF_EVENT_ID)),
            TimestampMillis::new(observed_at)?,
        );
        Ok(AgentDepartureReconciliationRequest::new(
            Uuid::from_u128(REQUEST_ID),
            AgentDepartureCommand::Handoff(Box::new(command)),
            AgentSessionId::from_uuid(Uuid::from_u128(SESSION_ID)),
            source_principal,
            [0xCD; 32],
        )?)
    }

    fn found_response(
        request: &AgentDepartureReconciliationRequest,
        at: i64,
    ) -> Result<Value, Box<dyn Error>> {
        let detach = request.command().detach().ok_or("detach request missing")?;
        Ok(json!({
            "scope": scope(),
            "destination_id": Uuid::from_u128(DESTINATION_ID).to_string(),
            "found": true,
            "retry_authorized": false,
            "request_equivalence_proven": false,
            "workspace_id": Uuid::from_u128(WORKSPACE_ID).to_string(),
            "migration_id": Uuid::from_u128(MIGRATION_ID).to_string(),
            "request_id": request.request_id().to_string(),
            "activity_id": detach.activity_id().into_uuid().to_string(),
            "kind": "detach",
            "agent_id": detach.agent_id().into_uuid().to_string(),
            "session_id": request.session_id().into_uuid().to_string(),
            "expected_agent_revision": detach.expected_revision().get(),
            "result_agent_revision": 8,
            "target_agent_id": null,
            "target_session_id": null,
            "result_coordinator_revision": null,
            "identity_digest": "ab".repeat(32),
            "reconciled_at_ms": at,
        }))
    }

    fn found_handoff_response(
        request: &AgentDepartureReconciliationRequest,
        at: i64,
    ) -> Result<Value, Box<dyn Error>> {
        let handoff = request
            .command()
            .handoff()
            .ok_or("handoff request missing")?;
        let actor = handoff.actor();
        Ok(json!({
            "scope": scope(),
            "destination_id": Uuid::from_u128(DESTINATION_ID).to_string(),
            "found": true,
            "retry_authorized": false,
            "request_equivalence_proven": false,
            "workspace_id": actor.workspace_id().into_uuid().to_string(),
            "migration_id": Uuid::from_u128(MIGRATION_ID).to_string(),
            "request_id": request.request_id().to_string(),
            "activity_id": actor.activity_id().into_uuid().to_string(),
            "kind": "handoff",
            "agent_id": actor.agent_id().into_uuid().to_string(),
            "session_id": request.session_id().into_uuid().to_string(),
            "expected_agent_revision": handoff.expected_source_agent_revision().get(),
            "result_agent_revision": handoff.expected_source_agent_revision().checked_next()?.get(),
            "target_agent_id": handoff.target_agent_id().into_uuid().to_string(),
            "target_session_id": handoff.target_session_id().into_uuid().to_string(),
            "result_coordinator_revision": actor.expected_revision().checked_next()?.get(),
            "identity_digest": encode_digest(request.identity_digest()),
            "reconciled_at_ms": at,
        }))
    }

    fn parse(
        value: Value,
        reference: MembershipSourceReference,
        request: &AgentDepartureReconciliationRequest,
        expected_at: Option<TimestampMillis>,
    ) -> Result<Option<DepartureObservation>, ForgeSourceError> {
        parse_departure_readback(value, &scope(), reference, request, expected_at)
    }

    #[test]
    fn handoff_payload_uses_typed_target_principal_and_complete_plan() -> Result<(), Box<dyn Error>>
    {
        let reference = source_reference()?;
        let handoff_request = handoff_request(200)?;
        let (payload, observed_at) = departure_mutation_payload(
            &scope(),
            reference,
            &handoff_request,
            WorkspaceId::from_uuid(Uuid::from_u128(WORKSPACE_ID)),
        )?;
        let handoff = handoff_request
            .command()
            .handoff()
            .ok_or("handoff command missing")?;
        assert_eq!(observed_at, handoff.observed_at());
        assert_eq!(
            payload["reconciliation"]["command"]["kind"],
            json!("handoff")
        );
        assert_eq!(
            payload["reconciliation"]["command"]["target_principal"],
            principal_json(handoff.target_principal())
        );
        assert_eq!(
            payload["reconciliation"]["command"]["expected_coordination"],
            json!({
                "goal": {
                    "goal_id": Uuid::from_u128(0xA014),
                    "expected_revision": 2,
                },
                "dag_revision": 3,
                "dag_head_revision": 4,
                "node": {
                    "node_id": Uuid::from_u128(0xA015),
                    "expected_revision": 5,
                },
            })
        );
        assert!(
            forge_backend::ops::parse_command(
                CommandOp::AgentDepartureReconcile,
                Uuid::from_u128(0xA020),
                &payload,
            )
            .is_ok()
        );

        let detach_request = request(100)?;
        let (detach_payload, _) = departure_mutation_payload(
            &scope(),
            reference,
            &detach_request,
            WorkspaceId::from_uuid(Uuid::from_u128(WORKSPACE_ID)),
        )?;
        assert_eq!(
            detach_payload["reconciliation"]["command"],
            json!({
                "kind": "detach",
                "activity_id": Uuid::from_u128(ACTIVITY_ID),
                "agent_id": Uuid::from_u128(AGENT_ID),
                "expected_agent_revision": 7,
                "observed_at_ms": 100,
            })
        );
        assert!(
            forge_backend::ops::parse_command(
                CommandOp::AgentDepartureReconcile,
                Uuid::from_u128(0xA021),
                &detach_payload,
            )
            .is_ok()
        );
        Ok(())
    }

    #[test]
    fn accepts_exact_handoff_receipt_and_keeps_original_get_timestamp() -> Result<(), Box<dyn Error>>
    {
        let reference = source_reference()?;
        let mutation_request = handoff_request(200)?;
        let observation = parse(
            found_handoff_response(&mutation_request, 200)?,
            reference,
            &mutation_request,
            Some(TimestampMillis::new(200)?),
        )?
        .ok_or("found handoff receipt missing")?;
        assert_eq!(observation.kind(), AgentDepartureKind::Handoff);
        assert_eq!(
            observation.target_agent_id(),
            Some(AgentInstanceId::from_uuid(Uuid::from_u128(TARGET_AGENT_ID)))
        );
        assert_eq!(
            observation.target_session_id(),
            Some(AgentSessionId::from_uuid(Uuid::from_u128(
                TARGET_SESSION_ID
            )))
        );
        assert_eq!(
            observation.result_coordinator_revision(),
            Some(ActivityCoordinatorRevision::new(12)?)
        );

        let retry_request = handoff_request(250)?;
        let old_receipt = parse(
            found_handoff_response(&retry_request, 200)?,
            reference,
            &retry_request,
            None,
        )?
        .ok_or("stored handoff receipt missing")?;
        assert_eq!(old_receipt.reconciled_at(), TimestampMillis::new(200)?);
        Ok(())
    }

    #[test]
    fn rejects_handoff_digest_target_and_revision_mismatches() -> Result<(), Box<dyn Error>> {
        let reference = source_reference()?;
        let request = handoff_request(200)?;
        let valid = found_handoff_response(&request, 200)?;
        let mut wrong_digest = valid.clone();
        wrong_digest["identity_digest"] = json!(encode_digest([0xCE; 32]));
        let mut wrong_target_agent = valid.clone();
        wrong_target_agent["target_agent_id"] = json!(Uuid::from_u128(0xA016).to_string());
        let mut wrong_target_session = valid.clone();
        wrong_target_session["target_session_id"] = json!(Uuid::from_u128(0xA017).to_string());
        let mut wrong_expected_agent_revision = valid.clone();
        wrong_expected_agent_revision["expected_agent_revision"] = json!(8);
        let mut wrong_result_agent_revision = valid.clone();
        wrong_result_agent_revision["result_agent_revision"] = json!(11);
        let mut wrong_result_coordinator_revision = valid;
        wrong_result_coordinator_revision["result_coordinator_revision"] = json!(13);

        for invalid in [
            wrong_digest,
            wrong_target_agent,
            wrong_target_session,
            wrong_expected_agent_revision,
            wrong_result_agent_revision,
            wrong_result_coordinator_revision,
        ] {
            assert!(matches!(
                parse(invalid, reference, &request, None),
                Err(ForgeSourceError::Protocol)
            ));
        }

        let wrong_session = AgentDepartureReconciliationRequest::new(
            request.request_id(),
            request.command().clone(),
            AgentSessionId::from_uuid(Uuid::from_u128(0xA018)),
            request.principal(),
            request.identity_digest(),
        );
        let wrong_principal = AgentDepartureReconciliationRequest::new(
            request.request_id(),
            request.command().clone(),
            request.session_id(),
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(0xA019))),
            request.identity_digest(),
        );
        assert!(wrong_session.is_err());
        assert!(wrong_principal.is_err());
        Ok(())
    }

    #[test]
    fn accepts_exact_detach_receipt_and_preserves_get_timestamp() -> Result<(), Box<dyn Error>> {
        let reference = source_reference()?;
        let mutation_request = request(100)?;
        let observation = parse(
            found_response(&mutation_request, 100)?,
            reference,
            &mutation_request,
            Some(TimestampMillis::new(100)?),
        )?
        .ok_or("found detach receipt missing")?;
        assert_eq!(observation.request_id(), Uuid::from_u128(REQUEST_ID));
        assert_eq!(observation.kind(), AgentDepartureKind::Detach);
        assert_eq!(observation.reconciled_at(), TimestampMillis::new(100)?);

        let retry_request = request(200)?;
        let old_receipt = parse(
            found_response(&retry_request, 100)?,
            reference,
            &retry_request,
            None,
        )?
        .ok_or("stored detach receipt missing")?;
        assert_eq!(old_receipt.reconciled_at(), TimestampMillis::new(100)?);
        Ok(())
    }

    #[test]
    fn rejects_missing_nullable_fields_unknown_fields_and_foreign_identity()
    -> Result<(), Box<dyn Error>> {
        let reference = source_reference()?;
        let request = request(100)?;

        for field in [
            "target_agent_id",
            "target_session_id",
            "result_coordinator_revision",
        ] {
            let mut missing = found_response(&request, 100)?;
            missing
                .as_object_mut()
                .ok_or("response object missing")?
                .remove(field);
            assert!(matches!(
                parse(
                    missing,
                    reference,
                    &request,
                    Some(TimestampMillis::new(100)?)
                ),
                Err(ForgeSourceError::Protocol)
            ));
        }

        let mut unknown = found_response(&request, 100)?;
        unknown["unrecognized"] = json!(true);
        assert!(matches!(
            parse(
                unknown,
                reference,
                &request,
                Some(TimestampMillis::new(100)?)
            ),
            Err(ForgeSourceError::Protocol)
        ));

        let mut foreign = found_response(&request, 100)?;
        foreign["session_id"] = json!(Uuid::from_u128(0xA099).to_string());
        assert!(matches!(
            parse(
                foreign,
                reference,
                &request,
                Some(TimestampMillis::new(100)?)
            ),
            Err(ForgeSourceError::Protocol)
        ));

        let mut target = found_response(&request, 100)?;
        target["target_agent_id"] = json!(Uuid::from_u128(0xA010).to_string());
        assert!(matches!(
            parse(
                target,
                reference,
                &request,
                Some(TimestampMillis::new(100)?)
            ),
            Err(ForgeSourceError::Protocol)
        ));
        Ok(())
    }

    #[test]
    fn mutation_response_must_echo_exact_request_timestamp() -> Result<(), Box<dyn Error>> {
        let reference = source_reference()?;
        let request = request(100)?;
        assert!(matches!(
            parse(
                found_response(&request, 99)?,
                reference,
                &request,
                Some(TimestampMillis::new(100)?),
            ),
            Err(ForgeSourceError::Protocol)
        ));
        Ok(())
    }

    #[test]
    fn not_found_response_has_only_exact_base_projection() -> Result<(), Box<dyn Error>> {
        let reference = source_reference()?;
        let request = request(100)?;
        let absent = json!({
            "scope": scope(),
            "destination_id": Uuid::from_u128(DESTINATION_ID).to_string(),
            "found": false,
            "retry_authorized": false,
            "request_equivalence_proven": false,
        });
        assert!(parse(absent, reference, &request, None)?.is_none());

        let mut malformed = json!({
            "scope": scope(),
            "destination_id": Uuid::from_u128(DESTINATION_ID).to_string(),
            "found": false,
            "retry_authorized": false,
            "request_equivalence_proven": false,
            "request_id": Uuid::from_u128(REQUEST_ID).to_string(),
        });
        assert!(matches!(
            parse(malformed.take(), reference, &request, None),
            Err(ForgeSourceError::Protocol)
        ));
        Ok(())
    }
}
