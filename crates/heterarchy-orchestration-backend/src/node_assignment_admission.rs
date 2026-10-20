//! Source-fenced pre-offer Node Assignment admission and pure recovery.

use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    routing::post,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use forge_backend::ops::{Command, CommandOp, parse_command};
use forge_control::AgentAssignmentMutation;
use forge_domain::{
    ActivityId, AgentAssignmentId, AgentInstanceId, AgentSessionId, AuthenticatedPrincipal,
    DagNodeId, DagRevisionNumber, GoalId, GoalRevision, TimestampMillis, WorkspaceId,
};
use heterarchy_orchestration_operations::{
    AssignmentAffinityCommand, AssignmentAffinityDisposition, AssignmentAffinityReceipt,
    NodeTeamBindingScope, OperationalActor,
};
use heterarchy_orchestration_runtime::participation::ParticipationTarget;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::{
    ApiError, BackendState, CoordinatorAuthorityError, RuntimeError,
    wire::node_assignment_admission::{
        NodeAssignmentAdmissionDispositionWire, NodeAssignmentAdmissionReceiptWire,
        NodeAssignmentAdmissionRecoverResponse, NodeAssignmentAdmissionRequest,
        NodeAssignmentAdmissionResponse, NodeAssignmentRequesterWire, NodeAssignmentScopeWire,
    },
};

const MAX_SOURCE_OFFER_BYTES: usize = 16_384;

pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/node-assignment-admissions/admit",
            post(admit),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/node-assignment-admissions/admit/recover",
            post(recover),
        )
}

async fn admit(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<NodeAssignmentAdmissionRequest>,
) -> Result<Json<NodeAssignmentAdmissionResponse>, ApiError> {
    let validated = validate_request(&path, &body, principal)?;
    let requester = validated.requester;
    let command = validated.command;
    let scope = body.scope.clone();
    let node_scope = command.node_scope;
    let owner = state.clone();
    state
        .run_owned(async move {
            owner
                .catalog_job(move |runtime| runtime.require_participation(requester, principal))
                .await?;
            owner.workspace_scope_proof(scope.clone()).await?;
            let first = owner
                .assignment_source_proof(
                    scope.clone(),
                    node_scope,
                    body.activity_id,
                    body.target.agent_id,
                    principal,
                )
                .await?;
            validate_source_proof(&body, principal, first)?;
            owner
                .runtime
                .ensure_broker_owner()
                .await
                .map_err(RuntimeError::BrokerOwner)
                .map_err(ApiError::Runtime)?;
            owner.workspace_scope_proof(scope.clone()).await?;
            let second = owner
                .assignment_source_proof(
                    scope,
                    node_scope,
                    body.activity_id,
                    body.target.agent_id,
                    principal,
                )
                .await?;
            validate_source_proof(&body, principal, second)?;
            let admitted = owner
                .catalog_job(move |runtime| {
                    runtime.admit_node_assignment_offer(requester, principal, command)
                })
                .await?;
            Ok(Json(NodeAssignmentAdmissionResponse {
                request: body,
                admission: project_receipt(&admitted.receipt)?,
                created: admitted.created,
            }))
        })
        .await
}

async fn recover(
    State(state): State<BackendState>,
    Path(path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<NodeAssignmentAdmissionRequest>,
) -> Result<Json<NodeAssignmentAdmissionRecoverResponse>, ApiError> {
    let validated = validate_request(&path, &body, principal)?;
    let admission = state
        .run_catalog(move |runtime| {
            runtime.recover_node_assignment_offer(validated.requester, principal, validated.command)
        })
        .await?
        .as_ref()
        .map(project_receipt)
        .transpose()?;
    Ok(Json(NodeAssignmentAdmissionRecoverResponse {
        request: body,
        admission,
        retry_authorized: false,
    }))
}

struct ValidatedAdmission {
    requester: ParticipationTarget,
    command: AssignmentAffinityCommand,
}

fn validate_request(
    path: &str,
    request: &NodeAssignmentAdmissionRequest,
    principal: AuthenticatedPrincipal,
) -> Result<ValidatedAdmission, ApiError> {
    crate::app_support::validate_workspace_reference(
        &request.scope,
        Some(request.coordinator_session_id),
    )?;
    if crate::canonical_uuid(path)? != request.scope.workspace_id
        || request.activity_id.is_nil()
        || request.coordinator_session_id.is_nil()
        || request.coordinator_agent_id.is_nil()
        || request.coordinator_revision <= 0
        || request.assignment_id.is_nil()
        || request.target.agent_id.is_nil()
        || request.target.session_id.is_nil()
        || request.target.agent_revision <= 0
        || request.team_id.is_some_and(|id| id.is_nil())
        || request
            .expected_binding_revision
            .is_some_and(|revision| revision == 0 || revision >= i64::MAX.unsigned_abs())
    {
        return Err(ApiError::InvalidIdentity);
    }
    let node_scope = source_scope(request)?;
    let offer_bytes = decode_offer_bytes(&request.source_offer_bytes_base64)?;
    let expected_digest = decode_sha256(&request.source_offer_sha256)?;
    let actual_digest: [u8; 32] = Sha256::digest(&offer_bytes).into();
    if actual_digest != expected_digest {
        return Err(ApiError::InvalidIdentity);
    }
    let payload: Value =
        serde_json::from_slice(&offer_bytes).map_err(|_| ApiError::InvalidIdentity)?;
    let parsed = parse_command(CommandOp::AgentAssignmentMutate, Uuid::now_v7(), &payload)
        .map_err(|_| ApiError::InvalidIdentity)?;
    let Command::AgentAssignmentMutate {
        request: parsed, ..
    } = parsed
    else {
        return Err(ApiError::InvalidIdentity);
    };
    let AgentAssignmentMutation::Offer(offer) = parsed.mutation else {
        return Err(ApiError::InvalidIdentity);
    };
    let coordinator = offer.coordinator();
    let target = offer.target();
    let target_principal = target.principal();
    let locator = parsed.scope.locator();
    if parsed.scope.workspace_id().into_uuid() != request.scope.workspace_id
        || parsed.scope.workspace_revision().get() != request.scope.workspace_revision
        || locator.location_id().into_uuid() != request.scope.location_id
        || locator.expected_revision().get() != request.scope.location_revision
        || locator.encode() != request.scope.locator
        || offer.assignment_id().into_uuid() != request.assignment_id
        || coordinator.workspace_id().into_uuid() != request.scope.workspace_id
        || coordinator.activity_id().into_uuid() != request.activity_id
        || coordinator.session_id().into_uuid() != request.coordinator_session_id
        || coordinator.agent_id().into_uuid() != request.coordinator_agent_id
        || coordinator.expected_revision().get() != request.coordinator_revision
        || coordinator.principal() != principal
        || target.agent_id().into_uuid() != request.target.agent_id
        || target.session_id().into_uuid() != request.target.session_id
        || target.agent_revision().get() != request.target.agent_revision
        || offer.goal_id().into_uuid() != request.source_fences.goal_id
        || offer.expected_goal_created_at().get() != request.source_fences.goal_created_at_ms
        || offer.expected_goal_revision().get() != request.source_fences.goal_revision
        || offer.dag_revision().get() != request.source_fences.dag_revision
        || offer.expected_head_revision().get() != request.source_fences.dag_head_revision
        || offer.node_id().into_uuid() != request.source_fences.node_id
        || offer.expected_node_revision().get() != request.source_fences.node_revision
    {
        return Err(ApiError::InvalidIdentity);
    }
    Ok(ValidatedAdmission {
        requester: ParticipationTarget {
            workspace_id: request.scope.workspace_id,
            activity_id: request.activity_id,
            agent_id: request.coordinator_agent_id,
            session_id: request.coordinator_session_id,
        },
        command: AssignmentAffinityCommand {
            workspace_id: WorkspaceId::from_uuid(request.scope.workspace_id),
            assignment_id: AgentAssignmentId::from_uuid(request.assignment_id),
            activity_id: ActivityId::from_uuid(request.activity_id),
            target_agent_id: AgentInstanceId::from_uuid(request.target.agent_id),
            target_session_id: AgentSessionId::from_uuid(request.target.session_id),
            expected_target_principal: target_principal,
            requester: OperationalActor::Participant {
                activity_id: ActivityId::from_uuid(request.activity_id),
                agent_id: AgentInstanceId::from_uuid(request.coordinator_agent_id),
                session_id: AgentSessionId::from_uuid(request.coordinator_session_id),
            },
            team_id: request.team_id,
            node_scope,
            expected_binding_revision: request.expected_binding_revision,
            assignment_payload_fingerprint: actual_digest,
            original_offer_payload: offer_bytes,
            observed_at: offer.observed_at(),
        },
    })
}

fn source_scope(
    request: &NodeAssignmentAdmissionRequest,
) -> Result<NodeTeamBindingScope, ApiError> {
    let fences = request.source_fences;
    if fences.goal_id.is_nil()
        || fences.node_id.is_nil()
        || fences.goal_created_at_ms < 0
        || fences.goal_revision <= 0
        || fences.dag_revision <= 0
        || fences.dag_base_goal_revision <= 0
        || fences.dag_head_revision <= 0
        || fences.node_revision <= 0
    {
        return Err(ApiError::InvalidIdentity);
    }
    NodeTeamBindingScope::new(
        WorkspaceId::from_uuid(request.scope.workspace_id),
        ActivityId::from_uuid(request.activity_id),
        GoalId::from_uuid(fences.goal_id),
        TimestampMillis::new(fences.goal_created_at_ms).map_err(|_| ApiError::InvalidIdentity)?,
        DagRevisionNumber::new(fences.dag_revision).map_err(|_| ApiError::InvalidIdentity)?,
        GoalRevision::new(fences.dag_base_goal_revision).map_err(|_| ApiError::InvalidIdentity)?,
        DagNodeId::from_uuid(fences.node_id),
    )
    .map_err(|_| ApiError::InvalidIdentity)
}

fn decode_offer_bytes(encoded: &str) -> Result<Vec<u8>, ApiError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| ApiError::InvalidIdentity)?;
    if bytes.is_empty()
        || bytes.len() > MAX_SOURCE_OFFER_BYTES
        || URL_SAFE_NO_PAD.encode(&bytes) != encoded
    {
        return Err(ApiError::InvalidIdentity);
    }
    Ok(bytes)
}

fn decode_sha256(value: &str) -> Result<[u8; 32], ApiError> {
    if value.len() != 64 {
        return Err(ApiError::InvalidIdentity);
    }
    let mut digest = [0_u8; 32];
    for (index, pair) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let high = hex_nibble(pair[0]).ok_or(ApiError::InvalidIdentity)?;
        let low = hex_nibble(pair[1]).ok_or(ApiError::InvalidIdentity)?;
        digest[index] = (high << 4) | low;
    }
    Ok(digest)
}

const fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

type SourceProof = (
    forge_domain::ActivityCoordinatorActor,
    crate::forge_source::node_assignment::NodeAssignmentObservation,
    crate::forge_source::node_assignment::NodeAssignmentTargetAgentObservation,
);

fn validate_source_proof(
    request: &NodeAssignmentAdmissionRequest,
    principal: AuthenticatedPrincipal,
    (actor, observation, target_agent): SourceProof,
) -> Result<(), ApiError> {
    let fences = request.source_fences;
    if actor.workspace_id().into_uuid() != request.scope.workspace_id
        || actor.activity_id().into_uuid() != request.activity_id
        || actor.agent_id().into_uuid() != request.coordinator_agent_id
        || actor.session_id().into_uuid() != request.coordinator_session_id
        || actor.expected_revision().get() != request.coordinator_revision
        || actor.principal() != principal
        || observation.workspace_id().into_uuid() != request.scope.workspace_id
        || observation.activity_id().into_uuid() != request.activity_id
        || observation.goal_created_at().get() != fences.goal_created_at_ms
        || observation.goal_revision().get() != fences.goal_revision
        || observation.dag_base_goal_revision().get() != fences.dag_base_goal_revision
        || observation.scope().goal_id().into_uuid() != fences.goal_id
        || observation.scope().dag_revision().get() != fences.dag_revision
        || observation.scope().node_id().into_uuid() != fences.node_id
        || observation.head().current_dag_revision().get() != fences.dag_revision
        || observation.head().head_revision().get() != fences.dag_head_revision
        || observation.node_revision().get() != fences.node_revision
        || observation.assignment().is_some()
        || target_agent.workspace_id().into_uuid() != request.scope.workspace_id
        || target_agent.activity_id().into_uuid() != request.activity_id
        || target_agent.agent_id().into_uuid() != request.target.agent_id
        || target_agent.revision().get() != request.target.agent_revision
        || !target_agent.attachment_state().is_attached()
    {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    Ok(())
}

fn project_receipt(
    receipt: &AssignmentAffinityReceipt,
) -> Result<NodeAssignmentAdmissionReceiptWire, ApiError> {
    let scope = receipt
        .node_scope()
        .ok_or(ApiError::Runtime(RuntimeError::Conflict))?;
    let requester = receipt.requester();
    let required = |value: Option<Uuid>| value.ok_or(ApiError::Runtime(RuntimeError::Conflict));
    let (participation_id, profile_id) =
        match (requester.participation_id(), requester.profile_id()) {
            (Some(participation), Some(profile)) => {
                (Some(participation.into_uuid()), Some(profile))
            }
            (None, None) => (None, None),
            _ => return Err(ApiError::Runtime(RuntimeError::Conflict)),
        };
    let disposition = match receipt.disposition() {
        AssignmentAffinityDisposition::Pending => NodeAssignmentAdmissionDispositionWire::Pending,
        AssignmentAffinityDisposition::Active => NodeAssignmentAdmissionDispositionWire::Active,
        _ => return Err(ApiError::Runtime(RuntimeError::Conflict)),
    };
    Ok(NodeAssignmentAdmissionReceiptWire {
        workspace_id: receipt.workspace_id().into_uuid(),
        assignment_id: receipt.assignment_id().into_uuid(),
        activity_id: receipt.activity_id().into_uuid(),
        target_agent_id: receipt.target_agent_id().into_uuid(),
        target_session_id: receipt.target_session_id().into_uuid(),
        target_participation_id: receipt.target_participation_id().into_uuid(),
        target_participant_id: receipt.target_participant_id(),
        team_id: receipt.team_id(),
        team_membership_revision: receipt.team_membership_revision(),
        node_scope: NodeAssignmentScopeWire {
            activity_id: scope.activity_id().into_uuid(),
            goal_id: scope.goal_id().into_uuid(),
            goal_created_at_ms: scope.goal_created_at().get(),
            dag_revision: scope.dag_revision().get(),
            dag_base_goal_revision: scope.dag_base_goal_revision().get(),
            node_id: scope.node_id().into_uuid(),
        },
        binding_revision: receipt.binding_revision(),
        disposition,
        requester: NodeAssignmentRequesterWire {
            activity_id: required(requester.activity_id().map(ActivityId::into_uuid))?,
            agent_id: required(requester.agent_id().map(AgentInstanceId::into_uuid))?,
            session_id: required(requester.session_id().map(AgentSessionId::into_uuid))?,
            participation_id,
            profile_id,
            participant_id: required(requester.participant_id())?,
        },
        source_offer_sha256: digest_hex(&receipt.assignment_payload_fingerprint()),
        observed_at_ms: receipt.observed_at().get(),
    })
}

fn digest_hex(digest: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

impl BackendState {
    async fn assignment_source_proof(
        &self,
        scope: crate::wire::WorkspaceQueryScope,
        node_scope: NodeTeamBindingScope,
        activity_id: Uuid,
        target_agent_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<SourceProof, ApiError> {
        let authority = self
            .coordinator_authority
            .clone()
            .ok_or(ApiError::CoordinatorUnavailable)?;
        let permit = self
            .forge_query_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ApiError::Unavailable)?;
        authority
            .node_assignment_admission_observation(
                scope,
                activity_id,
                target_agent_id,
                principal,
                node_scope,
                permit,
            )
            .await
            .map_err(map_authority_error)
    }
}

fn map_authority_error(error: CoordinatorAuthorityError) -> ApiError {
    match error {
        CoordinatorAuthorityError::Denied => ApiError::Runtime(RuntimeError::Forbidden),
        CoordinatorAuthorityError::Stale => ApiError::Runtime(RuntimeError::Conflict),
        CoordinatorAuthorityError::Unavailable => ApiError::CoordinatorUnavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{
        WorkspaceQueryScope,
        node_assignment_admission::{NodeAssignmentSourceFencesWire, NodeAssignmentTargetWire},
    };
    use forge_domain::DirectTokenId;
    use serde_json::json;

    fn request(relative_root: &str) -> NodeAssignmentAdmissionRequest {
        let workspace = Uuid::from_u128(1);
        let location = Uuid::from_u128(2);
        let activity = Uuid::from_u128(3);
        let assignment = Uuid::from_u128(4);
        let coordinator_session = Uuid::from_u128(5);
        let coordinator_agent = Uuid::from_u128(6);
        let target_session = Uuid::from_u128(7);
        let target_agent = Uuid::from_u128(8);
        let goal = Uuid::from_u128(9);
        let node = Uuid::from_u128(10);
        let token = Uuid::from_u128(11);
        let payload = serde_json::to_vec(&json!({
            "scope": {
                "workspace_id": workspace,
                "workspace_revision": 1,
                "locator": {
                    "location_id": location,
                    "expected_revision": 1,
                    "relative_root": relative_root
                }
            },
            "activity_id": activity,
            "assignment_id": assignment,
            "observed_at_ms": 12,
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
                "expected_goal_created_at_ms": 13,
                "dag_revision": 1,
                "expected_head_revision": 1,
                "node_id": node,
                "expected_node_revision": 1,
                "credit_budget": null
            }
        }))
        .unwrap_or_default();
        let digest: [u8; 32] = Sha256::digest(&payload).into();
        NodeAssignmentAdmissionRequest {
            scope: WorkspaceQueryScope {
                workspace_id: workspace,
                location_id: location,
                location_revision: 1,
                workspace_revision: 1,
                locator: format!("hf1:{location}:1:Lg"),
            },
            activity_id: activity,
            coordinator_session_id: coordinator_session,
            coordinator_agent_id: coordinator_agent,
            coordinator_revision: 1,
            assignment_id: assignment,
            source_fences: NodeAssignmentSourceFencesWire {
                goal_id: goal,
                goal_created_at_ms: 13,
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
        }
    }

    #[test]
    fn inner_offer_locator_must_equal_the_verified_outer_locator() {
        let principal =
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(11)));
        let exact = request(".");
        assert!(validate_request(&exact.scope.workspace_id.to_string(), &exact, principal).is_ok());
        let hostile = request("child");
        assert!(matches!(
            validate_request(&hostile.scope.workspace_id.to_string(), &hostile, principal),
            Err(ApiError::InvalidIdentity)
        ));
    }

    fn digest_hex(digest: &[u8; 32]) -> String {
        use std::fmt::Write as _;
        digest
            .iter()
            .fold(String::with_capacity(64), |mut result, byte| {
                let _ = write!(result, "{byte:02x}");
                result
            })
    }
}
