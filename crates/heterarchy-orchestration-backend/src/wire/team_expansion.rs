//! Strict transport values for Team-specific cap and Orchestrator expansion APIs.

use forge_domain::TimestampMillis;
use heterarchy_orchestration_operations::{
    TeamCapacityReceipt, TeamCapacitySnapshot, TeamExpansionActor, TeamExpansionDecisionReceipt,
    TeamExpansionDecisionState, TeamExpansionRequestReceipt, TeamExpansionSlotAssignmentReceipt,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::WorkspaceQueryScope;

/// Maximum JSON body accepted by this bounded command family.
pub const MAX_TEAM_EXPANSION_BODY_BYTES: usize = 16 * 1024;

/// Workspace-scoped authenticated Session selector.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionActorWire {
    pub scope: WorkspaceQueryScope,
    pub activity_id: Uuid,
    pub session_id: Uuid,
}

/// A target selected by the Workspace Orchestrator; Agent identity is derived
/// from the Activity and Session by the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionTargetWire {
    pub activity_id: Uuid,
    pub session_id: Uuid,
}

/// Read explicit configured capacity; `snapshot: null` means legacy unknown.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamCapacityQueryRequest {
    pub actor: TeamExpansionActorWire,
    pub team_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamCapacitySnapshotWire {
    pub workspace_id: Uuid,
    pub team_id: Uuid,
    pub max_members: u8,
    pub revision: u64,
    pub recorded_at_ms: i64,
    pub actor: TeamExpansionActorReceiptWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamCapacityQueryResponse {
    pub actor: TeamExpansionActorWire,
    pub team_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub snapshot: Option<TeamCapacitySnapshotWire>,
}

/// Change an explicit per-Team cap. No user-supplied timestamp is accepted.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamCapacitySetAction {
    pub team_id: Uuid,
    pub request_id: Uuid,
    pub expected_team_revision: u64,
    pub expected_capacity_revision: u64,
    /// Workspace Orchestrator epoch observed when this exact setting was issued.
    pub expected_leadership_revision: i64,
    pub max_members: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionRequestAction {
    pub team_id: Uuid,
    pub request_id: Uuid,
    pub expected_team_revision: u64,
    pub expected_capacity_revision: u64,
    pub expected_leadership_revision: i64,
    pub requested_count: u8,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum TeamExpansionDecisionWire {
    Approve { approved_count: u8 },
    Reject,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionDecisionAction {
    pub team_id: Uuid,
    pub request_id: Uuid,
    pub decision_id: Uuid,
    pub expected_team_revision: u64,
    pub expected_capacity_revision: u64,
    pub expected_leadership_revision: i64,
    pub expected_requester_leadership_revision: i64,
    pub decision: TeamExpansionDecisionWire,
}

/// Finalize or recover only the cooldown anchor for an immutable approval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionFinalizeAnchorAction {
    pub request_id: Uuid,
    pub decision_id: Uuid,
    pub expected_leadership_revision: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionSlotAssignmentAction {
    pub team_id: Uuid,
    pub approval_id: Uuid,
    pub ordinal: u8,
    pub request_id: Uuid,
    pub expected_team_revision: u64,
    pub expected_capacity_revision: u64,
    pub expected_leadership_revision: i64,
    pub target: TeamExpansionTargetWire,
}

/// One exact expansion mutation. The same immutable intent is retained for
/// unknown-outcome readback; clients never invent a replacement request ID.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum TeamExpansionMutationActionWire {
    SetCapacity {
        command: TeamCapacitySetAction,
    },
    Request {
        command: TeamExpansionRequestAction,
    },
    Decide {
        command: TeamExpansionDecisionAction,
    },
    FinalizeAnchor {
        command: TeamExpansionFinalizeAnchorAction,
    },
    AssignSlot {
        command: TeamExpansionSlotAssignmentAction,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionMutationRequest {
    pub actor: TeamExpansionActorWire,
    pub action: TeamExpansionMutationActionWire,
}

pub type TeamExpansionReadbackIntent = TeamExpansionMutationRequest;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionReadbackRequest {
    pub intent: TeamExpansionReadbackIntent,
    pub current_actor: TeamExpansionActorWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionActorReceiptWire {
    pub activity_id: Uuid,
    pub agent_id: Uuid,
    pub session_id: Uuid,
    pub participation_id: Uuid,
    pub profile_id: Uuid,
    pub participant_id: Uuid,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionRequestReceiptWire {
    pub request_id: Uuid,
    pub workspace_id: Uuid,
    pub team_id: Uuid,
    pub expected_team_revision: u64,
    pub capacity_revision: u64,
    pub requester_leadership_revision: i64,
    pub requested_count: u8,
    pub reason: String,
    pub actor: TeamExpansionActorReceiptWire,
    pub recorded_at_ms: i64,
    pub replayed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamExpansionDecisionStateWire {
    ApprovedPendingAnchor,
    Approved,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionDecisionReceiptWire {
    pub decision_id: Uuid,
    pub request_id: Uuid,
    pub workspace_id: Uuid,
    pub team_id: Uuid,
    pub state: TeamExpansionDecisionStateWire,
    pub approved_count: u8,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub cooldown_started_at_ms: Option<i64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub cooldown_until_ms: Option<i64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub finalized_by: Option<TeamExpansionActorReceiptWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub finalized_by_leadership_revision: Option<i64>,
    pub expected_team_revision: u64,
    pub capacity_revision: u64,
    pub leadership_revision: i64,
    pub actor: TeamExpansionActorReceiptWire,
    pub recorded_at_ms: i64,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionSlotAssignmentReceiptWire {
    pub request_id: Uuid,
    pub approval_id: Uuid,
    pub ordinal: u8,
    pub workspace_id: Uuid,
    pub team_id: Uuid,
    pub participation_id: Uuid,
    pub profile_id: Uuid,
    pub participant_id: Uuid,
    pub target: TeamExpansionActorReceiptWire,
    pub actor: TeamExpansionActorReceiptWire,
    pub recorded_at_ms: i64,
    pub replayed: bool,
}

/// Exact result corresponding to one original expansion mutation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum TeamExpansionMutationResultWire {
    CapacitySet {
        receipt: Box<TeamCapacityReceiptWire>,
    },
    Request {
        receipt: Box<TeamExpansionRequestReceiptWire>,
    },
    Decision {
        receipt: Box<TeamExpansionDecisionReceiptWire>,
    },
    SlotAssignment {
        receipt: Box<TeamExpansionSlotAssignmentReceiptWire>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamCapacityReceiptWire {
    pub request_id: Uuid,
    pub expected_team_revision: u64,
    pub result_team_revision: u64,
    pub expected_capacity_revision: u64,
    /// Original issuer epoch, preserved on exact readback even if a new
    /// Orchestrator is currently authorized to inspect the receipt.
    pub leadership_revision: i64,
    pub snapshot: TeamCapacitySnapshotWire,
    pub actor: TeamExpansionActorReceiptWire,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionMutationResponse {
    pub request: TeamExpansionMutationRequest,
    pub result: TeamExpansionMutationResultWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionReadbackResponse {
    pub request: TeamExpansionReadbackRequest,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub result: Option<TeamExpansionMutationResultWire>,
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

fn actor_receipt(actor: &TeamExpansionActor) -> TeamExpansionActorReceiptWire {
    TeamExpansionActorReceiptWire {
        activity_id: actor.activity_id().into_uuid(),
        agent_id: actor.agent_id().into_uuid(),
        session_id: actor.session_id().into_uuid(),
        participation_id: actor.participation_id().into_uuid(),
        profile_id: actor.profile_id(),
        participant_id: actor.participant_id(),
        display_name: actor.display_name().to_owned(),
    }
}

impl From<&TeamCapacitySnapshot> for TeamCapacitySnapshotWire {
    fn from(snapshot: &TeamCapacitySnapshot) -> Self {
        Self {
            workspace_id: snapshot.workspace_id().into_uuid(),
            team_id: snapshot.team_id(),
            max_members: snapshot.max_members().get(),
            revision: snapshot.revision(),
            recorded_at_ms: snapshot.recorded_at().get(),
            actor: actor_receipt(snapshot.actor()),
        }
    }
}

impl From<&TeamCapacityReceipt> for TeamCapacityReceiptWire {
    fn from(receipt: &TeamCapacityReceipt) -> Self {
        Self {
            request_id: receipt.request_id(),
            expected_team_revision: receipt.expected_team_revision(),
            result_team_revision: receipt.result_team_revision(),
            expected_capacity_revision: receipt.expected_capacity_revision(),
            leadership_revision: receipt.leadership_revision(),
            snapshot: TeamCapacitySnapshotWire::from(receipt.snapshot()),
            actor: actor_receipt(receipt.actor()),
            replayed: receipt.replayed(),
        }
    }
}

impl From<&TeamExpansionRequestReceipt> for TeamExpansionRequestReceiptWire {
    fn from(receipt: &TeamExpansionRequestReceipt) -> Self {
        Self {
            request_id: receipt.request_id(),
            workspace_id: receipt.workspace_id().into_uuid(),
            team_id: receipt.team_id(),
            expected_team_revision: receipt.expected_team_revision(),
            capacity_revision: receipt.capacity_revision(),
            requester_leadership_revision: receipt.requester_leadership_revision(),
            requested_count: receipt.requested_count(),
            reason: receipt.reason().to_owned(),
            actor: actor_receipt(receipt.actor()),
            recorded_at_ms: receipt.recorded_at().get(),
            replayed: receipt.replayed(),
        }
    }
}

impl From<&TeamExpansionDecisionReceipt> for TeamExpansionDecisionReceiptWire {
    fn from(receipt: &TeamExpansionDecisionReceipt) -> Self {
        Self {
            decision_id: receipt.decision_id(),
            request_id: receipt.request_id(),
            workspace_id: receipt.workspace_id().into_uuid(),
            team_id: receipt.team_id(),
            state: match receipt.state() {
                TeamExpansionDecisionState::Approved => TeamExpansionDecisionStateWire::Approved,
                TeamExpansionDecisionState::ApprovedPendingAnchor => {
                    TeamExpansionDecisionStateWire::ApprovedPendingAnchor
                }
                TeamExpansionDecisionState::Rejected => TeamExpansionDecisionStateWire::Rejected,
            },
            approved_count: receipt.approved_count(),
            cooldown_started_at_ms: receipt.cooldown_started_at().map(TimestampMillis::get),
            cooldown_until_ms: receipt.cooldown_until().map(TimestampMillis::get),
            finalized_by: receipt.finalized_by().map(actor_receipt),
            finalized_by_leadership_revision: receipt.finalized_by_leadership_revision(),
            expected_team_revision: receipt.expected_team_revision(),
            capacity_revision: receipt.capacity_revision(),
            leadership_revision: receipt.leadership_revision(),
            actor: actor_receipt(receipt.actor()),
            recorded_at_ms: receipt.recorded_at().get(),
            replayed: receipt.replayed(),
        }
    }
}

impl From<&TeamExpansionSlotAssignmentReceipt> for TeamExpansionSlotAssignmentReceiptWire {
    fn from(receipt: &TeamExpansionSlotAssignmentReceipt) -> Self {
        Self {
            request_id: receipt.request_id(),
            approval_id: receipt.slot().approval_id(),
            ordinal: receipt.slot().ordinal(),
            workspace_id: receipt.workspace_id().into_uuid(),
            team_id: receipt.team_id(),
            participation_id: receipt.participation_id().into_uuid(),
            profile_id: receipt.profile_id(),
            participant_id: receipt.participant_id(),
            target: actor_receipt(receipt.target()),
            actor: actor_receipt(receipt.actor()),
            recorded_at_ms: receipt.recorded_at().get(),
            replayed: receipt.replayed(),
        }
    }
}

fn valid_actor(actor: &TeamExpansionActorWire) -> bool {
    !actor.scope.workspace_id.is_nil()
        && !actor.scope.location_id.is_nil()
        && actor.scope.location_revision > 0
        && actor.scope.workspace_revision > 0
        && !actor.activity_id.is_nil()
        && !actor.session_id.is_nil()
}

fn valid_revision_pair(team_revision: u64, capacity_revision: u64) -> bool {
    team_revision > 0
        && i64::try_from(team_revision).is_ok()
        && i64::try_from(capacity_revision).is_ok()
}

#[must_use]
pub fn valid_mutation_request(request: &TeamExpansionMutationRequest) -> bool {
    if !valid_actor(&request.actor) {
        return false;
    }
    match &request.action {
        TeamExpansionMutationActionWire::SetCapacity { command } => {
            !command.team_id.is_nil()
                && !command.request_id.is_nil()
                && valid_revision_pair(
                    command.expected_team_revision,
                    command.expected_capacity_revision,
                )
                && command.expected_leadership_revision > 0
                && command.max_members <= 8
        }
        TeamExpansionMutationActionWire::Request { command } => {
            !command.team_id.is_nil()
                && !command.request_id.is_nil()
                && valid_revision_pair(
                    command.expected_team_revision,
                    command.expected_capacity_revision,
                )
                && command.expected_leadership_revision > 0
                && (1..=8).contains(&command.requested_count)
                && !command.reason.trim().is_empty()
                && command.reason.len() <= 2048
        }
        TeamExpansionMutationActionWire::Decide { command } => {
            !command.team_id.is_nil()
                && !command.request_id.is_nil()
                && !command.decision_id.is_nil()
                && valid_revision_pair(
                    command.expected_team_revision,
                    command.expected_capacity_revision,
                )
                && command.expected_leadership_revision > 0
                && command.expected_requester_leadership_revision > 0
                && matches!(
                    command.decision,
                    TeamExpansionDecisionWire::Reject
                        | TeamExpansionDecisionWire::Approve {
                            approved_count: 1..=8
                        }
                )
        }
        TeamExpansionMutationActionWire::FinalizeAnchor { command } => {
            !command.request_id.is_nil()
                && !command.decision_id.is_nil()
                && command.expected_leadership_revision > 0
        }
        TeamExpansionMutationActionWire::AssignSlot { command } => {
            !command.team_id.is_nil()
                && !command.request_id.is_nil()
                && !command.approval_id.is_nil()
                && command.ordinal <= 7
                && valid_revision_pair(
                    command.expected_team_revision,
                    command.expected_capacity_revision,
                )
                && command.expected_leadership_revision > 0
                && !command.target.activity_id.is_nil()
                && !command.target.session_id.is_nil()
        }
    }
}

#[must_use]
pub fn valid_readback_request(request: &TeamExpansionReadbackRequest) -> bool {
    valid_mutation_request(&request.intent)
        && valid_actor(&request.current_actor)
        && request.current_actor.scope.workspace_id == request.intent.actor.scope.workspace_id
}

#[must_use]
pub fn valid_capacity_snapshot(snapshot: &TeamCapacitySnapshotWire) -> bool {
    !snapshot.workspace_id.is_nil()
        && !snapshot.team_id.is_nil()
        && snapshot.max_members <= 8
        && snapshot.revision > 0
        && snapshot.recorded_at_ms >= 0
}

fn actor_receipt_matches_selector(
    receipt: &TeamExpansionActorReceiptWire,
    selector: &TeamExpansionActorWire,
) -> bool {
    receipt.activity_id == selector.activity_id && receipt.session_id == selector.session_id
}

fn valid_actor_receipt(receipt: &TeamExpansionActorReceiptWire) -> bool {
    [
        receipt.activity_id,
        receipt.agent_id,
        receipt.session_id,
        receipt.participation_id,
        receipt.profile_id,
        receipt.participant_id,
    ]
    .iter()
    .all(|identity| !identity.is_nil())
        && !receipt.display_name.trim().is_empty()
        && receipt.display_name.len() <= 128
}

fn valid_decision_response(
    command: &TeamExpansionDecisionAction,
    receipt: &TeamExpansionDecisionReceiptWire,
    actor: &TeamExpansionActorWire,
) -> bool {
    receipt.decision_id == command.decision_id
        && receipt.request_id == command.request_id
        && receipt.workspace_id == actor.scope.workspace_id
        && receipt.team_id == command.team_id
        && receipt.expected_team_revision == command.expected_team_revision
        && receipt.leadership_revision == command.expected_leadership_revision
        && receipt.capacity_revision == command.expected_capacity_revision
        && actor_receipt_matches_selector(&receipt.actor, actor)
        && match command.decision {
            TeamExpansionDecisionWire::Reject => {
                receipt.state == TeamExpansionDecisionStateWire::Rejected
                    && receipt.approved_count == 0
                    && receipt.cooldown_started_at_ms.is_none()
                    && receipt.cooldown_until_ms.is_none()
                    && receipt.finalized_by.is_none()
                    && receipt.finalized_by_leadership_revision.is_none()
            }
            TeamExpansionDecisionWire::Approve { approved_count } => {
                receipt.approved_count == approved_count && valid_decision_anchor(receipt)
            }
        }
}

fn valid_finalize_response(
    command: &TeamExpansionFinalizeAnchorAction,
    receipt: &TeamExpansionDecisionReceiptWire,
    actor: &TeamExpansionActorWire,
) -> bool {
    receipt.request_id == command.request_id
        && receipt.decision_id == command.decision_id
        && receipt.workspace_id == actor.scope.workspace_id
        && receipt.state == TeamExpansionDecisionStateWire::Approved
        && receipt.approved_count > 0
        && valid_anchored_decision(receipt)
        && if receipt.replayed {
            // Exact replays retain the first Orchestrator's attribution,
            // even when a newly elected Orchestrator performs recovery.
            receipt
                .finalized_by
                .as_ref()
                .is_some_and(valid_actor_receipt)
        } else {
            receipt
                .finalized_by
                .as_ref()
                .is_some_and(|finalizer| actor_receipt_matches_selector(finalizer, actor))
                && receipt.finalized_by_leadership_revision
                    == Some(command.expected_leadership_revision)
        }
}

#[must_use]
pub fn valid_mutation_response(
    response: &TeamExpansionMutationResponse,
    request: &TeamExpansionMutationRequest,
) -> bool {
    if &response.request != request {
        return false;
    }
    match (&request.action, &response.result) {
        (
            TeamExpansionMutationActionWire::SetCapacity { command },
            TeamExpansionMutationResultWire::CapacitySet { receipt },
        ) => {
            receipt.request_id == command.request_id
                && receipt.expected_team_revision == command.expected_team_revision
                && receipt.result_team_revision == command.expected_team_revision.saturating_add(1)
                && receipt.expected_capacity_revision == command.expected_capacity_revision
                && receipt.leadership_revision == command.expected_leadership_revision
                && receipt.snapshot.workspace_id == request.actor.scope.workspace_id
                && receipt.snapshot.team_id == command.team_id
                && receipt.snapshot.max_members == command.max_members
                && receipt.snapshot.revision > 0
                && receipt.snapshot.revision == command.expected_capacity_revision.saturating_add(1)
                && actor_receipt_matches_selector(&receipt.actor, &request.actor)
                && actor_receipt_matches_selector(&receipt.snapshot.actor, &request.actor)
                && valid_capacity_snapshot(&receipt.snapshot)
        }
        (
            TeamExpansionMutationActionWire::Request { command },
            TeamExpansionMutationResultWire::Request { receipt },
        ) => {
            receipt.request_id == command.request_id
                && receipt.workspace_id == request.actor.scope.workspace_id
                && receipt.team_id == command.team_id
                && receipt.expected_team_revision == command.expected_team_revision
                && receipt.capacity_revision == command.expected_capacity_revision
                && receipt.requester_leadership_revision == command.expected_leadership_revision
                && receipt.requested_count == command.requested_count
                && receipt.reason == command.reason
                && actor_receipt_matches_selector(&receipt.actor, &request.actor)
        }
        (
            TeamExpansionMutationActionWire::Decide { command },
            TeamExpansionMutationResultWire::Decision { receipt },
        ) => valid_decision_response(command, receipt, &request.actor),
        (
            TeamExpansionMutationActionWire::FinalizeAnchor { command },
            TeamExpansionMutationResultWire::Decision { receipt },
        ) => valid_finalize_response(command, receipt, &request.actor),
        (
            TeamExpansionMutationActionWire::AssignSlot { command },
            TeamExpansionMutationResultWire::SlotAssignment { receipt },
        ) => {
            receipt.request_id == command.request_id
                && receipt.approval_id == command.approval_id
                && receipt.ordinal == command.ordinal
                && receipt.workspace_id == request.actor.scope.workspace_id
                && receipt.team_id == command.team_id
                && receipt.target.activity_id == command.target.activity_id
                && receipt.target.session_id == command.target.session_id
                && actor_receipt_matches_selector(&receipt.actor, &request.actor)
        }
        _ => false,
    }
}

fn valid_decision_anchor(receipt: &TeamExpansionDecisionReceiptWire) -> bool {
    if receipt.leadership_revision <= 0 {
        return false;
    }
    match receipt.state {
        TeamExpansionDecisionStateWire::ApprovedPendingAnchor => {
            receipt.cooldown_started_at_ms.is_none()
                && receipt.cooldown_until_ms.is_none()
                && receipt.finalized_by.is_none()
                && receipt.finalized_by_leadership_revision.is_none()
        }
        TeamExpansionDecisionStateWire::Approved => valid_anchored_decision(receipt),
        TeamExpansionDecisionStateWire::Rejected => false,
    }
}

fn valid_anchored_decision(receipt: &TeamExpansionDecisionReceiptWire) -> bool {
    let (Some(started), Some(until), Some(finalized_by), Some(revision)) = (
        receipt.cooldown_started_at_ms,
        receipt.cooldown_until_ms,
        receipt.finalized_by.as_ref(),
        receipt.finalized_by_leadership_revision,
    ) else {
        return false;
    };
    started >= 0
        && started >= receipt.recorded_at_ms
        && until.checked_sub(started) == Some(60 * 60 * 1_000)
        && receipt.leadership_revision > 0
        && revision > 0
        && valid_actor_receipt(&receipt.actor)
        && valid_actor_receipt(finalized_by)
}

#[must_use]
pub fn valid_readback_response(
    response: &TeamExpansionReadbackResponse,
    request: &TeamExpansionReadbackRequest,
) -> bool {
    if &response.request != request {
        return false;
    }
    response.result.as_ref().is_none_or(|result| {
        if let TeamExpansionMutationActionWire::FinalizeAnchor { command } = &request.intent.action
        {
            let TeamExpansionMutationResultWire::Decision { receipt } = result else {
                return false;
            };
            return receipt.decision_id == command.decision_id
                && receipt.request_id == command.request_id
                && receipt.workspace_id == request.intent.actor.scope.workspace_id
                && receipt.state == TeamExpansionDecisionStateWire::Approved
                && receipt.approved_count > 0
                && !receipt.replayed
                && valid_anchored_decision(receipt)
                && receipt.finalized_by.as_ref().is_some_and(|actor| {
                    actor_receipt_matches_selector(actor, &request.intent.actor)
                })
                && receipt.finalized_by_leadership_revision
                    == Some(command.expected_leadership_revision);
        }
        let mutation_response = TeamExpansionMutationResponse {
            request: request.intent.clone(),
            result: result.clone(),
        };
        valid_mutation_response(&mutation_response, &request.intent)
    })
}

#[must_use]
pub fn capacity_query_request_is_valid(request: &TeamCapacityQueryRequest) -> bool {
    valid_actor(&request.actor) && !request.team_id.is_nil()
}

#[must_use]
pub fn capacity_query_response_is_valid(
    response: &TeamCapacityQueryResponse,
    request: &TeamCapacityQueryRequest,
) -> bool {
    response.actor == request.actor
        && response.team_id == request.team_id
        && response
            .snapshot
            .as_ref()
            .is_none_or(valid_capacity_snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn selector(session_id: Uuid) -> TeamExpansionActorWire {
        TeamExpansionActorWire {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            activity_id: Uuid::from_u128(5),
            session_id,
        }
    }

    fn actor_receipt(session_id: Uuid) -> TeamExpansionActorReceiptWire {
        TeamExpansionActorReceiptWire {
            activity_id: Uuid::from_u128(5),
            agent_id: Uuid::from_u128(6),
            session_id,
            participation_id: Uuid::from_u128(7),
            profile_id: Uuid::from_u128(8),
            participant_id: Uuid::from_u128(9),
            display_name: "orchestrator".to_owned(),
        }
    }

    #[test]
    fn capacity_query_requires_explicit_nullable_snapshot_field()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = TeamCapacityQueryRequest {
            actor: selector(Uuid::from_u128(10)),
            team_id: Uuid::from_u128(11),
        };
        let response = TeamCapacityQueryResponse {
            actor: request.actor.clone(),
            team_id: request.team_id,
            snapshot: None,
        };
        let baseline = serde_json::to_value(response)?;
        assert!(serde_json::from_value::<TeamCapacityQueryResponse>(baseline.clone()).is_ok());
        let mut missing = baseline;
        missing
            .as_object_mut()
            .ok_or("capacity response must be an object")?
            .remove("snapshot");
        assert!(serde_json::from_value::<TeamCapacityQueryResponse>(missing).is_err());
        Ok(())
    }

    #[test]
    fn approved_pending_anchor_is_a_durable_typed_state() {
        let request = TeamExpansionMutationRequest {
            actor: selector(Uuid::from_u128(10)),
            action: TeamExpansionMutationActionWire::Decide {
                command: TeamExpansionDecisionAction {
                    team_id: Uuid::from_u128(11),
                    request_id: Uuid::from_u128(12),
                    decision_id: Uuid::from_u128(13),
                    expected_team_revision: 2,
                    expected_capacity_revision: 3,
                    expected_leadership_revision: 4,
                    expected_requester_leadership_revision: 5,
                    decision: TeamExpansionDecisionWire::Approve { approved_count: 1 },
                },
            },
        };
        let response = TeamExpansionMutationResponse {
            request: request.clone(),
            result: TeamExpansionMutationResultWire::Decision {
                receipt: Box::new(TeamExpansionDecisionReceiptWire {
                    decision_id: Uuid::from_u128(13),
                    request_id: Uuid::from_u128(12),
                    workspace_id: request.actor.scope.workspace_id,
                    team_id: Uuid::from_u128(11),
                    state: TeamExpansionDecisionStateWire::ApprovedPendingAnchor,
                    approved_count: 1,
                    cooldown_started_at_ms: None,
                    cooldown_until_ms: None,
                    finalized_by: None,
                    finalized_by_leadership_revision: None,
                    expected_team_revision: 2,
                    capacity_revision: 3,
                    leadership_revision: 4,
                    actor: actor_receipt(Uuid::from_u128(10)),
                    recorded_at_ms: 100,
                    replayed: false,
                }),
            },
        };
        assert!(valid_mutation_response(&response, &request));
    }

    #[test]
    fn finalize_anchor_readback_correlates_decision_but_keeps_original_actor()
    -> Result<(), Box<dyn std::error::Error>> {
        let original_actor = selector(Uuid::from_u128(10));
        let current_actor = selector(Uuid::from_u128(20));
        let intent = TeamExpansionMutationRequest {
            actor: current_actor.clone(),
            action: TeamExpansionMutationActionWire::FinalizeAnchor {
                command: TeamExpansionFinalizeAnchorAction {
                    request_id: Uuid::from_u128(12),
                    decision_id: Uuid::from_u128(13),
                    expected_leadership_revision: 6,
                },
            },
        };
        let request = TeamExpansionReadbackRequest {
            intent: intent.clone(),
            current_actor: current_actor.clone(),
        };
        let response = TeamExpansionReadbackResponse {
            request: request.clone(),
            result: Some(TeamExpansionMutationResultWire::Decision {
                receipt: Box::new(TeamExpansionDecisionReceiptWire {
                    decision_id: Uuid::from_u128(13),
                    request_id: Uuid::from_u128(12),
                    workspace_id: original_actor.scope.workspace_id,
                    team_id: Uuid::from_u128(11),
                    state: TeamExpansionDecisionStateWire::Approved,
                    approved_count: 1,
                    cooldown_started_at_ms: Some(102),
                    cooldown_until_ms: Some(102 + 60 * 60 * 1_000),
                    finalized_by: Some(actor_receipt(current_actor.session_id)),
                    finalized_by_leadership_revision: Some(6),
                    expected_team_revision: 2,
                    capacity_revision: 4,
                    leadership_revision: 4,
                    actor: actor_receipt(original_actor.session_id),
                    recorded_at_ms: 101,
                    replayed: false,
                }),
            }),
        };
        assert!(valid_readback_response(&response, &request));

        let mut regressed_anchor = response.clone();
        let Some(TeamExpansionMutationResultWire::Decision { receipt }) =
            regressed_anchor.result.as_mut()
        else {
            return Err("finalize anchor readback receipt".into());
        };
        receipt.cooldown_started_at_ms = Some(100);
        receipt.cooldown_until_ms = Some(100 + 60 * 60 * 1_000);
        assert!(!valid_readback_response(&regressed_anchor, &request));

        let mut replayed_readback = response;
        let Some(TeamExpansionMutationResultWire::Decision { receipt }) =
            replayed_readback.result.as_mut()
        else {
            return Err("finalize anchor readback receipt".into());
        };
        receipt.replayed = true;
        assert!(!valid_readback_response(&replayed_readback, &request));
        Ok(())
    }
    fn actor() -> TeamExpansionActorWire {
        TeamExpansionActorWire {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            activity_id: Uuid::from_u128(5),
            session_id: Uuid::from_u128(6),
        }
    }

    #[test]
    fn all_expansion_actions_round_trip_with_closed_nested_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let actions = [
            TeamExpansionMutationActionWire::SetCapacity {
                command: TeamCapacitySetAction {
                    team_id: Uuid::from_u128(7),
                    request_id: Uuid::from_u128(8),
                    expected_team_revision: 1,
                    expected_capacity_revision: 1,
                    expected_leadership_revision: 3,
                    max_members: 5,
                },
            },
            TeamExpansionMutationActionWire::Request {
                command: TeamExpansionRequestAction {
                    team_id: Uuid::from_u128(7),
                    request_id: Uuid::from_u128(9),
                    expected_team_revision: 1,
                    expected_capacity_revision: 1,
                    expected_leadership_revision: 2,
                    requested_count: 1,
                    reason: "explicit request".to_owned(),
                },
            },
            TeamExpansionMutationActionWire::Decide {
                command: TeamExpansionDecisionAction {
                    team_id: Uuid::from_u128(7),
                    request_id: Uuid::from_u128(9),
                    decision_id: Uuid::from_u128(10),
                    expected_team_revision: 1,
                    expected_capacity_revision: 1,
                    expected_leadership_revision: 3,
                    expected_requester_leadership_revision: 2,
                    decision: TeamExpansionDecisionWire::Approve { approved_count: 1 },
                },
            },
            TeamExpansionMutationActionWire::FinalizeAnchor {
                command: TeamExpansionFinalizeAnchorAction {
                    request_id: Uuid::from_u128(9),
                    decision_id: Uuid::from_u128(10),
                    expected_leadership_revision: 3,
                },
            },
            TeamExpansionMutationActionWire::AssignSlot {
                command: TeamExpansionSlotAssignmentAction {
                    team_id: Uuid::from_u128(7),
                    approval_id: Uuid::from_u128(10),
                    ordinal: 1,
                    request_id: Uuid::from_u128(11),
                    expected_team_revision: 1,
                    expected_capacity_revision: 1,
                    expected_leadership_revision: 3,
                    target: TeamExpansionTargetWire {
                        activity_id: Uuid::from_u128(12),
                        session_id: Uuid::from_u128(13),
                    },
                },
            },
        ];

        for action in actions {
            let request = TeamExpansionMutationRequest {
                actor: actor(),
                action,
            };
            let encoded = serde_json::to_vec(&request)?;
            let decoded: TeamExpansionMutationRequest = serde_json::from_slice(&encoded)?;
            assert_eq!(decoded, request);
            assert!(valid_mutation_request(&decoded));
        }

        let request = TeamExpansionMutationRequest {
            actor: actor(),
            action: TeamExpansionMutationActionWire::Request {
                command: TeamExpansionRequestAction {
                    team_id: Uuid::from_u128(7),
                    request_id: Uuid::from_u128(8),
                    expected_team_revision: 1,
                    expected_capacity_revision: 1,
                    expected_leadership_revision: 2,
                    requested_count: 1,
                    reason: "explicit request".to_owned(),
                },
            },
        };
        let mut encoded = serde_json::to_value(&request)?;
        encoded["action"]["command"]["unknown"] = serde_json::json!(true);
        assert!(serde_json::from_value::<TeamExpansionMutationRequest>(encoded).is_err());
        Ok(())
    }

    #[test]
    fn slot_wire_accepts_zero_based_ordinals_and_defers_approval_count_to_owner() {
        let request = |ordinal| TeamExpansionMutationRequest {
            actor: actor(),
            action: TeamExpansionMutationActionWire::AssignSlot {
                command: TeamExpansionSlotAssignmentAction {
                    team_id: Uuid::from_u128(7),
                    approval_id: Uuid::from_u128(8),
                    ordinal,
                    request_id: Uuid::from_u128(9),
                    expected_team_revision: 1,
                    expected_capacity_revision: 1,
                    expected_leadership_revision: 3,
                    target: TeamExpansionTargetWire {
                        activity_id: Uuid::from_u128(10),
                        session_id: Uuid::from_u128(11),
                    },
                },
            },
        };

        assert!(valid_mutation_request(&request(0)));
        // A wire request has no approved_count to consult. The owner rejects
        // ordinal 1 when the referenced decision authorized only one slot.
        assert!(valid_mutation_request(&request(1)));
        assert!(!valid_mutation_request(&request(8)));
    }

    #[test]
    fn capacity_query_missing_snapshot_is_distinct_from_explicit_null()
    -> Result<(), Box<dyn std::error::Error>> {
        let response = TeamCapacityQueryResponse {
            actor: actor(),
            team_id: Uuid::from_u128(7),
            snapshot: None,
        };
        let baseline = serde_json::to_value(response)?;
        assert!(serde_json::from_value::<TeamCapacityQueryResponse>(baseline.clone()).is_ok());
        let mut missing = baseline;
        missing
            .as_object_mut()
            .ok_or("capacity response must serialize as an object")?
            .remove("snapshot");
        assert!(serde_json::from_value::<TeamCapacityQueryResponse>(missing).is_err());
        Ok(())
    }

    #[test]
    fn pending_anchor_is_typed_and_not_a_finalized_approval() {
        let request = TeamExpansionMutationRequest {
            actor: actor(),
            action: TeamExpansionMutationActionWire::Decide {
                command: TeamExpansionDecisionAction {
                    team_id: Uuid::from_u128(7),
                    request_id: Uuid::from_u128(8),
                    decision_id: Uuid::from_u128(9),
                    expected_team_revision: 1,
                    expected_capacity_revision: 1,
                    expected_leadership_revision: 3,
                    expected_requester_leadership_revision: 2,
                    decision: TeamExpansionDecisionWire::Approve { approved_count: 1 },
                },
            },
        };
        let receipt = TeamExpansionDecisionReceiptWire {
            decision_id: Uuid::from_u128(9),
            request_id: Uuid::from_u128(8),
            workspace_id: request.actor.scope.workspace_id,
            team_id: Uuid::from_u128(7),
            state: TeamExpansionDecisionStateWire::ApprovedPendingAnchor,
            approved_count: 1,
            cooldown_started_at_ms: None,
            cooldown_until_ms: None,
            finalized_by: None,
            finalized_by_leadership_revision: None,
            expected_team_revision: 1,
            capacity_revision: 1,
            leadership_revision: 3,
            actor: TeamExpansionActorReceiptWire {
                activity_id: request.actor.activity_id,
                agent_id: Uuid::from_u128(14),
                session_id: request.actor.session_id,
                participation_id: Uuid::from_u128(15),
                profile_id: Uuid::from_u128(16),
                participant_id: Uuid::from_u128(17),
                display_name: "orchestrator".to_owned(),
            },
            recorded_at_ms: 100,
            replayed: false,
        };
        let response = TeamExpansionMutationResponse {
            request: request.clone(),
            result: TeamExpansionMutationResultWire::Decision {
                receipt: Box::new(receipt),
            },
        };
        assert!(valid_mutation_response(&response, &request));
    }

    #[test]
    fn recovered_finalize_receipt_preserves_original_and_finalizer_attribution() {
        let original_decision_actor = selector(Uuid::from_u128(10));
        let finalizer_actor = selector(Uuid::from_u128(20));
        let request = TeamExpansionMutationRequest {
            actor: finalizer_actor.clone(),
            action: TeamExpansionMutationActionWire::FinalizeAnchor {
                command: TeamExpansionFinalizeAnchorAction {
                    request_id: Uuid::from_u128(8),
                    decision_id: Uuid::from_u128(9),
                    expected_leadership_revision: 6,
                },
            },
        };
        let receipt = TeamExpansionDecisionReceiptWire {
            decision_id: Uuid::from_u128(9),
            request_id: Uuid::from_u128(8),
            workspace_id: finalizer_actor.scope.workspace_id,
            team_id: Uuid::from_u128(7),
            state: TeamExpansionDecisionStateWire::Approved,
            approved_count: 1,
            cooldown_started_at_ms: Some(100),
            cooldown_until_ms: Some(100 + 60 * 60 * 1_000),
            finalized_by: Some(actor_receipt(finalizer_actor.session_id)),
            finalized_by_leadership_revision: Some(6),
            expected_team_revision: 1,
            capacity_revision: 2,
            leadership_revision: 3,
            actor: actor_receipt(original_decision_actor.session_id),
            recorded_at_ms: 100,
            replayed: false,
        };
        let mutation = TeamExpansionMutationResponse {
            request: request.clone(),
            result: TeamExpansionMutationResultWire::Decision {
                receipt: Box::new(receipt.clone()),
            },
        };
        assert!(valid_mutation_response(&mutation, &request));

        // A later read may use the original requester after the finalizer's
        // Session changes, while both historical actor identities remain exact.
        let readback = TeamExpansionReadbackRequest {
            intent: request,
            current_actor: original_decision_actor,
        };
        let response = TeamExpansionReadbackResponse {
            request: readback.clone(),
            result: Some(TeamExpansionMutationResultWire::Decision {
                receipt: Box::new(receipt),
            }),
        };
        assert!(valid_readback_response(&response, &readback));
    }

    #[test]
    fn replaced_orchestrator_replay_matches_original_finalizer_readback() {
        let original_finalizer = selector(Uuid::from_u128(10));
        let current_orchestrator = selector(Uuid::from_u128(20));
        let replay_request = TeamExpansionMutationRequest {
            actor: current_orchestrator.clone(),
            action: TeamExpansionMutationActionWire::FinalizeAnchor {
                command: TeamExpansionFinalizeAnchorAction {
                    request_id: Uuid::from_u128(8),
                    decision_id: Uuid::from_u128(9),
                    expected_leadership_revision: 6,
                },
            },
        };
        let mut replayed_receipt = TeamExpansionDecisionReceiptWire {
            decision_id: Uuid::from_u128(9),
            request_id: Uuid::from_u128(8),
            workspace_id: current_orchestrator.scope.workspace_id,
            team_id: Uuid::from_u128(7),
            state: TeamExpansionDecisionStateWire::Approved,
            approved_count: 1,
            cooldown_started_at_ms: Some(100),
            cooldown_until_ms: Some(100 + 60 * 60 * 1_000),
            finalized_by: Some(actor_receipt(original_finalizer.session_id)),
            finalized_by_leadership_revision: Some(3),
            expected_team_revision: 1,
            capacity_revision: 2,
            leadership_revision: 4,
            actor: actor_receipt(Uuid::from_u128(30)),
            recorded_at_ms: 100,
            replayed: true,
        };
        let replay_response = TeamExpansionMutationResponse {
            request: replay_request.clone(),
            result: TeamExpansionMutationResultWire::Decision {
                receipt: Box::new(replayed_receipt.clone()),
            },
        };
        assert!(valid_mutation_response(&replay_response, &replay_request));

        // A current O can read the same immutable decision while the readback
        // intent retains the original finalizer's actor and leadership epoch.
        let original_intent = TeamExpansionMutationRequest {
            actor: original_finalizer.clone(),
            action: TeamExpansionMutationActionWire::FinalizeAnchor {
                command: TeamExpansionFinalizeAnchorAction {
                    request_id: Uuid::from_u128(8),
                    decision_id: Uuid::from_u128(9),
                    expected_leadership_revision: 3,
                },
            },
        };
        let readback_request = TeamExpansionReadbackRequest {
            intent: original_intent,
            current_actor: current_orchestrator,
        };
        replayed_receipt.replayed = false;
        let readback_response = TeamExpansionReadbackResponse {
            request: readback_request.clone(),
            result: Some(TeamExpansionMutationResultWire::Decision {
                receipt: Box::new(replayed_receipt.clone()),
            }),
        };
        assert!(valid_readback_response(
            &readback_response,
            &readback_request
        ));

        // A substituted source actor cannot claim the stored finalizer receipt.
        let mut substituted = readback_request.clone();
        substituted.intent.actor.session_id = Uuid::from_u128(40);
        let substituted_response = TeamExpansionReadbackResponse {
            request: substituted.clone(),
            result: Some(TeamExpansionMutationResultWire::Decision {
                receipt: Box::new(replayed_receipt.clone()),
            }),
        };
        assert!(!valid_readback_response(
            &substituted_response,
            &substituted
        ));

        let fresh_wrong_attribution = TeamExpansionMutationResponse {
            request: replay_request.clone(),
            result: TeamExpansionMutationResultWire::Decision {
                receipt: Box::new(TeamExpansionDecisionReceiptWire {
                    finalized_by: Some(actor_receipt(original_finalizer.session_id)),
                    finalized_by_leadership_revision: Some(3),
                    replayed: false,
                    ..replayed_receipt
                }),
            },
        };
        assert!(!valid_mutation_response(
            &fresh_wrong_attribution,
            &replay_request
        ));
    }
}
