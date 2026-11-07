//! Strict V2 Team-expansion approval contract with an explicit Workspace hold
//! target for every approved ordinal.

use heterarchy_orchestration_runtime::leadership::member_agent_id;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::team_expansion::{
    TeamExpansionActorReceiptWire, TeamExpansionActorWire, TeamExpansionDecisionAction,
    TeamExpansionDecisionReceiptWire, TeamExpansionDecisionStateWire,
};

/// One existing, unassigned Workspace Profile selected for an approved slot.
/// New Profile provisioning remains behind its own source-authorized flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionApprovalTargetV2Wire {
    pub profile_id: Uuid,
    pub participation_id: Uuid,
}

/// Exact target reserved for one zero-based ordinal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionApprovalSlotV2Wire {
    pub ordinal: u8,
    pub target: TeamExpansionApprovalTargetV2Wire,
}

/// V2 approval intent. The separate endpoint preserves the strict V1 request
/// shape; an approval includes one existing Workspace Profile per slot.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionDecisionV2Request {
    pub actor: TeamExpansionActorWire,
    pub command: TeamExpansionDecisionAction,
    pub slot_plan: Vec<TeamExpansionApprovalSlotV2Wire>,
}

/// Caller attempting exact readback. `intent` remains the original request;
/// a current Orchestrator can be the separate reader after Session changes.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionDecisionV2ReadbackRequest {
    pub intent: TeamExpansionDecisionV2Request,
    pub current_actor: TeamExpansionActorWire,
}

/// Durable V2 decision together with its frozen ordinal target plan.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionDecisionV2ReceiptWire {
    pub decision: TeamExpansionDecisionReceiptWire,
    pub slot_plan: Vec<TeamExpansionApprovalSlotV2Wire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionDecisionV2Response {
    pub request: TeamExpansionDecisionV2Request,
    pub receipt: TeamExpansionDecisionV2ReceiptWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionDecisionV2ReadbackResponse {
    pub request: TeamExpansionDecisionV2ReadbackRequest,
    pub result: Option<TeamExpansionDecisionV2ReceiptWire>,
}

#[must_use]
pub fn valid_decision_v2_request(request: &TeamExpansionDecisionV2Request) -> bool {
    if !valid_actor(&request.actor) || !valid_decision_command(&request.command) {
        return false;
    }
    let mut profiles = Vec::with_capacity(request.slot_plan.len());
    let mut participations = Vec::with_capacity(request.slot_plan.len());
    match request.command.decision {
        super::team_expansion::TeamExpansionDecisionWire::Approve { approved_count } => {
            if request.slot_plan.len() != usize::from(approved_count) {
                return false;
            }
            request.slot_plan.iter().enumerate().all(|(index, slot)| {
                let Ok(expected_ordinal) = u8::try_from(index) else {
                    return false;
                };
                if slot.ordinal != expected_ordinal
                    || slot.target.profile_id.is_nil()
                    || slot.target.participation_id.is_nil()
                    || profiles.contains(&slot.target.profile_id)
                    || participations.contains(&slot.target.participation_id)
                {
                    return false;
                }
                profiles.push(slot.target.profile_id);
                participations.push(slot.target.participation_id);
                true
            })
        }
        super::team_expansion::TeamExpansionDecisionWire::Reject => request.slot_plan.is_empty(),
    }
}

#[must_use]
pub fn valid_decision_v2_readback_request(
    request: &TeamExpansionDecisionV2ReadbackRequest,
) -> bool {
    valid_decision_v2_request(&request.intent)
        && valid_actor(&request.current_actor)
        && request.current_actor.scope.workspace_id == request.intent.actor.scope.workspace_id
}

#[must_use]
pub fn valid_decision_v2_response(
    response: &TeamExpansionDecisionV2Response,
    request: &TeamExpansionDecisionV2Request,
) -> bool {
    &response.request == request
        && response.slot_plan_matches_request(request)
        && valid_decision_v2_receipt(&response.receipt, request)
}

#[must_use]
pub fn valid_decision_v2_readback_response(
    response: &TeamExpansionDecisionV2ReadbackResponse,
    request: &TeamExpansionDecisionV2ReadbackRequest,
) -> bool {
    &response.request == request
        && response
            .result
            .as_ref()
            .is_none_or(|receipt| valid_decision_v2_receipt(receipt, &request.intent))
}

impl TeamExpansionDecisionV2Response {
    fn slot_plan_matches_request(&self, request: &TeamExpansionDecisionV2Request) -> bool {
        self.receipt.slot_plan == request.slot_plan
    }
}

fn valid_decision_v2_receipt(
    result: &TeamExpansionDecisionV2ReceiptWire,
    request: &TeamExpansionDecisionV2Request,
) -> bool {
    let receipt = &result.decision;
    let command = &request.command;
    receipt.decision_id == command.decision_id
        && receipt.request_id == command.request_id
        && receipt.workspace_id == request.actor.scope.workspace_id
        && receipt.team_id == command.team_id
        && receipt.expected_team_revision == command.expected_team_revision
        && receipt.capacity_revision == command.expected_capacity_revision
        && receipt.leadership_revision == command.expected_leadership_revision
        && receipt.actor.activity_id == request.actor.activity_id
        && receipt.actor.agent_id
            == member_agent_id(
                request.actor.scope.workspace_id,
                request.actor.activity_id,
                request.actor.session_id,
            )
            .into_uuid()
        && receipt.actor.session_id == request.actor.session_id
        && valid_actor_receipt(&receipt.actor)
        && receipt.recorded_at_ms >= 0
        && result.slot_plan == request.slot_plan
        && match command.decision {
            super::team_expansion::TeamExpansionDecisionWire::Reject => {
                receipt.state == TeamExpansionDecisionStateWire::Rejected
                    && receipt.approved_count == 0
                    && receipt.cooldown_started_at_ms.is_none()
                    && receipt.cooldown_until_ms.is_none()
                    && receipt.finalized_by.is_none()
                    && receipt.finalized_by_leadership_revision.is_none()
            }
            super::team_expansion::TeamExpansionDecisionWire::Approve { approved_count } => {
                receipt.approved_count == approved_count && valid_approved_receipt(receipt)
            }
        }
}

fn valid_approved_receipt(receipt: &TeamExpansionDecisionReceiptWire) -> bool {
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
        TeamExpansionDecisionStateWire::Approved => {
            let (Some(start), Some(until), Some(finalizer), Some(revision)) = (
                receipt.cooldown_started_at_ms,
                receipt.cooldown_until_ms,
                receipt.finalized_by.as_ref(),
                receipt.finalized_by_leadership_revision,
            ) else {
                return false;
            };
            start >= receipt.recorded_at_ms
                && until.checked_sub(start) == Some(60 * 60 * 1_000)
                && revision > 0
                && valid_actor_receipt(finalizer)
        }
        TeamExpansionDecisionStateWire::Rejected => false,
    }
}

fn valid_decision_command(command: &TeamExpansionDecisionAction) -> bool {
    !command.team_id.is_nil()
        && !command.request_id.is_nil()
        && !command.decision_id.is_nil()
        && command.expected_team_revision > 0
        && i64::try_from(command.expected_team_revision).is_ok()
        && i64::try_from(command.expected_capacity_revision).is_ok()
        && command.expected_leadership_revision > 0
        && command.expected_requester_leadership_revision > 0
        && match command.decision {
            super::team_expansion::TeamExpansionDecisionWire::Approve { approved_count } => {
                (1..=8).contains(&approved_count)
            }
            super::team_expansion::TeamExpansionDecisionWire::Reject => true,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{
        WorkspaceQueryScope,
        team_expansion::{TeamExpansionDecisionStateWire, TeamExpansionDecisionWire},
    };

    fn actor(session_id: Uuid) -> TeamExpansionActorWire {
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

    fn request() -> TeamExpansionDecisionV2Request {
        TeamExpansionDecisionV2Request {
            actor: actor(Uuid::from_u128(6)),
            command: TeamExpansionDecisionAction {
                team_id: Uuid::from_u128(7),
                request_id: Uuid::from_u128(8),
                decision_id: Uuid::from_u128(9),
                expected_team_revision: 2,
                expected_capacity_revision: 1,
                expected_leadership_revision: 3,
                expected_requester_leadership_revision: 1,
                decision: TeamExpansionDecisionWire::Approve { approved_count: 2 },
            },
            slot_plan: vec![
                TeamExpansionApprovalSlotV2Wire {
                    ordinal: 0,
                    target: TeamExpansionApprovalTargetV2Wire {
                        profile_id: Uuid::from_u128(10),
                        participation_id: Uuid::from_u128(11),
                    },
                },
                TeamExpansionApprovalSlotV2Wire {
                    ordinal: 1,
                    target: TeamExpansionApprovalTargetV2Wire {
                        profile_id: Uuid::from_u128(12),
                        participation_id: Uuid::from_u128(13),
                    },
                },
            ],
        }
    }

    fn receipt(request: &TeamExpansionDecisionV2Request) -> TeamExpansionDecisionV2ReceiptWire {
        TeamExpansionDecisionV2ReceiptWire {
            decision: TeamExpansionDecisionReceiptWire {
                decision_id: request.command.decision_id,
                request_id: request.command.request_id,
                workspace_id: request.actor.scope.workspace_id,
                team_id: request.command.team_id,
                state: TeamExpansionDecisionStateWire::ApprovedPendingAnchor,
                approved_count: 2,
                cooldown_started_at_ms: None,
                cooldown_until_ms: None,
                finalized_by: None,
                finalized_by_leadership_revision: None,
                expected_team_revision: request.command.expected_team_revision,
                capacity_revision: request.command.expected_capacity_revision,
                leadership_revision: request.command.expected_leadership_revision,
                actor: TeamExpansionActorReceiptWire {
                    activity_id: request.actor.activity_id,
                    agent_id: member_agent_id(
                        request.actor.scope.workspace_id,
                        request.actor.activity_id,
                        request.actor.session_id,
                    )
                    .into_uuid(),
                    session_id: request.actor.session_id,
                    participation_id: Uuid::from_u128(15),
                    profile_id: Uuid::from_u128(16),
                    participant_id: Uuid::from_u128(17),
                    display_name: "Orchestrator".to_owned(),
                },
                recorded_at_ms: 100,
                replayed: false,
            },
            slot_plan: request.slot_plan.clone(),
        }
    }

    #[test]
    fn v2_requires_complete_ordered_distinct_profile_plan() {
        let request = request();
        assert!(valid_decision_v2_request(&request));

        let mut wrong_count = request.clone();
        wrong_count.slot_plan.pop();
        assert!(!valid_decision_v2_request(&wrong_count));

        let mut wrong_ordinal = request.clone();
        wrong_ordinal.slot_plan[1].ordinal = 0;
        assert!(!valid_decision_v2_request(&wrong_ordinal));

        let mut duplicate_profile = request;
        duplicate_profile.slot_plan[1].target.profile_id =
            duplicate_profile.slot_plan[0].target.profile_id;
        assert!(!valid_decision_v2_request(&duplicate_profile));
    }

    #[test]
    fn v2_reject_has_no_slot_plan_and_readback_preserves_current_reader() {
        let mut request = request();
        request.command.decision = TeamExpansionDecisionWire::Reject;
        request.slot_plan.clear();
        assert!(valid_decision_v2_request(&request));

        let readback = TeamExpansionDecisionV2ReadbackRequest {
            intent: request,
            current_actor: actor(Uuid::from_u128(18)),
        };
        assert!(valid_decision_v2_readback_request(&readback));
        let mut foreign = readback.clone();
        foreign.current_actor.scope.workspace_id = Uuid::from_u128(19);
        assert!(!valid_decision_v2_readback_request(&foreign));
    }

    #[test]
    fn v2_response_rejects_changed_plan_even_when_decision_matches() {
        let request = request();
        let response = TeamExpansionDecisionV2Response {
            request: request.clone(),
            receipt: receipt(&request),
        };
        assert!(valid_decision_v2_response(&response, &request));

        let mut changed = response;
        changed.receipt.slot_plan[0].target.profile_id = Uuid::from_u128(20);
        assert!(!valid_decision_v2_response(&changed, &request));
    }

    #[test]
    fn v2_response_rejects_wrong_non_nil_actor_agent_identity() {
        let request = request();
        let mut response = TeamExpansionDecisionV2Response {
            request: request.clone(),
            receipt: receipt(&request),
        };
        response.receipt.decision.actor.agent_id = Uuid::from_u128(21);
        assert!(!valid_decision_v2_response(&response, &request));
    }
}
