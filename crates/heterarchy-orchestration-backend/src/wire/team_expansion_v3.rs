//! Strict V3 Team-expansion approval contract with exact existing Home evidence.
//!
//! This API can reserve existing unassigned Profiles or record an existing
//! source Home Team and membership revision. `NewProfile` is intentionally
//! absent until staged provisioning authority is available.

use heterarchy_orchestration_runtime::leadership::member_agent_id;
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::team_expansion::{
    TeamExpansionActorReceiptWire, TeamExpansionActorWire, TeamExpansionDecisionAction,
    TeamExpansionDecisionReceiptWire, TeamExpansionDecisionStateWire,
};

/// Existing target for one zero-based approval slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum TeamExpansionApprovalTargetV3Wire {
    /// Existing Profile with no current Home Team.
    ExistingWorkspaceProfile {
        profile_id: Uuid,
        participation_id: Uuid,
    },
    /// Existing Profile whose exact current source Home is recorded.
    /// Approval does not consent to or complete a transfer.
    ExistingHomeTransfer {
        profile_id: Uuid,
        participation_id: Uuid,
        source_team_id: Uuid,
        source_membership_revision: u64,
    },
}

/// One ordered V3 target plan entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionApprovalSlotV3Wire {
    pub ordinal: u8,
    pub target: TeamExpansionApprovalTargetV3Wire,
}

/// V3 approval intent with exact source Home evidence on transfer slots.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionDecisionV3Request {
    pub actor: TeamExpansionActorWire,
    pub command: TeamExpansionDecisionAction,
    pub slot_plan: Vec<TeamExpansionApprovalSlotV3Wire>,
}

/// Exact readback keeps the original issuer intent separate from the current
/// Orchestrator or original issuer authenticated as reader.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionDecisionV3ReadbackRequest {
    pub intent: TeamExpansionDecisionV3Request,
    pub current_actor: TeamExpansionActorWire,
}

/// Durable decision with the exact V3 target plan supplied to Operations.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionDecisionV3ReceiptWire {
    pub decision: TeamExpansionDecisionReceiptWire,
    pub slot_plan: Vec<TeamExpansionApprovalSlotV3Wire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionDecisionV3Response {
    pub request: TeamExpansionDecisionV3Request,
    pub receipt: TeamExpansionDecisionV3ReceiptWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionDecisionV3ReadbackResponse {
    pub request: TeamExpansionDecisionV3ReadbackRequest,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub result: Option<TeamExpansionDecisionV3ReceiptWire>,
}

/// Read an immutable V3 decision source by durable Workspace and decision ID.
/// The current actor is independently authenticated; the original plan is not
/// supplied or reconstructed from current memberships by this request.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionDecisionV3ProjectionRequest {
    pub workspace_id: Uuid,
    pub decision_id: Uuid,
    pub current_actor: TeamExpansionActorWire,
}

/// V3 decision, original issuer attribution, and ordered source plan returned
/// by the authorized Operations owner projection. Because the query has no
/// prior plan, a consumer cannot independently compare a valid-looking source
/// revision with the historical one; Operations owns that source comparison.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionDecisionV3ProjectionWire {
    pub decision: TeamExpansionDecisionReceiptWire,
    pub original_issuer: TeamExpansionActorReceiptWire,
    pub slot_plan: Vec<TeamExpansionApprovalSlotV3Wire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionDecisionV3ProjectionResponse {
    pub request: TeamExpansionDecisionV3ProjectionRequest,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub result: Option<TeamExpansionDecisionV3ProjectionWire>,
}

#[must_use]
pub fn valid_decision_v3_request(request: &TeamExpansionDecisionV3Request) -> bool {
    if !valid_actor(&request.actor) || !valid_decision_command(&request.command) {
        return false;
    }
    let super::team_expansion::TeamExpansionDecisionWire::Approve { approved_count } =
        request.command.decision
    else {
        return false;
    };
    valid_v3_slot_plan(&request.slot_plan, approved_count, request.command.team_id)
}

#[must_use]
pub fn valid_decision_v3_projection_request(
    request: &TeamExpansionDecisionV3ProjectionRequest,
) -> bool {
    !request.workspace_id.is_nil()
        && !request.decision_id.is_nil()
        && valid_actor(&request.current_actor)
        && request.current_actor.scope.workspace_id == request.workspace_id
}

/// Validate projection structure, identity, canonical actor attribution, and
/// ordered source-plan constraints. The query intentionally has no original
/// plan, so only Operations can establish historical source-revision truth.
#[must_use]
pub fn valid_decision_v3_projection_response(
    response: &TeamExpansionDecisionV3ProjectionResponse,
    request: &TeamExpansionDecisionV3ProjectionRequest,
) -> bool {
    &response.request == request
        && response
            .result
            .as_ref()
            .is_none_or(|projection| valid_projection(projection, request))
}

fn valid_projection(
    projection: &TeamExpansionDecisionV3ProjectionWire,
    request: &TeamExpansionDecisionV3ProjectionRequest,
) -> bool {
    let decision = &projection.decision;
    decision.decision_id == request.decision_id
        && !decision.request_id.is_nil()
        && decision.workspace_id == request.workspace_id
        && !decision.team_id.is_nil()
        && decision.expected_team_revision > 0
        && i64::try_from(decision.expected_team_revision).is_ok()
        && i64::try_from(decision.capacity_revision).is_ok()
        && decision.leadership_revision > 0
        && (1..=8).contains(&decision.approved_count)
        && decision.recorded_at_ms >= 0
        && decision.actor == projection.original_issuer
        && valid_actor_receipt(&projection.original_issuer, request.workspace_id)
        && valid_approved_receipt(decision, request.workspace_id)
        && valid_v3_slot_plan(
            &projection.slot_plan,
            decision.approved_count,
            decision.team_id,
        )
}

fn valid_v3_slot_plan(
    slot_plan: &[TeamExpansionApprovalSlotV3Wire],
    approved_count: u8,
    destination_team_id: Uuid,
) -> bool {
    if slot_plan.len() != usize::from(approved_count) {
        return false;
    }
    let mut profiles = Vec::with_capacity(slot_plan.len());
    let mut participations = Vec::with_capacity(slot_plan.len());
    let mut has_home_transfer = false;
    let exact_plan = slot_plan.iter().enumerate().all(|(index, slot)| {
        let Ok(expected_ordinal) = u8::try_from(index) else {
            return false;
        };
        if slot.ordinal != expected_ordinal {
            return false;
        }
        let (profile_id, participation_id) = match slot.target {
            TeamExpansionApprovalTargetV3Wire::ExistingWorkspaceProfile {
                profile_id,
                participation_id,
            } => (profile_id, participation_id),
            TeamExpansionApprovalTargetV3Wire::ExistingHomeTransfer {
                profile_id,
                participation_id,
                source_team_id,
                source_membership_revision,
            } => {
                has_home_transfer = true;
                if source_team_id.is_nil()
                    || source_team_id == destination_team_id
                    || source_membership_revision == 0
                    || i64::try_from(source_membership_revision).is_err()
                {
                    return false;
                }
                (profile_id, participation_id)
            }
        };
        if profile_id.is_nil()
            || participation_id.is_nil()
            || profiles.contains(&profile_id)
            || participations.contains(&participation_id)
        {
            return false;
        }
        profiles.push(profile_id);
        participations.push(participation_id);
        true
    });
    exact_plan && has_home_transfer
}

#[must_use]
pub fn valid_decision_v3_readback_request(
    request: &TeamExpansionDecisionV3ReadbackRequest,
) -> bool {
    valid_decision_v3_request(&request.intent)
        && valid_actor(&request.current_actor)
        && request.current_actor.scope.workspace_id == request.intent.actor.scope.workspace_id
}

#[must_use]
pub fn valid_decision_v3_response(
    response: &TeamExpansionDecisionV3Response,
    request: &TeamExpansionDecisionV3Request,
) -> bool {
    &response.request == request && valid_decision_v3_receipt(&response.receipt, request)
}

#[must_use]
pub fn valid_decision_v3_readback_response(
    response: &TeamExpansionDecisionV3ReadbackResponse,
    request: &TeamExpansionDecisionV3ReadbackRequest,
) -> bool {
    &response.request == request
        && response
            .result
            .as_ref()
            .is_none_or(|receipt| valid_decision_v3_receipt(receipt, &request.intent))
}

fn valid_decision_v3_receipt(
    result: &TeamExpansionDecisionV3ReceiptWire,
    request: &TeamExpansionDecisionV3Request,
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
        && valid_actor_receipt(&receipt.actor, request.actor.scope.workspace_id)
        && receipt.recorded_at_ms >= 0
        && result.slot_plan == request.slot_plan
        && match command.decision {
            super::team_expansion::TeamExpansionDecisionWire::Approve { approved_count } => {
                receipt.approved_count == approved_count
                    && valid_approved_receipt(receipt, request.actor.scope.workspace_id)
            }
            super::team_expansion::TeamExpansionDecisionWire::Reject => false,
        }
}

fn valid_approved_receipt(receipt: &TeamExpansionDecisionReceiptWire, workspace_id: Uuid) -> bool {
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
                && valid_actor_receipt(finalizer, workspace_id)
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
        && matches!(
            command.decision,
            super::team_expansion::TeamExpansionDecisionWire::Approve { approved_count }
                if (1..=8).contains(&approved_count)
        )
}

fn valid_actor(actor: &TeamExpansionActorWire) -> bool {
    !actor.scope.workspace_id.is_nil()
        && !actor.scope.location_id.is_nil()
        && actor.scope.location_revision > 0
        && actor.scope.workspace_revision > 0
        && !actor.activity_id.is_nil()
        && !actor.session_id.is_nil()
}

fn valid_actor_receipt(receipt: &TeamExpansionActorReceiptWire, workspace_id: Uuid) -> bool {
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
        && receipt.agent_id
            == member_agent_id(workspace_id, receipt.activity_id, receipt.session_id).into_uuid()
        && !receipt.display_name.trim().is_empty()
        && receipt.display_name.len() <= 128
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
    use std::io;

    use super::*;
    use crate::wire::{WorkspaceQueryScope, team_expansion::TeamExpansionDecisionWire};

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

    fn request() -> TeamExpansionDecisionV3Request {
        TeamExpansionDecisionV3Request {
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
                TeamExpansionApprovalSlotV3Wire {
                    ordinal: 0,
                    target: TeamExpansionApprovalTargetV3Wire::ExistingWorkspaceProfile {
                        profile_id: Uuid::from_u128(10),
                        participation_id: Uuid::from_u128(11),
                    },
                },
                TeamExpansionApprovalSlotV3Wire {
                    ordinal: 1,
                    target: TeamExpansionApprovalTargetV3Wire::ExistingHomeTransfer {
                        profile_id: Uuid::from_u128(12),
                        participation_id: Uuid::from_u128(13),
                        source_team_id: Uuid::from_u128(14),
                        source_membership_revision: 15,
                    },
                },
            ],
        }
    }

    fn receipt(request: &TeamExpansionDecisionV3Request) -> TeamExpansionDecisionV3ReceiptWire {
        TeamExpansionDecisionV3ReceiptWire {
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
                    participation_id: Uuid::from_u128(16),
                    profile_id: Uuid::from_u128(17),
                    participant_id: Uuid::from_u128(18),
                    display_name: "Orchestrator".to_owned(),
                },
                recorded_at_ms: 100,
                replayed: false,
            },
            slot_plan: request.slot_plan.clone(),
        }
    }

    fn projection(
        request: &TeamExpansionDecisionV3Request,
    ) -> TeamExpansionDecisionV3ProjectionWire {
        let decision = receipt(request).decision;
        TeamExpansionDecisionV3ProjectionWire {
            original_issuer: decision.actor.clone(),
            decision,
            slot_plan: request.slot_plan.clone(),
        }
    }

    #[test]
    fn v3_requires_exact_home_source_and_response_plan() -> Result<(), serde_json::Error> {
        let request = request();
        assert!(valid_decision_v3_request(&request));
        let encoded = serde_json::to_value(&request)?;
        assert_eq!(
            encoded["slotPlan"][1]["target"]["type"],
            "existing_home_transfer"
        );
        assert_eq!(
            encoded["slotPlan"][1]["target"]["sourceMembershipRevision"],
            15
        );
        let response = TeamExpansionDecisionV3Response {
            request: request.clone(),
            receipt: receipt(&request),
        };
        assert!(valid_decision_v3_response(&response, &request));

        let mut substituted_plan = receipt(&request);
        let TeamExpansionApprovalTargetV3Wire::ExistingHomeTransfer {
            source_membership_revision,
            ..
        } = &mut substituted_plan.slot_plan[1].target
        else {
            unreachable!();
        };
        *source_membership_revision += 1;
        let substituted_response = TeamExpansionDecisionV3Response {
            request: request.clone(),
            receipt: substituted_plan,
        };
        assert!(!valid_decision_v3_response(&substituted_response, &request));

        let mut same_team = request.clone();
        let TeamExpansionApprovalTargetV3Wire::ExistingHomeTransfer { source_team_id, .. } =
            &mut same_team.slot_plan[1].target
        else {
            unreachable!();
        };
        *source_team_id = same_team.command.team_id;
        assert!(!valid_decision_v3_request(&same_team));

        let mut unassigned_only = request.clone();
        unassigned_only.slot_plan[1].target =
            TeamExpansionApprovalTargetV3Wire::ExistingWorkspaceProfile {
                profile_id: Uuid::from_u128(19),
                participation_id: Uuid::from_u128(20),
            };
        assert!(!valid_decision_v3_request(&unassigned_only));

        let mut unsupported = serde_json::to_value(request)?;
        unsupported["slotPlan"][0]["target"] = serde_json::json!({
            "type": "new_profile"
        });
        assert!(serde_json::from_value::<TeamExpansionDecisionV3Request>(unsupported).is_err());
        Ok(())
    }

    #[test]
    fn v3_readback_preserves_reader_plan_and_requires_nullable_result()
    -> Result<(), Box<dyn std::error::Error>> {
        let intent = request();
        let readback = TeamExpansionDecisionV3ReadbackRequest {
            intent: intent.clone(),
            current_actor: actor(Uuid::from_u128(19)),
        };
        let response = TeamExpansionDecisionV3ReadbackResponse {
            request: readback.clone(),
            result: Some(receipt(&intent)),
        };
        assert!(valid_decision_v3_readback_request(&readback));
        assert!(valid_decision_v3_readback_response(&response, &readback));

        let mut foreign = readback.clone();
        foreign.current_actor.scope.workspace_id = Uuid::from_u128(20);
        assert!(!valid_decision_v3_readback_request(&foreign));

        let mut missing_result = serde_json::to_value(response)?;
        let Some(object) = missing_result.as_object_mut() else {
            return Err(io::Error::other("response fixture must be a JSON object").into());
        };
        object.remove("result");
        assert!(
            serde_json::from_value::<TeamExpansionDecisionV3ReadbackResponse>(missing_result)
                .is_err()
        );
        let explicit_null = serde_json::json!({ "request": readback, "result": null });
        let parsed =
            serde_json::from_value::<TeamExpansionDecisionV3ReadbackResponse>(explicit_null)?;
        assert!(parsed.result.is_none());
        Ok(())
    }

    #[test]
    fn v3_receipts_require_canonical_actor_ids() {
        let request = request();
        let mut bad_issuer_receipt = receipt(&request);
        bad_issuer_receipt.decision.actor.agent_id = Uuid::from_u128(21);
        let response = TeamExpansionDecisionV3Response {
            request: request.clone(),
            receipt: bad_issuer_receipt,
        };
        assert!(!valid_decision_v3_response(&response, &request));

        let mut anchored_receipt = receipt(&request);
        anchored_receipt.decision.state = TeamExpansionDecisionStateWire::Approved;
        anchored_receipt.decision.cooldown_started_at_ms = Some(100);
        anchored_receipt.decision.cooldown_until_ms = Some(3_600_100);
        let mut finalizer = anchored_receipt.decision.actor.clone();
        finalizer.agent_id = Uuid::from_u128(22);
        anchored_receipt.decision.finalized_by = Some(finalizer);
        anchored_receipt.decision.finalized_by_leadership_revision = Some(4);
        let response = TeamExpansionDecisionV3Response {
            request: request.clone(),
            receipt: anchored_receipt,
        };
        assert!(!valid_decision_v3_response(&response, &request));
    }

    #[test]
    fn v3_projection_recovers_exact_source_by_id_without_original_plan()
    -> Result<(), Box<dyn std::error::Error>> {
        let original = request();
        let request = TeamExpansionDecisionV3ProjectionRequest {
            workspace_id: original.actor.scope.workspace_id,
            decision_id: original.command.decision_id,
            current_actor: actor(Uuid::from_u128(23)),
        };
        let encoded_request = serde_json::to_value(&request)?;
        assert!(encoded_request.get("intent").is_none());
        assert!(encoded_request.get("slotPlan").is_none());
        let response = TeamExpansionDecisionV3ProjectionResponse {
            request: request.clone(),
            result: Some(projection(&original)),
        };
        assert!(valid_decision_v3_projection_request(&request));
        assert!(valid_decision_v3_projection_response(&response, &request));

        let mut foreign_workspace = request.clone();
        foreign_workspace.current_actor.scope.workspace_id = Uuid::from_u128(24);
        assert!(!valid_decision_v3_projection_request(&foreign_workspace));

        let mut wrong_decision = response.clone();
        let Some(projection) = wrong_decision.result.as_mut() else {
            return Err(io::Error::other("projection fixture must be present").into());
        };
        projection.decision.decision_id = Uuid::from_u128(25);
        assert!(!valid_decision_v3_projection_response(
            &wrong_decision,
            &request
        ));

        let mut wrong_issuer = response.clone();
        let wrong_session = Uuid::from_u128(26);
        let Some(projection) = wrong_issuer.result.as_mut() else {
            return Err(io::Error::other("projection fixture must be present").into());
        };
        projection.original_issuer.session_id = wrong_session;
        projection.decision.actor.session_id = wrong_session;
        assert!(!valid_decision_v3_projection_response(
            &wrong_issuer,
            &request
        ));

        let mut wrong_source = response.clone();
        let Some(projection) = wrong_source.result.as_mut() else {
            return Err(io::Error::other("projection fixture must be present").into());
        };
        let TeamExpansionApprovalTargetV3Wire::ExistingHomeTransfer {
            source_membership_revision,
            ..
        } = &mut projection.slot_plan[1].target
        else {
            unreachable!();
        };
        *source_membership_revision = 0;
        assert!(!valid_decision_v3_projection_response(
            &wrong_source,
            &request
        ));

        let absent = TeamExpansionDecisionV3ProjectionResponse {
            request: request.clone(),
            result: None,
        };
        assert!(valid_decision_v3_projection_response(&absent, &request));

        let mut missing_result = serde_json::to_value(response)?;
        let Some(object) = missing_result.as_object_mut() else {
            return Err(io::Error::other("response fixture must be a JSON object").into());
        };
        object.remove("result");
        assert!(
            serde_json::from_value::<TeamExpansionDecisionV3ProjectionResponse>(missing_result)
                .is_err()
        );
        let explicit_null = serde_json::json!({ "request": request, "result": null });
        let parsed =
            serde_json::from_value::<TeamExpansionDecisionV3ProjectionResponse>(explicit_null)?;
        assert!(parsed.result.is_none());
        Ok(())
    }
}
