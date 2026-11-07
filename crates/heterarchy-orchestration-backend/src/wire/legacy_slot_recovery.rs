//! Exact transport contract for recovering one pre-Workspace-hold approval
//! slot into a current Workspace-capacity reservation.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::team_expansion::{TeamExpansionActorReceiptWire, TeamExpansionActorWire};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LegacySlotRecoveryIntentWire {
    /// Original Workspace that scopes the recovery source and target.
    pub workspace_id: Uuid,
    /// New idempotency key for the recovery operation.
    pub recovery_request_id: Uuid,
    /// Original approval identity and ordinal are preserved exactly.
    pub approval_id: Uuid,
    pub ordinal: u8,
    pub team_id: Uuid,
    pub expected_team_revision: u64,
    pub expected_capacity_revision: u64,
    /// Existing Workspace Profile selected by the current Orchestrator.
    pub target_profile_id: Uuid,
    pub target_participation_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LegacySlotRecoveryRequest {
    /// Current Workspace Orchestrator performing this reservation recovery.
    pub actor: TeamExpansionActorWire,
    pub intent: LegacySlotRecoveryIntentWire,
    pub expected_leadership_revision: i64,
}

/// The current actor may recover the same original intent after a response was
/// lost; this does not change the original approval identity.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LegacySlotRecoveryReadbackRequest {
    /// Exact source, slot, target and Workspace recovered by the original
    /// operation. The currently authenticated reader is separate.
    pub intent: LegacySlotRecoveryIntentWire,
    pub current_actor: TeamExpansionActorWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LegacySlotRecoveryReceiptWire {
    pub intent: LegacySlotRecoveryIntentWire,
    /// Original Team Leader request retained by the existing approval.
    pub approval_request_id: Uuid,
    /// Epoch of the Workspace Orchestrator who first recorded the recovery.
    pub leadership_revision: i64,
    pub actor: TeamExpansionActorReceiptWire,
    pub recorded_at_ms: i64,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LegacySlotRecoveryResponse {
    pub request: LegacySlotRecoveryRequest,
    pub receipt: LegacySlotRecoveryReceiptWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LegacySlotRecoveryReadbackResponse {
    pub request: LegacySlotRecoveryReadbackRequest,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub result: Option<LegacySlotRecoveryReceiptWire>,
}

fn deserialize_required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[must_use]
pub fn valid_legacy_slot_recovery_intent(intent: &LegacySlotRecoveryIntentWire) -> bool {
    !intent.workspace_id.is_nil()
        && !intent.recovery_request_id.is_nil()
        && !intent.approval_id.is_nil()
        && intent.recovery_request_id != intent.approval_id
        && intent.ordinal <= 7
        && !intent.team_id.is_nil()
        && intent.expected_team_revision > 0
        && i64::try_from(intent.expected_team_revision).is_ok()
        && i64::try_from(intent.expected_capacity_revision).is_ok()
        && !intent.target_profile_id.is_nil()
        && !intent.target_participation_id.is_nil()
}

#[must_use]
pub fn valid_legacy_slot_recovery_request(request: &LegacySlotRecoveryRequest) -> bool {
    valid_actor(&request.actor)
        && request.actor.scope.workspace_id == request.intent.workspace_id
        && valid_legacy_slot_recovery_intent(&request.intent)
        && request.expected_leadership_revision > 0
}

#[must_use]
pub fn valid_legacy_slot_recovery_readback_request(
    request: &LegacySlotRecoveryReadbackRequest,
) -> bool {
    valid_legacy_slot_recovery_intent(&request.intent)
        && valid_actor(&request.current_actor)
        && request.current_actor.scope.workspace_id == request.intent.workspace_id
}

#[must_use]
pub fn valid_legacy_slot_recovery_response(
    response: &LegacySlotRecoveryResponse,
    request: &LegacySlotRecoveryRequest,
) -> bool {
    &response.request == request
        && valid_legacy_slot_recovery_receipt(
            &response.receipt,
            &request.intent,
            request.actor.scope.workspace_id,
        )
        && (response.receipt.replayed
            || (actor_matches(&response.receipt.actor, &request.actor)
                && response.receipt.leadership_revision == request.expected_leadership_revision))
}

#[must_use]
pub fn valid_legacy_slot_recovery_readback_response(
    response: &LegacySlotRecoveryReadbackResponse,
    request: &LegacySlotRecoveryReadbackRequest,
) -> bool {
    &response.request == request
        && response.result.as_ref().is_none_or(|receipt| {
            valid_legacy_slot_recovery_receipt(
                receipt,
                &request.intent,
                request.intent.workspace_id,
            )
        })
}

fn valid_legacy_slot_recovery_receipt(
    receipt: &LegacySlotRecoveryReceiptWire,
    intent: &LegacySlotRecoveryIntentWire,
    workspace_id: Uuid,
) -> bool {
    receipt.intent == *intent
        && !receipt.approval_request_id.is_nil()
        && receipt.leadership_revision > 0
        && receipt.recorded_at_ms >= 0
        && valid_actor_receipt(&receipt.actor)
        && receipt.actor.agent_id
            == heterarchy_orchestration_runtime::leadership::member_agent_id(
                workspace_id,
                receipt.actor.activity_id,
                receipt.actor.session_id,
            )
            .into_uuid()
}

fn actor_matches(receipt: &TeamExpansionActorReceiptWire, actor: &TeamExpansionActorWire) -> bool {
    receipt.activity_id == actor.activity_id && receipt.session_id == actor.session_id
}

fn valid_actor(actor: &TeamExpansionActorWire) -> bool {
    !actor.scope.workspace_id.is_nil()
        && !actor.scope.location_id.is_nil()
        && actor.scope.location_revision > 0
        && actor.scope.workspace_revision > 0
        && !actor.activity_id.is_nil()
        && !actor.session_id.is_nil()
}

fn valid_actor_receipt(actor: &TeamExpansionActorReceiptWire) -> bool {
    [
        actor.activity_id,
        actor.agent_id,
        actor.session_id,
        actor.participation_id,
        actor.profile_id,
        actor.participant_id,
    ]
    .iter()
    .all(|identity| !identity.is_nil())
        && !actor.display_name.trim().is_empty()
        && actor.display_name.len() <= 128
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::leadership::WorkspaceQueryScope;
    use heterarchy_orchestration_runtime::leadership::member_agent_id;

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

    fn intent() -> LegacySlotRecoveryIntentWire {
        LegacySlotRecoveryIntentWire {
            workspace_id: Uuid::from_u128(1),
            recovery_request_id: Uuid::from_u128(6),
            approval_id: Uuid::from_u128(7),
            ordinal: 2,
            team_id: Uuid::from_u128(8),
            expected_team_revision: 3,
            expected_capacity_revision: 2,
            target_profile_id: Uuid::from_u128(9),
            target_participation_id: Uuid::from_u128(10),
        }
    }

    fn actor_receipt() -> TeamExpansionActorReceiptWire {
        let actor = actor(Uuid::from_u128(14));
        TeamExpansionActorReceiptWire {
            activity_id: actor.activity_id,
            agent_id: member_agent_id(
                actor.scope.workspace_id,
                actor.activity_id,
                actor.session_id,
            )
            .into_uuid(),
            session_id: actor.session_id,
            participation_id: Uuid::from_u128(15),
            profile_id: Uuid::from_u128(16),
            participant_id: Uuid::from_u128(17),
            display_name: "Orchestrator".to_owned(),
        }
    }

    #[test]
    fn recovery_keeps_original_approval_and_new_recovery_identity_distinct() {
        let intent = intent();
        assert!(valid_legacy_slot_recovery_intent(&intent));
        let mut changed = intent;
        changed.recovery_request_id = Uuid::from_u128(11);
        assert!(valid_legacy_slot_recovery_intent(&changed));
        assert_eq!(changed.approval_id, intent.approval_id);
        assert_eq!(changed.ordinal, intent.ordinal);
    }

    #[test]
    fn readback_binds_the_current_reader_to_the_same_workspace() {
        let request = LegacySlotRecoveryReadbackRequest {
            intent: intent(),
            current_actor: actor(Uuid::from_u128(12)),
        };
        assert!(valid_legacy_slot_recovery_readback_request(&request));
        let mut foreign = request;
        foreign.current_actor.scope.workspace_id = Uuid::from_u128(13);
        assert!(!valid_legacy_slot_recovery_readback_request(&foreign));
    }

    #[test]
    fn readback_binds_workspace_and_preserves_original_writer_attribution() {
        let source_intent = intent();
        let request = LegacySlotRecoveryReadbackRequest {
            intent: source_intent,
            current_actor: actor(Uuid::from_u128(12)),
        };
        let receipt = LegacySlotRecoveryReceiptWire {
            intent: source_intent,
            approval_request_id: Uuid::from_u128(18),
            leadership_revision: 5,
            actor: actor_receipt(),
            recorded_at_ms: 100,
            replayed: true,
        };
        let response = LegacySlotRecoveryReadbackResponse {
            request: request.clone(),
            result: Some(receipt.clone()),
        };
        assert!(valid_legacy_slot_recovery_readback_response(
            &response, &request
        ));

        let mut wrong_writer = response.clone();
        if let Some(receipt) = wrong_writer.result.as_mut() {
            receipt.actor.agent_id = Uuid::from_u128(19);
        }
        assert!(!valid_legacy_slot_recovery_readback_response(
            &wrong_writer,
            &request,
        ));

        let mut wrong_workspace = response;
        wrong_workspace.request.current_actor.scope.workspace_id = Uuid::from_u128(20);
        assert!(!valid_legacy_slot_recovery_readback_response(
            &wrong_workspace,
            &request,
        ));
    }

    #[test]
    fn recovery_receipt_checks_original_actor_agent_from_historical_selector() {
        let request = LegacySlotRecoveryRequest {
            actor: actor(Uuid::from_u128(14)),
            intent: intent(),
            expected_leadership_revision: 5,
        };
        let mut response = LegacySlotRecoveryResponse {
            request: request.clone(),
            receipt: LegacySlotRecoveryReceiptWire {
                intent: request.intent,
                approval_request_id: Uuid::from_u128(18),
                leadership_revision: 5,
                actor: actor_receipt(),
                recorded_at_ms: 100,
                replayed: false,
            },
        };
        assert!(valid_legacy_slot_recovery_response(&response, &request));
        response.receipt.actor.session_id = Uuid::from_u128(20);
        response.receipt.actor.agent_id = member_agent_id(
            request.actor.scope.workspace_id,
            response.receipt.actor.activity_id,
            response.receipt.actor.session_id,
        )
        .into_uuid();
        assert!(!valid_legacy_slot_recovery_response(&response, &request));
    }

    #[test]
    fn readback_requires_result_key_but_accepts_explicit_absence()
    -> Result<(), Box<dyn std::error::Error>> {
        let response = LegacySlotRecoveryReadbackResponse {
            request: LegacySlotRecoveryReadbackRequest {
                intent: intent(),
                current_actor: actor(Uuid::from_u128(12)),
            },
            result: None,
        };
        let mut value = serde_json::to_value(response)?;
        assert_eq!(
            serde_json::from_value::<LegacySlotRecoveryReadbackResponse>(value.clone())?.result,
            None
        );
        value
            .as_object_mut()
            .ok_or("response is not a JSON object")?
            .remove("result");
        assert!(serde_json::from_value::<LegacySlotRecoveryReadbackResponse>(value).is_err());
        Ok(())
    }
}
