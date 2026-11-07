//! Typed consent, withdrawal and inbound-seat reservation contracts.
//!
//! These Operations lifecycle facts do not activate source work, change
//! Broker ownership, certify a task, or release the inbound Team seat.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::WorkspaceQueryScope;

/// Maximum JSON payload for one lifecycle statement or exact readback.
pub const MAX_TEMPORARY_DISPATCH_LIFECYCLE_BODY_BYTES: usize = 32 * 1024;

/// Current authenticated Activity and Session used for one lifecycle operation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TemporaryDispatchLifecycleActorWire {
    pub scope: WorkspaceQueryScope,
    pub activity_id: Uuid,
    pub session_id: Uuid,
}

/// Which current authority is making a consent statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TemporaryDispatchConsentKindWire {
    TargetAcceptance,
    HomeLeaderConsent,
    WorkspaceOrchestratorApproval,
}

/// One exact consent, withdrawal or initial reservation command.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TemporaryDispatchLifecycleMutationRequest {
    pub actor: TemporaryDispatchLifecycleActorWire,
    pub request_id: Uuid,
    pub action: TemporaryDispatchLifecycleActionWire,
}

/// Closed set of Operations lifecycle mutations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum TemporaryDispatchLifecycleActionWire {
    Consent {
        receipt_id: Uuid,
        kind: TemporaryDispatchConsentKindWire,
        expected_revision: u64,
    },
    WithdrawConsent {
        receipt_id: Uuid,
        consent_receipt_id: Uuid,
        kind: TemporaryDispatchConsentKindWire,
        expected_revision: u64,
    },
    Reserve {
        expected_revision: u64,
    },
}

/// Original command retained for exact recovery after an uncertain reply.
pub type TemporaryDispatchLifecycleReadbackIntent = TemporaryDispatchLifecycleMutationRequest;

/// Read an exact lifecycle result using a separately authenticated current Session.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TemporaryDispatchLifecycleReadbackRequest {
    pub intent: TemporaryDispatchLifecycleReadbackIntent,
    pub current_actor: TemporaryDispatchLifecycleActorWire,
}

/// Immutable actor provenance retained by Operations.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TemporaryDispatchLifecycleActorReceiptWire {
    pub activity_id: Uuid,
    pub agent_id: Uuid,
    pub session_id: Uuid,
    pub participation_id: Uuid,
    pub profile_id: Uuid,
    pub participant_id: Uuid,
    pub display_name: String,
}

/// Immutable consent statement receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TemporaryDispatchConsentReceiptWire {
    pub request_id: Uuid,
    pub receipt_id: Uuid,
    pub kind: TemporaryDispatchConsentKindWire,
    pub revision: u64,
    pub authority_revision: u64,
    pub actor: TemporaryDispatchLifecycleActorReceiptWire,
    pub recorded_at_ms: i64,
    pub replayed: bool,
}

/// Immutable receipt withdrawing one exact consent statement.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TemporaryDispatchConsentWithdrawalReceiptWire {
    pub request_id: Uuid,
    pub receipt_id: Uuid,
    pub consent_receipt_id: Uuid,
    pub kind: TemporaryDispatchConsentKindWire,
    pub revision: u64,
    pub authority_revision: u64,
    pub actor: TemporaryDispatchLifecycleActorReceiptWire,
    pub recorded_at_ms: i64,
    pub replayed: bool,
}

/// Operations-owned state of one inbound Team reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TemporaryDispatchReservationStateWire {
    PendingConflict,
    Reserved,
}

/// Immutable reservation receipt; this is not execution activation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TemporaryDispatchReservationReceiptWire {
    pub request_id: Uuid,
    pub state: TemporaryDispatchReservationStateWire,
    pub revision: u64,
    pub target_participation_id: Uuid,
    pub target_profile_id: Uuid,
    pub target_membership_revision: u64,
    pub requesting_leadership_revision: u64,
    pub home_leadership_revision: u64,
    pub workspace_leadership_revision: u64,
    pub target_consent_receipt_id: Uuid,
    pub home_leader_consent_receipt_id: Uuid,
    pub orchestrator_consent_receipt_id: Uuid,
    pub actor: TemporaryDispatchLifecycleActorReceiptWire,
    pub recorded_at_ms: i64,
    pub replayed: bool,
}

/// One result matching the corresponding lifecycle command variant.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum TemporaryDispatchLifecycleResultWire {
    Consent {
        receipt: Box<TemporaryDispatchConsentReceiptWire>,
    },
    WithdrawConsent {
        receipt: Box<TemporaryDispatchConsentWithdrawalReceiptWire>,
    },
    Reservation {
        receipt: Box<TemporaryDispatchReservationReceiptWire>,
    },
}

/// Echo the exact command and the immutable resulting receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TemporaryDispatchLifecycleMutationResponse {
    pub request: TemporaryDispatchLifecycleMutationRequest,
    pub result: TemporaryDispatchLifecycleResultWire,
}

/// Exact readback; a missing receipt is a snapshot absence only.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TemporaryDispatchLifecycleReadbackResponse {
    pub request: TemporaryDispatchLifecycleReadbackRequest,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub result: Option<TemporaryDispatchLifecycleResultWire>,
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

pub(crate) fn valid_actor(actor: &TemporaryDispatchLifecycleActorWire) -> bool {
    let scope = &actor.scope;
    !scope.workspace_id.is_nil()
        && !scope.location_id.is_nil()
        && scope.location_revision > 0
        && scope.workspace_revision > 0
        && !scope.locator.is_empty()
        && scope.locator.len() <= 4096
        && !scope.locator.chars().any(char::is_control)
        && !actor.activity_id.is_nil()
        && !actor.session_id.is_nil()
}

pub(crate) fn valid_mutation_request(request: &TemporaryDispatchLifecycleMutationRequest) -> bool {
    if !valid_actor(&request.actor) || request.request_id.is_nil() {
        return false;
    }
    match request.action {
        TemporaryDispatchLifecycleActionWire::Consent {
            receipt_id,
            expected_revision,
            ..
        } => {
            !receipt_id.is_nil()
                && receipt_id != request.request_id
                && expected_revision < i64::MAX as u64
        }
        TemporaryDispatchLifecycleActionWire::WithdrawConsent {
            receipt_id,
            consent_receipt_id,
            expected_revision,
            ..
        } => {
            !receipt_id.is_nil()
                && !consent_receipt_id.is_nil()
                && receipt_id != consent_receipt_id
                && receipt_id != request.request_id
                && expected_revision > 0
                && expected_revision < i64::MAX as u64
        }
        TemporaryDispatchLifecycleActionWire::Reserve { expected_revision } => {
            expected_revision == 0
        }
    }
}

pub(crate) fn valid_readback_request(request: &TemporaryDispatchLifecycleReadbackRequest) -> bool {
    valid_mutation_request(&request.intent)
        && valid_actor(&request.current_actor)
        && request.current_actor.scope.workspace_id == request.intent.actor.scope.workspace_id
}

fn valid_actor_receipt(actor: &TemporaryDispatchLifecycleActorReceiptWire) -> bool {
    !actor.activity_id.is_nil()
        && !actor.agent_id.is_nil()
        && !actor.session_id.is_nil()
        && !actor.participation_id.is_nil()
        && !actor.profile_id.is_nil()
        && !actor.participant_id.is_nil()
        && !actor.display_name.is_empty()
        && actor.display_name.len() <= 128
}

fn valid_consent_receipt(receipt: &TemporaryDispatchConsentReceiptWire) -> bool {
    !receipt.request_id.is_nil()
        && !receipt.receipt_id.is_nil()
        && receipt.revision > 0
        && i64::try_from(receipt.revision).is_ok()
        && receipt.authority_revision > 0
        && i64::try_from(receipt.authority_revision).is_ok()
        && receipt.recorded_at_ms >= 0
        && valid_actor_receipt(&receipt.actor)
}

fn valid_withdrawal_receipt(receipt: &TemporaryDispatchConsentWithdrawalReceiptWire) -> bool {
    valid_consent_receipt(&TemporaryDispatchConsentReceiptWire {
        request_id: receipt.request_id,
        receipt_id: receipt.receipt_id,
        kind: receipt.kind,
        revision: receipt.revision,
        authority_revision: receipt.authority_revision,
        actor: receipt.actor.clone(),
        recorded_at_ms: receipt.recorded_at_ms,
        replayed: receipt.replayed,
    }) && !receipt.consent_receipt_id.is_nil()
        && receipt.consent_receipt_id != receipt.receipt_id
}

fn valid_reservation_receipt(receipt: &TemporaryDispatchReservationReceiptWire) -> bool {
    !receipt.request_id.is_nil()
        && receipt.revision == 1
        && !receipt.target_participation_id.is_nil()
        && !receipt.target_profile_id.is_nil()
        && receipt.target_membership_revision > 0
        && receipt.requesting_leadership_revision > 0
        && receipt.home_leadership_revision > 0
        && receipt.workspace_leadership_revision > 0
        && !receipt.target_consent_receipt_id.is_nil()
        && !receipt.home_leader_consent_receipt_id.is_nil()
        && !receipt.orchestrator_consent_receipt_id.is_nil()
        && receipt.recorded_at_ms >= 0
        && valid_actor_receipt(&receipt.actor)
}

pub(crate) fn valid_mutation_response(
    response: &TemporaryDispatchLifecycleMutationResponse,
    request: &TemporaryDispatchLifecycleMutationRequest,
) -> bool {
    if response.request != *request {
        return false;
    }
    match (&request.action, &response.result) {
        (
            TemporaryDispatchLifecycleActionWire::Consent {
                receipt_id,
                kind,
                expected_revision,
            },
            TemporaryDispatchLifecycleResultWire::Consent { receipt },
        ) => {
            valid_consent_receipt(receipt)
                && receipt.request_id == request.request_id
                && receipt.receipt_id == *receipt_id
                && receipt.kind == *kind
                && receipt.revision == expected_revision + 1
                && receipt.actor.activity_id == request.actor.activity_id
                && receipt.actor.session_id == request.actor.session_id
        }
        (
            TemporaryDispatchLifecycleActionWire::WithdrawConsent {
                receipt_id,
                consent_receipt_id,
                kind,
                expected_revision,
            },
            TemporaryDispatchLifecycleResultWire::WithdrawConsent { receipt },
        ) => {
            valid_withdrawal_receipt(receipt)
                && receipt.request_id == request.request_id
                && receipt.receipt_id == *receipt_id
                && receipt.consent_receipt_id == *consent_receipt_id
                && receipt.kind == *kind
                && receipt.revision == expected_revision + 1
                && receipt.actor.activity_id == request.actor.activity_id
                && receipt.actor.session_id == request.actor.session_id
        }
        (
            TemporaryDispatchLifecycleActionWire::Reserve { expected_revision },
            TemporaryDispatchLifecycleResultWire::Reservation { receipt },
        ) => {
            valid_reservation_receipt(receipt)
                && receipt.request_id == request.request_id
                && *expected_revision == 0
                && receipt.actor.activity_id == request.actor.activity_id
                && receipt.actor.session_id == request.actor.session_id
        }
        _ => false,
    }
}

pub(crate) fn valid_readback_response(
    response: &TemporaryDispatchLifecycleReadbackResponse,
    request: &TemporaryDispatchLifecycleReadbackRequest,
) -> bool {
    response.request == *request
        && response
            .result
            .as_ref()
            .is_none_or(|result| result_matches_intent(result, &request.intent))
}

fn result_matches_intent(
    result: &TemporaryDispatchLifecycleResultWire,
    intent: &TemporaryDispatchLifecycleMutationRequest,
) -> bool {
    if !valid_mutation_request(intent) {
        return false;
    }
    match (&intent.action, result) {
        (
            TemporaryDispatchLifecycleActionWire::Consent {
                receipt_id,
                kind,
                expected_revision,
            },
            TemporaryDispatchLifecycleResultWire::Consent { receipt },
        ) => {
            valid_consent_receipt(receipt)
                && receipt.request_id == intent.request_id
                && receipt.receipt_id == *receipt_id
                && receipt.kind == *kind
                && receipt.revision == expected_revision + 1
                && receipt.actor.activity_id == intent.actor.activity_id
                && receipt.actor.session_id == intent.actor.session_id
        }
        (
            TemporaryDispatchLifecycleActionWire::WithdrawConsent {
                receipt_id,
                consent_receipt_id,
                kind,
                expected_revision,
            },
            TemporaryDispatchLifecycleResultWire::WithdrawConsent { receipt },
        ) => {
            valid_withdrawal_receipt(receipt)
                && receipt.request_id == intent.request_id
                && receipt.receipt_id == *receipt_id
                && receipt.consent_receipt_id == *consent_receipt_id
                && receipt.kind == *kind
                && receipt.revision == expected_revision + 1
                && receipt.actor.activity_id == intent.actor.activity_id
                && receipt.actor.session_id == intent.actor.session_id
        }
        (
            TemporaryDispatchLifecycleActionWire::Reserve { expected_revision },
            TemporaryDispatchLifecycleResultWire::Reservation { receipt },
        ) => {
            valid_reservation_receipt(receipt)
                && receipt.request_id == intent.request_id
                && *expected_revision == 0
                && receipt.actor.activity_id == intent.actor.activity_id
                && receipt.actor.session_id == intent.actor.session_id
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn actor() -> TemporaryDispatchLifecycleActorWire {
        TemporaryDispatchLifecycleActorWire {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "tf-lifecycle-test".to_owned(),
            },
            activity_id: Uuid::from_u128(5),
            session_id: Uuid::from_u128(6),
        }
    }

    fn request(
        action: TemporaryDispatchLifecycleActionWire,
    ) -> TemporaryDispatchLifecycleMutationRequest {
        TemporaryDispatchLifecycleMutationRequest {
            actor: actor(),
            request_id: Uuid::from_u128(7),
            action,
        }
    }

    #[test]
    fn consent_withdrawal_and_reservation_round_trip_with_closed_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let requests = [
            request(TemporaryDispatchLifecycleActionWire::Consent {
                receipt_id: Uuid::from_u128(8),
                kind: TemporaryDispatchConsentKindWire::TargetAcceptance,
                expected_revision: 0,
            }),
            request(TemporaryDispatchLifecycleActionWire::WithdrawConsent {
                receipt_id: Uuid::from_u128(9),
                consent_receipt_id: Uuid::from_u128(8),
                kind: TemporaryDispatchConsentKindWire::TargetAcceptance,
                expected_revision: 1,
            }),
            request(TemporaryDispatchLifecycleActionWire::Reserve {
                expected_revision: 0,
            }),
        ];
        for original in requests {
            let encoded = serde_json::to_vec(&original)?;
            let decoded: TemporaryDispatchLifecycleMutationRequest =
                serde_json::from_slice(&encoded)?;
            assert_eq!(decoded, original);
            let encoded_value: Value = serde_json::from_slice(&encoded)?;
            assert!(encoded_value["action"].get("type").is_some());
            let mut with_unknown: Value = serde_json::from_slice(&encoded)?;
            with_unknown["unknown"] = json!(true);
            assert!(
                serde_json::from_value::<TemporaryDispatchLifecycleMutationRequest>(with_unknown)
                    .is_err()
            );
            let mut with_unknown_action: Value = serde_json::from_slice(&encoded)?;
            let action = with_unknown_action
                .get_mut("action")
                .and_then(Value::as_object_mut)
                .ok_or_else(|| std::io::Error::other("mutation action must encode as an object"))?;
            action.insert("unsupported".to_owned(), json!(true));
            assert!(
                serde_json::from_value::<TemporaryDispatchLifecycleMutationRequest>(
                    with_unknown_action
                )
                .is_err()
            );
        }
        let consent = request(TemporaryDispatchLifecycleActionWire::Consent {
            receipt_id: Uuid::from_u128(8),
            kind: TemporaryDispatchConsentKindWire::TargetAcceptance,
            expected_revision: 0,
        });
        let mut missing_revision = serde_json::to_value(consent)?;
        let root = missing_revision
            .as_object_mut()
            .ok_or_else(|| std::io::Error::other("mutation request must encode as an object"))?;
        let action = root
            .get_mut("action")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| std::io::Error::other("mutation action must encode as an object"))?;
        action.remove("expectedRevision");
        assert!(
            serde_json::from_value::<TemporaryDispatchLifecycleMutationRequest>(missing_revision)
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn readback_result_is_required_but_explicit_null_is_valid()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = TemporaryDispatchLifecycleReadbackRequest {
            intent: request(TemporaryDispatchLifecycleActionWire::Reserve {
                expected_revision: 0,
            }),
            current_actor: actor(),
        };
        let mut absent: Value = serde_json::to_value(TemporaryDispatchLifecycleReadbackResponse {
            request: request.clone(),
            result: None,
        })?;
        assert!(
            serde_json::from_value::<TemporaryDispatchLifecycleReadbackResponse>(absent.clone())
                .is_ok()
        );
        absent["unknown"] = json!(true);
        assert!(
            serde_json::from_value::<TemporaryDispatchLifecycleReadbackResponse>(absent).is_err()
        );

        let mut missing: Value =
            serde_json::to_value(TemporaryDispatchLifecycleReadbackResponse {
                request,
                result: None,
            })?;
        let object = missing
            .as_object_mut()
            .ok_or_else(|| std::io::Error::other("readback response must encode as an object"))?;
        object.remove("result");
        assert!(
            serde_json::from_value::<TemporaryDispatchLifecycleReadbackResponse>(missing).is_err()
        );
        Ok(())
    }

    #[test]
    fn pending_reservation_is_explicitly_not_a_reserved_result()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = request(TemporaryDispatchLifecycleActionWire::Reserve {
            expected_revision: 0,
        });
        let response = TemporaryDispatchLifecycleMutationResponse {
            request: request.clone(),
            result: TemporaryDispatchLifecycleResultWire::Reservation {
                receipt: Box::new(TemporaryDispatchReservationReceiptWire {
                    request_id: request.request_id,
                    state: TemporaryDispatchReservationStateWire::PendingConflict,
                    revision: 1,
                    target_participation_id: Uuid::from_u128(20),
                    target_profile_id: Uuid::from_u128(21),
                    target_membership_revision: 1,
                    requesting_leadership_revision: 1,
                    home_leadership_revision: 1,
                    workspace_leadership_revision: 1,
                    target_consent_receipt_id: Uuid::from_u128(22),
                    home_leader_consent_receipt_id: Uuid::from_u128(23),
                    orchestrator_consent_receipt_id: Uuid::from_u128(24),
                    actor: TemporaryDispatchLifecycleActorReceiptWire {
                        activity_id: request.actor.activity_id,
                        agent_id: Uuid::from_u128(25),
                        session_id: request.actor.session_id,
                        participation_id: Uuid::from_u128(26),
                        profile_id: Uuid::from_u128(27),
                        participant_id: Uuid::from_u128(28),
                        display_name: "orchestrator".to_owned(),
                    },
                    recorded_at_ms: 100,
                    replayed: false,
                }),
            },
        };
        assert!(valid_mutation_response(&response, &request));
        let encoded = serde_json::to_value(&response)?;
        assert_eq!(encoded["result"]["result"], json!("reservation"));
        assert_eq!(
            encoded["result"]["receipt"]["state"],
            json!("pending_conflict")
        );
        let decoded: TemporaryDispatchLifecycleMutationResponse = serde_json::from_value(encoded)?;
        assert_eq!(decoded, response);
        Ok(())
    }
}
