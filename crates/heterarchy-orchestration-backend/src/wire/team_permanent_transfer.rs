//! Strict wire contracts for permanent Team transfer intent and consents.
//!
//! These APIs preserve an Orchestrator proposal, source Home Leader consent,
//! and target Profile acceptance. They do not describe membership cutover,
//! work settlement, or handoff.

use heterarchy_orchestration_runtime::leadership::member_agent_id;
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::team_expansion::{TeamExpansionActorReceiptWire, TeamExpansionActorWire};

mod generation;
mod preparation;
mod source;
pub use generation::*;
pub use preparation::*;
pub use source::*;

/// One exact Orchestrator proposal against an approved destination slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferIntentActionWire {
    pub transfer_id: Uuid,
    pub source_team_id: Uuid,
    pub destination_team_id: Uuid,
    pub target_participation_id: Uuid,
    pub destination_request_id: Uuid,
    pub destination_decision_id: Uuid,
    pub destination_ordinal: u8,
    pub expected_destination_team_revision: u64,
    pub expected_destination_capacity_revision: u64,
    pub expected_workspace_leadership_revision: i64,
    pub expected_source_membership_revision: u64,
    pub expected_source_leadership_revision: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferIntentRequest {
    pub actor: TeamExpansionActorWire,
    pub command: TeamPermanentTransferIntentActionWire,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamPermanentTransferConsentKindWire {
    SourceHomeLeader,
    TargetAgentAcceptance,
}

/// One immutable source Leader consent or target acceptance statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferConsentActionWire {
    pub receipt_id: Uuid,
    pub transfer_id: Uuid,
    pub kind: TeamPermanentTransferConsentKindWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferConsentRequest {
    pub actor: TeamExpansionActorWire,
    pub command: TeamPermanentTransferConsentActionWire,
}

/// Read an authorized transfer snapshot by immutable Workspace and transfer
/// IDs using an independently authenticated current reader.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferReadbackRequest {
    pub workspace_id: Uuid,
    pub transfer_id: Uuid,
    pub current_actor: TeamExpansionActorWire,
}

/// Read one exact consent receipt while allowing the authenticated reader to
/// differ from the original consentor.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferConsentReadbackRequest {
    pub intent: TeamPermanentTransferConsentRequest,
    pub current_actor: TeamExpansionActorWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferIntentReceiptWire {
    pub transfer_id: Uuid,
    pub workspace_id: Uuid,
    pub source_team_id: Uuid,
    pub destination_team_id: Uuid,
    pub target_participation_id: Uuid,
    pub target_profile_id: Uuid,
    pub source_membership_revision: u64,
    pub source_leadership_revision: i64,
    pub source_leader_participation_id: Uuid,
    pub source_leader_profile_id: Uuid,
    pub destination_team_revision: u64,
    pub destination_capacity_revision: u64,
    pub destination_workspace_leadership_revision: i64,
    pub destination_request_id: Uuid,
    pub destination_decision_id: Uuid,
    pub destination_ordinal: u8,
    pub workspace_actor: TeamExpansionActorReceiptWire,
    pub recorded_at_ms: i64,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferConsentReceiptWire {
    pub receipt_id: Uuid,
    pub transfer_id: Uuid,
    pub kind: TeamPermanentTransferConsentKindWire,
    pub authority_revision: i64,
    pub membership_revision: u64,
    pub actor: TeamExpansionActorReceiptWire,
    pub recorded_at_ms: i64,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferSnapshotWire {
    pub intent: TeamPermanentTransferIntentReceiptWire,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub source_home_leader_consent: Option<TeamPermanentTransferConsentReceiptWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub target_agent_acceptance: Option<TeamPermanentTransferConsentReceiptWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferIntentResponse {
    pub request: TeamPermanentTransferIntentRequest,
    pub receipt: TeamPermanentTransferIntentReceiptWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferConsentResponse {
    pub request: TeamPermanentTransferConsentRequest,
    pub receipt: TeamPermanentTransferConsentReceiptWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferReadbackResponse {
    pub request: TeamPermanentTransferReadbackRequest,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub result: Option<TeamPermanentTransferSnapshotWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferConsentReadbackResponse {
    pub request: TeamPermanentTransferConsentReadbackRequest,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub result: Option<TeamPermanentTransferConsentReceiptWire>,
}

#[must_use]
pub fn valid_intent_request(request: &TeamPermanentTransferIntentRequest) -> bool {
    valid_actor(&request.actor) && valid_intent_action(&request.command)
}

#[must_use]
pub fn valid_intent_response(
    response: &TeamPermanentTransferIntentResponse,
    request: &TeamPermanentTransferIntentRequest,
) -> bool {
    &response.request == request && valid_intent_receipt(&response.receipt, Some(request))
}

#[must_use]
pub fn valid_consent_request(request: &TeamPermanentTransferConsentRequest) -> bool {
    valid_actor(&request.actor)
        && !request.command.receipt_id.is_nil()
        && !request.command.transfer_id.is_nil()
        && request.command.receipt_id != request.command.transfer_id
}

#[must_use]
pub fn valid_consent_response(
    response: &TeamPermanentTransferConsentResponse,
    request: &TeamPermanentTransferConsentRequest,
) -> bool {
    let receipt = &response.receipt;
    &response.request == request
        && valid_consent_receipt(receipt, request.actor.scope.workspace_id)
        && receipt.receipt_id == request.command.receipt_id
        && receipt.transfer_id == request.command.transfer_id
        && receipt.kind == request.command.kind
        && (receipt.replayed
            || (receipt.actor.activity_id == request.actor.activity_id
                && receipt.actor.session_id == request.actor.session_id
                && receipt.actor.agent_id
                    == member_agent_id(
                        request.actor.scope.workspace_id,
                        request.actor.activity_id,
                        request.actor.session_id,
                    )
                    .into_uuid()))
}

#[must_use]
pub fn valid_readback_request(request: &TeamPermanentTransferReadbackRequest) -> bool {
    !request.workspace_id.is_nil()
        && !request.transfer_id.is_nil()
        && valid_actor(&request.current_actor)
        && request.current_actor.scope.workspace_id == request.workspace_id
}

#[must_use]
pub fn valid_readback_response(
    response: &TeamPermanentTransferReadbackResponse,
    request: &TeamPermanentTransferReadbackRequest,
) -> bool {
    &response.request == request
        && response
            .result
            .as_ref()
            .is_none_or(|snapshot| valid_snapshot(snapshot, request))
}

#[must_use]
pub fn valid_consent_readback_request(
    request: &TeamPermanentTransferConsentReadbackRequest,
) -> bool {
    valid_consent_request(&request.intent)
        && valid_actor(&request.current_actor)
        && request.current_actor.scope.workspace_id == request.intent.actor.scope.workspace_id
}

#[must_use]
pub fn valid_consent_readback_response(
    response: &TeamPermanentTransferConsentReadbackResponse,
    request: &TeamPermanentTransferConsentReadbackRequest,
) -> bool {
    &response.request == request
        && response.result.as_ref().is_none_or(|receipt| {
            valid_consent_receipt(receipt, request.intent.actor.scope.workspace_id)
                && receipt.receipt_id == request.intent.command.receipt_id
                && receipt.transfer_id == request.intent.command.transfer_id
                && receipt.kind == request.intent.command.kind
                && receipt.actor.activity_id == request.intent.actor.activity_id
                && receipt.actor.session_id == request.intent.actor.session_id
                && receipt.actor.agent_id
                    == member_agent_id(
                        request.intent.actor.scope.workspace_id,
                        request.intent.actor.activity_id,
                        request.intent.actor.session_id,
                    )
                    .into_uuid()
        })
}

fn valid_snapshot(
    snapshot: &TeamPermanentTransferSnapshotWire,
    request: &TeamPermanentTransferReadbackRequest,
) -> bool {
    let intent = &snapshot.intent;
    valid_intent_receipt(intent, None)
        && intent.workspace_id == request.workspace_id
        && intent.transfer_id == request.transfer_id
        && snapshot
            .source_home_leader_consent
            .as_ref()
            .is_none_or(|consent| {
                valid_consent_receipt(consent, intent.workspace_id)
                    && consent.transfer_id == intent.transfer_id
                    && consent.kind == TeamPermanentTransferConsentKindWire::SourceHomeLeader
                    && consent.authority_revision == intent.source_leadership_revision
                    && consent.membership_revision == intent.source_membership_revision
                    && consent.actor.participation_id == intent.source_leader_participation_id
                    && consent.actor.profile_id == intent.source_leader_profile_id
                    && consent.recorded_at_ms >= intent.recorded_at_ms
            })
        && snapshot
            .target_agent_acceptance
            .as_ref()
            .is_none_or(|consent| {
                valid_consent_receipt(consent, intent.workspace_id)
                    && consent.transfer_id == intent.transfer_id
                    && consent.kind == TeamPermanentTransferConsentKindWire::TargetAgentAcceptance
                    && i64::try_from(intent.source_membership_revision).ok()
                        == Some(consent.authority_revision)
                    && consent.membership_revision == intent.source_membership_revision
                    && consent.actor.participation_id == intent.target_participation_id
                    && consent.actor.profile_id == intent.target_profile_id
                    && consent.recorded_at_ms >= intent.recorded_at_ms
            })
}

fn valid_intent_receipt(
    receipt: &TeamPermanentTransferIntentReceiptWire,
    request: Option<&TeamPermanentTransferIntentRequest>,
) -> bool {
    let actor = &receipt.workspace_actor;
    let structural = !receipt.transfer_id.is_nil()
        && !receipt.workspace_id.is_nil()
        && !receipt.source_team_id.is_nil()
        && !receipt.destination_team_id.is_nil()
        && receipt.source_team_id != receipt.destination_team_id
        && !receipt.target_participation_id.is_nil()
        && !receipt.target_profile_id.is_nil()
        && receipt.source_membership_revision > 0
        && i64::try_from(receipt.source_membership_revision).is_ok()
        && receipt.source_leadership_revision > 0
        && !receipt.source_leader_participation_id.is_nil()
        && !receipt.source_leader_profile_id.is_nil()
        && receipt.source_leader_participation_id != receipt.target_participation_id
        && receipt.source_leader_profile_id != receipt.target_profile_id
        && receipt.destination_team_revision > 0
        && i64::try_from(receipt.destination_team_revision).is_ok()
        && receipt.destination_capacity_revision > 0
        && i64::try_from(receipt.destination_capacity_revision).is_ok()
        && receipt.destination_workspace_leadership_revision > 0
        && !receipt.destination_request_id.is_nil()
        && !receipt.destination_decision_id.is_nil()
        && receipt.destination_ordinal <= 7
        && valid_actor_receipt(actor, receipt.workspace_id)
        && receipt.recorded_at_ms >= 0;
    structural
        && request.is_none_or(|request| {
            receipt.transfer_id == request.command.transfer_id
                && receipt.workspace_id == request.actor.scope.workspace_id
                && receipt.source_team_id == request.command.source_team_id
                && receipt.destination_team_id == request.command.destination_team_id
                && receipt.target_participation_id == request.command.target_participation_id
                && receipt.source_membership_revision
                    == request.command.expected_source_membership_revision
                && receipt.source_leadership_revision
                    == request.command.expected_source_leadership_revision
                && receipt.destination_team_revision
                    == request.command.expected_destination_team_revision
                && receipt.destination_capacity_revision
                    == request.command.expected_destination_capacity_revision
                && receipt.destination_workspace_leadership_revision
                    == request.command.expected_workspace_leadership_revision
                && receipt.destination_request_id == request.command.destination_request_id
                && receipt.destination_decision_id == request.command.destination_decision_id
                && receipt.destination_ordinal == request.command.destination_ordinal
                && (receipt.replayed
                    || (actor.activity_id == request.actor.activity_id
                        && actor.session_id == request.actor.session_id
                        && actor.agent_id
                            == member_agent_id(
                                request.actor.scope.workspace_id,
                                request.actor.activity_id,
                                request.actor.session_id,
                            )
                            .into_uuid()))
        })
}

fn valid_intent_action(command: &TeamPermanentTransferIntentActionWire) -> bool {
    !command.transfer_id.is_nil()
        && !command.source_team_id.is_nil()
        && !command.destination_team_id.is_nil()
        && command.source_team_id != command.destination_team_id
        && !command.target_participation_id.is_nil()
        && !command.destination_request_id.is_nil()
        && !command.destination_decision_id.is_nil()
        && command.destination_ordinal <= 7
        && command.expected_destination_team_revision > 0
        && i64::try_from(command.expected_destination_team_revision).is_ok()
        && command.expected_destination_capacity_revision > 0
        && i64::try_from(command.expected_destination_capacity_revision).is_ok()
        && command.expected_workspace_leadership_revision > 0
        && command.expected_source_membership_revision > 0
        && i64::try_from(command.expected_source_membership_revision).is_ok()
        && command.expected_source_leadership_revision > 0
}

fn valid_consent_receipt(
    receipt: &TeamPermanentTransferConsentReceiptWire,
    workspace_id: Uuid,
) -> bool {
    !receipt.receipt_id.is_nil()
        && !receipt.transfer_id.is_nil()
        && receipt.receipt_id != receipt.transfer_id
        && receipt.authority_revision > 0
        && receipt.membership_revision > 0
        && valid_actor_receipt(&receipt.actor, workspace_id)
        && receipt.recorded_at_ms >= 0
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
    use crate::wire::WorkspaceQueryScope;

    fn actor(activity_id: Uuid, session_id: Uuid) -> TeamExpansionActorWire {
        TeamExpansionActorWire {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            activity_id,
            session_id,
        }
    }

    fn actor_receipt(
        actor: &TeamExpansionActorWire,
        participation_id: Uuid,
        profile_id: Uuid,
    ) -> TeamExpansionActorReceiptWire {
        TeamExpansionActorReceiptWire {
            activity_id: actor.activity_id,
            agent_id: member_agent_id(
                actor.scope.workspace_id,
                actor.activity_id,
                actor.session_id,
            )
            .into_uuid(),
            session_id: actor.session_id,
            participation_id,
            profile_id,
            participant_id: Uuid::from_u128(20),
            display_name: "transfer actor".to_owned(),
        }
    }

    fn intent_request() -> TeamPermanentTransferIntentRequest {
        TeamPermanentTransferIntentRequest {
            actor: actor(Uuid::from_u128(3), Uuid::from_u128(4)),
            command: TeamPermanentTransferIntentActionWire {
                transfer_id: Uuid::from_u128(5),
                source_team_id: Uuid::from_u128(6),
                destination_team_id: Uuid::from_u128(7),
                target_participation_id: Uuid::from_u128(8),
                destination_request_id: Uuid::from_u128(9),
                destination_decision_id: Uuid::from_u128(10),
                destination_ordinal: 0,
                expected_destination_team_revision: 2,
                expected_destination_capacity_revision: 3,
                expected_workspace_leadership_revision: 4,
                expected_source_membership_revision: 5,
                expected_source_leadership_revision: 6,
            },
        }
    }

    fn intent_receipt(
        request: &TeamPermanentTransferIntentRequest,
    ) -> TeamPermanentTransferIntentReceiptWire {
        TeamPermanentTransferIntentReceiptWire {
            transfer_id: request.command.transfer_id,
            workspace_id: request.actor.scope.workspace_id,
            source_team_id: request.command.source_team_id,
            destination_team_id: request.command.destination_team_id,
            target_participation_id: request.command.target_participation_id,
            target_profile_id: Uuid::from_u128(11),
            source_membership_revision: request.command.expected_source_membership_revision,
            source_leadership_revision: request.command.expected_source_leadership_revision,
            source_leader_participation_id: Uuid::from_u128(12),
            source_leader_profile_id: Uuid::from_u128(13),
            destination_team_revision: request.command.expected_destination_team_revision,
            destination_capacity_revision: request.command.expected_destination_capacity_revision,
            destination_workspace_leadership_revision: request
                .command
                .expected_workspace_leadership_revision,
            destination_request_id: request.command.destination_request_id,
            destination_decision_id: request.command.destination_decision_id,
            destination_ordinal: request.command.destination_ordinal,
            workspace_actor: actor_receipt(
                &request.actor,
                Uuid::from_u128(14),
                Uuid::from_u128(15),
            ),
            recorded_at_ms: 100,
            replayed: false,
        }
    }

    fn snapshot(request: &TeamPermanentTransferIntentRequest) -> TeamPermanentTransferSnapshotWire {
        let intent = intent_receipt(request);
        let source_actor = actor(Uuid::from_u128(16), Uuid::from_u128(17));
        let target_actor = actor(Uuid::from_u128(18), Uuid::from_u128(19));
        TeamPermanentTransferSnapshotWire {
            intent,
            source_home_leader_consent: Some(TeamPermanentTransferConsentReceiptWire {
                receipt_id: Uuid::from_u128(21),
                transfer_id: request.command.transfer_id,
                kind: TeamPermanentTransferConsentKindWire::SourceHomeLeader,
                authority_revision: request.command.expected_source_leadership_revision,
                membership_revision: request.command.expected_source_membership_revision,
                actor: actor_receipt(&source_actor, Uuid::from_u128(12), Uuid::from_u128(13)),
                recorded_at_ms: 101,
                replayed: false,
            }),
            target_agent_acceptance: Some(TeamPermanentTransferConsentReceiptWire {
                receipt_id: Uuid::from_u128(22),
                transfer_id: request.command.transfer_id,
                kind: TeamPermanentTransferConsentKindWire::TargetAgentAcceptance,
                authority_revision: 5,
                membership_revision: request.command.expected_source_membership_revision,
                actor: actor_receipt(
                    &target_actor,
                    request.command.target_participation_id,
                    Uuid::from_u128(11),
                ),
                recorded_at_ms: 102,
                replayed: false,
            }),
        }
    }

    #[test]
    fn intent_request_and_receipt_preserve_all_cas_authorities() {
        let request = intent_request();
        assert!(valid_intent_request(&request));
        let response = TeamPermanentTransferIntentResponse {
            request: request.clone(),
            receipt: intent_receipt(&request),
        };
        assert!(valid_intent_response(&response, &request));

        let mut alias_request = request.clone();
        alias_request.actor.activity_id = Uuid::from_u128(32);
        alias_request.actor.session_id = Uuid::from_u128(33);
        let mut replayed = response.clone();
        replayed.request = alias_request.clone();
        replayed.receipt.replayed = true;
        assert!(valid_intent_response(&replayed, &alias_request));
        replayed.receipt.replayed = false;
        assert!(!valid_intent_response(&replayed, &alias_request));

        let mut changed_revision = response.clone();
        changed_revision
            .receipt
            .destination_workspace_leadership_revision += 1;
        assert!(!valid_intent_response(&changed_revision, &request));

        let mut wrong_agent = response;
        wrong_agent.receipt.workspace_actor.agent_id = Uuid::from_u128(23);
        assert!(!valid_intent_response(&wrong_agent, &request));
    }

    #[test]
    fn consent_reuses_exact_kind_and_canonical_actor() {
        let request = TeamPermanentTransferConsentRequest {
            actor: actor(Uuid::from_u128(24), Uuid::from_u128(25)),
            command: TeamPermanentTransferConsentActionWire {
                receipt_id: Uuid::from_u128(26),
                transfer_id: Uuid::from_u128(5),
                kind: TeamPermanentTransferConsentKindWire::SourceHomeLeader,
            },
        };
        let response = TeamPermanentTransferConsentResponse {
            request: request.clone(),
            receipt: TeamPermanentTransferConsentReceiptWire {
                receipt_id: request.command.receipt_id,
                transfer_id: request.command.transfer_id,
                kind: request.command.kind,
                authority_revision: 6,
                membership_revision: 5,
                actor: actor_receipt(&request.actor, Uuid::from_u128(12), Uuid::from_u128(13)),
                recorded_at_ms: 101,
                replayed: false,
            },
        };
        assert!(valid_consent_request(&request));
        assert!(valid_consent_response(&response, &request));

        let mut alias_request = request.clone();
        alias_request.actor.activity_id = Uuid::from_u128(34);
        alias_request.actor.session_id = Uuid::from_u128(35);
        let mut replayed = response.clone();
        replayed.request = alias_request.clone();
        replayed.receipt.replayed = true;
        assert!(valid_consent_response(&replayed, &alias_request));
        replayed.receipt.replayed = false;
        assert!(!valid_consent_response(&replayed, &alias_request));

        let mut wrong_kind = response.clone();
        wrong_kind.receipt.kind = TeamPermanentTransferConsentKindWire::TargetAgentAcceptance;
        assert!(!valid_consent_response(&wrong_kind, &request));

        let mut wrong_actor = response;
        wrong_actor.receipt.actor.agent_id = Uuid::from_u128(27);
        assert!(!valid_consent_response(&wrong_actor, &request));
    }

    #[test]
    fn exact_consent_readback_keeps_original_intent_separate_from_reader()
    -> Result<(), Box<dyn std::error::Error>> {
        let intent = TeamPermanentTransferConsentRequest {
            actor: actor(Uuid::from_u128(36), Uuid::from_u128(37)),
            command: TeamPermanentTransferConsentActionWire {
                receipt_id: Uuid::from_u128(38),
                transfer_id: Uuid::from_u128(5),
                kind: TeamPermanentTransferConsentKindWire::TargetAgentAcceptance,
            },
        };
        let request = TeamPermanentTransferConsentReadbackRequest {
            intent: intent.clone(),
            current_actor: actor(Uuid::from_u128(39), Uuid::from_u128(40)),
        };
        let receipt = TeamPermanentTransferConsentReceiptWire {
            receipt_id: intent.command.receipt_id,
            transfer_id: intent.command.transfer_id,
            kind: intent.command.kind,
            authority_revision: 5,
            membership_revision: 5,
            actor: actor_receipt(&intent.actor, Uuid::from_u128(8), Uuid::from_u128(11)),
            recorded_at_ms: 101,
            replayed: false,
        };
        let response = TeamPermanentTransferConsentReadbackResponse {
            request: request.clone(),
            result: Some(receipt.clone()),
        };
        assert!(valid_consent_readback_request(&request));
        assert!(valid_consent_readback_response(&response, &request));
        assert_ne!(
            request.current_actor.session_id,
            request.intent.actor.session_id
        );
        let encoded = serde_json::to_value(&response)?;
        assert!(encoded["result"].get("sourceHomeLeaderConsent").is_none());
        assert!(encoded["result"].get("targetAgentAcceptance").is_none());

        let mut wrong_original_actor = response.clone();
        let Some(receipt) = wrong_original_actor.result.as_mut() else {
            return Err(io::Error::other("consent fixture must be present").into());
        };
        let foreign_consentor = actor(Uuid::from_u128(43), Uuid::from_u128(44));
        receipt.actor = actor_receipt(&foreign_consentor, Uuid::from_u128(45), Uuid::from_u128(46));
        assert!(!valid_consent_readback_response(
            &wrong_original_actor,
            &request
        ));

        let mut wrong_receipt = response.clone();
        wrong_receipt
            .result
            .as_mut()
            .ok_or_else(|| io::Error::other("consent fixture must be present"))?
            .receipt_id = Uuid::from_u128(41);
        assert!(!valid_consent_readback_response(&wrong_receipt, &request));

        let mut wrong_transfer = response.clone();
        wrong_transfer
            .result
            .as_mut()
            .ok_or_else(|| io::Error::other("consent fixture must be present"))?
            .transfer_id = Uuid::from_u128(43);
        assert!(!valid_consent_readback_response(&wrong_transfer, &request));

        let mut wrong_kind = response.clone();
        wrong_kind
            .result
            .as_mut()
            .ok_or_else(|| io::Error::other("consent fixture must be present"))?
            .kind = TeamPermanentTransferConsentKindWire::SourceHomeLeader;
        assert!(!valid_consent_readback_response(&wrong_kind, &request));

        let mut wrong_actor = response.clone();
        wrong_actor
            .result
            .as_mut()
            .ok_or_else(|| io::Error::other("consent fixture must be present"))?
            .actor
            .agent_id = Uuid::from_u128(42);
        assert!(!valid_consent_readback_response(&wrong_actor, &request));

        let absent = TeamPermanentTransferConsentReadbackResponse {
            request: request.clone(),
            result: None,
        };
        assert!(valid_consent_readback_response(&absent, &request));

        let mut missing = serde_json::to_value(response)?;
        let Some(object) = missing.as_object_mut() else {
            return Err(io::Error::other("consent response must be an object").into());
        };
        object.remove("result");
        assert!(
            serde_json::from_value::<TeamPermanentTransferConsentReadbackResponse>(missing)
                .is_err()
        );
        let null = serde_json::json!({ "request": request, "result": null });
        let parsed = serde_json::from_value::<TeamPermanentTransferConsentReadbackResponse>(null)?;
        assert!(parsed.result.is_none());
        Ok(())
    }

    #[test]
    fn snapshot_readback_is_scoped_exact_nullable_and_does_not_need_proposal_intent()
    -> Result<(), Box<dyn std::error::Error>> {
        let original = intent_request();
        let request = TeamPermanentTransferReadbackRequest {
            workspace_id: original.actor.scope.workspace_id,
            transfer_id: original.command.transfer_id,
            current_actor: actor(Uuid::from_u128(28), Uuid::from_u128(29)),
        };
        let encoded = serde_json::to_value(&request)?;
        assert!(encoded.get("intent").is_none());
        let response = TeamPermanentTransferReadbackResponse {
            request: request.clone(),
            result: Some(snapshot(&original)),
        };
        assert!(valid_readback_request(&request));
        assert!(valid_readback_response(&response, &request));

        let mut foreign = request.clone();
        foreign.current_actor.scope.workspace_id = Uuid::from_u128(30);
        assert!(!valid_readback_request(&foreign));

        let mut wrong_transfer = response.clone();
        let Some(snapshot) = wrong_transfer.result.as_mut() else {
            return Err(io::Error::other("snapshot fixture must be present").into());
        };
        snapshot.intent.transfer_id = Uuid::from_u128(31);
        assert!(!valid_readback_response(&wrong_transfer, &request));

        let absent = TeamPermanentTransferReadbackResponse {
            request: request.clone(),
            result: None,
        };
        assert!(valid_readback_response(&absent, &request));

        let mut missing = serde_json::to_value(response)?;
        let Some(object) = missing.as_object_mut() else {
            return Err(io::Error::other("fixture response must be an object").into());
        };
        object.remove("result");
        assert!(serde_json::from_value::<TeamPermanentTransferReadbackResponse>(missing).is_err());
        let null = serde_json::json!({ "request": request, "result": null });
        let parsed = serde_json::from_value::<TeamPermanentTransferReadbackResponse>(null)?;
        assert!(parsed.result.is_none());
        Ok(())
    }
}
