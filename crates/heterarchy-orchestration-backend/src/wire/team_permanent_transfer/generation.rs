//! Renewal and immutable generation readback wire contracts.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    TeamPermanentTransferIntentReceiptWire, TeamPermanentTransferIntentRequest,
    TeamPermanentTransferReadbackRequest, valid_intent_receipt,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamPermanentTransferGenerationStateWire {
    IntentOnly,
    Preparing,
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferGenerationWire {
    pub number: u64,
    #[serde(deserialize_with = "super::deserialize_required_option")]
    pub predecessor_transfer_id: Option<Uuid>,
    pub state: TeamPermanentTransferGenerationStateWire,
    pub recorded_at_ms: i64,
    pub head_transfer_id: Uuid,
    pub head_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferGenerationSnapshotWire {
    pub intent: TeamPermanentTransferIntentReceiptWire,
    pub generation: TeamPermanentTransferGenerationWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferRenewalRequest {
    pub proposal: TeamPermanentTransferIntentRequest,
    pub predecessor_transfer_id: Uuid,
    pub expected_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferRenewalResponse {
    pub request: TeamPermanentTransferRenewalRequest,
    pub receipt: TeamPermanentTransferGenerationSnapshotWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferGenerationReadbackResponse {
    pub request: TeamPermanentTransferReadbackRequest,
    #[serde(deserialize_with = "super::deserialize_required_option")]
    pub result: Option<TeamPermanentTransferGenerationSnapshotWire>,
}

#[must_use]
pub fn valid_renewal_request(request: &TeamPermanentTransferRenewalRequest) -> bool {
    super::valid_intent_request(&request.proposal)
        && !request.predecessor_transfer_id.is_nil()
        && request.predecessor_transfer_id != request.proposal.command.transfer_id
        && request.expected_generation > 0
        && request
            .expected_generation
            .checked_add(1)
            .is_some_and(|next| i64::try_from(next).is_ok())
        && valid_scope_locator(&request.proposal.actor)
}

#[must_use]
pub fn valid_renewal_response(
    response: &TeamPermanentTransferRenewalResponse,
    request: &TeamPermanentTransferRenewalRequest,
) -> bool {
    let generation = &response.receipt.generation;
    &response.request == request
        && valid_renewal_request(request)
        && valid_generation_snapshot(&response.receipt)
        && valid_intent_receipt(&response.receipt.intent, Some(&request.proposal))
        && generation.predecessor_transfer_id == Some(request.predecessor_transfer_id)
        && request.expected_generation.checked_add(1) == Some(generation.number)
        && match generation.state {
            TeamPermanentTransferGenerationStateWire::IntentOnly => true,
            TeamPermanentTransferGenerationStateWire::Preparing => response.receipt.intent.replayed,
            TeamPermanentTransferGenerationStateWire::Superseded => {
                response.receipt.intent.replayed
            }
        }
}

#[must_use]
pub fn valid_generation_readback_request(request: &TeamPermanentTransferReadbackRequest) -> bool {
    super::valid_readback_request(request) && valid_scope_locator(&request.current_actor)
}

#[must_use]
pub fn valid_generation_readback_response(
    response: &TeamPermanentTransferGenerationReadbackResponse,
    request: &TeamPermanentTransferReadbackRequest,
) -> bool {
    &response.request == request
        && response.result.as_ref().is_none_or(|snapshot| {
            valid_generation_snapshot(snapshot)
                && snapshot.intent.workspace_id == request.workspace_id
                && snapshot.intent.transfer_id == request.transfer_id
        })
}

/// Verify that one authorized child-generation read proves this exact renewal.
/// The service-authorized reader is intentionally separate from the proposal's
/// historically recorded Orchestrator actor.
#[must_use]
pub fn valid_exact_renewal_recovery(
    request: &TeamPermanentTransferRenewalRequest,
    renewed: &TeamPermanentTransferGenerationSnapshotWire,
) -> bool {
    let new = &renewed.intent;
    let new_generation = &renewed.generation;
    let proposal_actor = &request.proposal.actor;

    valid_renewal_request(request)
        && valid_generation_snapshot(renewed)
        && valid_intent_receipt(new, Some(&request.proposal))
        // Readback must prove the actor retained by the uncertain request even
        // if the stored receipt is being replayed to a different reader.
        && new.workspace_actor.activity_id == proposal_actor.activity_id
        && new.workspace_actor.session_id == proposal_actor.session_id
        && new.workspace_actor.agent_id
            == super::member_agent_id(
                proposal_actor.scope.workspace_id,
                proposal_actor.activity_id,
                proposal_actor.session_id,
            )
            .into_uuid()
        && new.transfer_id == request.proposal.command.transfer_id
        && new.workspace_id == proposal_actor.scope.workspace_id
        && new_generation.predecessor_transfer_id == Some(request.predecessor_transfer_id)
        && request.expected_generation.checked_add(1) == Some(new_generation.number)
}

fn valid_scope_locator(actor: &super::TeamExpansionActorWire) -> bool {
    let locator = &actor.scope.locator;
    !locator.is_empty() && locator.len() <= 4096 && !locator.chars().any(char::is_control)
}

fn valid_generation_snapshot(snapshot: &TeamPermanentTransferGenerationSnapshotWire) -> bool {
    let intent = &snapshot.intent;
    let generation = &snapshot.generation;
    let ancestry_valid = match (generation.number, generation.predecessor_transfer_id) {
        (1, None) => true,
        (number, Some(predecessor_id)) => {
            number > 1 && !predecessor_id.is_nil() && predecessor_id != intent.transfer_id
        }
        _ => false,
    };
    let head_valid = match generation.state {
        TeamPermanentTransferGenerationStateWire::IntentOnly
        | TeamPermanentTransferGenerationStateWire::Preparing => {
            generation.head_transfer_id == intent.transfer_id
                && generation.head_generation == generation.number
        }
        TeamPermanentTransferGenerationStateWire::Superseded => {
            generation.head_transfer_id != intent.transfer_id
                && generation.head_generation > generation.number
        }
    };

    valid_intent_receipt(intent, None)
        && generation.number > 0
        && i64::try_from(generation.number).is_ok()
        && ancestry_valid
        && !generation.head_transfer_id.is_nil()
        && generation.predecessor_transfer_id != Some(generation.head_transfer_id)
        && generation.head_generation > 0
        && i64::try_from(generation.head_generation).is_ok()
        && head_valid
        && generation.recorded_at_ms >= 0
        && generation.recorded_at_ms == intent.recorded_at_ms
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{
        WorkspaceQueryScope,
        team_expansion::{TeamExpansionActorReceiptWire, TeamExpansionActorWire},
        team_permanent_transfer::TeamPermanentTransferIntentActionWire,
    };

    fn actor(activity_id: u128, session_id: u128) -> TeamExpansionActorWire {
        TeamExpansionActorWire {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            activity_id: Uuid::from_u128(activity_id),
            session_id: Uuid::from_u128(session_id),
        }
    }

    fn actor_receipt(
        actor: &TeamExpansionActorWire,
        participation: u128,
        profile: u128,
    ) -> TeamExpansionActorReceiptWire {
        TeamExpansionActorReceiptWire {
            activity_id: actor.activity_id,
            agent_id: super::super::member_agent_id(
                actor.scope.workspace_id,
                actor.activity_id,
                actor.session_id,
            )
            .into_uuid(),
            session_id: actor.session_id,
            participation_id: Uuid::from_u128(participation),
            profile_id: Uuid::from_u128(profile),
            participant_id: Uuid::from_u128(50),
            display_name: "transfer actor".to_owned(),
        }
    }

    fn request() -> TeamPermanentTransferRenewalRequest {
        let actor = actor(10, 11);
        TeamPermanentTransferRenewalRequest {
            proposal: TeamPermanentTransferIntentRequest {
                actor,
                command: TeamPermanentTransferIntentActionWire {
                    transfer_id: Uuid::from_u128(20),
                    source_team_id: Uuid::from_u128(30),
                    destination_team_id: Uuid::from_u128(31),
                    target_participation_id: Uuid::from_u128(32),
                    destination_request_id: Uuid::from_u128(33),
                    destination_decision_id: Uuid::from_u128(34),
                    destination_ordinal: 1,
                    expected_destination_team_revision: 2,
                    expected_destination_capacity_revision: 3,
                    expected_workspace_leadership_revision: 4,
                    expected_source_membership_revision: 5,
                    expected_source_leadership_revision: 7,
                },
            },
            predecessor_transfer_id: Uuid::from_u128(21),
            expected_generation: 3,
        }
    }

    fn intent_receipt(
        proposal: &TeamPermanentTransferIntentRequest,
        transfer_id: Uuid,
        leader_participation: u128,
        leader_profile: u128,
        leadership_revision: i64,
        recorded_at_ms: i64,
    ) -> TeamPermanentTransferIntentReceiptWire {
        TeamPermanentTransferIntentReceiptWire {
            transfer_id,
            workspace_id: proposal.actor.scope.workspace_id,
            source_team_id: proposal.command.source_team_id,
            destination_team_id: proposal.command.destination_team_id,
            target_participation_id: proposal.command.target_participation_id,
            target_profile_id: Uuid::from_u128(40),
            source_membership_revision: proposal.command.expected_source_membership_revision,
            source_leadership_revision: leadership_revision,
            source_leader_participation_id: Uuid::from_u128(leader_participation),
            source_leader_profile_id: Uuid::from_u128(leader_profile),
            destination_team_revision: proposal.command.expected_destination_team_revision,
            destination_capacity_revision: proposal.command.expected_destination_capacity_revision,
            destination_workspace_leadership_revision: proposal
                .command
                .expected_workspace_leadership_revision,
            destination_request_id: proposal.command.destination_request_id,
            destination_decision_id: proposal.command.destination_decision_id,
            destination_ordinal: proposal.command.destination_ordinal,
            workspace_actor: actor_receipt(&proposal.actor, 41, 42),
            recorded_at_ms,
            replayed: false,
        }
    }

    fn snapshot(
        intent: TeamPermanentTransferIntentReceiptWire,
        number: u64,
        predecessor: Option<Uuid>,
        state: TeamPermanentTransferGenerationStateWire,
        head_transfer_id: Uuid,
        head_generation: u64,
    ) -> TeamPermanentTransferGenerationSnapshotWire {
        TeamPermanentTransferGenerationSnapshotWire {
            generation: TeamPermanentTransferGenerationWire {
                number,
                predecessor_transfer_id: predecessor,
                state,
                recorded_at_ms: intent.recorded_at_ms,
                head_transfer_id,
                head_generation,
            },
            intent,
        }
    }

    fn recovered_pair() -> (
        TeamPermanentTransferRenewalRequest,
        TeamPermanentTransferGenerationSnapshotWire,
    ) {
        let request = request();
        let renewed = snapshot(
            intent_receipt(
                &request.proposal,
                request.proposal.command.transfer_id,
                45,
                46,
                7,
                101,
            ),
            request.expected_generation + 1,
            Some(request.predecessor_transfer_id),
            TeamPermanentTransferGenerationStateWire::IntentOnly,
            request.proposal.command.transfer_id,
            request.expected_generation + 1,
        );
        (request, renewed)
    }

    #[test]
    fn renewal_echo_and_generation_metadata_are_correlated() {
        let (request, receipt) = recovered_pair();
        let response = TeamPermanentTransferRenewalResponse {
            request: request.clone(),
            receipt: receipt.clone(),
        };
        assert!(valid_renewal_request(&request));
        assert!(valid_renewal_response(&response, &request));

        let mut wrong_parent = response.clone();
        wrong_parent.receipt.generation.predecessor_transfer_id = Some(Uuid::from_u128(99));
        assert!(!valid_renewal_response(&wrong_parent, &request));

        let mut wrong_head = response.clone();
        wrong_head.receipt.generation.head_generation += 1;
        assert!(!valid_renewal_response(&wrong_head, &request));

        let mut superseded_replay = response.clone();
        superseded_replay.receipt.intent.replayed = true;
        superseded_replay.receipt.generation.state =
            TeamPermanentTransferGenerationStateWire::Superseded;
        superseded_replay.receipt.generation.head_transfer_id = Uuid::from_u128(98);
        superseded_replay.receipt.generation.head_generation += 1;
        assert!(valid_renewal_response(&superseded_replay, &request));

        let mut fresh_superseded = superseded_replay.clone();
        fresh_superseded.receipt.intent.replayed = false;
        assert!(!valid_renewal_response(&fresh_superseded, &request));

        let mut malformed_superseded_head = superseded_replay;
        malformed_superseded_head
            .receipt
            .generation
            .head_transfer_id = malformed_superseded_head.receipt.intent.transfer_id;
        assert!(!valid_renewal_response(
            &malformed_superseded_head,
            &request
        ));

        let mut wrong_request = request.clone();
        wrong_request
            .proposal
            .command
            .expected_source_leadership_revision += 1;
        assert!(!valid_renewal_response(&response, &wrong_request));
    }

    #[test]
    fn exact_recovery_uses_one_child_snapshot_and_retained_proposal_actor() {
        let (request, renewed) = recovered_pair();
        assert!(valid_exact_renewal_recovery(&request, &renewed));

        let mut later_superseded = renewed.clone();
        later_superseded.generation.state = TeamPermanentTransferGenerationStateWire::Superseded;
        later_superseded.generation.head_transfer_id = Uuid::from_u128(98);
        later_superseded.generation.head_generation += 1;
        assert!(valid_exact_renewal_recovery(&request, &later_superseded));

        let mut wrong_slot = renewed.clone();
        wrong_slot.intent.destination_decision_id = Uuid::from_u128(80);
        assert!(!valid_exact_renewal_recovery(&request, &wrong_slot));

        let mut wrong_revision = renewed.clone();
        wrong_revision.intent.destination_team_revision += 1;
        assert!(!valid_exact_renewal_recovery(&request, &wrong_revision));

        let mut wrong_historical_actor = renewed.clone();
        wrong_historical_actor.intent.workspace_actor.session_id = Uuid::from_u128(81);
        assert!(!valid_exact_renewal_recovery(
            &request,
            &wrong_historical_actor
        ));

        let mut wrong_historical_agent = renewed;
        wrong_historical_agent.intent.workspace_actor.agent_id = Uuid::from_u128(82);
        assert!(!valid_exact_renewal_recovery(
            &request,
            &wrong_historical_agent
        ));
    }

    #[test]
    fn generation_readback_is_scoped_to_the_requested_route_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let (renewal, renewed) = recovered_pair();
        let request = TeamPermanentTransferReadbackRequest {
            workspace_id: renewal.proposal.actor.scope.workspace_id,
            transfer_id: renewed.intent.transfer_id,
            current_actor: actor(50, 51),
        };
        let response = TeamPermanentTransferGenerationReadbackResponse {
            request: request.clone(),
            result: Some(renewed),
        };
        assert!(valid_generation_readback_request(&request));
        assert!(valid_generation_readback_response(&response, &request));

        let mut foreign = response.clone();
        foreign
            .result
            .as_mut()
            .ok_or("fixture is absent")?
            .intent
            .transfer_id = Uuid::from_u128(99);
        assert!(!valid_generation_readback_response(&foreign, &request));

        let mut invalid_reader = request.clone();
        invalid_reader.current_actor.scope.locator = "bad\nlocator".to_owned();
        assert!(!valid_generation_readback_request(&invalid_reader));
        Ok(())
    }

    #[test]
    fn nullable_generation_fields_must_be_present_on_the_wire()
    -> Result<(), Box<dyn std::error::Error>> {
        let (request, receipt) = recovered_pair();
        let mut value =
            serde_json::to_value(TeamPermanentTransferRenewalResponse { request, receipt })?;
        let generation = value
            .get_mut("receipt")
            .and_then(|receipt| receipt.get_mut("generation"))
            .and_then(serde_json::Value::as_object_mut)
            .ok_or("generation object missing")?;
        generation.remove("predecessorTransferId");
        assert!(serde_json::from_value::<TeamPermanentTransferRenewalResponse>(value).is_err());

        let readback = TeamPermanentTransferGenerationReadbackResponse {
            request: TeamPermanentTransferReadbackRequest {
                workspace_id: Uuid::from_u128(1),
                transfer_id: Uuid::from_u128(2),
                current_actor: actor(3, 4),
            },
            result: None,
        };
        let mut value = serde_json::to_value(readback)?;
        value
            .as_object_mut()
            .ok_or("readback response object missing")?
            .remove("result");
        assert!(
            serde_json::from_value::<TeamPermanentTransferGenerationReadbackResponse>(value)
                .is_err()
        );
        Ok(())
    }
}
