//! Strict wire requests and receipts for the Operations PREPARING boundary.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{TeamExpansionActorReceiptWire, TeamExpansionActorWire, valid_actor};
use crate::wire::team_expansion::TeamExpansionActorReceiptWire as ActorReceipt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferPreparationActionWire {
    pub preparation_id: Uuid,
    pub transfer_id: Uuid,
    pub expected_generation: u64,
    pub expected_workspace_leadership_revision: i64,
    pub source_leader_consent_id: Uuid,
    pub target_acceptance_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferPreparationRequest {
    pub actor: TeamExpansionActorWire,
    pub command: TeamPermanentTransferPreparationActionWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamPermanentTransferPreparationStateWire {
    Preparing,
}

/// Non-secret principal identity used to validate an effect binding at Forge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum TeamPermanentTransferCapturedPrincipalWire {
    TrustedStdio,
    DirectToken { token_id: Uuid },
    OAuthGrant { grant_id: Uuid },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamPermanentTransferBindingOriginWire {
    Registration,
    LegacyAdoption,
}

/// One exact current W effect binding in the original sealed manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferBindingWire {
    pub participant_id: Uuid,
    pub activity_id: Uuid,
    pub agent_id: Uuid,
    pub session_id: Uuid,
    pub source_request_id: Uuid,
    pub origin: TeamPermanentTransferBindingOriginWire,
    pub principal: TeamPermanentTransferCapturedPrincipalWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferPreparationReceiptWire {
    pub preparation_id: Uuid,
    pub workspace_id: Uuid,
    pub transfer_id: Uuid,
    pub source_team_id: Uuid,
    pub destination_team_id: Uuid,
    pub generation: u64,
    pub workspace_leadership_revision: i64,
    pub source_leader_consent_id: Uuid,
    pub target_acceptance_id: Uuid,
    pub target_participation_id: Uuid,
    pub target_profile_id: Uuid,
    pub target_participant_ids: Vec<Uuid>,
    pub identity_version: u8,
    pub participation_revision: u64,
    pub source_membership_revision: u64,
    pub intent_digest: String,
    pub payload_digest: String,
    pub actor: TeamExpansionActorReceiptWire,
    pub recorded_at_ms: i64,
    pub session_count: u16,
    pub binding_count: u16,
    pub state: TeamPermanentTransferPreparationStateWire,
    pub replayed: bool,
    pub sessions: Vec<Uuid>,
    /// Exact sorted current effect bindings in the immutable source capture.
    pub bindings: Vec<TeamPermanentTransferBindingWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferPreparationResponse {
    pub request: TeamPermanentTransferPreparationRequest,
    pub receipt: TeamPermanentTransferPreparationReceiptWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferPreparationReadbackRequest {
    pub workspace_id: Uuid,
    pub preparation_id: Uuid,
    pub current_actor: TeamExpansionActorWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamPermanentTransferPreparationReadbackResponse {
    pub request: TeamPermanentTransferPreparationReadbackRequest,
    #[serde(deserialize_with = "super::deserialize_required_option")]
    pub result: Option<TeamPermanentTransferPreparationReceiptWire>,
}

#[must_use]
pub fn valid_preparation_request(request: &TeamPermanentTransferPreparationRequest) -> bool {
    let command = request.command;
    valid_actor(&request.actor)
        && valid_scope_locator(&request.actor)
        && !command.preparation_id.is_nil()
        && !command.transfer_id.is_nil()
        && command.preparation_id != command.transfer_id
        && command.expected_generation > 0
        && i64::try_from(command.expected_generation).is_ok()
        && command.expected_workspace_leadership_revision > 0
        && !command.source_leader_consent_id.is_nil()
        && !command.target_acceptance_id.is_nil()
        && command.source_leader_consent_id != command.target_acceptance_id
}

#[must_use]
pub fn valid_preparation_receipt(
    receipt: &TeamPermanentTransferPreparationReceiptWire,
    request: &TeamPermanentTransferPreparationRequest,
) -> bool {
    let command = request.command;
    receipt.state == TeamPermanentTransferPreparationStateWire::Preparing
        && receipt.preparation_id == command.preparation_id
        && receipt.workspace_id == request.actor.scope.workspace_id
        && receipt.transfer_id == command.transfer_id
        && !receipt.source_team_id.is_nil()
        && !receipt.destination_team_id.is_nil()
        && receipt.source_team_id != receipt.destination_team_id
        && receipt.generation == command.expected_generation
        && receipt.workspace_leadership_revision == command.expected_workspace_leadership_revision
        && receipt.source_leader_consent_id == command.source_leader_consent_id
        && receipt.target_acceptance_id == command.target_acceptance_id
        && !receipt.target_participation_id.is_nil()
        && !receipt.target_profile_id.is_nil()
        && valid_target_participants(&receipt.target_participant_ids)
        && receipt.identity_version == 1
        && receipt.participation_revision > 0
        && i64::try_from(receipt.participation_revision).is_ok()
        && receipt.source_membership_revision > 0
        && i64::try_from(receipt.source_membership_revision).is_ok()
        && valid_digest(&receipt.intent_digest)
        && valid_digest(&receipt.payload_digest)
        && valid_manifest(receipt)
        && valid_actor_receipt(&receipt.actor, receipt.workspace_id)
        && receipt.actor.activity_id == request.actor.activity_id
        && receipt.actor.session_id == request.actor.session_id
        && receipt.actor.agent_id
            == super::member_agent_id(
                request.actor.scope.workspace_id,
                request.actor.activity_id,
                request.actor.session_id,
            )
            .into_uuid()
        && receipt.recorded_at_ms >= 0
}

#[must_use]
pub fn valid_preparation_response(
    response: &TeamPermanentTransferPreparationResponse,
    request: &TeamPermanentTransferPreparationRequest,
) -> bool {
    &response.request == request
        && valid_preparation_request(request)
        && valid_preparation_receipt(&response.receipt, request)
}

#[must_use]
pub fn valid_preparation_readback_request(
    request: &TeamPermanentTransferPreparationReadbackRequest,
) -> bool {
    !request.workspace_id.is_nil()
        && !request.preparation_id.is_nil()
        && valid_actor(&request.current_actor)
        && valid_scope_locator(&request.current_actor)
        && request.current_actor.scope.workspace_id == request.workspace_id
}

#[must_use]
pub fn valid_preparation_readback_response(
    response: &TeamPermanentTransferPreparationReadbackResponse,
    request: &TeamPermanentTransferPreparationReadbackRequest,
) -> bool {
    &response.request == request
        && response
            .result
            .as_ref()
            .is_none_or(|receipt| valid_preparation_receipt_for_selector(receipt, request))
}

/// Verify one authorized readback proves the exact retained preparation.
#[must_use]
pub fn valid_exact_preparation_recovery(
    original: &TeamPermanentTransferPreparationRequest,
    receipt: &TeamPermanentTransferPreparationReceiptWire,
) -> bool {
    valid_preparation_request(original)
        && valid_preparation_receipt(receipt, original)
        && receipt.actor.activity_id == original.actor.activity_id
        && receipt.actor.session_id == original.actor.session_id
        && receipt.actor.agent_id
            == super::member_agent_id(
                original.actor.scope.workspace_id,
                original.actor.activity_id,
                original.actor.session_id,
            )
            .into_uuid()
}

fn valid_preparation_receipt_for_selector(
    receipt: &TeamPermanentTransferPreparationReceiptWire,
    request: &TeamPermanentTransferPreparationReadbackRequest,
) -> bool {
    receipt.preparation_id == request.preparation_id
        && receipt.workspace_id == request.workspace_id
        && !receipt.transfer_id.is_nil()
        && !receipt.source_team_id.is_nil()
        && !receipt.destination_team_id.is_nil()
        && receipt.source_team_id != receipt.destination_team_id
        && receipt.generation > 0
        && i64::try_from(receipt.generation).is_ok()
        && receipt.workspace_leadership_revision > 0
        && !receipt.source_leader_consent_id.is_nil()
        && !receipt.target_acceptance_id.is_nil()
        && receipt.source_leader_consent_id != receipt.target_acceptance_id
        && !receipt.target_participation_id.is_nil()
        && !receipt.target_profile_id.is_nil()
        && valid_target_participants(&receipt.target_participant_ids)
        && receipt.identity_version == 1
        && receipt.participation_revision > 0
        && i64::try_from(receipt.participation_revision).is_ok()
        && receipt.source_membership_revision > 0
        && i64::try_from(receipt.source_membership_revision).is_ok()
        && valid_actor_receipt(&receipt.actor, request.workspace_id)
        && receipt.recorded_at_ms >= 0
        && valid_digest(&receipt.intent_digest)
        && valid_digest(&receipt.payload_digest)
        && valid_manifest(receipt)
        && receipt.state == TeamPermanentTransferPreparationStateWire::Preparing
}

fn valid_manifest(receipt: &TeamPermanentTransferPreparationReceiptWire) -> bool {
    if !valid_target_participants(&receipt.target_participant_ids)
        || !(1..=128).contains(&receipt.session_count)
        || receipt.sessions.len() != usize::from(receipt.session_count)
        || receipt.sessions.iter().any(Uuid::is_nil)
        || receipt.sessions.windows(2).any(|pair| pair[0] >= pair[1])
        || receipt.binding_count > 256
        || receipt.bindings.len() != usize::from(receipt.binding_count)
        || receipt.bindings.iter().any(|binding| {
            !receipt
                .target_participant_ids
                .contains(&binding.participant_id)
                || binding.activity_id.is_nil()
                || binding.agent_id.is_nil()
                || binding.session_id.is_nil()
                || binding.source_request_id.is_nil()
                || !receipt.sessions.contains(&binding.session_id)
                || !valid_principal(&binding.principal)
        })
        || receipt
            .bindings
            .windows(2)
            .any(|pair| pair[0].agent_id >= pair[1].agent_id)
        || receipt.bindings.iter().enumerate().any(|(index, binding)| {
            receipt.bindings[index + 1..].iter().any(|other| {
                other.activity_id == binding.activity_id && other.session_id == binding.session_id
            })
        })
    {
        return false;
    }
    receipt.bindings.iter().enumerate().all(|(index, binding)| {
        receipt.bindings[index + 1..]
            .iter()
            .filter(|other| other.session_id == binding.session_id)
            .all(|other| other.principal == binding.principal)
    })
}

fn valid_target_participants(participants: &[Uuid]) -> bool {
    !participants.is_empty()
        && participants.len() <= 128
        && participants.iter().all(|participant| !participant.is_nil())
        && participants.windows(2).all(|pair| pair[0] < pair[1])
}

fn valid_principal(principal: &TeamPermanentTransferCapturedPrincipalWire) -> bool {
    match principal {
        TeamPermanentTransferCapturedPrincipalWire::TrustedStdio => true,
        TeamPermanentTransferCapturedPrincipalWire::DirectToken { token_id }
        | TeamPermanentTransferCapturedPrincipalWire::OAuthGrant { grant_id: token_id } => {
            !token_id.is_nil()
        }
    }
}

fn valid_actor_receipt(receipt: &ActorReceipt, workspace_id: Uuid) -> bool {
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
            == super::member_agent_id(workspace_id, receipt.activity_id, receipt.session_id)
                .into_uuid()
        && !receipt.display_name.trim().is_empty()
        && receipt.display_name.len() <= 128
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_scope_locator(actor: &TeamExpansionActorWire) -> bool {
    !actor.scope.locator.is_empty()
        && actor.scope.locator.len() <= 4096
        && !actor.scope.locator.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::WorkspaceQueryScope;

    fn actor(session: u128) -> TeamExpansionActorWire {
        TeamExpansionActorWire {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            activity_id: Uuid::from_u128(3),
            session_id: Uuid::from_u128(session),
        }
    }

    fn request() -> TeamPermanentTransferPreparationRequest {
        TeamPermanentTransferPreparationRequest {
            actor: actor(4),
            command: TeamPermanentTransferPreparationActionWire {
                preparation_id: Uuid::from_u128(5),
                transfer_id: Uuid::from_u128(6),
                expected_generation: 2,
                expected_workspace_leadership_revision: 7,
                source_leader_consent_id: Uuid::from_u128(8),
                target_acceptance_id: Uuid::from_u128(9),
            },
        }
    }

    fn receipt(
        request: &TeamPermanentTransferPreparationRequest,
    ) -> TeamPermanentTransferPreparationReceiptWire {
        TeamPermanentTransferPreparationReceiptWire {
            preparation_id: request.command.preparation_id,
            workspace_id: request.actor.scope.workspace_id,
            transfer_id: request.command.transfer_id,
            source_team_id: Uuid::from_u128(40),
            destination_team_id: Uuid::from_u128(41),
            generation: request.command.expected_generation,
            workspace_leadership_revision: request.command.expected_workspace_leadership_revision,
            source_leader_consent_id: request.command.source_leader_consent_id,
            target_acceptance_id: request.command.target_acceptance_id,
            target_participation_id: Uuid::from_u128(10),
            target_profile_id: Uuid::from_u128(11),
            target_participant_ids: vec![Uuid::from_u128(15), Uuid::from_u128(16)],
            identity_version: 1,
            participation_revision: 2,
            source_membership_revision: 5,
            intent_digest: "ab".repeat(32),
            payload_digest: "cd".repeat(32),
            sessions: vec![
                Uuid::from_u128(20),
                Uuid::from_u128(21),
                Uuid::from_u128(22),
            ],
            bindings: vec![
                TeamPermanentTransferBindingWire {
                    participant_id: Uuid::from_u128(15),
                    activity_id: Uuid::from_u128(30),
                    agent_id: Uuid::from_u128(31),
                    session_id: Uuid::from_u128(20),
                    source_request_id: Uuid::from_u128(32),
                    origin: TeamPermanentTransferBindingOriginWire::Registration,
                    principal: TeamPermanentTransferCapturedPrincipalWire::DirectToken {
                        token_id: Uuid::from_u128(33),
                    },
                },
                TeamPermanentTransferBindingWire {
                    participant_id: Uuid::from_u128(16),
                    activity_id: Uuid::from_u128(30),
                    agent_id: Uuid::from_u128(34),
                    session_id: Uuid::from_u128(21),
                    source_request_id: Uuid::from_u128(35),
                    origin: TeamPermanentTransferBindingOriginWire::LegacyAdoption,
                    principal: TeamPermanentTransferCapturedPrincipalWire::DirectToken {
                        token_id: Uuid::from_u128(33),
                    },
                },
            ],
            actor: TeamExpansionActorReceiptWire {
                activity_id: request.actor.activity_id,
                agent_id: super::super::member_agent_id(
                    request.actor.scope.workspace_id,
                    request.actor.activity_id,
                    request.actor.session_id,
                )
                .into_uuid(),
                session_id: request.actor.session_id,
                participation_id: Uuid::from_u128(12),
                profile_id: Uuid::from_u128(13),
                participant_id: Uuid::from_u128(14),
                display_name: "Original Orchestrator".to_owned(),
            },
            recorded_at_ms: 20,
            session_count: 3,
            binding_count: 2,
            state: TeamPermanentTransferPreparationStateWire::Preparing,
            replayed: false,
        }
    }

    #[test]
    fn preparation_response_pins_consents_generation_actor_and_bounded_capture() {
        let request = request();
        assert!(valid_preparation_request(&request));
        let response = TeamPermanentTransferPreparationResponse {
            request: request.clone(),
            receipt: receipt(&request),
        };
        assert!(valid_preparation_response(&response, &request));

        let mut dropped_workspace_alias = response.clone();
        dropped_workspace_alias.receipt.target_participant_ids.pop();
        assert!(!valid_preparation_response(
            &dropped_workspace_alias,
            &request,
        ));

        let mut wrong_consent = response.clone();
        wrong_consent.receipt.target_acceptance_id = Uuid::from_u128(91);
        assert!(!valid_preparation_response(&wrong_consent, &request));

        let mut wrong_generation = response.clone();
        wrong_generation.receipt.generation += 1;
        assert!(!valid_preparation_response(&wrong_generation, &request));

        let mut wrong_actor = response.clone();
        wrong_actor.receipt.actor.session_id = Uuid::from_u128(92);
        assert!(!valid_preparation_response(&wrong_actor, &request));

        let mut oversized_capture = response;
        oversized_capture.receipt.session_count = 129;
        assert!(!valid_preparation_response(&oversized_capture, &request));
    }

    #[test]
    fn exact_readback_keeps_original_actor_separate_from_current_reader()
    -> Result<(), Box<dyn std::error::Error>> {
        let original = request();
        let receipt = receipt(&original);
        let readback = TeamPermanentTransferPreparationReadbackRequest {
            workspace_id: original.actor.scope.workspace_id,
            preparation_id: original.command.preparation_id,
            current_actor: actor(15),
        };
        assert!(valid_preparation_readback_request(&readback));
        let response = TeamPermanentTransferPreparationReadbackResponse {
            request: readback.clone(),
            result: Some(receipt.clone()),
        };
        assert!(valid_preparation_readback_response(&response, &readback));
        assert!(valid_exact_preparation_recovery(&original, &receipt));

        let mut changed_identity = original.clone();
        changed_identity.actor.session_id = Uuid::from_u128(16);
        assert!(!valid_exact_preparation_recovery(
            &changed_identity,
            &receipt
        ));

        let mut changed_digest = receipt.clone();
        changed_digest.payload_digest.make_ascii_uppercase();
        assert!(!valid_preparation_readback_response(
            &TeamPermanentTransferPreparationReadbackResponse {
                request: readback.clone(),
                result: Some(changed_digest),
            },
            &readback,
        ));

        let absent = TeamPermanentTransferPreparationReadbackResponse {
            request: readback.clone(),
            result: None,
        };
        assert!(valid_preparation_readback_response(&absent, &readback));

        let mut missing = serde_json::to_value(response)?;
        let object = missing
            .as_object_mut()
            .ok_or_else(|| std::io::Error::other("wire response is not an object"))?;
        object.remove("result");
        assert!(
            serde_json::from_value::<TeamPermanentTransferPreparationReadbackResponse>(missing)
                .is_err()
        );
        Ok(())
    }
}
