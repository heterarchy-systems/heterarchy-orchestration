//! Strict wire contracts for deferring an existing T2 checkpoint.
//!
//! A defer and its next-check plan are administrative observations, not
//! independent evidence, an adjudication, a score, or a Leader decision.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::WorkspaceQueryScope;
use super::mission_evaluation::MissionEvaluationMutationContextWire;

const MAX_REASON_BYTES: usize = 4_096;
const MAX_NEXT_CHECK_BYTES: usize = 2_048;
const MAX_SQLITE_INTEGER: u64 = i64::MAX as u64;

/// Caller-owned intent to defer one exact, already started plan revision.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionT2DeferRequest {
    pub context: MissionEvaluationMutationContextWire,
    pub request_id: Uuid,
    pub checkpoint_id: Uuid,
    pub mission_id: Uuid,
    pub plan_id: Uuid,
    pub expected_revision: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expected_previous_checkpoint_id: Option<Uuid>,
    pub reason: String,
    pub next_check_condition: String,
    pub next_check_at_ms: i64,
}

/// Authenticated Workspace read actor, distinct from the original defer author.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionT2CheckpointReaderWire {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub sender_activity_id: Uuid,
    pub expected_workspace_leadership_revision: i64,
}

/// Source-free immutable intent plus the current authenticated read actor.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionT2DeferReadbackRequest {
    pub intent: MissionT2DeferContentWire,
    pub reader: MissionT2CheckpointReaderWire,
}

/// Immutable caller content accepted by an O2 recovery query.
///
/// It contains no O1 Activity, Agent, Session, or leadership epoch. Operations
/// supplies those original source facts from its receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionT2DeferContentWire {
    pub request_id: Uuid,
    pub checkpoint_id: Uuid,
    pub workspace_id: Uuid,
    pub mission_id: Uuid,
    pub plan_id: Uuid,
    pub expected_revision: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expected_previous_checkpoint_id: Option<Uuid>,
    pub reason: String,
    pub next_check_condition: String,
    pub next_check_at_ms: i64,
}

/// Closed provenance class for the original accountable Workspace actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionT2AccountableOwnerKindWire {
    ProfileParticipant,
    NativeParticipant,
}

/// Original accountable actor source recorded at the first successful commit.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionT2AccountableOwnerWire {
    pub activity_id: Uuid,
    pub agent_id: Uuid,
    pub session_id: Uuid,
    pub kind: MissionT2AccountableOwnerKindWire,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub participation_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub profile_id: Option<Uuid>,
    pub participant_id: Uuid,
    pub display_name: String,
    pub leadership_revision: i64,
}

/// Immutable Operations record for one deferred T2 check.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionT2DeferredCheckpointWire {
    pub request_id: Uuid,
    pub checkpoint_id: Uuid,
    pub workspace_id: Uuid,
    pub mission_id: Uuid,
    pub plan_id: Uuid,
    pub revision: u64,
    pub sequence: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub previous_checkpoint_id: Option<Uuid>,
    pub reason: String,
    pub next_check_condition: String,
    pub next_check_at_ms: i64,
    /// Operations-sampled server time; callers cannot set this field.
    pub recorded_at_ms: i64,
    pub accountable_owner: MissionT2AccountableOwnerWire,
}

/// Result of a new T2 defer or its exact idempotent replay.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionT2DeferResponse {
    pub scope: WorkspaceQueryScope,
    pub request: MissionT2DeferRequest,
    pub checkpoint: MissionT2DeferredCheckpointWire,
    pub replayed: bool,
}

/// Scoped exact request lookup; absence never grants blind-retry authority.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionT2DeferReadbackResponse {
    pub request: MissionT2DeferReadbackRequest,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub checkpoint: Option<MissionT2DeferredCheckpointWire>,
}

#[must_use]
pub fn valid_defer_request(request: &MissionT2DeferRequest) -> bool {
    valid_mutation_context(&request.context)
        && !request.request_id.is_nil()
        && !request.checkpoint_id.is_nil()
        && !request.mission_id.is_nil()
        && !request.plan_id.is_nil()
        && valid_revision(request.expected_revision)
        && request
            .expected_previous_checkpoint_id
            .is_none_or(|id| !id.is_nil())
        && valid_text(&request.reason, MAX_REASON_BYTES)
        && valid_text(&request.next_check_condition, MAX_NEXT_CHECK_BYTES)
        && request.next_check_at_ms >= 0
}

#[must_use]
pub fn valid_readback_request(request: &MissionT2DeferReadbackRequest) -> bool {
    valid_defer_content(&request.intent)
        && request.reader.scope.workspace_id == request.intent.workspace_id
        && valid_read_scope(&request.reader.scope, request.reader.session_id)
        && !request.reader.sender_activity_id.is_nil()
        && request.reader.expected_workspace_leadership_revision >= 1
}

#[must_use]
pub fn valid_defer_response(
    response: &MissionT2DeferResponse,
    request: &MissionT2DeferRequest,
) -> bool {
    response.scope == request.context.scope
        && response.request == *request
        && valid_checkpoint_content(&response.checkpoint, &defer_content(request))
        && (response.replayed
            || (response.checkpoint.accountable_owner.activity_id
                == request.context.sender_activity_id
                && response.checkpoint.accountable_owner.session_id == request.context.session_id
                && response.checkpoint.accountable_owner.leadership_revision
                    == request.context.expected_workspace_leadership_revision))
}

#[must_use]
pub fn valid_readback_response(
    response: &MissionT2DeferReadbackResponse,
    request: &MissionT2DeferReadbackRequest,
) -> bool {
    response.request == *request
        && response
            .checkpoint
            .as_ref()
            .is_none_or(|checkpoint| valid_checkpoint_content(checkpoint, &request.intent))
}

fn valid_checkpoint_content(
    checkpoint: &MissionT2DeferredCheckpointWire,
    intent: &MissionT2DeferContentWire,
) -> bool {
    checkpoint.request_id == intent.request_id
        && checkpoint.checkpoint_id == intent.checkpoint_id
        && checkpoint.workspace_id == intent.workspace_id
        && checkpoint.mission_id == intent.mission_id
        && checkpoint.plan_id == intent.plan_id
        && checkpoint.revision == intent.expected_revision
        && checkpoint.sequence <= MAX_SQLITE_INTEGER
        && checkpoint
            .previous_checkpoint_id
            .is_none_or(|id| !id.is_nil())
        && checkpoint.previous_checkpoint_id == intent.expected_previous_checkpoint_id
        && checkpoint.reason == intent.reason
        && checkpoint.next_check_condition == intent.next_check_condition
        && checkpoint.next_check_at_ms == intent.next_check_at_ms
        && checkpoint.recorded_at_ms >= 0
        && checkpoint.next_check_at_ms > checkpoint.recorded_at_ms
        && valid_owner(&checkpoint.accountable_owner, checkpoint.workspace_id)
}

fn defer_content(request: &MissionT2DeferRequest) -> MissionT2DeferContentWire {
    MissionT2DeferContentWire {
        request_id: request.request_id,
        checkpoint_id: request.checkpoint_id,
        workspace_id: request.context.scope.workspace_id,
        mission_id: request.mission_id,
        plan_id: request.plan_id,
        expected_revision: request.expected_revision,
        expected_previous_checkpoint_id: request.expected_previous_checkpoint_id,
        reason: request.reason.clone(),
        next_check_condition: request.next_check_condition.clone(),
        next_check_at_ms: request.next_check_at_ms,
    }
}

fn valid_defer_content(content: &MissionT2DeferContentWire) -> bool {
    !content.request_id.is_nil()
        && !content.checkpoint_id.is_nil()
        && !content.workspace_id.is_nil()
        && !content.mission_id.is_nil()
        && !content.plan_id.is_nil()
        && valid_revision(content.expected_revision)
        && content
            .expected_previous_checkpoint_id
            .is_none_or(|id| !id.is_nil())
        && valid_text(&content.reason, MAX_REASON_BYTES)
        && valid_text(&content.next_check_condition, MAX_NEXT_CHECK_BYTES)
        && content.next_check_at_ms >= 0
}

fn valid_owner(owner: &MissionT2AccountableOwnerWire, workspace_id: Uuid) -> bool {
    !owner.activity_id.is_nil()
        && !owner.session_id.is_nil()
        && !owner.participant_id.is_nil()
        && owner.leadership_revision >= 1
        && owner.agent_id
            == heterarchy_orchestration_runtime::leadership::member_agent_id(
                workspace_id,
                owner.activity_id,
                owner.session_id,
            )
            .into_uuid()
        && valid_text(&owner.display_name, 128)
        && match owner.kind {
            MissionT2AccountableOwnerKindWire::ProfileParticipant => {
                owner.participation_id.is_some_and(|id| !id.is_nil())
                    && owner.profile_id.is_some_and(|id| !id.is_nil())
            }
            MissionT2AccountableOwnerKindWire::NativeParticipant => {
                owner.participation_id.is_none() && owner.profile_id.is_none()
            }
        }
}

fn valid_mutation_context(context: &MissionEvaluationMutationContextWire) -> bool {
    valid_read_scope(&context.scope, context.session_id)
        && !context.sender_activity_id.is_nil()
        && context.expected_workspace_leadership_revision >= 1
}

fn valid_read_scope(scope: &WorkspaceQueryScope, session_id: Uuid) -> bool {
    !scope.workspace_id.is_nil()
        && !scope.location_id.is_nil()
        && scope.location_revision > 0
        && scope.workspace_revision > 0
        && valid_text(&scope.locator, 256)
        && !session_id.is_nil()
}

fn valid_revision(revision: u64) -> bool {
    (1..=MAX_SQLITE_INTEGER).contains(&revision)
}

fn valid_text(value: &str, max_bytes: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max_bytes && !value.as_bytes().contains(&0)
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
    use serde_json::json;

    fn scope() -> WorkspaceQueryScope {
        WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 3,
            workspace_revision: 4,
            locator: "workspace".to_owned(),
        }
    }

    fn request() -> MissionT2DeferRequest {
        MissionT2DeferRequest {
            context: MissionEvaluationMutationContextWire {
                scope: scope(),
                session_id: Uuid::from_u128(5),
                sender_activity_id: Uuid::from_u128(6),
                expected_workspace_leadership_revision: 7,
            },
            request_id: Uuid::from_u128(8),
            checkpoint_id: Uuid::from_u128(9),
            mission_id: Uuid::from_u128(10),
            plan_id: Uuid::from_u128(11),
            expected_revision: 12,
            expected_previous_checkpoint_id: None,
            reason: "independent review pending".to_owned(),
            next_check_condition: "read reviewer receipt".to_owned(),
            next_check_at_ms: 100,
        }
    }

    fn defer_content(intent: &MissionT2DeferRequest) -> MissionT2DeferContentWire {
        MissionT2DeferContentWire {
            request_id: intent.request_id,
            checkpoint_id: intent.checkpoint_id,
            workspace_id: intent.context.scope.workspace_id,
            mission_id: intent.mission_id,
            plan_id: intent.plan_id,
            expected_revision: intent.expected_revision,
            expected_previous_checkpoint_id: intent.expected_previous_checkpoint_id,
            reason: intent.reason.clone(),
            next_check_condition: intent.next_check_condition.clone(),
            next_check_at_ms: intent.next_check_at_ms,
        }
    }

    fn deferred_checkpoint(intent: &MissionT2DeferContentWire) -> MissionT2DeferredCheckpointWire {
        let activity_id = Uuid::from_u128(30);
        let session_id = Uuid::from_u128(31);
        MissionT2DeferredCheckpointWire {
            request_id: intent.request_id,
            checkpoint_id: intent.checkpoint_id,
            workspace_id: intent.workspace_id,
            mission_id: intent.mission_id,
            plan_id: intent.plan_id,
            revision: intent.expected_revision,
            sequence: 0,
            previous_checkpoint_id: None,
            reason: intent.reason.clone(),
            next_check_condition: intent.next_check_condition.clone(),
            next_check_at_ms: intent.next_check_at_ms,
            recorded_at_ms: 40,
            accountable_owner: MissionT2AccountableOwnerWire {
                activity_id,
                agent_id: heterarchy_orchestration_runtime::leadership::member_agent_id(
                    intent.workspace_id,
                    activity_id,
                    session_id,
                )
                .into_uuid(),
                session_id,
                kind: MissionT2AccountableOwnerKindWire::ProfileParticipant,
                participation_id: Some(Uuid::from_u128(14)),
                profile_id: Some(Uuid::from_u128(15)),
                participant_id: Uuid::from_u128(16),
                display_name: "Workspace Orchestrator".to_owned(),
                leadership_revision: 32,
            },
        }
    }

    #[test]
    fn defer_request_is_closed_camel_case_and_has_no_server_time()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = request();
        let value = serde_json::to_value(&request)?;
        assert_eq!(
            serde_json::from_value::<MissionT2DeferRequest>(value.clone())?,
            request
        );
        assert!(value.get("recordedAtMs").is_none());
        let mut unknown = value;
        unknown["callerAgentId"] = json!(Uuid::from_u128(13));
        assert!(serde_json::from_value::<MissionT2DeferRequest>(unknown).is_err());
        Ok(())
    }

    #[test]
    fn nullable_predecessor_and_readback_checkpoint_are_required_keys()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = request();
        let mut mutation_value = serde_json::to_value(&request)?;
        mutation_value
            .as_object_mut()
            .ok_or("object")?
            .remove("expectedPreviousCheckpointId");
        assert!(serde_json::from_value::<MissionT2DeferRequest>(mutation_value).is_err());
        let mut value = serde_json::to_value(defer_content(&request))?;
        value
            .as_object_mut()
            .ok_or("object")?
            .remove("expectedPreviousCheckpointId");
        assert!(serde_json::from_value::<MissionT2DeferContentWire>(value).is_err());
        let readback = MissionT2DeferReadbackResponse {
            request: MissionT2DeferReadbackRequest {
                intent: defer_content(&request),
                reader: MissionT2CheckpointReaderWire {
                    scope: scope(),
                    session_id: Uuid::from_u128(5),
                    sender_activity_id: Uuid::from_u128(6),
                    expected_workspace_leadership_revision: 7,
                },
            },
            checkpoint: None,
        };
        let mut value = serde_json::to_value(readback)?;
        value.as_object_mut().ok_or("object")?.remove("checkpoint");
        assert!(serde_json::from_value::<MissionT2DeferReadbackResponse>(value).is_err());
        Ok(())
    }

    #[test]
    fn readback_accepts_source_proof_separate_from_current_reader() {
        let original = request();
        let intent = defer_content(&original);
        let checkpoint = deferred_checkpoint(&intent);
        let readback = MissionT2DeferReadbackRequest {
            intent: intent.clone(),
            reader: MissionT2CheckpointReaderWire {
                scope: WorkspaceQueryScope {
                    workspace_id: intent.workspace_id,
                    location_id: Uuid::from_u128(20),
                    location_revision: 21,
                    workspace_revision: 22,
                    locator: "current-workspace-view".to_owned(),
                },
                session_id: Uuid::from_u128(23),
                sender_activity_id: Uuid::from_u128(24),
                expected_workspace_leadership_revision: 25,
            },
        };
        let response = MissionT2DeferReadbackResponse {
            request: readback.clone(),
            checkpoint: Some(checkpoint.clone()),
        };
        assert!(valid_readback_response(&response, &readback));

        let mut invalid_owner = checkpoint.clone();
        invalid_owner.accountable_owner.activity_id = Uuid::nil();
        assert!(!valid_readback_response(
            &MissionT2DeferReadbackResponse {
                request: readback.clone(),
                checkpoint: Some(invalid_owner),
            },
            &readback,
        ));
        let mut wrong_agent = checkpoint;
        wrong_agent.accountable_owner.activity_id = Uuid::from_u128(30);
        wrong_agent.accountable_owner.agent_id = Uuid::from_u128(32);
        assert!(!valid_readback_response(
            &MissionT2DeferReadbackResponse {
                request: readback.clone(),
                checkpoint: Some(wrong_agent),
            },
            &readback,
        ));
    }

    #[test]
    fn direct_replay_may_return_original_actor_attribution() {
        let original = request();
        let checkpoint = deferred_checkpoint(&defer_content(&original));
        let mut rebound_request = original.clone();
        rebound_request.context.sender_activity_id = Uuid::from_u128(30);
        rebound_request.context.session_id = Uuid::from_u128(31);
        let replay = MissionT2DeferResponse {
            scope: rebound_request.context.scope.clone(),
            request: rebound_request.clone(),
            checkpoint: checkpoint.clone(),
            replayed: true,
        };
        assert!(valid_defer_response(&replay, &rebound_request));
        let falsely_new = MissionT2DeferResponse {
            replayed: false,
            ..replay
        };
        assert!(!valid_defer_response(&falsely_new, &rebound_request));
    }
}
