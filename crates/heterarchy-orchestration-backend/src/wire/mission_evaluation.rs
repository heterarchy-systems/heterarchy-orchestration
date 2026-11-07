//! Strict HTTP contracts for durable Mission planning and request readback.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::WorkspaceQueryScope;

/// Authenticated caller context for a Mission mutation.
///
/// There is deliberately no actor kind or Agent ID in this DTO. The runtime
/// derives the Ready participant from this Session and Activity pair.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvaluationMutationContextWire {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub sender_activity_id: Uuid,
    pub expected_workspace_leadership_revision: i64,
}

/// Create one active Mission for an existing Team.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionCreateRequest {
    pub context: MissionEvaluationMutationContextWire,
    pub request_id: Uuid,
    pub mission_id: Uuid,
    pub team_id: Uuid,
    pub purpose: String,
}

/// Record one stable personal obligation under a Mission.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionObligationRecordRequest {
    pub context: MissionEvaluationMutationContextWire,
    pub request_id: Uuid,
    pub mission_id: Uuid,
    pub obligation_id: Uuid,
    pub participation_id: Uuid,
    pub work_unit_id: Uuid,
    pub weight: u64,
    pub acceptance_criteria: String,
    pub due_at_ms: i64,
}

/// Publish one immutable version of a Mission evaluation plan.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvaluationPlanPublishRequest {
    pub context: MissionEvaluationMutationContextWire,
    pub request_id: Uuid,
    pub mission_id: Uuid,
    pub plan_id: Uuid,
    pub version_id: Uuid,
    pub expected_head_revision: u64,
    pub t1_submission_cutoff_ms: i64,
    pub t2_checkpoint_ms: i64,
    pub obligation_ids: Vec<Uuid>,
}

/// Explicitly start one exact published plan revision.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvaluationPlanStartRequest {
    pub context: MissionEvaluationMutationContextWire,
    pub request_id: Uuid,
    pub mission_id: Uuid,
    pub plan_id: Uuid,
    pub expected_revision: u64,
}

/// Exact original mutation intent retained after an uncertain HTTP outcome.
///
/// This value is recovery context, not a retry instruction. Read the durable
/// request receipt first; an absent receipt while the owner is still running
/// does not prove that the original mutation will not commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MissionEvaluationMutationRequest {
    Create(MissionCreateRequest),
    RecordObligation(MissionObligationRecordRequest),
    PublishPlan(MissionEvaluationPlanPublishRequest),
    StartPlan(MissionEvaluationPlanStartRequest),
}

impl MissionEvaluationMutationRequest {
    #[must_use]
    pub fn scope(&self) -> &WorkspaceQueryScope {
        match self {
            Self::Create(request) => &request.context.scope,
            Self::RecordObligation(request) => &request.context.scope,
            Self::PublishPlan(request) => &request.context.scope,
            Self::StartPlan(request) => &request.context.scope,
        }
    }

    #[must_use]
    pub const fn session_id(&self) -> Uuid {
        match self {
            Self::Create(request) => request.context.session_id,
            Self::RecordObligation(request) => request.context.session_id,
            Self::PublishPlan(request) => request.context.session_id,
            Self::StartPlan(request) => request.context.session_id,
        }
    }

    #[must_use]
    pub const fn sender_activity_id(&self) -> Uuid {
        match self {
            Self::Create(request) => request.context.sender_activity_id,
            Self::RecordObligation(request) => request.context.sender_activity_id,
            Self::PublishPlan(request) => request.context.sender_activity_id,
            Self::StartPlan(request) => request.context.sender_activity_id,
        }
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        match self {
            Self::Create(request) => request.request_id,
            Self::RecordObligation(request) => request.request_id,
            Self::PublishPlan(request) => request.request_id,
            Self::StartPlan(request) => request.request_id,
        }
    }
}

/// Current authenticated participant selector for mutation-receipt readback.
///
/// It intentionally carries no leadership revision: readback proves the stored
/// logical actor, while Operations allows a legitimate current Session rebind.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MissionEvaluationRequestReadActor {
    /// Current Workspace Activity of the authenticated participant.
    pub sender_activity_id: Uuid,
    /// Current logical Session owned by the authenticated principal.
    pub session_id: Uuid,
}

/// Read one immutable Mission by exact Workspace scope.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionReadRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub mission_id: Uuid,
}

/// Read one immutable obligation and bind it to its expected Mission.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionObligationReadRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub mission_id: Uuid,
    pub obligation_id: Uuid,
}

/// Read one immutable evaluation-plan version.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvaluationPlanVersionReadRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub mission_id: Uuid,
    pub plan_id: Uuid,
    pub revision: u64,
}

/// Read the explicit start receipt for one Mission evaluation plan.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvaluationPlanStartReadRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub mission_id: Uuid,
    pub plan_id: Uuid,
}

/// Read one durable mutation receipt by its exact request ID and actor.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvaluationRequestReadRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub sender_activity_id: Uuid,
    pub request_id: Uuid,
}

/// Complete durable Mission record.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionWire {
    pub mission_id: Uuid,
    pub workspace_id: Uuid,
    pub team_id: Uuid,
    pub purpose: String,
    pub created_at_ms: i64,
    pub revision: u64,
}

/// Complete durable personal obligation record.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionObligationWire {
    pub obligation_id: Uuid,
    pub workspace_id: Uuid,
    pub mission_id: Uuid,
    pub participation_id: Uuid,
    pub work_unit_id: Uuid,
    pub weight: u64,
    pub acceptance_criteria: String,
    pub due_at_ms: i64,
    pub recorded_at_ms: i64,
    pub revision: u64,
}

/// Frozen obligation and positive integer weight in one plan snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvaluationPlanObligationWire {
    pub obligation_id: Uuid,
    pub weight: u64,
}

/// Immutable, revisioned evaluation-plan snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvaluationPlanVersionWire {
    pub plan_id: Uuid,
    pub version_id: Uuid,
    pub workspace_id: Uuid,
    pub mission_id: Uuid,
    pub revision: u64,
    pub t0_commit_at_ms: i64,
    pub t1_submission_cutoff_ms: i64,
    pub t2_checkpoint_ms: i64,
    pub cohort: Vec<MissionEvaluationPlanObligationWire>,
}

/// Immutable receipt for one explicitly started plan revision.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvaluationPlanStartWire {
    pub request_id: Uuid,
    pub workspace_id: Uuid,
    pub mission_id: Uuid,
    pub plan_id: Uuid,
    pub revision: u64,
    pub started_at_ms: i64,
}

/// Closed mutation kind stored in a durable request receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(
    rename_all = "camelCase",
    tag = "kind",
    content = "record",
    deny_unknown_fields
)]
pub enum MissionEvaluationRecordedMutationWire {
    MissionCreated(MissionWire),
    ObligationRecorded(MissionObligationWire),
    PlanPublished(MissionEvaluationPlanVersionWire),
    PlanStarted(MissionEvaluationPlanStartWire),
}

/// Result of creating a Mission, including exact replay status.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionCreateResponse {
    pub scope: WorkspaceQueryScope,
    pub request_id: Uuid,
    pub mission: MissionWire,
    pub replayed: bool,
}

/// Result of recording an obligation, including exact replay status.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionObligationRecordResponse {
    pub scope: WorkspaceQueryScope,
    pub request_id: Uuid,
    pub obligation: MissionObligationWire,
    pub replayed: bool,
}

/// Result of publishing a plan version, including exact replay status.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvaluationPlanPublishResponse {
    pub scope: WorkspaceQueryScope,
    pub request_id: Uuid,
    pub version: MissionEvaluationPlanVersionWire,
    pub replayed: bool,
}

/// Result of starting one exact plan revision, including exact replay status.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvaluationPlanStartResponse {
    pub scope: WorkspaceQueryScope,
    pub request_id: Uuid,
    pub start: MissionEvaluationPlanStartWire,
    pub replayed: bool,
}

/// Exact historical Mission readback; `mission` is always present as a key.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionReadResponse {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub mission_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub mission: Option<MissionWire>,
}

/// Exact historical obligation readback; `obligation` is always present as a key.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionObligationReadResponse {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub mission_id: Uuid,
    pub obligation_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub obligation: Option<MissionObligationWire>,
}

/// Exact immutable evaluation-plan version readback.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvaluationPlanVersionReadResponse {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub mission_id: Uuid,
    pub plan_id: Uuid,
    pub revision: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub version: Option<MissionEvaluationPlanVersionWire>,
}

/// Exact start-receipt readback.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvaluationPlanStartReadResponse {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub mission_id: Uuid,
    pub plan_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub start: Option<MissionEvaluationPlanStartWire>,
}

/// Exact mutation receipt readback; absence remains an observation only.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MissionEvaluationRequestReadResponse {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub sender_activity_id: Uuid,
    pub request_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub mutation: Option<MissionEvaluationRecordedMutationWire>,
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

    fn scope() -> WorkspaceQueryScope {
        WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 3,
            workspace_revision: 4,
            locator: "workspace".to_owned(),
        }
    }

    fn context() -> MissionEvaluationMutationContextWire {
        MissionEvaluationMutationContextWire {
            scope: scope(),
            session_id: Uuid::from_u128(5),
            sender_activity_id: Uuid::from_u128(6),
            expected_workspace_leadership_revision: 7,
        }
    }

    fn create_request() -> MissionCreateRequest {
        MissionCreateRequest {
            context: context(),
            request_id: Uuid::from_u128(8),
            mission_id: Uuid::from_u128(9),
            team_id: Uuid::from_u128(10),
            purpose: "deliver bounded work".to_owned(),
        }
    }

    #[test]
    fn mutation_request_is_camel_case_closed_and_has_no_authority_timestamps()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = create_request();
        let encoded = serde_json::to_value(&request)?;
        assert_eq!(
            serde_json::from_value::<MissionCreateRequest>(encoded.clone())?,
            request
        );
        assert!(encoded["context"].get("senderActivityId").is_some());
        assert!(
            encoded["context"]
                .get("expectedWorkspaceLeadershipRevision")
                .is_some()
        );
        assert!(encoded.get("createdAtMs").is_none());

        let mut actor = encoded.clone();
        actor["actor"] = serde_json::json!("localOperator");
        assert!(serde_json::from_value::<MissionCreateRequest>(actor).is_err());

        let mut timestamp = encoded;
        timestamp["createdAtMs"] = serde_json::json!(10);
        assert!(serde_json::from_value::<MissionCreateRequest>(timestamp).is_err());
        Ok(())
    }

    #[test]
    fn nullable_receipt_is_required_and_unknown_fields_are_rejected()
    -> Result<(), Box<dyn std::error::Error>> {
        let response = MissionEvaluationRequestReadResponse {
            scope: scope(),
            session_id: Uuid::from_u128(5),
            sender_activity_id: Uuid::from_u128(6),
            request_id: Uuid::from_u128(8),
            mutation: None,
        };
        let encoded = serde_json::to_value(&response)?;
        assert_eq!(encoded["mutation"], serde_json::Value::Null);
        assert_eq!(
            serde_json::from_value::<MissionEvaluationRequestReadResponse>(encoded.clone())?,
            response
        );

        let mut missing = encoded.clone();
        missing
            .as_object_mut()
            .ok_or("response object")?
            .remove("mutation");
        assert!(serde_json::from_value::<MissionEvaluationRequestReadResponse>(missing).is_err());

        let mut unknown = encoded;
        unknown["operator"] = serde_json::Value::Null;
        assert!(serde_json::from_value::<MissionEvaluationRequestReadResponse>(unknown).is_err());
        Ok(())
    }

    #[test]
    fn client_cannot_supply_server_owned_mutation_times() -> Result<(), Box<dyn std::error::Error>>
    {
        let context = context();
        let obligation = MissionObligationRecordRequest {
            context: context.clone(),
            request_id: Uuid::from_u128(8),
            mission_id: Uuid::from_u128(9),
            obligation_id: Uuid::from_u128(10),
            participation_id: Uuid::from_u128(11),
            work_unit_id: Uuid::from_u128(12),
            weight: 2,
            acceptance_criteria: "pass review".to_owned(),
            due_at_ms: 30,
        };
        let mut encoded = serde_json::to_value(&obligation)?;
        encoded["recordedAtMs"] = serde_json::json!(20);
        assert!(serde_json::from_value::<MissionObligationRecordRequest>(encoded).is_err());

        let plan = MissionEvaluationPlanPublishRequest {
            context: context.clone(),
            request_id: Uuid::from_u128(13),
            mission_id: Uuid::from_u128(9),
            plan_id: Uuid::from_u128(14),
            version_id: Uuid::from_u128(15),
            expected_head_revision: 0,
            t1_submission_cutoff_ms: 40,
            t2_checkpoint_ms: 50,
            obligation_ids: vec![Uuid::from_u128(10)],
        };
        let mut encoded = serde_json::to_value(&plan)?;
        assert_eq!(encoded["t1SubmissionCutoffMs"], 40);
        assert_eq!(encoded["t2CheckpointMs"], 50);
        encoded["t0CommitAtMs"] = serde_json::json!(35);
        assert!(serde_json::from_value::<MissionEvaluationPlanPublishRequest>(encoded).is_err());

        let start = MissionEvaluationPlanStartRequest {
            context,
            request_id: Uuid::from_u128(16),
            mission_id: Uuid::from_u128(9),
            plan_id: Uuid::from_u128(14),
            expected_revision: 1,
        };
        let mut encoded = serde_json::to_value(&start)?;
        encoded["startedAtMs"] = serde_json::json!(60);
        assert!(serde_json::from_value::<MissionEvaluationPlanStartRequest>(encoded).is_err());
        Ok(())
    }

    #[test]
    fn mutation_receipt_read_requires_current_actor_without_a_leadership_fence()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = MissionEvaluationRequestReadRequest {
            scope: scope(),
            session_id: Uuid::from_u128(5),
            sender_activity_id: Uuid::from_u128(6),
            request_id: Uuid::from_u128(8),
        };
        let encoded = serde_json::to_value(&request)?;
        assert_eq!(
            serde_json::from_value::<MissionEvaluationRequestReadRequest>(encoded.clone())?,
            request
        );
        assert!(encoded.get("senderActivityId").is_some());
        assert!(encoded.get("expectedWorkspaceLeadershipRevision").is_none());

        let mut fake_fence = encoded;
        fake_fence["expectedWorkspaceLeadershipRevision"] = serde_json::json!(1);
        assert!(serde_json::from_value::<MissionEvaluationRequestReadRequest>(fake_fence).is_err());
        Ok(())
    }
}
