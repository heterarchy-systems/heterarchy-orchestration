//! Strict pre-offer Node Assignment admission and pure recovery contracts.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::leadership::WorkspaceQueryScope;

/// Exact Source Goal/DAG/node fences covered by an offer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeAssignmentSourceFencesWire {
    pub goal_id: Uuid,
    pub goal_created_at_ms: i64,
    pub goal_revision: i64,
    pub dag_revision: i64,
    pub dag_base_goal_revision: i64,
    pub dag_head_revision: i64,
    pub node_id: Uuid,
    pub node_revision: i64,
}

/// Exact Ready target expected by the future Forge offer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeAssignmentTargetWire {
    pub agent_id: Uuid,
    pub session_id: Uuid,
    pub agent_revision: i64,
}

/// Complete immutable admission intent.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeAssignmentAdmissionRequest {
    pub scope: WorkspaceQueryScope,
    pub activity_id: Uuid,
    pub coordinator_session_id: Uuid,
    pub coordinator_agent_id: Uuid,
    pub coordinator_revision: i64,
    pub assignment_id: Uuid,
    pub source_fences: NodeAssignmentSourceFencesWire,
    pub target: NodeAssignmentTargetWire,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub team_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expected_binding_revision: Option<u64>,
    pub source_offer_bytes_base64: String,
    pub source_offer_sha256: String,
}

/// Exact node generation stored in one affinity receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeAssignmentScopeWire {
    pub activity_id: Uuid,
    pub goal_id: Uuid,
    pub goal_created_at_ms: i64,
    pub dag_revision: i64,
    pub dag_base_goal_revision: i64,
    pub node_id: Uuid,
}

/// Immutable requester attribution stored with the admission. Profile requesters
/// carry both Profile identifiers; native requesters carry neither.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeAssignmentRequesterWire {
    pub activity_id: Uuid,
    pub agent_id: Uuid,
    pub session_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub participation_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub profile_id: Option<Uuid>,
    pub participant_id: Uuid,
}

/// Current durable lifecycle of an admitted affinity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeAssignmentAdmissionDispositionWire {
    Pending,
    Active,
}

/// Exact immutable admission receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeAssignmentAdmissionReceiptWire {
    pub workspace_id: Uuid,
    pub assignment_id: Uuid,
    pub activity_id: Uuid,
    pub target_agent_id: Uuid,
    pub target_session_id: Uuid,
    pub target_participation_id: Uuid,
    pub target_participant_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub team_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub team_membership_revision: Option<u64>,
    pub node_scope: NodeAssignmentScopeWire,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub binding_revision: Option<u64>,
    pub disposition: NodeAssignmentAdmissionDispositionWire,
    pub requester: NodeAssignmentRequesterWire,
    pub source_offer_sha256: String,
    pub observed_at_ms: i64,
}

/// Successful first admission or exact replay.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeAssignmentAdmissionResponse {
    pub request: NodeAssignmentAdmissionRequest,
    pub admission: NodeAssignmentAdmissionReceiptWire,
    pub created: bool,
}

/// Pure readback of the exact immutable admission intent.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeAssignmentAdmissionRecoverResponse {
    pub request: NodeAssignmentAdmissionRequest,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub admission: Option<NodeAssignmentAdmissionReceiptWire>,
    pub retry_authorized: bool,
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}
