//! Source-fenced `WorkCredit` score query contracts.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

/// Exact `Workspace` selector for a `WorkCredit` score query.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkCreditScoreQueryRequest {
    pub scope: super::leadership::WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
}

/// Source-fenced `WorkCredit` score query and its owner snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkCreditScoreQueryResponse {
    pub scope: super::leadership::WorkspaceQueryScope,
    pub snapshot: WorkCreditScoreWire,
}

/// Full `Workspace` or `Team` `WorkCredit` score snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkCreditScoreWire {
    pub workspace_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub team_id: Option<Uuid>,
    pub window_ms: i64,
    pub total_credits: i64,
    pub workspace_arithmetic_mean: i64,
    pub mean_remainder_numerator: i64,
    pub mean_subject_count: i64,
    pub subject_total_credits: i64,
    pub unattributed_count: i64,
    pub fulfillment: WorkCreditFulfillmentWire,
    pub provenance: WorkCreditProvenanceWire,
    pub per_subject: Vec<WorkCreditSubjectScoreWire>,
}

/// Workspace-wide and per-subject fulfillment counts.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkCreditFulfillmentWire {
    pub completed: i64,
    pub evaluated: i64,
    pub per_subject: Vec<WorkCreditSubjectFulfillmentWire>,
}

/// Fulfillment counts for one logical subject.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkCreditSubjectFulfillmentWire {
    pub subject: WorkCreditSubjectIdentityWire,
    pub source_participant_count: u64,
    pub completed: i64,
    pub evaluated: i64,
}

/// Provenance totals from the Operations score snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkCreditProvenanceWire {
    pub submitted: i64,
    pub matched: i64,
}

/// One logical subject's complete identity and score.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkCreditSubjectScoreWire {
    pub subject: WorkCreditSubjectIdentityWire,
    pub source_participant_count: u64,
    pub total_credits: i64,
    pub entry_count: i64,
}

/// Identity bindings supplied by the Operations owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkCreditSubjectIdentityWire {
    pub kind: WorkCreditSubjectKindWire,
    pub subject_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub participation_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub profile_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub legacy_participant_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub participation_revision: Option<u64>,
}

/// The closed set of `WorkCredit` subject kinds owned by `Operations`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum WorkCreditSubjectKindWire {
    #[serde(rename = "WorkspaceParticipation")]
    WorkspaceParticipation,
    #[serde(rename = "LegacyParticipant")]
    LegacyParticipant,
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}
