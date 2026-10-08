//! Shared Profile configuration and lifecycle mutation contracts.

use super::AgentProfileResponse;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Validated mutable Profile configuration request.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProfileUpdateRequest {
    pub request_id: String,
    pub expected_revision: i64,
    pub display_name: String,
    pub preferred_model: Option<String>,
    pub preferred_reasoning: Option<String>,
    pub base_capabilities: Vec<String>,
}

/// Profile lifecycle mutation request.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProfileLifecycleRequest {
    pub request_id: String,
    pub expected_revision: i64,
}

/// Immutable Profile mutation receipt or mutation outcome.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileMutationResponse {
    pub request_id: Uuid,
    pub expected_revision: Option<i64>,
    pub kind: String,
    pub profile: AgentProfileResponse,
    pub replayed: bool,
}

/// Source-fenced Workspace Profile projection selector.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceProfilesQueryRequest {
    pub scope: super::leadership::WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
}

/// One Profile projected into a validated Workspace scope.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceProfileWire {
    pub profile: AgentProfileResponse,
    pub state: String,
}

/// One logical Session-to-Profile mapping in the scoped projection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceSessionProfileWire {
    pub session_id: Uuid,
    pub profile_id: Uuid,
}

/// Bounded source-fenced Workspace Profile projection.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceProfilesQueryResponse {
    pub scope: super::leadership::WorkspaceQueryScope,
    pub profiles: Vec<WorkspaceProfileWire>,
    pub session_profiles: Vec<WorkspaceSessionProfileWire>,
}
