//! Source-fenced, read-only Team query contracts.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::leadership::WorkspaceQueryScope;

/// Bounded Team list query. The optional Session is an authorization selector;
/// the request credential remains the caller authority.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamListQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub limit: u32,
}

/// Complete Team list page with its source and selector echoed exactly.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamListQueryResponse {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub limit: u32,
    pub teams: Vec<TeamWire>,
    pub truncated: bool,
}

/// Bounded Team query by the native Team and optional mutation selectors.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamGetQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub team_id: Uuid,
    pub request_id: Option<Uuid>,
}

/// Complete Team read projection and optional exact historical receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamGetQueryResponse {
    pub scope: WorkspaceQueryScope,
    pub session_id: Option<Uuid>,
    pub team_id: Uuid,
    pub request_id: Option<Uuid>,
    pub team: Option<TeamWire>,
    pub members: Vec<TeamMemberWire>,
    pub members_truncated: bool,
    pub mutation: Option<TeamMutationWire>,
}

/// Complete authoritative Team record, including creator provenance.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamWire {
    pub team_id: Uuid,
    pub workspace_id: Uuid,
    pub name: String,
    pub purpose: String,
    pub state: String,
    pub revision: u64,
    pub created_by: TeamCreatorWire,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub formed_at_ms: Option<i64>,
}

/// Creator identities persisted with the Team source record.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamCreatorWire {
    pub key: String,
    pub display_name: String,
    pub participation_id: Option<Uuid>,
    pub profile_id: Option<Uuid>,
    pub participant_id: Option<Uuid>,
    pub session_id: Option<Uuid>,
}

/// One bounded durable membership record and its optional Profile decoration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamMemberWire {
    pub workspace_id: Uuid,
    pub team_id: Uuid,
    pub participation_id: Uuid,
    pub profile_id: Option<Uuid>,
    pub display_name: Option<String>,
    pub role: Option<String>,
    pub state: String,
    pub revision: u64,
    pub joined_at_ms: i64,
    pub left_at_ms: Option<i64>,
}

/// Historical Team mutation receipt returned by an exact request ID query.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamMutationWire {
    pub team: TeamWire,
    pub membership: Option<TeamMemberWire>,
    pub initial_members: Vec<TeamMemberWire>,
    pub created: bool,
    pub replayed: bool,
}
