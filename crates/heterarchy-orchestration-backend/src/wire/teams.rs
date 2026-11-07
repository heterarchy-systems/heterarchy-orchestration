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

/// Exact v2 mutation source and immutable receipt. The v1 Team query never
/// serializes this projection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamMutationV2Wire {
    pub team: TeamWire,
    pub membership: Option<TeamMemberWire>,
    pub initial_members: Vec<TeamMemberWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub capacity: Option<TeamCapacityReceiptV2Wire>,
    /// Lowercase hexadecimal SHA-256 digest using the shared v1 roster contract.
    #[serde(deserialize_with = "deserialize_required_option")]
    pub initial_roster_digest_v1: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expansion_slot: Option<super::team_commands::TeamExpansionSlotV2Request>,
    /// Required for every successful v2 Join, including direct mutation
    /// replies. Missing source evidence leaves the mutation outcome unknown.
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expansion_join_source: Option<TeamExpansionJoinSourceV2Wire>,
    /// Mirrors whether this operation inserted its row: a Team on CREATE or
    /// a membership on JOIN. For JOIN, this matches membership revision 1.
    pub created: bool,
    pub replayed: bool,
}

/// Immutable capacity generation from a v2 Team create receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamCapacityReceiptV2Wire {
    pub team_id: Uuid,
    pub workspace_id: Uuid,
    pub max_members: u8,
    pub revision: u64,
    pub recorded_at_ms: i64,
    pub actor: super::team_expansion::TeamExpansionActorReceiptWire,
    pub leadership_revision: i64,
}

/// Original actor and exact decision slot consumed by one v2 Join.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionJoinSourceV2Wire {
    pub activity_id: Uuid,
    pub agent_id: Uuid,
    pub session_id: Uuid,
    pub decision_id: Uuid,
    pub ordinal: u8,
    pub role: String,
}

pub(crate) const fn v2_join_created_flag_matches_membership_revision(
    created: bool,
    membership_revision: u64,
) -> bool {
    membership_revision > 0 && created == (membership_revision == 1)
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[cfg(test)]
mod tests {
    use super::v2_join_created_flag_matches_membership_revision;

    #[test]
    fn v2_join_created_tracks_first_row_vs_rejoin_revision() {
        assert!(v2_join_created_flag_matches_membership_revision(true, 1));
        assert!(v2_join_created_flag_matches_membership_revision(false, 2));
        assert!(!v2_join_created_flag_matches_membership_revision(false, 1));
        assert!(!v2_join_created_flag_matches_membership_revision(true, 2));
        assert!(!v2_join_created_flag_matches_membership_revision(false, 0));
    }
}
