//! Source-scoped Team mutation requests and complete receipts.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::leadership::WorkspaceQueryScope;
use super::teams::TeamMutationWire;

/// One initial Team member selected by its Activity and logical Session.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamRosterMemberRequest {
    pub activity_id: Uuid,
    pub session_id: Uuid,
    pub role: String,
}

/// Create one source-scoped Team with its exact frozen initial roster.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamCreateRequest {
    pub scope: WorkspaceQueryScope,
    pub request_id: Uuid,
    pub team_id: Uuid,
    pub activity_id: Uuid,
    pub session_id: Uuid,
    pub expected_sender_leadership_revision: i64,
    pub name: String,
    pub purpose: String,
    pub initial_members: Vec<TeamRosterMemberRequest>,
}

/// Join the caller's logical participant to one exact Team revision.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamJoinRequest {
    pub scope: WorkspaceQueryScope,
    pub request_id: Uuid,
    pub team_id: Uuid,
    pub activity_id: Uuid,
    pub session_id: Uuid,
    pub expected_team_revision: u64,
    pub role: String,
}

/// Leave one exact Team revision while retaining the membership receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamLeaveRequest {
    pub scope: WorkspaceQueryScope,
    pub request_id: Uuid,
    pub team_id: Uuid,
    pub activity_id: Uuid,
    pub session_id: Uuid,
    pub expected_team_revision: u64,
}

/// Retire one exact Team revision through the operator-authorized route.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamRetireRequest {
    pub scope: WorkspaceQueryScope,
    pub request_id: Uuid,
    pub team_id: Uuid,
    pub expected_team_revision: u64,
}

/// Exact mutation envelope with the complete immutable Operations receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TeamMutationResponse {
    pub scope: WorkspaceQueryScope,
    pub request_id: Uuid,
    pub team_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_team_mutation")]
    pub mutation: TeamMutationWire,
}

fn deserialize_required_team_mutation<'de, D>(deserializer: D) -> Result<TeamMutationWire, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    if !has_required_nullable_team_fields(&value) {
        return Err(serde::de::Error::custom(
            "Team mutation omitted required nullable metadata",
        ));
    }
    serde_json::from_value(value).map_err(serde::de::Error::custom)
}

fn has_required_nullable_team_fields(mutation: &serde_json::Value) -> bool {
    let Some(mutation) = mutation.as_object() else {
        return false;
    };
    if !mutation.contains_key("membership") {
        return false;
    }
    let Some(team) = mutation.get("team").and_then(serde_json::Value::as_object) else {
        return false;
    };
    if !team.contains_key("formedAtMs") {
        return false;
    }
    let Some(creator) = team.get("createdBy").and_then(serde_json::Value::as_object) else {
        return false;
    };
    if !["participationId", "profileId", "participantId", "sessionId"]
        .iter()
        .all(|field| creator.contains_key(*field))
    {
        return false;
    }
    if let Some(member) = mutation.get("membership")
        && !member.is_null()
        && !has_required_nullable_member_fields(member)
    {
        return false;
    }
    mutation
        .get("initialMembers")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|members| members.iter().all(has_required_nullable_member_fields))
}

fn has_required_nullable_member_fields(member: &serde_json::Value) -> bool {
    let Some(member) = member.as_object() else {
        return false;
    };
    ["profileId", "displayName", "role", "leftAtMs"]
        .iter()
        .all(|field| member.contains_key(*field))
}

#[cfg(test)]
mod tests {
    use super::{TeamMutationResponse, TeamMutationWire};
    use crate::wire::leadership::WorkspaceQueryScope;
    use crate::wire::teams::{TeamCreatorWire, TeamMemberWire, TeamWire};
    use uuid::Uuid;

    fn response() -> TeamMutationResponse {
        let workspace_id = Uuid::from_u128(1);
        let team_id = Uuid::from_u128(2);
        TeamMutationResponse {
            scope: WorkspaceQueryScope {
                workspace_id,
                location_id: Uuid::from_u128(3),
                location_revision: 1,
                workspace_revision: 1,
                locator: "workspace".to_owned(),
            },
            request_id: Uuid::from_u128(4),
            team_id,
            mutation: TeamMutationWire {
                team: TeamWire {
                    team_id,
                    workspace_id,
                    name: "review".to_owned(),
                    purpose: "review source".to_owned(),
                    state: "active".to_owned(),
                    revision: 1,
                    created_by: TeamCreatorWire {
                        key: "session:creator".to_owned(),
                        display_name: "Creator".to_owned(),
                        participation_id: None,
                        profile_id: None,
                        participant_id: Some(Uuid::from_u128(5)),
                        session_id: Some(Uuid::from_u128(6)),
                    },
                    created_at_ms: 10,
                    updated_at_ms: 10,
                    formed_at_ms: Some(10),
                },
                membership: Some(TeamMemberWire {
                    workspace_id,
                    team_id,
                    participation_id: Uuid::from_u128(7),
                    profile_id: None,
                    display_name: None,
                    role: Some("reviewer".to_owned()),
                    state: "joined".to_owned(),
                    revision: 1,
                    joined_at_ms: 10,
                    left_at_ms: None,
                }),
                initial_members: Vec::new(),
                created: true,
                replayed: false,
            },
        }
    }

    #[test]
    fn team_mutation_receipt_requires_every_nullable_metadata_field()
    -> Result<(), Box<dyn std::error::Error>> {
        let baseline = serde_json::to_value(response())?;
        assert!(serde_json::from_value::<TeamMutationResponse>(baseline.clone()).is_ok());

        for pointer in [
            "/mutation/membership",
            "/mutation/team/formedAtMs",
            "/mutation/team/createdBy/participationId",
            "/mutation/team/createdBy/profileId",
            "/mutation/team/createdBy/participantId",
            "/mutation/team/createdBy/sessionId",
            "/mutation/membership/profileId",
            "/mutation/membership/displayName",
            "/mutation/membership/role",
            "/mutation/membership/leftAtMs",
        ] {
            let mut missing = baseline.clone();
            let (parent, key) = pointer.rsplit_once('/').ok_or("invalid JSON pointer")?;
            let parent = missing.pointer_mut(parent).ok_or("missing JSON object")?;
            parent
                .as_object_mut()
                .ok_or("JSON parent must be an object")?
                .remove(key);
            assert!(
                serde_json::from_value::<TeamMutationResponse>(missing).is_err(),
                "omitted nullable field {pointer} must be rejected"
            );
        }
        Ok(())
    }
}
