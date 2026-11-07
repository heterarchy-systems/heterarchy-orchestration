//! Source-scoped Team mutation requests and complete receipts.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::leadership::WorkspaceQueryScope;
use super::teams::{TeamMutationV2Wire, TeamMutationWire};

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

/// Create one Team with an explicit per-Team cap, preserving the original
/// source-scoped v1 request fields and adding the v2 capacity contract.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamCreateV2Request {
    pub scope: WorkspaceQueryScope,
    pub request_id: Uuid,
    pub team_id: Uuid,
    pub activity_id: Uuid,
    pub session_id: Uuid,
    pub expected_sender_leadership_revision: i64,
    pub name: String,
    pub purpose: String,
    pub initial_members: Vec<TeamRosterMemberRequest>,
    pub max_members: u8,
}

/// Exact Orchestrator-selected expansion decision slot for a v2 Team join.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamExpansionSlotV2Request {
    pub decision_id: Uuid,
    pub ordinal: u8,
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

/// Join one Team by consuming an exact Orchestrator decision slot.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamJoinV2Request {
    pub scope: WorkspaceQueryScope,
    pub request_id: Uuid,
    pub team_id: Uuid,
    pub activity_id: Uuid,
    pub session_id: Uuid,
    pub expected_team_revision: u64,
    pub role: String,
    pub expansion_slot: TeamExpansionSlotV2Request,
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

/// V2 create or join outcome with its source-backed receipt projection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamMutationV2Response {
    pub scope: WorkspaceQueryScope,
    pub request_id: Uuid,
    pub team_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_team_mutation_v2")]
    pub mutation: TeamMutationV2Wire,
}

/// Exact original v2 mutation intent carried by an owner-scoped receipt query.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(
    tag = "kind",
    content = "request",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum TeamMutationV2Intent {
    Create(TeamCreateV2Request),
    Join(TeamJoinV2Request),
}

/// Current authenticated participant reader for one owner-scoped v2 receipt.
/// A current Workspace Orchestrator uses the same Session identity; Operations
/// grants access only when that participant is currently the Orchestrator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamMutationReaderV2Wire {
    pub activity_id: Uuid,
    pub session_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamMutationV2ReadbackRequest {
    pub reader: TeamMutationReaderV2Wire,
    pub intent: TeamMutationV2Intent,
}

/// Source receipt query deliberately returns the same `None` for unrelated
/// readers, absent request IDs, and v1 records with no v2 source evidence.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamMutationV2ReadbackResponse {
    pub scope: WorkspaceQueryScope,
    pub request_id: Uuid,
    pub team_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_v2_receipt")]
    pub receipt: Option<TeamMutationV2Wire>,
}

fn deserialize_required_v2_receipt<'de, D>(
    deserializer: D,
) -> Result<Option<TeamMutationV2Wire>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<TeamMutationV2Wire>::deserialize(deserializer)
}

fn deserialize_required_team_mutation_v2<'de, D>(
    deserializer: D,
) -> Result<TeamMutationV2Wire, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    let has_v2_receipt_fields = value.as_object().is_some_and(|mutation| {
        [
            "capacity",
            "initialRosterDigestV1",
            "expansionSlot",
            "expansionJoinSource",
        ]
        .iter()
        .all(|field| mutation.contains_key(*field))
    });
    if !has_v2_receipt_fields || !has_required_nullable_team_fields(&value) {
        return Err(serde::de::Error::custom(
            "v2 Team mutation omitted required nullable source metadata",
        ));
    }
    serde_json::from_value(value).map_err(serde::de::Error::custom)
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
    use super::{
        TeamCreateRequest, TeamCreateV2Request, TeamExpansionSlotV2Request, TeamJoinRequest,
        TeamJoinV2Request, TeamMutationResponse, TeamMutationV2ReadbackResponse, TeamMutationWire,
    };
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
        let mutation = baseline["mutation"].as_object().ok_or("mutation object")?;
        assert_eq!(mutation.len(), 5, "v1 mutation keeps its frozen field set");
        assert!(!mutation.contains_key("capacity"));
        assert!(!mutation.contains_key("expansionSlot"));

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

    #[test]
    fn v1_create_and_join_requests_keep_the_frozen_strict_json_shape()
    -> Result<(), Box<dyn std::error::Error>> {
        let scope = WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(3),
            location_revision: 1,
            workspace_revision: 1,
            locator: "workspace".to_owned(),
        };
        let create = TeamCreateRequest {
            scope: scope.clone(),
            request_id: Uuid::from_u128(4),
            team_id: Uuid::from_u128(2),
            activity_id: Uuid::from_u128(5),
            session_id: Uuid::from_u128(6),
            expected_sender_leadership_revision: 1,
            name: "review".to_owned(),
            purpose: "review source".to_owned(),
            initial_members: vec![],
        };
        let create_json = serde_json::to_value(&create)?;
        assert!(serde_json::from_value::<TeamCreateRequest>(create_json.clone()).is_ok());
        let create_fields = create_json.as_object().ok_or("create object")?;
        assert_eq!(create_fields.len(), 9);
        assert!(!create_fields.contains_key("maxMembers"));
        let mut v2_create_field = create_json;
        v2_create_field["maxMembers"] = serde_json::json!(2);
        assert!(serde_json::from_value::<TeamCreateRequest>(v2_create_field).is_err());

        let join = TeamJoinRequest {
            scope,
            request_id: Uuid::from_u128(7),
            team_id: Uuid::from_u128(2),
            activity_id: Uuid::from_u128(5),
            session_id: Uuid::from_u128(6),
            expected_team_revision: 1,
            role: "reviewer".to_owned(),
        };
        let join_json = serde_json::to_value(&join)?;
        assert!(serde_json::from_value::<TeamJoinRequest>(join_json.clone()).is_ok());
        let join_fields = join_json.as_object().ok_or("join object")?;
        assert_eq!(join_fields.len(), 7);
        assert!(!join_fields.contains_key("expansionSlot"));
        let mut v2_join_field = join_json;
        v2_join_field["expansionSlot"] = serde_json::json!({
            "decisionId": Uuid::from_u128(8),
            "ordinal": 0,
        });
        assert!(serde_json::from_value::<TeamJoinRequest>(v2_join_field).is_err());
        Ok(())
    }

    #[test]
    fn v2_mutations_have_separate_explicit_cap_and_decision_slot_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let scope = WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(3),
            location_revision: 1,
            workspace_revision: 1,
            locator: "workspace".to_owned(),
        };
        let create = TeamCreateV2Request {
            scope: scope.clone(),
            request_id: Uuid::from_u128(4),
            team_id: Uuid::from_u128(2),
            activity_id: Uuid::from_u128(5),
            session_id: Uuid::from_u128(6),
            expected_sender_leadership_revision: 1,
            name: "review".to_owned(),
            purpose: "review source".to_owned(),
            initial_members: vec![],
            max_members: 2,
        };
        let create_json = serde_json::to_value(&create)?;
        assert_eq!(create_json["maxMembers"], 2);
        assert!(serde_json::from_value::<TeamCreateV2Request>(create_json).is_ok());

        let join = TeamJoinV2Request {
            scope,
            request_id: Uuid::from_u128(7),
            team_id: Uuid::from_u128(2),
            activity_id: Uuid::from_u128(5),
            session_id: Uuid::from_u128(6),
            expected_team_revision: 1,
            role: "reviewer".to_owned(),
            expansion_slot: TeamExpansionSlotV2Request {
                decision_id: Uuid::from_u128(8),
                ordinal: 0,
            },
        };
        let join_json = serde_json::to_value(&join)?;
        assert_eq!(
            join_json["expansionSlot"]["decisionId"],
            serde_json::json!(Uuid::from_u128(8))
        );
        assert!(join_json["expansionSlot"].get("approvalId").is_none());
        assert!(serde_json::from_value::<TeamJoinV2Request>(join_json).is_ok());
        Ok(())
    }

    #[test]
    fn v2_owner_readback_requires_an_explicit_nullable_receipt()
    -> Result<(), Box<dyn std::error::Error>> {
        let response = TeamMutationV2ReadbackResponse {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(3),
                location_revision: 1,
                workspace_revision: 1,
                locator: "workspace".to_owned(),
            },
            request_id: Uuid::from_u128(4),
            team_id: Uuid::from_u128(2),
            receipt: None,
        };
        let encoded = serde_json::to_value(response)?;
        assert!(serde_json::from_value::<TeamMutationV2ReadbackResponse>(encoded.clone()).is_ok());
        let mut missing = encoded;
        missing
            .as_object_mut()
            .ok_or("readback response object")?
            .remove("receipt");
        assert!(serde_json::from_value::<TeamMutationV2ReadbackResponse>(missing).is_err());
        Ok(())
    }
}
