//! Typed Node-Team query, mutation, and snapshot wire contracts.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::WorkspaceQueryScope;

/// Exact caller-owned Workspace/Session selector and requested Node generation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeTeamQueryRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub sender_activity_id: Uuid,
    pub node_scope: NodeTeamScopeWire,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub after_assignment_id: Option<Uuid>,
}

/// Caller-owned compare-and-swap request for one exact Node-Team binding.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeTeamSetRequest {
    pub scope: WorkspaceQueryScope,
    pub session_id: Uuid,
    pub sender_activity_id: Uuid,
    pub node_scope: NodeTeamScopeWire,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expected_revision: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub team_id: Option<Uuid>,
    pub sender_leadership_revision: i64,
}

/// Exact generation coordinates for a Node-Team query or mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeTeamScopeWire {
    pub activity_id: Uuid,
    pub goal_id: Uuid,
    pub goal_created_at_ms: i64,
    pub dag_revision: i64,
    pub dag_base_goal_revision: i64,
    pub node_id: Uuid,
}

impl From<heterarchy_orchestration_operations::NodeTeamBindingScope> for NodeTeamScopeWire {
    fn from(scope: heterarchy_orchestration_operations::NodeTeamBindingScope) -> Self {
        Self {
            activity_id: scope.activity_id().into_uuid(),
            goal_id: scope.goal_id().into_uuid(),
            goal_created_at_ms: scope.goal_created_at().get(),
            dag_revision: scope.dag_revision().get(),
            dag_base_goal_revision: scope.dag_base_goal_revision().get(),
            node_id: scope.node_id().into_uuid(),
        }
    }
}

/// Exact request echo and existing native Node-Team snapshot projection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeTeamQueryResponse {
    pub request: NodeTeamQueryRequest,
    pub snapshot: NodeTeamSnapshotWire,
}

/// Exact request echo and the resulting Node-Team binding state.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeTeamSetResponse {
    pub request: NodeTeamSetRequest,
    pub binding: NodeTeamBindingWire,
}

/// Binding state and one bounded reservation page for the exact requested Node.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeTeamSnapshotWire {
    pub workspace_id: Uuid,
    pub node_scope: NodeTeamScopeWire,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub after_assignment_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub binding: Option<NodeTeamBindingWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub legacy_binding: Option<NodeTeamBindingWire>,
    pub reservations: Vec<NodeTeamReservationWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub next_assignment_id: Option<Uuid>,
}

/// Exact or aggregate-only binding returned by the native Node-Team projection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeTeamBindingWire {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub activity_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub goal_created_at_ms: Option<i64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub dag_base_goal_revision: Option<i64>,
    pub goal_id: Uuid,
    pub dag_revision: i64,
    pub node_id: Uuid,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub team_id: Option<Uuid>,
    pub bound_by: String,
    pub bound_at_ms: i64,
    pub revision: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub legacy_retirement: Option<NodeTeamLegacyRetirementWire>,
}

/// Immutable evidence that an aggregate-only legacy binding was retired.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeTeamLegacyRetirementWire {
    pub request_id: Uuid,
    pub retired_at_ms: i64,
    pub binding_revision: u64,
    pub authority: NodeTeamRetirementAuthorityWire,
}

/// Closed authority class recorded by legacy retirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeTeamRetirementAuthorityWire {
    LocalOperator,
    WorkspaceLeader,
}

/// One bounded assignment reservation related to the requested Node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeTeamReservationWire {
    pub activity_id: Uuid,
    pub assignment_id: Uuid,
    pub disposition: NodeTeamReservationDispositionWire,
    pub scope_relation: NodeTeamReservationScopeRelationWire,
}

/// Closed set of reservation lifecycle states exposed by the native projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeTeamReservationDispositionWire {
    Pending,
    Active,
    LegacyUnknown,
}

/// Closed relationship between a reservation and the requested Node generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeTeamReservationScopeRelationWire {
    Exact,
    HistoricalCoordinates,
    LegacyUnknown,
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
    use serde_json::Value;

    fn request() -> NodeTeamQueryRequest {
        NodeTeamQueryRequest {
            scope: WorkspaceQueryScope {
                workspace_id: Uuid::from_u128(1),
                location_id: Uuid::from_u128(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            session_id: Uuid::from_u128(5),
            sender_activity_id: Uuid::from_u128(6),
            node_scope: NodeTeamScopeWire {
                activity_id: Uuid::from_u128(12),
                goal_id: Uuid::from_u128(7),
                goal_created_at_ms: 8,
                dag_revision: 9,
                dag_base_goal_revision: 10,
                node_id: Uuid::from_u128(11),
            },
            after_assignment_id: None,
        }
    }

    fn set_request(expected_revision: Option<u64>, team_id: Option<Uuid>) -> NodeTeamSetRequest {
        let query = request();
        NodeTeamSetRequest {
            scope: query.scope,
            session_id: query.session_id,
            sender_activity_id: query.sender_activity_id,
            node_scope: query.node_scope,
            expected_revision,
            team_id,
            sender_leadership_revision: 13,
        }
    }

    fn binding(activity_id: Option<Uuid>) -> NodeTeamBindingWire {
        NodeTeamBindingWire {
            activity_id,
            goal_created_at_ms: activity_id.map(|_| 8),
            dag_base_goal_revision: activity_id.map(|_| 10),
            goal_id: Uuid::from_u128(7),
            dag_revision: 9,
            node_id: Uuid::from_u128(11),
            team_id: None,
            bound_by: "operator".to_owned(),
            bound_at_ms: 12,
            revision: 2,
            legacy_retirement: None,
        }
    }

    fn set_response(request: NodeTeamSetRequest) -> NodeTeamSetResponse {
        let mut binding = binding(Some(request.node_scope.activity_id));
        binding.team_id = request.team_id;
        binding.bound_by = "orchestrator".to_owned();
        binding.revision = request.expected_revision.map_or(1, |revision| revision + 1);
        NodeTeamSetResponse { request, binding }
    }

    fn response(request: NodeTeamQueryRequest) -> NodeTeamQueryResponse {
        NodeTeamQueryResponse {
            snapshot: NodeTeamSnapshotWire {
                workspace_id: request.scope.workspace_id,
                node_scope: request.node_scope,
                after_assignment_id: None,
                binding: Some(binding(Some(request.node_scope.activity_id))),
                legacy_binding: Some(binding(None)),
                reservations: vec![NodeTeamReservationWire {
                    activity_id: request.node_scope.activity_id,
                    assignment_id: Uuid::from_u128(13),
                    disposition: NodeTeamReservationDispositionWire::Pending,
                    scope_relation: NodeTeamReservationScopeRelationWire::HistoricalCoordinates,
                }],
                next_assignment_id: Some(Uuid::from_u128(14)),
            },
            request,
        }
    }

    #[test]
    fn request_and_snapshot_round_trip_native_camel_case_projection()
    -> Result<(), Box<dyn std::error::Error>> {
        let query = request();
        let encoded = serde_json::to_value(&query)?;
        assert_eq!(
            encoded["scope"]["workspaceId"],
            Uuid::from_u128(1).to_string()
        );
        assert_eq!(encoded["senderActivityId"], Uuid::from_u128(6).to_string());
        assert_eq!(
            encoded["nodeScope"]["activityId"],
            Uuid::from_u128(12).to_string()
        );
        assert_eq!(encoded["nodeScope"]["goalCreatedAtMs"], 8);
        assert_eq!(encoded["afterAssignmentId"], Value::Null);
        assert_eq!(
            serde_json::from_value::<NodeTeamQueryRequest>(encoded)?,
            query
        );

        let response = response(query);
        let encoded = serde_json::to_value(&response)?;
        assert_eq!(
            encoded["request"]["sessionId"],
            Uuid::from_u128(5).to_string()
        );
        assert_eq!(
            encoded["snapshot"]["legacyBinding"]["activityId"],
            Value::Null
        );
        assert_eq!(
            encoded["snapshot"]["reservations"][0]["scopeRelation"],
            "historical_coordinates"
        );
        assert_eq!(
            serde_json::from_value::<NodeTeamQueryResponse>(encoded)?,
            response
        );
        Ok(())
    }

    #[test]
    fn nullable_selector_and_snapshot_fields_are_required_and_unknown_fields_rejected()
    -> Result<(), Box<dyn std::error::Error>> {
        let query = request();
        let mut encoded = serde_json::to_value(&query)?;
        encoded
            .as_object_mut()
            .ok_or("request JSON object")?
            .remove("afterAssignmentId");
        assert!(serde_json::from_value::<NodeTeamQueryRequest>(encoded).is_err());

        let response = response(query);
        let encoded = serde_json::to_value(&response)?;
        for (path, field) in [
            ("snapshot", "binding"),
            ("snapshot", "legacyBinding"),
            ("snapshot", "afterAssignmentId"),
            ("snapshot", "nextAssignmentId"),
        ] {
            let mut omitted = encoded.clone();
            omitted[path]
                .as_object_mut()
                .ok_or("snapshot JSON object")?
                .remove(field);
            assert!(serde_json::from_value::<NodeTeamQueryResponse>(omitted).is_err());
        }

        let mut missing_generation = encoded.clone();
        missing_generation["snapshot"]["binding"]
            .as_object_mut()
            .ok_or("binding JSON object")?
            .remove("activityId");
        assert!(serde_json::from_value::<NodeTeamQueryResponse>(missing_generation).is_err());

        let mut unknown = encoded;
        unknown["request"]["principal"] = Value::Null;
        assert!(serde_json::from_value::<NodeTeamQueryResponse>(unknown).is_err());
        Ok(())
    }

    #[test]
    fn set_contract_uses_required_nullable_fields_and_strict_camel_case()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = set_request(None, None);
        let encoded = serde_json::to_value(&request)?;
        assert_eq!(encoded["expectedRevision"], Value::Null);
        assert_eq!(encoded["teamId"], Value::Null);
        assert_eq!(encoded["senderLeadershipRevision"], 13);
        assert_eq!(
            serde_json::from_value::<NodeTeamSetRequest>(encoded.clone())?,
            request
        );

        for field in ["expectedRevision", "teamId"] {
            let mut omitted = encoded.clone();
            omitted
                .as_object_mut()
                .ok_or("set request JSON object")?
                .remove(field);
            assert!(serde_json::from_value::<NodeTeamSetRequest>(omitted).is_err());
        }

        let response = set_response(set_request(Some(1), Some(Uuid::from_u128(20))));
        let mut response_json = serde_json::to_value(&response)?;
        assert_eq!(response_json["binding"]["boundBy"], "orchestrator");
        assert_eq!(
            serde_json::from_value::<NodeTeamSetResponse>(response_json.clone())?,
            response
        );
        response_json["request"]["unexpected"] = Value::Null;
        assert!(serde_json::from_value::<NodeTeamSetResponse>(response_json).is_err());
        Ok(())
    }
}
