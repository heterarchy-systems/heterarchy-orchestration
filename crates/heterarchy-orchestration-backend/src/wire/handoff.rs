//! Typed exact request and readback contracts for Coordinator handoff.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::departure::{DepartureReceiptWire, DepartureTarget};

/// Exact caller intent for transferring one Activity Coordinator binding.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HandoffRequest {
    pub source: DepartureTarget,
    pub target_agent_id: Uuid,
    pub target_session_id: Uuid,
    pub expected_target_agent_revision: i64,
    pub expected_coordinator_revision: i64,
    pub expected_plan: HandoffPlanWire,
    pub request_id: Uuid,
}

/// Exact current Goal, DAG and head-node fence carried by a handoff request.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HandoffPlanWire {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub goal: Option<HandoffGoalWire>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub dag_revision: Option<i64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub dag_head_revision: Option<i64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub node: Option<HandoffNodeWire>,
}

/// Goal identity and expected revision in the current plan fence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HandoffGoalWire {
    pub goal_id: Uuid,
    pub expected_revision: i64,
}

/// Head-node identity and expected revision in the current plan fence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HandoffNodeWire {
    pub node_id: Uuid,
    pub expected_revision: i64,
}

/// Handoff result committed by Orchestration Operations.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HandoffResponse {
    pub request: HandoffRequest,
    pub receipt: DepartureReceiptWire,
    pub replayed: bool,
}

/// Exact historical handoff readback, including explicit absence.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HandoffQueryResponse {
    pub request: HandoffRequest,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub receipt: Option<DepartureReceiptWire>,
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
    use crate::wire::WorkspaceQueryScope;

    fn fixture_request() -> HandoffRequest {
        HandoffRequest {
            source: DepartureTarget {
                scope: WorkspaceQueryScope {
                    workspace_id: Uuid::from_u128(1),
                    location_id: Uuid::from_u128(2),
                    location_revision: 3,
                    workspace_revision: 4,
                    locator: "workspace".to_owned(),
                },
                activity_id: Uuid::from_u128(5),
                agent_id: Uuid::from_u128(6),
                session_id: Uuid::from_u128(7),
                expected_agent_revision: 8,
            },
            target_agent_id: Uuid::from_u128(9),
            target_session_id: Uuid::from_u128(10),
            expected_target_agent_revision: 11,
            expected_coordinator_revision: 12,
            expected_plan: HandoffPlanWire {
                goal: Some(HandoffGoalWire {
                    goal_id: Uuid::from_u128(13),
                    expected_revision: 14,
                }),
                dag_revision: Some(15),
                dag_head_revision: Some(16),
                node: Some(HandoffNodeWire {
                    node_id: Uuid::from_u128(17),
                    expected_revision: 18,
                }),
            },
            request_id: Uuid::from_u128(19),
        }
    }

    #[test]
    fn handoff_request_round_trips_complete_plan_and_explicit_nulls()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = fixture_request();
        let encoded = serde_json::to_value(&request)?;
        assert_eq!(
            serde_json::from_value::<HandoffRequest>(encoded.clone())?,
            request
        );
        assert_eq!(
            encoded["expectedPlan"]["goal"]["goalId"],
            Uuid::from_u128(13).to_string()
        );
        assert_eq!(encoded["expectedPlan"]["node"]["expectedRevision"], 18);

        let mut empty_plan = request;
        empty_plan.expected_plan = HandoffPlanWire {
            goal: None,
            dag_revision: None,
            dag_head_revision: None,
            node: None,
        };
        let encoded = serde_json::to_value(&empty_plan)?;
        for field in ["goal", "dagRevision", "dagHeadRevision", "node"] {
            assert_eq!(encoded["expectedPlan"][field], serde_json::Value::Null);
        }
        assert_eq!(
            serde_json::from_value::<HandoffRequest>(encoded)?,
            empty_plan
        );
        Ok(())
    }

    #[test]
    fn handoff_plan_and_query_require_nullable_fields_and_reject_unknowns()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = fixture_request();
        let mut encoded = serde_json::to_value(&request)?;
        encoded["expectedPlan"]
            .as_object_mut()
            .ok_or("plan JSON object")?
            .remove("dagRevision");
        assert!(serde_json::from_value::<HandoffRequest>(encoded).is_err());

        let query = HandoffQueryResponse {
            request: fixture_request(),
            receipt: None,
        };
        let mut encoded = serde_json::to_value(&query)?;
        assert_eq!(encoded["receipt"], serde_json::Value::Null);
        encoded
            .as_object_mut()
            .ok_or("query JSON object")?
            .remove("receipt");
        assert!(serde_json::from_value::<HandoffQueryResponse>(encoded).is_err());

        let mut unknown = serde_json::to_value(fixture_request())?;
        unknown["principal"] = serde_json::Value::Null;
        assert!(serde_json::from_value::<HandoffRequest>(unknown).is_err());
        Ok(())
    }
}
