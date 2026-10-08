//! Bounded client for the Goal/DAG source reads.

use std::collections::BTreeSet;

use forge_domain::{
    DagHeadRevision, DagNodeId, DagNodeOrdinal, DagNodeRevision, DagRevisionNumber, GoalRevision,
    MAX_DAG_EDGES, MAX_DAG_NODES, TimestampMillis,
};
use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use super::super::{ClientError, OrchestrationHttpClient, canonical_identity};
use crate::wire::forge_reads::{
    DagCurrentQueryRequest, DagCurrentQueryResponse, DagCurrentWire, DagNodeWire,
    GoalGetQueryRequest, GoalGetQueryResponse, GoalListQueryRequest, GoalListQueryResponse,
    GoalWire,
};
use crate::wire::leadership::WorkspaceQueryScope;

const MAX_WORKSPACE_LOCATOR_BYTES: usize = 4096;
const MAX_GOAL_LIST_RESULTS: u32 = 100;

impl OrchestrationHttpClient {
    /// Observe the bounded Goal list for one exact Activity.
    ///
    /// # Errors
    /// Returns invalid input, a bounded transport error, a typed service
    /// rejection, or a protocol error for a response exceeding its bounds.
    pub async fn goal_list(
        &self,
        authorization: &HeaderValue,
        request: &GoalListQueryRequest,
    ) -> Result<GoalListQueryResponse, ClientError> {
        validate_common(&request.scope, request.session_id, request.activity_id)?;
        if request.limit == 0 || request.limit > MAX_GOAL_LIST_RESULTS {
            return Err(ClientError::InvalidRequest);
        }
        let response: GoalListQueryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/forge/goal-list",
                    request.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if response.goals.len() > usize::try_from(request.limit).unwrap_or(usize::MAX)
            || !response
                .goals
                .iter()
                .all(|goal| valid_goal(goal, request.scope.workspace_id))
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Observe one exact Goal row; a missing row returns `goal: null`.
    ///
    /// # Errors
    /// Returns invalid input, a bounded transport error, a typed service
    /// rejection, or a protocol error for an inconsistent response.
    pub async fn goal_get(
        &self,
        authorization: &HeaderValue,
        request: &GoalGetQueryRequest,
    ) -> Result<GoalGetQueryResponse, ClientError> {
        validate_common(&request.scope, request.session_id, request.activity_id)?;
        if request.goal_id.is_nil() {
            return Err(ClientError::InvalidRequest);
        }
        let response: GoalGetQueryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/forge/goal-get",
                    request.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if let Some(goal) = &response.goal
            && (!valid_goal(goal, request.scope.workspace_id)
                || goal.goal_id != request.goal_id.to_string())
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }

    /// Observe the current DAG revision for one Goal; a missing DAG returns
    /// `dag: null`.
    ///
    /// # Errors
    /// Returns invalid input, a bounded transport error, a typed service
    /// rejection, or a protocol error for an inconsistent response.
    pub async fn dag_get_current(
        &self,
        authorization: &HeaderValue,
        request: &DagCurrentQueryRequest,
    ) -> Result<DagCurrentQueryResponse, ClientError> {
        validate_common(&request.scope, request.session_id, request.activity_id)?;
        if request.goal_id.is_nil() {
            return Err(ClientError::InvalidRequest);
        }
        let response: DagCurrentQueryResponse = self
            .call(
                authorization,
                Method::POST,
                format!(
                    "/internal/v1/workspaces/{}/forge/dag-get-current",
                    request.scope.workspace_id
                ),
                Some(request),
                None,
            )
            .await?;
        if let Some(dag) = &response.dag
            && !valid_dag(dag, request.goal_id)
        {
            return Err(ClientError::Protocol);
        }
        Ok(response)
    }
}

fn validate_common(
    scope: &WorkspaceQueryScope,
    session_id: Option<Uuid>,
    activity_id: Uuid,
) -> Result<(), ClientError> {
    if scope.workspace_id.is_nil()
        || scope.location_id.is_nil()
        || scope.location_revision <= 0
        || scope.workspace_revision <= 0
        || scope.locator.is_empty()
        || scope.locator.len() > MAX_WORKSPACE_LOCATOR_BYTES
        || scope.locator.chars().any(char::is_control)
        || session_id.is_some_and(|id| id.is_nil())
        || activity_id.is_nil()
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn valid_goal(goal: &GoalWire, workspace_id: Uuid) -> bool {
    canonical_identity(&goal.goal_id).is_ok()
        && goal.workspace_id == workspace_id.to_string()
        && goal.revision > 0
}

fn valid_node(node: &DagNodeWire) -> bool {
    canonical_node_id(&node.node_id).is_some()
        && DagNodeOrdinal::new(node.ordinal).is_ok()
        && DagNodeRevision::new(node.revision).is_ok()
        && TimestampMillis::new(node.created_at_ms).is_ok()
        && TimestampMillis::new(node.updated_at_ms).is_ok()
        && node.updated_at_ms >= node.created_at_ms
}

fn valid_dag(dag: &DagCurrentWire, requested_goal_id: Uuid) -> bool {
    if canonical_identity(&dag.goal_id).ok() != Some(requested_goal_id)
        || dag.goal_id != requested_goal_id.to_string()
        || canonical_identity(&dag.revision_id).is_err()
        || GoalRevision::new(dag.base_goal_revision).is_err()
        || DagHeadRevision::new(dag.head_revision).is_err()
        || TimestampMillis::new(dag.created_at_ms).is_err()
        || TimestampMillis::new(dag.head_updated_at_ms).is_err()
        || !(1..=MAX_DAG_NODES).contains(&dag.nodes.len())
        || dag.edges.len() > MAX_DAG_EDGES
    {
        return false;
    }

    let Ok(dag_revision) = DagRevisionNumber::new(dag.dag_revision) else {
        return false;
    };
    let parent_is_valid = match dag.parent_revision {
        None => dag_revision == DagRevisionNumber::INITIAL,
        Some(parent_revision) => {
            DagRevisionNumber::new(parent_revision).and_then(DagRevisionNumber::checked_next)
                == Ok(dag_revision)
        }
    };
    if !parent_is_valid || !dag.nodes.iter().all(valid_node) {
        return false;
    }

    let Some(node_ids) = dag
        .nodes
        .iter()
        .map(|node| canonical_node_id(&node.node_id))
        .collect::<Option<Vec<_>>>()
    else {
        return false;
    };
    let known_node_ids = node_ids.into_iter().collect::<BTreeSet<_>>();
    dag.nodes.iter().all(|node| {
        node.waiting_on.iter().all(|node_id| {
            canonical_node_id(node_id).is_some_and(|node_id| known_node_ids.contains(&node_id))
        })
    }) && dag.edges.iter().all(|edge| {
        canonical_node_id(&edge.upstream).is_some_and(|node_id| known_node_ids.contains(&node_id))
            && canonical_node_id(&edge.downstream)
                .is_some_and(|node_id| known_node_ids.contains(&node_id))
    })
}

fn canonical_node_id(value: &str) -> Option<DagNodeId> {
    canonical_identity(value).ok().map(DagNodeId::from_uuid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::forge_reads::{DagCurrentWire, DagLifecycleWire, DagStatusWire};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn scope() -> WorkspaceQueryScope {
        WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 3,
            workspace_revision: 4,
            locator: "locator".to_owned(),
        }
    }

    #[tokio::test]
    async fn goal_list_and_get_reject_foreign_workspace_and_malformed_ids() -> TestResult {
        use crate::client::tests::{fixture, reply};
        use serde_json::json;
        let scope = scope();
        let goal_id = Uuid::from_u128(9);
        let valid = json!({"goalId":goal_id,"workspaceId":scope.workspace_id,"revision":2,
            "status":{"kind":"active","reason":null},"objective":"owner goal",
            "successCriteria":[],"constraints":[],"outOfScope":[],
            "verificationRequirements":[],"stopConditions":[],"createdAtMs":1,"updatedAtMs":2});
        let mut foreign = valid.clone();
        foreign["workspaceId"] = json!(Uuid::from_u128(99));
        let mut malformed = valid.clone();
        malformed["goalId"] = json!("not-a-goal-id");
        let mut nil = valid.clone();
        nil["goalId"] = json!(Uuid::nil());
        for goal in [foreign, malformed, nil] {
            let (client, server) =
                fixture(Some(reply("200 OK", &json!({"goals":[goal.clone()]})))).await?;
            let request = GoalListQueryRequest {
                scope: scope.clone(),
                session_id: None,
                activity_id: Uuid::from_u128(5),
                limit: 32,
            };
            assert!(matches!(
                client
                    .goal_list(
                        &HeaderValue::from_static("Bearer identity-fixture"),
                        &request
                    )
                    .await,
                Err(ClientError::Protocol)
            ));
            server.await??;
            let (client, server) = fixture(Some(reply("200 OK", &json!({"goal":goal})))).await?;
            let request = GoalGetQueryRequest {
                scope: scope.clone(),
                session_id: None,
                activity_id: Uuid::from_u128(5),
                goal_id,
            };
            assert!(matches!(
                client
                    .goal_get(
                        &HeaderValue::from_static("Bearer identity-fixture"),
                        &request
                    )
                    .await,
                Err(ClientError::Protocol)
            ));
            server.await??;
        }
        Ok(())
    }

    #[tokio::test]
    async fn dag_read_rejects_invalid_revision_and_node_identities() -> TestResult {
        use crate::client::tests::{fixture, reply};
        use serde_json::json;
        let request = DagCurrentQueryRequest {
            scope: scope(),
            session_id: None,
            activity_id: Uuid::from_u128(6),
            goal_id: Uuid::from_u128(7),
        };
        let mut valid = json!({"goalId":request.goal_id,"revisionId":Uuid::from_u128(8),"baseGoalRevision":1,"dagRevision":2,"parentRevision":1,"createdAtMs":3,"headRevision":4,"headUpdatedAtMs":5,
            "nodes":[{"nodeId":Uuid::from_u128(9),"ordinal":0,"title":"node","objective":"owner node","acceptance":[],"lifecycle":"pending","status":"ready","waitingOn":[],"revision":1,"evidenceCount":0,"createdAtMs":3,"updatedAtMs":3}],"edges":[]});
        let mut dependent = valid["nodes"][0].clone();
        dependent["nodeId"] = json!(Uuid::from_u128(10));
        dependent["ordinal"] = json!(1);
        dependent["status"] = json!("pending");
        dependent["waitingOn"] = json!([Uuid::from_u128(9)]);
        valid["nodes"]
            .as_array_mut()
            .ok_or("nodes array")?
            .push(dependent);
        valid["edges"] = json!([{"upstream":Uuid::from_u128(9),"downstream":Uuid::from_u128(10)}]);
        let (client, server) =
            fixture(Some(reply("200 OK", &json!({"dag":valid.clone()})))).await?;
        assert!(
            client
                .dag_get_current(&HeaderValue::from_static("Bearer dag-identity"), &request)
                .await?
                .dag
                .is_some()
        );
        server.await??;
        let mut revision_id = valid.clone();
        revision_id["revisionId"] = json!("not-a-revision-id");
        let mut node_id = valid.clone();
        node_id["nodes"][0]["nodeId"] = json!("not-a-node-id");
        let mut zero = valid.clone();
        zero["dagRevision"] = json!(0);
        let mut parent = valid.clone();
        parent["parentRevision"] = json!(null);
        let mut unknown_dependency = valid.clone();
        unknown_dependency["nodes"][0]["waitingOn"] = json!([Uuid::from_u128(99)]);
        let mut unknown_edge = valid.clone();
        unknown_edge["edges"] =
            json!([{"upstream":Uuid::from_u128(9),"downstream":Uuid::from_u128(99)}]);
        for invalid in [
            revision_id,
            node_id,
            zero,
            parent,
            unknown_dependency,
            unknown_edge,
        ] {
            let (client, server) = fixture(Some(reply("200 OK", &json!({"dag":invalid})))).await?;
            assert!(matches!(
                client
                    .dag_get_current(&HeaderValue::from_static("Bearer dag-identity"), &request)
                    .await,
                Err(ClientError::Protocol)
            ));
            server.await??;
        }
        Ok(())
    }

    #[test]
    fn goal_list_bounds_and_goal_identity_contract_is_enforced() -> TestResult {
        let request = GoalListQueryRequest {
            scope: scope(),
            session_id: None,
            activity_id: Uuid::from_u128(5),
            limit: 32,
        };
        validate_common(&request.scope, request.session_id, request.activity_id)?;
        assert!(request.limit > 0 && request.limit <= MAX_GOAL_LIST_RESULTS);

        let goal = GoalWire {
            goal_id: Uuid::from_u128(9).to_string(),
            workspace_id: request.scope.workspace_id.to_string(),
            revision: 2,
            status: crate::wire::forge_reads::GoalStatusWire {
                kind: crate::wire::forge_reads::GoalStatusKindWire::Active,
                reason: None,
            },
            objective: "obj".to_owned(),
            success_criteria: vec![],
            constraints: vec![],
            out_of_scope: vec![],
            verification_requirements: vec![],
            stop_conditions: vec![],
            created_at_ms: 1,
            updated_at_ms: 2,
        };
        assert!(valid_goal(&goal, request.scope.workspace_id));

        let nil_activity = GoalListQueryRequest {
            scope: scope(),
            session_id: None,
            activity_id: Uuid::nil(),
            limit: 1,
        };
        assert!(
            validate_common(
                &nil_activity.scope,
                nil_activity.session_id,
                nil_activity.activity_id
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn dag_response_must_echo_the_requested_goal_identity() {
        let request = DagCurrentQueryRequest {
            scope: scope(),
            session_id: None,
            activity_id: Uuid::from_u128(6),
            goal_id: Uuid::from_u128(7),
        };
        let dag = DagCurrentWire {
            goal_id: Uuid::from_u128(7).to_string(),
            revision_id: Uuid::from_u128(8).to_string(),
            base_goal_revision: 1,
            dag_revision: 1,
            parent_revision: None,
            created_at_ms: 3,
            head_revision: 4,
            head_updated_at_ms: 5,
            nodes: vec![DagNodeWire {
                node_id: Uuid::from_u128(9).to_string(),
                ordinal: 0,
                title: "node".to_owned(),
                objective: "objective".to_owned(),
                acceptance: vec![],
                lifecycle: DagLifecycleWire::Pending,
                status: DagStatusWire::Pending,
                waiting_on: vec![],
                revision: 1,
                evidence_count: 0,
                created_at_ms: 3,
                updated_at_ms: 3,
            }],
            edges: vec![],
        };
        assert_eq!(dag.goal_id, request.goal_id.to_string());
        assert!(valid_dag(&dag, request.goal_id));
    }
}
