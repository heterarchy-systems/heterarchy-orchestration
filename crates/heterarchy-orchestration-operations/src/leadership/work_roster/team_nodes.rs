//! Immutable snapshots of the selected Team's exact node-to-team mappings.

use crate::{OperationsError, wire};
use forge_domain::{
    ActivityId, DagNodeId, DagRevisionNumber, GoalId, GoalRevision, MAX_WORK_CUT_TEAM_NODES,
    TimestampMillis, WorkCutNodeScope, WorkCutScope, WorkspaceId,
};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TeamNodeWitnessWire {
    pub(super) activity: [u8; 16],
    pub(super) goal: [u8; 16],
    pub(super) goal_created_at_ms: i64,
    pub(super) dag_base_goal_revision: i64,
    pub(super) dag_revision: i64,
    pub(super) node: [u8; 16],
    pub(super) mapping_revision: i64,
}

impl TeamNodeWitnessWire {
    fn work_cut_node_scope(self) -> Result<WorkCutNodeScope, OperationsError> {
        let goal_created_at = TimestampMillis::new(self.goal_created_at_ms)
            .map_err(|_| OperationsError::Corrupt("work roster Team node timestamp"))?;
        let dag_base_goal_revision = GoalRevision::new(self.dag_base_goal_revision)
            .map_err(|_| OperationsError::Corrupt("work roster Team node Goal revision"))?;
        let dag_revision = DagRevisionNumber::new(self.dag_revision)
            .map_err(|_| OperationsError::Corrupt("work roster Team node DAG revision"))?;
        WorkCutNodeScope::new(
            ActivityId::from_uuid(Uuid::from_bytes(self.activity)),
            GoalId::from_uuid(Uuid::from_bytes(self.goal)),
            goal_created_at,
            dag_base_goal_revision,
            dag_revision,
            DagNodeId::from_uuid(Uuid::from_bytes(self.node)),
        )
        .map_err(|_| OperationsError::Corrupt("work roster Team node identity"))
    }
}

struct RawTeamNodeWitness {
    activity: Vec<u8>,
    goal: Vec<u8>,
    goal_created_at_ms: Option<i64>,
    dag_base_goal_revision: Option<i64>,
    dag_revision: i64,
    node: Vec<u8>,
    mapping_revision: i64,
}

pub(super) fn validate_team_nodes(nodes: &[TeamNodeWitnessWire]) -> Result<(), OperationsError> {
    if nodes.len() > MAX_WORK_CUT_TEAM_NODES {
        return Err(OperationsError::Corrupt("work roster Team node bounds"));
    }
    let mut previous = None;
    for witness in nodes {
        if witness.mapping_revision < 1 {
            return Err(OperationsError::Corrupt(
                "work roster Team node mapping revision",
            ));
        }
        let scope = witness.work_cut_node_scope()?;
        if previous.is_some_and(|value| value >= scope) {
            return Err(OperationsError::Corrupt("work roster Team node ordering"));
        }
        previous = Some(scope);
    }
    Ok(())
}

pub(super) fn capture_team_nodes(
    tx: &Connection,
    workspace: WorkspaceId,
    team: Uuid,
) -> Result<Vec<TeamNodeWitnessWire>, OperationsError> {
    let unresolved_legacy: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_dag_node_team_bindings WHERE workspace_id=?1 AND activity_id IS NULL AND team_id=?2 AND legacy_retirement_request_id IS NULL)",
        params![workspace.into_uuid().as_bytes(), team.as_bytes()],
        |row| row.get(0),
    )?;
    if unresolved_legacy {
        return Err(OperationsError::Conflict(
            "work roster Team has unresolved legacy node mappings",
        ));
    }

    let limit = i64::try_from(MAX_WORK_CUT_TEAM_NODES + 1)
        .map_err(|_| OperationsError::Bounds("work roster Team node limit"))?;
    let mut statement = tx.prepare(
        "SELECT substr(activity_id,1,17),substr(goal_id,1,17),goal_created_at_ms,dag_base_goal_revision,dag_revision,substr(node_id,1,17),revision FROM ops_dag_node_team_bindings WHERE workspace_id=?1 AND team_id=?2 AND activity_id IS NOT NULL AND legacy_retirement_request_id IS NULL ORDER BY activity_id,goal_id,goal_created_at_ms,dag_base_goal_revision,dag_revision,node_id LIMIT ?3",
    )?;
    let raw = statement
        .query_map(
            params![workspace.into_uuid().as_bytes(), team.as_bytes(), limit],
            |row| {
                Ok(RawTeamNodeWitness {
                    activity: row.get(0)?,
                    goal: row.get(1)?,
                    goal_created_at_ms: row.get(2)?,
                    dag_base_goal_revision: row.get(3)?,
                    dag_revision: row.get(4)?,
                    node: row.get(5)?,
                    mapping_revision: row.get(6)?,
                })
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;
    if raw.len() > MAX_WORK_CUT_TEAM_NODES {
        return Err(OperationsError::Bounds("work roster Team node mappings"));
    }

    let nodes = raw
        .into_iter()
        .map(|row| {
            Ok(TeamNodeWitnessWire {
                activity: wire::id(row.activity)?,
                goal: wire::id(row.goal)?,
                goal_created_at_ms: row.goal_created_at_ms.ok_or(OperationsError::Corrupt(
                    "work roster Team node Goal creation time",
                ))?,
                dag_base_goal_revision: row.dag_base_goal_revision.ok_or(
                    OperationsError::Corrupt("work roster Team node Goal revision"),
                )?,
                dag_revision: row.dag_revision,
                node: wire::id(row.node)?,
                mapping_revision: row.mapping_revision,
            })
        })
        .collect::<Result<Vec<_>, OperationsError>>()?;
    validate_team_nodes(&nodes)?;
    Ok(nodes)
}

pub(super) fn forge_work_cut_scope(
    team: Uuid,
    nodes: &[TeamNodeWitnessWire],
) -> Result<WorkCutScope, OperationsError> {
    validate_team_nodes(nodes)?;
    Ok(WorkCutScope::Team {
        team_id: team,
        nodes: nodes
            .iter()
            .copied()
            .map(TeamNodeWitnessWire::work_cut_node_scope)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

pub(super) fn ensure_team_node_mapping_open(
    tx: &Connection,
    workspace: WorkspaceId,
    current_team: Option<Uuid>,
    next_team: Option<Uuid>,
) -> Result<(), OperationsError> {
    if current_team.is_none() && next_team.is_none() {
        return Ok(());
    }
    let blocked: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_leadership_work_rosters AS r JOIN ops_leadership_elections AS e ON e.workspace_id=r.workspace_id AND e.election_id=r.election_id WHERE r.workspace_id=?1 AND e.scope_kind=2 AND e.team_id IN(?2,?3) AND r.state IN(0,1))",
        params![
            workspace.into_uuid().as_bytes(),
            current_team.as_ref().map(|id| id.as_bytes().as_slice()),
            next_team.as_ref().map(|id| id.as_bytes().as_slice()),
        ],
        |row| row.get(0),
    )?;
    if blocked {
        return Err(OperationsError::Conflict(
            "Team node mappings are frozen for leadership work cut",
        ));
    }
    Ok(())
}
