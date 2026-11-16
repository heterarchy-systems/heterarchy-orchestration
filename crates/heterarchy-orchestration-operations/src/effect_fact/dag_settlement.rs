//! Durable `DagSettled` effect-fact projection, atomic governance linkage, and
//! decentralized `Team Leader` vs exclusive `Workspace Orchestrator` authority checks.

use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, GoalId, WorkspaceId};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::dag_settlement::sql::{
    COUNT_ACCEPTED_WORK_CREDITS_SQL, COUNT_EVALUATED_ASSIGNMENTS_SQL, DAG_SETTLED_SQL,
    DAG_SETTLEMENT_GOVERNANCE_LINKS_SQL, EXISTS_DAG_SETTLEMENT_IDENTITY_SQL,
    INSERT_DAG_SETTLEMENT_GOVERNANCE_LINK_SQL, INSERT_DAG_SETTLEMENT_PROJECTION_SQL,
    SELECT_ACTIVE_WORKSPACE_LEADERSHIP_REVISION_SQL, SELECT_CALLER_EFFECT_BINDING_PARTICIPANT_SQL,
    SELECT_COORDINATOR_PARTICIPANT_SQL, SELECT_DAG_SETTLEMENT_GOVERNANCE_LINK_SQL,
    SELECT_DAG_SETTLEMENT_PROJECTION_PAGE_SQL, SELECT_GOVERNANCE_LINK_SETTLEMENT_SQL,
    SELECT_LATEST_STARTED_MISSION_PLAN_SQL, SELECT_TEAM_LEADER_AUTHORITY_SQL,
    SELECT_WORKSPACE_ORCHESTRATOR_AUTHORITY_SQL,
};
use crate::{NodeTeamBindingScope, OperationalActor, OperationsCatalog, OperationsError, wire};

const OBJECTS: &[(&str, &str, &str)] = &[
    ("table", "ops_dag_settlement_projection", DAG_SETTLED_SQL),
    (
        "table",
        "ops_dag_settlement_governance_links",
        DAG_SETTLEMENT_GOVERNANCE_LINKS_SQL,
    ),
];

pub(crate) const fn schema_objects() -> &'static [(&'static str, &'static str, &'static str)] {
    OBJECTS
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    connection.execute_batch(DAG_SETTLED_SQL)?;
    connection.execute_batch(DAG_SETTLEMENT_GOVERNANCE_LINKS_SQL)?;
    Ok(())
}

/// Transport-neutral `DagSettled` effect-fact candidate before catalog commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DagSettledFact {
    pub sequence: u64,
    pub migration_id: Uuid,
    pub destination_id: Uuid,
    pub settlement_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub activity_id: ActivityId,
    pub goal_id: GoalId,
    pub goal_revision: i64,
    pub dag_revision: i64,
    pub settled_file_count: u64,
    pub settled_tree_digest: [u8; 32],
    pub coordinator_agent_id: AgentInstanceId,
    pub coordinator_session_id: Option<AgentSessionId>,
    pub settled_at_ms: i64,
}

/// Durable `DagSettled` projection row read back from the catalog.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DagSettledProjectionRow {
    migration_id: Uuid,
    source_sequence: u64,
    fact_digest: [u8; 32],
    settlement_id: Uuid,
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    goal_id: GoalId,
    goal_revision: i64,
    dag_revision: i64,
    settled_file_count: u64,
    settled_tree_digest: [u8; 32],
    coordinator_agent_id: AgentInstanceId,
    coordinator_session_id: Option<AgentSessionId>,
    settled_at_ms: i64,
}

impl DagSettledProjectionRow {
    #[must_use]
    pub const fn migration_id(self) -> Uuid {
        self.migration_id
    }
    #[must_use]
    pub const fn source_sequence(self) -> u64 {
        self.source_sequence
    }
    #[must_use]
    pub const fn fact_digest(self) -> [u8; 32] {
        self.fact_digest
    }
    #[must_use]
    pub const fn settlement_id(self) -> Uuid {
        self.settlement_id
    }
    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        self.workspace_id
    }
    #[must_use]
    pub const fn activity_id(self) -> ActivityId {
        self.activity_id
    }
    #[must_use]
    pub const fn goal_id(self) -> GoalId {
        self.goal_id
    }
    #[must_use]
    pub const fn goal_revision(self) -> i64 {
        self.goal_revision
    }
    #[must_use]
    pub const fn dag_revision(self) -> i64 {
        self.dag_revision
    }
    #[must_use]
    pub const fn settled_file_count(self) -> u64 {
        self.settled_file_count
    }
    #[must_use]
    pub const fn settled_tree_digest(self) -> [u8; 32] {
        self.settled_tree_digest
    }
    #[must_use]
    pub const fn coordinator_agent_id(self) -> AgentInstanceId {
        self.coordinator_agent_id
    }
    #[must_use]
    pub const fn coordinator_session_id(self) -> Option<AgentSessionId> {
        self.coordinator_session_id
    }
    #[must_use]
    pub const fn settled_at_ms(self) -> i64 {
        self.settled_at_ms
    }
}

/// Durable governance link derived atomically inside the `DagSettled` consumer transaction.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DagSettlementGovernanceLink {
    workspace_id: WorkspaceId,
    goal_id: GoalId,
    dag_revision: i64,
    migration_id: Uuid,
    source_sequence: u64,
    settlement_id: Uuid,
    coordinator_agent_id: AgentInstanceId,
    coordinator_participant_id: Option<Uuid>,
    workspace_leadership_revision: Option<i64>,
    accepted_work_credit_count: u64,
    evaluated_assignment_count: u64,
    started_mission_plan_id: Option<Uuid>,
    started_mission_plan_revision: Option<u64>,
    settled_at_ms: i64,
}

impl DagSettlementGovernanceLink {
    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        self.workspace_id
    }
    #[must_use]
    pub const fn goal_id(self) -> GoalId {
        self.goal_id
    }
    #[must_use]
    pub const fn dag_revision(self) -> i64 {
        self.dag_revision
    }
    #[must_use]
    pub const fn migration_id(self) -> Uuid {
        self.migration_id
    }
    #[must_use]
    pub const fn source_sequence(self) -> u64 {
        self.source_sequence
    }
    #[must_use]
    pub const fn settlement_id(self) -> Uuid {
        self.settlement_id
    }
    #[must_use]
    pub const fn coordinator_agent_id(self) -> AgentInstanceId {
        self.coordinator_agent_id
    }
    #[must_use]
    pub const fn coordinator_participant_id(self) -> Option<Uuid> {
        self.coordinator_participant_id
    }
    #[must_use]
    pub const fn workspace_leadership_revision(self) -> Option<i64> {
        self.workspace_leadership_revision
    }
    #[must_use]
    pub const fn accepted_work_credit_count(self) -> u64 {
        self.accepted_work_credit_count
    }
    #[must_use]
    pub const fn evaluated_assignment_count(self) -> u64 {
        self.evaluated_assignment_count
    }
    #[must_use]
    pub const fn started_mission_plan_id(self) -> Option<Uuid> {
        self.started_mission_plan_id
    }
    #[must_use]
    pub const fn started_mission_plan_revision(self) -> Option<u64> {
        self.started_mission_plan_revision
    }
    #[must_use]
    pub const fn settled_at_ms(self) -> i64 {
        self.settled_at_ms
    }
    /// Whether the goal's evaluated assignment fulfillment satisfies the 30% Recall voter gate.
    #[must_use]
    pub const fn recall_voter_fulfillment_eligible(self) -> bool {
        if self.evaluated_assignment_count == 0 {
            return false;
        }
        let completed = self.accepted_work_credit_count as u128;
        let evaluated = self.evaluated_assignment_count as u128;
        completed * (crate::leadership::VOTER_FULFILLMENT_INELIGIBLE_DEN as u128)
            > evaluated * (crate::leadership::VOTER_FULFILLMENT_INELIGIBLE_NUM as u128)
    }
}

/// Verified `Workspace Orchestrator` fence witness required before `dag.settle`.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkspaceDagSettlementAuthority {
    workspace_id: WorkspaceId,
    leader_participant_id: Option<Uuid>,
    leadership_revision: i64,
}

impl WorkspaceDagSettlementAuthority {
    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        self.workspace_id
    }
    #[must_use]
    pub const fn leader_participant_id(self) -> Option<Uuid> {
        self.leader_participant_id
    }
    #[must_use]
    pub const fn leadership_revision(self) -> i64 {
        self.leadership_revision
    }
}

/// Verified decentralized `Team Leader` witness for node-level `start` / `complete`.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeLifecycleAuthority {
    scope: NodeTeamBindingScope,
    team_id: Uuid,
    leader_participant_id: Uuid,
    team_leadership_revision: i64,
}

impl NodeLifecycleAuthority {
    pub const fn scope(self) -> NodeTeamBindingScope {
        self.scope
    }
    #[must_use]
    pub const fn team_id(self) -> Uuid {
        self.team_id
    }
    #[must_use]
    pub const fn leader_participant_id(self) -> Uuid {
        self.leader_participant_id
    }
    #[must_use]
    pub const fn team_leadership_revision(self) -> i64 {
        self.team_leadership_revision
    }
}

pub(super) fn dag_settled_digest(fact: &DagSettledFact) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"heterarchy.orchestration.effect-fact.dag-settled.v1\0");
    digest.update(fact.sequence.to_be_bytes());
    digest.update(fact.migration_id.as_bytes());
    digest.update(fact.destination_id.as_bytes());
    digest.update(fact.settlement_id.as_bytes());
    digest.update(fact.workspace_id.into_uuid().as_bytes());
    digest.update(fact.activity_id.into_uuid().as_bytes());
    digest.update(fact.goal_id.into_uuid().as_bytes());
    digest.update(fact.goal_revision.to_be_bytes());
    digest.update(fact.dag_revision.to_be_bytes());
    digest.update(fact.settled_file_count.to_be_bytes());
    digest.update(fact.settled_tree_digest);
    digest.update(fact.coordinator_agent_id.into_uuid().as_bytes());
    super::update_optional_uuid(
        &mut digest,
        fact.coordinator_session_id
            .map(forge_domain::AgentSessionId::into_uuid),
    );
    digest.update(fact.settled_at_ms.to_be_bytes());
    digest.finalize().into()
}

pub(super) fn insert_dag_settled_projection(
    transaction: &Transaction<'_>,
    fact: &DagSettledFact,
) -> Result<(), OperationsError> {
    let duplicate: bool = transaction.query_row(
        EXISTS_DAG_SETTLEMENT_IDENTITY_SQL,
        params![
            fact.migration_id.as_bytes(),
            fact.settlement_id.as_bytes(),
            fact.goal_id.into_uuid().as_bytes(),
            fact.dag_revision,
        ],
        |row| row.get(0),
    )?;
    if duplicate {
        return Err(OperationsError::Conflict(
            "effect fact dag settlement identity",
        ));
    }
    let settled_file_count = i64::try_from(fact.settled_file_count)
        .map_err(|_| OperationsError::Bounds("dag settled file count"))?;
    transaction.execute(
        INSERT_DAG_SETTLEMENT_PROJECTION_SQL,
        params![
            fact.migration_id.as_bytes(),
            super::signed(fact.sequence)?,
            dag_settled_digest(fact).as_slice(),
            fact.settlement_id.as_bytes(),
            fact.workspace_id.into_uuid().as_bytes(),
            fact.activity_id.into_uuid().as_bytes(),
            fact.goal_id.into_uuid().as_bytes(),
            fact.goal_revision,
            fact.dag_revision,
            settled_file_count,
            fact.settled_tree_digest.as_slice(),
            fact.coordinator_agent_id.into_uuid().as_bytes(),
            fact.coordinator_session_id
                .map(|id| id.into_uuid().as_bytes().to_vec()),
            fact.settled_at_ms,
        ],
    )?;
    Ok(())
}

pub(super) fn derive_dag_settlement_governance(
    transaction: &Transaction<'_>,
    fact: &DagSettledFact,
) -> Result<(), OperationsError> {
    let existing: Option<Vec<u8>> = transaction
        .query_row(
            SELECT_GOVERNANCE_LINK_SETTLEMENT_SQL,
            params![
                fact.workspace_id.into_uuid().as_bytes(),
                fact.goal_id.into_uuid().as_bytes(),
                fact.dag_revision,
            ],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(raw_settlement) = existing {
        let recorded = Uuid::from_bytes(wire::id(raw_settlement)?);
        return if recorded == fact.settlement_id {
            Ok(())
        } else {
            Err(OperationsError::Conflict("dag settlement governance link"))
        };
    }

    let coordinator_participant_id = transaction
        .query_row(
            SELECT_COORDINATOR_PARTICIPANT_SQL,
            params![
                fact.workspace_id.into_uuid().as_bytes(),
                fact.activity_id.into_uuid().as_bytes(),
                fact.coordinator_agent_id.into_uuid().as_bytes(),
            ],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?
        .map(wire::id)
        .transpose()?
        .map(Uuid::from_bytes);

    let workspace_leadership_revision: Option<i64> = transaction
        .query_row(
            SELECT_ACTIVE_WORKSPACE_LEADERSHIP_REVISION_SQL,
            params![fact.workspace_id.into_uuid().as_bytes()],
            |row| row.get(0),
        )
        .optional()?;

    let accepted_work_credit_count: i64 = transaction.query_row(
        COUNT_ACCEPTED_WORK_CREDITS_SQL,
        params![
            fact.workspace_id.into_uuid().as_bytes(),
            fact.goal_id.into_uuid().as_bytes(),
        ],
        |row| row.get(0),
    )?;
    let evaluated_assignment_count: i64 = transaction.query_row(
        COUNT_EVALUATED_ASSIGNMENTS_SQL,
        params![
            fact.workspace_id.into_uuid().as_bytes(),
            fact.goal_id.into_uuid().as_bytes(),
        ],
        |row| row.get(0),
    )?;
    let started_mission_plan: Option<(Vec<u8>, i64)> = transaction
        .query_row(
            SELECT_LATEST_STARTED_MISSION_PLAN_SQL,
            params![fact.workspace_id.into_uuid().as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let (plan_id_bytes, plan_revision) = match started_mission_plan {
        Some((raw_plan, revision)) => (Some(wire::id(raw_plan)?.to_vec()), Some(revision)),
        None => (None, None),
    };

    transaction.execute(
        INSERT_DAG_SETTLEMENT_GOVERNANCE_LINK_SQL,
        params![
            fact.workspace_id.into_uuid().as_bytes(),
            fact.goal_id.into_uuid().as_bytes(),
            fact.dag_revision,
            fact.migration_id.as_bytes(),
            super::signed(fact.sequence)?,
            fact.settlement_id.as_bytes(),
            fact.coordinator_agent_id.into_uuid().as_bytes(),
            coordinator_participant_id.map(|id| id.into_bytes().to_vec()),
            workspace_leadership_revision,
            accepted_work_credit_count,
            evaluated_assignment_count,
            plan_id_bytes,
            plan_revision,
            fact.settled_at_ms,
        ],
    )?;
    Ok(())
}

#[derive(Debug)]
struct RawDagSettledRow {
    migration: Vec<u8>,
    sequence: i64,
    digest: Vec<u8>,
    settlement: Vec<u8>,
    workspace: Vec<u8>,
    activity: Vec<u8>,
    goal: Vec<u8>,
    goal_revision: i64,
    dag_revision: i64,
    settled_file_count: i64,
    settled_tree_digest: Vec<u8>,
    coordinator_agent: Vec<u8>,
    coordinator_session: Option<Vec<u8>>,
    settled_at_ms: i64,
}

pub(super) fn load_dag_settled_projection(
    connection: &Connection,
    migration_id: Uuid,
) -> Result<Vec<DagSettledProjectionRow>, OperationsError> {
    let limit = super::page_limit()?;
    let mut statement = connection.prepare(SELECT_DAG_SETTLEMENT_PROJECTION_PAGE_SQL)?;
    let rows = statement
        .query_map(params![migration_id.as_bytes(), limit], |row| {
            Ok(RawDagSettledRow {
                migration: row.get(0)?,
                sequence: row.get(1)?,
                digest: row.get(2)?,
                settlement: row.get(3)?,
                workspace: row.get(4)?,
                activity: row.get(5)?,
                goal: row.get(6)?,
                goal_revision: row.get(7)?,
                dag_revision: row.get(8)?,
                settled_file_count: row.get(9)?,
                settled_tree_digest: row.get(10)?,
                coordinator_agent: row.get(11)?,
                coordinator_session: row.get(12)?,
                settled_at_ms: row.get(13)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    rows.into_iter().map(decode_dag_settled_row).collect()
}

fn decode_dag_settled_row(
    row: RawDagSettledRow,
) -> Result<DagSettledProjectionRow, OperationsError> {
    let settled_tree_digest: [u8; 32] = row
        .settled_tree_digest
        .try_into()
        .map_err(|_| OperationsError::Corrupt("effect fact dag settled tree digest"))?;
    if settled_tree_digest == [0_u8; 32] {
        return Err(OperationsError::Corrupt(
            "effect fact dag settled tree digest",
        ));
    }
    Ok(DagSettledProjectionRow {
        migration_id: Uuid::from_bytes(wire::id(row.migration)?),
        source_sequence: u64::try_from(row.sequence)
            .map_err(|_| OperationsError::Corrupt("effect fact projection sequence"))?,
        fact_digest: row
            .digest
            .try_into()
            .map_err(|_| OperationsError::Corrupt("effect fact projection digest"))?,
        settlement_id: Uuid::from_bytes(wire::id(row.settlement)?),
        workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(row.workspace)?)),
        activity_id: ActivityId::from_uuid(Uuid::from_bytes(wire::id(row.activity)?)),
        goal_id: GoalId::from_uuid(Uuid::from_bytes(wire::id(row.goal)?)),
        goal_revision: row.goal_revision,
        dag_revision: row.dag_revision,
        settled_file_count: u64::try_from(row.settled_file_count)
            .map_err(|_| OperationsError::Corrupt("effect fact dag settled file count"))?,
        settled_tree_digest,
        coordinator_agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(
            row.coordinator_agent,
        )?)),
        coordinator_session_id: row
            .coordinator_session
            .map(wire::id)
            .transpose()?
            .map(|id| AgentSessionId::from_uuid(Uuid::from_bytes(id))),
        settled_at_ms: row.settled_at_ms,
    })
}

#[derive(Debug)]
struct RawGovernanceLinkRow {
    migration: Vec<u8>,
    sequence: i64,
    settlement: Vec<u8>,
    coordinator_agent: Vec<u8>,
    coordinator_participant: Option<Vec<u8>>,
    leadership_revision: Option<i64>,
    accepted_credits: i64,
    evaluated_assignments: i64,
    plan_id: Option<Vec<u8>>,
    plan_revision: Option<i64>,
    settled_at_ms: i64,
}

fn load_governance_link(
    connection: &Connection,
    workspace_id: WorkspaceId,
    goal_id: GoalId,
    dag_revision: i64,
) -> Result<Option<DagSettlementGovernanceLink>, OperationsError> {
    if workspace_id.into_uuid().is_nil() || goal_id.into_uuid().is_nil() || dag_revision < 1 {
        return Err(OperationsError::Conflict(
            "dag settlement governance lookup",
        ));
    }
    let raw: Option<RawGovernanceLinkRow> = connection
        .query_row(
            SELECT_DAG_SETTLEMENT_GOVERNANCE_LINK_SQL,
            params![
                workspace_id.into_uuid().as_bytes(),
                goal_id.into_uuid().as_bytes(),
                dag_revision,
            ],
            |row| {
                Ok(RawGovernanceLinkRow {
                    migration: row.get(0)?,
                    sequence: row.get(1)?,
                    settlement: row.get(2)?,
                    coordinator_agent: row.get(3)?,
                    coordinator_participant: row.get(4)?,
                    leadership_revision: row.get(5)?,
                    accepted_credits: row.get(6)?,
                    evaluated_assignments: row.get(7)?,
                    plan_id: row.get(8)?,
                    plan_revision: row.get(9)?,
                    settled_at_ms: row.get(10)?,
                })
            },
        )
        .optional()?;
    raw.map(|row| decode_governance_link(workspace_id, goal_id, dag_revision, row))
        .transpose()
}

fn decode_governance_link(
    workspace_id: WorkspaceId,
    goal_id: GoalId,
    dag_revision: i64,
    row: RawGovernanceLinkRow,
) -> Result<DagSettlementGovernanceLink, OperationsError> {
    let started_mission_plan_id = row.plan_id.map(wire::id).transpose()?.map(Uuid::from_bytes);
    let started_mission_plan_revision = row
        .plan_revision
        .map(u64::try_from)
        .transpose()
        .map_err(|_| OperationsError::Corrupt("dag settlement plan revision"))?;
    Ok(DagSettlementGovernanceLink {
        workspace_id,
        goal_id,
        dag_revision,
        migration_id: Uuid::from_bytes(wire::id(row.migration)?),
        source_sequence: u64::try_from(row.sequence)
            .map_err(|_| OperationsError::Corrupt("dag settlement sequence"))?,
        settlement_id: Uuid::from_bytes(wire::id(row.settlement)?),
        coordinator_agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(
            row.coordinator_agent,
        )?)),
        coordinator_participant_id: row
            .coordinator_participant
            .map(wire::id)
            .transpose()?
            .map(Uuid::from_bytes),
        workspace_leadership_revision: row.leadership_revision,
        accepted_work_credit_count: u64::try_from(row.accepted_credits)
            .map_err(|_| OperationsError::Corrupt("dag settlement work credit count"))?,
        evaluated_assignment_count: u64::try_from(row.evaluated_assignments)
            .map_err(|_| OperationsError::Corrupt("dag settlement evaluated count"))?,
        started_mission_plan_id,
        started_mission_plan_revision,
        settled_at_ms: row.settled_at_ms,
    })
}

impl OperationsCatalog {
    /// Read one bounded page of durable `DagSettled` projections ordered by source sequence.
    ///
    /// # Errors
    /// Rejects malformed persisted projections and database read failures.
    pub fn dag_settled_projection(
        &self,
        migration_id: Uuid,
    ) -> Result<Vec<DagSettledProjectionRow>, OperationsError> {
        load_dag_settled_projection(&self.connection, migration_id)
    }

    /// Read the durable governance link derived when `(workspace_id, goal_id, dag_revision)` settled.
    ///
    /// # Errors
    /// Rejects invalid lookup coordinates, corrupt rows, and database read failures.
    pub fn dag_settlement_governance_link(
        &self,
        workspace_id: WorkspaceId,
        goal_id: GoalId,
        dag_revision: i64,
    ) -> Result<Option<DagSettlementGovernanceLink>, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        let link = load_governance_link(&tx, workspace_id, goal_id, dag_revision)?;
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(link)
    }

    /// Authorize an exclusive `Workspace Orchestrator` (`scope_kind = 0`) to invoke `dag.settle`,
    /// fenced by the exact active `leadership_revision`. Decentralized `Team Leader`s (`scope_kind = 2`)
    /// are rejected fail-closed unless they also hold active Workspace Orchestrator leadership.
    ///
    /// # Errors
    /// Rejects non-active Workspace leadership, stale `expected_leadership_revision`, or non-orchestrator callers.
    pub fn authorize_workspace_dag_settlement(
        &self,
        workspace_id: WorkspaceId,
        actor: OperationalActor,
        expected_leadership_revision: i64,
    ) -> Result<WorkspaceDagSettlementAuthority, OperationsError> {
        if workspace_id.into_uuid().is_nil() || expected_leadership_revision < 1 {
            return Err(OperationsError::Conflict("dag settlement leadership fence"));
        }
        let tx = self.connection.unchecked_transaction()?;
        let row: Option<(String, Option<Vec<u8>>, i64)> = tx
            .query_row(
                SELECT_WORKSPACE_ORCHESTRATOR_AUTHORITY_SQL,
                params![workspace_id.into_uuid().as_bytes()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let Some((state, leader_raw, revision)) = row else {
            return Err(OperationsError::Conflict("workspace orchestrator missing"));
        };
        if state != "ACTIVE" || revision != expected_leadership_revision {
            return Err(OperationsError::Conflict(
                "workspace orchestrator fence mismatch",
            ));
        }
        let leader_participant_id = leader_raw
            .map(wire::id)
            .transpose()?
            .map(Uuid::from_bytes)
            .ok_or(OperationsError::Corrupt(
                "workspace orchestrator participant",
            ))?;
        match actor {
            OperationalActor::LocalOperator => {}
            OperationalActor::Participant {
                activity_id,
                agent_id,
                session_id,
            } => {
                let participant_raw: Option<Vec<u8>> = tx
                    .query_row(
                        SELECT_CALLER_EFFECT_BINDING_PARTICIPANT_SQL,
                        params![
                            workspace_id.into_uuid().as_bytes(),
                            activity_id.into_uuid().as_bytes(),
                            agent_id.into_uuid().as_bytes(),
                            session_id.into_uuid().as_bytes(),
                        ],
                        |r| r.get(0),
                    )
                    .optional()?;
                let caller_participant = participant_raw
                    .map(wire::id)
                    .transpose()?
                    .map(Uuid::from_bytes)
                    .ok_or(OperationsError::Conflict("dag settlement caller unbound"))?;
                if caller_participant != leader_participant_id {
                    return Err(OperationsError::Conflict(
                        "dag settlement requires workspace orchestrator",
                    ));
                }
            }
        }
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(WorkspaceDagSettlementAuthority {
            workspace_id,
            leader_participant_id: Some(leader_participant_id),
            leadership_revision: revision,
        })
    }

    /// Authorize a decentralized `Team Leader` (`scope_kind = 2`) to start or complete a DAG node
    /// bound to their active Team via `ops_dag_node_team_bindings`.
    ///
    /// # Errors
    /// Rejects unbound/tombstoned nodes, stale `expected_team_leadership_revision`, or non-leader callers.
    pub fn authorize_team_leader_node_transition(
        &self,
        scope: NodeTeamBindingScope,
        actor: OperationalActor,
        expected_team_leadership_revision: i64,
    ) -> Result<NodeLifecycleAuthority, OperationsError> {
        if expected_team_leadership_revision < 1 {
            return Err(OperationsError::Conflict("team leadership fence"));
        }
        let binding = self
            .node_team_binding(scope)?
            .ok_or(OperationsError::Conflict("node team binding missing"))?;
        let team_id = binding
            .team_id()
            .ok_or(OperationsError::Conflict("node team binding tombstoned"))?;
        let tx = self.connection.unchecked_transaction()?;
        let row: Option<(String, Option<Vec<u8>>, i64)> = tx
            .query_row(
                SELECT_TEAM_LEADER_AUTHORITY_SQL,
                params![
                    scope.workspace_id().into_uuid().as_bytes(),
                    team_id.as_bytes()
                ],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let Some((state, leader_raw, revision)) = row else {
            return Err(OperationsError::Conflict("team leader missing"));
        };
        if state != "ACTIVE" || revision != expected_team_leadership_revision {
            return Err(OperationsError::Conflict("team leader fence mismatch"));
        }
        let leader_participant_id = leader_raw
            .map(wire::id)
            .transpose()?
            .map(Uuid::from_bytes)
            .ok_or(OperationsError::Corrupt("team leader participant"))?;
        match actor {
            OperationalActor::LocalOperator => {}
            OperationalActor::Participant {
                activity_id,
                agent_id,
                session_id,
            } => {
                let participant_raw: Option<Vec<u8>> = tx
                    .query_row(
                        SELECT_CALLER_EFFECT_BINDING_PARTICIPANT_SQL,
                        params![
                            scope.workspace_id().into_uuid().as_bytes(),
                            activity_id.into_uuid().as_bytes(),
                            agent_id.into_uuid().as_bytes(),
                            session_id.into_uuid().as_bytes(),
                        ],
                        |r| r.get(0),
                    )
                    .optional()?;
                let caller_participant = participant_raw
                    .map(wire::id)
                    .transpose()?
                    .map(Uuid::from_bytes)
                    .ok_or(OperationsError::Conflict("team node caller unbound"))?;
                if caller_participant != leader_participant_id {
                    return Err(OperationsError::Conflict(
                        "node transition requires team leader",
                    ));
                }
            }
        }
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(NodeLifecycleAuthority {
            scope,
            team_id,
            leader_participant_id,
            team_leadership_revision: revision,
        })
    }
}

#[cfg(test)]
mod tests {
    use forge_domain::{
        AgentAssignmentId, DagNodeId, DagRevisionNumber, GoalRevision, TimestampMillis,
        WorkCreditBudget,
    };
    use forge_persistence::AssignmentReviewedOutcome;

    use super::*;
    use crate::{AssignmentReviewedFact, EffectFactInput};

    struct SettlementFixture {
        root: std::path::PathBuf,
        catalog: OperationsCatalog,
        destination: Uuid,
        migration: Uuid,
        workspace: WorkspaceId,
        activity: ActivityId,
        goal: GoalId,
    }

    impl SettlementFixture {
        fn new(label: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let root = std::env::temp_dir().join(format!(
                "orchestration-dag-settlement-{label}-{}",
                Uuid::now_v7()
            ));
            std::fs::create_dir_all(&root)?;
            let destination = Uuid::from_u128(0xD501);
            let migration = Uuid::from_u128(0xD502);
            let workspace = WorkspaceId::from_uuid(Uuid::from_u128(0xD503));
            let activity = ActivityId::from_uuid(Uuid::from_u128(0xD504));
            let goal = GoalId::from_uuid(Uuid::from_u128(0xD505));
            let catalog = OperationsCatalog::open(&root.join("catalog.sqlite3"), destination)?;
            catalog.connection.execute(
                "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
                params![
                    migration.as_bytes(),
                    workspace.into_uuid().as_bytes(),
                    destination.as_bytes(),
                    [0_u8; 32].as_slice(),
                ],
            )?;
            Ok(Self {
                root,
                catalog,
                destination,
                migration,
                workspace,
                activity,
                goal,
            })
        }
    }

    impl Drop for SettlementFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    fn sample_facts(
        fx: &SettlementFixture,
    ) -> (
        crate::effect_fact::ExecutionTerminalFact,
        AssignmentReviewedFact,
        DagSettledFact,
    ) {
        let node_id = DagNodeId::from_uuid(Uuid::from_u128(0xD511));
        let target_agent_id = AgentInstanceId::from_uuid(Uuid::from_u128(0xD512));
        let target_session_id = Some(AgentSessionId::from_uuid(Uuid::from_u128(0xD513)));
        let execution = crate::effect_fact::ExecutionTerminalFact {
            sequence: 1,
            migration_id: fx.migration,
            destination_id: fx.destination,
            workspace_id: fx.workspace,
            execution_id: forge_domain::ExecutionId::from_uuid(Uuid::from_u128(0xD509)),
            activity_id: fx.activity,
            execution_revision: 3,
            execution_state: forge_domain::ExecutionState::Completed,
            observed_at_ms: 900,
            agent_id: target_agent_id,
            agent_revision: 2,
            session_id: target_session_id,
            goal_id: fx.goal,
            goal_revision: 2,
            dag_revision: 3,
            node_id,
            node_revision: 2,
            verification_input_id: Some(Uuid::from_u128(0xD599)),
            verification_input_fingerprint: Some([7_u8; 32]),
            verification_disposition: Some(0),
            verification_output_integrity: Some(0),
            verification_output_fingerprint: Some([8_u8; 32]),
        };
        let reviewed = AssignmentReviewedFact {
            sequence: 2,
            migration_id: fx.migration,
            destination_id: fx.destination,
            workspace_id: fx.workspace,
            assignment_id: AgentAssignmentId::from_uuid(Uuid::from_u128(0xD510)),
            activity_id: fx.activity,
            goal_id: fx.goal,
            node_id,
            target_agent_id,
            target_session_id,
            outcome: AssignmentReviewedOutcome::Accepted,
            credit_budget: Some(WorkCreditBudget::Three),
            reviewer_agent_id: AgentInstanceId::from_uuid(Uuid::from_u128(0xD514)),
            reviewer_session_id: Some(AgentSessionId::from_uuid(Uuid::from_u128(0xD515))),
            result_digest: Some([0x11; 32]),
            observed_at_ms: 1_000,
        };
        let settled = DagSettledFact {
            sequence: 3,
            migration_id: fx.migration,
            destination_id: fx.destination,
            settlement_id: Uuid::from_u128(0xD520),
            workspace_id: fx.workspace,
            activity_id: fx.activity,
            goal_id: fx.goal,
            goal_revision: 2,
            dag_revision: 3,
            settled_file_count: 5,
            settled_tree_digest: [0x42; 32],
            coordinator_agent_id: AgentInstanceId::from_uuid(Uuid::from_u128(0xD521)),
            coordinator_session_id: Some(AgentSessionId::from_uuid(Uuid::from_u128(0xD522))),
            settled_at_ms: 2_000,
        };
        (execution, reviewed, settled)
    }

    #[test]
    fn dag_settled_projects_replays_preserves_work_credit_and_rejects_gaps()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fx = SettlementFixture::new("projection")?;
        let (execution, reviewed, settled) = sample_facts(&fx);

        let progress = fx.catalog.apply_effect_fact_inputs(
            fx.migration,
            &[
                EffectFactInput::Execution(execution),
                EffectFactInput::AssignmentReviewed(reviewed),
                EffectFactInput::DagSettled(settled),
            ],
        )?;
        assert_eq!(progress.last_sequence(), 3);
        let credit_rows: i64 = fx.catalog.connection.query_row(
            "SELECT COUNT(*) FROM ops_work_credit_entries WHERE workspace_id=?1",
            params![fx.workspace.into_uuid().as_bytes()],
            |row| row.get(0),
        )?;
        assert_eq!(credit_rows, 1);

        let rows = fx.catalog.dag_settled_projection(fx.migration)?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].settlement_id(), settled.settlement_id);
        assert_eq!(rows[0].settled_file_count(), 5);

        let link = fx
            .catalog
            .dag_settlement_governance_link(fx.workspace, fx.goal, 3)?
            .ok_or("missing governance link")?;
        assert_eq!(link.accepted_work_credit_count(), 1);
        assert_eq!(link.evaluated_assignment_count(), 1);
        assert!(link.recall_voter_fulfillment_eligible());

        let replay = fx.catalog.apply_effect_fact_inputs(
            fx.migration,
            &[
                EffectFactInput::Execution(execution),
                EffectFactInput::AssignmentReviewed(reviewed),
                EffectFactInput::DagSettled(settled),
            ],
        )?;
        assert_eq!(replay.last_sequence(), 3);

        let mut divergent = settled;
        divergent.settled_file_count = 9;
        assert!(
            fx.catalog
                .apply_effect_fact_inputs(fx.migration, &[EffectFactInput::DagSettled(divergent)])
                .is_err()
        );

        let mut gapped = settled;
        gapped.sequence = 5;
        gapped.settlement_id = Uuid::from_u128(0xD529);
        gapped.dag_revision = 4;
        assert!(
            fx.catalog
                .apply_effect_fact_inputs(fx.migration, &[EffectFactInput::DagSettled(gapped)])
                .is_err()
        );
        assert_eq!(fx.catalog.dag_settled_projection(fx.migration)?.len(), 1);
        Ok(())
    }

    fn seed_participant_binding(
        fx: &SettlementFixture,
        participant_id: Uuid,
        name: &str,
        agent: AgentInstanceId,
        session: AgentSessionId,
    ) -> Result<(), rusqlite::Error> {
        fx.catalog.connection.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,?4,'leader',1,1,1)",
            params![
                fx.workspace.into_uuid().as_bytes(),
                participant_id.as_bytes(),
                session.into_uuid().as_bytes(),
                name,
            ],
        )?;
        fx.catalog.connection.execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
            params![
                fx.workspace.into_uuid().as_bytes(),
                participant_id.as_bytes(),
                fx.activity.into_uuid().as_bytes(),
                session.into_uuid().as_bytes(),
                agent.into_uuid().as_bytes(),
                Uuid::now_v7().as_bytes(),
            ],
        )?;
        Ok(())
    }

    #[test]
    fn leadership_fences_separate_team_leader_nodes_from_workspace_dag_settlement()
    -> Result<(), Box<dyn std::error::Error>> {
        let fx = SettlementFixture::new("authority")?;
        let orchestrator_participant = Uuid::from_u128(0xD601);
        let team_leader_participant = Uuid::from_u128(0xD602);
        let team_id = Uuid::from_u128(0xD603);
        let node_id = DagNodeId::from_uuid(Uuid::from_u128(0xD604));
        let orch_agent = AgentInstanceId::from_uuid(Uuid::from_u128(0xD611));
        let orch_session = AgentSessionId::from_uuid(Uuid::from_u128(0xD612));
        let tl_agent = AgentInstanceId::from_uuid(Uuid::from_u128(0xD621));
        let tl_session = AgentSessionId::from_uuid(Uuid::from_u128(0xD622));

        seed_participant_binding(
            &fx,
            orchestrator_participant,
            "orch",
            orch_agent,
            orch_session,
        )?;
        seed_participant_binding(&fx, team_leader_participant, "tl", tl_agent, tl_session)?;
        fx.catalog.connection.execute(
            "INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_at_ms,updated_at_ms,formed_at_ms) VALUES(?1,?2,'alpha','exec','ACTIVE',1,'participant:tl','tl',1,1,1)",
            params![team_id.as_bytes(), fx.workspace.into_uuid().as_bytes()],
        )?;
        fx.catalog.connection.execute(
            "INSERT INTO ops_dag_node_team_bindings(workspace_id,activity_id,goal_id,goal_created_at_ms,dag_revision,dag_base_goal_revision,node_id,team_id,bound_by,bound_at_ms,revision) VALUES(?1,?2,?3,1,1,1,?4,?5,'operator',1,1)",
            params![
                fx.workspace.into_uuid().as_bytes(),
                fx.activity.into_uuid().as_bytes(),
                fx.goal.into_uuid().as_bytes(),
                node_id.into_uuid().as_bytes(),
                team_id.as_bytes(),
            ],
        )?;
        fx.catalog.connection.execute(
            "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,0,NULL,NULL,'ACTIVE',?2,3,1,NULL,1),(?1,2,NULL,?3,'ACTIVE',?4,2,1,NULL,1)",
            params![
                fx.workspace.into_uuid().as_bytes(),
                orchestrator_participant.as_bytes(),
                team_id.as_bytes(),
                team_leader_participant.as_bytes(),
            ],
        )?;

        let node_scope = NodeTeamBindingScope::new(
            fx.workspace,
            fx.activity,
            fx.goal,
            TimestampMillis::new(1)?,
            DagRevisionNumber::INITIAL,
            GoalRevision::INITIAL,
            node_id,
        )?;
        let tl_actor = OperationalActor::Participant {
            activity_id: fx.activity,
            agent_id: tl_agent,
            session_id: tl_session,
        };
        let orch_actor = OperationalActor::Participant {
            activity_id: fx.activity,
            agent_id: orch_agent,
            session_id: orch_session,
        };

        let tl_auth = fx
            .catalog
            .authorize_team_leader_node_transition(node_scope, tl_actor, 2)?;
        assert_eq!(tl_auth.team_id(), team_id);
        assert!(
            fx.catalog
                .authorize_workspace_dag_settlement(fx.workspace, tl_actor, 3)
                .is_err()
        );

        let orch_auth =
            fx.catalog
                .authorize_workspace_dag_settlement(fx.workspace, orch_actor, 3)?;
        assert_eq!(orch_auth.leadership_revision(), 3);
        assert!(
            fx.catalog
                .authorize_workspace_dag_settlement(fx.workspace, orch_actor, 2)
                .is_err()
        );
        Ok(())
    }
}
