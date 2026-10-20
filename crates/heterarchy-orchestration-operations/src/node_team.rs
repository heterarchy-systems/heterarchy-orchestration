//! Durable node-to-team responsibility binding for the Workbench.
//!
//! A binding is scoped to one immutable Forge Goal/DAG generation. Historical
//! rows without that generation remain aggregate-only migration evidence.

use forge_domain::{
    ActivityId, AgentInstanceId, AgentSessionId, DagNodeId, DagRevisionNumber, GoalId,
    GoalRevision, TimestampMillis, WorkspaceId,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use uuid::Uuid;

use crate::{
    OperationalActor, OperationsCatalog, OperationsError, leadership,
    operational_actor::resolve_actor, wire,
};

pub const MAX_NODE_TEAM_BINDINGS_PER_READ: usize = 64;

pub(crate) const NODE_TEAM_BINDINGS_V35_SQL: &str = "CREATE TABLE ops_dag_node_team_bindings (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id != zeroblob(16)), goal_id BLOB NOT NULL CHECK(length(goal_id)=16 AND goal_id != zeroblob(16)), dag_revision INTEGER NOT NULL CHECK(dag_revision>=1), node_id BLOB NOT NULL CHECK(length(node_id)=16 AND node_id != zeroblob(16)), team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id != zeroblob(16)), bound_by TEXT NOT NULL CHECK(bound_by IN('operator','orchestrator')), bound_at_ms INTEGER NOT NULL CHECK(bound_at_ms>=0), revision INTEGER NOT NULL CHECK(revision>=1), PRIMARY KEY(workspace_id,goal_id,dag_revision,node_id))";

const NODE_TEAM_BINDINGS_SQL: &str = "CREATE TABLE ops_dag_node_team_bindings (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id != zeroblob(16)), activity_id BLOB CHECK(activity_id IS NULL OR (length(activity_id)=16 AND activity_id != zeroblob(16))), goal_id BLOB NOT NULL CHECK(length(goal_id)=16 AND goal_id != zeroblob(16)), goal_created_at_ms INTEGER CHECK(goal_created_at_ms IS NULL OR goal_created_at_ms>=0), dag_revision INTEGER NOT NULL CHECK(dag_revision>=1), dag_base_goal_revision INTEGER CHECK(dag_base_goal_revision IS NULL OR dag_base_goal_revision>=1), node_id BLOB NOT NULL CHECK(length(node_id)=16 AND node_id != zeroblob(16)), team_id BLOB CHECK(team_id IS NULL OR (length(team_id)=16 AND team_id != zeroblob(16))), bound_by TEXT NOT NULL CHECK(bound_by IN('operator','orchestrator')), bound_at_ms INTEGER NOT NULL CHECK(bound_at_ms>=0), revision INTEGER NOT NULL CHECK(revision>=1), legacy_retirement_request_id BLOB CHECK(legacy_retirement_request_id IS NULL OR (length(legacy_retirement_request_id)=16 AND legacy_retirement_request_id != zeroblob(16))), legacy_retired_at_ms INTEGER CHECK(legacy_retired_at_ms IS NULL OR legacy_retired_at_ms>=0), legacy_retired_by TEXT CHECK(legacy_retired_by IS NULL OR legacy_retired_by IN('operator','orchestrator')), legacy_retired_activity_id BLOB CHECK(legacy_retired_activity_id IS NULL OR (length(legacy_retired_activity_id)=16 AND legacy_retired_activity_id != zeroblob(16))), legacy_retired_agent_id BLOB CHECK(legacy_retired_agent_id IS NULL OR (length(legacy_retired_agent_id)=16 AND legacy_retired_agent_id != zeroblob(16))), legacy_retired_session_id BLOB CHECK(legacy_retired_session_id IS NULL OR (length(legacy_retired_session_id)=16 AND legacy_retired_session_id != zeroblob(16))), legacy_retired_participant_id BLOB CHECK(legacy_retired_participant_id IS NULL OR (length(legacy_retired_participant_id)=16 AND legacy_retired_participant_id != zeroblob(16))), legacy_retired_leadership_revision INTEGER CHECK(legacy_retired_leadership_revision IS NULL OR legacy_retired_leadership_revision>=1), PRIMARY KEY(workspace_id,activity_id,goal_id,goal_created_at_ms,dag_revision,dag_base_goal_revision,node_id), CHECK((activity_id IS NULL AND goal_created_at_ms IS NULL AND dag_base_goal_revision IS NULL) OR (activity_id IS NOT NULL AND goal_created_at_ms IS NOT NULL AND dag_base_goal_revision IS NOT NULL)), CHECK(legacy_retirement_request_id IS NULL OR activity_id IS NULL), CHECK((legacy_retirement_request_id IS NULL AND legacy_retired_at_ms IS NULL AND legacy_retired_by IS NULL AND legacy_retired_activity_id IS NULL AND legacy_retired_agent_id IS NULL AND legacy_retired_session_id IS NULL AND legacy_retired_participant_id IS NULL AND legacy_retired_leadership_revision IS NULL) OR (legacy_retirement_request_id IS NOT NULL AND legacy_retired_at_ms IS NOT NULL AND legacy_retired_by='operator' AND legacy_retired_activity_id IS NULL AND legacy_retired_agent_id IS NULL AND legacy_retired_session_id IS NULL AND legacy_retired_participant_id IS NULL AND legacy_retired_leadership_revision IS NULL) OR (legacy_retirement_request_id IS NOT NULL AND legacy_retired_at_ms IS NOT NULL AND legacy_retired_by='orchestrator' AND legacy_retired_activity_id IS NOT NULL AND legacy_retired_agent_id IS NOT NULL AND legacy_retired_session_id IS NOT NULL AND legacy_retired_participant_id IS NOT NULL AND legacy_retired_leadership_revision IS NOT NULL)))";
const NODE_TEAM_BINDINGS_SCOPE_INDEX: &str = "CREATE INDEX ops_dag_node_team_bindings_scope_idx ON ops_dag_node_team_bindings(workspace_id,goal_id,dag_revision,node_id,activity_id,goal_created_at_ms,dag_base_goal_revision)";
const NODE_TEAM_BINDINGS_LEGACY_RETIREMENT_INDEX: &str = "CREATE UNIQUE INDEX ops_dag_node_team_bindings_legacy_retirement_request_idx ON ops_dag_node_team_bindings(legacy_retirement_request_id) WHERE legacy_retirement_request_id IS NOT NULL";

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        (
            "table",
            "ops_dag_node_team_bindings",
            NODE_TEAM_BINDINGS_SQL,
        ),
        (
            "index",
            "ops_dag_node_team_bindings_scope_idx",
            NODE_TEAM_BINDINGS_SCOPE_INDEX,
        ),
        (
            "index",
            "ops_dag_node_team_bindings_legacy_retirement_request_idx",
            NODE_TEAM_BINDINGS_LEGACY_RETIREMENT_INDEX,
        ),
    ]
}

pub(crate) fn schema_objects_v35() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![(
        "table",
        "ops_dag_node_team_bindings",
        NODE_TEAM_BINDINGS_V35_SQL,
    )]
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in schema_objects() {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

/// Add exact generation columns while retaining each old row as unknown scope.
pub(crate) fn migrate_v35_to_v36(tx: &Transaction<'_>) -> Result<(), OperationsError> {
    tx.execute_batch(
        "ALTER TABLE ops_dag_node_team_bindings RENAME TO ops_dag_node_team_bindings_v35",
    )?;
    create_schema(tx)?;
    tx.execute(
        "INSERT INTO ops_dag_node_team_bindings(workspace_id,activity_id,goal_id,goal_created_at_ms,dag_revision,dag_base_goal_revision,node_id,team_id,bound_by,bound_at_ms,revision) SELECT workspace_id,NULL,goal_id,NULL,dag_revision,NULL,node_id,team_id,bound_by,bound_at_ms,revision FROM ops_dag_node_team_bindings_v35",
        [],
    )?;
    tx.execute_batch("DROP TABLE ops_dag_node_team_bindings_v35")?;
    Ok(())
}

/// One exact immutable Activity/Goal/DAG/node identity.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeTeamBindingScope {
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    goal_id: GoalId,
    goal_created_at: TimestampMillis,
    dag_revision: DagRevisionNumber,
    dag_base_goal_revision: GoalRevision,
    node_id: DagNodeId,
}

impl NodeTeamBindingScope {
    /// Construct a validated exact binding identity.
    ///
    /// # Errors
    /// Rejects nil UUIDs.
    pub fn new(
        workspace_id: WorkspaceId,
        activity_id: ActivityId,
        goal_id: GoalId,
        goal_created_at: TimestampMillis,
        dag_revision: DagRevisionNumber,
        dag_base_goal_revision: GoalRevision,
        node_id: DagNodeId,
    ) -> Result<Self, OperationsError> {
        if workspace_id.into_uuid().is_nil()
            || activity_id.into_uuid().is_nil()
            || goal_id.into_uuid().is_nil()
            || node_id.into_uuid().is_nil()
        {
            return Err(OperationsError::Conflict("node binding scope identity"));
        }
        Ok(Self {
            workspace_id,
            activity_id,
            goal_id,
            goal_created_at,
            dag_revision,
            dag_base_goal_revision,
            node_id,
        })
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
    pub const fn goal_created_at(self) -> TimestampMillis {
        self.goal_created_at
    }

    #[must_use]
    pub const fn dag_revision(self) -> DagRevisionNumber {
        self.dag_revision
    }

    #[must_use]
    pub const fn dag_base_goal_revision(self) -> GoalRevision {
        self.dag_base_goal_revision
    }

    #[must_use]
    pub const fn node_id(self) -> DagNodeId {
        self.node_id
    }
}

/// A compare-and-swap binding mutation. `team_id = None` writes a retained
/// unbound tombstone; `expected_revision = None` means no exact row existed.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetNodeTeamBindingCommand {
    pub scope: NodeTeamBindingScope,
    pub expected_revision: Option<u64>,
    pub team_id: Option<Uuid>,
    pub actor: OperationalActor,
    pub expected_sender_leadership_revision: Option<i64>,
    pub observed_at: TimestampMillis,
}

/// One request to retire one historical binding whose exact generation was
/// never recorded. It preserves the old row and its revision as evidence.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetireLegacyNodeTeamBindingCommand {
    pub workspace_id: WorkspaceId,
    pub goal_id: GoalId,
    pub dag_revision: DagRevisionNumber,
    pub node_id: DagNodeId,
    pub expected_revision: u64,
    pub request_id: Uuid,
    pub actor: OperationalActor,
    pub expected_sender_leadership_revision: Option<i64>,
    pub observed_at: TimestampMillis,
}

/// The authority evidence accepted by one legacy-binding retirement CAS.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyNodeTeamBindingRetirementAuthority {
    LocalOperator,
    WorkspaceLeader {
        activity_id: ActivityId,
        agent_id: AgentInstanceId,
        session_id: AgentSessionId,
        participant_id: Uuid,
        leadership_revision: i64,
    },
}

/// Immutable audit/readback of one retired legacy binding row.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyNodeTeamBindingRetirementReceipt {
    request_id: Uuid,
    workspace_id: WorkspaceId,
    goal_id: GoalId,
    dag_revision: DagRevisionNumber,
    node_id: DagNodeId,
    team_id: Option<Uuid>,
    bound_by: String,
    bound_at: TimestampMillis,
    binding_revision: u64,
    retired_at: TimestampMillis,
    authority: LegacyNodeTeamBindingRetirementAuthority,
    replayed: bool,
}

impl LegacyNodeTeamBindingRetirementReceipt {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }
    #[must_use]
    pub const fn goal_id(&self) -> GoalId {
        self.goal_id
    }
    #[must_use]
    pub const fn dag_revision(&self) -> DagRevisionNumber {
        self.dag_revision
    }
    #[must_use]
    pub const fn node_id(&self) -> DagNodeId {
        self.node_id
    }
    #[must_use]
    pub const fn team_id(&self) -> Option<Uuid> {
        self.team_id
    }
    #[must_use]
    pub fn bound_by(&self) -> &str {
        &self.bound_by
    }
    #[must_use]
    pub const fn bound_at(&self) -> TimestampMillis {
        self.bound_at
    }
    #[must_use]
    pub const fn binding_revision(&self) -> u64 {
        self.binding_revision
    }
    #[must_use]
    pub const fn retired_at(&self) -> TimestampMillis {
        self.retired_at
    }
    pub const fn authority(&self) -> LegacyNodeTeamBindingRetirementAuthority {
        self.authority
    }
    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// One exact binding or an aggregate-only legacy row when `scope` is absent.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeTeamBinding {
    scope: Option<NodeTeamBindingScope>,
    goal_id: GoalId,
    dag_revision: DagRevisionNumber,
    node_id: DagNodeId,
    team_id: Option<Uuid>,
    bound_by: String,
    bound_at: TimestampMillis,
    revision: u64,
    legacy_retirement: Option<LegacyNodeTeamBindingRetirementReceipt>,
}

impl NodeTeamBinding {
    #[must_use]
    pub const fn scope(&self) -> Option<NodeTeamBindingScope> {
        self.scope
    }

    #[must_use]
    pub const fn team_id(&self) -> Option<Uuid> {
        self.team_id
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub fn bound_by(&self) -> &str {
        &self.bound_by
    }

    #[must_use]
    pub const fn bound_at(&self) -> TimestampMillis {
        self.bound_at
    }

    #[must_use]
    pub const fn goal_id(&self) -> GoalId {
        self.goal_id
    }

    #[must_use]
    pub const fn dag_revision(&self) -> DagRevisionNumber {
        self.dag_revision
    }

    #[must_use]
    pub const fn node_id(&self) -> DagNodeId {
        self.node_id
    }

    #[must_use]
    pub const fn legacy_retirement(&self) -> Option<&LegacyNodeTeamBindingRetirementReceipt> {
        self.legacy_retirement.as_ref()
    }
}

/// Bounded aggregate binding read; `truncated` indicates retained rows beyond
/// the current page without silently implying that the Workspace has no more.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeTeamBindingPage {
    bindings: Vec<NodeTeamBinding>,
    truncated: bool,
}

impl NodeTeamBindingPage {
    pub fn bindings(&self) -> &[NodeTeamBinding] {
        &self.bindings
    }

    #[must_use]
    pub const fn truncated(&self) -> bool {
        self.truncated
    }
}

#[derive(Debug, Clone)]
struct LegacyRetirementColumns {
    request_id: Option<Vec<u8>>,
    retired_at: Option<i64>,
    retired_by: Option<String>,
    activity_id: Option<Vec<u8>>,
    agent_id: Option<Vec<u8>>,
    session_id: Option<Vec<u8>>,
    participant_id: Option<Vec<u8>>,
    leadership_revision: Option<i64>,
}

impl LegacyRetirementColumns {
    fn from_row(row: &rusqlite::Row<'_>, start: usize) -> rusqlite::Result<Self> {
        Ok(Self {
            request_id: row.get(start)?,
            retired_at: row.get(start + 1)?,
            retired_by: row.get(start + 2)?,
            activity_id: row.get(start + 3)?,
            agent_id: row.get(start + 4)?,
            session_id: row.get(start + 5)?,
            participant_id: row.get(start + 6)?,
            leadership_revision: row.get(start + 7)?,
        })
    }
}

#[derive(Debug, Clone)]
struct LegacyBindingSummary {
    workspace_id: WorkspaceId,
    goal_id: GoalId,
    dag_revision: DagRevisionNumber,
    node_id: DagNodeId,
    team_id: Option<Uuid>,
    bound_by: String,
    bound_at: TimestampMillis,
    revision: u64,
}

fn decode_legacy_retirement(
    summary: &LegacyBindingSummary,
    columns: LegacyRetirementColumns,
) -> Result<Option<LegacyNodeTeamBindingRetirementReceipt>, OperationsError> {
    let all_absent = columns.request_id.is_none()
        && columns.retired_at.is_none()
        && columns.retired_by.is_none()
        && columns.activity_id.is_none()
        && columns.agent_id.is_none()
        && columns.session_id.is_none()
        && columns.participant_id.is_none()
        && columns.leadership_revision.is_none();
    if all_absent {
        return Ok(None);
    }
    let request_id = Uuid::from_bytes(wire::id(columns.request_id.ok_or(
        OperationsError::Corrupt("legacy binding retirement request"),
    )?)?);
    let retired_at = TimestampMillis::new(
        columns
            .retired_at
            .ok_or(OperationsError::Corrupt("legacy binding retirement time"))?,
    )
    .map_err(|_| OperationsError::Corrupt("legacy binding retirement time"))?;
    let authority = match columns.retired_by.as_deref() {
        Some("operator")
            if columns.activity_id.is_none()
                && columns.agent_id.is_none()
                && columns.session_id.is_none()
                && columns.participant_id.is_none()
                && columns.leadership_revision.is_none() =>
        {
            LegacyNodeTeamBindingRetirementAuthority::LocalOperator
        }
        Some("orchestrator") => LegacyNodeTeamBindingRetirementAuthority::WorkspaceLeader {
            activity_id: ActivityId::from_uuid(Uuid::from_bytes(wire::id(
                columns
                    .activity_id
                    .ok_or(OperationsError::Corrupt("legacy retirement Activity"))?,
            )?)),
            agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(
                columns
                    .agent_id
                    .ok_or(OperationsError::Corrupt("legacy retirement Agent"))?,
            )?)),
            session_id: AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(
                columns
                    .session_id
                    .ok_or(OperationsError::Corrupt("legacy retirement session"))?,
            )?)),
            participant_id: Uuid::from_bytes(wire::id(
                columns
                    .participant_id
                    .ok_or(OperationsError::Corrupt("legacy retirement participant"))?,
            )?),
            leadership_revision: columns
                .leadership_revision
                .filter(|revision| *revision >= 1)
                .ok_or(OperationsError::Corrupt(
                    "legacy retirement leadership revision",
                ))?,
        },
        _ => {
            return Err(OperationsError::Corrupt(
                "legacy binding retirement authority",
            ));
        }
    };
    Ok(Some(LegacyNodeTeamBindingRetirementReceipt {
        request_id,
        workspace_id: summary.workspace_id,
        goal_id: summary.goal_id,
        dag_revision: summary.dag_revision,
        node_id: summary.node_id,
        team_id: summary.team_id,
        bound_by: summary.bound_by.clone(),
        bound_at: summary.bound_at,
        binding_revision: summary.revision,
        retired_at,
        authority,
        replayed: false,
    }))
}

impl OperationsCatalog {
    /// Set or tombstone one exact binding using a revision-checked write.
    ///
    /// # Errors
    /// Rejects stale revisions, non-active Teams, unauthorized actors, live
    /// assignment affinities and catalog failures.
    pub fn set_node_team_binding(
        &mut self,
        command: SetNodeTeamBindingCommand,
    ) -> Result<NodeTeamBinding, OperationsError> {
        set_node_team_binding(&mut self.connection, command)
    }

    /// Read one exact generation. A returned binding with no Team is an
    /// explicit retained tombstone; no row means no observed binding.
    ///
    /// # Errors
    /// Rejects corrupt rows and database read failures.
    pub fn node_team_binding(
        &self,
        scope: NodeTeamBindingScope,
    ) -> Result<Option<NodeTeamBinding>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let binding = read_node_team_binding_in_transaction(&transaction, scope)?;
        crate::catalog::commit_read(transaction, self.destination_id())?;
        Ok(binding)
    }

    /// Read one aggregate-only legacy row by its retained coordinates.
    ///
    /// # Errors
    /// Rejects duplicate/corrupt legacy rows and database read failures.
    pub fn legacy_node_team_binding(
        &self,
        workspace_id: WorkspaceId,
        goal_id: GoalId,
        dag_revision: DagRevisionNumber,
        node_id: DagNodeId,
    ) -> Result<Option<NodeTeamBinding>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let binding = read_legacy_node_team_binding_in_transaction(
            &transaction,
            workspace_id,
            goal_id,
            dag_revision,
            node_id,
        )?;
        crate::catalog::commit_read(transaction, self.destination_id())?;
        Ok(binding)
    }

    /// Retire one blocking legacy row with an exact revision and stable request key.
    ///
    /// # Errors
    /// Rejects stale identity/revision, unresolved assignment reservations,
    /// unauthorized actors and storage failures.
    pub fn retire_legacy_node_team_binding(
        &mut self,
        command: RetireLegacyNodeTeamBindingCommand,
    ) -> Result<LegacyNodeTeamBindingRetirementReceipt, OperationsError> {
        retire_legacy_node_team_binding(&mut self.connection, command)
    }

    /// Read retirement evidence by its stable request UUID after an uncertain commit.
    ///
    /// # Errors
    /// Rejects corrupt evidence and database read failures.
    pub fn read_legacy_node_team_binding_retirement(
        &self,
        request_id: Uuid,
    ) -> Result<Option<LegacyNodeTeamBindingRetirementReceipt>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let receipt =
            read_legacy_node_team_binding_retirement_in_transaction(&transaction, request_id)?;
        crate::catalog::commit_read(transaction, self.destination_id())?;
        Ok(receipt)
    }

    /// Read a bounded Workspace binding projection; exact routing uses
    /// `node_team_binding` and never depends on this aggregate page.
    ///
    /// # Errors
    /// Rejects corrupt rows and database read failures.
    pub fn node_team_bindings(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<NodeTeamBindingPage, OperationsError> {
        node_team_bindings(&self.connection, workspace_id)
    }
}

/// Read the exact generation from an existing transaction without opening a
/// nested SQLite snapshot.
///
/// # Errors
/// Rejects corrupt rows and database read failures.
pub(crate) fn read_node_team_binding_in_transaction(
    tx: &Transaction<'_>,
    scope: NodeTeamBindingScope,
) -> Result<Option<NodeTeamBinding>, OperationsError> {
    let row = tx
        .query_row(
            "SELECT team_id,bound_by,bound_at_ms,revision FROM ops_dag_node_team_bindings WHERE workspace_id=?1 AND activity_id=?2 AND goal_id=?3 AND goal_created_at_ms=?4 AND dag_revision=?5 AND dag_base_goal_revision=?6 AND node_id=?7",
            params![
                scope.workspace_id.into_uuid().as_bytes(),
                scope.activity_id.into_uuid().as_bytes(),
                scope.goal_id.into_uuid().as_bytes(),
                scope.goal_created_at.get(),
                scope.dag_revision.get(),
                scope.dag_base_goal_revision.get(),
                scope.node_id.into_uuid().as_bytes(),
            ],
            |row| {
                Ok((
                    row.get::<_, Option<Vec<u8>>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .optional()?;
    row.map(|(team_id, bound_by, bound_at_ms, revision)| {
        Ok(NodeTeamBinding {
            scope: Some(scope),
            goal_id: scope.goal_id,
            dag_revision: scope.dag_revision,
            node_id: scope.node_id,
            team_id: decode_optional_uuid(team_id)?,
            bound_by,
            bound_at: TimestampMillis::new(bound_at_ms)
                .map_err(|_| OperationsError::Corrupt("node binding timestamp"))?,
            revision: decode_revision(revision, "node binding revision")?,
            legacy_retirement: None,
        })
    })
    .transpose()
}

fn read_legacy_node_team_binding_in_transaction(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    goal_id: GoalId,
    dag_revision: DagRevisionNumber,
    node_id: DagNodeId,
) -> Result<Option<NodeTeamBinding>, OperationsError> {
    let mut statement = tx.prepare(
        "SELECT team_id,bound_by,bound_at_ms,revision,legacy_retirement_request_id,legacy_retired_at_ms,legacy_retired_by,substr(legacy_retired_activity_id,1,17),substr(legacy_retired_agent_id,1,17),substr(legacy_retired_session_id,1,17),substr(legacy_retired_participant_id,1,17),legacy_retired_leadership_revision FROM ops_dag_node_team_bindings WHERE workspace_id=?1 AND activity_id IS NULL AND goal_id=?2 AND dag_revision=?3 AND node_id=?4 LIMIT 2",
    )?;
    let mut rows = statement.query(params![
        workspace_id.into_uuid().as_bytes(),
        goal_id.into_uuid().as_bytes(),
        dag_revision.get(),
        node_id.into_uuid().as_bytes(),
    ])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let team_id = decode_optional_uuid(row.get(0)?)?;
    let bound_by = row.get::<_, String>(1)?;
    let bound_at = TimestampMillis::new(row.get(2)?)
        .map_err(|_| OperationsError::Corrupt("node binding timestamp"))?;
    let revision = decode_revision(row.get(3)?, "node binding revision")?;
    let retirement = LegacyRetirementColumns::from_row(row, 4)?;
    if rows.next()?.is_some() {
        return Err(OperationsError::Corrupt("duplicate legacy node binding"));
    }
    let summary = LegacyBindingSummary {
        workspace_id,
        goal_id,
        dag_revision,
        node_id,
        team_id,
        bound_by: bound_by.clone(),
        bound_at,
        revision,
    };
    Ok(Some(NodeTeamBinding {
        scope: None,
        goal_id,
        dag_revision,
        node_id,
        team_id,
        bound_by,
        bound_at,
        revision,
        legacy_retirement: decode_legacy_retirement(&summary, retirement)?,
    }))
}

fn read_legacy_node_team_binding_retirement_in_transaction(
    tx: &Transaction<'_>,
    request_id: Uuid,
) -> Result<Option<LegacyNodeTeamBindingRetirementReceipt>, OperationsError> {
    let row = tx
        .query_row(
            "SELECT substr(workspace_id,1,17),substr(goal_id,1,17),dag_revision,substr(node_id,1,17),team_id,bound_by,bound_at_ms,revision,substr(legacy_retirement_request_id,1,17),legacy_retired_at_ms,legacy_retired_by,substr(legacy_retired_activity_id,1,17),substr(legacy_retired_agent_id,1,17),substr(legacy_retired_session_id,1,17),substr(legacy_retired_participant_id,1,17),legacy_retired_leadership_revision FROM ops_dag_node_team_bindings WHERE legacy_retirement_request_id=?1",
            params![request_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, Option<Vec<u8>>>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                    LegacyRetirementColumns::from_row(row, 8)?,
                ))
            },
        )
        .optional()?;
    let Some((workspace, goal, dag, node, team, bound_by, bound_at, revision, retirement)) = row
    else {
        return Ok(None);
    };
    let workspace_id = WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(workspace)?));
    let goal_id = GoalId::from_uuid(Uuid::from_bytes(wire::id(goal)?));
    let dag_revision = DagRevisionNumber::new(dag)
        .map_err(|_| OperationsError::Corrupt("legacy binding DAG revision"))?;
    let node_id = DagNodeId::from_uuid(Uuid::from_bytes(wire::id(node)?));
    let summary = LegacyBindingSummary {
        workspace_id,
        goal_id,
        dag_revision,
        node_id,
        team_id: decode_optional_uuid(team)?,
        bound_by,
        bound_at: TimestampMillis::new(bound_at)
            .map_err(|_| OperationsError::Corrupt("node binding timestamp"))?,
        revision: decode_revision(revision, "node binding revision")?,
    };
    let receipt = decode_legacy_retirement(&summary, retirement)?.ok_or(
        OperationsError::Corrupt("legacy binding retirement readback"),
    )?;
    if receipt.request_id != request_id {
        return Err(OperationsError::Corrupt(
            "legacy binding retirement request",
        ));
    }
    Ok(Some(receipt))
}

fn set_node_team_binding(
    connection: &mut Connection,
    command: SetNodeTeamBindingCommand,
) -> Result<NodeTeamBinding, OperationsError> {
    validate_mutation(command)?;
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let _ = authorize_binding_actor(&tx, command)?;
    if let Some(team_id) = command.team_id {
        require_active_team(&tx, command.scope.workspace_id, team_id)?;
    }
    if has_legacy_binding(&tx, command.scope)? {
        return Err(OperationsError::Conflict(
            "node binding requires legacy generation reconciliation",
        ));
    }
    if crate::team_assignment::has_legacy_unknown_affinity_for_workspace(
        &tx,
        command.scope.workspace_id,
    )? {
        return Err(OperationsError::Conflict(
            "node binding requires legacy admission reconciliation",
        ));
    }
    if crate::team_assignment::has_pending_or_active_affinity_for_scope(&tx, command.scope)? {
        return Err(OperationsError::Conflict(
            "node binding has a pending or active assignment",
        ));
    }
    let current = read_node_team_binding_in_transaction(&tx, command.scope)?;
    let current_revision = current.as_ref().map(NodeTeamBinding::revision);
    if current_revision != command.expected_revision {
        return Err(OperationsError::Conflict("node binding revision"));
    }
    let current_team = current.as_ref().and_then(NodeTeamBinding::team_id);
    crate::leadership::work_roster::ensure_team_node_mapping_open(
        &tx,
        command.scope.workspace_id,
        current_team,
        command.team_id,
    )?;
    let revision = command.expected_revision.map_or(Ok(1_i64), |value| {
        let current =
            i64::try_from(value).map_err(|_| OperationsError::Bounds("node binding revision"))?;
        current
            .checked_add(1)
            .ok_or(OperationsError::Bounds("node binding revision"))
    })?;
    let scope = command.scope;
    let bound_by = if matches!(command.actor, OperationalActor::LocalOperator) {
        "operator"
    } else {
        "orchestrator"
    };
    if current.is_none() {
        tx.execute(
                "INSERT INTO ops_dag_node_team_bindings(workspace_id,activity_id,goal_id,goal_created_at_ms,dag_revision,dag_base_goal_revision,node_id,team_id,bound_by,bound_at_ms,revision) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                params![
                    scope.workspace_id.into_uuid().as_bytes(),
                    scope.activity_id.into_uuid().as_bytes(),
                    scope.goal_id.into_uuid().as_bytes(),
                    scope.goal_created_at.get(),
                    scope.dag_revision.get(),
                    scope.dag_base_goal_revision.get(),
                    scope.node_id.into_uuid().as_bytes(),
                    command.team_id.map(|id| id.into_bytes().to_vec()),
                    bound_by,
                    command.observed_at.get(),
                    revision,
                ],
            )?;
    } else {
        let changed = tx.execute(
                "UPDATE ops_dag_node_team_bindings SET team_id=?1,bound_by=?2,bound_at_ms=?3,revision=?4 WHERE workspace_id=?5 AND activity_id=?6 AND goal_id=?7 AND goal_created_at_ms=?8 AND dag_revision=?9 AND dag_base_goal_revision=?10 AND node_id=?11 AND revision=?12",
                params![
                    command.team_id.map(|id| id.into_bytes().to_vec()),
                    bound_by,
                    command.observed_at.get(),
                    revision,
                    scope.workspace_id.into_uuid().as_bytes(),
                    scope.activity_id.into_uuid().as_bytes(),
                    scope.goal_id.into_uuid().as_bytes(),
                    scope.goal_created_at.get(),
                    scope.dag_revision.get(),
                    scope.dag_base_goal_revision.get(),
                    scope.node_id.into_uuid().as_bytes(),
                    i64::try_from(command.expected_revision.unwrap_or_default())
                        .map_err(|_| OperationsError::Bounds("node binding revision"))?,
                ],
            )?;
        if changed != 1 {
            return Err(OperationsError::Conflict("node binding revision"));
        }
    }
    let binding = read_node_team_binding_in_transaction(&tx, scope)?
        .ok_or(OperationsError::Corrupt("node binding write readback"))?;
    tx.commit()
        .map_err(|source| OperationsError::NodeTeamBindingCommitUnconfirmed {
            scope: Box::new(scope),
            source,
        })?;
    Ok(binding)
}

fn retire_legacy_node_team_binding(
    connection: &mut Connection,
    command: RetireLegacyNodeTeamBindingCommand,
) -> Result<LegacyNodeTeamBindingRetirementReceipt, OperationsError> {
    validate_legacy_retirement_command(command)?;
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if let Some(existing) =
        read_legacy_node_team_binding_retirement_in_transaction(&tx, command.request_id)?
    {
        if retirement_matches_command(&existing, command) {
            let mut replay = existing;
            replay.replayed = true;
            tx.commit()?;
            return Ok(replay);
        }
        return Err(OperationsError::Conflict(
            "legacy binding retirement request replay",
        ));
    }
    let authority = authorize_binding_actor_identity(
        &tx,
        command.workspace_id,
        command.actor,
        command.expected_sender_leadership_revision,
    )?;
    if crate::team_assignment::has_unresolved_affinity_for_legacy_binding(
        &tx,
        command.workspace_id,
        command.goal_id,
        command.dag_revision,
        command.node_id,
    )? {
        return Err(OperationsError::Conflict(
            "legacy binding retirement has unresolved assignment reservation",
        ));
    }
    let current = read_legacy_node_team_binding_in_transaction(
        &tx,
        command.workspace_id,
        command.goal_id,
        command.dag_revision,
        command.node_id,
    )?
    .ok_or(OperationsError::Conflict("legacy node binding is missing"))?;
    if current.legacy_retirement.is_some() {
        return Err(OperationsError::Conflict(
            "legacy node binding already retired",
        ));
    }
    if current.revision != command.expected_revision {
        return Err(OperationsError::Conflict("legacy node binding revision"));
    }
    let authority_columns = retirement_authority_columns(authority);
    let changed = tx.execute(
        "UPDATE ops_dag_node_team_bindings SET legacy_retirement_request_id=?1,legacy_retired_at_ms=?2,legacy_retired_by=?3,legacy_retired_activity_id=?4,legacy_retired_agent_id=?5,legacy_retired_session_id=?6,legacy_retired_participant_id=?7,legacy_retired_leadership_revision=?8 WHERE workspace_id=?9 AND activity_id IS NULL AND goal_id=?10 AND dag_revision=?11 AND node_id=?12 AND revision=?13 AND legacy_retirement_request_id IS NULL",
        params![
            command.request_id.as_bytes(),
            command.observed_at.get(),
            authority_columns.retired_by,
            authority_columns.activity,
            authority_columns.agent,
            authority_columns.session,
            authority_columns.participant,
            authority_columns.leadership_revision,
            command.workspace_id.into_uuid().as_bytes(),
            command.goal_id.into_uuid().as_bytes(),
            command.dag_revision.get(),
            command.node_id.into_uuid().as_bytes(),
            i64::try_from(command.expected_revision)
                .map_err(|_| OperationsError::Bounds("legacy binding revision"))?,
        ],
    )?;
    if changed != 1 {
        return Err(OperationsError::Conflict("legacy node binding revision"));
    }
    let receipt = read_legacy_node_team_binding_retirement_in_transaction(&tx, command.request_id)?
        .ok_or(OperationsError::Corrupt(
            "legacy binding retirement readback",
        ))?;
    if !retirement_matches_command(&receipt, command)
        || receipt.authority != authority
        || receipt.retired_at != command.observed_at
    {
        return Err(OperationsError::Corrupt(
            "legacy binding retirement readback",
        ));
    }
    tx.commit()
        .map_err(|source| OperationsError::OperationCommitUnconfirmed {
            workspace_id: command.workspace_id,
            request_id: command.request_id,
            source,
        })?;
    Ok(receipt)
}

fn validate_legacy_retirement_command(
    command: RetireLegacyNodeTeamBindingCommand,
) -> Result<(), OperationsError> {
    if command.workspace_id.into_uuid().is_nil()
        || command.goal_id.into_uuid().is_nil()
        || command.node_id.into_uuid().is_nil()
        || command.request_id.is_nil()
        || command.expected_revision == 0
        || i64::try_from(command.expected_revision).is_err()
        || command
            .expected_sender_leadership_revision
            .is_some_and(|revision| revision < 1)
    {
        return Err(OperationsError::Conflict(
            "legacy binding retirement command",
        ));
    }
    Ok(())
}

struct EncodedLegacyBindingRetirementAuthority {
    retired_by: &'static str,
    activity: Option<Vec<u8>>,
    agent: Option<Vec<u8>>,
    session: Option<Vec<u8>>,
    participant: Option<Vec<u8>>,
    leadership_revision: Option<i64>,
}

fn retirement_authority_columns(
    authority: LegacyNodeTeamBindingRetirementAuthority,
) -> EncodedLegacyBindingRetirementAuthority {
    match authority {
        LegacyNodeTeamBindingRetirementAuthority::LocalOperator => {
            EncodedLegacyBindingRetirementAuthority {
                retired_by: "operator",
                activity: None,
                agent: None,
                session: None,
                participant: None,
                leadership_revision: None,
            }
        }
        LegacyNodeTeamBindingRetirementAuthority::WorkspaceLeader {
            activity_id,
            agent_id,
            session_id,
            participant_id,
            leadership_revision,
        } => EncodedLegacyBindingRetirementAuthority {
            retired_by: "orchestrator",
            activity: Some(activity_id.into_uuid().into_bytes().to_vec()),
            agent: Some(agent_id.into_uuid().into_bytes().to_vec()),
            session: Some(session_id.into_uuid().into_bytes().to_vec()),
            participant: Some(participant_id.into_bytes().to_vec()),
            leadership_revision: Some(leadership_revision),
        },
    }
}

fn retirement_matches_command(
    receipt: &LegacyNodeTeamBindingRetirementReceipt,
    command: RetireLegacyNodeTeamBindingCommand,
) -> bool {
    let actor_matches = match (receipt.authority, command.actor) {
        (
            LegacyNodeTeamBindingRetirementAuthority::LocalOperator,
            OperationalActor::LocalOperator,
        ) => command.expected_sender_leadership_revision.is_none(),
        (
            LegacyNodeTeamBindingRetirementAuthority::WorkspaceLeader {
                activity_id,
                agent_id,
                session_id,
                leadership_revision,
                ..
            },
            OperationalActor::Participant {
                activity_id: command_activity,
                agent_id: command_agent,
                session_id: command_session,
            },
        ) => {
            activity_id == command_activity
                && agent_id == command_agent
                && session_id == command_session
                && command.expected_sender_leadership_revision == Some(leadership_revision)
        }
        _ => false,
    };
    actor_matches
        && receipt.workspace_id == command.workspace_id
        && receipt.goal_id == command.goal_id
        && receipt.dag_revision == command.dag_revision
        && receipt.node_id == command.node_id
        && receipt.binding_revision == command.expected_revision
}

fn authorize_binding_actor(
    tx: &Transaction<'_>,
    command: SetNodeTeamBindingCommand,
) -> Result<LegacyNodeTeamBindingRetirementAuthority, OperationsError> {
    authorize_binding_actor_identity(
        tx,
        command.scope.workspace_id,
        command.actor,
        command.expected_sender_leadership_revision,
    )
}

fn authorize_binding_actor_identity(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    actor_input: OperationalActor,
    expected_leadership_revision: Option<i64>,
) -> Result<LegacyNodeTeamBindingRetirementAuthority, OperationsError> {
    let actor = resolve_actor(tx, workspace_id, actor_input)?;
    match (actor_input, expected_leadership_revision) {
        (OperationalActor::LocalOperator, None) => {
            Ok(LegacyNodeTeamBindingRetirementAuthority::LocalOperator)
        }
        (OperationalActor::LocalOperator, Some(_)) => {
            Err(OperationsError::Conflict("operator leadership revision"))
        }
        (OperationalActor::Participant { .. }, Some(expected_revision)) => {
            if !actor.is_profile_participant() {
                return Err(OperationsError::Conflict(
                    "node binding requires Profile participation",
                ));
            }
            let authority = leadership::current_leadership_authority_in_transaction(
                tx,
                crate::LeadershipScope::workspace(workspace_id),
            )?
            .ok_or(OperationsError::Conflict(
                "node binding requires current Workspace leader",
            ))?;
            if actor.participant_id() != Some(authority.participant_id())
                || expected_revision != authority.leadership_revision()
            {
                return Err(OperationsError::Conflict(
                    "node binding Workspace leader changed",
                ));
            }
            let OperationalActor::Participant {
                activity_id,
                agent_id,
                session_id,
            } = actor_input
            else {
                return Err(OperationsError::Corrupt("binding actor variant"));
            };
            Ok(LegacyNodeTeamBindingRetirementAuthority::WorkspaceLeader {
                activity_id,
                agent_id,
                session_id,
                participant_id: authority.participant_id(),
                leadership_revision: expected_revision,
            })
        }
        (OperationalActor::Participant { .. }, None) => Err(OperationsError::Conflict(
            "node binding requires expected leadership revision",
        )),
    }
}

fn require_active_team(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<(), OperationsError> {
    if team_id.is_nil() {
        return Err(OperationsError::Conflict("node binding Team identity"));
    }
    let state = tx
        .query_row(
            "SELECT state FROM ops_teams WHERE workspace_id=?1 AND team_id=?2",
            params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if state.as_deref() != Some("ACTIVE") {
        return Err(OperationsError::Conflict("node binding Team is not active"));
    }
    crate::teams::ensure_team_formed(tx, workspace_id, team_id)
}

fn validate_mutation(command: SetNodeTeamBindingCommand) -> Result<(), OperationsError> {
    if command.expected_revision == Some(0)
        || command
            .expected_revision
            .is_some_and(|revision| i64::try_from(revision).is_err())
        || command.team_id.is_some_and(|team_id| team_id.is_nil())
        || command
            .expected_sender_leadership_revision
            .is_some_and(|revision| revision < 1)
    {
        return Err(OperationsError::Conflict("node binding mutation"));
    }
    Ok(())
}

fn has_legacy_binding(
    tx: &Transaction<'_>,
    scope: NodeTeamBindingScope,
) -> Result<bool, OperationsError> {
    has_legacy_node_binding(tx, scope)
}

pub(crate) fn has_legacy_node_binding(
    tx: &Transaction<'_>,
    scope: NodeTeamBindingScope,
) -> Result<bool, OperationsError> {
    Ok(tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_dag_node_team_bindings WHERE workspace_id=?1 AND goal_id=?2 AND dag_revision=?3 AND node_id=?4 AND activity_id IS NULL AND legacy_retirement_request_id IS NULL)",
        params![
            scope.workspace_id.into_uuid().as_bytes(),
            scope.goal_id.into_uuid().as_bytes(),
            scope.dag_revision.get(),
            scope.node_id.into_uuid().as_bytes(),
        ],
        |row| row.get(0),
    )?)
}

fn node_team_bindings(
    connection: &Connection,
    workspace_id: WorkspaceId,
) -> Result<NodeTeamBindingPage, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT activity_id,goal_id,goal_created_at_ms,dag_revision,dag_base_goal_revision,node_id,team_id,bound_by,bound_at_ms,revision,legacy_retirement_request_id,legacy_retired_at_ms,legacy_retired_by,substr(legacy_retired_activity_id,1,17),substr(legacy_retired_agent_id,1,17),substr(legacy_retired_session_id,1,17),substr(legacy_retired_participant_id,1,17),legacy_retired_leadership_revision FROM ops_dag_node_team_bindings WHERE workspace_id=?1 ORDER BY bound_at_ms DESC,goal_id,dag_revision,node_id,activity_id,goal_created_at_ms,dag_base_goal_revision LIMIT ?2",
    )?;
    let rows = statement.query_map(
        params![
            workspace_id.into_uuid().as_bytes(),
            i64::try_from(MAX_NODE_TEAM_BINDINGS_PER_READ + 1)
                .map_err(|_| OperationsError::Bounds("node binding page limit"))?,
        ],
        |row| {
            Ok((
                row.get::<_, Option<Vec<u8>>>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Vec<u8>>(5)?,
                row.get::<_, Option<Vec<u8>>>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, i64>(8)?,
                row.get::<_, i64>(9)?,
                LegacyRetirementColumns::from_row(row, 10)?,
            ))
        },
    )?;
    let mut bindings = Vec::new();
    for row in rows {
        let row = row?;
        let goal_id = GoalId::from_uuid(Uuid::from_bytes(wire::id(row.1)?));
        let dag_revision = DagRevisionNumber::new(row.3)
            .map_err(|_| OperationsError::Corrupt("node binding DAG revision"))?;
        let node_id = DagNodeId::from_uuid(Uuid::from_bytes(wire::id(row.5)?));
        let scope = decode_scope(
            workspace_id,
            row.0,
            goal_id,
            row.2,
            dag_revision,
            row.4,
            node_id,
        )?;
        let team_id = decode_optional_uuid(row.6)?;
        let bound_at = TimestampMillis::new(row.8)
            .map_err(|_| OperationsError::Corrupt("node binding timestamp"))?;
        let revision = decode_revision(row.9, "node binding revision")?;
        let summary = LegacyBindingSummary {
            workspace_id,
            goal_id,
            dag_revision,
            node_id,
            team_id,
            bound_by: row.7.clone(),
            bound_at,
            revision,
        };
        bindings.push(NodeTeamBinding {
            scope,
            goal_id,
            dag_revision,
            node_id,
            team_id,
            bound_by: row.7,
            bound_at,
            revision,
            legacy_retirement: decode_legacy_retirement(&summary, row.10)?,
        });
    }
    let truncated = bindings.len() > MAX_NODE_TEAM_BINDINGS_PER_READ;
    bindings.truncate(MAX_NODE_TEAM_BINDINGS_PER_READ);
    Ok(NodeTeamBindingPage {
        bindings,
        truncated,
    })
}

fn decode_scope(
    workspace_id: WorkspaceId,
    activity_id: Option<Vec<u8>>,
    goal_id: GoalId,
    goal_created_at_ms: Option<i64>,
    dag_revision: DagRevisionNumber,
    dag_base_goal_revision: Option<i64>,
    node_id: DagNodeId,
) -> Result<Option<NodeTeamBindingScope>, OperationsError> {
    match (activity_id, goal_created_at_ms, dag_base_goal_revision) {
        (None, None, None) => Ok(None),
        (Some(activity), Some(created_at), Some(base_revision)) => {
            Ok(Some(NodeTeamBindingScope::new(
                workspace_id,
                ActivityId::from_uuid(Uuid::from_bytes(wire::id(activity)?)),
                goal_id,
                TimestampMillis::new(created_at)
                    .map_err(|_| OperationsError::Corrupt("node binding Goal generation"))?,
                dag_revision,
                GoalRevision::new(base_revision)
                    .map_err(|_| OperationsError::Corrupt("node binding DAG base Goal revision"))?,
                node_id,
            )?))
        }
        _ => Err(OperationsError::Corrupt("node binding scope shape")),
    }
}

fn decode_optional_uuid(value: Option<Vec<u8>>) -> Result<Option<Uuid>, OperationsError> {
    value
        .map(|bytes| wire::id(bytes).map(Uuid::from_bytes))
        .transpose()
}

fn decode_revision(value: i64, name: &'static str) -> Result<u64, OperationsError> {
    let revision = u64::try_from(value).map_err(|_| OperationsError::Corrupt(name))?;
    if revision == 0 {
        return Err(OperationsError::Corrupt(name));
    }
    Ok(revision)
}
