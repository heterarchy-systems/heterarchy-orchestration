//! Catalog identity, normalized canonical schema and durable import progress.

mod sql;

use crate::operational_messages;
use crate::{
    OperationsError, activation, adoption, agent_provision, agent_request,
    agent_request_resolution, approval, binding_rejection, cursor, departure, effect_binding,
    effect_fact, election_subject, events, inbox, leadership, members, membership, node_team,
    presence, registration, replacement_registration, session_replacement, teams, wire,
    work_credit, work_credit_subject, workspace_participation,
};
use forge_domain::WorkspaceId;
use forge_persistence::{
    AgentOperationsExportCursor as Cursor, AgentOperationsFreeze, AgentOperationsFreezeState,
};
use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};
use std::path::Path;
use uuid::Uuid;

const APPLICATION_ID: i64 = 0x484F_4F50;

/// Single canonical SQLite `PRAGMA user_version` for the Operations catalog.
pub const OPERATIONS_SCHEMA_VERSION: i64 = 1;
pub(crate) const VERSION: i64 = OPERATIONS_SCHEMA_VERSION;

use sql::{
    COUNT_NON_INTERNAL_SCHEMA_OBJECTS_SQL, EXISTS_ACTIVE_WORKSPACE_IMPORT_SQL, IDENTITY_SQL,
    IMPORT_INDEX_SQL, IMPORTS_SQL, INSERT_CATALOG_IDENTITY_SQL, INSERT_IMPORT_SQL, PAGES_SQL,
    SELECT_CATALOG_DESTINATION_SQL, SELECT_IMPORT_PROGRESS_SQL, SELECT_SCHEMA_OBJECT_SQL,
    SELECT_VERIFIED_IMPORT_MIGRATION_SQL,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportPhase {
    Importing,
    Imported,
    Verified,
    Aborted,
}

impl ImportPhase {
    pub(crate) fn decode(value: i64) -> Result<Self, OperationsError> {
        match value {
            0 => Ok(Self::Importing),
            1 => Ok(Self::Imported),
            2 => Ok(Self::Verified),
            3 => Ok(Self::Aborted),
            _ => Err(OperationsError::Corrupt("import phase")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportProgress {
    pub(crate) migration: Uuid,
    pub(crate) workspace: WorkspaceId,
    pub(crate) destination: Uuid,
    pub(crate) source_revision: u64,
    pub(crate) phase: ImportPhase,
    pub(crate) next: Option<Cursor>,
    pub(crate) pages: u64,
    pub(crate) records: u64,
    pub(crate) digest: [u8; 32],
    pub(crate) verified_pages: u64,
    pub(crate) verify_digest: [u8; 32],
}

impl ImportProgress {
    #[must_use]
    pub const fn migration_id(&self) -> Uuid {
        self.migration
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace
    }

    #[must_use]
    pub const fn destination_id(&self) -> Uuid {
        self.destination
    }

    #[must_use]
    pub const fn source_revision(&self) -> u64 {
        self.source_revision
    }

    #[must_use]
    pub const fn phase(&self) -> ImportPhase {
        self.phase
    }

    #[must_use]
    pub const fn next_cursor(&self) -> Option<Cursor> {
        self.next
    }

    #[must_use]
    pub const fn page_count(&self) -> u64 {
        self.pages
    }

    #[must_use]
    pub const fn record_count(&self) -> u64 {
        self.records
    }

    #[must_use]
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    #[must_use]
    pub const fn verified_pages(&self) -> u64 {
        self.verified_pages
    }

    #[must_use]
    pub const fn verify_digest(&self) -> [u8; 32] {
        self.verify_digest
    }
}

/// Single-owner synchronous persistence; place it on an owned bounded worker.
pub struct OperationsCatalog {
    pub(super) connection: Connection,
    pub(super) destination: Uuid,
}

impl OperationsCatalog {
    /// Open this destination's own catalog, rejecting foreign database identities.
    ///
    /// # Errors
    /// Rejects foreign or damaged catalogs, mismatched destination identities and storage failures.
    pub fn open(path: &Path, destination: Uuid) -> Result<Self, OperationsError> {
        validate_catalog_path(path, destination, false)?;
        let mut connection = Connection::open(path)?;
        connection.busy_timeout(std::time::Duration::from_secs(2))?;
        let version = {
            let snapshot = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
            let version = catalog_version(&snapshot, destination)?;
            snapshot.commit()?;
            version
        };
        connection.pragma_update(None, "foreign_keys", true)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        if version < VERSION {
            initialize_catalog(&mut connection, destination)?;
        }
        validate_schema(&connection)?;
        Ok(Self {
            connection,
            destination,
        })
    }

    /// Open an existing current catalog without creating, migrating or changing
    /// any SQLite schema/connection journal state.
    ///
    /// # Errors
    /// Rejects missing/non-regular/symlink paths, foreign or damaged catalogs,
    /// non-current schema versions that require migration, and storage failures.
    pub fn open_existing_read_only(
        path: &Path,
        destination: Uuid,
    ) -> Result<Self, OperationsError> {
        validate_catalog_path(path, destination, true)?;
        let mut connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        connection.busy_timeout(std::time::Duration::from_secs(2))?;
        let version = {
            let snapshot = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
            let app: i64 = snapshot.pragma_query_value(None, "application_id", |r| r.get(0))?;
            let user_version: i64 =
                snapshot.pragma_query_value(None, "user_version", |r| r.get(0))?;
            if app == APPLICATION_ID && user_version != VERSION {
                return Err(OperationsError::Conflict("catalog migration required"));
            }
            let version = catalog_version(&snapshot, destination)?;
            snapshot.commit()?;
            version
        };
        if version != VERSION {
            return Err(OperationsError::Conflict("catalog migration required"));
        }
        validate_schema(&connection)?;
        Ok(Self {
            connection,
            destination,
        })
    }

    #[must_use]
    pub const fn destination_id(&self) -> Uuid {
        self.destination
    }

    /// Read durable import progress for one migration identity.
    ///
    /// # Errors
    /// Rejects malformed stored progress and database read failures.
    pub fn progress(&self, migration: Uuid) -> Result<Option<ImportProgress>, OperationsError> {
        load_progress(&self.connection, migration)
    }

    /// Read the unique active, verified import for an exact Workspace.
    /// Historical aborted imports are excluded by the same ownership boundary
    /// enforced by the catalog's unique active-Workspace index.
    ///
    /// # Errors
    /// Rejects invalid identities, corrupt progress and storage/readback failure.
    pub fn verified_workspace_import(
        &self,
        workspace: WorkspaceId,
    ) -> Result<Option<ImportProgress>, OperationsError> {
        if workspace.into_uuid().is_nil() {
            return Err(OperationsError::Bounds("verified import Workspace"));
        }
        let tx = self.connection.unchecked_transaction()?;
        let migration = tx
            .query_row(
                SELECT_VERIFIED_IMPORT_MIGRATION_SQL,
                params![
                    workspace.into_uuid().as_bytes(),
                    self.destination.as_bytes()
                ],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?;
        let progress = migration
            .map(|bytes| {
                let migration = Uuid::from_bytes(wire::id(bytes)?);
                load_progress(&tx, migration)?
                    .ok_or(OperationsError::Corrupt("verified import disappeared"))
            })
            .transpose()?;
        if progress.as_ref().is_some_and(|row| {
            row.phase() != ImportPhase::Verified
                || row.workspace_id() != workspace
                || row.destination_id() != self.destination
        }) {
            return Err(OperationsError::Corrupt(
                "verified import Workspace identity",
            ));
        }
        commit_read(tx, self.destination)?;
        Ok(progress)
    }

    /// Start or resume import under a real frozen source identity. This never activates it.
    ///
    /// # Errors
    /// Rejects a released or foreign source, conflicting ownership and uncertain database commits.
    pub fn prepare(
        &mut self,
        source: &AgentOperationsFreeze,
    ) -> Result<ImportProgress, OperationsError> {
        if source.state() != AgentOperationsFreezeState::Frozen
            || source.destination_id() != self.destination
        {
            return Err(OperationsError::Conflict("source freeze/destination"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = load_progress(&tx, source.migration_id())? {
            if existing.workspace != source.workspace_id()
                || existing.destination != source.destination_id()
                || existing.source_revision != source.revision()
            {
                return Err(OperationsError::Conflict("import identity"));
            }
            return Ok(existing);
        }
        let occupied: bool = tx.query_row(
            EXISTS_ACTIVE_WORKSPACE_IMPORT_SQL,
            params![source.workspace_id().into_uuid().as_bytes()],
            |r| r.get(0),
        )?;
        if occupied {
            return Err(OperationsError::Conflict("workspace import owner"));
        }
        tx.execute(
            INSERT_IMPORT_SQL,
            params![
                source.migration_id().as_bytes(),
                source.workspace_id().into_uuid().as_bytes(),
                source.destination_id().as_bytes(),
                signed(source.revision())?,
                cursor::encode(Cursor::default())?,
                [0u8; 32].as_slice()
            ],
        )?;
        let result = load_progress(&tx, source.migration_id())?
            .ok_or(OperationsError::Corrupt("import insert"))?;
        commit(tx, source.migration_id())?;
        Ok(result)
    }
}

impl OperationsCatalog {
    /// Create a scoped Team and preserve exact creator provenance.
    ///
    /// # Errors
    /// Rejects invalid scope, replay conflicts, bounds and unconfirmed commits.
    pub fn create_team(
        &mut self,
        command: crate::TeamCreateCommand,
    ) -> Result<crate::TeamMutationRecord, OperationsError> {
        teams::create_team(&mut self.connection, command)
    }

    /// Read one exact durable Team mutation, including legacy unformed results.
    /// Reading neither repeats the mutation nor assigns missing historical roles.
    ///
    /// # Errors
    /// Rejects invalid scope, unverified Workspace and corrupt stored receipts.
    pub fn team_mutation(
        &self,
        workspace_id: WorkspaceId,
        team_id: Uuid,
        request_id: Uuid,
    ) -> Result<Option<crate::TeamMutationRecord>, OperationsError> {
        teams::read_team_mutation(&self.connection, workspace_id, team_id, request_id)
    }

    /// Join the authenticated logical participant to one exact Team revision.
    ///
    /// # Errors
    /// Rejects non-participants, stale scope, conflicts and storage failures.
    pub fn join_team(
        &mut self,
        command: crate::TeamJoinCommand,
    ) -> Result<crate::TeamMutationRecord, OperationsError> {
        teams::join_team(&mut self.connection, command)
    }

    /// Leave a Team while retaining the membership audit trail.
    ///
    /// # Errors
    /// Rejects invalid membership, stale revision, conflicts and storage failures.
    pub fn leave_team(
        &mut self,
        command: crate::TeamLeaveCommand,
    ) -> Result<crate::TeamMutationRecord, OperationsError> {
        teams::leave_team(&mut self.connection, command)
    }

    /// Retire a Team through the trusted local operator boundary.
    ///
    /// # Errors
    /// Rejects unauthorized actors, stale revisions and storage failures.
    pub fn retire_team(
        &mut self,
        command: crate::TeamRetireCommand,
    ) -> Result<crate::TeamMutationRecord, OperationsError> {
        teams::retire_team(&mut self.connection, command)
    }

    /// Read one exact Workspace-scoped Team.
    ///
    /// # Errors
    /// Rejects malformed identity and corrupt or unavailable storage.
    pub fn team(
        &self,
        workspace_id: WorkspaceId,
        team_id: Uuid,
    ) -> Result<Option<crate::TeamRecord>, OperationsError> {
        teams::read_team(&self.connection, workspace_id, team_id)
    }

    /// Read a bounded Team prefix with explicit truncation.
    ///
    /// # Errors
    /// Rejects invalid limits and corrupt or unavailable storage.
    pub fn teams(
        &self,
        workspace_id: WorkspaceId,
        limit: u32,
    ) -> Result<crate::TeamReadSet<crate::TeamRecord>, OperationsError> {
        teams::list_teams(&self.connection, workspace_id, limit)
    }

    /// Read logical Team memberships without multiplying Runtime Sessions.
    ///
    /// # Errors
    /// Rejects wrong Team scope, invalid limits and storage failures.
    pub fn team_members(
        &self,
        workspace_id: WorkspaceId,
        team_id: Uuid,
        limit: u32,
    ) -> Result<crate::TeamReadSet<crate::TeamMemberRecord>, OperationsError> {
        teams::list_team_members(&self.connection, workspace_id, team_id, limit)
    }

    /// Count the current JOINED members per Team for one Workspace in a
    /// single bounded read.
    ///
    /// # Errors
    /// Rejects wrong Workspace scope and storage failures.
    pub fn team_member_counts(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<std::collections::HashMap<Uuid, u64>, OperationsError> {
        teams::team_member_counts(&self.connection, workspace_id)
    }
}

fn validate_catalog_path(
    path: &Path,
    destination: Uuid,
    existing_only: bool,
) -> Result<(), OperationsError> {
    if !path.is_absolute() || destination.is_nil() {
        return Err(OperationsError::Conflict("catalog identity/path"));
    }
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.is_file() || metadata.file_type().is_symlink() => {
            Err(OperationsError::Conflict("catalog file kind"))
        }
        Err(source) if existing_only && source.kind() == std::io::ErrorKind::NotFound => {
            Err(OperationsError::Conflict("catalog path"))
        }
        Err(source) if existing_only => Err(source.into()),
        Ok(_) | Err(_) => Ok(()),
    }
}

fn catalog_version(connection: &Connection, destination: Uuid) -> Result<i64, OperationsError> {
    let app: i64 = connection.pragma_query_value(None, "application_id", |r| r.get(0))?;
    let version: i64 = connection.pragma_query_value(None, "user_version", |r| r.get(0))?;
    let count: i64 =
        connection.query_row(COUNT_NON_INTERNAL_SCHEMA_OBJECTS_SQL, [], |r| r.get(0))?;
    let fresh = app == 0 && version == 0 && count == 0;
    if fresh {
        return Ok(0);
    }
    if app != APPLICATION_ID || version != VERSION {
        return Err(OperationsError::Conflict("foreign catalog"));
    }
    validate_schema(connection)?;
    let stored: Vec<u8> = connection.query_row(SELECT_CATALOG_DESTINATION_SQL, [], |r| r.get(0))?;
    if wire::id(stored)? != destination.into_bytes() {
        return Err(OperationsError::Conflict("destination identity"));
    }
    Ok(VERSION)
}

fn initialize_catalog(
    connection: &mut Connection,
    destination: Uuid,
) -> Result<(), OperationsError> {
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    match catalog_version(&tx, destination)? {
        0 => {
            create_canonical_core_schema(&tx)?;
            create_canonical_governance_schema(&tx)?;
            create_autonomy_schema(&tx)?;
            seal_catalog_identity(&tx, destination)?;
        }
        VERSION => {}
        _ => return Err(OperationsError::Conflict("foreign catalog")),
    }
    let foreign_key_violation: Option<String> = tx
        .query_row("PRAGMA foreign_key_check", [], |row| row.get(0))
        .optional()?;
    if foreign_key_violation.is_some() {
        return Err(OperationsError::Corrupt("catalog migration foreign key"));
    }
    validate_schema(&tx)?;
    tx.commit()
        .map_err(|source| OperationsError::CatalogCommitUnconfirmed {
            destination_id: destination,
            source,
        })
}

fn create_canonical_core_schema(tx: &Transaction<'_>) -> Result<(), OperationsError> {
    for (_, _, sql) in base_objects() {
        tx.execute_batch(sql)?;
    }
    members::create_schema(tx)?;
    events::create_schema(tx)?;
    registration::create_schema(tx)?;
    membership::create_schema(tx)?;
    activation::create_schema(tx)?;
    binding_rejection::create_schema(tx)?;
    presence::create_schema(tx)?;
    inbox::create_schema(tx)?;
    effect_binding::create_schema(tx)?;
    adoption::create_schema(tx)?;
    departure::create_schema(tx)?;
    effect_fact::create_schema(tx)?;
    work_credit::create_schema(tx)?;
    leadership::create_schema(tx)?;
    election_subject::create_schema(tx)?;
    work_credit_subject::create_schema(tx)?;
    approval::create_schema(tx)?;
    crate::approval_versions::create_schema(tx)?;
    workspace_participation::create_schema(tx)?;
    session_replacement::create_schema(tx)?;
    replacement_registration::create_schema(tx)?;
    agent_request::create_schema(tx)?;
    agent_request_resolution::create_schema(tx)?;
    agent_provision::create_schema(tx)?;
    agent_provision::create_failure_schema(tx)?;
    agent_provision::create_uncertainty_retirement_schema(tx)?;
    agent_request::create_fulfillment_schema(tx)?;
    Ok(())
}

fn create_canonical_governance_schema(tx: &Transaction<'_>) -> Result<(), OperationsError> {
    teams::create_schema(tx)?;
    operational_messages::create_schema(tx)?;
    operational_messages::create_unknown_isolation_schema(tx)?;
    crate::import_handoff::create_schema(tx)?;
    crate::team_assignment::create_schema(tx)?;
    node_team::create_schema(tx)?;
    leadership::governance::create_schema(tx)?;
    leadership::work_exchange::create_schema(tx)?;
    crate::registration_profile::create_schema(tx)?;
    leadership::work_roster::create_schema(tx)?;
    crate::mission_evaluation::create_schema(tx)?;
    crate::mission_evaluation::create_checkpoint_schema(tx)?;
    crate::mission_evidence::create_schema(tx)?;
    crate::temporary_dispatch::create_schema(tx)?;
    crate::reviewer_qualification_rule::create_schema(tx)?;
    crate::temporary_dispatch::lifecycle::create_lifecycle_schema(tx)?;
    crate::reviewer_participation::create_schema(tx)?;
    crate::team_expansion::create_schema(tx)?;
    crate::team_create_source::create_schema(tx)?;
    crate::team_expansion::create_workspace_hold_schema(tx)?;
    crate::team_expansion::create_legacy_slot_recovery_schema(tx)?;
    crate::team_expansion::create_existing_home_transfer_schema(tx)?;
    crate::team_permanent_transfer::schema::create_schema(tx)?;
    crate::team_permanent_transfer::generation::create_schema(tx)?;
    crate::team_permanent_transfer::preparation::schema::create_schema(tx)?;
    crate::team_permanent_transfer::source::schema::create_schema(tx)?;
    crate::home_acceptance::schema::create_schema(tx)?;
    Ok(())
}

fn create_autonomy_schema(tx: &Transaction<'_>) -> Result<(), OperationsError> {
    crate::workspace_scope_registry::schema::create_schema(tx)?;
    crate::reviewer_assignment::schema::create_schema(tx)?;
    crate::assignment_execution_snapshot::schema::create_schema(tx)?;
    effect_fact::create_dag_settlement_schema(tx)?;
    crate::swarm::schema::create_schema(tx)?;
    Ok(())
}

fn autonomy_schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    let mut objects = crate::workspace_scope_registry::schema::schema_objects().to_vec();
    objects.extend_from_slice(crate::reviewer_assignment::schema::schema_objects());
    objects.extend_from_slice(crate::assignment_execution_snapshot::schema::schema_objects());
    objects.extend_from_slice(effect_fact::dag_settlement_schema_objects());
    objects.extend_from_slice(crate::swarm::schema::schema_objects());
    objects
}

fn seal_catalog_identity(tx: &Transaction<'_>, destination: Uuid) -> Result<(), OperationsError> {
    tx.execute(INSERT_CATALOG_IDENTITY_SQL, params![destination.as_bytes()])?;
    tx.pragma_update(None, "application_id", APPLICATION_ID)?;
    tx.pragma_update(None, "user_version", VERSION)?;
    Ok(())
}

pub(crate) fn signed(value: u64) -> Result<i64, OperationsError> {
    i64::try_from(value).map_err(|_| OperationsError::Bounds("counter"))
}

pub(crate) fn commit(tx: Transaction<'_>, migration_id: Uuid) -> Result<(), OperationsError> {
    tx.commit()
        .map_err(|source| OperationsError::CommitUnconfirmed {
            migration_id,
            source,
        })
}

pub(crate) fn commit_read(
    transaction: Transaction<'_>,
    destination: Uuid,
) -> Result<(), OperationsError> {
    transaction
        .commit()
        .map_err(|source| OperationsError::CatalogCommitUnconfirmed {
            destination_id: destination,
            source,
        })
}

pub(crate) fn load_progress(
    c: &Connection,
    migration: Uuid,
) -> Result<Option<ImportProgress>, OperationsError> {
    let raw = c
        .query_row(
            SELECT_IMPORT_PROGRESS_SQL,
            params![migration.as_bytes()],
            |r| {
                Ok(RawProgress {
                    workspace: r.get(0)?,
                    destination: r.get(1)?,
                    revision: r.get(2)?,
                    phase: r.get(3)?,
                    next: r.get(4)?,
                    pages: r.get(5)?,
                    records: r.get(6)?,
                    digest: r.get(7)?,
                    verified: r.get(8)?,
                    verify_digest: r.get(9)?,
                })
            },
        )
        .optional()?;
    raw.map(|v| decode_progress(migration, v)).transpose()
}

fn decode_progress(migration: Uuid, v: RawProgress) -> Result<ImportProgress, OperationsError> {
    let phase = ImportPhase::decode(v.phase)?;
    let next = v.next.as_deref().map(cursor::decode).transpose()?;
    let pages = u64::try_from(v.pages).map_err(|_| OperationsError::Corrupt("page count"))?;
    let verified =
        u64::try_from(v.verified).map_err(|_| OperationsError::Corrupt("verified count"))?;
    let digest: [u8; 32] = v
        .digest
        .try_into()
        .map_err(|_| OperationsError::Corrupt("digest"))?;
    let verify_digest: [u8; 32] = v
        .verify_digest
        .try_into()
        .map_err(|_| OperationsError::Corrupt("verify digest"))?;
    if verified > pages
        || v.revision < 1
        || (phase == ImportPhase::Importing && next.is_none())
        || (matches!(phase, ImportPhase::Imported | ImportPhase::Verified) && next.is_some())
        || (phase == ImportPhase::Verified && (verified != pages || digest != verify_digest))
    {
        return Err(OperationsError::Corrupt("import progress shape"));
    }
    Ok(ImportProgress {
        migration,
        workspace: WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(v.workspace)?)),
        destination: Uuid::from_bytes(wire::id(v.destination)?),
        source_revision: u64::try_from(v.revision)
            .map_err(|_| OperationsError::Corrupt("source revision"))?,
        phase,
        next,
        pages,
        records: u64::try_from(v.records).map_err(|_| OperationsError::Corrupt("record count"))?,
        digest,
        verified_pages: verified,
        verify_digest,
    })
}

struct RawProgress {
    workspace: Vec<u8>,
    destination: Vec<u8>,
    revision: i64,
    phase: i64,
    next: Option<Vec<u8>>,
    pages: i64,
    records: i64,
    digest: Vec<u8>,
    verified: i64,
    verify_digest: Vec<u8>,
}

fn base_objects() -> [(&'static str, &'static str, &'static str); 4] {
    [
        ("table", "ops_catalog_identity", IDENTITY_SQL),
        ("table", "ops_imports", IMPORTS_SQL),
        ("index", "ops_imports_workspace_owner_idx", IMPORT_INDEX_SQL),
        ("table", "ops_import_pages", PAGES_SQL),
    ]
}

fn normalize(s: &str) -> String {
    s.chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>()
        .trim_end_matches(';')
        .to_owned()
}

fn extend_owned(
    objects: &mut Vec<(&'static str, &'static str, String)>,
    extra: impl IntoIterator<Item = (&'static str, &'static str, &'static str)>,
) {
    objects.extend(
        extra
            .into_iter()
            .map(|(kind, name, sql)| (kind, name, sql.to_owned())),
    );
}

fn base_objects_owned() -> Vec<(&'static str, &'static str, String)> {
    base_objects()
        .into_iter()
        .map(|(kind, name, sql)| (kind, name, sql.to_owned()))
        .collect()
}

fn canonical_schema_objects() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = base_objects_owned();
    append_core_schema_objects(&mut objects);
    append_governance_schema_objects(&mut objects);
    extend_owned(&mut objects, autonomy_schema_objects());
    objects
}

fn append_core_schema_objects(objects: &mut Vec<(&'static str, &'static str, String)>) {
    extend_owned(objects, members::schema_objects());
    extend_owned(objects, events::schema_objects());
    extend_owned(objects, registration::schema_objects());
    extend_owned(objects, membership::schema_objects());
    extend_owned(objects, activation::schema_objects());
    extend_owned(objects, binding_rejection::schema_objects());
    extend_owned(objects, presence::schema_objects());
    extend_owned(objects, inbox::schema_objects());
    extend_owned(objects, effect_binding::schema_objects());
    extend_owned(objects, adoption::schema_objects());
    extend_owned(objects, departure::schema_objects());
    objects.extend(effect_fact::schema_objects_owned());
    extend_owned(objects, work_credit::schema_objects());
    extend_owned(objects, leadership::schema_objects());
    extend_owned(objects, election_subject::schema_objects());
    extend_owned(objects, work_credit_subject::schema_objects());
    extend_owned(objects, approval::schema_objects());
    extend_owned(objects, crate::approval_versions::schema_objects());
    extend_owned(objects, workspace_participation::schema_objects());
    extend_owned(objects, session_replacement::schema_objects());
    extend_owned(objects, replacement_registration::schema_objects());
    extend_owned(objects, agent_request::schema_objects());
    extend_owned(objects, agent_request_resolution::schema_objects());
    extend_owned(objects, agent_provision::schema_objects());
    extend_owned(objects, agent_provision::failure_schema_objects());
    extend_owned(
        objects,
        agent_provision::uncertainty_retirement_schema_objects(),
    );
}

fn append_governance_schema_objects(objects: &mut Vec<(&'static str, &'static str, String)>) {
    extend_owned(objects, teams::schema_objects());
    extend_owned(objects, operational_messages::schema_objects());
    extend_owned(
        objects,
        operational_messages::unknown_isolation_schema_objects(),
    );
    extend_owned(objects, crate::import_handoff::schema_objects());
    extend_owned(objects, crate::team_assignment::schema_objects());
    extend_owned(objects, node_team::schema_objects());
    extend_owned(objects, leadership::governance::schema_objects());
    extend_owned(objects, leadership::work_exchange::schema_objects());
    extend_owned(objects, crate::registration_profile::schema_objects());
    extend_owned(objects, leadership::work_roster::schema_objects());
    extend_owned(objects, crate::mission_evaluation::schema_objects());
    extend_owned(
        objects,
        crate::mission_evaluation::checkpoint_schema_objects(),
    );
    extend_owned(objects, crate::mission_evidence::schema_objects());
    extend_owned(objects, crate::temporary_dispatch::schema_objects());
    extend_owned(
        objects,
        crate::reviewer_qualification_rule::schema_objects()
            .iter()
            .copied(),
    );
    extend_owned(
        objects,
        crate::temporary_dispatch::lifecycle::lifecycle_schema_objects(),
    );
    extend_owned(
        objects,
        crate::reviewer_participation::schema_objects()
            .iter()
            .copied(),
    );
    extend_owned(
        objects,
        crate::team_expansion::schema_objects().iter().copied(),
    );
    extend_owned(
        objects,
        crate::team_create_source::schema_objects().iter().copied(),
    );
    extend_owned(
        objects,
        crate::team_expansion::workspace_hold_schema_objects()
            .iter()
            .copied(),
    );
    extend_owned(
        objects,
        crate::team_expansion::legacy_slot_recovery_schema_objects()
            .iter()
            .copied(),
    );
    extend_owned(
        objects,
        crate::team_expansion::existing_home_transfer_schema_objects()
            .iter()
            .copied(),
    );
    extend_owned(
        objects,
        crate::team_permanent_transfer::schema::schema_objects()
            .iter()
            .copied(),
    );
    extend_owned(
        objects,
        crate::team_permanent_transfer::generation::schema_objects()
            .iter()
            .copied(),
    );
    extend_owned(
        objects,
        crate::team_permanent_transfer::preparation::schema::schema_objects()
            .iter()
            .copied(),
    );
    extend_owned(
        objects,
        crate::team_permanent_transfer::source::schema::schema_objects()
            .iter()
            .copied(),
    );
    extend_owned(objects, crate::home_acceptance::schema::schema_objects());
}

fn validate_schema(c: &Connection) -> Result<(), OperationsError> {
    crate::team_permanent_transfer::source::schema::validate_schema(c)?;
    validate_exact_schema_objects(c, &canonical_schema_objects())
}

fn normalize_schema_object(sql: &str, kind: &str, name: &str) -> String {
    let normalized = normalize(sql);
    if kind == "table" {
        let quoted = format!("CREATETABLE\"{name}\"(");
        if let Some(body) = normalized.strip_prefix(&quoted) {
            return format!("CREATETABLE{name}({body}");
        }
    }
    normalized
}

fn validate_exact_schema_objects<S: AsRef<str>>(
    connection: &Connection,
    objects: &[(&str, &str, S)],
) -> Result<(), OperationsError> {
    let count: i64 =
        connection.query_row(COUNT_NON_INTERNAL_SCHEMA_OBJECTS_SQL, [], |row| row.get(0))?;
    if usize::try_from(count).ok() != Some(objects.len()) {
        return Err(OperationsError::Corrupt("schema inventory"));
    }
    for (kind, name, sql) in objects {
        let actual: Option<String> =
            connection.query_row(SELECT_SCHEMA_OBJECT_SQL, params![kind, name], |row| {
                row.get(0)
            })?;
        if actual
            .as_deref()
            .map(|value| normalize_schema_object(value, kind, name))
            != Some(normalize_schema_object(sql.as_ref(), kind, name))
        {
            return Err(OperationsError::Corrupt("schema object"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_open_initializes_canonical_schema_version_one_and_reopens_idempotently()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(OPERATIONS_SCHEMA_VERSION, 1);
        assert_eq!(VERSION, 1);
        let root = std::env::temp_dir().join(format!("operations-canonical-v1-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::now_v7();

        let catalog = OperationsCatalog::open(&path, destination)?;
        assert_eq!(catalog.destination_id(), destination);
        assert_eq!(
            catalog
                .connection
                .pragma_query_value(None, "application_id", |row| row.get::<_, i64>(0))?,
            APPLICATION_ID
        );
        assert_eq!(
            catalog
                .connection
                .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))?,
            OPERATIONS_SCHEMA_VERSION
        );
        validate_schema(&catalog.connection)?;
        drop(catalog);

        let reopened = OperationsCatalog::open(&path, destination)?;
        assert_eq!(
            reopened
                .connection
                .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))?,
            OPERATIONS_SCHEMA_VERSION
        );
        validate_schema(&reopened.connection)?;
        drop(reopened);

        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn open_existing_read_only_preserves_bytes_and_rejects_missing_or_unmigrated()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("operations-readonly-v1-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let missing = root.join("missing.sqlite3");
        let destination = Uuid::now_v7();
        assert!(matches!(
            OperationsCatalog::open_existing_read_only(&missing, destination),
            Err(OperationsError::Conflict("catalog path"))
        ));
        assert!(!missing.exists());

        let uninitialized = root.join("empty.sqlite3");
        drop(Connection::open(&uninitialized)?);
        assert!(matches!(
            OperationsCatalog::open_existing_read_only(&uninitialized, destination),
            Err(OperationsError::Conflict("catalog migration required"))
        ));

        let path = root.join("operations.sqlite3");
        let writable = OperationsCatalog::open(&path, destination)?;
        writable
            .connection
            .pragma_update(None, "journal_mode", "DELETE")?;
        drop(writable);
        let before = std::fs::read(&path)?;
        let readonly = OperationsCatalog::open_existing_read_only(&path, destination)?;
        assert_eq!(readonly.destination_id(), destination);
        drop(readonly);
        assert_eq!(std::fs::read(&path)?, before);

        let raw = Connection::open(&path)?;
        raw.pragma_update(None, "user_version", 2_i64)?;
        drop(raw);
        assert!(matches!(
            OperationsCatalog::open_existing_read_only(&path, destination),
            Err(OperationsError::Conflict("catalog migration required"))
        ));

        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn open_rejects_non_canonical_version_foreign_app_and_mismatched_destination()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("operations-reject-v1-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xD101);
        drop(OperationsCatalog::open(&path, destination)?);

        assert!(matches!(
            OperationsCatalog::open(&path, Uuid::from_u128(0xD102)),
            Err(OperationsError::Conflict("destination identity"))
        ));

        let raw = Connection::open(&path)?;
        raw.pragma_update(None, "user_version", 70_i64)?;
        drop(raw);
        assert!(matches!(
            OperationsCatalog::open(&path, destination),
            Err(OperationsError::Conflict("foreign catalog"))
        ));

        let foreign_path = root.join("foreign.sqlite3");
        let foreign = Connection::open(&foreign_path)?;
        foreign.execute_batch("CREATE TABLE unrelated (id INTEGER PRIMARY KEY)")?;
        drop(foreign);
        assert!(matches!(
            OperationsCatalog::open(&foreign_path, destination),
            Err(OperationsError::Conflict("foreign catalog"))
        ));

        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn concurrent_initializers_accept_already_sealed_canonical_catalog()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("operations-race-v1-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::now_v7();

        let mut first = Connection::open(&path)?;
        let mut second = Connection::open(&path)?;
        first.pragma_update(None, "foreign_keys", true)?;
        second.pragma_update(None, "foreign_keys", true)?;

        initialize_catalog(&mut first, destination)?;
        initialize_catalog(&mut second, destination)?;
        validate_schema(&second)?;

        drop(first);
        drop(second);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
