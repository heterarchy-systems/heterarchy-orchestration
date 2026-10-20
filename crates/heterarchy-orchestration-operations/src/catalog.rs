//! Catalog identity, normalized schema and durable import progress.
use crate::operational_messages;
use crate::{
    OperationsError, activation, adoption, agent_provision, agent_request,
    agent_request_resolution, approval, binding_rejection, cursor, departure, effect_binding,
    effect_fact, election_subject, events, inbox, leadership, members, membership, node_team,
    presence, profile_deletion, provider_deletion, registration, replacement_registration,
    session_replacement, teams, wire, work_credit, work_credit_subject, workspace_participation,
    workspace_project,
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
pub(crate) const VERSION: i64 = 52;
const IDENTITY_SQL: &str = "CREATE TABLE ops_catalog_identity (singleton INTEGER PRIMARY KEY CHECK(singleton=1), destination_id BLOB NOT NULL UNIQUE CHECK(length(destination_id)=16))";
const IMPORTS_SQL: &str = "CREATE TABLE ops_imports (migration_id BLOB PRIMARY KEY NOT NULL CHECK(length(migration_id)=16), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), destination_id BLOB NOT NULL CHECK(length(destination_id)=16), source_revision INTEGER NOT NULL CHECK(source_revision>=1), phase INTEGER NOT NULL CHECK(phase IN(0,1,2,3)), next_cursor BLOB CHECK(next_cursor IS NULL OR length(next_cursor) BETWEEN 1 AND 512), page_count INTEGER NOT NULL CHECK(page_count>=0), record_count INTEGER NOT NULL CHECK(record_count>=0), import_digest BLOB NOT NULL CHECK(length(import_digest)=32), verified_pages INTEGER NOT NULL CHECK(verified_pages BETWEEN 0 AND page_count), verify_digest BLOB NOT NULL CHECK(length(verify_digest)=32), FOREIGN KEY(destination_id) REFERENCES ops_catalog_identity(destination_id), CHECK(phase NOT IN(1,2) OR page_count>=5), CHECK((phase=0 AND next_cursor IS NOT NULL) OR (phase IN(1,2) AND next_cursor IS NULL) OR phase=3), CHECK(phase!=2 OR (verified_pages=page_count AND import_digest=verify_digest)))";
const IMPORT_INDEX: &str = "CREATE UNIQUE INDEX ops_imports_workspace_owner_idx ON ops_imports(workspace_id) WHERE phase!=3";
const PAGES_SQL: &str = "CREATE TABLE ops_import_pages (migration_id BLOB NOT NULL REFERENCES ops_imports(migration_id) CHECK(length(migration_id)=16), ordinal INTEGER NOT NULL CHECK(ordinal>=0), cursor BLOB NOT NULL CHECK(length(cursor) BETWEEN 1 AND 512), next_cursor BLOB CHECK(next_cursor IS NULL OR length(next_cursor) BETWEEN 1 AND 512), table_kind INTEGER NOT NULL CHECK(table_kind BETWEEN 0 AND 4), record_count INTEGER NOT NULL CHECK(record_count BETWEEN 0 AND 64), digest BLOB NOT NULL CHECK(length(digest)=32), PRIMARY KEY(migration_id,ordinal), UNIQUE(migration_id,cursor))";

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
            // Rebuilding referenced tables requires SQLite's documented
            // foreign-key-off migration sequence, outside the transaction.
            // The initializer revalidates the exact source schema under its
            // write lock and checks all foreign keys before committing.
            connection.pragma_update(None, "foreign_keys", false)?;
            let initialized = initialize_catalog(&mut connection, destination);
            connection.pragma_update(None, "foreign_keys", true)?;
            initialized?;
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
    /// older schema versions that require migration, and storage failures.
    pub fn open_existing_read_only(
        path: &Path,
        destination: Uuid,
    ) -> Result<Self, OperationsError> {
        validate_catalog_path(path, destination, true)?;
        let mut connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        connection.busy_timeout(std::time::Duration::from_secs(2))?;
        let version = {
            let snapshot = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
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
        let migration = tx.query_row(
            "SELECT substr(migration_id,1,17) FROM ops_imports WHERE workspace_id=?1 AND destination_id=?2 AND phase=2",
            params![workspace.into_uuid().as_bytes(), self.destination.as_bytes()],
            |row| row.get::<_, Vec<u8>>(0),
        ).optional()?;
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
            "SELECT EXISTS(SELECT 1 FROM ops_imports WHERE workspace_id=?1 AND phase!=3)",
            params![source.workspace_id().into_uuid().as_bytes()],
            |r| r.get(0),
        )?;
        if occupied {
            return Err(OperationsError::Conflict("workspace import owner"));
        }
        tx.execute("INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,?4,0,?5,0,0,?6,0,?6)",params![source.migration_id().as_bytes(),source.workspace_id().into_uuid().as_bytes(),source.destination_id().as_bytes(),signed(source.revision())?,cursor::encode(Cursor::default())?,[0u8;32].as_slice()])?;
        let result = load_progress(&tx, source.migration_id())?
            .ok_or(OperationsError::Corrupt("import insert"))?;
        commit(tx, source.migration_id())?;
        Ok(result)
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
fn validate_catalog_schema_version(
    connection: &Connection,
    version: i64,
) -> Result<(), OperationsError> {
    match version {
        1 => validate_schema_v1(connection),
        2 => validate_schema_v2(connection),
        3 => validate_schema_v3(connection),
        4 => validate_schema_v4(connection),
        5 => validate_schema_v5(connection),
        6 => validate_schema_v6(connection),
        7 => validate_schema_v7(connection),
        8 => validate_schema_v8(connection),
        9 => validate_schema_v9(connection),
        10 => validate_schema_v10(connection),
        11 => validate_schema_v11(connection),
        12 => validate_schema_v12(connection),
        13 => validate_schema_v13(connection),
        14 => validate_schema_v14(connection),
        15 => validate_schema_v15(connection),
        16 => validate_schema_v16(connection),
        17 => validate_schema_v17(connection),
        18 => validate_schema_v18(connection),
        19 => validate_schema_v19(connection),
        20 => validate_schema_v20(connection),
        21 => validate_schema_v21(connection),
        22 => validate_schema_v22(connection),
        23 => validate_schema_v23(connection),
        24 => validate_schema_v24(connection),
        25 => validate_schema_v25(connection),
        26 => validate_schema_v26(connection),
        27 => validate_schema_v27(connection),
        28 => validate_schema_v28(connection),
        29 => validate_schema_v29(connection),
        30 => validate_schema_v30(connection),
        31 => validate_schema_v31(connection),
        32 => validate_schema_v32(connection),
        33 => validate_schema_v33(connection),
        34 => validate_schema_v34(connection),
        35 => validate_schema_v35(connection),
        36 => validate_schema_v36(connection),
        37 => validate_schema_v37(connection),
        38 => validate_schema_v38(connection),
        39 => validate_schema_v39(connection),
        40 => validate_schema_v40(connection),
        41 => validate_schema_v41(connection),
        42 => validate_schema_v42(connection),
        43 => validate_schema_v43(connection),
        44 => validate_schema_v44(connection),
        45 => validate_schema_v45(connection),
        46 => validate_schema_v46(connection),
        47 => validate_schema_v47(connection),
        48 => validate_schema_v48(connection),
        49 => validate_schema_v49(connection),
        50 => validate_schema_v50(connection),
        51 => validate_schema_v51(connection),
        VERSION => validate_schema(connection),
        _ => Err(OperationsError::Conflict("foreign catalog")),
    }
}

fn catalog_version(connection: &Connection, destination: Uuid) -> Result<i64, OperationsError> {
    let app: i64 = connection.pragma_query_value(None, "application_id", |r| r.get(0))?;
    let version: i64 = connection.pragma_query_value(None, "user_version", |r| r.get(0))?;
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
        [],
        |r| r.get(0),
    )?;
    let fresh = app == 0 && version == 0 && count == 0;
    if !fresh && app != APPLICATION_ID {
        return Err(OperationsError::Conflict("foreign catalog"));
    }
    if !fresh {
        validate_catalog_schema_version(connection, version)?;
    }
    if !fresh {
        let stored: Vec<u8> = connection.query_row(
            "SELECT substr(destination_id,1,17) FROM ops_catalog_identity WHERE singleton=1",
            [],
            |r| r.get(0),
        )?;
        if wire::id(stored)? != destination.into_bytes() {
            return Err(OperationsError::Conflict("destination identity"));
        }
    }
    Ok(if fresh { 0 } else { version })
}

fn initialize_catalog(
    connection: &mut Connection,
    destination: Uuid,
) -> Result<(), OperationsError> {
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    match catalog_version(&tx, destination)? {
        0 => {
            for (_, _, sql) in base_objects() {
                tx.execute_batch(sql)?;
            }
            members::create_schema(&tx)?;
            events::create_schema(&tx)?;
            registration::create_schema(&tx)?;
            membership::create_schema(&tx)?;
            activation::create_schema(&tx)?;
            binding_rejection::create_schema(&tx)?;
            presence::create_schema(&tx)?;
            inbox::create_schema(&tx)?;
            effect_binding::create_schema(&tx)?;
            adoption::create_schema(&tx)?;
            departure::create_schema(&tx)?;
            effect_fact::create_schema(&tx)?;
            work_credit::create_schema(&tx)?;
            leadership::create_schema(&tx)?;
            election_subject::create_schema(&tx)?;
            work_credit_subject::create_schema(&tx)?;
            approval::create_schema(&tx)?;
            crate::approval_versions::create_schema(&tx)?;
            workspace_participation::create_schema(&tx)?;
            session_replacement::create_schema(&tx)?;
            replacement_registration::create_schema(&tx)?;
            agent_request::create_schema(&tx)?;
            agent_request_resolution::create_schema(&tx)?;
            agent_provision::create_schema(&tx)?;
            agent_provision::create_failure_schema(&tx)?;
            agent_provision::create_uncertainty_retirement_schema(&tx)?;
            agent_provision::create_project_route_schema(&tx)?;
            workspace_project::create_schema(&tx)?;
            workspace_project::create_enrollment_schema(&tx)?;
            agent_request::create_fulfillment_schema(&tx)?;
            teams::create_schema(&tx)?;
            operational_messages::create_schema(&tx)?;
            operational_messages::create_project_route_schema(&tx)?;
            operational_messages::create_unknown_isolation_schema(&tx)?;
            crate::import_handoff::create_schema(&tx)?;
            crate::team_assignment::create_schema(&tx)?;
            node_team::create_schema(&tx)?;
            leadership::governance::create_schema(&tx)?;
            profile_deletion::create_schema(&tx)?;
            provider_deletion::create_schema(&tx)?;
            leadership::work_exchange::create_schema(&tx)?;
            crate::registration_profile::create_schema(&tx)?;
            leadership::work_roster::create_schema(&tx)?;
            seal_catalog_identity(&tx, destination)?;
        }
        version => create_schema_upgrade(&tx, version)?,
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

/// Additive upgrades for catalogs initialized by an older schema version;
/// every arm creates only objects its own version predates, then stamps the
/// current catalog version.
fn create_legacy_schema_upgrade(tx: &Transaction<'_>, version: i64) -> Result<(), OperationsError> {
    match version {
        1..=5 => upgrade_early_schema(tx, version)?,
        6 => {
            adoption::create_schema(tx)?;
            departure::create_schema(tx)?;
            effect_fact::create_schema(tx)?;
            work_credit::create_schema(tx)?;
            leadership::create_schema(tx)?;
            approval::create_schema(tx)?;
        }
        7 => {
            departure::create_schema(tx)?;
            effect_fact::create_schema(tx)?;
            work_credit::create_schema(tx)?;
            leadership::create_schema(tx)?;
            approval::create_schema(tx)?;
        }
        8 => {
            effect_fact::create_schema(tx)?;
            work_credit::create_schema(tx)?;
            leadership::create_schema(tx)?;
            approval::create_schema(tx)?;
        }
        9 => {
            effect_fact::create_change_set_schema(tx)?;
            effect_fact::create_assignment_review_schema(tx)?;
            effect_fact::create_assignment_offer_schema(tx)?;
            effect_fact::create_assignment_submit_schema(tx)?;
            effect_fact::create_assignment_submit_source_refs_table(tx)?;
            work_credit::create_schema(tx)?;
            leadership::create_schema(tx)?;
            approval::create_schema(tx)?;
        }
        10 => {
            effect_fact::add_change_set_producer_columns(tx)?;
            effect_fact::create_assignment_review_schema(tx)?;
            effect_fact::create_assignment_offer_schema(tx)?;
            effect_fact::create_assignment_submit_schema(tx)?;
            effect_fact::create_assignment_submit_source_refs_table(tx)?;
            work_credit::create_schema(tx)?;
            leadership::create_schema(tx)?;
            approval::create_schema(tx)?;
        }
        11 => {
            effect_fact::add_change_set_producer_columns(tx)?;
            effect_fact::create_assignment_offer_schema(tx)?;
            effect_fact::create_assignment_submit_schema(tx)?;
            effect_fact::create_assignment_submit_source_refs_table(tx)?;
            leadership::create_schema(tx)?;
            approval::create_schema(tx)?;
        }
        12 => {
            effect_fact::add_change_set_producer_columns(tx)?;
            effect_fact::create_assignment_submit_schema(tx)?;
            effect_fact::create_assignment_submit_source_refs_table(tx)?;
            approval::create_schema(tx)?;
        }
        13 => {
            effect_fact::add_change_set_producer_columns(tx)?;
            effect_fact::create_assignment_submit_source_refs_schema(tx)?;
            approval::create_schema(tx)?;
        }
        14 => {
            effect_fact::create_assignment_submit_source_refs_schema(tx)?;
            leadership::create_confirm_intents_schema(tx)?;
            approval::create_schema(tx)?;
        }
        15 => {
            effect_fact::create_assignment_submit_source_refs_schema(tx)?;
            approval::create_schema(tx)?;
        }
        16 => {
            effect_fact::create_assignment_submit_source_refs_schema(tx)?;
        }
        17 | 19 | 20 | 21 | 22 | 23 | 24 | 25 | 26 | 27 | 28 | 29 | 30 | 31 | 32 | 33 | 34 | 35
        | 36 | 37 | 38 | 39 | 40 | 41 | 42 | 43 | 44 | 45 | 46 | 47 | 48 | 49 | 50 | 51 => {}
        18 => {
            leadership::rebuild_receipts_v19(tx)?;
        }
        VERSION => return Ok(()),
        _ => return Err(OperationsError::Conflict("catalog version changed")),
    }
    Ok(())
}

fn create_schema_upgrade(tx: &Transaction<'_>, version: i64) -> Result<(), OperationsError> {
    create_legacy_schema_upgrade(tx, version)?;
    if version == VERSION {
        return Ok(());
    }
    if (10..=17).contains(&version) {
        effect_fact::add_change_set_digest_version_column(tx)?;
    }
    if (12..=19).contains(&version) {
        leadership::rebuild_leadership_v20(tx)?;
    }
    if version <= 20 {
        workspace_participation::create_schema(tx)?;
    }
    if version <= 21 {
        session_replacement::create_schema(tx)?;
    }
    if version <= 22 {
        replacement_registration::create_schema(tx)?;
    }
    if version <= 23 {
        election_subject::create_schema(tx)?;
    }
    if version <= 24 {
        work_credit_subject::create_schema(tx)?;
    }
    if version <= 25 {
        agent_request::create_schema(tx)?;
    } else if version == 26 {
        agent_request::migrate_v26_to_v27(tx)?;
    }
    if version <= 26 {
        agent_request_resolution::create_schema(tx)?;
    }
    if version <= 27 {
        agent_provision::create_schema(tx)?;
    } else if matches!(version, 28 | 29) {
        agent_provision::migrate_v29_to_v30(tx)?;
    }
    if version <= 28 {
        agent_request::create_fulfillment_schema(tx)?;
    }
    if version <= 30 {
        teams::create_schema(tx)?;
        operational_messages::create_schema(tx)?;
    }
    if version == 31 {
        operational_messages::migrate_v31_to_v32(tx)?;
    }
    if version <= 31 {
        leadership::migrate_v31_to_v32(tx)?;
        election_subject::migrate_v31_to_v32(tx)?;
        work_credit_subject::migrate_v31_to_v32(tx)?;
        crate::team_assignment::create_schema(tx)?;
        leadership::governance::create_schema(tx)?;
    }
    if version <= 33 {
        node_team::create_schema(tx)?;
    }
    if version <= 32 {
        agent_provision::create_failure_schema(tx)?;
    }
    if version <= 34 {
        leadership::reconcile_legacy_leadership_elections_for_v35(tx)?;
        election_subject::migrate_v34_to_v35(tx)?;
    }
    if (31..=35).contains(&version) {
        operational_messages::migrate_v35_to_v36(tx)?;
    }
    upgrade_workspace_message_author_schema(tx, version)?;
    if (32..=35).contains(&version) {
        crate::team_assignment::migrate_v35_to_v36(tx)?;
    }
    if (34..=35).contains(&version) {
        node_team::migrate_v35_to_v36(tx)?;
    }
    if (31..=36).contains(&version) {
        teams::migrate_v36_to_v37(tx)?;
    }
    if version <= 37 {
        profile_deletion::create_schema(tx)?;
    }
    if version <= 38 {
        provider_deletion::create_schema(tx)?;
    }
    if version <= 39 {
        leadership::work_exchange::create_schema(tx)?;
    }
    if version <= 40 {
        crate::registration_profile::create_schema(tx)?;
    }
    if version <= 41 {
        leadership::work_roster::create_schema(tx)?;
    }
    if version <= 42 {
        crate::approval_versions::create_schema(tx)?;
    }
    upgrade_catalog_features(tx, version)?;
    tx.pragma_update(None, "user_version", VERSION)?;
    Ok(())
}

fn upgrade_catalog_features(tx: &Transaction<'_>, version: i64) -> Result<(), OperationsError> {
    upgrade_binding_rejection_schema(tx, version)?;
    upgrade_uncertainty_retirement_schema(tx, version)?;
    if (36..=47).contains(&version) {
        crate::team_assignment::migrate_v47_to_v48(tx)?;
    }
    if version <= 48 {
        workspace_project::create_schema(tx)?;
        agent_provision::create_project_route_schema(tx)?;
        operational_messages::create_project_route_schema(tx)?;
    }
    if version <= 49 {
        workspace_project::create_enrollment_schema(tx)?;
    }
    if version <= 50 {
        operational_messages::create_unknown_isolation_schema(tx)?;
    }
    if version <= 51 {
        crate::import_handoff::create_schema(tx)?;
    }
    Ok(())
}

fn upgrade_workspace_message_author_schema(
    tx: &Transaction<'_>,
    version: i64,
) -> Result<(), OperationsError> {
    if (31..=46).contains(&version) {
        operational_messages::migrate_v46_to_v47(tx)?;
    }
    Ok(())
}

fn upgrade_uncertainty_retirement_schema(
    tx: &Transaction<'_>,
    version: i64,
) -> Result<(), OperationsError> {
    if version <= 45 {
        agent_provision::create_uncertainty_retirement_schema(tx)?;
    }
    Ok(())
}

fn upgrade_binding_rejection_schema(
    tx: &Transaction<'_>,
    version: i64,
) -> Result<(), OperationsError> {
    if version <= 43 {
        binding_rejection::create_schema(tx)?;
    } else if version == 44 {
        binding_rejection::create_resolution_schema(tx)?;
    }
    Ok(())
}

fn upgrade_early_schema(tx: &Transaction<'_>, version: i64) -> Result<(), OperationsError> {
    if version == 1 {
        registration::create_schema(tx)?;
    }
    if version <= 2 {
        membership::create_schema(tx)?;
    }
    if version <= 2 {
        activation::create_schema(tx)?;
    }
    if version <= 3 {
        presence::create_schema(tx)?;
    }
    if version <= 4 {
        inbox::create_schema(tx)?;
    }
    effect_binding::create_schema(tx)?;
    adoption::create_schema(tx)?;
    departure::create_schema(tx)?;
    effect_fact::create_schema(tx)?;
    work_credit::create_schema(tx)?;
    leadership::create_schema(tx)?;
    approval::create_schema(tx)?;
    effect_binding::migrate_from_v5(tx)?;
    Ok(())
}

fn seal_catalog_identity(tx: &Transaction<'_>, destination: Uuid) -> Result<(), OperationsError> {
    tx.execute(
        "INSERT INTO ops_catalog_identity(singleton,destination_id) VALUES(1,?1)",
        params![destination.as_bytes()],
    )?;
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
    let raw=c.query_row("SELECT substr(workspace_id,1,17),substr(destination_id,1,17),source_revision,phase,substr(next_cursor,1,513),page_count,record_count,substr(import_digest,1,33),verified_pages,substr(verify_digest,1,33) FROM ops_imports WHERE migration_id=?1",params![migration.as_bytes()],|r|Ok(RawProgress{workspace:r.get(0)?,destination:r.get(1)?,revision:r.get(2)?,phase:r.get(3)?,next:r.get(4)?,pages:r.get(5)?,records:r.get(6)?,digest:r.get(7)?,verified:r.get(8)?,verify_digest:r.get(9)?})).optional()?;
    raw.map(|v| {
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
            records: u64::try_from(v.records)
                .map_err(|_| OperationsError::Corrupt("record count"))?,
            digest,
            verified_pages: verified,
            verify_digest,
        })
    })
    .transpose()
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
fn base_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("table", "ops_catalog_identity", IDENTITY_SQL),
        ("table", "ops_imports", IMPORTS_SQL),
        ("index", "ops_imports_workspace_owner_idx", IMPORT_INDEX),
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

/// Owned-triplet builder for versioned schema validation lists whose
/// `change_set` SQL is derived from the current canonical definition.
fn extend_owned(
    objects: &mut Vec<(&'static str, &'static str, String)>,
    extra: Vec<(&'static str, &'static str, &'static str)>,
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

fn schema_v32_objects() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = schema_v31_objects();
    for replacements in [
        leadership::schema_objects(),
        election_subject::schema_objects_v34(),
        work_credit_subject::schema_objects(),
        operational_messages::schema_objects_v35(),
    ] {
        for (kind, name, sql) in replacements {
            objects.retain(|(old_kind, old_name, _)| *old_kind != kind || *old_name != name);
            objects.push((kind, name, sql.to_owned()));
        }
    }
    extend_owned(&mut objects, crate::team_assignment::schema_objects_v35());
    extend_owned(&mut objects, leadership::governance::schema_objects());
    objects
}

fn validate_schema_v32(c: &Connection) -> Result<(), OperationsError> {
    validate_exact_schema_objects(c, &schema_v32_objects())
}

fn schema_v33_objects() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = schema_v32_objects();
    extend_owned(&mut objects, agent_provision::failure_schema_objects());
    objects
}

fn validate_schema_v33(c: &Connection) -> Result<(), OperationsError> {
    validate_exact_schema_objects(c, &schema_v33_objects())
}

fn schema_v34_objects() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = schema_v33_objects();
    extend_owned(&mut objects, node_team::schema_objects_v35());
    objects
}

fn validate_schema_v34(c: &Connection) -> Result<(), OperationsError> {
    validate_exact_schema_objects(c, &schema_v34_objects())
}

fn schema_v35_objects() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = schema_v34_objects();
    for (kind, name, sql) in election_subject::schema_objects() {
        objects.retain(|(old_kind, old_name, _)| *old_kind != kind || *old_name != name);
        objects.push((kind, name, sql.to_owned()));
    }
    objects
}

fn validate_schema_v35(c: &Connection) -> Result<(), OperationsError> {
    validate_exact_schema_objects(c, &schema_v35_objects())
}

fn schema_v36_objects() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = schema_v35_objects();
    for replacements in [
        operational_messages::schema_objects_v46(),
        crate::team_assignment::schema_objects_v47(),
        node_team::schema_objects(),
    ] {
        for (kind, name, sql) in replacements {
            objects.retain(|(old_kind, old_name, _)| *old_kind != kind || *old_name != name);
            objects.push((kind, name, sql.to_owned()));
        }
    }
    objects
}

fn validate_schema_v36(c: &Connection) -> Result<(), OperationsError> {
    validate_exact_schema_objects(c, &schema_v36_objects())
}

fn schema_v37_objects() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = schema_v36_objects();
    for (kind, name, sql) in teams::schema_objects() {
        objects.retain(|(old_kind, old_name, _)| *old_kind != kind || *old_name != name);
        objects.push((kind, name, sql.to_owned()));
    }
    objects
}

fn validate_schema_v37(c: &Connection) -> Result<(), OperationsError> {
    validate_exact_schema_objects(c, &schema_v37_objects())
}

fn schema_v38_objects() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = schema_v37_objects();
    extend_owned(&mut objects, profile_deletion::schema_objects());
    objects
}

fn validate_schema_v38(c: &Connection) -> Result<(), OperationsError> {
    validate_exact_schema_objects(c, &schema_v38_objects())
}

fn schema_v39_objects() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = schema_v38_objects();
    extend_owned(&mut objects, provider_deletion::schema_objects());
    objects
}

fn validate_schema_v39(c: &Connection) -> Result<(), OperationsError> {
    validate_exact_schema_objects(c, &schema_v39_objects())
}

fn schema_v40_objects() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = schema_v39_objects();
    extend_owned(&mut objects, leadership::work_exchange::schema_objects());
    objects
}

fn validate_schema_v40(c: &Connection) -> Result<(), OperationsError> {
    validate_exact_schema_objects(c, &schema_v40_objects())
}

fn schema_v41_objects() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = schema_v40_objects();
    extend_owned(&mut objects, crate::registration_profile::schema_objects());
    objects
}

fn validate_schema_v41(c: &Connection) -> Result<(), OperationsError> {
    validate_exact_schema_objects(c, &schema_v41_objects())
}

fn schema_v42_objects() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = schema_v41_objects();
    extend_owned(&mut objects, leadership::work_roster::schema_objects());
    objects
}

fn validate_schema_v42(c: &Connection) -> Result<(), OperationsError> {
    validate_exact_schema_objects(c, &schema_v42_objects())
}

fn schema_v43_objects() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = schema_v42_objects();
    extend_owned(&mut objects, crate::approval_versions::schema_objects());
    objects
}

fn validate_schema_v43(c: &Connection) -> Result<(), OperationsError> {
    validate_exact_schema_objects(c, &schema_v43_objects())
}

fn validate_schema(c: &Connection) -> Result<(), OperationsError> {
    validate_schema_v52(c)
}

fn schema_v52_objects() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = schema_v51_objects();
    extend_owned(&mut objects, crate::import_handoff::schema_objects());
    objects
}

fn validate_schema_v52(c: &Connection) -> Result<(), OperationsError> {
    validate_exact_schema_objects(c, &schema_v52_objects())
}

fn schema_v47_objects() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = schema_v46_objects();
    for (kind, name, sql) in operational_messages::schema_objects() {
        objects.retain(|(old_kind, old_name, _)| *old_kind != kind || *old_name != name);
        objects.push((kind, name, sql.to_owned()));
    }
    objects
}

fn validate_schema_v47(c: &Connection) -> Result<(), OperationsError> {
    validate_exact_schema_objects(c, &schema_v47_objects())
}

fn schema_v48_objects() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = schema_v47_objects();
    for (kind, name, sql) in crate::team_assignment::schema_objects() {
        objects.retain(|(old_kind, old_name, _)| *old_kind != kind || *old_name != name);
        objects.push((kind, name, sql.to_owned()));
    }
    objects
}

fn validate_schema_v48(c: &Connection) -> Result<(), OperationsError> {
    validate_exact_schema_objects(c, &schema_v48_objects())
}

fn schema_v49_objects() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = schema_v48_objects();
    extend_owned(&mut objects, workspace_project::schema_objects());
    extend_owned(
        &mut objects,
        agent_provision::project_route_schema_objects(),
    );
    extend_owned(
        &mut objects,
        operational_messages::project_route_schema_objects(),
    );
    objects
}

fn validate_schema_v49(c: &Connection) -> Result<(), OperationsError> {
    validate_exact_schema_objects(c, &schema_v49_objects())
}

fn schema_v50_objects() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = schema_v49_objects();
    extend_owned(&mut objects, workspace_project::enrollment_schema_objects());
    objects
}

fn validate_schema_v50(c: &Connection) -> Result<(), OperationsError> {
    validate_exact_schema_objects(c, &schema_v50_objects())
}

fn schema_v51_objects() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = schema_v50_objects();
    extend_owned(
        &mut objects,
        operational_messages::unknown_isolation_schema_objects(),
    );
    objects
}

fn validate_schema_v51(c: &Connection) -> Result<(), OperationsError> {
    validate_exact_schema_objects(c, &schema_v51_objects())
}

fn schema_v46_objects() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = schema_v45_objects();
    extend_owned(
        &mut objects,
        agent_provision::uncertainty_retirement_schema_objects(),
    );
    objects
}

fn validate_schema_v46(c: &Connection) -> Result<(), OperationsError> {
    validate_exact_schema_objects(c, &schema_v46_objects())
}

fn schema_v45_objects() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = schema_v44_objects();
    extend_owned(&mut objects, vec![binding_rejection::schema_objects()[2]]);
    objects
}

fn validate_schema_v45(c: &Connection) -> Result<(), OperationsError> {
    validate_exact_schema_objects(c, &schema_v45_objects())
}

fn schema_v44_objects() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = schema_v43_objects();
    extend_owned(&mut objects, binding_rejection::schema_objects_v44());
    objects
}

fn validate_schema_v44(c: &Connection) -> Result<(), OperationsError> {
    validate_exact_schema_objects(c, &schema_v44_objects())
}

fn validate_schema_v31(c: &Connection) -> Result<(), OperationsError> {
    validate_exact_schema_objects(c, &schema_v31_objects())
}

fn schema_v31_objects() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = schema_v30_objects();
    extend_owned(&mut objects, teams::schema_objects_v36());
    extend_owned(&mut objects, operational_messages::schema_objects_v31());
    objects
}

fn validate_schema_v30(c: &Connection) -> Result<(), OperationsError> {
    validate_exact_schema_objects(c, &schema_v30_objects())
}

fn schema_v30_objects() -> Vec<(&'static str, &'static str, String)> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    extend_owned(&mut objects, adoption::schema_objects());
    extend_owned(&mut objects, departure::schema_objects());
    objects.extend(effect_fact::schema_objects_owned());
    extend_owned(&mut objects, work_credit::schema_objects());
    extend_owned(&mut objects, leadership::schema_objects_v31());
    extend_owned(&mut objects, election_subject::schema_objects_v31());
    extend_owned(&mut objects, work_credit_subject::schema_objects_v31());
    extend_owned(&mut objects, approval::schema_objects());
    extend_owned(&mut objects, workspace_participation::schema_objects());
    extend_owned(&mut objects, session_replacement::schema_objects());
    extend_owned(&mut objects, replacement_registration::schema_objects());
    extend_owned(&mut objects, agent_request::schema_objects());
    extend_owned(&mut objects, agent_request_resolution::schema_objects());
    extend_owned(&mut objects, agent_provision::schema_objects());
    objects
}

/// Schema as it existed at catalog version 29: `AgentRequest` fulfillment was
/// durable, while ready-runtime reuse was not yet a provider-effect operation.
fn validate_schema_v29(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    extend_owned(&mut objects, adoption::schema_objects());
    extend_owned(&mut objects, departure::schema_objects());
    objects.extend(effect_fact::schema_objects_owned());
    extend_owned(&mut objects, work_credit::schema_objects());
    extend_owned(&mut objects, leadership::schema_objects_v31());
    extend_owned(&mut objects, election_subject::schema_objects_v31());
    extend_owned(&mut objects, work_credit_subject::schema_objects_v31());
    extend_owned(&mut objects, approval::schema_objects());
    extend_owned(&mut objects, workspace_participation::schema_objects());
    extend_owned(&mut objects, session_replacement::schema_objects());
    extend_owned(&mut objects, replacement_registration::schema_objects());
    extend_owned(&mut objects, agent_request::schema_objects());
    extend_owned(&mut objects, agent_request_resolution::schema_objects());
    extend_owned(&mut objects, agent_provision::schema_objects_v29());
    validate_exact_schema_objects(c, &objects)
}

/// Schema as it existed at catalog version 28: provider-effect operations were
/// durable, while `AgentRequest` terminal fulfillment was not yet persisted.
fn validate_schema_v28(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    extend_owned(&mut objects, adoption::schema_objects());
    extend_owned(&mut objects, departure::schema_objects());
    objects.extend(effect_fact::schema_objects_owned());
    extend_owned(&mut objects, work_credit::schema_objects());
    extend_owned(&mut objects, leadership::schema_objects_v31());
    extend_owned(&mut objects, election_subject::schema_objects_v31());
    extend_owned(&mut objects, work_credit_subject::schema_objects_v31());
    extend_owned(&mut objects, approval::schema_objects());
    extend_owned(&mut objects, workspace_participation::schema_objects());
    extend_owned(&mut objects, session_replacement::schema_objects());
    extend_owned(&mut objects, replacement_registration::schema_objects());
    extend_owned(&mut objects, agent_request::schema_objects_v28());
    extend_owned(&mut objects, agent_request_resolution::schema_objects());
    extend_owned(&mut objects, agent_provision::schema_objects_v29());
    validate_exact_schema_objects(c, &objects)
}

/// Schema as it existed at catalog version 27: durable resolver reservation
/// rows existed, but provider-effect operations were not yet journaled.
fn validate_schema_v27(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    extend_owned(&mut objects, adoption::schema_objects());
    extend_owned(&mut objects, departure::schema_objects());
    objects.extend(effect_fact::schema_objects_owned());
    extend_owned(&mut objects, work_credit::schema_objects());
    extend_owned(&mut objects, leadership::schema_objects_v31());
    extend_owned(&mut objects, election_subject::schema_objects_v31());
    extend_owned(&mut objects, work_credit_subject::schema_objects_v31());
    extend_owned(&mut objects, approval::schema_objects());
    extend_owned(&mut objects, workspace_participation::schema_objects());
    extend_owned(&mut objects, session_replacement::schema_objects());
    extend_owned(&mut objects, replacement_registration::schema_objects());
    extend_owned(&mut objects, agent_request::schema_objects_v28());
    extend_owned(&mut objects, agent_request_resolution::schema_objects());
    validate_exact_schema_objects(c, &objects)
}

/// Schema as it existed at catalog version 26: durable `AgentRequest` intent
/// existed, but resolver reservation rows and the `ResolutionReserved` state did not.
fn validate_schema_v26(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    extend_owned(&mut objects, adoption::schema_objects());
    extend_owned(&mut objects, departure::schema_objects());
    objects.extend(effect_fact::schema_objects_owned());
    extend_owned(&mut objects, work_credit::schema_objects());
    extend_owned(&mut objects, leadership::schema_objects_v31());
    extend_owned(&mut objects, election_subject::schema_objects_v31());
    extend_owned(&mut objects, work_credit_subject::schema_objects_v31());
    extend_owned(&mut objects, approval::schema_objects());
    extend_owned(&mut objects, workspace_participation::schema_objects());
    extend_owned(&mut objects, session_replacement::schema_objects());
    extend_owned(&mut objects, replacement_registration::schema_objects());
    extend_owned(&mut objects, agent_request::schema_objects_v26());
    validate_exact_schema_objects(c, &objects)
}

/// Schema as it existed at catalog version 25: Phase-F `WorkCredit` subject
/// authority was present, while autonomous `AgentRequest` intent was not yet a
/// durable Operations object.
fn validate_schema_v25(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    extend_owned(&mut objects, adoption::schema_objects());
    extend_owned(&mut objects, departure::schema_objects());
    objects.extend(effect_fact::schema_objects_owned());
    extend_owned(&mut objects, work_credit::schema_objects());
    extend_owned(&mut objects, leadership::schema_objects_v31());
    extend_owned(&mut objects, election_subject::schema_objects_v31());
    extend_owned(&mut objects, work_credit_subject::schema_objects_v31());
    extend_owned(&mut objects, approval::schema_objects());
    extend_owned(&mut objects, workspace_participation::schema_objects());
    extend_owned(&mut objects, session_replacement::schema_objects());
    extend_owned(&mut objects, replacement_registration::schema_objects());
    validate_exact_schema_objects(c, &objects)
}

/// Schema as it existed at catalog version 24: election voter authority was
/// participation-aware, while `WorkCredit` scores and election qualification
/// were still participant-keyed.
fn validate_schema_v24(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    extend_owned(&mut objects, adoption::schema_objects());
    extend_owned(&mut objects, departure::schema_objects());
    objects.extend(effect_fact::schema_objects_owned());
    extend_owned(&mut objects, work_credit::schema_objects());
    extend_owned(&mut objects, leadership::schema_objects_v31());
    extend_owned(&mut objects, election_subject::schema_objects_v31());
    extend_owned(&mut objects, approval::schema_objects());
    extend_owned(&mut objects, workspace_participation::schema_objects());
    extend_owned(&mut objects, session_replacement::schema_objects());
    extend_owned(&mut objects, replacement_registration::schema_objects());
    validate_exact_schema_objects(c, &objects)
}

/// Schema as it existed at catalog version 23: Phase-D replacement
/// registration was durable, but election voter authority was still stored
/// directly by participant identity.
fn validate_schema_v23(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    extend_owned(&mut objects, adoption::schema_objects());
    extend_owned(&mut objects, departure::schema_objects());
    objects.extend(effect_fact::schema_objects_owned());
    extend_owned(&mut objects, work_credit::schema_objects());
    extend_owned(&mut objects, leadership::schema_objects_v31());
    extend_owned(&mut objects, approval::schema_objects());
    extend_owned(&mut objects, workspace_participation::schema_objects());
    extend_owned(&mut objects, session_replacement::schema_objects());
    extend_owned(&mut objects, replacement_registration::schema_objects());
    validate_exact_schema_objects(c, &objects)
}

/// Schema as it existed at catalog version 22: logical Session replacement
/// had its durable rebind receipt, but replacement registrations still reused
/// no dedicated Pending/Reserved/Committed/Ready journal.
fn validate_schema_v22(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    extend_owned(&mut objects, adoption::schema_objects());
    extend_owned(&mut objects, departure::schema_objects());
    objects.extend(effect_fact::schema_objects_owned());
    extend_owned(&mut objects, work_credit::schema_objects());
    extend_owned(&mut objects, leadership::schema_objects_v31());
    extend_owned(&mut objects, approval::schema_objects());
    extend_owned(&mut objects, workspace_participation::schema_objects());
    extend_owned(&mut objects, session_replacement::schema_objects());
    validate_exact_schema_objects(c, &objects)
}

/// Schema as it existed at catalog version 21: the shadow Global Profile to
/// `WorkspaceParticipation` anchor existed, but logical Session replacement
/// had no durable dedup/rebind journal yet.
fn validate_schema_v21(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    extend_owned(&mut objects, adoption::schema_objects());
    extend_owned(&mut objects, departure::schema_objects());
    objects.extend(effect_fact::schema_objects_owned());
    extend_owned(&mut objects, work_credit::schema_objects());
    extend_owned(&mut objects, leadership::schema_objects_v31());
    extend_owned(&mut objects, approval::schema_objects());
    extend_owned(&mut objects, workspace_participation::schema_objects());
    validate_exact_schema_objects(c, &objects)
}

/// Schema as it existed at catalog version 20, before the shadow Global
/// `AgentProfile` -> `WorkspaceParticipation` anchor was introduced. All prior
/// operational authority tables are otherwise identical to their current
/// forms.
fn validate_schema_v20(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    extend_owned(&mut objects, adoption::schema_objects());
    extend_owned(&mut objects, departure::schema_objects());
    objects.extend(effect_fact::schema_objects_owned());
    extend_owned(&mut objects, work_credit::schema_objects());
    extend_owned(&mut objects, leadership::schema_objects_v31());
    extend_owned(&mut objects, approval::schema_objects());
    validate_exact_schema_objects(c, &objects)
}

fn validate_schema_v10(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    extend_owned(&mut objects, adoption::schema_objects());
    extend_owned(&mut objects, departure::schema_objects());
    objects.extend(effect_fact::schema_objects_v10());
    validate_exact_schema_objects(c, &objects)
}

/// Schema as it existed at catalog version 11: assignment reviews and work
/// credits were present, but assignment offers and the leadership journal
/// were not yet.
fn validate_schema_v11(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    extend_owned(&mut objects, adoption::schema_objects());
    extend_owned(&mut objects, departure::schema_objects());
    objects.extend(effect_fact::schema_objects_v11());
    extend_owned(&mut objects, work_credit::schema_objects());
    validate_exact_schema_objects(c, &objects)
}

/// Schema as it existed at catalog version 12: the leadership journal was
/// present, but assignment submissions had no durable projection yet.
fn validate_schema_v12(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    extend_owned(&mut objects, adoption::schema_objects());
    extend_owned(&mut objects, departure::schema_objects());
    objects.extend(effect_fact::schema_objects_v12());
    extend_owned(&mut objects, work_credit::schema_objects());
    extend_owned(&mut objects, leadership::schema_objects_v31());
    validate_exact_schema_objects(c, &objects)
}

/// Schema as it existed at catalog version 13: assignment submissions were
/// durably projected, but change-set facts carried no producer attribution.
fn validate_schema_v13(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    extend_owned(&mut objects, adoption::schema_objects());
    extend_owned(&mut objects, departure::schema_objects());
    objects.extend(effect_fact::schema_objects_v13());
    extend_owned(&mut objects, work_credit::schema_objects());
    extend_owned(&mut objects, leadership::schema_objects_v31());
    validate_exact_schema_objects(c, &objects)
}

/// Schema as it existed at catalog version 14: the leadership journal was
/// present, but leadership confirmation carried no durable intent journal.
fn validate_schema_v14(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    extend_owned(&mut objects, adoption::schema_objects());
    extend_owned(&mut objects, departure::schema_objects());
    objects.extend(effect_fact::schema_objects_v16());
    extend_owned(&mut objects, work_credit::schema_objects());
    objects.extend(leadership::schema_objects_v14());
    validate_exact_schema_objects(c, &objects)
}

/// Schema as it existed at catalog version 15: leadership confirmation had
/// its durable intent journal, but the operational approval authority
/// (policy, requests, admission records and issuer configuration) was not
/// yet present.
fn validate_schema_v15(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    extend_owned(&mut objects, adoption::schema_objects());
    extend_owned(&mut objects, departure::schema_objects());
    objects.extend(effect_fact::schema_objects_v16());
    extend_owned(&mut objects, work_credit::schema_objects());
    extend_owned(&mut objects, leadership::schema_objects_v31());
    validate_exact_schema_objects(c, &objects)
}

/// Schema as it existed at catalog version 16, before assignment-submission
/// source-reference provenance gained its marker and child projection.
fn validate_schema_v16(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    extend_owned(&mut objects, adoption::schema_objects());
    extend_owned(&mut objects, departure::schema_objects());
    objects.extend(effect_fact::schema_objects_v16());
    extend_owned(&mut objects, work_credit::schema_objects());
    extend_owned(&mut objects, leadership::schema_objects_v31());
    extend_owned(&mut objects, approval::schema_objects());
    validate_exact_schema_objects(c, &objects)
}

/// Schema as it existed at catalog version 17, before change-set digest
/// version markers were added for source-witnessed replay upgrades.
fn validate_schema_v17(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    extend_owned(&mut objects, adoption::schema_objects());
    extend_owned(&mut objects, departure::schema_objects());
    objects.extend(effect_fact::schema_objects_v17());
    extend_owned(&mut objects, work_credit::schema_objects());
    extend_owned(&mut objects, leadership::schema_objects_v31());
    extend_owned(&mut objects, approval::schema_objects());
    validate_exact_schema_objects(c, &objects)
}

/// Schema as it existed at catalog version 18, before the leadership receipt
/// vocabulary gained the handoff and recovery receipts.
fn validate_schema_v18(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    extend_owned(&mut objects, adoption::schema_objects());
    extend_owned(&mut objects, departure::schema_objects());
    objects.extend(effect_fact::schema_objects_owned());
    extend_owned(&mut objects, work_credit::schema_objects());
    objects.extend(leadership::schema_objects_v18());
    extend_owned(&mut objects, approval::schema_objects());
    validate_exact_schema_objects(c, &objects)
}

/// Schema as it existed at catalog version 19: all current objects were
/// present, but the leadership current-state table still admitted the retired
/// `HANDOFF_PENDING` state.
fn validate_schema_v19(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    extend_owned(&mut objects, adoption::schema_objects());
    extend_owned(&mut objects, departure::schema_objects());
    objects.extend(effect_fact::schema_objects_owned());
    extend_owned(&mut objects, work_credit::schema_objects());
    objects.extend(leadership::schema_objects_v19());
    extend_owned(&mut objects, approval::schema_objects());
    validate_exact_schema_objects(c, &objects)
}

fn validate_schema_v9(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    extend_owned(&mut objects, adoption::schema_objects());
    extend_owned(&mut objects, departure::schema_objects());
    objects.extend(effect_fact::schema_objects_v9());
    validate_exact_schema_objects(c, &objects)
}

fn validate_schema_v8(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    extend_owned(&mut objects, adoption::schema_objects());
    extend_owned(&mut objects, departure::schema_objects());
    validate_exact_schema_objects(c, &objects)
}

fn validate_schema_v7(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    extend_owned(&mut objects, adoption::schema_objects());
    validate_exact_schema_objects(c, &objects)
}

/// SQLite quotes a table name when ALTER TABLE renames its replacement.
/// Normalize only that exact catalog-owned identifier; preserve all columns,
/// constraints, literals and other SQL tokens in the existing exact check.
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
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
        [],
        |row| row.get(0),
    )?;
    if usize::try_from(count).ok() != Some(objects.len()) {
        return Err(OperationsError::Corrupt("schema inventory"));
    }
    for (kind, name, sql) in objects {
        let actual: Option<String> = connection.query_row(
            "SELECT CASE WHEN length(CAST(sql AS BLOB))<=16384 THEN sql END FROM sqlite_schema WHERE type=?1 AND name=?2",
            params![kind, name],
            |row| row.get(0),
        )?;
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

fn validate_schema_v4(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    validate_exact_schema_objects(c, &objects)
}

fn validate_schema_v5(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    validate_exact_schema_objects(c, &objects)
}

fn validate_schema_v6(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects_owned();
    extend_owned(&mut objects, members::schema_objects());
    extend_owned(&mut objects, events::schema_objects());
    extend_owned(&mut objects, registration::schema_objects());
    extend_owned(&mut objects, membership::schema_objects());
    extend_owned(&mut objects, activation::schema_objects());
    extend_owned(&mut objects, presence::schema_objects());
    extend_owned(&mut objects, inbox::schema_objects());
    extend_owned(&mut objects, effect_binding::schema_objects());
    validate_exact_schema_objects(c, &objects)
}

fn validate_schema_v3(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects();
    objects.extend(members::schema_objects());
    objects.extend(events::schema_objects());
    objects.extend(registration::schema_objects());
    objects.extend(membership::schema_objects());
    objects.extend(activation::schema_objects());
    let count: i64 = c.query_row(
        "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
        [],
        |r| r.get(0),
    )?;
    if usize::try_from(count).ok() != Some(objects.len()) {
        return Err(OperationsError::Corrupt("schema inventory"));
    }
    for (kind, name, sql) in objects {
        let actual: Option<String> = c.query_row(
            "SELECT CASE WHEN length(CAST(sql AS BLOB))<=16384 THEN sql END FROM sqlite_schema WHERE type=?1 AND name=?2",
            params![kind, name],
            |r| r.get(0),
        )?;
        if actual.as_deref().map(normalize) != Some(normalize(sql)) {
            return Err(OperationsError::Corrupt("schema object"));
        }
    }
    Ok(())
}

fn validate_schema_v2(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects();
    objects.extend(members::schema_objects());
    objects.extend(events::schema_objects());
    objects.extend(registration::schema_objects());
    let count: i64 = c.query_row(
        "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
        [],
        |r| r.get(0),
    )?;
    if usize::try_from(count).ok() != Some(objects.len()) {
        return Err(OperationsError::Corrupt("schema inventory"));
    }
    for (kind, name, sql) in objects {
        let actual: Option<String> = c.query_row(
            "SELECT CASE WHEN length(CAST(sql AS BLOB))<=16384 THEN sql END FROM sqlite_schema WHERE type=?1 AND name=?2",
            params![kind, name],
            |r| r.get(0),
        )?;
        if actual.as_deref().map(normalize) != Some(normalize(sql)) {
            return Err(OperationsError::Corrupt("schema object"));
        }
    }
    Ok(())
}

fn validate_schema_v1(c: &Connection) -> Result<(), OperationsError> {
    let mut objects = base_objects();
    objects.extend(members::schema_objects());
    objects.extend(events::schema_objects());
    let count: i64 = c.query_row(
        "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
        [],
        |r| r.get(0),
    )?;
    if usize::try_from(count).ok() != Some(objects.len()) {
        return Err(OperationsError::Corrupt("schema inventory"));
    }
    for (kind, name, sql) in objects {
        let actual: Option<String> = c.query_row(
            "SELECT CASE WHEN length(CAST(sql AS BLOB))<=16384 THEN sql END FROM sqlite_schema WHERE type=?1 AND name=?2",
            params![kind, name],
            |r| r.get(0),
        )?;
        if actual.as_deref().map(normalize) != Some(normalize(sql)) {
            return Err(OperationsError::Corrupt("schema object"));
        }
    }
    Ok(())
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

/// Reconstruct old catalog fixtures without retaining later application tables.
/// Production migrations never drop these objects.
#[cfg(test)]
pub(crate) fn drop_v31_objects_for_legacy_fixture(
    connection: &Connection,
) -> Result<(), OperationsError> {
    restore_v34_shapes_for_legacy_fixture(connection)?;
    connection.execute_batch(
        "DROP TABLE IF EXISTS ops_governance_invitations;
         DROP TABLE IF EXISTS ops_leadership_review_statements;
         DROP TABLE IF EXISTS ops_leadership_reviews;
         DROP TABLE IF EXISTS ops_assignment_team_affinities;
         DROP TABLE IF EXISTS ops_provider_message_outbox;
         DROP TABLE IF EXISTS ops_workspace_messages;
         DROP TABLE IF EXISTS ops_provider_message_routes;
         DROP TABLE IF EXISTS ops_team_member_events;
         DROP TABLE IF EXISTS ops_team_requests;
         DROP TABLE IF EXISTS ops_team_members;
         DROP TABLE IF EXISTS ops_teams;",
    )?;
    restore_v31_shapes_for_legacy_fixture(connection)?;
    Ok(())
}

#[cfg(test)]
pub(crate) fn assert_fixture_table_schema(
    connection: &Connection,
    name: &str,
    expected: &str,
) -> Result<(), OperationsError> {
    let actual: String = connection.query_row(
        "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?1",
        params![name],
        |row| row.get(0),
    )?;
    assert_eq!(
        normalize_schema_object(&actual, "table", name),
        normalize_schema_object(expected, "table", name)
    );
    Ok(())
}

/// Reconstruct the pre-v35 message, election, binding, and affinity schemas
/// while preserving historical columns and rows. Fixture preparation only.
#[cfg(test)]
pub(crate) fn restore_v34_shapes_for_legacy_fixture(
    connection: &Connection,
) -> Result<(), OperationsError> {
    drop_v49_project_schema_for_legacy_fixture(connection)?;
    connection.execute_batch("DROP TABLE IF EXISTS ops_agent_provision_uncertainty_retirements")?;
    crate::binding_rejection::drop_schema_for_legacy_fixture(connection)?;
    connection.execute_batch("DROP TABLE IF EXISTS ops_approval_request_versions")?;
    leadership::work_roster::drop_schema_for_legacy_fixture(connection)?;
    connection.execute_batch("DROP TABLE IF EXISTS ops_registration_profile_scopes; DROP TABLE IF EXISTS ops_leadership_work_exchanges")?;
    provider_deletion::drop_schema_for_legacy_fixture(connection)?;
    connection.execute_batch("DROP TABLE IF EXISTS ops_agent_profile_deletion_barriers")?;
    restore_v35_operational_message_shapes_for_legacy_fixture(connection)?;
    connection.execute_batch("DROP TABLE IF EXISTS ops_team_create_roster")?;
    restore_fixture_table_shapes(connection, teams::schema_objects_v36())?;
    restore_fixture_table_shapes(connection, crate::team_assignment::schema_objects_v35())?;
    restore_fixture_table_shapes(connection, node_team::schema_objects_v35())?;
    connection.execute_batch("DROP TABLE IF EXISTS ops_election_team_leader_sources")?;
    restore_fixture_table_shapes(connection, election_subject::schema_objects_v34())
}

/// Remove post-v48 Project, UNKNOWN-isolation, and handoff-seal schemas from a
/// newer test fixture before restamping it as a historical catalog. Production
/// migrations never call this helper; durable receipts prevent fixture downgrade.
#[cfg(test)]
pub(crate) fn drop_v49_project_schema_for_legacy_fixture(
    connection: &Connection,
) -> Result<(), OperationsError> {
    crate::import_handoff::drop_schema_for_legacy_fixture(connection)?;
    drop_v51_unknown_isolation_schema_for_legacy_fixture(connection)?;
    workspace_project::drop_enrollment_schema_for_legacy_fixture(connection)?;
    connection.execute_batch(
        "DROP TABLE IF EXISTS ops_provider_message_project_receipts;
         DROP TABLE IF EXISTS ops_provider_message_project_routes;
         DROP TABLE IF EXISTS ops_agent_provision_project_routes;
         DROP TABLE IF EXISTS ops_workspace_chatgpt_project_receipts;
         DROP TABLE IF EXISTS ops_workspace_chatgpt_project_bindings;",
    )?;
    Ok(())
}

/// Remove only the empty v51 isolation journal from a historical test fixture.
/// A receipt is durable evidence and must never be discarded by fixture setup.
#[cfg(test)]
pub(crate) fn drop_v51_unknown_isolation_schema_for_legacy_fixture(
    connection: &Connection,
) -> Result<(), OperationsError> {
    let has_table: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='ops_provider_message_unknown_isolations')",
        [],
        |row| row.get(0),
    )?;
    if has_table {
        let receipt_count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM ops_provider_message_unknown_isolations",
            [],
            |row| row.get(0),
        )?;
        if receipt_count != 0 {
            return Err(OperationsError::Conflict(
                "legacy fixture contains UNKNOWN isolation receipts",
            ));
        }
    }
    connection.execute_batch(
        "DROP TRIGGER IF EXISTS ops_provider_message_unknown_isolations_no_update;
         DROP TRIGGER IF EXISTS ops_provider_message_unknown_isolations_no_delete;
         DROP TABLE IF EXISTS ops_provider_message_unknown_isolations;",
    )?;
    Ok(())
}

/// Restore exact pre-DAG-base/pre-generation message and outbox shapes for fixtures.
/// Drop the base first; in-place drops preserve the outbox foreign-key target.
#[cfg(test)]
fn restore_v35_operational_message_shapes_for_legacy_fixture(
    connection: &Connection,
) -> Result<(), OperationsError> {
    let has_base_revision: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('ops_workspace_messages') WHERE name='dag_base_goal_revision')",
        [],
        |row| row.get(0),
    )?;
    if has_base_revision {
        connection.execute_batch(
            "ALTER TABLE ops_workspace_messages DROP COLUMN dag_base_goal_revision",
        )?;
    }
    let has_generation: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('ops_workspace_messages') WHERE name='goal_created_at_ms')",
        [],
        |row| row.get(0),
    )?;
    if has_generation {
        connection
            .execute_batch("ALTER TABLE ops_workspace_messages DROP COLUMN goal_created_at_ms")?;
    }
    let objects = operational_messages::schema_objects_v35();
    restore_fixture_table_shapes(connection, objects.clone())?;
    for (kind, name, sql) in objects {
        if kind != "index" || name != "ops_provider_message_outbox_queue_idx" {
            continue;
        }
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='index' AND name=?1)",
            params![name],
            |row| row.get(0),
        )?;
        if !exists {
            connection.execute_batch(sql)?;
        }
    }
    Ok(())
}

/// Test-only reconstruction of actual pre-Team table shapes. No production
/// caller may erase Team columns or reinterpret historical scope identities.
#[cfg(test)]
fn restore_v31_shapes_for_legacy_fixture(connection: &Connection) -> Result<(), OperationsError> {
    for objects in [
        leadership::schema_objects_v31(),
        election_subject::schema_objects_v31(),
        work_credit_subject::schema_objects_v31(),
    ] {
        restore_fixture_table_shapes(connection, objects)?;
    }
    Ok(())
}

#[cfg(test)]
fn restore_fixture_table_shapes(
    connection: &Connection,
    objects: Vec<(&str, &str, &str)>,
) -> Result<(), OperationsError> {
    for (kind, name, sql) in objects {
        if kind != "table" {
            continue;
        }
        let actual: Option<String> = connection
            .query_row(
                "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?1",
                params![name],
                |row| row.get(0),
            )
            .optional()?;
        let Some(actual) = actual else {
            continue;
        };
        if normalize_schema_object(&actual, "table", name)
            == normalize_schema_object(sql, "table", name)
        {
            continue;
        }
        let temporary = format!("{name}_historical_fixture");
        connection.execute_batch(&sql.replacen(
            &format!("CREATE TABLE {name} "),
            &format!("CREATE TABLE {temporary} "),
            1,
        ))?;
        let columns = {
            let mut query =
                connection.prepare("SELECT name FROM pragma_table_info(?1) ORDER BY cid")?;
            query
                .query_map(params![temporary], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
        };
        let columns = columns
            .iter()
            .map(|column| format!("\"{column}\""))
            .collect::<Vec<_>>()
            .join(",");
        connection.execute(
            &format!("INSERT INTO {temporary}({columns}) SELECT {columns} FROM {name}"),
            [],
        )?;
        connection.execute(&format!("DROP TABLE {name}"), [])?;
        connection.execute_batch(&format!("ALTER TABLE {temporary} RENAME TO {name}"))?;
        if name == "ops_leadership" {
            for (kind, _, sql) in leadership::schema_objects_v31() {
                if kind == "index" {
                    connection.execute_batch(sql)?;
                }
            }
        }
    }
    Ok(())
}

/// Restore the v46 message table before a legacy migration test lowers the
/// catalog version from a current v48 fixture. Production migrations never
/// downgrade a catalog.
#[cfg(test)]
pub(crate) fn restore_v46_message_schema_for_legacy_fixture(
    connection: &Connection,
) -> Result<(), OperationsError> {
    drop_v49_project_schema_for_legacy_fixture(connection)?;
    let message_sql = operational_messages::schema_objects_v46()
        .into_iter()
        .find(|(_, name, _)| *name == "ops_workspace_messages")
        .map(|(_, _, sql)| sql)
        .ok_or(OperationsError::Corrupt("v46 message fixture schema"))?;
    let explicit_objects = {
        let mut statement = connection.prepare(
            "SELECT sql FROM sqlite_schema WHERE tbl_name='ops_workspace_messages' AND type IN('index','trigger') AND sql IS NOT NULL ORDER BY type,name",
        )?;
        statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?
    };
    connection.pragma_update(None, "foreign_keys", false)?;
    connection.execute_batch("BEGIN IMMEDIATE")?;
    let result = (|| {
        connection.execute_batch(&message_sql.replace(
            "CREATE TABLE ops_workspace_messages ",
            "CREATE TABLE ops_workspace_messages_v46 ",
        ))?;
        connection.execute(
            "INSERT INTO ops_workspace_messages_v46(message_id,workspace_id,channel_kind,channel_participation_id,author_kind,author_participation_id,author_profile_id,author_participant_id,author_session_id,author_display_name,origin_kind,body,sequence,submitted_at_ms,activity_id,goal_id,dag_revision,node_id,provider_conversation_id,provider_turn_id,provider_source_kind,provider_source_profile_id,provider_source_participation_id,provider_source_label,goal_created_at_ms,dag_base_goal_revision) SELECT message_id,workspace_id,channel_kind,channel_participation_id,author_kind,author_participation_id,author_profile_id,author_participant_id,author_session_id,author_display_name,origin_kind,body,sequence,submitted_at_ms,activity_id,goal_id,dag_revision,node_id,provider_conversation_id,provider_turn_id,provider_source_kind,provider_source_profile_id,provider_source_participation_id,provider_source_label,goal_created_at_ms,dag_base_goal_revision FROM ops_workspace_messages",
            [],
        )?;
        connection.execute_batch(
            "DROP TABLE ops_workspace_messages; ALTER TABLE ops_workspace_messages_v46 RENAME TO ops_workspace_messages;",
        )?;
        for sql in explicit_objects {
            connection.execute_batch(&sql)?;
        }
        connection.execute_batch("COMMIT")?;
        Ok::<_, OperationsError>(())
    })();
    if result.is_err() {
        let _ = connection.execute_batch("ROLLBACK");
    }
    connection.pragma_update(None, "foreign_keys", true)?;
    result?;
    let foreign_key_violation: Option<String> = connection
        .query_row("PRAGMA foreign_key_check", [], |row| row.get(0))
        .optional()?;
    if foreign_key_violation.is_some() {
        return Err(OperationsError::Corrupt("v46 fixture foreign key"));
    }
    Ok(())
}

/// Restore the v47 affinity table before a historical migration test lowers
/// the catalog version from a current v48 fixture. All persisted columns and
/// evidence bytes are copied verbatim. Production migrations never downgrade
/// a catalog.
#[cfg(test)]
pub(crate) fn restore_v47_assignment_schema_for_legacy_fixture(
    connection: &Connection,
) -> Result<(), OperationsError> {
    drop_v49_project_schema_for_legacy_fixture(connection)?;
    let table_sql = crate::team_assignment::schema_objects_v47()
        .into_iter()
        .find(|(_, name, _)| *name == "ops_assignment_team_affinities")
        .map(|(_, _, sql)| sql)
        .ok_or(OperationsError::Corrupt("v47 assignment fixture schema"))?;
    let explicit_objects = {
        let mut statement = connection.prepare(
            "SELECT sql FROM sqlite_schema WHERE tbl_name='ops_assignment_team_affinities' AND type IN('index','trigger') AND sql IS NOT NULL ORDER BY type,name",
        )?;
        statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?
    };
    connection.pragma_update(None, "foreign_keys", false)?;
    connection.execute_batch("BEGIN IMMEDIATE")?;
    let result = (|| {
        connection.execute_batch(&table_sql.replace(
            "CREATE TABLE ops_assignment_team_affinities ",
            "CREATE TABLE ops_assignment_team_affinities_v47 ",
        ))?;
        let columns = {
            let mut query =
                connection.prepare("SELECT name FROM pragma_table_info(?1) ORDER BY cid")?;
            query
                .query_map(["ops_assignment_team_affinities_v47"], |row| {
                    row.get::<_, String>(0)
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        let columns = columns
            .iter()
            .map(|column| format!("\"{column}\""))
            .collect::<Vec<_>>()
            .join(",");
        connection.execute(
            &format!(
                "INSERT INTO ops_assignment_team_affinities_v47({columns}) SELECT {columns} FROM ops_assignment_team_affinities"
            ),
            [],
        )?;
        connection.execute_batch(
            "DROP TABLE ops_assignment_team_affinities;
             ALTER TABLE ops_assignment_team_affinities_v47 RENAME TO ops_assignment_team_affinities;",
        )?;
        for sql in explicit_objects {
            connection.execute_batch(&sql)?;
        }
        connection.execute_batch("COMMIT")?;
        Ok::<_, OperationsError>(())
    })();
    if result.is_err() {
        let _ = connection.execute_batch("ROLLBACK");
    }
    connection.pragma_update(None, "foreign_keys", true)?;
    result?;
    let foreign_key_violation: Option<String> = connection
        .query_row("PRAGMA foreign_key_check", [], |row| row.get(0))
        .optional()?;
    if foreign_key_violation.is_some() {
        return Err(OperationsError::Corrupt("v47 fixture foreign key"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    type CatalogTableSnapshot = Vec<Vec<rusqlite::types::Value>>;
    type ForeignKeySnapshotRow = (i64, String, String, String, String);
    type ForeignKeySnapshot = Vec<ForeignKeySnapshotRow>;
    type V46NativeAuthorShape = (
        i64,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
    );

    #[derive(Clone, Copy)]
    enum V46MessageCorruption {
        InvalidLegacyAuthorShape,
        UnsupportedFutureNativeAuthor,
    }

    fn create_v46_message_migration_fixture(
        path: &Path,
        destination: Uuid,
        corruption: Option<V46MessageCorruption>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let old = Connection::open(path)?;
        for (_, _, sql) in schema_v46_objects() {
            old.execute_batch(&sql)?;
        }
        old.pragma_update(None, "foreign_keys", true)?;

        let workspace = Uuid::from_u128(0xA471);
        let profile = Uuid::from_u128(0xA472);
        let participation = Uuid::from_u128(0xA473);
        let session = Uuid::from_u128(0xA474);
        let participant = Uuid::from_u128(0xA475);
        let operator_message = Uuid::from_u128(0xA480);
        let participant_message = Uuid::from_u128(0xA481);
        let system_message = Uuid::from_u128(0xA482);
        let provider_message = Uuid::from_u128(0xA483);
        let outbox_message = Uuid::from_u128(0xA484);
        let election = Uuid::from_u128(0xA490);
        let request = Uuid::from_u128(0xA491);
        let migration = Uuid::from_u128(0xA496);

        old.execute(
            "INSERT INTO ops_catalog_identity(singleton,destination_id) VALUES(1,?1)",
            params![destination.as_bytes()],
        )?;
        old.pragma_update(None, "application_id", APPLICATION_ID)?;
        old.pragma_update(None, "user_version", 46_i64)?;
        old.execute(
            "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,5,?4,5,?4)",
            params![migration.as_bytes(), workspace.as_bytes(), destination.as_bytes(), [0xA4_u8; 32].as_slice()],
        )?;
        old.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,10,1)",
            params![workspace.as_bytes(), profile.as_bytes(), participation.as_bytes()],
        )?;
        old.execute(
            "INSERT INTO ops_workspace_messages(message_id,workspace_id,channel_kind,author_kind,author_display_name,origin_kind,body,sequence,submitted_at_ms) VALUES(?1,?2,0,0,'Local operator',0,'Preserved operator message',1,10)",
            params![operator_message.as_bytes(), workspace.as_bytes()],
        )?;
        old.execute(
            "INSERT INTO ops_workspace_messages(message_id,workspace_id,channel_kind,author_kind,author_participation_id,author_profile_id,author_participant_id,author_session_id,author_display_name,origin_kind,body,sequence,submitted_at_ms) VALUES(?1,?2,0,1,?3,?4,?5,?6,'Profile participant',0,'Preserved participant message',2,20)",
            params![participant_message.as_bytes(), workspace.as_bytes(), participation.as_bytes(), profile.as_bytes(), participant.as_bytes(), session.as_bytes()],
        )?;
        old.execute(
            "INSERT INTO ops_workspace_messages(message_id,workspace_id,channel_kind,author_kind,author_display_name,origin_kind,body,sequence,submitted_at_ms) VALUES(?1,?2,0,3,'System',0,'Preserved system message',3,30)",
            params![system_message.as_bytes(), workspace.as_bytes()],
        )?;
        old.execute(
            "INSERT INTO ops_workspace_messages(message_id,workspace_id,channel_kind,author_kind,author_display_name,origin_kind,body,sequence,submitted_at_ms,provider_conversation_id,provider_turn_id,provider_source_kind,provider_source_label) VALUES(?1,?2,0,2,'Provider',1,'Preserved provider message',4,40,'migration-conversation','migration-turn',1,'Provider')",
            params![provider_message.as_bytes(), workspace.as_bytes()],
        )?;
        if corruption.is_some() {
            old.pragma_update(None, "ignore_check_constraints", true)?;
        }
        let (author_kind, author_participant_id, author_session_id) = match corruption {
            Some(V46MessageCorruption::InvalidLegacyAuthorShape) => {
                (0, Some(participant.as_bytes()), None)
            }
            Some(V46MessageCorruption::UnsupportedFutureNativeAuthor) => {
                (4, Some(participant.as_bytes()), Some(session.as_bytes()))
            }
            None => (0, None, None),
        };
        old.execute(
            "INSERT INTO ops_workspace_messages(message_id,workspace_id,channel_kind,author_kind,author_participant_id,author_session_id,author_display_name,origin_kind,body,sequence,submitted_at_ms) VALUES(?1,?2,0,?3,?4,?5,'Broken fixture',0,'Will force migration rollback',5,50)",
            params![outbox_message.as_bytes(), workspace.as_bytes(), author_kind, author_participant_id, author_session_id],
        )?;
        if corruption.is_some() {
            old.pragma_update(None, "ignore_check_constraints", false)?;
        }
        old.execute(
            "INSERT INTO ops_provider_message_routes(provider_family,runtime_session_id,provider_conversation_id,workspace_id,recipient_profile_id,recipient_participation_id,created_at_ms) VALUES(1,?1,'outbox-conversation',?2,?3,?4,50)",
            params![session.as_bytes(), workspace.as_bytes(), profile.as_bytes(), participation.as_bytes()],
        )?;
        let witness = br#"{"version":1,"operation_id":"00000000-0000-4000-8000-00000000a492","participation_revision":1,"runtime_revision":1,"sender_leadership_revision":null,"sender":{"kind":"operator"},"target":{"kind":"agent","assignment_id":"00000000-0000-4000-8000-00000000a493","assignment_revision":1,"agent_id":"00000000-0000-4000-8000-00000000a494","session_id":"00000000-0000-4000-8000-00000000a495"}}"#;
        old.execute(
            "INSERT INTO ops_provider_message_outbox(message_id,workspace_id,recipient_profile_id,recipient_participation_id,provider_family,runtime_session_id,provider_conversation_id,state,queued_at_ms,last_dispatch_at_ms,updated_at_ms,revision,provider_turn_id,provider_body,node_route_witness) VALUES(?1,?2,?3,?4,1,?5,'outbox-conversation',1,50,0,50,1,NULL,NULL,?6)",
            params![outbox_message.as_bytes(), workspace.as_bytes(), profile.as_bytes(), participation.as_bytes(), session.as_bytes(), witness.as_slice()],
        )?;
        old.execute(
            "INSERT INTO ops_leadership_elections(election_id,workspace_id,scope_kind,kind,state,candidate_participant_id,ballot_deadline_ms,created_at_ms,closed_at_ms) VALUES(?1,?2,0,'RECALL','CANCELLED',?3,100,10,20)",
            params![election.as_bytes(), workspace.as_bytes(), participant.as_bytes()],
        )?;
        old.execute(
            "INSERT INTO ops_leadership_reviews(request_id,election_id,workspace_id,scope_kind,expected_leadership_revision,proposer_kind,proposer_display_name,reason,defense_deadline_ms,discussion_deadline_ms,ballot_deadline_ms,created_at_ms) VALUES(?1,?2,?3,0,1,0,'Local operator','Migration preservation fixture',100,200,300,10)",
            params![request.as_bytes(), election.as_bytes(), workspace.as_bytes()],
        )?;
        old.execute(
            "INSERT INTO ops_governance_invitations(request_id,election_id,workspace_id,phase,recipient_participation_id,message_id,deadline_ms,queued_at_ms) VALUES(?1,?2,?3,'DEFENSE',?4,?5,100,50)",
            params![request.as_bytes(), election.as_bytes(), workspace.as_bytes(), participation.as_bytes(), operator_message.as_bytes()],
        )?;
        old.pragma_update(None, "foreign_keys", true)?;
        old.pragma_update(None, "application_id", APPLICATION_ID)?;
        validate_schema_v46(&old)?;
        Ok(())
    }

    fn snapshot_table(
        connection: &Connection,
        table: &str,
        order_by: &str,
    ) -> Result<CatalogTableSnapshot, Box<dyn std::error::Error>> {
        let mut statement =
            connection.prepare(&format!("SELECT * FROM {table} ORDER BY {order_by}"))?;
        let column_count = statement.column_count();
        Ok(statement
            .query_map([], |row| {
                (0..column_count)
                    .map(|index| row.get(index))
                    .collect::<Result<Vec<_>, _>>()
            })?
            .collect::<Result<Vec<_>, _>>()?)
    }

    #[test]
    fn v46_native_author_migration_preserves_legacy_messages_and_child_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("operations-v46-messages-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xA470);
        create_v46_message_migration_fixture(&path, destination, None)?;

        let old = Connection::open(&path)?;
        let messages = snapshot_table(&old, "ops_workspace_messages", "message_id")?;
        let outbox = snapshot_table(&old, "ops_provider_message_outbox", "message_id")?;
        let invitations = snapshot_table(&old, "ops_governance_invitations", "message_id")?;
        let witness: Vec<u8> = old.query_row(
            "SELECT node_route_witness FROM ops_provider_message_outbox WHERE message_id=?1",
            params![Uuid::from_u128(0xA484).as_bytes()],
            |row| row.get(0),
        )?;
        let outbox_foreign_keys = snapshot_foreign_keys(&old, "ops_provider_message_outbox")?;
        let invitation_foreign_keys = snapshot_foreign_keys(&old, "ops_governance_invitations")?;
        drop(old);

        let upgraded = OperationsCatalog::open(&path, destination)?;
        assert_v46_migration_preserved(
            &upgraded,
            &messages,
            &outbox,
            &invitations,
            &witness,
            &outbox_foreign_keys,
            &invitation_foreign_keys,
        )?;
        drop(upgraded);
        let reopened = OperationsCatalog::open(&path, destination)?;
        validate_schema(&reopened.connection)?;
        drop(reopened);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn failed_v46_native_author_migration_rolls_back_and_reopens_after_repair()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("operations-v46-rollback-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xA470);
        create_v46_message_migration_fixture(
            &path,
            destination,
            Some(V46MessageCorruption::InvalidLegacyAuthorShape),
        )?;

        assert!(OperationsCatalog::open(&path, destination).is_err());
        let raw = Connection::open(&path)?;
        assert_eq!(
            raw.pragma_query_value::<i64, _>(None, "user_version", |row| row.get(0))?,
            46
        );
        validate_schema_v46(&raw)?;
        assert_eq!(
            raw.query_row(
                "SELECT author_participant_id FROM ops_workspace_messages WHERE message_id=?1",
                params![Uuid::from_u128(0xA484).as_bytes()],
                |row| row.get::<_, Vec<u8>>(0),
            )?,
            Uuid::from_u128(0xA475).as_bytes()
        );
        raw.execute(
            "UPDATE ops_workspace_messages SET author_participant_id=NULL WHERE message_id=?1",
            params![Uuid::from_u128(0xA484).as_bytes()],
        )?;
        drop(raw);

        let repaired = OperationsCatalog::open(&path, destination)?;
        assert_eq!(
            repaired
                .connection
                .pragma_query_value::<i64, _>(None, "user_version", |row| row.get(0))?,
            VERSION
        );
        validate_schema(&repaired.connection)?;
        assert!(
            repaired
                .workspace_message(Uuid::from_u128(0xA484))?
                .is_some()
        );
        drop(repaired);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn v46_migration_rejects_native_author_kind_without_changing_the_catalog()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!(
            "operations-v46-native-kind-rollback-{}",
            Uuid::now_v7()
        ));
        std::fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xA470);
        create_v46_message_migration_fixture(
            &path,
            destination,
            Some(V46MessageCorruption::UnsupportedFutureNativeAuthor),
        )?;

        let raw = Connection::open(&path)?;
        let native_shape: V46NativeAuthorShape = raw.query_row(
                "SELECT author_kind,author_participation_id,author_profile_id,author_participant_id,author_session_id FROM ops_workspace_messages WHERE message_id=?1",
                params![Uuid::from_u128(0xA484).as_bytes()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )?;
        assert_eq!(native_shape.0, 4);
        assert_eq!(native_shape.1, None);
        assert_eq!(native_shape.2, None);
        assert_eq!(
            native_shape.3,
            Some(Uuid::from_u128(0xA475).as_bytes().to_vec())
        );
        assert_eq!(
            native_shape.4,
            Some(Uuid::from_u128(0xA474).as_bytes().to_vec())
        );
        let messages = snapshot_table(&raw, "ops_workspace_messages", "message_id")?;
        let outbox = snapshot_table(&raw, "ops_provider_message_outbox", "message_id")?;
        let routes = snapshot_table(&raw, "ops_provider_message_routes", "provider_family")?;
        let invitations = snapshot_table(&raw, "ops_governance_invitations", "message_id")?;
        let outbox_foreign_keys = snapshot_foreign_keys(&raw, "ops_provider_message_outbox")?;
        let invitation_foreign_keys = snapshot_foreign_keys(&raw, "ops_governance_invitations")?;
        drop(raw);

        let Some(error) = OperationsCatalog::open(&path, destination).err() else {
            std::fs::remove_dir_all(&root)?;
            return Err("v46 migration accepted a future Native author kind".into());
        };
        assert!(matches!(error, OperationsError::Corrupt(_)));

        let raw = Connection::open(&path)?;
        assert_eq!(
            raw.pragma_query_value::<i64, _>(None, "user_version", |row| row.get(0))?,
            46
        );
        validate_schema_v46(&raw)?;
        assert_eq!(
            snapshot_table(&raw, "ops_workspace_messages", "message_id")?,
            messages
        );
        assert_eq!(
            snapshot_table(&raw, "ops_provider_message_outbox", "message_id")?,
            outbox
        );
        assert_eq!(
            snapshot_table(&raw, "ops_provider_message_routes", "provider_family")?,
            routes
        );
        assert_eq!(
            snapshot_table(&raw, "ops_governance_invitations", "message_id")?,
            invitations
        );
        assert_eq!(
            snapshot_foreign_keys(&raw, "ops_provider_message_outbox")?,
            outbox_foreign_keys
        );
        assert_eq!(
            snapshot_foreign_keys(&raw, "ops_governance_invitations")?,
            invitation_foreign_keys
        );
        let mut foreign_key_check = raw.prepare("PRAGMA foreign_key_check")?;
        assert!(foreign_key_check.query([])?.next()?.is_none());
        drop(foreign_key_check);
        drop(raw);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    fn snapshot_foreign_keys(
        connection: &Connection,
        table: &str,
    ) -> Result<ForeignKeySnapshot, Box<dyn std::error::Error>> {
        let mut statement = connection.prepare(&format!("PRAGMA foreign_key_list('{table}')"))?;
        Ok(statement
            .query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?)
    }

    fn assert_v46_migration_preserved(
        upgraded: &OperationsCatalog,
        messages: &CatalogTableSnapshot,
        outbox: &CatalogTableSnapshot,
        invitations: &CatalogTableSnapshot,
        witness: &[u8],
        outbox_foreign_keys: &ForeignKeySnapshot,
        invitation_foreign_keys: &ForeignKeySnapshot,
    ) -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(messages.len(), 5);
        assert_eq!(outbox.len(), 1);
        assert_eq!(invitations.len(), 1);
        assert_eq!(
            upgraded
                .connection
                .pragma_query_value::<i64, _>(None, "user_version", |row| row.get(0))?,
            VERSION
        );
        assert_eq!(
            &snapshot_table(&upgraded.connection, "ops_workspace_messages", "message_id")?,
            messages
        );
        assert_eq!(
            &snapshot_table(
                &upgraded.connection,
                "ops_provider_message_outbox",
                "message_id"
            )?,
            outbox
        );
        assert_eq!(
            &snapshot_table(
                &upgraded.connection,
                "ops_governance_invitations",
                "message_id"
            )?,
            invitations
        );
        assert_eq!(
            upgraded.connection.query_row(
                "SELECT node_route_witness FROM ops_provider_message_outbox WHERE message_id=?1",
                params![Uuid::from_u128(0xA484).as_bytes()],
                |row| row.get::<_, Vec<u8>>(0),
            )?,
            witness
        );
        assert_eq!(
            &snapshot_foreign_keys(&upgraded.connection, "ops_provider_message_outbox")?,
            outbox_foreign_keys
        );
        assert_eq!(
            &snapshot_foreign_keys(&upgraded.connection, "ops_governance_invitations")?,
            invitation_foreign_keys
        );
        assert_eq!(
            upgraded
                .connection
                .query_row("PRAGMA foreign_key_check", [], |row| row.get::<_, i64>(0))
                .optional()?,
            None
        );
        validate_schema(&upgraded.connection)?;

        for (message_id, author) in [
            (
                Uuid::from_u128(0xA480),
                operational_messages::MessageAuthor::Operator,
            ),
            (
                Uuid::from_u128(0xA481),
                operational_messages::MessageAuthor::Participant {
                    participation_id: crate::WorkspaceParticipationId::from_uuid(Uuid::from_u128(
                        0xA473,
                    ))?,
                    profile_id: Uuid::from_u128(0xA472),
                    participant_id: Uuid::from_u128(0xA475),
                    session_id: forge_domain::AgentSessionId::from_uuid(Uuid::from_u128(0xA474)),
                },
            ),
            (
                Uuid::from_u128(0xA482),
                operational_messages::MessageAuthor::System,
            ),
        ] {
            assert_eq!(
                upgraded
                    .workspace_message(message_id)?
                    .ok_or("legacy message missing")?
                    .author(),
                &author
            );
        }
        assert!(matches!(
            upgraded
                .workspace_message(Uuid::from_u128(0xA483))?
                .ok_or("provider message missing")?
                .author(),
            operational_messages::MessageAuthor::Provider { .. }
        ));
        Ok(())
    }

    #[test]
    fn v38_schema_remains_exact_and_migrates_additively_to_v42()
    -> Result<(), Box<dyn std::error::Error>> {
        let root =
            std::env::temp_dir().join(format!("operations-v38-provider-delete-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::now_v7();
        let old = Connection::open(&path)?;
        for (_, _, sql) in schema_v38_objects() {
            old.execute_batch(&sql)?;
        }
        old.execute(
            "INSERT INTO ops_catalog_identity(singleton,destination_id) VALUES(1,?1)",
            params![destination.as_bytes()],
        )?;
        old.pragma_update(None, "application_id", APPLICATION_ID)?;
        old.pragma_update(None, "user_version", 38)?;
        validate_schema_v38(&old)?;
        drop(old);

        let upgraded = OperationsCatalog::open(&path, destination)?;
        let version: i64 = upgraded
            .connection
            .pragma_query_value(None, "user_version", |row| row.get(0))?;
        assert_eq!(version, VERSION);
        validate_schema(&upgraded.connection)?;
        drop(upgraded);
        std::fs::remove_file(path)?;
        std::fs::remove_dir(root)?;
        Ok(())
    }

    #[test]
    fn v37_schema_remains_exact_and_migrates_additively_to_v42()
    -> Result<(), Box<dyn std::error::Error>> {
        let root =
            std::env::temp_dir().join(format!("operations-v37-profile-delete-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::now_v7();
        let old = Connection::open(&path)?;
        for (_, _, sql) in schema_v37_objects() {
            old.execute_batch(&sql)?;
        }
        old.execute(
            "INSERT INTO ops_catalog_identity(singleton,destination_id) VALUES(1,?1)",
            params![destination.as_bytes()],
        )?;
        old.pragma_update(None, "application_id", APPLICATION_ID)?;
        old.pragma_update(None, "user_version", 37)?;
        validate_schema_v37(&old)?;
        drop(old);

        let upgraded = OperationsCatalog::open(&path, destination)?;
        let version: i64 = upgraded
            .connection
            .pragma_query_value(None, "user_version", |row| row.get(0))?;
        assert_eq!(version, VERSION);
        let barrier_table_exists: bool = upgraded.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema
             WHERE type='table' AND name='ops_agent_profile_deletion_barriers')",
            [],
            |row| row.get(0),
        )?;
        assert!(barrier_table_exists);
        let progress_table_exists: bool = upgraded.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema
             WHERE type='table' AND name='ops_agent_profile_deletion_progress')",
            [],
            |row| row.get(0),
        )?;
        assert!(progress_table_exists);
        validate_schema(&upgraded.connection)?;
        drop(upgraded);
        std::fs::remove_file(path)?;
        std::fs::remove_dir(root)?;
        Ok(())
    }

    fn seed_legacy_chat_messages(
        connection: &Connection,
        workspace: Uuid,
    ) -> Result<(), OperationsError> {
        connection.execute(
            "INSERT INTO ops_workspace_messages(message_id,workspace_id,channel_kind,author_kind,author_display_name,origin_kind,body,sequence,submitted_at_ms) VALUES(?1,?2,0,0,'Local operator',0,'Preserved operator message',1,10)",
            params![Uuid::from_u128(0x3213).as_bytes(), workspace.as_bytes()],
        )?;
        connection.execute(
            "INSERT INTO ops_workspace_messages(message_id,workspace_id,channel_kind,author_kind,author_display_name,origin_kind,body,sequence,submitted_at_ms,provider_conversation_id,provider_turn_id,provider_source_kind,provider_source_label) VALUES(?1,?2,0,2,'Unknown provider',1,'Preserved provider message',2,20,'migration-conversation','migration-turn',1,'Unknown provider')",
            params![Uuid::from_u128(0x3214).as_bytes(), workspace.as_bytes()],
        )?;
        Ok(())
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "Keeps before-and-after migration evidence together for each historical schema."
    )]
    fn v30_and_v31_upgrade_preserve_elections_credits_and_team_history()
    -> Result<(), Box<dyn std::error::Error>> {
        for version in [30, 31] {
            let root =
                std::env::temp_dir().join(format!("governance-migration-{}", Uuid::now_v7()));
            std::fs::create_dir(&root)?;
            let path = root.join("operations.sqlite3");
            let destination = Uuid::from_u128(0x3201);
            let workspace = Uuid::from_u128(0x3202);
            let activity = Uuid::from_u128(0x3203);
            let participant = Uuid::from_u128(0x3204);
            let election = Uuid::from_u128(0x3205);
            let old = Connection::open(&path)?;
            let objects = if version == 30 {
                schema_v30_objects()
            } else {
                schema_v31_objects()
            };
            for (_, _, sql) in objects {
                old.execute_batch(&sql)?;
            }
            old.execute(
                "INSERT INTO ops_catalog_identity VALUES(1,?1)",
                params![destination.as_bytes()],
            )?;
            old.pragma_update(None, "application_id", APPLICATION_ID)?;
            old.pragma_update(None, "user_version", version)?;
            old.execute("INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,0,NULL,'ACTIVE',?2,5,NULL,NULL,100)", params![workspace.as_bytes(),participant.as_bytes()])?;
            old.execute("INSERT INTO ops_leadership_elections VALUES(?1,?2,0,NULL,'RECALL','CLOSED_NO_MAJORITY',?3,?3,90,NULL,0,1,0,10,100)", params![election.as_bytes(),workspace.as_bytes(),participant.as_bytes()])?;
            old.execute(
                "INSERT INTO ops_election_electorate VALUES(?1,?2)",
                params![election.as_bytes(), participant.as_bytes()],
            )?;
            old.execute(
                "INSERT INTO ops_ballots VALUES(?1,?2,'NO',1,80)",
                params![election.as_bytes(), participant.as_bytes()],
            )?;
            old.execute(
                "INSERT INTO ops_leadership_receipts VALUES(?1,0,?2,1,'BOOTSTRAP',?3,NULL,?4,1,10)",
                params![
                    workspace.as_bytes(),
                    activity.as_bytes(),
                    Uuid::from_u128(0x3206).as_bytes(),
                    participant.as_bytes()
                ],
            )?;
            old.execute(
                "INSERT INTO ops_work_credit_entries VALUES(?1,?2,?3,?4,?5,?6,?7,?8,3,1,?9,50,?10)",
                params![
                    Uuid::from_u128(0x3207).as_bytes(),
                    workspace.as_bytes(),
                    Uuid::from_u128(0x3206).as_bytes(),
                    participant.as_bytes(),
                    Uuid::from_u128(0x3208).as_bytes(),
                    activity.as_bytes(),
                    Uuid::from_u128(0x3209).as_bytes(),
                    Uuid::from_u128(0x3210).as_bytes(),
                    Uuid::from_u128(0x3211).as_bytes(),
                    [1_u8; 32].as_slice()
                ],
            )?;
            old.execute(
                "INSERT INTO ops_election_subject_cutovers VALUES(?1,?2,0,NULL,1,2,1,1,1,?3,10)",
                params![
                    election.as_bytes(),
                    workspace.as_bytes(),
                    [3_u8; 32].as_slice()
                ],
            )?;
            old.execute(
                "INSERT INTO ops_election_voter_subjects VALUES(?1,1,?2,NULL,NULL,1)",
                params![election.as_bytes(), participant.as_bytes()],
            )?;
            old.execute(
                "INSERT INTO ops_election_voter_sources VALUES(?1,?2,1,1,?2)",
                params![election.as_bytes(), participant.as_bytes()],
            )?;
            old.execute(
                "INSERT INTO ops_election_subject_ballots VALUES(?1,1,?2,?2,'NO',1,80)",
                params![election.as_bytes(), participant.as_bytes()],
            )?;
            old.execute("INSERT INTO ops_election_work_credit_snapshots VALUES(?1,?2,?3,1,?3,NULL,NULL,1,1,1000,100,3,3,3,0,1,1,?4)",params![election.as_bytes(),workspace.as_bytes(),participant.as_bytes(),[2_u8;32].as_slice()])?;
            if version == 31 {
                old.execute("INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_at_ms,updated_at_ms) VALUES(?1,?2,'Preserved team','migration evidence','ACTIVE',1,'operator','Local operator',10,10)", params![Uuid::from_u128(0x3212).as_bytes(),workspace.as_bytes()])?;
            }
            if version == 31 {
                seed_legacy_chat_messages(&old, workspace)?;
            }
            validate_catalog_schema_version(&old, version)?;
            let tables = [
                "ops_leadership",
                "ops_leadership_elections",
                "ops_election_electorate",
                "ops_ballots",
                "ops_leadership_receipts",
                "ops_work_credit_entries",
                "ops_election_subject_cutovers",
                "ops_election_voter_subjects",
                "ops_election_voter_sources",
                "ops_election_subject_ballots",
                "ops_election_work_credit_snapshots",
            ];
            let mut before = Vec::new();
            for table in tables
                .into_iter()
                .chain((version == 31).then_some("ops_workspace_messages"))
            {
                let mut statement = old.prepare(&format!("SELECT * FROM {table}"))?;
                let columns = statement
                    .column_names()
                    .iter()
                    .map(|name| format!("\"{name}\""))
                    .collect::<Vec<_>>();
                let count = columns.len();
                let rows = statement
                    .query_map([], |row| {
                        (0..count)
                            .map(|index| row.get::<_, rusqlite::types::Value>(index))
                            .collect::<Result<Vec<_>, _>>()
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                before.push((table, columns.join(","), count, rows));
            }
            drop(old);
            let current = OperationsCatalog::open(&path, destination)?;
            assert_eq!(
                current
                    .connection
                    .pragma_query_value::<i64, _>(None, "user_version", |row| row.get(0))?,
                VERSION
            );
            assert_eq!(
                current
                    .connection
                    .pragma_query_value::<i64, _>(None, "foreign_keys", |row| row.get(0))?,
                1
            );
            assert!(
                current
                    .connection
                    .query_row("PRAGMA foreign_key_check", [], |row| row
                        .get::<_, String>(0))
                    .optional()?
                    .is_none()
            );
            for (table, columns, count, expected) in before {
                let mut statement = current
                    .connection
                    .prepare(&format!("SELECT {columns} FROM {table}"))?;
                let actual = statement
                    .query_map([], |row| {
                        (0..count)
                            .map(|index| row.get::<_, rusqlite::types::Value>(index))
                            .collect::<Result<Vec<_>, _>>()
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                assert_eq!(actual, expected, "version {version}: {table}");
            }
            assert_eq!(
                current.connection.query_row(
                    "SELECT COUNT(*) FROM ops_leadership WHERE team_id IS NOT NULL",
                    [],
                    |row| row.get::<_, i64>(0)
                )?,
                0
            );
            if version == 31 {
                assert_eq!(
                    current
                        .connection
                        .query_row("SELECT name FROM ops_teams", [], |row| row
                            .get::<_, String>(0))?,
                    "Preserved team"
                );
            }
            drop(current);
            drop(OperationsCatalog::open(&path, destination)?);
            std::fs::remove_dir_all(root)?;
        }
        Ok(())
    }

    #[test]
    fn stale_schema_initializer_rechecks_after_other_connection_commits()
    -> Result<(), Box<dyn std::error::Error>> {
        let root =
            std::env::temp_dir().join(format!("orchestration-catalog-race-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let path = root.join("catalog.sqlite3");
        let destination = Uuid::from_u128(0xEE01);
        drop(OperationsCatalog::open(&path, destination)?);
        let mut first = Connection::open(&path)?;
        crate::workspace_participation::drop_schema_for_legacy_fixture(&first)?;
        first.execute_batch("DROP TABLE ops_admission_records; DROP TABLE ops_approval_requests; DROP TABLE ops_approval_policy; DROP TABLE ops_issuer_config; DROP TABLE ops_leadership_confirm_intents; DROP TABLE ops_ballots; DROP TABLE ops_election_electorate; DROP TABLE ops_leadership_elections; DROP TABLE ops_leadership_receipts; DROP TABLE ops_leadership; DROP TABLE ops_work_credit_entries; DROP TABLE ops_assignment_submit_source_refs; DROP TABLE ops_assignment_submit_projection; DROP TABLE ops_assignment_offer_projection; DROP TABLE ops_assignment_review_projection; DROP TABLE ops_change_set_projection; DROP TABLE ops_execution_terminal_projection; DROP TABLE ops_effect_fact_consumers; DROP TABLE ops_logical_departure_receipts; DROP TABLE ops_logical_departures; DROP TABLE ops_logical_departure_events; DROP TABLE ops_legacy_adoptions; DROP TABLE ops_current_effect_bindings; DROP TRIGGER ops_logical_receipt_state_insert; DROP TABLE ops_logical_receipt_state; DROP TABLE ops_logical_presence; DROP TABLE ops_binding_activations; DROP TABLE ops_logical_receipts; DROP TABLE ops_logical_bindings; DROP TABLE ops_logical_events; DROP TABLE ops_logical_members; DROP TABLE ops_registration_requests; DROP TABLE ops_pending_participants; PRAGMA user_version=1;")?;
        let mut second = Connection::open(&path)?;
        assert_eq!(catalog_version(&first, destination)?, 1);
        assert_eq!(catalog_version(&second, destination)?, 1);
        initialize_catalog(&mut first, destination)?;
        // The second caller's preflight observed v1, but its write transaction
        // must accept the already-committed current schema rather than recreating tables.
        initialize_catalog(&mut second, destination)?;
        assert_eq!(catalog_version(&second, destination)?, VERSION);
        drop(first);
        drop(second);
        drop(OperationsCatalog::open(&path, destination)?);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn open_existing_read_only_missing_path_does_not_create()
    -> Result<(), Box<dyn std::error::Error>> {
        let root =
            std::env::temp_dir().join(format!("operations-read-only-missing-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let path = root.join("missing.sqlite3");
        let result = OperationsCatalog::open_existing_read_only(&path, Uuid::from_u128(0xEE10));
        assert!(result.is_err());
        assert!(!path.exists());
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn open_existing_read_only_rejects_v32_without_migration()
    -> Result<(), Box<dyn std::error::Error>> {
        let root =
            std::env::temp_dir().join(format!("operations-read-only-v32-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xEE11);
        drop(OperationsCatalog::open(&path, destination)?);
        let connection = Connection::open(&path)?;
        restore_v34_shapes_for_legacy_fixture(&connection)?;
        connection.execute_batch(
            "DROP TABLE ops_dag_node_team_bindings;
             DROP TABLE ops_agent_provision_failures;
             PRAGMA user_version=32;",
        )?;
        drop(connection);

        let result = OperationsCatalog::open_existing_read_only(&path, destination);
        assert!(result.is_err());
        let check = Connection::open(&path)?;
        assert_eq!(
            check.pragma_query_value::<i64, _>(None, "user_version", |row| row.get(0))?,
            32
        );
        assert_eq!(
            check.query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name='ops_agent_provision_failures'",
                [],
                |row| row.get::<_, i64>(0),
            )?,
            0
        );
        drop(check);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn v34_upgrade_preserves_closed_election_sources_without_fabricating_team_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("operations-v34-voters-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let path = root.join("catalog.sqlite3");
        let destination = Uuid::from_u128(0xEE20);
        let workspace = Uuid::from_u128(0xEE21);
        let election = Uuid::from_u128(0xEE22);
        let participant = Uuid::from_u128(0xEE23);
        let old = Connection::open(&path)?;
        for (_, _, sql) in schema_v34_objects() {
            old.execute_batch(&sql)?;
        }
        old.execute(
            "INSERT INTO ops_catalog_identity VALUES(1,?1)",
            params![destination.as_bytes()],
        )?;
        old.pragma_update(None, "application_id", APPLICATION_ID)?;
        old.pragma_update(None, "user_version", 34)?;
        old.execute("INSERT INTO ops_leadership_elections(election_id,workspace_id,scope_kind,activity_id,kind,state,incumbent_participant_id,candidate_participant_id,ballot_deadline_ms,elected_participant_id,tally_yes,tally_no,tally_abstain,created_at_ms,closed_at_ms) VALUES(?1,?2,0,NULL,'RECALL','CLOSED_NO_MAJORITY',?3,?3,90,NULL,0,1,0,10,100)",
            params![election.as_bytes(), workspace.as_bytes(), participant.as_bytes()])?;
        old.execute("INSERT INTO ops_election_subject_cutovers(election_id,workspace_id,scope_kind,source_leadership_revision,result_leadership_revision,subject_revision,source_participant_count,voter_subject_count,electorate_digest,created_at_ms) VALUES(?1,?2,0,7,8,1,1,1,?3,10)",
            params![election.as_bytes(), workspace.as_bytes(), [3_u8;32].as_slice()])?;
        old.execute(
            "INSERT INTO ops_election_voter_subjects VALUES(?1,1,?2,NULL,NULL,1)",
            params![election.as_bytes(), participant.as_bytes()],
        )?;
        old.execute(
            "INSERT INTO ops_election_voter_sources VALUES(?1,?2,7,1,?2)",
            params![election.as_bytes(), participant.as_bytes()],
        )?;
        old.execute(
            "INSERT INTO ops_election_subject_ballots VALUES(?1,1,?2,?2,'NO',1,80)",
            params![election.as_bytes(), participant.as_bytes()],
        )?;
        validate_schema_v34(&old)?;
        drop(old);
        let upgraded = OperationsCatalog::open(&path, destination)?;
        let source: (Vec<u8>, i64, Option<i64>, i64, Vec<u8>) = upgraded.connection.query_row(
            "SELECT participant_id,membership_revision,team_membership_revision,subject_kind,subject_id FROM ops_election_voter_sources WHERE election_id=?1",
            params![election.as_bytes()], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?)))?;
        assert_eq!(
            source,
            (
                participant.as_bytes().to_vec(),
                7,
                None,
                1,
                participant.as_bytes().to_vec()
            )
        );
        assert_eq!(
            election_subject::cast_tallies(&upgraded.connection, election)?,
            (0, 1, 0)
        );
        assert_eq!(
            upgraded.connection.query_row(
                "SELECT COUNT(*) FROM ops_election_team_leader_sources",
                [],
                |row| row.get::<_, i64>(0)
            )?,
            0
        );
        assert_eq!(
            upgraded
                .connection
                .pragma_query_value::<i64, _>(None, "user_version", |row| row.get(0))?,
            VERSION
        );
        drop(upgraded);
        drop(OperationsCatalog::open(&path, destination)?);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn v34_live_elections_cancel_without_losing_votes_or_unknown_confirms()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("operations-v34-policy-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let path = root.join("catalog.sqlite3");
        let destination = Uuid::from_u128(0xEF00);
        let old = Connection::open(&path)?;
        for (_, _, sql) in schema_v34_objects() {
            old.execute_batch(&sql)?;
        }
        old.execute(
            "INSERT INTO ops_catalog_identity VALUES(1,?1)",
            params![destination.as_bytes()],
        )?;
        old.pragma_update(None, "application_id", APPLICATION_ID)?;
        old.pragma_update(None, "user_version", 34)?;
        // Workspace/Team open decisions, unknown Broker outcome, historical
        // confirmation, and an orphan decision whose scope already moved on.
        let cases = [
            (
                0_i64,
                "ELECTING",
                "ELECTING",
                true,
                false,
                true,
                "FAILOVER_REQUIRED",
            ),
            (2, "ELECTING", "ELECTING", true, false, false, "VACANT"),
            (
                0,
                "AUTHORITY_PENDING",
                "CLOSED_ELECTED",
                true,
                true,
                true,
                "RECOVERY_REQUIRED",
            ),
            (
                2,
                "AUTHORITY_PENDING",
                "CLOSED_ELECTED",
                true,
                false,
                true,
                "FAILOVER_REQUIRED",
            ),
            (0, "ACTIVE", "CLOSED_ELECTED", false, false, true, "ACTIVE"),
            (2, "ACTIVE", "ELECTING", false, false, true, "ACTIVE"),
        ];
        for (index, &(kind, phase, election_state, pointer, live, has_leader, _)) in
            cases.iter().enumerate()
        {
            let seed = 0xEF10 + u128::try_from(index)? * 16;
            let workspace = Uuid::from_u128(seed);
            let election = Uuid::from_u128(seed + 1);
            let participant = Uuid::from_u128(seed + 2);
            let team = (kind == 2).then(|| Uuid::from_u128(seed + 3).as_bytes().to_vec());
            let elected = election_state == "CLOSED_ELECTED";
            old.execute("INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,?2,NULL,?3,?4,?5,?7,NULL,?6,150)", params![workspace.as_bytes(),kind,team,phase,has_leader.then(|| participant.as_bytes().to_vec()),pointer.then(|| election.as_bytes().to_vec()),if index==4 {9_i64} else {7_i64}])?;
            old.execute("INSERT INTO ops_leadership_elections(election_id,workspace_id,scope_kind,activity_id,team_id,kind,state,incumbent_participant_id,candidate_participant_id,ballot_deadline_ms,elected_participant_id,tally_yes,tally_no,tally_abstain,created_at_ms,closed_at_ms) VALUES(?1,?2,?3,NULL,?4,'RECALL',?5,?6,?6,90,?7,?8,?9,?9,10,?10)", params![election.as_bytes(),workspace.as_bytes(),kind,team,election_state,participant.as_bytes(),elected.then(|| participant.as_bytes().to_vec()),elected.then_some(1_i64),elected.then_some(0_i64),elected.then_some(100_i64)])?;
            old.execute("INSERT INTO ops_election_subject_cutovers(election_id,workspace_id,scope_kind,activity_id,team_id,source_leadership_revision,result_leadership_revision,subject_revision,source_participant_count,voter_subject_count,electorate_digest,created_at_ms) VALUES(?1,?2,?3,NULL,?4,6,7,1,1,1,?5,10)", params![election.as_bytes(),workspace.as_bytes(),kind,team,[3_u8;32].as_slice()])?;
            old.execute(
                "INSERT INTO ops_election_voter_subjects VALUES(?1,1,?2,NULL,NULL,1)",
                params![election.as_bytes(), participant.as_bytes()],
            )?;
            old.execute(
                "INSERT INTO ops_election_voter_sources VALUES(?1,?2,7,1,?2)",
                params![election.as_bytes(), participant.as_bytes()],
            )?;
            old.execute(
                "INSERT INTO ops_election_subject_ballots VALUES(?1,1,?2,?2,'YES',1,80)",
                params![election.as_bytes(), participant.as_bytes()],
            )?;
            if live || index == 4 {
                old.execute("INSERT INTO ops_leadership_confirm_intents(request_id,kind,workspace_id,scope_kind,activity_id,team_id,election_id,elected_participant_id,expected_local_revision,requested_fence_revision,owner_instance_id,remote_recorded_revision,confirmed_at_ms,state,created_at_ms,resolved_at_ms) VALUES(?1,'election_confirm',?2,?3,NULL,?4,?5,?6,7,7,?7,?8,?9,?10,100,?9)",params![Uuid::from_u128(seed+4).as_bytes(),workspace.as_bytes(),kind,team,election.as_bytes(),participant.as_bytes(),destination.as_bytes(),(!live).then_some(7_i64),(!live).then_some(120_i64),if live {"pending"} else {"committed"}])?;
            }
            if index == 4 {
                old.execute("INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,election_id,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,0,zeroblob(16),zeroblob(16),1,'CONFIRMED',?2,?3,8,120)",params![workspace.as_bytes(),election.as_bytes(),participant.as_bytes()])?;
                old.execute("INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,election_id,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,0,zeroblob(16),zeroblob(16),2,'CONFIRMED',NULL,?2,9,150)",params![workspace.as_bytes(),participant.as_bytes()])?;
            }
        }
        // Contradictory committed authority rolls back the entire migration,
        // including earlier candidates in the same keyset page.
        old.execute("UPDATE ops_leadership SET state='AUTHORITY_PENDING',current_election_id=?2 WHERE workspace_id=?1",params![Uuid::from_u128(0xEF50).as_bytes(),Uuid::from_u128(0xEF51).as_bytes()])?;
        assert!(matches!(
            OperationsCatalog::open(&path, destination),
            Err(OperationsError::Corrupt(_))
        ));
        assert_eq!(
            old.pragma_query_value::<i64, _>(None, "user_version", |row| row.get(0))?,
            34
        );
        assert_eq!(
            old.query_row(
                "SELECT state FROM ops_leadership_elections WHERE election_id=?1",
                params![Uuid::from_u128(0xEF11).as_bytes()],
                |row| row.get::<_, String>(0)
            )?,
            "ELECTING"
        );
        old.execute("UPDATE ops_leadership SET state='ACTIVE',current_election_id=NULL WHERE workspace_id=?1",params![Uuid::from_u128(0xEF50).as_bytes()])?;
        validate_schema_v34(&old)?;
        drop(old);
        let mut upgraded = OperationsCatalog::open(&path, destination)?;
        for (index, &(_, _, old_election_state, pointer, live, _, expected)) in
            cases.iter().enumerate()
        {
            let seed = 0xEF10 + u128::try_from(index)? * 16;
            let workspace = Uuid::from_u128(seed);
            let election = Uuid::from_u128(seed + 1);
            let state: (String,i64,Option<Vec<u8>>) = upgraded.connection.query_row("SELECT state,leadership_revision,current_election_id FROM ops_leadership WHERE workspace_id=?1",params![workspace.as_bytes()],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?)))?;
            assert_eq!(state.0, expected, "case {index}");
            assert_eq!(
                state.1,
                if pointer {
                    8
                } else if index == 4 {
                    9
                } else {
                    7
                }
            );
            assert_eq!(state.2, live.then(|| election.as_bytes().to_vec()));
            let result: (String,Option<i64>,Option<i64>) = upgraded.connection.query_row("SELECT state,closed_at_ms,tally_yes FROM ops_leadership_elections WHERE election_id=?1",params![election.as_bytes()],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?)))?;
            assert_eq!(
                result.0,
                if index == 4 {
                    "CLOSED_ELECTED"
                } else {
                    "CANCELLED"
                }
            );
            assert_eq!(
                result.1,
                Some(if old_election_state == "CLOSED_ELECTED" {
                    100
                } else {
                    150
                })
            );
            assert_eq!(
                result.2,
                (old_election_state == "CLOSED_ELECTED").then_some(1)
            );
            assert_eq!(
                election_subject::cast_tallies(&upgraded.connection, election)?,
                (1, 0, 0)
            );
            let audit: i64 = upgraded.connection.query_row(
                "SELECT COUNT(*) FROM ops_leadership_receipts WHERE workspace_id=?1",
                params![workspace.as_bytes()],
                |row| row.get(0),
            )?;
            assert_eq!(audit, if index == 4 { 2 } else { i64::from(pointer) });
        }
        let scope =
            crate::LeadershipScope::workspace(WorkspaceId::from_uuid(Uuid::from_u128(0xEF30)));
        let replay =
            upgraded.prepare_leadership_confirm(crate::PrepareLeadershipConfirmCommand {
                scope,
                election_id: Uuid::from_u128(0xEF31),
                expected_revision: 7,
                request_id: Uuid::from_u128(0xEF34),
                now_ms: 160,
            })?;
        let crate::PrepareLeadershipConfirmOutcome::Prepared(prepared) = replay else {
            return Err("pending intent was discarded during migration".into());
        };
        let outcome = crate::LeadershipFenceOutcome::from_request(prepared.fence_request(), 7);
        assert!(
            upgraded
                .commit_leadership_confirm(&prepared, outcome, 161)
                .is_err()
        );
        assert_eq!(
            upgraded
                .leadership_scope_view(scope, 162)?
                .ok_or("scope missing")?
                .state(),
            crate::LeadershipState::FailoverRequired
        );
        let counts:(i64,i64)=upgraded.connection.query_row("SELECT (SELECT COUNT(*) FROM ops_leadership_receipts),(SELECT COUNT(*) FROM ops_election_voter_sources)",[],|row|Ok((row.get(0)?,row.get(1)?)))?;
        drop(upgraded);
        let reopened = OperationsCatalog::open(&path, destination)?;
        assert_eq!(reopened.connection.query_row("SELECT (SELECT COUNT(*) FROM ops_leadership_receipts),(SELECT COUNT(*) FROM ops_election_voter_sources)",[],|row|Ok((row.get::<_,i64>(0)?,row.get::<_,i64>(1)?)))?,counts);
        drop(reopened);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn v35_upgrade_preserves_elections_outbox_and_legacy_node_admissions()
    -> Result<(), Box<dyn std::error::Error>> {
        #[derive(Debug, PartialEq, Eq)]
        struct MigratedAffinityRow {
            disposition: String,
            node_goal: Option<Vec<u8>>,
            binding_revision: Option<i64>,
            original_offer_payload: Option<Vec<u8>>,
        }

        type MigratedBindingRow = (
            Option<Vec<u8>>,
            Option<i64>,
            Option<i64>,
            Option<Vec<u8>>,
            Option<String>,
            i64,
        );
        let root = std::env::temp_dir().join(format!("operations-v35-outbox-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let path = root.join("catalog.sqlite3");
        let destination = Uuid::from_u128(0xF100);
        let workspace = Uuid::from_u128(0xF101);
        let election = Uuid::from_u128(0xF102);
        let incumbent = Uuid::from_u128(0xF103);
        let candidate = Uuid::from_u128(0xF104);
        let profile = Uuid::from_u128(0xF105);
        let participation = Uuid::from_u128(0xF106);
        let runtime_session = Uuid::from_u128(0xF107);
        let message = Uuid::from_u128(0xF108);
        let assignment = Uuid::from_u128(0xF109);
        let activity = Uuid::from_u128(0xF10A);
        let target_agent = Uuid::from_u128(0xF10B);
        let target_session = Uuid::from_u128(0xF10C);
        let target_participant = Uuid::from_u128(0xF10D);
        let node_goal = Uuid::from_u128(0xF10E);
        let node_id = Uuid::from_u128(0xF10F);
        let node_team = Uuid::from_u128(0xF110);

        let old = Connection::open(&path)?;
        for (_, _, sql) in schema_v35_objects() {
            old.execute_batch(&sql)?;
        }
        old.execute(
            "INSERT INTO ops_catalog_identity VALUES(1,?1)",
            params![destination.as_bytes()],
        )?;
        old.pragma_update(None, "application_id", APPLICATION_ID)?;
        old.pragma_update(None, "user_version", 35)?;
        old.execute(
            "INSERT INTO ops_workspace_participations VALUES(?1,?2,?3,10,1)",
            params![
                workspace.as_bytes(),
                profile.as_bytes(),
                participation.as_bytes()
            ],
        )?;

        old.execute(
            "INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_at_ms,updated_at_ms) VALUES(?1,?2,'Legacy Team','','ACTIVE',2,'operator','Local operator',10,11)",
            params![node_team.as_bytes(),workspace.as_bytes()],
        )?;
        old.execute(
            "INSERT INTO ops_team_members(workspace_id,team_id,participation_id,state,revision,joined_at_ms,left_at_ms) VALUES(?1,?2,?3,'JOINED',1,11,NULL)",
            params![workspace.as_bytes(),node_team.as_bytes(),participation.as_bytes()],
        )?;

        let old_create_request = Uuid::from_u128(0xF120);
        let old_join_request = Uuid::from_u128(0xF121);
        old.execute(
            "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
            params![Uuid::from_u128(0xF122).as_bytes(),workspace.as_bytes(),destination.as_bytes(),[0_u8;32].as_slice()],
        )?;
        old.execute(
            "INSERT INTO ops_team_requests(request_id,operation,workspace_id,team_id,actor_key,actor_display_name,name,purpose,result_team_state,result_team_revision,result_team_updated_at_ms,created,observed_at_ms) VALUES(?1,'CREATE',?2,?3,'operator','Local operator','Legacy Team','','ACTIVE',1,10,1,10)",
            params![old_create_request.as_bytes(),workspace.as_bytes(),node_team.as_bytes()],
        )?;
        old.execute(
            "INSERT INTO ops_team_requests(request_id,operation,workspace_id,team_id,actor_key,actor_display_name,actor_participation_id,actor_profile_id,actor_participant_id,actor_session_id,expected_team_revision,result_team_state,result_team_revision,result_team_updated_at_ms,result_membership_state,result_membership_participation_id,result_membership_revision,result_membership_joined_at_ms,created,observed_at_ms) VALUES(?1,'JOIN',?2,?3,?4,'Historical member',?5,?6,?7,?8,1,'ACTIVE',2,11,'JOINED',?5,1,11,1,11)",
            params![old_join_request.as_bytes(),workspace.as_bytes(),node_team.as_bytes(),format!("participant:{participation}"),participation.as_bytes(),profile.as_bytes(),target_participant.as_bytes(),target_session.as_bytes()],
        )?;
        old.execute(
            "INSERT INTO ops_team_member_events(request_id,workspace_id,team_id,participation_id,state,membership_revision,actor_key,actor_display_name,actor_participation_id,actor_profile_id,actor_participant_id,actor_session_id,observed_at_ms) VALUES(?1,?2,?3,?4,'JOINED',1,?5,'Historical member',?4,?6,?7,?8,11)",
            params![old_join_request.as_bytes(),workspace.as_bytes(),node_team.as_bytes(),participation.as_bytes(),format!("participant:{participation}"),profile.as_bytes(),target_participant.as_bytes(),target_session.as_bytes()],
        )?;

        old.execute(
            "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,0,NULL,NULL,'ELECTING',?2,7,NULL,?3,150)",
            params![workspace.as_bytes(), incumbent.as_bytes(), election.as_bytes()],
        )?;
        old.execute(
            "INSERT INTO ops_leadership_elections(election_id,workspace_id,scope_kind,activity_id,team_id,kind,state,incumbent_participant_id,candidate_participant_id,ballot_deadline_ms,elected_participant_id,tally_yes,tally_no,tally_abstain,created_at_ms,closed_at_ms) VALUES(?1,?2,0,NULL,NULL,'RECALL','ELECTING',?3,?4,200,NULL,NULL,NULL,NULL,100,NULL)",
            params![election.as_bytes(), workspace.as_bytes(), incumbent.as_bytes(), candidate.as_bytes()],
        )?;
        old.execute(
            "INSERT INTO ops_election_subject_cutovers(election_id,workspace_id,scope_kind,activity_id,team_id,source_leadership_revision,result_leadership_revision,subject_revision,source_participant_count,voter_subject_count,electorate_digest,created_at_ms) VALUES(?1,?2,0,NULL,NULL,6,7,1,1,1,?3,100)",
            params![election.as_bytes(), workspace.as_bytes(), [3_u8; 32].as_slice()],
        )?;
        old.execute(
            "INSERT INTO ops_election_voter_subjects VALUES(?1,0,?2,?3,1,1)",
            params![
                election.as_bytes(),
                participation.as_bytes(),
                profile.as_bytes()
            ],
        )?;
        old.execute(
            "INSERT INTO ops_election_voter_sources(election_id,participant_id,membership_revision,team_membership_revision,subject_kind,subject_id) VALUES(?1,?2,1,NULL,0,?3)",
            params![election.as_bytes(), incumbent.as_bytes(), participation.as_bytes()],
        )?;
        old.execute(
            "INSERT INTO ops_election_subject_ballots VALUES(?1,0,?2,?3,'YES',1,120)",
            params![
                election.as_bytes(),
                participation.as_bytes(),
                incumbent.as_bytes()
            ],
        )?;
        old.execute(
            "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,election_id,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,0,zeroblob(16),zeroblob(16),1,'BOOTSTRAP',NULL,?2,6,90)",
            params![workspace.as_bytes(), incumbent.as_bytes()],
        )?;
        old.execute(
            "INSERT INTO ops_dag_node_team_bindings(workspace_id,goal_id,dag_revision,node_id,team_id,bound_by,bound_at_ms,revision) VALUES(?1,?2,2,?3,?4,'operator',99,4)",
            params![workspace.as_bytes(), node_goal.as_bytes(), node_id.as_bytes(), node_team.as_bytes()],
        )?;
        old.execute(
            "INSERT INTO ops_assignment_team_affinities(workspace_id,assignment_id,activity_id,target_agent_id,target_session_id,target_participation_id,target_participant_id,team_id,team_membership_revision,requester_key,requester_display_name,assignment_payload_fingerprint,observed_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,NULL,NULL,'operator','Local operator',?8,100)",
            params![
                workspace.as_bytes(),
                assignment.as_bytes(),
                activity.as_bytes(),
                target_agent.as_bytes(),
                target_session.as_bytes(),
                participation.as_bytes(),
                target_participant.as_bytes(),
                [8_u8; 32].as_slice(),
            ],
        )?;

        old.execute(
            "INSERT INTO ops_provider_message_routes(provider_family,runtime_session_id,provider_conversation_id,workspace_id,recipient_profile_id,recipient_participation_id,created_at_ms) VALUES(1,?1,'conversation-v35',?2,?3,?4,10)",
            params![runtime_session.as_bytes(), workspace.as_bytes(), profile.as_bytes(), participation.as_bytes()],
        )?;
        old.execute(
            "INSERT INTO ops_workspace_messages(message_id,workspace_id,channel_kind,author_kind,author_display_name,origin_kind,body,sequence,submitted_at_ms) VALUES(?1,?2,0,0,'Local operator',0,'Legacy operator request',1,10)",
            params![message.as_bytes(), workspace.as_bytes()],
        )?;
        old.execute(
            "INSERT INTO ops_provider_message_outbox(message_id,workspace_id,recipient_profile_id,recipient_participation_id,provider_family,runtime_session_id,provider_conversation_id,state,queued_at_ms,last_dispatch_at_ms,updated_at_ms,revision,provider_turn_id,provider_body) VALUES(?1,?2,?3,?4,1,?5,'conversation-v35',1,10,0,10,1,NULL,NULL)",
            params![message.as_bytes(), workspace.as_bytes(), profile.as_bytes(), participation.as_bytes(), runtime_session.as_bytes()],
        )?;
        validate_schema_v35(&old)?;
        drop(old);

        let upgraded = OperationsCatalog::open(&path, destination)?;
        let leadership_state: (String, i64, Option<Vec<u8>>) = upgraded.connection.query_row(
            "SELECT state,leadership_revision,current_election_id FROM ops_leadership WHERE workspace_id=?1 AND scope_kind=0",
            params![workspace.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        assert_eq!(
            leadership_state,
            ("ELECTING".to_owned(), 7, Some(election.as_bytes().to_vec()))
        );
        assert_eq!(
            upgraded.connection.query_row(
                "SELECT state FROM ops_leadership_elections WHERE election_id=?1",
                params![election.as_bytes()],
                |row| row.get::<_, String>(0),
            )?,
            "ELECTING"
        );
        assert_eq!(
            upgraded.connection.query_row(
                "SELECT leader_participant_id FROM ops_leadership WHERE workspace_id=?1 AND scope_kind=0",
                params![workspace.as_bytes()],
                |row| row.get::<_, Option<Vec<u8>>>(0),
            )?,
            Some(incumbent.as_bytes().to_vec())
        );
        assert_eq!(
            upgraded.connection.query_row(
                "SELECT choice FROM ops_election_subject_ballots WHERE election_id=?1",
                params![election.as_bytes()],
                |row| row.get::<_, String>(0),
            )?,
            "YES"
        );
        let history: (String, Option<Vec<u8>>, i64, i64) = upgraded.connection.query_row(
            "SELECT receipt_kind,participant_id,leadership_revision,recorded_at_ms FROM ops_leadership_receipts WHERE workspace_id=?1 AND scope_kind=0 AND receipt_ordinal=1",
            params![workspace.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        assert_eq!(
            history,
            (
                "BOOTSTRAP".to_owned(),
                Some(incumbent.as_bytes().to_vec()),
                6,
                90
            )
        );
        let outbox_state: (i64, i64) = upgraded.connection.query_row(
            "SELECT state,revision FROM ops_provider_message_outbox WHERE message_id=?1",
            params![message.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(outbox_state, (1, 1));
        let witness: Option<Vec<u8>> = upgraded.connection.query_row(
            "SELECT node_route_witness FROM ops_provider_message_outbox WHERE message_id=?1",
            params![message.as_bytes()],
            |row| row.get(0),
        )?;
        assert_eq!(witness, None);
        let migrated_affinity: MigratedAffinityRow =
            upgraded.connection.query_row(
                "SELECT disposition,node_goal_id,node_binding_revision,original_offer_payload FROM ops_assignment_team_affinities WHERE workspace_id=?1 AND assignment_id=?2",
                params![workspace.as_bytes(), assignment.as_bytes()],
                |row| {
                    Ok(MigratedAffinityRow {
                        disposition: row.get(0)?,
                        node_goal: row.get(1)?,
                        binding_revision: row.get(2)?,
                        original_offer_payload: row.get(3)?,
                    })
                },
            )?;
        assert_eq!(
            migrated_affinity,
            MigratedAffinityRow {
                disposition: "LEGACY_UNKNOWN".to_owned(),
                node_goal: None,
                binding_revision: None,
                original_offer_payload: None,
            }
        );

        let migrated_binding: MigratedBindingRow =
            upgraded.connection.query_row(
                "SELECT activity_id,goal_created_at_ms,dag_base_goal_revision,team_id,bound_by,revision FROM ops_dag_node_team_bindings WHERE workspace_id=?1 AND goal_id=?2 AND dag_revision=2 AND node_id=?3",
                params![workspace.as_bytes(), node_goal.as_bytes(), node_id.as_bytes()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
            )?;
        assert_eq!(
            migrated_binding,
            (
                None,
                None,
                None,
                Some(node_team.as_bytes().to_vec()),
                Some("operator".to_owned()),
                4
            )
        );
        let legacy_formation: (String, String, i64, Option<i64>) = upgraded.connection.query_row(
            "SELECT purpose,state,revision,formed_at_ms FROM ops_teams WHERE workspace_id=?1 AND team_id=?2",
            params![workspace.as_bytes(),node_team.as_bytes()],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)),
        )?;
        assert_eq!(
            legacy_formation,
            (String::new(), "ACTIVE".to_owned(), 2, None)
        );
        let legacy_role: Option<String> = upgraded.connection.query_row(
            "SELECT role FROM ops_team_members WHERE workspace_id=?1 AND team_id=?2 AND participation_id=?3",
            params![workspace.as_bytes(),node_team.as_bytes(),participation.as_bytes()],
            |row| row.get(0),
        )?;
        assert_eq!(legacy_role, None);
        assert_eq!(
            upgraded.connection.query_row(
                "SELECT COUNT(*) FROM ops_team_create_roster",
                [],
                |row| row.get::<_, i64>(0)
            )?,
            0
        );
        let legacy_create = upgraded
            .team_mutation(
                WorkspaceId::from_uuid(workspace),
                node_team,
                old_create_request,
            )?
            .ok_or("legacy create receipt missing")?;
        assert!(legacy_create.created());
        assert!(legacy_create.initial_members().is_empty());
        assert_eq!(legacy_create.team().formed_at(), None);
        assert_eq!(legacy_create.team().revision(), 1);
        let legacy_join = upgraded
            .team_mutation(
                WorkspaceId::from_uuid(workspace),
                node_team,
                old_join_request,
            )?
            .ok_or("legacy join receipt missing")?;
        assert_eq!(
            legacy_join
                .membership()
                .ok_or("legacy member receipt missing")?
                .role(),
            None
        );
        assert_eq!(legacy_join.team().revision(), 2);
        assert!(
            upgraded
                .team_mutation(
                    WorkspaceId::from_uuid(workspace),
                    Uuid::from_u128(0xFFFF),
                    old_join_request
                )
                .is_err()
        );
        assert_eq!(
            upgraded.connection.query_row(
                "SELECT COUNT(*) FROM ops_team_member_events",
                [],
                |row| row.get::<_, i64>(0)
            )?,
            1
        );
        let migrated_legacy_retirement: Option<Vec<u8>> = upgraded.connection.query_row(
            "SELECT legacy_retirement_request_id FROM ops_dag_node_team_bindings WHERE workspace_id=?1 AND activity_id IS NULL AND goal_id=?2 AND dag_revision=2 AND node_id=?3",
            params![workspace.as_bytes(), node_goal.as_bytes(), node_id.as_bytes()],
            |row| row.get(0),
        )?;
        assert_eq!(migrated_legacy_retirement, None);
        let recovered_exact_scope = crate::NodeTeamBindingScope::new(
            forge_domain::WorkspaceId::from_uuid(workspace),
            forge_domain::ActivityId::from_uuid(activity),
            forge_domain::GoalId::from_uuid(node_goal),
            forge_domain::TimestampMillis::new(10)?,
            forge_domain::DagRevisionNumber::new(2)?,
            forge_domain::GoalRevision::INITIAL,
            forge_domain::DagNodeId::from_uuid(node_id),
        )?;
        assert!(upgraded.node_team_binding(recovered_exact_scope)?.is_none());
        let aggregate =
            upgraded.node_team_bindings(forge_domain::WorkspaceId::from_uuid(workspace))?;
        let legacy_view = aggregate
            .bindings()
            .iter()
            .find(|binding| binding.goal_id().into_uuid() == node_goal)
            .ok_or("legacy binding missing from aggregate page")?;
        assert_eq!(legacy_view.scope(), None);
        assert_eq!(legacy_view.team_id(), Some(node_team));
        assert_eq!(legacy_view.revision(), 4);
        let goal_generation: Option<i64> = upgraded.connection.query_row(
            "SELECT goal_created_at_ms FROM ops_workspace_messages WHERE message_id=?1",
            params![message.as_bytes()],
            |row| row.get(0),
        )?;
        assert_eq!(goal_generation, None);
        let dag_base_goal_revision: Option<i64> = upgraded.connection.query_row(
            "SELECT dag_base_goal_revision FROM ops_workspace_messages WHERE message_id=?1",
            params![message.as_bytes()],
            |row| row.get(0),
        )?;
        assert_eq!(dag_base_goal_revision, None);
        assert_eq!(
            upgraded
                .connection
                .pragma_query_value::<i64, _>(None, "user_version", |row| row.get(0))?,
            VERSION
        );
        drop(upgraded);

        let reopened = OperationsCatalog::open(&path, destination)?;
        assert_eq!(
            reopened.connection.query_row(
                "SELECT state FROM ops_leadership_elections WHERE election_id=?1",
                params![election.as_bytes()],
                |row| row.get::<_, String>(0),
            )?,
            "ELECTING"
        );
        assert_eq!(
            reopened.connection.query_row(
                "SELECT COUNT(*) FROM ops_election_subject_ballots WHERE election_id=?1",
                params![election.as_bytes()],
                |row| row.get::<_, i64>(0),
            )?,
            1
        );
        let reopened_witness: Option<Vec<u8>> = reopened.connection.query_row(
            "SELECT node_route_witness FROM ops_provider_message_outbox WHERE message_id=?1",
            params![message.as_bytes()],
            |row| row.get(0),
        )?;
        assert_eq!(reopened_witness, None);
        let reopened_legacy_payload: Option<Vec<u8>> = reopened.connection.query_row(
            "SELECT original_offer_payload FROM ops_assignment_team_affinities WHERE workspace_id=?1 AND assignment_id=?2",
            params![workspace.as_bytes(), assignment.as_bytes()],
            |row| row.get(0),
        )?;
        assert_eq!(reopened_legacy_payload, None);
        let reopened_generation: Option<i64> = reopened.connection.query_row(
            "SELECT goal_created_at_ms FROM ops_workspace_messages WHERE message_id=?1",
            params![message.as_bytes()],
            |row| row.get(0),
        )?;
        assert_eq!(reopened_generation, None);
        let reopened_dag_base_goal_revision: Option<i64> = reopened.connection.query_row(
            "SELECT dag_base_goal_revision FROM ops_workspace_messages WHERE message_id=?1",
            params![message.as_bytes()],
            |row| row.get(0),
        )?;
        assert_eq!(reopened_dag_base_goal_revision, None);
        assert_eq!(
            reopened.connection.query_row(
                "SELECT disposition FROM ops_assignment_team_affinities WHERE workspace_id=?1 AND assignment_id=?2",
                params![workspace.as_bytes(), assignment.as_bytes()],
                |row| row.get::<_, String>(0),
            )?,
            "LEGACY_UNKNOWN"
        );
        assert_eq!(
            reopened.connection.query_row(
                "SELECT revision FROM ops_dag_node_team_bindings WHERE workspace_id=?1 AND goal_id=?2 AND dag_revision=2 AND node_id=?3",
                params![workspace.as_bytes(), node_goal.as_bytes(), node_id.as_bytes()],
                |row| row.get::<_, i64>(0),
            )?,
            4
        );
        drop(reopened);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn migrates_v33_catalog_without_node_team_schema() -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!(
            "operations-v33-node-team-migration-{}",
            Uuid::now_v7()
        ));
        std::fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xEE12);
        drop(OperationsCatalog::open(&path, destination)?);

        let connection = Connection::open(&path)?;
        restore_v34_shapes_for_legacy_fixture(&connection)?;
        connection.execute_batch(
            "DROP TABLE ops_dag_node_team_bindings;
             PRAGMA user_version=33;",
        )?;
        drop(connection);

        drop(OperationsCatalog::open(&path, destination)?);
        let check = Connection::open(&path)?;
        assert_eq!(
            check.pragma_query_value::<i64, _>(None, "user_version", |row| row.get(0))?,
            VERSION
        );
        assert_eq!(
            check.query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name='ops_dag_node_team_bindings'",
                [],
                |row| row.get::<_, i64>(0),
            )?,
            1
        );
        drop(check);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    // Candidate test item for catalog.rs::tests. It makes a real v40 unscoped
    // registration, performs the additive v40->41 open, and proves migration does
    // not invent historical Profile attribution. Exact typed evidence attachment
    // is covered by workspace_profile_scope.rs using the normal link API.

    #[test]
    fn v40_registration_upgrade_preserves_request_without_inferred_profile_scope()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::{OperationsCatalog, RegistrationRequest};
        use forge_domain::{
            ActivityId, AgentDeclaredRole, AgentDisplayName, AgentInstanceId, AgentSessionId,
            AgentTransportKind, AuthenticatedPrincipal, TimestampMillis,
        };
        use forge_persistence::AgentBindingReservationRequest;

        let root = std::env::temp_dir().join(format!(
            "operations-v40-registration-profile-scope-{}",
            Uuid::now_v7()
        ));
        std::fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xF401);
        let workspace = Uuid::from_u128(0xF402);
        let migration = Uuid::from_u128(0xF403);
        let request_id = Uuid::from_u128(0xF404);
        let old = Connection::open(&path)?;
        old.pragma_update(None, "foreign_keys", true)?;
        for (_, _, sql) in schema_v40_objects() {
            old.execute_batch(&sql)?;
        }
        old.execute(
            "INSERT INTO ops_catalog_identity(singleton,destination_id) VALUES(1,?1)",
            params![destination.as_bytes()],
        )?;
        old.pragma_update(None, "application_id", APPLICATION_ID)?;
        old.pragma_update(None, "user_version", 40)?;
        validate_schema_v40(&old)?;
        let digest = [0xB4_u8; 32];
        old.execute(
            "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
            params![
                migration.as_bytes(),
                workspace.as_bytes(),
                destination.as_bytes(),
                digest.as_slice(),
            ],
        )?;
        let binding = AgentBindingReservationRequest::new(
            request_id,
            ActivityId::from_uuid(Uuid::from_u128(0xF405)),
            AgentInstanceId::from_uuid(Uuid::from_u128(0xF406)),
            AgentSessionId::from_uuid(Uuid::from_u128(0xF407)),
            AuthenticatedPrincipal::TrustedStdio,
            TimestampMillis::new(100)?,
        )?;
        let request = RegistrationRequest::new(
            Uuid::from_u128(0xF408),
            binding,
            AgentDisplayName::new("legacy unscoped member".to_owned())?,
            AgentDeclaredRole::new("worker".to_owned())?,
            AgentTransportKind::Stdio,
        )?;
        old.execute(
            "INSERT INTO ops_pending_participants(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind) VALUES(?1,?2,?3,0,NULL,?4,?5,?6)",
            params![workspace.as_bytes(), request.proposed_participant_id().as_bytes(), request.binding().session_id().into_uuid().as_bytes(), request.display_name().as_str(), request.declared_role().as_str(), crate::registration::encode_transport(request.transport())],
        )?;
        old.execute(
            "INSERT INTO ops_registration_requests(request_id,migration_id,workspace_id,destination_id,activity_id,session_id,participant_id,agent_id,request_wire,phase,reserved_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,0,NULL)",
            params![request_id.as_bytes(), migration.as_bytes(), workspace.as_bytes(), destination.as_bytes(), request.binding().activity_id().into_uuid().as_bytes(), request.binding().session_id().into_uuid().as_bytes(), request.proposed_participant_id().as_bytes(), request.binding().agent_id().into_uuid().as_bytes(), request.encode()?],
        )?;
        drop(old);

        let upgraded = OperationsCatalog::open(&path, destination)?;
        assert_eq!(
            upgraded
                .registration_progress(request_id)?
                .ok_or("legacy request lost")?
                .request(),
            &request
        );
        assert!(upgraded.registration_profile_scope(request_id)?.is_none());
        let scope_count: i64 = upgraded.connection.query_row(
            "SELECT COUNT(*) FROM ops_registration_profile_scopes",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(
            scope_count, 0,
            "schema migration must not fabricate Profile evidence"
        );
        validate_schema(&upgraded.connection)?;
        drop(upgraded);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn v41_upgrade_creates_an_empty_leadership_work_roster_journal()
    -> Result<(), Box<dyn std::error::Error>> {
        let root =
            std::env::temp_dir().join(format!("operations-v41-work-roster-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xE421);
        let old = Connection::open(&path)?;
        for (_, _, sql) in schema_v41_objects() {
            old.execute_batch(&sql)?;
        }
        old.execute(
            "INSERT INTO ops_catalog_identity(singleton,destination_id) VALUES(1,?1)",
            params![destination.as_bytes()],
        )?;
        old.pragma_update(None, "application_id", APPLICATION_ID)?;
        old.pragma_update(None, "user_version", 41)?;
        validate_schema_v41(&old)?;
        drop(old);

        let upgraded = OperationsCatalog::open(&path, destination)?;
        let version: i64 = upgraded
            .connection
            .pragma_query_value(None, "user_version", |row| row.get(0))?;
        assert_eq!(version, VERSION);
        for table in [
            "ops_leadership_work_rosters",
            "ops_leadership_work_roster_barriers",
            "ops_leadership_work_roster_sessions",
        ] {
            let count: i64 = upgraded.connection.query_row(
                &format!("SELECT COUNT(*) FROM {table}"),
                [],
                |row| row.get(0),
            )?;
            assert_eq!(count, 0, "migration must not invent roster cuts");
        }
        validate_schema(&upgraded.connection)?;
        drop(upgraded);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "Keeps complete historical-row, migration, reopen and read-only evidence in one regression."
    )]
    fn v42_approval_versions_preserve_all_existing_request_and_admission_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        fn snapshot(
            connection: &Connection,
            table: &str,
            order: &str,
        ) -> rusqlite::Result<Vec<Vec<rusqlite::types::Value>>> {
            let mut statement =
                connection.prepare(&format!("SELECT * FROM {table} ORDER BY {order}"))?;
            statement
                .query_map([], |row| {
                    (0..row.as_ref().column_count())
                        .map(|index| row.get(index))
                        .collect()
                })?
                .collect()
        }
        let root = std::env::temp_dir().join(format!("approval-v42-baseline-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::now_v7();
        let old = Connection::open(&path)?;
        for (_, _, sql) in schema_v42_objects() {
            old.execute_batch(&sql)?;
        }
        old.execute(
            "INSERT INTO ops_catalog_identity(singleton,destination_id) VALUES(1,?1)",
            params![destination.as_bytes()],
        )?;
        old.pragma_update(None, "application_id", APPLICATION_ID)?;
        old.pragma_update(None, "user_version", 42)?;
        let workspace = Uuid::from_u128(0xE430);
        let session = Uuid::from_u128(0xE431);
        let auto_scope = Uuid::from_u128(0xE432);
        let decider = Uuid::from_u128(0xE433);
        let approved = Uuid::from_u128(2);
        for (ordinal, state) in [(1_u128, "pending"), (2, "approved"), (3, "cancelled")] {
            let request = Uuid::from_u128(ordinal);
            let decision_time = (state != "pending").then_some(2_i64);
            let actor = (state == "approved").then_some(decider.as_bytes().as_slice());
            let auto = (state == "approved").then_some(auto_scope.as_bytes().as_slice());
            old.execute("INSERT INTO ops_approval_requests(request_id,workspace_id,action_class,request_digest,principal_kind,principal_id,session_id,workspace_read,workspace_write,network,policy_revision,auto_scope_id,state,requested_at_ms,expires_at_ms,decided_at_ms,decided_by) VALUES(?1,?2,'direct_structured',?3,0,NULL,?4,1,0,0,1,?5,?6,1,100,?7,?8)", params![request.as_bytes(),workspace.as_bytes(),[7_u8;32].as_slice(),session.as_bytes(),auto,state,decision_time,actor])?;
        }
        let admission = Uuid::from_u128(0xE434);
        old.execute("INSERT INTO ops_admission_records(admission_id,request_id,workspace_id,issuer_id,backend_id,issued_at_ms,expires_at_ms,state,consumed_at_ms) VALUES(?1,?2,?3,?4,?5,2,90,'issued',NULL)", params![admission.as_bytes(),approved.as_bytes(),workspace.as_bytes(),Uuid::from_u128(0xE435).to_string(),Uuid::from_u128(0xE436).to_string()])?;
        validate_schema_v42(&old)?;
        let requests = snapshot(&old, "ops_approval_requests", "request_id")?;
        let admissions = snapshot(&old, "ops_admission_records", "admission_id")?;
        let identity = snapshot(&old, "ops_catalog_identity", "singleton")?;
        let events = snapshot(&old, "ops_events", "event_id")?;
        drop(old);
        for _ in 0..2 {
            let upgraded = OperationsCatalog::open(&path, destination)?;
            assert_eq!(
                snapshot(&upgraded.connection, "ops_approval_requests", "request_id")?,
                requests
            );
            assert_eq!(
                snapshot(
                    &upgraded.connection,
                    "ops_admission_records",
                    "admission_id"
                )?,
                admissions
            );
            assert_eq!(
                snapshot(&upgraded.connection, "ops_catalog_identity", "singleton")?,
                identity
            );
            assert_eq!(
                snapshot(&upgraded.connection, "ops_events", "event_id")?,
                events
            );
            let mut statement = upgraded.connection.prepare(
                "SELECT revision FROM ops_approval_request_versions ORDER BY request_id",
            )?;
            let versions: Vec<i64> = statement
                .query_map([], |row| row.get(0))?
                .collect::<rusqlite::Result<_>>()?;
            assert_eq!(versions, vec![1, 1, 1]);
            assert_eq!(
                upgraded
                    .connection
                    .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))?,
                VERSION
            );
            validate_schema(&upgraded.connection)?;
        }
        let file_before = std::fs::read(&path)?;
        let readonly = OperationsCatalog::open_existing_read_only(&path, destination)?;
        assert_eq!(
            snapshot(&readonly.connection, "ops_approval_requests", "request_id")?,
            requests
        );
        assert_eq!(
            snapshot(
                &readonly.connection,
                "ops_admission_records",
                "admission_id"
            )?,
            admissions
        );
        assert_eq!(
            snapshot(&readonly.connection, "ops_catalog_identity", "singleton")?,
            identity
        );
        assert_eq!(
            snapshot(&readonly.connection, "ops_events", "event_id")?,
            events
        );
        assert_eq!(
            readonly
                .connection
                .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))?,
            VERSION
        );
        drop(readonly);
        assert_eq!(
            std::fs::read(&path)?,
            file_before,
            "read-only current catalog access changed database bytes"
        );
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn v43_upgrade_preserves_catalog_identity_and_adds_empty_binding_rejections()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!(
            "operations-v43-binding-rejections-{}",
            Uuid::now_v7()
        ));
        std::fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::now_v7();
        let old = Connection::open(&path)?;
        for (_, _, sql) in schema_v43_objects() {
            old.execute_batch(&sql)?;
        }
        old.execute(
            "INSERT INTO ops_catalog_identity(singleton,destination_id) VALUES(1,?1)",
            params![destination.as_bytes()],
        )?;
        old.pragma_update(None, "application_id", APPLICATION_ID)?;
        old.pragma_update(None, "user_version", 43)?;
        validate_schema_v43(&old)?;
        drop(old);

        let upgraded = OperationsCatalog::open(&path, destination)?;
        let stored_destination: Vec<u8> = upgraded.connection.query_row(
            "SELECT substr(destination_id,1,17) FROM ops_catalog_identity WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(wire::id(stored_destination)?, destination.into_bytes());
        let rejection_count: i64 = upgraded.connection.query_row(
            "SELECT COUNT(*) FROM ops_binding_rejections",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(rejection_count, 0);
        let attempt_count: i64 = upgraded.connection.query_row(
            "SELECT COUNT(*) FROM ops_binding_rejection_attempts",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(attempt_count, 0);
        let resolution_count: i64 = upgraded.connection.query_row(
            "SELECT COUNT(*) FROM ops_binding_rejection_attempt_resolutions",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(resolution_count, 0);
        assert_eq!(
            upgraded
                .connection
                .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))?,
            VERSION
        );
        validate_schema(&upgraded.connection)?;
        drop(upgraded);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}

#[cfg(test)]
mod assignment_affinity_schema_evolution_tests {
    use super::*;
    use rusqlite::Connection;
    use std::{error::Error, path::Path};

    #[derive(Clone, Copy)]
    enum RequesterShape {
        Operator,
        Profile,
        Mixed,
    }

    fn insert_affinity(
        connection: &Connection,
        workspace: Uuid,
        assignment: Uuid,
        requester_shape: RequesterShape,
        sealed: bool,
    ) -> Result<(), Box<dyn Error>> {
        let requester_activity = Uuid::from_u128(0xB101);
        let requester_agent = Uuid::from_u128(0xB102);
        let requester_session = Uuid::from_u128(0xB103);
        let requester_participation = Uuid::from_u128(0xB104);
        let requester_profile = Uuid::from_u128(0xB105);
        let requester_participant = Uuid::from_u128(0xB106);
        let profile_fields = !matches!(requester_shape, RequesterShape::Operator);
        let valid_profile = matches!(requester_shape, RequesterShape::Profile);
        let identity_activity = profile_fields.then_some(requester_activity.as_bytes().as_slice());
        let identity_agent = profile_fields.then_some(requester_agent.as_bytes().as_slice());
        let identity_session = profile_fields.then_some(requester_session.as_bytes().as_slice());
        let identity_participation =
            valid_profile.then_some(requester_participation.as_bytes().as_slice());
        let identity_profile = profile_fields.then_some(requester_profile.as_bytes().as_slice());
        let identity_participant =
            profile_fields.then_some(requester_participant.as_bytes().as_slice());
        let requester_key = match requester_shape {
            RequesterShape::Operator => "operator".to_owned(),
            RequesterShape::Profile => format!("participant:{requester_participation}"),
            RequesterShape::Mixed => format!("participant:{requester_profile}"),
        };
        let team = Uuid::from_u128(0xB110);
        let target_agent = Uuid::from_u128(0xB111);
        let target_session = Uuid::from_u128(0xB112);
        let target_participation = Uuid::from_u128(0xB113);
        let target_participant = Uuid::from_u128(0xB114);
        let goal = Uuid::from_u128(0xB115);
        let node = Uuid::from_u128(0xB116);
        let seal_agent = Uuid::from_u128(0xB117);
        let seal_session = Uuid::from_u128(0xB118);
        let current_activity = Uuid::from_u128(0xB119);
        let disposition = if sealed { "QUIESCED" } else { "PENDING" };
        let seal_authority = sealed.then_some("COORDINATOR");
        let seal_observation = sealed.then_some("FOREIGN_ACTIVITY");
        let seal_agent_bytes = sealed.then_some(seal_agent.as_bytes().as_slice());
        let seal_session_bytes = sealed.then_some(seal_session.as_bytes().as_slice());
        let seal_current_activity_bytes = sealed.then_some(current_activity.as_bytes().as_slice());
        let seal_revision = sealed.then_some(7_i64);
        let seal_time = sealed.then_some(120_i64);
        let seal_applied_time = sealed.then_some(121_i64);
        let seal_current_state = sealed.then_some("RUNNING");

        connection.execute(
            "INSERT INTO ops_assignment_team_affinities(
                workspace_id,assignment_id,activity_id,target_agent_id,target_session_id,
                target_participation_id,target_participant_id,team_id,team_membership_revision,
                requester_activity_id,requester_agent_id,requester_session_id,
                requester_participation_id,requester_profile_id,requester_participant_id,
                requester_key,requester_display_name,assignment_payload_fingerprint,
                observed_at_ms,node_goal_id,goal_created_at_ms,node_dag_revision,
                node_dag_base_goal_revision,node_id,node_binding_revision,disposition,
                original_offer_payload,seal_sealed_at_ms,seal_applied_at_ms,
                seal_authority_kind,seal_authority_agent_id,seal_authority_session_id,
                seal_authority_revision,seal_observation_kind,seal_current_activity_id,
                seal_current_state,seal_current_revision
            ) VALUES(
                ?1,?2,?3,?4,?5,?6,?7,?8,4,?9,?10,?11,?12,?13,?14,?15,?16,?17,
                110,?18,20,1,1,?19,3,?20,?21,?22,?23,?24,?25,?26,?27,?28,?29,?30,?31
            )",
            rusqlite::params![
                workspace.as_bytes(),
                assignment.as_bytes(),
                requester_activity.as_bytes(),
                target_agent.as_bytes(),
                target_session.as_bytes(),
                target_participation.as_bytes(),
                target_participant.as_bytes(),
                team.as_bytes(),
                identity_activity,
                identity_agent,
                identity_session,
                identity_participation,
                identity_profile,
                identity_participant,
                requester_key,
                if valid_profile {
                    "Profile requester"
                } else {
                    "Local operator"
                },
                [0xB1_u8; 32].as_slice(),
                goal.as_bytes(),
                node.as_bytes(),
                disposition,
                [0xB2_u8, 0, 0xB3].as_slice(),
                seal_time,
                seal_applied_time,
                seal_authority,
                seal_agent_bytes,
                seal_session_bytes,
                seal_revision,
                seal_observation,
                seal_current_activity_bytes,
                seal_current_state,
                seal_revision,
            ],
        )?;
        Ok(())
    }

    fn snapshot(connection: &Connection) -> rusqlite::Result<Vec<Vec<rusqlite::types::Value>>> {
        let mut statement = connection.prepare(
            "SELECT * FROM ops_assignment_team_affinities ORDER BY workspace_id,assignment_id",
        )?;
        statement
            .query_map([], |row| {
                (0..row.as_ref().column_count())
                    .map(|index| row.get(index))
                    .collect()
            })?
            .collect()
    }

    fn create_v47(path: &Path, destination: Uuid) -> Result<Connection, Box<dyn Error>> {
        let connection = Connection::open(path)?;
        let objects = schema_v47_objects();
        for (kind, _, sql) in &objects {
            if *kind == "table" {
                connection.execute_batch(sql)?;
            }
        }
        for (kind, _, sql) in &objects {
            if *kind != "table" {
                connection.execute_batch(sql)?;
            }
        }
        connection.execute(
            "INSERT INTO ops_catalog_identity(singleton,destination_id) VALUES(1,?1)",
            rusqlite::params![destination.as_bytes()],
        )?;
        connection.pragma_update(None, "application_id", APPLICATION_ID)?;
        connection.pragma_update(None, "user_version", 47_i64)?;
        validate_schema_v47(&connection)?;
        Ok(connection)
    }

    fn assert_no_foreign_key_violations(connection: &Connection) -> rusqlite::Result<()> {
        let mut statement = connection.prepare("PRAGMA foreign_key_check")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        assert!(rows.collect::<Result<Vec<_>, _>>()?.is_empty());
        Ok(())
    }

    #[test]
    fn v47_affinity_upgrade_preserves_all_columns_and_seal_provenance() -> Result<(), Box<dyn Error>>
    {
        let root = std::env::temp_dir().join(format!("assignment-affinity-v47-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xB001);
        let workspace = Uuid::from_u128(0xB002);
        let old = create_v47(&path, destination)?;
        insert_affinity(
            &old,
            workspace,
            Uuid::from_u128(0xB003),
            RequesterShape::Operator,
            false,
        )?;
        insert_affinity(
            &old,
            workspace,
            Uuid::from_u128(0xB004),
            RequesterShape::Profile,
            true,
        )?;
        let before = snapshot(&old)?;
        drop(old);

        let upgraded = OperationsCatalog::open(&path, destination)?;
        assert_eq!(
            upgraded
                .connection
                .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))?,
            VERSION
        );
        assert_eq!(snapshot(&upgraded.connection)?, before);
        assert_no_foreign_key_violations(&upgraded.connection)?;
        validate_schema(&upgraded.connection)?;
        drop(upgraded);

        let reopened = OperationsCatalog::open(&path, destination)?;
        assert_eq!(snapshot(&reopened.connection)?, before);
        assert_no_foreign_key_violations(&reopened.connection)?;
        validate_schema(&reopened.connection)?;
        drop(reopened);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn v47_mixed_requester_identity_rejects_upgrade_without_partial_rewrite()
    -> Result<(), Box<dyn Error>> {
        let root =
            std::env::temp_dir().join(format!("assignment-affinity-mixed-v47-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xB011);
        let workspace = Uuid::from_u128(0xB012);
        let old = create_v47(&path, destination)?;
        insert_affinity(
            &old,
            workspace,
            Uuid::from_u128(0xB013),
            RequesterShape::Operator,
            false,
        )?;
        old.pragma_update(None, "ignore_check_constraints", true)?;
        insert_affinity(
            &old,
            workspace,
            Uuid::from_u128(0xB014),
            RequesterShape::Mixed,
            false,
        )?;
        old.pragma_update(None, "ignore_check_constraints", false)?;
        validate_schema_v47(&old)?;
        let before = snapshot(&old)?;
        drop(old);

        assert!(OperationsCatalog::open(&path, destination).is_err());

        let unchanged = Connection::open(&path)?;
        assert_eq!(
            unchanged.pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))?,
            47
        );
        assert_eq!(snapshot(&unchanged)?, before);
        assert_no_foreign_key_violations(&unchanged)?;
        validate_schema_v47(&unchanged)?;
        drop(unchanged);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
