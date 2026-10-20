//! Immutable Source proof that one verified Operations import was handed off.

use crate::{ImportPhase, ImportProgress, OperationsCatalog, OperationsError, catalog};
use forge_domain::{ForgeBackendId, OrchestrationIssuerId, WorkspaceId};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::str::FromStr;
use uuid::Uuid;

const SEALS_SQL: &str = "CREATE TABLE ops_import_handoff_seals (migration_id BLOB PRIMARY KEY NOT NULL REFERENCES ops_imports(migration_id) CHECK(length(migration_id)=16), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id!=zeroblob(16)), destination_id BLOB NOT NULL CHECK(length(destination_id)=16 AND destination_id!=zeroblob(16)), source_revision INTEGER NOT NULL CHECK(typeof(source_revision)='integer' AND source_revision>=1), verified_digest BLOB NOT NULL CHECK(length(verified_digest)=32), page_count INTEGER NOT NULL CHECK(typeof(page_count)='integer' AND page_count>=0), record_count INTEGER NOT NULL CHECK(typeof(record_count)='integer' AND record_count>=0), verified_pages INTEGER NOT NULL CHECK(typeof(verified_pages)='integer' AND verified_pages=page_count), issuer_id TEXT NOT NULL CHECK(length(issuer_id)=36 AND issuer_id=lower(issuer_id)), backend_id TEXT NOT NULL CHECK(length(backend_id)=36 AND backend_id=lower(backend_id)), sealed_at_ms INTEGER NOT NULL CHECK(typeof(sealed_at_ms)='integer' AND sealed_at_ms>=0)) STRICT";
const SEAL_INSERT_GUARD_SQL: &str = "CREATE TRIGGER ops_import_handoff_seals_validate_insert BEFORE INSERT ON ops_import_handoff_seals WHEN NOT EXISTS (SELECT 1 FROM ops_imports AS i JOIN ops_issuer_config AS issuer ON issuer.singleton=1 WHERE i.migration_id=NEW.migration_id AND i.workspace_id=NEW.workspace_id AND i.destination_id=NEW.destination_id AND i.source_revision=NEW.source_revision AND i.phase=2 AND i.next_cursor IS NULL AND i.page_count=NEW.page_count AND i.record_count=NEW.record_count AND i.verified_pages=NEW.verified_pages AND i.import_digest=NEW.verified_digest AND i.verify_digest=NEW.verified_digest AND issuer.issuer_id=NEW.issuer_id AND issuer.backend_id=NEW.backend_id) BEGIN SELECT RAISE(ABORT,'operations import handoff seal does not match verified source'); END";
const SEAL_UPDATE_GUARD_SQL: &str = "CREATE TRIGGER ops_import_handoff_seals_no_update BEFORE UPDATE ON ops_import_handoff_seals BEGIN SELECT RAISE(ABORT,'operations import handoff seals are immutable'); END";
const SEAL_DELETE_GUARD_SQL: &str = "CREATE TRIGGER ops_import_handoff_seals_no_delete BEFORE DELETE ON ops_import_handoff_seals BEGIN SELECT RAISE(ABORT,'operations import handoff seals are immutable'); END";
const IMPORT_UPDATE_GUARD_SQL: &str = "CREATE TRIGGER ops_import_handoff_sealed_import_no_update BEFORE UPDATE ON ops_imports WHEN EXISTS (SELECT 1 FROM ops_import_handoff_seals WHERE migration_id=OLD.migration_id) BEGIN SELECT RAISE(ABORT,'sealed operations imports are immutable'); END";
const IMPORT_DELETE_GUARD_SQL: &str = "CREATE TRIGGER ops_import_handoff_sealed_import_no_delete BEFORE DELETE ON ops_imports WHEN EXISTS (SELECT 1 FROM ops_import_handoff_seals WHERE migration_id=OLD.migration_id) BEGIN SELECT RAISE(ABORT,'sealed operations imports cannot be deleted'); END";

/// Immutable Source-derived evidence for one verified import handoff.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportHandoffSeal {
    migration_id: Uuid,
    workspace_id: WorkspaceId,
    destination_id: Uuid,
    source_revision: u64,
    verified_digest: [u8; 32],
    page_count: u64,
    record_count: u64,
    verified_pages: u64,
    issuer_id: OrchestrationIssuerId,
    backend_id: ForgeBackendId,
    sealed_at_ms: i64,
}

impl ImportHandoffSeal {
    #[must_use]
    pub const fn migration_id(&self) -> Uuid {
        self.migration_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn destination_id(&self) -> Uuid {
        self.destination_id
    }

    #[must_use]
    pub const fn source_revision(&self) -> u64 {
        self.source_revision
    }

    #[must_use]
    pub const fn verified_digest(&self) -> [u8; 32] {
        self.verified_digest
    }

    #[must_use]
    pub const fn page_count(&self) -> u64 {
        self.page_count
    }

    #[must_use]
    pub const fn record_count(&self) -> u64 {
        self.record_count
    }

    #[must_use]
    pub const fn verified_pages(&self) -> u64 {
        self.verified_pages
    }

    pub const fn issuer_id(&self) -> OrchestrationIssuerId {
        self.issuer_id
    }

    /// Target Forge backend captured from Source's admission issuer configuration.
    pub const fn backend_id(&self) -> ForgeBackendId {
        self.backend_id
    }

    #[must_use]
    pub const fn sealed_at_ms(&self) -> i64 {
        self.sealed_at_ms
    }
}

/// Caller expectations are equality fences only; Source derives the proof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportHandoffSealCommand {
    pub migration_id: Uuid,
    pub workspace_id: Uuid,
    pub destination_id: Uuid,
    pub expected_source_revision: u64,
    pub expected_verified_digest: [u8; 32],
    pub sealed_at_ms: i64,
}

/// Current non-aborted import and its optional exact handoff seal.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentImportHandoff {
    progress: Option<ImportProgress>,
    handoff_seal: Option<ImportHandoffSeal>,
}

impl CurrentImportHandoff {
    #[must_use]
    pub const fn progress(&self) -> Option<&ImportProgress> {
        self.progress.as_ref()
    }

    #[must_use]
    pub const fn handoff_seal(&self) -> Option<&ImportHandoffSeal> {
        self.handoff_seal.as_ref()
    }
}

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("table", "ops_import_handoff_seals", SEALS_SQL),
        (
            "trigger",
            "ops_import_handoff_seals_validate_insert",
            SEAL_INSERT_GUARD_SQL,
        ),
        (
            "trigger",
            "ops_import_handoff_seals_no_update",
            SEAL_UPDATE_GUARD_SQL,
        ),
        (
            "trigger",
            "ops_import_handoff_seals_no_delete",
            SEAL_DELETE_GUARD_SQL,
        ),
        (
            "trigger",
            "ops_import_handoff_sealed_import_no_update",
            IMPORT_UPDATE_GUARD_SQL,
        ),
        (
            "trigger",
            "ops_import_handoff_sealed_import_no_delete",
            IMPORT_DELETE_GUARD_SQL,
        ),
    ]
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in schema_objects() {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn drop_schema_for_legacy_fixture(
    connection: &Connection,
) -> Result<(), OperationsError> {
    let has_table: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='ops_import_handoff_seals')",
        [],
        |row| row.get(0),
    )?;
    if has_table {
        let seal_count: i64 =
            connection.query_row("SELECT COUNT(*) FROM ops_import_handoff_seals", [], |row| {
                row.get(0)
            })?;
        if seal_count != 0 {
            return Err(OperationsError::Conflict(
                "legacy fixture contains handoff seals",
            ));
        }
    }
    connection.execute_batch(
        "DROP TRIGGER IF EXISTS ops_import_handoff_sealed_import_no_delete;
         DROP TRIGGER IF EXISTS ops_import_handoff_sealed_import_no_update;
         DROP TRIGGER IF EXISTS ops_import_handoff_seals_no_delete;
         DROP TRIGGER IF EXISTS ops_import_handoff_seals_no_update;
         DROP TRIGGER IF EXISTS ops_import_handoff_seals_validate_insert;
         DROP TABLE IF EXISTS ops_import_handoff_seals;",
    )?;
    Ok(())
}

impl OperationsCatalog {
    /// Seal one verified import and return Source-owned immutable evidence.
    ///
    /// The IMMEDIATE transaction serializes this operation against import abort.
    /// Exact replay returns the original proof, including its original seal time
    /// and issuer/backend snapshot.
    ///
    /// # Errors
    /// Rejects foreign identities, stale revision/digest expectations, an
    /// unverified or inconsistent import, missing issuer configuration, and
    /// persistence failures.
    pub fn seal_import_handoff(
        &mut self,
        command: &ImportHandoffSealCommand,
    ) -> Result<ImportHandoffSeal, OperationsError> {
        validate_command(command, self.destination)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;

        if let Some(existing) = load_seal(&tx, command.migration_id)? {
            validate_expected_seal(&existing, command)?;
            catalog::commit_read(tx, self.destination)?;
            return Ok(existing);
        }

        let progress = catalog::load_progress(&tx, command.migration_id)?
            .ok_or(OperationsError::Conflict("import not prepared"))?;
        validate_verified_progress(&progress, command)?;
        crate::importer::verify_verified_import_counts(&tx, &progress)?;

        let issuer_config = crate::approval::admission_issuer_config_from(&tx)?
            .ok_or(OperationsError::Conflict("admission issuer not configured"))?;
        let issuer_text = issuer_config.issuer().to_string();
        let backend_text = issuer_config.backend_id().to_string();
        let issuer_id = issuer_config.issuer();
        let backend_id = issuer_config.backend_id();
        tx.execute(
            "INSERT INTO ops_import_handoff_seals(migration_id,workspace_id,destination_id,source_revision,verified_digest,page_count,record_count,verified_pages,issuer_id,backend_id,sealed_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            params![
                command.migration_id.as_bytes(),
                command.workspace_id.as_bytes(),
                command.destination_id.as_bytes(),
                catalog::signed(progress.source_revision)?,
                progress.digest.as_slice(),
                catalog::signed(progress.pages)?,
                catalog::signed(progress.records)?,
                catalog::signed(progress.verified_pages)?,
                issuer_text,
                backend_text,
                command.sealed_at_ms,
            ],
        )?;
        let proof = load_seal(&tx, command.migration_id)?
            .ok_or(OperationsError::Corrupt("handoff seal readback"))?;
        validate_seal_progress(&proof, &progress)?;
        if proof.issuer_id != issuer_id || proof.backend_id != backend_id {
            return Err(OperationsError::Corrupt("handoff issuer readback"));
        }
        catalog::commit(tx, command.migration_id)?;
        Ok(proof)
    }

    /// Read one exact optional seal without contacting Forge.
    ///
    /// # Errors
    /// Rejects invalid identities, mismatched import targets, corrupt receipts,
    /// and catalog failures.
    pub fn import_handoff_seal(
        &self,
        migration_id: Uuid,
        workspace_id: Uuid,
        destination_id: Uuid,
    ) -> Result<Option<ImportHandoffSeal>, OperationsError> {
        validate_target(migration_id, workspace_id, destination_id, self.destination)?;
        let tx = self.connection.unchecked_transaction()?;
        let progress = catalog::load_progress(&tx, migration_id)?;
        let seal = load_seal(&tx, migration_id)?;
        match (progress.as_ref(), seal.as_ref()) {
            (None, Some(_)) => return Err(OperationsError::Corrupt("orphan handoff seal")),
            (Some(progress), _) => {
                validate_target_progress(progress, workspace_id, destination_id)?;
            }
            (None, None) => {}
        }
        if let (Some(progress), Some(seal)) = (progress.as_ref(), seal.as_ref()) {
            validate_seal_progress(seal, progress)?;
        }
        catalog::commit_read(tx, self.destination)?;
        Ok(seal)
    }

    /// Read the unique active import for one Workspace and this catalog.
    ///
    /// A present active import owned by a different destination is a conflict,
    /// never an empty result.
    ///
    /// # Errors
    /// Rejects invalid identities, foreign destination ownership, inconsistent
    /// rows, and catalog failures.
    pub fn current_import_handoff(
        &self,
        workspace_id: Uuid,
        destination_id: Uuid,
    ) -> Result<CurrentImportHandoff, OperationsError> {
        if workspace_id.is_nil() || destination_id.is_nil() {
            return Err(OperationsError::Conflict("current import identity"));
        }
        if destination_id != self.destination {
            return Err(OperationsError::Conflict("current import destination"));
        }
        let tx = self.connection.unchecked_transaction()?;
        let mut statement = tx.prepare(
            "SELECT substr(migration_id,1,17),substr(destination_id,1,17) FROM ops_imports WHERE workspace_id=?1 AND phase!=3 ORDER BY migration_id LIMIT 2",
        )?;
        let mut rows = statement.query(params![workspace_id.as_bytes()])?;
        let first = rows
            .next()?
            .map(|row| {
                Ok::<_, OperationsError>((
                    decode_uuid(&row.get::<_, Vec<u8>>(0)?, "import migration identity")?,
                    decode_uuid(&row.get::<_, Vec<u8>>(1)?, "import destination identity")?,
                ))
            })
            .transpose()?;
        if rows.next()?.is_some() {
            return Err(OperationsError::Corrupt(
                "multiple active workspace imports",
            ));
        }
        drop(rows);
        drop(statement);

        let result = if let Some((migration_id, current_destination)) = first {
            if current_destination != destination_id || current_destination != self.destination {
                return Err(OperationsError::Conflict("current import destination"));
            }
            let progress = catalog::load_progress(&tx, migration_id)?
                .ok_or(OperationsError::Corrupt("active import progress missing"))?;
            validate_target_progress(&progress, workspace_id, destination_id)?;
            let handoff_seal = load_seal(&tx, migration_id)?;
            if let Some(seal) = handoff_seal.as_ref() {
                validate_seal_progress(seal, &progress)?;
            }
            CurrentImportHandoff {
                progress: Some(progress),
                handoff_seal,
            }
        } else {
            CurrentImportHandoff {
                progress: None,
                handoff_seal: None,
            }
        };
        catalog::commit_read(tx, self.destination)?;
        Ok(result)
    }
}

fn validate_command(
    command: &ImportHandoffSealCommand,
    destination: Uuid,
) -> Result<(), OperationsError> {
    validate_target(
        command.migration_id,
        command.workspace_id,
        command.destination_id,
        destination,
    )?;
    if command.expected_source_revision == 0
        || i64::try_from(command.expected_source_revision).is_err()
        || command.expected_verified_digest == [0; 32]
        || command.sealed_at_ms < 0
    {
        return Err(OperationsError::Conflict("handoff seal expectations"));
    }
    Ok(())
}

fn validate_target(
    migration_id: Uuid,
    workspace_id: Uuid,
    destination_id: Uuid,
    catalog_destination: Uuid,
) -> Result<(), OperationsError> {
    if migration_id.is_nil() || workspace_id.is_nil() || destination_id.is_nil() {
        return Err(OperationsError::Conflict("handoff target identity"));
    }
    if destination_id != catalog_destination {
        return Err(OperationsError::Conflict("handoff target destination"));
    }
    Ok(())
}

fn validate_target_progress(
    progress: &ImportProgress,
    workspace_id: Uuid,
    destination_id: Uuid,
) -> Result<(), OperationsError> {
    if progress.workspace_id().into_uuid() != workspace_id
        || progress.destination_id() != destination_id
    {
        return Err(OperationsError::Conflict("handoff import target"));
    }
    Ok(())
}

fn validate_expected_seal(
    seal: &ImportHandoffSeal,
    command: &ImportHandoffSealCommand,
) -> Result<(), OperationsError> {
    if seal.migration_id != command.migration_id
        || seal.workspace_id.into_uuid() != command.workspace_id
        || seal.destination_id != command.destination_id
        || seal.source_revision != command.expected_source_revision
        || seal.verified_digest != command.expected_verified_digest
    {
        return Err(OperationsError::Conflict("handoff seal replay"));
    }
    Ok(())
}

fn validate_verified_progress(
    progress: &ImportProgress,
    command: &ImportHandoffSealCommand,
) -> Result<(), OperationsError> {
    validate_target_progress(progress, command.workspace_id, command.destination_id)?;
    if progress.source_revision != command.expected_source_revision
        || progress.phase != ImportPhase::Verified
        || progress.next.is_some()
        || progress.verified_pages != progress.pages
        || progress.digest != command.expected_verified_digest
        || progress.verify_digest != command.expected_verified_digest
    {
        return Err(OperationsError::Conflict("import not exactly verified"));
    }
    Ok(())
}

fn validate_seal_progress(
    seal: &ImportHandoffSeal,
    progress: &ImportProgress,
) -> Result<(), OperationsError> {
    if seal.migration_id != progress.migration_id()
        || seal.workspace_id != progress.workspace_id()
        || seal.destination_id != progress.destination_id()
        || seal.source_revision != progress.source_revision()
        || seal.verified_digest != progress.digest()
        || seal.page_count != progress.page_count()
        || seal.record_count != progress.record_count()
        || seal.verified_pages != progress.verified_pages()
        || progress.phase() != ImportPhase::Verified
        || progress.next_cursor().is_some()
        || progress.digest() != progress.verify_digest()
    {
        return Err(OperationsError::Corrupt("handoff seal import snapshot"));
    }
    Ok(())
}

fn load_seal(
    connection: &Connection,
    migration_id: Uuid,
) -> Result<Option<ImportHandoffSeal>, OperationsError> {
    let row = connection
        .query_row(
            "SELECT substr(migration_id,1,17),substr(workspace_id,1,17),substr(destination_id,1,17),source_revision,substr(verified_digest,1,33),page_count,record_count,verified_pages,issuer_id,backend_id,sealed_at_ms FROM ops_import_handoff_seals WHERE migration_id=?1",
            params![migration_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, i64>(10)?,
                ))
            },
        )
        .optional()?;
    row.map(decode_seal).transpose()
}

type RawSeal = (
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    i64,
    Vec<u8>,
    i64,
    i64,
    i64,
    String,
    String,
    i64,
);

fn decode_seal(row: RawSeal) -> Result<ImportHandoffSeal, OperationsError> {
    let (
        migration,
        workspace,
        destination,
        source_revision,
        digest,
        pages,
        records,
        verified,
        issuer,
        backend,
        sealed_at_ms,
    ) = row;
    let issuer_id = OrchestrationIssuerId::from_str(&issuer)
        .map_err(|_| OperationsError::Corrupt("handoff seal issuer"))?;
    let backend_id = ForgeBackendId::from_str(&backend)
        .map_err(|_| OperationsError::Corrupt("handoff seal backend"))?;
    if issuer_id.to_string() != issuer || backend_id.to_string() != backend || sealed_at_ms < 0 {
        return Err(OperationsError::Corrupt("handoff seal identity"));
    }
    Ok(ImportHandoffSeal {
        migration_id: decode_uuid(&migration, "handoff migration")?,
        workspace_id: WorkspaceId::from_uuid(decode_uuid(&workspace, "handoff workspace")?),
        destination_id: decode_uuid(&destination, "handoff destination")?,
        source_revision: unsigned(source_revision, "handoff source revision")?,
        verified_digest: digest
            .try_into()
            .map_err(|_| OperationsError::Corrupt("handoff digest length"))?,
        page_count: unsigned(pages, "handoff page count")?,
        record_count: unsigned(records, "handoff record count")?,
        verified_pages: unsigned(verified, "handoff verified page count")?,
        issuer_id,
        backend_id,
        sealed_at_ms,
    })
}

fn decode_uuid(bytes: &[u8], field: &'static str) -> Result<Uuid, OperationsError> {
    Uuid::from_slice(bytes).map_err(|_| OperationsError::Corrupt(field))
}

fn unsigned(value: i64, field: &'static str) -> Result<u64, OperationsError> {
    u64::try_from(value).map_err(|_| OperationsError::Corrupt(field))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ConfigureAdmissionIssuerCommand, ImportPhase, OperationsCatalog, catalog::VERSION,
    };
    use forge_domain::{ForgeBackendId, OrchestrationIssuerId};
    use std::{error::Error, fs, path::PathBuf};

    struct Fixture {
        root: PathBuf,
        path: PathBuf,
        migration_id: Uuid,
        workspace_id: Uuid,
        destination_id: Uuid,
        digest: [u8; 32],
    }

    impl Fixture {
        fn new(name: &str) -> Result<(Self, OperationsCatalog), Box<dyn Error>> {
            let root = std::env::temp_dir().join(format!(
                "operations-import-handoff-{name}-{}",
                Uuid::now_v7()
            ));
            fs::create_dir_all(&root)?;
            let path = root.join("operations.sqlite3");
            let destination_id = Uuid::from_u128(0xA510);
            let workspace_id = Uuid::from_u128(0xA520);
            let migration_id = Uuid::from_u128(0xA530);
            let digest = [0x5A; 32];
            let mut catalog = OperationsCatalog::open(&path, destination_id)?;
            let _configured = catalog.ensure_admission_issuer(ConfigureAdmissionIssuerCommand {
                issuer: OrchestrationIssuerId::from_uuid(Uuid::from_u128(0xA540)),
                backend_id: ForgeBackendId::from_uuid(Uuid::from_u128(0xA550)),
                admission_ttl_ms: crate::MIN_ADMISSION_TTL_MS,
                now_ms: 1,
            })?;
            let fixture = Self {
                root,
                path,
                migration_id,
                workspace_id,
                destination_id,
                digest,
            };
            fixture.insert_verified_import(&mut catalog)?;
            Ok((fixture, catalog))
        }

        fn insert_verified_import(
            &self,
            catalog: &mut OperationsCatalog,
        ) -> Result<(), Box<dyn Error>> {
            catalog.connection.execute(
                "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
                params![
                    self.migration_id.as_bytes(),
                    self.workspace_id.as_bytes(),
                    self.destination_id.as_bytes(),
                    self.digest.as_slice(),
                ],
            )?;
            for ordinal in 0..5_i64 {
                let cursor = [u8::try_from(ordinal + 1)?];
                catalog.connection.execute(
                    "INSERT INTO ops_import_pages(migration_id,ordinal,cursor,next_cursor,table_kind,record_count,digest) VALUES(?1,?2,?3,NULL,?2,0,?4)",
                    params![
                        self.migration_id.as_bytes(),
                        ordinal,
                        cursor.as_slice(),
                        self.digest.as_slice(),
                    ],
                )?;
            }
            Ok(())
        }

        fn command(&self, sealed_at_ms: i64) -> ImportHandoffSealCommand {
            ImportHandoffSealCommand {
                migration_id: self.migration_id,
                workspace_id: self.workspace_id,
                destination_id: self.destination_id,
                expected_source_revision: 1,
                expected_verified_digest: self.digest,
                sealed_at_ms,
            }
        }

        fn cleanup(self) -> Result<(), Box<dyn Error>> {
            fs::remove_dir_all(self.root)?;
            Ok(())
        }
    }

    #[test]
    fn abort_and_seal_serialize_in_both_orders() -> Result<(), Box<dyn Error>> {
        let (fixture, mut catalog) = Fixture::new("abort-first")?;
        assert!(matches!(
            catalog.abort_import(fixture.migration_id),
            Ok(progress) if progress.phase() == ImportPhase::Aborted
        ));
        assert!(matches!(
            catalog.seal_import_handoff(&fixture.command(2)),
            Err(OperationsError::Conflict("import not exactly verified"))
        ));
        assert!(
            catalog
                .import_handoff_seal(
                    fixture.migration_id,
                    fixture.workspace_id,
                    fixture.destination_id
                )?
                .is_none()
        );
        fixture.cleanup()?;

        let (fixture, mut catalog) = Fixture::new("seal-first")?;
        let seal = catalog.seal_import_handoff(&fixture.command(2))?;
        assert_eq!(seal.sealed_at_ms(), 2);
        assert!(matches!(
            catalog.abort_import(fixture.migration_id),
            Err(OperationsError::Conflict("import handoff sealed"))
        ));
        let phase: i64 = catalog.connection.query_row(
            "SELECT phase FROM ops_imports WHERE migration_id=?1",
            params![fixture.migration_id.as_bytes()],
            |row| row.get(0),
        )?;
        assert_eq!(phase, 2);
        assert!(
            catalog
                .connection
                .execute(
                    "UPDATE ops_imports SET phase=3 WHERE migration_id=?1",
                    params![fixture.migration_id.as_bytes()]
                )
                .is_err()
        );
        assert!(
            catalog
                .connection
                .execute(
                    "DELETE FROM ops_import_handoff_seals WHERE migration_id=?1",
                    params![fixture.migration_id.as_bytes()]
                )
                .is_err()
        );
        fixture.cleanup()?;
        Ok(())
    }

    #[test]
    fn exact_replay_returns_original_proof_and_changed_expectations_conflict()
    -> Result<(), Box<dyn Error>> {
        let (fixture, mut catalog) = Fixture::new("replay")?;
        let first = catalog.seal_import_handoff(&fixture.command(2))?;
        let replay = catalog.seal_import_handoff(&fixture.command(99))?;
        assert_eq!(replay, first);

        let mut changed_revision = fixture.command(99);
        changed_revision.expected_source_revision = 2;
        assert!(matches!(
            catalog.seal_import_handoff(&changed_revision),
            Err(OperationsError::Conflict("handoff seal replay"))
        ));
        let mut changed_digest = fixture.command(99);
        changed_digest.expected_verified_digest[0] ^= 1;
        assert!(matches!(
            catalog.seal_import_handoff(&changed_digest),
            Err(OperationsError::Conflict("handoff seal replay"))
        ));
        fixture.cleanup()?;
        Ok(())
    }

    #[test]
    fn seal_is_persistent_exact_and_exposed_by_current_workspace_query()
    -> Result<(), Box<dyn Error>> {
        let (fixture, mut catalog) = Fixture::new("readback")?;
        let first = catalog.seal_import_handoff(&fixture.command(2))?;
        let seal = catalog
            .import_handoff_seal(
                fixture.migration_id,
                fixture.workspace_id,
                fixture.destination_id,
            )?
            .ok_or("seal missing")?;
        assert_eq!(seal, first);
        let current =
            catalog.current_import_handoff(fixture.workspace_id, fixture.destination_id)?;
        assert_eq!(current.handoff_seal(), Some(&first));
        assert_eq!(
            current.progress().map(ImportProgress::phase),
            Some(ImportPhase::Verified)
        );
        assert!(matches!(
            catalog.current_import_handoff(fixture.workspace_id, Uuid::from_u128(0xA560)),
            Err(OperationsError::Conflict("current import destination"))
        ));
        assert!(matches!(
            catalog.import_handoff_seal(
                fixture.migration_id,
                Uuid::from_u128(0xA561),
                fixture.destination_id
            ),
            Err(OperationsError::Conflict("handoff import target"))
        ));
        drop(catalog);
        let reopened =
            OperationsCatalog::open_existing_read_only(&fixture.path, fixture.destination_id)?;
        let reopened_seal = reopened
            .import_handoff_seal(
                fixture.migration_id,
                fixture.workspace_id,
                fixture.destination_id,
            )?
            .ok_or("reopened seal missing")?;
        assert_eq!(reopened_seal, first);
        drop(reopened);
        fixture.cleanup()?;
        Ok(())
    }

    #[test]
    fn fresh_catalog_has_additive_v52_schema() -> Result<(), Box<dyn Error>> {
        let (fixture, catalog) = Fixture::new("schema")?;
        assert_eq!(
            catalog
                .connection
                .pragma_query_value::<i64, _>(None, "user_version", |row| row.get(0))?,
            VERSION
        );
        assert_eq!(VERSION, 52);
        let object_count: i64 = catalog.connection.query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE name LIKE 'ops_import_handoff_%' OR name LIKE 'ops_import_handoff_sealed_import_%'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(object_count, 6);
        drop(catalog);
        fixture.cleanup()?;
        Ok(())
    }

    #[test]
    fn catalog_v51_upgrades_additively_to_v52() -> Result<(), Box<dyn Error>> {
        let (fixture, initial) = Fixture::new("v51-upgrade")?;
        let path = fixture.path.clone();
        let destination = fixture.destination_id;
        drop(initial);

        let connection = Connection::open(&path)?;
        drop_schema_for_legacy_fixture(&connection)?;
        connection.pragma_update(None, "user_version", 51_i64)?;
        drop(connection);

        let upgraded = OperationsCatalog::open(&path, destination)?;
        assert_eq!(
            upgraded
                .connection
                .pragma_query_value::<i64, _>(None, "user_version", |row| row.get(0))?,
            VERSION
        );
        let progress = upgraded
            .progress(fixture.migration_id)?
            .ok_or("verified v51 import history missing")?;
        assert_eq!(progress.phase(), ImportPhase::Verified);
        assert_eq!(progress.page_count(), 5);
        assert_eq!(progress.digest(), fixture.digest);
        let issuer = upgraded
            .admission_issuer_config()?
            .ok_or("v51 issuer configuration missing")?;
        assert_eq!(issuer.issuer().into_uuid(), Uuid::from_u128(0xA540));
        assert_eq!(issuer.backend_id().into_uuid(), Uuid::from_u128(0xA550));
        let seals: i64 = upgraded.connection.query_row(
            "SELECT COUNT(*) FROM ops_import_handoff_seals",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(seals, 0);
        let foreign_key_violation: Option<String> = upgraded
            .connection
            .query_row("PRAGMA foreign_key_check", [], |row| row.get(0))
            .optional()?;
        assert!(foreign_key_violation.is_none());
        drop(upgraded);
        fixture.cleanup()?;
        Ok(())
    }
}
