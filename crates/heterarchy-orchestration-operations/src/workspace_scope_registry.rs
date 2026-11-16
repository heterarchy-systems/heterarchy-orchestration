//! Explicit fail-closed durable `Workspace` scope registry.
//!
//! Empty registries reject every candidate scope with [`ScopeResolutionError::EmptyRegistry`].
//! Mutations advance both `revision` and monotonic `generation` across unregister and
//! re-register transitions so stale callers holding a retired generation are fenced out.

pub(crate) mod schema;
mod sql;

use std::collections::BTreeSet;

use forge_domain::WorkspaceId;
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::sqlite_codecs::{
    decode_non_nil_uuid, decode_positive_u64, derive_deterministic_uuid, finalize_operation_commit,
};
use crate::{
    MAX_WORKSPACE_AUTHORITY_SCOPES, OperationsCatalog, OperationsError, WorkspaceProjectScope,
};

const REGISTER_DIGEST_DOMAIN: &[u8] =
    b"heterarchy.orchestration.workspace_scope_registry.register.v1\0";
const UNREGISTER_DIGEST_DOMAIN: &[u8] =
    b"heterarchy.orchestration.workspace_scope_registry.unregister.v1\0";
const SEED_REQUEST_DOMAIN: &[u8] = b"heterarchy.orchestration.workspace_scope_registry.seed.v1\0";

use sql::{
    COUNT_ACTIVE_WORKSPACE_SCOPES_SQL, INSERT_WORKSPACE_SCOPE_MUTATION_SQL,
    INSERT_WORKSPACE_SCOPE_SQL, SELECT_ACTIVE_WORKSPACE_SCOPES_SQL,
    SELECT_WORKSPACE_SCOPE_BY_ID_SQL, SELECT_WORKSPACE_SCOPE_MUTATION_SQL,
    UPDATE_WORKSPACE_SCOPE_ACTIVE_SQL, UPDATE_WORKSPACE_SCOPE_UNREGISTERED_SQL,
};

/// Fail-closed domain denial returned when resolving a candidate `WorkspaceProjectScope`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ScopeResolutionError {
    #[error("Workspace scope registry has zero active scopes")]
    EmptyRegistry,
    #[error("Workspace {workspace_id} is not actively registered")]
    Unregistered { workspace_id: WorkspaceId },
    #[error(
        "candidate Workspace {workspace_id} scope does not match registered generation {registered_generation}"
    )]
    StaleScope {
        workspace_id: WorkspaceId,
        registered_generation: u64,
    },
    #[error(
        "Workspace scope generation fence mismatch: expected {expected_generation}, actual {actual_generation}"
    )]
    GenerationFenceMismatch {
        expected_generation: u64,
        actual_generation: u64,
    },
}

/// Durable readback or mutation receipt for one registered `WorkspaceProjectScope`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredWorkspaceScopeRecord {
    scope: WorkspaceProjectScope,
    active: bool,
    revision: u64,
    generation: u64,
    updated_at_ms: i64,
    replayed: bool,
}

impl RegisteredWorkspaceScopeRecord {
    #[must_use]
    pub const fn scope(&self) -> &WorkspaceProjectScope {
        &self.scope
    }

    #[must_use]
    pub const fn active(&self) -> bool {
        self.active
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn updated_at_ms(&self) -> i64 {
        self.updated_at_ms
    }

    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Compare-and-swap command to register or update an active `WorkspaceProjectScope`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeRegistrationCommand {
    pub request_id: Uuid,
    pub expected_revision: u64,
    pub scope: WorkspaceProjectScope,
    pub observed_at_ms: i64,
}

impl ScopeRegistrationCommand {
    /// Construct a validated registration command.
    ///
    /// # Errors
    /// Rejects nil request IDs, out-of-range revisions, invalid scopes, and negative timestamps.
    pub fn new(
        request_id: Uuid,
        expected_revision: u64,
        scope: WorkspaceProjectScope,
        observed_at_ms: i64,
    ) -> Result<Self, OperationsError> {
        let command = Self {
            request_id,
            expected_revision,
            scope,
            observed_at_ms,
        };
        command.validate()?;
        Ok(command)
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn expected_revision(&self) -> u64 {
        self.expected_revision
    }

    #[must_use]
    pub const fn scope(&self) -> &WorkspaceProjectScope {
        &self.scope
    }

    #[must_use]
    pub const fn observed_at_ms(&self) -> i64 {
        self.observed_at_ms
    }

    fn validate(&self) -> Result<(), OperationsError> {
        if self.request_id.is_nil() {
            return Err(OperationsError::Bounds(
                "workspace scope registration request",
            ));
        }
        if self.observed_at_ms < 0 {
            return Err(OperationsError::Bounds(
                "workspace scope observed timestamp",
            ));
        }
        crate::catalog::signed(self.expected_revision)?;
        self.scope.validate()?;
        Ok(())
    }
}

/// Compare-and-swap command to mark an active `Workspace` scope as unregistered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScopeUnregisterCommand {
    pub request_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub expected_revision: u64,
    pub observed_at_ms: i64,
}

impl ScopeUnregisterCommand {
    /// Construct a validated unregistration command.
    ///
    /// # Errors
    /// Rejects nil identities, zero or out-of-range revisions, and negative timestamps.
    pub fn new(
        request_id: Uuid,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        observed_at_ms: i64,
    ) -> Result<Self, OperationsError> {
        let command = Self {
            request_id,
            workspace_id,
            expected_revision,
            observed_at_ms,
        };
        command.validate()?;
        Ok(command)
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn expected_revision(&self) -> u64 {
        self.expected_revision
    }

    #[must_use]
    pub const fn observed_at_ms(&self) -> i64 {
        self.observed_at_ms
    }

    fn validate(&self) -> Result<(), OperationsError> {
        self.validate_identity_and_timestamp()?;
        if self.expected_revision == 0 {
            return Err(OperationsError::Conflict(
                "workspace scope unregister requires positive revision",
            ));
        }
        Ok(())
    }

    fn validate_identity_and_timestamp(&self) -> Result<(), OperationsError> {
        if self.request_id.is_nil() || self.workspace_id.into_uuid().is_nil() {
            return Err(OperationsError::Bounds(
                "workspace scope unregister identity",
            ));
        }
        if self.observed_at_ms < 0 {
            return Err(OperationsError::Bounds(
                "workspace scope observed timestamp",
            ));
        }
        crate::catalog::signed(self.expected_revision)?;
        Ok(())
    }
}

impl OperationsCatalog {
    /// Register a new `WorkspaceProjectScope` or update an existing scope's revision and generation.
    ///
    /// # Errors
    /// Rejects invalid inputs, conflicting `request_id` reuse, stale `expected_revision`,
    /// active-scope cap overflow, and storage or unconfirmed-commit failures.
    pub fn register_workspace_scope(
        &mut self,
        command: &ScopeRegistrationCommand,
    ) -> Result<RegisteredWorkspaceScopeRecord, OperationsError> {
        command.validate()?;
        let digest = register_digest(command.expected_revision, &command.scope);
        let workspace_id = WorkspaceId::from_uuid(command.scope.workspace_id());
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(replayed) =
            load_register_replay(&tx, command.request_id, &command.scope, digest)?
        {
            tx.commit()?;
            return Ok(replayed);
        }
        let current = load_registry_row(&tx, command.scope.workspace_id())?;
        let adds_active = current.as_ref().is_none_or(|row| !row.active);
        if adds_active && count_active_scopes(&tx)? >= MAX_WORKSPACE_AUTHORITY_SCOPES {
            return Err(OperationsError::Bounds(
                "maximum active Workspace scopes exceeded",
            ));
        }
        let (next_revision, next_generation) =
            next_register_counters(current.as_ref(), command.expected_revision)?;
        persist_registered_scope(
            &tx,
            &command.scope,
            current.is_some(),
            command.expected_revision,
            next_revision,
            next_generation,
            command.observed_at_ms,
        )?;
        insert_mutation(
            &tx,
            &MutationRowInput {
                request_id: command.request_id,
                workspace_id: command.scope.workspace_id(),
                kind: "REGISTER",
                expected_revision: command.expected_revision,
                resulting_revision: next_revision,
                resulting_generation: next_generation,
                payload_digest: digest,
                recorded_at_ms: command.observed_at_ms,
            },
        )?;
        let fresh = RegisteredWorkspaceScopeRecord {
            scope: command.scope.clone(),
            active: true,
            revision: next_revision,
            generation: next_generation,
            updated_at_ms: command.observed_at_ms,
            replayed: false,
        };
        finalize_operation_commit(tx.commit(), fresh, workspace_id, command.request_id, || {
            load_register_replay(&self.connection, command.request_id, &command.scope, digest)
        })
    }

    /// Mark an active `Workspace` scope as unregistered while advancing monotonic generation.
    ///
    /// # Errors
    /// Rejects invalid inputs, conflicting `request_id` reuse, missing or inactive scopes,
    /// stale revisions, and storage or unconfirmed-commit failures.
    pub fn unregister_workspace_scope(
        &mut self,
        command: &ScopeUnregisterCommand,
    ) -> Result<RegisteredWorkspaceScopeRecord, OperationsError> {
        command.validate_identity_and_timestamp()?;
        let digest = unregister_digest(command);
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(replayed) = load_unregister_replay(&tx, command, digest)? {
            tx.commit()?;
            return Ok(replayed);
        }
        if command.expected_revision == 0 {
            return Err(OperationsError::Conflict(
                "workspace scope unregister requires positive revision",
            ));
        }
        let current = load_registry_row(&tx, command.workspace_id.into_uuid())?
            .ok_or(OperationsError::Conflict("workspace scope is not active"))?;
        if !current.active {
            return Err(OperationsError::Conflict("workspace scope is not active"));
        }
        if current.revision != command.expected_revision {
            return Err(OperationsError::Conflict(
                "workspace scope revision mismatch",
            ));
        }
        let next_revision = increment_u64(current.revision, "workspace scope revision")?;
        let next_generation = increment_u64(current.generation, "workspace scope generation")?;
        let updated = tx.execute(
            UPDATE_WORKSPACE_SCOPE_UNREGISTERED_SQL,
            params![
                command.workspace_id.into_uuid().as_bytes(),
                crate::catalog::signed(next_revision)?,
                crate::catalog::signed(next_generation)?,
                command.observed_at_ms,
                crate::catalog::signed(command.expected_revision)?,
            ],
        )?;
        if updated != 1 {
            return Err(OperationsError::Conflict(
                "workspace scope revision mismatch",
            ));
        }
        insert_mutation(
            &tx,
            &MutationRowInput {
                request_id: command.request_id,
                workspace_id: command.workspace_id.into_uuid(),
                kind: "UNREGISTER",
                expected_revision: command.expected_revision,
                resulting_revision: next_revision,
                resulting_generation: next_generation,
                payload_digest: digest,
                recorded_at_ms: command.observed_at_ms,
            },
        )?;
        let fresh = RegisteredWorkspaceScopeRecord {
            scope: current.scope,
            active: false,
            revision: next_revision,
            generation: next_generation,
            updated_at_ms: command.observed_at_ms,
            replayed: false,
        };
        finalize_operation_commit(
            tx.commit(),
            fresh,
            command.workspace_id,
            command.request_id,
            || load_unregister_replay(&self.connection, command, digest),
        )
    }

    /// Seed initial `'ACTIVE'` rows for configured startup scopes only when absent.
    ///
    /// Existing `'ACTIVE'` or `'UNREGISTERED'` rows are left untouched.
    ///
    /// # Errors
    /// Rejects negative timestamps, duplicate `workspace_id` entries in `scopes`,
    /// active-scope cap overflow, and storage or unconfirmed-commit failures.
    pub fn seed_workspace_scopes_if_absent(
        &mut self,
        scopes: &[WorkspaceProjectScope],
        observed_at_ms: i64,
    ) -> Result<Vec<RegisteredWorkspaceScopeRecord>, OperationsError> {
        if observed_at_ms < 0 {
            return Err(OperationsError::Bounds(
                "workspace scope observed timestamp",
            ));
        }
        if scopes.len() > MAX_WORKSPACE_AUTHORITY_SCOPES {
            return Err(OperationsError::Bounds(
                "maximum active Workspace scopes exceeded",
            ));
        }
        validate_seed_batch(scopes)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut active_count = count_active_scopes(&tx)?;
        let mut results = Vec::with_capacity(scopes.len());
        for scope in scopes {
            if let Some(mut existing) = load_registry_row(&tx, scope.workspace_id())? {
                existing.replayed = true;
                results.push(existing);
                continue;
            }
            if active_count >= MAX_WORKSPACE_AUTHORITY_SCOPES {
                return Err(OperationsError::Bounds(
                    "maximum active Workspace scopes exceeded",
                ));
            }
            let request_id = derive_deterministic_uuid(
                SEED_REQUEST_DOMAIN,
                self.destination,
                scope.workspace_id(),
            );
            persist_registered_scope(&tx, scope, false, 0, 1, 1, observed_at_ms)?;
            insert_mutation(
                &tx,
                &MutationRowInput {
                    request_id,
                    workspace_id: scope.workspace_id(),
                    kind: "REGISTER",
                    expected_revision: 0,
                    resulting_revision: 1,
                    resulting_generation: 1,
                    payload_digest: register_digest(0, scope),
                    recorded_at_ms: observed_at_ms,
                },
            )?;
            active_count += 1;
            results.push(RegisteredWorkspaceScopeRecord {
                scope: scope.clone(),
                active: true,
                revision: 1,
                generation: 1,
                updated_at_ms: observed_at_ms,
                replayed: false,
            });
        }
        tx.commit()
            .map_err(|source| OperationsError::CatalogCommitUnconfirmed {
                destination_id: self.destination,
                source,
            })?;
        Ok(results)
    }

    /// Resolve and authorize a candidate `WorkspaceProjectScope` against the durable registry.
    ///
    /// Returns `Ok(Err(ScopeResolutionError))` on fail-closed policy denial and
    /// `Err(OperationsError)` on storage or catalog corruption failures.
    ///
    /// # Errors
    /// Returns [`OperationsError`] when SQLite readback fails or persisted rows are corrupt.
    pub fn resolve_registered_workspace_scope(
        &self,
        candidate: &WorkspaceProjectScope,
        expected_generation: Option<u64>,
    ) -> Result<Result<RegisteredWorkspaceScopeRecord, ScopeResolutionError>, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        let active_count = count_active_scopes(&tx)?;
        if active_count == 0 {
            crate::catalog::commit_read(tx, self.destination)?;
            return Ok(Err(ScopeResolutionError::EmptyRegistry));
        }
        let workspace_id = WorkspaceId::from_uuid(candidate.workspace_id());
        let Some(row) = load_registry_row(&tx, candidate.workspace_id())? else {
            crate::catalog::commit_read(tx, self.destination)?;
            return Ok(Err(ScopeResolutionError::Unregistered { workspace_id }));
        };
        crate::catalog::commit_read(tx, self.destination)?;
        if !row.active {
            return Ok(Err(ScopeResolutionError::Unregistered { workspace_id }));
        }
        if candidate.location_id() != row.scope.location_id()
            || candidate.location_revision() != row.scope.location_revision()
            || candidate.workspace_revision() != row.scope.workspace_revision()
            || candidate.locator() != row.scope.locator()
        {
            return Ok(Err(ScopeResolutionError::StaleScope {
                workspace_id,
                registered_generation: row.generation,
            }));
        }
        if let Some(expected) = expected_generation
            && expected != row.generation
        {
            return Ok(Err(ScopeResolutionError::GenerationFenceMismatch {
                expected_generation: expected,
                actual_generation: row.generation,
            }));
        }
        Ok(Ok(row))
    }

    /// Resolve a candidate `WorkspaceProjectScope` with a mandatory `expected_generation` fence.
    ///
    /// # Errors
    /// Returns [`OperationsError`] when SQLite readback fails or persisted rows are corrupt.
    pub fn resolve_registered_workspace_scope_fenced(
        &self,
        candidate: &WorkspaceProjectScope,
        expected_generation: u64,
    ) -> Result<Result<RegisteredWorkspaceScopeRecord, ScopeResolutionError>, OperationsError> {
        self.resolve_registered_workspace_scope(candidate, Some(expected_generation))
    }

    /// List all currently `'ACTIVE'` registered `Workspace` scopes ordered by `workspace_id`.
    ///
    /// # Errors
    /// Returns [`OperationsError`] when SQLite readback fails or stored state exceeds bounds.
    pub fn list_active_workspace_scopes(
        &self,
    ) -> Result<Vec<RegisteredWorkspaceScopeRecord>, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        let limit = i64::try_from(MAX_WORKSPACE_AUTHORITY_SCOPES)
            .map_err(|_| OperationsError::Bounds("workspace scope query limit"))?
            + 1;
        let mut statement = tx.prepare(SELECT_ACTIVE_WORKSPACE_SCOPES_SQL)?;
        let rows = statement
            .query_map(params![limit], read_raw_registry_row)?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        let records = rows
            .into_iter()
            .map(decode_registry_row)
            .collect::<Result<Vec<_>, _>>()?;
        if records.len() > MAX_WORKSPACE_AUTHORITY_SCOPES
            || records.iter().any(|record| !record.active)
        {
            return Err(OperationsError::Corrupt(
                "active workspace scope inventory bounds",
            ));
        }
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(records)
    }
}

struct MutationRowInput {
    request_id: Uuid,
    workspace_id: Uuid,
    kind: &'static str,
    expected_revision: u64,
    resulting_revision: u64,
    resulting_generation: u64,
    payload_digest: [u8; 32],
    recorded_at_ms: i64,
}

struct StoredMutationRow {
    workspace_id: Uuid,
    kind: String,
    resulting_revision: u64,
    resulting_generation: u64,
    payload_digest: [u8; 32],
    recorded_at_ms: i64,
}

struct RawRegistryRow {
    workspace_id: Vec<u8>,
    state: String,
    location_id: Vec<u8>,
    location_revision: i64,
    workspace_revision: i64,
    locator: String,
    revision: i64,
    generation: i64,
    updated_at_ms: i64,
}

fn validate_seed_batch(scopes: &[WorkspaceProjectScope]) -> Result<(), OperationsError> {
    let mut seen = BTreeSet::new();
    for scope in scopes {
        scope.validate()?;
        if !seen.insert(scope.workspace_id()) {
            return Err(OperationsError::Conflict(
                "duplicate Workspace in seed batch",
            ));
        }
    }
    Ok(())
}

fn register_digest(expected_revision: u64, scope: &WorkspaceProjectScope) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(REGISTER_DIGEST_DOMAIN);
    hasher.update(expected_revision.to_le_bytes());
    hasher.update(scope.workspace_id().as_bytes());
    hasher.update(scope.location_id().as_bytes());
    hasher.update(scope.location_revision().to_le_bytes());
    hasher.update(scope.workspace_revision().to_le_bytes());
    crate::sqlite_codecs::hash_str(&mut hasher, scope.locator());
    hasher.finalize().into()
}

fn unregister_digest(command: &ScopeUnregisterCommand) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(UNREGISTER_DIGEST_DOMAIN);
    hasher.update(command.expected_revision.to_le_bytes());
    hasher.update(command.workspace_id.into_uuid().as_bytes());
    hasher.finalize().into()
}

fn next_register_counters(
    current: Option<&RegisteredWorkspaceScopeRecord>,
    expected_revision: u64,
) -> Result<(u64, u64), OperationsError> {
    match current {
        None => {
            if expected_revision != 0 {
                return Err(OperationsError::Conflict(
                    "initial Workspace scope registration requires expected_revision 0",
                ));
            }
            Ok((1, 1))
        }
        Some(existing) => {
            if existing.revision != expected_revision {
                return Err(OperationsError::Conflict(
                    "workspace scope revision mismatch",
                ));
            }
            Ok((
                increment_u64(existing.revision, "workspace scope revision")?,
                increment_u64(existing.generation, "workspace scope generation")?,
            ))
        }
    }
}

fn increment_u64(value: u64, label: &'static str) -> Result<u64, OperationsError> {
    let next = value.checked_add(1).ok_or(OperationsError::Bounds(label))?;
    crate::catalog::signed(next)?;
    Ok(next)
}

fn persist_registered_scope(
    tx: &Transaction<'_>,
    scope: &WorkspaceProjectScope,
    exists: bool,
    expected_revision: u64,
    next_revision: u64,
    next_generation: u64,
    observed_at_ms: i64,
) -> Result<(), OperationsError> {
    let affected = if exists {
        tx.execute(
            UPDATE_WORKSPACE_SCOPE_ACTIVE_SQL,
            params![
                scope.workspace_id().as_bytes(),
                scope.location_id().as_bytes(),
                scope.location_revision(),
                scope.workspace_revision(),
                scope.locator(),
                crate::catalog::signed(next_revision)?,
                crate::catalog::signed(next_generation)?,
                observed_at_ms,
                crate::catalog::signed(expected_revision)?,
            ],
        )?
    } else {
        tx.execute(
            INSERT_WORKSPACE_SCOPE_SQL,
            params![
                scope.workspace_id().as_bytes(),
                scope.location_id().as_bytes(),
                scope.location_revision(),
                scope.workspace_revision(),
                scope.locator(),
                crate::catalog::signed(next_revision)?,
                crate::catalog::signed(next_generation)?,
                observed_at_ms,
            ],
        )?
    };
    if affected != 1 {
        return Err(OperationsError::Conflict(
            "workspace scope revision mismatch",
        ));
    }
    Ok(())
}

fn insert_mutation(tx: &Transaction<'_>, input: &MutationRowInput) -> Result<(), OperationsError> {
    tx.execute(
        INSERT_WORKSPACE_SCOPE_MUTATION_SQL,
        params![
            input.request_id.as_bytes(),
            input.workspace_id.as_bytes(),
            input.kind,
            crate::catalog::signed(input.expected_revision)?,
            crate::catalog::signed(input.resulting_revision)?,
            crate::catalog::signed(input.resulting_generation)?,
            input.payload_digest.as_slice(),
            input.recorded_at_ms,
        ],
    )?;
    Ok(())
}

fn count_active_scopes(connection: &Connection) -> Result<usize, OperationsError> {
    let count: i64 =
        connection.query_row(COUNT_ACTIVE_WORKSPACE_SCOPES_SQL, [], |row| row.get(0))?;
    usize::try_from(count).map_err(|_| OperationsError::Corrupt("active workspace scope count"))
}

fn load_mutation(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<StoredMutationRow>, OperationsError> {
    let raw = connection
        .query_row(
            SELECT_WORKSPACE_SCOPE_MUTATION_SQL,
            params![request_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            },
        )
        .optional()?;
    raw.map(
        |(workspace_bytes, kind, revision, generation, digest_bytes, recorded_at_ms)| {
            let workspace_id = decode_non_nil_uuid(&workspace_bytes, "mutation workspace_id")?;
            let payload_digest: [u8; 32] = digest_bytes
                .try_into()
                .map_err(|_| OperationsError::Corrupt("mutation payload_digest"))?;
            if payload_digest == [0_u8; 32] || recorded_at_ms < 0 {
                return Err(OperationsError::Corrupt("mutation receipt bounds"));
            }
            Ok(StoredMutationRow {
                workspace_id,
                kind,
                resulting_revision: decode_positive_u64(revision, "mutation resulting_revision")?,
                resulting_generation: decode_positive_u64(
                    generation,
                    "mutation resulting_generation",
                )?,
                payload_digest,
                recorded_at_ms,
            })
        },
    )
    .transpose()
}

fn load_register_replay(
    connection: &Connection,
    request_id: Uuid,
    scope: &WorkspaceProjectScope,
    expected_digest: [u8; 32],
) -> Result<Option<RegisteredWorkspaceScopeRecord>, OperationsError> {
    let Some(mutation) = load_mutation(connection, request_id)? else {
        return Ok(None);
    };
    if mutation.kind != "REGISTER"
        || mutation.workspace_id != scope.workspace_id()
        || mutation.payload_digest != expected_digest
    {
        return Err(OperationsError::Conflict(
            "workspace scope request_id reused with different payload",
        ));
    }
    Ok(Some(RegisteredWorkspaceScopeRecord {
        scope: scope.clone(),
        active: true,
        revision: mutation.resulting_revision,
        generation: mutation.resulting_generation,
        updated_at_ms: mutation.recorded_at_ms,
        replayed: true,
    }))
}

fn load_unregister_replay(
    connection: &Connection,
    command: &ScopeUnregisterCommand,
    expected_digest: [u8; 32],
) -> Result<Option<RegisteredWorkspaceScopeRecord>, OperationsError> {
    let Some(mutation) = load_mutation(connection, command.request_id)? else {
        return Ok(None);
    };
    if mutation.kind != "UNREGISTER"
        || mutation.workspace_id != command.workspace_id.into_uuid()
        || mutation.payload_digest != expected_digest
    {
        return Err(OperationsError::Conflict(
            "workspace scope request_id reused with different payload",
        ));
    }
    let row = load_registry_row(connection, mutation.workspace_id)?.ok_or(
        OperationsError::Corrupt("workspace scope registry row missing for mutation"),
    )?;
    Ok(Some(RegisteredWorkspaceScopeRecord {
        scope: row.scope,
        active: false,
        revision: mutation.resulting_revision,
        generation: mutation.resulting_generation,
        updated_at_ms: mutation.recorded_at_ms,
        replayed: true,
    }))
}

fn load_registry_row(
    connection: &Connection,
    workspace_id: Uuid,
) -> Result<Option<RegisteredWorkspaceScopeRecord>, OperationsError> {
    let raw = connection
        .query_row(
            SELECT_WORKSPACE_SCOPE_BY_ID_SQL,
            params![workspace_id.as_bytes()],
            read_raw_registry_row,
        )
        .optional()?;
    raw.map(decode_registry_row).transpose()
}

fn read_raw_registry_row(row: &Row<'_>) -> rusqlite::Result<RawRegistryRow> {
    Ok(RawRegistryRow {
        workspace_id: row.get(0)?,
        state: row.get(1)?,
        location_id: row.get(2)?,
        location_revision: row.get(3)?,
        workspace_revision: row.get(4)?,
        locator: row.get(5)?,
        revision: row.get(6)?,
        generation: row.get(7)?,
        updated_at_ms: row.get(8)?,
    })
}

fn decode_registry_row(
    raw: RawRegistryRow,
) -> Result<RegisteredWorkspaceScopeRecord, OperationsError> {
    let workspace_id = decode_non_nil_uuid(&raw.workspace_id, "registry workspace_id")?;
    let location_id = decode_non_nil_uuid(&raw.location_id, "registry location_id")?;
    let active = match raw.state.as_str() {
        "ACTIVE" => true,
        "UNREGISTERED" => false,
        _ => return Err(OperationsError::Corrupt("registry scope state")),
    };
    if raw.updated_at_ms < 0 {
        return Err(OperationsError::Corrupt("registry updated_at_ms"));
    }
    let scope = WorkspaceProjectScope::new(
        workspace_id,
        location_id,
        raw.location_revision,
        raw.workspace_revision,
        raw.locator,
    )
    .map_err(|_| OperationsError::Corrupt("registry WorkspaceProjectScope"))?;
    Ok(RegisteredWorkspaceScopeRecord {
        scope,
        active,
        revision: decode_positive_u64(raw.revision, "registry revision")?,
        generation: decode_positive_u64(raw.generation, "registry generation")?,
        updated_at_ms: raw.updated_at_ms,
        replayed: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    use std::path::PathBuf;

    type TestResult = Result<(), Box<dyn Error>>;
    type ScopeResolutionCase<'a> = (
        &'a str,
        WorkspaceProjectScope,
        Option<u64>,
        Result<u64, ScopeResolutionError>,
    );

    const DESTINATION_ID: Uuid = Uuid::from_u128(0x9100);
    const WORKSPACE_ONE: Uuid = Uuid::from_u128(0x9101);
    const WORKSPACE_TWO: Uuid = Uuid::from_u128(0x9102);
    const LOCATION_ONE: Uuid = Uuid::from_u128(0x9201);
    const LOCATION_TWO: Uuid = Uuid::from_u128(0x9202);

    struct TempCatalogDir {
        dir: PathBuf,
    }

    impl TempCatalogDir {
        fn open_catalog() -> Result<(Self, OperationsCatalog), Box<dyn Error>> {
            let dir = std::env::temp_dir()
                .join(format!("workspace-scope-registry-test-{}", Uuid::now_v7()));
            std::fs::create_dir_all(&dir)?;
            let guard = Self { dir };
            let catalog =
                OperationsCatalog::open(&guard.dir.join("operations.sqlite3"), DESTINATION_ID)?;
            Ok((guard, catalog))
        }
    }

    impl Drop for TempCatalogDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    struct ScopeFixtureBuilder {
        workspace_id: Uuid,
        location_id: Uuid,
        location_revision: i64,
        workspace_revision: i64,
        locator: String,
    }

    impl ScopeFixtureBuilder {
        fn new(workspace_id: Uuid) -> Self {
            Self {
                workspace_id,
                location_id: LOCATION_ONE,
                location_revision: 1,
                workspace_revision: 1,
                locator: format!("file:///workspaces/{workspace_id}"),
            }
        }

        fn location_id(mut self, value: Uuid) -> Self {
            self.location_id = value;
            self
        }

        fn location_revision(mut self, value: i64) -> Self {
            self.location_revision = value;
            self
        }

        fn workspace_revision(mut self, value: i64) -> Self {
            self.workspace_revision = value;
            self
        }

        fn locator(mut self, value: &str) -> Self {
            self.locator = value.to_owned();
            self
        }

        fn build(self) -> Result<WorkspaceProjectScope, OperationsError> {
            WorkspaceProjectScope::new(
                self.workspace_id,
                self.location_id,
                self.location_revision,
                self.workspace_revision,
                self.locator,
            )
        }
    }

    #[test]
    fn empty_registry_fails_closed_for_any_candidate_scope() -> TestResult {
        let (_dir, catalog) = TempCatalogDir::open_catalog()?;
        let candidate = ScopeFixtureBuilder::new(WORKSPACE_ONE).build()?;
        let resolution = catalog.resolve_registered_workspace_scope(&candidate, None)?;
        assert_eq!(resolution, Err(ScopeResolutionError::EmptyRegistry));
        assert!(catalog.list_active_workspace_scopes()?.is_empty());
        Ok(())
    }

    #[test]
    fn lifecycle_advances_monotonic_generation_across_unregister_and_reregister() -> TestResult {
        let (_dir, mut catalog) = TempCatalogDir::open_catalog()?;
        let initial_scope = ScopeFixtureBuilder::new(WORKSPACE_ONE).build()?;
        let register_cmd = ScopeRegistrationCommand::new(
            Uuid::from_u128(0xA001),
            0,
            initial_scope.clone(),
            1_000,
        )?;
        let first = catalog.register_workspace_scope(&register_cmd)?;
        assert!(first.active());
        assert_eq!(
            (first.revision(), first.generation(), first.replayed()),
            (1, 1, false)
        );

        let replay = catalog.register_workspace_scope(&register_cmd)?;
        assert!(replay.replayed());
        assert_eq!((replay.revision(), replay.generation()), (1, 1));

        let conflict_cmd = ScopeRegistrationCommand::new(
            Uuid::from_u128(0xA001),
            0,
            ScopeFixtureBuilder::new(WORKSPACE_ONE)
                .workspace_revision(2)
                .build()?,
            1_001,
        )?;
        assert!(matches!(
            catalog.register_workspace_scope(&conflict_cmd),
            Err(OperationsError::Conflict(_))
        ));

        let unregister_cmd = ScopeUnregisterCommand::new(
            Uuid::from_u128(0xA002),
            WorkspaceId::from_uuid(WORKSPACE_ONE),
            1,
            2_000,
        )?;
        let unregistered = catalog.unregister_workspace_scope(&unregister_cmd)?;
        assert!(!unregistered.active());
        assert_eq!((unregistered.revision(), unregistered.generation()), (2, 2));
        assert_eq!(
            catalog.resolve_registered_workspace_scope(&initial_scope, None)?,
            Err(ScopeResolutionError::EmptyRegistry)
        );

        let reregistered_scope = ScopeFixtureBuilder::new(WORKSPACE_ONE)
            .workspace_revision(3)
            .build()?;
        let reregister_cmd = ScopeRegistrationCommand::new(
            Uuid::from_u128(0xA003),
            2,
            reregistered_scope.clone(),
            3_000,
        )?;
        let reregistered = catalog.register_workspace_scope(&reregister_cmd)?;
        assert!(reregistered.active());
        assert_eq!((reregistered.revision(), reregistered.generation()), (3, 3));

        assert_eq!(
            catalog.resolve_registered_workspace_scope(&reregistered_scope, Some(1))?,
            Err(ScopeResolutionError::GenerationFenceMismatch {
                expected_generation: 1,
                actual_generation: 3,
            })
        );
        let authorized =
            catalog.resolve_registered_workspace_scope(&reregistered_scope, Some(3))?;
        assert_eq!(authorized.map(|row| row.generation()), Ok(3));
        Ok(())
    }

    #[test]
    fn resolution_dictionary_distinguishes_unregistered_stale_and_fence_denials() -> TestResult {
        let (_dir, mut catalog) = TempCatalogDir::open_catalog()?;
        let active_one = ScopeFixtureBuilder::new(WORKSPACE_ONE)
            .location_revision(2)
            .workspace_revision(4)
            .locator("file:///ws/one")
            .build()?;
        let active_two = ScopeFixtureBuilder::new(WORKSPACE_TWO).build()?;
        catalog.register_workspace_scope(&ScopeRegistrationCommand::new(
            Uuid::from_u128(0xB001),
            0,
            active_one.clone(),
            100,
        )?)?;
        catalog.register_workspace_scope(&ScopeRegistrationCommand::new(
            Uuid::from_u128(0xB002),
            0,
            active_two,
            101,
        )?)?;
        catalog.unregister_workspace_scope(&ScopeUnregisterCommand::new(
            Uuid::from_u128(0xB003),
            WorkspaceId::from_uuid(WORKSPACE_TWO),
            1,
            102,
        )?)?;

        let cases: &[ScopeResolutionCase<'_>] = &[
            (
                "missing_workspace",
                ScopeFixtureBuilder::new(Uuid::from_u128(0x9999)).build()?,
                None,
                Err(ScopeResolutionError::Unregistered {
                    workspace_id: WorkspaceId::from_uuid(Uuid::from_u128(0x9999)),
                }),
            ),
            (
                "unregistered_workspace",
                ScopeFixtureBuilder::new(WORKSPACE_TWO).build()?,
                None,
                Err(ScopeResolutionError::Unregistered {
                    workspace_id: WorkspaceId::from_uuid(WORKSPACE_TWO),
                }),
            ),
            (
                "stale_location_id",
                ScopeFixtureBuilder::new(WORKSPACE_ONE)
                    .location_id(LOCATION_TWO)
                    .location_revision(2)
                    .workspace_revision(4)
                    .locator("file:///ws/one")
                    .build()?,
                None,
                Err(ScopeResolutionError::StaleScope {
                    workspace_id: WorkspaceId::from_uuid(WORKSPACE_ONE),
                    registered_generation: 1,
                }),
            ),
            (
                "stale_locator",
                ScopeFixtureBuilder::new(WORKSPACE_ONE)
                    .location_revision(2)
                    .workspace_revision(4)
                    .locator("file:///ws/other")
                    .build()?,
                None,
                Err(ScopeResolutionError::StaleScope {
                    workspace_id: WorkspaceId::from_uuid(WORKSPACE_ONE),
                    registered_generation: 1,
                }),
            ),
            (
                "generation_fence_mismatch",
                active_one.clone(),
                Some(9),
                Err(ScopeResolutionError::GenerationFenceMismatch {
                    expected_generation: 9,
                    actual_generation: 1,
                }),
            ),
            ("exact_match", active_one, Some(1), Ok(1)),
        ];

        for (label, candidate, fence, expected) in cases {
            let outcome = catalog
                .resolve_registered_workspace_scope(candidate, *fence)?
                .map(|record| record.generation());
            assert_eq!(&outcome, expected, "case {label} failed");
        }
        Ok(())
    }

    #[test]
    fn seed_if_absent_populates_missing_and_preserves_unregistered_rows() -> TestResult {
        let (_dir, mut catalog) = TempCatalogDir::open_catalog()?;
        let ws_one = ScopeFixtureBuilder::new(WORKSPACE_ONE).build()?;
        let ws_two = ScopeFixtureBuilder::new(WORKSPACE_TWO).build()?;

        catalog.register_workspace_scope(&ScopeRegistrationCommand::new(
            Uuid::from_u128(0xC001),
            0,
            ws_one.clone(),
            10,
        )?)?;
        catalog.unregister_workspace_scope(&ScopeUnregisterCommand::new(
            Uuid::from_u128(0xC002),
            WorkspaceId::from_uuid(WORKSPACE_ONE),
            1,
            20,
        )?)?;

        let modified_one = ScopeFixtureBuilder::new(WORKSPACE_ONE)
            .workspace_revision(99)
            .build()?;
        let seeded =
            catalog.seed_workspace_scopes_if_absent(&[modified_one, ws_two.clone()], 30)?;
        assert_eq!(seeded.len(), 2);
        assert!(!seeded[0].active());
        assert_eq!(
            (
                seeded[0].revision(),
                seeded[0].generation(),
                seeded[0].replayed()
            ),
            (2, 2, true)
        );
        assert!(seeded[1].active());
        assert_eq!(
            (
                seeded[1].revision(),
                seeded[1].generation(),
                seeded[1].replayed()
            ),
            (1, 1, false)
        );

        let active = catalog.list_active_workspace_scopes()?;
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].scope(), &ws_two);
        Ok(())
    }

    #[test]
    fn active_scope_cap_is_enforced_while_allowing_in_place_active_updates() -> TestResult {
        let (_dir, mut catalog) = TempCatalogDir::open_catalog()?;
        for index in 0..MAX_WORKSPACE_AUTHORITY_SCOPES {
            let ws_id = Uuid::from_u128(0xD000 + u128::try_from(index)?);
            let req_id = Uuid::from_u128(0xE000 + u128::try_from(index)?);
            let scope = ScopeFixtureBuilder::new(ws_id).build()?;
            catalog
                .register_workspace_scope(&ScopeRegistrationCommand::new(req_id, 0, scope, 50)?)?;
        }
        let overflow_scope = ScopeFixtureBuilder::new(Uuid::from_u128(0xFFFF)).build()?;
        assert!(matches!(
            catalog.register_workspace_scope(&ScopeRegistrationCommand::new(
                Uuid::from_u128(0xF001),
                0,
                overflow_scope,
                51,
            )?),
            Err(OperationsError::Bounds(_))
        ));

        let updated_first = ScopeFixtureBuilder::new(Uuid::from_u128(0xD000))
            .workspace_revision(2)
            .build()?;
        let updated = catalog.register_workspace_scope(&ScopeRegistrationCommand::new(
            Uuid::from_u128(0xF002),
            1,
            updated_first,
            52,
        )?)?;
        assert_eq!((updated.revision(), updated.generation()), (2, 2));
        Ok(())
    }
}
