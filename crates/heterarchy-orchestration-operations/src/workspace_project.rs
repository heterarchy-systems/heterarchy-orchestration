//! Durable Workspace to `ChatGPT` Project bindings and immutable readback receipts.

use forge_domain::TimestampMillis;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::{collections::BTreeSet, fmt, str::FromStr};
use uuid::Uuid;

use crate::{OperationsCatalog, OperationsError, wire};

const PROJECT_ID_PREFIX: &str = "g-p-";
const PROJECT_ID_HEX_BYTES: usize = 32;
const MAX_LOCATOR_BYTES: usize = 4096;
pub const MAX_WORKSPACE_AUTHORITY_SCOPES: usize = 64;

const BINDINGS_SQL: &str = r"
CREATE TABLE ops_workspace_chatgpt_project_bindings (
    workspace_id BLOB PRIMARY KEY NOT NULL CHECK(length(workspace_id)=16 AND workspace_id != zeroblob(16)),
    project_id TEXT NOT NULL CHECK(length(CAST(project_id AS BLOB))=36 AND substr(project_id,1,4)='g-p-' AND substr(project_id,5,32) NOT GLOB '*[^0-9a-f]*'),
    revision INTEGER NOT NULL CHECK(revision>=1),
    configured_at_ms INTEGER NOT NULL CHECK(configured_at_ms>=0)
);
";

const RECEIPTS_SQL: &str = r"
CREATE TABLE ops_workspace_chatgpt_project_receipts (
    request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)),
    workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id != zeroblob(16)),
    location_id BLOB NOT NULL CHECK(length(location_id)=16 AND location_id != zeroblob(16)),
    location_revision INTEGER NOT NULL CHECK(location_revision>=1),
    workspace_revision INTEGER NOT NULL CHECK(workspace_revision>=1),
    locator TEXT NOT NULL CHECK(length(CAST(locator AS BLOB)) BETWEEN 1 AND 4096),
    expected_revision INTEGER CHECK(expected_revision IS NULL OR expected_revision>=1),
    project_id TEXT NOT NULL CHECK(length(CAST(project_id AS BLOB))=36 AND substr(project_id,1,4)='g-p-' AND substr(project_id,5,32) NOT GLOB '*[^0-9a-f]*'),
    binding_revision INTEGER NOT NULL CHECK(binding_revision>=1),
    configured_at_ms INTEGER NOT NULL CHECK(configured_at_ms>=0)
);
";

const WORK_SCOPES_SQL: &str = r"
CREATE TABLE ops_workspace_chatgpt_project_work_scopes (
    workspace_id BLOB PRIMARY KEY NOT NULL CHECK(length(workspace_id)=16 AND workspace_id != zeroblob(16)),
    location_id BLOB NOT NULL CHECK(length(location_id)=16 AND location_id != zeroblob(16)),
    location_revision INTEGER NOT NULL CHECK(location_revision>=1),
    workspace_revision INTEGER NOT NULL CHECK(workspace_revision>=1),
    locator TEXT NOT NULL CHECK(length(CAST(locator AS BLOB)) BETWEEN 1 AND 4096),
    binding_revision INTEGER NOT NULL CHECK(binding_revision>=1)
) STRICT;
";

const RECEIPT_NO_UPDATE_TRIGGER: &str = r"
CREATE TRIGGER ops_workspace_chatgpt_project_receipts_no_update
BEFORE UPDATE ON ops_workspace_chatgpt_project_receipts
BEGIN
    SELECT RAISE(ABORT, 'ChatGPT Project binding receipts are immutable');
END;
";

const RECEIPT_NO_DELETE_TRIGGER: &str = r"
CREATE TRIGGER ops_workspace_chatgpt_project_receipts_no_delete
BEFORE DELETE ON ops_workspace_chatgpt_project_receipts
BEGIN
    SELECT RAISE(ABORT, 'ChatGPT Project binding receipts are immutable');
END;
";

/// Validated stable `ChatGPT` Project identity observed in the product URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChatGptProjectId(u128);

impl ChatGptProjectId {
    /// Return the canonical provider identity.
    #[must_use]
    pub const fn as_u128(self) -> u128 {
        self.0
    }
}

impl TryFrom<&str> for ChatGptProjectId {
    type Error = OperationsError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let suffix = value
            .strip_prefix(PROJECT_ID_PREFIX)
            .ok_or(OperationsError::Conflict("ChatGPT Project identity"))?;
        if suffix.len() != PROJECT_ID_HEX_BYTES
            || !suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(OperationsError::Conflict("ChatGPT Project identity"));
        }
        let parsed = u128::from_str_radix(suffix, 16)
            .map_err(|_| OperationsError::Conflict("ChatGPT Project identity"))?;
        Ok(Self(parsed))
    }
}

impl FromStr for ChatGptProjectId {
    type Err = OperationsError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value)
    }
}

impl fmt::Display for ChatGptProjectId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{PROJECT_ID_PREFIX}{:032x}", self.0)
    }
}

/// Canonical positive signed-64-bit Workspace Project binding revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkspaceProjectBindingRevision(i64);

impl WorkspaceProjectBindingRevision {
    /// Construct one positive, SQLite-compatible revision.
    ///
    /// # Errors
    /// Rejects zero and negative values.
    pub fn new(value: i64) -> Result<Self, OperationsError> {
        if value <= 0 {
            return Err(OperationsError::Conflict(
                "ChatGPT Project binding revision",
            ));
        }
        Ok(Self(value))
    }

    /// The SQLite signed-64-bit representation.
    #[must_use]
    pub const fn as_i64(self) -> i64 {
        self.0
    }
}

impl FromStr for WorkspaceProjectBindingRevision {
    type Err = OperationsError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let parsed = value
            .parse::<i64>()
            .map_err(|_| OperationsError::Conflict("ChatGPT Project binding revision"))?;
        let revision = Self::new(parsed)?;
        if revision.to_string() != value {
            return Err(OperationsError::Conflict(
                "ChatGPT Project binding revision",
            ));
        }
        Ok(revision)
    }
}

impl fmt::Display for WorkspaceProjectBindingRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Immutable Project identity and binding revision frozen onto a route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkspaceProjectReference {
    project_id: ChatGptProjectId,
    binding_revision: WorkspaceProjectBindingRevision,
}

impl WorkspaceProjectReference {
    /// Construct one checked frozen route value.
    #[must_use]
    pub const fn new(
        project_id: ChatGptProjectId,
        binding_revision: WorkspaceProjectBindingRevision,
    ) -> Self {
        Self {
            project_id,
            binding_revision,
        }
    }

    #[must_use]
    pub const fn project_id(&self) -> ChatGptProjectId {
        self.project_id
    }

    #[must_use]
    pub const fn binding_revision(&self) -> WorkspaceProjectBindingRevision {
        self.binding_revision
    }
}

/// Canonical Forge Workspace scope retained in a configure receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceProjectScope {
    workspace_id: Uuid,
    location_id: Uuid,
    location_revision: i64,
    workspace_revision: i64,
    locator: String,
}

impl WorkspaceProjectScope {
    /// Validate and normalize one exact `WorkspaceQueryScope`.
    ///
    /// # Errors
    /// Rejects nil identities, non-positive revisions, and oversized/empty locators.
    pub fn new(
        workspace_id: Uuid,
        location_id: Uuid,
        location_revision: i64,
        workspace_revision: i64,
        locator: String,
    ) -> Result<Self, OperationsError> {
        if workspace_id.is_nil()
            || location_id.is_nil()
            || location_revision <= 0
            || workspace_revision <= 0
            || locator.is_empty()
            || locator.len() > MAX_LOCATOR_BYTES
        {
            return Err(OperationsError::Conflict("ChatGPT Project Workspace scope"));
        }
        Ok(Self {
            workspace_id,
            location_id,
            location_revision,
            workspace_revision,
            locator,
        })
    }

    #[must_use]
    pub const fn workspace_id(&self) -> Uuid {
        self.workspace_id
    }

    #[must_use]
    pub const fn location_id(&self) -> Uuid {
        self.location_id
    }

    #[must_use]
    pub const fn location_revision(&self) -> i64 {
        self.location_revision
    }

    #[must_use]
    pub const fn workspace_revision(&self) -> i64 {
        self.workspace_revision
    }

    #[must_use]
    pub fn locator(&self) -> &str {
        &self.locator
    }
}

/// Current Workspace-to-Project binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceProjectBinding {
    workspace_id: Uuid,
    project_id: ChatGptProjectId,
    revision: WorkspaceProjectBindingRevision,
    configured_at: TimestampMillis,
}

impl WorkspaceProjectBinding {
    #[must_use]
    pub const fn workspace_id(&self) -> Uuid {
        self.workspace_id
    }

    #[must_use]
    pub const fn project_id(&self) -> ChatGptProjectId {
        self.project_id
    }

    #[must_use]
    pub const fn revision(&self) -> WorkspaceProjectBindingRevision {
        self.revision
    }

    #[must_use]
    pub const fn configured_at(&self) -> TimestampMillis {
        self.configured_at
    }

    #[must_use]
    pub fn project_reference(&self) -> WorkspaceProjectReference {
        WorkspaceProjectReference::new(self.project_id, self.revision)
    }
}

/// Immutable receipt for one exact configure request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceProjectBindingReceipt {
    request_id: Uuid,
    scope: WorkspaceProjectScope,
    expected_revision: Option<WorkspaceProjectBindingRevision>,
    project_id: ChatGptProjectId,
    binding: WorkspaceProjectBinding,
}

impl WorkspaceProjectBindingReceipt {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn scope(&self) -> &WorkspaceProjectScope {
        &self.scope
    }

    #[must_use]
    pub const fn expected_revision(&self) -> Option<WorkspaceProjectBindingRevision> {
        self.expected_revision
    }

    #[must_use]
    pub const fn project_id(&self) -> ChatGptProjectId {
        self.project_id
    }

    #[must_use]
    pub const fn binding(&self) -> &WorkspaceProjectBinding {
        &self.binding
    }

    fn matches(&self, command: &WorkspaceProjectConfigureCommand) -> bool {
        self.scope == command.scope
            && self.expected_revision == command.expected_revision
            && self.project_id == command.project_id
    }
}

/// Exact, Source-timestamped configure request passed from the authenticated
/// Runtime boundary into the Operations catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceProjectConfigureCommand {
    request_id: Uuid,
    scope: WorkspaceProjectScope,
    expected_revision: Option<WorkspaceProjectBindingRevision>,
    project_id: ChatGptProjectId,
    configured_at: TimestampMillis,
}

impl WorkspaceProjectConfigureCommand {
    /// Construct one checked immutable request identity.
    ///
    /// # Errors
    /// Rejects a nil request ID.
    pub fn new(
        request_id: Uuid,
        scope: WorkspaceProjectScope,
        expected_revision: Option<WorkspaceProjectBindingRevision>,
        project_id: ChatGptProjectId,
        configured_at: TimestampMillis,
    ) -> Result<Self, OperationsError> {
        if request_id.is_nil() {
            return Err(OperationsError::Conflict(
                "ChatGPT Project request identity",
            ));
        }
        Ok(Self {
            request_id,
            scope,
            expected_revision,
            project_id,
            configured_at,
        })
    }
}

/// Configure response from one atomic CAS and immutable receipt commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceProjectConfigureOutcome {
    receipt: WorkspaceProjectBindingReceipt,
    current_binding: WorkspaceProjectBinding,
    replayed: bool,
}

impl WorkspaceProjectConfigureOutcome {
    #[must_use]
    pub const fn receipt(&self) -> &WorkspaceProjectBindingReceipt {
        &self.receipt
    }

    #[must_use]
    pub const fn current_binding(&self) -> &WorkspaceProjectBinding {
        &self.current_binding
    }

    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Current Source Workspace scope enrolled by a successful Project configure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceProjectEnrollment {
    scope: WorkspaceProjectScope,
    binding_revision: WorkspaceProjectBindingRevision,
}

impl WorkspaceProjectEnrollment {
    #[must_use]
    pub const fn scope(&self) -> &WorkspaceProjectScope {
        &self.scope
    }

    #[must_use]
    pub const fn binding_revision(&self) -> WorkspaceProjectBindingRevision {
        self.binding_revision
    }
}

impl OperationsCatalog {
    /// Read the current Workspace Project binding from one SQLite snapshot.
    ///
    /// # Errors
    /// Rejects nil Workspace identities, malformed persisted rows, and storage errors.
    pub fn workspace_project_binding(
        &self,
        workspace_id: Uuid,
    ) -> Result<Option<WorkspaceProjectBinding>, OperationsError> {
        if workspace_id.is_nil() {
            return Err(OperationsError::Conflict(
                "ChatGPT Project Workspace identity",
            ));
        }
        let tx = self.connection.unchecked_transaction()?;
        let result = load_binding(&tx, workspace_id);
        match result {
            Ok(binding) => {
                crate::catalog::commit_read(tx, self.destination)?;
                Ok(binding)
            }
            Err(error) => Err(error),
        }
    }

    /// Read the bounded current Source enrollment projection. Historical
    /// Project receipts are never used to backfill this authority.
    ///
    /// # Errors
    /// Rejects malformed, stale, orphaned, or over-capacity persisted rows.
    pub fn workspace_project_enrollments(
        &self,
    ) -> Result<Vec<WorkspaceProjectEnrollment>, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        let result = load_workspace_project_enrollments(&tx);
        match result {
            Ok(enrollments) => {
                crate::catalog::commit_read(tx, self.destination)?;
                Ok(enrollments)
            }
            Err(error) => Err(error),
        }
    }

    /// Configure one Project with exact-revision CAS and immutable request
    /// readback. Replays match the full original payload, independent of the
    /// Workspace's current binding.
    ///
    /// # Errors
    /// Rejects stale revisions, request-ID payload conflicts, malformed values,
    /// and persistence failures.
    pub fn configure_workspace_project(
        &mut self,
        command: WorkspaceProjectConfigureCommand,
        static_workspace_ids: &[Uuid],
    ) -> Result<WorkspaceProjectConfigureOutcome, OperationsError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(receipt) = load_receipt(&tx, command.request_id)? {
            if !receipt.matches(&command) {
                return Err(OperationsError::Conflict("ChatGPT Project request payload"));
            }
            let current_binding = load_binding(&tx, command.scope.workspace_id)?
                .ok_or(OperationsError::Corrupt("ChatGPT Project binding missing"))?;
            tx.commit()
                .map_err(|source| OperationsError::CatalogCommitUnconfirmed {
                    destination_id: self.destination,
                    source,
                })?;
            return Ok(WorkspaceProjectConfigureOutcome {
                receipt,
                current_binding,
                replayed: true,
            });
        }

        let WorkspaceProjectConfigureCommand {
            request_id,
            scope,
            expected_revision,
            project_id,
            configured_at,
        } = command;
        let workspace_id = scope.workspace_id;
        let current = load_binding(&tx, workspace_id)?;
        let revision =
            next_workspace_project_binding_revision(current.as_ref(), expected_revision)?;
        ensure_workspace_scope_capacity(&tx, workspace_id, static_workspace_ids)?;
        let binding = WorkspaceProjectBinding {
            workspace_id,
            project_id,
            revision,
            configured_at,
        };
        tx.execute(
            "INSERT INTO ops_workspace_chatgpt_project_bindings(workspace_id,project_id,revision,configured_at_ms) VALUES(?1,?2,?3,?4) ON CONFLICT(workspace_id) DO UPDATE SET project_id=excluded.project_id,revision=excluded.revision,configured_at_ms=excluded.configured_at_ms",
            params![
                binding.workspace_id.as_bytes(),
                binding.project_id.to_string(),
                binding.revision.as_i64(),
                binding.configured_at.get(),
            ],
        )?;
        let receipt = WorkspaceProjectBindingReceipt {
            request_id,
            scope,
            expected_revision,
            project_id,
            binding: binding.clone(),
        };
        insert_receipt(&tx, &receipt)?;
        let readback = load_binding(&tx, binding.workspace_id)?
            .ok_or(OperationsError::Corrupt("ChatGPT Project binding readback"))?;
        let receipt_readback = load_receipt(&tx, request_id)?
            .ok_or(OperationsError::Corrupt("ChatGPT Project receipt readback"))?;
        if readback != binding || receipt_readback != receipt {
            return Err(OperationsError::Corrupt(
                "ChatGPT Project configure readback",
            ));
        }
        upsert_workspace_project_enrollment(&tx, receipt.scope(), revision)?;
        let enrollment_readback = load_workspace_project_enrollments(&tx)?
            .into_iter()
            .find(|enrollment| enrollment.scope.workspace_id == workspace_id)
            .ok_or(OperationsError::Corrupt(
                "ChatGPT Project enrollment readback",
            ))?;
        if enrollment_readback.scope != *receipt.scope()
            || enrollment_readback.binding_revision != revision
        {
            return Err(OperationsError::Corrupt(
                "ChatGPT Project enrollment configure readback",
            ));
        }
        tx.commit()
            .map_err(|source| OperationsError::CatalogCommitUnconfirmed {
                destination_id: self.destination,
                source,
            })?;
        Ok(WorkspaceProjectConfigureOutcome {
            receipt,
            current_binding: readback,
            replayed: false,
        })
    }

    /// Read the exact immutable configure receipt without granting retry
    /// authority. A stored request ID with another Workspace scope conflicts.
    ///
    /// # Errors
    /// Rejects nil request identities, scope mismatches, malformed rows, and storage errors.
    pub fn workspace_project_receipt(
        &self,
        request_id: Uuid,
        scope: &WorkspaceProjectScope,
    ) -> Result<Option<WorkspaceProjectBindingReceipt>, OperationsError> {
        if request_id.is_nil() {
            return Err(OperationsError::Conflict(
                "ChatGPT Project request identity",
            ));
        }
        let tx = self.connection.unchecked_transaction()?;
        let result = load_receipt(&tx, request_id).and_then(|receipt| {
            if receipt
                .as_ref()
                .is_some_and(|receipt| receipt.scope != *scope)
            {
                return Err(OperationsError::Conflict("ChatGPT Project receipt scope"));
            }
            Ok(receipt)
        });
        match result {
            Ok(receipt) => {
                crate::catalog::commit_read(tx, self.destination)?;
                Ok(receipt)
            }
            Err(error) => Err(error),
        }
    }
}

fn ensure_workspace_scope_capacity(
    connection: &Connection,
    workspace_id: Uuid,
    static_workspace_ids: &[Uuid],
) -> Result<(), OperationsError> {
    let mut workspace_ids = BTreeSet::new();
    for static_workspace_id in static_workspace_ids {
        if static_workspace_id.is_nil() {
            return Err(OperationsError::Conflict(
                "ChatGPT Project static Workspace scope",
            ));
        }
        workspace_ids.insert(*static_workspace_id);
    }
    let mut statement = connection.prepare(
        "SELECT workspace_id FROM ops_workspace_chatgpt_project_work_scopes ORDER BY workspace_id",
    )?;
    let rows = statement.query_map([], |row| row.get::<_, Vec<u8>>(0))?;
    for row in rows {
        let bytes = row?;
        let id = Uuid::from_slice(&bytes)
            .map_err(|_| OperationsError::Corrupt("ChatGPT Project enrollment identity"))?;
        if id.is_nil() {
            return Err(OperationsError::Corrupt(
                "ChatGPT Project enrollment identity",
            ));
        }
        workspace_ids.insert(id);
    }
    if !workspace_ids.contains(&workspace_id)
        && workspace_ids.len() >= MAX_WORKSPACE_AUTHORITY_SCOPES
    {
        return Err(OperationsError::Conflict(
            "ChatGPT Project Workspace scope capacity",
        ));
    }
    Ok(())
}

fn next_workspace_project_binding_revision(
    current: Option<&WorkspaceProjectBinding>,
    expected_revision: Option<WorkspaceProjectBindingRevision>,
) -> Result<WorkspaceProjectBindingRevision, OperationsError> {
    match (current, expected_revision) {
        (None, None) => WorkspaceProjectBindingRevision::new(1),
        (Some(current), Some(expected)) if current.revision == expected => {
            let next = current
                .revision
                .as_i64()
                .checked_add(1)
                .ok_or(OperationsError::Bounds("ChatGPT Project binding revision"))?;
            WorkspaceProjectBindingRevision::new(next)
        }
        _ => Err(OperationsError::Conflict(
            "ChatGPT Project binding revision CAS",
        )),
    }
}

fn upsert_workspace_project_enrollment(
    connection: &Connection,
    scope: &WorkspaceProjectScope,
    binding_revision: WorkspaceProjectBindingRevision,
) -> Result<(), OperationsError> {
    connection.execute(
        "INSERT INTO ops_workspace_chatgpt_project_work_scopes(workspace_id,location_id,location_revision,workspace_revision,locator,binding_revision) VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(workspace_id) DO UPDATE SET location_id=excluded.location_id,location_revision=excluded.location_revision,workspace_revision=excluded.workspace_revision,locator=excluded.locator,binding_revision=excluded.binding_revision",
        params![
            scope.workspace_id.as_bytes(),
            scope.location_id.as_bytes(),
            scope.location_revision,
            scope.workspace_revision,
            scope.locator,
            binding_revision.as_i64(),
        ],
    )?;
    Ok(())
}

fn load_workspace_project_enrollments(
    connection: &Connection,
) -> Result<Vec<WorkspaceProjectEnrollment>, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT e.workspace_id,e.location_id,e.location_revision,e.workspace_revision,e.locator,e.binding_revision,b.revision FROM ops_workspace_chatgpt_project_work_scopes AS e LEFT JOIN ops_workspace_chatgpt_project_bindings AS b ON b.workspace_id=e.workspace_id ORDER BY e.workspace_id",
    )?;
    let mut rows = statement.query([])?;
    let mut enrollments = Vec::new();
    while let Some(row) = rows.next()? {
        if enrollments.len() == MAX_WORKSPACE_AUTHORITY_SCOPES {
            return Err(OperationsError::Corrupt(
                "ChatGPT Project enrollment capacity",
            ));
        }
        let workspace_id = Uuid::from_slice(&row.get::<_, Vec<u8>>(0)?)
            .map_err(|_| OperationsError::Corrupt("ChatGPT Project enrollment identity"))?;
        let location_id = Uuid::from_slice(&row.get::<_, Vec<u8>>(1)?)
            .map_err(|_| OperationsError::Corrupt("ChatGPT Project enrollment Location"))?;
        let location_revision = row.get::<_, i64>(2)?;
        let workspace_revision = row.get::<_, i64>(3)?;
        let locator = row.get::<_, String>(4)?;
        let binding_revision = row.get::<_, i64>(5)?;
        let current_binding_revision = row.get::<_, Option<i64>>(6)?;
        if workspace_id.is_nil() || location_id.is_nil() {
            return Err(OperationsError::Corrupt(
                "ChatGPT Project enrollment identity",
            ));
        }
        let binding_revision = WorkspaceProjectBindingRevision::new(binding_revision)
            .map_err(|_| OperationsError::Corrupt("ChatGPT Project enrollment revision"))?;
        if current_binding_revision != Some(binding_revision.as_i64()) {
            return Err(OperationsError::Corrupt(
                "ChatGPT Project enrollment binding",
            ));
        }
        let scope = WorkspaceProjectScope::new(
            workspace_id,
            location_id,
            location_revision,
            workspace_revision,
            locator,
        )
        .map_err(|_| OperationsError::Corrupt("ChatGPT Project enrollment scope"))?;
        enrollments.push(WorkspaceProjectEnrollment {
            scope,
            binding_revision,
        });
    }
    Ok(enrollments)
}

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        (
            "table",
            "ops_workspace_chatgpt_project_bindings",
            BINDINGS_SQL,
        ),
        (
            "table",
            "ops_workspace_chatgpt_project_receipts",
            RECEIPTS_SQL,
        ),
        (
            "trigger",
            "ops_workspace_chatgpt_project_receipts_no_update",
            RECEIPT_NO_UPDATE_TRIGGER,
        ),
        (
            "trigger",
            "ops_workspace_chatgpt_project_receipts_no_delete",
            RECEIPT_NO_DELETE_TRIGGER,
        ),
    ]
}

pub(crate) fn enrollment_schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![(
        "table",
        "ops_workspace_chatgpt_project_work_scopes",
        WORK_SCOPES_SQL,
    )]
}

#[cfg(test)]
pub(crate) fn drop_enrollment_schema_for_legacy_fixture(
    connection: &Connection,
) -> Result<(), OperationsError> {
    connection.execute_batch("DROP TABLE IF EXISTS ops_workspace_chatgpt_project_work_scopes;")?;
    Ok(())
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in schema_objects() {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

pub(crate) fn create_enrollment_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in enrollment_schema_objects() {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

pub(crate) fn load_binding(
    connection: &Connection,
    workspace_id: Uuid,
) -> Result<Option<WorkspaceProjectBinding>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT project_id,revision,configured_at_ms FROM ops_workspace_chatgpt_project_bindings WHERE workspace_id=?1",
            params![workspace_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional()?;
    raw.map(|(project_id, revision, configured_at)| {
        Ok(WorkspaceProjectBinding {
            workspace_id,
            project_id: ChatGptProjectId::try_from(project_id.as_str())
                .map_err(|_| OperationsError::Corrupt("ChatGPT Project identity"))?,
            revision: WorkspaceProjectBindingRevision::new(revision)
                .map_err(|_| OperationsError::Corrupt("ChatGPT Project binding revision"))?,
            configured_at: TimestampMillis::new(configured_at)
                .map_err(|_| OperationsError::Corrupt("ChatGPT Project configured time"))?,
        })
    })
    .transpose()
}

fn load_receipt(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<WorkspaceProjectBindingReceipt>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(workspace_id,1,17),substr(location_id,1,17),location_revision,workspace_revision,locator,expected_revision,project_id,binding_revision,configured_at_ms FROM ops_workspace_chatgpt_project_receipts WHERE request_id=?1",
            params![request_id.as_bytes()],
            |row| {
                Ok(RawWorkspaceProjectReceipt {
                    workspace_id: row.get(0)?,
                    location_id: row.get(1)?,
                    location_revision: row.get(2)?,
                    workspace_revision: row.get(3)?,
                    locator: row.get(4)?,
                    expected_revision: row.get(5)?,
                    project_id: row.get(6)?,
                    binding_revision: row.get(7)?,
                    configured_at: row.get(8)?,
                })
            },
        )
        .optional()?;
    raw.map(|raw| decode_receipt(request_id, raw)).transpose()
}

struct RawWorkspaceProjectReceipt {
    workspace_id: Vec<u8>,
    location_id: Vec<u8>,
    location_revision: i64,
    workspace_revision: i64,
    locator: String,
    expected_revision: Option<i64>,
    project_id: String,
    binding_revision: i64,
    configured_at: i64,
}

fn decode_receipt(
    request_id: Uuid,
    raw: RawWorkspaceProjectReceipt,
) -> Result<WorkspaceProjectBindingReceipt, OperationsError> {
    let RawWorkspaceProjectReceipt {
        workspace_id,
        location_id,
        location_revision,
        workspace_revision,
        locator,
        expected_revision,
        project_id,
        binding_revision,
        configured_at,
    } = raw;
    let scope = WorkspaceProjectScope::new(
        Uuid::from_bytes(wire::id(workspace_id)?),
        Uuid::from_bytes(wire::id(location_id)?),
        location_revision,
        workspace_revision,
        locator,
    )
    .map_err(|_| OperationsError::Corrupt("ChatGPT Project receipt scope"))?;
    let project_id = ChatGptProjectId::try_from(project_id.as_str())
        .map_err(|_| OperationsError::Corrupt("ChatGPT Project receipt identity"))?;
    let revision = WorkspaceProjectBindingRevision::new(binding_revision)
        .map_err(|_| OperationsError::Corrupt("ChatGPT Project receipt revision"))?;
    let expected_revision = expected_revision
        .map(WorkspaceProjectBindingRevision::new)
        .transpose()
        .map_err(|_| OperationsError::Corrupt("ChatGPT Project receipt expected revision"))?;
    let configured_at = TimestampMillis::new(configured_at)
        .map_err(|_| OperationsError::Corrupt("ChatGPT Project receipt time"))?;
    let binding = WorkspaceProjectBinding {
        workspace_id: scope.workspace_id,
        project_id,
        revision,
        configured_at,
    };
    Ok(WorkspaceProjectBindingReceipt {
        request_id,
        scope,
        expected_revision,
        project_id,
        binding,
    })
}

fn insert_receipt(
    transaction: &rusqlite::Transaction<'_>,
    receipt: &WorkspaceProjectBindingReceipt,
) -> Result<(), OperationsError> {
    transaction.execute(
        "INSERT INTO ops_workspace_chatgpt_project_receipts(request_id,workspace_id,location_id,location_revision,workspace_revision,locator,expected_revision,project_id,binding_revision,configured_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        params![
            receipt.request_id.as_bytes(),
            receipt.scope.workspace_id.as_bytes(),
            receipt.scope.location_id.as_bytes(),
            receipt.scope.location_revision,
            receipt.scope.workspace_revision,
            receipt.scope.locator,
            receipt.expected_revision.map(WorkspaceProjectBindingRevision::as_i64),
            receipt.project_id.to_string(),
            receipt.binding.revision.as_i64(),
            receipt.binding.configured_at.get(),
        ],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentProvisionEffectKind, AgentProvisionState, AgentRequestCommand,
        AgentRequestEffectScope, AgentRequestIndependence, AgentRequestIntent,
        AgentRequestRequester, AgentRequestResolutionSelection, CommitAgentRequestResolution,
    };
    use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, TimestampMillis, WorkspaceId};
    use heterarchy_orchestration_sessions::{AgentProfileId, AgentProfileRuntimeSessionId};
    use rusqlite::Connection;
    use std::{error::Error, fs, path::Path, path::PathBuf};

    fn project_catalog() -> Result<(PathBuf, PathBuf, Uuid, OperationsCatalog), Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!("workspace-project-{}", Uuid::now_v7()));
        fs::create_dir_all(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xA100);
        let catalog = OperationsCatalog::open(&path, destination)?;
        Ok((root, path, destination, catalog))
    }

    fn scope(workspace_id: Uuid) -> Result<WorkspaceProjectScope, OperationsError> {
        WorkspaceProjectScope::new(
            workspace_id,
            Uuid::from_u128(0xA101),
            1,
            1,
            "workspace-locator".to_owned(),
        )
    }

    fn project_id(value: &str) -> Result<ChatGptProjectId, OperationsError> {
        ChatGptProjectId::try_from(value)
    }

    fn timestamp(value: i64) -> Result<TimestampMillis, OperationsError> {
        TimestampMillis::new(value).map_err(|_| OperationsError::Conflict("test timestamp"))
    }

    #[test]
    fn project_identity_and_revision_parsers_are_canonical() -> Result<(), Box<dyn Error>> {
        let id = ChatGptProjectId::try_from("g-p-6ad231f7ad7c8191bfc1d5ff782c171f")?;
        assert_eq!(id.to_string(), "g-p-6ad231f7ad7c8191bfc1d5ff782c171f");
        for invalid in [
            "g-p-6ad231f7ad7c8191bfc1d5ff782c171",
            "g-p-6ad231f7ad7c8191bfc1d5ff782c171G",
            "g-p-6ad231f7ad7c8191bfc1d5ff782c171f0",
            "g-p-6ad231f7ad7c8191bfc1d5ff782c171f/",
        ] {
            assert!(ChatGptProjectId::try_from(invalid).is_err());
        }
        let max_revision = WorkspaceProjectBindingRevision::from_str("9223372036854775807")?;
        assert_eq!(max_revision.as_i64(), i64::MAX);
        for invalid in ["", "0", "01", "+1", "-1", "9223372036854775808"] {
            assert!(WorkspaceProjectBindingRevision::from_str(invalid).is_err());
        }
        Ok(())
    }

    #[test]
    fn configure_uses_exact_request_replay_and_revision_cas() -> Result<(), Box<dyn Error>> {
        let (root, _, _, mut catalog) = project_catalog()?;
        let workspace_id = Uuid::from_u128(0xA102);
        let scope = scope(workspace_id)?;
        let project_a = project_id("g-p-6ad231f7ad7c8191bfc1d5ff782c171f")?;
        let project_b = project_id("g-p-7ad231f7ad7c8191bfc1d5ff782c171f")?;
        assert!(catalog.workspace_project_binding(workspace_id)?.is_none());

        let first_command = WorkspaceProjectConfigureCommand::new(
            Uuid::from_u128(0xA103),
            scope.clone(),
            None,
            project_a,
            timestamp(10)?,
        )?;
        let first = catalog.configure_workspace_project(first_command.clone(), &[])?;
        assert!(!first.replayed());
        assert_eq!(first.receipt().binding().revision().as_i64(), 1);
        assert_eq!(first.current_binding().project_id(), project_a);

        let second = catalog.configure_workspace_project(
            WorkspaceProjectConfigureCommand::new(
                Uuid::from_u128(0xA104),
                scope.clone(),
                Some(first.current_binding().revision()),
                project_b,
                timestamp(20)?,
            )?,
            &[],
        )?;
        assert!(!second.replayed());
        assert_eq!(second.current_binding().revision().as_i64(), 2);

        let replay = catalog.configure_workspace_project(first_command.clone(), &[])?;
        assert!(replay.replayed());
        assert_eq!(replay.receipt(), first.receipt());
        assert_eq!(replay.current_binding(), second.current_binding());

        assert!(matches!(
            catalog.configure_workspace_project(
                WorkspaceProjectConfigureCommand::new(
                    first_command.request_id,
                    scope.clone(),
                    None,
                    project_b,
                    timestamp(30)?,
                )?,
                &[],
            ),
            Err(OperationsError::Conflict("ChatGPT Project request payload"))
        ));
        assert!(matches!(
            catalog.configure_workspace_project(
                WorkspaceProjectConfigureCommand::new(
                    Uuid::from_u128(0xA105),
                    scope.clone(),
                    Some(first.current_binding().revision()),
                    project_a,
                    timestamp(30)?,
                )?,
                &[],
            ),
            Err(OperationsError::Conflict(
                "ChatGPT Project binding revision CAS"
            ))
        ));

        assert_eq!(
            catalog.workspace_project_receipt(first.receipt().request_id(), &scope)?,
            Some(first.receipt().clone())
        );
        assert_eq!(
            catalog.workspace_project_receipt(Uuid::from_u128(0xA106), &scope)?,
            None
        );
        let wrong_scope = WorkspaceProjectScope::new(
            workspace_id,
            Uuid::from_u128(0xA107),
            1,
            1,
            "another-workspace-locator".to_owned(),
        )?;
        assert!(matches!(
            catalog.workspace_project_receipt(first.receipt().request_id(), &wrong_scope),
            Err(OperationsError::Conflict("ChatGPT Project receipt scope"))
        ));

        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn configure_enrollment_is_atomic_and_old_replay_keeps_the_latest_scope()
    -> Result<(), Box<dyn Error>> {
        let (root, _, _, mut catalog) = project_catalog()?;
        let workspace_id = Uuid::from_u128(0xA110);
        let first_scope = scope(workspace_id)?;
        let first_command = WorkspaceProjectConfigureCommand::new(
            Uuid::from_u128(0xA111),
            first_scope.clone(),
            None,
            project_id("g-p-6ad231f7ad7c8191bfc1d5ff782c171f")?,
            timestamp(10)?,
        )?;
        let first = catalog.configure_workspace_project(first_command.clone(), &[])?;
        let first_enrollment = catalog
            .workspace_project_enrollments()?
            .into_iter()
            .next()
            .ok_or("first enrollment")?;
        assert_eq!(first_enrollment.scope(), &first_scope);
        assert_eq!(first_enrollment.binding_revision().as_i64(), 1);

        let current_scope = WorkspaceProjectScope::new(
            workspace_id,
            first_scope.location_id(),
            2,
            2,
            "workspace-locator-v2".to_owned(),
        )?;
        let current = catalog.configure_workspace_project(
            WorkspaceProjectConfigureCommand::new(
                Uuid::from_u128(0xA112),
                current_scope.clone(),
                Some(first.current_binding().revision()),
                project_id("g-p-7ad231f7ad7c8191bfc1d5ff782c171f")?,
                timestamp(20)?,
            )?,
            &[],
        )?;
        assert_eq!(current.current_binding().revision().as_i64(), 2);
        let replay = catalog.configure_workspace_project(first_command, &[])?;
        assert!(replay.replayed());
        assert_eq!(replay.receipt(), first.receipt());
        assert_eq!(replay.current_binding(), current.current_binding());
        let enrollment = catalog
            .workspace_project_enrollments()?
            .into_iter()
            .next()
            .ok_or("current enrollment")?;
        assert_eq!(enrollment.scope(), &current_scope);
        assert_eq!(enrollment.binding_revision().as_i64(), 2);

        catalog.connection.execute_batch(
            "CREATE TRIGGER fail_workspace_project_scope_insert
             BEFORE INSERT ON ops_workspace_chatgpt_project_work_scopes
             BEGIN SELECT RAISE(ABORT, 'injected enrollment failure'); END;",
        )?;
        let failed_request = Uuid::from_u128(0xA113);
        assert!(
            catalog
                .configure_workspace_project(
                    WorkspaceProjectConfigureCommand::new(
                        failed_request,
                        current_scope.clone(),
                        Some(current.current_binding().revision()),
                        project_id("g-p-6ad231f7ad7c8191bfc1d5ff782c171f")?,
                        timestamp(30)?,
                    )?,
                    &[],
                )
                .is_err()
        );
        catalog
            .connection
            .execute_batch("DROP TRIGGER fail_workspace_project_scope_insert;")?;
        assert_eq!(
            catalog.workspace_project_binding(workspace_id)?,
            Some(current.current_binding().clone())
        );
        assert_eq!(
            catalog.workspace_project_receipt(failed_request, &current_scope)?,
            None
        );
        assert_eq!(
            catalog
                .workspace_project_enrollments()?
                .into_iter()
                .next()
                .ok_or("enrollment after failed transaction")?
                .scope(),
            &current_scope
        );

        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn concurrent_configures_cannot_exceed_static_plus_dynamic_scope_capacity()
    -> Result<(), Box<dyn Error>> {
        let (root, path, destination, catalog) = project_catalog()?;
        drop(catalog);
        let static_workspace_ids = (0..MAX_WORKSPACE_AUTHORITY_SCOPES - 1)
            .map(|offset| {
                u128::try_from(offset)
                    .map(|offset| Uuid::from_u128(0xB100 + offset))
                    .map_err(|_| OperationsError::Bounds("test static scope offset"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let workers = (0_u8..2)
            .map(|offset| {
                let path = path.clone();
                let static_workspace_ids = static_workspace_ids.clone();
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    (|| -> Result<bool, OperationsError> {
                        let mut catalog = OperationsCatalog::open(&path, destination)?;
                        let workspace_id = Uuid::from_u128(0xC100 + u128::from(offset));
                        let command = WorkspaceProjectConfigureCommand::new(
                            Uuid::from_u128(0xC200 + u128::from(offset)),
                            WorkspaceProjectScope::new(
                                workspace_id,
                                Uuid::from_u128(0xB200),
                                1,
                                1,
                                format!("concurrent-workspace-{offset}"),
                            )?,
                            None,
                            ChatGptProjectId::try_from("g-p-6ad231f7ad7c8191bfc1d5ff782c171f")?,
                            TimestampMillis::new(100 + i64::from(offset))
                                .map_err(|_| OperationsError::Conflict("test timestamp"))?,
                        )?;
                        barrier.wait();
                        match catalog.configure_workspace_project(command, &static_workspace_ids) {
                            Ok(_) => Ok(true),
                            Err(OperationsError::Conflict(
                                "ChatGPT Project Workspace scope capacity",
                            )) => Ok(false),
                            Err(error) => Err(error),
                        }
                    })()
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let outcomes = workers
            .into_iter()
            .map(std::thread::JoinHandle::join)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| OperationsError::Corrupt("capacity worker panic"))?;
        assert_eq!(
            outcomes
                .iter()
                .filter(|result| result.as_ref().is_ok_and(|success| *success))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|result| result.as_ref().is_ok_and(|success| !*success))
                .count(),
            1
        );
        assert_eq!(outcomes.iter().filter(|result| result.is_err()).count(), 0);
        assert_eq!(
            OperationsCatalog::open(&path, destination)?
                .workspace_project_enrollments()?
                .len(),
            1
        );
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn v49_upgrade_preserves_project_history_without_backfilling_enrollment()
    -> Result<(), Box<dyn Error>> {
        let (root, path, destination, mut catalog) = project_catalog()?;
        let workspace_id = Uuid::from_u128(0xA120);
        let scope = scope(workspace_id)?;
        let configured = catalog.configure_workspace_project(
            WorkspaceProjectConfigureCommand::new(
                Uuid::from_u128(0xA121),
                scope.clone(),
                None,
                project_id("g-p-6ad231f7ad7c8191bfc1d5ff782c171f")?,
                timestamp(11)?,
            )?,
            &[],
        )?;
        let binding = configured.current_binding().clone();
        let receipt = configured.receipt().clone();
        drop(catalog);

        let old = Connection::open(&path)?;
        crate::import_handoff::drop_schema_for_legacy_fixture(&old)?;
        old.execute_batch(
            "DROP TRIGGER ops_provider_message_unknown_isolations_no_update;
             DROP TRIGGER ops_provider_message_unknown_isolations_no_delete;
             DROP TABLE ops_provider_message_unknown_isolations;
             DROP TABLE ops_workspace_chatgpt_project_work_scopes;
             PRAGMA user_version=49;",
        )?;
        drop(old);

        let upgraded = OperationsCatalog::open(&path, destination)?;
        assert_eq!(
            upgraded
                .connection
                .pragma_query_value::<i64, _>(None, "user_version", |row| row.get(0))?,
            crate::catalog::VERSION
        );
        assert_eq!(
            upgraded.workspace_project_binding(workspace_id)?,
            Some(binding)
        );
        assert_eq!(
            upgraded.workspace_project_receipt(receipt.request_id(), &scope)?,
            Some(receipt)
        );
        assert!(upgraded.workspace_project_enrollments()?.is_empty());

        drop(upgraded);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    fn submit_two_slot_project_request(
        catalog: &mut OperationsCatalog,
        workspace_id: Uuid,
        request_id: Uuid,
    ) -> Result<(), Box<dyn Error>> {
        let request = AgentRequestCommand::new(
            request_id,
            AgentRequestRequester::new(
                WorkspaceId::from_uuid(workspace_id),
                ActivityId::from_uuid(Uuid::from_u128(0xA203)),
                AgentInstanceId::from_uuid(Uuid::from_u128(0xA204)),
                AgentSessionId::from_uuid(Uuid::from_u128(0xA205)),
                1,
            )?,
            AgentRequestIntent::new(
                "preserve one confirmed and one unknown route".to_owned(),
                "verification".to_owned(),
                2,
                AgentRequestEffectScope::ReadOnly,
                AgentRequestIndependence::None,
                "verify schema upgrade history".to_owned(),
            )?,
            timestamp(2)?,
        )?;
        catalog.submit_agent_request(&request)?;
        catalog.commit_agent_request_resolution(&CommitAgentRequestResolution::new(
            request_id,
            1,
            1,
            vec![
                AgentRequestResolutionSelection::provision_new(0)?,
                AgentRequestResolutionSelection::provision_new(1)?,
            ],
            timestamp(3)?,
        )?)?;
        Ok(())
    }

    fn prepare_rebound_project_operations(
        catalog: &mut OperationsCatalog,
        request_id: Uuid,
        scope: WorkspaceProjectScope,
    ) -> Result<[crate::AgentProvisionPrepareOutcome; 2], Box<dyn Error>> {
        assert!(matches!(
            catalog.prepare_agent_provision_operation(request_id, 0, timestamp(4)?),
            Err(OperationsError::Conflict(
                "ChatGPT Project binding required for new provision"
            ))
        ));
        let configured = catalog.configure_workspace_project(
            WorkspaceProjectConfigureCommand::new(
                Uuid::from_u128(0xA201),
                scope.clone(),
                None,
                project_id("g-p-6ad231f7ad7c8191bfc1d5ff782c171f")?,
                timestamp(5)?,
            )?,
            &[],
        )?;
        let first = catalog.prepare_agent_provision_operation(request_id, 0, timestamp(6)?)?;
        let rebound = catalog.configure_workspace_project(
            WorkspaceProjectConfigureCommand::new(
                Uuid::from_u128(0xA206),
                scope,
                Some(configured.current_binding().revision()),
                project_id("g-p-7ad231f7ad7c8191bfc1d5ff782c171f")?,
                timestamp(7)?,
            )?,
            &[],
        )?;
        let second = catalog.prepare_agent_provision_operation(request_id, 1, timestamp(8)?)?;
        assert_eq!(
            first.operation().project(),
            Some(configured.current_binding().project_reference())
        );
        assert_eq!(
            catalog
                .agent_provision_operation(first.operation().operation_id())?
                .as_ref()
                .and_then(crate::AgentProvisionOperation::project),
            Some(configured.current_binding().project_reference())
        );
        assert_eq!(
            second.operation().project(),
            Some(rebound.current_binding().project_reference())
        );
        for (index, prepared) in [&first, &second].into_iter().enumerate() {
            let profile = AgentProfileId::from_uuid(Uuid::from_u128(0xA210 + index as u128))?;
            let runtime =
                AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xA220 + index as u128))?;
            catalog.bind_agent_provision_target(
                prepared.operation().operation_id(),
                prepared.operation().revision(),
                profile,
                runtime,
                timestamp(9)?,
            )?;
        }
        Ok([first, second])
    }

    fn confirm_one_and_mark_one_unknown(
        catalog: &mut OperationsCatalog,
    ) -> Result<(Uuid, Uuid), Box<dyn Error>> {
        let first = catalog
            .next_agent_provision_dispatch(timestamp(10)?)?
            .ok_or("first provision dispatch")?;
        assert_eq!(
            first.operation().effect_kind(),
            AgentProvisionEffectKind::ProvisionNew
        );
        let confirmed = catalog.confirm_agent_provision_conversation(
            first.operation().operation_id(),
            first.operation().revision(),
            "confirmed-before-v49",
            timestamp(10_011)?,
        )?;
        assert_eq!(confirmed.state(), AgentProvisionState::Confirmed);

        let second = catalog
            .next_agent_provision_dispatch(timestamp(20_012)?)?
            .ok_or("second provision dispatch")?;
        let unknown = catalog.mark_agent_provision_effect_unknown(
            second.operation().operation_id(),
            second.operation().revision(),
            timestamp(20_013)?,
        )?;
        assert_eq!(unknown.state(), AgentProvisionState::EffectUnknown);
        Ok((confirmed.operation_id(), unknown.operation_id()))
    }

    fn downgrade_project_fixture_to_v48(path: &Path) -> Result<(), Box<dyn Error>> {
        let connection = Connection::open(path)?;
        crate::import_handoff::drop_schema_for_legacy_fixture(&connection)?;
        connection.execute_batch(
            "DROP TRIGGER ops_provider_message_unknown_isolations_no_update;
             DROP TRIGGER ops_provider_message_unknown_isolations_no_delete;
             DROP TABLE ops_provider_message_unknown_isolations;
             DROP TABLE ops_workspace_chatgpt_project_work_scopes;
             DROP TABLE ops_provider_message_project_receipts;
             DROP TABLE ops_provider_message_project_routes;
             DROP TABLE ops_agent_provision_project_routes;
             DROP TABLE ops_workspace_chatgpt_project_receipts;
             DROP TABLE ops_workspace_chatgpt_project_bindings;
             PRAGMA user_version=48;",
        )?;
        Ok(())
    }

    #[test]
    fn schema_48_upgrade_preserves_confirmed_and_unknown_provision_rows_without_backfill()
    -> Result<(), Box<dyn Error>> {
        let (root, path, destination, mut catalog) = project_catalog()?;
        let workspace = Uuid::from_u128(0xA200);
        let request_id = Uuid::from_u128(0xA202);
        submit_two_slot_project_request(&mut catalog, workspace, request_id)?;
        let _prepared =
            prepare_rebound_project_operations(&mut catalog, request_id, scope(workspace)?)?;
        let (confirmed_id, unknown_id) = confirm_one_and_mark_one_unknown(&mut catalog)?;
        drop(catalog);
        downgrade_project_fixture_to_v48(&path)?;

        let reopened = OperationsCatalog::open(&path, destination)?;
        assert!(reopened.workspace_project_binding(workspace)?.is_none());
        let preserved_confirmed = reopened
            .agent_provision_operation(confirmed_id)?
            .ok_or("confirmed operation")?;
        assert_eq!(preserved_confirmed.state(), AgentProvisionState::Confirmed);
        assert_eq!(
            preserved_confirmed.provider_conversation_id(),
            Some("confirmed-before-v49")
        );
        assert_eq!(preserved_confirmed.project(), None);
        let preserved_unknown = reopened
            .agent_provision_operation(unknown_id)?
            .ok_or("unknown operation")?;
        assert_eq!(
            preserved_unknown.state(),
            AgentProvisionState::EffectUnknown
        );
        assert_eq!(preserved_unknown.project(), None);

        drop(reopened);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn migrated_legacy_prepared_creation_cannot_release_without_frozen_project()
    -> Result<(), Box<dyn Error>> {
        use crate::{
            AgentProvisionState, AgentRequestCommand, AgentRequestEffectScope,
            AgentRequestIndependence, AgentRequestIntent, AgentRequestRequester,
            AgentRequestResolutionSelection, CommitAgentRequestResolution,
        };
        use heterarchy_orchestration_sessions::{AgentProfileId, AgentProfileRuntimeSessionId};

        let (root, path, destination, mut catalog) = project_catalog()?;
        let workspace = Uuid::from_u128(0xA300);
        let request_id = Uuid::from_u128(0xA301);
        let request = AgentRequestCommand::new(
            request_id,
            AgentRequestRequester::new(
                WorkspaceId::from_uuid(workspace),
                ActivityId::from_uuid(Uuid::from_u128(0xA302)),
                AgentInstanceId::from_uuid(Uuid::from_u128(0xA303)),
                AgentSessionId::from_uuid(Uuid::from_u128(0xA304)),
                1,
            )?,
            AgentRequestIntent::new(
                "legacy prepared creation migration".to_owned(),
                "verification".to_owned(),
                1,
                AgentRequestEffectScope::ReadOnly,
                AgentRequestIndependence::None,
                "legacy row has no frozen Project route".to_owned(),
            )?,
            timestamp(1)?,
        )?;
        catalog.submit_agent_request(&request)?;
        catalog.commit_agent_request_resolution(&CommitAgentRequestResolution::new(
            request_id,
            1,
            1,
            vec![AgentRequestResolutionSelection::provision_new(0)?],
            timestamp(2)?,
        )?)?;
        let configured = catalog.configure_workspace_project(
            WorkspaceProjectConfigureCommand::new(
                Uuid::from_u128(0xA305),
                scope(workspace)?,
                None,
                project_id("g-p-6ad231f7ad7c8191bfc1d5ff782c171f")?,
                timestamp(3)?,
            )?,
            &[],
        )?;
        let prepared = catalog.prepare_agent_provision_operation(request_id, 0, timestamp(4)?)?;
        assert_eq!(
            prepared.operation().project(),
            Some(configured.current_binding().project_reference())
        );
        catalog.bind_agent_provision_target(
            prepared.operation().operation_id(),
            prepared.operation().revision(),
            AgentProfileId::from_uuid(Uuid::from_u128(0xA306))?,
            AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xA307))?,
            timestamp(5)?,
        )?;
        let operation_id = prepared.operation().operation_id();
        drop(catalog);

        let connection = Connection::open(&path)?;
        crate::import_handoff::drop_schema_for_legacy_fixture(&connection)?;
        connection.execute_batch(
            "DROP TRIGGER ops_provider_message_unknown_isolations_no_update;
             DROP TRIGGER ops_provider_message_unknown_isolations_no_delete;
             DROP TABLE ops_provider_message_unknown_isolations;
             DROP TABLE ops_workspace_chatgpt_project_work_scopes;
             DROP TABLE ops_provider_message_project_receipts;
             DROP TABLE ops_provider_message_project_routes;
             DROP TABLE ops_agent_provision_project_routes;
             DROP TABLE ops_workspace_chatgpt_project_receipts;
             DROP TABLE ops_workspace_chatgpt_project_bindings;
             PRAGMA user_version=48;",
        )?;
        drop(connection);

        let mut reopened = OperationsCatalog::open(&path, destination)?;
        let legacy = reopened
            .agent_provision_operation(operation_id)?
            .ok_or("migrated legacy operation")?;
        assert_eq!(legacy.state(), AgentProvisionState::Prepared);
        assert_eq!(legacy.project(), None);
        assert!(matches!(
            reopened.next_agent_provision_dispatch(timestamp(6)?),
            Err(OperationsError::Conflict(
                "ChatGPT Project binding required before new provision effect"
            ))
        ));
        assert_eq!(
            reopened
                .agent_provision_operation(operation_id)?
                .ok_or("legacy operation after refused dispatch")?
                .state(),
            AgentProvisionState::Prepared
        );

        drop(reopened);
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
