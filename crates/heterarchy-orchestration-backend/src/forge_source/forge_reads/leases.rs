//! Source read observations for the command/diff/writer-lease family:
//! `command.list`, `diff.read`, `writer_lease.get`, and `writer_lease.list`.
//!
//! `diff.list`, `diff_doctor`, and `diff_artifact_doctor` have no configured
//! Forge HTTP contract and intentionally have no codec here: they stay
//! fail-closed on the backend source surface until a real contract exists.
//!
//! Decoders mirror the established native/HTTP acceptance class, including
//! the bare-hex catalog fingerprint recomposed into the canonical
//! algorithm-prefixed form, Writer Lease scope pages re-composed at the
//! native page limits, and the status-derived blocking/recovery hints.

use forge_backend::ForgeHttpClientError;
use forge_backend::ops::CommandOp;
use serde_json::{Value, json};
use uuid::Uuid;

use super::super::{ForgeSourceError, ForgeSourceHttpClient, map_command_error};

/// The established Writer Lease scope page limit for `writer_lease.get`.
pub const WRITER_LEASE_SCOPE_PAGE_LIMIT: usize = 4;
/// The established per-lease scope page limit inside `writer_lease.list`.
pub const WRITER_LEASE_LIST_SCOPE_PAGE_LIMIT: usize = 1;
/// The source catalog's active-lease bound, surfaced in the projection.
pub const WRITER_LEASE_ENTRY_LIMIT: usize = 256;

impl ForgeWriterLeaseProjection {
    /// The source catalog's active-lease bound surfaced in the projection.
    pub const LEASE_ENTRY_LIMIT: usize = WRITER_LEASE_ENTRY_LIMIT;
    /// The source scope-entry bound surfaced in the projection.
    pub const SCOPE_ENTRY_LIMIT: usize = WRITER_LEASE_SCOPE_ENTRY_LIMIT;
    /// The source path byte bound surfaced in the projection.
    pub const PATH_BYTE_LIMIT: usize = WRITER_LEASE_PATH_BYTE_LIMIT;
}
/// The source scope-entry bound, surfaced in the projection.
pub const WRITER_LEASE_SCOPE_ENTRY_LIMIT: usize = 64;
/// The source path byte bound, surfaced in the projection.
pub const WRITER_LEASE_PATH_BYTE_LIMIT: usize = 4_096;

/// One discovered project command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeProjectCommand {
    command_id: String,
    class: ForgeProjectCommandClass,
}

impl ForgeProjectCommand {
    /// The exact stable command id returned by Forge.
    #[must_use]
    pub fn command_id(&self) -> &str {
        &self.command_id
    }

    /// The closed command class.
    #[must_use]
    pub const fn class(&self) -> ForgeProjectCommandClass {
        self.class
    }
}

/// The closed project-command class vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgeProjectCommandClass {
    Verify,
    GeneratedPackage,
}

impl ForgeProjectCommandClass {
    /// The exact wire vocabulary string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Verify => "verify",
            Self::GeneratedPackage => "generated_package",
        }
    }
}

/// One bounded `command.list` observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeCommandList {
    activity_id: String,
    catalog_fingerprint: String,
    commands: Vec<ForgeProjectCommand>,
}

impl ForgeCommandList {
    /// The exact Activity identity echoed by Forge.
    #[must_use]
    pub fn activity_id(&self) -> &str {
        &self.activity_id
    }

    /// The canonical algorithm-prefixed catalog fingerprint.
    #[must_use]
    pub fn catalog_fingerprint(&self) -> &str {
        &self.catalog_fingerprint
    }

    /// The bounded command list.
    #[must_use]
    pub fn commands(&self) -> &[ForgeProjectCommand] {
        &self.commands
    }
}

/// One bounded `diff.read` observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeDiffRead {
    scope: crate::forge_source::forge_reads::file::ForgeSourceScope,
    diff_id: String,
    workspace_id: String,
    activity_id: String,
    change_set_id: String,
    origin: ForgeDiffOrigin,
    mutation_id: Option<String>,
    entries: Vec<ForgeDiffEntry>,
    state: ForgeDiffEvidenceState,
    render_kind: ForgeDiffRenderKind,
    compressed_hash: Option<String>,
    uncompressed_hash: Option<String>,
    compressed_size_bytes: Option<u64>,
    uncompressed_size_bytes: Option<u64>,
    renderer_version: u32,
    redaction_policy_version: u32,
    created_at_ms: i64,
    updated_at_ms: i64,
    revision: i64,
    health: ForgeDiffArtifactHealth,
    content: Option<String>,
}

impl ForgeDiffRead {
    /// The exact source scope echoed by Forge.
    #[must_use]
    pub const fn scope(&self) -> &crate::forge_source::forge_reads::file::ForgeSourceScope {
        &self.scope
    }

    /// The exact Diff identity string.
    #[must_use]
    pub fn diff_id(&self) -> &str {
        &self.diff_id
    }

    /// The exact Workspace identity string.
    #[must_use]
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }

    /// The exact Activity identity string.
    #[must_use]
    pub fn activity_id(&self) -> &str {
        &self.activity_id
    }

    /// The exact `ChangeSet` identity string.
    #[must_use]
    pub fn change_set_id(&self) -> &str {
        &self.change_set_id
    }

    /// The closed diff-origin vocabulary.
    #[must_use]
    pub const fn origin(&self) -> ForgeDiffOrigin {
        self.origin
    }

    /// The optional origin mutation identity.
    #[must_use]
    pub fn mutation_id(&self) -> Option<&str> {
        self.mutation_id.as_deref()
    }

    /// The diff entry list.
    #[must_use]
    pub fn entries(&self) -> &[ForgeDiffEntry] {
        &self.entries
    }

    /// The closed evidence state.
    #[must_use]
    pub const fn state(&self) -> ForgeDiffEvidenceState {
        self.state
    }

    /// The closed render kind.
    #[must_use]
    pub const fn render_kind(&self) -> ForgeDiffRenderKind {
        self.render_kind
    }

    /// The optional compressed artifact hash.
    #[must_use]
    pub fn compressed_hash(&self) -> Option<&str> {
        self.compressed_hash.as_deref()
    }

    /// The optional uncompressed artifact hash.
    #[must_use]
    pub fn uncompressed_hash(&self) -> Option<&str> {
        self.uncompressed_hash.as_deref()
    }

    /// The optional compressed artifact size.
    #[must_use]
    pub const fn compressed_size_bytes(&self) -> Option<u64> {
        self.compressed_size_bytes
    }

    /// The optional uncompressed artifact size.
    #[must_use]
    pub const fn uncompressed_size_bytes(&self) -> Option<u64> {
        self.uncompressed_size_bytes
    }

    /// The renderer version.
    #[must_use]
    pub const fn renderer_version(&self) -> u32 {
        self.renderer_version
    }

    /// The redaction policy version.
    #[must_use]
    pub const fn redaction_policy_version(&self) -> u32 {
        self.redaction_policy_version
    }

    /// Creation time in epoch milliseconds.
    #[must_use]
    pub const fn created_at_ms(&self) -> i64 {
        self.created_at_ms
    }

    /// Last update time in epoch milliseconds.
    #[must_use]
    pub const fn updated_at_ms(&self) -> i64 {
        self.updated_at_ms
    }

    /// The exact Diff revision.
    #[must_use]
    pub const fn revision(&self) -> i64 {
        self.revision
    }

    /// The closed artifact health vocabulary.
    #[must_use]
    pub const fn health(&self) -> ForgeDiffArtifactHealth {
        self.health
    }

    /// The optional rendered content.
    #[must_use]
    pub fn content(&self) -> Option<&str> {
        self.content.as_deref()
    }
}

/// The closed diff-origin vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgeDiffOrigin {
    FileMutation,
    TreeChangeSet,
}

/// One diff entry observed from Forge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeDiffEntry {
    subject: ForgeDiffEntrySubject,
    primary_path_token: String,
    secondary_path_token: Option<String>,
    change_kind: ForgeDiffChangeKind,
    additions: u32,
    deletions: u32,
}

impl ForgeDiffEntry {
    /// The closed entry subject.
    #[must_use]
    pub const fn subject(&self) -> ForgeDiffEntrySubject {
        self.subject
    }

    /// The canonical primary path token.
    #[must_use]
    pub fn primary_path_token(&self) -> &str {
        &self.primary_path_token
    }

    /// The canonical secondary path token (moves only).
    #[must_use]
    pub fn secondary_path_token(&self) -> Option<&str> {
        self.secondary_path_token.as_deref()
    }

    /// The closed change kind.
    #[must_use]
    pub const fn change_kind(&self) -> ForgeDiffChangeKind {
        self.change_kind
    }

    /// Added line count.
    #[must_use]
    pub const fn additions(&self) -> u32 {
        self.additions
    }

    /// Deleted line count.
    #[must_use]
    pub const fn deletions(&self) -> u32 {
        self.deletions
    }
}

/// The closed diff entry subject vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgeDiffEntrySubject {
    File,
    Directory,
}

/// The closed diff change-kind vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgeDiffChangeKind {
    Created,
    Modified,
    Deleted,
    Moved,
}

/// The closed diff evidence state vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgeDiffEvidenceState {
    Intent,
    Ready,
    SummaryOnly,
    Abandoned,
    PurgePending,
    Tombstoned,
}

/// The closed diff render-kind vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgeDiffRenderKind {
    Text,
    Binary,
    Oversized,
    Structured,
}

/// The closed diff artifact health vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgeDiffArtifactHealth {
    Available,
    NotApplicable,
    Archived,
    Missing,
    Corrupt,
}

/// One Writer Lease scope entry observed from Forge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeWriterLeaseScopeEntry {
    kind: ForgeWriterLeaseScopeKind,
    path_token: String,
    display_path: String,
    display_path_truncated: bool,
}

impl ForgeWriterLeaseScopeEntry {
    /// The closed scope-kind vocabulary.
    #[must_use]
    pub const fn kind(&self) -> ForgeWriterLeaseScopeKind {
        self.kind
    }

    /// The canonical URL-safe unpadded base64 path token.
    #[must_use]
    pub fn path_token(&self) -> &str {
        &self.path_token
    }

    /// The bounded lossy display path.
    #[must_use]
    pub fn display_path(&self) -> &str {
        &self.display_path
    }

    /// Whether the display path was truncated.
    #[must_use]
    pub const fn display_path_truncated(&self) -> bool {
        self.display_path_truncated
    }
}

/// The closed Writer Lease scope-kind vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgeWriterLeaseScopeKind {
    ExactPath,
    DirectorySubtree,
}

impl ForgeWriterLeaseScopeKind {
    /// The exact wire vocabulary string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExactPath => "exact_path",
            Self::DirectorySubtree => "directory_subtree",
        }
    }
}

/// One full Writer Lease projection observed from Forge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeWriterLeaseProjection {
    lease_id: String,
    owner_activity_id: String,
    agent_id: String,
    owner_session_id: Option<String>,
    revision: i64,
    lease_until_ms: i64,
    observed_at_ms: i64,
    state: ForgeWriterLeaseState,
    status: ForgeWriterLeaseStatus,
    total_scope_count: usize,
    scope_offset: usize,
    next_scope_offset: Option<usize>,
    scope_page_limit: usize,
    scopes: Vec<ForgeWriterLeaseScopeEntry>,
}

impl ForgeWriterLeaseProjection {
    /// The exact lease identity string.
    #[must_use]
    pub fn lease_id(&self) -> &str {
        &self.lease_id
    }

    /// The owner Activity identity string.
    #[must_use]
    pub fn owner_activity_id(&self) -> &str {
        &self.owner_activity_id
    }

    /// The owner Agent identity string.
    #[must_use]
    pub fn agent_id(&self) -> &str {
        &self.agent_id
    }

    /// The optional owner Session identity string.
    #[must_use]
    pub fn owner_session_id(&self) -> Option<&str> {
        self.owner_session_id.as_deref()
    }

    /// The exact lease revision.
    #[must_use]
    pub const fn revision(&self) -> i64 {
        self.revision
    }

    /// Lease expiry in epoch milliseconds.
    #[must_use]
    pub const fn lease_until_ms(&self) -> i64 {
        self.lease_until_ms
    }

    /// Observation time in epoch milliseconds.
    #[must_use]
    pub const fn observed_at_ms(&self) -> i64 {
        self.observed_at_ms
    }

    /// The closed lease state.
    #[must_use]
    pub const fn state(&self) -> ForgeWriterLeaseState {
        self.state
    }

    /// The closed derived lease status.
    #[must_use]
    pub const fn status(&self) -> ForgeWriterLeaseStatus {
        self.status
    }

    /// The status-derived blocking reason, mirroring the native projection.
    #[must_use]
    pub fn blocking_reason(&self) -> Option<&'static str> {
        (self.status == ForgeWriterLeaseStatus::ExpiredRecoveryRequired)
            .then_some("expired_writer_lease")
    }

    /// The status-derived recovery action, mirroring the native projection.
    #[must_use]
    pub fn recovery_action(&self) -> &'static str {
        if self.status == ForgeWriterLeaseStatus::ExpiredRecoveryRequired {
            "writer_lease_recovery_release"
        } else {
            "none"
        }
    }

    /// The total scope-entry count observed by Forge.
    #[must_use]
    pub const fn total_scope_count(&self) -> usize {
        self.total_scope_count
    }

    /// The page offset this observation starts at.
    #[must_use]
    pub const fn scope_offset(&self) -> usize {
        self.scope_offset
    }

    /// The next page offset, when more scope entries exist.
    #[must_use]
    pub const fn next_scope_offset(&self) -> Option<usize> {
        self.next_scope_offset
    }

    /// The page limit applied to this observation.
    #[must_use]
    pub const fn scope_page_limit(&self) -> usize {
        self.scope_page_limit
    }

    /// The bounded scope page.
    #[must_use]
    pub fn scopes(&self) -> &[ForgeWriterLeaseScopeEntry] {
        &self.scopes
    }
}

/// The closed Writer Lease state vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgeWriterLeaseState {
    Active,
    Released,
}

/// The closed derived Writer Lease status vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgeWriterLeaseStatus {
    Active,
    ExpiredRecoveryRequired,
    Released,
}

/// One bounded `command.list` observation through the configured source.
///
/// # Errors
/// Returns a typed invalid-scope, denied, unavailable, stale, or protocol
/// error.
/// Observe one exact Diff (`diff.read`).
///
/// # Errors
/// Returns a typed invalid-scope, denied, unavailable, stale, or protocol
/// error.
/// Observe one Writer Lease projection (`writer_lease.get`).
///
/// # Errors
/// Returns a typed invalid-scope, denied, unavailable, stale, or protocol
/// error.
/// Observe the bounded Writer Lease list (`writer_lease.list`).
///
/// # Errors
/// Returns a typed invalid-scope, denied, unavailable, stale, or protocol
/// error.
/// Whether the `writer_lease.list` readback carries a continuation cursor.
#[must_use]
pub fn writer_lease_list_truncated(readback_next_cursor: Option<&Value>) -> bool {
    readback_next_cursor.is_some_and(|cursor| !cursor.is_null())
}

fn scope_json(scope: &forge_backend::ops::ScopeDto) -> Value {
    json!({
        "workspace_id": scope.workspace_id,
        "workspace_revision": scope.workspace_revision,
        "locator": {
            "location_id": scope.locator.location_id,
            "expected_revision": scope.locator.expected_revision,
            "relative_root": scope.locator.relative_root,
        },
    })
}

impl ForgeSourceHttpClient {
    /// Observe the bounded project command list (`command.list`).
    ///
    /// # Errors
    /// Returns a typed invalid-scope, denied, unavailable, stale, or protocol
    /// error.
    pub async fn command_list(
        &self,
        scope: &forge_backend::ops::ScopeDto,
        activity_id: Uuid,
        runtime_instance_id: Uuid,
        observed_at_ms: i64,
    ) -> Result<ForgeCommandList, ForgeSourceError> {
        if activity_id.is_nil() || runtime_instance_id.is_nil() {
            return Err(ForgeSourceError::InvalidScope);
        }
        let readback = self
            .commands()
            .command(
                CommandOp::CommandList,
                json!({
                    "scope": scope_json(scope),
                    "activity": {
                        "activity_id": activity_id,
                        "runtime_instance_id": runtime_instance_id,
                        "observed_at_ms": observed_at_ms,
                    },
                }),
            )
            .await
            .map_err(map_command_error)?;
        command_list_from_wire(&readback)
    }

    /// Observe one exact Diff (`diff.read`).
    ///
    /// # Errors
    /// Returns a typed invalid-scope, denied, unavailable, stale, or protocol
    /// error.
    pub async fn diff_read(
        &self,
        scope: &forge_backend::ops::ScopeDto,
        diff_id: Uuid,
    ) -> Result<ForgeDiffRead, ForgeSourceError> {
        if diff_id.is_nil() {
            return Err(ForgeSourceError::InvalidScope);
        }
        let readback = self
            .commands()
            .command_classified(
                CommandOp::DiffRead,
                json!({
                    "scope": scope_json(scope),
                    "diff_id": diff_id,
                }),
            )
            .await
            .map_err(|failure| {
                // Forge HTTP collapses the control `CONTROL_DIFF_NOT_FOUND`
                // into the unavailable bucket while retaining the `evidence`
                // control category; that combination is the absent-subject
                // observation the native surface reports.
                if failure.wire_code == forge_backend::ForgeHttpServiceCode::ForgeUnavailable
                    && failure.category == "evidence"
                    && !failure.retryable
                {
                    ForgeSourceError::NotFound
                } else {
                    map_command_error(ForgeHttpClientError::Service {
                        status: failure.status,
                        code: failure.wire_code,
                    })
                }
            })?;
        diff_read_from_wire(&readback)
    }

    /// Observe one Writer Lease projection (`writer_lease.get`).
    ///
    /// # Errors
    /// Returns a typed invalid-scope, denied, unavailable, stale, or protocol
    /// error.
    pub async fn writer_lease_get(
        &self,
        scope: &forge_backend::ops::ScopeDto,
        lease_id: Uuid,
        scope_offset: u64,
        expected_revision: Option<i64>,
    ) -> Result<Option<ForgeWriterLeaseProjection>, ForgeSourceError> {
        if lease_id.is_nil() {
            return Err(ForgeSourceError::InvalidScope);
        }
        let mut payload = json!({
            "scope": scope_json(scope),
            "writer_lease_id": lease_id,
            "scope_offset": scope_offset,
        });
        if let Some(revision) = expected_revision {
            payload
                .as_object_mut()
                .ok_or(ForgeSourceError::Protocol)?
                .insert("expected_revision".to_owned(), json!(revision));
        }
        let readback = self
            .commands()
            .command(CommandOp::WriterLeaseGet, payload)
            .await
            .map_err(map_command_error)?;
        writer_lease_projection_from_wire(&readback, WRITER_LEASE_SCOPE_PAGE_LIMIT)
    }

    /// Observe the bounded Diff list for one exact Activity (`diff.list`).
    ///
    /// # Errors
    /// Returns a typed invalid-scope, denied, unavailable, stale, or protocol
    /// error.
    pub async fn diff_list(
        &self,
        scope: &forge_backend::ops::ScopeDto,
        activity_id: Uuid,
        limit: u32,
    ) -> Result<Vec<ForgeDiffEvidence>, ForgeSourceError> {
        if activity_id.is_nil() || limit == 0 {
            return Err(ForgeSourceError::InvalidScope);
        }
        let readback = self
            .commands()
            .command(
                CommandOp::DiffList,
                json!({
                    "scope": scope_json(scope),
                    "activity_id": activity_id,
                }),
            )
            .await
            .map_err(map_command_error)?;
        let mut diffs = diff_list_from_wire(&readback)?;
        diffs.truncate(limit as usize);
        Ok(diffs)
    }

    /// Observe one Diff doctor observation (`diff.doctor`).
    ///
    /// # Errors
    /// Returns a typed invalid-scope, denied, unavailable, stale, or protocol
    /// error.
    pub async fn diff_doctor(
        &self,
        scope: &forge_backend::ops::ScopeDto,
        diff_id: Uuid,
    ) -> Result<ForgeDiffDoctor, ForgeSourceError> {
        if diff_id.is_nil() {
            return Err(ForgeSourceError::InvalidScope);
        }
        let readback = self
            .commands()
            .command(
                CommandOp::DiffDoctor,
                json!({
                    "scope": scope_json(scope),
                    "diff_id": diff_id,
                }),
            )
            .await
            .map_err(map_command_error)?;
        diff_doctor_from_wire(&readback)
    }

    /// Observe the bounded Writer Lease list (`writer_lease.list`).
    ///
    /// # Errors
    /// Returns a typed invalid-scope, denied, unavailable, stale, or protocol
    /// error.
    pub async fn writer_lease_list(
        &self,
        scope: &forge_backend::ops::ScopeDto,
        limit: u64,
        cursor: Option<Uuid>,
    ) -> Result<(Vec<ForgeWriterLeaseProjection>, bool), ForgeSourceError> {
        let readback = {
            if limit == 0 {
                return Err(ForgeSourceError::InvalidScope);
            }
            let mut payload = json!({
                "scope": scope_json(scope),
                "limit": limit,
            });
            if let Some(cursor) = cursor {
                payload
                    .as_object_mut()
                    .ok_or(ForgeSourceError::Protocol)?
                    .insert("cursor".to_owned(), json!(cursor));
            }
            self.commands()
                .command(CommandOp::WriterLeaseList, payload)
                .await
                .map_err(map_command_error)?
        };
        let leases = readback
            .get("leases")
            .and_then(Value::as_array)
            .ok_or(ForgeSourceError::Protocol)?
            .iter()
            .map(|lease| {
                writer_lease_projection_from_wire(lease, WRITER_LEASE_LIST_SCOPE_PAGE_LIMIT)?
                    .ok_or(ForgeSourceError::Protocol)
            })
            .collect::<Result<Vec<_>, ForgeSourceError>>()?;
        let truncated = writer_lease_list_truncated(readback.get("next_cursor"));
        Ok((leases, truncated))
    }
}

/// Decode the bounded `command.list` readback, recomposing the bare-hex
/// catalog fingerprint into the canonical algorithm-prefixed form.
///
/// # Errors
/// Returns `Protocol` for missing fields or a malformed fingerprint.
pub fn command_list_from_wire(readback: &Value) -> Result<ForgeCommandList, ForgeSourceError> {
    let commands = readback
        .get("commands")
        .and_then(Value::as_array)
        .ok_or(ForgeSourceError::Protocol)?
        .iter()
        .map(|command| {
            Ok(ForgeProjectCommand {
                command_id: required_string(command.get("id"))?,
                class: match command
                    .get("class")
                    .and_then(Value::as_str)
                    .ok_or(ForgeSourceError::Protocol)?
                {
                    "verify" => ForgeProjectCommandClass::Verify,
                    "generated_package" => ForgeProjectCommandClass::GeneratedPackage,
                    _ => return Err(ForgeSourceError::Protocol),
                },
            })
        })
        .collect::<Result<Vec<_>, ForgeSourceError>>()?;
    Ok(ForgeCommandList {
        activity_id: required_uuid_string(readback.get("activity_id"))?,
        catalog_fingerprint: catalog_fingerprint_from_wire(readback.get("catalog_fingerprint"))?,
        commands,
    })
}

/// Decode one bounded `diff.read` readback.
///
/// # Errors
/// Returns `Protocol` for any missing, malformed, or unknown-vocabulary
/// field.
pub fn diff_read_from_wire(readback: &Value) -> Result<ForgeDiffRead, ForgeSourceError> {
    let diff = readback.get("diff").ok_or(ForgeSourceError::Protocol)?;
    let entries = diff
        .get("entries")
        .and_then(Value::as_array)
        .ok_or(ForgeSourceError::Protocol)?
        .iter()
        .map(|entry| {
            Ok(ForgeDiffEntry {
                subject: match entry
                    .get("subject")
                    .and_then(Value::as_str)
                    .ok_or(ForgeSourceError::Protocol)?
                {
                    "file" => ForgeDiffEntrySubject::File,
                    "directory" => ForgeDiffEntrySubject::Directory,
                    _ => return Err(ForgeSourceError::Protocol),
                },
                primary_path_token: required_string(entry.get("primary_path_base64"))?,
                secondary_path_token: optional_string(entry.get("secondary_path_base64"))?,
                change_kind: match entry
                    .get("change_kind")
                    .and_then(Value::as_str)
                    .ok_or(ForgeSourceError::Protocol)?
                {
                    "created" => ForgeDiffChangeKind::Created,
                    "modified" => ForgeDiffChangeKind::Modified,
                    "deleted" => ForgeDiffChangeKind::Deleted,
                    "moved" => ForgeDiffChangeKind::Moved,
                    _ => return Err(ForgeSourceError::Protocol),
                },
                additions: required_u32(entry.get("additions"))?,
                deletions: required_u32(entry.get("deletions"))?,
            })
        })
        .collect::<Result<Vec<_>, ForgeSourceError>>()?;
    Ok(ForgeDiffRead {
        scope: super::file::source_scope_from_wire(readback.get("scope"))?,
        diff_id: required_uuid_string(diff.get("diff_id"))?,
        workspace_id: required_uuid_string(diff.get("workspace_id"))?,
        activity_id: required_uuid_string(diff.get("activity_id"))?,
        change_set_id: required_uuid_string(diff.get("change_set_id"))?,
        origin: match diff
            .get("origin")
            .and_then(Value::as_str)
            .ok_or(ForgeSourceError::Protocol)?
        {
            "file_mutation" => ForgeDiffOrigin::FileMutation,
            "tree_change_set" => ForgeDiffOrigin::TreeChangeSet,
            _ => return Err(ForgeSourceError::Protocol),
        },
        mutation_id: optional_uuid_string(diff.get("origin_mutation_id"))?,
        entries,
        state: match diff
            .get("state")
            .and_then(Value::as_str)
            .ok_or(ForgeSourceError::Protocol)?
        {
            "intent" => ForgeDiffEvidenceState::Intent,
            "ready" => ForgeDiffEvidenceState::Ready,
            "summary_only" => ForgeDiffEvidenceState::SummaryOnly,
            "abandoned" => ForgeDiffEvidenceState::Abandoned,
            "purge_pending" => ForgeDiffEvidenceState::PurgePending,
            "tombstoned" => ForgeDiffEvidenceState::Tombstoned,
            _ => return Err(ForgeSourceError::Protocol),
        },
        render_kind: match diff
            .get("render_kind")
            .and_then(Value::as_str)
            .ok_or(ForgeSourceError::Protocol)?
        {
            "text" => ForgeDiffRenderKind::Text,
            "binary" => ForgeDiffRenderKind::Binary,
            "oversized" => ForgeDiffRenderKind::Oversized,
            "structured" => ForgeDiffRenderKind::Structured,
            _ => return Err(ForgeSourceError::Protocol),
        },
        compressed_hash: optional_string(diff.get("compressed_hash"))?,
        uncompressed_hash: optional_string(diff.get("uncompressed_hash"))?,
        compressed_size_bytes: optional_u64(diff.get("compressed_size_bytes"))?,
        uncompressed_size_bytes: optional_u64(diff.get("uncompressed_size_bytes"))?,
        renderer_version: required_u32(diff.get("renderer_version"))?,
        redaction_policy_version: required_u32(diff.get("redaction_policy_version"))?,
        created_at_ms: required_i64(diff.get("created_at_ms"))?,
        updated_at_ms: required_i64(diff.get("updated_at_ms"))?,
        revision: required_i64(diff.get("revision"))?,
        health: match readback
            .get("health")
            .and_then(Value::as_str)
            .ok_or(ForgeSourceError::Protocol)?
        {
            "available" => ForgeDiffArtifactHealth::Available,
            "not_applicable" => ForgeDiffArtifactHealth::NotApplicable,
            "archived" => ForgeDiffArtifactHealth::Archived,
            "missing" => ForgeDiffArtifactHealth::Missing,
            "corrupt" => ForgeDiffArtifactHealth::Corrupt,
            _ => return Err(ForgeSourceError::Protocol),
        },
        content: optional_string(readback.get("content"))?,
    })
}

/// Decode the canonical scope-entry path token and its bounded display
/// projection.
///
/// # Errors
/// Returns `Protocol` for a missing or non-canonical path token.
fn scope_display(entry: &Value) -> Result<(String, bool), ForgeSourceError> {
    let path = super::file::decode_path_token(&required_string(entry.get("path_base64"))?)?;
    Ok(super::file::display_relative_path(&path))
}

/// Decode one Writer Lease projection row; `found: false` decodes to
/// `None`.
///
/// # Errors
/// Returns `Protocol` for any missing, malformed, or unknown-vocabulary
/// field.
pub fn writer_lease_projection_from_wire(
    readback: &Value,
    scope_limit: usize,
) -> Result<Option<ForgeWriterLeaseProjection>, ForgeSourceError> {
    if readback.get("found").and_then(Value::as_bool) == Some(false) {
        return Ok(None);
    }
    let status = match readback
        .get("status")
        .and_then(Value::as_str)
        .ok_or(ForgeSourceError::Protocol)?
    {
        "active" => ForgeWriterLeaseStatus::Active,
        "expired_recovery_required" => ForgeWriterLeaseStatus::ExpiredRecoveryRequired,
        "released" => ForgeWriterLeaseStatus::Released,
        _ => return Err(ForgeSourceError::Protocol),
    };
    let total_scope_count = usize::try_from(required_i64(readback.get("total_scope_count"))?)
        .map_err(|_| ForgeSourceError::Protocol)?;
    let scope_offset = usize::try_from(required_i64(readback.get("scope_offset"))?)
        .map_err(|_| ForgeSourceError::Protocol)?;
    let page_limit = scope_limit.min(WRITER_LEASE_SCOPE_PAGE_LIMIT);
    let scopes = readback
        .get("scopes")
        .and_then(Value::as_array)
        .ok_or(ForgeSourceError::Protocol)?
        .iter()
        .take(page_limit)
        .map(|entry| {
            Ok(ForgeWriterLeaseScopeEntry {
                kind: match entry
                    .get("kind")
                    .and_then(Value::as_str)
                    .ok_or(ForgeSourceError::Protocol)?
                {
                    "exact_path" => ForgeWriterLeaseScopeKind::ExactPath,
                    "directory_subtree" => ForgeWriterLeaseScopeKind::DirectorySubtree,
                    _ => return Err(ForgeSourceError::Protocol),
                },
                path_token: required_string(entry.get("path_base64"))?,
                display_path: {
                    let (display, _) = scope_display(entry)?;
                    display
                },
                display_path_truncated: scope_display(entry)?.1,
            })
        })
        .collect::<Result<Vec<_>, ForgeSourceError>>()?;
    let next_scope_offset =
        (scope_offset + scopes.len() < total_scope_count).then_some(scope_offset + scopes.len());
    Ok(Some(ForgeWriterLeaseProjection {
        lease_id: required_string(readback.get("writer_lease_id"))?,
        owner_activity_id: required_uuid_string(readback.get("owner_activity_id"))?,
        agent_id: required_uuid_string(readback.get("agent_id"))?,
        owner_session_id: optional_uuid_string(readback.get("owner_session_id"))?,
        revision: required_i64(readback.get("revision"))?,
        lease_until_ms: required_i64(readback.get("lease_until_ms"))?,
        observed_at_ms: required_i64(readback.get("observed_at_ms"))?,
        state: match readback
            .get("state")
            .and_then(Value::as_str)
            .ok_or(ForgeSourceError::Protocol)?
        {
            "active" => ForgeWriterLeaseState::Active,
            "released" => ForgeWriterLeaseState::Released,
            _ => return Err(ForgeSourceError::Protocol),
        },
        status,
        total_scope_count,
        scope_offset,
        next_scope_offset,
        scope_page_limit: page_limit,
        scopes,
    }))
}

/// Recompose the bare 64-char lowercase catalog-hash hex into the canonical
/// algorithm-prefixed form via the typed hash.
///
/// # Errors
/// Returns `Protocol` for a malformed or non-lowercase-hex fingerprint.
pub fn catalog_fingerprint_from_wire(value: Option<&Value>) -> Result<String, ForgeSourceError> {
    let hex = value
        .and_then(Value::as_str)
        .ok_or(ForgeSourceError::Protocol)?;
    let bytes = decode_fixed_lower_hex::<32>(hex).ok_or(ForgeSourceError::Protocol)?;
    Ok(forge_domain::FileContentHash::from_bytes(bytes).to_string())
}

fn decode_fixed_lower_hex<const N: usize>(value: &str) -> Option<[u8; N]> {
    if value.len() != N * 2 {
        return None;
    }
    let mut decoded = [0_u8; N];
    for (index, pair) in value.as_bytes().chunks(2).enumerate() {
        let high = lower_hex_nibble(pair[0])?;
        let low = lower_hex_nibble(pair[1])?;
        decoded[index] = (high << 4) | low;
    }
    Some(decoded)
}

fn lower_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn required_uuid_string(value: Option<&Value>) -> Result<String, ForgeSourceError> {
    let raw = required_string(value)?;
    let parsed = Uuid::parse_str(&raw).map_err(|_| ForgeSourceError::Protocol)?;
    if parsed.is_nil() || parsed.to_string() != raw {
        return Err(ForgeSourceError::Protocol);
    }
    Ok(raw)
}

fn optional_uuid_string(value: Option<&Value>) -> Result<Option<String>, ForgeSourceError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(_) => required_uuid_string(value).map(Some),
    }
}

fn required_string(value: Option<&Value>) -> Result<String, ForgeSourceError> {
    value
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or(ForgeSourceError::Protocol)
}

fn optional_string(value: Option<&Value>) -> Result<Option<String>, ForgeSourceError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.clone())),
        Some(_) => Err(ForgeSourceError::Protocol),
    }
}

fn required_i64(value: Option<&Value>) -> Result<i64, ForgeSourceError> {
    value
        .and_then(Value::as_i64)
        .ok_or(ForgeSourceError::Protocol)
}

fn required_u32(value: Option<&Value>) -> Result<u32, ForgeSourceError> {
    let value = value
        .and_then(Value::as_i64)
        .ok_or(ForgeSourceError::Protocol)?;
    u32::try_from(value).map_err(|_| ForgeSourceError::Protocol)
}

fn required_u64(value: Option<&Value>) -> Result<u64, ForgeSourceError> {
    value
        .and_then(Value::as_u64)
        .ok_or(ForgeSourceError::Protocol)
}

fn optional_u64(value: Option<&Value>) -> Result<Option<u64>, ForgeSourceError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(_) => required_u64(value).map(Some),
    }
}

/// The flattened evidence fields shared by `diff.read` and `diff.list`
/// readbacks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeDiffEvidence {
    pub(super) diff_id: String,
    pub(super) workspace_id: String,
    pub(super) activity_id: String,
    pub(super) change_set_id: String,
    pub(super) origin: ForgeDiffOrigin,
    pub(super) mutation_id: Option<String>,
    pub(super) entries: Vec<ForgeDiffEntry>,
    pub(super) state: ForgeDiffEvidenceState,
    pub(super) render_kind: ForgeDiffRenderKind,
    pub(super) compressed_hash: Option<String>,
    pub(super) uncompressed_hash: Option<String>,
    pub(super) compressed_size_bytes: Option<u64>,
    pub(super) uncompressed_size_bytes: Option<u64>,
    pub(super) renderer_version: u32,
    pub(super) redaction_policy_version: u32,
    pub(super) created_at_ms: i64,
    pub(super) updated_at_ms: i64,
    pub(super) revision: i64,
}

impl ForgeDiffEvidence {
    /// The exact Diff identity string.
    #[must_use]
    pub fn diff_id(&self) -> &str {
        &self.diff_id
    }

    /// The exact Workspace identity string.
    #[must_use]
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }

    /// The exact Activity identity string.
    #[must_use]
    pub fn activity_id(&self) -> &str {
        &self.activity_id
    }

    /// The exact `ChangeSet` identity string.
    #[must_use]
    pub fn change_set_id(&self) -> &str {
        &self.change_set_id
    }

    /// The closed diff-origin vocabulary.
    #[must_use]
    pub const fn origin(&self) -> ForgeDiffOrigin {
        self.origin
    }

    /// The optional origin mutation identity.
    #[must_use]
    pub fn mutation_id(&self) -> Option<&str> {
        self.mutation_id.as_deref()
    }

    /// The diff entry list.
    #[must_use]
    pub fn entries(&self) -> &[ForgeDiffEntry] {
        &self.entries
    }

    /// The closed evidence state.
    #[must_use]
    pub const fn state(&self) -> ForgeDiffEvidenceState {
        self.state
    }

    /// The closed render kind.
    #[must_use]
    pub const fn render_kind(&self) -> ForgeDiffRenderKind {
        self.render_kind
    }

    /// The optional compressed artifact hash.
    #[must_use]
    pub fn compressed_hash(&self) -> Option<&str> {
        self.compressed_hash.as_deref()
    }

    /// The optional uncompressed artifact hash.
    #[must_use]
    pub fn uncompressed_hash(&self) -> Option<&str> {
        self.uncompressed_hash.as_deref()
    }

    /// The optional compressed artifact size.
    #[must_use]
    pub const fn compressed_size_bytes(&self) -> Option<u64> {
        self.compressed_size_bytes
    }

    /// The optional uncompressed artifact size.
    #[must_use]
    pub const fn uncompressed_size_bytes(&self) -> Option<u64> {
        self.uncompressed_size_bytes
    }

    /// The renderer version.
    #[must_use]
    pub const fn renderer_version(&self) -> u32 {
        self.renderer_version
    }

    /// The redaction policy version.
    #[must_use]
    pub const fn redaction_policy_version(&self) -> u32 {
        self.redaction_policy_version
    }

    /// Creation time in epoch milliseconds.
    #[must_use]
    pub const fn created_at_ms(&self) -> i64 {
        self.created_at_ms
    }

    /// Last update time in epoch milliseconds.
    #[must_use]
    pub const fn updated_at_ms(&self) -> i64 {
        self.updated_at_ms
    }

    /// The exact Diff revision.
    #[must_use]
    pub const fn revision(&self) -> i64 {
        self.revision
    }
}

/// Decode the shared evidence fields of a `diff.read`/`diff.list` entry.
///
/// # Errors
/// Returns `Protocol` for any missing, malformed, or unknown-vocabulary
/// field.
pub fn diff_evidence_from_wire(value: &Value) -> Result<ForgeDiffEvidence, ForgeSourceError> {
    let entries = value
        .get("entries")
        .and_then(Value::as_array)
        .ok_or(ForgeSourceError::Protocol)?
        .iter()
        .map(|entry| {
            Ok(ForgeDiffEntry {
                subject: match entry
                    .get("subject")
                    .and_then(Value::as_str)
                    .ok_or(ForgeSourceError::Protocol)?
                {
                    "file" => ForgeDiffEntrySubject::File,
                    "directory" => ForgeDiffEntrySubject::Directory,
                    _ => return Err(ForgeSourceError::Protocol),
                },
                primary_path_token: required_string(entry.get("primary_path_base64"))?,
                secondary_path_token: optional_string(entry.get("secondary_path_base64"))?,
                change_kind: match entry
                    .get("change_kind")
                    .and_then(Value::as_str)
                    .ok_or(ForgeSourceError::Protocol)?
                {
                    "created" => ForgeDiffChangeKind::Created,
                    "modified" => ForgeDiffChangeKind::Modified,
                    "deleted" => ForgeDiffChangeKind::Deleted,
                    "moved" => ForgeDiffChangeKind::Moved,
                    _ => return Err(ForgeSourceError::Protocol),
                },
                additions: required_u32(entry.get("additions"))?,
                deletions: required_u32(entry.get("deletions"))?,
            })
        })
        .collect::<Result<Vec<_>, ForgeSourceError>>()?;
    Ok(ForgeDiffEvidence {
        diff_id: required_uuid_string(value.get("diff_id"))?,
        workspace_id: required_uuid_string(value.get("workspace_id"))?,
        activity_id: required_uuid_string(value.get("activity_id"))?,
        change_set_id: required_uuid_string(value.get("change_set_id"))?,
        origin: match value
            .get("origin")
            .and_then(Value::as_str)
            .ok_or(ForgeSourceError::Protocol)?
        {
            "file_mutation" => ForgeDiffOrigin::FileMutation,
            "tree_change_set" => ForgeDiffOrigin::TreeChangeSet,
            _ => return Err(ForgeSourceError::Protocol),
        },
        mutation_id: optional_uuid_string(value.get("origin_mutation_id"))?,
        entries,
        state: match value
            .get("state")
            .and_then(Value::as_str)
            .ok_or(ForgeSourceError::Protocol)?
        {
            "intent" => ForgeDiffEvidenceState::Intent,
            "ready" => ForgeDiffEvidenceState::Ready,
            "summary_only" => ForgeDiffEvidenceState::SummaryOnly,
            "abandoned" => ForgeDiffEvidenceState::Abandoned,
            "purge_pending" => ForgeDiffEvidenceState::PurgePending,
            "tombstoned" => ForgeDiffEvidenceState::Tombstoned,
            _ => return Err(ForgeSourceError::Protocol),
        },
        render_kind: match value
            .get("render_kind")
            .and_then(Value::as_str)
            .ok_or(ForgeSourceError::Protocol)?
        {
            "text" => ForgeDiffRenderKind::Text,
            "binary" => ForgeDiffRenderKind::Binary,
            "oversized" => ForgeDiffRenderKind::Oversized,
            "structured" => ForgeDiffRenderKind::Structured,
            _ => return Err(ForgeSourceError::Protocol),
        },
        compressed_hash: optional_string(value.get("compressed_hash"))?,
        uncompressed_hash: optional_string(value.get("uncompressed_hash"))?,
        compressed_size_bytes: optional_u64(value.get("compressed_size_bytes"))?,
        uncompressed_size_bytes: optional_u64(value.get("uncompressed_size_bytes"))?,
        renderer_version: required_u32(value.get("renderer_version"))?,
        redaction_policy_version: required_u32(value.get("redaction_policy_version"))?,
        created_at_ms: required_i64(value.get("created_at_ms"))?,
        updated_at_ms: required_i64(value.get("updated_at_ms"))?,
        revision: required_i64(value.get("revision"))?,
    })
}

/// Decode one bounded `diff.list` readback.
///
/// # Errors
/// Returns `Protocol` for any missing or malformed field.
pub fn diff_list_from_wire(readback: &Value) -> Result<Vec<ForgeDiffEvidence>, ForgeSourceError> {
    readback
        .get("diffs")
        .and_then(Value::as_array)
        .ok_or(ForgeSourceError::Protocol)?
        .iter()
        .map(diff_evidence_from_wire)
        .collect()
}

/// One bounded `diff.doctor` observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeDiffDoctor {
    pub(super) diff_id: String,
    pub(super) health: ForgeDiffArtifactHealth,
    pub(super) evidence: ForgeDiffEvidence,
}

impl ForgeDiffDoctor {
    /// The exact Diff identity string.
    #[must_use]
    pub fn diff_id(&self) -> &str {
        &self.diff_id
    }

    /// The closed artifact health vocabulary.
    #[must_use]
    pub const fn health(&self) -> ForgeDiffArtifactHealth {
        self.health
    }

    /// The evidence observation.
    #[must_use]
    pub const fn evidence(&self) -> &ForgeDiffEvidence {
        &self.evidence
    }
}

/// Decode one bounded `diff.doctor` readback.
///
/// # Errors
/// Returns `Protocol` for any missing or malformed field.
pub fn diff_doctor_from_wire(readback: &Value) -> Result<ForgeDiffDoctor, ForgeSourceError> {
    Ok(ForgeDiffDoctor {
        diff_id: required_uuid_string(readback.get("diff_id"))?,
        health: match readback
            .get("health")
            .and_then(Value::as_str)
            .ok_or(ForgeSourceError::Protocol)?
        {
            "available" => ForgeDiffArtifactHealth::Available,
            "not_applicable" => ForgeDiffArtifactHealth::NotApplicable,
            "archived" => ForgeDiffArtifactHealth::Archived,
            "missing" => ForgeDiffArtifactHealth::Missing,
            "corrupt" => ForgeDiffArtifactHealth::Corrupt,
            _ => return Err(ForgeSourceError::Protocol),
        },
        evidence: diff_evidence_from_wire(
            readback.get("evidence").ok_or(ForgeSourceError::Protocol)?,
        )?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn command_readback() -> Value {
        json!({
            "activity_id": Uuid::from_u128(0xB801),
            "catalog_fingerprint": "00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff",
            "commands": [
                {"id": "cmd-verify", "class": "verify"},
                {"id": "cmd-pkg", "class": "generated_package"},
            ],
        })
    }

    #[test]
    fn command_list_recomposes_the_canonical_fingerprint() -> TestResult {
        let list = command_list_from_wire(&command_readback())?;
        assert_eq!(list.activity_id(), Uuid::from_u128(0xB801).to_string());
        assert!(
            list.catalog_fingerprint().ends_with("00ff00ff")
                && list.catalog_fingerprint().contains(':')
        );
        assert_eq!(list.commands().len(), 2);
        assert_eq!(list.commands()[0].class(), ForgeProjectCommandClass::Verify);
        assert_eq!(
            list.commands()[1].class(),
            ForgeProjectCommandClass::GeneratedPackage
        );

        let mut unknown_class = command_readback();
        unknown_class["commands"][0]["class"] = json!("deploy");
        assert_eq!(
            command_list_from_wire(&unknown_class),
            Err(ForgeSourceError::Protocol)
        );

        let mut bad_hex = command_readback();
        bad_hex["catalog_fingerprint"] =
            json!("ZZFF00FF00FF00FF00FF00FF00FF00FF00FF00FF00FF00FF00FF00FF00FF00FF");
        assert_eq!(
            command_list_from_wire(&bad_hex),
            Err(ForgeSourceError::Protocol)
        );
        Ok(())
    }

    fn diff_readback() -> Value {
        json!({
            "scope": {
                "workspace_id": Uuid::from_u128(0xB802),
                "workspace_revision": 8,
                "locator": {
                    "location_id": Uuid::from_u128(0xB803),
                    "expected_revision": 5,
                    "relative_root": "demo-ws",
                },
            },
            "diff": {
                "diff_id": Uuid::from_u128(0xB804),
                "workspace_id": Uuid::from_u128(0xB802),
                "activity_id": Uuid::from_u128(0xB805),
                "change_set_id": Uuid::from_u128(0xB806),
                "origin": "tree_change_set",
                "origin_mutation_id": null,
                "entries": [
                    {"subject": "file", "primary_path_base64": "bGliLnJz",
                     "secondary_path_base64": null, "change_kind": "modified",
                     "additions": 4, "deletions": 1}
                ],
                "state": "ready",
                "render_kind": "text",
                "compressed_hash": null,
                "uncompressed_hash": "sha256:abcd",
                "compressed_size_bytes": null,
                "uncompressed_size_bytes": 120,
                "renderer_version": 3,
                "redaction_policy_version": 1,
                "created_at_ms": 10,
                "updated_at_ms": 20,
                "revision": 2,
            },
            "health": "available",
            "content": "diff text",
        })
    }

    #[test]
    fn diff_read_preserves_entries_hashes_and_closed_vocabularies() -> TestResult {
        let read = diff_read_from_wire(&diff_readback())?;
        assert_eq!(read.diff_id(), Uuid::from_u128(0xB804).to_string());
        assert_eq!(read.origin(), ForgeDiffOrigin::TreeChangeSet);
        assert_eq!(read.entries().len(), 1);
        assert_eq!(read.entries()[0].subject(), ForgeDiffEntrySubject::File);
        assert_eq!(
            read.entries()[0].change_kind(),
            ForgeDiffChangeKind::Modified
        );
        assert_eq!(read.entries()[0].primary_path_token(), "bGliLnJz");
        assert_eq!(read.state(), ForgeDiffEvidenceState::Ready);
        assert_eq!(read.health(), ForgeDiffArtifactHealth::Available);
        assert_eq!(read.content(), Some("diff text"));
        assert_eq!(read.uncompressed_size_bytes(), Some(120));

        let mut unknown_state = diff_readback();
        unknown_state["diff"]["state"] = json!("vibing");
        assert_eq!(
            diff_read_from_wire(&unknown_state),
            Err(ForgeSourceError::Protocol)
        );

        let mut missing_health = diff_readback();
        missing_health
            .as_object_mut()
            .ok_or("object")?
            .remove("health");
        assert_eq!(
            diff_read_from_wire(&missing_health),
            Err(ForgeSourceError::Protocol)
        );
        Ok(())
    }

    fn lease_readback() -> Value {
        json!({
            "writer_lease_id": Uuid::from_u128(0xB807),
            "owner_activity_id": Uuid::from_u128(0xB808),
            "agent_id": Uuid::from_u128(0xB809),
            "owner_session_id": null,
            "revision": 6,
            "lease_until_ms": 200,
            "observed_at_ms": 100,
            "state": "active",
            "status": "expired_recovery_required",
            "total_scope_count": 3,
            "scope_offset": 2,
            "scopes": [
                {"kind": "exact_path", "path_base64": "bGliLnJz"}
            ],
        })
    }

    #[test]
    fn diff_list_and_doctor_readbacks_preserve_exact_projections() -> TestResult {
        let list_readback = json!({
            "diffs": [diff_readback()["diff"]],
            "diff_count": 1,
            "scope": {
                "workspace_id": Uuid::from_u128(0xB802),
                "workspace_revision": 8,
                "locator": {
                    "location_id": Uuid::from_u128(0xB803),
                    "expected_revision": 5,
                    "relative_root": "demo-ws",
                },
            },
            "activity_id": Uuid::from_u128(0xB805),
        });
        let rows = diff_list_from_wire(&list_readback)?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].diff_id(), Uuid::from_u128(0xB804).to_string());

        let mut non_evidence = list_readback.clone();
        non_evidence["diffs"][0] = json!({"found": false});
        assert_eq!(
            diff_list_from_wire(&non_evidence),
            Err(ForgeSourceError::Protocol)
        );

        let doctor_readback = json!({
            "diff_id": Uuid::from_u128(0xB804),
            "health": "archived",
            "evidence": diff_readback()["diff"],
        });
        let doctor = diff_doctor_from_wire(&doctor_readback)?;
        assert_eq!(doctor.diff_id(), Uuid::from_u128(0xB804).to_string());
        assert_eq!(doctor.health(), ForgeDiffArtifactHealth::Archived);
        assert_eq!(
            doctor.evidence().diff_id(),
            Uuid::from_u128(0xB804).to_string()
        );

        let mut unknown_health = doctor_readback;
        unknown_health["health"] = json!("perfect");
        assert_eq!(
            diff_doctor_from_wire(&unknown_health),
            Err(ForgeSourceError::Protocol)
        );
        Ok(())
    }

    #[test]
    fn lease_projection_derives_status_hints_and_page_bounds() -> TestResult {
        let lease =
            writer_lease_projection_from_wire(&lease_readback(), WRITER_LEASE_SCOPE_PAGE_LIMIT)?
                .ok_or("expected a lease")?;
        assert_eq!(lease.lease_id(), Uuid::from_u128(0xB807).to_string());
        assert_eq!(
            lease.status(),
            ForgeWriterLeaseStatus::ExpiredRecoveryRequired
        );
        assert_eq!(lease.blocking_reason(), Some("expired_writer_lease"));
        assert_eq!(lease.recovery_action(), "writer_lease_recovery_release");
        assert_eq!(lease.total_scope_count(), 3);
        assert_eq!(lease.scope_offset(), 2);
        assert_eq!(lease.scopes().len(), 1);
        assert_eq!(
            lease.scopes()[0].kind(),
            ForgeWriterLeaseScopeKind::ExactPath
        );
        assert_eq!(lease.scopes()[0].display_path(), "lib.rs");
        assert_eq!(lease.next_scope_offset(), None);

        let not_found = json!({"found": false});
        assert_eq!(
            writer_lease_projection_from_wire(&not_found, WRITER_LEASE_SCOPE_PAGE_LIMIT)?,
            None
        );

        let mut unknown_status = lease_readback();
        unknown_status["status"] = json!("pending");
        assert_eq!(
            writer_lease_projection_from_wire(&unknown_status, WRITER_LEASE_SCOPE_PAGE_LIMIT),
            Err(ForgeSourceError::Protocol)
        );
        Ok(())
    }

    #[test]
    fn lease_list_truncation_mirrors_cursor_presence() {
        assert!(writer_lease_list_truncated(Some(&json!("cursor-1"))));
        assert!(writer_lease_list_truncated(Some(&json!({}))));
        assert!(!writer_lease_list_truncated(Some(&Value::Null)));
        assert!(!writer_lease_list_truncated(None));
    }
}
