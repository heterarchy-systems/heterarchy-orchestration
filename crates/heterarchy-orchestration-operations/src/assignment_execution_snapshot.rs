//! Pre-spawn immutable `AssignmentExecutionSnapshot` and durable Launch Ledger state machine.
//!
//! Resolves and freezes an execution attempt's engine, model, reasoning, role-prompt digest,
//! worktree path, lease, secret-free MCP grant window, permissions, and timeouts against the
//! authoritative `AgentProfile` `ExecutionPreference` before spawning any worker process, and
//! tracks single-spawn process ownership and crash-safe boot reconciliation in SQLite.

pub(crate) mod schema;
mod sql;

use std::path::{Component, Path, PathBuf};

use forge_domain::{
    ActivityId, AgentAssignmentId, DagNodeId, DagRevisionNumber, DirectTokenId, GoalId, WorkspaceId,
};
use heterarchy_orchestration_sessions::{
    AgentProfileId, AgentProfileRevision, CliEngine, ExecutionPreference, RolePromptDigest,
};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::sqlite_codecs::{
    decode_bool_flag, decode_non_nil_uuid, decode_non_zero_digest, decode_positive_u64,
    finalize_operation_commit, hash_str,
};
use crate::{OperationsCatalog, OperationsError};

/// Maximum timeout duration accepted for startup, execution, or termination grace (`24h`).
pub const MAX_EXECUTION_TIMEOUT_MS: u64 = 86_400_000;
/// Maximum UTF-8 byte length of a validated [`ForgeWorktreeRef`] path.
pub const MAX_WORKTREE_PATH_BYTES: usize = 4_096;
/// Maximum UTF-8 byte length of `ProcessOwnershipIdentity` `start_time` or `boot_id`.
pub const MAX_PROCESS_OWNERSHIP_FIELD_BYTES: usize = 256;
/// Maximum rows reconciled in one bounded boot-recovery query.
const MAX_BOOT_RECONCILE_ROWS: i64 = 16_384;

const FORBIDDEN_SSD_RUSTBUILD_PREFIX: &str = "/Volumes/256.SSD/RustBuild";
const SNAPSHOT_DIGEST_DOMAIN: &[u8] =
    b"heterarchy.orchestration.assignment_execution_snapshot.v1\0";

use sql::{
    EXISTS_SNAPSHOT_OR_ATTEMPT_SQL, INSERT_EXECUTION_SNAPSHOT_SQL,
    INSERT_INITIAL_LAUNCH_LEDGER_SQL, SELECT_IN_FLIGHT_LEDGERS_ALL_SQL,
    SELECT_IN_FLIGHT_LEDGERS_BY_WORKSPACE_SQL, SELECT_LEDGER_BY_ATTEMPT_SQL,
    SELECT_LEDGER_BY_SNAPSHOT_ID_SQL, SELECT_SNAPSHOT_BY_ATTEMPT_SQL,
    SELECT_SNAPSHOT_BY_REQUEST_ID_SQL, UPDATE_LAUNCH_LEDGER_STATE_SQL,
};

/// Fail-closed validation error returned while constructing or resolving an [`AssignmentExecutionSnapshot`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SnapshotValidationError {
    #[error("AgentProfile has no configured ExecutionPreference")]
    MissingExecutionPreference,
    #[error("snapshot profile_revision does not match current AgentProfileRevision")]
    ProfileRevisionMismatch,
    #[error("resolved_engine does not match AgentProfile ExecutionPreference")]
    EngineMismatch,
    #[error("resolved_model does not match AgentProfile ExecutionPreference")]
    ModelMismatch,
    #[error("resolved_reasoning does not match AgentProfile ExecutionPreference")]
    ReasoningMismatch,
    #[error("role_prompt_digest does not match AgentProfile ExecutionPreference")]
    RolePromptMismatch,
    #[error("resolved_engine and resolved_model are provider-incompatible")]
    IncompatibleEngineModel,
    #[error("invalid Forge worktree path: {0}")]
    InvalidWorktreePath(&'static str),
    #[error("worktree path under /Volumes/256.SSD/RustBuild is forbidden")]
    ForbiddenSsdBuildWorktree,
    #[error("invalid execution snapshot identity: {0}")]
    InvalidIdentity(&'static str),
    #[error("invalid MCP grant validity window")]
    InvalidGrantWindow,
    #[error("invalid execution timeout: {0}")]
    InvalidTimeout(&'static str),
}

/// Validated canonical UTF-8 absolute path to a Forge node worktree outside the SSD build cache.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ForgeWorktreeRef(String);

impl ForgeWorktreeRef {
    /// Validate and construct a [`ForgeWorktreeRef`].
    ///
    /// # Errors
    /// Rejects non-UTF-8 paths, relative paths, root `"/"`, empty/`CurDir`/`ParentDir` segments,
    /// control characters, paths exceeding `4096` bytes, and any path inside `/Volumes/256.SSD/RustBuild`.
    pub fn new(path: PathBuf) -> Result<Self, SnapshotValidationError> {
        let raw = path
            .into_os_string()
            .into_string()
            .map_err(|_| SnapshotValidationError::InvalidWorktreePath("non-UTF-8 worktree path"))?;
        if raw.is_empty() || raw.len() > MAX_WORKTREE_PATH_BYTES {
            return Err(SnapshotValidationError::InvalidWorktreePath(
                "worktree path length out of bounds",
            ));
        }
        if raw.chars().any(char::is_control) {
            return Err(SnapshotValidationError::InvalidWorktreePath(
                "worktree path contains control characters",
            ));
        }
        if !Path::new(&raw).is_absolute() {
            return Err(SnapshotValidationError::InvalidWorktreePath(
                "worktree path must be absolute",
            ));
        }
        let normalized = if raw.len() > 1 {
            raw.strip_suffix('/').unwrap_or(&raw)
        } else {
            raw.as_str()
        };
        if normalized == "/" {
            return Err(SnapshotValidationError::InvalidWorktreePath(
                "worktree path cannot be filesystem root",
            ));
        }
        validate_worktree_segments(normalized)?;
        if is_forbidden_ssd_worktree(Path::new(normalized)) {
            return Err(SnapshotValidationError::ForbiddenSsdBuildWorktree);
        }
        Ok(Self(normalized.to_owned()))
    }

    #[must_use]
    pub fn as_path(&self) -> &Path {
        Path::new(&self.0)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn validate_worktree_segments(normalized: &str) -> Result<(), SnapshotValidationError> {
    let tail = normalized
        .strip_prefix('/')
        .ok_or(SnapshotValidationError::InvalidWorktreePath(
            "worktree path must start with '/'",
        ))?;
    for segment in tail.split('/') {
        if segment.is_empty() {
            return Err(SnapshotValidationError::InvalidWorktreePath(
                "worktree path contains empty segment",
            ));
        }
        if segment == "." {
            return Err(SnapshotValidationError::InvalidWorktreePath(
                "worktree path contains CurDir segment",
            ));
        }
        if segment == ".." {
            return Err(SnapshotValidationError::InvalidWorktreePath(
                "worktree path contains ParentDir segment",
            ));
        }
    }
    for component in Path::new(normalized).components() {
        if matches!(component, Component::CurDir | Component::ParentDir) {
            return Err(SnapshotValidationError::InvalidWorktreePath(
                "worktree path contains relative traversal component",
            ));
        }
    }
    Ok(())
}

fn is_forbidden_ssd_worktree(path: &Path) -> bool {
    if path.starts_with(FORBIDDEN_SSD_RUSTBUILD_PREFIX) {
        return true;
    }
    let mut normal = path.components().filter_map(|component| match component {
        Component::Normal(part) => part.to_str(),
        _ => None,
    });
    matches!(
        (normal.next(), normal.next(), normal.next()),
        (Some(first), Some(second), Some(third))
            if first.eq_ignore_ascii_case("Volumes")
                && second.eq_ignore_ascii_case("256.SSD")
                && third.eq_ignore_ascii_case("RustBuild")
    )
}

/// Immutable lease witness captured in an [`AssignmentExecutionSnapshot`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseRef {
    lease_id: Uuid,
    lease_revision: u64,
    expires_at_ms: i64,
}

impl LeaseRef {
    /// Construct a validated [`LeaseRef`].
    ///
    /// # Errors
    /// Rejects nil `lease_id`, zero or non-`i64` `lease_revision`, and negative `expires_at_ms`.
    pub fn new(
        lease_id: Uuid,
        lease_revision: u64,
        expires_at_ms: i64,
    ) -> Result<Self, SnapshotValidationError> {
        if lease_id.is_nil() {
            return Err(SnapshotValidationError::InvalidIdentity(
                "lease_id cannot be nil",
            ));
        }
        if lease_revision == 0 || i64::try_from(lease_revision).is_err() {
            return Err(SnapshotValidationError::InvalidIdentity(
                "lease_revision must be positive",
            ));
        }
        if expires_at_ms < 0 {
            return Err(SnapshotValidationError::InvalidTimeout(
                "lease expires_at_ms must be non-negative",
            ));
        }
        Ok(Self {
            lease_id,
            lease_revision,
            expires_at_ms,
        })
    }

    #[must_use]
    pub const fn lease_id(self) -> Uuid {
        self.lease_id
    }

    #[must_use]
    pub const fn lease_revision(self) -> u64 {
        self.lease_revision
    }

    #[must_use]
    pub const fn expires_at_ms(self) -> i64 {
        self.expires_at_ms
    }
}

/// Secret-free MCP direct-token grant metadata captured in an [`AssignmentExecutionSnapshot`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpGrant {
    token_id: DirectTokenId,
    issued_at_ms: i64,
    expires_at_ms: i64,
}

impl McpGrant {
    /// Construct a validated secret-free [`McpGrant`].
    ///
    /// # Errors
    /// Rejects nil `token_id`, negative `issued_at_ms`, or `expires_at_ms <= issued_at_ms`.
    pub fn new(
        token_id: DirectTokenId,
        issued_at_ms: i64,
        expires_at_ms: i64,
    ) -> Result<Self, SnapshotValidationError> {
        if token_id.into_uuid().is_nil() {
            return Err(SnapshotValidationError::InvalidIdentity(
                "mcp grant token_id cannot be nil",
            ));
        }
        if issued_at_ms < 0 || expires_at_ms <= issued_at_ms {
            return Err(SnapshotValidationError::InvalidGrantWindow);
        }
        Ok(Self {
            token_id,
            issued_at_ms,
            expires_at_ms,
        })
    }

    #[must_use]
    pub const fn token_id(self) -> DirectTokenId {
        self.token_id
    }

    #[must_use]
    pub const fn issued_at_ms(self) -> i64 {
        self.issued_at_ms
    }

    #[must_use]
    pub const fn expires_at_ms(self) -> i64 {
        self.expires_at_ms
    }
}

/// Explicit sandbox permission flags frozen for one execution attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionPermissions {
    bits: u8,
}

impl ExecutionPermissions {
    const WORKSPACE_WRITE_BIT: u8 = 1 << 0;
    const LOCAL_SHELL_BIT: u8 = 1 << 1;
    const NETWORK_BIT: u8 = 1 << 2;

    #[must_use]
    pub const fn new(
        allow_workspace_write: bool,
        allow_local_shell: bool,
        allow_network: bool,
    ) -> Self {
        let mut bits = 0_u8;
        if allow_workspace_write {
            bits |= Self::WORKSPACE_WRITE_BIT;
        }
        if allow_local_shell {
            bits |= Self::LOCAL_SHELL_BIT;
        }
        if allow_network {
            bits |= Self::NETWORK_BIT;
        }
        Self { bits }
    }

    #[must_use]
    pub const fn allow_workspace_write(self) -> bool {
        (self.bits & Self::WORKSPACE_WRITE_BIT) != 0
    }

    #[must_use]
    pub const fn allow_local_shell(self) -> bool {
        (self.bits & Self::LOCAL_SHELL_BIT) != 0
    }

    #[must_use]
    pub const fn allow_network(self) -> bool {
        (self.bits & Self::NETWORK_BIT) != 0
    }
}

/// Bounded startup, execution, and termination grace timeouts in milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionTimeouts {
    startup_timeout: u64,
    execution_timeout: u64,
    termination_grace: u64,
}

impl ExecutionTimeouts {
    /// Construct validated [`ExecutionTimeouts`], requiring each value in `1..=86_400_000`.
    ///
    /// # Errors
    /// Returns [`SnapshotValidationError::InvalidTimeout`] when any timeout is `0` or exceeds `86_400_000`.
    pub fn new(
        startup_timeout_ms: u64,
        execution_timeout_ms: u64,
        termination_grace_ms: u64,
    ) -> Result<Self, SnapshotValidationError> {
        if !(1..=MAX_EXECUTION_TIMEOUT_MS).contains(&startup_timeout_ms) {
            return Err(SnapshotValidationError::InvalidTimeout(
                "startup_timeout_ms must be within 1..=86_400_000",
            ));
        }
        if !(1..=MAX_EXECUTION_TIMEOUT_MS).contains(&execution_timeout_ms) {
            return Err(SnapshotValidationError::InvalidTimeout(
                "execution_timeout_ms must be within 1..=86_400_000",
            ));
        }
        if !(1..=MAX_EXECUTION_TIMEOUT_MS).contains(&termination_grace_ms) {
            return Err(SnapshotValidationError::InvalidTimeout(
                "termination_grace_ms must be within 1..=86_400_000",
            ));
        }
        Ok(Self {
            startup_timeout: startup_timeout_ms,
            execution_timeout: execution_timeout_ms,
            termination_grace: termination_grace_ms,
        })
    }

    #[must_use]
    pub const fn startup_timeout_ms(self) -> u64 {
        self.startup_timeout
    }

    #[must_use]
    pub const fn execution_timeout_ms(self) -> u64 {
        self.execution_timeout
    }

    #[must_use]
    pub const fn termination_grace_ms(self) -> u64 {
        self.termination_grace
    }
}

/// Exact 6-tuple identifying a single execution attempt across Workspace, Goal, DAG revision, Node, and Assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExecutionAttemptKey {
    workspace_id: WorkspaceId,
    goal_id: GoalId,
    dag_revision: DagRevisionNumber,
    node_id: DagNodeId,
    assignment_id: AgentAssignmentId,
    attempt: u32,
}

impl ExecutionAttemptKey {
    /// Construct a validated [`ExecutionAttemptKey`].
    ///
    /// # Errors
    /// Rejects nil UUIDs, non-positive `dag_revision`, or `attempt == 0`.
    pub fn new(
        workspace_id: WorkspaceId,
        goal_id: GoalId,
        dag_revision: DagRevisionNumber,
        node_id: DagNodeId,
        assignment_id: AgentAssignmentId,
        attempt: u32,
    ) -> Result<Self, SnapshotValidationError> {
        if workspace_id.into_uuid().is_nil() {
            return Err(SnapshotValidationError::InvalidIdentity(
                "workspace_id cannot be nil",
            ));
        }
        if goal_id.into_uuid().is_nil() {
            return Err(SnapshotValidationError::InvalidIdentity(
                "goal_id cannot be nil",
            ));
        }
        if dag_revision.get() < 1 {
            return Err(SnapshotValidationError::InvalidIdentity(
                "dag_revision must be positive",
            ));
        }
        if node_id.into_uuid().is_nil() {
            return Err(SnapshotValidationError::InvalidIdentity(
                "node_id cannot be nil",
            ));
        }
        if assignment_id.into_uuid().is_nil() {
            return Err(SnapshotValidationError::InvalidIdentity(
                "assignment_id cannot be nil",
            ));
        }
        if attempt == 0 {
            return Err(SnapshotValidationError::InvalidIdentity(
                "attempt must be at least 1",
            ));
        }
        Ok(Self {
            workspace_id,
            goal_id,
            dag_revision,
            node_id,
            assignment_id,
            attempt,
        })
    }

    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn goal_id(self) -> GoalId {
        self.goal_id
    }

    #[must_use]
    pub const fn dag_revision(self) -> DagRevisionNumber {
        self.dag_revision
    }

    #[must_use]
    pub const fn node_id(self) -> DagNodeId {
        self.node_id
    }

    #[must_use]
    pub const fn assignment_id(self) -> AgentAssignmentId {
        self.assignment_id
    }

    #[must_use]
    pub const fn attempt(self) -> u32 {
        self.attempt
    }
}

/// Unvalidated input bundle passed to [`AssignmentExecutionSnapshot::resolve_from_profile`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentExecutionSnapshotParts {
    pub snapshot_id: Uuid,
    pub attempt_key: ExecutionAttemptKey,
    pub activity_id: ActivityId,
    pub profile_id: AgentProfileId,
    pub profile_revision: AgentProfileRevision,
    pub resolved_engine: CliEngine,
    pub resolved_model: String,
    pub resolved_reasoning: Option<String>,
    pub role_prompt_digest: Option<RolePromptDigest>,
    pub worktree: ForgeWorktreeRef,
    pub lease: LeaseRef,
    pub mcp_grant: Option<McpGrant>,
    pub permissions: ExecutionPermissions,
    pub timeouts: ExecutionTimeouts,
    pub committed_at_ms: i64,
}

/// Immutable pre-spawn execution contract frozen before launching an agent worker process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentExecutionSnapshot {
    snapshot_id: Uuid,
    attempt_key: ExecutionAttemptKey,
    activity_id: ActivityId,
    profile_id: AgentProfileId,
    profile_revision: AgentProfileRevision,
    resolved_engine: CliEngine,
    resolved_model: String,
    resolved_reasoning: Option<String>,
    role_prompt_digest: Option<RolePromptDigest>,
    worktree: ForgeWorktreeRef,
    lease: LeaseRef,
    mcp_grant: Option<McpGrant>,
    permissions: ExecutionPermissions,
    timeouts: ExecutionTimeouts,
    committed_at_ms: i64,
}

impl AssignmentExecutionSnapshot {
    /// Validate and freeze an [`AssignmentExecutionSnapshot`] against the authoritative `AgentProfile` preference.
    ///
    /// # Errors
    /// Rejects missing preferences, revision/engine/model/reasoning/role-prompt mismatches,
    /// provider-incompatible `(resolved_engine, resolved_model)` pairs, nil identities, and
    /// negative `committed_at_ms`.
    pub fn resolve_from_profile(
        parts: AssignmentExecutionSnapshotParts,
        preference: Option<&ExecutionPreference>,
        current_profile_revision: AgentProfileRevision,
    ) -> Result<Self, SnapshotValidationError> {
        validate_snapshot_identities(&parts)?;
        let Some(pref) = preference else {
            return Err(SnapshotValidationError::MissingExecutionPreference);
        };
        if parts.profile_revision != current_profile_revision {
            return Err(SnapshotValidationError::ProfileRevisionMismatch);
        }
        if parts.resolved_engine != pref.cli_engine() {
            return Err(SnapshotValidationError::EngineMismatch);
        }
        if parts.resolved_model != pref.preferred_model()
            || parts.resolved_model.trim().is_empty()
            || parts.resolved_model.len() > 128
        {
            return Err(SnapshotValidationError::ModelMismatch);
        }
        if parts.resolved_reasoning.as_deref() != pref.preferred_reasoning() {
            return Err(SnapshotValidationError::ReasoningMismatch);
        }
        if let Some(reasoning) = &parts.resolved_reasoning
            && (reasoning.trim().is_empty() || reasoning.len() > 64)
        {
            return Err(SnapshotValidationError::ReasoningMismatch);
        }
        if parts.role_prompt_digest != pref.role_prompt_ref() {
            return Err(SnapshotValidationError::RolePromptMismatch);
        }
        if !is_engine_model_compatible(parts.resolved_engine, &parts.resolved_model) {
            return Err(SnapshotValidationError::IncompatibleEngineModel);
        }
        Ok(Self::from_validated_parts(parts))
    }

    fn from_validated_parts(parts: AssignmentExecutionSnapshotParts) -> Self {
        Self {
            snapshot_id: parts.snapshot_id,
            attempt_key: parts.attempt_key,
            activity_id: parts.activity_id,
            profile_id: parts.profile_id,
            profile_revision: parts.profile_revision,
            resolved_engine: parts.resolved_engine,
            resolved_model: parts.resolved_model,
            resolved_reasoning: parts.resolved_reasoning,
            role_prompt_digest: parts.role_prompt_digest,
            worktree: parts.worktree,
            lease: parts.lease,
            mcp_grant: parts.mcp_grant,
            permissions: parts.permissions,
            timeouts: parts.timeouts,
            committed_at_ms: parts.committed_at_ms,
        }
    }

    #[must_use]
    pub const fn snapshot_id(&self) -> Uuid {
        self.snapshot_id
    }

    #[must_use]
    pub const fn attempt_key(&self) -> ExecutionAttemptKey {
        self.attempt_key
    }

    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity_id
    }

    #[must_use]
    pub const fn profile_id(&self) -> AgentProfileId {
        self.profile_id
    }

    #[must_use]
    pub const fn profile_revision(&self) -> AgentProfileRevision {
        self.profile_revision
    }

    #[must_use]
    pub const fn resolved_engine(&self) -> CliEngine {
        self.resolved_engine
    }

    #[must_use]
    pub fn resolved_model(&self) -> &str {
        &self.resolved_model
    }

    #[must_use]
    pub fn resolved_reasoning(&self) -> Option<&str> {
        self.resolved_reasoning.as_deref()
    }

    #[must_use]
    pub const fn role_prompt_digest(&self) -> Option<RolePromptDigest> {
        self.role_prompt_digest
    }

    #[must_use]
    pub const fn worktree(&self) -> &ForgeWorktreeRef {
        &self.worktree
    }

    #[must_use]
    pub const fn lease(&self) -> LeaseRef {
        self.lease
    }

    #[must_use]
    pub const fn mcp_grant(&self) -> Option<McpGrant> {
        self.mcp_grant
    }

    #[must_use]
    pub const fn permissions(&self) -> ExecutionPermissions {
        self.permissions
    }

    #[must_use]
    pub const fn timeouts(&self) -> ExecutionTimeouts {
        self.timeouts
    }

    #[must_use]
    pub const fn committed_at_ms(&self) -> i64 {
        self.committed_at_ms
    }
}

fn validate_snapshot_identities(
    parts: &AssignmentExecutionSnapshotParts,
) -> Result<(), SnapshotValidationError> {
    if parts.snapshot_id.is_nil() {
        return Err(SnapshotValidationError::InvalidIdentity(
            "snapshot_id cannot be nil",
        ));
    }
    if parts.activity_id.into_uuid().is_nil() {
        return Err(SnapshotValidationError::InvalidIdentity(
            "activity_id cannot be nil",
        ));
    }
    if parts.profile_id.into_uuid().is_nil() {
        return Err(SnapshotValidationError::InvalidIdentity(
            "profile_id cannot be nil",
        ));
    }
    if parts.profile_revision.get() < 1 {
        return Err(SnapshotValidationError::InvalidIdentity(
            "profile_revision must be positive",
        ));
    }
    if parts.committed_at_ms < 0 {
        return Err(SnapshotValidationError::InvalidTimeout(
            "committed_at_ms must be non-negative",
        ));
    }
    Ok(())
}

fn is_engine_model_compatible(engine: CliEngine, model: &str) -> bool {
    let trimmed = model.trim();
    if trimmed.is_empty() {
        return false;
    }
    match engine {
        CliEngine::Codex => {
            !trimmed.starts_with("claude-")
                && !trimmed.starts_with("gemini-")
                && !trimmed.starts_with("gpt-5")
        }
        CliEngine::Agy => trimmed.starts_with("claude-") || trimmed.starts_with("gemini-"),
    }
}

/// Verified OS process-group ownership witness (`pid`, `pgid`, `start_time`, `boot_id`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessOwnershipIdentity {
    pid: u32,
    pgid: u32,
    start_time: String,
    boot_id: String,
}

impl ProcessOwnershipIdentity {
    /// Construct a validated [`ProcessOwnershipIdentity`].
    ///
    /// # Errors
    /// Rejects `pid == 0`, `pgid == 0`, empty/whitespace-only/`>256B`/control-char `start_time` or `boot_id`.
    pub fn new(
        process_id: u32,
        group_id: u32,
        start_time: String,
        boot_id: String,
    ) -> Result<Self, SnapshotValidationError> {
        if process_id == 0 {
            return Err(SnapshotValidationError::InvalidIdentity(
                "process pid must be positive",
            ));
        }
        if group_id == 0 {
            return Err(SnapshotValidationError::InvalidIdentity(
                "process pgid must be positive",
            ));
        }
        let clean_start = validate_ownership_field(start_time, "process start_time")?;
        let clean_boot = validate_ownership_field(boot_id, "process boot_id")?;
        Ok(Self {
            pid: process_id,
            pgid: group_id,
            start_time: clean_start,
            boot_id: clean_boot,
        })
    }

    #[must_use]
    pub const fn pid(&self) -> u32 {
        self.pid
    }

    #[must_use]
    pub const fn pgid(&self) -> u32 {
        self.pgid
    }

    #[must_use]
    pub fn start_time(&self) -> &str {
        &self.start_time
    }

    #[must_use]
    pub fn boot_id(&self) -> &str {
        &self.boot_id
    }
}

fn validate_ownership_field(
    raw: String,
    label: &'static str,
) -> Result<String, SnapshotValidationError> {
    if raw.len() > MAX_PROCESS_OWNERSHIP_FIELD_BYTES || raw.chars().any(char::is_control) {
        return Err(SnapshotValidationError::InvalidIdentity(label));
    }
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(SnapshotValidationError::InvalidIdentity(label));
    }
    if trimmed.len() == raw.len() {
        return Ok(raw);
    }
    Ok(trimmed.to_owned())
}

/// Cause recorded when a spawned execution attempt transitions to [`LaunchLedgerState::Unknown`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LaunchUnknownReason {
    BootRecoveryAfterSpawn,
    ExecutionTimedOut,
    OwnershipMismatch,
    ForcedTermination,
}

impl LaunchUnknownReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BootRecoveryAfterSpawn => "BOOT_RECOVERY_AFTER_SPAWN",
            Self::ExecutionTimedOut => "EXECUTION_TIMED_OUT",
            Self::OwnershipMismatch => "OWNERSHIP_MISMATCH",
            Self::ForcedTermination => "FORCED_TERMINATION",
        }
    }

    fn parse(raw: &str) -> Result<Self, OperationsError> {
        match raw {
            "BOOT_RECOVERY_AFTER_SPAWN" => Ok(Self::BootRecoveryAfterSpawn),
            "EXECUTION_TIMED_OUT" => Ok(Self::ExecutionTimedOut),
            "OWNERSHIP_MISMATCH" => Ok(Self::OwnershipMismatch),
            "FORCED_TERMINATION" => Ok(Self::ForcedTermination),
            _ => Err(OperationsError::Corrupt("launch ledger unknown_reason")),
        }
    }
}

/// Durable lifecycle state of an execution attempt's Launch Ledger row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LaunchLedgerState {
    Prepared,
    NotStarted {
        reconciled_at_ms: i64,
    },
    Spawned {
        ownership: ProcessOwnershipIdentity,
        spawned_at_ms: i64,
    },
    Exited {
        ownership: ProcessOwnershipIdentity,
        exit_code: i32,
        exited_at_ms: i64,
    },
    Unknown {
        ownership: Option<ProcessOwnershipIdentity>,
        reason: LaunchUnknownReason,
        recorded_at_ms: i64,
    },
}

/// Durable readback or mutation receipt for one attempt's Launch Ledger entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchLedgerRecord {
    snapshot_id: Uuid,
    attempt_key: ExecutionAttemptKey,
    state: LaunchLedgerState,
    revision: u64,
    updated_at_ms: i64,
}

impl LaunchLedgerRecord {
    #[must_use]
    pub const fn snapshot_id(&self) -> Uuid {
        self.snapshot_id
    }

    #[must_use]
    pub const fn attempt_key(&self) -> ExecutionAttemptKey {
        self.attempt_key
    }

    #[must_use]
    pub const fn state(&self) -> &LaunchLedgerState {
        &self.state
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub const fn updated_at_ms(&self) -> i64 {
        self.updated_at_ms
    }
}

impl OperationsCatalog {
    /// Atomically persist an immutable [`AssignmentExecutionSnapshot`] and its initial `Prepared` ledger row.
    ///
    /// # Errors
    /// Rejects nil `request_id`, conflicting `request_id` reuse, duplicate `snapshot_id` or
    /// `attempt_key` under another `request_id`, and SQLite or unconfirmed-commit failures.
    pub fn commit_assignment_execution_snapshot(
        &mut self,
        request_id: Uuid,
        snapshot: &AssignmentExecutionSnapshot,
    ) -> Result<(AssignmentExecutionSnapshot, LaunchLedgerRecord), OperationsError> {
        if request_id.is_nil() {
            return Err(OperationsError::Bounds("execution snapshot request_id"));
        }
        let digest = snapshot_digest(snapshot);
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(replayed) = load_snapshot_replay(&tx, request_id, snapshot, digest)? {
            tx.commit()?;
            return Ok(replayed);
        }
        if snapshot_or_attempt_exists(&tx, snapshot.snapshot_id, snapshot.attempt_key)? {
            return Err(OperationsError::Conflict(
                "assignment execution snapshot already committed for attempt",
            ));
        }
        insert_snapshot_row(&tx, request_id, snapshot, digest)?;
        insert_initial_ledger_row(&tx, snapshot)?;
        let ledger = LaunchLedgerRecord {
            snapshot_id: snapshot.snapshot_id,
            attempt_key: snapshot.attempt_key,
            state: LaunchLedgerState::Prepared,
            revision: 1,
            updated_at_ms: snapshot.committed_at_ms,
        };
        let fresh = (snapshot.clone(), ledger);
        finalize_operation_commit(
            tx.commit(),
            fresh,
            snapshot.attempt_key.workspace_id,
            request_id,
            || load_snapshot_replay(&self.connection, request_id, snapshot, digest),
        )
    }

    /// Read the durable [`AssignmentExecutionSnapshot`] and [`LaunchLedgerRecord`] for `attempt_key`.
    ///
    /// # Errors
    /// Returns [`OperationsError`] on SQLite read failures or corrupt persisted state.
    pub fn get_assignment_execution_snapshot(
        &self,
        attempt_key: ExecutionAttemptKey,
    ) -> Result<Option<(AssignmentExecutionSnapshot, LaunchLedgerRecord)>, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        let pair = load_snapshot_and_ledger_by_attempt(&tx, attempt_key)?;
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(pair)
    }

    /// Transition an attempt's Launch Ledger from `Prepared` to `Spawned` once and only once.
    ///
    /// # Errors
    /// Returns `OperationsError::Conflict("execution attempt cannot be re-spawned")` if the row
    /// is in any state other than `Prepared`, or conflict/bounds/storage errors on CAS or DB failure.
    pub fn record_execution_spawned(
        &mut self,
        attempt_key: ExecutionAttemptKey,
        expected_revision: u64,
        ownership: ProcessOwnershipIdentity,
        spawned_at_ms: i64,
    ) -> Result<LaunchLedgerRecord, OperationsError> {
        self.transition_ledger_row(attempt_key, expected_revision, spawned_at_ms, |state| {
            match state {
                LaunchLedgerState::Prepared => Ok(LaunchLedgerState::Spawned {
                    ownership,
                    spawned_at_ms,
                }),
                _ => Err(OperationsError::Conflict(
                    "execution attempt cannot be re-spawned",
                )),
            }
        })
    }

    /// Transition an attempt's Launch Ledger from `Spawned` to `Exited`.
    ///
    /// # Errors
    /// Rejects non-`Spawned` rows, stale `expected_revision`, negative `exited_at_ms`, and DB errors.
    pub fn record_execution_exited(
        &mut self,
        attempt_key: ExecutionAttemptKey,
        expected_revision: u64,
        exit_code: i32,
        exited_at_ms: i64,
    ) -> Result<LaunchLedgerRecord, OperationsError> {
        self.transition_ledger_row(
            attempt_key,
            expected_revision,
            exited_at_ms,
            |state| match state {
                LaunchLedgerState::Spawned { ownership, .. } => Ok(LaunchLedgerState::Exited {
                    ownership,
                    exit_code,
                    exited_at_ms,
                }),
                _ => Err(OperationsError::Conflict(
                    "execution attempt is not in Spawned state",
                )),
            },
        )
    }

    /// Transition an attempt's Launch Ledger from `Spawned` to `Unknown`, preserving prior ownership.
    ///
    /// # Errors
    /// Rejects non-`Spawned` rows, stale `expected_revision`, negative `recorded_at_ms`, and DB errors.
    pub fn record_execution_unknown(
        &mut self,
        attempt_key: ExecutionAttemptKey,
        expected_revision: u64,
        reason: LaunchUnknownReason,
        recorded_at_ms: i64,
    ) -> Result<LaunchLedgerRecord, OperationsError> {
        self.transition_ledger_row(attempt_key, expected_revision, recorded_at_ms, |state| {
            match state {
                LaunchLedgerState::Spawned { ownership, .. } => Ok(LaunchLedgerState::Unknown {
                    ownership: Some(ownership),
                    reason,
                    recorded_at_ms,
                }),
                _ => Err(OperationsError::Conflict(
                    "execution attempt is not in Spawned state",
                )),
            }
        })
    }

    fn transition_ledger_row(
        &mut self,
        attempt_key: ExecutionAttemptKey,
        expected_revision: u64,
        observed_at_ms: i64,
        build_next_state: impl FnOnce(LaunchLedgerState) -> Result<LaunchLedgerState, OperationsError>,
    ) -> Result<LaunchLedgerRecord, OperationsError> {
        validate_ledger_transition_inputs(expected_revision, observed_at_ms)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_ledger_by_attempt(&tx, attempt_key)?
            .ok_or(OperationsError::Conflict("execution launch ledger missing"))?;
        let next_state = build_next_state(current.state)?;
        if current.revision != expected_revision {
            return Err(OperationsError::Conflict(
                "execution launch ledger revision mismatch",
            ));
        }
        let next_revision = increment_ledger_revision(current.revision)?;
        update_ledger_state(
            &tx,
            current.snapshot_id,
            expected_revision,
            next_revision,
            &next_state,
            observed_at_ms,
        )?;
        tx.commit()
            .map_err(|source| OperationsError::CatalogCommitUnconfirmed {
                destination_id: self.destination,
                source,
            })?;
        Ok(LaunchLedgerRecord {
            snapshot_id: current.snapshot_id,
            attempt_key,
            state: next_state,
            revision: next_revision,
            updated_at_ms: observed_at_ms,
        })
    }

    /// Atomically reconcile in-flight Launch Ledger rows on supervisor boot recovery.
    ///
    /// Transitions every `Prepared` row to `NotStarted { reconciled_at_ms }` and every
    /// `Spawned` row to `Unknown { ownership: Some(prior_ownership), reason: BootRecoveryAfterSpawn, recorded_at_ms: reconciled_at_ms }`,
    /// incrementing `revision` on each transitioned row and returning the updated records.
    ///
    /// # Errors
    /// Rejects blank/oversized `current_boot_id`, nil `workspace_id`, negative `reconciled_at_ms`,
    /// and SQLite read/write or unconfirmed-commit failures.
    pub fn reconcile_launch_ledger_on_boot(
        &mut self,
        workspace_id: Option<WorkspaceId>,
        current_boot_id: &str,
        reconciled_at_ms: i64,
    ) -> Result<Vec<LaunchLedgerRecord>, OperationsError> {
        validate_boot_reconcile_inputs(workspace_id, current_boot_id, reconciled_at_ms)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let pending = load_in_flight_ledgers(&tx, workspace_id)?;
        let mut updated_records = Vec::with_capacity(pending.len());
        for row in pending {
            let next_revision = increment_ledger_revision(row.revision)?;
            let next_state = match row.state {
                LaunchLedgerState::Prepared => LaunchLedgerState::NotStarted { reconciled_at_ms },
                LaunchLedgerState::Spawned { ownership, .. } => LaunchLedgerState::Unknown {
                    ownership: Some(ownership),
                    reason: LaunchUnknownReason::BootRecoveryAfterSpawn,
                    recorded_at_ms: reconciled_at_ms,
                },
                _ => {
                    return Err(OperationsError::Corrupt(
                        "unexpected non-inflight launch ledger state",
                    ));
                }
            };
            update_ledger_state(
                &tx,
                row.snapshot_id,
                row.revision,
                next_revision,
                &next_state,
                reconciled_at_ms,
            )?;
            updated_records.push(LaunchLedgerRecord {
                snapshot_id: row.snapshot_id,
                attempt_key: row.attempt_key,
                state: next_state,
                revision: next_revision,
                updated_at_ms: reconciled_at_ms,
            });
        }
        tx.commit()
            .map_err(|source| OperationsError::CatalogCommitUnconfirmed {
                destination_id: self.destination,
                source,
            })?;
        Ok(updated_records)
    }
}

fn validate_ledger_transition_inputs(
    expected_revision: u64,
    timestamp_ms: i64,
) -> Result<(), OperationsError> {
    if timestamp_ms < 0 {
        return Err(OperationsError::Bounds("launch ledger timestamp"));
    }
    if expected_revision == 0 {
        return Err(OperationsError::Conflict(
            "execution launch ledger revision mismatch",
        ));
    }
    crate::catalog::signed(expected_revision)?;
    Ok(())
}

fn validate_boot_reconcile_inputs(
    workspace_id: Option<WorkspaceId>,
    current_boot_id: &str,
    reconciled_at_ms: i64,
) -> Result<(), OperationsError> {
    if reconciled_at_ms < 0 {
        return Err(OperationsError::Bounds("boot reconciliation timestamp"));
    }
    if workspace_id.is_some_and(|ws| ws.into_uuid().is_nil()) {
        return Err(OperationsError::Bounds("boot reconciliation workspace_id"));
    }
    if current_boot_id.trim().is_empty()
        || current_boot_id.len() > MAX_PROCESS_OWNERSHIP_FIELD_BYTES
        || current_boot_id.chars().any(char::is_control)
    {
        return Err(OperationsError::Bounds(
            "boot reconciliation current_boot_id",
        ));
    }
    Ok(())
}

fn increment_ledger_revision(current: u64) -> Result<u64, OperationsError> {
    let next = current
        .checked_add(1)
        .ok_or(OperationsError::Bounds("launch ledger revision"))?;
    crate::catalog::signed(next)?;
    Ok(next)
}

fn snapshot_or_attempt_exists(
    connection: &Connection,
    snapshot_id: Uuid,
    attempt_key: ExecutionAttemptKey,
) -> Result<bool, OperationsError> {
    connection
        .query_row(
            EXISTS_SNAPSHOT_OR_ATTEMPT_SQL,
            params![
                snapshot_id.as_bytes(),
                attempt_key.workspace_id.into_uuid().as_bytes(),
                attempt_key.goal_id.into_uuid().as_bytes(),
                attempt_key.dag_revision.get(),
                attempt_key.node_id.into_uuid().as_bytes(),
                attempt_key.assignment_id.into_uuid().as_bytes(),
                i64::from(attempt_key.attempt),
            ],
            |row| row.get(0),
        )
        .map_err(OperationsError::from)
}

fn insert_snapshot_row(
    tx: &Transaction<'_>,
    request_id: Uuid,
    snapshot: &AssignmentExecutionSnapshot,
    payload_digest: [u8; 32],
) -> Result<(), OperationsError> {
    let lease_rev_i64 = crate::catalog::signed(snapshot.lease.lease_revision)?;
    let startup_i64 = crate::catalog::signed(snapshot.timeouts.startup_timeout)?;
    let execution_i64 = crate::catalog::signed(snapshot.timeouts.execution_timeout)?;
    let grace_i64 = crate::catalog::signed(snapshot.timeouts.termination_grace)?;
    let grant_token = snapshot.mcp_grant.map(|grant| grant.token_id.into_uuid());
    let grant_issued = snapshot.mcp_grant.map(McpGrant::issued_at_ms);
    let grant_expires = snapshot.mcp_grant.map(McpGrant::expires_at_ms);
    tx.execute(
        INSERT_EXECUTION_SNAPSHOT_SQL,
        params![
            snapshot.snapshot_id.as_bytes(),
            request_id.as_bytes(),
            payload_digest.as_slice(),
            snapshot.attempt_key.workspace_id.into_uuid().as_bytes(),
            snapshot.attempt_key.goal_id.into_uuid().as_bytes(),
            snapshot.attempt_key.dag_revision.get(),
            snapshot.attempt_key.node_id.into_uuid().as_bytes(),
            snapshot.attempt_key.assignment_id.into_uuid().as_bytes(),
            i64::from(snapshot.attempt_key.attempt),
            snapshot.activity_id.into_uuid().as_bytes(),
            snapshot.profile_id.into_uuid().as_bytes(),
            snapshot.profile_revision.get(),
            snapshot.resolved_engine.as_str(),
            snapshot.resolved_model.as_str(),
            snapshot.resolved_reasoning.as_deref(),
            snapshot
                .role_prompt_digest
                .as_ref()
                .map(|d| d.as_bytes().as_slice()),
            snapshot.worktree.as_str(),
            snapshot.lease.lease_id.as_bytes(),
            lease_rev_i64,
            snapshot.lease.expires_at_ms,
            grant_token.as_ref().map(Uuid::as_bytes),
            grant_issued,
            grant_expires,
            i64::from(snapshot.permissions.allow_workspace_write()),
            i64::from(snapshot.permissions.allow_local_shell()),
            i64::from(snapshot.permissions.allow_network()),
            startup_i64,
            execution_i64,
            grace_i64,
            snapshot.committed_at_ms,
        ],
    )?;
    Ok(())
}

fn insert_initial_ledger_row(
    tx: &Transaction<'_>,
    snapshot: &AssignmentExecutionSnapshot,
) -> Result<(), OperationsError> {
    tx.execute(
        INSERT_INITIAL_LAUNCH_LEDGER_SQL,
        params![
            snapshot.snapshot_id.as_bytes(),
            snapshot.attempt_key.workspace_id.into_uuid().as_bytes(),
            snapshot.attempt_key.goal_id.into_uuid().as_bytes(),
            snapshot.attempt_key.dag_revision.get(),
            snapshot.attempt_key.node_id.into_uuid().as_bytes(),
            snapshot.attempt_key.assignment_id.into_uuid().as_bytes(),
            i64::from(snapshot.attempt_key.attempt),
            snapshot.committed_at_ms,
        ],
    )?;
    Ok(())
}

struct EncodedLedgerState<'a> {
    state_tag: &'static str,
    pid: Option<i64>,
    pgid: Option<i64>,
    start_time: Option<&'a str>,
    boot_id: Option<&'a str>,
    exit_code: Option<i64>,
    unknown_reason: Option<&'static str>,
    recorded_at_ms: Option<i64>,
}

fn encode_ledger_state(state: &LaunchLedgerState) -> EncodedLedgerState<'_> {
    match state {
        LaunchLedgerState::Prepared => EncodedLedgerState {
            state_tag: "PREPARED",
            pid: None,
            pgid: None,
            start_time: None,
            boot_id: None,
            exit_code: None,
            unknown_reason: None,
            recorded_at_ms: None,
        },
        LaunchLedgerState::NotStarted { reconciled_at_ms } => EncodedLedgerState {
            state_tag: "NOT_STARTED",
            pid: None,
            pgid: None,
            start_time: None,
            boot_id: None,
            exit_code: None,
            unknown_reason: None,
            recorded_at_ms: Some(*reconciled_at_ms),
        },
        LaunchLedgerState::Spawned {
            ownership,
            spawned_at_ms,
        } => EncodedLedgerState {
            state_tag: "SPAWNED",
            pid: Some(i64::from(ownership.pid)),
            pgid: Some(i64::from(ownership.pgid)),
            start_time: Some(&ownership.start_time),
            boot_id: Some(&ownership.boot_id),
            exit_code: None,
            unknown_reason: None,
            recorded_at_ms: Some(*spawned_at_ms),
        },
        LaunchLedgerState::Exited {
            ownership,
            exit_code,
            exited_at_ms,
        } => EncodedLedgerState {
            state_tag: "EXITED",
            pid: Some(i64::from(ownership.pid)),
            pgid: Some(i64::from(ownership.pgid)),
            start_time: Some(&ownership.start_time),
            boot_id: Some(&ownership.boot_id),
            exit_code: Some(i64::from(*exit_code)),
            unknown_reason: None,
            recorded_at_ms: Some(*exited_at_ms),
        },
        LaunchLedgerState::Unknown {
            ownership,
            reason,
            recorded_at_ms,
        } => EncodedLedgerState {
            state_tag: "UNKNOWN",
            pid: ownership.as_ref().map(|item| i64::from(item.pid)),
            pgid: ownership.as_ref().map(|item| i64::from(item.pgid)),
            start_time: ownership.as_ref().map(ProcessOwnershipIdentity::start_time),
            boot_id: ownership.as_ref().map(ProcessOwnershipIdentity::boot_id),
            exit_code: None,
            unknown_reason: Some(reason.as_str()),
            recorded_at_ms: Some(*recorded_at_ms),
        },
    }
}

fn update_ledger_state(
    tx: &Transaction<'_>,
    snapshot_id: Uuid,
    expected_revision: u64,
    next_revision: u64,
    next_state: &LaunchLedgerState,
    updated_at_ms: i64,
) -> Result<(), OperationsError> {
    let encoded = encode_ledger_state(next_state);
    let updated = tx.execute(
        UPDATE_LAUNCH_LEDGER_STATE_SQL,
        params![
            snapshot_id.as_bytes(),
            encoded.state_tag,
            encoded.pid,
            encoded.pgid,
            encoded.start_time,
            encoded.boot_id,
            encoded.exit_code,
            encoded.unknown_reason,
            encoded.recorded_at_ms,
            crate::catalog::signed(next_revision)?,
            updated_at_ms,
            crate::catalog::signed(expected_revision)?,
        ],
    )?;
    if updated != 1 {
        return Err(OperationsError::Conflict(
            "execution launch ledger revision mismatch",
        ));
    }
    Ok(())
}

struct RawLedgerRow {
    snapshot_id: Vec<u8>,
    workspace_id: Vec<u8>,
    goal_id: Vec<u8>,
    dag_revision: i64,
    node_id: Vec<u8>,
    assignment_id: Vec<u8>,
    attempt: i64,
    state: String,
    pid: Option<i64>,
    pgid: Option<i64>,
    start_time: Option<String>,
    boot_id: Option<String>,
    exit_code: Option<i64>,
    unknown_reason: Option<String>,
    state_recorded_at_ms: Option<i64>,
    revision: i64,
    updated_at_ms: i64,
}

fn read_raw_ledger_row(row: &Row<'_>) -> rusqlite::Result<RawLedgerRow> {
    Ok(RawLedgerRow {
        snapshot_id: row.get(0)?,
        workspace_id: row.get(1)?,
        goal_id: row.get(2)?,
        dag_revision: row.get(3)?,
        node_id: row.get(4)?,
        assignment_id: row.get(5)?,
        attempt: row.get(6)?,
        state: row.get(7)?,
        pid: row.get(8)?,
        pgid: row.get(9)?,
        start_time: row.get(10)?,
        boot_id: row.get(11)?,
        exit_code: row.get(12)?,
        unknown_reason: row.get(13)?,
        state_recorded_at_ms: row.get(14)?,
        revision: row.get(15)?,
        updated_at_ms: row.get(16)?,
    })
}

fn decode_ledger_row(raw: RawLedgerRow) -> Result<LaunchLedgerRecord, OperationsError> {
    let snapshot_id = decode_non_nil_uuid(&raw.snapshot_id, "ledger snapshot_id")?;
    let attempt_key = decode_attempt_key(
        &raw.workspace_id,
        &raw.goal_id,
        raw.dag_revision,
        &raw.node_id,
        &raw.assignment_id,
        raw.attempt,
    )?;
    let ownership = decode_optional_ownership(raw.pid, raw.pgid, raw.start_time, raw.boot_id)?;
    let state = decode_ledger_state(
        &raw.state,
        ownership,
        raw.exit_code,
        raw.unknown_reason.as_deref(),
        raw.state_recorded_at_ms,
    )?;
    if raw.updated_at_ms < 0 {
        return Err(OperationsError::Corrupt("ledger updated_at_ms"));
    }
    Ok(LaunchLedgerRecord {
        snapshot_id,
        attempt_key,
        state,
        revision: decode_positive_u64(raw.revision, "ledger revision")?,
        updated_at_ms: raw.updated_at_ms,
    })
}

fn decode_optional_ownership(
    process_id: Option<i64>,
    group_id: Option<i64>,
    start_time: Option<String>,
    boot_id: Option<String>,
) -> Result<Option<ProcessOwnershipIdentity>, OperationsError> {
    match (process_id, group_id, start_time, boot_id) {
        (None, None, None, None) => Ok(None),
        (Some(raw_process_id), Some(raw_group_id), Some(start), Some(boot)) => {
            let parsed_process_id = u32::try_from(raw_process_id)
                .map_err(|_| OperationsError::Corrupt("ledger pid"))?;
            let parsed_group_id =
                u32::try_from(raw_group_id).map_err(|_| OperationsError::Corrupt("ledger pgid"))?;
            let identity =
                ProcessOwnershipIdentity::new(parsed_process_id, parsed_group_id, start, boot)
                    .map_err(|_| OperationsError::Corrupt("ledger ownership identity"))?;
            Ok(Some(identity))
        }
        _ => Err(OperationsError::Corrupt("ledger partial ownership columns")),
    }
}

fn decode_ledger_state(
    tag: &str,
    ownership: Option<ProcessOwnershipIdentity>,
    exit_code: Option<i64>,
    unknown_reason: Option<&str>,
    recorded_at_ms: Option<i64>,
) -> Result<LaunchLedgerState, OperationsError> {
    match tag {
        "PREPARED" => {
            if ownership.is_some()
                || exit_code.is_some()
                || unknown_reason.is_some()
                || recorded_at_ms.is_some()
            {
                return Err(OperationsError::Corrupt("ledger PREPARED columns"));
            }
            Ok(LaunchLedgerState::Prepared)
        }
        "NOT_STARTED" => {
            let ts = decode_non_negative_ts(recorded_at_ms, "ledger NOT_STARTED timestamp")?;
            if ownership.is_some() || exit_code.is_some() || unknown_reason.is_some() {
                return Err(OperationsError::Corrupt("ledger NOT_STARTED columns"));
            }
            Ok(LaunchLedgerState::NotStarted {
                reconciled_at_ms: ts,
            })
        }
        "SPAWNED" => {
            let ts = decode_non_negative_ts(recorded_at_ms, "ledger SPAWNED timestamp")?;
            let owner = ownership.ok_or(OperationsError::Corrupt("ledger SPAWNED ownership"))?;
            if exit_code.is_some() || unknown_reason.is_some() {
                return Err(OperationsError::Corrupt("ledger SPAWNED columns"));
            }
            Ok(LaunchLedgerState::Spawned {
                ownership: owner,
                spawned_at_ms: ts,
            })
        }
        "EXITED" => {
            let ts = decode_non_negative_ts(recorded_at_ms, "ledger EXITED timestamp")?;
            let owner = ownership.ok_or(OperationsError::Corrupt("ledger EXITED ownership"))?;
            let raw_code = exit_code.ok_or(OperationsError::Corrupt("ledger EXITED exit_code"))?;
            let code = i32::try_from(raw_code)
                .map_err(|_| OperationsError::Corrupt("ledger exit_code"))?;
            if unknown_reason.is_some() {
                return Err(OperationsError::Corrupt("ledger EXITED unknown_reason"));
            }
            Ok(LaunchLedgerState::Exited {
                ownership: owner,
                exit_code: code,
                exited_at_ms: ts,
            })
        }
        "UNKNOWN" => {
            let ts = decode_non_negative_ts(recorded_at_ms, "ledger UNKNOWN timestamp")?;
            if exit_code.is_some() {
                return Err(OperationsError::Corrupt("ledger UNKNOWN exit_code"));
            }
            let reason_raw =
                unknown_reason.ok_or(OperationsError::Corrupt("ledger UNKNOWN reason"))?;
            Ok(LaunchLedgerState::Unknown {
                ownership,
                reason: LaunchUnknownReason::parse(reason_raw)?,
                recorded_at_ms: ts,
            })
        }
        _ => Err(OperationsError::Corrupt("ledger state tag")),
    }
}

struct RawSnapshotRow {
    snapshot_id: Vec<u8>,
    payload_digest: Vec<u8>,
    workspace_id: Vec<u8>,
    goal_id: Vec<u8>,
    dag_revision: i64,
    node_id: Vec<u8>,
    assignment_id: Vec<u8>,
    attempt: i64,
    activity_id: Vec<u8>,
    profile_id: Vec<u8>,
    profile_revision: i64,
    resolved_engine: String,
    resolved_model: String,
    resolved_reasoning: Option<String>,
    role_prompt_digest: Option<Vec<u8>>,
    worktree_path: String,
    lease_id: Vec<u8>,
    lease_revision: i64,
    lease_expires_at_ms: i64,
    mcp_grant_token_id: Option<Vec<u8>>,
    mcp_grant_issued_at_ms: Option<i64>,
    mcp_grant_expires_at_ms: Option<i64>,
    allow_workspace_write: i64,
    allow_local_shell: i64,
    allow_network: i64,
    startup_timeout_ms: i64,
    execution_timeout_ms: i64,
    termination_grace_ms: i64,
    committed_at_ms: i64,
}

fn read_raw_snapshot_row(row: &Row<'_>) -> rusqlite::Result<RawSnapshotRow> {
    Ok(RawSnapshotRow {
        snapshot_id: row.get(0)?,
        payload_digest: row.get(1)?,
        workspace_id: row.get(2)?,
        goal_id: row.get(3)?,
        dag_revision: row.get(4)?,
        node_id: row.get(5)?,
        assignment_id: row.get(6)?,
        attempt: row.get(7)?,
        activity_id: row.get(8)?,
        profile_id: row.get(9)?,
        profile_revision: row.get(10)?,
        resolved_engine: row.get(11)?,
        resolved_model: row.get(12)?,
        resolved_reasoning: row.get(13)?,
        role_prompt_digest: row.get(14)?,
        worktree_path: row.get(15)?,
        lease_id: row.get(16)?,
        lease_revision: row.get(17)?,
        lease_expires_at_ms: row.get(18)?,
        mcp_grant_token_id: row.get(19)?,
        mcp_grant_issued_at_ms: row.get(20)?,
        mcp_grant_expires_at_ms: row.get(21)?,
        allow_workspace_write: row.get(22)?,
        allow_local_shell: row.get(23)?,
        allow_network: row.get(24)?,
        startup_timeout_ms: row.get(25)?,
        execution_timeout_ms: row.get(26)?,
        termination_grace_ms: row.get(27)?,
        committed_at_ms: row.get(28)?,
    })
}

fn decode_snapshot_row(
    raw: RawSnapshotRow,
) -> Result<(AssignmentExecutionSnapshot, [u8; 32]), OperationsError> {
    let snapshot_id = decode_non_nil_uuid(&raw.snapshot_id, "snapshot_id")?;
    let digest = decode_non_zero_digest(&raw.payload_digest, "snapshot payload_digest")?;
    let attempt_key = decode_attempt_key(
        &raw.workspace_id,
        &raw.goal_id,
        raw.dag_revision,
        &raw.node_id,
        &raw.assignment_id,
        raw.attempt,
    )?;
    let activity_id = ActivityId::from_uuid(decode_non_nil_uuid(&raw.activity_id, "activity_id")?);
    let profile_id = AgentProfileId::from_uuid(decode_non_nil_uuid(&raw.profile_id, "profile_id")?)
        .map_err(|_| OperationsError::Corrupt("snapshot profile_id"))?;
    let profile_revision = AgentProfileRevision::new(raw.profile_revision)
        .map_err(|_| OperationsError::Corrupt("snapshot profile_revision"))?;
    let resolved_engine = CliEngine::parse(&raw.resolved_engine)
        .map_err(|_| OperationsError::Corrupt("snapshot resolved_engine"))?;
    if !is_engine_model_compatible(resolved_engine, &raw.resolved_model)
        || raw.resolved_model.len() > 128
    {
        return Err(OperationsError::Corrupt("snapshot resolved_model"));
    }
    if raw
        .resolved_reasoning
        .as_ref()
        .is_some_and(|r| r.trim().is_empty() || r.len() > 64)
    {
        return Err(OperationsError::Corrupt("snapshot resolved_reasoning"));
    }
    let role_prompt_digest = raw
        .role_prompt_digest
        .map(|bytes| {
            RolePromptDigest::from_slice(&bytes)
                .map_err(|_| OperationsError::Corrupt("snapshot role_prompt_digest"))
        })
        .transpose()?;
    let worktree = ForgeWorktreeRef::new(PathBuf::from(raw.worktree_path))
        .map_err(|_| OperationsError::Corrupt("snapshot worktree_path"))?;
    let lease = decode_lease(&raw.lease_id, raw.lease_revision, raw.lease_expires_at_ms)?;
    let mcp_grant = decode_mcp_grant(
        raw.mcp_grant_token_id.as_deref(),
        raw.mcp_grant_issued_at_ms,
        raw.mcp_grant_expires_at_ms,
    )?;
    let permissions = decode_permissions(
        raw.allow_workspace_write,
        raw.allow_local_shell,
        raw.allow_network,
    )?;
    let timeouts = decode_timeouts(
        raw.startup_timeout_ms,
        raw.execution_timeout_ms,
        raw.termination_grace_ms,
    )?;
    if raw.committed_at_ms < 0 {
        return Err(OperationsError::Corrupt("snapshot committed_at_ms"));
    }
    Ok((
        AssignmentExecutionSnapshot::from_validated_parts(AssignmentExecutionSnapshotParts {
            snapshot_id,
            attempt_key,
            activity_id,
            profile_id,
            profile_revision,
            resolved_engine,
            resolved_model: raw.resolved_model,
            resolved_reasoning: raw.resolved_reasoning,
            role_prompt_digest,
            worktree,
            lease,
            mcp_grant,
            permissions,
            timeouts,
            committed_at_ms: raw.committed_at_ms,
        }),
        digest,
    ))
}

fn decode_attempt_key(
    workspace_bytes: &[u8],
    goal_bytes: &[u8],
    dag_revision: i64,
    node_bytes: &[u8],
    assignment_bytes: &[u8],
    attempt: i64,
) -> Result<ExecutionAttemptKey, OperationsError> {
    let workspace_id = WorkspaceId::from_uuid(decode_non_nil_uuid(
        workspace_bytes,
        "attempt workspace_id",
    )?);
    let goal_id = GoalId::from_uuid(decode_non_nil_uuid(goal_bytes, "attempt goal_id")?);
    let dag_rev = DagRevisionNumber::new(dag_revision)
        .map_err(|_| OperationsError::Corrupt("attempt dag_revision"))?;
    let node_id = DagNodeId::from_uuid(decode_non_nil_uuid(node_bytes, "attempt node_id")?);
    let assignment_id = AgentAssignmentId::from_uuid(decode_non_nil_uuid(
        assignment_bytes,
        "attempt assignment_id",
    )?);
    let attempt_u32 =
        u32::try_from(attempt).map_err(|_| OperationsError::Corrupt("attempt number"))?;
    ExecutionAttemptKey::new(
        workspace_id,
        goal_id,
        dag_rev,
        node_id,
        assignment_id,
        attempt_u32,
    )
    .map_err(|_| OperationsError::Corrupt("execution attempt key"))
}

fn decode_lease(
    lease_bytes: &[u8],
    lease_revision: i64,
    expires_at_ms: i64,
) -> Result<LeaseRef, OperationsError> {
    let lease_id = decode_non_nil_uuid(lease_bytes, "snapshot lease_id")?;
    let rev = decode_positive_u64(lease_revision, "snapshot lease_revision")?;
    LeaseRef::new(lease_id, rev, expires_at_ms)
        .map_err(|_| OperationsError::Corrupt("snapshot lease"))
}

fn decode_mcp_grant(
    token_bytes: Option<&[u8]>,
    issued_at_ms: Option<i64>,
    expires_at_ms: Option<i64>,
) -> Result<Option<McpGrant>, OperationsError> {
    match (token_bytes, issued_at_ms, expires_at_ms) {
        (None, None, None) => Ok(None),
        (Some(bytes), Some(issued), Some(expires)) => {
            let token_id = DirectTokenId::from_uuid(decode_non_nil_uuid(
                bytes,
                "snapshot mcp_grant_token_id",
            )?);
            McpGrant::new(token_id, issued, expires)
                .map(Some)
                .map_err(|_| OperationsError::Corrupt("snapshot mcp_grant"))
        }
        _ => Err(OperationsError::Corrupt("snapshot partial mcp_grant")),
    }
}

fn decode_permissions(
    write_flag: i64,
    shell_flag: i64,
    network_flag: i64,
) -> Result<ExecutionPermissions, OperationsError> {
    Ok(ExecutionPermissions::new(
        decode_bool_flag(write_flag, "allow_workspace_write")?,
        decode_bool_flag(shell_flag, "allow_local_shell")?,
        decode_bool_flag(network_flag, "allow_network")?,
    ))
}

fn decode_timeouts(
    startup_ms: i64,
    execution_ms: i64,
    grace_ms: i64,
) -> Result<ExecutionTimeouts, OperationsError> {
    let startup = decode_positive_u64(startup_ms, "startup_timeout_ms")?;
    let execution = decode_positive_u64(execution_ms, "execution_timeout_ms")?;
    let grace = decode_positive_u64(grace_ms, "termination_grace_ms")?;
    ExecutionTimeouts::new(startup, execution, grace)
        .map_err(|_| OperationsError::Corrupt("snapshot timeouts"))
}

fn load_ledger_by_attempt(
    connection: &Connection,
    attempt_key: ExecutionAttemptKey,
) -> Result<Option<LaunchLedgerRecord>, OperationsError> {
    let raw = connection
        .query_row(
            SELECT_LEDGER_BY_ATTEMPT_SQL,
            params![
                attempt_key.workspace_id.into_uuid().as_bytes(),
                attempt_key.goal_id.into_uuid().as_bytes(),
                attempt_key.dag_revision.get(),
                attempt_key.node_id.into_uuid().as_bytes(),
                attempt_key.assignment_id.into_uuid().as_bytes(),
                i64::from(attempt_key.attempt),
            ],
            read_raw_ledger_row,
        )
        .optional()?;
    raw.map(decode_ledger_row).transpose()
}

fn load_ledger_by_snapshot_id(
    connection: &Connection,
    snapshot_id: Uuid,
) -> Result<Option<LaunchLedgerRecord>, OperationsError> {
    let raw = connection
        .query_row(
            SELECT_LEDGER_BY_SNAPSHOT_ID_SQL,
            params![snapshot_id.as_bytes()],
            read_raw_ledger_row,
        )
        .optional()?;
    raw.map(decode_ledger_row).transpose()
}

fn load_snapshot_and_ledger_by_attempt(
    connection: &Connection,
    attempt_key: ExecutionAttemptKey,
) -> Result<Option<(AssignmentExecutionSnapshot, LaunchLedgerRecord)>, OperationsError> {
    let raw_snapshot = connection
        .query_row(
            SELECT_SNAPSHOT_BY_ATTEMPT_SQL,
            params![
                attempt_key.workspace_id.into_uuid().as_bytes(),
                attempt_key.goal_id.into_uuid().as_bytes(),
                attempt_key.dag_revision.get(),
                attempt_key.node_id.into_uuid().as_bytes(),
                attempt_key.assignment_id.into_uuid().as_bytes(),
                i64::from(attempt_key.attempt),
            ],
            read_raw_snapshot_row,
        )
        .optional()?;
    let Some(raw_row) = raw_snapshot else {
        return Ok(None);
    };
    let (snapshot, _) = decode_snapshot_row(raw_row)?;
    let ledger = load_ledger_by_snapshot_id(connection, snapshot.snapshot_id)?.ok_or(
        OperationsError::Corrupt("launch ledger missing for snapshot"),
    )?;
    if ledger.attempt_key != snapshot.attempt_key {
        return Err(OperationsError::Corrupt(
            "launch ledger attempt_key mismatch",
        ));
    }
    Ok(Some((snapshot, ledger)))
}

fn load_snapshot_replay(
    connection: &Connection,
    request_id: Uuid,
    expected_snapshot: &AssignmentExecutionSnapshot,
    expected_digest: [u8; 32],
) -> Result<Option<(AssignmentExecutionSnapshot, LaunchLedgerRecord)>, OperationsError> {
    let raw = connection
        .query_row(
            SELECT_SNAPSHOT_BY_REQUEST_ID_SQL,
            params![request_id.as_bytes()],
            read_raw_snapshot_row,
        )
        .optional()?;
    let Some(raw_row) = raw else {
        return Ok(None);
    };
    let (stored_snapshot, stored_digest) = decode_snapshot_row(raw_row)?;
    if stored_digest != expected_digest || &stored_snapshot != expected_snapshot {
        return Err(OperationsError::Conflict(
            "execution snapshot request_id reused with different snapshot",
        ));
    }
    let ledger = load_ledger_by_snapshot_id(connection, stored_snapshot.snapshot_id)?
        .ok_or(OperationsError::Corrupt("launch ledger missing on replay"))?;
    Ok(Some((stored_snapshot, ledger)))
}

fn load_in_flight_ledgers(
    connection: &Connection,
    workspace_id: Option<WorkspaceId>,
) -> Result<Vec<LaunchLedgerRecord>, OperationsError> {
    let raw_rows = if let Some(ws) = workspace_id {
        let mut statement = connection.prepare(SELECT_IN_FLIGHT_LEDGERS_BY_WORKSPACE_SQL)?;
        statement
            .query_map(
                params![ws.into_uuid().as_bytes(), MAX_BOOT_RECONCILE_ROWS + 1],
                read_raw_ledger_row,
            )?
            .collect::<Result<Vec<_>, _>>()?
    } else {
        let mut statement = connection.prepare(SELECT_IN_FLIGHT_LEDGERS_ALL_SQL)?;
        statement
            .query_map(params![MAX_BOOT_RECONCILE_ROWS + 1], read_raw_ledger_row)?
            .collect::<Result<Vec<_>, _>>()?
    };
    let limit = usize::try_from(MAX_BOOT_RECONCILE_ROWS)
        .map_err(|_| OperationsError::Bounds("boot reconciliation limit"))?;
    if raw_rows.len() > limit {
        return Err(OperationsError::Bounds(
            "in-flight launch ledger rows exceed boot reconciliation bound",
        ));
    }
    raw_rows.into_iter().map(decode_ledger_row).collect()
}

fn snapshot_digest(snapshot: &AssignmentExecutionSnapshot) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(SNAPSHOT_DIGEST_DOMAIN);
    hasher.update(snapshot.snapshot_id.as_bytes());
    hasher.update(snapshot.attempt_key.workspace_id.into_uuid().as_bytes());
    hasher.update(snapshot.attempt_key.goal_id.into_uuid().as_bytes());
    hasher.update(snapshot.attempt_key.dag_revision.get().to_le_bytes());
    hasher.update(snapshot.attempt_key.node_id.into_uuid().as_bytes());
    hasher.update(snapshot.attempt_key.assignment_id.into_uuid().as_bytes());
    hasher.update(snapshot.attempt_key.attempt.to_le_bytes());
    hasher.update(snapshot.activity_id.into_uuid().as_bytes());
    hasher.update(snapshot.profile_id.into_uuid().as_bytes());
    hasher.update(snapshot.profile_revision.get().to_le_bytes());
    hash_str(&mut hasher, snapshot.resolved_engine.as_str());
    hash_str(&mut hasher, &snapshot.resolved_model);
    match &snapshot.resolved_reasoning {
        Some(reasoning) => {
            hasher.update([1_u8]);
            hash_str(&mut hasher, reasoning);
        }
        None => hasher.update([0_u8]),
    }
    match &snapshot.role_prompt_digest {
        Some(prompt) => {
            hasher.update([1_u8]);
            hasher.update(prompt.as_bytes());
        }
        None => hasher.update([0_u8]),
    }
    hash_str(&mut hasher, snapshot.worktree.as_str());
    hasher.update(snapshot.lease.lease_id.as_bytes());
    hasher.update(snapshot.lease.lease_revision.to_le_bytes());
    hasher.update(snapshot.lease.expires_at_ms.to_le_bytes());
    match snapshot.mcp_grant {
        Some(grant) => {
            hasher.update([1_u8]);
            hasher.update(grant.token_id.into_uuid().as_bytes());
            hasher.update(grant.issued_at_ms.to_le_bytes());
            hasher.update(grant.expires_at_ms.to_le_bytes());
        }
        None => hasher.update([0_u8]),
    }
    hasher.update([snapshot.permissions.bits]);
    hasher.update(snapshot.timeouts.startup_timeout.to_le_bytes());
    hasher.update(snapshot.timeouts.execution_timeout.to_le_bytes());
    hasher.update(snapshot.timeouts.termination_grace.to_le_bytes());
    hasher.update(snapshot.committed_at_ms.to_le_bytes());
    hasher.finalize().into()
}

fn decode_non_negative_ts(value: Option<i64>, label: &'static str) -> Result<i64, OperationsError> {
    value
        .filter(|ts| *ts >= 0)
        .ok_or(OperationsError::Corrupt(label))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    type TestResult = Result<(), Box<dyn Error>>;

    const DESTINATION_ID: Uuid = Uuid::from_u128(0x6100);
    const WORKSPACE_ONE: Uuid = Uuid::from_u128(0x6101);
    const WORKSPACE_TWO: Uuid = Uuid::from_u128(0x6102);

    struct TempCatalog {
        dir: PathBuf,
    }

    impl TempCatalog {
        fn open() -> Result<(Self, OperationsCatalog), Box<dyn Error>> {
            let dir = std::env::temp_dir().join(format!(
                "assignment-execution-snapshot-test-{}",
                Uuid::now_v7()
            ));
            std::fs::create_dir_all(&dir)?;
            let guard = Self { dir };
            let catalog =
                OperationsCatalog::open(&guard.dir.join("operations.sqlite3"), DESTINATION_ID)?;
            Ok((guard, catalog))
        }
    }

    impl Drop for TempCatalog {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    struct SnapshotBuilder {
        snapshot_id: Uuid,
        workspace_id: Uuid,
        attempt: u32,
        profile_revision: i64,
        engine: CliEngine,
        model: String,
        reasoning: Option<String>,
        role_prompt: Option<[u8; 32]>,
    }

    impl SnapshotBuilder {
        fn new(tag: u128) -> Self {
            Self {
                snapshot_id: Uuid::from_u128(0x6200 + tag),
                workspace_id: WORKSPACE_ONE,
                attempt: 1,
                profile_revision: 3,
                engine: CliEngine::Codex,
                model: "codex-spark-1".to_owned(),
                reasoning: Some("high".to_owned()),
                role_prompt: Some([0x55_u8; 32]),
            }
        }

        fn workspace_id(mut self, workspace_id: Uuid) -> Self {
            self.workspace_id = workspace_id;
            self
        }

        fn attempt(mut self, attempt: u32) -> Self {
            self.attempt = attempt;
            self
        }

        fn engine_model(mut self, engine: CliEngine, model: &str) -> Self {
            self.engine = engine;
            self.model = model.to_owned();
            self
        }

        fn build_parts(&self) -> Result<AssignmentExecutionSnapshotParts, Box<dyn Error>> {
            let attempt_key = ExecutionAttemptKey::new(
                WorkspaceId::from_uuid(self.workspace_id),
                GoalId::from_uuid(Uuid::from_u128(0x6301)),
                DagRevisionNumber::INITIAL,
                DagNodeId::from_uuid(Uuid::from_u128(0x6302)),
                AgentAssignmentId::from_uuid(Uuid::from_u128(0x6303)),
                self.attempt,
            )?;
            let role_prompt_digest = self.role_prompt.map(RolePromptDigest::new).transpose()?;
            Ok(AssignmentExecutionSnapshotParts {
                snapshot_id: self.snapshot_id,
                attempt_key,
                activity_id: ActivityId::from_uuid(Uuid::from_u128(0x6304)),
                profile_id: AgentProfileId::from_uuid(Uuid::from_u128(0x6305))?,
                profile_revision: AgentProfileRevision::new(self.profile_revision)?,
                resolved_engine: self.engine,
                resolved_model: self.model.clone(),
                resolved_reasoning: self.reasoning.clone(),
                role_prompt_digest,
                worktree: ForgeWorktreeRef::new(PathBuf::from(
                    "/var/tmp/heterarchy-node-worktrees/node-1",
                ))?,
                lease: LeaseRef::new(Uuid::from_u128(0x6306), 1, 90_000)?,
                mcp_grant: Some(McpGrant::new(
                    DirectTokenId::from_uuid(Uuid::from_u128(0x6307)),
                    1_000,
                    60_000,
                )?),
                permissions: ExecutionPermissions::new(true, true, false),
                timeouts: ExecutionTimeouts::new(10_000, 300_000, 5_000)?,
                committed_at_ms: 1_500,
            })
        }

        fn build_preference(&self) -> Result<ExecutionPreference, Box<dyn Error>> {
            let prompt = self.role_prompt.map(RolePromptDigest::new).transpose()?;
            Ok(ExecutionPreference::new(
                self.engine,
                self.model.clone(),
                self.reasoning.clone(),
                vec!["rust".to_owned()],
                prompt,
            )?)
        }

        fn build_resolved(&self) -> Result<AssignmentExecutionSnapshot, Box<dyn Error>> {
            let parts = self.build_parts()?;
            let pref = self.build_preference()?;
            let current_rev = AgentProfileRevision::new(self.profile_revision)?;
            Ok(AssignmentExecutionSnapshot::resolve_from_profile(
                parts,
                Some(&pref),
                current_rev,
            )?)
        }
    }

    #[test]
    fn worktree_ref_dictionary_enforces_canonical_non_ssd_paths() {
        let cases: &[(&str, Result<&str, SnapshotValidationError>)] = &[
            (
                "/var/tmp/heterarchy-node-worktrees/node-1",
                Ok("/var/tmp/heterarchy-node-worktrees/node-1"),
            ),
            (
                "/Volumes/256.SSD/RustBuild",
                Err(SnapshotValidationError::ForbiddenSsdBuildWorktree),
            ),
            (
                "/Volumes/256.SSD/RustBuild/forbidden/worktree",
                Err(SnapshotValidationError::ForbiddenSsdBuildWorktree),
            ),
            (
                "relative/worktree",
                Err(SnapshotValidationError::InvalidWorktreePath(
                    "worktree path must be absolute",
                )),
            ),
            (
                "/var/tmp/../escape",
                Err(SnapshotValidationError::InvalidWorktreePath(
                    "worktree path contains ParentDir segment",
                )),
            ),
            (
                "/var/tmp/./curdir",
                Err(SnapshotValidationError::InvalidWorktreePath(
                    "worktree path contains CurDir segment",
                )),
            ),
        ];

        for (raw, expected) in cases {
            let actual = ForgeWorktreeRef::new(PathBuf::from(raw));
            assert_eq!(
                actual.as_ref().map(ForgeWorktreeRef::as_str),
                expected.as_ref().copied(),
                "worktree case {raw} failed"
            );
        }
    }

    #[test]
    fn profile_resolution_dictionary_rejects_mismatches_and_incompatible_models() -> TestResult {
        let base = SnapshotBuilder::new(1);
        let pref = base.build_preference()?;
        let rev_three = AgentProfileRevision::new(3)?;

        assert_eq!(
            AssignmentExecutionSnapshot::resolve_from_profile(base.build_parts()?, None, rev_three),
            Err(SnapshotValidationError::MissingExecutionPreference)
        );
        assert_eq!(
            AssignmentExecutionSnapshot::resolve_from_profile(
                base.build_parts()?,
                Some(&pref),
                AgentProfileRevision::new(4)?,
            ),
            Err(SnapshotValidationError::ProfileRevisionMismatch)
        );

        let mut wrong_engine = base.build_parts()?;
        wrong_engine.resolved_engine = CliEngine::Agy;
        assert_eq!(
            AssignmentExecutionSnapshot::resolve_from_profile(wrong_engine, Some(&pref), rev_three,),
            Err(SnapshotValidationError::EngineMismatch)
        );

        let incompatible_cases = [
            (CliEngine::Codex, "claude-sonnet-4"),
            (CliEngine::Codex, "gemini-2.5-pro"),
            (CliEngine::Codex, "gpt-5-retired"),
            (CliEngine::Agy, "codex-spark-1"),
        ];
        for (index, (engine, model)) in incompatible_cases.into_iter().enumerate() {
            let builder =
                SnapshotBuilder::new(u128::try_from(index + 10)?).engine_model(engine, model);
            let bad_pref = builder.build_preference()?;
            let outcome = AssignmentExecutionSnapshot::resolve_from_profile(
                builder.build_parts()?,
                Some(&bad_pref),
                rev_three,
            );
            assert_eq!(
                outcome,
                Err(SnapshotValidationError::IncompatibleEngineModel)
            );
        }

        let valid_agy = SnapshotBuilder::new(30).engine_model(CliEngine::Agy, "claude-sonnet-4-6");
        let resolved_agy = valid_agy.build_resolved()?;
        assert_eq!(resolved_agy.resolved_engine(), CliEngine::Agy);
        Ok(())
    }

    #[test]
    fn launch_ledger_enforces_single_spawn_cas_and_boot_reconciliation() -> TestResult {
        let (_guard, mut catalog) = TempCatalog::open()?;
        let snap_prepared = SnapshotBuilder::new(1).attempt(1).build_resolved()?;
        let snap_spawned = SnapshotBuilder::new(2).attempt(2).build_resolved()?;
        let snap_exited = SnapshotBuilder::new(3)
            .workspace_id(WORKSPACE_TWO)
            .attempt(1)
            .build_resolved()?;

        let req_one = Uuid::from_u128(0x6901);
        let (_, ledger_one) =
            catalog.commit_assignment_execution_snapshot(req_one, &snap_prepared)?;
        assert_eq!(ledger_one.state(), &LaunchLedgerState::Prepared);
        assert_eq!(
            catalog
                .commit_assignment_execution_snapshot(req_one, &snap_prepared)?
                .1
                .revision(),
            1
        );

        catalog.commit_assignment_execution_snapshot(Uuid::from_u128(0x6902), &snap_spawned)?;
        catalog.commit_assignment_execution_snapshot(Uuid::from_u128(0x6903), &snap_exited)?;

        let owner_two = ProcessOwnershipIdentity::new(
            4200,
            4200,
            "2026-11-15T12:00:00Z".to_owned(),
            "boot-alpha".to_owned(),
        )?;
        let spawned_two = catalog.record_execution_spawned(
            snap_spawned.attempt_key(),
            1,
            owner_two.clone(),
            2_000,
        )?;
        assert_eq!(spawned_two.revision(), 2);

        let respawn_err = catalog.record_execution_spawned(
            snap_spawned.attempt_key(),
            2,
            owner_two.clone(),
            2_100,
        );
        assert!(matches!(
            respawn_err,
            Err(OperationsError::Conflict(
                "execution attempt cannot be re-spawned"
            ))
        ));

        let owner_three = ProcessOwnershipIdentity::new(
            5100,
            5100,
            "2026-11-15T12:01:00Z".to_owned(),
            "boot-alpha".to_owned(),
        )?;
        catalog.record_execution_spawned(snap_exited.attempt_key(), 1, owner_three, 2_200)?;
        let exited = catalog.record_execution_exited(snap_exited.attempt_key(), 2, 0, 2_500)?;
        assert_eq!(exited.revision(), 3);

        let reconciled = catalog.reconcile_launch_ledger_on_boot(None, "boot-beta", 5_000)?;
        assert_eq!(reconciled.len(), 2);
        assert_eq!(
            reconciled[0].state(),
            &LaunchLedgerState::NotStarted {
                reconciled_at_ms: 5_000
            }
        );
        assert_eq!(
            reconciled[1].state(),
            &LaunchLedgerState::Unknown {
                ownership: Some(owner_two),
                reason: LaunchUnknownReason::BootRecoveryAfterSpawn,
                recorded_at_ms: 5_000,
            }
        );
        Ok(())
    }

    #[test]
    fn unknown_transition_and_readback_preserve_snapshot_and_ownership() -> TestResult {
        let (_guard, mut catalog) = TempCatalog::open()?;
        let snapshot = SnapshotBuilder::new(40).attempt(4).build_resolved()?;
        catalog.commit_assignment_execution_snapshot(Uuid::from_u128(0x6950), &snapshot)?;
        let owner = ProcessOwnershipIdentity::new(
            7700,
            7700,
            "2026-11-15T12:05:00Z".to_owned(),
            "boot-gamma".to_owned(),
        )?;
        catalog.record_execution_spawned(snapshot.attempt_key(), 1, owner.clone(), 3_000)?;
        let unknown = catalog.record_execution_unknown(
            snapshot.attempt_key(),
            2,
            LaunchUnknownReason::ExecutionTimedOut,
            4_000,
        )?;
        assert_eq!(unknown.revision(), 3);
        let readback = catalog
            .get_assignment_execution_snapshot(snapshot.attempt_key())?
            .ok_or("expected committed snapshot")?;
        assert_eq!(readback.0, snapshot);
        assert_eq!(
            readback.1.state(),
            &LaunchLedgerState::Unknown {
                ownership: Some(owner),
                reason: LaunchUnknownReason::ExecutionTimedOut,
                recorded_at_ms: 4_000,
            }
        );
        Ok(())
    }
}
