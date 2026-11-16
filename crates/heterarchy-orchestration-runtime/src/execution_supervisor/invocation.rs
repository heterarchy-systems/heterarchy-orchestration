//! Deterministic `CLI` executable resolution, argument construction, worktree
//! defense-in-depth validation, and host environment scrubbing.

mod environment;
mod path_guard;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use forge_domain::WorkspaceId;
use heterarchy_orchestration_operations::AssignmentExecutionSnapshot;
use heterarchy_orchestration_sessions::CliEngine;
use uuid::Uuid;

use super::ExecutionSupervisorError;
use environment::{
    build_scrubbed_environment, resolve_effective_search_path, sanitize_ambient_host_env,
};
use path_guard::{
    is_allowed_tmpdir, resolve_executable_target, validate_bearer_file,
    validate_search_path_entries,
};

pub use path_guard::validate_worktree_dir;
#[cfg(test)]
pub(super) use path_guard::{is_forbidden_ssd_path, validate_worktree_dir_with_forbidden};

/// Canonical external `SSD` Rust build root that must never be used as an
/// agent execution worktree, executable target, bearer-file location, or child `TMPDIR`.
pub const FORBIDDEN_SSD_BUILD_PREFIX: &str = "/Volumes/256.SSD/RustBuild";

/// Bounded host environment variable allowlist retained after `env_clear()`.
pub const ALLOWLISTED_HOST_ENV_KEYS: [&str; 4] = ["HOME", "LANG", "PATH", "TMPDIR"];

/// Maximum UTF-8 byte length permitted for a single worker `CLI` prompt payload (`128 KiB`).
pub const MAX_CLI_PROMPT_BYTES: usize = 128 * 1024;

const DEFAULT_CODEX_BINARY: &str = "codex";
const DEFAULT_AGY_BINARY: &str = "agy";
const DEFAULT_SAFE_SEARCH_PATH: &str = "/usr/bin:/bin";
const MAX_MODEL_TOKEN_BYTES: usize = 128;
const MAX_REASONING_TIER_BYTES: usize = 64;

/// Bounded, non-secret runtime execution context forwarded to a local `CLI` worker.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct CliExecutionContext {
    prompt: Option<String>,
    workspace_id: Option<WorkspaceId>,
    swarm_run_id: Option<Uuid>,
    swarm_member_id: Option<Uuid>,
    orchestration_backend_addr: Option<SocketAddr>,
    bearer_file: Option<PathBuf>,
}

impl fmt::Debug for CliExecutionContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CliExecutionContext")
            .field("prompt", &self.prompt)
            .field("workspace_id", &self.workspace_id)
            .field("swarm_run_id", &self.swarm_run_id)
            .field("swarm_member_id", &self.swarm_member_id)
            .field(
                "orchestration_backend_addr",
                &self.orchestration_backend_addr,
            )
            .field(
                "bearer_file",
                &self.bearer_file.as_ref().map(|_| "[REDACTED_PATH]"),
            )
            .finish()
    }
}

impl CliExecutionContext {
    /// Construct an empty execution context with no prompt or optional Swarm metadata.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            prompt: None,
            workspace_id: None,
            swarm_run_id: None,
            swarm_member_id: None,
            orchestration_backend_addr: None,
            bearer_file: None,
        }
    }

    /// Attach a bounded worker prompt payload (`1..=MAX_CLI_PROMPT_BYTES` non-whitespace, NUL-free).
    ///
    /// Prompts are always appended after `"--"` on the child command line so a prompt
    /// starting with `-` can never be parsed as an option flag.
    ///
    /// # Errors
    /// Rejects empty/whitespace-only prompts, NUL bytes, and payloads exceeding [`MAX_CLI_PROMPT_BYTES`].
    pub fn with_prompt<S: Into<String>>(
        mut self,
        prompt: S,
    ) -> Result<Self, ExecutionSupervisorError> {
        let prompt = prompt.into();
        validate_prompt(&prompt)?;
        self.prompt = Some(prompt);
        Ok(self)
    }

    /// Attach the expected `WorkspaceId` for cross-workspace consistency verification.
    ///
    /// # Errors
    /// Rejects a nil `WorkspaceId`.
    pub fn with_workspace_id(
        mut self,
        workspace_id: WorkspaceId,
    ) -> Result<Self, ExecutionSupervisorError> {
        if workspace_id.into_uuid().is_nil() {
            return Err(ExecutionSupervisorError::InvalidExecutionContext(
                "workspace_id cannot be nil",
            ));
        }
        self.workspace_id = Some(workspace_id);
        Ok(self)
    }

    /// Attach the active `SwarmRunId` exposed via `HETERARCHY_SWARM_RUN_ID`.
    ///
    /// # Errors
    /// Rejects a nil `swarm_run_id`.
    pub fn with_swarm_run_id(
        mut self,
        swarm_run_id: Uuid,
    ) -> Result<Self, ExecutionSupervisorError> {
        if swarm_run_id.is_nil() {
            return Err(ExecutionSupervisorError::InvalidExecutionContext(
                "swarm_run_id cannot be nil",
            ));
        }
        self.swarm_run_id = Some(swarm_run_id);
        Ok(self)
    }

    /// Attach the active Swarm `member_id` exposed via `HETERARCHY_SWARM_MEMBER_ID`.
    ///
    /// # Errors
    /// Rejects a nil `swarm_member_id`.
    pub fn with_swarm_member_id(
        mut self,
        swarm_member_id: Uuid,
    ) -> Result<Self, ExecutionSupervisorError> {
        if swarm_member_id.is_nil() {
            return Err(ExecutionSupervisorError::InvalidExecutionContext(
                "swarm_member_id cannot be nil",
            ));
        }
        self.swarm_member_id = Some(swarm_member_id);
        Ok(self)
    }

    /// Attach the loopback Orchestration backend address exposed via
    /// `HETERARCHY_ORCHESTRATION_BACKEND_ADDR`.
    ///
    /// # Errors
    /// Rejects non-loopback or zero-port socket addresses.
    pub fn with_orchestration_backend_addr(
        mut self,
        addr: SocketAddr,
    ) -> Result<Self, ExecutionSupervisorError> {
        validate_backend_addr(addr)?;
        self.orchestration_backend_addr = Some(addr);
        Ok(self)
    }

    /// Attach a validated bearer-token file path exposed via
    /// `HETERARCHY_ORCHESTRATION_BEARER_FILE`.
    ///
    /// Token bytes are never read or logged by the supervisor; only file metadata
    /// and path containment are verified.
    ///
    /// # Errors
    /// Rejects empty, relative, `/Volumes/256.SSD/RustBuild`-scoped, missing,
    /// symlinked, non-regular, zero-byte, or oversized (`> 4096` byte) paths.
    pub fn with_bearer_file(mut self, path: PathBuf) -> Result<Self, ExecutionSupervisorError> {
        validate_bearer_file(&path)?;
        self.bearer_file = Some(path);
        Ok(self)
    }

    #[must_use]
    pub fn prompt(&self) -> Option<&str> {
        self.prompt.as_deref()
    }

    #[must_use]
    pub const fn workspace_id(&self) -> Option<WorkspaceId> {
        self.workspace_id
    }

    #[must_use]
    pub const fn swarm_run_id(&self) -> Option<Uuid> {
        self.swarm_run_id
    }

    #[must_use]
    pub const fn swarm_member_id(&self) -> Option<Uuid> {
        self.swarm_member_id
    }

    #[must_use]
    pub const fn orchestration_backend_addr(&self) -> Option<SocketAddr> {
        self.orchestration_backend_addr
    }

    #[must_use]
    pub fn bearer_file(&self) -> Option<&Path> {
        self.bearer_file.as_deref()
    }

    pub(super) fn validate_against_snapshot(
        &self,
        snapshot: &AssignmentExecutionSnapshot,
    ) -> Result<(), ExecutionSupervisorError> {
        if let Some(prompt) = &self.prompt {
            validate_prompt(prompt)?;
        }
        if let Some(workspace_id) = self.workspace_id {
            if workspace_id.into_uuid().is_nil() {
                return Err(ExecutionSupervisorError::InvalidExecutionContext(
                    "workspace_id cannot be nil",
                ));
            }
            if workspace_id != snapshot.attempt_key().workspace_id() {
                return Err(ExecutionSupervisorError::InvalidExecutionContext(
                    "workspace_id does not match snapshot attempt_key",
                ));
            }
        }
        if let Some(run_id) = self.swarm_run_id
            && run_id.is_nil()
        {
            return Err(ExecutionSupervisorError::InvalidExecutionContext(
                "swarm_run_id cannot be nil",
            ));
        }
        if let Some(member_id) = self.swarm_member_id
            && member_id.is_nil()
        {
            return Err(ExecutionSupervisorError::InvalidExecutionContext(
                "swarm_member_id cannot be nil",
            ));
        }
        if let Some(addr) = self.orchestration_backend_addr {
            validate_backend_addr(addr)?;
        }
        if let Some(bearer_file) = &self.bearer_file {
            validate_bearer_file(bearer_file)?;
        }
        Ok(())
    }
}

fn validate_prompt(prompt: &str) -> Result<(), ExecutionSupervisorError> {
    if prompt.trim().is_empty() {
        return Err(ExecutionSupervisorError::InvalidExecutionContext(
            "prompt cannot be empty",
        ));
    }
    if prompt.len() > MAX_CLI_PROMPT_BYTES {
        return Err(ExecutionSupervisorError::InvalidExecutionContext(
            "prompt exceeds MAX_CLI_PROMPT_BYTES",
        ));
    }
    if prompt.as_bytes().contains(&0) {
        return Err(ExecutionSupervisorError::InvalidExecutionContext(
            "prompt cannot contain NUL bytes",
        ));
    }
    Ok(())
}

fn validate_backend_addr(addr: SocketAddr) -> Result<(), ExecutionSupervisorError> {
    if !addr.ip().is_loopback() || addr.port() == 0 {
        return Err(ExecutionSupervisorError::InvalidExecutionContext(
            "orchestration_backend_addr must be a non-zero loopback socket address",
        ));
    }
    Ok(())
}

fn validate_model_token(model: &str) -> Result<&str, ExecutionSupervisorError> {
    if model.is_empty()
        || model.len() > MAX_MODEL_TOKEN_BYTES
        || model.starts_with('-')
        || !model
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b'/' | b':'))
    {
        return Err(ExecutionSupervisorError::InvalidCliArgument { field: "model" });
    }
    Ok(model)
}

fn validate_reasoning_tier(tier: &str) -> Result<&str, ExecutionSupervisorError> {
    if tier.is_empty()
        || tier.len() > MAX_REASONING_TIER_BYTES
        || tier.starts_with('-')
        || !tier
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(ExecutionSupervisorError::InvalidCliArgument { field: "reasoning" });
    }
    Ok(tier)
}

/// Explicit command override used by deterministic process-group supervision
/// tests (such as `/bin/sh -c ...`) without bypassing worktree or environment
/// validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOverride {
    program: PathBuf,
    args: Vec<OsString>,
}

impl CommandOverride {
    /// Construct an explicit command override with an executable path and
    /// ordered argument vector.
    #[must_use]
    pub fn new<P, I, S>(program: P, args: I) -> Self
    where
        P: Into<PathBuf>,
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        Self {
            program: program.into(),
            args: args.into_iter().map(Into::into).collect(),
        }
    }

    #[must_use]
    pub fn program(&self) -> &Path {
        &self.program
    }

    #[must_use]
    pub fn args(&self) -> &[OsString] {
        &self.args
    }
}

/// Validated, secret-free local process invocation plan derived from one
/// [`AssignmentExecutionSnapshot`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliInvocationPlan {
    executable: PathBuf,
    args: Vec<OsString>,
    env: BTreeMap<String, OsString>,
    current_dir: PathBuf,
}

impl CliInvocationPlan {
    #[must_use]
    pub fn executable(&self) -> &Path {
        &self.executable
    }

    #[must_use]
    pub fn args(&self) -> &[OsString] {
        &self.args
    }

    #[must_use]
    pub const fn env(&self) -> &BTreeMap<String, OsString> {
        &self.env
    }

    #[must_use]
    pub fn current_dir(&self) -> &Path {
        &self.current_dir
    }

    /// Re-validate `current_dir`, `PATH`, `TMPDIR`, and `executable` right
    /// before OS process spawn, returning the concrete canonical executable
    /// path outside `/Volumes/256.SSD/RustBuild` and outside `current_dir`.
    ///
    /// # Errors
    /// Rejects mutated/symlinked worktrees, forbidden or empty `PATH` entries,
    /// mutable worktree executables, or missing/broken executable targets.
    pub fn revalidate_before_spawn(&self) -> Result<PathBuf, ExecutionSupervisorError> {
        validate_worktree_dir(&self.current_dir)?;
        let canonical_worktree = fs::canonicalize(&self.current_dir)?;
        if let Some(tmpdir) = self.env.get("TMPDIR")
            && !is_allowed_tmpdir(Path::new(tmpdir))
        {
            return Err(ExecutionSupervisorError::ForbiddenSsdBuildWorktree(
                PathBuf::from(tmpdir),
            ));
        }
        let search_path = self
            .env
            .get("PATH")
            .cloned()
            .unwrap_or_else(|| OsString::from(DEFAULT_SAFE_SEARCH_PATH));
        validate_search_path_entries(&search_path, &self.current_dir, &canonical_worktree)?;
        resolve_executable_target(
            &self.executable,
            &self.current_dir,
            &canonical_worktree,
            &search_path,
            true,
        )
    }
}

/// Maps an [`AssignmentExecutionSnapshot`] into a scrubbed [`CliInvocationPlan`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliExecutableResolver {
    codex_binary: PathBuf,
    agy_binary: PathBuf,
    command_override: Option<CommandOverride>,
}

impl Default for CliExecutableResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl CliExecutableResolver {
    /// Create a resolver using default `PATH`-resolved `codex` and `agy` binary
    /// names.
    #[must_use]
    pub fn new() -> Self {
        Self {
            codex_binary: PathBuf::from(DEFAULT_CODEX_BINARY),
            agy_binary: PathBuf::from(DEFAULT_AGY_BINARY),
            command_override: None,
        }
    }

    /// Configure an explicit binary path or name for [`CliEngine::Codex`].
    #[must_use]
    pub fn with_codex_binary<P: Into<PathBuf>>(mut self, binary: P) -> Self {
        self.codex_binary = binary.into();
        self
    }

    /// Configure an explicit binary path or name for [`CliEngine::Agy`].
    #[must_use]
    pub fn with_agy_binary<P: Into<PathBuf>>(mut self, binary: P) -> Self {
        self.agy_binary = binary.into();
        self
    }

    /// Override the resolved command and arguments while preserving worktree
    /// and environment scrubbing checks.
    #[must_use]
    pub fn with_command_override(mut self, command_override: CommandOverride) -> Self {
        self.command_override = Some(command_override);
        self
    }

    #[must_use]
    pub fn codex_binary(&self) -> &Path {
        &self.codex_binary
    }

    #[must_use]
    pub fn agy_binary(&self) -> &Path {
        &self.agy_binary
    }

    #[must_use]
    pub const fn command_override(&self) -> Option<&CommandOverride> {
        self.command_override.as_ref()
    }

    /// Resolve a [`CliInvocationPlan`] using the current process environment
    /// filtered through [`ALLOWLISTED_HOST_ENV_KEYS`] with ambient `SSD` build
    /// toolchains scrubbed from `PATH` and `TMPDIR`.
    ///
    /// # Errors
    /// Rejects missing, relative, or `/Volumes/256.SSD/RustBuild`-prefixed
    /// worktrees and invalid or forbidden executable paths.
    pub fn resolve(
        &self,
        snapshot: &AssignmentExecutionSnapshot,
    ) -> Result<CliInvocationPlan, ExecutionSupervisorError> {
        self.resolve_with_context(snapshot, &CliExecutionContext::default())
    }

    /// Resolve a [`CliInvocationPlan`] with an explicit non-secret
    /// [`CliExecutionContext`] using the scrubbed current process environment.
    ///
    /// # Errors
    /// Rejects invalid execution contexts, disallowed model/reasoning tokens,
    /// missing/relative/forbidden worktrees, and invalid or forbidden
    /// executable paths.
    pub fn resolve_with_context(
        &self,
        snapshot: &AssignmentExecutionSnapshot,
        context: &CliExecutionContext,
    ) -> Result<CliInvocationPlan, ExecutionSupervisorError> {
        let worktree = snapshot.worktree().as_path();
        validate_worktree_dir(worktree)?;
        let canonical_worktree = fs::canonicalize(worktree)?;
        let sanitized_host_env =
            sanitize_ambient_host_env(std::env::vars_os(), worktree, &canonical_worktree);
        self.resolve_with_env_and_context(snapshot, sanitized_host_env, context)
    }

    /// Resolve a [`CliInvocationPlan`] from an explicit host environment
    /// iterator, validating `PATH` entries and executable targets against
    /// `/Volumes/256.SSD/RustBuild` and mutable worktree paths, and scrubbing
    /// all non-allowlisted keys and any `TMPDIR` pointing into
    /// `/Volumes/256.SSD/RustBuild`.
    ///
    /// # Errors
    /// Rejects missing, relative, or `/Volumes/256.SSD/RustBuild`-prefixed
    /// worktrees, forbidden or empty/worktree `PATH` entries, and invalid,
    /// mutable-worktree, or forbidden executable paths.
    pub fn resolve_with_env<I, K, V>(
        &self,
        snapshot: &AssignmentExecutionSnapshot,
        host_env: I,
    ) -> Result<CliInvocationPlan, ExecutionSupervisorError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: Into<OsString>,
    {
        self.resolve_with_env_and_context(snapshot, host_env, &CliExecutionContext::default())
    }

    /// Resolve a [`CliInvocationPlan`] from an explicit host environment
    /// iterator and non-secret [`CliExecutionContext`].
    ///
    /// # Errors
    /// Rejects invalid execution contexts, disallowed model/reasoning tokens,
    /// missing/relative/forbidden worktrees, forbidden or empty/worktree `PATH`
    /// entries, and invalid, mutable-worktree, or forbidden executable paths.
    pub fn resolve_with_env_and_context<I, K, V>(
        &self,
        snapshot: &AssignmentExecutionSnapshot,
        host_env: I,
        context: &CliExecutionContext,
    ) -> Result<CliInvocationPlan, ExecutionSupervisorError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: Into<OsString>,
    {
        let worktree = snapshot.worktree().as_path();
        validate_worktree_dir(worktree)?;
        context.validate_against_snapshot(snapshot)?;
        let canonical_worktree = fs::canonicalize(worktree)?;

        let collected_env: Vec<(OsString, OsString)> = host_env
            .into_iter()
            .map(|(key, val)| (key.as_ref().to_os_string(), val.into()))
            .collect();
        let effective_path = resolve_effective_search_path(&collected_env);
        validate_search_path_entries(&effective_path, worktree, &canonical_worktree)?;

        let (raw_executable, args) = self.build_command_parts(snapshot, context)?;
        let executable = resolve_executable_target(
            &raw_executable,
            worktree,
            &canonical_worktree,
            &effective_path,
            false,
        )?;
        let env = build_scrubbed_environment(snapshot, context, collected_env, effective_path);
        Ok(CliInvocationPlan {
            executable,
            args,
            env,
            current_dir: worktree.to_path_buf(),
        })
    }

    fn build_command_parts(
        &self,
        snapshot: &AssignmentExecutionSnapshot,
        context: &CliExecutionContext,
    ) -> Result<(PathBuf, Vec<OsString>), ExecutionSupervisorError> {
        if let Some(command_override) = &self.command_override {
            return Ok((
                command_override.program.clone(),
                command_override.args.clone(),
            ));
        }
        let model = validate_model_token(snapshot.resolved_model())?;
        let permissions = snapshot.permissions();
        match snapshot.resolved_engine() {
            CliEngine::Codex => {
                let mut args = vec![
                    OsString::from("exec"),
                    OsString::from("-m"),
                    OsString::from(model),
                ];
                if let Some(reasoning) = snapshot.resolved_reasoning() {
                    let tier = validate_reasoning_tier(reasoning)?;
                    args.push(OsString::from("-c"));
                    args.push(OsString::from(format!("model_reasoning_effort={tier:?}")));
                }
                let sandbox_mode = if permissions.allow_workspace_write() {
                    "workspace-write"
                } else {
                    "read-only"
                };
                args.push(OsString::from("-s"));
                args.push(OsString::from(sandbox_mode));
                if !permissions.allow_network() {
                    args.push(OsString::from("-c"));
                    args.push(OsString::from(
                        "sandbox_workspace_write.network_access=false",
                    ));
                }
                if let Some(prompt) = context.prompt() {
                    // Always place `--` before the positional prompt so prompts starting with `-`
                    // cannot be parsed as `codex exec` flags.
                    args.push(OsString::from("--"));
                    args.push(OsString::from(prompt));
                }
                Ok((self.codex_binary.clone(), args))
            }
            CliEngine::Agy => {
                let mut args = vec![
                    OsString::from("--print"),
                    OsString::from("--disable-slash-commands"),
                    OsString::from("--model"),
                    OsString::from(model),
                ];
                if let Some(reasoning) = snapshot.resolved_reasoning() {
                    let tier = validate_reasoning_tier(reasoning)?;
                    args.push(OsString::from("--effort"));
                    args.push(OsString::from(tier));
                }
                let mode = if permissions.allow_workspace_write() {
                    "accept-edits"
                } else {
                    "plan"
                };
                args.push(OsString::from("--mode"));
                args.push(OsString::from(mode));
                if !permissions.allow_workspace_write()
                    || !permissions.allow_local_shell()
                    || !permissions.allow_network()
                {
                    args.push(OsString::from("--sandbox"));
                }
                if let Some(prompt) = context.prompt() {
                    // Always place `--` before the positional prompt so prompts starting with `-`
                    // cannot be parsed as `agy` flags.
                    args.push(OsString::from("--"));
                    args.push(OsString::from(prompt));
                }
                Ok((self.agy_binary.clone(), args))
            }
        }
    }
}
