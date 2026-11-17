use std::error::Error;
use std::ffi::OsString;
use std::fs;
use std::os::unix::ffi::OsStringExt as _;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

use forge_domain::{
    ActivityId, AgentAssignmentId, DagNodeId, DagRevisionNumber, DirectTokenId, GoalId, WorkspaceId,
};
use heterarchy_orchestration_operations::{
    AssignmentExecutionSnapshot, AssignmentExecutionSnapshotParts, ExecutionAttemptKey,
    ExecutionPermissions, ExecutionTimeouts, ForgeWorktreeRef, LeaseRef, McpGrant,
};
use heterarchy_orchestration_sessions::{
    AgentProfileId, AgentProfileRevision, CliEngine, ExecutionPreference, RolePromptDigest,
};
use uuid::Uuid;

use super::path_guard::MAX_BEARER_FILE_BYTES;
use super::{
    CliExecutableResolver, CliExecutionContext, ExecutionSupervisorError,
    FORBIDDEN_SSD_BUILD_PREFIX, is_forbidden_ssd_path,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

struct ScratchDir {
    path: PathBuf,
}

impl ScratchDir {
    fn new_non_ssd(prefix: &str) -> TestResult<Self> {
        let ambient_tmp = std::env::temp_dir();
        let base = if is_forbidden_ssd_path(&ambient_tmp) {
            PathBuf::from("/var/tmp")
        } else {
            ambient_tmp
        };
        let path = base.join(format!("{prefix}-{}", Uuid::now_v7()));
        fs::create_dir_all(&path)?;
        Ok(Self { path })
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn sample_snapshot(workspace_id: WorkspaceId) -> TestResult<AssignmentExecutionSnapshot> {
    let attempt_key = ExecutionAttemptKey::new(
        workspace_id,
        GoalId::from_uuid(Uuid::from_u128(0xAA01)),
        DagRevisionNumber::new(1)?,
        DagNodeId::from_uuid(Uuid::from_u128(0xAA02)),
        AgentAssignmentId::from_uuid(Uuid::from_u128(0xAA03)),
        1,
    )?;
    let profile_revision = AgentProfileRevision::new(1)?;
    let digest = RolePromptDigest::new([0x33; 32])?;
    let preference = ExecutionPreference::new(
        CliEngine::Codex,
        "gpt-6-luna".to_owned(),
        Some("high".to_owned()),
        vec!["rust".to_owned()],
        Some(digest),
    )?;
    let parts = AssignmentExecutionSnapshotParts {
        snapshot_id: Uuid::from_u128(0xAA04),
        attempt_key,
        activity_id: ActivityId::from_uuid(Uuid::from_u128(0xAA05)),
        profile_id: AgentProfileId::from_uuid(Uuid::from_u128(0xAA06))?,
        profile_revision,
        resolved_engine: CliEngine::Codex,
        resolved_model: "gpt-6-luna".to_owned(),
        resolved_reasoning: Some("high".to_owned()),
        role_prompt_digest: Some(digest),
        worktree: ForgeWorktreeRef::new(PathBuf::from(env!("CARGO_MANIFEST_DIR")))?,
        lease: LeaseRef::new(Uuid::from_u128(0xAA07), 1, 1_700_000_060_000)?,
        mcp_grant: Some(McpGrant::new(
            DirectTokenId::from_uuid(Uuid::from_u128(0xAA08)),
            1_700_000_000_000,
            1_700_000_060_000,
        )?),
        permissions: ExecutionPermissions::new(true, true, false),
        timeouts: ExecutionTimeouts::new(1_000, 5_000, 300)?,
        committed_at_ms: 1_700_000_001_000,
    };
    Ok(AssignmentExecutionSnapshot::resolve_from_profile(
        parts,
        Some(&preference),
        profile_revision,
    )?)
}

#[test]
fn bearer_file_and_workspace_id_inject_into_scrubbed_environment_without_reading_secret()
-> TestResult {
    let scratch = ScratchDir::new_non_ssd("inv-bearer-valid")?;
    let token_path = scratch.path.join("worker.bearer");
    let secret_sentinel = "tok-must-never-be-read-or-logged";
    fs::write(&token_path, secret_sentinel)?;

    let max_len_path = scratch.path.join("max-4096.bearer");
    let max_bytes = usize::try_from(MAX_BEARER_FILE_BYTES)?;
    fs::write(&max_len_path, vec![b'x'; max_bytes])?;

    let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(0xBB01));
    let run_id = Uuid::from_u128(0xBB02);
    let member_id = Uuid::from_u128(0xBB03);
    let backend_addr = "127.0.0.1:8798".parse()?;

    let context = CliExecutionContext::new()
        .with_workspace_id(workspace_id)?
        .with_swarm_run_id(run_id)?
        .with_swarm_member_id(member_id)?
        .with_orchestration_backend_addr(backend_addr)?
        .with_bearer_file(token_path.clone())?;
    assert_eq!(context.bearer_file(), Some(token_path.as_path()));

    let debug_str = format!("{context:?}");
    assert!(debug_str.contains("[REDACTED_PATH]"));
    assert!(!debug_str.contains(&token_path.display().to_string()));
    assert!(!debug_str.contains(secret_sentinel));

    let snapshot = sample_snapshot(workspace_id)?;
    let resolver = CliExecutableResolver::new().with_codex_binary("/usr/local/bin/codex");
    let plan = resolver.resolve_with_env_and_context(
        &snapshot,
        [
            ("PATH", "/usr/bin:/bin"),
            ("HETERARCHY_WORKSPACE_ID", "spoofed-workspace"),
            ("HETERARCHY_ORCHESTRATION_BEARER_FILE", "/spoofed/bearer"),
        ],
        &context,
    )?;

    assert_eq!(
        plan.env().get("HETERARCHY_WORKSPACE_ID"),
        Some(&OsString::from(workspace_id.to_string()))
    );
    assert_eq!(
        plan.env().get("HETERARCHY_SWARM_RUN_ID"),
        Some(&OsString::from(run_id.to_string()))
    );
    assert_eq!(
        plan.env().get("HETERARCHY_SWARM_MEMBER_ID"),
        Some(&OsString::from(member_id.to_string()))
    );
    assert_eq!(
        plan.env().get("HETERARCHY_ORCHESTRATION_BACKEND_ADDR"),
        Some(&OsString::from("127.0.0.1:8798"))
    );
    assert_eq!(
        plan.env().get("HETERARCHY_ORCHESTRATION_BEARER_FILE"),
        Some(&token_path.as_os_str().to_os_string())
    );

    // Exact 4096-byte boundary is accepted; ambient HETERARCHY_ORCHESTRATION_BEARER_FILE
    // is scrubbed when context.bearer_file() is None.
    let max_ctx = CliExecutionContext::new().with_bearer_file(max_len_path)?;
    assert!(max_ctx.bearer_file().is_some());
    let no_bearer_plan = resolver.resolve_with_env_and_context(
        &snapshot,
        [
            ("PATH", "/usr/bin:/bin"),
            ("HETERARCHY_ORCHESTRATION_BEARER_FILE", "/spoofed/bearer"),
        ],
        &CliExecutionContext::default(),
    )?;
    assert!(
        !no_bearer_plan
            .env()
            .contains_key("HETERARCHY_ORCHESTRATION_BEARER_FILE")
    );
    Ok(())
}

#[test]
fn with_bearer_file_rejects_empty_relative_missing_symlink_oversized_and_ssd_paths() -> TestResult {
    let scratch = ScratchDir::new_non_ssd("inv-bearer-reject")?;
    let real_file = scratch.path.join("real.bearer");
    fs::write(&real_file, "token-ok")?;

    assert!(matches!(
        CliExecutionContext::new().with_bearer_file(PathBuf::new()),
        Err(ExecutionSupervisorError::InvalidExecutionContext(_))
    ));
    assert!(matches!(
        CliExecutionContext::new().with_bearer_file(PathBuf::from("relative/token")),
        Err(ExecutionSupervisorError::InvalidExecutionContext(_))
    ));
    let nul_path = PathBuf::from(OsString::from_vec(b"/var/tmp/bad\0token".to_vec()));
    assert!(matches!(
        CliExecutionContext::new().with_bearer_file(nul_path),
        Err(ExecutionSupervisorError::InvalidExecutionContext(_))
    ));
    assert!(matches!(
        CliExecutionContext::new().with_bearer_file(scratch.path.join("missing.bearer")),
        Err(ExecutionSupervisorError::InvalidExecutionContext(_))
    ));
    assert!(matches!(
        CliExecutionContext::new().with_bearer_file(scratch.path.clone()),
        Err(ExecutionSupervisorError::InvalidExecutionContext(_))
    ));

    let symlink_path = scratch.path.join("link.bearer");
    symlink(&real_file, &symlink_path)?;
    assert!(matches!(
        CliExecutionContext::new().with_bearer_file(symlink_path),
        Err(ExecutionSupervisorError::InvalidExecutionContext(_))
    ));

    let empty_file = scratch.path.join("empty.bearer");
    fs::write(&empty_file, b"")?;
    assert!(matches!(
        CliExecutionContext::new().with_bearer_file(empty_file),
        Err(ExecutionSupervisorError::InvalidExecutionContext(_))
    ));

    let oversized_file = scratch.path.join("oversized.bearer");
    let over_bytes = usize::try_from(MAX_BEARER_FILE_BYTES + 1)?;
    fs::write(&oversized_file, vec![b'a'; over_bytes])?;
    assert!(matches!(
        CliExecutionContext::new().with_bearer_file(oversized_file),
        Err(ExecutionSupervisorError::InvalidExecutionContext(_))
    ));

    for ssd_path in [
        PathBuf::from(format!("{FORBIDDEN_SSD_BUILD_PREFIX}/bearer")),
        PathBuf::from("/volumes/256.ssd/rustbuild/case-variant/bearer"),
        PathBuf::from("/var/tmp/../../Volumes/256.SSD/RustBuild/traversal/bearer"),
    ] {
        assert!(matches!(
            CliExecutionContext::new().with_bearer_file(ssd_path),
            Err(ExecutionSupervisorError::InvalidExecutionContext(_))
        ));
    }

    // Ancestor links into the forbidden root are rejected even when it is absent.
    let ssd_root_link = scratch.path.join("ssd-root-link");
    symlink(FORBIDDEN_SSD_BUILD_PREFIX, &ssd_root_link)?;
    assert!(matches!(
        CliExecutionContext::new().with_bearer_file(ssd_root_link.join("real.bearer")),
        Err(ExecutionSupervisorError::InvalidExecutionContext(
            "bearer_file cannot reside under /Volumes/256.SSD/RustBuild"
        ))
    ));

    // Real SSD files require the mount, independently of the runner's TMPDIR.
    if Path::new(FORBIDDEN_SSD_BUILD_PREFIX).is_dir() {
        let path = Path::new(FORBIDDEN_SSD_BUILD_PREFIX)
            .join(format!("inv-bearer-ssd-{}", Uuid::now_v7()));
        fs::create_dir(&path)?;
        let ssd_dir = ScratchDir { path };
        let ssd_real_file = ssd_dir.path.join("real.bearer");
        fs::write(&ssd_real_file, "token-on-ssd")?;
        assert!(matches!(
            CliExecutionContext::new().with_bearer_file(ssd_real_file),
            Err(ExecutionSupervisorError::InvalidExecutionContext(_))
        ));

        let ssd_dir_link = scratch.path.join("ssd-dir-link");
        symlink(&ssd_dir.path, &ssd_dir_link)?;
        assert!(matches!(
            CliExecutionContext::new().with_bearer_file(ssd_dir_link.join("real.bearer")),
            Err(ExecutionSupervisorError::InvalidExecutionContext(_))
        ));
    }

    // Re-validation before spawn catches post-builder replacement with a symlink.
    let mut_file = scratch.path.join("mutating.bearer");
    fs::write(&mut_file, "initial-token")?;
    let ctx = CliExecutionContext::new().with_bearer_file(mut_file.clone())?;
    fs::remove_file(&mut_file)?;
    symlink(&real_file, &mut_file)?;
    let snapshot = sample_snapshot(WorkspaceId::from_uuid(Uuid::from_u128(0xBB09)))?;
    let resolver = CliExecutableResolver::new().with_codex_binary("/usr/local/bin/codex");
    assert!(matches!(
        resolver.resolve_with_env_and_context(&snapshot, [("PATH", "/usr/bin:/bin")], &ctx),
        Err(ExecutionSupervisorError::InvalidExecutionContext(_))
    ));
    Ok(())
}
