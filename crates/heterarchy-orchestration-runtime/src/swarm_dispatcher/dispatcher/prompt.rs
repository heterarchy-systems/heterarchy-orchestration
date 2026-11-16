//! Worker wakeup prompt formatting and [`CliExecutionContext`] assembly for
//! dispatched Swarm member turns.

use std::borrow::Cow;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::Path;

use heterarchy_orchestration_operations::SwarmWakeupBatch;

use super::SwarmDispatchTurnRequest;
use crate::execution_supervisor::{
    CliExecutionContext, ExecutionSupervisorError, MAX_CLI_PROMPT_BYTES,
};

const SWARM_BOARD_CLI_INSTRUCTIONS: &str = "\
\n## Swarm Board CLI\n\
Environment variables set for this turn: `HETERARCHY_WORKSPACE_ID`, `HETERARCHY_SWARM_RUN_ID`, `HETERARCHY_SWARM_MEMBER_ID`, `HETERARCHY_ORCHESTRATION_BACKEND_ADDR`, and `HETERARCHY_ORCHESTRATION_BEARER_FILE`.\n\
Use `heterarchy-orchestration-backend` to coordinate on the Swarm board:\n\
- `heterarchy-orchestration-backend swarm post --channel main --body \"...\"`\n\
- `heterarchy-orchestration-backend swarm read --channel main --after-seq <seq>`\n\
- `heterarchy-orchestration-backend swarm status`\n\
- `heterarchy-orchestration-backend swarm verdict --panel-id <uuid> --verdict accepted|rejected --diff-digest <hex> --rationale \"...\"`\n";

pub(super) fn build_swarm_execution_context(
    request: &SwarmDispatchTurnRequest,
    wakeup_batch: &SwarmWakeupBatch,
    orchestration_backend_addr: Option<SocketAddr>,
    worker_bearer_file: Option<&Path>,
) -> Result<CliExecutionContext, ExecutionSupervisorError> {
    let prompt = format_swarm_wakeup_prompt(wakeup_batch);
    let mut context = CliExecutionContext::new()
        .with_prompt(prompt)?
        .with_workspace_id(request.workspace_id)?
        .with_swarm_run_id(request.swarm_run_id)?
        .with_swarm_member_id(request.member_id)?;
    if let Some(addr) = orchestration_backend_addr {
        context = context.with_orchestration_backend_addr(addr)?;
    }
    if let Some(bearer_file) = worker_bearer_file {
        context = context.with_bearer_file(bearer_file.to_path_buf())?;
    }
    Ok(context)
}

pub(super) fn format_swarm_wakeup_prompt(batch: &SwarmWakeupBatch) -> String {
    let member = batch.member();
    let mut prompt = String::new();
    let _ = writeln!(
        &mut prompt,
        "Swarm wakeup delta for @{} (role={:?}, epoch={}, seq={}..={}):",
        member.roster_name(),
        member.role_kind(),
        member.attempt_epoch(),
        batch.committed_seen_seq(),
        batch.inflight_target_seq(),
    );
    if batch.messages().is_empty() {
        let _ = writeln!(
            &mut prompt,
            "(no board messages in claimed delta; inspect swarm status and proceed)"
        );
    } else {
        for msg in batch.messages() {
            let clean_body: Cow<'_, str> = if msg.body().contains('\0') {
                Cow::Owned(msg.body().replace('\0', ""))
            } else {
                Cow::Borrowed(msg.body())
            };
            match msg.author_epoch() {
                Some(epoch) => {
                    let _ = writeln!(
                        &mut prompt,
                        "[#{} #{}:@{} (epoch {})] {}",
                        msg.sequence(),
                        msg.channel_name(),
                        msg.author_roster_name(),
                        epoch,
                        clean_body,
                    );
                }
                None => {
                    let _ = writeln!(
                        &mut prompt,
                        "[#{} #{}:@{}] {}",
                        msg.sequence(),
                        msg.channel_name(),
                        msg.author_roster_name(),
                        clean_body,
                    );
                }
            }
        }
    }
    prompt.push_str(SWARM_BOARD_CLI_INSTRUCTIONS);
    if prompt.len() > MAX_CLI_PROMPT_BYTES {
        truncate_utf8_to_max_bytes(&mut prompt, MAX_CLI_PROMPT_BYTES);
    }
    prompt
}

fn truncate_utf8_to_max_bytes(text: &mut String, max_bytes: usize) {
    const SUFFIX: &str = "\n...[truncated]";
    if text.len() <= max_bytes {
        return;
    }
    let cap = max_bytes.saturating_sub(SUFFIX.len());
    let mut end = cap.min(text.len());
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    if text.len().saturating_add(SUFFIX.len()) <= max_bytes {
        text.push_str(SUFFIX);
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::path::PathBuf;

    use forge_domain::{AuthenticatedPrincipal, DirectTokenId, WorkspaceId};
    use heterarchy_orchestration_operations::{
        AddSwarmMemberCommand, ClaimSwarmWakeupCommand, CreateSwarmRunCommand, MessageContext,
        OperationsCatalog, PostSwarmMessageCommand, SwarmCommitOid, SwarmMemberIdentity,
        SwarmMessageAuthor, SwarmRoleKind, SwarmRunMode, SwarmWorkspaceMode,
    };
    use heterarchy_orchestration_sessions::AgentProfileId;
    use uuid::Uuid;

    use super::*;

    type TestResult<T = ()> = Result<T, Box<dyn Error>>;

    struct TempCatalogDir(PathBuf);

    impl TempCatalogDir {
        fn new() -> TestResult<Self> {
            let path = std::env::temp_dir().join(format!("swarm-prompt-test-{}", Uuid::now_v7()));
            std::fs::create_dir_all(&path)?;
            Ok(Self(path))
        }
    }

    impl Drop for TempCatalogDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    struct TestMemberSpec<'a> {
        member_id: Uuid,
        roster_name: &'a str,
        role_kind: SwarmRoleKind,
        parent_member_id: Option<Uuid>,
        seed: u128,
    }

    fn add_test_member(
        catalog: &mut OperationsCatalog,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
        spec: &TestMemberSpec<'_>,
    ) -> TestResult {
        catalog.add_swarm_member(&AddSwarmMemberCommand {
            member_id: spec.member_id,
            workspace_id,
            swarm_run_id,
            roster_name: spec.roster_name.to_owned(),
            role_kind: spec.role_kind,
            parent_member_id: spec.parent_member_id,
            workspace_mode: SwarmWorkspaceMode::Inherit,
            worktree_path: None,
            identity: SwarmMemberIdentity::new(
                AgentProfileId::from_uuid(Uuid::from_u128(spec.seed))?,
                Uuid::from_u128(spec.seed + 1),
                AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(
                    spec.seed + 2,
                ))),
            )?,
            initial_channels: vec![],
            added_at_ms: 1_010,
        })?;
        Ok(())
    }

    #[test]
    fn format_swarm_wakeup_prompt_includes_cli_contract_and_env_vars() -> TestResult {
        let dir = TempCatalogDir::new()?;
        let mut catalog =
            OperationsCatalog::open(&dir.0.join("ops.sqlite3"), Uuid::from_u128(0xE001))?;
        let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(0xE002));
        let run = catalog.create_swarm_run(&CreateSwarmRunCommand {
            request_id: Uuid::from_u128(0xE003),
            workspace_id,
            title: "Prompt CLI contract test".to_owned(),
            mode: SwarmRunMode::Standalone,
            max_members: 8,
            max_spawn_depth: 2,
            max_total_turns: 32,
            required_verifier_quorum: 1,
            allow_worker_all_mention: false,
            baseline_commit_oid: SwarmCommitOid::new("a".repeat(40))?,
            baseline_tree_blake3: [0x11; 32],
            created_at_ms: 1_000,
        })?;
        let snap = catalog
            .get_swarm_run_snapshot(workspace_id, run.swarm_run_id())?
            .ok_or("missing snapshot")?;
        let main_channel_id = snap.channels()[0].channel_id();

        let coord_id = Uuid::from_u128(0xE010);
        add_test_member(
            &mut catalog,
            workspace_id,
            run.swarm_run_id(),
            &TestMemberSpec {
                member_id: coord_id,
                roster_name: "coordinator",
                role_kind: SwarmRoleKind::Coordinator,
                parent_member_id: None,
                seed: 0xE011,
            },
        )?;
        let worker_id = Uuid::from_u128(0xE020);
        add_test_member(
            &mut catalog,
            workspace_id,
            run.swarm_run_id(),
            &TestMemberSpec {
                member_id: worker_id,
                roster_name: "worker-1",
                role_kind: SwarmRoleKind::Worker,
                parent_member_id: Some(coord_id),
                seed: 0xE021,
            },
        )?;
        catalog.post_swarm_message(&PostSwarmMessageCommand {
            message_id: Uuid::from_u128(0xE030),
            workspace_id,
            swarm_run_id: run.swarm_run_id(),
            channel_id: main_channel_id,
            author: SwarmMessageAuthor::Member(coord_id),
            body: "@worker-1 check board".to_owned(),
            context: MessageContext::default(),
            submitted_at_ms: 1_030,
        })?;
        let batch = catalog.claim_swarm_wakeup_batch(ClaimSwarmWakeupCommand {
            workspace_id,
            swarm_run_id: run.swarm_run_id(),
            member_id: worker_id,
            snapshot_id: Uuid::from_u128(0xE040),
            claimed_at_ms: 1_040,
        })?;

        let prompt = format_swarm_wakeup_prompt(&batch);
        assert!(prompt.len() <= MAX_CLI_PROMPT_BYTES);
        assert!(prompt.contains("Swarm wakeup delta for @worker-1"));
        assert!(prompt.contains("[#1 #main:@coordinator (epoch 0)] @worker-1 check board"));
        assert!(prompt.contains("## Swarm Board CLI"));
        for env_var in [
            "HETERARCHY_WORKSPACE_ID",
            "HETERARCHY_SWARM_RUN_ID",
            "HETERARCHY_SWARM_MEMBER_ID",
            "HETERARCHY_ORCHESTRATION_BACKEND_ADDR",
            "HETERARCHY_ORCHESTRATION_BEARER_FILE",
        ] {
            assert!(prompt.contains(env_var), "missing {env_var}");
        }
        for cmd in [
            "heterarchy-orchestration-backend swarm post --channel main --body \"...\"",
            "heterarchy-orchestration-backend swarm read --channel main --after-seq <seq>",
            "heterarchy-orchestration-backend swarm status",
            "heterarchy-orchestration-backend swarm verdict --panel-id <uuid> --verdict accepted|rejected --diff-digest <hex> --rationale \"...\"",
        ] {
            assert!(prompt.contains(cmd), "missing CLI instruction {cmd}");
        }
        Ok(())
    }

    #[test]
    fn truncate_utf8_preserves_char_boundaries() {
        let mut text = format!("{}한글{}", "a".repeat(50), "b".repeat(40));
        let max_bytes = 52 + "\n...[truncated]".len();
        truncate_utf8_to_max_bytes(&mut text, max_bytes);
        assert!(text.ends_with("\n...[truncated]"));
        assert!(text.len() <= max_bytes);
    }
}
