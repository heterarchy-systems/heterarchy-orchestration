use std::error::Error;
use std::path::PathBuf;

use forge_domain::{
    AuthenticatedPrincipal, DagNodeId, DagRevisionNumber, DirectTokenId, GoalId, OAuthGrantId,
    WorkspaceId,
};
use heterarchy_orchestration_sessions::AgentProfileId;
use rusqlite::params;
use uuid::Uuid;

use super::*;
use crate::{
    ForgeWorktreeRef, MessageContext, OPERATIONS_SCHEMA_VERSION, OperationsCatalog,
    ReviewVerdictOutcome,
};

type TestResult = Result<(), Box<dyn Error>>;

const DESTINATION_ID: Uuid = Uuid::from_u128(0x9100_0000_0001);

struct TempCatalog {
    dir: PathBuf,
    path: PathBuf,
}

impl TempCatalog {
    fn open() -> Result<(Self, OperationsCatalog), Box<dyn Error>> {
        let dir = std::env::temp_dir().join(format!("swarm-ops-test-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("operations.sqlite3");
        let guard = Self { dir, path };
        let catalog = OperationsCatalog::open(&guard.path, DESTINATION_ID)?;
        Ok((guard, catalog))
    }

    fn reopen(&self) -> Result<OperationsCatalog, Box<dyn Error>> {
        Ok(OperationsCatalog::open(&self.path, DESTINATION_ID)?)
    }

    fn open_read_only(&self) -> Result<OperationsCatalog, Box<dyn Error>> {
        Ok(OperationsCatalog::open_existing_read_only(
            &self.path,
            DESTINATION_ID,
        )?)
    }
}

impl Drop for TempCatalog {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn ws(seed: u128) -> WorkspaceId {
    WorkspaceId::from_uuid(Uuid::from_u128(seed))
}

fn commit(fill: char) -> Result<SwarmCommitOid, Box<dyn Error>> {
    Ok(SwarmCommitOid::new(
        std::iter::repeat_n(fill, 40).collect::<String>(),
    )?)
}

fn digest(byte: u8) -> [u8; 32] {
    [byte; 32]
}

fn identity(seed: u128) -> Result<SwarmMemberIdentity, Box<dyn Error>> {
    let profile = AgentProfileId::from_uuid(Uuid::from_u128(0xA000 + seed))?;
    let participant = Uuid::from_u128(0xB000 + seed);
    let principal = AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(
        0xC000 + seed,
    )));
    Ok(SwarmMemberIdentity::new(profile, participant, principal)?)
}

fn standalone_run_cmd(
    request_seed: u128,
    workspace_id: WorkspaceId,
) -> Result<CreateSwarmRunCommand, Box<dyn Error>> {
    Ok(CreateSwarmRunCommand {
        request_id: Uuid::from_u128(request_seed),
        workspace_id,
        title: "Swarm integration test".to_owned(),
        mode: SwarmRunMode::Standalone,
        max_members: 16,
        max_spawn_depth: 4,
        max_total_turns: 100,
        required_verifier_quorum: 1,
        allow_worker_all_mention: false,
        baseline_commit_oid: commit('a')?,
        baseline_tree_blake3: digest(0x11),
        created_at_ms: 1_000,
    })
}

struct MemberSpec<'a> {
    member_seed: u128,
    roster_name: &'a str,
    role_kind: SwarmRoleKind,
    parent_member_id: Option<Uuid>,
    workspace_mode: SwarmWorkspaceMode,
    identity_seed: u128,
    added_at_ms: i64,
}

fn build_member_cmd(
    workspace_id: WorkspaceId,
    swarm_run_id: Uuid,
    spec: &MemberSpec<'_>,
) -> Result<AddSwarmMemberCommand, Box<dyn Error>> {
    let worktree_path = match spec.workspace_mode {
        SwarmWorkspaceMode::Inherit => None,
        SwarmWorkspaceMode::Share | SwarmWorkspaceMode::Branch => Some(ForgeWorktreeRef::new(
            PathBuf::from(format!("/tmp/swarm-wt-{}", spec.member_seed)),
        )?),
    };
    Ok(AddSwarmMemberCommand {
        member_id: Uuid::from_u128(spec.member_seed),
        workspace_id,
        swarm_run_id,
        roster_name: spec.roster_name.to_owned(),
        role_kind: spec.role_kind,
        parent_member_id: spec.parent_member_id,
        workspace_mode: spec.workspace_mode,
        worktree_path,
        identity: identity(spec.identity_seed)?,
        initial_channels: Vec::new(),
        added_at_ms: spec.added_at_ms,
    })
}

fn add_member(
    catalog: &mut OperationsCatalog,
    run: &SwarmRunRecord,
    spec: &MemberSpec<'_>,
) -> Result<SwarmMemberRecord, Box<dyn Error>> {
    let cmd = build_member_cmd(run.workspace_id(), run.swarm_run_id(), spec)?;
    Ok(catalog.add_swarm_member(&cmd)?)
}

fn setup_triad(
    catalog: &mut OperationsCatalog,
    run: &SwarmRunRecord,
    base_seed: u128,
) -> Result<(SwarmMemberRecord, SwarmMemberRecord, SwarmMemberRecord), Box<dyn Error>> {
    let coord = add_member(
        catalog,
        run,
        &MemberSpec {
            member_seed: base_seed,
            roster_name: "coordinator",
            role_kind: SwarmRoleKind::Coordinator,
            parent_member_id: None,
            workspace_mode: SwarmWorkspaceMode::Inherit,
            identity_seed: 10,
            added_at_ms: 1_100,
        },
    )?;
    let worker = add_member(
        catalog,
        run,
        &MemberSpec {
            member_seed: base_seed + 1,
            roster_name: "worker-1",
            role_kind: SwarmRoleKind::Worker,
            parent_member_id: Some(coord.member_id()),
            workspace_mode: SwarmWorkspaceMode::Share,
            identity_seed: 11,
            added_at_ms: 1_101,
        },
    )?;
    let verifier = add_member(
        catalog,
        run,
        &MemberSpec {
            member_seed: base_seed + 2,
            roster_name: "verifier-1",
            role_kind: SwarmRoleKind::Verifier,
            parent_member_id: Some(coord.member_id()),
            workspace_mode: SwarmWorkspaceMode::Inherit,
            identity_seed: 12,
            added_at_ms: 1_102,
        },
    )?;
    Ok((coord, worker, verifier))
}

fn claim_batch(
    catalog: &mut OperationsCatalog,
    run: &SwarmRunRecord,
    member_id: Uuid,
    snap_seed: u128,
    claimed_at_ms: i64,
) -> Result<SwarmWakeupBatch, SwarmError> {
    catalog.claim_swarm_wakeup_batch(ClaimSwarmWakeupCommand {
        workspace_id: run.workspace_id(),
        swarm_run_id: run.swarm_run_id(),
        member_id,
        snapshot_id: Uuid::from_u128(snap_seed),
        claimed_at_ms,
    })
}

fn post_msg(
    catalog: &mut OperationsCatalog,
    run: &SwarmRunRecord,
    channel_id: Uuid,
    author: SwarmMessageAuthor,
    msg_seed: u128,
    body: &str,
    submitted_at_ms: i64,
) -> Result<SwarmPostMessageOutcome, SwarmError> {
    catalog.post_swarm_message(&PostSwarmMessageCommand {
        message_id: Uuid::from_u128(msg_seed),
        workspace_id: run.workspace_id(),
        swarm_run_id: run.swarm_run_id(),
        channel_id,
        author,
        body: body.to_owned(),
        context: MessageContext::default(),
        submitted_at_ms,
    })
}

#[test]
fn schema_v1_open_reopen_and_readonly_preserve_swarm_tables() -> TestResult {
    let (guard, mut catalog) = TempCatalog::open()?;
    let workspace_id = ws(0x1001);
    let cmd = standalone_run_cmd(0x1002, workspace_id)?;
    let run = catalog.create_swarm_run(&cmd)?;
    assert_eq!(run.state(), SwarmRunState::Active);
    assert!(!run.replayed());
    let replayed = catalog.create_swarm_run(&cmd)?;
    assert!(replayed.replayed());
    assert_eq!(replayed.swarm_run_id(), run.swarm_run_id());

    let main = catalog
        .get_swarm_channel_by_name(workspace_id, run.swarm_run_id(), SWARM_MAIN_CHANNEL_NAME)?
        .ok_or("auto-created main channel missing")?;
    assert_eq!(main.name(), SWARM_MAIN_CHANNEL_NAME);
    drop(catalog);

    let reopened = guard.reopen()?;
    let version: i64 = reopened
        .connection
        .pragma_query_value(None, "user_version", |row| row.get(0))?;
    assert_eq!(version, OPERATIONS_SCHEMA_VERSION);
    let loaded = reopened
        .get_swarm_run(workspace_id, run.swarm_run_id())?
        .ok_or("reopened swarm run missing")?;
    assert_eq!(loaded.swarm_run_id(), run.swarm_run_id());
    drop(reopened);

    let readonly = guard.open_read_only()?;
    let snapshot = readonly
        .get_swarm_run_snapshot(workspace_id, run.swarm_run_id())?
        .ok_or("readonly snapshot missing")?;
    assert_eq!(snapshot.channels().len(), 1);
    Ok(())
}

#[test]
fn two_phase_watermark_crash_and_quota_preserve_committed_seen_seq() -> TestResult {
    let (_guard, mut catalog) = TempCatalog::open()?;
    let workspace_id = ws(0x2001);
    let run = catalog.create_swarm_run(&standalone_run_cmd(0x2002, workspace_id)?)?;
    let main_ch = catalog
        .get_swarm_channel_by_name(workspace_id, run.swarm_run_id(), "main")?
        .ok_or("main missing")?;

    let (coord, worker, _) = setup_triad(&mut catalog, &run, 0x2101)?;

    post_msg(
        &mut catalog,
        &run,
        main_ch.channel_id(),
        SwarmMessageAuthor::Member(coord.member_id()),
        0x2201,
        "@worker-1 task 1",
        1_200,
    )?;
    let post2 = post_msg(
        &mut catalog,
        &run,
        main_ch.channel_id(),
        SwarmMessageAuthor::Member(coord.member_id()),
        0x2202,
        "@worker-1 task 2",
        1_201,
    )?;
    assert_eq!(post2.coalesced_doorbell_member_ids(), &[worker.member_id()]);

    let batch1 = claim_batch(&mut catalog, &run, worker.member_id(), 0x2301, 1_300)?;
    assert_eq!(batch1.committed_seen_seq(), 0);
    assert_eq!(batch1.inflight_target_seq(), 2);

    post_msg(
        &mut catalog,
        &run,
        main_ch.channel_id(),
        SwarmMessageAuthor::Member(coord.member_id()),
        0x2203,
        "@worker-1 task 3 mid-turn",
        1_350,
    )?;

    let crashed = catalog.fail_or_suspend_swarm_member_turn(FailOrSuspendSwarmTurnCommand {
        workspace_id,
        swarm_run_id: run.swarm_run_id(),
        member_id: worker.member_id(),
        expected_snapshot_id: batch1.snapshot_id(),
        disposition: SwarmTurnFailureDisposition::TransientRetry,
        consume_turn: false,
        observed_at_ms: 1_400,
    })?;
    assert_eq!(crashed.committed_seen_seq(), 0);
    assert_eq!(crashed.member_state(), SwarmMemberState::WakeQueued);

    let batch2 = claim_batch(&mut catalog, &run, worker.member_id(), 0x2302, 1_450)?;
    assert_eq!(batch2.messages().len(), 3);
    assert_eq!(batch2.inflight_target_seq(), 3);

    let suspended = catalog.fail_or_suspend_swarm_member_turn(FailOrSuspendSwarmTurnCommand {
        workspace_id,
        swarm_run_id: run.swarm_run_id(),
        member_id: worker.member_id(),
        expected_snapshot_id: batch2.snapshot_id(),
        disposition: SwarmTurnFailureDisposition::QuotaExceeded,
        consume_turn: true,
        observed_at_ms: 1_500,
    })?;
    assert_eq!(suspended.committed_seen_seq(), 0);
    assert_eq!(suspended.member_state(), SwarmMemberState::SuspendedQuota);

    let new_profile = AgentProfileId::from_uuid(Uuid::from_u128(0x9999_0002))?;
    let requeued =
        catalog.switch_swarm_member_profile_and_requeue(SwitchSwarmMemberProfileCommand {
            workspace_id,
            swarm_run_id: run.swarm_run_id(),
            member_id: worker.member_id(),
            new_profile_id: new_profile,
            updated_at_ms: 1_550,
        })?;
    assert_eq!(requeued.committed_seen_seq(), 0);
    assert_eq!(requeued.member_state(), SwarmMemberState::WakeQueued);
    assert_eq!(requeued.identity().profile_id(), new_profile);
    Ok(())
}

#[test]
fn working_doorbell_arrival_transitions_clean_completion_to_wake_queued() -> TestResult {
    let (_guard, mut catalog) = TempCatalog::open()?;
    let workspace_id = ws(0x2501);
    let run = catalog.create_swarm_run(&standalone_run_cmd(0x2502, workspace_id)?)?;
    let main_ch = catalog
        .get_swarm_channel_by_name(workspace_id, run.swarm_run_id(), "main")?
        .ok_or("main missing")?;
    let coord = catalog.add_swarm_member(&build_member_cmd(
        workspace_id,
        run.swarm_run_id(),
        &MemberSpec {
            member_seed: 0x2511,
            roster_name: "coordinator",
            role_kind: SwarmRoleKind::Coordinator,
            parent_member_id: None,
            workspace_mode: SwarmWorkspaceMode::Inherit,
            identity_seed: 1,
            added_at_ms: 1_100,
        },
    )?)?;
    let worker = catalog.add_swarm_member(&build_member_cmd(
        workspace_id,
        run.swarm_run_id(),
        &MemberSpec {
            member_seed: 0x2512,
            roster_name: "worker-b",
            role_kind: SwarmRoleKind::Worker,
            parent_member_id: Some(coord.member_id()),
            workspace_mode: SwarmWorkspaceMode::Share,
            identity_seed: 2,
            added_at_ms: 1_101,
        },
    )?)?;

    post_msg(
        &mut catalog,
        &run,
        main_ch.channel_id(),
        SwarmMessageAuthor::Member(coord.member_id()),
        0x2521,
        "@worker-b first",
        1_200,
    )?;
    let snap1 = Uuid::from_u128(0x2531);
    catalog.claim_swarm_wakeup_batch(ClaimSwarmWakeupCommand {
        workspace_id,
        swarm_run_id: run.swarm_run_id(),
        member_id: worker.member_id(),
        snapshot_id: snap1,
        claimed_at_ms: 1_250,
    })?;
    post_msg(
        &mut catalog,
        &run,
        main_ch.channel_id(),
        SwarmMessageAuthor::Member(coord.member_id()),
        0x2522,
        "@worker-b follow-up while working",
        1_260,
    )?;
    let completed = catalog.complete_swarm_member_turn(&CompleteSwarmTurnCommand {
        workspace_id,
        swarm_run_id: run.swarm_run_id(),
        member_id: worker.member_id(),
        expected_snapshot_id: snap1,
        output: Some(SwarmTurnOutput {
            commit_oid: commit('b')?,
            tree_blake3: digest(0x22),
        }),
        completed_at_ms: 1_300,
    })?;
    assert_eq!(completed.committed_seen_seq(), 1);
    assert_eq!(completed.member_state(), SwarmMemberState::WakeQueued);

    let snap2 = Uuid::from_u128(0x2532);
    let next_batch = catalog.claim_swarm_wakeup_batch(ClaimSwarmWakeupCommand {
        workspace_id,
        swarm_run_id: run.swarm_run_id(),
        member_id: worker.member_id(),
        snapshot_id: snap2,
        claimed_at_ms: 1_310,
    })?;
    assert_eq!(next_batch.messages().len(), 1);
    assert_eq!(next_batch.messages()[0].sequence(), 2);
    let waiting = catalog.complete_swarm_member_turn(&CompleteSwarmTurnCommand {
        workspace_id,
        swarm_run_id: run.swarm_run_id(),
        member_id: worker.member_id(),
        expected_snapshot_id: snap2,
        output: None,
        completed_at_ms: 1_350,
    })?;
    assert_eq!(waiting.committed_seen_seq(), 2);
    assert_eq!(waiting.member_state(), SwarmMemberState::Waiting);
    Ok(())
}

#[test]
fn coalescing_enforces_32_message_bound() -> TestResult {
    let (_guard, mut catalog) = TempCatalog::open()?;
    let workspace_id = ws(0x3001);
    let run = catalog.create_swarm_run(&standalone_run_cmd(0x3002, workspace_id)?)?;
    let main_ch = catalog
        .get_swarm_channel_by_name(workspace_id, run.swarm_run_id(), "main")?
        .ok_or("main missing")?;
    let coord = catalog.add_swarm_member(&build_member_cmd(
        workspace_id,
        run.swarm_run_id(),
        &MemberSpec {
            member_seed: 0x3101,
            roster_name: "coordinator",
            role_kind: SwarmRoleKind::Coordinator,
            parent_member_id: None,
            workspace_mode: SwarmWorkspaceMode::Inherit,
            identity_seed: 1,
            added_at_ms: 1_100,
        },
    )?)?;
    let worker = catalog.add_swarm_member(&build_member_cmd(
        workspace_id,
        run.swarm_run_id(),
        &MemberSpec {
            member_seed: 0x3102,
            roster_name: "worker-count",
            role_kind: SwarmRoleKind::Worker,
            parent_member_id: Some(coord.member_id()),
            workspace_mode: SwarmWorkspaceMode::Share,
            identity_seed: 2,
            added_at_ms: 1_101,
        },
    )?)?;

    for idx in 1..=40_u128 {
        post_msg(
            &mut catalog,
            &run,
            main_ch.channel_id(),
            SwarmMessageAuthor::Member(coord.member_id()),
            0x3200 + idx,
            &format!("@worker-count msg {idx}"),
            1_200 + i64::try_from(idx)?,
        )?;
    }

    let first_snap = Uuid::from_u128(0x3301);
    let first_batch = catalog.claim_swarm_wakeup_batch(ClaimSwarmWakeupCommand {
        workspace_id,
        swarm_run_id: run.swarm_run_id(),
        member_id: worker.member_id(),
        snapshot_id: first_snap,
        claimed_at_ms: 1_300,
    })?;
    assert_eq!(
        first_batch.messages().len(),
        MAX_SNAPSHOT_COALESCED_MESSAGES
    );
    assert_eq!(first_batch.inflight_target_seq(), 32);

    let after_first = catalog.complete_swarm_member_turn(&CompleteSwarmTurnCommand {
        workspace_id,
        swarm_run_id: run.swarm_run_id(),
        member_id: worker.member_id(),
        expected_snapshot_id: first_snap,
        output: None,
        completed_at_ms: 1_310,
    })?;
    assert_eq!(after_first.member_state(), SwarmMemberState::WakeQueued);

    let second_batch = catalog.claim_swarm_wakeup_batch(ClaimSwarmWakeupCommand {
        workspace_id,
        swarm_run_id: run.swarm_run_id(),
        member_id: worker.member_id(),
        snapshot_id: Uuid::from_u128(0x3302),
        claimed_at_ms: 1_320,
    })?;
    assert_eq!(second_batch.messages().len(), 8);
    assert_eq!(second_batch.inflight_target_seq(), 40);
    Ok(())
}

#[test]
fn coalescing_enforces_64_kib_byte_bound() -> TestResult {
    let (_guard, mut catalog) = TempCatalog::open()?;
    let workspace_id = ws(0x3500);
    let run = catalog.create_swarm_run(&standalone_run_cmd(0x3501, workspace_id)?)?;
    let main_ch = catalog
        .get_swarm_channel_by_name(workspace_id, run.swarm_run_id(), "main")?
        .ok_or("main missing")?;
    let coord = catalog.add_swarm_member(&build_member_cmd(
        workspace_id,
        run.swarm_run_id(),
        &MemberSpec {
            member_seed: 0x3511,
            roster_name: "coordinator",
            role_kind: SwarmRoleKind::Coordinator,
            parent_member_id: None,
            workspace_mode: SwarmWorkspaceMode::Inherit,
            identity_seed: 10,
            added_at_ms: 1_100,
        },
    )?)?;
    let worker = catalog.add_swarm_member(&build_member_cmd(
        workspace_id,
        run.swarm_run_id(),
        &MemberSpec {
            member_seed: 0x3512,
            roster_name: "byte-worker",
            role_kind: SwarmRoleKind::Worker,
            parent_member_id: Some(coord.member_id()),
            workspace_mode: SwarmWorkspaceMode::Share,
            identity_seed: 11,
            added_at_ms: 1_101,
        },
    )?)?;

    let prefix = "@byte-worker ";
    let mut big_body = String::from(prefix);
    big_body.extend(std::iter::repeat_n('x', 16 * 1024 - prefix.len()));
    assert_eq!(big_body.len(), 16 * 1024);
    for idx in 1..=6_u128 {
        post_msg(
            &mut catalog,
            &run,
            main_ch.channel_id(),
            SwarmMessageAuthor::Member(coord.member_id()),
            0x3600 + idx,
            &big_body,
            1_200 + i64::try_from(idx)?,
        )?;
    }

    let first_snap = Uuid::from_u128(0x3701);
    let first_batch = catalog.claim_swarm_wakeup_batch(ClaimSwarmWakeupCommand {
        workspace_id,
        swarm_run_id: run.swarm_run_id(),
        member_id: worker.member_id(),
        snapshot_id: first_snap,
        claimed_at_ms: 1_300,
    })?;
    assert_eq!(first_batch.messages().len(), 4);
    assert_eq!(first_batch.total_body_bytes(), MAX_SNAPSHOT_COALESCED_BYTES);

    let after_first = catalog.complete_swarm_member_turn(&CompleteSwarmTurnCommand {
        workspace_id,
        swarm_run_id: run.swarm_run_id(),
        member_id: worker.member_id(),
        expected_snapshot_id: first_snap,
        output: None,
        completed_at_ms: 1_310,
    })?;
    assert_eq!(after_first.member_state(), SwarmMemberState::WakeQueued);

    let second_batch = catalog.claim_swarm_wakeup_batch(ClaimSwarmWakeupCommand {
        workspace_id,
        swarm_run_id: run.swarm_run_id(),
        member_id: worker.member_id(),
        snapshot_id: Uuid::from_u128(0x3702),
        claimed_at_ms: 1_320,
    })?;
    assert_eq!(second_batch.messages().len(), 2);
    assert_eq!(second_batch.total_body_bytes(), 32 * 1024);
    Ok(())
}

#[test]
fn channel_isolation_and_worker_all_policy_gate() -> TestResult {
    let (_guard, mut catalog) = TempCatalog::open()?;
    let workspace_id = ws(0x4001);
    let run = catalog.create_swarm_run(&standalone_run_cmd(0x4002, workspace_id)?)?;
    let main_ch = catalog
        .get_swarm_channel_by_name(workspace_id, run.swarm_run_id(), "main")?
        .ok_or("main missing")?;
    let backend_ch = catalog.create_swarm_channel(&CreateSwarmChannelCommand {
        channel_id: Uuid::from_u128(0x4010),
        workspace_id,
        swarm_run_id: run.swarm_run_id(),
        name: "backend".to_owned(),
        purpose: Some("Backend isolation".to_owned()),
        created_by_member_id: None,
        created_at_ms: 1_050,
    })?;
    let coord = catalog.add_swarm_member(&build_member_cmd(
        workspace_id,
        run.swarm_run_id(),
        &MemberSpec {
            member_seed: 0x4100,
            roster_name: "coordinator",
            role_kind: SwarmRoleKind::Coordinator,
            parent_member_id: None,
            workspace_mode: SwarmWorkspaceMode::Inherit,
            identity_seed: 1,
            added_at_ms: 1_100,
        },
    )?)?;
    let backend_w = catalog.add_swarm_member(&AddSwarmMemberCommand {
        initial_channels: vec!["backend".to_owned()],
        ..build_member_cmd(
            workspace_id,
            run.swarm_run_id(),
            &MemberSpec {
                member_seed: 0x4101,
                roster_name: "backend-w",
                role_kind: SwarmRoleKind::Worker,
                parent_member_id: Some(coord.member_id()),
                workspace_mode: SwarmWorkspaceMode::Share,
                identity_seed: 2,
                added_at_ms: 1_101,
            },
        )?
    })?;
    let frontend_w = catalog.add_swarm_member(&build_member_cmd(
        workspace_id,
        run.swarm_run_id(),
        &MemberSpec {
            member_seed: 0x4102,
            roster_name: "frontend-w",
            role_kind: SwarmRoleKind::Worker,
            parent_member_id: Some(coord.member_id()),
            workspace_mode: SwarmWorkspaceMode::Share,
            identity_seed: 3,
            added_at_ms: 1_102,
        },
    )?)?;

    let worker_all = post_msg(
        &mut catalog,
        &run,
        main_ch.channel_id(),
        SwarmMessageAuthor::Member(backend_w.member_id()),
        0x4201,
        "@all stop",
        1_200,
    );
    assert!(matches!(
        worker_all,
        Err(SwarmError::WorkerAllMentionForbidden)
    ));

    let ch_post = post_msg(
        &mut catalog,
        &run,
        backend_ch.channel_id(),
        SwarmMessageAuthor::Member(coord.member_id()),
        0x4202,
        "@#backend start backend",
        1_210,
    )?;
    assert_eq!(ch_post.woken_member_ids(), &[backend_w.member_id()]);
    let snap = catalog
        .get_swarm_run_snapshot(workspace_id, run.swarm_run_id())?
        .ok_or("snapshot missing")?;
    let frontend_state = snap
        .members()
        .iter()
        .find(|m| m.member_id() == frontend_w.member_id())
        .ok_or("frontend missing")?
        .member_state();
    assert_eq!(frontend_state, SwarmMemberState::Waiting);
    Ok(())
}

fn assert_depth_and_verifier_spawn_guards(
    catalog: &mut OperationsCatalog,
    run: &SwarmRunRecord,
    worker_a_id: Uuid,
) -> TestResult {
    let bad_v_cmd = build_member_cmd(
        run.workspace_id(),
        run.swarm_run_id(),
        &MemberSpec {
            member_seed: 0x4603,
            roster_name: "bad-verifier",
            role_kind: SwarmRoleKind::Verifier,
            parent_member_id: Some(worker_a_id),
            workspace_mode: SwarmWorkspaceMode::Inherit,
            identity_seed: 4,
            added_at_ms: 1_103,
        },
    )?;
    assert!(matches!(
        catalog.add_swarm_member(&bad_v_cmd),
        Err(SwarmError::WorkerVerifierSpawnForbidden)
    ));
    let mut parent = worker_a_id;
    for depth in 2..=4_u128 {
        let helper = add_member(
            catalog,
            run,
            &MemberSpec {
                member_seed: 0x4610 + depth,
                roster_name: &format!("helper-d{depth}"),
                role_kind: SwarmRoleKind::Helper,
                parent_member_id: Some(parent),
                workspace_mode: SwarmWorkspaceMode::Share,
                identity_seed: 10 + depth,
                added_at_ms: 1_110 + i64::try_from(depth)?,
            },
        )?;
        assert_eq!(helper.spawn_depth(), u8::try_from(depth)?);
        parent = helper.member_id();
    }
    let depth5_cmd = build_member_cmd(
        run.workspace_id(),
        run.swarm_run_id(),
        &MemberSpec {
            member_seed: 0x4615,
            roster_name: "helper-d5",
            role_kind: SwarmRoleKind::Helper,
            parent_member_id: Some(parent),
            workspace_mode: SwarmWorkspaceMode::Share,
            identity_seed: 15,
            added_at_ms: 1_120,
        },
    )?;
    assert!(matches!(
        catalog.add_swarm_member(&depth5_cmd),
        Err(SwarmError::SpawnDepthExceeded {
            depth: 5,
            max_depth: 4
        })
    ));
    Ok(())
}

fn assert_ping_pong_and_livelock_breaker(
    catalog: &mut OperationsCatalog,
    run: &SwarmRunRecord,
    channel_id: Uuid,
    coord_id: Uuid,
    first_worker_id: Uuid,
    second_worker_id: Uuid,
) -> TestResult {
    post_msg(
        catalog,
        run,
        channel_id,
        SwarmMessageAuthor::Member(second_worker_id),
        0x4701,
        "@worker-a ping",
        1_200,
    )?;
    let batch_wa = claim_batch(catalog, run, first_worker_id, 0x4710, 1_210)?;
    post_msg(
        catalog,
        run,
        channel_id,
        SwarmMessageAuthor::Member(first_worker_id),
        0x4702,
        "@worker-b pong",
        1_215,
    )?;
    let done_wa = catalog.complete_swarm_member_turn(&CompleteSwarmTurnCommand {
        workspace_id: run.workspace_id(),
        swarm_run_id: run.swarm_run_id(),
        member_id: first_worker_id,
        expected_snapshot_id: batch_wa.snapshot_id(),
        output: None,
        completed_at_ms: 1_220,
    })?;
    assert_eq!(done_wa.ping_pong_streak(), 1);

    let livelock = catalog.break_swarm_livelock(&BreakSwarmLivelockCommand {
        message_id: Uuid::from_u128(0x4720),
        workspace_id: run.workspace_id(),
        swarm_run_id: run.swarm_run_id(),
        channel_id,
        looping_member_ids: vec![first_worker_id, second_worker_id],
        coordinator_member_id: coord_id,
        body: "[livelock] worker-a <-> worker-b stopped".to_owned(),
        observed_at_ms: 1_230,
    })?;
    assert!(
        livelock
            .stopped_members()
            .iter()
            .all(|m| m.member_state() == SwarmMemberState::Stopped)
    );
    let queued = catalog.list_wake_queued_swarm_members(run.workspace_id(), run.swarm_run_id())?;
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].member_id(), coord_id);
    Ok(())
}

#[test]
fn spawn_depth_cascade_kill_ping_pong_and_livelock_breaker() -> TestResult {
    let (_guard, mut catalog) = TempCatalog::open()?;
    let workspace_id = ws(0x4501);
    let run = catalog.create_swarm_run(&standalone_run_cmd(0x4502, workspace_id)?)?;
    let main_ch = catalog
        .get_swarm_channel_by_name(workspace_id, run.swarm_run_id(), "main")?
        .ok_or("main missing")?;
    let coord = add_member(
        &mut catalog,
        &run,
        &MemberSpec {
            member_seed: 0x4600,
            roster_name: "coordinator",
            role_kind: SwarmRoleKind::Coordinator,
            parent_member_id: None,
            workspace_mode: SwarmWorkspaceMode::Inherit,
            identity_seed: 1,
            added_at_ms: 1_100,
        },
    )?;
    let worker_a = add_member(
        &mut catalog,
        &run,
        &MemberSpec {
            member_seed: 0x4601,
            roster_name: "worker-a",
            role_kind: SwarmRoleKind::Worker,
            parent_member_id: Some(coord.member_id()),
            workspace_mode: SwarmWorkspaceMode::Share,
            identity_seed: 2,
            added_at_ms: 1_101,
        },
    )?;
    let worker_b = add_member(
        &mut catalog,
        &run,
        &MemberSpec {
            member_seed: 0x4602,
            roster_name: "worker-b",
            role_kind: SwarmRoleKind::Worker,
            parent_member_id: Some(coord.member_id()),
            workspace_mode: SwarmWorkspaceMode::Share,
            identity_seed: 3,
            added_at_ms: 1_102,
        },
    )?;

    assert_depth_and_verifier_spawn_guards(&mut catalog, &run, worker_a.member_id())?;
    assert_ping_pong_and_livelock_breaker(
        &mut catalog,
        &run,
        main_ch.channel_id(),
        coord.member_id(),
        worker_a.member_id(),
        worker_b.member_id(),
    )?;

    let killed = catalog.kill_swarm_member(KillSwarmMemberCommand {
        workspace_id,
        swarm_run_id: run.swarm_run_id(),
        member_id: worker_a.member_id(),
        killed_at_ms: 1_250,
    })?;
    assert_eq!(killed.len(), 4);
    Ok(())
}

fn assert_sybil_verifiers_rejected(
    catalog: &mut OperationsCatalog,
    run: &SwarmRunRecord,
    coord_id: Uuid,
    worker_id: Uuid,
    helper_id: Uuid,
) -> TestResult {
    let share_v_cmd = build_member_cmd(
        run.workspace_id(),
        run.swarm_run_id(),
        &MemberSpec {
            member_seed: 0x5103,
            roster_name: "share-verifier",
            role_kind: SwarmRoleKind::Verifier,
            parent_member_id: Some(coord_id),
            workspace_mode: SwarmWorkspaceMode::Share,
            identity_seed: 4,
            added_at_ms: 1_103,
        },
    )?;
    assert!(matches!(
        catalog.add_swarm_member(&share_v_cmd),
        Err(SwarmError::VerifierMustInherit)
    ));
    assert!(matches!(
        catalog.record_swarm_verdict(&RecordSwarmVerdictCommand {
            request_id: Uuid::from_u128(0x5201),
            workspace_id: run.workspace_id(),
            swarm_run_id: run.swarm_run_id(),
            panel_id: None,
            verifier_member_id: helper_id,
            outcome: ReviewVerdictOutcome::Accepted,
            reviewed_diff_digest: digest(0x77),
            rationale: "LGTM".to_owned(),
            recorded_at_ms: 1_200,
        }),
        Err(SwarmError::SybilLineageViolation { .. })
    ));
    let sybil_helper_v = catalog.add_swarm_member(&AddSwarmMemberCommand {
        identity: SwarmMemberIdentity::new(
            identity(3)?.profile_id(),
            Uuid::from_u128(0xBEEF_0001),
            AuthenticatedPrincipal::OAuthGrant(OAuthGrantId::from_uuid(Uuid::from_u128(
                0xBEEF_0002,
            ))),
        )?,
        ..build_member_cmd(
            run.workspace_id(),
            run.swarm_run_id(),
            &MemberSpec {
                member_seed: 0x5104,
                roster_name: "sybil-profile-v",
                role_kind: SwarmRoleKind::Verifier,
                parent_member_id: Some(coord_id),
                workspace_mode: SwarmWorkspaceMode::Inherit,
                identity_seed: 40,
                added_at_ms: 1_104,
            },
        )?
    })?;
    assert!(matches!(
        catalog.create_swarm_verification_panel(&CreateSwarmVerificationPanelCommand {
            request_id: Uuid::from_u128(0x5202),
            workspace_id: run.workspace_id(),
            swarm_run_id: run.swarm_run_id(),
            author_member_ids: vec![worker_id],
            verifier_member_ids: vec![sybil_helper_v.member_id()],
            required_quorum: 1,
            created_at_ms: 1_210,
        }),
        Err(SwarmError::SybilLineageViolation { .. })
    ));
    Ok(())
}

fn assert_verdict_triggers_immutable(catalog: &OperationsCatalog, verdict_id: Uuid) {
    assert!(
        catalog
            .connection
            .execute(
                "UPDATE ops_swarm_verdicts SET outcome='REJECTED' WHERE verdict_id=?1",
                params![verdict_id.as_bytes()],
            )
            .is_err()
    );
    assert!(
        catalog
            .connection
            .execute(
                "DELETE FROM ops_swarm_verdicts WHERE verdict_id=?1",
                params![verdict_id.as_bytes()],
            )
            .is_err()
    );
}

#[test]
fn sybil_helper_lineage_self_review_rejected_and_verdicts_are_immutable() -> TestResult {
    let (_guard, mut catalog) = TempCatalog::open()?;
    let workspace_id = ws(0x5001);
    let run = catalog.create_swarm_run(&standalone_run_cmd(0x5002, workspace_id)?)?;
    let coord = add_member(
        &mut catalog,
        &run,
        &MemberSpec {
            member_seed: 0x5100,
            roster_name: "coordinator",
            role_kind: SwarmRoleKind::Coordinator,
            parent_member_id: None,
            workspace_mode: SwarmWorkspaceMode::Inherit,
            identity_seed: 1,
            added_at_ms: 1_100,
        },
    )?;
    let worker = add_member(
        &mut catalog,
        &run,
        &MemberSpec {
            member_seed: 0x5101,
            roster_name: "worker",
            role_kind: SwarmRoleKind::Worker,
            parent_member_id: Some(coord.member_id()),
            workspace_mode: SwarmWorkspaceMode::Share,
            identity_seed: 2,
            added_at_ms: 1_101,
        },
    )?;
    let helper = add_member(
        &mut catalog,
        &run,
        &MemberSpec {
            member_seed: 0x5102,
            roster_name: "helper",
            role_kind: SwarmRoleKind::Helper,
            parent_member_id: Some(worker.member_id()),
            workspace_mode: SwarmWorkspaceMode::Share,
            identity_seed: 3,
            added_at_ms: 1_102,
        },
    )?;

    assert_sybil_verifiers_rejected(
        &mut catalog,
        &run,
        coord.member_id(),
        worker.member_id(),
        helper.member_id(),
    )?;

    let ind_v = add_member(
        &mut catalog,
        &run,
        &MemberSpec {
            member_seed: 0x5106,
            roster_name: "independent-v",
            role_kind: SwarmRoleKind::Verifier,
            parent_member_id: Some(coord.member_id()),
            workspace_mode: SwarmWorkspaceMode::Inherit,
            identity_seed: 99,
            added_at_ms: 1_106,
        },
    )?;
    let panel = catalog.create_swarm_verification_panel(&CreateSwarmVerificationPanelCommand {
        request_id: Uuid::from_u128(0x5204),
        workspace_id,
        swarm_run_id: run.swarm_run_id(),
        author_member_ids: vec![worker.member_id()],
        verifier_member_ids: vec![ind_v.member_id()],
        required_quorum: 1,
        created_at_ms: 1_230,
    })?;
    let verdict = catalog.record_swarm_verdict(&RecordSwarmVerdictCommand {
        request_id: Uuid::from_u128(0x5205),
        workspace_id,
        swarm_run_id: run.swarm_run_id(),
        panel_id: Some(panel.panel_id()),
        verifier_member_id: ind_v.member_id(),
        outcome: ReviewVerdictOutcome::Accepted,
        reviewed_diff_digest: digest(0x77),
        rationale: "Independent verification passed".to_owned(),
        recorded_at_ms: 1_240,
    })?;
    assert_verdict_triggers_immutable(&catalog, verdict.verdict_id());
    Ok(())
}

#[test]
fn dag_node_hybrid_halts_at_candidate_ready_in_rust_and_sql() -> TestResult {
    let (_guard, mut catalog) = TempCatalog::open()?;
    let workspace_id = ws(0x6001);
    let hybrid_run = catalog.create_swarm_run(&CreateSwarmRunCommand {
        request_id: Uuid::from_u128(0x6002),
        workspace_id,
        title: "Hybrid DAG node micro-swarm".to_owned(),
        mode: SwarmRunMode::DagNodeHybrid {
            goal_id: GoalId::from_uuid(Uuid::from_u128(0x6010)),
            dag_revision: DagRevisionNumber::INITIAL,
            node_id: DagNodeId::from_uuid(Uuid::from_u128(0x6011)),
            declared_write_manifest_blake3: digest(0x44),
        },
        max_members: 8,
        max_spawn_depth: 2,
        max_total_turns: 20,
        required_verifier_quorum: 1,
        allow_worker_all_mention: false,
        baseline_commit_oid: commit('a')?,
        baseline_tree_blake3: digest(0x11),
        created_at_ms: 1_000,
    })?;
    let (_coord, _worker, verifier) = setup_triad(&mut catalog, &hybrid_run, 0x6100)?;
    let candidate = catalog.record_swarm_convergence_candidate(&RecordSwarmConvergenceCommand {
        workspace_id,
        swarm_run_id: hybrid_run.swarm_run_id(),
        converged_commit_oid: commit('d')?,
        converged_tree_blake3: digest(0x55),
        converged_diff_digest: digest(0x66),
        recorded_at_ms: 1_200,
    })?;
    assert_eq!(candidate.state(), SwarmRunState::CandidateReady);
    catalog.record_swarm_verdict(&RecordSwarmVerdictCommand {
        request_id: Uuid::from_u128(0x6201),
        workspace_id,
        swarm_run_id: hybrid_run.swarm_run_id(),
        panel_id: None,
        verifier_member_id: verifier.member_id(),
        outcome: ReviewVerdictOutcome::Accepted,
        reviewed_diff_digest: digest(0x66),
        rationale: "Hybrid candidate verified".to_owned(),
        recorded_at_ms: 1_250,
    })?;
    assert_eq!(
        catalog
            .verify_swarm_convergence_gate(workspace_id, hybrid_run.swarm_run_id())?
            .accepted_count(),
        1
    );
    assert!(matches!(
        catalog.settle_standalone_swarm_run(SettleStandaloneSwarmCommand {
            request_id: Uuid::from_u128(0x6301),
            workspace_id,
            swarm_run_id: hybrid_run.swarm_run_id(),
            settled_at_ms: 1_300,
        }),
        Err(SwarmError::DagNodeHybridCannotSettleDirectly)
    ));
    assert!(
        catalog
            .connection
            .execute(
                "UPDATE ops_swarm_runs SET state='SETTLED', settlement_request_id=?2 WHERE swarm_run_id=?1",
                params![
                    hybrid_run.swarm_run_id().as_bytes(),
                    Uuid::from_u128(0x6302).as_bytes()
                ],
            )
            .is_err()
    );
    Ok(())
}

#[test]
fn standalone_enforces_converged_diff_digest_gate_and_rejects_stale_verdict() -> TestResult {
    let (_guard, mut catalog) = TempCatalog::open()?;
    let workspace_id = ws(0x6500);
    let solo = catalog.create_swarm_run(&standalone_run_cmd(0x6501, workspace_id)?)?;
    let (_coord, _worker, verifier) = setup_triad(&mut catalog, &solo, 0x6510)?;
    catalog.record_swarm_convergence_candidate(&RecordSwarmConvergenceCommand {
        workspace_id,
        swarm_run_id: solo.swarm_run_id(),
        converged_commit_oid: commit('e')?,
        converged_tree_blake3: digest(0x77),
        converged_diff_digest: digest(0x88),
        recorded_at_ms: 2_100,
    })?;
    assert!(matches!(
        catalog.verify_swarm_convergence_gate(workspace_id, solo.swarm_run_id()),
        Err(SwarmError::ReviewNotAccepted)
    ));

    catalog.record_swarm_verdict(&RecordSwarmVerdictCommand {
        request_id: Uuid::from_u128(0x6601),
        workspace_id,
        swarm_run_id: solo.swarm_run_id(),
        panel_id: None,
        verifier_member_id: verifier.member_id(),
        outcome: ReviewVerdictOutcome::Accepted,
        reviewed_diff_digest: digest(0x99),
        rationale: "Accepted older candidate".to_owned(),
        recorded_at_ms: 2_150,
    })?;
    assert!(matches!(
        catalog.verify_swarm_convergence_gate(workspace_id, solo.swarm_run_id()),
        Err(SwarmError::VerdictDiffDigestMismatch)
    ));

    catalog.record_swarm_verdict(&RecordSwarmVerdictCommand {
        request_id: Uuid::from_u128(0x6602),
        workspace_id,
        swarm_run_id: solo.swarm_run_id(),
        panel_id: None,
        verifier_member_id: verifier.member_id(),
        outcome: ReviewVerdictOutcome::Accepted,
        reviewed_diff_digest: digest(0x88),
        rationale: "Verified exact converged_diff_digest".to_owned(),
        recorded_at_ms: 2_250,
    })?;
    let settled = catalog.settle_standalone_swarm_run(SettleStandaloneSwarmCommand {
        request_id: Uuid::from_u128(0x6702),
        workspace_id,
        swarm_run_id: solo.swarm_run_id(),
        settled_at_ms: 2_300,
    })?;
    assert_eq!(settled.state(), SwarmRunState::Settled);
    assert!(!settled.replayed());
    let replayed = catalog.settle_standalone_swarm_run(SettleStandaloneSwarmCommand {
        request_id: Uuid::from_u128(0x6702),
        workspace_id,
        swarm_run_id: solo.swarm_run_id(),
        settled_at_ms: 2_300,
    })?;
    assert!(replayed.replayed());
    Ok(())
}

#[test]
fn http_retries_with_newer_timestamps_replay_idempotently() -> TestResult {
    let (_guard, mut catalog) = TempCatalog::open()?;
    let workspace_id = ws(0x7001);
    let mut run_cmd = standalone_run_cmd(0x7002, workspace_id)?;
    let run = catalog.create_swarm_run(&run_cmd)?;
    run_cmd.created_at_ms += 42;
    let run_replay = catalog.create_swarm_run(&run_cmd)?;
    assert!(run_replay.replayed());
    assert_eq!(run_replay.swarm_run_id(), run.swarm_run_id());
    run_cmd.title = "Mutated retry".to_owned();
    assert!(catalog.create_swarm_run(&run_cmd).is_err());

    let main_ch = catalog
        .get_swarm_channel_by_name(workspace_id, run.swarm_run_id(), SWARM_MAIN_CHANNEL_NAME)?
        .ok_or("main missing")?;
    let (coord, worker, verifier) = setup_triad(&mut catalog, &run, 0x7100)?;
    let first_msg = post_msg(
        &mut catalog,
        &run,
        main_ch.channel_id(),
        SwarmMessageAuthor::Member(coord.member_id()),
        0x7201,
        "@worker-1 idempotent",
        1_200,
    )?;
    assert!(!first_msg.replayed());
    let retry_msg = post_msg(
        &mut catalog,
        &run,
        main_ch.channel_id(),
        SwarmMessageAuthor::Member(coord.member_id()),
        0x7201,
        "@worker-1 idempotent",
        1_299,
    )?;
    assert!(retry_msg.replayed());
    assert_eq!(
        retry_msg.message().sequence(),
        first_msg.message().sequence()
    );
    assert!(
        post_msg(
            &mut catalog,
            &run,
            main_ch.channel_id(),
            SwarmMessageAuthor::Member(coord.member_id()),
            0x7201,
            "@worker-1 altered",
            1_299,
        )
        .is_err()
    );

    let mut panel_cmd = CreateSwarmVerificationPanelCommand {
        request_id: Uuid::from_u128(0x7301),
        workspace_id,
        swarm_run_id: run.swarm_run_id(),
        author_member_ids: vec![worker.member_id()],
        verifier_member_ids: vec![verifier.member_id()],
        required_quorum: 1,
        created_at_ms: 1_300,
    };
    let panel = catalog.create_swarm_verification_panel(&panel_cmd)?;
    assert!(!panel.replayed());
    panel_cmd.created_at_ms += 75;
    let panel_replay = catalog.create_swarm_verification_panel(&panel_cmd)?;
    assert!(panel_replay.replayed());
    assert_eq!(panel_replay.panel_id(), panel.panel_id());

    let mut verdict_cmd = RecordSwarmVerdictCommand {
        request_id: Uuid::from_u128(0x7401),
        workspace_id,
        swarm_run_id: run.swarm_run_id(),
        panel_id: Some(panel.panel_id()),
        verifier_member_id: verifier.member_id(),
        outcome: ReviewVerdictOutcome::Accepted,
        reviewed_diff_digest: digest(0x55),
        rationale: "LGTM".to_owned(),
        recorded_at_ms: 1_400,
    };
    let verdict = catalog.record_swarm_verdict(&verdict_cmd)?;
    assert!(!verdict.replayed());
    verdict_cmd.recorded_at_ms += 88;
    let verdict_replay = catalog.record_swarm_verdict(&verdict_cmd)?;
    assert!(verdict_replay.replayed());
    assert_eq!(verdict_replay.verdict_id(), verdict.verdict_id());
    Ok(())
}
