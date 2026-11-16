use std::error::Error;
use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;

use uuid::Uuid;

use super::{SwarmAction, SwarmCliCommand, SwarmCliError, SwarmPayload, execute, parse_with_env};
use crate::cli_support::{
    BACKEND_ADDR_ENV, BEARER_FILE_ENV, CliSupportError, MAX_BEARER_FILE_BYTES, SWARM_MEMBER_ID_ENV,
    SWARM_RUN_ID_ENV, WORKSPACE_ID_ENV,
};

fn args(items: &[&str]) -> Vec<OsString> {
    items.iter().map(OsString::from).collect()
}

fn no_env(_key: &str) -> Option<OsString> {
    None
}

struct TempPath(PathBuf);

impl TempPath {
    fn new(label: &str) -> Self {
        Self(std::env::temp_dir().join(format!(
            "heterarchy-swarm-cli-{label}-{}-{}",
            std::process::id(),
            Uuid::now_v7()
        )))
    }
}

impl Drop for TempPath {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[test]
fn non_swarm_and_help_flags_dispatch_cleanly() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        parse_with_env(&args(&["token", "--help"]), no_env)?,
        SwarmCliCommand::NotSwarmCommand
    );
    for help_args in [
        &["swarm"][..],
        &["swarm", "--help"],
        &["swarm", "-h"],
        &["swarm", "help"],
        &["swarm", "create", "--help"],
        &["swarm", "dispatch", "--help"],
        &["swarm", "stall-check", "-h"],
    ] {
        assert_eq!(
            parse_with_env(&args(help_args), no_env)?,
            SwarmCliCommand::Help
        );
    }
    Ok(())
}

#[test]
fn rejects_raw_bearer_argv_missing_bearer_file_and_non_loopback_addr() {
    let ws = Uuid::from_u128(0x21).to_string();
    for forbidden in [
        &["swarm", "list", "--workspace-id", &ws, "--bearer", "leaked"][..],
        &["swarm", "--bearer=leaked", "list", "--workspace-id", &ws],
        &["swarm", "list", "--workspace-id", &ws, "--token", "leaked"],
    ] {
        let result = parse_with_env(&args(forbidden), no_env);
        assert!(matches!(
            result,
            Err(SwarmCliError(CliSupportError::RawBearerFlagForbidden))
        ));
        if let Err(err) = result {
            assert!(!err.to_string().contains("leaked"));
        }
    }

    assert!(matches!(
        parse_with_env(&args(&["swarm", "list", "--workspace-id", &ws]), no_env),
        Err(SwarmCliError(CliSupportError::MissingBearerFile))
    ));

    assert!(matches!(
        parse_with_env(
            &args(&[
                "swarm",
                "list",
                "--workspace-id",
                &ws,
                "--bearer-file",
                "/tmp/tok",
                "--addr",
                "0.0.0.0:8798",
            ]),
            no_env,
        ),
        Err(SwarmCliError(CliSupportError::InvalidBackendAddress))
    ));
}

#[test]
fn parses_inline_and_request_file_swarm_subcommands() -> Result<(), Box<dyn Error>> {
    let ws = Uuid::from_u128(0x301).to_string();
    let run_id = Uuid::from_u128(0x302).to_string();
    let req_id = Uuid::from_u128(0x303).to_string();
    let oid = "1".repeat(40);
    let tree_hex = "a1".repeat(32);

    let create_cmd = parse_with_env(
        &args(&[
            "swarm",
            "create",
            "--workspace-id",
            &ws,
            "--request-id",
            &req_id,
            "--title",
            "CLI Swarm Run",
            "--max-members",
            "8",
            "--max-spawn-depth",
            "3",
            "--max-total-turns",
            "64",
            "--required-verifier-quorum",
            "1",
            "--baseline-commit-oid",
            &oid,
            "--baseline-tree-blake3",
            &tree_hex,
        ]),
        |key| match key {
            BEARER_FILE_ENV => Some(OsString::from("/tmp/swarm-secret-file")),
            _ => None,
        },
    )?;
    let SwarmCliCommand::Run(opts) = create_cmd else {
        return Err("expected SwarmCliCommand::Run".into());
    };
    assert!(!format!("{opts:?}").contains("/tmp/swarm-secret-file"));
    assert!(matches!(
        opts.action(),
        SwarmAction::Create {
            payload: SwarmPayload::Inline(_),
            ..
        }
    ));

    let post_file_cmd = parse_with_env(
        &args(&[
            "swarm",
            "post",
            "--bearer-file",
            "/tmp/swarm-secret-file",
            "--workspace-id",
            &ws,
            "--swarm-run-id",
            &run_id,
            "--request",
            "/tmp/post.json",
        ]),
        no_env,
    )?;
    let SwarmCliCommand::Run(post_opts) = post_file_cmd else {
        return Err("expected SwarmCliCommand::Run".into());
    };
    assert!(matches!(
        post_opts.action(),
        SwarmAction::Post {
            payload: SwarmPayload::RequestFile(_),
            ..
        }
    ));

    // Mixing --request with inline --body fails closed.
    assert!(matches!(
        parse_with_env(
            &args(&[
                "swarm",
                "post",
                "--bearer-file",
                "/tmp/swarm-secret-file",
                "--workspace-id",
                &ws,
                "--swarm-run-id",
                &run_id,
                "--request",
                "/tmp/post.json",
                "--body",
                "ambiguous",
            ]),
            no_env,
        ),
        Err(SwarmCliError(CliSupportError::InvalidArguments))
    ));
    Ok(())
}

#[test]
fn spawned_worker_env_fallbacks_parse_post_read_and_status() -> Result<(), Box<dyn Error>> {
    let ws = Uuid::from_u128(0x601);
    let run_id = Uuid::from_u128(0x602);
    let member_id = Uuid::from_u128(0x603);

    let worker_env = |key: &str| match key {
        WORKSPACE_ID_ENV => Some(OsString::from(ws.to_string())),
        SWARM_RUN_ID_ENV => Some(OsString::from(run_id.to_string())),
        SWARM_MEMBER_ID_ENV => Some(OsString::from(member_id.to_string())),
        BACKEND_ADDR_ENV => Some(OsString::from("127.0.0.1:9200")),
        BEARER_FILE_ENV => Some(OsString::from("/tmp/worker-bearer")),
        _ => None,
    };

    let SwarmCliCommand::Run(post_opts) = parse_with_env(
        &args(&["swarm", "post", "--channel", "main", "--body", "report"]),
        worker_env,
    )?
    else {
        return Err("expected Run".into());
    };
    let SwarmAction::Post {
        workspace_id,
        swarm_run_id,
        channel_name,
        payload: SwarmPayload::Inline(req),
    } = post_opts.action()
    else {
        return Err("expected Inline Post".into());
    };
    assert_eq!((*workspace_id, *swarm_run_id), (ws, run_id));
    assert_eq!(channel_name.as_deref(), Some("main"));
    assert_eq!(
        req.author_member_id.as_deref(),
        Some(member_id.to_string().as_str())
    );
    assert_eq!(req.body, "report");

    let SwarmCliCommand::Run(read_opts) = parse_with_env(
        &args(&["swarm", "read", "--channel", "main", "--after-seq", "0"]),
        worker_env,
    )?
    else {
        return Err("expected Run".into());
    };
    let SwarmAction::Read {
        workspace_id,
        swarm_run_id,
        channel_name,
        payload: SwarmPayload::Inline(query),
    } = read_opts.action()
    else {
        return Err("expected Inline Read".into());
    };
    assert_eq!((*workspace_id, *swarm_run_id), (ws, run_id));
    assert_eq!(channel_name.as_deref(), Some("main"));
    assert_eq!(query.since_sequence, Some(0));

    let SwarmCliCommand::Run(status_opts) =
        parse_with_env(&args(&["swarm", "status"]), worker_env)?
    else {
        return Err("expected Run".into());
    };
    assert_eq!(
        status_opts.action(),
        &SwarmAction::Status {
            workspace_id: ws,
            swarm_run_id: Some(run_id),
            request_file: None,
        }
    );
    Ok(())
}

#[test]
fn spawned_worker_env_fallbacks_parse_verdict_and_dispatch() -> Result<(), Box<dyn Error>> {
    let ws = Uuid::from_u128(0x601);
    let run_id = Uuid::from_u128(0x602);
    let member_id = Uuid::from_u128(0x603);
    let panel_id = Uuid::from_u128(0x604);
    let snap_id = Uuid::from_u128(0x605);
    let act_id = Uuid::from_u128(0x606);
    let lease_id = Uuid::from_u128(0x607);
    let diff_hex = "d1".repeat(32);

    let worker_env = |key: &str| match key {
        WORKSPACE_ID_ENV => Some(OsString::from(ws.to_string())),
        SWARM_RUN_ID_ENV => Some(OsString::from(run_id.to_string())),
        SWARM_MEMBER_ID_ENV => Some(OsString::from(member_id.to_string())),
        BACKEND_ADDR_ENV => Some(OsString::from("127.0.0.1:9200")),
        BEARER_FILE_ENV => Some(OsString::from("/tmp/worker-bearer")),
        _ => None,
    };

    let SwarmCliCommand::Run(verdict_opts) = parse_with_env(
        &args(&[
            "swarm",
            "verdict",
            "--panel-id",
            &panel_id.to_string(),
            "--verdict",
            "accepted",
            "--diff-digest",
            &diff_hex,
            "--rationale",
            "all clean",
        ]),
        worker_env,
    )?
    else {
        return Err("expected Run".into());
    };
    let SwarmAction::Verdict {
        payload: SwarmPayload::Inline(verdict_req),
        ..
    } = verdict_opts.action()
    else {
        return Err("expected Verdict".into());
    };
    assert_eq!(verdict_req.verifier_member_id, member_id.to_string());
    assert_eq!(verdict_req.outcome, "accepted");

    let SwarmCliCommand::Run(dispatch_opts) = parse_with_env(
        &args(&[
            "swarm",
            "dispatch",
            "--snapshot-id",
            &snap_id.to_string(),
            "--activity-id",
            &act_id.to_string(),
            "--default-worktree-path",
            "/var/tmp/forge-swarm/worker-wt",
            "--lease-id",
            &lease_id.to_string(),
            "--lease-revision",
            "1",
            "--lease-expires-at-ms",
            "60000",
            "--allow-workspace-write",
            "--startup-timeout-ms",
            "1000",
            "--execution-timeout-ms",
            "5000",
            "--termination-grace-ms",
            "500",
        ]),
        worker_env,
    )?
    else {
        return Err("expected Run".into());
    };
    assert!(matches!(
        dispatch_opts.action(),
        SwarmAction::Dispatch {
            workspace_id: w,
            swarm_run_id: r,
            member_id: m,
            payload: SwarmPayload::Inline(_),
        } if (*w, *r, *m) == (ws, run_id, member_id)
    ));
    Ok(())
}

#[tokio::test]
async fn execute_rejects_missing_whitespace_and_oversized_bearer_files()
-> Result<(), Box<dyn Error>> {
    let ws = Uuid::from_u128(0x401).to_string();
    let missing = TempPath::new("missing");
    let SwarmCliCommand::Run(missing_opts) = parse_with_env(
        &args(&[
            "swarm",
            "list",
            "--workspace-id",
            &ws,
            "--bearer-file",
            missing.0.to_str().ok_or("utf8")?,
        ]),
        no_env,
    )?
    else {
        return Err("expected Run".into());
    };
    assert!(matches!(
        execute(&missing_opts).await,
        Err(SwarmCliError(CliSupportError::InsecureBearerFile))
    ));

    let interior_newline = TempPath::new("interior-newline");
    fs::write(&interior_newline.0, "line1\nline2\n")?;
    let SwarmCliCommand::Run(nl_opts) = parse_with_env(
        &args(&[
            "swarm",
            "list",
            "--workspace-id",
            &ws,
            "--bearer-file",
            interior_newline.0.to_str().ok_or("utf8")?,
        ]),
        no_env,
    )?
    else {
        return Err("expected Run".into());
    };
    let nl_err = execute(&nl_opts).await;
    assert!(matches!(
        nl_err,
        Err(SwarmCliError(CliSupportError::InvalidBearerSecret))
    ));
    if let Err(err) = nl_err {
        assert!(!err.to_string().contains("line1"));
    }

    let oversized = TempPath::new("oversized");
    fs::write(&oversized.0, vec![b'x'; MAX_BEARER_FILE_BYTES + 8])?;
    let SwarmCliCommand::Run(over_opts) = parse_with_env(
        &args(&[
            "swarm",
            "list",
            "--workspace-id",
            &ws,
            "--bearer-file",
            oversized.0.to_str().ok_or("utf8")?,
        ]),
        no_env,
    )?
    else {
        return Err("expected Run".into());
    };
    assert!(matches!(
        execute(&over_opts).await,
        Err(SwarmCliError(CliSupportError::BearerFileOversized))
    ));
    Ok(())
}

#[test]
fn forge_converge_and_settle_subcommands_parse_cleanly() -> Result<(), Box<dyn Error>> {
    let ws = Uuid::from_u128(0x501);
    let run_id = Uuid::from_u128(0x502);
    let member_id = Uuid::from_u128(0x503);
    let session_id = Uuid::from_u128(0x504);
    let coord_id = Uuid::from_u128(0x505);
    let migration_id = Uuid::from_u128(0x506);

    let SwarmCliCommand::Run(conv) = parse_with_env(
        &args(&[
            "swarm",
            "forge-converge",
            "--bearer-file",
            "/var/tmp/tok",
            "--workspace-id",
            &ws.to_string(),
            "--swarm-run-id",
            &run_id.to_string(),
            "--session-id",
            &session_id.to_string(),
            "--candidate-member-id",
            &member_id.to_string(),
            "--stage-candidate-only",
        ]),
        no_env,
    )?
    else {
        return Err("expected forge-converge Run".into());
    };
    assert!(matches!(
        conv.action(),
        SwarmAction::ForgeConverge {
            workspace_id,
            swarm_run_id,
            payload: SwarmPayload::Inline(req),
        } if (*workspace_id, *swarm_run_id) == (ws, run_id)
            && req.session_id == session_id.to_string()
            && req.candidate_member_ids == vec![member_id.to_string()]
            && req.stage_candidate_only
    ));

    let SwarmCliCommand::Run(settle) = parse_with_env(
        &args(&[
            "swarm",
            "forge-settle",
            "--bearer-file",
            "/var/tmp/tok",
            "--workspace-id",
            &ws.to_string(),
            "--swarm-run-id",
            &run_id.to_string(),
            "--session-id",
            &session_id.to_string(),
            "--coordinator-agent-id",
            &coord_id.to_string(),
            "--outbox-migration-id",
            &migration_id.to_string(),
            "--outbox-after-sequence",
            "2",
            "--purge-ephemeral",
        ]),
        no_env,
    )?
    else {
        return Err("expected forge-settle Run".into());
    };
    assert!(matches!(
        settle.action(),
        SwarmAction::ForgeSettle {
            workspace_id,
            swarm_run_id,
            payload: SwarmPayload::Inline(req),
        } if (*workspace_id, *swarm_run_id) == (ws, run_id)
            && req.coordinator_agent_id == coord_id.to_string()
            && req.outbox_migration_id.as_deref() == Some(migration_id.to_string().as_str())
            && req.outbox_after_sequence == Some(2)
            && req.purge_ephemeral
    ));
    Ok(())
}

#[test]
fn outbox_sync_and_dispatch_forge_session_id_parse_cleanly() -> Result<(), Box<dyn Error>> {
    let ws = Uuid::from_u128(0x501);
    let run_id = Uuid::from_u128(0x502);
    let member_id = Uuid::from_u128(0x503);
    let session_id = Uuid::from_u128(0x504);
    let migration_id = Uuid::from_u128(0x506);

    let SwarmCliCommand::Run(sync) = parse_with_env(
        &args(&[
            "swarm",
            "outbox-sync",
            "--bearer-file",
            "/var/tmp/tok",
            "--workspace-id",
            &ws.to_string(),
            "--migration-id",
            &migration_id.to_string(),
            "--after-sequence",
            "0",
            "--limit",
            "32",
        ]),
        no_env,
    )?
    else {
        return Err("expected outbox-sync Run".into());
    };
    assert!(matches!(
        sync.action(),
        SwarmAction::OutboxSync {
            workspace_id,
            payload: SwarmPayload::Inline(req),
        } if *workspace_id == ws
            && req.migration_id == migration_id.to_string()
            && req.limit == Some(32)
    ));

    let SwarmCliCommand::Run(disp) = parse_with_env(
        &args(&[
            "swarm",
            "dispatch",
            "--bearer-file",
            "/var/tmp/tok",
            "--workspace-id",
            &ws.to_string(),
            "--swarm-run-id",
            &run_id.to_string(),
            "--member-id",
            &member_id.to_string(),
            "--snapshot-id",
            &Uuid::from_u128(0x507).to_string(),
            "--activity-id",
            &Uuid::from_u128(0x508).to_string(),
            "--lease-id",
            &Uuid::from_u128(0x509).to_string(),
            "--lease-revision",
            "1",
            "--lease-expires-at-ms",
            "10000",
            "--startup-timeout-ms",
            "1000",
            "--execution-timeout-ms",
            "5000",
            "--termination-grace-ms",
            "1000",
            "--forge-session-id",
            &session_id.to_string(),
        ]),
        no_env,
    )?
    else {
        return Err("expected dispatch Run".into());
    };
    assert!(matches!(
        disp.action(),
        SwarmAction::Dispatch {
            payload: SwarmPayload::Inline(req),
            ..
        } if req.forge_session_id.as_deref() == Some(session_id.to_string().as_str())
            && req.default_worktree_path.is_empty()
    ));
    Ok(())
}
