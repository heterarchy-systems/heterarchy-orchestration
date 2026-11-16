use std::error::Error;
use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;

use uuid::Uuid;

use super::{
    AutonomyAction, AutonomyCliCommand, AutonomyCliError, AutonomyPayload, execute, parse_with_env,
};
use crate::cli_support::{
    BACKEND_ADDR_ENV, BEARER_FILE_ENV, CliSupportError, MAX_BEARER_FILE_BYTES, WORKSPACE_ID_ENV,
};
use crate::wire::autonomy_loop::WorkspaceScopeMutationRequest;

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
            "heterarchy-autonomy-cli-{label}-{}-{}",
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
fn non_autonomy_and_help_flags_dispatch_cleanly() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        parse_with_env(&args(&["--version"]), no_env)?,
        AutonomyCliCommand::NotAutonomyCommand
    );
    for help_args in [
        &["autonomy"][..],
        &["autonomy", "--help"],
        &["autonomy", "-h"],
        &["autonomy", "scope-mutate", "--help"],
        &["autonomy", "dag-settlement-authority", "--help"],
        &["autonomy", "dag-settlement-governance", "-h"],
    ] {
        assert_eq!(
            parse_with_env(&args(help_args), no_env)?,
            AutonomyCliCommand::Help
        );
    }
    Ok(())
}

#[test]
fn raw_bearer_flags_and_missing_or_non_loopback_connection_fail_closed() {
    let ws = Uuid::from_u128(0x11).to_string();
    for forbidden in [
        &["autonomy", "scope-get", "--bearer", "secret-reveal"][..],
        &["autonomy", "scope-get", "--bearer=secret-reveal"],
        &["autonomy", "scope-get", "--token", "secret-reveal"],
        &["autonomy", "scope-get", "--authorization", "Bearer secret"],
    ] {
        let err = parse_with_env(&args(forbidden), no_env);
        assert!(
            matches!(
                err,
                Err(AutonomyCliError(CliSupportError::RawBearerFlagForbidden))
            ),
            "expected RawBearerFlagForbidden, got {err:?}"
        );
        if let Err(actual) = err {
            assert!(!actual.to_string().contains("secret-reveal"));
        }
    }

    assert!(matches!(
        parse_with_env(
            &args(&["autonomy", "scope-get", "--workspace-id", &ws]),
            no_env
        ),
        Err(AutonomyCliError(CliSupportError::MissingBearerFile))
    ));

    assert!(matches!(
        parse_with_env(
            &args(&[
                "autonomy",
                "scope-get",
                "--bearer-file",
                "/tmp/tok",
                "--addr",
                "192.0.2.10:8798",
            ]),
            no_env,
        ),
        Err(AutonomyCliError(CliSupportError::InvalidBackendAddress))
    ));
}

#[test]
fn parses_direct_flags_and_request_file_for_scope_subcommands() -> Result<(), Box<dyn Error>> {
    let ws = Uuid::from_u128(0x101).to_string();
    let loc = Uuid::from_u128(0x102).to_string();
    let req_id = Uuid::from_u128(0x103).to_string();

    let env_map = |key: &str| match key {
        BACKEND_ADDR_ENV => Some(OsString::from("127.0.0.1:9001")),
        BEARER_FILE_ENV => Some(OsString::from("/tmp/autonomy-bearer-token")),
        WORKSPACE_ID_ENV => Some(OsString::from(&ws)),
        _ => None,
    };

    let parsed = parse_with_env(
        &args(&[
            "autonomy",
            "scope-mutate",
            "--register",
            "--request-id",
            &req_id,
            "--expected-revision",
            "0",
            "--workspace-id",
            &ws,
            "--location-id",
            &loc,
            "--location-revision",
            "1",
            "--workspace-revision",
            "2",
            "--locator",
            "hf1:autonomy-test",
        ]),
        env_map,
    )?;
    let AutonomyCliCommand::Run(opts) = parsed else {
        return Err("expected AutonomyCliCommand::Run".into());
    };
    assert_eq!(opts.connection().addr(), "127.0.0.1:9001".parse()?);
    assert!(!format!("{opts:?}").contains("/tmp/autonomy-bearer-token"));
    assert!(matches!(
        opts.action(),
        AutonomyAction::ScopeMutate(AutonomyPayload::Inline(
            WorkspaceScopeMutationRequest::Register { .. }
        ))
    ));

    // Mixing --request and inline flags fails closed.
    assert!(matches!(
        parse_with_env(
            &args(&[
                "autonomy",
                "scope-mutate",
                "--bearer-file",
                "/tmp/tok",
                "--request",
                "/tmp/payload.json",
                "--unregister",
            ]),
            no_env,
        ),
        Err(AutonomyCliError(CliSupportError::InvalidArguments))
    ));
    Ok(())
}

#[test]
fn parses_dag_settlement_authority_and_governance_subcommands() -> Result<(), Box<dyn Error>> {
    let ws = Uuid::from_u128(0x101).to_string();
    let goal_id = Uuid::from_u128(0x104).to_string();
    let act_id = Uuid::from_u128(0x105).to_string();
    let agent_id = Uuid::from_u128(0x106).to_string();
    let sess_id = Uuid::from_u128(0x107).to_string();

    let env_map = |key: &str| match key {
        BACKEND_ADDR_ENV => Some(OsString::from("127.0.0.1:9001")),
        BEARER_FILE_ENV => Some(OsString::from("/tmp/autonomy-bearer-token")),
        WORKSPACE_ID_ENV => Some(OsString::from(&ws)),
        _ => None,
    };

    let auth_cmd = parse_with_env(
        &args(&[
            "autonomy",
            "dag-settlement-authority",
            "--expected-leadership-revision",
            "0",
        ]),
        env_map,
    )?;
    assert!(matches!(auth_cmd, AutonomyCliCommand::Run(_)));

    let auth_part_cmd = parse_with_env(
        &args(&[
            "autonomy",
            "dag-settlement-authority",
            "--expected-leadership-revision",
            "3",
            "--activity-id",
            &act_id,
            "--agent-id",
            &agent_id,
            "--session-id",
            &sess_id,
        ]),
        env_map,
    )?;
    assert!(matches!(auth_part_cmd, AutonomyCliCommand::Run(_)));

    // Partial participant coordinates fail closed.
    assert!(matches!(
        parse_with_env(
            &args(&[
                "autonomy",
                "dag-settlement-authority",
                "--expected-leadership-revision",
                "1",
                "--activity-id",
                &act_id,
            ]),
            env_map,
        ),
        Err(AutonomyCliError(CliSupportError::InvalidArguments))
    ));

    let gov_cmd = parse_with_env(
        &args(&[
            "autonomy",
            "dag-settlement-governance",
            "--goal-id",
            &goal_id,
            "--dag-revision",
            "2",
        ]),
        env_map,
    )?;
    assert!(matches!(gov_cmd, AutonomyCliCommand::Run(_)));
    Ok(())
}

#[tokio::test]
async fn execute_rejects_missing_empty_oversized_and_symlink_bearer_files()
-> Result<(), Box<dyn Error>> {
    let missing = TempPath::new("missing-bearer");
    let cmd = parse_with_env(
        &args(&[
            "autonomy",
            "scope-get",
            "--bearer-file",
            missing.0.to_str().ok_or("utf8")?,
        ]),
        no_env,
    )?;
    let AutonomyCliCommand::Run(missing_opts) = cmd else {
        return Err("expected Run".into());
    };
    assert!(matches!(
        execute(&missing_opts).await,
        Err(AutonomyCliError(CliSupportError::InsecureBearerFile))
    ));

    let empty = TempPath::new("empty-bearer");
    fs::write(&empty.0, "\n")?;
    let AutonomyCliCommand::Run(empty_opts) = parse_with_env(
        &args(&[
            "autonomy",
            "scope-get",
            "--bearer-file",
            empty.0.to_str().ok_or("utf8")?,
        ]),
        no_env,
    )?
    else {
        return Err("expected Run".into());
    };
    assert!(matches!(
        execute(&empty_opts).await,
        Err(AutonomyCliError(CliSupportError::InvalidBearerSecret))
    ));

    let oversized = TempPath::new("oversized-bearer");
    fs::write(&oversized.0, vec![b'a'; MAX_BEARER_FILE_BYTES + 1])?;
    let AutonomyCliCommand::Run(over_opts) = parse_with_env(
        &args(&[
            "autonomy",
            "scope-get",
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
        Err(AutonomyCliError(CliSupportError::BearerFileOversized))
    ));

    #[cfg(unix)]
    {
        let link = TempPath::new("symlink-bearer");
        std::os::unix::fs::symlink(&empty.0, &link.0)?;
        let AutonomyCliCommand::Run(link_opts) = parse_with_env(
            &args(&[
                "autonomy",
                "scope-get",
                "--bearer-file",
                link.0.to_str().ok_or("utf8")?,
            ]),
            no_env,
        )?
        else {
            return Err("expected Run".into());
        };
        assert!(matches!(
            execute(&link_opts).await,
            Err(AutonomyCliError(CliSupportError::InsecureBearerFile))
        ));
    }
    Ok(())
}
