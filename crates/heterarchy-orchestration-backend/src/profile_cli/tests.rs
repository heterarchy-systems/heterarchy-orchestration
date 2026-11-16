use std::error::Error;
use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;

use uuid::Uuid;

use super::{
    ProfileAction, ProfileCliCommand, ProfileCliError, ProfilePayload, execute, parse_with_env,
};
use crate::cli_support::{
    BACKEND_ADDR_ENV, BEARER_FILE_ENV, CliSupportError, MAX_BEARER_FILE_BYTES, WORKSPACE_ID_ENV,
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
            "heterarchy-profile-cli-{label}-{}-{}",
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
fn non_profile_and_help_flags_dispatch_cleanly() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        parse_with_env(&args(&["--version"]), no_env)?,
        ProfileCliCommand::NotProfileCommand
    );
    for help_args in [
        &["profile"][..],
        &["profile", "--help"],
        &["profile", "-h"],
        &["profile", "help"],
        &["profile", "create", "--help"],
        &["profile", "workspace-query", "-h"],
    ] {
        assert_eq!(
            parse_with_env(&args(help_args), no_env)?,
            ProfileCliCommand::Help
        );
    }
    Ok(())
}

#[test]
fn rejects_raw_bearer_flags_missing_bearer_and_non_loopback_addr() {
    for forbidden in [
        &["profile", "list", "--bearer", "secret-reveal"][..],
        &["profile", "list", "--bearer=secret-reveal"],
        &["profile", "list", "--token", "secret-reveal"],
        &["profile", "list", "--authorization", "Bearer secret-reveal"],
    ] {
        let err = parse_with_env(&args(forbidden), no_env);
        assert!(matches!(
            err,
            Err(ProfileCliError(CliSupportError::RawBearerFlagForbidden))
        ));
        if let Err(actual) = err {
            assert!(!actual.to_string().contains("secret-reveal"));
        }
    }

    assert!(matches!(
        parse_with_env(&args(&["profile", "list"]), no_env),
        Err(ProfileCliError(CliSupportError::MissingBearerFile))
    ));

    assert!(matches!(
        parse_with_env(
            &args(&[
                "profile",
                "list",
                "--bearer-file",
                "/tmp/tok",
                "--addr",
                "192.0.2.50:8798",
            ]),
            no_env,
        ),
        Err(ProfileCliError(CliSupportError::InvalidBackendAddress))
    ));
}

#[test]
fn parses_profile_create_list_get_and_update() -> Result<(), Box<dyn Error>> {
    let req_id = Uuid::from_u128(0x201).to_string();
    let prof_id = Uuid::from_u128(0x202).to_string();
    let digest_hex = "a5".repeat(32);

    let env_map = |key: &str| match key {
        BACKEND_ADDR_ENV => Some(OsString::from("127.0.0.1:9100")),
        BEARER_FILE_ENV => Some(OsString::from("/tmp/profile-secret-token")),
        _ => None,
    };

    let create_cmd = parse_with_env(
        &args(&[
            "profile",
            "create",
            "--request-id",
            &req_id,
            "--stable-name",
            "worker-carbon",
            "--display-name",
            "Worker Carbon",
            "--provider-family",
            "unbound",
            "--cli-engine",
            "agy",
            "--preferred-model",
            "gemini-4-carbon-high",
            "--preferred-reasoning",
            "high",
            "--capability",
            "rust",
            "--role-prompt-digest",
            &digest_hex,
        ]),
        env_map,
    )?;
    let ProfileCliCommand::Run(create_opts) = create_cmd else {
        return Err("expected Run".into());
    };
    assert_eq!(create_opts.connection().addr(), "127.0.0.1:9100".parse()?);
    assert!(!format!("{create_opts:?}").contains("/tmp/profile-secret-token"));
    let ProfileAction::Create(ProfilePayload::Inline(create_req)) = create_opts.action() else {
        return Err("expected inline create".into());
    };
    assert_eq!(create_req.stable_name, "worker-carbon");
    assert_eq!(
        create_req
            .execution_preference
            .as_ref()
            .map(|p| p.cli_engine.as_str()),
        Some("agy")
    );

    assert!(matches!(
        parse_with_env(&args(&["profile", "list", "--limit", "16"]), env_map)?,
        ProfileCliCommand::Run(_)
    ));
    assert!(matches!(
        parse_with_env(
            &args(&["profile", "get", "--profile-id", &prof_id]),
            env_map
        )?,
        ProfileCliCommand::Run(_)
    ));
    assert!(matches!(
        parse_with_env(
            &args(&[
                "profile",
                "update",
                "--profile-id",
                &prof_id,
                "--request-id",
                &req_id,
                "--expected-revision",
                "1",
                "--display-name",
                "Updated Worker",
                "--cli-engine",
                "codex",
                "--preferred-model",
                "gpt-6-astra",
            ]),
            env_map,
        )?,
        ProfileCliCommand::Run(_)
    ));
    Ok(())
}

#[test]
fn parses_profile_lifecycle_mutation_receipt_and_workspace_query() -> Result<(), Box<dyn Error>> {
    let req_id = Uuid::from_u128(0x201).to_string();
    let prof_id = Uuid::from_u128(0x202).to_string();
    let ws_id = Uuid::from_u128(0x203).to_string();
    let loc_id = Uuid::from_u128(0x204).to_string();

    let env_map = |key: &str| match key {
        BACKEND_ADDR_ENV => Some(OsString::from("127.0.0.1:9100")),
        BEARER_FILE_ENV => Some(OsString::from("/tmp/profile-secret-token")),
        WORKSPACE_ID_ENV => Some(OsString::from(&ws_id)),
        _ => None,
    };

    for sub in ["disable", "retire"] {
        assert!(matches!(
            parse_with_env(
                &args(&[
                    "profile",
                    sub,
                    "--profile-id",
                    &prof_id,
                    "--request-id",
                    &req_id,
                    "--expected-revision",
                    "2",
                ]),
                env_map,
            )?,
            ProfileCliCommand::Run(_)
        ));
    }
    assert!(matches!(
        parse_with_env(
            &args(&["profile", "mutation-get", "--request-id", &req_id]),
            env_map
        )?,
        ProfileCliCommand::Run(_)
    ));
    assert!(matches!(
        parse_with_env(
            &args(&[
                "profile",
                "workspace-query",
                "--location-id",
                &loc_id,
                "--location-revision",
                "1",
                "--workspace-revision",
                "1",
                "--locator",
                "hf1:profile-ws",
            ]),
            env_map,
        )?,
        ProfileCliCommand::Run(_)
    ));

    // Mixing --request and inline flags fails closed.
    assert!(matches!(
        parse_with_env(
            &args(&[
                "profile",
                "update",
                "--profile-id",
                &prof_id,
                "--request",
                "/tmp/update.json",
                "--display-name",
                "ambiguous",
            ]),
            env_map,
        ),
        Err(ProfileCliError(CliSupportError::InvalidArguments))
    ));
    Ok(())
}

#[tokio::test]
async fn execute_enforces_bearer_file_hygiene() -> Result<(), Box<dyn Error>> {
    let oversized = TempPath::new("oversized");
    fs::write(&oversized.0, vec![b'z'; MAX_BEARER_FILE_BYTES + 1])?;
    let ProfileCliCommand::Run(opts) = parse_with_env(
        &args(&[
            "profile",
            "list",
            "--bearer-file",
            oversized.0.to_str().ok_or("utf8")?,
        ]),
        no_env,
    )?
    else {
        return Err("expected Run".into());
    };
    assert!(matches!(
        execute(&opts).await,
        Err(ProfileCliError(CliSupportError::BearerFileOversized))
    ));
    Ok(())
}
