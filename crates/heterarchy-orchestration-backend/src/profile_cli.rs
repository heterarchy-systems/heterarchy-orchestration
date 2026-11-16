//! Operator CLI subcommands for global `AgentProfile` creation, inspection,
//! configuration updates, lifecycle transitions, mutation receipts, and
//! source-fenced Workspace projections.
//!
//! Follows the `parse` + `execute` contract of [`crate::token_cli`]: raw bearer
//! tokens are never accepted on `argv`, `--bearer-file` is read with bounded
//! secret hygiene, and deterministic pretty-printed JSON is returned for `stdout`.

mod parse_subcommands;

use std::env;
use std::ffi::OsString;
use std::path::PathBuf;

use serde::Deserialize;
use thiserror::Error;
use uuid::Uuid;

use crate::cli_support::{
    CliConnectionOptions, CliPayload, CliSupportError, ParsedSubcommandHead, format_json_output,
    parse_canonical_uuid, parse_subcommand_head, read_bounded_json_file,
};
use crate::wire::AgentProfileCreateRequest;
use crate::wire::profiles::{
    ProfileLifecycleRequest, ProfileUpdateRequest, WorkspaceProfilesQueryRequest,
};

/// Maximum JSON request file size accepted by `profile` CLI subcommands (`64 KiB`).
pub const MAX_PROFILE_CLI_BODY_BYTES: usize = 65_536;

/// Default page limit when `profile list` omits `--limit`.
pub const DEFAULT_PROFILE_CLI_LIST_LIMIT: u32 = 64;

/// Usage summary for the `profile` CLI subcommand family.
pub const PROFILE_CLI_USAGE: &str = "usage: heterarchy-orchestration-backend profile <create|list|get|update|disable|retire|mutation-get|workspace-query> --bearer-file <path> [--addr <ip:port>] [--request <json-file> | <flags...>]";

/// Payload source alias for `profile` CLI subcommands.
pub type ProfilePayload<T> = CliPayload<T>;

/// Parsed `profile` CLI dispatch result.
#[derive(Debug, PartialEq, Eq)]
pub enum ProfileCliCommand {
    /// Arguments belong to another top-level command or daemon mode.
    NotProfileCommand,
    /// Operator requested `profile` usage output.
    Help,
    /// Validated `profile` subcommand ready for execution.
    Run(Box<ProfileCliOptions>),
}

/// Typed `profile` action paired with its validated payload source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfileAction {
    Create(ProfilePayload<AgentProfileCreateRequest>),
    List {
        limit: Option<u32>,
        request_file: Option<PathBuf>,
    },
    Get {
        profile_id: Option<Uuid>,
        request_file: Option<PathBuf>,
    },
    Update {
        profile_id: Uuid,
        payload: ProfilePayload<ProfileUpdateRequest>,
    },
    Disable {
        profile_id: Uuid,
        payload: ProfilePayload<ProfileLifecycleRequest>,
    },
    Retire {
        profile_id: Uuid,
        payload: ProfilePayload<ProfileLifecycleRequest>,
    },
    MutationGet {
        request_id: Option<Uuid>,
        request_file: Option<PathBuf>,
    },
    WorkspaceQuery(ProfilePayload<WorkspaceProfilesQueryRequest>),
}

/// Validated `profile` CLI invocation options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileCliOptions {
    connection: CliConnectionOptions,
    action: ProfileAction,
}

impl ProfileCliOptions {
    #[must_use]
    pub const fn connection(&self) -> &CliConnectionOptions {
        &self.connection
    }

    #[must_use]
    pub const fn action(&self) -> &ProfileAction {
        &self.action
    }
}

/// Safe, redacted errors returned while parsing or executing `profile` CLI commands.
#[derive(Debug, Error)]
#[error(transparent)]
pub struct ProfileCliError(#[from] pub CliSupportError);

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProfileListSelector {
    #[serde(default)]
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProfileIdSelector {
    profile_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MutationReceiptSelector {
    request_id: String,
}

/// Parse `profile` subcommands using ambient `HETERARCHY_*` environment fallbacks.
///
/// # Errors
/// Rejects malformed flags, forbidden raw bearer flags, missing `--bearer-file`,
/// or non-loopback `--addr` values.
pub fn parse(arguments: &[OsString]) -> Result<ProfileCliCommand, ProfileCliError> {
    parse_with_env(arguments, |key| env::var_os(key))
}

/// Pure, deterministic parser accepting an explicit environment lookup closure for testing.
///
/// # Errors
/// Returns [`ProfileCliError`] when arguments or connection options are invalid.
pub fn parse_with_env<F>(
    arguments: &[OsString],
    env_lookup: F,
) -> Result<ProfileCliCommand, ProfileCliError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let (subcommand, mut index, mut conn) = match parse_subcommand_head("profile", arguments)? {
        ParsedSubcommandHead::NotThisCommand => return Ok(ProfileCliCommand::NotProfileCommand),
        ParsedSubcommandHead::Help => return Ok(ProfileCliCommand::Help),
        ParsedSubcommandHead::Subcommand {
            name,
            next_index,
            conn,
        } => (name, next_index, conn),
    };
    let action =
        parse_subcommands::dispatch(subcommand, arguments, &mut index, &mut conn, &env_lookup)?;
    let connection = conn.resolve_from_env(&env_lookup)?;
    Ok(ProfileCliCommand::Run(Box::new(ProfileCliOptions {
        connection,
        action,
    })))
}

/// Execute a validated `profile` CLI command against the loopback `OrchestrationHttpClient`.
///
/// # Errors
/// Returns redacted bearer-file, request-JSON, or bounded HTTP client failures.
pub async fn execute(options: &ProfileCliOptions) -> Result<String, ProfileCliError> {
    let (client, bearer) = options.connection.open_client_and_bearer()?;
    let auth = bearer.header_value();
    let json = execute_action(&client, auth, &options.action).await?;
    Ok(json)
}

async fn execute_action(
    client: &crate::client::OrchestrationHttpClient,
    auth: &hyper::header::HeaderValue,
    action: &ProfileAction,
) -> Result<String, CliSupportError> {
    match action {
        ProfileAction::Create(payload) => {
            let req = payload.resolve(MAX_PROFILE_CLI_BODY_BYTES)?;
            format_json_output(&client.create_profile(auth, &req).await?)
        }
        ProfileAction::List {
            limit,
            request_file,
        } => {
            let resolved_limit = resolve_list_limit(*limit, request_file.as_ref())?;
            format_json_output(&client.profile_list(auth, resolved_limit).await?)
        }
        ProfileAction::Get {
            profile_id,
            request_file,
        } => {
            let id = resolve_profile_id(*profile_id, request_file.as_ref())?;
            format_json_output(&client.profile(auth, id).await?)
        }
        ProfileAction::Update {
            profile_id,
            payload,
        } => {
            let req = payload.resolve(MAX_PROFILE_CLI_BODY_BYTES)?;
            format_json_output(&client.update_profile(auth, *profile_id, &req).await?)
        }
        ProfileAction::Disable {
            profile_id,
            payload,
        } => {
            let req = payload.resolve(MAX_PROFILE_CLI_BODY_BYTES)?;
            format_json_output(&client.disable_profile(auth, *profile_id, &req).await?)
        }
        ProfileAction::Retire {
            profile_id,
            payload,
        } => {
            let req = payload.resolve(MAX_PROFILE_CLI_BODY_BYTES)?;
            format_json_output(&client.retire_profile(auth, *profile_id, &req).await?)
        }
        ProfileAction::MutationGet {
            request_id,
            request_file,
        } => {
            let id = resolve_mutation_request_id(*request_id, request_file.as_ref())?;
            format_json_output(&client.profile_mutation_receipt(auth, id).await?)
        }
        ProfileAction::WorkspaceQuery(payload) => {
            let req = payload.resolve(MAX_PROFILE_CLI_BODY_BYTES)?;
            format_json_output(&client.workspace_profiles(auth, &req).await?)
        }
    }
}

fn resolve_list_limit(
    limit: Option<u32>,
    request_file: Option<&PathBuf>,
) -> Result<u32, CliSupportError> {
    match (limit, request_file) {
        (Some(explicit), None) => Ok(explicit),
        (None, Some(path)) => {
            let selector: ProfileListSelector =
                read_bounded_json_file(path, MAX_PROFILE_CLI_BODY_BYTES)?;
            Ok(selector.limit.unwrap_or(DEFAULT_PROFILE_CLI_LIST_LIMIT))
        }
        (None, None) => Ok(DEFAULT_PROFILE_CLI_LIST_LIMIT),
        (Some(_), Some(_)) => Err(CliSupportError::InvalidArguments),
    }
}

fn resolve_profile_id(
    profile_id: Option<Uuid>,
    request_file: Option<&PathBuf>,
) -> Result<Uuid, CliSupportError> {
    match (profile_id, request_file) {
        (Some(id), None) => Ok(id),
        (None, Some(path)) => {
            let selector: ProfileIdSelector =
                read_bounded_json_file(path, MAX_PROFILE_CLI_BODY_BYTES)?;
            parse_canonical_uuid(&selector.profile_id)
        }
        _ => Err(CliSupportError::InvalidArguments),
    }
}

fn resolve_mutation_request_id(
    request_id: Option<Uuid>,
    request_file: Option<&PathBuf>,
) -> Result<Uuid, CliSupportError> {
    match (request_id, request_file) {
        (Some(id), None) => Ok(id),
        (None, Some(path)) => {
            let selector: MutationReceiptSelector =
                read_bounded_json_file(path, MAX_PROFILE_CLI_BODY_BYTES)?;
            parse_canonical_uuid(&selector.request_id)
        }
        _ => Err(CliSupportError::InvalidArguments),
    }
}

#[cfg(test)]
mod tests;
