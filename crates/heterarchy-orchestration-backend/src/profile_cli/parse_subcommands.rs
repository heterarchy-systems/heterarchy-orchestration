//! Flag and `--request <json-file>` parsers for the 8 `profile` CLI subcommands.

use std::ffi::OsString;

use uuid::Uuid;

use super::{ProfileAction, ProfilePayload};
use crate::cli_support::{
    CliSupportError, ExecutionPreferenceFlagBuilder, RawConnectionFlags, WORKSPACE_ID_ENV,
    env_canonical_uuid, next_path_value, next_utf8_value, parse_canonical_uuid, parse_exact,
    set_canonical_uuid_string, set_once,
};
use crate::wire::profiles::{
    ProfileLifecycleRequest, ProfileUpdateRequest, WorkspaceProfilesQueryRequest,
};
use crate::wire::{AgentProfileCreateRequest, WorkspaceQueryScope};

pub(super) fn dispatch<F>(
    subcommand: &str,
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<ProfileAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    match subcommand {
        "create" => parse_create(arguments, index, conn),
        "list" => parse_list(arguments, index, conn),
        "get" => parse_get(arguments, index, conn),
        "update" => parse_update(arguments, index, conn),
        "disable" => parse_lifecycle(arguments, index, conn, false),
        "retire" => parse_lifecycle(arguments, index, conn, true),
        "mutation-get" => parse_mutation_get(arguments, index, conn),
        "workspace-query" => parse_workspace_query(arguments, index, conn, env_lookup),
        _ => Err(CliSupportError::InvalidArguments),
    }
}

#[derive(Default)]
struct CreateFlags {
    request_id: Option<String>,
    stable_name: Option<String>,
    display_name: Option<String>,
    provider_family: Option<String>,
    pref: ExecutionPreferenceFlagBuilder,
}

fn parse_create(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
) -> Result<ProfileAction, CliSupportError> {
    let mut request_file = None;
    let mut flags = CreateFlags::default();
    let mut saw_inline = false;

    while *index < arguments.len() {
        if conn.try_consume(arguments, index)? {
            continue;
        }
        let flag = arguments[*index]
            .to_str()
            .ok_or(CliSupportError::InvalidArguments)?;
        if flag == "--request" {
            set_once(&mut request_file, next_path_value(arguments, index)?)?;
            continue;
        }
        saw_inline = true;
        if flags.pref.try_consume(flag, arguments, index)? {
            continue;
        }
        match flag {
            "--request-id" => set_canonical_uuid_string(&mut flags.request_id, arguments, index)?,
            "--stable-name" => set_once(
                &mut flags.stable_name,
                next_utf8_value(arguments, index)?.to_owned(),
            )?,
            "--display-name" => set_once(
                &mut flags.display_name,
                next_utf8_value(arguments, index)?.to_owned(),
            )?,
            "--provider-family" => {
                let raw = next_utf8_value(arguments, index)?;
                if !matches!(raw, "unbound" | "UNBOUND") {
                    return Err(CliSupportError::InvalidArguments);
                }
                set_once(&mut flags.provider_family, raw.to_ascii_lowercase())?;
            }
            _ => return Err(CliSupportError::InvalidArguments),
        }
    }
    if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(ProfileAction::Create(ProfilePayload::RequestFile(path)));
    }
    let resolved = flags.pref.into_profile_fields()?;
    Ok(ProfileAction::Create(ProfilePayload::Inline(
        AgentProfileCreateRequest {
            request_id: flags.request_id.ok_or(CliSupportError::InvalidArguments)?,
            stable_name: flags.stable_name.ok_or(CliSupportError::InvalidArguments)?,
            display_name: flags
                .display_name
                .ok_or(CliSupportError::InvalidArguments)?,
            preferred_model: resolved.preferred_model,
            preferred_reasoning: resolved.preferred_reasoning,
            base_capabilities: resolved.base_capabilities,
            execution_preference: resolved.execution_preference,
        },
    )))
}

fn parse_list(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
) -> Result<ProfileAction, CliSupportError> {
    let mut limit = None;
    let mut request_file = None;
    while *index < arguments.len() {
        if conn.try_consume(arguments, index)? {
            continue;
        }
        match arguments[*index].to_str() {
            Some("--limit") => {
                set_once(&mut limit, parse_exact(next_utf8_value(arguments, index)?)?)?;
            }
            Some("--request") => set_once(&mut request_file, next_path_value(arguments, index)?)?,
            _ => return Err(CliSupportError::InvalidArguments),
        }
    }
    if limit.is_some() && request_file.is_some() {
        return Err(CliSupportError::InvalidArguments);
    }
    Ok(ProfileAction::List {
        limit,
        request_file,
    })
}

fn parse_get(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
) -> Result<ProfileAction, CliSupportError> {
    let mut profile_id = None;
    let mut request_file = None;
    while *index < arguments.len() {
        if conn.try_consume(arguments, index)? {
            continue;
        }
        match arguments[*index].to_str() {
            Some("--profile-id") => set_once(
                &mut profile_id,
                parse_canonical_uuid(next_utf8_value(arguments, index)?)?,
            )?,
            Some("--request") => set_once(&mut request_file, next_path_value(arguments, index)?)?,
            _ => return Err(CliSupportError::InvalidArguments),
        }
    }
    if profile_id.is_some() == request_file.is_some() {
        return Err(CliSupportError::InvalidArguments);
    }
    Ok(ProfileAction::Get {
        profile_id,
        request_file,
    })
}

#[derive(Default)]
struct UpdateFlags {
    request_id: Option<String>,
    expected_revision: Option<i64>,
    display_name: Option<String>,
    pref: ExecutionPreferenceFlagBuilder,
}

fn parse_update(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
) -> Result<ProfileAction, CliSupportError> {
    let mut profile_id = None;
    let mut request_file = None;
    let mut flags = UpdateFlags::default();
    let mut saw_inline = false;

    while *index < arguments.len() {
        if conn.try_consume(arguments, index)? {
            continue;
        }
        let flag = arguments[*index]
            .to_str()
            .ok_or(CliSupportError::InvalidArguments)?;
        if flag == "--profile-id" {
            set_once(
                &mut profile_id,
                parse_canonical_uuid(next_utf8_value(arguments, index)?)?,
            )?;
            continue;
        }
        if flag == "--request" {
            set_once(&mut request_file, next_path_value(arguments, index)?)?;
            continue;
        }
        saw_inline = true;
        if flags.pref.try_consume(flag, arguments, index)? {
            continue;
        }
        match flag {
            "--request-id" => set_canonical_uuid_string(&mut flags.request_id, arguments, index)?,
            "--expected-revision" => set_once(
                &mut flags.expected_revision,
                parse_exact(next_utf8_value(arguments, index)?)?,
            )?,
            "--display-name" => set_once(
                &mut flags.display_name,
                next_utf8_value(arguments, index)?.to_owned(),
            )?,
            _ => return Err(CliSupportError::InvalidArguments),
        }
    }
    let profile_id = profile_id.ok_or(CliSupportError::InvalidArguments)?;
    if let Some(path) = request_file {
        if saw_inline || flags.pref.has_any() {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(ProfileAction::Update {
            profile_id,
            payload: ProfilePayload::RequestFile(path),
        });
    }
    let resolved = flags.pref.into_profile_fields()?;
    Ok(ProfileAction::Update {
        profile_id,
        payload: ProfilePayload::Inline(ProfileUpdateRequest {
            request_id: flags.request_id.ok_or(CliSupportError::InvalidArguments)?,
            expected_revision: flags
                .expected_revision
                .ok_or(CliSupportError::InvalidArguments)?,
            display_name: flags
                .display_name
                .ok_or(CliSupportError::InvalidArguments)?,
            preferred_model: resolved.preferred_model,
            preferred_reasoning: resolved.preferred_reasoning,
            base_capabilities: resolved.base_capabilities,
            execution_preference: resolved.execution_preference,
        }),
    })
}

fn parse_lifecycle(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    retire: bool,
) -> Result<ProfileAction, CliSupportError> {
    let mut profile_id = None;
    let mut request_file = None;
    let mut request_id = None;
    let mut expected_revision = None;
    let mut saw_inline = false;

    while *index < arguments.len() {
        if conn.try_consume(arguments, index)? {
            continue;
        }
        let flag = arguments[*index]
            .to_str()
            .ok_or(CliSupportError::InvalidArguments)?;
        if flag == "--profile-id" {
            set_once(
                &mut profile_id,
                parse_canonical_uuid(next_utf8_value(arguments, index)?)?,
            )?;
            continue;
        }
        if flag == "--request" {
            set_once(&mut request_file, next_path_value(arguments, index)?)?;
            continue;
        }
        saw_inline = true;
        match flag {
            "--request-id" => set_canonical_uuid_string(&mut request_id, arguments, index)?,
            "--expected-revision" => set_once(
                &mut expected_revision,
                parse_exact(next_utf8_value(arguments, index)?)?,
            )?,
            _ => return Err(CliSupportError::InvalidArguments),
        }
    }
    let profile_id = profile_id.ok_or(CliSupportError::InvalidArguments)?;
    let payload = if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        ProfilePayload::RequestFile(path)
    } else {
        ProfilePayload::Inline(ProfileLifecycleRequest {
            request_id: request_id.ok_or(CliSupportError::InvalidArguments)?,
            expected_revision: expected_revision.ok_or(CliSupportError::InvalidArguments)?,
        })
    };
    if retire {
        Ok(ProfileAction::Retire {
            profile_id,
            payload,
        })
    } else {
        Ok(ProfileAction::Disable {
            profile_id,
            payload,
        })
    }
}

fn parse_mutation_get(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
) -> Result<ProfileAction, CliSupportError> {
    let mut request_id = None;
    let mut request_file = None;
    while *index < arguments.len() {
        if conn.try_consume(arguments, index)? {
            continue;
        }
        match arguments[*index].to_str() {
            Some("--request-id") => set_once(
                &mut request_id,
                parse_canonical_uuid(next_utf8_value(arguments, index)?)?,
            )?,
            Some("--request") => set_once(&mut request_file, next_path_value(arguments, index)?)?,
            _ => return Err(CliSupportError::InvalidArguments),
        }
    }
    if request_id.is_some() == request_file.is_some() {
        return Err(CliSupportError::InvalidArguments);
    }
    Ok(ProfileAction::MutationGet {
        request_id,
        request_file,
    })
}

#[derive(Default)]
struct WorkspaceQueryFlags {
    workspace_id: Option<Uuid>,
    location_id: Option<Uuid>,
    location_revision: Option<i64>,
    workspace_revision: Option<i64>,
    locator: Option<String>,
    session_id: Option<Uuid>,
}

fn parse_workspace_query<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<ProfileAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut request_file = None;
    let mut flags = WorkspaceQueryFlags::default();
    let mut saw_inline = false;

    while *index < arguments.len() {
        if conn.try_consume(arguments, index)? {
            continue;
        }
        let flag = arguments[*index]
            .to_str()
            .ok_or(CliSupportError::InvalidArguments)?;
        if flag == "--request" {
            set_once(&mut request_file, next_path_value(arguments, index)?)?;
            continue;
        }
        saw_inline = true;
        consume_workspace_query_flag(&mut flags, flag, arguments, index)?;
    }
    if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(ProfileAction::WorkspaceQuery(ProfilePayload::RequestFile(
            path,
        )));
    }
    let workspace_id = match flags.workspace_id {
        Some(explicit) => explicit,
        None => env_canonical_uuid(env_lookup, WORKSPACE_ID_ENV)?
            .ok_or(CliSupportError::InvalidArguments)?,
    };
    Ok(ProfileAction::WorkspaceQuery(ProfilePayload::Inline(
        WorkspaceProfilesQueryRequest {
            scope: WorkspaceQueryScope {
                workspace_id,
                location_id: flags.location_id.ok_or(CliSupportError::InvalidArguments)?,
                location_revision: flags
                    .location_revision
                    .ok_or(CliSupportError::InvalidArguments)?,
                workspace_revision: flags
                    .workspace_revision
                    .ok_or(CliSupportError::InvalidArguments)?,
                locator: flags.locator.ok_or(CliSupportError::InvalidArguments)?,
            },
            session_id: flags.session_id,
        },
    )))
}

fn consume_workspace_query_flag(
    flags: &mut WorkspaceQueryFlags,
    flag: &str,
    arguments: &[OsString],
    index: &mut usize,
) -> Result<(), CliSupportError> {
    match flag {
        "--workspace-id" => set_once(
            &mut flags.workspace_id,
            parse_canonical_uuid(next_utf8_value(arguments, index)?)?,
        ),
        "--location-id" => set_once(
            &mut flags.location_id,
            parse_canonical_uuid(next_utf8_value(arguments, index)?)?,
        ),
        "--location-revision" => set_once(
            &mut flags.location_revision,
            parse_exact(next_utf8_value(arguments, index)?)?,
        ),
        "--workspace-revision" => set_once(
            &mut flags.workspace_revision,
            parse_exact(next_utf8_value(arguments, index)?)?,
        ),
        "--locator" => set_once(
            &mut flags.locator,
            next_utf8_value(arguments, index)?.to_owned(),
        ),
        "--session-id" => set_once(
            &mut flags.session_id,
            parse_canonical_uuid(next_utf8_value(arguments, index)?)?,
        ),
        _ => Err(CliSupportError::InvalidArguments),
    }
}
