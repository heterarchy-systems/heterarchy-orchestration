//! Flag and `--request <json-file>` parsers for East-West `Orchestration <-> Forge`
//! `forge-converge`, `forge-settle`, and `outbox-sync` Swarm CLI subcommands.

use std::ffi::OsString;

use uuid::Uuid;

use super::parse_subcommands::{RunScopeFlags, resolve_workspace_id};
use super::{SwarmAction, SwarmPayload};
use crate::cli_support::{
    CliSupportError, RawConnectionFlags, next_path_value, next_utf8_value, parse_canonical_uuid,
    parse_exact, set_canonical_uuid_string, set_once,
};
use crate::wire::swarm::{
    ForgeConvergeSwarmRequest, ForgeSettleSwarmRequest, SyncAutonomyOutboxRequest,
};

pub(super) fn dispatch_bridge<F>(
    subcommand: &str,
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<SwarmAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    match subcommand {
        "forge-converge" => parse_forge_converge(arguments, index, conn, env_lookup),
        "forge-settle" => parse_forge_settle(arguments, index, conn, env_lookup),
        "outbox-sync" => parse_outbox_sync(arguments, index, conn, env_lookup),
        _ => Err(CliSupportError::InvalidArguments),
    }
}

#[derive(Default)]
struct ForgeConvergeFlags {
    request_id: Option<String>,
    session_id: Option<String>,
    candidate_member_ids: Vec<String>,
    stage_candidate_only: bool,
}

fn parse_forge_converge<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<SwarmAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut scope = RunScopeFlags::default();
    let mut flags = ForgeConvergeFlags::default();
    let mut saw_inline = false;

    while *index < arguments.len() {
        if conn.try_consume(arguments, index)? {
            continue;
        }
        let flag = arguments[*index]
            .to_str()
            .ok_or(CliSupportError::InvalidArguments)?;
        if scope.try_consume(flag, arguments, index)? {
            continue;
        }
        saw_inline = true;
        match flag {
            "--request-id" => set_canonical_uuid_string(&mut flags.request_id, arguments, index)?,
            "--session-id" | "--forge-session-id" => {
                set_canonical_uuid_string(&mut flags.session_id, arguments, index)?;
            }
            "--candidate-member-id" => {
                let id = parse_canonical_uuid(next_utf8_value(arguments, index)?)?;
                flags.candidate_member_ids.push(id.to_string());
            }
            "--stage-candidate-only" if !flags.stage_candidate_only => {
                flags.stage_candidate_only = true;
                *index += 1;
            }
            _ => return Err(CliSupportError::InvalidArguments),
        }
    }

    let (workspace_id, swarm_run_id, request_file) = scope.require_run(env_lookup)?;
    if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(SwarmAction::ForgeConverge {
            workspace_id,
            swarm_run_id,
            payload: SwarmPayload::RequestFile(path),
        });
    }
    Ok(SwarmAction::ForgeConverge {
        workspace_id,
        swarm_run_id,
        payload: SwarmPayload::Inline(ForgeConvergeSwarmRequest {
            request_id: flags
                .request_id
                .unwrap_or_else(|| Uuid::now_v7().to_string()),
            session_id: flags.session_id.ok_or(CliSupportError::InvalidArguments)?,
            candidate_member_ids: flags.candidate_member_ids,
            stage_candidate_only: flags.stage_candidate_only,
        }),
    })
}

#[derive(Default)]
struct ForgeSettleFlags {
    request_id: Option<String>,
    session_id: Option<String>,
    activity_id: Option<String>,
    coordinator_agent_id: Option<String>,
    outbox_migration_id: Option<String>,
    outbox_after_sequence: Option<u64>,
    purge_ephemeral: bool,
}

fn parse_forge_settle<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<SwarmAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut scope = RunScopeFlags::default();
    let mut flags = ForgeSettleFlags::default();
    let mut saw_inline = false;

    while *index < arguments.len() {
        if conn.try_consume(arguments, index)? {
            continue;
        }
        let flag = arguments[*index]
            .to_str()
            .ok_or(CliSupportError::InvalidArguments)?;
        if scope.try_consume(flag, arguments, index)? {
            continue;
        }
        saw_inline = true;
        consume_forge_settle_flag(&mut flags, flag, arguments, index)?;
    }

    let (workspace_id, swarm_run_id, request_file) = scope.require_run(env_lookup)?;
    if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(SwarmAction::ForgeSettle {
            workspace_id,
            swarm_run_id,
            payload: SwarmPayload::RequestFile(path),
        });
    }
    Ok(SwarmAction::ForgeSettle {
        workspace_id,
        swarm_run_id,
        payload: SwarmPayload::Inline(ForgeSettleSwarmRequest {
            request_id: flags
                .request_id
                .unwrap_or_else(|| Uuid::now_v7().to_string()),
            session_id: flags.session_id.ok_or(CliSupportError::InvalidArguments)?,
            activity_id: flags.activity_id,
            coordinator_agent_id: flags
                .coordinator_agent_id
                .ok_or(CliSupportError::InvalidArguments)?,
            outbox_migration_id: flags.outbox_migration_id,
            outbox_after_sequence: flags.outbox_after_sequence,
            purge_ephemeral: flags.purge_ephemeral,
        }),
    })
}

fn consume_forge_settle_flag(
    flags: &mut ForgeSettleFlags,
    flag: &str,
    arguments: &[OsString],
    index: &mut usize,
) -> Result<(), CliSupportError> {
    match flag {
        "--request-id" => set_canonical_uuid_string(&mut flags.request_id, arguments, index),
        "--session-id" | "--forge-session-id" => {
            set_canonical_uuid_string(&mut flags.session_id, arguments, index)
        }
        "--activity-id" => set_canonical_uuid_string(&mut flags.activity_id, arguments, index),
        "--coordinator-agent-id" => {
            set_canonical_uuid_string(&mut flags.coordinator_agent_id, arguments, index)
        }
        "--outbox-migration-id" => {
            set_canonical_uuid_string(&mut flags.outbox_migration_id, arguments, index)
        }
        "--outbox-after-sequence" => set_once(
            &mut flags.outbox_after_sequence,
            parse_exact(next_utf8_value(arguments, index)?)?,
        ),
        "--purge-ephemeral" if !flags.purge_ephemeral => {
            flags.purge_ephemeral = true;
            *index += 1;
            Ok(())
        }
        _ => Err(CliSupportError::InvalidArguments),
    }
}

fn parse_outbox_sync<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<SwarmAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut workspace_id = None;
    let mut request_file = None;
    let mut migration_id = None;
    let mut after_sequence = None;
    let mut limit = None;
    let mut saw_inline = false;

    while *index < arguments.len() {
        if conn.try_consume(arguments, index)? {
            continue;
        }
        let flag = arguments[*index]
            .to_str()
            .ok_or(CliSupportError::InvalidArguments)?;
        match flag {
            "--workspace-id" => {
                set_once(
                    &mut workspace_id,
                    parse_canonical_uuid(next_utf8_value(arguments, index)?)?,
                )?;
            }
            "--request" => {
                set_once(&mut request_file, next_path_value(arguments, index)?)?;
            }
            "--migration-id" => {
                saw_inline = true;
                set_canonical_uuid_string(&mut migration_id, arguments, index)?;
            }
            "--after-sequence" => {
                saw_inline = true;
                set_once(
                    &mut after_sequence,
                    parse_exact(next_utf8_value(arguments, index)?)?,
                )?;
            }
            "--limit" => {
                saw_inline = true;
                set_once(&mut limit, parse_exact(next_utf8_value(arguments, index)?)?)?;
            }
            _ => return Err(CliSupportError::InvalidArguments),
        }
    }

    let workspace_id = resolve_workspace_id(workspace_id, env_lookup)?;
    if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(SwarmAction::OutboxSync {
            workspace_id,
            payload: SwarmPayload::RequestFile(path),
        });
    }
    Ok(SwarmAction::OutboxSync {
        workspace_id,
        payload: SwarmPayload::Inline(SyncAutonomyOutboxRequest {
            migration_id: migration_id.ok_or(CliSupportError::InvalidArguments)?,
            after_sequence: after_sequence.unwrap_or(0),
            limit,
        }),
    })
}
