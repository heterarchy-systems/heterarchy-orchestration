//! Flag and `--request <json-file>` parsers for Swarm turn supervision, model
//! switching, adversarial verification panels, convergence, settlement, abort,
//! and stall remediation CLI subcommands.

use std::ffi::OsString;

use uuid::Uuid;

use super::parse_subcommands::{RunScopeFlags, resolve_member_id, resolve_member_id_string};
use super::{SwarmAction, SwarmPayload};
use crate::cli_support::{
    CliSupportError, ExecutionPreferenceFlagBuilder, RawConnectionFlags, next_utf8_value,
    parse_canonical_uuid, parse_exact, set_canonical_uuid_string, set_once,
};
use crate::wire::autonomy_loop::{ExecutionPermissionsWire, ExecutionTimeoutsWire, McpGrantWire};
use crate::wire::swarm::{
    AbortSwarmRunRequest, ClaimSwarmWakeupRequest, CompleteSwarmTurnRequest, ConvergeSwarmRequest,
    CreateSwarmVerificationPanelRequest, DispatchSwarmTurnRequest, FailSwarmTurnRequest,
    RecordSwarmVerdictRequest, SettleSwarmRequest, SwarmStallCheckRequest,
    SwitchSwarmMemberModelRequest,
};

pub(super) fn dispatch_turn_or_governance<F>(
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
        "claim" => parse_claim(arguments, index, conn, env_lookup),
        "complete" => parse_complete(arguments, index, conn, env_lookup),
        "fail" => parse_fail(arguments, index, conn, env_lookup),
        "dispatch" => parse_dispatch(arguments, index, conn, env_lookup),
        "model-switch" => parse_model_switch(arguments, index, conn, env_lookup),
        "panel-create" => parse_panel_create(arguments, index, conn, env_lookup),
        "verdict" => parse_verdict(arguments, index, conn, env_lookup),
        "converge" => parse_converge(arguments, index, conn, env_lookup),
        "settle" => parse_settle(arguments, index, conn, env_lookup),
        "abort" => parse_abort(arguments, index, conn, env_lookup),
        "stall-check" => parse_stall_check(arguments, index, conn, env_lookup),
        _ => Err(CliSupportError::InvalidArguments),
    }
}

fn parse_claim<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<SwarmAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut scope = RunScopeFlags::default();
    let mut member_id = None;
    let mut snapshot_id = None;

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
        match flag {
            "--member-id" => set_once(
                &mut member_id,
                parse_canonical_uuid(next_utf8_value(arguments, index)?)?,
            )?,
            "--snapshot-id" => set_canonical_uuid_string(&mut snapshot_id, arguments, index)?,
            _ => return Err(CliSupportError::InvalidArguments),
        }
    }
    let (workspace_id, swarm_run_id, request_file) = scope.require_run(env_lookup)?;
    let member_id = resolve_member_id(member_id, env_lookup)?;
    if let Some(path) = request_file {
        if snapshot_id.is_some() {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(SwarmAction::Claim {
            workspace_id,
            swarm_run_id,
            member_id,
            payload: SwarmPayload::RequestFile(path),
        });
    }
    Ok(SwarmAction::Claim {
        workspace_id,
        swarm_run_id,
        member_id,
        payload: SwarmPayload::Inline(ClaimSwarmWakeupRequest {
            snapshot_id: snapshot_id.ok_or(CliSupportError::InvalidArguments)?,
        }),
    })
}

fn parse_complete<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<SwarmAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut scope = RunScopeFlags::default();
    let mut member_id = None;
    let mut expected_snapshot_id = None;
    let mut output_commit_oid = None;
    let mut output_tree_blake3_hex = None;
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
        if flag == "--member-id" {
            set_once(
                &mut member_id,
                parse_canonical_uuid(next_utf8_value(arguments, index)?)?,
            )?;
            continue;
        }
        saw_inline = true;
        match flag {
            "--expected-snapshot-id" => {
                set_canonical_uuid_string(&mut expected_snapshot_id, arguments, index)?;
            }
            "--output-commit-oid" => set_once(
                &mut output_commit_oid,
                next_utf8_value(arguments, index)?.to_owned(),
            )?,
            "--output-tree-blake3" => set_once(
                &mut output_tree_blake3_hex,
                next_utf8_value(arguments, index)?.to_owned(),
            )?,
            _ => return Err(CliSupportError::InvalidArguments),
        }
    }
    let (workspace_id, swarm_run_id, request_file) = scope.require_run(env_lookup)?;
    let member_id = resolve_member_id(member_id, env_lookup)?;
    if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(SwarmAction::Complete {
            workspace_id,
            swarm_run_id,
            member_id,
            payload: SwarmPayload::RequestFile(path),
        });
    }
    if output_commit_oid.is_some() != output_tree_blake3_hex.is_some() {
        return Err(CliSupportError::InvalidArguments);
    }
    Ok(SwarmAction::Complete {
        workspace_id,
        swarm_run_id,
        member_id,
        payload: SwarmPayload::Inline(CompleteSwarmTurnRequest {
            expected_snapshot_id: expected_snapshot_id.ok_or(CliSupportError::InvalidArguments)?,
            output_commit_oid,
            output_tree_blake3_hex,
        }),
    })
}

fn parse_fail<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<SwarmAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut scope = RunScopeFlags::default();
    let mut member_id = None;
    let mut expected_snapshot_id = None;
    let mut disposition = None;
    let mut consume_turn = false;
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
        if flag == "--member-id" {
            set_once(
                &mut member_id,
                parse_canonical_uuid(next_utf8_value(arguments, index)?)?,
            )?;
            continue;
        }
        saw_inline = true;
        match flag {
            "--expected-snapshot-id" => {
                set_canonical_uuid_string(&mut expected_snapshot_id, arguments, index)?;
            }
            "--disposition" => set_once(
                &mut disposition,
                next_utf8_value(arguments, index)?.to_owned(),
            )?,
            "--consume-turn" if !consume_turn => {
                consume_turn = true;
                *index += 1;
            }
            _ => return Err(CliSupportError::InvalidArguments),
        }
    }
    let (workspace_id, swarm_run_id, request_file) = scope.require_run(env_lookup)?;
    let member_id = resolve_member_id(member_id, env_lookup)?;
    if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(SwarmAction::Fail {
            workspace_id,
            swarm_run_id,
            member_id,
            payload: SwarmPayload::RequestFile(path),
        });
    }
    Ok(SwarmAction::Fail {
        workspace_id,
        swarm_run_id,
        member_id,
        payload: SwarmPayload::Inline(FailSwarmTurnRequest {
            expected_snapshot_id: expected_snapshot_id.ok_or(CliSupportError::InvalidArguments)?,
            disposition: disposition.ok_or(CliSupportError::InvalidArguments)?,
            consume_turn,
        }),
    })
}

#[derive(Default)]
struct DispatchFlags {
    snapshot_request_id: Option<String>,
    snapshot_id: Option<String>,
    activity_id: Option<String>,
    default_worktree_path: Option<String>,
    lease_id: Option<String>,
    lease_revision: Option<u64>,
    lease_expires_at_ms: Option<i64>,
    mcp_token_id: Option<String>,
    mcp_issued_at_ms: Option<i64>,
    mcp_expires_at_ms: Option<i64>,
    permission_bits: u8,
    startup_timeout_ms: Option<u64>,
    execution_timeout_ms: Option<u64>,
    termination_grace_ms: Option<u64>,
    dispatched_at_ms: Option<i64>,
    clean_exit_output_commit_oid: Option<String>,
    clean_exit_output_tree_blake3_hex: Option<String>,
}

fn parse_dispatch<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<SwarmAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut scope = RunScopeFlags::default();
    let mut member_id = None;
    let mut flags = DispatchFlags::default();
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
        if flag == "--member-id" {
            set_once(
                &mut member_id,
                parse_canonical_uuid(next_utf8_value(arguments, index)?)?,
            )?;
            continue;
        }
        saw_inline = true;
        consume_dispatch_flag(&mut flags, flag, arguments, index)?;
    }
    let (workspace_id, swarm_run_id, request_file) = scope.require_run(env_lookup)?;
    let member_id = resolve_member_id(member_id, env_lookup)?;
    if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(SwarmAction::Dispatch {
            workspace_id,
            swarm_run_id,
            member_id,
            payload: SwarmPayload::RequestFile(path),
        });
    }
    Ok(SwarmAction::Dispatch {
        workspace_id,
        swarm_run_id,
        member_id,
        payload: SwarmPayload::Inline(build_dispatch_request(flags)?),
    })
}

fn consume_dispatch_flag(
    flags: &mut DispatchFlags,
    flag: &str,
    arguments: &[OsString],
    index: &mut usize,
) -> Result<(), CliSupportError> {
    match flag {
        "--snapshot-request-id" => {
            set_canonical_uuid_string(&mut flags.snapshot_request_id, arguments, index)
        }
        "--snapshot-id" => set_canonical_uuid_string(&mut flags.snapshot_id, arguments, index),
        "--activity-id" => set_canonical_uuid_string(&mut flags.activity_id, arguments, index),
        "--default-worktree-path" | "--worktree-path" => set_once(
            &mut flags.default_worktree_path,
            next_utf8_value(arguments, index)?.to_owned(),
        ),
        "--lease-id" => set_canonical_uuid_string(&mut flags.lease_id, arguments, index),
        "--lease-revision" => set_once(
            &mut flags.lease_revision,
            parse_exact(next_utf8_value(arguments, index)?)?,
        ),
        "--lease-expires-at-ms" => set_once(
            &mut flags.lease_expires_at_ms,
            parse_exact(next_utf8_value(arguments, index)?)?,
        ),
        _ => consume_dispatch_grant_and_timeout_flag(flags, flag, arguments, index),
    }
}

fn consume_dispatch_grant_and_timeout_flag(
    flags: &mut DispatchFlags,
    flag: &str,
    arguments: &[OsString],
    index: &mut usize,
) -> Result<(), CliSupportError> {
    match flag {
        "--mcp-token-id" => set_canonical_uuid_string(&mut flags.mcp_token_id, arguments, index),
        "--mcp-issued-at-ms" => set_once(
            &mut flags.mcp_issued_at_ms,
            parse_exact(next_utf8_value(arguments, index)?)?,
        ),
        "--mcp-expires-at-ms" => set_once(
            &mut flags.mcp_expires_at_ms,
            parse_exact(next_utf8_value(arguments, index)?)?,
        ),
        "--allow-workspace-write" if (flags.permission_bits & 0b001) == 0 => {
            flags.permission_bits |= 0b001;
            *index += 1;
            Ok(())
        }
        "--allow-local-shell" if (flags.permission_bits & 0b010) == 0 => {
            flags.permission_bits |= 0b010;
            *index += 1;
            Ok(())
        }
        "--allow-network" if (flags.permission_bits & 0b100) == 0 => {
            flags.permission_bits |= 0b100;
            *index += 1;
            Ok(())
        }
        "--startup-timeout-ms" => set_once(
            &mut flags.startup_timeout_ms,
            parse_exact(next_utf8_value(arguments, index)?)?,
        ),
        "--execution-timeout-ms" => set_once(
            &mut flags.execution_timeout_ms,
            parse_exact(next_utf8_value(arguments, index)?)?,
        ),
        "--termination-grace-ms" => set_once(
            &mut flags.termination_grace_ms,
            parse_exact(next_utf8_value(arguments, index)?)?,
        ),
        "--dispatched-at-ms" => set_once(
            &mut flags.dispatched_at_ms,
            parse_exact(next_utf8_value(arguments, index)?)?,
        ),
        "--clean-exit-output-commit-oid" => set_once(
            &mut flags.clean_exit_output_commit_oid,
            next_utf8_value(arguments, index)?.to_owned(),
        ),
        "--clean-exit-output-tree-blake3" => set_once(
            &mut flags.clean_exit_output_tree_blake3_hex,
            next_utf8_value(arguments, index)?.to_owned(),
        ),
        _ => Err(CliSupportError::InvalidArguments),
    }
}

fn build_dispatch_request(
    flags: DispatchFlags,
) -> Result<DispatchSwarmTurnRequest, CliSupportError> {
    if flags.clean_exit_output_commit_oid.is_some()
        != flags.clean_exit_output_tree_blake3_hex.is_some()
    {
        return Err(CliSupportError::InvalidArguments);
    }
    let mcp_grant = match (
        flags.mcp_token_id,
        flags.mcp_issued_at_ms,
        flags.mcp_expires_at_ms,
    ) {
        (None, None, None) => None,
        (Some(token_id), Some(issued_at_ms), Some(expires_at_ms)) => Some(McpGrantWire {
            token_id,
            issued_at_ms,
            expires_at_ms,
        }),
        _ => return Err(CliSupportError::InvalidArguments),
    };
    Ok(DispatchSwarmTurnRequest {
        snapshot_request_id: flags
            .snapshot_request_id
            .unwrap_or_else(|| Uuid::now_v7().to_string()),
        snapshot_id: flags.snapshot_id.ok_or(CliSupportError::InvalidArguments)?,
        activity_id: flags.activity_id.ok_or(CliSupportError::InvalidArguments)?,
        default_worktree_path: flags
            .default_worktree_path
            .ok_or(CliSupportError::InvalidArguments)?,
        lease_id: flags.lease_id.ok_or(CliSupportError::InvalidArguments)?,
        lease_revision: flags
            .lease_revision
            .ok_or(CliSupportError::InvalidArguments)?,
        lease_expires_at_ms: flags
            .lease_expires_at_ms
            .ok_or(CliSupportError::InvalidArguments)?,
        mcp_grant,
        permissions: ExecutionPermissionsWire {
            allow_workspace_write: (flags.permission_bits & 0b001) != 0,
            allow_local_shell: (flags.permission_bits & 0b010) != 0,
            allow_network: (flags.permission_bits & 0b100) != 0,
        },
        timeouts: ExecutionTimeoutsWire {
            startup_timeout_ms: flags
                .startup_timeout_ms
                .ok_or(CliSupportError::InvalidArguments)?,
            execution_timeout_ms: flags
                .execution_timeout_ms
                .ok_or(CliSupportError::InvalidArguments)?,
            termination_grace_ms: flags
                .termination_grace_ms
                .ok_or(CliSupportError::InvalidArguments)?,
        },
        dispatched_at_ms: flags.dispatched_at_ms,
        clean_exit_output_commit_oid: flags.clean_exit_output_commit_oid,
        clean_exit_output_tree_blake3_hex: flags.clean_exit_output_tree_blake3_hex,
    })
}

#[derive(Default)]
struct ModelSwitchFlags {
    new_profile_id: Option<String>,
    request_id: Option<String>,
    profile_id: Option<String>,
    expected_profile_revision: Option<i64>,
    pref: ExecutionPreferenceFlagBuilder,
}

fn parse_model_switch<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<SwarmAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut scope = RunScopeFlags::default();
    let mut member_id = None;
    let mut flags = ModelSwitchFlags::default();
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
        if flag == "--member-id" {
            set_once(
                &mut member_id,
                parse_canonical_uuid(next_utf8_value(arguments, index)?)?,
            )?;
            continue;
        }
        saw_inline = true;
        consume_model_switch_flag(&mut flags, flag, arguments, index)?;
    }
    let (workspace_id, swarm_run_id, request_file) = scope.require_run(env_lookup)?;
    let member_id = resolve_member_id(member_id, env_lookup)?;
    if let Some(path) = request_file {
        if saw_inline || flags.pref.has_any() {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(SwarmAction::ModelSwitch {
            workspace_id,
            swarm_run_id,
            member_id,
            payload: SwarmPayload::RequestFile(path),
        });
    }
    Ok(SwarmAction::ModelSwitch {
        workspace_id,
        swarm_run_id,
        member_id,
        payload: SwarmPayload::Inline(build_model_switch_request(flags)?),
    })
}

fn consume_model_switch_flag(
    flags: &mut ModelSwitchFlags,
    flag: &str,
    arguments: &[OsString],
    index: &mut usize,
) -> Result<(), CliSupportError> {
    if flags.pref.try_consume(flag, arguments, index)? {
        return Ok(());
    }
    match flag {
        "--new-profile-id" => {
            set_canonical_uuid_string(&mut flags.new_profile_id, arguments, index)
        }
        "--request-id" => set_canonical_uuid_string(&mut flags.request_id, arguments, index),
        "--profile-id" => set_canonical_uuid_string(&mut flags.profile_id, arguments, index),
        "--expected-profile-revision" => set_once(
            &mut flags.expected_profile_revision,
            parse_exact(next_utf8_value(arguments, index)?)?,
        ),
        _ => Err(CliSupportError::InvalidArguments),
    }
}

fn build_model_switch_request(
    flags: ModelSwitchFlags,
) -> Result<SwitchSwarmMemberModelRequest, CliSupportError> {
    if let Some(new_profile_id) = flags.new_profile_id {
        if flags.request_id.is_some()
            || flags.profile_id.is_some()
            || flags.expected_profile_revision.is_some()
            || flags.pref.has_any()
        {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(SwitchSwarmMemberModelRequest {
            new_profile_id: Some(new_profile_id),
            request_id: None,
            profile_id: None,
            expected_profile_revision: None,
            new_preference: None,
        });
    }
    Ok(SwitchSwarmMemberModelRequest {
        new_profile_id: None,
        request_id: Some(flags.request_id.ok_or(CliSupportError::InvalidArguments)?),
        profile_id: Some(flags.profile_id.ok_or(CliSupportError::InvalidArguments)?),
        expected_profile_revision: Some(
            flags
                .expected_profile_revision
                .ok_or(CliSupportError::InvalidArguments)?,
        ),
        new_preference: Some(flags.pref.into_required_preference()?),
    })
}

fn parse_panel_create<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<SwarmAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut scope = RunScopeFlags::default();
    let mut request_id = None;
    let mut author_member_ids = Vec::new();
    let mut verifier_member_ids = Vec::new();
    let mut required_quorum = None;
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
            "--request-id" => set_canonical_uuid_string(&mut request_id, arguments, index)?,
            "--author-member-id" => {
                let raw = next_utf8_value(arguments, index)?;
                let _ = parse_canonical_uuid(raw)?;
                author_member_ids.push(raw.to_owned());
            }
            "--verifier-member-id" => {
                let raw = next_utf8_value(arguments, index)?;
                let _ = parse_canonical_uuid(raw)?;
                verifier_member_ids.push(raw.to_owned());
            }
            "--required-quorum" => set_once(
                &mut required_quorum,
                parse_exact(next_utf8_value(arguments, index)?)?,
            )?,
            _ => return Err(CliSupportError::InvalidArguments),
        }
    }
    let (workspace_id, swarm_run_id, request_file) = scope.require_run(env_lookup)?;
    if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(SwarmAction::PanelCreate {
            workspace_id,
            swarm_run_id,
            payload: SwarmPayload::RequestFile(path),
        });
    }
    Ok(SwarmAction::PanelCreate {
        workspace_id,
        swarm_run_id,
        payload: SwarmPayload::Inline(CreateSwarmVerificationPanelRequest {
            request_id: request_id.ok_or(CliSupportError::InvalidArguments)?,
            author_member_ids,
            verifier_member_ids,
            required_quorum: required_quorum.ok_or(CliSupportError::InvalidArguments)?,
        }),
    })
}

fn parse_verdict<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<SwarmAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut scope = RunScopeFlags::default();
    let mut request_id = None;
    let mut panel_id = None;
    let mut verifier_member_id = None;
    let mut outcome = None;
    let mut reviewed_diff_digest_hex = None;
    let mut rationale = None;
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
            "--request-id" => set_canonical_uuid_string(&mut request_id, arguments, index)?,
            "--panel-id" => set_canonical_uuid_string(&mut panel_id, arguments, index)?,
            "--verifier-member-id" => {
                set_canonical_uuid_string(&mut verifier_member_id, arguments, index)?;
            }
            "--outcome" | "--verdict" => {
                set_once(&mut outcome, next_utf8_value(arguments, index)?.to_owned())?;
            }
            "--reviewed-diff-digest" | "--diff-digest" => set_once(
                &mut reviewed_diff_digest_hex,
                next_utf8_value(arguments, index)?.to_owned(),
            )?,
            "--rationale" => set_once(
                &mut rationale,
                next_utf8_value(arguments, index)?.to_owned(),
            )?,
            _ => return Err(CliSupportError::InvalidArguments),
        }
    }
    let (workspace_id, swarm_run_id, request_file) = scope.require_run(env_lookup)?;
    if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(SwarmAction::Verdict {
            workspace_id,
            swarm_run_id,
            payload: SwarmPayload::RequestFile(path),
        });
    }
    Ok(SwarmAction::Verdict {
        workspace_id,
        swarm_run_id,
        payload: SwarmPayload::Inline(RecordSwarmVerdictRequest {
            request_id: request_id.unwrap_or_else(|| Uuid::now_v7().to_string()),
            panel_id,
            verifier_member_id: resolve_member_id_string(verifier_member_id, env_lookup)?,
            outcome: outcome.ok_or(CliSupportError::InvalidArguments)?,
            reviewed_diff_digest_hex: reviewed_diff_digest_hex
                .ok_or(CliSupportError::InvalidArguments)?,
            rationale: rationale.ok_or(CliSupportError::InvalidArguments)?,
        }),
    })
}

fn parse_converge<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<SwarmAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut scope = RunScopeFlags::default();
    let mut request_id = None;
    let mut converged_commit_oid = None;
    let mut converged_tree_blake3_hex = None;
    let mut converged_diff_digest_hex = None;
    let mut target_state = None;
    let mut stage_candidate_only = false;
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
            "--request-id" => set_canonical_uuid_string(&mut request_id, arguments, index)?,
            "--converged-commit-oid" => set_once(
                &mut converged_commit_oid,
                next_utf8_value(arguments, index)?.to_owned(),
            )?,
            "--converged-tree-blake3" => set_once(
                &mut converged_tree_blake3_hex,
                next_utf8_value(arguments, index)?.to_owned(),
            )?,
            "--converged-diff-digest" => set_once(
                &mut converged_diff_digest_hex,
                next_utf8_value(arguments, index)?.to_owned(),
            )?,
            "--target-state" => set_once(
                &mut target_state,
                next_utf8_value(arguments, index)?.to_owned(),
            )?,
            "--stage-candidate-only" if !stage_candidate_only => {
                stage_candidate_only = true;
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
        return Ok(SwarmAction::Converge {
            workspace_id,
            swarm_run_id,
            payload: SwarmPayload::RequestFile(path),
        });
    }
    Ok(SwarmAction::Converge {
        workspace_id,
        swarm_run_id,
        payload: SwarmPayload::Inline(ConvergeSwarmRequest {
            request_id: request_id.ok_or(CliSupportError::InvalidArguments)?,
            converged_commit_oid: converged_commit_oid.ok_or(CliSupportError::InvalidArguments)?,
            converged_tree_blake3_hex: converged_tree_blake3_hex
                .ok_or(CliSupportError::InvalidArguments)?,
            converged_diff_digest_hex: converged_diff_digest_hex
                .ok_or(CliSupportError::InvalidArguments)?,
            target_state,
            stage_candidate_only,
        }),
    })
}

fn parse_settle<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<SwarmAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut scope = RunScopeFlags::default();
    let mut request_id = None;
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
        if flag == "--request-id" {
            set_canonical_uuid_string(&mut request_id, arguments, index)?;
        } else {
            return Err(CliSupportError::InvalidArguments);
        }
    }
    let (workspace_id, swarm_run_id, request_file) = scope.require_run(env_lookup)?;
    if let Some(path) = request_file {
        if request_id.is_some() {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(SwarmAction::Settle {
            workspace_id,
            swarm_run_id,
            payload: SwarmPayload::RequestFile(path),
        });
    }
    Ok(SwarmAction::Settle {
        workspace_id,
        swarm_run_id,
        payload: SwarmPayload::Inline(SettleSwarmRequest {
            request_id: request_id.ok_or(CliSupportError::InvalidArguments)?,
        }),
    })
}

fn parse_abort<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<SwarmAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut scope = RunScopeFlags::default();
    while *index < arguments.len() {
        if conn.try_consume(arguments, index)? {
            continue;
        }
        let flag = arguments[*index]
            .to_str()
            .ok_or(CliSupportError::InvalidArguments)?;
        if !scope.try_consume(flag, arguments, index)? {
            return Err(CliSupportError::InvalidArguments);
        }
    }
    let (workspace_id, swarm_run_id, request_file) = scope.require_run(env_lookup)?;
    let payload = request_file.map_or(
        SwarmPayload::Inline(AbortSwarmRunRequest {}),
        SwarmPayload::RequestFile,
    );
    Ok(SwarmAction::Abort {
        workspace_id,
        swarm_run_id,
        payload,
    })
}

fn parse_stall_check<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<SwarmAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut scope = RunScopeFlags::default();
    let mut stall_grace_ms = None;
    let mut max_stall_nudges = None;
    let mut max_ping_pong_streak = None;
    let mut max_member_turns = None;
    let mut observed_at_ms = None;
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
            "--stall-grace-ms" => set_once(
                &mut stall_grace_ms,
                parse_exact(next_utf8_value(arguments, index)?)?,
            )?,
            "--max-stall-nudges" => set_once(
                &mut max_stall_nudges,
                parse_exact(next_utf8_value(arguments, index)?)?,
            )?,
            "--max-ping-pong-streak" => set_once(
                &mut max_ping_pong_streak,
                parse_exact(next_utf8_value(arguments, index)?)?,
            )?,
            "--max-member-turns" => set_once(
                &mut max_member_turns,
                parse_exact(next_utf8_value(arguments, index)?)?,
            )?,
            "--observed-at-ms" => set_once(
                &mut observed_at_ms,
                parse_exact(next_utf8_value(arguments, index)?)?,
            )?,
            _ => return Err(CliSupportError::InvalidArguments),
        }
    }
    let (workspace_id, swarm_run_id, request_file) = scope.require_run(env_lookup)?;
    if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(SwarmAction::StallCheck {
            workspace_id,
            swarm_run_id,
            payload: SwarmPayload::RequestFile(path),
        });
    }
    Ok(SwarmAction::StallCheck {
        workspace_id,
        swarm_run_id,
        payload: SwarmPayload::Inline(SwarmStallCheckRequest {
            stall_grace_ms,
            max_stall_nudges,
            max_ping_pong_streak,
            max_member_turns,
            observed_at_ms,
        }),
    })
}
