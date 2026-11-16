//! Flag and `--request <json-file>` parsers for the 9 `autonomy` CLI subcommands.

use std::ffi::OsString;

use uuid::Uuid;

use super::{AutonomyAction, AutonomyPayload};
use crate::cli_support::{
    AuthorFlagBuilder, CliSupportError, RawConnectionFlags, WORKSPACE_ID_ENV, env_canonical_uuid,
    next_path_value, next_utf8_value, parse_canonical_uuid, parse_exact, set_canonical_uuid_string,
    set_once,
};
use crate::wire::WorkspaceQueryScope;
use crate::wire::autonomy_loop::{
    AssignReviewerRequest, DagSettlementAuthorityCheckRequest, DagSettlementGovernanceQuery,
    DagSettlementParticipantWire, ExecutionPermissionsWire, ExecutionSnapshotQuery,
    ExecutionTimeoutsWire, McpGrantWire, PrepareExecutionSnapshotRequest,
    RecordReviewVerdictRequest, ReviewerCandidateWire, VerifyNodeCompletionGateRequest,
    WorkCorrelationWire, WorkspaceScopeMutationRequest,
};

pub(super) fn dispatch<F>(
    subcommand: &str,
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<AutonomyAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    match subcommand {
        "scope-get" => parse_scope_get(arguments, index, conn),
        "scope-mutate" => parse_scope_mutate(arguments, index, conn, env_lookup),
        "reviewer-assign" => parse_reviewer_assign(arguments, index, conn, env_lookup),
        "reviewer-verdict" => parse_reviewer_verdict(arguments, index, conn, env_lookup),
        "reviewer-verify-gate" => parse_reviewer_verify_gate(arguments, index, conn, env_lookup),
        "snapshot-prepare" => parse_snapshot_prepare(arguments, index, conn, env_lookup),
        "snapshot-get" => parse_snapshot_get(arguments, index, conn, env_lookup),
        "dag-settlement-authority" => {
            parse_dag_settlement_authority(arguments, index, conn, env_lookup)
        }
        "dag-settlement-governance" => {
            parse_dag_settlement_governance(arguments, index, conn, env_lookup)
        }
        _ => Err(CliSupportError::InvalidArguments),
    }
}

fn resolve_workspace_id<F>(explicit: Option<Uuid>, env_lookup: &F) -> Result<Uuid, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    match explicit {
        Some(id) => Ok(id),
        None => env_canonical_uuid(env_lookup, WORKSPACE_ID_ENV)?
            .ok_or(CliSupportError::InvalidArguments),
    }
}

fn parse_scope_get(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
) -> Result<AutonomyAction, CliSupportError> {
    let mut workspace_id = None;
    let mut request_file = None;
    let mut all = false;
    while *index < arguments.len() {
        if conn.try_consume(arguments, index)? {
            continue;
        }
        match arguments[*index].to_str() {
            Some("--workspace-id") => {
                let raw = next_utf8_value(arguments, index)?;
                set_once(&mut workspace_id, parse_canonical_uuid(raw)?)?;
            }
            Some("--request") => set_once(&mut request_file, next_path_value(arguments, index)?)?,
            Some("--all") if !all => {
                all = true;
                *index += 1;
            }
            _ => return Err(CliSupportError::InvalidArguments),
        }
    }
    if (workspace_id.is_some() && request_file.is_some())
        || (all && (workspace_id.is_some() || request_file.is_some()))
    {
        return Err(CliSupportError::InvalidArguments);
    }
    Ok(AutonomyAction::ScopeGet {
        workspace_id,
        request_file,
    })
}

#[derive(Default)]
struct ScopeMutateFlags {
    mode: Option<bool>,
    request_id: Option<String>,
    expected_revision: Option<u64>,
    workspace_id: Option<Uuid>,
    location_id: Option<Uuid>,
    location_revision: Option<i64>,
    workspace_revision: Option<i64>,
    locator: Option<String>,
}

fn parse_scope_mutate<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<AutonomyAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut request_file = None;
    let mut flags = ScopeMutateFlags::default();
    let mut saw_inline = false;
    while *index < arguments.len() {
        if conn.try_consume(arguments, index)? {
            continue;
        }
        if arguments[*index].to_str() == Some("--request") {
            set_once(&mut request_file, next_path_value(arguments, index)?)?;
            continue;
        }
        saw_inline = true;
        consume_scope_mutate_flag(&mut flags, arguments, index)?;
    }
    if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(AutonomyAction::ScopeMutate(AutonomyPayload::RequestFile(
            path,
        )));
    }
    build_inline_scope_mutation(flags, env_lookup)
        .map(AutonomyPayload::Inline)
        .map(AutonomyAction::ScopeMutate)
}

fn consume_scope_mutate_flag(
    flags: &mut ScopeMutateFlags,
    arguments: &[OsString],
    index: &mut usize,
) -> Result<(), CliSupportError> {
    match arguments[*index].to_str() {
        Some("--register") => {
            set_once(&mut flags.mode, true)?;
            *index += 1;
        }
        Some("--unregister") => {
            set_once(&mut flags.mode, false)?;
            *index += 1;
        }
        Some("--request-id") => set_canonical_uuid_string(&mut flags.request_id, arguments, index)?,
        Some("--expected-revision") => set_once(
            &mut flags.expected_revision,
            parse_exact(next_utf8_value(arguments, index)?)?,
        )?,
        Some("--workspace-id") => set_once(
            &mut flags.workspace_id,
            parse_canonical_uuid(next_utf8_value(arguments, index)?)?,
        )?,
        Some("--location-id") => set_once(
            &mut flags.location_id,
            parse_canonical_uuid(next_utf8_value(arguments, index)?)?,
        )?,
        Some("--location-revision") => set_once(
            &mut flags.location_revision,
            parse_exact(next_utf8_value(arguments, index)?)?,
        )?,
        Some("--workspace-revision") => set_once(
            &mut flags.workspace_revision,
            parse_exact(next_utf8_value(arguments, index)?)?,
        )?,
        Some("--locator") => set_once(
            &mut flags.locator,
            next_utf8_value(arguments, index)?.to_owned(),
        )?,
        _ => return Err(CliSupportError::InvalidArguments),
    }
    Ok(())
}

fn build_inline_scope_mutation<F>(
    flags: ScopeMutateFlags,
    env_lookup: &F,
) -> Result<WorkspaceScopeMutationRequest, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let is_register = flags.mode.ok_or(CliSupportError::InvalidArguments)?;
    let request_id = flags.request_id.ok_or(CliSupportError::InvalidArguments)?;
    let expected_revision = flags
        .expected_revision
        .ok_or(CliSupportError::InvalidArguments)?;
    let workspace_id = resolve_workspace_id(flags.workspace_id, env_lookup)?;
    if is_register {
        Ok(WorkspaceScopeMutationRequest::Register {
            request_id,
            expected_revision,
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
        })
    } else {
        if flags.location_id.is_some()
            || flags.location_revision.is_some()
            || flags.workspace_revision.is_some()
            || flags.locator.is_some()
        {
            return Err(CliSupportError::InvalidArguments);
        }
        Ok(WorkspaceScopeMutationRequest::Unregister {
            workspace_id: workspace_id.to_string(),
            request_id,
            expected_revision,
        })
    }
}

#[derive(Default)]
struct CorrelationFlags {
    goal_id: Option<String>,
    dag_revision: Option<i64>,
    node_id: Option<String>,
    assignment_id: Option<String>,
    attempt: Option<u32>,
}

impl CorrelationFlags {
    fn try_consume(
        &mut self,
        arguments: &[OsString],
        index: &mut usize,
    ) -> Result<bool, CliSupportError> {
        match arguments[*index].to_str() {
            Some("--goal-id") => {
                set_canonical_uuid_string(&mut self.goal_id, arguments, index)?;
                Ok(true)
            }
            Some("--dag-revision") => {
                set_once(
                    &mut self.dag_revision,
                    parse_exact(next_utf8_value(arguments, index)?)?,
                )?;
                Ok(true)
            }
            Some("--node-id") => {
                set_canonical_uuid_string(&mut self.node_id, arguments, index)?;
                Ok(true)
            }
            Some("--assignment-id") => {
                set_canonical_uuid_string(&mut self.assignment_id, arguments, index)?;
                Ok(true)
            }
            Some("--attempt") => {
                set_once(
                    &mut self.attempt,
                    parse_exact(next_utf8_value(arguments, index)?)?,
                )?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn build(self) -> Result<WorkCorrelationWire, CliSupportError> {
        Ok(WorkCorrelationWire {
            goal_id: self.goal_id.ok_or(CliSupportError::InvalidArguments)?,
            dag_revision: self.dag_revision.ok_or(CliSupportError::InvalidArguments)?,
            node_id: self.node_id.ok_or(CliSupportError::InvalidArguments)?,
            assignment_id: self
                .assignment_id
                .ok_or(CliSupportError::InvalidArguments)?,
            attempt: self.attempt.ok_or(CliSupportError::InvalidArguments)?,
        })
    }
}

fn parse_reviewer_assign<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<AutonomyAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut workspace_id = None;
    let mut request_file = None;
    let mut request_id = None;
    let mut required_domain = None;
    let mut corr = CorrelationFlags::default();
    let mut executor = AuthorFlagBuilder::default();
    let mut leader = AuthorFlagBuilder::default();
    let mut candidates = Vec::new();
    let mut saw_inline = false;

    while *index < arguments.len() {
        if conn.try_consume(arguments, index)? {
            continue;
        }
        let flag = arguments[*index]
            .to_str()
            .ok_or(CliSupportError::InvalidArguments)?;
        if flag == "--workspace-id" {
            set_once(
                &mut workspace_id,
                parse_canonical_uuid(next_utf8_value(arguments, index)?)?,
            )?;
            continue;
        }
        if flag == "--request" {
            set_once(&mut request_file, next_path_value(arguments, index)?)?;
            continue;
        }
        saw_inline = true;
        if corr.try_consume(arguments, index)?
            || executor.try_consume_prefixed("--executor", flag, arguments, index)?
            || leader.try_consume_prefixed("--leader", flag, arguments, index)?
        {
            continue;
        }
        match flag {
            "--request-id" => set_canonical_uuid_string(&mut request_id, arguments, index)?,
            "--required-domain" => set_once(
                &mut required_domain,
                next_utf8_value(arguments, index)?.to_owned(),
            )?,
            "--candidate-json" => {
                let raw = next_utf8_value(arguments, index)?;
                let parsed: ReviewerCandidateWire =
                    serde_json::from_str(raw).map_err(|_| CliSupportError::InvalidRequestJson)?;
                candidates.push(parsed);
            }
            _ => return Err(CliSupportError::InvalidArguments),
        }
    }
    let workspace_id = resolve_workspace_id(workspace_id, env_lookup)?;
    if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(AutonomyAction::ReviewerAssign {
            workspace_id,
            payload: AutonomyPayload::RequestFile(path),
        });
    }
    Ok(AutonomyAction::ReviewerAssign {
        workspace_id,
        payload: AutonomyPayload::Inline(AssignReviewerRequest {
            request_id: request_id.ok_or(CliSupportError::InvalidArguments)?,
            correlation: corr.build()?,
            required_domain: required_domain.ok_or(CliSupportError::InvalidArguments)?,
            executor: executor.build()?,
            assigning_leader: leader.build()?,
            candidates,
        }),
    })
}

fn parse_reviewer_verdict<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<AutonomyAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut workspace_id = None;
    let mut request_file = None;
    let mut request_id = None;
    let mut corr = CorrelationFlags::default();
    let mut expected_revision = None;
    let mut reviewer_agent_id = None;
    let mut reviewer_session_id = None;
    let mut verdict = None;
    let mut handoff_diff_digest_hex = None;
    let mut saw_inline = false;

    while *index < arguments.len() {
        if conn.try_consume(arguments, index)? {
            continue;
        }
        let flag = arguments[*index]
            .to_str()
            .ok_or(CliSupportError::InvalidArguments)?;
        if flag == "--workspace-id" {
            set_once(
                &mut workspace_id,
                parse_canonical_uuid(next_utf8_value(arguments, index)?)?,
            )?;
            continue;
        }
        if flag == "--request" {
            set_once(&mut request_file, next_path_value(arguments, index)?)?;
            continue;
        }
        saw_inline = true;
        if corr.try_consume(arguments, index)? {
            continue;
        }
        match flag {
            "--request-id" => set_canonical_uuid_string(&mut request_id, arguments, index)?,
            "--expected-revision" => set_once(
                &mut expected_revision,
                parse_exact(next_utf8_value(arguments, index)?)?,
            )?,
            "--reviewer-agent-id" => {
                set_canonical_uuid_string(&mut reviewer_agent_id, arguments, index)?;
            }
            "--reviewer-session-id" => {
                set_canonical_uuid_string(&mut reviewer_session_id, arguments, index)?;
            }
            "--verdict" => set_once(&mut verdict, next_utf8_value(arguments, index)?.to_owned())?,
            "--handoff-diff-digest" => set_once(
                &mut handoff_diff_digest_hex,
                next_utf8_value(arguments, index)?.to_owned(),
            )?,
            _ => return Err(CliSupportError::InvalidArguments),
        }
    }
    let workspace_id = resolve_workspace_id(workspace_id, env_lookup)?;
    if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(AutonomyAction::ReviewerVerdict {
            workspace_id,
            payload: AutonomyPayload::RequestFile(path),
        });
    }
    Ok(AutonomyAction::ReviewerVerdict {
        workspace_id,
        payload: AutonomyPayload::Inline(RecordReviewVerdictRequest {
            request_id: request_id.ok_or(CliSupportError::InvalidArguments)?,
            correlation: corr.build()?,
            expected_revision: expected_revision.ok_or(CliSupportError::InvalidArguments)?,
            reviewer_agent_id: reviewer_agent_id.ok_or(CliSupportError::InvalidArguments)?,
            reviewer_session_id: reviewer_session_id.ok_or(CliSupportError::InvalidArguments)?,
            reviewer: None,
            verdict: verdict.ok_or(CliSupportError::InvalidArguments)?,
            handoff_diff_digest_hex: handoff_diff_digest_hex
                .ok_or(CliSupportError::InvalidArguments)?,
        }),
    })
}

fn parse_reviewer_verify_gate<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<AutonomyAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut workspace_id = None;
    let mut request_file = None;
    let mut corr = CorrelationFlags::default();
    let mut digest_hex = None;
    let mut saw_inline = false;

    while *index < arguments.len() {
        if conn.try_consume(arguments, index)? {
            continue;
        }
        let flag = arguments[*index]
            .to_str()
            .ok_or(CliSupportError::InvalidArguments)?;
        if flag == "--workspace-id" {
            set_once(
                &mut workspace_id,
                parse_canonical_uuid(next_utf8_value(arguments, index)?)?,
            )?;
            continue;
        }
        if flag == "--request" {
            set_once(&mut request_file, next_path_value(arguments, index)?)?;
            continue;
        }
        saw_inline = true;
        if corr.try_consume(arguments, index)? {
            continue;
        }
        if flag == "--current-handoff-diff-digest" {
            set_once(
                &mut digest_hex,
                next_utf8_value(arguments, index)?.to_owned(),
            )?;
        } else {
            return Err(CliSupportError::InvalidArguments);
        }
    }
    let workspace_id = resolve_workspace_id(workspace_id, env_lookup)?;
    if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(AutonomyAction::ReviewerVerifyGate {
            workspace_id,
            payload: AutonomyPayload::RequestFile(path),
        });
    }
    Ok(AutonomyAction::ReviewerVerifyGate {
        workspace_id,
        payload: AutonomyPayload::Inline(VerifyNodeCompletionGateRequest {
            correlation: corr.build()?,
            current_handoff_diff_digest_hex: digest_hex.ok_or(CliSupportError::InvalidArguments)?,
        }),
    })
}

#[derive(Default)]
struct SnapshotPrepareFlags {
    request_id: Option<String>,
    snapshot_id: Option<String>,
    corr: CorrelationFlags,
    activity_id: Option<String>,
    profile_id: Option<String>,
    profile_revision: Option<i64>,
    resolved_engine: Option<String>,
    resolved_model: Option<String>,
    resolved_reasoning: Option<String>,
    role_prompt_digest_hex: Option<String>,
    worktree_path: Option<String>,
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
}

fn parse_snapshot_prepare<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<AutonomyAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut workspace_id = None;
    let mut request_file = None;
    let mut flags = SnapshotPrepareFlags::default();
    let mut saw_inline = false;

    while *index < arguments.len() {
        if conn.try_consume(arguments, index)? {
            continue;
        }
        let flag = arguments[*index]
            .to_str()
            .ok_or(CliSupportError::InvalidArguments)?;
        if flag == "--workspace-id" {
            set_once(
                &mut workspace_id,
                parse_canonical_uuid(next_utf8_value(arguments, index)?)?,
            )?;
            continue;
        }
        if flag == "--request" {
            set_once(&mut request_file, next_path_value(arguments, index)?)?;
            continue;
        }
        saw_inline = true;
        consume_snapshot_prepare_flag(&mut flags, flag, arguments, index)?;
    }
    let workspace_id = resolve_workspace_id(workspace_id, env_lookup)?;
    if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(AutonomyAction::SnapshotPrepare {
            workspace_id,
            payload: AutonomyPayload::RequestFile(path),
        });
    }
    Ok(AutonomyAction::SnapshotPrepare {
        workspace_id,
        payload: AutonomyPayload::Inline(build_snapshot_prepare_request(flags)?),
    })
}

fn consume_snapshot_prepare_flag(
    flags: &mut SnapshotPrepareFlags,
    flag: &str,
    arguments: &[OsString],
    index: &mut usize,
) -> Result<(), CliSupportError> {
    if flags.corr.try_consume(arguments, index)? {
        return Ok(());
    }
    match flag {
        "--request-id" => set_canonical_uuid_string(&mut flags.request_id, arguments, index),
        "--snapshot-id" => set_canonical_uuid_string(&mut flags.snapshot_id, arguments, index),
        "--activity-id" => set_canonical_uuid_string(&mut flags.activity_id, arguments, index),
        "--profile-id" => set_canonical_uuid_string(&mut flags.profile_id, arguments, index),
        "--profile-revision" => set_once(
            &mut flags.profile_revision,
            parse_exact(next_utf8_value(arguments, index)?)?,
        ),
        "--resolved-engine" => set_once(
            &mut flags.resolved_engine,
            next_utf8_value(arguments, index)?.to_owned(),
        ),
        "--resolved-model" => set_once(
            &mut flags.resolved_model,
            next_utf8_value(arguments, index)?.to_owned(),
        ),
        "--resolved-reasoning" => set_once(
            &mut flags.resolved_reasoning,
            next_utf8_value(arguments, index)?.to_owned(),
        ),
        "--role-prompt-digest" => set_once(
            &mut flags.role_prompt_digest_hex,
            next_utf8_value(arguments, index)?.to_owned(),
        ),
        "--worktree-path" => set_once(
            &mut flags.worktree_path,
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
        _ => consume_snapshot_grant_and_timeout_flag(flags, flag, arguments, index),
    }
}

fn consume_snapshot_grant_and_timeout_flag(
    flags: &mut SnapshotPrepareFlags,
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
        _ => Err(CliSupportError::InvalidArguments),
    }
}

fn build_snapshot_prepare_request(
    flags: SnapshotPrepareFlags,
) -> Result<PrepareExecutionSnapshotRequest, CliSupportError> {
    let corr = flags.corr.build()?;
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
    Ok(PrepareExecutionSnapshotRequest {
        request_id: flags.request_id.ok_or(CliSupportError::InvalidArguments)?,
        snapshot_id: flags.snapshot_id.ok_or(CliSupportError::InvalidArguments)?,
        goal_id: corr.goal_id,
        dag_revision: corr.dag_revision,
        node_id: corr.node_id,
        assignment_id: corr.assignment_id,
        attempt: corr.attempt,
        activity_id: flags.activity_id.ok_or(CliSupportError::InvalidArguments)?,
        profile_id: flags.profile_id.ok_or(CliSupportError::InvalidArguments)?,
        profile_revision: flags
            .profile_revision
            .ok_or(CliSupportError::InvalidArguments)?,
        resolved_engine: flags
            .resolved_engine
            .ok_or(CliSupportError::InvalidArguments)?,
        resolved_model: flags
            .resolved_model
            .ok_or(CliSupportError::InvalidArguments)?,
        resolved_reasoning: flags.resolved_reasoning,
        role_prompt_digest_hex: flags.role_prompt_digest_hex,
        worktree_path: flags
            .worktree_path
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
    })
}

fn parse_snapshot_get<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<AutonomyAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut workspace_id = None;
    let mut request_file = None;
    let mut corr = CorrelationFlags::default();
    let mut saw_inline = false;

    while *index < arguments.len() {
        if conn.try_consume(arguments, index)? {
            continue;
        }
        let flag = arguments[*index]
            .to_str()
            .ok_or(CliSupportError::InvalidArguments)?;
        if flag == "--workspace-id" {
            set_once(
                &mut workspace_id,
                parse_canonical_uuid(next_utf8_value(arguments, index)?)?,
            )?;
            continue;
        }
        if flag == "--request" {
            set_once(&mut request_file, next_path_value(arguments, index)?)?;
            continue;
        }
        saw_inline = true;
        if !corr.try_consume(arguments, index)? {
            return Err(CliSupportError::InvalidArguments);
        }
    }
    let workspace_id = resolve_workspace_id(workspace_id, env_lookup)?;
    if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(AutonomyAction::SnapshotGet {
            workspace_id,
            payload: AutonomyPayload::RequestFile(path),
        });
    }
    let built = corr.build()?;
    Ok(AutonomyAction::SnapshotGet {
        workspace_id,
        payload: AutonomyPayload::Inline(ExecutionSnapshotQuery {
            goal_id: built.goal_id,
            dag_revision: built.dag_revision,
            node_id: built.node_id,
            assignment_id: built.assignment_id,
            attempt: built.attempt,
        }),
    })
}

fn parse_dag_settlement_authority<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<AutonomyAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut workspace_id = None;
    let mut request_file = None;
    let mut expected_leadership_revision = None;
    let mut activity_id = None;
    let mut agent_id = None;
    let mut session_id = None;
    let mut saw_inline = false;

    while *index < arguments.len() {
        if conn.try_consume(arguments, index)? {
            continue;
        }
        let flag = arguments[*index]
            .to_str()
            .ok_or(CliSupportError::InvalidArguments)?;
        if flag == "--workspace-id" {
            set_once(
                &mut workspace_id,
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
            "--expected-leadership-revision" => set_once(
                &mut expected_leadership_revision,
                parse_exact(next_utf8_value(arguments, index)?)?,
            )?,
            "--activity-id" => set_canonical_uuid_string(&mut activity_id, arguments, index)?,
            "--agent-id" => set_canonical_uuid_string(&mut agent_id, arguments, index)?,
            "--session-id" => set_canonical_uuid_string(&mut session_id, arguments, index)?,
            _ => return Err(CliSupportError::InvalidArguments),
        }
    }
    let workspace_id = resolve_workspace_id(workspace_id, env_lookup)?;
    if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(AutonomyAction::DagSettlementAuthority {
            workspace_id,
            payload: AutonomyPayload::RequestFile(path),
        });
    }
    let participant = match (activity_id, agent_id, session_id) {
        (None, None, None) => None,
        (Some(activity_id), Some(agent_id), Some(session_id)) => {
            Some(DagSettlementParticipantWire {
                activity_id,
                agent_id,
                session_id,
            })
        }
        _ => return Err(CliSupportError::InvalidArguments),
    };
    let rev = expected_leadership_revision.ok_or(CliSupportError::InvalidArguments)?;
    if rev < 0 {
        return Err(CliSupportError::InvalidArguments);
    }
    Ok(AutonomyAction::DagSettlementAuthority {
        workspace_id,
        payload: AutonomyPayload::Inline(DagSettlementAuthorityCheckRequest {
            expected_leadership_revision: rev,
            participant,
        }),
    })
}

fn parse_dag_settlement_governance<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<AutonomyAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut workspace_id = None;
    let mut request_file = None;
    let mut goal_id = None;
    let mut dag_revision = None;
    let mut saw_inline = false;

    while *index < arguments.len() {
        if conn.try_consume(arguments, index)? {
            continue;
        }
        let flag = arguments[*index]
            .to_str()
            .ok_or(CliSupportError::InvalidArguments)?;
        if flag == "--workspace-id" {
            set_once(
                &mut workspace_id,
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
            "--goal-id" => set_canonical_uuid_string(&mut goal_id, arguments, index)?,
            "--dag-revision" => set_once(
                &mut dag_revision,
                parse_exact(next_utf8_value(arguments, index)?)?,
            )?,
            _ => return Err(CliSupportError::InvalidArguments),
        }
    }
    let workspace_id = resolve_workspace_id(workspace_id, env_lookup)?;
    if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(AutonomyAction::DagSettlementGovernance {
            workspace_id,
            payload: AutonomyPayload::RequestFile(path),
        });
    }
    let rev: i64 = dag_revision.ok_or(CliSupportError::InvalidArguments)?;
    if rev < 1 {
        return Err(CliSupportError::InvalidArguments);
    }
    Ok(AutonomyAction::DagSettlementGovernance {
        workspace_id,
        payload: AutonomyPayload::Inline(DagSettlementGovernanceQuery {
            goal_id: goal_id.ok_or(CliSupportError::InvalidArguments)?,
            dag_revision: rev,
        }),
    })
}
