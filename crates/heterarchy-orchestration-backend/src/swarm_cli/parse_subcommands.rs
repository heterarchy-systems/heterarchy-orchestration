//! Flag and `--request <json-file>` parsers for Swarm run, roster, channel, and
//! board CLI subcommands, including spawned-worker `HETERARCHY_*` environment fallbacks.

use std::ffi::OsString;
use std::path::PathBuf;

use uuid::Uuid;

use super::{SwarmAction, SwarmPayload, parse_turns};
use crate::cli_support::{
    AuthorFlagBuilder, CliSupportError, RawConnectionFlags, SWARM_MEMBER_ID_ENV, SWARM_RUN_ID_ENV,
    WORKSPACE_ID_ENV, env_canonical_uuid, next_path_value, next_utf8_value, parse_canonical_uuid,
    parse_exact, set_canonical_uuid_string, set_once,
};
use crate::wire::swarm::{
    AddSwarmMemberRequest, CreateSwarmChannelRequest, CreateSwarmRunRequest,
    DagNodeHybridTargetWire, KillSwarmMemberRequest, ListSwarmRunsQuery, PostSwarmMessageRequest,
    ReadSwarmMessagesQuery, SubscribeSwarmChannelRequest, SwarmRunModeWire,
};

pub(super) fn dispatch<F>(
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
        "create" => parse_create(arguments, index, conn, env_lookup),
        "list" => parse_list(arguments, index, conn, env_lookup),
        "status" => parse_status(arguments, index, conn, env_lookup),
        "channel-create" => parse_channel_create(arguments, index, conn, env_lookup),
        "subscribe" => parse_subscribe(arguments, index, conn, env_lookup),
        "member-add" => parse_member_add(arguments, index, conn, env_lookup),
        "member-kill" => parse_member_kill(arguments, index, conn, env_lookup),
        "post" => parse_post(arguments, index, conn, env_lookup),
        "read" => parse_read(arguments, index, conn, env_lookup),
        other => {
            parse_turns::dispatch_turn_or_governance(other, arguments, index, conn, env_lookup)
        }
    }
}

pub(super) fn resolve_workspace_id<F>(
    explicit: Option<Uuid>,
    env_lookup: &F,
) -> Result<Uuid, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    match explicit {
        Some(id) => Ok(id),
        None => env_canonical_uuid(env_lookup, WORKSPACE_ID_ENV)?
            .ok_or(CliSupportError::InvalidArguments),
    }
}

pub(super) fn resolve_member_id<F>(
    explicit: Option<Uuid>,
    env_lookup: &F,
) -> Result<Uuid, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    match explicit {
        Some(id) => Ok(id),
        None => env_canonical_uuid(env_lookup, SWARM_MEMBER_ID_ENV)?
            .ok_or(CliSupportError::InvalidArguments),
    }
}

pub(super) fn resolve_member_id_string<F>(
    explicit: Option<String>,
    env_lookup: &F,
) -> Result<String, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    match explicit {
        Some(id) => Ok(id),
        None => env_canonical_uuid(env_lookup, SWARM_MEMBER_ID_ENV)?
            .map(|id| id.to_string())
            .ok_or(CliSupportError::InvalidArguments),
    }
}

#[derive(Default)]
pub(super) struct RunScopeFlags {
    pub(super) workspace_id: Option<Uuid>,
    pub(super) swarm_run_id: Option<Uuid>,
    pub(super) request_file: Option<PathBuf>,
}

impl RunScopeFlags {
    pub(super) fn try_consume(
        &mut self,
        flag: &str,
        arguments: &[OsString],
        index: &mut usize,
    ) -> Result<bool, CliSupportError> {
        match flag {
            "--workspace-id" => {
                set_once(
                    &mut self.workspace_id,
                    parse_canonical_uuid(next_utf8_value(arguments, index)?)?,
                )?;
                Ok(true)
            }
            "--swarm-run-id" => {
                set_once(
                    &mut self.swarm_run_id,
                    parse_canonical_uuid(next_utf8_value(arguments, index)?)?,
                )?;
                Ok(true)
            }
            "--request" => {
                set_once(&mut self.request_file, next_path_value(arguments, index)?)?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    pub(super) fn require_run<F>(
        self,
        env_lookup: &F,
    ) -> Result<(Uuid, Uuid, Option<PathBuf>), CliSupportError>
    where
        F: Fn(&str) -> Option<OsString>,
    {
        let workspace_id = resolve_workspace_id(self.workspace_id, env_lookup)?;
        let swarm_run_id = match self.swarm_run_id {
            Some(id) => id,
            None => env_canonical_uuid(env_lookup, SWARM_RUN_ID_ENV)?
                .ok_or(CliSupportError::InvalidArguments)?,
        };
        Ok((workspace_id, swarm_run_id, self.request_file))
    }
}

#[derive(Default)]
struct CreateFlags {
    request_id: Option<String>,
    title: Option<String>,
    mode_kind: Option<String>,
    goal_id: Option<String>,
    dag_revision: Option<i64>,
    node_id: Option<String>,
    manifest_blake3_hex: Option<String>,
    max_members: Option<u32>,
    max_spawn_depth: Option<u8>,
    max_total_turns: Option<u64>,
    required_verifier_quorum: Option<u32>,
    allow_worker_all_mention: bool,
    baseline_commit_oid: Option<String>,
    baseline_tree_blake3_hex: Option<String>,
}

fn parse_create<F>(
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
    let mut flags = CreateFlags::default();
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
        consume_create_flag(&mut flags, flag, arguments, index)?;
    }
    let workspace_id = resolve_workspace_id(workspace_id, env_lookup)?;
    if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(SwarmAction::Create {
            workspace_id,
            payload: SwarmPayload::RequestFile(path),
        });
    }
    Ok(SwarmAction::Create {
        workspace_id,
        payload: SwarmPayload::Inline(build_create_request(flags)?),
    })
}

fn consume_create_flag(
    flags: &mut CreateFlags,
    flag: &str,
    arguments: &[OsString],
    index: &mut usize,
) -> Result<(), CliSupportError> {
    match flag {
        "--request-id" => set_canonical_uuid_string(&mut flags.request_id, arguments, index),
        "--title" => set_once(
            &mut flags.title,
            next_utf8_value(arguments, index)?.to_owned(),
        ),
        "--mode" => set_once(
            &mut flags.mode_kind,
            next_utf8_value(arguments, index)?.to_owned(),
        ),
        "--goal-id" => set_canonical_uuid_string(&mut flags.goal_id, arguments, index),
        "--dag-revision" => set_once(
            &mut flags.dag_revision,
            parse_exact(next_utf8_value(arguments, index)?)?,
        ),
        "--node-id" => set_canonical_uuid_string(&mut flags.node_id, arguments, index),
        "--declared-write-manifest-blake3" => set_once(
            &mut flags.manifest_blake3_hex,
            next_utf8_value(arguments, index)?.to_owned(),
        ),
        "--max-members" => set_once(
            &mut flags.max_members,
            parse_exact(next_utf8_value(arguments, index)?)?,
        ),
        "--max-spawn-depth" => set_once(
            &mut flags.max_spawn_depth,
            parse_exact(next_utf8_value(arguments, index)?)?,
        ),
        "--max-total-turns" => set_once(
            &mut flags.max_total_turns,
            parse_exact(next_utf8_value(arguments, index)?)?,
        ),
        "--required-verifier-quorum" => set_once(
            &mut flags.required_verifier_quorum,
            parse_exact(next_utf8_value(arguments, index)?)?,
        ),
        "--allow-worker-all-mention" if !flags.allow_worker_all_mention => {
            flags.allow_worker_all_mention = true;
            *index += 1;
            Ok(())
        }
        "--baseline-commit-oid" => set_once(
            &mut flags.baseline_commit_oid,
            next_utf8_value(arguments, index)?.to_owned(),
        ),
        "--baseline-tree-blake3" => set_once(
            &mut flags.baseline_tree_blake3_hex,
            next_utf8_value(arguments, index)?.to_owned(),
        ),
        _ => Err(CliSupportError::InvalidArguments),
    }
}

fn build_create_request(flags: CreateFlags) -> Result<CreateSwarmRunRequest, CliSupportError> {
    let kind = flags.mode_kind.unwrap_or_else(|| "STANDALONE".to_owned());
    let hybrid_target = match (
        flags.goal_id,
        flags.dag_revision,
        flags.node_id,
        flags.manifest_blake3_hex,
    ) {
        (None, None, None, None) => None,
        (
            Some(goal_id),
            Some(dag_revision),
            Some(node_id),
            Some(declared_write_manifest_blake3_hex),
        ) => Some(DagNodeHybridTargetWire {
            goal_id,
            dag_revision,
            node_id,
            declared_write_manifest_blake3_hex,
        }),
        _ => return Err(CliSupportError::InvalidArguments),
    };
    Ok(CreateSwarmRunRequest {
        request_id: flags.request_id.ok_or(CliSupportError::InvalidArguments)?,
        title: flags.title.ok_or(CliSupportError::InvalidArguments)?,
        mode: SwarmRunModeWire {
            kind,
            hybrid_target,
        },
        max_members: flags.max_members.ok_or(CliSupportError::InvalidArguments)?,
        max_spawn_depth: flags
            .max_spawn_depth
            .ok_or(CliSupportError::InvalidArguments)?,
        max_total_turns: flags
            .max_total_turns
            .ok_or(CliSupportError::InvalidArguments)?,
        required_verifier_quorum: flags
            .required_verifier_quorum
            .ok_or(CliSupportError::InvalidArguments)?,
        allow_worker_all_mention: flags.allow_worker_all_mention,
        baseline_commit_oid: flags
            .baseline_commit_oid
            .ok_or(CliSupportError::InvalidArguments)?,
        baseline_tree_blake3_hex: flags
            .baseline_tree_blake3_hex
            .ok_or(CliSupportError::InvalidArguments)?,
        scope: None,
    })
}

fn parse_list<F>(
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
    let mut limit = None;
    while *index < arguments.len() {
        if conn.try_consume(arguments, index)? {
            continue;
        }
        match arguments[*index].to_str() {
            Some("--workspace-id") => set_once(
                &mut workspace_id,
                parse_canonical_uuid(next_utf8_value(arguments, index)?)?,
            )?,
            Some("--request") => set_once(&mut request_file, next_path_value(arguments, index)?)?,
            Some("--limit") => {
                set_once(&mut limit, parse_exact(next_utf8_value(arguments, index)?)?)?;
            }
            _ => return Err(CliSupportError::InvalidArguments),
        }
    }
    let workspace_id = resolve_workspace_id(workspace_id, env_lookup)?;
    if let Some(path) = request_file {
        if limit.is_some() {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(SwarmAction::List {
            workspace_id,
            payload: SwarmPayload::RequestFile(path),
        });
    }
    Ok(SwarmAction::List {
        workspace_id,
        payload: SwarmPayload::Inline(ListSwarmRunsQuery { limit }),
    })
}

fn parse_status<F>(
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
    let workspace_id = resolve_workspace_id(scope.workspace_id, env_lookup)?;
    if scope.swarm_run_id.is_some() && scope.request_file.is_some() {
        return Err(CliSupportError::InvalidArguments);
    }
    let swarm_run_id = match (scope.swarm_run_id, &scope.request_file) {
        (Some(id), None) => Some(id),
        (None, Some(_)) => None,
        (None, None) => Some(
            env_canonical_uuid(env_lookup, SWARM_RUN_ID_ENV)?
                .ok_or(CliSupportError::InvalidArguments)?,
        ),
        (Some(_), Some(_)) => return Err(CliSupportError::InvalidArguments),
    };
    Ok(SwarmAction::Status {
        workspace_id,
        swarm_run_id,
        request_file: scope.request_file,
    })
}

fn parse_channel_create<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<SwarmAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut scope = RunScopeFlags::default();
    let mut channel_id = None;
    let mut name = None;
    let mut purpose = None;
    let mut created_by = None;
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
            "--channel-id" => set_canonical_uuid_string(&mut channel_id, arguments, index)?,
            "--name" => set_once(&mut name, next_utf8_value(arguments, index)?.to_owned())?,
            "--purpose" => set_once(&mut purpose, next_utf8_value(arguments, index)?.to_owned())?,
            "--created-by-member-id" => {
                set_canonical_uuid_string(&mut created_by, arguments, index)?;
            }
            _ => return Err(CliSupportError::InvalidArguments),
        }
    }
    let (workspace_id, swarm_run_id, request_file) = scope.require_run(env_lookup)?;
    if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(SwarmAction::ChannelCreate {
            workspace_id,
            swarm_run_id,
            payload: SwarmPayload::RequestFile(path),
        });
    }
    let created_by_member_id = match created_by {
        Some(explicit) => Some(explicit),
        None => env_canonical_uuid(env_lookup, SWARM_MEMBER_ID_ENV)?.map(|id| id.to_string()),
    };
    Ok(SwarmAction::ChannelCreate {
        workspace_id,
        swarm_run_id,
        payload: SwarmPayload::Inline(CreateSwarmChannelRequest {
            channel_id: channel_id.unwrap_or_else(|| Uuid::now_v7().to_string()),
            name: name.ok_or(CliSupportError::InvalidArguments)?,
            purpose,
            created_by_member_id,
        }),
    })
}

fn parse_subscribe<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<SwarmAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut scope = RunScopeFlags::default();
    let mut channel_id = None;
    let mut member_id = None;

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
            "--channel-id" => set_once(
                &mut channel_id,
                parse_canonical_uuid(next_utf8_value(arguments, index)?)?,
            )?,
            "--member-id" => set_canonical_uuid_string(&mut member_id, arguments, index)?,
            _ => return Err(CliSupportError::InvalidArguments),
        }
    }
    let (workspace_id, swarm_run_id, request_file) = scope.require_run(env_lookup)?;
    let channel_id = channel_id.ok_or(CliSupportError::InvalidArguments)?;
    if let Some(path) = request_file {
        if member_id.is_some() {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(SwarmAction::Subscribe {
            workspace_id,
            swarm_run_id,
            channel_id,
            payload: SwarmPayload::RequestFile(path),
        });
    }
    Ok(SwarmAction::Subscribe {
        workspace_id,
        swarm_run_id,
        channel_id,
        payload: SwarmPayload::Inline(SubscribeSwarmChannelRequest {
            member_id: resolve_member_id_string(member_id, env_lookup)?,
        }),
    })
}

#[derive(Default)]
struct MemberAddFlags {
    member_id: Option<String>,
    roster_name: Option<String>,
    role_kind: Option<String>,
    parent_member_id: Option<String>,
    workspace_mode: Option<String>,
    worktree_path: Option<String>,
    author: AuthorFlagBuilder,
    initial_channels: Vec<String>,
}

fn parse_member_add<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<SwarmAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut scope = RunScopeFlags::default();
    let mut flags = MemberAddFlags::default();
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
        consume_member_add_flag(&mut flags, flag, arguments, index)?;
    }
    let (workspace_id, swarm_run_id, request_file) = scope.require_run(env_lookup)?;
    if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(SwarmAction::MemberAdd {
            workspace_id,
            swarm_run_id,
            payload: SwarmPayload::RequestFile(path),
        });
    }
    let role_kind = flags.role_kind.ok_or(CliSupportError::InvalidArguments)?;
    let parent_member_id = match flags.parent_member_id {
        Some(explicit) => Some(explicit),
        None if !matches!(role_kind.as_str(), "coordinator" | "COORDINATOR") => {
            env_canonical_uuid(env_lookup, SWARM_MEMBER_ID_ENV)?.map(|id| id.to_string())
        }
        None => None,
    };
    Ok(SwarmAction::MemberAdd {
        workspace_id,
        swarm_run_id,
        payload: SwarmPayload::Inline(AddSwarmMemberRequest {
            member_id: flags.member_id.ok_or(CliSupportError::InvalidArguments)?,
            roster_name: flags.roster_name.ok_or(CliSupportError::InvalidArguments)?,
            role_kind,
            parent_member_id,
            workspace_mode: flags
                .workspace_mode
                .ok_or(CliSupportError::InvalidArguments)?,
            worktree_path: flags.worktree_path,
            identity: flags.author.build()?,
            initial_channels: flags.initial_channels,
        }),
    })
}

fn consume_member_add_flag(
    flags: &mut MemberAddFlags,
    flag: &str,
    arguments: &[OsString],
    index: &mut usize,
) -> Result<(), CliSupportError> {
    if flags
        .author
        .try_consume_prefixed("-", flag, arguments, index)?
    {
        return Ok(());
    }
    match flag {
        "--member-id" => set_canonical_uuid_string(&mut flags.member_id, arguments, index),
        "--roster-name" => set_once(
            &mut flags.roster_name,
            next_utf8_value(arguments, index)?.to_owned(),
        ),
        "--role-kind" => set_once(
            &mut flags.role_kind,
            next_utf8_value(arguments, index)?.to_owned(),
        ),
        "--parent-member-id" => {
            set_canonical_uuid_string(&mut flags.parent_member_id, arguments, index)
        }
        "--workspace-mode" => set_once(
            &mut flags.workspace_mode,
            next_utf8_value(arguments, index)?.to_owned(),
        ),
        "--worktree-path" => set_once(
            &mut flags.worktree_path,
            next_utf8_value(arguments, index)?.to_owned(),
        ),
        "--channel" => {
            flags
                .initial_channels
                .push(next_utf8_value(arguments, index)?.to_owned());
            Ok(())
        }
        _ => Err(CliSupportError::InvalidArguments),
    }
}

fn parse_member_kill<F>(
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
        } else {
            return Err(CliSupportError::InvalidArguments);
        }
    }
    let (workspace_id, swarm_run_id, request_file) = scope.require_run(env_lookup)?;
    let member_id = resolve_member_id(member_id, env_lookup)?;
    let payload = request_file.map_or(
        SwarmPayload::Inline(KillSwarmMemberRequest {}),
        SwarmPayload::RequestFile,
    );
    Ok(SwarmAction::MemberKill {
        workspace_id,
        swarm_run_id,
        member_id,
        payload,
    })
}

fn parse_post<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<SwarmAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut scope = RunScopeFlags::default();
    let mut message_id = None;
    let mut channel_id = None;
    let mut channel_name = None;
    let mut author_member_id = None;
    let mut system_author = false;
    let mut body = None;
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
            "--message-id" => set_canonical_uuid_string(&mut message_id, arguments, index)?,
            "--channel-id" => set_canonical_uuid_string(&mut channel_id, arguments, index)?,
            "--channel" => set_once(
                &mut channel_name,
                next_utf8_value(arguments, index)?.to_owned(),
            )?,
            "--author-member-id" => {
                set_canonical_uuid_string(&mut author_member_id, arguments, index)?;
            }
            "--system" if !system_author => {
                system_author = true;
                *index += 1;
            }
            "--body" => set_once(&mut body, next_utf8_value(arguments, index)?.to_owned())?,
            _ => return Err(CliSupportError::InvalidArguments),
        }
    }
    let (workspace_id, swarm_run_id, request_file) = scope.require_run(env_lookup)?;
    if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(SwarmAction::Post {
            workspace_id,
            swarm_run_id,
            channel_name: None,
            payload: SwarmPayload::RequestFile(path),
        });
    }
    if (channel_id.is_some() && channel_name.is_some())
        || (system_author && author_member_id.is_some())
    {
        return Err(CliSupportError::InvalidArguments);
    }
    let resolved_author = if system_author {
        None
    } else if let Some(explicit) = author_member_id {
        Some(explicit)
    } else {
        env_canonical_uuid(env_lookup, SWARM_MEMBER_ID_ENV)?.map(|id| id.to_string())
    };
    Ok(SwarmAction::Post {
        workspace_id,
        swarm_run_id,
        channel_name,
        payload: SwarmPayload::Inline(PostSwarmMessageRequest {
            message_id: message_id.unwrap_or_else(|| Uuid::now_v7().to_string()),
            channel_id,
            author_member_id: resolved_author,
            body: body.ok_or(CliSupportError::InvalidArguments)?,
        }),
    })
}

fn parse_read<F>(
    arguments: &[OsString],
    index: &mut usize,
    conn: &mut RawConnectionFlags,
    env_lookup: &F,
) -> Result<SwarmAction, CliSupportError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let mut scope = RunScopeFlags::default();
    let mut since_sequence = None;
    let mut channel_id = None;
    let mut channel_name = None;
    let mut limit = None;
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
            "--since-sequence" | "--after-seq" => set_once(
                &mut since_sequence,
                parse_exact(next_utf8_value(arguments, index)?)?,
            )?,
            "--channel-id" => set_canonical_uuid_string(&mut channel_id, arguments, index)?,
            "--channel" => set_once(
                &mut channel_name,
                next_utf8_value(arguments, index)?.to_owned(),
            )?,
            "--limit" => set_once(&mut limit, parse_exact(next_utf8_value(arguments, index)?)?)?,
            _ => return Err(CliSupportError::InvalidArguments),
        }
    }
    let (workspace_id, swarm_run_id, request_file) = scope.require_run(env_lookup)?;
    if let Some(path) = request_file {
        if saw_inline {
            return Err(CliSupportError::InvalidArguments);
        }
        return Ok(SwarmAction::Read {
            workspace_id,
            swarm_run_id,
            channel_name: None,
            payload: SwarmPayload::RequestFile(path),
        });
    }
    if channel_id.is_some() && channel_name.is_some() {
        return Err(CliSupportError::InvalidArguments);
    }
    Ok(SwarmAction::Read {
        workspace_id,
        swarm_run_id,
        channel_name,
        payload: SwarmPayload::Inline(ReadSwarmMessagesQuery {
            since_sequence,
            channel_id,
            limit,
        }),
    })
}
