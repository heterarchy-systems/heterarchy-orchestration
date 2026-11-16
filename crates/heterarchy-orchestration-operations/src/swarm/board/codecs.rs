//! SQLite row readers, decoders, and lookup helpers for Swarm runs, members,
//! channels, subscriptions, doorbells, and board messages.

use forge_domain::{DagNodeId, DagRevisionNumber, GoalId, WorkspaceId};
use heterarchy_orchestration_sessions::AgentProfileId;
use rusqlite::{Connection, OptionalExtension, Row, params};
use uuid::Uuid;

use super::super::sql::{
    SELECT_SWARM_BOARD_SINCE_ALL_CHANNELS_SQL, SELECT_SWARM_BOARD_SINCE_CHANNEL_SQL,
    SELECT_SWARM_CHANNEL_BY_ID_SQL, SELECT_SWARM_CHANNEL_BY_NAME_SQL,
    SELECT_SWARM_CHANNEL_SUBSCRIBER_IDS_SQL, SELECT_SWARM_CHANNELS_BY_RUN_SQL,
    SELECT_SWARM_DOORBELLS_BY_RUN_SQL, SELECT_SWARM_MEMBER_BY_ID_SQL,
    SELECT_SWARM_MEMBER_BY_ROSTER_SQL, SELECT_SWARM_MEMBERS_BY_RUN_SQL,
    SELECT_SWARM_MESSAGE_BY_ID_SQL, SELECT_SWARM_RUN_BY_ID_SQL, SELECT_SWARM_RUN_BY_REQUEST_ID_SQL,
    SELECT_SWARM_SUBSCRIPTIONS_BY_RUN_SQL,
};
use super::super::{
    MAX_SWARM_CHANNELS_PER_RUN, MAX_SWARM_MEMBERS_PER_RUN, MAX_SWARM_SPAWN_DEPTH, SwarmCommitOid,
    SwarmError, SwarmMemberIdentity, validate_channel_name, validate_roster_name, validate_title,
};
use super::{
    MAX_DOORBELL_READ_LIMIT, MAX_SUBSCRIPTION_READ_LIMIT, SwarmBoardMessage, SwarmChannelRecord,
    SwarmDoorbellReason, SwarmDoorbellRecord, SwarmMemberRecord, SwarmMemberState, SwarmRoleKind,
    SwarmRunMode, SwarmRunRecord, SwarmRunState, SwarmWorkspaceMode,
};
use crate::sqlite_codecs::{
    decode_bool_flag, decode_non_negative_u64, decode_non_nil_uuid, decode_non_zero_digest,
    decode_optional_non_nil_uuid, decode_positive_u32, decode_positive_u64, decode_principal,
    decode_utf8,
};
use crate::{ForgeWorktreeRef, MAX_MESSAGE_BODY_BYTES, OperationsError};

pub(super) struct RawRunRow {
    swarm_run_id: Vec<u8>,
    workspace_id: Vec<u8>,
    _request_id: Vec<u8>,
    payload_digest: Vec<u8>,
    title_bytes: Vec<u8>,
    mode_kind: i64,
    goal_id: Option<Vec<u8>>,
    dag_revision: Option<i64>,
    node_id: Option<Vec<u8>>,
    declared_write_manifest_blake3: Option<Vec<u8>>,
    state: String,
    max_members: i64,
    max_spawn_depth: i64,
    max_total_turns: i64,
    total_turns_used: i64,
    required_verifier_quorum: i64,
    allow_worker_all_mention: i64,
    baseline_commit_oid_bytes: Vec<u8>,
    baseline_tree_blake3: Vec<u8>,
    converged_commit_oid_bytes: Option<Vec<u8>>,
    converged_tree_blake3: Option<Vec<u8>>,
    converged_diff_digest: Option<Vec<u8>>,
    stall_nudge_count: i64,
    settlement_request_id: Option<Vec<u8>>,
    revision: i64,
    created_at_ms: i64,
    updated_at_ms: i64,
}

pub(super) fn read_raw_run_row(row: &Row<'_>) -> rusqlite::Result<RawRunRow> {
    Ok(RawRunRow {
        swarm_run_id: row.get(0)?,
        workspace_id: row.get(1)?,
        _request_id: row.get(2)?,
        payload_digest: row.get(3)?,
        title_bytes: row.get(4)?,
        mode_kind: row.get(5)?,
        goal_id: row.get(6)?,
        dag_revision: row.get(7)?,
        node_id: row.get(8)?,
        declared_write_manifest_blake3: row.get(9)?,
        state: row.get(10)?,
        max_members: row.get(11)?,
        max_spawn_depth: row.get(12)?,
        max_total_turns: row.get(13)?,
        total_turns_used: row.get(14)?,
        required_verifier_quorum: row.get(15)?,
        allow_worker_all_mention: row.get(16)?,
        baseline_commit_oid_bytes: row.get(17)?,
        baseline_tree_blake3: row.get(18)?,
        converged_commit_oid_bytes: row.get(19)?,
        converged_tree_blake3: row.get(20)?,
        converged_diff_digest: row.get(21)?,
        stall_nudge_count: row.get(22)?,
        settlement_request_id: row.get(23)?,
        revision: row.get(24)?,
        created_at_ms: row.get(25)?,
        updated_at_ms: row.get(26)?,
    })
}

pub(super) fn decode_run_row_with_digest(
    raw: RawRunRow,
) -> Result<(SwarmRunRecord, [u8; 32]), OperationsError> {
    let swarm_run_id = decode_non_nil_uuid(&raw.swarm_run_id, "swarm_run_id")?;
    let workspace_id = WorkspaceId::from_uuid(decode_non_nil_uuid(
        &raw.workspace_id,
        "swarm workspace_id",
    )?);
    let payload_digest = decode_non_zero_digest(&raw.payload_digest, "swarm payload_digest")?;
    let title = decode_utf8(raw.title_bytes, "swarm title")?;
    validate_title(&title).map_err(|_| OperationsError::Corrupt("swarm title"))?;
    let mode = decode_run_mode(
        raw.mode_kind,
        raw.goal_id.as_deref(),
        raw.dag_revision,
        raw.node_id.as_deref(),
        raw.declared_write_manifest_blake3.as_deref(),
    )?;
    let state = SwarmRunState::parse(&raw.state)?;
    let max_members = decode_positive_u32(raw.max_members, "swarm max_members")?;
    let max_spawn_depth = u8::try_from(raw.max_spawn_depth)
        .ok()
        .filter(|d| *d <= MAX_SWARM_SPAWN_DEPTH)
        .ok_or(OperationsError::Corrupt("swarm max_spawn_depth"))?;
    let max_total_turns = decode_positive_u64(raw.max_total_turns, "swarm max_total_turns")?;
    let total_turns_used = decode_non_negative_u64(raw.total_turns_used, "swarm total_turns_used")?;
    let required_verifier_quorum = decode_positive_u32(
        raw.required_verifier_quorum,
        "swarm required_verifier_quorum",
    )?;
    let allow_worker_all_mention = decode_bool_flag(
        raw.allow_worker_all_mention,
        "swarm allow_worker_all_mention",
    )?;
    let baseline_commit_oid = SwarmCommitOid::new(decode_utf8(
        raw.baseline_commit_oid_bytes,
        "swarm baseline_commit_oid",
    )?)
    .map_err(|_| OperationsError::Corrupt("swarm baseline_commit_oid"))?;
    let baseline_tree_blake3 =
        decode_non_zero_digest(&raw.baseline_tree_blake3, "swarm baseline_tree_blake3")?;
    let converged = decode_converged_fields(
        raw.converged_commit_oid_bytes,
        raw.converged_tree_blake3.as_deref(),
        raw.converged_diff_digest.as_deref(),
    )?;
    let stall_nudge_count = u32::try_from(raw.stall_nudge_count)
        .map_err(|_| OperationsError::Corrupt("swarm stall_nudge_count"))?;
    let settlement_request_id = decode_optional_non_nil_uuid(
        raw.settlement_request_id.as_deref(),
        "swarm settlement_request_id",
    )?;
    let revision = decode_positive_u64(raw.revision, "swarm run revision")?;
    if raw.created_at_ms < 0 || raw.updated_at_ms < raw.created_at_ms {
        return Err(OperationsError::Corrupt("swarm run timestamps"));
    }
    Ok((
        SwarmRunRecord {
            swarm_run_id,
            workspace_id,
            title,
            mode,
            state,
            max_members,
            max_spawn_depth,
            max_total_turns,
            total_turns_used,
            required_verifier_quorum,
            allow_worker_all_mention,
            baseline_commit_oid,
            baseline_tree_blake3,
            converged_commit_oid: converged.commit_oid,
            converged_tree_blake3: converged.tree_blake3,
            converged_diff_digest: converged.diff_digest,
            stall_nudge_count,
            settlement_request_id,
            revision,
            created_at_ms: raw.created_at_ms,
            updated_at_ms: raw.updated_at_ms,
            replayed: false,
        },
        payload_digest,
    ))
}

fn decode_run_mode(
    mode_kind: i64,
    goal_id_raw: Option<&[u8]>,
    dag_revision_raw: Option<i64>,
    node_id_raw: Option<&[u8]>,
    manifest_raw: Option<&[u8]>,
) -> Result<SwarmRunMode, OperationsError> {
    match mode_kind {
        0 => Ok(SwarmRunMode::Standalone),
        1 => {
            let goal_id = GoalId::from_uuid(decode_non_nil_uuid(
                goal_id_raw.ok_or(OperationsError::Corrupt("swarm goal_id"))?,
                "swarm goal_id",
            )?);
            let dag_revision = DagRevisionNumber::new(
                dag_revision_raw.ok_or(OperationsError::Corrupt("swarm dag_revision"))?,
            )
            .map_err(|_| OperationsError::Corrupt("swarm dag_revision"))?;
            let node_id = DagNodeId::from_uuid(decode_non_nil_uuid(
                node_id_raw.ok_or(OperationsError::Corrupt("swarm node_id"))?,
                "swarm node_id",
            )?);
            let declared_write_manifest_blake3 = decode_non_zero_digest(
                manifest_raw.ok_or(OperationsError::Corrupt("swarm declared_write_manifest"))?,
                "swarm declared_write_manifest",
            )?;
            Ok(SwarmRunMode::DagNodeHybrid {
                goal_id,
                dag_revision,
                node_id,
                declared_write_manifest_blake3,
            })
        }
        _ => Err(OperationsError::Corrupt("swarm mode_kind")),
    }
}

struct ConvergedFields {
    commit_oid: Option<SwarmCommitOid>,
    tree_blake3: Option<[u8; 32]>,
    diff_digest: Option<[u8; 32]>,
}

fn decode_converged_fields(
    commit_bytes: Option<Vec<u8>>,
    tree_bytes: Option<&[u8]>,
    digest_bytes: Option<&[u8]>,
) -> Result<ConvergedFields, OperationsError> {
    let commit_oid = commit_bytes
        .map(|bytes| {
            SwarmCommitOid::new(decode_utf8(bytes, "swarm converged_commit_oid")?)
                .map_err(|_| OperationsError::Corrupt("swarm converged_commit_oid"))
        })
        .transpose()?;
    let tree_blake3 = tree_bytes
        .map(|bytes| decode_non_zero_digest(bytes, "swarm converged_tree_blake3"))
        .transpose()?;
    let diff_digest = digest_bytes
        .map(|bytes| decode_non_zero_digest(bytes, "swarm converged_diff_digest"))
        .transpose()?;
    Ok(ConvergedFields {
        commit_oid,
        tree_blake3,
        diff_digest,
    })
}

pub(super) fn decode_run_row(raw: RawRunRow) -> Result<SwarmRunRecord, OperationsError> {
    decode_run_row_with_digest(raw).map(|(record, _)| record)
}

pub(super) fn load_run_replay(
    conn: &Connection,
    request_id: Uuid,
    expected_digest: [u8; 32],
) -> Result<Option<SwarmRunRecord>, SwarmError> {
    let raw = conn
        .query_row(
            SELECT_SWARM_RUN_BY_REQUEST_ID_SQL,
            params![request_id.as_bytes()],
            read_raw_run_row,
        )
        .optional()
        .map_err(OperationsError::from)?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    let (mut record, stored_digest) = decode_run_row_with_digest(raw)?;
    if stored_digest != expected_digest {
        return Err(OperationsError::Conflict(
            "swarm run request_id reused with different payload",
        )
        .into());
    }
    record.replayed = true;
    Ok(Some(record))
}

pub(in crate::swarm) fn load_run_by_id(
    conn: &Connection,
    workspace_id: WorkspaceId,
    swarm_run_id: Uuid,
) -> Result<Option<SwarmRunRecord>, OperationsError> {
    let raw = conn
        .query_row(
            SELECT_SWARM_RUN_BY_ID_SQL,
            params![workspace_id.into_uuid().as_bytes(), swarm_run_id.as_bytes()],
            read_raw_run_row,
        )
        .optional()?;
    raw.map(decode_run_row).transpose()
}

pub(in crate::swarm) fn require_run_by_id(
    conn: &Connection,
    workspace_id: WorkspaceId,
    swarm_run_id: Uuid,
) -> Result<SwarmRunRecord, SwarmError> {
    load_run_by_id(conn, workspace_id, swarm_run_id)?.ok_or(SwarmError::NotFound("swarm run"))
}

pub(super) struct RawMemberRow {
    member_id: Vec<u8>,
    swarm_run_id: Vec<u8>,
    workspace_id: Vec<u8>,
    roster_name_bytes: Vec<u8>,
    role_kind: String,
    parent_member_id: Option<Vec<u8>>,
    spawn_depth: i64,
    workspace_mode: String,
    worktree_path_bytes: Option<Vec<u8>>,
    member_state: String,
    profile_id: Vec<u8>,
    participant_id: Vec<u8>,
    principal_kind: i64,
    principal_id: Option<Vec<u8>>,
    committed_seen_seq: i64,
    inflight_target_seq: i64,
    active_snapshot_id: Option<Vec<u8>>,
    attempt_epoch: i64,
    turns_used: i64,
    ping_pong_peer_id: Option<Vec<u8>>,
    ping_pong_streak: i64,
    last_output_commit_oid_bytes: Option<Vec<u8>>,
    last_output_tree_blake3: Option<Vec<u8>>,
    created_at_ms: i64,
    updated_at_ms: i64,
}

pub(super) fn read_raw_member_row(row: &Row<'_>) -> rusqlite::Result<RawMemberRow> {
    Ok(RawMemberRow {
        member_id: row.get(0)?,
        swarm_run_id: row.get(1)?,
        workspace_id: row.get(2)?,
        roster_name_bytes: row.get(3)?,
        role_kind: row.get(4)?,
        parent_member_id: row.get(5)?,
        spawn_depth: row.get(6)?,
        workspace_mode: row.get(7)?,
        worktree_path_bytes: row.get(8)?,
        member_state: row.get(9)?,
        profile_id: row.get(10)?,
        participant_id: row.get(11)?,
        principal_kind: row.get(12)?,
        principal_id: row.get(13)?,
        committed_seen_seq: row.get(14)?,
        inflight_target_seq: row.get(15)?,
        active_snapshot_id: row.get(16)?,
        attempt_epoch: row.get(17)?,
        turns_used: row.get(18)?,
        ping_pong_peer_id: row.get(19)?,
        ping_pong_streak: row.get(20)?,
        last_output_commit_oid_bytes: row.get(21)?,
        last_output_tree_blake3: row.get(22)?,
        created_at_ms: row.get(23)?,
        updated_at_ms: row.get(24)?,
    })
}

pub(super) fn decode_member_row(raw: RawMemberRow) -> Result<SwarmMemberRecord, OperationsError> {
    let member_id = decode_non_nil_uuid(&raw.member_id, "swarm member_id")?;
    let swarm_run_id = decode_non_nil_uuid(&raw.swarm_run_id, "swarm_run_id")?;
    let workspace_id = WorkspaceId::from_uuid(decode_non_nil_uuid(
        &raw.workspace_id,
        "swarm workspace_id",
    )?);
    let roster_name = decode_utf8(raw.roster_name_bytes, "swarm roster_name")?;
    validate_roster_name(&roster_name)
        .map_err(|_| OperationsError::Corrupt("swarm roster_name"))?;
    let role_kind = SwarmRoleKind::parse(&raw.role_kind)?;
    let parent_member_id =
        decode_optional_non_nil_uuid(raw.parent_member_id.as_deref(), "swarm parent_member_id")?;
    let spawn_depth = u8::try_from(raw.spawn_depth)
        .ok()
        .filter(|d| *d <= MAX_SWARM_SPAWN_DEPTH)
        .ok_or(OperationsError::Corrupt("swarm spawn_depth"))?;
    let workspace_mode = SwarmWorkspaceMode::parse(&raw.workspace_mode)?;
    let worktree_path = raw
        .worktree_path_bytes
        .map(|bytes| {
            let text = decode_utf8(bytes, "swarm worktree_path")?;
            ForgeWorktreeRef::new(text.into())
                .map_err(|_| OperationsError::Corrupt("swarm worktree_path"))
        })
        .transpose()?;
    let member_state = SwarmMemberState::parse(&raw.member_state)?;
    let profile_uuid = decode_non_nil_uuid(&raw.profile_id, "swarm member profile_id")?;
    let profile_id = AgentProfileId::from_uuid(profile_uuid)
        .map_err(|_| OperationsError::Corrupt("swarm member profile_id"))?;
    let participant_id = decode_non_nil_uuid(&raw.participant_id, "swarm member participant_id")?;
    let principal = decode_principal(
        raw.principal_kind,
        raw.principal_id.as_deref(),
        "swarm member principal",
    )?;
    let identity = SwarmMemberIdentity::new(profile_id, participant_id, principal)
        .map_err(|_| OperationsError::Corrupt("swarm member identity"))?;
    let committed_seen_seq =
        decode_non_negative_u64(raw.committed_seen_seq, "swarm committed_seen_seq")?;
    let inflight_target_seq =
        decode_non_negative_u64(raw.inflight_target_seq, "swarm inflight_target_seq")?;
    let active_snapshot_id = decode_optional_non_nil_uuid(
        raw.active_snapshot_id.as_deref(),
        "swarm active_snapshot_id",
    )?;
    let attempt_epoch = decode_non_negative_u64(raw.attempt_epoch, "swarm attempt_epoch")?;
    let turns_used = decode_non_negative_u64(raw.turns_used, "swarm turns_used")?;
    let ping_pong_peer_id =
        decode_optional_non_nil_uuid(raw.ping_pong_peer_id.as_deref(), "swarm ping_pong_peer_id")?;
    let ping_pong_streak = u32::try_from(raw.ping_pong_streak)
        .map_err(|_| OperationsError::Corrupt("swarm ping_pong_streak"))?;
    let last_output_commit_oid = raw
        .last_output_commit_oid_bytes
        .map(|bytes| {
            SwarmCommitOid::new(decode_utf8(bytes, "swarm last_output_commit_oid")?)
                .map_err(|_| OperationsError::Corrupt("swarm last_output_commit_oid"))
        })
        .transpose()?;
    let last_output_tree_blake3 = raw
        .last_output_tree_blake3
        .as_deref()
        .map(|bytes| decode_non_zero_digest(bytes, "swarm last_output_tree_blake3"))
        .transpose()?;
    if raw.created_at_ms < 0 || raw.updated_at_ms < raw.created_at_ms {
        return Err(OperationsError::Corrupt("swarm member timestamps"));
    }
    Ok(SwarmMemberRecord {
        member_id,
        swarm_run_id,
        workspace_id,
        roster_name,
        role_kind,
        parent_member_id,
        spawn_depth,
        workspace_mode,
        worktree_path,
        member_state,
        identity,
        committed_seen_seq,
        inflight_target_seq,
        active_snapshot_id,
        attempt_epoch,
        turns_used,
        ping_pong_peer_id,
        ping_pong_streak,
        last_output_commit_oid,
        last_output_tree_blake3,
        created_at_ms: raw.created_at_ms,
        updated_at_ms: raw.updated_at_ms,
    })
}

pub(super) fn load_member_by_id(
    conn: &Connection,
    swarm_run_id: Uuid,
    member_id: Uuid,
) -> Result<Option<SwarmMemberRecord>, OperationsError> {
    let raw = conn
        .query_row(
            SELECT_SWARM_MEMBER_BY_ID_SQL,
            params![swarm_run_id.as_bytes(), member_id.as_bytes()],
            read_raw_member_row,
        )
        .optional()?;
    raw.map(decode_member_row).transpose()
}

pub(in crate::swarm) fn require_member_by_id(
    conn: &Connection,
    swarm_run_id: Uuid,
    member_id: Uuid,
) -> Result<SwarmMemberRecord, SwarmError> {
    load_member_by_id(conn, swarm_run_id, member_id)?.ok_or(SwarmError::NotFound("swarm member"))
}

pub(super) fn load_member_by_roster(
    conn: &Connection,
    swarm_run_id: Uuid,
    roster_name: &str,
) -> Result<Option<SwarmMemberRecord>, OperationsError> {
    let raw = conn
        .query_row(
            SELECT_SWARM_MEMBER_BY_ROSTER_SQL,
            params![swarm_run_id.as_bytes(), roster_name],
            read_raw_member_row,
        )
        .optional()?;
    raw.map(decode_member_row).transpose()
}

pub(in crate::swarm) fn load_members_by_run(
    conn: &Connection,
    swarm_run_id: Uuid,
) -> Result<Vec<SwarmMemberRecord>, OperationsError> {
    let limit = i64::try_from(MAX_SWARM_MEMBERS_PER_RUN * 2)
        .map_err(|_| OperationsError::Bounds("swarm members query limit"))?;
    let mut stmt = conn.prepare(SELECT_SWARM_MEMBERS_BY_RUN_SQL)?;
    let raw_rows = stmt
        .query_map(params![swarm_run_id.as_bytes(), limit], read_raw_member_row)?
        .collect::<Result<Vec<_>, _>>()?;
    drop(stmt);
    raw_rows.into_iter().map(decode_member_row).collect()
}

struct RawChannelRow {
    channel_id: Vec<u8>,
    swarm_run_id: Vec<u8>,
    workspace_id: Vec<u8>,
    name_bytes: Vec<u8>,
    purpose_bytes: Option<Vec<u8>>,
    created_by_member_id: Option<Vec<u8>>,
    created_at_ms: i64,
}

fn read_raw_channel_row(row: &Row<'_>) -> rusqlite::Result<RawChannelRow> {
    Ok(RawChannelRow {
        channel_id: row.get(0)?,
        swarm_run_id: row.get(1)?,
        workspace_id: row.get(2)?,
        name_bytes: row.get(3)?,
        purpose_bytes: row.get(4)?,
        created_by_member_id: row.get(5)?,
        created_at_ms: row.get(6)?,
    })
}

fn decode_channel_row(raw: RawChannelRow) -> Result<SwarmChannelRecord, OperationsError> {
    let channel_id = decode_non_nil_uuid(&raw.channel_id, "swarm channel_id")?;
    let swarm_run_id = decode_non_nil_uuid(&raw.swarm_run_id, "swarm_run_id")?;
    let workspace_id = WorkspaceId::from_uuid(decode_non_nil_uuid(
        &raw.workspace_id,
        "swarm workspace_id",
    )?);
    let name = decode_utf8(raw.name_bytes, "swarm channel name")?;
    validate_channel_name(&name).map_err(|_| OperationsError::Corrupt("swarm channel name"))?;
    let purpose = raw
        .purpose_bytes
        .map(|bytes| decode_utf8(bytes, "swarm channel purpose"))
        .transpose()?;
    let created_by_member_id =
        decode_optional_non_nil_uuid(raw.created_by_member_id.as_deref(), "swarm channel creator")?;
    if raw.created_at_ms < 0 {
        return Err(OperationsError::Corrupt("swarm channel created_at_ms"));
    }
    Ok(SwarmChannelRecord {
        channel_id,
        swarm_run_id,
        workspace_id,
        name,
        purpose,
        created_by_member_id,
        created_at_ms: raw.created_at_ms,
    })
}

pub(super) fn load_channel_by_id(
    conn: &Connection,
    swarm_run_id: Uuid,
    channel_id: Uuid,
) -> Result<Option<SwarmChannelRecord>, OperationsError> {
    let raw = conn
        .query_row(
            SELECT_SWARM_CHANNEL_BY_ID_SQL,
            params![swarm_run_id.as_bytes(), channel_id.as_bytes()],
            read_raw_channel_row,
        )
        .optional()?;
    raw.map(decode_channel_row).transpose()
}

pub(super) fn require_channel_by_id(
    conn: &Connection,
    swarm_run_id: Uuid,
    channel_id: Uuid,
) -> Result<SwarmChannelRecord, SwarmError> {
    load_channel_by_id(conn, swarm_run_id, channel_id)?.ok_or(SwarmError::NotFound("swarm channel"))
}

pub(super) fn load_channel_by_name(
    conn: &Connection,
    swarm_run_id: Uuid,
    name: &str,
) -> Result<Option<SwarmChannelRecord>, OperationsError> {
    let raw = conn
        .query_row(
            SELECT_SWARM_CHANNEL_BY_NAME_SQL,
            params![swarm_run_id.as_bytes(), name],
            read_raw_channel_row,
        )
        .optional()?;
    raw.map(decode_channel_row).transpose()
}

pub(super) fn load_channels_by_run(
    conn: &Connection,
    swarm_run_id: Uuid,
) -> Result<Vec<SwarmChannelRecord>, OperationsError> {
    let limit = i64::try_from(MAX_SWARM_CHANNELS_PER_RUN + 1)
        .map_err(|_| OperationsError::Bounds("swarm channel query limit"))?;
    let mut stmt = conn.prepare(SELECT_SWARM_CHANNELS_BY_RUN_SQL)?;
    let raw_rows = stmt
        .query_map(
            params![swarm_run_id.as_bytes(), limit],
            read_raw_channel_row,
        )?
        .collect::<Result<Vec<_>, _>>()?;
    drop(stmt);
    raw_rows.into_iter().map(decode_channel_row).collect()
}

pub(super) fn load_subscriptions_by_run(
    conn: &Connection,
    swarm_run_id: Uuid,
) -> Result<Vec<(Uuid, Uuid)>, OperationsError> {
    let mut stmt = conn.prepare(SELECT_SWARM_SUBSCRIPTIONS_BY_RUN_SQL)?;
    let raw_rows = stmt
        .query_map(
            params![swarm_run_id.as_bytes(), MAX_SUBSCRIPTION_READ_LIMIT],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )?
        .collect::<Result<Vec<_>, _>>()?;
    drop(stmt);
    raw_rows
        .into_iter()
        .map(|(ch, m)| {
            Ok((
                decode_non_nil_uuid(&ch, "swarm subscription channel_id")?,
                decode_non_nil_uuid(&m, "swarm subscription member_id")?,
            ))
        })
        .collect()
}

pub(super) fn load_active_channel_subscriber_ids(
    conn: &Connection,
    swarm_run_id: Uuid,
    channel_id: Uuid,
) -> Result<Vec<Uuid>, OperationsError> {
    let limit = i64::try_from(MAX_SWARM_MEMBERS_PER_RUN * 2)
        .map_err(|_| OperationsError::Bounds("swarm subscriber query limit"))?;
    let mut stmt = conn.prepare(SELECT_SWARM_CHANNEL_SUBSCRIBER_IDS_SQL)?;
    let raw_rows = stmt
        .query_map(
            params![swarm_run_id.as_bytes(), channel_id.as_bytes(), limit],
            |row| row.get::<_, Vec<u8>>(0),
        )?
        .collect::<Result<Vec<_>, _>>()?;
    drop(stmt);
    raw_rows
        .into_iter()
        .map(|bytes| decode_non_nil_uuid(&bytes, "swarm subscriber member_id"))
        .collect()
}

pub(super) fn load_doorbells_by_run(
    conn: &Connection,
    swarm_run_id: Uuid,
) -> Result<Vec<SwarmDoorbellRecord>, OperationsError> {
    let mut stmt = conn.prepare(SELECT_SWARM_DOORBELLS_BY_RUN_SQL)?;
    let raw_rows = stmt
        .query_map(
            params![swarm_run_id.as_bytes(), MAX_DOORBELL_READ_LIMIT],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;
    drop(stmt);
    raw_rows
        .into_iter()
        .map(|(run_bytes, target_bytes, seq, reason, enqueued_at_ms)| {
            if enqueued_at_ms < 0 {
                return Err(OperationsError::Corrupt("swarm doorbell enqueued_at_ms"));
            }
            Ok(SwarmDoorbellRecord {
                swarm_run_id: decode_non_nil_uuid(&run_bytes, "swarm_run_id")?,
                target_member_id: decode_non_nil_uuid(&target_bytes, "swarm target_member_id")?,
                sequence: decode_positive_u64(seq, "swarm doorbell sequence")?,
                reason: SwarmDoorbellReason::parse(&reason)?,
                enqueued_at_ms,
            })
        })
        .collect()
}

pub(super) struct RawBoardMessageRow {
    message_id: Vec<u8>,
    workspace_id: Vec<u8>,
    sequence: i64,
    swarm_run_id: Option<Vec<u8>>,
    channel_id: Option<Vec<u8>>,
    channel_name_bytes: Option<Vec<u8>>,
    author_member_id: Option<Vec<u8>>,
    author_display_name_bytes: Vec<u8>,
    author_epoch: Option<i64>,
    body_bytes: Vec<u8>,
    submitted_at_ms: i64,
}

pub(super) fn read_raw_board_message_row(row: &Row<'_>) -> rusqlite::Result<RawBoardMessageRow> {
    Ok(RawBoardMessageRow {
        message_id: row.get(0)?,
        workspace_id: row.get(1)?,
        sequence: row.get(2)?,
        swarm_run_id: row.get(3)?,
        channel_id: row.get(4)?,
        channel_name_bytes: row.get(5)?,
        author_member_id: row.get(6)?,
        author_display_name_bytes: row.get(7)?,
        author_epoch: row.get(8)?,
        body_bytes: row.get(9)?,
        submitted_at_ms: row.get(10)?,
    })
}

pub(super) fn decode_board_message_row(
    raw: RawBoardMessageRow,
) -> Result<SwarmBoardMessage, OperationsError> {
    let message_id = decode_non_nil_uuid(&raw.message_id, "swarm message_id")?;
    let workspace_id = WorkspaceId::from_uuid(decode_non_nil_uuid(
        &raw.workspace_id,
        "swarm workspace_id",
    )?);
    let sequence = decode_positive_u64(raw.sequence, "swarm message sequence")?;
    let swarm_run_id = decode_non_nil_uuid(
        raw.swarm_run_id
            .as_deref()
            .ok_or(OperationsError::Conflict(
                "message_id already used outside swarm",
            ))?,
        "swarm_run_id",
    )?;
    let channel_id = decode_non_nil_uuid(
        raw.channel_id
            .as_deref()
            .ok_or(OperationsError::Corrupt("swarm channel_id"))?,
        "swarm channel_id",
    )?;
    let channel_name = decode_utf8(
        raw.channel_name_bytes
            .ok_or(OperationsError::Corrupt("swarm channel_name"))?,
        "swarm channel_name",
    )?;
    let author_member_id =
        decode_optional_non_nil_uuid(raw.author_member_id.as_deref(), "swarm author_member_id")?;
    let author_roster_name =
        decode_utf8(raw.author_display_name_bytes, "swarm author_roster_name")?;
    let author_epoch = raw
        .author_epoch
        .map(|value| decode_non_negative_u64(value, "swarm author_epoch"))
        .transpose()?;
    let body = decode_utf8(raw.body_bytes, "swarm message body")?;
    if body.is_empty() || body.len() > MAX_MESSAGE_BODY_BYTES {
        return Err(OperationsError::Corrupt("swarm message body bounds"));
    }
    if raw.submitted_at_ms < 0 {
        return Err(OperationsError::Corrupt("swarm message submitted_at_ms"));
    }
    Ok(SwarmBoardMessage {
        message_id,
        workspace_id,
        sequence,
        swarm_run_id,
        channel_id,
        channel_name,
        author_member_id,
        author_roster_name,
        author_epoch,
        body,
        submitted_at_ms: raw.submitted_at_ms,
    })
}

pub(super) fn load_swarm_message_by_id(
    conn: &Connection,
    message_id: Uuid,
) -> Result<Option<SwarmBoardMessage>, OperationsError> {
    let raw = conn
        .query_row(
            SELECT_SWARM_MESSAGE_BY_ID_SQL,
            params![message_id.as_bytes()],
            read_raw_board_message_row,
        )
        .optional()?;
    raw.map(decode_board_message_row).transpose()
}

pub(super) fn query_board_messages_with_params<P: rusqlite::Params>(
    conn: &Connection,
    sql: &str,
    query_params: P,
) -> Result<Vec<SwarmBoardMessage>, SwarmError> {
    let mut stmt = conn.prepare(sql).map_err(OperationsError::from)?;
    let rows = stmt
        .query_map(query_params, read_raw_board_message_row)
        .map_err(OperationsError::from)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(OperationsError::from)?;
    drop(stmt);
    rows.into_iter()
        .map(decode_board_message_row)
        .collect::<Result<Vec<_>, _>>()
        .map_err(SwarmError::from)
}

pub(super) fn query_board_messages_since(
    conn: &Connection,
    workspace_id: WorkspaceId,
    swarm_run_id: Uuid,
    channel_id: Option<Uuid>,
    after: i64,
    sql_limit: i64,
) -> Result<Vec<SwarmBoardMessage>, SwarmError> {
    if let Some(channel_id) = channel_id {
        query_board_messages_with_params(
            conn,
            SELECT_SWARM_BOARD_SINCE_CHANNEL_SQL,
            params![
                workspace_id.into_uuid().as_bytes(),
                swarm_run_id.as_bytes(),
                channel_id.as_bytes(),
                after,
                sql_limit,
            ],
        )
    } else {
        query_board_messages_with_params(
            conn,
            SELECT_SWARM_BOARD_SINCE_ALL_CHANNELS_SQL,
            params![
                workspace_id.into_uuid().as_bytes(),
                swarm_run_id.as_bytes(),
                after,
                sql_limit,
            ],
        )
    }
}
