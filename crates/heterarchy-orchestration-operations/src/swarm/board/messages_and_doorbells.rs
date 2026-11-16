//! Swarm board message overlay (`ops_workspace_messages`), `@mention` parsing,
//! doorbell coalescing, incremental board queries, and stall/livelock persistence.

use std::collections::{BTreeMap, BTreeSet};

use forge_domain::{DagRevisionNumber, WorkspaceId};
use rusqlite::{Transaction, TransactionBehavior, params};
use uuid::Uuid;

use super::super::sql::{
    INCREMENT_SWARM_RUN_STALL_NUDGE_SQL, INSERT_SWARM_CHANNEL_MESSAGE_SQL,
    INSERT_SWARM_DOORBELL_SQL, INSERT_SWARM_WORKSPACE_MESSAGE_SQL,
    SELECT_NEXT_WORKSPACE_MESSAGE_SEQUENCE_SQL, UPDATE_SWARM_MEMBER_PING_PONG_SQL,
    UPDATE_SWARM_MEMBER_STOP_SQL, UPDATE_SWARM_MEMBER_WAKE_QUEUED_SQL,
};
use super::super::{
    MAX_SWARM_BOARD_PAGE_SIZE, MAX_SWARM_CHANNEL_NAME_BYTES, MAX_SWARM_ROSTER_NAME_BYTES,
    SwarmError, check_timestamp_non_negative, map_sqlite_conflict, validate_non_nil_uuid,
    validate_workspace_id,
};
use super::codecs::{
    load_active_channel_subscriber_ids, load_channel_by_name, load_member_by_roster,
    load_members_by_run, load_swarm_message_by_id, query_board_messages_since,
    require_channel_by_id, require_member_by_id, require_run_by_id,
};
use super::convergence::abort_run_in_tx;
use super::{
    BreakSwarmLivelockCommand, BreakSwarmLivelockOutcome, PostSwarmMessageCommand,
    ReadSwarmBoardQuery, RecordSwarmStallNudgeCommand, SwarmBoardMessage, SwarmBoardPage,
    SwarmDoorbellReason, SwarmMemberRecord, SwarmMemberState, SwarmMessageAuthor,
    SwarmPostMessageOutcome, SwarmRoleKind, SwarmRunRecord, SwarmStallNudgeOutcome,
};
use crate::sqlite_codecs::decode_non_negative_u64;
use crate::{MessageContext, OperationsCatalog, OperationsError, validate_chat_text};

impl OperationsCatalog {
    /// Post a Swarm board message, atomically allocating a workspace-global monotonic
    /// `ops_workspace_messages.sequence`, recording `ops_swarm_channel_messages`,
    /// parsing `@member_name` / `@#channel_name` / `@all` mentions into `ops_swarm_doorbells`,
    /// updating `ping_pong_streak`, and transitioning `Waiting -> WakeQueued`.
    ///
    /// # Errors
    /// Rejects invalid payloads, killed authors, worker `@all` mentions when policy-gated,
    /// terminal runs, replay mismatches, or storage failures.
    pub fn post_swarm_message(
        &mut self,
        command: &PostSwarmMessageCommand,
    ) -> Result<SwarmPostMessageOutcome, SwarmError> {
        command.validate()?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(OperationsError::from)?;
        let run = require_run_by_id(&tx, command.workspace_id, command.swarm_run_id)?;
        if let Some(existing) = load_swarm_message_by_id(&tx, command.message_id)? {
            ensure_message_replay_matches(&existing, command)?;
            tx.commit().map_err(OperationsError::from)?;
            return Ok(SwarmPostMessageOutcome {
                message: existing,
                woken_member_ids: Vec::new(),
                coalesced_doorbell_member_ids: Vec::new(),
                replayed: true,
            });
        }
        if run.state.is_terminal() {
            return Err(SwarmError::InvalidStateTransition(
                "cannot post message to terminal Swarm run",
            ));
        }
        if command.submitted_at_ms < run.created_at_ms {
            return Err(
                OperationsError::Bounds("swarm submitted_at_ms precedes run creation").into(),
            );
        }
        let channel = require_channel_by_id(&tx, command.swarm_run_id, command.channel_id)?;
        if channel.workspace_id != command.workspace_id {
            return Err(SwarmError::NotFound("swarm channel in workspace"));
        }
        let (author_member, author_kind, author_epoch) =
            resolve_message_author(&tx, &run, command.author)?;
        let author_display_name = author_member
            .as_ref()
            .map_or("System", SwarmMemberRecord::roster_name);
        let (doorbells, direct_targets) =
            resolve_mention_targets(&tx, &run, author_member.as_ref(), &command.body)?;
        let sequence = insert_board_message_rows(
            &tx,
            &BoardMessageInsert {
                message_id: command.message_id,
                workspace_id: command.workspace_id,
                swarm_run_id: command.swarm_run_id,
                channel_id: command.channel_id,
                body: &command.body,
                context: &command.context,
                submitted_at_ms: command.submitted_at_ms,
                author_kind,
                author_display_name,
                author_member_id: author_member.as_ref().map(SwarmMemberRecord::member_id),
                author_epoch,
            },
        )?;
        let (woken_member_ids, coalesced_doorbell_member_ids) = dispatch_doorbells_and_ping_pong(
            &tx,
            &run,
            author_member.as_ref(),
            &doorbells,
            &direct_targets,
            sequence,
            command.submitted_at_ms,
        )?;
        let message = load_swarm_message_by_id(&tx, command.message_id)?
            .ok_or(OperationsError::Corrupt("inserted swarm message missing"))?;
        tx.commit().map_err(OperationsError::from)?;
        Ok(SwarmPostMessageOutcome {
            message,
            woken_member_ids,
            coalesced_doorbell_member_ids,
            replayed: false,
        })
    }

    /// Incrementally read Swarm board messages for a run (and optional channel) after `after_sequence`.
    ///
    /// # Errors
    /// Rejects invalid limits, missing runs/channels, or corrupt rows.
    pub fn read_swarm_board_since(
        &self,
        query: ReadSwarmBoardQuery,
    ) -> Result<SwarmBoardPage, SwarmError> {
        validate_workspace_id(query.workspace_id)?;
        validate_non_nil_uuid(query.swarm_run_id, "swarm_run_id")?;
        if let Some(channel_id) = query.channel_id {
            validate_non_nil_uuid(channel_id, "swarm channel_id")?;
        }
        if !(1..=MAX_SWARM_BOARD_PAGE_SIZE).contains(&query.limit) {
            return Err(OperationsError::Bounds("swarm board page limit").into());
        }
        let after = query
            .after_sequence
            .map(crate::catalog::signed)
            .transpose()?
            .unwrap_or(0);
        let sql_limit = i64::try_from(query.limit + 1)
            .map_err(|_| OperationsError::Bounds("swarm board page limit"))?;
        let tx = self
            .connection
            .unchecked_transaction()
            .map_err(OperationsError::from)?;
        let _run = require_run_by_id(&tx, query.workspace_id, query.swarm_run_id)?;
        if let Some(channel_id) = query.channel_id {
            let _channel = require_channel_by_id(&tx, query.swarm_run_id, channel_id)?;
        }
        let mut messages = query_board_messages_since(
            &tx,
            query.workspace_id,
            query.swarm_run_id,
            query.channel_id,
            after,
            sql_limit,
        )?;
        let has_more = messages.len() > query.limit;
        if has_more {
            messages.pop();
        }
        let next_after_sequence = has_more
            .then(|| messages.last().map(SwarmBoardMessage::sequence))
            .flatten();
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(SwarmBoardPage {
            messages,
            next_after_sequence,
            has_more,
        })
    }

    /// Record a deterministic system stall or livelock nudge doorbell, escalating to `Aborted`
    /// when `stall_nudge_count >= max_stall_nudges` on stall checks.
    ///
    /// # Errors
    /// Rejects invalid reasons, terminal runs, missing targets, or storage failures.
    pub fn record_swarm_stall_nudge(
        &mut self,
        command: &RecordSwarmStallNudgeCommand,
    ) -> Result<SwarmStallNudgeOutcome, SwarmError> {
        validate_non_nil_uuid(command.message_id, "swarm message_id")?;
        validate_workspace_id(command.workspace_id)?;
        validate_non_nil_uuid(command.swarm_run_id, "swarm_run_id")?;
        validate_non_nil_uuid(command.channel_id, "swarm channel_id")?;
        validate_non_nil_uuid(command.target_member_id, "swarm target_member_id")?;
        validate_chat_text(&command.body)?;
        check_timestamp_non_negative(command.observed_at_ms, "swarm observed_at_ms")?;
        if !matches!(
            command.reason,
            SwarmDoorbellReason::SystemStall | SwarmDoorbellReason::SystemLivelock
        ) {
            return Err(OperationsError::Bounds("swarm stall nudge reason").into());
        }

        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(OperationsError::from)?;
        let run = require_run_by_id(&tx, command.workspace_id, command.swarm_run_id)?;
        if run.state.is_terminal() {
            return Err(SwarmError::InvalidStateTransition(
                "cannot nudge terminal Swarm run",
            ));
        }
        if command.reason == SwarmDoorbellReason::SystemStall
            && run.stall_nudge_count >= command.max_stall_nudges
        {
            let aborted = abort_run_in_tx(&tx, &run, command.observed_at_ms)?;
            tx.commit().map_err(OperationsError::from)?;
            return Ok(SwarmStallNudgeOutcome {
                run: aborted,
                message: None,
                escalated_to_aborted: true,
            });
        }
        let channel = require_channel_by_id(&tx, command.swarm_run_id, command.channel_id)?;
        if channel.workspace_id != command.workspace_id {
            return Err(SwarmError::NotFound("swarm channel in workspace"));
        }
        let target = require_member_by_id(&tx, command.swarm_run_id, command.target_member_id)?;
        if target.member_state == SwarmMemberState::Killed {
            return Err(SwarmError::InvalidStateTransition(
                "cannot nudge killed Swarm member",
            ));
        }
        let empty_context = MessageContext::default();
        let sequence = insert_board_message_rows(
            &tx,
            &BoardMessageInsert {
                message_id: command.message_id,
                workspace_id: command.workspace_id,
                swarm_run_id: command.swarm_run_id,
                channel_id: command.channel_id,
                body: &command.body,
                context: &empty_context,
                submitted_at_ms: command.observed_at_ms,
                author_kind: 3,
                author_display_name: "System",
                author_member_id: None,
                author_epoch: None,
            },
        )?;
        enqueue_doorbell_and_wake(
            &tx,
            command.swarm_run_id,
            command.target_member_id,
            sequence,
            command.reason,
            command.observed_at_ms,
        )?;
        if command.reason == SwarmDoorbellReason::SystemStall {
            tx.execute(
                INCREMENT_SWARM_RUN_STALL_NUDGE_SQL,
                params![
                    command.workspace_id.into_uuid().as_bytes(),
                    command.swarm_run_id.as_bytes(),
                    command.observed_at_ms,
                ],
            )
            .map_err(OperationsError::from)?;
        }
        let updated_run = require_run_by_id(&tx, command.workspace_id, command.swarm_run_id)?;
        let message = load_swarm_message_by_id(&tx, command.message_id)?
            .ok_or(OperationsError::Corrupt("inserted stall message missing"))?;
        tx.commit().map_err(OperationsError::from)?;
        Ok(SwarmStallNudgeOutcome {
            run: updated_run,
            message: Some(message),
            escalated_to_aborted: false,
        })
    }

    /// Stop non-working looping members (`Waiting`/`WakeQueued`/`SuspendedQuota` -> `Stopped`)
    /// to halt ping-pong auto-wakeups and enqueue a `[livelock]` doorbell to the `Coordinator`.
    ///
    /// # Errors
    /// Rejects invalid identities, terminal runs, non-`Coordinator` escalation targets,
    /// `Working` looping members (which must finish/fail their snapshot first), or storage errors.
    pub fn break_swarm_livelock(
        &mut self,
        command: &BreakSwarmLivelockCommand,
    ) -> Result<BreakSwarmLivelockOutcome, SwarmError> {
        validate_non_nil_uuid(command.message_id, "swarm message_id")?;
        validate_workspace_id(command.workspace_id)?;
        validate_non_nil_uuid(command.swarm_run_id, "swarm_run_id")?;
        validate_non_nil_uuid(command.channel_id, "swarm channel_id")?;
        validate_non_nil_uuid(command.coordinator_member_id, "swarm coordinator_member_id")?;
        if command.looping_member_ids.is_empty() {
            return Err(OperationsError::Bounds("swarm looping_member_ids").into());
        }
        for &id in &command.looping_member_ids {
            validate_non_nil_uuid(id, "swarm looping_member_id")?;
            if id == command.coordinator_member_id {
                return Err(OperationsError::Conflict(
                    "coordinator cannot be stopped as a looping worker pair",
                )
                .into());
            }
        }
        validate_chat_text(&command.body)?;
        check_timestamp_non_negative(command.observed_at_ms, "swarm observed_at_ms")?;

        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(OperationsError::from)?;
        let run = require_run_by_id(&tx, command.workspace_id, command.swarm_run_id)?;
        if run.state.is_terminal() {
            return Err(SwarmError::InvalidStateTransition(
                "cannot break livelock on terminal Swarm run",
            ));
        }
        let channel = require_channel_by_id(&tx, command.swarm_run_id, command.channel_id)?;
        if channel.workspace_id != command.workspace_id {
            return Err(SwarmError::NotFound("swarm channel in workspace"));
        }
        let coordinator =
            require_member_by_id(&tx, command.swarm_run_id, command.coordinator_member_id)?;
        if coordinator.role_kind != SwarmRoleKind::Coordinator
            || coordinator.member_state == SwarmMemberState::Killed
        {
            return Err(SwarmError::InvalidStateTransition(
                "livelock escalation target must be active Coordinator",
            ));
        }

        let stopped_members = stop_looping_members_in_tx(
            &tx,
            command.swarm_run_id,
            &command.looping_member_ids,
            command.observed_at_ms,
        )?;
        let escalation_message = enqueue_livelock_escalation_in_tx(&tx, command)?;
        tx.commit().map_err(OperationsError::from)?;
        Ok(BreakSwarmLivelockOutcome {
            stopped_members,
            escalation_message,
        })
    }
}

struct BoardMessageInsert<'a> {
    message_id: Uuid,
    workspace_id: WorkspaceId,
    swarm_run_id: Uuid,
    channel_id: Uuid,
    body: &'a str,
    context: &'a MessageContext,
    submitted_at_ms: i64,
    author_kind: i64,
    author_display_name: &'a str,
    author_member_id: Option<Uuid>,
    author_epoch: Option<u64>,
}

fn insert_board_message_rows(
    tx: &Transaction<'_>,
    input: &BoardMessageInsert<'_>,
) -> Result<u64, SwarmError> {
    let current_seq_i64: i64 = tx
        .query_row(
            SELECT_NEXT_WORKSPACE_MESSAGE_SEQUENCE_SQL,
            params![input.workspace_id.into_uuid().as_bytes()],
            |row| row.get(0),
        )
        .map_err(OperationsError::from)?;
    let current_seq = decode_non_negative_u64(current_seq_i64, "workspace message sequence")?;
    let sequence = current_seq
        .checked_add(1)
        .ok_or(OperationsError::Bounds("workspace message sequence"))?;
    let seq_i64 = crate::catalog::signed(sequence)?;
    let epoch_i64 = input.author_epoch.map(crate::catalog::signed).transpose()?;
    let activity_id = input
        .context
        .activity_id()
        .map(forge_domain::ActivityId::into_uuid);
    let goal_id = input.context.goal_id().map(forge_domain::GoalId::into_uuid);
    let node_id = input
        .context
        .node_id()
        .map(forge_domain::DagNodeId::into_uuid);
    tx.execute(
        INSERT_SWARM_WORKSPACE_MESSAGE_SQL,
        params![
            input.message_id.as_bytes(),
            input.workspace_id.into_uuid().as_bytes(),
            input.author_kind,
            input.author_display_name,
            input.body,
            seq_i64,
            input.submitted_at_ms,
            activity_id.as_ref().map(Uuid::as_bytes),
            goal_id.as_ref().map(Uuid::as_bytes),
            input.context.dag_revision().map(DagRevisionNumber::get),
            node_id.as_ref().map(Uuid::as_bytes),
            input
                .context
                .goal_created_at()
                .map(forge_domain::TimestampMillis::get),
            input
                .context
                .dag_base_goal_revision()
                .map(forge_domain::GoalRevision::get),
        ],
    )
    .map_err(|err| map_sqlite_conflict(err, "swarm workspace message insert"))?;
    tx.execute(
        INSERT_SWARM_CHANNEL_MESSAGE_SQL,
        params![
            input.workspace_id.into_uuid().as_bytes(),
            seq_i64,
            input.swarm_run_id.as_bytes(),
            input.channel_id.as_bytes(),
            input.author_member_id.as_ref().map(Uuid::as_bytes),
            epoch_i64,
        ],
    )
    .map_err(|err| map_sqlite_conflict(err, "swarm channel message insert"))?;
    Ok(sequence)
}

fn enqueue_doorbell_and_wake(
    tx: &Transaction<'_>,
    swarm_run_id: Uuid,
    target_member_id: Uuid,
    sequence: u64,
    reason: SwarmDoorbellReason,
    observed_at_ms: i64,
) -> Result<bool, SwarmError> {
    tx.execute(
        INSERT_SWARM_DOORBELL_SQL,
        params![
            swarm_run_id.as_bytes(),
            target_member_id.as_bytes(),
            crate::catalog::signed(sequence)?,
            reason.as_str(),
            observed_at_ms,
        ],
    )
    .map_err(OperationsError::from)?;
    let transitioned = tx
        .execute(
            UPDATE_SWARM_MEMBER_WAKE_QUEUED_SQL,
            params![
                swarm_run_id.as_bytes(),
                target_member_id.as_bytes(),
                observed_at_ms
            ],
        )
        .map_err(OperationsError::from)?;
    Ok(transitioned == 1)
}

fn resolve_message_author(
    tx: &Transaction<'_>,
    run: &SwarmRunRecord,
    author: SwarmMessageAuthor,
) -> Result<(Option<SwarmMemberRecord>, i64, Option<u64>), SwarmError> {
    match author {
        SwarmMessageAuthor::System => Ok((None, 3, None)),
        SwarmMessageAuthor::Member(member_id) => {
            let member = require_member_by_id(tx, run.swarm_run_id, member_id)?;
            if member.member_state == SwarmMemberState::Killed {
                return Err(SwarmError::InvalidStateTransition(
                    "killed member cannot post board messages",
                ));
            }
            let epoch = member.attempt_epoch;
            Ok((Some(member), 0, Some(epoch)))
        }
    }
}

enum ParsedMention<'a> {
    All,
    Channel(&'a str),
    Member(&'a str),
}

fn parse_mentions(body: &str) -> Vec<ParsedMention<'_>> {
    let bytes = body.as_bytes();
    let mut mentions = Vec::new();
    let mut idx = 0;
    while idx < bytes.len() {
        if bytes[idx] != b'@' {
            idx += 1;
            continue;
        }
        let valid_prefix = idx == 0 || {
            let prev = bytes[idx - 1];
            !prev.is_ascii_alphanumeric() && prev != b'_' && prev != b'-'
        };
        if !valid_prefix {
            idx += 1;
            continue;
        }
        if idx + 1 < bytes.len() && bytes[idx + 1] == b'#' {
            let start = idx + 2;
            let end = scan_ident(bytes, start);
            if end > start && end - start <= MAX_SWARM_CHANNEL_NAME_BYTES {
                mentions.push(ParsedMention::Channel(&body[start..end]));
            }
            idx = end.max(idx + 2);
            continue;
        }
        let start = idx + 1;
        let end = scan_ident(bytes, start);
        if end > start {
            let token = &body[start..end];
            if token == "all" {
                mentions.push(ParsedMention::All);
            } else if token.len() <= MAX_SWARM_ROSTER_NAME_BYTES {
                mentions.push(ParsedMention::Member(token));
            }
        }
        idx = end.max(idx + 1);
    }
    mentions
}

fn scan_ident(bytes: &[u8], start: usize) -> usize {
    if start >= bytes.len() {
        return start;
    }
    let first = bytes[start];
    if !(first.is_ascii_lowercase() || first.is_ascii_digit()) {
        return start;
    }
    let mut end = start + 1;
    while end < bytes.len() {
        let ch = bytes[end];
        if ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == b'-' || ch == b'_' {
            end += 1;
        } else {
            break;
        }
    }
    end
}

fn resolve_mention_targets(
    tx: &Transaction<'_>,
    run: &SwarmRunRecord,
    author: Option<&SwarmMemberRecord>,
    body: &str,
) -> Result<(BTreeMap<Uuid, SwarmDoorbellReason>, BTreeSet<Uuid>), SwarmError> {
    let parsed = parse_mentions(body);
    let mut targets: BTreeMap<Uuid, SwarmDoorbellReason> = BTreeMap::new();
    let mut direct_targets: BTreeSet<Uuid> = BTreeSet::new();
    let mut checked_all = false;
    let mut seen_channels: BTreeSet<&str> = BTreeSet::new();
    let mut seen_members: BTreeSet<&str> = BTreeSet::new();
    let author_id = author.map(SwarmMemberRecord::member_id);

    for mention in parsed {
        match mention {
            ParsedMention::All => {
                if checked_all {
                    continue;
                }
                checked_all = true;
                if let Some(member) = author
                    && member.role_kind != SwarmRoleKind::Coordinator
                    && !run.allow_worker_all_mention
                {
                    return Err(SwarmError::WorkerAllMentionForbidden);
                }
                for candidate in load_members_by_run(tx, run.swarm_run_id)? {
                    if candidate.member_state != SwarmMemberState::Killed
                        && Some(candidate.member_id) != author_id
                    {
                        upsert_target_reason(
                            &mut targets,
                            candidate.member_id,
                            SwarmDoorbellReason::AllMention,
                        );
                    }
                }
            }
            ParsedMention::Channel(channel_name) => {
                if !seen_channels.insert(channel_name) {
                    continue;
                }
                if let Some(channel) = load_channel_by_name(tx, run.swarm_run_id, channel_name)? {
                    let subscriber_ids = load_active_channel_subscriber_ids(
                        tx,
                        run.swarm_run_id,
                        channel.channel_id,
                    )?;
                    for subscriber_id in subscriber_ids {
                        if Some(subscriber_id) != author_id {
                            upsert_target_reason(
                                &mut targets,
                                subscriber_id,
                                SwarmDoorbellReason::ChannelMention,
                            );
                        }
                    }
                }
            }
            ParsedMention::Member(roster_name) => {
                if !seen_members.insert(roster_name) {
                    continue;
                }
                if let Some(member) = load_member_by_roster(tx, run.swarm_run_id, roster_name)?
                    && member.member_state != SwarmMemberState::Killed
                    && Some(member.member_id) != author_id
                {
                    upsert_target_reason(
                        &mut targets,
                        member.member_id,
                        SwarmDoorbellReason::DirectMention,
                    );
                    direct_targets.insert(member.member_id);
                }
            }
        }
    }
    Ok((targets, direct_targets))
}

fn upsert_target_reason(
    targets: &mut BTreeMap<Uuid, SwarmDoorbellReason>,
    target_id: Uuid,
    reason: SwarmDoorbellReason,
) {
    targets
        .entry(target_id)
        .and_modify(|existing| {
            if reason.precedence() > existing.precedence() {
                *existing = reason;
            }
        })
        .or_insert(reason);
}

fn dispatch_doorbells_and_ping_pong(
    tx: &Transaction<'_>,
    run: &SwarmRunRecord,
    author: Option<&SwarmMemberRecord>,
    doorbells: &BTreeMap<Uuid, SwarmDoorbellReason>,
    direct_targets: &BTreeSet<Uuid>,
    sequence: u64,
    submitted_at_ms: i64,
) -> Result<(Vec<Uuid>, Vec<Uuid>), SwarmError> {
    let mut woken = Vec::new();
    let mut coalesced = Vec::new();
    for (&target_id, &reason) in doorbells {
        if enqueue_doorbell_and_wake(
            tx,
            run.swarm_run_id,
            target_id,
            sequence,
            reason,
            submitted_at_ms,
        )? {
            woken.push(target_id);
        } else {
            coalesced.push(target_id);
        }
    }

    if let Some(author_member) = author
        && direct_targets.len() == 1
        && let Some(&peer_id) = direct_targets.iter().next()
    {
        let next_streak = if author_member.ping_pong_peer_id == Some(peer_id) {
            author_member.ping_pong_streak.saturating_add(1)
        } else {
            1
        };
        tx.execute(
            UPDATE_SWARM_MEMBER_PING_PONG_SQL,
            params![
                run.swarm_run_id.as_bytes(),
                author_member.member_id.as_bytes(),
                peer_id.as_bytes(),
                i64::from(next_streak),
                submitted_at_ms,
            ],
        )
        .map_err(OperationsError::from)?;
    }

    Ok((woken, coalesced))
}

fn ensure_message_replay_matches(
    existing: &SwarmBoardMessage,
    command: &PostSwarmMessageCommand,
) -> Result<(), SwarmError> {
    let expected_author = match command.author {
        SwarmMessageAuthor::Member(id) => Some(id),
        SwarmMessageAuthor::System => None,
    };
    if existing.workspace_id != command.workspace_id
        || existing.swarm_run_id != command.swarm_run_id
        || existing.channel_id != command.channel_id
        || existing.author_member_id != expected_author
        || existing.body != command.body
    {
        return Err(
            OperationsError::Conflict("swarm message_id reused with different payload").into(),
        );
    }
    Ok(())
}

fn stop_looping_members_in_tx(
    tx: &Transaction<'_>,
    swarm_run_id: Uuid,
    looping_member_ids: &[Uuid],
    observed_at_ms: i64,
) -> Result<Vec<SwarmMemberRecord>, SwarmError> {
    let mut unique_ids = looping_member_ids.to_vec();
    unique_ids.sort_unstable();
    unique_ids.dedup();
    let mut stopped_members = Vec::with_capacity(unique_ids.len());
    for member_id in unique_ids {
        let member = require_member_by_id(tx, swarm_run_id, member_id)?;
        if member.member_state == SwarmMemberState::Working {
            return Err(SwarmError::InvalidStateTransition(
                "cannot livelock-stop Working member with active snapshot",
            ));
        }
        if matches!(
            member.member_state,
            SwarmMemberState::Waiting
                | SwarmMemberState::WakeQueued
                | SwarmMemberState::SuspendedQuota
        ) {
            tx.execute(
                UPDATE_SWARM_MEMBER_STOP_SQL,
                params![
                    swarm_run_id.as_bytes(),
                    member_id.as_bytes(),
                    observed_at_ms
                ],
            )
            .map_err(OperationsError::from)?;
        }
        stopped_members.push(require_member_by_id(tx, swarm_run_id, member_id)?);
    }
    Ok(stopped_members)
}

fn enqueue_livelock_escalation_in_tx(
    tx: &Transaction<'_>,
    command: &BreakSwarmLivelockCommand,
) -> Result<SwarmBoardMessage, SwarmError> {
    let empty_context = MessageContext::default();
    let sequence = insert_board_message_rows(
        tx,
        &BoardMessageInsert {
            message_id: command.message_id,
            workspace_id: command.workspace_id,
            swarm_run_id: command.swarm_run_id,
            channel_id: command.channel_id,
            body: &command.body,
            context: &empty_context,
            submitted_at_ms: command.observed_at_ms,
            author_kind: 3,
            author_display_name: "System",
            author_member_id: None,
            author_epoch: None,
        },
    )?;
    enqueue_doorbell_and_wake(
        tx,
        command.swarm_run_id,
        command.coordinator_member_id,
        sequence,
        SwarmDoorbellReason::SystemLivelock,
        command.observed_at_ms,
    )?;
    load_swarm_message_by_id(tx, command.message_id)?
        .ok_or(OperationsError::Corrupt("inserted livelock message missing").into())
}
