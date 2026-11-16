//! Swarm roster membership, spawn hierarchy guards, two-phase watermarks,
//! coalesced wakeup batch claiming, turn settlement, and quota profile switching.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use forge_domain::WorkspaceId;
use rusqlite::{Transaction, TransactionBehavior, params};
use uuid::Uuid;

use super::super::sql::{
    COUNT_ACTIVE_SWARM_MEMBERS_SQL, COUNT_TOTAL_SWARM_MEMBERS_SQL, COUNT_WORKING_SWARM_MEMBERS_SQL,
    DELETE_CONSUMED_SWARM_DOORBELLS_SQL, EXISTS_PENDING_SWARM_DOORBELL_AFTER_SQL,
    INCREMENT_SWARM_RUN_TURNS_SQL, INSERT_SWARM_CHANNEL_SUBSCRIPTION_SQL, INSERT_SWARM_MEMBER_SQL,
    SELECT_SWARM_WAKEUP_MESSAGES_SQL, SELECT_WAKE_QUEUED_SWARM_MEMBERS_SQL,
    UPDATE_SWARM_MEMBER_CLAIM_WORKING_SQL, UPDATE_SWARM_MEMBER_COMPLETE_TURN_SQL,
    UPDATE_SWARM_MEMBER_FAIL_OR_SUSPEND_SQL, UPDATE_SWARM_MEMBER_KILL_SQL,
    UPDATE_SWARM_MEMBER_PROFILE_REQUEUE_SQL,
};
use super::super::{
    MAX_SNAPSHOT_COALESCED_BYTES, MAX_SNAPSHOT_COALESCED_MESSAGES, MAX_SWARM_MEMBERS_PER_RUN,
    MAX_SWARM_SPAWN_DEPTH, SWARM_MAIN_CHANNEL_NAME, SwarmCommitOid, SwarmError,
    check_timestamp_non_negative, map_sqlite_conflict, validate_non_nil_uuid,
    validate_non_zero_digest, validate_workspace_id,
};
use super::codecs::{
    decode_board_message_row, decode_member_row, load_channel_by_name, load_member_by_id,
    load_members_by_run, read_raw_board_message_row, read_raw_member_row, require_member_by_id,
    require_run_by_id,
};
use super::{
    AddSwarmMemberCommand, ClaimSwarmWakeupCommand, CompleteSwarmTurnCommand,
    FailOrSuspendSwarmTurnCommand, KillSwarmMemberCommand, SwarmBoardMessage, SwarmMemberRecord,
    SwarmMemberState, SwarmRoleKind, SwarmRunRecord, SwarmTurnFailureDisposition, SwarmTurnOutput,
    SwarmWakeupBatch, SwarmWorkspaceMode, SwitchSwarmMemberProfileCommand,
};
use crate::sqlite_codecs::{decode_non_negative_u64, encode_principal};
use crate::{ForgeWorktreeRef, OperationsCatalog, OperationsError};

impl OperationsCatalog {
    /// Add a member to a Swarm run, enforcing `max_members`, `max_spawn_depth <= 4`,
    /// worker-to-`Helper`-only spawn authority, and `Verifier` `Inherit`-only workspace mode.
    ///
    /// # Errors
    /// Fails closed with [`SwarmError::WorkerVerifierSpawnForbidden`],
    /// [`SwarmError::VerifierMustInherit`], [`SwarmError::SpawnDepthExceeded`],
    /// [`SwarmError::MaxMembersExceeded`], or [`OperationsError`].
    pub fn add_swarm_member(
        &mut self,
        command: &AddSwarmMemberCommand,
    ) -> Result<SwarmMemberRecord, SwarmError> {
        command.validate()?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(OperationsError::from)?;
        let run = require_run_by_id(&tx, command.workspace_id, command.swarm_run_id)?;
        if run.state.is_terminal() {
            return Err(SwarmError::InvalidStateTransition(
                "cannot add member to terminal Swarm run",
            ));
        }
        if command.added_at_ms < run.created_at_ms {
            return Err(
                OperationsError::Bounds("swarm member added_at_ms precedes run creation").into(),
            );
        }
        if let Some(existing) = load_member_by_id(&tx, command.swarm_run_id, command.member_id)? {
            ensure_member_replay_matches(&existing, command)?;
            tx.commit().map_err(OperationsError::from)?;
            return Ok(existing);
        }
        if command.role_kind == SwarmRoleKind::Verifier
            && command.workspace_mode != SwarmWorkspaceMode::Inherit
        {
            return Err(SwarmError::VerifierMustInherit);
        }
        let spawn_depth = resolve_and_validate_spawn_parent(&tx, &run, command)?;
        enforce_member_capacity(&tx, &run)?;
        let (principal_kind, principal_id) = encode_principal(command.identity.principal());
        tx.execute(
            INSERT_SWARM_MEMBER_SQL,
            params![
                command.member_id.as_bytes(),
                command.swarm_run_id.as_bytes(),
                command.workspace_id.into_uuid().as_bytes(),
                command.roster_name.as_str(),
                command.role_kind.as_str(),
                command.parent_member_id.as_ref().map(Uuid::as_bytes),
                i64::from(spawn_depth),
                command.workspace_mode.as_str(),
                command.worktree_path.as_ref().map(ForgeWorktreeRef::as_str),
                command.identity.profile_id().into_uuid().as_bytes(),
                command.identity.participant_id().as_bytes(),
                principal_kind,
                principal_id.as_ref().map(Uuid::as_bytes),
                command.added_at_ms,
            ],
        )
        .map_err(|err| map_sqlite_conflict(err, "swarm member duplicate roster_name or role"))?;

        subscribe_initial_channels(&tx, command)?;
        let member = load_member_by_id(&tx, command.swarm_run_id, command.member_id)?
            .ok_or(OperationsError::Corrupt("inserted swarm member missing"))?;
        tx.commit().map_err(OperationsError::from)?;
        Ok(member)
    }

    /// Kill a Swarm member and cascade-cancel all descendant helpers in its spawn subtree.
    ///
    /// # Errors
    /// Rejects invalid identities, missing runs/members, or storage errors.
    pub fn kill_swarm_member(
        &mut self,
        command: KillSwarmMemberCommand,
    ) -> Result<Vec<SwarmMemberRecord>, SwarmError> {
        validate_workspace_id(command.workspace_id)?;
        validate_non_nil_uuid(command.swarm_run_id, "swarm_run_id")?;
        validate_non_nil_uuid(command.member_id, "swarm member_id")?;
        check_timestamp_non_negative(command.killed_at_ms, "swarm killed_at_ms")?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(OperationsError::from)?;
        let run = require_run_by_id(&tx, command.workspace_id, command.swarm_run_id)?;
        if command.killed_at_ms < run.created_at_ms {
            return Err(OperationsError::Bounds("swarm killed_at_ms precedes run creation").into());
        }
        let target = require_member_by_id(&tx, command.swarm_run_id, command.member_id)?;
        if target.workspace_id != command.workspace_id {
            return Err(SwarmError::NotFound("swarm member in workspace"));
        }
        let all_members = load_members_by_run(&tx, command.swarm_run_id)?;
        let subtree_ids = collect_descendant_subtree(command.member_id, &all_members);
        let mut killed = Vec::with_capacity(subtree_ids.len());
        for member_id in subtree_ids {
            tx.execute(
                UPDATE_SWARM_MEMBER_KILL_SQL,
                params![
                    command.swarm_run_id.as_bytes(),
                    member_id.as_bytes(),
                    command.killed_at_ms
                ],
            )
            .map_err(OperationsError::from)?;
            let updated = require_member_by_id(&tx, command.swarm_run_id, member_id)?;
            killed.push(updated);
        }
        tx.commit().map_err(OperationsError::from)?;
        Ok(killed)
    }

    /// List members currently in `WakeQueued` state for a Swarm run.
    ///
    /// # Errors
    /// Rejects invalid identities, missing runs, or corrupt stored rows.
    pub fn list_wake_queued_swarm_members(
        &self,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
    ) -> Result<Vec<SwarmMemberRecord>, SwarmError> {
        validate_workspace_id(workspace_id)?;
        validate_non_nil_uuid(swarm_run_id, "swarm_run_id")?;
        let tx = self
            .connection
            .unchecked_transaction()
            .map_err(OperationsError::from)?;
        let _run = require_run_by_id(&tx, workspace_id, swarm_run_id)?;
        let limit = i64::try_from(MAX_SWARM_MEMBERS_PER_RUN)
            .map_err(|_| OperationsError::Bounds("swarm member query limit"))?;
        let mut stmt = tx
            .prepare(SELECT_WAKE_QUEUED_SWARM_MEMBERS_SQL)
            .map_err(OperationsError::from)?;
        let raw_rows = stmt
            .query_map(params![swarm_run_id.as_bytes(), limit], read_raw_member_row)
            .map_err(OperationsError::from)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(OperationsError::from)?;
        drop(stmt);
        let members = raw_rows
            .into_iter()
            .map(decode_member_row)
            .collect::<Result<Vec<_>, _>>()?;
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(members)
    }

    /// Claim a coalesced wakeup message batch (`<= 32` messages and `<= 64 KiB` body bytes),
    /// bind `active_snapshot_id` and `inflight_target_seq`, and transition `WakeQueued -> Working`.
    ///
    /// # Errors
    /// Rejects non-`WakeQueued` members, terminal runs, exhausted turn budgets, or storage errors.
    pub fn claim_swarm_wakeup_batch(
        &mut self,
        command: ClaimSwarmWakeupCommand,
    ) -> Result<SwarmWakeupBatch, SwarmError> {
        validate_workspace_id(command.workspace_id)?;
        validate_non_nil_uuid(command.swarm_run_id, "swarm_run_id")?;
        validate_non_nil_uuid(command.member_id, "swarm member_id")?;
        validate_non_nil_uuid(command.snapshot_id, "swarm active_snapshot_id")?;
        check_timestamp_non_negative(command.claimed_at_ms, "swarm claimed_at_ms")?;

        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(OperationsError::from)?;
        let run = require_run_by_id(&tx, command.workspace_id, command.swarm_run_id)?;
        if run.state.is_terminal() {
            return Err(SwarmError::InvalidStateTransition(
                "cannot claim wakeup on terminal Swarm run",
            ));
        }
        if command.claimed_at_ms < run.created_at_ms {
            return Err(
                OperationsError::Bounds("swarm claimed_at_ms precedes run creation").into(),
            );
        }
        let working_count = count_working_members(&tx, command.swarm_run_id)?;
        if run.total_turns_used.saturating_add(working_count) >= run.max_total_turns {
            return Err(SwarmError::TurnBudgetExceeded {
                used: run.total_turns_used,
                max_turns: run.max_total_turns,
            });
        }
        let member = require_member_by_id(&tx, command.swarm_run_id, command.member_id)?;
        if member.workspace_id != command.workspace_id {
            return Err(SwarmError::NotFound("swarm member in workspace"));
        }
        if member.member_state != SwarmMemberState::WakeQueued
            || member.active_snapshot_id.is_some()
        {
            return Err(SwarmError::InvalidStateTransition(
                "only WakeQueued members without an active snapshot can claim a wakeup batch",
            ));
        }
        let (messages, total_body_bytes) = load_coalesced_wakeup_messages(
            &tx,
            command.swarm_run_id,
            member.committed_seen_seq,
            command.member_id,
        )?;
        let Some(last_msg) = messages.last() else {
            return Err(OperationsError::Corrupt(
                "wake-queued swarm member has zero unread relevant messages",
            )
            .into());
        };
        let inflight_target_seq = last_msg.sequence;
        if inflight_target_seq <= member.committed_seen_seq {
            return Err(OperationsError::Corrupt("swarm inflight_target_seq").into());
        }
        let changed = tx
            .execute(
                UPDATE_SWARM_MEMBER_CLAIM_WORKING_SQL,
                params![
                    command.swarm_run_id.as_bytes(),
                    command.member_id.as_bytes(),
                    command.snapshot_id.as_bytes(),
                    crate::catalog::signed(inflight_target_seq)?,
                    command.claimed_at_ms,
                ],
            )
            .map_err(OperationsError::from)?;
        if changed != 1 {
            return Err(SwarmError::InvalidStateTransition(
                "swarm wakeup claim CAS mismatch",
            ));
        }
        let updated_member = require_member_by_id(&tx, command.swarm_run_id, command.member_id)?;
        tx.commit().map_err(OperationsError::from)?;
        Ok(SwarmWakeupBatch {
            swarm_run_id: command.swarm_run_id,
            member: updated_member,
            snapshot_id: command.snapshot_id,
            committed_seen_seq: member.committed_seen_seq,
            inflight_target_seq,
            messages,
            total_body_bytes,
        })
    }

    /// Cleanly complete an active Swarm member turn: advances `committed_seen_seq = inflight_target_seq`,
    /// clears `active_snapshot_id`, records output commit/tree (for `Share`/`Branch`), resets
    /// `ping_pong_streak` if `output_tree_blake3` changed, and transitions to `WakeQueued` if new
    /// doorbells arrived while `Working` (or `Waiting` otherwise).
    ///
    /// # Errors
    /// Rejects non-`Working` members, snapshot mismatches, output emitted by `Inherit` members,
    /// exhausted turn budgets, or storage failures.
    pub fn complete_swarm_member_turn(
        &mut self,
        command: &CompleteSwarmTurnCommand,
    ) -> Result<SwarmMemberRecord, SwarmError> {
        validate_workspace_id(command.workspace_id)?;
        validate_non_nil_uuid(command.swarm_run_id, "swarm_run_id")?;
        validate_non_nil_uuid(command.member_id, "swarm member_id")?;
        validate_non_nil_uuid(command.expected_snapshot_id, "swarm expected_snapshot_id")?;
        check_timestamp_non_negative(command.completed_at_ms, "swarm completed_at_ms")?;
        if let Some(output) = &command.output {
            validate_non_zero_digest(output.tree_blake3, "swarm output tree_blake3")?;
        }

        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(OperationsError::from)?;
        let run = require_run_by_id(&tx, command.workspace_id, command.swarm_run_id)?;
        if command.completed_at_ms < run.created_at_ms {
            return Err(
                OperationsError::Bounds("swarm completed_at_ms precedes run creation").into(),
            );
        }
        let member = require_member_by_id(&tx, command.swarm_run_id, command.member_id)?;
        if member.workspace_id != command.workspace_id {
            return Err(SwarmError::NotFound("swarm member in workspace"));
        }
        if member.member_state != SwarmMemberState::Working
            || member.active_snapshot_id != Some(command.expected_snapshot_id)
        {
            return Err(SwarmError::InvalidStateTransition(
                "member is not Working under expected_snapshot_id",
            ));
        }
        if member.workspace_mode == SwarmWorkspaceMode::Inherit && command.output.is_some() {
            return Err(OperationsError::Conflict(
                "Inherit workspace mode member cannot emit output commit",
            )
            .into());
        }
        let new_committed_seq = member.inflight_target_seq;
        let next_state = resolve_post_turn_member_state(
            &tx,
            command.swarm_run_id,
            command.member_id,
            new_committed_seq,
        )?;
        let (next_commit_oid, next_tree_blake3, next_peer_id, next_streak) =
            resolve_turn_output_and_streak(&member, command.output.as_ref());
        increment_run_turns_in_tx(&tx, &run, command.completed_at_ms)?;
        let changed = tx
            .execute(
                UPDATE_SWARM_MEMBER_COMPLETE_TURN_SQL,
                params![
                    command.swarm_run_id.as_bytes(),
                    command.member_id.as_bytes(),
                    command.expected_snapshot_id.as_bytes(),
                    next_state.as_str(),
                    crate::catalog::signed(new_committed_seq)?,
                    next_peer_id.as_ref().map(Uuid::as_bytes),
                    next_streak,
                    next_commit_oid,
                    next_tree_blake3,
                    command.completed_at_ms,
                ],
            )
            .map_err(OperationsError::from)?;
        if changed != 1 {
            return Err(SwarmError::InvalidStateTransition(
                "complete_swarm_member_turn CAS mismatch",
            ));
        }
        let updated = require_member_by_id(&tx, command.swarm_run_id, command.member_id)?;
        tx.commit().map_err(OperationsError::from)?;
        Ok(updated)
    }

    /// Release an in-flight Swarm member turn on crash, timeout, quota exhaustion, or stop.
    /// Preserves `committed_seen_seq` unchanged so zero unread messages are lost.
    ///
    /// # Errors
    /// Rejects non-`Working` members, snapshot mismatches, exhausted turn budgets (when `consume_turn`),
    /// or storage errors.
    pub fn fail_or_suspend_swarm_member_turn(
        &mut self,
        command: FailOrSuspendSwarmTurnCommand,
    ) -> Result<SwarmMemberRecord, SwarmError> {
        validate_workspace_id(command.workspace_id)?;
        validate_non_nil_uuid(command.swarm_run_id, "swarm_run_id")?;
        validate_non_nil_uuid(command.member_id, "swarm member_id")?;
        validate_non_nil_uuid(command.expected_snapshot_id, "swarm expected_snapshot_id")?;
        check_timestamp_non_negative(command.observed_at_ms, "swarm observed_at_ms")?;

        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(OperationsError::from)?;
        let run = require_run_by_id(&tx, command.workspace_id, command.swarm_run_id)?;
        if command.observed_at_ms < run.created_at_ms {
            return Err(
                OperationsError::Bounds("swarm observed_at_ms precedes run creation").into(),
            );
        }
        let member = require_member_by_id(&tx, command.swarm_run_id, command.member_id)?;
        if member.workspace_id != command.workspace_id {
            return Err(SwarmError::NotFound("swarm member in workspace"));
        }
        if member.member_state != SwarmMemberState::Working
            || member.active_snapshot_id != Some(command.expected_snapshot_id)
        {
            return Err(SwarmError::InvalidStateTransition(
                "member is not Working under expected_snapshot_id",
            ));
        }
        let next_state = match command.disposition {
            SwarmTurnFailureDisposition::QuotaExceeded => SwarmMemberState::SuspendedQuota,
            SwarmTurnFailureDisposition::Stopped => SwarmMemberState::Stopped,
            SwarmTurnFailureDisposition::TransientRetry => {
                if has_pending_doorbell_after(
                    &tx,
                    command.swarm_run_id,
                    command.member_id,
                    member.committed_seen_seq,
                )? {
                    SwarmMemberState::WakeQueued
                } else {
                    SwarmMemberState::Waiting
                }
            }
        };
        let turn_delta = i64::from(command.consume_turn);
        if command.consume_turn {
            increment_run_turns_in_tx(&tx, &run, command.observed_at_ms)?;
        }
        let changed = tx
            .execute(
                UPDATE_SWARM_MEMBER_FAIL_OR_SUSPEND_SQL,
                params![
                    command.swarm_run_id.as_bytes(),
                    command.member_id.as_bytes(),
                    command.expected_snapshot_id.as_bytes(),
                    next_state.as_str(),
                    turn_delta,
                    command.observed_at_ms,
                ],
            )
            .map_err(OperationsError::from)?;
        if changed != 1 {
            return Err(SwarmError::InvalidStateTransition(
                "fail_or_suspend_swarm_member_turn CAS mismatch",
            ));
        }
        let updated = require_member_by_id(&tx, command.swarm_run_id, command.member_id)?;
        tx.commit().map_err(OperationsError::from)?;
        Ok(updated)
    }

    /// Switch a `SuspendedQuota` member's `AgentProfileId` at an attempt boundary and requeue it
    /// to `WakeQueued` (if unconsumed doorbells exist) or `Waiting`, preserving `committed_seen_seq`.
    ///
    /// # Errors
    /// Rejects non-`SuspendedQuota` members, terminal runs, invalid IDs, or storage failures.
    pub fn switch_swarm_member_profile_and_requeue(
        &mut self,
        command: SwitchSwarmMemberProfileCommand,
    ) -> Result<SwarmMemberRecord, SwarmError> {
        validate_workspace_id(command.workspace_id)?;
        validate_non_nil_uuid(command.swarm_run_id, "swarm_run_id")?;
        validate_non_nil_uuid(command.member_id, "swarm member_id")?;
        validate_non_nil_uuid(command.new_profile_id.into_uuid(), "swarm new_profile_id")?;
        check_timestamp_non_negative(command.updated_at_ms, "swarm updated_at_ms")?;

        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(OperationsError::from)?;
        let run = require_run_by_id(&tx, command.workspace_id, command.swarm_run_id)?;
        if run.state.is_terminal() {
            return Err(SwarmError::InvalidStateTransition(
                "cannot requeue member on terminal Swarm run",
            ));
        }
        let member = require_member_by_id(&tx, command.swarm_run_id, command.member_id)?;
        if member.workspace_id != command.workspace_id {
            return Err(SwarmError::NotFound("swarm member in workspace"));
        }
        if member.member_state != SwarmMemberState::SuspendedQuota {
            return Err(SwarmError::InvalidStateTransition(
                "only SuspendedQuota members can switch profile and requeue",
            ));
        }
        let next_state = if has_pending_doorbell_after(
            &tx,
            command.swarm_run_id,
            command.member_id,
            member.committed_seen_seq,
        )? {
            SwarmMemberState::WakeQueued
        } else {
            SwarmMemberState::Waiting
        };
        let changed = tx
            .execute(
                UPDATE_SWARM_MEMBER_PROFILE_REQUEUE_SQL,
                params![
                    command.swarm_run_id.as_bytes(),
                    command.member_id.as_bytes(),
                    command.new_profile_id.into_uuid().as_bytes(),
                    next_state.as_str(),
                    command.updated_at_ms,
                ],
            )
            .map_err(OperationsError::from)?;
        if changed != 1 {
            return Err(SwarmError::InvalidStateTransition(
                "switch_swarm_member_profile_and_requeue CAS mismatch",
            ));
        }
        let updated = require_member_by_id(&tx, command.swarm_run_id, command.member_id)?;
        tx.commit().map_err(OperationsError::from)?;
        Ok(updated)
    }
}

fn has_pending_doorbell_after(
    tx: &Transaction<'_>,
    swarm_run_id: Uuid,
    member_id: Uuid,
    seq: u64,
) -> Result<bool, SwarmError> {
    tx.query_row(
        EXISTS_PENDING_SWARM_DOORBELL_AFTER_SQL,
        params![
            swarm_run_id.as_bytes(),
            member_id.as_bytes(),
            crate::catalog::signed(seq)?,
        ],
        |row| row.get(0),
    )
    .map_err(|err| SwarmError::from(OperationsError::from(err)))
}

fn resolve_post_turn_member_state(
    tx: &Transaction<'_>,
    swarm_run_id: Uuid,
    member_id: Uuid,
    new_committed_seq: u64,
) -> Result<SwarmMemberState, SwarmError> {
    let seq_i64 = crate::catalog::signed(new_committed_seq)?;
    tx.execute(
        DELETE_CONSUMED_SWARM_DOORBELLS_SQL,
        params![swarm_run_id.as_bytes(), member_id.as_bytes(), seq_i64],
    )
    .map_err(OperationsError::from)?;
    let has_more_doorbells =
        has_pending_doorbell_after(tx, swarm_run_id, member_id, new_committed_seq)?;
    Ok(if has_more_doorbells {
        SwarmMemberState::WakeQueued
    } else {
        SwarmMemberState::Waiting
    })
}

fn resolve_turn_output_and_streak<'a>(
    member: &'a SwarmMemberRecord,
    output: Option<&'a SwarmTurnOutput>,
) -> (Option<&'a str>, Option<&'a [u8]>, Option<Uuid>, i64) {
    let (next_commit_oid, next_tree_blake3, tree_changed) = match output {
        Some(out) => {
            let changed = member.last_output_tree_blake3 != Some(out.tree_blake3);
            (
                Some(out.commit_oid.as_str()),
                Some(out.tree_blake3.as_slice()),
                changed,
            )
        }
        None => (
            member
                .last_output_commit_oid
                .as_ref()
                .map(SwarmCommitOid::as_str),
            member
                .last_output_tree_blake3
                .as_ref()
                .map(<[u8; 32]>::as_slice),
            false,
        ),
    };
    let (next_peer_id, next_streak) = if tree_changed {
        (None, 0_i64)
    } else {
        (member.ping_pong_peer_id, i64::from(member.ping_pong_streak))
    };
    (next_commit_oid, next_tree_blake3, next_peer_id, next_streak)
}

fn increment_run_turns_in_tx(
    tx: &Transaction<'_>,
    run: &SwarmRunRecord,
    observed_at_ms: i64,
) -> Result<(), SwarmError> {
    let updated = tx
        .execute(
            INCREMENT_SWARM_RUN_TURNS_SQL,
            params![
                run.workspace_id.into_uuid().as_bytes(),
                run.swarm_run_id.as_bytes(),
                observed_at_ms,
            ],
        )
        .map_err(OperationsError::from)?;
    if updated != 1 {
        return Err(SwarmError::TurnBudgetExceeded {
            used: run.total_turns_used,
            max_turns: run.max_total_turns,
        });
    }
    Ok(())
}

fn resolve_and_validate_spawn_parent(
    tx: &Transaction<'_>,
    run: &SwarmRunRecord,
    command: &AddSwarmMemberCommand,
) -> Result<u8, SwarmError> {
    match command.parent_member_id {
        None => {
            if command.role_kind == SwarmRoleKind::Helper {
                return Err(SwarmError::InvalidStateTransition(
                    "Helper member requires parent_member_id",
                ));
            }
            Ok(0)
        }
        Some(parent_id) => {
            if command.role_kind == SwarmRoleKind::Coordinator {
                return Err(SwarmError::InvalidStateTransition(
                    "Coordinator member cannot have parent_member_id",
                ));
            }
            let parent = require_member_by_id(tx, run.swarm_run_id, parent_id)?;
            if matches!(
                parent.member_state,
                SwarmMemberState::Killed | SwarmMemberState::Stopped
            ) {
                return Err(SwarmError::InvalidStateTransition(
                    "killed or stopped member cannot spawn children",
                ));
            }
            match parent.role_kind {
                SwarmRoleKind::Coordinator => {}
                SwarmRoleKind::Worker | SwarmRoleKind::Helper => {
                    if command.role_kind != SwarmRoleKind::Helper {
                        return Err(SwarmError::WorkerVerifierSpawnForbidden);
                    }
                }
                SwarmRoleKind::Verifier | SwarmRoleKind::Monitor => {
                    return Err(SwarmError::WorkerVerifierSpawnForbidden);
                }
            }
            let next_depth = parent.spawn_depth.saturating_add(1);
            if next_depth > run.max_spawn_depth || next_depth > MAX_SWARM_SPAWN_DEPTH {
                return Err(SwarmError::SpawnDepthExceeded {
                    depth: next_depth,
                    max_depth: run.max_spawn_depth.min(MAX_SWARM_SPAWN_DEPTH),
                });
            }
            Ok(next_depth)
        }
    }
}

fn enforce_member_capacity(tx: &Transaction<'_>, run: &SwarmRunRecord) -> Result<(), SwarmError> {
    let active_raw: i64 = tx
        .query_row(
            COUNT_ACTIVE_SWARM_MEMBERS_SQL,
            params![run.swarm_run_id.as_bytes()],
            |row| row.get(0),
        )
        .map_err(OperationsError::from)?;
    let active = usize::try_from(active_raw)
        .map_err(|_| OperationsError::Corrupt("swarm active member count"))?;
    let max_members = usize::try_from(run.max_members)
        .map_err(|_| OperationsError::Corrupt("swarm max_members"))?;
    if active >= max_members {
        return Err(SwarmError::MaxMembersExceeded {
            current: active,
            max_members,
        });
    }
    let total_raw: i64 = tx
        .query_row(
            COUNT_TOTAL_SWARM_MEMBERS_SQL,
            params![run.swarm_run_id.as_bytes()],
            |row| row.get(0),
        )
        .map_err(OperationsError::from)?;
    let total = usize::try_from(total_raw)
        .map_err(|_| OperationsError::Corrupt("swarm total member count"))?;
    if total >= MAX_SWARM_MEMBERS_PER_RUN * 2 {
        return Err(OperationsError::Bounds("swarm historical member ceiling").into());
    }
    Ok(())
}

fn subscribe_initial_channels(
    tx: &Transaction<'_>,
    command: &AddSwarmMemberCommand,
) -> Result<(), SwarmError> {
    let main_channel = load_channel_by_name(tx, command.swarm_run_id, SWARM_MAIN_CHANNEL_NAME)?
        .ok_or(OperationsError::Corrupt("swarm main channel missing"))?;
    tx.execute(
        INSERT_SWARM_CHANNEL_SUBSCRIPTION_SQL,
        params![
            command.swarm_run_id.as_bytes(),
            main_channel.channel_id.as_bytes(),
            command.member_id.as_bytes(),
            command.added_at_ms,
        ],
    )
    .map_err(OperationsError::from)?;
    for channel_name in &command.initial_channels {
        let channel = load_channel_by_name(tx, command.swarm_run_id, channel_name)?
            .ok_or(SwarmError::NotFound("swarm initial channel"))?;
        tx.execute(
            INSERT_SWARM_CHANNEL_SUBSCRIPTION_SQL,
            params![
                command.swarm_run_id.as_bytes(),
                channel.channel_id.as_bytes(),
                command.member_id.as_bytes(),
                command.added_at_ms,
            ],
        )
        .map_err(OperationsError::from)?;
    }
    Ok(())
}

fn ensure_member_replay_matches(
    existing: &SwarmMemberRecord,
    command: &AddSwarmMemberCommand,
) -> Result<(), SwarmError> {
    if existing.workspace_id != command.workspace_id
        || existing.swarm_run_id != command.swarm_run_id
        || existing.roster_name != command.roster_name
        || existing.role_kind != command.role_kind
        || existing.parent_member_id != command.parent_member_id
        || existing.workspace_mode != command.workspace_mode
        || existing.worktree_path != command.worktree_path
        || existing.identity != command.identity
    {
        return Err(OperationsError::Conflict(
            "swarm member_id already exists with different specification",
        )
        .into());
    }
    Ok(())
}

fn collect_descendant_subtree(root_id: Uuid, all_members: &[SwarmMemberRecord]) -> Vec<Uuid> {
    let mut children_by_parent: BTreeMap<Uuid, Vec<Uuid>> = BTreeMap::new();
    for member in all_members {
        if let Some(parent_id) = member.parent_member_id {
            children_by_parent
                .entry(parent_id)
                .or_default()
                .push(member.member_id);
        }
    }
    let mut visited = BTreeSet::new();
    let mut queue = VecDeque::new();
    let mut ordered = Vec::new();
    visited.insert(root_id);
    queue.push_back(root_id);
    while let Some(current) = queue.pop_front() {
        ordered.push(current);
        if let Some(children) = children_by_parent.get(&current) {
            for &child in children {
                if visited.insert(child) {
                    queue.push_back(child);
                }
            }
        }
    }
    ordered
}

fn load_coalesced_wakeup_messages(
    tx: &Transaction<'_>,
    swarm_run_id: Uuid,
    committed_seen_seq: u64,
    member_id: Uuid,
) -> Result<(Vec<SwarmBoardMessage>, usize), SwarmError> {
    let limit = i64::try_from(MAX_SNAPSHOT_COALESCED_MESSAGES)
        .map_err(|_| OperationsError::Bounds("swarm wakeup coalesced limit"))?;
    let mut stmt = tx
        .prepare(SELECT_SWARM_WAKEUP_MESSAGES_SQL)
        .map_err(OperationsError::from)?;
    let raw_rows = stmt
        .query_map(
            params![
                swarm_run_id.as_bytes(),
                crate::catalog::signed(committed_seen_seq)?,
                member_id.as_bytes(),
                limit,
            ],
            read_raw_board_message_row,
        )
        .map_err(OperationsError::from)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(OperationsError::from)?;
    drop(stmt);

    let mut batch = Vec::with_capacity(raw_rows.len());
    let mut total_body_bytes = 0_usize;
    for raw in raw_rows {
        let msg = decode_board_message_row(raw)?;
        let body_len = msg.body.len();
        if !batch.is_empty()
            && total_body_bytes.saturating_add(body_len) > MAX_SNAPSHOT_COALESCED_BYTES
        {
            break;
        }
        total_body_bytes = total_body_bytes.saturating_add(body_len);
        batch.push(msg);
        if batch.len() >= MAX_SNAPSHOT_COALESCED_MESSAGES {
            break;
        }
    }
    Ok((batch, total_body_bytes))
}

fn count_working_members(tx: &Transaction<'_>, swarm_run_id: Uuid) -> Result<u64, SwarmError> {
    let raw: i64 = tx
        .query_row(
            COUNT_WORKING_SWARM_MEMBERS_SQL,
            params![swarm_run_id.as_bytes()],
            |row| row.get(0),
        )
        .map_err(OperationsError::from)?;
    decode_non_negative_u64(raw, "swarm working member count").map_err(SwarmError::from)
}
