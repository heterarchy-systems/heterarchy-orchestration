use std::collections::{BTreeMap, BTreeSet};

use forge_domain::WorkspaceId;
use heterarchy_orchestration_operations::{
    BreakSwarmLivelockCommand, BreakSwarmLivelockOutcome, MAX_SWARM_BOARD_PAGE_SIZE,
    OperationsCatalog, OperationsError, ReadSwarmBoardQuery, RecordSwarmStallNudgeCommand,
    SWARM_MAIN_CHANNEL_NAME, SwarmBoardMessage, SwarmChannelRecord, SwarmDoorbellReason,
    SwarmError, SwarmMemberRecord, SwarmMemberState, SwarmRoleKind, SwarmRunSnapshot,
    SwarmRunState, SwarmStallNudgeOutcome, SwarmVerdictRecord,
};
use uuid::Uuid;

use super::{
    DEFAULT_MAX_MEMBER_TURNS, DEFAULT_MAX_PING_PONG_STREAK, DEFAULT_MAX_STALL_NUDGES,
    DEFAULT_STALL_GRACE_MS,
};

/// Bounded policy thresholds governing [`SwarmStallMonitor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwarmStallMonitorPolicy {
    pub stall_grace_ms: i64,
    pub max_stall_nudges: u32,
    pub max_ping_pong_streak: u32,
    pub max_member_turns: u64,
}

impl Default for SwarmStallMonitorPolicy {
    fn default() -> Self {
        Self {
            stall_grace_ms: DEFAULT_STALL_GRACE_MS,
            max_stall_nudges: DEFAULT_MAX_STALL_NUDGES,
            max_ping_pong_streak: DEFAULT_MAX_PING_PONG_STREAK,
            max_member_turns: DEFAULT_MAX_MEMBER_TURNS,
        }
    }
}

impl SwarmStallMonitorPolicy {
    /// Validate policy bounds (`stall_grace_ms >= 1`, `max_stall_nudges >= 1`,
    /// `max_ping_pong_streak >= 2`, `max_member_turns >= 1`).
    ///
    /// # Errors
    /// Returns [`OperationsError::Bounds`] when any threshold is out of range.
    pub fn validate(&self) -> Result<(), OperationsError> {
        if self.stall_grace_ms < 1 {
            return Err(OperationsError::Bounds("swarm stall_grace_ms"));
        }
        if self.max_stall_nudges < 1 {
            return Err(OperationsError::Bounds("swarm max_stall_nudges"));
        }
        if self.max_ping_pong_streak < 2 {
            return Err(OperationsError::Bounds("swarm max_ping_pong_streak"));
        }
        if self.max_member_turns < 1 {
            return Err(OperationsError::Bounds("swarm max_member_turns"));
        }
        Ok(())
    }
}

/// Deterministic classification explaining why a single member was chosen for a `[stall]` nudge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwarmBottleneckReason {
    WakeQueuedVerifier,
    WakeQueuedWorker,
    UnreadPendingDoorbell,
    AllMembersWaitingCoordinatorFallback,
}

/// Deterministic classification explaining why [`SwarmStallMonitor`] tripped the livelock breaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwarmLivelockReason {
    PingPongStreakExceeded { observed_streak: u32 },
    MemberTurnCapExceeded { turns_used: u64 },
}

/// Pure-Rust, zero-token diagnostic decision computed from a [`SwarmRunSnapshot`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SwarmMonitorDiagnosis {
    Healthy {
        run_state: SwarmRunState,
        working_members: usize,
    },
    SilentStall {
        target_member_id: Uuid,
        target_roster_name: String,
        reason: SwarmBottleneckReason,
        idle_ms: i64,
        next_nudge_number: u32,
        will_escalate_to_aborted: bool,
    },
    Livelock {
        looping_member_ids: Vec<Uuid>,
        looping_roster_names: Vec<String>,
        coordinator_member_id: Uuid,
        coordinator_roster_name: String,
        reason: SwarmLivelockReason,
    },
}

/// Persisted remediation outcome returned by [`SwarmStallMonitor::evaluate_and_remediate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SwarmMonitorActionReport {
    NoAction {
        run_state: SwarmRunState,
        working_members: usize,
    },
    StallNudged {
        target_member_id: Uuid,
        reason: SwarmBottleneckReason,
        outcome: SwarmStallNudgeOutcome,
    },
    StallAborted {
        target_member_id: Uuid,
        reason: SwarmBottleneckReason,
        outcome: SwarmStallNudgeOutcome,
    },
    LivelockBroken {
        reason: SwarmLivelockReason,
        outcome: BreakSwarmLivelockOutcome,
    },
}

/// Zero-token deterministic pure-Rust Stall & Livelock state inspector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SwarmStallMonitor {
    policy: SwarmStallMonitorPolicy,
}

impl SwarmStallMonitor {
    /// Construct a validated [`SwarmStallMonitor`].
    ///
    /// # Errors
    /// Rejects invalid threshold bounds via [`SwarmStallMonitorPolicy::validate`].
    pub fn new(policy: SwarmStallMonitorPolicy) -> Result<Self, OperationsError> {
        policy.validate()?;
        Ok(Self { policy })
    }

    #[must_use]
    pub const fn policy(&self) -> SwarmStallMonitorPolicy {
        self.policy
    }

    /// Pure deterministic state inspection (0 LLM tokens, 0 database mutations).
    ///
    /// # Errors
    /// Rejects invalid policy bounds or negative `observed_at_ms`.
    pub fn diagnose(
        &self,
        snapshot: &SwarmRunSnapshot,
        verdicts: &[SwarmVerdictRecord],
        observed_at_ms: i64,
    ) -> Result<SwarmMonitorDiagnosis, SwarmError> {
        self.diagnose_with_board(snapshot, verdicts, None, observed_at_ms)
    }

    fn diagnose_with_board(
        &self,
        snapshot: &SwarmRunSnapshot,
        verdicts: &[SwarmVerdictRecord],
        messages: Option<&[SwarmBoardMessage]>,
        observed_at_ms: i64,
    ) -> Result<SwarmMonitorDiagnosis, SwarmError> {
        self.policy.validate()?;
        if observed_at_ms < 0 {
            return Err(OperationsError::Bounds("swarm monitor observed_at_ms").into());
        }
        let run = snapshot.run();
        let working_members = snapshot
            .members()
            .iter()
            .filter(|member| member.member_state() == SwarmMemberState::Working)
            .count();

        if run.state().is_terminal() {
            return Ok(SwarmMonitorDiagnosis::Healthy {
                run_state: run.state(),
                working_members,
            });
        }

        if let Some(livelock) = self.detect_livelock(snapshot.members(), verdicts, messages) {
            return Ok(livelock);
        }

        if working_members > 0
            || !matches!(
                run.state(),
                SwarmRunState::Active | SwarmRunState::Converging
            )
        {
            return Ok(SwarmMonitorDiagnosis::Healthy {
                run_state: run.state(),
                working_members,
            });
        }

        let latest_activity_ms = latest_run_activity_ms(snapshot, verdicts);
        let idle_ms = observed_at_ms.saturating_sub(latest_activity_ms);
        if idle_ms < self.policy.stall_grace_ms {
            return Ok(SwarmMonitorDiagnosis::Healthy {
                run_state: run.state(),
                working_members: 0,
            });
        }

        let Some((target, reason)) = select_single_stall_bottleneck(snapshot) else {
            return Ok(SwarmMonitorDiagnosis::Healthy {
                run_state: run.state(),
                working_members: 0,
            });
        };

        let will_escalate_to_aborted = run.stall_nudge_count() >= self.policy.max_stall_nudges;
        Ok(SwarmMonitorDiagnosis::SilentStall {
            target_member_id: target.member_id(),
            target_roster_name: target.roster_name().to_owned(),
            reason,
            idle_ms,
            next_nudge_number: run.stall_nudge_count().saturating_add(1),
            will_escalate_to_aborted,
        })
    }

    /// Inspect `(workspace_id, swarm_run_id)` and atomically apply `[stall]` nudge,
    /// `Aborted` escalation, or `[livelock]` pair halt + `Coordinator` escalation when triggered.
    ///
    /// # Errors
    /// Returns [`SwarmError::NotFound`] if the run or `main` channel is missing, or catalog errors.
    pub fn evaluate_and_remediate(
        &self,
        catalog: &mut OperationsCatalog,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
        observed_at_ms: i64,
    ) -> Result<SwarmMonitorActionReport, SwarmError> {
        let (snapshot, verdicts, messages) =
            load_monitor_inputs(catalog, workspace_id, swarm_run_id)?;
        let diagnosis =
            self.diagnose_with_board(&snapshot, &verdicts, Some(&messages), observed_at_ms)?;
        match diagnosis {
            SwarmMonitorDiagnosis::Healthy {
                run_state,
                working_members,
            } => Ok(SwarmMonitorActionReport::NoAction {
                run_state,
                working_members,
            }),
            SwarmMonitorDiagnosis::SilentStall {
                target_member_id,
                target_roster_name,
                reason,
                idle_ms,
                next_nudge_number,
                ..
            } => {
                let main_channel_id = find_main_channel_id(&snapshot)?;
                let body = format!(
                    "@{target_roster_name} [stall] run idle for {idle_ms}ms with 0 working members (nudge {next_nudge_number}/{}); resume or report blocker.",
                    self.policy.max_stall_nudges
                );
                let outcome = catalog.record_swarm_stall_nudge(&RecordSwarmStallNudgeCommand {
                    message_id: Uuid::now_v7(),
                    workspace_id,
                    swarm_run_id,
                    channel_id: main_channel_id,
                    target_member_id,
                    reason: SwarmDoorbellReason::SystemStall,
                    body,
                    max_stall_nudges: self.policy.max_stall_nudges,
                    observed_at_ms,
                })?;
                if outcome.escalated_to_aborted() {
                    Ok(SwarmMonitorActionReport::StallAborted {
                        target_member_id,
                        reason,
                        outcome,
                    })
                } else {
                    Ok(SwarmMonitorActionReport::StallNudged {
                        target_member_id,
                        reason,
                        outcome,
                    })
                }
            }
            SwarmMonitorDiagnosis::Livelock {
                looping_member_ids,
                looping_roster_names,
                coordinator_member_id,
                coordinator_roster_name,
                reason,
            } => {
                let main_channel_id = find_main_channel_id(&snapshot)?;
                let halted_list = looping_roster_names.join(", ");
                let body = format!(
                    "@{coordinator_roster_name} [livelock] halted non-progressing member(s) [{halted_list}]; coordinator intervention required."
                );
                let outcome = catalog.break_swarm_livelock(&BreakSwarmLivelockCommand {
                    message_id: Uuid::now_v7(),
                    workspace_id,
                    swarm_run_id,
                    channel_id: main_channel_id,
                    looping_member_ids,
                    coordinator_member_id,
                    body,
                    observed_at_ms,
                })?;
                Ok(SwarmMonitorActionReport::LivelockBroken { reason, outcome })
            }
        }
    }

    fn detect_livelock(
        &self,
        members: &[SwarmMemberRecord],
        verdicts: &[SwarmVerdictRecord],
        messages: Option<&[SwarmBoardMessage]>,
    ) -> Option<SwarmMonitorDiagnosis> {
        let coordinator = members.iter().find(|member| {
            member.role_kind() == SwarmRoleKind::Coordinator
                && member.member_state() != SwarmMemberState::Killed
        })?;
        let by_id: BTreeMap<Uuid, &SwarmMemberRecord> =
            members.iter().map(|m| (m.member_id(), m)).collect();
        let distinct_reviewed_diffs = u64::try_from(
            verdicts
                .iter()
                .map(SwarmVerdictRecord::reviewed_diff_digest)
                .collect::<BTreeSet<_>>()
                .len(),
        )
        .unwrap_or(u64::MAX);

        members.iter().find_map(|member| {
            self.check_member_livelock(
                member,
                coordinator,
                &by_id,
                verdicts,
                messages,
                distinct_reviewed_diffs,
            )
        })
    }

    fn check_member_livelock(
        &self,
        member: &SwarmMemberRecord,
        coordinator: &SwarmMemberRecord,
        by_id: &BTreeMap<Uuid, &SwarmMemberRecord>,
        verdicts: &[SwarmVerdictRecord],
        messages: Option<&[SwarmBoardMessage]>,
        distinct_reviewed_diffs: u64,
    ) -> Option<SwarmMonitorDiagnosis> {
        if !is_livelock_stoppable_member(member) {
            return None;
        }
        let member_unprogressed = unprogressed_ping_pong_streak(member, by_id, verdicts, messages);

        if let Some(peer_id) = member.ping_pong_peer_id() {
            if let Some(peer) = by_id.get(&peer_id).copied()
                && is_livelock_stoppable_member(peer)
                && peer.ping_pong_peer_id() == Some(member.member_id())
            {
                let peer_unprogressed =
                    unprogressed_ping_pong_streak(peer, by_id, verdicts, messages);
                let combined = member_unprogressed.saturating_add(peer_unprogressed);
                if combined >= self.policy.max_ping_pong_streak {
                    let mut pair = [member, peer];
                    pair.sort_by_key(|entry| entry.member_id());
                    return Some(build_livelock_diagnosis(
                        &pair,
                        coordinator,
                        SwarmLivelockReason::PingPongStreakExceeded {
                            observed_streak: combined,
                        },
                    ));
                }
            } else if member_unprogressed >= self.policy.max_ping_pong_streak
                && by_id
                    .get(&peer_id)
                    .is_none_or(|peer| peer.member_state() != SwarmMemberState::Working)
            {
                return Some(build_livelock_diagnosis(
                    &[member],
                    coordinator,
                    SwarmLivelockReason::PingPongStreakExceeded {
                        observed_streak: member_unprogressed,
                    },
                ));
            }
        }

        if unprogressed_member_turns(member, verdicts, distinct_reviewed_diffs)
            >= self.policy.max_member_turns
        {
            let mut looping = vec![member];
            if let Some(peer_id) = member.ping_pong_peer_id()
                && let Some(peer) = by_id.get(&peer_id).copied()
                && is_livelock_stoppable_member(peer)
                && peer.ping_pong_peer_id() == Some(member.member_id())
            {
                looping.push(peer);
                looping.sort_by_key(|entry| entry.member_id());
            }
            return Some(build_livelock_diagnosis(
                &looping,
                coordinator,
                SwarmLivelockReason::MemberTurnCapExceeded {
                    turns_used: member.turns_used(),
                },
            ));
        }

        None
    }
}

fn build_livelock_diagnosis(
    looping: &[&SwarmMemberRecord],
    coordinator: &SwarmMemberRecord,
    reason: SwarmLivelockReason,
) -> SwarmMonitorDiagnosis {
    SwarmMonitorDiagnosis::Livelock {
        looping_member_ids: looping.iter().map(|m| m.member_id()).collect(),
        looping_roster_names: looping.iter().map(|m| m.roster_name().to_owned()).collect(),
        coordinator_member_id: coordinator.member_id(),
        coordinator_roster_name: coordinator.roster_name().to_owned(),
        reason,
    }
}

fn load_monitor_inputs(
    catalog: &OperationsCatalog,
    workspace_id: WorkspaceId,
    swarm_run_id: Uuid,
) -> Result<
    (
        SwarmRunSnapshot,
        Vec<SwarmVerdictRecord>,
        Vec<SwarmBoardMessage>,
    ),
    SwarmError,
> {
    const MAX_MONITOR_BOARD_PAGES: usize = 32;
    let snapshot = catalog
        .get_swarm_run_snapshot(workspace_id, swarm_run_id)?
        .ok_or(SwarmError::NotFound("swarm run"))?;
    let verdicts = if let Some(panel) =
        catalog.get_swarm_verification_panel(workspace_id, swarm_run_id, None)?
    {
        catalog.list_swarm_verdicts(workspace_id, swarm_run_id, panel.panel_id())?
    } else {
        Vec::new()
    };
    let mut messages = Vec::new();
    let mut after_sequence = None;
    for _ in 0..MAX_MONITOR_BOARD_PAGES {
        let page = catalog.read_swarm_board_since(ReadSwarmBoardQuery {
            workspace_id,
            swarm_run_id,
            channel_id: None,
            after_sequence,
            limit: MAX_SWARM_BOARD_PAGE_SIZE,
        })?;
        messages.extend_from_slice(page.messages());
        if let Some(next_after) = page.next_after_sequence()
            && page.has_more()
        {
            after_sequence = Some(next_after);
        } else {
            break;
        }
    }
    Ok((snapshot, verdicts, messages))
}

fn is_livelock_stoppable_member(member: &SwarmMemberRecord) -> bool {
    member.role_kind() != SwarmRoleKind::Coordinator
        && matches!(
            member.member_state(),
            SwarmMemberState::Waiting
                | SwarmMemberState::WakeQueued
                | SwarmMemberState::SuspendedQuota
        )
}

/// Computes the number of consecutive direct `@peer` ping-pong messages by `member`
/// in its current peer streak that are NOT backed by distinct diff-bound `Verifier`
/// verdict progress.
///
/// `Inherit` `Verifier`s never emit output commits (`complete_swarm_member_turn` is
/// always called with `output: None`, so `ops_swarm_members.ping_pong_streak` is not
/// reset on verdict submission, and `updated_at_ms` advances past `verdict.recorded_at_ms`).
/// Each distinct candidate diff reviewed by the `Verifier` in the current peer-streak
/// window credits one review round, preventing false-positive livelock halts across
/// multi-round `NeedsChanges` fix-and-verify cycles while preserving immediate detection
/// of non-progressing loops (including after peer switches).
fn unprogressed_ping_pong_streak(
    member: &SwarmMemberRecord,
    by_id: &BTreeMap<Uuid, &SwarmMemberRecord>,
    verdicts: &[SwarmVerdictRecord],
    messages: Option<&[SwarmBoardMessage]>,
) -> u32 {
    let raw_streak = member.ping_pong_streak();
    if raw_streak == 0 {
        return 0;
    }
    let Some(peer_id) = member.ping_pong_peer_id() else {
        return raw_streak;
    };
    let peer = by_id.get(&peer_id).copied();
    let verifier_id = if member.role_kind() == SwarmRoleKind::Verifier {
        Some(member.member_id())
    } else if let Some(p) = peer
        && p.role_kind() == SwarmRoleKind::Verifier
    {
        Some(p.member_id())
    } else {
        None
    };
    let Some(verifier_member_id) = verifier_id else {
        return raw_streak;
    };
    let cutoff_ms = peer.and_then(|p| current_streak_cutoff_ms(member, p.roster_name(), messages));
    let progressed_rounds = verdicts
        .iter()
        .filter(|verdict| {
            verdict.verifier_member_id() == verifier_member_id
                && cutoff_ms.is_none_or(|cutoff| verdict.recorded_at_ms() > cutoff)
        })
        .count();
    let progressed_u32 = u32::try_from(progressed_rounds).unwrap_or(u32::MAX);
    raw_streak.saturating_sub(progressed_u32)
}

fn unprogressed_member_turns(
    member: &SwarmMemberRecord,
    verdicts: &[SwarmVerdictRecord],
    distinct_reviewed_diffs: u64,
) -> u64 {
    if member.role_kind() == SwarmRoleKind::Verifier {
        let member_verdicts = u64::try_from(
            verdicts
                .iter()
                .filter(|verdict| verdict.verifier_member_id() == member.member_id())
                .count(),
        )
        .unwrap_or(u64::MAX);
        return member.turns_used().saturating_sub(member_verdicts);
    }
    let tree_progress_turns = if member.last_output_tree_blake3().is_some() {
        distinct_reviewed_diffs.max(1)
    } else {
        distinct_reviewed_diffs
    };
    member.turns_used().saturating_sub(tree_progress_turns)
}

fn current_streak_cutoff_ms(
    member: &SwarmMemberRecord,
    peer_roster_name: &str,
    messages: Option<&[SwarmBoardMessage]>,
) -> Option<i64> {
    let msgs = messages?;
    let streak = usize::try_from(member.ping_pong_streak()).ok()?;
    if streak == 0 {
        return Some(member.updated_at_ms());
    }
    let mention_token = format!("@{peer_roster_name}");
    let reset_by_tree_output =
        member.role_kind() != SwarmRoleKind::Verifier && member.last_output_tree_blake3().is_some();
    let mut matched_in_streak = 0_usize;
    let mut oldest_streak_msg_ms = None;
    for msg in msgs
        .iter()
        .rev()
        .filter(|m| m.author_member_id() == Some(member.member_id()))
    {
        if matched_in_streak < streak && message_mentions_token(msg.body(), &mention_token) {
            matched_in_streak += 1;
            oldest_streak_msg_ms = Some(msg.submitted_at_ms());
            if reset_by_tree_output && matched_in_streak == streak {
                return oldest_streak_msg_ms;
            }
            continue;
        }
        if matched_in_streak >= streak {
            return Some(msg.submitted_at_ms());
        }
    }
    if reset_by_tree_output {
        oldest_streak_msg_ms
    } else {
        None
    }
}

fn message_mentions_token(body: &str, mention_token: &str) -> bool {
    let haystack = body.as_bytes();
    let needle = mention_token.as_bytes();
    if needle.is_empty() || haystack.len() < needle.len() {
        return false;
    }
    let mut offset = 0_usize;
    while let Some(rel) = body[offset..].find(mention_token) {
        let pos = offset + rel;
        let valid_before = pos == 0 || {
            let prev = haystack[pos - 1];
            !prev.is_ascii_alphanumeric() && prev != b'_' && prev != b'-'
        };
        let end = pos + needle.len();
        let valid_after = end == haystack.len() || {
            let next = haystack[end];
            !next.is_ascii_lowercase() && !next.is_ascii_digit() && next != b'_' && next != b'-'
        };
        if valid_before && valid_after {
            return true;
        }
        offset = pos + 1;
    }
    false
}

fn latest_run_activity_ms(snapshot: &SwarmRunSnapshot, verdicts: &[SwarmVerdictRecord]) -> i64 {
    let mut latest = snapshot.run().updated_at_ms();
    for member in snapshot.members() {
        latest = latest.max(member.updated_at_ms());
    }
    for doorbell in snapshot.pending_doorbells() {
        latest = latest.max(doorbell.enqueued_at_ms());
    }
    for verdict in verdicts {
        latest = latest.max(verdict.recorded_at_ms());
    }
    latest
}

fn select_single_stall_bottleneck(
    snapshot: &SwarmRunSnapshot,
) -> Option<(&SwarmMemberRecord, SwarmBottleneckReason)> {
    let members = snapshot.members();

    if let Some(verifier) = members
        .iter()
        .filter(|m| {
            m.member_state() == SwarmMemberState::WakeQueued
                && m.role_kind() == SwarmRoleKind::Verifier
        })
        .min_by_key(|m| (m.updated_at_ms(), m.member_id()))
    {
        return Some((verifier, SwarmBottleneckReason::WakeQueuedVerifier));
    }

    if let Some(wake_queued) = members
        .iter()
        .filter(|m| m.member_state() == SwarmMemberState::WakeQueued)
        .min_by_key(|m| {
            (
                m.updated_at_ms(),
                std::cmp::Reverse(m.spawn_depth()),
                m.member_id(),
            )
        })
    {
        return Some((wake_queued, SwarmBottleneckReason::WakeQueuedWorker));
    }

    let by_id: BTreeMap<Uuid, &SwarmMemberRecord> =
        members.iter().map(|m| (m.member_id(), m)).collect();
    if let Some((_, target)) = snapshot
        .pending_doorbells()
        .iter()
        .filter_map(|doorbell| {
            let member = by_id.get(&doorbell.target_member_id()).copied()?;
            (member.member_state() == SwarmMemberState::Waiting
                && doorbell.sequence() > member.committed_seen_seq())
            .then_some(((doorbell.enqueued_at_ms(), member.member_id()), member))
        })
        .min_by_key(|(sort_key, _)| *sort_key)
    {
        return Some((target, SwarmBottleneckReason::UnreadPendingDoorbell));
    }

    let has_only_suspended_without_coordinator = members
        .iter()
        .any(|m| m.member_state() == SwarmMemberState::SuspendedQuota)
        && !members.iter().any(|m| {
            m.role_kind() == SwarmRoleKind::Coordinator
                && m.member_state() == SwarmMemberState::Waiting
        });
    if has_only_suspended_without_coordinator {
        return None;
    }

    members
        .iter()
        .find(|m| {
            m.role_kind() == SwarmRoleKind::Coordinator
                && !matches!(
                    m.member_state(),
                    SwarmMemberState::Killed | SwarmMemberState::Stopped
                )
        })
        .map(|coordinator| {
            (
                coordinator,
                SwarmBottleneckReason::AllMembersWaitingCoordinatorFallback,
            )
        })
}

fn find_main_channel_id(snapshot: &SwarmRunSnapshot) -> Result<Uuid, SwarmError> {
    snapshot
        .channels()
        .iter()
        .find(|ch| ch.name() == SWARM_MAIN_CHANNEL_NAME)
        .or_else(|| snapshot.channels().first())
        .map(SwarmChannelRecord::channel_id)
        .ok_or(SwarmError::NotFound("swarm main channel"))
}
