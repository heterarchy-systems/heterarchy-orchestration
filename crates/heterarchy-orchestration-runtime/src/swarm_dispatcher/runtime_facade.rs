use forge_domain::WorkspaceId;
use heterarchy_orchestration_operations::{
    AddSwarmMemberCommand, ClaimSwarmWakeupCommand, CompleteSwarmTurnCommand,
    CreateSwarmChannelCommand, CreateSwarmRunCommand, CreateSwarmVerificationPanelCommand,
    FailOrSuspendSwarmTurnCommand, KillSwarmMemberCommand, PostSwarmMessageCommand,
    ReadSwarmBoardQuery, RecordSwarmConvergenceCommand, RecordSwarmVerdictCommand,
    SettleStandaloneSwarmCommand, SubscribeSwarmChannelCommand, SwarmBoardPage, SwarmChannelRecord,
    SwarmConvergenceGateOutcome, SwarmError, SwarmMemberRecord, SwarmPostMessageOutcome,
    SwarmRunRecord, SwarmRunSnapshot, SwarmVerdictRecord, SwarmVerificationPanelRecord,
    SwarmWakeupBatch, SwitchSwarmMemberProfileCommand,
};
use uuid::Uuid;

use crate::execution_supervisor::ExecutionRunner;
use crate::{OrchestrationRuntime, RuntimeError};

use super::quota_and_recovery::{map_profile_switch_to_runtime, resolve_or_update_switch_profile};
use super::{
    AutonomyOutboxBridge, SwarmModelSwitchOutcome, SwarmMonitorActionReport,
    SwarmRuntimeDispatcher, SwarmStallMonitor, SwarmStallMonitorPolicy,
    SwitchSwarmMemberModelCommand,
};

fn split_swarm_result<T>(
    outcome: Result<T, SwarmError>,
) -> Result<Result<T, SwarmError>, RuntimeError> {
    match outcome {
        Ok(value) => Ok(Ok(value)),
        Err(SwarmError::Operations(ops_err)) => Err(RuntimeError::from(ops_err)),
        Err(domain_err) => Ok(Err(domain_err)),
    }
}

impl OrchestrationRuntime {
    /// Build a [`SwarmRuntimeDispatcher`] over this runtime's catalogs and owner lease.
    #[must_use]
    pub fn swarm_dispatcher<R: ExecutionRunner + 'static>(
        &self,
        runner: R,
    ) -> SwarmRuntimeDispatcher<R> {
        SwarmRuntimeDispatcher::from_runtime(self, runner)
    }

    /// Build an [`AutonomyOutboxBridge`] recognizing both `DAG_SETTLED_KIND = 5` and `SWARM_SETTLED_KIND = 6`.
    #[must_use]
    pub fn autonomy_outbox_bridge(&self) -> AutonomyOutboxBridge {
        AutonomyOutboxBridge::from_runtime(self)
    }

    /// Create or idempotently replay a `Standalone` or `DagNodeHybrid` Swarm run.
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`], returning domain rejections in `Ok(Err(_))`.
    pub fn create_swarm_run(
        &self,
        command: &CreateSwarmRunCommand,
    ) -> Result<Result<SwarmRunRecord, SwarmError>, RuntimeError> {
        split_swarm_result(self.catalog()?.create_swarm_run(command))
    }

    /// Read one [`SwarmRunRecord`] by `(workspace_id, swarm_run_id)`.
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`].
    pub fn get_swarm_run(
        &self,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
    ) -> Result<Result<Option<SwarmRunRecord>, SwarmError>, RuntimeError> {
        split_swarm_result(self.catalog()?.get_swarm_run(workspace_id, swarm_run_id))
    }

    /// List bounded Swarm runs for `workspace_id`.
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`].
    pub fn list_swarm_runs(
        &self,
        workspace_id: WorkspaceId,
        limit: usize,
    ) -> Result<Result<Vec<SwarmRunRecord>, SwarmError>, RuntimeError> {
        split_swarm_result(self.catalog()?.list_swarm_runs(workspace_id, limit))
    }

    /// Read a consistent [`SwarmRunSnapshot`] (run, channels, roster, watermarks, doorbells).
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`].
    pub fn get_swarm_run_snapshot(
        &self,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
    ) -> Result<Result<Option<SwarmRunSnapshot>, SwarmError>, RuntimeError> {
        split_swarm_result(
            self.catalog()?
                .get_swarm_run_snapshot(workspace_id, swarm_run_id),
        )
    }

    /// Add a member to an active Swarm roster, enforcing spawn depth, capacity, and worker-to-`Helper`-only authority.
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`].
    pub fn add_swarm_member(
        &self,
        command: &AddSwarmMemberCommand,
    ) -> Result<Result<SwarmMemberRecord, SwarmError>, RuntimeError> {
        split_swarm_result(self.catalog()?.add_swarm_member(command))
    }

    /// Kill a Swarm member and cascade-cancel all descendant helpers.
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`].
    pub fn kill_swarm_member(
        &self,
        command: KillSwarmMemberCommand,
    ) -> Result<Result<Vec<SwarmMemberRecord>, SwarmError>, RuntimeError> {
        split_swarm_result(self.catalog()?.kill_swarm_member(command))
    }

    /// Create an area channel inside an active Swarm run.
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`].
    pub fn create_swarm_channel(
        &self,
        command: &CreateSwarmChannelCommand,
    ) -> Result<Result<SwarmChannelRecord, SwarmError>, RuntimeError> {
        split_swarm_result(self.catalog()?.create_swarm_channel(command))
    }

    /// Resolve a Swarm channel by name.
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`].
    pub fn get_swarm_channel_by_name(
        &self,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
        name: &str,
    ) -> Result<Result<Option<SwarmChannelRecord>, SwarmError>, RuntimeError> {
        split_swarm_result(self.catalog()?.get_swarm_channel_by_name(
            workspace_id,
            swarm_run_id,
            name,
        ))
    }

    /// Subscribe an active Swarm member to an area channel.
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`].
    pub fn subscribe_swarm_channel(
        &self,
        command: SubscribeSwarmChannelCommand,
    ) -> Result<Result<bool, SwarmError>, RuntimeError> {
        split_swarm_result(self.catalog()?.subscribe_swarm_channel(command))
    }

    /// Append a `#seq` Swarm board message and enqueue `@mention` doorbells.
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`].
    pub fn post_swarm_message(
        &self,
        command: &PostSwarmMessageCommand,
    ) -> Result<Result<SwarmPostMessageOutcome, SwarmError>, RuntimeError> {
        split_swarm_result(self.catalog()?.post_swarm_message(command))
    }

    /// Incrementally read Swarm board messages after `after_sequence`.
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`].
    pub fn read_swarm_board_since(
        &self,
        query: ReadSwarmBoardQuery,
    ) -> Result<Result<SwarmBoardPage, SwarmError>, RuntimeError> {
        split_swarm_result(self.catalog()?.read_swarm_board_since(query))
    }

    /// List `WakeQueued` members ready for dispatch in `(workspace_id, swarm_run_id)`.
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`].
    pub fn list_wake_queued_swarm_members(
        &self,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
    ) -> Result<Result<Vec<SwarmMemberRecord>, SwarmError>, RuntimeError> {
        split_swarm_result(
            self.catalog()?
                .list_wake_queued_swarm_members(workspace_id, swarm_run_id),
        )
    }

    /// Claim a coalesced wakeup batch (`<= 32` msgs, `<= 64 KiB`) and transition `WakeQueued -> Working`.
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`].
    pub fn claim_swarm_wakeup_batch(
        &self,
        command: ClaimSwarmWakeupCommand,
    ) -> Result<Result<SwarmWakeupBatch, SwarmError>, RuntimeError> {
        split_swarm_result(self.catalog()?.claim_swarm_wakeup_batch(command))
    }

    /// Advance `committed_seen_seq = inflight_target_seq` upon clean Swarm turn completion.
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`].
    pub fn complete_swarm_member_turn(
        &self,
        command: &CompleteSwarmTurnCommand,
    ) -> Result<Result<SwarmMemberRecord, SwarmError>, RuntimeError> {
        split_swarm_result(self.catalog()?.complete_swarm_member_turn(command))
    }

    /// Release an in-flight Swarm turn on crash, timeout, quota exhaustion, or stop while preserving `committed_seen_seq`.
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`].
    pub fn fail_or_suspend_swarm_member_turn(
        &self,
        command: FailOrSuspendSwarmTurnCommand,
    ) -> Result<Result<SwarmMemberRecord, SwarmError>, RuntimeError> {
        split_swarm_result(self.catalog()?.fail_or_suspend_swarm_member_turn(command))
    }

    /// Switch a `SuspendedQuota` member's model/profile without holding both catalog locks simultaneously.
    ///
    /// # Errors
    /// Maps profile/operations persistence errors into [`RuntimeError`] and domain errors into `Ok(Err(_))`.
    pub fn switch_swarm_member_model_and_requeue(
        &self,
        command: &SwitchSwarmMemberModelCommand,
    ) -> Result<Result<SwarmModelSwitchOutcome, SwarmError>, RuntimeError> {
        let (profile_id, profile_revision, resolved_preference) = {
            let mut guard = self.profile_catalog_guard()?;
            resolve_or_update_switch_profile(&mut guard, &command.target, command.updated_at_ms)
                .map_err(map_profile_switch_to_runtime)?
        };

        let outcome = self.catalog()?.switch_swarm_member_profile_and_requeue(
            SwitchSwarmMemberProfileCommand {
                workspace_id: command.workspace_id,
                swarm_run_id: command.swarm_run_id,
                member_id: command.member_id,
                new_profile_id: profile_id,
                updated_at_ms: command.updated_at_ms,
            },
        );
        match outcome {
            Ok(member) => Ok(Ok(SwarmModelSwitchOutcome {
                member,
                profile_id,
                profile_revision,
                resolved_preference,
            })),
            Err(err) => split_swarm_result(Err(err)),
        }
    }

    /// Create a lineage-separated Swarm verification panel.
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`].
    pub fn create_swarm_verification_panel(
        &self,
        command: &CreateSwarmVerificationPanelCommand,
    ) -> Result<Result<SwarmVerificationPanelRecord, SwarmError>, RuntimeError> {
        split_swarm_result(self.catalog()?.create_swarm_verification_panel(command))
    }

    /// Read the active or specified Swarm verification panel.
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`].
    pub fn get_swarm_verification_panel(
        &self,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
        panel_id: Option<Uuid>,
    ) -> Result<Result<Option<SwarmVerificationPanelRecord>, SwarmError>, RuntimeError> {
        split_swarm_result(self.catalog()?.get_swarm_verification_panel(
            workspace_id,
            swarm_run_id,
            panel_id,
        ))
    }

    /// List immutable verdicts recorded on a Swarm verification panel.
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`].
    pub fn list_swarm_verdicts(
        &self,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
        panel_id: Uuid,
    ) -> Result<Result<Vec<SwarmVerdictRecord>, SwarmError>, RuntimeError> {
        split_swarm_result(self.catalog()?.list_swarm_verdicts(
            workspace_id,
            swarm_run_id,
            panel_id,
        ))
    }

    /// Record an immutable diff-digest-bound verdict from a lineage-separated `Verifier`.
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`].
    pub fn record_swarm_verdict(
        &self,
        command: &RecordSwarmVerdictCommand,
    ) -> Result<Result<SwarmVerdictRecord, SwarmError>, RuntimeError> {
        split_swarm_result(self.catalog()?.record_swarm_verdict(command))
    }

    /// Verify that a Swarm run holds a quorum of `Accepted` independent verdicts matching `converged_diff_digest`.
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`].
    pub fn verify_swarm_convergence_gate(
        &self,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
    ) -> Result<Result<SwarmConvergenceGateOutcome, SwarmError>, RuntimeError> {
        split_swarm_result(
            self.catalog()?
                .verify_swarm_convergence_gate(workspace_id, swarm_run_id),
        )
    }

    /// Transition an `Active` Swarm run to `Converging`.
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`].
    pub fn mark_swarm_converging(
        &self,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
        observed_at_ms: i64,
    ) -> Result<Result<SwarmRunRecord, SwarmError>, RuntimeError> {
        split_swarm_result(self.catalog()?.mark_swarm_converging(
            workspace_id,
            swarm_run_id,
            observed_at_ms,
        ))
    }

    /// Record a converged candidate commit/tree/diff digest and transition the Swarm run to `CandidateReady`.
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`].
    pub fn record_swarm_convergence_candidate(
        &self,
        command: &RecordSwarmConvergenceCommand,
    ) -> Result<Result<SwarmRunRecord, SwarmError>, RuntimeError> {
        split_swarm_result(self.catalog()?.record_swarm_convergence_candidate(command))
    }

    /// Settle a `CandidateReady` `Standalone` Swarm run after verifying the independent verifier quorum gate.
    /// `DagNodeHybrid` runs fail closed with [`SwarmError::DagNodeHybridCannotSettleDirectly`].
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`].
    pub fn settle_standalone_swarm_run(
        &self,
        command: SettleStandaloneSwarmCommand,
    ) -> Result<Result<SwarmRunRecord, SwarmError>, RuntimeError> {
        split_swarm_result(self.catalog()?.settle_standalone_swarm_run(command))
    }

    /// Abort a non-settled Swarm run and cascade-kill all active members.
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`].
    pub fn abort_swarm_run(
        &self,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
        observed_at_ms: i64,
    ) -> Result<Result<SwarmRunRecord, SwarmError>, RuntimeError> {
        split_swarm_result(self.catalog()?.abort_swarm_run(
            workspace_id,
            swarm_run_id,
            observed_at_ms,
        ))
    }

    /// Run the zero-token pure-Rust [`SwarmStallMonitor`] against `(workspace_id, swarm_run_id)`
    /// and persist any `[stall]` nudge, `Aborted` escalation, or `[livelock]` breaker transition.
    ///
    /// # Errors
    /// Lifts [`SwarmError::Operations`] into [`RuntimeError`].
    pub fn evaluate_swarm_stall_and_livelock(
        &self,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
        policy: SwarmStallMonitorPolicy,
        observed_at_ms: i64,
    ) -> Result<Result<SwarmMonitorActionReport, SwarmError>, RuntimeError> {
        let monitor = SwarmStallMonitor::new(policy)?;
        let mut catalog = self.catalog()?;
        split_swarm_result(monitor.evaluate_and_remediate(
            &mut catalog,
            workspace_id,
            swarm_run_id,
            observed_at_ms,
        ))
    }
}
