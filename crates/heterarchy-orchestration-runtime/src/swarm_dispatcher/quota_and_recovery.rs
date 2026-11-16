use std::collections::BTreeSet;

use forge_domain::{TimestampMillis, WorkspaceId};
use heterarchy_orchestration_operations::{
    FailOrSuspendSwarmTurnCommand, LaunchLedgerState, SnapshotValidationError, SwarmError,
    SwarmMemberRecord, SwarmMemberState, SwarmTurnFailureDisposition,
    SwitchSwarmMemberProfileCommand,
};
use heterarchy_orchestration_sessions::{
    AgentProfileCatalog, AgentProfileError, AgentProfileId, AgentProfileLifecycleState,
    AgentProfileRevision, ExecutionPreference, ProfileConfiguration, ProfileMutation,
    ProfileMutationCommand,
};
use uuid::Uuid;

use crate::execution_supervisor::{BootRecoveryReport, ExecutionRunner};
use crate::{RuntimeError, profile_error_to_runtime};

use super::{SwarmDispatchError, SwarmRuntimeDispatcher};

/// Target model/profile change applied during attempt-boundary quota failover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SwarmModelSwitchTarget {
    /// Mutate the specified `AgentProfile`'s [`ExecutionPreference`] at `expected_profile_revision`
    /// and rebind/requeue the `SuspendedQuota` member.
    UpdateProfilePreference {
        request_id: Uuid,
        profile_id: AgentProfileId,
        expected_profile_revision: AgentProfileRevision,
        new_preference: ExecutionPreference,
    },
    /// Rebind the `SuspendedQuota` member to an existing active `AgentProfileId`
    /// that already has a configured [`ExecutionPreference`].
    RebindActiveProfile { new_profile_id: AgentProfileId },
}

/// Command to switch a `SuspendedQuota` member's model/profile at an attempt boundary
/// without losing any unread board messages (`committed_seen_seq` preserved).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwitchSwarmMemberModelCommand {
    pub workspace_id: WorkspaceId,
    pub swarm_run_id: Uuid,
    pub member_id: Uuid,
    pub target: SwarmModelSwitchTarget,
    pub updated_at_ms: i64,
}

/// Receipt returned after switching a `SuspendedQuota` member's model and requeuing it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmModelSwitchOutcome {
    pub member: SwarmMemberRecord,
    pub profile_id: AgentProfileId,
    pub profile_revision: AgentProfileRevision,
    pub resolved_preference: ExecutionPreference,
}

/// Summary of reconciling in-flight `Working` Swarm members alongside launch-ledger boot recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmBootRecoveryReport {
    pub supervisor_report: BootRecoveryReport,
    pub reconciled_members: Vec<SwarmMemberRecord>,
}

pub(super) enum ProfileSwitchError {
    Missing(AgentProfileId),
    Inactive(AgentProfileId),
    MissingExecutionPreference,
    ProfileCatalog(AgentProfileError),
}

impl From<AgentProfileError> for ProfileSwitchError {
    fn from(error: AgentProfileError) -> Self {
        Self::ProfileCatalog(error)
    }
}

impl From<ProfileSwitchError> for SwarmDispatchError {
    fn from(error: ProfileSwitchError) -> Self {
        match error {
            ProfileSwitchError::Missing(id) => Self::ProfileMissing(id),
            ProfileSwitchError::Inactive(id) => Self::ProfileInactive(id),
            ProfileSwitchError::MissingExecutionPreference => {
                Self::SnapshotValidation(SnapshotValidationError::MissingExecutionPreference)
            }
            ProfileSwitchError::ProfileCatalog(err) => Self::ProfileCatalog(err),
        }
    }
}

pub(super) fn map_profile_switch_to_runtime(error: ProfileSwitchError) -> RuntimeError {
    match error {
        ProfileSwitchError::Missing(_) => RuntimeError::NotFound,
        ProfileSwitchError::Inactive(_) => RuntimeError::Conflict,
        ProfileSwitchError::MissingExecutionPreference => RuntimeError::InvalidInput,
        ProfileSwitchError::ProfileCatalog(err) => profile_error_to_runtime(err),
    }
}

/// Shared profile resolution/mutation boundary for `SuspendedQuota` model switches,
/// guaranteeing `OperationsCatalog` and `AgentProfileCatalog` locks are never held together.
pub(super) fn resolve_or_update_switch_profile(
    guard: &mut AgentProfileCatalog,
    target: &SwarmModelSwitchTarget,
    updated_at_ms: i64,
) -> Result<(AgentProfileId, AgentProfileRevision, ExecutionPreference), ProfileSwitchError> {
    match target {
        SwarmModelSwitchTarget::UpdateProfilePreference {
            request_id,
            profile_id,
            expected_profile_revision,
            new_preference,
        } => {
            let existing = guard
                .resolve(*profile_id)?
                .ok_or(ProfileSwitchError::Missing(*profile_id))?;
            if existing.lifecycle_state() != AgentProfileLifecycleState::Active {
                return Err(ProfileSwitchError::Inactive(*profile_id));
            }
            let config = ProfileConfiguration::new(
                existing.display_name().to_owned(),
                Some(new_preference.preferred_model().to_owned()),
                new_preference.preferred_reasoning().map(str::to_owned),
                existing.base_capabilities().to_vec(),
            )?
            .with_execution_preference(Some(new_preference.clone()));
            let observed_at =
                TimestampMillis::new(updated_at_ms).map_err(AgentProfileError::from)?;
            let mutation = ProfileMutationCommand::new(
                *request_id,
                *profile_id,
                *expected_profile_revision,
                ProfileMutation::Update(config),
                observed_at,
            )?;
            let receipt = guard.mutate_profile(&mutation)?;
            let updated = receipt.profile();
            let resolved_pref = updated
                .execution_preference()
                .cloned()
                .ok_or(ProfileSwitchError::MissingExecutionPreference)?;
            Ok((*profile_id, updated.revision(), resolved_pref))
        }
        SwarmModelSwitchTarget::RebindActiveProfile { new_profile_id } => {
            let profile = guard
                .resolve(*new_profile_id)?
                .ok_or(ProfileSwitchError::Missing(*new_profile_id))?;
            if profile.lifecycle_state() != AgentProfileLifecycleState::Active {
                return Err(ProfileSwitchError::Inactive(*new_profile_id));
            }
            let resolved_pref = profile
                .execution_preference()
                .cloned()
                .ok_or(ProfileSwitchError::MissingExecutionPreference)?;
            Ok((*new_profile_id, profile.revision(), resolved_pref))
        }
    }
}

impl<R: ExecutionRunner + 'static> SwarmRuntimeDispatcher<R> {
    /// Explicitly transition an in-flight `Working` member to `SuspendedQuota` while preserving
    /// `committed_seen_seq` unchanged.
    ///
    /// # Errors
    /// Rejects non-`Working` members, snapshot mismatches, or catalog persistence failures.
    pub fn suspend_member_for_quota(
        &self,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
        member_id: Uuid,
        expected_snapshot_id: Uuid,
        observed_at_ms: i64,
    ) -> Result<SwarmMemberRecord, SwarmDispatchError> {
        let mut catalog = self.lock_operations()?;
        Ok(
            catalog.fail_or_suspend_swarm_member_turn(FailOrSuspendSwarmTurnCommand {
                workspace_id,
                swarm_run_id,
                member_id,
                expected_snapshot_id,
                disposition: SwarmTurnFailureDisposition::QuotaExceeded,
                consume_turn: false,
                observed_at_ms,
            })?,
        )
    }

    /// Switch a `SuspendedQuota` member's model/profile at an attempt boundary and requeue it
    /// without losing a single unread board message (`committed_seen_seq` preserved).
    ///
    /// On the next `dispatch_member_turn`, the updated [`ExecutionPreference`] and incremented
    /// `attempt_epoch` are frozen into a fresh `ExecutionAttemptKey.attempt` and fresh immutable
    /// [`AssignmentExecutionSnapshot`].
    ///
    /// # Errors
    /// Rejects non-`SuspendedQuota` members, missing/inactive profiles, missing preferences,
    /// stale profile revisions, or catalog failures.
    pub fn switch_member_model_and_requeue(
        &self,
        command: &SwitchSwarmMemberModelCommand,
    ) -> Result<SwarmModelSwitchOutcome, SwarmDispatchError> {
        let (profile_id, profile_revision, resolved_preference) = {
            let mut guard = self.lock_profiles()?;
            resolve_or_update_switch_profile(&mut guard, &command.target, command.updated_at_ms)?
        };

        let member = {
            let mut catalog = self.lock_operations()?;
            catalog.switch_swarm_member_profile_and_requeue(SwitchSwarmMemberProfileCommand {
                workspace_id: command.workspace_id,
                swarm_run_id: command.swarm_run_id,
                member_id: command.member_id,
                new_profile_id: profile_id,
                updated_at_ms: command.updated_at_ms,
            })?
        };

        Ok(SwarmModelSwitchOutcome {
            member,
            profile_id,
            profile_revision,
            resolved_preference,
        })
    }

    /// Reconcile launch-ledger records on daemon boot and release any stranded `Working`
    /// Swarm members in `(workspace_id, swarm_run_id)` while preserving `committed_seen_seq`.
    ///
    /// Members whose un-spawned snapshot reconciled to `NotStarted` return to `WakeQueued`/`Waiting`
    /// (`TransientRetry`); members whose process was already `Spawned` enter `Stopped` so unpinned
    /// orphans are never auto-retried.
    ///
    /// # Errors
    /// Returns boot-recovery or catalog persistence errors.
    pub async fn recover_swarm_run_on_boot(
        &self,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
        reconciled_at_ms: i64,
    ) -> Result<SwarmBootRecoveryReport, SwarmDispatchError> {
        let supervisor_report = self
            .supervisor
            .recover_on_boot(Some(workspace_id), reconciled_at_ms)
            .await?;

        let mut not_started_snapshots = BTreeSet::new();
        for entry in &supervisor_report.entries {
            if matches!(entry.ledger.state(), LaunchLedgerState::NotStarted { .. }) {
                not_started_snapshots.insert(entry.ledger.snapshot_id());
            }
        }

        let reconciled_members = {
            let mut catalog = self.lock_operations()?;
            let Some(snapshot) = catalog.get_swarm_run_snapshot(workspace_id, swarm_run_id)? else {
                return Err(SwarmError::NotFound("swarm run").into());
            };
            let mut updated = Vec::new();
            for member in snapshot.members() {
                if member.member_state() == SwarmMemberState::Working
                    && let Some(active_snapshot_id) = member.active_snapshot_id()
                {
                    let disposition = if not_started_snapshots.contains(&active_snapshot_id) {
                        SwarmTurnFailureDisposition::TransientRetry
                    } else {
                        SwarmTurnFailureDisposition::Stopped
                    };
                    let reconciled = catalog.fail_or_suspend_swarm_member_turn(
                        FailOrSuspendSwarmTurnCommand {
                            workspace_id,
                            swarm_run_id,
                            member_id: member.member_id(),
                            expected_snapshot_id: active_snapshot_id,
                            disposition,
                            consume_turn: false,
                            observed_at_ms: reconciled_at_ms.max(member.updated_at_ms()),
                        },
                    )?;
                    updated.push(reconciled);
                }
            }
            updated
        };

        Ok(SwarmBootRecoveryReport {
            supervisor_report,
            reconciled_members,
        })
    }
}
