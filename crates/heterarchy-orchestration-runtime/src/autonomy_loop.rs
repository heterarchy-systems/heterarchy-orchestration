//! Runtime autonomy bridge for CLI execution preferences, registered Workspace scopes,
//! independent reviewer assignment/gating, leadership-fenced DAG settlement authority,
//! and pre-spawn `AssignmentExecutionSnapshot` preparation.

use forge_domain::{GoalId, TimestampMillis, WorkspaceId};
use heterarchy_orchestration_operations::{
    AssignmentExecutionSnapshot, AssignmentExecutionSnapshotParts, DagSettledProjectionRow,
    DagSettlementGovernanceLink, ExecutionAttemptKey, LaunchLedgerRecord, NodeLifecycleAuthority,
    NodeTeamBindingScope, OperationalActor, OperationsError, RegisteredWorkspaceScopeRecord,
    ReviewVerdictCommand, ReviewerAssignmentCommand, ReviewerAssignmentError,
    ReviewerAssignmentRecord, ScopeRegistrationCommand, ScopeResolutionError,
    ScopeUnregisterCommand, SnapshotValidationError, WorkCorrelationKey,
    WorkspaceDagSettlementAuthority, WorkspaceParticipationId, WorkspaceProjectScope,
};
use heterarchy_orchestration_sessions::{
    AgentProfileCreationOrigin, AgentProfileError, AgentProfileId, AgentProfileLifecycleState,
    CliEngine, CreateAgentProfile, ExecutionPreference, RolePromptDigest,
};
use uuid::Uuid;

use crate::{
    AgentProfileCreateCommand, AgentProfileCreateSummary, OrchestrationRuntime, RuntimeError,
    agent_profile_summary, profile_error_to_runtime,
};

/// Compare-and-swap command alias for recording a diff-digest-bound reviewer verdict.
pub type RecordReviewVerdictCommand = ReviewVerdictCommand;

/// Transport-neutral projection of a reusable Profile [`ExecutionPreference`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionPreferenceSummary {
    pub cli_engine: &'static str,
    pub preferred_model: String,
    pub preferred_reasoning: Option<String>,
    pub capabilities: Vec<String>,
    pub role_prompt_digest_hex: Option<String>,
}

impl ExecutionPreferenceSummary {
    /// Convert a domain [`ExecutionPreference`] into its transport-neutral summary.
    #[must_use]
    pub fn from_domain(pref: &ExecutionPreference) -> Self {
        Self {
            cli_engine: match pref.cli_engine() {
                CliEngine::Codex => "codex",
                CliEngine::Agy => "agy",
            },
            preferred_model: pref.preferred_model().to_owned(),
            preferred_reasoning: pref.preferred_reasoning().map(str::to_owned),
            capabilities: pref.capabilities().to_vec(),
            role_prompt_digest_hex: pref
                .role_prompt_ref()
                .as_ref()
                .map(RolePromptDigest::to_hex),
        }
    }
}

impl OrchestrationRuntime {
    /// Create or replay one app-global reusable Agent Profile with an optional
    /// CLI [`ExecutionPreference`].
    ///
    /// Broker owner validation must be completed by the caller before this
    /// synchronous catalog mutation is admitted.
    ///
    /// # Errors
    /// Returns invalid input, replay conflict, catalog ownership, or persistence
    /// failures.
    pub fn create_agent_profile_with_execution_preference(
        &self,
        command: AgentProfileCreateCommand,
        execution_preference: Option<ExecutionPreference>,
    ) -> Result<AgentProfileCreateSummary, RuntimeError> {
        let profile_id =
            AgentProfileId::from_uuid(Uuid::now_v7()).map_err(profile_error_to_runtime)?;
        let created_at = TimestampMillis::new(command.created_at_ms)
            .map_err(AgentProfileError::from)
            .map_err(profile_error_to_runtime)?;
        let create = CreateAgentProfile::new(
            profile_id,
            command.request_id,
            command.stable_name,
            command.display_name,
            command.preferred_model,
            command.preferred_reasoning,
            command.base_capabilities,
            AgentProfileCreationOrigin::UserManual,
            created_at,
        )
        .map_err(profile_error_to_runtime)?
        .with_execution_preference(execution_preference);
        let mut catalog = self.profile_catalog_guard()?;
        let outcome = catalog
            .create_profile(&create)
            .map_err(profile_error_to_runtime)?;
        Ok(AgentProfileCreateSummary {
            profile: agent_profile_summary(outcome.profile()),
            created: outcome.created(),
        })
    }

    /// Register a new [`WorkspaceProjectScope`] or advance an existing scope's
    /// revision and generation.
    ///
    /// # Errors
    /// Rejects invalid bounds, stale expected revisions, conflicting `request_id`
    /// reuse, active-scope cap overflow, poisoned locks, or persistence failures.
    pub fn register_workspace_scope(
        &self,
        command: &ScopeRegistrationCommand,
    ) -> Result<RegisteredWorkspaceScopeRecord, RuntimeError> {
        Ok(self.catalog()?.register_workspace_scope(command)?)
    }

    /// Mark an active Workspace scope as unregistered while advancing monotonic
    /// generation.
    ///
    /// # Errors
    /// Rejects invalid bounds, missing or inactive scopes, stale revisions,
    /// conflicting `request_id` reuse, poisoned locks, or persistence failures.
    pub fn unregister_workspace_scope(
        &self,
        command: &ScopeUnregisterCommand,
    ) -> Result<RegisteredWorkspaceScopeRecord, RuntimeError> {
        Ok(self.catalog()?.unregister_workspace_scope(command)?)
    }

    /// Seed initial `'ACTIVE'` rows for configured startup scopes when absent,
    /// returning the count of newly inserted scope rows.
    ///
    /// # Errors
    /// Rejects negative timestamps, duplicate `workspace_id` entries, active-scope
    /// cap overflow, poisoned locks, or persistence failures.
    pub fn seed_workspace_scopes_if_absent(
        &self,
        scopes: &[WorkspaceProjectScope],
        now_ms: i64,
    ) -> Result<usize, RuntimeError> {
        let records = self
            .catalog()?
            .seed_workspace_scopes_if_absent(scopes, now_ms)?;
        Ok(records.iter().filter(|record| !record.replayed()).count())
    }

    /// Resolve and authorize a candidate [`WorkspaceProjectScope`] fail-closed
    /// against the durable registry.
    ///
    /// # Errors
    /// Returns [`RuntimeError`] on poisoned catalog ownership or persistence
    /// corruption/readback failures; policy denials are returned inside
    /// `Ok(Err(ScopeResolutionError))`.
    pub fn resolve_registered_workspace_scope(
        &self,
        candidate: &WorkspaceProjectScope,
        expected_generation: Option<u64>,
    ) -> Result<Result<RegisteredWorkspaceScopeRecord, ScopeResolutionError>, RuntimeError> {
        Ok(self
            .catalog()?
            .resolve_registered_workspace_scope(candidate, expected_generation)?)
    }

    /// List all currently `'ACTIVE'` registered Workspace scopes ordered by
    /// `workspace_id`.
    ///
    /// # Errors
    /// Returns [`RuntimeError`] on poisoned catalog ownership or persistence
    /// readback/bounds failures.
    pub fn list_active_workspace_scopes(
        &self,
    ) -> Result<Vec<RegisteredWorkspaceScopeRecord>, RuntimeError> {
        Ok(self.catalog()?.list_active_workspace_scopes()?)
    }

    /// Deterministically select and persist an independent, interested, qualified
    /// reviewer for one work attempt.
    ///
    /// # Errors
    /// Lifts [`ReviewerAssignmentError::Operations`] into [`RuntimeError`], while
    /// preserving typed domain rejections inside `Ok(Err(ReviewerAssignmentError))`.
    pub fn assign_independent_reviewer(
        &self,
        command: &ReviewerAssignmentCommand,
    ) -> Result<Result<ReviewerAssignmentRecord, ReviewerAssignmentError>, RuntimeError> {
        let outcome = self.catalog()?.assign_independent_reviewer(command);
        split_reviewer_assignment_result(outcome)
    }

    /// Record a diff-digest-bound reviewer verdict using compare-and-swap on
    /// `expected_revision`.
    ///
    /// # Errors
    /// Lifts [`ReviewerAssignmentError::Operations`] into [`RuntimeError`], while
    /// preserving typed domain rejections inside `Ok(Err(ReviewerAssignmentError))`.
    pub fn record_review_verdict(
        &self,
        command: &RecordReviewVerdictCommand,
    ) -> Result<Result<ReviewerAssignmentRecord, ReviewerAssignmentError>, RuntimeError> {
        let outcome = self.catalog()?.record_review_verdict(command);
        split_reviewer_assignment_result(outcome)
    }

    /// Verify fail-closed that `correlation` holds an `Accepted` independent
    /// review verdict whose `handoff_diff_digest` matches `current_handoff_diff_digest`.
    ///
    /// # Errors
    /// Lifts [`ReviewerAssignmentError::Operations`] into [`RuntimeError`], while
    /// preserving typed gate rejections inside `Ok(Err(ReviewerAssignmentError))`.
    pub fn verify_node_completion_review_gate(
        &self,
        correlation: WorkCorrelationKey,
        current_handoff_diff_digest: [u8; 32],
    ) -> Result<Result<ReviewerAssignmentRecord, ReviewerAssignmentError>, RuntimeError> {
        let outcome = self
            .catalog()?
            .verify_node_completion_review_gate(&correlation, current_handoff_diff_digest);
        split_reviewer_assignment_result(outcome)
    }

    /// Read the current durable [`ReviewerAssignmentRecord`] for `correlation`,
    /// if assigned.
    ///
    /// # Errors
    /// Returns [`RuntimeError`] on invalid `correlation` bounds, poisoned catalog
    /// ownership, or persistence read failures.
    pub fn get_reviewer_assignment(
        &self,
        correlation: WorkCorrelationKey,
    ) -> Result<Option<ReviewerAssignmentRecord>, RuntimeError> {
        Ok(self.catalog()?.get_reviewer_assignment(&correlation)?)
    }

    /// Verify fail-closed that `participation_id` in `workspace_id` is currently
    /// bound to `(agent_id, session_id)`.
    ///
    /// # Errors
    /// Returns [`ReviewerAssignmentError`] when the session binding is missing,
    /// mismatched, poisoned, or fails persistence validation.
    pub fn verify_reviewer_session_binding(
        &self,
        workspace_id: WorkspaceId,
        participation_id: WorkspaceParticipationId,
        agent_id: Uuid,
        session_id: Uuid,
    ) -> Result<(), ReviewerAssignmentError> {
        self.catalog()
            .map_err(|_| {
                ReviewerAssignmentError::Operations(OperationsError::Corrupt(
                    "operations catalog owner poisoned",
                ))
            })?
            .verify_reviewer_session_binding(workspace_id, participation_id, agent_id, session_id)
    }

    /// Authorize an exclusive Workspace Orchestrator (`scope_kind = 0`) to invoke
    /// `dag.settle`, fenced by `expected_leadership_revision`.
    ///
    /// # Errors
    /// Rejects missing/inactive Workspace leadership, stale leadership revisions,
    /// non-orchestrator callers, poisoned locks, or persistence failures.
    pub fn authorize_workspace_dag_settlement(
        &self,
        workspace_id: WorkspaceId,
        actor: OperationalActor,
        expected_leadership_revision: i64,
    ) -> Result<WorkspaceDagSettlementAuthority, RuntimeError> {
        Ok(self.catalog()?.authorize_workspace_dag_settlement(
            workspace_id,
            actor,
            expected_leadership_revision,
        )?)
    }

    /// Authorize a decentralized Team Leader (`scope_kind = 2`) to start or
    /// complete a DAG node bound to their active Team.
    ///
    /// # Errors
    /// Rejects unbound/tombstoned nodes, stale team leadership revisions,
    /// non-leader callers, poisoned locks, or persistence failures.
    pub fn authorize_team_leader_node_transition(
        &self,
        scope: NodeTeamBindingScope,
        actor: OperationalActor,
        expected_team_leadership_revision: i64,
    ) -> Result<NodeLifecycleAuthority, RuntimeError> {
        Ok(self.catalog()?.authorize_team_leader_node_transition(
            scope,
            actor,
            expected_team_leadership_revision,
        )?)
    }

    /// Read one bounded page of durable `DagSettled` projections ordered by
    /// source sequence.
    ///
    /// # Errors
    /// Returns [`RuntimeError`] on poisoned catalog ownership, corrupt rows, or
    /// persistence read failures.
    pub fn dag_settled_projection(
        &self,
        migration_id: Uuid,
    ) -> Result<Vec<DagSettledProjectionRow>, RuntimeError> {
        Ok(self.catalog()?.dag_settled_projection(migration_id)?)
    }

    /// Read the durable governance link derived when
    /// `(workspace_id, goal_id, dag_revision)` settled.
    ///
    /// # Errors
    /// Rejects invalid lookup coordinates, poisoned catalog ownership, corrupt
    /// rows, or persistence read failures.
    pub fn dag_settlement_governance_link(
        &self,
        workspace_id: WorkspaceId,
        goal_id: GoalId,
        dag_revision: i64,
    ) -> Result<Option<DagSettlementGovernanceLink>, RuntimeError> {
        Ok(self
            .catalog()?
            .dag_settlement_governance_link(workspace_id, goal_id, dag_revision)?)
    }

    /// Validate `parts` against the live active `AgentProfile` and commit the
    /// immutable [`AssignmentExecutionSnapshot`] plus initial `Prepared`
    /// [`LaunchLedgerRecord`].
    ///
    /// Lock discipline: acquires `self.profiles` briefly to inspect lifecycle
    /// state and clone `(execution_preference, revision)`, drops the profile
    /// guard before resolving or acquiring `self.operations`, and never holds
    /// both catalog mutexes simultaneously.
    ///
    /// # Errors
    /// Returns [`RuntimeError::NotFound`] for unknown profiles,
    /// [`RuntimeError::Conflict`] for inactive profiles or duplicate attempt
    /// commits, [`RuntimeError::InvalidInput`] for nil `request_id`, or
    /// persistence failures. Validation mismatches return `Ok(Err(SnapshotValidationError))`.
    pub fn prepare_assignment_execution_snapshot(
        &self,
        request_id: Uuid,
        parts: AssignmentExecutionSnapshotParts,
    ) -> Result<Result<LaunchLedgerRecord, SnapshotValidationError>, RuntimeError> {
        let profile_guard = self.profile_catalog_guard()?;
        let profile = profile_guard
            .resolve(parts.profile_id)
            .map_err(profile_error_to_runtime)?
            .ok_or(RuntimeError::NotFound)?;
        if profile.lifecycle_state() != AgentProfileLifecycleState::Active {
            return Err(RuntimeError::Conflict);
        }
        let (pref, current_revision) =
            (profile.execution_preference().cloned(), profile.revision());
        drop(profile_guard);

        let snapshot = match AssignmentExecutionSnapshot::resolve_from_profile(
            parts,
            pref.as_ref(),
            current_revision,
        ) {
            Ok(snapshot) => snapshot,
            Err(validation_err) => return Ok(Err(validation_err)),
        };

        let (_committed_snapshot, ledger) = self
            .catalog()?
            .commit_assignment_execution_snapshot(request_id, &snapshot)?;
        Ok(Ok(ledger))
    }

    /// Read the durable [`LaunchLedgerRecord`] for `attempt_key`, if prepared.
    ///
    /// # Errors
    /// Returns [`RuntimeError`] on poisoned catalog ownership, corrupt state, or
    /// persistence read failures.
    pub fn get_assignment_execution_snapshot(
        &self,
        attempt_key: ExecutionAttemptKey,
    ) -> Result<Option<LaunchLedgerRecord>, RuntimeError> {
        let pair = self
            .catalog()?
            .get_assignment_execution_snapshot(attempt_key)?;
        Ok(pair.map(|(_snapshot, ledger)| ledger))
    }
}

fn split_reviewer_assignment_result(
    outcome: Result<ReviewerAssignmentRecord, ReviewerAssignmentError>,
) -> Result<Result<ReviewerAssignmentRecord, ReviewerAssignmentError>, RuntimeError> {
    match outcome {
        Ok(record) => Ok(Ok(record)),
        Err(ReviewerAssignmentError::Operations(ops_err)) => Err(RuntimeError::from(ops_err)),
        Err(domain_err) => Ok(Err(domain_err)),
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::path::{Path, PathBuf};

    use forge_domain::{
        ActivityId, AgentAssignmentId, AuthenticatedPrincipal, DagNodeId, DagRevisionNumber,
        DirectTokenId,
    };
    use heterarchy_orchestration_operations::{
        ExecutionAuthorIdentity, ExecutionPermissions, ExecutionTimeouts, ForgeWorktreeRef,
        LaunchLedgerState, LeaseRef, McpGrant, ReviewVerdictOutcome, ReviewerCandidateIdentity,
        WorkspaceParticipationId,
    };
    use heterarchy_orchestration_sessions::{
        AgentProfileRevision, ProfileMutation, ProfileMutationCommand,
    };

    use super::*;
    use crate::OPERATIONS_CATALOG_FILE_NAME;

    type TestResult = Result<(), Box<dyn Error>>;

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new(label: &str) -> Result<Self, std::io::Error> {
            let root = std::env::temp_dir().join(format!("{label}-{}", Uuid::now_v7()));
            std::fs::create_dir(&root)?;
            Ok(Self(root))
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn profile_command(request_id: Uuid, name: &str) -> AgentProfileCreateCommand {
        AgentProfileCreateCommand {
            request_id,
            stable_name: name.to_owned(),
            display_name: format!("Display {name}"),
            preferred_model: Some("gpt-6-luna".to_owned()),
            preferred_reasoning: Some("max".to_owned()),
            base_capabilities: vec!["rust".to_owned()],
            created_at_ms: 10,
        }
    }

    fn codex_preference() -> Result<(ExecutionPreference, RolePromptDigest), Box<dyn Error>> {
        let prompt_digest = RolePromptDigest::new([0x42; 32])?;
        let preference = ExecutionPreference::new(
            CliEngine::Codex,
            "gpt-6-luna".to_owned(),
            Some("max".to_owned()),
            vec!["verify".to_owned(), "code".to_owned()],
            Some(prompt_digest),
        )?;
        Ok((preference, prompt_digest))
    }

    fn sample_attempt_key(attempt: u32) -> Result<ExecutionAttemptKey, Box<dyn Error>> {
        Ok(ExecutionAttemptKey::new(
            WorkspaceId::from_uuid(Uuid::from_u128(0xB101)),
            GoalId::from_uuid(Uuid::from_u128(0xB102)),
            DagRevisionNumber::INITIAL,
            DagNodeId::from_uuid(Uuid::from_u128(0xB103)),
            AgentAssignmentId::from_uuid(Uuid::from_u128(0xB104)),
            attempt,
        )?)
    }

    fn build_snapshot_parts(
        snapshot_id: Uuid,
        attempt_key: ExecutionAttemptKey,
        profile_id: AgentProfileId,
        profile_revision: AgentProfileRevision,
        role_prompt_digest: Option<RolePromptDigest>,
    ) -> Result<AssignmentExecutionSnapshotParts, Box<dyn Error>> {
        Ok(AssignmentExecutionSnapshotParts {
            snapshot_id,
            attempt_key,
            activity_id: ActivityId::from_uuid(Uuid::from_u128(0xB105)),
            profile_id,
            profile_revision,
            resolved_engine: CliEngine::Codex,
            resolved_model: "gpt-6-luna".to_owned(),
            resolved_reasoning: Some("max".to_owned()),
            role_prompt_digest,
            worktree: ForgeWorktreeRef::new(PathBuf::from("/var/tmp/forge-worktrees/node-alpha"))?,
            lease: LeaseRef::new(Uuid::from_u128(0xB106), 1, 90_000)?,
            mcp_grant: Some(McpGrant::new(
                DirectTokenId::from_uuid(Uuid::from_u128(0xB107)),
                100,
                90_000,
            )?),
            permissions: ExecutionPermissions::new(true, true, false),
            timeouts: ExecutionTimeouts::new(5_000, 60_000, 5_000)?,
            committed_at_ms: 500,
        })
    }

    #[test]
    fn snapshot_preparation_commits_happy_path_and_rejects_revision_mismatch() -> TestResult {
        let root = TestRoot::new("autonomy-snapshot-happy")?;
        let runtime = OrchestrationRuntime::open(&root.0)?;
        let (pref, prompt_digest) = codex_preference()?;

        let created = runtime.create_agent_profile_with_execution_preference(
            profile_command(Uuid::from_u128(0xB201), "codex-worker"),
            Some(pref),
        )?;
        assert!(created.created);
        let summary_pref = created
            .profile
            .execution_preference
            .ok_or("expected execution_preference summary on created profile")?;
        assert_eq!(summary_pref.cli_engine, "codex");
        assert_eq!(summary_pref.preferred_model, "gpt-6-luna");
        assert_eq!(summary_pref.preferred_reasoning.as_deref(), Some("max"));
        assert_eq!(summary_pref.capabilities, vec!["code", "verify"]);
        assert_eq!(
            summary_pref.role_prompt_digest_hex.as_deref(),
            Some(prompt_digest.to_hex().as_str())
        );

        let worker_id = AgentProfileId::from_uuid(created.profile.profile_id)?;
        let first_attempt = sample_attempt_key(1)?;
        let happy_parts = build_snapshot_parts(
            Uuid::from_u128(0xB301),
            first_attempt,
            worker_id,
            AgentProfileRevision::new(created.profile.revision)?,
            Some(prompt_digest),
        )?;
        let ledger = runtime
            .prepare_assignment_execution_snapshot(Uuid::from_u128(0xB401), happy_parts)?
            .map_err(|err| format!("expected snapshot commit, got {err:?}"))?;
        assert_eq!(ledger.snapshot_id(), Uuid::from_u128(0xB301));
        assert_eq!(ledger.state(), &LaunchLedgerState::Prepared);
        assert_eq!(
            runtime.get_assignment_execution_snapshot(first_attempt)?,
            Some(ledger)
        );

        let stale_parts = build_snapshot_parts(
            Uuid::from_u128(0xB302),
            sample_attempt_key(2)?,
            worker_id,
            AgentProfileRevision::new(created.profile.revision + 1)?,
            Some(prompt_digest),
        )?;
        assert_eq!(
            runtime.prepare_assignment_execution_snapshot(Uuid::from_u128(0xB402), stale_parts)?,
            Err(SnapshotValidationError::ProfileRevisionMismatch)
        );
        Ok(())
    }

    #[test]
    fn snapshot_preparation_rejects_missing_preference_and_disabled_profile() -> TestResult {
        let root = TestRoot::new("autonomy-snapshot-rejections")?;
        let runtime = OrchestrationRuntime::open(&root.0)?;
        let (pref, prompt_digest) = codex_preference()?;

        let unconfigured = runtime
            .create_agent_profile(profile_command(Uuid::from_u128(0xB202), "unbound-worker"))?;
        assert_eq!(unconfigured.profile.execution_preference, None);
        let missing_pref_parts = build_snapshot_parts(
            Uuid::from_u128(0xB303),
            sample_attempt_key(3)?,
            AgentProfileId::from_uuid(unconfigured.profile.profile_id)?,
            AgentProfileRevision::new(unconfigured.profile.revision)?,
            Some(prompt_digest),
        )?;
        assert_eq!(
            runtime.prepare_assignment_execution_snapshot(
                Uuid::from_u128(0xB403),
                missing_pref_parts,
            )?,
            Err(SnapshotValidationError::MissingExecutionPreference)
        );

        let active_target = runtime.create_agent_profile_with_execution_preference(
            profile_command(Uuid::from_u128(0xB203), "disable-target"),
            Some(pref),
        )?;
        let worker_id = AgentProfileId::from_uuid(active_target.profile.profile_id)?;
        let disable_cmd = ProfileMutationCommand::new(
            Uuid::from_u128(0xB501),
            worker_id,
            AgentProfileRevision::new(active_target.profile.revision)?,
            ProfileMutation::Disable,
            TimestampMillis::new(600)?,
        )?;
        let disabled = runtime.mutate_agent_profile(&disable_cmd)?;
        let disabled_parts = build_snapshot_parts(
            Uuid::from_u128(0xB304),
            sample_attempt_key(4)?,
            worker_id,
            AgentProfileRevision::new(disabled.profile.revision)?,
            Some(prompt_digest),
        )?;
        assert!(matches!(
            runtime.prepare_assignment_execution_snapshot(Uuid::from_u128(0xB404), disabled_parts),
            Err(RuntimeError::Conflict)
        ));
        Ok(())
    }

    #[test]
    fn workspace_scope_registry_resolves_and_fails_closed_through_runtime() -> TestResult {
        let root = TestRoot::new("autonomy-scope-registry")?;
        let runtime = OrchestrationRuntime::open(&root.0)?;

        let primary_scope = WorkspaceProjectScope::new(
            Uuid::from_u128(0xC101),
            Uuid::from_u128(0xC201),
            1,
            1,
            "file:///srv/workspaces/primary".to_owned(),
        )?;
        let secondary_scope = WorkspaceProjectScope::new(
            Uuid::from_u128(0xC102),
            Uuid::from_u128(0xC202),
            1,
            1,
            "file:///srv/workspaces/secondary".to_owned(),
        )?;

        assert_eq!(
            runtime.resolve_registered_workspace_scope(&primary_scope, None)?,
            Err(ScopeResolutionError::EmptyRegistry)
        );

        let registered = runtime.register_workspace_scope(&ScopeRegistrationCommand::new(
            Uuid::from_u128(0xC301),
            0,
            primary_scope.clone(),
            100,
        )?)?;
        assert!(registered.active());
        assert_eq!(registered.revision(), 1);
        assert_eq!(registered.generation(), 1);

        let resolved = runtime
            .resolve_registered_workspace_scope(&primary_scope, Some(1))?
            .map_err(|err| format!("expected active scope resolution, got {err:?}"))?;
        assert_eq!(resolved.generation(), 1);

        assert_eq!(
            runtime.resolve_registered_workspace_scope(&primary_scope, Some(2))?,
            Err(ScopeResolutionError::GenerationFenceMismatch {
                expected_generation: 2,
                actual_generation: 1,
            })
        );

        let seeded = runtime.seed_workspace_scopes_if_absent(
            &[primary_scope.clone(), secondary_scope.clone()],
            150,
        )?;
        assert_eq!(seeded, 1);
        assert_eq!(runtime.list_active_workspace_scopes()?.len(), 2);

        let unregistered = runtime.unregister_workspace_scope(&ScopeUnregisterCommand::new(
            Uuid::from_u128(0xC302),
            WorkspaceId::from_uuid(primary_scope.workspace_id()),
            1,
            200,
        )?)?;
        assert!(!unregistered.active());
        assert_eq!(unregistered.generation(), 2);
        assert_eq!(
            runtime.resolve_registered_workspace_scope(&primary_scope, None)?,
            Err(ScopeResolutionError::Unregistered {
                workspace_id: WorkspaceId::from_uuid(primary_scope.workspace_id()),
            })
        );
        assert_eq!(runtime.list_active_workspace_scopes()?.len(), 1);
        Ok(())
    }

    struct ReviewerGateFixture {
        workspace_uuid: Uuid,
        correlation: WorkCorrelationKey,
        leader: ExecutionAuthorIdentity,
        executor: ExecutionAuthorIdentity,
        self_candidate: ReviewerCandidateIdentity,
        independent_candidate: ReviewerCandidateIdentity,
    }

    impl ReviewerGateFixture {
        fn build() -> Result<Self, Box<dyn Error>> {
            let workspace_uuid = Uuid::from_u128(0xD101);
            let correlation = WorkCorrelationKey::new(
                WorkspaceId::from_uuid(workspace_uuid),
                GoalId::from_uuid(Uuid::from_u128(0xD102)),
                DagRevisionNumber::INITIAL,
                DagNodeId::from_uuid(Uuid::from_u128(0xD103)),
                AgentAssignmentId::from_uuid(Uuid::from_u128(0xD104)),
                1,
            )?;
            let leader = ExecutionAuthorIdentity::new(
                AgentProfileId::from_uuid(Uuid::from_u128(0xD201))?,
                Uuid::from_u128(0xD301),
                AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(
                    0xD401,
                ))),
            )?;
            let executor = ExecutionAuthorIdentity::new(
                AgentProfileId::from_uuid(Uuid::from_u128(0xD202))?,
                Uuid::from_u128(0xD302),
                AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(
                    0xD402,
                ))),
            )?;
            let self_candidate = ReviewerCandidateIdentity::new(
                WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xD501))?,
                executor.profile_id(),
                Uuid::from_u128(0xD399),
                AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(
                    0xD499,
                ))),
            )?;
            let independent_candidate = ReviewerCandidateIdentity::new(
                WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xD502))?,
                AgentProfileId::from_uuid(Uuid::from_u128(0xD203))?,
                Uuid::from_u128(0xD303),
                AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(
                    0xD403,
                ))),
            )?;
            Ok(Self {
                workspace_uuid,
                correlation,
                leader,
                executor,
                self_candidate,
                independent_candidate,
            })
        }
    }

    fn seed_interested_reviewer(
        root: &Path,
        workspace_uuid: Uuid,
        candidate: &ReviewerCandidateIdentity,
    ) -> TestResult {
        let conn = rusqlite::Connection::open(root.join(OPERATIONS_CATALOG_FILE_NAME))?;
        conn.execute(
            "INSERT INTO ops_workspace_participations(
                workspace_id,agent_profile_id,participation_id,created_at_ms,revision
             ) VALUES(?1,?2,?3,1,1)",
            rusqlite::params![
                workspace_uuid.as_bytes(),
                candidate.profile_id().into_uuid().as_bytes(),
                candidate.participation_id().into_uuid().as_bytes(),
            ],
        )?;
        let receipt_req_id = Uuid::from_u128(0xD901);
        conn.execute(
            "INSERT INTO ops_reviewer_participation_receipts(
                workspace_id,participation_id,profile_id,participant_id,request_id,
                revision,expected_revision,statement,withdraws_request_id,withdraws_revision,
                activity_id,agent_id,session_id,display_name,recorded_at_ms
             ) VALUES(?1,?2,?3,?4,?5,1,0,'INTERESTED',NULL,NULL,?6,?7,?8,'Reviewer',10)",
            rusqlite::params![
                workspace_uuid.as_bytes(),
                candidate.participation_id().into_uuid().as_bytes(),
                candidate.profile_id().into_uuid().as_bytes(),
                candidate.participant_id().as_bytes(),
                receipt_req_id.as_bytes(),
                Uuid::from_u128(0xD902).as_bytes(),
                Uuid::from_u128(0xD903).as_bytes(),
                Uuid::from_u128(0xD904).as_bytes(),
            ],
        )?;
        conn.execute(
            "INSERT INTO ops_reviewer_participation_heads(
                workspace_id,participation_id,current_revision,request_id
             ) VALUES(?1,?2,1,?3)",
            rusqlite::params![
                workspace_uuid.as_bytes(),
                candidate.participation_id().into_uuid().as_bytes(),
                receipt_req_id.as_bytes(),
            ],
        )?;
        Ok(())
    }

    #[test]
    fn reviewer_assignment_preserves_self_review_and_unqualified_rejections() -> TestResult {
        let root = TestRoot::new("autonomy-reviewer-rejections")?;
        let runtime = OrchestrationRuntime::open(&root.0)?;
        let fixture = ReviewerGateFixture::build()?;

        let self_review_outcome =
            runtime.assign_independent_reviewer(&ReviewerAssignmentCommand {
                request_id: Uuid::from_u128(0xD601),
                correlation: fixture.correlation,
                assigning_leader: fixture.leader,
                executor: fixture.executor,
                candidates: vec![fixture.self_candidate],
                qualification_domain: None,
                assigned_at_ms: 100,
            })?;
        assert!(matches!(
            self_review_outcome,
            Err(ReviewerAssignmentError::SelfReview { .. })
        ));

        let uninterested_outcome =
            runtime.assign_independent_reviewer(&ReviewerAssignmentCommand {
                request_id: Uuid::from_u128(0xD602),
                correlation: fixture.correlation,
                assigning_leader: fixture.leader,
                executor: fixture.executor,
                candidates: vec![fixture.independent_candidate],
                qualification_domain: None,
                assigned_at_ms: 100,
            })?;
        assert!(matches!(
            uninterested_outcome,
            Err(ReviewerAssignmentError::NoQualifiedReviewer)
        ));

        assert!(matches!(
            runtime.assign_independent_reviewer(&ReviewerAssignmentCommand {
                request_id: Uuid::nil(),
                correlation: fixture.correlation,
                assigning_leader: fixture.leader,
                executor: fixture.executor,
                candidates: vec![fixture.independent_candidate],
                qualification_domain: None,
                assigned_at_ms: 100,
            }),
            Err(RuntimeError::InvalidInput)
        ));
        Ok(())
    }

    #[test]
    fn reviewer_verdict_gate_enforces_diff_digest_invalidation() -> TestResult {
        let root = TestRoot::new("autonomy-reviewer-gate")?;
        let runtime = OrchestrationRuntime::open(&root.0)?;
        let fixture = ReviewerGateFixture::build()?;

        seed_interested_reviewer(
            &root.0,
            fixture.workspace_uuid,
            &fixture.independent_candidate,
        )?;
        let assigned = runtime
            .assign_independent_reviewer(&ReviewerAssignmentCommand {
                request_id: Uuid::from_u128(0xD603),
                correlation: fixture.correlation,
                assigning_leader: fixture.leader,
                executor: fixture.executor,
                candidates: vec![fixture.independent_candidate],
                qualification_domain: None,
                assigned_at_ms: 110,
            })?
            .map_err(|err| format!("expected reviewer assignment, got {err:?}"))?;
        assert_eq!(assigned.reviewer(), fixture.independent_candidate);
        assert_eq!(
            runtime.get_reviewer_assignment(fixture.correlation)?,
            Some(assigned)
        );

        let accepted_digest = [0x5A; 32];
        let mutated_digest = [0x7C; 32];
        assert!(matches!(
            runtime.verify_node_completion_review_gate(fixture.correlation, accepted_digest)?,
            Err(ReviewerAssignmentError::ReviewNotAccepted)
        ));

        let verdict_record = runtime
            .record_review_verdict(&RecordReviewVerdictCommand {
                request_id: Uuid::from_u128(0xD701),
                correlation: fixture.correlation,
                expected_revision: 1,
                reviewer_principal: fixture.independent_candidate.principal(),
                reviewer_profile_id: fixture.independent_candidate.profile_id(),
                reviewer_participant_id: fixture.independent_candidate.participant_id(),
                outcome: ReviewVerdictOutcome::Accepted,
                handoff_diff_digest: accepted_digest,
                rationale: "Verified clean diff and tests".to_owned(),
                recorded_at_ms: 120,
            })?
            .map_err(|err| format!("expected verdict commit, got {err:?}"))?;
        assert_eq!(verdict_record.revision(), 2);

        assert!(matches!(
            runtime.verify_node_completion_review_gate(fixture.correlation, mutated_digest)?,
            Err(ReviewerAssignmentError::VerdictDiffDigestMismatch)
        ));
        let gated = runtime
            .verify_node_completion_review_gate(fixture.correlation, accepted_digest)?
            .map_err(|err| format!("expected gate acceptance, got {err:?}"))?;
        assert_eq!(gated.revision(), 2);
        Ok(())
    }
}
