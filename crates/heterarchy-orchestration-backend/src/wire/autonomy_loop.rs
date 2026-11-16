//! Shared HTTP wire contracts for Orchestration autonomy-loop operations.
//!
//! Request payloads reject unknown fields (`deny_unknown_fields`) so caller typos
//! and secret-bearing fields (such as raw MCP bearer tokens) fail closed at the
//! transport boundary before reaching runtime or SQLite catalogs.

use std::fmt::Write as _;

use forge_domain::{
    ActivityId, AgentAssignmentId, AgentInstanceId, AgentSessionId, AuthenticatedPrincipal,
    DagNodeId, DagRevisionNumber, DirectTokenId, GoalId, OAuthGrantId, WorkspaceId,
};
use heterarchy_orchestration_operations::{
    DagSettlementGovernanceLink, ExecutionAuthorIdentity, LaunchLedgerRecord, LaunchLedgerState,
    OperationalActor, ProcessOwnershipIdentity, RecordedReviewVerdict,
    RegisteredWorkspaceScopeRecord, ReviewVerdictOutcome, ReviewerAssignmentRecord,
    ReviewerCandidateIdentity, WorkCorrelationKey, WorkspaceDagSettlementAuthority,
    WorkspaceParticipationId,
};
use heterarchy_orchestration_runtime::ExecutionPreferenceSummary;
use heterarchy_orchestration_sessions::{
    AgentProfileError, AgentProfileId, CliEngine, ExecutionPreference, RolePromptDigest,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{ApiError, canonical_uuid};

/// Maximum JSON request body size accepted by autonomy-loop endpoints (`64 KiB`).
pub const MAX_AUTONOMY_LOOP_BODY_BYTES: usize = 65_536;

/// Shared wire representation of an `AgentProfile` `ExecutionPreference`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecutionPreferenceWire {
    pub cli_engine: String,
    pub preferred_model: String,
    pub preferred_reasoning: Option<String>,
    pub capabilities: Vec<String>,
    pub role_prompt_digest_hex: Option<String>,
}

impl ExecutionPreferenceWire {
    /// Validate and convert this wire payload into a domain [`ExecutionPreference`].
    ///
    /// # Errors
    /// Rejects unknown CLI engines, invalid model/reasoning/capability strings,
    /// or malformed SHA-256 role-prompt hex digests.
    pub fn to_execution_preference(&self) -> Result<ExecutionPreference, AgentProfileError> {
        let engine = CliEngine::parse(&self.cli_engine)?;
        let role_prompt_ref = self
            .role_prompt_digest_hex
            .as_deref()
            .map(RolePromptDigest::from_hex)
            .transpose()?;
        ExecutionPreference::new(
            engine,
            self.preferred_model.clone(),
            self.preferred_reasoning.clone(),
            self.capabilities.clone(),
            role_prompt_ref,
        )
    }

    /// Project a validated domain [`ExecutionPreference`] into wire form.
    #[must_use]
    pub fn from_execution_preference(preference: &ExecutionPreference) -> Self {
        Self {
            cli_engine: preference.cli_engine().as_str().to_owned(),
            preferred_model: preference.preferred_model().to_owned(),
            preferred_reasoning: preference.preferred_reasoning().map(str::to_owned),
            capabilities: preference.capabilities().to_vec(),
            role_prompt_digest_hex: preference
                .role_prompt_ref()
                .as_ref()
                .map(RolePromptDigest::to_hex),
        }
    }

    /// Project a runtime [`ExecutionPreferenceSummary`] into wire form.
    #[must_use]
    pub fn from_summary(summary: &ExecutionPreferenceSummary) -> Self {
        Self {
            cli_engine: owned_str(&summary.cli_engine),
            preferred_model: summary.preferred_model.clone(),
            preferred_reasoning: summary.preferred_reasoning.clone(),
            capabilities: summary.capabilities.clone(),
            role_prompt_digest_hex: summary.role_prompt_digest_hex.clone(),
        }
    }
}

fn owned_str<S: AsRef<str> + ?Sized>(value: &S) -> String {
    value.as_ref().to_owned()
}

impl TryFrom<&ExecutionPreferenceWire> for ExecutionPreference {
    type Error = AgentProfileError;

    fn try_from(value: &ExecutionPreferenceWire) -> Result<Self, Self::Error> {
        value.to_execution_preference()
    }
}

impl TryFrom<ExecutionPreferenceWire> for ExecutionPreference {
    type Error = AgentProfileError;

    fn try_from(value: ExecutionPreferenceWire) -> Result<Self, Self::Error> {
        value.to_execution_preference()
    }
}

impl From<&ExecutionPreference> for ExecutionPreferenceWire {
    fn from(value: &ExecutionPreference) -> Self {
        Self::from_execution_preference(value)
    }
}

impl From<ExecutionPreference> for ExecutionPreferenceWire {
    fn from(value: ExecutionPreference) -> Self {
        Self::from_execution_preference(&value)
    }
}

impl From<&ExecutionPreferenceSummary> for ExecutionPreferenceWire {
    fn from(value: &ExecutionPreferenceSummary) -> Self {
        Self::from_summary(value)
    }
}

impl From<ExecutionPreferenceSummary> for ExecutionPreferenceWire {
    fn from(value: ExecutionPreferenceSummary) -> Self {
        Self::from_summary(&value)
    }
}

/// Operator request to register or advance an explicit Workspace project scope.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ScopeRegisterRequest {
    pub request_id: String,
    pub expected_revision: u64,
    pub scope: crate::wire::WorkspaceQueryScope,
}

/// Operator request to deactivate a registered Workspace project scope.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ScopeUnregisterRequest {
    pub request_id: String,
    pub expected_revision: u64,
}

/// Discriminated operator mutation to register/advance or unregister a Workspace scope.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(
    tag = "action",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum WorkspaceScopeMutationRequest {
    /// Register a new Workspace scope or advance an existing scope's revision.
    Register {
        request_id: String,
        expected_revision: u64,
        scope: crate::wire::WorkspaceQueryScope,
    },
    /// Deactivate a currently active Workspace scope.
    Unregister {
        workspace_id: String,
        request_id: String,
        expected_revision: u64,
    },
}

/// Single-Workspace scope lookup response returned by `GET /internal/v1/workspaces/{workspace_id}/scopes`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceScopeQueryResponse {
    pub workspace_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<RegisteredWorkspaceScopeResponse>,
}

/// Readback of a durable Workspace project scope registration record.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisteredWorkspaceScopeResponse {
    pub workspace_id: Uuid,
    pub location_id: Uuid,
    pub location_revision: i64,
    pub workspace_revision: i64,
    pub locator: String,
    pub active: bool,
    pub revision: u64,
    pub generation: u64,
    pub updated_at_ms: i64,
    pub replayed: bool,
}

impl From<&RegisteredWorkspaceScopeRecord> for RegisteredWorkspaceScopeResponse {
    fn from(record: &RegisteredWorkspaceScopeRecord) -> Self {
        let scope = record.scope();
        Self {
            workspace_id: scope.workspace_id(),
            location_id: scope.location_id(),
            location_revision: scope.location_revision(),
            workspace_revision: scope.workspace_revision(),
            locator: scope.locator().to_owned(),
            active: record.active(),
            revision: record.revision(),
            generation: record.generation(),
            updated_at_ms: record.updated_at_ms(),
            replayed: record.replayed(),
        }
    }
}

impl From<RegisteredWorkspaceScopeRecord> for RegisteredWorkspaceScopeResponse {
    fn from(record: RegisteredWorkspaceScopeRecord) -> Self {
        Self::from(&record)
    }
}

/// Workspace-relative correlation coordinates for one node-assignment attempt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkCorrelationWire {
    pub goal_id: String,
    pub dag_revision: i64,
    pub node_id: String,
    pub assignment_id: String,
    pub attempt: u32,
}

impl WorkCorrelationWire {
    pub(crate) fn to_correlation(
        &self,
        workspace_id: Uuid,
    ) -> Result<WorkCorrelationKey, ApiError> {
        let goal_id = GoalId::from_uuid(canonical_uuid(&self.goal_id)?);
        let dag_revision = DagRevisionNumber::new(self.dag_revision)
            .map_err(|_| ApiError::Runtime(crate::RuntimeError::InvalidInput))?;
        let node_id = DagNodeId::from_uuid(canonical_uuid(&self.node_id)?);
        let assignment_id = AgentAssignmentId::from_uuid(canonical_uuid(&self.assignment_id)?);
        WorkCorrelationKey::new(
            WorkspaceId::from_uuid(workspace_id),
            goal_id,
            dag_revision,
            node_id,
            assignment_id,
            self.attempt,
        )
        .map_err(|_| ApiError::Runtime(crate::RuntimeError::InvalidInput))
    }
}

/// Explicit principal discriminator recorded on executor, leader, or reviewer identities.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PrincipalIdentityWire {
    pub kind: String,
    #[serde(default)]
    pub id: String,
}

impl PrincipalIdentityWire {
    pub(crate) fn to_principal(&self) -> Result<AuthenticatedPrincipal, ApiError> {
        match self.kind.as_str() {
            "trusted_stdio" => {
                if !self.id.is_empty() {
                    return Err(ApiError::InvalidIdentity);
                }
                Ok(AuthenticatedPrincipal::TrustedStdio)
            }
            "direct_token" => Ok(AuthenticatedPrincipal::DirectToken(
                DirectTokenId::from_uuid(canonical_uuid(&self.id)?),
            )),
            "oauth_grant" => Ok(AuthenticatedPrincipal::OAuthGrant(OAuthGrantId::from_uuid(
                canonical_uuid(&self.id)?,
            ))),
            _ => Err(ApiError::InvalidIdentity),
        }
    }

    #[must_use]
    pub fn from_principal(principal: AuthenticatedPrincipal) -> Self {
        match principal {
            AuthenticatedPrincipal::TrustedStdio => Self {
                kind: "trusted_stdio".to_owned(),
                id: String::new(),
            },
            AuthenticatedPrincipal::DirectToken(token_id) => Self {
                kind: "direct_token".to_owned(),
                id: token_id.into_uuid().to_string(),
            },
            AuthenticatedPrincipal::OAuthGrant(grant_id) => Self {
                kind: "oauth_grant".to_owned(),
                id: grant_id.into_uuid().to_string(),
            },
        }
    }
}

/// Three-axis identity for an execution author or assigning `Team Leader`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecutionAuthorWire {
    pub profile_id: String,
    pub participant_id: String,
    pub principal: PrincipalIdentityWire,
}

impl ExecutionAuthorWire {
    pub(crate) fn to_author_identity(&self) -> Result<ExecutionAuthorIdentity, ApiError> {
        let profile_id = AgentProfileId::from_uuid(canonical_uuid(&self.profile_id)?)
            .map_err(|_| ApiError::InvalidIdentity)?;
        let participant_id = canonical_uuid(&self.participant_id)?;
        let principal = self.principal.to_principal()?;
        ExecutionAuthorIdentity::new(profile_id, participant_id, principal)
            .map_err(|_| ApiError::Runtime(crate::RuntimeError::InvalidInput))
    }
}

/// Candidate reviewer entry supplied for deterministic independent reviewer selection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReviewerCandidateWire {
    pub profile_id: String,
    pub participant_id: String,
    pub participation_id: String,
    pub principal: PrincipalIdentityWire,
    pub agent_id: String,
    pub session_id: String,
}

impl ReviewerCandidateWire {
    pub(crate) fn to_candidate_identity(&self) -> Result<ReviewerCandidateIdentity, ApiError> {
        let _agent_id = canonical_uuid(&self.agent_id)?;
        let _session_id = canonical_uuid(&self.session_id)?;
        let participation_id =
            WorkspaceParticipationId::from_uuid(canonical_uuid(&self.participation_id)?)
                .map_err(|_| ApiError::InvalidIdentity)?;
        let profile_id = AgentProfileId::from_uuid(canonical_uuid(&self.profile_id)?)
            .map_err(|_| ApiError::InvalidIdentity)?;
        let participant_id = canonical_uuid(&self.participant_id)?;
        let principal = self.principal.to_principal()?;
        ReviewerCandidateIdentity::new(participation_id, profile_id, participant_id, principal)
            .map_err(|_| ApiError::Runtime(crate::RuntimeError::InvalidInput))
    }
}

/// Request to deterministically select and bind an independent reviewer.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AssignReviewerRequest {
    pub request_id: String,
    pub correlation: WorkCorrelationWire,
    pub required_domain: String,
    pub executor: ExecutionAuthorWire,
    pub assigning_leader: ExecutionAuthorWire,
    pub candidates: Vec<ReviewerCandidateWire>,
}

/// Compare-and-swap request to record a diff-digest-bound review verdict.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RecordReviewVerdictRequest {
    pub request_id: String,
    pub correlation: WorkCorrelationWire,
    pub expected_revision: u64,
    pub reviewer_agent_id: String,
    pub reviewer_session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reviewer: Option<ExecutionAuthorWire>,
    pub verdict: String,
    pub handoff_diff_digest_hex: String,
}

/// Request to verify the fail-closed review gate immediately prior to node completion.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VerifyNodeCompletionGateRequest {
    pub correlation: WorkCorrelationWire,
    pub current_handoff_diff_digest_hex: String,
}

/// Response projection of an executor or assigning `Team Leader` three-axis identity.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedAuthorWire {
    pub profile_id: Uuid,
    pub participant_id: Uuid,
    pub principal: PrincipalIdentityWire,
}

impl From<ExecutionAuthorIdentity> for ResolvedAuthorWire {
    fn from(author: ExecutionAuthorIdentity) -> Self {
        Self {
            profile_id: author.profile_id().into_uuid(),
            participant_id: author.participant_id(),
            principal: PrincipalIdentityWire::from_principal(author.principal()),
        }
    }
}

/// Response projection of an assigned independent reviewer's durable identity.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignedReviewerWire {
    pub participation_id: Uuid,
    pub profile_id: Uuid,
    pub participant_id: Uuid,
    pub principal: PrincipalIdentityWire,
}

impl From<ReviewerCandidateIdentity> for AssignedReviewerWire {
    fn from(reviewer: ReviewerCandidateIdentity) -> Self {
        Self {
            participation_id: reviewer.participation_id().into_uuid(),
            profile_id: reviewer.profile_id().into_uuid(),
            participant_id: reviewer.participant_id(),
            principal: PrincipalIdentityWire::from_principal(reviewer.principal()),
        }
    }
}

/// Response projection of a diff-digest-bound reviewer verdict receipt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordedReviewVerdictWire {
    pub request_id: Uuid,
    pub outcome: String,
    pub handoff_diff_digest_hex: String,
    pub rationale: String,
    pub recorded_at_ms: i64,
}

impl From<&RecordedReviewVerdict> for RecordedReviewVerdictWire {
    fn from(verdict: &RecordedReviewVerdict) -> Self {
        Self {
            request_id: verdict.request_id(),
            outcome: verdict.outcome().as_str().to_owned(),
            handoff_diff_digest_hex: encode_sha256_hex(verdict.handoff_diff_digest()),
            rationale: verdict.rationale().to_owned(),
            recorded_at_ms: verdict.recorded_at_ms(),
        }
    }
}

/// Response projection of a durable [`ReviewerAssignmentRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewerAssignmentWire {
    pub assignment_record_id: Uuid,
    pub workspace_id: Uuid,
    pub goal_id: Uuid,
    pub dag_revision: i64,
    pub node_id: Uuid,
    pub assignment_id: Uuid,
    pub attempt: u32,
    pub reviewer: AssignedReviewerWire,
    pub executor: ResolvedAuthorWire,
    pub assigning_leader: ResolvedAuthorWire,
    pub qualification_domain: Option<String>,
    pub rule_generation: Option<u64>,
    pub revision: u64,
    pub assigned_at_ms: i64,
    pub verdict: Option<RecordedReviewVerdictWire>,
    pub replayed: bool,
}

impl From<&ReviewerAssignmentRecord> for ReviewerAssignmentWire {
    fn from(record: &ReviewerAssignmentRecord) -> Self {
        let correlation = record.correlation();
        Self {
            assignment_record_id: record.assignment_record_id(),
            workspace_id: correlation.workspace_id().into_uuid(),
            goal_id: correlation.goal_id().into_uuid(),
            dag_revision: correlation.dag_revision().get(),
            node_id: correlation.node_id().into_uuid(),
            assignment_id: correlation.assignment_id().into_uuid(),
            attempt: correlation.attempt(),
            reviewer: AssignedReviewerWire::from(record.reviewer()),
            executor: ResolvedAuthorWire::from(record.executor()),
            assigning_leader: ResolvedAuthorWire::from(record.assigning_leader()),
            qualification_domain: record
                .qualification_domain()
                .map(|domain| domain.as_str().to_owned()),
            rule_generation: record.rule_generation(),
            revision: record.revision(),
            assigned_at_ms: record.assigned_at_ms(),
            verdict: record.verdict().map(RecordedReviewVerdictWire::from),
            replayed: record.replayed(),
        }
    }
}

/// Structured outcome returned for reviewer assignment, verdict, and gate endpoints.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewerOperationResponse {
    pub admitted: bool,
    pub rejection_code: Option<String>,
    pub detail: Option<String>,
    pub assignment: Option<ReviewerAssignmentWire>,
}

impl ReviewerOperationResponse {
    #[must_use]
    pub fn admitted(record: &ReviewerAssignmentRecord) -> Self {
        Self {
            admitted: true,
            rejection_code: None,
            detail: None,
            assignment: Some(ReviewerAssignmentWire::from(record)),
        }
    }

    #[must_use]
    pub fn rejected(code: &'static str, detail: String) -> Self {
        Self {
            admitted: false,
            rejection_code: Some(code.to_owned()),
            detail: Some(detail),
            assignment: None,
        }
    }
}

/// Secret-free MCP direct-token grant metadata frozen into an execution snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct McpGrantWire {
    pub token_id: String,
    pub issued_at_ms: i64,
    pub expires_at_ms: i64,
}

/// Explicit sandbox permission flags frozen for one execution attempt.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecutionPermissionsWire {
    pub allow_workspace_write: bool,
    pub allow_local_shell: bool,
    pub allow_network: bool,
}

/// Bounded startup, execution, and termination grace timeouts in milliseconds.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecutionTimeoutsWire {
    pub startup_timeout_ms: u64,
    pub execution_timeout_ms: u64,
    pub termination_grace_ms: u64,
}

/// Pre-spawn request to validate and commit an immutable `AssignmentExecutionSnapshot`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PrepareExecutionSnapshotRequest {
    pub request_id: String,
    pub snapshot_id: String,
    pub goal_id: String,
    pub dag_revision: i64,
    pub node_id: String,
    pub assignment_id: String,
    pub attempt: u32,
    pub activity_id: String,
    pub profile_id: String,
    pub profile_revision: i64,
    pub resolved_engine: String,
    pub resolved_model: String,
    pub resolved_reasoning: Option<String>,
    pub role_prompt_digest_hex: Option<String>,
    pub worktree_path: String,
    pub lease_id: String,
    pub lease_revision: u64,
    pub lease_expires_at_ms: i64,
    pub mcp_grant: Option<McpGrantWire>,
    pub permissions: ExecutionPermissionsWire,
    pub timeouts: ExecutionTimeoutsWire,
}

/// OS process-group ownership witness attached to a spawned Launch Ledger entry.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessOwnershipWire {
    pub pid: u32,
    pub pgid: u32,
    pub start_time: String,
    pub boot_id: String,
}

impl From<&ProcessOwnershipIdentity> for ProcessOwnershipWire {
    fn from(ownership: &ProcessOwnershipIdentity) -> Self {
        Self {
            pid: ownership.pid(),
            pgid: ownership.pgid(),
            start_time: ownership.start_time().to_owned(),
            boot_id: ownership.boot_id().to_owned(),
        }
    }
}

/// Lifecycle state projection of a Launch Ledger row.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LaunchLedgerStateWire {
    pub status: String,
    pub ownership: Option<ProcessOwnershipWire>,
    pub exit_code: Option<i32>,
    pub unknown_reason: Option<String>,
    pub observed_at_ms: Option<i64>,
}

impl From<&LaunchLedgerState> for LaunchLedgerStateWire {
    fn from(state: &LaunchLedgerState) -> Self {
        match state {
            LaunchLedgerState::Prepared => Self {
                status: "PREPARED".to_owned(),
                ownership: None,
                exit_code: None,
                unknown_reason: None,
                observed_at_ms: None,
            },
            LaunchLedgerState::NotStarted { reconciled_at_ms } => Self {
                status: "NOT_STARTED".to_owned(),
                ownership: None,
                exit_code: None,
                unknown_reason: None,
                observed_at_ms: Some(*reconciled_at_ms),
            },
            LaunchLedgerState::Spawned {
                ownership,
                spawned_at_ms,
            } => Self {
                status: "SPAWNED".to_owned(),
                ownership: Some(ProcessOwnershipWire::from(ownership)),
                exit_code: None,
                unknown_reason: None,
                observed_at_ms: Some(*spawned_at_ms),
            },
            LaunchLedgerState::Exited {
                ownership,
                exit_code,
                exited_at_ms,
            } => Self {
                status: "EXITED".to_owned(),
                ownership: Some(ProcessOwnershipWire::from(ownership)),
                exit_code: Some(*exit_code),
                unknown_reason: None,
                observed_at_ms: Some(*exited_at_ms),
            },
            LaunchLedgerState::Unknown {
                ownership,
                reason,
                recorded_at_ms,
            } => Self {
                status: "UNKNOWN".to_owned(),
                ownership: ownership.as_ref().map(ProcessOwnershipWire::from),
                exit_code: None,
                unknown_reason: Some(reason.as_str().to_owned()),
                observed_at_ms: Some(*recorded_at_ms),
            },
        }
    }
}

/// Response projection of a durable [`LaunchLedgerRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LaunchLedgerRecordWire {
    pub snapshot_id: Uuid,
    pub workspace_id: Uuid,
    pub goal_id: Uuid,
    pub dag_revision: i64,
    pub node_id: Uuid,
    pub assignment_id: Uuid,
    pub attempt: u32,
    pub state: LaunchLedgerStateWire,
    pub revision: u64,
    pub updated_at_ms: i64,
}

impl From<&LaunchLedgerRecord> for LaunchLedgerRecordWire {
    fn from(record: &LaunchLedgerRecord) -> Self {
        let attempt = record.attempt_key();
        Self {
            snapshot_id: record.snapshot_id(),
            workspace_id: attempt.workspace_id().into_uuid(),
            goal_id: attempt.goal_id().into_uuid(),
            dag_revision: attempt.dag_revision().get(),
            node_id: attempt.node_id().into_uuid(),
            assignment_id: attempt.assignment_id().into_uuid(),
            attempt: attempt.attempt(),
            state: LaunchLedgerStateWire::from(record.state()),
            revision: record.revision(),
            updated_at_ms: record.updated_at_ms(),
        }
    }
}

/// Structured outcome returned when preparing an `AssignmentExecutionSnapshot`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionSnapshotOutcomeResponse {
    pub committed: bool,
    pub rejection_code: Option<String>,
    pub detail: Option<String>,
    pub record: Option<LaunchLedgerRecordWire>,
}

impl ExecutionSnapshotOutcomeResponse {
    #[must_use]
    pub fn committed(record: &LaunchLedgerRecord) -> Self {
        Self {
            committed: true,
            rejection_code: None,
            detail: None,
            record: Some(LaunchLedgerRecordWire::from(record)),
        }
    }

    #[must_use]
    pub fn rejected(code: &'static str, detail: String) -> Self {
        Self {
            committed: false,
            rejection_code: Some(code.to_owned()),
            detail: Some(detail),
            record: None,
        }
    }
}

/// Query parameters selecting one prepared `AssignmentExecutionSnapshot` Launch Ledger entry.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecutionSnapshotQuery {
    pub goal_id: String,
    pub dag_revision: i64,
    pub node_id: String,
    pub assignment_id: String,
    pub attempt: u32,
}

impl ExecutionSnapshotQuery {
    pub(crate) fn to_attempt_key(
        &self,
        workspace_id: Uuid,
    ) -> Result<heterarchy_orchestration_operations::ExecutionAttemptKey, ApiError> {
        let goal_id = GoalId::from_uuid(canonical_uuid(&self.goal_id)?);
        let dag_revision = DagRevisionNumber::new(self.dag_revision)
            .map_err(|_| ApiError::Runtime(crate::RuntimeError::InvalidInput))?;
        let node_id = DagNodeId::from_uuid(canonical_uuid(&self.node_id)?);
        let assignment_id = AgentAssignmentId::from_uuid(canonical_uuid(&self.assignment_id)?);
        heterarchy_orchestration_operations::ExecutionAttemptKey::new(
            WorkspaceId::from_uuid(workspace_id),
            goal_id,
            dag_revision,
            node_id,
            assignment_id,
            self.attempt,
        )
        .map_err(|_| ApiError::Runtime(crate::RuntimeError::InvalidInput))
    }
}

/// Readback response for `GET /internal/v1/workspaces/{workspace_id}/execution-snapshots`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecutionSnapshotQueryResponse {
    pub workspace_id: Uuid,
    pub goal_id: Uuid,
    pub dag_revision: i64,
    pub node_id: Uuid,
    pub assignment_id: Uuid,
    pub attempt: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<LaunchLedgerRecordWire>,
}

/// Optional participant selector for `Workspace Orchestrator` settlement fence checks.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DagSettlementParticipantWire {
    pub activity_id: String,
    pub agent_id: String,
    pub session_id: String,
}

impl DagSettlementParticipantWire {
    pub(crate) fn to_actor(&self) -> Result<OperationalActor, ApiError> {
        Ok(OperationalActor::Participant {
            activity_id: ActivityId::from_uuid(canonical_uuid(&self.activity_id)?),
            agent_id: AgentInstanceId::from_uuid(canonical_uuid(&self.agent_id)?),
            session_id: AgentSessionId::from_uuid(canonical_uuid(&self.session_id)?),
        })
    }
}

/// Request to verify `Workspace Orchestrator` leadership revision before `dag.settle`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DagSettlementAuthorityCheckRequest {
    pub expected_leadership_revision: i64,
    #[serde(default)]
    pub participant: Option<DagSettlementParticipantWire>,
}

/// Verified `Workspace Orchestrator` fence witness response.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DagSettlementAuthorityResponse {
    pub workspace_id: Uuid,
    pub leader_participant_id: Option<Uuid>,
    pub leadership_revision: i64,
}

impl From<WorkspaceDagSettlementAuthority> for DagSettlementAuthorityResponse {
    fn from(authority: WorkspaceDagSettlementAuthority) -> Self {
        Self {
            workspace_id: authority.workspace_id().into_uuid(),
            leader_participant_id: authority.leader_participant_id(),
            leadership_revision: authority.leadership_revision(),
        }
    }
}

/// Query parameters selecting a settled Goal/DAG governance link within a Workspace.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DagSettlementGovernanceQuery {
    pub goal_id: String,
    pub dag_revision: i64,
}

/// Durable governance linkage derived atomically from a `DagSettled` effect fact.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DagSettlementGovernanceResponse {
    pub workspace_id: Uuid,
    pub goal_id: Uuid,
    pub dag_revision: i64,
    pub migration_id: Uuid,
    pub source_sequence: u64,
    pub settlement_id: Uuid,
    pub coordinator_agent_id: Uuid,
    pub coordinator_participant_id: Option<Uuid>,
    pub workspace_leadership_revision: Option<i64>,
    pub accepted_work_credit_count: u64,
    pub evaluated_assignment_count: u64,
    pub started_mission_plan_id: Option<Uuid>,
    pub started_mission_plan_revision: Option<u64>,
    pub settled_at_ms: i64,
    pub recall_voter_fulfillment_eligible: bool,
}

impl From<DagSettlementGovernanceLink> for DagSettlementGovernanceResponse {
    fn from(link: DagSettlementGovernanceLink) -> Self {
        Self {
            workspace_id: link.workspace_id().into_uuid(),
            goal_id: link.goal_id().into_uuid(),
            dag_revision: link.dag_revision(),
            migration_id: link.migration_id(),
            source_sequence: link.source_sequence(),
            settlement_id: link.settlement_id(),
            coordinator_agent_id: link.coordinator_agent_id().into_uuid(),
            coordinator_participant_id: link.coordinator_participant_id(),
            workspace_leadership_revision: link.workspace_leadership_revision(),
            accepted_work_credit_count: link.accepted_work_credit_count(),
            evaluated_assignment_count: link.evaluated_assignment_count(),
            started_mission_plan_id: link.started_mission_plan_id(),
            started_mission_plan_revision: link.started_mission_plan_revision(),
            settled_at_ms: link.settled_at_ms(),
            recall_voter_fulfillment_eligible: link.recall_voter_fulfillment_eligible(),
        }
    }
}

pub(crate) fn parse_review_verdict_outcome(raw: &str) -> Result<ReviewVerdictOutcome, ApiError> {
    match raw {
        "accepted" | "ACCEPTED" => Ok(ReviewVerdictOutcome::Accepted),
        "rejected" | "REJECTED" => Ok(ReviewVerdictOutcome::Rejected),
        "needs_changes" | "needsChanges" | "NEEDS_CHANGES" => {
            Ok(ReviewVerdictOutcome::NeedsChanges)
        }
        _ => Err(ApiError::Runtime(crate::RuntimeError::InvalidInput)),
    }
}

pub(crate) fn parse_sha256_hex(raw: &str) -> Result<[u8; 32], ApiError> {
    if raw.len() != 64 {
        return Err(ApiError::Runtime(crate::RuntimeError::InvalidInput));
    }
    let bytes = raw.as_bytes();
    let mut out = [0_u8; 32];
    for (index, slot) in out.iter_mut().enumerate() {
        let high = decode_lowercase_hex_nibble(bytes[index * 2])?;
        let low = decode_lowercase_hex_nibble(bytes[index * 2 + 1])?;
        *slot = (high << 4) | low;
    }
    Ok(out)
}

const fn decode_lowercase_hex_nibble(byte: u8) -> Result<u8, ApiError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(ApiError::Runtime(crate::RuntimeError::InvalidInput)),
    }
}

/// Format a 32-byte SHA-256 digest as 64 lowercase hexadecimal ASCII characters.
#[must_use]
pub fn encode_sha256_hex(digest: [u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for byte in digest {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mcp_grant_wire_rejects_secret_or_bearer_fields() {
        let with_secret = r#"{"tokenId":"0196ed00-0000-7000-8000-000000000001","issuedAtMs":10,"expiresAtMs":20,"secret":"raw-token"}"#;
        assert!(serde_json::from_str::<McpGrantWire>(with_secret).is_err());

        let with_bearer = r#"{"tokenId":"0196ed00-0000-7000-8000-000000000001","issuedAtMs":10,"expiresAtMs":20,"bearer":"raw-token"}"#;
        assert!(serde_json::from_str::<McpGrantWire>(with_bearer).is_err());
    }

    #[test]
    fn execution_preference_wire_roundtrips_domain_value() -> Result<(), Box<dyn std::error::Error>>
    {
        let digest_hex = "a3".repeat(32);
        let wire = ExecutionPreferenceWire {
            cli_engine: "codex".to_owned(),
            preferred_model: "o4-mini".to_owned(),
            preferred_reasoning: Some("xhigh".to_owned()),
            capabilities: vec!["axum".to_owned(), "rust".to_owned()],
            role_prompt_digest_hex: Some(digest_hex),
        };
        let domain = wire.to_execution_preference()?;
        let roundtripped = ExecutionPreferenceWire::from_execution_preference(&domain);
        assert_eq!(roundtripped, wire);
        Ok(())
    }

    #[test]
    fn sha256_hex_codec_enforces_lowercase_64_char_digest() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut digest = [0_u8; 32];
        digest[0] = 0x0a;
        digest[31] = 0xfe;
        let encoded = encode_sha256_hex(digest);
        assert_eq!(encoded.len(), 64);
        assert_eq!(
            parse_sha256_hex(&encoded).map_err(|err| format!("valid hex rejected: {err:?}"))?,
            digest
        );
        assert!(parse_sha256_hex(&encoded.to_ascii_uppercase()).is_err());
        assert!(parse_sha256_hex("abcd").is_err());
        Ok(())
    }

    #[test]
    fn record_review_verdict_request_defaults_and_roundtrips_optional_reviewer()
    -> Result<(), Box<dyn std::error::Error>> {
        let omitted = RecordReviewVerdictRequest {
            request_id: "0196ed00-0000-7000-8000-000000000001".to_owned(),
            correlation: WorkCorrelationWire {
                goal_id: "0196ed00-0000-7000-8000-000000000002".to_owned(),
                dag_revision: 1,
                node_id: "0196ed00-0000-7000-8000-000000000003".to_owned(),
                assignment_id: "0196ed00-0000-7000-8000-000000000004".to_owned(),
                attempt: 1,
            },
            expected_revision: 1,
            reviewer_agent_id: "0196ed00-0000-7000-8000-000000000005".to_owned(),
            reviewer_session_id: "0196ed00-0000-7000-8000-000000000006".to_owned(),
            reviewer: None,
            verdict: "accepted".to_owned(),
            handoff_diff_digest_hex: "a3".repeat(32),
        };
        let encoded_omitted = serde_json::to_string(&omitted)?;
        assert!(!encoded_omitted.contains("\"reviewer\""));
        let decoded_omitted: RecordReviewVerdictRequest = serde_json::from_str(&encoded_omitted)?;
        assert_eq!(decoded_omitted, omitted);

        let mut with_reviewer = omitted;
        with_reviewer.reviewer = Some(ExecutionAuthorWire {
            profile_id: "0196ed00-0000-7000-8000-000000000007".to_owned(),
            participant_id: "0196ed00-0000-7000-8000-000000000008".to_owned(),
            principal: PrincipalIdentityWire {
                kind: "trusted_stdio".to_owned(),
                id: String::new(),
            },
        });
        let encoded_with = serde_json::to_string(&with_reviewer)?;
        let decoded_with: RecordReviewVerdictRequest = serde_json::from_str(&encoded_with)?;
        assert_eq!(decoded_with, with_reviewer);
        Ok(())
    }
}
