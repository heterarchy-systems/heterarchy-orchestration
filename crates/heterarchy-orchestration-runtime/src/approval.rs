//! Transport-neutral Approval / Auto-Approval / Admission application
//! operations.
//!
//! The durable authority (policy CAS, auto-scope matching, admission
//! envelopes, expiry reconciliation) lives in
//! `heterarchy-orchestration-operations`. This module exposes it to any
//! transport with the plan's O9 contracts preserved:
//!
//! * policy get/set behind the exact `expected_revision` CAS — an absent
//!   revision only matches an absent policy, and vice versa;
//! * `unlimited` auto-approval means "no use-count ceiling", never an
//!   exemption from expiry, disable, revision or permission-scope limits;
//! * decision attribution comes from the authenticated caller (the adapter
//!   derives `decided_by` from the bearer principal), never from a body
//!   field;
//! * approval success and actual execution start stay distinct: this layer
//!   issues the admission envelope only; Forge still fences execution;
//! * response loss is safe — the same `request_id` submission replays to
//!   the stored outcome.
//!
//! Clocks are caller-supplied; this layer never reads a clock.

use forge_domain::WorkspaceId;
use forge_domain::{
    ApprovalRequestState, AutoApprovalUseLimit, ExecutionActionClass, ExecutionPermissions,
};
use heterarchy_orchestration_operations::{
    ApprovalRequestQuery, DecideApprovalCommand, SetApprovalPolicyCommand, SubmitApprovalCommand,
};
use uuid::Uuid;

use crate::{OrchestrationRuntime, RuntimeError};

/// Bounded projection of an exact permission ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PermissionCeilingSummary {
    /// Workspace read permission bit.
    pub workspace_read: bool,
    /// Workspace write permission bit.
    pub workspace_write: bool,
    /// Network permission bit.
    pub network: bool,
}

/// Runtime policy view: the policy plus per-scope remaining use counts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalPolicyRuntimeSummary {
    /// Workspace scope.
    pub workspace_id: Uuid,
    /// Whether automatic decisions are currently enabled.
    pub enabled: bool,
    /// Exact policy revision.
    pub revision: u64,
    /// Per-scope remaining-use views.
    pub scopes: Vec<ApprovalScopeRuntimeSummary>,
}

/// One scope's ceiling and its remaining uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalScopeRuntimeSummary {
    /// Scope identity.
    pub scope_id: Uuid,
    /// Execution action class label.
    pub action: &'static str,
    /// Exact permission ceiling.
    pub permission_ceiling: PermissionCeilingSummary,
    /// Use-count ceiling label.
    pub max_uses: &'static str,
    /// Scope expiry timestamp.
    pub expires_at_ms: i64,
    /// Remaining uses for finite scopes; `None` for unlimited scopes.
    pub remaining_uses: Option<u16>,
}

/// Bounded projection of one durable approval request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalRequestSummary {
    /// Exact request identity.
    pub request_id: Uuid,
    /// Opaque canonical current-state version persisted by the approval owner.
    pub revision: u64,
    /// Workspace scope.
    pub workspace_id: Uuid,
    /// Exact SHA-256 digest of the submitted execution request.
    pub request_digest: [u8; 32],
    /// Execution action class label.
    pub action: &'static str,
    /// Submitting logical session.
    pub session_id: Uuid,
    /// Request lifecycle state label.
    pub state: &'static str,
    /// Exact permission ceiling requested.
    pub requested_permissions: PermissionCeilingSummary,
    /// Policy revision matched at submission, when auto-approved.
    pub policy_revision: Option<u64>,
    /// Auto scope matched at submission, when auto-approved.
    pub auto_scope_id: Option<Uuid>,
    /// Submission timestamp.
    pub requested_at_ms: i64,
    /// Expiry timestamp.
    pub expires_at_ms: i64,
    /// Decision timestamp, when decided.
    pub decided_at_ms: Option<i64>,
    /// Attributing decider, when approve/reject.
    pub decided_by: Option<Uuid>,
}

/// Submission outcome: the durable request, whether an admission envelope
/// was issued, and replay status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalSubmissionSummary {
    /// Durable request record.
    pub request: ApprovalRequestSummary,
    /// Issued admission envelope, when auto-approved.
    pub admission: Option<AdmissionSummary>,
    /// True when this submission replayed an existing request.
    pub replay: bool,
}

/// Decision outcome: the updated request and any envelope issued by an
/// approval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalDecisionSummary {
    /// Updated durable request record.
    pub request: ApprovalRequestSummary,
    /// Issued admission envelope, when the decision approved.
    pub admission: Option<AdmissionSummary>,
}

/// Issued admission envelope projection. Forge still validates and fences
/// the actual execution; this envelope is the typed grant handoff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionSummary {
    /// Envelope identity.
    pub admission_id: Uuid,
    /// Approving request identity.
    pub request_id: Uuid,
    /// Workspace scope.
    pub workspace_id: Uuid,
    /// Issuing Orchestration issuer identity.
    pub issuer: String,
    /// Forge backend the grant is bound to.
    pub backend_id: String,
    /// Issue timestamp.
    pub issued_at_ms: i64,
    /// Expiry timestamp.
    pub expires_at_ms: i64,
}

/// One committed request revision in a bounded bulk observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApprovalRequestVersionSummary {
    /// Exact request identity.
    pub request_id: Uuid,
    /// Workspace scope.
    pub workspace_id: Uuid,
    /// Actual committed revision.
    pub revision: u64,
}

/// One committed policy revision in a bounded bulk observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApprovalPolicyVersionSummary {
    /// Policy identity and scope.
    pub workspace_id: Uuid,
    /// Actual committed revision.
    pub revision: u64,
}

/// Automatic-approval stop result with bounded committed observations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalPolicyDisableSummary {
    /// Total policies disabled, including those beyond the observation bound.
    pub changed: usize,
    /// At most 64 actual committed policy revisions.
    pub policies: Vec<ApprovalPolicyVersionSummary>,
    /// Consumers must refresh canonical state when observations are incomplete.
    pub requires_resync: bool,
}

/// Expiry reconciliation counters and bounded committed observations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalExpirySummary {
    /// Pending requests expired by this pass.
    pub expired_requests: u64,
    /// Issued envelopes expired by this pass.
    pub expired_admissions: u64,
    /// At most 64 actual committed request revisions.
    pub requests: Vec<ApprovalRequestVersionSummary>,
    /// True for truncated observations or admission changes without versions.
    pub requires_resync: bool,
}

impl OrchestrationRuntime {
    /// Create or compare-and-set replace one workspace approval policy.
    ///
    /// # Errors
    /// Returns invalid input, stale/absent expected-revision conflict, or
    /// durable catalog failures.
    pub fn set_approval_policy(
        &self,
        command: SetApprovalPolicyCommand,
    ) -> Result<crate::ApprovalPolicySummary, RuntimeError> {
        let mut catalog = self.catalog()?;
        let view = catalog.set_approval_policy(command)?;
        Ok(crate::approval_policy_summary(&view))
    }

    /// Read the runtime policy view: per-scope ceilings and remaining use
    /// counts for the exact workspace.
    ///
    /// # Errors
    /// Returns durable catalog failures or a poisoned-owner error.
    pub fn approval_policy_runtime(
        &self,
        workspace_id: Uuid,
    ) -> Result<Option<ApprovalPolicyRuntimeSummary>, RuntimeError> {
        let mut catalog = self.catalog()?;
        let snapshot = catalog.approval_policy_runtime(workspace(workspace_id)?)?;
        Ok(snapshot.as_ref().map(|view| ApprovalPolicyRuntimeSummary {
            workspace_id: view.workspace_id.into_uuid(),
            enabled: view.enabled,
            revision: view.revision,
            scopes: view
                .scopes
                .iter()
                .map(|scope| ApprovalScopeRuntimeSummary {
                    scope_id: scope.scope.scope_id(),
                    action: action_label(scope.scope.action()),
                    permission_ceiling: permission_ceiling(scope.scope.permission_ceiling()),
                    max_uses: use_limit_label(scope.scope.max_uses()),
                    expires_at_ms: scope.scope.expires_at_ms(),
                    remaining_uses: scope.remaining_uses,
                })
                .collect(),
        }))
    }

    /// Submit one execution approval request. An auto-scope match approves
    /// immediately and issues the admission envelope in the same durable
    /// transaction; everything else parks as pending. The same `request_id`
    /// replays to the stored outcome.
    ///
    /// # Errors
    /// Returns invalid input, replay conflict, TTL bounds, a missing
    /// admission issuer for an auto-approval, or durable catalog failures.
    pub fn submit_approval(
        &self,
        command: SubmitApprovalCommand,
    ) -> Result<ApprovalSubmissionSummary, RuntimeError> {
        let mut catalog = self.catalog()?;
        let submission = catalog.submit_approval(command)?;
        Ok(ApprovalSubmissionSummary {
            request: ApprovalRequestSummary::from_record(submission.request()),
            admission: submission.admission().map(AdmissionSummary::from_issuance),
            replay: submission.replay(),
        })
    }

    /// Decide one pending request: approve (issuing the admission envelope),
    /// reject, or cancel. `workspace_id` fences the exact request scope, and
    /// `decided_by` must be the authenticated caller's owned open logical
    /// Session UUID; adapters derive and validate both before this call,
    /// never from an untrusted body identity. Expired pending requests fail
    /// closed.
    ///
    /// # Errors
    /// Returns missing/non-pending conflict, stale decision time, a missing
    /// admission issuer for an approval, or durable catalog failures.
    pub fn decide_approval(
        &self,
        command: DecideApprovalCommand,
    ) -> Result<ApprovalDecisionSummary, RuntimeError> {
        let mut catalog = self.catalog()?;
        let outcome = catalog.decide_approval(command)?;
        Ok(ApprovalDecisionSummary {
            request: ApprovalRequestSummary::from_record(outcome.request()),
            admission: outcome.admission().map(AdmissionSummary::from_issuance),
        })
    }

    /// Read one exact approval request by durable identity without scanning a
    /// workspace list.
    ///
    /// # Errors
    /// Returns durable catalog failures or a poisoned-owner error.
    pub fn approval_request(
        &self,
        request_id: Uuid,
    ) -> Result<Option<ApprovalRequestSummary>, RuntimeError> {
        let catalog = self.catalog()?;
        Ok(catalog
            .approval_request(request_id)?
            .as_ref()
            .map(ApprovalRequestSummary::from_record))
    }

    /// Exact request plus historical issuance from the same owner read snapshot.
    /// This is observation metadata, not current admission authority.
    ///
    /// # Errors
    /// Returns durable catalog or owner-lock failures.
    pub fn approval_request_snapshot(
        &self,
        request_id: Uuid,
    ) -> Result<Option<ApprovalDecisionSummary>, RuntimeError> {
        let catalog = self.catalog()?;
        Ok(catalog
            .approval_request_snapshot(request_id)?
            .map(|outcome| ApprovalDecisionSummary {
                request: ApprovalRequestSummary::from_record(outcome.request()),
                admission: outcome.admission().map(AdmissionSummary::from_issuance),
            }))
    }

    /// List one workspace's approval requests in submission order, optionally
    /// filtered by exact state, bounded by the durable authority's list limit.
    ///
    /// # Errors
    /// Returns out-of-bound limits, over-limit workspace rows, or durable
    /// catalog failures.
    pub fn approval_requests(
        &self,
        query: ApprovalRequestQuery,
    ) -> Result<Vec<ApprovalRequestSummary>, RuntimeError> {
        let catalog = self.catalog()?;
        let requests = catalog.approval_requests(query)?;
        Ok(requests
            .iter()
            .map(ApprovalRequestSummary::from_record)
            .collect())
    }

    /// Expire elapsed pending requests and issued envelopes. The caller owns
    /// the observation clock; there is no background loop.
    ///
    /// # Errors
    /// Returns invalid input or durable catalog failures.
    pub fn reconcile_approval_expiry(
        &self,
        now_ms: i64,
    ) -> Result<ApprovalExpirySummary, RuntimeError> {
        let mut catalog = self.catalog()?;
        let reconciliation = catalog.reconcile_approval_expiry(now_ms)?;
        Ok(ApprovalExpirySummary {
            expired_requests: reconciliation.expired_requests(),
            expired_admissions: reconciliation.expired_admissions(),
            requests: reconciliation
                .requests()
                .iter()
                .map(|change| ApprovalRequestVersionSummary {
                    request_id: change.request_id().into_uuid(),
                    workspace_id: change.workspace_id().into_uuid(),
                    revision: change.revision(),
                })
                .collect(),
            requires_resync: reconciliation.requires_resync(),
        })
    }

    /// Stop future automatic decisions for one workspace, or for all
    /// workspaces with `None` (operator emergency stop). Existing requests
    /// and envelopes are not rewritten.
    ///
    /// # Errors
    /// Returns invalid input, revision overflow, or durable catalog failures.
    pub fn disable_automatic_approval(
        &self,
        workspace_id: Option<Uuid>,
        now_ms: i64,
    ) -> Result<usize, RuntimeError> {
        Ok(self
            .disable_automatic_approval_observed(workspace_id, now_ms)?
            .changed)
    }

    /// Disable all selected enabled policies, retaining bounded committed versions.
    ///
    /// # Errors
    /// Returns invalid input, revision overflow, or durable catalog failures.
    pub fn disable_automatic_approval_observed(
        &self,
        workspace_id: Option<Uuid>,
        now_ms: i64,
    ) -> Result<ApprovalPolicyDisableSummary, RuntimeError> {
        let workspace = workspace_id.map(workspace).transpose()?;
        let mut catalog = self.catalog()?;
        let outcome = catalog.disable_automatic_approval_observed(workspace, now_ms)?;
        Ok(ApprovalPolicyDisableSummary {
            changed: outcome.changed(),
            policies: outcome
                .policies()
                .iter()
                .map(|change| ApprovalPolicyVersionSummary {
                    workspace_id: change.workspace_id().into_uuid(),
                    revision: change.revision(),
                })
                .collect(),
            requires_resync: outcome.requires_resync(),
        })
    }
}

fn workspace(value: Uuid) -> Result<WorkspaceId, RuntimeError> {
    if value.is_nil() {
        return Err(RuntimeError::InvalidInput);
    }
    Ok(WorkspaceId::from_uuid(value))
}

impl ApprovalRequestSummary {
    fn from_record(record: &heterarchy_orchestration_operations::ApprovalRequestRecord) -> Self {
        Self {
            revision: record.revision(),
            request_id: record.request_id().into_uuid(),
            workspace_id: record.workspace_id().into_uuid(),
            request_digest: record.request_digest(),
            action: action_label(record.action()),
            session_id: record.session_id(),
            state: state_label(record.state()),
            requested_permissions: permission_ceiling(record.requested_permissions()),
            policy_revision: record.policy_revision(),
            auto_scope_id: record.auto_scope_id(),
            requested_at_ms: record.requested_at_ms(),
            expires_at_ms: record.expires_at_ms(),
            decided_at_ms: record.decided_at_ms(),
            decided_by: record.decided_by(),
        }
    }
}

impl AdmissionSummary {
    fn from_issuance(issuance: &heterarchy_orchestration_operations::AdmissionIssuance) -> Self {
        Self {
            admission_id: issuance.admission_id(),
            request_id: issuance.request_id().into_uuid(),
            workspace_id: issuance.workspace_id().into_uuid(),
            issuer: issuance.issuer().to_string(),
            backend_id: issuance.backend_id().to_string(),
            issued_at_ms: issuance.issued_at_ms(),
            expires_at_ms: issuance.expires_at_ms(),
        }
    }
}

/// Deterministic action-class labels shared by every transport projection.
#[must_use]
pub const fn action_label(action: ExecutionActionClass) -> &'static str {
    match action {
        ExecutionActionClass::DirectStructured => "direct_structured",
        ExecutionActionClass::DiscoveredProjectCommand => "discovered_project_command",
        ExecutionActionClass::ShellEscape => "shell_escape",
        ExecutionActionClass::AnalyzedShell => "analyzed_shell",
        ExecutionActionClass::GeneratedProjectCommand => "generated_project_command",
    }
}

/// Deterministic request-state labels shared by every transport projection.
#[must_use]
pub const fn state_label(state: ApprovalRequestState) -> &'static str {
    match state {
        ApprovalRequestState::Pending => "pending",
        ApprovalRequestState::Approved => "approved",
        ApprovalRequestState::Rejected => "rejected",
        ApprovalRequestState::AutoApproved => "auto_approved",
        ApprovalRequestState::Denied => "denied",
        ApprovalRequestState::Cancelled => "cancelled",
        ApprovalRequestState::Expired => "expired",
        ApprovalRequestState::Superseded => "superseded",
    }
}

/// Deterministic permission-ceiling projection.
#[must_use]
pub const fn permission_ceiling(permissions: ExecutionPermissions) -> PermissionCeilingSummary {
    PermissionCeilingSummary {
        workspace_read: permissions.workspace_read(),
        workspace_write: permissions.workspace_write(),
        network: permissions.network(),
    }
}

/// Use-count ceiling label: `unlimited` means "no use-count ceiling" and
/// nothing else.
#[must_use]
pub const fn use_limit_label(limit: AutoApprovalUseLimit) -> &'static str {
    match limit.get() {
        Some(_) => "finite",
        None => "unlimited",
    }
}
