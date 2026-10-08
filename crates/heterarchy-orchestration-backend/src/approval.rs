//! Axum adapter for the Approval / Auto-Approval / Admission application
//! layer.
//!
//! Thin by contract: the authenticated bearer principal is the only identity
//! source. A decision's Session selector is checked against that principal;
//! `decided_by` records its owned open logical Session, and the submission
//! principal comes from authentication — no body field
//! can attribute a decision. Approval success issues the durable admission
//! envelope; actual execution start remains Forge's fenced job.

mod mutations;
mod scoped;

use axum::extract::{Extension, Path, Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use forge_domain::{
    ApprovalRequestState, AuthenticatedPrincipal, AutoApprovalUseLimit, ExecutionActionClass,
    ExecutionPermissions,
};
use heterarchy_orchestration_operations::{
    ApprovalAutoScope, ApprovalRequestQuery, SetApprovalPolicyCommand, SubmitApprovalCommand,
};
use heterarchy_orchestration_runtime::RuntimeError;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::BackendState;
use crate::RealtimeEventKind;
use crate::app_support::{LaneError, now_millis};

/// Approval routes; merge before the router takes its state.
pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .merge(scoped::routes())
        .merge(mutations::routes())
        .route(
            "/internal/v1/workspaces/{workspace_id}/approval-policy",
            get(super::approval_policy).put(set_policy),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/approval-policy/runtime",
            get(policy_runtime),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/approval-requests",
            get(list_requests).post(submit),
        )
        .route(
            "/internal/v1/approval-requests/{request_id}/decision",
            post(decide),
        )
        .route(
            "/internal/v1/approval-automatic/disable",
            post(disable_automatic),
        )
        .route(
            "/internal/v1/approval-expiry/reconcile",
            post(reconcile_expiry),
        )
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AutoScopeBody {
    scope_id: String,
    action: String,
    ceiling: PermissionCeilingBody,
    /// `None` means the explicitly unlimited use-count policy.
    max_uses: Option<u16>,
    expires_at_ms: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PermissionCeilingBody {
    workspace_read: bool,
    workspace_write: bool,
    network: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SetPolicyBody {
    enabled: bool,
    auto_scopes: Vec<AutoScopeBody>,
    /// `None` only matches an absent policy; `Some` only matches that exact
    /// revision.
    expected_revision: Option<u64>,
}

fn action_class(value: &str) -> Result<ExecutionActionClass, LaneError> {
    match value {
        "direct_structured" => Ok(ExecutionActionClass::DirectStructured),
        "discovered_project_command" => Ok(ExecutionActionClass::DiscoveredProjectCommand),
        "shell_escape" => Ok(ExecutionActionClass::ShellEscape),
        "analyzed_shell" => Ok(ExecutionActionClass::AnalyzedShell),
        "generated_project_command" => Ok(ExecutionActionClass::GeneratedProjectCommand),
        _ => Err(LaneError::InvalidIdentity),
    }
}

fn auto_scope(body: &AutoScopeBody) -> Result<ApprovalAutoScope, LaneError> {
    let limit = match body.max_uses {
        Some(count) => AutoApprovalUseLimit::new(count).map_err(|_| LaneError::InvalidIdentity)?,
        None => AutoApprovalUseLimit::unlimited(),
    };
    ApprovalAutoScope::with_use_limit(
        canonical_uuid(Some(body.scope_id.as_str()))?,
        action_class(&body.action)?,
        ExecutionPermissions::new(
            body.ceiling.workspace_read,
            body.ceiling.workspace_write,
            body.ceiling.network,
        ),
        limit,
        body.expires_at_ms,
    )
    .map_err(|error| LaneError::from(RuntimeError::from(error)))
}

async fn set_policy(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<SetPolicyBody>,
) -> Result<Json<crate::ApprovalPolicyResponse>, LaneError> {
    let scopes = body
        .auto_scopes
        .iter()
        .map(auto_scope)
        .collect::<Result<Vec<_>, LaneError>>()?;
    let command = SetApprovalPolicyCommand {
        workspace_id: ops_workspace(canonical_uuid(Some(workspace_id.as_str()))?)?,
        enabled: body.enabled,
        auto_scopes: scopes,
        expected_revision: body.expected_revision,
        now_ms: now_millis()?,
    };
    let publisher = state.clone();
    let summary = state
        .run_operator_mutation(principal, move |runtime| {
            let summary = runtime.set_approval_policy(command)?;
            publisher.publish_state_changed(
                RealtimeEventKind::ApprovalPolicyChanged,
                summary.workspace_id,
                summary.revision,
                Some(summary.workspace_id),
            );
            Ok(summary)
        })
        .await?;
    Ok(Json(crate::ApprovalPolicyResponse::from(summary)))
}

async fn policy_runtime(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Option<PolicyRuntimeWire>>, LaneError> {
    let session = crate::app_support::session_selector(&headers)?;
    let workspace = canonical_uuid(Some(workspace_id.as_str()))?;
    let snapshot = state
        .run_workspace_query(workspace, session, principal, move |runtime| {
            runtime.approval_policy_runtime(workspace)
        })
        .await?;
    Ok(Json(snapshot.map(|view| {
        PolicyRuntimeWire {
            workspace_id: view.workspace_id,
            enabled: view.enabled,
            revision: view.revision,
            scopes: view
                .scopes
                .iter()
                .map(|scope| ScopeRuntimeWire {
                    scope_id: scope.scope_id,
                    action: scope.action,
                    ceiling: PermissionCeilingWire::from(scope.permission_ceiling),
                    max_uses: scope.max_uses,
                    expires_at_ms: scope.expires_at_ms,
                    remaining_uses: scope.remaining_uses,
                })
                .collect(),
        }
    })))
}

fn ops_workspace(value: Uuid) -> Result<forge_domain::WorkspaceId, LaneError> {
    if value.is_nil() {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(forge_domain::WorkspaceId::from_uuid(value))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SubmitBody {
    request_id: String,
    action: String,
    /// Hex-encoded SHA-256 digest of the exact execution request.
    request_digest: String,
    session_id: String,
    ceiling: PermissionCeilingBody,
    ttl_ms: i64,
}

async fn submit(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<SubmitBody>,
) -> Result<Json<SubmissionWire>, LaneError> {
    let request_id = canonical_uuid(Some(body.request_id.as_str()))?;
    let command = SubmitApprovalCommand {
        request_id: forge_domain::ApprovalRequestId::from_uuid(request_id),
        workspace_id: ops_workspace(canonical_uuid(Some(workspace_id.as_str()))?)?,
        action: action_class(&body.action)?,
        request_digest: request_digest(&body.request_digest)?,
        principal,
        session_id: canonical_uuid(Some(body.session_id.as_str()))?,
        requested_permissions: ExecutionPermissions::new(
            body.ceiling.workspace_read,
            body.ceiling.workspace_write,
            body.ceiling.network,
        ),
        ttl_ms: body.ttl_ms,
        now_ms: now_millis()?,
    };
    let publisher = state.clone();
    let submission = state
        .run_workspace_session_mutation(
            command.workspace_id.into_uuid(),
            command.session_id,
            principal,
            move |runtime| {
                let submission = runtime.submit_approval(command)?;
                if !submission.replay {
                    publisher.publish_state_changed(
                        RealtimeEventKind::ApprovalRequestChanged,
                        submission.request.request_id,
                        submission.request.revision,
                        Some(submission.request.workspace_id),
                    );
                }
                Ok(submission)
            },
        )
        .await?;
    Ok(Json(SubmissionWire::from_summary(&submission)))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DecisionBody {
    scope: crate::wire::WorkspaceQueryScope,
    session_id: Uuid,
    decision: String,
}

async fn decide(
    State(state): State<BackendState>,
    Path(request_id): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<DecisionBody>,
) -> Result<Json<DecisionWire>, LaneError> {
    crate::app_support::validate_workspace_reference(&body.scope, Some(body.session_id))?;
    let outcome = mutations::decide_in_scope(
        &state,
        body.scope,
        body.session_id,
        canonical_uuid(Some(request_id.as_str()))?,
        &body.decision,
        principal,
    )
    .await?;
    Ok(Json(DecisionWire::from_summary(&outcome)))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RequestListQuery {
    state: Option<String>,
    limit: Option<usize>,
}

async fn list_requests(
    State(state): State<BackendState>,
    Path(workspace_id): Path<String>,
    Query(query): Query<RequestListQuery>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Vec<RequestWire>>, LaneError> {
    let session = crate::app_support::session_selector(&headers)?;
    let list_state = match query.state.as_deref() {
        None => None,
        Some("pending") => Some(ApprovalRequestState::Pending),
        Some("approved") => Some(ApprovalRequestState::Approved),
        Some("rejected") => Some(ApprovalRequestState::Rejected),
        Some("auto_approved") => Some(ApprovalRequestState::AutoApproved),
        Some("denied") => Some(ApprovalRequestState::Denied),
        Some("cancelled") => Some(ApprovalRequestState::Cancelled),
        Some("expired") => Some(ApprovalRequestState::Expired),
        Some("superseded") => Some(ApprovalRequestState::Superseded),
        Some(_) => return Err(LaneError::InvalidIdentity),
    };
    let command = ApprovalRequestQuery {
        workspace_id: ops_workspace(canonical_uuid(Some(workspace_id.as_str()))?)?,
        state: list_state,
        limit: query.limit.unwrap_or(64),
    };
    let requests = state
        .run_workspace_query(
            command.workspace_id.into_uuid(),
            session,
            principal,
            move |runtime| runtime.approval_requests(command),
        )
        .await?;
    Ok(Json(
        requests.iter().map(RequestWire::from_summary).collect(),
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DisableBody {
    workspace_id: Option<String>,
}

async fn disable_automatic(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<DisableBody>,
) -> Result<Json<DisableWire>, LaneError> {
    let workspace = match &body.workspace_id {
        Some(value) => Some(canonical_uuid(Some(value.as_str()))?),
        None => None,
    };
    let now = now_millis()?;
    let publisher = state.clone();
    let summary = state
        .run_operator_control_mutation(principal, move |runtime| {
            let summary = runtime.disable_automatic_approval_observed(workspace, now)?;
            for change in &summary.policies {
                publisher.publish_state_changed(
                    RealtimeEventKind::ApprovalPolicyChanged,
                    change.workspace_id,
                    change.revision,
                    Some(change.workspace_id),
                );
            }
            if summary.requires_resync {
                publisher.require_snapshot_resync();
            }
            Ok(summary)
        })
        .await?;
    Ok(Json(DisableWire {
        disabled_policies: summary.changed,
    }))
}

async fn reconcile_expiry(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<ExpiryWire>, LaneError> {
    let now = now_millis()?;
    let publisher = state.clone();
    let summary = state
        .run_operator_control_mutation(principal, move |runtime| {
            let summary = runtime.reconcile_approval_expiry(now)?;
            for change in &summary.requests {
                publisher.publish_state_changed(
                    RealtimeEventKind::ApprovalRequestChanged,
                    change.request_id,
                    change.revision,
                    Some(change.workspace_id),
                );
            }
            if summary.requires_resync {
                publisher.require_snapshot_resync();
            }
            Ok(summary)
        })
        .await?;
    Ok(Json(ExpiryWire {
        expired_requests: summary.expired_requests,
        expired_admissions: summary.expired_admissions,
    }))
}

fn canonical_uuid(value: Option<&str>) -> Result<Uuid, LaneError> {
    let Some(value) = value else {
        return Err(LaneError::InvalidIdentity);
    };
    let parsed = Uuid::parse_str(value).map_err(|_| LaneError::InvalidIdentity)?;
    if parsed.is_nil() || parsed.as_hyphenated().to_string() != value {
        return Err(LaneError::InvalidIdentity);
    }
    Ok(parsed)
}

fn request_digest(hex: &str) -> Result<[u8; 32], LaneError> {
    if hex.len() != 64 {
        return Err(LaneError::InvalidIdentity);
    }
    let mut digest = [0_u8; 32];
    for (index, byte) in digest.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16)
            .map_err(|_| LaneError::InvalidIdentity)?;
    }
    Ok(digest)
}

// --- wire projections ---

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PolicyRuntimeWire {
    workspace_id: Uuid,
    enabled: bool,
    revision: u64,
    scopes: Vec<ScopeRuntimeWire>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ScopeRuntimeWire {
    scope_id: Uuid,
    action: &'static str,
    ceiling: PermissionCeilingWire,
    max_uses: &'static str,
    expires_at_ms: i64,
    remaining_uses: Option<u16>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PermissionCeilingWire {
    workspace_read: bool,
    workspace_write: bool,
    network: bool,
}

impl PermissionCeilingWire {
    fn from(summary: heterarchy_orchestration_runtime::approval::PermissionCeilingSummary) -> Self {
        Self {
            workspace_read: summary.workspace_read,
            workspace_write: summary.workspace_write,
            network: summary.network,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SubmissionWire {
    request: RequestWire,
    admission: Option<AdmissionWire>,
    replay: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DecisionWire {
    request: RequestWire,
    admission: Option<AdmissionWire>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RequestWire {
    request_id: Uuid,
    revision: u64,
    request_digest: String,
    workspace_id: Uuid,
    action: &'static str,
    session_id: Uuid,
    state: &'static str,
    requested_permissions: PermissionCeilingWire,
    policy_revision: Option<u64>,
    auto_scope_id: Option<Uuid>,
    requested_at_ms: i64,
    expires_at_ms: i64,
    decided_at_ms: Option<i64>,
    decided_by: Option<Uuid>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AdmissionWire {
    admission_id: Uuid,
    request_id: Uuid,
    workspace_id: Uuid,
    issuer: String,
    backend_id: String,
    issued_at_ms: i64,
    expires_at_ms: i64,
}

impl SubmissionWire {
    fn from_summary(
        summary: &heterarchy_orchestration_runtime::approval::ApprovalSubmissionSummary,
    ) -> Self {
        Self {
            request: RequestWire::from_summary(&summary.request),
            admission: summary.admission.as_ref().map(AdmissionWire::from_summary),
            replay: summary.replay,
        }
    }
}

impl DecisionWire {
    fn from_summary(
        summary: &heterarchy_orchestration_runtime::approval::ApprovalDecisionSummary,
    ) -> Self {
        Self {
            request: RequestWire::from_summary(&summary.request),
            admission: summary.admission.as_ref().map(AdmissionWire::from_summary),
        }
    }
}

impl RequestWire {
    fn from_summary(
        summary: &heterarchy_orchestration_runtime::approval::ApprovalRequestSummary,
    ) -> Self {
        Self {
            revision: summary.revision,
            request_id: summary.request_id,
            request_digest: scoped::digest_hex(&summary.request_digest),
            workspace_id: summary.workspace_id,
            action: summary.action,
            session_id: summary.session_id,
            state: summary.state,
            requested_permissions: PermissionCeilingWire::from(summary.requested_permissions),
            policy_revision: summary.policy_revision,
            auto_scope_id: summary.auto_scope_id,
            requested_at_ms: summary.requested_at_ms,
            expires_at_ms: summary.expires_at_ms,
            decided_at_ms: summary.decided_at_ms,
            decided_by: summary.decided_by,
        }
    }
}

impl AdmissionWire {
    fn from_summary(
        summary: &heterarchy_orchestration_runtime::approval::AdmissionSummary,
    ) -> Self {
        Self {
            admission_id: summary.admission_id,
            request_id: summary.request_id,
            workspace_id: summary.workspace_id,
            issuer: summary.issuer.clone(),
            backend_id: summary.backend_id.clone(),
            issued_at_ms: summary.issued_at_ms,
            expires_at_ms: summary.expires_at_ms,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DisableWire {
    disabled_policies: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ExpiryWire {
    expired_requests: u64,
    expired_admissions: u64,
}
