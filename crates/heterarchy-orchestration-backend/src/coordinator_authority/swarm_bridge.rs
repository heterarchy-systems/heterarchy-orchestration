//! East-West `Orchestration <-> Forge` Swarm workspace and autonomy outbox bridge.
//!
//! Routes Swarm workspace isolation, snapshot capture, 3-way merge convergence,
//! standalone/hybrid settlement, purge, and bounded outbox page reads across
//! whichever Forge transport (`SourceTransport::Control` or `SourceTransport::Http`)
//! the host configured on [`CoordinatorAuthority`].

use std::path::{Path, PathBuf};

use forge_backend::ForgeHttpClientError;
use forge_backend::ops::effect_facts::wire::EffectFactOutboxResponse;
use forge_backend::ops::swarm::{
    SettlementJournalStateWire, SwarmConvergenceOutcomeWire, SwarmMemberOutputWire,
    SwarmSettlementReceiptWire, SwarmTargetWire, SwarmWorkspaceModeWire, SwarmWorkspaceReceiptWire,
};
use forge_control::{
    ControlClientError, ControlErrorCode, ControlOperationOutcome, EffectFactOutboxReadRequest,
    MAX_SWARM_CONVERGENCE_CANDIDATES, ResolvedWorkspaceScope, SettlementJournalState,
    SwarmConvergeRequest, SwarmConvergenceCandidate, SwarmConvergenceOutcome,
    SwarmMaterializeHybridRequest, SwarmMemberCaptureRequest, SwarmMemberOutputReceipt,
    SwarmPurgeRequest, SwarmSettleRequest, SwarmSettlementReceipt, SwarmWorkspaceProvisionRequest,
    SwarmWorkspaceReceipt,
};
use forge_domain::{
    ActivityId, AgentInstanceId, AgentSessionId, AuthenticatedPrincipal, DagNodeId,
    DagRevisionNumber, GoalId, GoalRevision, SwarmMemberId, SwarmRunId, SwarmSettlementTarget,
    SwarmWorkspaceMode as ForgeSwarmWorkspaceMode, TimestampMillis,
};
use heterarchy_orchestration_operations::{
    ForgeWorktreeRef, SwarmCommitOid, SwarmRunMode, SwarmTurnOutput, SwarmWorkspaceMode,
};
use tokio::sync::OwnedSemaphorePermit;
use uuid::Uuid;

use super::{
    CoordinatorAuthority, CoordinatorAuthorityError, SourceTransport, control_job,
    map_control_service, map_operations_source_error,
};

/// Input parameters for provisioning one isolated Swarm member workspace on Forge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForgeSwarmProvisionInput {
    pub workspace_id: Uuid,
    pub session_id: Uuid,
    pub principal: AuthenticatedPrincipal,
    pub swarm_run_id: Uuid,
    pub member_id: Uuid,
    pub mode: SwarmWorkspaceMode,
    pub run_mode: SwarmRunMode,
    pub candidate_commit_oid: Option<String>,
    pub observed_at_ms: i64,
}

/// Validated baseline receipt returned after provisioning a Swarm member workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForgeSwarmWorkspaceProvisioned {
    pub worktree: ForgeWorktreeRef,
    pub baseline_commit_oid: SwarmCommitOid,
    pub baseline_tree_blake3: [u8; 32],
}

/// Input parameters for capturing one completed Swarm member workspace snapshot on Forge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ForgeSwarmCaptureInput {
    pub workspace_id: Uuid,
    pub session_id: Uuid,
    pub principal: AuthenticatedPrincipal,
    pub swarm_run_id: Uuid,
    pub member_id: Uuid,
    pub captured_at_ms: i64,
}

/// Input parameters for converging ordered Swarm candidate commits on Forge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForgeSwarmConvergeInput {
    pub workspace_id: Uuid,
    pub session_id: Uuid,
    pub principal: AuthenticatedPrincipal,
    pub swarm_run_id: Uuid,
    pub candidates: Vec<SwarmConvergenceCandidate>,
    pub observed_at_ms: i64,
}

/// Typed outcome returned by Forge Swarm candidate convergence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ForgeSwarmConvergeBridgeOutcome {
    Converged {
        converged_commit_oid: SwarmCommitOid,
        converged_tree_blake3: [u8; 32],
        converged_diff_digest: [u8; 32],
        changed_files: Vec<String>,
    },
    Conflict {
        conflicting_member_id: Uuid,
        conflicting_paths: Vec<String>,
    },
}

/// Input parameters for atomically settling a standalone Swarm run into `<workspace_root>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForgeSwarmSettleInput {
    pub workspace_id: Uuid,
    pub session_id: Uuid,
    pub principal: AuthenticatedPrincipal,
    pub activity_id: Option<Uuid>,
    pub coordinator_agent_id: Uuid,
    pub swarm_run_id: Uuid,
    pub settlement_id: Uuid,
    pub converged_commit_oid: String,
    pub expected_converged_tree_blake3: [u8; 32],
    pub expected_converged_diff_digest: [u8; 32],
    pub observed_at_ms: i64,
}

/// Validated standalone Swarm settlement outcome from Forge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForgeSwarmSettleBridgeOutcome {
    pub settlement_id: Uuid,
    pub converged_commit_oid: SwarmCommitOid,
    pub converged_tree_blake3: [u8; 32],
    pub converged_diff_digest: [u8; 32],
    pub settled_file_count: usize,
    pub settled_at_ms: i64,
    pub outbox_sequence: Option<u64>,
}

/// Input parameters for materializing a converged `DagNodeHybrid` candidate into its node worktree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForgeSwarmHybridMaterializeInput {
    pub workspace_id: Uuid,
    pub session_id: Uuid,
    pub principal: AuthenticatedPrincipal,
    pub activity_id: Uuid,
    pub swarm_run_id: Uuid,
    pub settlement_id: Uuid,
    pub goal_id: Uuid,
    pub expected_goal_revision: i64,
    pub expected_dag_revision: i64,
    pub node_id: Uuid,
    pub converged_commit_oid: String,
    pub expected_converged_tree_blake3: [u8; 32],
    pub expected_converged_diff_digest: [u8; 32],
    pub observed_at_ms: i64,
}

/// Validated `DagNodeHybrid` materialization outcome from Forge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForgeSwarmHybridMaterializeBridgeOutcome {
    pub settlement_id: Uuid,
    pub goal_id: Uuid,
    pub dag_revision: i64,
    pub node_id: Uuid,
    pub converged_commit_oid: SwarmCommitOid,
    pub converged_tree_blake3: [u8; 32],
    pub converged_diff_digest: [u8; 32],
    pub settled_file_count: usize,
    pub settled_at_ms: i64,
    pub upserted_paths: Vec<String>,
    pub deleted_paths: Vec<String>,
}

/// Input parameters for purging ephemeral Swarm worktrees and journals on Forge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ForgeSwarmPurgeInput {
    pub workspace_id: Uuid,
    pub session_id: Uuid,
    pub principal: AuthenticatedPrincipal,
    pub swarm_run_id: Uuid,
}

/// Input parameters for reading one bounded autonomy outbox page over HTTP.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ForgeAutonomyOutboxPageInput {
    pub workspace_id: Uuid,
    pub migration_id: Uuid,
    pub destination_id: Uuid,
    pub after_sequence: u64,
    pub limit: u32,
}

impl CoordinatorAuthority {
    /// Provision an isolated Swarm member workspace (`Inherit`, `Share`, or `Branch`) on Forge.
    pub(crate) async fn swarm_workspace_provision(
        &self,
        input: ForgeSwarmProvisionInput,
        permit: OwnedSemaphorePermit,
    ) -> Result<ForgeSwarmWorkspaceProvisioned, CoordinatorAuthorityError> {
        match &self.transport {
            SourceTransport::Http(_) => {
                let _permit = permit;
                let client = self.swarm_http_client()?;
                let scope = self.trusted_workspace_scope(input.workspace_id)?;
                let request = build_provision_request(scope, &input)?;
                let receipt = client
                    .swarm_workspace_provision(&request)
                    .await
                    .map_err(map_swarm_http_error)?;
                provisioned_from_http(&receipt, &request)
            }
            SourceTransport::Control { .. } => {
                let authority = self.clone();
                control_job(permit, move || {
                    let scope = authority.trusted_workspace_scope(input.workspace_id)?;
                    let request = build_provision_request(scope, &input)?;
                    let receipt = authority
                        .control()?
                        .swarm_workspace_provision(request.clone())
                        .map_err(map_swarm_control_error)?;
                    provisioned_from_control(&receipt, &request)
                })
                .await
            }
        }
    }

    /// Capture one completed Swarm member's Git snapshot into a validated [`SwarmTurnOutput`].
    pub(crate) async fn swarm_member_capture(
        &self,
        input: ForgeSwarmCaptureInput,
        permit: OwnedSemaphorePermit,
    ) -> Result<SwarmTurnOutput, CoordinatorAuthorityError> {
        match &self.transport {
            SourceTransport::Http(_) => {
                let _permit = permit;
                let client = self.swarm_http_client()?;
                let scope = self.trusted_workspace_scope(input.workspace_id)?;
                let request = build_capture_request(scope, &input)?;
                let wire = client
                    .swarm_member_capture(&request)
                    .await
                    .map_err(map_swarm_http_error)?;
                capture_from_http(&wire, &request)
            }
            SourceTransport::Control { .. } => {
                let authority = self.clone();
                control_job(permit, move || {
                    let scope = authority.trusted_workspace_scope(input.workspace_id)?;
                    let request = build_capture_request(scope, &input)?;
                    let receipt = authority
                        .control()?
                        .swarm_member_capture(request.clone())
                        .map_err(map_swarm_control_error)?;
                    capture_from_control(&receipt, &request)
                })
                .await
            }
        }
    }

    /// Converge ordered Swarm member outputs via deterministic 3-way merge on Forge.
    pub(crate) async fn swarm_converge(
        &self,
        input: ForgeSwarmConvergeInput,
        permit: OwnedSemaphorePermit,
    ) -> Result<ForgeSwarmConvergeBridgeOutcome, CoordinatorAuthorityError> {
        match &self.transport {
            SourceTransport::Http(_) => {
                let _permit = permit;
                let client = self.swarm_http_client()?;
                let scope = self.trusted_workspace_scope(input.workspace_id)?;
                let request = build_converge_request(scope, &input)?;
                let wire = client
                    .swarm_converge(&request)
                    .await
                    .map_err(map_swarm_http_error)?;
                converge_from_http(wire, request.swarm_run_id.into_uuid())
            }
            SourceTransport::Control { .. } => {
                let authority = self.clone();
                control_job(permit, move || {
                    let scope = authority.trusted_workspace_scope(input.workspace_id)?;
                    let request = build_converge_request(scope, &input)?;
                    let run_id = request.swarm_run_id.into_uuid();
                    let outcome = authority
                        .control()?
                        .swarm_converge(request)
                        .map_err(map_swarm_control_error)?;
                    converge_from_control(outcome, run_id)
                })
                .await
            }
        }
    }

    /// Atomically settle a converged standalone Swarm run into `<workspace_root>` on Forge.
    pub(crate) async fn swarm_settle_standalone(
        &self,
        input: ForgeSwarmSettleInput,
        permit: OwnedSemaphorePermit,
    ) -> Result<ForgeSwarmSettleBridgeOutcome, CoordinatorAuthorityError> {
        match &self.transport {
            SourceTransport::Http(_) => {
                let _permit = permit;
                let client = self.swarm_http_client()?;
                let scope = self.trusted_workspace_scope(input.workspace_id)?;
                let request = build_settle_request(scope, &input)?;
                let wire = client
                    .swarm_settle(&request)
                    .await
                    .map_err(map_swarm_http_error)?;
                settle_from_http(&wire, &input)
            }
            SourceTransport::Control { .. } => {
                let authority = self.clone();
                control_job(permit, move || {
                    let scope = authority.trusted_workspace_scope(input.workspace_id)?;
                    let request = build_settle_request(scope, &input)?;
                    let outcome = authority
                        .control()?
                        .swarm_settle(request)
                        .map_err(map_swarm_control_error)?;
                    settle_from_control(&outcome.receipt, outcome.outbox_sequence, &input)
                })
                .await
            }
        }
    }

    /// Materialize a converged `DagNodeHybrid` candidate into its target DAG node worktree.
    pub(crate) async fn swarm_materialize_hybrid(
        &self,
        input: ForgeSwarmHybridMaterializeInput,
        permit: OwnedSemaphorePermit,
    ) -> Result<ForgeSwarmHybridMaterializeBridgeOutcome, CoordinatorAuthorityError> {
        match &self.transport {
            SourceTransport::Http(_) => {
                let _permit = permit;
                let client = self.swarm_http_client()?;
                let scope = self.trusted_workspace_scope(input.workspace_id)?;
                let request = build_hybrid_materialize_request(scope, &input)?;
                let wire = client
                    .swarm_materialize_hybrid(&request)
                    .await
                    .map_err(map_swarm_http_error)?;
                hybrid_from_http(wire, &input)
            }
            SourceTransport::Control { .. } => {
                let authority = self.clone();
                control_job(permit, move || {
                    let scope = authority.trusted_workspace_scope(input.workspace_id)?;
                    let request = build_hybrid_materialize_request(scope, &input)?;
                    let receipt = authority
                        .control()?
                        .swarm_materialize_hybrid(request)
                        .map_err(map_swarm_control_error)?;
                    hybrid_from_control(&receipt, &input)
                })
                .await
            }
        }
    }

    /// Purge ephemeral Swarm member worktrees, manifests, and settlement journals on Forge.
    pub(crate) async fn swarm_purge(
        &self,
        input: ForgeSwarmPurgeInput,
        permit: OwnedSemaphorePermit,
    ) -> Result<bool, CoordinatorAuthorityError> {
        match &self.transport {
            SourceTransport::Http(_) => {
                let _permit = permit;
                let client = self.swarm_http_client()?;
                let scope = self.trusted_workspace_scope(input.workspace_id)?;
                let request = build_purge_request(scope, input)?;
                let wire = client
                    .swarm_purge(&request)
                    .await
                    .map_err(map_swarm_http_error)?;
                Ok(wire.purged)
            }
            SourceTransport::Control { .. } => {
                let authority = self.clone();
                control_job(permit, move || {
                    let scope = authority.trusted_workspace_scope(input.workspace_id)?;
                    let request = build_purge_request(scope, input)?;
                    authority
                        .control()?
                        .swarm_purge(request)
                        .map_err(map_swarm_control_error)?;
                    Ok(true)
                })
                .await
            }
        }
    }

    /// Fetch one bounded Forge effect-fact outbox page through `self.effect_facts` in HTTP mode.
    ///
    /// Control-backed authorities fail closed with [`CoordinatorAuthorityError::Unavailable`].
    pub(crate) async fn autonomy_outbox_page(
        &self,
        input: ForgeAutonomyOutboxPageInput,
        permit: OwnedSemaphorePermit,
    ) -> Result<EffectFactOutboxResponse, CoordinatorAuthorityError> {
        let _permit = permit;
        let client = self.swarm_http_client()?;
        let scope = self.trusted_workspace_scope(input.workspace_id)?;
        ensure_non_nil(input.migration_id)?;
        ensure_non_nil(input.destination_id)?;
        if input.limit == 0 {
            return Err(CoordinatorAuthorityError::Stale);
        }
        client
            .effect_fact_outbox(&EffectFactOutboxReadRequest {
                scope,
                migration_id: input.migration_id,
                destination_id: input.destination_id,
                after: input.after_sequence,
                limit: input.limit,
            })
            .await
            .map_err(map_swarm_http_error)
    }

    fn swarm_http_client(
        &self,
    ) -> Result<&forge_backend::ForgeHttpClient, CoordinatorAuthorityError> {
        if !matches!(&self.transport, SourceTransport::Http(_)) {
            return Err(CoordinatorAuthorityError::Unavailable);
        }
        self.effect_facts
            .as_ref()
            .ok_or(CoordinatorAuthorityError::Unavailable)
    }
}

fn build_provision_request(
    scope: ResolvedWorkspaceScope,
    input: &ForgeSwarmProvisionInput,
) -> Result<SwarmWorkspaceProvisionRequest, CoordinatorAuthorityError> {
    ensure_non_nil(input.session_id)?;
    ensure_non_nil(input.swarm_run_id)?;
    ensure_non_nil(input.member_id)?;
    let observed_at = parse_timestamp(input.observed_at_ms)?;
    let target = to_forge_settlement_target(input.run_mode)?;
    let candidate_commit_oid = input
        .candidate_commit_oid
        .as_deref()
        .map(|raw| {
            parse_commit_oid(raw, CoordinatorAuthorityError::Stale)
                .map(|oid| oid.as_str().to_owned())
        })
        .transpose()?;
    Ok(SwarmWorkspaceProvisionRequest {
        scope,
        session_id: AgentSessionId::from_uuid(input.session_id),
        principal: input.principal,
        swarm_run_id: SwarmRunId::from_uuid(input.swarm_run_id),
        member_id: SwarmMemberId::from_uuid(input.member_id),
        mode: to_forge_workspace_mode(input.mode),
        target,
        candidate_commit_oid,
        observed_at,
    })
}

fn build_capture_request(
    scope: ResolvedWorkspaceScope,
    input: &ForgeSwarmCaptureInput,
) -> Result<SwarmMemberCaptureRequest, CoordinatorAuthorityError> {
    ensure_non_nil(input.session_id)?;
    ensure_non_nil(input.swarm_run_id)?;
    ensure_non_nil(input.member_id)?;
    let captured_at = parse_timestamp(input.captured_at_ms)?;
    Ok(SwarmMemberCaptureRequest {
        scope,
        session_id: AgentSessionId::from_uuid(input.session_id),
        principal: input.principal,
        swarm_run_id: SwarmRunId::from_uuid(input.swarm_run_id),
        member_id: SwarmMemberId::from_uuid(input.member_id),
        captured_at,
    })
}

fn build_converge_request(
    scope: ResolvedWorkspaceScope,
    input: &ForgeSwarmConvergeInput,
) -> Result<SwarmConvergeRequest, CoordinatorAuthorityError> {
    ensure_non_nil(input.session_id)?;
    ensure_non_nil(input.swarm_run_id)?;
    let observed_at = parse_timestamp(input.observed_at_ms)?;
    if input.candidates.is_empty() || input.candidates.len() > MAX_SWARM_CONVERGENCE_CANDIDATES {
        return Err(CoordinatorAuthorityError::Stale);
    }
    let mut normalized = Vec::with_capacity(input.candidates.len());
    for candidate in &input.candidates {
        ensure_non_nil(candidate.member_id.into_uuid())?;
        let expected_output_commit_oid = candidate
            .expected_output_commit_oid
            .as_deref()
            .map(|raw| {
                parse_commit_oid(raw, CoordinatorAuthorityError::Stale)
                    .map(|oid| oid.as_str().to_owned())
            })
            .transpose()?;
        let expected_diff_digest = candidate
            .expected_diff_digest
            .map(|digest| ensure_non_zero_digest(digest, CoordinatorAuthorityError::Stale))
            .transpose()?;
        normalized.push(SwarmConvergenceCandidate {
            member_id: candidate.member_id,
            expected_output_commit_oid,
            expected_diff_digest,
        });
    }
    Ok(SwarmConvergeRequest {
        scope,
        session_id: AgentSessionId::from_uuid(input.session_id),
        principal: input.principal,
        swarm_run_id: SwarmRunId::from_uuid(input.swarm_run_id),
        candidates: normalized,
        observed_at,
    })
}

fn build_settle_request(
    scope: ResolvedWorkspaceScope,
    input: &ForgeSwarmSettleInput,
) -> Result<SwarmSettleRequest, CoordinatorAuthorityError> {
    ensure_non_nil(input.session_id)?;
    ensure_non_nil(input.coordinator_agent_id)?;
    ensure_non_nil(input.swarm_run_id)?;
    ensure_non_nil(input.settlement_id)?;
    if let Some(activity_id) = input.activity_id {
        ensure_non_nil(activity_id)?;
    }
    let settled_at = parse_timestamp(input.observed_at_ms)?;
    let converged_commit_oid = parse_commit_oid(
        &input.converged_commit_oid,
        CoordinatorAuthorityError::Stale,
    )?;
    ensure_non_zero_digest(
        input.expected_converged_tree_blake3,
        CoordinatorAuthorityError::Stale,
    )?;
    let expected_diff_digest = ensure_non_zero_digest(
        input.expected_converged_diff_digest,
        CoordinatorAuthorityError::Stale,
    )?;
    Ok(SwarmSettleRequest {
        scope,
        activity_id: input.activity_id.map(ActivityId::from_uuid),
        session_id: AgentSessionId::from_uuid(input.session_id),
        principal: input.principal,
        coordinator_agent_id: AgentInstanceId::from_uuid(input.coordinator_agent_id),
        swarm_run_id: SwarmRunId::from_uuid(input.swarm_run_id),
        settlement_id: input.settlement_id,
        converged_commit_oid: converged_commit_oid.as_str().to_owned(),
        expected_diff_digest: Some(expected_diff_digest),
        settled_at,
    })
}

fn build_hybrid_materialize_request(
    scope: ResolvedWorkspaceScope,
    input: &ForgeSwarmHybridMaterializeInput,
) -> Result<SwarmMaterializeHybridRequest, CoordinatorAuthorityError> {
    ensure_non_nil(input.session_id)?;
    ensure_non_nil(input.activity_id)?;
    ensure_non_nil(input.swarm_run_id)?;
    ensure_non_nil(input.settlement_id)?;
    ensure_non_nil(input.goal_id)?;
    ensure_non_nil(input.node_id)?;
    let settled_at = parse_timestamp(input.observed_at_ms)?;
    let expected_goal_revision = GoalRevision::new(input.expected_goal_revision)
        .map_err(|_| CoordinatorAuthorityError::Stale)?;
    let expected_dag_revision = DagRevisionNumber::new(input.expected_dag_revision)
        .map_err(|_| CoordinatorAuthorityError::Stale)?;
    let converged_commit_oid = parse_commit_oid(
        &input.converged_commit_oid,
        CoordinatorAuthorityError::Stale,
    )?;
    ensure_non_zero_digest(
        input.expected_converged_tree_blake3,
        CoordinatorAuthorityError::Stale,
    )?;
    let expected_diff_digest = ensure_non_zero_digest(
        input.expected_converged_diff_digest,
        CoordinatorAuthorityError::Stale,
    )?;
    Ok(SwarmMaterializeHybridRequest {
        scope,
        activity_id: ActivityId::from_uuid(input.activity_id),
        session_id: AgentSessionId::from_uuid(input.session_id),
        principal: input.principal,
        swarm_run_id: SwarmRunId::from_uuid(input.swarm_run_id),
        settlement_id: input.settlement_id,
        goal_id: GoalId::from_uuid(input.goal_id),
        expected_goal_revision,
        expected_dag_revision,
        node_id: DagNodeId::from_uuid(input.node_id),
        converged_commit_oid: converged_commit_oid.as_str().to_owned(),
        expected_diff_digest: Some(expected_diff_digest),
        settled_at,
    })
}

fn build_purge_request(
    scope: ResolvedWorkspaceScope,
    input: ForgeSwarmPurgeInput,
) -> Result<SwarmPurgeRequest, CoordinatorAuthorityError> {
    ensure_non_nil(input.session_id)?;
    ensure_non_nil(input.swarm_run_id)?;
    Ok(SwarmPurgeRequest {
        scope,
        session_id: AgentSessionId::from_uuid(input.session_id),
        principal: input.principal,
        swarm_run_id: SwarmRunId::from_uuid(input.swarm_run_id),
    })
}

struct ParsedProvisionParts<'a> {
    swarm_run_id: Uuid,
    member_id: Uuid,
    mode: ForgeSwarmWorkspaceMode,
    target: SwarmSettlementTarget,
    worktree_path: &'a Path,
    input_commit_oid: &'a str,
    initial_tree_blake3: &'a str,
}

fn provisioned_from_control(
    receipt: &SwarmWorkspaceReceipt,
    request: &SwarmWorkspaceProvisionRequest,
) -> Result<ForgeSwarmWorkspaceProvisioned, CoordinatorAuthorityError> {
    provisioned_from_parts(
        &ParsedProvisionParts {
            swarm_run_id: receipt.swarm_run_id().into_uuid(),
            member_id: receipt.member_id().into_uuid(),
            mode: receipt.mode(),
            target: receipt.target(),
            worktree_path: receipt.worktree_path(),
            input_commit_oid: receipt.input_commit_oid(),
            initial_tree_blake3: receipt.initial_tree_blake3(),
        },
        request,
    )
}

fn provisioned_from_http(
    wire: &SwarmWorkspaceReceiptWire,
    request: &SwarmWorkspaceProvisionRequest,
) -> Result<ForgeSwarmWorkspaceProvisioned, CoordinatorAuthorityError> {
    provisioned_from_parts(
        &ParsedProvisionParts {
            swarm_run_id: wire.swarm_run_id,
            member_id: wire.member_id,
            mode: wire_mode_to_forge(wire.mode),
            target: wire_target_to_forge(wire.target)?,
            worktree_path: Path::new(&wire.worktree_path),
            input_commit_oid: &wire.input_commit_oid,
            initial_tree_blake3: &wire.initial_tree_blake3,
        },
        request,
    )
}

fn provisioned_from_parts(
    parts: &ParsedProvisionParts<'_>,
    request: &SwarmWorkspaceProvisionRequest,
) -> Result<ForgeSwarmWorkspaceProvisioned, CoordinatorAuthorityError> {
    if parts.swarm_run_id != request.swarm_run_id.into_uuid()
        || parts.member_id != request.member_id.into_uuid()
        || parts.mode != request.mode
        || parts.target != request.target
    {
        return Err(CoordinatorAuthorityError::Unavailable);
    }
    let worktree = ForgeWorktreeRef::new(PathBuf::from(parts.worktree_path))
        .map_err(|_| CoordinatorAuthorityError::Unavailable)?;
    let baseline_commit_oid = parse_commit_oid(
        parts.input_commit_oid,
        CoordinatorAuthorityError::Unavailable,
    )?;
    let baseline_tree_blake3 = decode_hex_digest_32(
        parts.initial_tree_blake3,
        CoordinatorAuthorityError::Unavailable,
    )?;
    Ok(ForgeSwarmWorkspaceProvisioned {
        worktree,
        baseline_commit_oid,
        baseline_tree_blake3,
    })
}

fn capture_from_control(
    receipt: &SwarmMemberOutputReceipt,
    request: &SwarmMemberCaptureRequest,
) -> Result<SwarmTurnOutput, CoordinatorAuthorityError> {
    capture_from_parts(
        receipt.swarm_run_id().into_uuid(),
        receipt.member_id().into_uuid(),
        receipt.output_commit_oid(),
        receipt.final_tree_blake3(),
        receipt.diff_digest(),
        request,
    )
}

fn capture_from_http(
    wire: &SwarmMemberOutputWire,
    request: &SwarmMemberCaptureRequest,
) -> Result<SwarmTurnOutput, CoordinatorAuthorityError> {
    let diff_digest =
        decode_hex_digest_32(&wire.diff_digest, CoordinatorAuthorityError::Unavailable)?;
    capture_from_parts(
        wire.swarm_run_id,
        wire.member_id,
        &wire.output_commit_oid,
        &wire.final_tree_blake3,
        diff_digest,
        request,
    )
}

fn capture_from_parts(
    swarm_run_id: Uuid,
    member_id: Uuid,
    output_commit_oid: &str,
    final_tree_blake3: &str,
    diff_digest: [u8; 32],
    request: &SwarmMemberCaptureRequest,
) -> Result<SwarmTurnOutput, CoordinatorAuthorityError> {
    if swarm_run_id != request.swarm_run_id.into_uuid()
        || member_id != request.member_id.into_uuid()
    {
        return Err(CoordinatorAuthorityError::Unavailable);
    }
    ensure_non_zero_digest(diff_digest, CoordinatorAuthorityError::Unavailable)?;
    let commit_oid = parse_commit_oid(output_commit_oid, CoordinatorAuthorityError::Unavailable)?;
    let tree_blake3 =
        decode_hex_digest_32(final_tree_blake3, CoordinatorAuthorityError::Unavailable)?;
    Ok(SwarmTurnOutput {
        commit_oid,
        tree_blake3,
    })
}

fn converge_from_control(
    outcome: SwarmConvergenceOutcome,
    expected_run_id: Uuid,
) -> Result<ForgeSwarmConvergeBridgeOutcome, CoordinatorAuthorityError> {
    match outcome {
        SwarmConvergenceOutcome::Converged {
            converged_commit_oid,
            converged_tree_blake3,
            converged_diff_digest,
            changed_files,
        } => converged_outcome_from_parts(
            expected_run_id,
            expected_run_id,
            &converged_commit_oid,
            &converged_tree_blake3,
            converged_diff_digest,
            changed_files,
        ),
        SwarmConvergenceOutcome::Conflict {
            conflicting_member_id,
            details,
        } => conflict_outcome_from_parts(
            expected_run_id,
            expected_run_id,
            conflicting_member_id.into_uuid(),
            details,
        ),
    }
}

fn converge_from_http(
    wire: SwarmConvergenceOutcomeWire,
    expected_run_id: Uuid,
) -> Result<ForgeSwarmConvergeBridgeOutcome, CoordinatorAuthorityError> {
    match wire {
        SwarmConvergenceOutcomeWire::Converged {
            swarm_run_id,
            converged_commit_oid,
            converged_tree_blake3,
            converged_diff_digest,
            changed_files,
        } => {
            let diff_digest = decode_hex_digest_32(
                &converged_diff_digest,
                CoordinatorAuthorityError::Unavailable,
            )?;
            converged_outcome_from_parts(
                swarm_run_id,
                expected_run_id,
                &converged_commit_oid,
                &converged_tree_blake3,
                diff_digest,
                changed_files,
            )
        }
        SwarmConvergenceOutcomeWire::Conflict {
            swarm_run_id,
            conflicting_member_id,
            details,
        } => conflict_outcome_from_parts(
            swarm_run_id,
            expected_run_id,
            conflicting_member_id,
            details,
        ),
    }
}

fn converged_outcome_from_parts(
    swarm_run_id: Uuid,
    expected_run_id: Uuid,
    converged_commit_oid: &str,
    converged_tree_blake3: &str,
    converged_diff_digest: [u8; 32],
    changed_files: Vec<String>,
) -> Result<ForgeSwarmConvergeBridgeOutcome, CoordinatorAuthorityError> {
    if swarm_run_id != expected_run_id {
        return Err(CoordinatorAuthorityError::Unavailable);
    }
    let commit_oid =
        parse_commit_oid(converged_commit_oid, CoordinatorAuthorityError::Unavailable)?;
    let tree_blake3 = decode_hex_digest_32(
        converged_tree_blake3,
        CoordinatorAuthorityError::Unavailable,
    )?;
    let diff_digest = ensure_non_zero_digest(
        converged_diff_digest,
        CoordinatorAuthorityError::Unavailable,
    )?;
    Ok(ForgeSwarmConvergeBridgeOutcome::Converged {
        converged_commit_oid: commit_oid,
        converged_tree_blake3: tree_blake3,
        converged_diff_digest: diff_digest,
        changed_files,
    })
}

fn conflict_outcome_from_parts(
    swarm_run_id: Uuid,
    expected_run_id: Uuid,
    conflicting_member_id: Uuid,
    conflicting_paths: Vec<String>,
) -> Result<ForgeSwarmConvergeBridgeOutcome, CoordinatorAuthorityError> {
    if swarm_run_id != expected_run_id || conflicting_member_id.is_nil() {
        return Err(CoordinatorAuthorityError::Unavailable);
    }
    Ok(ForgeSwarmConvergeBridgeOutcome::Conflict {
        conflicting_member_id,
        conflicting_paths,
    })
}

struct ParsedSettlementParts<'a> {
    settlement_id: &'a str,
    swarm_run_id: Uuid,
    target: SwarmSettlementTarget,
    applied: bool,
    settled_at_ms: i64,
    converged_commit_oid: &'a str,
    settled_tree_blake3: &'a str,
    converged_diff_digest: [u8; 32],
    settled_file_count: u64,
}

fn settle_from_control(
    receipt: &SwarmSettlementReceipt,
    outbox_sequence: Option<u64>,
    input: &ForgeSwarmSettleInput,
) -> Result<ForgeSwarmSettleBridgeOutcome, CoordinatorAuthorityError> {
    settle_from_parts(
        &ParsedSettlementParts {
            settlement_id: receipt.settlement_id(),
            swarm_run_id: receipt.swarm_run_id().into_uuid(),
            target: receipt.target(),
            applied: receipt.state() == SettlementJournalState::Applied,
            settled_at_ms: receipt.settled_at().get(),
            converged_commit_oid: receipt.converged_commit_oid(),
            settled_tree_blake3: receipt.settled_tree_blake3(),
            converged_diff_digest: receipt.converged_diff_digest(),
            settled_file_count: receipt.settled_file_count(),
        },
        outbox_sequence,
        input,
    )
}

fn settle_from_http(
    wire: &forge_backend::ops::swarm::SwarmSettleWire,
    input: &ForgeSwarmSettleInput,
) -> Result<ForgeSwarmSettleBridgeOutcome, CoordinatorAuthorityError> {
    let diff_digest = decode_hex_digest_32(
        &wire.receipt.converged_diff_digest,
        CoordinatorAuthorityError::Unavailable,
    )?;
    let target = wire_target_to_forge(wire.receipt.target)?;
    settle_from_parts(
        &ParsedSettlementParts {
            settlement_id: &wire.receipt.settlement_id,
            swarm_run_id: wire.receipt.swarm_run_id,
            target,
            applied: wire.receipt.state == SettlementJournalStateWire::Applied,
            settled_at_ms: wire.receipt.settled_at_ms,
            converged_commit_oid: &wire.receipt.converged_commit_oid,
            settled_tree_blake3: &wire.receipt.settled_tree_blake3,
            converged_diff_digest: diff_digest,
            settled_file_count: wire.receipt.settled_file_count,
        },
        wire.outbox_sequence,
        input,
    )
}

fn settle_from_parts(
    parts: &ParsedSettlementParts<'_>,
    outbox_sequence: Option<u64>,
    input: &ForgeSwarmSettleInput,
) -> Result<ForgeSwarmSettleBridgeOutcome, CoordinatorAuthorityError> {
    if outbox_sequence == Some(0) || parts.target != SwarmSettlementTarget::StandaloneSwarm {
        return Err(CoordinatorAuthorityError::Unavailable);
    }
    let validated = validate_settled_receipt(
        parts,
        input.settlement_id,
        input.swarm_run_id,
        &input.converged_commit_oid,
        input.expected_converged_tree_blake3,
        input.expected_converged_diff_digest,
    )?;
    Ok(ForgeSwarmSettleBridgeOutcome {
        settlement_id: validated.settlement_id,
        converged_commit_oid: validated.commit_oid,
        converged_tree_blake3: validated.tree_blake3,
        converged_diff_digest: validated.diff_digest,
        settled_file_count: validated.settled_file_count,
        settled_at_ms: validated.settled_at_ms,
        outbox_sequence,
    })
}

fn hybrid_from_control(
    receipt: &SwarmSettlementReceipt,
    input: &ForgeSwarmHybridMaterializeInput,
) -> Result<ForgeSwarmHybridMaterializeBridgeOutcome, CoordinatorAuthorityError> {
    let parts = ParsedSettlementParts {
        settlement_id: receipt.settlement_id(),
        swarm_run_id: receipt.swarm_run_id().into_uuid(),
        target: receipt.target(),
        applied: receipt.state() == SettlementJournalState::Applied,
        settled_at_ms: receipt.settled_at().get(),
        converged_commit_oid: receipt.converged_commit_oid(),
        settled_tree_blake3: receipt.settled_tree_blake3(),
        converged_diff_digest: receipt.converged_diff_digest(),
        settled_file_count: receipt.settled_file_count(),
    };
    hybrid_from_parts(
        &parts,
        receipt.upserted_paths().to_vec(),
        receipt.deleted_paths().to_vec(),
        input,
    )
}

fn hybrid_from_http(
    wire: SwarmSettlementReceiptWire,
    input: &ForgeSwarmHybridMaterializeInput,
) -> Result<ForgeSwarmHybridMaterializeBridgeOutcome, CoordinatorAuthorityError> {
    let diff_digest = decode_hex_digest_32(
        &wire.converged_diff_digest,
        CoordinatorAuthorityError::Unavailable,
    )?;
    let target = wire_target_to_forge(wire.target)?;
    let parts = ParsedSettlementParts {
        settlement_id: &wire.settlement_id,
        swarm_run_id: wire.swarm_run_id,
        target,
        applied: wire.state == SettlementJournalStateWire::Applied,
        settled_at_ms: wire.settled_at_ms,
        converged_commit_oid: &wire.converged_commit_oid,
        settled_tree_blake3: &wire.settled_tree_blake3,
        converged_diff_digest: diff_digest,
        settled_file_count: wire.settled_file_count,
    };
    hybrid_from_parts(&parts, wire.upserted_paths, wire.deleted_paths, input)
}

fn hybrid_from_parts(
    parts: &ParsedSettlementParts<'_>,
    upserted_paths: Vec<String>,
    deleted_paths: Vec<String>,
    input: &ForgeSwarmHybridMaterializeInput,
) -> Result<ForgeSwarmHybridMaterializeBridgeOutcome, CoordinatorAuthorityError> {
    let expected_dag_revision = DagRevisionNumber::new(input.expected_dag_revision)
        .map_err(|_| CoordinatorAuthorityError::Stale)?;
    let expected_target = SwarmSettlementTarget::DagNodeHybrid {
        goal_id: GoalId::from_uuid(input.goal_id),
        dag_revision: expected_dag_revision,
        node_id: DagNodeId::from_uuid(input.node_id),
    };
    if parts.target != expected_target {
        return Err(CoordinatorAuthorityError::Unavailable);
    }
    let validated = validate_settled_receipt(
        parts,
        input.settlement_id,
        input.swarm_run_id,
        &input.converged_commit_oid,
        input.expected_converged_tree_blake3,
        input.expected_converged_diff_digest,
    )?;
    Ok(ForgeSwarmHybridMaterializeBridgeOutcome {
        settlement_id: validated.settlement_id,
        goal_id: input.goal_id,
        dag_revision: input.expected_dag_revision,
        node_id: input.node_id,
        converged_commit_oid: validated.commit_oid,
        converged_tree_blake3: validated.tree_blake3,
        converged_diff_digest: validated.diff_digest,
        settled_file_count: validated.settled_file_count,
        settled_at_ms: validated.settled_at_ms,
        upserted_paths,
        deleted_paths,
    })
}

struct ValidatedSettlement {
    settlement_id: Uuid,
    commit_oid: SwarmCommitOid,
    tree_blake3: [u8; 32],
    diff_digest: [u8; 32],
    settled_file_count: usize,
    settled_at_ms: i64,
}

fn validate_settled_receipt(
    parts: &ParsedSettlementParts<'_>,
    expected_settlement_id: Uuid,
    expected_swarm_run_id: Uuid,
    expected_commit_oid: &str,
    expected_tree_blake3: [u8; 32],
    expected_diff_digest: [u8; 32],
) -> Result<ValidatedSettlement, CoordinatorAuthorityError> {
    if !parts.applied || parts.swarm_run_id != expected_swarm_run_id || parts.settled_at_ms < 0 {
        return Err(CoordinatorAuthorityError::Unavailable);
    }
    let settlement_id = parse_settlement_uuid(parts.settlement_id, expected_settlement_id)?;
    let expected_oid = parse_commit_oid(expected_commit_oid, CoordinatorAuthorityError::Stale)?;
    let commit_oid = parse_commit_oid(
        parts.converged_commit_oid,
        CoordinatorAuthorityError::Unavailable,
    )?;
    if commit_oid != expected_oid {
        return Err(CoordinatorAuthorityError::Unavailable);
    }
    let tree_blake3 = decode_hex_digest_32(
        parts.settled_tree_blake3,
        CoordinatorAuthorityError::Unavailable,
    )?;
    let diff_digest = ensure_non_zero_digest(
        parts.converged_diff_digest,
        CoordinatorAuthorityError::Unavailable,
    )?;
    if tree_blake3 != expected_tree_blake3 || diff_digest != expected_diff_digest {
        return Err(CoordinatorAuthorityError::Stale);
    }
    let settled_file_count = usize::try_from(parts.settled_file_count)
        .map_err(|_| CoordinatorAuthorityError::Unavailable)?;
    Ok(ValidatedSettlement {
        settlement_id,
        commit_oid,
        tree_blake3,
        diff_digest,
        settled_file_count,
        settled_at_ms: parts.settled_at_ms,
    })
}

const fn to_forge_workspace_mode(mode: SwarmWorkspaceMode) -> ForgeSwarmWorkspaceMode {
    match mode {
        SwarmWorkspaceMode::Inherit => ForgeSwarmWorkspaceMode::Inherit,
        SwarmWorkspaceMode::Share => ForgeSwarmWorkspaceMode::Share,
        SwarmWorkspaceMode::Branch => ForgeSwarmWorkspaceMode::Branch,
    }
}

fn to_forge_settlement_target(
    run_mode: SwarmRunMode,
) -> Result<SwarmSettlementTarget, CoordinatorAuthorityError> {
    match run_mode {
        SwarmRunMode::Standalone => Ok(SwarmSettlementTarget::StandaloneSwarm),
        SwarmRunMode::DagNodeHybrid {
            goal_id,
            dag_revision,
            node_id,
            declared_write_manifest_blake3,
        } => {
            ensure_non_nil(goal_id.into_uuid())?;
            ensure_non_nil(node_id.into_uuid())?;
            if dag_revision.get() < 1 {
                return Err(CoordinatorAuthorityError::Stale);
            }
            ensure_non_zero_digest(
                declared_write_manifest_blake3,
                CoordinatorAuthorityError::Stale,
            )?;
            Ok(SwarmSettlementTarget::DagNodeHybrid {
                goal_id,
                dag_revision,
                node_id,
            })
        }
    }
}

const fn wire_mode_to_forge(mode: SwarmWorkspaceModeWire) -> ForgeSwarmWorkspaceMode {
    match mode {
        SwarmWorkspaceModeWire::Inherit => ForgeSwarmWorkspaceMode::Inherit,
        SwarmWorkspaceModeWire::Share => ForgeSwarmWorkspaceMode::Share,
        SwarmWorkspaceModeWire::Branch => ForgeSwarmWorkspaceMode::Branch,
    }
}

fn wire_target_to_forge(
    target: SwarmTargetWire,
) -> Result<SwarmSettlementTarget, CoordinatorAuthorityError> {
    match target {
        SwarmTargetWire::StandaloneSwarm => Ok(SwarmSettlementTarget::StandaloneSwarm),
        SwarmTargetWire::DagNodeHybrid {
            goal_id,
            dag_revision,
            node_id,
        } => {
            if goal_id.is_nil() || node_id.is_nil() {
                return Err(CoordinatorAuthorityError::Unavailable);
            }
            let dag_revision = DagRevisionNumber::new(dag_revision)
                .map_err(|_| CoordinatorAuthorityError::Unavailable)?;
            Ok(SwarmSettlementTarget::DagNodeHybrid {
                goal_id: GoalId::from_uuid(goal_id),
                dag_revision,
                node_id: DagNodeId::from_uuid(node_id),
            })
        }
    }
}

fn ensure_non_nil(id: Uuid) -> Result<(), CoordinatorAuthorityError> {
    if id.is_nil() {
        return Err(CoordinatorAuthorityError::Stale);
    }
    Ok(())
}

fn parse_timestamp(ms: i64) -> Result<TimestampMillis, CoordinatorAuthorityError> {
    TimestampMillis::new(ms).map_err(|_| CoordinatorAuthorityError::Stale)
}

fn parse_commit_oid(
    raw: &str,
    error: CoordinatorAuthorityError,
) -> Result<SwarmCommitOid, CoordinatorAuthorityError> {
    SwarmCommitOid::new(raw).map_err(|_| error)
}

fn parse_settlement_uuid(raw: &str, expected: Uuid) -> Result<Uuid, CoordinatorAuthorityError> {
    let parsed = Uuid::parse_str(raw).map_err(|_| CoordinatorAuthorityError::Unavailable)?;
    if parsed != expected {
        return Err(CoordinatorAuthorityError::Unavailable);
    }
    Ok(parsed)
}

fn ensure_non_zero_digest(
    digest: [u8; 32],
    error: CoordinatorAuthorityError,
) -> Result<[u8; 32], CoordinatorAuthorityError> {
    if digest == [0_u8; 32] {
        return Err(error);
    }
    Ok(digest)
}

fn decode_hex_digest_32(
    hex: &str,
    error: CoordinatorAuthorityError,
) -> Result<[u8; 32], CoordinatorAuthorityError> {
    let bytes = hex.as_bytes();
    if bytes.len() != 64 {
        return Err(error);
    }
    let (pairs, remainder) = bytes.as_chunks::<2>();
    if !remainder.is_empty() {
        return Err(error);
    }
    let mut out = [0_u8; 32];
    for (index, pair) in pairs.iter().enumerate() {
        let high = hex_nibble(pair[0]).ok_or(error)?;
        let low = hex_nibble(pair[1]).ok_or(error)?;
        out[index] = (high << 4) | low;
    }
    ensure_non_zero_digest(out, error)
}

const fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn map_swarm_http_error(error: ForgeHttpClientError) -> CoordinatorAuthorityError {
    match error {
        ForgeHttpClientError::InvalidRequest => CoordinatorAuthorityError::Stale,
        other => map_operations_source_error(other),
    }
}

fn map_swarm_control_error(error: ControlClientError) -> CoordinatorAuthorityError {
    match error {
        ControlClientError::QueueFull
        | ControlClientError::WorkerClosed
        | ControlClientError::WorkerShuttingDown
        | ControlClientError::ResponseDropped => CoordinatorAuthorityError::Unavailable,
        ControlClientError::Service(service) => {
            map_swarm_control_service(service.code(), service.view().outcome())
        }
    }
}

fn map_swarm_control_service(
    code: ControlErrorCode,
    outcome: ControlOperationOutcome,
) -> CoordinatorAuthorityError {
    if matches!(
        outcome,
        ControlOperationOutcome::Unknown | ControlOperationOutcome::CommittedButUnconfirmed
    ) {
        return CoordinatorAuthorityError::Unavailable;
    }
    match code {
        ControlErrorCode::AuthorizationRequired
        | ControlErrorCode::FilePathDenied
        | ControlErrorCode::WorkspaceScopeOverlap => CoordinatorAuthorityError::Denied,
        ControlErrorCode::InvalidInput
        | ControlErrorCode::InvalidPathKind
        | ControlErrorCode::LocationRevisionConflict
        | ControlErrorCode::WorkspaceQueueSettingsConflict
        | ControlErrorCode::ActivityLeaseConflict
        | ControlErrorCode::GoalNotFound
        | ControlErrorCode::GoalConflict
        | ControlErrorCode::GoalCompletionBlocked
        | ControlErrorCode::GoalEvidenceConflict
        | ControlErrorCode::DagNotFound
        | ControlErrorCode::DagConflict
        | ControlErrorCode::AgentNotFound
        | ControlErrorCode::AgentDagPlanConflict
        | ControlErrorCode::AgentDagReplayConflict
        | ControlErrorCode::CoordinatorNotFound
        | ControlErrorCode::FileConflict
        | ControlErrorCode::ProjectionResyncRequired => CoordinatorAuthorityError::Stale,
        other => map_control_service(other, outcome),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_backend::ForgeHttpServiceCode;
    use forge_control::WorkspaceLocator;
    use forge_domain::{LocationId, LocationRevision, WorkspaceId, WorkspaceRevision};
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    fn sample_digest(seed: u8) -> [u8; 32] {
        [seed; 32]
    }

    fn sample_hex_digest(seed: u8) -> String {
        format!("{seed:02x}").repeat(32)
    }

    fn sample_oid() -> String {
        "a".repeat(40)
    }

    fn sample_principal() -> AuthenticatedPrincipal {
        AuthenticatedPrincipal::DirectToken(forge_domain::DirectTokenId::from_uuid(
            Uuid::from_u128(7),
        ))
    }

    fn sample_resolved_scope(
        workspace_id: Uuid,
    ) -> Result<ResolvedWorkspaceScope, Box<dyn std::error::Error>> {
        let locator = WorkspaceLocator::new(
            LocationId::from_uuid(Uuid::from_u128(11)),
            LocationRevision::new(1)?,
            Path::new("."),
        )?;
        Ok(ResolvedWorkspaceScope::from_observation(
            WorkspaceId::from_uuid(workspace_id),
            WorkspaceRevision::new(1)?,
            locator,
        ))
    }

    fn test_http_authority(
        scopes: Vec<ResolvedWorkspaceScope>,
    ) -> Result<CoordinatorAuthority, Box<dyn std::error::Error>> {
        let commands = crate::forge_commands::ForgeCommandHttpClient::new(
            std::net::SocketAddr::from(([127, 0, 0, 1], 1)),
            "test-bearer-secret",
            std::time::Duration::from_secs(1),
        )?;
        let source = crate::forge_source::ForgeSourceHttpClient::new(commands);
        Ok(CoordinatorAuthority::from_http(source, scopes)?)
    }

    #[test]
    fn control_error_mapping_preserves_uncertainty_and_swarm_determinacy() {
        for outcome in [
            ControlOperationOutcome::Unknown,
            ControlOperationOutcome::CommittedButUnconfirmed,
        ] {
            assert_eq!(
                map_swarm_control_service(ControlErrorCode::DagConflict, outcome),
                CoordinatorAuthorityError::Unavailable
            );
            assert_eq!(
                map_swarm_control_service(ControlErrorCode::AuthorizationRequired, outcome),
                CoordinatorAuthorityError::Unavailable
            );
        }

        for code in [
            ControlErrorCode::DagConflict,
            ControlErrorCode::AgentDagReplayConflict,
            ControlErrorCode::WorkspaceRootConflict,
            ControlErrorCode::InvalidInput,
            ControlErrorCode::InvalidPathKind,
            ControlErrorCode::CoordinatorNotFound,
            ControlErrorCode::ActivityLeaseConflict,
        ] {
            assert_eq!(
                map_swarm_control_service(code, ControlOperationOutcome::NotAdmitted),
                CoordinatorAuthorityError::Stale
            );
        }

        for code in [
            ControlErrorCode::AuthorizationRequired,
            ControlErrorCode::FilePathDenied,
            ControlErrorCode::AgentSessionClosed,
            ControlErrorCode::AgentSessionConflict,
        ] {
            assert_eq!(
                map_swarm_control_service(code, ControlOperationOutcome::NotAdmitted),
                CoordinatorAuthorityError::Denied
            );
        }

        assert_eq!(
            map_swarm_control_error(ControlClientError::QueueFull),
            CoordinatorAuthorityError::Unavailable
        );
        assert_eq!(
            map_swarm_control_service(
                ControlErrorCode::PersistenceFailure,
                ControlOperationOutcome::NotAdmitted
            ),
            CoordinatorAuthorityError::Unavailable
        );
    }

    #[test]
    fn http_error_mapping_preserves_determinate_swarm_classes() {
        assert_eq!(
            map_swarm_http_error(ForgeHttpClientError::InvalidRequest),
            CoordinatorAuthorityError::Stale
        );
        assert_eq!(
            map_swarm_http_error(ForgeHttpClientError::DestinationMismatch),
            CoordinatorAuthorityError::Stale
        );
        assert_eq!(
            map_swarm_http_error(ForgeHttpClientError::Service {
                status: 403,
                code: ForgeHttpServiceCode::WorkspaceConfinementDenied,
            }),
            CoordinatorAuthorityError::Denied
        );
        assert_eq!(
            map_swarm_http_error(ForgeHttpClientError::Service {
                status: 409,
                code: ForgeHttpServiceCode::PlanningConflict,
            }),
            CoordinatorAuthorityError::Stale
        );
        assert_eq!(
            map_swarm_http_error(ForgeHttpClientError::Service {
                status: 503,
                code: ForgeHttpServiceCode::ForgeUnavailable,
            }),
            CoordinatorAuthorityError::Unavailable
        );
    }

    #[test]
    fn hex_digest_decoder_rejects_malformed_and_zero_digests() {
        let upper = "AB".repeat(32);
        assert_eq!(
            decode_hex_digest_32(&upper, CoordinatorAuthorityError::Unavailable),
            Ok([0xAB_u8; 32])
        );
        assert_eq!(
            decode_hex_digest_32(&"a".repeat(63), CoordinatorAuthorityError::Unavailable),
            Err(CoordinatorAuthorityError::Unavailable)
        );
        assert_eq!(
            decode_hex_digest_32(&"g".repeat(64), CoordinatorAuthorityError::Unavailable),
            Err(CoordinatorAuthorityError::Unavailable)
        );
        assert_eq!(
            decode_hex_digest_32(&"0".repeat(64), CoordinatorAuthorityError::Unavailable),
            Err(CoordinatorAuthorityError::Unavailable)
        );
    }

    #[test]
    fn provision_and_capture_converters_validate_receipt_bounds()
    -> Result<(), Box<dyn std::error::Error>> {
        let workspace_id = Uuid::from_u128(100);
        let scope = sample_resolved_scope(workspace_id)?;
        let principal = sample_principal();
        let swarm_run_id = Uuid::from_u128(200);
        let member_id = Uuid::from_u128(300);
        let goal_id = Uuid::from_u128(600);
        let node_id = Uuid::from_u128(700);
        let session_id = Uuid::from_u128(900);

        let provision_req = build_provision_request(
            scope.clone(),
            &ForgeSwarmProvisionInput {
                workspace_id,
                session_id,
                principal,
                swarm_run_id,
                member_id,
                mode: SwarmWorkspaceMode::Branch,
                run_mode: SwarmRunMode::DagNodeHybrid {
                    goal_id: GoalId::from_uuid(goal_id),
                    dag_revision: DagRevisionNumber::new(2)?,
                    node_id: DagNodeId::from_uuid(node_id),
                    declared_write_manifest_blake3: sample_digest(3),
                },
                candidate_commit_oid: Some(sample_oid()),
                observed_at_ms: 10,
            },
        )?;
        let mut provision_wire = SwarmWorkspaceReceiptWire {
            swarm_run_id,
            member_id,
            mode: SwarmWorkspaceModeWire::Branch,
            target: SwarmTargetWire::DagNodeHybrid {
                goal_id,
                dag_revision: 2,
                node_id,
            },
            worktree_path: "/Volumes/256.SSD/RustBuild/forbidden".to_owned(),
            provisioned_at_ms: 10,
            initial_tree_blake3: sample_hex_digest(0x11),
            initial_file_count: 1,
            input_commit_oid: sample_oid(),
            completed: None,
        };
        assert_eq!(
            provisioned_from_http(&provision_wire, &provision_req),
            Err(CoordinatorAuthorityError::Unavailable)
        );
        provision_wire.worktree_path = "/tmp/forge-worktree-branch".to_owned();
        let provisioned = provisioned_from_http(&provision_wire, &provision_req)?;
        assert_eq!(provisioned.baseline_tree_blake3, sample_digest(0x11));

        let capture_req = build_capture_request(
            scope,
            &ForgeSwarmCaptureInput {
                workspace_id,
                session_id,
                principal,
                swarm_run_id,
                member_id,
                captured_at_ms: 20,
            },
        )?;
        let capture_wire = SwarmMemberOutputWire {
            swarm_run_id,
            member_id,
            mode: SwarmWorkspaceModeWire::Branch,
            input_commit_oid: sample_oid(),
            output_commit_oid: sample_oid(),
            initial_tree_blake3: sample_hex_digest(0x11),
            final_tree_blake3: sample_hex_digest(0x22),
            final_file_count: 2,
            changed_paths: vec!["src/lib.rs".to_owned()],
            diff_digest: sample_hex_digest(0x33),
            captured_at_ms: 20,
        };
        let captured = capture_from_http(&capture_wire, &capture_req)?;
        assert_eq!(captured.tree_blake3, sample_digest(0x22));
        Ok(())
    }

    #[test]
    fn converge_converters_validate_outcome_and_conflict() -> Result<(), Box<dyn std::error::Error>>
    {
        let swarm_run_id = Uuid::from_u128(200);
        let member_id = Uuid::from_u128(300);

        let converged_http = converge_from_http(
            SwarmConvergenceOutcomeWire::Converged {
                swarm_run_id,
                converged_commit_oid: sample_oid(),
                converged_tree_blake3: sample_hex_digest(0x44),
                converged_diff_digest: sample_hex_digest(0x55),
                changed_files: vec!["src/lib.rs".to_owned()],
            },
            swarm_run_id,
        )?;
        assert!(matches!(
            converged_http,
            ForgeSwarmConvergeBridgeOutcome::Converged { .. }
        ));
        let conflict_ctrl = converge_from_control(
            SwarmConvergenceOutcome::Conflict {
                conflicting_member_id: SwarmMemberId::from_uuid(member_id),
                details: vec!["src/lib.rs".to_owned()],
            },
            swarm_run_id,
        )?;
        assert_eq!(
            conflict_ctrl,
            ForgeSwarmConvergeBridgeOutcome::Conflict {
                conflicting_member_id: member_id,
                conflicting_paths: vec!["src/lib.rs".to_owned()],
            }
        );
        Ok(())
    }

    #[test]
    fn settle_and_hybrid_converters_enforce_tree_digest_cas()
    -> Result<(), Box<dyn std::error::Error>> {
        let workspace_id = Uuid::from_u128(100);
        let principal = sample_principal();
        let swarm_run_id = Uuid::from_u128(200);
        let settlement_id = Uuid::from_u128(400);
        let activity_id = Uuid::from_u128(500);
        let goal_id = Uuid::from_u128(600);
        let node_id = Uuid::from_u128(700);
        let coordinator_agent_id = Uuid::from_u128(800);
        let session_id = Uuid::from_u128(900);

        let settle_input = ForgeSwarmSettleInput {
            workspace_id,
            session_id,
            principal,
            activity_id: Some(activity_id),
            coordinator_agent_id,
            swarm_run_id,
            settlement_id,
            converged_commit_oid: sample_oid(),
            expected_converged_tree_blake3: sample_digest(0x44),
            expected_converged_diff_digest: sample_digest(0x55),
            observed_at_ms: 30,
        };
        let mut settle_wire = forge_backend::ops::swarm::SwarmSettleWire {
            receipt: SwarmSettlementReceiptWire {
                settlement_id: settlement_id.to_string(),
                swarm_run_id,
                target: SwarmTargetWire::StandaloneSwarm,
                state: SettlementJournalStateWire::Applied,
                settled_at_ms: 30,
                baseline_commit_oid: sample_oid(),
                converged_commit_oid: sample_oid(),
                baseline_tree_blake3: sample_hex_digest(0x11),
                settled_tree_blake3: sample_hex_digest(0x99),
                converged_diff_digest: sample_hex_digest(0x55),
                settled_file_count: 1,
                upserted_paths: vec!["src/lib.rs".to_owned()],
                deleted_paths: Vec::new(),
            },
            outbox_sequence: Some(1),
        };
        assert_eq!(
            settle_from_http(&settle_wire, &settle_input),
            Err(CoordinatorAuthorityError::Stale)
        );
        settle_wire.receipt.settled_tree_blake3 = sample_hex_digest(0x44);
        let settled = settle_from_http(&settle_wire, &settle_input)?;
        assert_eq!(settled.outbox_sequence, Some(1));
        assert_eq!(settled.settled_file_count, 1);

        let hybrid_input = ForgeSwarmHybridMaterializeInput {
            workspace_id,
            session_id,
            principal,
            activity_id,
            swarm_run_id,
            settlement_id,
            goal_id,
            expected_goal_revision: 1,
            expected_dag_revision: 2,
            node_id,
            converged_commit_oid: sample_oid(),
            expected_converged_tree_blake3: sample_digest(0x44),
            expected_converged_diff_digest: sample_digest(0x55),
            observed_at_ms: 40,
        };
        let hybrid_wire = SwarmSettlementReceiptWire {
            settlement_id: settlement_id.to_string(),
            swarm_run_id,
            target: SwarmTargetWire::DagNodeHybrid {
                goal_id,
                dag_revision: 2,
                node_id,
            },
            state: SettlementJournalStateWire::Applied,
            settled_at_ms: 40,
            baseline_commit_oid: sample_oid(),
            converged_commit_oid: sample_oid(),
            baseline_tree_blake3: sample_hex_digest(0x11),
            settled_tree_blake3: sample_hex_digest(0x44),
            converged_diff_digest: sample_hex_digest(0x55),
            settled_file_count: 1,
            upserted_paths: vec!["src/lib.rs".to_owned()],
            deleted_paths: Vec::new(),
        };
        let hybrid = hybrid_from_http(hybrid_wire, &hybrid_input)?;
        assert_eq!(hybrid.node_id, node_id);
        assert_eq!(hybrid.dag_revision, 2);
        Ok(())
    }

    fn sample_http_with_effects(
        configured_ws: Uuid,
    ) -> Result<CoordinatorAuthority, Box<dyn std::error::Error>> {
        let commands = crate::forge_commands::ForgeCommandHttpClient::new(
            std::net::SocketAddr::from(([127, 0, 0, 1], 1)),
            "test-bearer-secret",
            std::time::Duration::from_secs(1),
        )?;
        let effect_client = forge_backend::ForgeHttpClient::new(
            std::net::SocketAddr::from(([127, 0, 0, 1], 1)),
            "test-bearer-secret",
            forge_domain::ForgeBackendId::from_uuid(Uuid::from_u128(77)),
            Uuid::from_u128(88),
            std::time::Duration::from_secs(1),
        )?;
        let source = crate::forge_source::ForgeSourceHttpClient::new(commands);
        Ok(
            CoordinatorAuthority::from_http(source, vec![sample_resolved_scope(configured_ws)?])?
                .with_effect_facts(effect_client),
        )
    }

    #[tokio::test]
    async fn missing_effect_client_fails_closed_across_bridge_methods()
    -> Result<(), Box<dyn std::error::Error>> {
        let slots = Arc::new(Semaphore::new(4));
        let principal = sample_principal();
        let configured_ws = Uuid::from_u128(1);
        let http_no_effects = test_http_authority(vec![sample_resolved_scope(configured_ws)?])?;
        let session_id = Uuid::from_u128(10);
        let swarm_run_id = Uuid::from_u128(20);
        let member_id = Uuid::from_u128(30);

        assert_eq!(
            http_no_effects
                .swarm_workspace_provision(
                    ForgeSwarmProvisionInput {
                        workspace_id: configured_ws,
                        session_id,
                        principal,
                        swarm_run_id,
                        member_id,
                        mode: SwarmWorkspaceMode::Share,
                        run_mode: SwarmRunMode::Standalone,
                        candidate_commit_oid: None,
                        observed_at_ms: 1,
                    },
                    slots.clone().acquire_owned().await?,
                )
                .await,
            Err(CoordinatorAuthorityError::Unavailable)
        );
        assert_eq!(
            http_no_effects
                .swarm_member_capture(
                    ForgeSwarmCaptureInput {
                        workspace_id: configured_ws,
                        session_id,
                        principal,
                        swarm_run_id,
                        member_id,
                        captured_at_ms: 2,
                    },
                    slots.clone().acquire_owned().await?,
                )
                .await,
            Err(CoordinatorAuthorityError::Unavailable)
        );
        assert_eq!(
            http_no_effects
                .swarm_converge(
                    ForgeSwarmConvergeInput {
                        workspace_id: configured_ws,
                        session_id,
                        principal,
                        swarm_run_id,
                        candidates: vec![SwarmConvergenceCandidate {
                            member_id: SwarmMemberId::from_uuid(member_id),
                            expected_output_commit_oid: None,
                            expected_diff_digest: None,
                        }],
                        observed_at_ms: 3,
                    },
                    slots.clone().acquire_owned().await?,
                )
                .await,
            Err(CoordinatorAuthorityError::Unavailable)
        );
        Ok(())
    }

    #[tokio::test]
    async fn foreign_scope_and_invalid_inputs_fail_deterministically()
    -> Result<(), Box<dyn std::error::Error>> {
        let slots = Arc::new(Semaphore::new(4));
        let principal = sample_principal();
        let configured_ws = Uuid::from_u128(1);
        let foreign_ws = Uuid::from_u128(2);
        let http_with_effects = sample_http_with_effects(configured_ws)?;
        let session_id = Uuid::from_u128(10);
        let swarm_run_id = Uuid::from_u128(20);

        assert_eq!(
            http_with_effects
                .swarm_purge(
                    ForgeSwarmPurgeInput {
                        workspace_id: foreign_ws,
                        session_id,
                        principal,
                        swarm_run_id,
                    },
                    slots.clone().acquire_owned().await?,
                )
                .await,
            Err(CoordinatorAuthorityError::Denied)
        );
        assert_eq!(
            http_with_effects
                .swarm_settle_standalone(
                    ForgeSwarmSettleInput {
                        workspace_id: configured_ws,
                        session_id,
                        principal,
                        activity_id: None,
                        coordinator_agent_id: Uuid::from_u128(40),
                        swarm_run_id,
                        settlement_id: Uuid::from_u128(50),
                        converged_commit_oid: sample_oid(),
                        expected_converged_tree_blake3: [0_u8; 32],
                        expected_converged_diff_digest: sample_digest(0x55),
                        observed_at_ms: 30,
                    },
                    slots.clone().acquire_owned().await?,
                )
                .await,
            Err(CoordinatorAuthorityError::Stale)
        );
        assert_eq!(
            http_with_effects
                .swarm_materialize_hybrid(
                    ForgeSwarmHybridMaterializeInput {
                        workspace_id: configured_ws,
                        session_id,
                        principal,
                        activity_id: Uuid::from_u128(45),
                        swarm_run_id,
                        settlement_id: Uuid::from_u128(55),
                        goal_id: Uuid::from_u128(60),
                        expected_goal_revision: 0,
                        expected_dag_revision: 1,
                        node_id: Uuid::from_u128(70),
                        converged_commit_oid: sample_oid(),
                        expected_converged_tree_blake3: sample_digest(0x44),
                        expected_converged_diff_digest: sample_digest(0x55),
                        observed_at_ms: 40,
                    },
                    slots.clone().acquire_owned().await?,
                )
                .await,
            Err(CoordinatorAuthorityError::Stale)
        );
        assert_eq!(
            http_with_effects
                .autonomy_outbox_page(
                    ForgeAutonomyOutboxPageInput {
                        workspace_id: configured_ws,
                        migration_id: session_id,
                        destination_id: Uuid::from_u128(88),
                        after_sequence: 0,
                        limit: 0,
                    },
                    slots.acquire_owned().await?,
                )
                .await,
            Err(CoordinatorAuthorityError::Stale)
        );
        Ok(())
    }
}
