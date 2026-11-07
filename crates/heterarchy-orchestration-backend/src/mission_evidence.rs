//! Authenticated Runtime/Forge integration for Mission source evidence.

mod projection;
mod source;

use axum::extract::DefaultBodyLimit;
use axum::extract::{Path, State};
use axum::routing::post;
use axum::{Extension, Json, Router};
use forge_backend::ForgeHttpClient;
use forge_control::ResolvedWorkspaceScope;
use forge_domain::{
    AuthenticatedPrincipal, EvaluationManifestProposal, EvaluationRegisteredManifest,
    EvaluationT1CoverageCut,
};
use heterarchy_orchestration_operations::mission_evidence::{
    MissionEvidenceIdentityPendingReason, MissionEvidenceIdentityResolution,
    MissionEvidenceManifestBindingOutcome, MissionEvidenceManifestBindingRecord,
    MissionEvidenceManifestBundle, MissionEvidenceManifestPreparation,
    MissionEvidenceMutationRecord, MissionEvidenceObservationBundle,
    MissionEvidenceObservationOutcome, MissionEvidenceObservationPartition,
    MissionEvidenceObservationRecord, MissionEvidencePreparationPendingTarget,
};
use heterarchy_orchestration_runtime::mission_evidence::{
    MissionEvidenceCandidateMemberSpec, MissionEvidenceCandidatePartitionSpec,
    MissionEvidenceManifestBindingSpec, MissionEvidenceObservationSpec,
    MissionEvidencePlanDigestSpec, MissionEvidencePreparationSpec,
};
use heterarchy_orchestration_runtime::{
    RuntimeError, mission_evaluation::MissionEvaluationParticipantSpec,
};
use std::collections::BTreeSet;
use uuid::Uuid;

use crate::catalog_execution::CatalogLane;
use crate::wire::mission_evidence::{
    MissionEvidenceBindingReadRequest, MissionEvidenceBindingReadResponse,
    MissionEvidenceCandidatePartitionWire, MissionEvidenceIdentityPendingReasonWire,
    MissionEvidenceManifestBindRequest, MissionEvidenceManifestBindResponse,
    MissionEvidenceManifestPrepareRequest, MissionEvidenceManifestPrepareResponse,
    MissionEvidenceMutationRequest, MissionEvidenceMutationStatusWire, MissionEvidenceMutationWire,
    MissionEvidenceObservationReadRequest, MissionEvidenceObservationReadResponse,
    MissionEvidenceObservationRequest, MissionEvidenceObservationResponse,
    MissionEvidencePendingReasonWire, MissionEvidencePendingSourceWire,
    MissionEvidencePendingTargetWire, MissionEvidencePlanDigestsRequest,
    MissionEvidencePlanDigestsResponse, MissionEvidencePreparationStatusWire,
    MissionEvidenceRequestReadActor, MissionEvidenceRequestReadRequest,
    MissionEvidenceRequestReadResponse, MissionEvidenceSourceStageWire,
};
use crate::{ApiError, BackendState, ScopedMutationAuthority};

const MAX_CANDIDATE_PARTITIONS: usize = 256;
const MAX_CANDIDATE_MEMBERS: usize = 256;
const MAX_SQLITE_INTEGER: u64 = i64::MAX.unsigned_abs();

/// Register the authenticated Mission evidence source and journal endpoints.
pub(crate) fn routes() -> Router<BackendState> {
    Router::new()
        .route(
            "/internal/v1/workspaces/{workspace_id}/mission-evidence/plan-digests/query",
            post(read_plan_digests),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/mission-evidence/manifests/prepare",
            post(prepare_manifests),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/mission-evidence/manifests/bind",
            post(bind_manifests),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/mission-evidence/plans/{plan_id}/revisions/{revision}/binding/query",
            post(read_binding),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/mission-evidence/observations/record",
            post(record_observation),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/mission-evidence/plans/{plan_id}/revisions/{revision}/observations/{request_id}/query",
            post(read_observation),
        )
        .route(
            "/internal/v1/workspaces/{workspace_id}/mission-evidence-requests/{request_id}/query",
            post(read_mutation_request),
        )
        .layer(DefaultBodyLimit::max(
            crate::wire::mission_evidence::MAX_MISSION_EVIDENCE_BODY_BYTES,
        ))
}

async fn read_plan_digests(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<MissionEvidencePlanDigestsRequest>,
) -> Result<Json<MissionEvidencePlanDigestsResponse>, ApiError> {
    validate_read_scope(&workspace_path, &request.scope, request.session_id)?;
    require_identity(request.sender_activity_id)?;
    require_identity(request.mission_id)?;
    require_identity(request.plan_id)?;
    require_revision(request.revision)?;
    let scope = request.scope.clone();
    let spec = MissionEvidencePlanDigestSpec {
        workspace_id: scope.workspace_id,
        mission_id: request.mission_id,
        plan_id: request.plan_id,
        plan_revision: request.revision,
        activity_id: request.sender_activity_id,
        session_id: request.session_id,
    };
    let digests = state
        .run_workspace_reference_query(
            scope.clone(),
            Some(request.session_id),
            principal,
            move |runtime, _| runtime.mission_evidence_plan_digests(spec, principal),
        )
        .await?;
    if digests.workspace_id().into_uuid() != scope.workspace_id
        || digests.mission_id().into_uuid() != request.mission_id
        || digests.plan_id().into_uuid() != request.plan_id
        || digests.revision() != request.revision
    {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    Ok(Json(MissionEvidencePlanDigestsResponse {
        scope,
        mission_id: request.mission_id,
        plan_id: digests.plan_id().into_uuid(),
        plan_version_id: digests.plan_version_id().into_uuid(),
        revision: digests.revision(),
        t1_submission_cutoff_ms: digests.t1_submission_cutoff_ms(),
        plan_digest: source::digest_hex(digests.plan_digest()),
        criteria_digest: source::digest_hex(digests.criteria_digest()),
        member_criteria_digests: digests
            .member_criteria_digests()
            .iter()
            .map(
                |member| crate::wire::mission_evidence::MissionEvidenceMemberCriteriaDigestWire {
                    obligation_id: member.obligation_id().into_uuid(),
                    work_unit_id: member.work_unit_id().into_uuid(),
                    participation_id: member.participation_id().into_uuid(),
                    digest: source::digest_hex(member.digest()),
                },
            )
            .collect(),
    }))
}

async fn prepare_manifests(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<MissionEvidenceManifestPrepareRequest>,
) -> Result<Json<MissionEvidenceManifestPrepareResponse>, ApiError> {
    validate_context(&workspace_path, &request.context)?;
    validate_plan_selectors(request.mission_id, request.plan_id, request.plan_revision)?;
    validate_partitions(&request.partitions)?;

    let spec = preparation_spec(
        request.context.scope.workspace_id,
        request.mission_id,
        request.plan_id,
        request.plan_revision,
        &request.partitions,
        &request.context,
    );
    let scope = request.context.scope.clone();
    let client = early_source_admission(&state, &request, principal).await?;
    let backend_id = client.expected_backend_id();
    let preparation = state
        .run_workspace_reference_query(
            scope.clone(),
            Some(request.context.session_id),
            principal,
            move |runtime, _| {
                runtime.prepare_mission_evidence_manifests(spec, backend_id, principal)
            },
        )
        .await?;
    Ok(Json(preparation_response(
        scope,
        request.mission_id,
        request.plan_id,
        request.plan_revision,
        &preparation,
    )?))
}

async fn bind_manifests(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<MissionEvidenceManifestBindRequest>,
) -> Result<Json<MissionEvidenceManifestBindResponse>, ApiError> {
    validate_context(&workspace_path, &request.context)?;
    require_identity(request.request_id)?;
    validate_plan_selectors(request.mission_id, request.plan_id, request.plan_revision)?;
    validate_partitions(&request.partitions)?;
    let scope = request.context.scope.clone();

    if let Some(response) = read_binding_replay(&state, &request, principal).await? {
        return Ok(Json(response));
    }

    let (trusted_scope, source_client) = configured_source(&state, &scope).await?;
    let preflight = prepare_local_manifests(
        &state,
        &request,
        source_client.expected_backend_id(),
        principal,
    )
    .await?;
    if !preflight.ready() {
        return Ok(Json(MissionEvidenceManifestBindResponse {
            scope,
            request_id: request.request_id,
            status: MissionEvidenceMutationStatusWire::Pending,
            replayed: false,
            binding: None,
            pending_targets: pending_target_wires(preflight.pending_targets())?,
            pending_sources: Vec::new(),
        }));
    }
    validate_source_client(&source_client, &trusted_scope, preflight.proposals())?;

    let manifests = match stage_manifests(
        &state,
        &source_client,
        &trusted_scope,
        preflight.proposals(),
    )
    .await?
    {
        ManifestSourceStage::Ready(manifests) => manifests,
        ManifestSourceStage::Pending(pending_sources) => {
            return Ok(Json(MissionEvidenceManifestBindResponse {
                scope,
                request_id: request.request_id,
                status: MissionEvidenceMutationStatusWire::Pending,
                replayed: false,
                binding: None,
                pending_targets: Vec::new(),
                pending_sources,
            }));
        }
    };
    let response = commit_manifest_binding(&state, scope, &request, manifests, principal).await?;
    Ok(Json(response))
}

async fn read_binding_replay(
    state: &BackendState,
    request: &MissionEvidenceManifestBindRequest,
    principal: AuthenticatedPrincipal,
) -> Result<Option<MissionEvidenceManifestBindResponse>, ApiError> {
    let context = &request.context;
    let Some(record) = read_request_record(
        state,
        &context.scope,
        context.session_id,
        context.sender_activity_id,
        request.request_id,
        principal,
    )
    .await?
    else {
        return Ok(None);
    };
    let MissionEvidenceMutationRecord::ManifestBinding(binding) = record else {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    };
    if !binding_matches_request(&binding, request) {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    Ok(Some(MissionEvidenceManifestBindResponse {
        scope: context.scope.clone(),
        request_id: request.request_id,
        status: MissionEvidenceMutationStatusWire::Complete,
        replayed: true,
        binding: Some(projection::binding_wire(&binding)),
        pending_targets: Vec::new(),
        pending_sources: Vec::new(),
    }))
}

async fn prepare_local_manifests(
    state: &BackendState,
    request: &MissionEvidenceManifestBindRequest,
    backend_id: forge_domain::ForgeBackendId,
    principal: AuthenticatedPrincipal,
) -> Result<MissionEvidenceManifestPreparation, ApiError> {
    let scope = request.context.scope.clone();
    let spec = preparation_spec(
        scope.workspace_id,
        request.mission_id,
        request.plan_id,
        request.plan_revision,
        &request.partitions,
        &request.context,
    );
    state
        .run_workspace_reference_query(
            scope,
            Some(request.context.session_id),
            principal,
            move |runtime, _| {
                runtime.prepare_mission_evidence_manifests(spec, backend_id, principal)
            },
        )
        .await
}

enum ManifestSourceStage {
    Ready(Vec<EvaluationRegisteredManifest>),
    Pending(Vec<MissionEvidencePendingSourceWire>),
}

async fn stage_manifests(
    state: &BackendState,
    source_client: &ForgeHttpClient,
    trusted_scope: &ResolvedWorkspaceScope,
    proposals: &[EvaluationManifestProposal],
) -> Result<ManifestSourceStage, ApiError> {
    let mut manifests = Vec::with_capacity(proposals.len());
    for proposal in proposals {
        match source::get_or_seal_manifest(state, source_client, trusted_scope, proposal).await? {
            source::SourceProgress::Ready(manifest) => manifests.push(manifest),
            source::SourceProgress::Pending(reason) => {
                return Ok(ManifestSourceStage::Pending(vec![pending_source(
                    proposal,
                    MissionEvidenceSourceStageWire::Manifest,
                    reason,
                    None,
                    None,
                )]));
            }
        }
    }
    Ok(ManifestSourceStage::Ready(manifests))
}

async fn commit_manifest_binding(
    state: &BackendState,
    scope: crate::wire::WorkspaceQueryScope,
    request: &MissionEvidenceManifestBindRequest,
    manifests: Vec<EvaluationRegisteredManifest>,
    principal: AuthenticatedPrincipal,
) -> Result<MissionEvidenceManifestBindResponse, ApiError> {
    let bundle = MissionEvidenceManifestBundle::new(manifests)
        .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?;
    let mutation = MissionEvidenceManifestBindingSpec {
        request_id: request.request_id,
        workspace_id: scope.workspace_id,
        mission_id: request.mission_id,
        plan_id: request.plan_id,
        plan_revision: request.plan_revision,
        bundle,
        participant: participant(&request.context),
    };
    let outcome: MissionEvidenceManifestBindingOutcome = state
        .run_workspace_reference_mutation(
            scope.clone(),
            request.context.session_id,
            principal,
            ScopedMutationAuthority::WorkspaceMember,
            CatalogLane::Ordinary,
            move |runtime, now| runtime.bind_mission_evidence_manifest(mutation, principal, now),
        )
        .await?;
    if !binding_matches_request(outcome.binding(), request) {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    Ok(MissionEvidenceManifestBindResponse {
        scope,
        request_id: request.request_id,
        status: MissionEvidenceMutationStatusWire::Complete,
        replayed: outcome.replayed(),
        binding: Some(projection::binding_wire(outcome.binding())),
        pending_targets: Vec::new(),
        pending_sources: Vec::new(),
    })
}

async fn read_binding(
    State(state): State<BackendState>,
    Path((workspace_path, plan_path, revision_path)): Path<(String, String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<MissionEvidenceBindingReadRequest>,
) -> Result<Json<MissionEvidenceBindingReadResponse>, ApiError> {
    validate_read_scope(&workspace_path, &request.scope, request.session_id)?;
    require_identity(request.mission_id)?;
    let plan_id = crate::canonical_uuid(&plan_path)?;
    let revision = canonical_revision(&revision_path)?;
    if request.plan_id != plan_id || request.plan_revision != revision {
        return Err(ApiError::InvalidIdentity);
    }
    let scope = request.scope.clone();
    let workspace_id = scope.workspace_id;
    let mission_id = request.mission_id;
    let request_plan_id = request.plan_id;
    let request_revision = request.plan_revision;
    let result = state
        .run_workspace_reference_query(
            scope.clone(),
            Some(request.session_id),
            principal,
            move |runtime, _| {
                runtime.mission_evidence_binding(
                    workspace_id,
                    mission_id,
                    request_plan_id,
                    request_revision,
                )
            },
        )
        .await?;
    Ok(Json(MissionEvidenceBindingReadResponse {
        scope,
        mission_id: request.mission_id,
        plan_id: request.plan_id,
        plan_revision: request.plan_revision,
        binding: result.as_ref().map(projection::binding_wire),
    }))
}

async fn record_observation(
    State(state): State<BackendState>,
    Path(workspace_path): Path<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<MissionEvidenceObservationRequest>,
) -> Result<Json<MissionEvidenceObservationResponse>, ApiError> {
    validate_context(&workspace_path, &request.context)?;
    require_identity(request.request_id)?;
    validate_plan_selectors(request.mission_id, request.plan_id, request.plan_revision)?;
    require_nonnegative_revision(request.expected_head_revision)?;
    let scope = request.context.scope.clone();
    if let Some(response) = read_observation_replay(&state, &request, principal).await? {
        return Ok(Json(response));
    }

    let preflight = prepare_observation_source(&state, &request, principal).await?;
    if !preflight.preparation.ready() {
        return Ok(Json(MissionEvidenceObservationResponse {
            scope,
            request_id: request.request_id,
            status: MissionEvidenceMutationStatusWire::Pending,
            replayed: false,
            observation: None,
            pending_targets: pending_target_wires(preflight.preparation.pending_targets())?,
            pending_sources: Vec::new(),
        }));
    }
    validate_source_client(
        &preflight.source_client,
        &preflight.trusted_scope,
        preflight.preparation.proposals(),
    )?;
    if !preparation_matches_binding(&preflight.preparation, &preflight.binding) {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    match stage_observation_cuts(&state, &preflight, &request).await? {
        ObservationCutStage::Ready(cuts) => {
            commit_observation(&state, scope, &request, cuts, principal)
                .await
                .map(Json)
        }
        ObservationCutStage::Pending(pending_sources) => {
            Ok(Json(MissionEvidenceObservationResponse {
                scope,
                request_id: request.request_id,
                status: MissionEvidenceMutationStatusWire::Pending,
                replayed: false,
                observation: None,
                pending_targets: Vec::new(),
                pending_sources,
            }))
        }
    }
}

async fn read_observation_replay(
    state: &BackendState,
    request: &MissionEvidenceObservationRequest,
    principal: AuthenticatedPrincipal,
) -> Result<Option<MissionEvidenceObservationResponse>, ApiError> {
    let context = &request.context;
    let Some(record) = read_request_record(
        state,
        &context.scope,
        context.session_id,
        context.sender_activity_id,
        request.request_id,
        principal,
    )
    .await?
    else {
        return Ok(None);
    };
    let MissionEvidenceMutationRecord::Observation(observation) = record else {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    };
    if !observation_matches_request(
        state,
        &context.scope,
        principal,
        MissionEvidenceRequestReadActor {
            sender_activity_id: context.sender_activity_id,
            session_id: context.session_id,
        },
        request,
        &observation,
    )
    .await?
    {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    Ok(Some(MissionEvidenceObservationResponse {
        scope: context.scope.clone(),
        request_id: request.request_id,
        status: MissionEvidenceMutationStatusWire::Complete,
        replayed: true,
        observation: Some(projection::observation_wire(
            &observation,
            request.mission_id,
        )),
        pending_targets: Vec::new(),
        pending_sources: Vec::new(),
    }))
}

struct ObservationPreflight {
    binding: MissionEvidenceManifestBindingRecord,
    preparation: MissionEvidenceManifestPreparation,
    trusted_scope: ResolvedWorkspaceScope,
    source_client: ForgeHttpClient,
}

async fn prepare_observation_source(
    state: &BackendState,
    request: &MissionEvidenceObservationRequest,
    principal: AuthenticatedPrincipal,
) -> Result<ObservationPreflight, ApiError> {
    let scope = request.context.scope.clone();
    let (trusted_scope, source_client) = configured_source(state, &scope).await?;
    let workspace_id = scope.workspace_id;
    let mission_id = request.mission_id;
    let plan_id = request.plan_id;
    let plan_revision = request.plan_revision;
    let session_id = request.context.session_id;
    let binding = state
        .run_workspace_reference_query(
            scope.clone(),
            Some(session_id),
            principal,
            move |runtime, _| {
                runtime
                    .mission_evidence_binding(workspace_id, mission_id, plan_id, plan_revision)?
                    .ok_or(RuntimeError::NotFound)
            },
        )
        .await?;
    let spec = MissionEvidencePreparationSpec {
        workspace_id: scope.workspace_id,
        mission_id: request.mission_id,
        plan_id: request.plan_id,
        plan_revision: request.plan_revision,
        partitions: candidates_from_binding(&binding),
        participant: participant(&request.context),
    };
    let backend_id = source_client.expected_backend_id();
    let preparation = state
        .run_workspace_reference_query(scope, Some(session_id), principal, move |runtime, _| {
            runtime.prepare_mission_evidence_manifests(spec, backend_id, principal)
        })
        .await?;
    Ok(ObservationPreflight {
        binding,
        preparation,
        trusted_scope,
        source_client,
    })
}

enum ObservationCutStage {
    Ready(MissionEvidenceObservationBundle),
    Pending(Vec<MissionEvidencePendingSourceWire>),
}

async fn stage_observation_cuts(
    state: &BackendState,
    preflight: &ObservationPreflight,
    request: &MissionEvidenceObservationRequest,
) -> Result<ObservationCutStage, ApiError> {
    let mut partitions = Vec::with_capacity(preflight.binding.manifest_bundle().manifests().len());
    for manifest in preflight.binding.manifest_bundle().manifests() {
        let proposal = manifest.proposal();
        let (t1_cut_id, t1_cut) = match stage_t1_cut(state, preflight, proposal).await? {
            T1CutStage::Ready(cut_id, cut) => (cut_id, cut),
            T1CutStage::Pending(pending_source) => {
                return Ok(ObservationCutStage::Pending(vec![pending_source]));
            }
        };
        let review_cut_id =
            heterarchy_orchestration_runtime::mission_evidence::mission_evidence_review_cut_id(
                proposal,
                request.request_id,
            )
            .map_err(ApiError::Runtime)?;
        let t1_request = forge_domain::EvaluationT1CloseRequest::new(
            t1_cut_id,
            proposal.manifest_id(),
            proposal.revision(),
            proposal.digest(),
            proposal.deadline(),
        );
        let review_cut = match source::get_or_create_review_cut(
            state,
            &preflight.source_client,
            &preflight.trusted_scope,
            proposal,
            &t1_request,
            &t1_cut,
            review_cut_id,
        )
        .await?
        {
            source::SourceProgress::Ready(cut) => cut,
            source::SourceProgress::Pending(reason) => {
                return Ok(ObservationCutStage::Pending(vec![pending_source(
                    proposal,
                    MissionEvidenceSourceStageWire::ReviewCut,
                    reason,
                    Some(t1_cut_id.into_uuid()),
                    Some(review_cut_id.into_uuid()),
                )]));
            }
        };
        partitions.push(
            MissionEvidenceObservationPartition::new(
                proposal.source().activity_id(),
                t1_cut,
                review_cut,
            )
            .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?,
        );
    }
    let bundle = MissionEvidenceObservationBundle::new(partitions)
        .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?;
    Ok(ObservationCutStage::Ready(bundle))
}

enum T1CutStage {
    Ready(forge_domain::EvaluationCutId, EvaluationT1CoverageCut),
    Pending(MissionEvidencePendingSourceWire),
}

async fn stage_t1_cut(
    state: &BackendState,
    preflight: &ObservationPreflight,
    proposal: &EvaluationManifestProposal,
) -> Result<T1CutStage, ApiError> {
    let cut_id =
        heterarchy_orchestration_runtime::mission_evidence::mission_evidence_t1_cut_id(proposal)
            .map_err(ApiError::Runtime)?;
    match source::get_or_close_t1(
        state,
        &preflight.source_client,
        &preflight.trusted_scope,
        proposal,
        cut_id,
    )
    .await?
    {
        source::SourceProgress::Ready(cut) => Ok(T1CutStage::Ready(cut_id, cut)),
        source::SourceProgress::Pending(reason) => Ok(T1CutStage::Pending(pending_source(
            proposal,
            MissionEvidenceSourceStageWire::T1Cut,
            reason,
            Some(cut_id.into_uuid()),
            None,
        ))),
    }
}

async fn commit_observation(
    state: &BackendState,
    scope: crate::wire::WorkspaceQueryScope,
    request: &MissionEvidenceObservationRequest,
    cuts: MissionEvidenceObservationBundle,
    principal: AuthenticatedPrincipal,
) -> Result<MissionEvidenceObservationResponse, ApiError> {
    let mutation = MissionEvidenceObservationSpec {
        request_id: request.request_id,
        workspace_id: scope.workspace_id,
        mission_id: request.mission_id,
        plan_id: request.plan_id,
        plan_revision: request.plan_revision,
        expected_head_revision: request.expected_head_revision,
        cuts,
        participant: participant(&request.context),
    };
    let outcome: MissionEvidenceObservationOutcome = state
        .run_workspace_reference_mutation(
            scope.clone(),
            request.context.session_id,
            principal,
            ScopedMutationAuthority::WorkspaceMember,
            CatalogLane::Ordinary,
            move |runtime, now| {
                runtime.record_mission_evidence_observation(mutation, principal, now)
            },
        )
        .await?;
    if outcome.observation().request_id() != request.request_id
        || outcome.observation().plan_id().into_uuid() != request.plan_id
        || outcome.observation().plan_revision() != request.plan_revision
        || outcome.observation().expected_head_revision() != request.expected_head_revision
    {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    Ok(MissionEvidenceObservationResponse {
        scope,
        request_id: request.request_id,
        status: MissionEvidenceMutationStatusWire::Complete,
        replayed: outcome.replayed(),
        observation: Some(projection::observation_wire(
            outcome.observation(),
            request.mission_id,
        )),
        pending_targets: Vec::new(),
        pending_sources: Vec::new(),
    })
}

async fn read_observation(
    State(state): State<BackendState>,
    Path((workspace_path, plan_path, revision_path, request_path)): Path<(
        String,
        String,
        String,
        String,
    )>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<MissionEvidenceObservationReadRequest>,
) -> Result<Json<MissionEvidenceObservationReadResponse>, ApiError> {
    validate_read_scope(&workspace_path, &request.scope, request.session_id)?;
    let plan_id = crate::canonical_uuid(&plan_path)?;
    let plan_revision = canonical_revision(&revision_path)?;
    let request_id = crate::canonical_uuid(&request_path)?;
    if request.plan_id != plan_id
        || request.plan_revision != plan_revision
        || request.request_id != request_id
    {
        return Err(ApiError::InvalidIdentity);
    }
    let scope = request.scope.clone();
    let workspace_id = scope.workspace_id;
    let mission_id = request.mission_id;
    let request_plan_id = request.plan_id;
    let request_revision = request.plan_revision;
    let request_id_selector = request.request_id;
    let result = state
        .run_workspace_reference_query(
            scope.clone(),
            Some(request.session_id),
            principal,
            move |runtime, _| {
                runtime.mission_evidence_observation(
                    workspace_id,
                    mission_id,
                    request_plan_id,
                    request_revision,
                    request_id_selector,
                )
            },
        )
        .await?;
    Ok(Json(MissionEvidenceObservationReadResponse {
        scope,
        mission_id: request.mission_id,
        plan_id: request.plan_id,
        plan_revision: request.plan_revision,
        request_id,
        observation: result
            .as_ref()
            .map(|record| projection::observation_wire(record, request.mission_id)),
    }))
}

async fn read_mutation_request(
    State(state): State<BackendState>,
    Path((workspace_path, request_path)): Path<(String, String)>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(request): Json<MissionEvidenceRequestReadRequest>,
) -> Result<Json<MissionEvidenceRequestReadResponse>, ApiError> {
    validate_read_scope(&workspace_path, &request.scope, request.session_id)?;
    require_identity(request.sender_activity_id)?;
    let request_id = crate::canonical_uuid(&request_path)?;
    if request.mutation.request_id() != request_id || request.mutation.scope() != &request.scope {
        return Err(ApiError::InvalidIdentity);
    }
    validate_mutation_intent(&request.mutation)?;
    let record = read_request_record(
        &state,
        &request.scope,
        request.session_id,
        request.sender_activity_id,
        request_id,
        principal,
    )
    .await?;
    let mutation = match record {
        Some(MissionEvidenceMutationRecord::ManifestBinding(binding)) => {
            let MissionEvidenceMutationRequest::Bind(intent) = &request.mutation else {
                return Err(ApiError::Runtime(RuntimeError::Conflict));
            };
            if !binding_matches_request(&binding, intent) {
                return Err(ApiError::Runtime(RuntimeError::Conflict));
            }
            Some(MissionEvidenceMutationWire::ManifestBinding(
                projection::binding_wire(&binding),
            ))
        }
        Some(MissionEvidenceMutationRecord::Observation(observation)) => {
            let MissionEvidenceMutationRequest::Observe(intent) = &request.mutation else {
                return Err(ApiError::Runtime(RuntimeError::Conflict));
            };
            if !observation_matches_request(
                &state,
                &request.scope,
                principal,
                MissionEvidenceRequestReadActor {
                    sender_activity_id: request.sender_activity_id,
                    session_id: request.session_id,
                },
                intent,
                &observation,
            )
            .await?
            {
                return Err(ApiError::Runtime(RuntimeError::Conflict));
            }
            Some(MissionEvidenceMutationWire::Observation(
                projection::observation_wire(&observation, intent.mission_id),
            ))
        }
        None => None,
    };
    Ok(Json(MissionEvidenceRequestReadResponse {
        scope: request.scope,
        session_id: request.session_id,
        sender_activity_id: request.sender_activity_id,
        request_id,
        mutation,
    }))
}

async fn read_request_record(
    state: &BackendState,
    scope: &crate::wire::WorkspaceQueryScope,
    session_id: Uuid,
    activity_id: Uuid,
    request_id: Uuid,
    principal: AuthenticatedPrincipal,
) -> Result<Option<MissionEvidenceMutationRecord>, ApiError> {
    let workspace_id = scope.workspace_id;
    state
        .run_workspace_reference_query(
            scope.clone(),
            Some(session_id),
            principal,
            move |runtime, _| {
                runtime.mission_evidence_request_for_actor(
                    workspace_id,
                    request_id,
                    activity_id,
                    session_id,
                    principal,
                )
            },
        )
        .await
}

async fn observation_matches_request(
    state: &BackendState,
    scope: &crate::wire::WorkspaceQueryScope,
    principal: AuthenticatedPrincipal,
    reader: MissionEvidenceRequestReadActor,
    intent: &MissionEvidenceObservationRequest,
    observation: &MissionEvidenceObservationRecord,
) -> Result<bool, ApiError> {
    if observation.request_id() != intent.request_id
        || observation.workspace_id().into_uuid() != scope.workspace_id
        || observation.plan_id().into_uuid() != intent.plan_id
        || observation.plan_revision() != intent.plan_revision
        || observation.expected_head_revision() != intent.expected_head_revision
        || observation
            .actor()
            .session_id()
            .map(forge_domain::AgentSessionId::into_uuid)
            != Some(intent.context.session_id)
        || observation.actor().leadership_revision()
            != Some(intent.context.expected_workspace_leadership_revision)
    {
        return Ok(false);
    }
    let workspace_id = scope.workspace_id;
    let mission_id = intent.mission_id;
    let plan_id = intent.plan_id;
    let plan_revision = intent.plan_revision;
    let binding = state
        .run_workspace_reference_query(
            scope.clone(),
            Some(reader.session_id),
            principal,
            move |runtime, _| {
                runtime.mission_evidence_binding(workspace_id, mission_id, plan_id, plan_revision)
            },
        )
        .await?;
    let Some(binding) = binding else {
        return Ok(false);
    };
    if observation.binding_request_id() != binding.request_id()
        || observation.cuts().partitions().len() != binding.manifest_bundle().manifests().len()
    {
        return Ok(false);
    }
    for manifest in binding.manifest_bundle().manifests() {
        let proposal = manifest.proposal();
        let Some(pair) = observation
            .cuts()
            .partitions()
            .iter()
            .find(|partition| partition.activity_id() == proposal.source().activity_id())
        else {
            return Ok(false);
        };
        let Ok(expected_t1_id) =
            heterarchy_orchestration_runtime::mission_evidence::mission_evidence_t1_cut_id(
                proposal,
            )
        else {
            return Ok(false);
        };
        let Ok(expected_review_id) =
            heterarchy_orchestration_runtime::mission_evidence::mission_evidence_review_cut_id(
                proposal,
                observation.request_id(),
            )
        else {
            return Ok(false);
        };
        if pair.t1_cut().cut_id() != expected_t1_id
            || pair.review_cut().review_cut_id() != expected_review_id
        {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn early_source_admission(
    state: &BackendState,
    request: &MissionEvidenceManifestPrepareRequest,
    principal: AuthenticatedPrincipal,
) -> Result<ForgeHttpClient, ApiError> {
    let context = &request.context;
    let scope = &context.scope;
    let spec = MissionEvidencePlanDigestSpec {
        workspace_id: scope.workspace_id,
        mission_id: request.mission_id,
        plan_id: request.plan_id,
        plan_revision: request.plan_revision,
        activity_id: context.sender_activity_id,
        session_id: context.session_id,
    };
    let digests = state
        .run_workspace_reference_query(
            scope.clone(),
            Some(context.session_id),
            principal,
            move |runtime, _| runtime.mission_evidence_plan_digests(spec, principal),
        )
        .await?;
    if digests.workspace_id().into_uuid() != scope.workspace_id
        || digests.mission_id().into_uuid() != request.mission_id
        || digests.plan_id().into_uuid() != request.plan_id
        || digests.revision() != request.plan_revision
    {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    Ok(configured_source(state, scope).await?.1)
}

async fn configured_source(
    state: &BackendState,
    scope: &crate::wire::WorkspaceQueryScope,
) -> Result<(ResolvedWorkspaceScope, ForgeHttpClient), ApiError> {
    state.workspace_scope_proof(scope.clone()).await?;
    let authority = state
        .coordinator_authority
        .as_ref()
        .ok_or(ApiError::WorkspaceScopeUnavailable)?;
    authority
        .mission_evidence_source(scope)
        .map_err(|error| match error {
            crate::CoordinatorAuthorityError::Denied => ApiError::Runtime(RuntimeError::Forbidden),
            crate::CoordinatorAuthorityError::Stale => ApiError::Runtime(RuntimeError::Conflict),
            crate::CoordinatorAuthorityError::Unavailable => ApiError::WorkspaceScopeUnavailable,
        })
}

fn validate_source_client(
    client: &ForgeHttpClient,
    trusted_scope: &ResolvedWorkspaceScope,
    proposals: &[EvaluationManifestProposal],
) -> Result<(), ApiError> {
    if proposals.is_empty() {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    for proposal in proposals {
        let source = proposal.source();
        if source.backend_id() != client.expected_backend_id()
            || source.delegation_destination_id() != client.destination_id()
            || source.workspace_id() != trusted_scope.workspace_id()
        {
            return Err(ApiError::Runtime(RuntimeError::Conflict));
        }
    }
    Ok(())
}

fn preparation_response(
    scope: crate::wire::WorkspaceQueryScope,
    mission_id: Uuid,
    plan_id: Uuid,
    plan_revision: u64,
    preparation: &MissionEvidenceManifestPreparation,
) -> Result<MissionEvidenceManifestPrepareResponse, ApiError> {
    Ok(MissionEvidenceManifestPrepareResponse {
        scope,
        mission_id,
        plan_id,
        plan_revision,
        status: if preparation.ready() {
            MissionEvidencePreparationStatusWire::Ready
        } else {
            MissionEvidencePreparationStatusWire::Pending
        },
        proposals: preparation
            .proposals()
            .iter()
            .map(source::proposal_wire)
            .collect(),
        pending_targets: pending_target_wires(preparation.pending_targets())?,
    })
}

fn pending_target_wires(
    targets: &[MissionEvidencePreparationPendingTarget],
) -> Result<Vec<MissionEvidencePendingTargetWire>, ApiError> {
    targets
        .iter()
        .map(|target| {
            let MissionEvidenceIdentityResolution::Pending {
                agent_id,
                session_id,
                reason,
                registration_request_id,
                activation_receipt_id,
            } = target.identity()
            else {
                return Err(ApiError::Runtime(RuntimeError::Conflict));
            };
            Ok(MissionEvidencePendingTargetWire {
                obligation_id: target.obligation_id().into_uuid(),
                agent_id: agent_id.into_uuid(),
                session_id: session_id.into_uuid(),
                reason: pending_identity_reason(*reason),
                registration_request_id: *registration_request_id,
                activation_receipt_id: *activation_receipt_id,
            })
        })
        .collect()
}

fn pending_identity_reason(
    reason: MissionEvidenceIdentityPendingReason,
) -> MissionEvidenceIdentityPendingReasonWire {
    match reason {
        MissionEvidenceIdentityPendingReason::NoRegistrationHistory => {
            MissionEvidenceIdentityPendingReasonWire::NoRegistrationHistory
        }
        MissionEvidenceIdentityPendingReason::RegistrationNotCommitted => {
            MissionEvidenceIdentityPendingReasonWire::RegistrationNotCommitted
        }
        MissionEvidenceIdentityPendingReason::ActivationMissing => {
            MissionEvidenceIdentityPendingReasonWire::ActivationMissing
        }
        MissionEvidenceIdentityPendingReason::ProfileLinkMissing => {
            MissionEvidenceIdentityPendingReasonWire::ProfileLinkMissing
        }
        MissionEvidenceIdentityPendingReason::ParticipationMappingMissing => {
            MissionEvidenceIdentityPendingReasonWire::ParticipationMappingMissing
        }
        MissionEvidenceIdentityPendingReason::ReplacementRebindMissing => {
            MissionEvidenceIdentityPendingReasonWire::ReplacementRebindMissing
        }
        MissionEvidenceIdentityPendingReason::AmbiguousHistory => {
            MissionEvidenceIdentityPendingReasonWire::AmbiguousHistory
        }
    }
}

fn pending_source(
    proposal: &EvaluationManifestProposal,
    stage: MissionEvidenceSourceStageWire,
    reason: MissionEvidencePendingReasonWire,
    cut_id: Option<Uuid>,
    review_cut_id: Option<Uuid>,
) -> MissionEvidencePendingSourceWire {
    MissionEvidencePendingSourceWire {
        activity_id: proposal.source().activity_id().into_uuid(),
        stage,
        reason,
        manifest_id: proposal.manifest_id().into_uuid(),
        manifest_revision: proposal.revision().get(),
        manifest_digest: source::digest_hex(proposal.digest()),
        cut_id,
        review_cut_id,
    }
}

fn preparation_spec(
    workspace_id: Uuid,
    mission_id: Uuid,
    plan_id: Uuid,
    plan_revision: u64,
    partitions: &[MissionEvidenceCandidatePartitionWire],
    context: &crate::wire::mission_evaluation::MissionEvaluationMutationContextWire,
) -> MissionEvidencePreparationSpec {
    MissionEvidencePreparationSpec {
        workspace_id,
        mission_id,
        plan_id,
        plan_revision,
        partitions: partitions.iter().map(candidate_partition).collect(),
        participant: participant(context),
    }
}

fn candidate_partition(
    partition: &MissionEvidenceCandidatePartitionWire,
) -> MissionEvidenceCandidatePartitionSpec {
    MissionEvidenceCandidatePartitionSpec {
        activity_id: partition.activity_id,
        members: partition
            .members
            .iter()
            .map(|member| MissionEvidenceCandidateMemberSpec {
                obligation_id: member.obligation_id,
                assignment_id: member.assignment_id,
                assignment_revision: member.assignment_revision,
                target_agent_id: member.target_agent_id,
                target_session_id: member.target_session_id,
            })
            .collect(),
    }
}

fn candidates_from_binding(
    binding: &MissionEvidenceManifestBindingRecord,
) -> Vec<MissionEvidenceCandidatePartitionSpec> {
    binding
        .manifest_bundle()
        .manifests()
        .iter()
        .map(|manifest| MissionEvidenceCandidatePartitionSpec {
            activity_id: manifest.proposal().source().activity_id().into_uuid(),
            members: manifest
                .proposal()
                .members()
                .iter()
                .map(|member| MissionEvidenceCandidateMemberSpec {
                    obligation_id: member.obligation_id().into_uuid(),
                    assignment_id: member.assignment_id().into_uuid(),
                    assignment_revision: member.assignment_revision().get(),
                    target_agent_id: member.target_agent_id().into_uuid(),
                    target_session_id: member.target_session_id().into_uuid(),
                })
                .collect(),
        })
        .collect()
}

fn preparation_matches_binding(
    preparation: &MissionEvidenceManifestPreparation,
    binding: &MissionEvidenceManifestBindingRecord,
) -> bool {
    preparation.proposals().len() == binding.manifest_bundle().manifests().len()
        && preparation.proposals().iter().all(|proposal| {
            binding
                .manifest_bundle()
                .manifests()
                .iter()
                .any(|registered| registered.proposal() == proposal)
        })
}

fn binding_matches_request(
    binding: &MissionEvidenceManifestBindingRecord,
    request: &MissionEvidenceManifestBindRequest,
) -> bool {
    if binding.request_id() != request.request_id
        || binding.workspace_id().into_uuid() != request.context.scope.workspace_id
        || binding.mission_id().into_uuid() != request.mission_id
        || binding.plan_id().into_uuid() != request.plan_id
        || binding.plan_revision() != request.plan_revision
        || binding
            .actor()
            .session_id()
            .map(forge_domain::AgentSessionId::into_uuid)
            != Some(request.context.session_id)
        || binding.actor().leadership_revision()
            != Some(request.context.expected_workspace_leadership_revision)
        || binding.manifest_bundle().manifests().len() != request.partitions.len()
    {
        return false;
    }
    request.partitions.iter().all(|partition| {
        binding
            .manifest_bundle()
            .manifests()
            .iter()
            .find(|manifest| {
                manifest.proposal().source().activity_id().into_uuid() == partition.activity_id
            })
            .is_some_and(|manifest| {
                let declarations = manifest.proposal().members();
                declarations.len() == partition.members.len()
                    && partition.members.iter().all(|candidate| {
                        declarations.iter().any(|member| {
                            member.obligation_id().into_uuid() == candidate.obligation_id
                                && member.assignment_id().into_uuid() == candidate.assignment_id
                                && member.assignment_revision().get()
                                    == candidate.assignment_revision
                                && member.target_agent_id().into_uuid() == candidate.target_agent_id
                                && member.target_session_id().into_uuid()
                                    == candidate.target_session_id
                        })
                    })
            })
    })
}

fn participant(
    context: &crate::wire::mission_evaluation::MissionEvaluationMutationContextWire,
) -> MissionEvaluationParticipantSpec {
    MissionEvaluationParticipantSpec {
        activity_id: context.sender_activity_id,
        session_id: context.session_id,
        expected_workspace_leadership_revision: context.expected_workspace_leadership_revision,
    }
}

fn validate_context(
    workspace_path: &str,
    context: &crate::wire::mission_evaluation::MissionEvaluationMutationContextWire,
) -> Result<(), ApiError> {
    validate_read_scope(workspace_path, &context.scope, context.session_id)?;
    require_identity(context.sender_activity_id)?;
    if context.expected_workspace_leadership_revision < 1 {
        return Err(ApiError::InvalidIdentity);
    }
    Ok(())
}

fn validate_mutation_intent(mutation: &MissionEvidenceMutationRequest) -> Result<(), ApiError> {
    match mutation {
        MissionEvidenceMutationRequest::Bind(request) => {
            let workspace_path = request.context.scope.workspace_id.to_string();
            validate_context(&workspace_path, &request.context)?;
            require_identity(request.request_id)?;
            validate_plan_selectors(request.mission_id, request.plan_id, request.plan_revision)?;
            validate_partitions(&request.partitions)
        }
        MissionEvidenceMutationRequest::Observe(request) => {
            let workspace_path = request.context.scope.workspace_id.to_string();
            validate_context(&workspace_path, &request.context)?;
            require_identity(request.request_id)?;
            validate_plan_selectors(request.mission_id, request.plan_id, request.plan_revision)?;
            require_nonnegative_revision(request.expected_head_revision)
        }
    }
}

fn validate_read_scope(
    workspace_path: &str,
    scope: &crate::wire::WorkspaceQueryScope,
    session_id: Uuid,
) -> Result<(), ApiError> {
    crate::app_support::validate_workspace_reference(scope, Some(session_id))?;
    if session_id.is_nil() || crate::canonical_uuid(workspace_path)? != scope.workspace_id {
        return Err(ApiError::InvalidIdentity);
    }
    Ok(())
}

fn validate_plan_selectors(mission_id: Uuid, plan_id: Uuid, revision: u64) -> Result<(), ApiError> {
    require_identity(mission_id)?;
    require_identity(plan_id)?;
    require_revision(revision)
}

fn validate_partitions(
    partitions: &[MissionEvidenceCandidatePartitionWire],
) -> Result<(), ApiError> {
    if partitions.is_empty() || partitions.len() > MAX_CANDIDATE_PARTITIONS {
        return Err(ApiError::InvalidIdentity);
    }
    let total = partitions.iter().try_fold(0_usize, |sum, partition| {
        sum.checked_add(partition.members.len())
    });
    if total.is_none_or(|count| count == 0 || count > MAX_CANDIDATE_MEMBERS) {
        return Err(ApiError::InvalidIdentity);
    }
    let mut activities = BTreeSet::new();
    let mut obligations = BTreeSet::new();
    let mut assignments = BTreeSet::new();
    for partition in partitions {
        require_identity(partition.activity_id)?;
        if !activities.insert(partition.activity_id) {
            return Err(ApiError::InvalidIdentity);
        }
        if partition.members.is_empty() {
            return Err(ApiError::InvalidIdentity);
        }
        for member in &partition.members {
            require_identity(member.obligation_id)?;
            require_identity(member.assignment_id)?;
            require_identity(member.target_agent_id)?;
            require_identity(member.target_session_id)?;
            if member.assignment_revision < 1
                || !obligations.insert(member.obligation_id)
                || !assignments.insert(member.assignment_id)
            {
                return Err(ApiError::InvalidIdentity);
            }
        }
    }
    Ok(())
}

fn require_identity(value: Uuid) -> Result<(), ApiError> {
    if value.is_nil() {
        Err(ApiError::InvalidIdentity)
    } else {
        Ok(())
    }
}

fn require_revision(value: u64) -> Result<(), ApiError> {
    if value == 0 || value > MAX_SQLITE_INTEGER {
        Err(ApiError::InvalidIdentity)
    } else {
        Ok(())
    }
}

fn require_nonnegative_revision(value: u64) -> Result<(), ApiError> {
    if value > MAX_SQLITE_INTEGER {
        Err(ApiError::InvalidIdentity)
    } else {
        Ok(())
    }
}

fn canonical_revision(value: &str) -> Result<u64, ApiError> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| ApiError::InvalidIdentity)?;
    if parsed.to_string() != value || parsed > MAX_SQLITE_INTEGER {
        return Err(ApiError::InvalidIdentity);
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc};

    use axum::extract::DefaultBodyLimit;
    use forge_domain::{AuthenticatedPrincipal, DirectTokenId};
    use http_body_util::{BodyExt, Full};
    use hyper::body::Bytes;
    use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE, HOST};
    use hyper::{Method, Request, StatusCode};
    use hyper_util::rt::TokioIo;
    use tokio::net::{TcpListener, TcpStream};
    use uuid::Uuid;

    use super::*;
    use crate::realtime::RealtimeHub;
    use crate::wire::mission_evaluation::MissionEvaluationMutationContextWire;
    use crate::wire::mission_evidence::{
        MissionEvidenceCandidateMemberWire, MissionEvidenceCandidatePartitionWire,
        MissionEvidenceManifestBindRequest,
    };
    use heterarchy_orchestration_runtime::OrchestrationRuntime;

    #[tokio::test]
    async fn route_rejects_oversized_json_before_source_work() -> Result<(), Box<dyn Error>> {
        let data_root =
            std::env::temp_dir().join(format!("mission-evidence-body-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&data_root)?;
        let runtime = OrchestrationRuntime::open(&data_root)?;
        let state = BackendState::new(runtime, Arc::new(RealtimeHub::new()));
        let principal =
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(0xE701)));
        let app = routes()
            .with_state(state.clone())
            .layer(DefaultBodyLimit::max(
                crate::wire::mission_evidence::MAX_MISSION_EVIDENCE_BODY_BYTES,
            ))
            .layer(Extension(principal));
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let stream = TcpStream::connect(address).await?;
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
        let connection_task = tokio::spawn(connection);
        let workspace_id = Uuid::from_u128(0xE702);
        let body_request = MissionEvidenceManifestBindRequest {
            context: MissionEvaluationMutationContextWire {
                scope: crate::wire::WorkspaceQueryScope {
                    workspace_id,
                    location_id: Uuid::from_u128(0xE703),
                    location_revision: 1,
                    workspace_revision: 1,
                    locator: "isolated".to_owned(),
                },
                session_id: Uuid::from_u128(0xE704),
                sender_activity_id: Uuid::from_u128(0xE705),
                expected_workspace_leadership_revision: 1,
            },
            request_id: Uuid::from_u128(0xE706),
            mission_id: Uuid::from_u128(0xE707),
            plan_id: Uuid::from_u128(0xE708),
            plan_revision: 1,
            partitions: vec![MissionEvidenceCandidatePartitionWire {
                activity_id: Uuid::from_u128(0xE709),
                members: vec![MissionEvidenceCandidateMemberWire {
                    obligation_id: Uuid::from_u128(0xE70A),
                    assignment_id: Uuid::from_u128(0xE70B),
                    assignment_revision: 1,
                    target_agent_id: Uuid::from_u128(0xE70C),
                    target_session_id: Uuid::from_u128(0xE70D),
                }],
            }],
        };
        let json_body = serde_json::to_string(&body_request)?;
        let body = format!(
            "{json_body}{}",
            " ".repeat(
                crate::wire::mission_evidence::MAX_MISSION_EVIDENCE_BODY_BYTES
                    .saturating_add(1)
                    .saturating_sub(json_body.len())
            )
        );
        assert!(body.len() > crate::wire::mission_evidence::MAX_MISSION_EVIDENCE_BODY_BYTES);
        let body_length = body.len();
        let request = Request::builder()
            .method(Method::POST)
            .uri(format!(
                "/internal/v1/workspaces/{workspace_id}/mission-evidence/manifests/bind"
            ))
            .header(HOST, address.to_string())
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, body_length)
            .body(Full::new(Bytes::from(body)))?;
        let response = sender.send_request(request).await?;
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let _body = response.into_body().collect().await?.to_bytes();
        connection_task.abort();
        let _connection_result = connection_task.await;
        server.abort();
        let _server_result = server.await;
        state.catalog_execution.stop_admission();
        state.catalog_execution.drain().await;
        drop(state);
        std::fs::remove_dir_all(data_root)?;
        Ok(())
    }
}
