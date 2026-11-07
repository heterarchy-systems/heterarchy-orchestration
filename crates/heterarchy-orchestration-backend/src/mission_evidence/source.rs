//! Source adapter for the configured Forge evaluation-evidence HTTP owner.

use crate::wire::mission_evidence::MissionEvidencePendingReasonWire;
use forge_backend::client::ForgeHttpEvaluationEvidenceError;
use forge_backend::{ForgeHttpClient, ops::evaluation_evidence::wire as source_wire};
use forge_control::{
    EvaluationEvidenceOperation, EvaluationEvidenceRequest, ResolvedWorkspaceScope,
};
use forge_domain::{
    AgentAssignmentId, AgentAssignmentRevision, AgentInstanceId, AgentSessionId,
    EvaluationAdmissionTiming, EvaluationClockUncertainty, EvaluationCoverageUnknownReason,
    EvaluationCutId, EvaluationDigest, EvaluationManifestMember, EvaluationManifestProposal,
    EvaluationOriginalAssignmentBinding, EvaluationOriginalBindingStatus,
    EvaluationRegisteredManifest, EvaluationRegisteredManifestMember, EvaluationReviewCut,
    EvaluationReviewCutId, EvaluationReviewCutMember, EvaluationReviewOutcome,
    EvaluationReviewReceipt, EvaluationReviewState, EvaluationSourceIdentity,
    EvaluationSourceSequence, EvaluationSubmissionReceipt, EvaluationT1CloseRequest,
    EvaluationT1Coverage, EvaluationT1CoverageCut, EvaluationT1CoverageMember, TimestampMillis,
};
use heterarchy_orchestration_runtime::RuntimeError;

use crate::{ApiError, BackendState};

pub(super) enum SourceProgress<T> {
    Ready(T),
    Pending(MissionEvidencePendingReasonWire),
}

pub(super) async fn get_or_seal_manifest(
    state: &BackendState,
    client: &ForgeHttpClient,
    scope: &ResolvedWorkspaceScope,
    proposal: &EvaluationManifestProposal,
) -> Result<SourceProgress<EvaluationRegisteredManifest>, ApiError> {
    match read_manifest(state, client, scope, proposal).await? {
        SourceProgress::Ready(Some(manifest)) => Ok(SourceProgress::Ready(manifest)),
        SourceProgress::Ready(None) => {
            let request = source_request(
                scope,
                proposal.source(),
                EvaluationEvidenceOperation::SealManifest {
                    proposal: proposal.clone(),
                },
            );
            let source_result =
                with_source_slot(state, client.seal_evaluation_manifest(&request)).await?;
            let response = match source_result {
                Ok(response) => response,
                Err(error) if error.is_unknown_outcome() => {
                    return match read_manifest(state, client, scope, proposal).await? {
                        SourceProgress::Ready(Some(manifest)) => {
                            Ok(SourceProgress::Ready(manifest))
                        }
                        SourceProgress::Ready(None) => Ok(SourceProgress::Pending(
                            MissionEvidencePendingReasonWire::MutationOutcomeUnknown,
                        )),
                        SourceProgress::Pending(_) => Ok(SourceProgress::Pending(
                            MissionEvidencePendingReasonWire::ReadbackUnavailable,
                        )),
                    };
                }
                Err(error) => return map_source_mutation_error(&error),
            };
            match response {
                source_wire::EvaluationEvidenceResponseWire::ManifestSealed {
                    source,
                    manifest,
                } if source == source_identity_wire(proposal.source()) => {
                    decode_registered_manifest(proposal, manifest).map(SourceProgress::Ready)
                }
                _ => Err(ApiError::Runtime(RuntimeError::Conflict)),
            }
        }
        SourceProgress::Pending(reason) => Ok(SourceProgress::Pending(reason)),
    }
}

pub(super) async fn get_or_close_t1(
    state: &BackendState,
    client: &ForgeHttpClient,
    scope: &ResolvedWorkspaceScope,
    proposal: &EvaluationManifestProposal,
    cut_id: EvaluationCutId,
) -> Result<SourceProgress<EvaluationT1CoverageCut>, ApiError> {
    let close_request = EvaluationT1CloseRequest::new(
        cut_id,
        proposal.manifest_id(),
        proposal.revision(),
        proposal.digest(),
        proposal.deadline(),
    );
    match read_t1_cut(state, client, scope, proposal, &close_request).await? {
        SourceProgress::Ready(Some(cut)) => Ok(SourceProgress::Ready(cut)),
        SourceProgress::Ready(None) => {
            let request = source_request(
                scope,
                proposal.source(),
                EvaluationEvidenceOperation::CloseT1 {
                    request: close_request,
                },
            );
            let source_result =
                with_source_slot(state, client.close_evaluation_t1(&request)).await?;
            let response = match source_result {
                Ok(response) => response,
                Err(error) if error.is_unknown_outcome() => {
                    return match read_t1_cut(state, client, scope, proposal, &close_request).await?
                    {
                        SourceProgress::Ready(Some(cut)) => Ok(SourceProgress::Ready(cut)),
                        SourceProgress::Ready(None) => Ok(SourceProgress::Pending(
                            MissionEvidencePendingReasonWire::MutationOutcomeUnknown,
                        )),
                        SourceProgress::Pending(_) => Ok(SourceProgress::Pending(
                            MissionEvidencePendingReasonWire::ReadbackUnavailable,
                        )),
                    };
                }
                Err(error) => return map_source_mutation_error(&error),
            };
            match response {
                source_wire::EvaluationEvidenceResponseWire::T1Closed { source, outcome }
                    if source == source_identity_wire(proposal.source()) =>
                {
                    match outcome {
                        source_wire::EvaluationT1ClosureWire::Closed { cut } => {
                            decode_t1_cut(proposal, &close_request, cut).map(SourceProgress::Ready)
                        }
                        source_wire::EvaluationT1ClosureWire::NotClosed { .. } => Ok(
                            SourceProgress::Pending(MissionEvidencePendingReasonWire::T1NotClosed),
                        ),
                    }
                }
                _ => Err(ApiError::Runtime(RuntimeError::Conflict)),
            }
        }
        SourceProgress::Pending(reason) => Ok(SourceProgress::Pending(reason)),
    }
}

pub(super) async fn get_or_create_review_cut(
    state: &BackendState,
    client: &ForgeHttpClient,
    scope: &ResolvedWorkspaceScope,
    proposal: &EvaluationManifestProposal,
    t1_request: &EvaluationT1CloseRequest,
    t1_cut: &EvaluationT1CoverageCut,
    review_cut_id: EvaluationReviewCutId,
) -> Result<SourceProgress<EvaluationReviewCut>, ApiError> {
    match read_review_cut(
        state,
        client,
        scope,
        proposal,
        t1_request,
        review_cut_id,
        t1_cut,
    )
    .await?
    {
        SourceProgress::Ready(Some(cut)) => Ok(SourceProgress::Ready(cut)),
        SourceProgress::Ready(None) => {
            let request = source_request(
                scope,
                proposal.source(),
                EvaluationEvidenceOperation::CreateReviewCut {
                    review_cut_id,
                    t1_request: *t1_request,
                },
            );
            let source_result =
                with_source_slot(state, client.create_evaluation_review_cut(&request)).await?;
            let response = match source_result {
                Ok(response) => response,
                Err(error) if error.is_unknown_outcome() => {
                    return match read_review_cut(
                        state,
                        client,
                        scope,
                        proposal,
                        t1_request,
                        review_cut_id,
                        t1_cut,
                    )
                    .await?
                    {
                        SourceProgress::Ready(Some(cut)) => Ok(SourceProgress::Ready(cut)),
                        SourceProgress::Ready(None) => Ok(SourceProgress::Pending(
                            MissionEvidencePendingReasonWire::MutationOutcomeUnknown,
                        )),
                        SourceProgress::Pending(_) => Ok(SourceProgress::Pending(
                            MissionEvidencePendingReasonWire::ReadbackUnavailable,
                        )),
                    };
                }
                Err(error) => return map_source_mutation_error(&error),
            };
            match response {
                source_wire::EvaluationEvidenceResponseWire::ReviewCutCreated {
                    source,
                    review_cut,
                } if source == source_identity_wire(proposal.source())
                    && review_cut.review_cut_id == review_cut_id.into_uuid()
                    && review_cut.t1_cut_id == t1_cut.cut_id().into_uuid() =>
                {
                    decode_review_cut(proposal, t1_cut, review_cut).map(SourceProgress::Ready)
                }
                _ => Err(ApiError::Runtime(RuntimeError::Conflict)),
            }
        }
        SourceProgress::Pending(reason) => Ok(SourceProgress::Pending(reason)),
    }
}

pub(super) async fn read_manifest(
    state: &BackendState,
    client: &ForgeHttpClient,
    scope: &ResolvedWorkspaceScope,
    proposal: &EvaluationManifestProposal,
) -> Result<SourceProgress<Option<EvaluationRegisteredManifest>>, ApiError> {
    let request = source_request(
        scope,
        proposal.source(),
        EvaluationEvidenceOperation::GetManifest {
            manifest_id: proposal.manifest_id(),
            revision: proposal.revision(),
            digest: proposal.digest(),
        },
    );
    let source_result = with_source_slot(state, client.get_evaluation_manifest(&request)).await?;
    let response = match source_result {
        Ok(response) => response,
        Err(error) => return map_source_read_error(&error),
    };
    match response {
        source_wire::EvaluationEvidenceResponseWire::ManifestRead {
            source,
            status,
            manifest,
        } if source == source_identity_wire(proposal.source()) => match (status, manifest) {
            (source_wire::EvaluationRecordReadStatus::Found, Some(manifest)) => {
                decode_registered_manifest(proposal, manifest)
                    .map(Some)
                    .map(SourceProgress::Ready)
            }
            (source_wire::EvaluationRecordReadStatus::Missing, None) => {
                Ok(SourceProgress::Ready(None))
            }
            _ => Err(ApiError::Runtime(RuntimeError::Conflict)),
        },
        _ => Err(ApiError::Runtime(RuntimeError::Conflict)),
    }
}

async fn read_t1_cut(
    state: &BackendState,
    client: &ForgeHttpClient,
    scope: &ResolvedWorkspaceScope,
    proposal: &EvaluationManifestProposal,
    close_request: &EvaluationT1CloseRequest,
) -> Result<SourceProgress<Option<EvaluationT1CoverageCut>>, ApiError> {
    let request = source_request(
        scope,
        proposal.source(),
        EvaluationEvidenceOperation::GetT1Cut {
            request: *close_request,
        },
    );
    let source_result = with_source_slot(state, client.get_evaluation_t1_cut(&request)).await?;
    let response = match source_result {
        Ok(response) => response,
        Err(error) => return map_source_read_error(&error),
    };
    match response {
        source_wire::EvaluationEvidenceResponseWire::T1Read {
            source,
            status,
            cut,
        } if source == source_identity_wire(proposal.source()) => match (status, cut) {
            (source_wire::EvaluationRecordReadStatus::Found, Some(cut)) => {
                decode_t1_cut(proposal, close_request, cut)
                    .map(Some)
                    .map(SourceProgress::Ready)
            }
            (source_wire::EvaluationRecordReadStatus::Missing, None) => {
                Ok(SourceProgress::Ready(None))
            }
            _ => Err(ApiError::Runtime(RuntimeError::Conflict)),
        },
        _ => Err(ApiError::Runtime(RuntimeError::Conflict)),
    }
}

async fn read_review_cut(
    state: &BackendState,
    client: &ForgeHttpClient,
    scope: &ResolvedWorkspaceScope,
    proposal: &EvaluationManifestProposal,
    t1_request: &EvaluationT1CloseRequest,
    review_cut_id: EvaluationReviewCutId,
    t1_cut: &EvaluationT1CoverageCut,
) -> Result<SourceProgress<Option<EvaluationReviewCut>>, ApiError> {
    let request = source_request(
        scope,
        proposal.source(),
        EvaluationEvidenceOperation::GetReviewCut {
            review_cut_id,
            t1_request: *t1_request,
        },
    );
    let source_result = with_source_slot(state, client.get_evaluation_review_cut(&request)).await?;
    let response = match source_result {
        Ok(response) => response,
        Err(error) => return map_source_read_error(&error),
    };
    match response {
        source_wire::EvaluationEvidenceResponseWire::ReviewCutRead {
            source,
            status,
            review_cut,
        } if source == source_identity_wire(proposal.source()) => match (status, review_cut) {
            (source_wire::EvaluationRecordReadStatus::Found, Some(review_cut)) => {
                if review_cut.review_cut_id != review_cut_id.into_uuid()
                    || review_cut.t1_cut_id != t1_cut.cut_id().into_uuid()
                {
                    return Err(ApiError::Runtime(RuntimeError::Conflict));
                }
                decode_review_cut(proposal, t1_cut, review_cut)
                    .map(Some)
                    .map(SourceProgress::Ready)
            }
            (source_wire::EvaluationRecordReadStatus::Missing, None) => {
                Ok(SourceProgress::Ready(None))
            }
            _ => Err(ApiError::Runtime(RuntimeError::Conflict)),
        },
        _ => Err(ApiError::Runtime(RuntimeError::Conflict)),
    }
}

fn source_request(
    scope: &ResolvedWorkspaceScope,
    source: EvaluationSourceIdentity,
    operation: EvaluationEvidenceOperation,
) -> EvaluationEvidenceRequest {
    EvaluationEvidenceRequest::new(scope.clone(), source, operation)
}

async fn with_source_slot<T>(
    state: &BackendState,
    operation: impl std::future::Future<Output = Result<T, ForgeHttpEvaluationEvidenceError>>,
) -> Result<Result<T, ForgeHttpEvaluationEvidenceError>, ApiError> {
    let _permit = state
        .forge_query_slots
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| ApiError::Unavailable)?;
    Ok(operation.await)
}

fn map_source_mutation_error<T>(
    error: &ForgeHttpEvaluationEvidenceError,
) -> Result<SourceProgress<T>, ApiError> {
    match error {
        ForgeHttpEvaluationEvidenceError::Rejected { .. } => {
            Err(ApiError::Runtime(RuntimeError::Conflict))
        }
        ForgeHttpEvaluationEvidenceError::ReadFailed { .. } => Ok(SourceProgress::Pending(
            MissionEvidencePendingReasonWire::SourceUnavailable,
        )),
        ForgeHttpEvaluationEvidenceError::UnknownOutcome { .. } => Ok(SourceProgress::Pending(
            MissionEvidencePendingReasonWire::MutationOutcomeUnknown,
        )),
    }
}

fn map_source_read_error<T>(
    error: &ForgeHttpEvaluationEvidenceError,
) -> Result<SourceProgress<T>, ApiError> {
    match error {
        ForgeHttpEvaluationEvidenceError::Rejected { .. } => {
            Err(ApiError::Runtime(RuntimeError::Conflict))
        }
        ForgeHttpEvaluationEvidenceError::ReadFailed { .. }
        | ForgeHttpEvaluationEvidenceError::UnknownOutcome { .. } => Ok(SourceProgress::Pending(
            MissionEvidencePendingReasonWire::SourceUnavailable,
        )),
    }
}

pub(super) fn proposal_wire(
    proposal: &EvaluationManifestProposal,
) -> source_wire::EvaluationManifestProposalWire {
    source_wire::EvaluationManifestProposalWire {
        manifest_id: proposal.manifest_id().into_uuid(),
        revision: proposal.revision().get(),
        source: source_identity_wire(proposal.source()),
        plan: source_wire::EvaluationPlanBindingWire {
            plan_id: proposal.plan().plan_id().into_uuid(),
            revision: proposal.plan().revision().get(),
            digest: digest_hex(proposal.plan().digest()),
        },
        deadline_ms: proposal.deadline().get(),
        digest: digest_hex(proposal.digest()),
        members: proposal
            .members()
            .iter()
            .copied()
            .map(member_wire)
            .collect(),
    }
}

pub(super) fn registered_manifest_wire(
    manifest: &EvaluationRegisteredManifest,
) -> source_wire::EvaluationRegisteredManifestWire {
    source_wire::EvaluationRegisteredManifestWire {
        proposal: proposal_wire(manifest.proposal()),
        members: manifest
            .members()
            .iter()
            .map(
                |member| source_wire::EvaluationRegisteredManifestMemberWire {
                    declaration: member_wire(member.declaration()),
                    binding: match member.binding() {
                        EvaluationOriginalBindingStatus::Bound(binding) => {
                            source_wire::EvaluationOriginalBindingWire::Bound {
                                digest: digest_hex(binding.digest()),
                                assignment_revision: binding.assignment_revision().get(),
                                source_sequence: binding.source_sequence().get(),
                            }
                        }
                        EvaluationOriginalBindingStatus::UnknownHistorical => {
                            source_wire::EvaluationOriginalBindingWire::UnknownHistorical
                        }
                    },
                },
            )
            .collect(),
        source_sequence: manifest.source_sequence().get(),
        seal_timing: timing_wire(&manifest.seal_timing()),
    }
}

pub(super) fn t1_cut_wire(
    cut: &EvaluationT1CoverageCut,
) -> source_wire::EvaluationT1CoverageCutWire {
    source_wire::EvaluationT1CoverageCutWire {
        cut_id: cut.cut_id().into_uuid(),
        manifest_id: cut.manifest_id().into_uuid(),
        manifest_revision: cut.manifest_revision().get(),
        manifest_digest: digest_hex(cut.manifest_digest()),
        deadline_ms: cut.deadline().get(),
        closed_at_ms: cut.closed_at().get(),
        source_high_water: cut.source_high_water().get(),
        members: cut
            .members()
            .iter()
            .map(|member| source_wire::EvaluationT1CoverageMemberWire {
                member: member_wire(member.member()),
                coverage: coverage_wire(member.coverage()),
            })
            .collect(),
    }
}

pub(super) fn review_cut_wire(cut: &EvaluationReviewCut) -> source_wire::EvaluationReviewCutWire {
    source_wire::EvaluationReviewCutWire {
        review_cut_id: cut.review_cut_id().into_uuid(),
        t1_cut_id: cut.t1_cut_id().into_uuid(),
        source_high_water: cut.source_high_water().get(),
        members: cut.members().iter().map(review_member_wire).collect(),
    }
}

pub(super) fn review_member_wire(
    member: &EvaluationReviewCutMember,
) -> source_wire::EvaluationReviewCutMemberWire {
    source_wire::EvaluationReviewCutMemberWire {
        member: member_wire(member.member()),
        coverage: coverage_wire(member.coverage()),
        submission: member.submission().map(submission_wire),
        review: review_state_wire(member.review()),
    }
}

fn decode_registered_manifest(
    proposal: &EvaluationManifestProposal,
    wire: source_wire::EvaluationRegisteredManifestWire,
) -> Result<EvaluationRegisteredManifest, ApiError> {
    if wire.proposal != proposal_wire(proposal) || wire.members.len() != proposal.members().len() {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    let mut members = Vec::with_capacity(wire.members.len());
    for (registered, expected) in wire.members.into_iter().zip(proposal.members()) {
        if registered.declaration != member_wire(*expected) {
            return Err(ApiError::Runtime(RuntimeError::Conflict));
        }
        let binding = match registered.binding {
            source_wire::EvaluationOriginalBindingWire::Bound {
                digest,
                assignment_revision,
                source_sequence,
            } => EvaluationOriginalBindingStatus::Bound(EvaluationOriginalAssignmentBinding::new(
                parse_digest(&digest)?,
                AgentAssignmentRevision::new(assignment_revision)
                    .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?,
                EvaluationSourceSequence::new(source_sequence)
                    .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?,
            )),
            source_wire::EvaluationOriginalBindingWire::UnknownHistorical => {
                EvaluationOriginalBindingStatus::UnknownHistorical
            }
        };
        members.push(EvaluationRegisteredManifestMember::new(*expected, binding));
    }
    EvaluationRegisteredManifest::from_source_records(
        proposal.clone(),
        members,
        EvaluationSourceSequence::new(wire.source_sequence)
            .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?,
        timing_from_wire(&wire.seal_timing)?,
    )
    .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))
}

fn decode_t1_cut(
    proposal: &EvaluationManifestProposal,
    request: &EvaluationT1CloseRequest,
    wire: source_wire::EvaluationT1CoverageCutWire,
) -> Result<EvaluationT1CoverageCut, ApiError> {
    if wire.cut_id != request.cut_id().into_uuid()
        || wire.manifest_id != proposal.manifest_id().into_uuid()
        || wire.manifest_revision != proposal.revision().get()
        || wire.manifest_digest != digest_hex(proposal.digest())
        || wire.deadline_ms != proposal.deadline().get()
    {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    let mut members = Vec::with_capacity(wire.members.len());
    for member in wire.members {
        members.push(EvaluationT1CoverageMember::new(
            match_member(proposal, &member.member)?,
            coverage_from_wire(member.coverage)?,
        ));
    }
    EvaluationT1CoverageCut::from_source_records(
        request.cut_id(),
        proposal,
        TimestampMillis::new(wire.deadline_ms)
            .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?,
        TimestampMillis::new(wire.closed_at_ms)
            .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?,
        EvaluationSourceSequence::new(wire.source_high_water)
            .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?,
        members,
    )
    .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))
}

fn decode_review_cut(
    proposal: &EvaluationManifestProposal,
    t1_cut: &EvaluationT1CoverageCut,
    wire: source_wire::EvaluationReviewCutWire,
) -> Result<EvaluationReviewCut, ApiError> {
    if wire.t1_cut_id != t1_cut.cut_id().into_uuid() {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    let mut members = Vec::with_capacity(wire.members.len());
    for member in wire.members {
        let declaration = match_member(proposal, &member.member)?;
        let coverage = coverage_from_wire(member.coverage)?;
        let submission = member
            .submission
            .as_ref()
            .map(submission_from_wire)
            .transpose()?;
        let review = review_state_from_wire(member.review)?;
        members.push(EvaluationReviewCutMember::new(
            declaration,
            coverage,
            submission,
            review,
        ));
    }
    EvaluationReviewCut::from_source_records(
        EvaluationReviewCutId::new(wire.review_cut_id)
            .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?,
        t1_cut,
        EvaluationSourceSequence::new(wire.source_high_water)
            .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?,
        members,
    )
    .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))
}

fn match_member(
    proposal: &EvaluationManifestProposal,
    wire: &source_wire::EvaluationManifestMemberWire,
) -> Result<EvaluationManifestMember, ApiError> {
    let expected = proposal
        .members()
        .iter()
        .find(|member| member.assignment_id().into_uuid() == wire.assignment_id)
        .copied()
        .ok_or(ApiError::Runtime(RuntimeError::Conflict))?;
    if member_wire(expected) != *wire {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    Ok(expected)
}

fn coverage_from_wire(
    coverage: source_wire::EvaluationT1CoverageWire,
) -> Result<EvaluationT1Coverage, ApiError> {
    match coverage {
        source_wire::EvaluationT1CoverageWire::Eligible {
            submitted_revision,
            receipt_sequence,
            admitted_at_ms,
            result_digest,
            sources_digest,
        } => Ok(EvaluationT1Coverage::Eligible {
            submitted_revision: AgentAssignmentRevision::new(submitted_revision)
                .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?,
            receipt_sequence: EvaluationSourceSequence::new(receipt_sequence)
                .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?,
            admitted_at: TimestampMillis::new(admitted_at_ms)
                .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?,
            result_digest: parse_digest(&result_digest)?,
            sources_digest: sources_digest.as_deref().map(parse_digest).transpose()?,
        }),
        source_wire::EvaluationT1CoverageWire::NoEligibleSubmission => {
            Ok(EvaluationT1Coverage::NoEligibleSubmission)
        }
        source_wire::EvaluationT1CoverageWire::Unknown { reason } => {
            Ok(EvaluationT1Coverage::Unknown {
                reason: unknown_reason_from_wire(reason),
            })
        }
    }
}

fn submission_from_wire(
    wire: &source_wire::EvaluationSubmissionReceiptWire,
) -> Result<EvaluationSubmissionReceipt, ApiError> {
    EvaluationSubmissionReceipt::from_source_record(
        AgentAssignmentId::from_uuid(wire.assignment_id),
        AgentAssignmentRevision::new(wire.submitted_revision)
            .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?,
        AgentInstanceId::from_uuid(wire.author_agent_id),
        AgentSessionId::from_uuid(wire.author_session_id),
        wire.original_binding_digest
            .as_deref()
            .map(parse_digest)
            .transpose()?,
        wire.criteria_digest
            .as_deref()
            .map(parse_digest)
            .transpose()?,
        EvaluationSourceSequence::new(wire.source_sequence)
            .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?,
        timing_from_wire(&wire.timing)?,
        parse_digest(&wire.result_digest)?,
        wire.sources_digest
            .as_deref()
            .map(parse_digest)
            .transpose()?,
    )
    .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))
}

fn review_state_from_wire(
    review: source_wire::EvaluationReviewStateWire,
) -> Result<EvaluationReviewState, ApiError> {
    match review {
        source_wire::EvaluationReviewStateWire::NotApplicable => {
            Ok(EvaluationReviewState::NotApplicable)
        }
        source_wire::EvaluationReviewStateWire::Pending {
            submitted_revision,
            receipt_sequence,
        } => Ok(EvaluationReviewState::Pending {
            submitted_revision: AgentAssignmentRevision::new(submitted_revision)
                .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?,
            receipt_sequence: EvaluationSourceSequence::new(receipt_sequence)
                .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?,
        }),
        source_wire::EvaluationReviewStateWire::Reviewed { receipt } => {
            let outcome = match receipt.outcome {
                source_wire::EvaluationReviewOutcomeWire::Accepted => {
                    EvaluationReviewOutcome::Accepted
                }
                source_wire::EvaluationReviewOutcomeWire::Rejected => {
                    EvaluationReviewOutcome::Rejected
                }
            };
            let receipt = EvaluationReviewReceipt::from_source_record(
                AgentAssignmentId::from_uuid(receipt.assignment_id),
                AgentAssignmentRevision::new(receipt.submitted_revision)
                    .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?,
                AgentAssignmentRevision::new(receipt.review_revision)
                    .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?,
                EvaluationSourceSequence::new(receipt.source_sequence)
                    .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?,
                outcome,
                AgentInstanceId::from_uuid(receipt.author_agent_id),
                AgentSessionId::from_uuid(receipt.author_session_id),
                AgentInstanceId::from_uuid(receipt.reviewer_agent_id),
                AgentSessionId::from_uuid(receipt.reviewer_session_id),
                parse_digest(&receipt.result_digest)?,
            )
            .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?;
            Ok(EvaluationReviewState::Reviewed(receipt))
        }
        source_wire::EvaluationReviewStateWire::Unknown { reason } => Ok(
            EvaluationReviewState::Unknown(unknown_reason_from_wire(reason)),
        ),
    }
}

fn unknown_reason_from_wire(
    reason: source_wire::EvaluationCoverageUnknownReasonWire,
) -> EvaluationCoverageUnknownReason {
    match reason {
        source_wire::EvaluationCoverageUnknownReasonWire::OriginalBindingMissing => {
            EvaluationCoverageUnknownReason::OriginalBindingMissing
        }
        source_wire::EvaluationCoverageUnknownReasonWire::LegacySubmissionWithoutReceipt => {
            EvaluationCoverageUnknownReason::LegacySubmissionWithoutReceipt
        }
        source_wire::EvaluationCoverageUnknownReasonWire::SubmissionTimingUnknown => {
            EvaluationCoverageUnknownReason::SubmissionTimingUnknown
        }
        source_wire::EvaluationCoverageUnknownReasonWire::ClockHistoryUncertain => {
            EvaluationCoverageUnknownReason::ClockHistoryUncertain
        }
        source_wire::EvaluationCoverageUnknownReasonWire::ReceiptBindingMismatch => {
            EvaluationCoverageUnknownReason::ReceiptBindingMismatch
        }
    }
}

fn member_wire(member: EvaluationManifestMember) -> source_wire::EvaluationManifestMemberWire {
    source_wire::EvaluationManifestMemberWire {
        work_unit_id: member.work_unit_id().into_uuid(),
        obligation_id: member.obligation_id().into_uuid(),
        assignment_id: member.assignment_id().into_uuid(),
        assignment_revision: member.assignment_revision().get(),
        target_agent_id: member.target_agent_id().into_uuid(),
        target_session_id: member.target_session_id().into_uuid(),
        criteria_digest: digest_hex(member.criteria_digest()),
    }
}

fn source_identity_wire(
    source: EvaluationSourceIdentity,
) -> source_wire::EvaluationSourceIdentityWire {
    source_wire::EvaluationSourceIdentityWire {
        backend_id: source.backend_id().into_uuid(),
        workspace_id: source.workspace_id().into_uuid(),
        activity_id: source.activity_id().into_uuid(),
        delegation_migration_id: source.delegation_migration_id(),
        delegation_destination_id: source.delegation_destination_id(),
        delegation_freeze_revision: source.delegation_freeze_revision().get(),
        delegation_manifest_digest: digest_hex(source.delegation_manifest_digest()),
    }
}

fn timing_from_wire(
    timing: &source_wire::EvaluationAdmissionTimingWire,
) -> Result<EvaluationAdmissionTiming, ApiError> {
    match timing {
        source_wire::EvaluationAdmissionTimingWire::Trusted { observed_at_ms } => {
            Ok(EvaluationAdmissionTiming::Trusted(
                TimestampMillis::new(*observed_at_ms)
                    .map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?,
            ))
        }
        source_wire::EvaluationAdmissionTimingWire::Unknown { reason } => {
            let reason = match *reason {
                source_wire::EvaluationClockUncertaintyWire::Unknown => {
                    EvaluationClockUncertainty::Unknown
                }
                source_wire::EvaluationClockUncertaintyWire::Regressed => {
                    EvaluationClockUncertainty::Regressed
                }
            };
            Ok(EvaluationAdmissionTiming::Unknown(reason))
        }
    }
}

fn timing_wire(timing: &EvaluationAdmissionTiming) -> source_wire::EvaluationAdmissionTimingWire {
    match timing {
        EvaluationAdmissionTiming::Trusted(value) => {
            source_wire::EvaluationAdmissionTimingWire::Trusted {
                observed_at_ms: value.get(),
            }
        }
        EvaluationAdmissionTiming::Unknown(reason) => {
            let reason = match reason {
                EvaluationClockUncertainty::Unknown => {
                    source_wire::EvaluationClockUncertaintyWire::Unknown
                }
                EvaluationClockUncertainty::Regressed => {
                    source_wire::EvaluationClockUncertaintyWire::Regressed
                }
            };
            source_wire::EvaluationAdmissionTimingWire::Unknown { reason }
        }
    }
}

fn coverage_wire(coverage: EvaluationT1Coverage) -> source_wire::EvaluationT1CoverageWire {
    match coverage {
        EvaluationT1Coverage::Eligible {
            submitted_revision,
            receipt_sequence,
            admitted_at,
            result_digest,
            sources_digest,
        } => source_wire::EvaluationT1CoverageWire::Eligible {
            submitted_revision: submitted_revision.get(),
            receipt_sequence: receipt_sequence.get(),
            admitted_at_ms: admitted_at.get(),
            result_digest: digest_hex(result_digest),
            sources_digest: sources_digest.map(digest_hex),
        },
        EvaluationT1Coverage::NoEligibleSubmission => {
            source_wire::EvaluationT1CoverageWire::NoEligibleSubmission
        }
        EvaluationT1Coverage::Unknown { reason } => {
            source_wire::EvaluationT1CoverageWire::Unknown {
                reason: unknown_reason_wire(reason),
            }
        }
    }
}

fn submission_wire(
    receipt: EvaluationSubmissionReceipt,
) -> source_wire::EvaluationSubmissionReceiptWire {
    source_wire::EvaluationSubmissionReceiptWire {
        assignment_id: receipt.assignment_id().into_uuid(),
        submitted_revision: receipt.submitted_revision().get(),
        author_agent_id: receipt.author_agent_id().into_uuid(),
        author_session_id: receipt.author_session_id().into_uuid(),
        original_binding_digest: receipt.original_binding_digest().map(digest_hex),
        criteria_digest: receipt.criteria_digest().map(digest_hex),
        source_sequence: receipt.source_sequence().get(),
        timing: timing_wire(&receipt.timing()),
        result_digest: digest_hex(receipt.result_digest()),
        sources_digest: receipt.sources_digest().map(digest_hex),
    }
}

fn review_state_wire(state: EvaluationReviewState) -> source_wire::EvaluationReviewStateWire {
    match state {
        EvaluationReviewState::NotApplicable => {
            source_wire::EvaluationReviewStateWire::NotApplicable
        }
        EvaluationReviewState::Pending {
            submitted_revision,
            receipt_sequence,
        } => source_wire::EvaluationReviewStateWire::Pending {
            submitted_revision: submitted_revision.get(),
            receipt_sequence: receipt_sequence.get(),
        },
        EvaluationReviewState::Reviewed(receipt) => {
            let outcome = match receipt.outcome() {
                EvaluationReviewOutcome::Accepted => {
                    source_wire::EvaluationReviewOutcomeWire::Accepted
                }
                EvaluationReviewOutcome::Rejected => {
                    source_wire::EvaluationReviewOutcomeWire::Rejected
                }
            };
            source_wire::EvaluationReviewStateWire::Reviewed {
                receipt: source_wire::EvaluationReviewReceiptWire {
                    assignment_id: receipt.assignment_id().into_uuid(),
                    submitted_revision: receipt.submitted_revision().get(),
                    review_revision: receipt.review_revision().get(),
                    source_sequence: receipt.source_sequence().get(),
                    outcome,
                    author_agent_id: receipt.author_agent_id().into_uuid(),
                    author_session_id: receipt.author_session_id().into_uuid(),
                    reviewer_agent_id: receipt.reviewer_agent_id().into_uuid(),
                    reviewer_session_id: receipt.reviewer_session_id().into_uuid(),
                    result_digest: digest_hex(receipt.result_digest()),
                },
            }
        }
        EvaluationReviewState::Unknown(reason) => source_wire::EvaluationReviewStateWire::Unknown {
            reason: unknown_reason_wire(reason),
        },
    }
}

fn unknown_reason_wire(
    reason: EvaluationCoverageUnknownReason,
) -> source_wire::EvaluationCoverageUnknownReasonWire {
    match reason {
        EvaluationCoverageUnknownReason::OriginalBindingMissing => {
            source_wire::EvaluationCoverageUnknownReasonWire::OriginalBindingMissing
        }
        EvaluationCoverageUnknownReason::LegacySubmissionWithoutReceipt => {
            source_wire::EvaluationCoverageUnknownReasonWire::LegacySubmissionWithoutReceipt
        }
        EvaluationCoverageUnknownReason::SubmissionTimingUnknown => {
            source_wire::EvaluationCoverageUnknownReasonWire::SubmissionTimingUnknown
        }
        EvaluationCoverageUnknownReason::ClockHistoryUncertain => {
            source_wire::EvaluationCoverageUnknownReasonWire::ClockHistoryUncertain
        }
        EvaluationCoverageUnknownReason::ReceiptBindingMismatch => {
            source_wire::EvaluationCoverageUnknownReasonWire::ReceiptBindingMismatch
        }
    }
}

pub(super) fn digest_hex(digest: EvaluationDigest) -> String {
    let mut output = String::with_capacity(64);
    for byte in digest.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn parse_digest(value: &str) -> Result<EvaluationDigest, ApiError> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    let mut bytes = [0_u8; 32];
    let (chunks, remainder) = value.as_bytes().as_chunks::<2>();
    if !remainder.is_empty() {
        return Err(ApiError::Runtime(RuntimeError::Conflict));
    }
    for (index, chunk) in chunks.iter().enumerate() {
        let pair =
            std::str::from_utf8(chunk).map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?;
        bytes[index] =
            u8::from_str_radix(pair, 16).map_err(|_| ApiError::Runtime(RuntimeError::Conflict))?;
    }
    EvaluationDigest::new(bytes).map_err(|_| ApiError::Runtime(RuntimeError::Conflict))
}
