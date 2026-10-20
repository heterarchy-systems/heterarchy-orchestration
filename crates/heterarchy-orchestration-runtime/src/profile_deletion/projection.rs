//! Read-only projection of Sessions intent, frozen manifest, and Operations
//! progress/receipts into one bounded Runtime summary.

use super::{
    AgentProfileDeletionDeferredReason, AgentProfileDeletionProgressSummary,
    AgentProfileDeletionStage,
};
use crate::{OrchestrationRuntime, RuntimeError};
use heterarchy_orchestration_operations::{
    AgentProfileDeletionFreeze, AgentProfileDeletionInFlightCounts, AgentProfileDeletionProgress,
    AgentProfileDeletionProgressDeferredReason, AgentProfileDeletionProgressPhase,
    AgentProfileProviderDeletionDeferredReason, AgentProfileProviderDeletionSummary,
    OperationsError,
};
use heterarchy_orchestration_sessions::{
    AgentProfile, AgentProfileDeletionIntent, AgentProfileDeletionManifest,
    AgentProfileLifecycleState,
};

pub(super) fn deletion_progress(
    runtime: &OrchestrationRuntime,
    intent: AgentProfileDeletionIntent,
    manifest: Option<AgentProfileDeletionManifest>,
    profile: &AgentProfile,
) -> Result<AgentProfileDeletionProgressSummary, RuntimeError> {
    if profile.lifecycle_state() == AgentProfileLifecycleState::Active
        || profile.revision() < intent.barrier_profile_revision()
    {
        return Err(profile_corrupt("Profile deletion admission barrier"));
    }

    let operations = runtime.catalog()?;
    let barrier = operations.profile_deletion_barrier(intent.profile_id())?;
    if barrier.is_some_and(|barrier| !barrier_matches_intent(barrier, intent)) {
        return Err(inconsistent("Profile deletion Operations barrier"));
    }
    let counts = match barrier {
        Some(_) => operations.profile_deletion_quiescence(&intent)?,
        None => operations.profile_deletion_in_flight_counts(intent.profile_id())?,
    };
    let pending_effects = pending_count(counts)?;
    let Some(progress) = operations.agent_profile_deletion_progress(intent.request_id())? else {
        if barrier.is_some() && manifest.is_some() {
            return Err(inconsistent(
                "Profile deletion manifest before progress freeze",
            ));
        }
        if barrier.is_none() && manifest.is_some() {
            return Err(inconsistent(
                "Profile deletion manifest before Operations barrier",
            ));
        }
        return Ok(AgentProfileDeletionProgressSummary {
            request_id: intent.request_id(),
            expected_profile_revision: intent.expected_profile_revision().get(),
            requested_at_ms: intent.requested_at().get(),
            stage: if barrier.is_none() {
                AgentProfileDeletionStage::ClosingAdmission
            } else if pending_effects > 0 {
                AgentProfileDeletionStage::WaitingForEffects
            } else {
                AgentProfileDeletionStage::CheckingHistory
            },
            pending_effects,
            linked_conversation_count: None,
            deleted_conversation_count: 0,
            deferred_reason: None,
            completed_at_ms: None,
            revision: None,
        });
    };
    if barrier.is_none() || !progress_matches_intent(&progress, intent) {
        return Err(inconsistent("Profile deletion progress scope"));
    }
    let provider_summary =
        operations.agent_profile_provider_deletion_summary(intent.request_id())?;
    if provider_summary.request_id() != intent.request_id()
        || provider_summary.profile_id() != intent.profile_id()
    {
        return Err(inconsistent("Profile deletion provider summary scope"));
    }

    let linked_count = validate_frozen_progress(&progress, provider_summary, manifest)?;
    if progress.phase() == AgentProfileDeletionProgressPhase::Completed {
        drop(operations);
        if !super::workflow::provider_receipts_complete(runtime, intent.request_id())? {
            return Err(inconsistent(
                "completed Profile deletion provider confirmation receipts",
            ));
        }
        return completed_progress(
            &progress,
            provider_summary,
            manifest,
            profile,
            pending_effects,
            linked_count,
        );
    }
    let deferred_reason = progress.deferred_reason().map(progress_reason).or_else(|| {
        provider_summary
            .latest_deferred_reason()
            .map(provider_reason)
    });
    let stage = current_stage(&progress, provider_summary, pending_effects);
    Ok(AgentProfileDeletionProgressSummary {
        request_id: intent.request_id(),
        expected_profile_revision: intent.expected_profile_revision().get(),
        requested_at_ms: intent.requested_at().get(),
        stage,
        pending_effects,
        linked_conversation_count: linked_count,
        deleted_conversation_count: provider_summary.deleted(),
        deferred_reason,
        completed_at_ms: None,
        revision: Some(progress.revision()),
    })
}

fn completed_progress(
    progress: &AgentProfileDeletionProgress,
    summary: AgentProfileProviderDeletionSummary,
    manifest: Option<AgentProfileDeletionManifest>,
    profile: &AgentProfile,
    pending_effects: u64,
    linked_count: Option<u64>,
) -> Result<AgentProfileDeletionProgressSummary, RuntimeError> {
    let manifest = manifest.ok_or_else(|| inconsistent("completed Profile deletion manifest"))?;
    let freeze = progress
        .freeze()
        .ok_or_else(|| inconsistent("completed Profile deletion freeze"))?;
    let completed_at = manifest
        .completed_at()
        .ok_or_else(|| inconsistent("completed Profile deletion timestamp"))?;
    if profile.lifecycle_state() != AgentProfileLifecycleState::Retired
        || freeze.completed_at() != Some(completed_at)
        || pending_effects != 0
        || summary.deleted() != freeze.linked_target_count()
        || target_count(summary)? != summary.deleted()
        || progress.deferred_reason().is_some()
        || summary.latest_deferred_reason().is_some()
    {
        return Err(inconsistent("completed Profile deletion evidence"));
    }
    Ok(AgentProfileDeletionProgressSummary {
        request_id: progress.request_id(),
        expected_profile_revision: progress.expected_profile_revision().get(),
        requested_at_ms: progress.requested_at().get(),
        stage: AgentProfileDeletionStage::Completed,
        pending_effects,
        linked_conversation_count: linked_count,
        deleted_conversation_count: summary.deleted(),
        deferred_reason: None,
        completed_at_ms: Some(completed_at.get()),
        revision: Some(progress.revision()),
    })
}

fn validate_frozen_progress(
    progress: &AgentProfileDeletionProgress,
    summary: AgentProfileProviderDeletionSummary,
    manifest: Option<AgentProfileDeletionManifest>,
) -> Result<Option<u64>, RuntimeError> {
    let Some(freeze) = progress.freeze() else {
        if target_count(summary)? != 0 {
            return Err(inconsistent("Profile deletion targets before freeze"));
        }
        if let Some(manifest) = manifest {
            // Sessions can commit before the next bounded Operations step.
            // Read this exact saga gap without authorizing provider effects.
            if progress.phase() != AgentProfileDeletionProgressPhase::FreezeTargets
                || manifest.request_id() != progress.request_id()
                || manifest.profile_id() != progress.profile_id()
                || manifest.profile_revision_at_freeze() < progress.barrier_profile_revision()
                || manifest.frozen_at() < progress.requested_at()
                || manifest.completed_at().is_some()
            {
                return Err(inconsistent(
                    "Profile deletion manifest before freeze checkpoint",
                ));
            }
            return Ok(Some(manifest.linked_target_count()));
        }
        return Ok(None);
    };
    let manifest = manifest.ok_or_else(|| inconsistent("Profile deletion manifest missing"))?;
    if manifest.request_id() != progress.request_id()
        || manifest.profile_id() != progress.profile_id()
        || !freeze_matches(freeze, manifest)
        || target_count(summary)? > freeze.linked_target_count()
    {
        return Err(inconsistent("Profile deletion frozen inventory"));
    }
    Ok(Some(freeze.linked_target_count()))
}

fn current_stage(
    progress: &AgentProfileDeletionProgress,
    summary: AgentProfileProviderDeletionSummary,
    pending_effects: u64,
) -> AgentProfileDeletionStage {
    if pending_effects > 0 {
        return AgentProfileDeletionStage::WaitingForEffects;
    }
    if progress.deferred_reason().is_some() {
        return AgentProfileDeletionStage::AttentionRequired;
    }
    match progress.phase() {
        AgentProfileDeletionProgressPhase::AuditProvision
        | AgentProfileDeletionProgressPhase::AuditMessages
        | AgentProfileDeletionProgressPhase::FreezeTargets => {
            AgentProfileDeletionStage::CheckingHistory
        }
        AgentProfileDeletionProgressPhase::PrepareDeletes => {
            AgentProfileDeletionStage::ProviderDeletionPending
        }
        AgentProfileDeletionProgressPhase::DeleteTargets => {
            if summary.effect_unknown() > 0 {
                AgentProfileDeletionStage::WaitingForConfirmation
            } else if summary.effect_pending() == 0 && summary.deferred() > 0 {
                AgentProfileDeletionStage::RetryScheduled
            } else {
                AgentProfileDeletionStage::DeletingConversations
            }
        }
        AgentProfileDeletionProgressPhase::Finalize => AgentProfileDeletionStage::Finishing,
        AgentProfileDeletionProgressPhase::Completed => AgentProfileDeletionStage::Completed,
    }
}

fn pending_count(counts: AgentProfileDeletionInFlightCounts) -> Result<u64, RuntimeError> {
    [
        counts.pending_provision_operations(),
        counts.unknown_provision_operations(),
        counts.observed_provision_operations(),
        counts.pending_provider_messages(),
        counts.unknown_provider_messages(),
    ]
    .into_iter()
    .try_fold(0_u64, u64::checked_add)
    .ok_or_else(|| inconsistent("Profile deletion pending-effect count"))
}

fn target_count(summary: AgentProfileProviderDeletionSummary) -> Result<u64, RuntimeError> {
    [
        summary.prepared(),
        summary.effect_pending(),
        summary.effect_unknown(),
        summary.deferred(),
        summary.deleted(),
    ]
    .into_iter()
    .try_fold(0_u64, u64::checked_add)
    .ok_or_else(|| inconsistent("Profile deletion provider-target count"))
}

fn freeze_matches(
    freeze: AgentProfileDeletionFreeze,
    manifest: AgentProfileDeletionManifest,
) -> bool {
    freeze.profile_revision_at_freeze() == manifest.profile_revision_at_freeze()
        && freeze.frozen_at() == manifest.frozen_at()
        && freeze.runtime_entry_count() == manifest.runtime_entry_count()
        && freeze.linked_target_count() == manifest.linked_target_count()
        && freeze.no_linked_target_count() == manifest.no_linked_target_count()
}

fn progress_matches_intent(
    progress: &AgentProfileDeletionProgress,
    intent: AgentProfileDeletionIntent,
) -> bool {
    progress.request_id() == intent.request_id()
        && progress.profile_id() == intent.profile_id()
        && progress.expected_profile_revision() == intent.expected_profile_revision()
        && progress.barrier_profile_revision() == intent.barrier_profile_revision()
        && progress.requested_at() == intent.requested_at()
}

fn barrier_matches_intent(
    barrier: heterarchy_orchestration_operations::AgentProfileDeletionBarrier,
    intent: AgentProfileDeletionIntent,
) -> bool {
    barrier.request_id() == intent.request_id()
        && barrier.profile_id() == intent.profile_id()
        && barrier.expected_profile_revision() == intent.expected_profile_revision()
        && barrier.barrier_profile_revision() == intent.barrier_profile_revision()
        && barrier.requested_at() == intent.requested_at()
}

fn progress_reason(
    reason: AgentProfileDeletionProgressDeferredReason,
) -> AgentProfileDeletionDeferredReason {
    match reason {
        AgentProfileDeletionProgressDeferredReason::HistoryMismatch => {
            AgentProfileDeletionDeferredReason::HistoryMismatch
        }
        AgentProfileDeletionProgressDeferredReason::CatalogUnavailable => {
            AgentProfileDeletionDeferredReason::CatalogUnavailable
        }
        AgentProfileDeletionProgressDeferredReason::ManifestMismatch => {
            AgentProfileDeletionDeferredReason::ManifestMismatch
        }
    }
}

fn provider_reason(
    reason: AgentProfileProviderDeletionDeferredReason,
) -> AgentProfileDeletionDeferredReason {
    match reason {
        AgentProfileProviderDeletionDeferredReason::ProviderBusy => {
            AgentProfileDeletionDeferredReason::ProviderBusy
        }
        AgentProfileProviderDeletionDeferredReason::AuthenticationRequired => {
            AgentProfileDeletionDeferredReason::AuthenticationRequired
        }
        AgentProfileProviderDeletionDeferredReason::SurfaceUnavailable => {
            AgentProfileDeletionDeferredReason::SurfaceUnavailable
        }
        AgentProfileProviderDeletionDeferredReason::ActionUnavailable => {
            AgentProfileDeletionDeferredReason::ActionUnavailable
        }
    }
}

fn profile_corrupt(label: &'static str) -> RuntimeError {
    RuntimeError::ProfileCatalog(
        heterarchy_orchestration_sessions::AgentProfileError::Corrupt(label),
    )
}

fn inconsistent(label: &'static str) -> RuntimeError {
    RuntimeError::persistence(OperationsError::Corrupt(label))
}
