//! One-step, bounded reconciliation of an immutable Sessions deletion intent
//! against the Operations barrier, reference history, and provider journal.

use super::AgentProfileDeletionStepResult;
use crate::{OrchestrationRuntime, RuntimeError};
use forge_domain::TimestampMillis;
use heterarchy_orchestration_operations::{
    AgentProfileDeletionProgress, AgentProfileDeletionProgressAdvance,
    AgentProfileDeletionProgressDeferredReason, AgentProfileDeletionProgressPhase,
    AgentProfileProviderDeletionOperation, AgentProvisionProviderFamily, AgentProvisionState,
    MAX_AGENT_PROFILE_DELETION_PAGE_SIZE, OperationsError,
};
use heterarchy_orchestration_sessions::{
    AgentProfile, AgentProfileDeletionIntent, AgentProfileDeletionManifest,
    AgentProfileDeletionRuntimeEntry, AgentProfileDeletionRuntimeTarget, AgentProfileId,
    AgentProfileLifecycleState, AgentProfileProviderFamily, AgentProfileRuntimeSession,
    AgentProfileRuntimeSessionId,
};
use uuid::Uuid;

const REFERENCES_PER_STEP: u32 = MAX_AGENT_PROFILE_DELETION_PAGE_SIZE;
const RUNTIMES_PER_STEP: u32 = 1;

pub(super) fn reconcile_request(
    runtime: &OrchestrationRuntime,
    intent: AgentProfileDeletionIntent,
    observed_at: TimestampMillis,
) -> Result<AgentProfileDeletionStepResult, RuntimeError> {
    if ensure_barrier(runtime, intent)? {
        return Ok(AgentProfileDeletionStepResult::Progressed);
    }
    let Some(progress) = read_or_begin_progress(runtime, intent)? else {
        return Ok(AgentProfileDeletionStepResult::Progressed);
    };
    if !progress_matches_intent(&progress, intent) {
        return defer(
            runtime,
            &progress,
            AgentProfileDeletionProgressDeferredReason::ManifestMismatch,
            observed_at,
        );
    }
    match progress.phase() {
        AgentProfileDeletionProgressPhase::AuditProvision => {
            audit_provision_page(runtime, intent, &progress, observed_at)
        }
        AgentProfileDeletionProgressPhase::AuditMessages => {
            audit_message_page(runtime, intent, &progress, observed_at)
        }
        AgentProfileDeletionProgressPhase::FreezeTargets => {
            freeze_or_record_manifest(runtime, intent, &progress, observed_at)
        }
        AgentProfileDeletionProgressPhase::PrepareDeletes => {
            prepare_runtime_page(runtime, intent, &progress, observed_at)
        }
        AgentProfileDeletionProgressPhase::DeleteTargets => {
            settle_delete_targets(runtime, intent, &progress, observed_at)
        }
        AgentProfileDeletionProgressPhase::Finalize => {
            finalize_profile(runtime, intent, &progress, observed_at)
        }
        AgentProfileDeletionProgressPhase::Completed => {
            Ok(AgentProfileDeletionStepResult::Complete)
        }
    }
}

fn ensure_barrier(
    runtime: &OrchestrationRuntime,
    intent: AgentProfileDeletionIntent,
) -> Result<bool, RuntimeError> {
    let existing = runtime
        .catalog()?
        .profile_deletion_barrier_by_request(intent.request_id())?;
    match existing {
        Some(barrier) if barrier_matches_intent(barrier, intent) => Ok(false),
        Some(_) => Err(inconsistent("Profile deletion barrier replay")),
        None => {
            let created = runtime.catalog()?.begin_profile_deletion_barrier(&intent);
            match created {
                Ok(barrier) if barrier_matches_intent(barrier, intent) => Ok(true),
                Ok(_) => Err(inconsistent("Profile deletion barrier readback")),
                Err(error) => {
                    let readback = runtime
                        .catalog()?
                        .profile_deletion_barrier_by_request(intent.request_id())?;
                    match readback {
                        Some(barrier) if barrier_matches_intent(barrier, intent) => Ok(true),
                        Some(_) => Err(inconsistent("Profile deletion barrier uncertain replay")),
                        None => Err(error.into()),
                    }
                }
            }
        }
    }
}

fn read_or_begin_progress(
    runtime: &OrchestrationRuntime,
    intent: AgentProfileDeletionIntent,
) -> Result<Option<AgentProfileDeletionProgress>, RuntimeError> {
    if let Some(progress) = read_progress(runtime, intent.request_id())? {
        return Ok(Some(progress));
    }
    let created = runtime
        .catalog()?
        .begin_agent_profile_deletion_progress(&intent);
    match created {
        Ok(_) => Ok(None),
        Err(error) => {
            let readback = read_progress(runtime, intent.request_id())?;
            if readback
                .as_ref()
                .is_some_and(|progress| progress_matches_intent(progress, intent))
            {
                // Do not execute the first phase in the same call that
                // recovered an uncertain progress-row creation.
                Ok(None)
            } else if readback.is_some() {
                Err(inconsistent("Profile deletion progress uncertain replay"))
            } else {
                Err(error.into())
            }
        }
    }
}

fn read_progress(
    runtime: &OrchestrationRuntime,
    request_id: Uuid,
) -> Result<Option<AgentProfileDeletionProgress>, RuntimeError> {
    Ok(runtime
        .catalog()?
        .agent_profile_deletion_progress(request_id)?)
}

fn audit_provision_page(
    runtime: &OrchestrationRuntime,
    intent: AgentProfileDeletionIntent,
    progress: &AgentProfileDeletionProgress,
    observed_at: TimestampMillis,
) -> Result<AgentProfileDeletionStepResult, RuntimeError> {
    let page = runtime
        .catalog()?
        .profile_deletion_provision_references_page(
            intent.profile_id(),
            progress.after_provision_operation_id(),
            REFERENCES_PER_STEP,
        )?;
    for reference in page.references() {
        if !provision_reference_matches_sessions(runtime, intent.profile_id(), reference)? {
            return defer(
                runtime,
                progress,
                AgentProfileDeletionProgressDeferredReason::HistoryMismatch,
                observed_at,
            );
        }
    }
    advance(
        runtime,
        progress,
        AgentProfileDeletionProgressAdvance::ProvisionReferencesPage {
            after_operation_id: page.next_operation_id(),
        },
        observed_at,
    )
}

fn provision_reference_matches_sessions(
    runtime: &OrchestrationRuntime,
    profile_id: AgentProfileId,
    reference: &heterarchy_orchestration_operations::AgentProfileDeletionProvisionReference,
) -> Result<bool, RuntimeError> {
    if reference.profile_id() != profile_id
        || reference.provider_family() != AgentProvisionProviderFamily::ChatGpt
    {
        return Ok(false);
    }
    let Some(runtime_id) = reference.runtime_session_id() else {
        return Ok(reference.provider_conversation_id().is_none()
            && matches!(
                reference.state(),
                AgentProvisionState::Prepared | AgentProvisionState::Cancelled
            ));
    };
    let Some(runtime_session) = read_runtime_session(runtime, runtime_id)? else {
        return Ok(false);
    };
    if runtime_session.profile_id() != profile_id
        || runtime_session.provider_family() != AgentProfileProviderFamily::ChatGpt
    {
        return Ok(false);
    }
    match reference.provider_conversation_id() {
        Some(conversation_id) => {
            Ok(runtime_session.provider_conversation_id() == Some(conversation_id))
        }
        None => Ok(!matches!(
            reference.state(),
            AgentProvisionState::Observed | AgentProvisionState::Confirmed
        )),
    }
}

fn audit_message_page(
    runtime: &OrchestrationRuntime,
    intent: AgentProfileDeletionIntent,
    progress: &AgentProfileDeletionProgress,
    observed_at: TimestampMillis,
) -> Result<AgentProfileDeletionStepResult, RuntimeError> {
    let page = runtime
        .catalog()?
        .profile_deletion_provider_message_references_page(
            intent.profile_id(),
            progress.after_message_id(),
            REFERENCES_PER_STEP,
        )?;
    for reference in page.references() {
        if !provider_message_reference_matches_sessions(
            runtime,
            intent.profile_id(),
            reference.target(),
        )? {
            return defer(
                runtime,
                progress,
                AgentProfileDeletionProgressDeferredReason::HistoryMismatch,
                observed_at,
            );
        }
    }
    advance(
        runtime,
        progress,
        AgentProfileDeletionProgressAdvance::ProviderMessageReferencesPage {
            after_message_id: page.next_message_id(),
        },
        observed_at,
    )
}

fn provider_message_reference_matches_sessions(
    runtime: &OrchestrationRuntime,
    profile_id: AgentProfileId,
    target: &heterarchy_orchestration_operations::ProviderMessageTarget,
) -> Result<bool, RuntimeError> {
    if target.profile_id() != profile_id
        || target.provider_family() != AgentProfileProviderFamily::ChatGpt
    {
        return Ok(false);
    }
    let Some(runtime_session) = read_runtime_session(runtime, target.runtime_session_id())? else {
        return Ok(false);
    };
    Ok(runtime_session.profile_id() == profile_id
        && runtime_session.provider_family() == target.provider_family()
        && runtime_session.provider_conversation_id() == Some(target.provider_conversation_id()))
}

fn read_runtime_session(
    runtime: &OrchestrationRuntime,
    runtime_id: AgentProfileRuntimeSessionId,
) -> Result<Option<AgentProfileRuntimeSession>, RuntimeError> {
    Ok(runtime
        .profile_catalog_guard()?
        .runtime_session(runtime_id)?)
}

fn freeze_or_record_manifest(
    runtime: &OrchestrationRuntime,
    intent: AgentProfileDeletionIntent,
    progress: &AgentProfileDeletionProgress,
    observed_at: TimestampMillis,
) -> Result<AgentProfileDeletionStepResult, RuntimeError> {
    let profile = read_profile(runtime, intent.profile_id())?.ok_or(RuntimeError::NotFound)?;
    if profile.lifecycle_state() == AgentProfileLifecycleState::Active
        || profile.revision() < intent.barrier_profile_revision()
        || progress.freeze().is_some()
    {
        return defer(
            runtime,
            progress,
            AgentProfileDeletionProgressDeferredReason::ManifestMismatch,
            observed_at,
        );
    }
    if !operations_quiescent(runtime, intent)? {
        return Ok(AgentProfileDeletionStepResult::Waiting);
    }
    let manifest = read_manifest(runtime, intent.request_id())?;
    if let Some(manifest) = manifest {
        if !manifest_matches_intent(&manifest, intent) || manifest.completed_at().is_some() {
            return defer(
                runtime,
                progress,
                AgentProfileDeletionProgressDeferredReason::ManifestMismatch,
                observed_at,
            );
        }
        return advance(
            runtime,
            progress,
            AgentProfileDeletionProgressAdvance::TargetsFrozen(manifest),
            observed_at,
        );
    }

    let frozen = runtime
        .profile_catalog_guard()?
        .freeze_profile_deletion_targets(intent.request_id(), profile.revision(), observed_at);
    match frozen {
        Ok(manifest) if manifest_matches_intent(&manifest, intent) => {
            Ok(AgentProfileDeletionStepResult::Progressed)
        }
        Ok(_) => defer(
            runtime,
            progress,
            AgentProfileDeletionProgressDeferredReason::ManifestMismatch,
            observed_at,
        ),
        Err(error) => match read_manifest(runtime, intent.request_id())? {
            Some(manifest) if manifest_matches_intent(&manifest, intent) => {
                Ok(AgentProfileDeletionStepResult::Progressed)
            }
            Some(_) => defer(
                runtime,
                progress,
                AgentProfileDeletionProgressDeferredReason::ManifestMismatch,
                observed_at,
            ),
            None => {
                let _ = error;
                defer(
                    runtime,
                    progress,
                    AgentProfileDeletionProgressDeferredReason::CatalogUnavailable,
                    observed_at,
                )
            }
        },
    }
}

fn prepare_runtime_page(
    runtime: &OrchestrationRuntime,
    intent: AgentProfileDeletionIntent,
    progress: &AgentProfileDeletionProgress,
    observed_at: TimestampMillis,
) -> Result<AgentProfileDeletionStepResult, RuntimeError> {
    let Some(manifest) = read_manifest(runtime, intent.request_id())? else {
        return defer(
            runtime,
            progress,
            AgentProfileDeletionProgressDeferredReason::ManifestMismatch,
            observed_at,
        );
    };
    if !manifest_matches_progress(&manifest, progress, false) {
        return defer(
            runtime,
            progress,
            AgentProfileDeletionProgressDeferredReason::ManifestMismatch,
            observed_at,
        );
    }
    if !operations_quiescent(runtime, intent)? {
        return Ok(AgentProfileDeletionStepResult::Waiting);
    }
    let page = runtime
        .profile_catalog_guard()?
        .profile_deletion_runtime_entries_page(
            intent.request_id(),
            progress.after_runtime_id(),
            RUNTIMES_PER_STEP,
        )?;
    let Some(entry) = page.entries().first() else {
        if page.next_runtime_id().is_some() {
            return defer(
                runtime,
                progress,
                AgentProfileDeletionProgressDeferredReason::ManifestMismatch,
                observed_at,
            );
        }
        // The keyset cursor proves every returned entry has been processed;
        // the owner audit proves the terminal page did not hide a missing or
        // changed frozen row before Operations accepts the phase transition.
        validate_manifest_entries(runtime, intent.request_id())?;
        return advance(
            runtime,
            progress,
            AgentProfileDeletionProgressAdvance::RuntimeEntriesPage {
                after_runtime_id: None,
            },
            observed_at,
        );
    };
    if entry.request_id() != intent.request_id() || entry.profile_id() != intent.profile_id() {
        return defer(
            runtime,
            progress,
            AgentProfileDeletionProgressDeferredReason::ManifestMismatch,
            observed_at,
        );
    }
    let Some(current_runtime) = read_runtime_session(runtime, entry.runtime_session_id())? else {
        return defer(
            runtime,
            progress,
            AgentProfileDeletionProgressDeferredReason::HistoryMismatch,
            observed_at,
        );
    };
    if current_runtime.profile_id() != entry.profile_id()
        || current_runtime.provider_family() != entry.provider_family()
        || current_runtime.revision() != entry.original_revision()
        || current_runtime.state() != entry.original_state()
        || !manifest_target_matches_runtime(entry, &current_runtime)
    {
        return defer(
            runtime,
            progress,
            AgentProfileDeletionProgressDeferredReason::HistoryMismatch,
            observed_at,
        );
    }
    prepare_runtime_entry(
        runtime,
        intent,
        progress,
        manifest,
        entry,
        page.next_runtime_id(),
        observed_at,
    )
}

fn prepare_runtime_entry(
    runtime: &OrchestrationRuntime,
    intent: AgentProfileDeletionIntent,
    progress: &AgentProfileDeletionProgress,
    manifest: AgentProfileDeletionManifest,
    entry: &AgentProfileDeletionRuntimeEntry,
    next_runtime_id: Option<AgentProfileRuntimeSessionId>,
    observed_at: TimestampMillis,
) -> Result<AgentProfileDeletionStepResult, RuntimeError> {
    match entry.target() {
        AgentProfileDeletionRuntimeTarget::LinkedProviderConversation { .. } => {
            prepare_linked_runtime(
                runtime,
                progress,
                manifest,
                entry,
                next_runtime_id,
                observed_at,
            )
        }
        AgentProfileDeletionRuntimeTarget::NoLinkedTarget => {
            if !no_linked_target_has_operations_proof(runtime, intent, entry)? {
                return defer(
                    runtime,
                    progress,
                    AgentProfileDeletionProgressDeferredReason::HistoryMismatch,
                    observed_at,
                );
            }
            if read_provider_deletion_target(
                runtime,
                manifest.request_id(),
                progress.after_runtime_id(),
                entry.runtime_session_id(),
            )?
            .is_some()
            {
                return defer(
                    runtime,
                    progress,
                    AgentProfileDeletionProgressDeferredReason::ManifestMismatch,
                    observed_at,
                );
            }
            advance(
                runtime,
                progress,
                AgentProfileDeletionProgressAdvance::RuntimeEntriesPage {
                    after_runtime_id: next_runtime_id,
                },
                observed_at,
            )
        }
    }
}

fn prepare_linked_runtime(
    runtime: &OrchestrationRuntime,
    progress: &AgentProfileDeletionProgress,
    manifest: AgentProfileDeletionManifest,
    entry: &AgentProfileDeletionRuntimeEntry,
    next_runtime_id: Option<AgentProfileRuntimeSessionId>,
    observed_at: TimestampMillis,
) -> Result<AgentProfileDeletionStepResult, RuntimeError> {
    match read_provider_deletion_target(
        runtime,
        manifest.request_id(),
        progress.after_runtime_id(),
        entry.runtime_session_id(),
    )? {
        Some(operation) if provider_deletion_matches_entry(&operation, manifest, entry) => advance(
            runtime,
            progress,
            AgentProfileDeletionProgressAdvance::RuntimeEntriesPage {
                after_runtime_id: next_runtime_id,
            },
            observed_at,
        ),
        Some(_) => defer(
            runtime,
            progress,
            AgentProfileDeletionProgressDeferredReason::ManifestMismatch,
            observed_at,
        ),
        None => {
            let prepared = runtime
                .catalog()?
                .prepare_agent_profile_provider_deletion_target(manifest, entry, observed_at);
            match prepared {
                Ok(Some(_)) => Ok(AgentProfileDeletionStepResult::Progressed),
                Ok(None) | Err(OperationsError::Conflict(_)) => defer(
                    runtime,
                    progress,
                    AgentProfileDeletionProgressDeferredReason::ManifestMismatch,
                    observed_at,
                ),
                Err(error) => match read_provider_deletion_target(
                    runtime,
                    manifest.request_id(),
                    progress.after_runtime_id(),
                    entry.runtime_session_id(),
                )? {
                    Some(operation)
                        if provider_deletion_matches_entry(&operation, manifest, entry) =>
                    {
                        Ok(AgentProfileDeletionStepResult::Progressed)
                    }
                    Some(_) => defer(
                        runtime,
                        progress,
                        AgentProfileDeletionProgressDeferredReason::ManifestMismatch,
                        observed_at,
                    ),
                    None => Err(error.into()),
                },
            }
        }
    }
}

fn manifest_target_matches_runtime(
    entry: &AgentProfileDeletionRuntimeEntry,
    runtime: &AgentProfileRuntimeSession,
) -> bool {
    match entry.target() {
        AgentProfileDeletionRuntimeTarget::LinkedProviderConversation { conversation_id } => {
            runtime.provider_conversation_id() == Some(conversation_id.as_str())
        }
        AgentProfileDeletionRuntimeTarget::NoLinkedTarget => {
            runtime.provider_conversation_id().is_none()
        }
    }
}

fn read_provider_deletion_target(
    runtime: &OrchestrationRuntime,
    request_id: Uuid,
    after_runtime_id: Option<AgentProfileRuntimeSessionId>,
    target_runtime_id: AgentProfileRuntimeSessionId,
) -> Result<Option<AgentProfileProviderDeletionOperation>, RuntimeError> {
    let page = runtime.catalog()?.agent_profile_provider_deletions_page(
        request_id,
        after_runtime_id,
        1,
    )?;
    let Some(operation) = page.operations().first() else {
        return Ok(None);
    };
    match operation.runtime_session_id().cmp(&target_runtime_id) {
        std::cmp::Ordering::Less => Err(inconsistent("provider deletion cursor order")),
        std::cmp::Ordering::Greater => Ok(None),
        std::cmp::Ordering::Equal => Ok(Some(operation.clone())),
    }
}

fn provider_deletion_matches_entry(
    operation: &AgentProfileProviderDeletionOperation,
    manifest: AgentProfileDeletionManifest,
    entry: &AgentProfileDeletionRuntimeEntry,
) -> bool {
    let AgentProfileDeletionRuntimeTarget::LinkedProviderConversation { conversation_id } =
        entry.target()
    else {
        return false;
    };
    operation.request_id() == manifest.request_id()
        && operation.profile_id() == manifest.profile_id()
        && operation.runtime_session_id() == entry.runtime_session_id()
        && operation.provider_family() == entry.provider_family()
        && operation.provider_conversation_id() == conversation_id.as_str()
        && operation.original_runtime_revision() == entry.original_revision()
        && operation.original_runtime_state() == entry.original_state()
}

fn no_linked_target_has_operations_proof(
    runtime: &OrchestrationRuntime,
    intent: AgentProfileDeletionIntent,
    entry: &AgentProfileDeletionRuntimeEntry,
) -> Result<bool, RuntimeError> {
    let summary = runtime
        .catalog()?
        .profile_deletion_runtime_reference_summary(&intent, entry.runtime_session_id())?;
    Ok(summary.request_id() == intent.request_id()
        && summary.profile_id() == entry.profile_id()
        && summary.runtime_session_id() == entry.runtime_session_id()
        && summary.provision_reference_count() > 0
        && summary.suppressed_or_failed_unlinked_provision_count()
            == summary.provision_reference_count()
        && !summary.has_other_provision_reference()
        && summary.provider_message_reference_count() == 0)
}

fn settle_delete_targets(
    runtime: &OrchestrationRuntime,
    intent: AgentProfileDeletionIntent,
    progress: &AgentProfileDeletionProgress,
    observed_at: TimestampMillis,
) -> Result<AgentProfileDeletionStepResult, RuntimeError> {
    validate_manifest_entries(runtime, intent.request_id())?;
    let Some(manifest) = read_manifest(runtime, intent.request_id())? else {
        return defer(
            runtime,
            progress,
            AgentProfileDeletionProgressDeferredReason::ManifestMismatch,
            observed_at,
        );
    };
    if !manifest_matches_progress(&manifest, progress, false) {
        return defer(
            runtime,
            progress,
            AgentProfileDeletionProgressDeferredReason::ManifestMismatch,
            observed_at,
        );
    }
    let summary = runtime
        .catalog()?
        .agent_profile_provider_deletion_summary(intent.request_id())?;
    if summary.profile_id() != intent.profile_id()
        || summary.deleted() != manifest.linked_target_count()
        || summary.prepared() != 0
        || summary.effect_pending() != 0
        || summary.effect_unknown() != 0
        || summary.deferred() != 0
    {
        return Ok(AgentProfileDeletionStepResult::Waiting);
    }
    if !provider_receipts_complete(runtime, intent.request_id())? {
        return Ok(AgentProfileDeletionStepResult::Waiting);
    }
    advance(
        runtime,
        progress,
        AgentProfileDeletionProgressAdvance::ProviderTargetsSettled,
        observed_at,
    )
}

fn finalize_profile(
    runtime: &OrchestrationRuntime,
    intent: AgentProfileDeletionIntent,
    progress: &AgentProfileDeletionProgress,
    observed_at: TimestampMillis,
) -> Result<AgentProfileDeletionStepResult, RuntimeError> {
    validate_manifest_entries(runtime, intent.request_id())?;
    let Some(manifest) = read_manifest(runtime, intent.request_id())? else {
        return defer(
            runtime,
            progress,
            AgentProfileDeletionProgressDeferredReason::ManifestMismatch,
            observed_at,
        );
    };
    if !manifest_matches_progress(&manifest, progress, true) {
        return defer(
            runtime,
            progress,
            AgentProfileDeletionProgressDeferredReason::ManifestMismatch,
            observed_at,
        );
    }
    if !delete_summary_is_complete(runtime, intent.profile_id(), manifest)? {
        return Ok(AgentProfileDeletionStepResult::Waiting);
    }
    if manifest.completed_at().is_none() {
        let finished = runtime
            .profile_catalog_guard()?
            .finish_profile_deletion(intent.request_id(), observed_at);
        return match finished {
            Ok(finished)
                if manifest_matches_intent(&finished, intent)
                    && manifest_matches_progress(&finished, progress, true)
                    && finished.completed_at().is_some() =>
            {
                Ok(AgentProfileDeletionStepResult::Progressed)
            }
            Ok(_) => defer(
                runtime,
                progress,
                AgentProfileDeletionProgressDeferredReason::ManifestMismatch,
                observed_at,
            ),
            Err(error) => match read_manifest(runtime, intent.request_id())? {
                Some(finished)
                    if manifest_matches_intent(&finished, intent)
                        && manifest_matches_progress(&finished, progress, true)
                        && finished.completed_at().is_some() =>
                {
                    Ok(AgentProfileDeletionStepResult::Progressed)
                }
                Some(_) => defer(
                    runtime,
                    progress,
                    AgentProfileDeletionProgressDeferredReason::ManifestMismatch,
                    observed_at,
                ),
                None => {
                    let _ = error;
                    defer(
                        runtime,
                        progress,
                        AgentProfileDeletionProgressDeferredReason::CatalogUnavailable,
                        observed_at,
                    )
                }
            },
        };
    }
    advance(
        runtime,
        progress,
        AgentProfileDeletionProgressAdvance::ProfileFinished(manifest),
        observed_at,
    )
}

fn delete_summary_is_complete(
    runtime: &OrchestrationRuntime,
    profile_id: AgentProfileId,
    manifest: AgentProfileDeletionManifest,
) -> Result<bool, RuntimeError> {
    let summary = runtime
        .catalog()?
        .agent_profile_provider_deletion_summary(manifest.request_id())?;
    Ok(summary.profile_id() == profile_id
        && summary.deleted() == manifest.linked_target_count()
        && summary.prepared() == 0
        && summary.effect_pending() == 0
        && summary.effect_unknown() == 0
        && summary.deferred() == 0
        && provider_receipts_complete(runtime, manifest.request_id())?)
}

fn validate_manifest_entries(
    runtime: &OrchestrationRuntime,
    request_id: Uuid,
) -> Result<(), RuntimeError> {
    runtime
        .profile_catalog_guard()?
        .validate_profile_deletion_manifest_entries(request_id)
        .map_err(Into::into)
}

pub(super) fn provider_receipts_complete(
    runtime: &OrchestrationRuntime,
    request_id: Uuid,
) -> Result<bool, RuntimeError> {
    let operations = runtime.catalog()?;
    let profiles = runtime.profile_catalog_guard()?;
    Ok(operations
        .profile_deletion_provider_receipts_complete_with_manifest(request_id, &profiles)?)
}

fn read_profile(
    runtime: &OrchestrationRuntime,
    profile_id: AgentProfileId,
) -> Result<Option<AgentProfile>, RuntimeError> {
    Ok(runtime.profile_catalog_guard()?.resolve(profile_id)?)
}

fn read_manifest(
    runtime: &OrchestrationRuntime,
    request_id: Uuid,
) -> Result<Option<AgentProfileDeletionManifest>, RuntimeError> {
    Ok(runtime
        .profile_catalog_guard()?
        .profile_deletion_manifest(request_id)?)
}

fn operations_quiescent(
    runtime: &OrchestrationRuntime,
    intent: AgentProfileDeletionIntent,
) -> Result<bool, RuntimeError> {
    let counts = runtime.catalog()?.profile_deletion_quiescence(&intent)?;
    Ok(counts.pending_provision_operations() == 0
        && counts.unknown_provision_operations() == 0
        && counts.observed_provision_operations() == 0
        && counts.pending_provider_messages() == 0
        && counts.unknown_provider_messages() == 0)
}

fn manifest_matches_intent(
    manifest: &AgentProfileDeletionManifest,
    intent: AgentProfileDeletionIntent,
) -> bool {
    manifest
        .linked_target_count()
        .checked_add(manifest.no_linked_target_count())
        == Some(manifest.runtime_entry_count())
        && manifest.request_id() == intent.request_id()
        && manifest.profile_id() == intent.profile_id()
        && manifest.profile_revision_at_freeze() >= intent.barrier_profile_revision()
        && manifest.frozen_at() >= intent.requested_at()
}

fn manifest_matches_progress(
    manifest: &AgentProfileDeletionManifest,
    progress: &AgentProfileDeletionProgress,
    allow_session_finished_ahead: bool,
) -> bool {
    let Some(freeze) = progress.freeze() else {
        return false;
    };
    let completion_matches = match (freeze.completed_at(), manifest.completed_at()) {
        (None, None) => true,
        (None, Some(completed_at)) => {
            allow_session_finished_ahead && completed_at >= freeze.frozen_at()
        }
        (Some(expected), Some(actual)) => expected == actual,
        (Some(_), None) => false,
    };
    manifest.request_id() == progress.request_id()
        && manifest.profile_id() == progress.profile_id()
        && manifest.profile_revision_at_freeze() == freeze.profile_revision_at_freeze()
        && manifest.frozen_at() == freeze.frozen_at()
        && manifest.runtime_entry_count() == freeze.runtime_entry_count()
        && manifest.linked_target_count() == freeze.linked_target_count()
        && manifest.no_linked_target_count() == freeze.no_linked_target_count()
        && completion_matches
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

fn advance(
    runtime: &OrchestrationRuntime,
    progress: &AgentProfileDeletionProgress,
    advance: AgentProfileDeletionProgressAdvance,
    observed_at: TimestampMillis,
) -> Result<AgentProfileDeletionStepResult, RuntimeError> {
    let result = runtime.catalog()?.advance_agent_profile_deletion_progress(
        progress.request_id(),
        progress.revision(),
        observed_at,
        advance,
    );
    match result {
        Ok(progress) => Ok(
            if progress.phase() == AgentProfileDeletionProgressPhase::Completed {
                AgentProfileDeletionStepResult::Complete
            } else {
                AgentProfileDeletionStepResult::Progressed
            },
        ),
        Err(error) => {
            let current = read_progress(runtime, progress.request_id())?;
            if current.as_ref().is_some_and(|current| {
                current.revision() != progress.revision() || current.phase() != progress.phase()
            }) || observed_at < progress.updated_at()
            {
                return Ok(AgentProfileDeletionStepResult::Waiting);
            }
            if matches!(&error, OperationsError::Conflict(_)) {
                return defer(
                    runtime,
                    progress,
                    AgentProfileDeletionProgressDeferredReason::ManifestMismatch,
                    observed_at,
                );
            }
            Err(error.into())
        }
    }
}

fn defer(
    runtime: &OrchestrationRuntime,
    progress: &AgentProfileDeletionProgress,
    reason: AgentProfileDeletionProgressDeferredReason,
    observed_at: TimestampMillis,
) -> Result<AgentProfileDeletionStepResult, RuntimeError> {
    match runtime.catalog()?.defer_agent_profile_deletion_progress(
        progress.request_id(),
        progress.revision(),
        reason,
        observed_at,
    ) {
        Ok(_) | Err(OperationsError::Conflict(_)) => Ok(AgentProfileDeletionStepResult::Deferred),
        Err(error) => Err(error.into()),
    }
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

fn inconsistent(label: &'static str) -> RuntimeError {
    RuntimeError::persistence(OperationsError::Corrupt(label))
}
