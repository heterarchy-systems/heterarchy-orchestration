//! Resumable Profile deletion orchestration over the Sessions and Operations
//! authorities. Calls advance one bounded step; provider effects remain with a
//! trusted HTTP consumer and exact Operations dispatch fences.

mod projection;
mod provider;
mod workflow;

pub use provider::{
    AgentProfileProviderDeletionDispatchSummary, AgentProfileProviderDeletionFence,
    AgentProfileProviderDeletionReadbackSummary, AgentProfileProviderDeletionReceiptInput,
    AgentProfileProviderDeletionReceiptSummary,
};

use crate::{OrchestrationRuntime, RuntimeError, agent_profile_summary, profile_error_to_runtime};
use forge_domain::TimestampMillis;
use heterarchy_orchestration_sessions::{
    AgentProfileDeletionCommand, AgentProfileDeletionIntent, AgentProfileId, AgentProfileRevision,
};
use uuid::Uuid;

/// One durable result from an explicit request to advance deletion recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProfileDeletionStepResult {
    Progressed,
    Waiting,
    Deferred,
    Complete,
}

/// User-visible phase derived from durable Sessions and Operations readback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProfileDeletionStage {
    ClosingAdmission,
    WaitingForEffects,
    CheckingHistory,
    ProviderDeletionPending,
    DeletingConversations,
    WaitingForConfirmation,
    RetryScheduled,
    AttentionRequired,
    Finishing,
    Completed,
}

/// Bounded durable failure category suitable for operational readback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProfileDeletionDeferredReason {
    HistoryMismatch,
    CatalogUnavailable,
    ManifestMismatch,
    ProviderBusy,
    AuthenticationRequired,
    SurfaceUnavailable,
    ActionUnavailable,
}

/// Durable summary for one deletion request. The manifest remains owned by
/// Sessions and provider outcomes remain owned by Operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentProfileDeletionProgressSummary {
    pub request_id: Uuid,
    pub expected_profile_revision: i64,
    pub requested_at_ms: i64,
    pub stage: AgentProfileDeletionStage,
    pub pending_effects: u64,
    pub linked_conversation_count: Option<u64>,
    pub deleted_conversation_count: u64,
    pub deferred_reason: Option<AgentProfileDeletionDeferredReason>,
    pub completed_at_ms: Option<i64>,
    pub revision: Option<u64>,
}

/// Exact current Profile and its read-only deletion projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileDeletionSnapshot {
    pub profile: crate::AgentProfileSummary,
    pub deletion: Option<AgentProfileDeletionProgressSummary>,
}

/// Result of one explicit bounded resume call and its subsequent durable read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileDeletionResumeSummary {
    pub step: AgentProfileDeletionStepResult,
    pub snapshot: AgentProfileDeletionSnapshot,
}

impl OrchestrationRuntime {
    /// Close fresh Profile admission and persist an immutable deletion intent.
    /// Retrying the same request identity reads back the original intent.
    /// Provider work is advanced only by [`Self::resume_agent_profile_deletion`].
    ///
    /// # Errors
    /// Returns invalid input, a stale/replayed request conflict, a missing
    /// Profile, owner poison, or a Sessions catalog failure.
    pub fn begin_agent_profile_deletion(
        &self,
        request_id: Uuid,
        profile_id: Uuid,
        expected_profile_revision: i64,
        requested_at_ms: i64,
    ) -> Result<AgentProfileDeletionSnapshot, RuntimeError> {
        let profile_id = AgentProfileId::from_uuid(profile_id).map_err(profile_error_to_runtime)?;
        let expected_revision = AgentProfileRevision::new(expected_profile_revision)
            .map_err(profile_error_to_runtime)?;
        let requested_at = TimestampMillis::new(requested_at_ms)
            .map_err(heterarchy_orchestration_sessions::AgentProfileError::from)
            .map_err(profile_error_to_runtime)?;
        let command = AgentProfileDeletionCommand::new(
            request_id,
            profile_id,
            expected_revision,
            requested_at,
        )
        .map_err(profile_error_to_runtime)?;

        let result = self
            .profile_catalog_guard()?
            .begin_profile_deletion(&command);
        let intent = match result {
            Ok(intent) => intent,
            Err(error) => {
                // Session commits can be uncertain. Read the exact request key
                // before reporting failure or allowing a caller to retry.
                let readback = self
                    .profile_catalog_guard()?
                    .profile_deletion_intent(request_id)
                    .map_err(profile_error_to_runtime)?;
                match readback {
                    Some(intent)
                        if intent.profile_id() == profile_id
                            && intent.expected_profile_revision() == expected_revision =>
                    {
                        intent
                    }
                    Some(_) => return Err(RuntimeError::Conflict),
                    None => return Err(profile_error_to_runtime(error)),
                }
            }
        };
        if intent.profile_id() != profile_id
            || intent.expected_profile_revision() != expected_revision
        {
            return Err(RuntimeError::Conflict);
        }
        self.agent_profile_deletion_for_intent(intent)
    }

    /// Read the current Profile and durable deletion progress without advancing
    /// any workflow state.
    ///
    /// # Errors
    /// Returns invalid identity, owner poison, or catalog/invariant failures.
    pub fn agent_profile_deletion(
        &self,
        profile_id: Uuid,
    ) -> Result<Option<AgentProfileDeletionSnapshot>, RuntimeError> {
        let profile_id = AgentProfileId::from_uuid(profile_id).map_err(profile_error_to_runtime)?;
        let (profile, intent, manifest) = {
            let catalog = self.profile_catalog_guard()?;
            let intent = catalog
                .profile_deletion_intent_for_profile(profile_id)
                .map_err(profile_error_to_runtime)?;
            let Some(profile) = catalog
                .resolve(profile_id)
                .map_err(profile_error_to_runtime)?
            else {
                return Ok(None);
            };
            let manifest = intent
                .map(|intent| catalog.profile_deletion_manifest(intent.request_id()))
                .transpose()
                .map_err(profile_error_to_runtime)?
                .flatten();
            (profile, intent, manifest)
        };
        let deletion = intent
            .map(|intent| projection::deletion_progress(self, intent, manifest, &profile))
            .transpose()?;
        Ok(Some(AgentProfileDeletionSnapshot {
            profile: agent_profile_summary(&profile),
            deletion,
        }))
    }

    /// Resolve a deletion request by its immutable Sessions request identity.
    /// This is read-only and never resumes reconciliation.
    ///
    /// # Errors
    /// Returns invalid identity, owner poison, or catalog/invariant failures.
    pub fn agent_profile_deletion_request(
        &self,
        request_id: Uuid,
    ) -> Result<Option<AgentProfileDeletionSnapshot>, RuntimeError> {
        if request_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        let intent = self
            .profile_catalog_guard()?
            .profile_deletion_intent(request_id)
            .map_err(profile_error_to_runtime)?;
        intent
            .map(|intent| self.agent_profile_deletion_for_intent(intent))
            .transpose()
    }

    /// Advance one persisted reconciliation page or phase boundary, then return
    /// fresh durable readback. Each reference page is at most 32 rows and each
    /// frozen Runtime target page is one row.
    ///
    /// # Errors
    /// Returns invalid identity/time, a missing request, owner poison, or a
    /// Sessions/Operations catalog failure.
    pub fn resume_agent_profile_deletion(
        &self,
        request_id: Uuid,
        observed_at_ms: i64,
    ) -> Result<AgentProfileDeletionResumeSummary, RuntimeError> {
        if request_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        let observed_at =
            TimestampMillis::new(observed_at_ms).map_err(|_| RuntimeError::InvalidInput)?;
        let intent = self
            .profile_catalog_guard()?
            .profile_deletion_intent(request_id)
            .map_err(profile_error_to_runtime)?
            .ok_or(RuntimeError::NotFound)?;
        let step = workflow::reconcile_request(self, intent, observed_at)?;
        let snapshot = self.agent_profile_deletion_for_intent(intent)?;
        Ok(AgentProfileDeletionResumeSummary { step, snapshot })
    }

    fn agent_profile_deletion_for_intent(
        &self,
        intent: AgentProfileDeletionIntent,
    ) -> Result<AgentProfileDeletionSnapshot, RuntimeError> {
        self.agent_profile_deletion(intent.profile_id().into_uuid())?
            .ok_or(RuntimeError::NotFound)
    }
}

#[cfg(test)]
mod tests;
