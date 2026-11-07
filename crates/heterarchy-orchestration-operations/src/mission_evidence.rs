//! Durable observations of registered Forge evaluation evidence.
//!
//! The module binds one started local Mission plan to a complete set of exact
//! registered Forge manifests partitioned by Activity, then appends matching
//! source-cut observations with historical actor provenance. It stores evidence
//! measures only; it does not decide personal nonperformance, award
//! `WorkCredit`, or publish election scores.

use forge_domain::WorkspaceId;

use crate::{MissionEvaluationPlanId, OperationsCatalog, OperationsError};

mod codec;
mod digest;
mod evaluate;
mod fingerprint;
mod historical_identity;
mod history;
mod model;
mod preparation;
mod schema;
mod store;
#[cfg(test)]
mod tests;

pub use digest::{MissionEvidenceMemberCriteriaDigest, MissionEvidencePlanDigests};
pub use model::{
    MissionEvidenceActorKind, MissionEvidenceActorReceipt, MissionEvidenceBindingState,
    MissionEvidenceBoundMember, MissionEvidenceDisposition, MissionEvidenceIdentityPendingReason,
    MissionEvidenceIdentityProof, MissionEvidenceIdentityResolution,
    MissionEvidenceManifestBindingCommand, MissionEvidenceManifestBindingOutcome,
    MissionEvidenceManifestBindingRecord, MissionEvidenceManifestBundle,
    MissionEvidenceMutationRecord, MissionEvidenceObservationBundle,
    MissionEvidenceObservationCommand, MissionEvidenceObservationMember,
    MissionEvidenceObservationOutcome, MissionEvidenceObservationPartition,
    MissionEvidenceObservationRecord, MissionEvidenceParticipationMeasures,
    MissionEvidenceRegistrationKind, MissionEvidenceUnresolvedReason,
};
pub use preparation::{
    MissionEvidenceManifestCandidateMember, MissionEvidenceManifestCandidatePartition,
    MissionEvidenceManifestPreparation, MissionEvidenceManifestPreparationCommand,
    MissionEvidenceManifestPreparationStatus, MissionEvidencePreparationPendingTarget,
};
pub(crate) use schema::{create_schema, schema_objects};

impl OperationsCatalog {
    /// Derive canonical local plan and criteria digests for one exact started plan.
    ///
    /// Runtime uses these values when preparing the Forge registration read.
    /// The source manifest must echo the plan digest and each member's
    /// criterion digest; those source values are checked again when bound.
    ///
    /// # Errors
    /// Rejects an invalid or non-started plan, malformed local obligations, and
    /// storage failures.
    pub fn mission_evidence_plan_digests(
        &self,
        workspace_id: WorkspaceId,
        plan_id: MissionEvaluationPlanId,
        revision: u64,
    ) -> Result<MissionEvidencePlanDigests, OperationsError> {
        if workspace_id.into_uuid().is_nil() {
            return Err(OperationsError::Conflict(
                "mission evidence Workspace identity",
            ));
        }
        let tx = self.connection.unchecked_transaction()?;
        let frozen = crate::mission_evaluation::frozen_plan_in_transaction(
            &tx,
            workspace_id,
            plan_id,
            revision,
        )?;
        let digests = digest::derive(&frozen.version, &frozen.obligations)?;
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(digests)
    }
}
