//! Transport-neutral `WorkCredit` / score observation operations.
//!
//! Read-only by design: credit awards happen inside reviewed execution
//! flows, election snapshots freeze inside election starts, and nothing here
//! mutates the ledger. Forge evidence is observed, never imported as Forge
//! authority. These reads back the scheduling/fairness observation surface
//! (plan O10) and the GUI timeline.

use forge_domain::WorkspaceId;
use heterarchy_orchestration_operations::{WorkCreditSubjectIdentity, WorkCreditSubjectKind};
use uuid::Uuid;

use crate::{OrchestrationRuntime, RuntimeError};

/// One awarded credit entry (bounded, newest first).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkCreditEntrySummary {
    /// Credit identity.
    pub credit_id: Uuid,
    /// Workspace scope.
    pub workspace_id: Uuid,
    /// Assignment the credit was reserved for.
    pub assignment_id: Uuid,
    /// Attributed participant, when one exists.
    pub participant_id: Option<Uuid>,
    /// Executing Agent.
    pub agent_id: Uuid,
    /// Activity scope.
    pub activity_id: Uuid,
    /// Goal scope.
    pub goal_id: Uuid,
    /// Node scope.
    pub node_id: Uuid,
    /// Frozen credit budget value (1/3/5).
    pub budget: i64,
    /// Award source journal sequence.
    pub award_source_sequence: u64,
    /// Executed evidence identity.
    pub evidence_execution_id: Uuid,
    /// Award timestamp.
    pub awarded_at_ms: i64,
}

/// One kind-4 submission with its exact kind-1 artifact provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitProvenanceSummary {
    /// Assignment identity.
    pub assignment_id: Uuid,
    /// Activity scope.
    pub activity_id: Uuid,
    /// Goal scope.
    pub goal_id: Uuid,
    /// Node scope.
    pub node_id: Uuid,
    /// Submitted-for Agent.
    pub target_agent_id: Uuid,
    /// Resolved artifact provenance; `None` fails closed (unattributed,
    /// failed `ChangeSet`, or non-matching scope — never an inferred
    /// producer).
    pub provenance: Option<ProvenanceMatchSummary>,
}

/// The exactly-attributed artifact evidence of one submission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvenanceMatchSummary {
    /// Applied `ChangeSet` identity.
    pub change_set_id: Uuid,
    /// Diff identity.
    pub diff_id: Uuid,
    /// Producer Agent.
    pub producer_agent_id: Uuid,
    /// Producer session, when recorded.
    pub producer_session_id: Option<Uuid>,
    /// Provenance dimension label.
    pub dimension: &'static str,
}

/// One 30-day score snapshot for a Workspace or Team scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkCreditScoreSnapshotSummary {
    /// Workspace scope.
    pub workspace_id: Uuid,
    /// Team scope, when team-scoped.
    pub team_id: Option<Uuid>,
    /// Observation window length.
    pub window_ms: i64,
    /// Per-subject scores.
    pub per_subject: Vec<SubjectScoreSummary>,
    /// Total credits in the window.
    pub total_credits: i64,
    /// Arithmetic mean per subject (floor).
    pub subject_arithmetic_mean: i64,
    /// Mean remainder numerator for exact division downstream.
    pub subject_mean_remainder_numerator: i64,
    /// Subjects counted in the mean.
    pub mean_subject_count: i64,
    /// Credits held by mean-counted subjects.
    pub subject_total_credits: i64,
    /// Unattributed credits.
    pub unattributed_count: i64,
    /// Fulfilled assignments in the window.
    pub fulfillment_completed: i64,
    /// Evaluated assignments in the window.
    pub fulfillment_evaluated: i64,
    /// Fulfillment counts grouped by the same logical subjects as the score.
    pub fulfillment_per_subject: Vec<SubjectFulfillmentSummary>,
    /// Kind-4 submissions observed in the source projection.
    pub provenance_submitted: i64,
    /// Submissions matched to exact, attributed artifact evidence.
    pub provenance_matched: i64,
}

/// One subject's score inside a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectScoreSummary {
    /// Full logical subject identity from the Operations owner.
    pub subject: WorkCreditSubjectIdentitySummary,
    /// Source participant count behind this score.
    pub source_participant_count: u64,
    /// Subject total credits.
    pub total_credits: i64,
    /// Credit entry count.
    pub entry_count: i64,
}

/// Full identity preserved from an Operations `WorkCredit` subject.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkCreditSubjectIdentitySummary {
    /// Closed subject kind owned by Operations.
    pub kind: WorkCreditSubjectKind,
    /// Subject identity.
    pub subject_id: Uuid,
    /// Participation identity for workspace-participation subjects.
    pub participation_id: Option<Uuid>,
    /// Linked Profile, when the subject has one.
    pub profile_id: Option<Uuid>,
    /// Legacy participant identity, when the subject is historical.
    pub legacy_participant_id: Option<Uuid>,
    /// Exact participation revision, when the subject is a participation.
    pub participation_revision: Option<u64>,
}

/// One subject's fulfillment projection inside a score snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectFulfillmentSummary {
    /// Full logical subject identity from the Operations owner.
    pub subject: WorkCreditSubjectIdentitySummary,
    /// Source participant count behind this aggregate.
    pub source_participant_count: u64,
    /// Distinct credited assignments completed.
    pub completed: i64,
    /// Distinct terminally evaluated assignments.
    pub evaluated: i64,
}

/// Frozen election-electorate credit snapshot receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectionSnapshotSummary {
    /// Election identity.
    pub election_id: Uuid,
    /// Workspace scope.
    pub workspace_id: Uuid,
    /// Team scope, when team-scoped.
    pub team_id: Option<Uuid>,
    /// Candidate participant.
    pub candidate_participant_id: Uuid,
    /// Candidate subject identity label parts.
    pub subject_kind: &'static str,
    pub subject_id: Uuid,
    /// Frozen source participant count.
    pub source_participant_count: u64,
    /// Frozen window length.
    pub window_ms: i64,
    /// Frozen evaluation timestamp.
    pub evaluated_at_ms: i64,
    /// Candidate total credits.
    pub candidate_total_credits: i64,
    /// Workspace total credits.
    pub workspace_total_credits: i64,
    /// Frozen whole-credit part of the Workspace subject mean.
    pub workspace_arithmetic_mean: i64,
    /// Frozen fractional-mean numerator.
    pub mean_remainder_numerator: i64,
    /// Frozen subject count used as the mean denominator.
    pub mean_subject_count: i64,
    /// Qualification already decided by the durable election owner.
    pub qualified: bool,
    /// Digest already checked while reading the durable snapshot.
    pub snapshot_digest: [u8; 32],
}

impl OrchestrationRuntime {
    /// Read the bounded, newest-first credit ledger of one Workspace.
    ///
    /// # Errors
    /// Returns invalid input or durable catalog failures.
    pub fn work_credit_entries(
        &self,
        workspace_id: Uuid,
    ) -> Result<Vec<WorkCreditEntrySummary>, RuntimeError> {
        let catalog = self.catalog()?;
        let rows = catalog.work_credit_entries(require_workspace(workspace_id)?)?;
        Ok(rows
            .iter()
            .map(|entry| WorkCreditEntrySummary {
                credit_id: entry.credit_id(),
                workspace_id: entry.workspace_id().into_uuid(),
                assignment_id: entry.assignment_id().into_uuid(),
                participant_id: entry.participant_id(),
                agent_id: entry.agent_id().into_uuid(),
                activity_id: entry.activity_id().into_uuid(),
                goal_id: entry.goal_id().into_uuid(),
                node_id: entry.node_id().into_uuid(),
                budget: entry.budget().value(),
                award_source_sequence: entry.award_source_sequence(),
                evidence_execution_id: entry.evidence_execution_id().into_uuid(),
                awarded_at_ms: entry.awarded_at_ms(),
            })
            .collect())
    }

    /// Read fulfilled versus evaluated assignment counts for one Workspace.
    ///
    /// # Errors
    /// Returns invalid input or durable catalog failures.
    pub fn work_credit_fulfillment(&self, workspace_id: Uuid) -> Result<(u64, u64), RuntimeError> {
        let catalog = self.catalog()?;
        let (completed, evaluated) =
            catalog.work_credit_fulfillment(require_workspace(workspace_id)?)?;
        Ok((completed, evaluated))
    }

    /// Read bounded kind-4 submission provenance rows for one Workspace.
    ///
    /// # Errors
    /// Returns invalid input or durable catalog failures.
    pub fn work_credit_submit_provenance(
        &self,
        workspace_id: Uuid,
    ) -> Result<Vec<SubmitProvenanceSummary>, RuntimeError> {
        let catalog = self.catalog()?;
        let rows = catalog.work_credit_submit_provenance(require_workspace(workspace_id)?)?;
        Ok(rows
            .iter()
            .map(|row| SubmitProvenanceSummary {
                assignment_id: row.assignment_id().into_uuid(),
                activity_id: row.activity_id().into_uuid(),
                goal_id: row.goal_id().into_uuid(),
                node_id: row.node_id().into_uuid(),
                target_agent_id: row.target_agent_id().into_uuid(),
                provenance: row.provenance().map(|matched| ProvenanceMatchSummary {
                    change_set_id: matched.change_set_id().into_uuid(),
                    diff_id: matched.diff_id().into_uuid(),
                    producer_agent_id: matched.producer_agent_id().into_uuid(),
                    producer_session_id: matched
                        .producer_session_id()
                        .map(forge_domain::AgentSessionId::into_uuid),
                    dimension: provenance_dimension_label(matched.dimension()),
                }),
            })
            .collect())
    }

    /// Read the 30-day Workspace credit score snapshot.
    ///
    /// # Errors
    /// Returns invalid input or durable catalog failures.
    pub fn work_credit_scores(
        &self,
        workspace_id: Uuid,
        now_ms: i64,
    ) -> Result<WorkCreditScoreSnapshotSummary, RuntimeError> {
        let catalog = self.catalog()?;
        let snapshot = catalog.work_credit_scores(require_workspace(workspace_id)?, now_ms)?;
        score_snapshot_summary(&snapshot)
    }

    /// Read the bounded full Workspace-scoped credit score snapshot.
    ///
    /// # Errors
    /// Returns invalid input, durable catalog failures, or a source-budget
    /// resynchronization request before full subject projection.
    pub fn work_credit_scores_bounded(
        &self,
        workspace_id: Uuid,
        now_ms: i64,
    ) -> Result<WorkCreditScoreSnapshotSummary, RuntimeError> {
        let catalog = self.catalog()?;
        let snapshot =
            catalog.work_credit_scores_bounded(require_workspace(workspace_id)?, now_ms)?;
        score_snapshot_summary(&snapshot)
    }

    /// Read the 30-day Team-scoped credit score snapshot.
    ///
    /// # Errors
    /// Returns invalid input or durable catalog failures.
    pub fn team_work_credit_scores(
        &self,
        workspace_id: Uuid,
        team_id: Uuid,
        now_ms: i64,
    ) -> Result<WorkCreditScoreSnapshotSummary, RuntimeError> {
        if team_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        let catalog = self.catalog()?;
        let snapshot =
            catalog.team_work_credit_scores(require_workspace(workspace_id)?, team_id, now_ms)?;
        score_snapshot_summary(&snapshot)
    }

    /// Read the frozen credit snapshot an election was qualified against.
    ///
    /// # Errors
    /// Returns invalid input or durable catalog failures.
    pub fn election_work_credit_snapshot(
        &self,
        election_id: Uuid,
    ) -> Result<Option<ElectionSnapshotSummary>, RuntimeError> {
        if election_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        let catalog = self.catalog()?;
        let receipt = catalog.election_work_credit_snapshot(election_id)?;
        Ok(receipt.map(|receipt| ElectionSnapshotSummary {
            election_id: receipt.election_id(),
            workspace_id: receipt.workspace_id().into_uuid(),
            team_id: receipt.team_id(),
            candidate_participant_id: receipt.candidate_participant_id(),
            subject_kind: subject_kind_label(receipt.subject().kind()),
            subject_id: receipt.subject().subject_id(),
            source_participant_count: receipt.source_participant_count(),
            window_ms: receipt.window_ms(),
            evaluated_at_ms: receipt.evaluated_at_ms(),
            candidate_total_credits: receipt.candidate_total_credits(),
            workspace_total_credits: receipt.workspace_total_credits(),
            workspace_arithmetic_mean: receipt.workspace_arithmetic_mean(),
            mean_remainder_numerator: receipt.mean_remainder_numerator(),
            mean_subject_count: receipt.mean_subject_count(),
            qualified: receipt.qualified(),
            snapshot_digest: receipt.snapshot_digest(),
        }))
    }
}

fn require_workspace(value: Uuid) -> Result<WorkspaceId, RuntimeError> {
    if value.is_nil() {
        return Err(RuntimeError::InvalidInput);
    }
    Ok(WorkspaceId::from_uuid(value))
}

fn score_snapshot_summary(
    snapshot: &heterarchy_orchestration_operations::WorkCreditScoreSnapshot,
) -> Result<WorkCreditScoreSnapshotSummary, RuntimeError> {
    let per_subject = snapshot
        .per_subject()
        .iter()
        .map(|score| {
            Ok(SubjectScoreSummary {
                subject: subject_identity_summary(score.subject())?,
                source_participant_count: score.source_participant_count(),
                total_credits: score.total_credits(),
                entry_count: score.entry_count(),
            })
        })
        .collect::<Result<Vec<_>, RuntimeError>>()?;
    let fulfillment_per_subject = snapshot
        .fulfillment_per_subject()
        .iter()
        .map(|fulfillment| {
            Ok(SubjectFulfillmentSummary {
                subject: subject_identity_summary(fulfillment.subject())?,
                source_participant_count: fulfillment.source_participant_count(),
                completed: fulfillment.completed(),
                evaluated: fulfillment.evaluated(),
            })
        })
        .collect::<Result<Vec<_>, RuntimeError>>()?;
    Ok(WorkCreditScoreSnapshotSummary {
        workspace_id: snapshot.workspace_id().into_uuid(),
        team_id: snapshot.team_id(),
        window_ms: snapshot.window_ms(),
        per_subject,
        total_credits: snapshot.total_credits(),
        subject_arithmetic_mean: snapshot.subject_arithmetic_mean(),
        subject_mean_remainder_numerator: snapshot.subject_mean_remainder_numerator(),
        mean_subject_count: snapshot.mean_subject_count(),
        subject_total_credits: snapshot.subject_total_credits(),
        unattributed_count: snapshot.unattributed_count(),
        fulfillment_completed: snapshot.fulfillment_completed(),
        fulfillment_evaluated: snapshot.fulfillment_evaluated(),
        fulfillment_per_subject,
        provenance_submitted: snapshot.provenance_submitted(),
        provenance_matched: snapshot.provenance_matched(),
    })
}

fn subject_identity_summary(
    subject: WorkCreditSubjectIdentity,
) -> Result<WorkCreditSubjectIdentitySummary, RuntimeError> {
    Ok(WorkCreditSubjectIdentitySummary {
        kind: subject.kind(),
        subject_id: subject.subject_id(),
        participation_id: subject
            .participation_id()?
            .map(heterarchy_orchestration_operations::WorkspaceParticipationId::into_uuid),
        profile_id: subject
            .profile_id()
            .map(heterarchy_orchestration_sessions::AgentProfileId::into_uuid),
        legacy_participant_id: subject.legacy_participant_id(),
        participation_revision: subject.participation_revision(),
    })
}

/// Deterministic subject-kind labels shared by every transport projection.
#[must_use]
pub const fn subject_kind_label(kind: WorkCreditSubjectKind) -> &'static str {
    match kind {
        WorkCreditSubjectKind::WorkspaceParticipation => "workspace_participation",
        WorkCreditSubjectKind::LegacyParticipant => "legacy_participant",
    }
}

/// Deterministic provenance-dimension label.
#[must_use]
pub const fn provenance_dimension_label(
    dimension: heterarchy_orchestration_operations::ProvenanceDimension,
) -> &'static str {
    match dimension {
        heterarchy_orchestration_operations::ProvenanceDimension::ExactSubmittedSourceReference => {
            "exact_submitted_source_reference"
        }
    }
}
