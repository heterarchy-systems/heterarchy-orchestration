//! Exact weighted evaluation of a frozen obligation cohort.
//!
//! The caller owns Mission/plan identity and authoritative evidence selection.
//! This calculation does not authenticate evidence, award `WorkCredit`, classify
//! personal responsibility, or grant a leadership role.

use std::{collections::BTreeSet, fmt};

/// Bounded calculation input; this is not a minimum-work eligibility rule.
pub const MAX_PERFORMANCE_MILESTONES: usize = 4_096;

/// Evidence already attributed to the exact original obligation by its owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MilestoneEvidence {
    /// Independent acceptance of the exact submitted version.
    Accepted { submitted_at_ms: u64 },
    /// The owning decision has established noncompletion for this evaluation.
    ConfirmedUnfulfilled,
    /// Review, source completeness, or responsibility is still unresolved.
    Pending,
}

/// One immutable, positive-weight obligation; retries reuse its identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PerformanceMilestone {
    obligation_id: u128,
    weight: u64,
    due_at_ms: u64,
    evidence: MilestoneEvidence,
}

impl PerformanceMilestone {
    /// Establish an exact obligation and weight before calculation.
    ///
    /// # Errors
    /// Rejects nil identity and zero weight. Scope and source authority remain
    /// the responsibility of the caller's frozen-plan/evidence boundary.
    pub fn new(
        obligation_id: u128,
        weight: u64,
        due_at_ms: u64,
        evidence: MilestoneEvidence,
    ) -> Result<Self, PerformanceError> {
        if obligation_id == 0 {
            return Err(PerformanceError::NilObligation);
        }
        if weight == 0 {
            return Err(PerformanceError::ZeroWeight);
        }
        Ok(Self {
            obligation_id,
            weight,
            due_at_ms,
            evidence,
        })
    }
}

/// Exact integer totals, never a rounded or floating-point percentage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PerformanceTotals {
    accepted_weight: u64,
    obligation_weight: u64,
}

impl PerformanceTotals {
    /// Independently accepted weight for the selected submission cutoff.
    #[must_use]
    pub const fn accepted_weight(self) -> u64 {
        self.accepted_weight
    }

    /// All due obligation weight, including resolved noncompletion.
    #[must_use]
    pub const fn obligation_weight(self) -> u64 {
        self.obligation_weight
    }
}

/// A pending review cannot be represented as a final zero-percent result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PerformanceEvaluation {
    /// No obligation is due in the frozen evaluation cohort.
    NotEvaluable,
    /// The totals are observations only; no final rate or winner is established.
    Pending {
        totals: PerformanceTotals,
        pending_weight: u64,
    },
    /// Every due obligation has an explicit resolved outcome.
    Final(PerformanceTotals),
}

/// Invalid cohort/evidence inputs are rejected rather than silently normalized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PerformanceError {
    NilObligation,
    ZeroWeight,
    TooManyMilestones,
    DuplicateObligation,
    SubmissionAfterCutoff,
    WeightOverflow,
}

impl fmt::Display for PerformanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NilObligation => "nil performance obligation",
            Self::ZeroWeight => "zero performance weight",
            Self::TooManyMilestones => "performance cohort exceeds the bounded input limit",
            Self::DuplicateObligation => "duplicate performance obligation",
            Self::SubmissionAfterCutoff => "accepted version was submitted after the cutoff",
            Self::WeightOverflow => "performance weight total overflow",
        })
    }
}

impl std::error::Error for PerformanceError {}

/// Evaluate due obligations at an explicit, frozen submission cutoff.
///
/// Future-due work is excluded even when completed early. An accepted version
/// submitted at the cutoff is included; later work must be evaluated in a later
/// cohort. Time passing alone never changes `Pending` to a failed obligation.
///
/// # Errors
/// Rejects oversized/duplicate cohorts, post-cutoff accepted versions for due
/// obligations, and overflowing totals. No partial result is returned on error.
pub fn evaluate_performance(
    submission_cutoff_ms: u64,
    milestones: &[PerformanceMilestone],
) -> Result<PerformanceEvaluation, PerformanceError> {
    if milestones.len() > MAX_PERFORMANCE_MILESTONES {
        return Err(PerformanceError::TooManyMilestones);
    }
    let mut identities = BTreeSet::new();
    let mut accepted_weight = 0_u64;
    let mut obligation_weight = 0_u64;
    let mut pending_weight = 0_u64;
    for milestone in milestones {
        if !identities.insert(milestone.obligation_id) {
            return Err(PerformanceError::DuplicateObligation);
        }
        if milestone.due_at_ms > submission_cutoff_ms {
            continue;
        }
        obligation_weight = obligation_weight
            .checked_add(milestone.weight)
            .ok_or(PerformanceError::WeightOverflow)?;
        match milestone.evidence {
            MilestoneEvidence::Accepted { submitted_at_ms } => {
                if submitted_at_ms > submission_cutoff_ms {
                    return Err(PerformanceError::SubmissionAfterCutoff);
                }
                accepted_weight = accepted_weight
                    .checked_add(milestone.weight)
                    .ok_or(PerformanceError::WeightOverflow)?;
            }
            MilestoneEvidence::ConfirmedUnfulfilled => {}
            MilestoneEvidence::Pending => {
                pending_weight = pending_weight
                    .checked_add(milestone.weight)
                    .ok_or(PerformanceError::WeightOverflow)?;
            }
        }
    }
    let totals = PerformanceTotals {
        accepted_weight,
        obligation_weight,
    };
    Ok(if obligation_weight == 0 {
        PerformanceEvaluation::NotEvaluable
    } else if pending_weight > 0 {
        PerformanceEvaluation::Pending {
            totals,
            pending_weight,
        }
    } else {
        PerformanceEvaluation::Final(totals)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(
        id: u128,
        weight: u64,
        due: u64,
        evidence: MilestoneEvidence,
    ) -> Result<PerformanceMilestone, PerformanceError> {
        PerformanceMilestone::new(id, weight, due, evidence)
    }

    #[test]
    fn weighted_rate_is_not_assignment_count() -> Result<(), PerformanceError> {
        let result = evaluate_performance(
            20,
            &[
                item(
                    1,
                    4,
                    20,
                    MilestoneEvidence::Accepted {
                        submitted_at_ms: 19,
                    },
                )?,
                item(2, 6, 20, MilestoneEvidence::ConfirmedUnfulfilled)?,
            ],
        )?;
        assert_eq!(
            result,
            PerformanceEvaluation::Final(PerformanceTotals {
                accepted_weight: 4,
                obligation_weight: 10
            })
        );
        Ok(())
    }

    #[test]
    fn pending_is_not_failure_and_future_work_is_not_current_denominator()
    -> Result<(), PerformanceError> {
        let result = evaluate_performance(
            20,
            &[
                item(
                    1,
                    4,
                    20,
                    MilestoneEvidence::Accepted {
                        submitted_at_ms: 20,
                    },
                )?,
                item(2, 6, 20, MilestoneEvidence::Pending)?,
                item(
                    3,
                    99,
                    21,
                    MilestoneEvidence::Accepted {
                        submitted_at_ms: 18,
                    },
                )?,
            ],
        )?;
        assert_eq!(
            result,
            PerformanceEvaluation::Pending {
                totals: PerformanceTotals {
                    accepted_weight: 4,
                    obligation_weight: 10
                },
                pending_weight: 6
            }
        );
        Ok(())
    }

    #[test]
    fn no_due_work_is_not_evaluable() -> Result<(), PerformanceError> {
        assert_eq!(
            evaluate_performance(20, &[])?,
            PerformanceEvaluation::NotEvaluable
        );
        assert_eq!(
            evaluate_performance(20, &[item(1, 1, 21, MilestoneEvidence::Pending)?])?,
            PerformanceEvaluation::NotEvaluable
        );
        Ok(())
    }

    #[test]
    fn retry_identity_and_late_version_cannot_inflate_a_snapshot() -> Result<(), PerformanceError> {
        let accepted = item(
            1,
            4,
            20,
            MilestoneEvidence::Accepted {
                submitted_at_ms: 20,
            },
        )?;
        assert_eq!(
            evaluate_performance(20, &[accepted, accepted]),
            Err(PerformanceError::DuplicateObligation)
        );
        assert_eq!(
            evaluate_performance(
                20,
                &[item(
                    2,
                    4,
                    20,
                    MilestoneEvidence::Accepted {
                        submitted_at_ms: 21
                    }
                )?]
            ),
            Err(PerformanceError::SubmissionAfterCutoff)
        );
        Ok(())
    }

    #[test]
    fn invalid_identity_weight_size_and_total_are_rejected() -> Result<(), PerformanceError> {
        assert_eq!(
            PerformanceMilestone::new(0, 1, 1, MilestoneEvidence::Pending),
            Err(PerformanceError::NilObligation)
        );
        assert_eq!(
            PerformanceMilestone::new(1, 0, 1, MilestoneEvidence::Pending),
            Err(PerformanceError::ZeroWeight)
        );
        let enormous = [
            item(1, u64::MAX, 1, MilestoneEvidence::Pending)?,
            item(2, 1, 1, MilestoneEvidence::Pending)?,
        ];
        assert_eq!(
            evaluate_performance(1, &enormous),
            Err(PerformanceError::WeightOverflow)
        );
        let oversized =
            vec![item(1, 1, 1, MilestoneEvidence::Pending)?; MAX_PERFORMANCE_MILESTONES + 1];
        assert_eq!(
            evaluate_performance(1, &oversized),
            Err(PerformanceError::TooManyMilestones)
        );
        Ok(())
    }
}
