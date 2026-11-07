//! Deterministic, pure policy calculations for organization governance.
//!
//! This module checks typed caller observations and performs bounded integer
//! policy calculations. It does not authenticate observations, read clocks,
//! persist elections, grant roles, or prove that evidence came from an
//! authoritative source. Callers must freeze the scope, electorate, times,
//! current Mission, and evaluation revision before invoking these helpers.

use crate::performance::{MAX_PERFORMANCE_MILESTONES, PerformanceEvaluation, PerformanceTotals};
use std::fmt;

/// Team voter tenure threshold from the governance recommendation.
pub const TEAM_VOTER_TENURE_MS: u64 = 60 * 60 * 1_000;
/// Shared processing bound for election and merit cohorts.
///
/// This follows the existing 4,096-item performance input bound. It caps the
/// amount of data one pure call processes; it is not a roster, electorate, or
/// candidate eligibility limit.
pub const MAX_GOVERNANCE_COHORT_ITEMS: usize = MAX_PERFORMANCE_MILESTONES;

/// Invalid or contradictory typed inputs to the pure policy helpers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrganizationGovernanceError {
    NilIdentity,
    ZeroRevision,
    ElectionOpenedBeforeMembership,
    DeadlineBeforeElectionOpen,
    ObservationBeforeElectionOpen,
    BallotBeforeElectionOpen,
    InvalidFinalPerformance,
    ElectorateTooLarge,
    BallotCountExceedsElectorate,
    DuplicateElectorateMember,
    DuplicateBallot,
    BallotOutsideElectorate,
    BallotAtOrAfterDeadline,
    BallotAfterObservation,
    ArithmeticOverflow,
    EmptyCandidateSet,
    TooManyCandidates,
    DuplicateCandidate,
    CandidateEvidenceMismatch,
    CandidateContextMismatch,
    RevalidationContextMismatch,
    RevalidatedCandidateSetMismatch,
}

impl fmt::Display for OrganizationGovernanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NilIdentity => "organization governance identity is nil",
            Self::ZeroRevision => "organization governance revision must be positive",
            Self::ElectionOpenedBeforeMembership => {
                "election opened before the Team membership timestamp"
            }
            Self::DeadlineBeforeElectionOpen => "election deadline predates election open",
            Self::ObservationBeforeElectionOpen => "election observation predates election open",
            Self::BallotBeforeElectionOpen => "ballot predates election open",
            Self::InvalidFinalPerformance => "final performance totals are invalid",
            Self::ElectorateTooLarge => "election electorate exceeds its bounded size",
            Self::BallotCountExceedsElectorate => {
                "effective ballot count exceeds the frozen electorate size"
            }
            Self::DuplicateElectorateMember => "election electorate contains a duplicate identity",
            Self::DuplicateBallot => "election has more than one effective ballot per identity",
            Self::BallotOutsideElectorate => "ballot identity is outside the frozen electorate",
            Self::BallotAtOrAfterDeadline => {
                "ballot was recorded at or after the election deadline"
            }
            Self::BallotAfterObservation => {
                "ballot was recorded after the supplied observation time"
            }
            Self::ArithmeticOverflow => "organization governance arithmetic overflow",
            Self::EmptyCandidateSet => "merit comparison candidate set is empty",
            Self::TooManyCandidates => "merit comparison exceeds its bounded candidate count",
            Self::DuplicateCandidate => "merit comparison contains a duplicate candidate",
            Self::CandidateEvidenceMismatch => {
                "candidate evidence belongs to a different candidate"
            }
            Self::CandidateContextMismatch => {
                "candidate evidence belongs to a different comparison context"
            }
            Self::RevalidationContextMismatch => {
                "candidate-set revalidation belongs to a different comparison context"
            }
            Self::RevalidatedCandidateSetMismatch => {
                "revalidated candidates do not match the exact non-declined candidate set"
            }
        })
    }
}

impl std::error::Error for OrganizationGovernanceError {}

/// Why the ordinary Team-voter gate is satisfied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeamVoterEligibilityBasis {
    /// Membership had reached the full 60-minute threshold at election open.
    Tenure,
    /// A final, same-scope performance evaluation was strictly above 30%.
    FinalizedPerformance,
}

/// Result of the ordinary Team-voter gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeamVoterEligibility {
    Eligible(TeamVoterEligibilityBasis),
    Ineligible,
}

/// Evaluate the Team-only `60 minutes OR finalized performance > 30%` gate.
///
/// `opened_at_ms` is the caller-frozen election open time. Tenure is checked
/// first, so a valid time path remains independent of missing or low scores.
/// `PerformanceEvaluation::Pending` and `PerformanceEvaluation::NotEvaluable`
/// never become a score.
///
/// # Errors
/// Returns an error when the election predates membership or a purported final
/// evaluation has an empty denominator or accepted weight above its total.
pub fn team_voter_eligibility(
    joined_at_ms: u64,
    opened_at_ms: u64,
    performance: PerformanceEvaluation,
) -> Result<TeamVoterEligibility, OrganizationGovernanceError> {
    let tenure_ms = opened_at_ms
        .checked_sub(joined_at_ms)
        .ok_or(OrganizationGovernanceError::ElectionOpenedBeforeMembership)?;
    let final_totals = match performance {
        PerformanceEvaluation::Final(totals) => {
            if !valid_performance_totals(totals) {
                return Err(OrganizationGovernanceError::InvalidFinalPerformance);
            }
            Some(totals)
        }
        PerformanceEvaluation::NotEvaluable | PerformanceEvaluation::Pending { .. } => None,
    };
    if tenure_ms >= TEAM_VOTER_TENURE_MS {
        return Ok(TeamVoterEligibility::Eligible(
            TeamVoterEligibilityBasis::Tenure,
        ));
    }

    if let Some(totals) = final_totals {
        // Products of two u64 values fit exactly in u128.
        let accepted = u128::from(totals.accepted_weight());
        let due = u128::from(totals.obligation_weight());
        if accepted * 100 > due * 30 {
            return Ok(TeamVoterEligibility::Eligible(
                TeamVoterEligibilityBasis::FinalizedPerformance,
            ));
        }
    }
    Ok(TeamVoterEligibility::Ineligible)
}

/// Caller-observed Workspace leadership status at the frozen election scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceVoterRole {
    /// A current, active Team Leader may vote once for that Team.
    ActiveTeamLeader,
    /// A handover-only incumbent is not an eligible Workspace voter.
    HandoverOnlyLeader,
    /// Any non-Leader or inactive role observation.
    Other,
}

/// Workspace voting has no additional tenure or performance gate.
#[must_use]
pub const fn workspace_voter_is_eligible(role: WorkspaceVoterRole) -> bool {
    matches!(role, WorkspaceVoterRole::ActiveTeamLeader)
}

/// The two ballot stages relevant to the N=4 Team recall exception.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeamBallotStage {
    Recall,
    SuccessorSelection,
}

/// Caller-observed position of a voter in this Team ballot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeamBallotParticipantPosition {
    /// The current incumbent whose recall is being decided.
    IncumbentUnderRecall,
    /// Any other Team member, including a removed incumbent in succession.
    Other,
}

/// Whether the narrow N=4 rule permits the recalled incumbent to be considered.
///
/// This is only the incumbent-inclusion exception. The roster owner still
/// checks the exact incumbent identity, current role, ordinary eligibility,
/// Team home membership, and transfer constraints. It never applies to a
/// successor ballot.
#[must_use]
pub const fn n4_recall_incumbent_may_be_included(
    registered_home_member_count: usize,
    ballot_stage: TeamBallotStage,
    participant_position: TeamBallotParticipantPosition,
) -> bool {
    registered_home_member_count == 4
        && matches!(ballot_stage, TeamBallotStage::Recall)
        && matches!(
            participant_position,
            TeamBallotParticipantPosition::IncumbentUnderRecall
        )
}

/// Team or Workspace election scope selects its distinct quorum formula.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionScope {
    Team,
    Workspace,
}

/// One already-resolved effective response from a frozen electorate member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffectiveElectionBallot {
    voter_id: u128,
    choice: ElectionVote,
    recorded_at_ms: u64,
}

impl EffectiveElectionBallot {
    /// Construct one response. The tally input later checks its scope/time.
    ///
    /// # Errors
    /// Rejects a nil voter identity.
    pub fn new(
        voter_id: u128,
        choice: ElectionVote,
        recorded_at_ms: u64,
    ) -> Result<Self, OrganizationGovernanceError> {
        if voter_id == 0 {
            return Err(OrganizationGovernanceError::NilIdentity);
        }
        Ok(Self {
            voter_id,
            choice,
            recorded_at_ms,
        })
    }
}

/// An explicit yes, no, or abstention; absence is represented by no ballot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionVote {
    Yes,
    No,
    Abstain,
}

/// Frozen inputs for one bounded election tally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectionTallyInput {
    scope: ElectionScope,
    electorate: Vec<u128>,
    ballots: Vec<EffectiveElectionBallot>,
    deadline_at_ms: u64,
    observed_at_ms: u64,
}

impl ElectionTallyInput {
    /// Validate and freeze a unique electorate and at most one effective ballot
    /// per member. Empty and one-person electorates are representable but can
    /// never pass. All scope kinds use the shared processing bound; this is not
    /// a roster eligibility cutoff.
    ///
    /// `observed_at_ms` is supplied by the caller. Any vote recorded exactly at
    /// the deadline or later is rejected, while the tally remains open until
    /// the observation time reaches the deadline.
    ///
    /// # Errors
    /// Rejects oversized or duplicate electorates, duplicate/foreign ballots,
    /// votes at/after the deadline, and votes later than the supplied cutoff.
    pub fn new(
        scope: ElectionScope,
        opened_at_ms: u64,
        electorate: Vec<u128>,
        ballots: Vec<EffectiveElectionBallot>,
        deadline_at_ms: u64,
        observed_at_ms: u64,
    ) -> Result<Self, OrganizationGovernanceError> {
        if electorate.len() > MAX_GOVERNANCE_COHORT_ITEMS {
            return Err(OrganizationGovernanceError::ElectorateTooLarge);
        }
        if ballots.len() > electorate.len() {
            return Err(OrganizationGovernanceError::BallotCountExceedsElectorate);
        }
        if deadline_at_ms < opened_at_ms {
            return Err(OrganizationGovernanceError::DeadlineBeforeElectionOpen);
        }
        if observed_at_ms < opened_at_ms {
            return Err(OrganizationGovernanceError::ObservationBeforeElectionOpen);
        }

        let mut electorate = electorate;
        if electorate.contains(&0) {
            return Err(OrganizationGovernanceError::NilIdentity);
        }
        electorate.sort_unstable();
        if electorate.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(OrganizationGovernanceError::DuplicateElectorateMember);
        }
        let mut ballots = ballots;
        ballots.sort_unstable_by_key(|ballot| ballot.voter_id);
        for pair in ballots.windows(2) {
            if pair[0].voter_id == pair[1].voter_id {
                return Err(OrganizationGovernanceError::DuplicateBallot);
            }
        }
        for ballot in &ballots {
            if electorate.binary_search(&ballot.voter_id).is_err() {
                return Err(OrganizationGovernanceError::BallotOutsideElectorate);
            }
            if ballot.recorded_at_ms < opened_at_ms {
                return Err(OrganizationGovernanceError::BallotBeforeElectionOpen);
            }
            if ballot.recorded_at_ms >= deadline_at_ms {
                return Err(OrganizationGovernanceError::BallotAtOrAfterDeadline);
            }
            if ballot.recorded_at_ms > observed_at_ms {
                return Err(OrganizationGovernanceError::BallotAfterObservation);
            }
        }

        Ok(Self {
            scope,
            electorate,
            ballots,
            deadline_at_ms,
            observed_at_ms,
        })
    }
}

/// Terminal classification after the supplied deadline, or `Open` before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionTallyStatus {
    Open,
    InsufficientElectorate,
    NoQuorum,
    Passed,
    Rejected,
}

/// Validated integer totals for a frozen election observation.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ElectionTally {
    electorate_count: usize,
    required_quorum: usize,
    yes_count: usize,
    no_count: usize,
    abstain_count: usize,
    absent_count: usize,
    status: ElectionTallyStatus,
}

impl ElectionTally {
    #[must_use]
    pub const fn electorate_count(self) -> usize {
        self.electorate_count
    }

    #[must_use]
    pub const fn required_quorum(self) -> usize {
        self.required_quorum
    }

    #[must_use]
    pub const fn yes_count(self) -> usize {
        self.yes_count
    }

    #[must_use]
    pub const fn no_count(self) -> usize {
        self.no_count
    }

    #[must_use]
    pub const fn abstain_count(self) -> usize {
        self.abstain_count
    }

    #[must_use]
    pub const fn absent_count(self) -> usize {
        self.absent_count
    }

    #[must_use]
    pub const fn status(self) -> ElectionTallyStatus {
        self.status
    }
}

/// Tally one frozen election. The result cannot become terminal before the
/// supplied deadline, regardless of the responses already present.
///
/// # Errors
/// Returns an arithmetic error if a checked count/quorum operation overflows.
pub fn tally_election(
    input: &ElectionTallyInput,
) -> Result<ElectionTally, OrganizationGovernanceError> {
    let electorate_count = input.electorate.len();
    let required_quorum = required_quorum(electorate_count, input.scope)?;
    let mut yes_count = 0_usize;
    let mut no_count = 0_usize;
    let mut abstain_count = 0_usize;
    for ballot in &input.ballots {
        let count = match ballot.choice {
            ElectionVote::Yes => &mut yes_count,
            ElectionVote::No => &mut no_count,
            ElectionVote::Abstain => &mut abstain_count,
        };
        *count = count
            .checked_add(1)
            .ok_or(OrganizationGovernanceError::ArithmeticOverflow)?;
    }
    let participation_count = yes_count
        .checked_add(no_count)
        .and_then(|count| count.checked_add(abstain_count))
        .ok_or(OrganizationGovernanceError::ArithmeticOverflow)?;
    let absent_count = electorate_count
        .checked_sub(participation_count)
        .ok_or(OrganizationGovernanceError::ArithmeticOverflow)?;

    let status = if input.observed_at_ms < input.deadline_at_ms {
        ElectionTallyStatus::Open
    } else if electorate_count < 2 {
        ElectionTallyStatus::InsufficientElectorate
    } else if participation_count < required_quorum {
        ElectionTallyStatus::NoQuorum
    } else if yes_count >= 2 && yes_count > electorate_count / 2 {
        ElectionTallyStatus::Passed
    } else {
        ElectionTallyStatus::Rejected
    };

    Ok(ElectionTally {
        electorate_count,
        required_quorum,
        yes_count,
        no_count,
        abstain_count,
        absent_count,
        status,
    })
}

fn required_quorum(
    electorate_count: usize,
    scope: ElectionScope,
) -> Result<usize, OrganizationGovernanceError> {
    let (numerator, denominator) = match scope {
        ElectionScope::Team => (2_usize, 3_usize),
        ElectionScope::Workspace => (3_usize, 4_usize),
    };
    let whole = (electorate_count / denominator)
        .checked_mul(numerator)
        .ok_or(OrganizationGovernanceError::ArithmeticOverflow)?;
    let remainder_product = (electorate_count % denominator)
        .checked_mul(numerator)
        .ok_or(OrganizationGovernanceError::ArithmeticOverflow)?;
    let fractional = remainder_product
        .checked_add(denominator - 1)
        .and_then(|value| value.checked_div(denominator))
        .ok_or(OrganizationGovernanceError::ArithmeticOverflow)?;
    whole
        .checked_add(fractional)
        .ok_or(OrganizationGovernanceError::ArithmeticOverflow)
}

/// Exact identity and revision for one caller-defined comparison snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MeritComparisonContext {
    comparison_id: u128,
    comparison_revision: u64,
    workspace_id: u128,
    team_id: u128,
    mission_id: u128,
    plan_id: u128,
    plan_revision: u64,
}

impl MeritComparisonContext {
    /// Bind observations to one exact Workspace, Team, current-Mission claim,
    /// plan revision, and comparison revision.
    ///
    /// This constructor checks identity shape only; it cannot prove that any
    /// referenced object is actually current or authoritative.
    ///
    /// # Errors
    /// Rejects nil identities and zero revisions.
    pub fn new(
        comparison_id: u128,
        comparison_revision: u64,
        workspace_id: u128,
        team_id: u128,
        mission_id: u128,
        plan_id: u128,
        plan_revision: u64,
    ) -> Result<Self, OrganizationGovernanceError> {
        if [comparison_id, workspace_id, team_id, mission_id, plan_id].contains(&0) {
            return Err(OrganizationGovernanceError::NilIdentity);
        }
        if comparison_revision == 0 || plan_revision == 0 {
            return Err(OrganizationGovernanceError::ZeroRevision);
        }
        Ok(Self {
            comparison_id,
            comparison_revision,
            workspace_id,
            team_id,
            mission_id,
            plan_id,
            plan_revision,
        })
    }

    #[must_use]
    pub const fn comparison_id(self) -> u128 {
        self.comparison_id
    }

    #[must_use]
    pub const fn comparison_revision(self) -> u64 {
        self.comparison_revision
    }

    #[must_use]
    pub const fn workspace_id(self) -> u128 {
        self.workspace_id
    }

    #[must_use]
    pub const fn team_id(self) -> u128 {
        self.team_id
    }

    #[must_use]
    pub const fn mission_id(self) -> u128 {
        self.mission_id
    }

    #[must_use]
    pub const fn plan_id(self) -> u128 {
        self.plan_id
    }

    #[must_use]
    pub const fn plan_revision(self) -> u64 {
        self.plan_revision
    }
}

/// Caller-attested evidence reference scoped to one candidate comparison.
///
/// The reference is an opaque typed identity, not a source-proof hash. Its
/// existence does not authenticate the underlying evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MeritEvidenceReference {
    context: MeritComparisonContext,
    candidate_id: u128,
    evidence_id: u128,
}

impl MeritEvidenceReference {
    /// Construct a caller-attested work or qualification reference.
    ///
    /// # Errors
    /// Rejects nil candidate/evidence identities.
    pub fn new(
        context: MeritComparisonContext,
        candidate_id: u128,
        evidence_id: u128,
    ) -> Result<Self, OrganizationGovernanceError> {
        if candidate_id == 0 || evidence_id == 0 {
            return Err(OrganizationGovernanceError::NilIdentity);
        }
        Ok(Self {
            context,
            candidate_id,
            evidence_id,
        })
    }
}

/// Caller-attested weighted evaluation bound to the same comparison context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MeritPerformanceObservation {
    context: MeritComparisonContext,
    candidate_id: u128,
    evaluation: PerformanceEvaluation,
}

impl MeritPerformanceObservation {
    /// Bind the existing pure performance result to a candidate and comparison.
    ///
    /// # Errors
    /// Rejects a nil candidate identity or malformed final totals.
    pub fn new(
        context: MeritComparisonContext,
        candidate_id: u128,
        evaluation: PerformanceEvaluation,
    ) -> Result<Self, OrganizationGovernanceError> {
        if candidate_id == 0 {
            return Err(OrganizationGovernanceError::NilIdentity);
        }
        if let PerformanceEvaluation::Final(totals) = evaluation
            && !valid_performance_totals(totals)
        {
            return Err(OrganizationGovernanceError::InvalidFinalPerformance);
        }
        Ok(Self {
            context,
            candidate_id,
            evaluation,
        })
    }
}

/// The caller's recorded response to this one merit-based role offer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeritCandidateResponse {
    Accepted,
    Declined,
    Unavailable,
    NoResponse,
}

/// One caller-attested candidate in a bounded current-Mission comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeritCandidate {
    candidate_id: u128,
    response: MeritCandidateResponse,
    substantive_work: Option<MeritEvidenceReference>,
    qualification: Option<MeritEvidenceReference>,
    performance: Option<MeritPerformanceObservation>,
}

impl MeritCandidate {
    /// Construct one candidate observation. Missing evidence remains explicit
    /// and causes a Hold if this candidate remains in the comparison set.
    ///
    /// # Errors
    /// Rejects a nil candidate identity or evidence references for another
    /// candidate. Context is checked when the full comparison is constructed.
    pub fn new(
        candidate_id: u128,
        response: MeritCandidateResponse,
        substantive_work: Option<MeritEvidenceReference>,
        qualification: Option<MeritEvidenceReference>,
        performance: Option<MeritPerformanceObservation>,
    ) -> Result<Self, OrganizationGovernanceError> {
        if candidate_id == 0 {
            return Err(OrganizationGovernanceError::NilIdentity);
        }
        if substantive_work.is_some_and(|observation| observation.candidate_id != candidate_id)
            || qualification.is_some_and(|observation| observation.candidate_id != candidate_id)
            || performance.is_some_and(|observation| observation.candidate_id != candidate_id)
        {
            return Err(OrganizationGovernanceError::CandidateEvidenceMismatch);
        }
        Ok(Self {
            candidate_id,
            response,
            substantive_work,
            qualification,
            performance,
        })
    }
}

/// Caller observation that revalidates the exact eligible, non-declined set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevalidatedComparison {
    context: MeritComparisonContext,
    eligible_candidate_ids: Vec<u128>,
}

impl RevalidatedComparison {
    /// Bind an exact eligible set to the comparison's identity and revision.
    ///
    /// An empty set is valid for the case where every original candidate
    /// explicitly declined. The caller, not this helper, authenticates the
    /// revalidation and any changed candidate eligibility.
    ///
    /// # Errors
    /// Rejects nil/duplicate identities or an oversized set.
    pub fn new(
        context: MeritComparisonContext,
        mut eligible_candidate_ids: Vec<u128>,
    ) -> Result<Self, OrganizationGovernanceError> {
        if eligible_candidate_ids.len() > MAX_GOVERNANCE_COHORT_ITEMS {
            return Err(OrganizationGovernanceError::TooManyCandidates);
        }
        if eligible_candidate_ids.contains(&0) {
            return Err(OrganizationGovernanceError::NilIdentity);
        }
        eligible_candidate_ids.sort_unstable();
        if eligible_candidate_ids
            .windows(2)
            .any(|pair| pair[0] == pair[1])
        {
            return Err(OrganizationGovernanceError::DuplicateCandidate);
        }
        Ok(Self {
            context,
            eligible_candidate_ids,
        })
    }
}

/// Exact frozen input for a first-merit comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeritComparisonInput {
    context: MeritComparisonContext,
    candidates: Vec<MeritCandidate>,
    revalidation: Option<RevalidatedComparison>,
}

impl MeritComparisonInput {
    /// Check candidate identity uniqueness, evidence scope, and any revalidated
    /// non-declined set. The vector represents the caller's exact prequalified
    /// candidates for one current-Mission/plan comparison.
    ///
    /// # Errors
    /// Rejects empty/oversized or duplicate candidates and context/set mismatch.
    pub fn new(
        context: MeritComparisonContext,
        mut candidates: Vec<MeritCandidate>,
        revalidation: Option<RevalidatedComparison>,
    ) -> Result<Self, OrganizationGovernanceError> {
        if candidates.is_empty() {
            return Err(OrganizationGovernanceError::EmptyCandidateSet);
        }
        if candidates.len() > MAX_GOVERNANCE_COHORT_ITEMS {
            return Err(OrganizationGovernanceError::TooManyCandidates);
        }
        candidates.sort_unstable_by_key(|candidate| candidate.candidate_id);
        if candidates
            .windows(2)
            .any(|pair| pair[0].candidate_id == pair[1].candidate_id)
        {
            return Err(OrganizationGovernanceError::DuplicateCandidate);
        }
        for candidate in &candidates {
            for observation_context in [
                candidate
                    .substantive_work
                    .map(|observation| observation.context),
                candidate
                    .qualification
                    .map(|observation| observation.context),
                candidate.performance.map(|observation| observation.context),
            ]
            .into_iter()
            .flatten()
            {
                if observation_context != context {
                    return Err(OrganizationGovernanceError::CandidateContextMismatch);
                }
            }
        }

        if let Some(revalidated) = &revalidation {
            if revalidated.context != context {
                return Err(OrganizationGovernanceError::RevalidationContextMismatch);
            }
            let current_ids: Vec<_> = candidates
                .iter()
                .filter(|candidate| candidate.response != MeritCandidateResponse::Declined)
                .map(|candidate| candidate.candidate_id)
                .collect();
            if revalidated.eligible_candidate_ids != current_ids {
                return Err(OrganizationGovernanceError::RevalidatedCandidateSetMismatch);
            }
        }

        Ok(Self {
            context,
            candidates,
            revalidation,
        })
    }
}

/// Why an otherwise scoped merit comparison remains unresolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeritHoldReason {
    RevalidationRequired,
    MissingSubstantiveWorkEvidence { candidate_id: u128 },
    MissingQualificationEvidence { candidate_id: u128 },
    MissingPerformanceEvaluation { candidate_id: u128 },
    PerformanceNotFinal { candidate_id: u128 },
    TopCandidateUnavailable { candidate_id: u128 },
    TopCandidateNoResponse { candidate_id: u128 },
}

/// A determinate Leaderless outcome from the merit comparison itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeritLeaderlessReason {
    NoEligibleCandidate,
    ExactTie { candidate_ids: Vec<u128> },
}

/// Result of pure merit comparison; it does not activate or grant a role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeritComparisonOutcome {
    Hold(MeritHoldReason),
    Leaderless(MeritLeaderlessReason),
    Selected { candidate_id: u128 },
}

/// Result bound to the exact comparison context supplied by the caller.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeritComparisonDecision {
    context: MeritComparisonContext,
    outcome: MeritComparisonOutcome,
}

impl MeritComparisonDecision {
    #[must_use]
    pub const fn context(&self) -> MeritComparisonContext {
        self.context
    }

    #[must_use]
    pub const fn outcome(&self) -> &MeritComparisonOutcome {
        &self.outcome
    }
}

type ScoredCandidate<'a> = (&'a MeritCandidate, PerformanceTotals);

/// Compare final weighted rates exactly; no float, tie break, or fallback score.
pub fn evaluate_initial_merit(input: &MeritComparisonInput) -> MeritComparisonDecision {
    if input
        .candidates
        .iter()
        .any(|candidate| candidate.response == MeritCandidateResponse::Declined)
        && input.revalidation.is_none()
    {
        return merit_hold(input.context, MeritHoldReason::RevalidationRequired);
    }

    match score_candidates(&input.candidates) {
        Ok(scored) => evaluate_scored_candidates(input.context, &scored),
        Err(reason) => merit_hold(input.context, reason),
    }
}

fn score_candidates(
    candidates: &[MeritCandidate],
) -> Result<Vec<ScoredCandidate<'_>>, MeritHoldReason> {
    let mut scored = Vec::new();
    for candidate in candidates
        .iter()
        .filter(|candidate| candidate.response != MeritCandidateResponse::Declined)
    {
        if candidate.substantive_work.is_none() {
            return Err(MeritHoldReason::MissingSubstantiveWorkEvidence {
                candidate_id: candidate.candidate_id,
            });
        }
        if candidate.qualification.is_none() {
            return Err(MeritHoldReason::MissingQualificationEvidence {
                candidate_id: candidate.candidate_id,
            });
        }
        let Some(observation) = candidate.performance else {
            return Err(MeritHoldReason::MissingPerformanceEvaluation {
                candidate_id: candidate.candidate_id,
            });
        };
        let PerformanceEvaluation::Final(totals) = observation.evaluation else {
            return Err(MeritHoldReason::PerformanceNotFinal {
                candidate_id: candidate.candidate_id,
            });
        };
        scored.push((candidate, totals));
    }
    Ok(scored)
}

fn evaluate_scored_candidates(
    context: MeritComparisonContext,
    scored: &[ScoredCandidate<'_>],
) -> MeritComparisonDecision {
    let Some((_, first_totals)) = scored.first() else {
        return MeritComparisonDecision {
            context,
            outcome: MeritComparisonOutcome::Leaderless(MeritLeaderlessReason::NoEligibleCandidate),
        };
    };
    let mut best = *first_totals;
    for (_, totals) in scored.iter().skip(1) {
        if compare_performance(*totals, best).is_gt() {
            best = *totals;
        }
    }
    let leaders: Vec<_> = scored
        .iter()
        .filter(|(_, totals)| compare_performance(*totals, best).is_eq())
        .map(|(candidate, _)| *candidate)
        .collect();

    if let Some(reason) = leaders
        .iter()
        .find_map(|candidate| match candidate.response {
            MeritCandidateResponse::Unavailable => Some(MeritHoldReason::TopCandidateUnavailable {
                candidate_id: candidate.candidate_id,
            }),
            MeritCandidateResponse::NoResponse => Some(MeritHoldReason::TopCandidateNoResponse {
                candidate_id: candidate.candidate_id,
            }),
            MeritCandidateResponse::Declined => Some(MeritHoldReason::RevalidationRequired),
            MeritCandidateResponse::Accepted => None,
        })
    {
        return merit_hold(context, reason);
    }

    if leaders.len() > 1 {
        return MeritComparisonDecision {
            context,
            outcome: MeritComparisonOutcome::Leaderless(MeritLeaderlessReason::ExactTie {
                candidate_ids: leaders
                    .iter()
                    .map(|candidate| candidate.candidate_id)
                    .collect(),
            }),
        };
    }

    let Some(leader) = leaders.first() else {
        return merit_hold(context, MeritHoldReason::RevalidationRequired);
    };
    match leader.response {
        MeritCandidateResponse::Accepted => MeritComparisonDecision {
            context,
            outcome: MeritComparisonOutcome::Selected {
                candidate_id: leader.candidate_id,
            },
        },
        MeritCandidateResponse::Unavailable => merit_hold(
            context,
            MeritHoldReason::TopCandidateUnavailable {
                candidate_id: leader.candidate_id,
            },
        ),
        MeritCandidateResponse::NoResponse => merit_hold(
            context,
            MeritHoldReason::TopCandidateNoResponse {
                candidate_id: leader.candidate_id,
            },
        ),
        MeritCandidateResponse::Declined => {
            // Declined candidates were removed from `scored` only after the
            // exact-set revalidation check above.
            merit_hold(context, MeritHoldReason::RevalidationRequired)
        }
    }
}

fn merit_hold(context: MeritComparisonContext, reason: MeritHoldReason) -> MeritComparisonDecision {
    MeritComparisonDecision {
        context,
        outcome: MeritComparisonOutcome::Hold(reason),
    }
}

fn compare_performance(left: PerformanceTotals, right: PerformanceTotals) -> std::cmp::Ordering {
    // Products of two u64 weights fit exactly in u128.
    let left_cross = u128::from(left.accepted_weight()) * u128::from(right.obligation_weight());
    let right_cross = u128::from(right.accepted_weight()) * u128::from(left.obligation_weight());
    left_cross.cmp(&right_cross)
}

fn valid_performance_totals(totals: PerformanceTotals) -> bool {
    totals.obligation_weight() > 0 && totals.accepted_weight() <= totals.obligation_weight()
}

#[cfg(test)]
mod tests;
