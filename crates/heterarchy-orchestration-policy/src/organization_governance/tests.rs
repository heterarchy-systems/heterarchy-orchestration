use super::*;
use crate::performance::{MilestoneEvidence, PerformanceMilestone, evaluate_performance};
use std::error::Error;

fn final_performance(
    accepted_weight: u64,
    obligation_weight: u64,
) -> Result<PerformanceEvaluation, Box<dyn Error>> {
    let mut milestones = Vec::new();
    if accepted_weight > 0 {
        milestones.push(PerformanceMilestone::new(
            1,
            accepted_weight,
            100,
            MilestoneEvidence::Accepted {
                submitted_at_ms: 50,
            },
        )?);
    }
    if obligation_weight > accepted_weight {
        milestones.push(PerformanceMilestone::new(
            2,
            obligation_weight - accepted_weight,
            100,
            MilestoneEvidence::ConfirmedUnfulfilled,
        )?);
    }
    Ok(evaluate_performance(100, &milestones)?)
}

fn context() -> Result<MeritComparisonContext, OrganizationGovernanceError> {
    MeritComparisonContext::new(1, 1, 2, 3, 4, 5, 6)
}

fn candidate(
    context: MeritComparisonContext,
    candidate_id: u128,
    response: MeritCandidateResponse,
    accepted_weight: u64,
    obligation_weight: u64,
) -> Result<MeritCandidate, Box<dyn Error>> {
    let work = MeritEvidenceReference::new(context, candidate_id, 100 + candidate_id)?;
    let qualification = MeritEvidenceReference::new(context, candidate_id, 200 + candidate_id)?;
    let evaluation = MeritPerformanceObservation::new(
        context,
        candidate_id,
        final_performance(accepted_weight, obligation_weight)?,
    )?;
    Ok(MeritCandidate::new(
        candidate_id,
        response,
        Some(work),
        Some(qualification),
        Some(evaluation),
    )?)
}

fn ballot(
    voter_id: u128,
    choice: ElectionVote,
    recorded_at_ms: u64,
) -> Result<EffectiveElectionBallot, OrganizationGovernanceError> {
    EffectiveElectionBallot::new(voter_id, choice, recorded_at_ms)
}

#[test]
fn team_voter_gate_observes_exact_tenure_and_performance_boundaries() -> Result<(), Box<dyn Error>>
{
    let no_score = PerformanceEvaluation::NotEvaluable;
    assert_eq!(
        team_voter_eligibility(0, TEAM_VOTER_TENURE_MS - 1, no_score)?,
        TeamVoterEligibility::Ineligible
    );
    assert_eq!(
        team_voter_eligibility(0, TEAM_VOTER_TENURE_MS, no_score)?,
        TeamVoterEligibility::Eligible(TeamVoterEligibilityBasis::Tenure)
    );

    let exactly_thirty = final_performance(30, 100)?;
    assert_eq!(
        team_voter_eligibility(1, 1, exactly_thirty)?,
        TeamVoterEligibility::Ineligible
    );
    let above_thirty = final_performance(31, 100)?;
    assert_eq!(
        team_voter_eligibility(1, 1, above_thirty)?,
        TeamVoterEligibility::Eligible(TeamVoterEligibilityBasis::FinalizedPerformance)
    );
    assert_eq!(
        team_voter_eligibility(1, 1, final_performance(u64::MAX, u64::MAX)?,)?,
        TeamVoterEligibility::Eligible(TeamVoterEligibilityBasis::FinalizedPerformance)
    );
    let pending = evaluate_performance(
        100,
        &[PerformanceMilestone::new(
            1,
            10,
            100,
            MilestoneEvidence::Pending,
        )?],
    )?;
    assert_eq!(
        team_voter_eligibility(1, 1, pending)?,
        TeamVoterEligibility::Ineligible
    );
    assert!(matches!(
        team_voter_eligibility(2, 1, no_score),
        Err(OrganizationGovernanceError::ElectionOpenedBeforeMembership)
    ));
    Ok(())
}

#[test]
fn workspace_gate_and_n4_recall_exception_stay_narrow() {
    assert!(workspace_voter_is_eligible(
        WorkspaceVoterRole::ActiveTeamLeader
    ));
    assert!(!workspace_voter_is_eligible(
        WorkspaceVoterRole::HandoverOnlyLeader
    ));
    assert!(!workspace_voter_is_eligible(WorkspaceVoterRole::Other));

    assert!(n4_recall_incumbent_may_be_included(
        4,
        TeamBallotStage::Recall,
        TeamBallotParticipantPosition::IncumbentUnderRecall,
    ));
    assert!(!n4_recall_incumbent_may_be_included(
        4,
        TeamBallotStage::SuccessorSelection,
        TeamBallotParticipantPosition::IncumbentUnderRecall,
    ));
    assert!(!n4_recall_incumbent_may_be_included(
        3,
        TeamBallotStage::Recall,
        TeamBallotParticipantPosition::IncumbentUnderRecall,
    ));
    assert!(!n4_recall_incumbent_may_be_included(
        4,
        TeamBallotStage::Recall,
        TeamBallotParticipantPosition::Other,
    ));
}

#[test]
fn workspace_two_yes_votes_of_three_pass_with_explicit_abstention() -> Result<(), Box<dyn Error>> {
    let input = ElectionTallyInput::new(
        ElectionScope::Workspace,
        0,
        vec![1, 2, 3],
        vec![
            ballot(1, ElectionVote::Yes, 5)?,
            ballot(2, ElectionVote::Yes, 6)?,
            ballot(3, ElectionVote::Abstain, 7)?,
        ],
        10,
        10,
    )?;
    let tally = tally_election(&input)?;
    assert_eq!(tally.required_quorum(), 3);
    assert_eq!(tally.yes_count(), 2);
    assert_eq!(tally.abstain_count(), 1);
    assert_eq!(tally.absent_count(), 0);
    assert_eq!(tally.status(), ElectionTallyStatus::Passed);
    Ok(())
}

#[test]
fn abstention_counts_as_participation_but_absence_does_not() -> Result<(), Box<dyn Error>> {
    let electorate = vec![1, 2, 3, 4];
    let with_abstention = ElectionTallyInput::new(
        ElectionScope::Workspace,
        0,
        electorate.clone(),
        vec![
            ballot(1, ElectionVote::Yes, 5)?,
            ballot(2, ElectionVote::Yes, 6)?,
            ballot(3, ElectionVote::Abstain, 7)?,
        ],
        10,
        10,
    )?;
    let tally = tally_election(&with_abstention)?;
    assert_eq!(tally.status(), ElectionTallyStatus::Rejected);
    assert_eq!(tally.abstain_count(), 1);
    assert_eq!(tally.absent_count(), 1);

    let absent = ElectionTallyInput::new(
        ElectionScope::Workspace,
        0,
        electorate,
        vec![
            ballot(1, ElectionVote::Yes, 5)?,
            ballot(2, ElectionVote::Yes, 6)?,
        ],
        10,
        10,
    )?;
    let tally = tally_election(&absent)?;
    assert_eq!(tally.status(), ElectionTallyStatus::NoQuorum);
    assert_eq!(tally.absent_count(), 2);
    Ok(())
}

#[test]
fn election_stays_open_until_deadline_and_rejects_a_vote_at_deadline() -> Result<(), Box<dyn Error>>
{
    let open = ElectionTallyInput::new(
        ElectionScope::Workspace,
        0,
        vec![1, 2, 3],
        vec![
            ballot(1, ElectionVote::Yes, 5)?,
            ballot(2, ElectionVote::Yes, 6)?,
            ballot(3, ElectionVote::Abstain, 7)?,
        ],
        10,
        9,
    )?;
    assert_eq!(tally_election(&open)?.status(), ElectionTallyStatus::Open);

    let closed = ElectionTallyInput::new(
        ElectionScope::Workspace,
        0,
        vec![1, 2, 3],
        vec![
            ballot(1, ElectionVote::Yes, 5)?,
            ballot(2, ElectionVote::Yes, 6)?,
            ballot(3, ElectionVote::Abstain, 7)?,
        ],
        10,
        10,
    )?;
    assert_eq!(
        tally_election(&closed)?.status(),
        ElectionTallyStatus::Passed
    );

    assert!(matches!(
        ElectionTallyInput::new(
            ElectionScope::Workspace,
            0,
            vec![1, 2, 3],
            vec![ballot(1, ElectionVote::Yes, 10)?],
            10,
            10,
        ),
        Err(OrganizationGovernanceError::BallotAtOrAfterDeadline)
    ));
    Ok(())
}

#[test]
fn election_input_rejects_duplicate_or_foreign_voters_and_future_ballots()
-> Result<(), Box<dyn Error>> {
    assert!(matches!(
        ElectionTallyInput::new(
            ElectionScope::Team,
            0,
            vec![1],
            vec![
                ballot(1, ElectionVote::Yes, 1)?,
                ballot(1, ElectionVote::No, 2)?,
            ],
            10,
            5,
        ),
        Err(OrganizationGovernanceError::BallotCountExceedsElectorate)
    ));
    assert!(matches!(
        ElectionTallyInput::new(
            ElectionScope::Team,
            0,
            vec![1, 2],
            vec![
                ballot(1, ElectionVote::Yes, 1)?,
                ballot(1, ElectionVote::No, 2)?,
            ],
            10,
            5,
        ),
        Err(OrganizationGovernanceError::DuplicateBallot)
    ));
    assert!(matches!(
        ElectionTallyInput::new(
            ElectionScope::Team,
            0,
            vec![1, 2],
            vec![ballot(3, ElectionVote::Yes, 1)?],
            10,
            5,
        ),
        Err(OrganizationGovernanceError::BallotOutsideElectorate)
    ));
    assert!(matches!(
        ElectionTallyInput::new(
            ElectionScope::Team,
            0,
            vec![1, 2],
            vec![ballot(1, ElectionVote::Yes, 6)?],
            10,
            5,
        ),
        Err(OrganizationGovernanceError::BallotAfterObservation)
    ));
    assert!(matches!(
        ElectionTallyInput::new(ElectionScope::Team, 0, vec![1, 1], Vec::new(), 10, 5,),
        Err(OrganizationGovernanceError::DuplicateElectorateMember)
    ));
    assert!(matches!(
        ElectionTallyInput::new(
            ElectionScope::Team,
            2,
            vec![1, 2],
            vec![ballot(1, ElectionVote::Yes, 1)?],
            10,
            5,
        ),
        Err(OrganizationGovernanceError::BallotBeforeElectionOpen)
    ));
    Ok(())
}

#[test]
fn bounded_processing_keeps_existing_over_roster_electorates_evaluable()
-> Result<(), Box<dyn Error>> {
    let team_input = ElectionTallyInput::new(
        ElectionScope::Team,
        0,
        (1..=9).collect(),
        Vec::new(),
        10,
        10,
    )?;
    assert_eq!(tally_election(&team_input)?.electorate_count(), 9);

    let workspace_input = ElectionTallyInput::new(
        ElectionScope::Workspace,
        0,
        (1..=5).collect(),
        Vec::new(),
        10,
        10,
    )?;
    assert_eq!(tally_election(&workspace_input)?.electorate_count(), 5);
    Ok(())
}

#[test]
fn small_electorate_cannot_pass_and_quorum_arithmetic_handles_usize_max()
-> Result<(), Box<dyn Error>> {
    let one_person = ElectionTallyInput::new(
        ElectionScope::Team,
        0,
        vec![1],
        vec![ballot(1, ElectionVote::Yes, 4)?],
        5,
        5,
    )?;
    assert_eq!(
        tally_election(&one_person)?.status(),
        ElectionTallyStatus::InsufficientElectorate
    );

    let team_quorum = required_quorum(usize::MAX, ElectionScope::Team)?;
    let expected_team = (usize::MAX / 3) * 2 + ((usize::MAX % 3) * 2).div_ceil(3);
    assert_eq!(team_quorum, expected_team);
    let workspace_quorum = required_quorum(usize::MAX, ElectionScope::Workspace)?;
    let expected_workspace = (usize::MAX / 4) * 3 + ((usize::MAX % 4) * 3).div_ceil(4);
    assert_eq!(workspace_quorum, expected_workspace);
    Ok(())
}

#[test]
fn exact_merit_fractions_leave_a_tie_leaderless() -> Result<(), Box<dyn Error>> {
    let context = context()?;
    let input = MeritComparisonInput::new(
        context,
        vec![
            candidate(context, 1, MeritCandidateResponse::Accepted, 1, 2)?,
            candidate(context, 2, MeritCandidateResponse::Accepted, 2, 4)?,
        ],
        None,
    )?;
    assert_eq!(
        evaluate_initial_merit(&input).outcome(),
        &MeritComparisonOutcome::Leaderless(MeritLeaderlessReason::ExactTie {
            candidate_ids: vec![1, 2],
        })
    );
    Ok(())
}

#[test]
fn pending_or_missing_merit_evidence_holds_before_ranking() -> Result<(), Box<dyn Error>> {
    let context = context()?;
    let pending = MeritPerformanceObservation::new(
        context,
        1,
        evaluate_performance(
            100,
            &[PerformanceMilestone::new(
                1,
                10,
                100,
                MilestoneEvidence::Pending,
            )?],
        )?,
    )?;
    let candidate = MeritCandidate::new(
        1,
        MeritCandidateResponse::Accepted,
        Some(MeritEvidenceReference::new(context, 1, 101)?),
        Some(MeritEvidenceReference::new(context, 1, 201)?),
        Some(pending),
    )?;
    let input = MeritComparisonInput::new(context, vec![candidate], None)?;
    assert_eq!(
        evaluate_initial_merit(&input).outcome(),
        &MeritComparisonOutcome::Hold(MeritHoldReason::PerformanceNotFinal { candidate_id: 1 })
    );

    let missing = MeritCandidate::new(
        2,
        MeritCandidateResponse::Accepted,
        None,
        Some(MeritEvidenceReference::new(context, 2, 202)?),
        Some(MeritPerformanceObservation::new(
            context,
            2,
            final_performance(1, 1)?,
        )?),
    )?;
    let input = MeritComparisonInput::new(context, vec![missing], None)?;
    assert_eq!(
        evaluate_initial_merit(&input).outcome(),
        &MeritComparisonOutcome::Hold(MeritHoldReason::MissingSubstantiveWorkEvidence {
            candidate_id: 2,
        })
    );

    let missing_qualification = MeritCandidate::new(
        3,
        MeritCandidateResponse::Accepted,
        Some(MeritEvidenceReference::new(context, 3, 103)?),
        None,
        Some(MeritPerformanceObservation::new(
            context,
            3,
            final_performance(1, 1)?,
        )?),
    )?;
    let input = MeritComparisonInput::new(context, vec![missing_qualification], None)?;
    assert_eq!(
        evaluate_initial_merit(&input).outcome(),
        &MeritComparisonOutcome::Hold(MeritHoldReason::MissingQualificationEvidence {
            candidate_id: 3,
        })
    );

    let missing_performance = MeritCandidate::new(
        4,
        MeritCandidateResponse::Accepted,
        Some(MeritEvidenceReference::new(context, 4, 104)?),
        Some(MeritEvidenceReference::new(context, 4, 204)?),
        None,
    )?;
    let input = MeritComparisonInput::new(context, vec![missing_performance], None)?;
    assert_eq!(
        evaluate_initial_merit(&input).outcome(),
        &MeritComparisonOutcome::Hold(MeritHoldReason::MissingPerformanceEvaluation {
            candidate_id: 4,
        })
    );
    Ok(())
}

#[test]
fn explicit_decline_requires_exact_revalidation_and_never_changes_its_score()
-> Result<(), Box<dyn Error>> {
    let context = context()?;
    let accepted = candidate(context, 1, MeritCandidateResponse::Accepted, 8, 10)?;
    let declined = MeritCandidate::new(2, MeritCandidateResponse::Declined, None, None, None)?;
    let without_revalidation =
        MeritComparisonInput::new(context, vec![accepted.clone(), declined.clone()], None)?;
    assert_eq!(
        evaluate_initial_merit(&without_revalidation).outcome(),
        &MeritComparisonOutcome::Hold(MeritHoldReason::RevalidationRequired)
    );

    assert!(matches!(
        MeritComparisonInput::new(
            context,
            vec![accepted.clone(), declined.clone()],
            Some(RevalidatedComparison::new(context, vec![2])?),
        ),
        Err(OrganizationGovernanceError::RevalidatedCandidateSetMismatch)
    ));

    let revalidated = RevalidatedComparison::new(context, vec![1])?;
    let input = MeritComparisonInput::new(context, vec![accepted, declined], Some(revalidated))?;
    assert_eq!(
        evaluate_initial_merit(&input).outcome(),
        &MeritComparisonOutcome::Selected { candidate_id: 1 }
    );

    let all_declined = MeritComparisonInput::new(
        context,
        vec![MeritCandidate::new(
            3,
            MeritCandidateResponse::Declined,
            None,
            None,
            None,
        )?],
        Some(RevalidatedComparison::new(context, Vec::new())?),
    )?;
    assert_eq!(
        evaluate_initial_merit(&all_declined).outcome(),
        &MeritComparisonOutcome::Leaderless(MeritLeaderlessReason::NoEligibleCandidate)
    );
    Ok(())
}

#[test]
fn unavailable_or_unanswered_top_candidate_does_not_promote_lower_candidate()
-> Result<(), Box<dyn Error>> {
    let context = context()?;
    let unavailable = candidate(context, 1, MeritCandidateResponse::Unavailable, 9, 10)?;
    let lower = candidate(context, 2, MeritCandidateResponse::Accepted, 8, 10)?;
    let input = MeritComparisonInput::new(context, vec![unavailable, lower], None)?;
    assert_eq!(
        evaluate_initial_merit(&input).outcome(),
        &MeritComparisonOutcome::Hold(MeritHoldReason::TopCandidateUnavailable { candidate_id: 1 })
    );

    let no_response = candidate(context, 3, MeritCandidateResponse::NoResponse, 9, 10)?;
    let lower = candidate(context, 4, MeritCandidateResponse::Accepted, 8, 10)?;
    let input = MeritComparisonInput::new(context, vec![no_response, lower], None)?;
    assert_eq!(
        evaluate_initial_merit(&input).outcome(),
        &MeritComparisonOutcome::Hold(MeritHoldReason::TopCandidateNoResponse { candidate_id: 3 })
    );
    Ok(())
}

#[test]
fn tied_top_with_unresolved_response_holds_without_fallback() -> Result<(), Box<dyn Error>> {
    let context = context()?;
    let cases = [
        (
            MeritCandidateResponse::Unavailable,
            MeritHoldReason::TopCandidateUnavailable { candidate_id: 2 },
        ),
        (
            MeritCandidateResponse::NoResponse,
            MeritHoldReason::TopCandidateNoResponse { candidate_id: 2 },
        ),
    ];

    for (response, expected_reason) in cases {
        let input = MeritComparisonInput::new(
            context,
            vec![
                candidate(context, 1, MeritCandidateResponse::Accepted, 9, 10)?,
                candidate(context, 2, response, 9, 10)?,
                candidate(context, 3, MeritCandidateResponse::Accepted, 8, 10)?,
            ],
            None,
        )?;
        assert_eq!(
            evaluate_initial_merit(&input).outcome(),
            &MeritComparisonOutcome::Hold(expected_reason)
        );
    }
    Ok(())
}

#[test]
fn comparison_rejects_evidence_from_another_context() -> Result<(), Box<dyn Error>> {
    let context = context()?;
    let different_context = MeritComparisonContext::new(1, 1, 2, 3, 4, 5, 7)?;
    let candidate = MeritCandidate::new(
        1,
        MeritCandidateResponse::Accepted,
        Some(MeritEvidenceReference::new(different_context, 1, 101)?),
        None,
        None,
    )?;
    assert!(matches!(
        MeritComparisonInput::new(context, vec![candidate], None),
        Err(OrganizationGovernanceError::CandidateContextMismatch)
    ));
    Ok(())
}

#[test]
fn bounded_merit_comparison_does_not_apply_the_team_roster_cap() -> Result<(), Box<dyn Error>> {
    let context = context()?;
    let candidates: Result<Vec<_>, Box<dyn Error>> = (1_u128..=9)
        .map(|candidate_id| {
            candidate(
                context,
                candidate_id,
                MeritCandidateResponse::Accepted,
                u64::try_from(candidate_id)?,
                10,
            )
        })
        .collect();
    let input = MeritComparisonInput::new(context, candidates?, None)?;
    assert_eq!(
        evaluate_initial_merit(&input).outcome(),
        &MeritComparisonOutcome::Selected { candidate_id: 9 }
    );
    Ok(())
}

#[test]
fn exact_merit_comparison_handles_maximum_u64_weights() -> Result<(), Box<dyn Error>> {
    let context = context()?;
    let input = MeritComparisonInput::new(
        context,
        vec![
            candidate(
                context,
                1,
                MeritCandidateResponse::Accepted,
                u64::MAX - 1,
                u64::MAX,
            )?,
            candidate(
                context,
                2,
                MeritCandidateResponse::Accepted,
                u64::MAX - 2,
                u64::MAX - 1,
            )?,
        ],
        None,
    )?;
    assert_eq!(
        evaluate_initial_merit(&input).outcome(),
        &MeritComparisonOutcome::Selected { candidate_id: 1 }
    );
    Ok(())
}
