//! Real HTTP coverage for Orchestrator-approved Team capacity slots.

use forge_domain::AuthenticatedPrincipal;
use hyper::StatusCode;
use hyper::header::HeaderValue;
use uuid::Uuid;

use heterarchy_orchestration_backend::client::{
    ClientError, OrchestrationHttpClient, ReadbackTarget,
};
use heterarchy_orchestration_backend::wire::leadership::LeadershipQueryRequest;
use heterarchy_orchestration_backend::wire::profiles::WorkspaceProfilesQueryRequest;
use heterarchy_orchestration_backend::wire::team_commands::{
    TeamExpansionSlotV2Request, TeamJoinV2Request, TeamMutationReaderV2Wire, TeamMutationV2Response,
};
use heterarchy_orchestration_backend::wire::team_expansion as wire;
use heterarchy_orchestration_backend::wire::team_expansion_v2 as wire_v2;
use heterarchy_orchestration_backend::wire::teams::{TeamGetQueryRequest, TeamListQueryRequest};

use super::{ForgeHost, MemberTarget, ReadyFixtureMember, TestResult, assert_ready_profile_member};

/// Source-backed actors and the Team whose dispatch reservation fills its
/// initial cap. The bootstrap uses one `DirectToken` principal across distinct
/// logical Sessions; role receipts must still identify the correct actors.
pub(super) struct TeamExpansionFlowContext<'a> {
    pub(super) client: &'a OrchestrationHttpClient,
    pub(super) bearer: &'a HeaderValue,
    pub(super) source: &'a ForgeHost,
    pub(super) creator: &'a MemberTarget,
    pub(super) principal: AuthenticatedPrincipal,
    pub(super) workspace_leader: &'a ReadyFixtureMember,
    pub(super) workspace_leadership_revision: i64,
    pub(super) team_id: Uuid,
    pub(super) team_leader: &'a ReadyFixtureMember,
    pub(super) team_peer: &'a ReadyFixtureMember,
    pub(super) team_leadership_revision: i64,
}

struct ExpansionActors {
    workspace_orchestrator: wire::TeamExpansionActorWire,
    team_leader: wire::TeamExpansionActorWire,
    team_peer: wire::TeamExpansionActorWire,
    target: wire::TeamExpansionActorWire,
}

#[derive(Clone, Copy)]
struct TeamCoordinates {
    team_revision: u64,
    capacity_revision: u64,
    max_members: u8,
}

/// Exercise cap increase, request, V2 approval, cooldown, slot assignment, and
/// Session-bound self-join over the real isolated HTTP services.
///
/// This fixture approves one already registered Workspace Profile that is
/// unassigned across all current Teams. It does not cover `NewProfile` creation
/// or transfer from another Team.
///
/// # Errors
/// Returns an error if the current source-backed leaders, exact cap/approval
/// receipts, unauthorized no-write checks, target binding, or self-join
/// readbacks differ from the durable Operations state.
pub(super) async fn assert_team_expansion_http_flow(
    context: &TeamExpansionFlowContext<'_>,
) -> TestResult {
    assert!(matches!(
        &context.principal,
        AuthenticatedPrincipal::DirectToken(_)
    ));
    // The helper creates a fixture-local Profile/Session and verifies Forge
    // activation; it does not call a provider-backed Profile provisioner.
    let target = stage_result(
        "TeamExpansion target Ready Session/Profile/Forge activation",
        assert_ready_profile_member(
            context.client,
            context.bearer,
            context.principal,
            context.source,
            context.creator,
            "isolated-team-expansion-target",
        )
        .await,
    )?;
    assert_distinct_actor(&target, context.team_leader);
    assert_distinct_actor(&target, context.workspace_leader);
    assert_distinct_actor(context.team_leader, context.workspace_leader);
    assert_distinct_actor(context.team_peer, context.workspace_leader);

    let actors = ExpansionActors {
        workspace_orchestrator: actor(context, context.workspace_leader),
        team_leader: actor(context, context.team_leader),
        team_peer: actor(context, context.team_peer),
        target: actor(context, &target),
    };
    stage_result(
        "TeamExpansion current Workspace and Team leadership readbacks",
        assert_current_leadership(context).await,
    )?;
    stage_result(
        "TeamExpansion existing target Profile and Session source readback",
        assert_existing_workspace_profile_source(context, &target).await,
    )?;
    let initial = stage_result(
        "TeamExpansion initial Team and capacity queries",
        read_team_coordinates(context, &actors.workspace_orchestrator).await,
    )?;
    assert_eq!(initial.max_members, 3, "R starts at its explicitly set cap");
    stage_result(
        "TeamExpansion verify target is unassigned across Workspace Teams",
        assert_target_not_yet_a_member(context, &target, &actors.workspace_orchestrator).await,
    )?;

    let coordinates = stage_result(
        "TeamExpansion bounded-cap denial and Orchestrator capacity update",
        raise_capacity_for_reserved_occupancy(context, &actors, initial).await,
    )?;
    let request = stage_result(
        "TeamExpansion Team Leader request mutation/readback",
        request_expansion(context, &actors, coordinates).await,
    )?;
    let decision = stage_result(
        "TeamExpansion V2 decision, pending anchor, finalization, and lock",
        approve_and_finalize(context, &actors, coordinates, &target, &request).await,
    )?;
    let assignment = stage_result(
        "TeamExpansion unauthorized and target-bound slot assignment",
        assign_target_slot(context, &actors, coordinates, &target, &decision).await,
    )?;
    stage_result(
        "TeamExpansion duplicate slot assignment and exact readback",
        assert_slot_cannot_be_reassigned(
            context,
            &actors,
            coordinates,
            &target,
            &assignment,
            &decision,
        )
        .await,
    )?;
    stage_result(
        "TeamExpansion target v2 self-join and owner-scoped readbacks",
        assert_target_self_join(context, coordinates, &target, &assignment).await,
    )?;
    Ok(())
}

fn actor(
    context: &TeamExpansionFlowContext<'_>,
    member: &ReadyFixtureMember,
) -> wire::TeamExpansionActorWire {
    wire::TeamExpansionActorWire {
        scope: context.creator.scope.clone(),
        activity_id: context.creator.activity_id,
        session_id: member.session_id,
    }
}

fn assert_distinct_actor(first: &ReadyFixtureMember, second: &ReadyFixtureMember) {
    assert_ne!(first.session_id, second.session_id);
    assert_ne!(first.agent_id, second.agent_id);
    assert_ne!(first.participant_id, second.participant_id);
    assert_ne!(first.profile_id, second.profile_id);
}

async fn assert_current_leadership(context: &TeamExpansionFlowContext<'_>) -> TestResult {
    let workspace = stage_result(
        "read Workspace leadership as the source-backed Orchestrator Session",
        context
            .client
            .leadership_view(
                context.bearer,
                &LeadershipQueryRequest {
                    scope: context.creator.scope.clone(),
                    session_id: Some(context.workspace_leader.session_id),
                    activity_id: None,
                    team_id: None,
                    review_after: None,
                    limit: 10,
                },
            )
            .await,
    )?
    .leadership
    .ok_or("source-backed Workspace Orchestrator is absent")?;
    assert_eq!(
        workspace.leader_participant_id,
        Some(context.workspace_leader.participant_id)
    );
    assert!(matches!(workspace.state.as_str(), "stabilizing" | "active"));
    assert_eq!(
        workspace.leadership_revision,
        context.workspace_leadership_revision
    );

    let team = stage_result(
        "read R Team leadership as its source-backed Leader Session",
        context
            .client
            .leadership_view(
                context.bearer,
                &LeadershipQueryRequest {
                    scope: context.creator.scope.clone(),
                    session_id: Some(context.team_leader.session_id),
                    activity_id: None,
                    team_id: Some(context.team_id),
                    review_after: None,
                    limit: 10,
                },
            )
            .await,
    )?
    .leadership
    .ok_or("source-backed R Team Leader is absent")?;
    assert_eq!(
        team.leader_participant_id,
        Some(context.team_leader.participant_id)
    );
    assert!(matches!(team.state.as_str(), "stabilizing" | "active"));
    assert_eq!(team.leadership_revision, context.team_leadership_revision);
    Ok(())
}

async fn read_team_coordinates(
    context: &TeamExpansionFlowContext<'_>,
    actor: &wire::TeamExpansionActorWire,
) -> TestResult<TeamCoordinates> {
    let team = stage_result(
        "read current R Team through v1 Team snapshot endpoint",
        context
            .client
            .team_get(
                context.bearer,
                &TeamGetQueryRequest {
                    scope: context.creator.scope.clone(),
                    session_id: Some(actor.session_id),
                    team_id: context.team_id,
                    request_id: None,
                },
            )
            .await,
    )?
    .team
    .ok_or("receiving Team is absent")?;
    assert_eq!(team.workspace_id, context.creator.scope.workspace_id);
    assert_eq!(team.state, "active");

    let capacity = stage_result(
        "read current R Team capacity",
        context
            .client
            .team_capacity(
                context.bearer,
                &wire::TeamCapacityQueryRequest {
                    actor: actor.clone(),
                    team_id: context.team_id,
                },
            )
            .await,
    )?
    .snapshot
    .ok_or("receiving Team has no explicit capacity")?;
    assert_eq!(capacity.workspace_id, context.creator.scope.workspace_id);
    assert_eq!(capacity.team_id, context.team_id);
    Ok(TeamCoordinates {
        team_revision: team.revision,
        capacity_revision: capacity.revision,
        max_members: capacity.max_members,
    })
}

async fn assert_target_not_yet_a_member(
    context: &TeamExpansionFlowContext<'_>,
    target: &ReadyFixtureMember,
    orchestrator: &wire::TeamExpansionActorWire,
) -> TestResult {
    let teams = stage_result(
        "TeamExpansion complete Workspace Team list for unassigned target proof",
        context
            .client
            .team_list(
                context.bearer,
                &TeamListQueryRequest {
                    scope: context.creator.scope.clone(),
                    session_id: Some(orchestrator.session_id),
                    limit: 64,
                },
            )
            .await,
    )?;
    if teams.truncated {
        return Err("Workspace Team list truncated during target-home check".into());
    }
    assert!(
        teams
            .teams
            .iter()
            .any(|team| team.team_id == context.team_id),
        "receiving Team is absent from the current Workspace list"
    );

    for team in &teams.teams {
        let readback = stage_result(
            &format!(
                "TeamExpansion target absence query team_id={}",
                team.team_id
            ),
            context
                .client
                .team_get(
                    context.bearer,
                    &TeamGetQueryRequest {
                        scope: context.creator.scope.clone(),
                        session_id: Some(orchestrator.session_id),
                        team_id: team.team_id,
                        request_id: None,
                    },
                )
                .await,
        )?;
        if readback.members_truncated {
            return Err(format!(
                "TeamExpansion target-home query truncated team_id={}",
                team.team_id
            )
            .into());
        }
        assert!(
            !readback
                .members
                .iter()
                .any(|member| member.profile_id == Some(target.profile_id)),
            "target Profile already has a Team membership"
        );
        if team.team_id == context.team_id {
            assert_eq!(readback.members.len(), 2);
        }
    }
    Ok(())
}

async fn assert_existing_workspace_profile_source(
    context: &TeamExpansionFlowContext<'_>,
    target: &ReadyFixtureMember,
) -> TestResult {
    let projection = stage_result(
        "read source-backed Workspace Profile and Session mapping",
        context
            .client
            .workspace_profiles(
                context.bearer,
                &WorkspaceProfilesQueryRequest {
                    scope: context.creator.scope.clone(),
                    session_id: Some(context.workspace_leader.session_id),
                },
            )
            .await,
    )?;
    let profile_count = projection
        .profiles
        .iter()
        .filter(|profile| profile.profile.profile_id == target.profile_id)
        .count();
    assert_eq!(
        profile_count, 1,
        "target Profile must belong to this Workspace"
    );
    let target_mappings = projection
        .session_profiles
        .iter()
        .filter(|mapping| mapping.session_id == target.session_id)
        .collect::<Vec<_>>();
    assert_eq!(target_mappings.len(), 1);
    assert_eq!(target_mappings[0].profile_id, target.profile_id);
    assert_ne!(target.participant_id, Uuid::nil());
    // Profile-backed registration derives this stable participant identity
    // from the same Workspace/Profile pair used by WorkspaceParticipation.
    // The V2 decision owner still checks that exact stored pair before commit.
    Ok(())
}

async fn raise_capacity_for_reserved_occupancy(
    context: &TeamExpansionFlowContext<'_>,
    actors: &ExpansionActors,
    initial: TeamCoordinates,
) -> TestResult<TeamCoordinates> {
    assert_eq!(initial.capacity_revision, 1);
    let below_occupancy = capacity_set_intent(
        context,
        &actors.workspace_orchestrator,
        initial,
        2,
        Uuid::now_v7(),
    );
    assert_rejected_status(
        &context
            .client
            .mutate_team_expansion(context.bearer, &below_occupancy)
            .await,
        StatusCode::BAD_REQUEST,
        "capacity below the two members plus the reserved inbound target",
    )?;
    assert!(
        read_expansion(
            context,
            &below_occupancy,
            &actors.workspace_orchestrator,
            "TeamExpansion rejected SetCapacity(2) exact no-write readback",
        )
        .await?
        .is_none(),
        "capacity below committed and held occupancy created a receipt"
    );

    let request_id = Uuid::now_v7();
    let request = capacity_set_intent(
        context,
        &actors.workspace_orchestrator,
        initial,
        4,
        request_id,
    );
    let response = stage_result(
        &format!("TeamExpansion SetCapacity(4) mutation request_id={request_id}"),
        context
            .client
            .mutate_team_expansion(context.bearer, &request)
            .await,
    )?;
    let wire::TeamExpansionMutationResultWire::CapacitySet { receipt } = response.result else {
        return Err("capacity update returned a non-capacity receipt".into());
    };
    assert_eq!(receipt.request_id, request_id);
    assert_eq!(receipt.expected_team_revision, initial.team_revision);
    assert_eq!(receipt.result_team_revision, initial.team_revision + 1);
    assert_eq!(
        receipt.expected_capacity_revision,
        initial.capacity_revision
    );
    assert_eq!(receipt.snapshot.max_members, 4);
    assert_eq!(receipt.snapshot.revision, initial.capacity_revision + 1);
    assert_expansion_actor(&receipt.actor, context, context.workspace_leader);

    let readback = read_expansion(
        context,
        &request,
        &actors.workspace_orchestrator,
        "TeamExpansion SetCapacity(4) exact receipt readback",
    )
    .await?;
    let Some(wire::TeamExpansionMutationResultWire::CapacitySet { receipt: stored }) = readback
    else {
        return Err("exact Team capacity receipt is absent".into());
    };
    assert_eq!(stored.request_id, receipt.request_id);
    assert_eq!(stored.snapshot.team_id, context.team_id);
    assert_eq!(stored.snapshot.max_members, 4);
    assert_eq!(stored.snapshot.revision, receipt.snapshot.revision);
    assert_expansion_actor(&stored.actor, context, context.workspace_leader);

    let current = read_team_coordinates(context, &actors.workspace_orchestrator).await?;
    assert_eq!(current.team_revision, receipt.result_team_revision);
    assert_eq!(current.capacity_revision, receipt.snapshot.revision);
    assert_eq!(current.max_members, 4);
    Ok(current)
}

fn capacity_set_intent(
    context: &TeamExpansionFlowContext<'_>,
    actor: &wire::TeamExpansionActorWire,
    coordinates: TeamCoordinates,
    max_members: u8,
    request_id: Uuid,
) -> wire::TeamExpansionMutationRequest {
    wire::TeamExpansionMutationRequest {
        actor: actor.clone(),
        action: wire::TeamExpansionMutationActionWire::SetCapacity {
            command: wire::TeamCapacitySetAction {
                team_id: context.team_id,
                request_id,
                expected_team_revision: coordinates.team_revision,
                expected_capacity_revision: coordinates.capacity_revision,
                expected_leadership_revision: context.workspace_leadership_revision,
                max_members,
            },
        },
    }
}

async fn request_expansion(
    context: &TeamExpansionFlowContext<'_>,
    actors: &ExpansionActors,
    coordinates: TeamCoordinates,
) -> TestResult<wire::TeamExpansionRequestReceiptWire> {
    let request = request_intent(
        context,
        &actors.team_leader,
        coordinates,
        "one approved member",
    );
    let request_id = match &request.action {
        wire::TeamExpansionMutationActionWire::Request { command } => command.request_id,
        _ => return Err("Team expansion helper produced a non-request intent".into()),
    };
    let response = stage_result(
        &format!("TeamExpansion Team Leader Request mutation request_id={request_id}"),
        context
            .client
            .mutate_team_expansion(context.bearer, &request)
            .await,
    )?;
    let wire::TeamExpansionMutationResultWire::Request { receipt } = response.result else {
        return Err("Team Leader request returned a non-request receipt".into());
    };
    assert_eq!(receipt.team_id, context.team_id);
    assert_eq!(receipt.expected_team_revision, coordinates.team_revision);
    assert_eq!(receipt.capacity_revision, coordinates.capacity_revision);
    assert_eq!(receipt.requested_count, 1);
    assert_eq!(receipt.reason, "one approved member");
    assert_expansion_actor(&receipt.actor, context, context.team_leader);
    let readback = read_expansion(
        context,
        &request,
        &actors.team_leader,
        "TeamExpansion Team Leader Request exact receipt readback",
    )
    .await?;
    let Some(wire::TeamExpansionMutationResultWire::Request { receipt: stored }) = readback else {
        return Err("exact Team expansion request is absent".into());
    };
    assert_eq!(stored.request_id, receipt.request_id);
    assert_eq!(stored.team_id, receipt.team_id);
    assert_eq!(stored.requested_count, receipt.requested_count);
    assert_eq!(
        stored.requester_leadership_revision,
        context.team_leadership_revision
    );
    assert_expansion_actor(&stored.actor, context, context.team_leader);
    Ok(*receipt)
}

fn request_intent(
    context: &TeamExpansionFlowContext<'_>,
    actor: &wire::TeamExpansionActorWire,
    coordinates: TeamCoordinates,
    reason: &str,
) -> wire::TeamExpansionMutationRequest {
    wire::TeamExpansionMutationRequest {
        actor: actor.clone(),
        action: wire::TeamExpansionMutationActionWire::Request {
            command: wire::TeamExpansionRequestAction {
                team_id: context.team_id,
                request_id: Uuid::now_v7(),
                expected_team_revision: coordinates.team_revision,
                expected_capacity_revision: coordinates.capacity_revision,
                expected_leadership_revision: context.team_leadership_revision,
                requested_count: 1,
                reason: reason.to_owned(),
            },
        },
    }
}

async fn approve_and_finalize(
    context: &TeamExpansionFlowContext<'_>,
    actors: &ExpansionActors,
    coordinates: TeamCoordinates,
    target: &ReadyFixtureMember,
    request_receipt: &wire::TeamExpansionRequestReceiptWire,
) -> TestResult<wire::TeamExpansionDecisionReceiptWire> {
    let pending =
        approve_pending_anchor(context, actors, coordinates, target, request_receipt).await?;
    finalize_anchor_and_check_lock(context, actors, coordinates, request_receipt, &pending).await
}

async fn approve_pending_anchor(
    context: &TeamExpansionFlowContext<'_>,
    actors: &ExpansionActors,
    coordinates: TeamCoordinates,
    target: &ReadyFixtureMember,
    request_receipt: &wire::TeamExpansionRequestReceiptWire,
) -> TestResult<wire::TeamExpansionDecisionReceiptWire> {
    assert_fresh_v1_approval_is_denied(context, actors, coordinates, request_receipt).await?;
    assert_team_leader_v2_approval_is_denied(context, actors, coordinates, target, request_receipt)
        .await?;
    record_v2_approval_and_readback(context, actors, coordinates, target, request_receipt).await
}

async fn assert_fresh_v1_approval_is_denied(
    context: &TeamExpansionFlowContext<'_>,
    actors: &ExpansionActors,
    coordinates: TeamCoordinates,
    request_receipt: &wire::TeamExpansionRequestReceiptWire,
) -> TestResult {
    let unsupported_v1_approval = decision_intent(
        context,
        &actors.workspace_orchestrator,
        coordinates,
        request_receipt,
        Uuid::now_v7(),
    );
    assert_conflict(
        &context
            .client
            .mutate_team_expansion(context.bearer, &unsupported_v1_approval)
            .await,
        "fresh V1 approval without a slot plan",
    )?;
    assert!(
        read_expansion(
            context,
            &unsupported_v1_approval,
            &actors.workspace_orchestrator,
            "fresh V1 approval exact no-write readback",
        )
        .await?
        .is_none(),
        "V1 approval without an exact target plan created a decision"
    );
    Ok(())
}

async fn assert_team_leader_v2_approval_is_denied(
    context: &TeamExpansionFlowContext<'_>,
    actors: &ExpansionActors,
    coordinates: TeamCoordinates,
    target: &ReadyFixtureMember,
    request_receipt: &wire::TeamExpansionRequestReceiptWire,
) -> TestResult {
    let unauthorized = decision_v2_intent(
        context,
        &actors.team_leader,
        coordinates,
        request_receipt,
        target,
        Uuid::now_v7(),
    );
    assert_conflict(
        &context
            .client
            .mutate_team_expansion_decision_v2(context.bearer, &unauthorized)
            .await,
        "Team Leader V2 approval attempt",
    )?;
    let readback = read_decision_v2(
        context,
        &unauthorized,
        &actors.team_leader,
        "TeamExpansion denied Team Leader V2 approval exact no-write readback",
    )
    .await?;
    assert!(
        readback.is_none(),
        "unauthorized V2 approval created a durable decision"
    );
    Ok(())
}

async fn record_v2_approval_and_readback(
    context: &TeamExpansionFlowContext<'_>,
    actors: &ExpansionActors,
    coordinates: TeamCoordinates,
    target: &ReadyFixtureMember,
    request_receipt: &wire::TeamExpansionRequestReceiptWire,
) -> TestResult<wire::TeamExpansionDecisionReceiptWire> {
    let approval = decision_v2_intent(
        context,
        &actors.workspace_orchestrator,
        coordinates,
        request_receipt,
        target,
        Uuid::now_v7(),
    );
    let response = stage_result(
        &format!(
            "TeamExpansion Orchestrator V2 approval mutation decision_id={}",
            approval.command.decision_id
        ),
        context
            .client
            .mutate_team_expansion_decision_v2(context.bearer, &approval)
            .await,
    )?;
    assert_eq!(response.request, approval);
    assert_eq!(response.receipt.slot_plan, approval.slot_plan);
    let pending = response.receipt.decision;
    assert_pending_anchor(
        &pending,
        context,
        request_receipt,
        approval.command.decision_id,
    );
    assert_expansion_actor(&request_receipt.actor, context, context.team_leader);
    assert_expansion_actor(&pending.actor, context, context.workspace_leader);
    assert_ne!(
        request_receipt.actor.participant_id,
        pending.actor.participant_id
    );

    let replay = stage_result(
        "TeamExpansion exact V2 decision replay with frozen target plan",
        context
            .client
            .mutate_team_expansion_decision_v2(context.bearer, &approval)
            .await,
    )?;
    assert_eq!(replay.request, approval);
    assert_eq!(replay.receipt.slot_plan, approval.slot_plan);
    let mut expected_replay = pending.clone();
    expected_replay.replayed = true;
    assert_eq!(replay.receipt.decision, expected_replay);
    assert_expansion_actor(
        &replay.receipt.decision.actor,
        context,
        context.workspace_leader,
    );

    let stored = read_decision_v2(
        context,
        &approval,
        &actors.workspace_orchestrator,
        "TeamExpansion exact V2 target-plan decision readback",
    )
    .await?;
    let stored = stored.ok_or("approved V2 decision readback is absent")?;
    assert_eq!(stored.slot_plan, approval.slot_plan);
    assert_eq!(stored.decision, pending);
    assert_pending_anchor(
        &stored.decision,
        context,
        request_receipt,
        approval.command.decision_id,
    );
    assert_expansion_actor(&stored.decision.actor, context, context.workspace_leader);
    Ok(stored.decision)
}

async fn finalize_anchor_and_check_lock(
    context: &TeamExpansionFlowContext<'_>,
    actors: &ExpansionActors,
    coordinates: TeamCoordinates,
    request_receipt: &wire::TeamExpansionRequestReceiptWire,
    pending: &wire::TeamExpansionDecisionReceiptWire,
) -> TestResult<wire::TeamExpansionDecisionReceiptWire> {
    let finalize = wire::TeamExpansionMutationRequest {
        actor: actors.workspace_orchestrator.clone(),
        action: wire::TeamExpansionMutationActionWire::FinalizeAnchor {
            command: wire::TeamExpansionFinalizeAnchorAction {
                request_id: request_receipt.request_id,
                decision_id: pending.decision_id,
                expected_leadership_revision: context.workspace_leadership_revision,
            },
        },
    };
    let response = stage_result(
        &format!(
            "TeamExpansion cooldown anchor finalization decision_id={}",
            pending.decision_id
        ),
        context
            .client
            .mutate_team_expansion(context.bearer, &finalize)
            .await,
    )?;
    let wire::TeamExpansionMutationResultWire::Decision { receipt: finalized } = response.result
    else {
        return Err("cooldown finalization returned a non-decision receipt".into());
    };
    assert_finalized_anchor(&finalized, context, request_receipt, pending.decision_id)?;
    let final_readback = read_expansion(
        context,
        &finalize,
        &actors.workspace_orchestrator,
        "TeamExpansion finalized cooldown exact decision readback",
    )
    .await?;
    let Some(wire::TeamExpansionMutationResultWire::Decision {
        receipt: stored_final,
    }) = final_readback
    else {
        return Err("finalized cooldown decision readback is absent".into());
    };
    assert_finalized_anchor(&stored_final, context, request_receipt, pending.decision_id)?;

    let blocked_request = request_intent(
        context,
        &actors.team_leader,
        coordinates,
        "request blocked by the confirmed cooldown lock",
    );
    assert_conflict(
        &context
            .client
            .mutate_team_expansion(context.bearer, &blocked_request)
            .await,
        "request inside the confirmed cooldown lock",
    )?;
    assert!(
        read_expansion(
            context,
            &blocked_request,
            &actors.team_leader,
            "TeamExpansion cooldown-blocked Request exact no-write readback",
        )
        .await?
        .is_none(),
        "cooldown-blocked request created a durable receipt"
    );
    Ok(*finalized)
}

fn decision_intent(
    context: &TeamExpansionFlowContext<'_>,
    actor: &wire::TeamExpansionActorWire,
    coordinates: TeamCoordinates,
    request: &wire::TeamExpansionRequestReceiptWire,
    decision_id: Uuid,
) -> wire::TeamExpansionMutationRequest {
    wire::TeamExpansionMutationRequest {
        actor: actor.clone(),
        action: wire::TeamExpansionMutationActionWire::Decide {
            command: wire::TeamExpansionDecisionAction {
                team_id: context.team_id,
                request_id: request.request_id,
                decision_id,
                expected_team_revision: coordinates.team_revision,
                expected_capacity_revision: coordinates.capacity_revision,
                expected_leadership_revision: context.workspace_leadership_revision,
                expected_requester_leadership_revision: request.requester_leadership_revision,
                decision: wire::TeamExpansionDecisionWire::Approve { approved_count: 1 },
            },
        },
    }
}

fn decision_v2_intent(
    context: &TeamExpansionFlowContext<'_>,
    actor: &wire::TeamExpansionActorWire,
    coordinates: TeamCoordinates,
    request: &wire::TeamExpansionRequestReceiptWire,
    target: &ReadyFixtureMember,
    decision_id: Uuid,
) -> wire_v2::TeamExpansionDecisionV2Request {
    wire_v2::TeamExpansionDecisionV2Request {
        actor: actor.clone(),
        command: wire::TeamExpansionDecisionAction {
            team_id: context.team_id,
            request_id: request.request_id,
            decision_id,
            expected_team_revision: coordinates.team_revision,
            expected_capacity_revision: coordinates.capacity_revision,
            expected_leadership_revision: context.workspace_leadership_revision,
            expected_requester_leadership_revision: request.requester_leadership_revision,
            decision: wire::TeamExpansionDecisionWire::Approve { approved_count: 1 },
        },
        slot_plan: vec![wire_v2::TeamExpansionApprovalSlotV2Wire {
            ordinal: 0,
            target: wire_v2::TeamExpansionApprovalTargetV2Wire {
                profile_id: target.profile_id,
                // Profile-backed registration uses the stable participant ID
                // as the WorkspaceParticipation ID. The owner validates this
                // exact stored Profile/participation pair before committing.
                participation_id: target.participant_id,
            },
        }],
    }
}

fn assert_pending_anchor(
    receipt: &wire::TeamExpansionDecisionReceiptWire,
    context: &TeamExpansionFlowContext<'_>,
    request: &wire::TeamExpansionRequestReceiptWire,
    decision_id: Uuid,
) {
    assert_eq!(receipt.decision_id, decision_id);
    assert_eq!(receipt.request_id, request.request_id);
    assert_eq!(receipt.team_id, context.team_id);
    assert_eq!(
        receipt.state,
        wire::TeamExpansionDecisionStateWire::ApprovedPendingAnchor
    );
    assert_eq!(receipt.approved_count, 1);
    assert_eq!(receipt.cooldown_started_at_ms, None);
    assert_eq!(receipt.cooldown_until_ms, None);
    assert_eq!(receipt.finalized_by, None);
    assert_eq!(receipt.finalized_by_leadership_revision, None);
}

fn assert_finalized_anchor(
    receipt: &wire::TeamExpansionDecisionReceiptWire,
    context: &TeamExpansionFlowContext<'_>,
    request: &wire::TeamExpansionRequestReceiptWire,
    decision_id: Uuid,
) -> TestResult {
    assert_eq!(receipt.decision_id, decision_id);
    assert_eq!(receipt.request_id, request.request_id);
    assert_eq!(receipt.team_id, context.team_id);
    assert_eq!(
        receipt.state,
        wire::TeamExpansionDecisionStateWire::Approved
    );
    assert_eq!(receipt.approved_count, 1);
    let started = receipt
        .cooldown_started_at_ms
        .ok_or("finalized cooldown start time is absent")?;
    let until = receipt
        .cooldown_until_ms
        .ok_or("finalized cooldown end time is absent")?;
    assert_eq!(until.checked_sub(started), Some(60 * 60 * 1_000_i64));
    assert_eq!(
        receipt.finalized_by_leadership_revision,
        Some(context.workspace_leadership_revision)
    );
    assert_expansion_actor(
        receipt
            .finalized_by
            .as_ref()
            .ok_or("cooldown finalizer is absent")?,
        context,
        context.workspace_leader,
    );
    Ok(())
}

async fn assign_target_slot(
    context: &TeamExpansionFlowContext<'_>,
    actors: &ExpansionActors,
    coordinates: TeamCoordinates,
    target: &ReadyFixtureMember,
    decision: &wire::TeamExpansionDecisionReceiptWire,
) -> TestResult<wire::TeamExpansionSlotAssignmentReceiptWire> {
    let unauthorized = slot_assignment_intent(
        context,
        &actors.team_peer,
        &actors.target,
        coordinates,
        decision.decision_id,
        Uuid::now_v7(),
    );
    assert_conflict(
        &context
            .client
            .mutate_team_expansion(context.bearer, &unauthorized)
            .await,
        "Team peer slot assignment attempt",
    )?;
    assert!(
        read_expansion(
            context,
            &unauthorized,
            &actors.workspace_orchestrator,
            "TeamExpansion peer assignment exact no-write readback as authorized O",
        )
        .await?
        .is_none(),
        "unauthorized slot assignment created a durable receipt"
    );

    let assignment_request_id = Uuid::now_v7();
    let assignment = slot_assignment_intent(
        context,
        &actors.workspace_orchestrator,
        &actors.target,
        coordinates,
        decision.decision_id,
        assignment_request_id,
    );
    let response = stage_result(
        &format!("TeamExpansion Orchestrator slot assignment request_id={assignment_request_id}"),
        context
            .client
            .mutate_team_expansion(context.bearer, &assignment)
            .await,
    )?;
    let wire::TeamExpansionMutationResultWire::SlotAssignment { receipt } = response.result else {
        return Err("Orchestrator slot selection returned another receipt type".into());
    };
    assert_eq!(receipt.request_id, assignment_request_id);
    assert_eq!(receipt.approval_id, decision.decision_id);
    assert_eq!(receipt.ordinal, 0);
    assert_eq!(receipt.team_id, context.team_id);
    assert_eq!(receipt.profile_id, target.profile_id);
    assert_eq!(receipt.target.session_id, target.session_id);
    assert_expansion_actor(&receipt.actor, context, context.workspace_leader);
    assert_expansion_actor(&receipt.target, context, target);
    let readback = read_expansion(
        context,
        &assignment,
        &actors.target,
        "TeamExpansion target-bound SlotAssignment exact readback",
    )
    .await?;
    let Some(wire::TeamExpansionMutationResultWire::SlotAssignment { receipt: stored }) = readback
    else {
        return Err("target-bound slot assignment readback is absent".into());
    };
    assert_eq!(stored.request_id, receipt.request_id);
    assert_eq!(stored.approval_id, receipt.approval_id);
    assert_eq!(stored.ordinal, receipt.ordinal);
    assert_eq!(stored.target, receipt.target);
    Ok(*receipt)
}

fn slot_assignment_intent(
    context: &TeamExpansionFlowContext<'_>,
    actor: &wire::TeamExpansionActorWire,
    target: &wire::TeamExpansionActorWire,
    coordinates: TeamCoordinates,
    decision_id: Uuid,
    request_id: Uuid,
) -> wire::TeamExpansionMutationRequest {
    wire::TeamExpansionMutationRequest {
        actor: actor.clone(),
        action: wire::TeamExpansionMutationActionWire::AssignSlot {
            command: wire::TeamExpansionSlotAssignmentAction {
                team_id: context.team_id,
                approval_id: decision_id,
                ordinal: 0,
                request_id,
                expected_team_revision: coordinates.team_revision,
                expected_capacity_revision: coordinates.capacity_revision,
                expected_leadership_revision: context.workspace_leadership_revision,
                target: wire::TeamExpansionTargetWire {
                    activity_id: target.activity_id,
                    session_id: target.session_id,
                },
            },
        },
    }
}

async fn assert_target_self_join(
    context: &TeamExpansionFlowContext<'_>,
    coordinates: TeamCoordinates,
    target: &ReadyFixtureMember,
    assignment: &wire::TeamExpansionSlotAssignmentReceiptWire,
) -> TestResult {
    let (request, response, recovered_receipt) =
        join_target_v2(context, coordinates, target, assignment).await?;
    assert_v2_join_source_readback(context, &request, target, &response, recovered_receipt).await?;
    assert_current_joined_team(context, target, assignment, &response).await
}

async fn join_target_v2(
    context: &TeamExpansionFlowContext<'_>,
    coordinates: TeamCoordinates,
    target: &ReadyFixtureMember,
    assignment: &wire::TeamExpansionSlotAssignmentReceiptWire,
) -> TestResult<(
    TeamJoinV2Request,
    TeamMutationV2Response,
    Option<TeamMutationV2Response>,
)> {
    let request = TeamJoinV2Request {
        scope: context.creator.scope.clone(),
        request_id: Uuid::now_v7(),
        team_id: context.team_id,
        activity_id: context.creator.activity_id,
        session_id: target.session_id,
        expected_team_revision: coordinates.team_revision,
        role: "member".to_owned(),
        expansion_slot: TeamExpansionSlotV2Request {
            decision_id: assignment.approval_id,
            ordinal: assignment.ordinal,
        },
    };
    let (response, recovered_receipt) = submit_join_v2_or_recover(context, &request).await?;
    assert_join_v2_response(
        context,
        coordinates.team_revision,
        &request,
        target,
        assignment,
        &response,
        recovered_receipt.is_some(),
    )?;
    Ok((request, response, recovered_receipt))
}

async fn submit_join_v2_or_recover(
    context: &TeamExpansionFlowContext<'_>,
    request: &TeamJoinV2Request,
) -> TestResult<(TeamMutationV2Response, Option<TeamMutationV2Response>)> {
    match context.client.join_team_v2(context.bearer, request).await {
        Ok(response) => Ok((response, None)),
        Err(ClientError::ReadbackRequired { readback }) => {
            match readback {
                ReadbackTarget::TeamJoinV2Mutation(original) if original.as_ref() == request => {}
                other => {
                    return Err(format!(
                        "TeamJoinV2 ReadbackRequired identity mismatch request_id={} target={other:?}",
                        request.request_id
                    )
                    .into());
                }
            }
            let recovered = stage_result(
                &format!(
                    "TeamJoinV2 COMMITTED_BUT_UNCONFIRMED owner source readback request_id={} decision_id={} target_session_id={}",
                    request.request_id,
                    request.expansion_slot.decision_id,
                    request.session_id
                ),
                context
                    .client
                    .team_join_mutation_readback_v2(
                        context.bearer,
                        request,
                        TeamMutationReaderV2Wire {
                            activity_id: request.activity_id,
                            session_id: request.session_id,
                        },
                    )
                    .await,
            )?
            .ok_or_else(|| {
                format!(
                    "TeamJoinV2 required source readback returned None request_id={} decision_id={} target_session_id={}",
                    request.request_id,
                    request.expansion_slot.decision_id,
                    request.session_id
                )
            })?;
            Ok((recovered.clone(), Some(recovered)))
        }
        Err(error) => Err(format!(
            "TeamJoinV2 mutation request_id={} target_session_id={}: {error:?}",
            request.request_id, request.session_id
        )
        .into()),
    }
}

fn assert_join_v2_response(
    context: &TeamExpansionFlowContext<'_>,
    expected_team_revision: u64,
    request: &TeamJoinV2Request,
    target: &ReadyFixtureMember,
    assignment: &wire::TeamExpansionSlotAssignmentReceiptWire,
    response: &TeamMutationV2Response,
    recovered: bool,
) -> TestResult {
    assert_eq!(response.mutation.replayed, recovered);
    assert_eq!(response.mutation.team.team_id, context.team_id);
    assert_eq!(response.mutation.team.revision, expected_team_revision + 1);
    assert_eq!(
        response.mutation.expansion_slot,
        Some(request.expansion_slot)
    );
    let join_source = response
        .mutation
        .expansion_join_source
        .as_ref()
        .ok_or("successful TeamJoinV2 response omitted its source actor/slot proof")?;
    assert_eq!(join_source.activity_id, request.activity_id);
    assert_eq!(join_source.agent_id, target.agent_id);
    assert_eq!(join_source.session_id, request.session_id);
    assert_eq!(join_source.decision_id, request.expansion_slot.decision_id);
    assert_eq!(join_source.ordinal, request.expansion_slot.ordinal);
    assert_eq!(join_source.role, request.role);
    let joined = response
        .mutation
        .membership
        .as_ref()
        .ok_or("target self-join receipt lacks membership")?;
    assert_eq!(joined.profile_id, Some(target.profile_id));
    assert_eq!(joined.participation_id, assignment.participation_id);
    assert_eq!(joined.state, "joined");
    assert_eq!(joined.role.as_deref(), Some("member"));
    Ok(())
}

async fn assert_v2_join_source_readback(
    context: &TeamExpansionFlowContext<'_>,
    request: &TeamJoinV2Request,
    target: &ReadyFixtureMember,
    response: &TeamMutationV2Response,
    recovered_receipt: Option<TeamMutationV2Response>,
) -> TestResult {
    let source_receipt = match recovered_receipt {
        Some(receipt) => receipt,
        None => stage_result(
            &format!(
                "TeamJoinV2 owner receipt readback request_id={}",
                request.request_id
            ),
            context
                .client
                .team_join_mutation_readback_v2(
                    context.bearer,
                    request,
                    TeamMutationReaderV2Wire {
                        activity_id: request.activity_id,
                        session_id: request.session_id,
                    },
                )
                .await,
        )?
        .ok_or_else(|| {
            format!(
                "TeamJoinV2 exact owner receipt is absent request_id={} decision_id={} target_session_id={}",
                request.request_id,
                request.expansion_slot.decision_id,
                request.session_id
            )
        })?,
    };
    assert!(source_receipt.mutation.replayed);
    assert_eq!(source_receipt.mutation.team, response.mutation.team);
    assert_eq!(
        source_receipt.mutation.membership,
        response.mutation.membership
    );
    assert_eq!(
        source_receipt.mutation.expansion_slot,
        Some(request.expansion_slot)
    );
    assert!(source_receipt.mutation.capacity.is_none());
    assert!(source_receipt.mutation.initial_roster_digest_v1.is_none());
    let join_source = source_receipt
        .mutation
        .expansion_join_source
        .as_ref()
        .ok_or("owner-scoped v2 join receipt omitted slot source")?;
    assert_eq!(join_source.activity_id, request.activity_id);
    assert_eq!(join_source.agent_id, target.agent_id);
    assert_eq!(join_source.session_id, request.session_id);
    assert_eq!(join_source.decision_id, request.expansion_slot.decision_id);
    assert_eq!(join_source.ordinal, request.expansion_slot.ordinal);
    Ok(())
}

async fn assert_current_joined_team(
    context: &TeamExpansionFlowContext<'_>,
    target: &ReadyFixtureMember,
    assignment: &wire::TeamExpansionSlotAssignmentReceiptWire,
    response: &TeamMutationV2Response,
) -> TestResult {
    let readback = stage_result(
        &format!(
            "TeamJoinV2 current Team snapshot request_id={}",
            response.request_id
        ),
        context
            .client
            .team_get(
                context.bearer,
                &TeamGetQueryRequest {
                    scope: context.creator.scope.clone(),
                    session_id: Some(target.session_id),
                    team_id: context.team_id,
                    request_id: None,
                },
            )
            .await,
    )?;
    assert_eq!(readback.team.as_ref(), Some(&response.mutation.team));
    assert_eq!(readback.members.len(), 3);
    assert!(!readback.members_truncated);
    assert!(readback.members.iter().any(|member| {
        member.profile_id == Some(target.profile_id)
            && member.participation_id == assignment.participation_id
            && member.state == "joined"
    }));
    Ok(())
}

async fn assert_slot_cannot_be_reassigned(
    context: &TeamExpansionFlowContext<'_>,
    actors: &ExpansionActors,
    coordinates: TeamCoordinates,
    target: &ReadyFixtureMember,
    assignment: &wire::TeamExpansionSlotAssignmentReceiptWire,
    decision: &wire::TeamExpansionDecisionReceiptWire,
) -> TestResult {
    let reused = slot_assignment_intent(
        context,
        &actors.workspace_orchestrator,
        &actors.target,
        coordinates,
        decision.decision_id,
        Uuid::now_v7(),
    );
    assert_conflict(
        &context
            .client
            .mutate_team_expansion(context.bearer, &reused)
            .await,
        "second assignment of one approved slot",
    )?;
    let original_intent = slot_assignment_intent(
        context,
        &actors.workspace_orchestrator,
        &actors.target,
        coordinates,
        decision.decision_id,
        assignment.request_id,
    );
    let readback = read_expansion(
        context,
        &original_intent,
        &actors.target,
        "TeamExpansion duplicate-slot denial original source receipt readback",
    )
    .await?;
    let Some(wire::TeamExpansionMutationResultWire::SlotAssignment { receipt: stored }) = readback
    else {
        return Err("original target-bound slot disappeared after reuse denial".into());
    };
    assert_eq!(stored.request_id, assignment.request_id);
    assert_eq!(stored.approval_id, assignment.approval_id);
    assert_eq!(stored.ordinal, assignment.ordinal);
    assert_eq!(stored.profile_id, target.profile_id);
    assert_eq!(stored.target.session_id, target.session_id);
    Ok(())
}

async fn read_expansion(
    context: &TeamExpansionFlowContext<'_>,
    intent: &wire::TeamExpansionMutationRequest,
    current_actor: &wire::TeamExpansionActorWire,
    stage: &str,
) -> TestResult<Option<wire::TeamExpansionMutationResultWire>> {
    let response = stage_result(
        stage,
        context
            .client
            .team_expansion_readback(
                context.bearer,
                &wire::TeamExpansionReadbackRequest {
                    intent: intent.clone(),
                    current_actor: current_actor.clone(),
                },
            )
            .await,
    )?;
    Ok(response.result)
}

async fn read_decision_v2(
    context: &TeamExpansionFlowContext<'_>,
    intent: &wire_v2::TeamExpansionDecisionV2Request,
    current_actor: &wire::TeamExpansionActorWire,
    stage: &str,
) -> TestResult<Option<wire_v2::TeamExpansionDecisionV2ReceiptWire>> {
    let request = wire_v2::TeamExpansionDecisionV2ReadbackRequest {
        intent: intent.clone(),
        current_actor: current_actor.clone(),
    };
    let response = stage_result(
        stage,
        context
            .client
            .team_expansion_decision_v2_readback(context.bearer, &request)
            .await,
    )?;
    assert_eq!(response.request, request);
    Ok(response.result)
}

fn stage_result<T, E: std::fmt::Debug>(stage: &str, result: Result<T, E>) -> TestResult<T> {
    result.map_err(|error| format!("{stage}: {error:?}").into())
}

fn assert_expansion_actor(
    receipt: &wire::TeamExpansionActorReceiptWire,
    context: &TeamExpansionFlowContext<'_>,
    member: &ReadyFixtureMember,
) {
    assert_eq!(receipt.activity_id, context.creator.activity_id);
    assert_eq!(receipt.agent_id, member.agent_id);
    assert_eq!(receipt.session_id, member.session_id);
    assert_eq!(receipt.profile_id, member.profile_id);
    assert_eq!(receipt.participant_id, member.participant_id);
}

fn assert_conflict<T: std::fmt::Debug>(
    result: &Result<T, ClientError>,
    operation: &str,
) -> TestResult {
    match result {
        Err(ClientError::Rejected { status, .. }) if *status == StatusCode::CONFLICT.as_u16() => {
            Ok(())
        }
        _ => Err(format!("{operation} did not return HTTP 409: {result:?}").into()),
    }
}

fn assert_rejected_status<T: std::fmt::Debug>(
    result: &Result<T, ClientError>,
    expected: StatusCode,
    operation: &str,
) -> TestResult {
    match result {
        Err(ClientError::Rejected { status, .. }) if *status == expected.as_u16() => Ok(()),
        _ => Err(format!(
            "{operation} did not return HTTP {}: {result:?}",
            expected.as_u16()
        )
        .into()),
    }
}
