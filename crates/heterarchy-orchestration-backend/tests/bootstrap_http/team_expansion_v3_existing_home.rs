//! Real isolated HTTP proof for approval of an existing Home Team member.
//!
//! Isolated HTTP observation of the exact existing-Home v3 approval, consent,
//! and opt-in PREPARING stages. It does not execute a Source cut or Home cutover.
//! All fixture Sessions share one Direct Token principal.

use std::collections::{BTreeMap, BTreeSet};

use forge_domain::AuthenticatedPrincipal;
use hyper::StatusCode;
use hyper::header::HeaderValue;
use uuid::Uuid;

use heterarchy_orchestration_backend::client::{ClientError, OrchestrationHttpClient};
use heterarchy_orchestration_backend::wire::leadership::LeadershipQueryRequest;
use heterarchy_orchestration_backend::wire::leadership_lifecycle::{
    LeadershipAdvanceRequest, LeadershipLifecycleTarget,
};
use heterarchy_orchestration_backend::wire::member_list::MemberListRequest;
use heterarchy_orchestration_backend::wire::participation::{
    ParticipantTarget, PresenceQueryRequest, PresenceTouchRequest,
};
use heterarchy_orchestration_backend::wire::profiles::WorkspaceProfilesQueryRequest;
use heterarchy_orchestration_backend::wire::team_expansion as expansion_wire;
use heterarchy_orchestration_backend::wire::team_expansion;
use heterarchy_orchestration_backend::wire::team_expansion_v2 as v2_wire;
use heterarchy_orchestration_backend::wire::team_expansion_v3 as v3_wire;
use heterarchy_orchestration_backend::wire::team_permanent_transfer as transfer_wire;
use heterarchy_orchestration_backend::wire::teams::{TeamGetQueryRequest, TeamListQueryRequest};

use super::{ForgeHost, MemberTarget, ReadyFixtureMember, TestResult, assert_ready_profile_member};

const WORKSPACE_LOGICAL_AGENT_CAP: usize = 12;

/// Source-backed actors and exact source/destination Teams for one v3 approval.
pub(super) struct ExistingHomeV3FlowContext<'a> {
    pub(super) client: &'a OrchestrationHttpClient,
    pub(super) backend_address: std::net::SocketAddr,
    pub(super) bearer: &'a HeaderValue,
    pub(super) source: &'a ForgeHost,
    pub(super) creator: &'a MemberTarget,
    pub(super) principal: AuthenticatedPrincipal,
    pub(super) workspace_leader: &'a ReadyFixtureMember,
    pub(super) workspace_leadership_revision: i64,
    pub(super) home_team_id: Uuid,
    pub(super) home_member: &'a ReadyFixtureMember,
    pub(super) source_home_leader: &'a ReadyFixtureMember,
    pub(super) source_home_leadership_revision: i64,
    pub(super) destination_team_id: Uuid,
    pub(super) destination_leader: &'a ReadyFixtureMember,
    pub(super) destination_leadership_revision: i64,
    pub(super) transfer_exercise: super::TransferExercise,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkspacePopulation {
    member_count: usize,
    profile_ids: BTreeSet<Uuid>,
    current_member_profiles: BTreeSet<Uuid>,
    occupancy_keys: BTreeSet<OccupancyKey>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum OccupancyKey {
    Profile(Uuid),
    Participant(Uuid),
}

struct TeamSnapshot {
    team: heterarchy_orchestration_backend::wire::teams::TeamWire,
    members: Vec<heterarchy_orchestration_backend::wire::teams::TeamMemberWire>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExpansionCoordinates {
    team_revision: u64,
    capacity_revision: u64,
    max_members: u8,
}

#[path = "team_expansion_v3_existing_home/home_source.rs"]
pub(crate) mod home_source;
#[path = "team_expansion_v3_existing_home/permanent_transfer.rs"]
mod permanent_transfer;
#[path = "team_expansion_v3_existing_home/preparation.rs"]
mod preparation;

/// Approves an exact existing Home Team target while the isolated Workspace is
/// full through source-backed HTTP registrations.
///
/// The Operations tests own the persistent capacity equation. This helper
/// builds the same Workspace identity categories from owner-visible queries:
/// stable Profile IDs and participant IDs for members without a Profile link.
/// It requires twelve unique keys before and after the v3 decision, while the
/// source Home and destination Team remain unchanged.
///
/// The parent calls this after R's V2 slot has been joined, so that earlier
/// hold is consumed and H has no earlier expansion hold. The sole new H hold
/// uses X's already counted Profile key.
///
/// # Errors
/// Returns an error if the current Orchestrator, Team Leader request, exact
/// Home member revision, v3 no-write rejections, decision receipt, or roster
/// and Workspace readbacks differ from their source-backed values.
pub(super) async fn assert_existing_home_v3_approval_http_flow(
    context: &ExistingHomeV3FlowContext<'_>,
) -> TestResult {
    assert_distinct_fixture_actors(context);
    let added = fill_workspace_to_logical_cap(context).await?;
    assert_unassigned_fixture_profiles(context, &added).await?;
    assert_current_authorities(context).await?;

    let population_before = read_workspace_population(context).await?;
    assert_eq!(
        population_before.occupancy_keys.len(),
        WORKSPACE_LOGICAL_AGENT_CAP
    );
    if !population_before
        .profile_ids
        .contains(&context.home_member.profile_id)
    {
        return Err("Home-transfer target has no existing Workspace Profile key".into());
    }
    let home_before = read_team_snapshot(context, context.home_team_id).await?;
    let home_member_before = exact_home_member(context, &home_before)?;
    exact_profile_member(&home_before, context.source_home_leader.profile_id)?;
    assert_eq!(
        home_member_before.profile_id,
        Some(context.home_member.profile_id)
    );
    assert_eq!(home_member_before.state, "joined");
    assert!(
        home_member_before
            .role
            .as_ref()
            .is_some_and(|role| !role.is_empty())
    );

    let destination_before = read_team_snapshot(context, context.destination_team_id).await?;
    assert!(
        !destination_before
            .members
            .iter()
            .any(|member| { member.profile_id == Some(context.home_member.profile_id) })
    );
    let (destination_before, coordinates) =
        ensure_destination_capacity_for_one_hold(context, destination_before).await?;
    assert_current_authorities(context).await?;
    let workspace_leader_destination_membership =
        exact_profile_member(&destination_before, context.workspace_leader.profile_id)?;
    assert!(usize::from(coordinates.max_members) > destination_before.members.len());

    let request = request_from_destination(context, &destination_before, &coordinates).await?;
    assert_v3_wrong_role_has_no_write(context, &request, home_member_before, &coordinates).await?;
    assert_rosters_and_population_unchanged(
        context,
        &home_before,
        &destination_before,
        &coordinates,
        &population_before,
        "wrong-role v3 approval",
    )
    .await?;
    assert_v3_mismatched_home_revision_has_no_write(
        context,
        &request,
        home_member_before,
        &coordinates,
    )
    .await?;
    assert_rosters_and_population_unchanged(
        context,
        &home_before,
        &destination_before,
        &coordinates,
        &population_before,
        "mismatched-Home-revision v3 approval",
    )
    .await?;

    let intent = v3_intent(context, &request, home_member_before, &coordinates);
    let approved = approve_and_read_exact_v3(context, &intent).await?;
    assert_v3_receipt(
        &approved,
        context,
        &intent,
        home_member_before,
        workspace_leader_destination_membership,
    )?;
    assert_v2_downgrade_is_rejected(context, &intent, &approved).await?;
    assert_rosters_and_population_unchanged(
        context,
        &home_before,
        &destination_before,
        &coordinates,
        &population_before,
        "approved existing-Home v3 decision",
    )
    .await?;
    Box::pin(permanent_transfer::assert_http_flow(
        context, &request, &intent, &approved,
    ))
    .await
}

fn assert_distinct_fixture_actors(context: &ExistingHomeV3FlowContext<'_>) {
    assert!(matches!(
        context.principal,
        AuthenticatedPrincipal::DirectToken(_)
    ));
    assert_ne!(
        context.workspace_leader.session_id,
        context.destination_leader.session_id
    );
    assert_ne!(
        context.home_member.profile_id,
        context.workspace_leader.profile_id
    );
    assert_ne!(
        context.home_member.profile_id,
        context.destination_leader.profile_id
    );
    assert_ne!(
        context.home_member.profile_id,
        context.source_home_leader.profile_id
    );
    assert_ne!(
        context.destination_leader.profile_id,
        context.source_home_leader.profile_id
    );
}

async fn fill_workspace_to_logical_cap(
    context: &ExistingHomeV3FlowContext<'_>,
) -> TestResult<Vec<ReadyFixtureMember>> {
    let mut population = read_workspace_population(context).await?;
    if population.occupancy_keys.len() > WORKSPACE_LOGICAL_AGENT_CAP {
        return Err("isolated Workspace already exceeds its logical Agent cap".into());
    }
    let mut added = Vec::new();
    while population.occupancy_keys.len() < WORKSPACE_LOGICAL_AGENT_CAP {
        let stable_name = format!("isolated-v3-capacity-{}", Uuid::now_v7());
        let member = assert_ready_profile_member(
            context.client,
            context.bearer,
            context.principal,
            context.source,
            context.creator,
            &stable_name,
        )
        .await
        .map_err(|error| format!("source-backed capacity Profile registration: {error:?}"))?;
        if population.profile_ids.contains(&member.profile_id) {
            return Err("capacity fixture reused a registered Profile identity".into());
        }
        added.push(member);
        population = read_workspace_population(context).await?;
        if population.occupancy_keys.len() > WORKSPACE_LOGICAL_AGENT_CAP {
            return Err(
                "source-backed registration exceeded the Workspace logical Agent cap".into(),
            );
        }
    }
    assert_eq!(population.occupancy_keys.len(), WORKSPACE_LOGICAL_AGENT_CAP);
    assert!(
        population
            .current_member_profiles
            .is_subset(&population.profile_ids)
    );
    Ok(added)
}

async fn read_workspace_population(
    context: &ExistingHomeV3FlowContext<'_>,
) -> TestResult<WorkspacePopulation> {
    let members = context
        .client
        .list_members(
            context.bearer,
            &MemberListRequest {
                scope: context.creator.scope.clone(),
                session_id: Some(context.workspace_leader.session_id),
                limit: 64,
            },
        )
        .await
        .map_err(|error| format!("Workspace logical member readback: {error:?}"))?
        .members;
    let profiles = context
        .client
        .workspace_profiles(
            context.bearer,
            &WorkspaceProfilesQueryRequest {
                scope: context.creator.scope.clone(),
                session_id: Some(context.workspace_leader.session_id),
            },
        )
        .await
        .map_err(|error| format!("Workspace Profile readback: {error:?}"))?;
    let mut profile_ids = profiles
        .profiles
        .iter()
        .map(|profile| profile.profile.profile_id)
        .collect::<BTreeSet<_>>();
    let session_profiles = profiles
        .session_profiles
        .iter()
        .map(|mapping| (mapping.session_id, mapping.profile_id))
        .collect::<BTreeMap<_, _>>();
    profile_ids.extend(session_profiles.values().copied());
    let mut current_member_profiles = BTreeSet::new();
    let mut occupancy_keys = profile_ids
        .iter()
        .copied()
        .map(OccupancyKey::Profile)
        .collect::<BTreeSet<_>>();
    for member in &members {
        if let Some(profile_id) = session_profiles.get(&member.session_id) {
            if !current_member_profiles.insert(*profile_id) {
                return Err("two Workspace logical members share one Profile identity".into());
            }
            if !profile_ids.contains(profile_id) {
                return Err("Workspace Session mapping references a missing Profile".into());
            }
        } else {
            occupancy_keys.insert(OccupancyKey::Participant(member.participant_id));
        }
    }
    if !current_member_profiles.is_subset(&profile_ids) {
        return Err("Workspace member mapping references a missing Profile".into());
    }
    Ok(WorkspacePopulation {
        member_count: members.len(),
        profile_ids,
        current_member_profiles,
        occupancy_keys,
    })
}

async fn assert_unassigned_fixture_profiles(
    context: &ExistingHomeV3FlowContext<'_>,
    added: &[ReadyFixtureMember],
) -> TestResult {
    if added.is_empty() {
        return Ok(());
    }
    let listing = context
        .client
        .team_list(
            context.bearer,
            &TeamListQueryRequest {
                scope: context.creator.scope.clone(),
                session_id: Some(context.workspace_leader.session_id),
                limit: 64,
            },
        )
        .await?;
    if listing.truncated {
        return Err("Team list truncated during v3 capacity setup".into());
    }
    for team in listing.teams {
        let snapshot = context
            .client
            .team_get(
                context.bearer,
                &TeamGetQueryRequest {
                    scope: context.creator.scope.clone(),
                    session_id: Some(context.workspace_leader.session_id),
                    team_id: team.team_id,
                    request_id: None,
                },
            )
            .await?;
        if snapshot.members_truncated {
            return Err("Team roster truncated during v3 capacity setup".into());
        }
        for added_member in added {
            if snapshot
                .members
                .iter()
                .any(|member| member.profile_id == Some(added_member.profile_id))
            {
                return Err("capacity-only Profile unexpectedly joined a Team".into());
            }
        }
    }
    Ok(())
}

async fn assert_current_authorities(context: &ExistingHomeV3FlowContext<'_>) -> TestResult {
    let workspace = context
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
        .await?
        .leadership
        .ok_or("current Workspace Orchestrator is absent")?;
    assert_eq!(
        workspace.leader_participant_id,
        Some(context.workspace_leader.participant_id)
    );
    assert_eq!(
        workspace.leadership_revision,
        context.workspace_leadership_revision
    );

    let destination = context
        .client
        .leadership_view(
            context.bearer,
            &LeadershipQueryRequest {
                scope: context.creator.scope.clone(),
                session_id: Some(context.destination_leader.session_id),
                activity_id: None,
                team_id: Some(context.destination_team_id),
                review_after: None,
                limit: 10,
            },
        )
        .await?
        .leadership
        .ok_or("destination Team Leader is absent")?;
    assert_eq!(
        destination.leader_participant_id,
        Some(context.destination_leader.participant_id)
    );
    assert_eq!(
        destination.leadership_revision,
        context.destination_leadership_revision
    );

    let source = context
        .client
        .leadership_view(
            context.bearer,
            &LeadershipQueryRequest {
                scope: context.creator.scope.clone(),
                session_id: Some(context.source_home_leader.session_id),
                activity_id: None,
                team_id: Some(context.home_team_id),
                review_after: None,
                limit: 10,
            },
        )
        .await?
        .leadership
        .ok_or("source Home Team Leader is absent")?;
    assert_eq!(
        source.leader_participant_id,
        Some(context.source_home_leader.participant_id)
    );
    assert_eq!(
        source.leadership_revision,
        context.source_home_leadership_revision
    );
    Ok(())
}

async fn read_team_snapshot(
    context: &ExistingHomeV3FlowContext<'_>,
    team_id: Uuid,
) -> TestResult<TeamSnapshot> {
    let response = context
        .client
        .team_get(
            context.bearer,
            &TeamGetQueryRequest {
                scope: context.creator.scope.clone(),
                session_id: Some(context.workspace_leader.session_id),
                team_id,
                request_id: None,
            },
        )
        .await?;
    if response.members_truncated {
        return Err(format!("Team roster truncated team_id={team_id}").into());
    }
    let team = response
        .team
        .ok_or_else(|| format!("Team missing team_id={team_id}"))?;
    if team.team_id != team_id
        || team.workspace_id != context.creator.scope.workspace_id
        || team.state != "active"
    {
        return Err("Team snapshot scope mismatch".into());
    }
    Ok(TeamSnapshot {
        team,
        members: response.members,
    })
}

fn exact_home_member<'a>(
    context: &ExistingHomeV3FlowContext<'_>,
    home: &'a TeamSnapshot,
) -> TestResult<&'a heterarchy_orchestration_backend::wire::teams::TeamMemberWire> {
    let mut matching = home.members.iter().filter(|member| {
        member.profile_id == Some(context.home_member.profile_id)
            && member.participation_id != Uuid::nil()
    });
    let member = matching
        .next()
        .ok_or("source Home Team has no exact target membership")?;
    if matching.next().is_some() {
        return Err("source Home Team has ambiguous target memberships".into());
    }
    if member.team_id != context.home_team_id
        || member.workspace_id != context.creator.scope.workspace_id
    {
        return Err("source Home membership scope mismatch".into());
    }
    Ok(member)
}

fn exact_profile_member(
    team: &TeamSnapshot,
    profile_id: Uuid,
) -> TestResult<&heterarchy_orchestration_backend::wire::teams::TeamMemberWire> {
    let mut matches = team
        .members
        .iter()
        .filter(|member| member.profile_id == Some(profile_id));
    let member = matches.next().ok_or("expected Team member is absent")?;
    if matches.next().is_some() || member.state != "joined" {
        return Err("expected Team member is ambiguous or not JOINED".into());
    }
    Ok(member)
}

async fn read_destination_coordinates(
    context: &ExistingHomeV3FlowContext<'_>,
    destination: &TeamSnapshot,
) -> TestResult<ExpansionCoordinates> {
    let capacity = context
        .client
        .team_capacity(
            context.bearer,
            &team_expansion::TeamCapacityQueryRequest {
                actor: actor(context, context.workspace_leader),
                team_id: context.destination_team_id,
            },
        )
        .await?
        .snapshot
        .ok_or("destination Team has no explicit cap")?;
    if capacity.team_id != context.destination_team_id
        || capacity.workspace_id != context.creator.scope.workspace_id
    {
        return Err("destination Team/capacity scope mismatch".into());
    }
    Ok(ExpansionCoordinates {
        team_revision: destination.team.revision,
        capacity_revision: capacity.revision,
        max_members: capacity.max_members,
    })
}

async fn ensure_destination_capacity_for_one_hold(
    context: &ExistingHomeV3FlowContext<'_>,
    destination: TeamSnapshot,
) -> TestResult<(TeamSnapshot, ExpansionCoordinates)> {
    let coordinates = read_destination_coordinates(context, &destination).await?;
    if usize::from(coordinates.max_members) < destination.members.len() {
        return Err("destination Team roster exceeds its configured cap".into());
    }
    if usize::from(coordinates.max_members) > destination.members.len() {
        return Ok((destination, coordinates));
    }
    let requested_cap = destination
        .members
        .len()
        .checked_add(1)
        .and_then(|count| u8::try_from(count).ok())
        .filter(|count| *count <= 8)
        .ok_or("destination Team cannot fit one Workspace hold")?;
    let intent = expansion_wire::TeamExpansionMutationRequest {
        actor: actor(context, context.workspace_leader),
        action: expansion_wire::TeamExpansionMutationActionWire::SetCapacity {
            command: expansion_wire::TeamCapacitySetAction {
                team_id: context.destination_team_id,
                request_id: Uuid::now_v7(),
                expected_team_revision: coordinates.team_revision,
                expected_capacity_revision: coordinates.capacity_revision,
                expected_leadership_revision: context.workspace_leadership_revision,
                max_members: requested_cap,
            },
        },
    };
    let response = context
        .client
        .mutate_team_expansion(context.bearer, &intent)
        .await
        .map_err(|error| format!("authorized destination Team capacity preparation: {error:?}"))?;
    if response.request != intent {
        return Err("destination Team capacity response intent mismatch".into());
    }
    let expansion_wire::TeamExpansionMutationResultWire::CapacitySet { receipt } = response.result
    else {
        return Err("destination Team capacity command returned the wrong result".into());
    };
    assert_eq!(receipt.snapshot.max_members, requested_cap);
    assert_eq!(receipt.snapshot.team_id, context.destination_team_id);
    assert_eq!(receipt.snapshot.revision, coordinates.capacity_revision + 1);

    let readback_request = expansion_wire::TeamExpansionReadbackRequest {
        intent: intent.clone(),
        current_actor: actor(context, context.workspace_leader),
    };
    let readback = context
        .client
        .team_expansion_readback(context.bearer, &readback_request)
        .await?;
    assert_eq!(readback.request, readback_request);
    let Some(expansion_wire::TeamExpansionMutationResultWire::CapacitySet { receipt: stored }) =
        readback.result
    else {
        return Err("destination Team capacity exact readback is absent".into());
    };
    assert_eq!(stored.request_id, receipt.request_id);
    assert_eq!(stored.snapshot.max_members, requested_cap);
    assert_eq!(stored.snapshot.revision, receipt.snapshot.revision);

    let updated = read_team_snapshot(context, context.destination_team_id).await?;
    assert_eq!(updated.members, destination.members);
    let updated_coordinates = read_destination_coordinates(context, &updated).await?;
    assert_eq!(updated_coordinates.max_members, requested_cap);
    Ok((updated, updated_coordinates))
}

async fn request_from_destination(
    context: &ExistingHomeV3FlowContext<'_>,
    destination: &TeamSnapshot,
    coordinates: &ExpansionCoordinates,
) -> TestResult<expansion_wire::TeamExpansionRequestReceiptWire> {
    let destination_leader_membership =
        exact_profile_member(destination, context.destination_leader.profile_id)?;
    let intent = expansion_wire::TeamExpansionMutationRequest {
        actor: actor(context, context.destination_leader),
        action: expansion_wire::TeamExpansionMutationActionWire::Request {
            command: expansion_wire::TeamExpansionRequestAction {
                team_id: context.destination_team_id,
                request_id: Uuid::now_v7(),
                expected_team_revision: coordinates.team_revision,
                expected_capacity_revision: coordinates.capacity_revision,
                expected_leadership_revision: context.destination_leadership_revision,
                requested_count: 1,
                reason: "approve one existing Home Team member".to_owned(),
            },
        },
    };
    let response = context
        .client
        .mutate_team_expansion(context.bearer, &intent)
        .await
        .map_err(|error| format!("destination Team Leader request: {error:?}"))?;
    if response.request != intent {
        return Err("Team expansion request echo changed".into());
    }
    let expansion_wire::TeamExpansionMutationResultWire::Request { receipt } = response.result
    else {
        return Err("Team Leader request returned the wrong result kind".into());
    };
    assert_request_receipt(
        context,
        &receipt,
        coordinates,
        destination_leader_membership,
    );
    let readback = context
        .client
        .team_expansion_readback(
            context.bearer,
            &expansion_wire::TeamExpansionReadbackRequest {
                intent,
                current_actor: actor(context, context.destination_leader),
            },
        )
        .await?;
    let Some(expansion_wire::TeamExpansionMutationResultWire::Request { receipt: stored }) =
        readback.result
    else {
        return Err("destination Team Leader request exact readback is absent".into());
    };
    assert_eq!(stored.request_id, receipt.request_id);
    assert_eq!(stored.actor, receipt.actor);
    assert_eq!(
        stored.requester_leadership_revision,
        receipt.requester_leadership_revision
    );
    Ok(*receipt)
}

fn assert_request_receipt(
    context: &ExistingHomeV3FlowContext<'_>,
    receipt: &expansion_wire::TeamExpansionRequestReceiptWire,
    coordinates: &ExpansionCoordinates,
    destination_leader_membership: &heterarchy_orchestration_backend::wire::teams::TeamMemberWire,
) {
    assert_eq!(receipt.team_id, context.destination_team_id);
    assert_eq!(receipt.workspace_id, context.creator.scope.workspace_id);
    assert_eq!(receipt.expected_team_revision, coordinates.team_revision);
    assert_eq!(receipt.capacity_revision, coordinates.capacity_revision);
    assert_eq!(receipt.requested_count, 1);
    assert_eq!(
        receipt.requester_leadership_revision,
        context.destination_leadership_revision
    );
    assert_expansion_actor(&receipt.actor, context, context.destination_leader);
    assert_eq!(
        receipt.actor.participation_id,
        destination_leader_membership.participation_id
    );
}

async fn assert_v3_wrong_role_has_no_write(
    context: &ExistingHomeV3FlowContext<'_>,
    request: &expansion_wire::TeamExpansionRequestReceiptWire,
    home_member: &heterarchy_orchestration_backend::wire::teams::TeamMemberWire,
    coordinates: &ExpansionCoordinates,
) -> TestResult {
    let mut intent = v3_intent(context, request, home_member, coordinates);
    intent.actor = actor(context, context.destination_leader);
    let result = context
        .client
        .mutate_team_expansion_decision_v3(context.bearer, &intent)
        .await;
    expect_conflict(&result, "non-Orchestrator v3 approval")?;
    assert_v3_readback_absent(context, &intent).await
}

async fn assert_v3_mismatched_home_revision_has_no_write(
    context: &ExistingHomeV3FlowContext<'_>,
    request: &expansion_wire::TeamExpansionRequestReceiptWire,
    home_member: &heterarchy_orchestration_backend::wire::teams::TeamMemberWire,
    coordinates: &ExpansionCoordinates,
) -> TestResult {
    let mut intent = v3_intent(context, request, home_member, coordinates);
    intent.command.decision_id = Uuid::now_v7();
    let v3_wire::TeamExpansionApprovalTargetV3Wire::ExistingHomeTransfer {
        source_membership_revision,
        ..
    } = &mut intent.slot_plan[0].target
    else {
        return Err("mismatch fixture is not an existing Home transfer".into());
    };
    *source_membership_revision = source_membership_revision
        .checked_add(1)
        .ok_or("Home membership revision overflow")?;
    let result = context
        .client
        .mutate_team_expansion_decision_v3(context.bearer, &intent)
        .await;
    expect_conflict(&result, "v3 approval with mismatched Home revision")?;
    assert_v3_readback_absent(context, &intent).await
}

async fn assert_v3_readback_absent(
    context: &ExistingHomeV3FlowContext<'_>,
    intent: &v3_wire::TeamExpansionDecisionV3Request,
) -> TestResult {
    let request = v3_wire::TeamExpansionDecisionV3ReadbackRequest {
        intent: intent.clone(),
        current_actor: actor(context, context.workspace_leader),
    };
    let response = context
        .client
        .team_expansion_decision_v3_readback(context.bearer, &request)
        .await?;
    assert_eq!(response.request, request);
    assert!(
        response.result.is_none(),
        "rejected v3 approval created a durable decision or hold"
    );
    Ok(())
}

async fn assert_rosters_and_population_unchanged(
    context: &ExistingHomeV3FlowContext<'_>,
    home_before: &TeamSnapshot,
    destination_before: &TeamSnapshot,
    destination_coordinates_before: &ExpansionCoordinates,
    population_before: &WorkspacePopulation,
    operation: &str,
) -> TestResult {
    let home_after = read_team_snapshot(context, context.home_team_id).await?;
    let destination_after = read_team_snapshot(context, context.destination_team_id).await?;
    if home_after.team != home_before.team || home_after.members != home_before.members {
        return Err(format!("{operation} changed the source Home Team").into());
    }
    if destination_after.team != destination_before.team
        || destination_after.members != destination_before.members
    {
        return Err(format!("{operation} changed the destination Team").into());
    }
    if read_destination_coordinates(context, &destination_after).await?
        != *destination_coordinates_before
    {
        return Err(format!("{operation} changed the destination Team capacity").into());
    }
    if read_workspace_population(context).await? != *population_before {
        return Err(format!("{operation} changed Workspace membership or occupancy").into());
    }
    Ok(())
}

fn v3_intent(
    context: &ExistingHomeV3FlowContext<'_>,
    request: &expansion_wire::TeamExpansionRequestReceiptWire,
    home_member: &heterarchy_orchestration_backend::wire::teams::TeamMemberWire,
    coordinates: &ExpansionCoordinates,
) -> v3_wire::TeamExpansionDecisionV3Request {
    v3_wire::TeamExpansionDecisionV3Request {
        actor: actor(context, context.workspace_leader),
        command: expansion_wire::TeamExpansionDecisionAction {
            team_id: context.destination_team_id,
            request_id: request.request_id,
            decision_id: Uuid::now_v7(),
            expected_team_revision: coordinates.team_revision,
            expected_capacity_revision: coordinates.capacity_revision,
            expected_leadership_revision: context.workspace_leadership_revision,
            expected_requester_leadership_revision: request.requester_leadership_revision,
            decision: expansion_wire::TeamExpansionDecisionWire::Approve { approved_count: 1 },
        },
        slot_plan: vec![v3_wire::TeamExpansionApprovalSlotV3Wire {
            ordinal: 0,
            target: v3_wire::TeamExpansionApprovalTargetV3Wire::ExistingHomeTransfer {
                profile_id: context.home_member.profile_id,
                participation_id: home_member.participation_id,
                source_team_id: context.home_team_id,
                source_membership_revision: home_member.revision,
            },
        }],
    }
}

async fn approve_and_read_exact_v3(
    context: &ExistingHomeV3FlowContext<'_>,
    intent: &v3_wire::TeamExpansionDecisionV3Request,
) -> TestResult<v3_wire::TeamExpansionDecisionV3ReceiptWire> {
    let direct = match context
        .client
        .mutate_team_expansion_decision_v3(context.bearer, intent)
        .await
    {
        Ok(response) => {
            if response.request != *intent {
                return Err("v3 approval response intent mismatch".into());
            }
            Some(response.receipt)
        }
        Err(
            ClientError::ReadbackRequired {
                readback:
                    heterarchy_orchestration_backend::client::ReadbackTarget::TeamExpansionDecisionV3(
                        original,
                    ),
            }
            | ClientError::UnknownOutcome {
                readback:
                    heterarchy_orchestration_backend::client::ReadbackTarget::TeamExpansionDecisionV3(
                        original,
                    ),
            },
        ) if original.as_ref() == intent => None,
        Err(error) => return Err(format!("v3 approval mutation failed: {error:?}").into()),
    };
    let receipt = read_v3_exact(context, intent).await?;
    if let Some(direct) = direct {
        assert_eq!(direct, receipt);
    }
    Ok(receipt)
}

async fn read_v3_exact(
    context: &ExistingHomeV3FlowContext<'_>,
    intent: &v3_wire::TeamExpansionDecisionV3Request,
) -> TestResult<v3_wire::TeamExpansionDecisionV3ReceiptWire> {
    let readback_request = v3_wire::TeamExpansionDecisionV3ReadbackRequest {
        intent: intent.clone(),
        current_actor: actor(context, context.workspace_leader),
    };
    let response = context
        .client
        .team_expansion_decision_v3_readback(context.bearer, &readback_request)
        .await
        .map_err(|error| format!("exact v3 decision readback: {error:?}"))?;
    assert_eq!(response.request, readback_request);
    response
        .result
        .ok_or_else(|| "exact v3 approval readback is absent".into())
}

async fn assert_v2_downgrade_is_rejected(
    context: &ExistingHomeV3FlowContext<'_>,
    v3_intent: &v3_wire::TeamExpansionDecisionV3Request,
    v3_receipt: &v3_wire::TeamExpansionDecisionV3ReceiptWire,
) -> TestResult {
    let v3_wire::TeamExpansionApprovalTargetV3Wire::ExistingHomeTransfer {
        profile_id,
        participation_id,
        ..
    } = v3_intent.slot_plan[0].target
    else {
        return Err("v2 downgrade fixture is not a Home transfer".into());
    };
    let downgrade = v2_wire::TeamExpansionDecisionV2Request {
        actor: v3_intent.actor.clone(),
        command: v3_intent.command.clone(),
        slot_plan: vec![v2_wire::TeamExpansionApprovalSlotV2Wire {
            ordinal: 0,
            target: v2_wire::TeamExpansionApprovalTargetV2Wire {
                profile_id,
                participation_id,
            },
        }],
    };
    let mutation = context
        .client
        .mutate_team_expansion_decision_v2(context.bearer, &downgrade)
        .await;
    expect_conflict(&mutation, "v2 downgrade of a v3 Home approval")?;

    let readback = context
        .client
        .team_expansion_decision_v2_readback(
            context.bearer,
            &v2_wire::TeamExpansionDecisionV2ReadbackRequest {
                intent: downgrade,
                current_actor: actor(context, context.workspace_leader),
            },
        )
        .await;
    expect_conflict(&readback, "v2 readback of a v3 Home approval")?;

    let exact = read_v3_exact(context, v3_intent).await?;
    assert_eq!(exact, *v3_receipt);
    Ok(())
}

fn assert_v3_receipt(
    receipt: &v3_wire::TeamExpansionDecisionV3ReceiptWire,
    context: &ExistingHomeV3FlowContext<'_>,
    intent: &v3_wire::TeamExpansionDecisionV3Request,
    home_member: &heterarchy_orchestration_backend::wire::teams::TeamMemberWire,
    workspace_leader_membership: &heterarchy_orchestration_backend::wire::teams::TeamMemberWire,
) -> TestResult {
    assert_eq!(receipt.slot_plan, intent.slot_plan);
    assert_eq!(
        receipt.decision.workspace_id,
        context.creator.scope.workspace_id
    );
    assert_eq!(receipt.decision.team_id, context.destination_team_id);
    assert_eq!(receipt.decision.request_id, intent.command.request_id);
    assert_eq!(receipt.decision.decision_id, intent.command.decision_id);
    assert_eq!(
        receipt.decision.expected_team_revision,
        intent.command.expected_team_revision
    );
    assert_eq!(
        receipt.decision.capacity_revision,
        intent.command.expected_capacity_revision
    );
    assert_eq!(
        receipt.decision.leadership_revision,
        intent.command.expected_leadership_revision
    );
    assert_eq!(receipt.decision.approved_count, 1);
    assert!(receipt.decision.recorded_at_ms >= 0);
    assert_eq!(
        receipt.decision.state,
        expansion_wire::TeamExpansionDecisionStateWire::ApprovedPendingAnchor
    );
    assert_eq!(receipt.decision.cooldown_started_at_ms, None);
    assert_eq!(receipt.decision.cooldown_until_ms, None);
    assert_eq!(receipt.decision.finalized_by, None);
    assert_eq!(receipt.decision.finalized_by_leadership_revision, None);
    assert_expansion_actor(&receipt.decision.actor, context, context.workspace_leader);
    assert_eq!(
        receipt.decision.actor.participation_id,
        workspace_leader_membership.participation_id
    );
    let v3_wire::TeamExpansionApprovalTargetV3Wire::ExistingHomeTransfer {
        profile_id,
        participation_id,
        source_team_id,
        source_membership_revision,
    } = receipt.slot_plan[0].target
    else {
        return Err("v3 receipt lost its existing Home target".into());
    };
    assert_eq!(profile_id, context.home_member.profile_id);
    assert_eq!(participation_id, home_member.participation_id);
    assert_eq!(source_team_id, context.home_team_id);
    assert_eq!(source_membership_revision, home_member.revision);
    Ok(())
}

async fn finalize_destination_anchor(
    context: &ExistingHomeV3FlowContext<'_>,
    expansion_request: &expansion_wire::TeamExpansionRequestReceiptWire,
    v3_intent: &v3_wire::TeamExpansionDecisionV3Request,
    pending: &v3_wire::TeamExpansionDecisionV3ReceiptWire,
    destination_orchestrator_membership: &heterarchy_orchestration_backend::wire::teams::TeamMemberWire,
) -> TestResult<v3_wire::TeamExpansionDecisionV3ReceiptWire> {
    if pending.decision.state
        != expansion_wire::TeamExpansionDecisionStateWire::ApprovedPendingAnchor
    {
        return Err("V3 decision must be pending before exact anchor finalization".into());
    }
    let intent = expansion_wire::TeamExpansionMutationRequest {
        actor: actor(context, context.workspace_leader),
        action: expansion_wire::TeamExpansionMutationActionWire::FinalizeAnchor {
            command: expansion_wire::TeamExpansionFinalizeAnchorAction {
                request_id: expansion_request.request_id,
                decision_id: pending.decision.decision_id,
                expected_leadership_revision: context.workspace_leadership_revision,
            },
        },
    };
    let direct = context
        .client
        .mutate_team_expansion(context.bearer, &intent)
        .await
        .map_err(|error| format!("exact V3 destination anchor finalization: {error:?}"))?;
    if direct.request != intent {
        return Err("destination anchor response intent mismatch".into());
    }
    let expansion_wire::TeamExpansionMutationResultWire::Decision { receipt: finalized } =
        direct.result
    else {
        return Err("destination anchor command returned the wrong result kind".into());
    };
    assert_finalized_anchor(
        context,
        &intent,
        &finalized,
        destination_orchestrator_membership,
    )?;

    let readback_request = expansion_wire::TeamExpansionReadbackRequest {
        intent: intent.clone(),
        current_actor: actor(context, context.workspace_leader),
    };
    let readback = context
        .client
        .team_expansion_readback(context.bearer, &readback_request)
        .await?;
    assert_eq!(readback.request, readback_request);
    let Some(expansion_wire::TeamExpansionMutationResultWire::Decision { receipt: stored }) =
        readback.result
    else {
        return Err("exact destination anchor readback is absent".into());
    };
    assert_eq!(*stored, *finalized);

    let v3_readback = read_v3_exact(context, v3_intent).await?;
    assert_eq!(v3_readback.slot_plan, v3_intent.slot_plan);
    assert_eq!(v3_readback.decision, *stored);
    Ok(v3_readback)
}

fn assert_finalized_anchor(
    context: &ExistingHomeV3FlowContext<'_>,
    intent: &expansion_wire::TeamExpansionMutationRequest,
    receipt: &expansion_wire::TeamExpansionDecisionReceiptWire,
    orchestrator_membership: &heterarchy_orchestration_backend::wire::teams::TeamMemberWire,
) -> TestResult {
    let expansion_wire::TeamExpansionMutationActionWire::FinalizeAnchor { command } = intent.action
    else {
        return Err("finalized anchor assertion received a non-finalize intent".into());
    };
    assert_eq!(receipt.request_id, command.request_id);
    assert_eq!(receipt.decision_id, command.decision_id);
    assert_eq!(receipt.team_id, context.destination_team_id);
    assert_eq!(
        receipt.state,
        expansion_wire::TeamExpansionDecisionStateWire::Approved
    );
    let (Some(start), Some(until), Some(finalizer), Some(finalizer_revision)) = (
        receipt.cooldown_started_at_ms,
        receipt.cooldown_until_ms,
        receipt.finalized_by.as_ref(),
        receipt.finalized_by_leadership_revision,
    ) else {
        return Err("finalized anchor receipt omitted its cooldown evidence".into());
    };
    assert_eq!(until.checked_sub(start), Some(60 * 60 * 1_000));
    assert_eq!(finalizer_revision, context.workspace_leadership_revision);
    assert_expansion_actor(finalizer, context, context.workspace_leader);
    assert_eq!(
        finalizer.participation_id,
        orchestrator_membership.participation_id
    );
    Ok(())
}

fn assert_expansion_actor(
    receipt: &expansion_wire::TeamExpansionActorReceiptWire,
    context: &ExistingHomeV3FlowContext<'_>,
    member: &ReadyFixtureMember,
) {
    assert_eq!(receipt.activity_id, context.creator.activity_id);
    assert_eq!(receipt.agent_id, member.agent_id);
    assert_eq!(receipt.session_id, member.session_id);
    assert_eq!(receipt.profile_id, member.profile_id);
    assert_eq!(receipt.participant_id, member.participant_id);
}

fn actor(
    context: &ExistingHomeV3FlowContext<'_>,
    member: &ReadyFixtureMember,
) -> expansion_wire::TeamExpansionActorWire {
    expansion_wire::TeamExpansionActorWire {
        scope: context.creator.scope.clone(),
        activity_id: context.creator.activity_id,
        session_id: member.session_id,
    }
}

fn expect_conflict<T: std::fmt::Debug>(
    result: &Result<T, ClientError>,
    operation: &str,
) -> TestResult {
    if matches!(
        result,
        Err(ClientError::Rejected { status, .. })
            if *status == StatusCode::CONFLICT.as_u16()
    ) {
        Ok(())
    } else {
        Err(format!("{operation} did not return HTTP 409: {result:?}").into())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "opt-in real Broker/Forge flow; same-Leader stabilization wait may take up to 20 minutes; not a successor-election or whole-tenure proof"]
async fn real_broker_existing_home_transfer_same_leader_stabilization_renewal_http() -> TestResult {
    let cluster = super::broker_cluster::Cluster::start().await?;
    let result = Box::pin(super::bootstrap_fixture_with_broker_options(
        cluster.orchestration_config()?,
        true,
        false,
        super::TransferExercise::SlowRenewal,
    ))
    .await;
    drop(cluster);
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "opt-in real Broker/Forge/Orchestration HTTP proof of HomeTransfer preparation and reply-loss recovery"]
async fn real_broker_existing_home_transfer_preparation_reply_loss_http() -> TestResult {
    use tracing_subscriber::prelude::*;
    let filter = tracing_subscriber::filter::Targets::new().with_target(
        "heterarchy_orchestration_backend::teams::team_permanent_transfer",
        tracing::Level::DEBUG,
    );
    let _subscriber = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_test_writer()
                .with_filter(filter),
        )
        .try_init();
    let cluster = super::broker_cluster::Cluster::start().await?;
    let result = Box::pin(super::bootstrap_fixture_with_broker_options(
        cluster.orchestration_config()?,
        true,
        false,
        super::TransferExercise::Preparation,
    ))
    .await;
    drop(cluster);
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "opt-in real Broker/Forge/Orchestration proof that missing Workspace identity keeps preparation pending"]
async fn real_broker_existing_home_transfer_preparation_unattributed_is_pending_http() -> TestResult
{
    let cluster = super::broker_cluster::Cluster::start().await?;
    let result = Box::pin(super::bootstrap_fixture_with_broker_options(
        cluster.orchestration_config()?,
        true,
        false,
        super::TransferExercise::UnattributedPreparation,
    ))
    .await;
    drop(cluster);
    result
}
