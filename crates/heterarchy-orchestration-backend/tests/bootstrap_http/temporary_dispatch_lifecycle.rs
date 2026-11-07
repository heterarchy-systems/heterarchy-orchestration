//! Real Broker/Forge/Orchestration proof for temporary-dispatch lifecycle facts.

use std::net::SocketAddr;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use forge_control::{AgentAssignmentMutation, AgentAssignmentMutationRequest, AgentContextRequest};
use forge_domain::{
    ActivityCoordinatorActor, AgentAssignmentId, AgentAssignmentState, AgentAssignmentTarget,
    AgentInstanceId, AgentSessionId, AuthenticatedPrincipal, DagHeadRevision, OfferAgentAssignment,
    TimestampMillis,
};
use hyper::StatusCode;
use hyper::header::HeaderValue;
use serde_json::json;
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use uuid::Uuid;

use super::{
    AgentListLimit, CoordinationQueryRequest, ForgeCommandHttpClient, ForgeHost,
    ForgeSourceHttpClient, MemberTarget as BootstrapTarget, MissionAssignmentSource,
    ReadyFixtureMember, RegisteredDagNode, RegisteredGoal, SourceAssignmentFixture, TestResult,
    current_test_time, scope_wire,
};
use heterarchy_orchestration_backend::client::{
    ClientError, OrchestrationHttpClient, ReadbackTarget,
};
use heterarchy_orchestration_backend::wire::leadership::LeadershipQueryRequest;
use heterarchy_orchestration_backend::wire::leadership_lifecycle::{
    LeadershipAdvanceRequest, LeadershipLifecycleTarget,
};
use heterarchy_orchestration_backend::wire::node_assignment_admission::{
    NodeAssignmentAdmissionRequest, NodeAssignmentSourceFencesWire, NodeAssignmentTargetWire,
};
use heterarchy_orchestration_backend::wire::node_team::NodeTeamSetRequest;
use heterarchy_orchestration_backend::wire::team_commands::{
    TeamCreateV2Request, TeamMutationReaderV2Wire, TeamMutationV2Response, TeamRosterMemberRequest,
};
use heterarchy_orchestration_backend::wire::teams::{TeamGetQueryRequest, TeamMemberWire};
use heterarchy_orchestration_backend::wire::temporary_dispatch as admission_wire;
use heterarchy_orchestration_backend::wire::temporary_dispatch_lifecycle as lifecycle_wire;
use heterarchy_orchestration_backend::wire::temporary_dispatch_lifecycle::TemporaryDispatchConsentKindWire;

/// Source inputs for one exact Node/Team-affined Forge offer.
pub(super) struct TeamAffinedOfferContext<'a> {
    pub(super) client: &'a OrchestrationHttpClient,
    pub(super) bearer: &'a HeaderValue,
    pub(super) source: &'a ForgeHost,
    pub(super) creator: &'a BootstrapTarget,
    pub(super) workspace_leader: &'a ReadyFixtureMember,
    pub(super) workspace_leadership_revision: i64,
    pub(super) principal: AuthenticatedPrincipal,
    pub(super) team_id: Uuid,
    pub(super) target: &'a ReadyFixtureMember,
    pub(super) assignments: &'a SourceAssignmentFixture,
    pub(super) node: &'a RegisteredDagNode,
}

/// Issue one source-backed Team-affined offer after the exact pre-offer
/// affinity has been admitted through the authenticated Orchestration API.
///
/// # Errors
/// Returns an error if the Ready target, current Coordinator, exact Node-Team
/// binding, pre-offer source proof, Forge mutation, or effect readback diverges.
pub(super) async fn offer_team_affined_assignment(
    context: &TeamAffinedOfferContext<'_>,
) -> TestResult<MissionAssignmentSource> {
    let before = assignment_effect_cursor(context.client, context.bearer, context.creator).await?;
    let coordinator = source_coordinator(context).await?;
    let (target, assignment_id, offered_at) = source_target(context, coordinator).await?;
    let node_scope = node_scope(context.creator, &context.assignments.goal, context.node);
    let binding_revision = bind_node_to_team(context, node_scope).await?;
    admit_exact_offer_affinity(
        context,
        coordinator,
        target,
        assignment_id,
        offered_at,
        node_scope,
        binding_revision,
    )
    .await?;
    let (assignment_id, target) =
        commit_source_offer(context, coordinator, target, assignment_id, offered_at).await?;
    let source_sequence = assert_offer_was_ingested(context, before, assignment_id, target).await?;
    Ok(MissionAssignmentSource {
        assignment_id,
        target,
        source_sequence,
    })
}

async fn assignment_effect_cursor(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    creator: &BootstrapTarget,
) -> TestResult<u64> {
    client
        .agent_coordination_get(
            bearer,
            &CoordinationQueryRequest {
                scope: creator.scope.clone(),
                activity_id: creator.activity_id,
                session_id: Some(creator.session_id),
            },
        )
        .await?
        .catch_up
        .map(|catch_up| catch_up.applied_through)
        .ok_or_else(|| "Forge effect cursor missing before Team-affined offer".into())
}

async fn source_coordinator(
    context: &TeamAffinedOfferContext<'_>,
) -> TestResult<ActivityCoordinatorActor> {
    let commands =
        ForgeCommandHttpClient::new(context.source.address, super::FORGE_SECRET, super::DEADLINE)?;
    ForgeSourceHttpClient::new(commands)
        .coordinator_actor(
            &context.source.scope,
            context.creator.activity_id,
            context.principal,
        )
        .await?
        .ok_or_else(|| "current Forge Coordinator is missing before Team-affined offer".into())
}

async fn source_target(
    context: &TeamAffinedOfferContext<'_>,
    coordinator: ActivityCoordinatorActor,
) -> TestResult<(AgentAssignmentTarget, AgentAssignmentId, TimestampMillis)> {
    let scope = context.source.scope.clone();
    let activity_id = forge_domain::ActivityId::from_uuid(context.creator.activity_id);
    let agent_id = AgentInstanceId::from_uuid(context.target.agent_id);
    let now = TimestampMillis::new(current_test_time()?)?;
    let focus_event_limit = AgentListLimit::new(4)?;
    let work_update_limit = AgentListLimit::new(4)?;
    let control = context.source.control_client.clone();
    let session_id = AgentSessionId::from_uuid(context.target.session_id);
    let principal = context.principal;
    let target = tokio::task::spawn_blocking(move || {
        let current = control
            .agent_get_context(AgentContextRequest {
                scope,
                activity_id,
                agent_id,
                now,
                focus_event_limit,
                work_update_limit,
            })
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "Team-affined offer target Forge Agent is absent".to_owned())?;
        let agent = current.attachment().agent();
        if agent.session_id() != Some(session_id)
            || agent.principal() != principal
            || !agent.attachment_state().is_attached()
            || agent.id() == coordinator.agent_id()
        {
            return Err(
                "Team-affined offer target is not a distinct attached Ready Agent".to_owned(),
            );
        }
        Ok::<_, String>(AgentAssignmentTarget::new(
            agent.id(),
            session_id,
            principal,
            agent.revision(),
        ))
    })
    .await??;
    Ok((target, AgentAssignmentId::from_uuid(Uuid::now_v7()), now))
}

fn node_scope(
    creator: &BootstrapTarget,
    goal: &RegisteredGoal,
    node: &RegisteredDagNode,
) -> heterarchy_orchestration_backend::wire::node_team::NodeTeamScopeWire {
    heterarchy_orchestration_backend::wire::node_team::NodeTeamScopeWire {
        activity_id: creator.activity_id,
        goal_id: goal.id().into_uuid(),
        goal_created_at_ms: goal.timestamps().created_at().get(),
        dag_revision: node.dag_revision().get(),
        dag_base_goal_revision: goal.revision().get(),
        node_id: node.id().into_uuid(),
    }
}

async fn bind_node_to_team(
    context: &TeamAffinedOfferContext<'_>,
    node_scope: heterarchy_orchestration_backend::wire::node_team::NodeTeamScopeWire,
) -> TestResult<u64> {
    let request = NodeTeamSetRequest {
        scope: context.creator.scope.clone(),
        session_id: context.workspace_leader.session_id,
        sender_activity_id: context.creator.activity_id,
        node_scope,
        expected_revision: None,
        team_id: Some(context.team_id),
        sender_leadership_revision: context.workspace_leadership_revision,
    };
    let set = context
        .client
        .node_team_set(context.bearer, &request)
        .await?;
    assert_eq!(set.request, request);
    assert_eq!(set.binding.activity_id, Some(node_scope.activity_id));
    assert_eq!(
        set.binding.goal_created_at_ms,
        Some(node_scope.goal_created_at_ms)
    );
    assert_eq!(
        set.binding.dag_base_goal_revision,
        Some(node_scope.dag_base_goal_revision)
    );
    assert_eq!(set.binding.goal_id, node_scope.goal_id);
    assert_eq!(set.binding.dag_revision, node_scope.dag_revision);
    assert_eq!(set.binding.node_id, node_scope.node_id);
    assert_eq!(set.binding.team_id, Some(context.team_id));
    assert_eq!(set.binding.revision, 1);
    assert_eq!(set.binding.bound_by, "orchestrator");
    let readback = context
        .client
        .node_team_read(
            context.bearer,
            &heterarchy_orchestration_backend::wire::node_team::NodeTeamQueryRequest {
                scope: request.scope,
                session_id: request.session_id,
                sender_activity_id: request.sender_activity_id,
                node_scope,
                after_assignment_id: None,
            },
        )
        .await?;
    assert_eq!(readback.snapshot.binding, Some(set.binding));
    Ok(readback
        .snapshot
        .binding
        .ok_or("Team-affined Node binding missing on readback")?
        .revision)
}

async fn admit_exact_offer_affinity(
    context: &TeamAffinedOfferContext<'_>,
    coordinator: ActivityCoordinatorActor,
    target: AgentAssignmentTarget,
    assignment_id: AgentAssignmentId,
    offered_at: TimestampMillis,
    node_scope: heterarchy_orchestration_backend::wire::node_team::NodeTeamScopeWire,
    binding_revision: u64,
) -> TestResult {
    let request = admission_request(
        context,
        coordinator,
        target,
        assignment_id,
        offered_at,
        node_scope,
        binding_revision,
    )?;
    let admitted = context
        .client
        .admit_node_assignment(context.bearer, &request)
        .await?;
    assert_eq!(admitted.request, request);
    assert!(admitted.created);
    assert_eq!(admitted.admission.assignment_id, assignment_id.into_uuid());
    assert_eq!(admitted.admission.team_id, Some(context.team_id));
    assert_eq!(admitted.admission.binding_revision, Some(binding_revision));
    assert_eq!(
        admitted.admission.target_agent_id,
        target.agent_id().into_uuid()
    );
    assert_eq!(
        admitted.admission.target_session_id,
        target.session_id().into_uuid()
    );
    assert_eq!(admitted.admission.node_scope.node_id, node_scope.node_id);
    Ok(())
}

fn admission_request(
    context: &TeamAffinedOfferContext<'_>,
    coordinator: ActivityCoordinatorActor,
    target: AgentAssignmentTarget,
    assignment_id: AgentAssignmentId,
    offered_at: TimestampMillis,
    node_scope: heterarchy_orchestration_backend::wire::node_team::NodeTeamScopeWire,
    binding_revision: u64,
) -> TestResult<NodeAssignmentAdmissionRequest> {
    let payload = serde_json::to_vec(&json!({
        "scope": super::forge_source_scope(context.source)?,
        "activity_id": context.creator.activity_id,
        "assignment_id": assignment_id.into_uuid(),
        "observed_at_ms": offered_at.get(),
        "action": {
            "kind": "offer",
            "coordinator": {
                "session_id": coordinator.session_id().into_uuid(),
                "agent_id": coordinator.agent_id().into_uuid(),
                "expected_revision": coordinator.expected_revision().get(),
                "principal": principal_wire(coordinator.principal())?
            },
            "target": {
                "agent_id": target.agent_id().into_uuid(),
                "session_id": target.session_id().into_uuid(),
                "expected_revision": target.agent_revision().get(),
                "principal": principal_wire(target.principal())?
            },
            "goal_id": context.assignments.goal.id().into_uuid(),
            "expected_goal_revision": context.assignments.goal.revision().get(),
            "expected_goal_created_at_ms": context.assignments.goal.timestamps().created_at().get(),
            "dag_revision": node_scope.dag_revision,
            "expected_head_revision": DagHeadRevision::INITIAL.get(),
            "node_id": node_scope.node_id,
            "expected_node_revision": context.node.revision().get(),
            "credit_budget": null
        }
    }))?;
    let digest: [u8; 32] = Sha256::digest(&payload).into();
    Ok(NodeAssignmentAdmissionRequest {
        scope: scope_wire(&context.source.scope),
        activity_id: context.creator.activity_id,
        coordinator_session_id: coordinator.session_id().into_uuid(),
        coordinator_agent_id: coordinator.agent_id().into_uuid(),
        coordinator_revision: coordinator.expected_revision().get(),
        assignment_id: assignment_id.into_uuid(),
        source_fences: NodeAssignmentSourceFencesWire {
            goal_id: context.assignments.goal.id().into_uuid(),
            goal_created_at_ms: context.assignments.goal.timestamps().created_at().get(),
            goal_revision: context.assignments.goal.revision().get(),
            dag_revision: node_scope.dag_revision,
            dag_base_goal_revision: node_scope.dag_base_goal_revision,
            dag_head_revision: DagHeadRevision::INITIAL.get(),
            node_id: node_scope.node_id,
            node_revision: context.node.revision().get(),
        },
        target: NodeAssignmentTargetWire {
            agent_id: target.agent_id().into_uuid(),
            session_id: target.session_id().into_uuid(),
            agent_revision: target.agent_revision().get(),
        },
        team_id: Some(context.team_id),
        expected_binding_revision: Some(binding_revision),
        source_offer_bytes_base64: URL_SAFE_NO_PAD.encode(&payload),
        source_offer_sha256: digest_hex(&digest),
    })
}

fn principal_wire(principal: AuthenticatedPrincipal) -> TestResult<serde_json::Value> {
    match principal {
        AuthenticatedPrincipal::DirectToken(token_id) => {
            Ok(json!({ "kind": "direct_token", "token_id": token_id.into_uuid() }))
        }
        _ => Err("Team-affined source offer requires the fixture DirectToken principal".into()),
    }
}

fn digest_hex(digest: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in digest {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 15)]));
    }
    output
}

async fn commit_source_offer(
    context: &TeamAffinedOfferContext<'_>,
    coordinator: ActivityCoordinatorActor,
    target: AgentAssignmentTarget,
    assignment_id: AgentAssignmentId,
    offered_at: TimestampMillis,
) -> TestResult<(AgentAssignmentId, AgentAssignmentTarget)> {
    let control = context.source.control_client.clone();
    let scope = context.source.scope.clone();
    let goal = context.assignments.goal.clone();
    let node = context.node.clone();
    let activity_id = forge_domain::ActivityId::from_uuid(context.creator.activity_id);
    let assignment = tokio::task::spawn_blocking(move || {
        let offered = control
            .agent_assignment_mutate(AgentAssignmentMutationRequest {
                scope,
                mutation: AgentAssignmentMutation::Offer(OfferAgentAssignment::new(
                    assignment_id,
                    coordinator,
                    target,
                    goal.id(),
                    goal.revision(),
                    goal.timestamps().created_at(),
                    node.dag_revision(),
                    DagHeadRevision::INITIAL,
                    node.id(),
                    node.revision(),
                    None,
                    offered_at,
                )),
            })
            .map_err(|error| error.to_string())?;
        if offered.id() != assignment_id
            || offered.activity_id() != activity_id
            || offered.state() != AgentAssignmentState::Offered
            || offered.target() != target
        {
            return Err("Forge returned a mismatched Team-affined Assignment offer".to_owned());
        }
        Ok::<_, String>((assignment_id, target))
    })
    .await??;
    Ok(assignment)
}

async fn assert_offer_was_ingested(
    context: &TeamAffinedOfferContext<'_>,
    before: u64,
    assignment_id: AgentAssignmentId,
    target: AgentAssignmentTarget,
) -> TestResult<u64> {
    let after = context
        .client
        .agent_coordination_get(
            context.bearer,
            &CoordinationQueryRequest {
                scope: context.creator.scope.clone(),
                activity_id: context.creator.activity_id,
                session_id: Some(context.creator.session_id),
            },
        )
        .await?
        .catch_up
        .ok_or("Forge effect cursor missing after Team-affined offer")?;
    assert_eq!(after.requested_after, before);
    assert_eq!(after.page_size, 1);
    assert_eq!(
        after.applied_through,
        before.checked_add(1).ok_or("source cursor overflow")?
    );
    assert!(!after.more_may_remain);
    let control = context.source.control_client.clone();
    let scope = context.source.scope.clone();
    let migration_id = after.migration_id;
    let destination_id = after.destination_id;
    let entries = tokio::task::spawn_blocking(move || {
        control
            .effect_fact_outbox(super::EffectFactOutboxReadRequest {
                scope,
                migration_id,
                destination_id,
                after: before,
                limit: 64,
            })
            .map_err(|error| error.to_string())
    })
    .await??;
    let [entry] = entries.as_slice() else {
        return Err("Forge source outbox did not return the exact Team offer page".into());
    };
    assert_eq!(entry.sequence(), after.applied_through);
    assert_eq!(entry.migration_id(), after.migration_id);
    assert_eq!(entry.destination_id(), after.destination_id);
    assert_eq!(
        entry.workspace_id().into_uuid(),
        context.creator.scope.workspace_id
    );
    match entry.fact() {
        super::EffectFact::AssignmentOffered(fact) => {
            assert_eq!(fact.assignment_id(), assignment_id);
            assert_eq!(fact.activity_id().into_uuid(), context.creator.activity_id);
            assert_eq!(
                fact.goal_id().into_uuid(),
                context.assignments.goal.id().into_uuid()
            );
            assert_eq!(fact.node_id().into_uuid(), context.node.id().into_uuid());
            assert_eq!(fact.target_agent_id(), target.agent_id());
            assert_eq!(fact.target_session_id(), Some(target.session_id()));
        }
        other => return Err(format!("unexpected Team leadership source fact: {other:?}").into()),
    }
    Ok(after.applied_through)
}

/// Confirm only the leadership bootstrap produced from the exact admitted
/// Team-affined Forge offer and verify its durable current readback.
///
/// # Errors
/// Returns an error if the exact offer did not create the expected Team
/// bootstrap, Broker confirmation fails, or leadership readback differs.
pub(super) async fn confirm_team_leadership(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    creator: &BootstrapTarget,
    leader: &ReadyFixtureMember,
    team_id: Uuid,
) -> TestResult<i64> {
    let query = LeadershipQueryRequest {
        scope: creator.scope.clone(),
        session_id: Some(leader.session_id),
        activity_id: None,
        team_id: Some(team_id),
        review_after: None,
        limit: 10,
    };
    let bootstrap = client
        .leadership_view(bearer, &query)
        .await?
        .leadership
        .ok_or("Team-affined AssignmentOffered did not bootstrap Team leadership")?;
    assert_eq!(bootstrap.team_id, Some(team_id));
    assert_eq!(bootstrap.state, "bootstrapping");
    assert_eq!(bootstrap.leader_participant_id, Some(leader.participant_id));
    let advance = client
        .advance_leadership(
            bearer,
            &LeadershipAdvanceRequest {
                target: LeadershipLifecycleTarget {
                    scope: creator.scope.clone(),
                    activity_id: None,
                    team_id: Some(team_id),
                    session_id: leader.session_id,
                },
            },
        )
        .await?;
    assert!(
        advance.advanced,
        "Broker-fenced Team bootstrap was not confirmed"
    );
    let confirmed = advance
        .leadership
        .ok_or("Team leadership disappeared after source confirmation")?;
    assert_eq!(confirmed.team_id, Some(team_id));
    assert!(matches!(confirmed.state.as_str(), "stabilizing" | "active"));
    assert_eq!(confirmed.leader_participant_id, Some(leader.participant_id));
    let readback = client
        .leadership_view(bearer, &query)
        .await?
        .leadership
        .ok_or("durable Team leadership readback missing")?;
    assert_eq!(readback.leadership_revision, confirmed.leadership_revision);
    assert_eq!(
        readback.leader_participant_id,
        confirmed.leader_participant_id
    );
    Ok(readback.leadership_revision)
}

/// Authenticated identities and the source Team already established by the
/// bootstrap flow before the Workspace Leader Session is detached.
pub(super) struct TemporaryDispatchFlowContext<'a> {
    pub(super) client: &'a OrchestrationHttpClient,
    pub(super) bearer: &'a HeaderValue,
    pub(super) backend_address: SocketAddr,
    pub(super) principal: AuthenticatedPrincipal,
    pub(super) source: &'a ForgeHost,
    pub(super) creator: &'a BootstrapTarget,
    pub(super) workspace_leader: &'a ReadyFixtureMember,
    pub(super) workspace_leadership_revision: i64,
    pub(super) home_team_id: Uuid,
    pub(super) home_leader: &'a ReadyFixtureMember,
    pub(super) target: &'a ReadyFixtureMember,
    pub(super) assignments: &'a SourceAssignmentFixture,
    pub(super) transfer_exercise: super::TransferExercise,
}

struct ReceivingTeamMembers {
    team_id: Uuid,
    leader: ReadyFixtureMember,
    peer: ReadyFixtureMember,
    leader_membership: TeamMemberWire,
    peer_membership: TeamMemberWire,
}

struct ReceivingTeamLeadership {
    revision: i64,
    assignment: MissionAssignmentSource,
}

struct LifecycleAuthoritySnapshot {
    workspace_leadership_revision: u64,
    home_leadership_revision: u64,
    requesting_leadership_revision: u64,
    target_membership: TeamMemberWire,
    home_leader_membership: TeamMemberWire,
    workspace_leader_membership: TeamMemberWire,
    requesting_leader_membership: TeamMemberWire,
}

/// Exercise admission, all three real consent roles, denial for a valid but
/// unauthorized actor, and reservation readback after dropping a committed
/// HTTP response. No source work is activated by this lifecycle proof.
///
/// # Errors
/// Returns an error if any real Session, source leadership, consent role,
/// denial, committed reservation, or exact recovery readback is missing.
pub(super) async fn assert_temporary_dispatch_http_flow(
    context: &TemporaryDispatchFlowContext<'_>,
) -> TestResult<MissionAssignmentSource> {
    let workspace_leadership_revision =
        assert_current_workspace_leader(context)
            .await
            .map_err(|error| {
                format!("TemporaryDispatch current Workspace Leader readback: {error:?}")
            })?;
    let receiving = create_receiving_team(context, workspace_leadership_revision)
        .await
        .map_err(|error| format!("TemporaryDispatch source-backed R Team create: {error:?}"))?;
    let requesting =
        establish_receiving_team_leader(context, &receiving, workspace_leadership_revision)
            .await
            .map_err(|error| {
                format!("TemporaryDispatch source-backed R Team Leader setup: {error:?}")
            })?;
    let authorities = lifecycle_authorities(
        context,
        &receiving,
        workspace_leadership_revision,
        requesting.revision,
    )
    .await
    .map_err(|error| {
        format!("TemporaryDispatch leadership/roster authority readback: {error:?}")
    })?;
    let admission = admit_exact_dispatch(context, &receiving, &authorities)
        .await
        .map_err(|error| format!("TemporaryDispatch admission mutation/readback: {error:?}"))?;
    assert_wrong_role_consent_has_no_receipt(context, &admission, &authorities, &receiving.leader)
        .await
        .map_err(|error| {
            format!("TemporaryDispatch wrong-role consent denial/readback: {error:?}")
        })?;
    let consents = record_current_consents(context, &admission, &authorities)
        .await
        .map_err(|error| {
            format!("TemporaryDispatch three consent mutations/readbacks: {error:?}")
        })?;
    reserve_and_recover(context, admission, &authorities, &consents)
        .await
        .map_err(|error| {
            format!("TemporaryDispatch response-drop reservation recovery: {error:?}")
        })?;
    let roster = read_team_members(
        context,
        receiving.team_id,
        context.workspace_leader.session_id,
    )
    .await
    .map_err(|error| format!("TemporaryDispatch reserved R Team roster query: {error:?}"))?;
    assert_eq!(
        roster.len(),
        2,
        "Operations reservation unexpectedly changed Team membership"
    );
    assert!(
        !roster
            .iter()
            .any(|member| member.profile_id == Some(context.target.profile_id))
    );
    super::team_expansion::assert_team_expansion_http_flow(
        &super::team_expansion::TeamExpansionFlowContext {
            client: context.client,
            bearer: context.bearer,
            source: context.source,
            creator: context.creator,
            principal: context.principal,
            workspace_leader: context.workspace_leader,
            workspace_leadership_revision,
            team_id: receiving.team_id,
            team_leader: &receiving.leader,
            team_peer: &receiving.peer,
            team_leadership_revision: requesting.revision,
        },
    )
    .await
    .map_err(|error| format!("TeamExpansion v2 bootstrap phase: {error:?}"))?;
    assert_v3_existing_home_approval_after_v2(
        context,
        &receiving,
        &authorities,
        workspace_leadership_revision,
        requesting.revision,
    )
    .await?;
    Ok(requesting.assignment)
}

/// Run the separate V3 Home approval after R's existing V2 cooldown flow.
///
/// # Errors
/// Returns an error if the current H/R source witnesses, the separate Team
/// capacity preparation, or exact V3 denial/readback assertions fail.
async fn assert_v3_existing_home_approval_after_v2(
    context: &TemporaryDispatchFlowContext<'_>,
    receiving: &ReceivingTeamMembers,
    authorities: &LifecycleAuthoritySnapshot,
    workspace_leadership_revision: i64,
    source_home_leadership_revision: i64,
) -> TestResult {
    let destination_leadership_revision = i64::try_from(authorities.home_leadership_revision)?;
    super::team_expansion_v3_existing_home::assert_existing_home_v3_approval_http_flow(
        &super::team_expansion_v3_existing_home::ExistingHomeV3FlowContext {
            client: context.client,
            backend_address: context.backend_address,
            bearer: context.bearer,
            source: context.source,
            creator: context.creator,
            principal: context.principal,
            workspace_leader: context.workspace_leader,
            workspace_leadership_revision,
            home_team_id: receiving.team_id,
            home_member: &receiving.peer,
            source_home_leader: &receiving.leader,
            source_home_leadership_revision,
            destination_team_id: context.home_team_id,
            destination_leader: context.home_leader,
            destination_leadership_revision,
            transfer_exercise: context.transfer_exercise,
        },
    )
    .await
    .map_err(|error| format!("TeamExpansion V3 existing Home approval phase: {error:?}"))?;
    Ok(())
}

/// Reconcile the final O cursor with the last confirmed Forge source event.
///
/// # Errors
/// Returns an error if O has not applied the exact receiving-Team offer, the
/// latest outbox entry differs, or Forge has a later unconsumed source fact.
pub(super) async fn assert_final_source_cursor(
    flow: &super::RealBrokerAgentRequestFlow<'_>,
    target: &BootstrapTarget,
    final_offer: MissionAssignmentSource,
) -> TestResult<u64> {
    let query = CoordinationQueryRequest {
        scope: target.scope.clone(),
        activity_id: target.activity_id,
        session_id: Some(target.session_id),
    };
    let catch_up = flow
        .client
        .agent_coordination_get(flow.bearer, &query)
        .await?
        .catch_up
        .ok_or("final Forge effect cursor is missing")?;
    assert_eq!(catch_up.migration_id, flow.progress.migration_id);
    assert_eq!(catch_up.destination_id, flow.progress.destination_id);
    assert_eq!(catch_up.requested_after, final_offer.source_sequence);
    assert_eq!(catch_up.applied_through, final_offer.source_sequence);
    assert_eq!(catch_up.page_size, 0);
    assert!(!catch_up.more_may_remain);

    let previous_sequence = final_offer
        .source_sequence
        .checked_sub(1)
        .ok_or("final Forge source sequence is zero")?;
    let control = flow.source.control_client.clone();
    let scope = flow.source.scope.clone();
    let migration_id = flow.progress.migration_id;
    let destination_id = flow.progress.destination_id;
    let latest = tokio::task::spawn_blocking(move || {
        control
            .effect_fact_outbox(super::EffectFactOutboxReadRequest {
                scope,
                migration_id,
                destination_id,
                after: previous_sequence,
                limit: 1,
            })
            .map_err(|error| error.to_string())
    })
    .await??;
    let [entry] = latest.as_slice() else {
        return Err("Forge did not return the final source outbox fact".into());
    };
    assert_eq!(entry.sequence(), final_offer.source_sequence);
    assert_eq!(entry.migration_id(), flow.progress.migration_id);
    assert_eq!(entry.destination_id(), flow.progress.destination_id);
    match entry.fact() {
        super::EffectFact::AssignmentOffered(fact) => {
            assert_eq!(fact.assignment_id(), final_offer.assignment_id);
            assert_eq!(fact.workspace_id().into_uuid(), target.scope.workspace_id);
            assert_eq!(fact.activity_id().into_uuid(), target.activity_id);
            assert_eq!(fact.target_agent_id(), final_offer.target.agent_id());
            assert_eq!(
                fact.target_session_id(),
                Some(final_offer.target.session_id())
            );
        }
        other => {
            return Err(
                format!("final Forge fact is not the receiving-Team offer: {other:?}").into(),
            );
        }
    }

    let control = flow.source.control_client.clone();
    let scope = flow.source.scope.clone();
    let migration_id = flow.progress.migration_id;
    let destination_id = flow.progress.destination_id;
    let later = tokio::task::spawn_blocking(move || {
        control
            .effect_fact_outbox(super::EffectFactOutboxReadRequest {
                scope,
                migration_id,
                destination_id,
                after: final_offer.source_sequence,
                limit: 1,
            })
            .map_err(|error| error.to_string())
    })
    .await??;
    assert!(
        later.is_empty(),
        "Forge has a later source fact after the R offer"
    );
    Ok(catch_up.applied_through)
}

async fn assert_current_workspace_leader(
    context: &TemporaryDispatchFlowContext<'_>,
) -> TestResult<i64> {
    let leadership = context
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
        .ok_or("Workspace Orchestrator disappeared before temporary dispatch")?;
    assert_eq!(
        leadership.leader_participant_id,
        Some(context.workspace_leader.participant_id)
    );
    assert_eq!(
        leadership.leadership_revision,
        context.workspace_leadership_revision
    );
    assert!(matches!(
        leadership.state.as_str(),
        "stabilizing" | "active"
    ));
    Ok(leadership.leadership_revision)
}

async fn create_receiving_team(
    context: &TemporaryDispatchFlowContext<'_>,
    workspace_leadership_revision: i64,
) -> TestResult<ReceivingTeamMembers> {
    let (leader, peer) = provision_receiving_team_candidates(context)
        .await
        .map_err(|error| format!("temporary-dispatch R Ready candidate provisioning: {error:?}"))?;

    let team_id = Uuid::now_v7();
    let request = TeamCreateV2Request {
        scope: context.creator.scope.clone(),
        request_id: Uuid::now_v7(),
        team_id,
        activity_id: context.creator.activity_id,
        session_id: context.workspace_leader.session_id,
        expected_sender_leadership_revision: workspace_leadership_revision,
        name: "Temporary dispatch receiving".to_owned(),
        purpose: "Receive one source-backed temporary dispatch reservation".to_owned(),
        initial_members: vec![
            TeamRosterMemberRequest {
                activity_id: context.creator.activity_id,
                session_id: leader.session_id,
                role: "member".to_owned(),
            },
            TeamRosterMemberRequest {
                activity_id: context.creator.activity_id,
                session_id: peer.session_id,
                role: "member".to_owned(),
            },
        ],
        // The two formed members plus the one incoming dispatch target.
        max_members: 3,
    };
    let created = context
        .client
        .create_team_v2(context.bearer, &request)
        .await
        .map_err(|error| {
            format!(
                "temporary-dispatch R TeamCreateV2 mutation request_id={}: {error:?}",
                request.request_id
            )
        })?;
    assert_receiving_team_create_v2(context, &request, &created)
        .await
        .map_err(|error| {
            format!(
                "temporary-dispatch R TeamCreateV2 receipt/cap/digest readback request_id={}: {error:?}",
                request.request_id
            )
        })?;
    let readback = context
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
        .await
        .map_err(|error| {
            format!(
                "temporary-dispatch R current Team/roster snapshot team_id={team_id}: {error:?}"
            )
        })?;
    assert_eq!(readback.team.as_ref(), Some(&created.mutation.team));
    assert_eq!(readback.members.len(), 2);
    assert!(!readback.members_truncated);
    let leader_membership = team_member(&readback.members, &leader)?;
    let peer_membership = team_member(&readback.members, &peer)?;
    assert_eq!(leader_membership.role.as_deref(), Some("member"));
    assert_eq!(peer_membership.role.as_deref(), Some("member"));
    Ok(ReceivingTeamMembers {
        team_id,
        leader,
        peer,
        leader_membership,
        peer_membership,
    })
}

async fn provision_receiving_team_candidates(
    context: &TemporaryDispatchFlowContext<'_>,
) -> TestResult<(ReadyFixtureMember, ReadyFixtureMember)> {
    let leader = super::assert_ready_profile_member(
        context.client,
        context.bearer,
        context.principal,
        context.source,
        context.creator,
        "isolated-tf-requesting-team-leader",
    )
    .await
    .map_err(|error| format!("temporary-dispatch R Team Leader Ready activation: {error:?}"))?;
    let peer = super::assert_ready_profile_member(
        context.client,
        context.bearer,
        context.principal,
        context.source,
        context.creator,
        "isolated-tf-requesting-team-peer",
    )
    .await
    .map_err(|error| format!("temporary-dispatch R peer Ready activation: {error:?}"))?;
    assert_distinct_member(&leader, &peer);
    assert_distinct_member(&leader, context.target);
    assert_distinct_member(&leader, context.home_leader);
    assert_distinct_member(&leader, context.workspace_leader);
    assert_distinct_member(&peer, context.target);
    assert_distinct_member(&peer, context.home_leader);
    assert_distinct_member(&peer, context.workspace_leader);
    Ok((leader, peer))
}

async fn assert_receiving_team_create_v2(
    context: &TemporaryDispatchFlowContext<'_>,
    request: &TeamCreateV2Request,
    created: &TeamMutationV2Response,
) -> TestResult {
    assert!(created.mutation.created);
    assert!(!created.mutation.replayed);
    assert_eq!(created.mutation.team.team_id, request.team_id);
    assert_eq!(created.mutation.initial_members.len(), 2);
    let capacity = created
        .mutation
        .capacity
        .as_ref()
        .ok_or("v2 receiving Team create omitted its cap receipt")?;
    assert_eq!(capacity.max_members, request.max_members);
    assert_eq!(capacity.team_id, request.team_id);
    assert_eq!(capacity.workspace_id, request.scope.workspace_id);
    assert_eq!(capacity.revision, 1);
    assert_eq!(
        capacity.leadership_revision,
        request.expected_sender_leadership_revision
    );
    assert_eq!(capacity.actor.activity_id, request.activity_id);
    assert_eq!(capacity.actor.agent_id, context.workspace_leader.agent_id);
    assert_eq!(capacity.actor.session_id, request.session_id);
    assert_eq!(
        capacity.actor.participant_id,
        context.workspace_leader.participant_id
    );
    assert_eq!(
        capacity.actor.profile_id,
        context.workspace_leader.profile_id
    );
    let home_members = read_team_members(
        context,
        context.home_team_id,
        context.workspace_leader.session_id,
    )
    .await?;
    let creator = team_member(&home_members, context.workspace_leader)?;
    assert_eq!(capacity.actor.participation_id, creator.participation_id);
    let digest = created
        .mutation
        .initial_roster_digest_v1
        .as_ref()
        .ok_or("v2 receiving Team create omitted its roster digest")?;
    assert_eq!(digest.len(), 64);

    let source_receipt = context
        .client
        .team_create_mutation_readback_v2(
            context.bearer,
            request,
            TeamMutationReaderV2Wire {
                activity_id: request.activity_id,
                session_id: request.session_id,
            },
        )
        .await
        .map_err(|error| {
            format!(
                "temporary-dispatch R TeamCreateV2 owner receipt readback request_id={}: {error:?}",
                request.request_id
            )
        })?
        .ok_or("owner-scoped v2 receiving Team create receipt is absent")?;
    let mut expected = created.mutation.clone();
    expected.replayed = true;
    assert_eq!(source_receipt.mutation, expected);
    Ok(())
}

fn assert_distinct_member(first: &ReadyFixtureMember, second: &ReadyFixtureMember) {
    assert_ne!(first.session_id, second.session_id);
    assert_ne!(first.agent_id, second.agent_id);
    assert_ne!(first.participant_id, second.participant_id);
    assert_ne!(first.profile_id, second.profile_id);
}

fn team_member(
    members: &[TeamMemberWire],
    identity: &ReadyFixtureMember,
) -> TestResult<TeamMemberWire> {
    let member = members
        .iter()
        .find(|member| member.profile_id == Some(identity.profile_id))
        .cloned()
        .ok_or("Ready Profile is absent from its Team readback")?;
    assert!(!member.participation_id.is_nil());
    assert_eq!(member.state, "joined");
    assert!(member.revision > 0);
    Ok(member)
}

async fn establish_receiving_team_leader(
    context: &TemporaryDispatchFlowContext<'_>,
    receiving: &ReceivingTeamMembers,
    workspace_leadership_revision: i64,
) -> TestResult<ReceivingTeamLeadership> {
    let offer_context = TeamAffinedOfferContext {
        client: context.client,
        bearer: context.bearer,
        source: context.source,
        creator: context.creator,
        workspace_leader: context.workspace_leader,
        workspace_leadership_revision,
        principal: context.principal,
        team_id: receiving.team_id,
        target: &receiving.leader,
        assignments: context.assignments,
        node: &context.assignments.dispatch_node,
    };
    let assignment = offer_team_affined_assignment(&offer_context).await?;
    let leadership_revision = confirm_team_leadership(
        context.client,
        context.bearer,
        context.creator,
        &receiving.leader,
        receiving.team_id,
    )
    .await?;
    assert!(leadership_revision > 0);
    assert_ne!(
        assignment.target.session_id().into_uuid(),
        context.workspace_leader.session_id
    );
    Ok(ReceivingTeamLeadership {
        revision: leadership_revision,
        assignment,
    })
}

async fn lifecycle_authorities(
    context: &TemporaryDispatchFlowContext<'_>,
    receiving: &ReceivingTeamMembers,
    workspace_leadership_revision: i64,
    requesting_leadership_revision: i64,
) -> TestResult<LifecycleAuthoritySnapshot> {
    let home = read_team_members(
        context,
        context.home_team_id,
        context.workspace_leader.session_id,
    )
    .await?;
    let receiving_members = read_team_members(
        context,
        receiving.team_id,
        context.workspace_leader.session_id,
    )
    .await?;
    let target_membership = team_member(&home, context.target)?;
    let home_leader_membership = team_member(&home, context.home_leader)?;
    let workspace_leader_membership = team_member(&home, context.workspace_leader)?;
    let requesting_leader_membership = team_member(&receiving_members, &receiving.leader)?;
    let peer_membership = team_member(&receiving_members, &receiving.peer)?;
    assert_eq!(requesting_leader_membership, receiving.leader_membership);
    assert_eq!(peer_membership, receiving.peer_membership);

    let home_leadership = team_leadership_view(
        context.client,
        context.bearer,
        context.creator,
        context.home_team_id,
        context.home_leader,
    )
    .await?;
    assert_eq!(
        home_leadership.leader_participant_id,
        Some(context.home_leader.participant_id)
    );
    assert!(matches!(
        home_leadership.state.as_str(),
        "stabilizing" | "active"
    ));
    let workspace_leadership = context
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
        .ok_or("Workspace Orchestrator leadership missing before consent")?;
    assert_eq!(
        workspace_leadership.leadership_revision,
        workspace_leadership_revision
    );
    assert_eq!(
        workspace_leadership.leader_participant_id,
        Some(context.workspace_leader.participant_id)
    );
    let requesting_leadership = team_leadership_view(
        context.client,
        context.bearer,
        context.creator,
        receiving.team_id,
        &receiving.leader,
    )
    .await?;
    assert_eq!(
        requesting_leadership.leader_participant_id,
        Some(receiving.leader.participant_id)
    );
    assert_eq!(
        requesting_leadership.leadership_revision,
        requesting_leadership_revision
    );
    Ok(LifecycleAuthoritySnapshot {
        workspace_leadership_revision: u64::try_from(workspace_leadership_revision)?,
        home_leadership_revision: u64::try_from(home_leadership.leadership_revision)?,
        requesting_leadership_revision: u64::try_from(requesting_leadership.leadership_revision)?,
        target_membership,
        home_leader_membership,
        workspace_leader_membership,
        requesting_leader_membership,
    })
}

async fn read_team_members(
    context: &TemporaryDispatchFlowContext<'_>,
    team_id: Uuid,
    viewer_session_id: Uuid,
) -> TestResult<Vec<TeamMemberWire>> {
    let response = context
        .client
        .team_get(
            context.bearer,
            &TeamGetQueryRequest {
                scope: context.creator.scope.clone(),
                session_id: Some(viewer_session_id),
                team_id,
                request_id: None,
            },
        )
        .await?;
    if response.team.is_none() || response.members_truncated {
        return Err("Team roster readback is missing or truncated".into());
    }
    Ok(response.members)
}

async fn team_leadership_view(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    creator: &BootstrapTarget,
    team_id: Uuid,
    actor: &ReadyFixtureMember,
) -> TestResult<heterarchy_orchestration_backend::wire::leadership::LeadershipWire> {
    client
        .leadership_view(
            bearer,
            &LeadershipQueryRequest {
                scope: creator.scope.clone(),
                session_id: Some(actor.session_id),
                activity_id: None,
                team_id: Some(team_id),
                review_after: None,
                limit: 10,
            },
        )
        .await?
        .leadership
        .ok_or_else(|| "current Team leadership is missing".into())
}

async fn admit_exact_dispatch(
    context: &TemporaryDispatchFlowContext<'_>,
    receiving: &ReceivingTeamMembers,
    authorities: &LifecycleAuthoritySnapshot,
) -> TestResult<admission_wire::TemporaryDispatchAdmissionRequest> {
    let requested_end_at_ms = current_test_time()?
        .checked_add(5 * 60 * 1_000)
        .ok_or("temporary dispatch end time overflow")?;
    let request = admission_wire::TemporaryDispatchAdmissionRequest {
        scope: context.creator.scope.clone(),
        requesting_team_id: receiving.team_id,
        request_id: Uuid::now_v7(),
        activity_id: context.creator.activity_id,
        session_id: receiving.leader.session_id,
        expected_leadership_revision: authorities.requesting_leadership_revision,
        target_participant_id: context.target.participant_id,
        requested_scope: "accept one bounded source-backed temporary dispatch".to_owned(),
        requested_end_at_ms,
    };
    let response = context
        .client
        .admit_temporary_dispatch(context.bearer, &request)
        .await?;
    assert_eq!(response.request, request);
    let admission_wire::TemporaryDispatchAdmissionOutcomeWire::Admitted { receipt } =
        response.outcome
    else {
        return Err("isolated temporary dispatch unexpectedly hit quota".into());
    };
    let receipt = *receipt;
    assert_admission_receipt(context, receiving, authorities, &request, &receipt);
    let readback = context
        .client
        .temporary_dispatch_readback(
            context.bearer,
            &admission_wire::TemporaryDispatchReadbackRequest {
                intent: request.clone(),
                current_scope: context.creator.scope.clone(),
                current_actor: admission_wire::TemporaryDispatchCurrentActorWire {
                    activity_id: context.creator.activity_id,
                    session_id: receiving.leader.session_id,
                },
            },
        )
        .await?
        .ok_or("exact temporary dispatch admission readback is absent")?;
    assert!(!receipt.replayed);
    assert!(readback.replayed);
    let mut expected_readback = receipt.clone();
    expected_readback.replayed = true;
    assert_eq!(readback, expected_readback);
    Ok(request)
}

fn assert_admission_receipt(
    context: &TemporaryDispatchFlowContext<'_>,
    receiving: &ReceivingTeamMembers,
    authorities: &LifecycleAuthoritySnapshot,
    request: &admission_wire::TemporaryDispatchAdmissionRequest,
    receipt: &admission_wire::TemporaryDispatchReceiptWire,
) {
    assert_eq!(receipt.request_id, request.request_id);
    assert_eq!(receipt.workspace_id, request.scope.workspace_id);
    assert_eq!(receipt.requesting_team_id, receiving.team_id);
    assert_eq!(
        receipt.leadership_revision,
        authorities.requesting_leadership_revision
    );
    assert_eq!(receipt.target_home_team_id, context.home_team_id);
    assert_eq!(receipt.target_participant_id, context.target.participant_id);
    assert_eq!(receipt.requested_scope, request.requested_scope);
    assert_eq!(receipt.requested_end_at_ms, request.requested_end_at_ms);
    assert!(receipt.admitted_at_ms >= 0);
    assert!(receipt.requested_end_at_ms > receipt.admitted_at_ms);
    assert!(!receipt.replayed);
    assert_admission_actor_receipt(
        &receipt.actor,
        context.creator.activity_id,
        &receiving.leader,
        &authorities.requesting_leader_membership,
    );
}

async fn assert_wrong_role_consent_has_no_receipt(
    context: &TemporaryDispatchFlowContext<'_>,
    admission: &admission_wire::TemporaryDispatchAdmissionRequest,
    authorities: &LifecycleAuthoritySnapshot,
    unauthorized_actor: &ReadyFixtureMember,
) -> TestResult {
    let request = consent_request(
        context,
        admission.request_id,
        unauthorized_actor,
        TemporaryDispatchConsentKindWire::HomeLeaderConsent,
        Uuid::now_v7(),
        0,
    );
    let denied = context
        .client
        .temporary_dispatch_lifecycle_mutate(context.bearer, &request)
        .await;
    assert!(
        matches!(
            denied,
            Err(ClientError::Rejected { status, .. })
                if status == StatusCode::CONFLICT.as_u16()
        ),
        "Receiving Team Leader cannot write the Home Leader consent: {denied:?}"
    );
    let readback_request =
        lifecycle_readback_request(request.clone(), context.creator, unauthorized_actor);
    assert_eq!(readback_request.current_actor, request.actor);
    let absent = context
        .client
        .temporary_dispatch_lifecycle_readback(context.bearer, &readback_request)
        .await?;
    assert_eq!(absent.request, readback_request);
    assert!(absent.result.is_none(), "rejected consent left a receipt");
    assert!(authorities.home_leadership_revision > 0);
    Ok(())
}

struct ConsentReceipts {
    target: lifecycle_wire::TemporaryDispatchConsentReceiptWire,
    home_leader: lifecycle_wire::TemporaryDispatchConsentReceiptWire,
    workspace_leader: lifecycle_wire::TemporaryDispatchConsentReceiptWire,
}

async fn record_current_consents(
    context: &TemporaryDispatchFlowContext<'_>,
    admission: &admission_wire::TemporaryDispatchAdmissionRequest,
    authorities: &LifecycleAuthoritySnapshot,
) -> TestResult<ConsentReceipts> {
    let target = record_consent(
        context,
        admission.request_id,
        context.target,
        &authorities.target_membership,
        TemporaryDispatchConsentKindWire::TargetAcceptance,
        authorities.target_membership.revision,
    )
    .await?;
    let home_leader = record_consent(
        context,
        admission.request_id,
        context.home_leader,
        &authorities.home_leader_membership,
        TemporaryDispatchConsentKindWire::HomeLeaderConsent,
        authorities.home_leadership_revision,
    )
    .await?;
    let workspace_leader = record_consent(
        context,
        admission.request_id,
        context.workspace_leader,
        &authorities.workspace_leader_membership,
        TemporaryDispatchConsentKindWire::WorkspaceOrchestratorApproval,
        authorities.workspace_leadership_revision,
    )
    .await?;
    Ok(ConsentReceipts {
        target,
        home_leader,
        workspace_leader,
    })
}

async fn record_consent(
    context: &TemporaryDispatchFlowContext<'_>,
    request_id: Uuid,
    actor: &ReadyFixtureMember,
    membership: &TeamMemberWire,
    kind: TemporaryDispatchConsentKindWire,
    authority_revision: u64,
) -> TestResult<lifecycle_wire::TemporaryDispatchConsentReceiptWire> {
    let request = consent_request(context, request_id, actor, kind, Uuid::now_v7(), 0);
    let response = context
        .client
        .temporary_dispatch_lifecycle_mutate(context.bearer, &request)
        .await?;
    assert_eq!(response.request, request);
    let lifecycle_wire::TemporaryDispatchLifecycleResultWire::Consent { receipt } = response.result
    else {
        return Err("temporary dispatch consent returned the wrong result kind".into());
    };
    let receipt = *receipt;
    assert_eq!(receipt.request_id, request_id);
    assert_eq!(receipt.receipt_id, receipt_id(&request)?);
    assert_eq!(receipt.kind, kind);
    assert_eq!(receipt.revision, 1);
    assert_eq!(receipt.authority_revision, authority_revision);
    assert!(receipt.recorded_at_ms >= 0);
    assert!(!receipt.replayed);
    assert_actor_receipt(
        &receipt.actor,
        context.creator.activity_id,
        actor,
        membership,
    );

    if kind == TemporaryDispatchConsentKindWire::TargetAcceptance {
        let replay = context
            .client
            .temporary_dispatch_lifecycle_mutate(context.bearer, &request)
            .await?;
        let lifecycle_wire::TemporaryDispatchLifecycleResultWire::Consent { receipt: replayed } =
            replay.result
        else {
            return Err("consent replay returned the wrong result kind".into());
        };
        assert!(replayed.replayed);
        assert_eq!(replayed.request_id, receipt.request_id);
        assert_eq!(replayed.receipt_id, receipt.receipt_id);
        assert_eq!(replayed.revision, receipt.revision);
        assert_eq!(replayed.authority_revision, receipt.authority_revision);
        assert_eq!(replayed.actor, receipt.actor);
        assert_eq!(replayed.recorded_at_ms, receipt.recorded_at_ms);
    }
    assert_consent_readback(context, &request, actor, &receipt).await?;
    Ok(receipt)
}

async fn assert_consent_readback(
    context: &TemporaryDispatchFlowContext<'_>,
    intent: &lifecycle_wire::TemporaryDispatchLifecycleMutationRequest,
    actor: &ReadyFixtureMember,
    expected: &lifecycle_wire::TemporaryDispatchConsentReceiptWire,
) -> TestResult {
    let request = lifecycle_readback_request(intent.clone(), context.creator, actor);
    let response = context
        .client
        .temporary_dispatch_lifecycle_readback(context.bearer, &request)
        .await?;
    assert_eq!(response.request, request);
    let Some(lifecycle_wire::TemporaryDispatchLifecycleResultWire::Consent { receipt }) =
        response.result
    else {
        return Err("exact consent receipt is absent or has the wrong result kind".into());
    };
    assert_eq!(receipt.as_ref(), expected);
    Ok(())
}

fn consent_request(
    context: &TemporaryDispatchFlowContext<'_>,
    request_id: Uuid,
    actor: &ReadyFixtureMember,
    kind: TemporaryDispatchConsentKindWire,
    receipt_id: Uuid,
    expected_revision: u64,
) -> lifecycle_wire::TemporaryDispatchLifecycleMutationRequest {
    lifecycle_wire::TemporaryDispatchLifecycleMutationRequest {
        actor: lifecycle_actor(context.creator, actor),
        request_id,
        action: lifecycle_wire::TemporaryDispatchLifecycleActionWire::Consent {
            receipt_id,
            kind,
            expected_revision,
        },
    }
}

fn receipt_id(
    request: &lifecycle_wire::TemporaryDispatchLifecycleMutationRequest,
) -> TestResult<Uuid> {
    match request.action {
        lifecycle_wire::TemporaryDispatchLifecycleActionWire::Consent { receipt_id, .. } => {
            Ok(receipt_id)
        }
        _ => Err("expected consent request".into()),
    }
}

fn lifecycle_actor(
    creator: &BootstrapTarget,
    actor: &ReadyFixtureMember,
) -> lifecycle_wire::TemporaryDispatchLifecycleActorWire {
    lifecycle_wire::TemporaryDispatchLifecycleActorWire {
        scope: creator.scope.clone(),
        activity_id: creator.activity_id,
        session_id: actor.session_id,
    }
}

fn lifecycle_readback_request(
    intent: lifecycle_wire::TemporaryDispatchLifecycleMutationRequest,
    creator: &BootstrapTarget,
    current_actor: &ReadyFixtureMember,
) -> lifecycle_wire::TemporaryDispatchLifecycleReadbackRequest {
    lifecycle_wire::TemporaryDispatchLifecycleReadbackRequest {
        intent,
        current_actor: lifecycle_actor(creator, current_actor),
    }
}

fn assert_actor_receipt(
    actual: &lifecycle_wire::TemporaryDispatchLifecycleActorReceiptWire,
    activity_id: Uuid,
    member: &ReadyFixtureMember,
    membership: &TeamMemberWire,
) {
    assert_eq!(actual.activity_id, activity_id);
    assert_eq!(actual.agent_id, member.agent_id);
    assert_eq!(actual.session_id, member.session_id);
    assert_eq!(actual.participation_id, membership.participation_id);
    assert_eq!(actual.profile_id, member.profile_id);
    assert_eq!(actual.participant_id, member.participant_id);
    assert!(!actual.display_name.is_empty());
}

fn assert_admission_actor_receipt(
    actual: &admission_wire::TemporaryDispatchActorReceiptWire,
    activity_id: Uuid,
    member: &ReadyFixtureMember,
    membership: &TeamMemberWire,
) {
    assert_eq!(actual.activity_id, activity_id);
    assert_eq!(actual.agent_id, member.agent_id);
    assert_eq!(actual.session_id, member.session_id);
    assert_eq!(actual.participation_id, membership.participation_id);
    assert_eq!(actual.profile_id, member.profile_id);
    assert_eq!(actual.participant_id, member.participant_id);
    assert!(!actual.display_name.is_empty());
}

async fn reserve_and_recover(
    context: &TemporaryDispatchFlowContext<'_>,
    admission: admission_wire::TemporaryDispatchAdmissionRequest,
    authorities: &LifecycleAuthoritySnapshot,
    consents: &ConsentReceipts,
) -> TestResult {
    let request = lifecycle_wire::TemporaryDispatchLifecycleMutationRequest {
        actor: lifecycle_actor(context.creator, context.workspace_leader),
        request_id: admission.request_id,
        action: lifecycle_wire::TemporaryDispatchLifecycleActionWire::Reserve {
            expected_revision: 0,
        },
    };
    let recovered =
        send_reservation_through_response_drop(context.backend_address, context.bearer, &request)
            .await?;
    let lifecycle_wire::TemporaryDispatchLifecycleResultWire::Reservation { receipt } = recovered
    else {
        return Err("reservation exact readback returned the wrong result kind".into());
    };
    assert_reservation_receipt(context, &admission, authorities, consents, receipt.as_ref());
    Ok(())
}

async fn send_reservation_through_response_drop(
    backend_address: SocketAddr,
    bearer: &HeaderValue,
    request: &lifecycle_wire::TemporaryDispatchLifecycleMutationRequest,
) -> TestResult<lifecycle_wire::TemporaryDispatchLifecycleResultWire> {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let proxy_address = listener.local_addr()?;
    let expected = request.clone();
    let expected_bearer = bearer.clone();
    let proxy = tokio::spawn(async move {
        drop_successful_lifecycle_response_once(
            listener,
            backend_address,
            &expected_bearer,
            &expected,
        )
        .await
    });
    let proxy_client = OrchestrationHttpClient::new(proxy_address, super::DEADLINE)?;
    let outcome = proxy_client
        .temporary_dispatch_lifecycle_mutate(bearer, request)
        .await;
    match outcome {
        Err(ClientError::UnknownOutcome {
            readback: ReadbackTarget::TemporaryDispatchLifecycleMutation(original),
        }) if original.as_ref() == request => {}
        other => {
            return Err(format!(
                "dropped committed response did not retain exact intent: {other:?}"
            )
            .into());
        }
    }
    proxy.await??;

    let recovery = lifecycle_wire::TemporaryDispatchLifecycleReadbackRequest {
        intent: request.clone(),
        current_actor: request.actor.clone(),
    };
    let direct_client = OrchestrationHttpClient::new(backend_address, super::DEADLINE)?;
    let response = direct_client
        .temporary_dispatch_lifecycle_readback(bearer, &recovery)
        .await?;
    assert_eq!(response.request, recovery);
    let result = response
        .result
        .ok_or("committed reservation is absent after response loss")?;
    Ok(result)
}

async fn drop_successful_lifecycle_response_once(
    listener: TcpListener,
    backend_address: SocketAddr,
    expected_bearer: &HeaderValue,
    expected: &lifecycle_wire::TemporaryDispatchLifecycleMutationRequest,
) -> Result<(), String> {
    let (mut downstream, _) = listener.accept().await.map_err(|error| error.to_string())?;
    let request_bytes = read_http_request(&mut downstream).await?;
    let (method, path, body) = http_request_parts(&request_bytes)?;
    if method != "POST" {
        return Err("response-drop proxy received a non-POST request".to_owned());
    }
    let expected_path = format!(
        "/internal/v1/workspaces/{}/temporary-dispatch/lifecycle",
        expected.actor.scope.workspace_id
    );
    if path != expected_path.as_str() {
        return Err("response-drop proxy received a different HTTP path".to_owned());
    }
    let parsed: lifecycle_wire::TemporaryDispatchLifecycleMutationRequest =
        serde_json::from_slice(body).map_err(|error| error.to_string())?;
    if &parsed != expected {
        return Err("response-drop proxy received a different mutation intent".to_owned());
    }
    if request_authorization(&request_bytes)?
        != expected_bearer.to_str().map_err(|e| e.to_string())?
    {
        return Err("response-drop proxy received a different bearer".to_owned());
    }
    let upstream = OrchestrationHttpClient::new(backend_address, super::DEADLINE)
        .map_err(|error| error.to_string())?;
    let response = upstream
        .temporary_dispatch_lifecycle_mutate(expected_bearer, &parsed)
        .await
        .map_err(|error| format!("real backend mutation failed before response drop: {error:?}"))?;
    assert_eq!(response.request, *expected);
    match response.result {
        lifecycle_wire::TemporaryDispatchLifecycleResultWire::Reservation { receipt }
            if receipt.request_id == expected.request_id
                && receipt.state
                    == lifecycle_wire::TemporaryDispatchReservationStateWire::Reserved
                && !receipt.replayed => {}
        _ => {
            return Err("upstream success response did not prove the exact reservation".to_owned());
        }
    }
    // Deliberately close downstream after the real service returned its
    // successful committed response. The caller must use readback only.
    drop(downstream);
    Ok(())
}

pub(super) async fn read_http_request(stream: &mut TcpStream) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    loop {
        let mut chunk = [0_u8; 4096];
        let count = stream
            .read(&mut chunk)
            .await
            .map_err(|error| error.to_string())?;
        if count == 0 {
            return Err("client closed before sending complete HTTP request".to_owned());
        }
        bytes.extend_from_slice(&chunk[..count]);
        let Some(header_end) = header_end(&bytes) else {
            continue;
        };
        let content_length = content_length(&bytes[..header_end])?;
        if bytes.len() >= header_end + 4 + content_length {
            bytes.truncate(header_end + 4 + content_length);
            return Ok(bytes);
        }
    }
}

pub(super) fn http_request_parts(bytes: &[u8]) -> Result<(&str, &str, &[u8]), String> {
    let end = header_end(bytes).ok_or_else(|| "HTTP request header is incomplete".to_owned())?;
    let header = std::str::from_utf8(&bytes[..end]).map_err(|error| error.to_string())?;
    let request_line = header
        .lines()
        .next()
        .ok_or_else(|| "HTTP request line is invalid".to_owned())?;
    let mut fields = request_line.split_whitespace();
    let method = fields
        .next()
        .ok_or_else(|| "HTTP request method is missing".to_owned())?;
    let path = fields
        .next()
        .ok_or_else(|| "HTTP request path is missing".to_owned())?;
    Ok((method, path, &bytes[end + 4..]))
}

pub(super) fn request_authorization(bytes: &[u8]) -> Result<&str, String> {
    let end = header_end(bytes).ok_or_else(|| "HTTP request header is incomplete".to_owned())?;
    let header = std::str::from_utf8(&bytes[..end]).map_err(|error| error.to_string())?;
    header
        .lines()
        .skip(1)
        .find_map(|line| {
            line.split_once(':')
                .filter(|(name, _)| name.eq_ignore_ascii_case("authorization"))
                .map(|(_, value)| value.trim())
        })
        .ok_or_else(|| "HTTP bearer header is missing".to_owned())
}

fn header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

fn content_length(header: &[u8]) -> Result<usize, String> {
    let header = std::str::from_utf8(header).map_err(|error| error.to_string())?;
    header
        .lines()
        .skip(1)
        .find_map(|line| {
            line.split_once(':')
                .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .map(|(_, value)| value.trim())
        })
        .ok_or_else(|| "HTTP Content-Length is missing".to_owned())?
        .parse::<usize>()
        .map_err(|error| error.to_string())
}

fn assert_reservation_receipt(
    context: &TemporaryDispatchFlowContext<'_>,
    admission: &admission_wire::TemporaryDispatchAdmissionRequest,
    authorities: &LifecycleAuthoritySnapshot,
    consents: &ConsentReceipts,
    receipt: &lifecycle_wire::TemporaryDispatchReservationReceiptWire,
) {
    assert_eq!(receipt.request_id, admission.request_id);
    assert_eq!(
        receipt.state,
        lifecycle_wire::TemporaryDispatchReservationStateWire::Reserved
    );
    assert_eq!(receipt.revision, 1);
    assert_eq!(
        receipt.target_participation_id,
        authorities.target_membership.participation_id
    );
    assert_eq!(receipt.target_profile_id, context.target.profile_id);
    assert_eq!(
        receipt.target_membership_revision,
        authorities.target_membership.revision
    );
    assert_eq!(
        receipt.requesting_leadership_revision,
        authorities.requesting_leadership_revision
    );
    assert_eq!(
        receipt.home_leadership_revision,
        authorities.home_leadership_revision
    );
    assert_eq!(
        receipt.workspace_leadership_revision,
        authorities.workspace_leadership_revision
    );
    assert_eq!(
        receipt.target_consent_receipt_id,
        consents.target.receipt_id
    );
    assert_eq!(
        receipt.home_leader_consent_receipt_id,
        consents.home_leader.receipt_id
    );
    assert_eq!(
        receipt.orchestrator_consent_receipt_id,
        consents.workspace_leader.receipt_id
    );
    assert_actor_receipt(
        &receipt.actor,
        context.creator.activity_id,
        context.workspace_leader,
        &authorities.workspace_leader_membership,
    );
    assert!(receipt.recorded_at_ms >= 0);
    assert!(!receipt.replayed);
}
