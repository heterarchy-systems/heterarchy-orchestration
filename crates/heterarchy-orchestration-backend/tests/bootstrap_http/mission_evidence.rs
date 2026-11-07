//! Real configured Forge HTTP source-evidence flow inside the isolated Broker fixture.

use std::time::Duration;

use forge_domain::AgentAssignmentRevision;
use heterarchy_orchestration_backend::client::{
    ClientError, OrchestrationHttpClient, ReadbackTarget,
};
use heterarchy_orchestration_backend::wire::mission_evaluation::MissionEvaluationMutationContextWire;
use heterarchy_orchestration_backend::wire::mission_evidence::{
    MissionEvidenceCandidateMemberWire, MissionEvidenceCandidatePartitionWire,
    MissionEvidenceManifestBindRequest, MissionEvidenceMutationRequest,
    MissionEvidenceMutationStatusWire, MissionEvidenceMutationWire,
    MissionEvidenceObservationRequest, MissionEvidencePlanDigestsRequest,
    MissionEvidenceRequestReadActor,
};
use heterarchy_orchestration_backend::wire::participation::{
    ParticipantTarget, PresenceQueryRequest,
};
use hyper::header::HeaderValue;
use uuid::Uuid;

use super::{ForgeHost, MemberTarget, ReadyFixtureMember, TestResult, current_test_time};

/// Exact requests and responses retained for a later post-rebind readback.
pub(super) struct MissionEvidenceRecoveryFixture {
    binding_request: MissionEvidenceManifestBindRequest,
    binding_response:
        heterarchy_orchestration_backend::wire::mission_evidence::MissionEvidenceManifestBindResponse,
    observation_request: MissionEvidenceObservationRequest,
    observation_response:
        heterarchy_orchestration_backend::wire::mission_evidence::MissionEvidenceObservationResponse,
}

/// Exact source and local selectors for one real Mission evidence flow.
pub(super) struct MissionEvidenceFlow<'a> {
    pub(super) client: &'a OrchestrationHttpClient,
    pub(super) bearer: &'a HeaderValue,
    pub(super) source: &'a ForgeHost,
    pub(super) target: &'a MemberTarget,
    pub(super) leader: &'a ReadyFixtureMember,
    pub(super) assignee: &'a ReadyFixtureMember,
    pub(super) leadership_revision: i64,
    pub(super) mission_id: Uuid,
    pub(super) plan_id: Uuid,
    pub(super) plan_revision: u64,
    pub(super) obligation_id: Uuid,
    pub(super) work_unit_id: Uuid,
    pub(super) assignment_id: Uuid,
    pub(super) t1_submission_cutoff_ms: i64,
}

impl MissionEvidenceFlow<'_> {
    fn mutation_context(&self) -> MissionEvaluationMutationContextWire {
        MissionEvaluationMutationContextWire {
            scope: self.target.scope.clone(),
            session_id: self.leader.session_id,
            sender_activity_id: self.target.activity_id,
            expected_workspace_leadership_revision: self.leadership_revision,
        }
    }

    fn reader(&self) -> MissionEvidenceRequestReadActor {
        MissionEvidenceRequestReadActor {
            sender_activity_id: self.target.activity_id,
            session_id: self.leader.session_id,
        }
    }
}

struct PreparedManifest {
    request: MissionEvidenceManifestBindRequest,
    manifest_id: Uuid,
    digest: String,
}

struct RecordedObservation {
    request: MissionEvidenceObservationRequest,
    response:
        heterarchy_orchestration_backend::wire::mission_evidence::MissionEvidenceObservationResponse,
}

/// Fetch and seal the exact Forge offer into the native journal, close the
/// fixed T1 after its real deadline, and append the complete source-cut vector.
/// The final receipt is read by the original semantic actor; a foreign Ready
/// actor is separately rejected by the exact-receipt readback check.
pub(super) async fn assert_mission_evidence_http_flow(
    flow: &MissionEvidenceFlow<'_>,
) -> TestResult<MissionEvidenceRecoveryFixture> {
    let prepared = prepare_manifest(flow).await?;
    let binding_response = bind_manifest(flow, &prepared).await?;
    let observation = observe_after_cutoff(flow).await?;
    let recovery = MissionEvidenceRecoveryFixture {
        binding_request: prepared.request,
        binding_response,
        observation_request: observation.request,
        observation_response: observation.response,
    };
    assert_actor_readback_rejects_foreign_and_reads_exact_actor(
        flow.client,
        flow.bearer,
        &recovery,
        flow.target,
        flow.reader(),
    )
    .await
    .map_err(|error| format!("Mission evidence actor-bound recovery: {error:?}"))?;
    Ok(recovery)
}

async fn prepare_manifest(flow: &MissionEvidenceFlow<'_>) -> TestResult<PreparedManifest> {
    let plan_digests = flow
        .client
        .mission_evidence_plan_digests(
            flow.bearer,
            &MissionEvidencePlanDigestsRequest {
                scope: flow.target.scope.clone(),
                session_id: flow.leader.session_id,
                sender_activity_id: flow.target.activity_id,
                mission_id: flow.mission_id,
                plan_id: flow.plan_id,
                revision: flow.plan_revision,
            },
        )
        .await
        .map_err(|error| format!("Mission evidence plan digests: {error:?}"))?;
    assert_eq!(plan_digests.mission_id, flow.mission_id);
    assert_eq!(plan_digests.plan_id, flow.plan_id);
    assert_eq!(plan_digests.revision, flow.plan_revision);
    assert!(plan_digests.member_criteria_digests.iter().any(|member| {
        member.obligation_id == flow.obligation_id && member.work_unit_id == flow.work_unit_id
    }));

    let request = MissionEvidenceManifestBindRequest {
        context: flow.mutation_context(),
        request_id: Uuid::now_v7(),
        mission_id: flow.mission_id,
        plan_id: flow.plan_id,
        plan_revision: flow.plan_revision,
        partitions: vec![MissionEvidenceCandidatePartitionWire {
            activity_id: flow.target.activity_id,
            members: vec![MissionEvidenceCandidateMemberWire {
                obligation_id: flow.obligation_id,
                assignment_id: flow.assignment_id,
                assignment_revision: AgentAssignmentRevision::INITIAL.get(),
                target_agent_id: flow.assignee.agent_id,
                target_session_id: flow.assignee.session_id,
            }],
        }],
    };
    let prepared = flow
        .client
        .mission_evidence_manifest_prepare(
            flow.bearer,
            &heterarchy_orchestration_backend::wire::mission_evidence::MissionEvidenceManifestPrepareRequest {
                context: flow.mutation_context(),
                mission_id: flow.mission_id,
                plan_id: flow.plan_id,
                plan_revision: flow.plan_revision,
                partitions: request.partitions.clone(),
            },
        )
        .await
        .map_err(|error| format!("Mission evidence source preflight: {error:?}"))?;
    assert_eq!(
        prepared.status,
        heterarchy_orchestration_backend::wire::mission_evidence::MissionEvidencePreparationStatusWire::Ready
    );
    assert_eq!(prepared.proposals.len(), 1);
    let proposal = &prepared.proposals[0];
    assert_eq!(proposal.members.len(), 1);
    assert_eq!(proposal.members[0].obligation_id, flow.obligation_id);
    assert_eq!(proposal.members[0].assignment_id, flow.assignment_id);
    assert_eq!(proposal.members[0].target_agent_id, flow.assignee.agent_id);
    assert_eq!(
        proposal.members[0].target_session_id,
        flow.assignee.session_id
    );
    assert_eq!(
        proposal.source.backend_id,
        flow.source.client.expected_backend_id().into_uuid()
    );
    assert_eq!(
        proposal.source.delegation_destination_id,
        flow.source.client.destination_id()
    );
    assert_eq!(proposal.source.workspace_id, flow.target.scope.workspace_id);
    assert_eq!(proposal.source.activity_id, flow.target.activity_id);
    assert_eq!(proposal.plan.plan_id, flow.plan_id);
    assert_eq!(proposal.plan.revision, i64::try_from(flow.plan_revision)?);
    assert_eq!(proposal.deadline_ms, flow.t1_submission_cutoff_ms);
    Ok(PreparedManifest {
        request,
        manifest_id: proposal.manifest_id,
        digest: proposal.digest.clone(),
    })
}

async fn bind_manifest(
    flow: &MissionEvidenceFlow<'_>,
    prepared: &PreparedManifest,
) -> TestResult<
    heterarchy_orchestration_backend::wire::mission_evidence::MissionEvidenceManifestBindResponse,
> {
    let response =
        bind_with_exact_recovery(flow.client, flow.bearer, &prepared.request, flow.reader())
            .await
            .map_err(|error| format!("Mission evidence manifest bind/recovery: {error:?}"))?;
    assert_eq!(response.status, MissionEvidenceMutationStatusWire::Complete);
    assert!(!response.replayed);
    let binding = response
        .binding
        .as_ref()
        .ok_or("complete source bind omitted its native binding")?;
    assert_eq!(binding.plan_id, flow.plan_id);
    assert_eq!(binding.plan_revision, flow.plan_revision);
    assert_eq!(binding.manifests.len(), 1);
    assert_eq!(
        binding.manifests[0].proposal.manifest_id,
        prepared.manifest_id
    );
    assert_eq!(binding.manifests[0].proposal.digest, prepared.digest);
    assert_eq!(binding.manifests[0].members.len(), 1);
    assert!(binding.manifests[0].source_sequence > 0);

    let replay = flow
        .client
        .mission_evidence_manifest_bind(flow.bearer, &prepared.request)
        .await
        .map_err(|error| format!("Mission evidence bind replay: {error:?}"))?;
    assert!(replay.replayed);
    assert_eq!(replay.binding, response.binding);
    let readback = flow
        .client
        .mission_evidence_binding_read(
            flow.bearer,
            &heterarchy_orchestration_backend::wire::mission_evidence::MissionEvidenceBindingReadRequest {
                scope: flow.target.scope.clone(),
                session_id: flow.leader.session_id,
                mission_id: flow.mission_id,
                plan_id: flow.plan_id,
                plan_revision: flow.plan_revision,
            },
        )
        .await
        .map_err(|error| format!("Mission evidence binding readback: {error:?}"))?;
    assert_eq!(readback.binding, response.binding);
    Ok(response)
}

async fn observe_after_cutoff(flow: &MissionEvidenceFlow<'_>) -> TestResult<RecordedObservation> {
    wait_for_cutoff(flow.t1_submission_cutoff_ms).await?;
    let request = MissionEvidenceObservationRequest {
        context: flow.mutation_context(),
        request_id: Uuid::now_v7(),
        mission_id: flow.mission_id,
        plan_id: flow.plan_id,
        plan_revision: flow.plan_revision,
        expected_head_revision: 0,
    };
    let response = observe_with_exact_recovery(flow.client, flow.bearer, &request, flow.reader())
        .await
        .map_err(|error| format!("Mission evidence observation/recovery: {error:?}"))?;
    assert_eq!(response.status, MissionEvidenceMutationStatusWire::Complete);
    assert!(!response.replayed);
    let observation = response
        .observation
        .as_ref()
        .ok_or("complete observation omitted its exact source cuts")?;
    assert_eq!(observation.cuts.len(), 1);
    assert_eq!(
        observation.cuts[0].t1_cut.deadline_ms,
        flow.t1_submission_cutoff_ms
    );
    assert!(observation.cuts[0].t1_cut.closed_at_ms > flow.t1_submission_cutoff_ms);
    assert_eq!(
        observation.cuts[0].review_cut.t1_cut_id,
        observation.cuts[0].t1_cut.cut_id
    );
    assert!(matches!(
        observation.cuts[0].t1_cut.members[0].coverage,
        forge_backend::ops::evaluation_evidence::wire::EvaluationT1CoverageWire::NoEligibleSubmission
    ));
    assert!(matches!(
        observation.cuts[0].review_cut.members[0].review,
        forge_backend::ops::evaluation_evidence::wire::EvaluationReviewStateWire::NotApplicable
    ));
    assert_eq!(observation.measures.len(), 1);
    assert_eq!(observation.measures[0].planned_due_weight, 5);
    assert_eq!(observation.measures[0].accepted_original_due_weight, 0);
    assert_eq!(observation.measures[0].unresolved_due_weight, 5);
    assert_eq!(observation.measures[0].future_weight, 0);

    let replay = flow
        .client
        .mission_evidence_observation_record(flow.bearer, &request)
        .await
        .map_err(|error| format!("Mission evidence observation replay: {error:?}"))?;
    assert!(replay.replayed);
    assert_eq!(replay.observation, response.observation);
    let readback = flow
        .client
        .mission_evidence_observation_read(
            flow.bearer,
            &heterarchy_orchestration_backend::wire::mission_evidence::MissionEvidenceObservationReadRequest {
                scope: flow.target.scope.clone(),
                session_id: flow.leader.session_id,
                mission_id: flow.mission_id,
                plan_id: flow.plan_id,
                plan_revision: flow.plan_revision,
                request_id: request.request_id,
            },
        )
        .await
        .map_err(|error| format!("Mission evidence observation readback: {error:?}"))?;
    assert_eq!(readback.observation, response.observation);
    Ok(RecordedObservation { request, response })
}

/// Rebind the same Profile participant after all work/leadership assertions,
/// then recover the original immutable receipts by exact actor-bound read.
/// The original bootstrap leadership is intentionally not restored here.
pub(super) async fn assert_session_rebind_readback(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    source: &ForgeHost,
    target: &MemberTarget,
    principal: forge_domain::AuthenticatedPrincipal,
    original_actor: &ReadyFixtureMember,
    evidence: &MissionEvidenceRecoveryFixture,
) -> TestResult<ReadyFixtureMember> {
    detach_ready_member(client, bearer, source, target, principal, original_actor).await?;
    let replacement_case = ProfileReplacementCase {
        client,
        bearer,
        source,
        target,
        principal,
        original_actor,
    };
    let replacement = create_ready_profile_replacement(&replacement_case).await?;

    let exact_reader = MissionEvidenceRequestReadActor {
        sender_activity_id: target.activity_id,
        session_id: replacement.session_id,
    };
    assert_actor_readback_rejects_foreign_and_reads_exact_actor(
        client,
        bearer,
        evidence,
        target,
        exact_reader,
    )
    .await?;

    let closed = client
        .close_session(
            bearer,
            original_actor.session_id,
            &heterarchy_orchestration_backend::wire::AgentSessionCloseRequest {
                expected_revision: original_actor.revision,
            },
        )
        .await?;
    assert_eq!(closed.state, "closed");
    assert_eq!(replacement.participant_id, original_actor.participant_id);
    assert_eq!(replacement.profile_id, original_actor.profile_id);
    assert_ne!(replacement.session_id, original_actor.session_id);
    assert_ne!(replacement.agent_id, original_actor.agent_id);
    Ok(replacement)
}

async fn detach_ready_member(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    source: &ForgeHost,
    target: &MemberTarget,
    principal: forge_domain::AuthenticatedPrincipal,
    member: &ReadyFixtureMember,
) -> TestResult {
    let departure_target = exact_departure_target(source, target, principal, member).await?;
    let presence = client
        .presence(
            bearer,
            &PresenceQueryRequest {
                target: ParticipantTarget {
                    scope: target.scope.clone(),
                    activity_id: target.activity_id,
                    agent_id: member.agent_id,
                    session_id: member.session_id,
                },
            },
        )
        .await?;
    let had_presence = presence.presence.is_some();
    if let Some(observed) = &presence.presence {
        assert_eq!(observed.participant_id, member.participant_id);
        assert_eq!(observed.agent_id, member.agent_id);
        assert_eq!(observed.session_id, member.session_id);
    }
    let receipt = detach_with_exact_readback(client, bearer, departure_target.clone()).await?;
    assert_eq!(
        receipt.presence_cleared, had_presence,
        "detach must clear exactly the presence observed before the mutation"
    );
    assert_workspace_leadership_vacated(client, bearer, target).await
}

async fn exact_departure_target(
    source: &ForgeHost,
    target: &MemberTarget,
    principal: forge_domain::AuthenticatedPrincipal,
    member: &ReadyFixtureMember,
) -> TestResult<heterarchy_orchestration_backend::wire::departure::DepartureTarget> {
    let commands =
        super::ForgeCommandHttpClient::new(source.address, super::FORGE_SECRET, super::DEADLINE)?;
    let coordinator = super::ForgeSourceHttpClient::new(commands)
        .coordinator_actor(&source.scope, target.activity_id, principal)
        .await?
        .ok_or("Forge Coordinator Actor is missing before Session replacement")?;
    if coordinator.agent_id().into_uuid() == member.agent_id
        || coordinator.session_id().into_uuid() == member.session_id
    {
        return Err("cannot detach the current Forge Activity Coordinator".into());
    }

    let activity_id = forge_domain::ActivityId::from_uuid(target.activity_id);
    let agent_id = forge_domain::AgentInstanceId::from_uuid(member.agent_id);
    let source_scope = source.scope.clone();
    let observed_at = super::TimestampMillis::new(super::current_test_time()?)?;
    let focus_event_limit = super::AgentListLimit::new(4)?;
    let work_update_limit = super::AgentListLimit::new(4)?;
    let control = source.control_client.clone();
    let context = tokio::task::spawn_blocking(move || {
        control.agent_get_context(super::AgentContextRequest {
            scope: source_scope,
            activity_id,
            agent_id,
            now: observed_at,
            focus_event_limit,
            work_update_limit,
        })
    })
    .await??
    .ok_or("Forge Agent context is absent before Workspace detach")?;
    let source_agent = context.attachment().agent();
    assert_eq!(
        source_agent.session_id(),
        Some(forge_domain::AgentSessionId::from_uuid(member.session_id))
    );
    assert_eq!(source_agent.principal(), principal);
    if !source_agent.attachment_state().is_attached() {
        return Err("Forge Agent is already detached".into());
    }
    Ok(
        heterarchy_orchestration_backend::wire::departure::DepartureTarget {
            scope: target.scope.clone(),
            activity_id: target.activity_id,
            agent_id: member.agent_id,
            session_id: member.session_id,
            expected_agent_revision: source_agent.revision().get(),
        },
    )
}

async fn detach_with_exact_readback(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    departure_target: heterarchy_orchestration_backend::wire::departure::DepartureTarget,
) -> TestResult<heterarchy_orchestration_backend::wire::departure::DepartureReceiptWire> {
    let departure_request = heterarchy_orchestration_backend::wire::departure::DepartureRequest {
        target: departure_target.clone(),
    };
    let request_id = heterarchy_orchestration_runtime::departure::detach_request_id(
        departure_target.scope.workspace_id,
        departure_target.activity_id,
        departure_target.agent_id,
        departure_target.expected_agent_revision,
    );
    let receipt = match client.detach_member(bearer, &departure_request).await {
        Ok(response) => response.receipt,
        Err(
            error @ (ClientError::UnknownOutcome { .. } | ClientError::ReadbackRequired { .. }),
        ) => {
            let exact_readback = matches!(
                &error,
                ClientError::UnknownOutcome {
                    readback: ReadbackTarget::MemberDeparture {
                        workspace_id,
                        activity_id,
                        agent_id,
                        session_id,
                        request_id: actual_request_id,
                        expected_agent_revision: actual_agent_revision,
                    },
                }
                | ClientError::ReadbackRequired {
                    readback: ReadbackTarget::MemberDeparture {
                        workspace_id,
                        activity_id,
                        agent_id,
                        session_id,
                        request_id: actual_request_id,
                        expected_agent_revision: actual_agent_revision,
                    },
                } if *workspace_id == departure_target.scope.workspace_id
                    && *activity_id == departure_target.activity_id
                    && *agent_id == departure_target.agent_id
                    && *session_id == departure_target.session_id
                    && *actual_request_id == request_id
                    && *actual_agent_revision == departure_target.expected_agent_revision
            );
            if !exact_readback {
                return Err(
                    format!("Forge departure has a foreign readback target: {error:?}").into(),
                );
            }
            let readback = client
                .member_departure(
                    bearer,
                    &heterarchy_orchestration_backend::wire::departure::DepartureQueryRequest {
                        target: departure_target.clone(),
                        request_id,
                    },
                )
                .await?;
            readback
                .receipt
                .ok_or("exact Forge departure receipt is absent after unknown outcome")?
        }
        Err(error) => return Err(error.into()),
    };
    assert_eq!(receipt.request_id, request_id);
    assert_eq!(receipt.workspace_id, departure_target.scope.workspace_id);
    assert_eq!(receipt.activity_id, departure_target.activity_id);
    assert_eq!(receipt.agent_id, departure_target.agent_id);
    assert_eq!(receipt.session_id, departure_target.session_id);
    assert_eq!(
        receipt.kind,
        heterarchy_orchestration_backend::wire::departure::DepartureKindWire::Detach
    );
    let departure_readback = client
        .member_departure(
            bearer,
            &heterarchy_orchestration_backend::wire::departure::DepartureQueryRequest {
                target: departure_target,
                request_id,
            },
        )
        .await?;
    assert_eq!(departure_readback.receipt, Some(receipt.clone()));
    Ok(receipt)
}

async fn assert_workspace_leadership_vacated(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    target: &MemberTarget,
) -> TestResult {
    let leadership = client
        .leadership_view(
            bearer,
            &heterarchy_orchestration_backend::wire::LeadershipQueryRequest {
                scope: target.scope.clone(),
                session_id: Some(target.session_id),
                activity_id: None,
                team_id: None,
                review_after: None,
                limit: 10,
            },
        )
        .await?
        .leadership
        .ok_or("Workspace leadership readback is missing after detach")?;
    assert_eq!(leadership.leader_participant_id, None);
    assert_eq!(leadership.state, "failover_required");
    Ok(())
}

struct ProfileReplacementCase<'a> {
    client: &'a OrchestrationHttpClient,
    bearer: &'a HeaderValue,
    source: &'a ForgeHost,
    target: &'a MemberTarget,
    principal: forge_domain::AuthenticatedPrincipal,
    original_actor: &'a ReadyFixtureMember,
}

#[derive(Clone, Copy)]
struct OpenedReplacementSession {
    session_id: Uuid,
    revision: i64,
}

async fn create_ready_profile_replacement(
    case: &ProfileReplacementCase<'_>,
) -> TestResult<ReadyFixtureMember> {
    let profile_id = verify_reviewer_profile_for_replacement(case).await?;
    let session = open_verified_replacement_session(case).await?;
    bind_replacement_profile_session(case, profile_id, session).await?;
    register_ready_profile_replacement(case, profile_id, session).await
}

async fn verify_reviewer_profile_for_replacement(
    case: &ProfileReplacementCase<'_>,
) -> TestResult<Uuid> {
    let profile = case
        .client
        .profile_list(case.bearer, 64)
        .await?
        .into_iter()
        .find(|profile| profile.stable_name == "isolated-bootstrap-reviewer")
        .ok_or("existing reviewer Profile is missing for Session replacement")?;
    assert_eq!(profile.profile_id, case.original_actor.profile_id);
    let original_link = case
        .client
        .profile_session_binding(
            case.bearer,
            profile.profile_id,
            case.original_actor.session_id,
        )
        .await?;
    assert_eq!(original_link.profile_id, profile.profile_id);
    assert_eq!(original_link.session_id, case.original_actor.session_id);
    Ok(profile.profile_id)
}

async fn open_verified_replacement_session(
    case: &ProfileReplacementCase<'_>,
) -> TestResult<OpenedReplacementSession> {
    let opened = case
        .client
        .open_session(
            case.bearer,
            &super::AgentSessionOpenRequest {
                request_id: Uuid::now_v7().to_string(),
            },
        )
        .await?;
    assert!(opened.created);
    assert_eq!(opened.session.state, "open");

    let session_id = forge_domain::AgentSessionId::from_uuid(opened.session.session_id);
    let control = case.source.control_client.clone();
    let principal = case.principal;
    let verified = tokio::task::spawn_blocking(move || {
        control.agent_session_resolve(super::AgentSessionResolveRequest {
            session_id,
            principal,
        })
    })
    .await??;
    assert_eq!(verified.id().into_uuid(), opened.session.session_id);
    assert_eq!(verified.principal(), case.principal);
    Ok(OpenedReplacementSession {
        session_id: opened.session.session_id,
        revision: opened.session.revision,
    })
}

async fn bind_replacement_profile_session(
    case: &ProfileReplacementCase<'_>,
    profile_id: Uuid,
    session: OpenedReplacementSession,
) -> TestResult {
    let profile_link = case
        .client
        .bind_profile_session(
            case.bearer,
            profile_id,
            &super::ProfileSessionBindRequest {
                request_id: Uuid::now_v7().to_string(),
                session_id: session.session_id.to_string(),
            },
        )
        .await?;
    assert!(profile_link.created);
    assert_eq!(profile_link.profile_id, profile_id);
    assert_eq!(profile_link.session_id, session.session_id);
    let link_readback = case
        .client
        .profile_session_binding(case.bearer, profile_id, session.session_id)
        .await?;
    assert_eq!(link_readback.request_id, profile_link.request_id);
    assert!(!link_readback.created);
    Ok(())
}

async fn register_ready_profile_replacement(
    case: &ProfileReplacementCase<'_>,
    profile_id: Uuid,
    session: OpenedReplacementSession,
) -> TestResult<ReadyFixtureMember> {
    let replacement_target = MemberTarget {
        scope: case.target.scope.clone(),
        activity_id: case.target.activity_id,
        session_id: session.session_id,
    };
    let registration = case
        .client
        .register_member(
            case.bearer,
            &super::fixture_registration(&replacement_target),
        )
        .await?;
    assert_eq!(registration.target, replacement_target);
    assert_eq!(
        registration.progress.phase,
        super::MemberPhaseWire::Reserved
    );
    assert_eq!(
        registration.progress.participant_id,
        case.original_actor.participant_id
    );
    assert_ne!(registration.progress.agent_id, case.original_actor.agent_id);
    super::assert_confirmed_member(case.client, case.bearer, &registration).await?;

    let ready = case
        .client
        .member_registration(
            case.bearer,
            &super::MemberRegistrationQueryRequest {
                target: replacement_target.clone(),
                request_id: registration.progress.request_id,
            },
        )
        .await?;
    assert_eq!(ready.target, replacement_target);
    assert_eq!(
        ready.progress.as_ref().map(|progress| progress.phase),
        Some(super::MemberPhaseWire::Ready)
    );
    assert_eq!(
        ready
            .progress
            .as_ref()
            .map(|progress| progress.participant_id),
        Some(case.original_actor.participant_id)
    );
    let commit = ready
        .commit
        .as_ref()
        .ok_or("replacement commit missing on readback")?;
    assert_eq!(commit.request_id, registration.progress.request_id);
    assert_eq!(commit.participant_id, case.original_actor.participant_id);
    let activation = ready
        .activation
        .as_ref()
        .ok_or("replacement activation missing on readback")?;
    assert_eq!(activation.agent_id, registration.progress.agent_id);
    assert_eq!(
        activation.participant_id,
        case.original_actor.participant_id
    );

    Ok(ReadyFixtureMember {
        session_id: session.session_id,
        agent_id: registration.progress.agent_id,
        participant_id: registration.progress.participant_id,
        profile_id,
        revision: session.revision,
    })
}

async fn bind_with_exact_recovery(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    request: &MissionEvidenceManifestBindRequest,
    reader: MissionEvidenceRequestReadActor,
) -> TestResult<
    heterarchy_orchestration_backend::wire::mission_evidence::MissionEvidenceManifestBindResponse,
> {
    match client.mission_evidence_manifest_bind(bearer, request).await {
        Ok(response) => Ok(response),
        Err(
            error @ (ClientError::UnknownOutcome { .. } | ClientError::ReadbackRequired { .. }),
        ) => {
            let target_matches = matches!(
                &error,
                ClientError::UnknownOutcome {
                    readback: ReadbackTarget::MissionEvidenceMutation(original),
                }
                | ClientError::ReadbackRequired {
                    readback: ReadbackTarget::MissionEvidenceMutation(original),
                } if original.as_ref() == &MissionEvidenceMutationRequest::Bind(request.clone())
            );
            if !target_matches {
                return Err(format!(
                    "mission evidence bind had a foreign recovery intent: {error:?}"
                )
                .into());
            }
            let readback = client
                .mission_evidence_mutation_readback(
                    bearer,
                    &MissionEvidenceMutationRequest::Bind(request.clone()),
                    reader,
                )
                .await?;
            let Some(MissionEvidenceMutationWire::ManifestBinding(binding)) = readback.mutation
            else {
                return Err("exact bind receipt is absent after unknown outcome".into());
            };
            Ok(heterarchy_orchestration_backend::wire::mission_evidence::MissionEvidenceManifestBindResponse {
                scope: request.context.scope.clone(),
                request_id: request.request_id,
                status: MissionEvidenceMutationStatusWire::Complete,
                replayed: true,
                binding: Some(binding),
                pending_targets: Vec::new(),
                pending_sources: Vec::new(),
            })
        }
        Err(error) => Err(error.into()),
    }
}

async fn observe_with_exact_recovery(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    request: &MissionEvidenceObservationRequest,
    reader: MissionEvidenceRequestReadActor,
) -> TestResult<
    heterarchy_orchestration_backend::wire::mission_evidence::MissionEvidenceObservationResponse,
> {
    match client
        .mission_evidence_observation_record(bearer, request)
        .await
    {
        Ok(response) => Ok(response),
        Err(
            error @ (ClientError::UnknownOutcome { .. } | ClientError::ReadbackRequired { .. }),
        ) => {
            let intent = MissionEvidenceMutationRequest::Observe(request.clone());
            let target_matches = matches!(
                &error,
                ClientError::UnknownOutcome {
                    readback: ReadbackTarget::MissionEvidenceMutation(original),
                }
                | ClientError::ReadbackRequired {
                    readback: ReadbackTarget::MissionEvidenceMutation(original),
                } if original.as_ref() == &intent
            );
            if !target_matches {
                return Err(format!(
                    "mission evidence observation had a foreign recovery intent: {error:?}"
                )
                .into());
            }
            let readback = client
                .mission_evidence_mutation_readback(bearer, &intent, reader)
                .await?;
            let Some(MissionEvidenceMutationWire::Observation(observation)) = readback.mutation
            else {
                return Err("exact observation receipt is absent after unknown outcome".into());
            };
            Ok(heterarchy_orchestration_backend::wire::mission_evidence::MissionEvidenceObservationResponse {
                scope: request.context.scope.clone(),
                request_id: request.request_id,
                status: MissionEvidenceMutationStatusWire::Complete,
                replayed: true,
                observation: Some(observation),
                pending_targets: Vec::new(),
                pending_sources: Vec::new(),
            })
        }
        Err(error) => Err(error.into()),
    }
}

async fn assert_actor_readback_rejects_foreign_and_reads_exact_actor(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    evidence: &MissionEvidenceRecoveryFixture,
    original: &MemberTarget,
    exact_reader: MissionEvidenceRequestReadActor,
) -> TestResult {
    let wrong_reader = MissionEvidenceRequestReadActor {
        sender_activity_id: original.activity_id,
        session_id: original.session_id,
    };
    for mutation in [
        MissionEvidenceMutationRequest::Bind(evidence.binding_request.clone()),
        MissionEvidenceMutationRequest::Observe(evidence.observation_request.clone()),
    ] {
        let rejected = client
            .mission_evidence_mutation_readback(bearer, &mutation, wrong_reader)
            .await;
        match rejected {
            Err(ClientError::Rejected { status: 409, .. }) => {}
            Ok(response) => {
                return Err(
                    format!("foreign Ready actor read a mutation receipt: {response:?}").into(),
                );
            }
            Err(error) => {
                return Err(
                    format!("foreign actor readback failed unexpectedly: {error:?}").into(),
                );
            }
        }
    }

    let binding_readback = client
        .mission_evidence_mutation_readback(
            bearer,
            &MissionEvidenceMutationRequest::Bind(evidence.binding_request.clone()),
            exact_reader,
        )
        .await?;
    assert_eq!(
        binding_readback.mutation,
        evidence
            .binding_response
            .binding
            .clone()
            .map(MissionEvidenceMutationWire::ManifestBinding)
    );
    let observation_readback = client
        .mission_evidence_mutation_readback(
            bearer,
            &MissionEvidenceMutationRequest::Observe(evidence.observation_request.clone()),
            exact_reader,
        )
        .await?;
    assert_eq!(
        observation_readback.mutation,
        evidence
            .observation_response
            .observation
            .clone()
            .map(MissionEvidenceMutationWire::Observation)
    );
    Ok(())
}

async fn wait_for_cutoff(cutoff_ms: i64) -> TestResult {
    let remaining = cutoff_ms
        .saturating_sub(current_test_time()?)
        .saturating_add(2);
    if remaining <= 0 {
        return Ok(());
    }
    if remaining > 5_000 {
        return Err(format!("fixture T1 window is too long to wait: {remaining} ms").into());
    }
    let delay = Duration::from_millis(u64::try_from(remaining)?);
    tokio::time::timeout(Duration::from_secs(5), tokio::time::sleep(delay)).await?;
    Ok(())
}
