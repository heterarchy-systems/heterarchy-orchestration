//! HTTP consent and optional durable preparation after an exact v3 approval.

use super::*;

pub(super) async fn assert_http_flow(
    context: &ExistingHomeV3FlowContext<'_>,
    expansion: &expansion_wire::TeamExpansionRequestReceiptWire,
    v3_intent: &v3_wire::TeamExpansionDecisionV3Request,
    pending: &v3_wire::TeamExpansionDecisionV3ReceiptWire,
) -> TestResult {
    let home_before = read_team_snapshot(context, context.home_team_id).await?;
    let destination_before = read_team_snapshot(context, context.destination_team_id).await?;
    let population_before = read_workspace_population(context).await?;
    let coordinates = read_destination_coordinates(context, &destination_before).await?;
    let target = exact_home_member(context, &home_before)?;
    let destination_orchestrator =
        exact_profile_member(&destination_before, context.workspace_leader.profile_id)?;
    let approved = finalize_destination_anchor(
        context,
        expansion,
        v3_intent,
        pending,
        destination_orchestrator,
    )
    .await?;
    let proposal = transfer_wire::TeamPermanentTransferIntentRequest {
        actor: actor(context, context.workspace_leader),
        command: transfer_wire::TeamPermanentTransferIntentActionWire {
            transfer_id: Uuid::now_v7(),
            source_team_id: context.home_team_id,
            destination_team_id: context.destination_team_id,
            target_participation_id: target.participation_id,
            destination_request_id: expansion.request_id,
            destination_decision_id: approved.decision.decision_id,
            destination_ordinal: 0,
            expected_destination_team_revision: coordinates.team_revision,
            expected_destination_capacity_revision: coordinates.capacity_revision,
            expected_workspace_leadership_revision: context.workspace_leadership_revision,
            expected_source_membership_revision: target.revision,
            expected_source_leadership_revision: context.source_home_leadership_revision,
        },
    };
    let proposed_receipt = assert_proposal_readback_before_consents(context, &proposal).await?;
    let source = record_and_read_consent(
        context,
        proposal.command.transfer_id,
        context.source_home_leader,
        transfer_wire::TeamPermanentTransferConsentKindWire::SourceHomeLeader,
    )
    .await?;
    let accepted = record_and_read_consent(
        context,
        proposal.command.transfer_id,
        context.home_member,
        transfer_wire::TeamPermanentTransferConsentKindWire::TargetAgentAcceptance,
    )
    .await?;
    let after = snapshot(context, proposal.command.transfer_id).await?;
    assert_eq!(after.intent, proposed_receipt);
    assert_eq!(after.source_home_leader_consent, Some(source.clone()));
    assert_eq!(after.target_agent_acceptance, Some(accepted.clone()));
    if matches!(
        context.transfer_exercise,
        super::super::TransferExercise::Preparation
            | super::super::TransferExercise::UnattributedPreparation
            | super::super::TransferExercise::SourceDispatch
    ) {
        super::preparation::assert_preparation_after_consent(
            context,
            super::preparation::PreparationBaseline {
                proposal: &proposal,
                proposal_receipt: &proposed_receipt,
                source_consent: &source,
                target_acceptance: &accepted,
                v3_intent,
                v3_receipt: &approved,
                home: &home_before,
                destination: &destination_before,
                coordinates: &coordinates,
                population: &population_before,
            },
        )
        .await?;
    }
    assert_rosters_and_population_unchanged(
        context,
        &home_before,
        &destination_before,
        &coordinates,
        &population_before,
        "permanent transfer proposal and consent",
    )
    .await?;
    if context.transfer_exercise == super::super::TransferExercise::SlowRenewal {
        Box::pin(assert_same_leader_stabilization_renewal_http_flow(
            context,
            &proposal,
            &proposed_receipt,
            &source,
            &accepted,
            v3_intent,
            &approved,
        ))
        .await?;
    }
    Ok(())
}

async fn assert_proposal_readback_before_consents(
    context: &ExistingHomeV3FlowContext<'_>,
    proposal: &transfer_wire::TeamPermanentTransferIntentRequest,
) -> TestResult<transfer_wire::TeamPermanentTransferIntentReceiptWire> {
    let proposed = context
        .client
        .propose_team_permanent_transfer(context.bearer, proposal)
        .await?;
    assert_eq!(proposed.request, *proposal);
    assert!(!proposed.receipt.replayed);
    assert_eq!(
        proposed.receipt.target_profile_id,
        context.home_member.profile_id
    );
    assert_eq!(
        proposed.receipt.source_leader_profile_id,
        context.source_home_leader.profile_id
    );
    assert_expansion_actor(
        &proposed.receipt.workspace_actor,
        context,
        context.workspace_leader,
    );
    let replay = context
        .client
        .propose_team_permanent_transfer(context.bearer, proposal)
        .await?;
    let mut expected_replay = proposed.receipt.clone();
    expected_replay.replayed = true;
    assert_eq!(replay.receipt, expected_replay);
    let before = snapshot(context, proposal.command.transfer_id).await?;
    assert_eq!(before.intent, proposed.receipt);
    assert_eq!(before.source_home_leader_consent, None);
    assert_eq!(before.target_agent_acceptance, None);
    assert_wrong_consent_has_no_receipt(context, proposal.command.transfer_id).await?;
    Ok(proposed.receipt)
}

fn leadership_query(
    context: &ExistingHomeV3FlowContext<'_>,
    reader: &ReadyFixtureMember,
) -> LeadershipQueryRequest {
    LeadershipQueryRequest {
        scope: context.creator.scope.clone(),
        session_id: Some(reader.session_id),
        activity_id: None,
        team_id: Some(context.home_team_id),
        review_after: None,
        limit: 10,
    }
}

/// Opt-in proof for an intent-only renewal after the source Team's same Leader
/// advances from Stabilizing to Active. The fixture's one Direct Token remains
/// the principal for several Sessions; this does not prove independent humans.
async fn assert_same_leader_stabilization_renewal_http_flow(
    context: &ExistingHomeV3FlowContext<'_>,
    original_proposal: &transfer_wire::TeamPermanentTransferIntentRequest,
    original_intent: &transfer_wire::TeamPermanentTransferIntentReceiptWire,
    original_source_consent: &transfer_wire::TeamPermanentTransferConsentReceiptWire,
    original_target_acceptance: &transfer_wire::TeamPermanentTransferConsentReceiptWire,
    v3_intent: &v3_wire::TeamExpansionDecisionV3Request,
    approved_v3: &v3_wire::TeamExpansionDecisionV3ReceiptWire,
) -> TestResult {
    let home_before = read_team_snapshot(context, context.home_team_id).await?;
    let destination_before = read_team_snapshot(context, context.destination_team_id).await?;
    let population_before = read_workspace_population(context).await?;
    let destination_coordinates_before =
        read_destination_coordinates(context, &destination_before).await?;
    assert_eq!(
        super::read_v3_exact(context, v3_intent).await?,
        *approved_v3
    );

    let active =
        advance_source_leader_after_deadline(context, original_proposal, original_intent).await?;
    assert_current_workspace_orchestrator(context, original_proposal).await?;

    let renewal =
        renewal_request_after_leader_advance(original_proposal, active.leadership_revision);
    let committed = renew_with_reply_loss(context, &renewal).await?;
    assert_committed_renewal(
        &committed,
        original_proposal,
        original_intent,
        active.leadership_revision,
    );
    assert_historical_generation_records(
        context,
        original_proposal,
        original_intent,
        original_source_consent,
        original_target_acceptance,
        &renewal,
    )
    .await?;
    assert_child_generation_consents(context, &renewal, &committed).await?;

    assert_eq!(
        super::read_v3_exact(context, v3_intent).await?,
        *approved_v3
    );
    assert_rosters_and_population_unchanged(
        context,
        &home_before,
        &destination_before,
        &destination_coordinates_before,
        &population_before,
        "same-Leader stabilization-epoch transfer renewal and fresh consents",
    )
    .await?;
    eprintln!(
        "same-Leader renewal stage verified: epoch advance, reply-loss recovery, historical records, fresh consents and unchanged Home/hold; remaining bootstrap stages follow"
    );
    Ok(())
}

async fn advance_source_leader_after_deadline(
    context: &ExistingHomeV3FlowContext<'_>,
    original_proposal: &transfer_wire::TeamPermanentTransferIntentRequest,
    original_intent: &transfer_wire::TeamPermanentTransferIntentReceiptWire,
) -> TestResult<heterarchy_orchestration_backend::wire::leadership::LeadershipWire> {
    let query = leadership_query(context, context.source_home_leader);
    let stabilizing = context
        .client
        .leadership_view(context.bearer, &query)
        .await?
        .leadership
        .ok_or("source Team Leader disappeared before the renewal probe")?;
    assert_eq!(stabilizing.state, "stabilizing");
    assert_eq!(
        stabilizing.leadership_revision,
        original_proposal
            .command
            .expected_source_leadership_revision
    );
    assert_eq!(
        stabilizing.leader_participation_id,
        Some(original_intent.source_leader_participation_id)
    );
    assert_eq!(
        stabilizing.leader_profile_id,
        Some(original_intent.source_leader_profile_id)
    );
    assert_eq!(
        stabilizing.leader_participant_id,
        Some(context.source_home_leader.participant_id)
    );
    let deadline_ms = stabilizing
        .stabilization_until_ms
        .ok_or("stabilizing source Team omitted its observed deadline")?;
    wait_for_observed_deadline(deadline_ms).await?;
    assert_source_leader_still_stabilizing(context, &query, &stabilizing, deadline_ms).await?;
    advance_source_leadership(context, &query, &stabilizing, original_intent).await
}

async fn wait_for_observed_deadline(deadline_ms: i64) -> TestResult {
    let now_ms = super::super::current_test_time()?;
    let remaining_ms = deadline_ms.saturating_sub(now_ms);
    let wait_cap = std::time::Duration::from_mins(20);
    if remaining_ms > i64::try_from(wait_cap.as_millis())? {
        return Err(format!(
            "source Team stabilization deadline exceeds the 20-minute opt-in bound: deadline={deadline_ms} now={now_ms}"
        )
        .into());
    }
    if remaining_ms > 0 {
        eprintln!(
            "opt-in transfer renewal waiting {remaining_ms} ms for observed source Team stabilization deadline {deadline_ms}"
        );
        let wait = std::time::Duration::from_millis(u64::try_from(remaining_ms)?);
        tokio::time::timeout(wait_cap, tokio::time::sleep(wait))
            .await
            .map_err(|_| "source Team stabilization wait exceeded the 20-minute bound")?;
    } else {
        eprintln!(
            "opt-in transfer renewal observed elapsed source Team stabilization deadline {deadline_ms}"
        );
    }
    Ok(())
}

async fn assert_source_leader_still_stabilizing(
    context: &ExistingHomeV3FlowContext<'_>,
    query: &LeadershipQueryRequest,
    observed: &heterarchy_orchestration_backend::wire::leadership::LeadershipWire,
    deadline_ms: i64,
) -> TestResult {
    let current = context
        .client
        .leadership_view(context.bearer, query)
        .await?
        .leadership
        .ok_or("source Team Leader disappeared at its stabilization deadline")?;
    assert_eq!(current.state, "stabilizing");
    assert_eq!(current.leadership_revision, observed.leadership_revision);
    assert_eq!(current.stabilization_until_ms, Some(deadline_ms));
    Ok(())
}

async fn advance_source_leadership(
    context: &ExistingHomeV3FlowContext<'_>,
    query: &LeadershipQueryRequest,
    before: &heterarchy_orchestration_backend::wire::leadership::LeadershipWire,
    original_intent: &transfer_wire::TeamPermanentTransferIntentReceiptWire,
) -> TestResult<heterarchy_orchestration_backend::wire::leadership::LeadershipWire> {
    refresh_member_presence_over_http(context, context.source_home_leader).await?;
    refresh_member_presence_over_http(context, context.workspace_leader).await?;
    let heartbeat_readback = context
        .client
        .leadership_view(context.bearer, query)
        .await?
        .leadership
        .ok_or("source Team Leader disappeared after authenticated presence touches")?;
    assert_eq!(heartbeat_readback.state, "stabilizing");
    assert_eq!(
        heartbeat_readback.leadership_revision,
        before.leadership_revision
    );
    let response = context
        .client
        .advance_leadership(
            context.bearer,
            &LeadershipAdvanceRequest {
                target: LeadershipLifecycleTarget {
                    scope: context.creator.scope.clone(),
                    activity_id: None,
                    team_id: Some(context.home_team_id),
                    session_id: context.source_home_leader.session_id,
                },
            },
        )
        .await?;
    if !response.advanced {
        return Err("source Team did not advance after its observed deadline".into());
    }
    let advanced = response
        .leadership
        .ok_or("source Team leadership disappeared during deadline advance")?;
    let active = context
        .client
        .leadership_view(context.bearer, query)
        .await?
        .leadership
        .ok_or("source Team Leader readback is absent after deadline advance")?;
    assert_eq!(active.state, "active");
    assert_eq!(
        active.leadership_revision,
        before
            .leadership_revision
            .checked_add(1)
            .ok_or("source Team leadership revision overflow")?
    );
    assert_eq!(advanced.leadership_revision, active.leadership_revision);
    assert_eq!(
        active.leader_participation_id,
        Some(original_intent.source_leader_participation_id)
    );
    assert_eq!(
        active.leader_profile_id,
        Some(original_intent.source_leader_profile_id)
    );
    assert_eq!(
        active.leader_participant_id,
        Some(context.source_home_leader.participant_id)
    );
    Ok(active)
}

async fn refresh_member_presence_over_http(
    context: &ExistingHomeV3FlowContext<'_>,
    member: &ReadyFixtureMember,
) -> TestResult {
    let target = ParticipantTarget {
        scope: context.creator.scope.clone(),
        activity_id: context.creator.activity_id,
        agent_id: member.agent_id,
        session_id: member.session_id,
    };
    let query = PresenceQueryRequest {
        target: target.clone(),
    };
    let before = context
        .client
        .presence(context.bearer, &query)
        .await?
        .presence;
    // Operations defines missing live presence as its implicit initial
    // revision 1. A present row always supplies the exact CAS revision.
    let expected_revision = match before.as_ref() {
        Some(presence) => presence.presence_revision,
        None => 1,
    };
    if let Some(presence) = &before {
        assert_eq!(presence.participant_id, member.participant_id);
        assert!(presence.membership_revision > 0);
    }

    // The real touch route samples Backend's trusted clock. No request time or
    // database mutation is supplied by this fixture.
    let touched = context
        .client
        .touch_presence(
            context.bearer,
            &PresenceTouchRequest {
                target: target.clone(),
                expected_presence_revision: expected_revision,
            },
        )
        .await?;
    assert_eq!(touched.target, target);
    assert_eq!(
        touched.presence.workspace_id,
        context.creator.scope.workspace_id
    );
    assert_eq!(touched.presence.participant_id, member.participant_id);
    assert_eq!(touched.presence.agent_id, member.agent_id);
    assert_eq!(touched.presence.session_id, member.session_id);
    assert_eq!(
        touched.presence.presence_revision,
        expected_revision
            .checked_add(1)
            .ok_or("presence revision overflow")?
    );
    if let Some(presence) = before {
        assert_eq!(
            touched.presence.membership_revision,
            presence.membership_revision
        );
        assert!(touched.presence.last_authenticated_at_ms >= presence.last_authenticated_at_ms);
    }
    let readback = context.client.presence(context.bearer, &query).await?;
    assert_eq!(readback.target, target);
    assert_eq!(readback.presence, Some(touched.presence));
    Ok(())
}

async fn assert_current_workspace_orchestrator(
    context: &ExistingHomeV3FlowContext<'_>,
    proposal: &transfer_wire::TeamPermanentTransferIntentRequest,
) -> TestResult {
    let query = LeadershipQueryRequest {
        scope: context.creator.scope.clone(),
        session_id: Some(context.workspace_leader.session_id),
        activity_id: None,
        team_id: None,
        review_after: None,
        limit: 10,
    };
    let workspace = context
        .client
        .leadership_view(context.bearer, &query)
        .await?
        .leadership
        .ok_or("Workspace Orchestrator disappeared before transfer renewal")?;
    assert_eq!(
        workspace.leader_participant_id,
        Some(context.workspace_leader.participant_id)
    );
    assert_eq!(
        workspace.leader_profile_id,
        Some(context.workspace_leader.profile_id)
    );
    assert_eq!(
        workspace.leadership_revision,
        proposal.command.expected_workspace_leadership_revision
    );
    Ok(())
}

fn renewal_request_after_leader_advance(
    original: &transfer_wire::TeamPermanentTransferIntentRequest,
    new_source_leadership_revision: i64,
) -> transfer_wire::TeamPermanentTransferRenewalRequest {
    let mut proposal = original.clone();
    proposal.command.transfer_id = Uuid::now_v7();
    proposal.command.expected_source_leadership_revision = new_source_leadership_revision;
    transfer_wire::TeamPermanentTransferRenewalRequest {
        proposal,
        predecessor_transfer_id: original.command.transfer_id,
        expected_generation: 1,
    }
}

fn assert_committed_renewal(
    committed: &transfer_wire::TeamPermanentTransferRenewalResponse,
    original_proposal: &transfer_wire::TeamPermanentTransferIntentRequest,
    original_intent: &transfer_wire::TeamPermanentTransferIntentReceiptWire,
    active_source_revision: i64,
) {
    let child_id = committed.receipt.intent.transfer_id;
    let mut expected_proposal = original_proposal.clone();
    expected_proposal.command.transfer_id = child_id;
    expected_proposal
        .command
        .expected_source_leadership_revision = active_source_revision;
    assert_eq!(committed.request.proposal, expected_proposal);
    assert_eq!(
        committed.request.predecessor_transfer_id,
        original_proposal.command.transfer_id
    );
    assert_eq!(committed.request.expected_generation, 1);
    assert!(!committed.receipt.intent.replayed);
    assert_eq!(committed.receipt.intent.transfer_id, child_id);
    assert_eq!(
        committed.receipt.intent.workspace_id,
        original_intent.workspace_id
    );
    assert_eq!(
        committed.receipt.intent.source_team_id,
        original_intent.source_team_id
    );
    assert_eq!(
        committed.receipt.intent.target_participation_id,
        original_intent.target_participation_id
    );
    assert_eq!(
        committed.receipt.intent.target_profile_id,
        original_intent.target_profile_id
    );
    assert_eq!(
        committed.receipt.intent.source_membership_revision,
        original_intent.source_membership_revision
    );
    assert_eq!(
        committed.receipt.intent.source_leadership_revision,
        active_source_revision
    );
    assert!(active_source_revision > original_intent.source_leadership_revision);
    assert_eq!(
        committed.receipt.intent.source_leader_participation_id,
        original_intent.source_leader_participation_id
    );
    assert_eq!(
        committed.receipt.intent.source_leader_profile_id,
        original_intent.source_leader_profile_id
    );
    assert_eq!(committed.receipt.generation.number, 2);
    assert_eq!(
        committed.receipt.generation.predecessor_transfer_id,
        Some(original_proposal.command.transfer_id)
    );
    assert_eq!(
        committed.receipt.generation.state,
        transfer_wire::TeamPermanentTransferGenerationStateWire::IntentOnly
    );
    assert_eq!(committed.receipt.generation.head_transfer_id, child_id);
    assert_eq!(committed.receipt.generation.head_generation, 2);
}

async fn assert_old_fresh_consent_is_rejected(
    context: &ExistingHomeV3FlowContext<'_>,
    transfer_id: Uuid,
) -> TestResult {
    let intent = transfer_wire::TeamPermanentTransferConsentRequest {
        actor: actor(context, context.source_home_leader),
        command: transfer_wire::TeamPermanentTransferConsentActionWire {
            receipt_id: Uuid::now_v7(),
            transfer_id,
            kind: transfer_wire::TeamPermanentTransferConsentKindWire::SourceHomeLeader,
        },
    };
    expect_conflict(
        &context
            .client
            .record_team_permanent_transfer_consent(context.bearer, &intent)
            .await,
        "fresh consent against a superseded permanent transfer generation",
    )?;
    let readback = transfer_wire::TeamPermanentTransferConsentReadbackRequest {
        intent,
        current_actor: actor(context, context.workspace_leader),
    };
    let result = context
        .client
        .read_team_permanent_transfer_consent(context.bearer, &readback)
        .await?;
    assert_eq!(result.request, readback);
    assert_eq!(result.result, None);
    Ok(())
}

async fn assert_historical_generation_records(
    context: &ExistingHomeV3FlowContext<'_>,
    original_proposal: &transfer_wire::TeamPermanentTransferIntentRequest,
    original_intent: &transfer_wire::TeamPermanentTransferIntentReceiptWire,
    original_source_consent: &transfer_wire::TeamPermanentTransferConsentReceiptWire,
    original_target_acceptance: &transfer_wire::TeamPermanentTransferConsentReceiptWire,
    renewal: &transfer_wire::TeamPermanentTransferRenewalRequest,
) -> TestResult {
    let old_id = original_proposal.command.transfer_id;
    let old_generation = generation_snapshot(context, old_id).await?;
    assert_eq!(old_generation.intent, *original_intent);
    assert_eq!(old_generation.generation.number, 1);
    assert_eq!(old_generation.generation.predecessor_transfer_id, None);
    assert_eq!(
        old_generation.generation.state,
        transfer_wire::TeamPermanentTransferGenerationStateWire::Superseded
    );
    assert_eq!(
        old_generation.generation.head_transfer_id,
        renewal.proposal.command.transfer_id
    );
    assert_eq!(old_generation.generation.head_generation, 2);

    let intent_replay = context
        .client
        .propose_team_permanent_transfer(context.bearer, original_proposal)
        .await?;
    let mut expected_intent = original_intent.clone();
    expected_intent.replayed = true;
    assert_eq!(intent_replay.receipt, expected_intent);

    assert_old_fresh_consent_is_rejected(context, old_id).await?;
    let snapshot = snapshot(context, old_id).await?;
    assert_eq!(snapshot.intent, *original_intent);
    assert_eq!(
        snapshot.source_home_leader_consent,
        Some(original_source_consent.clone())
    );
    assert_eq!(
        snapshot.target_agent_acceptance,
        Some(original_target_acceptance.clone())
    );
    assert_historical_consent_replay(
        context,
        old_id,
        original_source_consent,
        context.source_home_leader,
        transfer_wire::TeamPermanentTransferConsentKindWire::SourceHomeLeader,
    )
    .await?;
    assert_historical_consent_replay(
        context,
        old_id,
        original_target_acceptance,
        context.home_member,
        transfer_wire::TeamPermanentTransferConsentKindWire::TargetAgentAcceptance,
    )
    .await
}

async fn assert_historical_consent_replay(
    context: &ExistingHomeV3FlowContext<'_>,
    transfer_id: Uuid,
    original: &transfer_wire::TeamPermanentTransferConsentReceiptWire,
    actor_member: &ReadyFixtureMember,
    kind: transfer_wire::TeamPermanentTransferConsentKindWire,
) -> TestResult {
    let request = transfer_wire::TeamPermanentTransferConsentRequest {
        actor: actor(context, actor_member),
        command: transfer_wire::TeamPermanentTransferConsentActionWire {
            receipt_id: original.receipt_id,
            transfer_id,
            kind,
        },
    };
    let replay = context
        .client
        .record_team_permanent_transfer_consent(context.bearer, &request)
        .await?;
    let mut expected = original.clone();
    expected.replayed = true;
    assert_eq!(replay.request, request);
    assert_eq!(replay.receipt, expected);
    Ok(())
}

async fn assert_child_generation_consents(
    context: &ExistingHomeV3FlowContext<'_>,
    renewal: &transfer_wire::TeamPermanentTransferRenewalRequest,
    committed: &transfer_wire::TeamPermanentTransferRenewalResponse,
) -> TestResult {
    let child_id = renewal.proposal.command.transfer_id;
    let source_consent = record_and_read_consent(
        context,
        child_id,
        context.source_home_leader,
        transfer_wire::TeamPermanentTransferConsentKindWire::SourceHomeLeader,
    )
    .await?;
    let target_acceptance = record_and_read_consent(
        context,
        child_id,
        context.home_member,
        transfer_wire::TeamPermanentTransferConsentKindWire::TargetAgentAcceptance,
    )
    .await?;
    assert_eq!(
        source_consent.authority_revision,
        committed.receipt.intent.source_leadership_revision
    );
    assert_eq!(
        source_consent.membership_revision,
        renewal.proposal.command.expected_source_membership_revision
    );
    let current = snapshot(context, child_id).await?;
    assert_eq!(current.intent, committed.receipt.intent);
    assert_eq!(current.source_home_leader_consent, Some(source_consent));
    assert_eq!(current.target_agent_acceptance, Some(target_acceptance));
    assert_eq!(
        generation_snapshot(context, child_id).await?,
        committed.receipt
    );
    Ok(())
}

async fn generation_snapshot(
    context: &ExistingHomeV3FlowContext<'_>,
    transfer_id: Uuid,
) -> TestResult<transfer_wire::TeamPermanentTransferGenerationSnapshotWire> {
    let request = transfer_wire::TeamPermanentTransferReadbackRequest {
        workspace_id: context.creator.scope.workspace_id,
        transfer_id,
        current_actor: actor(context, context.workspace_leader),
    };
    let response = context
        .client
        .read_team_permanent_transfer_generation(context.bearer, &request)
        .await?;
    assert_eq!(response.request, request);
    response
        .result
        .ok_or_else(|| "permanent transfer generation readback absent".into())
}

async fn renew_with_reply_loss(
    context: &ExistingHomeV3FlowContext<'_>,
    request: &transfer_wire::TeamPermanentTransferRenewalRequest,
) -> TestResult<transfer_wire::TeamPermanentTransferRenewalResponse> {
    use heterarchy_orchestration_backend::client::ReadbackTarget;
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let proxy_client =
        OrchestrationHttpClient::new(listener.local_addr()?, super::super::DEADLINE)?;
    let expected = request.clone();
    let bearer = context.bearer.clone();
    let backend = context.backend_address;
    let proxy = tokio::spawn(async move {
        tokio::time::timeout(
            super::super::DEADLINE,
            forward_and_drop_renewal(listener, backend, bearer, expected),
        )
        .await
        .map_err(|_| "renewal response-drop proxy timed out".to_owned())?
    });
    let result = proxy_client
        .renew_team_permanent_transfer(context.bearer, request)
        .await;
    let committed = proxy.await??;
    if !matches!(result, Err(ClientError::UnknownOutcome {
        readback: ReadbackTarget::TeamPermanentTransferRenewal(ref original)
    }) if original.as_ref() == request)
    {
        return Err(
            format!("renewal reply loss did not retain the exact request: {result:?}").into(),
        );
    }
    if committed.request != *request || committed.receipt.intent.replayed {
        return Err("upstream did not prove a fresh exact transfer renewal".into());
    }
    let recovered = context
        .client
        .recover_team_permanent_transfer_renewal(
            context.bearer,
            request,
            &actor(context, context.workspace_leader),
        )
        .await?
        .ok_or("exact child-generation readback did not resolve the lost renewal reply")?;
    assert_eq!(recovered, committed.receipt);
    Ok(committed)
}

async fn forward_and_drop_renewal(
    listener: tokio::net::TcpListener,
    backend: std::net::SocketAddr,
    bearer: HeaderValue,
    expected: transfer_wire::TeamPermanentTransferRenewalRequest,
) -> Result<transfer_wire::TeamPermanentTransferRenewalResponse, String> {
    use super::super::temporary_dispatch_lifecycle::{
        http_request_parts, read_http_request, request_authorization,
    };
    let (mut downstream, _) = listener.accept().await.map_err(|error| error.to_string())?;
    let bytes = read_http_request(&mut downstream).await?;
    let (method, path, body) = http_request_parts(&bytes)?;
    let expected_path = format!(
        "/internal/v1/workspaces/{}/teams/permanent-transfers/renew",
        expected.proposal.actor.scope.workspace_id
    );
    if method != "POST"
        || path != expected_path
        || request_authorization(&bytes)? != bearer.to_str().map_err(|error| error.to_string())?
    {
        return Err("renewal response-drop proxy request scope mismatch".to_owned());
    }
    let parsed: transfer_wire::TeamPermanentTransferRenewalRequest =
        serde_json::from_slice(body).map_err(|error| error.to_string())?;
    if parsed != expected {
        return Err("renewal response-drop proxy request changed".to_owned());
    }
    let upstream = OrchestrationHttpClient::new(backend, super::super::DEADLINE)
        .map_err(|error| error.to_string())?;
    let response = upstream
        .renew_team_permanent_transfer(&bearer, &parsed)
        .await
        .map_err(|error| format!("upstream renewal before reply loss: {error:?}"))?;
    if response.request != expected
        || response.receipt.intent.replayed
        || response.receipt.generation.number != expected.expected_generation + 1
    {
        return Err("upstream did not confirm a fresh exact renewal generation".to_owned());
    }
    // The owner committed and returned the exact result; only the client reply
    // is dropped, so recovery must use durable readback without another write.
    drop(downstream);
    Ok(response)
}

pub(super) async fn snapshot(
    context: &ExistingHomeV3FlowContext<'_>,
    transfer_id: Uuid,
) -> TestResult<transfer_wire::TeamPermanentTransferSnapshotWire> {
    let request = transfer_wire::TeamPermanentTransferReadbackRequest {
        workspace_id: context.creator.scope.workspace_id,
        transfer_id,
        current_actor: actor(context, context.workspace_leader),
    };
    let response = context
        .client
        .read_team_permanent_transfer_snapshot(context.bearer, &request)
        .await?;
    assert_eq!(response.request, request);
    response
        .result
        .ok_or_else(|| "permanent transfer snapshot absent".into())
}

async fn record_and_read_consent(
    context: &ExistingHomeV3FlowContext<'_>,
    transfer_id: Uuid,
    member: &ReadyFixtureMember,
    kind: transfer_wire::TeamPermanentTransferConsentKindWire,
) -> TestResult<transfer_wire::TeamPermanentTransferConsentReceiptWire> {
    let request = transfer_wire::TeamPermanentTransferConsentRequest {
        actor: actor(context, member),
        command: transfer_wire::TeamPermanentTransferConsentActionWire {
            receipt_id: Uuid::now_v7(),
            transfer_id,
            kind,
        },
    };
    let result =
        if kind == transfer_wire::TeamPermanentTransferConsentKindWire::TargetAgentAcceptance {
            record_with_reply_loss(context, &request).await?
        } else {
            context
                .client
                .record_team_permanent_transfer_consent(context.bearer, &request)
                .await?
        };
    assert_eq!(result.request, request);
    assert!(!result.receipt.replayed);
    assert_expansion_actor(&result.receipt.actor, context, member);
    for reader in [member, context.workspace_leader] {
        let query = transfer_wire::TeamPermanentTransferConsentReadbackRequest {
            intent: request.clone(),
            current_actor: actor(context, reader),
        };
        let read = context
            .client
            .read_team_permanent_transfer_consent(context.bearer, &query)
            .await?;
        assert_eq!(read.request, query);
        assert_eq!(read.result, Some(result.receipt.clone()));
    }
    let replay = context
        .client
        .record_team_permanent_transfer_consent(context.bearer, &request)
        .await?;
    let mut expected = result.receipt.clone();
    expected.replayed = true;
    assert_eq!(replay.receipt, expected);
    Ok(result.receipt)
}

async fn record_with_reply_loss(
    context: &ExistingHomeV3FlowContext<'_>,
    request: &transfer_wire::TeamPermanentTransferConsentRequest,
) -> TestResult<transfer_wire::TeamPermanentTransferConsentResponse> {
    use heterarchy_orchestration_backend::client::ReadbackTarget;
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let proxy_client =
        OrchestrationHttpClient::new(listener.local_addr()?, super::super::DEADLINE)?;
    let expected = request.clone();
    let bearer = context.bearer.clone();
    let backend = context.backend_address;
    let proxy = tokio::spawn(async move {
        tokio::time::timeout(
            super::super::DEADLINE,
            forward_and_drop_consent(listener, backend, bearer, expected),
        )
        .await
        .map_err(|_| "consent response-drop proxy timed out".to_owned())?
    });
    let result = proxy_client
        .record_team_permanent_transfer_consent(context.bearer, request)
        .await;
    let upstream = proxy.await??;
    if !matches!(result, Err(ClientError::UnknownOutcome {
        readback: ReadbackTarget::TeamPermanentTransferConsent(ref original)
    }) if original.as_ref() == request)
    {
        return Err(
            format!("consent reply loss did not retain the exact request: {result:?}").into(),
        );
    }
    let query = transfer_wire::TeamPermanentTransferConsentReadbackRequest {
        intent: request.clone(),
        current_actor: actor(context, context.workspace_leader),
    };
    let read = context
        .client
        .read_team_permanent_transfer_consent(context.bearer, &query)
        .await?;
    assert_eq!(read.request, query);
    assert_eq!(read.result, Some(upstream.receipt.clone()));
    Ok(upstream)
}

async fn forward_and_drop_consent(
    listener: tokio::net::TcpListener,
    backend: std::net::SocketAddr,
    bearer: HeaderValue,
    expected: transfer_wire::TeamPermanentTransferConsentRequest,
) -> Result<transfer_wire::TeamPermanentTransferConsentResponse, String> {
    use super::super::temporary_dispatch_lifecycle::{
        http_request_parts, read_http_request, request_authorization,
    };
    let (mut downstream, _) = listener.accept().await.map_err(|error| error.to_string())?;
    let bytes = read_http_request(&mut downstream).await?;
    let (method, path, body) = http_request_parts(&bytes)?;
    let expected_path = format!(
        "/internal/v1/workspaces/{}/teams/permanent-transfers/consent",
        expected.actor.scope.workspace_id
    );
    if method != "POST"
        || path != expected_path
        || request_authorization(&bytes)? != bearer.to_str().map_err(|error| error.to_string())?
    {
        return Err("consent response-drop proxy request scope mismatch".to_owned());
    }
    let parsed: transfer_wire::TeamPermanentTransferConsentRequest =
        serde_json::from_slice(body).map_err(|error| error.to_string())?;
    if parsed != expected {
        return Err("consent response-drop proxy request changed".to_owned());
    }
    let upstream = OrchestrationHttpClient::new(backend, super::super::DEADLINE)
        .map_err(|error| error.to_string())?;
    let response = upstream
        .record_team_permanent_transfer_consent(&bearer, &parsed)
        .await
        .map_err(|error| format!("upstream consent before reply loss: {error:?}"))?;
    if response.request != expected || response.receipt.replayed {
        return Err("upstream did not prove a fresh exact consent".to_owned());
    }
    // The service has already confirmed the write. Only its downstream reply
    // is lost; this is not a database crash or an uncertain source commit.
    drop(downstream);
    Ok(response)
}

async fn assert_wrong_consent_has_no_receipt(
    context: &ExistingHomeV3FlowContext<'_>,
    transfer_id: Uuid,
) -> TestResult {
    let intent = transfer_wire::TeamPermanentTransferConsentRequest {
        actor: actor(context, context.destination_leader),
        command: transfer_wire::TeamPermanentTransferConsentActionWire {
            receipt_id: Uuid::now_v7(),
            transfer_id,
            kind: transfer_wire::TeamPermanentTransferConsentKindWire::SourceHomeLeader,
        },
    };
    let denied = context
        .client
        .record_team_permanent_transfer_consent(context.bearer, &intent)
        .await;
    expect_conflict(&denied, "destination Leader acting as source Leader")?;
    let request = transfer_wire::TeamPermanentTransferConsentReadbackRequest {
        intent,
        current_actor: actor(context, context.workspace_leader),
    };
    let read = context
        .client
        .read_team_permanent_transfer_consent(context.bearer, &request)
        .await?;
    assert_eq!(read.request, request);
    assert_eq!(read.result, None);
    Ok(())
}
