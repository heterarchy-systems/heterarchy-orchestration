//! Isolated real HTTP proof of Home transfer preparation and exact recovery.

use super::*;

pub(super) struct PreparationBaseline<'a> {
    pub(super) proposal: &'a transfer_wire::TeamPermanentTransferIntentRequest,
    pub(super) proposal_receipt: &'a transfer_wire::TeamPermanentTransferIntentReceiptWire,
    pub(super) source_consent: &'a transfer_wire::TeamPermanentTransferConsentReceiptWire,
    pub(super) target_acceptance: &'a transfer_wire::TeamPermanentTransferConsentReceiptWire,
    pub(super) v3_intent: &'a v3_wire::TeamExpansionDecisionV3Request,
    pub(super) v3_receipt: &'a v3_wire::TeamExpansionDecisionV3ReceiptWire,
    pub(super) home: &'a TeamSnapshot,
    pub(super) destination: &'a TeamSnapshot,
    pub(super) coordinates: &'a ExpansionCoordinates,
    pub(super) population: &'a WorkspacePopulation,
}

pub(super) async fn assert_preparation_after_consent(
    context: &ExistingHomeV3FlowContext<'_>,
    baseline: PreparationBaseline<'_>,
) -> TestResult {
    let request = preparation_request(
        context,
        baseline.proposal,
        baseline.source_consent,
        baseline.target_acceptance,
    );
    let wrong_actor = transfer_wire::TeamPermanentTransferPreparationRequest {
        actor: actor(context, context.source_home_leader),
        command: request.command,
    };
    expect_conflict(
        &context
            .client
            .prepare_team_permanent_transfer(context.bearer, &wrong_actor)
            .await,
        "non-Orchestrator attempted HomeTransfer preparation",
    )?;
    assert_preparation_absent(context, &request).await?;

    let mut wrong_consent = request.clone();
    wrong_consent.command.source_leader_consent_id = baseline.target_acceptance.receipt_id;
    wrong_consent.command.target_acceptance_id = baseline.source_consent.receipt_id;
    expect_conflict(
        &context
            .client
            .prepare_team_permanent_transfer(context.bearer, &wrong_consent)
            .await,
        "swapped HomeTransfer consents were accepted",
    )?;
    assert_preparation_absent(context, &request).await?;

    if context.transfer_exercise == super::super::TransferExercise::UnattributedPreparation {
        return assert_unattributed_preparation_held(context, &request, &baseline).await;
    }
    let committed = prepare_with_reply_loss(context, &request).await?;
    assert_prepared_identity(context, &request, &committed, &baseline);

    let recovered = context
        .client
        .recover_team_permanent_transfer_preparation(
            context.bearer,
            &request,
            &actor(context, context.workspace_leader),
        )
        .await?
        .ok_or("exact original HomeTransfer preparation readback was absent")?;
    assert_eq!(
        recovered, committed,
        "readback differed from the actual upstream commit"
    );
    assert_eq!(
        context
            .client
            .read_team_permanent_transfer_generation(
                context.bearer,
                &transfer_wire::TeamPermanentTransferReadbackRequest {
                    workspace_id: request.actor.scope.workspace_id,
                    transfer_id: request.command.transfer_id,
                    current_actor: actor(context, context.workspace_leader),
                },
            )
            .await?
            .result
            .ok_or("prepared transfer generation disappeared")?
            .generation
            .state,
        transfer_wire::TeamPermanentTransferGenerationStateWire::Preparing,
    );
    assert_eq!(
        super::permanent_transfer::snapshot(context, request.command.transfer_id).await?,
        transfer_wire::TeamPermanentTransferSnapshotWire {
            intent: baseline.proposal_receipt.clone(),
            source_home_leader_consent: Some(baseline.source_consent.clone()),
            target_agent_acceptance: Some(baseline.target_acceptance.clone()),
        },
    );
    let approved = super::read_v3_exact(context, baseline.v3_intent).await?;
    assert_eq!(approved, *baseline.v3_receipt);
    assert_rosters_and_population_unchanged(
        context,
        baseline.home,
        baseline.destination,
        baseline.coordinates,
        baseline.population,
        "HomeTransfer preparation and reply-loss readback",
    )
    .await?;
    if context.transfer_exercise == super::super::TransferExercise::SourceDispatch {
        super::home_source::assert_source_dispatch_after_preparation(
            context, &request, &committed, &baseline,
        )
        .await?;
    }
    Ok(())
}

async fn assert_unattributed_preparation_held(
    context: &ExistingHomeV3FlowContext<'_>,
    request: &transfer_wire::TeamPermanentTransferPreparationRequest,
    baseline: &PreparationBaseline<'_>,
) -> TestResult {
    expect_conflict(
        &context
            .client
            .prepare_team_permanent_transfer(context.bearer, request)
            .await,
        "unattributed Workspace binding must keep preparation pending",
    )?;
    assert_preparation_absent(context, request).await?;
    let generation = context
        .client
        .read_team_permanent_transfer_generation(
            context.bearer,
            &transfer_wire::TeamPermanentTransferReadbackRequest {
                workspace_id: request.actor.scope.workspace_id,
                transfer_id: request.command.transfer_id,
                current_actor: actor(context, context.workspace_leader),
            },
        )
        .await?
        .result
        .ok_or("unprepared transfer generation missing")?;
    assert_eq!(
        generation.generation.state,
        transfer_wire::TeamPermanentTransferGenerationStateWire::IntentOnly
    );
    assert_eq!(
        super::read_v3_exact(context, baseline.v3_intent).await?,
        *baseline.v3_receipt
    );
    assert_rosters_and_population_unchanged(
        context,
        baseline.home,
        baseline.destination,
        baseline.coordinates,
        baseline.population,
        "unattributed preparation hold",
    )
    .await
}

fn assert_prepared_identity(
    context: &ExistingHomeV3FlowContext<'_>,
    request: &transfer_wire::TeamPermanentTransferPreparationRequest,
    committed: &transfer_wire::TeamPermanentTransferPreparationReceiptWire,
    baseline: &PreparationBaseline<'_>,
) {
    assert_eq!(committed.transfer_id, baseline.proposal.command.transfer_id);
    assert_eq!(
        committed.source_team_id,
        baseline.proposal.command.source_team_id
    );
    assert_eq!(
        committed.destination_team_id,
        baseline.proposal.command.destination_team_id
    );
    assert_eq!(committed.generation, request.command.expected_generation);
    assert_eq!(
        committed.workspace_leadership_revision,
        context.workspace_leadership_revision
    );
    assert_eq!(
        committed.source_leader_consent_id,
        baseline.source_consent.receipt_id
    );
    assert_eq!(
        committed.target_acceptance_id,
        baseline.target_acceptance.receipt_id
    );
    assert_eq!(committed.target_profile_id, context.home_member.profile_id);
    assert_eq!(
        committed.target_participation_id,
        baseline.proposal.command.target_participation_id
    );
    assert_eq!(
        committed.sessions.len(),
        usize::from(committed.session_count)
    );
    assert_eq!(
        committed.bindings.len(),
        usize::from(committed.binding_count)
    );
    assert!(committed.sessions.contains(&context.home_member.session_id));
    assert!(
        committed
            .target_participant_ids
            .contains(&context.home_member.participant_id)
    );
    assert!(committed.bindings.iter().any(|binding| {
        binding.participant_id == context.home_member.participant_id
            && binding.session_id == context.home_member.session_id
    }));
    assert_expansion_actor(&committed.actor, context, context.workspace_leader);
}

async fn assert_preparation_absent(
    context: &ExistingHomeV3FlowContext<'_>,
    original: &transfer_wire::TeamPermanentTransferPreparationRequest,
) -> TestResult {
    let query = transfer_wire::TeamPermanentTransferPreparationReadbackRequest {
        workspace_id: original.actor.scope.workspace_id,
        preparation_id: original.command.preparation_id,
        current_actor: actor(context, context.workspace_leader),
    };
    let response = context
        .client
        .read_team_permanent_transfer_preparation(context.bearer, &query)
        .await?;
    assert_eq!(response.request, query);
    assert!(
        response.result.is_none(),
        "rejected preparation left a barrier receipt"
    );
    Ok(())
}

fn preparation_request(
    context: &ExistingHomeV3FlowContext<'_>,
    proposal: &transfer_wire::TeamPermanentTransferIntentRequest,
    source: &transfer_wire::TeamPermanentTransferConsentReceiptWire,
    target: &transfer_wire::TeamPermanentTransferConsentReceiptWire,
) -> transfer_wire::TeamPermanentTransferPreparationRequest {
    transfer_wire::TeamPermanentTransferPreparationRequest {
        actor: actor(context, context.workspace_leader),
        command: transfer_wire::TeamPermanentTransferPreparationActionWire {
            preparation_id: Uuid::now_v7(),
            transfer_id: proposal.command.transfer_id,
            expected_generation: 1,
            expected_workspace_leadership_revision: context.workspace_leadership_revision,
            source_leader_consent_id: source.receipt_id,
            target_acceptance_id: target.receipt_id,
        },
    }
}

async fn prepare_with_reply_loss(
    context: &ExistingHomeV3FlowContext<'_>,
    request: &transfer_wire::TeamPermanentTransferPreparationRequest,
) -> TestResult<transfer_wire::TeamPermanentTransferPreparationReceiptWire> {
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
            forward_and_drop_preparation(listener, backend, bearer, expected),
        )
        .await
        .map_err(|_| "preparation response-drop proxy timed out".to_owned())?
    });
    let result = proxy_client
        .prepare_team_permanent_transfer(context.bearer, request)
        .await;
    let committed = proxy.await??;
    if !matches!(result, Err(ClientError::UnknownOutcome {
        readback: ReadbackTarget::TeamPermanentTransferPreparation(ref original)
    }) if original.as_ref() == request)
    {
        return Err(
            format!("preparation reply loss did not retain the exact request: {result:?}").into(),
        );
    }
    if committed.preparation_id != request.command.preparation_id
        || committed.source_leader_consent_id != request.command.source_leader_consent_id
        || committed.target_acceptance_id != request.command.target_acceptance_id
        || committed.replayed
    {
        return Err("upstream did not commit the exact fresh preparation".into());
    }
    Ok(committed)
}

async fn forward_and_drop_preparation(
    listener: tokio::net::TcpListener,
    backend: std::net::SocketAddr,
    bearer: HeaderValue,
    expected: transfer_wire::TeamPermanentTransferPreparationRequest,
) -> Result<transfer_wire::TeamPermanentTransferPreparationReceiptWire, String> {
    use super::super::temporary_dispatch_lifecycle::{
        http_request_parts, read_http_request, request_authorization,
    };
    let (mut downstream, _) = listener.accept().await.map_err(|error| error.to_string())?;
    let bytes = read_http_request(&mut downstream).await?;
    let (method, path, body) = http_request_parts(&bytes)?;
    let expected_path = format!(
        "/internal/v1/workspaces/{}/teams/permanent-transfers/prepare",
        expected.actor.scope.workspace_id
    );
    if method != "POST"
        || path != expected_path
        || request_authorization(&bytes)? != bearer.to_str().map_err(|error| error.to_string())?
    {
        return Err("preparation response-drop proxy request scope mismatch".to_owned());
    }
    let parsed: transfer_wire::TeamPermanentTransferPreparationRequest =
        serde_json::from_slice(body).map_err(|error| error.to_string())?;
    if parsed != expected {
        return Err("preparation response-drop proxy request changed".to_owned());
    }
    let upstream = OrchestrationHttpClient::new(backend, super::super::DEADLINE)
        .map_err(|error| error.to_string())?;
    let response = upstream
        .prepare_team_permanent_transfer(&bearer, &parsed)
        .await
        .map_err(|error| format!("upstream preparation before reply loss: {error:?}"))?;
    if response.request != expected || response.receipt.replayed {
        return Err("upstream did not confirm a fresh exact preparation".to_owned());
    }
    drop(downstream);
    Ok(response.receipt)
}
