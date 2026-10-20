use super::*;

pub(super) async fn assert_native_member_registration(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    principal: AuthenticatedPrincipal,
    source: &ForgeHost,
    scope: &WorkspaceQueryScope,
    activity_id: Uuid,
) -> TestResult {
    let nonce = Uuid::now_v7();
    let opened = client
        .open_session(
            bearer,
            &AgentSessionOpenRequest {
                request_id: nonce.to_string(),
            },
        )
        .await?;
    assert!(opened.created);
    assert_eq!(opened.session.state, "open");

    let session_id = AgentSessionId::from_uuid(opened.session.session_id);
    let control = source.control_client.clone();
    let verified = tokio::task::spawn_blocking(move || {
        control.agent_session_resolve(AgentSessionResolveRequest {
            session_id,
            principal,
        })
    })
    .await??;
    assert_eq!(verified.id().into_uuid(), opened.session.session_id);
    assert_eq!(verified.principal(), principal);

    let target = MemberTarget {
        scope: scope.clone(),
        activity_id,
        session_id: opened.session.session_id,
    };
    let request = fixture_registration(&target);
    let registration = client.register_member(bearer, &request).await?;
    assert_eq!(registration.progress.phase, MemberPhaseWire::Reserved);
    assert_eq!(registration.progress.session_id, opened.session.session_id);

    let replay = client.register_member(bearer, &request).await?;
    assert_eq!(replay.progress, registration.progress);

    let mut changed = request;
    "changed native replay".clone_into(&mut changed.display_name);
    assert!(matches!(
        client.register_member(bearer, &changed).await,
        Err(ClientError::Rejected { status, .. })
            if status == StatusCode::CONFLICT.as_u16()
    ));

    let (other_activity_id, _, _) = bootstrap_fixture_activity(source)?;
    let other_target = MemberTarget {
        scope: scope.clone(),
        activity_id: other_activity_id.into_uuid(),
        session_id: opened.session.session_id,
    };
    let other_activity = client
        .register_member(bearer, &fixture_registration(&other_target))
        .await?;
    assert_eq!(
        other_activity.progress.participant_id,
        registration.progress.participant_id
    );
    assert_ne!(
        other_activity.progress.request_id,
        registration.progress.request_id
    );
    assert_ne!(
        other_activity.progress.agent_id,
        registration.progress.agent_id
    );
    bind_fixture_profile_session(
        client,
        bearer,
        opened.session.session_id,
        "isolated-native-session-linked-later",
    )
    .await?;
    assert!(matches!(
        client
            .register_member(bearer, &fixture_registration(&target))
            .await,
        Err(ClientError::Rejected { status, .. })
            if status == StatusCode::CONFLICT.as_u16()
    ));
    assert_confirmed_member(client, bearer, &registration).await?;
    assert_confirmed_member(client, bearer, &other_activity).await?;

    let workspace_profiles = client
        .workspace_profiles(
            bearer,
            &heterarchy_orchestration_backend::wire::profiles::WorkspaceProfilesQueryRequest {
                scope: scope.clone(),
                session_id: Some(opened.session.session_id),
            },
        )
        .await?;
    assert!(
        workspace_profiles
            .session_profiles
            .iter()
            .all(|binding| binding.session_id != opened.session.session_id)
    );
    Ok(())
}

pub(super) async fn assert_profile_session_anchor(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    scope: &WorkspaceQueryScope,
    session_id: Uuid,
) -> TestResult {
    let projection = client
        .workspace_profiles(
            bearer,
            &heterarchy_orchestration_backend::wire::profiles::WorkspaceProfilesQueryRequest {
                scope: scope.clone(),
                session_id: Some(session_id),
            },
        )
        .await?;
    let mappings: Vec<_> = projection
        .session_profiles
        .iter()
        .filter(|mapping| mapping.session_id == session_id)
        .collect();
    assert_eq!(mappings.len(), 1);
    assert!(
        projection
            .profiles
            .iter()
            .any(|profile| profile.profile.profile_id == mappings[0].profile_id)
    );
    Ok(())
}

pub(super) async fn assert_profile_member_registration(
    client: &OrchestrationHttpClient,
    bearer: &HeaderValue,
    target: &MemberTarget,
    request_id: Uuid,
) -> TestResult<MemberRegisterResponse> {
    let registration = client
        .register_member(bearer, &fixture_registration(target))
        .await?;
    assert_eq!(registration.progress.request_id, request_id);
    assert!(registration.progress.reserved);
    let replay = client
        .register_member(bearer, &fixture_registration(target))
        .await?;
    assert_eq!(replay.progress, registration.progress);
    assert_confirmed_member(client, bearer, &registration).await?;
    assert_profile_session_anchor(client, bearer, &target.scope, target.session_id).await?;
    Ok(registration)
}
