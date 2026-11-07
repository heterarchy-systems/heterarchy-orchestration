use super::*;
use heterarchy_orchestration_sessions::{
    HomeAcceptanceOperationsAdmission, HomeAcceptanceOperationsResolution,
    HomeAcceptanceProfileOperationsAdmission, HomeAcceptanceProfileOperationsResolution,
};

pub(super) fn create(
    root: &Path,
    operations: &SourceDispatchFixture,
) -> SourceDispatchResult<(
    AgentProfileCatalog,
    AgentSessionCatalog,
    AgentProfile,
    RegisteredAgentSession,
)> {
    let mut profiles = AgentProfileCatalog::open(root.join("profiles.sqlite3"))?;
    let mut sessions = AgentSessionCatalog::open(root.join("sessions.sqlite3"))?;
    let profile = profiles
        .create_profile(&CreateAgentProfile::new(
            AgentProfileId::from_uuid(MEMBER_FOUR_PROFILE)?,
            Uuid::from_u128(0xAC30),
            "native-home-recipient".to_owned(),
            "Native Home recipient".to_owned(),
            AgentProfileProviderFamily::ChatGpt,
            None,
            None,
            vec!["backend".to_owned()],
            AgentProfileCreationOrigin::UserManual,
            ts(10)?,
        )?)?
        .profile()
        .clone();
    let session = sessions
        .open_session(&OpenAgentSession::new(
            AgentSessionId::from_uuid(MEMBER_FOUR_SESSION),
            AgentSessionOpenRequestId::from_uuid(Uuid::from_u128(0xAC31)),
            AuthenticatedPrincipal::TrustedStdio,
            ts(20)?,
        ))?
        .session()
        .clone();
    let mapping:Vec<u8>=operations.transfer.catalog.connection.query_row(
        "SELECT profile_mapping_request_id FROM ops_workspace_participation_mappings WHERE workspace_id=?1 AND agent_profile_id=?2 AND session_id=?3",
        params![WORKSPACE.into_uuid().as_bytes(),MEMBER_FOUR_PROFILE.as_bytes(),MEMBER_FOUR_SESSION.as_bytes()],|row|row.get(0),
    )?;
    let _link = profiles.link_resolved_logical_session(
        Uuid::from_slice(&mapping)?,
        profile.id(),
        &session,
        ts(30)?,
    )?;
    Ok((profiles, sessions, profile, session))
}

pub(super) fn settle(
    f: &mut Fixture,
    source: &HomeHandoffAcceptanceOutcomeRecord,
    operations: &HomeAcceptanceResolutionReceipt,
    holders: Option<(
        &HomeAcceptanceSessionRecord,
        &HomeAcceptanceProfilePinRecord,
    )>,
) -> SourceDispatchResult<(HomeAcceptanceSessionRecord, HomeAcceptanceProfilePinRecord)> {
    let profile_admission = operations
        .admission()
        .map(|admission| {
            HomeAcceptanceProfileOperationsAdmission::new(
                admission.acceptance_id(),
                admission.receipt_id(),
                admission.receipt_digest(),
            )
        })
        .transpose()?;
    let profile_resolution = HomeAcceptanceProfileOperationsResolution::new(
        source,
        operations.acceptance_id(),
        operations.identity().issuer(),
        operations.resolution_id(),
        operations.request().request_digest(),
        operations.outcome(),
        operations.source_payload_digest(),
        operations.resolution_digest(),
        profile_admission,
        holders.and_then(|(_, profile)| profile.profile_pin()),
    )?;
    let profile_intent = HomeAcceptanceProfileIntent::new(
        source.request(),
        f.identity.issuer(),
        Some((f.profile.id(), f.profile.revision())),
    )?;
    let profile_time = ts(730)?;
    let profile = f.profiles.resolve_home_acceptance_profile(
        &profile_intent,
        f.identity.issuer(),
        source,
        &profile_resolution,
        || Ok(profile_time),
    )?;
    let session_admission = operations
        .admission()
        .map(|admission| -> SourceDispatchResult<_> {
            let held = holders
                .and_then(|(session, _)| session.held_reference())
                .ok_or("original Session hold missing")?;
            Ok(HomeAcceptanceOperationsAdmission::new(
                admission.acceptance_id(),
                admission.receipt_id(),
                admission.receipt_digest(),
                held,
            )?)
        })
        .transpose()?;
    let session_resolution = HomeAcceptanceOperationsResolution::new(
        source,
        operations.resolution_digest(),
        operations.source_payload_digest(),
        session_admission,
    )?;
    let session_intent = HomeAcceptanceSessionIntent::new(
        source.request(),
        f.identity.issuer(),
        f.session.revision(),
    )?;
    let session_time = ts(740)?;
    let session = f.sessions.resolve_home_acceptance_session(
        &session_intent,
        f.identity.issuer(),
        source,
        session_resolution,
        || Ok(session_time),
    )?;
    Ok((session, profile))
}
