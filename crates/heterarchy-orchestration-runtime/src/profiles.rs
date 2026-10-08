//! Profile configuration/lifecycle application operations and frozen readback.

pub mod workspace;

use crate::{
    AgentProfileSummary, OrchestrationRuntime, RuntimeError, agent_profile_summary,
    profile_error_to_runtime,
};
use forge_domain::{AgentSessionId, AgentSessionState, AuthenticatedPrincipal, TimestampMillis};
use heterarchy_orchestration_sessions::{
    AgentProfileId, ProfileMutationCommand, ProfileMutationKind, ProfileMutationReceipt,
};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileMutationSummary {
    pub request_id: Uuid,
    pub profile: AgentProfileSummary,
    pub expected_revision: Option<i64>,
    pub kind: &'static str,
    pub replayed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProfileSessionBindingSummary {
    pub request_id: Uuid,
    pub profile_id: Uuid,
    pub profile_revision: i64,
    pub session_id: Uuid,
    pub session_revision: i64,
    pub created: bool,
}

impl OrchestrationRuntime {
    /// Bind one authenticated open logical Session to an active Profile.
    ///
    /// The Session is resolved through the canonical principal-bound catalog
    /// before the existing immutable Profile link algorithm runs. Replays
    /// therefore retain the original Profile and Session revisions.
    ///
    /// # Errors
    /// Returns forbidden for foreign or closed Sessions, invalid identity/time,
    /// replay conflicts, inactive/missing Profiles or catalog failures.
    pub fn bind_profile_session(
        &self,
        request_id: Uuid,
        profile_id: Uuid,
        session_id: Uuid,
        principal: AuthenticatedPrincipal,
        now_ms: i64,
    ) -> Result<ProfileSessionBindingSummary, RuntimeError> {
        let profile_id = AgentProfileId::from_uuid(profile_id).map_err(profile_error_to_runtime)?;
        let session = self.resolve_owned_session(session_id, principal)?;
        if session.state() != AgentSessionState::Open {
            return Err(RuntimeError::Forbidden);
        }
        let linked_at = TimestampMillis::new(now_ms).map_err(|_| RuntimeError::InvalidInput)?;
        let outcome = self
            .profile_catalog_guard()?
            .link_resolved_logical_session(request_id, profile_id, &session, linked_at)
            .map_err(profile_error_to_runtime)?;
        Ok(binding_summary(outcome.link(), outcome.created()))
    }

    /// Read one Profile-to-Session link for its authenticated Session owner.
    /// Terminal Sessions remain readable for durable recovery/readback.
    ///
    /// # Errors
    /// Returns forbidden for a missing or foreign Session and catalog failures.
    pub fn profile_session_binding(
        &self,
        profile_id: Uuid,
        session_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<ProfileSessionBindingSummary>, RuntimeError> {
        let profile_id = AgentProfileId::from_uuid(profile_id).map_err(profile_error_to_runtime)?;
        let _session = self.session(session_id, principal)?;
        let link = self
            .profile_catalog_guard()?
            .logical_session_link(AgentSessionId::from_uuid(session_id))
            .map_err(profile_error_to_runtime)?;
        Ok(link
            .filter(|link| link.profile_id() == profile_id)
            .map(|link| binding_summary(link, false)))
    }

    /// Commit one exact Profile request with a durable immutable receipt.
    ///
    /// # Errors
    /// Preserves validation, stale/replay conflicts, lifecycle safety and unknown
    /// commit identity for exact receipt readback.
    pub fn mutate_agent_profile(
        &self,
        command: &ProfileMutationCommand,
    ) -> Result<ProfileMutationSummary, RuntimeError> {
        self.profile_catalog_guard()?
            .mutate_profile(command)
            .map(|receipt| summary(&receipt))
            .map_err(profile_error_to_runtime)
    }

    /// Read the original committed snapshot, including after later mutations.
    ///
    /// # Errors
    /// Returns invalid selector or catalog/receipt corruption failures.
    pub fn profile_mutation_receipt(
        &self,
        request_id: Uuid,
    ) -> Result<Option<ProfileMutationSummary>, RuntimeError> {
        if request_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        self.profile_catalog_guard()?
            .mutation_receipt(request_id)
            .map(|receipt| receipt.as_ref().map(summary))
            .map_err(profile_error_to_runtime)
    }
}

fn summary(receipt: &ProfileMutationReceipt) -> ProfileMutationSummary {
    ProfileMutationSummary {
        request_id: receipt.request_id(),
        profile: agent_profile_summary(receipt.profile()),
        expected_revision: receipt
            .expected_revision()
            .map(heterarchy_orchestration_sessions::AgentProfileRevision::get),
        kind: match receipt.kind() {
            ProfileMutationKind::OriginalCreate => "create",
            ProfileMutationKind::Update => "update",
            ProfileMutationKind::Disable => "disable",
            ProfileMutationKind::Retire => "retire",
        },
        replayed: receipt.replayed(),
    }
}

fn binding_summary(
    link: heterarchy_orchestration_sessions::AgentProfileLogicalSessionLink,
    created: bool,
) -> ProfileSessionBindingSummary {
    ProfileSessionBindingSummary {
        request_id: link.request_id(),
        profile_id: link.profile_id().into_uuid(),
        profile_revision: link.profile_revision().get(),
        session_id: link.session_id().into_uuid(),
        session_revision: link.session_revision().get(),
        created,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use forge_domain::{AuthenticatedPrincipal, DirectTokenId};

    use super::*;

    fn create_command(request_id: Uuid, name: &str) -> crate::AgentProfileCreateCommand {
        crate::AgentProfileCreateCommand {
            request_id,
            stable_name: name.to_owned(),
            display_name: name.to_owned(),
            preferred_model: Some("gpt-6-luna".to_owned()),
            preferred_reasoning: Some("max".to_owned()),
            base_capabilities: vec!["test".to_owned()],
            created_at_ms: 1,
        }
    }

    #[test]
    fn profile_session_binding_enforces_owner_lifecycle_and_replay()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("profile-session-binding-{}", Uuid::now_v7()));
        fs::create_dir_all(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        let owner =
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(1)));
        let foreign =
            AuthenticatedPrincipal::DirectToken(DirectTokenId::from_uuid(Uuid::from_u128(2)));
        let profile = runtime
            .create_agent_profile(create_command(Uuid::from_u128(10), "profile-one"))?
            .profile;
        let second_profile = runtime
            .create_agent_profile(create_command(Uuid::from_u128(11), "profile-two"))?
            .profile;
        let session_id = runtime
            .open_session(Uuid::from_u128(20), owner, 1)?
            .session
            .session_id;
        let request_id = Uuid::from_u128(30);

        let created =
            runtime.bind_profile_session(request_id, profile.profile_id, session_id, owner, 2)?;
        assert!(created.created);
        assert_eq!(created.request_id, request_id);
        assert_eq!(created.profile_revision, profile.revision);
        assert_eq!(created.session_revision, 1);

        let alias_replay = runtime.bind_profile_session(
            Uuid::from_u128(31),
            profile.profile_id,
            session_id,
            owner,
            3,
        )?;
        assert!(!alias_replay.created);
        assert_eq!(alias_replay.request_id, request_id);
        assert_eq!(alias_replay.profile_id, created.profile_id);
        assert_eq!(alias_replay.session_id, created.session_id);

        assert!(matches!(
            runtime.bind_profile_session(
                request_id,
                second_profile.profile_id,
                session_id,
                owner,
                4,
            ),
            Err(RuntimeError::Conflict)
        ));
        let second_session = runtime
            .open_session(Uuid::from_u128(21), owner, 5)?
            .session
            .session_id;
        assert!(matches!(
            runtime.bind_profile_session(request_id, profile.profile_id, second_session, owner, 6),
            Err(RuntimeError::Conflict)
        ));
        assert_eq!(
            runtime.profile_session_binding(second_profile.profile_id, session_id, owner)?,
            None
        );
        assert!(matches!(
            runtime.bind_profile_session(
                Uuid::from_u128(32),
                profile.profile_id,
                second_session,
                foreign,
                7,
            ),
            Err(RuntimeError::Forbidden)
        ));
        runtime.close_session(second_session, owner, 1, 8)?;
        assert!(matches!(
            runtime.bind_profile_session(
                Uuid::from_u128(33),
                profile.profile_id,
                second_session,
                owner,
                9,
            ),
            Err(RuntimeError::Forbidden)
        ));

        runtime.close_session(session_id, owner, 1, 10)?;
        let terminal = runtime
            .profile_session_binding(profile.profile_id, session_id, owner)?
            .ok_or("terminal owner link disappeared")?;
        assert_eq!(terminal.request_id, request_id);
        assert_eq!(terminal.session_id, session_id);
        assert!(!terminal.created);
        assert!(matches!(
            runtime.profile_session_binding(profile.profile_id, session_id, foreign),
            Err(RuntimeError::Forbidden)
        ));

        drop(runtime);
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
