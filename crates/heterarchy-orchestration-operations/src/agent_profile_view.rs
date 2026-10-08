//! Phase-C read composition for Global Agent Profile inspection.
//!
//! This module composes already-durable Profile/Session/Workspace identity facts
//! into one bounded read model. It creates no membership, vote, leadership,
//! `WorkCredit`, assignment, Writer Lease, or Forge effect authority.

use forge_domain::{
    ActivityId, AgentInstanceId, AgentSessionId, AgentSessionRevision, AgentSessionState,
    RegisteredAgentSession, TimestampMillis, WorkspaceId,
};
use heterarchy_orchestration_sessions::{
    AgentProfile, AgentProfileCatalog, AgentProfileError, AgentProfileId,
    AgentProfileLogicalSessionLink, AgentProfileRevision,
};
use thiserror::Error;
use uuid::Uuid;

use crate::{OperationsCatalog, OperationsError, WorkspaceParticipationId};

/// Bounded number of caller-verified logical Session snapshots accepted by one
/// composed Profile view.
pub const MAX_AGENT_PROFILE_VERIFIED_SESSIONS: usize = 32;

/// Durable source that binds one linked Session into this Workspace view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProfileWorkspaceSessionBindingKind {
    InitialMapping,
    SessionRebind,
}

/// Redacted current runtime observation for one linked logical Session.
///
/// `Unknown` means the caller did not provide a principal-verified Session
/// record. It never means the Session is absent or closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProfileSessionRuntimeObservation {
    Unknown,
    Open {
        revision: AgentSessionRevision,
        updated_at: TimestampMillis,
    },
    Closed {
        revision: AgentSessionRevision,
        updated_at: TimestampMillis,
    },
}

/// One current Forge effect binding in the composed read view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentProfileWorkspaceEffectView {
    activity_id: ActivityId,
    agent_id: AgentInstanceId,
}

impl AgentProfileWorkspaceEffectView {
    #[must_use]
    pub const fn activity_id(self) -> ActivityId {
        self.activity_id
    }

    #[must_use]
    pub const fn agent_id(self) -> AgentInstanceId {
        self.agent_id
    }
}

/// Current legacy logical participant/effect projection for one Session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileWorkspaceCurrentIdentityView {
    participant_id: Uuid,
    effect_bindings: Vec<AgentProfileWorkspaceEffectView>,
}

impl AgentProfileWorkspaceCurrentIdentityView {
    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }

    #[must_use]
    pub fn effect_bindings(&self) -> &[AgentProfileWorkspaceEffectView] {
        &self.effect_bindings
    }
}

/// One linked logical Session as seen through a single Workspace scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileWorkspaceSessionView {
    link_request_id: Uuid,
    session_id: AgentSessionId,
    linked_profile_revision: AgentProfileRevision,
    linked_session_revision: AgentSessionRevision,
    linked_at: TimestampMillis,
    runtime: AgentProfileSessionRuntimeObservation,
    participation_binding_kind: Option<AgentProfileWorkspaceSessionBindingKind>,
    participation_mapping_request_id: Option<Uuid>,
    mapped_participant_id: Option<Uuid>,
    mapped_membership_revision: Option<u64>,
    current_identity: Option<AgentProfileWorkspaceCurrentIdentityView>,
}

impl AgentProfileWorkspaceSessionView {
    #[must_use]
    pub const fn link_request_id(&self) -> Uuid {
        self.link_request_id
    }

    pub const fn session_id(&self) -> AgentSessionId {
        self.session_id
    }

    #[must_use]
    pub const fn linked_profile_revision(&self) -> AgentProfileRevision {
        self.linked_profile_revision
    }

    pub const fn linked_session_revision(&self) -> AgentSessionRevision {
        self.linked_session_revision
    }

    #[must_use]
    pub const fn linked_at(&self) -> TimestampMillis {
        self.linked_at
    }

    #[must_use]
    pub const fn runtime(&self) -> AgentProfileSessionRuntimeObservation {
        self.runtime
    }

    #[must_use]
    pub const fn participation_binding_kind(
        &self,
    ) -> Option<AgentProfileWorkspaceSessionBindingKind> {
        self.participation_binding_kind
    }

    #[must_use]
    pub const fn participation_mapping_request_id(&self) -> Option<Uuid> {
        self.participation_mapping_request_id
    }

    #[must_use]
    pub const fn mapped_participant_id(&self) -> Option<Uuid> {
        self.mapped_participant_id
    }

    #[must_use]
    pub const fn mapped_membership_revision(&self) -> Option<u64> {
        self.mapped_membership_revision
    }

    #[must_use]
    pub const fn current_identity(&self) -> Option<&AgentProfileWorkspaceCurrentIdentityView> {
        self.current_identity.as_ref()
    }
}

/// Bounded Phase-C projection joining a global Profile with one Workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileWorkspaceReadView {
    profile: AgentProfile,
    workspace_id: WorkspaceId,
    participation_id: Option<WorkspaceParticipationId>,
    participation_revision: Option<u64>,
    session_history_truncated: bool,
    sessions: Vec<AgentProfileWorkspaceSessionView>,
}

impl AgentProfileWorkspaceReadView {
    #[must_use]
    pub const fn profile(&self) -> &AgentProfile {
        &self.profile
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn participation_id(&self) -> Option<WorkspaceParticipationId> {
        self.participation_id
    }

    #[must_use]
    pub const fn participation_revision(&self) -> Option<u64> {
        self.participation_revision
    }

    #[must_use]
    pub const fn session_history_truncated(&self) -> bool {
        self.session_history_truncated
    }

    #[must_use]
    pub fn sessions(&self) -> &[AgentProfileWorkspaceSessionView] {
        &self.sessions
    }
}

/// Read-composition failures. No branch changes any operational authority.
#[derive(Debug, Error)]
pub enum AgentProfileWorkspaceReadError {
    #[error("global Agent Profile read failed")]
    Profile(#[from] AgentProfileError),
    #[error("Workspace identity read failed")]
    Operations(#[from] OperationsError),
    #[error("Agent Profile read composition is inconsistent: {0}")]
    Inconsistent(&'static str),
    #[error("Agent Profile verified Session input exceeds the bounded limit")]
    VerifiedSessionBound,
}

impl OperationsCatalog {
    /// Compose one global Profile and one Workspace into a bounded read-only view.
    ///
    /// `verified_sessions` must already have been resolved through the
    /// principal-bound Session authority. The composer never opens or bypasses
    /// the Session catalog itself.
    ///
    /// # Errors
    /// Returns Profile/Operations read failures, bounded-input failures, or
    /// contradictory revision/identity observations.
    pub fn agent_profile_workspace_read_view(
        &self,
        profile_catalog: &AgentProfileCatalog,
        profile_id: AgentProfileId,
        workspace_id: WorkspaceId,
        verified_sessions: &[RegisteredAgentSession],
    ) -> Result<Option<AgentProfileWorkspaceReadView>, AgentProfileWorkspaceReadError> {
        if verified_sessions.len() > MAX_AGENT_PROFILE_VERIFIED_SESSIONS {
            return Err(AgentProfileWorkspaceReadError::VerifiedSessionBound);
        }
        let Some(profile) = profile_catalog.resolve(profile_id)? else {
            return Ok(None);
        };
        let history = profile_catalog.logical_session_history(profile_id)?;
        let participation = self.workspace_participation(workspace_id, profile_id)?;
        let mut sessions = Vec::with_capacity(history.links().len());
        for link in history.links() {
            if link.profile_revision() > profile.revision() {
                return Err(AgentProfileWorkspaceReadError::Inconsistent(
                    "linked Profile revision is newer than current Profile",
                ));
            }
            sessions.push(self.compose_profile_session_view(
                workspace_id,
                *link,
                participation.as_ref(),
                verified_sessions,
            )?);
        }
        let (participation_id, participation_revision) = if let Some(value) = participation.as_ref()
        {
            (Some(value.participation_id()), Some(value.revision()))
        } else {
            (None, None)
        };
        Ok(Some(AgentProfileWorkspaceReadView {
            profile,
            workspace_id,
            participation_id,
            participation_revision,
            session_history_truncated: history.truncated(),
            sessions,
        }))
    }

    fn compose_profile_session_view(
        &self,
        workspace_id: WorkspaceId,
        link: AgentProfileLogicalSessionLink,
        participation: Option<&crate::WorkspaceParticipationSnapshot>,
        verified_sessions: &[RegisteredAgentSession],
    ) -> Result<AgentProfileWorkspaceSessionView, AgentProfileWorkspaceReadError> {
        let runtime = verified_runtime_observation(link, verified_sessions)?;
        let mapping = participation.and_then(|value| {
            value
                .session_mappings()
                .iter()
                .find(|mapping| mapping.session_id() == link.session_id())
        });
        let rebind = if mapping.is_none() {
            self.workspace_participation_session_rebind_for_session(
                workspace_id,
                link.session_id(),
            )?
        } else {
            None
        };
        if let Some(receipt) = rebind {
            let Some(participation) = participation else {
                return Err(AgentProfileWorkspaceReadError::Inconsistent(
                    "Session rebind exists without WorkspaceParticipation",
                ));
            };
            if receipt.profile_id() != link.profile_id()
                || receipt.profile_revision() != link.profile_revision()
                || receipt.profile_mapping_request_id() != link.request_id()
                || receipt.new_session_revision() != link.session_revision()
                || receipt.participation_id() != participation.participation_id()
            {
                return Err(AgentProfileWorkspaceReadError::Inconsistent(
                    "Session rebind receipt differs from durable Profile link",
                ));
            }
        }
        let mapped_participant_id = mapping.map(|value| value.participant_id()).or_else(|| {
            rebind.map(crate::WorkspaceParticipationSessionRebindReceipt::participant_id)
        });
        let mapped_membership_revision = mapping
            .map(|value| value.participant_membership_revision())
            .or_else(|| {
                rebind.map(
                    crate::WorkspaceParticipationSessionRebindReceipt::participant_membership_revision,
                )
            });
        let current_identity = self
            .logical_identity_for_session(workspace_id, link.session_id())?
            .map(|identity| {
                if let Some(mapped_participant_id) = mapped_participant_id
                    && identity.participant_id() != mapped_participant_id
                {
                    return Err(AgentProfileWorkspaceReadError::Inconsistent(
                        "current participant differs from WorkspaceParticipation mapping",
                    ));
                }
                let effect_bindings = identity
                    .effect_bindings()
                    .iter()
                    .copied()
                    .map(|binding| AgentProfileWorkspaceEffectView {
                        activity_id: binding.activity_id(),
                        agent_id: binding.agent_id(),
                    })
                    .collect();
                Ok(AgentProfileWorkspaceCurrentIdentityView {
                    participant_id: identity.participant_id(),
                    effect_bindings,
                })
            })
            .transpose()?;
        Ok(AgentProfileWorkspaceSessionView {
            link_request_id: link.request_id(),
            session_id: link.session_id(),
            linked_profile_revision: link.profile_revision(),
            linked_session_revision: link.session_revision(),
            linked_at: link.linked_at(),
            runtime,
            participation_binding_kind: mapping
                .map(|_| AgentProfileWorkspaceSessionBindingKind::InitialMapping)
                .or_else(|| rebind.map(|_| AgentProfileWorkspaceSessionBindingKind::SessionRebind)),
            participation_mapping_request_id: mapping.map(|value| value.request_id()).or_else(
                || rebind.map(crate::WorkspaceParticipationSessionRebindReceipt::request_id),
            ),
            mapped_participant_id,
            mapped_membership_revision,
            current_identity,
        })
    }
}

fn verified_runtime_observation(
    link: AgentProfileLogicalSessionLink,
    verified_sessions: &[RegisteredAgentSession],
) -> Result<AgentProfileSessionRuntimeObservation, AgentProfileWorkspaceReadError> {
    let mut matched = None;
    for session in verified_sessions {
        if session.id() != link.session_id() {
            continue;
        }
        if matched.is_some() {
            return Err(AgentProfileWorkspaceReadError::Inconsistent(
                "duplicate verified Session observation",
            ));
        }
        matched = Some(session);
    }
    let Some(session) = matched else {
        return Ok(AgentProfileSessionRuntimeObservation::Unknown);
    };
    if session.revision() < link.session_revision() {
        return Err(AgentProfileWorkspaceReadError::Inconsistent(
            "verified Session revision predates the durable Profile link",
        ));
    }
    Ok(match session.state() {
        AgentSessionState::Open => AgentProfileSessionRuntimeObservation::Open {
            revision: session.revision(),
            updated_at: session.updated_at(),
        },
        AgentSessionState::Closed => AgentProfileSessionRuntimeObservation::Closed {
            revision: session.revision(),
            updated_at: session.updated_at(),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_domain::{AgentSessionOpenRequestId, AuthenticatedPrincipal, OpenAgentSession};
    use heterarchy_orchestration_sessions::{
        AgentProfileCreationOrigin, AgentProfileProviderFamily, AgentSessionCatalog,
        CreateAgentProfile, link_existing_logical_session,
    };
    use rusqlite::params;
    use std::error::Error;

    struct Fixture {
        root: std::path::PathBuf,
        profiles: AgentProfileCatalog,
        sessions: AgentSessionCatalog,
        operations: OperationsCatalog,
        profile_id: AgentProfileId,
        workspace_id: WorkspaceId,
        participation_id: WorkspaceParticipationId,
        principal: AuthenticatedPrincipal,
        historical_session: RegisteredAgentSession,
        current_session: RegisteredAgentSession,
    }

    struct ProfileSessionSetup {
        profiles: AgentProfileCatalog,
        sessions: AgentSessionCatalog,
        profile_id: AgentProfileId,
        principal: AuthenticatedPrincipal,
        historical_session: RegisteredAgentSession,
        current_session: RegisteredAgentSession,
        historical_link: AgentProfileLogicalSessionLink,
        current_link: AgentProfileLogicalSessionLink,
    }

    fn prepare_profile_sessions(
        root: &std::path::Path,
    ) -> Result<ProfileSessionSetup, Box<dyn Error>> {
        let mut profiles = AgentProfileCatalog::open(root.join("agent-profiles.sqlite3"))?;
        let mut sessions = AgentSessionCatalog::open(root.join("agent-sessions.sqlite3"))?;
        let profile_id = AgentProfileId::from_uuid(Uuid::from_u128(0xC002))?;
        let profile = CreateAgentProfile::new(
            profile_id,
            Uuid::from_u128(0xC003),
            "luna-rust-01".to_owned(),
            "Luna Rust 01".to_owned(),
            AgentProfileProviderFamily::ChatGpt,
            Some("gpt-5.6-luna".to_owned()),
            Some("max".to_owned()),
            vec!["backend".to_owned(), "rust".to_owned()],
            AgentProfileCreationOrigin::UserManual,
            TimestampMillis::new(10)?,
        )?;
        let _profile = profiles.create_profile(&profile)?;
        let principal = AuthenticatedPrincipal::TrustedStdio;
        let historical_session_id = AgentSessionId::from_uuid(Uuid::from_u128(0xC010));
        let current_session_id = AgentSessionId::from_uuid(Uuid::from_u128(0xC011));
        let historical_session = sessions
            .open_session(&OpenAgentSession::new(
                historical_session_id,
                AgentSessionOpenRequestId::from_uuid(Uuid::from_u128(0xC012)),
                principal,
                TimestampMillis::new(11)?,
            ))?
            .session()
            .clone();
        let current_session = sessions
            .open_session(&OpenAgentSession::new(
                current_session_id,
                AgentSessionOpenRequestId::from_uuid(Uuid::from_u128(0xC013)),
                principal,
                TimestampMillis::new(12)?,
            ))?
            .session()
            .clone();
        let historical_link = link_existing_logical_session(
            &mut profiles,
            &sessions,
            profile_id,
            historical_session_id,
            principal,
            Uuid::from_u128(0xC014),
            TimestampMillis::new(13)?,
        )?
        .link();
        let current_link = link_existing_logical_session(
            &mut profiles,
            &sessions,
            profile_id,
            current_session_id,
            principal,
            Uuid::from_u128(0xC015),
            TimestampMillis::new(14)?,
        )?
        .link();
        Ok(ProfileSessionSetup {
            profiles,
            sessions,
            profile_id,
            principal,
            historical_session,
            current_session,
            historical_link,
            current_link,
        })
    }

    fn prepare_workspace(
        root: &std::path::Path,
        setup: &ProfileSessionSetup,
    ) -> Result<(OperationsCatalog, WorkspaceId, WorkspaceParticipationId), Box<dyn Error>> {
        let mut operations =
            OperationsCatalog::open(&root.join("operations.sqlite3"), Uuid::from_u128(0xC001))?;
        let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(0xC020));
        let historical_participant = Uuid::from_u128(0xC021);
        let current_participant = Uuid::from_u128(0xC022);
        for (participant_id, session_id, name) in [
            (
                historical_participant,
                setup.historical_session.id(),
                "historical-session",
            ),
            (
                current_participant,
                setup.current_session.id(),
                "current-session",
            ),
        ] {
            operations.connection.execute(
                "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,?4,'worker',1,1,15)",
                params![
                    workspace_id.into_uuid().as_bytes(),
                    participant_id.as_bytes(),
                    session_id.into_uuid().as_bytes(),
                    name,
                ],
            )?;
        }
        let current_activity = ActivityId::from_uuid(Uuid::from_u128(0xC023));
        let current_agent = AgentInstanceId::from_uuid(Uuid::from_u128(0xC024));
        operations.connection.execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
            params![
                workspace_id.into_uuid().as_bytes(),
                current_participant.as_bytes(),
                current_activity.into_uuid().as_bytes(),
                setup.current_session.id().into_uuid().as_bytes(),
                current_agent.into_uuid().as_bytes(),
                Uuid::from_u128(0xC025).as_bytes(),
            ],
        )?;
        let historical_identity = operations
            .logical_identity_for_session(workspace_id, setup.historical_session.id())?
            .ok_or("historical Session identity missing")?;
        let current_identity = operations
            .logical_identity_for_session(workspace_id, setup.current_session.id())?
            .ok_or("current Session identity missing")?;
        let participation_id = WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xC030))?;
        let _first = operations.map_workspace_participation(
            Uuid::from_u128(0xC031),
            participation_id,
            setup.historical_link,
            &historical_identity,
            1,
            TimestampMillis::new(16)?,
        )?;
        let second = operations.map_workspace_participation(
            Uuid::from_u128(0xC032),
            WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xC033))?,
            setup.current_link,
            &current_identity,
            1,
            TimestampMillis::new(17)?,
        )?;
        assert_eq!(second.participation().participation_id(), participation_id);
        Ok((operations, workspace_id, participation_id))
    }

    fn fixture() -> Result<Fixture, Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!(
            "orchestration-agent-profile-view-{}",
            Uuid::now_v7()
        ));
        std::fs::create_dir(&root)?;
        let setup = prepare_profile_sessions(&root)?;
        let (operations, workspace_id, participation_id) = prepare_workspace(&root, &setup)?;
        Ok(Fixture {
            root,
            profiles: setup.profiles,
            sessions: setup.sessions,
            operations,
            profile_id: setup.profile_id,
            workspace_id,
            participation_id,
            principal: setup.principal,
            historical_session: setup.historical_session,
            current_session: setup.current_session,
        })
    }

    #[test]
    fn read_view_composes_global_profile_participation_runtime_and_effects_without_mutation()
    -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture()?;
        let before_changes = fixture.operations.connection.total_changes();
        let view = fixture
            .operations
            .agent_profile_workspace_read_view(
                &fixture.profiles,
                fixture.profile_id,
                fixture.workspace_id,
                std::slice::from_ref(&fixture.current_session),
            )?
            .ok_or("Profile view missing")?;
        assert_eq!(view.profile().id(), fixture.profile_id);
        assert_eq!(view.participation_id(), Some(fixture.participation_id));
        assert_eq!(view.sessions().len(), 2);
        assert!(!view.session_history_truncated());
        let a = view
            .sessions()
            .iter()
            .find(|session| session.session_id() == fixture.historical_session.id())
            .ok_or("historical Session view missing")?;
        let b = view
            .sessions()
            .iter()
            .find(|session| session.session_id() == fixture.current_session.id())
            .ok_or("current Session view missing")?;
        assert_eq!(a.runtime(), AgentProfileSessionRuntimeObservation::Unknown);
        assert!(matches!(
            b.runtime(),
            AgentProfileSessionRuntimeObservation::Open { .. }
        ));
        assert!(a.mapped_participant_id().is_some());
        assert_eq!(
            a.current_identity()
                .map(|identity| identity.effect_bindings().len()),
            Some(0)
        );
        assert_eq!(
            b.current_identity()
                .map(|identity| identity.effect_bindings().len()),
            Some(1)
        );
        assert_eq!(
            fixture.operations.connection.total_changes(),
            before_changes
        );

        let closed_a = fixture.sessions.close(
            fixture.historical_session.id(),
            fixture.principal,
            fixture.historical_session.revision(),
            TimestampMillis::new(20)?,
        )?;
        let view = fixture
            .operations
            .agent_profile_workspace_read_view(
                &fixture.profiles,
                fixture.profile_id,
                fixture.workspace_id,
                &[closed_a.clone(), fixture.current_session.clone()],
            )?
            .ok_or("Profile view after close missing")?;
        let a = view
            .sessions()
            .iter()
            .find(|session| session.session_id() == closed_a.id())
            .ok_or("closed session A view missing")?;
        assert!(matches!(
            a.runtime(),
            AgentProfileSessionRuntimeObservation::Closed { .. }
        ));
        let root = fixture.root.clone();
        drop(fixture);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
