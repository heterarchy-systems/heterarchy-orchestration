//! Bounded Profile projection composed from existing participation and Profile owners.
//! Participation mappings here are observations, never permission or effect authority.

use crate::{AgentProfileSummary, OrchestrationRuntime, RuntimeError, agent_profile_summary};
use forge_domain::WorkspaceId;
use heterarchy_orchestration_operations::{OperationsError, WorkspaceParticipationReadSet};
use heterarchy_orchestration_sessions::{AgentProfileError, AgentProfileOperationalState};
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceProfileItem {
    pub profile: AgentProfileSummary,
    pub state: AgentProfileOperationalState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkspaceSessionProfile {
    pub session_id: Uuid,
    pub profile_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceProfileProjection {
    pub profiles: Vec<WorkspaceProfileItem>,
    pub session_profiles: Vec<WorkspaceSessionProfile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkspaceProfileObservation {
    participation: WorkspaceParticipationReadSet,
    current_sessions: Vec<WorkspaceSessionProfile>,
}

#[derive(Debug, Error)]
pub enum WorkspaceProfileQueryError {
    #[error("invalid Workspace identity")]
    InvalidWorkspace,
    #[error("Workspace participation read failed")]
    Operations(#[from] OperationsError),
    #[error("Agent Profile read failed")]
    Profile(#[from] AgentProfileError),
    #[error("catalog lock poisoned: {0}")]
    LockPoisoned(&'static str),
    #[error("Workspace Profile projection inconsistent: {0}")]
    Inconsistent(&'static str),
    #[error("Workspace participation changed during Profile read")]
    Changed,
}

impl OrchestrationRuntime {
    /// Read the same bounded Profile/Session projection for GUI and HTTP.
    /// Release Operations before Profile reads and revalidate the participation
    /// snapshot afterward. This snapshot has no realtime cursor or authority grant.
    ///
    /// # Errors
    /// Fails closed on truncation, missing Profiles, ambiguous mappings, changed
    /// participation, poisoned guards or persistence failures.
    pub fn workspace_profile_projection(
        &self,
        workspace_id: Uuid,
    ) -> Result<WorkspaceProfileProjection, WorkspaceProfileQueryError> {
        if workspace_id.is_nil() {
            return Err(WorkspaceProfileQueryError::InvalidWorkspace);
        }
        let workspace = WorkspaceId::from_uuid(workspace_id);
        let participation = self.workspace_profile_participation(workspace)?;
        let session_profiles = session_profiles(&participation)?;
        let profiles = {
            let catalog = self
                .profiles
                .lock()
                .map_err(|_| WorkspaceProfileQueryError::LockPoisoned("Agent Profile catalog"))?;
            participation
                .participation
                .participations()
                .iter()
                .map(|entry| {
                    let (profile, state) = catalog
                        .resolve_with_operational_state(entry.profile_id())?
                        .ok_or(WorkspaceProfileQueryError::Inconsistent(
                            "Workspace participation references a missing Agent Profile",
                        ))?;
                    Ok(WorkspaceProfileItem {
                        profile: agent_profile_summary(&profile),
                        state,
                    })
                })
                .collect::<Result<Vec<_>, WorkspaceProfileQueryError>>()?
        };
        self.verify_workspace_profile_participation(workspace, &participation)?;
        Ok(WorkspaceProfileProjection {
            profiles,
            session_profiles,
        })
    }

    fn workspace_profile_participation(
        &self,
        workspace: WorkspaceId,
    ) -> Result<WorkspaceProfileObservation, WorkspaceProfileQueryError> {
        let catalog = self
            .operations
            .lock()
            .map_err(|_| WorkspaceProfileQueryError::LockPoisoned("operations catalog"))?;
        let read = catalog.workspace_participations(workspace)?;
        if read.truncated() {
            return Err(WorkspaceProfileQueryError::Inconsistent(
                "Workspace Agent participation read is truncated",
            ));
        }
        let current_sessions = read
            .participations()
            .iter()
            .map(|entry| {
                let current = catalog
                    .workspace_participation_session_expectation(workspace, entry.profile_id())?
                    .ok_or(WorkspaceProfileQueryError::Inconsistent(
                        "Workspace participation current Session is missing",
                    ))?;
                Ok(WorkspaceSessionProfile {
                    session_id: current.current_session_id().into_uuid(),
                    profile_id: entry.profile_id().into_uuid(),
                })
            })
            .collect::<Result<Vec<_>, WorkspaceProfileQueryError>>()?;
        Ok(WorkspaceProfileObservation {
            participation: read,
            current_sessions,
        })
    }

    fn verify_workspace_profile_participation(
        &self,
        workspace: WorkspaceId,
        expected: &WorkspaceProfileObservation,
    ) -> Result<(), WorkspaceProfileQueryError> {
        if &self.workspace_profile_participation(workspace)? != expected {
            return Err(WorkspaceProfileQueryError::Changed);
        }
        Ok(())
    }
}

fn session_profiles(
    read: &WorkspaceProfileObservation,
) -> Result<Vec<WorkspaceSessionProfile>, WorkspaceProfileQueryError> {
    let mut mapped = HashMap::new();
    for participation in read.participation.participations() {
        let profile = participation.profile_id().into_uuid();
        for mapping in participation.session_mappings() {
            let session = mapping.session_id().into_uuid();
            if mapped
                .insert(session, profile)
                .is_some_and(|previous| previous != profile)
            {
                return Err(WorkspaceProfileQueryError::Inconsistent(
                    "logical Session maps to multiple Agent Profiles in one Workspace",
                ));
            }
        }
    }
    for current in &read.current_sessions {
        if mapped
            .insert(current.session_id, current.profile_id)
            .is_some_and(|previous| previous != current.profile_id)
        {
            return Err(WorkspaceProfileQueryError::Inconsistent(
                "current logical Session maps to multiple Agent Profiles",
            ));
        }
    }
    let mut mapped = mapped
        .into_iter()
        .map(|(session_id, profile_id)| WorkspaceSessionProfile {
            session_id,
            profile_id,
        })
        .collect::<Vec<_>>();
    mapped.sort_unstable_by_key(|entry| entry.session_id);
    Ok(mapped)
}

impl From<WorkspaceProfileQueryError> for RuntimeError {
    fn from(error: WorkspaceProfileQueryError) -> Self {
        match error {
            WorkspaceProfileQueryError::InvalidWorkspace => Self::InvalidInput,
            WorkspaceProfileQueryError::Operations(error) => Self::from(error),
            WorkspaceProfileQueryError::Profile(error) => Self::from(error),
            WorkspaceProfileQueryError::LockPoisoned(_) => Self::CatalogOwnerPoisoned,
            WorkspaceProfileQueryError::Inconsistent(_) | WorkspaceProfileQueryError::Changed => {
                Self::ResyncRequired
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AgentProfileCreateCommand;
    use std::{error::Error, path::PathBuf};

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new(label: &str) -> Result<Self, std::io::Error> {
            let root = std::env::temp_dir().join(format!("{label}-{}", Uuid::now_v7()));
            std::fs::create_dir(&root)?;
            Ok(Self(root))
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            if let Err(error) = std::fs::remove_dir_all(&self.0) {
                eprintln!("failed to remove test root {}: {error}", self.0.display());
            }
        }
    }

    #[derive(Clone, Copy)]
    struct ParticipationSeed {
        workspace_id: Uuid,
        profile_id: Uuid,
        profile_revision: i64,
        participation_id: Uuid,
        participant_id: Uuid,
        session_id: Uuid,
        request_id: Uuid,
        principal_id: Uuid,
    }

    fn seed_participation(
        connection: &rusqlite::Connection,
        seed: ParticipationSeed,
    ) -> Result<(), rusqlite::Error> {
        connection.execute(
            "INSERT INTO ops_workspace_participations(
                workspace_id,agent_profile_id,participation_id,created_at_ms,revision
             ) VALUES(?1,?2,?3,10,1)",
            rusqlite::params![
                seed.workspace_id.as_bytes(),
                seed.profile_id.as_bytes(),
                seed.participation_id.as_bytes(),
            ],
        )?;
        connection.execute(
            "INSERT INTO ops_logical_members(
                workspace_id,participant_id,session_id,principal_kind,principal_id,
                display_name,declared_role,transport_kind,membership_revision,created_at_ms
             ) VALUES(?1,?2,?3,1,?4,'projection member','researcher',2,1,10)",
            rusqlite::params![
                seed.workspace_id.as_bytes(),
                seed.participant_id.as_bytes(),
                seed.session_id.as_bytes(),
                seed.principal_id.as_bytes(),
            ],
        )?;
        connection.execute(
            "INSERT INTO ops_workspace_participation_mappings(
                request_id,workspace_id,participation_id,agent_profile_id,profile_revision,
                profile_mapping_request_id,session_id,session_revision,participant_id,
                participant_membership_revision,mapped_at_ms
             ) VALUES(?1,?2,?3,?4,?5,?1,?6,1,?7,1,10)",
            rusqlite::params![
                seed.request_id.as_bytes(),
                seed.workspace_id.as_bytes(),
                seed.participation_id.as_bytes(),
                seed.profile_id.as_bytes(),
                seed.profile_revision,
                seed.session_id.as_bytes(),
                seed.participant_id.as_bytes(),
            ],
        )?;
        Ok(())
    }

    #[test]
    fn workspace_projection_reads_profile_and_rejects_missing_participation_profile()
    -> Result<(), Box<dyn Error>> {
        let root = TestRoot::new("workspace-profile-projection")?;
        let runtime = OrchestrationRuntime::open(&root.0)?;
        let profile = runtime
            .create_agent_profile(AgentProfileCreateCommand {
                request_id: Uuid::from_u128(0xA201),
                stable_name: "projection-agent".to_owned(),
                display_name: "Projection Agent".to_owned(),
                preferred_model: Some("gpt-5.6-luna".to_owned()),
                preferred_reasoning: Some("max".to_owned()),
                base_capabilities: vec!["backend".to_owned(), "rust".to_owned()],
                created_at_ms: 1,
            })?
            .profile;
        let profile_id = profile.profile_id;
        let profile_revision = profile.revision;

        let workspace_id = Uuid::from_u128(0xA202);
        let participation_id = Uuid::from_u128(0xA203);
        let participant_id = Uuid::from_u128(0xA204);
        let session_id = Uuid::from_u128(0xA205);
        let mapping_request_id = Uuid::from_u128(0xA206);
        let seed = rusqlite::Connection::open(root.0.join("agent-operations.sqlite3"))?;
        seed_participation(
            &seed,
            ParticipationSeed {
                workspace_id,
                profile_id,
                profile_revision,
                participation_id,
                participant_id,
                session_id,
                request_id: mapping_request_id,
                principal_id: Uuid::from_u128(0xA207),
            },
        )?;
        drop(seed);

        let projection = runtime.workspace_profile_projection(workspace_id)?;
        assert_eq!(
            projection.profiles,
            [WorkspaceProfileItem {
                profile,
                state: heterarchy_orchestration_sessions::AgentProfileOperationalState::Offline,
            }]
        );
        assert_eq!(
            projection.session_profiles,
            [WorkspaceSessionProfile {
                session_id,
                profile_id: projection.profiles[0].profile.profile_id,
            }]
        );

        let missing_profile_id = Uuid::from_u128(0xA208);
        let seed = rusqlite::Connection::open(root.0.join("agent-operations.sqlite3"))?;
        seed_participation(
            &seed,
            ParticipationSeed {
                workspace_id,
                profile_id: missing_profile_id,
                profile_revision: 1,
                participation_id: Uuid::from_u128(0xA209),
                participant_id: Uuid::from_u128(0xA20A),
                session_id: Uuid::from_u128(0xA20B),
                request_id: Uuid::from_u128(0xA20C),
                principal_id: Uuid::from_u128(0xA20D),
            },
        )?;
        drop(seed);
        assert!(matches!(
            runtime.workspace_profile_projection(workspace_id),
            Err(WorkspaceProfileQueryError::Inconsistent(
                "Workspace participation references a missing Agent Profile"
            ))
        ));

        drop(runtime);
        Ok(())
    }
}
