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
                    let profile = catalog.resolve(entry.profile_id())?.ok_or(
                        WorkspaceProfileQueryError::Inconsistent(
                            "Workspace participation references a missing Agent Profile",
                        ),
                    )?;
                    let state = catalog.operational_state(entry.profile_id())?.ok_or(
                        WorkspaceProfileQueryError::Inconsistent(
                            "Workspace participation Agent Profile state is missing",
                        ),
                    )?;
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
