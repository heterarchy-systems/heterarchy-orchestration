//! Resolve public operational authors without confusing roles with identities.
//!
//! The native host may construct `LocalOperator`; external MCP adapters must
//! construct `Participant` only after validating the issued Session principal.
//! Every participant is revalidated against its current Workspace effect binding.

use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, AgentTransportKind, WorkspaceId};
use heterarchy_orchestration_sessions::AgentProfileId;
use rusqlite::{Connection, OptionalExtension, params};
use uuid::Uuid;

use crate::{
    OperationsError, WorkspaceParticipationId,
    effect_binding::{self, EffectBindingSource},
    registration::{self, RegistrationPhase},
    registration_profile, wire,
};

/// Trusted ingress actor. A role or caller-supplied display name is not authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationalActor {
    LocalOperator,
    Participant {
        activity_id: ActivityId,
        agent_id: AgentInstanceId,
        session_id: AgentSessionId,
    },
}

/// Immutable identity resolved inside one mutation transaction.
///
/// Keeping the identities disjoint prevents a native Session with no Profile
/// participation from being interpreted as the `LocalOperator`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResolvedOperationalActor {
    Operator {
        display_name: String,
    },
    ProfileParticipant {
        participation_id: WorkspaceParticipationId,
        profile_id: Uuid,
        participant_id: Uuid,
        session_id: AgentSessionId,
        display_name: String,
    },
    NativeParticipant {
        participant_id: Uuid,
        session_id: AgentSessionId,
        display_name: String,
    },
}

impl ResolvedOperationalActor {
    pub(crate) fn key(&self) -> String {
        match self {
            Self::Operator { .. } => "operator".to_owned(),
            Self::ProfileParticipant {
                participation_id, ..
            } => format!("participant:{participation_id}"),
            Self::NativeParticipant { participant_id, .. } => {
                format!("native-participant:{participant_id}")
            }
        }
    }

    pub(crate) const fn is_operator(&self) -> bool {
        matches!(self, Self::Operator { .. })
    }

    pub(crate) const fn is_profile_participant(&self) -> bool {
        matches!(self, Self::ProfileParticipant { .. })
    }

    pub(crate) const fn is_native_participant(&self) -> bool {
        matches!(self, Self::NativeParticipant { .. })
    }

    pub(crate) const fn participation_id(&self) -> Option<WorkspaceParticipationId> {
        match self {
            Self::ProfileParticipant {
                participation_id, ..
            } => Some(*participation_id),
            Self::Operator { .. } | Self::NativeParticipant { .. } => None,
        }
    }

    pub(crate) const fn profile_id(&self) -> Option<Uuid> {
        match self {
            Self::ProfileParticipant { profile_id, .. } => Some(*profile_id),
            Self::Operator { .. } | Self::NativeParticipant { .. } => None,
        }
    }

    pub(crate) const fn participant_id(&self) -> Option<Uuid> {
        match self {
            Self::ProfileParticipant { participant_id, .. }
            | Self::NativeParticipant { participant_id, .. } => Some(*participant_id),
            Self::Operator { .. } => None,
        }
    }

    pub(crate) const fn session_id(&self) -> Option<AgentSessionId> {
        match self {
            Self::ProfileParticipant { session_id, .. }
            | Self::NativeParticipant { session_id, .. } => Some(*session_id),
            Self::Operator { .. } => None,
        }
    }

    pub(crate) fn display_name(&self) -> &str {
        match self {
            Self::Operator { display_name }
            | Self::ProfileParticipant { display_name, .. }
            | Self::NativeParticipant { display_name, .. } => display_name,
        }
    }
}

pub(crate) fn resolve_actor(
    connection: &Connection,
    workspace_id: WorkspaceId,
    actor: OperationalActor,
) -> Result<ResolvedOperationalActor, OperationsError> {
    let OperationalActor::Participant {
        activity_id,
        agent_id,
        session_id,
    } = actor
    else {
        return Ok(ResolvedOperationalActor::Operator {
            display_name: "Local operator".to_owned(),
        });
    };
    let binding =
        effect_binding::exact(connection, workspace_id, activity_id, agent_id, session_id)?.ok_or(
            OperationsError::Conflict("operational actor is not a Ready Workspace participant"),
        )?;
    let participant_id = binding.participant_id();

    if binding.source() == EffectBindingSource::Registration
        && let Some(progress) = registration::progress_at(connection, binding.source_request_id())?
    {
        let request_id = binding.source_request_id();
        if progress.phase() != RegistrationPhase::Ready
            || progress.workspace_id() != workspace_id
            || progress.participant_id() != participant_id
            || progress.request().binding().request_id() != request_id
            || progress.request().binding().activity_id() != activity_id
            || progress.request().binding().agent_id() != agent_id
            || progress.request().binding().session_id() != session_id
        {
            return Err(OperationsError::Conflict(
                "operational actor registration source changed",
            ));
        }
        if let Some(scope) = registration_profile::load(connection, request_id)? {
            if scope.request_id() != request_id
                || scope.workspace_id() != workspace_id
                || scope.session_id() != session_id
            {
                return Err(OperationsError::Corrupt(
                    "operational actor registration Profile scope",
                ));
            }
            return resolve_profile_actor(
                connection,
                workspace_id,
                participant_id,
                session_id,
                Some(scope.profile_id()),
            );
        }
        let native_participant_id = crate::native_session_participation_id(
            workspace_id.into_uuid(),
            session_id.into_uuid(),
        );
        if participant_id == native_participant_id
            && progress.request().transport() == AgentTransportKind::StreamableHttp
        {
            return resolve_native_actor(connection, workspace_id, participant_id, session_id);
        }
    }

    resolve_profile_actor(connection, workspace_id, participant_id, session_id, None)
}

fn resolve_profile_actor(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
    session_id: AgentSessionId,
    expected_profile_id: Option<AgentProfileId>,
) -> Result<ResolvedOperationalActor, OperationsError> {
    let expected_profile_bytes = expected_profile_id.map(|id| id.into_uuid().into_bytes().to_vec());
    let row = connection
        .query_row(
            "SELECT substr(m.participation_id,1,17),substr(m.agent_profile_id,1,17),substr(l.display_name,1,129) FROM ops_workspace_participation_mappings AS m JOIN ops_workspace_participations AS p ON p.workspace_id=m.workspace_id AND p.participation_id=m.participation_id AND p.agent_profile_id=m.agent_profile_id JOIN ops_logical_members AS l ON l.workspace_id=m.workspace_id AND l.participant_id=m.participant_id WHERE m.workspace_id=?1 AND m.participant_id=?2 AND l.session_id=?3 AND (?4 IS NULL OR m.agent_profile_id=?4)",
            params![
                workspace_id.into_uuid().as_bytes(),
                participant_id.as_bytes(),
                session_id.into_uuid().as_bytes(),
                expected_profile_bytes,
            ],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?
        .ok_or(OperationsError::Conflict(
            "operational actor has no current logical participation",
        ))?;
    validate_display_name(&row.2)?;
    let profile_id = Uuid::from_bytes(wire::id(row.1)?);
    if expected_profile_id.is_some_and(|expected| expected.into_uuid() != profile_id) {
        return Err(OperationsError::Corrupt(
            "operational actor registration Profile mismatch",
        ));
    }
    Ok(ResolvedOperationalActor::ProfileParticipant {
        participation_id: WorkspaceParticipationId::from_uuid(Uuid::from_bytes(wire::id(row.0)?))?,
        profile_id,
        participant_id,
        session_id,
        display_name: row.2,
    })
}

fn resolve_native_actor(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
    session_id: AgentSessionId,
) -> Result<ResolvedOperationalActor, OperationsError> {
    let display_name = connection
        .query_row(
            "SELECT substr(display_name,1,129) FROM ops_logical_members WHERE workspace_id=?1 AND participant_id=?2 AND session_id=?3",
            params![
                workspace_id.into_uuid().as_bytes(),
                participant_id.as_bytes(),
                session_id.into_uuid().as_bytes(),
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or(OperationsError::Conflict(
            "native operational actor has no current logical Session",
        ))?;
    validate_display_name(&display_name)?;
    Ok(ResolvedOperationalActor::NativeParticipant {
        participant_id,
        session_id,
        display_name,
    })
}

fn validate_display_name(value: &str) -> Result<(), OperationsError> {
    if value.is_empty() || value.len() > 128 {
        return Err(OperationsError::Corrupt("operational actor display name"));
    }
    Ok(())
}
