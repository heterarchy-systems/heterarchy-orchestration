//! Resolve public operational authors without confusing roles with identities.
//!
//! The native host may construct `LocalOperator`; external MCP adapters must
//! construct `Participant` only after validating the issued Session principal.
//! Every participant is revalidated against its current Workspace effect binding.

use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, WorkspaceId};
use rusqlite::{Connection, OptionalExtension, params};
use uuid::Uuid;

use crate::{OperationsError, WorkspaceParticipationId, effect_binding, wire};

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

/// Immutable attribution snapshot resolved inside the mutation transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedOperationalActor {
    pub(crate) participation_id: Option<WorkspaceParticipationId>,
    pub(crate) profile_id: Option<Uuid>,
    pub(crate) participant_id: Option<Uuid>,
    pub(crate) session_id: Option<AgentSessionId>,
    pub(crate) display_name: String,
}

impl ResolvedOperationalActor {
    pub(crate) fn key(&self) -> String {
        self.participation_id
            .map_or_else(|| "operator".to_owned(), |id| format!("participant:{id}"))
    }

    pub(crate) const fn is_operator(&self) -> bool {
        self.participation_id.is_none()
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
        return Ok(ResolvedOperationalActor {
            participation_id: None,
            profile_id: None,
            participant_id: None,
            session_id: None,
            display_name: "Local operator".to_owned(),
        });
    };
    let binding =
        effect_binding::exact(connection, workspace_id, activity_id, agent_id, session_id)?.ok_or(
            OperationsError::Conflict("operational actor is not a Ready Workspace participant"),
        )?;
    let participant_id = binding.participant_id();
    // The participation mapping is immutable origin evidence. Session rebinds
    // update the logical member and current effect binding, not that origin.
    let row = connection.query_row(
        "SELECT substr(m.participation_id,1,17),substr(m.agent_profile_id,1,17),substr(l.display_name,1,129) FROM ops_workspace_participation_mappings AS m JOIN ops_workspace_participations AS p ON p.workspace_id=m.workspace_id AND p.participation_id=m.participation_id AND p.agent_profile_id=m.agent_profile_id JOIN ops_logical_members AS l ON l.workspace_id=m.workspace_id AND l.participant_id=m.participant_id WHERE m.workspace_id=?1 AND m.participant_id=?2 AND l.session_id=?3",
        params![workspace_id.into_uuid().as_bytes(), participant_id.as_bytes(), session_id.into_uuid().as_bytes()],
        |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?, row.get::<_, String>(2)?)),
    ).optional()?.ok_or(OperationsError::Conflict("operational actor has no current logical participation"))?;
    if row.2.is_empty() || row.2.len() > 128 {
        return Err(OperationsError::Corrupt("operational actor display name"));
    }
    Ok(ResolvedOperationalActor {
        participation_id: Some(WorkspaceParticipationId::from_uuid(Uuid::from_bytes(
            wire::id(row.0)?,
        ))?),
        profile_id: Some(Uuid::from_bytes(wire::id(row.1)?)),
        participant_id: Some(participant_id),
        session_id: Some(session_id),
        display_name: row.2,
    })
}
