//! Steady-state logical presence owned by Orchestration.
//!
//! Imported `ops_presence` rows are historical Forge-Agent observations. This
//! module keeps the live presence authority separate and resolves every touch
//! through an activated logical member plus the exact Activity-local Forge
//! binding that correlated the caller with that logical participant.

use crate::{OperationsCatalog, OperationsError, effect_binding, registration, wire};
use forge_domain::{
    ActivityId, AgentDisplayName, AgentInstanceId, AgentPresenceRevision, AgentSessionId,
    AuthenticatedPrincipal, TimestampMillis, WorkspaceId,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use uuid::Uuid;

const LOGICAL_PRESENCE_SQL: &str = "CREATE TABLE ops_logical_presence (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), participant_id BLOB NOT NULL CHECK(length(participant_id)=16 AND participant_id != zeroblob(16)), session_id BLOB NOT NULL CHECK(length(session_id)=16 AND session_id != zeroblob(16)), last_authenticated_at_ms INTEGER NOT NULL CHECK(last_authenticated_at_ms>=0), presence_revision INTEGER NOT NULL CHECK(presence_revision>=2), PRIMARY KEY(workspace_id,participant_id), UNIQUE(workspace_id,session_id), FOREIGN KEY(workspace_id,participant_id) REFERENCES ops_logical_members(workspace_id,participant_id))";

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![("table", "ops_logical_presence", LOGICAL_PRESENCE_SQL)]
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in schema_objects() {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

/// Current live presence for one Ready logical participant.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalPresenceSnapshot {
    workspace_id: WorkspaceId,
    participant_id: Uuid,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    display_name: AgentDisplayName,
    membership_revision: u64,
    presence_revision: AgentPresenceRevision,
    last_authenticated_at: TimestampMillis,
}

impl LogicalPresenceSnapshot {
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }

    #[must_use]
    pub const fn agent_id(&self) -> AgentInstanceId {
        self.agent_id
    }

    pub const fn session_id(&self) -> AgentSessionId {
        self.session_id
    }

    #[must_use]
    pub const fn display_name(&self) -> &AgentDisplayName {
        &self.display_name
    }

    #[must_use]
    pub const fn membership_revision(&self) -> u64 {
        self.membership_revision
    }

    pub const fn presence_revision(&self) -> AgentPresenceRevision {
        self.presence_revision
    }

    #[must_use]
    pub const fn last_authenticated_at(&self) -> TimestampMillis {
        self.last_authenticated_at
    }
}

/// Exact live-presence subject resolved from one Workspace, Activity, Forge binding, and session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalPresenceTarget {
    workspace: WorkspaceId,
    activity: ActivityId,
    agent: AgentInstanceId,
    session: AgentSessionId,
}

impl LogicalPresenceTarget {
    #[must_use]
    pub const fn new(
        workspace_id: WorkspaceId,
        activity_id: ActivityId,
        agent_id: AgentInstanceId,
        session_id: AgentSessionId,
    ) -> Self {
        Self {
            workspace: workspace_id,
            activity: activity_id,
            agent: agent_id,
            session: session_id,
        }
    }
}

/// One exact CAS update of Orchestration-owned logical presence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalPresenceTouch {
    target: LogicalPresenceTarget,
    principal: AuthenticatedPrincipal,
    expected_revision: AgentPresenceRevision,
    observed_at: TimestampMillis,
}

impl LogicalPresenceTouch {
    #[must_use]
    pub const fn new(
        target: LogicalPresenceTarget,
        principal: AuthenticatedPrincipal,
        expected_revision: AgentPresenceRevision,
        observed_at: TimestampMillis,
    ) -> Self {
        Self {
            target,
            principal,
            expected_revision,
            observed_at,
        }
    }
}

impl OperationsCatalog {
    /// Read current live logical presence through one exact Ready membership binding.
    ///
    /// An absent row remains absent; reading never initializes or advances presence.
    ///
    /// # Errors
    /// Rejects unknown or non-Ready bindings, principal/session mismatches,
    /// corrupt rows, and storage failures.
    pub fn logical_presence(
        &self,
        target: LogicalPresenceTarget,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<LogicalPresenceSnapshot>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let binding = resolve_ready_logical_binding(
            &transaction,
            target.workspace,
            target.activity,
            target.agent,
            target.session,
            principal,
        )?;
        let stored = load_presence(&transaction, target.workspace, binding.participant_id)?;
        let snapshot = match stored {
            None => None,
            Some(stored) => {
                if stored.session_id != target.session {
                    return Err(OperationsError::Conflict("presence revision"));
                }
                Some(LogicalPresenceSnapshot {
                    workspace_id: target.workspace,
                    participant_id: binding.participant_id,
                    agent_id: target.agent,
                    session_id: target.session,
                    display_name: binding.display_name,
                    membership_revision: binding.membership_revision,
                    presence_revision: stored.revision,
                    last_authenticated_at: stored.last_authenticated_at,
                })
            }
        };
        crate::catalog::commit_read(transaction, self.destination)?;
        Ok(snapshot)
    }

    /// Touch live logical presence through one exact Ready membership binding.
    ///
    /// Absence represents the implicit initial revision 1. The first accepted
    /// touch therefore requires revision 1 and persists revision 2. Later
    /// writes use exact CAS and never modify imported Forge presence rows.
    ///
    /// # Errors
    /// Rejects unknown or non-Ready bindings, principal/session mismatches,
    /// stale revisions, corrupt rows, and uncertain commits.
    pub fn touch_logical_presence(
        &mut self,
        command: LogicalPresenceTouch,
    ) -> Result<LogicalPresenceSnapshot, OperationsError> {
        let LogicalPresenceTouch {
            target,
            principal,
            expected_revision,
            observed_at,
        } = command;
        let LogicalPresenceTarget {
            workspace: workspace_id,
            activity: activity_id,
            agent: agent_id,
            session: session_id,
        } = target;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let binding = resolve_ready_logical_binding(
            &transaction,
            workspace_id,
            activity_id,
            agent_id,
            session_id,
            principal,
        )?;

        let next_revision = expected_revision
            .checked_next()
            .map_err(|_| OperationsError::Bounds("presence revision"))?;
        if let Some(current) = load_presence(&transaction, workspace_id, binding.participant_id)? {
            if current.session_id != session_id || current.revision != expected_revision {
                return Err(OperationsError::Conflict("presence revision"));
            }
            let changed = transaction.execute(
                "UPDATE ops_logical_presence SET last_authenticated_at_ms=?1,presence_revision=?2 WHERE workspace_id=?3 AND participant_id=?4 AND session_id=?5 AND presence_revision=?6",
                params![
                    observed_at.get(),
                    next_revision.get(),
                    workspace_id.into_uuid().as_bytes(),
                    binding.participant_id.as_bytes(),
                    session_id.into_uuid().as_bytes(),
                    expected_revision.get(),
                ],
            )?;
            if changed != 1 {
                return Err(OperationsError::Conflict("presence revision"));
            }
        } else {
            if expected_revision != AgentPresenceRevision::INITIAL {
                return Err(OperationsError::Conflict("presence revision"));
            }
            transaction.execute(
                "INSERT INTO ops_logical_presence(workspace_id,participant_id,session_id,last_authenticated_at_ms,presence_revision) VALUES(?1,?2,?3,?4,?5)",
                params![
                    workspace_id.into_uuid().as_bytes(),
                    binding.participant_id.as_bytes(),
                    session_id.into_uuid().as_bytes(),
                    observed_at.get(),
                    next_revision.get(),
                ],
            )?;
        }

        let snapshot = LogicalPresenceSnapshot {
            workspace_id,
            participant_id: binding.participant_id,
            agent_id,
            session_id,
            display_name: binding.display_name,
            membership_revision: binding.membership_revision,
            presence_revision: next_revision,
            last_authenticated_at: observed_at,
        };
        transaction
            .commit()
            .map_err(|source| OperationsError::PresenceCommitUnconfirmed {
                workspace_id: workspace_id.into_uuid(),
                participant_id: binding.participant_id,
                source,
            })?;
        Ok(snapshot)
    }
}

#[derive(Debug)]
pub(crate) struct ReadyLogicalBinding {
    pub(crate) participant_id: Uuid,
    pub(crate) display_name: AgentDisplayName,
    pub(crate) membership_revision: u64,
}

pub(crate) fn resolve_ready_logical_binding(
    connection: &Connection,
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    principal: AuthenticatedPrincipal,
) -> Result<ReadyLogicalBinding, OperationsError> {
    let binding = resolve_ready_logical_target_binding(
        connection,
        workspace_id,
        activity_id,
        agent_id,
        session_id,
    )?;
    let raw = connection
        .query_row(
            "SELECT principal_kind,substr(principal_id,1,17) FROM ops_logical_members WHERE workspace_id=?1 AND participant_id=?2 AND session_id=?3 LIMIT 1",
            params![
                workspace_id.into_uuid().as_bytes(),
                binding.participant_id.as_bytes(),
                session_id.into_uuid().as_bytes(),
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<Vec<u8>>>(1)?,
                ))
            },
        )
        .optional()?
        .ok_or(OperationsError::Corrupt("logical binding member missing"))?;
    let stored_principal = registration::decode_principal_fields(raw.0, raw.1.as_deref())?;
    if stored_principal != principal {
        return Err(OperationsError::Conflict("logical binding principal"));
    }
    Ok(binding)
}

/// Resolve the exact Ready logical participant for one current Forge effect
/// binding without a caller-principal check. The departing handoff target is
/// resolved this way because the Forge reconciliation receipt is the
/// authority witness for the target's own principal.
pub(crate) fn resolve_ready_logical_target_binding(
    connection: &Connection,
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
) -> Result<ReadyLogicalBinding, OperationsError> {
    let binding =
        effect_binding::exact(connection, workspace_id, activity_id, agent_id, session_id)?
            .ok_or(OperationsError::Conflict("logical member not ready"))?;
    let raw = connection
        .query_row(
            "SELECT substr(CAST(display_name AS BLOB),1,129),membership_revision FROM ops_logical_members WHERE workspace_id=?1 AND participant_id=?2 AND session_id=?3 LIMIT 1",
            params![
                workspace_id.into_uuid().as_bytes(),
                binding.participant_id().as_bytes(),
                session_id.into_uuid().as_bytes(),
            ],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                ))
            },
        )
        .optional()?
        .ok_or(OperationsError::Corrupt("logical binding member missing"))?;
    let display_name =
        String::from_utf8(raw.0).map_err(|_| OperationsError::Corrupt("logical member display"))?;
    Ok(ReadyLogicalBinding {
        participant_id: binding.participant_id(),
        display_name: AgentDisplayName::new(display_name)
            .map_err(|_| OperationsError::Corrupt("logical member display"))?,
        membership_revision: u64::try_from(raw.1)
            .map_err(|_| OperationsError::Corrupt("logical membership revision"))?,
    })
}

#[derive(Debug, Clone, Copy)]
struct StoredPresence {
    session_id: AgentSessionId,
    revision: AgentPresenceRevision,
    last_authenticated_at: TimestampMillis,
}

fn load_presence(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
) -> Result<Option<StoredPresence>, OperationsError> {
    connection
        .query_row(
            "SELECT substr(session_id,1,17),presence_revision,last_authenticated_at_ms FROM ops_logical_presence WHERE workspace_id=?1 AND participant_id=?2 LIMIT 1",
            params![workspace_id.into_uuid().as_bytes(), participant_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional()?
        .map(|(session, revision, last_authenticated_at)| {
            Ok(StoredPresence {
                session_id: AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(session)?)),
                revision: AgentPresenceRevision::new(revision)
                    .map_err(|_| OperationsError::Corrupt("logical presence revision"))?,
                last_authenticated_at: TimestampMillis::new(last_authenticated_at)
                    .map_err(|_| OperationsError::Corrupt("logical presence timestamp"))?,
            })
        })
        .transpose()
}
