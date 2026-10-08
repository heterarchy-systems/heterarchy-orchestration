//! Canonical current Orchestration projection of Forge-owned Agent effect bindings.
//!
//! Registration activation and legacy adoption have different immutable source
//! witnesses, but current operational reads must not duplicate that distinction
//! in presence/inbox/coordination queries. This table is written only after one
//! exact Forge witness is durably accepted by Orchestration.

use crate::{OperationsCatalog, OperationsError, wire};
use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, WorkspaceId};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use uuid::Uuid;

const CURRENT_EFFECT_BINDINGS_SQL: &str = "CREATE TABLE ops_current_effect_bindings (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), participant_id BLOB NOT NULL CHECK(length(participant_id)=16 AND participant_id != zeroblob(16)), activity_id BLOB NOT NULL CHECK(length(activity_id)=16), session_id BLOB NOT NULL CHECK(length(session_id)=16 AND session_id != zeroblob(16)), agent_id BLOB NOT NULL CHECK(length(agent_id)=16 AND agent_id != zeroblob(16)), source_kind INTEGER NOT NULL CHECK(source_kind IN(0,1)), source_request_id BLOB NOT NULL CHECK(length(source_request_id)=16 AND source_request_id != zeroblob(16)), PRIMARY KEY(workspace_id,activity_id,agent_id), UNIQUE(workspace_id,agent_id), UNIQUE(workspace_id,activity_id,session_id), UNIQUE(workspace_id,participant_id,activity_id), FOREIGN KEY(workspace_id,participant_id) REFERENCES ops_logical_members(workspace_id,participant_id))";
/// Exact mapping refuses partial identity projections beyond this many current
/// Activity bindings for one Workspace logical Session.
pub const MAX_LOGICAL_SESSION_EFFECT_BINDINGS: usize = 64;

/// One current Forge effect binding observed for a Workspace logical Session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalSessionEffectBinding {
    activity_id: ActivityId,
    agent_id: AgentInstanceId,
}

impl LogicalSessionEffectBinding {
    #[must_use]
    pub const fn activity_id(self) -> ActivityId {
        self.activity_id
    }

    #[must_use]
    pub const fn agent_id(self) -> AgentInstanceId {
        self.agent_id
    }
}

/// Read-only exact projection from one existing logical Session into the
/// current Workspace participant/effect identity graph.
///
/// This projection is migration evidence only. It does not create a Profile,
/// participant, role, leadership record, vote, or Forge effect permission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalSessionIdentityProjection {
    workspace_id: WorkspaceId,
    participant_id: Uuid,
    session_id: AgentSessionId,
    membership_revision: u64,
    effect_bindings: Vec<LogicalSessionEffectBinding>,
}

impl LogicalSessionIdentityProjection {
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }

    pub const fn session_id(&self) -> AgentSessionId {
        self.session_id
    }

    #[must_use]
    pub const fn membership_revision(&self) -> u64 {
        self.membership_revision
    }

    #[must_use]
    pub fn effect_bindings(&self) -> &[LogicalSessionEffectBinding] {
        &self.effect_bindings
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EffectBindingSource {
    Registration,
    LegacyAdoption,
}

impl EffectBindingSource {
    const fn encode(self) -> i64 {
        match self {
            Self::Registration => 0,
            Self::LegacyAdoption => 1,
        }
    }

    fn decode(value: i64) -> Result<Self, OperationsError> {
        match value {
            0 => Ok(Self::Registration),
            1 => Ok(Self::LegacyAdoption),
            _ => Err(OperationsError::Corrupt("current effect binding source")),
        }
    }
}

#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CurrentEffectBinding {
    workspace_id: WorkspaceId,
    participant_id: Uuid,
    activity_id: ActivityId,
    session_id: AgentSessionId,
    agent_id: AgentInstanceId,
    source: EffectBindingSource,
    source_request_id: Uuid,
}

impl CurrentEffectBinding {
    pub(crate) const fn participant_id(self) -> Uuid {
        self.participant_id
    }
    pub(crate) const fn activity_id(self) -> ActivityId {
        self.activity_id
    }
}

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![(
        "table",
        "ops_current_effect_bindings",
        CURRENT_EFFECT_BINDINGS_SQL,
    )]
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    connection.execute_batch(CURRENT_EFFECT_BINDINGS_SQL)?;
    Ok(())
}

/// Backfill only registrations that already have a real Forge activation
/// witness. No readiness is inferred from membership commit alone.
pub(crate) fn migrate_from_v5(connection: &Connection) -> Result<(), OperationsError> {
    connection.execute(
        "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) SELECT b.workspace_id,b.participant_id,b.activity_id,b.session_id,b.agent_id,0,b.request_id FROM ops_logical_bindings b JOIN ops_binding_activations a ON a.request_id=b.request_id",
        [],
    )?;
    Ok(())
}

pub(crate) fn record_registration(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
    activity_id: ActivityId,
    session_id: AgentSessionId,
    agent_id: AgentInstanceId,
    request_id: Uuid,
) -> Result<CurrentEffectBinding, OperationsError> {
    record(
        tx,
        CurrentEffectBinding {
            workspace_id,
            participant_id,
            activity_id,
            session_id,
            agent_id,
            source: EffectBindingSource::Registration,
            source_request_id: request_id,
        },
    )
}

fn record(
    tx: &Transaction<'_>,
    value: CurrentEffectBinding,
) -> Result<CurrentEffectBinding, OperationsError> {
    if let Some(existing) = by_agent(tx, value.workspace_id, value.agent_id)? {
        return if existing == value {
            Ok(existing)
        } else {
            Err(OperationsError::Conflict("current effect binding"))
        };
    }
    tx.execute(
        "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,?6,?7)",
        params![
            value.workspace_id.into_uuid().as_bytes(),
            value.participant_id.as_bytes(),
            value.activity_id.into_uuid().as_bytes(),
            value.session_id.into_uuid().as_bytes(),
            value.agent_id.into_uuid().as_bytes(),
            value.source.encode(),
            value.source_request_id.as_bytes(),
        ],
    )?;
    by_agent(tx, value.workspace_id, value.agent_id)?
        .filter(|stored| *stored == value)
        .ok_or(OperationsError::Corrupt("current effect binding readback"))
}

pub(crate) fn by_agent(
    connection: &Connection,
    workspace_id: WorkspaceId,
    agent_id: AgentInstanceId,
) -> Result<Option<CurrentEffectBinding>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(workspace_id,1,17),substr(participant_id,1,17),substr(activity_id,1,17),substr(session_id,1,17),substr(agent_id,1,17),source_kind,substr(source_request_id,1,17) FROM ops_current_effect_bindings WHERE workspace_id=?1 AND agent_id=?2 LIMIT 1",
            params![workspace_id.into_uuid().as_bytes(), agent_id.into_uuid().as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, Vec<u8>>(6)?,
                ))
            },
        )
        .optional()?;
    raw.map(decode).transpose()
}

pub(crate) fn exact(
    connection: &Connection,
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
) -> Result<Option<CurrentEffectBinding>, OperationsError> {
    let value = by_agent(connection, workspace_id, agent_id)?;
    Ok(value
        .filter(|binding| binding.activity_id == activity_id && binding.session_id == session_id))
}

pub(crate) fn retire_exact(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
    activity_id: ActivityId,
    session_id: AgentSessionId,
    agent_id: AgentInstanceId,
) -> Result<(), OperationsError> {
    let removed = tx.execute(
        "DELETE FROM ops_current_effect_bindings WHERE workspace_id=?1 AND participant_id=?2 AND activity_id=?3 AND session_id=?4 AND agent_id=?5",
        params![
            workspace_id.into_uuid().as_bytes(),
            participant_id.as_bytes(),
            activity_id.into_uuid().as_bytes(),
            session_id.into_uuid().as_bytes(),
            agent_id.into_uuid().as_bytes(),
        ],
    )?;
    if removed != 1 {
        return Err(OperationsError::Corrupt(
            "current effect binding retirement",
        ));
    }
    Ok(())
}

impl OperationsCatalog {
    /// Resolve a Ready logical participant for one exact Forge Agent effect
    /// binding without consulting legacy operational Agent state.
    ///
    /// # Errors
    /// Returns storage failures or corrupt current effect-binding state.
    pub fn ready_participant_for_agent(
        &self,
        workspace_id: WorkspaceId,
        activity_id: ActivityId,
        agent_id: AgentInstanceId,
    ) -> Result<Option<Uuid>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let value = by_agent(&transaction, workspace_id, agent_id)?
            .filter(|binding| binding.activity_id() == activity_id)
            .map(CurrentEffectBinding::participant_id);
        crate::catalog::commit_read(transaction, self.destination)?;
        Ok(value)
    }

    /// Project one exact app-global logical Session into the current
    /// Workspace participant and Forge effect-binding identities.
    ///
    /// The logical member may exist with zero current effect bindings. More
    /// than [`MAX_LOGICAL_SESSION_EFFECT_BINDINGS`] is rejected rather than
    /// returning a truncated identity graph.
    ///
    /// # Errors
    /// Returns storage/corruption failures or a bound error instead of a
    /// partial projection.
    pub fn logical_identity_for_session(
        &self,
        workspace_id: WorkspaceId,
        session_id: AgentSessionId,
    ) -> Result<Option<LogicalSessionIdentityProjection>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let participant: Option<(Vec<u8>, i64)> = transaction
            .query_row(
                "SELECT substr(participant_id,1,17),membership_revision FROM ops_logical_members WHERE workspace_id=?1 AND session_id=?2 LIMIT 1",
                params![
                    workspace_id.into_uuid().as_bytes(),
                    session_id.into_uuid().as_bytes()
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((participant_id, membership_revision)) = participant else {
            crate::catalog::commit_read(transaction, self.destination)?;
            return Ok(None);
        };
        let participant_id = Uuid::from_bytes(wire::id(participant_id)?);
        let projection = logical_identity_for_member_in_transaction(
            &transaction,
            workspace_id,
            participant_id,
            session_id,
            membership_revision,
        )?;
        crate::catalog::commit_read(transaction, self.destination)?;
        Ok(Some(projection))
    }

    /// Resolve a logical participant's current Session and its current Forge
    /// effect bindings from one read snapshot. Participation mapping rows keep
    /// their immutable origin Session and are not used as current authority.
    ///
    /// # Errors
    /// Returns storage/corruption failures or a bound error instead of a
    /// partial projection.
    pub fn logical_identity_for_participant(
        &self,
        workspace_id: WorkspaceId,
        participant_id: Uuid,
    ) -> Result<Option<LogicalSessionIdentityProjection>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let current: Option<(Vec<u8>, i64)> = transaction
            .query_row(
                "SELECT substr(session_id,1,17),membership_revision FROM ops_logical_members WHERE workspace_id=?1 AND participant_id=?2 LIMIT 1",
                params![workspace_id.into_uuid().as_bytes(), participant_id.as_bytes()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((session_id, membership_revision)) = current else {
            crate::catalog::commit_read(transaction, self.destination)?;
            return Ok(None);
        };
        let session_id = AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(session_id)?));
        let projection = logical_identity_for_member_in_transaction(
            &transaction,
            workspace_id,
            participant_id,
            session_id,
            membership_revision,
        )?;
        crate::catalog::commit_read(transaction, self.destination)?;
        Ok(Some(projection))
    }
}

fn logical_identity_for_member_in_transaction(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
    session_id: AgentSessionId,
    membership_revision: i64,
) -> Result<LogicalSessionIdentityProjection, OperationsError> {
    let membership_revision = u64::try_from(membership_revision)
        .map_err(|_| OperationsError::Corrupt("logical member revision"))?;
    if membership_revision == 0 {
        return Err(OperationsError::Corrupt("logical member revision"));
    }
    let limit = i64::try_from(MAX_LOGICAL_SESSION_EFFECT_BINDINGS + 1)
        .map_err(|_| OperationsError::Bounds("logical Session effect-binding limit"))?;
    let mut statement = transaction.prepare(
        "SELECT substr(activity_id,1,17),substr(agent_id,1,17) FROM ops_current_effect_bindings WHERE workspace_id=?1 AND participant_id=?2 AND session_id=?3 ORDER BY activity_id,agent_id LIMIT ?4",
    )?;
    let mut rows = statement.query(params![
        workspace_id.into_uuid().as_bytes(),
        participant_id.as_bytes(),
        session_id.into_uuid().as_bytes(),
        limit,
    ])?;
    let mut effect_bindings = Vec::new();
    while let Some(row) = rows.next()? {
        if effect_bindings.len() >= MAX_LOGICAL_SESSION_EFFECT_BINDINGS {
            return Err(OperationsError::Bounds("logical Session effect bindings"));
        }
        effect_bindings.push(LogicalSessionEffectBinding {
            activity_id: ActivityId::from_uuid(Uuid::from_bytes(wire::id(row.get(0)?)?)),
            agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(row.get(1)?)?)),
        });
    }
    drop(rows);
    drop(statement);
    Ok(LogicalSessionIdentityProjection {
        workspace_id,
        participant_id,
        session_id,
        membership_revision,
        effect_bindings,
    })
}

type RawBinding = (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, i64, Vec<u8>);

fn decode(raw: RawBinding) -> Result<CurrentEffectBinding, OperationsError> {
    Ok(CurrentEffectBinding {
        workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(raw.0)?)),
        participant_id: Uuid::from_bytes(wire::id(raw.1)?),
        activity_id: ActivityId::from_uuid(Uuid::from_bytes(wire::id(raw.2)?)),
        session_id: AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(raw.3)?)),
        agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(raw.4)?)),
        source: EffectBindingSource::decode(raw.5)?,
        source_request_id: Uuid::from_bytes(wire::id(raw.6)?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn logical_session_identity_projection_is_exact_and_read_only() -> Result<(), Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!(
            "orchestration-logical-session-identity-{}",
            Uuid::now_v7()
        ));
        std::fs::create_dir(&root)?;
        let destination = Uuid::from_u128(0xE001);
        let catalog = OperationsCatalog::open(&root.join("catalog.sqlite3"), destination)?;
        let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(0xE002));
        let participant_id = Uuid::from_u128(0xE003);
        let session_id = AgentSessionId::from_uuid(Uuid::from_u128(0xE004));
        catalog.connection.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,'profile mapping member','worker',1,1,1)",
            params![
                workspace_id.into_uuid().as_bytes(),
                participant_id.as_bytes(),
                session_id.into_uuid().as_bytes(),
            ],
        )?;
        let bindings = [
            (
                ActivityId::from_uuid(Uuid::from_u128(0xE005)),
                AgentInstanceId::from_uuid(Uuid::from_u128(0xE006)),
                Uuid::from_u128(0xE007),
            ),
            (
                ActivityId::from_uuid(Uuid::from_u128(0xE008)),
                AgentInstanceId::from_uuid(Uuid::from_u128(0xE009)),
                Uuid::from_u128(0xE00A),
            ),
        ];
        for (activity_id, agent_id, request_id) in bindings {
            catalog.connection.execute(
                "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
                params![
                    workspace_id.into_uuid().as_bytes(),
                    participant_id.as_bytes(),
                    activity_id.into_uuid().as_bytes(),
                    session_id.into_uuid().as_bytes(),
                    agent_id.into_uuid().as_bytes(),
                    request_id.as_bytes(),
                ],
            )?;
        }
        let before_changes = catalog.connection.total_changes();
        let projection = catalog
            .logical_identity_for_session(workspace_id, session_id)?
            .ok_or("logical Session projection missing")?;
        assert_eq!(projection.workspace_id(), workspace_id);
        assert_eq!(projection.participant_id(), participant_id);
        assert_eq!(projection.session_id(), session_id);
        assert_eq!(projection.effect_bindings().len(), 2);
        assert_eq!(catalog.connection.total_changes(), before_changes);
        assert_eq!(
            catalog.logical_identity_for_session(
                workspace_id,
                AgentSessionId::from_uuid(Uuid::from_u128(0xE00B))
            )?,
            None
        );
        drop(catalog);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
