//! Owned member metadata and separate immutable imported effect references.
use crate::{
    OperationsError,
    wire::{FocusWire, MemberWire, id},
};
use forge_domain::{AgentInstanceId, RegisteredAgentInstance, WorkspaceId};
use rusqlite::{Connection, OptionalExtension, Transaction, params};

const SCOPES: &str = "CREATE TABLE ops_scopes (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), activity_id BLOB NOT NULL CHECK(length(activity_id)=16), PRIMARY KEY(workspace_id,activity_id))";
const MEMBERS: &str = "CREATE TABLE ops_members (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), agent_id BLOB NOT NULL CHECK(length(agent_id)=16), activity_id BLOB NOT NULL CHECK(length(activity_id)=16), session_id BLOB CHECK(session_id IS NULL OR length(session_id)=16), display_name TEXT NOT NULL CHECK(length(CAST(display_name AS BLOB)) BETWEEN 1 AND 128), declared_role TEXT NOT NULL CHECK(length(CAST(declared_role AS BLOB)) BETWEEN 1 AND 128), transport_kind INTEGER NOT NULL CHECK(transport_kind IN(0,1,2,3)), principal_kind INTEGER NOT NULL CHECK(principal_kind IN(0,1,2)), principal_id BLOB CHECK(principal_id IS NULL OR length(principal_id)=16), attachment_state INTEGER NOT NULL CHECK(attachment_state IN(0,1)), binding_revision INTEGER NOT NULL CHECK(binding_revision>=1), created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0), updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms>=created_at_ms), PRIMARY KEY(workspace_id,agent_id), UNIQUE(workspace_id,agent_id,activity_id), FOREIGN KEY(workspace_id,activity_id) REFERENCES ops_scopes(workspace_id,activity_id), CHECK((principal_kind=0 AND principal_id IS NULL) OR (principal_kind IN(1,2) AND principal_id IS NOT NULL)))";
const SESSION_INDEX: &str = "CREATE UNIQUE INDEX ops_members_scope_session_idx ON ops_members(workspace_id,activity_id,session_id) WHERE session_id IS NOT NULL";
const REFERENCES: &str = "CREATE TABLE ops_import_effect_references (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), agent_id BLOB NOT NULL CHECK(length(agent_id)=16), focus_snapshot BLOB NOT NULL CHECK(length(focus_snapshot) BETWEEN 1 AND 2048), PRIMARY KEY(workspace_id,agent_id), FOREIGN KEY(workspace_id,agent_id) REFERENCES ops_members(workspace_id,agent_id))";
const COORDINATORS: &str = "CREATE TABLE ops_activity_coordinators (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), activity_id BLOB NOT NULL CHECK(length(activity_id)=16), agent_id BLOB NOT NULL CHECK(length(agent_id)=16), revision INTEGER NOT NULL CHECK(revision>=1), updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms>=0), PRIMARY KEY(workspace_id,activity_id), FOREIGN KEY(workspace_id,activity_id) REFERENCES ops_scopes(workspace_id,activity_id), FOREIGN KEY(workspace_id,agent_id) REFERENCES ops_members(workspace_id,agent_id))";

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("table", "ops_scopes", SCOPES),
        ("table", "ops_members", MEMBERS),
        ("index", "ops_members_scope_session_idx", SESSION_INDEX),
        ("table", "ops_import_effect_references", REFERENCES),
        ("table", "ops_activity_coordinators", COORDINATORS),
    ]
}
pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in schema_objects() {
        connection.execute_batch(sql)?;
    }
    Ok(())
}
pub(crate) fn scope(
    tx: &Transaction<'_>,
    workspace: [u8; 16],
    activity: [u8; 16],
) -> Result<(), OperationsError> {
    tx.execute(
        "INSERT INTO ops_scopes(workspace_id,activity_id) VALUES(?1,?2) ON CONFLICT DO NOTHING",
        params![workspace.as_slice(), activity.as_slice()],
    )?;
    Ok(())
}

pub(crate) fn put(
    tx: &Transaction<'_>,
    agent: &RegisteredAgentInstance,
) -> Result<(), OperationsError> {
    if let Some(existing) = get(tx, agent.workspace_id(), agent.id())? {
        return if existing == *agent {
            Ok(())
        } else {
            Err(OperationsError::Conflict("member payload"))
        };
    }
    let v = MemberWire::from_agent(agent);
    scope(tx, v.workspace, v.activity)?;
    tx.execute("INSERT INTO ops_members(workspace_id,agent_id,activity_id,session_id,display_name,declared_role,transport_kind,principal_kind,principal_id,attachment_state,binding_revision,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",params![v.workspace.as_slice(),v.agent.as_slice(),v.activity.as_slice(),v.session.as_ref().map(<[u8;16]>::as_slice),v.display_name,v.declared_role,v.transport,v.principal_kind,v.principal_id.as_ref().map(<[u8;16]>::as_slice),v.attachment,v.binding_revision,v.created_at,v.updated_at])?;
    let focus = serde_json::to_vec(&v.source_focus)?;
    if focus.len() > 2048 {
        return Err(OperationsError::Bounds("imported focus"));
    }
    tx.execute("INSERT INTO ops_import_effect_references(workspace_id,agent_id,focus_snapshot) VALUES(?1,?2,?3)",params![v.workspace.as_slice(),v.agent.as_slice(),focus])?;
    if get(tx, agent.workspace_id(), agent.id())?.as_ref() != Some(agent) {
        return Err(OperationsError::Corrupt("member write readback"));
    }
    Ok(())
}

pub(crate) fn get(
    connection: &Connection,
    workspace: WorkspaceId,
    agent: AgentInstanceId,
) -> Result<Option<RegisteredAgentInstance>, OperationsError> {
    let raw=connection.query_row("SELECT substr(m.agent_id,1,17),substr(m.workspace_id,1,17),substr(m.activity_id,1,17),substr(m.session_id,1,17),substr(CAST(m.display_name AS BLOB),1,129),substr(CAST(m.declared_role AS BLOB),1,129),m.transport_kind,m.principal_kind,substr(m.principal_id,1,17),m.attachment_state,m.binding_revision,m.created_at_ms,m.updated_at_ms,substr(r.focus_snapshot,1,2049) FROM ops_members m JOIN ops_import_effect_references r ON r.workspace_id=m.workspace_id AND r.agent_id=m.agent_id WHERE m.workspace_id=?1 AND m.agent_id=?2",params![workspace.into_uuid().as_bytes(),agent.into_uuid().as_bytes()],|row|Ok(RawMember{agent:row.get(0)?,workspace:row.get(1)?,activity:row.get(2)?,session:row.get(3)?,display:row.get(4)?,role:row.get(5)?,transport:row.get(6)?,principal_kind:row.get(7)?,principal_id:row.get(8)?,attachment:row.get(9)?,revision:row.get(10)?,created:row.get(11)?,updated:row.get(12)?,focus:row.get(13)?})).optional()?;
    let Some(v) = raw else {
        let present: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_members WHERE workspace_id=?1 AND agent_id=?2)",
            params![
                workspace.into_uuid().as_bytes(),
                agent.into_uuid().as_bytes()
            ],
            |r| r.get(0),
        )?;
        return if present {
            Err(OperationsError::Corrupt(
                "missing imported effect reference",
            ))
        } else {
            Ok(None)
        };
    };
    if v.focus.len() > 2048 {
        return Err(OperationsError::Corrupt("focus bound"));
    }
    let focus: FocusWire = serde_json::from_slice(&v.focus)?;
    let value = MemberWire {
        agent: id(v.agent)?,
        workspace: id(v.workspace)?,
        activity: id(v.activity)?,
        session: v.session.map(id).transpose()?,
        display_name: String::from_utf8(v.display)
            .map_err(|_| OperationsError::Corrupt("display UTF8"))?,
        declared_role: String::from_utf8(v.role)
            .map_err(|_| OperationsError::Corrupt("role UTF8"))?,
        transport: v.transport,
        principal_kind: v.principal_kind,
        principal_id: v.principal_id.map(id).transpose()?,
        attachment: v.attachment,
        binding_revision: v.revision,
        created_at: v.created,
        updated_at: v.updated,
        source_focus: focus,
    };
    value.agent().map(Some)
}
struct RawMember {
    agent: Vec<u8>,
    workspace: Vec<u8>,
    activity: Vec<u8>,
    session: Option<Vec<u8>>,
    display: Vec<u8>,
    role: Vec<u8>,
    transport: i64,
    principal_kind: i64,
    principal_id: Option<Vec<u8>>,
    attachment: i64,
    revision: i64,
    created: i64,
    updated: i64,
    focus: Vec<u8>,
}

pub(crate) fn put_coordinator(
    tx: &Transaction<'_>,
    value: forge_domain::ActivityCoordinatorBinding,
) -> Result<(), OperationsError> {
    if let Some(existing) = get_coordinator(tx, value.workspace_id(), value.activity_id())? {
        return if existing == value {
            Ok(())
        } else {
            Err(OperationsError::Conflict("activity planning owner"))
        };
    }
    scope(
        tx,
        value.workspace_id().into_uuid().into_bytes(),
        value.activity_id().into_uuid().into_bytes(),
    )?;
    tx.execute("INSERT INTO ops_activity_coordinators(workspace_id,activity_id,agent_id,revision,updated_at_ms) VALUES(?1,?2,?3,?4,?5)",params![value.workspace_id().into_uuid().as_bytes(),value.activity_id().into_uuid().as_bytes(),value.agent_id().into_uuid().as_bytes(),value.revision().get(),value.updated_at().get()])?;
    Ok(())
}
pub(crate) fn get_coordinator(
    connection: &Connection,
    workspace: WorkspaceId,
    activity: forge_domain::ActivityId,
) -> Result<Option<forge_domain::ActivityCoordinatorBinding>, OperationsError> {
    let value=connection.query_row("SELECT substr(agent_id,1,17),revision,updated_at_ms FROM ops_activity_coordinators WHERE workspace_id=?1 AND activity_id=?2",params![workspace.into_uuid().as_bytes(),activity.into_uuid().as_bytes()],|row|Ok((row.get::<_,Vec<u8>>(0)?,row.get::<_,i64>(1)?,row.get::<_,i64>(2)?))).optional()?;
    value
        .map(|(agent, revision, updated)| {
            Ok(forge_domain::ActivityCoordinatorBinding::new(
                workspace,
                activity,
                AgentInstanceId::from_uuid(uuid::Uuid::from_bytes(id(agent)?)),
                forge_domain::ActivityCoordinatorRevision::new(revision)
                    .map_err(|_| OperationsError::Corrupt("planning revision"))?,
                forge_domain::TimestampMillis::new(updated)
                    .map_err(|_| OperationsError::Corrupt("planning timestamp"))?,
            ))
        })
        .transpose()
}
