//! Immutable Profile/Session evidence attached before registration effects.

use crate::{OperationsCatalog, OperationsError, catalog, registration};
use forge_domain::{AgentSessionId, AgentSessionRevision, WorkspaceId};
use heterarchy_orchestration_sessions::{
    AgentProfileId, AgentProfileLogicalSessionLink, AgentProfileRevision,
};
use rusqlite::{Connection, OptionalExtension, params};
use uuid::Uuid;

const TABLE: &str = "CREATE TABLE ops_registration_profile_scopes (request_id BLOB PRIMARY KEY NOT NULL REFERENCES ops_registration_requests(request_id) CHECK(length(request_id)=16), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), profile_id BLOB NOT NULL CHECK(length(profile_id)=16 AND profile_id!=zeroblob(16)), link_request_id BLOB NOT NULL CHECK(length(link_request_id)=16 AND link_request_id!=zeroblob(16)), profile_revision INTEGER NOT NULL CHECK(profile_revision>0), session_id BLOB NOT NULL CHECK(length(session_id)=16 AND session_id!=zeroblob(16)), session_revision INTEGER NOT NULL CHECK(session_revision>0))";
const INDEX: &str = "CREATE INDEX ops_registration_profile_scopes_profile ON ops_registration_profile_scopes(workspace_id,profile_id,request_id)";

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("table", "ops_registration_profile_scopes", TABLE),
        ("index", "ops_registration_profile_scopes_profile", INDEX),
    ]
}
pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    connection.execute_batch(TABLE)?;
    connection.execute_batch(INDEX)?;
    Ok(())
}

/// The exact host-resolved link used to admit or resume a registration.
/// Absence on an older request is unknown Profile attribution, not proof that
/// the request belongs to no Profile.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegistrationProfileScope {
    request_id: Uuid,
    workspace_id: WorkspaceId,
    profile_id: AgentProfileId,
    link_request_id: Uuid,
    profile_revision: AgentProfileRevision,
    session_id: AgentSessionId,
    session_revision: AgentSessionRevision,
}
impl RegistrationProfileScope {
    #[must_use]
    pub const fn request_id(self) -> Uuid {
        self.request_id
    }
    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        self.workspace_id
    }
    #[must_use]
    pub const fn profile_id(self) -> AgentProfileId {
        self.profile_id
    }
    #[must_use]
    pub const fn profile_link_request_id(self) -> Uuid {
        self.link_request_id
    }
    #[must_use]
    pub const fn profile_revision(self) -> AgentProfileRevision {
        self.profile_revision
    }
    pub const fn session_id(self) -> AgentSessionId {
        self.session_id
    }
    pub const fn session_revision(self) -> AgentSessionRevision {
        self.session_revision
    }
}

impl OperationsCatalog {
    /// Read immutable registration Profile evidence without resolving current mappings.
    ///
    /// # Errors
    /// Rejects corrupt/mismatched evidence and storage failures.
    pub fn registration_profile_scope(
        &self,
        request_id: Uuid,
    ) -> Result<Option<RegistrationProfileScope>, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        let result = load(&tx, request_id)?;
        catalog::commit_read(tx, self.destination)?;
        Ok(result)
    }
}

pub(crate) fn record(
    connection: &Connection,
    workspace_id: WorkspaceId,
    request: &registration::RegistrationRequest,
    link: AgentProfileLogicalSessionLink,
) -> Result<(), OperationsError> {
    if request.binding().session_id() != link.session_id() {
        return Err(OperationsError::Conflict("registration Profile Session"));
    }
    let expected = RegistrationProfileScope {
        request_id: request.binding().request_id(),
        workspace_id,
        profile_id: link.profile_id(),
        link_request_id: link.request_id(),
        profile_revision: link.profile_revision(),
        session_id: link.session_id(),
        session_revision: link.session_revision(),
    };
    if let Some(existing) = load(connection, expected.request_id)? {
        if existing != expected {
            return Err(OperationsError::Conflict("registration Profile replay"));
        }
        return Ok(());
    }
    crate::leadership::work_roster::ensure_profile_admission_open(
        connection,
        workspace_id,
        link.profile_id(),
    )?;
    crate::leadership::work_roster::ensure_session_admission_open(
        connection,
        workspace_id,
        link.session_id(),
    )?;
    connection.execute("INSERT INTO ops_registration_profile_scopes(request_id,workspace_id,profile_id,link_request_id,profile_revision,session_id,session_revision) VALUES(?1,?2,?3,?4,?5,?6,?7)", params![expected.request_id.as_bytes(),workspace_id.into_uuid().as_bytes(),expected.profile_id.into_uuid().as_bytes(),expected.link_request_id.as_bytes(),expected.profile_revision.get(),expected.session_id.into_uuid().as_bytes(),expected.session_revision.get()])?;
    Ok(())
}

struct RawScope {
    request: Vec<u8>,
    workspace: Vec<u8>,
    profile: Vec<u8>,
    link: Vec<u8>,
    profile_revision: i64,
    session: Vec<u8>,
    session_revision: i64,
    request_workspace: Vec<u8>,
    request_session: Vec<u8>,
}
fn id(bytes: Vec<u8>) -> Result<Uuid, OperationsError> {
    Ok(Uuid::from_bytes(crate::wire::id(bytes)?))
}

fn load(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<RegistrationProfileScope>, OperationsError> {
    let raw = connection.query_row("SELECT substr(s.request_id,1,17),substr(s.workspace_id,1,17),substr(s.profile_id,1,17),substr(s.link_request_id,1,17),s.profile_revision,substr(s.session_id,1,17),s.session_revision,substr(r.workspace_id,1,17),substr(r.session_id,1,17) FROM ops_registration_profile_scopes s JOIN ops_registration_requests r ON r.request_id=s.request_id WHERE s.request_id=?1", params![request_id.as_bytes()], |row| Ok(RawScope { request:row.get(0)?, workspace:row.get(1)?, profile:row.get(2)?, link:row.get(3)?, profile_revision:row.get(4)?, session:row.get(5)?, session_revision:row.get(6)?, request_workspace:row.get(7)?, request_session:row.get(8)? })).optional()?;
    raw.map(|raw| {
        if raw.workspace != raw.request_workspace || raw.session != raw.request_session {
            return Err(OperationsError::Corrupt("registration Profile binding"));
        }
        let link_request_id = id(raw.link)?;
        if link_request_id.is_nil() {
            return Err(OperationsError::Corrupt("registration Profile link"));
        }
        Ok(RegistrationProfileScope {
            request_id: id(raw.request)?,
            workspace_id: WorkspaceId::from_uuid(id(raw.workspace)?),
            profile_id: AgentProfileId::from_uuid(id(raw.profile)?)
                .map_err(|_| OperationsError::Corrupt("registration Profile"))?,
            link_request_id,
            profile_revision: AgentProfileRevision::new(raw.profile_revision)
                .map_err(|_| OperationsError::Corrupt("registration Profile revision"))?,
            session_id: AgentSessionId::from_uuid(id(raw.session)?),
            session_revision: AgentSessionRevision::new(raw.session_revision)
                .map_err(|_| OperationsError::Corrupt("registration Session revision"))?,
        })
    })
    .transpose()
}
