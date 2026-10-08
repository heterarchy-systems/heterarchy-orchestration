//! Durable Workspace-scoped Team identity and membership.
//!
//! Teams are an Orchestration product object. They are deliberately separate
//! from Activity planning ownership and Broker Consumer Groups. This module
//! owns only the durable foundation; the catalog/native/MCP layers decide how
//! these bounded operations are exposed.

use std::collections::HashMap;

use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, TimestampMillis, WorkspaceId};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use uuid::Uuid;

use crate::{
    OperationsError, WorkspaceParticipationId,
    operational_actor::{OperationalActor, ResolvedOperationalActor, resolve_actor},
    wire,
};

mod candidates;
pub use candidates::{TeamFormationCandidate, TeamFormationCandidatePage};

pub const MAX_TEAMS_PER_READ: usize = 256;
pub const MAX_TEAM_MEMBERS_PER_READ: usize = 256;
const MAX_TEAM_NAME_BYTES: usize = 128;
const MAX_TEAM_PURPOSE_BYTES: usize = 2_048;
const MAX_TEAM_ROLE_BYTES: usize = 256;
pub const MIN_FORMING_TEAM_MEMBERS: usize = 2;
pub const MAX_INITIAL_TEAM_MEMBERS: usize = 64;

const TEAMS_SQL_V36: &str = "CREATE TABLE ops_teams (team_id BLOB PRIMARY KEY NOT NULL CHECK(length(team_id)=16 AND team_id != zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), name TEXT NOT NULL CHECK(length(CAST(name AS BLOB)) BETWEEN 1 AND 128), purpose TEXT NOT NULL CHECK(length(CAST(purpose AS BLOB)) BETWEEN 0 AND 2048), state TEXT NOT NULL CHECK(state IN('ACTIVE','RETIRED')), revision INTEGER NOT NULL CHECK(revision>=1), created_by_key TEXT NOT NULL CHECK(length(CAST(created_by_key AS BLOB)) BETWEEN 1 AND 256), created_by_display_name TEXT NOT NULL CHECK(length(CAST(created_by_display_name AS BLOB)) BETWEEN 1 AND 128), created_by_participation_id BLOB CHECK(created_by_participation_id IS NULL OR length(created_by_participation_id)=16), created_by_profile_id BLOB CHECK(created_by_profile_id IS NULL OR length(created_by_profile_id)=16), created_by_participant_id BLOB CHECK(created_by_participant_id IS NULL OR length(created_by_participant_id)=16), created_by_session_id BLOB CHECK(created_by_session_id IS NULL OR length(created_by_session_id)=16), created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0), updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms>=created_at_ms), UNIQUE(workspace_id,team_id))";
const TEAM_REQUESTS_SQL_V36: &str = "CREATE TABLE ops_team_requests (request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), operation TEXT NOT NULL CHECK(operation IN('CREATE','JOIN','LEAVE','RETIRE')), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id != zeroblob(16)), actor_key TEXT NOT NULL CHECK(length(CAST(actor_key AS BLOB)) BETWEEN 1 AND 256), actor_display_name TEXT NOT NULL CHECK(length(CAST(actor_display_name AS BLOB)) BETWEEN 1 AND 128), actor_participation_id BLOB CHECK(actor_participation_id IS NULL OR length(actor_participation_id)=16), actor_profile_id BLOB CHECK(actor_profile_id IS NULL OR length(actor_profile_id)=16), actor_participant_id BLOB CHECK(actor_participant_id IS NULL OR length(actor_participant_id)=16), actor_session_id BLOB CHECK(actor_session_id IS NULL OR length(actor_session_id)=16), name TEXT CHECK(name IS NULL OR length(CAST(name AS BLOB)) BETWEEN 1 AND 128), purpose TEXT CHECK(purpose IS NULL OR length(CAST(purpose AS BLOB)) BETWEEN 0 AND 2048), expected_team_revision INTEGER CHECK(expected_team_revision IS NULL OR expected_team_revision>=1), result_team_state TEXT NOT NULL CHECK(result_team_state IN('ACTIVE','RETIRED')), result_team_revision INTEGER NOT NULL CHECK(result_team_revision>=1), result_team_updated_at_ms INTEGER NOT NULL CHECK(result_team_updated_at_ms>=0), result_membership_state TEXT CHECK(result_membership_state IS NULL OR result_membership_state IN('JOINED','LEFT')), result_membership_participation_id BLOB CHECK(result_membership_participation_id IS NULL OR length(result_membership_participation_id)=16), result_membership_revision INTEGER CHECK(result_membership_revision IS NULL OR result_membership_revision>=1), result_membership_joined_at_ms INTEGER CHECK(result_membership_joined_at_ms IS NULL OR result_membership_joined_at_ms>=0), result_membership_left_at_ms INTEGER CHECK(result_membership_left_at_ms IS NULL OR result_membership_left_at_ms>=0), created INTEGER NOT NULL CHECK(created IN(0,1)), observed_at_ms INTEGER NOT NULL CHECK(observed_at_ms>=0))";
const TEAM_MEMBERS_SQL_V36: &str = "CREATE TABLE ops_team_members (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id != zeroblob(16)), participation_id BLOB NOT NULL CHECK(length(participation_id)=16 AND participation_id != zeroblob(16)), state TEXT NOT NULL CHECK(state IN('JOINED','LEFT')), revision INTEGER NOT NULL CHECK(revision>=1), joined_at_ms INTEGER NOT NULL CHECK(joined_at_ms>=0), left_at_ms INTEGER CHECK(left_at_ms IS NULL OR left_at_ms>=joined_at_ms), PRIMARY KEY(workspace_id,team_id,participation_id), FOREIGN KEY(workspace_id,team_id) REFERENCES ops_teams(workspace_id,team_id), FOREIGN KEY(workspace_id,participation_id) REFERENCES ops_workspace_participations(workspace_id,participation_id))";
const TEAM_MEMBER_EVENTS_SQL_V36: &str = "CREATE TABLE ops_team_member_events (request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id != zeroblob(16)), participation_id BLOB NOT NULL CHECK(length(participation_id)=16 AND participation_id != zeroblob(16)), state TEXT NOT NULL CHECK(state IN('JOINED','LEFT')), membership_revision INTEGER NOT NULL CHECK(membership_revision>=1), actor_key TEXT NOT NULL CHECK(length(CAST(actor_key AS BLOB)) BETWEEN 1 AND 256), actor_display_name TEXT NOT NULL CHECK(length(CAST(actor_display_name AS BLOB)) BETWEEN 1 AND 128), actor_participation_id BLOB CHECK(actor_participation_id IS NULL OR length(actor_participation_id)=16), actor_profile_id BLOB CHECK(actor_profile_id IS NULL OR length(actor_profile_id)=16), actor_participant_id BLOB CHECK(actor_participant_id IS NULL OR length(actor_participant_id)=16), actor_session_id BLOB CHECK(actor_session_id IS NULL OR length(actor_session_id)=16), observed_at_ms INTEGER NOT NULL CHECK(observed_at_ms>=0), FOREIGN KEY(request_id) REFERENCES ops_team_requests(request_id), FOREIGN KEY(workspace_id,team_id,participation_id) REFERENCES ops_team_members(workspace_id,team_id,participation_id))";
const TEAMS_SQL: &str = "CREATE TABLE ops_teams (team_id BLOB PRIMARY KEY NOT NULL CHECK(length(team_id)=16 AND team_id != zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), name TEXT NOT NULL CHECK(length(CAST(name AS BLOB)) BETWEEN 1 AND 128), purpose TEXT NOT NULL CHECK(length(CAST(purpose AS BLOB)) BETWEEN 0 AND 2048), state TEXT NOT NULL CHECK(state IN('ACTIVE','RETIRED')), revision INTEGER NOT NULL CHECK(revision>=1), created_by_key TEXT NOT NULL CHECK(length(CAST(created_by_key AS BLOB)) BETWEEN 1 AND 256), created_by_display_name TEXT NOT NULL CHECK(length(CAST(created_by_display_name AS BLOB)) BETWEEN 1 AND 128), created_by_participation_id BLOB CHECK(created_by_participation_id IS NULL OR length(created_by_participation_id)=16), created_by_profile_id BLOB CHECK(created_by_profile_id IS NULL OR length(created_by_profile_id)=16), created_by_participant_id BLOB CHECK(created_by_participant_id IS NULL OR length(created_by_participant_id)=16), created_by_session_id BLOB CHECK(created_by_session_id IS NULL OR length(created_by_session_id)=16), created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0), updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms>=created_at_ms), formed_at_ms INTEGER CHECK(formed_at_ms IS NULL OR formed_at_ms>=created_at_ms), UNIQUE(workspace_id,team_id))";
const TEAM_REQUESTS_SQL: &str = "CREATE TABLE ops_team_requests (request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), operation TEXT NOT NULL CHECK(operation IN('CREATE','JOIN','LEAVE','RETIRE')), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id != zeroblob(16)), actor_key TEXT NOT NULL CHECK(length(CAST(actor_key AS BLOB)) BETWEEN 1 AND 256), actor_display_name TEXT NOT NULL CHECK(length(CAST(actor_display_name AS BLOB)) BETWEEN 1 AND 128), actor_participation_id BLOB CHECK(actor_participation_id IS NULL OR length(actor_participation_id)=16), actor_profile_id BLOB CHECK(actor_profile_id IS NULL OR length(actor_profile_id)=16), actor_participant_id BLOB CHECK(actor_participant_id IS NULL OR length(actor_participant_id)=16), actor_session_id BLOB CHECK(actor_session_id IS NULL OR length(actor_session_id)=16), name TEXT CHECK(name IS NULL OR length(CAST(name AS BLOB)) BETWEEN 1 AND 128), purpose TEXT CHECK(purpose IS NULL OR length(CAST(purpose AS BLOB)) BETWEEN 0 AND 2048), expected_team_revision INTEGER CHECK(expected_team_revision IS NULL OR expected_team_revision>=1), result_team_state TEXT NOT NULL CHECK(result_team_state IN('ACTIVE','RETIRED')), result_team_revision INTEGER NOT NULL CHECK(result_team_revision>=1), result_team_updated_at_ms INTEGER NOT NULL CHECK(result_team_updated_at_ms>=0), result_membership_state TEXT CHECK(result_membership_state IS NULL OR result_membership_state IN('JOINED','LEFT')), result_membership_participation_id BLOB CHECK(result_membership_participation_id IS NULL OR length(result_membership_participation_id)=16), result_membership_revision INTEGER CHECK(result_membership_revision IS NULL OR result_membership_revision>=1), result_membership_joined_at_ms INTEGER CHECK(result_membership_joined_at_ms IS NULL OR result_membership_joined_at_ms>=0), result_membership_left_at_ms INTEGER CHECK(result_membership_left_at_ms IS NULL OR result_membership_left_at_ms>=0), created INTEGER NOT NULL CHECK(created IN(0,1)), observed_at_ms INTEGER NOT NULL CHECK(observed_at_ms>=0), request_actor_kind TEXT CHECK(request_actor_kind IS NULL OR request_actor_kind IN('OPERATOR','PARTICIPANT')), request_actor_activity_id BLOB CHECK(request_actor_activity_id IS NULL OR (length(request_actor_activity_id)=16 AND request_actor_activity_id != zeroblob(16))), request_actor_agent_id BLOB CHECK(request_actor_agent_id IS NULL OR (length(request_actor_agent_id)=16 AND request_actor_agent_id != zeroblob(16))), request_leadership_revision INTEGER CHECK(request_leadership_revision IS NULL OR request_leadership_revision>=1), role TEXT CHECK(role IS NULL OR (length(CAST(role AS BLOB)) BETWEEN 1 AND 256 AND length(trim(role))>0)), result_membership_role TEXT CHECK(result_membership_role IS NULL OR (length(CAST(result_membership_role AS BLOB)) BETWEEN 1 AND 256 AND length(trim(result_membership_role))>0)), CHECK((request_actor_kind IS NULL AND request_actor_activity_id IS NULL AND request_actor_agent_id IS NULL AND request_leadership_revision IS NULL) OR (request_actor_kind='OPERATOR' AND request_actor_activity_id IS NULL AND request_actor_agent_id IS NULL AND request_leadership_revision IS NULL) OR (request_actor_kind='PARTICIPANT' AND request_actor_activity_id IS NOT NULL AND request_actor_agent_id IS NOT NULL)))";
const TEAM_MEMBERS_SQL: &str = "CREATE TABLE ops_team_members (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id != zeroblob(16)), participation_id BLOB NOT NULL CHECK(length(participation_id)=16 AND participation_id != zeroblob(16)), state TEXT NOT NULL CHECK(state IN('JOINED','LEFT')), revision INTEGER NOT NULL CHECK(revision>=1), joined_at_ms INTEGER NOT NULL CHECK(joined_at_ms>=0), left_at_ms INTEGER CHECK(left_at_ms IS NULL OR left_at_ms>=joined_at_ms), role TEXT CHECK(role IS NULL OR (length(CAST(role AS BLOB)) BETWEEN 1 AND 256 AND length(trim(role))>0)), PRIMARY KEY(workspace_id,team_id,participation_id), FOREIGN KEY(workspace_id,team_id) REFERENCES ops_teams(workspace_id,team_id), FOREIGN KEY(workspace_id,participation_id) REFERENCES ops_workspace_participations(workspace_id,participation_id))";
const TEAM_MEMBER_EVENTS_SQL: &str = "CREATE TABLE ops_team_member_events (request_id BLOB NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id != zeroblob(16)), participation_id BLOB NOT NULL CHECK(length(participation_id)=16 AND participation_id != zeroblob(16)), state TEXT NOT NULL CHECK(state IN('JOINED','LEFT')), membership_revision INTEGER NOT NULL CHECK(membership_revision>=1), actor_key TEXT NOT NULL CHECK(length(CAST(actor_key AS BLOB)) BETWEEN 1 AND 256), actor_display_name TEXT NOT NULL CHECK(length(CAST(actor_display_name AS BLOB)) BETWEEN 1 AND 128), actor_participation_id BLOB CHECK(actor_participation_id IS NULL OR length(actor_participation_id)=16), actor_profile_id BLOB CHECK(actor_profile_id IS NULL OR length(actor_profile_id)=16), actor_participant_id BLOB CHECK(actor_participant_id IS NULL OR length(actor_participant_id)=16), actor_session_id BLOB CHECK(actor_session_id IS NULL OR length(actor_session_id)=16), observed_at_ms INTEGER NOT NULL CHECK(observed_at_ms>=0), role TEXT CHECK(role IS NULL OR (length(CAST(role AS BLOB)) BETWEEN 1 AND 256 AND length(trim(role))>0)), PRIMARY KEY(request_id,participation_id), FOREIGN KEY(request_id) REFERENCES ops_team_requests(request_id), FOREIGN KEY(workspace_id,team_id,participation_id) REFERENCES ops_team_members(workspace_id,team_id,participation_id))";

const TEAM_CREATE_ROSTER_SQL: &str = "CREATE TABLE ops_team_create_roster (request_id BLOB NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 63), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id != zeroblob(16)), participation_id BLOB NOT NULL CHECK(length(participation_id)=16 AND participation_id != zeroblob(16)), activity_id BLOB NOT NULL CHECK(length(activity_id)=16 AND activity_id != zeroblob(16)), agent_id BLOB NOT NULL CHECK(length(agent_id)=16 AND agent_id != zeroblob(16)), session_id BLOB NOT NULL CHECK(length(session_id)=16 AND session_id != zeroblob(16)), participant_id BLOB NOT NULL CHECK(length(participant_id)=16 AND participant_id != zeroblob(16)), profile_id BLOB NOT NULL CHECK(length(profile_id)=16 AND profile_id != zeroblob(16)), role TEXT NOT NULL CHECK(length(CAST(role AS BLOB)) BETWEEN 1 AND 256 AND length(trim(role))>0), membership_revision INTEGER NOT NULL CHECK(membership_revision>=1), joined_at_ms INTEGER NOT NULL CHECK(joined_at_ms>=0), PRIMARY KEY(request_id,participation_id), UNIQUE(request_id,ordinal), FOREIGN KEY(request_id) REFERENCES ops_team_requests(request_id), FOREIGN KEY(workspace_id,team_id,participation_id) REFERENCES ops_team_members(workspace_id,team_id,participation_id))";

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("table", "ops_teams", TEAMS_SQL),
        ("table", "ops_team_requests", TEAM_REQUESTS_SQL),
        ("table", "ops_team_members", TEAM_MEMBERS_SQL),
        ("table", "ops_team_member_events", TEAM_MEMBER_EVENTS_SQL),
        ("table", "ops_team_create_roster", TEAM_CREATE_ROSTER_SQL),
    ]
}

pub(crate) fn schema_objects_v36() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("table", "ops_teams", TEAMS_SQL_V36),
        ("table", "ops_team_requests", TEAM_REQUESTS_SQL_V36),
        ("table", "ops_team_members", TEAM_MEMBERS_SQL_V36),
        (
            "table",
            "ops_team_member_events",
            TEAM_MEMBER_EVENTS_SQL_V36,
        ),
    ]
}

/// Rebuild the four frozen V36 Team tables in current V37 shape while preserving
/// every historical row with NULL formation/role/provenance fields.
pub(crate) fn migrate_v36_to_v37(tx: &Transaction<'_>) -> Result<(), OperationsError> {
    tx.execute_batch(
        "ALTER TABLE ops_team_member_events RENAME TO ops_team_member_events_v36;
         ALTER TABLE ops_team_members RENAME TO ops_team_members_v36;
         ALTER TABLE ops_team_requests RENAME TO ops_team_requests_v36;
         ALTER TABLE ops_teams RENAME TO ops_teams_v36;",
    )?;
    create_schema(tx)?;
    tx.execute_batch(
        "INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_by_participation_id,created_by_profile_id,created_by_participant_id,created_by_session_id,created_at_ms,updated_at_ms,formed_at_ms)
         SELECT team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_by_participation_id,created_by_profile_id,created_by_participant_id,created_by_session_id,created_at_ms,updated_at_ms,NULL FROM ops_teams_v36;
         INSERT INTO ops_team_requests(request_id,operation,workspace_id,team_id,actor_key,actor_display_name,actor_participation_id,actor_profile_id,actor_participant_id,actor_session_id,name,purpose,expected_team_revision,result_team_state,result_team_revision,result_team_updated_at_ms,result_membership_state,result_membership_participation_id,result_membership_revision,result_membership_joined_at_ms,result_membership_left_at_ms,created,observed_at_ms,request_actor_kind,request_actor_activity_id,request_actor_agent_id,request_leadership_revision,role,result_membership_role)
         SELECT request_id,operation,workspace_id,team_id,actor_key,actor_display_name,actor_participation_id,actor_profile_id,actor_participant_id,actor_session_id,name,purpose,expected_team_revision,result_team_state,result_team_revision,result_team_updated_at_ms,result_membership_state,result_membership_participation_id,result_membership_revision,result_membership_joined_at_ms,result_membership_left_at_ms,created,observed_at_ms,NULL,NULL,NULL,NULL,NULL,NULL FROM ops_team_requests_v36;
         INSERT INTO ops_team_members(workspace_id,team_id,participation_id,state,revision,joined_at_ms,left_at_ms,role)
         SELECT workspace_id,team_id,participation_id,state,revision,joined_at_ms,left_at_ms,NULL FROM ops_team_members_v36;
         INSERT INTO ops_team_member_events(request_id,workspace_id,team_id,participation_id,state,membership_revision,actor_key,actor_display_name,actor_participation_id,actor_profile_id,actor_participant_id,actor_session_id,observed_at_ms,role)
         SELECT request_id,workspace_id,team_id,participation_id,state,membership_revision,actor_key,actor_display_name,actor_participation_id,actor_profile_id,actor_participant_id,actor_session_id,observed_at_ms,NULL FROM ops_team_member_events_v36;
         DROP TABLE ops_team_member_events_v36;
         DROP TABLE ops_team_members_v36;
         DROP TABLE ops_team_requests_v36;
         DROP TABLE ops_teams_v36;",
    )?;
    Ok(())
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in schema_objects() {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeamState {
    Active,
    Retired,
}

impl TeamState {
    fn encode(self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::Retired => "RETIRED",
        }
    }

    fn decode(value: &str) -> Result<Self, OperationsError> {
        match value {
            "ACTIVE" => Ok(Self::Active),
            "RETIRED" => Ok(Self::Retired),
            _ => Err(OperationsError::Corrupt("team state")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeamMembershipState {
    Joined,
    Left,
}

impl TeamMembershipState {
    fn encode(self) -> &'static str {
        match self {
            Self::Joined => "JOINED",
            Self::Left => "LEFT",
        }
    }

    fn decode(value: &str) -> Result<Self, OperationsError> {
        match value {
            "JOINED" => Ok(Self::Joined),
            "LEFT" => Ok(Self::Left),
            _ => Err(OperationsError::Corrupt("team membership state")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamCreator {
    key: String,
    display_name: String,
    participation_id: Option<WorkspaceParticipationId>,
    profile_id: Option<Uuid>,
    participant_id: Option<Uuid>,
    session_id: Option<AgentSessionId>,
}

impl TeamCreator {
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    #[must_use]
    pub const fn participation_id(&self) -> Option<WorkspaceParticipationId> {
        self.participation_id
    }

    #[must_use]
    pub const fn profile_id(&self) -> Option<Uuid> {
        self.profile_id
    }

    #[must_use]
    pub const fn participant_id(&self) -> Option<Uuid> {
        self.participant_id
    }

    #[must_use]
    pub const fn session_id(&self) -> Option<AgentSessionId> {
        self.session_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamRecord {
    team_id: Uuid,
    workspace_id: WorkspaceId,
    name: String,
    purpose: String,
    state: TeamState,
    revision: u64,
    created_by: TeamCreator,
    created_at: TimestampMillis,
    updated_at: TimestampMillis,
    formed_at: Option<TimestampMillis>,
}

impl TeamRecord {
    #[must_use]
    pub const fn team_id(&self) -> Uuid {
        self.team_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn purpose(&self) -> &str {
        &self.purpose
    }

    #[must_use]
    pub const fn state(&self) -> TeamState {
        self.state
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub const fn created_by(&self) -> &TeamCreator {
        &self.created_by
    }

    #[must_use]
    pub const fn created_at(&self) -> TimestampMillis {
        self.created_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> TimestampMillis {
        self.updated_at
    }

    #[must_use]
    pub const fn formed_at(&self) -> Option<TimestampMillis> {
        self.formed_at
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamMemberRecord {
    workspace_id: WorkspaceId,
    team_id: Uuid,
    participation_id: WorkspaceParticipationId,
    profile_id: Option<Uuid>,
    role: Option<String>,
    state: TeamMembershipState,
    revision: u64,
    joined_at: TimestampMillis,
    left_at: Option<TimestampMillis>,
}

impl TeamMemberRecord {
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn team_id(&self) -> Uuid {
        self.team_id
    }

    #[must_use]
    pub const fn participation_id(&self) -> WorkspaceParticipationId {
        self.participation_id
    }

    #[must_use]
    pub const fn profile_id(&self) -> Option<Uuid> {
        self.profile_id
    }

    #[must_use]
    pub fn role(&self) -> Option<&str> {
        self.role.as_deref()
    }

    #[must_use]
    pub const fn state(&self) -> TeamMembershipState {
        self.state
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub const fn joined_at(&self) -> TimestampMillis {
        self.joined_at
    }

    #[must_use]
    pub const fn left_at(&self) -> Option<TimestampMillis> {
        self.left_at
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamReadSet<T> {
    items: Vec<T>,
    truncated: bool,
}

impl<T> TeamReadSet<T> {
    #[must_use]
    pub fn items(&self) -> &[T] {
        &self.items
    }

    #[must_use]
    pub const fn truncated(&self) -> bool {
        self.truncated
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamMutationRecord {
    team: TeamRecord,
    membership: Option<TeamMemberRecord>,
    initial_members: Vec<TeamMemberRecord>,
    created: bool,
    replayed: bool,
}

impl TeamMutationRecord {
    #[must_use]
    pub const fn team(&self) -> &TeamRecord {
        &self.team
    }

    #[must_use]
    pub const fn membership(&self) -> Option<&TeamMemberRecord> {
        self.membership.as_ref()
    }

    #[must_use]
    pub fn initial_members(&self) -> &[TeamMemberRecord] {
        &self.initial_members
    }

    #[must_use]
    pub const fn created(&self) -> bool {
        self.created
    }

    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Complete authoritative Team read assembled from one verified Workspace
/// snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamQuerySnapshot {
    /// Current Team record, or `None` when the exact Team ID is absent.
    pub team: Option<TeamRecord>,
    /// Bounded member page from the same SQLite read transaction.
    pub members: TeamReadSet<TeamMemberRecord>,
    /// Exact historical receipt when the requested mutation ID exists.
    pub mutation: Option<TeamMutationRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamCreateCommand {
    pub request_id: Uuid,
    pub team_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub actor: OperationalActor,
    pub expected_sender_leadership_revision: Option<i64>,
    pub name: String,
    pub purpose: String,
    pub initial_members: Vec<TeamRosterMemberInput>,
    pub observed_at: TimestampMillis,
}

/// One exact authenticated participant assigned to a newly formed Team.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamRosterMemberInput {
    pub activity_id: ActivityId,
    pub agent_id: AgentInstanceId,
    pub session_id: AgentSessionId,
    pub role: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamJoinCommand {
    pub request_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub team_id: Uuid,
    pub expected_team_revision: u64,
    pub actor: OperationalActor,
    pub role: String,
    pub observed_at: TimestampMillis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamLeaveCommand {
    pub request_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub team_id: Uuid,
    pub expected_team_revision: u64,
    pub actor: OperationalActor,
    pub observed_at: TimestampMillis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamRetireCommand {
    pub request_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub team_id: Uuid,
    pub expected_team_revision: u64,
    pub actor: OperationalActor,
    pub observed_at: TimestampMillis,
}

pub(crate) fn create_team(
    connection: &mut Connection,
    mut command: TeamCreateCommand,
) -> Result<TeamMutationRecord, OperationsError> {
    command.initial_members = canonicalize_roster(std::mem::take(&mut command.initial_members))?;
    validate_request_id(command.request_id)?;
    validate_team_id(command.team_id)?;
    validate_text(&command.name, MAX_TEAM_NAME_BYTES, "team name", true)?;
    validate_text(
        &command.purpose,
        MAX_TEAM_PURPOSE_BYTES,
        "team purpose",
        true,
    )?;
    validate_team_creator_fence_input(command.actor, command.expected_sender_leadership_revision)?;
    let observed_at = command.observed_at.get();
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    ensure_verified_workspace(&transaction, command.workspace_id)?;
    if let Some(existing) = load_request(&transaction, command.request_id)? {
        let fingerprint = RequestFingerprint::create(existing.fingerprint.actor.clone(), &command);
        return replay_or_conflict(
            transaction,
            command.workspace_id,
            command.request_id,
            &existing,
            &fingerprint,
        );
    }
    if team_exists_any(&transaction, command.team_id)? {
        return Err(OperationsError::Conflict("team identity"));
    }
    let actor = resolve_actor(&transaction, command.workspace_id, command.actor)?;
    authorize_team_creator(
        &transaction,
        command.workspace_id,
        command.actor,
        &actor,
        command.expected_sender_leadership_revision,
    )?;
    let roster = resolve_initial_roster(
        &transaction,
        command.workspace_id,
        command.team_id,
        command.initial_members.clone(),
        observed_at,
    )?;
    let fingerprint = RequestFingerprint::create(TeamCreator::from_actor(&actor), &command);
    let record = insert_formed_team(
        &transaction,
        &command,
        &actor,
        &roster,
        &fingerprint,
        observed_at,
    )?;
    commit_team(transaction, command.workspace_id, command.request_id)?;
    Ok(record)
}

fn insert_formed_team(
    transaction: &Transaction<'_>,
    command: &TeamCreateCommand,
    actor: &ResolvedOperationalActor,
    roster: &[ResolvedTeamRosterMember],
    fingerprint: &RequestFingerprint,
    observed_at: i64,
) -> Result<TeamMutationRecord, OperationsError> {
    let creator = TeamCreator::from_actor(actor);
    transaction.execute(
        "INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_by_participation_id,created_by_profile_id,created_by_participant_id,created_by_session_id,created_at_ms,updated_at_ms,formed_at_ms) VALUES(?1,?2,?3,?4,'ACTIVE',1,?5,?6,?7,?8,?9,?10,?11,?11,?11)",
        params![
            command.team_id.as_bytes(),
            command.workspace_id.into_uuid().as_bytes(),
            command.name.as_str(),
            command.purpose.as_str(),
            creator.key,
            creator.display_name,
            creator.participation_id.map(|id| id.into_uuid().into_bytes().to_vec()),
            creator.profile_id.map(|id| id.into_bytes().to_vec()),
            creator.participant_id.map(|id| id.into_bytes().to_vec()),
            creator.session_id.map(|id| id.into_uuid().into_bytes().to_vec()),
            observed_at,
        ],
    )?;
    let mut initial_members = Vec::with_capacity(roster.len());
    for member in roster {
        transaction.execute(
            "INSERT INTO ops_team_members(workspace_id,team_id,participation_id,state,revision,joined_at_ms,left_at_ms,role) VALUES(?1,?2,?3,'JOINED',1,?4,NULL,?5)",
            params![
                command.workspace_id.into_uuid().as_bytes(),
                command.team_id.as_bytes(),
                member.member.participation_id.into_uuid().as_bytes(),
                observed_at,
                member.input.role,
            ],
        )?;
        initial_members.push(member.member.clone());
    }
    let team = load_team_row(transaction, command.workspace_id, command.team_id)?
        .ok_or(OperationsError::Corrupt("team insert readback"))?
        .record();
    let record = TeamMutationRecord {
        team,
        membership: None,
        initial_members,
        created: true,
        replayed: false,
    };
    insert_request(
        transaction,
        command.request_id,
        fingerprint,
        &record,
        observed_at,
    )?;
    for (ordinal, member) in roster.iter().enumerate() {
        insert_team_create_roster_member(
            transaction,
            command.request_id,
            ordinal,
            command.workspace_id,
            command.team_id,
            member,
        )?;
        insert_membership_event(
            transaction,
            command.request_id,
            &member.member,
            actor,
            observed_at,
        )?;
    }
    Ok(record)
}

#[derive(Debug, Clone)]
struct ResolvedTeamRosterMember {
    input: TeamRosterMemberInput,
    actor: ResolvedOperationalActor,
    member: TeamMemberRecord,
}

fn canonicalize_roster(
    mut members: Vec<TeamRosterMemberInput>,
) -> Result<Vec<TeamRosterMemberInput>, OperationsError> {
    if !(MIN_FORMING_TEAM_MEMBERS..=MAX_INITIAL_TEAM_MEMBERS).contains(&members.len()) {
        return Err(OperationsError::Bounds("team initial roster"));
    }
    for member in &members {
        if member.activity_id.into_uuid().is_nil()
            || member.agent_id.into_uuid().is_nil()
            || member.session_id.into_uuid().is_nil()
        {
            return Err(OperationsError::Conflict("team initial roster identity"));
        }
        validate_text(&member.role, MAX_TEAM_ROLE_BYTES, "team role", true)?;
    }
    members.sort_by(|left, right| {
        (
            left.activity_id.into_uuid(),
            left.agent_id.into_uuid(),
            left.session_id.into_uuid(),
            left.role.as_str(),
        )
            .cmp(&(
                right.activity_id.into_uuid(),
                right.agent_id.into_uuid(),
                right.session_id.into_uuid(),
                right.role.as_str(),
            ))
    });
    if members.windows(2).any(|pair| {
        pair[0].activity_id == pair[1].activity_id
            && pair[0].agent_id == pair[1].agent_id
            && pair[0].session_id == pair[1].session_id
    }) {
        return Err(OperationsError::Conflict("duplicate team roster actor"));
    }
    Ok(members)
}

fn validate_team_creator_fence_input(
    actor: OperationalActor,
    expected_leadership_revision: Option<i64>,
) -> Result<(), OperationsError> {
    match (actor, expected_leadership_revision) {
        (OperationalActor::LocalOperator, None) => Ok(()),
        (OperationalActor::Participant { .. }, Some(revision)) if revision >= 1 => Ok(()),
        (OperationalActor::LocalOperator, Some(_)) => {
            Err(OperationsError::Conflict("operator leadership revision"))
        }
        (OperationalActor::Participant { .. }, _) => Err(OperationsError::Conflict(
            "team create requires expected Workspace leadership revision",
        )),
    }
}

fn authorize_team_creator(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    actor_input: OperationalActor,
    actor: &ResolvedOperationalActor,
    expected_leadership_revision: Option<i64>,
) -> Result<(), OperationsError> {
    match (actor_input, expected_leadership_revision) {
        (OperationalActor::LocalOperator, None) if actor.is_operator() => Ok(()),
        (OperationalActor::Participant { .. }, Some(expected_revision)) => {
            let authority = crate::leadership::current_leadership_authority_in_transaction(
                transaction,
                crate::LeadershipScope::workspace(workspace_id),
            )?
            .ok_or(OperationsError::Conflict(
                "team create requires current Workspace leader",
            ))?;
            if actor.participant_id != Some(authority.participant_id())
                || expected_revision != authority.leadership_revision()
            {
                return Err(OperationsError::Conflict(
                    "team create Workspace leadership changed",
                ));
            }
            Ok(())
        }
        _ => Err(OperationsError::Conflict("team create actor authority")),
    }
}

fn resolve_initial_roster(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    roster: Vec<TeamRosterMemberInput>,
    observed_at: i64,
) -> Result<Vec<ResolvedTeamRosterMember>, OperationsError> {
    let mut result = Vec::with_capacity(roster.len());
    let mut participants = Vec::with_capacity(roster.len());
    let mut participations = Vec::with_capacity(roster.len());
    for input in roster {
        let actor = resolve_actor(
            transaction,
            workspace_id,
            OperationalActor::Participant {
                activity_id: input.activity_id,
                agent_id: input.agent_id,
                session_id: input.session_id,
            },
        )?;
        let participant_id = actor.participant_id.ok_or(OperationsError::Conflict(
            "team roster member is not a participant",
        ))?;
        let participation_id = actor.participation_id.ok_or(OperationsError::Conflict(
            "team roster member has no participation",
        ))?;
        if participants.contains(&participant_id) || participations.contains(&participation_id) {
            return Err(OperationsError::Conflict(
                "duplicate team roster participant",
            ));
        }
        participants.push(participant_id);
        participations.push(participation_id);
        result.push(ResolvedTeamRosterMember {
            member: TeamMemberRecord {
                workspace_id,
                team_id,
                participation_id,
                profile_id: actor.profile_id,
                role: Some(input.role.clone()),
                state: TeamMembershipState::Joined,
                revision: 1,
                joined_at: TimestampMillis::new(observed_at)
                    .map_err(|_| OperationsError::Corrupt("team roster joined timestamp"))?,
                left_at: None,
            },
            input,
            actor,
        });
    }
    Ok(result)
}

fn insert_team_create_roster_member(
    transaction: &Transaction<'_>,
    request_id: Uuid,
    ordinal: usize,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    member: &ResolvedTeamRosterMember,
) -> Result<(), OperationsError> {
    let ordinal =
        i64::try_from(ordinal).map_err(|_| OperationsError::Bounds("team roster ordinal"))?;
    let participation_id = member.member.participation_id;
    let participant_id = member
        .actor
        .participant_id
        .ok_or(OperationsError::Corrupt("team roster participant readback"))?;
    let profile_id = member
        .actor
        .profile_id
        .ok_or(OperationsError::Corrupt("team roster profile readback"))?;
    transaction.execute(
        "INSERT INTO ops_team_create_roster(request_id,ordinal,workspace_id,team_id,participation_id,activity_id,agent_id,session_id,participant_id,profile_id,role,membership_revision,joined_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
        params![
            request_id.as_bytes(),
            ordinal,
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes(),
            participation_id.into_uuid().as_bytes(),
            member.input.activity_id.into_uuid().as_bytes(),
            member.input.agent_id.into_uuid().as_bytes(),
            member.input.session_id.into_uuid().as_bytes(),
            participant_id.as_bytes(),
            profile_id.as_bytes(),
            member.input.role,
            i64::try_from(member.member.revision).map_err(|_| OperationsError::Bounds("team member revision"))?,
            member.member.joined_at.get(),
        ],
    )?;
    Ok(())
}

pub(crate) fn join_team(
    connection: &mut Connection,
    command: TeamJoinCommand,
) -> Result<TeamMutationRecord, OperationsError> {
    mutate_membership(connection, &MembershipMutation::Join(command))
}

pub(crate) fn leave_team(
    connection: &mut Connection,
    command: TeamLeaveCommand,
) -> Result<TeamMutationRecord, OperationsError> {
    mutate_membership(connection, &MembershipMutation::Leave(command))
}

pub(crate) fn retire_team(
    connection: &mut Connection,
    command: TeamRetireCommand,
) -> Result<TeamMutationRecord, OperationsError> {
    validate_request_id(command.request_id)?;
    validate_team_id(command.team_id)?;
    validate_revision(command.expected_team_revision)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    ensure_verified_workspace(&transaction, command.workspace_id)?;
    let actor = resolve_actor(&transaction, command.workspace_id, command.actor)?;
    if !actor.is_operator() {
        return Err(OperationsError::Conflict(
            "team retire requires local operator",
        ));
    }
    let fingerprint = RequestFingerprint::simple(
        TeamOperation::Retire,
        command.workspace_id,
        command.team_id,
        &actor,
        Some(command.expected_team_revision),
        None,
    );
    if let Some(existing) = load_request(&transaction, command.request_id)? {
        return replay_or_conflict(
            transaction,
            command.workspace_id,
            command.request_id,
            &existing,
            &fingerprint,
        );
    }
    let row = load_team_row(&transaction, command.workspace_id, command.team_id)?
        .ok_or(OperationsError::Conflict("team workspace scope"))?;
    if row.revision != command.expected_team_revision {
        return Err(OperationsError::Conflict("team revision"));
    }
    if row.state == TeamState::Retired {
        return Err(OperationsError::Conflict("team already retired"));
    }
    let next_revision = increment_revision(row.revision)?;
    let observed_at = command.observed_at.get();
    let changed = transaction.execute(
		"UPDATE ops_teams SET state='RETIRED',revision=?1,updated_at_ms=?2 WHERE workspace_id=?3 AND team_id=?4 AND revision=?5 AND state='ACTIVE'",
		params![
			i64::try_from(next_revision).map_err(|_| OperationsError::Bounds("team revision"))?,
			observed_at,
			command.workspace_id.into_uuid().as_bytes(),
			command.team_id.as_bytes(),
			i64::try_from(command.expected_team_revision)
				.map_err(|_| OperationsError::Bounds("team revision"))?,
		],
	)?;
    if changed != 1 {
        return Err(OperationsError::Conflict("team revision"));
    }
    let team = load_team_row(&transaction, command.workspace_id, command.team_id)?
        .ok_or(OperationsError::Corrupt("team retire readback"))?
        .record();
    let record = TeamMutationRecord {
        team,
        membership: None,
        initial_members: Vec::new(),
        created: false,
        replayed: false,
    };
    insert_request(
        &transaction,
        command.request_id,
        &fingerprint,
        &record,
        observed_at,
    )?;
    crate::leadership::retire_team_leadership(
        &transaction,
        command.workspace_id,
        command.team_id,
        observed_at,
    )?;
    commit_team(transaction, command.workspace_id, command.request_id)?;
    Ok(record)
}

pub(crate) fn read_team(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<Option<TeamRecord>, OperationsError> {
    validate_team_id(team_id)?;
    let transaction = connection.unchecked_transaction()?;
    ensure_verified_workspace(&transaction, workspace_id)?;
    let result = read_team_in_transaction(&transaction, workspace_id, team_id)?;
    commit_team_read(transaction)?;
    Ok(result)
}

fn read_team_in_transaction(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<Option<TeamRecord>, OperationsError> {
    validate_team_id(team_id)?;
    match load_team_row(connection, workspace_id, team_id)? {
        Some(row) => Ok(Some(row.record())),
        None if team_exists_any(connection, team_id)? => {
            Err(OperationsError::Conflict("team workspace scope"))
        }
        None => Ok(None),
    }
}

pub(crate) fn list_teams(
    connection: &Connection,
    workspace_id: WorkspaceId,
    limit: u32,
) -> Result<TeamReadSet<TeamRecord>, OperationsError> {
    let limit = bounded_limit(limit, MAX_TEAMS_PER_READ, "team list")?;
    let transaction = connection.unchecked_transaction()?;
    ensure_verified_workspace(&transaction, workspace_id)?;
    let mut statement = transaction.prepare("SELECT team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_by_participation_id,created_by_profile_id,created_by_participant_id,created_by_session_id,created_at_ms,updated_at_ms,formed_at_ms FROM ops_teams WHERE workspace_id=?1 ORDER BY team_id LIMIT ?2")?;
    let rows = statement
        .query_map(
            params![
                workspace_id.into_uuid().as_bytes(),
                i64::try_from(limit + 1).map_err(|_| OperationsError::Bounds("team list"))?
            ],
            decode_team_row,
        )?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    let truncated = rows.len() > limit;
    let items = rows
        .into_iter()
        .take(limit)
        .map(TeamRow::record)
        .collect::<Vec<_>>();
    commit_team_read(transaction)?;
    Ok(TeamReadSet { items, truncated })
}

/// Count the current JOINED members per Team for one Workspace in a single
/// bounded read. Left memberships are excluded: the count is the live team
/// size shown on the operator dashboard.
pub(crate) fn team_member_counts(
    connection: &Connection,
    workspace_id: WorkspaceId,
) -> Result<HashMap<Uuid, u64>, OperationsError> {
    let transaction = connection.unchecked_transaction()?;
    ensure_verified_workspace(&transaction, workspace_id)?;
    let mut statement = transaction.prepare(
        "SELECT team_id,COUNT(*) FROM ops_team_members WHERE workspace_id=?1 AND state='JOINED' GROUP BY team_id",
    )?;
    let rows = statement.query_map(params![workspace_id.into_uuid().as_bytes()], |row| {
        let team_id = Uuid::from_bytes(wire::id(row.get(0)?).map_err(to_sql_error)?);
        let count = row.get::<_, i64>(1)?;
        let count = u64::try_from(count)
            .map_err(|_| to_sql_error(OperationsError::Bounds("team member count")))?;
        Ok((team_id, count))
    })?;
    let mut counts = HashMap::new();
    for row in rows {
        let (team_id, count) = row?;
        counts.insert(team_id, count);
    }
    drop(statement);
    commit_team_read(transaction)?;
    Ok(counts)
}

/// Fresh Team admission only. The caller binds `t` to `ops_teams`; frozen
/// election authority and already-admitted work deliberately do not use it.
pub(crate) const FORMED_TEAM_ADMISSION_PREDICATE: &str =
    "t.state='ACTIVE' AND t.formed_at_ms IS NOT NULL
     AND NOT EXISTS(SELECT 1 FROM ops_team_members missing_role
       WHERE missing_role.workspace_id=t.workspace_id AND missing_role.team_id=t.team_id
         AND missing_role.state='JOINED' AND missing_role.role IS NULL)
     AND EXISTS(SELECT 1 FROM ops_team_members member
       JOIN ops_workspace_participation_mappings mapping
         ON mapping.workspace_id=member.workspace_id AND mapping.participation_id=member.participation_id
       JOIN ops_logical_members logical
         ON logical.workspace_id=mapping.workspace_id AND logical.participant_id=mapping.participant_id
       JOIN ops_current_effect_bindings binding
         ON binding.workspace_id=logical.workspace_id AND binding.participant_id=logical.participant_id
            AND binding.session_id=logical.session_id
       WHERE member.workspace_id=t.workspace_id AND member.team_id=t.team_id AND member.state='JOINED'
       GROUP BY member.workspace_id,member.team_id
       HAVING COUNT(DISTINCT member.participation_id)>=2 AND COUNT(DISTINCT logical.participant_id)>=2)";

pub(crate) fn ensure_team_formed(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<(), OperationsError> {
    validate_team_id(team_id)?;
    let formed: bool = connection.query_row(
        &format!("SELECT EXISTS(SELECT 1 FROM ops_teams t WHERE t.workspace_id=?1 AND t.team_id=?2 AND ({FORMED_TEAM_ADMISSION_PREDICATE}))"),
        params![workspace_id.into_uuid().as_bytes(),team_id.as_bytes()],
        |row| row.get(0),
    )?;
    if !formed {
        return Err(OperationsError::Conflict("team is not formed"));
    }
    Ok(())
}

pub(crate) fn list_team_members(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    limit: u32,
) -> Result<TeamReadSet<TeamMemberRecord>, OperationsError> {
    validate_team_id(team_id)?;
    bounded_limit(limit, MAX_TEAM_MEMBERS_PER_READ, "team member list")?;
    let transaction = connection.unchecked_transaction()?;
    ensure_verified_workspace(&transaction, workspace_id)?;
    let page = list_team_members_in_transaction(&transaction, workspace_id, team_id, limit)?;
    commit_team_read(transaction)?;
    Ok(page)
}

fn list_team_members_in_transaction(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    limit: u32,
) -> Result<TeamReadSet<TeamMemberRecord>, OperationsError> {
    validate_team_id(team_id)?;
    let limit = bounded_limit(limit, MAX_TEAM_MEMBERS_PER_READ, "team member list")?;
    if !team_exists(connection, workspace_id, team_id)? {
        return Err(OperationsError::Conflict("team workspace scope"));
    }
    let mut statement = connection.prepare("SELECT m.workspace_id,m.team_id,m.participation_id,m.state,m.revision,m.joined_at_ms,m.left_at_ms,p.agent_profile_id,m.role FROM ops_team_members AS m LEFT JOIN ops_workspace_participations AS p ON p.workspace_id=m.workspace_id AND p.participation_id=m.participation_id WHERE m.workspace_id=?1 AND m.team_id=?2 ORDER BY m.participation_id LIMIT ?3")?;
    let rows = statement
        .query_map(
            params![
                workspace_id.into_uuid().as_bytes(),
                team_id.as_bytes(),
                i64::try_from(limit + 1)
                    .map_err(|_| OperationsError::Bounds("team member list"))?
            ],
            decode_team_member_row,
        )?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    let truncated = rows.len() > limit;
    let items = rows.into_iter().take(limit).collect();
    Ok(TeamReadSet { items, truncated })
}

enum MembershipMutation {
    Join(TeamJoinCommand),
    Leave(TeamLeaveCommand),
}

impl MembershipMutation {
    fn request_id(&self) -> Uuid {
        match self {
            Self::Join(command) => command.request_id,
            Self::Leave(command) => command.request_id,
        }
    }

    fn workspace_id(&self) -> WorkspaceId {
        match self {
            Self::Join(command) => command.workspace_id,
            Self::Leave(command) => command.workspace_id,
        }
    }

    fn team_id(&self) -> Uuid {
        match self {
            Self::Join(command) => command.team_id,
            Self::Leave(command) => command.team_id,
        }
    }

    fn expected_revision(&self) -> u64 {
        match self {
            Self::Join(command) => command.expected_team_revision,
            Self::Leave(command) => command.expected_team_revision,
        }
    }

    fn actor(&self) -> OperationalActor {
        match self {
            Self::Join(command) => command.actor,
            Self::Leave(command) => command.actor,
        }
    }

    fn observed_at(&self) -> TimestampMillis {
        match self {
            Self::Join(command) => command.observed_at,
            Self::Leave(command) => command.observed_at,
        }
    }

    fn operation(&self) -> TeamOperation {
        match self {
            Self::Join(_) => TeamOperation::Join,
            Self::Leave(_) => TeamOperation::Leave,
        }
    }

    fn role(&self) -> Option<&str> {
        match self {
            Self::Join(command) => Some(command.role.as_str()),
            Self::Leave(_) => None,
        }
    }
}

fn mutate_membership(
    connection: &mut Connection,
    command: &MembershipMutation,
) -> Result<TeamMutationRecord, OperationsError> {
    validate_request_id(command.request_id())?;
    validate_team_id(command.team_id())?;
    validate_revision(command.expected_revision())?;
    if let Some(role) = command.role() {
        validate_text(role, MAX_TEAM_ROLE_BYTES, "team role", true)?;
    }
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    ensure_verified_workspace(&transaction, command.workspace_id())?;
    let actor = resolve_actor(&transaction, command.workspace_id(), command.actor())?;
    let participation_id = actor.participation_id.ok_or(OperationsError::Conflict(
        "team membership requires participant",
    ))?;
    let fingerprint = RequestFingerprint::simple(
        command.operation(),
        command.workspace_id(),
        command.team_id(),
        &actor,
        Some(command.expected_revision()),
        command.role(),
    );
    if let Some(existing) = load_request(&transaction, command.request_id())? {
        return replay_or_conflict(
            transaction,
            command.workspace_id(),
            command.request_id(),
            &existing,
            &fingerprint,
        );
    }
    let row = load_team_row(&transaction, command.workspace_id(), command.team_id())?
        .ok_or(OperationsError::Conflict("team workspace scope"))?;
    if row.revision != command.expected_revision() {
        return Err(OperationsError::Conflict("team revision"));
    }
    if row.state == TeamState::Retired {
        return Err(OperationsError::Conflict("team retired"));
    }
    let observed_at = command.observed_at().get();
    let existing = load_team_member(
        &transaction,
        command.workspace_id(),
        command.team_id(),
        participation_id,
    )?;
    let (membership, created) = apply_membership_change(
        &transaction,
        command,
        participation_id,
        existing,
        observed_at,
    )?;
    let next_team_revision = increment_revision(row.revision)?;
    let changed = transaction.execute(
		"UPDATE ops_teams SET revision=?1,updated_at_ms=?2 WHERE workspace_id=?3 AND team_id=?4 AND revision=?5 AND state='ACTIVE'",
		params![i64::try_from(next_team_revision).map_err(|_| OperationsError::Bounds("team revision"))?, observed_at, command.workspace_id().into_uuid().as_bytes(), command.team_id().as_bytes(), i64::try_from(command.expected_revision()).map_err(|_| OperationsError::Bounds("team revision"))?],
	)?;
    if changed != 1 {
        return Err(OperationsError::Conflict("team revision"));
    }
    let team = load_team_row(&transaction, command.workspace_id(), command.team_id())?
        .ok_or(OperationsError::Corrupt("team membership team readback"))?
        .record();
    let record = TeamMutationRecord {
        team,
        membership: Some(membership),
        initial_members: Vec::new(),
        created,
        replayed: false,
    };
    insert_request(
        &transaction,
        command.request_id(),
        &fingerprint,
        &record,
        observed_at,
    )?;
    let membership = record
        .membership
        .as_ref()
        .ok_or(OperationsError::Corrupt("team membership record"))?;
    insert_membership_event(
        &transaction,
        command.request_id(),
        membership,
        &actor,
        observed_at,
    )?;
    if membership.state() == TeamMembershipState::Left {
        crate::leadership::reconcile_team_departure(
            &transaction,
            command.workspace_id(),
            command.team_id(),
            participation_id,
            observed_at,
        )?;
    }
    commit_team(transaction, command.workspace_id(), command.request_id())?;
    Ok(record)
}

fn apply_membership_change(
    transaction: &Transaction<'_>,
    command: &MembershipMutation,
    participation_id: WorkspaceParticipationId,
    existing: Option<TeamMemberRecord>,
    observed_at: i64,
) -> Result<(TeamMemberRecord, bool), OperationsError> {
    match (command.operation(), existing) {
        (TeamOperation::Join, None) => {
            ensure_participation(transaction, command.workspace_id(), participation_id)?;
            let role = command
                .role()
                .ok_or(OperationsError::Corrupt("team join role"))?;
            transaction.execute(
				"INSERT INTO ops_team_members(workspace_id,team_id,participation_id,state,revision,joined_at_ms,left_at_ms,role) VALUES(?1,?2,?3,'JOINED',1,?4,NULL,?5)",
				params![command.workspace_id().into_uuid().as_bytes(), command.team_id().as_bytes(), participation_id.into_uuid().as_bytes(), observed_at, role],
			)?;
            Ok((
                load_team_member(
                    transaction,
                    command.workspace_id(),
                    command.team_id(),
                    participation_id,
                )?
                .ok_or(OperationsError::Corrupt("team member join readback"))?,
                true,
            ))
        }
        (TeamOperation::Join, Some(current)) if current.state == TeamMembershipState::Left => {
            let next = increment_revision(current.revision)?;
            let role = command
                .role()
                .ok_or(OperationsError::Corrupt("team join role"))?;
            transaction.execute(
				"UPDATE ops_team_members SET state='JOINED',revision=?1,joined_at_ms=?2,left_at_ms=NULL,role=?3 WHERE workspace_id=?4 AND team_id=?5 AND participation_id=?6 AND revision=?7 AND state='LEFT'",
				params![i64::try_from(next).map_err(|_| OperationsError::Bounds("team membership revision"))?, observed_at, role, command.workspace_id().into_uuid().as_bytes(), command.team_id().as_bytes(), participation_id.into_uuid().as_bytes(), i64::try_from(current.revision).map_err(|_| OperationsError::Bounds("team membership revision"))?],
			)?;
            Ok((
                load_team_member(
                    transaction,
                    command.workspace_id(),
                    command.team_id(),
                    participation_id,
                )?
                .ok_or(OperationsError::Corrupt("team member rejoin readback"))?,
                false,
            ))
        }
        (TeamOperation::Join, Some(_)) => {
            Err(OperationsError::Conflict("team member already joined"))
        }
        (TeamOperation::Leave, Some(current)) if current.state == TeamMembershipState::Joined => {
            let next = increment_revision(current.revision)?;
            transaction.execute(
				"UPDATE ops_team_members SET state='LEFT',revision=?1,left_at_ms=?2 WHERE workspace_id=?3 AND team_id=?4 AND participation_id=?5 AND revision=?6 AND state='JOINED'",
				params![i64::try_from(next).map_err(|_| OperationsError::Bounds("team membership revision"))?, observed_at, command.workspace_id().into_uuid().as_bytes(), command.team_id().as_bytes(), participation_id.into_uuid().as_bytes(), i64::try_from(current.revision).map_err(|_| OperationsError::Bounds("team membership revision"))?],
			)?;
            Ok((
                load_team_member(
                    transaction,
                    command.workspace_id(),
                    command.team_id(),
                    participation_id,
                )?
                .ok_or(OperationsError::Corrupt("team member leave readback"))?,
                false,
            ))
        }
        (TeamOperation::Leave, Some(_)) => Err(OperationsError::Conflict("team member not joined")),
        (TeamOperation::Leave, None) => Err(OperationsError::Conflict("team member missing")),
        (TeamOperation::Create | TeamOperation::Retire, _) => {
            Err(OperationsError::Corrupt("team membership operation"))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TeamOperation {
    Create,
    Join,
    Leave,
    Retire,
}

impl TeamOperation {
    fn encode(self) -> &'static str {
        match self {
            Self::Create => "CREATE",
            Self::Join => "JOIN",
            Self::Leave => "LEAVE",
            Self::Retire => "RETIRE",
        }
    }

    fn decode(value: &str) -> Result<Self, OperationsError> {
        match value {
            "CREATE" => Ok(Self::Create),
            "JOIN" => Ok(Self::Join),
            "LEAVE" => Ok(Self::Leave),
            "RETIRE" => Ok(Self::Retire),
            _ => Err(OperationsError::Corrupt("team request operation")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RequestFingerprint {
    operation: TeamOperation,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    actor: TeamCreator,
    name: Option<String>,
    purpose: Option<String>,
    expected_revision: Option<u64>,
    request_actor: Option<OperationalActor>,
    expected_sender_leadership_revision: Option<i64>,
    role: Option<String>,
    initial_roster: Vec<TeamRosterMemberInput>,
}

impl RequestFingerprint {
    fn create(actor: TeamCreator, command: &TeamCreateCommand) -> Self {
        Self {
            operation: TeamOperation::Create,
            workspace_id: command.workspace_id,
            team_id: command.team_id,
            actor,
            name: Some(command.name.clone()),
            purpose: Some(command.purpose.clone()),
            expected_revision: None,
            request_actor: Some(command.actor),
            expected_sender_leadership_revision: command.expected_sender_leadership_revision,
            role: None,
            initial_roster: command.initial_members.clone(),
        }
    }

    fn simple(
        operation: TeamOperation,
        workspace_id: WorkspaceId,
        team_id: Uuid,
        actor: &ResolvedOperationalActor,
        expected_revision: Option<u64>,
        role: Option<&str>,
    ) -> Self {
        Self {
            operation,
            workspace_id,
            team_id,
            actor: TeamCreator::from_actor(actor),
            name: None,
            purpose: None,
            expected_revision,
            request_actor: None,
            expected_sender_leadership_revision: None,
            role: role.map(str::to_owned),
            initial_roster: Vec::new(),
        }
    }

    fn semantically_matches(&self, other: &Self) -> bool {
        self.operation == other.operation
            && self.workspace_id == other.workspace_id
            && self.team_id == other.team_id
            && self.actor.key == other.actor.key
            && self.actor.participation_id == other.actor.participation_id
            && self.actor.profile_id == other.actor.profile_id
            && self.actor.participant_id == other.actor.participant_id
            && self.actor.session_id == other.actor.session_id
            && self.name == other.name
            && self.purpose == other.purpose
            && self.expected_revision == other.expected_revision
            && self.request_actor == other.request_actor
            && self.expected_sender_leadership_revision == other.expected_sender_leadership_revision
            && self.role == other.role
            && self.initial_roster == other.initial_roster
    }
}

impl TeamCreator {
    fn from_actor(actor: &ResolvedOperationalActor) -> Self {
        Self {
            key: actor.key(),
            display_name: actor.display_name.clone(),
            participation_id: actor.participation_id,
            profile_id: actor.profile_id,
            participant_id: actor.participant_id,
            session_id: actor.session_id,
        }
    }
}

#[derive(Debug)]
struct TeamRow {
    team_id: Uuid,
    workspace_id: WorkspaceId,
    name: String,
    purpose: String,
    state: TeamState,
    revision: u64,
    created_by: TeamCreator,
    created_at: TimestampMillis,
    updated_at: TimestampMillis,
    formed_at: Option<TimestampMillis>,
}

impl TeamRow {
    fn record(self) -> TeamRecord {
        TeamRecord {
            team_id: self.team_id,
            workspace_id: self.workspace_id,
            name: self.name,
            purpose: self.purpose,
            state: self.state,
            revision: self.revision,
            created_by: self.created_by,
            created_at: self.created_at,
            updated_at: self.updated_at,
            formed_at: self.formed_at,
        }
    }
}

#[derive(Debug, Clone)]
struct RawRequest {
    fingerprint: RequestFingerprint,
    result_team_state: TeamState,
    result_team_revision: u64,
    result_team_updated_at: TimestampMillis,
    result_membership: Option<TeamMemberRecord>,
    initial_members: Vec<TeamMemberRecord>,
    created: bool,
}

fn load_team_row(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<Option<TeamRow>, OperationsError> {
    Ok(connection.query_row("SELECT team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_by_participation_id,created_by_profile_id,created_by_participant_id,created_by_session_id,created_at_ms,updated_at_ms,formed_at_ms FROM ops_teams WHERE workspace_id=?1 AND team_id=?2 LIMIT 1", params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()], decode_team_row).optional()?)
}

fn decode_team_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TeamRow> {
    let team_id: Vec<u8> = row.get(0)?;
    let workspace_id: Vec<u8> = row.get(1)?;
    let creator = TeamCreator {
        key: row.get(6)?,
        display_name: row.get(7)?,
        participation_id: decode_optional_participation(row.get(8)?).map_err(to_sql_error)?,
        profile_id: decode_optional_uuid(row.get(9)?).map_err(to_sql_error)?,
        participant_id: decode_optional_uuid(row.get(10)?).map_err(to_sql_error)?,
        session_id: decode_optional_session(row.get(11)?).map_err(to_sql_error)?,
    };
    Ok(TeamRow {
        team_id: Uuid::from_bytes(wire::id(team_id).map_err(to_sql_error)?),
        workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(
            wire::id(workspace_id).map_err(to_sql_error)?,
        )),
        name: row.get(2)?,
        purpose: row.get(3)?,
        state: TeamState::decode(&row.get::<_, String>(4)?).map_err(to_sql_error)?,
        revision: decode_revision(row.get(5)?).map_err(to_sql_error)?,
        created_by: creator,
        created_at: decode_timestamp(row.get(12)?).map_err(to_sql_error)?,
        updated_at: decode_timestamp(row.get(13)?).map_err(to_sql_error)?,
        formed_at: row
            .get::<_, Option<i64>>(14)?
            .map(decode_timestamp)
            .transpose()
            .map_err(to_sql_error)?,
    })
}

fn load_team_member(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    participation_id: WorkspaceParticipationId,
) -> Result<Option<TeamMemberRecord>, OperationsError> {
    Ok(connection.query_row("SELECT m.workspace_id,m.team_id,m.participation_id,m.state,m.revision,m.joined_at_ms,m.left_at_ms,p.agent_profile_id,m.role FROM ops_team_members AS m LEFT JOIN ops_workspace_participations AS p ON p.workspace_id=m.workspace_id AND p.participation_id=m.participation_id WHERE m.workspace_id=?1 AND m.team_id=?2 AND m.participation_id=?3 LIMIT 1", params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes(), participation_id.into_uuid().as_bytes()], decode_team_member_row).optional()?)
}

fn decode_team_member_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TeamMemberRecord> {
    let workspace = WorkspaceId::from_uuid(Uuid::from_bytes(
        wire::id(row.get(0)?).map_err(to_sql_error)?,
    ));
    let team_id = Uuid::from_bytes(wire::id(row.get(1)?).map_err(to_sql_error)?);
    let participation_id = WorkspaceParticipationId::from_uuid(Uuid::from_bytes(
        wire::id(row.get(2)?).map_err(to_sql_error)?,
    ))
    .map_err(to_sql_error)?;
    let profile_id = row
        .get::<_, Option<Vec<u8>>>(7)?
        .map(|value| wire::id(value).map(Uuid::from_bytes))
        .transpose()
        .map_err(to_sql_error)?;
    Ok(TeamMemberRecord {
        workspace_id: workspace,
        team_id,
        participation_id,
        profile_id,
        role: row.get(8)?,
        state: TeamMembershipState::decode(&row.get::<_, String>(3)?).map_err(to_sql_error)?,
        revision: decode_revision(row.get(4)?).map_err(to_sql_error)?,
        joined_at: decode_timestamp(row.get(5)?).map_err(to_sql_error)?,
        left_at: row
            .get::<_, Option<i64>>(6)?
            .map(decode_timestamp)
            .transpose()
            .map_err(to_sql_error)?,
    })
}

fn decode_optional_uuid(value: Option<Vec<u8>>) -> Result<Option<Uuid>, OperationsError> {
    value
        .map(|bytes| wire::id(bytes).map(Uuid::from_bytes))
        .transpose()
}

fn decode_optional_participation(
    value: Option<Vec<u8>>,
) -> Result<Option<WorkspaceParticipationId>, OperationsError> {
    value
        .map(|bytes| {
            wire::id(bytes).and_then(|id| WorkspaceParticipationId::from_uuid(Uuid::from_bytes(id)))
        })
        .transpose()
}

fn decode_optional_session(
    value: Option<Vec<u8>>,
) -> Result<Option<AgentSessionId>, OperationsError> {
    value
        .map(|bytes| wire::id(bytes).map(|id| AgentSessionId::from_uuid(Uuid::from_bytes(id))))
        .transpose()
}

fn decode_revision(value: i64) -> Result<u64, OperationsError> {
    let value = u64::try_from(value).map_err(|_| OperationsError::Corrupt("team revision"))?;
    if value == 0 {
        return Err(OperationsError::Corrupt("team revision"));
    }
    Ok(value)
}

fn decode_timestamp(value: i64) -> Result<TimestampMillis, OperationsError> {
    TimestampMillis::new(value).map_err(|_| OperationsError::Corrupt("team timestamp"))
}

fn to_sql_error(error: OperationsError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Blob, Box::new(error))
}

fn team_exists(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<bool, OperationsError> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_teams WHERE workspace_id=?1 AND team_id=?2)",
        params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()],
        |row| row.get(0),
    )?)
}

fn team_exists_any(connection: &Connection, team_id: Uuid) -> Result<bool, OperationsError> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_teams WHERE team_id=?1)",
        params![team_id.as_bytes()],
        |row| row.get(0),
    )?)
}

fn ensure_participation(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participation_id: WorkspaceParticipationId,
) -> Result<(), OperationsError> {
    let exists: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM ops_workspace_participations WHERE workspace_id=?1 AND participation_id=?2)", params![workspace_id.into_uuid().as_bytes(), participation_id.into_uuid().as_bytes()], |row| row.get(0))?;
    if exists {
        Ok(())
    } else {
        Err(OperationsError::Conflict(
            "team participant workspace scope",
        ))
    }
}

fn validate_request_id(value: Uuid) -> Result<(), OperationsError> {
    if value.is_nil() {
        Err(OperationsError::Conflict("team request identity"))
    } else {
        Ok(())
    }
}

fn validate_team_id(value: Uuid) -> Result<(), OperationsError> {
    if value.is_nil() {
        Err(OperationsError::Conflict("team identity"))
    } else {
        Ok(())
    }
}

fn validate_revision(value: u64) -> Result<(), OperationsError> {
    if value == 0 {
        Err(OperationsError::Conflict("team revision"))
    } else {
        Ok(())
    }
}

fn increment_revision(value: u64) -> Result<u64, OperationsError> {
    value
        .checked_add(1)
        .ok_or(OperationsError::Bounds("team revision"))
}

fn bounded_limit(value: u32, max: usize, label: &'static str) -> Result<usize, OperationsError> {
    let value = usize::try_from(value).map_err(|_| OperationsError::Bounds(label))?;
    if value == 0 || value > max {
        return Err(OperationsError::Bounds(label));
    }
    Ok(value)
}

fn validate_text(
    value: &str,
    max: usize,
    label: &'static str,
    nonempty: bool,
) -> Result<(), OperationsError> {
    if (nonempty && value.trim().is_empty()) || value.len() > max {
        Err(OperationsError::Bounds(label))
    } else {
        Ok(())
    }
}

pub(crate) fn ensure_verified_workspace(
    connection: &Connection,
    workspace_id: WorkspaceId,
) -> Result<(), OperationsError> {
    let verified: bool = connection.query_row(
		"SELECT EXISTS(SELECT 1 FROM ops_imports AS i JOIN ops_catalog_identity AS c ON c.singleton=1 AND c.destination_id=i.destination_id WHERE i.workspace_id=?1 AND i.phase=2)",
		params![workspace_id.into_uuid().as_bytes()],
		|row| row.get(0),
	)?;
    if verified {
        Ok(())
    } else {
        Err(OperationsError::Conflict(
            "team workspace import not verified",
        ))
    }
}

fn commit_team(
    transaction: Transaction<'_>,
    workspace_id: WorkspaceId,
    request_id: Uuid,
) -> Result<(), OperationsError> {
    transaction
        .commit()
        .map_err(|source| OperationsError::OperationCommitUnconfirmed {
            workspace_id,
            request_id,
            source,
        })
}

fn commit_team_read(transaction: Transaction<'_>) -> Result<(), OperationsError> {
    transaction.commit().map_err(OperationsError::Sqlite)
}

fn load_request(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<RawRequest>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT operation,workspace_id,team_id,actor_key,actor_display_name,actor_participation_id,actor_profile_id,actor_participant_id,actor_session_id,name,purpose,expected_team_revision,result_team_state,result_team_revision,result_team_updated_at_ms,result_membership_state,result_membership_participation_id,result_membership_revision,result_membership_joined_at_ms,result_membership_left_at_ms,created,observed_at_ms,request_actor_kind,request_actor_activity_id,request_actor_agent_id,request_leadership_revision,role,result_membership_role FROM ops_team_requests WHERE request_id=?1 LIMIT 1",
            params![request_id.as_bytes()],
            decode_team_request_row,
        )
        .optional()?;
    let Some(mut raw) = raw else {
        return Ok(None);
    };
    let (roster, initial_members) = load_team_create_roster(
        connection,
        request_id,
        raw.fingerprint.workspace_id,
        raw.fingerprint.team_id,
    )?;
    if raw.fingerprint.operation == TeamOperation::Create {
        if raw.fingerprint.request_actor.is_some()
            && !(MIN_FORMING_TEAM_MEMBERS..=MAX_INITIAL_TEAM_MEMBERS).contains(&roster.len())
        {
            return Err(OperationsError::Corrupt("team create roster evidence"));
        }
        raw.fingerprint.initial_roster = roster;
        raw.initial_members = initial_members;
    } else if !roster.is_empty() {
        return Err(OperationsError::Corrupt("non-create Team roster evidence"));
    }
    Ok(Some(raw))
}

fn decode_team_request_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawRequest> {
    let workspace_id = WorkspaceId::from_uuid(Uuid::from_bytes(
        wire::id(row.get(1)?).map_err(to_sql_error)?,
    ));
    let team_id = Uuid::from_bytes(wire::id(row.get(2)?).map_err(to_sql_error)?);
    let actor = TeamCreator {
        key: row.get(3)?,
        display_name: row.get(4)?,
        participation_id: decode_optional_participation(row.get(5)?).map_err(to_sql_error)?,
        profile_id: decode_optional_uuid(row.get(6)?).map_err(to_sql_error)?,
        participant_id: decode_optional_uuid(row.get(7)?).map_err(to_sql_error)?,
        session_id: decode_optional_session(row.get(8)?).map_err(to_sql_error)?,
    };
    let result_membership_role: Option<String> = row.get(27)?;
    if let Some(role) = result_membership_role.as_deref() {
        validate_text(role, MAX_TEAM_ROLE_BYTES, "team role", true).map_err(to_sql_error)?;
    }
    let result_membership =
        decode_request_membership(row, workspace_id, team_id, &actor, result_membership_role)?;
    let request_actor = decode_request_actor(
        row.get::<_, Option<String>>(22)?.as_deref(),
        row.get(23)?,
        row.get(24)?,
        actor.session_id,
    )
    .map_err(to_sql_error)?;
    Ok(RawRequest {
        fingerprint: RequestFingerprint {
            operation: TeamOperation::decode(&row.get::<_, String>(0)?).map_err(to_sql_error)?,
            workspace_id,
            team_id,
            actor,
            name: row.get(9)?,
            purpose: row.get(10)?,
            expected_revision: row
                .get::<_, Option<i64>>(11)?
                .map(decode_revision)
                .transpose()
                .map_err(to_sql_error)?,
            request_actor,
            expected_sender_leadership_revision: row.get(25)?,
            role: row.get(26)?,
            initial_roster: Vec::new(),
        },
        result_team_state: TeamState::decode(&row.get::<_, String>(12)?).map_err(to_sql_error)?,
        result_team_revision: decode_revision(row.get(13)?).map_err(to_sql_error)?,
        result_team_updated_at: decode_timestamp(row.get(14)?).map_err(to_sql_error)?,
        result_membership,
        initial_members: Vec::new(),
        created: row.get::<_, i64>(20)? != 0,
    })
}

fn decode_request_membership(
    row: &rusqlite::Row<'_>,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    actor: &TeamCreator,
    role: Option<String>,
) -> rusqlite::Result<Option<TeamMemberRecord>> {
    let membership_state: Option<String> = row.get(15)?;
    match membership_state {
        Some(state) => {
            let participation_id = decode_optional_participation(row.get(16)?)
                .map_err(to_sql_error)?
                .ok_or_else(|| to_sql_error(OperationsError::Corrupt("team request membership")))?;
            Ok(Some(TeamMemberRecord {
                workspace_id,
                team_id,
                profile_id: (actor.participation_id == Some(participation_id))
                    .then_some(actor.profile_id)
                    .flatten(),
                participation_id,
                role,
                state: TeamMembershipState::decode(&state).map_err(to_sql_error)?,
                revision: decode_revision(row.get(17)?).map_err(to_sql_error)?,
                joined_at: decode_timestamp(row.get(18)?).map_err(to_sql_error)?,
                left_at: row
                    .get::<_, Option<i64>>(19)?
                    .map(decode_timestamp)
                    .transpose()
                    .map_err(to_sql_error)?,
            }))
        }
        None if role.is_none() => Ok(None),
        None => Err(to_sql_error(OperationsError::Corrupt(
            "team request membership role",
        ))),
    }
}

fn decode_request_actor(
    kind: Option<&str>,
    activity_id: Option<Vec<u8>>,
    agent_id: Option<Vec<u8>>,
    session_id: Option<AgentSessionId>,
) -> Result<Option<OperationalActor>, OperationsError> {
    match (kind, activity_id, agent_id, session_id) {
        (None, None, None, _) => Ok(None),
        (Some("OPERATOR"), None, None, None) => Ok(Some(OperationalActor::LocalOperator)),
        (Some("PARTICIPANT"), Some(activity), Some(agent), Some(session)) => {
            Ok(Some(OperationalActor::Participant {
                activity_id: ActivityId::from_uuid(Uuid::from_bytes(wire::id(activity)?)),
                agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(agent)?)),
                session_id: session,
            }))
        }
        _ => Err(OperationsError::Corrupt("team request actor identity")),
    }
}

fn load_team_create_roster(
    connection: &Connection,
    request_id: Uuid,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<(Vec<TeamRosterMemberInput>, Vec<TeamMemberRecord>), OperationsError> {
    let mut statement = connection.prepare(
        "SELECT ordinal,activity_id,agent_id,session_id,participation_id,participant_id,profile_id,role,membership_revision,joined_at_ms FROM ops_team_create_roster WHERE request_id=?1 AND workspace_id=?2 AND team_id=?3 ORDER BY ordinal",
    )?;
    let rows = statement.query_map(
        params![
            request_id.as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes()
        ],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, Vec<u8>>(3)?,
                row.get::<_, Vec<u8>>(4)?,
                row.get::<_, Vec<u8>>(5)?,
                row.get::<_, Vec<u8>>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, i64>(8)?,
                row.get::<_, i64>(9)?,
            ))
        },
    )?;
    let mut roster = Vec::new();
    let mut members = Vec::new();
    let mut participants = Vec::new();
    let mut participations = Vec::new();
    for row in rows {
        let (
            ordinal,
            activity,
            agent,
            session,
            participation,
            participant,
            profile,
            role,
            revision,
            joined_at,
        ) = row?;
        if ordinal
            != i64::try_from(roster.len())
                .map_err(|_| OperationsError::Bounds("team roster ordinal"))?
        {
            return Err(OperationsError::Corrupt("team roster order"));
        }
        validate_text(&role, MAX_TEAM_ROLE_BYTES, "team role", true)?;
        let activity_id = ActivityId::from_uuid(Uuid::from_bytes(wire::id(activity)?));
        let agent_id = AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(agent)?));
        let session_id = AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(session)?));
        let participation_id =
            WorkspaceParticipationId::from_uuid(Uuid::from_bytes(wire::id(participation)?))?;
        let participant_id = Uuid::from_bytes(wire::id(participant)?);
        let profile_id = Uuid::from_bytes(wire::id(profile)?);
        if participants.contains(&participant_id) || participations.contains(&participation_id) {
            return Err(OperationsError::Corrupt(
                "duplicate persisted Team roster member",
            ));
        }
        participants.push(participant_id);
        participations.push(participation_id);
        roster.push(TeamRosterMemberInput {
            activity_id,
            agent_id,
            session_id,
            role: role.clone(),
        });
        members.push(TeamMemberRecord {
            workspace_id,
            team_id,
            participation_id,
            profile_id: Some(profile_id),
            role: Some(role),
            state: TeamMembershipState::Joined,
            revision: decode_revision(revision)?,
            joined_at: decode_timestamp(joined_at)?,
            left_at: None,
        });
    }
    drop(statement);
    Ok((roster, members))
}

fn insert_request(
    transaction: &Transaction<'_>,
    request_id: Uuid,
    fingerprint: &RequestFingerprint,
    record: &TeamMutationRecord,
    observed_at_ms: i64,
) -> Result<(), OperationsError> {
    let membership = record.membership.as_ref();
    let (request_actor_kind, request_actor_activity, request_actor_agent) =
        request_actor_columns(fingerprint.request_actor);
    transaction.execute(
		"INSERT INTO ops_team_requests(request_id,operation,workspace_id,team_id,actor_key,actor_display_name,actor_participation_id,actor_profile_id,actor_participant_id,actor_session_id,name,purpose,expected_team_revision,result_team_state,result_team_revision,result_team_updated_at_ms,result_membership_state,result_membership_participation_id,result_membership_revision,result_membership_joined_at_ms,result_membership_left_at_ms,created,observed_at_ms,request_actor_kind,request_actor_activity_id,request_actor_agent_id,request_leadership_revision,role,result_membership_role) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28,?29)",
		params![
			request_id.as_bytes(),
			fingerprint.operation.encode(),
			fingerprint.workspace_id.into_uuid().as_bytes(),
			fingerprint.team_id.as_bytes(),
			fingerprint.actor.key.clone(),
			fingerprint.actor.display_name.clone(),
			fingerprint.actor.participation_id.map(|id| id.into_uuid().into_bytes().to_vec()),
			fingerprint.actor.profile_id.map(|id| id.into_bytes().to_vec()),
			fingerprint.actor.participant_id.map(|id| id.into_bytes().to_vec()),
			fingerprint.actor.session_id.map(|id| id.into_uuid().into_bytes().to_vec()),
			fingerprint.name.clone(),
			fingerprint.purpose.clone(),
            fingerprint.expected_revision.map(i64::try_from).transpose().map_err(|_| OperationsError::Bounds("team revision"))?,
			record.team.state.encode(),
			i64::try_from(record.team.revision).map_err(|_| OperationsError::Bounds("team revision"))?,
			record.team.updated_at.get(),
			membership.map(|value| value.state.encode()),
			membership.map(|value| value.participation_id.into_uuid().into_bytes().to_vec()),
			membership.map(|value| i64::try_from(value.revision)).transpose().map_err(|_| OperationsError::Bounds("team membership revision"))?,
			membership.map(|value| value.joined_at.get()),
            membership.and_then(|value| value.left_at.map(TimestampMillis::get)),
            i32::from(record.created),
			observed_at_ms,
			request_actor_kind,
			request_actor_activity,
			request_actor_agent,
			fingerprint.expected_sender_leadership_revision,
			fingerprint.role.as_deref(),
			membership.and_then(|value| value.role.as_deref()),
		],
	)?;
    Ok(())
}

fn request_actor_columns(
    actor: Option<OperationalActor>,
) -> (Option<&'static str>, Option<Vec<u8>>, Option<Vec<u8>>) {
    match actor {
        None => (None, None, None),
        Some(OperationalActor::LocalOperator) => (Some("OPERATOR"), None, None),
        Some(OperationalActor::Participant {
            activity_id,
            agent_id,
            ..
        }) => (
            Some("PARTICIPANT"),
            Some(activity_id.into_uuid().into_bytes().to_vec()),
            Some(agent_id.into_uuid().into_bytes().to_vec()),
        ),
    }
}

fn insert_membership_event(
    transaction: &Transaction<'_>,
    request_id: Uuid,
    membership: &TeamMemberRecord,
    actor: &ResolvedOperationalActor,
    observed_at_ms: i64,
) -> Result<(), OperationsError> {
    transaction.execute(
        "INSERT INTO ops_team_member_events(request_id,workspace_id,team_id,participation_id,state,membership_revision,actor_key,actor_display_name,actor_participation_id,actor_profile_id,actor_participant_id,actor_session_id,observed_at_ms,role) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
		params![
			request_id.as_bytes(),
			membership.workspace_id.into_uuid().as_bytes(),
			membership.team_id.as_bytes(),
			membership.participation_id.into_uuid().as_bytes(),
			membership.state.encode(),
			i64::try_from(membership.revision).map_err(|_| OperationsError::Bounds("team membership revision"))?,
			actor.key(),
			actor.display_name.as_str(),
			actor.participation_id.map(|id| id.into_uuid().into_bytes().to_vec()),
			actor.profile_id.map(|id| id.into_bytes().to_vec()),
			actor.participant_id.map(|id| id.into_bytes().to_vec()),
            actor.session_id.map(|id| id.into_uuid().into_bytes().to_vec()),
            observed_at_ms,
            membership.role.as_deref(),
		],
	)?;
    Ok(())
}

fn replay_or_conflict(
    transaction: Transaction<'_>,
    workspace_id: WorkspaceId,
    request_id: Uuid,
    existing: &RawRequest,
    fingerprint: &RequestFingerprint,
) -> Result<TeamMutationRecord, OperationsError> {
    if !existing.fingerprint.semantically_matches(fingerprint) {
        return Err(OperationsError::Conflict("team request replay"));
    }
    let record = stored_mutation_record(&transaction, existing)?;
    commit_team(transaction, workspace_id, request_id)?;
    Ok(record)
}

fn stored_mutation_record(
    connection: &Connection,
    existing: &RawRequest,
) -> Result<TeamMutationRecord, OperationsError> {
    let row = load_team_row(
        connection,
        existing.fingerprint.workspace_id,
        existing.fingerprint.team_id,
    )?
    .ok_or(OperationsError::Corrupt("team request team missing"))?;
    let team = row.record_at(
        existing.result_team_state,
        existing.result_team_revision,
        existing.result_team_updated_at,
    )?;
    Ok(TeamMutationRecord {
        team,
        membership: existing.result_membership.clone(),
        initial_members: existing.initial_members.clone(),
        created: existing.created,
        replayed: true,
    })
}

pub(crate) fn read_team_mutation(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    request_id: Uuid,
) -> Result<Option<TeamMutationRecord>, OperationsError> {
    validate_team_id(team_id)?;
    validate_request_id(request_id)?;
    let tx = connection.unchecked_transaction()?;
    ensure_verified_workspace(&tx, workspace_id)?;
    let result = read_team_mutation_in_transaction(&tx, workspace_id, team_id, request_id)?;
    commit_team_read(tx)?;
    Ok(result)
}

fn read_team_mutation_in_transaction(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    request_id: Uuid,
) -> Result<Option<TeamMutationRecord>, OperationsError> {
    validate_team_id(team_id)?;
    validate_request_id(request_id)?;
    match load_request(connection, request_id)? {
        None => Ok(None),
        Some(existing) => {
            if existing.fingerprint.workspace_id != workspace_id
                || existing.fingerprint.team_id != team_id
            {
                return Err(OperationsError::Conflict("Team request readback scope"));
            }
            Ok(Some(stored_mutation_record(connection, &existing)?))
        }
    }
}

impl crate::OperationsCatalog {
    /// Read the current Team, a bounded member page, and an optional historical
    /// mutation receipt from one verified Workspace snapshot.
    ///
    /// The snapshot retains complete owner records so transport projections
    /// preserve creator, roster, revision, and receipt data without rebuilding
    /// them from separate reads.
    ///
    /// # Errors
    /// Rejects invalid identities or limits, unverified/cross-Workspace scope,
    /// corrupt receipts, and storage failures.
    pub fn team_query_snapshot(
        &self,
        workspace_id: WorkspaceId,
        team_id: Uuid,
        member_limit: u32,
        request_id: Option<Uuid>,
    ) -> Result<TeamQuerySnapshot, OperationsError> {
        read_team_query_snapshot(
            &self.connection,
            workspace_id,
            team_id,
            member_limit,
            request_id,
        )
    }
}

fn read_team_query_snapshot(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    member_limit: u32,
    request_id: Option<Uuid>,
) -> Result<TeamQuerySnapshot, OperationsError> {
    validate_team_id(team_id)?;
    bounded_limit(member_limit, MAX_TEAM_MEMBERS_PER_READ, "team member list")?;
    if let Some(request_id) = request_id {
        validate_request_id(request_id)?;
    }

    let transaction = connection.unchecked_transaction()?;
    ensure_verified_workspace(&transaction, workspace_id)?;
    // Keep the native get ordering: resolve the requested receipt independently
    // of whether a current Team row is present.
    let mutation = request_id
        .map(|request_id| {
            read_team_mutation_in_transaction(&transaction, workspace_id, team_id, request_id)
        })
        .transpose()?
        .flatten();
    let team = read_team_in_transaction(&transaction, workspace_id, team_id)?;
    let members = if team.is_some() {
        list_team_members_in_transaction(&transaction, workspace_id, team_id, member_limit)?
    } else {
        TeamReadSet {
            items: Vec::new(),
            truncated: false,
        }
    };
    commit_team_read(transaction)?;
    Ok(TeamQuerySnapshot {
        team,
        members,
        mutation,
    })
}

impl TeamRow {
    fn record_at(
        self,
        state: TeamState,
        revision: u64,
        updated_at: TimestampMillis,
    ) -> Result<TeamRecord, OperationsError> {
        if revision == 0 || updated_at.get() < self.created_at.get() {
            return Err(OperationsError::Corrupt("team request result"));
        }
        Ok(TeamRecord {
            team_id: self.team_id,
            workspace_id: self.workspace_id,
            name: self.name,
            purpose: self.purpose,
            state,
            revision,
            created_by: self.created_by,
            created_at: self.created_at,
            updated_at,
            formed_at: self.formed_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId};
    use rusqlite::Connection;
    use std::error::Error;
    use std::path::PathBuf;

    const WORKSPACE: Uuid = Uuid::from_u128(0x1000);
    const WORKSPACE_TWO: Uuid = Uuid::from_u128(0x2000);
    const PROFILE: Uuid = Uuid::from_u128(0x3000);
    const PARTICIPATION: Uuid = Uuid::from_u128(0x4000);
    const PARTICIPANT: Uuid = Uuid::from_u128(0x5000);
    const ACTIVITY: Uuid = Uuid::from_u128(0x6000);
    const AGENT: Uuid = Uuid::from_u128(0x7000);
    const SESSION: Uuid = Uuid::from_u128(0x8000);
    const PROFILE_TWO: Uuid = Uuid::from_u128(0x3100);
    const PARTICIPATION_TWO: Uuid = Uuid::from_u128(0x4100);
    const PARTICIPANT_TWO: Uuid = Uuid::from_u128(0x5100);
    const ACTIVITY_TWO: Uuid = Uuid::from_u128(0x6100);
    const AGENT_TWO: Uuid = Uuid::from_u128(0x7100);
    const SESSION_TWO: Uuid = Uuid::from_u128(0x8100);
    const PROFILE_THREE: Uuid = Uuid::from_u128(0x3200);
    const PARTICIPATION_THREE: Uuid = Uuid::from_u128(0x4200);
    const PARTICIPANT_THREE: Uuid = Uuid::from_u128(0x5200);
    const ACTIVITY_THREE: Uuid = Uuid::from_u128(0x6200);
    const AGENT_THREE: Uuid = Uuid::from_u128(0x7200);
    const SESSION_THREE: Uuid = Uuid::from_u128(0x8200);

    fn workspace(value: Uuid) -> WorkspaceId {
        WorkspaceId::from_uuid(value)
    }

    fn timestamp(value: i64) -> Result<TimestampMillis, OperationsError> {
        TimestampMillis::new(value).map_err(|_| OperationsError::Conflict("test timestamp"))
    }

    fn local() -> OperationalActor {
        OperationalActor::LocalOperator
    }

    fn participant() -> OperationalActor {
        OperationalActor::Participant {
            activity_id: ActivityId::from_uuid(ACTIVITY),
            agent_id: AgentInstanceId::from_uuid(AGENT),
            session_id: AgentSessionId::from_uuid(SESSION),
        }
    }

    fn create_connection(path: Option<&PathBuf>) -> Result<Connection, Box<dyn Error>> {
        let fresh = path.is_none_or(|value| !value.exists());
        let connection = match path {
            Some(path) => Connection::open(path)?,
            None => Connection::open_in_memory()?,
        };
        connection.pragma_update(None, "foreign_keys", true)?;
        if fresh {
            connection
				.execute_batch(
					"CREATE TABLE ops_catalog_identity (singleton INTEGER PRIMARY KEY CHECK(singleton=1), destination_id BLOB NOT NULL UNIQUE CHECK(length(destination_id)=16));
				 CREATE TABLE ops_imports (migration_id BLOB PRIMARY KEY, workspace_id BLOB NOT NULL, destination_id BLOB NOT NULL, phase INTEGER NOT NULL);
				 CREATE TABLE ops_workspace_participations (workspace_id BLOB NOT NULL, agent_profile_id BLOB NOT NULL, participation_id BLOB NOT NULL, created_at_ms INTEGER NOT NULL, revision INTEGER NOT NULL, PRIMARY KEY(workspace_id,agent_profile_id), UNIQUE(workspace_id,participation_id));
				 CREATE TABLE ops_workspace_participation_mappings (request_id BLOB PRIMARY KEY, workspace_id BLOB NOT NULL, participation_id BLOB NOT NULL, agent_profile_id BLOB NOT NULL, profile_revision INTEGER NOT NULL, profile_mapping_request_id BLOB NOT NULL, session_id BLOB NOT NULL, session_revision INTEGER NOT NULL, participant_id BLOB NOT NULL, participant_membership_revision INTEGER NOT NULL, mapped_at_ms INTEGER NOT NULL);
				 CREATE TABLE ops_logical_members (workspace_id BLOB NOT NULL, participant_id BLOB NOT NULL, session_id BLOB NOT NULL, display_name TEXT NOT NULL, PRIMARY KEY(workspace_id,participant_id));
				 CREATE TABLE ops_current_effect_bindings (workspace_id BLOB NOT NULL, participant_id BLOB NOT NULL, activity_id BLOB NOT NULL, session_id BLOB NOT NULL, agent_id BLOB NOT NULL, source_kind INTEGER NOT NULL, source_request_id BLOB NOT NULL, PRIMARY KEY(workspace_id,agent_id));",
				)
				?;
        }
        if fresh {
            create_schema(&connection)?;
            crate::leadership::create_schema(&connection)?;
            seed_workspace(&connection, workspace(WORKSPACE))?;
            seed_workspace(&connection, workspace(WORKSPACE_TWO))?;
            seed_participant(&connection, workspace(WORKSPACE))?;
            seed_participant_identity(
                &connection,
                workspace(WORKSPACE),
                PROFILE_TWO,
                PARTICIPATION_TWO,
                PARTICIPANT_TWO,
                ACTIVITY_TWO,
                AGENT_TWO,
                SESSION_TWO,
                Uuid::from_u128(0xB100),
            )?;
            seed_participant_identity(
                &connection,
                workspace(WORKSPACE),
                PROFILE_THREE,
                PARTICIPATION_THREE,
                PARTICIPANT_THREE,
                ACTIVITY_THREE,
                AGENT_THREE,
                SESSION_THREE,
                Uuid::from_u128(0xB200),
            )?;
        }
        Ok(connection)
    }

    fn seed_workspace(
        connection: &Connection,
        workspace: WorkspaceId,
    ) -> Result<(), Box<dyn Error>> {
        let destination = Uuid::from_u128(0x9000);
        let migration = Uuid::from_u128(workspace.into_uuid().as_u128() + 0xA000);
        connection.execute(
            "INSERT OR IGNORE INTO ops_catalog_identity(singleton,destination_id) VALUES(1,?1)",
            params![destination.as_bytes()],
        )?;
        connection
			.execute(
				"INSERT OR IGNORE INTO ops_imports(migration_id,workspace_id,destination_id,phase) VALUES(?1,?2,?3,2)",
				params![migration.as_bytes(), workspace.into_uuid().as_bytes(), destination.as_bytes()],
			)
			?;
        Ok(())
    }

    fn seed_participant(
        connection: &Connection,
        workspace: WorkspaceId,
    ) -> Result<(), Box<dyn Error>> {
        seed_participant_identity(
            connection,
            workspace,
            PROFILE,
            PARTICIPATION,
            PARTICIPANT,
            ACTIVITY,
            AGENT,
            SESSION,
            Uuid::from_u128(0xB000),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn seed_participant_identity(
        connection: &Connection,
        workspace: WorkspaceId,
        profile: Uuid,
        participation: Uuid,
        participant: Uuid,
        activity: Uuid,
        agent: Uuid,
        session: Uuid,
        request_seed: Uuid,
    ) -> Result<(), Box<dyn Error>> {
        connection
			.execute(
				"INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,1,1)",
				params![workspace.into_uuid().as_bytes(), profile.as_bytes(), participation.as_bytes()],
			)
			?;
        connection
			.execute(
				"INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,1)",
				params![request_seed.as_bytes(), workspace.into_uuid().as_bytes(), participation.as_bytes(), profile.as_bytes(), Uuid::from_u128(request_seed.as_u128() + 1).as_bytes(), session.as_bytes(), participant.as_bytes()],
			)
			?;
        connection
			.execute(
				"INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,display_name) VALUES(?1,?2,?3,'Participant')",
				params![workspace.into_uuid().as_bytes(), participant.as_bytes(), session.as_bytes()],
			)
			?;
        connection
			.execute(
				"INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
				params![workspace.into_uuid().as_bytes(), participant.as_bytes(), activity.as_bytes(), session.as_bytes(), agent.as_bytes(), Uuid::from_u128(request_seed.as_u128() + 2).as_bytes()],
			)
			?;
        Ok(())
    }

    fn create_command(
        request_id: Uuid,
        workspace_id: WorkspaceId,
    ) -> Result<TeamCreateCommand, OperationsError> {
        Ok(TeamCreateCommand {
            request_id,
            team_id: Uuid::from_u128(0xA100),
            workspace_id,
            actor: local(),
            expected_sender_leadership_revision: None,
            name: "Review Team".to_owned(),
            purpose: "Review durable work".to_owned(),
            initial_members: vec![
                TeamRosterMemberInput {
                    activity_id: ActivityId::from_uuid(ACTIVITY_TWO),
                    agent_id: AgentInstanceId::from_uuid(AGENT_TWO),
                    session_id: AgentSessionId::from_uuid(SESSION_TWO),
                    role: "reviewer".to_owned(),
                },
                TeamRosterMemberInput {
                    activity_id: ActivityId::from_uuid(ACTIVITY_THREE),
                    agent_id: AgentInstanceId::from_uuid(AGENT_THREE),
                    session_id: AgentSessionId::from_uuid(SESSION_THREE),
                    role: "implementer".to_owned(),
                },
            ],
            observed_at: timestamp(10)?,
        })
    }

    #[test]
    fn formation_rejects_invalid_rosters_without_partial_team_or_member_effects()
    -> Result<(), Box<dyn Error>> {
        let mut connection = create_connection(None)?;
        let base = create_command(Uuid::from_u128(0xAD01), workspace(WORKSPACE))?;
        let mut blank_purpose = base.clone();
        blank_purpose.purpose = "  ".to_owned();
        let mut one_member = base.clone();
        one_member.initial_members.truncate(1);
        let mut repeated_agent = base.clone();
        repeated_agent.initial_members[1] = repeated_agent.initial_members[0].clone();
        repeated_agent.initial_members[1].role =
            "a different role is still the same Agent".to_owned();
        let mut missing_role = base.clone();
        missing_role.initial_members[0].role = " ".to_owned();
        let mut stale_second = base.clone();
        stale_second.initial_members[1].session_id =
            AgentSessionId::from_uuid(Uuid::from_u128(0xDEAD));
        let mut not_leader = base.clone();
        not_leader.actor = participant();
        not_leader.expected_sender_leadership_revision = Some(1);
        for command in [
            blank_purpose,
            one_member,
            repeated_agent,
            missing_role,
            stale_second,
            not_leader,
        ] {
            assert!(matches!(
                create_team(&mut connection, command),
                Err(OperationsError::Bounds(_) | OperationsError::Conflict(_))
            ));
            for table in [
                "ops_teams",
                "ops_team_members",
                "ops_team_requests",
                "ops_team_member_events",
                "ops_team_create_roster",
            ] {
                assert_eq!(
                    connection
                        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| row
                            .get::<_, i64>(0))?,
                    0
                );
            }
        }
        let mut valid = base;
        valid.initial_members[0].role = "leader".to_owned();
        let created = create_team(&mut connection, valid)?;
        assert_eq!(created.initial_members().len(), 2);
        assert_eq!(created.team().formed_at(), Some(timestamp(10)?));
        assert_eq!(
            connection.query_row("SELECT COUNT(*) FROM ops_team_member_events", [], |row| row
                .get::<_, i64>(0))?,
            2
        );
        assert_eq!(
            connection.query_row("SELECT COUNT(*) FROM ops_leadership", [], |row| row
                .get::<_, i64>(0))?,
            0
        );
        Ok(())
    }

    #[test]
    fn formation_creator_requires_current_leadership_but_replays_original_admission()
    -> Result<(), Box<dyn Error>> {
        let mut connection = create_connection(None)?;
        crate::election_subject::create_schema(&connection)?;
        connection.execute(
            "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,0,NULL,NULL,'ACTIVE',?2,1,NULL,NULL,1)",
            params![workspace(WORKSPACE).into_uuid().as_bytes(),PARTICIPANT.as_bytes()],
        )?;
        connection.execute(
            "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,0,zeroblob(16),zeroblob(16),1,'CONFIRMED',?2,1,1)",
            params![workspace(WORKSPACE).into_uuid().as_bytes(),PARTICIPANT.as_bytes()],
        )?;
        let mut command = create_command(Uuid::from_u128(0xAD02), workspace(WORKSPACE))?;
        command.actor = participant();
        command.expected_sender_leadership_revision = Some(2);
        assert!(matches!(
            create_team(&mut connection, command.clone()),
            Err(OperationsError::Conflict(
                "team create Workspace leadership changed"
            ))
        ));
        command.expected_sender_leadership_revision = Some(1);
        let created = create_team(&mut connection, command.clone())?;
        assert!(created.created());
        connection.execute(
            "UPDATE ops_leadership SET leadership_revision=2 WHERE workspace_id=?1",
            params![workspace(WORKSPACE).into_uuid().as_bytes()],
        )?;
        command.observed_at = timestamp(99)?;
        let replay = create_team(&mut connection, command)?;
        assert!(replay.replayed());
        assert_eq!(replay.initial_members(), created.initial_members());
        assert_eq!(replay.team(), created.team());
        Ok(())
    }

    #[test]
    fn create_replay_and_reopen_preserves_creator_and_revision() -> Result<(), Box<dyn Error>> {
        let path = std::env::temp_dir().join(format!("heterarchy-team-{}.db", Uuid::now_v7()));
        {
            let mut connection = create_connection(Some(&path))?;
            let first = create_team(
                &mut connection,
                create_command(Uuid::from_u128(0xA101), workspace(WORKSPACE))?,
            )?;
            assert!(first.created());
            assert!(!first.replayed());
            let mut replay_command = create_command(Uuid::from_u128(0xA101), workspace(WORKSPACE))?;
            replay_command.observed_at = timestamp(99)?;
            let replay = create_team(&mut connection, replay_command)?;
            assert!(replay.replayed());
            assert_eq!(replay.team().revision(), 1);
            let mut changed = create_command(Uuid::from_u128(0xA101), workspace(WORKSPACE))?;
            changed.purpose = "Changed replay payload".to_owned();
            changed.observed_at = timestamp(100)?;
            assert!(matches!(
                create_team(&mut connection, changed),
                Err(OperationsError::Conflict("team request replay"))
            ));
        }
        {
            let connection = create_connection(Some(&path))?;
            let team = read_team(&connection, workspace(WORKSPACE), Uuid::from_u128(0xA100))?
                .ok_or_else(|| std::io::Error::other("team exists"))?;
            assert_eq!(team.name(), "Review Team");
            assert_eq!(team.created_by().key(), "operator");
        }
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn create_replay_preserves_canonical_roster_after_session_rebind() -> Result<(), Box<dyn Error>>
    {
        let mut connection = create_connection(None)?;
        let request_id = Uuid::from_u128(0xA110);
        let command = create_command(request_id, workspace(WORKSPACE))?;
        let first = create_team(&mut connection, command.clone())?;
        assert_eq!(first.initial_members().len(), MIN_FORMING_TEAM_MEMBERS);
        assert!(
            first
                .initial_members()
                .iter()
                .all(|member| member.state() == TeamMembershipState::Joined
                    && member.role().is_some())
        );
        connection.execute(
            "UPDATE ops_logical_members SET session_id=?1,display_name='Rebound roster member' WHERE workspace_id=?2 AND participant_id=?3",
            params![
                Uuid::from_u128(0x8300).as_bytes(),
                workspace(WORKSPACE).into_uuid().as_bytes(),
                PARTICIPANT_TWO.as_bytes(),
            ],
        )?;
        connection.execute(
            "DELETE FROM ops_current_effect_bindings WHERE workspace_id=?1 AND activity_id=?2 AND agent_id=?3",
            params![
                workspace(WORKSPACE).into_uuid().as_bytes(),
                ACTIVITY_TWO.as_bytes(),
                AGENT_TWO.as_bytes(),
            ],
        )?;
        let mut reordered = command.clone();
        reordered.initial_members.reverse();
        reordered.observed_at = timestamp(99)?;
        let replay = create_team(&mut connection, reordered)?;
        assert!(replay.replayed());
        assert_eq!(replay.team().team_id(), first.team().team_id());
        assert_eq!(replay.team().created_at(), timestamp(10)?);
        assert_eq!(replay.initial_members(), first.initial_members());
        let mut changed_role = command;
        changed_role.initial_members[0].role = "different-role".to_owned();
        assert!(matches!(
            create_team(&mut connection, changed_role),
            Err(OperationsError::Conflict("team request replay"))
        ));
        Ok(())
    }

    #[test]
    fn participant_self_join_leave_retains_leave_audit() -> Result<(), Box<dyn Error>> {
        let mut connection = create_connection(None)?;
        let team = create_team(
            &mut connection,
            create_command(Uuid::from_u128(0xA102), workspace(WORKSPACE))?,
        )?;
        let local_join = TeamJoinCommand {
            request_id: Uuid::from_u128(0xA103),
            workspace_id: workspace(WORKSPACE),
            team_id: team.team().team_id(),
            expected_team_revision: 1,
            actor: local(),
            role: "reviewer".to_owned(),
            observed_at: timestamp(20)?,
        };
        assert!(matches!(
            join_team(&mut connection, local_join),
            Err(OperationsError::Conflict(
                "team membership requires participant"
            ))
        ));
        let joined = join_team(
            &mut connection,
            TeamJoinCommand {
                request_id: Uuid::from_u128(0xA104),
                workspace_id: workspace(WORKSPACE),
                team_id: team.team().team_id(),
                expected_team_revision: 1,
                actor: participant(),
                role: "reviewer".to_owned(),
                observed_at: timestamp(20)?,
            },
        )?;
        let joined_membership = joined
            .membership()
            .ok_or_else(|| std::io::Error::other("joined membership"))?;
        assert_eq!(joined_membership.state(), TeamMembershipState::Joined);
        assert_eq!(joined_membership.profile_id(), Some(PROFILE));
        assert_eq!(joined_membership.role(), Some("reviewer"));
        let left = leave_team(
            &mut connection,
            TeamLeaveCommand {
                request_id: Uuid::from_u128(0xA105),
                workspace_id: workspace(WORKSPACE),
                team_id: team.team().team_id(),
                expected_team_revision: 2,
                actor: participant(),
                observed_at: timestamp(30)?,
            },
        )?;
        let left_membership = left
            .membership()
            .ok_or_else(|| std::io::Error::other("left membership"))?;
        assert_eq!(left_membership.state(), TeamMembershipState::Left);
        let members =
            list_team_members(&connection, workspace(WORKSPACE), team.team().team_id(), 10)?;
        assert_eq!(members.items().len(), 3);
        assert!(!members.truncated());
        assert_eq!(members.items()[0].state(), TeamMembershipState::Left);
        assert_eq!(members.items()[0].profile_id(), Some(PROFILE));
        assert!(matches!(
            join_team(
                &mut connection,
                TeamJoinCommand {
                    request_id: Uuid::from_u128(0xA10A),
                    workspace_id: workspace(WORKSPACE),
                    team_id: team.team().team_id(),
                    expected_team_revision: 1,
                    actor: participant(),
                    role: "stale".to_owned(),
                    observed_at: timestamp(31)?
                }
            ),
            Err(OperationsError::Conflict("team revision"))
        ));
        let events: i64 =
            connection.query_row("SELECT COUNT(*) FROM ops_team_member_events", [], |row| {
                row.get(0)
            })?;
        assert_eq!(events, 4);
        let replay = leave_team(
            &mut connection,
            TeamLeaveCommand {
                request_id: Uuid::from_u128(0xA105),
                workspace_id: workspace(WORKSPACE),
                team_id: team.team().team_id(),
                expected_team_revision: 2,
                actor: participant(),
                observed_at: timestamp(30)?,
            },
        )?;
        assert!(replay.replayed());
        Ok(())
    }

    #[test]
    fn team_query_reads_full_snapshot_and_exact_receipt_without_mutation()
    -> Result<(), Box<dyn Error>> {
        let mut connection = create_connection(None)?;
        let request_id = Uuid::from_u128(0xA120);
        let created = create_team(
            &mut connection,
            create_command(request_id, workspace(WORKSPACE))?,
        )?;
        let team_id = created.team().team_id();
        let before_request_count: i64 =
            connection.query_row("SELECT COUNT(*) FROM ops_team_requests", [], |row| {
                row.get(0)
            })?;

        let snapshot = read_team_query_snapshot(
            &connection,
            workspace(WORKSPACE),
            team_id,
            64,
            Some(request_id),
        )?;
        let current = snapshot
            .team
            .as_ref()
            .ok_or_else(|| std::io::Error::other("current team"))?;
        assert_eq!(current.team_id(), team_id);
        assert_eq!(current.workspace_id(), workspace(WORKSPACE));
        assert_eq!(current.name(), "Review Team");
        assert_eq!(current.purpose(), "Review durable work");
        assert_eq!(current.state(), TeamState::Active);
        assert_eq!(current.revision(), 1);
        assert_eq!(current.created_by().key(), "operator");
        assert_eq!(current.created_by().display_name(), "Local operator");
        assert_eq!(current.created_by().participation_id(), None);
        assert_eq!(current.created_at(), timestamp(10)?);
        assert_eq!(current.updated_at(), timestamp(10)?);
        assert_eq!(current.formed_at(), Some(timestamp(10)?));

        assert_eq!(snapshot.members.items().len(), 2);
        assert!(!snapshot.members.truncated());
        assert!(snapshot.members.items().iter().all(|member| {
            member.workspace_id() == workspace(WORKSPACE)
                && member.team_id() == team_id
                && member.state() == TeamMembershipState::Joined
                && member.revision() == 1
                && member.left_at().is_none()
        }));
        assert_eq!(
            snapshot
                .members
                .items()
                .iter()
                .map(|member| member.participation_id().into_uuid())
                .collect::<Vec<_>>(),
            vec![PARTICIPATION_TWO, PARTICIPATION_THREE]
        );

        let receipt = snapshot
            .mutation
            .as_ref()
            .ok_or_else(|| std::io::Error::other("exact mutation receipt"))?;
        assert_eq!(receipt.team().team_id(), team_id);
        assert_eq!(receipt.team().workspace_id(), workspace(WORKSPACE));
        assert_eq!(receipt.team().revision(), 1);
        assert_eq!(receipt.initial_members().len(), 2);
        assert!(receipt.created());
        assert!(receipt.replayed());
        assert_eq!(
            receipt
                .initial_members()
                .iter()
                .map(|member| member.participation_id().into_uuid())
                .collect::<Vec<_>>(),
            vec![PARTICIPATION_TWO, PARTICIPATION_THREE]
        );

        let repeated = read_team_query_snapshot(
            &connection,
            workspace(WORKSPACE),
            team_id,
            64,
            Some(request_id),
        )?;
        assert_eq!(snapshot, repeated);
        let after_request_count: i64 =
            connection.query_row("SELECT COUNT(*) FROM ops_team_requests", [], |row| {
                row.get(0)
            })?;
        assert_eq!(after_request_count, before_request_count);

        Ok(())
    }

    #[test]
    fn team_query_rejects_cross_workspace_and_wrong_receipt_identity() -> Result<(), Box<dyn Error>>
    {
        let mut connection = create_connection(None)?;
        let request_id = Uuid::from_u128(0xA121);
        let team = create_team(
            &mut connection,
            create_command(request_id, workspace(WORKSPACE))?,
        )?;
        let team_id = team.team().team_id();

        assert!(matches!(
            read_team_query_snapshot(&connection, workspace(WORKSPACE_TWO), team_id, 64, None),
            Err(OperationsError::Conflict("team workspace scope"))
        ));
        assert!(matches!(
            read_team_query_snapshot(
                &connection,
                workspace(WORKSPACE_TWO),
                team_id,
                64,
                Some(request_id),
            ),
            Err(OperationsError::Conflict("Team request readback scope"))
        ));
        assert!(matches!(
            read_team_query_snapshot(
                &connection,
                workspace(WORKSPACE),
                Uuid::from_u128(0xA122),
                64,
                Some(request_id),
            ),
            Err(OperationsError::Conflict("Team request readback scope"))
        ));
        Ok(())
    }

    #[test]
    fn wrong_workspace_stale_revision_and_retire_actor_fail_closed() -> Result<(), Box<dyn Error>> {
        let mut connection = create_connection(None)?;
        let team = create_team(
            &mut connection,
            create_command(Uuid::from_u128(0xA106), workspace(WORKSPACE))?,
        )?;
        let mut wrong = create_command(Uuid::from_u128(0xA106), workspace(WORKSPACE_TWO))?;
        wrong.team_id = team.team().team_id();
        assert!(matches!(
            create_team(&mut connection, wrong),
            Err(OperationsError::Conflict("team request replay"))
        ));
        assert!(matches!(
            retire_team(
                &mut connection,
                TeamRetireCommand {
                    request_id: Uuid::from_u128(0xA107),
                    workspace_id: workspace(WORKSPACE),
                    team_id: team.team().team_id(),
                    expected_team_revision: 1,
                    actor: participant(),
                    observed_at: timestamp(40)?
                }
            ),
            Err(OperationsError::Conflict(
                "team retire requires local operator"
            ))
        ));
        let retired = retire_team(
            &mut connection,
            TeamRetireCommand {
                request_id: Uuid::from_u128(0xA108),
                workspace_id: workspace(WORKSPACE),
                team_id: team.team().team_id(),
                expected_team_revision: 1,
                actor: local(),
                observed_at: timestamp(40)?,
            },
        )?;
        assert_eq!(retired.team().state(), TeamState::Retired);
        assert!(matches!(
            join_team(
                &mut connection,
                TeamJoinCommand {
                    request_id: Uuid::from_u128(0xA109),
                    workspace_id: workspace(WORKSPACE),
                    team_id: team.team().team_id(),
                    expected_team_revision: 2,
                    actor: participant(),
                    role: "observer".to_owned(),
                    observed_at: timestamp(50)?
                }
            ),
            Err(OperationsError::Conflict("team retired"))
        ));
        Ok(())
    }
}
