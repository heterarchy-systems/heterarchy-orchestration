//! Shadow Workspace participation anchor for the Global Agent Profile identity cutover.
//!
//! Phase B introduces a durable `(Workspace, AgentProfile) -> WorkspaceParticipation`
//! identity without changing membership, JOINED, election, `WorkCredit`, assignment,
//! or Forge effect authority. Existing logical Session/member rows remain the
//! operational source of truth until later cutover phases.

use forge_domain::{AgentSessionId, TimestampMillis, WorkspaceId};
use heterarchy_orchestration_sessions::{
    AgentProfileId, AgentProfileLogicalSessionLink, AgentProfileRevision,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use uuid::Uuid;

use crate::{LogicalSessionIdentityProjection, OperationsCatalog, OperationsError, wire};

const WORKSPACE_PARTICIPATIONS_SQL: &str = "CREATE TABLE ops_workspace_participations (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), agent_profile_id BLOB NOT NULL CHECK(length(agent_profile_id)=16 AND agent_profile_id != zeroblob(16)), participation_id BLOB NOT NULL CHECK(length(participation_id)=16 AND participation_id != zeroblob(16)), created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0), revision INTEGER NOT NULL CHECK(revision>0), PRIMARY KEY(workspace_id,agent_profile_id), UNIQUE(workspace_id,participation_id))";
const WORKSPACE_PARTICIPATION_MAPPINGS_SQL: &str = "CREATE TABLE ops_workspace_participation_mappings (request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), participation_id BLOB NOT NULL CHECK(length(participation_id)=16 AND participation_id != zeroblob(16)), agent_profile_id BLOB NOT NULL CHECK(length(agent_profile_id)=16 AND agent_profile_id != zeroblob(16)), profile_revision INTEGER NOT NULL CHECK(profile_revision>0), profile_mapping_request_id BLOB NOT NULL CHECK(length(profile_mapping_request_id)=16 AND profile_mapping_request_id != zeroblob(16)), session_id BLOB NOT NULL CHECK(length(session_id)=16 AND session_id != zeroblob(16)), session_revision INTEGER NOT NULL CHECK(session_revision>0), participant_id BLOB NOT NULL CHECK(length(participant_id)=16 AND participant_id != zeroblob(16)), participant_membership_revision INTEGER NOT NULL CHECK(participant_membership_revision>0), mapped_at_ms INTEGER NOT NULL CHECK(mapped_at_ms>=0), UNIQUE(workspace_id,session_id), UNIQUE(workspace_id,participant_id), FOREIGN KEY(workspace_id,agent_profile_id) REFERENCES ops_workspace_participations(workspace_id,agent_profile_id), FOREIGN KEY(workspace_id,participant_id) REFERENCES ops_logical_members(workspace_id,participant_id))";
pub const MAX_WORKSPACE_PARTICIPATIONS_PER_READ: usize = 256;

/// Exact non-nil scoped logical participation identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkspaceParticipationId(Uuid);

impl WorkspaceParticipationId {
    /// Construct a non-nil participation identity.
    ///
    /// # Errors
    /// Rejects nil UUIDs.
    pub fn from_uuid(value: Uuid) -> Result<Self, OperationsError> {
        if value.is_nil() {
            return Err(OperationsError::Conflict(
                "workspace participation identity",
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn into_uuid(self) -> Uuid {
        self.0
    }
}

impl std::fmt::Display for WorkspaceParticipationId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

/// One legacy logical Session/member mapping attached to a shadow
/// `WorkspaceParticipation` anchor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkspaceParticipationSessionMapping {
    request_id: Uuid,
    participation_id: WorkspaceParticipationId,
    profile_id: AgentProfileId,
    profile_revision: AgentProfileRevision,
    profile_mapping_request_id: Uuid,
    session_id: AgentSessionId,
    session_revision: forge_domain::AgentSessionRevision,
    participant_id: Uuid,
    participant_membership_revision: u64,
    mapped_at: TimestampMillis,
}

impl WorkspaceParticipationSessionMapping {
    #[must_use]
    pub const fn request_id(self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn participation_id(self) -> WorkspaceParticipationId {
        self.participation_id
    }

    #[must_use]
    pub const fn profile_id(self) -> AgentProfileId {
        self.profile_id
    }

    #[must_use]
    pub const fn profile_revision(self) -> AgentProfileRevision {
        self.profile_revision
    }

    #[must_use]
    pub const fn profile_mapping_request_id(self) -> Uuid {
        self.profile_mapping_request_id
    }

    pub const fn session_id(self) -> AgentSessionId {
        self.session_id
    }

    pub const fn session_revision(self) -> forge_domain::AgentSessionRevision {
        self.session_revision
    }

    #[must_use]
    pub const fn participant_id(self) -> Uuid {
        self.participant_id
    }

    #[must_use]
    pub const fn participant_membership_revision(self) -> u64 {
        self.participant_membership_revision
    }

    #[must_use]
    pub const fn mapped_at(self) -> TimestampMillis {
        self.mapped_at
    }
}

/// Shadow participation readback. It is not yet election or membership
/// authority; the attached mappings identify current legacy rows only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceParticipationSnapshot {
    workspace_id: WorkspaceId,
    profile_id: AgentProfileId,
    participation_id: WorkspaceParticipationId,
    created_at: TimestampMillis,
    revision: u64,
    session_mappings: Vec<WorkspaceParticipationSessionMapping>,
}

impl WorkspaceParticipationSnapshot {
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn profile_id(&self) -> AgentProfileId {
        self.profile_id
    }

    #[must_use]
    pub const fn participation_id(&self) -> WorkspaceParticipationId {
        self.participation_id
    }

    #[must_use]
    pub const fn created_at(&self) -> TimestampMillis {
        self.created_at
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub fn session_mappings(&self) -> &[WorkspaceParticipationSessionMapping] {
        &self.session_mappings
    }
}

/// Bounded stable-order Workspace participation read for GUI/control-plane
/// projections. `truncated` forces callers to fail closed rather than silently
/// presenting an incomplete Agent set as authoritative.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceParticipationReadSet {
    participations: Vec<WorkspaceParticipationSnapshot>,
    truncated: bool,
}

impl WorkspaceParticipationReadSet {
    #[must_use]
    pub fn participations(&self) -> &[WorkspaceParticipationSnapshot] {
        &self.participations
    }

    #[must_use]
    pub const fn truncated(&self) -> bool {
        self.truncated
    }
}

/// Durable receipt for one Phase-B mapping attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceParticipationMappingReceipt {
    participation: WorkspaceParticipationSnapshot,
    mapping: WorkspaceParticipationSessionMapping,
    participation_created: bool,
    mapping_created: bool,
}

impl WorkspaceParticipationMappingReceipt {
    #[must_use]
    pub fn participation(&self) -> &WorkspaceParticipationSnapshot {
        &self.participation
    }

    #[must_use]
    pub const fn mapping(&self) -> WorkspaceParticipationSessionMapping {
        self.mapping
    }

    #[must_use]
    pub const fn participation_created(&self) -> bool {
        self.participation_created
    }

    #[must_use]
    pub const fn mapping_created(&self) -> bool {
        self.mapping_created
    }
}

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        (
            "table",
            "ops_workspace_participations",
            WORKSPACE_PARTICIPATIONS_SQL,
        ),
        (
            "table",
            "ops_workspace_participation_mappings",
            WORKSPACE_PARTICIPATION_MAPPINGS_SQL,
        ),
    ]
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    connection.execute_batch(WORKSPACE_PARTICIPATIONS_SQL)?;
    connection.execute_batch(WORKSPACE_PARTICIPATION_MAPPINGS_SQL)?;
    Ok(())
}

/// Remove post-v20 additions when a unit test reconstructs an exact legacy
/// catalog shape. The composed drops refuse durable isolation/handoff receipts.
/// Production migrations never call this helper.
#[cfg(test)]
pub(crate) fn drop_schema_for_legacy_fixture(
    connection: &Connection,
) -> Result<(), OperationsError> {
    crate::catalog::drop_v49_project_schema_for_legacy_fixture(connection)?;
    crate::work_credit_subject::drop_schema_for_legacy_fixture(connection)?;
    connection.execute_batch(
        "DROP TABLE IF EXISTS ops_election_subject_ballots; \
         DROP TABLE IF EXISTS ops_election_voter_sources; \
         DROP TABLE IF EXISTS ops_election_voter_subjects; \
         DROP TABLE IF EXISTS ops_election_subject_cutovers; \
         DROP TABLE IF EXISTS ops_workspace_participation_replacement_registrations; \
         DROP TABLE IF EXISTS ops_workspace_participation_session_rebinds; \
         DROP TABLE IF EXISTS ops_workspace_participation_mappings; \
         DROP TABLE IF EXISTS ops_workspace_participations;",
    )?;
    Ok(())
}

impl OperationsCatalog {
    /// Attach one verified Profile/logical-Session mapping to the single shadow
    /// `WorkspaceParticipation` anchor for `(workspace, profile)`.
    ///
    /// The supplied logical identity projection must come from this catalog's
    /// existing member/effect read model. A second Session for the same Profile
    /// and Workspace reuses the existing participation anchor instead of
    /// creating a second logical voter/member identity.
    ///
    /// # Errors
    /// Rejects stale member revisions, Profile/Session mismatches, competing
    /// Session/participant remaps, replay substitution, bounds, or uncertain
    /// persistence.
    pub fn map_workspace_participation(
        &mut self,
        request_id: Uuid,
        proposed_participation_id: WorkspaceParticipationId,
        profile_link: AgentProfileLogicalSessionLink,
        logical_identity: &LogicalSessionIdentityProjection,
        expected_membership_revision: u64,
        mapped_at: TimestampMillis,
    ) -> Result<WorkspaceParticipationMappingReceipt, OperationsError> {
        if request_id.is_nil() || expected_membership_revision == 0 {
            return Err(OperationsError::Conflict(
                "workspace participation mapping input",
            ));
        }
        if profile_link.session_id() != logical_identity.session_id() {
            return Err(OperationsError::Conflict(
                "workspace participation Profile/Session mapping",
            ));
        }
        let destination = self.destination;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let workspace_id = logical_identity.workspace_id();
        let participant_id = logical_identity.participant_id();
        verify_member_revision(
            &tx,
            workspace_id,
            participant_id,
            profile_link.session_id(),
            expected_membership_revision,
        )?;

        if let Some(existing) = mapping_by_request(&tx, request_id)? {
            ensure_mapping_replay(
                existing,
                profile_link,
                logical_identity,
                expected_membership_revision,
            )?;
            let participation = load_participation(&tx, workspace_id, profile_link.profile_id())?
                .ok_or(OperationsError::Corrupt(
                "workspace participation replay anchor",
            ))?;
            commit_mapping_transaction(tx, destination)?;
            return Ok(WorkspaceParticipationMappingReceipt {
                participation,
                mapping: existing,
                participation_created: false,
                mapping_created: false,
            });
        }

        crate::leadership::work_roster::ensure_identity_admission_open(
            &tx,
            workspace_id,
            profile_link.session_id(),
            Some(profile_link.profile_id()),
        )?;

        let (participation_id, participation_created) = resolve_or_create_participation(
            &tx,
            workspace_id,
            profile_link.profile_id(),
            proposed_participation_id,
            mapped_at,
        )?;

        if let Some(existing) = existing_source_mapping(
            &tx,
            workspace_id,
            participation_id,
            profile_link,
            participant_id,
        )? {
            let participation = load_participation(&tx, workspace_id, profile_link.profile_id())?
                .ok_or(OperationsError::Corrupt(
                "workspace participation existing anchor",
            ))?;
            commit_mapping_transaction(tx, destination)?;
            return Ok(WorkspaceParticipationMappingReceipt {
                participation,
                mapping: existing,
                participation_created,
                mapping_created: false,
            });
        }

        let mapping = WorkspaceParticipationSessionMapping {
            request_id,
            participation_id,
            profile_id: profile_link.profile_id(),
            profile_revision: profile_link.profile_revision(),
            profile_mapping_request_id: profile_link.request_id(),
            session_id: profile_link.session_id(),
            session_revision: profile_link.session_revision(),
            participant_id,
            participant_membership_revision: expected_membership_revision,
            mapped_at,
        };
        insert_mapping(&tx, workspace_id, mapping)?;
        let participation = load_participation(&tx, workspace_id, profile_link.profile_id())?
            .ok_or(OperationsError::Corrupt(
                "workspace participation insert readback",
            ))?;
        commit_mapping_transaction(tx, destination)?;
        Ok(WorkspaceParticipationMappingReceipt {
            participation,
            mapping,
            participation_created,
            mapping_created: true,
        })
    }

    /// Read one shadow participation by exact `(workspace, profile)` identity.
    ///
    /// # Errors
    /// Returns storage, corruption, or bound failures.
    pub fn workspace_participation(
        &self,
        workspace_id: WorkspaceId,
        profile_id: AgentProfileId,
    ) -> Result<Option<WorkspaceParticipationSnapshot>, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        let result = load_participation(&tx, workspace_id, profile_id)?;
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(result)
    }

    /// Read the bounded shadow participation set for one exact Workspace.
    /// Results are ordered by creation time and stable Profile identity.
    ///
    /// # Errors
    /// Returns storage/corruption failures or mapping-bound failures from any
    /// participation included in the bounded read.
    pub fn workspace_participations(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<WorkspaceParticipationReadSet, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        let query_limit = i64::try_from(MAX_WORKSPACE_PARTICIPATIONS_PER_READ + 1)
            .map_err(|_| OperationsError::Bounds("Workspace participation read limit"))?;
        let mut statement = tx.prepare(
            "SELECT substr(agent_profile_id,1,17) FROM ops_workspace_participations \
             WHERE workspace_id=?1 ORDER BY created_at_ms ASC, agent_profile_id ASC LIMIT ?2",
        )?;
        let rows = statement.query_map(
            params![workspace_id.into_uuid().as_bytes(), query_limit],
            |row| row.get::<_, Vec<u8>>(0),
        )?;
        let mut profile_ids = rows.collect::<Result<Vec<_>, _>>()?;
        let truncated = profile_ids.len() > MAX_WORKSPACE_PARTICIPATIONS_PER_READ;
        profile_ids.truncate(MAX_WORKSPACE_PARTICIPATIONS_PER_READ);
        drop(statement);

        let mut participations = Vec::with_capacity(profile_ids.len());
        for raw_profile_id in profile_ids {
            let profile_id = AgentProfileId::from_uuid(Uuid::from_bytes(wire::id(raw_profile_id)?))
                .map_err(|_| {
                    OperationsError::Corrupt("workspace participation Profile identity")
                })?;
            let participation = load_participation(&tx, workspace_id, profile_id)?.ok_or(
                OperationsError::Corrupt("workspace participation list readback"),
            )?;
            participations.push(participation);
        }
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(WorkspaceParticipationReadSet {
            participations,
            truncated,
        })
    }
}

fn resolve_or_create_participation(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    profile_id: AgentProfileId,
    proposed_participation_id: WorkspaceParticipationId,
    mapped_at: TimestampMillis,
) -> Result<(WorkspaceParticipationId, bool), OperationsError> {
    if let Some(existing) = load_participation(tx, workspace_id, profile_id)? {
        return Ok((existing.participation_id(), false));
    }
    tx.execute(
        "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,?4,1)",
        params![
            workspace_id.into_uuid().as_bytes(),
            profile_id.into_uuid().as_bytes(),
            proposed_participation_id.into_uuid().as_bytes(),
            mapped_at.get(),
        ],
    )?;
    Ok((proposed_participation_id, true))
}

fn existing_source_mapping(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    participation_id: WorkspaceParticipationId,
    profile_link: AgentProfileLogicalSessionLink,
    participant_id: Uuid,
) -> Result<Option<WorkspaceParticipationSessionMapping>, OperationsError> {
    if let Some(existing) = mapping_by_session(tx, workspace_id, profile_link.session_id())? {
        ensure_same_anchor(
            existing,
            participation_id,
            profile_link.profile_id(),
            participant_id,
        )?;
        return Ok(Some(existing));
    }
    if let Some(existing) = mapping_by_participant(tx, workspace_id, participant_id)? {
        ensure_same_anchor(
            existing,
            participation_id,
            profile_link.profile_id(),
            participant_id,
        )?;
        return Ok(Some(existing));
    }
    Ok(None)
}

fn commit_mapping_transaction(
    tx: Transaction<'_>,
    destination: Uuid,
) -> Result<(), OperationsError> {
    tx.commit()
        .map_err(|source| OperationsError::CatalogCommitUnconfirmed {
            destination_id: destination,
            source,
        })
}

fn verify_member_revision(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
    session_id: AgentSessionId,
    expected_membership_revision: u64,
) -> Result<(), OperationsError> {
    let raw: Option<(Vec<u8>, i64)> = connection
        .query_row(
            "SELECT substr(session_id,1,17),membership_revision FROM ops_logical_members WHERE workspace_id=?1 AND participant_id=?2 LIMIT 1",
            params![workspace_id.into_uuid().as_bytes(), participant_id.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((stored_session, stored_revision)) = raw else {
        return Err(OperationsError::Conflict(
            "workspace participation source member",
        ));
    };
    if AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(stored_session)?)) != session_id
        || u64::try_from(stored_revision)
            .map_err(|_| OperationsError::Corrupt("logical membership revision"))?
            != expected_membership_revision
    {
        return Err(OperationsError::Conflict(
            "workspace participation source member revision",
        ));
    }
    Ok(())
}

pub(crate) fn load_participation(
    connection: &Connection,
    workspace_id: WorkspaceId,
    profile_id: AgentProfileId,
) -> Result<Option<WorkspaceParticipationSnapshot>, OperationsError> {
    let raw: Option<(Vec<u8>, i64, i64)> = connection
        .query_row(
            "SELECT substr(participation_id,1,17),created_at_ms,revision FROM ops_workspace_participations WHERE workspace_id=?1 AND agent_profile_id=?2 LIMIT 1",
            params![
                workspace_id.into_uuid().as_bytes(),
                profile_id.into_uuid().as_bytes()
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((participation_id, created_at, revision)) = raw else {
        return Ok(None);
    };
    let participation_id =
        WorkspaceParticipationId::from_uuid(Uuid::from_bytes(wire::id(participation_id)?))?;
    let mut statement = connection.prepare(
        "SELECT substr(request_id,1,17),substr(participation_id,1,17),substr(agent_profile_id,1,17),profile_revision,substr(profile_mapping_request_id,1,17),substr(session_id,1,17),session_revision,substr(participant_id,1,17),participant_membership_revision,mapped_at_ms FROM ops_workspace_participation_mappings WHERE workspace_id=?1 AND participation_id=?2 ORDER BY mapped_at_ms,session_id LIMIT 65",
    )?;
    let mut rows = statement.query(params![
        workspace_id.into_uuid().as_bytes(),
        participation_id.into_uuid().as_bytes()
    ])?;
    let mut mappings = Vec::new();
    while let Some(row) = rows.next()? {
        if mappings.len() >= 64 {
            return Err(OperationsError::Bounds(
                "workspace participation Session mappings",
            ));
        }
        mappings.push(decode_mapping_row(row)?);
    }
    Ok(Some(WorkspaceParticipationSnapshot {
        workspace_id,
        profile_id,
        participation_id,
        created_at: TimestampMillis::new(created_at)
            .map_err(|_| OperationsError::Corrupt("workspace participation timestamp"))?,
        revision: u64::try_from(revision)
            .map_err(|_| OperationsError::Corrupt("workspace participation revision"))?,
        session_mappings: mappings,
    }))
}

fn mapping_by_request(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<WorkspaceParticipationSessionMapping>, OperationsError> {
    load_mapping(
        connection,
        "SELECT substr(request_id,1,17),substr(participation_id,1,17),substr(agent_profile_id,1,17),profile_revision,substr(profile_mapping_request_id,1,17),substr(session_id,1,17),session_revision,substr(participant_id,1,17),participant_membership_revision,mapped_at_ms FROM ops_workspace_participation_mappings WHERE request_id=?1 LIMIT 1",
        request_id.as_bytes(),
    )
}

fn mapping_by_session(
    connection: &Connection,
    workspace_id: WorkspaceId,
    session_id: AgentSessionId,
) -> Result<Option<WorkspaceParticipationSessionMapping>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(request_id,1,17),substr(participation_id,1,17),substr(agent_profile_id,1,17),profile_revision,substr(profile_mapping_request_id,1,17),substr(session_id,1,17),session_revision,substr(participant_id,1,17),participant_membership_revision,mapped_at_ms FROM ops_workspace_participation_mappings WHERE workspace_id=?1 AND session_id=?2 LIMIT 1",
            params![workspace_id.into_uuid().as_bytes(), session_id.into_uuid().as_bytes()],
            decode_mapping_row,
        )
        .optional()?;
    Ok(raw)
}

fn mapping_by_participant(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
) -> Result<Option<WorkspaceParticipationSessionMapping>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(request_id,1,17),substr(participation_id,1,17),substr(agent_profile_id,1,17),profile_revision,substr(profile_mapping_request_id,1,17),substr(session_id,1,17),session_revision,substr(participant_id,1,17),participant_membership_revision,mapped_at_ms FROM ops_workspace_participation_mappings WHERE workspace_id=?1 AND participant_id=?2 LIMIT 1",
            params![workspace_id.into_uuid().as_bytes(), participant_id.as_bytes()],
            decode_mapping_row,
        )
        .optional()?;
    Ok(raw)
}

fn load_mapping(
    connection: &Connection,
    sql: &str,
    key: &[u8],
) -> Result<Option<WorkspaceParticipationSessionMapping>, OperationsError> {
    Ok(connection
        .query_row(sql, params![key], decode_mapping_row)
        .optional()?)
}

fn decode_mapping_row(
    row: &rusqlite::Row<'_>,
) -> Result<WorkspaceParticipationSessionMapping, rusqlite::Error> {
    decode_mapping_row_inner(row).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Blob, Box::new(error))
    })
}

fn decode_mapping_row_inner(
    row: &rusqlite::Row<'_>,
) -> Result<WorkspaceParticipationSessionMapping, OperationsError> {
    let request_id = Uuid::from_bytes(wire::id(row.get(0)?)?);
    let participation_id =
        WorkspaceParticipationId::from_uuid(Uuid::from_bytes(wire::id(row.get(1)?)?))?;
    let profile_id = AgentProfileId::from_uuid(Uuid::from_bytes(wire::id(row.get(2)?)?))
        .map_err(|_| OperationsError::Corrupt("workspace participation Profile identity"))?;
    let profile_revision = AgentProfileRevision::new(row.get(3)?)
        .map_err(|_| OperationsError::Corrupt("workspace participation Profile revision"))?;
    let profile_mapping_request_id = Uuid::from_bytes(wire::id(row.get(4)?)?);
    let session_id = AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(row.get(5)?)?));
    let session_revision = forge_domain::AgentSessionRevision::new(row.get(6)?)
        .map_err(|_| OperationsError::Corrupt("workspace participation Session revision"))?;
    let participant_id = Uuid::from_bytes(wire::id(row.get(7)?)?);
    let participant_membership_revision = u64::try_from(row.get::<_, i64>(8)?)
        .map_err(|_| OperationsError::Corrupt("workspace participation membership revision"))?;
    let mapped_at = TimestampMillis::new(row.get(9)?)
        .map_err(|_| OperationsError::Corrupt("workspace participation mapping timestamp"))?;
    Ok(WorkspaceParticipationSessionMapping {
        request_id,
        participation_id,
        profile_id,
        profile_revision,
        profile_mapping_request_id,
        session_id,
        session_revision,
        participant_id,
        participant_membership_revision,
        mapped_at,
    })
}

fn insert_mapping(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    mapping: WorkspaceParticipationSessionMapping,
) -> Result<(), OperationsError> {
    tx.execute(
        "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
        params![
            mapping.request_id.as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            mapping.participation_id.into_uuid().as_bytes(),
            mapping.profile_id.into_uuid().as_bytes(),
            mapping.profile_revision.get(),
            mapping.profile_mapping_request_id.as_bytes(),
            mapping.session_id.into_uuid().as_bytes(),
            mapping.session_revision.get(),
            mapping.participant_id.as_bytes(),
            i64::try_from(mapping.participant_membership_revision)
                .map_err(|_| OperationsError::Bounds("workspace participation membership revision"))?,
            mapping.mapped_at.get(),
        ],
    )?;
    Ok(())
}

fn ensure_mapping_replay(
    existing: WorkspaceParticipationSessionMapping,
    profile_link: AgentProfileLogicalSessionLink,
    logical_identity: &LogicalSessionIdentityProjection,
    expected_membership_revision: u64,
) -> Result<(), OperationsError> {
    if existing.profile_id != profile_link.profile_id()
        || existing.profile_revision != profile_link.profile_revision()
        || existing.profile_mapping_request_id != profile_link.request_id()
        || existing.session_id != profile_link.session_id()
        || existing.session_revision != profile_link.session_revision()
        || existing.participant_id != logical_identity.participant_id()
        || existing.participant_membership_revision != expected_membership_revision
    {
        return Err(OperationsError::Conflict(
            "workspace participation mapping replay",
        ));
    }
    Ok(())
}

fn ensure_same_anchor(
    existing: WorkspaceParticipationSessionMapping,
    participation_id: WorkspaceParticipationId,
    profile_id: AgentProfileId,
    participant_id: Uuid,
) -> Result<(), OperationsError> {
    if existing.participation_id != participation_id
        || existing.profile_id != profile_id
        || existing.participant_id != participant_id
    {
        return Err(OperationsError::Conflict(
            "workspace participation competing mapping",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_domain::{
        AgentSessionOpenRequestId, AuthenticatedPrincipal, OpenAgentSession, TimestampMillis,
    };
    use heterarchy_orchestration_sessions::{
        AgentProfileCatalog, AgentProfileCreationOrigin, AgentProfileProviderFamily,
        AgentSessionCatalog, CreateAgentProfile, link_existing_logical_session,
    };
    use std::error::Error;

    struct Fixture {
        root: std::path::PathBuf,
        profiles: AgentProfileCatalog,
        sessions: AgentSessionCatalog,
        operations: OperationsCatalog,
        workspace_id: WorkspaceId,
        profile_id: AgentProfileId,
        principal: AuthenticatedPrincipal,
    }

    fn fixture(name: &str) -> Result<Fixture, Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!(
            "orchestration-workspace-participation-{name}-{}",
            Uuid::now_v7()
        ));
        std::fs::create_dir(&root)?;
        let mut profiles = AgentProfileCatalog::open(root.join("agent-profiles.sqlite3"))?;
        let sessions = AgentSessionCatalog::open(root.join("agent-sessions.sqlite3"))?;
        let profile_id = AgentProfileId::from_uuid(Uuid::from_u128(0xF101))?;
        let profile = CreateAgentProfile::new(
            profile_id,
            Uuid::from_u128(0xF102),
            "luna-rust-global".to_owned(),
            "Luna Rust Global".to_owned(),
            AgentProfileProviderFamily::ChatGpt,
            Some("gpt-5.6-luna".to_owned()),
            Some("max".to_owned()),
            vec!["rust".to_owned()],
            AgentProfileCreationOrigin::UserManual,
            TimestampMillis::new(10)?,
        )?;
        let _created = profiles.create_profile(&profile)?;
        let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(0xF103));
        let destination = Uuid::from_u128(0xF104);
        let operations = OperationsCatalog::open(&root.join("operations.sqlite3"), destination)?;
        Ok(Fixture {
            root,
            profiles,
            sessions,
            operations,
            workspace_id,
            profile_id,
            principal: AuthenticatedPrincipal::TrustedStdio,
        })
    }

    fn open_and_seed_member(
        fixture: &mut Fixture,
        session_seed: u128,
        open_request_seed: u128,
        participant_seed: u128,
        linked_at_ms: i64,
    ) -> Result<
        (
            AgentProfileLogicalSessionLink,
            LogicalSessionIdentityProjection,
        ),
        Box<dyn Error>,
    > {
        let session_id = AgentSessionId::from_uuid(Uuid::from_u128(session_seed));
        let _opened = fixture.sessions.open_session(&OpenAgentSession::new(
            session_id,
            AgentSessionOpenRequestId::from_uuid(Uuid::from_u128(open_request_seed)),
            fixture.principal,
            TimestampMillis::new(linked_at_ms - 1)?,
        ))?;
        let link = link_existing_logical_session(
            &mut fixture.profiles,
            &fixture.sessions,
            fixture.profile_id,
            session_id,
            fixture.principal,
            Uuid::from_u128(open_request_seed + 0x100),
            TimestampMillis::new(linked_at_ms)?,
        )?
        .link();
        let participant_id = Uuid::from_u128(participant_seed);
        fixture.operations.connection.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,?4,'worker',1,1,?5)",
            params![
                fixture.workspace_id.into_uuid().as_bytes(),
                participant_id.as_bytes(),
                session_id.into_uuid().as_bytes(),
                format!("member-{participant_seed:x}"),
                linked_at_ms,
            ],
        )?;
        let projection = fixture
            .operations
            .logical_identity_for_session(fixture.workspace_id, session_id)?
            .ok_or("seeded logical Session identity missing")?;
        Ok((link, projection))
    }

    fn table_count(catalog: &OperationsCatalog, table: &str) -> Result<i64, Box<dyn Error>> {
        let sql = format!("SELECT COUNT(*) FROM {table}");
        Ok(catalog.connection.query_row(&sql, [], |row| row.get(0))?)
    }

    fn table_schema(connection: &Connection, table: &str) -> Result<String, Box<dyn Error>> {
        Ok(connection.query_row(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?1",
            params![table],
            |row| row.get(0),
        )?)
    }

    #[test]
    fn same_profile_two_sessions_same_workspace_reuse_one_shadow_participation()
    -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture("same-profile-two-sessions")?;
        let (link_a, projection_a) =
            open_and_seed_member(&mut fixture, 0xF110, 0xF120, 0xF130, 20)?;
        let (link_b, projection_b) =
            open_and_seed_member(&mut fixture, 0xF111, 0xF121, 0xF131, 30)?;
        let member_count_before = table_count(&fixture.operations, "ops_logical_members")?;
        let leadership_count_before = table_count(&fixture.operations, "ops_leadership")?;
        let credit_count_before = table_count(&fixture.operations, "ops_work_credit_entries")?;

        let participation_a = WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xF140))?;
        let first = fixture.operations.map_workspace_participation(
            Uuid::from_u128(0xF150),
            participation_a,
            link_a,
            &projection_a,
            1,
            TimestampMillis::new(40)?,
        )?;
        assert!(first.participation_created());
        assert!(first.mapping_created());
        assert_eq!(first.participation().participation_id(), participation_a);
        assert_eq!(first.participation().session_mappings().len(), 1);

        let proposed_b = WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xF141))?;
        let second = fixture.operations.map_workspace_participation(
            Uuid::from_u128(0xF151),
            proposed_b,
            link_b,
            &projection_b,
            1,
            TimestampMillis::new(50)?,
        )?;
        assert!(!second.participation_created());
        assert!(second.mapping_created());
        assert_eq!(second.participation().participation_id(), participation_a);
        assert_ne!(second.participation().participation_id(), proposed_b);
        assert_eq!(second.participation().session_mappings().len(), 2);

        let readback = fixture
            .operations
            .workspace_participation(fixture.workspace_id, fixture.profile_id)?
            .ok_or("workspace participation readback missing")?;
        assert_eq!(readback.participation_id(), participation_a);
        assert_eq!(readback.session_mappings().len(), 2);
        let listed = fixture
            .operations
            .workspace_participations(fixture.workspace_id)?;
        assert!(!listed.truncated());
        assert_eq!(listed.participations().len(), 1);
        assert_eq!(listed.participations()[0].profile_id(), fixture.profile_id);
        assert_eq!(
            listed.participations()[0].participation_id(),
            participation_a
        );
        assert_eq!(
            table_count(&fixture.operations, "ops_workspace_participations")?,
            1
        );
        assert_eq!(
            table_count(&fixture.operations, "ops_workspace_participation_mappings")?,
            2
        );

        // Phase B is shadow-only: existing membership and unrelated authority
        // surfaces are byte-for-byte outside this write path.
        assert_eq!(
            table_count(&fixture.operations, "ops_logical_members")?,
            member_count_before
        );
        assert_eq!(member_count_before, 2);
        assert_eq!(
            table_count(&fixture.operations, "ops_leadership")?,
            leadership_count_before
        );
        assert_eq!(
            table_count(&fixture.operations, "ops_work_credit_entries")?,
            credit_count_before
        );

        drop(fixture.operations);
        drop(fixture.sessions);
        drop(fixture.profiles);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn same_profile_in_two_workspaces_gets_distinct_participation_anchors()
    -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture("workspace-isolation")?;
        let (link_a, projection_a) =
            open_and_seed_member(&mut fixture, 0xF210, 0xF220, 0xF230, 20)?;
        let participation_a = WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xF240))?;
        let first = fixture.operations.map_workspace_participation(
            Uuid::from_u128(0xF250),
            participation_a,
            link_a,
            &projection_a,
            1,
            TimestampMillis::new(30)?,
        )?;

        let workspace_b = WorkspaceId::from_uuid(Uuid::from_u128(0xF260));
        let session_b = AgentSessionId::from_uuid(Uuid::from_u128(0xF211));
        let _opened = fixture.sessions.open_session(&OpenAgentSession::new(
            session_b,
            AgentSessionOpenRequestId::from_uuid(Uuid::from_u128(0xF221)),
            fixture.principal,
            TimestampMillis::new(31)?,
        ))?;
        let link_b = link_existing_logical_session(
            &mut fixture.profiles,
            &fixture.sessions,
            fixture.profile_id,
            session_b,
            fixture.principal,
            Uuid::from_u128(0xF321),
            TimestampMillis::new(32)?,
        )?
        .link();
        let participant_b = Uuid::from_u128(0xF231);
        fixture.operations.connection.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,'workspace-b-member','worker',1,1,33)",
            params![
                workspace_b.into_uuid().as_bytes(),
                participant_b.as_bytes(),
                session_b.into_uuid().as_bytes(),
            ],
        )?;
        let projection_b = fixture
            .operations
            .logical_identity_for_session(workspace_b, session_b)?
            .ok_or("workspace B logical Session identity missing")?;
        let participation_b = WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xF241))?;
        let second = fixture.operations.map_workspace_participation(
            Uuid::from_u128(0xF251),
            participation_b,
            link_b,
            &projection_b,
            1,
            TimestampMillis::new(34)?,
        )?;

        assert_eq!(first.participation().participation_id(), participation_a);
        assert_eq!(second.participation().participation_id(), participation_b);
        assert_ne!(participation_a, participation_b);
        assert_eq!(
            table_count(&fixture.operations, "ops_workspace_participations")?,
            2
        );

        drop(fixture.operations);
        drop(fixture.sessions);
        drop(fixture.profiles);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn v20_upgrade_preserves_legacy_authority_and_restores_shadow_participation()
    -> Result<(), Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!(
            "orchestration-workspace-participation-v20-{}",
            Uuid::now_v7()
        ));
        std::fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xF301);
        let catalog = OperationsCatalog::open(&path, destination)?;
        drop(catalog);

        let connection = Connection::open(&path)?;
        let leadership_schema_before = table_schema(&connection, "ops_leadership")?;
        drop_schema_for_legacy_fixture(&connection)?;
        let member_schema_before = table_schema(&connection, "ops_logical_members")?;
        let credit_schema_before = table_schema(&connection, "ops_work_credit_entries")?;
        connection.pragma_update(None, "user_version", 20_i64)?;
        drop(connection);

        let migrated = OperationsCatalog::open(&path, destination)?;
        let version: i64 = migrated
            .connection
            .pragma_query_value(None, "user_version", |row| row.get(0))?;
        assert_eq!(version, crate::catalog::VERSION);
        assert_eq!(
            table_schema(&migrated.connection, "ops_logical_members")?,
            member_schema_before
        );
        crate::catalog::assert_fixture_table_schema(
            &migrated.connection,
            "ops_leadership",
            &leadership_schema_before,
        )?;
        assert_eq!(
            table_schema(&migrated.connection, "ops_work_credit_entries")?,
            credit_schema_before
        );
        let shadow_objects: i64 = migrated.connection.query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name IN ('ops_workspace_participations','ops_workspace_participation_mappings')",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(shadow_objects, 2);

        drop(migrated);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    // Candidate test items for workspace_participation.rs::tests. This is a
    // scratch-only insertion snippet; reuse the existing Fixture and
    // open_and_seed_member helpers in that module.

    use crate::RegistrationRequest;
    use forge_domain::{
        ActivityId, AgentDeclaredRole, AgentDisplayName, AgentInstanceId, AgentTransportKind,
    };
    use forge_persistence::AgentBindingReservationRequest;

    fn seed_profile_scope_verified_import(
        fixture: &Fixture,
        migration: Uuid,
    ) -> Result<(), Box<dyn Error>> {
        let digest = [0xA7_u8; 32];
        fixture.operations.connection.execute(
            "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
            params![
                migration.as_bytes(),
                fixture.workspace_id.into_uuid().as_bytes(),
                fixture.operations.destination_id().as_bytes(),
                digest.as_slice(),
            ],
        )?;
        Ok(())
    }

    fn profile_scope_registration_request(
        fixture: &Fixture,
        link: AgentProfileLogicalSessionLink,
        request_seed: u128,
        activity_seed: u128,
        agent_seed: u128,
        participant_seed: u128,
    ) -> Result<RegistrationRequest, Box<dyn Error>> {
        let binding = AgentBindingReservationRequest::new(
            Uuid::from_u128(request_seed),
            ActivityId::from_uuid(Uuid::from_u128(activity_seed)),
            AgentInstanceId::from_uuid(Uuid::from_u128(agent_seed)),
            link.session_id(),
            fixture.principal,
            TimestampMillis::new(1_000)?,
        )?;
        Ok(RegistrationRequest::new(
            Uuid::from_u128(participant_seed),
            binding,
            AgentDisplayName::new(format!("profile-scope-{request_seed:x}"))?,
            AgentDeclaredRole::new("worker".to_owned())?,
            AgentTransportKind::Stdio,
        )?)
    }

    fn profile_scope_registration_counts(
        catalog: &OperationsCatalog,
    ) -> Result<(i64, i64, i64), Box<dyn Error>> {
        Ok((
            table_count(catalog, "ops_registration_requests")?,
            table_count(catalog, "ops_pending_participants")?,
            table_count(catalog, "ops_registration_profile_scopes")?,
        ))
    }

    fn assert_profile_scope_matches_link(
        scope: crate::RegistrationProfileScope,
        workspace_id: WorkspaceId,
        request_id: Uuid,
        link: AgentProfileLogicalSessionLink,
    ) {
        assert_eq!(scope.request_id(), request_id);
        assert_eq!(scope.workspace_id(), workspace_id);
        assert_eq!(scope.profile_id(), link.profile_id());
        assert_eq!(scope.profile_link_request_id(), link.request_id());
        assert_eq!(scope.profile_revision(), link.profile_revision());
        assert_eq!(scope.session_id(), link.session_id());
        assert_eq!(scope.session_revision(), link.session_revision());
    }

    #[test]
    fn profile_scope_is_explicit_stable_and_request_session_mismatch_rolls_back()
    -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture("registration-profile-scope")?;
        let (link_a, _) = open_and_seed_member(&mut fixture, 0xF310, 0xF320, 0xF330, 20)?;
        let (link_b, _) = open_and_seed_member(&mut fixture, 0xF311, 0xF321, 0xF331, 30)?;
        let migration = Uuid::from_u128(0xF350);
        seed_profile_scope_verified_import(&fixture, migration)?;
        let request_a =
            profile_scope_registration_request(&fixture, link_a, 0xF360, 0xF361, 0xF362, 0xF363)?;
        let pending_a = fixture
            .operations
            .prepare_registration(migration, &request_a)?;
        assert!(
            fixture
                .operations
                .registration_profile_scope(request_a.binding().request_id())?
                .is_none()
        );
        assert_eq!(
            fixture
                .operations
                .prepare_profile_registration(migration, &request_a, link_a)?,
            pending_a
        );
        let scope_a = fixture
            .operations
            .registration_profile_scope(request_a.binding().request_id())?
            .ok_or("trusted Profile link missing")?;
        assert_profile_scope_matches_link(
            scope_a,
            fixture.workspace_id,
            request_a.binding().request_id(),
            link_a,
        );

        let request_b =
            profile_scope_registration_request(&fixture, link_b, 0xF370, 0xF371, 0xF372, 0xF373)?;
        let before = profile_scope_registration_counts(&fixture.operations)?;
        assert!(
            fixture
                .operations
                .prepare_profile_registration(migration, &request_b, link_a)
                .is_err()
        );
        assert_eq!(
            profile_scope_registration_counts(&fixture.operations)?,
            before,
            "Session mismatch must roll back pending identity, request, and Profile scope"
        );
        assert!(
            fixture
                .operations
                .registration_progress(request_b.binding().request_id())?
                .is_none()
        );
        // The exact rejected request can succeed with real matching evidence; no new ID.
        let pending_b = fixture
            .operations
            .prepare_profile_registration(migration, &request_b, link_b)?;
        let destination = fixture.operations.destination_id();
        drop(fixture.operations);
        fixture.operations =
            OperationsCatalog::open(&fixture.root.join("operations.sqlite3"), destination)?;
        for (request, link, pending) in [
            (&request_a, link_a, pending_a),
            (&request_b, link_b, pending_b),
        ] {
            assert_eq!(
                fixture
                    .operations
                    .prepare_profile_registration(migration, request, link)?,
                pending
            );
            let scope = fixture
                .operations
                .registration_profile_scope(request.binding().request_id())?
                .ok_or("reopened Profile scope missing")?;
            assert_profile_scope_matches_link(
                scope,
                fixture.workspace_id,
                request.binding().request_id(),
                link,
            );
        }
        drop(fixture.operations);
        drop(fixture.sessions);
        drop(fixture.profiles);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }
}
