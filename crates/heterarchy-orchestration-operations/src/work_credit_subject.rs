//! Phase-F `WorkCredit` subject projection and election-start score freeze.
//!
//! The append-only `ops_work_credit_entries` ledger keeps participant/Agent
//! provenance exactly as awarded. Read authority is projected onto one scoped
//! logical subject: an existing `WorkspaceParticipation` when a durable mapping
//! exists, otherwise a transitional legacy participant subject. This lets
//! historical participant rows merge without rewriting evidence.

use crate::{OperationsError, WorkCreditScoreSnapshot, WorkspaceParticipationId, wire};
use forge_domain::WorkspaceId;
use heterarchy_orchestration_sessions::AgentProfileId;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use uuid::Uuid;

const SNAPSHOT_DIGEST_DOMAIN: &[u8] = b"heterarchy.work-credit.election-subject-snapshot.v1\0";

const ELECTION_SNAPSHOT_SQL_V31: &str = "CREATE TABLE ops_election_work_credit_snapshots (election_id BLOB PRIMARY KEY NOT NULL REFERENCES ops_leadership_elections(election_id) CHECK(length(election_id)=16 AND election_id != zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), candidate_participant_id BLOB NOT NULL CHECK(length(candidate_participant_id)=16 AND candidate_participant_id != zeroblob(16)), subject_kind INTEGER NOT NULL CHECK(subject_kind IN(0,1)), subject_id BLOB NOT NULL CHECK(length(subject_id)=16 AND subject_id != zeroblob(16)), agent_profile_id BLOB CHECK(agent_profile_id IS NULL OR (length(agent_profile_id)=16 AND agent_profile_id != zeroblob(16))), participation_revision INTEGER CHECK(participation_revision IS NULL OR participation_revision>0), subject_revision INTEGER NOT NULL CHECK(subject_revision=1), source_participant_count INTEGER NOT NULL CHECK(source_participant_count>=1), window_ms INTEGER NOT NULL CHECK(window_ms>0), evaluated_at_ms INTEGER NOT NULL CHECK(evaluated_at_ms>=0), candidate_total_credits INTEGER NOT NULL CHECK(candidate_total_credits>=0), workspace_total_credits INTEGER NOT NULL CHECK(workspace_total_credits>=0), workspace_arithmetic_mean INTEGER NOT NULL CHECK(workspace_arithmetic_mean>=0), mean_remainder_numerator INTEGER NOT NULL CHECK(mean_remainder_numerator>=0), mean_subject_count INTEGER NOT NULL CHECK(mean_subject_count>=0), qualified INTEGER NOT NULL CHECK(qualified IN(0,1)), snapshot_digest BLOB NOT NULL CHECK(length(snapshot_digest)=32 AND snapshot_digest != zeroblob(32)), CHECK((subject_kind=0 AND agent_profile_id IS NOT NULL AND participation_revision IS NOT NULL) OR (subject_kind=1 AND agent_profile_id IS NULL AND participation_revision IS NULL AND source_participant_count=1)), CHECK(mean_subject_count=0 OR mean_remainder_numerator<mean_subject_count))";
const ELECTION_SNAPSHOT_SQL: &str = "CREATE TABLE ops_election_work_credit_snapshots (election_id BLOB PRIMARY KEY NOT NULL REFERENCES ops_leadership_elections(election_id) CHECK(length(election_id)=16 AND election_id != zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), candidate_participant_id BLOB NOT NULL CHECK(length(candidate_participant_id)=16 AND candidate_participant_id != zeroblob(16)), subject_kind INTEGER NOT NULL CHECK(subject_kind IN(0,1)), subject_id BLOB NOT NULL CHECK(length(subject_id)=16 AND subject_id != zeroblob(16)), agent_profile_id BLOB CHECK(agent_profile_id IS NULL OR (length(agent_profile_id)=16 AND agent_profile_id != zeroblob(16))), participation_revision INTEGER CHECK(participation_revision IS NULL OR participation_revision>0), subject_revision INTEGER NOT NULL CHECK(subject_revision=1), source_participant_count INTEGER NOT NULL CHECK(source_participant_count>=1), window_ms INTEGER NOT NULL CHECK(window_ms>0), evaluated_at_ms INTEGER NOT NULL CHECK(evaluated_at_ms>=0), candidate_total_credits INTEGER NOT NULL CHECK(candidate_total_credits>=0), workspace_total_credits INTEGER NOT NULL CHECK(workspace_total_credits>=0), workspace_arithmetic_mean INTEGER NOT NULL CHECK(workspace_arithmetic_mean>=0), mean_remainder_numerator INTEGER NOT NULL CHECK(mean_remainder_numerator>=0), mean_subject_count INTEGER NOT NULL CHECK(mean_subject_count>=0), qualified INTEGER NOT NULL CHECK(qualified IN(0,1)), snapshot_digest BLOB NOT NULL CHECK(length(snapshot_digest)=32 AND snapshot_digest != zeroblob(32)), team_id BLOB CHECK(team_id IS NULL OR (length(team_id)=16 AND team_id != zeroblob(16))), CHECK((subject_kind=0 AND agent_profile_id IS NOT NULL AND participation_revision IS NOT NULL) OR (subject_kind=1 AND agent_profile_id IS NULL AND participation_revision IS NULL AND source_participant_count=1)), CHECK(mean_subject_count=0 OR mean_remainder_numerator<mean_subject_count))";

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![(
        "table",
        "ops_election_work_credit_snapshots",
        ELECTION_SNAPSHOT_SQL,
    )]
}

pub(crate) fn schema_objects_v31() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![(
        "table",
        "ops_election_work_credit_snapshots",
        ELECTION_SNAPSHOT_SQL_V31,
    )]
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    connection.execute_batch(ELECTION_SNAPSHOT_SQL)?;
    Ok(())
}

pub(crate) fn migrate_v31_to_v32(transaction: &Transaction<'_>) -> Result<(), OperationsError> {
    let has_team_id: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info(?1) WHERE name='team_id')",
        ["ops_election_work_credit_snapshots"],
        |row| row.get(0),
    )?;
    if has_team_id {
        return Ok(());
    }
    transaction.execute(
        "ALTER TABLE ops_election_work_credit_snapshots ADD COLUMN team_id BLOB CHECK(team_id IS NULL OR (length(team_id)=16 AND team_id != zeroblob(16)))",
		[],
	)?;
    Ok(())
}

#[cfg(test)]
pub(crate) fn drop_schema_for_legacy_fixture(
    connection: &Connection,
) -> Result<(), OperationsError> {
    crate::agent_request_resolution::drop_schema_for_legacy_fixture(connection)?;
    crate::agent_request::drop_schema_for_legacy_fixture(connection)?;
    connection.execute_batch("DROP TABLE IF EXISTS ops_election_work_credit_snapshots;")?;
    Ok(())
}

#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum WorkCreditSubjectKind {
    WorkspaceParticipation,
    LegacyParticipant,
}

impl WorkCreditSubjectKind {
    const fn encode(self) -> i64 {
        match self {
            Self::WorkspaceParticipation => 0,
            Self::LegacyParticipant => 1,
        }
    }

    fn decode(value: i64) -> Result<Self, OperationsError> {
        match value {
            0 => Ok(Self::WorkspaceParticipation),
            1 => Ok(Self::LegacyParticipant),
            _ => Err(OperationsError::Corrupt("WorkCredit subject kind")),
        }
    }
}

#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkCreditSubjectIdentity {
    kind: WorkCreditSubjectKind,
    subject_id: Uuid,
    profile_id: Option<AgentProfileId>,
    participation_revision: Option<u64>,
}

impl WorkCreditSubjectIdentity {
    pub const fn kind(self) -> WorkCreditSubjectKind {
        self.kind
    }

    #[must_use]
    pub const fn subject_id(self) -> Uuid {
        self.subject_id
    }

    #[must_use]
    pub const fn profile_id(self) -> Option<AgentProfileId> {
        self.profile_id
    }

    /// Decode the typed participation identity for a participation subject.
    ///
    /// # Errors
    /// Returns corruption when a stored participation subject is nil.
    pub fn participation_id(self) -> Result<Option<WorkspaceParticipationId>, OperationsError> {
        match self.kind {
            WorkCreditSubjectKind::WorkspaceParticipation => {
                WorkspaceParticipationId::from_uuid(self.subject_id).map(Some)
            }
            WorkCreditSubjectKind::LegacyParticipant => Ok(None),
        }
    }

    #[must_use]
    pub const fn legacy_participant_id(self) -> Option<Uuid> {
        match self.kind {
            WorkCreditSubjectKind::WorkspaceParticipation => None,
            WorkCreditSubjectKind::LegacyParticipant => Some(self.subject_id),
        }
    }

    #[must_use]
    pub const fn participation_revision(self) -> Option<u64> {
        self.participation_revision
    }
}

#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkCreditSubjectScore {
    subject: WorkCreditSubjectIdentity,
    source_participant_count: u64,
    total_credits: i64,
    entry_count: i64,
}

impl WorkCreditSubjectScore {
    pub const fn subject(self) -> WorkCreditSubjectIdentity {
        self.subject
    }

    #[must_use]
    pub const fn source_participant_count(self) -> u64 {
        self.source_participant_count
    }

    #[must_use]
    pub const fn total_credits(self) -> i64 {
        self.total_credits
    }

    #[must_use]
    pub const fn entry_count(self) -> i64 {
        self.entry_count
    }
}

#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkCreditSubjectFulfillment {
    subject: WorkCreditSubjectIdentity,
    source_participant_count: u64,
    completed: i64,
    evaluated: i64,
}

impl WorkCreditSubjectFulfillment {
    pub const fn subject(self) -> WorkCreditSubjectIdentity {
        self.subject
    }

    #[must_use]
    pub const fn source_participant_count(self) -> u64 {
        self.source_participant_count
    }

    #[must_use]
    pub const fn completed(self) -> i64 {
        self.completed
    }

    #[must_use]
    pub const fn evaluated(self) -> i64 {
        self.evaluated
    }
}

#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkCreditElectionSnapshotReceipt {
    election_id: Uuid,
    workspace_id: WorkspaceId,
    team_id: Option<Uuid>,
    candidate_participant_id: Uuid,
    subject: WorkCreditSubjectIdentity,
    source_participant_count: u64,
    window_ms: i64,
    evaluated_at_ms: i64,
    candidate_total_credits: i64,
    workspace_total_credits: i64,
    workspace_arithmetic_mean: i64,
    mean_remainder_numerator: i64,
    mean_subject_count: i64,
    qualified: bool,
    snapshot_digest: [u8; 32],
}

impl WorkCreditElectionSnapshotReceipt {
    #[must_use]
    pub const fn election_id(self) -> Uuid {
        self.election_id
    }

    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn team_id(self) -> Option<Uuid> {
        self.team_id
    }

    #[must_use]
    pub const fn candidate_participant_id(self) -> Uuid {
        self.candidate_participant_id
    }

    pub const fn subject(self) -> WorkCreditSubjectIdentity {
        self.subject
    }

    #[must_use]
    pub const fn source_participant_count(self) -> u64 {
        self.source_participant_count
    }

    #[must_use]
    pub const fn window_ms(self) -> i64 {
        self.window_ms
    }

    #[must_use]
    pub const fn evaluated_at_ms(self) -> i64 {
        self.evaluated_at_ms
    }

    #[must_use]
    pub const fn candidate_total_credits(self) -> i64 {
        self.candidate_total_credits
    }

    #[must_use]
    pub const fn workspace_total_credits(self) -> i64 {
        self.workspace_total_credits
    }

    #[must_use]
    pub const fn workspace_arithmetic_mean(self) -> i64 {
        self.workspace_arithmetic_mean
    }

    #[must_use]
    pub const fn mean_remainder_numerator(self) -> i64 {
        self.mean_remainder_numerator
    }

    #[must_use]
    pub const fn mean_subject_count(self) -> i64 {
        self.mean_subject_count
    }

    #[must_use]
    pub const fn qualified(self) -> bool {
        self.qualified
    }

    #[must_use]
    pub const fn snapshot_digest(self) -> [u8; 32] {
        self.snapshot_digest
    }
}

#[derive(Debug)]
struct ScoreBuilder {
    subject: WorkCreditSubjectIdentity,
    source_participant_count: u64,
    total_credits: i64,
    entry_count: i64,
}

#[derive(Debug)]
struct FulfillmentBuilder {
    subject: WorkCreditSubjectIdentity,
    source_participant_count: u64,
    completed: i64,
    evaluated: i64,
}

type SubjectMappings = BTreeMap<Uuid, WorkCreditSubjectIdentity>;

fn mapped_subject(
    participation: Vec<u8>,
    profile: Vec<u8>,
    revision: i64,
) -> Result<WorkCreditSubjectIdentity, OperationsError> {
    let revision = u64::try_from(revision)
        .map_err(|_| OperationsError::Corrupt("WorkCredit participation revision"))?;
    if revision == 0 {
        return Err(OperationsError::Corrupt(
            "WorkCredit participation revision",
        ));
    }
    Ok(WorkCreditSubjectIdentity {
        kind: WorkCreditSubjectKind::WorkspaceParticipation,
        subject_id: Uuid::from_bytes(wire::id(participation)?),
        profile_id: Some(
            AgentProfileId::from_uuid(Uuid::from_bytes(wire::id(profile)?))
                .map_err(|_| OperationsError::Corrupt("WorkCredit subject profile"))?,
        ),
        participation_revision: Some(revision),
    })
}

fn load_subject_mappings(
    connection: &Connection,
    workspace_id: WorkspaceId,
) -> Result<SubjectMappings, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT substr(m.participant_id,1,17),substr(p.participation_id,1,17),substr(p.agent_profile_id,1,17),p.revision FROM ops_workspace_participation_mappings m JOIN ops_workspace_participations p ON p.workspace_id=m.workspace_id AND p.participation_id=m.participation_id AND p.agent_profile_id=m.agent_profile_id WHERE m.workspace_id=?1 ORDER BY m.participant_id ASC",
    )?;
    let rows = statement.query_map(params![workspace_id.into_uuid().as_bytes()], |row| {
        Ok((
            row.get::<_, Vec<u8>>(0)?,
            row.get::<_, Vec<u8>>(1)?,
            row.get::<_, Vec<u8>>(2)?,
            row.get::<_, i64>(3)?,
        ))
    })?;
    let mut mappings = BTreeMap::new();
    for row in rows {
        let (participant, participation, profile, revision) = row?;
        let participant_id = Uuid::from_bytes(wire::id(participant)?);
        let subject = mapped_subject(participation, profile, revision)?;
        if mappings.insert(participant_id, subject).is_some() {
            return Err(OperationsError::Corrupt("WorkCredit participant mapping"));
        }
    }
    Ok(mappings)
}

fn subject_for_participant(
    mappings: &SubjectMappings,
    participant_id: Uuid,
) -> WorkCreditSubjectIdentity {
    mappings
        .get(&participant_id)
        .copied()
        .unwrap_or(WorkCreditSubjectIdentity {
            kind: WorkCreditSubjectKind::LegacyParticipant,
            subject_id: participant_id,
            profile_id: None,
            participation_revision: None,
        })
}

pub(crate) fn resolve_subject(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
) -> Result<WorkCreditSubjectIdentity, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(p.participation_id,1,17),substr(p.agent_profile_id,1,17),p.revision FROM ops_workspace_participation_mappings m JOIN ops_workspace_participations p ON p.workspace_id=m.workspace_id AND p.participation_id=m.participation_id AND p.agent_profile_id=m.agent_profile_id WHERE m.workspace_id=?1 AND m.participant_id=?2",
            params![workspace_id.into_uuid().as_bytes(), participant_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional()?;
    let Some((participation, profile, revision)) = raw else {
        return Ok(WorkCreditSubjectIdentity {
            kind: WorkCreditSubjectKind::LegacyParticipant,
            subject_id: participant_id,
            profile_id: None,
            participation_revision: None,
        });
    };
    mapped_subject(participation, profile, revision)
}

pub(crate) fn aggregate_scores(
    connection: &Connection,
    workspace_id: WorkspaceId,
    per_participant: &[crate::work_credit::WorkCreditParticipantScore],
) -> Result<Vec<WorkCreditSubjectScore>, OperationsError> {
    let mappings = load_subject_mappings(connection, workspace_id)?;
    let mut builders = BTreeMap::<(WorkCreditSubjectKind, Uuid), ScoreBuilder>::new();
    for score in per_participant {
        let subject = subject_for_participant(&mappings, score.participant_id());
        let key = (subject.kind(), subject.subject_id());
        let builder = builders.entry(key).or_insert(ScoreBuilder {
            subject,
            source_participant_count: 0,
            total_credits: 0,
            entry_count: 0,
        });
        if builder.subject != subject {
            return Err(OperationsError::Corrupt("WorkCredit subject mapping"));
        }
        builder.source_participant_count = builder
            .source_participant_count
            .checked_add(1)
            .ok_or(OperationsError::Bounds("WorkCredit subject sources"))?;
        builder.total_credits = builder
            .total_credits
            .checked_add(score.total_credits())
            .ok_or(OperationsError::Bounds("WorkCredit subject credits"))?;
        builder.entry_count = builder
            .entry_count
            .checked_add(score.entry_count())
            .ok_or(OperationsError::Bounds("WorkCredit subject entries"))?;
    }
    Ok(builders
        .into_values()
        .map(|builder| WorkCreditSubjectScore {
            subject: builder.subject,
            source_participant_count: builder.source_participant_count,
            total_credits: builder.total_credits,
            entry_count: builder.entry_count,
        })
        .collect())
}

pub(crate) fn aggregate_fulfillment(
    connection: &Connection,
    workspace_id: WorkspaceId,
    per_participant: &[crate::work_credit::WorkCreditParticipantFulfillment],
) -> Result<Vec<WorkCreditSubjectFulfillment>, OperationsError> {
    let mappings = load_subject_mappings(connection, workspace_id)?;
    let mut builders = BTreeMap::<(WorkCreditSubjectKind, Uuid), FulfillmentBuilder>::new();
    for value in per_participant {
        let subject = subject_for_participant(&mappings, value.participant_id());
        let key = (subject.kind(), subject.subject_id());
        let builder = builders.entry(key).or_insert(FulfillmentBuilder {
            subject,
            source_participant_count: 0,
            completed: 0,
            evaluated: 0,
        });
        if builder.subject != subject {
            return Err(OperationsError::Corrupt(
                "WorkCredit fulfillment subject mapping",
            ));
        }
        builder.source_participant_count = builder
            .source_participant_count
            .checked_add(1)
            .ok_or(OperationsError::Bounds("WorkCredit fulfillment sources"))?;
        builder.completed = builder
            .completed
            .checked_add(value.completed())
            .ok_or(OperationsError::Bounds("WorkCredit fulfillment completed"))?;
        builder.evaluated = builder
            .evaluated
            .checked_add(value.evaluated())
            .ok_or(OperationsError::Bounds("WorkCredit fulfillment evaluated"))?;
    }
    Ok(builders
        .into_values()
        .map(|builder| WorkCreditSubjectFulfillment {
            subject: builder.subject,
            source_participant_count: builder.source_participant_count,
            completed: builder.completed,
            evaluated: builder.evaluated,
        })
        .collect())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WorkCreditElectionSnapshotCommand {
    pub(crate) election_id: Uuid,
    pub(crate) candidate_participant_id: Uuid,
    pub(crate) candidate_subject: WorkCreditSubjectIdentity,
    pub(crate) candidate_source_participant_count: u64,
    pub(crate) candidate_total_credits: i64,
    pub(crate) qualified: bool,
    pub(crate) evaluated_at_ms: i64,
}

pub(crate) fn persist_election_snapshot(
    transaction: &Transaction<'_>,
    command: WorkCreditElectionSnapshotCommand,
    snapshot: &WorkCreditScoreSnapshot,
) -> Result<WorkCreditElectionSnapshotReceipt, OperationsError> {
    ensure_team_scope(transaction, snapshot.workspace_id(), snapshot.team_id())?;
    let command = SnapshotValues {
        election_id: command.election_id,
        workspace_id: snapshot.workspace_id(),
        team_id: snapshot.team_id(),
        candidate_participant_id: command.candidate_participant_id,
        candidate_subject: command.candidate_subject,
        candidate_source_participant_count: command.candidate_source_participant_count,
        window_ms: snapshot.window_ms(),
        evaluated_at_ms: command.evaluated_at_ms,
        candidate_total_credits: command.candidate_total_credits,
        workspace_total_credits: snapshot.subject_total_credits(),
        workspace_arithmetic_mean: snapshot.subject_arithmetic_mean(),
        mean_remainder_numerator: snapshot.subject_mean_remainder_numerator(),
        mean_subject_count: snapshot.mean_subject_count(),
        qualified: command.qualified,
    };
    validate_snapshot_values(command)?;
    let digest = snapshot_digest(command);
    if let Some(existing) = load_election_snapshot(transaction, command.election_id)? {
        if existing != receipt_from_values(command, digest) {
            return Err(OperationsError::Conflict(
                "WorkCredit election snapshot replay",
            ));
        }
        return Ok(existing);
    }
    transaction.execute(
        "INSERT INTO ops_election_work_credit_snapshots(election_id,workspace_id,team_id,candidate_participant_id,subject_kind,subject_id,agent_profile_id,participation_revision,subject_revision,source_participant_count,window_ms,evaluated_at_ms,candidate_total_credits,workspace_total_credits,workspace_arithmetic_mean,mean_remainder_numerator,mean_subject_count,qualified,snapshot_digest) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,1,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)",
        params![
            command.election_id.as_bytes(),
            command.workspace_id.into_uuid().as_bytes(),
            command.team_id.map(|id| id.into_bytes().to_vec()),
            command.candidate_participant_id.as_bytes(),
            command.candidate_subject.kind().encode(),
            command.candidate_subject.subject_id().as_bytes(),
            command.candidate_subject.profile_id().map(|id| id.into_uuid().as_bytes().to_vec()),
            command.candidate_subject
                .participation_revision()
                .map(crate::catalog::signed)
                .transpose()?,
            crate::catalog::signed(command.candidate_source_participant_count)?,
            command.window_ms,
            command.evaluated_at_ms,
            command.candidate_total_credits,
            command.workspace_total_credits,
            command.workspace_arithmetic_mean,
            command.mean_remainder_numerator,
            command.mean_subject_count,
            i64::from(command.qualified),
            digest.as_slice(),
        ],
    )?;
    let stored = load_election_snapshot(transaction, command.election_id)?.ok_or(
        OperationsError::Corrupt("WorkCredit election snapshot insert"),
    )?;
    if stored != receipt_from_values(command, digest) {
        return Err(OperationsError::Corrupt(
            "WorkCredit election snapshot readback",
        ));
    }
    Ok(stored)
}

pub(crate) fn subject_score_for_participant(
    connection: &Connection,
    workspace_id: WorkspaceId,
    snapshot: &WorkCreditScoreSnapshot,
    participant_id: Uuid,
) -> Result<(WorkCreditSubjectIdentity, u64, i64), OperationsError> {
    let subject = resolve_subject(connection, workspace_id, participant_id)?;
    let score = snapshot
        .per_subject()
        .iter()
        .find(|score| score.subject() == subject);
    Ok(match score {
        Some(score) => (
            subject,
            score.source_participant_count(),
            score.total_credits(),
        ),
        None => (subject, 1, 0),
    })
}

impl crate::OperationsCatalog {
    /// Read one frozen Phase-F `WorkCredit` qualification snapshot for an election.
    ///
    /// # Errors
    /// Returns storage or corrupt snapshot failures.
    pub fn election_work_credit_snapshot(
        &self,
        election_id: Uuid,
    ) -> Result<Option<WorkCreditElectionSnapshotReceipt>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let result = load_election_snapshot(&transaction, election_id);
        match result {
            Ok(value) => {
                crate::catalog::commit_read(transaction, self.destination)?;
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct SnapshotValues {
    election_id: Uuid,
    workspace_id: WorkspaceId,
    team_id: Option<Uuid>,
    candidate_participant_id: Uuid,
    candidate_subject: WorkCreditSubjectIdentity,
    candidate_source_participant_count: u64,
    window_ms: i64,
    evaluated_at_ms: i64,
    candidate_total_credits: i64,
    workspace_total_credits: i64,
    workspace_arithmetic_mean: i64,
    mean_remainder_numerator: i64,
    mean_subject_count: i64,
    qualified: bool,
}

fn validate_snapshot_values(values: SnapshotValues) -> Result<(), OperationsError> {
    if values.election_id.is_nil()
        || values.candidate_participant_id.is_nil()
        || values.team_id.is_some_and(|team_id| team_id.is_nil())
        || values.candidate_source_participant_count == 0
        || values.window_ms <= 0
        || values.evaluated_at_ms < 0
        || values.candidate_total_credits < 0
        || values.workspace_total_credits < 0
        || values.workspace_arithmetic_mean < 0
        || values.mean_remainder_numerator < 0
        || values.mean_subject_count < 0
        || (values.mean_subject_count == 0 && values.mean_remainder_numerator != 0)
        || (values.mean_subject_count > 0
            && values.mean_remainder_numerator >= values.mean_subject_count)
    {
        return Err(OperationsError::Conflict(
            "WorkCredit election snapshot input",
        ));
    }
    Ok(())
}

fn ensure_team_scope(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Option<Uuid>,
) -> Result<(), OperationsError> {
    let Some(team_id) = team_id else {
        return Ok(());
    };
    if team_id.is_nil() {
        return Err(OperationsError::Conflict("WorkCredit Team identity"));
    }
    let owned: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_teams WHERE workspace_id=?1 AND team_id=?2)",
        params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()],
        |row| row.get(0),
    )?;
    if !owned {
        return Err(OperationsError::Conflict("WorkCredit Team workspace scope"));
    }
    Ok(())
}

fn snapshot_digest(values: SnapshotValues) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(SNAPSHOT_DIGEST_DOMAIN);
    digest.update(values.election_id.as_bytes());
    digest.update(values.workspace_id.into_uuid().as_bytes());
    if let Some(team_id) = values.team_id {
        digest.update([1]);
        digest.update(team_id.as_bytes());
    }
    digest.update(values.candidate_participant_id.as_bytes());
    digest.update(values.candidate_subject.kind().encode().to_be_bytes());
    digest.update(values.candidate_subject.subject_id().as_bytes());
    match values.candidate_subject.profile_id() {
        Some(profile_id) => {
            digest.update([1]);
            digest.update(profile_id.into_uuid().as_bytes());
        }
        None => digest.update([0]),
    }
    digest.update(
        values
            .candidate_subject
            .participation_revision()
            .unwrap_or(0)
            .to_be_bytes(),
    );
    digest.update(values.candidate_source_participant_count.to_be_bytes());
    digest.update(values.window_ms.to_be_bytes());
    digest.update(values.evaluated_at_ms.to_be_bytes());
    digest.update(values.candidate_total_credits.to_be_bytes());
    digest.update(values.workspace_total_credits.to_be_bytes());
    digest.update(values.workspace_arithmetic_mean.to_be_bytes());
    digest.update(values.mean_remainder_numerator.to_be_bytes());
    digest.update(values.mean_subject_count.to_be_bytes());
    digest.update([u8::from(values.qualified)]);
    digest.finalize().into()
}

fn receipt_from_values(
    values: SnapshotValues,
    snapshot_digest: [u8; 32],
) -> WorkCreditElectionSnapshotReceipt {
    WorkCreditElectionSnapshotReceipt {
        election_id: values.election_id,
        workspace_id: values.workspace_id,
        team_id: values.team_id,
        candidate_participant_id: values.candidate_participant_id,
        subject: values.candidate_subject,
        source_participant_count: values.candidate_source_participant_count,
        window_ms: values.window_ms,
        evaluated_at_ms: values.evaluated_at_ms,
        candidate_total_credits: values.candidate_total_credits,
        workspace_total_credits: values.workspace_total_credits,
        workspace_arithmetic_mean: values.workspace_arithmetic_mean,
        mean_remainder_numerator: values.mean_remainder_numerator,
        mean_subject_count: values.mean_subject_count,
        qualified: values.qualified,
        snapshot_digest,
    }
}

fn decode_snapshot_subject(
    kind: i64,
    subject_id: Vec<u8>,
    profile: Option<Vec<u8>>,
    participation_revision: Option<i64>,
    subject_revision: i64,
    source_count: i64,
) -> Result<(WorkCreditSubjectIdentity, u64), OperationsError> {
    let kind = WorkCreditSubjectKind::decode(kind)?;
    let profile_id = profile
        .map(|value| {
            AgentProfileId::from_uuid(Uuid::from_bytes(wire::id(value)?))
                .map_err(|_| OperationsError::Corrupt("WorkCredit snapshot profile"))
        })
        .transpose()?;
    let participation_revision = participation_revision
        .map(|value| {
            let revision = u64::try_from(value).map_err(|_| {
                OperationsError::Corrupt("WorkCredit snapshot participation revision")
            })?;
            if revision == 0 {
                return Err(OperationsError::Corrupt(
                    "WorkCredit snapshot participation revision",
                ));
            }
            Ok(revision)
        })
        .transpose()?;
    let subject = WorkCreditSubjectIdentity {
        kind,
        subject_id: Uuid::from_bytes(wire::id(subject_id)?),
        profile_id,
        participation_revision,
    };
    if subject_revision != 1
        || matches!(kind, WorkCreditSubjectKind::WorkspaceParticipation)
            != (profile_id.is_some() && participation_revision.is_some())
        || matches!(kind, WorkCreditSubjectKind::LegacyParticipant)
            && (profile_id.is_some() || participation_revision.is_some() || source_count != 1)
    {
        return Err(OperationsError::Corrupt("WorkCredit snapshot subject"));
    }
    let source_participant_count = u64::try_from(source_count)
        .map_err(|_| OperationsError::Corrupt("WorkCredit snapshot sources"))?;
    Ok((subject, source_participant_count))
}

fn load_election_snapshot(
    connection: &Connection,
    election_id: Uuid,
) -> Result<Option<WorkCreditElectionSnapshotReceipt>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(workspace_id,1,17),substr(team_id,1,17),substr(candidate_participant_id,1,17),subject_kind,substr(subject_id,1,17),substr(agent_profile_id,1,17),participation_revision,subject_revision,source_participant_count,window_ms,evaluated_at_ms,candidate_total_credits,workspace_total_credits,workspace_arithmetic_mean,mean_remainder_numerator,mean_subject_count,qualified,substr(snapshot_digest,1,33) FROM ops_election_work_credit_snapshots WHERE election_id=?1",
            params![election_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Option<Vec<u8>>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, Option<Vec<u8>>>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, i64>(9)?,
                    row.get::<_, i64>(10)?,
                    row.get::<_, i64>(11)?,
                    row.get::<_, i64>(12)?,
                    row.get::<_, i64>(13)?,
                    row.get::<_, i64>(14)?,
                    row.get::<_, i64>(15)?,
                    row.get::<_, i64>(16)?,
                    row.get::<_, Vec<u8>>(17)?,
                ))
            },
        )
        .optional()?;
    raw.map(
        |(
            workspace,
            team,
            candidate,
            kind,
            subject_id,
            profile,
            participation_revision,
            subject_revision,
            source_count,
            window_ms,
            evaluated_at_ms,
            candidate_total_credits,
            workspace_total_credits,
            workspace_arithmetic_mean,
            mean_remainder_numerator,
            mean_subject_count,
            qualified,
            digest,
        )| {
            let (subject, source_participant_count) = decode_snapshot_subject(
                kind,
                subject_id,
                profile,
                participation_revision,
                subject_revision,
                source_count,
            )?;
            let stored_digest: [u8; 32] = digest
                .try_into()
                .map_err(|_| OperationsError::Corrupt("WorkCredit snapshot digest"))?;
            let values = SnapshotValues {
                election_id,
                workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(workspace)?)),
                team_id: optional_uuid_value(team)?,
                candidate_participant_id: Uuid::from_bytes(wire::id(candidate)?),
                candidate_subject: subject,
                candidate_source_participant_count: source_participant_count,
                window_ms,
                evaluated_at_ms,
                candidate_total_credits,
                workspace_total_credits,
                workspace_arithmetic_mean,
                mean_remainder_numerator,
                mean_subject_count,
                qualified: match qualified {
                    0 => false,
                    1 => true,
                    _ => {
                        return Err(OperationsError::Corrupt(
                            "WorkCredit snapshot qualification",
                        ));
                    }
                },
            };
            ensure_team_scope(connection, values.workspace_id, values.team_id)?;
            validate_snapshot_values(values)
                .map_err(|_| OperationsError::Corrupt("WorkCredit snapshot shape"))?;
            if stored_digest == [0; 32] || stored_digest != snapshot_digest(values) {
                return Err(OperationsError::Corrupt("WorkCredit snapshot digest"));
            }
            Ok(receipt_from_values(values, stored_digest))
        },
    )
    .transpose()
}

fn optional_uuid_value(value: Option<Vec<u8>>) -> Result<Option<Uuid>, OperationsError> {
    value
        .map(|value| Ok(Uuid::from_bytes(wire::id(value)?)))
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_domain::{AgentInstanceId, AgentSessionId};
    use rusqlite::params;
    use std::{error::Error, fs};

    struct SubjectFixture {
        root: std::path::PathBuf,
        catalog: crate::OperationsCatalog,
        workspace: WorkspaceId,
        keeper: Uuid,
        participation_id: WorkspaceParticipationId,
        profile_id: AgentProfileId,
    }

    fn table_sql(connection: &Connection, name: &str) -> Result<String, Box<dyn Error>> {
        Ok(connection.query_row(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?1",
            params![name],
            |row| row.get(0),
        )?)
    }

    fn insert_member(
        connection: &Connection,
        workspace: WorkspaceId,
        participant: Uuid,
        session: AgentSessionId,
        label: &str,
    ) -> Result<(), Box<dyn Error>> {
        connection.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,?4,'worker',1,1,1)",
            params![
                workspace.into_uuid().as_bytes(),
                participant.as_bytes(),
                session.into_uuid().as_bytes(),
                label,
            ],
        )?;
        Ok(())
    }

    fn insert_credit(
        connection: &Connection,
        workspace: WorkspaceId,
        ordinal: u128,
        participant: Uuid,
        budget: i64,
    ) -> Result<(), Box<dyn Error>> {
        connection.execute(
            "INSERT INTO ops_work_credit_entries(credit_id,workspace_id,assignment_id,participant_id,agent_id,activity_id,goal_id,node_id,budget,award_source_sequence,evidence_execution_id,awarded_at_ms,created_digest) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,1,?10,100,?11)",
            params![
                Uuid::from_u128(0xF100 + ordinal).as_bytes(),
                workspace.into_uuid().as_bytes(),
                Uuid::from_u128(0xF200 + ordinal).as_bytes(),
                participant.as_bytes(),
                AgentInstanceId::from_uuid(Uuid::from_u128(0xF300 + ordinal))
                    .into_uuid()
                    .as_bytes(),
                Uuid::from_u128(0xF400 + ordinal).as_bytes(),
                Uuid::from_u128(0xF500 + ordinal).as_bytes(),
                Uuid::from_u128(0xF600 + ordinal).as_bytes(),
                budget,
                Uuid::from_u128(0xF700 + ordinal).as_bytes(),
                [u8::try_from(ordinal + 1)?; 32].as_slice(),
            ],
        )?;
        Ok(())
    }

    fn subject_fixture() -> Result<SubjectFixture, Box<dyn Error>> {
        let root =
            std::env::temp_dir().join(format!("heterarchy-work-credit-subject-{}", Uuid::now_v7()));
        fs::create_dir(&root)?;
        let destination = Uuid::from_u128(0xF001);
        let workspace = WorkspaceId::from_uuid(Uuid::from_u128(0xF002));
        let catalog =
            crate::OperationsCatalog::open(&root.join("operations.sqlite3"), destination)?;
        let participant_a = Uuid::from_u128(0xF010);
        let participant_b = Uuid::from_u128(0xF011);
        let keeper = Uuid::from_u128(0xF012);
        let session_a = AgentSessionId::from_uuid(Uuid::from_u128(0xF020));
        let session_b = AgentSessionId::from_uuid(Uuid::from_u128(0xF021));
        let keeper_session = AgentSessionId::from_uuid(Uuid::from_u128(0xF022));
        insert_member(
            &catalog.connection,
            workspace,
            participant_a,
            session_a,
            "A",
        )?;
        insert_member(
            &catalog.connection,
            workspace,
            participant_b,
            session_b,
            "B",
        )?;
        insert_member(
            &catalog.connection,
            workspace,
            keeper,
            keeper_session,
            "Keeper",
        )?;
        let participation_id = WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xF030))?;
        let profile_id = AgentProfileId::from_uuid(Uuid::from_u128(0xF031))?;
        catalog.connection.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,1,1)",
            params![
                workspace.into_uuid().as_bytes(),
                profile_id.into_uuid().as_bytes(),
                participation_id.into_uuid().as_bytes(),
            ],
        )?;
        for (ordinal, (participant, session)) in
            [(participant_a, session_a), (participant_b, session_b)]
                .into_iter()
                .enumerate()
        {
            catalog.connection.execute(
                "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,1)",
                params![
                    Uuid::from_u128(0xF040 + u128::try_from(ordinal)?).as_bytes(),
                    workspace.into_uuid().as_bytes(),
                    participation_id.into_uuid().as_bytes(),
                    profile_id.into_uuid().as_bytes(),
                    Uuid::from_u128(0xF050 + u128::try_from(ordinal)?).as_bytes(),
                    session.into_uuid().as_bytes(),
                    participant.as_bytes(),
                ],
            )?;
        }
        insert_credit(&catalog.connection, workspace, 0, participant_a, 3)?;
        insert_credit(&catalog.connection, workspace, 1, participant_b, 5)?;
        insert_credit(&catalog.connection, workspace, 2, keeper, 1)?;
        Ok(SubjectFixture {
            root,
            catalog,
            workspace,
            keeper,
            participation_id,
            profile_id,
        })
    }

    #[test]
    fn two_participant_credit_rows_mapped_to_one_participation_aggregate_one_subject_score()
    -> Result<(), Box<dyn Error>> {
        let fixture = subject_fixture()?;
        let snapshot = crate::work_credit::work_credit_scores_at(
            &fixture.catalog.connection,
            fixture.workspace,
            200,
        )?;
        assert_eq!(snapshot.per_subject().len(), 2);
        assert_eq!(snapshot.subject_arithmetic_mean(), 4);
        assert_eq!(snapshot.subject_mean_remainder_numerator(), 1);
        assert_eq!(snapshot.mean_subject_count(), 2);
        let participation_score = snapshot
            .per_subject()
            .iter()
            .find(|score| {
                score.subject().participation_id().ok().flatten() == Some(fixture.participation_id)
            })
            .ok_or("participation WorkCredit score missing")?;
        assert_eq!(participation_score.total_credits(), 8);
        assert_eq!(participation_score.entry_count(), 2);
        assert_eq!(participation_score.source_participant_count(), 2);
        let keeper_score = snapshot
            .per_subject()
            .iter()
            .find(|score| score.subject().legacy_participant_id() == Some(fixture.keeper))
            .ok_or("legacy WorkCredit subject missing")?;
        assert_eq!(keeper_score.total_credits(), 1);
        assert_eq!(snapshot.total_credits(), 9);
        drop(fixture.catalog);
        fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn subject_resolution_keeps_same_profile_isolated_by_workspace() -> Result<(), Box<dyn Error>> {
        let fixture = subject_fixture()?;
        let workspace_b = WorkspaceId::from_uuid(Uuid::from_u128(0xF802));
        let participant_b = Uuid::from_u128(0xF810);
        let session_b = AgentSessionId::from_uuid(Uuid::from_u128(0xF820));
        let participation_b = WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xF830))?;
        insert_member(
            &fixture.catalog.connection,
            workspace_b,
            participant_b,
            session_b,
            "Workspace B",
        )?;
        fixture.catalog.connection.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,1,1)",
            params![
                workspace_b.into_uuid().as_bytes(),
                fixture.profile_id.into_uuid().as_bytes(),
                participation_b.into_uuid().as_bytes(),
            ],
        )?;
        fixture.catalog.connection.execute(
            "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,1)",
            params![
                Uuid::from_u128(0xF840).as_bytes(),
                workspace_b.into_uuid().as_bytes(),
                participation_b.into_uuid().as_bytes(),
                fixture.profile_id.into_uuid().as_bytes(),
                Uuid::from_u128(0xF850).as_bytes(),
                session_b.into_uuid().as_bytes(),
                participant_b.as_bytes(),
            ],
        )?;
        insert_credit(
            &fixture.catalog.connection,
            workspace_b,
            9,
            participant_b,
            5,
        )?;
        let snapshot_a = crate::work_credit::work_credit_scores_at(
            &fixture.catalog.connection,
            fixture.workspace,
            200,
        )?;
        let snapshot_b = crate::work_credit::work_credit_scores_at(
            &fixture.catalog.connection,
            workspace_b,
            200,
        )?;
        assert_eq!(snapshot_a.subject_total_credits(), 9);
        assert_eq!(snapshot_b.subject_total_credits(), 5);
        assert_eq!(snapshot_b.per_subject().len(), 1);
        assert_eq!(
            snapshot_b.per_subject()[0].subject().participation_id()?,
            Some(participation_b)
        );
        drop(fixture.catalog);
        fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn team_snapshot_persistence_rejects_foreign_team_scope() -> Result<(), Box<dyn Error>> {
        let fixture = subject_fixture()?;
        let team_id = Uuid::from_u128(0xF860);
        fixture.catalog.connection.execute(
            "INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_by_participation_id,created_by_profile_id,created_by_participant_id,created_by_session_id,created_at_ms,updated_at_ms) VALUES(?1,?2,'Snapshot Team','scope','ACTIVE',1,'operator','Local operator',NULL,NULL,NULL,NULL,1,1)",
            params![team_id.as_bytes(), fixture.workspace.into_uuid().as_bytes()],
        )?;
        let snapshot = crate::work_credit::team_work_credit_scores_at(
            &fixture.catalog.connection,
            fixture.workspace,
            team_id,
            200,
        )?;
        fixture.catalog.connection.execute(
            "DELETE FROM ops_teams WHERE workspace_id=?1 AND team_id=?2",
            params![fixture.workspace.into_uuid().as_bytes(), team_id.as_bytes()],
        )?;
        let foreign_workspace = WorkspaceId::from_uuid(Uuid::from_u128(0xF862));
        fixture.catalog.connection.execute(
            "INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_by_participation_id,created_by_profile_id,created_by_participant_id,created_by_session_id,created_at_ms,updated_at_ms) VALUES(?1,?2,'Foreign Snapshot Team','scope','ACTIVE',1,'operator','Local operator',NULL,NULL,NULL,NULL,1,1)",
            params![team_id.as_bytes(), foreign_workspace.into_uuid().as_bytes()],
        )?;
        let command = WorkCreditElectionSnapshotCommand {
            election_id: Uuid::from_u128(0xF861),
            candidate_participant_id: fixture.keeper,
            candidate_subject: WorkCreditSubjectIdentity {
                kind: WorkCreditSubjectKind::LegacyParticipant,
                subject_id: fixture.keeper,
                profile_id: None,
                participation_revision: None,
            },
            candidate_source_participant_count: 1,
            candidate_total_credits: 0,
            qualified: false,
            evaluated_at_ms: 200,
        };
        let result = {
            let transaction = fixture.catalog.connection.unchecked_transaction()?;
            persist_election_snapshot(&transaction, command, &snapshot)
        };
        assert!(matches!(
            result,
            Err(OperationsError::Conflict("WorkCredit Team workspace scope"))
        ));
        drop(fixture.catalog);
        fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn v24_to_v25_adds_only_election_work_credit_snapshot_table() -> Result<(), Box<dyn Error>> {
        let root =
            std::env::temp_dir().join(format!("heterarchy-work-credit-current-{}", Uuid::now_v7()));
        fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xF900);
        let catalog = crate::OperationsCatalog::open(&path, destination)?;
        drop(catalog);
        let connection = Connection::open(&path)?;
        let ledger_before = table_sql(&connection, "ops_work_credit_entries")?;
        let participation_before = table_sql(&connection, "ops_workspace_participations")?;
        let leadership_before = table_sql(&connection, "ops_leadership")?;
        let election_subject_before = table_sql(&connection, "ops_election_voter_subjects")?;
        drop_schema_for_legacy_fixture(&connection)?;
        connection.pragma_update(None, "user_version", 24_i64)?;
        drop(connection);
        let migrated = crate::OperationsCatalog::open(&path, destination)?;
        let version: i64 = migrated
            .connection
            .pragma_query_value(None, "user_version", |row| row.get(0))?;
        assert_eq!(version, crate::catalog::VERSION);
        assert_eq!(
            table_sql(&migrated.connection, "ops_work_credit_entries")?,
            ledger_before
        );
        assert_eq!(
            table_sql(&migrated.connection, "ops_workspace_participations")?,
            participation_before
        );
        crate::catalog::assert_fixture_table_schema(
            &migrated.connection,
            "ops_leadership",
            &leadership_before,
        )?;
        assert_eq!(
            table_sql(&migrated.connection, "ops_election_voter_subjects")?,
            election_subject_before
        );
        let added: i64 = migrated.connection.query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name='ops_election_work_credit_snapshots'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(added, 1);
        drop(migrated);
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
