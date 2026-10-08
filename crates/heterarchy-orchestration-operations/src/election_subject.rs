//! Phase-E participation-aware election voter authority.
//!
//! Candidate membership and the frozen voting electorate are separate: the
//! former includes current Workspace members; the latter contains only eligible
//! current Team members or Workspace Team leaders.
use crate::{
    OperationsCatalog, OperationsError, WorkspaceParticipationId,
    leadership::{
        CURRENT_LEADERSHIP_AUTHORITY_PREDICATE, LeadershipScope, NEW_MEMBER_PROBATION_MS,
    },
    wire,
};
use forge_domain::{ActivityId, WorkspaceId};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use uuid::Uuid;

const MAX_VOTER_SOURCES: usize = 64;
const CUTOVER_DIGEST_DOMAIN: &[u8] = b"heterarchy.election-voter-subject-cutover.v2\0";

const CUTOVERS_SQL_V31: &str = "CREATE TABLE ops_election_subject_cutovers (election_id BLOB PRIMARY KEY NOT NULL REFERENCES ops_leadership_elections(election_id) CHECK(length(election_id)=16 AND election_id != zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), scope_kind INTEGER NOT NULL CHECK(scope_kind IN(0,1)), activity_id BLOB CHECK(activity_id IS NULL OR length(activity_id)=16), source_leadership_revision INTEGER NOT NULL CHECK(source_leadership_revision>=1), result_leadership_revision INTEGER NOT NULL CHECK(result_leadership_revision=source_leadership_revision+1), subject_revision INTEGER NOT NULL CHECK(subject_revision=1), source_participant_count INTEGER NOT NULL CHECK(source_participant_count BETWEEN 1 AND 64), voter_subject_count INTEGER NOT NULL CHECK(voter_subject_count BETWEEN 1 AND source_participant_count), electorate_digest BLOB NOT NULL CHECK(length(electorate_digest)=32 AND electorate_digest != zeroblob(32)), created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0), CHECK((scope_kind=0 AND activity_id IS NULL) OR (scope_kind=1 AND activity_id IS NOT NULL)))";
const CUTOVERS_SQL: &str = "CREATE TABLE ops_election_subject_cutovers (election_id BLOB PRIMARY KEY NOT NULL REFERENCES ops_leadership_elections(election_id) CHECK(length(election_id)=16 AND election_id != zeroblob(16)), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), scope_kind INTEGER NOT NULL CHECK(scope_kind IN(0,1,2)), activity_id BLOB CHECK(activity_id IS NULL OR length(activity_id)=16), team_id BLOB CHECK(team_id IS NULL OR length(team_id)=16), source_leadership_revision INTEGER NOT NULL CHECK(source_leadership_revision>=1), result_leadership_revision INTEGER NOT NULL CHECK(result_leadership_revision=source_leadership_revision+1), subject_revision INTEGER NOT NULL CHECK(subject_revision=1), source_participant_count INTEGER NOT NULL CHECK(source_participant_count BETWEEN 1 AND 64), voter_subject_count INTEGER NOT NULL CHECK(voter_subject_count BETWEEN 1 AND source_participant_count), electorate_digest BLOB NOT NULL CHECK(length(electorate_digest)=32 AND electorate_digest != zeroblob(32)), created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0), CHECK((scope_kind=0 AND activity_id IS NULL AND team_id IS NULL) OR (scope_kind=1 AND activity_id IS NOT NULL AND team_id IS NULL) OR (scope_kind=2 AND activity_id IS NULL AND team_id IS NOT NULL)))";
const SUBJECTS_SQL: &str = "CREATE TABLE ops_election_voter_subjects (election_id BLOB NOT NULL REFERENCES ops_election_subject_cutovers(election_id) CHECK(length(election_id)=16), subject_kind INTEGER NOT NULL CHECK(subject_kind IN(0,1)), subject_id BLOB NOT NULL CHECK(length(subject_id)=16 AND subject_id != zeroblob(16)), agent_profile_id BLOB CHECK(agent_profile_id IS NULL OR (length(agent_profile_id)=16 AND agent_profile_id != zeroblob(16))), participation_revision INTEGER CHECK(participation_revision IS NULL OR participation_revision>0), source_participant_count INTEGER NOT NULL CHECK(source_participant_count BETWEEN 1 AND 64), PRIMARY KEY(election_id,subject_kind,subject_id), CHECK((subject_kind=0 AND agent_profile_id IS NOT NULL AND participation_revision IS NOT NULL) OR (subject_kind=1 AND agent_profile_id IS NULL AND participation_revision IS NULL AND source_participant_count=1)))";
const SOURCES_SQL: &str = "CREATE TABLE ops_election_voter_sources (election_id BLOB NOT NULL CHECK(length(election_id)=16), participant_id BLOB NOT NULL CHECK(length(participant_id)=16 AND participant_id != zeroblob(16)), membership_revision INTEGER NOT NULL CHECK(membership_revision>0), team_membership_revision INTEGER CHECK(team_membership_revision IS NULL OR team_membership_revision>0), subject_kind INTEGER NOT NULL CHECK(subject_kind IN(0,1)), subject_id BLOB NOT NULL CHECK(length(subject_id)=16 AND subject_id != zeroblob(16)), PRIMARY KEY(election_id,participant_id), FOREIGN KEY(election_id,subject_kind,subject_id) REFERENCES ops_election_voter_subjects(election_id,subject_kind,subject_id))";
const SOURCES_SQL_V34: &str = "CREATE TABLE ops_election_voter_sources (election_id BLOB NOT NULL CHECK(length(election_id)=16), participant_id BLOB NOT NULL CHECK(length(participant_id)=16 AND participant_id != zeroblob(16)), membership_revision INTEGER NOT NULL CHECK(membership_revision>0), subject_kind INTEGER NOT NULL CHECK(subject_kind IN(0,1)), subject_id BLOB NOT NULL CHECK(length(subject_id)=16 AND subject_id != zeroblob(16)), PRIMARY KEY(election_id,participant_id), FOREIGN KEY(election_id,subject_kind,subject_id) REFERENCES ops_election_voter_subjects(election_id,subject_kind,subject_id))";
const BALLOTS_SQL: &str = "CREATE TABLE ops_election_subject_ballots (election_id BLOB NOT NULL CHECK(length(election_id)=16), subject_kind INTEGER NOT NULL CHECK(subject_kind IN(0,1)), subject_id BLOB NOT NULL CHECK(length(subject_id)=16 AND subject_id != zeroblob(16)), cast_by_participant_id BLOB NOT NULL CHECK(length(cast_by_participant_id)=16 AND cast_by_participant_id != zeroblob(16)), choice TEXT NOT NULL CHECK(choice IN('YES','NO','ABSTAIN')), ballot_revision INTEGER NOT NULL CHECK(ballot_revision=1), cast_at_ms INTEGER NOT NULL CHECK(cast_at_ms>=0), PRIMARY KEY(election_id,subject_kind,subject_id), FOREIGN KEY(election_id,subject_kind,subject_id) REFERENCES ops_election_voter_subjects(election_id,subject_kind,subject_id))";
const TEAM_LEADER_SOURCES_SQL: &str = "CREATE TABLE ops_election_team_leader_sources (election_id BLOB NOT NULL REFERENCES ops_election_subject_cutovers(election_id) CHECK(length(election_id)=16), team_id BLOB NOT NULL CHECK(length(team_id)=16 AND team_id != zeroblob(16)), leader_participant_id BLOB NOT NULL CHECK(length(leader_participant_id)=16 AND leader_participant_id != zeroblob(16)), leader_participation_id BLOB NOT NULL CHECK(length(leader_participation_id)=16 AND leader_participation_id != zeroblob(16)), team_revision INTEGER NOT NULL CHECK(team_revision>0), leadership_revision INTEGER NOT NULL CHECK(leadership_revision>0), team_membership_revision INTEGER NOT NULL CHECK(team_membership_revision>0), PRIMARY KEY(election_id,team_id))";

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    let mut objects = schema_objects_v34();
    objects.retain(|(kind, name, _)| *kind != "table" || *name != "ops_election_voter_sources");
    objects.push(("table", "ops_election_voter_sources", SOURCES_SQL));
    objects.push((
        "table",
        "ops_election_team_leader_sources",
        TEAM_LEADER_SOURCES_SQL,
    ));
    objects
}

/// Exact election-subject schema objects before Workspace electorate
/// provenance was added at catalog version 35.
pub(crate) fn schema_objects_v34() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("table", "ops_election_subject_cutovers", CUTOVERS_SQL),
        ("table", "ops_election_voter_subjects", SUBJECTS_SQL),
        ("table", "ops_election_voter_sources", SOURCES_SQL_V34),
        ("table", "ops_election_subject_ballots", BALLOTS_SQL),
    ]
}

pub(crate) fn schema_objects_v31() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("table", "ops_election_subject_cutovers", CUTOVERS_SQL_V31),
        ("table", "ops_election_voter_subjects", SUBJECTS_SQL),
        ("table", "ops_election_voter_sources", SOURCES_SQL_V34),
        ("table", "ops_election_subject_ballots", BALLOTS_SQL),
    ]
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in schema_objects() {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

pub(crate) fn migrate_v34_to_v35(tx: &Transaction<'_>) -> Result<(), OperationsError> {
    if !table_has_column(tx, "ops_election_voter_sources", "team_membership_revision")? {
        tx.execute_batch(&SOURCES_SQL.replacen(
            "CREATE TABLE ops_election_voter_sources ",
            "CREATE TABLE ops_election_voter_sources_v35 ",
            1,
        ))?;
        tx.execute(
            "INSERT INTO ops_election_voter_sources_v35(election_id,participant_id,membership_revision,team_membership_revision,subject_kind,subject_id) SELECT election_id,participant_id,membership_revision,NULL,subject_kind,subject_id FROM ops_election_voter_sources",
            [],
        )?;
        tx.execute_batch("DROP TABLE ops_election_voter_sources; ALTER TABLE ops_election_voter_sources_v35 RENAME TO ops_election_voter_sources")?;
    }
    let exists = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='ops_election_team_leader_sources')",
        [],
        |row| row.get::<_, bool>(0),
    )?;
    if !exists {
        tx.execute_batch(TEAM_LEADER_SOURCES_SQL)?;
    }
    Ok(())
}

/// Rebuild the one changed election-subject table from catalog v31. The
/// catalog migration owner disables foreign-key enforcement only outside its
/// IMMEDIATE transaction, rebuilds the parent election table in the same
/// migration, runs `foreign_key_check`, and restores enforcement before
/// returning. This helper never toggles connection-wide pragmas itself.
pub(crate) fn migrate_v31_to_v32(tx: &Transaction<'_>) -> Result<(), OperationsError> {
    if table_has_column(tx, "ops_election_subject_cutovers", "team_id")? {
        return Ok(());
    }
    tx.execute_batch(&CUTOVERS_SQL.replacen(
        "CREATE TABLE ops_election_subject_cutovers ",
        "CREATE TABLE ops_election_subject_cutovers_v32 ",
        1,
    ))?;
    tx.execute(
        "INSERT INTO ops_election_subject_cutovers_v32(election_id,workspace_id,scope_kind,activity_id,team_id,source_leadership_revision,result_leadership_revision,subject_revision,source_participant_count,voter_subject_count,electorate_digest,created_at_ms) SELECT election_id,workspace_id,scope_kind,activity_id,NULL,source_leadership_revision,result_leadership_revision,subject_revision,source_participant_count,voter_subject_count,electorate_digest,created_at_ms FROM ops_election_subject_cutovers",
        [],
    )?;
    tx.execute("DROP TABLE ops_election_subject_cutovers", [])?;
    tx.execute_batch(
        "ALTER TABLE ops_election_subject_cutovers_v32 RENAME TO ops_election_subject_cutovers",
    )?;
    Ok(())
}

fn table_has_column(
    connection: &Connection,
    table: &str,
    column: &str,
) -> Result<bool, OperationsError> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        if name == column {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
pub(crate) fn drop_schema_for_legacy_fixture(
    connection: &Connection,
) -> Result<(), OperationsError> {
    crate::work_credit_subject::drop_schema_for_legacy_fixture(connection)?;
    connection.execute_batch(
        "DROP TABLE IF EXISTS ops_election_team_leader_sources;          DROP TABLE IF EXISTS ops_election_subject_ballots;          DROP TABLE IF EXISTS ops_election_voter_sources;          DROP TABLE IF EXISTS ops_election_voter_subjects;          DROP TABLE IF EXISTS ops_election_subject_cutovers;",
    )?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum VoterSubjectKind {
    WorkspaceParticipation,
    LegacyParticipant,
}

impl VoterSubjectKind {
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
            _ => Err(OperationsError::Corrupt("election voter subject kind")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VoterSubject {
    kind: VoterSubjectKind,
    subject_id: Uuid,
    profile_id: Option<Uuid>,
    participation_revision: Option<u64>,
    source_participant_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VoterSource {
    participant_id: Uuid,
    membership_revision: u64,
    team_membership_revision: Option<u64>,
    kind: VoterSubjectKind,
    subject_id: Uuid,
}

#[derive(Debug, Clone, Copy)]
struct VoterCandidate {
    participant_id: Uuid,
    membership_revision: u64,
    team_membership_revision: Option<u64>,
    joined_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TeamLeaderSource {
    team_id: Uuid,
    leader_participant_id: Uuid,
    leader_participation_id: Uuid,
    team_revision: u64,
    leadership_revision: u64,
    team_membership_revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ElectionSubjectCutoverCommand {
    pub(crate) election_id: Uuid,
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) activity_id: Option<ActivityId>,
    pub(crate) team_id: Option<Uuid>,
    pub(crate) source_leadership_revision: u64,
    pub(crate) result_leadership_revision: u64,
    pub(crate) created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ElectionSubjectSnapshot {
    subjects: Vec<VoterSubject>,
    sources: Vec<VoterSource>,
    team_leaders: Vec<TeamLeaderSource>,
}

impl ElectionSubjectSnapshot {
    pub(crate) fn participant_ids(&self) -> impl Iterator<Item = Uuid> + '_ {
        self.sources.iter().map(|source| source.participant_id)
    }

    pub(crate) fn contains_participant(&self, participant_id: Uuid) -> bool {
        self.sources
            .iter()
            .any(|source| source.participant_id == participant_id)
    }

    pub(crate) fn voter_subject_count(&self) -> usize {
        self.subjects.len()
    }

    fn source_participant_count(&self) -> usize {
        self.sources.len()
    }
}
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ElectionSubjectCutoverReceipt {
    election_id: Uuid,
    workspace_id: WorkspaceId,
    activity_id: Option<ActivityId>,
    team_id: Option<Uuid>,
    source_leadership_revision: u64,
    result_leadership_revision: u64,
    subject_revision: u64,
    source_participant_count: u64,
    voter_subject_count: u64,
    electorate_digest: [u8; 32],
    created_at_ms: i64,
}

impl ElectionSubjectCutoverReceipt {
    #[must_use]
    pub const fn election_id(self) -> Uuid {
        self.election_id
    }

    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn activity_id(self) -> Option<ActivityId> {
        self.activity_id
    }

    #[must_use]
    pub const fn team_id(self) -> Option<Uuid> {
        self.team_id
    }

    #[must_use]
    pub const fn source_leadership_revision(self) -> u64 {
        self.source_leadership_revision
    }

    #[must_use]
    pub const fn result_leadership_revision(self) -> u64 {
        self.result_leadership_revision
    }

    #[must_use]
    pub const fn subject_revision(self) -> u64 {
        self.subject_revision
    }

    #[must_use]
    pub const fn source_participant_count(self) -> u64 {
        self.source_participant_count
    }

    #[must_use]
    pub const fn voter_subject_count(self) -> u64 {
        self.voter_subject_count
    }

    #[must_use]
    pub const fn electorate_digest(self) -> [u8; 32] {
        self.electorate_digest
    }

    #[must_use]
    pub const fn created_at_ms(self) -> i64 {
        self.created_at_ms
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubjectBallotOutcome {
    stored_choice: String,
    replay: bool,
    voter_participation_id: Option<WorkspaceParticipationId>,
}

impl SubjectBallotOutcome {
    pub(crate) fn stored_choice(&self) -> &str {
        &self.stored_choice
    }

    pub(crate) const fn replay(&self) -> bool {
        self.replay
    }

    pub(crate) const fn voter_participation_id(&self) -> Option<WorkspaceParticipationId> {
        self.voter_participation_id
    }
}

#[derive(Debug)]
struct SubjectBuilder {
    kind: VoterSubjectKind,
    subject_id: Uuid,
    profile_id: Option<Uuid>,
    participation_revision: Option<u64>,
    source_count: u64,
}

pub(crate) fn snapshot_ready_subjects(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    activity_id: Option<ActivityId>,
) -> Result<ElectionSubjectSnapshot, OperationsError> {
    let sources = ready_participant_sources(transaction, workspace_id, activity_id)?;
    build_snapshot(transaction, workspace_id, sources, true)
}

/// Current membership pool for candidate selection and authority readback.
pub(crate) fn snapshot_ready_subjects_for_scope(
    transaction: &Transaction<'_>,
    scope: LeadershipScope,
) -> Result<ElectionSubjectSnapshot, OperationsError> {
    if scope.team_id().is_none() {
        return snapshot_ready_subjects(transaction, scope.workspace_id(), scope.activity_id());
    }
    let team_id = scope
        .team_id()
        .ok_or(OperationsError::Corrupt("election leadership scope target"))?;
    let sources = ready_team_participant_sources(transaction, scope.workspace_id(), team_id)?;
    build_snapshot(transaction, scope.workspace_id(), sources, false)
}

/// Freeze the voting pool and eligibility inside the election-start transaction.
pub(crate) fn snapshot_voting_subjects_for_scope(
    transaction: &Transaction<'_>,
    scope: LeadershipScope,
    now_ms: i64,
) -> Result<ElectionSubjectSnapshot, OperationsError> {
    if now_ms < 0 {
        return Err(OperationsError::Conflict("leadership electorate time"));
    }
    let (candidates, mut team_leaders) = if let Some(team_id) = scope.team_id() {
        (
            ready_team_voter_candidates(transaction, scope.workspace_id(), team_id)?,
            Vec::new(),
        )
    } else if scope.activity_id().is_none() {
        let mut participants = BTreeMap::new();
        let mut leaders = Vec::new();
        for (participant, revision, started_at, provenance) in
            ready_workspace_team_leaders(transaction, scope.workspace_id())?
        {
            leaders.push(provenance);
            let identity = (revision, started_at);
            if let Some(previous) = participants.insert(participant, identity)
                && previous != identity
            {
                return Err(OperationsError::Corrupt("workspace Team leader mapping"));
            }
        }
        (
            participants
                .into_iter()
                .map(
                    |(participant_id, (membership_revision, joined_at_ms))| VoterCandidate {
                        participant_id,
                        membership_revision,
                        joined_at_ms,
                        team_membership_revision: None,
                    },
                )
                .collect(),
            leaders,
        )
    } else {
        return snapshot_ready_subjects(transaction, scope.workspace_id(), scope.activity_id());
    };
    let per_participant =
        crate::work_credit::participant_fulfillment_rows(transaction, scope.workspace_id())?;
    let fulfillment = crate::work_credit_subject::aggregate_fulfillment(
        transaction,
        scope.workspace_id(),
        &per_participant,
    )?;
    let mut eligible = BTreeMap::new();
    for candidate in candidates {
        if now_ms.saturating_sub(candidate.joined_at_ms) < NEW_MEMBER_PROBATION_MS {
            continue;
        }
        let subject = crate::work_credit_subject::resolve_subject(
            transaction,
            scope.workspace_id(),
            candidate.participant_id,
        )?;
        let ineligible = fulfillment
            .iter()
            .find(|row| row.subject() == subject)
            .is_some_and(|row| {
                row.evaluated() > 0
                    && i128::from(row.completed())
                        * i128::from(crate::leadership::VOTER_FULFILLMENT_INELIGIBLE_DEN)
                        <= i128::from(row.evaluated())
                            * i128::from(crate::leadership::VOTER_FULFILLMENT_INELIGIBLE_NUM)
            });
        if !ineligible {
            eligible.insert(candidate.participant_id, candidate);
        }
    }
    let sources = eligible
        .values()
        .map(|candidate| (candidate.participant_id, candidate.membership_revision))
        .collect();
    let mut snapshot = build_snapshot(transaction, scope.workspace_id(), sources, false)?;
    for source in &mut snapshot.sources {
        source.team_membership_revision = eligible
            .get(&source.participant_id)
            .and_then(|candidate| candidate.team_membership_revision);
    }
    team_leaders.retain(|source| snapshot.contains_participant(source.leader_participant_id));
    snapshot.team_leaders = team_leaders;
    if snapshot.sources.is_empty() || snapshot.subjects.is_empty() {
        return Err(OperationsError::Conflict("leadership electorate empty"));
    }
    Ok(snapshot)
}

fn ready_team_voter_candidates(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<Vec<VoterCandidate>, OperationsError> {
    let limit = i64::try_from(MAX_VOTER_SOURCES + 1)
        .map_err(|_| OperationsError::Bounds("leadership electorate"))?;
    let mut statement=connection.prepare(
        "SELECT DISTINCT substr(m.participant_id,1,17),lm.membership_revision,tm.revision,tm.joined_at_ms
         FROM ops_team_members tm
         JOIN ops_teams t ON t.workspace_id=tm.workspace_id AND t.team_id=tm.team_id
         JOIN ops_workspace_participation_mappings m ON m.workspace_id=tm.workspace_id AND m.participation_id=tm.participation_id
         JOIN ops_workspace_participations p ON p.workspace_id=m.workspace_id AND p.participation_id=m.participation_id AND p.agent_profile_id=m.agent_profile_id
         JOIN ops_logical_members lm ON lm.workspace_id=m.workspace_id AND lm.participant_id=m.participant_id AND lm.membership_revision=m.participant_membership_revision
         WHERE tm.workspace_id=?1 AND tm.team_id=?2 AND tm.state='JOINED' AND t.state='ACTIVE'
           AND EXISTS(SELECT 1 FROM ops_current_effect_bindings current_binding
             WHERE current_binding.workspace_id=lm.workspace_id
               AND current_binding.participant_id=lm.participant_id
               AND current_binding.session_id=lm.session_id)
         ORDER BY m.participant_id LIMIT ?3")?;
    let rows = statement
        .query_map(
            params![
                workspace_id.into_uuid().as_bytes(),
                team_id.as_bytes(),
                limit
            ],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;
    if rows.len() > MAX_VOTER_SOURCES {
        return Err(OperationsError::Bounds("leadership electorate"));
    }
    rows.into_iter()
        .map(|(participant, revision, team_revision, joined)| {
            let revision = u64::try_from(revision)
                .map_err(|_| OperationsError::Corrupt("election member revision"))?;
            let team_revision = u64::try_from(team_revision)
                .map_err(|_| OperationsError::Corrupt("election Team member revision"))?;
            if revision == 0 || team_revision == 0 || joined < 0 {
                return Err(OperationsError::Corrupt("election team membership"));
            }
            Ok(VoterCandidate {
                participant_id: Uuid::from_bytes(wire::id(participant)?),
                membership_revision: revision,
                team_membership_revision: Some(team_revision),
                joined_at_ms: joined,
            })
        })
        .collect()
}

// Team source reads share these membership joins; the leadership authority
// gate itself is owned by leadership.rs and is appended by both callers.
// Age and fulfillment are frozen only by the start path. Mapping Sessions are
// immutable origin evidence; the logical member owns the current Session.
const CURRENT_CONFIRMED_TEAM_LEADER_FROM: &str = "FROM ops_teams t
         JOIN ops_leadership l
           ON l.workspace_id=t.workspace_id AND l.scope_kind=2 AND l.team_id=t.team_id
         JOIN ops_workspace_participation_mappings m
           ON m.workspace_id=l.workspace_id AND m.participant_id=l.leader_participant_id
         JOIN ops_workspace_participations p
           ON p.workspace_id=m.workspace_id AND p.participation_id=m.participation_id
              AND p.agent_profile_id=m.agent_profile_id
         JOIN ops_team_members tm
           ON tm.workspace_id=t.workspace_id AND tm.team_id=t.team_id
              AND tm.participation_id=m.participation_id AND tm.state='JOINED'
         JOIN ops_logical_members lm
           ON lm.workspace_id=m.workspace_id AND lm.participant_id=m.participant_id
              AND lm.membership_revision=m.participant_membership_revision
         WHERE t.workspace_id=?1 AND t.state='ACTIVE'";

pub(crate) fn workspace_electorate_lost_team_leadership(
    connection: &Connection,
    workspace: WorkspaceId,
    election: Uuid,
) -> Result<bool, OperationsError> {
    if !has_cutover(connection, election)? {
        return Err(OperationsError::Conflict(
            "legacy Workspace electorate requires policy cutover",
        ));
    }
    // The outer snapshot is bounded by MAX_VOTER_SOURCES. An Agent with
    // multiple Teams or source aliases retains its seat until its last role is lost.
    Ok(connection.query_row(
        &format!(
            "SELECT EXISTS(SELECT 1 FROM ops_election_voter_subjects s
            WHERE s.election_id=?2 AND (s.subject_kind!=0 OR NOT EXISTS(
                SELECT 1 {CURRENT_CONFIRMED_TEAM_LEADER_FROM}
                AND ({CURRENT_LEADERSHIP_AUTHORITY_PREDICATE})
                AND p.participation_id=s.subject_id)))"
        ),
        params![workspace.into_uuid().as_bytes(), election.as_bytes()],
        |row| row.get(0),
    )?)
}

fn ready_workspace_team_leaders(
    connection: &Connection,
    workspace_id: WorkspaceId,
) -> Result<Vec<(Uuid, u64, i64, TeamLeaderSource)>, OperationsError> {
    let limit = i64::try_from(MAX_VOTER_SOURCES + 1)
        .map_err(|_| OperationsError::Bounds("leadership electorate"))?;
    let mut statement = connection.prepare(&format!(
        "SELECT DISTINCT substr(t.team_id,1,17),substr(l.leader_participant_id,1,17),
                substr(tm.participation_id,1,17),tm.revision,t.revision,l.leadership_revision,
                lm.membership_revision,lm.created_at_ms
         {CURRENT_CONFIRMED_TEAM_LEADER_FROM}
         AND ({CURRENT_LEADERSHIP_AUTHORITY_PREDICATE})
         AND ({formation})
         ORDER BY t.team_id LIMIT ?2",
        formation = crate::teams::FORMED_TEAM_ADMISSION_PREDICATE
    ))?;
    let rows = statement
        .query_map(params![workspace_id.into_uuid().as_bytes(), limit], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, i64>(7)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    if rows.len() > MAX_VOTER_SOURCES {
        return Err(OperationsError::Bounds(
            "leadership electorate Team sources",
        ));
    }
    rows.into_iter()
        .map(
            |(
                team,
                participant,
                participation,
                team_member_rev,
                team_rev,
                leadership_rev,
                member_rev,
                created,
            )| {
                let team_membership_revision = u64::try_from(team_member_rev)
                    .map_err(|_| OperationsError::Corrupt("election Team member revision"))?;
                let team_revision = u64::try_from(team_rev)
                    .map_err(|_| OperationsError::Corrupt("election Team revision"))?;
                let leadership_revision = u64::try_from(leadership_rev)
                    .map_err(|_| OperationsError::Corrupt("election Team leadership revision"))?;
                let member_revision = u64::try_from(member_rev)
                    .map_err(|_| OperationsError::Corrupt("election member revision"))?;
                if team_membership_revision == 0
                    || team_revision == 0
                    || leadership_revision == 0
                    || member_revision == 0
                    || created < 0
                {
                    return Err(OperationsError::Corrupt("election Team leader source"));
                }
                let leader = Uuid::from_bytes(wire::id(participant)?);
                Ok((
                    leader,
                    member_revision,
                    created,
                    TeamLeaderSource {
                        team_id: Uuid::from_bytes(wire::id(team)?),
                        leader_participant_id: leader,
                        leader_participation_id: Uuid::from_bytes(wire::id(participation)?),
                        team_revision,
                        leadership_revision,
                        team_membership_revision,
                    },
                ))
            },
        )
        .collect()
}

fn build_snapshot(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    sources: Vec<(Uuid, u64)>,
    allow_legacy: bool,
) -> Result<ElectionSubjectSnapshot, OperationsError> {
    let mut subjects = BTreeMap::<(VoterSubjectKind, Uuid), SubjectBuilder>::new();
    let mut voter_sources = Vec::with_capacity(sources.len());
    for (participant_id, membership_revision) in sources {
        let mapped = mapped_participation(transaction, workspace_id, participant_id)?;
        let (kind, subject_id, profile_id, participation_revision) = match mapped {
            Some((participation_id, profile_id, revision)) => (
                VoterSubjectKind::WorkspaceParticipation,
                participation_id,
                Some(profile_id),
                Some(revision),
            ),
            None if allow_legacy => (
                VoterSubjectKind::LegacyParticipant,
                participant_id,
                None,
                None,
            ),
            None => {
                return Err(OperationsError::Corrupt(
                    "election voter participation mapping",
                ));
            }
        };
        let key = (kind, subject_id);
        let builder = subjects.entry(key).or_insert(SubjectBuilder {
            kind,
            subject_id,
            profile_id,
            participation_revision,
            source_count: 0,
        });
        if builder.profile_id != profile_id
            || builder.participation_revision != participation_revision
        {
            return Err(OperationsError::Corrupt("election voter subject mapping"));
        }
        builder.source_count = builder
            .source_count
            .checked_add(1)
            .ok_or(OperationsError::Bounds("election voter subject sources"))?;
        voter_sources.push(VoterSource {
            participant_id,
            membership_revision,
            team_membership_revision: None,
            kind,
            subject_id,
        });
    }
    let voter_subjects = subjects
        .into_values()
        .map(|builder| VoterSubject {
            kind: builder.kind,
            subject_id: builder.subject_id,
            profile_id: builder.profile_id,
            participation_revision: builder.participation_revision,
            source_participant_count: builder.source_count,
        })
        .collect::<Vec<_>>();
    if voter_sources.len() > MAX_VOTER_SOURCES || voter_subjects.len() > MAX_VOTER_SOURCES {
        return Err(OperationsError::Bounds("leadership electorate"));
    }
    Ok(ElectionSubjectSnapshot {
        subjects: voter_subjects,
        sources: voter_sources,
        team_leaders: Vec::new(),
    })
}
pub(crate) fn persist_cutover(
    transaction: &Transaction<'_>,
    command: ElectionSubjectCutoverCommand,
    snapshot: &ElectionSubjectSnapshot,
) -> Result<ElectionSubjectCutoverReceipt, OperationsError> {
    let expected_result_revision = command.source_leadership_revision.checked_add(1);
    if command.election_id.is_nil()
        || command.created_at_ms < 0
        || command.source_leadership_revision == 0
        || expected_result_revision != Some(command.result_leadership_revision)
        || snapshot.sources.is_empty()
        || snapshot.subjects.is_empty()
    {
        return Err(OperationsError::Conflict("election subject cutover input"));
    }
    if snapshot
        .sources
        .iter()
        .any(|source| source.team_membership_revision.is_some() != command.team_id.is_some())
    {
        return Err(OperationsError::Conflict(
            "election Team membership snapshot",
        ));
    }
    let workspace_election = command.activity_id.is_none() && command.team_id.is_none();
    if workspace_election == snapshot.team_leaders.is_empty() {
        return Err(OperationsError::Conflict(
            "workspace election Team provenance",
        ));
    }
    if let Some(existing) = load_cutover(transaction, command.election_id)? {
        let expected_digest = cutover_digest(
            command.election_id,
            command.workspace_id,
            command.activity_id,
            command.team_id,
            command.source_leadership_revision,
            command.result_leadership_revision,
            snapshot,
        )?;
        if existing.workspace_id != command.workspace_id
            || existing.activity_id != command.activity_id
            || existing.team_id != command.team_id
            || existing.source_leadership_revision != command.source_leadership_revision
            || existing.result_leadership_revision != command.result_leadership_revision
            || existing.source_participant_count
                != u64::try_from(snapshot.source_participant_count())
                    .map_err(|_| OperationsError::Bounds("election voter sources"))?
            || existing.voter_subject_count
                != u64::try_from(snapshot.voter_subject_count())
                    .map_err(|_| OperationsError::Bounds("election voter subjects"))?
            || existing.electorate_digest != expected_digest
            || existing.created_at_ms != command.created_at_ms
        {
            return Err(OperationsError::Conflict("election subject cutover replay"));
        }
        return Ok(existing);
    }
    let digest = cutover_digest(
        command.election_id,
        command.workspace_id,
        command.activity_id,
        command.team_id,
        command.source_leadership_revision,
        command.result_leadership_revision,
        snapshot,
    )?;
    let scope_kind = match (command.activity_id, command.team_id) {
        (None, None) => 0,
        (Some(_), None) => 1,
        (None, Some(_)) => 2,
        (Some(_), Some(_)) => {
            return Err(OperationsError::Conflict("election subject scope"));
        }
    };
    insert_cutover_rows(transaction, &command, snapshot, scope_kind, digest)?;
    let stored = load_cutover(transaction, command.election_id)?
        .ok_or(OperationsError::Corrupt("election subject cutover insert"))?;
    if stored.electorate_digest != digest {
        return Err(OperationsError::Corrupt(
            "election subject cutover readback",
        ));
    }
    Ok(stored)
}

fn insert_cutover_rows(
    transaction: &Transaction<'_>,
    command: &ElectionSubjectCutoverCommand,
    snapshot: &ElectionSubjectSnapshot,
    scope_kind: i64,
    digest: [u8; 32],
) -> Result<(), OperationsError> {
    transaction.execute(
        "INSERT INTO ops_election_subject_cutovers(election_id,workspace_id,scope_kind,activity_id,team_id,source_leadership_revision,result_leadership_revision,subject_revision,source_participant_count,voter_subject_count,electorate_digest,created_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,1,?8,?9,?10,?11)",
        params![
            command.election_id.as_bytes(),
            command.workspace_id.into_uuid().as_bytes(),
            scope_kind,
            command.activity_id.map(|value| value.into_uuid().as_bytes().to_vec()),
            command.team_id.map(|value| value.as_bytes().to_vec()),
            crate::catalog::signed(command.source_leadership_revision)?,
            crate::catalog::signed(command.result_leadership_revision)?,
            i64::try_from(snapshot.source_participant_count())
                .map_err(|_| OperationsError::Bounds("election voter sources"))?,
            i64::try_from(snapshot.voter_subject_count())
                .map_err(|_| OperationsError::Bounds("election voter subjects"))?,
            digest.as_slice(),
            command.created_at_ms,
        ],
    )?;
    for subject in &snapshot.subjects {
        transaction.execute(
            "INSERT INTO ops_election_voter_subjects(election_id,subject_kind,subject_id,agent_profile_id,participation_revision,source_participant_count) VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                command.election_id.as_bytes(),
                subject.kind.encode(),
                subject.subject_id.as_bytes(),
                subject.profile_id.map(|value| value.as_bytes().to_vec()),
                subject
                    .participation_revision
                    .map(crate::catalog::signed)
                    .transpose()?,
                crate::catalog::signed(subject.source_participant_count)?,
            ],
        )?;
    }
    for source in &snapshot.sources {
        transaction.execute(
            "INSERT INTO ops_election_voter_sources(election_id,participant_id,membership_revision,team_membership_revision,subject_kind,subject_id) VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                command.election_id.as_bytes(),
                source.participant_id.as_bytes(),
                crate::catalog::signed(source.membership_revision)?,
                source.team_membership_revision.map(crate::catalog::signed).transpose()?,
                source.kind.encode(),
                source.subject_id.as_bytes(),
            ],
        )?;
    }
    for source in &snapshot.team_leaders {
        transaction.execute(
            "INSERT INTO ops_election_team_leader_sources(election_id,team_id,leader_participant_id,leader_participation_id,team_revision,leadership_revision,team_membership_revision) VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![
                command.election_id.as_bytes(),
                source.team_id.as_bytes(),
                source.leader_participant_id.as_bytes(),
                source.leader_participation_id.as_bytes(),
                crate::catalog::signed(source.team_revision)?,
                crate::catalog::signed(source.leadership_revision)?,
                crate::catalog::signed(source.team_membership_revision)?,
            ],
        )?;
    }
    Ok(())
}

pub(crate) fn has_cutover(
    connection: &Connection,
    election_id: Uuid,
) -> Result<bool, OperationsError> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_election_subject_cutovers WHERE election_id=?1)",
        params![election_id.as_bytes()],
        |row| row.get(0),
    )?)
}

pub(crate) fn electorate_size(
    connection: &Connection,
    election_id: Uuid,
) -> Result<i64, OperationsError> {
    if has_cutover(connection, election_id)? {
        Ok(connection.query_row(
            "SELECT COUNT(*) FROM ops_election_voter_subjects WHERE election_id=?1",
            params![election_id.as_bytes()],
            |row| row.get(0),
        )?)
    } else {
        Ok(connection.query_row(
            "SELECT COUNT(*) FROM ops_election_electorate WHERE election_id=?1",
            params![election_id.as_bytes()],
            |row| row.get(0),
        )?)
    }
}

pub(crate) fn cast_tallies(
    connection: &Connection,
    election_id: Uuid,
) -> Result<(i64, i64, i64), OperationsError> {
    let sql = if has_cutover(connection, election_id)? {
        "SELECT choice,COUNT(*) FROM ops_election_subject_ballots WHERE election_id=?1 GROUP BY choice"
    } else {
        "SELECT choice,COUNT(*) FROM ops_ballots WHERE election_id=?1 GROUP BY choice"
    };
    let mut statement = connection.prepare(sql)?;
    let rows = statement
        .query_map(params![election_id.as_bytes()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut yes = 0_i64;
    let mut no = 0_i64;
    let mut abstain = 0_i64;
    for (choice, count) in rows {
        match choice.as_str() {
            "YES" => yes = count,
            "NO" => no = count,
            "ABSTAIN" => abstain = count,
            _ => return Err(OperationsError::Corrupt("leadership ballot choice")),
        }
    }
    Ok((yes, no, abstain))
}

pub(crate) fn cast_subject_ballot(
    transaction: &Transaction<'_>,
    election_id: Uuid,
    voter_participant_id: Uuid,
    choice: &str,
    cast_at_ms: i64,
) -> Result<SubjectBallotOutcome, OperationsError> {
    if !matches!(choice, "YES" | "NO" | "ABSTAIN") || cast_at_ms < 0 {
        return Err(OperationsError::Conflict("leadership ballot input"));
    }
    let raw = transaction
        .query_row(
            "SELECT s.subject_kind,substr(s.subject_id,1,17) FROM ops_election_voter_sources s WHERE s.election_id=?1 AND s.participant_id=?2",
            params![election_id.as_bytes(), voter_participant_id.as_bytes()],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()?;
    let Some((kind_raw, subject_raw)) = raw else {
        return Err(OperationsError::Conflict("leadership voter electorate"));
    };
    let kind = VoterSubjectKind::decode(kind_raw)?;
    let subject_id = Uuid::from_bytes(wire::id(subject_raw)?);
    let existing = transaction
        .query_row(
            "SELECT choice FROM ops_election_subject_ballots WHERE election_id=?1 AND subject_kind=?2 AND subject_id=?3",
            params![election_id.as_bytes(), kind.encode(), subject_id.as_bytes()],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let participation_id = match kind {
        VoterSubjectKind::WorkspaceParticipation => {
            Some(WorkspaceParticipationId::from_uuid(subject_id)?)
        }
        VoterSubjectKind::LegacyParticipant => None,
    };
    if let Some(stored_choice) = existing {
        if stored_choice != choice {
            return Err(OperationsError::Conflict("leadership ballot choice"));
        }
        return Ok(SubjectBallotOutcome {
            stored_choice,
            replay: true,
            voter_participation_id: participation_id,
        });
    }
    // Replays above preserve the original receipt. A fresh vote requires the
    // same Team membership that was frozen, so leave/rejoin cannot revive it.
    let membership_valid: bool = transaction.query_row(
        "SELECT c.team_id IS NULL OR EXISTS(
            SELECT 1 FROM ops_team_members tm
            JOIN ops_teams t ON t.workspace_id=tm.workspace_id AND t.team_id=tm.team_id
            JOIN ops_workspace_participation_mappings m ON m.workspace_id=tm.workspace_id
                AND m.participation_id=tm.participation_id AND m.participant_id=s.participant_id
            JOIN ops_logical_members lm ON lm.workspace_id=m.workspace_id
                AND lm.participant_id=m.participant_id
            WHERE tm.workspace_id=c.workspace_id AND tm.team_id=c.team_id
                AND tm.participation_id=s.subject_id AND tm.state='JOINED' AND t.state='ACTIVE'
                AND tm.revision=s.team_membership_revision
                AND lm.membership_revision=s.membership_revision
                AND m.participant_membership_revision=s.membership_revision
                AND EXISTS(SELECT 1 FROM ops_current_effect_bindings current_binding
             WHERE current_binding.workspace_id=lm.workspace_id
               AND current_binding.participant_id=lm.participant_id
               AND current_binding.session_id=lm.session_id)
        ) FROM ops_election_subject_cutovers c JOIN ops_election_voter_sources s
            ON s.election_id=c.election_id WHERE c.election_id=?1 AND s.participant_id=?2",
        params![election_id.as_bytes(), voter_participant_id.as_bytes()],
        |row| row.get(0),
    )?;
    if !membership_valid {
        return Err(OperationsError::Conflict(
            "leadership voter team membership",
        ));
    }
    transaction.execute(
        "INSERT INTO ops_election_subject_ballots(election_id,subject_kind,subject_id,cast_by_participant_id,choice,ballot_revision,cast_at_ms) VALUES(?1,?2,?3,?4,?5,1,?6)",
        params![
            election_id.as_bytes(),
            kind.encode(),
            subject_id.as_bytes(),
            voter_participant_id.as_bytes(),
            choice,
            cast_at_ms,
        ],
    )?;
    Ok(SubjectBallotOutcome {
        stored_choice: choice.to_owned(),
        replay: false,
        voter_participation_id: participation_id,
    })
}

#[cfg(test)]
pub(crate) fn frozen_source_participants(
    connection: &Connection,
    election_id: Uuid,
) -> Result<Vec<Uuid>, OperationsError> {
    let (sql, is_cutover) = if has_cutover(connection, election_id)? {
        (
            "SELECT substr(participant_id,1,17) FROM ops_election_voter_sources WHERE election_id=?1 ORDER BY participant_id",
            true,
        )
    } else {
        (
            "SELECT substr(participant_id,1,17) FROM ops_election_electorate WHERE election_id=?1 ORDER BY participant_id",
            false,
        )
    };
    let mut statement = connection.prepare(sql)?;
    let rows = statement
        .query_map(params![election_id.as_bytes()], |row| {
            row.get::<_, Vec<u8>>(0)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    if rows.len() > MAX_VOTER_SOURCES {
        return Err(OperationsError::Bounds("leadership electorate"));
    }
    let participants = rows
        .into_iter()
        .map(|value| Ok(Uuid::from_bytes(wire::id(value)?)))
        .collect::<Result<Vec<_>, OperationsError>>()?;
    if is_cutover && participants.is_empty() {
        return Err(OperationsError::Corrupt("election voter sources"));
    }
    Ok(participants)
}

pub(crate) fn participant_in_open_electorate(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
) -> Result<bool, OperationsError> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_election_voter_sources s JOIN ops_leadership_elections l ON l.election_id=s.election_id WHERE l.workspace_id=?1 AND l.state='ELECTING' AND s.participant_id=?2 UNION ALL SELECT 1 FROM ops_election_electorate e JOIN ops_leadership_elections l ON l.election_id=e.election_id WHERE l.workspace_id=?1 AND l.state='ELECTING' AND e.participant_id=?2)",
        params![workspace_id.into_uuid().as_bytes(), participant_id.as_bytes()],
        |row| row.get(0),
    )?)
}

impl OperationsCatalog {
    /// Read one durable voter-subject cutover receipt for a Phase-E election.
    ///
    /// # Errors
    /// Returns storage or corrupt receipt failures.
    pub fn election_subject_cutover(
        &self,
        election_id: Uuid,
    ) -> Result<Option<ElectionSubjectCutoverReceipt>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let result = load_cutover(&transaction, election_id);
        match result {
            Ok(value) => {
                crate::catalog::commit_read(transaction, self.destination)?;
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }
}

fn ready_participant_sources(
    connection: &Connection,
    workspace_id: WorkspaceId,
    activity_id: Option<ActivityId>,
) -> Result<Vec<(Uuid, u64)>, OperationsError> {
    let limit = i64::try_from(MAX_VOTER_SOURCES + 1)
        .map_err(|_| OperationsError::Bounds("leadership electorate"))?;
    let (sql, activity) = match activity_id {
        Some(activity) => (
            "SELECT DISTINCT substr(b.participant_id,1,17),m.membership_revision FROM ops_current_effect_bindings b JOIN ops_logical_members m ON m.workspace_id=b.workspace_id AND m.participant_id=b.participant_id AND m.session_id=b.session_id WHERE b.workspace_id=?1 AND b.activity_id=?2 ORDER BY b.participant_id LIMIT ?3",
            Some(activity),
        ),
        None => (
            "SELECT DISTINCT substr(b.participant_id,1,17),m.membership_revision FROM ops_current_effect_bindings b JOIN ops_logical_members m ON m.workspace_id=b.workspace_id AND m.participant_id=b.participant_id AND m.session_id=b.session_id WHERE b.workspace_id=?1 ORDER BY b.participant_id LIMIT ?2",
            None,
        ),
    };
    let rows = if let Some(activity) = activity {
        let mut statement = connection.prepare(sql)?;
        statement
            .query_map(
                params![
                    workspace_id.into_uuid().as_bytes(),
                    activity.into_uuid().as_bytes(),
                    limit
                ],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?)),
            )?
            .collect::<Result<Vec<_>, _>>()?
    } else {
        let mut statement = connection.prepare(sql)?;
        statement
            .query_map(params![workspace_id.into_uuid().as_bytes(), limit], |row| {
                Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    if rows.len() > MAX_VOTER_SOURCES {
        return Err(OperationsError::Bounds("leadership electorate"));
    }
    rows.into_iter()
        .map(|(participant, membership_revision)| {
            let membership_revision = u64::try_from(membership_revision)
                .map_err(|_| OperationsError::Corrupt("election member revision"))?;
            if membership_revision == 0 {
                return Err(OperationsError::Corrupt("election member revision"));
            }
            Ok((
                Uuid::from_bytes(wire::id(participant)?),
                membership_revision,
            ))
        })
        .collect()
}

fn ready_team_participant_sources(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<Vec<(Uuid, u64)>, OperationsError> {
    let limit = i64::try_from(MAX_VOTER_SOURCES + 1)
        .map_err(|_| OperationsError::Bounds("leadership electorate"))?;
    let mut statement = connection.prepare(
        "SELECT DISTINCT substr(m.participant_id,1,17),lm.membership_revision
         FROM ops_team_members tm
         JOIN ops_teams t
           ON t.workspace_id=tm.workspace_id AND t.team_id=tm.team_id
         JOIN ops_workspace_participation_mappings m
           ON m.workspace_id=tm.workspace_id AND m.participation_id=tm.participation_id
         JOIN ops_logical_members lm
           ON lm.workspace_id=m.workspace_id AND lm.participant_id=m.participant_id
         WHERE tm.workspace_id=?1 AND tm.team_id=?2 AND tm.state='JOINED' AND t.state='ACTIVE'
           AND EXISTS(SELECT 1 FROM ops_current_effect_bindings current_binding
             WHERE current_binding.workspace_id=lm.workspace_id
               AND current_binding.participant_id=lm.participant_id
               AND current_binding.session_id=lm.session_id)
         ORDER BY m.participant_id LIMIT ?3",
    )?;
    let rows = statement
        .query_map(
            params![
                workspace_id.into_uuid().as_bytes(),
                team_id.as_bytes(),
                limit
            ],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?)),
        )?
        .collect::<Result<Vec<_>, _>>()?;
    if rows.len() > MAX_VOTER_SOURCES {
        return Err(OperationsError::Bounds("leadership electorate"));
    }
    rows.into_iter()
        .map(|(participant, membership_revision)| {
            let membership_revision = u64::try_from(membership_revision)
                .map_err(|_| OperationsError::Corrupt("election member revision"))?;
            if membership_revision == 0 {
                return Err(OperationsError::Corrupt("election member revision"));
            }
            Ok((
                Uuid::from_bytes(wire::id(participant)?),
                membership_revision,
            ))
        })
        .collect()
}

fn mapped_participation(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
) -> Result<Option<(Uuid, Uuid, u64)>, OperationsError> {
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
    raw.map(|(participation, profile, revision)| {
        let revision = u64::try_from(revision)
            .map_err(|_| OperationsError::Corrupt("workspace participation revision"))?;
        if revision == 0 {
            return Err(OperationsError::Corrupt("workspace participation revision"));
        }
        Ok((
            Uuid::from_bytes(wire::id(participation)?),
            Uuid::from_bytes(wire::id(profile)?),
            revision,
        ))
    })
    .transpose()
}

fn cutover_digest(
    election_id: Uuid,
    workspace_id: WorkspaceId,
    activity_id: Option<ActivityId>,
    team_id: Option<Uuid>,
    source_leadership_revision: u64,
    result_leadership_revision: u64,
    snapshot: &ElectionSubjectSnapshot,
) -> Result<[u8; 32], OperationsError> {
    let mut digest = Sha256::new();
    digest.update(CUTOVER_DIGEST_DOMAIN);
    digest.update(election_id.as_bytes());
    digest.update(workspace_id.into_uuid().as_bytes());
    match (activity_id, team_id) {
        (Some(activity), None) => {
            digest.update([1]);
            digest.update(activity.into_uuid().as_bytes());
        }
        (None, Some(team)) => {
            digest.update([2]);
            digest.update(team.as_bytes());
        }
        (None, None) => digest.update([0]),
        (Some(_), Some(_)) => return Err(OperationsError::Conflict("election subject scope")),
    }
    digest.update(source_leadership_revision.to_be_bytes());
    digest.update(result_leadership_revision.to_be_bytes());
    digest.update(
        u64::try_from(snapshot.subjects.len())
            .map_err(|_| OperationsError::Bounds("election voter subjects"))?
            .to_be_bytes(),
    );
    for subject in &snapshot.subjects {
        digest.update(subject.kind.encode().to_be_bytes());
        digest.update(subject.subject_id.as_bytes());
        match subject.profile_id {
            Some(profile_id) => {
                digest.update([1]);
                digest.update(profile_id.as_bytes());
            }
            None => digest.update([0]),
        }
        digest.update(subject.participation_revision.unwrap_or(0).to_be_bytes());
        digest.update(subject.source_participant_count.to_be_bytes());
    }
    digest.update(
        u64::try_from(snapshot.sources.len())
            .map_err(|_| OperationsError::Bounds("election voter sources"))?
            .to_be_bytes(),
    );
    for source in &snapshot.sources {
        digest.update(source.participant_id.as_bytes());
        digest.update(source.membership_revision.to_be_bytes());
        digest.update(source.team_membership_revision.unwrap_or(0).to_be_bytes());
        digest.update(source.kind.encode().to_be_bytes());
        digest.update(source.subject_id.as_bytes());
    }
    digest.update(
        u64::try_from(snapshot.team_leaders.len())
            .map_err(|_| OperationsError::Bounds("election Team leader sources"))?
            .to_be_bytes(),
    );
    for source in &snapshot.team_leaders {
        digest.update(source.team_id.as_bytes());
        digest.update(source.leader_participant_id.as_bytes());
        digest.update(source.leader_participation_id.as_bytes());
        digest.update(source.team_revision.to_be_bytes());
        digest.update(source.leadership_revision.to_be_bytes());
        digest.update(source.team_membership_revision.to_be_bytes());
    }
    Ok(digest.finalize().into())
}

fn load_cutover(
    connection: &Connection,
    election_id: Uuid,
) -> Result<Option<ElectionSubjectCutoverReceipt>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(workspace_id,1,17),scope_kind,substr(activity_id,1,17),substr(team_id,1,17),source_leadership_revision,result_leadership_revision,subject_revision,source_participant_count,voter_subject_count,substr(electorate_digest,1,33),created_at_ms FROM ops_election_subject_cutovers WHERE election_id=?1",
            params![election_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<Vec<u8>>>(2)?,
                    row.get::<_, Option<Vec<u8>>>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, Vec<u8>>(9)?,
                    row.get::<_, i64>(10)?,
                ))
            },
        )
        .optional()?;
    raw.map(
        |(
            workspace,
            scope_kind,
            activity,
            team,
            source_revision,
            result_revision,
            subject_revision,
            source_count,
            subject_count,
            digest,
            created_at_ms,
        )| {
            let activity_id = match (scope_kind, activity.as_ref()) {
                (0 | 2, None) => None,
                (1, Some(value)) => Some(ActivityId::from_uuid(Uuid::from_bytes(wire::id(
                    value.clone(),
                )?))),
                _ => return Err(OperationsError::Corrupt("election subject scope")),
            };
            let team_id = match (scope_kind, team) {
                (0 | 1, None) => None,
                (2, Some(value)) => Some(Uuid::from_bytes(wire::id(value)?)),
                _ => return Err(OperationsError::Corrupt("election subject scope")),
            };
            let source_leadership_revision = u64::try_from(source_revision)
                .map_err(|_| OperationsError::Corrupt("election subject source revision"))?;
            let result_leadership_revision = u64::try_from(result_revision)
                .map_err(|_| OperationsError::Corrupt("election subject result revision"))?;
            let subject_revision = u64::try_from(subject_revision)
                .map_err(|_| OperationsError::Corrupt("election subject revision"))?;
            let source_participant_count = u64::try_from(source_count)
                .map_err(|_| OperationsError::Corrupt("election voter source count"))?;
            let voter_subject_count = u64::try_from(subject_count)
                .map_err(|_| OperationsError::Corrupt("election voter subject count"))?;
            let electorate_digest: [u8; 32] = digest
                .try_into()
                .map_err(|_| OperationsError::Corrupt("election subject digest"))?;
            if source_leadership_revision == 0
                || source_leadership_revision.checked_add(1) != Some(result_leadership_revision)
                || subject_revision != 1
                || source_participant_count == 0
                || voter_subject_count == 0
                || voter_subject_count > source_participant_count
                || electorate_digest == [0; 32]
                || created_at_ms < 0
            {
                return Err(OperationsError::Corrupt("election subject cutover shape"));
            }
            Ok(ElectionSubjectCutoverReceipt {
                election_id,
                workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(workspace)?)),
                activity_id,
                team_id,
                source_leadership_revision,
                result_leadership_revision,
                subject_revision,
                source_participant_count,
                voter_subject_count,
                electorate_digest,
                created_at_ms,
            })
        },
    )
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
        catalog: OperationsCatalog,
        workspace: WorkspaceId,
        activity: ActivityId,
        election_id: Uuid,
        participant_a: Uuid,
        participant_b: Uuid,
        participation_id: WorkspaceParticipationId,
    }

    fn table_sql(connection: &Connection, name: &str) -> Result<String, Box<dyn Error>> {
        Ok(connection.query_row(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?1",
            params![name],
            |row| row.get(0),
        )?)
    }

    fn subject_fixture() -> Result<SubjectFixture, Box<dyn Error>> {
        let root =
            std::env::temp_dir().join(format!("heterarchy-election-subject-{}", Uuid::now_v7()));
        fs::create_dir(&root)?;
        let destination = Uuid::from_u128(0xE500);
        let workspace = WorkspaceId::from_uuid(Uuid::from_u128(0xE501));
        let activity = ActivityId::from_uuid(Uuid::from_u128(0xE502));
        let catalog = OperationsCatalog::open(&root.join("operations.sqlite3"), destination)?;
        let participant_a = Uuid::from_u128(0xE510);
        let participant_b = Uuid::from_u128(0xE511);
        let session_a = AgentSessionId::from_uuid(Uuid::from_u128(0xE520));
        let session_b = AgentSessionId::from_uuid(Uuid::from_u128(0xE521));
        let participation_id = WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xE530))?;
        let profile_id = Uuid::from_u128(0xE531);
        catalog.connection.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,1,1)",
            params![
                workspace.into_uuid().as_bytes(),
                profile_id.as_bytes(),
                participation_id.into_uuid().as_bytes(),
            ],
        )?;
        for (ordinal, (participant, session)) in
            [(participant_a, session_a), (participant_b, session_b)]
                .into_iter()
                .enumerate()
        {
            catalog.connection.execute(
                "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,?4,'worker',1,1,1)",
                params![
                    workspace.into_uuid().as_bytes(),
                    participant.as_bytes(),
                    session.into_uuid().as_bytes(),
                    format!("subject member {ordinal}"),
                ],
            )?;
            catalog.connection.execute(
                "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,1)",
                params![
                    Uuid::from_u128(0xE540 + u128::try_from(ordinal)?).as_bytes(),
                    workspace.into_uuid().as_bytes(),
                    participation_id.into_uuid().as_bytes(),
                    profile_id.as_bytes(),
                    Uuid::from_u128(0xE550 + u128::try_from(ordinal)?).as_bytes(),
                    session.into_uuid().as_bytes(),
                    participant.as_bytes(),
                ],
            )?;
            catalog.connection.execute(
                "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
                params![
                    workspace.into_uuid().as_bytes(),
                    participant.as_bytes(),
                    activity.into_uuid().as_bytes(),
                    session.into_uuid().as_bytes(),
                    AgentInstanceId::from_uuid(Uuid::from_u128(0xE560 + u128::try_from(ordinal)?))
                        .into_uuid()
                        .as_bytes(),
                    Uuid::from_u128(0xE570 + u128::try_from(ordinal)?).as_bytes(),
                ],
            )?;
        }
        let election_id = Uuid::from_u128(0xE580);
        catalog.connection.execute(
            "INSERT INTO ops_leadership_elections(election_id,workspace_id,scope_kind,activity_id,kind,state,incumbent_participant_id,candidate_participant_id,ballot_deadline_ms,elected_participant_id,tally_yes,tally_no,tally_abstain,created_at_ms,closed_at_ms) VALUES(?1,?2,1,?4,'RECALL','ELECTING',NULL,?3,10000,NULL,NULL,NULL,NULL,100,NULL)",
            params![
                election_id.as_bytes(),
                workspace.into_uuid().as_bytes(),
                participant_a.as_bytes(),
                activity.into_uuid().as_bytes(),
            ],
        )?;
        Ok(SubjectFixture {
            root,
            catalog,
            workspace,
            activity,
            election_id,
            participant_a,
            participant_b,
            participation_id,
        })
    }

    fn seed_team_voters(fixture: &SubjectFixture) -> Result<(Uuid, Uuid), Box<dyn Error>> {
        let c = &fixture.catalog.connection;
        let workspace = fixture.workspace.into_uuid();
        let newcomer = Uuid::from_u128(0xE590);
        let new_participation = Uuid::from_u128(0xE591);
        let profile = Uuid::from_u128(0xE592);
        let session = Uuid::from_u128(0xE593);
        c.execute(
            "INSERT INTO ops_workspace_participations VALUES(?1,?2,?3,1,1)",
            params![
                workspace.as_bytes(),
                profile.as_bytes(),
                new_participation.as_bytes()
            ],
        )?;
        c.execute("INSERT INTO ops_logical_members SELECT workspace_id,?1,?2,principal_kind,principal_id,'new member',declared_role,transport_kind,membership_revision,created_at_ms FROM ops_logical_members WHERE workspace_id=?3 AND participant_id=?4",
            params![newcomer.as_bytes(), session.as_bytes(), workspace.as_bytes(), fixture.participant_a.as_bytes()])?;
        c.execute("INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,1)",
            params![Uuid::from_u128(0xE594).as_bytes(), workspace.as_bytes(), new_participation.as_bytes(), profile.as_bytes(), Uuid::from_u128(0xE595).as_bytes(), session.as_bytes(), newcomer.as_bytes()])?;
        c.execute("INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
            params![workspace.as_bytes(), newcomer.as_bytes(), fixture.activity.into_uuid().as_bytes(), session.as_bytes(), Uuid::from_u128(0xE596).as_bytes(), Uuid::from_u128(0xE597).as_bytes()])?;
        for (team, leader) in [
            (0xE5A0, fixture.participant_a),
            (0xE5A1, fixture.participant_b),
        ] {
            let team = Uuid::from_u128(team);
            c.execute("INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_at_ms,updated_at_ms,formed_at_ms) VALUES(?1,?2,?3,'election authority','ACTIVE',1,'operator','Local operator',1,1,1)",
                params![team.as_bytes(), workspace.as_bytes(), format!("Team {team}")])?;
            c.execute("INSERT INTO ops_team_members(workspace_id,team_id,participation_id,state,revision,joined_at_ms,left_at_ms,role) VALUES(?1,?2,?3,'JOINED',7,1,NULL,'verification'),(?1,?2,?4,'JOINED',1,200,NULL,'verification')",
                params![workspace.as_bytes(), team.as_bytes(), fixture.participation_id.into_uuid().as_bytes(), new_participation.as_bytes()])?;
            c.execute("INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,2,NULL,?2,'ACTIVE',?3,3,NULL,NULL,3)",
                params![workspace.as_bytes(), team.as_bytes(), leader.as_bytes()])?;
            c.execute("INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,2,zeroblob(16),?2,1,'CONFIRMED',?3,2,2)",
                params![workspace.as_bytes(), team.as_bytes(), leader.as_bytes()])?;
        }
        Ok((Uuid::from_u128(0xE5A0), newcomer))
    }

    #[test]
    fn team_electorate_freezes_probation_and_membership_incarnation() -> Result<(), Box<dyn Error>>
    {
        let fixture = subject_fixture()?;
        let (team, newcomer) = seed_team_voters(&fixture)?;
        let now = NEW_MEMBER_PROBATION_MS + 1;
        fixture.catalog.connection.execute("UPDATE ops_leadership_elections SET scope_kind=2,activity_id=NULL,team_id=?1,ballot_deadline_ms=?2 WHERE election_id=?3",
            params![team.as_bytes(), now+1000, fixture.election_id.as_bytes()])?;
        let tx = fixture.catalog.connection.unchecked_transaction()?;
        assert!(matches!(
            snapshot_voting_subjects_for_scope(
                &tx,
                LeadershipScope::team(fixture.workspace, team),
                now - 1
            ),
            Err(OperationsError::Conflict("leadership electorate empty"))
        ));
        let snapshot = snapshot_voting_subjects_for_scope(
            &tx,
            LeadershipScope::team(fixture.workspace, team),
            now,
        )?;
        assert_eq!(snapshot.source_participant_count(), 2);
        assert_eq!(snapshot.voter_subject_count(), 1);
        assert!(!snapshot.contains_participant(newcomer));
        let _cutover = persist_cutover(
            &tx,
            ElectionSubjectCutoverCommand {
                election_id: fixture.election_id,
                workspace_id: fixture.workspace,
                activity_id: None,
                team_id: Some(team),
                source_leadership_revision: 3,
                result_leadership_revision: 4,
                created_at_ms: now,
            },
            &snapshot,
        )?;
        let revisions: (i64, Option<i64>) = tx.query_row("SELECT membership_revision,team_membership_revision FROM ops_election_voter_sources WHERE election_id=?1 AND participant_id=?2",
            params![fixture.election_id.as_bytes(),fixture.participant_a.as_bytes()], |row| Ok((row.get(0)?,row.get(1)?)))?;
        assert_eq!(revisions, (1, Some(7)));
        assert!(matches!(
            cast_subject_ballot(&tx, fixture.election_id, newcomer, "YES", now + 200),
            Err(OperationsError::Conflict("leadership voter electorate"))
        ));
        tx.execute_batch("SAVEPOINT before_departure")?;
        tx.execute("UPDATE ops_team_members SET state='LEFT',revision=8,left_at_ms=?1 WHERE team_id=?2 AND participation_id=?3",
            params![now,team.as_bytes(),fixture.participation_id.into_uuid().as_bytes()])?;
        assert!(matches!(
            cast_subject_ballot(
                &tx,
                fixture.election_id,
                fixture.participant_a,
                "YES",
                now + 1
            ),
            Err(OperationsError::Conflict(
                "leadership voter team membership"
            ))
        ));
        tx.execute("UPDATE ops_team_members SET state='JOINED',revision=9,joined_at_ms=?1,left_at_ms=NULL WHERE team_id=?2 AND participation_id=?3",
            params![now+1,team.as_bytes(),fixture.participation_id.into_uuid().as_bytes()])?;
        assert!(matches!(
            cast_subject_ballot(
                &tx,
                fixture.election_id,
                fixture.participant_b,
                "YES",
                now + 2
            ),
            Err(OperationsError::Conflict(
                "leadership voter team membership"
            ))
        ));
        assert_eq!(electorate_size(&tx, fixture.election_id)?, 1);
        assert_eq!(cast_tallies(&tx, fixture.election_id)?, (0, 0, 0));
        tx.execute_batch("ROLLBACK TO before_departure; RELEASE before_departure")?;
        assert!(
            !cast_subject_ballot(
                &tx,
                fixture.election_id,
                fixture.participant_a,
                "YES",
                now + 1
            )?
            .replay()
        );
        assert!(
            cast_subject_ballot(
                &tx,
                fixture.election_id,
                fixture.participant_b,
                "YES",
                now + 2
            )?
            .replay()
        );
        tx.commit()?;
        drop(fixture.catalog);
        fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "One transaction fixture compares normal review, failover, missing cutover and shared fulfillment without inventing separate authority."
    )]
    #[test]
    fn workspace_votes_require_confirmed_team_leaders_and_shared_subject_fulfillment()
    -> Result<(), Box<dyn Error>> {
        let fixture = subject_fixture()?;
        let (team_a, non_leader) = seed_team_voters(&fixture)?;
        let team_b = Uuid::from_u128(0xE5A1);
        let now = NEW_MEMBER_PROBATION_MS + 1;
        let tx = fixture.catalog.connection.unchecked_transaction()?;
        let scope = LeadershipScope::workspace(fixture.workspace);
        let snapshot = snapshot_voting_subjects_for_scope(&tx, scope, now)?;
        assert_eq!(snapshot.source_participant_count(), 2);
        assert_eq!(snapshot.voter_subject_count(), 1);
        assert_eq!(snapshot.team_leaders.len(), 2);
        assert!(!snapshot.contains_participant(non_leader));

        tx.execute_batch("SAVEPOINT formation_admission")?;
        tx.execute("UPDATE ops_leadership_elections SET scope_kind=0,activity_id=NULL,team_id=NULL WHERE election_id=?1",params![fixture.election_id.as_bytes()])?;
        let _frozen = persist_cutover(
            &tx,
            ElectionSubjectCutoverCommand {
                election_id: fixture.election_id,
                workspace_id: fixture.workspace,
                activity_id: None,
                team_id: None,
                source_leadership_revision: 7,
                result_leadership_revision: 8,
                created_at_ms: now,
            },
            &snapshot,
        )?;
        tx.execute(
            "UPDATE ops_teams SET formed_at_ms=NULL WHERE workspace_id=?1",
            params![fixture.workspace.into_uuid().as_bytes()],
        )?;
        assert!(ready_workspace_team_leaders(&tx, fixture.workspace)?.is_empty());
        assert!(!workspace_electorate_lost_team_leadership(
            &tx,
            fixture.workspace,
            fixture.election_id
        )?);
        tx.execute(
            "UPDATE ops_teams SET formed_at_ms=1 WHERE workspace_id=?1",
            params![fixture.workspace.into_uuid().as_bytes()],
        )?;
        tx.execute(
            "DELETE FROM ops_current_effect_bindings WHERE workspace_id=?1 AND participant_id=?2",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                non_leader.as_bytes()
            ],
        )?;
        assert!(ready_workspace_team_leaders(&tx, fixture.workspace)?.is_empty());
        assert!(!workspace_electorate_lost_team_leadership(
            &tx,
            fixture.workspace,
            fixture.election_id
        )?);
        tx.execute_batch("ROLLBACK TO formation_admission; RELEASE formation_admission")?;

        // An Activity leader is Ready only for the Activity recorded on its
        // current effect binding, even when the same Session binds elsewhere.
        let unrelated_activity = ActivityId::from_uuid(Uuid::from_u128(0xE5B8));
        tx.execute(
            "INSERT INTO ops_scopes(workspace_id,activity_id) VALUES(?1,?2) ON CONFLICT DO NOTHING",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                unrelated_activity.into_uuid().as_bytes(),
            ],
        )?;
        tx.execute(
            "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,1,?2,NULL,'ACTIVE',?3,3,NULL,NULL,3)",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                unrelated_activity.into_uuid().as_bytes(),
                fixture.participant_a.as_bytes(),
            ],
        )?;
        tx.execute(
            "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,work_assignment_id,election_id,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,1,?2,zeroblob(16),1,'CONFIRMED',NULL,NULL,?3,2,2)",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                unrelated_activity.into_uuid().as_bytes(),
                fixture.participant_a.as_bytes(),
            ],
        )?;
        assert!(
            crate::leadership::current_leadership_authority_in_transaction(
                &tx,
                LeadershipScope::activity(fixture.workspace, unrelated_activity),
            )?
            .is_none()
        );
        tx.execute_batch("SAVEPOINT confirmed_leaders; DELETE FROM ops_leadership_receipts")?;
        assert!(matches!(
            snapshot_voting_subjects_for_scope(&tx, scope, now),
            Err(OperationsError::Conflict("leadership electorate empty"))
        ));
        tx.execute_batch("ROLLBACK TO confirmed_leaders; RELEASE confirmed_leaders")?;

        // An incumbent from an ordinary ACTIVE scope may steer during its
        // exact current ELECTING cutover. The same source is excluded when a
        // FAILOVER receipt exists at the cutover's immutable source revision.
        tx.execute_batch("SAVEPOINT leader_authority_origin")?;
        let team_scope = LeadershipScope::team(fixture.workspace, team_a);
        let team_snapshot = snapshot_voting_subjects_for_scope(&tx, team_scope, now)?;
        tx.execute(
            "UPDATE ops_leadership_elections SET scope_kind=2,activity_id=NULL,team_id=?1 WHERE election_id=?2",
            params![team_a.as_bytes(), fixture.election_id.as_bytes()],
        )?;
        let _cutover = persist_cutover(
            &tx,
            ElectionSubjectCutoverCommand {
                election_id: fixture.election_id,
                workspace_id: fixture.workspace,
                activity_id: None,
                team_id: Some(team_a),
                source_leadership_revision: 3,
                result_leadership_revision: 4,
                created_at_ms: now,
            },
            &team_snapshot,
        )?;
        tx.execute(
            "UPDATE ops_leadership SET state='ELECTING',leadership_revision=4,current_election_id=?1 WHERE workspace_id=?2 AND scope_kind=2 AND activity_id IS NULL AND team_id=?3",
            params![
                fixture.election_id.as_bytes(),
                fixture.workspace.into_uuid().as_bytes(),
                team_a.as_bytes(),
            ],
        )?;
        let normal_review = ready_workspace_team_leaders(&tx, fixture.workspace)?;
        assert_eq!(normal_review.len(), 2);
        tx.execute(
            "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,work_assignment_id,election_id,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,2,zeroblob(16),?2,2,'FAILOVER',NULL,NULL,?3,3,3)",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                team_a.as_bytes(),
                fixture.participant_a.as_bytes(),
            ],
        )?;
        let failover_origin = ready_workspace_team_leaders(&tx, fixture.workspace)?;
        assert_eq!(failover_origin.len(), 1);
        assert_eq!(failover_origin[0].3.team_id, team_b);
        tx.execute_batch("ROLLBACK TO leader_authority_origin; RELEASE leader_authority_origin")?;

        // A current election without its exact scope cutover fails closed; the
        // same-row revision is never used to infer a missing source cutover.
        tx.execute_batch("SAVEPOINT leader_missing_cutover")?;
        tx.execute(
            "UPDATE ops_leadership_elections SET scope_kind=2,activity_id=NULL,team_id=?1 WHERE election_id=?2",
            params![team_a.as_bytes(), fixture.election_id.as_bytes()],
        )?;
        tx.execute(
            "UPDATE ops_leadership SET state='ELECTING',leadership_revision=4,current_election_id=?1 WHERE workspace_id=?2 AND scope_kind=2 AND activity_id IS NULL AND team_id=?3",
            params![
                fixture.election_id.as_bytes(),
                fixture.workspace.into_uuid().as_bytes(),
                team_a.as_bytes(),
            ],
        )?;
        let missing_cutover = ready_workspace_team_leaders(&tx, fixture.workspace)?;
        assert_eq!(missing_cutover.len(), 1);
        assert_eq!(missing_cutover[0].3.team_id, team_b);
        tx.execute_batch("ROLLBACK TO leader_missing_cutover; RELEASE leader_missing_cutover")?;
        let migration = Uuid::from_u128(0xE5B0);
        tx.execute("INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
            params![migration.as_bytes(),fixture.workspace.into_uuid().as_bytes(),fixture.catalog.destination_id().as_bytes(),[0_u8;32].as_slice()])?;
        tx.execute("INSERT INTO ops_effect_fact_consumers(migration_id,workspace_id,destination_id,last_sequence,applied_count) VALUES(?1,?2,?3,0,0)",
            params![migration.as_bytes(),fixture.workspace.into_uuid().as_bytes(),fixture.catalog.destination_id().as_bytes()])?;
        tx.execute("INSERT INTO ops_assignment_review_projection(migration_id,source_sequence,fact_digest,assignment_id,workspace_id,activity_id,goal_id,node_id,target_agent_id,outcome,reviewer_agent_id,observed_at_ms) VALUES(?1,1,?2,?3,?4,?5,?6,?7,?8,'Rejected',?9,10)",
            params![migration.as_bytes(),[1_u8;32].as_slice(),Uuid::from_u128(0xE5B1).as_bytes(),fixture.workspace.into_uuid().as_bytes(),fixture.activity.into_uuid().as_bytes(),Uuid::from_u128(0xE5B2).as_bytes(),Uuid::from_u128(0xE5B3).as_bytes(),Uuid::from_u128(0xE561).as_bytes(),Uuid::from_u128(0xE5B4).as_bytes()])?;
        // The measured failure belongs to source B; source A is the same
        // logical Agent and must not bypass it as an unmeasured alias.
        assert!(matches!(
            snapshot_voting_subjects_for_scope(&tx, scope, now),
            Err(OperationsError::Conflict("leadership electorate empty"))
        ));
        tx.commit()?;
        drop(fixture.catalog);
        fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn two_legacy_participants_mapped_to_one_participation_freeze_one_voter_seat()
    -> Result<(), Box<dyn Error>> {
        let fixture = subject_fixture()?;
        let transaction = fixture.catalog.connection.unchecked_transaction()?;
        let snapshot =
            snapshot_ready_subjects(&transaction, fixture.workspace, Some(fixture.activity))?;
        assert_eq!(snapshot.source_participant_count(), 2);
        assert_eq!(snapshot.voter_subject_count(), 1);
        assert!(snapshot.contains_participant(fixture.participant_a));
        assert!(snapshot.contains_participant(fixture.participant_b));
        let receipt = persist_cutover(
            &transaction,
            ElectionSubjectCutoverCommand {
                election_id: fixture.election_id,
                workspace_id: fixture.workspace,
                activity_id: Some(fixture.activity),
                team_id: None,
                source_leadership_revision: 7,
                result_leadership_revision: 8,
                created_at_ms: 100,
            },
            &snapshot,
        )?;
        assert_eq!(receipt.source_participant_count(), 2);
        assert_eq!(receipt.voter_subject_count(), 1);
        assert_ne!(receipt.electorate_digest(), [0; 32]);
        assert_eq!(electorate_size(&transaction, fixture.election_id)?, 1);
        transaction.commit()?;
        drop(fixture.catalog);
        fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn two_sources_of_one_participation_share_one_ballot_and_conflicting_second_vote_fails()
    -> Result<(), Box<dyn Error>> {
        let fixture = subject_fixture()?;
        let transaction = fixture.catalog.connection.unchecked_transaction()?;
        let snapshot =
            snapshot_ready_subjects(&transaction, fixture.workspace, Some(fixture.activity))?;
        let _receipt = persist_cutover(
            &transaction,
            ElectionSubjectCutoverCommand {
                election_id: fixture.election_id,
                workspace_id: fixture.workspace,
                activity_id: Some(fixture.activity),
                team_id: None,
                source_leadership_revision: 7,
                result_leadership_revision: 8,
                created_at_ms: 100,
            },
            &snapshot,
        )?;
        let first = cast_subject_ballot(
            &transaction,
            fixture.election_id,
            fixture.participant_a,
            "YES",
            101,
        )?;
        assert!(!first.replay());
        assert_eq!(
            first.voter_participation_id(),
            Some(fixture.participation_id)
        );
        let replay = cast_subject_ballot(
            &transaction,
            fixture.election_id,
            fixture.participant_b,
            "YES",
            102,
        )?;
        assert!(replay.replay());
        assert_eq!(
            replay.voter_participation_id(),
            Some(fixture.participation_id)
        );
        assert!(matches!(
            cast_subject_ballot(
                &transaction,
                fixture.election_id,
                fixture.participant_b,
                "NO",
                103,
            ),
            Err(OperationsError::Conflict("leadership ballot choice"))
        ));
        let ballot_count: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM ops_election_subject_ballots WHERE election_id=?1",
            params![fixture.election_id.as_bytes()],
            |row| row.get(0),
        )?;
        assert_eq!(ballot_count, 1);
        assert_eq!(cast_tallies(&transaction, fixture.election_id)?, (1, 0, 0));
        transaction.commit()?;
        drop(fixture.catalog);
        fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn v23_to_v24_adds_only_election_subject_authority_tables() -> Result<(), Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!(
            "heterarchy-election-subject-migration-{}",
            Uuid::now_v7()
        ));
        fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let destination = Uuid::from_u128(0xE590);
        let catalog = OperationsCatalog::open(&path, destination)?;
        drop(catalog);
        let connection = Connection::open(&path)?;
        let leadership_before = table_sql(&connection, "ops_leadership")?;
        let electorate_before = table_sql(&connection, "ops_election_electorate")?;
        let ballots_before = table_sql(&connection, "ops_ballots")?;
        let credit_before = table_sql(&connection, "ops_work_credit_entries")?;
        drop_schema_for_legacy_fixture(&connection)?;
        connection.pragma_update(None, "user_version", 23_i64)?;
        drop(connection);

        let migrated = OperationsCatalog::open(&path, destination)?;
        let version: i64 = migrated
            .connection
            .pragma_query_value(None, "user_version", |row| row.get(0))?;
        assert_eq!(version, crate::catalog::VERSION);
        crate::catalog::assert_fixture_table_schema(
            &migrated.connection,
            "ops_leadership",
            &leadership_before,
        )?;
        assert_eq!(
            table_sql(&migrated.connection, "ops_election_electorate")?,
            electorate_before
        );
        assert_eq!(
            table_sql(&migrated.connection, "ops_ballots")?,
            ballots_before
        );
        assert_eq!(
            table_sql(&migrated.connection, "ops_work_credit_entries")?,
            credit_before
        );
        for table in [
            "ops_election_subject_cutovers",
            "ops_election_voter_subjects",
            "ops_election_voter_sources",
            "ops_election_subject_ballots",
        ] {
            let count: i64 = migrated.connection.query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name=?1",
                params![table],
                |row| row.get(0),
            )?;
            assert_eq!(count, 1, "missing v24 table {table}");
        }
        drop(migrated);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn workspace_scope_deduplicates_one_participant_across_multiple_activity_bindings()
    -> Result<(), Box<dyn Error>> {
        let fixture = subject_fixture()?;
        let second_activity = ActivityId::from_uuid(Uuid::from_u128(0xE503));
        fixture.catalog.connection.execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) SELECT workspace_id,participant_id,?1,session_id,?2,0,?3 FROM ops_logical_members WHERE workspace_id=?4 AND participant_id=?5",
            params![
                second_activity.into_uuid().as_bytes(),
                AgentInstanceId::from_uuid(Uuid::from_u128(0xE56F))
                    .into_uuid()
                    .as_bytes(),
                Uuid::from_u128(0xE57F).as_bytes(),
                fixture.workspace.into_uuid().as_bytes(),
                fixture.participant_a.as_bytes(),
            ],
        )?;
        let transaction = fixture.catalog.connection.unchecked_transaction()?;
        let workspace_snapshot = snapshot_ready_subjects(&transaction, fixture.workspace, None)?;
        assert_eq!(workspace_snapshot.source_participant_count(), 2);
        assert_eq!(workspace_snapshot.voter_subject_count(), 1);
        let second_activity_snapshot =
            snapshot_ready_subjects(&transaction, fixture.workspace, Some(second_activity))?;
        assert_eq!(second_activity_snapshot.source_participant_count(), 1);
        assert_eq!(second_activity_snapshot.voter_subject_count(), 1);
        transaction.commit()?;
        drop(fixture.catalog);
        fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn pre_v24_election_without_cutover_keeps_legacy_participant_ballot_authority()
    -> Result<(), Box<dyn Error>> {
        let mut fixture = subject_fixture()?;
        fixture.catalog.connection.execute(
            "INSERT INTO ops_election_electorate(election_id,participant_id) VALUES(?1,?2), (?1,?3)",
            params![
                fixture.election_id.as_bytes(),
                fixture.participant_a.as_bytes(),
                fixture.participant_b.as_bytes(),
            ],
        )?;
        assert_eq!(
            electorate_size(&fixture.catalog.connection, fixture.election_id)?,
            2
        );
        fixture.catalog.connection.execute(
            "INSERT INTO ops_ballots(election_id,voter_participant_id,choice,ballot_revision,cast_at_ms) VALUES(?1,?2,'YES',1,100)",
            params![
                fixture.election_id.as_bytes(),
                fixture.participant_a.as_bytes(),
            ],
        )?;
        assert!(matches!(
            fixture.catalog.cast_ballot(crate::CastBallotCommand {
                election_id: fixture.election_id,
                voter_participant_id: fixture.participant_a,
                choice: crate::BallotChoice::Yes,
                now_ms: 101,
            }),
            Err(OperationsError::Conflict(
                "legacy leadership election is read-only"
            ))
        ));
        assert_eq!(
            cast_tallies(&fixture.catalog.connection, fixture.election_id)?,
            (1, 0, 0)
        );
        let subject_ballots: i64 = fixture.catalog.connection.query_row(
            "SELECT COUNT(*) FROM ops_election_subject_ballots WHERE election_id=?1",
            params![fixture.election_id.as_bytes()],
            |row| row.get(0),
        )?;
        assert_eq!(subject_ballots, 0);
        drop(fixture.catalog);
        fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn same_profile_in_two_workspaces_keeps_voter_subject_authority_isolated()
    -> Result<(), Box<dyn Error>> {
        let fixture = subject_fixture()?;
        let workspace_b = WorkspaceId::from_uuid(Uuid::from_u128(0xE601));
        let activity_b = ActivityId::from_uuid(Uuid::from_u128(0xE602));
        let participant_b = Uuid::from_u128(0xE610);
        let session_b = AgentSessionId::from_uuid(Uuid::from_u128(0xE620));
        let participation_b = WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xE630))?;
        let profile_id = Uuid::from_u128(0xE531);
        let election_b = Uuid::from_u128(0xE680);
        fixture.catalog.connection.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,1,1)",
            params![workspace_b.into_uuid().as_bytes(), profile_id.as_bytes(), participation_b.into_uuid().as_bytes()],
        )?;
        fixture.catalog.connection.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,'workspace-b','worker',1,1,1)",
            params![workspace_b.into_uuid().as_bytes(), participant_b.as_bytes(), session_b.into_uuid().as_bytes()],
        )?;
        fixture.catalog.connection.execute(
            "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,1)",
            params![Uuid::from_u128(0xE640).as_bytes(), workspace_b.into_uuid().as_bytes(), participation_b.into_uuid().as_bytes(), profile_id.as_bytes(), Uuid::from_u128(0xE641).as_bytes(), session_b.into_uuid().as_bytes(), participant_b.as_bytes()],
        )?;
        fixture.catalog.connection.execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
            params![workspace_b.into_uuid().as_bytes(), participant_b.as_bytes(), activity_b.into_uuid().as_bytes(), session_b.into_uuid().as_bytes(), Uuid::from_u128(0xE650).as_bytes(), Uuid::from_u128(0xE651).as_bytes()],
        )?;
        fixture.catalog.connection.execute(
            "INSERT INTO ops_leadership_elections(election_id,workspace_id,scope_kind,activity_id,kind,state,incumbent_participant_id,candidate_participant_id,ballot_deadline_ms,elected_participant_id,tally_yes,tally_no,tally_abstain,created_at_ms,closed_at_ms) VALUES(?1,?2,1,?4,'RECALL','ELECTING',NULL,?3,10000,NULL,NULL,NULL,NULL,100,NULL)",
            params![election_b.as_bytes(), workspace_b.into_uuid().as_bytes(), participant_b.as_bytes(), activity_b.into_uuid().as_bytes()],
        )?;
        let tx = fixture.catalog.connection.unchecked_transaction()?;
        let snapshot_a = snapshot_ready_subjects(&tx, fixture.workspace, Some(fixture.activity))?;
        let snapshot_b = snapshot_ready_subjects(&tx, workspace_b, Some(activity_b))?;
        let _a = persist_cutover(
            &tx,
            ElectionSubjectCutoverCommand {
                election_id: fixture.election_id,
                workspace_id: fixture.workspace,
                activity_id: Some(fixture.activity),
                team_id: None,
                source_leadership_revision: 7,
                result_leadership_revision: 8,
                created_at_ms: 100,
            },
            &snapshot_a,
        )?;
        let _b = persist_cutover(
            &tx,
            ElectionSubjectCutoverCommand {
                election_id: election_b,
                workspace_id: workspace_b,
                activity_id: Some(activity_b),
                team_id: None,
                source_leadership_revision: 7,
                result_leadership_revision: 8,
                created_at_ms: 100,
            },
            &snapshot_b,
        )?;
        assert_eq!(electorate_size(&tx, fixture.election_id)?, 1);
        assert_eq!(electorate_size(&tx, election_b)?, 1);
        let _vote_a =
            cast_subject_ballot(&tx, fixture.election_id, fixture.participant_a, "YES", 101)?;
        let _vote_b = cast_subject_ballot(&tx, election_b, participant_b, "NO", 101)?;
        assert_eq!(cast_tallies(&tx, fixture.election_id)?, (1, 0, 0));
        assert_eq!(cast_tallies(&tx, election_b)?, (0, 1, 0));
        tx.commit()?;
        drop(fixture.catalog);
        fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn team_snapshot_uses_only_active_joined_participations() -> Result<(), Box<dyn Error>> {
        let fixture = subject_fixture()?;
        let team_id = Uuid::from_u128(0xE590);
        let participant_c = Uuid::from_u128(0xE512);
        let session_c = AgentSessionId::from_uuid(Uuid::from_u128(0xE522));
        fixture.catalog.connection.execute(
            "INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_at_ms,updated_at_ms) VALUES(?1,?2,'Team','team','ACTIVE',1,'test','Test',1,1)",
            params![team_id.as_bytes(), fixture.workspace.into_uuid().as_bytes()],
        )?;
        fixture.catalog.connection.execute(
            "INSERT INTO ops_team_members(workspace_id,team_id,participation_id,state,revision,joined_at_ms,left_at_ms) VALUES(?1,?2,?3,'JOINED',1,1,NULL)",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                team_id.as_bytes(),
                fixture.participation_id.into_uuid().as_bytes(),
            ],
        )?;
        fixture.catalog.connection.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,'Unmapped','worker',1,1,1)",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                participant_c.as_bytes(),
                session_c.into_uuid().as_bytes(),
            ],
        )?;
        let transaction = fixture.catalog.connection.unchecked_transaction()?;
        let snapshot = snapshot_ready_subjects_for_scope(
            &transaction,
            LeadershipScope::team(fixture.workspace, team_id),
        )?;
        assert_eq!(snapshot.source_participant_count(), 2);
        assert_eq!(snapshot.voter_subject_count(), 1);
        assert!(snapshot.contains_participant(fixture.participant_a));
        assert!(snapshot.contains_participant(fixture.participant_b));
        assert!(!snapshot.contains_participant(participant_c));
        transaction.commit()?;
        drop(fixture.catalog);
        fs::remove_dir_all(fixture.root)?;
        Ok(())
    }
}
