//! Exact readback for one frozen-subject leadership ballot.

use forge_domain::WorkspaceId;
use rusqlite::{OptionalExtension, params};
use uuid::Uuid;

use crate::{
    OperationsCatalog, OperationsError, WorkspaceParticipationId, catalog, election_subject, wire,
};

use super::{BallotChoice, load_election};

/// One immutable ballot row resolved through its frozen election source.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeadershipBallotSnapshot {
    election_id: Uuid,
    voter_participant_id: Uuid,
    voter_participation_id: Option<Uuid>,
    cast_by_participant_id: Uuid,
    choice: BallotChoice,
    ballot_revision: i64,
    cast_at_ms: i64,
}

impl LeadershipBallotSnapshot {
    #[must_use]
    pub const fn election_id(&self) -> Uuid {
        self.election_id
    }

    /// The exact source participant selected by the caller.
    #[must_use]
    pub const fn voter_participant_id(&self) -> Uuid {
        self.voter_participant_id
    }

    /// Canonical `WorkspaceParticipation` subject, absent only for a legacy
    /// participant subject frozen into a cutover election.
    #[must_use]
    pub const fn voter_participation_id(&self) -> Option<Uuid> {
        self.voter_participation_id
    }

    /// Participant that actually inserted the immutable ballot row. This may
    /// differ from `voter_participant_id` when multiple sources share a seat.
    #[must_use]
    pub const fn cast_by_participant_id(&self) -> Uuid {
        self.cast_by_participant_id
    }

    pub const fn choice(&self) -> BallotChoice {
        self.choice
    }

    #[must_use]
    pub const fn ballot_revision(&self) -> i64 {
        self.ballot_revision
    }

    #[must_use]
    pub const fn cast_at_ms(&self) -> i64 {
        self.cast_at_ms
    }
}

impl OperationsCatalog {
    /// Read one ballot through the exact frozen voter source for this election.
    ///
    /// Closed elections remain readable. A missing election, frozen source, or
    /// ballot returns `None`; a source mapped to a shared subject returns that
    /// subject's stored ballot while preserving both the selected source and
    /// the actual participant recorded as its caster.
    ///
    /// # Errors
    /// Rejects invalid IDs, a Workspace mismatch, a pre-cutover election, and
    /// corrupt persisted ballot identity, choice, revision, or timestamp.
    pub fn leadership_ballot(
        &self,
        workspace: WorkspaceId,
        election_id: Uuid,
        voter_participant_id: Uuid,
    ) -> Result<Option<LeadershipBallotSnapshot>, OperationsError> {
        if workspace.into_uuid().is_nil() || election_id.is_nil() || voter_participant_id.is_nil() {
            return Err(OperationsError::Conflict("leadership ballot identity"));
        }

        let tx = self.connection.unchecked_transaction()?;
        let Some(election) = load_election(&tx, election_id)? else {
            catalog::commit_read(tx, self.destination)?;
            return Ok(None);
        };
        if election.scope.workspace_id() != workspace {
            return Err(OperationsError::Conflict("leadership ballot Workspace"));
        }
        if !election_subject::has_cutover(&tx, election_id)? {
            return Err(OperationsError::Conflict(
                "legacy leadership election is read-only",
            ));
        }

        let ballot = tx
            .query_row(
                "SELECT source.subject_kind,substr(source.subject_id,1,17),substr(ballot.cast_by_participant_id,1,17),ballot.choice,ballot.ballot_revision,ballot.cast_at_ms,EXISTS(SELECT 1 FROM ops_election_voter_sources AS caster WHERE caster.election_id=ballot.election_id AND caster.participant_id=ballot.cast_by_participant_id AND caster.subject_kind=ballot.subject_kind AND caster.subject_id=ballot.subject_id) FROM ops_election_voter_sources AS source JOIN ops_election_subject_ballots AS ballot ON ballot.election_id=source.election_id AND ballot.subject_kind=source.subject_kind AND ballot.subject_id=source.subject_id WHERE source.election_id=?1 AND source.participant_id=?2",
                params![election_id.as_bytes(), voter_participant_id.as_bytes()],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, bool>(6)?,
                    ))
                },
            )
            .optional()?;

        let snapshot = ballot
            .map(
                |(
                    subject_kind,
                    subject_raw,
                    cast_by_raw,
                    choice_raw,
                    ballot_revision,
                    cast_at_ms,
                    caster_matches_subject,
                )| {
                    let subject_id = Uuid::from_bytes(wire::id(subject_raw)?);
                    let cast_by_participant_id = Uuid::from_bytes(wire::id(cast_by_raw)?);
                    if subject_id.is_nil() || cast_by_participant_id.is_nil() {
                        return Err(OperationsError::Corrupt("leadership ballot identity"));
                    }
                    if !caster_matches_subject {
                        return Err(OperationsError::Corrupt("leadership ballot caster source"));
                    }
                    let voter_participation_id = match subject_kind {
                        0 => Some(WorkspaceParticipationId::from_uuid(subject_id)?.into_uuid()),
                        1 => None,
                        _ => {
                            return Err(OperationsError::Corrupt("election voter subject kind"));
                        }
                    };
                    if ballot_revision != 1 {
                        return Err(OperationsError::Corrupt("leadership ballot revision"));
                    }
                    if cast_at_ms < 0 {
                        return Err(OperationsError::Corrupt("leadership ballot timestamp"));
                    }
                    Ok(LeadershipBallotSnapshot {
                        election_id,
                        voter_participant_id,
                        voter_participation_id,
                        cast_by_participant_id,
                        choice: BallotChoice::decode(&choice_raw)?,
                        ballot_revision,
                        cast_at_ms,
                    })
                },
            )
            .transpose()?;
        catalog::commit_read(tx, self.destination)?;
        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use forge_domain::WorkspaceId;

    use super::*;
    use crate::{CastBallotCommand, CloseElectionCommand, ElectionState};

    struct Fixture {
        root: PathBuf,
        catalog: OperationsCatalog,
        workspace: WorkspaceId,
        election_id: Uuid,
        participants: Vec<Uuid>,
        participation_ids: Vec<Uuid>,
    }

    fn fixture(
        source_count: usize,
        shared_subject: bool,
        cutover: bool,
    ) -> Result<Fixture, Box<dyn std::error::Error>> {
        if source_count == 0 || source_count > 64 || (shared_subject && source_count < 2) {
            return Err("invalid frozen electorate fixture size".into());
        }
        let seed = Uuid::now_v7().as_u128();
        let root = std::env::temp_dir().join(format!(
            "orchestration-leadership-ballot-{}",
            Uuid::now_v7()
        ));
        std::fs::create_dir(&root)?;
        let destination = Uuid::from_u128(seed + 1);
        let workspace = WorkspaceId::from_uuid(Uuid::from_u128(seed + 2));
        let election_id = Uuid::from_u128(seed + 3);
        let deadline_ms = 1_000_i64;
        let catalog = OperationsCatalog::open(&root.join("catalog.sqlite3"), destination)?;
        let participants = seeded_ids(seed, 0x1000, source_count)?;
        let subject_count = if shared_subject {
            source_count - 1
        } else {
            source_count
        };
        let participation_ids = seeded_ids(seed, 0x2000, subject_count)?;
        let incumbent = participants
            .first()
            .copied()
            .ok_or("frozen electorate fixture has no participant")?;
        let candidate = Uuid::from_u128(seed + 0x3000);
        seed_election(
            &catalog,
            workspace,
            election_id,
            incumbent,
            candidate,
            deadline_ms,
        )?;

        if cutover {
            catalog.connection.execute(
                "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,0,NULL,NULL,'ELECTING',?2,2,NULL,?3,1)",
                params![
                    workspace.into_uuid().as_bytes(),
                    incumbent.as_bytes(),
                    election_id.as_bytes(),
                ],
            )?;
            let source_count_sql = i64::try_from(source_count)?;
            let subject_count_sql = i64::try_from(subject_count)?;
            catalog.connection.execute(
                "INSERT INTO ops_election_subject_cutovers(election_id,workspace_id,scope_kind,activity_id,team_id,source_leadership_revision,result_leadership_revision,subject_revision,source_participant_count,voter_subject_count,electorate_digest,created_at_ms) VALUES(?1,?2,0,NULL,NULL,1,2,1,?3,?4,?5,1)",
                params![
                    election_id.as_bytes(),
                    workspace.into_uuid().as_bytes(),
                    source_count_sql,
                    subject_count_sql,
                    [0xA5_u8; 32].as_slice(),
                ],
            )?;

            let mut subject_sources = vec![1_i64; subject_count];
            if shared_subject {
                subject_sources[0] = 2;
            }
            for (subject_ordinal, (participation_id, source_participant_count)) in
                participation_ids.iter().zip(subject_sources).enumerate()
            {
                let ordinal = u128::try_from(subject_ordinal)?;
                let profile_id = Uuid::from_u128(seed + 0x4000 + ordinal);
                catalog.connection.execute(
                    "INSERT INTO ops_election_voter_subjects(election_id,subject_kind,subject_id,agent_profile_id,participation_revision,source_participant_count) VALUES(?1,0,?2,?3,1,?4)",
                    params![
                        election_id.as_bytes(),
                        participation_id.as_bytes(),
                        profile_id.as_bytes(),
                        source_participant_count,
                    ],
                )?;
            }
            for (source_ordinal, participant) in participants.iter().enumerate() {
                let subject_ordinal = if shared_subject && source_ordinal == 1 {
                    0
                } else if shared_subject && source_ordinal > 1 {
                    source_ordinal - 1
                } else {
                    source_ordinal
                };
                catalog.connection.execute(
                    "INSERT INTO ops_election_voter_sources(election_id,participant_id,membership_revision,team_membership_revision,subject_kind,subject_id) VALUES(?1,?2,1,NULL,0,?3)",
                    params![
                        election_id.as_bytes(),
                        participant.as_bytes(),
                        participation_ids[subject_ordinal].as_bytes(),
                    ],
                )?;
            }
        }

        Ok(Fixture {
            root,
            catalog,
            workspace,
            election_id,
            participants,
            participation_ids,
        })
    }

    fn seeded_ids(
        seed: u128,
        offset: u128,
        count: usize,
    ) -> Result<Vec<Uuid>, std::num::TryFromIntError> {
        (0..count)
            .map(|ordinal| Ok(Uuid::from_u128(seed + offset + u128::try_from(ordinal)?)))
            .collect()
    }

    fn seed_election(
        catalog: &OperationsCatalog,
        workspace: WorkspaceId,
        election_id: Uuid,
        incumbent: Uuid,
        candidate: Uuid,
        deadline_ms: i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        catalog.connection.execute(
            "INSERT INTO ops_leadership_elections(election_id,workspace_id,scope_kind,activity_id,team_id,kind,state,incumbent_participant_id,candidate_participant_id,ballot_deadline_ms,elected_participant_id,tally_yes,tally_no,tally_abstain,created_at_ms,closed_at_ms) VALUES(?1,?2,0,NULL,NULL,'RECALL','ELECTING',?3,?4,?5,NULL,NULL,NULL,NULL,1,NULL)",
            params![election_id.as_bytes(), workspace.into_uuid().as_bytes(), incumbent.as_bytes(), candidate.as_bytes(), deadline_ms],
        )?;
        Ok(())
    }

    #[test]
    fn readback_preserves_shared_subject_caster_after_close_at_64_source_bound()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = fixture(64, true, true)?;
        let workspace = fixture.workspace;
        let election_id = fixture.election_id;
        let first_source = fixture.participants[0];
        let alias_source = fixture.participants[1];
        let last_source = fixture.participants[63];
        let shared_participation = fixture.participation_ids[0];
        assert!(
            fixture
                .catalog
                .leadership_ballot(workspace, election_id, alias_source)?
                .is_none()
        );

        let first = fixture.catalog.cast_ballot(CastBallotCommand {
            election_id,
            voter_participant_id: first_source,
            choice: BallotChoice::Yes,
            now_ms: 100,
        })?;
        assert!(!first.replay());
        assert_eq!(
            first
                .voter_participation_id()
                .map(WorkspaceParticipationId::into_uuid),
            Some(shared_participation)
        );

        let alias_replay = fixture.catalog.cast_ballot(CastBallotCommand {
            election_id,
            voter_participant_id: alias_source,
            choice: BallotChoice::Yes,
            now_ms: 101,
        })?;
        assert!(alias_replay.replay());
        assert_eq!(alias_replay.voter_participant_id(), alias_source);

        let final_source = fixture.catalog.cast_ballot(CastBallotCommand {
            election_id,
            voter_participant_id: last_source,
            choice: BallotChoice::No,
            now_ms: 102,
        })?;
        assert!(!final_source.replay());
        let tally = fixture.catalog.close_election(CloseElectionCommand {
            election_id,
            now_ms: 1_001,
        })?;
        assert_eq!(tally.state(), ElectionState::ClosedNoMajority);

        let alias = fixture
            .catalog
            .leadership_ballot(workspace, election_id, alias_source)?
            .ok_or("shared-subject ballot missing after close")?;
        assert_eq!(alias.election_id(), election_id);
        assert_eq!(alias.voter_participant_id(), alias_source);
        assert_eq!(alias.voter_participation_id(), Some(shared_participation));
        assert_eq!(alias.cast_by_participant_id(), first_source);
        assert_eq!(alias.choice(), BallotChoice::Yes);
        assert_eq!(alias.ballot_revision(), 1);
        assert_eq!(alias.cast_at_ms(), 100);

        let last = fixture
            .catalog
            .leadership_ballot(workspace, election_id, last_source)?
            .ok_or("64th-source ballot missing after close")?;
        assert_eq!(last.voter_participant_id(), last_source);
        assert_eq!(last.cast_by_participant_id(), last_source);
        assert_eq!(
            last.voter_participation_id(),
            Some(fixture.participation_ids[62])
        );
        assert_eq!(last.choice(), BallotChoice::No);
        assert_eq!(last.cast_at_ms(), 102);

        let root = fixture.root.clone();
        drop(fixture);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn missing_ballot_is_absent_and_foreign_workspace_conflicts()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = fixture(2, false, true)?;
        let participant = fixture.participants[0];
        assert!(
            fixture
                .catalog
                .leadership_ballot(fixture.workspace, fixture.election_id, participant)?
                .is_none()
        );
        assert!(matches!(
            fixture.catalog.leadership_ballot(
                WorkspaceId::from_uuid(Uuid::from_u128(
                    fixture.workspace.into_uuid().as_u128() + 90
                )),
                fixture.election_id,
                participant,
            ),
            Err(OperationsError::Conflict("leadership ballot Workspace"))
        ));
        assert!(
            fixture
                .catalog
                .leadership_ballot(
                    fixture.workspace,
                    Uuid::from_u128(fixture.election_id.as_u128() + 90),
                    participant,
                )?
                .is_none()
        );
        let root = fixture.root.clone();
        drop(fixture);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn legacy_election_without_frozen_subject_cutover_fails_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = fixture(2, false, false)?;
        assert!(matches!(
            fixture.catalog.leadership_ballot(
                fixture.workspace,
                fixture.election_id,
                fixture.participants[0],
            ),
            Err(OperationsError::Conflict(
                "legacy leadership election is read-only"
            ))
        ));
        let root = fixture.root.clone();
        drop(fixture);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn corrupt_ballot_identity_revision_and_time_are_rejected()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = fixture(2, false, true)?;
        let workspace = fixture.workspace;
        let election_id = fixture.election_id;
        let participant = fixture.participants[0];
        let _accepted = fixture.catalog.cast_ballot(CastBallotCommand {
            election_id,
            voter_participant_id: participant,
            choice: BallotChoice::Yes,
            now_ms: 100,
        })?;
        fixture
            .catalog
            .connection
            .execute_batch("PRAGMA ignore_check_constraints=ON;")?;

        fixture.catalog.connection.execute(
            "UPDATE ops_election_subject_ballots SET cast_by_participant_id=zeroblob(16) WHERE election_id=?1",
            params![election_id.as_bytes()],
        )?;
        assert!(matches!(
            fixture
                .catalog
                .leadership_ballot(workspace, election_id, participant),
            Err(OperationsError::Corrupt("leadership ballot identity"))
        ));
        for invalid_caster in [Uuid::from_u128(0xF001), fixture.participants[1]] {
            fixture.catalog.connection.execute(
                "UPDATE ops_election_subject_ballots SET cast_by_participant_id=?2 WHERE election_id=?1",
                params![election_id.as_bytes(), invalid_caster.as_bytes()],
            )?;
            assert!(matches!(
                fixture
                    .catalog
                    .leadership_ballot(workspace, election_id, participant),
                Err(OperationsError::Corrupt("leadership ballot caster source"))
            ));
        }
        fixture.catalog.connection.execute(
            "UPDATE ops_election_subject_ballots SET cast_by_participant_id=?2,ballot_revision=2 WHERE election_id=?1",
            params![election_id.as_bytes(), participant.as_bytes()],
        )?;
        assert!(matches!(
            fixture
                .catalog
                .leadership_ballot(workspace, election_id, participant),
            Err(OperationsError::Corrupt("leadership ballot revision"))
        ));
        fixture.catalog.connection.execute(
            "UPDATE ops_election_subject_ballots SET ballot_revision=1,cast_at_ms=-1 WHERE election_id=?1",
            params![election_id.as_bytes()],
        )?;
        assert!(matches!(
            fixture
                .catalog
                .leadership_ballot(workspace, election_id, participant),
            Err(OperationsError::Corrupt("leadership ballot timestamp"))
        ));
        fixture
            .catalog
            .connection
            .execute_batch("PRAGMA ignore_check_constraints=OFF;")?;

        let root = fixture.root.clone();
        drop(fixture);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
