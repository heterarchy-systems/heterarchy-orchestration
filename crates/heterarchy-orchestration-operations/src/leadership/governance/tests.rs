use super::*;
use crate::LeadershipScope;
use crate::{LeadershipConfirmIntentState, PrepareLeadershipConfirmCommand, TeamRetireCommand};
use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, TimestampMillis, WorkspaceId};
use rusqlite::params;
use std::path::PathBuf;
use uuid::Uuid;

pub(crate) const NOW: i64 = 10_000_000;
const MEMBER_CREATED_AT_MS: i64 = NOW - crate::leadership::NEW_MEMBER_PROBATION_MS;

pub(crate) struct Fixture {
    pub(crate) root: PathBuf,
    pub(crate) catalog: OperationsCatalog,
    pub(crate) workspace: WorkspaceId,
    pub(crate) leader: Uuid,
    pub(crate) candidate: Uuid,
    pub(crate) leader_participation: Uuid,
    pub(crate) leader_actor: OperationalActor,
    pub(crate) candidate_actor: OperationalActor,
}

pub(crate) fn fixture(seed: u128) -> Result<Fixture, Box<dyn std::error::Error>> {
    let root =
        std::env::temp_dir().join(format!("leadership-governance-{seed:x}-{}", Uuid::now_v7()));
    std::fs::create_dir(&root)?;
    let destination = Uuid::from_u128(seed);
    let migration = Uuid::from_u128(seed + 1);
    let workspace = WorkspaceId::from_uuid(Uuid::from_u128(seed + 2));
    let activity = ActivityId::from_uuid(Uuid::from_u128(seed + 3));
    let leader = Uuid::from_u128(seed + 4);
    let candidate = Uuid::from_u128(seed + 5);
    let leader_participation = Uuid::from_u128(seed + 0x230);
    let candidate_participation = Uuid::from_u128(seed + 0x231);
    let mut catalog = OperationsCatalog::open(&root.join("catalog.sqlite3"), destination)?;

    catalog.connection.execute(
        "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
        params![
            migration.as_bytes(),
            workspace.into_uuid().as_bytes(),
            destination.as_bytes(),
            [0_u8; 32].as_slice()
        ],
    )?;

    let leader_actor = insert_participant(
        &mut catalog,
        workspace,
        activity,
        leader,
        seed + 0x20,
        seed + 0x30,
        "leader",
    )?;
    let candidate_actor = insert_participant(
        &mut catalog,
        workspace,
        activity,
        candidate,
        seed + 0x21,
        seed + 0x31,
        "candidate",
    )?;
    insert_credit(
        &mut catalog,
        workspace,
        activity,
        leader,
        seed + 0x40,
        seed + 0x50,
        1,
    )?;
    insert_credit(
        &mut catalog,
        workspace,
        activity,
        candidate,
        seed + 0x41,
        seed + 0x51,
        5,
    )?;

    catalog.connection.execute(
        "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,0,NULL,NULL,'ACTIVE',?2,1,NULL,NULL,?3)",
        params![workspace.into_uuid().as_bytes(), leader.as_bytes(), NOW],
    )?;
    seed_workspace_teams(
        &catalog.connection,
        workspace,
        seed,
        leader,
        candidate,
        leader_participation,
        candidate_participation,
    )?;

    Ok(Fixture {
        root,
        catalog,
        workspace,
        leader,
        candidate,
        leader_participation,
        leader_actor,
        candidate_actor,
    })
}

fn exact_statement_readback(
    fixture: &Fixture,
    request_id: Uuid,
    statement_id: Uuid,
) -> Result<LeadershipStatementReadback, Box<dyn std::error::Error>> {
    let readback = fixture
        .catalog
        .leadership_statement(fixture.workspace, statement_id)?
        .ok_or(OperationsError::Corrupt(
            "missing leadership statement readback",
        ))?;
    assert_eq!(readback.workspace_id(), fixture.workspace);
    assert_eq!(readback.request_id(), request_id);
    assert_eq!(readback.statement().statement_id(), statement_id);
    Ok(readback)
}

fn seed_workspace_teams(
    connection: &rusqlite::Connection,
    workspace: WorkspaceId,
    seed: u128,
    leader: Uuid,
    candidate: Uuid,
    leader_participation: Uuid,
    candidate_participation: Uuid,
) -> rusqlite::Result<()> {
    for (team_offset, team_leader, assignment_offset) in
        [(0x60, leader, 0x70), (0x61, candidate, 0x71)]
    {
        let team_id = Uuid::from_u128(seed + team_offset);
        connection.execute(
            "INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_at_ms,updated_at_ms,formed_at_ms) VALUES(?1,?2,'Voting Team','workspace electorate','ACTIVE',1,'test','Test',?3,?3,?3)",
            params![
                team_id.as_bytes(),
                workspace.into_uuid().as_bytes(),
                MEMBER_CREATED_AT_MS,
            ],
        )?;
        for participation_id in [leader_participation, candidate_participation] {
            connection.execute(
                "INSERT INTO ops_team_members(workspace_id,team_id,participation_id,state,revision,joined_at_ms,left_at_ms,role) VALUES(?1,?2,?3,'JOINED',1,?4,NULL,'verification')",
                params![
                    workspace.into_uuid().as_bytes(),
                    team_id.as_bytes(),
                    participation_id.as_bytes(),
                    MEMBER_CREATED_AT_MS,
                ],
            )?;
        }
        connection.execute(
            "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,2,NULL,?2,'ACTIVE',?3,2,NULL,NULL,?4)",
            params![
                workspace.into_uuid().as_bytes(),
                team_id.as_bytes(),
                team_leader.as_bytes(),
                NOW,
            ],
        )?;
        connection.execute(
            "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,work_assignment_id,election_id,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,2,zeroblob(16),?2,1,'BOOTSTRAP',?3,NULL,?4,1,?5), (?1,2,zeroblob(16),?2,2,'CONFIRMED',NULL,NULL,?4,2,?6)",
            params![
                workspace.into_uuid().as_bytes(),
                team_id.as_bytes(),
                Uuid::from_u128(seed + assignment_offset).as_bytes(),
                team_leader.as_bytes(),
                MEMBER_CREATED_AT_MS,
                NOW,
            ],
        )?;
    }
    Ok(())
}

fn insert_participant(
    catalog: &mut OperationsCatalog,
    workspace: WorkspaceId,
    activity: ActivityId,
    participant: Uuid,
    session_seed: u128,
    agent_seed: u128,
    name: &str,
) -> Result<OperationalActor, Box<dyn std::error::Error>> {
    let profile_id = Uuid::from_u128(agent_seed + 0x100);
    let participation_id = Uuid::from_u128(agent_seed + 0x200);
    let session_id = Uuid::from_u128(session_seed);
    let agent_id = AgentInstanceId::from_uuid(Uuid::from_u128(agent_seed));
    let request_id = Uuid::from_u128(agent_seed + 0x300);
    catalog.connection.execute(
        "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,?4,'worker',1,1,?5)",
        params![
            workspace.into_uuid().as_bytes(),
            participant.as_bytes(),
            session_id.as_bytes(),
            name,
            MEMBER_CREATED_AT_MS,
        ],
    )?;
    catalog.connection.execute(
        "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,?4,1)",
        params![
            workspace.into_uuid().as_bytes(),
            profile_id.as_bytes(),
            participation_id.as_bytes(),
            MEMBER_CREATED_AT_MS,
        ],
    )?;
    catalog.connection.execute(
        "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,?8)",
        params![
            request_id.as_bytes(),
            workspace.into_uuid().as_bytes(),
            participation_id.as_bytes(),
            profile_id.as_bytes(),
            Uuid::from_u128(agent_seed + 0x301).as_bytes(),
            session_id.as_bytes(),
            participant.as_bytes(),
            MEMBER_CREATED_AT_MS,
        ],
    )?;
    catalog.connection.execute(
        "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
        params![
            workspace.into_uuid().as_bytes(),
            participant.as_bytes(),
            activity.into_uuid().as_bytes(),
            session_id.as_bytes(),
            agent_id.into_uuid().as_bytes(),
            Uuid::from_u128(agent_seed + 0x302).as_bytes(),
        ],
    )?;
    Ok(OperationalActor::Participant {
        activity_id: activity,
        agent_id,
        session_id: AgentSessionId::from_uuid(session_id),
    })
}

fn insert_credit(
    catalog: &mut OperationsCatalog,
    workspace: WorkspaceId,
    activity: ActivityId,
    participant: Uuid,
    assignment_seed: u128,
    agent_seed: u128,
    budget: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    catalog.connection.execute(
        "INSERT INTO ops_work_credit_entries(credit_id,workspace_id,assignment_id,participant_id,agent_id,activity_id,goal_id,node_id,budget,award_source_sequence,evidence_execution_id,awarded_at_ms,created_digest) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,1,?10,?11,?12)",
        params![
            Uuid::from_u128(assignment_seed + 0x10).as_bytes(),
            workspace.into_uuid().as_bytes(),
            Uuid::from_u128(assignment_seed).as_bytes(),
            participant.as_bytes(),
            Uuid::from_u128(agent_seed).as_bytes(),
            activity.into_uuid().as_bytes(),
            Uuid::from_u128(assignment_seed + 1).as_bytes(),
            Uuid::from_u128(assignment_seed + 2).as_bytes(),
            budget,
            Uuid::from_u128(assignment_seed + 3).as_bytes(),
            NOW,
            [9_u8; 32].as_slice(),
        ],
    )?;
    Ok(())
}

fn request_at_revision(
    fixture: &mut Fixture,
    request_id: Uuid,
    now_ms: i64,
    expected_revision: i64,
    actor: OperationalActor,
) -> Result<LeadershipReviewReceipt, OperationsError> {
    fixture
        .catalog
        .request_leadership_review(LeadershipReviewCommand {
            request_id,
            scope: LeadershipScope::workspace(fixture.workspace),
            expected_revision,
            actor,
            candidate_participant_id: fixture.candidate,
            reason: "review reason".to_owned(),
            now_ms,
        })
}

fn request_as(
    fixture: &mut Fixture,
    request_id: Uuid,
    now_ms: i64,
    actor: OperationalActor,
) -> Result<LeadershipReviewReceipt, OperationsError> {
    request_at_revision(fixture, request_id, now_ms, 1, actor)
}

fn request(
    fixture: &mut Fixture,
    request_id: Uuid,
    now_ms: i64,
) -> Result<LeadershipReviewReceipt, OperationsError> {
    let leader_actor = fixture.leader_actor;
    request_as(fixture, request_id, now_ms, leader_actor)
}

fn insert_workspace_receipt(
    fixture: &Fixture,
    receipt_kind: &str,
    election_id: Option<Uuid>,
    participant: Uuid,
    recorded_at_ms: i64,
) -> rusqlite::Result<()> {
    let ordinal: i64 = fixture.catalog.connection.query_row(
        "SELECT COALESCE(MAX(receipt_ordinal),0)+1 FROM ops_leadership_receipts WHERE workspace_id=?1 AND scope_kind=0 AND activity_id=zeroblob(16) AND team_id=zeroblob(16)",
        params![fixture.workspace.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    fixture.catalog.connection.execute(
        "INSERT INTO ops_leadership_receipts(workspace_id,scope_kind,activity_id,team_id,receipt_ordinal,receipt_kind,work_assignment_id,election_id,participant_id,leadership_revision,recorded_at_ms) VALUES(?1,0,zeroblob(16),zeroblob(16),?2,?3,NULL,?4,?5,?6,?7)",
        params![
            fixture.workspace.into_uuid().as_bytes(),
            ordinal,
            receipt_kind,
            election_id.map(|id| id.as_bytes().to_vec()),
            participant.as_bytes(),
            ordinal + 1,
            recorded_at_ms,
        ],
    )?;
    Ok(())
}

fn insert_workspace_election_receipt(
    fixture: &Fixture,
    election_id: Uuid,
    incumbent: Uuid,
    elected: Uuid,
    receipt_kind: &str,
    recorded_at_ms: i64,
) -> rusqlite::Result<()> {
    fixture.catalog.connection.execute(
        "INSERT INTO ops_leadership_elections(election_id,workspace_id,scope_kind,activity_id,team_id,kind,state,incumbent_participant_id,candidate_participant_id,ballot_deadline_ms,elected_participant_id,tally_yes,tally_no,tally_abstain,created_at_ms,closed_at_ms) VALUES(?1,?2,0,NULL,NULL,'RECALL','CLOSED_ELECTED',?3,?4,?5,?4,1,0,0,?6,?5)",
        params![
            election_id.as_bytes(),
            fixture.workspace.into_uuid().as_bytes(),
            incumbent.as_bytes(),
            elected.as_bytes(),
            recorded_at_ms,
            recorded_at_ms.saturating_sub(1),
        ],
    )?;
    insert_workspace_receipt(
        fixture,
        receipt_kind,
        Some(election_id),
        elected,
        recorded_at_ms,
    )
}

fn insert_profile_session_alias(
    fixture: &Fixture,
    profile_id: Uuid,
    participation_id: Uuid,
    participant_id: Uuid,
    session_id: Uuid,
    request_id: Uuid,
    mapped_at_ms: i64,
) -> rusqlite::Result<()> {
    fixture.catalog.connection.execute(
        "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,'profile alias','worker',1,1,?4)",
        params![
            fixture.workspace.into_uuid().as_bytes(),
            participant_id.as_bytes(),
            session_id.as_bytes(),
            mapped_at_ms,
        ],
    )?;
    fixture.catalog.connection.execute(
        "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,?8)",
        params![
            request_id.as_bytes(),
            fixture.workspace.into_uuid().as_bytes(),
            participation_id.as_bytes(),
            profile_id.as_bytes(),
            Uuid::from_u128(request_id.as_u128() + 1).as_bytes(),
            session_id.as_bytes(),
            participant_id.as_bytes(),
            mapped_at_ms,
        ],
    )?;
    Ok(())
}

fn insert_election_candidate_subject(
    fixture: &Fixture,
    election_id: Uuid,
    candidate_participant_id: Uuid,
    candidate_subject_id: Uuid,
    profile_id: Uuid,
) -> rusqlite::Result<()> {
    fixture.catalog.connection.execute(
        "INSERT INTO ops_election_work_credit_snapshots(election_id,workspace_id,team_id,candidate_participant_id,subject_kind,subject_id,agent_profile_id,participation_revision,subject_revision,source_participant_count,window_ms,evaluated_at_ms,candidate_total_credits,workspace_total_credits,workspace_arithmetic_mean,mean_remainder_numerator,mean_subject_count,qualified,snapshot_digest) VALUES(?1,?2,NULL,?3,0,?4,?5,1,1,2,10000,?6,6,6,6,0,1,1,?7)",
        params![
            election_id.as_bytes(),
            fixture.workspace.into_uuid().as_bytes(),
            candidate_participant_id.as_bytes(),
            candidate_subject_id.as_bytes(),
            profile_id.as_bytes(),
            NOW,
            [0x51_u8; 32].as_slice(),
        ],
    )?;
    Ok(())
}

fn insert_review_incumbent_identity_witness(
    fixture: &Fixture,
    seed: u128,
    election_id: Uuid,
) -> rusqlite::Result<()> {
    let profile = Uuid::from_u128(seed + 0x130);
    let session = Uuid::from_u128(seed + 0x20);
    let activity = Uuid::from_u128(seed + 3);
    let agent = Uuid::from_u128(seed + 0x30);
    fixture.catalog.connection.execute(
        "INSERT INTO ops_leadership_reviews(request_id,election_id,workspace_id,scope_kind,activity_id,team_id,expected_leadership_revision,proposer_kind,proposer_activity_id,proposer_agent_id,proposer_session_id,proposer_participation_id,proposer_profile_id,proposer_participant_id,proposer_display_name,incumbent_participant_id,incumbent_participation_id,incumbent_profile_id,incumbent_session_id,incumbent_display_name,reason,defense_deadline_ms,discussion_deadline_ms,ballot_deadline_ms,defense_closed_at_ms,incumbent_statement_id,created_at_ms) VALUES(?1,?2,?3,0,NULL,NULL,1,1,?4,?5,?6,?7,?8,?9,'leader',?9,?7,?8,?6,'leader','identity witness',?10,?11,?12,NULL,NULL,?13)",
        params![
            Uuid::from_u128(seed + 0x911).as_bytes(),
            election_id.as_bytes(),
            fixture.workspace.into_uuid().as_bytes(),
            activity.as_bytes(),
            agent.as_bytes(),
            session.as_bytes(),
            fixture.leader_participation.as_bytes(),
            profile.as_bytes(),
            fixture.leader.as_bytes(),
            NOW + 60_000,
            NOW + 90_000,
            NOW + 210_000,
            NOW,
        ],
    )?;
    Ok(())
}

fn insert_election_incumbent_participation_source(
    fixture: &Fixture,
    election_id: Uuid,
    alias_participant_id: Uuid,
    profile_id: Uuid,
) -> rusqlite::Result<()> {
    fixture.catalog.connection.execute(
        "INSERT INTO ops_election_subject_cutovers(election_id,workspace_id,scope_kind,activity_id,team_id,source_leadership_revision,result_leadership_revision,subject_revision,source_participant_count,voter_subject_count,electorate_digest,created_at_ms) VALUES(?1,?2,0,NULL,NULL,1,2,1,2,1,?3,?4)",
        params![
            election_id.as_bytes(),
            fixture.workspace.into_uuid().as_bytes(),
            [0x61_u8; 32].as_slice(),
            NOW,
        ],
    )?;
    fixture.catalog.connection.execute(
        "INSERT INTO ops_election_voter_subjects(election_id,subject_kind,subject_id,agent_profile_id,participation_revision,source_participant_count) VALUES(?1,0,?2,?3,1,2)",
        params![
            election_id.as_bytes(),
            fixture.leader_participation.as_bytes(),
            profile_id.as_bytes(),
        ],
    )?;
    fixture.catalog.connection.execute(
        "INSERT INTO ops_election_voter_sources(election_id,participant_id,membership_revision,team_membership_revision,subject_kind,subject_id) VALUES(?1,?2,1,NULL,0,?3),(?1,?4,1,NULL,0,?3)",
        params![
            election_id.as_bytes(),
            fixture.leader.as_bytes(),
            fixture.leader_participation.as_bytes(),
            alias_participant_id.as_bytes(),
        ],
    )?;
    Ok(())
}

fn assert_late_mapping_preserves_cooldown(
    seed: u128,
    mapped_at_ms: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = fixture(seed)?;
    let alias = Uuid::from_u128(seed + 0x900);
    insert_workspace_election_receipt(
        &fixture,
        Uuid::from_u128(seed + 0x910),
        fixture.leader,
        alias,
        "CONFIRMED",
        NOW,
    )?;
    insert_profile_session_alias(
        &fixture,
        Uuid::from_u128(seed + 0x130),
        fixture.leader_participation,
        alias,
        Uuid::from_u128(seed + 0x901),
        Uuid::from_u128(seed + 0x902),
        mapped_at_ms,
    )?;
    let leader_actor = fixture.leader_actor;
    assert!(matches!(
        request_at_revision(
            &mut fixture,
            Uuid::from_u128(seed + 0x920),
            NOW + 1,
            1,
            leader_actor,
        ),
        Err(OperationsError::Conflict(
            "leadership re-election cooldown active"
        ))
    ));
    drop(fixture.catalog);
    std::fs::remove_dir_all(fixture.root)?;
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn review_lifecycle_has_bounded_windows_early_close_replay_and_forgery_guards()
-> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = fixture(0x7100)?;
    let request_id = Uuid::from_u128(0x7200);
    let receipt = request(&mut fixture, request_id, NOW)?;
    assert_eq!(receipt.phase(), LeadershipReviewPhase::Defense);
    assert_eq!(receipt.defense_deadline_ms(), NOW + 60_000);
    assert_eq!(receipt.discussion_deadline_ms(), NOW + 90_000);
    assert_eq!(receipt.ballot_deadline_ms(), NOW + 210_000);

    let replay = request(&mut fixture, request_id, NOW + 1)?;
    assert!(replay.replay());
    assert!(matches!(
        fixture
            .catalog
            .request_leadership_review(LeadershipReviewCommand {
                request_id,
                scope: LeadershipScope::workspace(fixture.workspace),
                expected_revision: 1,
                actor: fixture.leader_actor,
                candidate_participant_id: fixture.candidate,
                reason: "changed".to_owned(),
                now_ms: NOW + 2,
            }),
        Err(OperationsError::Conflict(
            "leadership review replay changed"
        ))
    ));

    let election_id = receipt.election_id();
    assert!(matches!(
        fixture
            .catalog
            .cast_ballot(super::super::CastBallotCommand {
                election_id,
                voter_participant_id: fixture.leader,
                choice: super::super::BallotChoice::Yes,
                now_ms: NOW + 1,
            }),
        Err(OperationsError::Conflict(
            "leadership review discussion phase"
        ))
    ));

    assert!(matches!(
        fixture
            .catalog
            .append_leadership_statement(LeadershipStatementCommand {
                statement_id: Uuid::from_u128(0x7300),
                request_id,
                election_id,
                workspace_id: fixture.workspace,
                actor: fixture.leader_actor,
                kind: LeadershipStatementKind::Discussion,
                body: "too early".to_owned(),
                now_ms: NOW + 60_000 - 1,
            }),
        Err(OperationsError::Conflict("leadership discussion deadline"))
    ));

    let defense_id = Uuid::from_u128(0x7301);
    let defense = fixture
        .catalog
        .append_leadership_statement(LeadershipStatementCommand {
            statement_id: defense_id,
            request_id,
            election_id,
            workspace_id: fixture.workspace,
            actor: fixture.leader_actor,
            kind: LeadershipStatementKind::Defense,
            body: "incumbent defense".to_owned(),
            now_ms: NOW + 1,
        })?;
    assert_eq!(defense.sequence(), 1);
    assert_eq!(defense.phase(), LeadershipReviewPhase::Discussion);

    let defense_replay =
        fixture
            .catalog
            .append_leadership_statement(LeadershipStatementCommand {
                statement_id: defense_id,
                request_id,
                election_id,
                workspace_id: fixture.workspace,
                actor: fixture.leader_actor,
                kind: LeadershipStatementKind::Defense,
                body: "incumbent defense".to_owned(),
                now_ms: NOW + 2,
            })?;
    assert!(defense_replay.replay());
    assert!(matches!(
        fixture
            .catalog
            .append_leadership_statement(LeadershipStatementCommand {
                statement_id: defense_id,
                request_id,
                election_id,
                workspace_id: fixture.workspace,
                actor: fixture.leader_actor,
                kind: LeadershipStatementKind::Defense,
                body: "changed defense".to_owned(),
                now_ms: NOW + 3,
            }),
        Err(OperationsError::Conflict("leadership statement replay"))
    ));

    // Early close at NOW+1: discussion ends at the defense deadline floor
    // (max(close + 30s, defense deadline) = NOW + 60_000).
    let discussion_deadline = NOW + 60_000;
    let statement = fixture
        .catalog
        .append_leadership_statement(LeadershipStatementCommand {
            statement_id: Uuid::from_u128(0x7302),
            request_id,
            election_id,
            workspace_id: fixture.workspace,
            actor: fixture.leader_actor,
            kind: LeadershipStatementKind::Discussion,
            body: "discussion".to_owned(),
            now_ms: discussion_deadline - 1,
        })?;
    assert_eq!(statement.sequence(), 2);
    assert!(matches!(
        fixture
            .catalog
            .append_leadership_statement(LeadershipStatementCommand {
                statement_id: Uuid::from_u128(0x7303),
                request_id,
                election_id,
                workspace_id: fixture.workspace,
                actor: fixture.leader_actor,
                kind: LeadershipStatementKind::Discussion,
                body: "too late".to_owned(),
                now_ms: discussion_deadline,
            }),
        Err(OperationsError::Conflict("leadership discussion deadline"))
    ));

    let snapshot = fixture
        .catalog
        .read_leadership_review(
            fixture.workspace,
            LeadershipReviewQuery::request(request_id),
            discussion_deadline,
            None,
            64,
        )?
        .ok_or("review missing")?;
    assert_eq!(snapshot.electorate().len(), 2);
    assert!(!snapshot.electorate_truncated());
    assert_eq!(snapshot.statements().statements().len(), 2);
    assert_eq!(
        snapshot
            .incumbent_response()
            .map(LeadershipStatementRecord::statement_id),
        Some(defense_id)
    );

    assert!(matches!(
        fixture.catalog.read_leadership_review(
            fixture.workspace,
            LeadershipReviewQuery::request(request_id),
            NOW,
            None,
            65,
        ),
        Err(OperationsError::Bounds("leadership statement page"))
    ));

    assert!(
        fixture
            .catalog
            .append_leadership_statement(LeadershipStatementCommand {
                statement_id: Uuid::from_u128(0x7304),
                request_id,
                election_id,
                workspace_id: WorkspaceId::from_uuid(Uuid::from_u128(0x7400)),
                actor: fixture.leader_actor,
                kind: LeadershipStatementKind::Discussion,
                body: "forged scope".to_owned(),
                now_ms: NOW + 2,
            })
            .is_err()
    );

    drop(fixture.catalog);
    std::fs::remove_dir_all(fixture.root)?;
    Ok(())
}

#[test]
fn review_snapshot_rejects_an_incumbent_statement_outside_its_canonical_context()
-> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = fixture(0x7300)?;
    let request_id = Uuid::from_u128(0x7310);
    let receipt = request(&mut fixture, request_id, NOW)?;
    let statement_id = Uuid::from_u128(0x7320);
    let defense = fixture
        .catalog
        .append_leadership_statement(LeadershipStatementCommand {
            statement_id,
            request_id,
            election_id: receipt.election_id(),
            workspace_id: fixture.workspace,
            actor: fixture.leader_actor,
            kind: LeadershipStatementKind::Defense,
            body: "canonical incumbent defense".to_owned(),
            now_ms: NOW + 1,
        })?;
    assert_eq!(defense.statement_id(), statement_id);

    let snapshot = fixture
        .catalog
        .read_leadership_review(
            fixture.workspace,
            LeadershipReviewQuery::request(request_id),
            NOW + 1,
            None,
            64,
        )?
        .ok_or("review missing")?;
    assert_eq!(
        snapshot
            .incumbent_response()
            .map(LeadershipStatementRecord::statement_id),
        Some(statement_id)
    );
    drop(snapshot);

    let foreign_workspace = Uuid::from_u128(0x7330);
    fixture.catalog.connection.execute(
        "UPDATE ops_leadership_review_statements SET workspace_id=?1 WHERE statement_id=?2",
        params![foreign_workspace.as_bytes(), statement_id.as_bytes()],
    )?;
    assert!(matches!(
        fixture.catalog.read_leadership_review(
            fixture.workspace,
            LeadershipReviewQuery::request(request_id),
            NOW + 1,
            None,
            64,
        ),
        Err(OperationsError::Corrupt(
            "leadership incumbent statement context"
        ))
    ));

    fixture.catalog.connection.execute(
        "UPDATE ops_leadership_review_statements SET workspace_id=?1,statement_kind='DISCUSSION',incumbent_final=0 WHERE statement_id=?2",
        params![
            fixture.workspace.into_uuid().as_bytes(),
            statement_id.as_bytes(),
        ],
    )?;
    assert!(matches!(
        fixture.catalog.read_leadership_review(
            fixture.workspace,
            LeadershipReviewQuery::request(request_id),
            NOW + 1,
            None,
            64,
        ),
        Err(OperationsError::Corrupt(
            "leadership incumbent statement context"
        ))
    ));

    drop(fixture.catalog);
    std::fs::remove_dir_all(fixture.root)?;
    Ok(())
}

#[test]
fn review_request_and_defense_authority_follow_agent_membership()
-> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = fixture(0x7500)?;
    assert!(matches!(
        request_as(
            &mut fixture,
            Uuid::from_u128(0x75FF),
            NOW,
            OperationalActor::LocalOperator,
        ),
        Err(OperationsError::Conflict(
            "leadership proposer requires participant"
        ))
    ));
    let election_count: i64 = fixture.catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_leadership_elections WHERE workspace_id=?1 AND scope_kind=0",
        params![fixture.workspace.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(election_count, 0);

    fixture.catalog.connection.execute(
        "UPDATE ops_logical_members SET created_at_ms=?1 WHERE workspace_id=?2 AND participant_id=?3",
        params![NOW, fixture.workspace.into_uuid().as_bytes(), fixture.leader.as_bytes()],
    )?;
    fixture.catalog.connection.execute(
        "UPDATE ops_workspace_participations SET created_at_ms=?1 WHERE workspace_id=?2 AND participation_id=?3",
        params![
            NOW,
            fixture.workspace.into_uuid().as_bytes(),
            fixture.leader_participation.as_bytes(),
        ],
    )?;
    fixture.catalog.connection.execute(
        "UPDATE ops_team_members SET joined_at_ms=?1 WHERE workspace_id=?2 AND participation_id=?3",
        params![
            NOW,
            fixture.workspace.into_uuid().as_bytes(),
            fixture.leader_participation.as_bytes(),
        ],
    )?;
    let candidate_actor = fixture.candidate_actor;
    let receipt = request_as(&mut fixture, Uuid::from_u128(0x7600), NOW, candidate_actor)?;
    let snapshot = fixture
        .catalog
        .read_leadership_review(
            fixture.workspace,
            LeadershipReviewQuery::request(receipt.request_id()),
            NOW,
            None,
            64,
        )?
        .ok_or("review missing")?;
    assert_eq!(snapshot.electorate().len(), 1);
    assert_eq!(snapshot.electorate()[0].participant_id(), fixture.candidate);
    assert!(matches!(
        fixture
            .catalog
            .append_leadership_statement(LeadershipStatementCommand {
                statement_id: Uuid::from_u128(0x7900),
                request_id: receipt.request_id(),
                election_id: receipt.election_id(),
                workspace_id: fixture.workspace,
                actor: fixture.candidate_actor,
                kind: LeadershipStatementKind::Defense,
                body: "candidate defense".to_owned(),
                now_ms: NOW + 1,
            }),
        Err(OperationsError::Conflict("leadership defense incumbent"))
    ));
    assert!(matches!(
        fixture
            .catalog
            .append_leadership_statement(LeadershipStatementCommand {
                statement_id: Uuid::from_u128(0x7901),
                request_id: receipt.request_id(),
                election_id: receipt.election_id(),
                workspace_id: fixture.workspace,
                actor: OperationalActor::LocalOperator,
                kind: LeadershipStatementKind::Defense,
                body: "operator defense".to_owned(),
                now_ms: NOW + 1,
            }),
        Err(OperationsError::Conflict(
            "leadership statements require participant"
        ))
    ));
    let defense = fixture
        .catalog
        .append_leadership_statement(LeadershipStatementCommand {
            statement_id: Uuid::from_u128(0x7902),
            request_id: receipt.request_id(),
            election_id: receipt.election_id(),
            workspace_id: fixture.workspace,
            actor: fixture.leader_actor,
            kind: LeadershipStatementKind::Defense,
            body: "incumbent defense outside electorate".to_owned(),
            now_ms: NOW + 1,
        })?;
    assert_eq!(defense.phase(), LeadershipReviewPhase::Discussion);
    drop(fixture.catalog);
    std::fs::remove_dir_all(fixture.root)?;
    Ok(())
}

#[test]
fn review_cooldown_tracks_confirmed_leader_changes_only() -> Result<(), Box<dyn std::error::Error>>
{
    let mut elected = fixture(0x7700)?;
    insert_workspace_election_receipt(
        &elected,
        Uuid::from_u128(0x7710),
        elected.leader,
        elected.candidate,
        "ELECTED",
        NOW,
    )?;
    let unconfirmed = request(&mut elected, Uuid::from_u128(0x7720), NOW + 1)?;
    assert!(!unconfirmed.replay());
    drop(elected.catalog);
    std::fs::remove_dir_all(elected.root)?;

    let mut reelected = fixture(0x7800)?;
    insert_workspace_election_receipt(
        &reelected,
        Uuid::from_u128(0x7810),
        reelected.leader,
        reelected.leader,
        "CONFIRMED",
        NOW,
    )?;
    reelected.catalog.connection.execute(
        "UPDATE ops_leadership SET leadership_revision=2 WHERE workspace_id=?1 AND scope_kind=0",
        params![reelected.workspace.into_uuid().as_bytes()],
    )?;
    let reelected_actor = reelected.leader_actor;
    let same_leader = request_at_revision(
        &mut reelected,
        Uuid::from_u128(0x7820),
        NOW + 1,
        2,
        reelected_actor,
    )?;
    assert!(!same_leader.replay());
    drop(reelected.catalog);
    std::fs::remove_dir_all(reelected.root)?;

    let mut changed = fixture(0x7900)?;
    let cooldown = crate::leadership::LEADER_CHANGE_COOLDOWN_MS;
    insert_workspace_election_receipt(
        &changed,
        Uuid::from_u128(0x7910),
        changed.leader,
        changed.candidate,
        "CONFIRMED",
        NOW,
    )?;
    insert_election_candidate_subject(
        &changed,
        Uuid::from_u128(0x7910),
        changed.candidate,
        Uuid::from_u128(0x7900 + 0x231),
        Uuid::from_u128(0x7900 + 0x131),
    )?;
    insert_review_incumbent_identity_witness(&changed, 0x7900, Uuid::from_u128(0x7910))?;
    insert_workspace_election_receipt(
        &changed,
        Uuid::from_u128(0x7911),
        changed.candidate,
        changed.candidate,
        "CONFIRMED",
        NOW + 100,
    )?;
    insert_workspace_receipt(&changed, "CONFIRMED", None, changed.candidate, NOW + 200)?;
    changed.catalog.connection.execute(
        "UPDATE ops_leadership SET state='ACTIVE',leader_participant_id=?1,leadership_revision=4,stabilization_until_ms=NULL,current_election_id=NULL,updated_at_ms=?2 WHERE workspace_id=?3 AND scope_kind=0",
        params![
            changed.candidate.as_bytes(),
            NOW + 200,
            changed.workspace.into_uuid().as_bytes(),
        ],
    )?;
    let request_id = Uuid::from_u128(0x7920);
    let changed_actor = changed.leader_actor;
    assert!(matches!(
        request_at_revision(&mut changed, request_id, NOW + 201, 4, changed_actor,),
        Err(OperationsError::Conflict(
            "leadership re-election cooldown active"
        ))
    ));
    let at_expiry =
        request_at_revision(&mut changed, request_id, NOW + cooldown, 4, changed_actor)?;
    assert!(!at_expiry.replay());
    drop(changed.catalog);
    std::fs::remove_dir_all(changed.root)?;
    Ok(())
}

#[test]
fn reviewed_profile_alias_uses_frozen_incumbent_and_candidate_identities()
-> Result<(), Box<dyn std::error::Error>> {
    let seed = 0x8200;
    let mut fixture = fixture(seed)?;
    let alias = Uuid::from_u128(seed + 0x900);
    let profile = Uuid::from_u128(seed + 0x130);
    let election_id = Uuid::from_u128(seed + 0x910);
    insert_profile_session_alias(
        &fixture,
        profile,
        fixture.leader_participation,
        alias,
        Uuid::from_u128(seed + 0x901),
        Uuid::from_u128(seed + 0x902),
        NOW - 1,
    )?;
    insert_workspace_election_receipt(
        &fixture,
        election_id,
        fixture.leader,
        alias,
        "CONFIRMED",
        NOW,
    )?;
    insert_election_candidate_subject(
        &fixture,
        election_id,
        alias,
        fixture.leader_participation,
        profile,
    )?;
    // The incumbent requested the review, but the different participant won.
    // The immutable incumbent/candidate participation identities prove that
    // this was a Session alias rather than a leader change.
    insert_review_incumbent_identity_witness(&fixture, seed, election_id)?;

    let leader_actor = fixture.leader_actor;
    let review = request_at_revision(
        &mut fixture,
        Uuid::from_u128(seed + 0x920),
        NOW + 1,
        1,
        leader_actor,
    )?;
    assert!(!review.replay());
    drop(fixture.catalog);
    std::fs::remove_dir_all(fixture.root)?;
    Ok(())
}

#[test]
fn core_profile_alias_uses_frozen_incumbent_voter_subject() -> Result<(), Box<dyn std::error::Error>>
{
    let seed = 0x8300;
    let mut fixture = fixture(seed)?;
    let alias = Uuid::from_u128(seed + 0x900);
    let profile = Uuid::from_u128(seed + 0x130);
    let election_id = Uuid::from_u128(seed + 0x910);
    insert_profile_session_alias(
        &fixture,
        profile,
        fixture.leader_participation,
        alias,
        Uuid::from_u128(seed + 0x901),
        Uuid::from_u128(seed + 0x902),
        NOW - 1,
    )?;
    insert_workspace_election_receipt(
        &fixture,
        election_id,
        fixture.leader,
        alias,
        "CONFIRMED",
        NOW,
    )?;
    insert_election_candidate_subject(
        &fixture,
        election_id,
        alias,
        fixture.leader_participation,
        profile,
    )?;
    insert_election_incumbent_participation_source(&fixture, election_id, alias, profile)?;

    let leader_actor = fixture.leader_actor;
    let review = request_at_revision(
        &mut fixture,
        Uuid::from_u128(seed + 0x920),
        NOW + 1,
        1,
        leader_actor,
    )?;
    assert!(!review.replay());
    drop(fixture.catalog);
    std::fs::remove_dir_all(fixture.root)?;
    Ok(())
}

#[test]
fn confirmed_leader_change_survives_later_profile_mapping() -> Result<(), Box<dyn std::error::Error>>
{
    assert_late_mapping_preserves_cooldown(0x8400, NOW - 1)?;
    assert_late_mapping_preserves_cooldown(0x8500, NOW)
}

#[test]
fn journal_first_replay_survives_removed_binding_and_closed_election()
-> Result<(), Box<dyn std::error::Error>> {
    removed_binding_rejects_new_statement_but_allows_review_replay()?;
    let statement_id = Uuid::from_u128(0x7C00);
    let mut fixture = fixture(0x7D00)?;
    let request_id = Uuid::from_u128(0x7E00);
    let receipt = request(&mut fixture, request_id, NOW)?;
    let statement = fixture
        .catalog
        .append_leadership_statement(LeadershipStatementCommand {
            statement_id,
            request_id,
            election_id: receipt.election_id(),
            workspace_id: fixture.workspace,
            actor: fixture.leader_actor,
            kind: LeadershipStatementKind::Defense,
            body: "stored before retirement".to_owned(),
            now_ms: NOW + 1,
        })?;
    assert_eq!(statement.sequence(), 1);
    fixture.catalog.connection.execute(
        "DELETE FROM ops_current_effect_bindings WHERE workspace_id=?1 AND participant_id=?2",
        params![
            fixture.workspace.into_uuid().as_bytes(),
            fixture.leader.as_bytes()
        ],
    )?;
    fixture.catalog.connection.execute(
        "UPDATE ops_leadership_elections SET state='CLOSED_NO_MAJORITY',tally_yes=0,tally_no=0,tally_abstain=0,closed_at_ms=?2 WHERE election_id=?1",
        params![receipt.election_id().as_bytes(), receipt.ballot_deadline_ms()],
    )?;
    fixture.catalog.connection.execute(
        "UPDATE ops_leadership SET state='ACTIVE',current_election_id=NULL,updated_at_ms=?2 WHERE workspace_id=?1 AND scope_kind=0",
        params![fixture.workspace.into_uuid().as_bytes(), receipt.ballot_deadline_ms()],
    )?;
    assert_closed_review_request_replay(
        &mut fixture,
        request_id,
        receipt.election_id(),
        receipt.ballot_deadline_ms() + 1,
    )?;
    let replay_command = LeadershipStatementCommand {
        statement_id,
        request_id,
        election_id: receipt.election_id(),
        workspace_id: fixture.workspace,
        actor: fixture.leader_actor,
        kind: LeadershipStatementKind::Defense,
        body: "stored before retirement".to_owned(),
        now_ms: receipt.ballot_deadline_ms() + 1,
    };
    assert!(
        fixture
            .catalog
            .leadership_statement_replay_exists(&replay_command)?
    );
    let replay = fixture
        .catalog
        .append_leadership_statement(replay_command)?;
    assert!(replay.replay());
    assert_eq!(replay.phase(), LeadershipReviewPhase::Resolved);
    let readback = exact_statement_readback(&fixture, request_id, statement_id)?;
    assert!(readback.statement().incumbent_final());
    drop(fixture.catalog);
    std::fs::remove_dir_all(fixture.root)?;
    Ok(())
}

fn assert_closed_review_request_replay(
    fixture: &mut Fixture,
    request_id: Uuid,
    election_id: Uuid,
    observed_at_ms: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let command = LeadershipReviewCommand {
        request_id,
        scope: LeadershipScope::workspace(fixture.workspace),
        expected_revision: 1,
        actor: fixture.leader_actor,
        candidate_participant_id: fixture.candidate,
        reason: "review reason".to_owned(),
        now_ms: observed_at_ms,
    };
    let replay = fixture.catalog.request_leadership_review(command.clone())?;
    assert!(replay.replay());
    assert_eq!(replay.election_id(), election_id);
    assert_eq!(replay.phase(), LeadershipReviewPhase::Resolved);

    let mut changed_reason = command.clone();
    changed_reason.reason = "changed reason".to_owned();
    assert!(matches!(
        fixture.catalog.request_leadership_review(changed_reason),
        Err(OperationsError::Conflict(
            "leadership review replay changed"
        ))
    ));
    let mut changed_revision = command.clone();
    changed_revision.expected_revision = 2;
    assert!(matches!(
        fixture.catalog.request_leadership_review(changed_revision),
        Err(OperationsError::Conflict(
            "leadership review replay changed"
        ))
    ));

    let snapshot = fixture
        .catalog
        .read_leadership_review(
            fixture.workspace,
            LeadershipReviewQuery::request(request_id),
            observed_at_ms,
            None,
            1,
        )?
        .ok_or(OperationsError::Corrupt(
            "closed review request readback missing",
        ))?;
    assert_eq!(snapshot.request_id(), request_id);
    assert_eq!(snapshot.election_id(), election_id);
    assert_eq!(
        snapshot.scope(),
        LeadershipScope::workspace(fixture.workspace)
    );
    assert_eq!(snapshot.expected_leadership_revision(), 1);
    assert_eq!(snapshot.candidate_participant_id(), fixture.candidate);
    assert_eq!(snapshot.reason(), "review reason");
    assert_eq!(snapshot.phase(), LeadershipReviewPhase::Resolved);

    let proposer = snapshot.proposer();
    assert_eq!(proposer.participant_id(), Some(fixture.leader));
    assert_eq!(
        proposer
            .participation_id()
            .map(WorkspaceParticipationId::into_uuid),
        Some(fixture.leader_participation)
    );
    if let OperationalActor::Participant {
        activity_id,
        agent_id,
        session_id,
    } = fixture.leader_actor
    {
        assert_eq!(snapshot.proposer_activity_id(), Some(activity_id));
        assert_eq!(snapshot.proposer_agent_id(), Some(agent_id));
        assert_eq!(proposer.session_id(), Some(session_id));
        assert_eq!(
            proposer.profile_id(),
            Some(Uuid::from_u128(agent_id.into_uuid().as_u128() + 0x100))
        );
    } else {
        return Err(std::io::Error::other("fixture proposer must be a participant").into());
    }
    Ok(())
}

#[test]
fn review_read_and_replay_paths_reject_a_mismatched_election_scope()
-> Result<(), Box<dyn std::error::Error>> {
    let seed = 0x8A00;
    let mut fixture = fixture(seed)?;
    let request_id = Uuid::from_u128(seed + 0x10);
    let receipt = request(&mut fixture, request_id, NOW)?;
    let scope = LeadershipScope::workspace(fixture.workspace);
    assert!(
        fixture
            .catalog
            .read_leadership_review(
                fixture.workspace,
                LeadershipReviewQuery::request(request_id),
                NOW,
                None,
                1,
            )?
            .is_some()
    );

    fixture.catalog.connection.execute(
        "UPDATE ops_leadership_elections SET scope_kind=2,activity_id=NULL,team_id=?2 WHERE election_id=?1",
        params![
            receipt.election_id().as_bytes(),
            Uuid::from_u128(seed + 0x60).as_bytes()
        ],
    )?;

    assert!(matches!(
        fixture.catalog.read_leadership_review(
            fixture.workspace,
            LeadershipReviewQuery::request(request_id),
            NOW + 1,
            None,
            1,
        ),
        Err(OperationsError::Corrupt("leadership review election scope"))
    ));
    assert!(matches!(
        fixture.catalog.leadership_review_replay_candidate(
            fixture.workspace,
            scope,
            request_id,
            fixture.leader_actor,
        ),
        Err(OperationsError::Corrupt("leadership review election scope"))
    ));

    assert!(matches!(
        fixture
            .catalog
            .request_leadership_review(LeadershipReviewCommand {
                request_id,
                scope,
                expected_revision: 1,
                actor: fixture.leader_actor,
                candidate_participant_id: fixture.candidate,
                reason: "review reason".to_owned(),
                now_ms: NOW + 1,
            }),
        Err(OperationsError::Corrupt("leadership review election scope"))
    ));

    drop(fixture.catalog);
    std::fs::remove_dir_all(fixture.root)?;
    Ok(())
}

fn removed_binding_rejects_new_statement_but_allows_review_replay()
-> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = fixture(0x7A00)?;
    let request_id = Uuid::from_u128(0x7B00);
    let command = LeadershipReviewCommand {
        request_id,
        scope: LeadershipScope::workspace(fixture.workspace),
        expected_revision: 1,
        actor: fixture.leader_actor,
        candidate_participant_id: fixture.candidate,
        reason: "journal replay".to_owned(),
        now_ms: NOW,
    };
    let receipt = fixture.catalog.request_leadership_review(command.clone())?;
    fixture.catalog.connection.execute(
        "DELETE FROM ops_current_effect_bindings WHERE workspace_id=?1 AND participant_id=?2",
        params![
            fixture.workspace.into_uuid().as_bytes(),
            fixture.leader.as_bytes()
        ],
    )?;
    let replay = fixture.catalog.request_leadership_review(command)?;
    assert!(replay.replay());
    assert_eq!(replay.election_id(), receipt.election_id());

    let statement = fixture
        .catalog
        .append_leadership_statement(LeadershipStatementCommand {
            statement_id: Uuid::from_u128(0x7C00),
            request_id,
            election_id: receipt.election_id(),
            workspace_id: fixture.workspace,
            actor: fixture.leader_actor,
            kind: LeadershipStatementKind::Defense,
            body: "stored before retirement".to_owned(),
            now_ms: NOW + 1,
        });
    assert!(
        statement.is_err(),
        "the binding was removed before this statement"
    );
    drop(fixture.catalog);
    std::fs::remove_dir_all(fixture.root)?;
    Ok(())
}

#[test]
fn leadership_statement_readback_is_exact_and_fails_closed_on_context_mismatch()
-> Result<(), Box<dyn std::error::Error>> {
    let seed = 0x7F00;
    let mut fixture = fixture(seed)?;
    let request_id = Uuid::from_u128(seed + 1);
    let receipt = request(&mut fixture, request_id, NOW)?;
    let statement_id = Uuid::from_u128(seed + 2);
    let statement_receipt =
        fixture
            .catalog
            .append_leadership_statement(LeadershipStatementCommand {
                statement_id,
                request_id,
                election_id: receipt.election_id(),
                workspace_id: fixture.workspace,
                actor: fixture.leader_actor,
                kind: LeadershipStatementKind::Defense,
                body: "read the stored statement".to_owned(),
                now_ms: NOW + 1,
            })?;
    assert_eq!(statement_receipt.sequence(), 1);

    let readback = exact_statement_readback(&fixture, request_id, statement_id)?;
    assert_eq!(readback.statement().election_id(), receipt.election_id());
    assert_eq!(readback.statement().body(), "read the stored statement");

    assert!(
        fixture
            .catalog
            .leadership_statement(fixture.workspace, Uuid::from_u128(seed + 3))?
            .is_none()
    );

    let foreign_workspace = WorkspaceId::from_uuid(Uuid::from_u128(seed + 4));
    let foreign_migration = Uuid::from_u128(seed + 5);
    fixture.catalog.connection.execute(
        "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
        params![
            foreign_migration.as_bytes(),
            foreign_workspace.into_uuid().as_bytes(),
            Uuid::from_u128(seed).as_bytes(),
            [0_u8; 32].as_slice()
        ],
    )?;
    assert!(matches!(
        fixture
            .catalog
            .leadership_statement(foreign_workspace, statement_id),
        Err(OperationsError::Conflict("leadership statement Workspace"))
    ));

    let unrelated_election = Uuid::from_u128(seed + 6);
    insert_workspace_election_receipt(
        &fixture,
        unrelated_election,
        fixture.leader,
        fixture.candidate,
        "CONFIRMED",
        NOW + 2,
    )?;
    fixture.catalog.connection.execute(
        "UPDATE ops_leadership_review_statements SET election_id=?2 WHERE statement_id=?1",
        params![statement_id.as_bytes(), unrelated_election.as_bytes()],
    )?;
    assert!(matches!(
        fixture
            .catalog
            .leadership_statement(fixture.workspace, statement_id),
        Err(OperationsError::Corrupt(
            "leadership statement review context"
        ))
    ));

    drop(fixture.catalog);
    std::fs::remove_dir_all(fixture.root)?;
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn team_retirement_preserves_unresolved_confirm_intents_and_ballots()
-> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = fixture(0xA000)?;
    let team_id = Uuid::from_u128(0xA100);
    let election_id = Uuid::from_u128(0xA200);
    let pending_request = Uuid::from_u128(0xA300);
    let remote_request = Uuid::from_u128(0xA301);
    let owner_id = Uuid::from_u128(0xA400);
    let leader_participation = Uuid::from_u128(0xA230);
    let candidate_participation = Uuid::from_u128(0xA231);
    let scope = LeadershipScope::team(fixture.workspace, team_id);
    let ballot_at = NOW + 100;

    fixture.catalog.connection.execute(
        "INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_by_participation_id,created_by_profile_id,created_by_participant_id,created_by_session_id,created_at_ms,updated_at_ms,formed_at_ms) VALUES(?1,?2,'Review Team','retirement test','ACTIVE',1,'operator','Local operator',NULL,NULL,NULL,NULL,?3,?3,?3)",
        params![team_id.as_bytes(), fixture.workspace.into_uuid().as_bytes(), NOW],
    )?;
    fixture.catalog.connection.execute(
        "INSERT INTO ops_team_members(workspace_id,team_id,participation_id,state,revision,joined_at_ms,left_at_ms,role) VALUES(?1,?2,?3,'JOINED',1,?4,NULL,'verification'), (?1,?2,?5,'JOINED',1,?4,NULL,'verification')",
        params![
            fixture.workspace.into_uuid().as_bytes(),
            team_id.as_bytes(),
            leader_participation.as_bytes(),
            NOW,
            candidate_participation.as_bytes(),
        ],
    )?;
    fixture.catalog.connection.execute(
        "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,2,NULL,?2,'AUTHORITY_PENDING',?3,1,NULL,?4,?5)",
        params![
            fixture.workspace.into_uuid().as_bytes(),
            team_id.as_bytes(),
            fixture.leader.as_bytes(),
            election_id.as_bytes(),
            ballot_at,
        ],
    )?;
    fixture.catalog.connection.execute(
        "INSERT INTO ops_leadership_elections(election_id,workspace_id,scope_kind,activity_id,team_id,kind,state,incumbent_participant_id,candidate_participant_id,ballot_deadline_ms,elected_participant_id,tally_yes,tally_no,tally_abstain,created_at_ms,closed_at_ms) VALUES(?1,?2,2,NULL,?3,'RECALL','CLOSED_ELECTED',?4,?5,?6,?5,1,0,0,?7,?6)",
        params![
            election_id.as_bytes(),
            fixture.workspace.into_uuid().as_bytes(),
            team_id.as_bytes(),
            fixture.leader.as_bytes(),
            fixture.candidate.as_bytes(),
            ballot_at,
            NOW,
        ],
    )?;
    fixture.catalog.connection.execute(
        "INSERT INTO ops_election_electorate(election_id,participant_id) VALUES(?1,?2),(?1,?3)",
        params![
            election_id.as_bytes(),
            fixture.leader.as_bytes(),
            fixture.candidate.as_bytes(),
        ],
    )?;
    fixture.catalog.connection.execute(
        "INSERT INTO ops_ballots(election_id,voter_participant_id,choice,ballot_revision,cast_at_ms) VALUES(?1,?2,'YES',1,?3)",
        params![election_id.as_bytes(), fixture.leader.as_bytes(), ballot_at],
    )?;
    for (request_id, state, remote_revision) in [
        (pending_request, "pending", None),
        (remote_request, "remote_recorded", Some(1_i64)),
    ] {
        fixture.catalog.connection.execute(
            "INSERT INTO ops_leadership_confirm_intents(request_id,kind,workspace_id,scope_kind,activity_id,team_id,election_id,elected_participant_id,expected_local_revision,requested_fence_revision,owner_instance_id,remote_recorded_revision,confirmed_at_ms,state,created_at_ms,resolved_at_ms) VALUES(?1,'election_confirm',?2,2,NULL,?3,?4,?5,1,1,?6,?7,NULL,?8,?9,NULL)",
            params![
                request_id.as_bytes(),
                fixture.workspace.into_uuid().as_bytes(),
                team_id.as_bytes(),
                election_id.as_bytes(),
                fixture.candidate.as_bytes(),
                owner_id.as_bytes(),
                remote_revision,
                state,
                NOW,
            ],
        )?;
    }

    let _prepared =
        match fixture
            .catalog
            .prepare_leadership_confirm(PrepareLeadershipConfirmCommand {
                scope,
                election_id,
                expected_revision: 1,
                request_id: pending_request,
                now_ms: ballot_at + 1,
            })? {
            crate::PrepareLeadershipConfirmOutcome::Prepared(prepared) => prepared,
            crate::PrepareLeadershipConfirmOutcome::Resolved(_) => {
                return Err("live team intent unexpectedly resolved".into());
            }
        };
    assert!(matches!(
        fixture.catalog.retire_team(TeamRetireCommand {
            request_id: Uuid::from_u128(0xA500),
            workspace_id: fixture.workspace,
            team_id,
            expected_team_revision: 1,
            actor: OperationalActor::LocalOperator,
            observed_at: TimestampMillis::new(ballot_at + 2)?,
        }),
        Err(OperationsError::Conflict(
            "Team retirement requires resolved leadership intent"
        ))
    ));
    let team_state: (String, i64) = fixture.catalog.connection.query_row(
        "SELECT state,revision FROM ops_teams WHERE workspace_id=?1 AND team_id=?2",
        params![fixture.workspace.into_uuid().as_bytes(), team_id.as_bytes()],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(team_state, ("ACTIVE".to_owned(), 1));
    // A candidate leaving cannot erase an unknown remote outcome either.
    let _left = fixture.catalog.leave_team(crate::TeamLeaveCommand {
        request_id: Uuid::from_u128(0xA501),
        workspace_id: fixture.workspace,
        team_id,
        expected_team_revision: 1,
        actor: fixture.candidate_actor,
        observed_at: TimestampMillis::new(ballot_at + 3)?,
    })?;
    assert!(
        fixture
            .catalog
            .reconcile_ineligible_elected_candidate(scope, 1, ballot_at + 4)?
            .is_none()
    );
    let view = fixture
        .catalog
        .leadership_scope_view(scope, ballot_at + 4)?
        .ok_or("Team leadership missing")?;
    assert_eq!(view.state(), crate::LeadershipState::AuthorityPending);
    assert_eq!(view.leader_participant_id(), Some(fixture.leader));
    assert_eq!(
        view.current_election().ok_or("preserved election")?.state(),
        crate::ElectionState::ClosedElected
    );
    let ballot_count: i64 = fixture.catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_ballots WHERE election_id=?1",
        params![election_id.as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(ballot_count, 1);
    for (request_id, expected) in [
        (pending_request, LeadershipConfirmIntentState::Pending),
        (remote_request, LeadershipConfirmIntentState::RemoteRecorded),
    ] {
        let intent = fixture
            .catalog
            .leadership_confirm_intent(request_id)?
            .ok_or("preserved intent missing")?;
        assert_eq!(intent.state(), expected);
    }
    drop(fixture.catalog);
    std::fs::remove_dir_all(fixture.root)?;
    Ok(())
}

#[test]
fn departed_winner_cannot_activate_and_history_survives_pointer_clear()
-> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = fixture(0x8100)?;
    let scope = LeadershipScope::workspace(fixture.workspace);
    let receipt = request(&mut fixture, Uuid::from_u128(0x8200), NOW)?;
    for voter in [fixture.leader, fixture.candidate] {
        let _ballot = fixture.catalog.cast_ballot(crate::CastBallotCommand {
            election_id: receipt.election_id(),
            voter_participant_id: voter,
            choice: crate::BallotChoice::Yes,
            now_ms: receipt.discussion_deadline_ms(),
        })?;
    }
    let tally = fixture
        .catalog
        .close_election(crate::CloseElectionCommand {
            election_id: receipt.election_id(),
            now_ms: receipt.ballot_deadline_ms(),
        })?;
    assert_eq!(tally.state(), crate::ElectionState::ClosedElected);
    fixture.catalog.connection.execute(
        "DELETE FROM ops_current_effect_bindings WHERE workspace_id=?1 AND participant_id=?2",
        params![
            fixture.workspace.into_uuid().as_bytes(),
            fixture.candidate.as_bytes()
        ],
    )?;
    let revision = fixture
        .catalog
        .leadership_scope_view(scope, receipt.ballot_deadline_ms())?
        .ok_or("missing scope")?
        .leadership_revision();
    assert!(
        fixture
            .catalog
            .prepare_leadership_confirm(crate::PrepareLeadershipConfirmCommand {
                scope,
                election_id: receipt.election_id(),
                expected_revision: revision,
                request_id: Uuid::from_u128(0x8201),
                now_ms: receipt.ballot_deadline_ms() + 1,
            })
            .is_err()
    );
    let repaired = fixture
        .catalog
        .reconcile_ineligible_elected_candidate(scope, revision, receipt.ballot_deadline_ms() + 1)?
        .ok_or("missing reconciliation")?;
    assert_eq!(repaired.state(), crate::LeadershipState::Active);
    assert_eq!(repaired.leader_participant_id(), Some(fixture.leader));
    assert!(
        fixture
            .catalog
            .leadership_scope_view(scope, receipt.ballot_deadline_ms() + 1)?
            .ok_or("missing scope")?
            .current_election()
            .is_none()
    );
    let history = fixture.catalog.leadership_review_history(
        scope,
        None,
        1,
        receipt.ballot_deadline_ms() + 1,
    )?;
    assert_eq!(history.entries().len(), 1);
    assert_eq!(history.entries()[0].request_id(), receipt.request_id());
    assert_eq!(
        history.entries()[0].phase(),
        LeadershipReviewPhase::Resolved
    );
    let recorded = fixture
        .catalog
        .read_leadership_review(
            fixture.workspace,
            LeadershipReviewQuery::request(receipt.request_id()),
            receipt.ballot_deadline_ms() + 1,
            None,
            64,
        )?
        .ok_or("missing review history")?;
    assert_eq!(recorded.electorate().len(), 2);
    assert!(
        recorded
            .electorate()
            .iter()
            .all(|voter| voter.choice() == Some(crate::BallotChoice::Yes))
    );
    let foreign_scope = LeadershipScope::activity(
        fixture.workspace,
        ActivityId::from_uuid(Uuid::from_u128(0x8202)),
    );
    assert!(
        fixture
            .catalog
            .leadership_review_history(foreign_scope, Some(receipt.request_id()), 1, NOW)
            .is_err()
    );
    drop(fixture.catalog);
    std::fs::remove_dir_all(fixture.root)?;
    Ok(())
}

#[test]
fn failover_origin_survives_incumbent_departure_during_the_review()
-> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = fixture(0x9100)?;
    let scope = LeadershipScope::workspace(fixture.workspace);
    let OperationalActor::Participant { session_id, .. } = fixture.leader_actor else {
        return Err("missing leader session".into());
    };
    fixture.catalog.connection.execute("INSERT INTO ops_logical_presence(workspace_id,participant_id,session_id,last_authenticated_at_ms,presence_revision) VALUES(?1,?2,?3,?4,2)",params![fixture.workspace.into_uuid().as_bytes(),fixture.leader.as_bytes(),session_id.into_uuid().as_bytes(),NOW])?;
    let failed_at = NOW + crate::LEADER_UNRESPONSIVE_AFTER_MS + 1;
    let failed = fixture
        .catalog
        .mark_failover_if_unresponsive(crate::MarkUnresponsiveFailoverCommand {
            scope,
            expected_revision: 1,
            now_ms: failed_at,
        })?
        .ok_or("missing failover")?;
    let requester = fixture.candidate_actor;
    let review = fixture
        .catalog
        .request_leadership_review(LeadershipReviewCommand {
            request_id: Uuid::from_u128(0x9200),
            scope,
            expected_revision: failed.leadership_revision(),
            actor: requester,
            candidate_participant_id: fixture.candidate,
            reason: "Leader stopped responding".to_owned(),
            now_ms: failed_at,
        })?;
    assert_eq!(review.phase(), LeadershipReviewPhase::Discussion);
    let OperationalActor::Participant { activity_id, .. } = fixture.leader_actor else {
        return Err("missing leader actor".into());
    };
    {
        let tx = fixture.catalog.connection.transaction()?;
        crate::leadership::reconcile_departing_leader(
            &tx,
            fixture.workspace,
            activity_id,
            fixture.leader,
            failed_at + 1,
        )?;
        tx.commit()?;
    }
    assert!(matches!(
        fixture.catalog.close_election(crate::CloseElectionCommand {
            election_id: review.election_id(),
            now_ms: review.ballot_deadline_ms(),
        }),
        Err(OperationsError::Conflict("leadership election closed"))
    ));
    let state: String = fixture.catalog.connection.query_row(
        "SELECT state FROM ops_leadership_elections WHERE election_id=?1",
        params![review.election_id().as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(state, "CANCELLED");
    let view = fixture
        .catalog
        .leadership_scope_view(scope, review.ballot_deadline_ms())?
        .ok_or("missing scope")?;
    assert_eq!(view.state(), crate::LeadershipState::FailoverRequired);
    assert_eq!(view.leader_participant_id(), None);
    drop(fixture.catalog);
    std::fs::remove_dir_all(fixture.root)?;
    Ok(())
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "Keeps role loss, later admissions and frozen history in one regression."
)]
fn workspace_election_cancels_only_after_a_voter_loses_its_last_team_leadership()
-> Result<(), Box<dyn std::error::Error>> {
    let seed = 0xE000;
    let mut fixture = fixture(seed)?;
    let extra_seed = seed + 0x500;
    seed_workspace_teams(
        &fixture.catalog.connection,
        fixture.workspace,
        extra_seed,
        fixture.leader,
        fixture.candidate,
        fixture.leader_participation,
        Uuid::from_u128(seed + 0x231),
    )?;
    let review = request(&mut fixture, Uuid::from_u128(seed + 0x1000), NOW)?;
    let _defense = fixture
        .catalog
        .append_leadership_statement(LeadershipStatementCommand {
            statement_id: Uuid::from_u128(seed + 0x1001),
            request_id: review.request_id(),
            election_id: review.election_id(),
            workspace_id: fixture.workspace,
            actor: fixture.leader_actor,
            kind: LeadershipStatementKind::Defense,
            body: "Continue the active work while reviewing leadership".to_owned(),
            now_ms: NOW + 1,
        })?;
    let ballot_at = NOW + 60_000;
    let _ballot = fixture.catalog.cast_ballot(crate::CastBallotCommand {
        election_id: review.election_id(),
        voter_participant_id: fixture.leader,
        choice: crate::BallotChoice::Yes,
        now_ms: ballot_at,
    })?;
    let first_leave = fixture.catalog.leave_team(crate::TeamLeaveCommand {
        request_id: Uuid::from_u128(seed + 0x1002),
        workspace_id: fixture.workspace,
        team_id: Uuid::from_u128(seed + 0x60),
        expected_team_revision: 1,
        actor: fixture.leader_actor,
        observed_at: TimestampMillis::new(ballot_at + 1)?,
    })?;
    assert_eq!(
        first_leave.membership().ok_or("first membership")?.state(),
        crate::TeamMembershipState::Left
    );
    let scope = LeadershipScope::workspace(fixture.workspace);
    let still_open = fixture
        .catalog
        .leadership_scope_view(scope, ballot_at + 1)?
        .ok_or("Workspace leadership")?;
    assert_eq!(still_open.state(), crate::LeadershipState::Electing);
    assert_eq!(
        still_open
            .current_election()
            .ok_or("open election")?
            .electorate_size(),
        2
    );
    // Later admission must not turn a bounded incumbent check into a new
    // full-Workspace electorate snapshot or change the frozen denominator.
    let OperationalActor::Participant { activity_id, .. } = fixture.leader_actor else {
        return Err("missing leader Activity".into());
    };
    for offset in 0..65_u128 {
        let _actor = insert_participant(
            &mut fixture.catalog,
            fixture.workspace,
            activity_id,
            Uuid::from_u128(seed + 0x2000 + offset),
            seed + 0x3000 + offset,
            seed + 0x4000 + offset,
            "non-voting member",
        )?;
    }
    let _last_leave = fixture.catalog.leave_team(crate::TeamLeaveCommand {
        request_id: Uuid::from_u128(seed + 0x1003),
        workspace_id: fixture.workspace,
        team_id: Uuid::from_u128(extra_seed + 0x60),
        expected_team_revision: 1,
        actor: fixture.leader_actor,
        observed_at: TimestampMillis::new(ballot_at + 2)?,
    })?;
    let restored = fixture
        .catalog
        .leadership_scope_view(scope, ballot_at + 2)?
        .ok_or("restored Workspace")?;
    assert_eq!(restored.state(), crate::LeadershipState::Active);
    assert_eq!(restored.leader_participant_id(), Some(fixture.leader));
    assert!(restored.current_election().is_none());
    let state: String = fixture.catalog.connection.query_row(
        "SELECT state FROM ops_leadership_elections WHERE election_id=?1",
        params![review.election_id().as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(state, "CANCELLED");
    assert_eq!(
        crate::election_subject::cast_tallies(&fixture.catalog.connection, review.election_id())?,
        (1, 0, 0)
    );
    assert_eq!(
        crate::election_subject::electorate_size(
            &fixture.catalog.connection,
            review.election_id()
        )?,
        2
    );
    let history_count: i64 = fixture.catalog.connection.query_row(
        "SELECT COUNT(*) FROM ops_leadership_review_statements WHERE election_id=?1",
        params![review.election_id().as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(history_count, 1);
    drop(fixture.catalog);
    std::fs::remove_dir_all(fixture.root)?;
    Ok(())
}

#[test]
fn preparing_team_confirmation_cancels_an_open_workspace_vote_before_remote_effect()
-> Result<(), Box<dyn std::error::Error>> {
    let seed = 0xF000;
    let mut fixture = fixture(seed)?;
    let review = request(&mut fixture, Uuid::from_u128(seed + 0x1000), NOW)?;
    let team_id = Uuid::from_u128(seed + 0x60);
    let team_election = Uuid::from_u128(seed + 0x1001);
    fixture.catalog.connection.execute(
        "INSERT INTO ops_leadership_elections(election_id,workspace_id,scope_kind,team_id,kind,state,incumbent_participant_id,candidate_participant_id,ballot_deadline_ms,elected_participant_id,tally_yes,tally_no,tally_abstain,created_at_ms,closed_at_ms) VALUES(?1,?2,2,?3,'RECALL','CLOSED_ELECTED',?4,?5,?6,?5,2,0,0,?7,?6)",
        params![team_election.as_bytes(),fixture.workspace.into_uuid().as_bytes(),team_id.as_bytes(),fixture.leader.as_bytes(),fixture.candidate.as_bytes(),NOW+1,NOW],
    )?;
    fixture.catalog.connection.execute(
        "UPDATE ops_leadership SET state='AUTHORITY_PENDING',leadership_revision=3,current_election_id=?1 WHERE workspace_id=?2 AND scope_kind=2 AND team_id=?3",
        params![team_election.as_bytes(),fixture.workspace.into_uuid().as_bytes(),team_id.as_bytes()],
    )?;
    let before = fixture
        .catalog
        .leadership_scope_view(LeadershipScope::workspace(fixture.workspace), NOW + 1)?
        .ok_or("Workspace missing")?;
    assert_eq!(before.state(), crate::LeadershipState::Electing);
    let outcome = fixture
        .catalog
        .prepare_leadership_confirm(PrepareLeadershipConfirmCommand {
            scope: LeadershipScope::team(fixture.workspace, team_id),
            election_id: team_election,
            expected_revision: 3,
            request_id: Uuid::from_u128(seed + 0x1002),
            now_ms: NOW + 2,
        })?;
    let crate::PrepareLeadershipConfirmOutcome::Prepared(prepared) = outcome else {
        return Err("Team confirmation was not prepared".into());
    };
    let state: String = fixture.catalog.connection.query_row(
        "SELECT state FROM ops_leadership_elections WHERE election_id=?1",
        params![review.election_id().as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(state, "CANCELLED");
    assert!(matches!(
        fixture.catalog.close_election(crate::CloseElectionCommand {
            election_id: review.election_id(),
            now_ms: review.ballot_deadline_ms(),
        }),
        Err(OperationsError::Conflict("leadership election closed"))
    ));
    let excluded = fixture.catalog.exclude_leadership_confirm(
        &prepared,
        crate::LeadershipFenceExclusionProof::stale_for(prepared.fence_request()),
        NOW + 3,
    )?;
    assert_eq!(
        excluded.intent_state(),
        LeadershipConfirmIntentState::Superseded
    );
    let restored = fixture
        .catalog
        .leadership_scope_view(LeadershipScope::workspace(fixture.workspace), NOW + 3)?
        .ok_or("Workspace missing")?;
    assert_eq!(restored.state(), crate::LeadershipState::Active);
    assert!(restored.current_election().is_none());
    drop(fixture.catalog);
    std::fs::remove_dir_all(fixture.root)?;
    Ok(())
}
