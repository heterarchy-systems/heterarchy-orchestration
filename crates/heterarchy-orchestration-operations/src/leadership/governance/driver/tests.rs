use crate::{ElectionState, LeadershipScope, LeadershipState, OperationsCatalog};
use forge_domain::{ActivityId, WorkspaceId};
use rusqlite::params;
use std::path::PathBuf;
use uuid::Uuid;

const NOW: i64 = 20_000_000;

struct Fixture {
    root: PathBuf,
    catalog: OperationsCatalog,
    workspace: WorkspaceId,
    leader: Uuid,
    candidate: Uuid,
    leader_participation: Uuid,
    leader_profile: Uuid,
    leader_session: Uuid,
}

fn fixture(seed: u128) -> Result<Fixture, Box<dyn std::error::Error>> {
    let root = std::env::temp_dir().join(format!("leadership-driver-{seed:x}-{}", Uuid::now_v7()));
    std::fs::create_dir(&root)?;
    let destination = Uuid::from_u128(seed);
    let migration = Uuid::from_u128(seed + 1);
    let workspace = WorkspaceId::from_uuid(Uuid::from_u128(seed + 2));
    let leader = Uuid::from_u128(seed + 10);
    let candidate = Uuid::from_u128(seed + 11);
    let activity = Uuid::from_u128(seed + 12);
    let leader_session = Uuid::from_u128(seed + 20);
    let candidate_session = Uuid::from_u128(seed + 21);
    let leader_profile = Uuid::from_u128(seed + 30);
    let candidate_profile = Uuid::from_u128(seed + 31);
    let leader_participation = Uuid::from_u128(seed + 40);
    let candidate_participation = Uuid::from_u128(seed + 41);
    let leader_agent = Uuid::from_u128(seed + 50);
    let candidate_agent = Uuid::from_u128(seed + 51);
    let catalog = OperationsCatalog::open(&root.join("catalog.sqlite3"), destination)?;
    catalog.connection.execute(
        "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
        params![
            migration.as_bytes(),
            workspace.into_uuid().as_bytes(),
            destination.as_bytes(),
            [0_u8; 32].as_slice()
        ],
    )?;
    for (participant, session, profile, participation, agent, name) in [
        (
            leader,
            leader_session,
            leader_profile,
            leader_participation,
            leader_agent,
            "leader",
        ),
        (
            candidate,
            candidate_session,
            candidate_profile,
            candidate_participation,
            candidate_agent,
            "candidate",
        ),
    ] {
        catalog.connection.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,?4,'worker',1,1,?5)",
            params![workspace.into_uuid().as_bytes(), participant.as_bytes(), session.as_bytes(), name, NOW],
        )?;
        catalog.connection.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,?4,1)",
            params![workspace.into_uuid().as_bytes(), profile.as_bytes(), participation.as_bytes(), NOW],
        )?;
        catalog.connection.execute(
            "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,?8)",
            params![
                Uuid::from_u128(agent.as_u128() + 0x100).as_bytes(),
                workspace.into_uuid().as_bytes(),
                participation.as_bytes(),
                profile.as_bytes(),
                Uuid::from_u128(agent.as_u128() + 0x101).as_bytes(),
                session.as_bytes(),
                participant.as_bytes(),
                NOW,
            ],
        )?;
        catalog.connection.execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
            params![
                workspace.into_uuid().as_bytes(),
                participant.as_bytes(),
                activity.as_bytes(),
                session.as_bytes(),
                agent.as_bytes(),
                Uuid::from_u128(agent.as_u128() + 0x102).as_bytes(),
            ],
        )?;
    }
    Ok(Fixture {
        root,
        catalog,
        workspace,
        leader,
        candidate,
        leader_participation,
        leader_profile,
        leader_session,
    })
}

#[allow(clippy::too_many_arguments)]
fn insert_review(
    fixture: &Fixture,
    request_id: Uuid,
    election_id: Uuid,
    scope: LeadershipScope,
    election_state: ElectionState,
    leadership_state: LeadershipState,
    current_election: Option<Uuid>,
    created_at_ms: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let activity = scope
        .activity_id()
        .map(|id| id.into_uuid().as_bytes().to_vec());
    let team = scope.team_id().map(|id| id.as_bytes().to_vec());
    let elected =
        matches!(election_state, ElectionState::ClosedElected).then_some(fixture.candidate);
    fixture.catalog.connection.execute(
        "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,?2,?3,?4,?5,?6,1,?7,?8,?9)",
        params![
            scope.workspace_id().into_uuid().as_bytes(),
            scope.scope_kind(),
            activity,
            team,
            leadership_state.encode(),
            fixture.leader.as_bytes(),
            (leadership_state == LeadershipState::Stabilizing).then_some(created_at_ms + 1),
            current_election.map(|id| id.as_bytes().to_vec()),
            created_at_ms,
        ],
    )?;
    fixture.catalog.connection.execute(
        "INSERT INTO ops_leadership_elections(election_id,workspace_id,scope_kind,activity_id,team_id,kind,state,incumbent_participant_id,candidate_participant_id,ballot_deadline_ms,elected_participant_id,tally_yes,tally_no,tally_abstain,created_at_ms,closed_at_ms) VALUES(?1,?2,?3,?4,?5,'RECALL',?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
        params![
            election_id.as_bytes(),
            scope.workspace_id().into_uuid().as_bytes(),
            scope.scope_kind(),
            activity,
            team,
            election_state.encode(),
            fixture.leader.as_bytes(),
            fixture.candidate.as_bytes(),
            created_at_ms + 10_000,
            elected.map(|id| id.as_bytes().to_vec()),
            elected.map(|_| 2_i64),
            elected.map(|_| 0_i64),
            elected.map(|_| 0_i64),
            created_at_ms,
            elected.map(|_| created_at_ms + 10_000),
        ],
    )?;
    fixture.catalog.connection.execute(
        "INSERT INTO ops_election_electorate(election_id,participant_id) VALUES(?1,?2),(?1,?3)",
        params![
            election_id.as_bytes(),
            fixture.leader.as_bytes(),
            fixture.candidate.as_bytes()
        ],
    )?;
    fixture.catalog.connection.execute(
        "INSERT INTO ops_leadership_reviews(request_id,election_id,workspace_id,scope_kind,activity_id,team_id,expected_leadership_revision,proposer_kind,proposer_activity_id,proposer_agent_id,proposer_session_id,proposer_participation_id,proposer_profile_id,proposer_participant_id,proposer_display_name,incumbent_participant_id,incumbent_participation_id,incumbent_profile_id,incumbent_session_id,incumbent_display_name,reason,defense_deadline_ms,discussion_deadline_ms,ballot_deadline_ms,defense_closed_at_ms,incumbent_statement_id,created_at_ms) VALUES(?1,?2,?3,?4,?5,?6,1,0,NULL,NULL,NULL,NULL,NULL,NULL,'Local operator',?7,?8,?9,?10,'leader','driver review',?11,?12,?13,NULL,NULL,?14)",
        params![
            request_id.as_bytes(),
            election_id.as_bytes(),
            scope.workspace_id().into_uuid().as_bytes(),
            scope.scope_kind(),
            activity,
            team,
            fixture.leader.as_bytes(),
            fixture.leader_participation.as_bytes(),
            fixture.leader_profile.as_bytes(),
            fixture.leader_session.as_bytes(),
            created_at_ms + 1_000,
            created_at_ms + 2_000,
            created_at_ms + 3_000,
            created_at_ms,
        ],
    )?;
    Ok(())
}

fn insert_maintenance_scope(
    fixture: &Fixture,
    scope: LeadershipScope,
    state: &str,
    team_state: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(team_id) = scope.team_id() {
        fixture.catalog.connection.execute(
            "INSERT INTO ops_teams(team_id,workspace_id,name,purpose,state,revision,created_by_key,created_by_display_name,created_by_participation_id,created_by_profile_id,created_by_participant_id,created_by_session_id,created_at_ms,updated_at_ms) VALUES(?1,?2,'Maintenance Team','driver test',?3,1,'operator','Local operator',NULL,NULL,NULL,NULL,?4,?4)",
            params![
                team_id.as_bytes(),
                fixture.workspace.into_uuid().as_bytes(),
                team_state.unwrap_or("ACTIVE"),
                NOW,
            ],
        )?;
    }
    fixture.catalog.connection.execute(
        "INSERT INTO ops_leadership(workspace_id,scope_kind,activity_id,team_id,state,leader_participant_id,leadership_revision,stabilization_until_ms,current_election_id,updated_at_ms) VALUES(?1,?2,?3,?4,?5,?6,1,NULL,NULL,?7)",
        params![
            scope.workspace_id().into_uuid().as_bytes(),
            scope.scope_kind(),
            scope.activity_id().map(|id| id.into_uuid().as_bytes().to_vec()),
            scope.team_id().map(|id| id.as_bytes().to_vec()),
            state,
            fixture.leader.as_bytes(),
            NOW,
        ],
    )?;
    Ok(())
}

#[test]
fn maintenance_scopes_page_exact_native_targets_without_reviews()
-> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = fixture(0xE000)?;
    let activity_a = ActivityId::from_uuid(Uuid::from_u128(0xE010));
    let activity_b = ActivityId::from_uuid(Uuid::from_u128(0xE011));
    let active_team = Uuid::from_u128(0xE020);
    let retired_team = Uuid::from_u128(0xE021);
    insert_maintenance_scope(
        &fixture,
        LeadershipScope::workspace(fixture.workspace),
        "BOOTSTRAPPING",
        None,
    )?;
    insert_maintenance_scope(
        &fixture,
        LeadershipScope::activity(fixture.workspace, activity_a),
        "STABILIZING",
        None,
    )?;
    insert_maintenance_scope(
        &fixture,
        LeadershipScope::team(fixture.workspace, active_team),
        "ACTIVE",
        Some("ACTIVE"),
    )?;
    insert_maintenance_scope(
        &fixture,
        LeadershipScope::team(fixture.workspace, retired_team),
        "ACTIVE",
        Some("RETIRED"),
    )?;
    insert_maintenance_scope(
        &fixture,
        LeadershipScope::activity(fixture.workspace, activity_b),
        "ACTIVE",
        None,
    )?;

    let first = fixture.catalog.leadership_maintenance_scopes(None, 2)?;
    assert_eq!(first.len(), 2);
    assert_eq!(first[0].1, LeadershipScope::workspace(fixture.workspace));
    assert_eq!(
        first[1].1,
        LeadershipScope::activity(fixture.workspace, activity_a)
    );
    let second = fixture
        .catalog
        .leadership_maintenance_scopes(Some(first[1].0), 8)?;
    assert_eq!(second.len(), 2);
    assert_eq!(
        second[0].1,
        LeadershipScope::team(fixture.workspace, active_team)
    );
    assert_eq!(
        second[1].1,
        LeadershipScope::activity(fixture.workspace, activity_b)
    );
    assert!(
        fixture
            .catalog
            .leadership_maintenance_scopes(Some(second[1].0), 8)?
            .is_empty()
    );
    assert!(
        fixture
            .catalog
            .leadership_maintenance_scopes(None, 9)
            .is_err()
    );
    assert!(
        fixture
            .catalog
            .leadership_maintenance_scopes(Some(-1), 1)
            .is_err()
    );
    verify_recovery_scan(&mut fixture, activity_a, activity_b)?;
    drop(fixture.catalog);
    std::fs::remove_dir_all(fixture.root)?;
    Ok(())
}

fn verify_recovery_scan(
    fixture: &mut Fixture,
    activity_a: ActivityId,
    activity_b: ActivityId,
) -> Result<(), Box<dyn std::error::Error>> {
    fixture.catalog.connection.execute(
        "UPDATE ops_leadership SET state='RECOVERY_REQUIRED',stabilization_until_ms=NULL WHERE scope_kind IN(1,2)", [],
    )?;
    let page = fixture.catalog.leadership_recovery_scopes(None, 1)?;
    assert_eq!(page.len(), 1);
    assert_eq!(
        page[0].1,
        LeadershipScope::activity(fixture.workspace, activity_a)
    );
    let rest = fixture
        .catalog
        .leadership_recovery_scopes(Some(page[0].0), 8)?;
    assert_eq!(rest.len(), 2); // Active Team and second Activity; retired Team excluded.
    assert_eq!(
        rest[1].1,
        LeadershipScope::activity(fixture.workspace, activity_b)
    );
    fixture
        .catalog
        .recover_terminal_leadership(page[0].1, NOW + 1)?;
    assert_eq!(fixture.catalog.leadership_recovery_scopes(None, 8)?, rest);
    assert!(fixture.catalog.leadership_recovery_scopes(None, 9).is_err());
    assert!(
        fixture
            .catalog
            .leadership_recovery_scopes(Some(-1), 1)
            .is_err()
    );
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn active_reviews_are_bounded_cursor_sorted_and_scope_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = fixture(0xC000)?;
    let activity = ActivityId::from_uuid(Uuid::from_u128(0xC012));
    let team = Uuid::from_u128(0xC013);
    let active_workspace = Uuid::from_u128(0xC100);
    let active_activity = Uuid::from_u128(0xC101);
    let active_team = Uuid::from_u128(0xC102);
    let canceled = Uuid::from_u128(0xC103);
    let retired = Uuid::from_u128(0xC104);
    let confirmed = Uuid::from_u128(0xC105);
    insert_review(
        &fixture,
        active_workspace,
        Uuid::from_u128(0xD100),
        LeadershipScope::workspace(fixture.workspace),
        ElectionState::Electing,
        LeadershipState::Electing,
        Some(Uuid::from_u128(0xD100)),
        NOW,
    )?;
    insert_review(
        &fixture,
        active_activity,
        Uuid::from_u128(0xD101),
        LeadershipScope::activity(fixture.workspace, activity),
        ElectionState::Electing,
        LeadershipState::Electing,
        Some(Uuid::from_u128(0xD101)),
        NOW + 1,
    )?;
    insert_review(
        &fixture,
        active_team,
        Uuid::from_u128(0xD102),
        LeadershipScope::team(fixture.workspace, team),
        ElectionState::ClosedElected,
        LeadershipState::AuthorityPending,
        Some(Uuid::from_u128(0xD102)),
        NOW + 2,
    )?;
    insert_review(
        &fixture,
        canceled,
        Uuid::from_u128(0xD103),
        LeadershipScope::activity(
            fixture.workspace,
            ActivityId::from_uuid(Uuid::from_u128(0xC014)),
        ),
        ElectionState::Cancelled,
        LeadershipState::Vacant,
        None,
        NOW + 3,
    )?;
    insert_review(
        &fixture,
        retired,
        Uuid::from_u128(0xD104),
        LeadershipScope::team(fixture.workspace, Uuid::from_u128(0xC015)),
        ElectionState::ClosedElected,
        LeadershipState::Vacant,
        None,
        NOW + 4,
    )?;
    insert_review(
        &fixture,
        confirmed,
        Uuid::from_u128(0xD105),
        LeadershipScope::activity(
            fixture.workspace,
            ActivityId::from_uuid(Uuid::from_u128(0xC016)),
        ),
        ElectionState::ClosedElected,
        LeadershipState::Stabilizing,
        None,
        NOW + 5,
    )?;
    let first = fixture.catalog.active_leadership_reviews(None, 2, NOW)?;
    assert_eq!(first.len(), 2);
    assert_eq!(first[0].request_id(), active_workspace);
    assert_eq!(first[1].request_id(), active_activity);
    assert_eq!(first[0].scope().team_id(), None);
    let second = fixture
        .catalog
        .active_leadership_reviews(Some(first[1].request_id()), 8, NOW)?;
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].request_id(), active_team);
    assert_eq!(second[0].scope().team_id(), Some(team));
    let returned = first
        .iter()
        .chain(second.iter())
        .map(crate::LeadershipReviewSnapshot::request_id)
        .collect::<Vec<_>>();
    assert!(!returned.contains(&canceled));
    assert!(!returned.contains(&retired));
    assert!(!returned.contains(&confirmed));
    assert!(
        fixture
            .catalog
            .active_leadership_reviews(Some(active_team), 8, NOW)?
            .is_empty()
    );
    assert!(
        fixture
            .catalog
            .active_leadership_reviews(None, 9, NOW)
            .is_err()
    );
    drop(fixture.catalog);
    std::fs::remove_dir_all(fixture.root)?;
    Ok(())
}
