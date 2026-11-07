//! Tests for the staged proposal's bounded source record.
//!
//! The SQLite source rows below are synthetic contract fixtures. They verify
//! the staging table boundary, not live Forge Coordinator evidence or Backend
//! authorization integration.

use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, WorkspaceId};
use rusqlite::{Connection, params};
use uuid::Uuid;

use crate::{
    AgentRequestEffectScope, AgentRequestIndependence, OperationsError, TeamExpansionSlotRef,
    WorkspaceParticipationId, operational_actor::OperationalActor,
};

use super::{
    CoordinatorAgentRequestProposal, CoordinatorStagedProposalCommand, ProposalActorSnapshot,
    ProposalApprovalSource, proposal_digest, schema, staged_identity_exists_globally,
};

fn id(byte: u8) -> Uuid {
    Uuid::from_bytes([byte; 16])
}

fn sample_command() -> Result<CoordinatorStagedProposalCommand, OperationsError> {
    let activity_id = ActivityId::from_uuid(id(1));
    let agent_id = AgentInstanceId::from_uuid(id(2));
    let session_id = AgentSessionId::from_uuid(id(3));
    let request = CoordinatorAgentRequestProposal::new(
        id(4),
        activity_id,
        agent_id,
        session_id,
        7,
        "Coordinate a single Team member request".to_owned(),
        "rust".to_owned(),
        AgentRequestEffectScope::ReadOnly,
        AgentRequestIndependence::None,
        "fill one approved slot".to_owned(),
    )?;
    Ok(CoordinatorStagedProposalCommand::new(
        id(5),
        WorkspaceId::from_uuid(id(6)),
        id(7),
        TeamExpansionSlotRef::new(id(8), 0),
        9,
        10,
        11,
        12,
        OperationalActor::Participant {
            activity_id,
            agent_id,
            session_id,
        },
        request,
    ))
}

#[test]
fn proposal_digest_binds_request_identity_scope_reason_and_source_fences() {
    let command = sample_command().expect("valid bounded request");
    let actor = ProposalActorSnapshot {
        profile_id: id(13),
        participation_id: WorkspaceParticipationId::from_uuid(id(14))
            .expect("non-nil participation id"),
        participant_id: id(15),
    };
    let approval = ProposalApprovalSource {
        request_id: id(16),
        expected_team_revision: 1,
        capacity_revision: 1,
    };
    let base = proposal_digest(&command, approval, actor).expect("digest");

    let mut changed_reason = command.clone();
    changed_reason.request.reason.push('!');
    assert_ne!(
        base,
        proposal_digest(&changed_reason, approval, actor).expect("changed digest")
    );

    let mut changed_fence = command.clone();
    changed_fence.expected_workspace_participation_revision += 1;
    assert_ne!(
        base,
        proposal_digest(&changed_fence, approval, actor).expect("changed digest")
    );

    assert_ne!(
        base,
        proposal_digest(
            &command,
            ProposalApprovalSource {
                request_id: id(17),
                ..approval
            },
            actor,
        )
        .expect("changed approval source digest")
    );
}

#[test]
fn synthetic_schema_fixture_keeps_the_approved_hold_and_only_records_proposal() {
    let connection = synthetic_source(2);
    let proposal_id = id(30);
    let request_id = id(31);
    insert_synthetic_proposal(&connection, proposal_id, request_id)
        .expect("approved unassigned NewProfile slot accepts a proposal");

    let status: String = connection
        .query_row(
            "SELECT status FROM ops_team_expansion_staged_agent_proposals WHERE proposal_id=?1",
            params![proposal_id.as_bytes()],
            |row| row.get(0),
        )
        .expect("read staged status");
    assert_eq!(status, "COORDINATOR_PROPOSED_UNVERIFIED");
    let revisions: (i64, i64, i64, i64) = connection
        .query_row(
            "SELECT approval_expected_team_revision,expected_team_revision,
                    approval_capacity_revision,capacity_revision
             FROM ops_team_expansion_staged_agent_proposals WHERE proposal_id=?1",
            params![proposal_id.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("read approval and current revision fences");
    assert_eq!(revisions, (1, 9, 1, 10));

    let canonical_requests: i64 = connection
        .query_row("SELECT COUNT(*) FROM ops_agent_requests", [], |row| {
            row.get(0)
        })
        .expect("read canonical request count");
    assert_eq!(canonical_requests, 0);
    for table in [
        "ops_agent_request_resolutions",
        "ops_agent_request_fulfillments",
        "ops_agent_provision_operations",
        "ops_workspace_participations",
        "ops_team_members",
    ] {
        assert!(
            !table_exists(&connection, table),
            "unexpected effect table {table}"
        );
    }

    for table in [
        "ops_team_expansion_decisions",
        "ops_team_expansion_workspace_hold_manifests",
        "ops_team_expansion_workspace_hold_plans",
        "ops_team_expansion_slots",
    ] {
        let rows: i64 = connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .expect("read source row count");
        assert_eq!(rows, 1, "staging changed approved source table {table}");
    }
    for table in [
        "ops_team_expansion_slot_assignments",
        "ops_team_expansion_slot_uses",
        "ops_team_expansion_workspace_hold_bindings",
    ] {
        let rows: i64 = connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .expect("read effect row count");
        assert_eq!(rows, 0, "staging changed reserved slot table {table}");
    }

    assert!(connection.execute(
        "UPDATE ops_team_expansion_staged_agent_proposals SET reason='changed' WHERE proposal_id=?1",
        params![proposal_id.as_bytes()],
    ).is_err());
    assert!(
        connection
            .execute(
                "DELETE FROM ops_team_expansion_staged_agent_proposals WHERE proposal_id=?1",
                params![proposal_id.as_bytes()],
            )
            .is_err()
    );
}

#[test]
fn synthetic_schema_rejects_an_existing_profile_target_for_new_profile_proposal() {
    let connection = synthetic_source(1);
    assert!(insert_synthetic_proposal(&connection, id(40), id(41)).is_err());
}

#[test]
fn synthetic_global_id_conflicts_share_one_boolean_projection() {
    let empty = synthetic_source(2);
    assert!(!staged_identity_exists_globally(&empty, id(50), id(51)).expect("empty identity read"));

    let proposal_collision = synthetic_source(2);
    insert_synthetic_proposal(&proposal_collision, id(52), id(53))
        .expect("synthetic proposal source");
    assert!(
        staged_identity_exists_globally(&proposal_collision, id(52), id(54))
            .expect("proposal collision read")
    );

    let staged_request_collision = synthetic_source(2);
    insert_synthetic_proposal(&staged_request_collision, id(55), id(56))
        .expect("synthetic proposal source");
    assert!(
        staged_identity_exists_globally(&staged_request_collision, id(57), id(56))
            .expect("staged request collision read")
    );

    let canonical_request_collision = synthetic_source(2);
    canonical_request_collision
        .execute(
            "INSERT INTO ops_agent_requests(request_id) VALUES(?1)",
            params![id(58).as_bytes()],
        )
        .expect("synthetic canonical AgentRequest id");
    assert!(
        staged_identity_exists_globally(&canonical_request_collision, id(59), id(58))
            .expect("canonical request collision read")
    );
}

fn synthetic_source(target_kind: i64) -> Connection {
    let mut connection = Connection::open_in_memory().expect("in-memory fixture");
    connection
        .execute_batch(
            "CREATE TABLE ops_team_expansion_decisions(
            workspace_id BLOB NOT NULL, team_id BLOB NOT NULL, decision_id BLOB NOT NULL,
            request_id BLOB NOT NULL, state TEXT NOT NULL, approved_count INTEGER NOT NULL,
            expected_team_revision INTEGER NOT NULL, capacity_revision INTEGER NOT NULL,
            decided_at_ms INTEGER NOT NULL,
            UNIQUE(workspace_id,team_id,decision_id,request_id));
         CREATE TABLE ops_team_expansion_workspace_hold_manifests(
            workspace_id BLOB NOT NULL, team_id BLOB NOT NULL, decision_id BLOB NOT NULL,
            request_id BLOB NOT NULL, slot_count INTEGER NOT NULL,
            UNIQUE(workspace_id,team_id,decision_id));
         CREATE TABLE ops_team_expansion_slots(
            workspace_id BLOB NOT NULL, team_id BLOB NOT NULL, decision_id BLOB NOT NULL,
            ordinal INTEGER NOT NULL, request_id BLOB NOT NULL,
            UNIQUE(workspace_id,team_id,decision_id,ordinal));
         CREATE TABLE ops_team_expansion_workspace_hold_plans(
            workspace_id BLOB NOT NULL, team_id BLOB NOT NULL, decision_id BLOB NOT NULL,
            ordinal INTEGER NOT NULL, target_kind INTEGER NOT NULL,
            target_profile_id BLOB, target_participation_id BLOB,
            UNIQUE(workspace_id,team_id,decision_id,ordinal));
         CREATE TABLE ops_team_expansion_cooldown_anchors(
            workspace_id BLOB NOT NULL, team_id BLOB NOT NULL, decision_id BLOB NOT NULL);
         CREATE TABLE ops_team_expansion_slot_assignments(
            workspace_id BLOB NOT NULL, team_id BLOB NOT NULL, decision_id BLOB NOT NULL,
            ordinal INTEGER NOT NULL, request_id BLOB);
         CREATE TABLE ops_team_expansion_slot_uses(
            workspace_id BLOB NOT NULL, team_id BLOB NOT NULL, decision_id BLOB NOT NULL,
            ordinal INTEGER NOT NULL, join_request_id BLOB);
         CREATE TABLE ops_team_expansion_workspace_hold_bindings(
            workspace_id BLOB NOT NULL, decision_id BLOB NOT NULL, ordinal INTEGER NOT NULL,
            binding_request_id BLOB);
         CREATE TABLE ops_agent_requests(request_id BLOB PRIMARY KEY);",
        )
        .expect("synthetic source tables");

    {
        let transaction = connection.transaction().expect("schema transaction");
        schema::create_schema(&transaction).expect("staged proposal schema");
        transaction.commit().expect("schema commit");
    }

    let workspace = id(1);
    let team = id(2);
    let decision = id(3);
    let approval_request = id(4);
    connection
        .execute(
            "INSERT INTO ops_team_expansion_decisions VALUES(?1,?2,?3,?4,'APPROVED',1,1,1,100)",
            params![
                workspace.as_bytes(),
                team.as_bytes(),
                decision.as_bytes(),
                approval_request.as_bytes()
            ],
        )
        .expect("synthetic approved decision");
    connection
        .execute(
            "INSERT INTO ops_team_expansion_workspace_hold_manifests VALUES(?1,?2,?3,?4,1)",
            params![
                workspace.as_bytes(),
                team.as_bytes(),
                decision.as_bytes(),
                approval_request.as_bytes()
            ],
        )
        .expect("synthetic hold manifest");
    connection
        .execute(
            "INSERT INTO ops_team_expansion_slots VALUES(?1,?2,?3,0,?4)",
            params![
                workspace.as_bytes(),
                team.as_bytes(),
                decision.as_bytes(),
                approval_request.as_bytes()
            ],
        )
        .expect("synthetic reserved slot");
    connection
        .execute(
            "INSERT INTO ops_team_expansion_workspace_hold_plans VALUES(?1,?2,?3,0,?4,NULL,NULL)",
            params![
                workspace.as_bytes(),
                team.as_bytes(),
                decision.as_bytes(),
                target_kind
            ],
        )
        .expect("synthetic target plan");
    connection
        .execute(
            "INSERT INTO ops_team_expansion_cooldown_anchors VALUES(?1,?2,?3)",
            params![workspace.as_bytes(), team.as_bytes(), decision.as_bytes()],
        )
        .expect("synthetic approval anchor");
    connection
}

fn insert_synthetic_proposal(
    connection: &Connection,
    proposal_id: Uuid,
    request_id: Uuid,
) -> rusqlite::Result<usize> {
    connection.execute(
        "INSERT INTO ops_team_expansion_staged_agent_proposals(
            proposal_id,request_id,workspace_id,team_id,decision_id,approval_request_id,
            approval_expected_team_revision,approval_capacity_revision,ordinal,status,
            expected_team_revision,capacity_revision,orchestrator_revision,workspace_participation_revision,
            activity_id,agent_id,session_id,coordinator_revision,requested_count,
            objective,capability,effect_scope,independence_requirement,reason,
            actor_profile_id,actor_participation_id,actor_participant_id,proposal_digest,recorded_at_ms)
         VALUES(?1,?2,?3,?4,?5,?6,1,1,0,'COORDINATOR_PROPOSED_UNVERIFIED',9,10,11,12,
                ?7,?8,?9,7,1,'objective','rust',0,0,'reason',?10,?11,?12,?13,100)",
        params![
            proposal_id.as_bytes(), request_id.as_bytes(), id(1).as_bytes(), id(2).as_bytes(),
            id(3).as_bytes(), id(4).as_bytes(), id(5).as_bytes(), id(6).as_bytes(),
            id(7).as_bytes(), id(8).as_bytes(), id(9).as_bytes(), id(10).as_bytes(),
            [0xAB_u8; 32].as_slice(),
        ],
    )
}

fn table_exists(connection: &Connection, table: &str) -> bool {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
            params![table],
            |row| row.get(0),
        )
        .expect("query synthetic schema")
}
