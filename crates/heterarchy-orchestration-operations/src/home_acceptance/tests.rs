//! Synthetic guard fixtures. These rows exercise caller guard placement only;
//! they do not prove or substitute for native Home admission.

use std::error::Error;

use rusqlite::{Connection, params};
use uuid::Uuid;

use forge_domain::AgentAssignmentId;

use crate::NodeTeamBindingScope;

/// Seed a minimal Pending Home work reservation for caller-level guard tests.
/// The intent/proposal snapshots are deliberately opaque synthetic bytes.
pub(crate) fn seed_pending_work_reservation(
    connection: &Connection,
    scope: NodeTeamBindingScope,
    root_assignment_id: AgentAssignmentId,
    successor_assignment_id: AgentAssignmentId,
) -> Result<Uuid, Box<dyn Error>> {
    if root_assignment_id.into_uuid().is_nil()
        || successor_assignment_id.into_uuid().is_nil()
        || root_assignment_id == successor_assignment_id
    {
        return Err("synthetic Home reservation assignment identity".into());
    }
    let acceptance_id = Uuid::now_v7();
    let issuer_id = Uuid::now_v7();
    let backend_id = Uuid::now_v7();
    let destination_id = Uuid::now_v7();
    let request_digest = [0x31_u8; 32];
    let proposal_digest = [0x32_u8; 32];
    let intent_digest = [0x33_u8; 32];
    let request_snapshot: &[u8] = b"synthetic-request";
    let proposal_snapshot: &[u8] = b"synthetic-proposal";
    connection.execute(
        "INSERT INTO ops_home_acceptance_intents(
            acceptance_id,workspace_id,issuer_id,source_backend_id,source_destination_id,
            predecessor_acceptance_id,predecessor_source_digest,request_digest,request_snapshot,
            proposal_snapshot,proposal_digest,intent_digest,recorded_at_ms
         ) VALUES(?1,?2,?3,?4,?5,NULL,NULL,?6,?7,?8,?9,?10,10)",
        params![
            acceptance_id.as_bytes(),
            scope.workspace_id().into_uuid().as_bytes(),
            issuer_id.as_bytes(),
            backend_id.as_bytes(),
            destination_id.as_bytes(),
            request_digest.as_slice(),
            request_snapshot,
            proposal_snapshot,
            proposal_digest.as_slice(),
            intent_digest.as_slice(),
        ],
    )?;
    connection.execute(
        "INSERT INTO ops_home_acceptance_work_reservations(
            acceptance_id,workspace_id,activity_id,goal_id,goal_created_at_ms,dag_revision,
            dag_base_goal_revision,node_id,root_assignment_id,successor_assignment_id,
            authority_kind,team_id,state,created_at_ms,updated_at_ms,terminal_source_digest
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,0,NULL,0,10,10,NULL)",
        params![
            acceptance_id.as_bytes(),
            scope.workspace_id().into_uuid().as_bytes(),
            scope.activity_id().into_uuid().as_bytes(),
            scope.goal_id().into_uuid().as_bytes(),
            scope.goal_created_at().get(),
            scope.dag_revision().get(),
            scope.dag_base_goal_revision().get(),
            scope.node_id().into_uuid().as_bytes(),
            root_assignment_id.into_uuid().as_bytes(),
            successor_assignment_id.into_uuid().as_bytes(),
        ],
    )?;
    Ok(acceptance_id)
}
