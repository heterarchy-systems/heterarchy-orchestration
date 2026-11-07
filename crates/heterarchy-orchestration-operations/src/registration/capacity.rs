use forge_domain::WorkspaceId;
use heterarchy_orchestration_sessions::AgentProfileId;
use rusqlite::Transaction;
#[cfg(test)]
use rusqlite::{Connection, params};
use uuid::Uuid;

use crate::OperationsError;

#[cfg(test)]
const WORKSPACE_AGENT_ADMISSION_SQL: &str = "WITH profile_associations AS (
    SELECT participant_id,agent_profile_id AS profile_id
    FROM ops_workspace_participation_mappings WHERE workspace_id=?1
    UNION
    SELECT r.participant_id,s.profile_id
    FROM ops_registration_profile_scopes AS s
    JOIN ops_registration_requests AS r
      ON r.request_id=s.request_id AND r.workspace_id=s.workspace_id
    WHERE s.workspace_id=?1 AND r.workspace_id=?1
), participant_ids AS (
    SELECT participant_id FROM ops_logical_members WHERE workspace_id=?1
    UNION
    SELECT participant_id FROM ops_pending_participants WHERE workspace_id=?1
), agent_keys(identity_kind,identity_id) AS (
    SELECT 0,agent_profile_id
    FROM ops_workspace_participations WHERE workspace_id=?1
    UNION
    SELECT 0,profile_id FROM profile_associations
    UNION
    SELECT 1,p.participant_id FROM participant_ids AS p
    WHERE NOT EXISTS (
        SELECT 1 FROM profile_associations AS a
        WHERE a.participant_id=p.participant_id
    )
)
SELECT COUNT(*) FROM agent_keys";

/// Check a new logical identity in the caller's IMMEDIATE transaction. The
/// caller writes its pending registration reservation before committing that
/// same transaction.
pub(super) fn ensure_admission_capacity(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    profile_id: Option<AgentProfileId>,
    participant_id: Uuid,
) -> Result<(), OperationsError> {
    crate::team_expansion::workspace_hold::ensure_registration_capacity_with_holds(
        transaction,
        workspace_id,
        profile_id,
        participant_id,
    )
}

#[cfg(test)]
fn workspace_agent_state(
    connection: &Connection,
    workspace_id: WorkspaceId,
) -> Result<usize, OperationsError> {
    let count: i64 = connection.query_row(
        WORKSPACE_AGENT_ADMISSION_SQL,
        params![workspace_id.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    usize::try_from(count).map_err(|_| OperationsError::Corrupt("workspace Agent count"))
}

#[cfg(test)]
mod tests;
