//! Bounded owner-facing read of currently active leadership reviews.

use forge_domain::{ActivityId, WorkspaceId};
use rusqlite::{Transaction, params};
use uuid::Uuid;

use crate::{OperationsCatalog, OperationsError};

const MAX_ACTIVE_REVIEW_PAGE: u32 = 8;
const REVIEW_STATEMENT_PAGE: u32 = 64;
const MAX_MAINTENANCE_PAGE: u32 = 8;

impl OperationsCatalog {
    /// Read bounded lifecycle scopes for the host-owned maintenance driver.
    /// The cursor is SQLite's ephemeral rowid and is only a keyset position;
    /// it is never durable authority or a mutation identity.
    ///
    /// # Errors
    /// Returns invalid cursor/limit, import-authority, storage, or corrupt
    /// native scope errors.
    pub fn leadership_maintenance_scopes(
        &self,
        after_rowid: Option<i64>,
        limit: u32,
    ) -> Result<Vec<(i64, super::super::LeadershipScope)>, OperationsError> {
        self.leadership_driver_scopes(after_rowid, limit, false)
    }

    /// Read scopes whose current recovery can progress from terminal facts.
    /// Live intents are replayed independently before this scan admits them.
    ///
    /// # Errors
    /// Rejects invalid page bounds, corrupt scope identities and storage errors.
    pub fn leadership_recovery_scopes(
        &self,
        after_rowid: Option<i64>,
        limit: u32,
    ) -> Result<Vec<(i64, super::super::LeadershipScope)>, OperationsError> {
        self.leadership_driver_scopes(after_rowid, limit, true)
    }

    fn leadership_driver_scopes(
        &self,
        after_rowid: Option<i64>,
        limit: u32,
        recovery: bool,
    ) -> Result<Vec<(i64, super::super::LeadershipScope)>, OperationsError> {
        if !(1..=MAX_MAINTENANCE_PAGE).contains(&limit) {
            return Err(OperationsError::Bounds("leadership maintenance page"));
        }
        if after_rowid.is_some_and(|rowid| rowid < 0) {
            return Err(OperationsError::Conflict("leadership maintenance cursor"));
        }
        let tx = self.connection.unchecked_transaction()?;
        let query_limit = i64::from(limit) + 1;
        let mut query = tx.prepare(
            "SELECT l.rowid,substr(l.workspace_id,1,17),l.scope_kind,substr(l.activity_id,1,17),substr(l.team_id,1,17) FROM ops_leadership l WHERE l.rowid>?1 AND ((?4=0 AND l.state IN ('BOOTSTRAPPING','STABILIZING','ACTIVE')) OR
              (?4=1 AND l.state IN ('AUTHORITY_PENDING','RECOVERY_REQUIRED')
                AND NOT EXISTS(SELECT 1 FROM ops_leadership_confirm_intents live
                  WHERE live.workspace_id=l.workspace_id AND live.scope_kind=l.scope_kind
                    AND live.activity_id IS l.activity_id AND live.team_id IS l.team_id
                    AND live.state IN ('pending','remote_recorded'))
                AND (l.state='RECOVERY_REQUIRED' OR EXISTS(
                  SELECT 1 FROM ops_leadership_confirm_intents terminal
                  WHERE terminal.workspace_id=l.workspace_id AND terminal.scope_kind=l.scope_kind
                    AND terminal.activity_id IS l.activity_id AND terminal.team_id IS l.team_id
                    AND terminal.election_id=l.current_election_id
                    AND terminal.state IN ('rejected','superseded'))))) AND EXISTS(SELECT 1 FROM ops_imports i WHERE i.workspace_id=l.workspace_id AND i.destination_id=?2 AND i.phase=2) AND (l.scope_kind IN (0,1) OR EXISTS(SELECT 1 FROM ops_teams t WHERE t.workspace_id=l.workspace_id AND t.team_id=l.team_id AND t.state='ACTIVE')) ORDER BY l.rowid LIMIT ?3",
        )?;
        let rows = query
            .query_map(
                params![
                    after_rowid.unwrap_or(0),
                    self.destination.as_bytes(),
                    query_limit,
                    recovery
                ],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, Option<Vec<u8>>>(3)?,
                        row.get::<_, Option<Vec<u8>>>(4)?,
                    ))
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;
        drop(query);
        let mut scopes = Vec::with_capacity(limit as usize);
        for (rowid, workspace, scope_kind, activity, team) in rows.into_iter().take(limit as usize)
        {
            let workspace_id =
                WorkspaceId::from_uuid(Uuid::from_bytes(crate::wire::id(workspace)?));
            let scope = match scope_kind {
                0 if activity.is_none() && team.is_none() => {
                    super::super::LeadershipScope::workspace(workspace_id)
                }
                1 if activity.is_some() && team.is_none() => {
                    super::super::LeadershipScope::activity(
                        workspace_id,
                        ActivityId::from_uuid(Uuid::from_bytes(crate::wire::id(activity.ok_or(
                            OperationsError::Corrupt("leadership maintenance activity"),
                        )?)?)),
                    )
                }
                2 if activity.is_none() && team.is_some() => super::super::LeadershipScope::team(
                    workspace_id,
                    Uuid::from_bytes(crate::wire::id(
                        team.ok_or(OperationsError::Corrupt("leadership maintenance team"))?,
                    )?),
                ),
                _ => return Err(OperationsError::Corrupt("leadership maintenance scope")),
            };
            scopes.push((rowid, scope));
        }
        tx.commit()?;
        Ok(scopes)
    }

    /// Read the bounded set of reviews that still own a live election pointer.
    /// Request IDs are the stable lexicographic cursor; an empty result means
    /// the caller has reached the end of the active set.
    ///
    /// # Errors
    /// Returns invalid cursor/limit, import-authority, storage, or corruption
    /// errors.
    pub fn active_leadership_reviews(
        &self,
        after_request_id: Option<Uuid>,
        limit: u32,
        now_ms: i64,
    ) -> Result<Vec<super::LeadershipReviewSnapshot>, OperationsError> {
        if !(1..=MAX_ACTIVE_REVIEW_PAGE).contains(&limit) {
            return Err(OperationsError::Bounds("active leadership review page"));
        }
        if after_request_id.is_some_and(|id| id.is_nil()) {
            return Err(OperationsError::Conflict("active leadership review cursor"));
        }
        if now_ms < 0 {
            return Err(OperationsError::Conflict("active leadership review clock"));
        }
        let tx = self.connection.unchecked_transaction()?;
        let after = after_request_id.map(|id| id.into_bytes().to_vec());
        let query_limit = i64::from(limit) + 1;
        let mut query = tx.prepare(
            "SELECT substr(r.request_id,1,17) FROM ops_leadership_reviews r JOIN ops_leadership_elections e ON e.election_id=r.election_id AND e.workspace_id=r.workspace_id AND e.scope_kind=r.scope_kind AND e.activity_id IS r.activity_id AND e.team_id IS r.team_id JOIN ops_leadership l ON l.workspace_id=r.workspace_id AND l.scope_kind=r.scope_kind AND l.activity_id IS r.activity_id AND l.team_id IS r.team_id AND l.current_election_id=r.election_id WHERE e.state IN ('ELECTING','CLOSED_ELECTED') AND l.state IN ('ELECTING','AUTHORITY_PENDING') AND (?1 IS NULL OR r.request_id>?1) ORDER BY r.request_id LIMIT ?2",
        )?;
        let request_ids = query
            .query_map(params![after, query_limit], |row| row.get::<_, Vec<u8>>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        drop(query);
        let mut snapshots = Vec::with_capacity(limit as usize);
        for raw_request_id in request_ids.into_iter().take(limit as usize) {
            let request_id = Uuid::from_bytes(crate::wire::id(raw_request_id)?);
            snapshots.push(read_active_review_snapshot(
                &tx,
                request_id,
                now_ms,
                self.destination,
            )?);
        }
        tx.commit()?;
        Ok(snapshots)
    }
}

fn read_active_review_snapshot(
    tx: &Transaction<'_>,
    request_id: Uuid,
    now_ms: i64,
    destination: Uuid,
) -> Result<super::LeadershipReviewSnapshot, OperationsError> {
    let review = super::load_review_by_request(tx, request_id)?
        .ok_or(OperationsError::Corrupt("active leadership review"))?;
    super::ensure_verified_workspace(tx, review.scope.workspace_id(), destination)?;
    let election = super::super::load_election(tx, review.election_id)?.ok_or(
        OperationsError::Corrupt("active leadership review election"),
    )?;
    let leadership = super::super::load_leadership(tx, review.scope)?
        .ok_or(OperationsError::Corrupt("active leadership review scope"))?;
    if !matches!(
        election.state,
        super::super::ElectionState::Electing | super::super::ElectionState::ClosedElected
    ) || !matches!(
        leadership.state,
        super::super::LeadershipState::Electing | super::super::LeadershipState::AuthorityPending
    ) || leadership.current_election_id != Some(review.election_id)
    {
        return Err(OperationsError::Corrupt(
            "active leadership review predicate",
        ));
    }
    super::read_review_snapshot_in_tx(tx, &review, now_ms, None, REVIEW_STATEMENT_PAGE)
}

#[cfg(test)]
mod tests;
