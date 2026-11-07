//! Pending registration census with positive source rejection evidence.
use crate::{OperationsError, wire};
use forge_domain::WorkspaceId;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Frozen, bounded-memory witness of the positive source receipts used by the
/// census. Its digest attests the original query snapshot, not a later census.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RegistrationCensus {
    destination_id: [u8; 16],
    terminal_count: u64,
    latest_rejection_ms: i64,
    receipt_digest: [u8; 32],
}

impl RegistrationCensus {
    pub(super) const fn destination_id(&self) -> Uuid {
        Uuid::from_bytes(self.destination_id)
    }
    pub(super) const fn observed_high_water_ms(&self) -> i64 {
        self.latest_rejection_ms
    }

    pub(super) fn validate(
        &self,
        workspace: WorkspaceId,
        profile: Uuid,
        participation: Uuid,
    ) -> Result<(), OperationsError> {
        if self.destination_id().is_nil()
            || self.latest_rejection_ms < 0
            || i64::try_from(self.terminal_count).is_err()
            || self.receipt_digest == [0; 32]
            || (self.terminal_count == 0
                && (self.latest_rejection_ms != 0
                    || self.receipt_digest
                        != finish_digest(
                            scope_digest(self.destination_id(), workspace, profile, participation),
                            0,
                            0,
                        )))
        {
            return Err(OperationsError::Corrupt(
                "transfer preparation registration census",
            ));
        }
        Ok(())
    }
}

fn scope_digest(
    destination: Uuid,
    workspace: WorkspaceId,
    profile: Uuid,
    participation: Uuid,
) -> Sha256 {
    let mut digest = Sha256::new();
    digest.update(b"heterarchy.orchestration.home-transfer.registration-census.v1\0");
    digest.update(destination.as_bytes());
    digest.update(workspace.into_uuid().as_bytes());
    digest.update(profile.as_bytes());
    digest.update(participation.as_bytes());
    digest
}

fn finish_digest(mut digest: Sha256, count: u64, latest: i64) -> [u8; 32] {
    digest.update(count.to_be_bytes());
    digest.update(latest.to_be_bytes());
    digest.finalize().into()
}

pub(super) fn ensure_settled(
    connection: &Connection,
    workspace: WorkspaceId,
    profile: Uuid,
    participation: Uuid,
) -> Result<RegistrationCensus, OperationsError> {
    let mut registrations = connection.prepare(
        "SELECT substr(r.request_id,1,17) FROM ops_registration_requests r
            LEFT JOIN ops_registration_profile_scopes p ON p.request_id=r.request_id
            WHERE r.workspace_id=?1 AND (
                (p.request_id IS NULL AND NOT EXISTS(
                    SELECT 1 FROM ops_binding_activations a WHERE a.request_id=r.request_id
                ))
                OR (p.profile_id=?2 AND (
                    NOT EXISTS(SELECT 1 FROM ops_binding_activations a WHERE a.request_id=r.request_id)
                    OR NOT EXISTS(
                        SELECT 1 FROM ops_logical_bindings lb
                        JOIN ops_workspace_participation_mappings m
                          ON m.workspace_id=lb.workspace_id AND m.participant_id=lb.participant_id
                        WHERE lb.request_id=r.request_id AND lb.workspace_id=r.workspace_id
                          AND m.participation_id=?3 AND m.agent_profile_id=p.profile_id
                          AND (m.session_id=r.session_id OR EXISTS(
                              SELECT 1 FROM ops_workspace_participation_session_rebinds h
                              WHERE h.workspace_id=m.workspace_id
                                AND h.participation_id=m.participation_id
                                AND h.participant_id=m.participant_id
                                AND h.agent_profile_id=m.agent_profile_id
                                AND (h.previous_session_id=r.session_id OR h.new_session_id=r.session_id)
                          ))
                    )
                ))
                OR (p.request_id IS NOT NULL AND
                    (p.workspace_id!=r.workspace_id OR p.session_id!=r.session_id))
            )
        ORDER BY r.request_id",
    )?;
    let mut pending = registrations.query(params![
        workspace.into_uuid().as_bytes(),
        profile.as_bytes(),
        participation.as_bytes(),
    ])?;
    let destination: Vec<u8> = connection.query_row(
        "SELECT destination_id FROM ops_catalog_identity WHERE singleton=1",
        [],
        |row| row.get(0),
    )?;
    let destination = Uuid::from_bytes(wire::id(destination)?);
    let mut digest = scope_digest(destination, workspace, profile, participation);
    let mut terminal_count = 0_u64;
    let mut latest_rejection_ms = 0;
    // Stream historical candidates: memory does not grow with old rejected
    // requests. Only a fully validated source rejection excludes a request;
    // an attempt, NotFound or an aborted import alone is not terminal proof.
    while let Some(row) = pending.next()? {
        let request = Uuid::from_bytes(wire::id(row.get(0)?)?);
        match crate::binding_rejection::registration_binding_rejection_at(
            connection,
            destination,
            request,
        )? {
            Some(receipt) if receipt.workspace_id() == workspace => {
                terminal_count = terminal_count
                    .checked_add(1)
                    .ok_or(OperationsError::Bounds(
                        "transfer registration census count",
                    ))?;
                latest_rejection_ms = latest_rejection_ms.max(receipt.rejected_at().get());
                digest.update(request.as_bytes());
                digest.update(receipt.receipt_digest());
                digest.update(receipt.rejected_at().get().to_be_bytes());
            }
            Some(_) => {
                return Err(OperationsError::Corrupt(
                    "transfer preparation rejected request scope",
                ));
            }
            None => {
                return Err(OperationsError::Conflict(
                    "permanent transfer Workspace target registration pending",
                ));
            }
        }
    }
    Ok(RegistrationCensus {
        destination_id: destination.into_bytes(),
        terminal_count,
        latest_rejection_ms,
        receipt_digest: finish_digest(digest, terminal_count, latest_rejection_ms),
    })
}
