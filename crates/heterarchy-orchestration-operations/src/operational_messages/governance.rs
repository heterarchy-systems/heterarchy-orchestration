//! Historical leadership-review invitations layered over the native message
//! outbox. Existing rows remain readable; new provider-backed invitation
//! enqueue is retired.

#![allow(clippy::must_use_candidate)]

use super::{
    MessageId, OperationsCatalog, OperationsError, ProviderMessageOutbox, ProviderMessageTarget,
};
use crate::{LeadershipReviewPhase, WorkspaceParticipationId};
use forge_domain::TimestampMillis;
use rusqlite::{OptionalExtension, params};
use uuid::Uuid;

const INVITATIONS_SQL: &str = r"
CREATE TABLE ops_governance_invitations (
    request_id BLOB NOT NULL REFERENCES ops_leadership_reviews(request_id) CHECK(length(request_id)=16 AND request_id != zeroblob(16)),
    election_id BLOB NOT NULL REFERENCES ops_leadership_elections(election_id) CHECK(length(election_id)=16 AND election_id != zeroblob(16)),
    workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16),
    phase TEXT NOT NULL CHECK(phase IN('DEFENSE','DISCUSSION','BALLOT')),
    recipient_participation_id BLOB NOT NULL CHECK(length(recipient_participation_id)=16 AND recipient_participation_id != zeroblob(16)),
    message_id BLOB NOT NULL UNIQUE REFERENCES ops_workspace_messages(message_id) CHECK(length(message_id)=16 AND message_id != zeroblob(16)),
    deadline_ms INTEGER NOT NULL CHECK(deadline_ms>=0),
    queued_at_ms INTEGER NOT NULL CHECK(queued_at_ms>=0),
    PRIMARY KEY(request_id,phase,recipient_participation_id),
    FOREIGN KEY(workspace_id,recipient_participation_id) REFERENCES ops_workspace_participations(workspace_id,participation_id)
);
";

const INVITATION_MESSAGE_INDEX_SQL: &str =
    "CREATE INDEX ops_governance_invitations_message_idx ON ops_governance_invitations(message_id)";

/// Schema hook for the governance invitation table.
pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("table", "ops_governance_invitations", INVITATIONS_SQL),
        (
            "index",
            "ops_governance_invitations_message_idx",
            INVITATION_MESSAGE_INDEX_SQL,
        ),
    ]
}

/// The exact phase captured by an invitation.  Awaiting and resolved reviews
/// never admit a new provider effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GovernanceInvitationPhase {
    Defense,
    Discussion,
    Ballot,
}

impl GovernanceInvitationPhase {
    fn encode(self) -> &'static str {
        match self {
            Self::Defense => "DEFENSE",
            Self::Discussion => "DISCUSSION",
            Self::Ballot => "BALLOT",
        }
    }

    fn decode(value: &str) -> Result<Self, OperationsError> {
        match value {
            "DEFENSE" => Ok(Self::Defense),
            "DISCUSSION" => Ok(Self::Discussion),
            "BALLOT" => Ok(Self::Ballot),
            _ => Err(OperationsError::Corrupt("governance invitation phase")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InvitationRow {
    request_id: Uuid,
    election_id: Uuid,
    workspace_id: forge_domain::WorkspaceId,
    phase: GovernanceInvitationPhase,
    recipient_participation_id: WorkspaceParticipationId,
    message_id: MessageId,
    deadline_ms: i64,
    queued_at_ms: i64,
}

impl InvitationRow {
    fn with_outbox(self, outbox: ProviderMessageOutbox) -> GovernanceInvitation {
        GovernanceInvitation {
            request_id: self.request_id,
            election_id: self.election_id,
            workspace_id: self.workspace_id,
            phase: self.phase,
            recipient_participation_id: self.recipient_participation_id,
            message_id: self.message_id,
            deadline_ms: self.deadline_ms,
            queued_at_ms: self.queued_at_ms,
            outbox,
        }
    }
}

/// Durable identity and outbox readback for one invitation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GovernanceInvitation {
    request_id: Uuid,
    election_id: Uuid,
    workspace_id: forge_domain::WorkspaceId,
    phase: GovernanceInvitationPhase,
    recipient_participation_id: WorkspaceParticipationId,
    message_id: MessageId,
    deadline_ms: i64,
    queued_at_ms: i64,
    outbox: ProviderMessageOutbox,
}

impl GovernanceInvitation {
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    pub const fn election_id(&self) -> Uuid {
        self.election_id
    }

    pub const fn workspace_id(&self) -> forge_domain::WorkspaceId {
        self.workspace_id
    }

    pub const fn phase(&self) -> GovernanceInvitationPhase {
        self.phase
    }

    pub const fn recipient_participation_id(&self) -> WorkspaceParticipationId {
        self.recipient_participation_id
    }

    pub const fn message_id(&self) -> MessageId {
        self.message_id
    }

    pub const fn deadline_ms(&self) -> i64 {
        self.deadline_ms
    }

    pub const fn queued_at_ms(&self) -> i64 {
        self.queued_at_ms
    }

    pub fn outbox(&self) -> &ProviderMessageOutbox {
        &self.outbox
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GovernanceInvitationQueueOutcome {
    invitation: GovernanceInvitation,
    created: bool,
}

impl GovernanceInvitationQueueOutcome {
    pub fn invitation(&self) -> &GovernanceInvitation {
        &self.invitation
    }

    pub const fn created(&self) -> bool {
        self.created
    }
}

impl OperationsCatalog {
    /// Retired provider-backed leadership invitation enqueue entrypoint.
    /// # Errors
    /// Always returns [`OperationsError::ProviderRetired`] without reading or
    /// writing the catalog. Existing invitations remain readable.
    pub fn enqueue_leadership_invitation(
        &mut self,
        request_id: Uuid,
        expected_phase: LeadershipReviewPhase,
        target: ProviderMessageTarget,
        now: TimestampMillis,
    ) -> Result<GovernanceInvitationQueueOutcome, OperationsError> {
        let _ = (request_id, expected_phase, target, now);
        Err(OperationsError::ProviderRetired)
    }

    /// Read one exact invitation and its immutable outbox target after a
    /// process restart.  This read does not require the phase or recipient to
    /// remain live.
    /// # Errors
    /// Returns verification, corruption, or SQLite readback errors.
    pub fn read_leadership_invitation(
        &self,
        request_id: Uuid,
        phase: GovernanceInvitationPhase,
        recipient_participation_id: WorkspaceParticipationId,
    ) -> Result<Option<GovernanceInvitation>, OperationsError> {
        let transaction = self.connection.unchecked_transaction()?;
        let invitation = transaction
            .query_row(
                "SELECT request_id,election_id,workspace_id,phase,recipient_participation_id,message_id,deadline_ms,queued_at_ms FROM ops_governance_invitations WHERE request_id=?1 AND phase=?2 AND recipient_participation_id=?3",
                params![
                    request_id.as_bytes(),
                    phase.encode(),
                    recipient_participation_id.into_uuid().as_bytes(),
                ],
                decode_invitation_row,
            )
            .optional()?;
        let Some(invitation) = invitation else {
            super::catalog_commit_read(transaction, self.destination)?;
            return Ok(None);
        };
        super::require_verified_workspace(&transaction, invitation.workspace_id, self.destination)?;
        let outbox = super::load_outbox(&transaction, invitation.message_id)?
            .ok_or(OperationsError::Corrupt("governance invitation outbox"))?;
        super::catalog_commit_read(transaction, self.destination)?;
        Ok(Some(invitation.with_outbox(outbox)))
    }

    /// Return the immutable leadership phase deadline attached to an outbox
    /// message.  Pending/unknown callers can inspect this without changing
    /// the outbox state; expiry never authorizes a blind retry.
    /// # Errors
    /// Returns verification, corruption, or SQLite readback errors.
    pub fn provider_message_expiry(
        &self,
        message_id: MessageId,
    ) -> Result<Option<i64>, OperationsError> {
        super::validate_message_id(message_id)?;
        let transaction = self.connection.unchecked_transaction()?;
        let value = transaction
            .query_row(
                "SELECT workspace_id,deadline_ms FROM ops_governance_invitations WHERE message_id=?1",
                params![message_id.as_bytes()],
                |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, i64>(1)?,
                    ))
                },
            )
            .optional()?;
        let Some((workspace, deadline)) = value else {
            super::catalog_commit_read(transaction, self.destination)?;
            return Ok(None);
        };
        let workspace = forge_domain::WorkspaceId::from_uuid(
            Uuid::from_slice(&workspace)
                .map_err(|_| OperationsError::Corrupt("governance invitation Workspace"))?,
        );
        super::require_verified_workspace(&transaction, workspace, self.destination)?;
        super::catalog_commit_read(transaction, self.destination)?;
        Ok(Some(deadline))
    }
}

/// Called by the existing dispatcher only for Queued invitations.  Pending
/// and Unknown rows intentionally bypass this check and stay Inspect-only.
#[allow(clippy::redundant_closure_for_method_calls)]
#[allow(clippy::too_many_arguments)]
fn decode_invitation_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<InvitationRow> {
    let request_id =
        Uuid::from_slice(&row.get::<_, Vec<u8>>(0)?).map_err(|_| rusqlite::Error::InvalidQuery)?;
    let election_id =
        Uuid::from_slice(&row.get::<_, Vec<u8>>(1)?).map_err(|_| rusqlite::Error::InvalidQuery)?;
    let workspace_id = forge_domain::WorkspaceId::from_uuid(
        Uuid::from_slice(&row.get::<_, Vec<u8>>(2)?).map_err(|_| rusqlite::Error::InvalidQuery)?,
    );
    let phase = GovernanceInvitationPhase::decode(&row.get::<_, String>(3)?)
        .map_err(|_| rusqlite::Error::InvalidQuery)?;
    let recipient = WorkspaceParticipationId::from_uuid(
        Uuid::from_slice(&row.get::<_, Vec<u8>>(4)?).map_err(|_| rusqlite::Error::InvalidQuery)?,
    )
    .map_err(|_| rusqlite::Error::InvalidQuery)?;
    let message_id =
        Uuid::from_slice(&row.get::<_, Vec<u8>>(5)?).map_err(|_| rusqlite::Error::InvalidQuery)?;
    Ok(InvitationRow {
        request_id,
        election_id,
        workspace_id,
        phase,
        recipient_participation_id: recipient,
        message_id,
        deadline_ms: row.get(6)?,
        queued_at_ms: row.get(7)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{error::Error, fs};

    #[test]
    fn leadership_invitation_enqueue_is_retired_without_mutation() -> Result<(), Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!(
            "heterarchy-governance-invitation-retired-{}",
            Uuid::now_v7()
        ));
        fs::create_dir(&root)?;
        let path = root.join("operations.sqlite3");
        let mut catalog = OperationsCatalog::open(&path, Uuid::now_v7())?;
        let workspace = forge_domain::WorkspaceId::from_uuid(Uuid::now_v7());
        let profile = heterarchy_orchestration_sessions::AgentProfileId::from_uuid(Uuid::now_v7())?;
        let participation = WorkspaceParticipationId::from_uuid(Uuid::now_v7())?;
        let runtime = heterarchy_orchestration_sessions::AgentProfileRuntimeSessionId::from_uuid(
            Uuid::now_v7(),
        )?;
        let target = ProviderMessageTarget::new(
            workspace,
            profile,
            participation,
            runtime,
            "retired-conversation".to_owned(),
        )?;
        let before: (i64, i64, i64) = catalog.connection.query_row(
            "SELECT (SELECT COUNT(*) FROM ops_governance_invitations), (SELECT COUNT(*) FROM ops_workspace_messages), (SELECT COUNT(*) FROM ops_provider_message_outbox)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        assert!(matches!(
            catalog.enqueue_leadership_invitation(
                Uuid::nil(),
                LeadershipReviewPhase::Defense,
                target,
                TimestampMillis::new(10)?,
            ),
            Err(OperationsError::ProviderRetired)
        ));
        let after: (i64, i64, i64) = catalog.connection.query_row(
            "SELECT (SELECT COUNT(*) FROM ops_governance_invitations), (SELECT COUNT(*) FROM ops_workspace_messages), (SELECT COUNT(*) FROM ops_provider_message_outbox)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        assert_eq!(after, before);
        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
