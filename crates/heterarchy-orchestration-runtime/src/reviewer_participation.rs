//! Runtime boundary for a statement made through an authenticated Agent Session.
//!
//! The stored interest is a historical self-statement only. This module does
//! not qualify a participant, assign a case, grant evidence access, or apply
//! withdrawal to an already assigned case.

use forge_domain::{
    ActivityId, AgentInstanceId, AgentSessionId, AuthenticatedPrincipal, WorkspaceId,
};
use heterarchy_orchestration_operations::{
    OperationalActor,
    reviewer_participation::{
        ReviewerParticipationCommand, ReviewerParticipationHistoryPage,
        ReviewerParticipationReceipt, ReviewerParticipationStatement,
    },
};
use uuid::Uuid;

use crate::{OrchestrationRuntime, RuntimeError, participation::ParticipationTarget};

/// The authenticated Session's exact Activity and Agent selectors.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReviewerParticipationTarget {
    /// Exact Workspace whose Agent membership is required.
    pub workspace_id: Uuid,
    /// Current source Activity hosting this Agent Session.
    pub activity_id: Uuid,
    /// Agent identity bound to the Activity and Session.
    pub agent_id: Uuid,
    /// Open Session owned by the HTTP authenticated principal.
    pub session_id: Uuid,
}

impl ReviewerParticipationTarget {
    fn participation_target(self) -> ParticipationTarget {
        ParticipationTarget {
            workspace_id: self.workspace_id,
            activity_id: self.activity_id,
            agent_id: self.agent_id,
            session_id: self.session_id,
        }
    }

    fn actor(self) -> OperationalActor {
        OperationalActor::Participant {
            activity_id: ActivityId::from_uuid(self.activity_id),
            agent_id: AgentInstanceId::from_uuid(self.agent_id),
            session_id: AgentSessionId::from_uuid(self.session_id),
        }
    }
}

impl OrchestrationRuntime {
    /// Record interest expressed through this authenticated Session and
    /// Workspace Profile participation.
    ///
    /// This binds the statement to the resolved Agent Session and logical
    /// Profile participation. It does not attest that the credential holder is
    /// an independent human Agent, or that one principal owns only one Agent.
    ///
    /// # Errors
    /// Rejects a closed/foreign Session, inaccessible Workspace, non-Profile
    /// participant, stale CAS, invalid withdrawal, or storage uncertainty. A
    /// response loss must be resolved with the same request ID.
    pub fn record_reviewer_participation(
        &self,
        target: ReviewerParticipationTarget,
        request_id: Uuid,
        expected_revision: u64,
        statement: ReviewerParticipationStatement,
        principal: AuthenticatedPrincipal,
    ) -> Result<ReviewerParticipationReceipt, RuntimeError> {
        self.require_participation(target.participation_target(), principal)?;
        let command = ReviewerParticipationCommand::new(
            WorkspaceId::from_uuid(target.workspace_id),
            request_id,
            expected_revision,
            target.actor(),
            statement,
        )?;
        self.catalog()?
            .record_reviewer_participation(&command)
            .map_err(RuntimeError::persistence)
    }

    /// Read one exact statement from the same stable Profile participation.
    /// A replacement Session can read history while the receipt keeps the
    /// original writer Session. It cannot replay an old mutation.
    ///
    /// # Errors
    /// Rejects a closed/foreign Session, inaccessible Workspace, non-Profile
    /// participant, invalid identity, or storage error.
    pub fn reviewer_participation_request(
        &self,
        target: ReviewerParticipationTarget,
        request_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<ReviewerParticipationReceipt>, RuntimeError> {
        self.require_participation(target.participation_target(), principal)?;
        self.catalog()?
            .reviewer_participation_request(
                WorkspaceId::from_uuid(target.workspace_id),
                request_id,
                target.actor(),
            )
            .map_err(RuntimeError::persistence)
    }

    /// Read an ascending, bounded history of the authenticated Profile's
    /// Workspace participation. History does not grant any Reviewer authority
    /// or prove independent human identity.
    ///
    /// # Errors
    /// Rejects a closed/foreign Session, inaccessible Workspace, non-Profile
    /// participant, invalid cursor or page size, or storage error.
    pub fn reviewer_participation_history(
        &self,
        target: ReviewerParticipationTarget,
        after_revision: Option<u64>,
        limit: u32,
        principal: AuthenticatedPrincipal,
    ) -> Result<ReviewerParticipationHistoryPage, RuntimeError> {
        self.require_participation(target.participation_target(), principal)?;
        self.catalog()?
            .reviewer_participation_history(
                WorkspaceId::from_uuid(target.workspace_id),
                target.actor(),
                after_revision,
                limit,
            )
            .map_err(RuntimeError::persistence)
    }
}
