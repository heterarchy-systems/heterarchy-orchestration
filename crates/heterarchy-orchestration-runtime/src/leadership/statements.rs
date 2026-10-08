//! Durable, read-only leadership statement lookup and replay delegation.

use forge_domain::{ActivityId, WorkspaceId};
use heterarchy_orchestration_operations::{
    LeadershipStatementCommand as OperationsStatementCommand, LeadershipStatementReadback,
};
use uuid::Uuid;

use super::{StatementCommand, member_agent_id};
use crate::{OrchestrationRuntime, RuntimeError};

/// Convert the runtime command to the exact operation-owner statement tuple.
pub(super) fn operations_command(
    command: StatementCommand,
) -> Result<OperationsStatementCommand, RuntimeError> {
    let workspace = super::require_workspace(command.workspace_id)?;
    let activity = ActivityId::from_uuid(super::require_activity(command.activity_id)?);
    let session = super::require_session(command.session_id)?;
    let now = super::timestamp(command.now_ms)?;
    let agent_id = member_agent_id(
        command.workspace_id,
        command.activity_id,
        command.session_id,
    );
    Ok(OperationsStatementCommand {
        workspace_id: workspace,
        statement_id: command.message_id,
        request_id: command.request_id,
        election_id: command.election_id,
        actor: heterarchy_orchestration_operations::OperationalActor::Participant {
            activity_id: activity,
            agent_id,
            session_id: session,
        },
        kind: command.kind,
        body: command.body,
        now_ms: now,
    })
}

impl OrchestrationRuntime {
    /// Read one exact statement from its verified Workspace's durable journal.
    ///
    /// The source boundary performs caller, session, and Forge-scope
    /// authorization before invoking this reference query.
    ///
    /// # Errors
    /// Rejects nil identities, an unverified Workspace, foreign statements,
    /// corrupt review context, catalog failures, or a poisoned owner.
    pub fn leadership_statement(
        &self,
        workspace_id: Uuid,
        message_id: Uuid,
    ) -> Result<Option<LeadershipStatementReadback>, RuntimeError> {
        if workspace_id.is_nil() || message_id.is_nil() {
            return Err(RuntimeError::InvalidInput);
        }
        let catalog = self.catalog()?;
        Ok(catalog.leadership_statement(WorkspaceId::from_uuid(workspace_id), message_id)?)
    }

    /// Check whether a command exactly matches one durable statement replay.
    ///
    /// This delegates journal-first replay validation to Operations and does
    /// not require the current election or member binding to remain open.
    ///
    /// # Errors
    /// Rejects invalid command identities or body, replay conflicts, catalog
    /// failures, or a poisoned owner.
    pub fn leadership_statement_replay_exists(
        &self,
        command: &StatementCommand,
    ) -> Result<bool, RuntimeError> {
        let command = operations_command(command.clone())?;
        let catalog = self.catalog()?;
        Ok(catalog.leadership_statement_replay_exists(&command)?)
    }
}
