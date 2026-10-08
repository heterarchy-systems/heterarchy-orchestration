//! Checked, read-only source observations for logical Agent departure.
//!
//! These values carry Forge's immutable reconciliation data into Operations.
//! They are not reconstructed Forge receipts or effect capabilities. The
//! observation owns Operations' source matching, replay validation, and journal
//! field mapping; SQL reads and writes remain with the departure catalog.

use crate::{
    LogicalDepartureKind, LogicalDepartureReceipt, LogicalDetachCommand, LogicalHandoffCommand,
    OperationsError,
};
use forge_domain::{
    ActivityCoordinatorRevision, ActivityId, AgentInstanceId, AgentInstanceRevision,
    AgentSessionId, TimestampMillis, WorkspaceId,
};
use forge_persistence::{AgentDepartureKind, AgentDepartureReconciliation};
use uuid::Uuid;

/// Typed fields returned by an exact Forge departure reconciliation read.
///
/// This is the raw Operations boundary input. Its fields are public so an
/// adapter can assemble the named source tuple; [`DepartureObservation::new`]
/// validates it before Operations uses it.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DepartureObservationParts {
    pub request_id: Uuid,
    pub migration_id: Uuid,
    pub destination_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub activity_id: ActivityId,
    pub kind: AgentDepartureKind,
    pub agent_id: AgentInstanceId,
    pub session_id: AgentSessionId,
    pub expected_agent_revision: AgentInstanceRevision,
    pub result_agent_revision: AgentInstanceRevision,
    pub target_agent_id: Option<AgentInstanceId>,
    pub target_session_id: Option<AgentSessionId>,
    pub result_coordinator_revision: Option<ActivityCoordinatorRevision>,
    pub identity_digest: [u8; 32],
    pub reconciled_at: TimestampMillis,
}

/// Checked read-only view of one Forge Agent departure reconciliation.
///
/// This value retains source data for Operations validation and journaling. It
/// does not grant access to Forge effects, reconstruct a Forge receipt, or
/// carry a second source digest authority.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DepartureObservation {
    parts: DepartureObservationParts,
}

impl DepartureObservation {
    /// Construct an Operations observation from typed source readback fields.
    ///
    /// # Errors
    /// Rejects nil identities, a zero identity digest, an invalid target shape,
    /// or a revision transition that Forge would reject when reading its
    /// reconciliation journal.
    pub fn new(parts: DepartureObservationParts) -> Result<Self, OperationsError> {
        if parts.request_id.is_nil()
            || parts.migration_id.is_nil()
            || parts.destination_id.is_nil()
            || parts.workspace_id.into_uuid().is_nil()
            || parts.activity_id.into_uuid().is_nil()
            || parts.agent_id.into_uuid().is_nil()
            || parts.session_id.into_uuid().is_nil()
            || parts
                .target_agent_id
                .is_some_and(|value| value.into_uuid().is_nil())
            || parts
                .target_session_id
                .is_some_and(|value| value.into_uuid().is_nil())
        {
            return Err(OperationsError::Conflict("departure observation identity"));
        }
        if parts.identity_digest == [0; 32] {
            return Err(OperationsError::Conflict("departure observation digest"));
        }
        let target_shape = (
            parts.target_agent_id.is_some(),
            parts.target_session_id.is_some(),
            parts.result_coordinator_revision.is_some(),
        );
        if !matches!(
            (parts.kind, target_shape),
            (AgentDepartureKind::Detach, (false, false, false))
                | (AgentDepartureKind::Handoff, (true, true, true))
        ) {
            return Err(OperationsError::Conflict(
                "departure observation target shape",
            ));
        }
        if parts.expected_agent_revision.checked_next().ok() != Some(parts.result_agent_revision) {
            return Err(OperationsError::Conflict("departure observation revision"));
        }
        Ok(Self { parts })
    }

    /// Request identity from Forge's reconciliation read.
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.parts.request_id
    }

    /// Source migration identity recorded by Forge.
    #[must_use]
    pub const fn migration_id(&self) -> Uuid {
        self.parts.migration_id
    }

    /// Destination identity recorded by Forge.
    #[must_use]
    pub const fn destination_id(&self) -> Uuid {
        self.parts.destination_id
    }

    /// Workspace identity recorded by Forge.
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.parts.workspace_id
    }

    /// Activity identity recorded by Forge.
    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.parts.activity_id
    }

    /// Departure lifecycle recorded by Forge.
    pub const fn kind(&self) -> AgentDepartureKind {
        self.parts.kind
    }

    /// Departing Agent identity recorded by Forge.
    #[must_use]
    pub const fn agent_id(&self) -> AgentInstanceId {
        self.parts.agent_id
    }

    /// Caller-owned logical Session identity recorded by Forge.
    pub const fn session_id(&self) -> AgentSessionId {
        self.parts.session_id
    }

    /// Agent revision fenced by the Forge departure command.
    #[must_use]
    pub const fn expected_agent_revision(&self) -> AgentInstanceRevision {
        self.parts.expected_agent_revision
    }

    /// Resulting Agent revision recorded by Forge.
    #[must_use]
    pub const fn result_agent_revision(&self) -> AgentInstanceRevision {
        self.parts.result_agent_revision
    }

    /// Handoff target Agent, when the reconciliation records a handoff.
    #[must_use]
    pub const fn target_agent_id(&self) -> Option<AgentInstanceId> {
        self.parts.target_agent_id
    }

    /// Handoff target Session, when the reconciliation records a handoff.
    #[must_use]
    pub const fn target_session_id(&self) -> Option<AgentSessionId> {
        self.parts.target_session_id
    }

    /// Resulting Coordinator revision, when the reconciliation records a handoff.
    #[must_use]
    pub const fn result_coordinator_revision(&self) -> Option<ActivityCoordinatorRevision> {
        self.parts.result_coordinator_revision
    }

    /// Identity digest bound to the original Forge departure request.
    #[must_use]
    pub const fn identity_digest(&self) -> [u8; 32] {
        self.parts.identity_digest
    }

    /// Time Forge recorded the reconciliation.
    #[must_use]
    pub const fn reconciled_at(&self) -> TimestampMillis {
        self.parts.reconciled_at
    }
}

impl From<&AgentDepartureReconciliation> for DepartureObservation {
    /// Copy all fields from an already-constructed Forge reconciliation.
    ///
    /// This conversion is intentionally infallible and does not rebuild or
    /// validate a Forge receipt.
    fn from(source: &AgentDepartureReconciliation) -> Self {
        Self {
            parts: DepartureObservationParts {
                request_id: source.request_id(),
                migration_id: source.migration_id(),
                destination_id: source.destination_id(),
                workspace_id: source.workspace_id(),
                activity_id: source.activity_id(),
                kind: source.kind(),
                agent_id: source.agent_id(),
                session_id: source.session_id(),
                expected_agent_revision: source.expected_agent_revision(),
                result_agent_revision: source.result_agent_revision(),
                target_agent_id: source.target_agent_id(),
                target_session_id: source.target_session_id(),
                result_coordinator_revision: source.result_coordinator_revision(),
                identity_digest: source.identity_digest(),
                reconciled_at: source.reconciled_at(),
            },
        }
    }
}

pub(crate) fn validate_detach_source(
    command: &LogicalDetachCommand,
    source: &DepartureObservation,
) -> Result<(), OperationsError> {
    if source.kind() != AgentDepartureKind::Detach
        || source.workspace_id() != command.workspace_id()
        || source.activity_id() != command.activity_id()
        || source.agent_id() != command.agent_id()
        || source.session_id() != command.session_id()
        || source.target_agent_id().is_some()
        || source.target_session_id().is_some()
        || source.result_coordinator_revision().is_some()
    {
        return Err(OperationsError::Conflict("departure source identity"));
    }
    Ok(())
}

pub(crate) fn validate_handoff_source(
    command: &LogicalHandoffCommand,
    source: &DepartureObservation,
) -> Result<(), OperationsError> {
    if source.kind() != AgentDepartureKind::Handoff
        || source.workspace_id() != command.workspace_id()
        || source.activity_id() != command.activity_id()
        || source.agent_id() != command.agent_id()
        || source.session_id() != command.session_id()
        || source.target_agent_id() != Some(command.target_agent_id())
        || source.target_session_id() != Some(command.target_session_id())
        || source.result_coordinator_revision().is_none()
    {
        return Err(OperationsError::Conflict("departure source identity"));
    }
    Ok(())
}

pub(crate) fn validate_detach_replay(
    receipt: &LogicalDepartureReceipt,
    source: &DepartureObservation,
    participant_id: Uuid,
    left_event_id: Uuid,
) -> Result<(), OperationsError> {
    if receipt.request_id() != source.request_id()
        || receipt.workspace_id() != source.workspace_id()
        || receipt.participant_id() != participant_id
        || receipt.activity_id() != source.activity_id()
        || receipt.agent_id() != source.agent_id()
        || receipt.session_id() != source.session_id()
        || receipt.kind() != LogicalDepartureKind::Detach
        || receipt.expected_agent_revision() != source.expected_agent_revision()
        || receipt.result_agent_revision() != source.result_agent_revision()
        || receipt.target_participant_id().is_some()
        || receipt.target_agent_id().is_some()
        || receipt.target_session_id().is_some()
        || receipt.result_coordinator_revision().is_some()
        || receipt.departed_event_id() != Some(left_event_id)
        || receipt.handoff_started_event_id().is_some()
        || receipt.handoff_completed_event_id().is_some()
        || receipt.source_reconciled_at() != source.reconciled_at()
    {
        return Err(OperationsError::Conflict("departure replay"));
    }
    Ok(())
}

pub(crate) fn validate_handoff_replay(
    receipt: &LogicalDepartureReceipt,
    source: &DepartureObservation,
    command: &LogicalHandoffCommand,
    started_event_id: Uuid,
    completed_event_id: Uuid,
) -> Result<(), OperationsError> {
    if receipt.request_id() != source.request_id()
        || receipt.workspace_id() != source.workspace_id()
        || receipt.participant_id() != command.participant_id()
        || receipt.activity_id() != source.activity_id()
        || receipt.agent_id() != source.agent_id()
        || receipt.session_id() != source.session_id()
        || receipt.kind() != LogicalDepartureKind::Handoff
        || receipt.expected_agent_revision() != source.expected_agent_revision()
        || receipt.result_agent_revision() != source.result_agent_revision()
        || receipt.target_participant_id() != Some(command.target_participant_id())
        || receipt.target_agent_id() != Some(command.target_agent_id())
        || receipt.target_session_id() != Some(command.target_session_id())
        || receipt.result_coordinator_revision() != source.result_coordinator_revision()
        || receipt.departed_event_id().is_some()
        || receipt.handoff_started_event_id() != Some(started_event_id)
        || receipt.handoff_completed_event_id() != Some(completed_event_id)
        || receipt.source_reconciled_at() != source.reconciled_at()
    {
        return Err(OperationsError::Conflict("departure replay"));
    }
    Ok(())
}

pub(crate) struct DepartureJournalInput {
    pub(crate) request_id: Uuid,
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) participant_id: Uuid,
    pub(crate) activity_id: ActivityId,
    pub(crate) agent_id: AgentInstanceId,
    pub(crate) session_id: AgentSessionId,
    pub(crate) kind: LogicalDepartureKind,
    pub(crate) expected_agent_revision: AgentInstanceRevision,
    pub(crate) result_agent_revision: AgentInstanceRevision,
    pub(crate) target_participant_id: Option<Uuid>,
    pub(crate) target_agent_id: Option<AgentInstanceId>,
    pub(crate) target_session_id: Option<AgentSessionId>,
    pub(crate) result_coordinator_revision: Option<ActivityCoordinatorRevision>,
    pub(crate) departed_event_id: Option<Uuid>,
    pub(crate) started_event_id: Option<Uuid>,
    pub(crate) completed_event_id: Option<Uuid>,
    pub(crate) membership_revision: u64,
    pub(crate) presence_cleared: bool,
    pub(crate) source_reconciled_at: TimestampMillis,
    pub(crate) committed_at: TimestampMillis,
}

pub(crate) fn detach_journal_input(
    command: &LogicalDetachCommand,
    source: &DepartureObservation,
    committed_at: TimestampMillis,
    left_event_id: Uuid,
    membership_revision: u64,
    presence_cleared: bool,
) -> DepartureJournalInput {
    DepartureJournalInput {
        request_id: source.request_id(),
        workspace_id: command.workspace_id(),
        participant_id: command.participant_id(),
        activity_id: command.activity_id(),
        agent_id: source.agent_id(),
        session_id: source.session_id(),
        kind: LogicalDepartureKind::Detach,
        expected_agent_revision: source.expected_agent_revision(),
        result_agent_revision: source.result_agent_revision(),
        target_participant_id: None,
        target_agent_id: None,
        target_session_id: None,
        result_coordinator_revision: None,
        departed_event_id: Some(left_event_id),
        started_event_id: None,
        completed_event_id: None,
        membership_revision,
        presence_cleared,
        source_reconciled_at: source.reconciled_at(),
        committed_at,
    }
}

pub(crate) fn handoff_journal_input(
    command: &LogicalHandoffCommand,
    source: &DepartureObservation,
    committed_at: TimestampMillis,
    started_event_id: Uuid,
    completed_event_id: Uuid,
    membership_revision: u64,
    presence_cleared: bool,
) -> DepartureJournalInput {
    DepartureJournalInput {
        request_id: source.request_id(),
        workspace_id: command.workspace_id(),
        participant_id: command.participant_id(),
        activity_id: command.activity_id(),
        agent_id: source.agent_id(),
        session_id: source.session_id(),
        kind: LogicalDepartureKind::Handoff,
        expected_agent_revision: source.expected_agent_revision(),
        result_agent_revision: source.result_agent_revision(),
        target_participant_id: Some(command.target_participant_id()),
        target_agent_id: Some(command.target_agent_id()),
        target_session_id: Some(command.target_session_id()),
        result_coordinator_revision: source.result_coordinator_revision(),
        departed_event_id: None,
        started_event_id: Some(started_event_id),
        completed_event_id: Some(completed_event_id),
        membership_revision,
        presence_cleared,
        source_reconciled_at: source.reconciled_at(),
        committed_at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn detach_parts() -> Result<DepartureObservationParts, Box<dyn std::error::Error>> {
        Ok(DepartureObservationParts {
            request_id: Uuid::from_u128(1),
            migration_id: Uuid::from_u128(2),
            destination_id: Uuid::from_u128(3),
            workspace_id: WorkspaceId::from_uuid(Uuid::from_u128(4)),
            activity_id: ActivityId::from_uuid(Uuid::from_u128(5)),
            kind: AgentDepartureKind::Detach,
            agent_id: AgentInstanceId::from_uuid(Uuid::from_u128(6)),
            session_id: AgentSessionId::from_uuid(Uuid::from_u128(7)),
            expected_agent_revision: AgentInstanceRevision::new(1)?,
            result_agent_revision: AgentInstanceRevision::new(2)?,
            target_agent_id: None,
            target_session_id: None,
            result_coordinator_revision: None,
            identity_digest: [1; 32],
            reconciled_at: TimestampMillis::new(8)?,
        })
    }

    #[test]
    fn rejects_nil_identity_and_zero_digest() -> Result<(), Box<dyn std::error::Error>> {
        let mut parts = detach_parts()?;
        parts.request_id = Uuid::nil();
        assert!(matches!(
            DepartureObservation::new(parts),
            Err(OperationsError::Conflict("departure observation identity"))
        ));

        let mut parts = detach_parts()?;
        parts.identity_digest = [0; 32];
        assert!(matches!(
            DepartureObservation::new(parts),
            Err(OperationsError::Conflict("departure observation digest"))
        ));
        Ok(())
    }

    #[test]
    fn rejects_incomplete_handoff_shape() -> Result<(), Box<dyn std::error::Error>> {
        let mut parts = detach_parts()?;
        parts.kind = AgentDepartureKind::Handoff;
        assert!(matches!(
            DepartureObservation::new(parts),
            Err(OperationsError::Conflict(
                "departure observation target shape"
            ))
        ));
        Ok(())
    }

    #[test]
    fn accepts_complete_handoff_shape() -> Result<(), Box<dyn std::error::Error>> {
        let mut parts = detach_parts()?;
        parts.kind = AgentDepartureKind::Handoff;
        parts.target_agent_id = Some(AgentInstanceId::from_uuid(Uuid::from_u128(9)));
        parts.target_session_id = Some(AgentSessionId::from_uuid(Uuid::from_u128(10)));
        parts.result_coordinator_revision = Some(ActivityCoordinatorRevision::new(2)?);

        assert!(DepartureObservation::new(parts).is_ok());
        Ok(())
    }

    #[test]
    fn rejects_revision_transition_not_accepted_by_forge() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut parts = detach_parts()?;
        parts.result_agent_revision = AgentInstanceRevision::new(3)?;
        assert!(matches!(
            DepartureObservation::new(parts),
            Err(OperationsError::Conflict("departure observation revision"))
        ));
        Ok(())
    }
}
