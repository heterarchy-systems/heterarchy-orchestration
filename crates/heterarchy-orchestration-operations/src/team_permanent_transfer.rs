//! Immutable proposal and consent records for a permanent Home-Team transfer.
//!
//! This slice records intent and the source Home Leader and target Agent
//! consents only. It does not move membership, assign the destination slot,
//! settle work/leases, or claim that the two authenticated Agent contexts are
//! independent principals.

pub(crate) mod generation;
pub(crate) mod preparation;
mod renewal;
pub(super) mod schema;
pub(crate) mod source;

pub use preparation::{
    TeamPermanentTransferBindingOrigin, TeamPermanentTransferPreparationBinding,
    TeamPermanentTransferPreparationCommand, TeamPermanentTransferPreparationReceipt,
};
pub use renewal::{
    TeamPermanentTransferGeneration, TeamPermanentTransferGenerationSnapshot,
    TeamPermanentTransferGenerationState, TeamPermanentTransferRenewalCommand,
};
pub use source::{
    TeamPermanentTransferSourceDispatchCommand, TeamPermanentTransferSourceDispatchOutcome,
    TeamPermanentTransferSourceDispatchPermit, TeamPermanentTransferSourceDispatchReadback,
    TeamPermanentTransferSourceDispatchReceipt, TeamPermanentTransferSourceDispatchReplay,
    TeamPermanentTransferSourceEnvelopeDigest, TeamPermanentTransferSourceReceiptCommand,
    TeamPermanentTransferSourceReceiptOutcome, TeamPermanentTransferSourceReceiptRecord,
    TeamPermanentTransferSourceWireIdentity, derive_team_permanent_transfer_source_request,
};

#[cfg(test)]
mod tests;

use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, TimestampMillis, WorkspaceId};
use rusqlite::{
    Connection, OptionalExtension, Transaction, TransactionBehavior, params, types::Value,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    CurrentLeadershipAuthority, LeadershipScope, OperationsCatalog, OperationsError,
    WorkspaceParticipationId,
    leadership::current_leadership_authority_in_transaction,
    operational_actor::{OperationalActor, ResolvedOperationalActor, resolve_actor},
    teams,
};

const INTENT_DIGEST_DOMAIN: &[u8] = b"heterarchy.orchestration.permanent_team_transfer.intent.v1\0";
const MAX_ORDINAL: u8 = 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamPermanentTransferScope {
    workspace: WorkspaceId,
    source_team: Uuid,
    destination_team: Uuid,
    target_participation: WorkspaceParticipationId,
}

impl TeamPermanentTransferScope {
    #[must_use]
    pub const fn new(
        workspace_id: WorkspaceId,
        source_team_id: Uuid,
        destination_team_id: Uuid,
        target_participation_id: WorkspaceParticipationId,
    ) -> Self {
        Self {
            workspace: workspace_id,
            source_team: source_team_id,
            destination_team: destination_team_id,
            target_participation: target_participation_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamPermanentTransferDestinationSlot {
    request_id: Uuid,
    decision_id: Uuid,
    ordinal: u8,
}

impl TeamPermanentTransferDestinationSlot {
    #[must_use]
    pub const fn new(request_id: Uuid, decision_id: Uuid, ordinal: u8) -> Self {
        Self {
            request_id,
            decision_id,
            ordinal,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamPermanentTransferRevisions {
    destination_team: u64,
    destination_capacity: u64,
    workspace_leadership: i64,
    source_membership: u64,
    source_leadership: i64,
}

impl TeamPermanentTransferRevisions {
    #[must_use]
    pub const fn new(
        destination_team: u64,
        destination_capacity: u64,
        workspace_leadership: i64,
        source_membership: u64,
        source_leadership: i64,
    ) -> Self {
        Self {
            destination_team,
            destination_capacity,
            workspace_leadership,
            source_membership,
            source_leadership,
        }
    }
}

/// An Orchestrator proposal to move one current Home-Team member into one
/// already-approved destination slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamPermanentTransferIntentCommand {
    transfer_id: Uuid,
    workspace_id: WorkspaceId,
    source_team_id: Uuid,
    destination_team_id: Uuid,
    target_participation_id: WorkspaceParticipationId,
    destination_request_id: Uuid,
    destination_decision_id: Uuid,
    destination_ordinal: u8,
    expected_destination_team_revision: u64,
    expected_destination_capacity_revision: u64,
    expected_workspace_leadership_revision: i64,
    expected_source_membership_revision: u64,
    expected_source_leadership_revision: i64,
    actor: OperationalActor,
}

impl TeamPermanentTransferIntentCommand {
    /// Construct one exact transfer proposal. The source membership and leader
    /// revisions are caller-observed CAS values and are rechecked under the
    /// Operations write transaction.
    ///
    /// # Errors
    /// Rejects nil or aliased identities, invalid slot ordinals/revisions, and
    /// `LocalOperator` actors.
    pub fn new(
        transfer_id: Uuid,
        scope: TeamPermanentTransferScope,
        destination: TeamPermanentTransferDestinationSlot,
        revisions: TeamPermanentTransferRevisions,
        actor: OperationalActor,
    ) -> Result<Self, OperationsError> {
        let TeamPermanentTransferScope {
            workspace: workspace_id,
            source_team: source_team_id,
            destination_team: destination_team_id,
            target_participation: target_participation_id,
        } = scope;
        let TeamPermanentTransferDestinationSlot {
            request_id: destination_request_id,
            decision_id: destination_decision_id,
            ordinal: destination_ordinal,
        } = destination;
        let TeamPermanentTransferRevisions {
            destination_team: expected_destination_team_revision,
            destination_capacity: expected_destination_capacity_revision,
            workspace_leadership: expected_workspace_leadership_revision,
            source_membership: expected_source_membership_revision,
            source_leadership: expected_source_leadership_revision,
        } = revisions;
        if transfer_id.is_nil()
            || workspace_id.into_uuid().is_nil()
            || source_team_id.is_nil()
            || destination_team_id.is_nil()
            || source_team_id == destination_team_id
            || target_participation_id.into_uuid().is_nil()
            || destination_request_id.is_nil()
            || destination_decision_id.is_nil()
            || destination_ordinal > MAX_ORDINAL
            || expected_destination_team_revision == 0
            || i64::try_from(expected_destination_team_revision).is_err()
            || expected_destination_capacity_revision == 0
            || i64::try_from(expected_destination_capacity_revision).is_err()
            || expected_workspace_leadership_revision < 1
            || expected_source_membership_revision == 0
            || i64::try_from(expected_source_membership_revision).is_err()
            || expected_source_leadership_revision < 1
            || !actor_input_is_valid(actor)
        {
            return Err(OperationsError::Conflict(
                "permanent Team transfer intent identity",
            ));
        }
        Ok(Self {
            transfer_id,
            workspace_id,
            source_team_id,
            destination_team_id,
            target_participation_id,
            destination_request_id,
            destination_decision_id,
            destination_ordinal,
            expected_destination_team_revision,
            expected_destination_capacity_revision,
            expected_workspace_leadership_revision,
            expected_source_membership_revision,
            expected_source_leadership_revision,
            actor,
        })
    }

    #[must_use]
    pub const fn transfer_id(&self) -> Uuid {
        self.transfer_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn source_team_id(&self) -> Uuid {
        self.source_team_id
    }

    #[must_use]
    pub const fn destination_team_id(&self) -> Uuid {
        self.destination_team_id
    }

    #[must_use]
    pub const fn target_participation_id(&self) -> WorkspaceParticipationId {
        self.target_participation_id
    }

    #[must_use]
    pub const fn destination_request_id(&self) -> Uuid {
        self.destination_request_id
    }

    #[must_use]
    pub const fn destination_decision_id(&self) -> Uuid {
        self.destination_decision_id
    }

    #[must_use]
    pub const fn destination_ordinal(&self) -> u8 {
        self.destination_ordinal
    }
}

/// Stable identity plus exact authenticated transport retained at write time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorSnapshot {
    activity_id: ActivityId,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    participation_id: WorkspaceParticipationId,
    profile_id: Uuid,
    participant_id: Uuid,
    display_name: String,
}

impl ActorSnapshot {
    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity_id
    }

    #[must_use]
    pub const fn agent_id(&self) -> AgentInstanceId {
        self.agent_id
    }

    pub const fn session_id(&self) -> AgentSessionId {
        self.session_id
    }

    #[must_use]
    pub const fn participation_id(&self) -> WorkspaceParticipationId {
        self.participation_id
    }

    #[must_use]
    pub const fn profile_id(&self) -> Uuid {
        self.profile_id
    }

    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }

    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }
}

impl ActorSnapshot {
    fn resolve(
        connection: &Connection,
        workspace_id: WorkspaceId,
        actor_input: OperationalActor,
    ) -> Result<Self, OperationsError> {
        let OperationalActor::Participant {
            activity_id,
            agent_id,
            session_id,
        } = actor_input
        else {
            return Err(OperationsError::Conflict(
                "permanent Team transfer requires a Profile participant",
            ));
        };
        if activity_id.into_uuid().is_nil()
            || agent_id.into_uuid().is_nil()
            || session_id.into_uuid().is_nil()
        {
            return Err(OperationsError::Conflict(
                "permanent Team transfer actor identity",
            ));
        }
        let resolved = resolve_actor(connection, workspace_id, actor_input)?;
        let ResolvedOperationalActor::ProfileParticipant {
            participation_id,
            profile_id,
            participant_id,
            session_id: resolved_session,
            display_name,
        } = resolved
        else {
            return Err(OperationsError::Conflict(
                "permanent Team transfer requires a Profile participant",
            ));
        };
        if resolved_session != session_id || display_name.is_empty() || display_name.len() > 128 {
            return Err(OperationsError::Conflict(
                "permanent Team transfer actor binding",
            ));
        }
        Ok(Self {
            activity_id,
            agent_id,
            session_id,
            participation_id,
            profile_id,
            participant_id,
            display_name,
        })
    }

    fn decode(raw: ActorRaw) -> Result<Self, OperationsError> {
        Ok(Self {
            activity_id: ActivityId::from_uuid(decode_id(
                raw.activity_id,
                "permanent Team transfer actor Activity",
            )?),
            agent_id: AgentInstanceId::from_uuid(decode_id(
                raw.agent_id,
                "permanent Team transfer actor Agent",
            )?),
            session_id: AgentSessionId::from_uuid(decode_id(
                raw.session_id,
                "permanent Team transfer actor Session",
            )?),
            participation_id: WorkspaceParticipationId::from_uuid(decode_id(
                raw.participation_id,
                "permanent Team transfer actor participation",
            )?)?,
            profile_id: decode_id(raw.profile_id, "permanent Team transfer actor Profile")?,
            participant_id: decode_id(
                raw.participant_id,
                "permanent Team transfer actor participant",
            )?,
            display_name: validate_display_name(raw.display_name)?,
        })
    }

    fn same_logical_actor(&self, other: &Self) -> bool {
        self.participation_id.into_uuid() == other.participation_id.into_uuid()
            && self.profile_id == other.profile_id
    }
}

struct ActorRaw {
    activity_id: Vec<u8>,
    agent_id: Vec<u8>,
    session_id: Vec<u8>,
    participation_id: Vec<u8>,
    profile_id: Vec<u8>,
    participant_id: Vec<u8>,
    display_name: String,
}

fn read_actor_raw(row: &rusqlite::Row<'_>, offset: usize) -> rusqlite::Result<ActorRaw> {
    Ok(ActorRaw {
        activity_id: row.get(offset)?,
        agent_id: row.get(offset + 1)?,
        session_id: row.get(offset + 2)?,
        participation_id: row.get(offset + 3)?,
        profile_id: row.get(offset + 4)?,
        participant_id: row.get(offset + 5)?,
        display_name: row.get(offset + 6)?,
    })
}

/// Immutable Orchestrator-authored proposal. It is not proof of completed
/// handoff, final transfer approval, membership cutover, or authority change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamPermanentTransferIntentReceipt {
    transfer_id: Uuid,
    workspace_id: WorkspaceId,
    source_team_id: Uuid,
    destination_team_id: Uuid,
    target_participation_id: WorkspaceParticipationId,
    target_profile_id: Uuid,
    source_membership_revision: u64,
    source_leadership_revision: i64,
    source_leader_participation_id: WorkspaceParticipationId,
    source_leader_profile_id: Uuid,
    destination_team_revision: u64,
    destination_capacity_revision: u64,
    destination_workspace_leadership_revision: i64,
    destination_request_id: Uuid,
    destination_decision_id: Uuid,
    destination_ordinal: u8,
    workspace_actor: ActorSnapshot,
    recorded_at: TimestampMillis,
    intent_digest: [u8; 32],
    replayed: bool,
}

impl TeamPermanentTransferIntentReceipt {
    #[must_use]
    pub const fn transfer_id(&self) -> Uuid {
        self.transfer_id
    }

    #[must_use]
    pub const fn source_team_id(&self) -> Uuid {
        self.source_team_id
    }

    #[must_use]
    pub const fn destination_team_id(&self) -> Uuid {
        self.destination_team_id
    }

    #[must_use]
    pub const fn target_profile_id(&self) -> Uuid {
        self.target_profile_id
    }

    #[must_use]
    pub const fn target_participation_id(&self) -> WorkspaceParticipationId {
        self.target_participation_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn source_leadership_revision(&self) -> i64 {
        self.source_leadership_revision
    }

    #[must_use]
    pub const fn destination_decision_id(&self) -> Uuid {
        self.destination_decision_id
    }

    #[must_use]
    pub const fn destination_ordinal(&self) -> u8 {
        self.destination_ordinal
    }

    #[must_use]
    pub const fn destination_request_id(&self) -> Uuid {
        self.destination_request_id
    }

    #[must_use]
    pub const fn destination_team_revision(&self) -> u64 {
        self.destination_team_revision
    }

    #[must_use]
    pub const fn destination_capacity_revision(&self) -> u64 {
        self.destination_capacity_revision
    }

    #[must_use]
    pub const fn destination_workspace_leadership_revision(&self) -> i64 {
        self.destination_workspace_leadership_revision
    }

    #[must_use]
    pub const fn source_leader_participation_id(&self) -> WorkspaceParticipationId {
        self.source_leader_participation_id
    }

    #[must_use]
    pub const fn source_leader_profile_id(&self) -> Uuid {
        self.source_leader_profile_id
    }

    #[must_use]
    pub const fn source_membership_revision(&self) -> u64 {
        self.source_membership_revision
    }

    #[must_use]
    pub const fn recorded_at(&self) -> TimestampMillis {
        self.recorded_at
    }

    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }

    #[must_use]
    pub const fn workspace_actor(&self) -> &ActorSnapshot {
        &self.workspace_actor
    }
}

/// The two independent profile-authenticated transfer consent categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeamPermanentTransferConsentKind {
    /// The current leader of the source Home Team approves the move proposal.
    SourceHomeLeader,
    /// The target Profile explicitly accepts this move proposal.
    TargetAgentAcceptance,
}

impl TeamPermanentTransferConsentKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::SourceHomeLeader => "SOURCE_HOME_LEADER",
            Self::TargetAgentAcceptance => "TARGET_AGENT",
        }
    }

    fn decode(value: &str) -> Result<Self, OperationsError> {
        match value {
            "SOURCE_HOME_LEADER" => Ok(Self::SourceHomeLeader),
            "TARGET_AGENT" => Ok(Self::TargetAgentAcceptance),
            _ => Err(OperationsError::Corrupt(
                "permanent Team transfer consent kind",
            )),
        }
    }
}

/// One idempotent consent command. There is one immutable consent per kind
/// and transfer intent; this API defines no withdrawal or re-consent policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamPermanentTransferConsentCommand {
    receipt_id: Uuid,
    workspace_id: WorkspaceId,
    transfer_id: Uuid,
    kind: TeamPermanentTransferConsentKind,
    actor: OperationalActor,
}

impl TeamPermanentTransferConsentCommand {
    /// Construct one exact authenticated consent command.
    ///
    /// # Errors
    /// Rejects nil/aliased receipt identities and non-Profile actors.
    pub fn new(
        receipt_id: Uuid,
        workspace_id: WorkspaceId,
        transfer_id: Uuid,
        kind: TeamPermanentTransferConsentKind,
        actor: OperationalActor,
    ) -> Result<Self, OperationsError> {
        if receipt_id.is_nil()
            || workspace_id.into_uuid().is_nil()
            || transfer_id.is_nil()
            || receipt_id == transfer_id
            || !actor_input_is_valid(actor)
        {
            return Err(OperationsError::Conflict(
                "permanent Team transfer consent identity",
            ));
        }
        Ok(Self {
            receipt_id,
            workspace_id,
            transfer_id,
            kind,
            actor,
        })
    }

    #[must_use]
    pub const fn receipt_id(&self) -> Uuid {
        self.receipt_id
    }

    #[must_use]
    pub const fn transfer_id(&self) -> Uuid {
        self.transfer_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn kind(&self) -> TeamPermanentTransferConsentKind {
        self.kind
    }
}

/// Immutable point-in-time profile consent. The current authority revision
/// is checked again by any future cutover owner; this receipt alone does not
/// keep that authority current.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamPermanentTransferConsentReceipt {
    receipt_id: Uuid,
    transfer_id: Uuid,
    kind: TeamPermanentTransferConsentKind,
    authority_revision: i64,
    membership_revision: u64,
    actor: ActorSnapshot,
    recorded_at: TimestampMillis,
    intent_digest: [u8; 32],
    replayed: bool,
}

impl TeamPermanentTransferConsentReceipt {
    #[must_use]
    pub const fn receipt_id(&self) -> Uuid {
        self.receipt_id
    }

    #[must_use]
    pub const fn transfer_id(&self) -> Uuid {
        self.transfer_id
    }

    #[must_use]
    pub const fn kind(&self) -> TeamPermanentTransferConsentKind {
        self.kind
    }

    #[must_use]
    pub const fn authority_revision(&self) -> i64 {
        self.authority_revision
    }

    #[must_use]
    pub const fn membership_revision(&self) -> u64 {
        self.membership_revision
    }

    #[must_use]
    pub const fn recorded_at(&self) -> TimestampMillis {
        self.recorded_at
    }

    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }

    #[must_use]
    pub const fn actor(&self) -> &ActorSnapshot {
        &self.actor
    }
}

/// Read-only point-in-time view for the current Workspace Orchestrator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamPermanentTransferSnapshot {
    intent: TeamPermanentTransferIntentReceipt,
    source_home_leader_consent: Option<TeamPermanentTransferConsentReceipt>,
    target_agent_acceptance: Option<TeamPermanentTransferConsentReceipt>,
}

impl TeamPermanentTransferSnapshot {
    #[must_use]
    pub const fn intent(&self) -> &TeamPermanentTransferIntentReceipt {
        &self.intent
    }

    #[must_use]
    pub const fn source_home_leader_consent(&self) -> Option<&TeamPermanentTransferConsentReceipt> {
        self.source_home_leader_consent.as_ref()
    }

    #[must_use]
    pub const fn target_agent_acceptance(&self) -> Option<&TeamPermanentTransferConsentReceipt> {
        self.target_agent_acceptance.as_ref()
    }
}

impl OperationsCatalog {
    /// Record the current Orchestrator's exact move proposal against an
    /// approved and cooldown-anchored destination slot.
    ///
    /// The durable rows are an intent plus consent history only; callers must
    /// separately obtain consents and later prove safe work/effect settlement.
    ///
    /// # Errors
    /// Rejects stale leader/team/capacity revisions, a non-current source Home,
    /// a destination slot that is missing, assigned, used or unanchored, and
    /// exact-ID payload conflicts.
    pub fn propose_team_permanent_transfer(
        &mut self,
        command: &TeamPermanentTransferIntentCommand,
        trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
    ) -> Result<TeamPermanentTransferIntentReceipt, OperationsError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        teams::ensure_verified_workspace(&transaction, command.workspace_id)?;
        let actor = ActorSnapshot::resolve(&transaction, command.workspace_id, command.actor)?;

        if let Some(existing) =
            load_intent_in_workspace(&transaction, command.workspace_id, command.transfer_id)?
        {
            if !intent_matches_command(&existing, command, &actor) {
                return Err(OperationsError::Conflict(
                    "permanent Team transfer intent replay",
                ));
            }
            renewal::require_root_generation(&transaction, &existing)?;
            transaction.commit()?;
            return Ok(with_intent_replay(existing));
        }
        if consent_receipt_id_exists(&transaction, command.workspace_id, command.transfer_id)? {
            return Err(OperationsError::Conflict(
                "permanent Team transfer intent identity",
            ));
        }
        let destination_slot_has_intent: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_team_permanent_transfer_intents WHERE workspace_id=?1 AND destination_decision_id=?2 AND destination_ordinal=?3)",
            params![
                command.workspace_id.into_uuid().as_bytes(),
                command.destination_decision_id.as_bytes(),
                i64::from(command.destination_ordinal),
            ],
            |row| row.get(0),
        )?;
        if destination_slot_has_intent {
            return Err(OperationsError::Conflict(
                "permanent Team transfer destination slot already has an intent",
            ));
        }

        let source = resolve_intent_source(&transaction, command, &actor)?;
        let recorded_at = trusted_now()?;
        ensure_transfer_proposal_time_not_regressed(
            &transaction,
            command,
            source.target_profile_id,
            recorded_at,
        )?;
        let receipt = build_intent_receipt(command, actor, &source, recorded_at)?;
        insert_intent(&transaction, &receipt)?;
        generation::create_root(&transaction, receipt.workspace_id, receipt.transfer_id)?;
        validate_live_transfer_intent(&transaction, &receipt)?;
        let commit = transaction.commit();
        match commit {
            Ok(()) => Ok(receipt),
            Err(source) => match renewal::recover_root_after_commit(
                &self.connection,
                command,
                &receipt.workspace_actor,
            ) {
                Ok(Some(existing)) => Ok(with_intent_replay(existing)),
                Ok(None) | Err(_) => Err(OperationsError::OperationCommitUnconfirmed {
                    workspace_id: command.workspace_id,
                    request_id: command.transfer_id,
                    source,
                }),
            },
        }
    }

    /// Record one authenticated, point-in-time source Leader or target Agent
    /// consent for an exact permanent-transfer intent.
    ///
    /// # Errors
    /// Rejects actors outside the exact current source leadership or target
    /// Profile participation, stale source revisions, and duplicate consent
    /// kinds with changed receipt identity.
    pub fn record_team_permanent_transfer_consent(
        &mut self,
        command: &TeamPermanentTransferConsentCommand,
        trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
    ) -> Result<TeamPermanentTransferConsentReceipt, OperationsError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        teams::ensure_verified_workspace(&transaction, command.workspace_id)?;
        let actor = ActorSnapshot::resolve(&transaction, command.workspace_id, command.actor)?;
        let intent =
            load_intent_in_workspace(&transaction, command.workspace_id, command.transfer_id)?
                .ok_or(OperationsError::Conflict(
                    "permanent Team transfer intent missing",
                ))?;

        if let Some(existing) = load_consent(
            &transaction,
            command.workspace_id,
            command.transfer_id,
            command.receipt_id,
        )? {
            if !consent_matches_command(&existing, command, &actor) {
                return Err(OperationsError::Conflict(
                    "permanent Team transfer consent replay",
                ));
            }
            transaction.commit()?;
            return Ok(with_consent_replay(existing));
        }
        if consent_receipt_id_exists(&transaction, command.workspace_id, command.receipt_id)? {
            return Err(OperationsError::Conflict(
                "permanent Team transfer consent receipt identity",
            ));
        }
        if consent_for_kind(
            &transaction,
            command.workspace_id,
            command.transfer_id,
            command.kind,
        )?
        .is_some()
        {
            return Err(OperationsError::Conflict(
                "permanent Team transfer consent already recorded",
            ));
        }
        validate_live_transfer_intent(&transaction, &intent)?;
        let (authority_revision, membership_revision) =
            validate_consent_actor(&transaction, &intent, command.kind, &actor)?;
        let recorded_at = trusted_now()?;
        ensure_consent_time_follows_intent(&intent, recorded_at)?;
        let receipt = TeamPermanentTransferConsentReceipt {
            receipt_id: command.receipt_id,
            transfer_id: command.transfer_id,
            kind: command.kind,
            authority_revision,
            membership_revision,
            actor,
            recorded_at,
            intent_digest: intent.intent_digest,
            replayed: false,
        };
        insert_consent(&transaction, &receipt, intent.workspace_id)?;
        validate_consent_history(&transaction, command.workspace_id, &receipt)?;
        let commit = transaction.commit();
        match commit {
            Ok(()) => Ok(receipt),
            Err(source) => match load_consent(
                &self.connection,
                command.workspace_id,
                command.transfer_id,
                command.receipt_id,
            ) {
                Ok(Some(existing))
                    if consent_matches_command(&existing, command, &receipt.actor) =>
                {
                    Ok(with_consent_replay(existing))
                }
                Ok(Some(_)) => Err(OperationsError::Conflict(
                    "permanent Team transfer consent after unconfirmed commit",
                )),
                Ok(None) | Err(_) => Err(OperationsError::OperationCommitUnconfirmed {
                    workspace_id: intent.workspace_id,
                    request_id: command.receipt_id,
                    source,
                }),
            },
        }
    }

    /// Read an intent and its point-in-time consents as the original proposer
    /// or the currently authorized Workspace Orchestrator.
    ///
    /// # Errors
    /// Rejects malformed identities, invalid actor bindings, corrupt source
    /// history, and storage failures. Unauthorized readers receive `None`.
    pub fn read_team_permanent_transfer_snapshot(
        &mut self,
        workspace_id: WorkspaceId,
        transfer_id: Uuid,
        actor_input: OperationalActor,
    ) -> Result<Option<TeamPermanentTransferSnapshot>, OperationsError> {
        if workspace_id.into_uuid().is_nil()
            || transfer_id.is_nil()
            || !actor_input_is_valid(actor_input)
        {
            return Err(OperationsError::Conflict(
                "permanent Team transfer readback identity",
            ));
        }
        let transaction = self.connection.unchecked_transaction()?;
        teams::ensure_verified_workspace(&transaction, workspace_id)?;
        let actor = ActorSnapshot::resolve(&transaction, workspace_id, actor_input)?;
        let Some(intent) = load_intent_in_workspace(&transaction, workspace_id, transfer_id)?
        else {
            return Ok(None);
        };
        if !intent.workspace_actor.same_logical_actor(&actor) {
            match authorize_current_orchestrator(&transaction, workspace_id, &actor) {
                Ok(_) => {}
                Err(OperationsError::Conflict(_)) => return Ok(None),
                Err(error) => return Err(error),
            }
        }
        let source_home_leader_consent = consent_for_kind(
            &transaction,
            workspace_id,
            transfer_id,
            TeamPermanentTransferConsentKind::SourceHomeLeader,
        )?;
        let target_agent_acceptance = consent_for_kind(
            &transaction,
            workspace_id,
            transfer_id,
            TeamPermanentTransferConsentKind::TargetAgentAcceptance,
        )?;
        let snapshot = TeamPermanentTransferSnapshot {
            intent,
            source_home_leader_consent,
            target_agent_acceptance,
        };
        transaction.commit()?;
        Ok(Some(snapshot))
    }

    /// Read one exact consent receipt as its original stable consentor or the
    /// current Workspace Orchestrator, without exposing sibling source.
    ///
    /// # Errors
    /// Rejects malformed identities, invalid current actor bindings, corrupt
    /// authorized history, and storage failures. Unrelated or absent selectors
    /// return `None`.
    pub fn read_team_permanent_transfer_consent(
        &self,
        workspace_id: WorkspaceId,
        transfer_id: Uuid,
        receipt_id: Uuid,
        actor_input: OperationalActor,
    ) -> Result<Option<TeamPermanentTransferConsentReceipt>, OperationsError> {
        if workspace_id.into_uuid().is_nil()
            || transfer_id.is_nil()
            || receipt_id.is_nil()
            || !actor_input_is_valid(actor_input)
        {
            return Err(OperationsError::Conflict(
                "permanent Team transfer consent readback identity",
            ));
        }
        let transaction = self.connection.unchecked_transaction()?;
        teams::ensure_verified_workspace(&transaction, workspace_id)?;
        let actor = ActorSnapshot::resolve(&transaction, workspace_id, actor_input)?;
        let Some((original_participation, original_profile)) =
            consent_actor_identity(&transaction, workspace_id, transfer_id, receipt_id)?
        else {
            transaction.commit()?;
            return Ok(None);
        };
        let original_consentor = matches!(
            (original_participation, original_profile),
            (Value::Blob(participation), Value::Blob(profile))
                if participation.as_slice() == actor.participation_id.into_uuid().as_bytes()
                    && profile.as_slice() == actor.profile_id.as_bytes()
        );
        if !original_consentor
            && !consent_reader_has_orchestrator_access(authorize_current_orchestrator(
                &transaction,
                workspace_id,
                &actor,
            ))?
        {
            transaction.commit()?;
            return Ok(None);
        }
        let receipt = load_consent(&transaction, workspace_id, transfer_id, receipt_id)?.ok_or(
            OperationsError::Corrupt("permanent Team transfer consent owner source missing"),
        )?;
        transaction.commit()?;
        Ok(Some(receipt))
    }
}

fn consent_reader_has_orchestrator_access(
    authority: Result<i64, OperationsError>,
) -> Result<bool, OperationsError> {
    match authority {
        Ok(_) => Ok(true),
        Err(OperationsError::Conflict(_)) => Ok(false),
        Err(error) => Err(error),
    }
}

fn consent_actor_identity(
    connection: &Connection,
    workspace_id: WorkspaceId,
    transfer_id: Uuid,
    receipt_id: Uuid,
) -> Result<Option<(Value, Value)>, OperationsError> {
    connection
        .query_row(
            "SELECT consent.actor_participation_id,consent.actor_profile_id
             FROM ops_team_permanent_transfer_consents AS consent
             JOIN ops_team_permanent_transfer_intents AS intent
               ON intent.transfer_id=consent.transfer_id
              AND intent.workspace_id=consent.workspace_id
              AND intent.intent_digest=consent.intent_digest
             WHERE consent.workspace_id=?1 AND consent.transfer_id=?2 AND consent.receipt_id=?3",
            params![
                workspace_id.into_uuid().as_bytes(),
                transfer_id.as_bytes(),
                receipt_id.as_bytes(),
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(Into::into)
}

#[derive(Debug, Clone, Copy)]
struct DestinationSlotSource {
    team_revision: u64,
    capacity_revision: u64,
}

struct ResolvedIntentSource {
    target_profile_id: Uuid,
    source_membership_revision: u64,
    source_leadership_revision: i64,
    source_leader_participation_id: WorkspaceParticipationId,
    source_leader_profile_id: Uuid,
    destination: DestinationSlotSource,
    orchestrator_revision: i64,
}

fn resolve_intent_source(
    transaction: &Transaction<'_>,
    command: &TeamPermanentTransferIntentCommand,
    actor: &ActorSnapshot,
) -> Result<ResolvedIntentSource, OperationsError> {
    let orchestrator_revision =
        authorize_current_orchestrator(transaction, command.workspace_id, actor)?;
    if orchestrator_revision != command.expected_workspace_leadership_revision {
        return Err(OperationsError::Conflict(
            "permanent Team transfer Orchestrator revision",
        ));
    }
    let (target_profile_id, source_membership_revision) = current_home_membership(
        transaction,
        command.workspace_id,
        command.source_team_id,
        command.target_participation_id,
    )?;
    preparation::ensure_profile_open(transaction, command.workspace_id, target_profile_id)?;
    let source_authority = current_authority(
        transaction,
        command.workspace_id,
        Some(command.source_team_id),
    )?
    .ok_or(OperationsError::Conflict(
        "permanent Team transfer source Leader missing",
    ))?;
    let (source_leader_participation_id, source_leader_profile_id) = participant_identity(
        transaction,
        command.workspace_id,
        source_authority.participant_id(),
    )?;
    if source_leader_participation_id == command.target_participation_id
        || source_leader_profile_id == target_profile_id
    {
        return Err(OperationsError::Conflict(
            "permanent Team transfer Leader must complete succession first",
        ));
    }
    if source_membership_revision != command.expected_source_membership_revision {
        return Err(OperationsError::Conflict(
            "permanent Team transfer source membership revision",
        ));
    }
    let source_leadership_revision = positive_revision(
        source_authority.leadership_revision(),
        "permanent Team transfer source leadership revision",
    )?;
    if source_leadership_revision != command.expected_source_leadership_revision {
        return Err(OperationsError::Conflict(
            "permanent Team transfer source leadership revision",
        ));
    }
    let destination = verify_destination_slot(
        transaction,
        command,
        target_profile_id,
        command.target_participation_id.into_uuid(),
    )?;
    Ok(ResolvedIntentSource {
        target_profile_id,
        source_membership_revision,
        source_leadership_revision,
        source_leader_participation_id,
        source_leader_profile_id,
        destination,
        orchestrator_revision,
    })
}

fn build_intent_receipt(
    command: &TeamPermanentTransferIntentCommand,
    actor: ActorSnapshot,
    source: &ResolvedIntentSource,
    recorded_at: TimestampMillis,
) -> Result<TeamPermanentTransferIntentReceipt, OperationsError> {
    let mut receipt = TeamPermanentTransferIntentReceipt {
        transfer_id: command.transfer_id,
        workspace_id: command.workspace_id,
        source_team_id: command.source_team_id,
        destination_team_id: command.destination_team_id,
        target_participation_id: command.target_participation_id,
        target_profile_id: source.target_profile_id,
        source_membership_revision: source.source_membership_revision,
        source_leadership_revision: source.source_leadership_revision,
        source_leader_participation_id: source.source_leader_participation_id,
        source_leader_profile_id: source.source_leader_profile_id,
        destination_team_revision: source.destination.team_revision,
        destination_capacity_revision: source.destination.capacity_revision,
        destination_workspace_leadership_revision: source.orchestrator_revision,
        destination_request_id: command.destination_request_id,
        destination_decision_id: command.destination_decision_id,
        destination_ordinal: command.destination_ordinal,
        workspace_actor: actor,
        recorded_at,
        intent_digest: [0_u8; 32],
        replayed: false,
    };
    receipt.intent_digest = intent_digest_from_receipt(&receipt)?;
    Ok(receipt)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TransferSlotSource {
    workspace_id: WorkspaceId,
    destination_team_id: Uuid,
    decision_id: Uuid,
    request_id: Uuid,
    ordinal: u8,
    target_profile_id: Uuid,
    target_participation_id: Uuid,
    source_team_id: Uuid,
    source_membership_revision: u64,
}

impl TransferSlotSource {
    fn from_command(command: &TeamPermanentTransferIntentCommand, target_profile_id: Uuid) -> Self {
        Self {
            workspace_id: command.workspace_id,
            destination_team_id: command.destination_team_id,
            decision_id: command.destination_decision_id,
            request_id: command.destination_request_id,
            ordinal: command.destination_ordinal,
            target_profile_id,
            target_participation_id: command.target_participation_id.into_uuid(),
            source_team_id: command.source_team_id,
            source_membership_revision: command.expected_source_membership_revision,
        }
    }

    fn from_intent(intent: &TeamPermanentTransferIntentReceipt) -> Self {
        Self {
            workspace_id: intent.workspace_id,
            destination_team_id: intent.destination_team_id,
            decision_id: intent.destination_decision_id,
            request_id: intent.destination_request_id,
            ordinal: intent.destination_ordinal,
            target_profile_id: intent.target_profile_id,
            target_participation_id: intent.target_participation_id.into_uuid(),
            source_team_id: intent.source_team_id,
            source_membership_revision: intent.source_membership_revision,
        }
    }
}

fn actor_input_is_valid(actor: OperationalActor) -> bool {
    match actor {
        OperationalActor::LocalOperator => false,
        OperationalActor::Participant {
            activity_id,
            agent_id,
            session_id,
        } => {
            !activity_id.into_uuid().is_nil()
                && !agent_id.into_uuid().is_nil()
                && !session_id.into_uuid().is_nil()
        }
    }
}

fn authorize_current_orchestrator(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    actor: &ActorSnapshot,
) -> Result<i64, OperationsError> {
    let authority = current_authority(transaction, workspace_id, None)?.ok_or(
        OperationsError::Conflict("permanent Team transfer requires current Orchestrator"),
    )?;
    if authority.participant_id() != actor.participant_id {
        return Err(OperationsError::Conflict(
            "permanent Team transfer Orchestrator actor",
        ));
    }
    positive_revision(
        authority.leadership_revision(),
        "permanent Team transfer Orchestrator revision",
    )
}

fn current_authority(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    team_id: Option<Uuid>,
) -> Result<Option<CurrentLeadershipAuthority>, OperationsError> {
    let scope = team_id.map_or_else(
        || LeadershipScope::workspace(workspace_id),
        |team| LeadershipScope::team(workspace_id, team),
    );
    current_leadership_authority_in_transaction(transaction, scope)
}

fn participant_identity(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
) -> Result<(WorkspaceParticipationId, Uuid), OperationsError> {
    let (count, participation, profile): (i64, Option<Vec<u8>>, Option<Vec<u8>>) = connection
        .query_row(
            "SELECT COUNT(DISTINCT mapping.participation_id),MIN(mapping.participation_id),MIN(mapping.agent_profile_id) FROM ops_workspace_participation_mappings AS mapping JOIN ops_workspace_participations AS participation ON participation.workspace_id=mapping.workspace_id AND participation.participation_id=mapping.participation_id AND participation.agent_profile_id=mapping.agent_profile_id WHERE mapping.workspace_id=?1 AND mapping.participant_id=?2",
            params![workspace_id.into_uuid().as_bytes(), participant_id.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
    if count != 1 {
        return Err(OperationsError::Conflict(
            "permanent Team transfer leader Profile identity is missing or ambiguous",
        ));
    }
    Ok((
        WorkspaceParticipationId::from_uuid(decode_id(
            participation.ok_or(OperationsError::Corrupt(
                "permanent Team transfer leader participation",
            ))?,
            "permanent Team transfer leader participation",
        )?)?,
        decode_id(
            profile.ok_or(OperationsError::Corrupt(
                "permanent Team transfer leader Profile",
            ))?,
            "permanent Team transfer leader Profile",
        )?,
    ))
}

fn current_home_membership(
    connection: &Connection,
    workspace_id: WorkspaceId,
    source_team_id: Uuid,
    participation_id: WorkspaceParticipationId,
) -> Result<(Uuid, u64), OperationsError> {
    let profile_bytes = connection
        .query_row(
            "SELECT agent_profile_id FROM ops_workspace_participations WHERE workspace_id=?1 AND participation_id=?2",
            params![workspace_id.into_uuid().as_bytes(), participation_id.into_uuid().as_bytes()],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?
        .ok_or(OperationsError::Conflict(
            "permanent Team transfer target participation missing",
        ))?;
    let profile_id = decode_id(profile_bytes, "permanent Team transfer target Profile")?;
    let (home_count, source_count, source_revision): (i64, i64, Option<i64>) = connection
        .query_row(
            "SELECT COUNT(DISTINCT member.team_id),COUNT(DISTINCT CASE WHEN member.team_id=?3 THEN member.team_id END),MIN(CASE WHEN member.team_id=?3 THEN member.revision END) FROM ops_team_members AS member JOIN ops_teams AS home ON home.workspace_id=member.workspace_id AND home.team_id=member.team_id AND home.state='ACTIVE' JOIN ops_workspace_participations AS participation ON participation.workspace_id=member.workspace_id AND participation.participation_id=member.participation_id WHERE member.workspace_id=?1 AND participation.agent_profile_id=?2 AND member.state='JOINED'",
            params![workspace_id.into_uuid().as_bytes(), profile_id.as_bytes(), source_team_id.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
    if home_count != 1 || source_count != 1 {
        return Err(OperationsError::Conflict(
            "permanent Team transfer target must have exactly one current Home Team",
        ));
    }
    let membership_revision = u64::try_from(source_revision.ok_or(OperationsError::Corrupt(
        "permanent Team transfer source membership revision",
    ))?)
    .map_err(|_| OperationsError::Corrupt("permanent Team transfer source membership"))?;
    if membership_revision == 0 {
        return Err(OperationsError::Corrupt(
            "permanent Team transfer source membership revision",
        ));
    }
    Ok((profile_id, membership_revision))
}

fn verify_destination_slot(
    connection: &Connection,
    command: &TeamPermanentTransferIntentCommand,
    target_profile_id: Uuid,
    target_participation_id: Uuid,
) -> Result<DestinationSlotSource, OperationsError> {
    let (decision_state, approved_count, slot_request, anchor_present, assignment_present, used):
        (String, i64, Vec<u8>, bool, bool, bool) = connection.query_row(
        "SELECT decision.state,decision.approved_count,slot.request_id,EXISTS(SELECT 1 FROM ops_team_expansion_cooldown_anchors AS anchor WHERE anchor.workspace_id=decision.workspace_id AND anchor.team_id=decision.team_id AND anchor.decision_id=decision.decision_id AND anchor.request_id=decision.request_id),EXISTS(SELECT 1 FROM ops_team_expansion_slot_assignments AS assignment WHERE assignment.workspace_id=slot.workspace_id AND assignment.team_id=slot.team_id AND assignment.decision_id=slot.decision_id AND assignment.ordinal=slot.ordinal),EXISTS(SELECT 1 FROM ops_team_expansion_slot_uses AS used WHERE used.workspace_id=slot.workspace_id AND used.team_id=slot.team_id AND used.decision_id=slot.decision_id AND used.ordinal=slot.ordinal) FROM ops_team_expansion_slots AS slot JOIN ops_team_expansion_decisions AS decision ON decision.workspace_id=slot.workspace_id AND decision.team_id=slot.team_id AND decision.decision_id=slot.decision_id AND decision.request_id=slot.request_id WHERE slot.workspace_id=?1 AND slot.team_id=?2 AND slot.decision_id=?3 AND slot.request_id=?4 AND slot.ordinal=?5",
        params![
            command.workspace_id.into_uuid().as_bytes(),
            command.destination_team_id.as_bytes(),
            command.destination_decision_id.as_bytes(),
            command.destination_request_id.as_bytes(),
            i64::from(command.destination_ordinal),
        ],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        },
    )?;
    if decision_state != "APPROVED"
        || approved_count <= i64::from(command.destination_ordinal)
        || decode_id(slot_request, "permanent Team transfer destination request")?
            != command.destination_request_id
        || !anchor_present
        || assignment_present
        || used
    {
        return Err(OperationsError::Conflict(
            "permanent Team transfer destination slot is unavailable",
        ));
    }
    let approved_count = u8::try_from(approved_count)
        .map_err(|_| OperationsError::Corrupt("Team expansion approved count"))?;
    let source = TransferSlotSource::from_command(command, target_profile_id);
    if source.target_participation_id != target_participation_id {
        return Err(OperationsError::Conflict(
            "permanent Team transfer target participation changed",
        ));
    }
    verify_workspace_existing_target_plan(connection, source, approved_count)?;
    verify_v3_existing_home_source(connection, source)?;
    let (team_revision, active) = current_team_revision_and_active(
        connection,
        command.workspace_id,
        command.destination_team_id,
    )?;
    if !active || team_revision != command.expected_destination_team_revision {
        return Err(OperationsError::Conflict(
            "permanent Team transfer destination revision",
        ));
    }
    let (capacity_revision, max_members): (i64, i64) = connection
        .query_row(
            "SELECT capacity_revision,max_members FROM ops_team_capacity_events WHERE workspace_id=?1 AND team_id=?2 ORDER BY capacity_revision DESC LIMIT 1",
            params![command.workspace_id.into_uuid().as_bytes(), command.destination_team_id.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .ok_or(OperationsError::Conflict(
            "permanent Team transfer destination capacity is not configured",
        ))?;
    if u64::try_from(capacity_revision).ok() != Some(command.expected_destination_capacity_revision)
        || !(0..=8).contains(&max_members)
    {
        return Err(OperationsError::Conflict(
            "permanent Team transfer destination capacity revision",
        ));
    }
    Ok(DestinationSlotSource {
        team_revision,
        capacity_revision: command.expected_destination_capacity_revision,
    })
}

fn current_team_revision_and_active(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<(u64, bool), OperationsError> {
    let (revision, state): (i64, String) = connection
        .query_row(
            "SELECT revision,state FROM ops_teams WHERE workspace_id=?1 AND team_id=?2",
            params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .ok_or(OperationsError::Conflict("permanent transfer Team missing"))?;
    let revision = positive_u64(revision, "permanent transfer Team revision")?;
    match state.as_str() {
        "ACTIVE" => Ok((revision, true)),
        "RETIRED" => Ok((revision, false)),
        _ => Err(OperationsError::Corrupt("permanent transfer Team state")),
    }
}

fn validate_live_transfer_intent(
    connection: &Transaction<'_>,
    intent: &TeamPermanentTransferIntentReceipt,
) -> Result<(), OperationsError> {
    let stored = load_intent_in_workspace(connection, intent.workspace_id, intent.transfer_id)?
        .ok_or(OperationsError::Corrupt(
            "permanent Team transfer intent missing",
        ))?;
    if stored != *intent {
        return Err(OperationsError::Corrupt(
            "permanent Team transfer intent history",
        ));
    }
    renewal::require_active_generation(connection, intent)?;
    let source = current_home_membership(
        connection,
        intent.workspace_id,
        intent.source_team_id,
        intent.target_participation_id,
    )?;
    if source.0 != intent.target_profile_id || source.1 != intent.source_membership_revision {
        return Err(OperationsError::Conflict(
            "permanent Team transfer source membership changed",
        ));
    }
    let authority =
        current_authority(connection, intent.workspace_id, Some(intent.source_team_id))?.ok_or(
            OperationsError::Conflict("permanent Team transfer source Leader missing"),
        )?;
    let (current_leader_participation, current_leader_profile) =
        participant_identity(connection, intent.workspace_id, authority.participant_id())?;
    if !source_authority_matches_intent(
        intent,
        authority.leadership_revision(),
        current_leader_participation,
        current_leader_profile,
    ) {
        return Err(OperationsError::Conflict(
            "permanent Team transfer source leadership changed",
        ));
    }
    let source = TransferSlotSource::from_intent(intent);
    verify_workspace_existing_target_plan(
        connection,
        source,
        approved_count_for_slot(connection, intent)?,
    )?;
    verify_v3_existing_home_source(connection, source)?;
    let slot_assigned_or_used: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_team_expansion_slot_assignments AS assignment WHERE assignment.workspace_id=?1 AND assignment.team_id=?2 AND assignment.decision_id=?3 AND assignment.ordinal=?4) OR EXISTS(SELECT 1 FROM ops_team_expansion_slot_uses AS used WHERE used.workspace_id=?1 AND used.team_id=?2 AND used.decision_id=?3 AND used.ordinal=?4)",
        params![
            intent.workspace_id.into_uuid().as_bytes(),
            intent.destination_team_id.as_bytes(),
            intent.destination_decision_id.as_bytes(),
            i64::from(intent.destination_ordinal),
        ],
        |row| row.get(0),
    )?;
    if slot_assigned_or_used {
        return Err(OperationsError::Conflict(
            "permanent Team transfer destination slot is no longer unassigned",
        ));
    }
    Ok(())
}

fn source_authority_matches_intent(
    intent: &TeamPermanentTransferIntentReceipt,
    current_leadership_revision: i64,
    current_leader_participation: WorkspaceParticipationId,
    current_leader_profile: Uuid,
) -> bool {
    source_leader_identity_matches(
        intent.source_leadership_revision,
        intent.source_leader_participation_id,
        intent.source_leader_profile_id,
        current_leadership_revision,
        current_leader_participation,
        current_leader_profile,
    )
}

fn source_leader_identity_matches(
    expected_leadership_revision: i64,
    expected_participation_id: WorkspaceParticipationId,
    expected_profile_id: Uuid,
    current_leadership_revision: i64,
    current_participation_id: WorkspaceParticipationId,
    current_profile_id: Uuid,
) -> bool {
    current_leadership_revision == expected_leadership_revision
        && current_participation_id == expected_participation_id
        && current_profile_id == expected_profile_id
}

struct V3ExistingHomeSourceRaw {
    profile: Vec<u8>,
    participation: Vec<u8>,
    source_team: Vec<u8>,
    source_revision: i64,
    slot_count: i64,
    transfer_count: i64,
}

fn verify_v3_existing_home_source(
    connection: &Connection,
    expected: TransferSlotSource,
) -> Result<(), OperationsError> {
    let source = connection
        .query_row(
            "SELECT transfer.target_profile_id,transfer.target_participation_id,transfer.source_team_id,transfer.source_membership_revision,manifest.slot_count,manifest.transfer_count FROM ops_team_expansion_existing_home_transfers AS transfer JOIN ops_team_expansion_v3_manifests AS manifest ON manifest.workspace_id=transfer.workspace_id AND manifest.team_id=transfer.destination_team_id AND manifest.decision_id=transfer.decision_id WHERE transfer.workspace_id=?1 AND transfer.destination_team_id=?2 AND transfer.decision_id=?3 AND manifest.request_id=?4 AND transfer.ordinal=?5",
            params![
                expected.workspace_id.into_uuid().as_bytes(),
                expected.destination_team_id.as_bytes(),
                expected.decision_id.as_bytes(),
                expected.request_id.as_bytes(),
                i64::from(expected.ordinal),
            ],
            |row| {
                Ok(V3ExistingHomeSourceRaw {
                    profile: row.get(0)?,
                    participation: row.get(1)?,
                    source_team: row.get(2)?,
                    source_revision: row.get(3)?,
                    slot_count: row.get(4)?,
                    transfer_count: row.get(5)?,
                })
            },
        )
        .optional()?;
    let Some(source) = source else {
        return Err(OperationsError::Conflict(
            "permanent Team transfer V3 source approval missing",
        ));
    };
    if decode_id(source.profile, "permanent Team transfer V3 target Profile")?
        != expected.target_profile_id
        || decode_id(
            source.participation,
            "permanent Team transfer V3 target participation",
        )? != expected.target_participation_id
        || decode_id(source.source_team, "permanent Team transfer V3 source Team")?
            != expected.source_team_id
        || positive_u64(
            source.source_revision,
            "permanent Team transfer V3 source revision",
        )? != expected.source_membership_revision
        || !(1_i64..=8_i64).contains(&source.slot_count)
        || !(1_i64..=source.slot_count).contains(&source.transfer_count)
    {
        return Err(OperationsError::Conflict(
            "permanent Team transfer source differs from V3 approval",
        ));
    }
    let (_, source_team_active) = current_team_revision_and_active(
        connection,
        expected.workspace_id,
        expected.source_team_id,
    )?;
    if !source_team_active {
        return Err(OperationsError::Conflict(
            "permanent Team transfer source Home Team is not active",
        ));
    }
    Ok(())
}

fn ensure_transfer_proposal_time_not_regressed(
    connection: &Connection,
    command: &TeamPermanentTransferIntentCommand,
    target_profile_id: Uuid,
    recorded_at: TimestampMillis,
) -> Result<(), OperationsError> {
    let source_tail: (i64, i64, i64, i64) = connection
        .query_row(
            "SELECT member.joined_at_ms,source_team.updated_at_ms,manifest.recorded_at_ms,anchor.anchor_at_ms FROM ops_team_expansion_existing_home_transfers AS transfer JOIN ops_team_expansion_v3_manifests AS manifest ON manifest.workspace_id=transfer.workspace_id AND manifest.team_id=transfer.destination_team_id AND manifest.decision_id=transfer.decision_id AND manifest.request_id=?4 JOIN ops_team_expansion_cooldown_anchors AS anchor ON anchor.workspace_id=transfer.workspace_id AND anchor.team_id=transfer.destination_team_id AND anchor.decision_id=transfer.decision_id AND anchor.request_id=manifest.request_id JOIN ops_team_members AS member ON member.workspace_id=transfer.workspace_id AND member.team_id=transfer.source_team_id AND member.participation_id=transfer.target_participation_id JOIN ops_teams AS source_team ON source_team.workspace_id=member.workspace_id AND source_team.team_id=member.team_id JOIN ops_workspace_participations AS participation ON participation.workspace_id=member.workspace_id AND participation.participation_id=member.participation_id WHERE transfer.workspace_id=?1 AND transfer.destination_team_id=?2 AND transfer.decision_id=?3 AND transfer.ordinal=?5 AND transfer.target_profile_id=?6 AND transfer.target_participation_id=?7 AND transfer.source_team_id=?8 AND transfer.source_membership_revision=?9 AND member.state='JOINED' AND member.revision=?9 AND participation.agent_profile_id=?6 AND source_team.state='ACTIVE'",
            params![
                command.workspace_id.into_uuid().as_bytes(),
                command.destination_team_id.as_bytes(),
                command.destination_decision_id.as_bytes(),
                command.destination_request_id.as_bytes(),
                i64::from(command.destination_ordinal),
                target_profile_id.as_bytes(),
                command.target_participation_id.into_uuid().as_bytes(),
                command.source_team_id.as_bytes(),
                to_i64(
                    command.expected_source_membership_revision,
                    "permanent Team transfer source membership revision",
                )?,
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?
        .ok_or(OperationsError::Conflict(
            "permanent Team transfer source timeline missing",
        ))?;
    let mut latest_source_event = 0_i64;
    for timestamp in [source_tail.0, source_tail.1, source_tail.2, source_tail.3] {
        TimestampMillis::new(timestamp)
            .map_err(|_| OperationsError::Corrupt("permanent Team transfer source timestamp"))?;
        latest_source_event = latest_source_event.max(timestamp);
    }
    if recorded_at.get() < latest_source_event {
        return Err(OperationsError::Conflict(
            "permanent Team transfer trusted clock regressed",
        ));
    }
    Ok(())
}

fn approved_count_for_slot(
    connection: &Connection,
    intent: &TeamPermanentTransferIntentReceipt,
) -> Result<u8, OperationsError> {
    let approved_count: i64 = connection.query_row(
        "SELECT approved_count FROM ops_team_expansion_decisions WHERE workspace_id=?1 AND team_id=?2 AND decision_id=?3 AND request_id=?4 AND state='APPROVED'",
        params![
            intent.workspace_id.into_uuid().as_bytes(),
            intent.destination_team_id.as_bytes(),
            intent.destination_decision_id.as_bytes(),
            intent.destination_request_id.as_bytes(),
        ],
        |row| row.get(0),
    )?;
    u8::try_from(approved_count)
        .map_err(|_| OperationsError::Corrupt("Team expansion approved count"))
}

fn verify_workspace_existing_target_plan(
    connection: &Connection,
    expected: TransferSlotSource,
    approved_count: u8,
) -> Result<(), OperationsError> {
    // No legacy or unmanifested approval can authorize a permanent transfer.
    // The exact existing Profile and its participation must have been frozen
    // by the approved Workspace hold plan.
    let plan = connection
        .query_row(
            "SELECT manifest.plan_version,manifest.slot_count,plan.target_kind,plan.target_profile_id,plan.target_participation_id FROM ops_team_expansion_workspace_hold_manifests AS manifest JOIN ops_team_expansion_workspace_hold_plans AS plan ON plan.workspace_id=manifest.workspace_id AND plan.team_id=manifest.team_id AND plan.decision_id=manifest.decision_id WHERE manifest.workspace_id=?1 AND manifest.team_id=?2 AND manifest.decision_id=?3 AND manifest.request_id=?4 AND plan.ordinal=?5",
            params![
                expected.workspace_id.into_uuid().as_bytes(),
                expected.destination_team_id.as_bytes(),
                expected.decision_id.as_bytes(),
                expected.request_id.as_bytes(),
                i64::from(expected.ordinal),
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<Vec<u8>>>(3)?,
                    row.get::<_, Option<Vec<u8>>>(4)?,
                ))
            },
        )
        .optional()?
        .ok_or(OperationsError::Conflict(
            "permanent Team transfer requires an exact Workspace approval target plan",
        ))?;
    let (plan_count, first_ordinal, last_ordinal): (i64, Option<i64>, Option<i64>) = connection.query_row(
        "SELECT COUNT(*),MIN(ordinal),MAX(ordinal) FROM ops_team_expansion_workspace_hold_plans WHERE workspace_id=?1 AND team_id=?2 AND decision_id=?3",
        params![
            expected.workspace_id.into_uuid().as_bytes(),
            expected.destination_team_id.as_bytes(),
            expected.decision_id.as_bytes(),
        ],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    let planned_profile = plan
        .3
        .map(|bytes| decode_id(bytes, "transfer target plan Profile"))
        .transpose()?;
    let planned_participation = plan
        .4
        .map(|bytes| decode_id(bytes, "transfer target plan participation"))
        .transpose()?;
    if plan.0 != 1
        || plan.1 != i64::from(approved_count)
        || plan_count != i64::from(approved_count)
        || first_ordinal != Some(0)
        || last_ordinal != Some(i64::from(approved_count) - 1)
        || plan.2 != 0
        || planned_profile != Some(expected.target_profile_id)
        || planned_participation != Some(expected.target_participation_id)
    {
        return Err(OperationsError::Conflict(
            "permanent Team transfer target differs from approved Workspace plan",
        ));
    }
    Ok(())
}

fn validate_consent_actor(
    transaction: &Transaction<'_>,
    intent: &TeamPermanentTransferIntentReceipt,
    kind: TeamPermanentTransferConsentKind,
    actor: &ActorSnapshot,
) -> Result<(i64, u64), OperationsError> {
    validate_live_transfer_intent(transaction, intent)?;
    match kind {
        TeamPermanentTransferConsentKind::SourceHomeLeader => {
            let authority = current_authority(
                transaction,
                intent.workspace_id,
                Some(intent.source_team_id),
            )?
            .ok_or(OperationsError::Conflict(
                "permanent Team transfer source Leader missing",
            ))?;
            if authority.leadership_revision() != intent.source_leadership_revision
                || authority.participant_id() != actor.participant_id
                || actor.participation_id != intent.source_leader_participation_id
                || actor.profile_id != intent.source_leader_profile_id
            {
                return Err(OperationsError::Conflict(
                    "permanent Team transfer requires current source Home Leader",
                ));
            }
            Ok((
                authority.leadership_revision(),
                intent.source_membership_revision,
            ))
        }
        TeamPermanentTransferConsentKind::TargetAgentAcceptance => {
            if actor.participation_id != intent.target_participation_id
                || actor.profile_id != intent.target_profile_id
            {
                return Err(OperationsError::Conflict(
                    "permanent Team transfer target Agent acceptance actor",
                ));
            }
            Ok((
                to_i64(
                    intent.source_membership_revision,
                    "permanent transfer target membership revision",
                )?,
                intent.source_membership_revision,
            ))
        }
    }
}

fn ensure_consent_time_follows_intent(
    intent: &TeamPermanentTransferIntentReceipt,
    recorded_at: TimestampMillis,
) -> Result<(), OperationsError> {
    if recorded_at < intent.recorded_at {
        return Err(OperationsError::Conflict(
            "permanent Team transfer consent time regression",
        ));
    }
    Ok(())
}

fn insert_intent(
    transaction: &Transaction<'_>,
    intent: &TeamPermanentTransferIntentReceipt,
) -> Result<(), OperationsError> {
    let actor = &intent.workspace_actor;
    transaction.execute(
        "INSERT INTO ops_team_permanent_transfer_intents(transfer_id,workspace_id,source_team_id,destination_team_id,source_membership_revision,source_leadership_revision,source_leader_participation_id,source_leader_profile_id,destination_team_revision,destination_capacity_revision,destination_workspace_leadership_revision,target_participation_id,target_profile_id,destination_request_id,destination_decision_id,destination_ordinal,workspace_actor_activity_id,workspace_actor_agent_id,workspace_actor_session_id,workspace_actor_participation_id,workspace_actor_profile_id,workspace_actor_participant_id,workspace_actor_display_name,recorded_at_ms,intent_digest) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25)",
        params![
            intent.transfer_id.as_bytes(),
            intent.workspace_id.into_uuid().as_bytes(),
            intent.source_team_id.as_bytes(),
            intent.destination_team_id.as_bytes(),
            to_i64(intent.source_membership_revision, "permanent transfer source revision")?,
            intent.source_leadership_revision,
            intent.source_leader_participation_id.into_uuid().as_bytes(),
            intent.source_leader_profile_id.as_bytes(),
            to_i64(intent.destination_team_revision, "permanent transfer destination revision")?,
            to_i64(intent.destination_capacity_revision, "permanent transfer capacity revision")?,
            intent.destination_workspace_leadership_revision,
            intent.target_participation_id.into_uuid().as_bytes(),
            intent.target_profile_id.as_bytes(),
            intent.destination_request_id.as_bytes(),
            intent.destination_decision_id.as_bytes(),
            i64::from(intent.destination_ordinal),
            actor.activity_id.into_uuid().as_bytes(),
            actor.agent_id.into_uuid().as_bytes(),
            actor.session_id.into_uuid().as_bytes(),
            actor.participation_id.into_uuid().as_bytes(),
            actor.profile_id.as_bytes(),
            actor.participant_id.as_bytes(),
            &actor.display_name,
            intent.recorded_at.get(),
            intent.intent_digest.as_slice(),
        ],
    )?;
    Ok(())
}

fn insert_consent(
    transaction: &Transaction<'_>,
    consent: &TeamPermanentTransferConsentReceipt,
    workspace_id: WorkspaceId,
) -> Result<(), OperationsError> {
    let actor = &consent.actor;
    transaction.execute(
        "INSERT INTO ops_team_permanent_transfer_consents(receipt_id,transfer_id,workspace_id,consent_kind,authority_revision,membership_revision,actor_activity_id,actor_agent_id,actor_session_id,actor_participation_id,actor_profile_id,actor_participant_id,actor_display_name,recorded_at_ms,intent_digest) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
        params![
            consent.receipt_id.as_bytes(),
            consent.transfer_id.as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            consent.kind.as_str(),
            consent.authority_revision,
            to_i64(consent.membership_revision, "permanent transfer consent membership")?,
            actor.activity_id.into_uuid().as_bytes(),
            actor.agent_id.into_uuid().as_bytes(),
            actor.session_id.into_uuid().as_bytes(),
            actor.participation_id.into_uuid().as_bytes(),
            actor.profile_id.as_bytes(),
            actor.participant_id.as_bytes(),
            &actor.display_name,
            consent.recorded_at.get(),
            consent.intent_digest.as_slice(),
        ],
    )?;
    Ok(())
}

fn load_intent_in_workspace(
    connection: &Connection,
    workspace_id: WorkspaceId,
    transfer_id: Uuid,
) -> Result<Option<TeamPermanentTransferIntentReceipt>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT workspace_id,source_team_id,destination_team_id,target_participation_id,target_profile_id,source_membership_revision,source_leadership_revision,source_leader_participation_id,source_leader_profile_id,destination_team_revision,destination_capacity_revision,destination_workspace_leadership_revision,destination_request_id,destination_decision_id,destination_ordinal,workspace_actor_activity_id,workspace_actor_agent_id,workspace_actor_session_id,workspace_actor_participation_id,workspace_actor_profile_id,workspace_actor_participant_id,workspace_actor_display_name,recorded_at_ms,intent_digest FROM ops_team_permanent_transfer_intents WHERE workspace_id=?1 AND transfer_id=?2",
            params![workspace_id.into_uuid().as_bytes(), transfer_id.as_bytes()],
            decode_intent_raw,
        )
        .optional()?;
    raw.map(|raw| raw.into_receipt(transfer_id)).transpose()
}

fn decode_intent_raw(row: &rusqlite::Row<'_>) -> rusqlite::Result<IntentRaw> {
    Ok(IntentRaw {
        workspace_id: row.get(0)?,
        source_team_id: row.get(1)?,
        destination_team_id: row.get(2)?,
        target_participation_id: row.get(3)?,
        target_profile_id: row.get(4)?,
        source_membership_revision: row.get(5)?,
        source_leadership_revision: row.get(6)?,
        source_leader_participation_id: row.get(7)?,
        source_leader_profile_id: row.get(8)?,
        destination_team_revision: row.get(9)?,
        destination_capacity_revision: row.get(10)?,
        destination_workspace_leadership_revision: row.get(11)?,
        destination_request_id: row.get(12)?,
        destination_decision_id: row.get(13)?,
        destination_ordinal: row.get(14)?,
        workspace_actor: read_actor_raw(row, 15)?,
        recorded_at_ms: row.get(22)?,
        intent_digest: row.get(23)?,
    })
}

struct IntentRaw {
    workspace_id: Vec<u8>,
    source_team_id: Vec<u8>,
    destination_team_id: Vec<u8>,
    target_participation_id: Vec<u8>,
    target_profile_id: Vec<u8>,
    source_membership_revision: i64,
    source_leadership_revision: i64,
    source_leader_participation_id: Vec<u8>,
    source_leader_profile_id: Vec<u8>,
    destination_team_revision: i64,
    destination_capacity_revision: i64,
    destination_workspace_leadership_revision: i64,
    destination_request_id: Vec<u8>,
    destination_decision_id: Vec<u8>,
    destination_ordinal: i64,
    workspace_actor: ActorRaw,
    recorded_at_ms: i64,
    intent_digest: Vec<u8>,
}

impl IntentRaw {
    fn into_receipt(
        self,
        transfer_id: Uuid,
    ) -> Result<TeamPermanentTransferIntentReceipt, OperationsError> {
        let receipt = TeamPermanentTransferIntentReceipt {
            transfer_id,
            workspace_id: WorkspaceId::from_uuid(decode_id(
                self.workspace_id,
                "permanent Team transfer Workspace",
            )?),
            source_team_id: decode_id(self.source_team_id, "permanent transfer source Team")?,
            destination_team_id: decode_id(
                self.destination_team_id,
                "permanent transfer destination Team",
            )?,
            target_participation_id: WorkspaceParticipationId::from_uuid(decode_id(
                self.target_participation_id,
                "permanent transfer target participation",
            )?)?,
            target_profile_id: decode_id(
                self.target_profile_id,
                "permanent transfer target Profile",
            )?,
            source_membership_revision: positive_u64(
                self.source_membership_revision,
                "permanent transfer source membership revision",
            )?,
            source_leadership_revision: positive_revision(
                self.source_leadership_revision,
                "permanent transfer source leadership revision",
            )?,
            source_leader_participation_id: WorkspaceParticipationId::from_uuid(decode_id(
                self.source_leader_participation_id,
                "permanent transfer source Leader participation",
            )?)?,
            source_leader_profile_id: decode_id(
                self.source_leader_profile_id,
                "permanent transfer source Leader Profile",
            )?,
            destination_team_revision: positive_u64(
                self.destination_team_revision,
                "permanent transfer destination Team revision",
            )?,
            destination_capacity_revision: positive_u64(
                self.destination_capacity_revision,
                "permanent transfer destination capacity revision",
            )?,
            destination_workspace_leadership_revision: positive_revision(
                self.destination_workspace_leadership_revision,
                "permanent transfer Workspace leadership revision",
            )?,
            destination_request_id: decode_id(
                self.destination_request_id,
                "permanent transfer destination request",
            )?,
            destination_decision_id: decode_id(
                self.destination_decision_id,
                "permanent transfer destination decision",
            )?,
            destination_ordinal: u8::try_from(self.destination_ordinal)
                .map_err(|_| OperationsError::Corrupt("permanent transfer destination ordinal"))?,
            workspace_actor: ActorSnapshot::decode(self.workspace_actor)?,
            recorded_at: TimestampMillis::new(self.recorded_at_ms)
                .map_err(|_| OperationsError::Corrupt("permanent transfer timestamp"))?,
            intent_digest: digest32(self.intent_digest, "permanent transfer intent digest")?,
            replayed: false,
        };
        if receipt.destination_ordinal > MAX_ORDINAL
            || receipt.source_team_id == receipt.destination_team_id
            || receipt.workspace_id.into_uuid().is_nil()
        {
            return Err(OperationsError::Corrupt("permanent transfer intent values"));
        }
        if intent_digest_from_receipt(&receipt)? != receipt.intent_digest {
            return Err(OperationsError::Corrupt("permanent transfer intent digest"));
        }
        Ok(receipt)
    }
}

fn load_consent(
    connection: &Connection,
    workspace_id: WorkspaceId,
    transfer_id: Uuid,
    receipt_id: Uuid,
) -> Result<Option<TeamPermanentTransferConsentReceipt>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT transfer_id,workspace_id,consent_kind,authority_revision,membership_revision,actor_activity_id,actor_agent_id,actor_session_id,actor_participation_id,actor_profile_id,actor_participant_id,actor_display_name,recorded_at_ms,intent_digest FROM ops_team_permanent_transfer_consents WHERE workspace_id=?1 AND transfer_id=?2 AND receipt_id=?3",
            params![workspace_id.into_uuid().as_bytes(), transfer_id.as_bytes(), receipt_id.as_bytes()],
            |row| {
                Ok(ConsentRaw {
                    transfer_id: row.get(0)?,
                    workspace_id: row.get(1)?,
                    kind: row.get(2)?,
                    authority_revision: row.get(3)?,
                    membership_revision: row.get(4)?,
                    actor: read_actor_raw(row, 5)?,
                    recorded_at_ms: row.get(12)?,
                    intent_digest: row.get(13)?,
                })
            },
        )
        .optional()?;
    raw.map(|raw| raw.into_receipt(receipt_id, connection))
        .transpose()
}

struct ConsentRaw {
    transfer_id: Vec<u8>,
    workspace_id: Vec<u8>,
    kind: String,
    authority_revision: i64,
    membership_revision: i64,
    actor: ActorRaw,
    recorded_at_ms: i64,
    intent_digest: Vec<u8>,
}

impl ConsentRaw {
    fn into_receipt(
        self,
        receipt_id: Uuid,
        connection: &Connection,
    ) -> Result<TeamPermanentTransferConsentReceipt, OperationsError> {
        let mut receipt = TeamPermanentTransferConsentReceipt {
            receipt_id,
            transfer_id: decode_id(self.transfer_id, "permanent transfer consent transfer")?,
            kind: TeamPermanentTransferConsentKind::decode(&self.kind)?,
            authority_revision: positive_revision(
                self.authority_revision,
                "permanent transfer consent authority revision",
            )?,
            membership_revision: positive_u64(
                self.membership_revision,
                "permanent transfer consent membership revision",
            )?,
            actor: ActorSnapshot::decode(self.actor)?,
            recorded_at: TimestampMillis::new(self.recorded_at_ms)
                .map_err(|_| OperationsError::Corrupt("permanent transfer consent timestamp"))?,
            intent_digest: digest32(self.intent_digest, "permanent transfer consent digest")?,
            replayed: false,
        };
        let workspace_id = WorkspaceId::from_uuid(decode_id(
            self.workspace_id,
            "permanent transfer consent Workspace",
        )?);
        let intent = load_intent_in_workspace(connection, workspace_id, receipt.transfer_id)?
            .ok_or(OperationsError::Corrupt(
                "permanent transfer consent intent missing",
            ))?;
        if intent.intent_digest != receipt.intent_digest
            || consent_authority_matches_intent(&receipt, &intent).is_err()
        {
            return Err(OperationsError::Corrupt(
                "permanent Team transfer consent history",
            ));
        }
        receipt.replayed = false;
        Ok(receipt)
    }
}

fn consent_authority_matches_intent(
    receipt: &TeamPermanentTransferConsentReceipt,
    intent: &TeamPermanentTransferIntentReceipt,
) -> Result<(), OperationsError> {
    let valid = match receipt.kind {
        TeamPermanentTransferConsentKind::SourceHomeLeader => {
            receipt.actor.participation_id == intent.source_leader_participation_id
                && receipt.actor.profile_id == intent.source_leader_profile_id
                && receipt.authority_revision == intent.source_leadership_revision
        }
        TeamPermanentTransferConsentKind::TargetAgentAcceptance => {
            receipt.actor.participation_id == intent.target_participation_id
                && receipt.actor.profile_id == intent.target_profile_id
                && u64::try_from(receipt.authority_revision).ok()
                    == Some(intent.source_membership_revision)
        }
    };
    if valid {
        Ok(())
    } else {
        Err(OperationsError::Corrupt(
            "permanent Team transfer consent actor binding",
        ))
    }
}

fn load_consent_by_transfer_and_kind(
    connection: &Connection,
    workspace_id: WorkspaceId,
    transfer_id: Uuid,
    kind: TeamPermanentTransferConsentKind,
) -> Result<Option<TeamPermanentTransferConsentReceipt>, OperationsError> {
    let receipt_id: Option<Vec<u8>> = connection
        .query_row(
            "SELECT receipt_id FROM ops_team_permanent_transfer_consents WHERE workspace_id=?1 AND transfer_id=?2 AND consent_kind=?3",
            params![workspace_id.into_uuid().as_bytes(), transfer_id.as_bytes(), kind.as_str()],
            |row| row.get(0),
        )
        .optional()?;
    receipt_id
        .map(|id| {
            load_consent(
                connection,
                workspace_id,
                transfer_id,
                decode_id(id, "permanent transfer consent ID")?,
            )
        })
        .transpose()
        .map(Option::flatten)
}

fn consent_for_kind(
    connection: &Connection,
    workspace_id: WorkspaceId,
    transfer_id: Uuid,
    kind: TeamPermanentTransferConsentKind,
) -> Result<Option<TeamPermanentTransferConsentReceipt>, OperationsError> {
    load_consent_by_transfer_and_kind(connection, workspace_id, transfer_id, kind)
}

fn consent_receipt_id_exists(
    connection: &Connection,
    workspace_id: WorkspaceId,
    id: Uuid,
) -> Result<bool, OperationsError> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_team_permanent_transfer_intents WHERE workspace_id=?1 AND transfer_id=?2 UNION ALL SELECT 1 FROM ops_team_permanent_transfer_consents WHERE workspace_id=?1 AND receipt_id=?2)",
            params![workspace_id.into_uuid().as_bytes(), id.as_bytes()],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

fn validate_consent_history(
    connection: &Connection,
    workspace_id: WorkspaceId,
    receipt: &TeamPermanentTransferConsentReceipt,
) -> Result<(), OperationsError> {
    let existing = load_consent(
        connection,
        workspace_id,
        receipt.transfer_id,
        receipt.receipt_id,
    )?
    .ok_or(OperationsError::Corrupt(
        "permanent transfer consent readback",
    ))?;
    if existing != *receipt {
        return Err(OperationsError::Corrupt(
            "permanent transfer consent readback",
        ));
    }
    Ok(())
}

fn consent_matches_command(
    receipt: &TeamPermanentTransferConsentReceipt,
    command: &TeamPermanentTransferConsentCommand,
    actor: &ActorSnapshot,
) -> bool {
    receipt.transfer_id == command.transfer_id
        && receipt.kind == command.kind
        && receipt.actor.same_logical_actor(actor)
}

fn intent_matches_command(
    receipt: &TeamPermanentTransferIntentReceipt,
    command: &TeamPermanentTransferIntentCommand,
    actor: &ActorSnapshot,
) -> bool {
    receipt.workspace_id == command.workspace_id
        && receipt.source_team_id == command.source_team_id
        && receipt.destination_team_id == command.destination_team_id
        && receipt.target_participation_id == command.target_participation_id
        && receipt.destination_request_id == command.destination_request_id
        && receipt.destination_decision_id == command.destination_decision_id
        && receipt.destination_ordinal == command.destination_ordinal
        && receipt.destination_team_revision == command.expected_destination_team_revision
        && receipt.destination_capacity_revision == command.expected_destination_capacity_revision
        && receipt.destination_workspace_leadership_revision
            == command.expected_workspace_leadership_revision
        && receipt.source_membership_revision == command.expected_source_membership_revision
        && receipt.source_leadership_revision == command.expected_source_leadership_revision
        && receipt.workspace_actor.same_logical_actor(actor)
}

fn with_intent_replay(
    mut receipt: TeamPermanentTransferIntentReceipt,
) -> TeamPermanentTransferIntentReceipt {
    receipt.replayed = true;
    receipt
}

fn with_consent_replay(
    mut receipt: TeamPermanentTransferConsentReceipt,
) -> TeamPermanentTransferConsentReceipt {
    receipt.replayed = true;
    receipt
}

fn intent_digest_from_receipt(
    receipt: &TeamPermanentTransferIntentReceipt,
) -> Result<[u8; 32], OperationsError> {
    let mut digest = Sha256::new();
    digest.update(INTENT_DIGEST_DOMAIN);
    for id in [
        receipt.transfer_id,
        receipt.workspace_id.into_uuid(),
        receipt.source_team_id,
        receipt.destination_team_id,
        receipt.target_participation_id.into_uuid(),
        receipt.target_profile_id,
        receipt.destination_request_id,
        receipt.destination_decision_id,
    ] {
        digest.update(id.as_bytes());
    }
    digest.update(receipt.source_membership_revision.to_be_bytes());
    digest.update(receipt.source_leadership_revision.to_be_bytes());
    digest.update(
        receipt
            .source_leader_participation_id
            .into_uuid()
            .as_bytes(),
    );
    digest.update(receipt.source_leader_profile_id.as_bytes());
    digest.update(receipt.destination_team_revision.to_be_bytes());
    digest.update(receipt.destination_capacity_revision.to_be_bytes());
    digest.update(
        receipt
            .destination_workspace_leadership_revision
            .to_be_bytes(),
    );
    digest.update([receipt.destination_ordinal]);
    // Persisted receipts are already validated for nonempty bounded display name.
    digest_actor(&mut digest, &receipt.workspace_actor)?;
    digest.update(receipt.recorded_at.get().to_be_bytes());
    Ok(digest.finalize().into())
}

fn digest_actor(digest: &mut Sha256, actor: &ActorSnapshot) -> Result<(), OperationsError> {
    for id in [
        actor.activity_id.into_uuid(),
        actor.agent_id.into_uuid(),
        actor.session_id.into_uuid(),
        actor.participation_id.into_uuid(),
        actor.profile_id,
        actor.participant_id,
    ] {
        digest.update(id.as_bytes());
    }
    let display_name = actor.display_name.as_bytes();
    digest.update(
        u32::try_from(display_name.len())
            .map_err(|_| OperationsError::Bounds("permanent transfer actor display name"))?
            .to_be_bytes(),
    );
    digest.update(display_name);
    Ok(())
}

fn positive_revision(value: i64, label: &'static str) -> Result<i64, OperationsError> {
    if value < 1 {
        return Err(OperationsError::Corrupt(label));
    }
    Ok(value)
}

fn positive_u64(value: i64, label: &'static str) -> Result<u64, OperationsError> {
    let value = u64::try_from(value).map_err(|_| OperationsError::Corrupt(label))?;
    if value == 0 {
        return Err(OperationsError::Corrupt(label));
    }
    Ok(value)
}

fn to_i64(value: u64, label: &'static str) -> Result<i64, OperationsError> {
    i64::try_from(value).map_err(|_| OperationsError::Bounds(label))
}

fn validate_display_name(display_name: String) -> Result<String, OperationsError> {
    if display_name.trim().is_empty() || display_name.len() > 128 {
        return Err(OperationsError::Corrupt(
            "permanent Team transfer actor display name",
        ));
    }
    Ok(display_name)
}

fn decode_id(bytes: Vec<u8>, label: &'static str) -> Result<Uuid, OperationsError> {
    let id = Uuid::from_bytes(crate::wire::id(bytes)?);
    if id.is_nil() {
        return Err(OperationsError::Corrupt(label));
    }
    Ok(id)
}

fn digest32(bytes: Vec<u8>, label: &'static str) -> Result<[u8; 32], OperationsError> {
    bytes
        .try_into()
        .map_err(|_| OperationsError::Corrupt(label))
}
