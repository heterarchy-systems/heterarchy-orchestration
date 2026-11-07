//! Effect-free Coordinator staffing proposals for an Orchestrator-approved
//! `NewProfile` Team expansion slot.
//!
//! A proposal preserves an exact single-AgentRequest intent before adoption.
//! It does **not** create `ops_agent_requests`, reserve a Sessions claim, run a
//! resolver, bind a Profile, or grant the submitted actor Coordinator authority.
//! The Backend caller must perform the existing current Forge Coordinator
//! proof before invoking this Operations boundary. The persisted actor is a
//! caller-provided Coordinator assertion plus a currently bound Workspace
//! participant snapshot, not an independently verified Forge receipt.
//!
//! Operations can fence the current Team revision, Team capacity revision,
//! Workspace Orchestrator leadership revision, and submitting Profile
//! participation revision. It has no aggregate Workspace revision in the
//! current source. This record also does not attest that existing Profile
//! candidates were exhausted; the approved hold's NewProfile classification
//! is preserved as source, not re-proven here.
//! The proposed AgentRequest ID is unique within staged proposals and checked
//! against canonical requests when staging. Because this record does not claim
//! that ID in `ops_agent_requests`, a later Orchestrator adoption must recheck
//! for a canonical-ID collision inside the same IMMEDIATE transaction before
//! inserting the canonical request or starting resolver/provider effects.

mod schema;
#[cfg(test)]
mod tests;

use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, TimestampMillis, WorkspaceId};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::{
    AgentRequestEffectScope, AgentRequestIndependence, AgentRequestIntent, AgentRequestRequester,
    LeadershipScope, OperationsCatalog, OperationsError, TeamExpansionSlotRef,
    WorkspaceParticipationId, effect_binding,
    leadership::current_leadership_authority_in_transaction,
    operational_actor::{OperationalActor, ResolvedOperationalActor, resolve_actor},
    teams, wire,
};

const PROPOSAL_DOMAIN: &[u8] = b"heterarchy.team-expansion-coordinator-proposal/v1\0";
const UNVERIFIED_STATUS: &str = "COORDINATOR_PROPOSED_UNVERIFIED";

pub(super) const fn schema_objects() -> &'static [(&'static str, &'static str, &'static str)] {
    schema::schema_objects()
}

pub(super) fn create_schema(transaction: &Transaction<'_>) -> Result<(), OperationsError> {
    schema::create_schema(transaction)
}

/// The immutable request content a Coordinator proposes for one reserved slot.
///
/// The count is fixed at one because an expansion slot is one logical Agent.
/// `request_id` is intended for a later Orchestrator adoption step; this record
/// does not make it a canonical `AgentRequest`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorAgentRequestProposal {
    request_id: Uuid,
    activity_id: ActivityId,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    coordinator_revision: u64,
    objective: String,
    capability: String,
    effect_scope: AgentRequestEffectScope,
    independence: AgentRequestIndependence,
    reason: String,
}

impl CoordinatorAgentRequestProposal {
    /// Construct one bounded, one-Agent request proposal.
    ///
    /// # Errors
    /// Rejects invalid IDs, a zero Coordinator revision, or malformed request
    /// text using the canonical AgentRequest policy validator.
    pub fn new(
        request_id: Uuid,
        activity_id: ActivityId,
        agent_id: AgentInstanceId,
        session_id: AgentSessionId,
        coordinator_revision: u64,
        objective: String,
        capability: String,
        effect_scope: AgentRequestEffectScope,
        independence: AgentRequestIndependence,
        reason: String,
    ) -> Result<Self, OperationsError> {
        if request_id.is_nil()
            || activity_id.into_uuid().is_nil()
            || agent_id.into_uuid().is_nil()
            || session_id.into_uuid().is_nil()
            || coordinator_revision == 0
        {
            return Err(OperationsError::Conflict(
                "staged AgentRequest proposal identity",
            ));
        }
        let _validated_intent = AgentRequestIntent::new(
            objective.clone(),
            capability.clone(),
            1,
            effect_scope,
            independence,
            reason.clone(),
        )?;
        Ok(Self {
            request_id,
            activity_id,
            agent_id,
            session_id,
            coordinator_revision,
            objective,
            capability,
            effect_scope,
            independence,
            reason,
        })
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity_id
    }

    #[must_use]
    pub const fn agent_id(&self) -> AgentInstanceId {
        self.agent_id
    }

    #[must_use]
    pub const fn session_id(&self) -> AgentSessionId {
        self.session_id
    }

    /// This is the Coordinator revision asserted by the caller. Operations
    /// does not verify the separate Forge Coordinator proof.
    #[must_use]
    pub const fn asserted_coordinator_revision(&self) -> u64 {
        self.coordinator_revision
    }

    #[must_use]
    pub fn objective(&self) -> &str {
        &self.objective
    }

    #[must_use]
    pub fn capability(&self) -> &str {
        &self.capability
    }

    #[must_use]
    pub const fn effect_scope(&self) -> AgentRequestEffectScope {
        self.effect_scope
    }

    #[must_use]
    pub const fn independence(&self) -> AgentRequestIndependence {
        self.independence
    }

    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

/// Idempotency identity and current Team/Workspace fences for one proposal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorStagedProposalCommand {
    proposal_id: Uuid,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    slot: TeamExpansionSlotRef,
    /// Current Team revision expected when this proposal is recorded.
    /// The approved decision's historical revision is stored separately.
    expected_team_revision: u64,
    /// Current Team capacity revision expected when this proposal is recorded.
    /// The approved decision's historical capacity revision is stored separately.
    expected_capacity_revision: u64,
    expected_orchestrator_revision: i64,
    expected_workspace_participation_revision: u64,
    requester: OperationalActor,
    request: CoordinatorAgentRequestProposal,
}

impl CoordinatorStagedProposalCommand {
    #[must_use]
    pub const fn new(
        proposal_id: Uuid,
        workspace_id: WorkspaceId,
        team_id: Uuid,
        slot: TeamExpansionSlotRef,
        expected_team_revision: u64,
        expected_capacity_revision: u64,
        expected_orchestrator_revision: i64,
        expected_workspace_participation_revision: u64,
        requester: OperationalActor,
        request: CoordinatorAgentRequestProposal,
    ) -> Self {
        Self {
            proposal_id,
            workspace_id,
            team_id,
            slot,
            expected_team_revision,
            expected_capacity_revision,
            expected_orchestrator_revision,
            expected_workspace_participation_revision,
            requester,
            request,
        }
    }
}

/// Durable proposal only. An external Coordinator proof remains required
/// before this request may be considered for Orchestrator adoption.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorStagedProposalReceipt {
    proposal_id: Uuid,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    slot: TeamExpansionSlotRef,
    approval_request_id: Uuid,
    approval_expected_team_revision: u64,
    approval_capacity_revision: u64,
    expected_team_revision: u64,
    expected_capacity_revision: u64,
    expected_orchestrator_revision: i64,
    expected_workspace_participation_revision: u64,
    request: CoordinatorAgentRequestProposal,
    actor_profile_id: Uuid,
    actor_participation_id: WorkspaceParticipationId,
    actor_participant_id: Uuid,
    proposal_digest: [u8; 32],
    recorded_at: TimestampMillis,
    replayed: bool,
}

impl CoordinatorStagedProposalReceipt {
    #[must_use]
    pub const fn request(&self) -> &CoordinatorAgentRequestProposal {
        &self.request
    }

    #[must_use]
    pub const fn proposal_id(&self) -> Uuid {
        self.proposal_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn team_id(&self) -> Uuid {
        self.team_id
    }

    #[must_use]
    pub const fn slot(&self) -> TeamExpansionSlotRef {
        self.slot
    }

    #[must_use]
    pub const fn approval_request_id(&self) -> Uuid {
        self.approval_request_id
    }

    #[must_use]
    pub const fn approval_expected_team_revision(&self) -> u64 {
        self.approval_expected_team_revision
    }

    #[must_use]
    pub const fn approval_capacity_revision(&self) -> u64 {
        self.approval_capacity_revision
    }

    #[must_use]
    pub const fn expected_team_revision(&self) -> u64 {
        self.expected_team_revision
    }

    #[must_use]
    pub const fn expected_capacity_revision(&self) -> u64 {
        self.expected_capacity_revision
    }

    #[must_use]
    pub const fn expected_orchestrator_revision(&self) -> i64 {
        self.expected_orchestrator_revision
    }

    /// Revision of the submitting Profile's Workspace participation row.
    /// Operations has no aggregate Workspace revision in the current source.
    #[must_use]
    pub const fn expected_workspace_participation_revision(&self) -> u64 {
        self.expected_workspace_participation_revision
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request.request_id
    }

    #[must_use]
    pub const fn actor_profile_id(&self) -> Uuid {
        self.actor_profile_id
    }

    #[must_use]
    pub const fn actor_participation_id(&self) -> WorkspaceParticipationId {
        self.actor_participation_id
    }

    #[must_use]
    pub const fn recorded_at(&self) -> TimestampMillis {
        self.recorded_at
    }

    #[must_use]
    pub const fn proposal_digest(&self) -> [u8; 32] {
        self.proposal_digest
    }

    /// Always returns the source-only, unverified status. This record is not
    /// an adopted AgentRequest or evidence of a verified Coordinator.
    #[must_use]
    pub const fn is_unverified_coordinator_proposal(&self) -> bool {
        true
    }

    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Result and readback queries for staged proposals.
impl OperationsCatalog {
    /// Record an effect-free Coordinator proposal for an approved `NewProfile`
    /// hold. The caller must first pass the Backend's current Forge
    /// Coordinator proof. This Operations boundary does not receive or verify
    /// that proof: it records an unverified proposal assertion and validates
    /// only the current Workspace participant binding and exact held slot.
    ///
    /// The Backend must keep the profile-slot/AgentRequest linkage in this
    /// source transaction. It must not submit the intent through the normal
    /// `/agent-requests` route first, since that route reserves resolution.
    ///
    /// # Errors
    /// Rejects invalid or changed replay, a nonparticipant, an unanchored,
    /// assigned, consumed, or non-`NewProfile` slot, stale Team/capacity/O
    /// revisions, and uncertain commits.
    pub fn record_staged_coordinator_agent_request_proposal(
        &mut self,
        command: &CoordinatorStagedProposalCommand,
        trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
    ) -> Result<CoordinatorStagedProposalReceipt, OperationsError> {
        validate_proposal_command(command)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        teams::ensure_verified_workspace(&transaction, command.workspace_id)?;
        let OperationalActor::Participant {
            activity_id,
            agent_id,
            session_id,
        } = command.requester
        else {
            return Err(OperationsError::Conflict(
                "staged proposal requires a Participant Coordinator assertion",
            ));
        };
        if activity_id != command.request.activity_id
            || agent_id != command.request.agent_id
            || session_id != command.request.session_id
        {
            return Err(OperationsError::Conflict(
                "staged proposal Coordinator actor mismatch",
            ));
        }
        if effect_binding::exact(
            &transaction,
            command.workspace_id,
            activity_id,
            agent_id,
            session_id,
        )?
        .is_none()
        {
            return Err(OperationsError::Conflict(
                "staged proposal Coordinator is not a Ready Workspace participant",
            ));
        }
        let actor = resolve_actor(&transaction, command.workspace_id, command.requester)?;
        let Some(actor_snapshot) = proposal_actor_snapshot(&actor) else {
            return Err(OperationsError::Conflict(
                "staged proposal requires a Profile Workspace participant",
            ));
        };
        if let Some(owner) =
            proposal_owner(&transaction, command.workspace_id, command.proposal_id)?
        {
            let same_issuer = owner.0.as_slice() == actor_snapshot.profile_id.as_bytes()
                && owner.1.as_slice() == actor_snapshot.participation_id.into_uuid().as_bytes();
            if same_issuer {
                let existing =
                    read_proposal(&transaction, command.workspace_id, command.proposal_id)?.ok_or(
                        OperationsError::Corrupt("staged proposal owner row disappeared"),
                    )?;
                if !proposal_matches_command(&existing, command, actor_snapshot) {
                    return Err(OperationsError::Conflict("staged proposal replay"));
                }
                transaction.commit()?;
                return Ok(CoordinatorStagedProposalReceipt {
                    replayed: true,
                    ..existing
                });
            }
        }
        let approval_source = validate_new_profile_slot(&transaction, command)?;
        let current_participation_revision = workspace_participation_revision(
            &transaction,
            command.workspace_id,
            actor_snapshot.profile_id,
            actor_snapshot.participation_id,
        )?;
        if current_participation_revision != command.expected_workspace_participation_revision {
            return Err(OperationsError::Conflict(
                "staged proposal Workspace participation revision",
            ));
        }
        let (current_team_revision, active) =
            super::team_revision_and_active(&transaction, command.workspace_id, command.team_id)?;
        let current_capacity_revision =
            super::load_current_capacity(&transaction, command.workspace_id, command.team_id)?
                .map(|snapshot| snapshot.revision());
        if !active
            || current_team_revision != command.expected_team_revision
            || current_capacity_revision != Some(command.expected_capacity_revision)
        {
            return Err(OperationsError::Conflict("staged proposal Team revision"));
        }
        let orchestrator = current_leadership_authority_in_transaction(
            &transaction,
            LeadershipScope::workspace(command.workspace_id),
        )?
        .ok_or(OperationsError::Conflict(
            "Workspace Orchestrator unavailable",
        ))?;
        if orchestrator.leadership_revision() != command.expected_orchestrator_revision {
            return Err(OperationsError::Conflict(
                "staged proposal Orchestrator revision",
            ));
        }

        // Only disclose whether an identity is already used after the caller,
        // current Team/Workspace fences, and exact held slot passed local
        // validation. A staged ID is not a global AgentRequest reservation.
        if staged_identity_exists_globally(
            &transaction,
            command.proposal_id,
            command.request.request_id,
        )? {
            return Err(OperationsError::Conflict("staged proposal identity"));
        }

        let recorded_at = trusted_now()?;
        super::ensure_source_time_not_regressed(
            &transaction,
            command.workspace_id,
            command.team_id,
            recorded_at,
        )?;
        write_proposal(
            &transaction,
            command,
            approval_source,
            actor_snapshot,
            recorded_at,
        )?;
        let fresh = read_proposal(&transaction, command.workspace_id, command.proposal_id)?
            .ok_or(OperationsError::Corrupt("staged proposal readback"))?;
        if !proposal_matches_command(&fresh, command, actor_snapshot) {
            return Err(OperationsError::Corrupt(
                "staged proposal readback mismatch",
            ));
        }
        let commit = transaction.commit();
        match commit {
            Ok(()) => Ok(fresh),
            Err(source) => {
                match read_proposal(&self.connection, command.workspace_id, command.proposal_id) {
                    Ok(Some(existing))
                        if proposal_matches_command(&existing, command, actor_snapshot) =>
                    {
                        Ok(CoordinatorStagedProposalReceipt {
                            replayed: true,
                            ..existing
                        })
                    }
                    Ok(Some(_)) => Err(OperationsError::Conflict(
                        "staged proposal after uncertain commit",
                    )),
                    Ok(None) | Err(_) => Err(OperationsError::OperationCommitUnconfirmed {
                        workspace_id: command.workspace_id,
                        request_id: command.proposal_id,
                        source,
                    }),
                }
            }
        }
    }

    /// Read one exact proposal for its original stable Workspace Profile or
    /// current Workspace Orchestrator. The durable row remains a proposal and
    /// cannot be used as an AgentRequest, resolver input, or effect grant.
    ///
    /// # Errors
    /// Rejects malformed source history, foreign Workspace scope, or changed
    /// authorized intent.
    pub fn staged_coordinator_agent_request_proposal(
        &self,
        workspace_id: WorkspaceId,
        proposal_id: Uuid,
        reader: OperationalActor,
    ) -> Result<Option<CoordinatorStagedProposalReceipt>, OperationsError> {
        if workspace_id.into_uuid().is_nil() || proposal_id.is_nil() {
            return Err(OperationsError::Conflict("staged proposal read identity"));
        }
        let transaction = self.connection.unchecked_transaction()?;
        teams::ensure_verified_workspace(&transaction, workspace_id)?;
        let actor = match resolve_actor(&transaction, workspace_id, reader) {
            Ok(actor) => actor,
            Err(OperationsError::Conflict(_)) => {
                transaction.commit()?;
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let Some(owner) = proposal_owner(&transaction, workspace_id, proposal_id)? else {
            transaction.commit()?;
            return Ok(None);
        };
        let is_original = actor
            .profile_id()
            .is_some_and(|profile| owner.0.as_slice() == profile.as_bytes())
            && actor.participation_id().is_some_and(|participation| {
                owner.1.as_slice() == participation.into_uuid().as_bytes()
            });
        let is_current_orchestrator = current_leadership_authority_in_transaction(
            &transaction,
            LeadershipScope::workspace(workspace_id),
        )?
        .is_some_and(|current| current.participant_id() == actor.participant_id());
        if !is_original && !is_current_orchestrator {
            transaction.commit()?;
            return Ok(None);
        }
        let value = read_proposal(&transaction, workspace_id, proposal_id)?.ok_or(
            OperationsError::Corrupt("staged proposal owner row disappeared"),
        )?;
        transaction.commit()?;
        Ok(Some(value))
    }
}

fn validate_proposal_command(
    command: &CoordinatorStagedProposalCommand,
) -> Result<(), OperationsError> {
    if command.proposal_id.is_nil()
        || command.workspace_id.into_uuid().is_nil()
        || command.team_id.is_nil()
        || command.slot.approval_id().is_nil()
        || command.request.request_id.is_nil()
        || command.proposal_id == command.request.request_id
        || command.slot.ordinal() > 7
        || command.expected_team_revision == 0
        || command.expected_capacity_revision == 0
        || command.expected_orchestrator_revision <= 0
        || command.expected_workspace_participation_revision == 0
    {
        return Err(OperationsError::Conflict(
            "staged proposal command identity",
        ));
    }
    let _intent = AgentRequestIntent::new(
        command.request.objective.clone(),
        command.request.capability.clone(),
        1,
        command.request.effect_scope,
        command.request.independence,
        command.request.reason.clone(),
    )?;
    let _requester = AgentRequestRequester::new(
        command.workspace_id,
        command.request.activity_id,
        command.request.agent_id,
        command.request.session_id,
        command.request.coordinator_revision,
    )?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProposalActorSnapshot {
    profile_id: Uuid,
    participation_id: WorkspaceParticipationId,
    participant_id: Uuid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProposalApprovalSource {
    request_id: Uuid,
    expected_team_revision: u64,
    capacity_revision: u64,
}

fn proposal_actor_snapshot(actor: &ResolvedOperationalActor) -> Option<ProposalActorSnapshot> {
    match actor {
        ResolvedOperationalActor::ProfileParticipant {
            participation_id,
            profile_id,
            participant_id,
            ..
        } => Some(ProposalActorSnapshot {
            profile_id: *profile_id,
            participation_id: *participation_id,
            participant_id: *participant_id,
        }),
        ResolvedOperationalActor::Operator { .. }
        | ResolvedOperationalActor::NativeParticipant { .. } => None,
    }
}

fn workspace_participation_revision(
    connection: &Connection,
    workspace_id: WorkspaceId,
    profile_id: Uuid,
    participation_id: WorkspaceParticipationId,
) -> Result<u64, OperationsError> {
    let revision: Option<i64> = connection
        .query_row(
            "SELECT revision FROM ops_workspace_participations
             WHERE workspace_id=?1 AND agent_profile_id=?2 AND participation_id=?3",
            params![
                workspace_id.into_uuid().as_bytes(),
                profile_id.as_bytes(),
                participation_id.into_uuid().as_bytes(),
            ],
            |row| row.get(0),
        )
        .optional()?;
    u64::try_from(revision.ok_or(OperationsError::Conflict(
        "staged proposal Workspace participation source",
    ))?)
    .map_err(|_| OperationsError::Corrupt("staged proposal Workspace participation revision"))
}

fn validate_new_profile_slot(
    transaction: &Transaction<'_>,
    command: &CoordinatorStagedProposalCommand,
) -> Result<ProposalApprovalSource, OperationsError> {
    let decision: Option<(Vec<u8>, Vec<u8>, String, i64, i64, i64, i64, i64, Option<i64>)> = transaction
        .query_row(
            "SELECT d.request_id,manifest.request_id,d.state,d.approved_count,
                    d.expected_team_revision,d.capacity_revision,d.decided_at_ms,
                    manifest.slot_count,a.anchor_at_ms
             FROM ops_team_expansion_decisions AS d
             JOIN ops_team_expansion_workspace_hold_manifests AS manifest
               ON manifest.workspace_id=d.workspace_id AND manifest.team_id=d.team_id
              AND manifest.decision_id=d.decision_id
             LEFT JOIN ops_team_expansion_cooldown_anchors AS a
               ON a.workspace_id=d.workspace_id AND a.team_id=d.team_id AND a.decision_id=d.decision_id
             WHERE d.workspace_id=?1 AND d.team_id=?2 AND d.decision_id=?3",
            params![
                command.workspace_id.into_uuid().as_bytes(),
                command.team_id.as_bytes(),
                command.slot.approval_id().as_bytes(),
            ],
            |row| Ok((
                row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?,
                row.get(5)?, row.get(6)?, row.get(7)?, row.get(8)?,
            )),
        )
        .optional()?;
    let Some((
        request_id,
        decision_manifest_request,
        state,
        approved_count,
        decision_team_revision,
        decision_capacity_revision,
        decided_at,
        manifest_slot_count,
        anchor_at,
    )) = decision
    else {
        return Err(OperationsError::Conflict(
            "staged proposal approval missing",
        ));
    };
    let approval_source = ProposalApprovalSource {
        request_id: Uuid::from_bytes(wire::id(request_id.clone())?),
        expected_team_revision: u64::try_from(decision_team_revision)
            .map_err(|_| OperationsError::Corrupt("staged proposal approval Team revision"))?,
        capacity_revision: u64::try_from(decision_capacity_revision)
            .map_err(|_| OperationsError::Corrupt("staged proposal approval capacity revision"))?,
    };
    if state != "APPROVED"
        || anchor_at.is_none()
        || command.slot.ordinal() >= u8::try_from(approved_count).unwrap_or(u8::MAX)
        || manifest_slot_count != approved_count
        || approval_source.expected_team_revision == 0
        || approval_source.capacity_revision == 0
        || Uuid::from_bytes(wire::id(decision_manifest_request)?) != approval_source.request_id
        || decided_at < 0
    {
        return Err(OperationsError::Conflict(
            "staged proposal approval not active",
        ));
    }
    let plan: Option<(
        Vec<u8>,
        i64,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
    )> = transaction
        .query_row(
            "SELECT manifest.request_id,plan.target_kind,plan.target_profile_id,
                    plan.target_participation_id,slot.request_id,assignment.request_id,
                    usage.join_request_id
             FROM ops_team_expansion_workspace_hold_manifests AS manifest
             JOIN ops_team_expansion_workspace_hold_plans AS plan
               ON plan.workspace_id=manifest.workspace_id AND plan.team_id=manifest.team_id
              AND plan.decision_id=manifest.decision_id
             JOIN ops_team_expansion_slots AS slot
               ON slot.workspace_id=plan.workspace_id AND slot.team_id=plan.team_id
              AND slot.decision_id=plan.decision_id AND slot.ordinal=plan.ordinal
             LEFT JOIN ops_team_expansion_slot_assignments AS assignment
               ON assignment.workspace_id=plan.workspace_id AND assignment.team_id=plan.team_id
              AND assignment.decision_id=plan.decision_id AND assignment.ordinal=plan.ordinal
             LEFT JOIN ops_team_expansion_slot_uses AS usage
               ON usage.workspace_id=plan.workspace_id AND usage.team_id=plan.team_id
              AND usage.decision_id=plan.decision_id AND usage.ordinal=plan.ordinal
             LEFT JOIN ops_team_expansion_workspace_hold_bindings AS binding
               ON binding.workspace_id=plan.workspace_id AND binding.decision_id=plan.decision_id
              AND binding.ordinal=plan.ordinal
             WHERE manifest.workspace_id=?1 AND manifest.team_id=?2 AND manifest.decision_id=?3
               AND plan.ordinal=?4 AND assignment.request_id IS NULL
               AND usage.join_request_id IS NULL AND binding.binding_request_id IS NULL",
            params![
                command.workspace_id.into_uuid().as_bytes(),
                command.team_id.as_bytes(),
                command.slot.approval_id().as_bytes(),
                i64::from(command.slot.ordinal()),
            ],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()?;
    let Some((
        manifest_request,
        kind,
        target_profile,
        target_participation,
        slot_request,
        assignment_request,
        use_request,
    )) = plan
    else {
        return Err(OperationsError::Conflict(
            "staged proposal slot unavailable",
        ));
    };
    if Uuid::from_bytes(wire::id(manifest_request)?) != approval_source.request_id
        || Uuid::from_bytes(wire::id(
            slot_request.ok_or(OperationsError::Corrupt("staged proposal slot request"))?,
        )?) != approval_source.request_id
        || target_profile.is_some()
        || target_participation.is_some()
        || assignment_request.is_some()
        || use_request.is_some()
        || kind != 2
    {
        return Err(OperationsError::Conflict(
            "staged proposal requires NewProfile hold",
        ));
    }
    Ok(approval_source)
}

fn write_proposal(
    transaction: &Transaction<'_>,
    command: &CoordinatorStagedProposalCommand,
    approval_source: ProposalApprovalSource,
    actor: ProposalActorSnapshot,
    recorded_at: TimestampMillis,
) -> Result<(), OperationsError> {
    let scope_code = match command.request.effect_scope {
        AgentRequestEffectScope::ReadOnly => 0_i64,
        AgentRequestEffectScope::SourceWrite => 1_i64,
    };
    let independence_code = match command.request.independence {
        AgentRequestIndependence::None => 0_i64,
        AgentRequestIndependence::IndependentFromCurrentWriter => 1_i64,
    };
    let digest = proposal_digest(command, approval_source, actor)?;
    transaction.execute(
        "INSERT INTO ops_team_expansion_staged_agent_proposals(
            proposal_id,request_id,workspace_id,team_id,decision_id,approval_request_id,
            approval_expected_team_revision,approval_capacity_revision,ordinal,status,
            expected_team_revision,capacity_revision,orchestrator_revision,
            workspace_participation_revision,
            activity_id,agent_id,session_id,coordinator_revision,requested_count,
            objective,capability,effect_scope,independence_requirement,reason,
            actor_profile_id,actor_participation_id,actor_participant_id,proposal_digest,recorded_at_ms)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28,?29)",
        params![
            command.proposal_id.as_bytes(),
            command.request.request_id.as_bytes(),
            command.workspace_id.into_uuid().as_bytes(),
            command.team_id.as_bytes(),
            command.slot.approval_id().as_bytes(),
            approval_source.request_id.as_bytes(),
            i64::try_from(approval_source.expected_team_revision)
                .map_err(|_| OperationsError::Bounds("staged proposal approval Team revision"))?,
            i64::try_from(approval_source.capacity_revision)
                .map_err(|_| OperationsError::Bounds("staged proposal approval capacity revision"))?,
            i64::from(command.slot.ordinal()),
            UNVERIFIED_STATUS,
            i64::try_from(command.expected_team_revision)
                .map_err(|_| OperationsError::Bounds("staged proposal Team revision"))?,
            i64::try_from(command.expected_capacity_revision)
                .map_err(|_| OperationsError::Bounds("staged proposal capacity revision"))?,
            command.expected_orchestrator_revision,
            i64::try_from(command.expected_workspace_participation_revision)
                .map_err(|_| OperationsError::Bounds("staged proposal Workspace participation revision"))?,
            command.request.activity_id.into_uuid().as_bytes(),
            command.request.agent_id.into_uuid().as_bytes(),
            command.request.session_id.into_uuid().as_bytes(),
            i64::try_from(command.request.coordinator_revision)
                .map_err(|_| OperationsError::Bounds("staged proposal coordinator revision"))?,
            1_i64,
            command.request.objective,
            command.request.capability,
            scope_code,
            independence_code,
            command.request.reason,
            actor.profile_id.as_bytes(),
            actor.participation_id.into_uuid().as_bytes(),
            actor.participant_id.as_bytes(),
            digest.as_slice(),
            recorded_at.get(),
        ],
    )?;
    Ok(())
}

fn read_proposal(
    connection: &Connection,
    workspace_id: WorkspaceId,
    proposal_id: Uuid,
) -> Result<Option<CoordinatorStagedProposalReceipt>, OperationsError> {
    let raw: Option<RawProposal> = connection
        .query_row(
            "SELECT proposal_id,request_id,workspace_id,team_id,decision_id,approval_request_id,
                    approval_expected_team_revision,approval_capacity_revision,ordinal,status,
                    expected_team_revision,capacity_revision,orchestrator_revision,
                    workspace_participation_revision,activity_id,agent_id,session_id,
                    coordinator_revision,requested_count,objective,capability,effect_scope,
                    independence_requirement,reason,actor_profile_id,actor_participation_id,
                    actor_participant_id,proposal_digest,recorded_at_ms
             FROM ops_team_expansion_staged_agent_proposals
             WHERE workspace_id=?1 AND proposal_id=?2",
            params![workspace_id.into_uuid().as_bytes(), proposal_id.as_bytes()],
            decode_raw_proposal,
        )
        .optional()?;
    raw.map(decode_proposal).transpose()
}

fn proposal_owner(
    connection: &Connection,
    workspace_id: WorkspaceId,
    proposal_id: Uuid,
) -> Result<Option<(Vec<u8>, Vec<u8>)>, OperationsError> {
    let raw: Option<(Vec<u8>, Vec<u8>)> = connection
        .query_row(
            "SELECT actor_profile_id,actor_participation_id FROM ops_team_expansion_staged_agent_proposals WHERE workspace_id=?1 AND proposal_id=?2",
            params![workspace_id.into_uuid().as_bytes(), proposal_id.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    Ok(raw)
}

fn staged_identity_exists_globally(
    connection: &Connection,
    proposal_id: Uuid,
    request_id: Uuid,
) -> Result<bool, OperationsError> {
    connection
        .query_row(
            "SELECT
                EXISTS(SELECT 1 FROM ops_team_expansion_staged_agent_proposals WHERE proposal_id=?1),
                EXISTS(SELECT 1 FROM ops_team_expansion_staged_agent_proposals WHERE request_id=?2),
                EXISTS(SELECT 1 FROM ops_agent_requests WHERE request_id=?2)",
            params![proposal_id.as_bytes(), request_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, bool>(0)?,
                    row.get::<_, bool>(1)?,
                    row.get::<_, bool>(2)?,
                ))
            },
        )
        .map(|(proposal_used, staged_request_used, canonical_request_used)| {
            proposal_used || staged_request_used || canonical_request_used
        })
        .map_err(OperationsError::from)
}

fn proposal_matches_command(
    proposal: &CoordinatorStagedProposalReceipt,
    command: &CoordinatorStagedProposalCommand,
    actor: ProposalActorSnapshot,
) -> bool {
    proposal.proposal_id == command.proposal_id
        && proposal.workspace_id == command.workspace_id
        && proposal.team_id == command.team_id
        && proposal.slot == command.slot
        && proposal.request == command.request
        && proposal.expected_team_revision == command.expected_team_revision
        && proposal.expected_capacity_revision == command.expected_capacity_revision
        && proposal.expected_orchestrator_revision == command.expected_orchestrator_revision
        && proposal.expected_workspace_participation_revision
            == command.expected_workspace_participation_revision
        && proposal.actor_profile_id == actor.profile_id
        && proposal.actor_participation_id == actor.participation_id
        && proposal.actor_participant_id == actor.participant_id
        && proposal_digest(
            command,
            ProposalApprovalSource {
                request_id: proposal.approval_request_id,
                expected_team_revision: proposal.approval_expected_team_revision,
                capacity_revision: proposal.approval_capacity_revision,
            },
            actor,
        )
        .is_ok_and(|digest| digest == proposal.proposal_digest)
}

fn proposal_digest(
    command: &CoordinatorStagedProposalCommand,
    approval_source: ProposalApprovalSource,
    actor: ProposalActorSnapshot,
) -> Result<[u8; 32], OperationsError> {
    let mut digest = Sha256::new();
    digest.update(PROPOSAL_DOMAIN);
    digest.update(command.proposal_id.as_bytes());
    digest.update(command.request.request_id.as_bytes());
    digest.update(command.workspace_id.into_uuid().as_bytes());
    digest.update(command.team_id.as_bytes());
    digest.update(command.slot.approval_id().as_bytes());
    digest.update(approval_source.request_id.as_bytes());
    digest.update(approval_source.expected_team_revision.to_be_bytes());
    digest.update(approval_source.capacity_revision.to_be_bytes());
    digest.update([command.slot.ordinal()]);
    digest.update(command.expected_team_revision.to_be_bytes());
    digest.update(command.expected_capacity_revision.to_be_bytes());
    digest.update(command.expected_orchestrator_revision.to_be_bytes());
    digest.update(
        command
            .expected_workspace_participation_revision
            .to_be_bytes(),
    );
    digest.update(command.request.activity_id.into_uuid().as_bytes());
    digest.update(command.request.agent_id.into_uuid().as_bytes());
    digest.update(command.request.session_id.into_uuid().as_bytes());
    digest.update(command.request.coordinator_revision.to_be_bytes());
    digest.update([1]); // fixed AgentRequest count for one expansion slot
    frame_text(&mut digest, &command.request.objective)?;
    frame_text(&mut digest, &command.request.capability)?;
    digest.update([match command.request.effect_scope {
        AgentRequestEffectScope::ReadOnly => 0,
        AgentRequestEffectScope::SourceWrite => 1,
    }]);
    digest.update([match command.request.independence {
        AgentRequestIndependence::None => 0,
        AgentRequestIndependence::IndependentFromCurrentWriter => 1,
    }]);
    frame_text(&mut digest, &command.request.reason)?;
    digest.update(actor.profile_id.as_bytes());
    digest.update(actor.participation_id.into_uuid().as_bytes());
    digest.update(actor.participant_id.as_bytes());
    Ok(digest.finalize().into())
}

fn frame_text(digest: &mut Sha256, text: &str) -> Result<(), OperationsError> {
    let bytes = text.as_bytes();
    let len = u32::try_from(bytes.len())
        .map_err(|_| OperationsError::Bounds("staged proposal digest text"))?;
    digest.update(len.to_be_bytes());
    digest.update(bytes);
    Ok(())
}

type RawProposal = (
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    i64,
    i64,
    i64,
    String,
    i64,
    i64,
    i64,
    i64,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    i64,
    i64,
    String,
    String,
    i64,
    i64,
    String,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    i64,
);

fn decode_raw_proposal(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawProposal> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
        row.get(9)?,
        row.get(10)?,
        row.get(11)?,
        row.get(12)?,
        row.get(13)?,
        row.get(14)?,
        row.get(15)?,
        row.get(16)?,
        row.get(17)?,
        row.get(18)?,
        row.get(19)?,
        row.get(20)?,
        row.get(21)?,
        row.get(22)?,
        row.get(23)?,
        row.get(24)?,
        row.get(25)?,
        row.get(26)?,
        row.get(27)?,
        row.get(28)?,
    ))
}

fn decode_proposal(raw: RawProposal) -> Result<CoordinatorStagedProposalReceipt, OperationsError> {
    let workspace_id = WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(raw.2)?));
    if raw.9 != UNVERIFIED_STATUS {
        return Err(OperationsError::Corrupt("staged proposal status"));
    }
    let activity_id = ActivityId::from_uuid(Uuid::from_bytes(wire::id(raw.14)?));
    let agent_id = AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(raw.15)?));
    let session_id = AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(raw.16)?));
    let coordinator_revision = u64::try_from(raw.17)
        .map_err(|_| OperationsError::Corrupt("staged proposal coordinator revision"))?;
    if raw.18 != 1 {
        return Err(OperationsError::Corrupt("staged proposal request count"));
    }
    let objective = raw.19;
    let capability = raw.20;
    let reason = raw.23;
    let effect_scope = match raw.21 {
        0 => AgentRequestEffectScope::ReadOnly,
        1 => AgentRequestEffectScope::SourceWrite,
        _ => return Err(OperationsError::Corrupt("staged proposal effect scope")),
    };
    let independence = match raw.22 {
        0 => AgentRequestIndependence::None,
        1 => AgentRequestIndependence::IndependentFromCurrentWriter,
        _ => return Err(OperationsError::Corrupt("staged proposal independence")),
    };
    let approval_expected_team_revision = u64::try_from(raw.6)
        .map_err(|_| OperationsError::Corrupt("staged proposal approval Team revision"))?;
    let approval_capacity_revision = u64::try_from(raw.7)
        .map_err(|_| OperationsError::Corrupt("staged proposal approval capacity revision"))?;
    let expected_team_revision = u64::try_from(raw.10)
        .map_err(|_| OperationsError::Corrupt("staged proposal Team revision"))?;
    let expected_capacity_revision = u64::try_from(raw.11)
        .map_err(|_| OperationsError::Corrupt("staged proposal capacity revision"))?;
    let expected_orchestrator_revision = raw.12;
    let expected_workspace_participation_revision = u64::try_from(raw.13).map_err(|_| {
        OperationsError::Corrupt("staged proposal Workspace participation revision")
    })?;
    let actor = ProposalActorSnapshot {
        profile_id: Uuid::from_bytes(wire::id(raw.24.clone())?),
        participation_id: WorkspaceParticipationId::from_uuid(Uuid::from_bytes(wire::id(
            raw.25.clone(),
        )?))?,
        participant_id: Uuid::from_bytes(wire::id(raw.26.clone())?),
    };
    let proposal_digest: [u8; 32] = raw
        .27
        .as_slice()
        .try_into()
        .map_err(|_| OperationsError::Corrupt("staged proposal digest"))?;
    let request = CoordinatorAgentRequestProposal {
        request_id: Uuid::from_bytes(wire::id(raw.1)?),
        activity_id,
        agent_id,
        session_id,
        coordinator_revision,
        objective,
        capability,
        effect_scope,
        independence,
        reason,
    };
    let receipt = CoordinatorStagedProposalReceipt {
        proposal_id: Uuid::from_bytes(wire::id(raw.0)?),
        workspace_id,
        team_id: Uuid::from_bytes(wire::id(raw.3)?),
        slot: TeamExpansionSlotRef::new(
            Uuid::from_bytes(wire::id(raw.4)?),
            u8::try_from(raw.8).map_err(|_| OperationsError::Corrupt("staged proposal ordinal"))?,
        ),
        approval_request_id: Uuid::from_bytes(wire::id(raw.5)?),
        approval_expected_team_revision,
        approval_capacity_revision,
        expected_team_revision,
        expected_capacity_revision,
        expected_orchestrator_revision,
        expected_workspace_participation_revision,
        request,
        actor_profile_id: actor.profile_id,
        actor_participation_id: actor.participation_id,
        actor_participant_id: actor.participant_id,
        proposal_digest,
        recorded_at: TimestampMillis::new(raw.28)
            .map_err(|_| OperationsError::Corrupt("staged proposal time"))?,
        replayed: false,
    };
    let command = CoordinatorStagedProposalCommand {
        proposal_id: receipt.proposal_id,
        workspace_id: receipt.workspace_id,
        team_id: receipt.team_id,
        slot: receipt.slot,
        expected_team_revision,
        expected_capacity_revision,
        expected_orchestrator_revision,
        expected_workspace_participation_revision,
        requester: OperationalActor::Participant {
            activity_id,
            agent_id,
            session_id,
        },
        request: receipt.request.clone(),
    };
    validate_proposal_command(&command)?;
    if proposal_digest(
        &command,
        ProposalApprovalSource {
            request_id: receipt.approval_request_id,
            expected_team_revision: receipt.approval_expected_team_revision,
            capacity_revision: receipt.approval_capacity_revision,
        },
        actor,
    )? != receipt.proposal_digest
    {
        return Err(OperationsError::Corrupt("staged proposal digest mismatch"));
    }
    Ok(receipt)
}
