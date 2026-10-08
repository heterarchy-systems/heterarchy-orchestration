//! Immutable Team affinity for one Forge Agent assignment.
//!
//! The affinity is an Orchestration admission witness.  Forge still owns the
//! assignment and its effects; this journal only records the exact Team scope
//! observed before the offer is sent to Forge.  A missing Team is stored as an
//! explicit `NULL` value so a later read can distinguish an admitted unscoped
//! assignment from an assignment for which admission never happened.

use crate::{
    NodeTeamBinding, NodeTeamBindingScope, OperationsCatalog, OperationsError,
    WorkspaceParticipationId, effect_binding, operational_actor::ResolvedOperationalActor, wire,
};
use forge_domain::{
    ActivityId, AgentAssignmentId, AgentAssignmentState, AgentInstanceId, AgentSessionId,
    DagNodeId, DagRevisionNumber, GoalId, GoalRevision, RegisteredAgentAssignment, TimestampMillis,
    WorkspaceId,
};
use forge_persistence::{AgentAssignmentOfferSealAuthority, AgentAssignmentOfferSealSnapshot};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use uuid::Uuid;

/// Maximum exact original serialized offer payload retained per affinity.
pub const MAX_ORIGINAL_OFFER_PAYLOAD_BYTES: usize = 16 * 1024;
/// Maximum rows returned by one unresolved-affinity recovery page.
pub const MAX_ASSIGNMENT_AFFINITY_RESERVATION_PAGE_SIZE: u32 = 32;

const ASSIGNMENT_AFFINITY_V35_SQL: &str = "CREATE TABLE ops_assignment_team_affinities (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id != zeroblob(16)), assignment_id BLOB NOT NULL CHECK(length(assignment_id)=16 AND assignment_id != zeroblob(16)), activity_id BLOB NOT NULL CHECK(length(activity_id)=16 AND activity_id != zeroblob(16)), target_agent_id BLOB NOT NULL CHECK(length(target_agent_id)=16 AND target_agent_id != zeroblob(16)), target_session_id BLOB NOT NULL CHECK(length(target_session_id)=16 AND target_session_id != zeroblob(16)), target_participation_id BLOB NOT NULL CHECK(length(target_participation_id)=16 AND target_participation_id != zeroblob(16)), target_participant_id BLOB NOT NULL CHECK(length(target_participant_id)=16 AND target_participant_id != zeroblob(16)), team_id BLOB CHECK(team_id IS NULL OR (length(team_id)=16 AND team_id != zeroblob(16))), team_membership_revision INTEGER CHECK(team_membership_revision IS NULL OR team_membership_revision>=1), requester_activity_id BLOB CHECK(requester_activity_id IS NULL OR (length(requester_activity_id)=16 AND requester_activity_id != zeroblob(16))), requester_agent_id BLOB CHECK(requester_agent_id IS NULL OR (length(requester_agent_id)=16 AND requester_agent_id != zeroblob(16))), requester_session_id BLOB CHECK(requester_session_id IS NULL OR (length(requester_session_id)=16 AND requester_session_id != zeroblob(16))), requester_participation_id BLOB CHECK(requester_participation_id IS NULL OR (length(requester_participation_id)=16 AND requester_participation_id != zeroblob(16))), requester_profile_id BLOB CHECK(requester_profile_id IS NULL OR (length(requester_profile_id)=16 AND requester_profile_id != zeroblob(16))), requester_participant_id BLOB CHECK(requester_participant_id IS NULL OR (length(requester_participant_id)=16 AND requester_participant_id != zeroblob(16))), requester_key TEXT NOT NULL CHECK(length(CAST(requester_key AS BLOB)) BETWEEN 1 AND 256), requester_display_name TEXT NOT NULL CHECK(length(CAST(requester_display_name AS BLOB)) BETWEEN 1 AND 128), assignment_payload_fingerprint BLOB NOT NULL CHECK(length(assignment_payload_fingerprint)=32), observed_at_ms INTEGER NOT NULL CHECK(observed_at_ms>=0), PRIMARY KEY(workspace_id,assignment_id), CHECK((team_id IS NULL AND team_membership_revision IS NULL) OR (team_id IS NOT NULL AND team_membership_revision IS NOT NULL)), CHECK((requester_activity_id IS NULL AND requester_agent_id IS NULL AND requester_session_id IS NULL AND requester_participation_id IS NULL AND requester_profile_id IS NULL AND requester_participant_id IS NULL) OR (requester_activity_id IS NOT NULL AND requester_agent_id IS NOT NULL AND requester_session_id IS NOT NULL AND requester_participation_id IS NOT NULL AND requester_profile_id IS NOT NULL AND requester_participant_id IS NOT NULL)))";
const ASSIGNMENT_AFFINITY_SQL: &str = "CREATE TABLE ops_assignment_team_affinities (workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id != zeroblob(16)), assignment_id BLOB NOT NULL CHECK(length(assignment_id)=16 AND assignment_id != zeroblob(16)), activity_id BLOB NOT NULL CHECK(length(activity_id)=16 AND activity_id != zeroblob(16)), target_agent_id BLOB NOT NULL CHECK(length(target_agent_id)=16 AND target_agent_id != zeroblob(16)), target_session_id BLOB NOT NULL CHECK(length(target_session_id)=16 AND target_session_id != zeroblob(16)), target_participation_id BLOB NOT NULL CHECK(length(target_participation_id)=16 AND target_participation_id != zeroblob(16)), target_participant_id BLOB NOT NULL CHECK(length(target_participant_id)=16 AND target_participant_id != zeroblob(16)), team_id BLOB CHECK(team_id IS NULL OR (length(team_id)=16 AND team_id != zeroblob(16))), team_membership_revision INTEGER CHECK(team_membership_revision IS NULL OR team_membership_revision>=1), requester_activity_id BLOB CHECK(requester_activity_id IS NULL OR (length(requester_activity_id)=16 AND requester_activity_id != zeroblob(16))), requester_agent_id BLOB CHECK(requester_agent_id IS NULL OR (length(requester_agent_id)=16 AND requester_agent_id != zeroblob(16))), requester_session_id BLOB CHECK(requester_session_id IS NULL OR (length(requester_session_id)=16 AND requester_session_id != zeroblob(16))), requester_participation_id BLOB CHECK(requester_participation_id IS NULL OR (length(requester_participation_id)=16 AND requester_participation_id != zeroblob(16))), requester_profile_id BLOB CHECK(requester_profile_id IS NULL OR (length(requester_profile_id)=16 AND requester_profile_id != zeroblob(16))), requester_participant_id BLOB CHECK(requester_participant_id IS NULL OR (length(requester_participant_id)=16 AND requester_participant_id != zeroblob(16))), requester_key TEXT NOT NULL CHECK(length(CAST(requester_key AS BLOB)) BETWEEN 1 AND 256), requester_display_name TEXT NOT NULL CHECK(length(CAST(requester_display_name AS BLOB)) BETWEEN 1 AND 128), assignment_payload_fingerprint BLOB NOT NULL CHECK(length(assignment_payload_fingerprint)=32), observed_at_ms INTEGER NOT NULL CHECK(observed_at_ms>=0), node_goal_id BLOB CHECK(node_goal_id IS NULL OR (length(node_goal_id)=16 AND node_goal_id != zeroblob(16))), goal_created_at_ms INTEGER CHECK(goal_created_at_ms IS NULL OR goal_created_at_ms>=0), node_dag_revision INTEGER CHECK(node_dag_revision IS NULL OR node_dag_revision>=1), node_dag_base_goal_revision INTEGER CHECK(node_dag_base_goal_revision IS NULL OR node_dag_base_goal_revision>=1), node_id BLOB CHECK(node_id IS NULL OR (length(node_id)=16 AND node_id != zeroblob(16))), node_binding_revision INTEGER CHECK(node_binding_revision IS NULL OR node_binding_revision>=1), disposition TEXT NOT NULL CHECK(disposition IN('PENDING','ACTIVE','TERMINAL','QUIESCED','LEGACY_UNKNOWN','LEGACY_QUIESCED')), original_offer_payload BLOB CHECK(original_offer_payload IS NULL OR length(original_offer_payload) BETWEEN 1 AND 16384), seal_sealed_at_ms INTEGER CHECK(seal_sealed_at_ms IS NULL OR seal_sealed_at_ms>=0), seal_applied_at_ms INTEGER CHECK(seal_applied_at_ms IS NULL OR seal_applied_at_ms>=0), seal_authority_kind TEXT CHECK(seal_authority_kind IS NULL OR seal_authority_kind IN('LOCAL_OPERATOR','COORDINATOR')), seal_authority_agent_id BLOB CHECK(seal_authority_agent_id IS NULL OR (length(seal_authority_agent_id)=16 AND seal_authority_agent_id != zeroblob(16))), seal_authority_session_id BLOB CHECK(seal_authority_session_id IS NULL OR (length(seal_authority_session_id)=16 AND seal_authority_session_id != zeroblob(16))), seal_authority_revision INTEGER CHECK(seal_authority_revision IS NULL OR seal_authority_revision>=1), seal_observation_kind TEXT CHECK(seal_observation_kind IS NULL OR seal_observation_kind IN('ABSENT_NOW_SEALED','FOREIGN_ACTIVITY','LEGACY_TERMINAL_SAME_ACTIVITY')), seal_current_activity_id BLOB CHECK(seal_current_activity_id IS NULL OR (length(seal_current_activity_id)=16 AND seal_current_activity_id != zeroblob(16))), seal_current_state TEXT CHECK(seal_current_state IS NULL OR seal_current_state IN('OFFERED','RUNNING','SUBMITTED','ACCEPTED','REJECTED','CANCELLED','INTERRUPTED')), seal_current_revision INTEGER CHECK(seal_current_revision IS NULL OR seal_current_revision>=1), PRIMARY KEY(workspace_id,assignment_id), CHECK((team_id IS NULL AND team_membership_revision IS NULL) OR (team_id IS NOT NULL AND team_membership_revision IS NOT NULL)), CHECK((requester_activity_id IS NULL AND requester_agent_id IS NULL AND requester_session_id IS NULL AND requester_participation_id IS NULL AND requester_profile_id IS NULL AND requester_participant_id IS NULL) OR (requester_activity_id IS NOT NULL AND requester_agent_id IS NOT NULL AND requester_session_id IS NOT NULL AND requester_participation_id IS NOT NULL AND requester_profile_id IS NOT NULL AND requester_participant_id IS NOT NULL)), CHECK((disposition IN('LEGACY_UNKNOWN','LEGACY_QUIESCED') AND node_goal_id IS NULL AND goal_created_at_ms IS NULL AND node_dag_revision IS NULL AND node_dag_base_goal_revision IS NULL AND node_id IS NULL AND node_binding_revision IS NULL) OR (disposition IN('PENDING','ACTIVE','TERMINAL','QUIESCED') AND node_goal_id IS NOT NULL AND goal_created_at_ms IS NOT NULL AND node_dag_revision IS NOT NULL AND node_dag_base_goal_revision IS NOT NULL AND node_id IS NOT NULL)), CHECK((disposition IN('LEGACY_UNKNOWN','LEGACY_QUIESCED') AND original_offer_payload IS NULL) OR (disposition IN('PENDING','ACTIVE','TERMINAL','QUIESCED') AND original_offer_payload IS NOT NULL)), CHECK((seal_sealed_at_ms IS NULL AND seal_applied_at_ms IS NULL AND seal_authority_kind IS NULL AND seal_authority_agent_id IS NULL AND seal_authority_session_id IS NULL AND seal_authority_revision IS NULL AND seal_observation_kind IS NULL AND seal_current_activity_id IS NULL AND seal_current_state IS NULL AND seal_current_revision IS NULL) OR (disposition IN('QUIESCED','LEGACY_QUIESCED') AND seal_sealed_at_ms IS NOT NULL AND seal_applied_at_ms IS NOT NULL AND seal_authority_kind IS NOT NULL AND seal_observation_kind IS NOT NULL)), CHECK((seal_authority_kind IS NULL AND seal_authority_agent_id IS NULL AND seal_authority_session_id IS NULL AND seal_authority_revision IS NULL) OR (seal_authority_kind='LOCAL_OPERATOR' AND seal_authority_agent_id IS NULL AND seal_authority_session_id IS NULL AND seal_authority_revision IS NULL) OR (seal_authority_kind='COORDINATOR' AND seal_authority_agent_id IS NOT NULL AND seal_authority_session_id IS NOT NULL AND seal_authority_revision IS NOT NULL)), CHECK((seal_observation_kind IS NULL AND seal_current_activity_id IS NULL AND seal_current_state IS NULL AND seal_current_revision IS NULL) OR (seal_observation_kind='ABSENT_NOW_SEALED' AND seal_current_activity_id IS NULL AND seal_current_state IS NULL AND seal_current_revision IS NULL) OR (seal_observation_kind IN('FOREIGN_ACTIVITY','LEGACY_TERMINAL_SAME_ACTIVITY') AND seal_current_activity_id IS NOT NULL AND seal_current_state IS NOT NULL AND seal_current_revision IS NOT NULL)))";
const ASSIGNMENT_AFFINITY_SCOPE_INDEX: &str = "CREATE INDEX ops_assignment_team_affinities_node_scope_idx ON ops_assignment_team_affinities(workspace_id,activity_id,node_goal_id,goal_created_at_ms,node_dag_revision,node_dag_base_goal_revision,node_id,disposition)";

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        (
            "table",
            "ops_assignment_team_affinities",
            ASSIGNMENT_AFFINITY_SQL,
        ),
        (
            "index",
            "ops_assignment_team_affinities_node_scope_idx",
            ASSIGNMENT_AFFINITY_SCOPE_INDEX,
        ),
    ]
}

pub(crate) fn schema_objects_v35() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![(
        "table",
        "ops_assignment_team_affinities",
        ASSIGNMENT_AFFINITY_V35_SQL,
    )]
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in schema_objects() {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

/// Preserve pre-generation affinities without inventing a Node scope.
pub(crate) fn migrate_v35_to_v36(tx: &Transaction<'_>) -> Result<(), OperationsError> {
    tx.execute_batch(
        "ALTER TABLE ops_assignment_team_affinities RENAME TO ops_assignment_team_affinities_v35",
    )?;
    create_schema(tx)?;
    tx.execute(
        "INSERT INTO ops_assignment_team_affinities(workspace_id,assignment_id,activity_id,target_agent_id,target_session_id,target_participation_id,target_participant_id,team_id,team_membership_revision,requester_activity_id,requester_agent_id,requester_session_id,requester_participation_id,requester_profile_id,requester_participant_id,requester_key,requester_display_name,assignment_payload_fingerprint,observed_at_ms,node_goal_id,goal_created_at_ms,node_dag_revision,node_dag_base_goal_revision,node_id,node_binding_revision,disposition) SELECT workspace_id,assignment_id,activity_id,target_agent_id,target_session_id,target_participation_id,target_participant_id,team_id,team_membership_revision,requester_activity_id,requester_agent_id,requester_session_id,requester_participation_id,requester_profile_id,requester_participant_id,requester_key,requester_display_name,assignment_payload_fingerprint,observed_at_ms,NULL,NULL,NULL,NULL,NULL,NULL,'LEGACY_UNKNOWN' FROM ops_assignment_team_affinities_v35",
        [],
    )?;
    tx.execute_batch("DROP TABLE ops_assignment_team_affinities_v35")?;
    Ok(())
}

/// The caller's trusted admission input.  The payload fingerprint is supplied
/// by the Forge-facing boundary and is never recomputed from a partial copy of
/// the assignment payload here.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentAffinityCommand {
    pub workspace_id: WorkspaceId,
    pub assignment_id: AgentAssignmentId,
    pub activity_id: ActivityId,
    pub target_agent_id: AgentInstanceId,
    pub target_session_id: AgentSessionId,
    pub requester: crate::OperationalActor,
    pub team_id: Option<Uuid>,
    pub node_scope: NodeTeamBindingScope,
    pub expected_binding_revision: Option<u64>,
    pub assignment_payload_fingerprint: [u8; 32],
    /// Exact bounded offer DTO bytes covered by the existing admission fingerprint.
    pub original_offer_payload: Vec<u8>,
    pub observed_at: TimestampMillis,
}

/// Why a trusted seal snapshot released this exact admission reservation.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssignmentOfferSealObservationKind {
    /// Forge reports no current Assignment row while the exact key is sealed.
    AbsentNowSealed,
    /// Forge reports a current row under a different Activity.
    ForeignActivity,
    /// A legacy row has a terminal same-Activity current Assignment; original identity remains unknown.
    LegacyTerminalSameActivity,
}

/// Immutable first seal evidence attached to an affinity disposition.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssignmentOfferSealEvidence {
    sealed_at: TimestampMillis,
    applied_at: TimestampMillis,
    authority: AgentAssignmentOfferSealAuthority,
    observation: AssignmentOfferSealObservationKind,
    current_activity_id: Option<ActivityId>,
    current_state: Option<AgentAssignmentState>,
    current_revision: Option<u64>,
}

impl AssignmentOfferSealEvidence {
    /// First durable Forge seal timestamp.
    #[must_use]
    pub const fn sealed_at(self) -> TimestampMillis {
        self.sealed_at
    }

    /// Time this Operations catalog first applied the seal observation.
    #[must_use]
    pub const fn applied_at(self) -> TimestampMillis {
        self.applied_at
    }

    /// Authority recorded by the first successful Forge seal.
    pub const fn authority(self) -> AgentAssignmentOfferSealAuthority {
        self.authority
    }

    /// Observation category retained with this first application.
    pub const fn observation(self) -> AssignmentOfferSealObservationKind {
        self.observation
    }

    /// Activity of the current Assignment row, if one was observed.
    #[must_use]
    pub const fn current_activity_id(self) -> Option<ActivityId> {
        self.current_activity_id
    }

    /// State of the current Assignment row, if one was observed.
    #[must_use]
    pub const fn current_state(self) -> Option<AgentAssignmentState> {
        self.current_state
    }

    /// Revision of the current Assignment row, if one was observed.
    #[must_use]
    pub const fn current_revision(self) -> Option<u64> {
        self.current_revision
    }
}

/// Immutable actor and target evidence captured by one admission journal row.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentAffinityActor {
    activity_id: Option<ActivityId>,
    agent_id: Option<AgentInstanceId>,
    session_id: Option<AgentSessionId>,
    participation_id: Option<WorkspaceParticipationId>,
    profile_id: Option<Uuid>,
    participant_id: Option<Uuid>,
    key: String,
    display_name: String,
}

impl AssignmentAffinityActor {
    #[must_use]
    pub const fn activity_id(&self) -> Option<ActivityId> {
        self.activity_id
    }

    #[must_use]
    pub const fn agent_id(&self) -> Option<AgentInstanceId> {
        self.agent_id
    }

    #[must_use]
    pub const fn session_id(&self) -> Option<AgentSessionId> {
        self.session_id
    }

    #[must_use]
    pub const fn participation_id(&self) -> Option<WorkspaceParticipationId> {
        self.participation_id
    }

    #[must_use]
    pub const fn profile_id(&self) -> Option<Uuid> {
        self.profile_id
    }

    #[must_use]
    pub const fn participant_id(&self) -> Option<Uuid> {
        self.participant_id
    }

    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }
}

/// Durable readback of one immutable assignment affinity admission.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentAffinityReceipt {
    workspace_id: WorkspaceId,
    assignment_id: AgentAssignmentId,
    activity_id: ActivityId,
    target_agent_id: AgentInstanceId,
    target_session_id: AgentSessionId,
    target_participation_id: WorkspaceParticipationId,
    target_participant_id: Uuid,
    team_id: Option<Uuid>,
    team_membership_revision: Option<u64>,
    node_scope: Option<NodeTeamBindingScope>,
    binding_revision: Option<u64>,
    disposition: AssignmentAffinityDisposition,
    requester: AssignmentAffinityActor,
    assignment_payload_fingerprint: [u8; 32],
    original_offer_payload: Option<Vec<u8>>,
    observed_at: TimestampMillis,
    seal_evidence: Option<AssignmentOfferSealEvidence>,
    replayed: bool,
}

/// Durable lifecycle of the exact Node-scoped admission reservation.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssignmentAffinityDisposition {
    /// Admission is journaled but no current Forge offer is observed yet.
    Pending,
    /// Forge reports the admitted assignment as currently active.
    Active,
    /// A trusted exact Forge assignment observation reached a terminal state.
    Terminal,
    /// A permanent exact Activity seal released only the binding reservation.
    Quiesced,
    /// Legacy admission is quiesced, but its original node scope remains unknown.
    LegacyUnknown,
    /// A seal released a legacy reservation without inventing its node scope.
    LegacyQuiesced,
}

impl AssignmentAffinityDisposition {
    fn encode(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Active => "ACTIVE",
            Self::Terminal => "TERMINAL",
            Self::Quiesced => "QUIESCED",
            Self::LegacyUnknown => "LEGACY_UNKNOWN",
            Self::LegacyQuiesced => "LEGACY_QUIESCED",
        }
    }

    fn decode(value: &str) -> Result<Self, OperationsError> {
        match value {
            "PENDING" => Ok(Self::Pending),
            "ACTIVE" => Ok(Self::Active),
            "TERMINAL" => Ok(Self::Terminal),
            "QUIESCED" => Ok(Self::Quiesced),
            "LEGACY_UNKNOWN" => Ok(Self::LegacyUnknown),
            "LEGACY_QUIESCED" => Ok(Self::LegacyQuiesced),
            _ => Err(OperationsError::Corrupt("assignment affinity disposition")),
        }
    }
}

impl AssignmentAffinityReceipt {
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn assignment_id(&self) -> AgentAssignmentId {
        self.assignment_id
    }

    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity_id
    }

    #[must_use]
    pub const fn target_agent_id(&self) -> AgentInstanceId {
        self.target_agent_id
    }

    pub const fn target_session_id(&self) -> AgentSessionId {
        self.target_session_id
    }

    #[must_use]
    pub const fn target_participation_id(&self) -> WorkspaceParticipationId {
        self.target_participation_id
    }

    #[must_use]
    pub const fn target_participant_id(&self) -> Uuid {
        self.target_participant_id
    }

    #[must_use]
    pub const fn team_id(&self) -> Option<Uuid> {
        self.team_id
    }

    #[must_use]
    pub const fn team_membership_revision(&self) -> Option<u64> {
        self.team_membership_revision
    }

    #[must_use]
    pub const fn node_scope(&self) -> Option<NodeTeamBindingScope> {
        self.node_scope
    }

    #[must_use]
    pub const fn binding_revision(&self) -> Option<u64> {
        self.binding_revision
    }

    pub const fn disposition(&self) -> AssignmentAffinityDisposition {
        self.disposition
    }

    pub fn requester(&self) -> &AssignmentAffinityActor {
        &self.requester
    }

    #[must_use]
    pub const fn assignment_payload_fingerprint(&self) -> [u8; 32] {
        self.assignment_payload_fingerprint
    }

    #[must_use]
    pub fn original_offer_payload(&self) -> Option<&[u8]> {
        self.original_offer_payload.as_deref()
    }

    #[must_use]
    pub const fn observed_at(&self) -> TimestampMillis {
        self.observed_at
    }

    #[must_use]
    pub const fn seal_evidence(&self) -> Option<&AssignmentOfferSealEvidence> {
        self.seal_evidence.as_ref()
    }

    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Bounded recovery view of unresolved affinity rows for one exact Node scope.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentAffinityReservationPage {
    receipts: Vec<AssignmentAffinityReceipt>,
    next_assignment: Option<AgentAssignmentId>,
}

impl AssignmentAffinityReservationPage {
    /// Rows in ascending assignment UUID order.
    pub fn receipts(&self) -> &[AssignmentAffinityReceipt] {
        &self.receipts
    }

    /// Exclusive keyset cursor for the next page; absent when this page is final.
    #[must_use]
    pub const fn next_assignment(&self) -> Option<AgentAssignmentId> {
        self.next_assignment
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ActorInput {
    activity: Option<ActivityId>,
    agent: Option<AgentInstanceId>,
    session: Option<AgentSessionId>,
}

fn actor_input(actor: crate::OperationalActor) -> ActorInput {
    match actor {
        crate::OperationalActor::LocalOperator => ActorInput {
            activity: None,
            agent: None,
            session: None,
        },
        crate::OperationalActor::Participant {
            activity_id,
            agent_id,
            session_id,
        } => ActorInput {
            activity: Some(activity_id),
            agent: Some(agent_id),
            session: Some(session_id),
        },
    }
}

impl OperationsCatalog {
    /// Record or exactly replay one immutable assignment Team-affinity decision.
    ///
    /// New rows require a current Ready target and, when a Team is supplied, an
    /// active same-Workspace Team membership observed as `JOINED`. Replays use
    /// only the stored identity and therefore remain readable after departure.
    ///
    /// # Errors
    /// Rejects invalid or conflicting identity, unavailable evidence and catalog failures.
    pub fn record_assignment_affinity(
        &mut self,
        command: &AssignmentAffinityCommand,
    ) -> Result<AssignmentAffinityReceipt, OperationsError> {
        record_assignment_affinity(&mut self.connection, command)
    }

    /// Read the immutable admission journal without requiring current target
    /// liveness. `Some(receipt)` distinguishes an explicit `team_id = None`
    /// admission from a missing admission row.
    ///
    /// # Errors
    /// Rejects invalid or conflicting identity, unavailable evidence and catalog failures.
    pub fn read_assignment_affinity(
        &self,
        workspace_id: WorkspaceId,
        assignment_id: AgentAssignmentId,
    ) -> Result<Option<AssignmentAffinityReceipt>, OperationsError> {
        read_assignment_affinity(
            &self.connection,
            self.destination,
            workspace_id,
            assignment_id,
        )
    }

    /// Reconcile one trusted Forge assignment observation into the durable
    /// affinity lifecycle. Terminal observations are irreversible.
    ///
    /// # Errors
    /// Rejects mismatched assignment identity, unknown affinity rows and storage failures.
    pub fn reconcile_assignment_affinity(
        &mut self,
        assignment: &RegisteredAgentAssignment,
        scope: NodeTeamBindingScope,
    ) -> Result<AssignmentAffinityReceipt, OperationsError> {
        reconcile_assignment_affinity(&mut self.connection, assignment, scope)
    }

    /// Apply one trusted Forge offer seal to the exact stored affinity key.
    /// Quiescence releases only the reservation; it is not a terminal or
    /// never-landed claim. Existing first evidence is returned unchanged.
    ///
    /// # Errors
    /// Rejects mismatched seal identity, same-Activity present assignments and storage failures.
    pub fn reconcile_assignment_offer_seal(
        &mut self,
        snapshot: &AgentAssignmentOfferSealSnapshot,
        applied_at: TimestampMillis,
    ) -> Result<Option<AssignmentAffinityReceipt>, OperationsError> {
        reconcile_assignment_offer_seal(&mut self.connection, snapshot, applied_at)
    }

    /// Read a bounded keyset page of unresolved exact-scope and legacy affinities.
    /// When an unretired legacy binding shares the old Goal/DAG/Node coordinates,
    /// include known scoped reservations at those coordinates so the caller can
    /// compare their original receipt scopes without manufacturing a match.
    ///
    /// # Errors
    /// Rejects invalid limits/cursors and corrupt rows or database failures.
    pub fn assignment_affinity_reservations(
        &self,
        scope: NodeTeamBindingScope,
        after_assignment: Option<AgentAssignmentId>,
        limit: u32,
    ) -> Result<AssignmentAffinityReservationPage, OperationsError> {
        assignment_affinity_reservations(
            &self.connection,
            self.destination,
            scope,
            after_assignment,
            limit,
        )
    }

    /// Read only the admitted Team scope for bootstrap callers.
    ///
    /// # Errors
    /// Rejects invalid or conflicting identity, unavailable evidence and catalog failures.
    pub fn admitted_assignment_team(
        &self,
        workspace_id: WorkspaceId,
        assignment_id: AgentAssignmentId,
    ) -> Result<Option<Uuid>, OperationsError> {
        admitted_assignment_team(&self.connection, workspace_id, assignment_id)
    }
}

/// Record or replay one exact immutable assignment affinity.
pub fn record_assignment_affinity(
    connection: &mut Connection,
    command: &AssignmentAffinityCommand,
) -> Result<AssignmentAffinityReceipt, OperationsError> {
    validate_command(command)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if let Some(existing) =
        load_affinity(&transaction, command.workspace_id, command.assignment_id)?
    {
        return replay_or_conflict(transaction, command, existing);
    }
    if affinity_exists_other_workspace(&transaction, command.assignment_id, command.workspace_id)? {
        return Err(OperationsError::Conflict(
            "assignment affinity workspace scope",
        ));
    }
    if assignment_offer_exists(&transaction, command.workspace_id, command.assignment_id)?
        || assignment_offer_exists_other_workspace(
            &transaction,
            command.assignment_id,
            command.workspace_id,
        )?
    {
        return Err(OperationsError::Conflict(
            "assignment offer predates affinity admission",
        ));
    }
    let verified: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_imports i JOIN ops_catalog_identity c ON c.singleton=1 AND c.destination_id=i.destination_id WHERE i.workspace_id=?1 AND i.phase=2)",
        params![command.workspace_id.into_uuid().as_bytes()], |row| row.get(0))?;
    if !verified {
        return Err(OperationsError::Conflict(
            "assignment affinity requires verified Workspace",
        ));
    }
    validate_node_binding(&transaction, command)?;
    let actor = crate::operational_actor::resolve_actor(
        &transaction,
        command.workspace_id,
        command.requester,
    )?;
    let target_participant_id = effect_binding::exact(
        &transaction,
        command.workspace_id,
        command.activity_id,
        command.target_agent_id,
        command.target_session_id,
    )?
    .map(effect_binding::CurrentEffectBinding::participant_id)
    .ok_or(OperationsError::Conflict("assignment target is not Ready"))?;
    let target_participation_id = mapped_participation(
        &transaction,
        command.workspace_id,
        target_participant_id,
        command.target_session_id,
    )?;
    let team_membership_revision = match command.team_id {
        Some(team_id) => Some(verify_team_membership(
            &transaction,
            command.workspace_id,
            team_id,
            target_participation_id,
        )?),
        None => None,
    };
    let actor_input = actor_input(command.requester);
    insert_affinity(
        &transaction,
        command,
        &actor,
        actor_input,
        target_participation_id,
        target_participant_id,
        team_membership_revision,
    )?;
    let stored = load_affinity(&transaction, command.workspace_id, command.assignment_id)?.ok_or(
        OperationsError::Corrupt("assignment affinity insert readback"),
    )?;
    if stored.receipt.target_participation_id != target_participation_id
        || stored.receipt.team_id != command.team_id
        || stored.receipt.assignment_payload_fingerprint != command.assignment_payload_fingerprint
    {
        return Err(OperationsError::Corrupt("assignment affinity readback"));
    }
    let receipt = with_replayed(stored, false);
    transaction
        .commit()
        .map_err(|source| OperationsError::OperationCommitUnconfirmed {
            workspace_id: command.workspace_id,
            request_id: command.assignment_id.into_uuid(),
            source,
        })?;
    Ok(receipt)
}

/// Return the immutable affinity row for one exact Workspace/assignment.
pub fn read_assignment_affinity(
    connection: &Connection,
    destination: Uuid,
    workspace_id: WorkspaceId,
    assignment_id: AgentAssignmentId,
) -> Result<Option<AssignmentAffinityReceipt>, OperationsError> {
    let transaction = connection.unchecked_transaction()?;
    let result =
        read_assignment_affinity_in_transaction(&transaction, workspace_id, assignment_id)?;
    crate::catalog::commit_read(transaction, destination)?;
    Ok(result)
}

fn assignment_affinity_reservations(
    connection: &Connection,
    destination: Uuid,
    scope: NodeTeamBindingScope,
    after_assignment: Option<AgentAssignmentId>,
    limit: u32,
) -> Result<AssignmentAffinityReservationPage, OperationsError> {
    if limit == 0
        || limit > MAX_ASSIGNMENT_AFFINITY_RESERVATION_PAGE_SIZE
        || after_assignment.is_some_and(|id| id.into_uuid().is_nil())
    {
        return Err(OperationsError::Bounds("assignment reservation page"));
    }
    let transaction = connection.unchecked_transaction()?;
    let related_legacy = crate::node_team::has_legacy_node_binding(&transaction, scope)?;
    let probe_limit = i64::from(limit) + 1;
    let mut statement = transaction.prepare(
        "SELECT substr(assignment_id,1,17) FROM ops_assignment_team_affinities WHERE workspace_id=?1 AND ((activity_id=?2 AND node_goal_id=?3 AND goal_created_at_ms=?4 AND node_dag_revision=?5 AND node_dag_base_goal_revision=?6 AND node_id=?7 AND disposition IN('PENDING','ACTIVE')) OR disposition='LEGACY_UNKNOWN' OR (disposition IN('PENDING','ACTIVE') AND node_goal_id=?3 AND node_dag_revision=?5 AND node_id=?7 AND EXISTS(SELECT 1 FROM ops_dag_node_team_bindings legacy WHERE legacy.workspace_id=ops_assignment_team_affinities.workspace_id AND legacy.activity_id IS NULL AND legacy.goal_id=?3 AND legacy.dag_revision=?5 AND legacy.node_id=?7 AND legacy.legacy_retirement_request_id IS NULL))) AND (?8 IS NULL OR assignment_id>?8) ORDER BY assignment_id LIMIT ?9",
    )?;
    let rows = statement.query_map(
        params![
            scope.workspace_id().into_uuid().as_bytes(),
            scope.activity_id().into_uuid().as_bytes(),
            scope.goal_id().into_uuid().as_bytes(),
            scope.goal_created_at().get(),
            scope.dag_revision().get(),
            scope.dag_base_goal_revision().get(),
            scope.node_id().into_uuid().as_bytes(),
            after_assignment.map(|id| id.into_uuid().into_bytes().to_vec()),
            probe_limit,
        ],
        |row| row.get::<_, Vec<u8>>(0),
    )?;
    let mut assignment_ids = Vec::new();
    for row in rows {
        assignment_ids.push(AgentAssignmentId::from_uuid(Uuid::from_bytes(wire::id(
            row?,
        )?)));
    }
    drop(statement);
    let page_size = usize::try_from(limit)
        .map_err(|_| OperationsError::Bounds("assignment reservation page limit"))?;
    let has_more = assignment_ids.len() > page_size;
    if has_more {
        assignment_ids.truncate(page_size);
    }
    let next_assignment = has_more.then(|| assignment_ids.last().copied()).flatten();
    let mut receipts = Vec::with_capacity(assignment_ids.len());
    for assignment_id in assignment_ids {
        let stored = load_affinity(&transaction, scope.workspace_id(), assignment_id)?.ok_or(
            OperationsError::Corrupt("assignment reservation row disappeared"),
        )?;
        let receipt = with_replayed(stored, false);
        let live = matches!(
            receipt.disposition,
            AssignmentAffinityDisposition::Pending | AssignmentAffinityDisposition::Active
        );
        let exact = receipt.node_scope == Some(scope);
        let historical = related_legacy
            && receipt.node_scope.is_some_and(|stored| {
                stored.goal_id() == scope.goal_id()
                    && stored.dag_revision() == scope.dag_revision()
                    && stored.node_id() == scope.node_id()
            });
        if !(live && (exact || historical))
            && receipt.disposition != AssignmentAffinityDisposition::LegacyUnknown
        {
            return Err(OperationsError::Corrupt(
                "assignment reservation page filter",
            ));
        }
        receipts.push(receipt);
    }
    crate::catalog::commit_read(transaction, destination)?;
    Ok(AssignmentAffinityReservationPage {
        receipts,
        next_assignment,
    })
}

/// Read an exact affinity from a transaction already owned by the caller.
///
/// # Errors
/// Rejects cross-Workspace aliases and corrupt rows.
pub(crate) fn read_assignment_affinity_in_transaction(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    assignment_id: AgentAssignmentId,
) -> Result<Option<AssignmentAffinityReceipt>, OperationsError> {
    let result = load_affinity(tx, workspace_id, assignment_id)?;
    if result.is_none() && affinity_exists_other_workspace(tx, assignment_id, workspace_id)? {
        return Err(OperationsError::Conflict(
            "assignment affinity workspace scope",
        ));
    }
    Ok(result.map(|value| with_replayed(value, false)))
}

/// Transaction-level bootstrap read. A missing row and an explicit unscoped
/// row both return `None`; callers must use `read_assignment_affinity` when
/// they need to distinguish those states.
pub fn admitted_assignment_team(
    connection: &Connection,
    workspace_id: WorkspaceId,
    assignment_id: AgentAssignmentId,
) -> Result<Option<Uuid>, OperationsError> {
    let team_bytes = connection
		.query_row(
			"SELECT substr(team_id,1,17) FROM ops_assignment_team_affinities WHERE workspace_id=?1 AND assignment_id=?2",
			params![workspace_id.into_uuid().as_bytes(), assignment_id.into_uuid().as_bytes()],
			|row| row.get::<_, Option<Vec<u8>>>(0),
		)
		.optional()?
		.flatten();
    let team = optional_uuid(team_bytes)?;
    Ok(team)
}

fn replay_or_conflict(
    transaction: Transaction<'_>,
    command: &AssignmentAffinityCommand,
    existing: StoredAffinity,
) -> Result<AssignmentAffinityReceipt, OperationsError> {
    let actor = actor_input(command.requester);
    if existing.matches_command(command, actor) {
        let receipt = with_replayed(existing, true);
        transaction.commit()?;
        return Ok(receipt);
    }
    Err(OperationsError::Conflict("assignment affinity replay"))
}

fn validate_command(command: &AssignmentAffinityCommand) -> Result<(), OperationsError> {
    if command.workspace_id.into_uuid().is_nil()
        || command.assignment_id.into_uuid().is_nil()
        || command.activity_id.into_uuid().is_nil()
        || command.target_agent_id.into_uuid().is_nil()
        || command.target_session_id.into_uuid().is_nil()
        || command.team_id.is_some_and(|value| value.is_nil())
        || command.node_scope.workspace_id() != command.workspace_id
        || command.node_scope.activity_id() != command.activity_id
        || command.expected_binding_revision == Some(0)
        || command
            .expected_binding_revision
            .is_some_and(|revision| i64::try_from(revision).is_err())
        || command.original_offer_payload.is_empty()
        || command.original_offer_payload.len() > MAX_ORIGINAL_OFFER_PAYLOAD_BYTES
    {
        return Err(OperationsError::Conflict("assignment affinity identity"));
    }
    Ok(())
}

fn validate_node_binding(
    transaction: &Transaction<'_>,
    command: &AssignmentAffinityCommand,
) -> Result<(), OperationsError> {
    if crate::node_team::has_legacy_node_binding(transaction, command.node_scope)? {
        return Err(OperationsError::Conflict(
            "node assignment requires legacy binding reconciliation",
        ));
    }
    let current =
        crate::node_team::read_node_team_binding_in_transaction(transaction, command.node_scope)?;
    let current_revision = current.as_ref().map(NodeTeamBinding::revision);
    let current_team = current.as_ref().and_then(NodeTeamBinding::team_id);
    if current_revision != command.expected_binding_revision || current_team != command.team_id {
        return Err(OperationsError::Conflict("node binding changed"));
    }
    Ok(())
}

/// Migration-only fail-closed guard. Unknown historical admissions may have
/// landed after any binding write, so mutation stays blocked until those rows
/// are explicitly reconciled; normal reads/admissions never use this as scope.
pub(crate) fn has_legacy_unknown_affinity_for_workspace(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
) -> Result<bool, OperationsError> {
    Ok(tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_assignment_team_affinities WHERE workspace_id=?1 AND disposition='LEGACY_UNKNOWN')",
        params![workspace_id.into_uuid().as_bytes()],
        |row| row.get(0),
    )?)
}

/// Guard legacy retirement by unresolved affinity rows that cannot be
/// disambiguated by the legacy binding's Activity or generation fields.
pub(crate) fn has_unresolved_affinity_for_legacy_binding(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    goal_id: GoalId,
    dag_revision: DagRevisionNumber,
    node_id: DagNodeId,
) -> Result<bool, OperationsError> {
    Ok(tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_assignment_team_affinities WHERE workspace_id=?1 AND (disposition='LEGACY_UNKNOWN' OR (disposition IN('PENDING','ACTIVE') AND node_goal_id=?2 AND node_dag_revision=?3 AND node_id=?4)))",
        params![
            workspace_id.into_uuid().as_bytes(),
            goal_id.into_uuid().as_bytes(),
            dag_revision.get(),
            node_id.into_uuid().as_bytes(),
        ],
        |row| row.get(0),
    )?)
}

/// Exact-scope reservation guard shared with binding CAS. `LegacyUnknown` rows
/// never act as a broad Workspace or Activity fallback.
pub(crate) fn has_pending_or_active_affinity_for_scope(
    tx: &Transaction<'_>,
    scope: NodeTeamBindingScope,
) -> Result<bool, OperationsError> {
    Ok(tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_assignment_team_affinities WHERE workspace_id=?1 AND activity_id=?2 AND node_goal_id=?3 AND goal_created_at_ms=?4 AND node_dag_revision=?5 AND node_dag_base_goal_revision=?6 AND node_id=?7 AND disposition IN('PENDING','ACTIVE'))",
        params![
            scope.workspace_id().into_uuid().as_bytes(),
            scope.activity_id().into_uuid().as_bytes(),
            scope.goal_id().into_uuid().as_bytes(),
            scope.goal_created_at().get(),
            scope.dag_revision().get(),
            scope.dag_base_goal_revision().get(),
            scope.node_id().into_uuid().as_bytes(),
        ],
        |row| row.get(0),
    )?)
}

fn reconcile_assignment_affinity(
    connection: &mut Connection,
    assignment: &RegisteredAgentAssignment,
    scope: NodeTeamBindingScope,
) -> Result<AssignmentAffinityReceipt, OperationsError> {
    validate_assignment_scope(assignment, scope)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let stored = load_affinity(&transaction, assignment.workspace_id(), assignment.id())?
        .ok_or(OperationsError::Conflict("assignment affinity is missing"))?;
    let receipt = &stored.receipt;
    if receipt.activity_id != assignment.activity_id()
        || receipt.target_agent_id != assignment.target().agent_id()
        || receipt.target_session_id != assignment.target().session_id()
    {
        return Err(OperationsError::Conflict(
            "assignment affinity Forge identity",
        ));
    }
    let Some(stored_scope) = receipt.node_scope else {
        let unchanged = with_replayed(stored, false);
        transaction.commit()?;
        return Ok(unchanged);
    };
    if stored_scope != scope {
        return Err(OperationsError::Conflict("assignment affinity Node scope"));
    }
    let next = match (receipt.disposition, assignment.state()) {
        (AssignmentAffinityDisposition::Terminal, _) => AssignmentAffinityDisposition::Terminal,
        (AssignmentAffinityDisposition::Quiesced, _) => AssignmentAffinityDisposition::Quiesced,
        (AssignmentAffinityDisposition::LegacyUnknown, _) => {
            AssignmentAffinityDisposition::LegacyUnknown
        }
        (AssignmentAffinityDisposition::LegacyQuiesced, _) => {
            AssignmentAffinityDisposition::LegacyQuiesced
        }
        (_, state) if state.is_terminal() => AssignmentAffinityDisposition::Terminal,
        (_, state) if state.is_active() => AssignmentAffinityDisposition::Active,
        _ => return Err(OperationsError::Corrupt("Forge assignment state")),
    };
    if next != receipt.disposition {
        let changed = transaction.execute(
            "UPDATE ops_assignment_team_affinities SET disposition=?1 WHERE workspace_id=?2 AND assignment_id=?3 AND disposition=?4",
            params![
                next.encode(),
                assignment.workspace_id().into_uuid().as_bytes(),
                assignment.id().into_uuid().as_bytes(),
                receipt.disposition.encode(),
            ],
        )?;
        if changed != 1 {
            return Err(OperationsError::Conflict(
                "assignment affinity disposition changed",
            ));
        }
    }
    let updated = load_affinity(&transaction, assignment.workspace_id(), assignment.id())?
        .ok_or(OperationsError::Corrupt("assignment affinity readback"))?;
    let updated = with_replayed(updated, false);
    transaction
        .commit()
        .map_err(|source| OperationsError::OperationCommitUnconfirmed {
            workspace_id: assignment.workspace_id(),
            request_id: assignment.id().into_uuid(),
            source,
        })?;
    Ok(updated)
}

fn reconcile_assignment_offer_seal(
    connection: &mut Connection,
    snapshot: &AgentAssignmentOfferSealSnapshot,
    applied_at: TimestampMillis,
) -> Result<Option<AssignmentAffinityReceipt>, OperationsError> {
    let workspace_id = snapshot.workspace_id();
    let assignment_id = snapshot.assignment_id();
    let requested_activity = snapshot.requested_activity_id();
    if workspace_id.into_uuid().is_nil()
        || assignment_id.into_uuid().is_nil()
        || requested_activity.into_uuid().is_nil()
    {
        return Err(OperationsError::Conflict("assignment offer seal identity"));
    }
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let Some(stored) = load_affinity(&transaction, workspace_id, assignment_id)? else {
        if affinity_exists_other_workspace(&transaction, assignment_id, workspace_id)? {
            return Err(OperationsError::Conflict(
                "assignment affinity workspace scope",
            ));
        }
        transaction.commit()?;
        return Ok(None);
    };
    if stored.receipt.activity_id != requested_activity {
        return Err(OperationsError::Conflict("assignment offer seal Activity"));
    }
    if let Some(evidence) = stored.receipt.seal_evidence {
        if matches!(
            stored.receipt.disposition,
            AssignmentAffinityDisposition::Quiesced | AssignmentAffinityDisposition::LegacyQuiesced
        ) && evidence.sealed_at == snapshot.sealed_at()
            && evidence.authority == snapshot.seal_authority()
        {
            let receipt = with_replayed(stored, true);
            transaction.commit()?;
            return Ok(Some(receipt));
        }
        return Err(OperationsError::Conflict("assignment offer seal replay"));
    }

    let decision = classify_seal_observation(
        stored.receipt.disposition,
        workspace_id,
        assignment_id,
        requested_activity,
        snapshot.current_assignment(),
    )?;
    let evidence = AssignmentOfferSealEvidence {
        sealed_at: snapshot.sealed_at(),
        applied_at,
        authority: snapshot.seal_authority(),
        observation: decision.observation,
        current_activity_id: decision.current_activity_id,
        current_state: decision.current_state,
        current_revision: decision.current_revision,
    };
    let receipt = persist_seal_evidence(&transaction, &stored, decision.disposition, evidence)?;
    transaction
        .commit()
        .map_err(|source| OperationsError::OperationCommitUnconfirmed {
            workspace_id,
            request_id: assignment_id.into_uuid(),
            source,
        })?;
    Ok(Some(receipt))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SealObservationDecision {
    disposition: AssignmentAffinityDisposition,
    observation: AssignmentOfferSealObservationKind,
    current_activity_id: Option<ActivityId>,
    current_state: Option<AgentAssignmentState>,
    current_revision: Option<u64>,
}

fn classify_seal_observation(
    stored: AssignmentAffinityDisposition,
    workspace_id: WorkspaceId,
    assignment_id: AgentAssignmentId,
    requested_activity: ActivityId,
    current: Option<&RegisteredAgentAssignment>,
) -> Result<SealObservationDecision, OperationsError> {
    let reservation_disposition = match stored {
        AssignmentAffinityDisposition::Pending | AssignmentAffinityDisposition::Active => {
            AssignmentAffinityDisposition::Quiesced
        }
        AssignmentAffinityDisposition::LegacyUnknown => {
            AssignmentAffinityDisposition::LegacyQuiesced
        }
        _ => {
            return Err(OperationsError::Conflict(
                "assignment reservation is not live",
            ));
        }
    };
    let Some(current) = current else {
        return Ok(SealObservationDecision {
            disposition: reservation_disposition,
            observation: AssignmentOfferSealObservationKind::AbsentNowSealed,
            current_activity_id: None,
            current_state: None,
            current_revision: None,
        });
    };
    if current.workspace_id() != workspace_id || current.id() != assignment_id {
        return Err(OperationsError::Conflict(
            "assignment offer seal current identity",
        ));
    }
    let observation = if current.activity_id() != requested_activity {
        AssignmentOfferSealObservationKind::ForeignActivity
    } else if stored == AssignmentAffinityDisposition::LegacyUnknown
        && current.state().is_terminal()
    {
        AssignmentOfferSealObservationKind::LegacyTerminalSameActivity
    } else {
        return Err(OperationsError::Conflict(
            "same-Activity assignment requires exact offer reconciliation",
        ));
    };
    Ok(SealObservationDecision {
        disposition: reservation_disposition,
        observation,
        current_activity_id: Some(current.activity_id()),
        current_state: Some(current.state()),
        current_revision: Some(
            u64::try_from(current.revision().get())
                .map_err(|_| OperationsError::Corrupt("Forge assignment revision"))?,
        ),
    })
}

fn persist_seal_evidence(
    transaction: &Transaction<'_>,
    stored: &StoredAffinity,
    disposition: AssignmentAffinityDisposition,
    evidence: AssignmentOfferSealEvidence,
) -> Result<AssignmentAffinityReceipt, OperationsError> {
    let receipt = &stored.receipt;
    let allowed_transition = matches!(
        (receipt.disposition, disposition, evidence.observation),
        (
            AssignmentAffinityDisposition::Pending | AssignmentAffinityDisposition::Active,
            AssignmentAffinityDisposition::Quiesced,
            AssignmentOfferSealObservationKind::AbsentNowSealed
                | AssignmentOfferSealObservationKind::ForeignActivity,
        ) | (
            AssignmentAffinityDisposition::LegacyUnknown,
            AssignmentAffinityDisposition::LegacyQuiesced,
            _,
        ),
    );
    if !allowed_transition {
        return Err(OperationsError::Conflict(
            "assignment affinity seal transition",
        ));
    }
    let (authority_kind, authority_agent, authority_session, authority_revision) = match evidence
        .authority
    {
        AgentAssignmentOfferSealAuthority::LocalOperator => ("LOCAL_OPERATOR", None, None, None),
        AgentAssignmentOfferSealAuthority::Coordinator {
            agent_id,
            session_id,
            expected_coordinator_revision,
        } => (
            "COORDINATOR",
            Some(agent_id.into_uuid().into_bytes().to_vec()),
            Some(session_id.into_uuid().into_bytes().to_vec()),
            Some(expected_coordinator_revision.get()),
        ),
    };
    let observation_kind = match evidence.observation {
        AssignmentOfferSealObservationKind::AbsentNowSealed => "ABSENT_NOW_SEALED",
        AssignmentOfferSealObservationKind::ForeignActivity => "FOREIGN_ACTIVITY",
        AssignmentOfferSealObservationKind::LegacyTerminalSameActivity => {
            "LEGACY_TERMINAL_SAME_ACTIVITY"
        }
    };
    let current_activity = evidence
        .current_activity_id
        .map(|activity| activity.into_uuid().into_bytes().to_vec());
    let current_state = evidence.current_state.map(encode_assignment_state);
    let current_revision = evidence
        .current_revision
        .map(i64::try_from)
        .transpose()
        .map_err(|_| OperationsError::Bounds("assignment seal current revision"))?;
    let changed = transaction.execute(
        "UPDATE ops_assignment_team_affinities SET disposition=?1,seal_sealed_at_ms=?2,seal_applied_at_ms=?3,seal_authority_kind=?4,seal_authority_agent_id=?5,seal_authority_session_id=?6,seal_authority_revision=?7,seal_observation_kind=?8,seal_current_activity_id=?9,seal_current_state=?10,seal_current_revision=?11 WHERE workspace_id=?12 AND assignment_id=?13 AND activity_id=?14 AND disposition=?15 AND seal_sealed_at_ms IS NULL",
        params![
            disposition.encode(),
            evidence.sealed_at.get(),
            evidence.applied_at.get(),
            authority_kind,
            authority_agent,
            authority_session,
            authority_revision,
            observation_kind,
            current_activity,
            current_state,
            current_revision,
            receipt.workspace_id.into_uuid().as_bytes(),
            receipt.assignment_id.into_uuid().as_bytes(),
            receipt.activity_id.into_uuid().as_bytes(),
            receipt.disposition.encode(),
        ],
    )?;
    if changed != 1 {
        return Err(OperationsError::Conflict(
            "assignment affinity seal changed",
        ));
    }
    let updated = load_affinity(transaction, receipt.workspace_id, receipt.assignment_id)?
        .ok_or(OperationsError::Corrupt("assignment seal readback"))?;
    if updated.receipt.disposition != disposition
        || updated.receipt.seal_evidence != Some(evidence)
        || updated.receipt.original_offer_payload != receipt.original_offer_payload
        || updated.receipt.assignment_payload_fingerprint != receipt.assignment_payload_fingerprint
    {
        return Err(OperationsError::Corrupt("assignment seal readback"));
    }
    Ok(with_replayed(updated, false))
}

fn validate_assignment_scope(
    assignment: &RegisteredAgentAssignment,
    scope: NodeTeamBindingScope,
) -> Result<(), OperationsError> {
    if assignment.workspace_id() != scope.workspace_id()
        || assignment.activity_id() != scope.activity_id()
        || assignment.goal_id() != scope.goal_id()
        || assignment.goal_created_at() != scope.goal_created_at()
        || assignment.dag_revision() != scope.dag_revision()
        || assignment.node_id() != scope.node_id()
    {
        return Err(OperationsError::Conflict("Forge assignment Node identity"));
    }
    Ok(())
}

fn mapped_participation(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
    session_id: AgentSessionId,
) -> Result<WorkspaceParticipationId, OperationsError> {
    let raw = connection
		.query_row(
			"SELECT substr(m.participation_id,1,17) FROM ops_workspace_participation_mappings m JOIN ops_logical_members l ON l.workspace_id=m.workspace_id AND l.participant_id=m.participant_id WHERE m.workspace_id=?1 AND m.participant_id=?2 AND l.session_id=?3",
			params![workspace_id.into_uuid().as_bytes(), participant_id.as_bytes(), session_id.into_uuid().as_bytes()],
			|row| row.get::<_, Vec<u8>>(0),
		)
		.optional()?
		.ok_or(OperationsError::Conflict(
			"assignment target has no Workspace participation mapping",
		))?;
    WorkspaceParticipationId::from_uuid(Uuid::from_bytes(wire::id(raw)?))
}

fn verify_team_membership(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    participation_id: WorkspaceParticipationId,
) -> Result<u64, OperationsError> {
    let state = connection
        .query_row(
            "SELECT state FROM ops_teams WHERE workspace_id=?1 AND team_id=?2",
            params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or(OperationsError::Conflict("team workspace scope"))?;
    if state != "ACTIVE" {
        return Err(OperationsError::Conflict("team is not active"));
    }
    crate::teams::ensure_team_formed(connection, workspace_id, team_id)?;
    let membership = connection
		.query_row(
			"SELECT state,revision FROM ops_team_members WHERE workspace_id=?1 AND team_id=?2 AND participation_id=?3",
			params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes(), participation_id.into_uuid().as_bytes()],
			|row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
		)
		.optional()?
		.ok_or(OperationsError::Conflict("target is not a Team member"))?;
    if membership.0 != "JOINED" {
        return Err(OperationsError::Conflict(
            "target Team membership is not JOINED",
        ));
    }
    u64::try_from(membership.1).map_err(|_| OperationsError::Corrupt("team membership revision"))
}

fn insert_affinity(
    transaction: &Transaction<'_>,
    command: &AssignmentAffinityCommand,
    actor: &ResolvedOperationalActor,
    actor_input: ActorInput,
    target_participation_id: WorkspaceParticipationId,
    target_participant_id: Uuid,
    team_membership_revision: Option<u64>,
) -> Result<(), OperationsError> {
    transaction.execute(
        "INSERT INTO ops_assignment_team_affinities(workspace_id,assignment_id,activity_id,target_agent_id,target_session_id,target_participation_id,target_participant_id,team_id,team_membership_revision,requester_activity_id,requester_agent_id,requester_session_id,requester_participation_id,requester_profile_id,requester_participant_id,requester_key,requester_display_name,assignment_payload_fingerprint,observed_at_ms,node_goal_id,goal_created_at_ms,node_dag_revision,node_dag_base_goal_revision,node_id,node_binding_revision,disposition,original_offer_payload) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,'PENDING',?26)",
        params![
            command.workspace_id.into_uuid().as_bytes(),
            command.assignment_id.into_uuid().as_bytes(),
            command.activity_id.into_uuid().as_bytes(),
            command.target_agent_id.into_uuid().as_bytes(),
            command.target_session_id.into_uuid().as_bytes(),
            target_participation_id.into_uuid().as_bytes(),
            target_participant_id.as_bytes(),
            command.team_id.map(|id| id.into_bytes().to_vec()),
            team_membership_revision
                .map(i64::try_from)
                .transpose()
                .map_err(|_| OperationsError::Bounds("team membership revision"))?,
            actor_input.activity.map(|id| id.into_uuid().into_bytes().to_vec()),
            actor_input.agent.map(|id| id.into_uuid().into_bytes().to_vec()),
            actor_input.session.map(|id| id.into_uuid().into_bytes().to_vec()),
            actor.participation_id.map(|id| id.into_uuid().into_bytes().to_vec()),
            actor.profile_id.map(|id| id.into_bytes().to_vec()),
            actor.participant_id.map(|id| id.into_bytes().to_vec()),
            actor.key(),
            actor.display_name.as_str(),
            command.assignment_payload_fingerprint.as_slice(),
            command.observed_at.get(),
            command.node_scope.goal_id().into_uuid().as_bytes(),
            command.node_scope.goal_created_at().get(),
            command.node_scope.dag_revision().get(),
            command.node_scope.dag_base_goal_revision().get(),
            command.node_scope.node_id().into_uuid().as_bytes(),
            command.expected_binding_revision
                .map(i64::try_from)
                .transpose()
                .map_err(|_| OperationsError::Bounds("node binding revision"))?,
            command.original_offer_payload.as_slice(),
        ],
    )?;
    Ok(())
}

fn assignment_offer_exists(
    connection: &Connection,
    workspace_id: WorkspaceId,
    assignment_id: AgentAssignmentId,
) -> Result<bool, OperationsError> {
    Ok(connection.query_row(
		"SELECT EXISTS(SELECT 1 FROM ops_assignment_offer_projection WHERE workspace_id=?1 AND assignment_id=?2)",
		params![workspace_id.into_uuid().as_bytes(), assignment_id.into_uuid().as_bytes()],
		|row| row.get(0),
	)?)
}

fn assignment_offer_exists_other_workspace(
    connection: &Connection,
    assignment_id: AgentAssignmentId,
    workspace_id: WorkspaceId,
) -> Result<bool, OperationsError> {
    Ok(connection.query_row(
		"SELECT EXISTS(SELECT 1 FROM ops_assignment_offer_projection WHERE assignment_id=?1 AND workspace_id<>?2)",
		params![assignment_id.into_uuid().as_bytes(), workspace_id.into_uuid().as_bytes()],
		|row| row.get(0),
	)?)
}

fn affinity_exists_other_workspace(
    connection: &Connection,
    assignment_id: AgentAssignmentId,
    workspace_id: WorkspaceId,
) -> Result<bool, OperationsError> {
    Ok(connection.query_row(
		"SELECT EXISTS(SELECT 1 FROM ops_assignment_team_affinities WHERE assignment_id=?1 AND workspace_id<>?2)",
		params![assignment_id.into_uuid().as_bytes(), workspace_id.into_uuid().as_bytes()],
		|row| row.get(0),
	)?)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredAffinity {
    receipt: AssignmentAffinityReceipt,
    requester_input: ActorInput,
}

impl StoredAffinity {
    fn matches_command(&self, command: &AssignmentAffinityCommand, actor: ActorInput) -> bool {
        let receipt = &self.receipt;
        receipt.workspace_id == command.workspace_id
            && receipt.assignment_id == command.assignment_id
            && receipt.activity_id == command.activity_id
            && receipt.target_agent_id == command.target_agent_id
            && receipt.target_session_id == command.target_session_id
            && receipt.team_id == command.team_id
            && receipt.node_scope == Some(command.node_scope)
            && receipt.binding_revision == command.expected_binding_revision
            && receipt.assignment_payload_fingerprint == command.assignment_payload_fingerprint
            && receipt.original_offer_payload.as_deref()
                == Some(command.original_offer_payload.as_slice())
            && self.requester_input == actor
    }
}

struct RawAffinityIdentity {
    activity: Vec<u8>,
    target_agent: Vec<u8>,
    target_session: Vec<u8>,
    target_participation: Vec<u8>,
    target_participant: Vec<u8>,
    team: Option<Vec<u8>>,
    team_membership_revision: Option<i64>,
}

struct RawAffinityRequester {
    activity: Option<Vec<u8>>,
    agent: Option<Vec<u8>>,
    session: Option<Vec<u8>>,
    participation: Option<Vec<u8>>,
    profile: Option<Vec<u8>>,
    participant: Option<Vec<u8>>,
    key: String,
    display_name: String,
}

struct RawAffinityAdmission {
    payload_fingerprint: Vec<u8>,
    observed_at: i64,
    node_goal: Option<Vec<u8>>,
    goal_created_at: Option<i64>,
    node_dag_revision: Option<i64>,
    node_dag_base_revision: Option<i64>,
    node: Option<Vec<u8>>,
    binding_revision: Option<i64>,
    disposition: String,
    original_offer_payload: Option<Vec<u8>>,
    seal_sealed_at: Option<i64>,
    seal_applied_at: Option<i64>,
    seal_authority_kind: Option<String>,
    seal_authority_agent: Option<Vec<u8>>,
    seal_authority_session: Option<Vec<u8>>,
    seal_authority_revision: Option<i64>,
    seal_observation_kind: Option<String>,
    seal_current_activity: Option<Vec<u8>>,
    seal_current_state: Option<String>,
    seal_current_revision: Option<i64>,
}

struct RawAffinityRow {
    identity: RawAffinityIdentity,
    requester: RawAffinityRequester,
    admission: RawAffinityAdmission,
}

impl RawAffinityRow {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            identity: RawAffinityIdentity {
                activity: row.get(0)?,
                target_agent: row.get(1)?,
                target_session: row.get(2)?,
                target_participation: row.get(3)?,
                target_participant: row.get(4)?,
                team: row.get(5)?,
                team_membership_revision: row.get(6)?,
            },
            requester: RawAffinityRequester {
                activity: row.get(7)?,
                agent: row.get(8)?,
                session: row.get(9)?,
                participation: row.get(10)?,
                profile: row.get(11)?,
                participant: row.get(12)?,
                key: row.get(13)?,
                display_name: row.get(14)?,
            },
            admission: RawAffinityAdmission {
                payload_fingerprint: row.get(15)?,
                observed_at: row.get(16)?,
                node_goal: row.get(17)?,
                goal_created_at: row.get(18)?,
                node_dag_revision: row.get(19)?,
                node_dag_base_revision: row.get(20)?,
                node: row.get(21)?,
                binding_revision: row.get(22)?,
                disposition: row.get(23)?,
                original_offer_payload: row.get(24)?,
                seal_sealed_at: row.get(25)?,
                seal_applied_at: row.get(26)?,
                seal_authority_kind: row.get(27)?,
                seal_authority_agent: row.get(28)?,
                seal_authority_session: row.get(29)?,
                seal_authority_revision: row.get(30)?,
                seal_observation_kind: row.get(31)?,
                seal_current_activity: row.get(32)?,
                seal_current_state: row.get(33)?,
                seal_current_revision: row.get(34)?,
            },
        })
    }
}

fn load_affinity(
    connection: &Connection,
    workspace_id: WorkspaceId,
    assignment_id: AgentAssignmentId,
) -> Result<Option<StoredAffinity>, OperationsError> {
    let raw = connection.query_row(
"SELECT substr(activity_id,1,17),substr(target_agent_id,1,17),substr(target_session_id,1,17),substr(target_participation_id,1,17),substr(target_participant_id,1,17),substr(team_id,1,17),team_membership_revision,substr(requester_activity_id,1,17),substr(requester_agent_id,1,17),substr(requester_session_id,1,17),substr(requester_participation_id,1,17),substr(requester_profile_id,1,17),substr(requester_participant_id,1,17),requester_key,requester_display_name,substr(assignment_payload_fingerprint,1,33),observed_at_ms,substr(node_goal_id,1,17),goal_created_at_ms,node_dag_revision,node_dag_base_goal_revision,substr(node_id,1,17),node_binding_revision,disposition,original_offer_payload,seal_sealed_at_ms,seal_applied_at_ms,seal_authority_kind,substr(seal_authority_agent_id,1,17),substr(seal_authority_session_id,1,17),seal_authority_revision,seal_observation_kind,substr(seal_current_activity_id,1,17),seal_current_state,seal_current_revision FROM ops_assignment_team_affinities WHERE workspace_id=?1 AND assignment_id=?2",
        params![workspace_id.into_uuid().as_bytes(),assignment_id.into_uuid().as_bytes()],
        RawAffinityRow::from_row,
    ).optional()?;
    raw.map(|raw| decode_affinity_row(raw, workspace_id, assignment_id))
        .transpose()
}

struct DecodedAffinityIdentity {
    activity_id: ActivityId,
    target_agent_id: AgentInstanceId,
    target_session_id: AgentSessionId,
    target_participation_id: WorkspaceParticipationId,
    target_participant_id: Uuid,
    team_id: Option<Uuid>,
    team_membership_revision: Option<u64>,
}

struct DecodedAffinityAdmission {
    assignment_payload_fingerprint: [u8; 32],
    original_offer_payload: Option<Vec<u8>>,
    observed_at: TimestampMillis,
    node_scope: Option<NodeTeamBindingScope>,
    binding_revision: Option<u64>,
    disposition: AssignmentAffinityDisposition,
    seal_evidence: Option<AssignmentOfferSealEvidence>,
}

fn decode_affinity_row(
    raw: RawAffinityRow,
    workspace_id: WorkspaceId,
    assignment_id: AgentAssignmentId,
) -> Result<StoredAffinity, OperationsError> {
    let identity = decode_affinity_identity(raw.identity)?;
    let (requester, requester_input) = decode_affinity_requester(raw.requester)?;
    let admission = decode_affinity_admission(raw.admission, workspace_id, identity.activity_id)?;
    let receipt = AssignmentAffinityReceipt {
        workspace_id,
        assignment_id,
        activity_id: identity.activity_id,
        target_agent_id: identity.target_agent_id,
        target_session_id: identity.target_session_id,
        target_participation_id: identity.target_participation_id,
        target_participant_id: identity.target_participant_id,
        team_id: identity.team_id,
        team_membership_revision: identity.team_membership_revision,
        node_scope: admission.node_scope,
        binding_revision: admission.binding_revision,
        disposition: admission.disposition,
        requester,
        assignment_payload_fingerprint: admission.assignment_payload_fingerprint,
        original_offer_payload: admission.original_offer_payload,
        observed_at: admission.observed_at,
        seal_evidence: admission.seal_evidence,
        replayed: false,
    };
    Ok(StoredAffinity {
        receipt,
        requester_input,
    })
}

fn decode_affinity_identity(
    raw: RawAffinityIdentity,
) -> Result<DecodedAffinityIdentity, OperationsError> {
    Ok(DecodedAffinityIdentity {
        activity_id: ActivityId::from_uuid(Uuid::from_bytes(wire::id(raw.activity)?)),
        target_agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(raw.target_agent)?)),
        target_session_id: AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(
            raw.target_session,
        )?)),
        target_participation_id: WorkspaceParticipationId::from_uuid(Uuid::from_bytes(wire::id(
            raw.target_participation,
        )?))?,
        target_participant_id: Uuid::from_bytes(wire::id(raw.target_participant)?),
        team_id: optional_uuid(raw.team)?,
        team_membership_revision: raw
            .team_membership_revision
            .map(|value| decode_positive_revision(value, "team membership revision"))
            .transpose()?,
    })
}

fn decode_affinity_requester(
    raw: RawAffinityRequester,
) -> Result<(AssignmentAffinityActor, ActorInput), OperationsError> {
    let requester_input = ActorInput {
        activity: optional_uuid(raw.activity)?.map(ActivityId::from_uuid),
        agent: optional_uuid(raw.agent)?.map(AgentInstanceId::from_uuid),
        session: optional_uuid(raw.session)?.map(AgentSessionId::from_uuid),
    };
    let requester = AssignmentAffinityActor {
        activity_id: requester_input.activity,
        agent_id: requester_input.agent,
        session_id: requester_input.session,
        participation_id: optional_uuid(raw.participation)?
            .map(WorkspaceParticipationId::from_uuid)
            .transpose()?,
        profile_id: optional_uuid(raw.profile)?,
        participant_id: optional_uuid(raw.participant)?,
        key: raw.key,
        display_name: raw.display_name,
    };
    Ok((requester, requester_input))
}

fn decode_affinity_admission(
    raw: RawAffinityAdmission,
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
) -> Result<DecodedAffinityAdmission, OperationsError> {
    let seal_evidence = decode_seal_evidence(&raw)?;
    let assignment_payload_fingerprint: [u8; 32] = raw
        .payload_fingerprint
        .try_into()
        .map_err(|_| OperationsError::Corrupt("assignment affinity fingerprint"))?;
    let observed_at = TimestampMillis::new(raw.observed_at)
        .map_err(|_| OperationsError::Corrupt("assignment affinity timestamp"))?;
    let node_scope = decode_node_scope(
        workspace_id,
        activity_id,
        raw.node_goal,
        raw.goal_created_at,
        raw.node_dag_revision,
        raw.node_dag_base_revision,
        raw.node,
    )?;
    let binding_revision = raw
        .binding_revision
        .map(|value| decode_positive_revision(value, "node binding revision"))
        .transpose()?;
    let disposition = AssignmentAffinityDisposition::decode(&raw.disposition)?;
    validate_affinity_scope_shape(disposition, node_scope, binding_revision)?;
    validate_offer_payload_provenance(disposition, raw.original_offer_payload.as_deref())?;
    validate_seal_evidence(disposition, activity_id, seal_evidence)?;
    Ok(DecodedAffinityAdmission {
        assignment_payload_fingerprint,
        original_offer_payload: raw.original_offer_payload,
        observed_at,
        node_scope,
        binding_revision,
        disposition,
        seal_evidence,
    })
}

fn validate_affinity_scope_shape(
    disposition: AssignmentAffinityDisposition,
    node_scope: Option<NodeTeamBindingScope>,
    binding_revision: Option<u64>,
) -> Result<(), OperationsError> {
    match (disposition, node_scope, binding_revision) {
        (
            AssignmentAffinityDisposition::LegacyUnknown
            | AssignmentAffinityDisposition::LegacyQuiesced,
            None,
            None,
        )
        | (
            AssignmentAffinityDisposition::Pending
            | AssignmentAffinityDisposition::Active
            | AssignmentAffinityDisposition::Terminal
            | AssignmentAffinityDisposition::Quiesced,
            Some(_),
            _,
        ) => Ok(()),
        _ => Err(OperationsError::Corrupt("assignment affinity scope shape")),
    }
}

fn validate_offer_payload_provenance(
    disposition: AssignmentAffinityDisposition,
    payload: Option<&[u8]>,
) -> Result<(), OperationsError> {
    if payload.is_some_and(|payload| {
        payload.is_empty() || payload.len() > MAX_ORIGINAL_OFFER_PAYLOAD_BYTES
    }) {
        return Err(OperationsError::Corrupt("assignment offer payload size"));
    }
    let legacy = matches!(
        disposition,
        AssignmentAffinityDisposition::LegacyUnknown
            | AssignmentAffinityDisposition::LegacyQuiesced
    );
    if legacy != payload.is_none() {
        return Err(OperationsError::Corrupt(
            "assignment offer payload provenance",
        ));
    }
    Ok(())
}

fn validate_seal_evidence(
    disposition: AssignmentAffinityDisposition,
    activity_id: ActivityId,
    seal_evidence: Option<AssignmentOfferSealEvidence>,
) -> Result<(), OperationsError> {
    if matches!(
        disposition,
        AssignmentAffinityDisposition::Quiesced | AssignmentAffinityDisposition::LegacyQuiesced
    ) != seal_evidence.is_some()
    {
        return Err(OperationsError::Corrupt(
            "assignment seal evidence disposition",
        ));
    }
    let Some(evidence) = seal_evidence else {
        return Ok(());
    };
    let valid = match evidence.observation {
        AssignmentOfferSealObservationKind::AbsentNowSealed => {
            evidence.current_activity_id.is_none()
                && evidence.current_state.is_none()
                && evidence.current_revision.is_none()
        }
        AssignmentOfferSealObservationKind::ForeignActivity => {
            matches!(
                disposition,
                AssignmentAffinityDisposition::Quiesced
                    | AssignmentAffinityDisposition::LegacyQuiesced
            ) && evidence
                .current_activity_id
                .is_some_and(|activity| activity != activity_id)
                && evidence.current_state.is_some()
                && evidence.current_revision.is_some()
        }
        AssignmentOfferSealObservationKind::LegacyTerminalSameActivity => {
            disposition == AssignmentAffinityDisposition::LegacyQuiesced
                && evidence.current_activity_id == Some(activity_id)
                && evidence
                    .current_state
                    .is_some_and(AgentAssignmentState::is_terminal)
                && evidence.current_revision.is_some()
        }
    };
    if valid {
        Ok(())
    } else {
        Err(OperationsError::Corrupt(
            "assignment seal evidence identity",
        ))
    }
}

fn decode_seal_evidence(
    raw: &RawAffinityAdmission,
) -> Result<Option<AssignmentOfferSealEvidence>, OperationsError> {
    let columns_absent = raw.seal_sealed_at.is_none()
        && raw.seal_applied_at.is_none()
        && raw.seal_authority_kind.is_none()
        && raw.seal_authority_agent.is_none()
        && raw.seal_authority_session.is_none()
        && raw.seal_authority_revision.is_none()
        && raw.seal_observation_kind.is_none()
        && raw.seal_current_activity.is_none()
        && raw.seal_current_state.is_none()
        && raw.seal_current_revision.is_none();
    if columns_absent {
        return Ok(None);
    }
    let sealed_at = TimestampMillis::new(
        raw.seal_sealed_at
            .ok_or(OperationsError::Corrupt("assignment seal timestamp"))?,
    )
    .map_err(|_| OperationsError::Corrupt("assignment seal timestamp"))?;
    let applied_at = TimestampMillis::new(raw.seal_applied_at.ok_or(OperationsError::Corrupt(
        "assignment seal applied timestamp",
    ))?)
    .map_err(|_| OperationsError::Corrupt("assignment seal applied timestamp"))?;
    let authority = match raw.seal_authority_kind.as_deref() {
        Some("LOCAL_OPERATOR")
            if raw.seal_authority_agent.is_none()
                && raw.seal_authority_session.is_none()
                && raw.seal_authority_revision.is_none() =>
        {
            AgentAssignmentOfferSealAuthority::LocalOperator
        }
        Some("COORDINATOR") => AgentAssignmentOfferSealAuthority::Coordinator {
            agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(
                raw.seal_authority_agent
                    .clone()
                    .ok_or(OperationsError::Corrupt("assignment seal authority agent"))?,
            )?)),
            session_id: AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(
                raw.seal_authority_session
                    .clone()
                    .ok_or(OperationsError::Corrupt(
                        "assignment seal authority session",
                    ))?,
            )?)),
            expected_coordinator_revision: forge_domain::ActivityCoordinatorRevision::new(
                raw.seal_authority_revision.ok_or(OperationsError::Corrupt(
                    "assignment seal authority revision",
                ))?,
            )
            .map_err(|_| OperationsError::Corrupt("assignment seal authority revision"))?,
        },
        _ => return Err(OperationsError::Corrupt("assignment seal authority")),
    };
    let (observation, current_activity_id, current_state, current_revision) =
        match raw.seal_observation_kind.as_deref() {
            Some("ABSENT_NOW_SEALED")
                if raw.seal_current_activity.is_none()
                    && raw.seal_current_state.is_none()
                    && raw.seal_current_revision.is_none() =>
            {
                (
                    AssignmentOfferSealObservationKind::AbsentNowSealed,
                    None,
                    None,
                    None,
                )
            }
            Some("FOREIGN_ACTIVITY" | "LEGACY_TERMINAL_SAME_ACTIVITY") => {
                let activity = ActivityId::from_uuid(Uuid::from_bytes(wire::id(
                    raw.seal_current_activity
                        .clone()
                        .ok_or(OperationsError::Corrupt("assignment seal current Activity"))?,
                )?));
                let state = decode_assignment_state(
                    raw.seal_current_state
                        .as_deref()
                        .ok_or(OperationsError::Corrupt("assignment seal current state"))?,
                )?;
                let revision = decode_positive_revision(
                    raw.seal_current_revision
                        .ok_or(OperationsError::Corrupt("assignment seal current revision"))?,
                    "assignment seal current revision",
                )?;
                let kind = if raw.seal_observation_kind.as_deref() == Some("FOREIGN_ACTIVITY") {
                    AssignmentOfferSealObservationKind::ForeignActivity
                } else {
                    AssignmentOfferSealObservationKind::LegacyTerminalSameActivity
                };
                (kind, Some(activity), Some(state), Some(revision))
            }
            _ => return Err(OperationsError::Corrupt("assignment seal observation")),
        };
    Ok(Some(AssignmentOfferSealEvidence {
        sealed_at,
        applied_at,
        authority,
        observation,
        current_activity_id,
        current_state,
        current_revision,
    }))
}

fn encode_assignment_state(state: AgentAssignmentState) -> &'static str {
    match state {
        AgentAssignmentState::Offered => "OFFERED",
        AgentAssignmentState::Running => "RUNNING",
        AgentAssignmentState::Submitted => "SUBMITTED",
        AgentAssignmentState::Accepted => "ACCEPTED",
        AgentAssignmentState::Rejected => "REJECTED",
        AgentAssignmentState::Cancelled => "CANCELLED",
        AgentAssignmentState::Interrupted => "INTERRUPTED",
    }
}

fn decode_assignment_state(value: &str) -> Result<AgentAssignmentState, OperationsError> {
    match value {
        "OFFERED" => Ok(AgentAssignmentState::Offered),
        "RUNNING" => Ok(AgentAssignmentState::Running),
        "SUBMITTED" => Ok(AgentAssignmentState::Submitted),
        "ACCEPTED" => Ok(AgentAssignmentState::Accepted),
        "REJECTED" => Ok(AgentAssignmentState::Rejected),
        "CANCELLED" => Ok(AgentAssignmentState::Cancelled),
        "INTERRUPTED" => Ok(AgentAssignmentState::Interrupted),
        _ => Err(OperationsError::Corrupt("assignment seal current state")),
    }
}

fn decode_node_scope(
    workspace_id: WorkspaceId,
    activity_id: ActivityId,
    goal_id: Option<Vec<u8>>,
    goal_created_at_ms: Option<i64>,
    dag_revision: Option<i64>,
    dag_base_goal_revision: Option<i64>,
    node_id: Option<Vec<u8>>,
) -> Result<Option<NodeTeamBindingScope>, OperationsError> {
    match (
        goal_id,
        goal_created_at_ms,
        dag_revision,
        dag_base_goal_revision,
        node_id,
    ) {
        (None, None, None, None, None) => Ok(None),
        (Some(goal), Some(created_at), Some(dag), Some(base), Some(node)) => {
            Ok(Some(NodeTeamBindingScope::new(
                workspace_id,
                activity_id,
                GoalId::from_uuid(Uuid::from_bytes(wire::id(goal)?)),
                TimestampMillis::new(created_at)
                    .map_err(|_| OperationsError::Corrupt("assignment affinity Goal generation"))?,
                DagRevisionNumber::new(dag)
                    .map_err(|_| OperationsError::Corrupt("assignment affinity DAG revision"))?,
                GoalRevision::new(base).map_err(|_| {
                    OperationsError::Corrupt("assignment affinity DAG base Goal revision")
                })?,
                DagNodeId::from_uuid(Uuid::from_bytes(wire::id(node)?)),
            )?))
        }
        _ => Err(OperationsError::Corrupt("assignment affinity Node scope")),
    }
}

fn decode_positive_revision(value: i64, name: &'static str) -> Result<u64, OperationsError> {
    let revision = u64::try_from(value).map_err(|_| OperationsError::Corrupt(name))?;
    if revision == 0 {
        return Err(OperationsError::Corrupt(name));
    }
    Ok(revision)
}

fn optional_uuid(value: Option<Vec<u8>>) -> Result<Option<Uuid>, OperationsError> {
    value
        .map(|value| Ok(Uuid::from_bytes(wire::id(value)?)))
        .transpose()
}

fn with_replayed(mut stored: StoredAffinity, replayed: bool) -> AssignmentAffinityReceipt {
    stored.receipt.replayed = replayed;
    stored.receipt
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OperationalActor;
    use forge_domain::AgentAssignmentState;
    use rusqlite::params;
    use std::{error::Error, fs};

    struct Fixture {
        root: std::path::PathBuf,
        catalog: OperationsCatalog,
        workspace: WorkspaceId,
        workspace_b: WorkspaceId,
        team: Uuid,
        participation: WorkspaceParticipationId,
        activity: ActivityId,
        agent: AgentInstanceId,
        session: AgentSessionId,
        scope: NodeTeamBindingScope,
        unassigned_scope: NodeTeamBindingScope,
    }

    fn scope_for(
        workspace: WorkspaceId,
        activity: ActivityId,
        seed: u128,
    ) -> Result<NodeTeamBindingScope, Box<dyn Error>> {
        Ok(NodeTeamBindingScope::new(
            workspace,
            activity,
            GoalId::from_uuid(Uuid::from_u128(seed)),
            TimestampMillis::new(50)?,
            DagRevisionNumber::new(1)?,
            GoalRevision::INITIAL,
            DagNodeId::from_uuid(Uuid::from_u128(seed + 1)),
        )?)
    }

    fn binding_command(
        scope: NodeTeamBindingScope,
        expected_revision: Option<u64>,
        team_id: Option<Uuid>,
        observed_at_ms: i64,
    ) -> Result<crate::SetNodeTeamBindingCommand, Box<dyn Error>> {
        Ok(crate::SetNodeTeamBindingCommand {
            scope,
            expected_revision,
            team_id,
            actor: OperationalActor::LocalOperator,
            expected_sender_leadership_revision: None,
            observed_at: TimestampMillis::new(observed_at_ms)?,
        })
    }

    fn seed_team_peer(
        catalog: &OperationsCatalog,
        workspace: WorkspaceId,
        activity: ActivityId,
    ) -> Result<(AgentInstanceId, AgentSessionId), Box<dyn Error>> {
        let participant = Uuid::from_u128(0xAF01);
        let profile = Uuid::from_u128(0xAF02);
        let participation = Uuid::from_u128(0xAF03);
        let session = AgentSessionId::from_uuid(Uuid::from_u128(0xAF04));
        let agent = AgentInstanceId::from_uuid(Uuid::from_u128(0xAF05));
        catalog.connection.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,'Team verifier','worker',1,1,1)",
            params![workspace.into_uuid().as_bytes(),participant.as_bytes(),session.into_uuid().as_bytes()],
        )?;
        catalog.connection.execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
            params![workspace.into_uuid().as_bytes(),participant.as_bytes(),activity.into_uuid().as_bytes(),session.into_uuid().as_bytes(),agent.into_uuid().as_bytes(),Uuid::from_u128(0xAF06).as_bytes()],
        )?;
        catalog.connection.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,1,1)",
            params![workspace.into_uuid().as_bytes(),profile.as_bytes(),participation.as_bytes()],
        )?;
        catalog.connection.execute(
            "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,1)",
            params![Uuid::from_u128(0xAF07).as_bytes(),workspace.into_uuid().as_bytes(),participation.as_bytes(),profile.as_bytes(),Uuid::from_u128(0xAF08).as_bytes(),session.into_uuid().as_bytes(),participant.as_bytes()],
        )?;
        Ok((agent, session))
    }

    fn fixture() -> Result<Fixture, Box<dyn Error>> {
        let root =
            std::env::temp_dir().join(format!("orchestration-team-affinity-{}", Uuid::now_v7()));
        fs::create_dir(&root)?;
        let destination = Uuid::from_u128(0xA001);
        let workspace = WorkspaceId::from_uuid(Uuid::from_u128(0xA002));
        let workspace_b = WorkspaceId::from_uuid(Uuid::from_u128(0xA003));
        let team = Uuid::from_u128(0xA004);
        let participation = WorkspaceParticipationId::from_uuid(Uuid::from_u128(0xA005))?;
        let profile = Uuid::from_u128(0xA006);
        let participant = Uuid::from_u128(0xA007);
        let session = AgentSessionId::from_uuid(Uuid::from_u128(0xA008));
        let activity = ActivityId::from_uuid(Uuid::from_u128(0xA009));
        let agent = AgentInstanceId::from_uuid(Uuid::from_u128(0xA00A));
        let mut catalog = OperationsCatalog::open(&root.join("operations.sqlite3"), destination)?;
        catalog.connection.execute("INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",params![Uuid::from_u128(0xA013).as_bytes(),workspace.into_uuid().as_bytes(),destination.as_bytes(),[0_u8;32].as_slice()])?;

        catalog.connection.execute(
			"INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,'Team target','worker',1,1,1)",
			params![workspace.into_uuid().as_bytes(), participant.as_bytes(), session.into_uuid().as_bytes()],
		)?;
        catalog.connection.execute(
			"INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
			params![workspace.into_uuid().as_bytes(), participant.as_bytes(), activity.into_uuid().as_bytes(), session.into_uuid().as_bytes(), agent.into_uuid().as_bytes(), Uuid::from_u128(0xA00B).as_bytes()],
		)?;
        catalog.connection.execute(
			"INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,1,1)",
			params![workspace.into_uuid().as_bytes(), profile.as_bytes(), participation.into_uuid().as_bytes()],
		)?;
        catalog.connection.execute(
			"INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,1)",
			params![Uuid::from_u128(0xA00C).as_bytes(), workspace.into_uuid().as_bytes(), participation.into_uuid().as_bytes(), profile.as_bytes(), Uuid::from_u128(0xA00D).as_bytes(), session.into_uuid().as_bytes(), participant.as_bytes()],
		)?;
        let (peer_agent, peer_session) = seed_team_peer(&catalog, workspace, activity)?;
        let _formed = catalog.create_team(crate::TeamCreateCommand {
            request_id: Uuid::from_u128(0xAF09),
            team_id: team,
            workspace_id: workspace,
            actor: OperationalActor::LocalOperator,
            expected_sender_leadership_revision: None,
            name: "Team".to_owned(),
            purpose: "scope".to_owned(),
            initial_members: vec![
                crate::TeamRosterMemberInput {
                    activity_id: activity,
                    agent_id: agent,
                    session_id: session,
                    role: "implementation".to_owned(),
                },
                crate::TeamRosterMemberInput {
                    activity_id: activity,
                    agent_id: peer_agent,
                    session_id: peer_session,
                    role: "verification".to_owned(),
                },
            ],
            observed_at: TimestampMillis::new(1)?,
        })?;
        // Existing cases exercise a later exact membership revision.
        catalog.connection.execute("UPDATE ops_team_members SET revision=4 WHERE workspace_id=?1 AND team_id=?2 AND participation_id=?3", params![workspace.into_uuid().as_bytes(),team.as_bytes(),participation.into_uuid().as_bytes()])?;
        let scope = scope_for(workspace, activity, 0xA100)?;
        let unassigned_scope = scope_for(workspace, activity, 0xA200)?;
        let _binding =
            catalog.set_node_team_binding(binding_command(scope, None, Some(team), 50)?)?;
        Ok(Fixture {
            root,
            catalog,
            workspace,
            workspace_b,
            team,
            participation,
            activity,
            agent,
            session,
            scope,
            unassigned_scope,
        })
    }

    fn command(
        fixture: &Fixture,
        assignment: u128,
        team_id: Option<Uuid>,
    ) -> Result<AssignmentAffinityCommand, Box<dyn Error>> {
        Ok(AssignmentAffinityCommand {
            workspace_id: fixture.workspace,
            assignment_id: AgentAssignmentId::from_uuid(Uuid::from_u128(assignment)),
            activity_id: fixture.activity,
            target_agent_id: fixture.agent,
            target_session_id: fixture.session,
            requester: OperationalActor::LocalOperator,
            team_id,
            node_scope: if team_id.is_some() {
                fixture.scope
            } else {
                fixture.unassigned_scope
            },
            expected_binding_revision: team_id.map(|_| 1),
            assignment_payload_fingerprint: [0xA1; 32],
            original_offer_payload: format!("offer-{assignment}").into_bytes(),
            observed_at: TimestampMillis::new(100)?,
        })
    }

    #[test]
    fn records_team_affinity_and_exact_replay_preserves_first_observed_at()
    -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture()?;
        let first_command = command(&fixture, 0xA010, Some(fixture.team))?;
        let first = fixture.catalog.record_assignment_affinity(&first_command)?;
        assert_eq!(first.team_id(), Some(fixture.team));
        assert_eq!(first.team_membership_revision(), Some(4));
        assert_eq!(first.target_participation_id(), fixture.participation);
        fixture.catalog.connection.execute(
			"UPDATE ops_team_members SET revision=5 WHERE workspace_id=?1 AND team_id=?2 AND participation_id=?3",
			params![fixture.workspace.into_uuid().as_bytes(), fixture.team.as_bytes(), fixture.participation.into_uuid().as_bytes()],
		)?;
        let replay_command = command(&fixture, 0xA010, Some(fixture.team))?;
        let replay = fixture
            .catalog
            .record_assignment_affinity(&replay_command)?;
        assert!(replay.replayed());
        assert_eq!(replay.observed_at().get(), 100);
        let mut changed_payload = command(&fixture, 0xA010, Some(fixture.team))?;
        changed_payload.original_offer_payload.push(b'!');
        assert!(matches!(
            fixture.catalog.record_assignment_affinity(&changed_payload),
            Err(OperationsError::Conflict("assignment affinity replay"))
        ));
        assert_eq!(
            admitted_assignment_team(
                &fixture.catalog.connection,
                fixture.workspace,
                replay.assignment_id()
            )?,
            Some(fixture.team)
        );
        fixture.catalog.connection.execute(
			"UPDATE ops_team_members SET state='LEFT' WHERE workspace_id=?1 AND team_id=?2 AND participation_id=?3",
			params![fixture.workspace.into_uuid().as_bytes(), fixture.team.as_bytes(), fixture.participation.into_uuid().as_bytes()],
		)?;
        let departed_command = command(&fixture, 0xA011, Some(fixture.team))?;
        assert!(
            fixture
                .catalog
                .record_assignment_affinity(&departed_command)
                .is_err()
        );
        fixture.catalog.connection.execute(
            "UPDATE ops_team_members SET state='JOINED' WHERE workspace_id=?1 AND team_id=?2 AND participation_id=?3",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                fixture.team.as_bytes(),
                fixture.participation.into_uuid().as_bytes()
            ],
        )?;
        fixture.catalog.connection.execute(
            "DELETE FROM ops_current_effect_bindings WHERE workspace_id=?1 AND activity_id=?2 AND agent_id=?3",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                fixture.activity.into_uuid().as_bytes(),
                fixture.agent.into_uuid().as_bytes()
            ],
        )?;
        let departed_target_command = command(&fixture, 0xA011, None)?;
        assert!(
            fixture
                .catalog
                .record_assignment_affinity(&departed_target_command)
                .is_err()
        );
        drop(fixture.catalog);
        fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn rejects_cross_workspace_and_retrofit_after_observed_offer() -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture()?;
        let first_command = command(&fixture, 0xA012, None)?;
        let first = fixture.catalog.record_assignment_affinity(&first_command)?;
        let mut cross = command(&fixture, 0xA012, None)?;
        cross.workspace_id = fixture.workspace_b;
        cross.node_scope = scope_for(fixture.workspace_b, fixture.activity, 0xA200)?;
        assert!(matches!(
            fixture.catalog.record_assignment_affinity(&cross),
            Err(OperationsError::Conflict(
                "assignment affinity workspace scope"
            ))
        ));
        let migration = Uuid::from_u128(0xA013);
        fixture.catalog.connection.execute(
			"INSERT INTO ops_effect_fact_consumers(migration_id,workspace_id,destination_id,last_sequence,applied_count) VALUES(?1,?2,?3,0,0)",
			params![migration.as_bytes(), fixture.workspace.into_uuid().as_bytes(), fixture.catalog.destination_id().as_bytes()],
		)?;
        let retrofit = command(&fixture, 0xA014, None)?;
        fixture.catalog.connection.execute(
			"INSERT INTO ops_assignment_offer_projection(migration_id,source_sequence,fact_digest,assignment_id,workspace_id,activity_id,goal_id,node_id,target_agent_id,target_session_id,credit_budget,offered_at_ms) VALUES(?1,1,?2,?3,?4,?5,?6,?7,?8,?9,NULL,100)",
			params![migration.as_bytes(), [1u8;32].as_slice(), retrofit.assignment_id.into_uuid().as_bytes(), fixture.workspace.into_uuid().as_bytes(), fixture.activity.into_uuid().as_bytes(), Uuid::from_u128(0xA015).as_bytes(), Uuid::from_u128(0xA016).as_bytes(), fixture.agent.into_uuid().as_bytes(), fixture.session.into_uuid().as_bytes()],
		)?;
        assert!(matches!(
            fixture.catalog.record_assignment_affinity(&retrofit),
            Err(OperationsError::Conflict(
                "assignment offer predates affinity admission"
            ))
        ));
        assert_eq!(first.team_id(), None);
        assert!(
            fixture
                .catalog
                .read_assignment_affinity(
                    fixture.workspace,
                    AgentAssignmentId::from_uuid(Uuid::from_u128(0xA099))
                )?
                .is_none()
        );
        drop(fixture.catalog);
        fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    fn registered_assignment(
        fixture: &Fixture,
        assignment_id: AgentAssignmentId,
        state: AgentAssignmentState,
    ) -> Result<RegisteredAgentAssignment, Box<dyn Error>> {
        registered_assignment_for_activity(fixture, assignment_id, fixture.activity, state)
    }

    fn registered_assignment_for_activity(
        fixture: &Fixture,
        assignment_id: AgentAssignmentId,
        activity_id: ActivityId,
        state: AgentAssignmentState,
    ) -> Result<RegisteredAgentAssignment, Box<dyn Error>> {
        let scope = fixture.scope;
        let coordinator = forge_domain::AgentAssignmentCoordinator::new(
            AgentInstanceId::from_uuid(Uuid::from_u128(0xA2AA)),
            AgentSessionId::from_uuid(Uuid::from_u128(0xA2AB)),
            forge_domain::AuthenticatedPrincipal::TrustedStdio,
            forge_domain::ActivityCoordinatorRevision::INITIAL,
        );
        let target = forge_domain::AgentAssignmentTarget::new(
            fixture.agent,
            fixture.session,
            forge_domain::AuthenticatedPrincipal::TrustedStdio,
            forge_domain::AgentInstanceRevision::INITIAL,
        );
        if state == AgentAssignmentState::Offered {
            return Ok(RegisteredAgentAssignment::offered(
                assignment_id,
                fixture.workspace,
                activity_id,
                scope.goal_id(),
                scope.goal_created_at(),
                GoalRevision::INITIAL,
                scope.dag_revision(),
                forge_domain::DagHeadRevision::INITIAL,
                scope.node_id(),
                forge_domain::DagNodeRevision::INITIAL,
                coordinator,
                target,
                None,
                TimestampMillis::new(100)?,
            )?);
        }
        let result = matches!(
            state,
            AgentAssignmentState::Submitted
                | AgentAssignmentState::Accepted
                | AgentAssignmentState::Rejected
        )
        .then(|| {
            forge_domain::AgentAssignmentResult::new(
                "reviewed fixture result".to_owned(),
                Vec::new(),
                None,
            )
        })
        .transpose()?;
        let reviewed_by = matches!(
            state,
            AgentAssignmentState::Accepted | AgentAssignmentState::Rejected
        )
        .then_some(coordinator);
        Ok(RegisteredAgentAssignment::from_parts(
            assignment_id,
            fixture.workspace,
            activity_id,
            scope.goal_id(),
            scope.goal_created_at(),
            GoalRevision::INITIAL,
            scope.dag_revision(),
            forge_domain::DagHeadRevision::INITIAL,
            scope.node_id(),
            forge_domain::DagNodeRevision::INITIAL,
            coordinator,
            target,
            state,
            false,
            None,
            result,
            reviewed_by,
            TimestampMillis::new(100)?,
            TimestampMillis::new(101)?,
            forge_domain::AgentAssignmentRevision::new(2)?,
            forge_domain::AgentAssignmentOrigin::CoordinatorOffer,
        )?)
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "One regression covers known/legacy seal decisions and durable evidence preservation."
    )]
    fn offer_seal_quiescence_preserves_evidence_and_never_invents_legacy_scope()
    -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture()?;
        let admission = command(&fixture, 0xA030, Some(fixture.team))?;
        let assignment_id = admission.assignment_id;
        let pending = fixture.catalog.record_assignment_affinity(&admission)?;
        let absent = classify_seal_observation(
            pending.disposition(),
            fixture.workspace,
            assignment_id,
            fixture.activity,
            None,
        )?;
        assert_eq!(absent.disposition, AssignmentAffinityDisposition::Quiesced);
        assert_eq!(
            absent.observation,
            AssignmentOfferSealObservationKind::AbsentNowSealed
        );
        let foreign_activity = ActivityId::from_uuid(Uuid::from_u128(0xA0FF));
        let foreign = registered_assignment_for_activity(
            &fixture,
            assignment_id,
            foreign_activity,
            AgentAssignmentState::Running,
        )?;
        let foreign_decision = classify_seal_observation(
            pending.disposition(),
            fixture.workspace,
            assignment_id,
            fixture.activity,
            Some(&foreign),
        )?;
        assert_eq!(
            foreign_decision.disposition,
            AssignmentAffinityDisposition::Quiesced
        );
        assert_eq!(
            foreign_decision.observation,
            AssignmentOfferSealObservationKind::ForeignActivity
        );
        assert_eq!(foreign_decision.current_activity_id, Some(foreign_activity));
        assert_eq!(
            foreign_decision.current_state,
            Some(AgentAssignmentState::Running)
        );
        let same_activity =
            registered_assignment(&fixture, assignment_id, AgentAssignmentState::Offered)?;
        assert!(matches!(
            classify_seal_observation(
                pending.disposition(),
                fixture.workspace,
                assignment_id,
                fixture.activity,
                Some(&same_activity),
            ),
            Err(OperationsError::Conflict(
                "same-Activity assignment requires exact offer reconciliation"
            ))
        ));
        let wrong_id = registered_assignment(
            &fixture,
            AgentAssignmentId::from_uuid(Uuid::from_u128(0xA0FE)),
            AgentAssignmentState::Offered,
        )?;
        assert!(matches!(
            classify_seal_observation(
                pending.disposition(),
                fixture.workspace,
                assignment_id,
                fixture.activity,
                Some(&wrong_id),
            ),
            Err(OperationsError::Conflict(
                "assignment offer seal current identity"
            ))
        ));

        let evidence = AssignmentOfferSealEvidence {
            sealed_at: TimestampMillis::new(150)?,
            applied_at: TimestampMillis::new(151)?,
            authority: AgentAssignmentOfferSealAuthority::LocalOperator,
            observation: absent.observation,
            current_activity_id: None,
            current_state: None,
            current_revision: None,
        };
        let transaction = fixture
            .catalog
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let stored = load_affinity(&transaction, fixture.workspace, assignment_id)?
            .ok_or("pending affinity missing")?;
        let quiesced = persist_seal_evidence(&transaction, &stored, absent.disposition, evidence)?;
        transaction.commit()?;
        assert_eq!(
            quiesced.disposition(),
            AssignmentAffinityDisposition::Quiesced
        );
        assert_eq!(quiesced.team_id(), Some(fixture.team));
        assert_eq!(quiesced.assignment_payload_fingerprint(), [0xA1; 32]);
        assert_eq!(
            quiesced.original_offer_payload(),
            Some(admission.original_offer_payload.as_slice())
        );
        assert_eq!(quiesced.observed_at().get(), 100);
        assert_eq!(quiesced.seal_evidence(), Some(&evidence));
        let stale_same_activity =
            registered_assignment(&fixture, assignment_id, AgentAssignmentState::Offered)?;
        let unchanged = fixture
            .catalog
            .reconcile_assignment_affinity(&stale_same_activity, fixture.scope)?;
        assert_eq!(
            unchanged.disposition(),
            AssignmentAffinityDisposition::Quiesced
        );
        assert_eq!(unchanged.seal_evidence(), Some(&evidence));
        let reservation_tx = fixture.catalog.connection.unchecked_transaction()?;
        assert!(!has_pending_or_active_affinity_for_scope(
            &reservation_tx,
            fixture.scope,
        )?);
        reservation_tx.commit()?;
        let catalog_path = fixture.root.join("operations.sqlite3");
        let destination = fixture.catalog.destination_id();
        drop(fixture.catalog);
        fixture.catalog = OperationsCatalog::open(&catalog_path, destination)?;
        let reopened = fixture
            .catalog
            .read_assignment_affinity(fixture.workspace, assignment_id)?
            .ok_or("quiesced affinity missing after reopen")?;
        assert_eq!(
            reopened.disposition(),
            AssignmentAffinityDisposition::Quiesced
        );
        assert_eq!(reopened.seal_evidence(), Some(&evidence));

        let legacy_command = command(&fixture, 0xA031, None)?;
        let _ = fixture
            .catalog
            .record_assignment_affinity(&legacy_command)?;
        fixture.catalog.connection.execute(
            "UPDATE ops_assignment_team_affinities SET node_goal_id=NULL,goal_created_at_ms=NULL,node_dag_revision=NULL,node_dag_base_goal_revision=NULL,node_id=NULL,node_binding_revision=NULL,disposition='LEGACY_UNKNOWN',original_offer_payload=NULL WHERE workspace_id=?1 AND assignment_id=?2",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                legacy_command.assignment_id.into_uuid().as_bytes(),
            ],
        )?;
        let legacy_terminal = registered_assignment(
            &fixture,
            legacy_command.assignment_id,
            AgentAssignmentState::Accepted,
        )?;
        let legacy_decision = classify_seal_observation(
            AssignmentAffinityDisposition::LegacyUnknown,
            fixture.workspace,
            legacy_command.assignment_id,
            fixture.activity,
            Some(&legacy_terminal),
        )?;
        assert_eq!(
            legacy_decision.disposition,
            AssignmentAffinityDisposition::LegacyQuiesced
        );
        assert_eq!(
            legacy_decision.observation,
            AssignmentOfferSealObservationKind::LegacyTerminalSameActivity
        );
        let legacy_evidence = AssignmentOfferSealEvidence {
            sealed_at: TimestampMillis::new(160)?,
            applied_at: TimestampMillis::new(161)?,
            authority: AgentAssignmentOfferSealAuthority::LocalOperator,
            observation: legacy_decision.observation,
            current_activity_id: legacy_decision.current_activity_id,
            current_state: legacy_decision.current_state,
            current_revision: legacy_decision.current_revision,
        };
        let transaction = fixture
            .catalog
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let stored = load_affinity(
            &transaction,
            fixture.workspace,
            legacy_command.assignment_id,
        )?
        .ok_or("legacy affinity missing")?;
        let _ = persist_seal_evidence(
            &transaction,
            &stored,
            legacy_decision.disposition,
            legacy_evidence,
        )?;
        transaction.commit()?;
        let legacy = fixture
            .catalog
            .read_assignment_affinity(fixture.workspace, legacy_command.assignment_id)?
            .ok_or("legacy affinity readback missing")?;
        assert_eq!(
            legacy.disposition(),
            AssignmentAffinityDisposition::LegacyQuiesced
        );
        assert_eq!(legacy.node_scope(), None);
        assert_eq!(legacy.original_offer_payload(), None);
        assert_eq!(legacy.seal_evidence(), Some(&legacy_evidence));
        drop(fixture.catalog);
        fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn legacy_binding_retirement_is_cas_and_ignores_unrelated_active_scope()
    -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture()?;
        let admission = command(&fixture, 0xA040, Some(fixture.team))?;
        let assignment_id = admission.assignment_id;
        let _ = fixture.catalog.record_assignment_affinity(&admission)?;
        let offered =
            registered_assignment(&fixture, assignment_id, AgentAssignmentState::Offered)?;
        let _ = fixture
            .catalog
            .reconcile_assignment_affinity(&offered, fixture.scope)?;
        let legacy_scope = fixture.unassigned_scope;
        fixture.catalog.connection.execute(
            "INSERT INTO ops_dag_node_team_bindings(workspace_id,activity_id,goal_id,goal_created_at_ms,dag_revision,dag_base_goal_revision,node_id,team_id,bound_by,bound_at_ms,revision) VALUES(?1,NULL,?2,NULL,?3,NULL,?4,?5,'operator',50,5)",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                legacy_scope.goal_id().into_uuid().as_bytes(),
                legacy_scope.dag_revision().get(),
                legacy_scope.node_id().into_uuid().as_bytes(),
                fixture.team.as_bytes(),
            ],
        )?;
        let request_id = Uuid::from_u128(0xA041);
        let workspace = fixture.workspace;
        let retirement_command =
            |expected_revision, observed_at| crate::RetireLegacyNodeTeamBindingCommand {
                workspace_id: workspace,
                goal_id: legacy_scope.goal_id(),
                dag_revision: legacy_scope.dag_revision(),
                node_id: legacy_scope.node_id(),
                expected_revision,
                request_id,
                actor: OperationalActor::LocalOperator,
                expected_sender_leadership_revision: None,
                observed_at,
            };
        assert!(matches!(
            fixture
                .catalog
                .retire_legacy_node_team_binding(
                    retirement_command(4, TimestampMillis::new(102)?,)
                ),
            Err(OperationsError::Conflict("legacy node binding revision"))
        ));
        let receipt = fixture
            .catalog
            .retire_legacy_node_team_binding(retirement_command(5, TimestampMillis::new(103)?))?;
        assert_eq!(receipt.request_id(), request_id);
        assert_eq!(receipt.binding_revision(), 5);
        assert_eq!(receipt.team_id(), Some(fixture.team));
        assert!(matches!(
            receipt.authority(),
            crate::LegacyNodeTeamBindingRetirementAuthority::LocalOperator
        ));
        assert_eq!(
            fixture
                .catalog
                .read_legacy_node_team_binding_retirement(request_id)?,
            Some(receipt.clone())
        );
        let replay = fixture
            .catalog
            .retire_legacy_node_team_binding(retirement_command(5, TimestampMillis::new(104)?))?;
        assert!(replay.replayed());
        assert_eq!(replay.retired_at(), receipt.retired_at());
        assert_eq!(
            fixture
                .catalog
                .read_assignment_affinity(fixture.workspace, assignment_id)?
                .map(|receipt| receipt.disposition()),
            Some(AssignmentAffinityDisposition::Active)
        );
        let legacy = fixture
            .catalog
            .legacy_node_team_binding(
                fixture.workspace,
                legacy_scope.goal_id(),
                legacy_scope.dag_revision(),
                legacy_scope.node_id(),
            )?
            .ok_or("retired legacy row disappeared")?;
        assert_eq!(legacy.scope(), None);
        assert_eq!(legacy.team_id(), Some(fixture.team));
        assert_eq!(legacy.revision(), 5);
        assert_eq!(legacy.legacy_retirement(), Some(&receipt));
        let exact_binding = fixture.catalog.set_node_team_binding(binding_command(
            legacy_scope,
            None,
            Some(fixture.team),
            104,
        )?)?;
        assert_eq!(exact_binding.scope(), Some(legacy_scope));
        assert_eq!(exact_binding.team_id(), Some(fixture.team));
        drop(fixture.catalog);
        fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn reservation_page_filters_before_limit_and_pages_legacy_unknown_by_assignment()
    -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture()?;
        let lower_other_scope = command(&fixture, 0xA049, None)?;
        let _ = fixture
            .catalog
            .record_assignment_affinity(&lower_other_scope)?;
        let exact = command(&fixture, 0xA050, Some(fixture.team))?;
        let _ = fixture.catalog.record_assignment_affinity(&exact)?;
        let legacy = command(&fixture, 0xA052, None)?;
        let _ = fixture.catalog.record_assignment_affinity(&legacy)?;
        fixture.catalog.connection.execute(
            "UPDATE ops_assignment_team_affinities SET node_goal_id=NULL,goal_created_at_ms=NULL,node_dag_revision=NULL,node_dag_base_goal_revision=NULL,node_id=NULL,node_binding_revision=NULL,disposition='LEGACY_UNKNOWN',original_offer_payload=NULL WHERE workspace_id=?1 AND assignment_id=?2",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                legacy.assignment_id.into_uuid().as_bytes(),
            ],
        )?;
        let legacy_scope = fixture.unassigned_scope;
        fixture.catalog.connection.execute(
            "INSERT INTO ops_dag_node_team_bindings(workspace_id,activity_id,goal_id,goal_created_at_ms,dag_revision,dag_base_goal_revision,node_id,team_id,bound_by,bound_at_ms,revision) VALUES(?1,NULL,?2,NULL,?3,NULL,?4,?5,'operator',50,5)",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                legacy_scope.goal_id().into_uuid().as_bytes(),
                legacy_scope.dag_revision().get(),
                legacy_scope.node_id().into_uuid().as_bytes(),
                fixture.team.as_bytes(),
            ],
        )?;
        assert!(matches!(
            fixture.catalog.retire_legacy_node_team_binding(
                crate::RetireLegacyNodeTeamBindingCommand {
                    workspace_id: fixture.workspace,
                    goal_id: legacy_scope.goal_id(),
                    dag_revision: legacy_scope.dag_revision(),
                    node_id: legacy_scope.node_id(),
                    expected_revision: 5,
                    request_id: Uuid::from_u128(0xA053),
                    actor: OperationalActor::LocalOperator,
                    expected_sender_leadership_revision: None,
                    observed_at: TimestampMillis::new(100)?,
                }
            ),
            Err(OperationsError::Conflict(
                "legacy binding retirement has unresolved assignment reservation"
            ))
        ));
        let first = fixture
            .catalog
            .assignment_affinity_reservations(fixture.scope, None, 1)?;
        assert_eq!(first.receipts().len(), 1);
        assert_eq!(first.receipts()[0].assignment_id(), exact.assignment_id);
        assert_eq!(
            first.receipts()[0].disposition(),
            AssignmentAffinityDisposition::Pending
        );
        assert_eq!(first.next_assignment(), Some(exact.assignment_id));
        let second = fixture.catalog.assignment_affinity_reservations(
            fixture.scope,
            first.next_assignment(),
            1,
        )?;
        assert_eq!(second.receipts().len(), 1);
        assert_eq!(second.receipts()[0].assignment_id(), legacy.assignment_id);
        assert_eq!(
            second.receipts()[0].disposition(),
            AssignmentAffinityDisposition::LegacyUnknown
        );
        assert_eq!(second.receipts()[0].node_scope(), None);
        assert_eq!(second.next_assignment(), None);
        assert!(matches!(
            fixture
                .catalog
                .assignment_affinity_reservations(fixture.scope, None, 0),
            Err(OperationsError::Bounds("assignment reservation page"))
        ));
        drop(fixture.catalog);
        fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn legacy_coordinates_include_related_scoped_reservation_without_rewriting_its_scope()
    -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture()?;
        let legacy_scope = fixture.unassigned_scope;
        let related_activity = ActivityId::from_uuid(Uuid::from_u128(0xA0FD));
        let related_agent = AgentInstanceId::from_uuid(Uuid::from_u128(0xA0FE));
        let participant: Vec<u8> = fixture.catalog.connection.query_row(
            "SELECT participant_id FROM ops_current_effect_bindings WHERE workspace_id=?1 AND activity_id=?2 AND agent_id=?3",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                fixture.activity.into_uuid().as_bytes(),
                fixture.agent.into_uuid().as_bytes(),
            ],
            |row| row.get(0),
        )?;
        fixture.catalog.connection.execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                participant,
                related_activity.into_uuid().as_bytes(),
                fixture.session.into_uuid().as_bytes(),
                related_agent.into_uuid().as_bytes(),
                Uuid::from_u128(0xA0F0).as_bytes(),
            ],
        )?;
        let related_scope = NodeTeamBindingScope::new(
            fixture.workspace,
            related_activity,
            legacy_scope.goal_id(),
            legacy_scope.goal_created_at(),
            legacy_scope.dag_revision(),
            legacy_scope.dag_base_goal_revision(),
            legacy_scope.node_id(),
        )?;
        let _ = fixture.catalog.set_node_team_binding(binding_command(
            related_scope,
            None,
            Some(fixture.team),
            60,
        )?)?;
        let mut admission = command(&fixture, 0xA0F1, Some(fixture.team))?;
        admission.activity_id = related_activity;
        admission.target_agent_id = related_agent;
        admission.node_scope = related_scope;
        let _ = fixture.catalog.record_assignment_affinity(&admission)?;
        fixture.catalog.connection.execute(
            "INSERT INTO ops_dag_node_team_bindings(workspace_id,activity_id,goal_id,goal_created_at_ms,dag_revision,dag_base_goal_revision,node_id,team_id,bound_by,bound_at_ms,revision) VALUES(?1,NULL,?2,NULL,?3,NULL,?4,?5,'operator',50,5)",
            params![
                fixture.workspace.into_uuid().as_bytes(),
                legacy_scope.goal_id().into_uuid().as_bytes(),
                legacy_scope.dag_revision().get(),
                legacy_scope.node_id().into_uuid().as_bytes(),
                fixture.team.as_bytes(),
            ],
        )?;
        let different_generation = NodeTeamBindingScope::new(
            fixture.workspace,
            fixture.activity,
            legacy_scope.goal_id(),
            TimestampMillis::new(75)?,
            legacy_scope.dag_revision(),
            GoalRevision::new(2)?,
            legacy_scope.node_id(),
        )?;
        let page =
            fixture
                .catalog
                .assignment_affinity_reservations(different_generation, None, 4)?;
        assert_eq!(page.receipts().len(), 1);
        assert_eq!(page.receipts()[0].assignment_id(), admission.assignment_id);
        assert_eq!(page.receipts()[0].node_scope(), Some(related_scope));
        assert_ne!(page.receipts()[0].node_scope(), Some(different_generation));
        let retirement = crate::RetireLegacyNodeTeamBindingCommand {
            workspace_id: fixture.workspace,
            goal_id: legacy_scope.goal_id(),
            dag_revision: legacy_scope.dag_revision(),
            node_id: legacy_scope.node_id(),
            expected_revision: 5,
            request_id: Uuid::from_u128(0xA0F2),
            actor: OperationalActor::LocalOperator,
            expected_sender_leadership_revision: None,
            observed_at: TimestampMillis::new(100)?,
        };
        assert!(matches!(
            fixture.catalog.retire_legacy_node_team_binding(retirement),
            Err(OperationsError::Conflict(
                "legacy binding retirement has unresolved assignment reservation"
            ))
        ));
        drop(fixture.catalog);
        fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "One regression follows the complete reservation, terminal release, and binding ABA sequence."
    )]
    fn exact_node_reservation_blocks_rebind_until_terminal_and_tombstone_keeps_revision()
    -> Result<(), Box<dyn Error>> {
        let mut fixture = fixture()?;
        let admission = command(&fixture, 0xA020, Some(fixture.team))?;
        let assignment_id = admission.assignment_id;
        let scope = admission.node_scope;
        let pending = fixture.catalog.record_assignment_affinity(&admission)?;
        assert_eq!(
            pending.disposition(),
            AssignmentAffinityDisposition::Pending
        );
        assert!(matches!(
            fixture
                .catalog
                .set_node_team_binding(binding_command(scope, Some(1), None, 101)?),
            Err(OperationsError::Conflict(
                "node binding has a pending or active assignment"
            ))
        ));

        let offered =
            registered_assignment(&fixture, assignment_id, AgentAssignmentState::Offered)?;
        let active = fixture
            .catalog
            .reconcile_assignment_affinity(&offered, scope)?;
        assert_eq!(active.disposition(), AssignmentAffinityDisposition::Active);
        assert!(matches!(
            fixture
                .catalog
                .set_node_team_binding(binding_command(scope, Some(1), None, 102)?),
            Err(OperationsError::Conflict(
                "node binding has a pending or active assignment"
            ))
        ));

        let cancelled =
            registered_assignment(&fixture, assignment_id, AgentAssignmentState::Cancelled)?;
        let terminal = fixture
            .catalog
            .reconcile_assignment_affinity(&cancelled, scope)?;
        assert_eq!(
            terminal.disposition(),
            AssignmentAffinityDisposition::Terminal
        );
        let stale_active = fixture
            .catalog
            .reconcile_assignment_affinity(&offered, scope)?;
        assert_eq!(
            stale_active.disposition(),
            AssignmentAffinityDisposition::Terminal
        );

        let tombstone =
            fixture
                .catalog
                .set_node_team_binding(binding_command(scope, Some(1), None, 103)?)?;
        assert_eq!(tombstone.team_id(), None);
        assert_eq!(tombstone.revision(), 2);
        assert!(matches!(
            fixture.catalog.set_node_team_binding(binding_command(
                scope,
                Some(1),
                Some(fixture.team),
                104
            )?),
            Err(OperationsError::Conflict("node binding revision"))
        ));
        assert_eq!(
            fixture
                .catalog
                .node_team_binding(scope)?
                .map(|binding| binding.revision()),
            Some(2)
        );
        let other_base_scope = NodeTeamBindingScope::new(
            scope.workspace_id(),
            scope.activity_id(),
            scope.goal_id(),
            scope.goal_created_at(),
            scope.dag_revision(),
            GoalRevision::new(2)?,
            scope.node_id(),
        )?;
        assert!(
            fixture
                .catalog
                .node_team_binding(other_base_scope)?
                .is_none()
        );
        let mut rebound_retry = command(&fixture, 0xA020, None)?;
        rebound_retry.node_scope = scope;
        rebound_retry.expected_binding_revision = Some(2);
        assert!(matches!(
            fixture.catalog.record_assignment_affinity(&rebound_retry),
            Err(OperationsError::Conflict("assignment affinity replay"))
        ));
        let mut next_assignment = command(&fixture, 0xA021, None)?;
        next_assignment.node_scope = scope;
        next_assignment.expected_binding_revision = Some(2);
        assert_eq!(
            fixture
                .catalog
                .record_assignment_affinity(&next_assignment)?
                .disposition(),
            AssignmentAffinityDisposition::Pending
        );
        drop(fixture.catalog);
        fs::remove_dir_all(fixture.root)?;
        Ok(())
    }
}
