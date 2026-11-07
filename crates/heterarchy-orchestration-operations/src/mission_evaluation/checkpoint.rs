//! Narrow source-persistence seam for a deferred T2 checkpoint.
//!
//! A defer record is an administrative follow-up fact. It does not adjudicate
//! evidence, finalize a score, choose a Leader, or advance a Mission.

use forge_domain::{ActivityId, AgentInstanceId, WorkspaceId};
use uuid::Uuid;

use crate::{
    MissionEvaluationAuthority, MissionEvaluationPlanId, MissionId, OperationalActor,
    OperationsCatalog, OperationsError, WorkspaceParticipationId,
};

mod schema;
mod store;
#[cfg(test)]
mod tests;

pub(crate) use schema::{create_schema, schema_objects};

const MAX_REASON_BYTES: usize = 4_096;
const MAX_NEXT_CHECK_BYTES: usize = 2_048;

/// Stable identity for one append-only T2 checkpoint entry.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MissionT2CheckpointId(Uuid);

impl MissionT2CheckpointId {
    /// Construct a non-nil checkpoint identity.
    ///
    /// # Errors
    /// Rejects the nil UUID.
    pub fn from_uuid(value: Uuid) -> Result<Self, OperationsError> {
        if value.is_nil() {
            return Err(OperationsError::Conflict("mission T2 checkpoint identity"));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn into_uuid(self) -> Uuid {
        self.0
    }
}

/// Immutable caller-owned content for one T2 defer request.
///
/// This contains no actor attribution or server time. Request-ID recovery uses
/// it with a separately authenticated reader; Operations returns the original
/// actor source from the stored record.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionT2DeferContent {
    request_id: Uuid,
    checkpoint_id: MissionT2CheckpointId,
    workspace_id: WorkspaceId,
    mission_id: MissionId,
    plan_id: MissionEvaluationPlanId,
    revision: u64,
    expected_previous_checkpoint_id: Option<MissionT2CheckpointId>,
    reason: String,
    next_check_condition: String,
    next_check_at_ms: i64,
}

impl MissionT2DeferContent {
    /// Construct a bounded, exact T2 defer content value.
    ///
    /// # Errors
    /// Rejects nil or inconsistent IDs, invalid plan revisions/times, blank
    /// text, and oversized reason/check condition.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        request_id: Uuid,
        checkpoint_id: MissionT2CheckpointId,
        workspace_id: WorkspaceId,
        mission_id: MissionId,
        plan_id: MissionEvaluationPlanId,
        revision: u64,
        expected_previous_checkpoint_id: Option<MissionT2CheckpointId>,
        reason: String,
        next_check_condition: String,
        next_check_at_ms: i64,
    ) -> Result<Self, OperationsError> {
        if request_id.is_nil()
            || workspace_id.into_uuid().is_nil()
            || mission_id.into_uuid().is_nil()
            || plan_id.into_uuid().is_nil()
            || expected_previous_checkpoint_id.is_some_and(|id| id.into_uuid().is_nil())
        {
            return Err(OperationsError::Conflict("mission T2 defer scope"));
        }
        if revision == 0 || revision > super::MAX_SQLITE_INTEGER {
            return Err(OperationsError::Bounds("mission T2 plan revision"));
        }
        validate_text(&reason, MAX_REASON_BYTES, "mission T2 defer reason")?;
        validate_text(
            &next_check_condition,
            MAX_NEXT_CHECK_BYTES,
            "mission T2 next check condition",
        )?;
        if next_check_at_ms < 0 {
            return Err(OperationsError::Conflict("mission T2 next check time"));
        }
        Ok(Self {
            request_id,
            checkpoint_id,
            workspace_id,
            mission_id,
            plan_id,
            revision,
            expected_previous_checkpoint_id,
            reason,
            next_check_condition,
            next_check_at_ms,
        })
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }
    pub const fn checkpoint_id(&self) -> MissionT2CheckpointId {
        self.checkpoint_id
    }
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }
    pub const fn mission_id(&self) -> MissionId {
        self.mission_id
    }
    pub const fn plan_id(&self) -> MissionEvaluationPlanId {
        self.plan_id
    }
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
    #[must_use]
    pub const fn expected_previous_checkpoint_id(&self) -> Option<MissionT2CheckpointId> {
        self.expected_previous_checkpoint_id
    }
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }
    #[must_use]
    pub fn next_check_condition(&self) -> &str {
        &self.next_check_condition
    }
    #[must_use]
    pub const fn next_check_at_ms(&self) -> i64 {
        self.next_check_at_ms
    }
}

/// Checked source mutation context binding immutable defer content to the
/// currently authenticated Workspace actor.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionT2DeferCommand {
    content: MissionT2DeferContent,
    authority: MissionEvaluationAuthority,
}

impl MissionT2DeferCommand {
    /// Construct a defer mutation from exact immutable content and current
    /// Workspace authority.
    ///
    /// # Errors
    /// Rejects native operator authority; only a participant can act as the
    /// Workspace leader for this T2 operation.
    pub fn new(
        content: MissionT2DeferContent,
        authority: MissionEvaluationAuthority,
    ) -> Result<Self, OperationsError> {
        if matches!(authority.actor, crate::OperationalActor::LocalOperator) {
            return Err(OperationsError::Conflict(
                "mission T2 defer requires current Workspace leader",
            ));
        }
        Ok(Self { content, authority })
    }

    pub fn content(&self) -> &MissionT2DeferContent {
        &self.content
    }
}

/// Immutable attribution for the Workspace leader who recorded a T2 defer.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionT2AccountableOwner {
    activity_id: ActivityId,
    agent_id: AgentInstanceId,
    session_id: forge_domain::AgentSessionId,
    actor_kind: i64,
    actor_key: String,
    participation_id: Option<WorkspaceParticipationId>,
    profile_id: Option<Uuid>,
    participant_id: Uuid,
    display_name: String,
    leadership_revision: i64,
}

impl MissionT2AccountableOwner {
    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity_id
    }
    #[must_use]
    pub const fn agent_id(&self) -> AgentInstanceId {
        self.agent_id
    }
    pub const fn session_id(&self) -> forge_domain::AgentSessionId {
        self.session_id
    }
    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
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
    pub fn display_name(&self) -> &str {
        &self.display_name
    }
    #[must_use]
    pub const fn leadership_revision(&self) -> i64 {
        self.leadership_revision
    }

    pub(super) fn validate(&self) -> Result<(), OperationsError> {
        let actor_fields_are_valid = match self.actor_kind {
            1 => self.participation_id.is_some() && self.profile_id.is_some(),
            2 => self.participation_id.is_none() && self.profile_id.is_none(),
            _ => false,
        };
        let expected_key = match self.actor_kind {
            1 => self.participation_id.map(|id| format!("participant:{id}")),
            2 => Some(format!("native-participant:{}", self.participant_id)),
            _ => None,
        };
        if self.activity_id.into_uuid().is_nil()
            || self.agent_id.into_uuid().is_nil()
            || self.session_id.into_uuid().is_nil()
            || self.participant_id.is_nil()
            || self
                .participation_id
                .is_some_and(|id| id.into_uuid().is_nil())
            || self
                .profile_id
                .is_some_and(|profile_id| profile_id.is_nil())
            || self.display_name.trim().is_empty()
            || self.display_name.len() > 128
            || self.display_name.as_bytes().contains(&0)
            || self.leadership_revision < 1
            || !actor_fields_are_valid
            || expected_key.as_deref() != Some(self.actor_key.as_str())
            || self.actor_key.is_empty()
            || self.actor_key.len() > 256
            || self.actor_key.as_bytes().contains(&0)
        {
            return Err(OperationsError::Corrupt("mission T2 accountable owner"));
        }
        Ok(())
    }

    pub(super) fn same_semantic_actor(&self, actor: &super::MissionActorSnapshot) -> bool {
        match (self.actor_kind, actor.identity.kind) {
            (1, 1) => {
                self.actor_key == actor.identity.key
                    && self.participation_id == actor.identity.participation_id
                    && self.profile_id == actor.identity.profile_id
            }
            (2, 2) => Some(self.participant_id) == actor.identity.participant_id,
            _ => false,
        }
    }

    pub(super) fn same_mutation_actor(&self, actor: &super::MissionActorSnapshot) -> bool {
        self.same_semantic_actor(actor)
            && Some(self.participant_id) == actor.identity.participant_id
    }
}

/// Immutable Orchestrator statement of one deferred T2 checkpoint.
///
/// The reason and follow-up condition are not independent review evidence or
/// a final performance judgment.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionT2DeferredCheckpoint {
    request_id: Uuid,
    checkpoint_id: MissionT2CheckpointId,
    workspace_id: WorkspaceId,
    mission_id: MissionId,
    plan_id: MissionEvaluationPlanId,
    revision: u64,
    sequence: u64,
    previous_checkpoint_id: Option<MissionT2CheckpointId>,
    reason: String,
    next_check_condition: String,
    next_check_at_ms: i64,
    recorded_at_ms: i64,
    accountable_owner: MissionT2AccountableOwner,
}

impl MissionT2DeferredCheckpoint {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }
    pub const fn checkpoint_id(&self) -> MissionT2CheckpointId {
        self.checkpoint_id
    }
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }
    pub const fn mission_id(&self) -> MissionId {
        self.mission_id
    }
    pub const fn plan_id(&self) -> MissionEvaluationPlanId {
        self.plan_id
    }
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }
    #[must_use]
    pub const fn previous_checkpoint_id(&self) -> Option<MissionT2CheckpointId> {
        self.previous_checkpoint_id
    }
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }
    #[must_use]
    pub fn next_check_condition(&self) -> &str {
        &self.next_check_condition
    }
    #[must_use]
    pub const fn next_check_at_ms(&self) -> i64 {
        self.next_check_at_ms
    }
    #[must_use]
    pub const fn recorded_at_ms(&self) -> i64 {
        self.recorded_at_ms
    }
    #[must_use]
    pub const fn accountable_owner_activity_id(&self) -> ActivityId {
        self.accountable_owner.activity_id
    }
    #[must_use]
    pub const fn accountable_owner_agent_id(&self) -> AgentInstanceId {
        self.accountable_owner.agent_id
    }
    pub fn accountable_owner(&self) -> &MissionT2AccountableOwner {
        &self.accountable_owner
    }

    /// Compare immutable caller-owned content with a stored receipt.
    ///
    /// This deliberately does not compare source actor attribution. The
    /// caller authenticates separately, and Operations returns the original
    /// source attribution from the receipt.
    #[must_use]
    pub fn matches_intent(&self, intent: &MissionT2DeferContent) -> bool {
        self.request_id == intent.request_id
            && self.checkpoint_id == intent.checkpoint_id
            && self.workspace_id == intent.workspace_id
            && self.mission_id == intent.mission_id
            && self.plan_id == intent.plan_id
            && self.revision == intent.revision
            && self.previous_checkpoint_id == intent.expected_previous_checkpoint_id
            && self.reason == intent.reason
            && self.next_check_condition == intent.next_check_condition
            && self.next_check_at_ms == intent.next_check_at_ms
    }
}

/// Result of one T2 defer write or exact request replay.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionT2DeferOutcome {
    checkpoint: MissionT2DeferredCheckpoint,
    replayed: bool,
}

impl MissionT2DeferOutcome {
    pub fn checkpoint(&self) -> &MissionT2DeferredCheckpoint {
        &self.checkpoint
    }
    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

impl OperationsCatalog {
    /// Append one defer record for an exact started Mission plan revision.
    ///
    /// A repeated defer links to the latest record explicitly supplied by the
    /// caller; IMMEDIATE serialization prevents concurrent branches. The record
    /// leaves the frozen plan, score, and Mission head untouched.
    ///
    /// # Errors
    /// Rejects stale authority, a mismatched/unstarted plan, a pre-T2 record
    /// time, a stale predecessor, replay conflicts, and corrupt storage.
    pub fn defer_mission_t2_checkpoint(
        &mut self,
        command: &MissionT2DeferCommand,
        trusted_now_ms: impl FnOnce() -> Result<i64, OperationsError>,
    ) -> Result<MissionT2DeferOutcome, OperationsError> {
        store::defer(
            &mut self.connection,
            self.destination,
            command,
            trusted_now_ms,
        )
    }

    /// Read one immutable defer record by exact Workspace and plan scope.
    ///
    /// # Errors
    /// Rejects unverified or cross-Workspace scope and corrupt storage.
    pub fn mission_t2_deferred_checkpoint(
        &self,
        workspace_id: WorkspaceId,
        plan_id: MissionEvaluationPlanId,
        revision: u64,
        checkpoint_id: MissionT2CheckpointId,
    ) -> Result<Option<MissionT2DeferredCheckpoint>, OperationsError> {
        store::read_one(
            &self.connection,
            self.destination,
            workspace_id,
            plan_id,
            revision,
            checkpoint_id,
        )
    }

    /// Read an exact defer receipt for its original semantic actor or the
    /// currently authorized Workspace leader.
    ///
    /// This reconciliation query is observation only. It returns the original
    /// Activity, Agent, Session, source time, and checkpoint content.
    ///
    /// # Errors
    /// Rejects unverified Workspace scope, unrelated participants, nonleaders,
    /// stale leadership, and corrupt storage.
    pub fn mission_t2_deferred_checkpoint_for_request(
        &self,
        workspace_id: WorkspaceId,
        request_id: Uuid,
        authority: MissionEvaluationAuthority,
    ) -> Result<Option<MissionT2DeferredCheckpoint>, OperationsError> {
        store::read_for_request(
            &self.connection,
            self.destination,
            workspace_id,
            request_id,
            authority,
        )
    }

    /// Read the latest defer in one exact plan revision, if any.
    ///
    /// # Errors
    /// Rejects unverified or cross-Workspace scope and corrupt storage.
    pub fn latest_mission_t2_deferred_checkpoint(
        &self,
        workspace_id: WorkspaceId,
        plan_id: MissionEvaluationPlanId,
        revision: u64,
    ) -> Result<Option<MissionT2DeferredCheckpoint>, OperationsError> {
        store::read_latest(
            &self.connection,
            self.destination,
            workspace_id,
            plan_id,
            revision,
        )
    }
}

impl MissionT2DeferCommand {
    fn matches_record(
        &self,
        record: &MissionT2DeferredCheckpoint,
        actor: &super::MissionActorSnapshot,
    ) -> bool {
        if matches!(self.authority.actor, OperationalActor::LocalOperator) {
            return false;
        }
        record.matches_intent(self.content()) && record.accountable_owner.same_mutation_actor(actor)
    }
}

fn validate_text(
    value: &str,
    max_bytes: usize,
    label: &'static str,
) -> Result<(), OperationsError> {
    if value.trim().is_empty() || value.len() > max_bytes || value.as_bytes().contains(&0) {
        return Err(OperationsError::Bounds(label));
    }
    Ok(())
}

fn validate_checkpoint(record: &MissionT2DeferredCheckpoint) -> Result<(), OperationsError> {
    if record.request_id.is_nil()
        || record.checkpoint_id.into_uuid().is_nil()
        || record.workspace_id.into_uuid().is_nil()
        || record.mission_id.into_uuid().is_nil()
        || record.plan_id.into_uuid().is_nil()
        || record.revision == 0
        || record.sequence > super::MAX_SQLITE_INTEGER
        || record
            .previous_checkpoint_id
            .is_some_and(|id| id.into_uuid().is_nil())
        || record.recorded_at_ms < 0
        || record.next_check_at_ms <= record.recorded_at_ms
    {
        return Err(OperationsError::Corrupt("mission T2 deferred checkpoint"));
    }
    validate_text(&record.reason, MAX_REASON_BYTES, "mission T2 defer reason")
        .map_err(|_| OperationsError::Corrupt("mission T2 defer reason"))?;
    validate_text(
        &record.next_check_condition,
        MAX_NEXT_CHECK_BYTES,
        "mission T2 next check condition",
    )
    .map_err(|_| OperationsError::Corrupt("mission T2 next check condition"))?;
    record.accountable_owner.validate()?;
    Ok(())
}
