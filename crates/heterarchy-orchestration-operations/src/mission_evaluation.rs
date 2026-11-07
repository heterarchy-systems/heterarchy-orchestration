//! Durable organization-owned mission planning facts.
//!
//! This module records Mission identity, stable personal obligations, and
//! immutable evaluation-plan versions. It does not decide acceptance, award
//! `WorkCredit`, or own Forge source evidence. A Team head is created with its
//! initial Mission here; no close/cutover operation changes that head without
//! the custody contract owned by a later slice.

use forge_domain::WorkspaceId;
use uuid::Uuid;

use crate::{OperationalActor, OperationsCatalog, OperationsError, WorkspaceParticipationId};

mod checkpoint;
mod schema;
mod store;
#[cfg(test)]
mod tests;

pub use checkpoint::{
    MissionT2AccountableOwner, MissionT2CheckpointId, MissionT2DeferCommand, MissionT2DeferContent,
    MissionT2DeferOutcome, MissionT2DeferredCheckpoint,
};
pub(crate) use checkpoint::{
    create_schema as create_checkpoint_schema, schema_objects as checkpoint_schema_objects,
};
pub(crate) use schema::{create_schema, schema_objects};
pub(crate) use store::{
    ActorIdentity as MissionActorIdentity, ActorSnapshot as MissionActorSnapshot,
    frozen_plan_in_transaction,
};

/// Validated historical planning facts read within the caller's transaction.
/// This observation is not mutation authority or a source-evidence certificate.
pub(crate) struct FrozenMissionEvaluationPlan {
    pub(crate) version: MissionEvaluationPlanVersion,
    pub(crate) start: MissionEvaluationPlanStartRecord,
    pub(crate) obligations: Vec<MissionObligationRecord>,
}

const MAX_PURPOSE_BYTES: usize = 2_048;
const MAX_ACCEPTANCE_CRITERIA_BYTES: usize = 8_192;
const MAX_PLAN_COHORT: usize = 256;
const MAX_SQLITE_INTEGER: u64 = i64::MAX.unsigned_abs();

macro_rules! checked_uuid_id {
    ($name:ident, $label:literal, $documentation:literal) => {
        #[must_use]
        #[doc = $documentation]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(Uuid);

        impl $name {
            /// Construct a non-nil identifier for this Mission evaluation record.
            ///
            /// # Errors
            /// Rejects the nil UUID.
            pub fn from_uuid(value: Uuid) -> Result<Self, OperationsError> {
                if value.is_nil() {
                    return Err(OperationsError::Conflict($label));
                }
                Ok(Self(value))
            }

            #[must_use]
            pub const fn into_uuid(self) -> Uuid {
                self.0
            }
        }
    };
}

checked_uuid_id!(
    MissionId,
    "mission identity",
    "Stable identity for one durable Mission."
);

checked_uuid_id!(
    MissionObligationId,
    "mission obligation identity",
    "Stable identity for one personal obligation."
);

checked_uuid_id!(
    MissionWorkUnitId,
    "mission work-unit identity",
    "Opaque identity of the original work unit assigned to an obligation."
);

checked_uuid_id!(
    MissionEvaluationPlanId,
    "mission evaluation plan identity",
    "Stable identity for one Mission evaluation plan and its version history."
);

checked_uuid_id!(
    MissionEvaluationPlanVersionId,
    "mission evaluation plan version identity",
    "Immutable identity for one published evaluation-plan version."
);

/// Positive, exact integer weight stored by SQLite as a signed 64-bit integer.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PositiveWeight(u64);

impl PositiveWeight {
    /// Construct a positive weight exactly representable by the SQLite schema.
    ///
    /// # Errors
    /// Rejects zero and values above `i64::MAX`.
    pub fn new(value: u64) -> Result<Self, OperationsError> {
        if value == 0 || i64::try_from(value).is_err() {
            return Err(OperationsError::Bounds("mission obligation weight"));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Mutation authority supplied by a trusted native boundary.
///
/// Participant actors must present the exact current Workspace leadership
/// revision. `LocalOperator` is reserved for a trusted native caller; external
/// adapters must authenticate their caller before constructing this value.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MissionEvaluationAuthority {
    actor: OperationalActor,
    expected_workspace_leadership_revision: Option<i64>,
}

impl MissionEvaluationAuthority {
    /// Construct an authority envelope whose actor and leadership witness agree.
    ///
    /// # Errors
    /// Rejects nil participant binding identities, absent participant fences,
    /// nonpositive revisions, and operator revisions.
    pub fn new(
        actor: OperationalActor,
        expected_workspace_leadership_revision: Option<i64>,
    ) -> Result<Self, OperationsError> {
        match (actor, expected_workspace_leadership_revision) {
            (OperationalActor::LocalOperator, None) => {}
            (
                OperationalActor::Participant {
                    activity_id,
                    agent_id,
                    session_id,
                },
                Some(revision),
            ) if !activity_id.into_uuid().is_nil()
                && !agent_id.into_uuid().is_nil()
                && !session_id.into_uuid().is_nil()
                && revision >= 1 => {}
            (OperationalActor::LocalOperator, Some(_)) => {
                return Err(OperationsError::Conflict(
                    "mission evaluation operator leadership revision",
                ));
            }
            (OperationalActor::Participant { .. }, _) => {
                return Err(OperationsError::Conflict(
                    "mission evaluation requires Workspace leadership revision",
                ));
            }
        }
        Ok(Self {
            actor,
            expected_workspace_leadership_revision,
        })
    }
}

/// Checked command to create the single active Mission for an exact Team.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionCreateCommand {
    request_id: Uuid,
    mission_id: MissionId,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    purpose: String,
    created_at_ms: i64,
    authority: MissionEvaluationAuthority,
}

impl MissionCreateCommand {
    /// Construct a new Mission creation request.
    ///
    /// # Errors
    /// Rejects nil or unscoped identities, invalid purpose text, and negative time.
    pub fn new(
        request_id: Uuid,
        mission_id: MissionId,
        workspace_id: WorkspaceId,
        team_id: Uuid,
        purpose: String,
        created_at_ms: i64,
        authority: MissionEvaluationAuthority,
    ) -> Result<Self, OperationsError> {
        validate_scope(workspace_id, request_id, team_id)?;
        validate_text(&purpose, MAX_PURPOSE_BYTES, "mission purpose", true)?;
        validate_time(created_at_ms, "mission creation time")?;
        Ok(Self {
            request_id,
            mission_id,
            workspace_id,
            team_id,
            purpose,
            created_at_ms,
            authority,
        })
    }
}

/// Immutable readback of one Mission identity.
///
/// This historical fact does not establish that the Mission is the Team's
/// current Mission; mutation commands check the separate current-head record.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionRecord {
    mission_id: MissionId,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    purpose: String,
    created_at_ms: i64,
    revision: u64,
}

impl MissionRecord {
    pub const fn mission_id(&self) -> MissionId {
        self.mission_id
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
    pub fn purpose(&self) -> &str {
        &self.purpose
    }
    #[must_use]
    pub const fn created_at_ms(&self) -> i64 {
        self.created_at_ms
    }
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
}

/// Result of a Mission creation request, including exact replay information.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionCreateOutcome {
    mission: MissionRecord,
    replayed: bool,
}

impl MissionCreateOutcome {
    pub fn mission(&self) -> &MissionRecord {
        &self.mission
    }
    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Checked command to record one stable personal obligation.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionObligationRecordCommand {
    request_id: Uuid,
    obligation_id: MissionObligationId,
    workspace_id: WorkspaceId,
    mission_id: MissionId,
    participation_id: WorkspaceParticipationId,
    work_unit_id: MissionWorkUnitId,
    weight: PositiveWeight,
    acceptance_criteria: String,
    due_at_ms: i64,
    recorded_at_ms: i64,
    authority: MissionEvaluationAuthority,
}

impl MissionObligationRecordCommand {
    /// Construct a checked obligation admission request.
    ///
    /// # Errors
    /// Rejects nil or unscoped identities, invalid criteria, and negative times.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        request_id: Uuid,
        obligation_id: MissionObligationId,
        workspace_id: WorkspaceId,
        mission_id: MissionId,
        participation_id: WorkspaceParticipationId,
        work_unit_id: MissionWorkUnitId,
        weight: PositiveWeight,
        acceptance_criteria: String,
        due_at_ms: i64,
        recorded_at_ms: i64,
        authority: MissionEvaluationAuthority,
    ) -> Result<Self, OperationsError> {
        validate_scope(workspace_id, request_id, participation_id.into_uuid())?;
        validate_text(
            &acceptance_criteria,
            MAX_ACCEPTANCE_CRITERIA_BYTES,
            "mission obligation acceptance criteria",
            true,
        )?;
        validate_time(due_at_ms, "mission obligation due time")?;
        validate_time(recorded_at_ms, "mission obligation record time")?;
        Ok(Self {
            request_id,
            obligation_id,
            workspace_id,
            mission_id,
            participation_id,
            work_unit_id,
            weight,
            acceptance_criteria,
            due_at_ms,
            recorded_at_ms,
            authority,
        })
    }
}

/// Immutable personal responsibility declared by the organization.
///
/// This record is not Forge source evidence, acceptance, or `WorkCredit`.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionObligationRecord {
    obligation_id: MissionObligationId,
    workspace_id: WorkspaceId,
    mission_id: MissionId,
    participation_id: WorkspaceParticipationId,
    work_unit_id: MissionWorkUnitId,
    weight: PositiveWeight,
    acceptance_criteria: String,
    due_at_ms: i64,
    recorded_at_ms: i64,
    revision: u64,
}

impl MissionObligationRecord {
    pub const fn obligation_id(&self) -> MissionObligationId {
        self.obligation_id
    }
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }
    pub const fn mission_id(&self) -> MissionId {
        self.mission_id
    }
    #[must_use]
    pub const fn participation_id(&self) -> WorkspaceParticipationId {
        self.participation_id
    }
    pub const fn work_unit_id(&self) -> MissionWorkUnitId {
        self.work_unit_id
    }
    pub const fn weight(&self) -> PositiveWeight {
        self.weight
    }
    #[must_use]
    pub fn acceptance_criteria(&self) -> &str {
        &self.acceptance_criteria
    }
    #[must_use]
    pub const fn due_at_ms(&self) -> i64 {
        self.due_at_ms
    }
    #[must_use]
    pub const fn recorded_at_ms(&self) -> i64 {
        self.recorded_at_ms
    }
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
}

/// Result of recording a personal obligation.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionObligationRecordOutcome {
    obligation: MissionObligationRecord,
    replayed: bool,
}

impl MissionObligationRecordOutcome {
    pub fn obligation(&self) -> &MissionObligationRecord {
        &self.obligation
    }
    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// One explicit obligation selector in an evaluation-plan publication request.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MissionEvaluationPlanObligationInput {
    obligation_id: MissionObligationId,
}

impl MissionEvaluationPlanObligationInput {
    pub const fn new(obligation_id: MissionObligationId) -> Self {
        Self { obligation_id }
    }

    pub const fn obligation_id(self) -> MissionObligationId {
        self.obligation_id
    }
}

/// Checked command to publish a new immutable evaluation-plan version.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvaluationPlanPublishCommand {
    request_id: Uuid,
    plan_id: MissionEvaluationPlanId,
    version_id: MissionEvaluationPlanVersionId,
    workspace_id: WorkspaceId,
    mission_id: MissionId,
    expected_head_revision: u64,
    t0_commit_at_ms: i64,
    t1_submission_cutoff_ms: i64,
    t2_checkpoint_ms: i64,
    cohort: Vec<MissionEvaluationPlanObligationInput>,
    authority: MissionEvaluationAuthority,
}

impl MissionEvaluationPlanPublishCommand {
    /// Construct a plan version whose cohort is unique and in deterministic ID order.
    ///
    /// `t0_commit_at_ms` is an explicit trusted application/worker observation;
    /// this persistence layer never reads a clock or accepts HTTP time itself.
    /// Its relation to T1 is checked for a fresh publication after request replay
    /// lookup, so an expired retry can still read back its original commit.
    ///
    /// # Errors
    /// Rejects nil scope/request identities, invalid revisions/timelines, and
    /// oversized or duplicate cohorts.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        request_id: Uuid,
        plan_id: MissionEvaluationPlanId,
        version_id: MissionEvaluationPlanVersionId,
        workspace_id: WorkspaceId,
        mission_id: MissionId,
        expected_head_revision: u64,
        t0_commit_at_ms: i64,
        t1_submission_cutoff_ms: i64,
        t2_checkpoint_ms: i64,
        mut cohort: Vec<MissionEvaluationPlanObligationInput>,
        authority: MissionEvaluationAuthority,
    ) -> Result<Self, OperationsError> {
        validate_scope(workspace_id, request_id, plan_id.into_uuid())?;
        if expected_head_revision >= MAX_SQLITE_INTEGER {
            return Err(OperationsError::Bounds("mission evaluation plan revision"));
        }
        validate_time(t0_commit_at_ms, "evaluation plan T0")?;
        validate_time(t1_submission_cutoff_ms, "evaluation plan T1")?;
        validate_time(t2_checkpoint_ms, "evaluation plan T2")?;
        if t1_submission_cutoff_ms > t2_checkpoint_ms {
            return Err(OperationsError::Conflict(
                "mission evaluation plan timeline",
            ));
        }
        if cohort.len() > MAX_PLAN_COHORT {
            return Err(OperationsError::Bounds("mission evaluation plan cohort"));
        }
        cohort.sort_by(|left, right| {
            left.obligation_id
                .into_uuid()
                .as_bytes()
                .cmp(right.obligation_id.into_uuid().as_bytes())
        });
        if cohort
            .windows(2)
            .any(|pair| pair[0].obligation_id == pair[1].obligation_id)
        {
            return Err(OperationsError::Conflict(
                "mission evaluation plan duplicate obligation",
            ));
        }
        Ok(Self {
            request_id,
            plan_id,
            version_id,
            workspace_id,
            mission_id,
            expected_head_revision,
            t0_commit_at_ms,
            t1_submission_cutoff_ms,
            t2_checkpoint_ms,
            cohort,
            authority,
        })
    }
}

/// Frozen obligation/weight pair captured by a published plan version.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MissionEvaluationPlanObligation {
    obligation_id: MissionObligationId,
    weight: PositiveWeight,
}

impl MissionEvaluationPlanObligation {
    pub const fn obligation_id(self) -> MissionObligationId {
        self.obligation_id
    }
    pub const fn weight(self) -> PositiveWeight {
        self.weight
    }
}

/// Immutable published revision of one Mission evaluation plan.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvaluationPlanVersion {
    plan_id: MissionEvaluationPlanId,
    version_id: MissionEvaluationPlanVersionId,
    workspace_id: WorkspaceId,
    mission_id: MissionId,
    revision: u64,
    t0_commit_at_ms: i64,
    t1_submission_cutoff_ms: i64,
    t2_checkpoint_ms: i64,
    cohort: Vec<MissionEvaluationPlanObligation>,
}

impl MissionEvaluationPlanVersion {
    pub const fn plan_id(&self) -> MissionEvaluationPlanId {
        self.plan_id
    }
    pub const fn version_id(&self) -> MissionEvaluationPlanVersionId {
        self.version_id
    }
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }
    pub const fn mission_id(&self) -> MissionId {
        self.mission_id
    }
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
    #[must_use]
    pub const fn t0_commit_at_ms(&self) -> i64 {
        self.t0_commit_at_ms
    }
    #[must_use]
    pub const fn t1_submission_cutoff_ms(&self) -> i64 {
        self.t1_submission_cutoff_ms
    }
    #[must_use]
    pub const fn t2_checkpoint_ms(&self) -> i64 {
        self.t2_checkpoint_ms
    }
    pub fn cohort(&self) -> &[MissionEvaluationPlanObligation] {
        &self.cohort
    }
}

/// Result of publishing one immutable plan version.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvaluationPlanPublishOutcome {
    version: MissionEvaluationPlanVersion,
    replayed: bool,
}

impl MissionEvaluationPlanPublishOutcome {
    pub fn version(&self) -> &MissionEvaluationPlanVersion {
        &self.version
    }
    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Checked command to explicitly start one exact published plan revision.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvaluationPlanStartCommand {
    request_id: Uuid,
    workspace_id: WorkspaceId,
    mission_id: MissionId,
    plan_id: MissionEvaluationPlanId,
    expected_revision: u64,
    authority: MissionEvaluationAuthority,
}

impl MissionEvaluationPlanStartCommand {
    /// Construct an explicit, exact-revision plan start request.
    ///
    /// # Errors
    /// Rejects nil identities or a nonpositive/unrepresentable revision.
    pub fn new(
        request_id: Uuid,
        workspace_id: WorkspaceId,
        mission_id: MissionId,
        plan_id: MissionEvaluationPlanId,
        expected_revision: u64,
        authority: MissionEvaluationAuthority,
    ) -> Result<Self, OperationsError> {
        validate_scope(workspace_id, request_id, plan_id.into_uuid())?;
        if expected_revision == 0 || expected_revision > MAX_SQLITE_INTEGER {
            return Err(OperationsError::Conflict(
                "mission evaluation plan start revision",
            ));
        }
        Ok(Self {
            request_id,
            workspace_id,
            mission_id,
            plan_id,
            expected_revision,
            authority,
        })
    }
}

/// Immutable start receipt for one exact plan revision.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvaluationPlanStartRecord {
    request_id: Uuid,
    workspace_id: WorkspaceId,
    mission_id: MissionId,
    plan_id: MissionEvaluationPlanId,
    revision: u64,
    started_at_ms: i64,
}

impl MissionEvaluationPlanStartRecord {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
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
    pub const fn started_at_ms(&self) -> i64 {
        self.started_at_ms
    }
}

/// Result of starting one exact evaluation-plan revision.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvaluationPlanStartOutcome {
    start: MissionEvaluationPlanStartRecord,
    replayed: bool,
}

impl MissionEvaluationPlanStartOutcome {
    pub fn start(&self) -> &MissionEvaluationPlanStartRecord {
        &self.start
    }
    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Closed set of durable mutation outcomes addressable by their request ID.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MissionEvaluationRecordedMutation {
    MissionCreated(MissionRecord),
    ObligationRecorded(MissionObligationRecord),
    PlanPublished(MissionEvaluationPlanVersion),
    PlanStarted(MissionEvaluationPlanStartRecord),
}

/// Read-only exact receipt for one stored Mission evaluation mutation.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionEvaluationRequestRecord {
    request_id: Uuid,
    workspace_id: WorkspaceId,
    mutation: MissionEvaluationRecordedMutation,
}

impl MissionEvaluationRequestRecord {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }
    pub fn mutation(&self) -> &MissionEvaluationRecordedMutation {
        &self.mutation
    }
}

impl OperationsCatalog {
    /// Create one active Mission for an exact active Team.
    ///
    /// # Errors
    /// Rejects unverified Workspace scope, unauthorized/stale authority,
    /// conflicting Mission or request identities, and corrupt/storage failures.
    pub fn create_mission(
        &mut self,
        command: &MissionCreateCommand,
    ) -> Result<MissionCreateOutcome, OperationsError> {
        store::create_mission(&mut self.connection, self.destination, command)
    }

    /// Read one immutable Mission by exact Workspace scope.
    ///
    /// A successful read is historical readback, not current-Mission authority.
    ///
    /// # Errors
    /// Rejects invalid or unverified Workspace scope, cross-Workspace IDs, and corrupt storage.
    pub fn mission(
        &self,
        workspace_id: WorkspaceId,
        mission_id: MissionId,
    ) -> Result<Option<MissionRecord>, OperationsError> {
        store::read_mission(&self.connection, self.destination, workspace_id, mission_id)
    }

    /// Record a stable personal responsibility for a currently joined Team participant.
    ///
    /// This operation does not accept Forge evidence or award `WorkCredit`.
    ///
    /// # Errors
    /// Rejects invalid scope, unauthorized/stale authority, non-joined participants,
    /// identity conflicts, and corrupt/storage failures.
    pub fn record_mission_obligation(
        &mut self,
        command: &MissionObligationRecordCommand,
    ) -> Result<MissionObligationRecordOutcome, OperationsError> {
        store::record_obligation(&mut self.connection, self.destination, command)
    }

    /// Read one immutable obligation by exact Workspace scope.
    ///
    /// # Errors
    /// Rejects invalid or unverified Workspace scope, cross-Workspace IDs, and corrupt storage.
    pub fn mission_obligation(
        &self,
        workspace_id: WorkspaceId,
        obligation_id: MissionObligationId,
    ) -> Result<Option<MissionObligationRecord>, OperationsError> {
        store::read_obligation(
            &self.connection,
            self.destination,
            workspace_id,
            obligation_id,
        )
    }

    /// Publish a new immutable version of one Mission evaluation plan.
    ///
    /// # Errors
    /// Rejects invalid scope, unauthorized/stale authority, invalid Mission/cohort,
    /// stale heads, started plans, request conflicts, and corrupt/storage failures.
    pub fn publish_mission_evaluation_plan(
        &mut self,
        command: &MissionEvaluationPlanPublishCommand,
    ) -> Result<MissionEvaluationPlanPublishOutcome, OperationsError> {
        store::publish_plan(&mut self.connection, self.destination, command)
    }

    /// Read one immutable plan version and its bounded obligation/weight snapshot.
    ///
    /// # Errors
    /// Rejects invalid or unverified Workspace scope, cross-Workspace IDs, and corrupt storage.
    pub fn mission_evaluation_plan_version(
        &self,
        workspace_id: WorkspaceId,
        plan_id: MissionEvaluationPlanId,
        revision: u64,
    ) -> Result<Option<MissionEvaluationPlanVersion>, OperationsError> {
        store::read_plan_version(
            &self.connection,
            self.destination,
            workspace_id,
            plan_id,
            revision,
        )
    }

    /// Explicitly start one exact current evaluation-plan revision.
    ///
    /// Starting freezes the plan head; it does not finalize scores or confer roles.
    ///
    /// # Errors
    /// Rejects invalid scope, unauthorized/stale authority, missing/stale revision,
    /// starts outside the published T0/T1 window, trusted-clock failures,
    /// conflicting starts, and corrupt/storage failures.
    pub fn start_mission_evaluation_plan(
        &mut self,
        command: &MissionEvaluationPlanStartCommand,
        trusted_now_ms: impl FnOnce() -> Result<i64, OperationsError>,
    ) -> Result<MissionEvaluationPlanStartOutcome, OperationsError> {
        store::start_plan(
            &mut self.connection,
            self.destination,
            command,
            trusted_now_ms,
        )
    }

    /// Read the immutable start receipt for a plan, if it has started.
    ///
    /// # Errors
    /// Rejects invalid or unverified Workspace scope, cross-Workspace IDs, and corrupt storage.
    pub fn mission_evaluation_plan_start(
        &self,
        workspace_id: WorkspaceId,
        plan_id: MissionEvaluationPlanId,
    ) -> Result<Option<MissionEvaluationPlanStartRecord>, OperationsError> {
        store::read_plan_start(&self.connection, self.destination, workspace_id, plan_id)
    }

    /// Read the exact durable outcome for one mutation request without retrying it.
    ///
    /// This read is scoped to a verified Workspace and never creates or mutates
    /// records; callers use it to reconcile an unknown write outcome before retry.
    /// It is a trusted persistence query: an outer Runtime/native caller must
    /// authorize access to the Workspace because request IDs are not credentials.
    ///
    /// # Errors
    /// Rejects nil/unverified Workspace scope, cross-Workspace request IDs, and
    /// corrupt or incomplete request referents.
    pub fn mission_evaluation_request(
        &self,
        workspace_id: WorkspaceId,
        request_id: Uuid,
    ) -> Result<Option<MissionEvaluationRequestRecord>, OperationsError> {
        store::read_request_record(&self.connection, self.destination, workspace_id, request_id)
    }

    /// Read a request receipt only for the same currently Ready semantic actor.
    ///
    /// Leadership is not rechecked: a former leader may reconcile its own
    /// durable outcome after a role change. The actor must still resolve as the
    /// same Workspace participant, and a request ID is not authorization.
    ///
    /// # Errors
    /// Rejects invalid/unverified Workspace scope, a non-Ready actor, and
    /// cross-actor or cross-Workspace request IDs.
    pub fn mission_evaluation_request_for_actor(
        &self,
        workspace_id: WorkspaceId,
        request_id: Uuid,
        actor: OperationalActor,
    ) -> Result<Option<MissionEvaluationRequestRecord>, OperationsError> {
        store::read_request_record_for_actor(
            &self.connection,
            self.destination,
            workspace_id,
            request_id,
            actor,
        )
    }
}

fn validate_scope(
    workspace_id: WorkspaceId,
    request_id: Uuid,
    target_id: Uuid,
) -> Result<(), OperationsError> {
    if workspace_id.into_uuid().is_nil() || request_id.is_nil() || target_id.is_nil() {
        return Err(OperationsError::Conflict(
            "mission evaluation scoped identity",
        ));
    }
    Ok(())
}

fn validate_text(
    value: &str,
    max_bytes: usize,
    label: &'static str,
    require_nonempty: bool,
) -> Result<(), OperationsError> {
    let trimmed = value.trim();
    if (require_nonempty && trimmed.is_empty())
        || value.len() > max_bytes
        || value.as_bytes().contains(&0)
    {
        return Err(OperationsError::Bounds(label));
    }
    Ok(())
}

fn validate_time(value: i64, label: &'static str) -> Result<(), OperationsError> {
    if value < 0 {
        return Err(OperationsError::Bounds(label));
    }
    Ok(())
}
