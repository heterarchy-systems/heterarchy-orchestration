//! Workspace-wide identity capacity reserved by an approved Team expansion.
//!
//! These helpers are deliberately transaction-scoped. The decision owner must
//! call `reserve_approved_workspace_holds` in the same IMMEDIATE transaction
//! that records the approval and its Team slots. They do not select Profiles,
//! prove provisioning authority, create registrations, or call other stores.

mod schema;
#[cfg(test)]
mod tests;

pub(super) use schema::{
    PRE_HOLD_DECISION_BACKFILL_SQL, create_recovery_schema,
    create_schema as create_workspace_hold_schema, recovery_schema_objects,
    schema_objects as workspace_hold_schema_objects,
};

use std::collections::BTreeSet;

use forge_domain::{TimestampMillis, WorkspaceId};
use heterarchy_orchestration_sessions::AgentProfileId;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use uuid::Uuid;

use super::existing_home_transfer::{self, ExistingHomeTransferSource};
use crate::{
    LeadershipScope, LegacySlotRecoveryCommand, LegacySlotRecoveryIntent,
    LegacySlotRecoveryReceipt, OperationsError, TeamExpansionActor, TeamExpansionApprovalSlotPlan,
    TeamExpansionApprovalSlotPlanV3, TeamExpansionSlotRef, TeamExpansionWorkspaceTarget,
    TeamExpansionWorkspaceTargetV3, leadership::current_leadership_authority_in_transaction, teams,
    wire,
};

const MAX_WORKSPACE_LOGICAL_AGENTS: usize = 12;
type StoredPlanParts = (i64, Option<Vec<u8>>, Option<Vec<u8>>);
type RawRecoveryTuple = (
    Vec<u8>,
    i64,
    Vec<u8>,
    Vec<u8>,
    i64,
    i64,
    i64,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    String,
    i64,
);

pub(super) fn recover_legacy_slot(
    connection: &mut Connection,
    command: &LegacySlotRecoveryCommand,
    trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
) -> Result<LegacySlotRecoveryReceipt, OperationsError> {
    validate_recovery_intent(command.intent)?;
    let transaction =
        connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let actor = authorize_recovery(&transaction, command)?;
    if let Some(existing) = load_recovery_by_request(
        &transaction,
        command.intent.workspace_id,
        command.intent.request_id,
        true,
    )? {
        if recovery_matches_intent(&existing, command.intent) {
            transaction.commit()?;
            return Ok(existing);
        }
        return Err(OperationsError::Conflict(
            "legacy Team expansion recovery replay",
        ));
    }
    if recovery_request_exists(&transaction, command.intent.request_id)? {
        return Err(OperationsError::Conflict(
            "legacy Team expansion recovery Workspace",
        ));
    }
    let approval_request_id = validate_recovery_source(&transaction, command.intent)?;
    validate_recovery_current_state(&transaction, command.intent, &actor)?;
    let recorded_at = trusted_now()?;
    super::ensure_source_time_not_regressed(
        &transaction,
        command.intent.workspace_id,
        command.intent.team_id,
        recorded_at,
    )?;
    transaction.execute(
        "INSERT INTO ops_team_expansion_legacy_slot_recoveries(request_id,decision_id,ordinal,approval_request_id,workspace_id,team_id,expected_team_revision,capacity_revision,leadership_revision,target_profile_id,target_participation_id,actor_activity_id,actor_agent_id,actor_session_id,actor_participation_id,actor_profile_id,actor_participant_id,actor_display_name,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)",
        params![
            command.intent.request_id.as_bytes(),
            command.intent.slot.approval_id().as_bytes(),
            i64::from(command.intent.slot.ordinal()),
            approval_request_id.as_bytes(),
            command.intent.workspace_id.into_uuid().as_bytes(),
            command.intent.team_id.as_bytes(),
            i64::try_from(command.intent.expected_team_revision)
                .map_err(|_| OperationsError::Bounds("Team revision"))?,
            i64::try_from(command.intent.expected_capacity_revision)
                .map_err(|_| OperationsError::Bounds("capacity revision"))?,
            command.expected_leadership_revision,
            command.intent.target_profile_id.as_bytes(),
            command.intent.target_participation_id.into_uuid().as_bytes(),
            actor.activity_id.into_uuid().as_bytes(),
            actor.agent_id.into_uuid().as_bytes(),
            actor.session_id.into_uuid().as_bytes(),
            actor.participation_id.into_uuid().as_bytes(),
            actor.profile_id.as_bytes(),
            actor.participant_id.as_bytes(),
            actor.display_name,
            recorded_at.get(),
        ],
    )?;
    let fresh = load_recovery_by_request(
        &transaction,
        command.intent.workspace_id,
        command.intent.request_id,
        false,
    )?
    .ok_or(OperationsError::Corrupt(
        "legacy Team expansion recovery readback",
    ))?;
    let commit_result = transaction.commit();
    match commit_result {
        Ok(()) => Ok(fresh),
        Err(source) => match load_recovery_by_request(
            connection,
            command.intent.workspace_id,
            command.intent.request_id,
            true,
        ) {
            Ok(Some(existing)) if recovery_matches_intent(&existing, command.intent) => {
                Ok(existing)
            }
            Ok(Some(_)) => Err(OperationsError::Conflict(
                "legacy Team expansion recovery after unconfirmed commit",
            )),
            Ok(None) | Err(_) => Err(OperationsError::OperationCommitUnconfirmed {
                workspace_id: command.intent.workspace_id,
                request_id: command.intent.request_id,
                source,
            }),
        },
    }
}

fn authorize_recovery(
    transaction: &Transaction<'_>,
    command: &LegacySlotRecoveryCommand,
) -> Result<TeamExpansionActor, OperationsError> {
    teams::ensure_verified_workspace(transaction, command.intent.workspace_id)?;
    let (actor, _) =
        super::validate_actor(transaction, command.intent.workspace_id, command.actor)?;
    let authority = current_leadership_authority_in_transaction(
        transaction,
        LeadershipScope::workspace(command.intent.workspace_id),
    )?
    .ok_or(OperationsError::Conflict(
        "legacy Team expansion recovery requires current Workspace Orchestrator",
    ))?;
    if authority.participant_id() != actor.participant_id
        || authority.leadership_revision() != command.expected_leadership_revision
    {
        return Err(OperationsError::Conflict(
            "legacy Team expansion recovery authority changed",
        ));
    }
    Ok(actor)
}

pub(super) fn read_legacy_slot_recovery(
    connection: &Connection,
    intent: &LegacySlotRecoveryIntent,
    reader: crate::OperationalActor,
) -> Result<Option<LegacySlotRecoveryReceipt>, OperationsError> {
    validate_recovery_intent(*intent)?;
    let transaction = connection.unchecked_transaction()?;
    teams::ensure_verified_workspace(&transaction, intent.workspace_id)?;
    let (reader, _) = super::validate_actor(&transaction, intent.workspace_id, reader)?;
    let current = current_leadership_authority_in_transaction(
        &transaction,
        LeadershipScope::workspace(intent.workspace_id),
    )?
    .is_some_and(|authority| authority.participant_id() == reader.participant_id);
    let owner: Option<(Vec<u8>, Vec<u8>)> = transaction
        .query_row(
            "SELECT actor_participation_id,actor_profile_id FROM ops_team_expansion_legacy_slot_recoveries WHERE workspace_id=?1 AND request_id=?2",
            params![
                intent.workspace_id.into_uuid().as_bytes(),
                intent.request_id.as_bytes(),
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if owner.is_none() {
        transaction.commit()?;
        return Ok(None);
    }
    let original = owner.is_some_and(|(participation, profile)| {
        participation.as_slice() == reader.participation_id.into_uuid().as_bytes()
            && profile.as_slice() == reader.profile_id.as_bytes()
    });
    if !current && !original {
        transaction.commit()?;
        return Ok(None);
    }
    let existing =
        load_recovery_by_request(&transaction, intent.workspace_id, intent.request_id, false)?
            .ok_or(OperationsError::Corrupt(
                "legacy Team expansion recovery owner source missing",
            ))?;
    if !recovery_matches_intent(&existing, *intent) {
        return Err(OperationsError::Conflict(
            "legacy Team expansion recovery readback mismatch",
        ));
    }
    transaction.commit()?;
    Ok(Some(existing))
}

/// The exact occupancy key used by registration admission.
///
/// Profile identities are stable across Session aliases. Participant keys
/// preserve legacy members with no canonical Profile association.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum WorkspaceIdentityKey {
    Profile(Uuid),
    Participant(Uuid),
    UnboundSlot { decision_id: Uuid, ordinal: u8 },
}

/// A target class frozen by the Orchestrator in the approval transaction.
///
/// Existing registered targets bind both stable Profile and participation
/// identities. `Unregistered` already names the stable identity, while
/// `New` reserves a unique slot key until a later authorized Profile
/// assignment binds it. These values do not grant authority to provision or
/// move an Agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WorkspaceHoldTarget {
    ExistingWorkspace {
        profile_id: Uuid,
        participation_id: Uuid,
    },
    Unregistered {
        profile_id: Uuid,
    },
    New,
}

/// One immutable target plan for one zero-based approved slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct WorkspaceHoldPlanSlot {
    ordinal: u8,
    target: WorkspaceHoldTarget,
}

impl WorkspaceHoldPlanSlot {
    #[must_use]
    pub(super) const fn new(ordinal: u8, target: WorkspaceHoldTarget) -> Self {
        Self { ordinal, target }
    }
}

pub(super) fn plans_from_v2(plans: &[TeamExpansionApprovalSlotPlan]) -> Vec<WorkspaceHoldPlanSlot> {
    plans
        .iter()
        .map(|plan| {
            let target = match plan.target {
                TeamExpansionWorkspaceTarget::ExistingWorkspaceProfile {
                    profile_id,
                    participation_id,
                } => WorkspaceHoldTarget::ExistingWorkspace {
                    profile_id,
                    participation_id: participation_id.into_uuid(),
                },
                TeamExpansionWorkspaceTarget::NewProfile => WorkspaceHoldTarget::New,
            };
            WorkspaceHoldPlanSlot::new(plan.ordinal, target)
        })
        .collect()
}

pub(super) fn plans_from_v3(
    plans: &[TeamExpansionApprovalSlotPlanV3],
) -> Vec<WorkspaceHoldPlanSlot> {
    plans
        .iter()
        .map(|plan| {
            let target = match plan.target {
                TeamExpansionWorkspaceTargetV3::ExistingWorkspaceProfile {
                    profile_id,
                    participation_id,
                }
                | TeamExpansionWorkspaceTargetV3::ExistingHomeTransfer {
                    profile_id,
                    participation_id,
                    ..
                } => WorkspaceHoldTarget::ExistingWorkspace {
                    profile_id,
                    participation_id: participation_id.into_uuid(),
                },
                TeamExpansionWorkspaceTargetV3::NewProfile => WorkspaceHoldTarget::New,
            };
            WorkspaceHoldPlanSlot::new(plan.ordinal, target)
        })
        .collect()
}

/// Current distinct Workspace occupants and capacity still available.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct WorkspaceCapacitySnapshot {
    used: usize,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct WorkspaceHoldReservation {
    scope: super::TeamExpansionScope,
    decision_id: Uuid,
    request_id: Uuid,
    approved_count: u8,
    recorded_at: TimestampMillis,
}

impl WorkspaceHoldReservation {
    pub(super) const fn new(
        scope: super::TeamExpansionScope,
        decision_id: Uuid,
        request_id: Uuid,
        approved_count: u8,
        recorded_at: TimestampMillis,
    ) -> Self {
        Self {
            scope,
            decision_id,
            request_id,
            approved_count,
            recorded_at,
        }
    }
}

/// Whether an approved decision is protected by a Workspace hold manifest or
/// predates that contract. The legacy state carries no reservation guarantee.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WorkspaceHoldDisposition {
    Reserved,
    LegacyUnreserved,
}

impl WorkspaceCapacitySnapshot {
    #[cfg(test)]
    #[must_use]
    pub(super) const fn used(self) -> usize {
        self.used
    }
}

/// Reserve one Workspace hold for every approved Team slot.
///
/// The caller owns the `IMMEDIATE` transaction and must already have inserted
/// the exact `APPROVED` decision and slot rows in that transaction. Every
/// ordinal must be supplied. Existing Workspace Profiles add no extra logical
/// Agent because their key already appears in the Workspace occupancy set;
/// an unregistered Profile adds one, and a new unknown target gets a unique
/// `(decision, ordinal)` key. Pending cooldown anchors still count because an
/// approved slot exists before the post-commit anchor is written.
///
/// # Errors
/// Returns an error for an incomplete or substituted slot plan, invalid or
/// conflicting Profile identities, corrupt source rows, or a Workspace
/// occupancy above twelve.
pub(super) fn reserve_approved_workspace_holds(
    transaction: &Transaction<'_>,
    reservation: WorkspaceHoldReservation,
    plans: &[WorkspaceHoldPlanSlot],
) -> Result<WorkspaceCapacitySnapshot, OperationsError> {
    reserve_workspace_holds_inner(transaction, reservation, plans, &[])
}

pub(super) fn reserve_approved_workspace_holds_v3(
    transaction: &Transaction<'_>,
    reservation: WorkspaceHoldReservation,
    plans: &[WorkspaceHoldPlanSlot],
    transfer_sources: &[ExistingHomeTransferSource],
) -> Result<WorkspaceCapacitySnapshot, OperationsError> {
    validate_v3_source_shape(plans, transfer_sources)?;
    existing_home_transfer::validate_current_sources(
        transaction,
        reservation.scope.workspace_id,
        reservation.scope.team_id,
        transfer_sources,
    )?;
    existing_home_transfer::ensure_source_time_not_regressed(
        transaction,
        reservation.scope.workspace_id,
        transfer_sources,
        reservation.recorded_at,
    )?;
    let snapshot =
        reserve_workspace_holds_inner(transaction, reservation, plans, transfer_sources)?;
    existing_home_transfer::insert_sources(
        transaction,
        &existing_home_transfer::V3SourceInsert {
            workspace_id: reservation.scope.workspace_id,
            destination_team_id: reservation.scope.team_id,
            decision_id: reservation.decision_id,
            request_id: reservation.request_id,
            slot_count: reservation.approved_count,
            sources: transfer_sources,
            recorded_at: reservation.recorded_at,
        },
    )?;
    Ok(snapshot)
}

fn reserve_workspace_holds_inner(
    transaction: &Transaction<'_>,
    reservation: WorkspaceHoldReservation,
    plans: &[WorkspaceHoldPlanSlot],
    transfer_sources: &[ExistingHomeTransferSource],
) -> Result<WorkspaceCapacitySnapshot, OperationsError> {
    let WorkspaceHoldReservation {
        scope,
        decision_id,
        request_id,
        approved_count,
        recorded_at,
    } = reservation;
    let workspace_id = scope.workspace_id;
    let team_id = scope.team_id;
    validate_identity(
        workspace_id,
        team_id,
        decision_id,
        request_id,
        approved_count,
    )?;
    validate_plan_ordinals(plans, approved_count)?;
    validate_approved_decision_and_slots(
        transaction,
        workspace_id,
        team_id,
        decision_id,
        request_id,
        approved_count,
    )?;

    let current = workspace_capacity_snapshot_for_approval(transaction, workspace_id, decision_id)?;
    if current.used > MAX_WORKSPACE_LOGICAL_AGENTS {
        return Err(OperationsError::Corrupt(
            "Workspace is already above the logical Agent capacity",
        ));
    }

    if manifest_exists(transaction, workspace_id, decision_id)? {
        return Err(OperationsError::Conflict(
            "Team expansion Workspace hold plan already exists",
        ));
    }
    validate_target_keys(
        transaction,
        workspace_id,
        team_id,
        decision_id,
        plans,
        transfer_sources,
    )?;

    transaction.execute(
        "INSERT INTO ops_team_expansion_workspace_hold_manifests(decision_id,request_id,workspace_id,team_id,plan_version,slot_count,recorded_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6)",
        params![
            decision_id.as_bytes(),
            request_id.as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes(),
            i64::from(approved_count),
            recorded_at.get(),
        ],
    )?;

    for plan in plans {
        let (target_kind, profile_id, participation_id) = plan.target.storage_parts();
        transaction.execute(
            "INSERT INTO ops_team_expansion_workspace_hold_plans(decision_id,ordinal,workspace_id,team_id,target_kind,target_profile_id,target_participation_id) VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![
                decision_id.as_bytes(),
                i64::from(plan.ordinal),
                workspace_id.into_uuid().as_bytes(),
                team_id.as_bytes(),
                target_kind,
                profile_id.as_ref().map(<[u8; 16]>::as_slice),
                participation_id.as_ref().map(<[u8; 16]>::as_slice),
            ],
        )?;
    }

    let snapshot = workspace_capacity_snapshot(transaction, workspace_id)?;
    if snapshot.used > MAX_WORKSPACE_LOGICAL_AGENTS {
        return Err(OperationsError::Bounds("workspace logical Agent capacity"));
    }
    validate_manifest_readback(
        transaction,
        workspace_id,
        team_id,
        decision_id,
        request_id,
        approved_count,
        plans,
    )?;
    Ok(snapshot)
}

fn validate_v3_source_shape(
    plans: &[WorkspaceHoldPlanSlot],
    sources: &[ExistingHomeTransferSource],
) -> Result<(), OperationsError> {
    if sources.is_empty() {
        return Err(OperationsError::Conflict(
            "Team expansion V3 requires an existing Home transfer",
        ));
    }
    let mut ordinals = BTreeSet::new();
    for source in sources {
        if !ordinals.insert(source.ordinal) {
            return Err(OperationsError::Conflict(
                "duplicate Team expansion transfer source ordinal",
            ));
        }
        let Some(plan) = plans.iter().find(|plan| plan.ordinal == source.ordinal) else {
            return Err(OperationsError::Conflict(
                "Team expansion transfer source slot missing",
            ));
        };
        if plan.target
            != (WorkspaceHoldTarget::ExistingWorkspace {
                profile_id: source.profile_id,
                participation_id: source.participation_id.into_uuid(),
            })
        {
            return Err(OperationsError::Conflict(
                "Team expansion transfer source target mismatch",
            ));
        }
    }
    Ok(())
}

impl WorkspaceHoldTarget {
    fn storage_parts(self) -> (i64, Option<[u8; 16]>, Option<[u8; 16]>) {
        match self {
            Self::ExistingWorkspace {
                profile_id,
                participation_id,
            } => (
                0,
                Some(*profile_id.as_bytes()),
                Some(*participation_id.as_bytes()),
            ),
            Self::Unregistered { profile_id } => (1, Some(*profile_id.as_bytes()), None),
            Self::New => (2, None, None),
        }
    }
}

/// Verify that the selected Ready Profile is the exact target reserved by the
/// approval. Call from the existing assignment transaction after its current
/// Orchestrator, source, and Team eligibility checks.
///
/// # Errors
/// Returns a conflict for a target that differs from the immutable approval
/// plan/binding or is not the current Workspace participation for the Profile.
pub(super) fn validate_slot_assignment_target(
    connection: &Connection,
    workspace_id: WorkspaceId,
    slot: TeamExpansionSlotRef,
    profile_id: Uuid,
    participation_id: Uuid,
) -> Result<(), OperationsError> {
    validate_workspace_profile_tuple(connection, workspace_id, profile_id, participation_id)?;
    let target = load_plan_target(connection, workspace_id, slot)?.ok_or(
        OperationsError::Conflict("Team expansion Workspace hold plan missing"),
    )?;
    match target {
        WorkspaceHoldTarget::ExistingWorkspace {
            profile_id: planned_profile,
            participation_id: planned_participation,
        } if planned_profile == profile_id && planned_participation == participation_id => Ok(()),
        WorkspaceHoldTarget::Unregistered {
            profile_id: planned_profile,
        } if planned_profile == profile_id => Ok(()),
        WorkspaceHoldTarget::New => {
            let binding = load_binding(connection, workspace_id, slot)?;
            if binding.is_some_and(|binding| binding.profile_id == profile_id) {
                Ok(())
            } else {
                Err(OperationsError::Conflict(
                    "Team expansion Workspace Profile binding mismatch",
                ))
            }
        }
        WorkspaceHoldTarget::ExistingWorkspace { .. }
        | WorkspaceHoldTarget::Unregistered { .. } => Err(OperationsError::Conflict(
            "Team expansion Workspace Profile target mismatch",
        )),
    }
}

/// Read the source-backed Workspace occupancy union in a caller-owned
/// transaction. A live slot use is validated through the existing Team owner
/// history reader before its hold is omitted. Legacy approved decisions with
/// no hold manifest stay visible through their existing API, but are not
/// retroactively counted as reserved.
///
/// # Errors
/// Returns corrupt-history errors for malformed mappings, inconsistent hold
/// manifests, missing slot rows, invalid slot-use receipts, or duplicate
/// active Profile plans.
pub(super) fn workspace_capacity_snapshot(
    connection: &Connection,
    workspace_id: WorkspaceId,
) -> Result<WorkspaceCapacitySnapshot, OperationsError> {
    workspace_capacity_snapshot_inner(connection, workspace_id, None)
}

#[cfg(test)]
pub(crate) fn workspace_capacity_used(
    connection: &Connection,
    workspace_id: WorkspaceId,
) -> Result<usize, OperationsError> {
    Ok(workspace_capacity_snapshot(connection, workspace_id)?.used)
}

fn workspace_capacity_snapshot_for_approval(
    connection: &Connection,
    workspace_id: WorkspaceId,
    current_decision_id: Uuid,
) -> Result<WorkspaceCapacitySnapshot, OperationsError> {
    workspace_capacity_snapshot_inner(connection, workspace_id, Some(current_decision_id))
}

fn workspace_capacity_snapshot_inner(
    connection: &Connection,
    workspace_id: WorkspaceId,
    allowed_unmanifested_decision: Option<Uuid>,
) -> Result<WorkspaceCapacitySnapshot, OperationsError> {
    validate_workspace_hold_history(connection, workspace_id, allowed_unmanifested_decision)?;
    let workspace = workspace_identity_keys(connection, workspace_id)?;
    let holds = active_workspace_hold_keys(connection, workspace_id)?;
    let keys = workspace_key_union(&workspace, &holds);
    let used = keys.len();
    Ok(WorkspaceCapacitySnapshot { used })
}

/// Check a normal Workspace registration against both current identities and
/// Team expansion holds. A registration that matches a Profile already held
/// by an approved slot may proceed: after its pending identity is recorded,
/// the set union still contains the same one logical Agent. All other new
/// identities must fit beneath the shared twelve-Agent bound.
///
/// The caller owns the surrounding IMMEDIATE transaction and must first bind
/// any synthetic Team hold to the exact Profile this registration intends to
/// create. That binding operation requires the separate verified provisioning
/// source/authority held by the caller.
///
/// # Errors
/// Rejects a Profile/participant mapping mismatch, corrupt hold history, or a
/// new logical Agent that would exceed the shared Workspace limit.
pub(crate) fn ensure_registration_capacity_with_holds(
    connection: &Connection,
    workspace_id: WorkspaceId,
    requested_profile_id: Option<AgentProfileId>,
    participant_id: Uuid,
) -> Result<(), OperationsError> {
    validate_non_nil(participant_id, "workspace Agent identity")?;
    let snapshot = workspace_capacity_snapshot(connection, workspace_id)?;
    let mapped_profile = profile_for_participant(connection, workspace_id, participant_id)?;
    let requested_profile = requested_profile_id.map(AgentProfileId::into_uuid);
    if let (Some(requested), Some(mapped)) = (requested_profile, mapped_profile)
        && requested != mapped
    {
        return Err(OperationsError::Conflict(
            "workspace Agent Profile identity",
        ));
    }
    let candidate = requested_profile.or(mapped_profile).map_or(
        WorkspaceIdentityKey::Participant(participant_id),
        WorkspaceIdentityKey::Profile,
    );
    let existing = workspace_identity_keys(connection, workspace_id)?;
    if existing.contains(&candidate) {
        return Ok(());
    }
    let holds = active_workspace_hold_keys(connection, workspace_id)?;
    if holds.contains(&candidate) {
        return Ok(());
    }
    if snapshot.used >= MAX_WORKSPACE_LOGICAL_AGENTS {
        return Err(OperationsError::Bounds("workspace logical Agent capacity"));
    }
    Ok(())
}

fn validate_identity(
    workspace_id: WorkspaceId,
    team_id: Uuid,
    decision_id: Uuid,
    request_id: Uuid,
    approved_count: u8,
) -> Result<(), OperationsError> {
    if workspace_id.into_uuid().is_nil()
        || team_id.is_nil()
        || decision_id.is_nil()
        || request_id.is_nil()
        || approved_count == 0
        || approved_count > 8
    {
        return Err(OperationsError::Conflict(
            "Team expansion Workspace hold identity",
        ));
    }
    Ok(())
}

fn validate_recovery_intent(intent: LegacySlotRecoveryIntent) -> Result<(), OperationsError> {
    if intent.request_id.is_nil()
        || intent.slot.approval_id().is_nil()
        || intent.request_id == intent.slot.approval_id()
        || intent.workspace_id.into_uuid().is_nil()
        || intent.team_id.is_nil()
        || intent.expected_team_revision == 0
        || intent.expected_capacity_revision == 0
        || intent.target_profile_id.is_nil()
    {
        return Err(OperationsError::Conflict(
            "legacy Team expansion recovery identity",
        ));
    }
    Ok(())
}

struct RecoverySourceRaw {
    request: Vec<u8>,
    state: String,
    classification: String,
    approved_count: i64,
    used: Option<Vec<u8>>,
    assigned: Option<Vec<u8>>,
}

fn validate_recovery_source(
    connection: &Connection,
    intent: LegacySlotRecoveryIntent,
) -> Result<Uuid, OperationsError> {
    let source: Option<RecoverySourceRaw> = connection
            .query_row(
                "SELECT marker.request_id,marker.decision_state,marker.classification,
                        decision.approved_count,used.join_request_id,assignment.request_id
                 FROM ops_team_expansion_workspace_hold_pre_hold_decisions AS marker
                 JOIN ops_team_expansion_decisions AS decision
                   ON decision.workspace_id=marker.workspace_id AND decision.team_id=marker.team_id
                  AND decision.decision_id=marker.decision_id AND decision.request_id=marker.request_id
                 JOIN ops_team_expansion_slots AS slot
                   ON slot.workspace_id=decision.workspace_id AND slot.team_id=decision.team_id
                  AND slot.decision_id=decision.decision_id AND slot.request_id=decision.request_id
                  AND slot.ordinal=?4
                 LEFT JOIN ops_team_expansion_slot_uses AS used
                   ON used.workspace_id=slot.workspace_id AND used.team_id=slot.team_id
                  AND used.decision_id=slot.decision_id AND used.ordinal=slot.ordinal
                 LEFT JOIN ops_team_expansion_slot_assignments AS assignment
                   ON assignment.workspace_id=slot.workspace_id AND assignment.team_id=slot.team_id
                  AND assignment.decision_id=slot.decision_id AND assignment.ordinal=slot.ordinal
                 WHERE marker.workspace_id=?1 AND marker.team_id=?2 AND marker.decision_id=?3",
                params![
                    intent.workspace_id.into_uuid().as_bytes(),
                    intent.team_id.as_bytes(),
                    intent.slot.approval_id().as_bytes(),
                    i64::from(intent.slot.ordinal()),
                ],
                |row| Ok(RecoverySourceRaw {
                    request: row.get(0)?, state: row.get(1)?, classification: row.get(2)?,
                    approved_count: row.get(3)?, used: row.get(4)?, assigned: row.get(5)?,
                }),
            )
            .optional()?;
    let Some(source) = source else {
        return Err(OperationsError::Conflict(
            "legacy Team expansion recovery source",
        ));
    };
    if source.state != "APPROVED"
        || source.classification != "PRE_HOLD"
        || intent.slot.ordinal()
            >= u8::try_from(source.approved_count)
                .map_err(|_| OperationsError::Corrupt("approved count"))?
        || source.used.is_some()
        || source.assigned.is_some()
        || manifest_exists(connection, intent.workspace_id, intent.slot.approval_id())?
    {
        return Err(OperationsError::Conflict(
            "legacy Team expansion slot is not recoverable",
        ));
    }
    let approval_request_id = Uuid::from_bytes(wire::id(source.request)?);
    if approval_request_id == intent.request_id {
        return Err(OperationsError::Conflict(
            "legacy recovery request reuses approval identity",
        ));
    }
    Ok(approval_request_id)
}

fn validate_recovery_current_state(
    connection: &Connection,
    intent: LegacySlotRecoveryIntent,
    actor: &TeamExpansionActor,
) -> Result<(), OperationsError> {
    let (team_revision, active) =
        super::team_revision_and_active(connection, intent.workspace_id, intent.team_id)?;
    if !active || team_revision != intent.expected_team_revision {
        return Err(OperationsError::Conflict(
            "legacy Team expansion recovery Team revision",
        ));
    }
    let capacity = super::load_current_capacity(connection, intent.workspace_id, intent.team_id)?
        .ok_or(OperationsError::Conflict("team capacity is not configured"))?;
    if capacity.revision != intent.expected_capacity_revision {
        return Err(OperationsError::Conflict(
            "legacy Team expansion recovery capacity revision",
        ));
    }
    validate_workspace_profile_tuple(
        connection,
        intent.workspace_id,
        intent.target_profile_id,
        intent.target_participation_id.into_uuid(),
    )?;
    let has_home: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_team_members WHERE workspace_id=?1 AND participation_id=?2 AND state='JOINED')",
        params![
            intent.workspace_id.into_uuid().as_bytes(),
            intent.target_participation_id.into_uuid().as_bytes(),
        ],
        |row| row.get(0),
    )?;
    if has_home {
        return Err(OperationsError::Conflict(
            "legacy recovery target has a home Team",
        ));
    }
    let inbound_dispatch: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_temporary_dispatch_reservations WHERE workspace_id=?1 AND receiving_team_id=?2 AND (target_participation_id=?3 OR target_profile_id=?4) AND state IN('RESERVED','SOURCE_READY','ACTIVE','DRAINING'))",
        params![
            intent.workspace_id.into_uuid().as_bytes(),
            intent.team_id.as_bytes(),
            intent.target_participation_id.into_uuid().as_bytes(),
            intent.target_profile_id.as_bytes(),
        ],
        |row| row.get(0),
    )?;
    if inbound_dispatch {
        return Err(OperationsError::Conflict(
            "legacy recovery target has inbound Team dispatch",
        ));
    }
    if actor.participation_id == intent.target_participation_id {
        return Err(OperationsError::Conflict(
            "Workspace Orchestrator cannot be the recovered target",
        ));
    }
    ensure_profile_has_no_other_active_workspace_hold(
        connection,
        intent.workspace_id,
        intent.target_profile_id,
        intent.slot,
    )?;
    let duplicate_recovery: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_team_expansion_legacy_slot_recoveries AS recovery LEFT JOIN ops_team_expansion_slot_uses AS used ON used.workspace_id=recovery.workspace_id AND used.team_id=recovery.team_id AND used.decision_id=recovery.decision_id AND used.ordinal=recovery.ordinal WHERE recovery.workspace_id=?1 AND recovery.target_profile_id=?2 AND used.join_request_id IS NULL)",
        params![intent.workspace_id.into_uuid().as_bytes(), intent.target_profile_id.as_bytes()],
        |row| row.get(0),
    )?;
    if duplicate_recovery {
        return Err(OperationsError::Conflict(
            "Profile already has an active legacy slot recovery",
        ));
    }
    let snapshot = workspace_capacity_snapshot(connection, intent.workspace_id)?;
    if snapshot.used > MAX_WORKSPACE_LOGICAL_AGENTS {
        return Err(OperationsError::Bounds("workspace logical Agent capacity"));
    }
    Ok(())
}

fn validate_plan_ordinals(
    plans: &[WorkspaceHoldPlanSlot],
    approved_count: u8,
) -> Result<(), OperationsError> {
    if plans.len() != usize::from(approved_count)
        || plans
            .iter()
            .enumerate()
            .any(|(ordinal, plan)| usize::from(plan.ordinal) != ordinal)
    {
        return Err(OperationsError::Conflict(
            "Team expansion Workspace hold slot plan",
        ));
    }
    let mut profiles = BTreeSet::new();
    for plan in plans {
        match plan.target {
            WorkspaceHoldTarget::ExistingWorkspace {
                profile_id,
                participation_id,
            } => {
                validate_non_nil(profile_id, "Team expansion Workspace hold Profile")?;
                validate_non_nil(
                    participation_id,
                    "Team expansion Workspace hold participation",
                )?;
                if !profiles.insert(profile_id) {
                    return Err(OperationsError::Conflict(
                        "Profile is selected by multiple Team expansion slots",
                    ));
                }
            }
            WorkspaceHoldTarget::Unregistered { profile_id } => {
                validate_non_nil(profile_id, "Team expansion Workspace hold Profile")?;
                if !profiles.insert(profile_id) {
                    return Err(OperationsError::Conflict(
                        "Profile is selected by multiple Team expansion slots",
                    ));
                }
            }
            WorkspaceHoldTarget::New => {}
        }
    }
    Ok(())
}

fn workspace_key_union(
    workspace: &BTreeSet<WorkspaceIdentityKey>,
    holds: &BTreeSet<WorkspaceIdentityKey>,
) -> BTreeSet<WorkspaceIdentityKey> {
    workspace.union(holds).copied().collect()
}

fn validate_approved_decision_and_slots(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    decision_id: Uuid,
    request_id: Uuid,
    approved_count: u8,
) -> Result<(), OperationsError> {
    let decision: Option<(String, Vec<u8>, Vec<u8>, i64)> = connection
        .query_row(
            "SELECT state,request_id,team_id,approved_count FROM ops_team_expansion_decisions WHERE workspace_id=?1 AND decision_id=?2",
            params![workspace_id.into_uuid().as_bytes(), decision_id.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((state, stored_request, stored_team, stored_count)) = decision else {
        return Err(OperationsError::Conflict("Team expansion decision missing"));
    };
    if state != "APPROVED"
        || Uuid::from_bytes(wire::id(stored_request)?) != request_id
        || Uuid::from_bytes(wire::id(stored_team)?) != team_id
        || u8::try_from(stored_count)
            .map_err(|_| OperationsError::Corrupt("Team expansion approved count"))?
            != approved_count
    {
        return Err(OperationsError::Conflict(
            "Team expansion Workspace hold decision mismatch",
        ));
    }
    for ordinal in 0..approved_count {
        let slot_exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_team_expansion_slots WHERE workspace_id=?1 AND team_id=?2 AND decision_id=?3 AND ordinal=?4 AND request_id=?5)",
            params![
                workspace_id.into_uuid().as_bytes(),
                team_id.as_bytes(),
                decision_id.as_bytes(),
                i64::from(ordinal),
                request_id.as_bytes(),
            ],
            |row| row.get(0),
        )?;
        if !slot_exists {
            return Err(OperationsError::Corrupt(
                "Team expansion approved slot missing",
            ));
        }
    }
    Ok(())
}

fn validate_target_keys(
    connection: &Connection,
    workspace_id: WorkspaceId,
    destination_team_id: Uuid,
    decision_id: Uuid,
    plans: &[WorkspaceHoldPlanSlot],
    transfer_sources: &[ExistingHomeTransferSource],
) -> Result<(), OperationsError> {
    let mut planned_profiles = BTreeSet::new();
    for plan in plans {
        match plan.target {
            WorkspaceHoldTarget::ExistingWorkspace {
                profile_id,
                participation_id,
            } => {
                validate_workspace_profile_tuple(
                    connection,
                    workspace_id,
                    profile_id,
                    participation_id,
                )?;
                if !planned_profiles.insert(profile_id) {
                    return Err(OperationsError::Conflict(
                        "Profile appears in multiple Team expansion slots",
                    ));
                }
                let transfer = transfer_sources
                    .iter()
                    .find(|source| source.ordinal == plan.ordinal);
                if transfer.is_none() {
                    let has_home: bool = connection.query_row(
                        "SELECT EXISTS(SELECT 1 FROM ops_team_members WHERE workspace_id=?1 AND participation_id=?2 AND state='JOINED')",
                        params![workspace_id.into_uuid().as_bytes(), participation_id.as_bytes()],
                        |row| row.get(0),
                    )?;
                    if has_home {
                        return Err(OperationsError::Conflict(
                            "existing home Team transfer requires a V3 source",
                        ));
                    }
                } else if transfer.is_some_and(|source| {
                    source.profile_id != profile_id
                        || source.participation_id.into_uuid() != participation_id
                        || source.source_team_id == destination_team_id
                }) {
                    return Err(OperationsError::Conflict(
                        "Team expansion transfer source target mismatch",
                    ));
                }
                ensure_profile_has_no_other_active_workspace_hold(
                    connection,
                    workspace_id,
                    profile_id,
                    TeamExpansionSlotRef::new(decision_id, plan.ordinal),
                )?;
            }
            WorkspaceHoldTarget::Unregistered { profile_id } => {
                let already_in_workspace = workspace_identity_keys(connection, workspace_id)?
                    .contains(&WorkspaceIdentityKey::Profile(profile_id));
                if already_in_workspace {
                    return Err(OperationsError::Conflict(
                        "Unregistered target Profile already occupies Workspace capacity",
                    ));
                }
            }
            WorkspaceHoldTarget::New => {}
        }
    }
    Ok(())
}

fn validate_workspace_profile_tuple(
    connection: &Connection,
    workspace_id: WorkspaceId,
    profile_id: Uuid,
    participation_id: Uuid,
) -> Result<(), OperationsError> {
    let exact: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_workspace_participations WHERE workspace_id=?1 AND agent_profile_id=?2 AND participation_id=?3)",
        params![
            workspace_id.into_uuid().as_bytes(),
            profile_id.as_bytes(),
            participation_id.as_bytes(),
        ],
        |row| row.get(0),
    )?;
    if exact {
        Ok(())
    } else {
        Err(OperationsError::Conflict(
            "Existing Workspace Profile target is not exact",
        ))
    }
}

fn manifest_exists(
    connection: &Connection,
    workspace_id: WorkspaceId,
    decision_id: Uuid,
) -> Result<bool, OperationsError> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_team_expansion_workspace_hold_manifests WHERE workspace_id=?1 AND decision_id=?2)",
        params![workspace_id.into_uuid().as_bytes(), decision_id.as_bytes()],
        |row| row.get(0),
    )?)
}

fn load_legacy_classification(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    decision_id: Uuid,
) -> Result<Option<(Uuid, String, String)>, OperationsError> {
    let raw: Option<(Vec<u8>, String, String)> = connection
        .query_row(
            "SELECT request_id,decision_state,classification FROM ops_team_expansion_workspace_hold_pre_hold_decisions WHERE workspace_id=?1 AND team_id=?2 AND decision_id=?3",
            params![
                workspace_id.into_uuid().as_bytes(),
                team_id.as_bytes(),
                decision_id.as_bytes(),
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    raw.map(|(request, state, classification)| {
        Ok((Uuid::from_bytes(wire::id(request)?), state, classification))
    })
    .transpose()
}

pub(super) fn is_legacy_approval(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    decision_id: Uuid,
    request_id: Uuid,
) -> Result<bool, OperationsError> {
    Ok(
        load_legacy_classification(connection, workspace_id, team_id, decision_id)?.is_some_and(
            |(stored_request, state, classification)| {
                stored_request == request_id && state == "APPROVED" && classification == "PRE_HOLD"
            },
        ),
    )
}

fn load_decision_identity(
    connection: &Connection,
    workspace_id: WorkspaceId,
    decision_id: Uuid,
) -> Result<Option<(Uuid, Uuid, String, i64)>, OperationsError> {
    let raw: Option<(Vec<u8>, Vec<u8>, String, i64)> = connection
        .query_row(
            "SELECT request_id,team_id,state,approved_count FROM ops_team_expansion_decisions WHERE workspace_id=?1 AND decision_id=?2",
            params![workspace_id.into_uuid().as_bytes(), decision_id.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    raw.map(|(request, team, state, count)| {
        Ok((
            Uuid::from_bytes(wire::id(request)?),
            Uuid::from_bytes(wire::id(team)?),
            state,
            count,
        ))
    })
    .transpose()
}

fn validate_manifest_readback(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    decision_id: Uuid,
    request_id: Uuid,
    approved_count: u8,
    plans: &[WorkspaceHoldPlanSlot],
) -> Result<(), OperationsError> {
    let manifest: Option<(Vec<u8>, Vec<u8>, i64, i64)> = connection
        .query_row(
            "SELECT request_id,team_id,plan_version,slot_count FROM ops_team_expansion_workspace_hold_manifests WHERE workspace_id=?1 AND decision_id=?2",
            params![workspace_id.into_uuid().as_bytes(), decision_id.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((stored_request, stored_team, version, count)) = manifest else {
        return Err(OperationsError::Corrupt(
            "Team expansion Workspace hold manifest missing",
        ));
    };
    if Uuid::from_bytes(wire::id(stored_request)?) != request_id
        || Uuid::from_bytes(wire::id(stored_team)?) != team_id
        || version != 1
        || count != i64::from(approved_count)
    {
        return Err(OperationsError::Corrupt(
            "Team expansion Workspace hold manifest mismatch",
        ));
    }
    for plan in plans {
        let actual: Option<StoredPlanParts> = connection
            .query_row(
                "SELECT target_kind,target_profile_id,target_participation_id FROM ops_team_expansion_workspace_hold_plans WHERE workspace_id=?1 AND team_id=?2 AND decision_id=?3 AND ordinal=?4",
                params![
                    workspace_id.into_uuid().as_bytes(),
                    team_id.as_bytes(),
                    decision_id.as_bytes(),
                    i64::from(plan.ordinal),
                ],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let expected = plan.target.storage_parts();
        let expected_profile = expected.1.map(|value| value.to_vec());
        let expected_participation = expected.2.map(|value| value.to_vec());
        if actual != Some((expected.0, expected_profile, expected_participation)) {
            return Err(OperationsError::Corrupt(
                "Team expansion Workspace hold plan readback mismatch",
            ));
        }
    }
    let actual_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ops_team_expansion_workspace_hold_plans WHERE workspace_id=?1 AND team_id=?2 AND decision_id=?3",
        params![
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes(),
            decision_id.as_bytes(),
        ],
        |row| row.get(0),
    )?;
    if actual_count != i64::from(approved_count) {
        return Err(OperationsError::Corrupt(
            "Team expansion Workspace hold slot count mismatch",
        ));
    }
    validate_workspace_hold_history(connection, workspace_id, None)
}

/// Check an approved decision's exact target plan when recovering a mutation
/// result. `LegacyUnreserved` explicitly means the decision predates Workspace
/// holds; callers preserve its receipt without treating it as reserved. New
/// approval writes must require `Reserved` before commit.
///
/// # Errors
/// Returns `Corrupt` when a present manifest is incomplete or differs from
/// the original slot plan.
pub(super) fn validate_workspace_hold_manifest(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    decision_id: Uuid,
    request_id: Uuid,
    approved_count: u8,
    plans: &[WorkspaceHoldPlanSlot],
) -> Result<WorkspaceHoldDisposition, OperationsError> {
    let has_manifest = manifest_exists(connection, workspace_id, decision_id)?;
    let legacy = load_legacy_classification(connection, workspace_id, team_id, decision_id)?;
    if has_manifest {
        if legacy.is_some() {
            return Err(OperationsError::Corrupt(
                "Team expansion decision is both legacy and reserved",
            ));
        }
        validate_manifest_readback(
            connection,
            workspace_id,
            team_id,
            decision_id,
            request_id,
            approved_count,
            plans,
        )?;
        return Ok(WorkspaceHoldDisposition::Reserved);
    }
    let Some((legacy_request, legacy_state, classification)) = legacy else {
        return Err(OperationsError::Corrupt(
            "Approved Team expansion Workspace hold manifest missing",
        ));
    };
    let current_decision = load_decision_identity(connection, workspace_id, decision_id)?;
    let Some((current_request, current_team, current_state, current_count)) = current_decision
    else {
        return Err(OperationsError::Corrupt("Team expansion decision missing"));
    };
    if legacy_request != request_id
        || classification != "PRE_HOLD"
        || legacy_state != current_state
        || current_request != request_id
        || current_team != team_id
        || current_state != "APPROVED"
        || current_count != i64::from(approved_count)
    {
        return Err(OperationsError::Corrupt(
            "Legacy Team expansion Workspace hold marker mismatch",
        ));
    }
    Ok(WorkspaceHoldDisposition::LegacyUnreserved)
}

pub(super) fn workspace_hold_manifest_matches_intent(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    decision_id: Uuid,
    request_id: Uuid,
    approved_count: u8,
    plans: &[WorkspaceHoldPlanSlot],
) -> Result<bool, OperationsError> {
    let manifest: Option<(Vec<u8>, Vec<u8>, i64, i64)> = connection
        .query_row(
            "SELECT request_id,team_id,plan_version,slot_count
             FROM ops_team_expansion_workspace_hold_manifests
             WHERE workspace_id=?1 AND decision_id=?2",
            params![workspace_id.into_uuid().as_bytes(), decision_id.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((stored_request, stored_team, version, count)) = manifest else {
        return Ok(false);
    };
    if Uuid::from_bytes(wire::id(stored_request)?) != request_id
        || Uuid::from_bytes(wire::id(stored_team)?) != team_id
        || version != 1
        || count != i64::from(approved_count)
        || plans.len() != usize::from(approved_count)
    {
        return Ok(false);
    }
    for plan in plans {
        let actual: Option<StoredPlanParts> = connection
            .query_row(
                "SELECT target_kind,target_profile_id,target_participation_id
                 FROM ops_team_expansion_workspace_hold_plans
                 WHERE workspace_id=?1 AND team_id=?2 AND decision_id=?3 AND ordinal=?4",
                params![
                    workspace_id.into_uuid().as_bytes(),
                    team_id.as_bytes(),
                    decision_id.as_bytes(),
                    i64::from(plan.ordinal),
                ],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let expected = plan.target.storage_parts();
        if actual
            != Some((
                expected.0,
                expected.1.map(|value| value.to_vec()),
                expected.2.map(|value| value.to_vec()),
            ))
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn workspace_identity_keys(
    connection: &Connection,
    workspace_id: WorkspaceId,
) -> Result<BTreeSet<WorkspaceIdentityKey>, OperationsError> {
    let mut statement = connection.prepare(
        "WITH profile_associations AS (
            SELECT participant_id,agent_profile_id AS profile_id
            FROM ops_workspace_participation_mappings WHERE workspace_id=?1
            UNION
            SELECT r.participant_id,s.profile_id
            FROM ops_registration_profile_scopes AS s
            JOIN ops_registration_requests AS r
              ON r.request_id=s.request_id AND r.workspace_id=s.workspace_id
            WHERE s.workspace_id=?1 AND r.workspace_id=?1
        ), participant_ids AS (
            SELECT participant_id FROM ops_logical_members WHERE workspace_id=?1
            UNION
            SELECT participant_id FROM ops_pending_participants WHERE workspace_id=?1
        ), agent_keys(identity_kind,identity_id) AS (
            SELECT 0,agent_profile_id
            FROM ops_workspace_participations WHERE workspace_id=?1
            UNION
            SELECT 0,profile_id FROM profile_associations
            UNION
            SELECT 1,p.participant_id FROM participant_ids AS p
            WHERE NOT EXISTS (
                SELECT 1 FROM profile_associations AS a
                WHERE a.participant_id=p.participant_id
            )
        )
        SELECT identity_kind,identity_id FROM agent_keys ORDER BY identity_kind,identity_id",
    )?;
    let rows = statement.query_map(params![workspace_id.into_uuid().as_bytes()], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
    })?;
    let mut keys = BTreeSet::new();
    for row in rows {
        let (kind, identity) = row?;
        let identity = Uuid::from_bytes(wire::id(identity)?);
        if identity.is_nil() {
            return Err(OperationsError::Corrupt("workspace Agent identity"));
        }
        let key = match kind {
            0 => WorkspaceIdentityKey::Profile(identity),
            1 => WorkspaceIdentityKey::Participant(identity),
            _ => return Err(OperationsError::Corrupt("workspace Agent key kind")),
        };
        keys.insert(key);
    }
    Ok(keys)
}

fn profile_for_participant(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participant_id: Uuid,
) -> Result<Option<Uuid>, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT profile_id FROM (
            SELECT agent_profile_id AS profile_id
            FROM ops_workspace_participation_mappings
            WHERE workspace_id=?1 AND participant_id=?2
            UNION
            SELECT scope.profile_id
            FROM ops_registration_profile_scopes AS scope
            JOIN ops_registration_requests AS request
              ON request.request_id=scope.request_id AND request.workspace_id=scope.workspace_id
            WHERE request.workspace_id=?1 AND request.participant_id=?2
        ) LIMIT 2",
    )?;
    let profiles = statement
        .query_map(
            params![
                workspace_id.into_uuid().as_bytes(),
                participant_id.as_bytes()
            ],
            |row| row.get::<_, Vec<u8>>(0),
        )?
        .collect::<Result<Vec<_>, _>>()?;
    match profiles.as_slice() {
        [] => Ok(None),
        [profile] => {
            let profile = Uuid::from_bytes(wire::id(profile.clone())?);
            if profile.is_nil() {
                Err(OperationsError::Corrupt("workspace Agent Profile identity"))
            } else {
                Ok(Some(profile))
            }
        }
        _ => Err(OperationsError::Corrupt("workspace Agent Profile mapping")),
    }
}

fn active_workspace_hold_keys(
    connection: &Connection,
    workspace_id: WorkspaceId,
) -> Result<BTreeSet<WorkspaceIdentityKey>, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT plan.decision_id,plan.ordinal,plan.target_kind,plan.target_profile_id,plan.target_participation_id,binding.profile_id
         FROM ops_team_expansion_workspace_hold_manifests AS manifest
         JOIN ops_team_expansion_decisions AS decision
           ON decision.workspace_id=manifest.workspace_id AND decision.team_id=manifest.team_id AND decision.decision_id=manifest.decision_id AND decision.request_id=manifest.request_id
         JOIN ops_team_expansion_workspace_hold_plans AS plan
           ON plan.workspace_id=manifest.workspace_id AND plan.team_id=manifest.team_id AND plan.decision_id=manifest.decision_id
         JOIN ops_team_expansion_slots AS slot
           ON slot.workspace_id=plan.workspace_id AND slot.team_id=plan.team_id AND slot.decision_id=plan.decision_id AND slot.ordinal=plan.ordinal
         LEFT JOIN ops_team_expansion_workspace_hold_bindings AS binding
           ON binding.workspace_id=plan.workspace_id AND binding.decision_id=plan.decision_id AND binding.ordinal=plan.ordinal
         LEFT JOIN ops_team_expansion_slot_uses AS slot_use
           ON slot_use.workspace_id=slot.workspace_id AND slot_use.team_id=slot.team_id AND slot_use.decision_id=slot.decision_id AND slot_use.ordinal=slot.ordinal
         WHERE manifest.workspace_id=?1 AND decision.state='APPROVED' AND slot_use.join_request_id IS NULL
         ORDER BY plan.decision_id,plan.ordinal",
    )?;
    let rows = statement.query_map(params![workspace_id.into_uuid().as_bytes()], |row| {
        Ok(HoldKeyRaw {
            decision_id: row.get(0)?,
            ordinal: row.get(1)?,
            target_kind: row.get(2)?,
            target_profile: row.get(3)?,
            target_participation: row.get(4)?,
            bound_profile: row.get(5)?,
        })
    })?;

    let mut keys = BTreeSet::new();
    let mut active_profiles = BTreeSet::new();
    for row in rows {
        let row = row?;
        let decision_id = Uuid::from_bytes(wire::id(row.decision_id)?);
        let ordinal = u8::try_from(row.ordinal)
            .map_err(|_| OperationsError::Corrupt("Team expansion hold ordinal"))?;
        let target_kind = row.target_kind;
        let (target_profile, _target_participation) =
            parse_target(target_kind, row.target_profile, row.target_participation)?;
        let bound_profile = row
            .bound_profile
            .map(|bytes| wire::id(bytes).map(Uuid::from_bytes))
            .transpose()?;
        let key = match (target_profile, bound_profile) {
            // The approval-time plan is immutable source evidence for the
            // reserved stable Profile. A later Workspace departure must not
            // free that seat or make unrelated capacity reads fail. Current
            // participation is revalidated when assigning or consuming.
            (Some(target), None) => WorkspaceIdentityKey::Profile(target),
            (None, Some(bound)) => WorkspaceIdentityKey::Profile(bound),
            (None, None) if target_kind == 2 => WorkspaceIdentityKey::UnboundSlot {
                decision_id,
                ordinal,
            },
            _ => {
                return Err(OperationsError::Corrupt(
                    "Team expansion Workspace hold target binding",
                ));
            }
        };
        if let WorkspaceIdentityKey::Profile(profile) = key
            && !active_profiles.insert(profile)
        {
            return Err(OperationsError::Corrupt(
                "Profile is held by multiple Team expansion slots",
            ));
        }
        if !keys.insert(key) {
            return Err(OperationsError::Corrupt(
                "duplicate active Team expansion Workspace hold",
            ));
        }
    }
    let mut recoveries = connection.prepare(
        "SELECT recovery.target_profile_id
         FROM ops_team_expansion_legacy_slot_recoveries AS recovery
         LEFT JOIN ops_team_expansion_slot_uses AS used
           ON used.workspace_id=recovery.workspace_id AND used.team_id=recovery.team_id
          AND used.decision_id=recovery.decision_id AND used.ordinal=recovery.ordinal
         WHERE recovery.workspace_id=?1 AND used.join_request_id IS NULL",
    )?;
    let rows = recoveries.query_map(params![workspace_id.into_uuid().as_bytes()], |row| {
        row.get::<_, Vec<u8>>(0)
    })?;
    for row in rows {
        let profile = Uuid::from_bytes(wire::id(row?)?);
        if !active_profiles.insert(profile) || !keys.insert(WorkspaceIdentityKey::Profile(profile))
        {
            return Err(OperationsError::Corrupt(
                "Profile is held by multiple Team expansion reservations",
            ));
        }
    }
    Ok(keys)
}

fn ensure_profile_has_no_other_active_workspace_hold(
    connection: &Connection,
    workspace_id: WorkspaceId,
    profile_id: Uuid,
    current_slot: TeamExpansionSlotRef,
) -> Result<(), OperationsError> {
    let duplicate: bool = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM ops_team_expansion_workspace_hold_plans AS plan
            JOIN ops_team_expansion_decisions AS decision
              ON decision.workspace_id=plan.workspace_id AND decision.team_id=plan.team_id AND decision.decision_id=plan.decision_id
            LEFT JOIN ops_team_expansion_workspace_hold_bindings AS binding
              ON binding.workspace_id=plan.workspace_id AND binding.decision_id=plan.decision_id AND binding.ordinal=plan.ordinal
            LEFT JOIN ops_team_expansion_slot_uses AS used
              ON used.workspace_id=plan.workspace_id AND used.team_id=plan.team_id AND used.decision_id=plan.decision_id AND used.ordinal=plan.ordinal
            WHERE plan.workspace_id=?1 AND decision.state='APPROVED' AND used.join_request_id IS NULL
              AND (plan.target_profile_id=?2 OR binding.profile_id=?2)
              AND (plan.decision_id<>?3 OR plan.ordinal<>?4)
        )",
        params![
            workspace_id.into_uuid().as_bytes(),
            profile_id.as_bytes(),
            current_slot.approval_id().as_bytes(),
            i64::from(current_slot.ordinal()),
        ],
        |row| row.get(0),
    )?;
    let legacy_duplicate: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_team_expansion_legacy_slot_recoveries AS recovery LEFT JOIN ops_team_expansion_slot_uses AS used ON used.workspace_id=recovery.workspace_id AND used.team_id=recovery.team_id AND used.decision_id=recovery.decision_id AND used.ordinal=recovery.ordinal WHERE recovery.workspace_id=?1 AND recovery.target_profile_id=?2 AND used.join_request_id IS NULL AND (recovery.decision_id<>?3 OR recovery.ordinal<>?4))",
        params![
            workspace_id.into_uuid().as_bytes(),
            profile_id.as_bytes(),
            current_slot.approval_id().as_bytes(),
            i64::from(current_slot.ordinal()),
        ],
        |row| row.get(0),
    )?;
    if duplicate || legacy_duplicate {
        return Err(OperationsError::Conflict(
            "Profile already has an active Team expansion hold",
        ));
    }
    Ok(())
}

fn load_plan_target(
    connection: &Connection,
    workspace_id: WorkspaceId,
    slot: TeamExpansionSlotRef,
) -> Result<Option<WorkspaceHoldTarget>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT target_kind,target_profile_id,target_participation_id FROM ops_team_expansion_workspace_hold_plans WHERE workspace_id=?1 AND decision_id=?2 AND ordinal=?3",
            params![
                workspace_id.into_uuid().as_bytes(),
                slot.approval_id().as_bytes(),
                i64::from(slot.ordinal()),
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<Vec<u8>>>(1)?,
                    row.get::<_, Option<Vec<u8>>>(2)?,
                ))
            },
        )
        .optional()?;
    let Some((kind, profile, participation)) = raw else {
        let recovery: Option<(Vec<u8>, Vec<u8>)> = connection
            .query_row(
                "SELECT target_profile_id,target_participation_id FROM ops_team_expansion_legacy_slot_recoveries WHERE workspace_id=?1 AND decision_id=?2 AND ordinal=?3",
                params![
                    workspace_id.into_uuid().as_bytes(),
                    slot.approval_id().as_bytes(),
                    i64::from(slot.ordinal()),
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        return recovery
            .map(|(profile, participation)| {
                Ok(WorkspaceHoldTarget::ExistingWorkspace {
                    profile_id: Uuid::from_bytes(wire::id(profile)?),
                    participation_id: Uuid::from_bytes(wire::id(participation)?),
                })
            })
            .transpose();
    };
    let (profile_id, participation_id) = parse_target(kind, profile, participation)?;
    match (kind, profile_id, participation_id) {
        (0, Some(profile_id), Some(participation_id)) => {
            Ok(Some(WorkspaceHoldTarget::ExistingWorkspace {
                profile_id,
                participation_id,
            }))
        }
        (1, Some(profile_id), None) => Ok(Some(WorkspaceHoldTarget::Unregistered { profile_id })),
        (2, None, None) => Ok(Some(WorkspaceHoldTarget::New)),
        _ => Err(OperationsError::Corrupt(
            "Team expansion Workspace hold target shape",
        )),
    }
}

fn load_binding(
    connection: &Connection,
    workspace_id: WorkspaceId,
    slot: TeamExpansionSlotRef,
) -> Result<Option<HoldBinding>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT profile_id FROM ops_team_expansion_workspace_hold_bindings WHERE workspace_id=?1 AND decision_id=?2 AND ordinal=?3",
            params![
                workspace_id.into_uuid().as_bytes(),
                slot.approval_id().as_bytes(),
                i64::from(slot.ordinal()),
            ],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?;
    raw.map(|profile_id| {
        Ok(HoldBinding {
            profile_id: Uuid::from_bytes(wire::id(profile_id)?),
        })
    })
    .transpose()
}

fn load_recovery_by_request(
    connection: &Connection,
    workspace_id: WorkspaceId,
    request_id: Uuid,
    replayed: bool,
) -> Result<Option<LegacySlotRecoveryReceipt>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT decision_id,ordinal,approval_request_id,team_id,expected_team_revision,
                    capacity_revision,leadership_revision,target_profile_id,target_participation_id,
                    actor_activity_id,actor_agent_id,actor_session_id,actor_participation_id,
                    actor_profile_id,actor_participant_id,actor_display_name,recorded_at_ms
             FROM ops_team_expansion_legacy_slot_recoveries
             WHERE workspace_id=?1 AND request_id=?2",
            params![workspace_id.into_uuid().as_bytes(), request_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, Vec<u8>>(7)?,
                    row.get::<_, Vec<u8>>(8)?,
                    row.get::<_, Vec<u8>>(9)?,
                    row.get::<_, Vec<u8>>(10)?,
                    row.get::<_, Vec<u8>>(11)?,
                    row.get::<_, Vec<u8>>(12)?,
                    row.get::<_, Vec<u8>>(13)?,
                    row.get::<_, Vec<u8>>(14)?,
                    row.get::<_, String>(15)?,
                    row.get::<_, i64>(16)?,
                ))
            },
        )
        .optional()?;
    raw.map(|raw| decode_recovery(request_id, workspace_id, replayed, raw))
        .transpose()
}

fn decode_recovery(
    request_id: Uuid,
    workspace_id: WorkspaceId,
    replayed: bool,
    raw: RawRecoveryTuple,
) -> Result<LegacySlotRecoveryReceipt, OperationsError> {
    let intent = LegacySlotRecoveryIntent {
        request_id,
        slot: TeamExpansionSlotRef::new(
            Uuid::from_bytes(wire::id(raw.0)?),
            u8::try_from(raw.1).map_err(|_| OperationsError::Corrupt("recovery ordinal"))?,
        ),
        workspace_id,
        team_id: Uuid::from_bytes(wire::id(raw.3)?),
        expected_team_revision: u64::try_from(raw.4)
            .map_err(|_| OperationsError::Corrupt("recovery Team revision"))?,
        expected_capacity_revision: u64::try_from(raw.5)
            .map_err(|_| OperationsError::Corrupt("recovery capacity revision"))?,
        target_profile_id: Uuid::from_bytes(wire::id(raw.7)?),
        target_participation_id: crate::WorkspaceParticipationId::from_uuid(Uuid::from_bytes(
            wire::id(raw.8)?,
        ))?,
    };
    Ok(LegacySlotRecoveryReceipt {
        intent,
        approval_request_id: Uuid::from_bytes(wire::id(raw.2)?),
        leadership_revision: raw.6,
        actor: TeamExpansionActor {
            activity_id: forge_domain::ActivityId::from_uuid(Uuid::from_bytes(wire::id(raw.9)?)),
            agent_id: forge_domain::AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(raw.10)?)),
            session_id: forge_domain::AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(
                raw.11,
            )?)),
            participation_id: crate::WorkspaceParticipationId::from_uuid(Uuid::from_bytes(
                wire::id(raw.12)?,
            ))?,
            profile_id: Uuid::from_bytes(wire::id(raw.13)?),
            participant_id: Uuid::from_bytes(wire::id(raw.14)?),
            display_name: raw.15,
        },
        recorded_at: TimestampMillis::new(raw.16)
            .map_err(|_| OperationsError::Corrupt("recovery timestamp"))?,
        replayed,
    })
}

fn recovery_request_exists(
    connection: &Connection,
    request_id: Uuid,
) -> Result<bool, OperationsError> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_team_expansion_legacy_slot_recoveries WHERE request_id=?1)",
            params![request_id.as_bytes()],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

fn recovery_matches_intent(
    receipt: &LegacySlotRecoveryReceipt,
    intent: LegacySlotRecoveryIntent,
) -> bool {
    receipt.intent == intent
}

fn validate_workspace_hold_history(
    connection: &Connection,
    workspace_id: WorkspaceId,
    allowed_unmanifested_decision: Option<Uuid>,
) -> Result<(), OperationsError> {
    let incomplete: bool = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM ops_team_expansion_workspace_hold_manifests AS manifest
            JOIN ops_team_expansion_decisions AS decision
              ON decision.workspace_id=manifest.workspace_id AND decision.team_id=manifest.team_id AND decision.decision_id=manifest.decision_id AND decision.request_id=manifest.request_id
            WHERE manifest.workspace_id=?1 AND (
                decision.state<>'APPROVED' OR decision.approved_count<>manifest.slot_count OR manifest.plan_version<>1 OR
                (SELECT COUNT(*) FROM ops_team_expansion_workspace_hold_plans AS plan WHERE plan.workspace_id=manifest.workspace_id AND plan.team_id=manifest.team_id AND plan.decision_id=manifest.decision_id)<>manifest.slot_count OR
                (SELECT MIN(plan.ordinal) FROM ops_team_expansion_workspace_hold_plans AS plan WHERE plan.workspace_id=manifest.workspace_id AND plan.team_id=manifest.team_id AND plan.decision_id=manifest.decision_id)<>0 OR
                (SELECT MAX(plan.ordinal) FROM ops_team_expansion_workspace_hold_plans AS plan WHERE plan.workspace_id=manifest.workspace_id AND plan.team_id=manifest.team_id AND plan.decision_id=manifest.decision_id)<>manifest.slot_count-1 OR
                (SELECT COUNT(*) FROM ops_team_expansion_slots AS slot WHERE slot.workspace_id=manifest.workspace_id AND slot.team_id=manifest.team_id AND slot.decision_id=manifest.decision_id AND slot.request_id=manifest.request_id)<>manifest.slot_count
            )
        )",
        params![workspace_id.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    let orphan_plan: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_team_expansion_workspace_hold_plans AS plan LEFT JOIN ops_team_expansion_workspace_hold_manifests AS manifest ON manifest.workspace_id=plan.workspace_id AND manifest.team_id=plan.team_id AND manifest.decision_id=plan.decision_id WHERE plan.workspace_id=?1 AND manifest.decision_id IS NULL)",
        params![workspace_id.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    let orphan_binding: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_team_expansion_workspace_hold_bindings AS binding LEFT JOIN ops_team_expansion_workspace_hold_plans AS plan ON plan.workspace_id=binding.workspace_id AND plan.decision_id=binding.decision_id AND plan.ordinal=binding.ordinal WHERE binding.workspace_id=?1 AND plan.decision_id IS NULL)",
        params![workspace_id.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    let orphan_manifest: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_team_expansion_workspace_hold_manifests AS manifest LEFT JOIN ops_team_expansion_decisions AS decision ON decision.workspace_id=manifest.workspace_id AND decision.team_id=manifest.team_id AND decision.decision_id=manifest.decision_id AND decision.request_id=manifest.request_id WHERE manifest.workspace_id=?1 AND decision.decision_id IS NULL)",
        params![workspace_id.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    let unclassified_approval: bool = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM ops_team_expansion_decisions AS decision
            LEFT JOIN ops_team_expansion_workspace_hold_manifests AS manifest
              ON manifest.workspace_id=decision.workspace_id AND manifest.team_id=decision.team_id AND manifest.decision_id=decision.decision_id AND manifest.request_id=decision.request_id
            LEFT JOIN ops_team_expansion_workspace_hold_pre_hold_decisions AS legacy
              ON legacy.workspace_id=decision.workspace_id AND legacy.team_id=decision.team_id AND legacy.decision_id=decision.decision_id AND legacy.request_id=decision.request_id
            WHERE decision.workspace_id=?1 AND decision.state='APPROVED' AND manifest.decision_id IS NULL AND legacy.decision_id IS NULL
              AND (?2 IS NULL OR decision.decision_id<>?2)
        )",
        params![
            workspace_id.into_uuid().as_bytes(),
            allowed_unmanifested_decision
                .as_ref()
                .map(|id| id.as_bytes().as_slice()),
        ],
        |row| row.get(0),
    )?;
    let doubly_classified: bool = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM ops_team_expansion_workspace_hold_manifests AS manifest
            JOIN ops_team_expansion_workspace_hold_pre_hold_decisions AS legacy
              ON legacy.workspace_id=manifest.workspace_id AND legacy.team_id=manifest.team_id AND legacy.decision_id=manifest.decision_id
            WHERE manifest.workspace_id=?1
        )",
        params![workspace_id.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    let mismatched_legacy: bool = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM ops_team_expansion_workspace_hold_pre_hold_decisions AS legacy
            LEFT JOIN ops_team_expansion_decisions AS decision
              ON decision.workspace_id=legacy.workspace_id AND decision.team_id=legacy.team_id AND decision.decision_id=legacy.decision_id AND decision.request_id=legacy.request_id
            WHERE legacy.workspace_id=?1 AND (decision.decision_id IS NULL OR decision.state<>legacy.decision_state OR legacy.classification<>'PRE_HOLD')
        )",
        params![workspace_id.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    if incomplete
        || orphan_plan
        || orphan_binding
        || orphan_manifest
        || unclassified_approval
        || doubly_classified
        || mismatched_legacy
    {
        return Err(OperationsError::Corrupt(
            "Team expansion Workspace hold history incomplete",
        ));
    }
    validate_consumed_hold_targets(connection, workspace_id)?;
    Ok(())
}

fn validate_consumed_hold_targets(
    connection: &Connection,
    workspace_id: WorkspaceId,
) -> Result<(), OperationsError> {
    let mut statement = connection.prepare(
        "SELECT plan.decision_id,plan.ordinal,plan.target_kind,plan.target_profile_id,plan.target_participation_id,binding.profile_id,slot_use.join_request_id,slot_use.profile_id,slot_use.participation_id
         FROM ops_team_expansion_workspace_hold_manifests AS manifest
         JOIN ops_team_expansion_decisions AS decision
           ON decision.workspace_id=manifest.workspace_id AND decision.team_id=manifest.team_id AND decision.decision_id=manifest.decision_id AND decision.request_id=manifest.request_id
         JOIN ops_team_expansion_workspace_hold_plans AS plan
           ON plan.workspace_id=manifest.workspace_id AND plan.team_id=manifest.team_id AND plan.decision_id=manifest.decision_id
         JOIN ops_team_expansion_slots AS slot
           ON slot.workspace_id=plan.workspace_id AND slot.team_id=plan.team_id AND slot.decision_id=plan.decision_id AND slot.ordinal=plan.ordinal
         JOIN ops_team_expansion_slot_uses AS slot_use
           ON slot_use.workspace_id=slot.workspace_id AND slot_use.team_id=slot.team_id AND slot_use.decision_id=slot.decision_id AND slot_use.ordinal=slot.ordinal
         LEFT JOIN ops_team_expansion_workspace_hold_bindings AS binding
           ON binding.workspace_id=plan.workspace_id AND binding.decision_id=plan.decision_id AND binding.ordinal=plan.ordinal
         WHERE manifest.workspace_id=?1 AND decision.state='APPROVED'
         ORDER BY plan.decision_id,plan.ordinal",
    )?;
    let rows = statement.query_map(params![workspace_id.into_uuid().as_bytes()], |row| {
        Ok((
            row.get::<_, Vec<u8>>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, Option<Vec<u8>>>(3)?,
            row.get::<_, Option<Vec<u8>>>(4)?,
            row.get::<_, Option<Vec<u8>>>(5)?,
            row.get::<_, Vec<u8>>(6)?,
            row.get::<_, Vec<u8>>(7)?,
            row.get::<_, Vec<u8>>(8)?,
        ))
    })?;
    for row in rows {
        let (
            decision_id_raw,
            ordinal_raw,
            target_kind,
            target_profile,
            target_participation,
            bound_profile,
            join_request_raw,
            used_profile_raw,
            used_participation_raw,
        ) = row?;
        let decision_id = Uuid::from_bytes(wire::id(decision_id_raw)?);
        let ordinal = u8::try_from(ordinal_raw)
            .map_err(|_| OperationsError::Corrupt("Team expansion hold ordinal"))?;
        let slot = TeamExpansionSlotRef::new(decision_id, ordinal);
        let join_request = Uuid::from_bytes(wire::id(join_request_raw)?);
        if super::slot_ref_for_join_request(connection, join_request)? != Some(slot) {
            return Err(OperationsError::Corrupt(
                "Team expansion Workspace hold slot use mismatch",
            ));
        }
        let (planned_profile, planned_participation) =
            parse_target(target_kind, target_profile, target_participation)?;
        let bound_profile = bound_profile
            .map(|bytes| wire::id(bytes).map(Uuid::from_bytes))
            .transpose()?;
        let used_profile = Uuid::from_bytes(wire::id(used_profile_raw)?);
        let used_participation = Uuid::from_bytes(wire::id(used_participation_raw)?);
        let expected_profile = match (planned_profile, bound_profile) {
            (Some(profile), None) | (None, Some(profile)) => profile,
            (None, None) => {
                return Err(OperationsError::Corrupt(
                    "unbound Team expansion slot was consumed",
                ));
            }
            (Some(_), Some(_)) => {
                return Err(OperationsError::Corrupt(
                    "fixed Team expansion target has a second Profile binding",
                ));
            }
        };
        if used_profile != expected_profile
            || planned_participation.is_some_and(|value| value != used_participation)
        {
            return Err(OperationsError::Corrupt(
                "Team expansion Workspace hold target and JOIN disagree",
            ));
        }
    }
    Ok(())
}

fn validate_non_nil(value: Uuid, field: &'static str) -> Result<(), OperationsError> {
    if value.is_nil() {
        Err(OperationsError::Conflict(field))
    } else {
        Ok(())
    }
}

fn parse_target(
    kind: i64,
    profile: Option<Vec<u8>>,
    participation: Option<Vec<u8>>,
) -> Result<(Option<Uuid>, Option<Uuid>), OperationsError> {
    let profile = profile
        .map(|bytes| wire::id(bytes).map(Uuid::from_bytes))
        .transpose()?;
    let participation = participation
        .map(|bytes| wire::id(bytes).map(Uuid::from_bytes))
        .transpose()?;
    let valid = match kind {
        0 => profile.is_some() && participation.is_some(),
        1 => profile.is_some() && participation.is_none(),
        2 => profile.is_none() && participation.is_none(),
        _ => false,
    };
    if !valid
        || profile.is_some_and(|value| value.is_nil())
        || participation.is_some_and(|value| value.is_nil())
    {
        return Err(OperationsError::Corrupt(
            "Team expansion Workspace hold target shape",
        ));
    }
    Ok((profile, participation))
}

struct HoldKeyRaw {
    decision_id: Vec<u8>,
    ordinal: i64,
    target_kind: i64,
    target_profile: Option<Vec<u8>>,
    target_participation: Option<Vec<u8>>,
    bound_profile: Option<Vec<u8>>,
}

struct HoldBinding {
    profile_id: Uuid,
}
