//! Runtime boundary for Team capacity decisions and Orchestrator-assigned slots.
//!
//! Operations owns revision checks, capacity holds, cooldown anchors and slot
//! use. This layer derives the acting Profile from the authenticated Session;
//! request bodies never provide a caller Agent or Profile ID.

use std::time::{SystemTime, UNIX_EPOCH};

use forge_domain::{AuthenticatedPrincipal, TimestampMillis, WorkspaceId};
use heterarchy_orchestration_operations::team_expansion::{
    TeamExpansionRevisions, TeamExpansionScope,
};
use heterarchy_orchestration_operations::{
    LegacySlotRecoveryCommand, LegacySlotRecoveryIntent,
    LegacySlotRecoveryReceipt as OperationsLegacySlotRecoveryReceipt, OperationalActor,
    OperationsError, TeamCapacityReceipt, TeamCapacitySetCommand, TeamCapacitySnapshot,
    TeamExpansionApprovalSlotPlan, TeamExpansionApprovalSlotPlanV3, TeamExpansionDecision,
    TeamExpansionDecisionCommand, TeamExpansionDecisionReceipt, TeamExpansionDecisionV2Command,
    TeamExpansionDecisionV3Command, TeamExpansionDecisionV3Projection, TeamExpansionRequestCommand,
    TeamExpansionRequestReceipt, TeamExpansionSlotAssignmentCommand,
    TeamExpansionSlotAssignmentReceipt, TeamExpansionSlotRef, TeamExpansionWorkspaceTarget,
    TeamExpansionWorkspaceTargetV3, TeamMemberCapacity, WorkspaceParticipationId,
};
use uuid::Uuid;

use crate::{OrchestrationRuntime, RuntimeError, teams::participant_actor};

/// Authenticated actor selectors for a Workspace-scoped Team operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamExpansionActorTarget {
    /// Workspace containing the Team.
    pub workspace_id: Uuid,
    /// Source Activity of the logical Session.
    pub activity_id: Uuid,
    /// Open Session authenticated by the request principal.
    pub session_id: Uuid,
}

/// Exact Ready target selected by the Workspace Orchestrator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamExpansionTarget {
    /// Activity of the selected target Session.
    pub activity_id: Uuid,
    /// Ready target Session; Agent/Profile identity is resolved by Operations.
    pub session_id: Uuid,
}

/// Change one explicit Team-specific member cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamCapacitySetSpec {
    /// Authenticated current Workspace Orchestrator.
    pub actor: TeamExpansionActorTarget,
    /// Team whose cap changes.
    pub team_id: Uuid,
    /// Exact mutation/readback identity.
    pub request_id: Uuid,
    /// CAS values observed by the Orchestrator.
    pub expected_team_revision: u64,
    pub expected_capacity_revision: u64,
    /// Workspace Orchestrator epoch observed when this exact setting was issued.
    pub expected_leadership_revision: i64,
    /// Explicit cap, bounded by the global limit.
    pub max_members: u8,
}

/// Request additional members as the current Team Leader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamExpansionRequestSpec {
    /// Authenticated Team Leader.
    pub actor: TeamExpansionActorTarget,
    /// Target Team and exact request identity.
    pub team_id: Uuid,
    pub request_id: Uuid,
    /// Team, cap and leadership CAS values observed by the requester.
    pub expected_team_revision: u64,
    pub expected_capacity_revision: u64,
    pub expected_leadership_revision: i64,
    /// Requested count and bounded reason.
    pub requested_count: u8,
    pub reason: String,
}

/// Workspace Orchestrator decision on one exact Team Leader request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeamExpansionDecisionSpec {
    /// Approve this many slots from the request.
    Approve { approved_count: u8 },
    /// Reject the request without a cooldown.
    Reject,
}

/// Orchestrator decision command and all observed CAS revisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamExpansionDecideSpec {
    /// Authenticated current Workspace Orchestrator.
    pub actor: TeamExpansionActorTarget,
    /// Team, request and decision identities.
    pub team_id: Uuid,
    pub request_id: Uuid,
    pub decision_id: Uuid,
    /// Revisions observed by the Orchestrator.
    pub expected_team_revision: u64,
    pub expected_capacity_revision: u64,
    pub expected_leadership_revision: i64,
    pub expected_requester_leadership_revision: i64,
    /// Exact decision payload.
    pub decision: TeamExpansionDecisionSpec,
}

/// Target for an exact Workspace-capacity reservation on one V2 approval slot.
/// New Profile provisioning is intentionally not exposed at this boundary
/// until its source-authorized Profile binding operation is available.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamExpansionDecisionV2TargetSpec {
    /// Existing stable Profile already registered in this Workspace.
    pub profile_id: Uuid,
    /// Exact stable Workspace participation for that Profile.
    pub participation_id: Uuid,
}

/// One ordered target plan entry for a Workspace-capacity-backed approval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamExpansionDecisionV2SlotSpec {
    /// Zero-based ordinal in the approval.
    pub ordinal: u8,
    /// Exact existing Profile reserved by this slot.
    pub target: TeamExpansionDecisionV2TargetSpec,
}

/// Exact V2 decision intent. An approval must include one target plan entry
/// per approved slot; a rejection has no entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamExpansionDecideV2Spec {
    /// Base decision, exact IDs and CAS revisions.
    pub decision: TeamExpansionDecideSpec,
    /// Complete ordered Workspace target plan.
    pub slot_plan: Vec<TeamExpansionDecisionV2SlotSpec>,
}

/// Target for one V3 approval slot. Home transfers retain the exact source
/// Team membership revision but do not authorize consent, acceptance,
/// assignment, handoff, or a completed transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeamExpansionDecisionV3TargetSpec {
    /// Existing Profile with no current Home Team.
    ExistingWorkspaceProfile {
        profile_id: Uuid,
        participation_id: Uuid,
    },
    /// Existing Profile with an exact current Home Team membership.
    ExistingHomeTransfer {
        profile_id: Uuid,
        participation_id: Uuid,
        source_team_id: Uuid,
        source_membership_revision: u64,
    },
}

/// One ordered V3 target plan entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamExpansionDecisionV3SlotSpec {
    /// Zero-based ordinal in the approval.
    pub ordinal: u8,
    /// Exact existing target and optional source Home evidence.
    pub target: TeamExpansionDecisionV3TargetSpec,
}

/// Exact V3 approval intent, retaining each target's source Home evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamExpansionDecideV3Spec {
    /// Base decision and CAS revisions.
    pub decision: TeamExpansionDecideSpec,
    /// Complete ordered Workspace target plan.
    pub slot_plan: Vec<TeamExpansionDecisionV3SlotSpec>,
}

/// Exact legacy approval target to recover into a current Workspace hold.
/// `recovery_request_id` is a new idempotency identity; `approval_id` and
/// `ordinal` continue to refer to the original approved Team slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LegacyTeamExpansionSlotRecoveryIntentSpec {
    pub recovery_request_id: Uuid,
    pub approval_id: Uuid,
    pub ordinal: u8,
    pub workspace_id: Uuid,
    pub team_id: Uuid,
    pub expected_team_revision: u64,
    pub expected_capacity_revision: u64,
    pub target_profile_id: Uuid,
    pub target_participation_id: Uuid,
}

/// Current Orchestrator operation that recovers one exact pre-hold slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoverLegacyTeamExpansionSlotSpec {
    pub actor: TeamExpansionActorTarget,
    pub intent: LegacyTeamExpansionSlotRecoveryIntentSpec,
    pub expected_leadership_revision: i64,
}

fn legacy_slot_recovery_intent(
    spec: LegacyTeamExpansionSlotRecoveryIntentSpec,
) -> Result<LegacySlotRecoveryIntent, RuntimeError> {
    let target_participation_id = WorkspaceParticipationId::from_uuid(spec.target_participation_id)
        .map_err(RuntimeError::persistence)?;
    let scope = TeamExpansionScope::new(
        WorkspaceId::from_uuid(require_identity(spec.workspace_id)?),
        require_identity(spec.team_id)?,
    );
    Ok(LegacySlotRecoveryIntent::from_recovery_identity(
        require_identity(spec.recovery_request_id)?,
        TeamExpansionSlotRef::new(require_identity(spec.approval_id)?, spec.ordinal),
        scope,
        spec.expected_team_revision,
        spec.expected_capacity_revision,
        require_identity(spec.target_profile_id)?,
        target_participation_id,
    ))
}

fn team_expansion_v2_command(
    spec: TeamExpansionDecideV2Spec,
) -> Result<
    (
        TeamExpansionDecisionV2Command,
        crate::participation::ParticipationTarget,
    ),
    RuntimeError,
> {
    let TeamExpansionDecideV2Spec {
        decision: spec,
        slot_plan,
    } = spec;
    let (base, participation) = team_expansion_base_command(spec)?;
    let slot_plan = slot_plan
        .into_iter()
        .map(|slot| {
            let participation_id =
                WorkspaceParticipationId::from_uuid(slot.target.participation_id)
                    .map_err(RuntimeError::persistence)?;
            Ok(TeamExpansionApprovalSlotPlan::new(
                slot.ordinal,
                TeamExpansionWorkspaceTarget::ExistingWorkspaceProfile {
                    profile_id: slot.target.profile_id,
                    participation_id,
                },
            ))
        })
        .collect::<Result<Vec<_>, RuntimeError>>()?;
    Ok((
        TeamExpansionDecisionV2Command::new(base, slot_plan),
        participation,
    ))
}

fn team_expansion_v3_command(
    spec: TeamExpansionDecideV3Spec,
) -> Result<
    (
        TeamExpansionDecisionV3Command,
        crate::participation::ParticipationTarget,
    ),
    RuntimeError,
> {
    let TeamExpansionDecideV3Spec {
        decision: spec,
        slot_plan,
    } = spec;
    if !matches!(spec.decision, TeamExpansionDecisionSpec::Approve { .. }) {
        return Err(RuntimeError::InvalidInput);
    }
    let (base, participation) = team_expansion_base_command(spec)?;
    let slot_plan = slot_plan
        .into_iter()
        .map(|slot| {
            let target = match slot.target {
                TeamExpansionDecisionV3TargetSpec::ExistingWorkspaceProfile {
                    profile_id,
                    participation_id,
                } => TeamExpansionWorkspaceTargetV3::ExistingWorkspaceProfile {
                    profile_id: require_identity(profile_id)?,
                    participation_id: WorkspaceParticipationId::from_uuid(participation_id)
                        .map_err(RuntimeError::persistence)?,
                },
                TeamExpansionDecisionV3TargetSpec::ExistingHomeTransfer {
                    profile_id,
                    participation_id,
                    source_team_id,
                    source_membership_revision,
                } => {
                    if source_membership_revision == 0
                        || i64::try_from(source_membership_revision).is_err()
                    {
                        return Err(RuntimeError::InvalidInput);
                    }
                    TeamExpansionWorkspaceTargetV3::ExistingHomeTransfer {
                        profile_id: require_identity(profile_id)?,
                        participation_id: WorkspaceParticipationId::from_uuid(participation_id)
                            .map_err(RuntimeError::persistence)?,
                        source_team_id: require_identity(source_team_id)?,
                        source_membership_revision,
                    }
                }
            };
            Ok(TeamExpansionApprovalSlotPlanV3::new(slot.ordinal, target))
        })
        .collect::<Result<Vec<_>, RuntimeError>>()?;
    Ok((
        TeamExpansionDecisionV3Command::new(base, slot_plan),
        participation,
    ))
}

fn team_expansion_base_command(
    spec: TeamExpansionDecideSpec,
) -> Result<
    (
        TeamExpansionDecisionCommand,
        crate::participation::ParticipationTarget,
    ),
    RuntimeError,
> {
    let workspace_id = WorkspaceId::from_uuid(require_identity(spec.actor.workspace_id)?);
    let (actor, participation) = resolve_actor(spec.actor)?;
    let scope = TeamExpansionScope::new(workspace_id, require_identity(spec.team_id)?);
    let revisions = TeamExpansionRevisions::new(
        spec.expected_team_revision,
        spec.expected_capacity_revision,
        spec.expected_leadership_revision,
    );
    let decision = match spec.decision {
        TeamExpansionDecisionSpec::Approve { approved_count } => {
            TeamExpansionDecision::Approve { approved_count }
        }
        TeamExpansionDecisionSpec::Reject => TeamExpansionDecision::Reject,
    };
    let command = TeamExpansionDecisionCommand::new(
        require_identity(spec.decision_id)?,
        require_identity(spec.request_id)?,
        scope,
        revisions,
        spec.expected_requester_leadership_revision,
        decision,
        actor,
    );
    Ok((command, participation))
}

/// Recover the second, cooldown-anchor transaction for an immutable approved
/// decision. This cannot change the original approved count or decision actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamExpansionFinalizeAnchorSpec {
    /// Current Workspace Orchestrator session used to finalize the anchor.
    pub actor: TeamExpansionActorTarget,
    /// Exact original Team Leader request and Orchestrator decision IDs.
    pub request_id: Uuid,
    pub decision_id: Uuid,
    /// Current Orchestrator leadership revision.
    pub expected_leadership_revision: i64,
}

/// Orchestrator assignment of one approved slot to a selected Ready Session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamExpansionSlotAssignSpec {
    /// Authenticated current Workspace Orchestrator.
    pub actor: TeamExpansionActorTarget,
    /// Exact approval slot and assignment request identity.
    pub slot: TeamExpansionSlotRef,
    pub request_id: Uuid,
    /// Target Team and observed revisions.
    pub team_id: Uuid,
    pub expected_team_revision: u64,
    pub expected_capacity_revision: u64,
    pub expected_leadership_revision: i64,
    /// Target Ready Session selected by the Orchestrator.
    pub target: TeamExpansionTarget,
}

fn require_identity(value: Uuid) -> Result<Uuid, RuntimeError> {
    if value.is_nil() {
        Err(RuntimeError::InvalidInput)
    } else {
        Ok(value)
    }
}

fn resolve_actor(
    target: TeamExpansionActorTarget,
) -> Result<(OperationalActor, crate::participation::ParticipationTarget), RuntimeError> {
    let workspace_id = require_identity(target.workspace_id)?;
    participant_actor(
        WorkspaceId::from_uuid(workspace_id),
        target.activity_id,
        target.session_id,
    )
}

fn resolve_target_actor(
    workspace_id: WorkspaceId,
    target: TeamExpansionTarget,
) -> Result<OperationalActor, RuntimeError> {
    participant_actor(workspace_id, target.activity_id, target.session_id).map(|(actor, _)| actor)
}

fn trusted_server_time() -> Result<TimestampMillis, OperationsError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| OperationsError::Bounds("Team expansion trusted clock"))?;
    let millis = i64::try_from(elapsed.as_millis())
        .map_err(|_| OperationsError::Bounds("Team expansion trusted clock"))?;
    TimestampMillis::new(millis)
        .map_err(|_| OperationsError::Bounds("Team expansion trusted clock"))
}

impl OrchestrationRuntime {
    /// Read the explicit cap. `None` stays visible for legacy Teams without a
    /// configured Orchestrator-selected capacity.
    ///
    /// # Errors
    /// Rejects foreign or closed Sessions, malformed IDs, or catalog failures.
    pub fn team_capacity(
        &self,
        actor: TeamExpansionActorTarget,
        team_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<TeamCapacitySnapshot>, RuntimeError> {
        let workspace_id = WorkspaceId::from_uuid(require_identity(actor.workspace_id)?);
        let (_, participation) = resolve_actor(actor)?;
        self.require_participation(participation, principal)?;
        self.catalog()?
            .team_capacity(workspace_id, require_identity(team_id)?)
            .map_err(RuntimeError::persistence)
    }

    /// Read the exact historical cap mutation for its original Orchestrator or
    /// an authorized current Workspace Orchestrator. This is request-bound
    /// history and is never reconstructed from the current cap head, which may
    /// belong to a later generation.
    ///
    /// # Errors
    /// Rejects a foreign Session, invalid identity, or durable catalog failure.
    pub fn team_capacity_set_receipt(
        &self,
        actor: TeamExpansionActorTarget,
        request_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<TeamCapacityReceipt>, RuntimeError> {
        let workspace_id = WorkspaceId::from_uuid(require_identity(actor.workspace_id)?);
        let (actor, participation) = resolve_actor(actor)?;
        self.require_participation(participation, principal)?;
        self.catalog()?
            .team_capacity_set_receipt(workspace_id, require_identity(request_id)?, actor)
            .map_err(RuntimeError::persistence)
    }

    /// Change the Team cap as the current Workspace Orchestrator.
    /// The owner samples the server clock after acquiring its IMMEDIATE lock.
    ///
    /// # Errors
    /// Rejects wrong role, stale revisions, capacity below occupancy, malformed
    /// input, or durable catalog failure.
    pub fn set_team_capacity(
        &self,
        spec: TeamCapacitySetSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<TeamCapacityReceipt, RuntimeError> {
        let workspace_id = WorkspaceId::from_uuid(require_identity(spec.actor.workspace_id)?);
        let (actor, participation) = resolve_actor(spec.actor)?;
        self.require_participation(participation, principal)?;
        let capacity =
            TeamMemberCapacity::new(spec.max_members).map_err(RuntimeError::persistence)?;
        let scope = TeamExpansionScope::new(workspace_id, require_identity(spec.team_id)?);
        let revisions = TeamExpansionRevisions::new(
            spec.expected_team_revision,
            spec.expected_capacity_revision,
            spec.expected_leadership_revision,
        );
        let command = TeamCapacitySetCommand::new(
            require_identity(spec.request_id)?,
            scope,
            revisions,
            capacity,
            actor,
        );
        self.catalog()?
            .set_team_capacity(&command, trusted_server_time)
            .map_err(RuntimeError::persistence)
    }

    /// Submit one exact expansion request as the current Team Leader.
    ///
    /// # Errors
    /// Rejects a non-Leader, stale revisions, cooldown, or catalog failure.
    pub fn request_team_expansion(
        &self,
        spec: TeamExpansionRequestSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<TeamExpansionRequestReceipt, RuntimeError> {
        let workspace_id = WorkspaceId::from_uuid(require_identity(spec.actor.workspace_id)?);
        let (actor, participation) = resolve_actor(spec.actor)?;
        self.require_participation(participation, principal)?;
        let scope = TeamExpansionScope::new(workspace_id, require_identity(spec.team_id)?);
        let revisions = TeamExpansionRevisions::new(
            spec.expected_team_revision,
            spec.expected_capacity_revision,
            spec.expected_leadership_revision,
        );
        let command = TeamExpansionRequestCommand::new(
            require_identity(spec.request_id)?,
            scope,
            revisions,
            spec.requested_count,
            spec.reason,
            actor,
        );
        self.catalog()?
            .request_team_expansion(&command, trusted_server_time)
            .map_err(RuntimeError::persistence)
    }

    /// Read one exact request as its original requester or authorized
    /// Workspace Orchestrator.
    ///
    /// # Errors
    /// Rejects foreign Sessions, malformed identity, or catalog failure.
    pub fn team_expansion_request(
        &self,
        actor: TeamExpansionActorTarget,
        request_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<TeamExpansionRequestReceipt>, RuntimeError> {
        let workspace_id = WorkspaceId::from_uuid(require_identity(actor.workspace_id)?);
        let (actor, participation) = resolve_actor(actor)?;
        self.require_participation(participation, principal)?;
        self.catalog()?
            .team_expansion_request(workspace_id, require_identity(request_id)?, actor)
            .map_err(RuntimeError::persistence)
    }

    /// Approve or reject the exact request as the Workspace Orchestrator.
    /// Cooldown time is sampled inside Operations after its writer lock.
    ///
    /// # Errors
    /// Rejects wrong role, changed leader/capacity revisions or catalog failure.
    pub fn decide_team_expansion(
        &self,
        spec: TeamExpansionDecideSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<TeamExpansionDecisionReceipt, RuntimeError> {
        let workspace_id = WorkspaceId::from_uuid(require_identity(spec.actor.workspace_id)?);
        let (actor, participation) = resolve_actor(spec.actor)?;
        self.require_participation(participation, principal)?;
        let request_id = require_identity(spec.request_id)?;
        let decision_id = require_identity(spec.decision_id)?;
        let scope = TeamExpansionScope::new(workspace_id, require_identity(spec.team_id)?);
        let revisions = TeamExpansionRevisions::new(
            spec.expected_team_revision,
            spec.expected_capacity_revision,
            spec.expected_leadership_revision,
        );
        let decision = match spec.decision {
            TeamExpansionDecisionSpec::Approve { approved_count } => {
                TeamExpansionDecision::Approve { approved_count }
            }
            TeamExpansionDecisionSpec::Reject => TeamExpansionDecision::Reject,
        };
        let command = TeamExpansionDecisionCommand::new(
            decision_id,
            request_id,
            scope,
            revisions,
            spec.expected_requester_leadership_revision,
            decision,
            actor,
        );
        self.catalog()?
            .decide_team_expansion(&command, trusted_server_time)
            .map_err(RuntimeError::persistence)
    }

    /// Approve or reject one exact Workspace-capacity-backed expansion plan.
    /// Every approved ordinal is bound to an existing Workspace Profile; this
    /// operation creates no Profile or Team membership.
    ///
    /// # Errors
    /// Rejects a non-Orchestrator, stale revisions, incomplete or mismatched
    /// slot plans, over-capacity decisions, duplicate IDs, or catalog failure.
    pub fn decide_team_expansion_v2(
        &self,
        spec: TeamExpansionDecideV2Spec,
        principal: AuthenticatedPrincipal,
    ) -> Result<TeamExpansionDecisionReceipt, RuntimeError> {
        let (command, participation) = team_expansion_v2_command(spec)?;
        self.require_participation(participation, principal)?;
        self.catalog()?
            .decide_team_expansion_v2(&command, trusted_server_time)
            .map_err(RuntimeError::persistence)
    }

    /// Commit one V3 approval with exact source Home evidence for each slot.
    /// A transfer target records its source Team and membership revision; this
    /// approval does not move that membership or authorize assignment/JOIN.
    ///
    /// # Errors
    /// Rejects a non-Orchestrator, rejection decisions, invalid source
    /// revisions, stale authority, mismatched plans, or catalog failure.
    pub fn decide_team_expansion_v3(
        &self,
        spec: TeamExpansionDecideV3Spec,
        principal: AuthenticatedPrincipal,
    ) -> Result<TeamExpansionDecisionReceipt, RuntimeError> {
        let (command, participation) = team_expansion_v3_command(spec)?;
        self.require_participation(participation, principal)?;
        self.catalog()?
            .decide_team_expansion_v3(&command, trusted_server_time)
            .map_err(RuntimeError::persistence)
    }

    /// Recover an exact V2 decision with its full original target plan.
    /// The original decision actor is retained in `intent`; `reader` is
    /// independently authenticated as the original requester or a current
    /// Workspace Orchestrator by the Operations owner.
    ///
    /// # Errors
    /// Rejects different Workspaces, an unauthorized reader, invalid intent,
    /// or corrupt/mismatched stored target plans.
    pub fn team_expansion_decision_v2_readback(
        &self,
        reader: TeamExpansionActorTarget,
        intent: &TeamExpansionDecideV2Spec,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<TeamExpansionDecisionReceipt>, RuntimeError> {
        let workspace_id = WorkspaceId::from_uuid(require_identity(reader.workspace_id)?);
        if workspace_id.into_uuid() != intent.decision.actor.workspace_id {
            return Err(RuntimeError::InvalidInput);
        }
        let (reader, participation) = resolve_actor(reader)?;
        self.require_participation(participation, principal)?;
        let (command, _) = team_expansion_v2_command(intent.clone())?;
        self.catalog()?
            .team_expansion_decision_v2_readback(&command, reader)
            .map_err(RuntimeError::persistence)
    }

    /// Recover one exact V3 decision and its original source Home evidence.
    /// The current reader is authenticated independently from the issuer
    /// preserved in `intent`; Operations authorizes the original issuer or a
    /// current Workspace Orchestrator.
    ///
    /// # Errors
    /// Rejects different Workspaces, an unauthorized reader, invalid
    /// source revisions, or corrupt/mismatched stored target plans.
    pub fn team_expansion_decision_v3_readback(
        &self,
        reader: TeamExpansionActorTarget,
        intent: &TeamExpansionDecideV3Spec,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<TeamExpansionDecisionReceipt>, RuntimeError> {
        let workspace_id = WorkspaceId::from_uuid(require_identity(reader.workspace_id)?);
        if workspace_id.into_uuid() != intent.decision.actor.workspace_id {
            return Err(RuntimeError::InvalidInput);
        }
        let (reader, participation) = resolve_actor(reader)?;
        self.require_participation(participation, principal)?;
        let (command, _) = team_expansion_v3_command(intent.clone())?;
        self.catalog()?
            .team_expansion_decision_v3_readback(&command, reader)
            .map_err(RuntimeError::persistence)
    }

    /// Reconstruct one immutable V3 decision source without requiring the
    /// current reader to possess the original issuer's target plan. Operations
    /// authorizes only the stored original issuer or a current Workspace O.
    ///
    /// # Errors
    /// Rejects a foreign Workspace, malformed IDs, an invalid reader Session,
    /// or inconsistent stored source history.
    pub fn team_expansion_decision_v3_projection(
        &self,
        workspace_id: Uuid,
        decision_id: Uuid,
        reader: TeamExpansionActorTarget,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<TeamExpansionDecisionV3Projection>, RuntimeError> {
        let workspace_id = WorkspaceId::from_uuid(require_identity(workspace_id)?);
        if workspace_id.into_uuid() != reader.workspace_id {
            return Err(RuntimeError::InvalidInput);
        }
        let decision_id = require_identity(decision_id)?;
        let (reader, participation) = resolve_actor(reader)?;
        self.require_participation(participation, principal)?;
        self.catalog()?
            .team_expansion_decision_v3_projection(workspace_id, decision_id, reader)
            .map_err(RuntimeError::persistence)
    }

    /// Recover one exact pre-hold legacy approval slot as a current Workspace
    /// reservation. The recovery ID is new; the original decision ID/ordinal
    /// remains unchanged.
    ///
    /// # Errors
    /// Rejects a non-Orchestrator, nonlegacy/used slot, stale revisions,
    /// ineligible or already-homed target, or unconfirmed commit.
    pub fn recover_legacy_team_expansion_slot(
        &self,
        spec: RecoverLegacyTeamExpansionSlotSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<OperationsLegacySlotRecoveryReceipt, RuntimeError> {
        let workspace_id = WorkspaceId::from_uuid(require_identity(spec.actor.workspace_id)?);
        if workspace_id.into_uuid() != spec.intent.workspace_id {
            return Err(RuntimeError::InvalidInput);
        }
        let (actor, participation) = resolve_actor(spec.actor)?;
        self.require_participation(participation, principal)?;
        if spec.expected_leadership_revision <= 0 {
            return Err(RuntimeError::InvalidInput);
        }
        let intent = legacy_slot_recovery_intent(spec.intent)?;
        let command =
            LegacySlotRecoveryCommand::new(intent, spec.expected_leadership_revision, actor);
        self.catalog()?
            .recover_legacy_team_expansion_slot(&command, trusted_server_time)
            .map_err(RuntimeError::persistence)
    }

    /// Read the exact source approval and target intent independently of the
    /// current reader Session. Operations authorizes the original issuer or a
    /// current Workspace Orchestrator; this call cannot perform a recovery.
    ///
    /// # Errors
    /// Rejects a foreign Workspace, inaccessible reader, or corrupt recovery
    /// history.
    pub fn legacy_team_expansion_slot_recovery(
        &self,
        reader: TeamExpansionActorTarget,
        intent: LegacyTeamExpansionSlotRecoveryIntentSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<OperationsLegacySlotRecoveryReceipt>, RuntimeError> {
        let workspace_id = WorkspaceId::from_uuid(require_identity(reader.workspace_id)?);
        if workspace_id.into_uuid() != intent.workspace_id {
            return Err(RuntimeError::InvalidInput);
        }
        let (reader, participation) = resolve_actor(reader)?;
        self.require_participation(participation, principal)?;
        let intent = legacy_slot_recovery_intent(intent)?;
        self.catalog()?
            .legacy_team_expansion_slot_recovery(&intent, reader)
            .map_err(RuntimeError::persistence)
    }

    /// Finalize or exactly replay the durable cooldown anchor for a previously
    /// approved decision. A new Orchestrator may recover the same immutable
    /// approval; the approved count and original decision remain unchanged.
    ///
    /// # Errors
    /// Rejects an unauthorized/currently stale Orchestrator, invalid approval,
    /// malformed identity, or an unconfirmed anchor write.
    pub fn finalize_team_expansion_anchor(
        &self,
        spec: TeamExpansionFinalizeAnchorSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<TeamExpansionDecisionReceipt, RuntimeError> {
        let workspace_id = WorkspaceId::from_uuid(require_identity(spec.actor.workspace_id)?);
        let (actor, participation) = resolve_actor(spec.actor)?;
        self.require_participation(participation, principal)?;
        let mut catalog = self.catalog()?;
        catalog
            .finalize_team_expansion_cooldown(
                workspace_id,
                require_identity(spec.request_id)?,
                require_identity(spec.decision_id)?,
                spec.expected_leadership_revision,
                actor,
                trusted_server_time,
            )
            .map_err(RuntimeError::persistence)
    }

    /// Read one exact decision as its requester or current Workspace Orchestrator.
    ///
    /// # Errors
    /// Rejects foreign Sessions, malformed identity, or catalog failure.
    pub fn team_expansion_decision(
        &self,
        actor: TeamExpansionActorTarget,
        decision_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<TeamExpansionDecisionReceipt>, RuntimeError> {
        let workspace_id = WorkspaceId::from_uuid(require_identity(actor.workspace_id)?);
        let (actor, participation) = resolve_actor(actor)?;
        self.require_participation(participation, principal)?;
        self.catalog()?
            .team_expansion_decision(workspace_id, require_identity(decision_id)?, actor)
            .map_err(RuntimeError::persistence)
    }

    /// Bind one exact approved slot to a selected Ready Profile Session.
    ///
    /// # Errors
    /// Rejects non-Orchestrator, foreign/unready target, stale revisions,
    /// malformed IDs, or catalog failure.
    pub fn assign_team_expansion_slot(
        &self,
        spec: TeamExpansionSlotAssignSpec,
        principal: AuthenticatedPrincipal,
    ) -> Result<TeamExpansionSlotAssignmentReceipt, RuntimeError> {
        let workspace_id = WorkspaceId::from_uuid(require_identity(spec.actor.workspace_id)?);
        let (actor, participation) = resolve_actor(spec.actor)?;
        self.require_participation(participation, principal)?;
        let target = resolve_target_actor(workspace_id, spec.target)?;
        let scope = TeamExpansionScope::new(workspace_id, require_identity(spec.team_id)?);
        let revisions = TeamExpansionRevisions::new(
            spec.expected_team_revision,
            spec.expected_capacity_revision,
            spec.expected_leadership_revision,
        );
        let command = TeamExpansionSlotAssignmentCommand::new(
            require_identity(spec.request_id)?,
            spec.slot,
            scope,
            revisions,
            target,
            actor,
        );
        self.catalog()?
            .assign_team_expansion_slot(&command, trusted_server_time)
            .map_err(RuntimeError::persistence)
    }

    /// Read one exact selected slot as its target Profile or current
    /// Workspace Orchestrator.
    ///
    /// # Errors
    /// Rejects foreign Sessions, malformed IDs, or catalog failure.
    pub fn team_expansion_slot_assignment(
        &self,
        actor: TeamExpansionActorTarget,
        slot: TeamExpansionSlotRef,
        principal: AuthenticatedPrincipal,
    ) -> Result<Option<TeamExpansionSlotAssignmentReceipt>, RuntimeError> {
        let workspace_id = WorkspaceId::from_uuid(require_identity(actor.workspace_id)?);
        let (actor, participation) = resolve_actor(actor)?;
        self.require_participation(participation, principal)?;
        self.catalog()?
            .team_expansion_slot_assignment(workspace_id, slot, actor)
            .map_err(RuntimeError::persistence)
    }
}
