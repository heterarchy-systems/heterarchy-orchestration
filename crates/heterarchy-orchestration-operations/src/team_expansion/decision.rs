//! Durable Team expansion decisions and post-commit cooldown anchors.

use super::{
    ActivityId, AgentInstanceId, AgentSessionId, Connection, GLOBAL_TEAM_MEMBER_CAP,
    LeadershipScope, OperationalActor, OperationsError, OptionalExtension,
    TEAM_EXPANSION_COOLDOWN_MS, TeamExpansionActor, TeamExpansionDecision,
    TeamExpansionDecisionCommand, TeamExpansionDecisionReceipt, TeamExpansionDecisionState,
    TeamExpansionDecisionV2Command, TeamExpansionDecisionV3Command,
    TeamExpansionDecisionV3Projection, TimestampMillis, Transaction, TransactionBehavior, Uuid,
    Value, WorkspaceId, WorkspaceParticipationId, actor_same,
    current_leadership_authority_in_transaction, ensure_expansion_request_unlocked,
    existing_home_transfer, full_team_occupancy, load_current_capacity, params,
    stored_profile_matches_reader, team_revision_and_active, validate_actor, validate_non_nil,
    wire,
};
use crate::{team_expansion::workspace_hold, teams};

const DECISION_SELECT: &str = "SELECT d.decision_id,d.request_id,d.workspace_id,d.team_id,d.state,d.approved_count,d.expected_team_revision,d.capacity_revision,d.leadership_revision,d.actor_activity_id,d.actor_agent_id,d.actor_session_id,d.actor_participation_id,d.actor_profile_id,d.actor_participant_id,d.actor_display_name,d.decided_at_ms,a.anchor_at_ms,a.cooldown_until_ms,a.finalized_by_activity_id,a.finalized_by_agent_id,a.finalized_by_session_id,a.finalized_by_participation_id,a.finalized_by_profile_id,a.finalized_by_participant_id,a.finalized_by_display_name,a.finalized_by_leadership_revision FROM ops_team_expansion_decisions AS d LEFT JOIN ops_team_expansion_cooldown_anchors AS a ON a.workspace_id=d.workspace_id AND a.team_id=d.team_id AND a.request_id=d.request_id AND a.decision_id=d.decision_id";

pub(super) fn decide_team_expansion(
    connection: &mut Connection,
    command: &TeamExpansionDecisionCommand,
    trusted_now: impl FnMut() -> Result<TimestampMillis, OperationsError>,
) -> Result<TeamExpansionDecisionReceipt, OperationsError> {
    decide_team_expansion_inner(connection, command, DecisionSource::Legacy, trusted_now)
}

pub(super) fn decide_team_expansion_v2(
    connection: &mut Connection,
    command: &TeamExpansionDecisionV2Command,
    trusted_now: impl FnMut() -> Result<TimestampMillis, OperationsError>,
) -> Result<TeamExpansionDecisionReceipt, OperationsError> {
    let plans = workspace_hold::plans_from_v2(&command.slot_plan);
    decide_team_expansion_inner(
        connection,
        &command.decision,
        DecisionSource::V2(&plans),
        trusted_now,
    )
}

pub(super) fn decide_team_expansion_v3(
    connection: &mut Connection,
    command: &TeamExpansionDecisionV3Command,
    trusted_now: impl FnMut() -> Result<TimestampMillis, OperationsError>,
) -> Result<TeamExpansionDecisionReceipt, OperationsError> {
    if !matches!(
        command.decision.decision,
        TeamExpansionDecision::Approve { .. }
    ) {
        return Err(OperationsError::Conflict(
            "Team expansion V3 is an approval source",
        ));
    }
    let plans = workspace_hold::plans_from_v3(&command.slot_plan);
    let transfers = existing_home_transfer::sources_from_v3(&command.slot_plan);
    decide_team_expansion_inner(
        connection,
        &command.decision,
        DecisionSource::V3 {
            plans: &plans,
            transfers: &transfers,
        },
        trusted_now,
    )
}

#[derive(Clone, Copy)]
enum DecisionSource<'a> {
    Legacy,
    V2(&'a [workspace_hold::WorkspaceHoldPlanSlot]),
    V3 {
        plans: &'a [workspace_hold::WorkspaceHoldPlanSlot],
        transfers: &'a [existing_home_transfer::ExistingHomeTransferSource],
    },
}

fn decide_team_expansion_inner(
    connection: &mut Connection,
    command: &TeamExpansionDecisionCommand,
    source: DecisionSource<'_>,
    mut trusted_now: impl FnMut() -> Result<TimestampMillis, OperationsError>,
) -> Result<TeamExpansionDecisionReceipt, OperationsError> {
    validate_decision_command(command)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    teams::ensure_verified_workspace(&transaction, command.workspace_id)?;

    let (actor, _) = validate_actor(&transaction, command.workspace_id, command.actor)?;
    if let Some(existing) = load_decision_by_id(
        &transaction,
        command.workspace_id,
        command.decision_id,
        false,
    )? {
        if decision_matches_command(&transaction, &existing, command, &actor)?
            && decision_source_matches(&transaction, &existing, command, source)?
        {
            transaction.commit()?;
            return Ok(with_decision_replay(existing));
        }
        return Err(OperationsError::Conflict("Team expansion decision replay"));
    }
    if decision_id_exists(&transaction, command.decision_id)? {
        return Err(OperationsError::Conflict(
            "Team expansion decision Workspace",
        ));
    }

    if matches!(command.decision, TeamExpansionDecision::Approve { .. })
        && matches!(source, DecisionSource::Legacy)
    {
        return Err(OperationsError::Conflict(
            "Team expansion V2 approval slot plan required",
        ));
    }

    let (state, approved_count) = prepare_new_decision(&transaction, command, &actor)?;
    let decided_at = trusted_now()?;
    ensure_source_time_not_regressed(
        &transaction,
        command.workspace_id,
        command.team_id,
        decided_at,
    )?;
    if state == "APPROVED" {
        ensure_expansion_request_unlocked(
            &transaction,
            command.workspace_id,
            command.team_id,
            decided_at,
        )?;
    }
    insert_decision(
        &transaction,
        command,
        &actor,
        state,
        approved_count,
        decided_at,
    )?;
    if state == "APPROVED" {
        insert_approved_slots(&transaction, command, approved_count)?;
        reserve_decision_source(&transaction, command, approved_count, source, decided_at)?;
    } else if !source_is_empty(source) {
        return Err(OperationsError::Conflict(
            "Rejected Team expansion cannot reserve Workspace holds",
        ));
    }
    let fresh = load_decision_by_id(
        &transaction,
        command.workspace_id,
        command.decision_id,
        false,
    )?
    .ok_or(OperationsError::Corrupt("Team expansion decision readback"))?;
    if !decision_matches_command(&transaction, &fresh, command, &actor)?
        || !decision_source_matches(&transaction, &fresh, command, source)?
    {
        return Err(OperationsError::Corrupt("Team expansion decision readback"));
    }
    let commit_result = transaction.commit();
    finish_decision_commit(connection, command, source, &actor, fresh, commit_result)
}

fn reserve_decision_source(
    transaction: &Transaction<'_>,
    command: &TeamExpansionDecisionCommand,
    approved_count: u8,
    source: DecisionSource<'_>,
    decided_at: TimestampMillis,
) -> Result<(), OperationsError> {
    let scope = super::TeamExpansionScope::new(command.workspace_id, command.team_id);
    let reservation = workspace_hold::WorkspaceHoldReservation::new(
        scope,
        command.decision_id,
        command.request_id,
        approved_count,
        decided_at,
    );
    match source {
        DecisionSource::Legacy => Err(OperationsError::Conflict(
            "Team expansion V2 approval slot plan required",
        )),
        DecisionSource::V2(plans) => {
            workspace_hold::reserve_approved_workspace_holds(transaction, reservation, plans)
                .map(|_| ())
        }
        DecisionSource::V3 { plans, transfers } => {
            workspace_hold::reserve_approved_workspace_holds_v3(
                transaction,
                reservation,
                plans,
                transfers,
            )
            .map(|_| ())
        }
    }
}

const fn source_is_empty(source: DecisionSource<'_>) -> bool {
    match source {
        DecisionSource::Legacy => true,
        DecisionSource::V2(plans) => plans.is_empty(),
        DecisionSource::V3 { .. } => false,
    }
}

fn finish_decision_commit(
    connection: &Connection,
    command: &TeamExpansionDecisionCommand,
    decision_source: DecisionSource<'_>,
    actor: &TeamExpansionActor,
    fresh: TeamExpansionDecisionReceipt,
    commit_result: Result<(), rusqlite::Error>,
) -> Result<TeamExpansionDecisionReceipt, OperationsError> {
    match commit_result {
        Ok(()) => Ok(fresh),
        Err(source) => match classify_commit_readback(
            load_decision_by_id(connection, command.workspace_id, command.decision_id, true),
            |existing| {
                decision_matches_command(connection, existing, command, actor).and_then(|base| {
                    if base {
                        decision_source_matches(connection, existing, command, decision_source)
                    } else {
                        Ok(false)
                    }
                })
            },
        ) {
            CommitReadback::Exact(existing) => Ok(existing),
            CommitReadback::Mismatch => Err(OperationsError::Conflict(
                "Team expansion decision replay after unconfirmed commit",
            )),
            CommitReadback::Unconfirmed => Err(OperationsError::OperationCommitUnconfirmed {
                workspace_id: command.workspace_id,
                request_id: command.decision_id,
                source,
            }),
        },
    }
}

enum CommitReadback<T> {
    Exact(T),
    Mismatch,
    Unconfirmed,
}

fn classify_commit_readback<T>(
    loaded: Result<Option<T>, OperationsError>,
    exact: impl FnOnce(&T) -> Result<bool, OperationsError>,
) -> CommitReadback<T> {
    match loaded {
        Ok(Some(value)) => match exact(&value) {
            Ok(true) => CommitReadback::Exact(value),
            Ok(false) => CommitReadback::Mismatch,
            Err(_) => CommitReadback::Unconfirmed,
        },
        Ok(None) | Err(_) => CommitReadback::Unconfirmed,
    }
}

#[cfg(test)]
mod commit_readback_tests {
    use super::{CommitReadback, classify_commit_readback};
    use crate::OperationsError;

    #[test]
    fn matcher_error_keeps_unknown_commit_unconfirmed() {
        let outcome = classify_commit_readback(Ok(Some(7_u8)), |_| {
            Err(OperationsError::Corrupt("synthetic readback failure"))
        });
        assert!(matches!(outcome, CommitReadback::Unconfirmed));
    }
}

pub(super) fn finalize_team_expansion_cooldown(
    connection: &mut Connection,
    workspace_id: WorkspaceId,
    request_id: Uuid,
    decision_id: Uuid,
    expected_leadership_revision: i64,
    actor_input: OperationalActor,
    trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
) -> Result<TeamExpansionDecisionReceipt, OperationsError> {
    validate_non_nil(request_id, "Team expansion request identity")?;
    validate_non_nil(decision_id, "Team expansion decision identity")?;
    if expected_leadership_revision < 1 {
        return Err(OperationsError::Bounds(
            "Team expansion finalizer leadership revision",
        ));
    }
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    teams::ensure_verified_workspace(&transaction, workspace_id)?;
    let (finalizer, _) = validate_actor(&transaction, workspace_id, actor_input)?;
    let existing = load_decision_by_id(&transaction, workspace_id, decision_id, false)?
        .ok_or(OperationsError::Conflict("Team expansion decision"))?;
    if existing.request_id != request_id {
        return Err(OperationsError::Conflict(
            "Team expansion cooldown decision identity",
        ));
    }
    if existing.state == TeamExpansionDecisionState::Rejected {
        return Err(OperationsError::Conflict(
            "Rejected Team expansion has no cooldown anchor",
        ));
    }

    let authority = current_leadership_authority_in_transaction(
        &transaction,
        LeadershipScope::workspace(workspace_id),
    )?
    .ok_or(OperationsError::Conflict(
        "Team expansion cooldown requires current Workspace Orchestrator",
    ))?;
    if authority.participant_id() != finalizer.participant_id
        || authority.leadership_revision() != expected_leadership_revision
    {
        return Err(OperationsError::Conflict(
            "Team expansion cooldown authority changed",
        ));
    }
    if existing.state == TeamExpansionDecisionState::Approved {
        // Finalization is immutable. A later current Orchestrator can recover
        // the same decision without impersonating or replacing the original
        // finalizer attribution.
        transaction.commit()?;
        return Ok(with_decision_replay(existing));
    }

    let anchor_context = CooldownAnchorContext {
        existing: &existing,
        request_id,
        decision_id,
        workspace_id,
        expected_leadership_revision,
        finalizer: &finalizer,
    };
    let (anchor_at, fresh) = write_cooldown_anchor(&transaction, &anchor_context, trusted_now)?;
    let commit_result = transaction.commit();
    match commit_result {
        Ok(()) => Ok(fresh),
        Err(source) => match load_decision_by_id(connection, workspace_id, decision_id, true) {
            Ok(Some(existing))
                if existing.state == TeamExpansionDecisionState::Approved
                    && existing.cooldown_started_at == Some(anchor_at)
                    && existing
                        .finalized_by()
                        .is_some_and(|recorded| actor_same(recorded, &finalizer))
                    && existing.finalized_by_leadership_revision()
                        == Some(expected_leadership_revision) =>
            {
                Ok(existing)
            }
            Ok(Some(_)) => Err(OperationsError::Conflict(
                "Team expansion cooldown replay after unconfirmed commit",
            )),
            Ok(None) | Err(_) => Err(OperationsError::OperationCommitUnconfirmed {
                workspace_id,
                request_id: decision_id,
                source,
            }),
        },
    }
}

struct CooldownAnchorContext<'a> {
    existing: &'a TeamExpansionDecisionReceipt,
    request_id: Uuid,
    decision_id: Uuid,
    workspace_id: WorkspaceId,
    expected_leadership_revision: i64,
    finalizer: &'a TeamExpansionActor,
}

fn write_cooldown_anchor(
    transaction: &Transaction<'_>,
    context: &CooldownAnchorContext<'_>,
    trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
) -> Result<(TimestampMillis, TeamExpansionDecisionReceipt), OperationsError> {
    let existing = context.existing;
    let request_id = context.request_id;
    let decision_id = context.decision_id;
    let workspace_id = context.workspace_id;
    let expected_leadership_revision = context.expected_leadership_revision;
    let finalizer = context.finalizer;
    let anchor_at = trusted_now()?;
    ensure_source_time_not_regressed(transaction, workspace_id, existing.team_id, anchor_at)?;
    if anchor_at < existing.recorded_at {
        return Err(OperationsError::Conflict(
            "Team expansion cooldown clock regressed",
        ));
    }
    let cooldown_until_ms = anchor_at
        .get()
        .checked_add(TEAM_EXPANSION_COOLDOWN_MS)
        .ok_or(OperationsError::Bounds("Team expansion cooldown timestamp"))?;
    TimestampMillis::new(cooldown_until_ms)
        .map_err(|_| OperationsError::Bounds("Team expansion cooldown timestamp"))?;
    transaction.execute(
        "INSERT INTO ops_team_expansion_cooldown_anchors(request_id,decision_id,workspace_id,team_id,anchor_at_ms,cooldown_until_ms,finalized_by_participation_id,finalized_by_profile_id,finalized_by_participant_id,finalized_by_activity_id,finalized_by_agent_id,finalized_by_session_id,finalized_by_display_name,finalized_by_leadership_revision) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
        params![
            request_id.as_bytes(), decision_id.as_bytes(), workspace_id.into_uuid().as_bytes(),
            existing.team_id.as_bytes(), anchor_at.get(), cooldown_until_ms,
            finalizer.participation_id.into_uuid().as_bytes(), finalizer.profile_id.as_bytes(),
            finalizer.participant_id.as_bytes(), finalizer.activity_id.into_uuid().as_bytes(),
            finalizer.agent_id.into_uuid().as_bytes(), finalizer.session_id.into_uuid().as_bytes(),
            finalizer.display_name, expected_leadership_revision,
        ],
    )?;
    let fresh = load_decision_by_id(transaction, workspace_id, decision_id, false)?
        .ok_or(OperationsError::Corrupt("Team expansion cooldown readback"))?;
    if fresh.state != TeamExpansionDecisionState::Approved
        || fresh.cooldown_started_at != Some(anchor_at)
        || fresh.cooldown_until.map(TimestampMillis::get) != Some(cooldown_until_ms)
        || fresh
            .finalized_by()
            .is_none_or(|recorded| !actor_same(recorded, finalizer))
        || fresh.finalized_by_leadership_revision() != Some(expected_leadership_revision)
    {
        return Err(OperationsError::Corrupt("Team expansion cooldown readback"));
    }
    Ok((anchor_at, fresh))
}

/// Read a decision for the current Workspace Orchestrator or its original
/// requester. Missing, foreign-Workspace, and unrelated decisions are all
/// projected as `None`; durable corruption is reported only after the reader
/// has been authorized for the full receipt.
pub(super) fn read_team_expansion_decision(
    connection: &Connection,
    workspace_id: WorkspaceId,
    decision_id: Uuid,
    actor_input: OperationalActor,
) -> Result<Option<TeamExpansionDecisionReceipt>, OperationsError> {
    validate_non_nil(decision_id, "Team expansion decision identity")?;
    let transaction = connection.unchecked_transaction()?;
    teams::ensure_verified_workspace(&transaction, workspace_id)?;
    let (reader, _) = validate_actor(&transaction, workspace_id, actor_input)?;
    let is_current_orchestrator = current_leadership_authority_in_transaction(
        &transaction,
        LeadershipScope::workspace(workspace_id),
    )?
    .is_some_and(|authority| authority.participant_id() == reader.participant_id);
    if is_current_orchestrator {
        let receipt = load_decision_by_id(&transaction, workspace_id, decision_id, false)?;
        transaction.commit()?;
        return Ok(receipt);
    }
    let Some((participation, profile)) =
        requester_identity_for_decision(&transaction, workspace_id, decision_id)?
    else {
        transaction.commit()?;
        return Ok(None);
    };
    if !stored_profile_matches_reader(participation, profile, &reader) {
        transaction.commit()?;
        return Ok(None);
    }
    let receipt = load_decision_by_id(&transaction, workspace_id, decision_id, false)?.ok_or(
        OperationsError::Corrupt("Team expansion authorized decision readback"),
    )?;
    transaction.commit()?;
    Ok(Some(receipt))
}

pub(super) fn read_team_expansion_decision_v2(
    connection: &Connection,
    intent: &TeamExpansionDecisionV2Command,
    reader: OperationalActor,
) -> Result<Option<TeamExpansionDecisionReceipt>, OperationsError> {
    let command = &intent.decision;
    validate_non_nil(command.decision_id, "Team expansion decision identity")?;
    let transaction = connection.unchecked_transaction()?;
    teams::ensure_verified_workspace(&transaction, command.workspace_id)?;
    let (reader, _) = validate_actor(&transaction, command.workspace_id, reader)?;
    let is_current_orchestrator = current_leadership_authority_in_transaction(
        &transaction,
        LeadershipScope::workspace(command.workspace_id),
    )?
    .is_some_and(|authority| authority.participant_id() == reader.participant_id);
    let original_actor = decision_actor_identity_for_decision(
        &transaction,
        command.workspace_id,
        command.decision_id,
    )?;
    let is_original_actor = original_actor.is_some_and(|(participation, profile)| {
        stored_profile_matches_reader(participation, profile, &reader)
    });
    if !is_current_orchestrator && !is_original_actor {
        transaction.commit()?;
        return Ok(None);
    }
    let Some(receipt) = load_decision_by_id(
        &transaction,
        command.workspace_id,
        command.decision_id,
        false,
    )?
    else {
        transaction.commit()?;
        return Ok(None);
    };
    let plans = workspace_hold::plans_from_v2(&intent.slot_plan);
    if !decision_matches_intent_readback(&transaction, &receipt, command)?
        || !decision_source_matches(&transaction, &receipt, command, DecisionSource::V2(&plans))?
    {
        return Err(OperationsError::Conflict(
            "Team expansion V2 decision readback mismatch",
        ));
    }
    transaction.commit()?;
    Ok(Some(receipt))
}

pub(super) fn read_team_expansion_decision_v3(
    connection: &Connection,
    intent: &TeamExpansionDecisionV3Command,
    reader: OperationalActor,
) -> Result<Option<TeamExpansionDecisionReceipt>, OperationsError> {
    let command = &intent.decision;
    validate_non_nil(command.decision_id, "Team expansion decision identity")?;
    let transaction = connection.unchecked_transaction()?;
    teams::ensure_verified_workspace(&transaction, command.workspace_id)?;
    let (reader, _) = validate_actor(&transaction, command.workspace_id, reader)?;
    let is_current_orchestrator = current_leadership_authority_in_transaction(
        &transaction,
        LeadershipScope::workspace(command.workspace_id),
    )?
    .is_some_and(|authority| authority.participant_id() == reader.participant_id);
    let original_actor = decision_actor_identity_for_decision(
        &transaction,
        command.workspace_id,
        command.decision_id,
    )?;
    let is_original_actor = original_actor.is_some_and(|(participation, profile)| {
        stored_profile_matches_reader(participation, profile, &reader)
    });
    if !is_current_orchestrator && !is_original_actor {
        transaction.commit()?;
        return Ok(None);
    }
    let Some(receipt) = load_decision_by_id(
        &transaction,
        command.workspace_id,
        command.decision_id,
        false,
    )?
    else {
        transaction.commit()?;
        return Ok(None);
    };
    let plans = workspace_hold::plans_from_v3(&intent.slot_plan);
    let transfers = existing_home_transfer::sources_from_v3(&intent.slot_plan);
    if !decision_matches_intent_readback(&transaction, &receipt, command)?
        || !decision_source_matches(
            &transaction,
            &receipt,
            command,
            DecisionSource::V3 {
                plans: &plans,
                transfers: &transfers,
            },
        )?
    {
        return Err(OperationsError::Conflict(
            "Team expansion V3 decision readback mismatch",
        ));
    }
    transaction.commit()?;
    Ok(Some(receipt))
}

pub(super) fn read_team_expansion_decision_v3_projection(
    connection: &Connection,
    workspace_id: WorkspaceId,
    decision_id: Uuid,
    reader: OperationalActor,
) -> Result<Option<TeamExpansionDecisionV3Projection>, OperationsError> {
    validate_non_nil(decision_id, "Team expansion decision identity")?;
    let transaction = connection.unchecked_transaction()?;
    teams::ensure_verified_workspace(&transaction, workspace_id)?;
    let (reader, _) = validate_actor(&transaction, workspace_id, reader)?;
    let current_orchestrator = current_leadership_authority_in_transaction(
        &transaction,
        LeadershipScope::workspace(workspace_id),
    )?
    .is_some_and(|authority| authority.participant_id() == reader.participant_id);
    let Some((issuer_participation, issuer_profile)) =
        v3_decision_actor_identity(&transaction, workspace_id, decision_id)?
    else {
        transaction.commit()?;
        return Ok(None);
    };
    let original_issuer =
        stored_profile_matches_reader(issuer_participation, issuer_profile, &reader);
    if !current_orchestrator && !original_issuer {
        transaction.commit()?;
        return Ok(None);
    }

    let decision = load_decision_by_id(&transaction, workspace_id, decision_id, false)?.ok_or(
        OperationsError::Corrupt("Team expansion V3 projection decision missing"),
    )?;
    let slot_plan = existing_home_transfer::load_original_plan(&transaction, &decision)?;
    let hold_plans = workspace_hold::plans_from_v3(&slot_plan);
    let transfers = existing_home_transfer::sources_from_v3(&slot_plan);
    if !workspace_hold::workspace_hold_manifest_matches_intent(
        &transaction,
        decision.workspace_id,
        decision.team_id,
        decision.decision_id,
        decision.request_id,
        decision.approved_count,
        &hold_plans,
    )? || !matches!(
        workspace_hold::validate_workspace_hold_manifest(
            &transaction,
            decision.workspace_id,
            decision.team_id,
            decision.decision_id,
            decision.request_id,
            decision.approved_count,
            &hold_plans,
        )?,
        workspace_hold::WorkspaceHoldDisposition::Reserved
    ) || !existing_home_transfer::source_matches_receipt(&transaction, &decision, &transfers)?
    {
        return Err(OperationsError::Corrupt(
            "Team expansion V3 projection source",
        ));
    }
    transaction.commit()?;
    Ok(Some(TeamExpansionDecisionV3Projection {
        decision,
        slot_plan,
    }))
}

fn decision_matches_intent_readback(
    connection: &Connection,
    receipt: &TeamExpansionDecisionReceipt,
    command: &TeamExpansionDecisionCommand,
) -> Result<bool, OperationsError> {
    let actor_matches = match command.actor {
        OperationalActor::Participant {
            activity_id,
            agent_id,
            session_id,
        } => {
            receipt.actor.activity_id == activity_id
                && receipt.actor.agent_id == agent_id
                && receipt.actor.session_id == session_id
        }
        OperationalActor::LocalOperator => false,
    };
    let request = load_request_for_decision(connection, command.workspace_id, command.request_id)?;
    let decision_matches = match command.decision {
        TeamExpansionDecision::Approve { approved_count } => {
            receipt.state != TeamExpansionDecisionState::Rejected
                && receipt.approved_count == approved_count
        }
        TeamExpansionDecision::Reject => {
            receipt.state == TeamExpansionDecisionState::Rejected && receipt.approved_count == 0
        }
    };
    Ok(receipt.decision_id == command.decision_id
        && receipt.request_id == command.request_id
        && receipt.workspace_id == command.workspace_id
        && receipt.team_id == command.team_id
        && receipt.expected_team_revision == command.expected_team_revision
        && receipt.capacity_revision == command.expected_capacity_revision
        && receipt.leadership_revision == command.expected_leadership_revision
        && actor_matches
        && decision_matches
        && request.is_some_and(|request| {
            request.requester_leadership_revision == command.expected_requester_leadership_revision
        }))
}

fn validate_decision_command(
    command: &TeamExpansionDecisionCommand,
) -> Result<(), OperationsError> {
    validate_non_nil(command.decision_id, "Team expansion decision identity")?;
    validate_non_nil(command.request_id, "Team expansion request identity")?;
    validate_non_nil(command.team_id, "Team expansion Team identity")?;
    if command.expected_team_revision == 0
        || command.expected_capacity_revision == 0
        || command.expected_leadership_revision < 1
        || command.expected_requester_leadership_revision < 1
        || i64::try_from(command.expected_team_revision).is_err()
        || i64::try_from(command.expected_capacity_revision).is_err()
    {
        return Err(OperationsError::Bounds("Team expansion decision revision"));
    }
    if let TeamExpansionDecision::Approve { approved_count } = command.decision
        && (approved_count == 0 || approved_count > GLOBAL_TEAM_MEMBER_CAP)
    {
        return Err(OperationsError::Bounds("Team expansion approved count"));
    }
    Ok(())
}

fn prepare_new_decision(
    transaction: &Transaction<'_>,
    command: &TeamExpansionDecisionCommand,
    actor: &TeamExpansionActor,
) -> Result<(&'static str, u8), OperationsError> {
    let request = load_request_for_decision(transaction, command.workspace_id, command.request_id)?
        .ok_or(OperationsError::Conflict("Team expansion request"))?;
    if request.team_id != command.team_id
        || request.requester_leadership_revision != command.expected_requester_leadership_revision
    {
        return Err(OperationsError::Conflict("Team expansion decision request"));
    }
    if request_has_decision(transaction, command.workspace_id, command.request_id)? {
        return Err(OperationsError::Conflict(
            "Team expansion request already decided",
        ));
    }
    let authority = current_leadership_authority_in_transaction(
        transaction,
        LeadershipScope::workspace(command.workspace_id),
    )?
    .ok_or(OperationsError::Conflict(
        "Team expansion decision requires current Workspace Orchestrator",
    ))?;
    if authority.participant_id() != actor.participant_id
        || authority.leadership_revision() != command.expected_leadership_revision
    {
        return Err(OperationsError::Conflict(
            "Team expansion decision authority changed",
        ));
    }
    let (team_revision, active) =
        team_revision_and_active(transaction, command.workspace_id, command.team_id)?;
    if !active || team_revision != command.expected_team_revision {
        return Err(OperationsError::Conflict(
            "Team expansion decision Team revision",
        ));
    }
    let capacity = load_current_capacity(transaction, command.workspace_id, command.team_id)?
        .ok_or(OperationsError::Conflict("team capacity is not configured"))?;
    if capacity.revision != command.expected_capacity_revision {
        return Err(OperationsError::Conflict(
            "Team expansion decision capacity revision",
        ));
    }
    let TeamExpansionDecision::Approve { approved_count } = command.decision else {
        return Ok(("REJECTED", 0));
    };
    if approved_count == 0 || approved_count > request.requested_count {
        return Err(OperationsError::Bounds("Team expansion approved count"));
    }
    let occupied = full_team_occupancy(transaction, command.workspace_id, command.team_id)?;
    let after_approval = occupied
        .checked_add(usize::from(approved_count))
        .ok_or(OperationsError::Bounds("Team expansion capacity"))?;
    if after_approval > usize::from(capacity.max_members().get()) {
        return Err(OperationsError::Bounds("Team expansion capacity"));
    }
    Ok(("APPROVED", approved_count))
}

fn insert_decision(
    transaction: &Transaction<'_>,
    command: &TeamExpansionDecisionCommand,
    actor: &TeamExpansionActor,
    state: &'static str,
    approved_count: u8,
    decided_at: TimestampMillis,
) -> Result<(), OperationsError> {
    transaction.execute(
        "INSERT INTO ops_team_expansion_decisions(decision_id,request_id,workspace_id,team_id,state,approved_count,expected_team_revision,capacity_revision,leadership_revision,actor_activity_id,actor_agent_id,actor_session_id,actor_participation_id,actor_profile_id,actor_participant_id,actor_display_name,decided_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
        params![
            command.decision_id.as_bytes(),
            command.request_id.as_bytes(),
            command.workspace_id.into_uuid().as_bytes(),
            command.team_id.as_bytes(),
            state,
            i64::from(approved_count),
            i64::try_from(command.expected_team_revision)
                .map_err(|_| OperationsError::Bounds("Team expansion Team revision"))?,
            i64::try_from(command.expected_capacity_revision)
                .map_err(|_| OperationsError::Bounds("Team expansion capacity revision"))?,
            command.expected_leadership_revision,
            actor.activity_id.into_uuid().as_bytes(),
            actor.agent_id.into_uuid().as_bytes(),
            actor.session_id.into_uuid().as_bytes(),
            actor.participation_id.into_uuid().as_bytes(),
            actor.profile_id.as_bytes(),
            actor.participant_id.as_bytes(),
            actor.display_name,
            decided_at.get(),
        ],
    )?;
    Ok(())
}

fn insert_approved_slots(
    transaction: &Transaction<'_>,
    command: &TeamExpansionDecisionCommand,
    approved_count: u8,
) -> Result<(), OperationsError> {
    for ordinal in 0..approved_count {
        transaction.execute(
            "INSERT INTO ops_team_expansion_slots(decision_id,request_id,ordinal,workspace_id,team_id) VALUES(?1,?2,?3,?4,?5)",
            params![
                command.decision_id.as_bytes(),
                command.request_id.as_bytes(),
                i64::from(ordinal),
                command.workspace_id.into_uuid().as_bytes(),
                command.team_id.as_bytes(),
            ],
        )?;
    }
    Ok(())
}

fn decision_matches_command(
    connection: &Connection,
    receipt: &TeamExpansionDecisionReceipt,
    command: &TeamExpansionDecisionCommand,
    actor: &TeamExpansionActor,
) -> Result<bool, OperationsError> {
    let request = load_request_for_decision(connection, command.workspace_id, command.request_id)?;
    let expected = match command.decision {
        TeamExpansionDecision::Approve { approved_count } => {
            receipt.state != TeamExpansionDecisionState::Rejected
                && receipt.approved_count == approved_count
                && decision_slots_match(
                    connection,
                    command.workspace_id,
                    command.team_id,
                    command.decision_id,
                    command.request_id,
                    approved_count,
                )?
        }
        TeamExpansionDecision::Reject => {
            receipt.state == TeamExpansionDecisionState::Rejected
                && receipt.approved_count == 0
                && decision_slots_match(
                    connection,
                    command.workspace_id,
                    command.team_id,
                    command.decision_id,
                    command.request_id,
                    0,
                )?
        }
    };
    Ok(receipt.decision_id == command.decision_id
        && receipt.request_id == command.request_id
        && receipt.workspace_id == command.workspace_id
        && receipt.team_id == command.team_id
        && receipt.expected_team_revision == command.expected_team_revision
        && receipt.capacity_revision == command.expected_capacity_revision
        && receipt.leadership_revision == command.expected_leadership_revision
        && actor_same(&receipt.actor, actor)
        && expected
        && request.is_some_and(|request| {
            request.requester_leadership_revision == command.expected_requester_leadership_revision
        }))
}

fn decision_source_matches(
    connection: &Connection,
    receipt: &TeamExpansionDecisionReceipt,
    command: &TeamExpansionDecisionCommand,
    source: DecisionSource<'_>,
) -> Result<bool, OperationsError> {
    match command.decision {
        TeamExpansionDecision::Reject => Ok(source_is_empty(source)),
        TeamExpansionDecision::Approve { approved_count } => {
            let (plans, transfers) = match source {
                DecisionSource::Legacy => {
                    return workspace_hold::is_legacy_approval(
                        connection,
                        receipt.workspace_id,
                        receipt.team_id,
                        receipt.decision_id,
                        receipt.request_id,
                    );
                }
                DecisionSource::V2(plans) => {
                    if existing_home_transfer::has_manifest(
                        connection,
                        command.workspace_id,
                        command.decision_id,
                    )? {
                        return Ok(false);
                    }
                    (plans, None)
                }
                DecisionSource::V3 { plans, transfers } => (plans, Some(transfers)),
            };
            if !workspace_hold::workspace_hold_manifest_matches_intent(
                connection,
                command.workspace_id,
                command.team_id,
                command.decision_id,
                command.request_id,
                approved_count,
                plans,
            )? {
                return Ok(false);
            }
            let holds_match = matches!(
                workspace_hold::validate_workspace_hold_manifest(
                    connection,
                    command.workspace_id,
                    command.team_id,
                    command.decision_id,
                    command.request_id,
                    approved_count,
                    plans,
                )?,
                workspace_hold::WorkspaceHoldDisposition::Reserved
            );
            if !holds_match {
                return Ok(false);
            }
            if let Some(transfers) = transfers {
                return existing_home_transfer::source_matches_intent(
                    connection, command, transfers,
                );
            }
            Ok(true)
        }
    }
}

fn decision_slots_match(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    decision_id: Uuid,
    request_id: Uuid,
    expected_count: u8,
) -> Result<bool, OperationsError> {
    let (count, mismatched_requests, minimum, maximum):
        (i64, i64, Option<i64>, Option<i64>) = connection.query_row(
        "SELECT COUNT(*),COALESCE(SUM(CASE WHEN request_id=?4 THEN 0 ELSE 1 END),0),MIN(ordinal),MAX(ordinal) FROM ops_team_expansion_slots WHERE workspace_id=?1 AND team_id=?2 AND decision_id=?3",
        params![
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes(),
            decision_id.as_bytes(),
            request_id.as_bytes(),
        ],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    let expected_count = i64::from(expected_count);
    Ok(if expected_count == 0 {
        count == 0 && mismatched_requests == 0 && minimum.is_none() && maximum.is_none()
    } else {
        count == expected_count
            && mismatched_requests == 0
            && minimum == Some(0)
            && maximum == Some(expected_count - 1)
    })
}

pub(super) fn load_decision_by_id(
    connection: &Connection,
    workspace_id: WorkspaceId,
    decision_id: Uuid,
    replayed: bool,
) -> Result<Option<TeamExpansionDecisionReceipt>, OperationsError> {
    let sql = format!("{DECISION_SELECT} WHERE d.workspace_id=?1 AND d.decision_id=?2");
    let raw = connection
        .query_row(
            &sql,
            params![workspace_id.into_uuid().as_bytes(), decision_id.as_bytes()],
            decode_decision_raw,
        )
        .optional()?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    let receipt = raw.into_receipt(replayed)?;
    let expected_slots = if receipt.state == TeamExpansionDecisionState::Rejected {
        0
    } else {
        receipt.approved_count
    };
    if !decision_slots_match(
        connection,
        receipt.workspace_id,
        receipt.team_id,
        receipt.decision_id,
        receipt.request_id,
        expected_slots,
    )? {
        return Err(OperationsError::Corrupt("Team expansion decision slot set"));
    }
    Ok(Some(receipt))
}

fn decision_id_exists(connection: &Connection, decision_id: Uuid) -> Result<bool, OperationsError> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_team_expansion_decisions WHERE decision_id=?1)",
            params![decision_id.as_bytes()],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

fn request_has_decision(
    connection: &Connection,
    workspace_id: WorkspaceId,
    request_id: Uuid,
) -> Result<bool, OperationsError> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_team_expansion_decisions WHERE workspace_id=?1 AND request_id=?2)",
            params![workspace_id.into_uuid().as_bytes(), request_id.as_bytes()],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

fn ensure_source_time_not_regressed(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    observed_at: TimestampMillis,
) -> Result<(), OperationsError> {
    let latest: Option<i64> = connection.query_row(
        "SELECT MAX(value) FROM (SELECT updated_at_ms AS value FROM ops_teams WHERE workspace_id=?1 AND team_id=?2 UNION ALL SELECT MAX(recorded_at_ms) FROM ops_team_capacity_events WHERE workspace_id=?1 AND team_id=?2 UNION ALL SELECT MAX(recorded_at_ms) FROM ops_team_expansion_requests WHERE workspace_id=?1 AND team_id=?2 UNION ALL SELECT MAX(decided_at_ms) FROM ops_team_expansion_decisions WHERE workspace_id=?1 AND team_id=?2 UNION ALL SELECT MAX(anchor_at_ms) FROM ops_team_expansion_cooldown_anchors WHERE workspace_id=?1 AND team_id=?2 UNION ALL SELECT MAX(recorded_at_ms) FROM ops_team_expansion_slot_assignments WHERE workspace_id=?1 AND team_id=?2)",
        params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()],
        |row| row.get(0),
    )?;
    let recovery_latest: Option<i64> = connection.query_row(
        "SELECT MAX(recorded_at_ms) FROM ops_team_expansion_legacy_slot_recoveries WHERE workspace_id=?1 AND team_id=?2",
        params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()],
        |row| row.get(0),
    )?;
    let latest = latest.into_iter().chain(recovery_latest).max();
    if latest.is_some_and(|latest| observed_at.get() < latest) {
        return Err(OperationsError::Conflict(
            "Team expansion trusted clock regressed",
        ));
    }
    Ok(())
}

fn requester_identity_for_decision(
    connection: &Connection,
    workspace_id: WorkspaceId,
    decision_id: Uuid,
) -> Result<Option<(Value, Value)>, OperationsError> {
    connection
        .query_row(
            "SELECT request.actor_participation_id,request.actor_profile_id FROM ops_team_expansion_decisions AS decision JOIN ops_team_expansion_requests AS request ON request.workspace_id=decision.workspace_id AND request.team_id=decision.team_id AND request.request_id=decision.request_id WHERE decision.workspace_id=?1 AND decision.decision_id=?2",
            params![workspace_id.into_uuid().as_bytes(), decision_id.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(Into::into)
}

fn decision_actor_identity_for_decision(
    connection: &Connection,
    workspace_id: WorkspaceId,
    decision_id: Uuid,
) -> Result<Option<(Value, Value)>, OperationsError> {
    connection
        .query_row(
            "SELECT actor_participation_id,actor_profile_id FROM ops_team_expansion_decisions WHERE workspace_id=?1 AND decision_id=?2",
            params![workspace_id.into_uuid().as_bytes(), decision_id.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(Into::into)
}

fn v3_decision_actor_identity(
    connection: &Connection,
    workspace_id: WorkspaceId,
    decision_id: Uuid,
) -> Result<Option<(Value, Value)>, OperationsError> {
    connection
        .query_row(
            "SELECT decision.actor_participation_id,decision.actor_profile_id
             FROM ops_team_expansion_decisions AS decision
             JOIN ops_team_expansion_v3_manifests AS source
               ON source.workspace_id=decision.workspace_id
              AND source.team_id=decision.team_id
              AND source.decision_id=decision.decision_id
              AND source.request_id=decision.request_id
             WHERE decision.workspace_id=?1 AND decision.decision_id=?2",
            params![workspace_id.into_uuid().as_bytes(), decision_id.as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(Into::into)
}

#[derive(Debug)]
struct DecisionRequestRaw {
    team_id: Uuid,
    requester_leadership_revision: i64,
    requested_count: u8,
}

fn load_request_for_decision(
    connection: &Connection,
    workspace_id: WorkspaceId,
    request_id: Uuid,
) -> Result<Option<DecisionRequestRaw>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT team_id,requester_leadership_revision,requested_count FROM ops_team_expansion_requests WHERE workspace_id=?1 AND request_id=?2",
            params![workspace_id.into_uuid().as_bytes(), request_id.as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional()?;
    raw.map(|(team_id, requester_revision, count)| {
        Ok(DecisionRequestRaw {
            team_id: Uuid::from_bytes(wire::id(team_id)?),
            requester_leadership_revision: requester_revision,
            requested_count: u8::try_from(count)
                .map_err(|_| OperationsError::Corrupt("Team expansion requested count"))?,
        })
    })
    .transpose()
}

#[derive(Debug)]
struct DecisionRaw {
    decision_id: Vec<u8>,
    request_id: Vec<u8>,
    workspace_id: Vec<u8>,
    team_id: Vec<u8>,
    state: String,
    approved_count: i64,
    expected_team_revision: i64,
    capacity_revision: i64,
    leadership_revision: i64,
    actor: ActorRaw,
    decided_at_ms: i64,
    anchor_at_ms: Option<i64>,
    cooldown_until_ms: Option<i64>,
    finalizer: Option<ActorRaw>,
    finalizer_leadership_revision: Option<i64>,
}

#[derive(Debug)]
struct ActorRaw {
    activity_id: Vec<u8>,
    agent_id: Vec<u8>,
    session_id: Vec<u8>,
    participation_id: Vec<u8>,
    profile_id: Vec<u8>,
    participant_id: Vec<u8>,
    display_name: String,
}

fn decode_decision_raw(row: &rusqlite::Row<'_>) -> rusqlite::Result<DecisionRaw> {
    let anchor_at_ms = row.get::<_, Option<i64>>(17)?;
    Ok(DecisionRaw {
        decision_id: row.get(0)?,
        request_id: row.get(1)?,
        workspace_id: row.get(2)?,
        team_id: row.get(3)?,
        state: row.get(4)?,
        approved_count: row.get(5)?,
        expected_team_revision: row.get(6)?,
        capacity_revision: row.get(7)?,
        leadership_revision: row.get(8)?,
        actor: ActorRaw {
            activity_id: row.get(9)?,
            agent_id: row.get(10)?,
            session_id: row.get(11)?,
            participation_id: row.get(12)?,
            profile_id: row.get(13)?,
            participant_id: row.get(14)?,
            display_name: row.get(15)?,
        },
        decided_at_ms: row.get(16)?,
        anchor_at_ms,
        cooldown_until_ms: row.get(18)?,
        finalizer: if anchor_at_ms.is_some() {
            Some(ActorRaw {
                activity_id: row.get(19)?,
                agent_id: row.get(20)?,
                session_id: row.get(21)?,
                participation_id: row.get(22)?,
                profile_id: row.get(23)?,
                participant_id: row.get(24)?,
                display_name: row.get(25)?,
            })
        } else {
            None
        },
        finalizer_leadership_revision: row.get(26)?,
    })
}

impl DecisionRaw {
    fn into_receipt(self, replayed: bool) -> Result<TeamExpansionDecisionReceipt, OperationsError> {
        let workspace_id = WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(self.workspace_id)?));
        let anchor_at = self
            .anchor_at_ms
            .map(TimestampMillis::new)
            .transpose()
            .map_err(|_| OperationsError::Corrupt("Team expansion cooldown timestamp"))?;
        let cooldown_until = self
            .cooldown_until_ms
            .map(TimestampMillis::new)
            .transpose()
            .map_err(|_| OperationsError::Corrupt("Team expansion cooldown timestamp"))?;
        if anchor_at.is_some() != cooldown_until.is_some()
            || anchor_at.is_some() != self.finalizer.is_some()
            || anchor_at.is_some() != self.finalizer_leadership_revision.is_some()
        {
            return Err(OperationsError::Corrupt("Team expansion cooldown anchor"));
        }
        if let (Some(start), Some(until)) = (anchor_at, cooldown_until)
            && start.get().checked_add(TEAM_EXPANSION_COOLDOWN_MS) != Some(until.get())
        {
            return Err(OperationsError::Corrupt("Team expansion cooldown duration"));
        }
        let state = match (self.state.as_str(), anchor_at) {
            ("APPROVED", None) => TeamExpansionDecisionState::ApprovedPendingAnchor,
            ("APPROVED", Some(_)) => TeamExpansionDecisionState::Approved,
            ("REJECTED", None) => TeamExpansionDecisionState::Rejected,
            _ => return Err(OperationsError::Corrupt("Team expansion decision state")),
        };
        if (state == TeamExpansionDecisionState::Rejected && self.approved_count != 0)
            || (state != TeamExpansionDecisionState::Rejected && self.approved_count < 1)
        {
            return Err(OperationsError::Corrupt("Team expansion approved count"));
        }
        Ok(TeamExpansionDecisionReceipt {
            decision_id: Uuid::from_bytes(wire::id(self.decision_id)?),
            request_id: Uuid::from_bytes(wire::id(self.request_id)?),
            workspace_id,
            team_id: Uuid::from_bytes(wire::id(self.team_id)?),
            state,
            approved_count: u8::try_from(self.approved_count)
                .map_err(|_| OperationsError::Corrupt("Team expansion approved count"))?,
            cooldown_started_at: anchor_at,
            cooldown_until,
            expected_team_revision: u64::try_from(self.expected_team_revision)
                .map_err(|_| OperationsError::Corrupt("Team expansion Team revision"))?,
            capacity_revision: u64::try_from(self.capacity_revision)
                .map_err(|_| OperationsError::Corrupt("Team expansion capacity revision"))?,
            leadership_revision: self.leadership_revision,
            actor: self.actor.into_actor()?,
            finalized_by: self.finalizer.map(ActorRaw::into_actor).transpose()?,
            finalized_by_leadership_revision: self.finalizer_leadership_revision,
            recorded_at: TimestampMillis::new(self.decided_at_ms)
                .map_err(|_| OperationsError::Corrupt("Team expansion decision timestamp"))?,
            replayed,
        })
    }
}

impl ActorRaw {
    fn into_actor(self) -> Result<TeamExpansionActor, OperationsError> {
        Ok(TeamExpansionActor {
            activity_id: ActivityId::from_uuid(Uuid::from_bytes(wire::id(self.activity_id)?)),
            agent_id: AgentInstanceId::from_uuid(Uuid::from_bytes(wire::id(self.agent_id)?)),
            session_id: AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(self.session_id)?)),
            participation_id: WorkspaceParticipationId::from_uuid(Uuid::from_bytes(wire::id(
                self.participation_id,
            )?))?,
            profile_id: Uuid::from_bytes(wire::id(self.profile_id)?),
            participant_id: Uuid::from_bytes(wire::id(self.participant_id)?),
            display_name: self.display_name,
        })
    }
}

fn with_decision_replay(mut receipt: TeamExpansionDecisionReceipt) -> TeamExpansionDecisionReceipt {
    receipt.replayed = true;
    receipt
}
