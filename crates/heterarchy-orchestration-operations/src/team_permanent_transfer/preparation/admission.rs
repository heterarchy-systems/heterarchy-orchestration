//! Current authority and narrowly scoped admission barriers for preparation.

use forge_domain::{AgentSessionId, WorkspaceId};
use rusqlite::{Connection, Transaction, params};
use uuid::Uuid;

use super::super::{
    TeamPermanentTransferConsentKind as Kind, TeamPermanentTransferIntentReceipt as Intent,
    current_authority, generation, load_consent, validate_consent_actor,
    validate_live_transfer_intent,
};
use super::{
    TeamPermanentTransferPreparationCommand as Command,
    TeamPermanentTransferPreparationReceipt as Receipt, ensure_unprepared,
};
use crate::OperationsError;

pub(super) fn validate(
    tx: &Transaction<'_>,
    command: &Command,
    intent: &Intent,
) -> Result<i64, OperationsError> {
    validate_live_transfer_intent(tx, intent)?;
    ensure_unprepared(tx, command.workspace_id, command.transfer_id)?;
    validate_current_state(
        tx,
        CurrentState {
            workspace_id: command.workspace_id,
            transfer_id: command.transfer_id,
            generation: command.expected_generation,
            source_leader_consent_id: command.source_leader_consent_id,
            target_acceptance_id: command.target_acceptance_id,
        },
        intent,
        false,
    )
}

/// Revalidate a persisted preparation's current admission sources while
/// allowing its own immutable target Profile barrier to remain installed.
/// The caller must load `preparation` through the checked preparation store
/// inside this same transaction so its barrier indexes are already verified.
pub(in crate::team_permanent_transfer) fn validate_current_source(
    tx: &Transaction<'_>,
    preparation: &Receipt,
    intent: &Intent,
) -> Result<i64, OperationsError> {
    if preparation.workspace_id() != intent.workspace_id
        || preparation.transfer_id() != intent.transfer_id
        || preparation.intent_digest() != intent.intent_digest
        || preparation.source_team_id() != intent.source_team_id
        || preparation.destination_team_id() != intent.destination_team_id
        || preparation.target_profile_id() != intent.target_profile_id
        || preparation.target_participation_id() != intent.target_participation_id
        || preparation.source_membership_revision() != intent.source_membership_revision
    {
        return Err(OperationsError::Conflict(
            "transfer preparation current source identity",
        ));
    }
    validate_live_transfer_intent(tx, intent)?;
    validate_current_state(
        tx,
        CurrentState {
            workspace_id: preparation.workspace_id(),
            transfer_id: preparation.transfer_id(),
            generation: preparation.generation(),
            source_leader_consent_id: preparation.source_leader_consent_id(),
            target_acceptance_id: preparation.target_acceptance_id(),
        },
        intent,
        true,
    )
}

#[derive(Clone, Copy)]
struct CurrentState {
    workspace_id: WorkspaceId,
    transfer_id: Uuid,
    generation: u64,
    source_leader_consent_id: Uuid,
    target_acceptance_id: Uuid,
}

fn validate_current_state(
    tx: &Transaction<'_>,
    current: CurrentState,
    intent: &Intent,
    preparation_barrier_is_held: bool,
) -> Result<i64, OperationsError> {
    if current.workspace_id != intent.workspace_id || current.transfer_id != intent.transfer_id {
        return Err(OperationsError::Conflict(
            "transfer preparation current source scope",
        ));
    }
    let generation = generation::read_generation(tx, current.workspace_id, current.transfer_id)?
        .ok_or(OperationsError::Corrupt(
            "transfer preparation generation missing",
        ))?;
    if !generation.active() || generation.generation() != current.generation {
        return Err(OperationsError::Conflict(
            "transfer preparation generation changed",
        ));
    }
    let profile =
        heterarchy_orchestration_sessions::AgentProfileId::from_uuid(intent.target_profile_id)
            .map_err(|_| OperationsError::Corrupt("transfer preparation target Profile"))?;
    if !preparation_barrier_is_held {
        crate::leadership::work_roster::ensure_profile_admission_open(
            tx,
            intent.workspace_id,
            profile,
        )?;
    }
    let mut high_water = intent.recorded_at.get();
    for (id, kind) in [
        (current.source_leader_consent_id, Kind::SourceHomeLeader),
        (current.target_acceptance_id, Kind::TargetAgentAcceptance),
    ] {
        let consent = load_consent(tx, intent.workspace_id, intent.transfer_id, id)?.ok_or(
            OperationsError::Conflict("transfer preparation consent missing"),
        )?;
        let (authority, membership) = validate_consent_actor(tx, intent, kind, &consent.actor)?;
        if consent.kind != kind
            || consent.intent_digest != intent.intent_digest
            || consent.authority_revision != authority
            || consent.membership_revision != membership
        {
            return Err(OperationsError::Conflict(
                "transfer preparation consent changed",
            ));
        }
        high_water = high_water.max(consent.recorded_at.get());
    }
    for scope in [None, Some(intent.source_team_id)] {
        let leader = current_authority(tx, intent.workspace_id, scope)?.ok_or(
            OperationsError::Conflict("transfer preparation leadership missing"),
        )?;
        let (participation, profile) =
            super::super::participant_identity(tx, intent.workspace_id, leader.participant_id())?;
        if participation == intent.target_participation_id || profile == intent.target_profile_id {
            return Err(OperationsError::Conflict(
                "transfer target must hand off leadership first",
            ));
        }
    }
    let target_active:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM ops_teams WHERE workspace_id=?1 AND team_id=?2 AND state='ACTIVE')",
        params![intent.workspace_id.into_uuid().as_bytes(),intent.destination_team_id.as_bytes()],|r|r.get(0))?;
    if !target_active {
        return Err(OperationsError::Conflict(
            "transfer preparation destination Team inactive",
        ));
    }
    let dispatch:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM ops_temporary_dispatch_reservations WHERE workspace_id=?1 AND (target_participation_id=?2 OR target_profile_id=?3) AND state IN('RESERVED','SOURCE_READY','ACTIVE','DRAINING'))",
        params![intent.workspace_id.into_uuid().as_bytes(),intent.target_participation_id.into_uuid().as_bytes(),intent.target_profile_id.as_bytes()],|r|r.get(0))?;
    if dispatch {
        return Err(OperationsError::Conflict(
            "transfer preparation target has unresolved temporary dispatch",
        ));
    }
    let authority_time: Option<i64> = tx.query_row(
        "SELECT MAX(updated_at_ms) FROM ops_leadership WHERE workspace_id=?1 AND ((scope_kind=0 AND team_id IS NULL AND activity_id IS NULL) OR (scope_kind=2 AND team_id=?2 AND activity_id IS NULL))",
        params![intent.workspace_id.into_uuid().as_bytes(), intent.source_team_id.as_bytes()],
        |r| r.get(0),
    )?;
    let team_time: Option<i64> = tx.query_row(
        "SELECT MAX(updated_at_ms) FROM ops_teams WHERE workspace_id=?1 AND team_id IN(?2,?3)",
        params![
            intent.workspace_id.into_uuid().as_bytes(),
            intent.source_team_id.as_bytes(),
            intent.destination_team_id.as_bytes()
        ],
        |r| r.get(0),
    )?;
    Ok(high_water
        .max(authority_time.unwrap_or(0))
        .max(team_time.unwrap_or(0)))
}

pub(crate) fn ensure_profile_open(
    c: &Connection,
    w: WorkspaceId,
    profile: Uuid,
) -> Result<(), OperationsError> {
    let blocked:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM ops_team_permanent_transfer_preparation_barriers WHERE workspace_id=?1 AND target_profile_id=?2)",
        params![w.into_uuid().as_bytes(),profile.as_bytes()],|r|r.get(0))?;
    if blocked {
        return Err(OperationsError::Conflict(
            "Profile admission is frozen for permanent transfer preparation",
        ));
    }
    Ok(())
}

pub(crate) fn ensure_session_open(
    c: &Connection,
    w: WorkspaceId,
    session: AgentSessionId,
) -> Result<(), OperationsError> {
    let blocked:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM ops_team_permanent_transfer_preparation_barriers b JOIN ops_team_permanent_transfer_preparation_sessions s ON s.workspace_id=b.workspace_id AND s.preparation_id=b.preparation_id WHERE b.workspace_id=?1 AND s.session_id=?2)",
        params![w.into_uuid().as_bytes(),session.into_uuid().as_bytes()],|r|r.get(0))?;
    if blocked {
        return Err(OperationsError::Conflict(
            "Session admission is frozen for permanent transfer preparation",
        ));
    }
    Ok(())
}

pub(crate) fn ensure_unscoped_open(c: &Connection, w: WorkspaceId) -> Result<(), OperationsError> {
    let blocked:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM ops_team_permanent_transfer_preparation_barriers WHERE workspace_id=?1)",
        params![w.into_uuid().as_bytes()],|r|r.get(0))?;
    if blocked {
        return Err(OperationsError::Conflict(
            "unscoped admission during permanent transfer preparation",
        ));
    }
    Ok(())
}

pub(crate) fn ensure_team_open(
    c: &Connection,
    w: WorkspaceId,
    team: Uuid,
) -> Result<(), OperationsError> {
    let blocked:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM ops_team_permanent_transfer_preparation_barriers b JOIN ops_team_permanent_transfer_preparations p ON p.workspace_id=b.workspace_id AND p.preparation_id=b.preparation_id JOIN ops_team_permanent_transfer_intents i ON i.workspace_id=p.workspace_id AND i.transfer_id=p.transfer_id WHERE b.workspace_id=?1 AND (i.source_team_id=?2 OR i.destination_team_id=?2))",
        params![w.into_uuid().as_bytes(),team.as_bytes()],|r|r.get(0))?;
    if blocked {
        return Err(OperationsError::Conflict(
            "Team retirement is frozen for permanent transfer preparation",
        ));
    }
    Ok(())
}
