use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, WorkspaceId};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use uuid::Uuid;

use crate::{
    MissionEvaluationAuthority, MissionEvaluationPlanId, MissionId, OperationsError,
    WorkspaceParticipationId, catalog, teams, wire,
};

use super::{
    MissionT2AccountableOwner, MissionT2CheckpointId, MissionT2DeferCommand, MissionT2DeferContent,
    MissionT2DeferOutcome, MissionT2DeferredCheckpoint, validate_checkpoint,
};
use crate::mission_evaluation::{MissionActorSnapshot, frozen_plan_in_transaction};

pub(super) fn defer(
    connection: &mut rusqlite::Connection,
    destination: Uuid,
    command: &MissionT2DeferCommand,
    trusted_now_ms: impl FnOnce() -> Result<i64, OperationsError>,
) -> Result<MissionT2DeferOutcome, OperationsError> {
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    teams::ensure_verified_workspace(&tx, command.content().workspace_id())?;
    let actor =
        MissionActorSnapshot::resolve(&tx, command.content().workspace_id(), command.authority)?;
    if actor.identity.kind == 0 {
        return Err(OperationsError::Conflict(
            "mission T2 defer requires Workspace leader",
        ));
    }

    if let Some(existing) = load_by_request(&tx, command.content().request_id())? {
        if !command.matches_record(&existing, &actor) {
            return Err(OperationsError::Conflict(
                "mission T2 defer request replay mismatch",
            ));
        }
        catalog::commit_read(tx, destination)?;
        return Ok(MissionT2DeferOutcome {
            checkpoint: existing,
            replayed: true,
        });
    }
    let request_collision: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_mission_evaluation_requests WHERE request_id=?1)",
        params![command.content().request_id().as_bytes()],
        |row| row.get(0),
    )?;
    if request_collision {
        return Err(OperationsError::Conflict(
            "mission T2 defer request identity already used",
        ));
    }

    let frozen = require_current_started_plan(&tx, command, &actor)?;
    let latest = load_latest(
        &tx,
        command.content().workspace_id(),
        command.content().plan_id(),
        command.content().revision(),
    )?;
    let (recorded_at_ms, sequence) = next_defer_position(
        command.content(),
        latest.as_ref(),
        frozen.version.t2_checkpoint_ms(),
        trusted_now_ms,
    )?;
    if checkpoint_identity_exists(&tx, command.content().checkpoint_id())? {
        return Err(OperationsError::Conflict(
            "mission T2 checkpoint identity already used",
        ));
    }
    let accountable_owner = build_accountable_owner(command.authority.actor, &actor)?;
    let checkpoint = MissionT2DeferredCheckpoint {
        request_id: command.content().request_id(),
        checkpoint_id: command.content().checkpoint_id(),
        workspace_id: command.content().workspace_id(),
        mission_id: command.content().mission_id(),
        plan_id: command.content().plan_id(),
        revision: command.content().revision(),
        sequence,
        previous_checkpoint_id: command.content().expected_previous_checkpoint_id(),
        reason: command.content().reason().to_owned(),
        next_check_condition: command.content().next_check_condition().to_owned(),
        next_check_at_ms: command.content().next_check_at_ms(),
        recorded_at_ms,
        accountable_owner,
    };
    validate_checkpoint(&checkpoint)?;
    commit_checkpoint(tx, command, &actor, &checkpoint)
}

fn require_current_started_plan(
    tx: &Transaction<'_>,
    command: &MissionT2DeferCommand,
    actor: &MissionActorSnapshot,
) -> Result<super::super::FrozenMissionEvaluationPlan, OperationsError> {
    actor.authorize_current(tx, command.content().workspace_id(), command.authority)?;
    let frozen = frozen_plan_in_transaction(
        tx,
        command.content().workspace_id(),
        command.content().plan_id(),
        command.content().revision(),
    )?;
    if frozen.version.mission_id() != command.content().mission_id()
        || frozen.start.mission_id() != command.content().mission_id()
        || frozen.start.plan_id() != command.content().plan_id()
        || frozen.start.revision() != command.content().revision()
    {
        return Err(OperationsError::Conflict(
            "mission T2 defer started plan identity",
        ));
    }
    let team_id = mission_team_id(
        tx,
        command.content().workspace_id(),
        command.content().mission_id(),
    )?;
    require_active_current_mission(
        tx,
        command.content().workspace_id(),
        team_id,
        command.content().mission_id(),
    )?;
    Ok(frozen)
}

fn next_defer_position(
    command: &MissionT2DeferContent,
    latest: Option<&MissionT2DeferredCheckpoint>,
    t2_checkpoint_at_ms: i64,
    trusted_now_ms: impl FnOnce() -> Result<i64, OperationsError>,
) -> Result<(i64, u64), OperationsError> {
    let expected_previous = latest.map(MissionT2DeferredCheckpoint::checkpoint_id);
    if command.expected_previous_checkpoint_id() != expected_previous {
        return Err(OperationsError::Conflict(
            "mission T2 defer predecessor changed",
        ));
    }
    let recorded_at_ms = trusted_now_ms()?;
    if recorded_at_ms < 0 {
        return Err(OperationsError::Conflict(
            "mission T2 trusted clock unavailable",
        ));
    }
    if recorded_at_ms < t2_checkpoint_at_ms {
        return Err(OperationsError::Conflict(
            "mission T2 defer before checkpoint",
        ));
    }
    if recorded_at_ms >= command.next_check_at_ms() {
        return Err(OperationsError::Conflict(
            "mission T2 next check time is not in the future",
        ));
    }
    if latest.is_some_and(|record| recorded_at_ms < record.recorded_at_ms()) {
        return Err(OperationsError::Conflict("mission T2 defer time regressed"));
    }
    let sequence = latest.map_or(Ok(0), |record| {
        record
            .sequence()
            .checked_add(1)
            .filter(|value| *value <= super::super::MAX_SQLITE_INTEGER)
            .ok_or(OperationsError::Bounds("mission T2 defer sequence"))
    })?;
    Ok((recorded_at_ms, sequence))
}

fn build_accountable_owner(
    source_actor: crate::OperationalActor,
    actor: &MissionActorSnapshot,
) -> Result<MissionT2AccountableOwner, OperationsError> {
    actor.validate()?;
    let crate::OperationalActor::Participant {
        activity_id,
        agent_id,
        session_id,
    } = source_actor
    else {
        return Err(OperationsError::Conflict(
            "mission T2 defer requires participant source actor",
        ));
    };
    let owner = MissionT2AccountableOwner {
        activity_id,
        agent_id,
        session_id,
        actor_kind: actor.identity.kind,
        actor_key: actor.identity.key.clone(),
        participation_id: actor.identity.participation_id,
        profile_id: actor.identity.profile_id,
        participant_id: actor
            .identity
            .participant_id
            .ok_or(OperationsError::Conflict(
                "mission T2 defer requires Workspace participant",
            ))?,
        display_name: actor.display_name.clone(),
        leadership_revision: actor.leadership_revision.ok_or(OperationsError::Conflict(
            "mission T2 defer requires Workspace leadership revision",
        ))?,
    };
    owner.validate()?;
    Ok(owner)
}

fn commit_checkpoint(
    tx: Transaction<'_>,
    command: &MissionT2DeferCommand,
    actor: &MissionActorSnapshot,
    checkpoint: &MissionT2DeferredCheckpoint,
) -> Result<MissionT2DeferOutcome, OperationsError> {
    insert_checkpoint(&tx, checkpoint)?;
    let readback = load_by_id(
        &tx,
        command.content().workspace_id(),
        command.content().plan_id(),
        command.content().revision(),
        command.content().checkpoint_id(),
    )?
    .ok_or(OperationsError::Corrupt(
        "mission T2 defer readback missing",
    ))?;
    if !command.matches_record(&readback, actor) {
        return Err(OperationsError::Corrupt(
            "mission T2 defer readback mismatch",
        ));
    }
    tx.commit()
        .map_err(|source| OperationsError::OperationCommitUnconfirmed {
            workspace_id: command.content().workspace_id(),
            request_id: command.content().request_id(),
            source,
        })?;
    Ok(MissionT2DeferOutcome {
        checkpoint: readback,
        replayed: false,
    })
}

pub(super) fn read_one(
    connection: &rusqlite::Connection,
    destination: Uuid,
    workspace_id: WorkspaceId,
    plan_id: MissionEvaluationPlanId,
    revision: u64,
    checkpoint_id: MissionT2CheckpointId,
) -> Result<Option<MissionT2DeferredCheckpoint>, OperationsError> {
    if workspace_id.into_uuid().is_nil()
        || plan_id.into_uuid().is_nil()
        || checkpoint_id.into_uuid().is_nil()
    {
        return Err(OperationsError::Conflict(
            "mission T2 checkpoint read scope",
        ));
    }
    if revision == 0 || revision > super::super::MAX_SQLITE_INTEGER {
        return Err(OperationsError::Bounds("mission T2 plan revision"));
    }
    let tx = connection.unchecked_transaction()?;
    teams::ensure_verified_workspace(&tx, workspace_id)?;
    let record = load_by_id(&tx, workspace_id, plan_id, revision, checkpoint_id)?;
    catalog::commit_read(tx, destination)?;
    Ok(record)
}

pub(super) fn read_for_request(
    connection: &rusqlite::Connection,
    destination: Uuid,
    workspace_id: WorkspaceId,
    request_id: Uuid,
    authority: MissionEvaluationAuthority,
) -> Result<Option<MissionT2DeferredCheckpoint>, OperationsError> {
    if workspace_id.into_uuid().is_nil() || request_id.is_nil() {
        return Err(OperationsError::Conflict("mission T2 request read scope"));
    }
    if matches!(authority.actor, crate::OperationalActor::LocalOperator) {
        return Err(OperationsError::Conflict(
            "mission T2 request read requires original actor or current Workspace leader",
        ));
    }
    let tx = connection.unchecked_transaction()?;
    teams::ensure_verified_workspace(&tx, workspace_id)?;
    let actor = MissionActorSnapshot::resolve(&tx, workspace_id, authority)?;
    let record = load_by_request_in_workspace(&tx, workspace_id, request_id)?;
    if let Some(record) = &record {
        if !record.accountable_owner.same_semantic_actor(&actor) {
            actor.authorize_current(&tx, workspace_id, authority)?;
        }
    } else {
        actor.authorize_current(&tx, workspace_id, authority)?;
    }
    catalog::commit_read(tx, destination)?;
    Ok(record)
}

pub(super) fn read_latest(
    connection: &rusqlite::Connection,
    destination: Uuid,
    workspace_id: WorkspaceId,
    plan_id: MissionEvaluationPlanId,
    revision: u64,
) -> Result<Option<MissionT2DeferredCheckpoint>, OperationsError> {
    if workspace_id.into_uuid().is_nil() || plan_id.into_uuid().is_nil() {
        return Err(OperationsError::Conflict(
            "mission T2 checkpoint read scope",
        ));
    }
    if revision == 0 || revision > super::super::MAX_SQLITE_INTEGER {
        return Err(OperationsError::Bounds("mission T2 plan revision"));
    }
    let tx = connection.unchecked_transaction()?;
    teams::ensure_verified_workspace(&tx, workspace_id)?;
    let record = load_latest(&tx, workspace_id, plan_id, revision)?;
    catalog::commit_read(tx, destination)?;
    Ok(record)
}

fn mission_team_id(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    mission_id: MissionId,
) -> Result<Uuid, OperationsError> {
    let bytes = tx
        .query_row(
            "SELECT substr(team_id,1,17) FROM ops_mission_evaluation_missions WHERE workspace_id=?1 AND mission_id=?2 LIMIT 1",
            params![workspace_id.into_uuid().as_bytes(), mission_id.into_uuid().as_bytes()],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?
        .ok_or(OperationsError::Conflict("mission T2 Mission scope"))?;
    decode_uuid(bytes, "mission T2 Team identity")
}

fn require_active_current_mission(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    mission_id: MissionId,
) -> Result<(), OperationsError> {
    let state = tx
        .query_row(
            "SELECT state FROM ops_teams WHERE workspace_id=?1 AND team_id=?2 LIMIT 1",
            params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if state.as_deref() != Some("ACTIVE") {
        return Err(OperationsError::Conflict("mission T2 Team is not active"));
    }
    let current: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_mission_evaluation_team_heads WHERE workspace_id=?1 AND team_id=?2 AND mission_id=?3)",
        params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes(), mission_id.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    if !current {
        return Err(OperationsError::Conflict(
            "mission T2 Mission is not current",
        ));
    }
    Ok(())
}

fn checkpoint_identity_exists(
    tx: &Transaction<'_>,
    checkpoint_id: MissionT2CheckpointId,
) -> Result<bool, OperationsError> {
    tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_mission_evaluation_t2_deferred_checkpoints WHERE checkpoint_id=?1)",
        params![checkpoint_id.into_uuid().as_bytes()],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

fn insert_checkpoint(
    tx: &Transaction<'_>,
    checkpoint: &MissionT2DeferredCheckpoint,
) -> Result<(), OperationsError> {
    let owner = &checkpoint.accountable_owner;
    let sequence = i64::try_from(checkpoint.sequence)
        .map_err(|_| OperationsError::Bounds("mission T2 defer sequence"))?;
    tx.execute(
        "INSERT INTO ops_mission_evaluation_t2_deferred_checkpoints(checkpoint_id,request_id,workspace_id,mission_id,plan_id,revision,sequence,previous_checkpoint_id,reason,next_check_condition,next_check_at_ms,recorded_at_ms,accountable_owner_activity_id,accountable_owner_agent_id,accountable_owner_kind,accountable_owner_key,accountable_owner_display_name,accountable_owner_participation_id,accountable_owner_profile_id,accountable_owner_participant_id,accountable_owner_session_id,accountable_owner_leadership_revision) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22)",
        params![
            checkpoint.checkpoint_id.into_uuid().as_bytes(),
            checkpoint.request_id.as_bytes(),
            checkpoint.workspace_id.into_uuid().as_bytes(),
            checkpoint.mission_id.into_uuid().as_bytes(),
            checkpoint.plan_id.into_uuid().as_bytes(),
            i64::try_from(checkpoint.revision)
                .map_err(|_| OperationsError::Bounds("mission T2 plan revision"))?,
            sequence,
            checkpoint.previous_checkpoint_id.map(|id| id.into_uuid().into_bytes().to_vec()),
            checkpoint.reason,
            checkpoint.next_check_condition,
            checkpoint.next_check_at_ms,
            checkpoint.recorded_at_ms,
            owner.activity_id.into_uuid().as_bytes(),
            owner.agent_id.into_uuid().as_bytes(),
            owner.actor_kind,
            owner.actor_key,
            owner.display_name,
            owner.participation_id.map(|id| id.into_uuid().into_bytes().to_vec()),
            owner.profile_id.map(Uuid::into_bytes),
            Some(owner.participant_id.into_bytes().to_vec()),
            Some(owner.session_id.into_uuid().into_bytes()),
            owner.leadership_revision,
        ],
    )?;
    Ok(())
}

fn load_by_request(
    tx: &Transaction<'_>,
    request_id: Uuid,
) -> Result<Option<MissionT2DeferredCheckpoint>, OperationsError> {
    load_checkpoint(tx, "request_id", request_id)
}

fn load_by_request_in_workspace(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    request_id: Uuid,
) -> Result<Option<MissionT2DeferredCheckpoint>, OperationsError> {
    let scope = tx
        .query_row(
            "SELECT substr(plan_id,1,17),revision,substr(checkpoint_id,1,17) FROM ops_mission_evaluation_t2_deferred_checkpoints WHERE request_id=?1 AND workspace_id=?2 LIMIT 1",
            params![request_id.as_bytes(), workspace_id.into_uuid().as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            },
        )
        .optional()?;
    let Some((plan_id, revision, checkpoint_id)) = scope else {
        return Ok(None);
    };
    let plan_id =
        MissionEvaluationPlanId::from_uuid(decode_uuid(plan_id, "mission T2 plan identity")?)
            .map_err(|_| OperationsError::Corrupt("mission T2 plan identity"))?;
    let revision = decode_revision(revision, "mission T2 plan revision")?;
    let checkpoint_id = MissionT2CheckpointId::from_uuid(decode_uuid(
        checkpoint_id,
        "mission T2 checkpoint identity",
    )?)
    .map_err(|_| OperationsError::Corrupt("mission T2 checkpoint identity"))?;
    load_by_id(tx, workspace_id, plan_id, revision, checkpoint_id)?
        .ok_or(OperationsError::Corrupt(
            "mission T2 scoped request receipt missing",
        ))
        .map(Some)
}

fn load_by_id(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    plan_id: MissionEvaluationPlanId,
    revision: u64,
    checkpoint_id: MissionT2CheckpointId,
) -> Result<Option<MissionT2DeferredCheckpoint>, OperationsError> {
    let matching_scope: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_mission_evaluation_t2_deferred_checkpoints WHERE checkpoint_id=?1 AND workspace_id=?2 AND plan_id=?3 AND revision=?4)",
        params![
            checkpoint_id.into_uuid().as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            plan_id.into_uuid().as_bytes(),
            i64::try_from(revision).map_err(|_| OperationsError::Bounds("mission T2 plan revision"))?,
        ],
        |row| row.get(0),
    )?;
    if !matching_scope {
        return Ok(None);
    }
    load_checkpoint(tx, "checkpoint_id", checkpoint_id.into_uuid())?.map_or_else(
        || {
            Err(OperationsError::Corrupt(
                "mission T2 checkpoint disappeared",
            ))
        },
        |record| {
            if record.workspace_id == workspace_id
                && record.plan_id == plan_id
                && record.revision == revision
            {
                Ok(Some(record))
            } else {
                Err(OperationsError::Corrupt("mission T2 checkpoint scope"))
            }
        },
    )
}

fn load_latest(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    plan_id: MissionEvaluationPlanId,
    revision: u64,
) -> Result<Option<MissionT2DeferredCheckpoint>, OperationsError> {
    let sequence = tx
        .query_row(
            "SELECT MAX(sequence) FROM ops_mission_evaluation_t2_deferred_checkpoints WHERE workspace_id=?1 AND plan_id=?2 AND revision=?3",
            params![
                workspace_id.into_uuid().as_bytes(),
                plan_id.into_uuid().as_bytes(),
                i64::try_from(revision).map_err(|_| OperationsError::Bounds("mission T2 plan revision"))?,
            ],
            |row| row.get::<_, Option<i64>>(0),
        )?;
    let Some(sequence) = sequence else {
        return Ok(None);
    };
    let sequence = u64::try_from(sequence)
        .map_err(|_| OperationsError::Corrupt("mission T2 defer sequence"))?;
    let raw = tx
        .query_row(
            "SELECT substr(checkpoint_id,1,17) FROM ops_mission_evaluation_t2_deferred_checkpoints WHERE workspace_id=?1 AND plan_id=?2 AND revision=?3 AND sequence=?4 LIMIT 1",
            params![
                workspace_id.into_uuid().as_bytes(),
                plan_id.into_uuid().as_bytes(),
                i64::try_from(revision).map_err(|_| OperationsError::Bounds("mission T2 plan revision"))?,
                i64::try_from(sequence).map_err(|_| OperationsError::Bounds("mission T2 defer sequence"))?,
            ],
            |row| row.get::<_, Vec<u8>>(0),
        )?;
    let checkpoint_id =
        MissionT2CheckpointId::from_uuid(decode_uuid(raw, "mission T2 checkpoint identity")?)
            .map_err(|_| OperationsError::Corrupt("mission T2 checkpoint identity"))?;
    load_by_id(tx, workspace_id, plan_id, revision, checkpoint_id)?
        .ok_or(OperationsError::Corrupt(
            "mission T2 latest checkpoint missing",
        ))
        .map(Some)
}

fn load_checkpoint(
    tx: &Transaction<'_>,
    selector: &'static str,
    key: Uuid,
) -> Result<Option<MissionT2DeferredCheckpoint>, OperationsError> {
    let sql = match selector {
        "request_id" => {
            "SELECT substr(request_id,1,17),substr(checkpoint_id,1,17),substr(workspace_id,1,17),substr(mission_id,1,17),substr(plan_id,1,17),revision,sequence,substr(previous_checkpoint_id,1,17),reason,next_check_condition,next_check_at_ms,recorded_at_ms,substr(accountable_owner_activity_id,1,17),substr(accountable_owner_agent_id,1,17),accountable_owner_kind,accountable_owner_key,accountable_owner_display_name,substr(accountable_owner_participation_id,1,17),substr(accountable_owner_profile_id,1,17),substr(accountable_owner_participant_id,1,17),substr(accountable_owner_session_id,1,17),accountable_owner_leadership_revision FROM ops_mission_evaluation_t2_deferred_checkpoints WHERE request_id=?1 LIMIT 1"
        }
        "checkpoint_id" => {
            "SELECT substr(request_id,1,17),substr(checkpoint_id,1,17),substr(workspace_id,1,17),substr(mission_id,1,17),substr(plan_id,1,17),revision,sequence,substr(previous_checkpoint_id,1,17),reason,next_check_condition,next_check_at_ms,recorded_at_ms,substr(accountable_owner_activity_id,1,17),substr(accountable_owner_agent_id,1,17),accountable_owner_kind,accountable_owner_key,accountable_owner_display_name,substr(accountable_owner_participation_id,1,17),substr(accountable_owner_profile_id,1,17),substr(accountable_owner_participant_id,1,17),substr(accountable_owner_session_id,1,17),accountable_owner_leadership_revision FROM ops_mission_evaluation_t2_deferred_checkpoints WHERE checkpoint_id=?1 LIMIT 1"
        }
        _ => return Err(OperationsError::Corrupt("mission T2 checkpoint selector")),
    };
    let raw = tx
        .query_row(sql, params![key.as_bytes()], |row| {
            Ok(RawCheckpoint {
                request_id: row.get(0)?,
                checkpoint_id: row.get(1)?,
                workspace_id: row.get(2)?,
                mission_id: row.get(3)?,
                plan_id: row.get(4)?,
                revision: row.get(5)?,
                sequence: row.get(6)?,
                previous_checkpoint_id: row.get(7)?,
                reason: row.get(8)?,
                next_check_condition: row.get(9)?,
                next_check_at_ms: row.get(10)?,
                recorded_at_ms: row.get(11)?,
                owner_activity_id: row.get(12)?,
                owner_agent_id: row.get(13)?,
                owner_kind: row.get(14)?,
                owner_key: row.get(15)?,
                owner_display_name: row.get(16)?,
                owner_participation_id: row.get(17)?,
                owner_profile_id: row.get(18)?,
                owner_participant_id: row.get(19)?,
                owner_session_id: row.get(20)?,
                owner_leadership_revision: row.get(21)?,
            })
        })
        .optional()?;
    raw.map(RawCheckpoint::decode).transpose()
}

#[derive(Debug)]
struct RawCheckpoint {
    request_id: Vec<u8>,
    checkpoint_id: Vec<u8>,
    workspace_id: Vec<u8>,
    mission_id: Vec<u8>,
    plan_id: Vec<u8>,
    revision: i64,
    sequence: i64,
    previous_checkpoint_id: Option<Vec<u8>>,
    reason: String,
    next_check_condition: String,
    next_check_at_ms: i64,
    recorded_at_ms: i64,
    owner_activity_id: Vec<u8>,
    owner_agent_id: Vec<u8>,
    owner_kind: i64,
    owner_key: String,
    owner_display_name: String,
    owner_participation_id: Option<Vec<u8>>,
    owner_profile_id: Option<Vec<u8>>,
    owner_participant_id: Vec<u8>,
    owner_session_id: Vec<u8>,
    owner_leadership_revision: Option<i64>,
}

impl RawCheckpoint {
    fn decode(self) -> Result<MissionT2DeferredCheckpoint, OperationsError> {
        let accountable_owner_activity_id = ActivityId::from_uuid(decode_uuid(
            self.owner_activity_id,
            "mission T2 owner Activity",
        )?);
        let accountable_owner_agent_id =
            AgentInstanceId::from_uuid(decode_uuid(self.owner_agent_id, "mission T2 owner Agent")?);
        let accountable_owner = MissionT2AccountableOwner {
            activity_id: accountable_owner_activity_id,
            agent_id: accountable_owner_agent_id,
            session_id: AgentSessionId::from_uuid(decode_uuid(
                self.owner_session_id,
                "mission T2 owner Session",
            )?),
            actor_kind: self.owner_kind,
            actor_key: self.owner_key,
            participation_id: self
                .owner_participation_id
                .map(decode_participation)
                .transpose()?,
            profile_id: self
                .owner_profile_id
                .map(|value| decode_uuid(value, "mission T2 owner Profile"))
                .transpose()?,
            participant_id: decode_uuid(self.owner_participant_id, "mission T2 owner participant")?,
            display_name: self.owner_display_name,
            leadership_revision: self
                .owner_leadership_revision
                .ok_or(OperationsError::Corrupt(
                    "mission T2 owner leadership revision",
                ))?,
        };
        accountable_owner.validate()?;
        let checkpoint_id = MissionT2CheckpointId::from_uuid(decode_uuid(
            self.checkpoint_id,
            "mission T2 checkpoint identity",
        )?)
        .map_err(|_| OperationsError::Corrupt("mission T2 checkpoint identity"))?;
        let previous_checkpoint_id = self
            .previous_checkpoint_id
            .map(|value| {
                MissionT2CheckpointId::from_uuid(decode_uuid(
                    value,
                    "mission T2 predecessor identity",
                )?)
                .map_err(|_| OperationsError::Corrupt("mission T2 predecessor identity"))
            })
            .transpose()?;
        let record = MissionT2DeferredCheckpoint {
            request_id: decode_uuid(self.request_id, "mission T2 request identity")?,
            checkpoint_id,
            workspace_id: decode_workspace(self.workspace_id)?,
            mission_id: MissionId::from_uuid(decode_uuid(
                self.mission_id,
                "mission T2 Mission identity",
            )?)
            .map_err(|_| OperationsError::Corrupt("mission T2 Mission identity"))?,
            plan_id: MissionEvaluationPlanId::from_uuid(decode_uuid(
                self.plan_id,
                "mission T2 plan identity",
            )?)
            .map_err(|_| OperationsError::Corrupt("mission T2 plan identity"))?,
            revision: decode_revision(self.revision, "mission T2 plan revision")?,
            sequence: decode_revision(self.sequence, "mission T2 sequence")?,
            previous_checkpoint_id,
            reason: self.reason,
            next_check_condition: self.next_check_condition,
            next_check_at_ms: self.next_check_at_ms,
            recorded_at_ms: self.recorded_at_ms,
            accountable_owner,
        };
        validate_checkpoint(&record)?;
        Ok(record)
    }
}

fn decode_uuid(bytes: Vec<u8>, label: &'static str) -> Result<Uuid, OperationsError> {
    let value = Uuid::from_bytes(wire::id(bytes)?);
    if value.is_nil() {
        return Err(OperationsError::Corrupt(label));
    }
    Ok(value)
}

fn decode_workspace(bytes: Vec<u8>) -> Result<WorkspaceId, OperationsError> {
    let value = decode_uuid(bytes, "mission T2 Workspace identity")?;
    Ok(WorkspaceId::from_uuid(value))
}

fn decode_participation(bytes: Vec<u8>) -> Result<WorkspaceParticipationId, OperationsError> {
    WorkspaceParticipationId::from_uuid(decode_uuid(bytes, "mission T2 owner participation")?)
        .map_err(|_| OperationsError::Corrupt("mission T2 owner participation"))
}

fn decode_revision(value: i64, label: &'static str) -> Result<u64, OperationsError> {
    u64::try_from(value).map_err(|_| OperationsError::Corrupt(label))
}
