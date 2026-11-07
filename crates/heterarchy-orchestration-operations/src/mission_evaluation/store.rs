use std::collections::BTreeMap;

use forge_domain::{AgentSessionId, WorkspaceId};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params, params_from_iter};
use uuid::Uuid;

use crate::{
    OperationsError, WorkspaceParticipationId, catalog, leadership, operational_actor, teams, wire,
};

use super::{
    MissionCreateCommand, MissionCreateOutcome, MissionEvaluationAuthority,
    MissionEvaluationPlanId, MissionEvaluationPlanObligation, MissionEvaluationPlanObligationInput,
    MissionEvaluationPlanPublishCommand, MissionEvaluationPlanPublishOutcome,
    MissionEvaluationPlanStartCommand, MissionEvaluationPlanStartOutcome,
    MissionEvaluationPlanStartRecord, MissionEvaluationPlanVersion, MissionEvaluationPlanVersionId,
    MissionEvaluationRecordedMutation, MissionEvaluationRequestRecord, MissionId,
    MissionObligationId, MissionObligationRecord, MissionObligationRecordCommand,
    MissionObligationRecordOutcome, MissionRecord, MissionWorkUnitId, PositiveWeight,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestOperation {
    MissionCreate,
    ObligationRecord,
    PlanPublish,
    PlanStart,
}

impl RequestOperation {
    const fn encode(self) -> i64 {
        match self {
            Self::MissionCreate => 0,
            Self::ObligationRecord => 1,
            Self::PlanPublish => 2,
            Self::PlanStart => 3,
        }
    }

    fn decode(value: i64) -> Result<Self, OperationsError> {
        match value {
            0 => Ok(Self::MissionCreate),
            1 => Ok(Self::ObligationRecord),
            2 => Ok(Self::PlanPublish),
            3 => Ok(Self::PlanStart),
            _ => Err(OperationsError::Corrupt(
                "mission evaluation request operation",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ActorIdentity {
    pub(crate) kind: i64,
    pub(crate) key: String,
    pub(crate) participation_id: Option<WorkspaceParticipationId>,
    pub(crate) profile_id: Option<Uuid>,
    pub(crate) participant_id: Option<Uuid>,
}

impl ActorIdentity {
    pub(crate) fn resolve(
        tx: &Transaction<'_>,
        workspace_id: WorkspaceId,
        actor_input: crate::OperationalActor,
    ) -> Result<(Self, operational_actor::ResolvedOperationalActor), OperationsError> {
        let actor = operational_actor::resolve_actor(tx, workspace_id, actor_input)?;
        let identity = Self {
            kind: if actor.is_operator() {
                0
            } else if actor.participation_id().is_some() {
                1
            } else {
                2
            },
            key: actor.key(),
            participation_id: actor.participation_id(),
            profile_id: actor.profile_id(),
            participant_id: actor.participant_id(),
        };
        identity.validate()?;
        Ok((identity, actor))
    }

    pub(crate) fn validate(&self) -> Result<(), OperationsError> {
        if self.key.is_empty() || self.key.len() > 256 || self.key.as_bytes().contains(&0) {
            return Err(OperationsError::Corrupt(
                "mission evaluation actor identity",
            ));
        }
        let fields_are_valid = match self.kind {
            0 => {
                self.participation_id.is_none()
                    && self.profile_id.is_none()
                    && self.participant_id.is_none()
            }
            1 => {
                self.participation_id.is_some()
                    && self.profile_id.is_some_and(|id| !id.is_nil())
                    && self.participant_id.is_some_and(|id| !id.is_nil())
            }
            2 => {
                self.participation_id.is_none()
                    && self.profile_id.is_none()
                    && self.participant_id.is_some_and(|id| !id.is_nil())
            }
            _ => false,
        };
        if !fields_are_valid {
            return Err(OperationsError::Corrupt(
                "mission evaluation actor identity",
            ));
        }
        let expected_key = match self.kind {
            0 => "operator".to_owned(),
            1 => format!(
                "participant:{}",
                self.participation_id.ok_or(OperationsError::Corrupt(
                    "mission evaluation actor participation",
                ))?
            ),
            2 => format!(
                "native-participant:{}",
                self.participant_id.ok_or(OperationsError::Corrupt(
                    "mission evaluation actor participant",
                ))?
            ),
            _ => return Err(OperationsError::Corrupt("mission evaluation actor kind")),
        };
        if self.key == expected_key {
            Ok(())
        } else {
            Err(OperationsError::Corrupt("mission evaluation actor key"))
        }
    }

    pub(crate) fn agrees_with(&self, other: &Self) -> bool {
        self == other
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ActorSnapshot {
    pub(crate) identity: ActorIdentity,
    pub(crate) display_name: String,
    pub(crate) session_id: Option<AgentSessionId>,
    pub(crate) leadership_revision: Option<i64>,
}

impl ActorSnapshot {
    pub(crate) fn resolve(
        tx: &Transaction<'_>,
        workspace_id: WorkspaceId,
        authority: MissionEvaluationAuthority,
    ) -> Result<Self, OperationsError> {
        let (identity, actor) = ActorIdentity::resolve(tx, workspace_id, authority.actor)?;
        match (
            authority.actor,
            authority.expected_workspace_leadership_revision,
        ) {
            (crate::OperationalActor::LocalOperator, None) if actor.is_operator() => {}
            (crate::OperationalActor::Participant { .. }, Some(revision))
                if !actor.is_operator() && revision >= 1 => {}
            _ => {
                return Err(OperationsError::Conflict(
                    "mission evaluation actor authority",
                ));
            }
        }

        let snapshot = Self {
            identity,
            display_name: actor.display_name().to_owned(),
            session_id: actor.session_id(),
            leadership_revision: authority.expected_workspace_leadership_revision,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub(crate) fn authorize_current(
        &self,
        tx: &Transaction<'_>,
        workspace_id: WorkspaceId,
        authority: MissionEvaluationAuthority,
    ) -> Result<(), OperationsError> {
        match (
            authority.actor,
            authority.expected_workspace_leadership_revision,
        ) {
            (crate::OperationalActor::LocalOperator, None) if self.identity.kind == 0 => Ok(()),
            (crate::OperationalActor::Participant { .. }, Some(expected_revision)) => {
                let current = leadership::current_leadership_authority_in_transaction(
                    tx,
                    crate::LeadershipScope::workspace(workspace_id),
                )?
                .ok_or(OperationsError::Conflict(
                    "mission evaluation requires current Workspace leader",
                ))?;
                if self.identity.participant_id != Some(current.participant_id())
                    || expected_revision != current.leadership_revision()
                {
                    return Err(OperationsError::Conflict(
                        "mission evaluation Workspace leadership changed",
                    ));
                }
                Ok(())
            }
            _ => Err(OperationsError::Conflict(
                "mission evaluation actor authority",
            )),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), OperationsError> {
        self.identity.validate()?;
        if self.display_name.is_empty()
            || self.display_name.len() > 128
            || self.display_name.as_bytes().contains(&0)
        {
            return Err(OperationsError::Corrupt(
                "mission evaluation actor snapshot",
            ));
        }
        let fields_are_valid = match self.identity.kind {
            0 => self.session_id.is_none() && self.leadership_revision.is_none(),
            1 | 2 => {
                self.session_id.is_some_and(|id| !id.into_uuid().is_nil())
                    && self
                        .leadership_revision
                        .is_some_and(|revision| revision >= 1)
            }
            _ => false,
        };
        if !fields_are_valid {
            return Err(OperationsError::Corrupt(
                "mission evaluation actor snapshot",
            ));
        }
        Ok(())
    }

    pub(crate) fn agrees_with(&self, other: &Self) -> bool {
        self.identity.agrees_with(&other.identity)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RequestReceipt {
    request_id: Uuid,
    operation: RequestOperation,
    workspace_id: WorkspaceId,
    target_id: Uuid,
    revision: u64,
    actor: ActorSnapshot,
}

impl RequestReceipt {
    fn agrees_with(
        &self,
        request_id: Uuid,
        operation: RequestOperation,
        workspace_id: WorkspaceId,
        target_id: Uuid,
        revision: u64,
        actor: &ActorSnapshot,
    ) -> bool {
        self.request_id == request_id
            && self.operation == operation
            && self.workspace_id == workspace_id
            && self.target_id == target_id
            && self.revision == revision
            && self.actor.agrees_with(actor)
    }
}

fn insert_request(
    tx: &Transaction<'_>,
    request_id: Uuid,
    operation: RequestOperation,
    workspace_id: WorkspaceId,
    target_id: Uuid,
    revision: u64,
    actor: &ActorSnapshot,
) -> Result<(), OperationsError> {
    let revision = i64::try_from(revision)
        .map_err(|_| OperationsError::Bounds("mission evaluation request revision"))?;
    tx.execute(
        "INSERT INTO ops_mission_evaluation_requests(request_id,operation,workspace_id,target_id,revision,actor_kind,actor_key,actor_display_name,actor_participation_id,actor_profile_id,actor_participant_id,actor_session_id,expected_leadership_revision) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
        params![
            request_id.as_bytes(),
            operation.encode(),
            workspace_id.into_uuid().as_bytes(),
            target_id.as_bytes(),
            revision,
            actor.identity.kind,
            actor.identity.key,
            actor.display_name,
            actor
                .identity
                .participation_id
                .map(|id| id.into_uuid().into_bytes().to_vec()),
            actor.identity.profile_id.map(Uuid::into_bytes),
            actor.identity.participant_id.map(Uuid::into_bytes),
            actor.session_id.map(|id| id.into_uuid().into_bytes()),
            actor.leadership_revision,
        ],
    )?;
    Ok(())
}

fn load_request(
    connection: &rusqlite::Connection,
    request_id: Uuid,
) -> Result<Option<RequestReceipt>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT operation,substr(workspace_id,1,17),substr(target_id,1,17),revision,actor_kind,actor_key,actor_display_name,substr(actor_participation_id,1,17),substr(actor_profile_id,1,17),substr(actor_participant_id,1,17),substr(actor_session_id,1,17),expected_leadership_revision FROM ops_mission_evaluation_requests WHERE request_id=?1 LIMIT 1",
            params![request_id.as_bytes()],
            |row| {
                Ok(RawRequest {
                    operation: row.get(0)?,
                    workspace_id: row.get(1)?,
                    target_id: row.get(2)?,
                    revision: row.get(3)?,
                    actor_kind: row.get(4)?,
                    actor_key: row.get(5)?,
                    actor_display_name: row.get(6)?,
                    actor_participation_id: row.get(7)?,
                    actor_profile_id: row.get(8)?,
                    actor_participant_id: row.get(9)?,
                    actor_session_id: row.get(10)?,
                    leadership_revision: row.get(11)?,
                })
            },
        )
        .optional()?;
    raw.map(|value| value.decode(request_id)).transpose()
}

struct RawRequest {
    operation: i64,
    workspace_id: Vec<u8>,
    target_id: Vec<u8>,
    revision: i64,
    actor_kind: i64,
    actor_key: String,
    actor_display_name: String,
    actor_participation_id: Option<Vec<u8>>,
    actor_profile_id: Option<Vec<u8>>,
    actor_participant_id: Option<Vec<u8>>,
    actor_session_id: Option<Vec<u8>>,
    leadership_revision: Option<i64>,
}

impl RawRequest {
    fn decode(self, request_id: Uuid) -> Result<RequestReceipt, OperationsError> {
        let workspace_id = decode_workspace(self.workspace_id)?;
        let target_id = decode_uuid(self.target_id, "mission evaluation request target")?;
        if self.revision <= 0 {
            return Err(OperationsError::Corrupt(
                "mission evaluation request revision",
            ));
        }
        let actor = ActorSnapshot {
            identity: ActorIdentity {
                kind: self.actor_kind,
                key: self.actor_key,
                participation_id: self
                    .actor_participation_id
                    .map(decode_participation)
                    .transpose()?,
                profile_id: self
                    .actor_profile_id
                    .map(|bytes| decode_uuid(bytes, "mission evaluation actor Profile"))
                    .transpose()?,
                participant_id: self
                    .actor_participant_id
                    .map(|bytes| decode_uuid(bytes, "mission evaluation actor participant"))
                    .transpose()?,
            },
            display_name: self.actor_display_name,
            session_id: self.actor_session_id.map(decode_session).transpose()?,
            leadership_revision: self.leadership_revision,
        };
        actor.validate()?;
        Ok(RequestReceipt {
            request_id,
            operation: RequestOperation::decode(self.operation)?,
            workspace_id,
            target_id,
            revision: u64::try_from(self.revision)
                .map_err(|_| OperationsError::Corrupt("mission evaluation request revision"))?,
            actor,
        })
    }
}

fn commit_write(
    tx: Transaction<'_>,
    workspace_id: WorkspaceId,
    request_id: Uuid,
) -> Result<(), OperationsError> {
    tx.commit()
        .map_err(|source| OperationsError::OperationCommitUnconfirmed {
            workspace_id,
            request_id,
            source,
        })
}

fn commit_read(tx: Transaction<'_>, destination: Uuid) -> Result<(), OperationsError> {
    catalog::commit_read(tx, destination)
}

fn ensure_workspace(
    tx: &rusqlite::Connection,
    workspace_id: WorkspaceId,
) -> Result<(), OperationsError> {
    if workspace_id.into_uuid().is_nil() {
        return Err(OperationsError::Conflict(
            "mission evaluation Workspace identity",
        ));
    }
    teams::ensure_verified_workspace(tx, workspace_id)
}

fn decode_uuid(bytes: Vec<u8>, label: &'static str) -> Result<Uuid, OperationsError> {
    let value = Uuid::from_bytes(wire::id(bytes)?);
    if value.is_nil() {
        return Err(OperationsError::Corrupt(label));
    }
    Ok(value)
}

fn decode_workspace(bytes: Vec<u8>) -> Result<WorkspaceId, OperationsError> {
    let value = Uuid::from_bytes(wire::id(bytes)?);
    if value.is_nil() {
        return Err(OperationsError::Corrupt("mission evaluation Workspace"));
    }
    Ok(WorkspaceId::from_uuid(value))
}

fn decode_participation(bytes: Vec<u8>) -> Result<WorkspaceParticipationId, OperationsError> {
    let value = decode_uuid(bytes, "mission evaluation participation")?;
    WorkspaceParticipationId::from_uuid(value)
        .map_err(|_| OperationsError::Corrupt("mission evaluation participation"))
}

fn decode_session(bytes: Vec<u8>) -> Result<AgentSessionId, OperationsError> {
    let value = decode_uuid(bytes, "mission evaluation actor Session")?;
    Ok(AgentSessionId::from_uuid(value))
}

fn valid_stored_text(value: &str, maximum: usize) -> bool {
    !value.trim().is_empty() && value.len() <= maximum && !value.as_bytes().contains(&0)
}

fn decode_revision(value: i64, label: &'static str) -> Result<u64, OperationsError> {
    if value <= 0 {
        return Err(OperationsError::Corrupt(label));
    }
    u64::try_from(value).map_err(|_| OperationsError::Corrupt(label))
}

fn conflict_replay() -> OperationsError {
    OperationsError::Conflict("mission evaluation request replay changed")
}

fn request_matches(
    stored: &RequestReceipt,
    request_id: Uuid,
    operation: RequestOperation,
    workspace_id: WorkspaceId,
    target_id: Uuid,
    revision: u64,
    actor: &ActorSnapshot,
) -> Result<(), OperationsError> {
    if stored.agrees_with(
        request_id,
        operation,
        workspace_id,
        target_id,
        revision,
        actor,
    ) {
        Ok(())
    } else {
        Err(conflict_replay())
    }
}

fn verify_stored_request(
    connection: &rusqlite::Connection,
    request_id: Uuid,
    operation: RequestOperation,
    workspace_id: WorkspaceId,
    target_id: Uuid,
    revision: u64,
) -> Result<RequestReceipt, OperationsError> {
    let stored = load_request(connection, request_id)?.ok_or(OperationsError::Corrupt(
        "mission evaluation request receipt",
    ))?;
    if stored.operation != operation
        || stored.workspace_id != workspace_id
        || stored.target_id != target_id
        || stored.revision != revision
    {
        return Err(OperationsError::Corrupt(
            "mission evaluation request receipt scope",
        ));
    }
    Ok(stored)
}

#[derive(Debug)]
struct RawMission {
    mission_id: Vec<u8>,
    workspace_id: Vec<u8>,
    team_id: Vec<u8>,
    create_request_id: Vec<u8>,
    purpose: String,
    created_at_ms: i64,
    revision: i64,
}

impl RawMission {
    fn decode(self) -> Result<(MissionRecord, Uuid), OperationsError> {
        let mission_id = MissionId::from_uuid(decode_uuid(
            self.mission_id,
            "mission evaluation Mission identity",
        )?)
        .map_err(|_| OperationsError::Corrupt("mission evaluation Mission identity"))?;
        let workspace_id = decode_workspace(self.workspace_id)?;
        let team_id = decode_uuid(self.team_id, "mission evaluation Team identity")?;
        let create_request_id = decode_uuid(
            self.create_request_id,
            "mission evaluation Mission request identity",
        )?;
        if !valid_stored_text(&self.purpose, super::MAX_PURPOSE_BYTES)
            || self.created_at_ms < 0
            || self.revision != 1
        {
            return Err(OperationsError::Corrupt("mission evaluation Mission"));
        }
        let record = MissionRecord {
            mission_id,
            workspace_id,
            team_id,
            purpose: self.purpose,
            created_at_ms: self.created_at_ms,
            revision: decode_revision(self.revision, "mission evaluation Mission revision")?,
        };
        Ok((record, create_request_id))
    }
}

fn load_mission(
    connection: &rusqlite::Connection,
    mission_id: MissionId,
) -> Result<Option<MissionRecord>, OperationsError> {
    Ok(load_mission_with_request(connection, mission_id)?.map(|(record, _request_id)| record))
}

fn load_mission_with_request(
    connection: &rusqlite::Connection,
    mission_id: MissionId,
) -> Result<Option<(MissionRecord, Uuid)>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(mission_id,1,17),substr(workspace_id,1,17),substr(team_id,1,17),substr(create_request_id,1,17),purpose,created_at_ms,revision FROM ops_mission_evaluation_missions WHERE mission_id=?1 LIMIT 1",
            params![mission_id.into_uuid().as_bytes()],
            |row| {
                Ok(RawMission {
                    mission_id: row.get(0)?,
                    workspace_id: row.get(1)?,
                    team_id: row.get(2)?,
                    create_request_id: row.get(3)?,
                    purpose: row.get(4)?,
                    created_at_ms: row.get(5)?,
                    revision: row.get(6)?,
                })
            },
        )
        .optional()?;
    raw.map(|row| {
        let (record, request_id) = row.decode()?;
        verify_stored_request(
            connection,
            request_id,
            RequestOperation::MissionCreate,
            record.workspace_id,
            mission_id.into_uuid(),
            record.revision,
        )?;
        Ok((record, request_id))
    })
    .transpose()
}

pub(super) fn create_mission(
    connection: &mut rusqlite::Connection,
    destination: Uuid,
    command: &MissionCreateCommand,
) -> Result<MissionCreateOutcome, OperationsError> {
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    ensure_workspace(&tx, command.workspace_id)?;
    let actor = ActorSnapshot::resolve(&tx, command.workspace_id, command.authority)?;

    if let Some(existing) = load_request(&tx, command.request_id)? {
        request_matches(
            &existing,
            command.request_id,
            RequestOperation::MissionCreate,
            command.workspace_id,
            command.mission_id.into_uuid(),
            1,
            &actor,
        )?;
        let mission = load_mission(&tx, command.mission_id)?.ok_or(OperationsError::Corrupt(
            "mission evaluation Mission receipt",
        ))?;
        if mission.workspace_id != command.workspace_id
            || mission.team_id != command.team_id
            || mission.purpose != command.purpose
        {
            return Err(conflict_replay());
        }
        catalog::commit_read(tx, destination)?;
        return Ok(MissionCreateOutcome {
            mission,
            replayed: true,
        });
    }
    actor.authorize_current(&tx, command.workspace_id, command.authority)?;

    if load_mission(&tx, command.mission_id)?.is_some() {
        return Err(OperationsError::Conflict(
            "mission evaluation Mission identity",
        ));
    }
    require_active_team(&tx, command.workspace_id, command.team_id)?;
    let active_mission_exists: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_mission_evaluation_team_heads WHERE workspace_id=?1 AND team_id=?2)",
        params![command.workspace_id.into_uuid().as_bytes(), command.team_id.as_bytes()],
        |row| row.get(0),
    )?;
    if active_mission_exists {
        return Err(OperationsError::Conflict(
            "active Mission already exists for Team",
        ));
    }

    insert_request(
        &tx,
        command.request_id,
        RequestOperation::MissionCreate,
        command.workspace_id,
        command.mission_id.into_uuid(),
        1,
        &actor,
    )?;
    tx.execute(
        "INSERT INTO ops_mission_evaluation_missions(mission_id,workspace_id,team_id,create_request_id,purpose,created_at_ms,revision) VALUES(?1,?2,?3,?4,?5,?6,1)",
        params![
            command.mission_id.into_uuid().as_bytes(),
            command.workspace_id.into_uuid().as_bytes(),
            command.team_id.as_bytes(),
            command.request_id.as_bytes(),
            command.purpose,
            command.created_at_ms,
        ],
    )?;
    tx.execute(
        "INSERT INTO ops_mission_evaluation_team_heads(workspace_id,team_id,mission_id,revision) VALUES(?1,?2,?3,1)",
        params![
            command.workspace_id.into_uuid().as_bytes(),
            command.team_id.as_bytes(),
            command.mission_id.into_uuid().as_bytes(),
        ],
    )?;
    let mission = load_mission(&tx, command.mission_id)?.ok_or(OperationsError::Corrupt(
        "mission evaluation Mission readback",
    ))?;
    commit_write(tx, command.workspace_id, command.request_id)?;
    Ok(MissionCreateOutcome {
        mission,
        replayed: false,
    })
}

pub(super) fn read_mission(
    connection: &rusqlite::Connection,
    destination: Uuid,
    workspace_id: WorkspaceId,
    mission_id: MissionId,
) -> Result<Option<MissionRecord>, OperationsError> {
    let tx = connection.unchecked_transaction()?;
    ensure_workspace(&tx, workspace_id)?;
    let mission = load_mission(&tx, mission_id)?;
    if mission
        .as_ref()
        .is_some_and(|record| record.workspace_id != workspace_id)
    {
        return Err(OperationsError::Conflict("mission Workspace scope"));
    }
    commit_read(tx, destination)?;
    Ok(mission)
}

#[derive(Debug)]
struct RawObligation {
    obligation_id: Vec<u8>,
    workspace_id: Vec<u8>,
    mission_id: Vec<u8>,
    participation_id: Vec<u8>,
    work_unit_id: Vec<u8>,
    request_id: Vec<u8>,
    weight: i64,
    acceptance_criteria: String,
    due_at_ms: i64,
    recorded_at_ms: i64,
    revision: i64,
}

impl RawObligation {
    fn decode(self) -> Result<(MissionObligationRecord, Uuid), OperationsError> {
        let obligation_id = MissionObligationId::from_uuid(decode_uuid(
            self.obligation_id,
            "mission evaluation obligation identity",
        )?)
        .map_err(|_| OperationsError::Corrupt("mission evaluation obligation identity"))?;
        let workspace_id = decode_workspace(self.workspace_id)?;
        let mission_id = MissionId::from_uuid(decode_uuid(
            self.mission_id,
            "mission evaluation obligation Mission",
        )?)
        .map_err(|_| OperationsError::Corrupt("mission evaluation obligation Mission"))?;
        let participation_id = decode_participation(self.participation_id)?;
        let work_unit_id = MissionWorkUnitId::from_uuid(decode_uuid(
            self.work_unit_id,
            "mission evaluation obligation work unit",
        )?)
        .map_err(|_| OperationsError::Corrupt("mission evaluation obligation work unit"))?;
        let request_id = decode_uuid(
            self.request_id,
            "mission evaluation obligation request identity",
        )?;
        if !valid_stored_text(
            &self.acceptance_criteria,
            super::MAX_ACCEPTANCE_CRITERIA_BYTES,
        ) || self.due_at_ms < 0
            || self.recorded_at_ms < 0
            || self.revision != 1
        {
            return Err(OperationsError::Corrupt("mission evaluation obligation"));
        }
        let weight = PositiveWeight::new(
            u64::try_from(self.weight)
                .map_err(|_| OperationsError::Corrupt("mission evaluation obligation weight"))?,
        )
        .map_err(|_| OperationsError::Corrupt("mission evaluation obligation weight"))?;
        let record = MissionObligationRecord {
            obligation_id,
            workspace_id,
            mission_id,
            participation_id,
            work_unit_id,
            weight,
            acceptance_criteria: self.acceptance_criteria,
            due_at_ms: self.due_at_ms,
            recorded_at_ms: self.recorded_at_ms,
            revision: decode_revision(self.revision, "mission evaluation obligation revision")?,
        };
        Ok((record, request_id))
    }
}

fn load_obligation(
    connection: &rusqlite::Connection,
    obligation_id: MissionObligationId,
) -> Result<Option<MissionObligationRecord>, OperationsError> {
    Ok(
        load_obligation_with_request(connection, obligation_id)?
            .map(|(record, _request_id)| record),
    )
}

fn load_obligation_with_request(
    connection: &rusqlite::Connection,
    obligation_id: MissionObligationId,
) -> Result<Option<(MissionObligationRecord, Uuid)>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(obligation_id,1,17),substr(workspace_id,1,17),substr(mission_id,1,17),substr(participation_id,1,17),substr(work_unit_id,1,17),substr(record_request_id,1,17),weight,acceptance_criteria,due_at_ms,recorded_at_ms,revision FROM ops_mission_evaluation_obligations WHERE obligation_id=?1 LIMIT 1",
            params![obligation_id.into_uuid().as_bytes()],
            |row| {
                Ok(RawObligation {
                    obligation_id: row.get(0)?,
                    workspace_id: row.get(1)?,
                    mission_id: row.get(2)?,
                    participation_id: row.get(3)?,
                    work_unit_id: row.get(4)?,
                    request_id: row.get(5)?,
                    weight: row.get(6)?,
                    acceptance_criteria: row.get(7)?,
                    due_at_ms: row.get(8)?,
                    recorded_at_ms: row.get(9)?,
                    revision: row.get(10)?,
                })
            },
        )
        .optional()?;
    raw.map(|row| {
        let (record, request_id) = row.decode()?;
        verify_stored_request(
            connection,
            request_id,
            RequestOperation::ObligationRecord,
            record.workspace_id,
            record.obligation_id.into_uuid(),
            record.revision,
        )?;
        Ok((record, request_id))
    })
    .transpose()
}

fn same_obligation_command(
    record: &MissionObligationRecord,
    command: &MissionObligationRecordCommand,
) -> bool {
    record.obligation_id == command.obligation_id
        && record.workspace_id == command.workspace_id
        && record.mission_id == command.mission_id
        && record.participation_id == command.participation_id
        && record.work_unit_id == command.work_unit_id
        && record.weight == command.weight
        && record.acceptance_criteria == command.acceptance_criteria
        && record.due_at_ms == command.due_at_ms
        && record.revision == 1
}

fn require_joined_team_participant(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    participation_id: WorkspaceParticipationId,
) -> Result<(), OperationsError> {
    let joined: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_team_members AS tm JOIN ops_workspace_participations AS wp ON wp.workspace_id=tm.workspace_id AND wp.participation_id=tm.participation_id WHERE tm.workspace_id=?1 AND tm.team_id=?2 AND tm.participation_id=?3 AND tm.state='JOINED')",
        params![
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes(),
            participation_id.into_uuid().as_bytes(),
        ],
        |row| row.get(0),
    )?;
    if joined {
        Ok(())
    } else {
        Err(OperationsError::Conflict(
            "mission obligation participant is not joined to exact Team",
        ))
    }
}

fn require_active_team(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    team_id: Uuid,
) -> Result<(), OperationsError> {
    let state = tx
        .query_row(
            "SELECT state FROM ops_teams WHERE workspace_id=?1 AND team_id=?2 LIMIT 1",
            params![workspace_id.into_uuid().as_bytes(), team_id.as_bytes()],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    match state.as_deref() {
        Some("ACTIVE") => Ok(()),
        Some("RETIRED") => Err(OperationsError::Conflict("mission evaluation Team retired")),
        Some(_) => Err(OperationsError::Corrupt("mission evaluation Team state")),
        None => Err(OperationsError::Conflict("mission evaluation Team scope")),
    }
}

fn require_current_mission(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    mission_id: MissionId,
) -> Result<(), OperationsError> {
    let is_current: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_mission_evaluation_team_heads WHERE workspace_id=?1 AND team_id=?2 AND mission_id=?3)",
        params![
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes(),
            mission_id.into_uuid().as_bytes(),
        ],
        |row| row.get(0),
    )?;
    if is_current {
        Ok(())
    } else {
        Err(OperationsError::Conflict(
            "mission is not current for exact Team",
        ))
    }
}

pub(super) fn record_obligation(
    connection: &mut rusqlite::Connection,
    destination: Uuid,
    command: &MissionObligationRecordCommand,
) -> Result<MissionObligationRecordOutcome, OperationsError> {
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    ensure_workspace(&tx, command.workspace_id)?;
    let actor = ActorSnapshot::resolve(&tx, command.workspace_id, command.authority)?;

    if let Some(existing) = load_request(&tx, command.request_id)? {
        request_matches(
            &existing,
            command.request_id,
            RequestOperation::ObligationRecord,
            command.workspace_id,
            command.obligation_id.into_uuid(),
            1,
            &actor,
        )?;
        let obligation = load_obligation(&tx, command.obligation_id)?.ok_or(
            OperationsError::Corrupt("mission evaluation obligation receipt"),
        )?;
        if !same_obligation_command(&obligation, command) {
            return Err(conflict_replay());
        }
        catalog::commit_read(tx, destination)?;
        return Ok(MissionObligationRecordOutcome {
            obligation,
            replayed: true,
        });
    }
    actor.authorize_current(&tx, command.workspace_id, command.authority)?;

    if load_obligation(&tx, command.obligation_id)?.is_some() {
        return Err(OperationsError::Conflict(
            "mission evaluation obligation identity",
        ));
    }
    let mission = load_mission(&tx, command.mission_id)?.ok_or(OperationsError::Conflict(
        "mission evaluation Mission missing",
    ))?;
    if mission.workspace_id != command.workspace_id {
        return Err(OperationsError::Conflict("mission Workspace scope"));
    }
    require_active_team(&tx, mission.workspace_id, mission.team_id)?;
    require_current_mission(
        &tx,
        command.workspace_id,
        mission.team_id,
        command.mission_id,
    )?;
    require_joined_team_participant(
        &tx,
        command.workspace_id,
        mission.team_id,
        command.participation_id,
    )?;

    let identity_exists: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_mission_evaluation_obligations WHERE workspace_id=?1 AND mission_id=?2 AND participation_id=?3 AND work_unit_id=?4)",
        params![
            command.workspace_id.into_uuid().as_bytes(),
            command.mission_id.into_uuid().as_bytes(),
            command.participation_id.into_uuid().as_bytes(),
            command.work_unit_id.into_uuid().as_bytes(),
        ],
        |row| row.get(0),
    )?;
    if identity_exists {
        return Err(OperationsError::Conflict(
            "mission personal obligation identity already exists",
        ));
    }

    insert_request(
        &tx,
        command.request_id,
        RequestOperation::ObligationRecord,
        command.workspace_id,
        command.obligation_id.into_uuid(),
        1,
        &actor,
    )?;
    tx.execute(
        "INSERT INTO ops_mission_evaluation_obligations(obligation_id,workspace_id,mission_id,participation_id,work_unit_id,record_request_id,weight,acceptance_criteria,due_at_ms,recorded_at_ms,revision) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,1)",
        params![
            command.obligation_id.into_uuid().as_bytes(),
            command.workspace_id.into_uuid().as_bytes(),
            command.mission_id.into_uuid().as_bytes(),
            command.participation_id.into_uuid().as_bytes(),
            command.work_unit_id.into_uuid().as_bytes(),
            command.request_id.as_bytes(),
            i64::try_from(command.weight.get())
                .map_err(|_| OperationsError::Bounds("mission obligation weight"))?,
            command.acceptance_criteria,
            command.due_at_ms,
            command.recorded_at_ms,
        ],
    )?;
    let obligation = load_obligation(&tx, command.obligation_id)?.ok_or(
        OperationsError::Corrupt("mission evaluation obligation readback"),
    )?;
    commit_write(tx, command.workspace_id, command.request_id)?;
    Ok(MissionObligationRecordOutcome {
        obligation,
        replayed: false,
    })
}

pub(super) fn read_obligation(
    connection: &rusqlite::Connection,
    destination: Uuid,
    workspace_id: WorkspaceId,
    obligation_id: MissionObligationId,
) -> Result<Option<MissionObligationRecord>, OperationsError> {
    let tx = connection.unchecked_transaction()?;
    ensure_workspace(&tx, workspace_id)?;
    let obligation = load_obligation(&tx, obligation_id)?;
    if obligation
        .as_ref()
        .is_some_and(|record| record.workspace_id != workspace_id)
    {
        return Err(OperationsError::Conflict("obligation Workspace scope"));
    }
    commit_read(tx, destination)?;
    Ok(obligation)
}

#[derive(Debug)]
struct RawPlanHead {
    plan_id: Vec<u8>,
    workspace_id: Vec<u8>,
    mission_id: Vec<u8>,
    head_revision: i64,
}

#[derive(Debug, Clone, Copy)]
struct PlanHead {
    plan_id: MissionEvaluationPlanId,
    workspace_id: WorkspaceId,
    mission_id: MissionId,
    head_revision: u64,
}

impl RawPlanHead {
    fn decode(self) -> Result<PlanHead, OperationsError> {
        let plan_id = MissionEvaluationPlanId::from_uuid(decode_uuid(
            self.plan_id,
            "mission evaluation plan identity",
        )?)
        .map_err(|_| OperationsError::Corrupt("mission evaluation plan identity"))?;
        let workspace_id = decode_workspace(self.workspace_id)?;
        let mission_id = MissionId::from_uuid(decode_uuid(
            self.mission_id,
            "mission evaluation plan Mission",
        )?)
        .map_err(|_| OperationsError::Corrupt("mission evaluation plan Mission"))?;
        let head_revision =
            decode_revision(self.head_revision, "mission evaluation plan head revision")?;
        Ok(PlanHead {
            plan_id,
            workspace_id,
            mission_id,
            head_revision,
        })
    }
}

fn load_plan_head_by_id(
    connection: &rusqlite::Connection,
    plan_id: MissionEvaluationPlanId,
) -> Result<Option<PlanHead>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(plan_id,1,17),substr(workspace_id,1,17),substr(mission_id,1,17),head_revision FROM ops_mission_evaluation_plans WHERE plan_id=?1 LIMIT 1",
            params![plan_id.into_uuid().as_bytes()],
            |row| {
                Ok(RawPlanHead {
                    plan_id: row.get(0)?,
                    workspace_id: row.get(1)?,
                    mission_id: row.get(2)?,
                    head_revision: row.get(3)?,
                })
            },
        )
        .optional()?;
    let head = raw.map(RawPlanHead::decode).transpose()?;
    if head.is_some_and(|value| value.plan_id != plan_id) {
        return Err(OperationsError::Corrupt("mission evaluation plan identity"));
    }
    Ok(head)
}

#[derive(Debug)]
struct RawPlanVersion {
    plan_id: Vec<u8>,
    workspace_id: Vec<u8>,
    mission_id: Vec<u8>,
    revision: i64,
    expected_head_revision: i64,
    version_id: Vec<u8>,
    request_id: Vec<u8>,
    t0_commit_at_ms: i64,
    t1_submission_cutoff_ms: i64,
    t2_checkpoint_ms: i64,
    cohort_count: i64,
    sealed: i64,
}

fn load_plan_version_by_revision(
    connection: &rusqlite::Connection,
    plan_id: MissionEvaluationPlanId,
    revision: u64,
) -> Result<Option<MissionEvaluationPlanVersion>, OperationsError> {
    let revision_i64 = i64::try_from(revision)
        .map_err(|_| OperationsError::Bounds("mission evaluation plan revision"))?;
    let raw = connection
        .query_row(
            "SELECT substr(plan_id,1,17),substr(workspace_id,1,17),substr(mission_id,1,17),revision,expected_head_revision,substr(version_id,1,17),substr(request_id,1,17),t0_commit_at_ms,t1_submission_cutoff_ms,t2_checkpoint_ms,cohort_count,sealed FROM ops_mission_evaluation_plan_versions WHERE plan_id=?1 AND revision=?2 LIMIT 1",
            params![plan_id.into_uuid().as_bytes(), revision_i64],
            raw_plan_version,
        )
        .optional()?;
    raw.map(|value| decode_plan_version(connection, value).map(|(version, _)| version))
        .transpose()
}

fn load_plan_version_by_version_id(
    connection: &rusqlite::Connection,
    version_id: MissionEvaluationPlanVersionId,
) -> Result<Option<MissionEvaluationPlanVersion>, OperationsError> {
    Ok(
        load_plan_version_by_version_id_with_request(connection, version_id)?
            .map(|(version, _request_id)| version),
    )
}

fn load_plan_version_by_version_id_with_request(
    connection: &rusqlite::Connection,
    version_id: MissionEvaluationPlanVersionId,
) -> Result<Option<(MissionEvaluationPlanVersion, Uuid)>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(plan_id,1,17),substr(workspace_id,1,17),substr(mission_id,1,17),revision,expected_head_revision,substr(version_id,1,17),substr(request_id,1,17),t0_commit_at_ms,t1_submission_cutoff_ms,t2_checkpoint_ms,cohort_count,sealed FROM ops_mission_evaluation_plan_versions WHERE version_id=?1 LIMIT 1",
            params![version_id.into_uuid().as_bytes()],
            raw_plan_version,
        )
        .optional()?;
    raw.map(|value| decode_plan_version(connection, value))
        .transpose()
}

fn raw_plan_version(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawPlanVersion> {
    Ok(RawPlanVersion {
        plan_id: row.get(0)?,
        workspace_id: row.get(1)?,
        mission_id: row.get(2)?,
        revision: row.get(3)?,
        expected_head_revision: row.get(4)?,
        version_id: row.get(5)?,
        request_id: row.get(6)?,
        t0_commit_at_ms: row.get(7)?,
        t1_submission_cutoff_ms: row.get(8)?,
        t2_checkpoint_ms: row.get(9)?,
        cohort_count: row.get(10)?,
        sealed: row.get(11)?,
    })
}

#[derive(Debug)]
struct RawPlanCohortEntry {
    ordinal: i64,
    obligation_id: Vec<u8>,
    weight_snapshot: i64,
    current_weight: Option<i64>,
    workspace_id: Vec<u8>,
    mission_id: Vec<u8>,
}

fn decode_plan_version(
    connection: &rusqlite::Connection,
    raw: RawPlanVersion,
) -> Result<(MissionEvaluationPlanVersion, Uuid), OperationsError> {
    let plan_id = MissionEvaluationPlanId::from_uuid(decode_uuid(
        raw.plan_id,
        "mission evaluation plan identity",
    )?)
    .map_err(|_| OperationsError::Corrupt("mission evaluation plan identity"))?;
    let workspace_id = decode_workspace(raw.workspace_id)?;
    let mission_id = MissionId::from_uuid(decode_uuid(
        raw.mission_id,
        "mission evaluation plan Mission",
    )?)
    .map_err(|_| OperationsError::Corrupt("mission evaluation plan Mission"))?;
    let revision = decode_revision(raw.revision, "mission evaluation plan revision")?;
    if raw.expected_head_revision < 0
        || raw.expected_head_revision == i64::MAX
        || i64::try_from(revision).ok() != raw.expected_head_revision.checked_add(1)
    {
        return Err(OperationsError::Corrupt(
            "mission evaluation plan version revision chain",
        ));
    }
    let version_id = MissionEvaluationPlanVersionId::from_uuid(decode_uuid(
        raw.version_id,
        "mission evaluation plan version identity",
    )?)
    .map_err(|_| OperationsError::Corrupt("mission evaluation plan version identity"))?;
    let request_id = decode_uuid(raw.request_id, "mission evaluation plan request identity")?;
    let max_cohort_count = i64::try_from(super::MAX_PLAN_COHORT)
        .map_err(|_| OperationsError::Bounds("mission evaluation plan cohort"))?;
    if raw.t0_commit_at_ms < 0
        || raw.t0_commit_at_ms >= raw.t1_submission_cutoff_ms
        || raw.t1_submission_cutoff_ms > raw.t2_checkpoint_ms
        || raw.cohort_count < 0
        || raw.cohort_count > max_cohort_count
        || raw.sealed != 1
    {
        return Err(OperationsError::Corrupt("mission evaluation plan timeline"));
    }
    let cohort = load_plan_cohort(
        connection,
        plan_id,
        workspace_id,
        mission_id,
        revision,
        raw.cohort_count,
    )?;
    verify_stored_request(
        connection,
        request_id,
        RequestOperation::PlanPublish,
        workspace_id,
        version_id.into_uuid(),
        revision,
    )?;
    Ok((
        MissionEvaluationPlanVersion {
            plan_id,
            version_id,
            workspace_id,
            mission_id,
            revision,
            t0_commit_at_ms: raw.t0_commit_at_ms,
            t1_submission_cutoff_ms: raw.t1_submission_cutoff_ms,
            t2_checkpoint_ms: raw.t2_checkpoint_ms,
            cohort,
        },
        request_id,
    ))
}

fn load_plan_cohort(
    connection: &rusqlite::Connection,
    plan_id: MissionEvaluationPlanId,
    workspace_id: WorkspaceId,
    mission_id: MissionId,
    revision: u64,
    expected_count: i64,
) -> Result<Vec<MissionEvaluationPlanObligation>, OperationsError> {
    let revision_i64 = i64::try_from(revision)
        .map_err(|_| OperationsError::Corrupt("mission evaluation plan revision"))?;
    let mut statement = connection.prepare(
        "SELECT c.ordinal,substr(c.obligation_id,1,17),c.weight_snapshot,o.weight,substr(c.workspace_id,1,17),substr(c.mission_id,1,17) FROM ops_mission_evaluation_plan_cohort AS c LEFT JOIN ops_mission_evaluation_obligations AS o ON o.workspace_id=c.workspace_id AND o.mission_id=c.mission_id AND o.obligation_id=c.obligation_id WHERE c.plan_id=?1 AND c.revision=?2 ORDER BY c.ordinal LIMIT 257",
    )?;
    let rows = statement
        .query_map(
            params![plan_id.into_uuid().as_bytes(), revision_i64],
            |row| {
                Ok(RawPlanCohortEntry {
                    ordinal: row.get(0)?,
                    obligation_id: row.get(1)?,
                    weight_snapshot: row.get(2)?,
                    current_weight: row.get(3)?,
                    workspace_id: row.get(4)?,
                    mission_id: row.get(5)?,
                })
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    if rows.len() > super::MAX_PLAN_COHORT || i64::try_from(rows.len()).ok() != Some(expected_count)
    {
        return Err(OperationsError::Corrupt(
            "mission evaluation plan cohort count",
        ));
    }
    let mut cohort = Vec::with_capacity(rows.len());
    let mut previous_id = None;
    for (expected_ordinal, row) in rows.into_iter().enumerate() {
        if i64::try_from(expected_ordinal).ok() != Some(row.ordinal)
            || decode_workspace(row.workspace_id)? != workspace_id
            || MissionId::from_uuid(decode_uuid(
                row.mission_id,
                "mission evaluation plan cohort Mission",
            )?)
            .map_err(|_| OperationsError::Corrupt("mission evaluation plan cohort Mission"))?
                != mission_id
        {
            return Err(OperationsError::Corrupt(
                "mission evaluation plan cohort order or scope",
            ));
        }
        let obligation_id = MissionObligationId::from_uuid(decode_uuid(
            row.obligation_id,
            "mission evaluation plan obligation",
        )?)
        .map_err(|_| OperationsError::Corrupt("mission evaluation plan obligation"))?;
        if previous_id.is_some_and(|previous: Uuid| {
            previous.as_bytes() >= obligation_id.into_uuid().as_bytes()
        }) {
            return Err(OperationsError::Corrupt(
                "mission evaluation plan cohort ordering",
            ));
        }
        previous_id = Some(obligation_id.into_uuid());
        let weight = decode_positive_weight(row.weight_snapshot)?;
        let current_weight = row.current_weight.ok_or(OperationsError::Corrupt(
            "mission evaluation plan obligation missing",
        ))?;
        if decode_positive_weight(current_weight)? != weight {
            return Err(OperationsError::Corrupt(
                "mission evaluation plan weight snapshot",
            ));
        }
        cohort.push(MissionEvaluationPlanObligation {
            obligation_id,
            weight,
        });
    }
    Ok(cohort)
}

fn decode_positive_weight(value: i64) -> Result<PositiveWeight, OperationsError> {
    PositiveWeight::new(
        u64::try_from(value)
            .map_err(|_| OperationsError::Corrupt("mission evaluation obligation weight"))?,
    )
    .map_err(|_| OperationsError::Corrupt("mission evaluation obligation weight"))
}

fn load_plan_cohort_weights(
    connection: &rusqlite::Connection,
    workspace_id: WorkspaceId,
    mission_id: MissionId,
    cohort: &[MissionEvaluationPlanObligationInput],
) -> Result<BTreeMap<Uuid, PositiveWeight>, OperationsError> {
    if cohort.is_empty() {
        return Ok(BTreeMap::new());
    }
    let placeholders = (3..(cohort.len() + 3))
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT substr(obligation_id,1,17),weight FROM ops_mission_evaluation_obligations WHERE workspace_id=?1 AND mission_id=?2 AND obligation_id IN ({placeholders})"
    );
    let mut values = Vec::with_capacity(cohort.len() + 2);
    values.push(rusqlite::types::Value::Blob(
        workspace_id.into_uuid().into_bytes().to_vec(),
    ));
    values.push(rusqlite::types::Value::Blob(
        mission_id.into_uuid().into_bytes().to_vec(),
    ));
    values.extend(cohort.iter().map(|entry| {
        rusqlite::types::Value::Blob(entry.obligation_id.into_uuid().into_bytes().to_vec())
    }));
    let mut statement = connection.prepare(&sql)?;
    let rows = statement
        .query_map(params_from_iter(values), |row| {
            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    if rows.len() != cohort.len() {
        return Err(OperationsError::Conflict(
            "mission evaluation cohort obligation scope",
        ));
    }
    let mut weights = BTreeMap::new();
    for (id, weight) in rows {
        let id = decode_uuid(id, "mission evaluation cohort obligation")?;
        if weights
            .insert(id, decode_positive_weight(weight)?)
            .is_some()
        {
            return Err(OperationsError::Corrupt(
                "mission evaluation cohort duplicate obligation",
            ));
        }
    }
    Ok(weights)
}

fn same_plan_command(
    version: &MissionEvaluationPlanVersion,
    command: &MissionEvaluationPlanPublishCommand,
) -> bool {
    version.plan_id == command.plan_id
        && version.version_id == command.version_id
        && version.workspace_id == command.workspace_id
        && version.mission_id == command.mission_id
        && version.revision == command.expected_head_revision.saturating_add(1)
        && version.t1_submission_cutoff_ms == command.t1_submission_cutoff_ms
        && version.t2_checkpoint_ms == command.t2_checkpoint_ms
        && version.cohort.len() == command.cohort.len()
        && version
            .cohort
            .iter()
            .zip(&command.cohort)
            .all(|(stored, requested)| stored.obligation_id == requested.obligation_id)
}

fn plan_has_started(
    connection: &rusqlite::Connection,
    plan_id: MissionEvaluationPlanId,
) -> Result<bool, OperationsError> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_mission_evaluation_plan_starts WHERE plan_id=?1)",
        params![plan_id.into_uuid().as_bytes()],
        |row| row.get(0),
    )?)
}

struct PreparedPlanPublication {
    weights: BTreeMap<Uuid, PositiveWeight>,
    has_plan_head: bool,
    expected_head_revision: i64,
    revision: i64,
    cohort_count: i64,
}

fn prepare_plan_publication(
    tx: &Transaction<'_>,
    command: &MissionEvaluationPlanPublishCommand,
    revision: u64,
) -> Result<PreparedPlanPublication, OperationsError> {
    if command.t0_commit_at_ms >= command.t1_submission_cutoff_ms {
        return Err(OperationsError::Conflict(
            "mission evaluation plan timeline",
        ));
    }
    if load_plan_version_by_version_id(tx, command.version_id)?.is_some() {
        return Err(OperationsError::Conflict(
            "mission evaluation plan version identity",
        ));
    }
    let mission = load_mission(tx, command.mission_id)?.ok_or(OperationsError::Conflict(
        "mission evaluation Mission missing",
    ))?;
    if mission.workspace_id != command.workspace_id {
        return Err(OperationsError::Conflict("mission Workspace scope"));
    }
    require_active_team(tx, mission.workspace_id, mission.team_id)?;
    require_current_mission(
        tx,
        command.workspace_id,
        mission.team_id,
        command.mission_id,
    )?;

    let head = load_plan_head_by_id(tx, command.plan_id)?;
    match head {
        Some(current)
            if current.workspace_id == command.workspace_id
                && current.mission_id == command.mission_id
                && current.head_revision == command.expected_head_revision => {}
        Some(_) => {
            return Err(OperationsError::Conflict(
                "mission evaluation plan head revision",
            ));
        }
        None if command.expected_head_revision == 0 => {}
        None => {
            return Err(OperationsError::Conflict(
                "mission evaluation plan head missing",
            ));
        }
    }
    if plan_has_started(tx, command.plan_id)? {
        return Err(OperationsError::Conflict(
            "mission evaluation plan already started",
        ));
    }
    let weights = load_plan_cohort_weights(
        tx,
        command.workspace_id,
        command.mission_id,
        &command.cohort,
    )?;
    Ok(PreparedPlanPublication {
        weights,
        has_plan_head: head.is_some(),
        expected_head_revision: i64::try_from(command.expected_head_revision)
            .map_err(|_| OperationsError::Bounds("mission evaluation plan revision"))?,
        revision: i64::try_from(revision)
            .map_err(|_| OperationsError::Bounds("mission evaluation plan revision"))?,
        cohort_count: i64::try_from(command.cohort.len())
            .map_err(|_| OperationsError::Bounds("mission evaluation plan cohort"))?,
    })
}

pub(super) fn publish_plan(
    connection: &mut rusqlite::Connection,
    destination: Uuid,
    command: &MissionEvaluationPlanPublishCommand,
) -> Result<MissionEvaluationPlanPublishOutcome, OperationsError> {
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    ensure_workspace(&tx, command.workspace_id)?;
    let actor = ActorSnapshot::resolve(&tx, command.workspace_id, command.authority)?;
    let revision = command
        .expected_head_revision
        .checked_add(1)
        .ok_or(OperationsError::Bounds("mission evaluation plan revision"))?;

    if let Some(existing) = load_request(&tx, command.request_id)? {
        request_matches(
            &existing,
            command.request_id,
            RequestOperation::PlanPublish,
            command.workspace_id,
            command.version_id.into_uuid(),
            revision,
            &actor,
        )?;
        let version = load_plan_version_by_version_id(&tx, command.version_id)?.ok_or(
            OperationsError::Corrupt("mission evaluation plan version receipt"),
        )?;
        if !same_plan_command(&version, command) {
            return Err(conflict_replay());
        }
        catalog::commit_read(tx, destination)?;
        return Ok(MissionEvaluationPlanPublishOutcome {
            version,
            replayed: true,
        });
    }
    actor.authorize_current(&tx, command.workspace_id, command.authority)?;
    let prepared = prepare_plan_publication(&tx, command, revision)?;

    let version = insert_published_plan_version(&tx, command, &actor, revision, &prepared)?;
    commit_write(tx, command.workspace_id, command.request_id)?;
    Ok(MissionEvaluationPlanPublishOutcome {
        version,
        replayed: false,
    })
}

fn insert_published_plan_version(
    tx: &Transaction<'_>,
    command: &MissionEvaluationPlanPublishCommand,
    actor: &ActorSnapshot,
    revision: u64,
    prepared: &PreparedPlanPublication,
) -> Result<MissionEvaluationPlanVersion, OperationsError> {
    if !prepared.has_plan_head {
        tx.execute(
            "INSERT INTO ops_mission_evaluation_plans(plan_id,workspace_id,mission_id,head_revision) VALUES(?1,?2,?3,0)",
            params![
                command.plan_id.into_uuid().as_bytes(),
                command.workspace_id.into_uuid().as_bytes(),
                command.mission_id.into_uuid().as_bytes(),
            ],
        )?;
    }
    insert_request(
        tx,
        command.request_id,
        RequestOperation::PlanPublish,
        command.workspace_id,
        command.version_id.into_uuid(),
        revision,
        actor,
    )?;
    tx.execute(
        "INSERT INTO ops_mission_evaluation_plan_versions(plan_id,workspace_id,mission_id,revision,expected_head_revision,version_id,request_id,t0_commit_at_ms,t1_submission_cutoff_ms,t2_checkpoint_ms,cohort_count,sealed) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,0)",
        params![
            command.plan_id.into_uuid().as_bytes(),
            command.workspace_id.into_uuid().as_bytes(),
            command.mission_id.into_uuid().as_bytes(),
            prepared.revision,
            prepared.expected_head_revision,
            command.version_id.into_uuid().as_bytes(),
            command.request_id.as_bytes(),
            command.t0_commit_at_ms,
            command.t1_submission_cutoff_ms,
            command.t2_checkpoint_ms,
            prepared.cohort_count,
        ],
    )?;
    insert_plan_cohort(tx, command, &prepared.weights, prepared.revision)?;
    let sealed = tx.execute(
        "UPDATE ops_mission_evaluation_plan_versions SET sealed=1 WHERE plan_id=?1 AND revision=?2 AND sealed=0",
        params![command.plan_id.into_uuid().as_bytes(), prepared.revision],
    )?;
    if sealed != 1 {
        return Err(OperationsError::Corrupt(
            "mission evaluation plan version seal",
        ));
    }
    let changed = tx.execute(
        "UPDATE ops_mission_evaluation_plans SET head_revision=?1 WHERE plan_id=?2 AND workspace_id=?3 AND mission_id=?4 AND head_revision=?5",
        params![
            prepared.revision,
            command.plan_id.into_uuid().as_bytes(),
            command.workspace_id.into_uuid().as_bytes(),
            command.mission_id.into_uuid().as_bytes(),
            prepared.expected_head_revision,
        ],
    )?;
    if changed != 1 {
        return Err(OperationsError::Conflict(
            "mission evaluation plan head revision",
        ));
    }
    load_plan_version_by_revision(tx, command.plan_id, revision)?
        .ok_or(OperationsError::Corrupt("mission evaluation plan readback"))
}

fn insert_plan_cohort(
    tx: &Transaction<'_>,
    command: &MissionEvaluationPlanPublishCommand,
    weights: &BTreeMap<Uuid, PositiveWeight>,
    revision: i64,
) -> Result<(), OperationsError> {
    let mut insert = tx.prepare(
        "INSERT INTO ops_mission_evaluation_plan_cohort(plan_id,workspace_id,mission_id,revision,ordinal,obligation_id,weight_snapshot) VALUES(?1,?2,?3,?4,?5,?6,?7)",
    )?;
    for (ordinal, entry) in command.cohort.iter().enumerate() {
        let weight = weights
            .get(&entry.obligation_id.into_uuid())
            .copied()
            .ok_or(OperationsError::Conflict(
                "mission evaluation cohort obligation scope",
            ))?;
        insert.execute(params![
            command.plan_id.into_uuid().as_bytes(),
            command.workspace_id.into_uuid().as_bytes(),
            command.mission_id.into_uuid().as_bytes(),
            revision,
            i64::try_from(ordinal)
                .map_err(|_| OperationsError::Bounds("mission evaluation cohort ordinal"))?,
            entry.obligation_id.into_uuid().as_bytes(),
            i64::try_from(weight.get())
                .map_err(|_| OperationsError::Bounds("mission obligation weight"))?,
        ])?;
    }
    Ok(())
}

pub(crate) fn frozen_plan_in_transaction(
    connection: &rusqlite::Connection,
    workspace_id: WorkspaceId,
    plan_id: MissionEvaluationPlanId,
    revision: u64,
) -> Result<super::FrozenMissionEvaluationPlan, OperationsError> {
    if revision == 0 || revision > super::MAX_SQLITE_INTEGER {
        return Err(OperationsError::Bounds("mission evaluation plan revision"));
    }
    ensure_workspace(connection, workspace_id)?;
    let start = load_plan_start(connection, plan_id)?.ok_or(OperationsError::Conflict(
        "mission evidence requires a started plan",
    ))?;
    if start.workspace_id != workspace_id || start.revision != revision {
        return Err(OperationsError::Conflict(
            "mission evidence frozen plan scope",
        ));
    }
    let version = load_plan_version_by_revision(connection, plan_id, revision)?.ok_or(
        OperationsError::Corrupt("mission evidence started plan version missing"),
    )?;
    if version.workspace_id != workspace_id || version.mission_id != start.mission_id {
        return Err(OperationsError::Corrupt(
            "mission evidence started plan scope",
        ));
    }
    let mut obligations = Vec::with_capacity(version.cohort.len());
    for member in &version.cohort {
        let obligation = load_obligation(connection, member.obligation_id)?.ok_or(
            OperationsError::Corrupt("mission evidence obligation missing"),
        )?;
        if obligation.workspace_id != workspace_id
            || obligation.mission_id != version.mission_id
            || obligation.weight != member.weight
        {
            return Err(OperationsError::Corrupt(
                "mission evidence obligation scope",
            ));
        }
        obligations.push(obligation);
    }
    Ok(super::FrozenMissionEvaluationPlan {
        version,
        start,
        obligations,
    })
}

pub(super) fn read_plan_version(
    connection: &rusqlite::Connection,
    destination: Uuid,
    workspace_id: WorkspaceId,
    plan_id: MissionEvaluationPlanId,
    revision: u64,
) -> Result<Option<MissionEvaluationPlanVersion>, OperationsError> {
    if revision == 0 || revision > super::MAX_SQLITE_INTEGER {
        return Err(OperationsError::Bounds("mission evaluation plan revision"));
    }
    let tx = connection.unchecked_transaction()?;
    ensure_workspace(&tx, workspace_id)?;
    let head = load_plan_head_by_id(&tx, plan_id)?;
    if head.is_some_and(|value| value.workspace_id != workspace_id) {
        return Err(OperationsError::Conflict("evaluation plan Workspace scope"));
    }
    let version = load_plan_version_by_revision(&tx, plan_id, revision)?;
    if let Some(version) = version.as_ref() {
        let Some(head) = head else {
            return Err(OperationsError::Corrupt(
                "mission evaluation plan version has no head",
            ));
        };
        if version.workspace_id != workspace_id {
            return Err(OperationsError::Conflict("evaluation plan Workspace scope"));
        }
        if version.mission_id != head.mission_id || version.revision > head.head_revision {
            return Err(OperationsError::Corrupt(
                "mission evaluation plan version beyond head",
            ));
        }
    }
    commit_read(tx, destination)?;
    Ok(version)
}

#[derive(Debug)]
struct RawPlanStart {
    request_id: Vec<u8>,
    workspace_id: Vec<u8>,
    mission_id: Vec<u8>,
    plan_id: Vec<u8>,
    revision: i64,
    started_at_ms: i64,
}

impl RawPlanStart {
    fn decode(self) -> Result<MissionEvaluationPlanStartRecord, OperationsError> {
        let request_id = decode_uuid(self.request_id, "mission evaluation plan start request")?;
        let workspace_id = decode_workspace(self.workspace_id)?;
        let mission_id = MissionId::from_uuid(decode_uuid(
            self.mission_id,
            "mission evaluation plan start Mission",
        )?)
        .map_err(|_| OperationsError::Corrupt("mission evaluation plan start Mission"))?;
        let plan_id = MissionEvaluationPlanId::from_uuid(decode_uuid(
            self.plan_id,
            "mission evaluation plan start identity",
        )?)
        .map_err(|_| OperationsError::Corrupt("mission evaluation plan start identity"))?;
        if self.started_at_ms < 0 {
            return Err(OperationsError::Corrupt(
                "mission evaluation plan start time",
            ));
        }
        Ok(MissionEvaluationPlanStartRecord {
            request_id,
            workspace_id,
            mission_id,
            plan_id,
            revision: decode_revision(self.revision, "mission evaluation plan start revision")?,
            started_at_ms: self.started_at_ms,
        })
    }
}

fn load_plan_start(
    connection: &rusqlite::Connection,
    plan_id: MissionEvaluationPlanId,
) -> Result<Option<MissionEvaluationPlanStartRecord>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(request_id,1,17),substr(workspace_id,1,17),substr(mission_id,1,17),substr(plan_id,1,17),revision,started_at_ms FROM ops_mission_evaluation_plan_starts WHERE plan_id=?1 LIMIT 1",
            params![plan_id.into_uuid().as_bytes()],
            |row| {
                Ok(RawPlanStart {
                    request_id: row.get(0)?,
                    workspace_id: row.get(1)?,
                    mission_id: row.get(2)?,
                    plan_id: row.get(3)?,
                    revision: row.get(4)?,
                    started_at_ms: row.get(5)?,
                })
            },
        )
        .optional()?;
    raw.map(|row| {
        let record = row.decode()?;
        verify_stored_request(
            connection,
            record.request_id,
            RequestOperation::PlanStart,
            record.workspace_id,
            record.plan_id.into_uuid(),
            record.revision,
        )?;
        let head = load_plan_head_by_id(connection, record.plan_id)?.ok_or(
            OperationsError::Corrupt("mission evaluation plan start head"),
        )?;
        if head.workspace_id != record.workspace_id
            || head.mission_id != record.mission_id
            || head.head_revision != record.revision
        {
            return Err(OperationsError::Corrupt(
                "mission evaluation plan start head scope",
            ));
        }
        if load_plan_version_by_revision(connection, record.plan_id, record.revision)?.is_none() {
            return Err(OperationsError::Corrupt(
                "mission evaluation plan start version missing",
            ));
        }
        Ok(record)
    })
    .transpose()
}

pub(super) fn start_plan(
    connection: &mut rusqlite::Connection,
    destination: Uuid,
    command: &MissionEvaluationPlanStartCommand,
    trusted_now_ms: impl FnOnce() -> Result<i64, OperationsError>,
) -> Result<MissionEvaluationPlanStartOutcome, OperationsError> {
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    ensure_workspace(&tx, command.workspace_id)?;
    let actor = ActorSnapshot::resolve(&tx, command.workspace_id, command.authority)?;

    if let Some(existing) = load_request(&tx, command.request_id)? {
        return replay_plan_start(tx, destination, command, &actor, &existing);
    }

    start_new_plan(tx, command, &actor, trusted_now_ms)
}

fn replay_plan_start(
    tx: Transaction<'_>,
    destination: Uuid,
    command: &MissionEvaluationPlanStartCommand,
    actor: &ActorSnapshot,
    existing: &RequestReceipt,
) -> Result<MissionEvaluationPlanStartOutcome, OperationsError> {
    request_matches(
        existing,
        command.request_id,
        RequestOperation::PlanStart,
        command.workspace_id,
        command.plan_id.into_uuid(),
        command.expected_revision,
        actor,
    )?;
    let start = load_plan_start(&tx, command.plan_id)?.ok_or(OperationsError::Corrupt(
        "mission evaluation plan start receipt",
    ))?;
    if start.request_id != command.request_id
        || start.workspace_id != command.workspace_id
        || start.mission_id != command.mission_id
        || start.revision != command.expected_revision
    {
        return Err(conflict_replay());
    }
    catalog::commit_read(tx, destination)?;
    Ok(MissionEvaluationPlanStartOutcome {
        start,
        replayed: true,
    })
}

fn start_new_plan(
    tx: Transaction<'_>,
    command: &MissionEvaluationPlanStartCommand,
    actor: &ActorSnapshot,
    trusted_now_ms: impl FnOnce() -> Result<i64, OperationsError>,
) -> Result<MissionEvaluationPlanStartOutcome, OperationsError> {
    actor.authorize_current(&tx, command.workspace_id, command.authority)?;

    let head = load_plan_head_by_id(&tx, command.plan_id)?
        .ok_or(OperationsError::Conflict("mission evaluation plan missing"))?;
    if head.workspace_id != command.workspace_id {
        return Err(OperationsError::Conflict("evaluation plan Workspace scope"));
    }
    if head.mission_id != command.mission_id {
        return Err(OperationsError::Conflict("evaluation plan Mission scope"));
    }
    let mission = load_mission(&tx, head.mission_id)?.ok_or(OperationsError::Corrupt(
        "mission evaluation plan Mission missing",
    ))?;
    if mission.workspace_id != command.workspace_id {
        return Err(OperationsError::Corrupt(
            "mission evaluation plan Mission scope",
        ));
    }
    require_active_team(&tx, command.workspace_id, mission.team_id)?;
    require_current_mission(&tx, command.workspace_id, mission.team_id, head.mission_id)?;
    if head.head_revision != command.expected_revision {
        return Err(OperationsError::Conflict(
            "mission evaluation plan start revision",
        ));
    }
    if plan_has_started(&tx, command.plan_id)? {
        return Err(OperationsError::Conflict(
            "mission evaluation plan already started",
        ));
    }
    let version = load_plan_version_by_revision(&tx, command.plan_id, command.expected_revision)?
        .ok_or(OperationsError::Corrupt(
        "mission evaluation plan head version missing",
    ))?;
    if version.mission_id != command.mission_id {
        return Err(OperationsError::Corrupt(
            "mission evaluation plan head version Mission scope",
        ));
    }

    // Sample only after the IMMEDIATE lock and source checks. The frozen window
    // bounds this recorded sample; it does not certify physical clock
    // monotonicity or that commit completes before T1.
    let started_at_ms = trusted_now_ms()?;
    if started_at_ms < version.t0_commit_at_ms || started_at_ms >= version.t1_submission_cutoff_ms {
        return Err(OperationsError::Conflict(
            "mission evaluation plan start outside T0/T1 window",
        ));
    }

    insert_request(
        &tx,
        command.request_id,
        RequestOperation::PlanStart,
        command.workspace_id,
        command.plan_id.into_uuid(),
        command.expected_revision,
        actor,
    )?;
    tx.execute(
        "INSERT INTO ops_mission_evaluation_plan_starts(plan_id,workspace_id,mission_id,revision,request_id,started_at_ms) VALUES(?1,?2,?3,?4,?5,?6)",
        params![
            command.plan_id.into_uuid().as_bytes(),
            command.workspace_id.into_uuid().as_bytes(),
            head.mission_id.into_uuid().as_bytes(),
            i64::try_from(command.expected_revision)
                .map_err(|_| OperationsError::Bounds("mission evaluation plan revision"))?,
            command.request_id.as_bytes(),
            started_at_ms,
        ],
    )?;
    let start = load_plan_start(&tx, command.plan_id)?.ok_or(OperationsError::Corrupt(
        "mission evaluation plan start readback",
    ))?;
    commit_write(tx, command.workspace_id, command.request_id)?;
    Ok(MissionEvaluationPlanStartOutcome {
        start,
        replayed: false,
    })
}

pub(super) fn read_plan_start(
    connection: &rusqlite::Connection,
    destination: Uuid,
    workspace_id: WorkspaceId,
    plan_id: MissionEvaluationPlanId,
) -> Result<Option<MissionEvaluationPlanStartRecord>, OperationsError> {
    let tx = connection.unchecked_transaction()?;
    ensure_workspace(&tx, workspace_id)?;
    if load_plan_head_by_id(&tx, plan_id)?.is_some_and(|head| head.workspace_id != workspace_id) {
        return Err(OperationsError::Conflict("evaluation plan Workspace scope"));
    }
    let start = load_plan_start(&tx, plan_id)?;
    if start
        .as_ref()
        .is_some_and(|record| record.workspace_id != workspace_id)
    {
        return Err(OperationsError::Conflict("evaluation plan Workspace scope"));
    }
    commit_read(tx, destination)?;
    Ok(start)
}

pub(super) fn read_request_record(
    connection: &rusqlite::Connection,
    destination: Uuid,
    workspace_id: WorkspaceId,
    request_id: Uuid,
) -> Result<Option<MissionEvaluationRequestRecord>, OperationsError> {
    if request_id.is_nil() {
        return Err(OperationsError::Conflict(
            "mission evaluation request identity",
        ));
    }
    let tx = connection.unchecked_transaction()?;
    ensure_workspace(&tx, workspace_id)?;
    let Some(receipt) = load_request(&tx, request_id)? else {
        commit_read(tx, destination)?;
        return Ok(None);
    };
    if receipt.workspace_id != workspace_id {
        return Err(OperationsError::Conflict(
            "mission evaluation request Workspace scope",
        ));
    }
    let record = request_record_from_receipt(&tx, request_id, workspace_id, &receipt)?;
    commit_read(tx, destination)?;
    Ok(Some(record))
}

pub(super) fn read_request_record_for_actor(
    connection: &rusqlite::Connection,
    destination: Uuid,
    workspace_id: WorkspaceId,
    request_id: Uuid,
    actor: crate::OperationalActor,
) -> Result<Option<MissionEvaluationRequestRecord>, OperationsError> {
    if request_id.is_nil() {
        return Err(OperationsError::Conflict(
            "mission evaluation request identity",
        ));
    }
    let tx = connection.unchecked_transaction()?;
    ensure_workspace(&tx, workspace_id)?;
    let actor_identity = ActorIdentity::resolve(&tx, workspace_id, actor)?.0;
    let Some(receipt) = load_request(&tx, request_id)? else {
        commit_read(tx, destination)?;
        return Ok(None);
    };
    if receipt.workspace_id != workspace_id {
        return Err(OperationsError::Conflict(
            "mission evaluation request Workspace scope",
        ));
    }
    if !receipt.actor.identity.agrees_with(&actor_identity) {
        return Err(OperationsError::Conflict(
            "mission evaluation request actor scope",
        ));
    }
    let record = request_record_from_receipt(&tx, request_id, workspace_id, &receipt)?;
    commit_read(tx, destination)?;
    Ok(Some(record))
}

fn request_record_from_receipt(
    connection: &rusqlite::Connection,
    request_id: Uuid,
    workspace_id: WorkspaceId,
    receipt: &RequestReceipt,
) -> Result<MissionEvaluationRequestRecord, OperationsError> {
    if receipt.request_id != request_id || receipt.workspace_id != workspace_id {
        return Err(OperationsError::Corrupt(
            "mission evaluation request receipt scope",
        ));
    }
    let mutation = match receipt.operation {
        RequestOperation::MissionCreate => {
            let mission_id = MissionId::from_uuid(receipt.target_id)
                .map_err(|_| OperationsError::Corrupt("mission request target"))?;
            let (mission, linked_request_id) =
                load_mission_with_request(connection, mission_id)?
                    .ok_or(OperationsError::Corrupt("mission request referent missing"))?;
            if linked_request_id != request_id
                || mission.workspace_id != workspace_id
                || mission.revision != receipt.revision
                || mission.mission_id != mission_id
            {
                return Err(OperationsError::Corrupt(
                    "mission request referent identity",
                ));
            }
            MissionEvaluationRecordedMutation::MissionCreated(mission)
        }
        RequestOperation::ObligationRecord => {
            let obligation_id = MissionObligationId::from_uuid(receipt.target_id)
                .map_err(|_| OperationsError::Corrupt("obligation request target"))?;
            let (obligation, linked_request_id) =
                load_obligation_with_request(connection, obligation_id)?.ok_or(
                    OperationsError::Corrupt("obligation request referent missing"),
                )?;
            if linked_request_id != request_id
                || obligation.workspace_id != workspace_id
                || obligation.revision != receipt.revision
                || obligation.obligation_id != obligation_id
            {
                return Err(OperationsError::Corrupt(
                    "obligation request referent identity",
                ));
            }
            MissionEvaluationRecordedMutation::ObligationRecorded(obligation)
        }
        RequestOperation::PlanPublish => {
            let version_id = MissionEvaluationPlanVersionId::from_uuid(receipt.target_id)
                .map_err(|_| OperationsError::Corrupt("plan request target"))?;
            let (version, linked_request_id) =
                load_plan_version_by_version_id_with_request(connection, version_id)?
                    .ok_or(OperationsError::Corrupt("plan request referent missing"))?;
            if linked_request_id != request_id
                || version.workspace_id != workspace_id
                || version.revision != receipt.revision
                || version.version_id != version_id
            {
                return Err(OperationsError::Corrupt("plan request referent identity"));
            }
            MissionEvaluationRecordedMutation::PlanPublished(version)
        }
        RequestOperation::PlanStart => {
            let plan_id = MissionEvaluationPlanId::from_uuid(receipt.target_id)
                .map_err(|_| OperationsError::Corrupt("plan start request target"))?;
            let start = load_plan_start(connection, plan_id)?.ok_or(OperationsError::Corrupt(
                "plan start request referent missing",
            ))?;
            if start.request_id != request_id
                || start.workspace_id != workspace_id
                || start.revision != receipt.revision
                || start.plan_id != plan_id
            {
                return Err(OperationsError::Corrupt(
                    "plan start request referent identity",
                ));
            }
            MissionEvaluationRecordedMutation::PlanStarted(start)
        }
    };
    Ok(MissionEvaluationRequestRecord {
        request_id,
        workspace_id,
        mutation,
    })
}
