//! Immutable preparation payload and its indexed admission identities.

use forge_domain::{TimestampMillis, WorkspaceId};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::super::{
    ActorSnapshot, TeamPermanentTransferConsentKind as Kind,
    TeamPermanentTransferIntentReceipt as Intent, generation, load_consent,
    load_intent_in_workspace,
};
use super::{
    TeamPermanentTransferPreparationCommand as Command,
    TeamPermanentTransferPreparationReceipt as Receipt, identity, schema,
};
use crate::OperationsError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Author {
    pub(super) activity: [u8; 16],
    pub(super) agent: [u8; 16],
    pub(super) session: [u8; 16],
    pub(super) participation: [u8; 16],
    pub(super) profile: [u8; 16],
    pub(super) participant: [u8; 16],
    pub(super) display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PreparationPayload {
    version: u8,
    pub(super) preparation_id: [u8; 16],
    pub(super) workspace_id: [u8; 16],
    destination_id: [u8; 16],
    pub(super) transfer_id: [u8; 16],
    pub(super) source_team: [u8; 16],
    pub(super) destination_team: [u8; 16],
    pub(super) generation: u64,
    pub(super) orchestrator_revision: i64,
    pub(super) source_leader_consent_id: [u8; 16],
    pub(super) target_acceptance_id: [u8; 16],
    pub(super) actor: Author,
    pub(super) identity: identity::CapturedIdentity,
    recorded_at_ms: i64,
}

impl PreparationPayload {
    pub(super) fn new(
        command: &Command,
        destination: Uuid,
        actor: &ActorSnapshot,
        identity: identity::CapturedIdentity,
        intent: &Intent,
        now: TimestampMillis,
    ) -> Self {
        Self {
            version: 1,
            preparation_id: command.preparation_id.into_bytes(),
            workspace_id: command.workspace_id.into_uuid().into_bytes(),
            destination_id: destination.into_bytes(),
            transfer_id: command.transfer_id.into_bytes(),
            source_team: intent.source_team_id.into_bytes(),
            destination_team: intent.destination_team_id.into_bytes(),
            generation: command.expected_generation,
            orchestrator_revision: command.expected_workspace_leadership_revision,
            source_leader_consent_id: command.source_leader_consent_id.into_bytes(),
            target_acceptance_id: command.target_acceptance_id.into_bytes(),
            actor: Author {
                activity: actor.activity_id.into_uuid().into_bytes(),
                agent: actor.agent_id.into_uuid().into_bytes(),
                session: actor.session_id.into_uuid().into_bytes(),
                participation: actor.participation_id.into_uuid().into_bytes(),
                profile: actor.profile_id.into_bytes(),
                participant: actor.participant_id.into_bytes(),
                display_name: actor.display_name.clone(),
            },
            identity,
            recorded_at_ms: now.get(),
        }
    }

    pub(super) const fn destination_id(&self) -> Uuid {
        Uuid::from_bytes(self.destination_id)
    }
}

pub(super) fn matches_command(receipt: &Receipt, command: &Command, actor: &ActorSnapshot) -> bool {
    let p = &receipt.payload;
    receipt.preparation_id() == command.preparation_id
        && receipt.workspace_id() == command.workspace_id
        && receipt.transfer_id() == command.transfer_id
        && p.generation == command.expected_generation
        && p.orchestrator_revision == command.expected_workspace_leadership_revision
        && receipt.source_leader_consent_id() == command.source_leader_consent_id
        && receipt.target_acceptance_id() == command.target_acceptance_id
        && p.actor.participation == *actor.participation_id.into_uuid().as_bytes()
        && p.actor.profile == *actor.profile_id.as_bytes()
        && p.actor.activity == *actor.activity_id.into_uuid().as_bytes()
        && p.actor.agent == *actor.agent_id.into_uuid().as_bytes()
        && p.actor.session == *actor.session_id.into_uuid().as_bytes()
}

pub(super) fn author(
    c: &Connection,
    w: WorkspaceId,
    id: Uuid,
) -> Result<Option<(Uuid, Uuid)>, OperationsError> {
    let row:Option<(Vec<u8>,Vec<u8>)>=c.query_row(
        "SELECT actor_participation_id,actor_profile_id FROM ops_team_permanent_transfer_preparations WHERE workspace_id=?1 AND preparation_id=?2",
        params![w.into_uuid().as_bytes(),id.as_bytes()],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    row.map(|(a, b)| {
        Ok((
            Uuid::from_bytes(crate::wire::id(a)?),
            Uuid::from_bytes(crate::wire::id(b)?),
        ))
    })
    .transpose()
}

pub(super) fn contains_transfer(
    c: &Connection,
    w: WorkspaceId,
    transfer: Uuid,
) -> Result<bool, OperationsError> {
    Ok(c.query_row("SELECT EXISTS(SELECT 1 FROM ops_team_permanent_transfer_preparations WHERE workspace_id=?1 AND transfer_id=?2)",
        params![w.into_uuid().as_bytes(),transfer.as_bytes()],|r|r.get(0))?)
}

pub(super) fn insert(
    c: &Connection,
    p: &PreparationPayload,
    intent: &Intent,
) -> Result<(), OperationsError> {
    validate_payload(c, p, intent)?;
    let bytes = serde_json::to_vec(p)
        .map_err(|_| OperationsError::Corrupt("transfer preparation encode"))?;
    if bytes.len() > schema::MAX_PAYLOAD {
        return Err(OperationsError::Bounds("transfer preparation payload"));
    }
    let digest = digest(&bytes);
    c.execute("INSERT INTO ops_team_permanent_transfer_preparations(workspace_id,preparation_id,transfer_id,generation,intent_digest,target_participation_id,target_profile_id,actor_participation_id,actor_profile_id,payload,payload_digest,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
        params![p.workspace_id.as_slice(),p.preparation_id.as_slice(),p.transfer_id.as_slice(),
            i64::try_from(p.generation).map_err(|_|OperationsError::Bounds("transfer preparation generation"))?,
            p.identity.intent_digest.as_slice(),p.identity.participation_id.as_slice(),p.identity.profile_id.as_slice(),
            p.actor.participation.as_slice(),p.actor.profile.as_slice(),bytes,digest.as_slice(),p.recorded_at_ms])?;
    for session in p.identity.session_ids() {
        c.execute("INSERT INTO ops_team_permanent_transfer_preparation_sessions(workspace_id,preparation_id,session_id) VALUES(?1,?2,?3)",
            params![p.workspace_id.as_slice(),p.preparation_id.as_slice(),session.into_uuid().as_bytes()])?;
    }
    c.execute("INSERT INTO ops_team_permanent_transfer_preparation_barriers(workspace_id,preparation_id,target_profile_id,target_participation_id) VALUES(?1,?2,?3,?4)",
        params![p.workspace_id.as_slice(),p.preparation_id.as_slice(),p.identity.profile_id.as_slice(),p.identity.participation_id.as_slice()])?;
    Ok(())
}

pub(super) fn load(
    c: &Connection,
    w: WorkspaceId,
    id: Uuid,
) -> Result<Option<Receipt>, OperationsError> {
    if c.is_autocommit() {
        let tx = c.unchecked_transaction()?;
        let result = read_record(&tx, w, id)?;
        tx.commit()?;
        Ok(result)
    } else {
        read_record(c, w, id)
    }
}

struct Row {
    transfer: Vec<u8>,
    generation: i64,
    intent_digest: Vec<u8>,
    participation: Vec<u8>,
    profile: Vec<u8>,
    author_participation: Vec<u8>,
    author_profile: Vec<u8>,
    payload: Option<Vec<u8>>,
    digest: Vec<u8>,
    recorded_at: i64,
}

fn read_record(
    c: &Connection,
    w: WorkspaceId,
    id: Uuid,
) -> Result<Option<Receipt>, OperationsError> {
    let row=c.query_row("SELECT transfer_id,generation,intent_digest,target_participation_id,target_profile_id,actor_participation_id,actor_profile_id,CASE WHEN length(payload)<=262144 THEN payload END,payload_digest,recorded_at_ms FROM ops_team_permanent_transfer_preparations WHERE workspace_id=?1 AND preparation_id=?2",
        params![w.into_uuid().as_bytes(),id.as_bytes()],|r|Ok(Row{transfer:r.get(0)?,generation:r.get(1)?,intent_digest:r.get(2)?,participation:r.get(3)?,profile:r.get(4)?,author_participation:r.get(5)?,author_profile:r.get(6)?,payload:r.get(7)?,digest:r.get(8)?,recorded_at:r.get(9)?})).optional()?;
    let Some(row) = row else {
        return Ok(None);
    };
    let bytes = row.payload.ok_or(OperationsError::Bounds(
        "transfer preparation stored payload",
    ))?;
    let p: PreparationPayload = serde_json::from_slice(&bytes)
        .map_err(|_| OperationsError::Corrupt("transfer preparation payload"))?;
    let checksum = digest(&bytes);
    if row.digest != checksum
        || p.workspace_id != *w.into_uuid().as_bytes()
        || p.preparation_id != *id.as_bytes()
        || row.transfer != p.transfer_id
        || u64::try_from(row.generation).ok() != Some(p.generation)
        || row.intent_digest != p.identity.intent_digest
        || row.participation != p.identity.participation_id
        || row.profile != p.identity.profile_id
        || row.author_participation != p.actor.participation
        || row.author_profile != p.actor.profile
        || row.recorded_at != p.recorded_at_ms
    {
        return Err(OperationsError::Corrupt("transfer preparation header"));
    }
    let canonical = serde_json::to_vec(&p)
        .map_err(|_| OperationsError::Corrupt("transfer preparation payload encode"))?;
    if canonical != bytes {
        return Err(OperationsError::Corrupt(
            "transfer preparation canonical payload",
        ));
    }
    let intent = load_intent_in_workspace(c, w, Uuid::from_bytes(p.transfer_id))?
        .ok_or(OperationsError::Corrupt("transfer preparation intent"))?;
    validate_payload(c, &p, &intent)?;
    validate_indexes(c, &p)?;
    let recorded_at = TimestampMillis::new(p.recorded_at_ms)
        .map_err(|_| OperationsError::Corrupt("transfer preparation timestamp"))?;
    let target_participation =
        crate::WorkspaceParticipationId::from_uuid(Uuid::from_bytes(p.identity.participation_id))?;
    let author_participation =
        crate::WorkspaceParticipationId::from_uuid(Uuid::from_bytes(p.actor.participation))?;
    Ok(Some(Receipt {
        payload: p,
        digest: checksum,
        recorded_at,
        target_participation,
        author_participation,
        replayed: false,
    }))
}

fn validate_payload(
    c: &Connection,
    p: &PreparationPayload,
    intent: &Intent,
) -> Result<(), OperationsError> {
    let ids = [
        p.preparation_id,
        p.workspace_id,
        p.destination_id,
        p.transfer_id,
        p.source_team,
        p.destination_team,
        p.source_leader_consent_id,
        p.target_acceptance_id,
        p.actor.activity,
        p.actor.agent,
        p.actor.session,
        p.actor.participation,
        p.actor.profile,
        p.actor.participant,
    ];
    if p.version != 1
        || ids.iter().any(|id| Uuid::from_bytes(*id).is_nil())
        || p.generation == 0
        || p.orchestrator_revision < 1
        || p.actor.display_name.trim().is_empty()
        || p.actor.display_name.len() > 128
        || p.source_leader_consent_id == p.target_acceptance_id
        || p.recorded_at_ms < intent.recorded_at.get()
        || p.source_team != *intent.source_team_id.as_bytes()
        || p.destination_team != *intent.destination_team_id.as_bytes()
    {
        return Err(OperationsError::Corrupt("transfer preparation shape"));
    }
    let destination: Vec<u8> = c.query_row(
        "SELECT destination_id FROM ops_catalog_identity WHERE singleton=1",
        [],
        |r| r.get(0),
    )?;
    if destination != p.destination_id
        || p.identity.census_destination_id().into_bytes() != p.destination_id
    {
        return Err(OperationsError::Corrupt("transfer preparation destination"));
    }
    identity::validate(&p.identity, intent)?;
    if p.recorded_at_ms < p.identity.observed_at_ms_high_water() {
        return Err(OperationsError::Corrupt(
            "transfer preparation identity time",
        ));
    }
    let generation = generation::read_generation(c, intent.workspace_id, intent.transfer_id)?
        .ok_or(OperationsError::Corrupt("transfer preparation ancestry"))?;
    if generation.generation() != p.generation {
        return Err(OperationsError::Corrupt("transfer preparation generation"));
    }
    for (id, kind) in [
        (p.source_leader_consent_id, Kind::SourceHomeLeader),
        (p.target_acceptance_id, Kind::TargetAgentAcceptance),
    ] {
        let consent = load_consent(
            c,
            intent.workspace_id,
            intent.transfer_id,
            Uuid::from_bytes(id),
        )?
        .ok_or(OperationsError::Corrupt("transfer preparation consent"))?;
        if consent.kind != kind
            || consent.intent_digest != intent.intent_digest
            || p.recorded_at_ms < consent.recorded_at.get()
        {
            return Err(OperationsError::Corrupt(
                "transfer preparation consent identity",
            ));
        }
    }
    Ok(())
}

fn validate_indexes(c: &Connection, p: &PreparationPayload) -> Result<(), OperationsError> {
    let barrier:Option<(Vec<u8>,Vec<u8>)>=c.query_row("SELECT target_profile_id,target_participation_id FROM ops_team_permanent_transfer_preparation_barriers WHERE workspace_id=?1 AND preparation_id=?2",
        params![p.workspace_id.as_slice(),p.preparation_id.as_slice()],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    if barrier
        != Some((
            p.identity.profile_id.to_vec(),
            p.identity.participation_id.to_vec(),
        ))
    {
        return Err(OperationsError::Corrupt("transfer preparation barrier"));
    }
    let mut statement=c.prepare("SELECT session_id FROM ops_team_permanent_transfer_preparation_sessions WHERE workspace_id=?1 AND preparation_id=?2 ORDER BY session_id LIMIT 129")?;
    let rows = statement
        .query_map(
            params![p.workspace_id.as_slice(), p.preparation_id.as_slice()],
            |r| r.get::<_, Vec<u8>>(0),
        )?
        .collect::<Result<Vec<_>, _>>()?;
    let expected: Vec<_> = p
        .identity
        .session_ids()
        .map(|s| s.into_uuid().as_bytes().to_vec())
        .collect();
    if rows != expected {
        return Err(OperationsError::Corrupt(
            "transfer preparation Session index",
        ));
    }
    Ok(())
}

fn digest(payload: &[u8]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"heterarchy.orchestration.home-transfer.preparation.v1\0");
    digest.update(payload);
    digest.finalize().into()
}
