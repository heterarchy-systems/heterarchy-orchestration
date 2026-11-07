//! Workspace-owned identity evidence for one permanent Home transfer.
//!
//! This snapshot is not a global Profile/Session link, Forge work inventory,
//! or proof that work is settled. It captures only the Operations-owned
//! Workspace participation, its logical Session history, and current effect
//! binding receipts. The caller must use one IMMEDIATE transaction for this
//! read and the durable transfer preparation/barrier write.

use super::super::TeamPermanentTransferIntentReceipt;
use crate::{OperationsError, wire};
use forge_domain::AgentSessionId;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const MAX_MAPPINGS: usize = 64;
const MAX_SESSIONS: usize = 128;
const MAX_REBINDS: usize = MAX_SESSIONS;
const MAX_BINDINGS: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CapturedIdentity {
    pub(super) version: u8,
    pub(super) transfer_id: [u8; 16],
    pub(super) intent_digest: [u8; 32],
    pub(super) workspace_id: [u8; 16],
    pub(super) source_team_id: [u8; 16],
    pub(super) participation_id: [u8; 16],
    pub(super) profile_id: [u8; 16],
    pub(super) source_membership_revision: u64,
    pub(super) participation_created_at_ms: i64,
    pub(super) participation_revision: u64,
    pub(super) observed_high_water_ms: i64,
    registration_census: super::pending_registration::RegistrationCensus,
    pub(super) members: Vec<CapturedMember>,
    pub(super) mappings: Vec<CapturedMapping>,
    pub(super) rebinds: Vec<CapturedRebind>,
    pub(super) sessions: Vec<CapturedSession>,
    pub(super) bindings: Vec<CapturedBinding>,
}

impl CapturedIdentity {
    pub(super) const fn census_destination_id(&self) -> Uuid {
        self.registration_census.destination_id()
    }
    pub(super) fn participant_ids(&self) -> impl ExactSizeIterator<Item = Uuid> + '_ {
        self.members
            .iter()
            .map(|member| Uuid::from_bytes(member.participant_id))
    }

    pub(super) fn session_ids(&self) -> impl ExactSizeIterator<Item = AgentSessionId> + '_ {
        self.sessions
            .iter()
            .map(|session| AgentSessionId::from_uuid(Uuid::from_bytes(session.session_id)))
    }

    pub(super) const fn session_count(&self) -> usize {
        self.sessions.len()
    }

    pub(super) const fn binding_count(&self) -> usize {
        self.bindings.len()
    }

    pub(super) const fn observed_at_ms_high_water(&self) -> i64 {
        self.observed_high_water_ms
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CapturedMember {
    pub(super) participant_id: [u8; 16],
    pub(super) current_session_id: [u8; 16],
    pub(super) principal_kind: i64,
    pub(super) principal_id: Option<[u8; 16]>,
    pub(super) membership_revision: u64,
    pub(super) created_at_ms: i64,
}

/// The immutable Workspace-to-logical-Session mapping receipt, including the
/// Profile Catalog link identity as it was observed by the Workspace owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CapturedMapping {
    pub(super) request_id: [u8; 16],
    pub(super) participant_id: [u8; 16],
    pub(super) profile_mapping_request_id: [u8; 16],
    pub(super) profile_revision: i64,
    pub(super) session_id: [u8; 16],
    pub(super) session_revision: i64,
    pub(super) participant_membership_revision: u64,
    pub(super) mapped_at_ms: i64,
}

/// An immutable Workspace Session-rebind edge. Both endpoint Session IDs are
/// included in the manifest's captured Session set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CapturedRebind {
    pub(super) request_id: [u8; 16],
    pub(super) participant_id: [u8; 16],
    pub(super) profile_mapping_request_id: [u8; 16],
    pub(super) profile_revision: i64,
    pub(super) previous_session_id: [u8; 16],
    pub(super) new_session_id: [u8; 16],
    pub(super) new_session_revision: i64,
    pub(super) participant_membership_revision: u64,
    pub(super) expected_participation_revision: u64,
    pub(super) result_participation_revision: u64,
    pub(super) bound_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CapturedSession {
    pub(super) participant_id: [u8; 16],
    pub(super) session_id: [u8; 16],
}

/// Exact current Operations effect-binding attribution at capture time.
/// Principal fields use the same closed `kind` / optional `id` encoding as
/// the leadership roster wire contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CapturedBinding {
    pub(super) participant_id: [u8; 16],
    pub(super) activity_id: [u8; 16],
    pub(super) agent_id: [u8; 16],
    pub(super) session_id: [u8; 16],
    pub(super) principal_kind: i64,
    pub(super) principal_id: Option<[u8; 16]>,
    pub(super) source_kind: i64,
    pub(super) source_request_id: [u8; 16],
}

struct Anchor {
    participation_id: [u8; 16],
    created_at_ms: i64,
    revision: u64,
}

struct MappingCapture {
    mappings: Vec<CapturedMapping>,
    participant_ids: std::collections::BTreeSet<[u8; 16]>,
    high_water: i64,
}

struct RawMember {
    session: Vec<u8>,
    principal_kind: i64,
    principal_id: Option<Vec<u8>>,
    membership_revision: i64,
    created_at_ms: i64,
}

struct RawMapping {
    request_id: Vec<u8>,
    participation_id: Vec<u8>,
    profile_id: Vec<u8>,
    profile_revision: i64,
    profile_mapping_request_id: Vec<u8>,
    session_id: Vec<u8>,
    session_revision: i64,
    participant_id: Vec<u8>,
    participant_membership_revision: i64,
    mapped_at_ms: i64,
}

struct RawRebind {
    request_id: Vec<u8>,
    participation_id: Vec<u8>,
    profile_id: Vec<u8>,
    profile_revision: i64,
    profile_mapping_request_id: Vec<u8>,
    previous_session_id: Vec<u8>,
    new_session_id: Vec<u8>,
    new_session_revision: i64,
    participant_id: Vec<u8>,
    participant_membership_revision: i64,
    expected_participation_revision: i64,
    result_participation_revision: i64,
    bound_at_ms: i64,
}

struct RawBinding {
    participant_id: Vec<u8>,
    activity_id: Vec<u8>,
    agent_id: Vec<u8>,
    session_id: Vec<u8>,
    current_session_id: Vec<u8>,
    principal_kind: i64,
    principal_id: Option<Vec<u8>>,
    source_kind: i64,
    source_request_id: Vec<u8>,
}

/// Capture one exact `(Workspace, Participation, Profile)` identity from
/// Operations-owned rows. `connection` must belong to the caller's active
/// IMMEDIATE transaction. No Profile Catalog lookup or external I/O occurs.
pub(super) fn capture(
    connection: &Connection,
    intent: &TeamPermanentTransferIntentReceipt,
) -> Result<CapturedIdentity, OperationsError> {
    validate_intent_identity(intent)?;
    let anchor = load_anchor(connection, intent)?;
    let MappingCapture {
        mappings,
        participant_ids,
        high_water: mapping_high_water,
    } = load_mappings(connection, intent, &anchor)?;
    let members = load_members(connection, intent, &participant_ids)?;
    let (rebinds, rebind_high_water) = load_rebinds(connection, intent, &anchor, &participant_ids)?;
    let registration_census = ensure_workspace_capture_complete(connection, intent)?;
    let sessions = load_sessions(connection, intent)?;
    let bindings = load_bindings(connection, intent, &members)?;
    let observed_high_water_ms = intent
        .recorded_at()
        .get()
        .max(anchor.created_at_ms)
        .max(
            members
                .iter()
                .map(|member| member.created_at_ms)
                .max()
                .unwrap_or(anchor.created_at_ms),
        )
        .max(mapping_high_water)
        .max(rebind_high_water)
        .max(registration_census.observed_high_water_ms());
    let captured = CapturedIdentity {
        version: 1,
        transfer_id: intent.transfer_id().into_bytes(),
        intent_digest: intent.intent_digest,
        workspace_id: intent.workspace_id().into_uuid().into_bytes(),
        source_team_id: intent.source_team_id().into_bytes(),
        participation_id: anchor.participation_id,
        profile_id: intent.target_profile_id().into_bytes(),
        source_membership_revision: intent.source_membership_revision(),
        participation_created_at_ms: anchor.created_at_ms,
        participation_revision: anchor.revision,
        observed_high_water_ms,
        registration_census,
        members,
        mappings,
        rebinds,
        sessions,
        bindings,
    };
    validate(&captured, intent)?;
    Ok(captured)
}

/// Validate an immutable identity payload read from a prepared transfer row.
/// This intentionally does not compare with current Workspace mappings: those
/// mappings may change after a successful cutover, while this payload remains
/// the historical preparation receipt.
pub(super) fn validate(
    identity: &CapturedIdentity,
    intent: &TeamPermanentTransferIntentReceipt,
) -> Result<(), OperationsError> {
    validate_intent_identity(intent)?;
    if identity.version != 1
        || identity.transfer_id != intent.transfer_id().into_bytes()
        || identity.intent_digest != intent.intent_digest
        || identity.workspace_id != intent.workspace_id().into_uuid().into_bytes()
        || identity.source_team_id != intent.source_team_id().into_bytes()
        || identity.profile_id != intent.target_profile_id().into_bytes()
        || identity.source_membership_revision != intent.source_membership_revision()
        || identity.participation_id != intent.target_participation_id().into_uuid().into_bytes()
        || identity.participation_revision == 0
        || identity.participation_created_at_ms < 0
        || identity.members.is_empty()
        || identity.members.len() > MAX_MAPPINGS
        || identity.mappings.is_empty()
        || identity.mappings.len() > MAX_MAPPINGS
        || identity.sessions.is_empty()
        || identity.sessions.len() > MAX_SESSIONS
        || identity.rebinds.len() > MAX_REBINDS
        || identity.bindings.len() > MAX_BINDINGS
    {
        return Err(OperationsError::Corrupt(
            "permanent transfer Workspace identity header or bounds",
        ));
    }
    for id in [
        &identity.transfer_id,
        &identity.workspace_id,
        &identity.source_team_id,
        &identity.participation_id,
        &identity.profile_id,
    ] {
        ensure_id(id, "permanent transfer Workspace identity UUID")?;
    }
    if identity
        .members
        .windows(2)
        .any(|rows| rows[0].participant_id >= rows[1].participant_id)
        || identity
            .mappings
            .windows(2)
            .any(|rows| mapping_order(rows[0]) >= mapping_order(rows[1]))
        || identity.rebinds.windows(2).any(|rows| {
            rows[0].expected_participation_revision >= rows[1].expected_participation_revision
        })
        || identity
            .sessions
            .windows(2)
            .any(|rows| session_order(rows[0]) >= session_order(rows[1]))
        || identity
            .bindings
            .windows(2)
            .any(|rows| rows[0].agent_id >= rows[1].agent_id)
    {
        return Err(OperationsError::Corrupt(
            "permanent transfer Workspace identity ordering",
        ));
    }
    let members = validate_members(identity)?;
    identity.registration_census.validate(
        intent.workspace_id(),
        intent.target_profile_id(),
        intent.target_participation_id().into_uuid(),
    )?;
    let expected_high_water = identity
        .mappings
        .iter()
        .map(|mapping| mapping.mapped_at_ms)
        .chain(identity.rebinds.iter().map(|rebind| rebind.bound_at_ms))
        .chain(identity.members.iter().map(|member| member.created_at_ms))
        .chain(std::iter::once(identity.participation_created_at_ms))
        .chain(std::iter::once(intent.recorded_at().get()))
        .chain(std::iter::once(
            identity.registration_census.observed_high_water_ms(),
        ))
        .max()
        .ok_or(OperationsError::Corrupt(
            "permanent transfer Workspace identity timestamps",
        ))?;
    if identity.observed_high_water_ms != expected_high_water {
        return Err(OperationsError::Corrupt(
            "permanent transfer Workspace identity high water",
        ));
    }
    let mapped_sessions = validate_mappings(identity, &members)?;
    validate_rebinds(identity, &members)?;
    validate_rebind_chain(
        identity.participation_revision,
        &members,
        &mapped_sessions,
        &identity.rebinds,
    )?;
    validate_session_binding_set(identity, &members, &mapped_sessions)
}

fn validate_members(
    identity: &CapturedIdentity,
) -> Result<std::collections::BTreeMap<[u8; 16], CapturedMember>, OperationsError> {
    let mut members = std::collections::BTreeMap::new();
    let mut current_sessions = std::collections::BTreeSet::new();
    for member in &identity.members {
        ensure_id(
            &member.participant_id,
            "permanent transfer participant UUID",
        )?;
        ensure_id(
            &member.current_session_id,
            "permanent transfer current Session UUID",
        )?;
        if member.membership_revision == 0
            || member.created_at_ms < 0
            || !valid_principal(member.principal_kind, member.principal_id)
            || !current_sessions.insert(member.current_session_id)
            || members.insert(member.participant_id, *member).is_some()
        {
            return Err(OperationsError::Corrupt(
                "permanent transfer Workspace member identity",
            ));
        }
    }
    Ok(members)
}

fn validate_mappings(
    identity: &CapturedIdentity,
    members: &std::collections::BTreeMap<[u8; 16], CapturedMember>,
) -> Result<
    std::collections::BTreeMap<[u8; 16], std::collections::BTreeSet<[u8; 16]>>,
    OperationsError,
> {
    let mut sessions = std::collections::BTreeMap::new();
    let mut requests = std::collections::BTreeSet::new();
    let mut all_sessions = std::collections::BTreeSet::new();
    for mapping in &identity.mappings {
        for id in [
            &mapping.request_id,
            &mapping.participant_id,
            &mapping.profile_mapping_request_id,
            &mapping.session_id,
        ] {
            ensure_id(id, "permanent transfer mapping UUID")?;
        }
        if mapping.profile_revision < 1
            || mapping.session_revision < 1
            || mapping.participant_membership_revision == 0
            || mapping.mapped_at_ms < 0
            || !members.contains_key(&mapping.participant_id)
            || !requests.insert(mapping.request_id)
            || !all_sessions.insert(mapping.session_id)
        {
            return Err(OperationsError::Corrupt(
                "permanent transfer Workspace mapping identity",
            ));
        }
        sessions
            .entry(mapping.participant_id)
            .or_insert_with(std::collections::BTreeSet::new)
            .insert(mapping.session_id);
    }
    if sessions.len() != members.len() {
        return Err(OperationsError::Corrupt(
            "permanent transfer Workspace participant coverage",
        ));
    }
    Ok(sessions)
}

fn validate_rebinds(
    identity: &CapturedIdentity,
    members: &std::collections::BTreeMap<[u8; 16], CapturedMember>,
) -> Result<(), OperationsError> {
    let mut requests = std::collections::BTreeSet::new();
    for rebind in &identity.rebinds {
        for id in [
            &rebind.request_id,
            &rebind.participant_id,
            &rebind.profile_mapping_request_id,
            &rebind.previous_session_id,
            &rebind.new_session_id,
        ] {
            ensure_id(id, "permanent transfer rebind UUID")?;
        }
        if rebind.profile_revision < 1
            || rebind.new_session_revision < 1
            || !members.contains_key(&rebind.participant_id)
            || rebind.participant_membership_revision == 0
            || rebind.expected_participation_revision.checked_add(1)
                != Some(rebind.result_participation_revision)
            || rebind.bound_at_ms < 0
            || rebind.previous_session_id == rebind.new_session_id
            || !requests.insert(rebind.request_id)
        {
            return Err(OperationsError::Corrupt(
                "permanent transfer Workspace rebind identity",
            ));
        }
    }
    Ok(())
}

fn validate_rebind_chain(
    participation_revision: u64,
    members: &std::collections::BTreeMap<[u8; 16], CapturedMember>,
    mapped_sessions: &std::collections::BTreeMap<[u8; 16], std::collections::BTreeSet<[u8; 16]>>,
    rebinds: &[CapturedRebind],
) -> Result<(), OperationsError> {
    let mut expected_revision = 1_u64;
    let mut last_session = std::collections::BTreeMap::new();
    for rebind in rebinds {
        let previous = last_session
            .get(&rebind.participant_id)
            .copied()
            .or_else(|| {
                mapped_sessions
                    .get(&rebind.participant_id)
                    .and_then(|sessions| {
                        sessions
                            .contains(&rebind.previous_session_id)
                            .then_some(rebind.previous_session_id)
                    })
            });
        if rebind.expected_participation_revision != expected_revision
            || rebind.expected_participation_revision.checked_add(1)
                != Some(rebind.result_participation_revision)
            || previous != Some(rebind.previous_session_id)
        {
            return Err(OperationsError::Conflict(
                "permanent transfer Workspace rebind history incomplete",
            ));
        }
        expected_revision = rebind.result_participation_revision;
        last_session.insert(rebind.participant_id, rebind.new_session_id);
    }
    if expected_revision != participation_revision {
        return Err(OperationsError::Conflict(
            "permanent transfer Workspace rebind revision history incomplete",
        ));
    }
    for (participant_id, member) in members {
        let current = last_session.get(participant_id).copied().or_else(|| {
            mapped_sessions.get(participant_id).and_then(|sessions| {
                sessions
                    .contains(&member.current_session_id)
                    .then_some(member.current_session_id)
            })
        });
        if current != Some(member.current_session_id) {
            return Err(OperationsError::Conflict(
                "permanent transfer Workspace participant Session history",
            ));
        }
    }
    Ok(())
}

fn validate_session_binding_set(
    identity: &CapturedIdentity,
    members: &std::collections::BTreeMap<[u8; 16], CapturedMember>,
    mapped_sessions: &std::collections::BTreeMap<[u8; 16], std::collections::BTreeSet<[u8; 16]>>,
) -> Result<(), OperationsError> {
    let mut expected = std::collections::BTreeSet::new();
    for (participant_id, sessions) in mapped_sessions {
        expected.extend(sessions.iter().map(|session| (*participant_id, *session)));
    }
    for (participant_id, member) in members {
        expected.insert((*participant_id, member.current_session_id));
    }
    for rebind in &identity.rebinds {
        expected.insert((rebind.participant_id, rebind.previous_session_id));
        expected.insert((rebind.participant_id, rebind.new_session_id));
    }
    let mut agents = std::collections::BTreeSet::new();
    for binding in &identity.bindings {
        for id in [
            &binding.participant_id,
            &binding.activity_id,
            &binding.agent_id,
            &binding.session_id,
            &binding.source_request_id,
        ] {
            ensure_id(id, "permanent transfer effect binding UUID")?;
        }
        let member = members
            .get(&binding.participant_id)
            .ok_or(OperationsError::Corrupt(
                "permanent transfer effect binding participant",
            ))?;
        if binding.session_id != member.current_session_id
            || !valid_principal(binding.principal_kind, binding.principal_id)
            || binding.principal_kind != member.principal_kind
            || binding.principal_id != member.principal_id
            || !matches!(binding.source_kind, 0 | 1)
            || !agents.insert(binding.agent_id)
        {
            return Err(OperationsError::Conflict(
                "permanent transfer Workspace effect binding is not current",
            ));
        }
        expected.insert((binding.participant_id, binding.session_id));
    }
    let mut actual = std::collections::BTreeSet::new();
    let mut all_sessions = std::collections::BTreeSet::new();
    for session in &identity.sessions {
        ensure_id(
            &session.participant_id,
            "permanent transfer Session participant UUID",
        )?;
        ensure_id(&session.session_id, "permanent transfer Session UUID")?;
        if !members.contains_key(&session.participant_id)
            || !all_sessions.insert(session.session_id)
            || !actual.insert((session.participant_id, session.session_id))
        {
            return Err(OperationsError::Corrupt(
                "permanent transfer Workspace Session identity",
            ));
        }
    }
    if expected != actual {
        return Err(OperationsError::Corrupt(
            "permanent transfer Workspace Session set",
        ));
    }
    Ok(())
}

fn validate_intent_identity(
    intent: &TeamPermanentTransferIntentReceipt,
) -> Result<(), OperationsError> {
    if intent.transfer_id().is_nil()
        || intent.workspace_id().into_uuid().is_nil()
        || intent.source_team_id().is_nil()
        || intent.target_profile_id().is_nil()
        || intent.target_participation_id().into_uuid().is_nil()
        || intent.source_membership_revision() == 0
        || intent.intent_digest == [0; 32]
    {
        return Err(OperationsError::Corrupt(
            "permanent transfer Workspace capture intent",
        ));
    }
    Ok(())
}

fn load_anchor(
    connection: &Connection,
    intent: &TeamPermanentTransferIntentReceipt,
) -> Result<Anchor, OperationsError> {
    let raw: Option<(Vec<u8>, i64, i64)> = connection
        .query_row(
            "SELECT substr(participation_id,1,17),created_at_ms,revision FROM ops_workspace_participations WHERE workspace_id=?1 AND agent_profile_id=?2",
            params![
                intent.workspace_id().into_uuid().as_bytes(),
                intent.target_profile_id().as_bytes(),
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((participation, created_at_ms, revision)) = raw else {
        return Err(OperationsError::Conflict(
            "permanent transfer Workspace participation missing",
        ));
    };
    let participation_id = wire::id(participation)?;
    if participation_id != intent.target_participation_id().into_uuid().into_bytes() {
        return Err(OperationsError::Conflict(
            "permanent transfer Workspace participation changed",
        ));
    }
    let anchor = Anchor {
        participation_id,
        created_at_ms,
        revision: positive_revision(revision, "Workspace participation revision")?,
    };
    if anchor.created_at_ms < 0 {
        return Err(OperationsError::Corrupt(
            "permanent transfer Workspace participation timestamp",
        ));
    }
    Ok(anchor)
}

fn load_mappings(
    connection: &Connection,
    intent: &TeamPermanentTransferIntentReceipt,
    anchor: &Anchor,
) -> Result<MappingCapture, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT substr(request_id,1,17),substr(participation_id,1,17),substr(agent_profile_id,1,17),profile_revision,substr(profile_mapping_request_id,1,17),substr(session_id,1,17),session_revision,substr(participant_id,1,17),participant_membership_revision,mapped_at_ms FROM ops_workspace_participation_mappings WHERE workspace_id=?1 AND participation_id=?2 ORDER BY session_id,request_id LIMIT ?3",
    )?;
    let rows = statement.query_map(
        params![
            intent.workspace_id().into_uuid().as_bytes(),
            anchor.participation_id.as_slice(),
            query_limit(MAX_MAPPINGS)?,
        ],
        |row| {
            Ok(RawMapping {
                request_id: row.get(0)?,
                participation_id: row.get(1)?,
                profile_id: row.get(2)?,
                profile_revision: row.get(3)?,
                profile_mapping_request_id: row.get(4)?,
                session_id: row.get(5)?,
                session_revision: row.get(6)?,
                participant_id: row.get(7)?,
                participant_membership_revision: row.get(8)?,
                mapped_at_ms: row.get(9)?,
            })
        },
    )?;
    let raw = rows.collect::<Result<Vec<_>, _>>()?;
    if raw.len() > MAX_MAPPINGS {
        return Err(OperationsError::Bounds(
            "permanent transfer Workspace mappings",
        ));
    }
    if raw.is_empty() {
        return Err(OperationsError::Conflict(
            "permanent transfer Workspace mapping history missing",
        ));
    }
    let mut participant_ids = std::collections::BTreeSet::new();
    let mut high_water = anchor.created_at_ms;
    let mut mappings = Vec::with_capacity(raw.len());
    for row in raw {
        let participation_id = checked_uuid(row.participation_id, "mapping Participation")?;
        let profile_id = checked_uuid(row.profile_id, "mapping Profile")?;
        if participation_id != anchor.participation_id
            || profile_id != intent.target_profile_id().into_bytes()
        {
            return Err(OperationsError::Corrupt(
                "permanent transfer Workspace mapping anchor",
            ));
        }
        let current_participant = checked_uuid(row.participant_id, "mapping participant")?;
        participant_ids.insert(current_participant);
        if row.mapped_at_ms < 0 {
            return Err(OperationsError::Corrupt(
                "permanent transfer Workspace mapping time",
            ));
        }
        high_water = high_water.max(row.mapped_at_ms);
        mappings.push(CapturedMapping {
            request_id: checked_uuid(row.request_id, "mapping request")?,
            participant_id: current_participant,
            profile_mapping_request_id: checked_uuid(
                row.profile_mapping_request_id,
                "Profile mapping request",
            )?,
            profile_revision: positive_i64(row.profile_revision, "Profile mapping revision")?,
            session_id: checked_uuid(row.session_id, "mapping Session")?,
            session_revision: positive_i64(row.session_revision, "mapping Session revision")?,
            participant_membership_revision: positive_revision(
                row.participant_membership_revision,
                "mapping participant revision",
            )?,
            mapped_at_ms: row.mapped_at_ms,
        });
    }
    if participant_ids.is_empty() {
        return Err(OperationsError::Corrupt(
            "permanent transfer Workspace participant missing",
        ));
    }
    Ok(MappingCapture {
        mappings,
        participant_ids,
        high_water,
    })
}

fn load_rebinds(
    connection: &Connection,
    intent: &TeamPermanentTransferIntentReceipt,
    anchor: &Anchor,
    participant_ids: &std::collections::BTreeSet<[u8; 16]>,
) -> Result<(Vec<CapturedRebind>, i64), OperationsError> {
    let mut statement = connection.prepare(
        "SELECT substr(request_id,1,17),substr(participation_id,1,17),substr(agent_profile_id,1,17),profile_revision,substr(profile_mapping_request_id,1,17),substr(previous_session_id,1,17),substr(new_session_id,1,17),new_session_revision,substr(participant_id,1,17),participant_membership_revision,expected_participation_revision,result_participation_revision,bound_at_ms FROM ops_workspace_participation_session_rebinds WHERE workspace_id=?1 AND participation_id=?2 ORDER BY expected_participation_revision,request_id LIMIT ?3",
    )?;
    let rows = statement.query_map(
        params![
            intent.workspace_id().into_uuid().as_bytes(),
            anchor.participation_id.as_slice(),
            query_limit(MAX_REBINDS)?,
        ],
        |row| {
            Ok(RawRebind {
                request_id: row.get(0)?,
                participation_id: row.get(1)?,
                profile_id: row.get(2)?,
                profile_revision: row.get(3)?,
                profile_mapping_request_id: row.get(4)?,
                previous_session_id: row.get(5)?,
                new_session_id: row.get(6)?,
                new_session_revision: row.get(7)?,
                participant_id: row.get(8)?,
                participant_membership_revision: row.get(9)?,
                expected_participation_revision: row.get(10)?,
                result_participation_revision: row.get(11)?,
                bound_at_ms: row.get(12)?,
            })
        },
    )?;
    let raw = rows.collect::<Result<Vec<_>, _>>()?;
    if raw.len() > MAX_REBINDS {
        return Err(OperationsError::Bounds(
            "permanent transfer Workspace rebind history",
        ));
    }
    let mut high_water = anchor.created_at_ms;
    let mut rebinds = Vec::with_capacity(raw.len());
    for row in raw {
        let rebind_participation = checked_uuid(row.participation_id, "rebind Participation")?;
        let rebind_profile = checked_uuid(row.profile_id, "rebind Profile")?;
        let rebind_participant = checked_uuid(row.participant_id, "rebind participant")?;
        if rebind_participation != anchor.participation_id
            || rebind_profile != intent.target_profile_id().into_bytes()
            || !participant_ids.contains(&rebind_participant)
            || row.bound_at_ms < 0
        {
            return Err(OperationsError::Corrupt(
                "permanent transfer Workspace rebind anchor",
            ));
        }
        high_water = high_water.max(row.bound_at_ms);
        rebinds.push(CapturedRebind {
            request_id: checked_uuid(row.request_id, "rebind request")?,
            participant_id: rebind_participant,
            profile_mapping_request_id: checked_uuid(
                row.profile_mapping_request_id,
                "rebind Profile request",
            )?,
            profile_revision: positive_i64(row.profile_revision, "rebind Profile revision")?,
            previous_session_id: checked_uuid(row.previous_session_id, "previous Session")?,
            new_session_id: checked_uuid(row.new_session_id, "new Session")?,
            new_session_revision: positive_i64(row.new_session_revision, "new Session revision")?,
            participant_membership_revision: positive_revision(
                row.participant_membership_revision,
                "rebind participant revision",
            )?,
            expected_participation_revision: positive_revision(
                row.expected_participation_revision,
                "expected rebind revision",
            )?,
            result_participation_revision: positive_revision(
                row.result_participation_revision,
                "result rebind revision",
            )?,
            bound_at_ms: row.bound_at_ms,
        });
    }
    Ok((rebinds, high_water))
}

fn load_members(
    connection: &Connection,
    intent: &TeamPermanentTransferIntentReceipt,
    participant_ids: &std::collections::BTreeSet<[u8; 16]>,
) -> Result<Vec<CapturedMember>, OperationsError> {
    if participant_ids.is_empty() || participant_ids.len() > MAX_MAPPINGS {
        return Err(OperationsError::Bounds(
            "permanent transfer Workspace participants",
        ));
    }
    let mut members = Vec::with_capacity(participant_ids.len());
    for participant_id in participant_ids {
        let raw: Option<RawMember> = connection
            .query_row(
            "SELECT substr(session_id,1,17),principal_kind,substr(principal_id,1,17),membership_revision,created_at_ms FROM ops_logical_members WHERE workspace_id=?1 AND participant_id=?2",
            params![
                intent.workspace_id().into_uuid().as_bytes(),
                participant_id.as_slice(),
            ],
            |row| {
                Ok(RawMember {
                    session: row.get(0)?,
                    principal_kind: row.get(1)?,
                    principal_id: row.get(2)?,
                    membership_revision: row.get(3)?,
                    created_at_ms: row.get(4)?,
                })
            },
            )
            .optional()?;
        let Some(RawMember {
            session,
            principal_kind,
            principal_id,
            membership_revision,
            created_at_ms,
        }) = raw
        else {
            return Err(OperationsError::Conflict(
                "permanent transfer Workspace logical member missing",
            ));
        };
        let member = CapturedMember {
            participant_id: *participant_id,
            current_session_id: wire::id(session)?,
            principal_kind,
            principal_id: principal_id.map(wire::id).transpose()?,
            membership_revision: positive_revision(membership_revision, "logical member revision")?,
            created_at_ms,
        };
        if created_at_ms < 0 || !valid_principal(member.principal_kind, member.principal_id) {
            return Err(OperationsError::Corrupt(
                "permanent transfer Workspace logical member",
            ));
        }
        members.push(member);
    }
    Ok(members)
}

fn ensure_workspace_capture_complete(
    connection: &Connection,
    intent: &TeamPermanentTransferIntentReceipt,
) -> Result<super::pending_registration::RegistrationCensus, OperationsError> {
    let unknown_bindings: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ops_current_effect_bindings b WHERE b.workspace_id=?1 AND NOT EXISTS(SELECT 1 FROM ops_workspace_participation_mappings m WHERE m.workspace_id=b.workspace_id AND m.participant_id=b.participant_id)",
        params![intent.workspace_id().into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    if unknown_bindings != 0 {
        return Err(OperationsError::Conflict(
            "permanent transfer Workspace effect binding attribution unresolved",
        ));
    }
    let registration_census = super::pending_registration::ensure_settled(
        connection,
        intent.workspace_id(),
        intent.target_profile_id(),
        intent.target_participation_id().into_uuid(),
    )?;
    let pending_replacement: bool = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM ops_workspace_participation_replacement_registrations
            WHERE workspace_id=?1 AND participation_id=?2 AND phase!=3
        )",
        params![
            intent.workspace_id().into_uuid().as_bytes(),
            intent.target_participation_id().into_uuid().as_bytes(),
        ],
        |row| row.get(0),
    )?;
    if pending_replacement {
        return Err(OperationsError::Conflict(
            "permanent transfer Workspace target replacement pending",
        ));
    }
    Ok(registration_census)
}

fn load_sessions(
    connection: &Connection,
    intent: &TeamPermanentTransferIntentReceipt,
) -> Result<Vec<CapturedSession>, OperationsError> {
    let mut statement = connection.prepare(
        "WITH mappings AS (
            SELECT participant_id,session_id FROM ops_workspace_participation_mappings
            WHERE workspace_id=?1 AND participation_id=?2
        ), sessions AS (
            SELECT participant_id,session_id FROM mappings
            UNION SELECT m.participant_id,l.session_id FROM mappings m
                JOIN ops_logical_members l ON l.workspace_id=?1 AND l.participant_id=m.participant_id
            UNION SELECT participant_id,previous_session_id FROM ops_workspace_participation_session_rebinds
                WHERE workspace_id=?1 AND participation_id=?2
            UNION SELECT participant_id,new_session_id FROM ops_workspace_participation_session_rebinds
                WHERE workspace_id=?1 AND participation_id=?2
            UNION SELECT b.participant_id,b.session_id FROM ops_current_effect_bindings b
                JOIN mappings m ON m.participant_id=b.participant_id WHERE b.workspace_id=?1
        ) SELECT substr(participant_id,1,17),substr(session_id,1,17)
          FROM sessions ORDER BY session_id,participant_id LIMIT ?3",
    )?;
    let rows = statement.query_map(
        params![
            intent.workspace_id().into_uuid().as_bytes(),
            intent.target_participation_id().into_uuid().as_bytes(),
            query_limit(MAX_SESSIONS)?,
        ],
        |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
    )?;
    let raw = rows.collect::<Result<Vec<_>, _>>()?;
    if raw.len() > MAX_SESSIONS {
        return Err(OperationsError::Bounds(
            "permanent transfer Workspace Sessions",
        ));
    }
    let mut sessions = Vec::with_capacity(raw.len());
    for (participant, session) in raw {
        sessions.push(CapturedSession {
            participant_id: wire::id(participant)?,
            session_id: wire::id(session)?,
        });
    }
    if sessions.is_empty()
        || sessions
            .windows(2)
            .any(|rows| session_order(rows[0]) >= session_order(rows[1]))
    {
        return Err(OperationsError::Conflict(
            "permanent transfer Workspace Session attribution",
        ));
    }
    Ok(sessions)
}

fn load_bindings(
    connection: &Connection,
    intent: &TeamPermanentTransferIntentReceipt,
    members: &[CapturedMember],
) -> Result<Vec<CapturedBinding>, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT substr(b.participant_id,1,17),substr(b.activity_id,1,17),
                substr(b.agent_id,1,17),substr(b.session_id,1,17),
                l.principal_kind,substr(l.principal_id,1,17),b.source_kind,
                substr(b.source_request_id,1,17),substr(l.session_id,1,17)
           FROM ops_current_effect_bindings b
           JOIN ops_workspace_participation_mappings m
             ON m.workspace_id=b.workspace_id AND m.participant_id=b.participant_id
           JOIN ops_logical_members l
             ON l.workspace_id=b.workspace_id AND l.participant_id=b.participant_id
          WHERE b.workspace_id=?1 AND m.participation_id=?2
          ORDER BY b.agent_id LIMIT ?3",
    )?;
    let rows = statement.query_map(
        params![
            intent.workspace_id().into_uuid().as_bytes(),
            intent.target_participation_id().into_uuid().as_bytes(),
            query_limit(MAX_BINDINGS)?,
        ],
        |row| {
            Ok(RawBinding {
                participant_id: row.get(0)?,
                activity_id: row.get(1)?,
                agent_id: row.get(2)?,
                session_id: row.get(3)?,
                current_session_id: row.get(8)?,
                principal_kind: row.get(4)?,
                principal_id: row.get(5)?,
                source_kind: row.get(6)?,
                source_request_id: row.get(7)?,
            })
        },
    )?;
    let raw = rows.collect::<Result<Vec<_>, _>>()?;
    if raw.len() > MAX_BINDINGS {
        return Err(OperationsError::Bounds(
            "permanent transfer Workspace effect bindings",
        ));
    }
    let mut bindings = Vec::with_capacity(raw.len());
    for row in raw {
        let participant_id = wire::id(row.participant_id)?;
        let session_id = wire::id(row.session_id)?;
        let current_session_id = wire::id(row.current_session_id)?;
        let member = members
            .iter()
            .find(|member| member.participant_id == participant_id)
            .ok_or(OperationsError::Corrupt(
                "permanent transfer Workspace binding participant missing",
            ))?;
        if session_id != member.current_session_id
            || current_session_id != member.current_session_id
        {
            return Err(OperationsError::Conflict(
                "permanent transfer Workspace effect binding pending reconciliation",
            ));
        }
        bindings.push(CapturedBinding {
            participant_id,
            activity_id: wire::id(row.activity_id)?,
            agent_id: wire::id(row.agent_id)?,
            session_id,
            principal_kind: row.principal_kind,
            principal_id: row.principal_id.map(wire::id).transpose()?,
            source_kind: row.source_kind,
            source_request_id: wire::id(row.source_request_id)?,
        });
    }
    for binding in &bindings {
        let member = members
            .iter()
            .find(|member| member.participant_id == binding.participant_id)
            .ok_or(OperationsError::Corrupt(
                "permanent transfer Workspace binding participant missing",
            ))?;
        if binding.principal_kind != member.principal_kind
            || binding.principal_id != member.principal_id
        {
            return Err(OperationsError::Conflict(
                "permanent transfer Workspace effect binding pending reconciliation",
            ));
        }
    }
    if bindings
        .iter()
        .any(|binding| !matches!(binding.source_kind, 0 | 1))
        || bindings
            .windows(2)
            .any(|rows| rows[0].agent_id >= rows[1].agent_id)
    {
        return Err(OperationsError::Corrupt(
            "permanent transfer Workspace effect binding",
        ));
    }
    Ok(bindings)
}

fn ensure_id(value: &[u8; 16], context: &'static str) -> Result<(), OperationsError> {
    if Uuid::from_bytes(*value).is_nil() {
        return Err(OperationsError::Corrupt(context));
    }
    Ok(())
}

fn checked_uuid(value: Vec<u8>, context: &'static str) -> Result<[u8; 16], OperationsError> {
    wire::id(value).map_err(|_| OperationsError::Corrupt(context))
}

fn positive_i64(value: i64, context: &'static str) -> Result<i64, OperationsError> {
    if value < 1 {
        return Err(OperationsError::Corrupt(context));
    }
    Ok(value)
}

fn positive_revision(value: i64, context: &'static str) -> Result<u64, OperationsError> {
    u64::try_from(positive_i64(value, context)?).map_err(|_| OperationsError::Corrupt(context))
}

fn query_limit(maximum: usize) -> Result<i64, OperationsError> {
    i64::try_from(maximum.saturating_add(1))
        .map_err(|_| OperationsError::Bounds("permanent transfer Workspace read limit"))
}

fn valid_principal(kind: i64, id: Option<[u8; 16]>) -> bool {
    match (kind, id) {
        (0, None) => true,
        (1 | 2, Some(id)) => !Uuid::from_bytes(id).is_nil(),
        _ => false,
    }
}

fn mapping_order(row: CapturedMapping) -> ([u8; 16], [u8; 16]) {
    (row.session_id, row.request_id)
}

fn session_order(row: CapturedSession) -> ([u8; 16], [u8; 16]) {
    (row.session_id, row.participant_id)
}
