//! Post-commit evidence that a specific Team membership event is durable.
//!
//! This module does not grant election eligibility or leadership authority.
//! It records an authenticated Profile participation observing its exact
//! committed `JOINED` event when the event is backed by either an explicit
//! Team-capacity CREATE roster or an approved expansion-slot JOIN. Legacy
//! `joined_at_ms` values and unmarked events never create an anchor. The
//! sampled time is taken only after the event and its source proof are read
//! inside a separate IMMEDIATE transaction.

use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, TimestampMillis, WorkspaceId};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::{
    OperationsCatalog, OperationsError, TeamMembershipState, WorkspaceParticipationId,
    operational_actor::{OperationalActor, ResolvedOperationalActor, resolve_actor},
};

mod schema;
#[cfg(test)]
mod tests;

const MEMBERSHIP_EVENT_DIGEST_DOMAIN: &[u8] =
    b"heterarchy-orchestration-team-membership-event-v1\0";
const TENURE_SOURCE_DIGEST_DOMAIN: &[u8] = b"heterarchy-orchestration-team-tenure-source-v1\0";

/// Source marker that permits a new post-commit tenure anchor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeamMembershipTenureSourceKind {
    ExplicitCapacityCreate,
    ApprovedSlotJoin,
}

impl TeamMembershipTenureSourceKind {
    const fn encode(self) -> &'static str {
        match self {
            Self::ExplicitCapacityCreate => "CAPACITY_CREATE",
            Self::ApprovedSlotJoin => "APPROVED_SLOT_JOIN",
        }
    }

    fn decode(value: &str) -> Option<Self> {
        match value {
            "CAPACITY_CREATE" => Some(Self::ExplicitCapacityCreate),
            "APPROVED_SLOT_JOIN" => Some(Self::ApprovedSlotJoin),
            _ => None,
        }
    }
}

/// An immutable, authenticated post-commit membership-time observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamMembershipTenureAnchor {
    workspace_id: WorkspaceId,
    team_id: Uuid,
    participation_id: WorkspaceParticipationId,
    profile_id: Uuid,
    membership_revision: u64,
    membership_request_id: Uuid,
    membership_state: TeamMembershipState,
    membership_observed_at: TimestampMillis,
    membership_event_digest: [u8; 32],
    source_kind: TeamMembershipTenureSourceKind,
    source_digest: [u8; 32],
    anchored_at: TimestampMillis,
    actor_activity_id: ActivityId,
    actor_agent_id: AgentInstanceId,
    actor_session_id: AgentSessionId,
    actor_participant_id: Uuid,
    actor_display_name: String,
    replayed: bool,
}

impl TeamMembershipTenureAnchor {
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }
    #[must_use]
    pub const fn team_id(&self) -> Uuid {
        self.team_id
    }
    #[must_use]
    pub const fn participation_id(&self) -> WorkspaceParticipationId {
        self.participation_id
    }
    #[must_use]
    pub const fn profile_id(&self) -> Uuid {
        self.profile_id
    }
    #[must_use]
    pub const fn membership_revision(&self) -> u64 {
        self.membership_revision
    }
    #[must_use]
    pub const fn membership_request_id(&self) -> Uuid {
        self.membership_request_id
    }
    #[must_use]
    pub const fn membership_state(&self) -> TeamMembershipState {
        self.membership_state
    }
    #[must_use]
    pub const fn membership_observed_at(&self) -> TimestampMillis {
        self.membership_observed_at
    }
    #[must_use]
    pub const fn membership_event_digest(&self) -> [u8; 32] {
        self.membership_event_digest
    }
    #[must_use]
    pub const fn source_kind(&self) -> TeamMembershipTenureSourceKind {
        self.source_kind
    }
    #[must_use]
    pub const fn source_digest(&self) -> [u8; 32] {
        self.source_digest
    }
    #[must_use]
    pub const fn anchored_at(&self) -> TimestampMillis {
        self.anchored_at
    }
    #[must_use]
    pub const fn actor_activity_id(&self) -> ActivityId {
        self.actor_activity_id
    }
    #[must_use]
    pub const fn actor_agent_id(&self) -> AgentInstanceId {
        self.actor_agent_id
    }
    #[must_use]
    pub const fn actor_session_id(&self) -> AgentSessionId {
        self.actor_session_id
    }
    #[must_use]
    pub const fn actor_participant_id(&self) -> Uuid {
        self.actor_participant_id
    }
    #[must_use]
    pub fn actor_display_name(&self) -> &str {
        &self.actor_display_name
    }
    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Request to attest the current authenticated Profile's exact membership
/// event after it has committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamMembershipTenureAnchorCommand {
    workspace_id: WorkspaceId,
    team_id: Uuid,
    participation_id: WorkspaceParticipationId,
    membership_revision: u64,
    membership_request_id: Uuid,
    actor: OperationalActor,
}

impl TeamMembershipTenureAnchorCommand {
    /// Construct a typed Team-membership anchor request.
    ///
    /// # Errors
    /// Rejects nil Team/request IDs and zero membership revisions.
    pub fn new(
        workspace_id: WorkspaceId,
        team_id: Uuid,
        participation_id: WorkspaceParticipationId,
        membership_revision: u64,
        membership_request_id: Uuid,
        actor: OperationalActor,
    ) -> Result<Self, OperationsError> {
        if team_id.is_nil() || membership_request_id.is_nil() || membership_revision == 0 {
            return Err(OperationsError::Conflict(
                "Team membership tenure anchor identity",
            ));
        }
        Ok(Self {
            workspace_id,
            team_id,
            participation_id,
            membership_revision,
            membership_request_id,
            actor,
        })
    }
}

pub(crate) const fn schema_objects() -> &'static [(&'static str, &'static str, &'static str)] {
    schema::schema_objects()
}

pub(crate) fn create_schema(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), OperationsError> {
    schema::create_schema(transaction)
}

impl OperationsCatalog {
    /// Record one authenticated, post-commit observation for the exact current
    /// membership generation. The Operations owner samples its clock only after
    /// the membership event is found inside a separate IMMEDIATE transaction.
    ///
    /// This creates no election eligibility; it supplies a source fact for a
    /// future policy consumer. Replayed request IDs are resolved before the
    /// clock is sampled.
    ///
    /// # Errors
    /// Rejects foreign or native actors, missing/changed membership events,
    /// clock regression/overflow, altered replay, and uncertain commits.
    pub fn establish_team_membership_tenure_anchor(
        &mut self,
        command: &TeamMembershipTenureAnchorCommand,
    ) -> Result<TeamMembershipTenureAnchor, OperationsError> {
        self.establish_team_membership_tenure_anchor_with_clock(command, trusted_now)
    }

    fn establish_team_membership_tenure_anchor_with_clock(
        &mut self,
        command: &TeamMembershipTenureAnchorCommand,
        trusted_now: impl FnOnce() -> Result<TimestampMillis, OperationsError>,
    ) -> Result<TeamMembershipTenureAnchor, OperationsError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        crate::teams::ensure_verified_workspace(&tx, command.workspace_id)?;
        let resolved = resolve_actor(&tx, command.workspace_id, command.actor)?;
        let actor = resolved_profile_actor(resolved, command.actor)?;
        if actor.participation_id != command.participation_id {
            return Err(OperationsError::Conflict(
                "Team tenure actor participation changed",
            ));
        }

        if let Some(existing) = load_anchor_by_membership(&tx, command)? {
            ensure_anchor_source_matches(&tx, &existing)?;
            if !anchor_matches_command(&existing, command, &actor) {
                return Err(OperationsError::Conflict("Team tenure anchor replay"));
            }
            tx.commit()?;
            return Ok(with_replay(existing));
        }

        if load_anchor_by_request(&tx, command)?.is_some() {
            return Err(OperationsError::Conflict("Team tenure request identity"));
        }
        let source_event = validate_exact_membership_event(&tx, command, &actor)?;
        let anchored_at = trusted_now()?;
        if anchored_at < source_event.observed_at {
            return Err(OperationsError::Conflict(
                "Team tenure anchor clock regression",
            ));
        }
        if let Some(latest) = latest_anchor(
            &tx,
            command.workspace_id,
            command.team_id,
            command.participation_id,
        )? {
            ensure_anchor_source_matches(&tx, &latest)?;
            if anchored_at < latest.anchored_at {
                return Err(OperationsError::Conflict(
                    "Team tenure history time regression",
                ));
            }
        }

        let anchor = TeamMembershipTenureAnchor {
            workspace_id: command.workspace_id,
            team_id: command.team_id,
            participation_id: command.participation_id,
            profile_id: actor.profile_id,
            membership_revision: command.membership_revision,
            membership_request_id: command.membership_request_id,
            membership_state: TeamMembershipState::Joined,
            membership_observed_at: source_event.observed_at,
            membership_event_digest: source_event.digest,
            source_kind: source_event.source_kind,
            source_digest: source_event.source_digest,
            anchored_at,
            actor_activity_id: actor.activity_id,
            actor_agent_id: actor.agent_id,
            actor_session_id: actor.session_id,
            actor_participant_id: actor.participant_id,
            actor_display_name: actor.display_name,
            replayed: false,
        };
        insert_anchor(&tx, &anchor)?;
        let commit = tx.commit();
        if let Err(source) = commit {
            return match load_anchor_by_membership(&self.connection, command) {
                Ok(Some(existing))
                    if ensure_anchor_source_matches(&self.connection, &existing).is_ok()
                        && anchor_matches_command(&existing, command, &actor) =>
                {
                    Ok(with_replay(existing))
                }
                Ok(Some(_)) => Err(OperationsError::Conflict(
                    "Team tenure anchor unknown replay",
                )),
                Ok(None) | Err(_) => Err(OperationsError::OperationCommitUnconfirmed {
                    workspace_id: command.workspace_id,
                    request_id: command.membership_request_id,
                    source,
                }),
            };
        }
        Ok(anchor)
    }

    /// Read this logical Profile participation's anchor for one exact Team
    /// membership generation. Reconnected Sessions retain their own evidence.
    ///
    /// # Errors
    /// Rejects non-Profile actors, invalid identities and corrupt storage.
    pub fn team_membership_tenure_anchor(
        &self,
        workspace_id: WorkspaceId,
        team_id: Uuid,
        participation_id: WorkspaceParticipationId,
        membership_revision: u64,
        actor: OperationalActor,
    ) -> Result<Option<TeamMembershipTenureAnchor>, OperationsError> {
        if team_id.is_nil() || membership_revision == 0 {
            return Err(OperationsError::Conflict(
                "Team membership tenure read identity",
            ));
        }
        let resolved = resolve_actor(&self.connection, workspace_id, actor)?;
        let actor = resolved_profile_actor(resolved, actor)?;
        if actor.participation_id != participation_id {
            return Ok(None);
        }
        let Some(anchor) = load_anchor_by_identity(
            &self.connection,
            workspace_id,
            team_id,
            participation_id,
            membership_revision,
        )?
        else {
            return Ok(None);
        };
        if anchor.profile_id != actor.profile_id {
            return Ok(None);
        }
        ensure_anchor_source_matches(&self.connection, &anchor)?;
        Ok(Some(anchor))
    }
}

#[derive(Debug, Clone)]
struct ProfileActor {
    participation_id: WorkspaceParticipationId,
    profile_id: Uuid,
    participant_id: Uuid,
    activity_id: ActivityId,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    display_name: String,
}

fn resolved_profile_actor(
    actor: ResolvedOperationalActor,
    source_actor: OperationalActor,
) -> Result<ProfileActor, OperationsError> {
    match actor {
        ResolvedOperationalActor::ProfileParticipant {
            participation_id,
            profile_id,
            participant_id,
            session_id,
            display_name,
        } => match source_actor {
            OperationalActor::Participant {
                activity_id,
                agent_id,
                session_id: source_session_id,
            } if source_session_id == session_id => Ok(ProfileActor {
                participation_id,
                profile_id,
                participant_id,
                activity_id,
                agent_id,
                session_id,
                display_name,
            }),
            OperationalActor::Participant { .. } => Err(OperationsError::Conflict(
                "Team tenure source Session changed",
            )),
            OperationalActor::LocalOperator => Err(OperationsError::Conflict(
                "Team tenure requires a Profile Session",
            )),
        },
        _ => Err(OperationsError::Conflict(
            "Team tenure requires a Profile participation",
        )),
    }
}

#[derive(Debug, Clone, Copy)]
struct MembershipSourceEvent {
    state: TeamMembershipState,
    revision: u64,
    observed_at: TimestampMillis,
    digest: [u8; 32],
    source_kind: TeamMembershipTenureSourceKind,
    source_digest: [u8; 32],
}

#[derive(Debug, Clone, Copy)]
struct DecodedMembershipEvent {
    state: TeamMembershipState,
    revision: u64,
    observed_at: TimestampMillis,
    digest: [u8; 32],
}

#[derive(Debug)]
struct RawMembershipSourceEvent {
    workspace_id: Vec<u8>,
    team_id: Vec<u8>,
    participation_id: Vec<u8>,
    request_id: Vec<u8>,
    state: String,
    revision: i64,
    actor_key: String,
    actor_display_name: String,
    actor_participation_id: Option<Vec<u8>>,
    actor_profile_id: Option<Vec<u8>>,
    actor_participant_id: Option<Vec<u8>>,
    actor_session_id: Option<Vec<u8>>,
    observed_at: i64,
    role: Option<String>,
}

fn validate_exact_membership_event(
    connection: &Connection,
    command: &TeamMembershipTenureAnchorCommand,
    actor: &ProfileActor,
) -> Result<MembershipSourceEvent, OperationsError> {
    let raw = load_membership_event_raw(
        connection,
        command.workspace_id,
        command.team_id,
        command.participation_id,
        command.membership_request_id,
    )?
    .ok_or(OperationsError::Conflict("Team membership event missing"))?;
    let event = decode_membership_source_event(&raw)?;
    if event.state != TeamMembershipState::Joined || event.revision != command.membership_revision {
        return Err(OperationsError::Conflict("Team membership event changed"));
    }
    let member: Option<(String, i64)> = connection.query_row(
        "SELECT state,revision FROM ops_team_members WHERE workspace_id=?1 AND team_id=?2 AND participation_id=?3",
        params![command.workspace_id.into_uuid().as_bytes(), command.team_id.as_bytes(), command.participation_id.into_uuid().as_bytes()],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ).optional()?;
    if !matches!(member, Some((ref member_state, member_revision)) if member_state == "JOINED" && u64::try_from(member_revision).ok() == Some(command.membership_revision))
    {
        return Err(OperationsError::Conflict(
            "Team membership is no longer current",
        ));
    }
    let identity = MembershipSourceIdentity {
        workspace_id: command.workspace_id,
        team_id: command.team_id,
        participation_id: command.participation_id,
        profile_id: actor.profile_id,
        membership_revision: command.membership_revision,
        request_id: command.membership_request_id,
    };
    let (source_kind, source_digest) =
        membership_source_proof(connection, &identity, &raw, event.digest, true)?;
    Ok(MembershipSourceEvent {
        source_kind,
        source_digest,
        ..event
    })
}

#[derive(Debug, Clone, Copy)]
struct MembershipSourceIdentity {
    workspace_id: WorkspaceId,
    team_id: Uuid,
    participation_id: WorkspaceParticipationId,
    profile_id: Uuid,
    membership_revision: u64,
    request_id: Uuid,
}

fn load_membership_event_raw(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    participation_id: WorkspaceParticipationId,
    request_id: Uuid,
) -> Result<Option<RawMembershipSourceEvent>, OperationsError> {
    let raw: Option<RawMembershipSourceEvent> = connection.query_row(
        "SELECT workspace_id,team_id,participation_id,request_id,state,membership_revision,actor_key,actor_display_name,actor_participation_id,actor_profile_id,actor_participant_id,actor_session_id,observed_at_ms,role FROM ops_team_member_events WHERE request_id=?1 AND workspace_id=?2 AND team_id=?3 AND participation_id=?4",
        params![request_id.as_bytes(), workspace_id.into_uuid().as_bytes(), team_id.as_bytes(), participation_id.into_uuid().as_bytes()],
        |row| Ok(RawMembershipSourceEvent {
            workspace_id: row.get(0)?,
            team_id: row.get(1)?,
            participation_id: row.get(2)?,
            request_id: row.get(3)?,
            state: row.get(4)?,
            revision: row.get(5)?,
            actor_key: row.get(6)?,
            actor_display_name: row.get(7)?,
            actor_participation_id: row.get(8)?,
            actor_profile_id: row.get(9)?,
            actor_participant_id: row.get(10)?,
            actor_session_id: row.get(11)?,
            observed_at: row.get(12)?,
            role: row.get(13)?,
        }),
    ).optional()?;
    Ok(raw)
}

fn decode_membership_source_event(
    raw: &RawMembershipSourceEvent,
) -> Result<DecodedMembershipEvent, OperationsError> {
    let state = match raw.state.as_str() {
        "JOINED" => TeamMembershipState::Joined,
        "LEFT" => TeamMembershipState::Left,
        _ => return Err(OperationsError::Corrupt("Team membership event state")),
    };
    let revision = u64::try_from(raw.revision)
        .map_err(|_| OperationsError::Corrupt("Team membership event revision"))?;
    let observed_at = TimestampMillis::new(raw.observed_at)
        .map_err(|_| OperationsError::Corrupt("Team membership event time"))?;
    let digest = membership_source_event_digest(raw)?;
    Ok(DecodedMembershipEvent {
        state,
        revision,
        observed_at,
        digest,
    })
}

fn membership_source_event_digest(
    raw: &RawMembershipSourceEvent,
) -> Result<[u8; 32], OperationsError> {
    let mut digest = Sha256::new();
    digest.update(MEMBERSHIP_EVENT_DIGEST_DOMAIN);
    for field in [
        &raw.workspace_id,
        &raw.team_id,
        &raw.participation_id,
        &raw.request_id,
    ] {
        update_digest_bytes(&mut digest, field)?;
    }
    update_digest_bytes(&mut digest, raw.state.as_bytes())?;
    digest.update(raw.revision.to_be_bytes());
    update_digest_bytes(&mut digest, raw.actor_key.as_bytes())?;
    update_digest_bytes(&mut digest, raw.actor_display_name.as_bytes())?;
    for field in [
        raw.actor_participation_id.as_deref(),
        raw.actor_profile_id.as_deref(),
        raw.actor_participant_id.as_deref(),
        raw.actor_session_id.as_deref(),
    ] {
        update_digest_optional_bytes(&mut digest, field)?;
    }
    digest.update(raw.observed_at.to_be_bytes());
    update_digest_optional_bytes(&mut digest, raw.role.as_deref().map(str::as_bytes))?;
    Ok(digest.finalize().into())
}

fn update_digest_bytes(digest: &mut Sha256, value: &[u8]) -> Result<(), OperationsError> {
    let length = u64::try_from(value.len())
        .map_err(|_| OperationsError::Bounds("Team tenure source digest"))?;
    digest.update(length.to_be_bytes());
    digest.update(value);
    Ok(())
}

fn update_digest_optional_bytes(
    digest: &mut Sha256,
    value: Option<&[u8]>,
) -> Result<(), OperationsError> {
    match value {
        Some(value) => {
            digest.update([1]);
            update_digest_bytes(digest, value)?;
        }
        None => digest.update([0]),
    }
    Ok(())
}

#[derive(Debug)]
struct RawTeamRequestSource {
    operation: String,
    actor_key: String,
    actor_display_name: String,
    actor_participation_id: Option<Vec<u8>>,
    actor_profile_id: Option<Vec<u8>>,
    actor_participant_id: Option<Vec<u8>>,
    actor_session_id: Option<Vec<u8>>,
    expected_team_revision: Option<i64>,
    result_team_state: String,
    result_team_revision: i64,
    result_team_updated_at: i64,
    result_membership_state: Option<String>,
    result_membership_participation_id: Option<Vec<u8>>,
    result_membership_revision: Option<i64>,
    result_membership_joined_at: Option<i64>,
    result_membership_left_at: Option<i64>,
    created: i64,
    observed_at: i64,
    request_actor_kind: Option<String>,
    request_actor_activity_id: Option<Vec<u8>>,
    request_actor_agent_id: Option<Vec<u8>>,
    request_leadership_revision: Option<i64>,
    role: Option<String>,
    result_membership_role: Option<String>,
}

fn load_team_request_source(
    connection: &Connection,
    identity: &MembershipSourceIdentity,
) -> Result<Option<RawTeamRequestSource>, OperationsError> {
    connection.query_row(
        "SELECT operation,actor_key,actor_display_name,actor_participation_id,actor_profile_id,actor_participant_id,actor_session_id,expected_team_revision,result_team_state,result_team_revision,result_team_updated_at_ms,result_membership_state,result_membership_participation_id,result_membership_revision,result_membership_joined_at_ms,result_membership_left_at_ms,created,observed_at_ms,request_actor_kind,request_actor_activity_id,request_actor_agent_id,request_leadership_revision,role,result_membership_role FROM ops_team_requests WHERE request_id=?1 AND workspace_id=?2 AND team_id=?3",
        params![identity.request_id.as_bytes(),identity.workspace_id.into_uuid().as_bytes(),identity.team_id.as_bytes()],
        |row| Ok(RawTeamRequestSource {
            operation: row.get(0)?, actor_key: row.get(1)?, actor_display_name: row.get(2)?,
            actor_participation_id: row.get(3)?, actor_profile_id: row.get(4)?, actor_participant_id: row.get(5)?,
            actor_session_id: row.get(6)?, expected_team_revision: row.get(7)?, result_team_state: row.get(8)?,
            result_team_revision: row.get(9)?, result_team_updated_at: row.get(10)?, result_membership_state: row.get(11)?,
            result_membership_participation_id: row.get(12)?, result_membership_revision: row.get(13)?,
            result_membership_joined_at: row.get(14)?, result_membership_left_at: row.get(15)?, created: row.get(16)?,
            observed_at: row.get(17)?, request_actor_kind: row.get(18)?, request_actor_activity_id: row.get(19)?,
            request_actor_agent_id: row.get(20)?, request_leadership_revision: row.get(21)?, role: row.get(22)?,
            result_membership_role: row.get(23)?,
        }),
    ).optional().map_err(Into::into)
}

fn team_request_source_digest(source: &RawTeamRequestSource) -> Result<[u8; 32], OperationsError> {
    let mut digest = Sha256::new();
    digest.update(b"heterarchy-orchestration-team-request-source-v1\0");
    update_digest_bytes(&mut digest, source.operation.as_bytes())?;
    update_digest_bytes(&mut digest, source.actor_key.as_bytes())?;
    update_digest_bytes(&mut digest, source.actor_display_name.as_bytes())?;
    for value in [
        source.actor_participation_id.as_deref(),
        source.actor_profile_id.as_deref(),
        source.actor_participant_id.as_deref(),
        source.actor_session_id.as_deref(),
    ] {
        update_digest_optional_bytes(&mut digest, value)?;
    }
    update_digest_optional_i64(&mut digest, source.expected_team_revision);
    update_digest_bytes(&mut digest, source.result_team_state.as_bytes())?;
    digest.update(source.result_team_revision.to_be_bytes());
    digest.update(source.result_team_updated_at.to_be_bytes());
    update_digest_optional_text(&mut digest, source.result_membership_state.as_deref())?;
    update_digest_optional_bytes(
        &mut digest,
        source.result_membership_participation_id.as_deref(),
    )?;
    update_digest_optional_i64(&mut digest, source.result_membership_revision);
    update_digest_optional_i64(&mut digest, source.result_membership_joined_at);
    update_digest_optional_i64(&mut digest, source.result_membership_left_at);
    digest.update(source.created.to_be_bytes());
    digest.update(source.observed_at.to_be_bytes());
    update_digest_optional_text(&mut digest, source.request_actor_kind.as_deref())?;
    update_digest_optional_bytes(&mut digest, source.request_actor_activity_id.as_deref())?;
    update_digest_optional_bytes(&mut digest, source.request_actor_agent_id.as_deref())?;
    update_digest_optional_i64(&mut digest, source.request_leadership_revision);
    update_digest_optional_text(&mut digest, source.role.as_deref())?;
    update_digest_optional_text(&mut digest, source.result_membership_role.as_deref())?;
    Ok(digest.finalize().into())
}

fn update_digest_optional_i64(digest: &mut Sha256, value: Option<i64>) {
    match value {
        Some(value) => {
            digest.update([1]);
            digest.update(value.to_be_bytes());
        }
        None => digest.update([0]),
    }
}

fn update_digest_optional_text(
    digest: &mut Sha256,
    value: Option<&str>,
) -> Result<(), OperationsError> {
    update_digest_optional_bytes(digest, value.map(str::as_bytes))
}

fn membership_source_proof(
    connection: &Connection,
    identity: &MembershipSourceIdentity,
    event: &RawMembershipSourceEvent,
    event_digest: [u8; 32],
    require_roster_actor_source: bool,
) -> Result<(TeamMembershipTenureSourceKind, [u8; 32]), OperationsError> {
    let request = load_team_request_source(connection, identity)?.ok_or(
        OperationsError::Conflict("Team tenure source request missing"),
    )?;
    if request.observed_at != event.observed_at
        || event.workspace_id.as_slice() != identity.workspace_id.into_uuid().as_bytes().as_slice()
        || event.team_id.as_slice() != identity.team_id.as_bytes().as_slice()
        || event.participation_id.as_slice()
            != identity.participation_id.into_uuid().as_bytes().as_slice()
        || event.request_id.as_slice() != identity.request_id.as_bytes().as_slice()
    {
        return Err(OperationsError::Corrupt(
            "Team tenure source request/event scope",
        ));
    }
    let request_digest = team_request_source_digest(&request)?;
    let (source_kind, marker_digest) = match request.operation.as_str() {
        "CREATE" if request.created == 1 => {
            if event.state != "JOINED"
                || event.revision != 1
                || request.result_team_state != "ACTIVE"
                || request.result_team_revision != 1
                || request.result_team_updated_at != event.observed_at
            {
                return Err(OperationsError::Conflict(
                    "Team tenure initial roster source mismatch",
                ));
            }
            let capacity = crate::team_expansion::capacity_receipt_snapshot_for_create(
                connection,
                identity.workspace_id,
                identity.team_id,
                identity.request_id,
            )?
            .ok_or(OperationsError::Conflict(
                "Team tenure explicit capacity source missing",
            ))?;
            if capacity.workspace_id() != identity.workspace_id
                || capacity.team_id() != identity.team_id
                || capacity.revision() != 1
                || capacity.recorded_at().get() != event.observed_at
                || capacity.max_members().get() < 2
            {
                return Err(OperationsError::Corrupt(
                    "Team tenure capacity create source",
                ));
            }
            let roster_digest = validate_team_create_roster_source(
                connection,
                identity,
                &request,
                event,
                &capacity,
                event_digest,
                require_roster_actor_source,
            )?;
            let capacity_digest = capacity_source_digest(&capacity)?;
            (
                TeamMembershipTenureSourceKind::ExplicitCapacityCreate,
                combine_source_digests(request_digest, capacity_digest, roster_digest),
            )
        }
        "JOIN" => {
            if !((request.created == 1 && identity.membership_revision == 1)
                || (request.created == 0 && identity.membership_revision > 1))
            {
                return Err(OperationsError::Corrupt(
                    "Team tenure JOIN creation/revision mismatch",
                ));
            }
            if request.result_team_state != "ACTIVE"
                || request.result_membership_state.as_deref() != Some("JOINED")
                || request.result_membership_participation_id.as_deref()
                    != Some(identity.participation_id.into_uuid().as_bytes().as_slice())
                || request.result_membership_revision
                    != Some(
                        i64::try_from(identity.membership_revision).map_err(|_| {
                            OperationsError::Bounds("Team tenure membership revision")
                        })?,
                    )
                || request.result_membership_joined_at != Some(event.observed_at)
                || request.result_team_updated_at != event.observed_at
                || request.result_membership_role.as_deref() != event.role.as_deref()
                || request.role.as_deref() != event.role.as_deref()
                || request.actor_participation_id.as_deref()
                    != Some(identity.participation_id.into_uuid().as_bytes().as_slice())
                || request.actor_profile_id.as_deref()
                    != Some(identity.profile_id.as_bytes().as_slice())
                || request.actor_participant_id != event.actor_participant_id
                || request.actor_session_id != event.actor_session_id
                || request.request_actor_kind.as_deref() != Some("PARTICIPANT")
                || request.request_actor_activity_id.is_none()
                || request.request_actor_agent_id.is_none()
                || request.actor_key != event.actor_key
                || request.actor_display_name != event.actor_display_name
                || request.actor_participant_id != event.actor_participant_id
                || event.actor_participation_id.as_deref()
                    != Some(identity.participation_id.into_uuid().as_bytes().as_slice())
                || event.actor_profile_id.as_deref()
                    != Some(identity.profile_id.as_bytes().as_slice())
            {
                return Err(OperationsError::Corrupt("Team tenure join request source"));
            }
            let slot =
                crate::team_expansion::slot_ref_for_join_request(connection, identity.request_id)?
                    .ok_or(OperationsError::Conflict(
                        "Team tenure approved slot use missing",
                    ))?;
            let use_row = load_slot_use_source(connection, identity)?.ok_or(
                OperationsError::Conflict("Team tenure approved slot use missing"),
            )?;
            if use_row.decision_id.as_slice() != slot.approval_id().as_bytes().as_slice()
                || use_row.ordinal != i64::from(slot.ordinal())
                || use_row.workspace_id.as_slice()
                    != identity.workspace_id.into_uuid().as_bytes().as_slice()
                || use_row.team_id.as_slice() != identity.team_id.as_bytes().as_slice()
                || use_row.participation_id.as_slice()
                    != identity.participation_id.into_uuid().as_bytes().as_slice()
                || use_row.profile_id.as_slice() != identity.profile_id.as_bytes().as_slice()
                || use_row.membership_revision
                    != i64::try_from(identity.membership_revision)
                        .map_err(|_| OperationsError::Bounds("Team tenure membership revision"))?
                || use_row.recorded_at != event.observed_at
            {
                return Err(OperationsError::Corrupt(
                    "Team tenure approved slot use source",
                ));
            }
            let slot_digest = slot_use_source_digest(&use_row)?;
            (
                TeamMembershipTenureSourceKind::ApprovedSlotJoin,
                combine_source_digests(request_digest, slot_digest, [0; 32]),
            )
        }
        _ => {
            return Err(OperationsError::Conflict(
                "Team tenure source evidence missing",
            ));
        }
    };
    let mut source = Sha256::new();
    source.update(TENURE_SOURCE_DIGEST_DOMAIN);
    update_digest_bytes(&mut source, source_kind.encode().as_bytes())?;
    source.update(event_digest);
    source.update(marker_digest);
    Ok((source_kind, source.finalize().into()))
}

fn combine_source_digests(first: [u8; 32], second: [u8; 32], third: [u8; 32]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"heterarchy-orchestration-team-tenure-markers-v1\0");
    digest.update(first);
    digest.update(second);
    digest.update(third);
    digest.finalize().into()
}

fn validate_create_source_actor(
    request: &RawTeamRequestSource,
    event: &RawMembershipSourceEvent,
    capacity_actor: &crate::team_expansion::TeamExpansionActor,
) -> Result<(), OperationsError> {
    let actor_key = format!("participant:{}", capacity_actor.participation_id());
    if request.request_actor_kind.as_deref() != Some("PARTICIPANT")
        || request.actor_key != actor_key
        || event.actor_key != actor_key
        || request.actor_display_name != capacity_actor.display_name()
        || event.actor_display_name != capacity_actor.display_name()
        || request.actor_participation_id.as_deref()
            != Some(
                capacity_actor
                    .participation_id()
                    .into_uuid()
                    .as_bytes()
                    .as_slice(),
            )
        || request.actor_profile_id.as_deref()
            != Some(capacity_actor.profile_id().as_bytes().as_slice())
        || request.actor_participant_id.as_deref()
            != Some(capacity_actor.participant_id().as_bytes().as_slice())
        || request.actor_session_id.as_deref()
            != Some(
                capacity_actor
                    .session_id()
                    .into_uuid()
                    .as_bytes()
                    .as_slice(),
            )
        || event.actor_participation_id.as_deref()
            != Some(
                capacity_actor
                    .participation_id()
                    .into_uuid()
                    .as_bytes()
                    .as_slice(),
            )
        || event.actor_profile_id.as_deref()
            != Some(capacity_actor.profile_id().as_bytes().as_slice())
        || event.actor_participant_id.as_deref()
            != Some(capacity_actor.participant_id().as_bytes().as_slice())
        || event.actor_session_id.as_deref()
            != Some(
                capacity_actor
                    .session_id()
                    .into_uuid()
                    .as_bytes()
                    .as_slice(),
            )
        || request.request_actor_activity_id.as_deref()
            != Some(
                capacity_actor
                    .activity_id()
                    .into_uuid()
                    .as_bytes()
                    .as_slice(),
            )
        || request.request_actor_agent_id.as_deref()
            != Some(capacity_actor.agent_id().into_uuid().as_bytes().as_slice())
    {
        return Err(OperationsError::Corrupt("Team tenure create source actor"));
    }
    Ok(())
}

fn validate_team_create_owner_source(
    connection: &Connection,
    identity: &MembershipSourceIdentity,
    request: &RawTeamRequestSource,
    event: &RawMembershipSourceEvent,
    capacity: &crate::team_expansion::TeamCapacitySnapshot,
) -> Result<(), OperationsError> {
    let capacity_actor = capacity.actor();
    if request.expected_team_revision.is_some()
        || request.request_leadership_revision != Some(capacity.leadership_revision())
        || request.result_membership_state.is_some()
        || request.result_membership_participation_id.is_some()
        || request.result_membership_revision.is_some()
        || request.result_membership_joined_at.is_some()
        || request.result_membership_left_at.is_some()
        || request.role.is_some()
        || request.result_membership_role.is_some()
    {
        return Err(OperationsError::Corrupt(
            "Team tenure create leadership/request source",
        ));
    }
    let creator = format!("participant:{}", capacity_actor.participation_id());
    let source: Option<(String, String, Option<Vec<u8>>, Option<Vec<u8>>, Option<Vec<u8>>, Option<Vec<u8>>, i64, Option<i64>)> = connection.query_row(
        "SELECT created_by_key,created_by_display_name,created_by_participation_id,created_by_profile_id,created_by_participant_id,created_by_session_id,created_at_ms,formed_at_ms FROM ops_teams WHERE workspace_id=?1 AND team_id=?2",
        params![identity.workspace_id.into_uuid().as_bytes(), identity.team_id.as_bytes()],
        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?)),
    ).optional()?;
    let Some((
        key,
        display_name,
        participation,
        profile,
        participant,
        session,
        created_at,
        formed_at,
    )) = source
    else {
        return Err(OperationsError::Corrupt(
            "Team tenure create Team source missing",
        ));
    };
    if key != creator
        || display_name != capacity_actor.display_name()
        || participation.as_deref()
            != Some(
                capacity_actor
                    .participation_id()
                    .into_uuid()
                    .as_bytes()
                    .as_slice(),
            )
        || profile.as_deref() != Some(capacity_actor.profile_id().as_bytes().as_slice())
        || participant.as_deref() != Some(capacity_actor.participant_id().as_bytes().as_slice())
        || session.as_deref()
            != Some(
                capacity_actor
                    .session_id()
                    .into_uuid()
                    .as_bytes()
                    .as_slice(),
            )
        || created_at != event.observed_at
        || formed_at != Some(event.observed_at)
    {
        return Err(OperationsError::Corrupt(
            "Team tenure create Team creator source",
        ));
    }
    Ok(())
}

#[derive(Debug)]
struct RawTeamCreateRosterSource {
    ordinal: i64,
    participation_id: Vec<u8>,
    activity_id: Vec<u8>,
    agent_id: Vec<u8>,
    session_id: Vec<u8>,
    participant_id: Vec<u8>,
    profile_id: Vec<u8>,
    role: String,
    membership_revision: i64,
    joined_at: i64,
}

fn load_team_create_roster_source(
    connection: &Connection,
    identity: &MembershipSourceIdentity,
) -> Result<Vec<RawTeamCreateRosterSource>, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT ordinal,participation_id,activity_id,agent_id,session_id,participant_id,profile_id,role,membership_revision,joined_at_ms FROM ops_team_create_roster WHERE request_id=?1 AND workspace_id=?2 AND team_id=?3 ORDER BY ordinal",
    )?;
    let rows = statement.query_map(
        params![
            identity.request_id.as_bytes(),
            identity.workspace_id.into_uuid().as_bytes(),
            identity.team_id.as_bytes(),
        ],
        |row| {
            Ok(RawTeamCreateRosterSource {
                ordinal: row.get(0)?,
                participation_id: row.get(1)?,
                activity_id: row.get(2)?,
                agent_id: row.get(3)?,
                session_id: row.get(4)?,
                participant_id: row.get(5)?,
                profile_id: row.get(6)?,
                role: row.get(7)?,
                membership_revision: row.get(8)?,
                joined_at: row.get(9)?,
            })
        },
    )?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

fn validate_team_create_roster_source(
    connection: &Connection,
    identity: &MembershipSourceIdentity,
    request: &RawTeamRequestSource,
    target_event: &RawMembershipSourceEvent,
    capacity: &crate::team_expansion::TeamCapacitySnapshot,
    target_event_digest: [u8; 32],
    require_roster_actor_source: bool,
) -> Result<[u8; 32], OperationsError> {
    let capacity_actor = capacity.actor();
    validate_team_create_source_actor(request, target_event, capacity_actor)?;
    validate_team_create_owner_source(connection, identity, request, target_event, capacity)?;

    let roster = load_team_create_roster_source(connection, identity)?;
    if !(crate::teams::MIN_FORMING_TEAM_MEMBERS..=crate::teams::MAX_INITIAL_TEAM_MEMBERS)
        .contains(&roster.len())
        || roster.len() > usize::from(capacity.max_members().get())
    {
        return Err(OperationsError::Corrupt("Team tenure create roster size"));
    }
    let expected_event_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ops_team_member_events WHERE request_id=?1 AND workspace_id=?2 AND team_id=?3",
        params![identity.request_id.as_bytes(),identity.workspace_id.into_uuid().as_bytes(),identity.team_id.as_bytes()],
        |row| row.get(0),
    )?;
    if usize::try_from(expected_event_count).ok() != Some(roster.len()) {
        return Err(OperationsError::Corrupt(
            "Team tenure create roster event count",
        ));
    }

    let mut participations = BTreeSet::new();
    let mut participants = BTreeSet::new();
    let mut canonical_roster = Vec::with_capacity(roster.len());
    let mut event_digests = Vec::with_capacity(roster.len());
    let mut target_found = false;
    for (ordinal, member) in roster.iter().enumerate() {
        if member.ordinal
            != i64::try_from(ordinal)
                .map_err(|_| OperationsError::Bounds("Team tenure roster ordinal"))?
            || member.membership_revision != 1
            || member.joined_at != target_event.observed_at
        {
            return Err(OperationsError::Corrupt(
                "Team tenure create roster generation",
            ));
        }
        crate::teams::validate_team_role(&member.role)?;
        let participation_id = WorkspaceParticipationId::from_uuid(decode_tenure_id(
            &member.participation_id,
            "Team tenure roster participation",
        )?)?;
        let activity_id = ActivityId::from_uuid(decode_tenure_id(
            &member.activity_id,
            "Team tenure roster Activity",
        )?);
        let agent_id = AgentInstanceId::from_uuid(decode_tenure_id(
            &member.agent_id,
            "Team tenure roster Agent",
        )?);
        let session_id = AgentSessionId::from_uuid(decode_tenure_id(
            &member.session_id,
            "Team tenure roster Session",
        )?);
        let participant_id =
            decode_tenure_id(&member.participant_id, "Team tenure roster participant")?;
        let profile_id = decode_tenure_id(&member.profile_id, "Team tenure roster Profile")?;
        if !participations.insert(participation_id) || !participants.insert(participant_id) {
            return Err(OperationsError::Corrupt(
                "Team tenure duplicate roster identity",
            ));
        }
        validate_roster_profile_mapping(
            connection,
            identity.workspace_id,
            participation_id,
            profile_id,
            participant_id,
        )?;
        validate_roster_actor_source(
            connection,
            identity.workspace_id,
            participation_id,
            profile_id,
            participant_id,
            activity_id,
            agent_id,
            session_id,
            require_roster_actor_source,
        )?;
        canonical_roster.push(crate::team_expansion::TeamRosterDigestInput::new(
            activity_id,
            session_id,
            &member.role,
        ));

        let member_event = load_membership_event_raw(
            connection,
            identity.workspace_id,
            identity.team_id,
            participation_id,
            identity.request_id,
        )?
        .ok_or(OperationsError::Corrupt(
            "Team tenure create roster event missing",
        ))?;
        let decoded_event = decode_membership_source_event(&member_event)?;
        if decoded_event.state != TeamMembershipState::Joined
            || decoded_event.revision != 1
            || decoded_event.observed_at.get() != member.joined_at
            || member_event.role.as_deref() != Some(member.role.as_str())
        {
            return Err(OperationsError::Corrupt(
                "Team tenure create roster event mismatch",
            ));
        }
        validate_create_source_actor(request, &member_event, capacity_actor)?;
        let member_event_digest = membership_source_event_digest(&member_event)?;
        if participation_id == identity.participation_id {
            if profile_id != identity.profile_id
                || identity.membership_revision != 1
                || member_event_digest != target_event_digest
            {
                return Err(OperationsError::Corrupt("Team tenure target roster source"));
            }
            target_found = true;
        }
        event_digests.push(member_event_digest);
    }
    if !target_found {
        return Err(OperationsError::Corrupt(
            "Team tenure target roster missing",
        ));
    }
    // Reuse the shared Team-create v1 contract (Activity + Session + role).
    // It intentionally omits Agent ID, so the row-level actor-source checks
    // above independently verify Activity/Agent/Session/participant/Profile.
    // This digest is recomputed from the source rows; it is not an independent
    // persisted copy of the original request body.
    let canonical_digest = crate::team_expansion::team_create_roster_digest(&canonical_roster)?;
    roster_source_digest(&roster, canonical_digest, &event_digests)
}

fn decode_tenure_id(value: &[u8], field: &'static str) -> Result<Uuid, OperationsError> {
    let id = Uuid::from_slice(value).map_err(|_| OperationsError::Corrupt(field))?;
    if id.is_nil() {
        return Err(OperationsError::Corrupt(field));
    }
    Ok(id)
}

fn validate_roster_profile_mapping(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participation_id: WorkspaceParticipationId,
    profile_id: Uuid,
    participant_id: Uuid,
) -> Result<(), OperationsError> {
    let matches: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_workspace_participations AS participation JOIN ops_workspace_participation_mappings AS mapping ON mapping.workspace_id=participation.workspace_id AND mapping.participation_id=participation.participation_id AND mapping.agent_profile_id=participation.agent_profile_id WHERE participation.workspace_id=?1 AND participation.participation_id=?2 AND participation.agent_profile_id=?3 AND mapping.participant_id=?4)",
        params![workspace_id.into_uuid().as_bytes(),participation_id.into_uuid().as_bytes(),profile_id.as_bytes(),participant_id.as_bytes()],
        |row| row.get(0),
    )?;
    if !matches {
        return Err(OperationsError::Corrupt(
            "Team tenure roster Profile mapping",
        ));
    }
    Ok(())
}

fn validate_roster_actor_source(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participation_id: WorkspaceParticipationId,
    profile_id: Uuid,
    participant_id: Uuid,
    activity_id: ActivityId,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    require_source: bool,
) -> Result<bool, OperationsError> {
    // Issuance must resolve this exact roster tuple through the current owner
    // binding. A replay/readback may see the roster Session after it has been
    // replaced; then the stored full-roster digest preserves the verified
    // snapshot without pretending that Session is still current.
    let actor = OperationalActor::Participant {
        activity_id,
        agent_id,
        session_id,
    };
    let Some(binding) =
        crate::effect_binding::exact(connection, workspace_id, activity_id, agent_id, session_id)?
    else {
        if require_source {
            return Err(OperationsError::Conflict(
                "Team tenure roster actor source missing",
            ));
        }
        return Ok(false);
    };
    if binding.participant_id() != participant_id {
        return Err(OperationsError::Corrupt(
            "Team tenure roster effect binding participant",
        ));
    }
    match resolve_actor(connection, workspace_id, actor)? {
        ResolvedOperationalActor::ProfileParticipant {
            participation_id: resolved_participation,
            profile_id: resolved_profile,
            participant_id: resolved_participant,
            ..
        } if resolved_participation == participation_id
            && resolved_profile == profile_id
            && resolved_participant == participant_id =>
        {
            Ok(true)
        }
        _ => Err(OperationsError::Corrupt(
            "Team tenure roster effect binding identity",
        )),
    }
}

fn roster_source_digest(
    roster: &[RawTeamCreateRosterSource],
    canonical_digest: crate::team_expansion::TeamCreateRosterDigest,
    event_digests: &[[u8; 32]],
) -> Result<[u8; 32], OperationsError> {
    let mut digest = Sha256::new();
    digest.update(b"heterarchy-orchestration-team-create-roster-source-v2\0");
    digest.update(canonical_digest.as_bytes());
    digest.update(
        u64::try_from(roster.len())
            .map_err(|_| OperationsError::Bounds("Team tenure roster count"))?
            .to_be_bytes(),
    );
    if roster.len() != event_digests.len() {
        return Err(OperationsError::Corrupt(
            "Team tenure roster event digest count",
        ));
    }
    for (row, event_digest) in roster.iter().zip(event_digests) {
        digest.update(row.ordinal.to_be_bytes());
        for field in [
            &row.participation_id,
            &row.activity_id,
            &row.agent_id,
            &row.session_id,
            &row.participant_id,
            &row.profile_id,
        ] {
            update_digest_bytes(&mut digest, field)?;
        }
        update_digest_bytes(&mut digest, row.role.as_bytes())?;
        digest.update(row.membership_revision.to_be_bytes());
        digest.update(row.joined_at.to_be_bytes());
        digest.update(event_digest);
    }
    Ok(digest.finalize().into())
}

fn capacity_source_digest(
    capacity: &crate::team_expansion::TeamCapacitySnapshot,
) -> Result<[u8; 32], OperationsError> {
    let mut digest = Sha256::new();
    digest.update(b"heterarchy-orchestration-team-capacity-create-source-v1\0");
    digest.update(capacity.workspace_id().into_uuid().as_bytes());
    digest.update(capacity.team_id().as_bytes());
    digest.update([capacity.max_members().get()]);
    digest.update(capacity.revision().to_be_bytes());
    digest.update(capacity.leadership_revision().to_be_bytes());
    digest.update(capacity.recorded_at().get().to_be_bytes());
    let actor = capacity.actor();
    digest.update(actor.activity_id().into_uuid().as_bytes());
    digest.update(actor.agent_id().into_uuid().as_bytes());
    digest.update(actor.session_id().into_uuid().as_bytes());
    digest.update(actor.participation_id().into_uuid().as_bytes());
    digest.update(actor.profile_id().as_bytes());
    digest.update(actor.participant_id().as_bytes());
    update_digest_bytes(&mut digest, actor.display_name().as_bytes())?;
    Ok(digest.finalize().into())
}

#[derive(Debug)]
struct RawSlotUseSource {
    decision_id: Vec<u8>,
    ordinal: i64,
    workspace_id: Vec<u8>,
    team_id: Vec<u8>,
    participation_id: Vec<u8>,
    profile_id: Vec<u8>,
    membership_revision: i64,
    recorded_at: i64,
}

fn load_slot_use_source(
    connection: &Connection,
    identity: &MembershipSourceIdentity,
) -> Result<Option<RawSlotUseSource>, OperationsError> {
    connection.query_row(
        "SELECT decision_id,ordinal,workspace_id,team_id,participation_id,profile_id,membership_revision,recorded_at_ms FROM ops_team_expansion_slot_uses WHERE join_request_id=?1",
        params![identity.request_id.as_bytes()],
        |row| Ok(RawSlotUseSource {
            decision_id: row.get(0)?, ordinal: row.get(1)?, workspace_id: row.get(2)?,
            team_id: row.get(3)?, participation_id: row.get(4)?, profile_id: row.get(5)?,
            membership_revision: row.get(6)?, recorded_at: row.get(7)?,
        }),
    ).optional().map_err(Into::into)
}

fn slot_use_source_digest(source: &RawSlotUseSource) -> Result<[u8; 32], OperationsError> {
    let mut digest = Sha256::new();
    digest.update(b"heterarchy-orchestration-team-approved-slot-use-source-v1\0");
    for field in [
        &source.decision_id,
        &source.workspace_id,
        &source.team_id,
        &source.participation_id,
        &source.profile_id,
    ] {
        update_digest_bytes(&mut digest, field)?;
    }
    digest.update(source.ordinal.to_be_bytes());
    digest.update(source.membership_revision.to_be_bytes());
    digest.update(source.recorded_at.to_be_bytes());
    Ok(digest.finalize().into())
}

fn ensure_anchor_source_matches(
    connection: &Connection,
    anchor: &TeamMembershipTenureAnchor,
) -> Result<(), OperationsError> {
    let identity = MembershipSourceIdentity {
        workspace_id: anchor.workspace_id,
        team_id: anchor.team_id,
        participation_id: anchor.participation_id,
        profile_id: anchor.profile_id,
        membership_revision: anchor.membership_revision,
        request_id: anchor.membership_request_id,
    };
    let raw = load_membership_event_raw(
        connection,
        anchor.workspace_id,
        anchor.team_id,
        anchor.participation_id,
        anchor.membership_request_id,
    )?
    .ok_or(OperationsError::Corrupt("Team tenure source event missing"))?;
    let source = decode_membership_source_event(&raw)?;
    if source.state != anchor.membership_state
        || source.revision != anchor.membership_revision
        || source.observed_at != anchor.membership_observed_at
        || source.digest != anchor.membership_event_digest
    {
        return Err(OperationsError::Corrupt("Team tenure source event changed"));
    }
    let (source_kind, source_digest) =
        membership_source_proof(connection, &identity, &raw, source.digest, false)
            .map_err(|_| OperationsError::Corrupt("Team tenure source proof changed"))?;
    if source_kind != anchor.source_kind || source_digest != anchor.source_digest {
        return Err(OperationsError::Corrupt("Team tenure source proof changed"));
    }
    Ok(())
}

fn insert_anchor(
    connection: &Connection,
    anchor: &TeamMembershipTenureAnchor,
) -> Result<(), OperationsError> {
    connection.execute(
        "INSERT INTO ops_team_membership_tenure_anchors(workspace_id,team_id,participation_id,profile_id,membership_revision,membership_request_id,membership_state,membership_observed_at_ms,membership_event_digest,source_kind,source_digest,anchored_at_ms,actor_activity_id,actor_agent_id,actor_session_id,actor_participant_id,actor_display_name) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
        params![anchor.workspace_id.into_uuid().as_bytes(),anchor.team_id.as_bytes(),anchor.participation_id.into_uuid().as_bytes(),anchor.profile_id.as_bytes(),i64::try_from(anchor.membership_revision).map_err(|_|OperationsError::Bounds("Team membership tenure revision"))?,anchor.membership_request_id.as_bytes(),"JOINED",anchor.membership_observed_at.get(),anchor.membership_event_digest.as_slice(),anchor.source_kind.encode(),anchor.source_digest.as_slice(),anchor.anchored_at.get(),anchor.actor_activity_id.into_uuid().as_bytes(),anchor.actor_agent_id.into_uuid().as_bytes(),anchor.actor_session_id.into_uuid().as_bytes(),anchor.actor_participant_id.as_bytes(),anchor.actor_display_name],
    )?;
    Ok(())
}

fn load_anchor_by_membership(
    connection: &Connection,
    command: &TeamMembershipTenureAnchorCommand,
) -> Result<Option<TeamMembershipTenureAnchor>, OperationsError> {
    load_anchor(
        connection,
        "workspace_id=?1 AND team_id=?2 AND participation_id=?3 AND membership_revision=?4",
        params![
            command.workspace_id.into_uuid().as_bytes(),
            command.team_id.as_bytes(),
            command.participation_id.into_uuid().as_bytes(),
            i64::try_from(command.membership_revision)
                .map_err(|_| OperationsError::Bounds("Team membership tenure revision"))?
        ],
    )
}

fn load_anchor_by_request(
    connection: &Connection,
    command: &TeamMembershipTenureAnchorCommand,
) -> Result<Option<TeamMembershipTenureAnchor>, OperationsError> {
    load_anchor(
        connection,
        "workspace_id=?1 AND team_id=?2 AND participation_id=?3 AND membership_request_id=?4",
        params![
            command.workspace_id.into_uuid().as_bytes(),
            command.team_id.as_bytes(),
            command.participation_id.into_uuid().as_bytes(),
            command.membership_request_id.as_bytes()
        ],
    )
}

fn load_anchor_by_identity(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    participation_id: WorkspaceParticipationId,
    revision: u64,
) -> Result<Option<TeamMembershipTenureAnchor>, OperationsError> {
    load_anchor(
        connection,
        "workspace_id=?1 AND team_id=?2 AND participation_id=?3 AND membership_revision=?4",
        params![
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes(),
            participation_id.into_uuid().as_bytes(),
            i64::try_from(revision)
                .map_err(|_| OperationsError::Bounds("Team membership tenure revision"))?
        ],
    )
}

fn load_anchor(
    connection: &Connection,
    selector: &str,
    values: impl rusqlite::Params,
) -> Result<Option<TeamMembershipTenureAnchor>, OperationsError> {
    let sql = format!(
        "SELECT workspace_id,team_id,participation_id,profile_id,membership_revision,membership_request_id,membership_state,membership_observed_at_ms,membership_event_digest,source_kind,source_digest,anchored_at_ms,actor_activity_id,actor_agent_id,actor_session_id,actor_participant_id,actor_display_name FROM ops_team_membership_tenure_anchors WHERE {selector} LIMIT 1"
    );
    connection
        .query_row(&sql, values, decode_anchor_row)
        .optional()
        .map_err(Into::into)
}

fn decode_anchor_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TeamMembershipTenureAnchor> {
    let workspace: Vec<u8> = row.get(0)?;
    let team: Vec<u8> = row.get(1)?;
    let participation: Vec<u8> = row.get(2)?;
    let profile: Vec<u8> = row.get(3)?;
    let membership_revision: i64 = row.get(4)?;
    let request: Vec<u8> = row.get(5)?;
    let state: String = row.get(6)?;
    let observed: i64 = row.get(7)?;
    let event_digest: Vec<u8> = row.get(8)?;
    let source_kind: String = row.get(9)?;
    let source_digest: Vec<u8> = row.get(10)?;
    let anchored: i64 = row.get(11)?;
    let activity: Vec<u8> = row.get(12)?;
    let agent: Vec<u8> = row.get(13)?;
    let session: Vec<u8> = row.get(14)?;
    let participant: Vec<u8> = row.get(15)?;
    let name: String = row.get(16)?;
    let membership_state = match state.as_str() {
        "JOINED" => TeamMembershipState::Joined,
        _ => return Err(rusqlite::Error::InvalidQuery),
    };
    let membership_event_digest: [u8; 32] = event_digest
        .try_into()
        .map_err(|_| rusqlite::Error::InvalidQuery)?;
    let membership_source_kind = TeamMembershipTenureSourceKind::decode(&source_kind)
        .ok_or(rusqlite::Error::InvalidQuery)?;
    let membership_source_digest: [u8; 32] = source_digest
        .try_into()
        .map_err(|_| rusqlite::Error::InvalidQuery)?;
    Ok(TeamMembershipTenureAnchor {
        workspace_id: WorkspaceId::from_uuid(
            Uuid::from_slice(&workspace).map_err(|_| rusqlite::Error::InvalidQuery)?,
        ),
        team_id: Uuid::from_slice(&team).map_err(|_| rusqlite::Error::InvalidQuery)?,
        participation_id: WorkspaceParticipationId::from_uuid(
            Uuid::from_slice(&participation).map_err(|_| rusqlite::Error::InvalidQuery)?,
        )
        .map_err(|_| rusqlite::Error::InvalidQuery)?,
        profile_id: Uuid::from_slice(&profile).map_err(|_| rusqlite::Error::InvalidQuery)?,
        membership_revision: u64::try_from(membership_revision)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        membership_request_id: Uuid::from_slice(&request)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        membership_state,
        membership_observed_at: TimestampMillis::new(observed)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        membership_event_digest,
        source_kind: membership_source_kind,
        source_digest: membership_source_digest,
        anchored_at: TimestampMillis::new(anchored).map_err(|_| rusqlite::Error::InvalidQuery)?,
        actor_activity_id: ActivityId::from_uuid(
            Uuid::from_slice(&activity).map_err(|_| rusqlite::Error::InvalidQuery)?,
        ),
        actor_agent_id: AgentInstanceId::from_uuid(
            Uuid::from_slice(&agent).map_err(|_| rusqlite::Error::InvalidQuery)?,
        ),
        actor_session_id: AgentSessionId::from_uuid(
            Uuid::from_slice(&session).map_err(|_| rusqlite::Error::InvalidQuery)?,
        ),
        actor_participant_id: Uuid::from_slice(&participant)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        actor_display_name: name,
        replayed: false,
    })
}

fn latest_anchor(
    connection: &Connection,
    workspace_id: WorkspaceId,
    team_id: Uuid,
    participation_id: WorkspaceParticipationId,
) -> Result<Option<TeamMembershipTenureAnchor>, OperationsError> {
    load_anchor(
        connection,
        "workspace_id=?1 AND team_id=?2 AND participation_id=?3 ORDER BY anchored_at_ms DESC,membership_revision DESC",
        params![
            workspace_id.into_uuid().as_bytes(),
            team_id.as_bytes(),
            participation_id.into_uuid().as_bytes()
        ],
    )
}

fn anchor_matches_command(
    anchor: &TeamMembershipTenureAnchor,
    command: &TeamMembershipTenureAnchorCommand,
    actor: &ProfileActor,
) -> bool {
    anchor.workspace_id == command.workspace_id
        && anchor.team_id == command.team_id
        && anchor.participation_id == command.participation_id
        && anchor.membership_revision == command.membership_revision
        && anchor.membership_request_id == command.membership_request_id
        && anchor.profile_id == actor.profile_id
}

fn trusted_now() -> Result<TimestampMillis, OperationsError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| OperationsError::Conflict("Team tenure clock before epoch"))?
        .as_millis();
    let millis =
        i64::try_from(millis).map_err(|_| OperationsError::Bounds("Team tenure timestamp"))?;
    TimestampMillis::new(millis).map_err(|_| OperationsError::Bounds("Team tenure timestamp"))
}

fn with_replay(mut anchor: TeamMembershipTenureAnchor) -> TeamMembershipTenureAnchor {
    anchor.replayed = true;
    anchor
}
