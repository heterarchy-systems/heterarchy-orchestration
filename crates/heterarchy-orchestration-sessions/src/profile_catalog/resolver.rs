//! Bounded reusable Profile resolver projection and app-global reservation claims.
//!
//! These rows are scheduling metadata only. They never grant Workspace
//! participation, leadership, vote, `WorkCredit`, Writer Lease, or Forge effects.

use forge_domain::TimestampMillis;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use uuid::Uuid;

use super::{
    AgentProfileCatalog, AgentProfileError, AgentProfileId, AgentProfileRevision,
    AgentProfileRuntimeSessionId, decode_uuid, normalize_sql, validate_schema_sql, validate_text,
};

mod sql;

use sql::{
    ACTIVE_CLAIM_FOR_PROFILE_SQL, ACTIVE_RESOLUTION_CLAIM_INDEX_SQL, CLAIM_BY_ID_SQL,
    CLAIM_BY_REQUEST_SLOT_SQL, RESOLUTION_CLAIMS_SQL, RESOLVER_CANDIDATES_SQL,
    SELECT_INDEX_SCHEMA_SQL,
};

/// Maximum compatible Profiles returned in one resolver read. The caller must
/// fail closed when `truncated` is true instead of assuming this is a complete set.
pub const MAX_AGENT_PROFILE_RESOLVER_CANDIDATES: usize = 128;

/// Reusable state observed from the Profile catalog. Current Workspace scope is
/// composed separately by Operations and is not stored in this global catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProfileResolverAvailability {
    Ready,
    OfflineResumable,
    OfflineNeedsSession,
    Busy,
    Transitioning,
    Error,
}

/// One capability-compatible active Profile. Runtime identity is populated only
/// when the resolver may safely reuse or wake an exact provider Runtime Session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentProfileResolverCandidate {
    profile_id: AgentProfileId,
    profile_revision: AgentProfileRevision,
    availability: AgentProfileResolverAvailability,
    runtime_session_id: Option<AgentProfileRuntimeSessionId>,
    runtime_session_revision: Option<i64>,
    last_used_at: Option<TimestampMillis>,
    updated_at: TimestampMillis,
}

impl AgentProfileResolverCandidate {
    #[must_use]
    pub const fn profile_id(self) -> AgentProfileId {
        self.profile_id
    }

    #[must_use]
    pub const fn profile_revision(self) -> AgentProfileRevision {
        self.profile_revision
    }

    #[must_use]
    pub const fn availability(self) -> AgentProfileResolverAvailability {
        self.availability
    }

    #[must_use]
    pub const fn runtime_session_id(self) -> Option<AgentProfileRuntimeSessionId> {
        self.runtime_session_id
    }

    #[must_use]
    pub const fn runtime_session_revision(self) -> Option<i64> {
        self.runtime_session_revision
    }

    #[must_use]
    pub const fn last_used_at(self) -> Option<TimestampMillis> {
        self.last_used_at
    }

    #[must_use]
    pub const fn updated_at(self) -> TimestampMillis {
        self.updated_at
    }
}

/// Bounded resolver candidate read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileResolverCandidates {
    candidates: Vec<AgentProfileResolverCandidate>,
    truncated: bool,
}

impl AgentProfileResolverCandidates {
    #[must_use]
    pub fn candidates(&self) -> &[AgentProfileResolverCandidate] {
        &self.candidates
    }

    #[must_use]
    pub const fn truncated(&self) -> bool {
        self.truncated
    }
}

/// Reservation kind chosen by the deterministic resolver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProfileResolutionClaimKind {
    ReuseCurrentScope,
    ReuseReadyRuntime,
    WakeExistingRuntime,
    WakeNewRuntime,
}

/// Exact Profile/Runtime target selected by the resolver before a global claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentProfileResolutionClaimTarget {
    kind: AgentProfileResolutionClaimKind,
    profile_id: AgentProfileId,
    profile_revision: AgentProfileRevision,
    runtime_session_id: Option<AgentProfileRuntimeSessionId>,
    runtime_session_revision: Option<i64>,
}

impl AgentProfileResolutionClaimTarget {
    /// Build one resolution target from exact Profile and optional Runtime Session evidence.
    ///
    /// # Errors
    /// Rejects runtime identity shapes that do not match the claim kind or non-positive revisions.
    pub fn new(
        kind: AgentProfileResolutionClaimKind,
        profile_id: AgentProfileId,
        profile_revision: AgentProfileRevision,
        runtime: Option<(AgentProfileRuntimeSessionId, i64)>,
    ) -> Result<Self, AgentProfileError> {
        let runtime_required = matches!(
            kind,
            AgentProfileResolutionClaimKind::ReuseReadyRuntime
                | AgentProfileResolutionClaimKind::WakeExistingRuntime
        );
        if runtime_required != runtime.is_some() {
            return Err(AgentProfileError::Corrupt(
                "Agent Profile resolution claim runtime shape",
            ));
        }
        let (runtime_session_id, runtime_session_revision) = match runtime {
            Some((id, revision)) if revision > 0 => (Some(id), Some(revision)),
            Some((_, revision)) => return Err(AgentProfileError::InvalidRevision(revision)),
            None => (None, None),
        };
        Ok(Self {
            kind,
            profile_id,
            profile_revision,
            runtime_session_id,
            runtime_session_revision,
        })
    }
}

/// Exact app-global Profile reservation input. Workspace facts are deliberately
/// absent; the claim serializes reusable Profile selection only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentProfileResolutionClaimCommand {
    claim_id: Uuid,
    agent_request_id: Uuid,
    slot_ordinal: u8,
    kind: AgentProfileResolutionClaimKind,
    profile_id: AgentProfileId,
    profile_revision: AgentProfileRevision,
    runtime_session_id: Option<AgentProfileRuntimeSessionId>,
    runtime_session_revision: Option<i64>,
    claimed_at: TimestampMillis,
}

impl AgentProfileResolutionClaimCommand {
    /// Build one exact resolver reservation command.
    ///
    /// # Errors
    /// Rejects nil ids or slots outside the `AgentRequest` fan-out bound.
    pub fn new(
        claim_id: Uuid,
        agent_request_id: Uuid,
        slot_ordinal: u8,
        target: AgentProfileResolutionClaimTarget,
        claimed_at: TimestampMillis,
    ) -> Result<Self, AgentProfileError> {
        if claim_id.is_nil() || agent_request_id.is_nil() {
            return Err(AgentProfileError::InvalidIdentity("resolution_claim"));
        }
        if slot_ordinal > 3 {
            return Err(AgentProfileError::InvalidIdentity(
                "resolution_slot_ordinal",
            ));
        }
        Ok(Self {
            claim_id,
            agent_request_id,
            slot_ordinal,
            kind: target.kind,
            profile_id: target.profile_id,
            profile_revision: target.profile_revision,
            runtime_session_id: target.runtime_session_id,
            runtime_session_revision: target.runtime_session_revision,
            claimed_at,
        })
    }
}

/// Durable app-global resolver reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentProfileResolutionClaim {
    claim_id: Uuid,
    agent_request_id: Uuid,
    slot_ordinal: u8,
    kind: AgentProfileResolutionClaimKind,
    profile_id: AgentProfileId,
    profile_revision: AgentProfileRevision,
    runtime_session_id: Option<AgentProfileRuntimeSessionId>,
    runtime_session_revision: Option<i64>,
    claimed_at: TimestampMillis,
    revision: i64,
}

impl AgentProfileResolutionClaim {
    #[must_use]
    pub const fn claim_id(self) -> Uuid {
        self.claim_id
    }
    #[must_use]
    pub const fn agent_request_id(self) -> Uuid {
        self.agent_request_id
    }
    #[must_use]
    pub const fn slot_ordinal(self) -> u8 {
        self.slot_ordinal
    }
    #[must_use]
    pub const fn kind(self) -> AgentProfileResolutionClaimKind {
        self.kind
    }
    #[must_use]
    pub const fn profile_id(self) -> AgentProfileId {
        self.profile_id
    }
    #[must_use]
    pub const fn profile_revision(self) -> AgentProfileRevision {
        self.profile_revision
    }
    #[must_use]
    pub const fn runtime_session_id(self) -> Option<AgentProfileRuntimeSessionId> {
        self.runtime_session_id
    }
    #[must_use]
    pub const fn runtime_session_revision(self) -> Option<i64> {
        self.runtime_session_revision
    }
    #[must_use]
    pub const fn claimed_at(self) -> TimestampMillis {
        self.claimed_at
    }
    #[must_use]
    pub const fn revision(self) -> i64 {
        self.revision
    }
}

pub(super) fn create_schema(transaction: &Transaction<'_>) -> Result<(), AgentProfileError> {
    transaction.execute_batch(RESOLUTION_CLAIMS_SQL)?;
    transaction.execute_batch(ACTIVE_RESOLUTION_CLAIM_INDEX_SQL)?;
    Ok(())
}

pub(super) fn validate_schema(connection: &Connection) -> Result<(), AgentProfileError> {
    validate_schema_sql(
        connection,
        "agent_profile_resolution_claims",
        RESOLUTION_CLAIMS_SQL,
    )?;
    validate_index_schema_sql(
        connection,
        "agent_profile_resolution_claims_active_profile_idx",
        ACTIVE_RESOLUTION_CLAIM_INDEX_SQL,
    )
}

fn validate_index_schema_sql(
    connection: &Connection,
    name: &'static str,
    expected: &str,
) -> Result<(), AgentProfileError> {
    let sql: Option<String> = connection
        .query_row(SELECT_INDEX_SCHEMA_SQL, params![name], |row| row.get(0))
        .optional()?;
    let Some(sql) = sql else {
        return Err(AgentProfileError::Schema(name));
    };
    if normalize_sql(&sql) != normalize_sql(expected) {
        return Err(AgentProfileError::Schema(name));
    }
    Ok(())
}

impl AgentProfileCatalog {
    /// Read all active capability-compatible Profiles needed by the resolver in
    /// one bounded SQL projection. Callers must fail closed on truncation.
    ///
    /// # Errors
    /// Rejects invalid capability text, malformed durable rows, or storage errors.
    pub fn resolver_candidates(
        &self,
        capability: &str,
    ) -> Result<AgentProfileResolverCandidates, AgentProfileError> {
        validate_text(
            "resolver_capability",
            capability,
            super::MAX_CAPABILITY_BYTES,
        )?;
        self.ensure_live_binding()?;
        // Existing ChatGPT RuntimeSessions remain readable history, while no
        // native provider adapter is registered to make a resolver candidate runnable.
        let mut statement = self.connection.prepare(RESOLVER_CANDIDATES_SQL)?;
        let rows = statement.query_map(params![capability, 0_i64], decode_candidate_row)?;
        Ok(AgentProfileResolverCandidates {
            candidates: rows.collect::<Result<Vec<_>, _>>()?,
            truncated: false,
        })
    }

    /// Atomically reserve all Profile claims selected by one bounded resolver pass.
    /// Exact slot replays remain idempotent; any conflict rolls back every new claim
    /// in the batch so a request never persists a half-reserved Profile set.
    ///
    /// # Errors
    /// Rejects batches above four entries, duplicate slots/Profiles, stale candidate
    /// revisions, cross-request active claims, replay conflicts, or storage failures.
    pub fn reserve_resolution_claims(
        &mut self,
        commands: &[AgentProfileResolutionClaimCommand],
    ) -> Result<Vec<AgentProfileResolutionClaim>, AgentProfileError> {
        if commands.len() > 4 {
            return Err(AgentProfileError::ResolutionClaimBatchBound(commands.len()));
        }
        let mut slots = [false; 4];
        let mut profiles = std::collections::BTreeSet::new();
        for command in commands {
            let slot = usize::from(command.slot_ordinal);
            if slots[slot] || !profiles.insert(command.profile_id) {
                return Err(AgentProfileError::ResolutionClaimBatchConflict);
            }
            slots[slot] = true;
        }
        if commands.is_empty() {
            return Ok(Vec::new());
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut claims = Vec::with_capacity(commands.len());
        for command in commands {
            claims.push(reserve_claim_in_transaction(&transaction, command)?.0);
        }
        transaction.commit()?;
        Ok(claims)
    }

    /// Read one reservation by its `AgentRequest` slot identity.
    ///
    /// # Errors
    /// Returns malformed durable rows or storage failures.
    pub fn resolution_claim(
        &self,
        agent_request_id: Uuid,
        slot_ordinal: u8,
    ) -> Result<Option<AgentProfileResolutionClaim>, AgentProfileError> {
        claim_by_request_slot(&self.connection, agent_request_id, slot_ordinal)
    }
}

fn reserve_claim_in_transaction(
    transaction: &Transaction<'_>,
    command: &AgentProfileResolutionClaimCommand,
) -> Result<(AgentProfileResolutionClaim, bool), AgentProfileError> {
    if let Some(existing) =
        claim_by_request_slot(transaction, command.agent_request_id, command.slot_ordinal)?
    {
        if claim_matches(existing, command) {
            return Ok((existing, false));
        }
        return Err(AgentProfileError::ResolutionClaimReplayConflict(
            command.agent_request_id,
        ));
    }
    if claim_by_id(transaction, command.claim_id)?.is_some() {
        return Err(AgentProfileError::ResolutionClaimReplayConflict(
            command.agent_request_id,
        ));
    }
    if active_claim_for_profile(transaction, command.profile_id)?.is_some() {
        return Err(AgentProfileError::ResolutionClaimConflict(
            command.profile_id,
        ));
    }
    Err(AgentProfileError::ProviderRetired)
}

fn decode_candidate_row(
    row: &rusqlite::Row<'_>,
) -> Result<AgentProfileResolverCandidate, rusqlite::Error> {
    let profile_id_raw: Vec<u8> = row.get(0)?;
    let profile_id = decode_profile_id_sql(&profile_id_raw)?;
    let profile_revision =
        AgentProfileRevision::new(row.get(1)?).map_err(|_| rusqlite::Error::InvalidQuery)?;
    let last_used_raw: Option<i64> = row.get(2)?;
    let updated_raw: i64 = row.get(3)?;
    let ready_id_raw: Option<Vec<u8>> = row.get(4)?;
    let ready_revision: Option<i64> = row.get(5)?;
    let offline_id_raw: Option<Vec<u8>> = row.get(6)?;
    let offline_revision: Option<i64> = row.get(7)?;
    let has_provisioning: i64 = row.get(8)?;
    let has_active: i64 = row.get(9)?;
    let has_suspending: i64 = row.get(10)?;
    let has_waking: i64 = row.get(11)?;
    let has_failed: i64 = row.get(12)?;
    let ready_id = ready_id_raw
        .as_deref()
        .map(decode_runtime_id_sql)
        .transpose()?;
    let offline_id = offline_id_raw
        .as_deref()
        .map(decode_runtime_id_sql)
        .transpose()?;
    let availability = if has_active != 0 {
        AgentProfileResolverAvailability::Busy
    } else if ready_id.is_some() {
        AgentProfileResolverAvailability::Ready
    } else if has_provisioning != 0 || has_suspending != 0 || has_waking != 0 {
        AgentProfileResolverAvailability::Transitioning
    } else if has_failed != 0 {
        AgentProfileResolverAvailability::Error
    } else if offline_id.is_some() {
        AgentProfileResolverAvailability::OfflineResumable
    } else {
        AgentProfileResolverAvailability::OfflineNeedsSession
    };
    let (runtime_session_id, runtime_session_revision) = match availability {
        AgentProfileResolverAvailability::Ready => (ready_id, ready_revision),
        AgentProfileResolverAvailability::OfflineResumable => (offline_id, offline_revision),
        AgentProfileResolverAvailability::OfflineNeedsSession
        | AgentProfileResolverAvailability::Busy
        | AgentProfileResolverAvailability::Transitioning
        | AgentProfileResolverAvailability::Error => (None, None),
    };
    Ok(AgentProfileResolverCandidate {
        profile_id,
        profile_revision,
        availability,
        runtime_session_id,
        runtime_session_revision,
        last_used_at: last_used_raw.map(timestamp_sql).transpose()?,
        updated_at: timestamp_sql(updated_raw)?,
    })
}

fn timestamp_sql(value: i64) -> Result<TimestampMillis, rusqlite::Error> {
    TimestampMillis::new(value).map_err(|_| rusqlite::Error::InvalidQuery)
}

fn decode_profile_id_sql(value: &[u8]) -> Result<AgentProfileId, rusqlite::Error> {
    let uuid =
        decode_uuid("resolver_profile_id", value).map_err(|_| rusqlite::Error::InvalidQuery)?;
    AgentProfileId::from_uuid(uuid).map_err(|_| rusqlite::Error::InvalidQuery)
}

fn decode_runtime_id_sql(value: &[u8]) -> Result<AgentProfileRuntimeSessionId, rusqlite::Error> {
    let uuid = decode_uuid("resolver_runtime_session_id", value)
        .map_err(|_| rusqlite::Error::InvalidQuery)?;
    AgentProfileRuntimeSessionId::from_uuid(uuid).map_err(|_| rusqlite::Error::InvalidQuery)
}

fn claim_by_request_slot(
    connection: &Connection,
    agent_request_id: Uuid,
    slot_ordinal: u8,
) -> Result<Option<AgentProfileResolutionClaim>, AgentProfileError> {
    let raw = connection
        .query_row(
            CLAIM_BY_REQUEST_SLOT_SQL,
            params![agent_request_id.as_bytes(), i64::from(slot_ordinal)],
            decode_claim_row,
        )
        .optional()?;
    raw.transpose()
}

fn claim_by_id(
    connection: &Connection,
    claim_id: Uuid,
) -> Result<Option<AgentProfileResolutionClaim>, AgentProfileError> {
    let raw = connection
        .query_row(
            CLAIM_BY_ID_SQL,
            params![claim_id.as_bytes()],
            decode_claim_row,
        )
        .optional()?;
    raw.transpose()
}

fn active_claim_for_profile(
    connection: &Connection,
    profile_id: AgentProfileId,
) -> Result<Option<AgentProfileResolutionClaim>, AgentProfileError> {
    let raw = connection
        .query_row(
            ACTIVE_CLAIM_FOR_PROFILE_SQL,
            params![profile_id.into_uuid().as_bytes()],
            decode_claim_row,
        )
        .optional()?;
    raw.transpose()
}

fn decode_claim_row(
    row: &rusqlite::Row<'_>,
) -> Result<Result<AgentProfileResolutionClaim, AgentProfileError>, rusqlite::Error> {
    Ok(decode_claim(&RawResolutionClaim {
        claim_id: row.get(0)?,
        request_id: row.get(1)?,
        slot: row.get(2)?,
        kind: row.get(3)?,
        profile_id: row.get(4)?,
        profile_revision: row.get(5)?,
        runtime_id: row.get(6)?,
        runtime_revision: row.get(7)?,
        claimed_at: row.get(8)?,
        revision: row.get(9)?,
    }))
}

struct RawResolutionClaim {
    claim_id: Vec<u8>,
    request_id: Vec<u8>,
    slot: i64,
    kind: i64,
    profile_id: Vec<u8>,
    profile_revision: i64,
    runtime_id: Option<Vec<u8>>,
    runtime_revision: Option<i64>,
    claimed_at: i64,
    revision: i64,
}

fn decode_claim(
    raw: &RawResolutionClaim,
) -> Result<AgentProfileResolutionClaim, AgentProfileError> {
    let slot_ordinal =
        u8::try_from(raw.slot).map_err(|_| AgentProfileError::Corrupt("resolution claim slot"))?;
    if slot_ordinal > 3 || raw.revision <= 0 {
        return Err(AgentProfileError::Corrupt("resolution claim bounds"));
    }
    let runtime_session_id = raw
        .runtime_id
        .as_ref()
        .map(|value| {
            decode_uuid("resolution_runtime_session_id", value)
                .and_then(AgentProfileRuntimeSessionId::from_uuid)
        })
        .transpose()?;
    if runtime_session_id.is_some() != raw.runtime_revision.is_some() {
        return Err(AgentProfileError::Corrupt(
            "resolution claim runtime revision",
        ));
    }
    Ok(AgentProfileResolutionClaim {
        claim_id: decode_uuid("resolution_claim_id", &raw.claim_id)?,
        agent_request_id: decode_uuid("resolution_agent_request_id", &raw.request_id)?,
        slot_ordinal,
        kind: decode_claim_kind(raw.kind)?,
        profile_id: AgentProfileId::from_uuid(decode_uuid(
            "resolution_profile_id",
            &raw.profile_id,
        )?)?,
        profile_revision: AgentProfileRevision::new(raw.profile_revision)?,
        runtime_session_id,
        runtime_session_revision: raw.runtime_revision,
        claimed_at: TimestampMillis::new(raw.claimed_at)?,
        revision: raw.revision,
    })
}

fn claim_matches(
    claim: AgentProfileResolutionClaim,
    command: &AgentProfileResolutionClaimCommand,
) -> bool {
    claim.claim_id == command.claim_id
        && claim.agent_request_id == command.agent_request_id
        && claim.slot_ordinal == command.slot_ordinal
        && claim.kind == command.kind
        && claim.profile_id == command.profile_id
        && claim.profile_revision == command.profile_revision
        && claim.runtime_session_id == command.runtime_session_id
        && claim.runtime_session_revision == command.runtime_session_revision
}

fn decode_claim_kind(value: i64) -> Result<AgentProfileResolutionClaimKind, AgentProfileError> {
    match value {
        1 => Ok(AgentProfileResolutionClaimKind::ReuseCurrentScope),
        2 => Ok(AgentProfileResolutionClaimKind::ReuseReadyRuntime),
        3 => Ok(AgentProfileResolutionClaimKind::WakeExistingRuntime),
        4 => Ok(AgentProfileResolutionClaimKind::WakeNewRuntime),
        _ => Err(AgentProfileError::Corrupt("resolution claim kind")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile_catalog::{
        AgentProfileCreationOrigin, AgentProfileRuntimeSession, AgentProfileRuntimeSessionState,
        CreateAgentProfile,
    };
    use std::{error::Error, fs};

    fn profile_command(
        proposed: u128,
        request: u128,
        stable_name: &str,
    ) -> Result<CreateAgentProfile, AgentProfileError> {
        CreateAgentProfile::new(
            AgentProfileId::from_uuid(Uuid::from_u128(proposed))?,
            Uuid::from_u128(request),
            stable_name.to_owned(),
            format!("{stable_name} display"),
            None,
            None,
            vec!["rust".to_owned()],
            AgentProfileCreationOrigin::AgentRequest,
            TimestampMillis::new(10)?,
        )
    }

    fn ready_runtime(
        catalog: &mut AgentProfileCatalog,
        profile_id: AgentProfileId,
        runtime_seed: u128,
        conversation_id: &str,
    ) -> Result<(AgentProfileRuntimeSessionId, AgentProfileRuntimeSession), AgentProfileError> {
        let runtime_id = AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(runtime_seed))?;
        let session = super::super::seed_historical_runtime_session(
            catalog,
            super::super::HistoricalRuntimeFixture {
                profile_id,
                runtime_id,
                request_id: Uuid::from_u128(runtime_seed + 1),
                conversation_id: Some(conversation_id.to_owned()),
                state: AgentProfileRuntimeSessionState::Ready,
                opened_at: TimestampMillis::new(20)?,
                last_seen_at: TimestampMillis::new(21)?,
                closed_at: None,
                revision: 2,
            },
        )?;
        Ok((runtime_id, session))
    }

    fn assert_claim_replay_and_race(
        catalog: &mut AgentProfileCatalog,
        profile: &super::super::AgentProfile,
        runtime_id: AgentProfileRuntimeSessionId,
        runtime: &AgentProfileRuntimeSession,
    ) -> Result<(), Box<dyn Error>> {
        let target = AgentProfileResolutionClaimTarget::new(
            AgentProfileResolutionClaimKind::ReuseReadyRuntime,
            profile.id(),
            profile.revision(),
            Some((runtime_id, runtime.revision().get())),
        )?;
        let command = AgentProfileResolutionClaimCommand::new(
            Uuid::from_u128(0xB301),
            Uuid::from_u128(0xB302),
            0,
            target,
            TimestampMillis::new(30)?,
        )?;
        catalog.connection.execute(
            "INSERT INTO agent_profile_resolution_claims(
                claim_id,agent_request_id,slot_ordinal,claim_kind,profile_id,profile_revision,
                runtime_session_id,runtime_session_revision,claimed_at_ms,released_at_ms,state,revision
             ) VALUES(?1,?2,0,2,?3,?4,?5,?6,30,NULL,1,1)",
            params![
                Uuid::from_u128(0xB301).as_bytes(),
                Uuid::from_u128(0xB302).as_bytes(),
                profile.id().into_uuid().as_bytes(),
                profile.revision().get(),
                runtime_id.into_uuid().as_bytes(),
                runtime.revision().get(),
            ],
        )?;
        let claims = catalog.reserve_resolution_claims(&[command])?;
        assert_eq!(claims.len(), 1);
        let claim = claims[0];
        let replay = catalog.reserve_resolution_claims(&[command])?;
        assert_eq!(replay, [claim]);

        let competing = AgentProfileResolutionClaimCommand::new(
            Uuid::from_u128(0xB303),
            Uuid::from_u128(0xB304),
            0,
            target,
            TimestampMillis::new(31)?,
        )?;
        assert!(matches!(
            catalog.reserve_resolution_claims(&[competing]),
            Err(AgentProfileError::ResolutionClaimConflict(profile_id))
                if profile_id == profile.id()
        ));
        let after_claim = catalog.resolver_candidates("rust")?;
        assert!(after_claim.candidates().is_empty());
        assert_eq!(
            catalog.resolution_claim(Uuid::from_u128(0xB302), 0)?,
            Some(claim)
        );
        Ok(())
    }

    #[test]
    fn retired_provider_candidates_are_empty_and_only_historical_claims_replay()
    -> Result<(), Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!("profile-resolver-{}", Uuid::now_v7()));
        fs::create_dir(&root)?;
        let path = root.join("agent-profiles.sqlite3");
        let mut catalog = AgentProfileCatalog::open(&path)?;

        let ready_profile = catalog
            .create_profile(&profile_command(0xB001, 0xB101, "resolver-ready")?)?
            .profile()
            .clone();
        let new_session_profile = catalog
            .create_profile(&profile_command(0xB002, 0xB102, "resolver-new-unbound")?)?
            .profile()
            .clone();

        let (ready_runtime_id, ready_session) = ready_runtime(
            &mut catalog,
            ready_profile.id(),
            0xB201,
            "resolver-ready-conversation",
        )?;
        let candidates = catalog.resolver_candidates("rust")?;
        assert!(candidates.candidates().is_empty());

        assert_claim_replay_and_race(
            &mut catalog,
            &ready_profile,
            ready_runtime_id,
            &ready_session,
        )?;

        let target = AgentProfileResolutionClaimTarget::new(
            AgentProfileResolutionClaimKind::WakeNewRuntime,
            new_session_profile.id(),
            new_session_profile.revision(),
            None,
        )?;
        let fresh = AgentProfileResolutionClaimCommand::new(
            Uuid::from_u128(0xB305),
            Uuid::from_u128(0xB306),
            0,
            target,
            TimestampMillis::new(32)?,
        )?;
        assert!(matches!(
            catalog.reserve_resolution_claims(&[fresh]),
            Err(AgentProfileError::ProviderRetired)
        ));
        assert_eq!(catalog.resolution_claim(Uuid::from_u128(0xB306), 0)?, None);

        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
