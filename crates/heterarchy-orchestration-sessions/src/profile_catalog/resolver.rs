//! Bounded reusable Profile resolver projection and app-global reservation claims.
//!
//! These rows are scheduling metadata only. They never grant Workspace
//! participation, leadership, vote, `WorkCredit`, Writer Lease, or Forge effects.

use forge_domain::TimestampMillis;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use uuid::Uuid;

use super::{
    AgentProfileCatalog, AgentProfileError, AgentProfileId, AgentProfileLifecycleState,
    AgentProfileRevision, AgentProfileRuntimeSessionId, AgentProfileRuntimeSessionState,
    decode_uuid, load_profile, load_runtime_session, normalize_sql, validate_schema_sql,
    validate_text,
};

/// Maximum compatible Profiles returned in one resolver read. The caller must
/// fail closed when `truncated` is true instead of assuming this is a complete set.
pub const MAX_AGENT_PROFILE_RESOLVER_CANDIDATES: usize = 128;

const RESOLUTION_CLAIMS_SQL: &str = r"
CREATE TABLE agent_profile_resolution_claims (
    claim_id BLOB PRIMARY KEY NOT NULL CHECK(length(claim_id) = 16 AND claim_id != zeroblob(16)),
    agent_request_id BLOB NOT NULL CHECK(length(agent_request_id) = 16 AND agent_request_id != zeroblob(16)),
    slot_ordinal INTEGER NOT NULL CHECK(slot_ordinal BETWEEN 0 AND 3),
    claim_kind INTEGER NOT NULL CHECK(claim_kind BETWEEN 1 AND 4),
    profile_id BLOB NOT NULL CHECK(length(profile_id) = 16 AND profile_id != zeroblob(16)),
    profile_revision INTEGER NOT NULL CHECK(profile_revision > 0),
    runtime_session_id BLOB CHECK(runtime_session_id IS NULL OR (length(runtime_session_id) = 16 AND runtime_session_id != zeroblob(16))),
    runtime_session_revision INTEGER CHECK(runtime_session_revision IS NULL OR runtime_session_revision > 0),
    claimed_at_ms INTEGER NOT NULL CHECK(claimed_at_ms >= 0),
    released_at_ms INTEGER CHECK(released_at_ms IS NULL OR released_at_ms >= claimed_at_ms),
    state INTEGER NOT NULL CHECK(state IN (1,2)),
    revision INTEGER NOT NULL CHECK(revision > 0),
    UNIQUE(agent_request_id, slot_ordinal),
    FOREIGN KEY(profile_id) REFERENCES agent_profiles(profile_id),
    FOREIGN KEY(runtime_session_id) REFERENCES agent_profile_runtime_sessions(runtime_session_id),
    CHECK((state = 1 AND released_at_ms IS NULL) OR (state = 2 AND released_at_ms IS NOT NULL)),
    CHECK((claim_kind IN (2,3) AND runtime_session_id IS NOT NULL AND runtime_session_revision IS NOT NULL) OR (claim_kind IN (1,4) AND runtime_session_id IS NULL AND runtime_session_revision IS NULL))
);
";

const ACTIVE_RESOLUTION_CLAIM_INDEX_SQL: &str = r"
CREATE UNIQUE INDEX agent_profile_resolution_claims_active_profile_idx
ON agent_profile_resolution_claims(profile_id)
WHERE state = 1;
";

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
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE type='index' AND name=?1",
            params![name],
            |row| row.get(0),
        )
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
        let limit = i64::try_from(MAX_AGENT_PROFILE_RESOLVER_CANDIDATES + 1)
            .map_err(|_| AgentProfileError::Corrupt("resolver candidate bound"))?;
        let mut statement = self.connection.prepare(RESOLVER_CANDIDATES_SQL)?;
        let rows = statement.query_map(params![capability, limit], decode_candidate_row)?;
        let mut candidates = rows.collect::<Result<Vec<_>, _>>()?;
        let truncated = candidates.len() > MAX_AGENT_PROFILE_RESOLVER_CANDIDATES;
        candidates.truncate(MAX_AGENT_PROFILE_RESOLVER_CANDIDATES);
        Ok(AgentProfileResolverCandidates {
            candidates,
            truncated,
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
    validate_claim_candidate(transaction, command)?;
    transaction.execute(
        "INSERT INTO agent_profile_resolution_claims(
            claim_id,agent_request_id,slot_ordinal,claim_kind,profile_id,profile_revision,
            runtime_session_id,runtime_session_revision,claimed_at_ms,released_at_ms,state,revision
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,NULL,1,1)",
        params![
            command.claim_id.as_bytes(),
            command.agent_request_id.as_bytes(),
            i64::from(command.slot_ordinal),
            encode_claim_kind(command.kind),
            command.profile_id.into_uuid().as_bytes(),
            command.profile_revision.get(),
            command
                .runtime_session_id
                .map(AgentProfileRuntimeSessionId::into_uuid)
                .map(Uuid::into_bytes)
                .map(Vec::from),
            command.runtime_session_revision,
            command.claimed_at.get(),
        ],
    )?;
    let claim = claim_by_id(transaction, command.claim_id)?.ok_or(AgentProfileError::Corrupt(
        "resolution claim insert readback",
    ))?;
    Ok((claim, true))
}

const RESOLVER_CANDIDATES_SQL: &str = r"
WITH eligible AS (
    SELECT p.profile_id,p.revision,p.last_used_at_ms,p.updated_at_ms
    FROM agent_profiles p
    JOIN agent_profile_capabilities c ON c.profile_id=p.profile_id
    WHERE p.lifecycle_state=1
      AND p.provider_family=1
      AND c.capability=?1
      AND NOT EXISTS (
          SELECT 1 FROM agent_profile_resolution_claims rc
          WHERE rc.profile_id=p.profile_id AND rc.state=1
      )
), ranked_runtime AS (
    SELECT r.runtime_session_id,r.profile_id,r.state,r.revision,r.last_seen_at_ms,
           ROW_NUMBER() OVER (
               PARTITION BY r.profile_id,r.state
               ORDER BY r.last_seen_at_ms DESC,r.runtime_session_id DESC
           ) AS state_rank
    FROM agent_profile_runtime_sessions r
    JOIN eligible e ON e.profile_id=r.profile_id
), status AS (
    SELECT profile_id,
           MAX(CASE WHEN state=1 THEN 1 ELSE 0 END) AS has_provisioning,
           MAX(CASE WHEN state=3 THEN 1 ELSE 0 END) AS has_active,
           MAX(CASE WHEN state=4 THEN 1 ELSE 0 END) AS has_suspending,
           MAX(CASE WHEN state=6 THEN 1 ELSE 0 END) AS has_waking,
           MAX(CASE WHEN state=7 THEN 1 ELSE 0 END) AS has_failed
    FROM ranked_runtime
    GROUP BY profile_id
)
SELECT e.profile_id,e.revision,e.last_used_at_ms,e.updated_at_ms,
       ready.runtime_session_id,ready.revision,
       offline.runtime_session_id,offline.revision,
       COALESCE(s.has_provisioning,0),COALESCE(s.has_active,0),
       COALESCE(s.has_suspending,0),COALESCE(s.has_waking,0),COALESCE(s.has_failed,0)
FROM eligible e
LEFT JOIN ranked_runtime ready
       ON ready.profile_id=e.profile_id AND ready.state=2 AND ready.state_rank=1
LEFT JOIN ranked_runtime offline
       ON offline.profile_id=e.profile_id AND offline.state=5 AND offline.state_rank=1
LEFT JOIN status s ON s.profile_id=e.profile_id
ORDER BY COALESCE(e.last_used_at_ms,-1) DESC,e.updated_at_ms DESC,e.profile_id ASC
LIMIT ?2
";

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

fn validate_claim_candidate(
    connection: &Connection,
    command: &AgentProfileResolutionClaimCommand,
) -> Result<(), AgentProfileError> {
    let profile = load_profile(connection, command.profile_id)?
        .ok_or(AgentProfileError::InvalidIdentity("resolution_profile_id"))?;
    if profile.revision() != command.profile_revision
        || profile.lifecycle_state() != AgentProfileLifecycleState::Active
    {
        return Err(AgentProfileError::ResolutionClaimConflict(
            command.profile_id,
        ));
    }
    match command.kind {
        AgentProfileResolutionClaimKind::ReuseCurrentScope => {
            let ready: bool = connection.query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM agent_profile_runtime_sessions
                    WHERE profile_id=?1 AND state=2
                 )",
                params![command.profile_id.into_uuid().as_bytes()],
                |row| row.get(0),
            )?;
            if ready {
                Ok(())
            } else {
                Err(AgentProfileError::ResolutionClaimConflict(
                    command.profile_id,
                ))
            }
        }
        AgentProfileResolutionClaimKind::ReuseReadyRuntime => {
            validate_runtime_claim(connection, command, AgentProfileRuntimeSessionState::Ready)
        }
        AgentProfileResolutionClaimKind::WakeExistingRuntime => validate_runtime_claim(
            connection,
            command,
            AgentProfileRuntimeSessionState::Offline,
        ),
        AgentProfileResolutionClaimKind::WakeNewRuntime => {
            let conflicting: bool = connection.query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM agent_profile_runtime_sessions
                    WHERE profile_id=?1 AND state IN (1,2,3,4,5,6,7)
                 )",
                params![command.profile_id.into_uuid().as_bytes()],
                |row| row.get(0),
            )?;
            if conflicting {
                Err(AgentProfileError::ResolutionClaimConflict(
                    command.profile_id,
                ))
            } else {
                Ok(())
            }
        }
    }
}

fn validate_runtime_claim(
    connection: &Connection,
    command: &AgentProfileResolutionClaimCommand,
    expected_state: AgentProfileRuntimeSessionState,
) -> Result<(), AgentProfileError> {
    let runtime_id = command
        .runtime_session_id
        .ok_or(AgentProfileError::Corrupt(
            "resolution claim runtime missing",
        ))?;
    let runtime = load_runtime_session(connection, runtime_id)?
        .ok_or(AgentProfileError::RuntimeSessionMissing(runtime_id))?;
    if runtime.profile_id() != command.profile_id
        || runtime.state() != expected_state
        || Some(runtime.revision().get()) != command.runtime_session_revision
    {
        return Err(AgentProfileError::ResolutionClaimConflict(
            command.profile_id,
        ));
    }
    Ok(())
}

fn claim_by_request_slot(
    connection: &Connection,
    agent_request_id: Uuid,
    slot_ordinal: u8,
) -> Result<Option<AgentProfileResolutionClaim>, AgentProfileError> {
    let raw = connection
        .query_row(
            "SELECT claim_id,agent_request_id,slot_ordinal,claim_kind,profile_id,profile_revision,
                    runtime_session_id,runtime_session_revision,claimed_at_ms,revision
             FROM agent_profile_resolution_claims
             WHERE agent_request_id=?1 AND slot_ordinal=?2 AND state=1",
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
            "SELECT claim_id,agent_request_id,slot_ordinal,claim_kind,profile_id,profile_revision,
                    runtime_session_id,runtime_session_revision,claimed_at_ms,revision
             FROM agent_profile_resolution_claims WHERE claim_id=?1 AND state=1",
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
            "SELECT claim_id,agent_request_id,slot_ordinal,claim_kind,profile_id,profile_revision,
                    runtime_session_id,runtime_session_revision,claimed_at_ms,revision
             FROM agent_profile_resolution_claims WHERE profile_id=?1 AND state=1",
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

const fn encode_claim_kind(kind: AgentProfileResolutionClaimKind) -> i64 {
    match kind {
        AgentProfileResolutionClaimKind::ReuseCurrentScope => 1,
        AgentProfileResolutionClaimKind::ReuseReadyRuntime => 2,
        AgentProfileResolutionClaimKind::WakeExistingRuntime => 3,
        AgentProfileResolutionClaimKind::WakeNewRuntime => 4,
    }
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
        AgentProfileCreationOrigin, AgentProfileProviderFamily, AgentProfileRuntimeSession,
        CreateAgentProfile, CreateAgentProfileRuntimeSession,
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
            AgentProfileProviderFamily::ChatGpt,
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
        let created = catalog.create_runtime_session(&CreateAgentProfileRuntimeSession::new(
            runtime_id,
            Uuid::from_u128(runtime_seed + 1),
            profile_id,
            AgentProfileProviderFamily::ChatGpt,
            TimestampMillis::new(20)?,
        )?)?;
        if !created.created() {
            return Err(AgentProfileError::Corrupt(
                "resolver test runtime create replay",
            ));
        }
        let ready = catalog.confirm_runtime_session_ready(
            runtime_id,
            created.session().revision(),
            conversation_id,
            TimestampMillis::new(21)?,
        )?;
        Ok((runtime_id, ready))
    }

    fn assert_candidate_matrix(
        candidates: &AgentProfileResolverCandidates,
        ready_profile: AgentProfileId,
        ready_runtime: AgentProfileRuntimeSessionId,
        offline_profile: AgentProfileId,
        offline_runtime: AgentProfileRuntimeSessionId,
        busy_profile: AgentProfileId,
        new_session_profile: AgentProfileId,
    ) -> Result<(), Box<dyn Error>> {
        assert!(!candidates.truncated());
        assert_eq!(candidates.candidates().len(), 4);
        let candidate = |profile_id| {
            candidates
                .candidates()
                .iter()
                .copied()
                .find(|item| item.profile_id() == profile_id)
                .ok_or("resolver candidate missing")
        };
        let ready = candidate(ready_profile)?;
        assert_eq!(
            ready.availability(),
            AgentProfileResolverAvailability::Ready
        );
        assert_eq!(ready.runtime_session_id(), Some(ready_runtime));
        let offline = candidate(offline_profile)?;
        assert_eq!(
            offline.availability(),
            AgentProfileResolverAvailability::OfflineResumable
        );
        assert_eq!(offline.runtime_session_id(), Some(offline_runtime));
        assert_eq!(
            candidate(busy_profile)?.availability(),
            AgentProfileResolverAvailability::Busy
        );
        assert_eq!(
            candidate(new_session_profile)?.availability(),
            AgentProfileResolverAvailability::OfflineNeedsSession
        );
        Ok(())
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
        assert!(
            after_claim
                .candidates()
                .iter()
                .all(|item| item.profile_id() != profile.id())
        );
        assert_eq!(
            catalog.resolution_claim(Uuid::from_u128(0xB302), 0)?,
            Some(claim)
        );
        Ok(())
    }

    #[test]
    fn candidates_and_claims_share_one_bounded_replay_safe_contract() -> Result<(), Box<dyn Error>>
    {
        let root = std::env::temp_dir().join(format!("profile-resolver-{}", Uuid::now_v7()));
        fs::create_dir(&root)?;
        let path = root.join("agent-profiles.sqlite3");
        let mut catalog = AgentProfileCatalog::open(&path)?;

        let ready_profile = catalog
            .create_profile(&profile_command(0xB001, 0xB101, "resolver-ready")?)?
            .profile()
            .clone();
        let offline_profile = catalog
            .create_profile(&profile_command(0xB002, 0xB102, "resolver-offline")?)?
            .profile()
            .clone();
        let busy_profile = catalog
            .create_profile(&profile_command(0xB003, 0xB103, "resolver-busy")?)?
            .profile()
            .clone();
        let new_session_profile = catalog
            .create_profile(&profile_command(0xB004, 0xB104, "resolver-new-session")?)?
            .profile()
            .clone();

        let (ready_runtime_id, ready_session) = ready_runtime(
            &mut catalog,
            ready_profile.id(),
            0xB201,
            "resolver-ready-conversation",
        )?;
        let (offline_runtime_id, offline_ready) = ready_runtime(
            &mut catalog,
            offline_profile.id(),
            0xB211,
            "resolver-offline-conversation",
        )?;
        let suspending = catalog.transition_runtime_session(
            offline_runtime_id,
            offline_ready.revision(),
            AgentProfileRuntimeSessionState::Suspending,
            TimestampMillis::new(22)?,
        )?;
        let _offline = catalog.transition_runtime_session(
            offline_runtime_id,
            suspending.revision(),
            AgentProfileRuntimeSessionState::Offline,
            TimestampMillis::new(23)?,
        )?;
        let (busy_runtime_id, busy_ready) = ready_runtime(
            &mut catalog,
            busy_profile.id(),
            0xB221,
            "resolver-busy-conversation",
        )?;
        let _busy = catalog.transition_runtime_session(
            busy_runtime_id,
            busy_ready.revision(),
            AgentProfileRuntimeSessionState::Active,
            TimestampMillis::new(22)?,
        )?;

        let candidates = catalog.resolver_candidates("rust")?;
        assert_candidate_matrix(
            &candidates,
            ready_profile.id(),
            ready_runtime_id,
            offline_profile.id(),
            offline_runtime_id,
            busy_profile.id(),
            new_session_profile.id(),
        )?;

        assert_claim_replay_and_race(
            &mut catalog,
            &ready_profile,
            ready_runtime_id,
            &ready_session,
        )?;

        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
