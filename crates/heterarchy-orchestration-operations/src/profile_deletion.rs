//! Durable per-Profile admission barriers for the provider-effect journal.
//!
//! Sessions owns the deletion intent and Profile lifecycle. Operations records
//! that exact intent before blocking new provider work. The in-flight counts
//! below are observational only: they do not prove provider idleness or
//! authorize final deletion.

use forge_domain::TimestampMillis;
use heterarchy_orchestration_sessions::{
    AgentProfileDeletionIntent, AgentProfileId, AgentProfileRevision,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use uuid::Uuid;

use crate::{OperationsCatalog, OperationsError, wire};

const PROFILE_DELETION_BARRIERS_SQL: &str = r"
CREATE TABLE ops_agent_profile_deletion_barriers (
    request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)),
    profile_id BLOB NOT NULL UNIQUE CHECK(length(profile_id)=16 AND profile_id != zeroblob(16)),
    expected_profile_revision INTEGER NOT NULL CHECK(expected_profile_revision>0),
    barrier_profile_revision INTEGER NOT NULL CHECK(barrier_profile_revision>0),
    requested_at_ms INTEGER NOT NULL CHECK(requested_at_ms>=0)
);
";

/// Immutable Operations readback of the Sessions-owned deletion intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentProfileDeletionBarrier {
    request_id: Uuid,
    profile_id: AgentProfileId,
    expected_profile_revision: AgentProfileRevision,
    barrier_profile_revision: AgentProfileRevision,
    requested_at: TimestampMillis,
}

impl AgentProfileDeletionBarrier {
    #[must_use]
    pub const fn request_id(self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn profile_id(self) -> AgentProfileId {
        self.profile_id
    }

    #[must_use]
    pub const fn expected_profile_revision(self) -> AgentProfileRevision {
        self.expected_profile_revision
    }

    #[must_use]
    pub const fn barrier_profile_revision(self) -> AgentProfileRevision {
        self.barrier_profile_revision
    }

    #[must_use]
    pub const fn requested_at(self) -> TimestampMillis {
        self.requested_at
    }
}

/// Counts of durable provider effects currently awaiting confirmation or inspection.
///
/// These counts are not a provider-idle proof and must not be used by themselves
/// to freeze deletion targets or finalize provider deletion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentProfileDeletionInFlightCounts {
    profile_id: AgentProfileId,
    pending_provision_operations: u64,
    unknown_provision_operations: u64,
    observed_provision_operations: u64,
    pending_provider_messages: u64,
    unknown_provider_messages: u64,
}

impl AgentProfileDeletionInFlightCounts {
    #[must_use]
    pub const fn profile_id(self) -> AgentProfileId {
        self.profile_id
    }

    #[must_use]
    pub const fn pending_provision_operations(self) -> u64 {
        self.pending_provision_operations
    }

    #[must_use]
    pub const fn unknown_provision_operations(self) -> u64 {
        self.unknown_provision_operations
    }

    #[must_use]
    pub const fn observed_provision_operations(self) -> u64 {
        self.observed_provision_operations
    }

    #[must_use]
    pub const fn pending_provider_messages(self) -> u64 {
        self.pending_provider_messages
    }

    #[must_use]
    pub const fn unknown_provider_messages(self) -> u64 {
        self.unknown_provider_messages
    }
}

impl OperationsCatalog {
    /// Persist the exact Sessions deletion intent as an immutable Profile barrier.
    ///
    /// Replaying the same request and all of its original fields returns the
    /// stored barrier. A different request cannot replace a Profile's barrier.
    ///
    /// # Errors
    /// Rejects changed request replays, attempts to reset a Profile barrier, or
    /// SQLite failures. An uncertain commit must be resolved by reading the
    /// same request identity.
    pub fn begin_profile_deletion_barrier(
        &mut self,
        intent: &AgentProfileDeletionIntent,
    ) -> Result<AgentProfileDeletionBarrier, OperationsError> {
        let request_id = intent.request_id();
        let profile_id = intent.profile_id();
        if request_id.is_nil() || profile_id.into_uuid().is_nil() {
            return Err(OperationsError::Conflict(
                "Agent Profile deletion barrier identity",
            ));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = load_barrier_by_request(&tx, request_id)? {
            if !barrier_matches_intent(&existing, intent) {
                return Err(OperationsError::Conflict(
                    "Agent Profile deletion barrier replay",
                ));
            }
            tx.commit()?;
            return Ok(existing);
        }
        if load_barrier_by_profile(&tx, profile_id)?.is_some() {
            return Err(OperationsError::Conflict(
                "Agent Profile deletion barrier already belongs to another request",
            ));
        }
        tx.execute(
            "INSERT INTO ops_agent_profile_deletion_barriers(
                request_id,profile_id,expected_profile_revision,
                barrier_profile_revision,requested_at_ms
             ) VALUES(?1,?2,?3,?4,?5)",
            params![
                request_id.as_bytes(),
                profile_id.into_uuid().as_bytes(),
                intent.expected_profile_revision().get(),
                intent.barrier_profile_revision().get(),
                intent.requested_at().get(),
            ],
        )?;
        let barrier = load_barrier_by_request(&tx, request_id)?
            .ok_or(OperationsError::Corrupt("Profile deletion barrier insert"))?;
        tx.commit().map_err(|source| {
            OperationsError::AgentProfileDeletionBarrierCommitUnconfirmed { request_id, source }
        })?;
        Ok(barrier)
    }

    /// Read the immutable deletion barrier for one exact Profile.
    ///
    /// # Errors
    /// Returns malformed durable rows or SQLite failures.
    pub fn profile_deletion_barrier(
        &self,
        profile_id: AgentProfileId,
    ) -> Result<Option<AgentProfileDeletionBarrier>, OperationsError> {
        load_barrier_by_profile(&self.connection, profile_id)
    }

    /// Read a deletion barrier by its stable Sessions request identity.
    ///
    /// # Errors
    /// Returns malformed durable rows or SQLite failures.
    pub fn profile_deletion_barrier_by_request(
        &self,
        request_id: Uuid,
    ) -> Result<Option<AgentProfileDeletionBarrier>, OperationsError> {
        if request_id.is_nil() {
            return Err(OperationsError::Conflict(
                "Agent Profile deletion request identity",
            ));
        }
        load_barrier_by_request(&self.connection, request_id)
    }

    /// Observe one exact deletion intent and its in-flight Operations effects in
    /// a single SQLite snapshot. These counts do not prove provider idleness.
    ///
    /// # Errors
    /// Rejects a missing or changed request/Profile barrier, malformed counters,
    /// or SQLite failures.
    pub fn profile_deletion_quiescence(
        &self,
        intent: &AgentProfileDeletionIntent,
    ) -> Result<AgentProfileDeletionInFlightCounts, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        let barrier = load_barrier_by_request(&tx, intent.request_id())?.ok_or(
            OperationsError::Conflict("Agent Profile deletion barrier missing"),
        )?;
        if !barrier_matches_intent(&barrier, intent) {
            return Err(OperationsError::Conflict(
                "Agent Profile deletion barrier changed",
            ));
        }
        let counts = profile_deletion_in_flight_counts_in_snapshot(&tx, intent.profile_id())?;
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(counts)
    }

    /// Read the exact Profile's currently in-flight provision and message rows.
    /// This is an observation, not a provider-idle or deletion-finalization proof.
    ///
    /// # Errors
    /// Returns malformed counters or SQLite failures.
    pub fn profile_deletion_in_flight_counts(
        &self,
        profile_id: AgentProfileId,
    ) -> Result<AgentProfileDeletionInFlightCounts, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        let counts = profile_deletion_in_flight_counts_in_snapshot(&tx, profile_id)?;
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(counts)
    }
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    connection.execute_batch(PROFILE_DELETION_BARRIERS_SQL)?;
    Ok(())
}

pub(crate) fn profile_deletion_in_flight_counts_in_snapshot(
    connection: &Connection,
    profile_id: AgentProfileId,
) -> Result<AgentProfileDeletionInFlightCounts, OperationsError> {
    let profile_bytes = profile_id.into_uuid().into_bytes();
    let provision_counts: (i64, i64, i64) = connection.query_row(
        "SELECT
            COALESCE(SUM(CASE WHEN state=2 THEN 1 ELSE 0 END),0),
            COALESCE(SUM(CASE WHEN state=3 THEN 1 ELSE 0 END),0),
            COALESCE(SUM(CASE WHEN state=4 THEN 1 ELSE 0 END),0)
         FROM ops_agent_provision_operations WHERE profile_id=?1",
        params![profile_bytes.as_slice()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    let message_counts: (i64, i64) = connection.query_row(
        "SELECT
            COALESCE(SUM(CASE WHEN state=2 THEN 1 ELSE 0 END),0),
            COALESCE(SUM(CASE WHEN state=5 THEN 1 ELSE 0 END),0)
         FROM ops_provider_message_outbox WHERE recipient_profile_id=?1",
        params![profile_bytes.as_slice()],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok(AgentProfileDeletionInFlightCounts {
        profile_id,
        pending_provision_operations: counter(provision_counts.0, "pending provision count")?,
        unknown_provision_operations: counter(provision_counts.1, "unknown provision count")?,
        observed_provision_operations: counter(provision_counts.2, "observed provision count")?,
        pending_provider_messages: counter(message_counts.0, "pending provider message count")?,
        unknown_provider_messages: counter(message_counts.1, "unknown provider message count")?,
    })
}

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![(
        "table",
        "ops_agent_profile_deletion_barriers",
        PROFILE_DELETION_BARRIERS_SQL,
    )]
}

pub(crate) fn require_profile_admission_open(
    connection: &Connection,
    profile_id: AgentProfileId,
) -> Result<(), OperationsError> {
    let blocked: bool = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM ops_agent_profile_deletion_barriers WHERE profile_id=?1
         )",
        params![profile_id.into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    if blocked {
        Err(OperationsError::Conflict(
            "Agent Profile deletion admission closed",
        ))
    } else {
        Ok(())
    }
}

pub(crate) fn load_barrier_by_request(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<AgentProfileDeletionBarrier>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(request_id,1,17),substr(profile_id,1,17),
                    expected_profile_revision,barrier_profile_revision,requested_at_ms
             FROM ops_agent_profile_deletion_barriers WHERE request_id=?1",
            params![request_id.as_bytes()],
            decode_raw_barrier,
        )
        .optional()?;
    raw.map(decode_barrier).transpose()
}

fn load_barrier_by_profile(
    connection: &Connection,
    profile_id: AgentProfileId,
) -> Result<Option<AgentProfileDeletionBarrier>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(request_id,1,17),substr(profile_id,1,17),
                    expected_profile_revision,barrier_profile_revision,requested_at_ms
             FROM ops_agent_profile_deletion_barriers WHERE profile_id=?1",
            params![profile_id.into_uuid().as_bytes()],
            decode_raw_barrier,
        )
        .optional()?;
    raw.map(decode_barrier).transpose()
}

pub(crate) fn barrier_matches_intent(
    barrier: &AgentProfileDeletionBarrier,
    intent: &AgentProfileDeletionIntent,
) -> bool {
    barrier.request_id == intent.request_id()
        && barrier.profile_id == intent.profile_id()
        && barrier.expected_profile_revision == intent.expected_profile_revision()
        && barrier.barrier_profile_revision == intent.barrier_profile_revision()
        && barrier.requested_at == intent.requested_at()
}

struct RawBarrier {
    request_id: Vec<u8>,
    profile_id: Vec<u8>,
    expected_profile_revision: i64,
    barrier_profile_revision: i64,
    requested_at_ms: i64,
}

fn decode_raw_barrier(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawBarrier> {
    Ok(RawBarrier {
        request_id: row.get(0)?,
        profile_id: row.get(1)?,
        expected_profile_revision: row.get(2)?,
        barrier_profile_revision: row.get(3)?,
        requested_at_ms: row.get(4)?,
    })
}

fn decode_barrier(raw: RawBarrier) -> Result<AgentProfileDeletionBarrier, OperationsError> {
    let request_id = Uuid::from_bytes(wire::id(raw.request_id)?);
    let profile_uuid = Uuid::from_bytes(wire::id(raw.profile_id)?);
    let profile_id = AgentProfileId::from_uuid(profile_uuid)
        .map_err(|_| OperationsError::Corrupt("Profile deletion barrier Profile ID"))?;
    let expected_profile_revision = AgentProfileRevision::new(raw.expected_profile_revision)
        .map_err(|_| OperationsError::Corrupt("Profile deletion expected revision"))?;
    let barrier_profile_revision = AgentProfileRevision::new(raw.barrier_profile_revision)
        .map_err(|_| OperationsError::Corrupt("Profile deletion barrier revision"))?;
    let requested_at = TimestampMillis::new(raw.requested_at_ms)
        .map_err(|_| OperationsError::Corrupt("Profile deletion requested timestamp"))?;
    Ok(AgentProfileDeletionBarrier {
        request_id,
        profile_id,
        expected_profile_revision,
        barrier_profile_revision,
        requested_at,
    })
}

fn counter(value: i64, field: &'static str) -> Result<u64, OperationsError> {
    u64::try_from(value).map_err(|_| OperationsError::Corrupt(field))
}
