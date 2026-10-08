//! Durable Profile deletion intent and admission barrier.
//!
//! The Sessions catalog records only local intent and admission state. Provider
//! deletion evidence belongs to the orchestrator/Operations saga.

use forge_domain::TimestampMillis;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use uuid::Uuid;

use super::{
    AgentProfileCatalog, AgentProfileError, AgentProfileId, AgentProfileLifecycleState,
    AgentProfileProviderFamily, AgentProfileRevision, AgentProfileRuntimeSessionId,
    AgentProfileRuntimeSessionRevision, AgentProfileRuntimeSessionState,
    MAX_PROVIDER_CONVERSATION_ID_BYTES, decode_provider, decode_runtime_state, decode_uuid,
    encode_lifecycle, encode_runtime_state, load_profile, validate_optional_text,
    validate_schema_sql,
};

const MAX_PROFILE_DELETION_INTENTS_PER_PAGE: usize = 32;
const MAX_PROFILE_DELETION_RUNTIME_ENTRIES_PER_PAGE: usize = 32;

pub(super) const PROFILE_DELETION_REQUESTS_TABLE_SQL: &str = r"
CREATE TABLE agent_profile_deletion_requests (
    request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id) = 16 AND request_id != zeroblob(16)),
    profile_id BLOB NOT NULL UNIQUE CHECK(length(profile_id) = 16 AND profile_id != zeroblob(16)),
    expected_profile_revision INTEGER NOT NULL CHECK(expected_profile_revision > 0),
    barrier_profile_revision INTEGER NOT NULL CHECK(barrier_profile_revision > 0),
    requested_at_ms INTEGER NOT NULL CHECK(requested_at_ms >= 0),
    FOREIGN KEY(profile_id) REFERENCES agent_profiles(profile_id)
);
";

pub(super) const PROFILE_DELETION_MANIFESTS_TABLE_SQL: &str = r"
CREATE TABLE agent_profile_deletion_manifests (
    request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id) = 16 AND request_id != zeroblob(16)),
    profile_id BLOB NOT NULL UNIQUE CHECK(length(profile_id) = 16 AND profile_id != zeroblob(16)),
    profile_revision_at_freeze INTEGER NOT NULL CHECK(profile_revision_at_freeze > 0),
    frozen_at_ms INTEGER NOT NULL CHECK(frozen_at_ms >= 0),
    completed_at_ms INTEGER CHECK(completed_at_ms IS NULL OR completed_at_ms >= frozen_at_ms),
    runtime_entry_count INTEGER NOT NULL CHECK(runtime_entry_count >= 0),
    linked_target_count INTEGER NOT NULL CHECK(linked_target_count >= 0),
    no_linked_target_count INTEGER NOT NULL CHECK(no_linked_target_count >= 0),
    FOREIGN KEY(request_id) REFERENCES agent_profile_deletion_requests(request_id),
    FOREIGN KEY(profile_id) REFERENCES agent_profiles(profile_id),
    CHECK(runtime_entry_count = linked_target_count + no_linked_target_count)
);
";

pub(super) const PROFILE_DELETION_RUNTIME_ENTRIES_TABLE_SQL: &str = r"
CREATE TABLE agent_profile_deletion_runtime_entries (
    request_id BLOB NOT NULL CHECK(length(request_id) = 16 AND request_id != zeroblob(16)),
    runtime_session_id BLOB NOT NULL CHECK(length(runtime_session_id) = 16 AND runtime_session_id != zeroblob(16)),
    provider_family INTEGER NOT NULL CHECK(provider_family = 1),
    provider_conversation_id TEXT CHECK(provider_conversation_id IS NULL OR length(CAST(provider_conversation_id AS BLOB)) BETWEEN 1 AND 512),
    original_state INTEGER NOT NULL CHECK(original_state BETWEEN 1 AND 8),
    original_revision INTEGER NOT NULL CHECK(original_revision > 0),
    PRIMARY KEY(request_id,runtime_session_id),
    FOREIGN KEY(request_id) REFERENCES agent_profile_deletion_manifests(request_id),
    FOREIGN KEY(runtime_session_id) REFERENCES agent_profile_runtime_sessions(runtime_session_id)
);
";

/// Exact request to stop new local admissions for one Profile before its
/// provider-side deletion saga begins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentProfileDeletionCommand {
    request_id: Uuid,
    profile_id: AgentProfileId,
    expected_profile_revision: AgentProfileRevision,
    requested_at: TimestampMillis,
}

impl AgentProfileDeletionCommand {
    /// Build one request with a stable retry identity and exact Profile CAS.
    ///
    /// # Errors
    /// Rejects a nil request identity.
    pub fn new(
        request_id: Uuid,
        profile_id: AgentProfileId,
        expected_profile_revision: AgentProfileRevision,
        requested_at: TimestampMillis,
    ) -> Result<Self, AgentProfileError> {
        if request_id.is_nil() {
            return Err(AgentProfileError::InvalidIdentity(
                "profile_deletion_request_id",
            ));
        }
        Ok(Self {
            request_id,
            profile_id,
            expected_profile_revision,
            requested_at,
        })
    }
}

/// Immutable proof that the local Profile admission barrier and deletion intent
/// were committed. This value does not certify provider-side deletion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentProfileDeletionIntent {
    request_id: Uuid,
    profile_id: AgentProfileId,
    expected_profile_revision: AgentProfileRevision,
    barrier_profile_revision: AgentProfileRevision,
    requested_at: TimestampMillis,
}

impl AgentProfileDeletionIntent {
    #[must_use]
    pub const fn request_id(self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn profile_id(self) -> AgentProfileId {
        self.profile_id
    }

    #[must_use]
    pub const fn requested_at(self) -> TimestampMillis {
        self.requested_at
    }

    #[must_use]
    pub const fn expected_profile_revision(self) -> AgentProfileRevision {
        self.expected_profile_revision
    }

    #[must_use]
    pub const fn barrier_profile_revision(self) -> AgentProfileRevision {
        self.barrier_profile_revision
    }
}

/// Immutable Sessions-side snapshot of one request's Runtime Session inventory.
/// A manifest proves only what Sessions recorded at freeze time, not external
/// provider deletion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentProfileDeletionManifest {
    request_id: Uuid,
    profile_id: AgentProfileId,
    profile_revision_at_freeze: AgentProfileRevision,
    frozen_at: TimestampMillis,
    completed_at: Option<TimestampMillis>,
    runtime_entry_count: u64,
    linked_target_count: u64,
    no_linked_target_count: u64,
}

impl AgentProfileDeletionManifest {
    #[must_use]
    pub const fn request_id(self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn profile_id(self) -> AgentProfileId {
        self.profile_id
    }

    #[must_use]
    pub const fn profile_revision_at_freeze(self) -> AgentProfileRevision {
        self.profile_revision_at_freeze
    }

    #[must_use]
    pub const fn frozen_at(self) -> TimestampMillis {
        self.frozen_at
    }

    #[must_use]
    pub const fn completed_at(self) -> Option<TimestampMillis> {
        self.completed_at
    }

    #[must_use]
    pub const fn runtime_entry_count(self) -> u64 {
        self.runtime_entry_count
    }

    #[must_use]
    pub const fn linked_target_count(self) -> u64 {
        self.linked_target_count
    }

    #[must_use]
    pub const fn no_linked_target_count(self) -> u64 {
        self.no_linked_target_count
    }
}

/// The exact local identity evidence frozen for one Runtime Session. A missing
/// conversation ID is explicit local state, not evidence that no remote target
/// exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentProfileDeletionRuntimeTarget {
    LinkedProviderConversation { conversation_id: String },
    NoLinkedTarget,
}

/// One Runtime Session copied into a deletion manifest without truncation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileDeletionRuntimeEntry {
    request_id: Uuid,
    profile_id: AgentProfileId,
    runtime_session_id: AgentProfileRuntimeSessionId,
    provider_family: AgentProfileProviderFamily,
    target: AgentProfileDeletionRuntimeTarget,
    original_revision: AgentProfileRuntimeSessionRevision,
    original_state: AgentProfileRuntimeSessionState,
}

impl AgentProfileDeletionRuntimeEntry {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn profile_id(&self) -> AgentProfileId {
        self.profile_id
    }

    #[must_use]
    pub const fn runtime_session_id(&self) -> AgentProfileRuntimeSessionId {
        self.runtime_session_id
    }

    #[must_use]
    pub const fn provider_family(&self) -> AgentProfileProviderFamily {
        self.provider_family
    }

    #[must_use]
    pub fn target(&self) -> &AgentProfileDeletionRuntimeTarget {
        &self.target
    }

    #[must_use]
    pub const fn original_revision(&self) -> AgentProfileRuntimeSessionRevision {
        self.original_revision
    }

    #[must_use]
    pub const fn original_state(&self) -> AgentProfileRuntimeSessionState {
        self.original_state
    }
}

/// One bounded ascending Runtime Session entry page for a frozen deletion
/// manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileDeletionRuntimeEntryPage {
    entries: Vec<AgentProfileDeletionRuntimeEntry>,
    next_runtime_id: Option<AgentProfileRuntimeSessionId>,
}

impl AgentProfileDeletionRuntimeEntryPage {
    #[must_use]
    pub fn entries(&self) -> &[AgentProfileDeletionRuntimeEntry] {
        &self.entries
    }

    /// Return the last row's Runtime Session ID when another page exists.
    #[must_use]
    pub const fn next_runtime_id(&self) -> Option<AgentProfileRuntimeSessionId> {
        self.next_runtime_id
    }
}

/// One ascending request-identity page of immutable Profile deletion intents.
/// No Profile lifecycle filter is applied, because retired Profiles may still
/// require reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileDeletionIntentPage {
    intents: Vec<AgentProfileDeletionIntent>,
    next_request_id: Option<Uuid>,
}

impl AgentProfileDeletionIntentPage {
    #[must_use]
    pub fn intents(&self) -> &[AgentProfileDeletionIntent] {
        &self.intents
    }

    /// Return the last row's request ID when more rows exist, for use as the
    /// next `after_request_id` cursor.
    #[must_use]
    pub const fn next_request_id(&self) -> Option<Uuid> {
        self.next_request_id
    }
}

pub(super) fn create_schema(transaction: &Transaction<'_>) -> Result<(), AgentProfileError> {
    transaction.execute_batch(PROFILE_DELETION_REQUESTS_TABLE_SQL)?;
    Ok(())
}

pub(super) fn create_manifest_schema(
    transaction: &Transaction<'_>,
) -> Result<(), AgentProfileError> {
    transaction.execute_batch(PROFILE_DELETION_MANIFESTS_TABLE_SQL)?;
    transaction.execute_batch(PROFILE_DELETION_RUNTIME_ENTRIES_TABLE_SQL)?;
    Ok(())
}

pub(super) fn validate_v4_schema(connection: &Connection) -> Result<(), AgentProfileError> {
    validate_schema_sql(
        connection,
        "agent_profile_deletion_requests",
        PROFILE_DELETION_REQUESTS_TABLE_SQL,
    )
}

pub(super) fn validate_schema(connection: &Connection) -> Result<(), AgentProfileError> {
    validate_v4_schema(connection)?;
    validate_schema_sql(
        connection,
        "agent_profile_deletion_manifests",
        PROFILE_DELETION_MANIFESTS_TABLE_SQL,
    )?;
    validate_schema_sql(
        connection,
        "agent_profile_deletion_runtime_entries",
        PROFILE_DELETION_RUNTIME_ENTRIES_TABLE_SQL,
    )
}

impl AgentProfileCatalog {
    /// Record the durable Sessions-side deletion intent and stop fresh Profile
    /// admission in the same transaction. A successful return says nothing
    /// about provider-side deletion; that proof is owned by the orchestrator
    /// and Operations saga.
    ///
    /// Replays match request ID, Profile ID, and expected revision. The first
    /// committed timestamp is retained and returned when a retry supplies a
    /// later observation time. Already Disabled or Retired Profiles are valid
    /// targets and keep their existing lifecycle revision.
    /// If commit or the post-commit path check has an uncertain result, read
    /// back by request ID or retry this exact command before creating another
    /// request.
    ///
    /// # Errors
    /// Rejects missing Profiles, stale revisions, a regressed timestamp,
    /// conflicting request replay, an existing deletion request under another
    /// identity, path-integrity failures, or SQLite errors.
    pub fn begin_profile_deletion(
        &mut self,
        command: &AgentProfileDeletionCommand,
    ) -> Result<AgentProfileDeletionIntent, AgentProfileError> {
        self.ensure_live_binding()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = load_intent_by_request(&transaction, command.request_id)? {
            if existing.profile_id != command.profile_id
                || existing.expected_profile_revision != command.expected_profile_revision
            {
                return Err(AgentProfileError::ProfileDeletionRequestReplayConflict(
                    command.request_id,
                ));
            }
            transaction.commit()?;
            self.ensure_live_binding()?;
            return Ok(existing);
        }
        if let Some(existing) = load_intent_by_profile(&transaction, command.profile_id)? {
            return Err(AgentProfileError::ProfileDeletionAlreadyRequested {
                profile_id: command.profile_id,
                request_id: existing.request_id,
            });
        }
        let profile = load_profile(&transaction, command.profile_id)?
            .ok_or(AgentProfileError::ProfileMissing(command.profile_id))?;
        if profile.revision() != command.expected_profile_revision {
            return Err(AgentProfileError::ProfileRevisionConflict {
                profile_id: command.profile_id,
                expected: command.expected_profile_revision.get(),
                actual: profile.revision().get(),
            });
        }
        if command.requested_at < profile.updated_at() {
            return Err(AgentProfileError::ProfileClockRegression {
                profile_id: command.profile_id,
                expected_at_least: profile.updated_at().get(),
                observed: command.requested_at.get(),
            });
        }

        let barrier_profile_revision = match profile.lifecycle_state() {
            AgentProfileLifecycleState::Active => {
                let next = AgentProfileRevision::new(
                    profile
                        .revision()
                        .get()
                        .checked_add(1)
                        .ok_or(AgentProfileError::InvalidRevision(i64::MAX))?,
                )?;
                let changed = transaction.execute(
                    "UPDATE agent_profiles
                     SET lifecycle_state=?1,updated_at_ms=?2,revision=?3
                     WHERE profile_id=?4 AND revision=?5 AND lifecycle_state=?6",
                    params![
                        super::encode_lifecycle(AgentProfileLifecycleState::Disabled),
                        command.requested_at.get(),
                        next.get(),
                        command.profile_id.into_uuid().as_bytes(),
                        profile.revision().get(),
                        super::encode_lifecycle(AgentProfileLifecycleState::Active),
                    ],
                )?;
                if changed != 1 {
                    return Err(AgentProfileError::Corrupt(
                        "Profile deletion barrier compare-and-set",
                    ));
                }
                next
            }
            AgentProfileLifecycleState::Disabled | AgentProfileLifecycleState::Retired => {
                profile.revision()
            }
        };
        let intent = AgentProfileDeletionIntent {
            request_id: command.request_id,
            profile_id: command.profile_id,
            expected_profile_revision: command.expected_profile_revision,
            barrier_profile_revision,
            requested_at: command.requested_at,
        };
        transaction.execute(
            "INSERT INTO agent_profile_deletion_requests(
                request_id,profile_id,expected_profile_revision,
                barrier_profile_revision,requested_at_ms
             ) VALUES(?1,?2,?3,?4,?5)",
            params![
                intent.request_id.as_bytes(),
                intent.profile_id.into_uuid().as_bytes(),
                intent.expected_profile_revision.get(),
                intent.barrier_profile_revision.get(),
                intent.requested_at.get(),
            ],
        )?;
        transaction.commit()?;
        self.ensure_live_binding()?;
        Ok(intent)
    }

    /// Read one durable deletion intent by its exact request identity.
    ///
    /// # Errors
    /// Returns corrupt-row, path-integrity, or SQLite errors.
    pub fn profile_deletion_intent(
        &self,
        request_id: Uuid,
    ) -> Result<Option<AgentProfileDeletionIntent>, AgentProfileError> {
        self.ensure_live_binding()?;
        load_intent_by_request(&self.connection, request_id)
    }

    /// Read the single durable deletion intent for one Profile, if requested.
    ///
    /// # Errors
    /// Returns corrupt-row, path-integrity, or SQLite errors.
    pub fn profile_deletion_intent_for_profile(
        &self,
        profile_id: AgentProfileId,
    ) -> Result<Option<AgentProfileDeletionIntent>, AgentProfileError> {
        self.ensure_live_binding()?;
        load_intent_by_profile(&self.connection, profile_id)
    }

    /// Read all immutable deletion intents in bounded ascending request-ID
    /// pages. The cursor is keyset-based and does not freeze an inventory: an
    /// intent inserted below the cursor during a scan appears on a later scan
    /// restarted with `None`.
    ///
    /// # Errors
    /// Rejects a nil cursor or limit outside 1..=32, and returns corrupt-row,
    /// path-integrity, or SQLite errors.
    pub fn profile_deletion_intents_page(
        &self,
        after_request_id: Option<Uuid>,
        limit: u32,
    ) -> Result<AgentProfileDeletionIntentPage, AgentProfileError> {
        self.ensure_live_binding()?;
        if after_request_id.is_some_and(|id| id.is_nil()) {
            return Err(AgentProfileError::InvalidIdentity(
                "profile_deletion_request_cursor",
            ));
        }
        let page_size = match usize::try_from(limit) {
            Ok(page_size) if (1..=MAX_PROFILE_DELETION_INTENTS_PER_PAGE).contains(&page_size) => {
                page_size
            }
            _ => {
                return Err(AgentProfileError::ProfileDeletionIntentPageLimit {
                    actual: limit,
                    maximum: MAX_PROFILE_DELETION_INTENTS_PER_PAGE,
                });
            }
        };
        profile_deletion_intents_page(&self.connection, after_request_id, page_size)
    }

    /// Freeze the complete Runtime Session inventory for one already persisted
    /// deletion intent. The host must first establish the Operations-side
    /// quiescence barrier. This operation copies every local Runtime row in one
    /// Immediate transaction and does not accept a caller-provided target list.
    ///
    /// Exact request/revision replay returns the original freeze timestamp and
    /// counts. Commit or post-commit uncertainty must be recovered by reading
    /// the manifest or retrying with the same request/revision.
    ///
    /// # Errors
    /// Rejects missing intent/Profile, an Active Profile, stale revisions,
    /// clock regression, replay conflict, or persistence errors.
    pub fn freeze_profile_deletion_targets(
        &mut self,
        request_id: Uuid,
        expected_profile_revision: AgentProfileRevision,
        frozen_at: TimestampMillis,
    ) -> Result<AgentProfileDeletionManifest, AgentProfileError> {
        if request_id.is_nil() {
            return Err(AgentProfileError::InvalidIdentity(
                "profile_deletion_request_id",
            ));
        }
        self.ensure_live_binding()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = load_manifest_by_request(&transaction, request_id)? {
            if existing.profile_revision_at_freeze != expected_profile_revision {
                return Err(AgentProfileError::ProfileDeletionManifestReplayConflict(
                    request_id,
                ));
            }
            transaction.commit()?;
            self.ensure_live_binding()?;
            return Ok(existing);
        }
        let manifest = freeze_manifest_in_transaction(
            &transaction,
            request_id,
            expected_profile_revision,
            frozen_at,
        )?;
        transaction.commit()?;
        self.ensure_live_binding()?;
        Ok(manifest)
    }

    /// Read one immutable frozen manifest by its deletion request identity.
    ///
    /// # Errors
    /// Returns corrupt-row, path-integrity, or SQLite errors.
    pub fn profile_deletion_manifest(
        &self,
        request_id: Uuid,
    ) -> Result<Option<AgentProfileDeletionManifest>, AgentProfileError> {
        self.ensure_live_binding()?;
        load_manifest_by_request(&self.connection, request_id)
    }

    /// Validate the complete frozen inventory in one read snapshot, streaming
    /// rows without retaining an unbounded collection.
    ///
    /// # Errors
    /// Rejects missing rows, changed target identities, malformed entries or counts.
    pub fn validate_profile_deletion_manifest_entries(
        &self,
        request_id: Uuid,
    ) -> Result<(), AgentProfileError> {
        self.ensure_live_binding()?;
        let tx = self.connection.unchecked_transaction()?;
        let manifest = load_manifest_by_request(&tx, request_id)?.ok_or(
            AgentProfileError::ProfileDeletionManifestMissing(request_id),
        )?;
        validate_manifest_inventory(&tx, manifest)?;
        tx.commit()?;
        self.ensure_live_binding()
    }

    /// Read one exact frozen Runtime entry without scanning preceding pages.
    ///
    /// # Errors
    /// Rejects a missing manifest and returns corrupt-row, path-integrity, or
    /// SQLite errors. A missing entry within an existing manifest returns None.
    pub fn profile_deletion_runtime_entry(
        &self,
        request_id: Uuid,
        runtime_session_id: AgentProfileRuntimeSessionId,
    ) -> Result<Option<AgentProfileDeletionRuntimeEntry>, AgentProfileError> {
        self.ensure_live_binding()?;
        let manifest = load_manifest_by_request(&self.connection, request_id)?.ok_or(
            AgentProfileError::ProfileDeletionManifestMissing(request_id),
        )?;
        let raw = self
            .connection
            .query_row(
                "SELECT runtime_session_id,provider_family,provider_conversation_id,
                    original_state,original_revision
             FROM agent_profile_deletion_runtime_entries
             WHERE request_id=?1 AND runtime_session_id=?2",
                params![
                    request_id.as_bytes(),
                    runtime_session_id.into_uuid().as_bytes()
                ],
                |row| {
                    Ok(RawDeletionRuntimeEntry {
                        runtime_session_id: row.get(0)?,
                        provider_family: row.get(1)?,
                        provider_conversation_id: row.get(2)?,
                        original_state: row.get(3)?,
                        original_revision: row.get(4)?,
                    })
                },
            )
            .optional()?;
        raw.as_ref()
            .map(|row| decode_runtime_entry(manifest, row))
            .transpose()
    }

    /// Read the complete frozen Runtime Session inventory in bounded UUID-keyset
    /// pages. A `NoLinkedTarget` entry means Sessions recorded a null
    /// conversation ID; it is not evidence that no remote operation exists.
    ///
    /// # Errors
    /// Rejects a missing manifest or a limit outside 1..=32, and returns
    /// corrupt-row, path-integrity, or SQLite errors.
    pub fn profile_deletion_runtime_entries_page(
        &self,
        request_id: Uuid,
        after_runtime_id: Option<AgentProfileRuntimeSessionId>,
        limit: u32,
    ) -> Result<AgentProfileDeletionRuntimeEntryPage, AgentProfileError> {
        self.ensure_live_binding()?;
        let manifest = load_manifest_by_request(&self.connection, request_id)?.ok_or(
            AgentProfileError::ProfileDeletionManifestMissing(request_id),
        )?;
        let page_size = match usize::try_from(limit) {
            Ok(page_size)
                if (1..=MAX_PROFILE_DELETION_RUNTIME_ENTRIES_PER_PAGE).contains(&page_size) =>
            {
                page_size
            }
            _ => {
                return Err(AgentProfileError::ProfileDeletionEntryPageLimit {
                    actual: limit,
                    maximum: MAX_PROFILE_DELETION_RUNTIME_ENTRIES_PER_PAGE,
                });
            }
        };
        deletion_runtime_entries_page(&self.connection, manifest, after_runtime_id, page_size)
    }

    /// Finalize the local deletion manifest after the host has verified a
    /// positive Operations deletion receipt for every linked provider target.
    /// The host must also reconcile operations represented by `NoLinkedTarget`;
    /// a null Sessions conversation ID does not prove no remote operation exists.
    /// This method accepts no receipt list or boolean proof; that authority
    /// remains with the host/Operations boundary. It preserves Runtime IDs,
    /// conversation identity, logical Session history, and already-closed row
    /// timestamps/revisions.
    ///
    /// Exact replay returns the first completed timestamp. After uncertain
    /// commit/post-commit outcomes, read back by request ID or retry this
    /// request before changing identity.
    ///
    /// # Errors
    /// Rejects missing manifests/Profiles, an Active Profile, a regressed
    /// completion clock, revision overflow, or persistence errors.
    pub fn finish_profile_deletion(
        &mut self,
        request_id: Uuid,
        completed_at: TimestampMillis,
    ) -> Result<AgentProfileDeletionManifest, AgentProfileError> {
        if request_id.is_nil() {
            return Err(AgentProfileError::InvalidIdentity(
                "profile_deletion_request_id",
            ));
        }
        self.ensure_live_binding()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let manifest = load_manifest_by_request(&transaction, request_id)?.ok_or(
            AgentProfileError::ProfileDeletionManifestMissing(request_id),
        )?;
        validate_manifest_inventory(&transaction, manifest)?;
        if manifest.completed_at.is_some() {
            transaction.commit()?;
            self.ensure_live_binding()?;
            return Ok(manifest);
        }
        let completed = finish_manifest_in_transaction(&transaction, manifest, completed_at)?;
        transaction.commit()?;
        self.ensure_live_binding()?;
        Ok(completed)
    }
}

fn validate_manifest_inventory(
    connection: &Connection,
    manifest: AgentProfileDeletionManifest,
) -> Result<(), AgentProfileError> {
    let intent = load_intent_by_request(connection, manifest.request_id)?
        .ok_or(AgentProfileError::Corrupt("frozen deletion intent missing"))?;
    if manifest.profile_id != intent.profile_id
        || manifest.profile_revision_at_freeze < intent.barrier_profile_revision
        || manifest.frozen_at < intent.requested_at
    {
        return Err(AgentProfileError::Corrupt(
            "frozen deletion intent fence changed",
        ));
    }
    let mut statement = connection.prepare(
        "SELECT e.runtime_session_id,e.provider_family,e.provider_conversation_id,
                e.original_state,e.original_revision,r.profile_id,r.provider_family,
                r.provider_conversation_id,r.revision
         FROM agent_profile_deletion_runtime_entries e
         LEFT JOIN agent_profile_runtime_sessions r ON r.runtime_session_id=e.runtime_session_id
         WHERE e.request_id=?1 ORDER BY e.runtime_session_id",
    )?;
    let mut rows = statement.query(params![manifest.request_id.as_bytes()])?;
    let mut counts = [0_u64; 3];
    while let Some(row) = rows.next()? {
        let raw = RawDeletionRuntimeEntry {
            runtime_session_id: row.get(0)?,
            provider_family: row.get(1)?,
            provider_conversation_id: row.get(2)?,
            original_state: row.get(3)?,
            original_revision: row.get(4)?,
        };
        let entry = decode_runtime_entry(manifest, &raw)?;
        let current_profile: Option<Vec<u8>> = row.get(5)?;
        let current_family: Option<i64> = row.get(6)?;
        let current_conversation: Option<String> = row.get(7)?;
        let current_revision: Option<i64> = row.get(8)?;
        if current_profile.as_deref() != Some(manifest.profile_id.into_uuid().as_bytes())
            || current_family != Some(raw.provider_family)
            || current_conversation != raw.provider_conversation_id
            || current_revision.is_none_or(|revision| revision < raw.original_revision)
        {
            return Err(AgentProfileError::Corrupt(
                "frozen deletion Runtime identity changed",
            ));
        }
        counts[0] = counts[0]
            .checked_add(1)
            .ok_or(AgentProfileError::Corrupt("deletion inventory count"))?;
        let kind = if matches!(
            entry.target(),
            AgentProfileDeletionRuntimeTarget::NoLinkedTarget
        ) {
            2
        } else {
            1
        };
        counts[kind] = counts[kind]
            .checked_add(1)
            .ok_or(AgentProfileError::Corrupt("deletion inventory count"))?;
    }
    if counts
        != [
            manifest.runtime_entry_count,
            manifest.linked_target_count,
            manifest.no_linked_target_count,
        ]
    {
        return Err(AgentProfileError::Corrupt(
            "frozen deletion inventory is incomplete",
        ));
    }
    Ok(())
}

#[derive(Debug)]
struct RawProfileDeletionIntent {
    request_id: Vec<u8>,
    profile_id: Vec<u8>,
    expected_profile_revision: i64,
    barrier_profile_revision: i64,
    requested_at: i64,
}

fn load_intent_by_request(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<AgentProfileDeletionIntent>, AgentProfileError> {
    load_intent(connection, "request_id", request_id.as_bytes())
}

fn load_intent_by_profile(
    connection: &Connection,
    profile_id: AgentProfileId,
) -> Result<Option<AgentProfileDeletionIntent>, AgentProfileError> {
    load_intent(connection, "profile_id", profile_id.into_uuid().as_bytes())
}

fn load_intent(
    connection: &Connection,
    key_column: &'static str,
    key: &[u8],
) -> Result<Option<AgentProfileDeletionIntent>, AgentProfileError> {
    let sql = match key_column {
        "request_id" => {
            "SELECT request_id,profile_id,expected_profile_revision,
                    barrier_profile_revision,requested_at_ms
             FROM agent_profile_deletion_requests WHERE request_id=?1"
        }
        "profile_id" => {
            "SELECT request_id,profile_id,expected_profile_revision,
                    barrier_profile_revision,requested_at_ms
             FROM agent_profile_deletion_requests WHERE profile_id=?1"
        }
        _ => return Err(AgentProfileError::Corrupt("deletion intent lookup key")),
    };
    let raw = connection
        .query_row(sql, params![key], |row| {
            Ok(RawProfileDeletionIntent {
                request_id: row.get(0)?,
                profile_id: row.get(1)?,
                expected_profile_revision: row.get(2)?,
                barrier_profile_revision: row.get(3)?,
                requested_at: row.get(4)?,
            })
        })
        .optional()?;
    raw.as_ref().map(decode_intent).transpose()
}

fn profile_deletion_intents_page(
    connection: &Connection,
    after_request_id: Option<Uuid>,
    page_size: usize,
) -> Result<AgentProfileDeletionIntentPage, AgentProfileError> {
    let lookahead_limit = i64::try_from(page_size + 1)
        .map_err(|_| AgentProfileError::Corrupt("Profile deletion intent page bound"))?;
    let after_request_id = after_request_id.map(|id| id.as_bytes().to_vec());
    let mut statement = connection.prepare(
        "SELECT request_id,profile_id,expected_profile_revision,
                barrier_profile_revision,requested_at_ms
         FROM agent_profile_deletion_requests
         WHERE (?1 IS NULL OR request_id>?1)
         ORDER BY request_id ASC LIMIT ?2",
    )?;
    let mut rows = statement
        .query_map(params![after_request_id, lookahead_limit], |row| {
            Ok(RawProfileDeletionIntent {
                request_id: row.get(0)?,
                profile_id: row.get(1)?,
                expected_profile_revision: row.get(2)?,
                barrier_profile_revision: row.get(3)?,
                requested_at: row.get(4)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let has_more = rows.len() > page_size;
    let next_request_id = if has_more {
        let id = decode_uuid(
            "profile_deletion_request_id",
            &rows[page_size - 1].request_id,
        )?;
        if id.is_nil() {
            return Err(AgentProfileError::Corrupt(
                "nil Profile deletion request id",
            ));
        }
        Some(id)
    } else {
        None
    };
    rows.truncate(page_size);
    let intents = rows
        .iter()
        .map(decode_intent)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AgentProfileDeletionIntentPage {
        intents,
        next_request_id,
    })
}

fn decode_intent(
    raw: &RawProfileDeletionIntent,
) -> Result<AgentProfileDeletionIntent, AgentProfileError> {
    let request_id = decode_uuid("profile_deletion_request_id", &raw.request_id)?;
    if request_id.is_nil() {
        return Err(AgentProfileError::Corrupt(
            "nil Profile deletion request id",
        ));
    }
    Ok(AgentProfileDeletionIntent {
        request_id,
        profile_id: AgentProfileId::from_uuid(decode_uuid(
            "profile_deletion_profile_id",
            &raw.profile_id,
        )?)?,
        expected_profile_revision: AgentProfileRevision::new(raw.expected_profile_revision)?,
        barrier_profile_revision: AgentProfileRevision::new(raw.barrier_profile_revision)?,
        requested_at: TimestampMillis::new(raw.requested_at)?,
    })
}

#[derive(Debug)]
struct RawDeletionManifest {
    request_id: Vec<u8>,
    profile_id: Vec<u8>,
    profile_revision_at_freeze: i64,
    frozen_at: i64,
    completed_at: Option<i64>,
    runtime_entry_count: i64,
    linked_target_count: i64,
    no_linked_target_count: i64,
}

fn load_manifest_by_request(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<AgentProfileDeletionManifest>, AgentProfileError> {
    let raw = connection
        .query_row(
            "SELECT request_id,profile_id,profile_revision_at_freeze,frozen_at_ms,
                    completed_at_ms,runtime_entry_count,linked_target_count,
                    no_linked_target_count
             FROM agent_profile_deletion_manifests WHERE request_id=?1",
            params![request_id.as_bytes()],
            |row| {
                Ok(RawDeletionManifest {
                    request_id: row.get(0)?,
                    profile_id: row.get(1)?,
                    profile_revision_at_freeze: row.get(2)?,
                    frozen_at: row.get(3)?,
                    completed_at: row.get(4)?,
                    runtime_entry_count: row.get(5)?,
                    linked_target_count: row.get(6)?,
                    no_linked_target_count: row.get(7)?,
                })
            },
        )
        .optional()?;
    raw.as_ref().map(decode_manifest).transpose()
}

fn decode_manifest(
    raw: &RawDeletionManifest,
) -> Result<AgentProfileDeletionManifest, AgentProfileError> {
    let request_id = decode_uuid("profile_deletion_manifest_request_id", &raw.request_id)?;
    if request_id.is_nil() {
        return Err(AgentProfileError::Corrupt(
            "nil Profile deletion manifest request id",
        ));
    }
    let frozen_at = TimestampMillis::new(raw.frozen_at)?;
    let completed_at = raw.completed_at.map(TimestampMillis::new).transpose()?;
    let runtime_entry_count = decode_entry_count(raw.runtime_entry_count)?;
    let linked_target_count = decode_entry_count(raw.linked_target_count)?;
    let no_linked_target_count = decode_entry_count(raw.no_linked_target_count)?;
    if linked_target_count.checked_add(no_linked_target_count) != Some(runtime_entry_count)
        || completed_at.is_some_and(|completed| completed < frozen_at)
    {
        return Err(AgentProfileError::Corrupt(
            "Profile deletion manifest counts or timestamps",
        ));
    }
    Ok(AgentProfileDeletionManifest {
        request_id,
        profile_id: AgentProfileId::from_uuid(decode_uuid(
            "profile_deletion_manifest_profile_id",
            &raw.profile_id,
        )?)?,
        profile_revision_at_freeze: AgentProfileRevision::new(raw.profile_revision_at_freeze)?,
        frozen_at,
        completed_at,
        runtime_entry_count,
        linked_target_count,
        no_linked_target_count,
    })
}

fn decode_entry_count(value: i64) -> Result<u64, AgentProfileError> {
    u64::try_from(value).map_err(|_| AgentProfileError::Corrupt("negative deletion entry count"))
}

pub(super) fn manifest_exists_for_profile(
    connection: &Connection,
    profile_id: AgentProfileId,
) -> Result<bool, AgentProfileError> {
    Ok(connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM agent_profile_deletion_manifests WHERE profile_id=?1
         )",
        params![profile_id.into_uuid().as_bytes()],
        |row| row.get(0),
    )?)
}

#[derive(Debug)]
struct RuntimeEntryStats {
    total: i64,
    linked: i64,
    unlinked: i64,
    max_last_seen: i64,
}

fn runtime_entry_stats(
    connection: &Connection,
    profile_id: AgentProfileId,
) -> Result<RuntimeEntryStats, AgentProfileError> {
    let stats = connection.query_row(
        "SELECT COUNT(*),
                COALESCE(SUM(CASE WHEN provider_conversation_id IS NOT NULL THEN 1 ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN provider_conversation_id IS NULL THEN 1 ELSE 0 END),0),
                COALESCE(MAX(last_seen_at_ms),0)
         FROM agent_profile_runtime_sessions WHERE profile_id=?1",
        params![profile_id.into_uuid().as_bytes()],
        |row| {
            Ok(RuntimeEntryStats {
                total: row.get(0)?,
                linked: row.get(1)?,
                unlinked: row.get(2)?,
                max_last_seen: row.get(3)?,
            })
        },
    )?;
    if stats.total < 0
        || stats.linked < 0
        || stats.unlinked < 0
        || stats.linked.checked_add(stats.unlinked) != Some(stats.total)
    {
        return Err(AgentProfileError::Corrupt(
            "Profile deletion Runtime Session counts",
        ));
    }
    Ok(stats)
}

fn freeze_manifest_in_transaction(
    transaction: &Transaction<'_>,
    request_id: Uuid,
    expected_profile_revision: AgentProfileRevision,
    frozen_at: TimestampMillis,
) -> Result<AgentProfileDeletionManifest, AgentProfileError> {
    let intent = load_intent_by_request(transaction, request_id)?
        .ok_or(AgentProfileError::ProfileDeletionIntentMissing(request_id))?;
    let profile = load_profile(transaction, intent.profile_id)?
        .ok_or(AgentProfileError::ProfileMissing(intent.profile_id))?;
    if profile.revision() != expected_profile_revision {
        return Err(AgentProfileError::ProfileRevisionConflict {
            profile_id: intent.profile_id,
            expected: expected_profile_revision.get(),
            actual: profile.revision().get(),
        });
    }
    if profile.lifecycle_state() == AgentProfileLifecycleState::Active {
        return Err(AgentProfileError::Corrupt(
            "deletion manifest requires inactive Profile",
        ));
    }
    let stats = runtime_entry_stats(transaction, intent.profile_id)?;
    let latest_required_time = profile
        .updated_at()
        .get()
        .max(stats.max_last_seen)
        .max(intent.requested_at().get());
    if frozen_at.get() < latest_required_time {
        return Err(AgentProfileError::ProfileClockRegression {
            profile_id: intent.profile_id,
            expected_at_least: latest_required_time,
            observed: frozen_at.get(),
        });
    }
    let manifest = AgentProfileDeletionManifest {
        request_id,
        profile_id: intent.profile_id,
        profile_revision_at_freeze: expected_profile_revision,
        frozen_at,
        completed_at: None,
        runtime_entry_count: decode_entry_count(stats.total)?,
        linked_target_count: decode_entry_count(stats.linked)?,
        no_linked_target_count: decode_entry_count(stats.unlinked)?,
    };
    transaction.execute(
        "INSERT INTO agent_profile_deletion_manifests(
            request_id,profile_id,profile_revision_at_freeze,frozen_at_ms,
            completed_at_ms,runtime_entry_count,linked_target_count,no_linked_target_count
         ) VALUES(?1,?2,?3,?4,NULL,?5,?6,?7)",
        params![
            manifest.request_id.as_bytes(),
            manifest.profile_id.into_uuid().as_bytes(),
            manifest.profile_revision_at_freeze.get(),
            manifest.frozen_at.get(),
            stats.total,
            stats.linked,
            stats.unlinked,
        ],
    )?;
    let inserted = transaction.execute(
        "INSERT INTO agent_profile_deletion_runtime_entries(
            request_id,runtime_session_id,provider_family,provider_conversation_id,
            original_state,original_revision
         )
         SELECT ?1,runtime_session_id,provider_family,provider_conversation_id,state,revision
         FROM agent_profile_runtime_sessions WHERE profile_id=?2",
        params![
            manifest.request_id.as_bytes(),
            manifest.profile_id.into_uuid().as_bytes(),
        ],
    )?;
    if i64::try_from(inserted).ok() != Some(stats.total) {
        return Err(AgentProfileError::Corrupt(
            "Profile deletion Runtime Session freeze count changed",
        ));
    }
    Ok(manifest)
}

#[derive(Debug)]
struct RawDeletionRuntimeEntry {
    runtime_session_id: Vec<u8>,
    provider_family: i64,
    provider_conversation_id: Option<String>,
    original_state: i64,
    original_revision: i64,
}

fn decode_runtime_entry(
    manifest: AgentProfileDeletionManifest,
    raw: &RawDeletionRuntimeEntry,
) -> Result<AgentProfileDeletionRuntimeEntry, AgentProfileError> {
    validate_optional_text(
        "provider_conversation_id",
        raw.provider_conversation_id.as_deref(),
        MAX_PROVIDER_CONVERSATION_ID_BYTES,
    )?;
    let target = match raw.provider_conversation_id.as_deref() {
        Some(conversation_id) => AgentProfileDeletionRuntimeTarget::LinkedProviderConversation {
            conversation_id: conversation_id.to_owned(),
        },
        None => AgentProfileDeletionRuntimeTarget::NoLinkedTarget,
    };
    Ok(AgentProfileDeletionRuntimeEntry {
        request_id: manifest.request_id,
        profile_id: manifest.profile_id,
        runtime_session_id: AgentProfileRuntimeSessionId::from_uuid(decode_uuid(
            "deletion_runtime_session_id",
            &raw.runtime_session_id,
        )?)?,
        provider_family: decode_provider(raw.provider_family)?,
        target,
        original_revision: AgentProfileRuntimeSessionRevision::new(raw.original_revision)?,
        original_state: decode_runtime_state(raw.original_state)?,
    })
}

fn deletion_runtime_entries_page(
    connection: &Connection,
    manifest: AgentProfileDeletionManifest,
    after_runtime_id: Option<AgentProfileRuntimeSessionId>,
    page_size: usize,
) -> Result<AgentProfileDeletionRuntimeEntryPage, AgentProfileError> {
    let limit = i64::try_from(page_size + 1)
        .map_err(|_| AgentProfileError::Corrupt("deletion Runtime entry page bound"))?;
    let after = after_runtime_id.map(|id| id.into_uuid().as_bytes().to_vec());
    let mut statement = connection.prepare(
        "SELECT runtime_session_id,provider_family,provider_conversation_id,
                original_state,original_revision
         FROM agent_profile_deletion_runtime_entries
         WHERE request_id=?1 AND (?2 IS NULL OR runtime_session_id>?2)
         ORDER BY runtime_session_id ASC LIMIT ?3",
    )?;
    let mut rows = statement
        .query_map(
            params![manifest.request_id.as_bytes(), after, limit],
            |row| {
                Ok(RawDeletionRuntimeEntry {
                    runtime_session_id: row.get(0)?,
                    provider_family: row.get(1)?,
                    provider_conversation_id: row.get(2)?,
                    original_state: row.get(3)?,
                    original_revision: row.get(4)?,
                })
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;
    let has_more = rows.len() > page_size;
    let next_runtime_id = if has_more {
        Some(AgentProfileRuntimeSessionId::from_uuid(decode_uuid(
            "deletion_runtime_session_id",
            &rows[page_size - 1].runtime_session_id,
        )?)?)
    } else {
        None
    };
    rows.truncate(page_size);
    let entries = rows
        .iter()
        .map(|row| decode_runtime_entry(manifest, row))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AgentProfileDeletionRuntimeEntryPage {
        entries,
        next_runtime_id,
    })
}

#[derive(Debug)]
struct FrozenRuntimeStats {
    total: i64,
    max_last_seen: i64,
    non_closed: i64,
}

fn frozen_runtime_stats(
    transaction: &Transaction<'_>,
    manifest: AgentProfileDeletionManifest,
) -> Result<FrozenRuntimeStats, AgentProfileError> {
    let closed_state = encode_runtime_state(AgentProfileRuntimeSessionState::Closed);
    let stats = transaction.query_row(
        "SELECT COUNT(*),COALESCE(MAX(r.last_seen_at_ms),0),
                COALESCE(SUM(CASE WHEN r.state!=?2 THEN 1 ELSE 0 END),0)
         FROM agent_profile_deletion_runtime_entries e
         JOIN agent_profile_runtime_sessions r
           ON r.runtime_session_id=e.runtime_session_id
         WHERE e.request_id=?1 AND r.profile_id=?3",
        params![
            manifest.request_id.as_bytes(),
            closed_state,
            manifest.profile_id.into_uuid().as_bytes(),
        ],
        |row| {
            Ok(FrozenRuntimeStats {
                total: row.get(0)?,
                max_last_seen: row.get(1)?,
                non_closed: row.get(2)?,
            })
        },
    )?;
    if stats.total < 0 || stats.non_closed < 0 {
        return Err(AgentProfileError::Corrupt(
            "negative frozen Runtime Session count",
        ));
    }
    Ok(stats)
}

fn finish_manifest_in_transaction(
    transaction: &Transaction<'_>,
    manifest: AgentProfileDeletionManifest,
    completed_at: TimestampMillis,
) -> Result<AgentProfileDeletionManifest, AgentProfileError> {
    let intent = load_intent_by_request(transaction, manifest.request_id)?.ok_or(
        AgentProfileError::ProfileDeletionIntentMissing(manifest.request_id),
    )?;
    if intent.profile_id() != manifest.profile_id {
        return Err(AgentProfileError::Corrupt(
            "Profile deletion manifest intent scope",
        ));
    }
    let profile = load_profile(transaction, manifest.profile_id)?
        .ok_or(AgentProfileError::ProfileMissing(manifest.profile_id))?;
    if profile.lifecycle_state() == AgentProfileLifecycleState::Active {
        return Err(AgentProfileError::Corrupt(
            "deletion completion requires inactive Profile",
        ));
    }
    let stats = frozen_runtime_stats(transaction, manifest)?;
    if i64::try_from(manifest.runtime_entry_count).ok() != Some(stats.total) {
        return Err(AgentProfileError::Corrupt(
            "frozen Runtime Session inventory changed",
        ));
    }
    let earliest_completion = manifest
        .frozen_at
        .get()
        .max(profile.updated_at().get())
        .max(stats.max_last_seen)
        .max(intent.requested_at().get());
    if completed_at.get() < earliest_completion {
        return Err(AgentProfileError::ProfileClockRegression {
            profile_id: manifest.profile_id,
            expected_at_least: earliest_completion,
            observed: completed_at.get(),
        });
    }
    ensure_frozen_runtime_revision_capacity(transaction, manifest)?;
    close_frozen_runtime_entries(transaction, manifest, completed_at, stats.non_closed)?;
    retire_manifest_profile(transaction, &profile, completed_at)?;
    mark_manifest_completed(transaction, manifest.request_id, completed_at)?;
    Ok(AgentProfileDeletionManifest {
        completed_at: Some(completed_at),
        ..manifest
    })
}

fn ensure_frozen_runtime_revision_capacity(
    transaction: &Transaction<'_>,
    manifest: AgentProfileDeletionManifest,
) -> Result<(), AgentProfileError> {
    let closed_state = encode_runtime_state(AgentProfileRuntimeSessionState::Closed);
    let revision_maxed: bool = transaction.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM agent_profile_deletion_runtime_entries e
            JOIN agent_profile_runtime_sessions r
              ON r.runtime_session_id=e.runtime_session_id
            WHERE e.request_id=?1 AND r.profile_id=?2
              AND r.state!=?3 AND r.revision>=?4
         )",
        params![
            manifest.request_id.as_bytes(),
            manifest.profile_id.into_uuid().as_bytes(),
            closed_state,
            i64::MAX,
        ],
        |row| row.get(0),
    )?;
    if revision_maxed {
        return Err(AgentProfileError::RuntimeSessionRevisionOverflow);
    }
    Ok(())
}

fn close_frozen_runtime_entries(
    transaction: &Transaction<'_>,
    manifest: AgentProfileDeletionManifest,
    completed_at: TimestampMillis,
    expected_changed: i64,
) -> Result<(), AgentProfileError> {
    let closed_state = encode_runtime_state(AgentProfileRuntimeSessionState::Closed);
    let changed = transaction.execute(
        "UPDATE agent_profile_runtime_sessions
         SET state=?1,closed_at_ms=?2,last_seen_at_ms=?2,revision=revision+1
         WHERE state!=?1 AND runtime_session_id IN (
             SELECT e.runtime_session_id
             FROM agent_profile_deletion_runtime_entries e
             JOIN agent_profile_deletion_manifests m ON m.request_id=e.request_id
             WHERE e.request_id=?3 AND m.profile_id=?4
         )",
        params![
            closed_state,
            completed_at.get(),
            manifest.request_id.as_bytes(),
            manifest.profile_id.into_uuid().as_bytes(),
        ],
    )?;
    if i64::try_from(changed).ok() != Some(expected_changed) {
        return Err(AgentProfileError::Corrupt(
            "frozen Runtime Session close count changed",
        ));
    }
    Ok(())
}

fn retire_manifest_profile(
    transaction: &Transaction<'_>,
    profile: &super::AgentProfile,
    completed_at: TimestampMillis,
) -> Result<(), AgentProfileError> {
    if profile.lifecycle_state() == AgentProfileLifecycleState::Retired {
        return Ok(());
    }
    let next_revision = AgentProfileRevision::new(
        profile
            .revision()
            .get()
            .checked_add(1)
            .ok_or(AgentProfileError::InvalidRevision(i64::MAX))?,
    )?;
    let changed = transaction.execute(
        "UPDATE agent_profiles
         SET lifecycle_state=?1,updated_at_ms=?2,revision=?3
         WHERE profile_id=?4 AND revision=?5 AND lifecycle_state=?6",
        params![
            encode_lifecycle(AgentProfileLifecycleState::Retired),
            completed_at.get(),
            next_revision.get(),
            profile.id().into_uuid().as_bytes(),
            profile.revision().get(),
            encode_lifecycle(AgentProfileLifecycleState::Disabled),
        ],
    )?;
    if changed != 1 {
        return Err(AgentProfileError::Corrupt(
            "Profile deletion retirement compare-and-set",
        ));
    }
    Ok(())
}

fn mark_manifest_completed(
    transaction: &Transaction<'_>,
    request_id: Uuid,
    completed_at: TimestampMillis,
) -> Result<(), AgentProfileError> {
    let changed = transaction.execute(
        "UPDATE agent_profile_deletion_manifests SET completed_at_ms=?1
         WHERE request_id=?2 AND completed_at_ms IS NULL",
        params![completed_at.get(), request_id.as_bytes()],
    )?;
    if changed != 1 {
        return Err(AgentProfileError::Corrupt(
            "Profile deletion manifest completion compare-and-set",
        ));
    }
    Ok(())
}
