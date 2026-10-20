//! Durable checkpoints and provider-deletion effects for exact Profile manifests.
//!
//! Sessions owns the frozen Runtime inventory. Operations stores one immutable
//! manifest checkpoint, one row per linked provider conversation, and trusted
//! host receipts. A local missing tab/page is never a deletion receipt.

use forge_domain::{TimestampMillis, WorkspaceId};
use heterarchy_orchestration_sessions::{
    AgentProfileDeletionIntent, AgentProfileDeletionManifest, AgentProfileDeletionRuntimeEntry,
    AgentProfileDeletionRuntimeTarget, AgentProfileId, AgentProfileProviderFamily,
    AgentProfileRevision, AgentProfileRuntimeSessionId, AgentProfileRuntimeSessionRevision,
    AgentProfileRuntimeSessionState,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    AgentProfileDeletionBarrier, AgentProfileDeletionInFlightCounts, AgentProvisionEffectKind,
    AgentProvisionProviderFamily, AgentProvisionState, ChatGptProjectId, OperationsCatalog,
    OperationsError, ProviderMessageState, ProviderMessageTarget, WorkspaceParticipationId,
    WorkspaceProjectBindingRevision, WorkspaceProjectReference, profile_deletion, wire,
};

/// Maximum rows returned by one reference or target read.
pub const MAX_AGENT_PROFILE_DELETION_PAGE_SIZE: u32 = 32;
const MAX_PROVIDER_ID_BYTES: usize = 512;
const MIN_PROVIDER_DELETION_RETRY_MS: i64 = 30_000;
const RECEIPT_NOT_STARTED_BEFORE_CLICK: i64 = 1;
const RECEIPT_POSITIVE_NO_EFFECT_READBACK: i64 = 2;
const RECEIPT_PROVIDER_DELETE_CONFIRMED: i64 = 3;
const PROFILE_PROVIDER_DELETION_OPERATION_DOMAIN: &[u8] =
    b"heterarchy.orchestration.agent-profile.provider-deletion.operation.v1\0";

const PROFILE_DELETION_PROGRESS_SQL: &str = r"
CREATE TABLE ops_agent_profile_deletion_progress (
    request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)),
    profile_id BLOB NOT NULL UNIQUE CHECK(length(profile_id)=16 AND profile_id != zeroblob(16)),
    expected_profile_revision INTEGER NOT NULL CHECK(expected_profile_revision>0),
    barrier_profile_revision INTEGER NOT NULL CHECK(barrier_profile_revision>0),
    requested_at_ms INTEGER NOT NULL CHECK(requested_at_ms>=0),
    phase INTEGER NOT NULL CHECK(phase BETWEEN 1 AND 7),
    deferred_reason INTEGER CHECK(deferred_reason IS NULL OR deferred_reason BETWEEN 1 AND 3),
    revision INTEGER NOT NULL CHECK(revision>0),
    after_provision_operation_id BLOB CHECK(after_provision_operation_id IS NULL OR length(after_provision_operation_id)=16),
    after_message_id BLOB CHECK(after_message_id IS NULL OR length(after_message_id)=16),
    after_runtime_id BLOB CHECK(after_runtime_id IS NULL OR length(after_runtime_id)=16),
    profile_revision_at_freeze INTEGER CHECK(profile_revision_at_freeze IS NULL OR profile_revision_at_freeze>0),
    frozen_at_ms INTEGER CHECK(frozen_at_ms IS NULL OR frozen_at_ms>=requested_at_ms),
    runtime_entry_count INTEGER CHECK(runtime_entry_count IS NULL OR runtime_entry_count>=0),
    linked_target_count INTEGER CHECK(linked_target_count IS NULL OR linked_target_count>=0),
    no_linked_target_count INTEGER CHECK(no_linked_target_count IS NULL OR no_linked_target_count>=0),
    completed_at_ms INTEGER CHECK(completed_at_ms IS NULL OR completed_at_ms>=frozen_at_ms),
    updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms>=requested_at_ms),
    FOREIGN KEY(request_id) REFERENCES ops_agent_profile_deletion_barriers(request_id),
    CHECK(
        (profile_revision_at_freeze IS NULL AND frozen_at_ms IS NULL
         AND runtime_entry_count IS NULL AND linked_target_count IS NULL
         AND no_linked_target_count IS NULL AND completed_at_ms IS NULL)
        OR
        (profile_revision_at_freeze IS NOT NULL AND frozen_at_ms IS NOT NULL
         AND runtime_entry_count IS NOT NULL AND linked_target_count IS NOT NULL
         AND no_linked_target_count IS NOT NULL
         AND runtime_entry_count=linked_target_count+no_linked_target_count)
    ),
    CHECK((phase BETWEEN 1 AND 3 AND profile_revision_at_freeze IS NULL)
       OR (phase BETWEEN 4 AND 7 AND profile_revision_at_freeze IS NOT NULL)),
    CHECK((phase=7 AND completed_at_ms IS NOT NULL AND deferred_reason IS NULL)
       OR (phase!=7 AND completed_at_ms IS NULL)),
    CHECK((phase=1 AND after_message_id IS NULL AND after_runtime_id IS NULL)
       OR (phase=2 AND after_provision_operation_id IS NULL AND after_runtime_id IS NULL)
       OR (phase=3 AND after_provision_operation_id IS NULL AND after_message_id IS NULL AND after_runtime_id IS NULL)
       OR (phase=4 AND after_provision_operation_id IS NULL AND after_message_id IS NULL)
       OR (phase IN(5,6,7) AND after_provision_operation_id IS NULL AND after_message_id IS NULL AND after_runtime_id IS NULL))
);
";

const PROFILE_PROVIDER_DELETIONS_SQL: &str = r"
CREATE TABLE ops_agent_profile_provider_deletions (
    operation_id BLOB PRIMARY KEY NOT NULL CHECK(length(operation_id)=16 AND operation_id != zeroblob(16)),
    request_id BLOB NOT NULL CHECK(length(request_id)=16 AND request_id != zeroblob(16)),
    profile_id BLOB NOT NULL CHECK(length(profile_id)=16 AND profile_id != zeroblob(16)),
    runtime_session_id BLOB NOT NULL CHECK(length(runtime_session_id)=16 AND runtime_session_id != zeroblob(16)),
    provider_family INTEGER NOT NULL CHECK(provider_family=1),
    provider_conversation_id TEXT NOT NULL CHECK(length(CAST(provider_conversation_id AS BLOB)) BETWEEN 1 AND 512),
    original_runtime_revision INTEGER NOT NULL CHECK(original_runtime_revision>0),
    original_runtime_state INTEGER NOT NULL CHECK(original_runtime_state BETWEEN 1 AND 8),
    state INTEGER NOT NULL CHECK(state BETWEEN 1 AND 5),
    attempt_count INTEGER NOT NULL CHECK(attempt_count>=0),
    deferred_reason INTEGER CHECK(deferred_reason IS NULL OR deferred_reason BETWEEN 1 AND 4),
    retry_after_ms INTEGER CHECK(retry_after_ms IS NULL OR retry_after_ms>=0),
    prepared_at_ms INTEGER NOT NULL CHECK(prepared_at_ms>=0),
    updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms>=prepared_at_ms),
    revision INTEGER NOT NULL CHECK(revision>0),
    UNIQUE(request_id,runtime_session_id),
    FOREIGN KEY(request_id) REFERENCES ops_agent_profile_deletion_progress(request_id),
    CHECK((state=4 AND deferred_reason IS NOT NULL AND retry_after_ms IS NOT NULL)
       OR (state!=4 AND deferred_reason IS NULL AND retry_after_ms IS NULL)),
    CHECK((state=1 AND attempt_count=0)
       OR (state IN(2,3,4,5) AND attempt_count>=1))
);
";

const PROFILE_PROVIDER_DELETION_RECEIPTS_SQL: &str = r"
CREATE TABLE ops_agent_profile_provider_deletion_receipts (
    operation_id BLOB NOT NULL CHECK(length(operation_id)=16 AND operation_id != zeroblob(16)),
    attempt_number INTEGER NOT NULL CHECK(attempt_number>=1),
    dispatch_revision INTEGER NOT NULL CHECK(dispatch_revision>0),
    dispatch_action INTEGER NOT NULL CHECK(dispatch_action IN(1,2)),
    outcome_kind INTEGER NOT NULL CHECK(outcome_kind BETWEEN 1 AND 3),
    deferred_reason INTEGER CHECK(deferred_reason IS NULL OR deferred_reason BETWEEN 1 AND 4),
    retry_after_ms INTEGER CHECK(retry_after_ms IS NULL OR retry_after_ms>=0),
    browser_session_id TEXT CHECK(browser_session_id IS NULL OR length(CAST(browser_session_id AS BLOB)) BETWEEN 1 AND 512),
    tab_id TEXT CHECK(tab_id IS NULL OR length(CAST(tab_id AS BLOB)) BETWEEN 1 AND 512),
    document_id TEXT CHECK(document_id IS NULL OR length(CAST(document_id AS BLOB)) BETWEEN 1 AND 512),
    observed_at_ms INTEGER NOT NULL CHECK(observed_at_ms>=0),
    PRIMARY KEY(operation_id,attempt_number),
    FOREIGN KEY(operation_id) REFERENCES ops_agent_profile_provider_deletions(operation_id),
    CHECK((outcome_kind IN(1,2) AND deferred_reason IS NOT NULL AND retry_after_ms IS NOT NULL)
       OR (outcome_kind=3 AND deferred_reason IS NULL AND retry_after_ms IS NULL)),
    CHECK(document_id IS NULL OR tab_id IS NOT NULL),
    CHECK(tab_id IS NULL OR browser_session_id IS NOT NULL)
);
";

const PROFILE_PROVIDER_DELETION_WORK_INDEX: &str = "CREATE INDEX ops_agent_profile_provider_deletions_work_idx ON ops_agent_profile_provider_deletions(state,retry_after_ms,prepared_at_ms,operation_id)";
const PROVISION_PROFILE_REFERENCE_INDEX: &str = "CREATE INDEX ops_agent_provision_profile_reference_idx ON ops_agent_provision_operations(profile_id,operation_id)";
const PROVISION_PROFILE_RUNTIME_REFERENCE_INDEX: &str = "CREATE INDEX ops_agent_provision_profile_runtime_reference_idx ON ops_agent_provision_operations(profile_id,runtime_session_id,operation_id)";
const MESSAGE_PROFILE_REFERENCE_INDEX: &str = "CREATE INDEX ops_provider_message_profile_reference_idx ON ops_provider_message_outbox(recipient_profile_id,message_id)";
const MESSAGE_PROFILE_RUNTIME_REFERENCE_INDEX: &str = "CREATE INDEX ops_provider_message_profile_runtime_reference_idx ON ops_provider_message_outbox(recipient_profile_id,runtime_session_id,message_id)";

/// Durable coordinator phase for one exact deletion request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProfileDeletionProgressPhase {
    AuditProvision,
    AuditMessages,
    FreezeTargets,
    PrepareDeletes,
    DeleteTargets,
    Finalize,
    Completed,
}

/// Immutable Sessions manifest facts copied into the Operations checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentProfileDeletionFreeze {
    profile_revision_at_freeze: AgentProfileRevision,
    frozen_at: TimestampMillis,
    runtime_entry_count: u64,
    linked_target_count: u64,
    no_linked_target_count: u64,
    completed_at: Option<TimestampMillis>,
}

impl AgentProfileDeletionFreeze {
    #[must_use]
    pub const fn profile_revision_at_freeze(self) -> AgentProfileRevision {
        self.profile_revision_at_freeze
    }

    #[must_use]
    pub const fn frozen_at(self) -> TimestampMillis {
        self.frozen_at
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

    #[must_use]
    pub const fn completed_at(self) -> Option<TimestampMillis> {
        self.completed_at
    }
}

/// Small actionable reason for a deferred cross-catalog coordinator step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProfileDeletionProgressDeferredReason {
    HistoryMismatch,
    CatalogUnavailable,
    ManifestMismatch,
}

/// Bounded durable cursor and exact barrier identity for one deletion request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileDeletionProgress {
    request_id: Uuid,
    profile_id: AgentProfileId,
    expected_profile_revision: AgentProfileRevision,
    barrier_profile_revision: AgentProfileRevision,
    requested_at: TimestampMillis,
    phase: AgentProfileDeletionProgressPhase,
    deferred_reason: Option<AgentProfileDeletionProgressDeferredReason>,
    revision: u64,
    after_provision_operation_id: Option<Uuid>,
    after_message_id: Option<Uuid>,
    after_runtime_id: Option<AgentProfileRuntimeSessionId>,
    freeze: Option<AgentProfileDeletionFreeze>,
    updated_at: TimestampMillis,
}

impl AgentProfileDeletionProgress {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn profile_id(&self) -> AgentProfileId {
        self.profile_id
    }

    #[must_use]
    pub const fn expected_profile_revision(&self) -> AgentProfileRevision {
        self.expected_profile_revision
    }

    #[must_use]
    pub const fn barrier_profile_revision(&self) -> AgentProfileRevision {
        self.barrier_profile_revision
    }

    #[must_use]
    pub const fn requested_at(&self) -> TimestampMillis {
        self.requested_at
    }

    #[must_use]
    pub const fn phase(&self) -> AgentProfileDeletionProgressPhase {
        self.phase
    }

    #[must_use]
    pub const fn deferred_reason(&self) -> Option<AgentProfileDeletionProgressDeferredReason> {
        self.deferred_reason
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub const fn after_provision_operation_id(&self) -> Option<Uuid> {
        self.after_provision_operation_id
    }

    #[must_use]
    pub const fn after_message_id(&self) -> Option<Uuid> {
        self.after_message_id
    }

    #[must_use]
    pub const fn after_runtime_id(&self) -> Option<AgentProfileRuntimeSessionId> {
        self.after_runtime_id
    }

    #[must_use]
    pub const fn freeze(&self) -> Option<AgentProfileDeletionFreeze> {
        self.freeze
    }

    #[must_use]
    pub const fn updated_at(&self) -> TimestampMillis {
        self.updated_at
    }
}

/// One typed bounded step in the request-scoped deletion coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProfileDeletionProgressAdvance {
    ProvisionReferencesPage {
        after_operation_id: Option<Uuid>,
    },
    ProviderMessageReferencesPage {
        after_message_id: Option<Uuid>,
    },
    TargetsFrozen(AgentProfileDeletionManifest),
    RuntimeEntriesPage {
        after_runtime_id: Option<AgentProfileRuntimeSessionId>,
    },
    ProviderTargetsSettled,
    ProfileFinished(AgentProfileDeletionManifest),
}

/// One scoped provision reference, including terminal and suppressed rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileDeletionProvisionReference {
    operation_id: Uuid,
    request_id: Uuid,
    slot_ordinal: u8,
    effect_kind: AgentProvisionEffectKind,
    provider_family: AgentProvisionProviderFamily,
    profile_id: AgentProfileId,
    runtime_session_id: Option<AgentProfileRuntimeSessionId>,
    state: AgentProvisionState,
    provider_conversation_id: Option<String>,
    project: Option<WorkspaceProjectReference>,
    prepared_at: TimestampMillis,
    updated_at: TimestampMillis,
    revision: u64,
}

impl AgentProfileDeletionProvisionReference {
    #[must_use]
    pub const fn operation_id(&self) -> Uuid {
        self.operation_id
    }
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }
    #[must_use]
    pub const fn slot_ordinal(&self) -> u8 {
        self.slot_ordinal
    }
    #[must_use]
    pub const fn effect_kind(&self) -> AgentProvisionEffectKind {
        self.effect_kind
    }
    #[must_use]
    pub const fn provider_family(&self) -> AgentProvisionProviderFamily {
        self.provider_family
    }
    #[must_use]
    pub const fn profile_id(&self) -> AgentProfileId {
        self.profile_id
    }
    #[must_use]
    pub const fn runtime_session_id(&self) -> Option<AgentProfileRuntimeSessionId> {
        self.runtime_session_id
    }
    #[must_use]
    pub const fn state(&self) -> AgentProvisionState {
        self.state
    }
    #[must_use]
    pub fn provider_conversation_id(&self) -> Option<&str> {
        self.provider_conversation_id.as_deref()
    }
    #[must_use]
    pub const fn project(&self) -> Option<WorkspaceProjectReference> {
        self.project
    }
    #[must_use]
    pub const fn prepared_at(&self) -> TimestampMillis {
        self.prepared_at
    }
    #[must_use]
    pub const fn updated_at(&self) -> TimestampMillis {
        self.updated_at
    }
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
}

/// One ascending bounded provision-reference page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileDeletionProvisionReferencePage {
    references: Vec<AgentProfileDeletionProvisionReference>,
    next_operation_id: Option<Uuid>,
}

impl AgentProfileDeletionProvisionReferencePage {
    #[must_use]
    pub fn references(&self) -> &[AgentProfileDeletionProvisionReference] {
        &self.references
    }
    #[must_use]
    pub const fn next_operation_id(&self) -> Option<Uuid> {
        self.next_operation_id
    }
}

/// One scoped provider-message outbox reference; message content is excluded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileDeletionProviderMessageReference {
    message_id: Uuid,
    target: ProviderMessageTarget,
    state: ProviderMessageState,
    queued_at: TimestampMillis,
    updated_at: TimestampMillis,
    revision: u64,
}

impl AgentProfileDeletionProviderMessageReference {
    #[must_use]
    pub const fn message_id(&self) -> Uuid {
        self.message_id
    }
    #[must_use]
    pub fn target(&self) -> &ProviderMessageTarget {
        &self.target
    }
    #[must_use]
    pub const fn state(&self) -> ProviderMessageState {
        self.state
    }
    #[must_use]
    pub const fn queued_at(&self) -> TimestampMillis {
        self.queued_at
    }
    #[must_use]
    pub const fn updated_at(&self) -> TimestampMillis {
        self.updated_at
    }
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
}

/// Complete indexed reference counts for one frozen Runtime identity.
/// The host may classify a Sessions `NoLinkedTarget` only when the provision
/// total is nonzero, every row is Prepared/Failed with no conversation ID, and
/// the message total is zero. These counts do not claim provider absence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentProfileDeletionRuntimeReferenceSummary {
    request_id: Uuid,
    profile_id: AgentProfileId,
    runtime_session_id: AgentProfileRuntimeSessionId,
    provision_reference_count: u64,
    suppressed_or_failed_unlinked_provision_count: u64,
    provider_message_reference_count: u64,
}

impl AgentProfileDeletionRuntimeReferenceSummary {
    #[must_use]
    pub const fn request_id(self) -> Uuid {
        self.request_id
    }
    #[must_use]
    pub const fn profile_id(self) -> AgentProfileId {
        self.profile_id
    }
    #[must_use]
    pub const fn runtime_session_id(self) -> AgentProfileRuntimeSessionId {
        self.runtime_session_id
    }
    #[must_use]
    pub const fn provision_reference_count(self) -> u64 {
        self.provision_reference_count
    }
    #[must_use]
    pub const fn suppressed_or_failed_unlinked_provision_count(self) -> u64 {
        self.suppressed_or_failed_unlinked_provision_count
    }
    #[must_use]
    pub const fn provider_message_reference_count(self) -> u64 {
        self.provider_message_reference_count
    }
    #[must_use]
    pub const fn has_other_provision_reference(self) -> bool {
        self.provision_reference_count != self.suppressed_or_failed_unlinked_provision_count
    }
}

/// One ascending bounded provider-message reference page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileDeletionProviderMessageReferencePage {
    references: Vec<AgentProfileDeletionProviderMessageReference>,
    next_message_id: Option<Uuid>,
}

impl AgentProfileDeletionProviderMessageReferencePage {
    #[must_use]
    pub fn references(&self) -> &[AgentProfileDeletionProviderMessageReference] {
        &self.references
    }
    #[must_use]
    pub const fn next_message_id(&self) -> Option<Uuid> {
        self.next_message_id
    }
}

/// Durable provider-deletion effect state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProfileProviderDeletionState {
    Prepared,
    EffectPending,
    EffectUnknown,
    Deferred,
    Deleted,
}

/// Closed reasons for a trusted host receipt that proves no delete was started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProfileProviderDeletionDeferredReason {
    ProviderBusy,
    AuthenticationRequired,
    SurfaceUnavailable,
    ActionUnavailable,
}

/// Host action returned for one exact delete target and attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProfileProviderDeletionAction {
    Execute,
    Inspect,
}

/// Immutable target and current durable state for one provider deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileProviderDeletionOperation {
    operation_id: Uuid,
    request_id: Uuid,
    profile_id: AgentProfileId,
    runtime_session_id: AgentProfileRuntimeSessionId,
    provider_family: AgentProfileProviderFamily,
    provider_conversation_id: String,
    original_runtime_revision: AgentProfileRuntimeSessionRevision,
    original_runtime_state: AgentProfileRuntimeSessionState,
    state: AgentProfileProviderDeletionState,
    attempt_number: u64,
    deferred_reason: Option<AgentProfileProviderDeletionDeferredReason>,
    last_deferred_reason: Option<AgentProfileProviderDeletionDeferredReason>,
    retry_after: Option<TimestampMillis>,
    prepared_at: TimestampMillis,
    updated_at: TimestampMillis,
    revision: u64,
}

impl AgentProfileProviderDeletionOperation {
    #[must_use]
    pub const fn operation_id(&self) -> Uuid {
        self.operation_id
    }
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
    pub fn provider_conversation_id(&self) -> &str {
        &self.provider_conversation_id
    }
    #[must_use]
    pub const fn original_runtime_revision(&self) -> AgentProfileRuntimeSessionRevision {
        self.original_runtime_revision
    }
    #[must_use]
    pub const fn original_runtime_state(&self) -> AgentProfileRuntimeSessionState {
        self.original_runtime_state
    }
    #[must_use]
    pub const fn state(&self) -> AgentProfileProviderDeletionState {
        self.state
    }
    #[must_use]
    pub const fn attempt_number(&self) -> u64 {
        self.attempt_number
    }
    #[must_use]
    pub const fn deferred_reason(&self) -> Option<AgentProfileProviderDeletionDeferredReason> {
        self.deferred_reason
    }
    #[must_use]
    pub const fn last_deferred_reason(&self) -> Option<AgentProfileProviderDeletionDeferredReason> {
        self.last_deferred_reason
    }
    #[must_use]
    pub const fn retry_after(&self) -> Option<TimestampMillis> {
        self.retry_after
    }
    #[must_use]
    pub const fn prepared_at(&self) -> TimestampMillis {
        self.prepared_at
    }
    #[must_use]
    pub const fn updated_at(&self) -> TimestampMillis {
        self.updated_at
    }
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
}

/// Result of idempotently preparing one exact frozen provider target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileProviderDeletionPrepareOutcome {
    operation: AgentProfileProviderDeletionOperation,
    created: bool,
}

impl AgentProfileProviderDeletionPrepareOutcome {
    #[must_use]
    pub fn operation(&self) -> &AgentProfileProviderDeletionOperation {
        &self.operation
    }
    #[must_use]
    pub const fn created(&self) -> bool {
        self.created
    }
}

/// Exact provider deletion action fence; no prompt/body or credential is carried.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileProviderDeletionDispatch {
    operation_id: Uuid,
    request_id: Uuid,
    profile_id: AgentProfileId,
    runtime_session_id: AgentProfileRuntimeSessionId,
    provider_family: AgentProfileProviderFamily,
    provider_conversation_id: String,
    attempt_number: u64,
    revision: u64,
    action: AgentProfileProviderDeletionAction,
}

impl AgentProfileProviderDeletionDispatch {
    #[must_use]
    pub const fn operation_id(&self) -> Uuid {
        self.operation_id
    }
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
    pub fn provider_conversation_id(&self) -> &str {
        &self.provider_conversation_id
    }
    #[must_use]
    pub const fn attempt_number(&self) -> u64 {
        self.attempt_number
    }
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
    #[must_use]
    pub const fn action(&self) -> AgentProfileProviderDeletionAction {
        self.action
    }
}

/// One target, attempt and bounded browser-observation outcome from the trusted host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileProviderDeletionReceipt {
    dispatch: AgentProfileProviderDeletionDispatch,
    outcome: AgentProfileProviderDeletionReceiptOutcome,
    browser_session_id: Option<String>,
    tab_id: Option<String>,
    document_id: Option<String>,
    observed_at: TimestampMillis,
}

/// A delete receipt must come from positive provider confirmation. No local
/// browser-page absence can construct the `Deleted` outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentProfileProviderDeletionReceiptOutcome {
    NotStartedBeforeClick {
        reason: AgentProfileProviderDeletionDeferredReason,
        retry_after: TimestampMillis,
    },
    PositiveNoEffectReadback {
        reason: AgentProfileProviderDeletionDeferredReason,
        retry_after: TimestampMillis,
    },
    ProviderDeleteConfirmed,
}

impl AgentProfileProviderDeletionReceipt {
    /// Record that the trusted host did not start the provider deletion.
    ///
    /// # Errors
    /// Rejects a dispatch other than `Execute`, a retry time before the minimum backoff,
    /// or invalid browser identity metadata/fence shape.
    #[allow(clippy::too_many_arguments)]
    pub fn not_started_before_click(
        dispatch: &AgentProfileProviderDeletionDispatch,
        reason: AgentProfileProviderDeletionDeferredReason,
        retry_after: TimestampMillis,
        browser_session_id: Option<String>,
        tab_id: Option<String>,
        document_id: Option<String>,
        observed_at: TimestampMillis,
    ) -> Result<Self, OperationsError> {
        if dispatch.action() != AgentProfileProviderDeletionAction::Execute {
            return Err(OperationsError::Conflict(
                "provider deletion Execute receipt",
            ));
        }
        Self::new(
            dispatch,
            AgentProfileProviderDeletionReceiptOutcome::NotStartedBeforeClick {
                reason,
                retry_after,
            },
            browser_session_id,
            tab_id,
            document_id,
            observed_at,
        )
    }

    /// Record an `Inspect` readback that positively establishes no delete effect.
    ///
    /// # Errors
    /// Rejects a dispatch other than `Inspect`, a retry time before the minimum backoff,
    /// or a missing or malformed exact browser target.
    #[allow(clippy::too_many_arguments)]
    pub fn confirmed_no_effect_after_readback(
        dispatch: &AgentProfileProviderDeletionDispatch,
        reason: AgentProfileProviderDeletionDeferredReason,
        retry_after: TimestampMillis,
        browser_session_id: Option<String>,
        tab_id: Option<String>,
        document_id: Option<String>,
        observed_at: TimestampMillis,
    ) -> Result<Self, OperationsError> {
        if dispatch.action() != AgentProfileProviderDeletionAction::Inspect {
            return Err(OperationsError::Conflict(
                "provider deletion Inspect receipt",
            ));
        }
        Self::new(
            dispatch,
            AgentProfileProviderDeletionReceiptOutcome::PositiveNoEffectReadback {
                reason,
                retry_after,
            },
            browser_session_id,
            tab_id,
            document_id,
            observed_at,
        )
    }

    /// Record positive provider confirmation for the exact deletion target.
    ///
    /// # Errors
    /// Rejects malformed browser identity metadata or a missing exact browser
    /// session, tab, or document identity.
    #[allow(clippy::too_many_arguments)]
    pub fn deleted_after_positive_provider_confirmation(
        dispatch: &AgentProfileProviderDeletionDispatch,
        browser_session_id: Option<String>,
        tab_id: Option<String>,
        document_id: Option<String>,
        observed_at: TimestampMillis,
    ) -> Result<Self, OperationsError> {
        Self::new(
            dispatch,
            AgentProfileProviderDeletionReceiptOutcome::ProviderDeleteConfirmed,
            browser_session_id,
            tab_id,
            document_id,
            observed_at,
        )
    }

    fn new(
        dispatch: &AgentProfileProviderDeletionDispatch,
        outcome: AgentProfileProviderDeletionReceiptOutcome,
        browser_session_id: Option<String>,
        tab_id: Option<String>,
        document_id: Option<String>,
        observed_at: TimestampMillis,
    ) -> Result<Self, OperationsError> {
        validate_optional_browser_identity(browser_session_id.as_deref(), "browser session")?;
        validate_optional_browser_identity(tab_id.as_deref(), "browser tab")?;
        validate_optional_browser_identity(document_id.as_deref(), "browser document")?;
        if document_id.is_some() && tab_id.is_none()
            || tab_id.is_some() && browser_session_id.is_none()
        {
            return Err(OperationsError::Conflict(
                "provider deletion browser fence shape",
            ));
        }
        match &outcome {
            AgentProfileProviderDeletionReceiptOutcome::NotStartedBeforeClick {
                retry_after,
                ..
            } => {
                let earliest = observed_at
                    .get()
                    .checked_add(MIN_PROVIDER_DELETION_RETRY_MS)
                    .ok_or(OperationsError::Bounds("provider deletion retry timestamp"))?;
                if retry_after.get() < earliest {
                    return Err(OperationsError::Conflict("provider deletion retry backoff"));
                }
            }
            AgentProfileProviderDeletionReceiptOutcome::PositiveNoEffectReadback {
                retry_after,
                ..
            } => {
                let earliest = observed_at
                    .get()
                    .checked_add(MIN_PROVIDER_DELETION_RETRY_MS)
                    .ok_or(OperationsError::Bounds("provider deletion retry timestamp"))?;
                if retry_after.get() < earliest
                    || browser_session_id.is_none()
                    || tab_id.is_none()
                    || document_id.is_none()
                {
                    return Err(OperationsError::Conflict(
                        "provider deletion no-effect receipt lacks exact browser proof",
                    ));
                }
            }
            AgentProfileProviderDeletionReceiptOutcome::ProviderDeleteConfirmed => {
                if browser_session_id.is_none() || tab_id.is_none() || document_id.is_none() {
                    return Err(OperationsError::Conflict(
                        "provider deletion confirmation lacks exact browser target",
                    ));
                }
            }
        }
        Ok(Self {
            dispatch: dispatch.clone(),
            outcome,
            browser_session_id,
            tab_id,
            document_id,
            observed_at,
        })
    }

    #[must_use]
    pub fn dispatch(&self) -> &AgentProfileProviderDeletionDispatch {
        &self.dispatch
    }
    #[must_use]
    pub fn outcome(&self) -> &AgentProfileProviderDeletionReceiptOutcome {
        &self.outcome
    }
    #[must_use]
    pub fn browser_session_id(&self) -> Option<&str> {
        self.browser_session_id.as_deref()
    }
    #[must_use]
    pub fn tab_id(&self) -> Option<&str> {
        self.tab_id.as_deref()
    }
    #[must_use]
    pub fn document_id(&self) -> Option<&str> {
        self.document_id.as_deref()
    }
    #[must_use]
    pub const fn observed_at(&self) -> TimestampMillis {
        self.observed_at
    }
}

/// One ascending bounded provider-deletion target page for a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileProviderDeletionPage {
    operations: Vec<AgentProfileProviderDeletionOperation>,
    next_runtime_id: Option<AgentProfileRuntimeSessionId>,
}

impl AgentProfileProviderDeletionPage {
    #[must_use]
    pub fn operations(&self) -> &[AgentProfileProviderDeletionOperation] {
        &self.operations
    }
    #[must_use]
    pub const fn next_runtime_id(&self) -> Option<AgentProfileRuntimeSessionId> {
        self.next_runtime_id
    }
}

impl OperationsCatalog {
    /// Pure observation of the globally single pending/unknown delete operation.
    ///
    /// # Errors
    /// Rejects corrupt ambiguity and catalog read failures.
    pub fn agent_profile_provider_deletion_in_flight(
        &self,
    ) -> Result<Option<AgentProfileProviderDeletionOperation>, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        let mut ids = in_flight_provider_deletion_ids(&tx)?;
        if ids.len() > 1 {
            return Err(OperationsError::Corrupt(
                "multiple in-flight provider deletion observations",
            ));
        }
        let operation = ids
            .pop()
            .map(|id| {
                load_provider_deletion(&tx, id)?.ok_or(OperationsError::Corrupt(
                    "provider deletion observation disappeared",
                ))
            })
            .transpose()?;
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(operation)
    }

    /// Start or read back the durable coordinator checkpoint for one exact intent.
    ///
    /// # Errors
    /// Rejects missing/changed barriers, conflicting request replays, or SQLite failures.
    pub fn begin_agent_profile_deletion_progress(
        &mut self,
        intent: &AgentProfileDeletionIntent,
    ) -> Result<AgentProfileDeletionProgress, OperationsError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let barrier = profile_deletion::load_barrier_by_request(&tx, intent.request_id())?.ok_or(
            OperationsError::Conflict("Agent Profile deletion barrier missing"),
        )?;
        if !profile_deletion::barrier_matches_intent(&barrier, intent) {
            return Err(OperationsError::Conflict(
                "Agent Profile deletion barrier changed",
            ));
        }
        if let Some(existing) = load_progress(&tx, intent.request_id())? {
            if !progress_matches_intent(&existing, intent) {
                return Err(OperationsError::Conflict(
                    "Agent Profile deletion progress replay",
                ));
            }
            tx.commit()?;
            return Ok(existing);
        }
        tx.execute(
            "INSERT INTO ops_agent_profile_deletion_progress(
                request_id,profile_id,expected_profile_revision,barrier_profile_revision,
                requested_at_ms,phase,deferred_reason,revision,after_provision_operation_id,
                after_message_id,after_runtime_id,profile_revision_at_freeze,frozen_at_ms,
                runtime_entry_count,linked_target_count,no_linked_target_count,completed_at_ms,
                updated_at_ms
             ) VALUES(?1,?2,?3,?4,?5,1,NULL,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,?5)",
            params![
                intent.request_id().as_bytes(),
                intent.profile_id().into_uuid().as_bytes(),
                intent.expected_profile_revision().get(),
                intent.barrier_profile_revision().get(),
                intent.requested_at().get(),
            ],
        )?;
        let progress = load_progress(&tx, intent.request_id())?
            .ok_or(OperationsError::Corrupt("Profile deletion progress insert"))?;
        tx.commit().map_err(|source| {
            OperationsError::AgentProfileDeletionProgressCommitUnconfirmed {
                request_id: intent.request_id(),
                source,
            }
        })?;
        Ok(progress)
    }

    /// Read the durable request-scoped reconciliation checkpoint.
    ///
    /// # Errors
    /// Rejects nil identity, malformed stored state, or SQLite failures.
    pub fn agent_profile_deletion_progress(
        &self,
        request_id: Uuid,
    ) -> Result<Option<AgentProfileDeletionProgress>, OperationsError> {
        validate_uuid(request_id, "Profile deletion request identity")?;
        load_progress(&self.connection, request_id)
    }

    /// Record one bounded coordinator page or phase transition using a revision CAS.
    ///
    /// `None` cursors finish their current page phase and advance to the next
    /// phase. The final Runtime Session page is accepted only when the prepared
    /// target count matches the frozen linked-target count.
    ///
    /// # Errors
    /// Rejects stale revisions/phases, changed barriers, non-monotonic cursors,
    /// incomplete target preparation, unsettled effects, or SQLite failures.
    pub fn advance_agent_profile_deletion_progress(
        &mut self,
        request_id: Uuid,
        expected_revision: u64,
        observed_at: TimestampMillis,
        advance: AgentProfileDeletionProgressAdvance,
    ) -> Result<AgentProfileDeletionProgress, OperationsError> {
        validate_uuid(request_id, "Profile deletion request identity")?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = load_progress(&tx, request_id)?.ok_or(OperationsError::Conflict(
            "Profile deletion progress missing",
        ))?;
        validate_progress_revision(&existing, expected_revision, observed_at)?;
        let barrier = profile_deletion::load_barrier_by_request(&tx, request_id)?.ok_or(
            OperationsError::Conflict("Agent Profile deletion barrier missing"),
        )?;
        if !barrier_matches_progress(&barrier, &existing) {
            return Err(OperationsError::Conflict(
                "Agent Profile deletion progress barrier changed",
            ));
        }
        let mut next = existing.clone();
        next.revision = next_progress_revision(existing.revision)?;
        next.updated_at = observed_at;
        next.deferred_reason = None;
        apply_progress_advance(&tx, &existing, &mut next, advance)?;
        update_progress(&tx, &existing, &next)?;
        let progress = load_progress(&tx, request_id)?
            .ok_or(OperationsError::Corrupt("Profile deletion progress update"))?;
        tx.commit().map_err(|source| {
            OperationsError::AgentProfileDeletionProgressCommitUnconfirmed { request_id, source }
        })?;
        Ok(progress)
    }

    /// Retain a concise coordinator reason without advancing its phase or cursor.
    /// Repeating the same reason is a readback and does not change revision/time.
    ///
    /// # Errors
    /// Rejects stale revisions, clock regression, completed requests, or SQLite failures.
    pub fn defer_agent_profile_deletion_progress(
        &mut self,
        request_id: Uuid,
        expected_revision: u64,
        reason: AgentProfileDeletionProgressDeferredReason,
        observed_at: TimestampMillis,
    ) -> Result<AgentProfileDeletionProgress, OperationsError> {
        validate_uuid(request_id, "Profile deletion request identity")?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = load_progress(&tx, request_id)?.ok_or(OperationsError::Conflict(
            "Profile deletion progress missing",
        ))?;
        if existing.revision != expected_revision || expected_revision == 0 {
            return Err(OperationsError::Conflict(
                "Profile deletion progress revision",
            ));
        }
        if existing.phase == AgentProfileDeletionProgressPhase::Completed {
            return Err(OperationsError::Conflict(
                "Profile deletion progress completed",
            ));
        }
        if existing.deferred_reason == Some(reason) {
            tx.commit()?;
            return Ok(existing);
        }
        if observed_at < existing.updated_at {
            return Err(OperationsError::Conflict("Profile deletion progress clock"));
        }
        let revision = next_progress_revision(existing.revision)?;
        let changed = tx.execute(
            "UPDATE ops_agent_profile_deletion_progress
             SET deferred_reason=?1,revision=?2,updated_at_ms=?3
             WHERE request_id=?4 AND revision=?5 AND phase=?6",
            params![
                encode_progress_deferred_reason(reason),
                signed_progress_revision(revision)?,
                observed_at.get(),
                request_id.as_bytes(),
                signed_progress_revision(expected_revision)?,
                encode_progress_phase(existing.phase),
            ],
        )?;
        if changed != 1 {
            return Err(OperationsError::Conflict("Profile deletion progress CAS"));
        }
        let progress = load_progress(&tx, request_id)?.ok_or(OperationsError::Corrupt(
            "Profile deletion progress defer readback",
        ))?;
        tx.commit().map_err(|source| {
            OperationsError::AgentProfileDeletionProgressCommitUnconfirmed { request_id, source }
        })?;
        Ok(progress)
    }
}

fn apply_progress_advance(
    transaction: &Transaction<'_>,
    existing: &AgentProfileDeletionProgress,
    next: &mut AgentProfileDeletionProgress,
    advance: AgentProfileDeletionProgressAdvance,
) -> Result<(), OperationsError> {
    match (existing.phase, advance) {
        (
            AgentProfileDeletionProgressPhase::AuditProvision,
            AgentProfileDeletionProgressAdvance::ProvisionReferencesPage { after_operation_id },
        ) => advance_provision_references_page(existing, next, after_operation_id),
        (
            AgentProfileDeletionProgressPhase::AuditMessages,
            AgentProfileDeletionProgressAdvance::ProviderMessageReferencesPage { after_message_id },
        ) => advance_provider_message_references_page(existing, next, after_message_id),
        (
            AgentProfileDeletionProgressPhase::FreezeTargets,
            AgentProfileDeletionProgressAdvance::TargetsFrozen(manifest),
        ) => advance_frozen_targets(transaction, existing, next, manifest),
        (
            AgentProfileDeletionProgressPhase::PrepareDeletes,
            AgentProfileDeletionProgressAdvance::RuntimeEntriesPage { after_runtime_id },
        ) => advance_runtime_entries_page(transaction, existing, next, after_runtime_id),
        (
            AgentProfileDeletionProgressPhase::DeleteTargets,
            AgentProfileDeletionProgressAdvance::ProviderTargetsSettled,
        ) => advance_settled_targets(transaction, existing, next),
        (
            AgentProfileDeletionProgressPhase::Finalize,
            AgentProfileDeletionProgressAdvance::ProfileFinished(manifest),
        ) => advance_finished_profile(transaction, existing, next, manifest),
        _ => Err(OperationsError::Conflict(
            "Profile deletion progress phase/advance mismatch",
        )),
    }
}

fn advance_provision_references_page(
    existing: &AgentProfileDeletionProgress,
    next: &mut AgentProfileDeletionProgress,
    after_operation_id: Option<Uuid>,
) -> Result<(), OperationsError> {
    require_monotonic_uuid_cursor(
        existing.after_provision_operation_id,
        after_operation_id,
        "provision reference cursor",
    )?;
    if after_operation_id.is_none() {
        next.phase = AgentProfileDeletionProgressPhase::AuditMessages;
    }
    next.after_provision_operation_id = after_operation_id;
    Ok(())
}

fn advance_provider_message_references_page(
    existing: &AgentProfileDeletionProgress,
    next: &mut AgentProfileDeletionProgress,
    after_message_id: Option<Uuid>,
) -> Result<(), OperationsError> {
    require_monotonic_uuid_cursor(
        existing.after_message_id,
        after_message_id,
        "provider message reference cursor",
    )?;
    if after_message_id.is_none() {
        next.phase = AgentProfileDeletionProgressPhase::FreezeTargets;
    }
    next.after_message_id = after_message_id;
    Ok(())
}

fn advance_frozen_targets(
    transaction: &Transaction<'_>,
    existing: &AgentProfileDeletionProgress,
    next: &mut AgentProfileDeletionProgress,
    manifest: AgentProfileDeletionManifest,
) -> Result<(), OperationsError> {
    let freeze = validate_manifest_for_freeze(existing, manifest)?;
    let counts = profile_deletion::profile_deletion_in_flight_counts_in_snapshot(
        transaction,
        existing.profile_id,
    )?;
    if has_in_flight_profile_effects(counts) {
        return Err(OperationsError::Conflict(
            "Profile provider effects remain in flight before freeze",
        ));
    }
    next.phase = AgentProfileDeletionProgressPhase::PrepareDeletes;
    next.freeze = Some(freeze);
    Ok(())
}

fn advance_runtime_entries_page(
    transaction: &Transaction<'_>,
    existing: &AgentProfileDeletionProgress,
    next: &mut AgentProfileDeletionProgress,
    after_runtime_id: Option<AgentProfileRuntimeSessionId>,
) -> Result<(), OperationsError> {
    require_monotonic_runtime_cursor(existing.after_runtime_id, after_runtime_id)?;
    if after_runtime_id.is_none() {
        let freeze = existing
            .freeze
            .ok_or(OperationsError::Corrupt("Profile deletion freeze missing"))?;
        let prepared_count = provider_deletion_count(transaction, existing.request_id)?;
        if prepared_count != freeze.linked_target_count {
            return Err(OperationsError::Conflict(
                "Profile deletion prepared target count mismatch",
            ));
        }
        next.phase = AgentProfileDeletionProgressPhase::DeleteTargets;
    }
    next.after_runtime_id = after_runtime_id;
    Ok(())
}

fn advance_settled_targets(
    transaction: &Transaction<'_>,
    existing: &AgentProfileDeletionProgress,
    next: &mut AgentProfileDeletionProgress,
) -> Result<(), OperationsError> {
    ensure_provider_targets_settled(transaction, existing)?;
    next.phase = AgentProfileDeletionProgressPhase::Finalize;
    Ok(())
}

fn advance_finished_profile(
    transaction: &Transaction<'_>,
    existing: &AgentProfileDeletionProgress,
    next: &mut AgentProfileDeletionProgress,
    manifest: AgentProfileDeletionManifest,
) -> Result<(), OperationsError> {
    let freeze = validate_manifest_finished(existing, manifest)?;
    ensure_provider_targets_settled(transaction, existing)?;
    next.phase = AgentProfileDeletionProgressPhase::Completed;
    next.freeze = Some(freeze);
    Ok(())
}

/// Current durable deletion-target counts for an exact request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentProfileProviderDeletionSummary {
    request_id: Uuid,
    profile_id: AgentProfileId,
    prepared: u64,
    effect_pending: u64,
    effect_unknown: u64,
    deferred: u64,
    deleted: u64,
    latest_deferred_reason: Option<AgentProfileProviderDeletionDeferredReason>,
}

impl AgentProfileProviderDeletionSummary {
    #[must_use]
    pub const fn request_id(self) -> Uuid {
        self.request_id
    }
    #[must_use]
    pub const fn profile_id(self) -> AgentProfileId {
        self.profile_id
    }
    #[must_use]
    pub const fn prepared(self) -> u64 {
        self.prepared
    }
    #[must_use]
    pub const fn effect_pending(self) -> u64 {
        self.effect_pending
    }
    #[must_use]
    pub const fn effect_unknown(self) -> u64 {
        self.effect_unknown
    }
    #[must_use]
    pub const fn deferred(self) -> u64 {
        self.deferred
    }
    #[must_use]
    pub const fn deleted(self) -> u64 {
        self.deleted
    }
    #[must_use]
    pub const fn latest_deferred_reason(
        self,
    ) -> Option<AgentProfileProviderDeletionDeferredReason> {
        self.latest_deferred_reason
    }
}

impl OperationsCatalog {
    /// Read complete provision/outbox reference counts for one exact Runtime
    /// in a single snapshot after verifying the immutable deletion barrier.
    ///
    /// # Errors
    /// Rejects a missing/changed barrier, malformed counters, or SQLite failures.
    pub fn profile_deletion_runtime_reference_summary(
        &self,
        intent: &AgentProfileDeletionIntent,
        runtime_session_id: AgentProfileRuntimeSessionId,
    ) -> Result<AgentProfileDeletionRuntimeReferenceSummary, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        let barrier = profile_deletion::load_barrier_by_request(&tx, intent.request_id())?.ok_or(
            OperationsError::Conflict("Agent Profile deletion barrier missing"),
        )?;
        if !profile_deletion::barrier_matches_intent(&barrier, intent) {
            return Err(OperationsError::Conflict(
                "Agent Profile deletion barrier changed",
            ));
        }
        let provision_counts: (i64, i64) = tx.query_row(
            "SELECT COUNT(*),
                    COALESCE(SUM(CASE
                        WHEN state IN(1,6) AND provider_conversation_id IS NULL THEN 1
                        ELSE 0 END),0)
             FROM ops_agent_provision_operations
             WHERE profile_id=?1 AND runtime_session_id=?2",
            params![
                intent.profile_id().into_uuid().as_bytes(),
                runtime_session_id.into_uuid().as_bytes(),
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let message_count: i64 = tx.query_row(
            "SELECT COUNT(*) FROM ops_provider_message_outbox
             WHERE recipient_profile_id=?1 AND runtime_session_id=?2",
            params![
                intent.profile_id().into_uuid().as_bytes(),
                runtime_session_id.into_uuid().as_bytes(),
            ],
            |row| row.get(0),
        )?;
        let summary = AgentProfileDeletionRuntimeReferenceSummary {
            request_id: intent.request_id(),
            profile_id: intent.profile_id(),
            runtime_session_id,
            provision_reference_count: counter(
                provision_counts.0,
                "Runtime provision reference count",
            )?,
            suppressed_or_failed_unlinked_provision_count: counter(
                provision_counts.1,
                "Runtime suppressed provision reference count",
            )?,
            provider_message_reference_count: counter(
                message_count,
                "Runtime provider message count",
            )?,
        };
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(summary)
    }

    /// Read all provisioner references scoped to one Profile, including terminal
    /// rows and Prepared targets whose Runtime Session is not yet bound.
    ///
    /// # Errors
    /// Rejects invalid page bounds, malformed stored rows, or SQLite failures.
    pub fn profile_deletion_provision_references_page(
        &self,
        profile_id: AgentProfileId,
        after_operation_id: Option<Uuid>,
        limit: u32,
    ) -> Result<AgentProfileDeletionProvisionReferencePage, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        let page = provision_reference_page(&tx, profile_id, None, after_operation_id, limit)?;
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(page)
    }

    /// Read provisioner references for one exact Profile and Runtime Session.
    /// This reverse-reference page lets the host reconcile each frozen
    /// `NoLinkedTarget` without rescanning every Ops row or treating absence as proof.
    ///
    /// # Errors
    /// Rejects invalid page bounds, malformed stored rows, or SQLite failures.
    pub fn profile_deletion_runtime_provision_references_page(
        &self,
        profile_id: AgentProfileId,
        runtime_session_id: AgentProfileRuntimeSessionId,
        after_operation_id: Option<Uuid>,
        limit: u32,
    ) -> Result<AgentProfileDeletionProvisionReferencePage, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        let page = provision_reference_page(
            &tx,
            profile_id,
            Some(runtime_session_id),
            after_operation_id,
            limit,
        )?;
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(page)
    }

    /// Read all provider outbox references scoped to one Profile. Message body
    /// and provider response content are deliberately excluded.
    ///
    /// # Errors
    /// Rejects invalid page bounds, malformed stored rows, or SQLite failures.
    pub fn profile_deletion_provider_message_references_page(
        &self,
        profile_id: AgentProfileId,
        after_message_id: Option<Uuid>,
        limit: u32,
    ) -> Result<AgentProfileDeletionProviderMessageReferencePage, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        let page = provider_message_reference_page(&tx, profile_id, None, after_message_id, limit)?;
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(page)
    }

    /// Read outbox references for one exact Profile and Runtime Session.
    /// Message body and provider response content are deliberately excluded.
    ///
    /// # Errors
    /// Rejects invalid page bounds, malformed stored rows, or SQLite failures.
    pub fn profile_deletion_runtime_provider_message_references_page(
        &self,
        profile_id: AgentProfileId,
        runtime_session_id: AgentProfileRuntimeSessionId,
        after_message_id: Option<Uuid>,
        limit: u32,
    ) -> Result<AgentProfileDeletionProviderMessageReferencePage, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        let page = provider_message_reference_page(
            &tx,
            profile_id,
            Some(runtime_session_id),
            after_message_id,
            limit,
        )?;
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(page)
    }

    /// Read the exact target-delete journal for one request without a total-row cap.
    ///
    /// # Errors
    /// Rejects missing requests, invalid cursors/limits, malformed rows, or SQLite failures.
    pub fn agent_profile_provider_deletions_page(
        &self,
        request_id: Uuid,
        after_runtime_id: Option<AgentProfileRuntimeSessionId>,
        limit: u32,
    ) -> Result<AgentProfileProviderDeletionPage, OperationsError> {
        validate_uuid(request_id, "provider deletion request identity")?;
        validate_reference_page(limit)?;
        if after_runtime_id.is_some_and(|value| value.into_uuid().is_nil()) {
            return Err(OperationsError::Conflict("provider deletion target cursor"));
        }
        let tx = self.connection.unchecked_transaction()?;
        let progress = load_progress(&tx, request_id)?.ok_or(OperationsError::Conflict(
            "Profile deletion progress missing",
        ))?;
        let after = after_runtime_id.map(|id| id.into_uuid().as_bytes().to_vec());
        let mut statement = tx.prepare(
            "SELECT substr(runtime_session_id,1,17)
             FROM ops_agent_profile_provider_deletions
             WHERE request_id=?1 AND (?2 IS NULL OR runtime_session_id>?2)
             ORDER BY runtime_session_id LIMIT ?3",
        )?;
        let rows = statement.query_map(
            params![request_id.as_bytes(), after, i64::from(limit) + 1],
            |row| row.get::<_, Vec<u8>>(0),
        )?;
        let mut runtime_ids = rows
            .map(|row| {
                AgentProfileRuntimeSessionId::from_uuid(Uuid::from_bytes(wire::id(row?)?))
                    .map_err(|_| OperationsError::Corrupt("provider deletion Runtime Session ID"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let has_more = runtime_ids.len() > limit as usize;
        if has_more {
            runtime_ids.pop();
        }
        let next_runtime_id = has_more.then(|| runtime_ids.last().copied()).flatten();
        drop(statement);
        let mut operations = Vec::with_capacity(runtime_ids.len());
        for runtime_id in runtime_ids {
            let operation_id = derived_provider_deletion_operation_id(request_id, runtime_id);
            let operation = load_provider_deletion(&tx, operation_id)?.ok_or(
                OperationsError::Corrupt("provider deletion page row missing"),
            )?;
            if operation.request_id != request_id
                || operation.profile_id != progress.profile_id
                || operation.runtime_session_id != runtime_id
            {
                return Err(OperationsError::Corrupt("provider deletion page scope"));
            }
            operations.push(operation);
        }
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(AgentProfileProviderDeletionPage {
            operations,
            next_runtime_id,
        })
    }

    /// Read one provider-deletion operation by its deterministic stable identity.
    ///
    /// # Errors
    /// Rejects a nil identity, malformed row, or SQLite failures.
    pub fn agent_profile_provider_deletion(
        &self,
        operation_id: Uuid,
    ) -> Result<Option<AgentProfileProviderDeletionOperation>, OperationsError> {
        validate_uuid(operation_id, "provider deletion operation identity")?;
        load_provider_deletion(&self.connection, operation_id)
    }

    /// Read the request's exact durable delete progress and outcome counts.
    /// Counts describe Operations rows; they are not provider-idle proof.
    ///
    /// # Errors
    /// Rejects missing progress, malformed state/counters, or SQLite failures.
    pub fn agent_profile_provider_deletion_summary(
        &self,
        request_id: Uuid,
    ) -> Result<AgentProfileProviderDeletionSummary, OperationsError> {
        validate_uuid(request_id, "provider deletion request identity")?;
        let tx = self.connection.unchecked_transaction()?;
        let progress = load_progress(&tx, request_id)?.ok_or(OperationsError::Conflict(
            "Profile deletion progress missing",
        ))?;
        let counts: (i64, i64, i64, i64, i64) = tx.query_row(
            "SELECT
                COALESCE(SUM(CASE WHEN state=1 THEN 1 ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN state=2 THEN 1 ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN state=3 THEN 1 ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN state=4 THEN 1 ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN state=5 THEN 1 ELSE 0 END),0)
             FROM ops_agent_profile_provider_deletions WHERE request_id=?1",
            params![request_id.as_bytes()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )?;
        let latest_reason: Option<i64> = tx
            .query_row(
                "SELECT deferred_reason
             FROM ops_agent_profile_provider_deletions
             WHERE request_id=?1 AND state=4 AND deferred_reason IS NOT NULL
             ORDER BY updated_at_ms DESC,operation_id DESC LIMIT 1",
                params![request_id.as_bytes()],
                |row| row.get(0),
            )
            .optional()?;
        let summary = AgentProfileProviderDeletionSummary {
            request_id,
            profile_id: progress.profile_id,
            prepared: counter(counts.0, "provider deletion prepared count")?,
            effect_pending: counter(counts.1, "provider deletion pending count")?,
            effect_unknown: counter(counts.2, "provider deletion unknown count")?,
            deferred: counter(counts.3, "provider deletion deferred count")?,
            deleted: counter(counts.4, "provider deletion deleted count")?,
            latest_deferred_reason: latest_reason.map(decode_deferred_reason).transpose()?,
        };
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(summary)
    }

    /// Read the exact durable receipt for one target attempt.
    ///
    /// # Errors
    /// Rejects invalid identity/attempt numbers, malformed receipts, or SQLite failures.
    pub fn agent_profile_provider_deletion_receipt(
        &self,
        operation_id: Uuid,
        attempt_number: u64,
    ) -> Result<Option<AgentProfileProviderDeletionReceipt>, OperationsError> {
        validate_uuid(operation_id, "provider deletion operation identity")?;
        if attempt_number == 0 {
            return Err(OperationsError::Conflict(
                "provider deletion attempt identity",
            ));
        }
        let tx = self.connection.unchecked_transaction()?;
        let receipt = load_provider_deletion_receipt(&tx, operation_id, attempt_number)?;
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(receipt)
    }

    /// Prove all linked targets have exact positive current-attempt receipts.
    /// Rows are streamed in a single snapshot with bounded memory.
    ///
    /// # Errors
    /// Rejects missing progress or corrupt target/receipt history.
    pub fn profile_deletion_provider_receipts_complete(
        &self,
        request_id: Uuid,
    ) -> Result<bool, OperationsError> {
        validate_uuid(request_id, "provider deletion request identity")?;
        let tx = self.connection.unchecked_transaction()?;
        let progress = load_progress(&tx, request_id)?.ok_or(OperationsError::Conflict(
            "Profile deletion progress missing",
        ))?;
        let complete = provider_receipts_complete_in(&tx, &progress)?;
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(complete)
    }

    /// Match positive receipts back to exact Sessions frozen targets.
    /// The caller supplies the existing read capability; no catalog is opened.
    /// Pages hold at most 32 entries and Operations rows are streamed.
    ///
    /// # Errors
    /// Rejects changed metadata, incomplete receipts or owner failures.
    pub fn profile_deletion_provider_receipts_complete_with_manifest(
        &self,
        request_id: Uuid,
        profiles: &heterarchy_orchestration_sessions::AgentProfileCatalog,
    ) -> Result<bool, OperationsError> {
        validate_uuid(request_id, "provider deletion request identity")?;
        let tx = self.connection.unchecked_transaction()?;
        let progress = load_progress(&tx, request_id)?.ok_or(OperationsError::Conflict(
            "Profile deletion progress missing",
        ))?;
        if !provider_receipts_complete_in(&tx, &progress)? {
            crate::catalog::commit_read(tx, self.destination)?;
            return Ok(false);
        }
        profiles
            .validate_profile_deletion_manifest_entries(request_id)
            .map_err(OperationsError::ProfileDeletionEvidence)?;
        let manifest = profiles
            .profile_deletion_manifest(request_id)
            .map_err(OperationsError::ProfileDeletionEvidence)?
            .ok_or(OperationsError::Corrupt(
                "Sessions frozen deletion manifest missing",
            ))?;
        let mut statement = tx.prepare(
            "SELECT operation_id,request_id,profile_id,runtime_session_id,
                    provider_family,provider_conversation_id,original_runtime_revision,
                    original_runtime_state,state,attempt_count,deferred_reason,retry_after_ms,
                    prepared_at_ms,updated_at_ms,revision,NULL
             FROM ops_agent_profile_provider_deletions WHERE request_id=?1 ORDER BY runtime_session_id",
        )?;
        let mut operations = statement.query(params![request_id.as_bytes()])?;
        let mut after = None;
        loop {
            let page = profiles
                .profile_deletion_runtime_entries_page(request_id, after, 32)
                .map_err(OperationsError::ProfileDeletionEvidence)?;
            for entry in page.entries() {
                let AgentProfileDeletionRuntimeTarget::LinkedProviderConversation {
                    conversation_id,
                } = entry.target()
                else {
                    continue;
                };
                let row = operations
                    .next()?
                    .ok_or(OperationsError::Corrupt("frozen provider target missing"))?;
                let operation = decode_provider_deletion(decode_raw_provider_deletion(row)?)?;
                validate_provider_deletion_target(&operation, manifest, entry, conversation_id)?;
            }
            let Some(next) = page.next_runtime_id() else {
                break;
            };
            if after.is_some_and(|previous| next <= previous) {
                return Err(OperationsError::Corrupt(
                    "frozen deletion cursor did not advance",
                ));
            }
            after = Some(next);
        }
        if operations.next()?.is_some() {
            return Err(OperationsError::Corrupt(
                "provider target is outside frozen inventory",
            ));
        }
        drop(operations);
        drop(statement);
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(true)
    }
}

impl OperationsCatalog {
    /// Persist one linked Session target from the completed Ops freeze phase.
    /// `NoLinkedTarget` entries intentionally create no provider operation.
    ///
    /// # Errors
    /// Rejects mismatched/incomplete manifests, wrong request/Profile entries,
    /// changed target replays, invalid timestamps, or SQLite failures.
    pub fn prepare_agent_profile_provider_deletion_target(
        &mut self,
        manifest: AgentProfileDeletionManifest,
        entry: &AgentProfileDeletionRuntimeEntry,
        prepared_at: TimestampMillis,
    ) -> Result<Option<AgentProfileProviderDeletionPrepareOutcome>, OperationsError> {
        validate_manifest_entry(manifest, entry)?;
        if manifest.completed_at().is_some() {
            return Err(OperationsError::Conflict(
                "provider deletion manifest already completed",
            ));
        }
        let linked_conversation_id = match entry.target() {
            AgentProfileDeletionRuntimeTarget::LinkedProviderConversation { conversation_id } => {
                validate_provider_conversation_id(conversation_id)?;
                Some(conversation_id.as_str())
            }
            AgentProfileDeletionRuntimeTarget::NoLinkedTarget => None,
        };
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let progress = load_progress(&tx, manifest.request_id())?.ok_or(
            OperationsError::Conflict("Profile deletion progress missing"),
        )?;
        if progress.phase != AgentProfileDeletionProgressPhase::PrepareDeletes
            || !manifest_matches_freeze(&progress, manifest, false)?
        {
            return Err(OperationsError::Conflict(
                "provider deletion target outside frozen preparation phase",
            ));
        }
        require_progress_barrier(&tx, &progress)?;
        if linked_conversation_id.is_none() {
            tx.commit()?;
            return Ok(None);
        }
        if prepared_at < manifest.frozen_at() || prepared_at < progress.updated_at {
            return Err(OperationsError::Conflict("provider deletion prepare clock"));
        }
        let conversation_id = linked_conversation_id
            .ok_or(OperationsError::Corrupt("linked provider deletion target"))?;
        let runtime_id = entry.runtime_session_id();
        let operation_id =
            derived_provider_deletion_operation_id(manifest.request_id(), runtime_id);
        if let Some(existing) = load_provider_deletion(&tx, operation_id)? {
            validate_provider_deletion_target(&existing, manifest, entry, conversation_id)?;
            tx.commit()?;
            return Ok(Some(AgentProfileProviderDeletionPrepareOutcome {
                operation: existing,
                created: false,
            }));
        }
        if load_provider_deletion_by_runtime(&tx, manifest.request_id(), runtime_id)?.is_some() {
            return Err(OperationsError::Conflict(
                "provider deletion Runtime target replay",
            ));
        }
        tx.execute(
            "INSERT INTO ops_agent_profile_provider_deletions(
                operation_id,request_id,profile_id,runtime_session_id,provider_family,
                provider_conversation_id,original_runtime_revision,original_runtime_state,
                state,attempt_count,deferred_reason,retry_after_ms,prepared_at_ms,
                updated_at_ms,revision
             ) VALUES(?1,?2,?3,?4,1,?5,?6,?7,1,0,NULL,NULL,?8,?8,1)",
            params![
                operation_id.as_bytes(),
                manifest.request_id().as_bytes(),
                manifest.profile_id().into_uuid().as_bytes(),
                runtime_id.into_uuid().as_bytes(),
                conversation_id,
                entry.original_revision().get(),
                encode_runtime_state(entry.original_state()),
                prepared_at.get(),
            ],
        )?;
        let operation = load_provider_deletion(&tx, operation_id)?
            .ok_or(OperationsError::Corrupt("provider deletion target insert"))?;
        tx.commit().map_err(|source| {
            OperationsError::AgentProfileProviderDeletionCommitUnconfirmed {
                operation_id,
                attempt_number: None,
                source,
            }
        })?;
        Ok(Some(AgentProfileProviderDeletionPrepareOutcome {
            operation,
            created: true,
        }))
    }

    /// Atomically grant at most one fresh provider-delete Execute permit across
    /// the Operations deletion journal. Existing Pending/Unknown work is Inspect-only.
    ///
    /// # Errors
    /// Rejects corrupt multiple in-flight delete attempts, clock regression,
    /// changed progress/barrier, malformed rows, or SQLite failures.
    pub fn next_agent_profile_provider_deletion_dispatch(
        &mut self,
        observed_at: TimestampMillis,
    ) -> Result<Option<AgentProfileProviderDeletionDispatch>, OperationsError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let in_flight = in_flight_provider_deletion_ids(&tx)?;
        if in_flight.len() > 1 {
            return Err(OperationsError::Corrupt(
                "multiple Profile provider deletions in flight",
            ));
        }
        if let Some(operation_id) = in_flight.first().copied() {
            let operation = load_provider_deletion(&tx, operation_id)?.ok_or(
                OperationsError::Corrupt("provider deletion in-flight readback"),
            )?;
            let dispatch =
                deletion_dispatch(&operation, AgentProfileProviderDeletionAction::Inspect);
            tx.commit()?;
            return Ok(Some(dispatch));
        }
        let operation_id = tx
            .query_row(
                "SELECT deletion.operation_id
                 FROM ops_agent_profile_provider_deletions AS deletion
                 JOIN ops_agent_profile_deletion_progress AS progress
                   ON progress.request_id=deletion.request_id
                  AND progress.profile_id=deletion.profile_id
                 JOIN ops_agent_profile_deletion_barriers AS barrier
                   ON barrier.request_id=deletion.request_id
                  AND barrier.profile_id=deletion.profile_id
                 WHERE progress.phase=5
                   AND (deletion.state=1
                        OR (deletion.state=4 AND deletion.retry_after_ms<=?1))
                   AND NOT EXISTS(
                        SELECT 1 FROM ops_agent_provision_operations AS provision
                         WHERE provision.profile_id=deletion.profile_id
                           AND provision.state IN(2,3,4)
                   )
                   AND NOT EXISTS(
                        SELECT 1 FROM ops_provider_message_outbox AS message
                         WHERE message.recipient_profile_id=deletion.profile_id
                           AND message.state IN(2,5)
                   )
                 ORDER BY deletion.prepared_at_ms,deletion.operation_id
                 LIMIT 1",
                params![observed_at.get()],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?
            .map(|bytes| wire::id(bytes).map(Uuid::from_bytes))
            .transpose()?;
        let Some(operation_id) = operation_id else {
            tx.commit()?;
            return Ok(None);
        };
        let existing = load_provider_deletion(&tx, operation_id)?.ok_or(
            OperationsError::Corrupt("provider deletion candidate missing"),
        )?;
        require_profile_deletion_delete_phase(&tx, &existing)?;
        let counts = profile_deletion::profile_deletion_in_flight_counts_in_snapshot(
            &tx,
            existing.profile_id,
        )?;
        if has_in_flight_profile_effects(counts) {
            return Err(OperationsError::Corrupt(
                "provider deletion query admitted in-flight Profile effects",
            ));
        }
        let (operation, dispatch) = release_provider_delete_execute(&tx, &existing, observed_at)?;
        commit_provider_deletion(tx, operation.operation_id, Some(operation.attempt_number))?;
        Ok(Some(dispatch))
    }

    /// Read or release the dispatch for one exact deletion operation identity.
    /// Pending/Unknown rows remain Inspect-only; a new Execute still requires
    /// `DeleteTargets` progress, an open in-flight slot, and zero scoped Ops effects.
    ///
    /// # Errors
    /// Rejects invalid identity, stale clocks, corrupt rows, or SQLite failures.
    pub fn agent_profile_provider_deletion_dispatch(
        &mut self,
        operation_id: Uuid,
        observed_at: TimestampMillis,
    ) -> Result<Option<AgentProfileProviderDeletionDispatch>, OperationsError> {
        validate_uuid(operation_id, "provider deletion operation identity")?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(existing) = load_provider_deletion(&tx, operation_id)? else {
            tx.commit()?;
            return Ok(None);
        };
        match existing.state {
            AgentProfileProviderDeletionState::EffectPending
            | AgentProfileProviderDeletionState::EffectUnknown => {
                let dispatch =
                    deletion_dispatch(&existing, AgentProfileProviderDeletionAction::Inspect);
                tx.commit()?;
                Ok(Some(dispatch))
            }
            AgentProfileProviderDeletionState::Deleted => {
                tx.commit()?;
                Ok(None)
            }
            AgentProfileProviderDeletionState::Prepared
            | AgentProfileProviderDeletionState::Deferred => {
                if deletion_in_flight_exists(&tx, Some(operation_id))? {
                    tx.commit()?;
                    return Ok(None);
                }
                require_profile_deletion_delete_phase(&tx, &existing)?;
                if existing.state == AgentProfileProviderDeletionState::Deferred
                    && existing
                        .retry_after
                        .is_some_and(|retry_after| observed_at < retry_after)
                {
                    tx.commit()?;
                    return Ok(None);
                }
                let counts = profile_deletion::profile_deletion_in_flight_counts_in_snapshot(
                    &tx,
                    existing.profile_id,
                )?;
                if has_in_flight_profile_effects(counts) {
                    tx.commit()?;
                    return Ok(None);
                }
                let (operation, dispatch) =
                    release_provider_delete_execute(&tx, &existing, observed_at)?;
                commit_provider_deletion(
                    tx,
                    operation.operation_id,
                    Some(operation.attempt_number),
                )?;
                Ok(Some(dispatch))
            }
        }
    }

    /// Reconstruct the exact action fence for a receipt roundtrip without granting
    /// a new permit. Execute is reconstructible only from its exact already-Pending
    /// revision/attempt; stale terminal callbacks require an existing receipt row.
    ///
    /// # Errors
    /// Rejects invalid identities, attempts, or malformed stored rows.
    pub fn agent_profile_provider_deletion_receipt_dispatch(
        &self,
        operation_id: Uuid,
        expected_revision: u64,
        attempt_number: u64,
        action: AgentProfileProviderDeletionAction,
    ) -> Result<Option<AgentProfileProviderDeletionDispatch>, OperationsError> {
        validate_uuid(operation_id, "provider deletion operation identity")?;
        if expected_revision == 0 || attempt_number == 0 {
            return Err(OperationsError::Conflict("provider deletion receipt fence"));
        }
        let tx = self.connection.unchecked_transaction()?;
        let Some(operation) = load_provider_deletion(&tx, operation_id)? else {
            crate::catalog::commit_read(tx, self.destination)?;
            return Ok(None);
        };
        let dispatch = match operation.state {
            AgentProfileProviderDeletionState::EffectPending
                if operation.revision == expected_revision
                    && operation.attempt_number == attempt_number =>
            {
                Some(deletion_dispatch(&operation, action))
            }
            AgentProfileProviderDeletionState::EffectUnknown
                if action == AgentProfileProviderDeletionAction::Inspect
                    && operation.revision == expected_revision
                    && operation.attempt_number == attempt_number =>
            {
                Some(deletion_dispatch(&operation, action))
            }
            AgentProfileProviderDeletionState::Deferred
            | AgentProfileProviderDeletionState::Deleted => {
                load_provider_deletion_receipt(&tx, operation_id, attempt_number)?
                    .filter(|receipt| {
                        receipt.dispatch.revision == expected_revision
                            && receipt.dispatch.action == action
                    })
                    .map(|receipt| receipt.dispatch)
            }
            AgentProfileProviderDeletionState::Prepared
            | AgentProfileProviderDeletionState::EffectPending
            | AgentProfileProviderDeletionState::EffectUnknown => None,
        };
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(dispatch)
    }

    /// Reconcile a lost dispatch response without ever retrying Unknown as Execute.
    ///
    /// # Errors
    /// Rejects changed attempts/revisions, invalid transitions, clock regression,
    /// or SQLite failures.
    pub fn mark_agent_profile_provider_deletion_unknown(
        &mut self,
        operation_id: Uuid,
        expected_revision: u64,
        attempt_number: u64,
        observed_at: TimestampMillis,
    ) -> Result<AgentProfileProviderDeletionOperation, OperationsError> {
        validate_uuid(operation_id, "provider deletion operation identity")?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = load_provider_deletion(&tx, operation_id)?.ok_or(
            OperationsError::Conflict("provider deletion operation missing"),
        )?;
        if existing.state == AgentProfileProviderDeletionState::EffectUnknown
            && expected_revision.checked_add(1) == Some(existing.revision)
            && existing.attempt_number == attempt_number
        {
            tx.commit()?;
            return Ok(existing);
        }
        if existing.state != AgentProfileProviderDeletionState::EffectPending
            || existing.revision != expected_revision
            || existing.attempt_number != attempt_number
            || observed_at < existing.updated_at
        {
            return Err(OperationsError::Conflict("provider deletion Unknown fence"));
        }
        let revision = next_progress_revision(existing.revision)?;
        let changed = tx.execute(
            "UPDATE ops_agent_profile_provider_deletions
             SET state=3,updated_at_ms=?1,revision=?2
             WHERE operation_id=?3 AND revision=?4 AND attempt_count=?5 AND state=2",
            params![
                observed_at.get(),
                signed_progress_revision(revision)?,
                operation_id.as_bytes(),
                signed_progress_revision(expected_revision)?,
                signed_progress_revision(attempt_number)?,
            ],
        )?;
        if changed != 1 {
            return Err(OperationsError::Conflict("provider deletion Unknown CAS"));
        }
        let operation = load_provider_deletion(&tx, operation_id)?.ok_or(
            OperationsError::Corrupt("provider deletion Unknown readback"),
        )?;
        commit_provider_deletion(tx, operation_id, Some(attempt_number))?;
        Ok(operation)
    }

    /// Persist a trusted host receipt for a pre-click deferral, positive no-effect
    /// readback, or positive provider delete confirmation. Exact attempt replay
    /// returns the same receipt; a different outcome conflicts.
    ///
    /// # Errors
    /// Rejects changed immutable targets, stale dispatch fences, invalid state
    /// transitions, malformed browser metadata, or SQLite failures.
    pub fn record_agent_profile_provider_deletion_receipt(
        &mut self,
        receipt: &AgentProfileProviderDeletionReceipt,
    ) -> Result<AgentProfileProviderDeletionReceipt, OperationsError> {
        let dispatch = &receipt.dispatch;
        validate_provider_deletion_receipt_dispatch(dispatch)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing_receipt) =
            load_provider_deletion_receipt(&tx, dispatch.operation_id, dispatch.attempt_number)?
        {
            if !provider_deletion_receipts_equal(&existing_receipt, receipt) {
                return Err(OperationsError::Conflict(
                    "provider deletion receipt replay",
                ));
            }
            tx.commit()?;
            return Ok(existing_receipt);
        }
        let stored = record_provider_deletion_receipt_in_transaction(&tx, receipt)?;
        commit_provider_deletion(tx, dispatch.operation_id, Some(dispatch.attempt_number))?;
        Ok(stored)
    }
}

fn validate_provider_deletion_receipt_dispatch(
    dispatch: &AgentProfileProviderDeletionDispatch,
) -> Result<(), OperationsError> {
    validate_uuid(
        dispatch.operation_id,
        "provider deletion operation identity",
    )?;
    if dispatch.attempt_number == 0 || dispatch.revision == 0 {
        return Err(OperationsError::Conflict("provider deletion receipt fence"));
    }
    Ok(())
}

fn record_provider_deletion_receipt_in_transaction(
    transaction: &Transaction<'_>,
    receipt: &AgentProfileProviderDeletionReceipt,
) -> Result<AgentProfileProviderDeletionReceipt, OperationsError> {
    let dispatch = &receipt.dispatch;
    let existing = load_provider_deletion(transaction, dispatch.operation_id)?.ok_or(
        OperationsError::Conflict("provider deletion operation missing"),
    )?;
    if !dispatch_matches_operation_target(dispatch, &existing)
        || dispatch.revision != existing.revision
        || dispatch.attempt_number != existing.attempt_number
        || !matches!(
            existing.state,
            AgentProfileProviderDeletionState::EffectPending
                | AgentProfileProviderDeletionState::EffectUnknown
        )
        || receipt.observed_at < existing.updated_at
    {
        return Err(OperationsError::Conflict("provider deletion receipt fence"));
    }
    let next_revision = next_progress_revision(existing.revision)?;
    let transition = receipt_transition(dispatch, &existing, receipt)?;
    insert_provider_deletion_receipt(transaction, receipt, transition.outcome_kind)?;
    let changed = transaction.execute(
        "UPDATE ops_agent_profile_provider_deletions
         SET state=?1,attempt_count=?2,deferred_reason=?3,retry_after_ms=?4,
             updated_at_ms=?5,revision=?6
         WHERE operation_id=?7 AND revision=?8 AND attempt_count=?2 AND state IN(2,3)",
        params![
            encode_provider_deletion_state(transition.state),
            signed_progress_revision(existing.attempt_number)?,
            transition.reason.map(encode_deferred_reason),
            transition.retry_after.map(TimestampMillis::get),
            receipt.observed_at.get(),
            signed_progress_revision(next_revision)?,
            existing.operation_id.as_bytes(),
            signed_progress_revision(existing.revision)?,
        ],
    )?;
    if changed != 1 {
        return Err(OperationsError::Conflict("provider deletion receipt CAS"));
    }
    load_provider_deletion_receipt(transaction, dispatch.operation_id, dispatch.attempt_number)?
        .ok_or(OperationsError::Corrupt(
            "provider deletion receipt insert readback",
        ))
}

struct ProviderDeletionReceiptTransition {
    state: AgentProfileProviderDeletionState,
    reason: Option<AgentProfileProviderDeletionDeferredReason>,
    retry_after: Option<TimestampMillis>,
    outcome_kind: i64,
}

fn receipt_transition(
    dispatch: &AgentProfileProviderDeletionDispatch,
    existing: &AgentProfileProviderDeletionOperation,
    receipt: &AgentProfileProviderDeletionReceipt,
) -> Result<ProviderDeletionReceiptTransition, OperationsError> {
    match &receipt.outcome {
        AgentProfileProviderDeletionReceiptOutcome::NotStartedBeforeClick {
            reason,
            retry_after,
        } => {
            if dispatch.action != AgentProfileProviderDeletionAction::Execute
                || existing.state != AgentProfileProviderDeletionState::EffectPending
            {
                return Err(OperationsError::Conflict(
                    "provider deletion pre-click receipt state",
                ));
            }
            Ok(ProviderDeletionReceiptTransition {
                state: AgentProfileProviderDeletionState::Deferred,
                reason: Some(*reason),
                retry_after: Some(*retry_after),
                outcome_kind: RECEIPT_NOT_STARTED_BEFORE_CLICK,
            })
        }
        AgentProfileProviderDeletionReceiptOutcome::PositiveNoEffectReadback {
            reason,
            retry_after,
        } => {
            if dispatch.action != AgentProfileProviderDeletionAction::Inspect {
                return Err(OperationsError::Conflict(
                    "provider deletion no-effect inspection",
                ));
            }
            Ok(ProviderDeletionReceiptTransition {
                state: AgentProfileProviderDeletionState::Deferred,
                reason: Some(*reason),
                retry_after: Some(*retry_after),
                outcome_kind: RECEIPT_POSITIVE_NO_EFFECT_READBACK,
            })
        }
        AgentProfileProviderDeletionReceiptOutcome::ProviderDeleteConfirmed => {
            Ok(ProviderDeletionReceiptTransition {
                state: AgentProfileProviderDeletionState::Deleted,
                reason: None,
                retry_after: None,
                outcome_kind: RECEIPT_PROVIDER_DELETE_CONFIRMED,
            })
        }
    }
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    connection.execute_batch(PROFILE_DELETION_PROGRESS_SQL)?;
    connection.execute_batch(PROFILE_PROVIDER_DELETIONS_SQL)?;
    connection.execute_batch(PROFILE_PROVIDER_DELETION_RECEIPTS_SQL)?;
    connection.execute_batch(PROFILE_PROVIDER_DELETION_WORK_INDEX)?;
    connection.execute_batch(PROVISION_PROFILE_REFERENCE_INDEX)?;
    connection.execute_batch(PROVISION_PROFILE_RUNTIME_REFERENCE_INDEX)?;
    connection.execute_batch(MESSAGE_PROFILE_REFERENCE_INDEX)?;
    connection.execute_batch(MESSAGE_PROFILE_RUNTIME_REFERENCE_INDEX)?;
    Ok(())
}

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        (
            "table",
            "ops_agent_profile_deletion_progress",
            PROFILE_DELETION_PROGRESS_SQL,
        ),
        (
            "table",
            "ops_agent_profile_provider_deletions",
            PROFILE_PROVIDER_DELETIONS_SQL,
        ),
        (
            "table",
            "ops_agent_profile_provider_deletion_receipts",
            PROFILE_PROVIDER_DELETION_RECEIPTS_SQL,
        ),
        (
            "index",
            "ops_agent_profile_provider_deletions_work_idx",
            PROFILE_PROVIDER_DELETION_WORK_INDEX,
        ),
        (
            "index",
            "ops_agent_provision_profile_reference_idx",
            PROVISION_PROFILE_REFERENCE_INDEX,
        ),
        (
            "index",
            "ops_agent_provision_profile_runtime_reference_idx",
            PROVISION_PROFILE_RUNTIME_REFERENCE_INDEX,
        ),
        (
            "index",
            "ops_provider_message_profile_reference_idx",
            MESSAGE_PROFILE_REFERENCE_INDEX,
        ),
        (
            "index",
            "ops_provider_message_profile_runtime_reference_idx",
            MESSAGE_PROFILE_RUNTIME_REFERENCE_INDEX,
        ),
    ]
}

#[cfg(test)]
pub(crate) fn drop_schema_for_legacy_fixture(
    connection: &Connection,
) -> Result<(), OperationsError> {
    connection.execute_batch(
        "DROP INDEX IF EXISTS ops_agent_provision_profile_reference_idx;
         DROP INDEX IF EXISTS ops_agent_provision_profile_runtime_reference_idx;
         DROP INDEX IF EXISTS ops_provider_message_profile_reference_idx;
         DROP INDEX IF EXISTS ops_provider_message_profile_runtime_reference_idx;
         DROP TABLE IF EXISTS ops_agent_profile_provider_deletion_receipts;
         DROP TABLE IF EXISTS ops_agent_profile_provider_deletions;
         DROP TABLE IF EXISTS ops_agent_profile_deletion_progress;",
    )?;
    Ok(())
}

fn validate_manifest_shape(
    manifest: AgentProfileDeletionManifest,
    allow_completed: bool,
) -> Result<AgentProfileDeletionFreeze, OperationsError> {
    let total = manifest
        .linked_target_count()
        .checked_add(manifest.no_linked_target_count())
        .ok_or(OperationsError::Bounds("Profile deletion manifest count"))?;
    if total != manifest.runtime_entry_count()
        || (allow_completed != manifest.completed_at().is_some())
        || manifest
            .completed_at()
            .is_some_and(|at| at < manifest.frozen_at())
    {
        return Err(OperationsError::Conflict("Profile deletion manifest shape"));
    }
    Ok(AgentProfileDeletionFreeze {
        profile_revision_at_freeze: manifest.profile_revision_at_freeze(),
        frozen_at: manifest.frozen_at(),
        runtime_entry_count: manifest.runtime_entry_count(),
        linked_target_count: manifest.linked_target_count(),
        no_linked_target_count: manifest.no_linked_target_count(),
        completed_at: manifest.completed_at(),
    })
}

fn validate_manifest_entry(
    manifest: AgentProfileDeletionManifest,
    entry: &AgentProfileDeletionRuntimeEntry,
) -> Result<(), OperationsError> {
    if entry.request_id() != manifest.request_id() || entry.profile_id() != manifest.profile_id() {
        return Err(OperationsError::Conflict(
            "Profile deletion Runtime entry scope",
        ));
    }
    let _ = validate_manifest_shape(manifest, false)?;
    Ok(())
}

fn validate_manifest_for_freeze(
    progress: &AgentProfileDeletionProgress,
    manifest: AgentProfileDeletionManifest,
) -> Result<AgentProfileDeletionFreeze, OperationsError> {
    if manifest.request_id() != progress.request_id
        || manifest.profile_id() != progress.profile_id
        || manifest.profile_revision_at_freeze() != progress.barrier_profile_revision
        || manifest.frozen_at() < progress.requested_at
    {
        return Err(OperationsError::Conflict(
            "Profile deletion freeze identity",
        ));
    }
    validate_manifest_shape(manifest, false)
}

fn validate_manifest_finished(
    progress: &AgentProfileDeletionProgress,
    manifest: AgentProfileDeletionManifest,
) -> Result<AgentProfileDeletionFreeze, OperationsError> {
    let completed = validate_manifest_shape(manifest, true)?;
    let frozen = progress
        .freeze
        .ok_or(OperationsError::Corrupt("Profile deletion freeze missing"))?;
    if manifest.request_id() != progress.request_id
        || manifest.profile_id() != progress.profile_id
        || completed.profile_revision_at_freeze != frozen.profile_revision_at_freeze
        || completed.frozen_at != frozen.frozen_at
        || completed.runtime_entry_count != frozen.runtime_entry_count
        || completed.linked_target_count != frozen.linked_target_count
        || completed.no_linked_target_count != frozen.no_linked_target_count
    {
        return Err(OperationsError::Conflict(
            "Profile deletion completion manifest",
        ));
    }
    Ok(completed)
}

fn manifest_matches_freeze(
    progress: &AgentProfileDeletionProgress,
    manifest: AgentProfileDeletionManifest,
    allow_completed: bool,
) -> Result<bool, OperationsError> {
    let freeze = validate_manifest_shape(manifest, allow_completed)?;
    Ok(manifest.request_id() == progress.request_id
        && manifest.profile_id() == progress.profile_id
        && progress.freeze == Some(freeze))
}

fn progress_matches_intent(
    progress: &AgentProfileDeletionProgress,
    intent: &AgentProfileDeletionIntent,
) -> bool {
    progress.request_id == intent.request_id()
        && progress.profile_id == intent.profile_id()
        && progress.expected_profile_revision == intent.expected_profile_revision()
        && progress.barrier_profile_revision == intent.barrier_profile_revision()
        && progress.requested_at == intent.requested_at()
}

fn barrier_matches_progress(
    barrier: &AgentProfileDeletionBarrier,
    progress: &AgentProfileDeletionProgress,
) -> bool {
    barrier.request_id() == progress.request_id
        && barrier.profile_id() == progress.profile_id
        && barrier.expected_profile_revision() == progress.expected_profile_revision
        && barrier.barrier_profile_revision() == progress.barrier_profile_revision
        && barrier.requested_at() == progress.requested_at
}

fn require_progress_barrier(
    connection: &Connection,
    progress: &AgentProfileDeletionProgress,
) -> Result<(), OperationsError> {
    let barrier = profile_deletion::load_barrier_by_request(connection, progress.request_id)?
        .ok_or(OperationsError::Conflict(
            "Agent Profile deletion barrier missing",
        ))?;
    if !barrier_matches_progress(&barrier, progress) {
        return Err(OperationsError::Conflict(
            "Agent Profile deletion barrier changed",
        ));
    }
    Ok(())
}

fn validate_progress_revision(
    progress: &AgentProfileDeletionProgress,
    expected_revision: u64,
    observed_at: TimestampMillis,
) -> Result<(), OperationsError> {
    if expected_revision == 0 || progress.revision != expected_revision {
        return Err(OperationsError::Conflict(
            "Profile deletion progress revision",
        ));
    }
    if observed_at < progress.updated_at {
        return Err(OperationsError::Conflict("Profile deletion progress clock"));
    }
    Ok(())
}

fn update_progress(
    transaction: &Transaction<'_>,
    existing: &AgentProfileDeletionProgress,
    next: &AgentProfileDeletionProgress,
) -> Result<(), OperationsError> {
    let freeze = next.freeze;
    let changed = transaction.execute(
        "UPDATE ops_agent_profile_deletion_progress
         SET phase=?1,deferred_reason=?2,revision=?3,after_provision_operation_id=?4,
             after_message_id=?5,after_runtime_id=?6,profile_revision_at_freeze=?7,
             frozen_at_ms=?8,runtime_entry_count=?9,linked_target_count=?10,
             no_linked_target_count=?11,completed_at_ms=?12,updated_at_ms=?13
         WHERE request_id=?14 AND revision=?15 AND phase=?16",
        params![
            encode_progress_phase(next.phase),
            next.deferred_reason.map(encode_progress_deferred_reason),
            signed_progress_revision(next.revision)?,
            next.after_provision_operation_id
                .map(|id| id.as_bytes().to_vec()),
            next.after_message_id.map(|id| id.as_bytes().to_vec()),
            next.after_runtime_id
                .map(|id| id.into_uuid().as_bytes().to_vec()),
            freeze.map(|value| value.profile_revision_at_freeze.get()),
            freeze.map(|value| value.frozen_at.get()),
            freeze
                .map(|value| signed_count(value.runtime_entry_count))
                .transpose()?,
            freeze
                .map(|value| signed_count(value.linked_target_count))
                .transpose()?,
            freeze
                .map(|value| signed_count(value.no_linked_target_count))
                .transpose()?,
            freeze.and_then(|value| value.completed_at.map(TimestampMillis::get)),
            next.updated_at.get(),
            existing.request_id.as_bytes(),
            signed_progress_revision(existing.revision)?,
            encode_progress_phase(existing.phase),
        ],
    )?;
    if changed != 1 {
        return Err(OperationsError::Conflict("Profile deletion progress CAS"));
    }
    Ok(())
}

fn load_progress(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<AgentProfileDeletionProgress>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(request_id,1,17),substr(profile_id,1,17),
                    expected_profile_revision,barrier_profile_revision,requested_at_ms,
                    phase,deferred_reason,revision,substr(after_provision_operation_id,1,17),
                    substr(after_message_id,1,17),substr(after_runtime_id,1,17),
                    profile_revision_at_freeze,frozen_at_ms,runtime_entry_count,
                    linked_target_count,no_linked_target_count,completed_at_ms,updated_at_ms
             FROM ops_agent_profile_deletion_progress WHERE request_id=?1",
            params![request_id.as_bytes()],
            decode_raw_progress,
        )
        .optional()?;
    raw.map(decode_progress).transpose()
}

struct RawProgress {
    request_id: Vec<u8>,
    profile_id: Vec<u8>,
    expected_profile_revision: i64,
    barrier_profile_revision: i64,
    requested_at_ms: i64,
    phase: i64,
    deferred_reason: Option<i64>,
    revision: i64,
    after_provision_operation_id: Option<Vec<u8>>,
    after_message_id: Option<Vec<u8>>,
    after_runtime_id: Option<Vec<u8>>,
    profile_revision_at_freeze: Option<i64>,
    frozen_at_ms: Option<i64>,
    runtime_entry_count: Option<i64>,
    linked_target_count: Option<i64>,
    no_linked_target_count: Option<i64>,
    completed_at_ms: Option<i64>,
    updated_at_ms: i64,
}

fn decode_raw_progress(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawProgress> {
    Ok(RawProgress {
        request_id: row.get(0)?,
        profile_id: row.get(1)?,
        expected_profile_revision: row.get(2)?,
        barrier_profile_revision: row.get(3)?,
        requested_at_ms: row.get(4)?,
        phase: row.get(5)?,
        deferred_reason: row.get(6)?,
        revision: row.get(7)?,
        after_provision_operation_id: row.get(8)?,
        after_message_id: row.get(9)?,
        after_runtime_id: row.get(10)?,
        profile_revision_at_freeze: row.get(11)?,
        frozen_at_ms: row.get(12)?,
        runtime_entry_count: row.get(13)?,
        linked_target_count: row.get(14)?,
        no_linked_target_count: row.get(15)?,
        completed_at_ms: row.get(16)?,
        updated_at_ms: row.get(17)?,
    })
}

fn decode_progress(raw: RawProgress) -> Result<AgentProfileDeletionProgress, OperationsError> {
    let freeze = decode_progress_freeze(&raw)?;
    let request_id = Uuid::from_bytes(wire::id(raw.request_id)?);
    let profile_id = decode_profile_id(raw.profile_id, "Profile deletion progress Profile")?;
    let expected_profile_revision = decode_profile_revision(
        raw.expected_profile_revision,
        "Profile deletion progress expected revision",
    )?;
    let barrier_profile_revision = decode_profile_revision(
        raw.barrier_profile_revision,
        "Profile deletion progress barrier revision",
    )?;
    let requested_at = decode_timestamp(raw.requested_at_ms, "Profile deletion request timestamp")?;
    let phase = decode_progress_phase(raw.phase)?;
    let deferred_reason = raw
        .deferred_reason
        .map(decode_progress_deferred_reason)
        .transpose()?;
    let revision = decode_positive_counter(raw.revision, "Profile deletion progress revision")?;
    let after_provision_operation_id = raw
        .after_provision_operation_id
        .map(|bytes| wire::id(bytes).map(Uuid::from_bytes))
        .transpose()?;
    let after_message_id = raw
        .after_message_id
        .map(|bytes| wire::id(bytes).map(Uuid::from_bytes))
        .transpose()?;
    let after_runtime_id = raw
        .after_runtime_id
        .map(|bytes| {
            AgentProfileRuntimeSessionId::from_uuid(Uuid::from_bytes(wire::id(bytes)?))
                .map_err(|_| OperationsError::Corrupt("Profile deletion progress Runtime ID"))
        })
        .transpose()?;
    let updated_at = decode_timestamp(
        raw.updated_at_ms,
        "Profile deletion progress updated timestamp",
    )?;
    let phase_requires_freeze = matches!(
        phase,
        AgentProfileDeletionProgressPhase::PrepareDeletes
            | AgentProfileDeletionProgressPhase::DeleteTargets
            | AgentProfileDeletionProgressPhase::Finalize
            | AgentProfileDeletionProgressPhase::Completed
    );
    if revision == 0
        || updated_at < requested_at
        || phase_requires_freeze != freeze.is_some()
        || (phase == AgentProfileDeletionProgressPhase::Completed)
            != freeze.is_some_and(|value| value.completed_at.is_some())
    {
        return Err(OperationsError::Corrupt(
            "Profile deletion progress row shape",
        ));
    }
    if phase == AgentProfileDeletionProgressPhase::Completed && deferred_reason.is_some() {
        return Err(OperationsError::Corrupt(
            "completed Profile deletion is deferred",
        ));
    }
    Ok(AgentProfileDeletionProgress {
        request_id,
        profile_id,
        expected_profile_revision,
        barrier_profile_revision,
        requested_at,
        phase,
        deferred_reason,
        revision,
        after_provision_operation_id,
        after_message_id,
        after_runtime_id,
        freeze,
        updated_at,
    })
}

fn decode_progress_freeze(
    raw: &RawProgress,
) -> Result<Option<AgentProfileDeletionFreeze>, OperationsError> {
    let freeze_fields = (
        raw.profile_revision_at_freeze,
        raw.frozen_at_ms,
        raw.runtime_entry_count,
        raw.linked_target_count,
        raw.no_linked_target_count,
    );
    let any_freeze_field = freeze_fields.0.is_some()
        || freeze_fields.1.is_some()
        || freeze_fields.2.is_some()
        || freeze_fields.3.is_some()
        || freeze_fields.4.is_some();
    if !any_freeze_field {
        if raw.completed_at_ms.is_some() {
            return Err(OperationsError::Corrupt(
                "Profile deletion completion without freeze",
            ));
        }
        return Ok(None);
    }
    let (
        Some(profile_revision),
        Some(frozen_at),
        Some(runtime_count),
        Some(linked_count),
        Some(no_linked_count),
    ) = freeze_fields
    else {
        return Err(OperationsError::Corrupt(
            "Profile deletion freeze row shape",
        ));
    };
    let runtime_entry_count = counter(runtime_count, "Profile deletion runtime count")?;
    let linked_target_count = counter(linked_count, "Profile deletion linked target count")?;
    let no_linked_target_count = counter(no_linked_count, "Profile deletion no-link target count")?;
    if linked_target_count.checked_add(no_linked_target_count) != Some(runtime_entry_count) {
        return Err(OperationsError::Corrupt("Profile deletion freeze counts"));
    }
    Ok(Some(AgentProfileDeletionFreeze {
        profile_revision_at_freeze: decode_profile_revision(
            profile_revision,
            "Profile deletion frozen revision",
        )?,
        frozen_at: decode_timestamp(frozen_at, "Profile deletion freeze timestamp")?,
        runtime_entry_count,
        linked_target_count,
        no_linked_target_count,
        completed_at: raw
            .completed_at_ms
            .map(|value| decode_timestamp(value, "Profile deletion completion timestamp"))
            .transpose()?,
    }))
}

fn provider_deletion_count(
    connection: &Connection,
    request_id: Uuid,
) -> Result<u64, OperationsError> {
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM ops_agent_profile_provider_deletions WHERE request_id=?1",
        params![request_id.as_bytes()],
        |row| row.get(0),
    )?;
    counter(count, "provider deletion target count")
}

fn ensure_provider_targets_settled(
    connection: &Connection,
    progress: &AgentProfileDeletionProgress,
) -> Result<(), OperationsError> {
    let freeze = progress
        .freeze
        .ok_or(OperationsError::Corrupt("Profile deletion freeze missing"))?;
    let (total, deleted): (i64, i64) = connection.query_row(
        "SELECT COUNT(*),COALESCE(SUM(CASE WHEN state=5 THEN 1 ELSE 0 END),0)
         FROM ops_agent_profile_provider_deletions WHERE request_id=?1",
        params![progress.request_id.as_bytes()],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let total = counter(total, "provider deletion target count")?;
    let deleted = counter(deleted, "provider deletion receipt count")?;
    if total != freeze.linked_target_count || deleted != total {
        return Err(OperationsError::Conflict(
            "provider deletion targets remain unsettled",
        ));
    }
    if !provider_receipts_complete_in(connection, progress)? {
        return Err(OperationsError::Conflict(
            "provider deletion receipts remain incomplete",
        ));
    }
    let in_flight = profile_deletion::profile_deletion_in_flight_counts_in_snapshot(
        connection,
        progress.profile_id,
    )?;
    if has_in_flight_profile_effects(in_flight) {
        return Err(OperationsError::Conflict(
            "Profile provider effects remain in flight",
        ));
    }
    Ok(())
}

fn provider_receipts_complete_in(
    connection: &Connection,
    progress: &AgentProfileDeletionProgress,
) -> Result<bool, OperationsError> {
    let Some(freeze) = progress.freeze else {
        return Ok(false);
    };
    let mut statement = connection.prepare(
        "SELECT d.operation_id,d.request_id,d.profile_id,d.runtime_session_id,
                d.provider_family,d.provider_conversation_id,d.original_runtime_revision,
                d.original_runtime_state,d.state,d.attempt_count,d.deferred_reason,
                d.retry_after_ms,d.prepared_at_ms,d.updated_at_ms,d.revision,NULL,
                r.dispatch_revision,r.dispatch_action,r.outcome_kind,r.deferred_reason,
                r.retry_after_ms,r.browser_session_id,r.tab_id,r.document_id,r.observed_at_ms
         FROM ops_agent_profile_provider_deletions d
         LEFT JOIN ops_agent_profile_provider_deletion_receipts r
           ON r.operation_id=d.operation_id AND r.attempt_number=d.attempt_count
         WHERE d.request_id=?1 ORDER BY d.runtime_session_id",
    )?;
    let mut rows = statement.query(params![progress.request_id.as_bytes()])?;
    let mut count = 0_u64;
    let mut complete = true;
    while let Some(row) = rows.next()? {
        let operation = decode_provider_deletion(decode_raw_provider_deletion(row)?)?;
        if operation.profile_id != progress.profile_id {
            return Err(OperationsError::Corrupt(
                "provider deletion receipt Profile changed",
            ));
        }
        count = count
            .checked_add(1)
            .ok_or(OperationsError::Corrupt("provider deletion receipt count"))?;
        if operation.state != AgentProfileProviderDeletionState::Deleted {
            complete = false;
            continue;
        }
        if row.get::<_, Option<i64>>(16)?.is_none() {
            return Err(OperationsError::Corrupt(
                "Deleted provider target has no receipt",
            ));
        }
        let receipt = decode_provider_deletion_receipt(
            &operation,
            operation.attempt_number,
            decode_receipt_columns(row, 16)?,
        )?;
        if receipt.outcome != AgentProfileProviderDeletionReceiptOutcome::ProviderDeleteConfirmed
            || receipt.dispatch.revision.checked_add(1) != Some(operation.revision)
            || receipt.observed_at != operation.updated_at
        {
            return Err(OperationsError::Corrupt(
                "Deleted provider receipt fence changed",
            ));
        }
    }
    if count > freeze.linked_target_count {
        return Err(OperationsError::Corrupt(
            "provider deletion target count exceeds freeze",
        ));
    }
    Ok(complete && count == freeze.linked_target_count)
}

fn has_in_flight_profile_effects(counts: AgentProfileDeletionInFlightCounts) -> bool {
    counts.pending_provision_operations() != 0
        || counts.unknown_provision_operations() != 0
        || counts.observed_provision_operations() != 0
        || counts.pending_provider_messages() != 0
        || counts.unknown_provider_messages() != 0
}

fn load_provider_deletion(
    connection: &Connection,
    operation_id: Uuid,
) -> Result<Option<AgentProfileProviderDeletionOperation>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(operation_id,1,17),substr(request_id,1,17),
                    substr(profile_id,1,17),substr(runtime_session_id,1,17),
                    provider_family,CAST(substr(CAST(provider_conversation_id AS BLOB),1,513) AS TEXT),
                    original_runtime_revision,original_runtime_state,state,attempt_count,
                    deferred_reason,retry_after_ms,prepared_at_ms,updated_at_ms,revision,
                    (SELECT deferred_reason FROM ops_agent_profile_provider_deletion_receipts
                     WHERE operation_id=ops_agent_profile_provider_deletions.operation_id
                       AND outcome_kind IN(1,2)
                     ORDER BY attempt_number DESC LIMIT 1)
             FROM ops_agent_profile_provider_deletions WHERE operation_id=?1",
            params![operation_id.as_bytes()],
            decode_raw_provider_deletion,
        )
        .optional()?;
    raw.map(decode_provider_deletion).transpose()
}

fn load_provider_deletion_by_runtime(
    connection: &Connection,
    request_id: Uuid,
    runtime_id: AgentProfileRuntimeSessionId,
) -> Result<Option<AgentProfileProviderDeletionOperation>, OperationsError> {
    let operation_id = connection
        .query_row(
            "SELECT substr(operation_id,1,17)
             FROM ops_agent_profile_provider_deletions
             WHERE request_id=?1 AND runtime_session_id=?2",
            params![request_id.as_bytes(), runtime_id.into_uuid().as_bytes()],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?
        .map(|bytes| wire::id(bytes).map(Uuid::from_bytes))
        .transpose()?;
    operation_id
        .map(|id| {
            load_provider_deletion(connection, id)?.ok_or(OperationsError::Corrupt(
                "provider deletion Runtime readback",
            ))
        })
        .transpose()
}

struct RawProviderDeletion {
    operation_id: Vec<u8>,
    request_id: Vec<u8>,
    profile_id: Vec<u8>,
    runtime_session_id: Vec<u8>,
    provider_family: i64,
    provider_conversation_id: String,
    original_runtime_revision: i64,
    original_runtime_state: i64,
    state: i64,
    attempt_number: i64,
    deferred_reason: Option<i64>,
    retry_after_ms: Option<i64>,
    prepared_at_ms: i64,
    updated_at_ms: i64,
    revision: i64,
    last_deferred_reason: Option<i64>,
}

fn decode_raw_provider_deletion(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawProviderDeletion> {
    Ok(RawProviderDeletion {
        operation_id: row.get(0)?,
        request_id: row.get(1)?,
        profile_id: row.get(2)?,
        runtime_session_id: row.get(3)?,
        provider_family: row.get(4)?,
        provider_conversation_id: row.get(5)?,
        original_runtime_revision: row.get(6)?,
        original_runtime_state: row.get(7)?,
        state: row.get(8)?,
        attempt_number: row.get(9)?,
        deferred_reason: row.get(10)?,
        retry_after_ms: row.get(11)?,
        prepared_at_ms: row.get(12)?,
        updated_at_ms: row.get(13)?,
        revision: row.get(14)?,
        last_deferred_reason: row.get(15)?,
    })
}

fn decode_provider_deletion(
    raw: RawProviderDeletion,
) -> Result<AgentProfileProviderDeletionOperation, OperationsError> {
    let operation_id = Uuid::from_bytes(wire::id(raw.operation_id)?);
    let request_id = Uuid::from_bytes(wire::id(raw.request_id)?);
    let profile_id = decode_profile_id(raw.profile_id, "provider deletion Profile")?;
    let runtime_session_id = AgentProfileRuntimeSessionId::from_uuid(Uuid::from_bytes(wire::id(
        raw.runtime_session_id,
    )?))
    .map_err(|_| OperationsError::Corrupt("provider deletion Runtime Session ID"))?;
    let provider_family = decode_profile_provider_family(raw.provider_family)?;
    validate_provider_conversation_id(&raw.provider_conversation_id)?;
    let original_runtime_revision =
        AgentProfileRuntimeSessionRevision::new(raw.original_runtime_revision)
            .map_err(|_| OperationsError::Corrupt("provider deletion original Runtime revision"))?;
    let original_runtime_state = decode_runtime_state(raw.original_runtime_state)?;
    let state = decode_provider_deletion_state(raw.state)?;
    let attempt_number = u64::try_from(raw.attempt_number)
        .map_err(|_| OperationsError::Corrupt("provider deletion attempt"))?;
    let deferred_reason = raw
        .deferred_reason
        .map(decode_deferred_reason)
        .transpose()?;
    let last_deferred_reason = raw
        .last_deferred_reason
        .map(decode_deferred_reason)
        .transpose()?;
    let retry_after = raw
        .retry_after_ms
        .map(|value| decode_timestamp(value, "provider deletion retry timestamp"))
        .transpose()?;
    let prepared_at = decode_timestamp(raw.prepared_at_ms, "provider deletion prepared timestamp")?;
    let updated_at = decode_timestamp(raw.updated_at_ms, "provider deletion updated timestamp")?;
    let revision = decode_positive_counter(raw.revision, "provider deletion revision")?;
    if updated_at < prepared_at
        || (state == AgentProfileProviderDeletionState::Prepared) != (attempt_number == 0)
        || (state == AgentProfileProviderDeletionState::Deferred)
            != (deferred_reason.is_some() && retry_after.is_some())
        || (state == AgentProfileProviderDeletionState::Deferred
            && deferred_reason != last_deferred_reason)
        || derived_provider_deletion_operation_id(request_id, runtime_session_id) != operation_id
    {
        return Err(OperationsError::Corrupt("provider deletion row shape"));
    }
    Ok(AgentProfileProviderDeletionOperation {
        operation_id,
        request_id,
        profile_id,
        runtime_session_id,
        provider_family,
        provider_conversation_id: raw.provider_conversation_id,
        original_runtime_revision,
        original_runtime_state,
        state,
        attempt_number,
        deferred_reason,
        last_deferred_reason,
        retry_after,
        prepared_at,
        updated_at,
        revision,
    })
}

fn validate_provider_deletion_target(
    operation: &AgentProfileProviderDeletionOperation,
    manifest: AgentProfileDeletionManifest,
    entry: &AgentProfileDeletionRuntimeEntry,
    conversation_id: &str,
) -> Result<(), OperationsError> {
    if operation.request_id != manifest.request_id()
        || operation.profile_id != manifest.profile_id()
        || operation.runtime_session_id != entry.runtime_session_id()
        || operation.provider_family != entry.provider_family()
        || operation.provider_conversation_id != conversation_id
        || operation.original_runtime_revision != entry.original_revision()
        || operation.original_runtime_state != entry.original_state()
        || operation.operation_id
            != derived_provider_deletion_operation_id(
                manifest.request_id(),
                entry.runtime_session_id(),
            )
    {
        return Err(OperationsError::Conflict(
            "provider deletion immutable target replay",
        ));
    }
    Ok(())
}

fn in_flight_provider_deletion_ids(connection: &Connection) -> Result<Vec<Uuid>, OperationsError> {
    let mut statement = connection.prepare(
        "SELECT substr(operation_id,1,17) FROM ops_agent_profile_provider_deletions
         WHERE state IN(2,3) ORDER BY updated_at_ms,operation_id LIMIT 2",
    )?;
    let rows = statement.query_map([], |row| row.get::<_, Vec<u8>>(0))?;
    rows.map(|row| wire::id(row?).map(Uuid::from_bytes))
        .collect()
}

fn deletion_in_flight_exists(
    connection: &Connection,
    except_operation_id: Option<Uuid>,
) -> Result<bool, OperationsError> {
    let except = except_operation_id.map(|id| id.as_bytes().to_vec());
    let exists: bool = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM ops_agent_profile_provider_deletions
             WHERE state IN(2,3) AND (?1 IS NULL OR operation_id!=?1)
         )",
        params![except],
        |row| row.get(0),
    )?;
    Ok(exists)
}

fn deletion_dispatch(
    operation: &AgentProfileProviderDeletionOperation,
    action: AgentProfileProviderDeletionAction,
) -> AgentProfileProviderDeletionDispatch {
    AgentProfileProviderDeletionDispatch {
        operation_id: operation.operation_id,
        request_id: operation.request_id,
        profile_id: operation.profile_id,
        runtime_session_id: operation.runtime_session_id,
        provider_family: operation.provider_family,
        provider_conversation_id: operation.provider_conversation_id.clone(),
        attempt_number: operation.attempt_number,
        revision: operation.revision,
        action,
    }
}

fn release_provider_delete_execute(
    transaction: &Transaction<'_>,
    existing: &AgentProfileProviderDeletionOperation,
    observed_at: TimestampMillis,
) -> Result<
    (
        AgentProfileProviderDeletionOperation,
        AgentProfileProviderDeletionDispatch,
    ),
    OperationsError,
> {
    if !matches!(
        existing.state,
        AgentProfileProviderDeletionState::Prepared | AgentProfileProviderDeletionState::Deferred
    ) {
        return Err(OperationsError::Conflict("provider deletion Execute state"));
    }
    if observed_at < existing.updated_at {
        return Err(OperationsError::Conflict("provider deletion Execute clock"));
    }
    if existing.state == AgentProfileProviderDeletionState::Deferred
        && existing
            .retry_after
            .is_some_and(|retry_after| observed_at < retry_after)
    {
        return Err(OperationsError::Conflict(
            "provider deletion retry is not due",
        ));
    }
    require_profile_deletion_delete_phase(transaction, existing)?;
    if deletion_in_flight_exists(transaction, Some(existing.operation_id))? {
        return Err(OperationsError::Conflict(
            "another provider deletion attempt is in flight",
        ));
    }
    let counts = profile_deletion::profile_deletion_in_flight_counts_in_snapshot(
        transaction,
        existing.profile_id,
    )?;
    if has_in_flight_profile_effects(counts) {
        return Err(OperationsError::Conflict(
            "Profile provider effects remain in flight",
        ));
    }
    let attempt_number = next_progress_revision(existing.attempt_number)?;
    let revision = next_progress_revision(existing.revision)?;
    let changed = transaction.execute(
        "UPDATE ops_agent_profile_provider_deletions
         SET state=2,attempt_count=?1,deferred_reason=NULL,retry_after_ms=NULL,
             updated_at_ms=?2,revision=?3
         WHERE operation_id=?4 AND revision=?5 AND attempt_count=?6 AND state=?7",
        params![
            signed_progress_revision(attempt_number)?,
            observed_at.get(),
            signed_progress_revision(revision)?,
            existing.operation_id.as_bytes(),
            signed_progress_revision(existing.revision)?,
            signed_progress_revision(existing.attempt_number)?,
            encode_provider_deletion_state(existing.state),
        ],
    )?;
    if changed != 1 {
        return Err(OperationsError::Conflict("provider deletion Execute CAS"));
    }
    let operation = load_provider_deletion(transaction, existing.operation_id)?.ok_or(
        OperationsError::Corrupt("provider deletion Execute readback"),
    )?;
    let dispatch = deletion_dispatch(&operation, AgentProfileProviderDeletionAction::Execute);
    Ok((operation, dispatch))
}

fn require_profile_deletion_delete_phase(
    connection: &Connection,
    operation: &AgentProfileProviderDeletionOperation,
) -> Result<(), OperationsError> {
    let progress = load_progress(connection, operation.request_id)?.ok_or(
        OperationsError::Conflict("Profile deletion progress missing"),
    )?;
    if progress.phase != AgentProfileDeletionProgressPhase::DeleteTargets
        || progress.profile_id != operation.profile_id
        || progress.freeze.is_none()
    {
        return Err(OperationsError::Conflict("provider deletion phase fence"));
    }
    require_progress_barrier(connection, &progress)
}

fn dispatch_matches_operation_target(
    dispatch: &AgentProfileProviderDeletionDispatch,
    operation: &AgentProfileProviderDeletionOperation,
) -> bool {
    dispatch.operation_id == operation.operation_id
        && dispatch.request_id == operation.request_id
        && dispatch.profile_id == operation.profile_id
        && dispatch.runtime_session_id == operation.runtime_session_id
        && dispatch.provider_family == operation.provider_family
        && dispatch.provider_conversation_id == operation.provider_conversation_id
}

fn provider_deletion_receipts_equal(
    left: &AgentProfileProviderDeletionReceipt,
    right: &AgentProfileProviderDeletionReceipt,
) -> bool {
    left == right
}

fn insert_provider_deletion_receipt(
    transaction: &Transaction<'_>,
    receipt: &AgentProfileProviderDeletionReceipt,
    outcome_kind: i64,
) -> Result<(), OperationsError> {
    let (reason, retry_after) = match receipt.outcome {
        AgentProfileProviderDeletionReceiptOutcome::NotStartedBeforeClick {
            reason,
            retry_after,
        }
        | AgentProfileProviderDeletionReceiptOutcome::PositiveNoEffectReadback {
            reason,
            retry_after,
        } => (
            Some(encode_deferred_reason(reason)),
            Some(retry_after.get()),
        ),
        AgentProfileProviderDeletionReceiptOutcome::ProviderDeleteConfirmed => (None, None),
    };
    transaction.execute(
        "INSERT INTO ops_agent_profile_provider_deletion_receipts(
            operation_id,attempt_number,dispatch_revision,dispatch_action,outcome_kind,
            deferred_reason,retry_after_ms,browser_session_id,tab_id,document_id,observed_at_ms
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
        params![
            receipt.dispatch.operation_id.as_bytes(),
            signed_progress_revision(receipt.dispatch.attempt_number)?,
            signed_progress_revision(receipt.dispatch.revision)?,
            encode_provider_deletion_action(receipt.dispatch.action),
            outcome_kind,
            reason,
            retry_after,
            receipt.browser_session_id.as_deref(),
            receipt.tab_id.as_deref(),
            receipt.document_id.as_deref(),
            receipt.observed_at.get(),
        ],
    )?;
    Ok(())
}

fn load_provider_deletion_receipt(
    connection: &Connection,
    operation_id: Uuid,
    attempt_number: u64,
) -> Result<Option<AgentProfileProviderDeletionReceipt>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT dispatch_revision,dispatch_action,outcome_kind,deferred_reason,
                    retry_after_ms,browser_session_id,tab_id,document_id,observed_at_ms
             FROM ops_agent_profile_provider_deletion_receipts
             WHERE operation_id=?1 AND attempt_number=?2",
            params![
                operation_id.as_bytes(),
                signed_progress_revision(attempt_number)?
            ],
            decode_raw_provider_deletion_receipt,
        )
        .optional()?;
    raw.map(|raw| {
        let operation = load_provider_deletion(connection, operation_id)?.ok_or(
            OperationsError::Corrupt("provider deletion receipt target missing"),
        )?;
        decode_provider_deletion_receipt(&operation, attempt_number, raw)
    })
    .transpose()
}

struct RawProviderDeletionReceipt {
    dispatch_revision: i64,
    dispatch_action: i64,
    outcome_kind: i64,
    deferred_reason: Option<i64>,
    retry_after_ms: Option<i64>,
    browser_session_id: Option<String>,
    tab_id: Option<String>,
    document_id: Option<String>,
    observed_at_ms: i64,
}

fn decode_raw_provider_deletion_receipt(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<RawProviderDeletionReceipt> {
    decode_receipt_columns(row, 0)
}

fn decode_receipt_columns(
    row: &rusqlite::Row<'_>,
    offset: usize,
) -> rusqlite::Result<RawProviderDeletionReceipt> {
    Ok(RawProviderDeletionReceipt {
        dispatch_revision: row.get(offset)?,
        dispatch_action: row.get(offset + 1)?,
        outcome_kind: row.get(offset + 2)?,
        deferred_reason: row.get(offset + 3)?,
        retry_after_ms: row.get(offset + 4)?,
        browser_session_id: row.get(offset + 5)?,
        tab_id: row.get(offset + 6)?,
        document_id: row.get(offset + 7)?,
        observed_at_ms: row.get(offset + 8)?,
    })
}

fn decode_provider_deletion_receipt(
    operation: &AgentProfileProviderDeletionOperation,
    attempt_number: u64,
    raw: RawProviderDeletionReceipt,
) -> Result<AgentProfileProviderDeletionReceipt, OperationsError> {
    let dispatch_revision = decode_positive_counter(
        raw.dispatch_revision,
        "provider deletion receipt dispatch revision",
    )?;
    let action = decode_provider_deletion_action(raw.dispatch_action)?;
    if attempt_number == 0
        || attempt_number > operation.attempt_number
        || dispatch_revision > operation.revision
    {
        return Err(OperationsError::Corrupt("provider deletion receipt fence"));
    }
    let outcome = match raw.outcome_kind {
        RECEIPT_NOT_STARTED_BEFORE_CLICK | RECEIPT_POSITIVE_NO_EFFECT_READBACK => {
            let reason = decode_deferred_reason(
                raw.deferred_reason
                    .ok_or(OperationsError::Corrupt("provider deletion receipt reason"))?,
            )?;
            let retry_after = decode_timestamp(
                raw.retry_after_ms.ok_or(OperationsError::Corrupt(
                    "provider deletion receipt retry timestamp",
                ))?,
                "provider deletion receipt retry timestamp",
            )?;
            if raw.outcome_kind == RECEIPT_NOT_STARTED_BEFORE_CLICK {
                AgentProfileProviderDeletionReceiptOutcome::NotStartedBeforeClick {
                    reason,
                    retry_after,
                }
            } else {
                AgentProfileProviderDeletionReceiptOutcome::PositiveNoEffectReadback {
                    reason,
                    retry_after,
                }
            }
        }
        RECEIPT_PROVIDER_DELETE_CONFIRMED => {
            if raw.deferred_reason.is_some() || raw.retry_after_ms.is_some() {
                return Err(OperationsError::Corrupt(
                    "provider deletion receipt terminal shape",
                ));
            }
            AgentProfileProviderDeletionReceiptOutcome::ProviderDeleteConfirmed
        }
        _ => {
            return Err(OperationsError::Corrupt(
                "provider deletion receipt outcome",
            ));
        }
    };
    validate_optional_browser_identity(raw.browser_session_id.as_deref(), "browser session")?;
    validate_optional_browser_identity(raw.tab_id.as_deref(), "browser tab")?;
    validate_optional_browser_identity(raw.document_id.as_deref(), "browser document")?;
    let requires_exact_browser_target = matches!(
        raw.outcome_kind,
        RECEIPT_POSITIVE_NO_EFFECT_READBACK | RECEIPT_PROVIDER_DELETE_CONFIRMED
    );
    if raw.document_id.is_some() && raw.tab_id.is_none()
        || raw.tab_id.is_some() && raw.browser_session_id.is_none()
        || (requires_exact_browser_target
            && (raw.browser_session_id.is_none()
                || raw.tab_id.is_none()
                || raw.document_id.is_none()))
    {
        return Err(OperationsError::Corrupt(
            "provider deletion browser fence shape",
        ));
    }
    let dispatch = AgentProfileProviderDeletionDispatch {
        operation_id: operation.operation_id,
        request_id: operation.request_id,
        profile_id: operation.profile_id,
        runtime_session_id: operation.runtime_session_id,
        provider_family: operation.provider_family,
        provider_conversation_id: operation.provider_conversation_id.clone(),
        attempt_number,
        revision: dispatch_revision,
        action,
    };
    Ok(AgentProfileProviderDeletionReceipt {
        dispatch,
        outcome,
        browser_session_id: raw.browser_session_id,
        tab_id: raw.tab_id,
        document_id: raw.document_id,
        observed_at: decode_timestamp(
            raw.observed_at_ms,
            "provider deletion receipt observed timestamp",
        )?,
    })
}

fn commit_provider_deletion(
    transaction: Transaction<'_>,
    operation_id: Uuid,
    attempt_number: Option<u64>,
) -> Result<(), OperationsError> {
    transaction.commit().map_err(|source| {
        OperationsError::AgentProfileProviderDeletionCommitUnconfirmed {
            operation_id,
            attempt_number,
            source,
        }
    })
}

fn validate_reference_page(limit: u32) -> Result<(), OperationsError> {
    if limit == 0 || limit > MAX_AGENT_PROFILE_DELETION_PAGE_SIZE {
        return Err(OperationsError::Bounds("Profile deletion reference page"));
    }
    Ok(())
}

fn validate_uuid(value: Uuid, field: &'static str) -> Result<(), OperationsError> {
    if value.is_nil() {
        Err(OperationsError::Conflict(field))
    } else {
        Ok(())
    }
}

fn validate_provider_conversation_id(value: &str) -> Result<(), OperationsError> {
    if value.is_empty() || value.len() > MAX_PROVIDER_ID_BYTES {
        return Err(OperationsError::Bounds("provider conversation identity"));
    }
    Ok(())
}

fn validate_optional_browser_identity(
    value: Option<&str>,
    field: &'static str,
) -> Result<(), OperationsError> {
    if value.is_some_and(|value| value.is_empty() || value.len() > MAX_PROVIDER_ID_BYTES) {
        return Err(OperationsError::Bounds(field));
    }
    Ok(())
}

fn derived_provider_deletion_operation_id(
    request_id: Uuid,
    runtime_id: AgentProfileRuntimeSessionId,
) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(PROFILE_PROVIDER_DELETION_OPERATION_DOMAIN);
    hasher.update(request_id.as_bytes());
    hasher.update(runtime_id.into_uuid().as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn require_monotonic_uuid_cursor(
    previous: Option<Uuid>,
    next: Option<Uuid>,
    field: &'static str,
) -> Result<(), OperationsError> {
    if next.is_some_and(|value| value.is_nil())
        || previous
            .zip(next)
            .is_some_and(|(previous, next)| next <= previous)
    {
        return Err(OperationsError::Conflict(field));
    }
    Ok(())
}

fn require_monotonic_runtime_cursor(
    previous: Option<AgentProfileRuntimeSessionId>,
    next: Option<AgentProfileRuntimeSessionId>,
) -> Result<(), OperationsError> {
    if next.is_some_and(|value| value.into_uuid().is_nil())
        || previous
            .zip(next)
            .is_some_and(|(previous, next)| next.into_uuid() <= previous.into_uuid())
    {
        return Err(OperationsError::Conflict("Profile deletion Runtime cursor"));
    }
    Ok(())
}

fn next_progress_revision(current: u64) -> Result<u64, OperationsError> {
    current
        .checked_add(1)
        .ok_or(OperationsError::Bounds("Profile deletion revision"))
}

fn signed_progress_revision(value: u64) -> Result<i64, OperationsError> {
    i64::try_from(value).map_err(|_| OperationsError::Bounds("Profile deletion revision"))
}

fn signed_count(value: u64) -> Result<i64, OperationsError> {
    i64::try_from(value).map_err(|_| OperationsError::Bounds("Profile deletion count"))
}

fn counter(value: i64, field: &'static str) -> Result<u64, OperationsError> {
    u64::try_from(value).map_err(|_| OperationsError::Corrupt(field))
}

fn decode_positive_counter(value: i64, field: &'static str) -> Result<u64, OperationsError> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(OperationsError::Corrupt(field))
}

fn decode_timestamp(value: i64, field: &'static str) -> Result<TimestampMillis, OperationsError> {
    TimestampMillis::new(value).map_err(|_| OperationsError::Corrupt(field))
}

fn decode_profile_id(
    bytes: Vec<u8>,
    field: &'static str,
) -> Result<AgentProfileId, OperationsError> {
    AgentProfileId::from_uuid(Uuid::from_bytes(wire::id(bytes)?))
        .map_err(|_| OperationsError::Corrupt(field))
}

fn decode_profile_revision(
    value: i64,
    field: &'static str,
) -> Result<AgentProfileRevision, OperationsError> {
    AgentProfileRevision::new(value).map_err(|_| OperationsError::Corrupt(field))
}

const fn encode_progress_phase(value: AgentProfileDeletionProgressPhase) -> i64 {
    match value {
        AgentProfileDeletionProgressPhase::AuditProvision => 1,
        AgentProfileDeletionProgressPhase::AuditMessages => 2,
        AgentProfileDeletionProgressPhase::FreezeTargets => 3,
        AgentProfileDeletionProgressPhase::PrepareDeletes => 4,
        AgentProfileDeletionProgressPhase::DeleteTargets => 5,
        AgentProfileDeletionProgressPhase::Finalize => 6,
        AgentProfileDeletionProgressPhase::Completed => 7,
    }
}

fn decode_progress_phase(value: i64) -> Result<AgentProfileDeletionProgressPhase, OperationsError> {
    match value {
        1 => Ok(AgentProfileDeletionProgressPhase::AuditProvision),
        2 => Ok(AgentProfileDeletionProgressPhase::AuditMessages),
        3 => Ok(AgentProfileDeletionProgressPhase::FreezeTargets),
        4 => Ok(AgentProfileDeletionProgressPhase::PrepareDeletes),
        5 => Ok(AgentProfileDeletionProgressPhase::DeleteTargets),
        6 => Ok(AgentProfileDeletionProgressPhase::Finalize),
        7 => Ok(AgentProfileDeletionProgressPhase::Completed),
        _ => Err(OperationsError::Corrupt("Profile deletion progress phase")),
    }
}

const fn encode_progress_deferred_reason(value: AgentProfileDeletionProgressDeferredReason) -> i64 {
    match value {
        AgentProfileDeletionProgressDeferredReason::HistoryMismatch => 1,
        AgentProfileDeletionProgressDeferredReason::CatalogUnavailable => 2,
        AgentProfileDeletionProgressDeferredReason::ManifestMismatch => 3,
    }
}

fn decode_progress_deferred_reason(
    value: i64,
) -> Result<AgentProfileDeletionProgressDeferredReason, OperationsError> {
    match value {
        1 => Ok(AgentProfileDeletionProgressDeferredReason::HistoryMismatch),
        2 => Ok(AgentProfileDeletionProgressDeferredReason::CatalogUnavailable),
        3 => Ok(AgentProfileDeletionProgressDeferredReason::ManifestMismatch),
        _ => Err(OperationsError::Corrupt(
            "Profile deletion progress deferred reason",
        )),
    }
}

const fn encode_deferred_reason(value: AgentProfileProviderDeletionDeferredReason) -> i64 {
    match value {
        AgentProfileProviderDeletionDeferredReason::ProviderBusy => 1,
        AgentProfileProviderDeletionDeferredReason::AuthenticationRequired => 2,
        AgentProfileProviderDeletionDeferredReason::SurfaceUnavailable => 3,
        AgentProfileProviderDeletionDeferredReason::ActionUnavailable => 4,
    }
}

fn decode_deferred_reason(
    value: i64,
) -> Result<AgentProfileProviderDeletionDeferredReason, OperationsError> {
    match value {
        1 => Ok(AgentProfileProviderDeletionDeferredReason::ProviderBusy),
        2 => Ok(AgentProfileProviderDeletionDeferredReason::AuthenticationRequired),
        3 => Ok(AgentProfileProviderDeletionDeferredReason::SurfaceUnavailable),
        4 => Ok(AgentProfileProviderDeletionDeferredReason::ActionUnavailable),
        _ => Err(OperationsError::Corrupt(
            "provider deletion deferred reason",
        )),
    }
}

const fn encode_provider_deletion_state(value: AgentProfileProviderDeletionState) -> i64 {
    match value {
        AgentProfileProviderDeletionState::Prepared => 1,
        AgentProfileProviderDeletionState::EffectPending => 2,
        AgentProfileProviderDeletionState::EffectUnknown => 3,
        AgentProfileProviderDeletionState::Deferred => 4,
        AgentProfileProviderDeletionState::Deleted => 5,
    }
}

fn decode_provider_deletion_state(
    value: i64,
) -> Result<AgentProfileProviderDeletionState, OperationsError> {
    match value {
        1 => Ok(AgentProfileProviderDeletionState::Prepared),
        2 => Ok(AgentProfileProviderDeletionState::EffectPending),
        3 => Ok(AgentProfileProviderDeletionState::EffectUnknown),
        4 => Ok(AgentProfileProviderDeletionState::Deferred),
        5 => Ok(AgentProfileProviderDeletionState::Deleted),
        _ => Err(OperationsError::Corrupt("provider deletion state")),
    }
}

const fn encode_provider_deletion_action(value: AgentProfileProviderDeletionAction) -> i64 {
    match value {
        AgentProfileProviderDeletionAction::Execute => 1,
        AgentProfileProviderDeletionAction::Inspect => 2,
    }
}

fn decode_provider_deletion_action(
    value: i64,
) -> Result<AgentProfileProviderDeletionAction, OperationsError> {
    match value {
        1 => Ok(AgentProfileProviderDeletionAction::Execute),
        2 => Ok(AgentProfileProviderDeletionAction::Inspect),
        _ => Err(OperationsError::Corrupt(
            "provider deletion dispatch action",
        )),
    }
}

fn decode_profile_provider_family(
    value: i64,
) -> Result<AgentProfileProviderFamily, OperationsError> {
    match value {
        1 => Ok(AgentProfileProviderFamily::ChatGpt),
        _ => Err(OperationsError::Corrupt(
            "provider deletion provider family",
        )),
    }
}

const fn encode_runtime_state(value: AgentProfileRuntimeSessionState) -> i64 {
    match value {
        AgentProfileRuntimeSessionState::Provisioning => 1,
        AgentProfileRuntimeSessionState::Ready => 2,
        AgentProfileRuntimeSessionState::Active => 3,
        AgentProfileRuntimeSessionState::Suspending => 4,
        AgentProfileRuntimeSessionState::Offline => 5,
        AgentProfileRuntimeSessionState::Waking => 6,
        AgentProfileRuntimeSessionState::Failed => 7,
        AgentProfileRuntimeSessionState::Closed => 8,
    }
}

fn decode_runtime_state(value: i64) -> Result<AgentProfileRuntimeSessionState, OperationsError> {
    match value {
        1 => Ok(AgentProfileRuntimeSessionState::Provisioning),
        2 => Ok(AgentProfileRuntimeSessionState::Ready),
        3 => Ok(AgentProfileRuntimeSessionState::Active),
        4 => Ok(AgentProfileRuntimeSessionState::Suspending),
        5 => Ok(AgentProfileRuntimeSessionState::Offline),
        6 => Ok(AgentProfileRuntimeSessionState::Waking),
        7 => Ok(AgentProfileRuntimeSessionState::Failed),
        8 => Ok(AgentProfileRuntimeSessionState::Closed),
        _ => Err(OperationsError::Corrupt(
            "provider deletion original Runtime state",
        )),
    }
}

fn decode_provision_state(value: i64) -> Result<AgentProvisionState, OperationsError> {
    match value {
        1 => Ok(AgentProvisionState::Prepared),
        2 => Ok(AgentProvisionState::EffectPending),
        3 => Ok(AgentProvisionState::EffectUnknown),
        4 => Ok(AgentProvisionState::Observed),
        5 => Ok(AgentProvisionState::Confirmed),
        6 => Ok(AgentProvisionState::Failed),
        7 => Ok(AgentProvisionState::Cancelled),
        _ => Err(OperationsError::Corrupt("Agent provision reference state")),
    }
}

fn decode_provision_effect_kind(value: i64) -> Result<AgentProvisionEffectKind, OperationsError> {
    match value {
        1 => Ok(AgentProvisionEffectKind::WakeExistingRuntime),
        2 => Ok(AgentProvisionEffectKind::WakeNewRuntime),
        3 => Ok(AgentProvisionEffectKind::ProvisionNew),
        4 => Ok(AgentProvisionEffectKind::ReuseReadyRuntime),
        _ => Err(OperationsError::Corrupt(
            "Agent provision reference effect kind",
        )),
    }
}

fn decode_provision_provider_family(
    value: i64,
) -> Result<AgentProvisionProviderFamily, OperationsError> {
    match value {
        1 => Ok(AgentProvisionProviderFamily::ChatGpt),
        _ => Err(OperationsError::Corrupt(
            "Agent provision reference provider family",
        )),
    }
}

fn decode_provider_message_state(value: i64) -> Result<ProviderMessageState, OperationsError> {
    match value {
        1 => Ok(ProviderMessageState::Queued),
        2 => Ok(ProviderMessageState::EffectPending),
        3 => Ok(ProviderMessageState::Delivered),
        4 => Ok(ProviderMessageState::Failed),
        5 => Ok(ProviderMessageState::Unknown),
        _ => Err(OperationsError::Corrupt("provider message reference state")),
    }
}

fn provision_reference_page(
    connection: &Connection,
    profile_id: AgentProfileId,
    runtime_session_id: Option<AgentProfileRuntimeSessionId>,
    after_operation_id: Option<Uuid>,
    limit: u32,
) -> Result<AgentProfileDeletionProvisionReferencePage, OperationsError> {
    validate_reference_page(limit)?;
    if after_operation_id.is_some_and(|value| value.is_nil()) {
        return Err(OperationsError::Conflict("provision reference cursor"));
    }
    let runtime = runtime_session_id.map(|id| id.into_uuid().as_bytes().to_vec());
    let after = after_operation_id.map(|id| id.as_bytes().to_vec());
    let mut statement = connection.prepare(
        "SELECT substr(operation.operation_id,1,17),substr(operation.request_id,1,17),slot_ordinal,
                effect_kind,provider_family,substr(profile_id,1,17),
                substr(runtime_session_id,1,17),state,
                CAST(substr(CAST(provider_conversation_id AS BLOB),1,513) AS TEXT),
                prepared_at_ms,updated_at_ms,revision,
                project_route.project_id,project_route.binding_revision
         FROM ops_agent_provision_operations AS operation
         LEFT JOIN ops_agent_provision_project_routes AS project_route
           ON project_route.operation_id=operation.operation_id
         WHERE profile_id=?1 AND (?2 IS NULL OR runtime_session_id=?2)
           AND (?3 IS NULL OR operation.operation_id>?3)
         ORDER BY operation.operation_id LIMIT ?4",
    )?;
    let rows = statement.query_map(
        params![
            profile_id.into_uuid().as_bytes(),
            runtime,
            after,
            i64::from(limit) + 1,
        ],
        decode_raw_provision_reference,
    )?;
    let mut references = rows
        .map(|row| decode_provision_reference(row?))
        .collect::<Result<Vec<_>, OperationsError>>()?;
    let has_more = references.len() > limit as usize;
    if has_more {
        references.pop();
    }
    let next_operation_id = has_more
        .then(|| {
            references
                .last()
                .map(AgentProfileDeletionProvisionReference::operation_id)
        })
        .flatten();
    Ok(AgentProfileDeletionProvisionReferencePage {
        references,
        next_operation_id,
    })
}

fn provider_message_reference_page(
    connection: &Connection,
    profile_id: AgentProfileId,
    runtime_session_id: Option<AgentProfileRuntimeSessionId>,
    after_message_id: Option<Uuid>,
    limit: u32,
) -> Result<AgentProfileDeletionProviderMessageReferencePage, OperationsError> {
    validate_reference_page(limit)?;
    if after_message_id.is_some_and(|value| value.is_nil()) {
        return Err(OperationsError::Conflict(
            "provider message reference cursor",
        ));
    }
    let runtime = runtime_session_id.map(|id| id.into_uuid().as_bytes().to_vec());
    let after = after_message_id.map(|id| id.as_bytes().to_vec());
    let mut statement = connection.prepare(
        "SELECT substr(outbox.message_id,1,17),substr(outbox.workspace_id,1,17),
                substr(outbox.recipient_profile_id,1,17),
                substr(outbox.recipient_participation_id,1,17),outbox.provider_family,
                substr(outbox.runtime_session_id,1,17),
                CAST(substr(CAST(outbox.provider_conversation_id AS BLOB),1,513) AS TEXT),
                outbox.state,outbox.queued_at_ms,outbox.updated_at_ms,outbox.revision,
                project_route.project_id,project_route.binding_revision
         FROM ops_provider_message_outbox AS outbox
         LEFT JOIN ops_provider_message_project_routes AS project_route
           ON project_route.message_id=outbox.message_id
         WHERE outbox.recipient_profile_id=?1
           AND (?2 IS NULL OR outbox.runtime_session_id=?2)
           AND (?3 IS NULL OR outbox.message_id>?3)
         ORDER BY outbox.message_id LIMIT ?4",
    )?;
    let rows = statement.query_map(
        params![
            profile_id.into_uuid().as_bytes(),
            runtime,
            after,
            i64::from(limit) + 1,
        ],
        decode_raw_provider_message_reference,
    )?;
    let mut references = rows
        .map(|row| decode_provider_message_reference(row?))
        .collect::<Result<Vec<_>, OperationsError>>()?;
    let has_more = references.len() > limit as usize;
    if has_more {
        references.pop();
    }
    let next_message_id = has_more
        .then(|| {
            references
                .last()
                .map(AgentProfileDeletionProviderMessageReference::message_id)
        })
        .flatten();
    Ok(AgentProfileDeletionProviderMessageReferencePage {
        references,
        next_message_id,
    })
}

struct RawProvisionReference {
    operation_id: Vec<u8>,
    request_id: Vec<u8>,
    slot_ordinal: i64,
    effect_kind: i64,
    provider_family: i64,
    profile_id: Vec<u8>,
    runtime_session_id: Option<Vec<u8>>,
    state: i64,
    provider_conversation_id: Option<String>,
    prepared_at_ms: i64,
    updated_at_ms: i64,
    revision: i64,
    project_id: Option<String>,
    project_binding_revision: Option<i64>,
}

fn decode_raw_provision_reference(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<RawProvisionReference> {
    Ok(RawProvisionReference {
        operation_id: row.get(0)?,
        request_id: row.get(1)?,
        slot_ordinal: row.get(2)?,
        effect_kind: row.get(3)?,
        provider_family: row.get(4)?,
        profile_id: row.get(5)?,
        runtime_session_id: row.get(6)?,
        state: row.get(7)?,
        provider_conversation_id: row.get(8)?,
        prepared_at_ms: row.get(9)?,
        updated_at_ms: row.get(10)?,
        revision: row.get(11)?,
        project_id: row.get(12)?,
        project_binding_revision: row.get(13)?,
    })
}

fn decode_provision_reference(
    raw: RawProvisionReference,
) -> Result<AgentProfileDeletionProvisionReference, OperationsError> {
    let slot_ordinal = u8::try_from(raw.slot_ordinal)
        .ok()
        .filter(|value| *value <= 3)
        .ok_or(OperationsError::Corrupt("Agent provision reference slot"))?;
    let runtime_session_id = raw
        .runtime_session_id
        .map(|bytes| {
            AgentProfileRuntimeSessionId::from_uuid(Uuid::from_bytes(wire::id(bytes)?))
                .map_err(|_| OperationsError::Corrupt("Agent provision reference Runtime ID"))
        })
        .transpose()?;
    if let Some(value) = raw.provider_conversation_id.as_deref() {
        validate_provider_conversation_id(value)?;
    }
    let prepared_at = decode_timestamp(
        raw.prepared_at_ms,
        "Agent provision reference prepared time",
    )?;
    let updated_at = decode_timestamp(raw.updated_at_ms, "Agent provision reference updated time")?;
    let revision = decode_positive_counter(raw.revision, "Agent provision reference revision")?;
    if updated_at < prepared_at {
        return Err(OperationsError::Corrupt("Agent provision reference clock"));
    }
    let project = decode_project_reference(
        raw.project_id,
        raw.project_binding_revision,
        "Agent provision reference Project",
    )?;
    Ok(AgentProfileDeletionProvisionReference {
        operation_id: Uuid::from_bytes(wire::id(raw.operation_id)?),
        request_id: Uuid::from_bytes(wire::id(raw.request_id)?),
        slot_ordinal,
        effect_kind: decode_provision_effect_kind(raw.effect_kind)?,
        provider_family: decode_provision_provider_family(raw.provider_family)?,
        profile_id: decode_profile_id(raw.profile_id, "Agent provision reference Profile ID")?,
        runtime_session_id,
        state: decode_provision_state(raw.state)?,
        provider_conversation_id: raw.provider_conversation_id,
        project,
        prepared_at,
        updated_at,
        revision,
    })
}

struct RawProviderMessageReference {
    message_id: Vec<u8>,
    workspace_id: Vec<u8>,
    profile_id: Vec<u8>,
    participation_id: Vec<u8>,
    provider_family: i64,
    runtime_session_id: Vec<u8>,
    provider_conversation_id: String,
    state: i64,
    queued_at_ms: i64,
    updated_at_ms: i64,
    revision: i64,
    project_id: Option<String>,
    project_binding_revision: Option<i64>,
}

fn decode_raw_provider_message_reference(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<RawProviderMessageReference> {
    Ok(RawProviderMessageReference {
        message_id: row.get(0)?,
        workspace_id: row.get(1)?,
        profile_id: row.get(2)?,
        participation_id: row.get(3)?,
        provider_family: row.get(4)?,
        runtime_session_id: row.get(5)?,
        provider_conversation_id: row.get(6)?,
        state: row.get(7)?,
        queued_at_ms: row.get(8)?,
        updated_at_ms: row.get(9)?,
        revision: row.get(10)?,
        project_id: row.get(11)?,
        project_binding_revision: row.get(12)?,
    })
}

fn decode_provider_message_reference(
    raw: RawProviderMessageReference,
) -> Result<AgentProfileDeletionProviderMessageReference, OperationsError> {
    let profile_id = decode_profile_id(raw.profile_id, "provider message reference Profile ID")?;
    let runtime_session_id = AgentProfileRuntimeSessionId::from_uuid(Uuid::from_bytes(wire::id(
        raw.runtime_session_id,
    )?))
    .map_err(|_| OperationsError::Corrupt("provider message reference Runtime ID"))?;
    validate_provider_conversation_id(&raw.provider_conversation_id)?;
    let project = decode_project_reference(
        raw.project_id,
        raw.project_binding_revision,
        "provider message reference Project",
    )?;
    let target = ProviderMessageTarget::new_with_project_route(
        WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(raw.workspace_id)?)),
        profile_id,
        WorkspaceParticipationId::from_uuid(Uuid::from_bytes(wire::id(raw.participation_id)?))?,
        AgentProfileProviderFamily::ChatGpt,
        runtime_session_id,
        raw.provider_conversation_id,
        project,
    )?;
    let queued_at = decode_timestamp(raw.queued_at_ms, "provider message reference queued time")?;
    let updated_at =
        decode_timestamp(raw.updated_at_ms, "provider message reference updated time")?;
    let revision = decode_positive_counter(raw.revision, "provider message reference revision")?;
    if updated_at < queued_at || raw.provider_family != 1 {
        return Err(OperationsError::Corrupt(
            "provider message reference row shape",
        ));
    }
    Ok(AgentProfileDeletionProviderMessageReference {
        message_id: Uuid::from_bytes(wire::id(raw.message_id)?),
        target,
        state: decode_provider_message_state(raw.state)?,
        queued_at,
        updated_at,
        revision,
    })
}

fn decode_project_reference(
    project_id: Option<String>,
    binding_revision: Option<i64>,
    description: &'static str,
) -> Result<Option<WorkspaceProjectReference>, OperationsError> {
    match (project_id, binding_revision) {
        (None, None) => Ok(None),
        (Some(project_id), Some(binding_revision)) => {
            let project_id = ChatGptProjectId::try_from(project_id.as_str())
                .map_err(|_| OperationsError::Corrupt(description))?;
            let binding_revision = WorkspaceProjectBindingRevision::new(binding_revision)
                .map_err(|_| OperationsError::Corrupt("provider route Project revision"))?;
            Ok(Some(WorkspaceProjectReference::new(
                project_id,
                binding_revision,
            )))
        }
        _ => Err(OperationsError::Corrupt("provider route Project shape")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{error::Error, fs, path::PathBuf};

    use heterarchy_orchestration_sessions::{
        AgentProfileCatalog, AgentProfileCreationOrigin, AgentProfileDeletionCommand,
        CreateAgentProfile, CreateAgentProfileRuntimeSession,
    };
    use rusqlite::params;

    struct Fixture {
        root: PathBuf,
        operations: OperationsCatalog,
        profiles: heterarchy_orchestration_sessions::AgentProfileCatalog,
        profile_id: AgentProfileId,
        intent: AgentProfileDeletionIntent,
        manifest: AgentProfileDeletionManifest,
        entries: Vec<AgentProfileDeletionRuntimeEntry>,
    }

    impl Fixture {
        fn open() -> Result<Self, Box<dyn Error>> {
            let root = std::env::temp_dir().join(format!("provider-delete-ops-{}", Uuid::now_v7()));
            fs::create_dir(&root)?;
            let profile_path = root.join("profiles.sqlite3");
            let operations_path = root.join("operations.sqlite3");
            let profile_id = AgentProfileId::from_uuid(Uuid::now_v7())?;
            let mut profiles = AgentProfileCatalog::open(&profile_path)?;
            let create = CreateAgentProfile::new(
                profile_id,
                Uuid::now_v7(),
                "provider-delete-fixture".to_owned(),
                "Provider deletion fixture".to_owned(),
                AgentProfileProviderFamily::ChatGpt,
                None,
                None,
                Vec::new(),
                AgentProfileCreationOrigin::UserManual,
                time(1)?,
            )?;
            let profile = profiles.create_profile(&create)?.profile().clone();
            let linked_runtime_id = AgentProfileRuntimeSessionId::from_uuid(Uuid::now_v7())?;
            let linked =
                profiles.create_runtime_session(&CreateAgentProfileRuntimeSession::new(
                    linked_runtime_id,
                    Uuid::now_v7(),
                    profile_id,
                    AgentProfileProviderFamily::ChatGpt,
                    time(2)?,
                )?)?;
            let linked = profiles.confirm_runtime_session_ready(
                linked_runtime_id,
                linked.session().revision(),
                "conversation-linked",
                time(3)?,
            )?;
            let no_link_runtime_id = AgentProfileRuntimeSessionId::from_uuid(Uuid::now_v7())?;
            let no_link =
                profiles.create_runtime_session(&CreateAgentProfileRuntimeSession::new(
                    no_link_runtime_id,
                    Uuid::now_v7(),
                    profile_id,
                    AgentProfileProviderFamily::ChatGpt,
                    time(4)?,
                )?)?;
            let no_link = profiles.transition_runtime_session(
                no_link_runtime_id,
                no_link.session().revision(),
                AgentProfileRuntimeSessionState::Failed,
                time(5)?,
            )?;
            assert_eq!(linked.state(), AgentProfileRuntimeSessionState::Ready);
            assert_eq!(no_link.state(), AgentProfileRuntimeSessionState::Failed);

            let intent = profiles.begin_profile_deletion(&AgentProfileDeletionCommand::new(
                Uuid::now_v7(),
                profile_id,
                profile.revision(),
                time(10)?,
            )?)?;
            let mut operations = OperationsCatalog::open(&operations_path, Uuid::now_v7())?;
            operations.begin_profile_deletion_barrier(&intent)?;
            let progress = operations.begin_agent_profile_deletion_progress(&intent)?;
            let progress = operations.advance_agent_profile_deletion_progress(
                intent.request_id(),
                progress.revision(),
                time(11)?,
                AgentProfileDeletionProgressAdvance::ProvisionReferencesPage {
                    after_operation_id: None,
                },
            )?;
            let progress = operations.advance_agent_profile_deletion_progress(
                intent.request_id(),
                progress.revision(),
                time(12)?,
                AgentProfileDeletionProgressAdvance::ProviderMessageReferencesPage {
                    after_message_id: None,
                },
            )?;
            let manifest = profiles.freeze_profile_deletion_targets(
                intent.request_id(),
                intent.barrier_profile_revision(),
                time(13)?,
            )?;
            let _ = operations.advance_agent_profile_deletion_progress(
                intent.request_id(),
                progress.revision(),
                time(14)?,
                AgentProfileDeletionProgressAdvance::TargetsFrozen(manifest),
            )?;
            let entries = profiles
                .profile_deletion_runtime_entries_page(intent.request_id(), None, 32)?
                .entries()
                .to_vec();
            Ok(Self {
                root,
                operations,
                profiles,
                profile_id,
                intent,
                manifest,
                entries,
            })
        }

        fn linked_entry(&self) -> Result<&AgentProfileDeletionRuntimeEntry, std::io::Error> {
            self.entries
                .iter()
                .find(|entry| {
                    matches!(
                        entry.target(),
                        AgentProfileDeletionRuntimeTarget::LinkedProviderConversation { .. }
                    )
                })
                .ok_or(std::io::Error::other("fixture linked entry"))
        }

        fn no_link_entry(&self) -> Result<&AgentProfileDeletionRuntimeEntry, std::io::Error> {
            self.entries
                .iter()
                .find(|entry| {
                    matches!(
                        entry.target(),
                        AgentProfileDeletionRuntimeTarget::NoLinkedTarget
                    )
                })
                .ok_or(std::io::Error::other("fixture no-link entry"))
        }

        fn prepare_targets(&mut self) -> Result<(), Box<dyn Error>> {
            for entry in self.entries.clone() {
                let _ = self
                    .operations
                    .prepare_agent_profile_provider_deletion_target(
                        self.manifest,
                        &entry,
                        time(15)?,
                    )?;
            }
            let progress = self
                .operations
                .agent_profile_deletion_progress(self.intent.request_id())?
                .ok_or("missing progress")?;
            let _ = self.operations.advance_agent_profile_deletion_progress(
                self.intent.request_id(),
                progress.revision(),
                time(16)?,
                AgentProfileDeletionProgressAdvance::RuntimeEntriesPage {
                    after_runtime_id: None,
                },
            )?;
            Ok(())
        }

        fn finish(self) -> Result<(), Box<dyn Error>> {
            let Self {
                root,
                operations,
                profiles,
                ..
            } = self;
            drop(operations);
            drop(profiles);
            fs::remove_dir_all(root)?;
            Ok(())
        }
    }

    fn time(value: i64) -> Result<TimestampMillis, Box<dyn Error>> {
        Ok(TimestampMillis::new(value)?)
    }

    fn browser_metadata() -> (Option<String>, Option<String>, Option<String>) {
        (
            Some("browser-session-fixture".to_owned()),
            Some("tab-fixture".to_owned()),
            Some("document-fixture".to_owned()),
        )
    }

    #[test]
    fn linked_target_replay_is_immutable_and_no_link_is_not_prepared() -> Result<(), Box<dyn Error>>
    {
        let mut fixture = Fixture::open()?;
        let linked = fixture.linked_entry()?.clone();
        let no_link = fixture.no_link_entry()?.clone();
        let first = fixture
            .operations
            .prepare_agent_profile_provider_deletion_target(fixture.manifest, &linked, time(15)?)?
            .ok_or("linked target did not prepare")?;
        assert!(first.created());
        assert_eq!(
            first.operation().provider_conversation_id(),
            "conversation-linked"
        );
        let replay = fixture
            .operations
            .prepare_agent_profile_provider_deletion_target(fixture.manifest, &linked, time(15)?)?
            .ok_or("exact target replay missing")?;
        assert!(!replay.created());
        assert_eq!(replay.operation(), first.operation());
        assert!(
            fixture
                .operations
                .prepare_agent_profile_provider_deletion_target(
                    fixture.manifest,
                    &no_link,
                    time(15)?,
                )?
                .is_none()
        );
        fixture.prepare_targets()?;
        let progress = fixture
            .operations
            .agent_profile_deletion_progress(fixture.intent.request_id())?
            .ok_or("progress readback missing")?;
        assert_eq!(
            progress.phase(),
            AgentProfileDeletionProgressPhase::DeleteTargets
        );
        assert_eq!(
            fixture
                .operations
                .agent_profile_provider_deletion_summary(fixture.intent.request_id())?
                .prepared(),
            1
        );
        fixture.finish()?;
        Ok(())
    }

    #[test]
    fn unknown_delete_remains_inspect_only_until_positive_no_effect_readback()
    -> Result<(), Box<dyn Error>> {
        let mut fixture = Fixture::open()?;
        fixture.prepare_targets()?;
        let execute = fixture
            .operations
            .next_agent_profile_provider_deletion_dispatch(time(20)?)?
            .ok_or("Execute missing")?;
        let unknown = fixture
            .operations
            .mark_agent_profile_provider_deletion_unknown(
                execute.operation_id(),
                execute.revision(),
                execute.attempt_number(),
                time(21)?,
            )?;
        let inspect = fixture
            .operations
            .next_agent_profile_provider_deletion_dispatch(time(22)?)?
            .ok_or("Unknown Inspect missing")?;
        assert_eq!(
            inspect.action(),
            AgentProfileProviderDeletionAction::Inspect
        );
        assert_eq!(inspect.operation_id(), unknown.operation_id());
        assert_eq!(inspect.attempt_number(), execute.attempt_number());
        assert!(
            fixture
                .operations
                .agent_profile_provider_deletion_receipt_dispatch(
                    inspect.operation_id(),
                    unknown.revision(),
                    inspect.attempt_number(),
                    AgentProfileProviderDeletionAction::Execute,
                )?
                .is_none()
        );
        let (browser, tab, document) = browser_metadata();
        let no_effect = AgentProfileProviderDeletionReceipt::confirmed_no_effect_after_readback(
            &inspect,
            AgentProfileProviderDeletionDeferredReason::ActionUnavailable,
            time(30_022)?,
            browser,
            tab,
            document,
            time(22)?,
        )?;
        assert_eq!(
            fixture
                .operations
                .record_agent_profile_provider_deletion_receipt(&no_effect)?,
            no_effect
        );
        assert_eq!(
            fixture
                .operations
                .record_agent_profile_provider_deletion_receipt(&no_effect)?,
            no_effect
        );
        assert_eq!(
            fixture
                .operations
                .agent_profile_provider_deletion_summary(fixture.intent.request_id())?
                .effect_unknown(),
            0
        );
        assert_eq!(
            fixture
                .operations
                .agent_profile_provider_deletion_summary(fixture.intent.request_id())?
                .deferred(),
            1
        );
        fixture.finish()?;
        Ok(())
    }

    fn assert_pre_click_deferral(
        fixture: &mut Fixture,
        first: &AgentProfileProviderDeletionDispatch,
        browser: Option<&str>,
        tab: Option<&str>,
        document: Option<&str>,
    ) -> Result<(), Box<dyn Error>> {
        let no_click = AgentProfileProviderDeletionReceipt::not_started_before_click(
            first,
            AgentProfileProviderDeletionDeferredReason::ProviderBusy,
            time(30_020)?,
            browser.map(str::to_owned),
            tab.map(str::to_owned),
            document.map(str::to_owned),
            time(20)?,
        )?;
        assert_eq!(
            fixture
                .operations
                .record_agent_profile_provider_deletion_receipt(&no_click)?,
            no_click
        );
        assert_eq!(
            fixture
                .operations
                .record_agent_profile_provider_deletion_receipt(&no_click)?,
            no_click
        );
        let deferred = fixture
            .operations
            .agent_profile_provider_deletion(first.operation_id())?
            .ok_or("deferred operation missing")?;
        assert_eq!(
            deferred.state(),
            AgentProfileProviderDeletionState::Deferred
        );
        assert_eq!(deferred.attempt_number(), 1);
        assert_eq!(
            deferred.last_deferred_reason(),
            Some(AgentProfileProviderDeletionDeferredReason::ProviderBusy)
        );
        assert_eq!(
            fixture
                .operations
                .agent_profile_provider_deletion_summary(fixture.intent.request_id())?
                .latest_deferred_reason(),
            Some(AgentProfileProviderDeletionDeferredReason::ProviderBusy)
        );
        assert!(
            fixture
                .operations
                .next_agent_profile_provider_deletion_dispatch(time(30_019)?)?
                .is_none()
        );
        Ok(())
    }

    fn confirm_retry_and_finalize(
        fixture: &mut Fixture,
        operation_id: Uuid,
        browser: Option<String>,
        tab: Option<String>,
        document: Option<String>,
    ) -> Result<(), Box<dyn Error>> {
        let second = fixture
            .operations
            .next_agent_profile_provider_deletion_dispatch(time(30_021)?)?
            .ok_or("retry Execute missing")?;
        assert_eq!(second.attempt_number(), 2);
        let deleted =
            AgentProfileProviderDeletionReceipt::deleted_after_positive_provider_confirmation(
                &second,
                browser,
                tab,
                document,
                time(30_022)?,
            )?;
        assert_eq!(
            fixture
                .operations
                .record_agent_profile_provider_deletion_receipt(&deleted)?,
            deleted
        );
        assert_eq!(
            fixture
                .operations
                .record_agent_profile_provider_deletion_receipt(&deleted)?,
            deleted
        );
        assert_eq!(
            fixture
                .operations
                .agent_profile_provider_deletion(operation_id)?
                .ok_or("deleted operation missing")?
                .state(),
            AgentProfileProviderDeletionState::Deleted
        );
        let summary = fixture
            .operations
            .agent_profile_provider_deletion_summary(fixture.intent.request_id())?;
        assert_eq!(summary.deleted(), 1);
        assert_eq!(summary.latest_deferred_reason(), None);
        let progress = fixture
            .operations
            .agent_profile_deletion_progress(fixture.intent.request_id())?
            .ok_or("progress readback missing")?;
        let progress = fixture.operations.advance_agent_profile_deletion_progress(
            fixture.intent.request_id(),
            progress.revision(),
            time(30_023)?,
            AgentProfileDeletionProgressAdvance::ProviderTargetsSettled,
        )?;
        assert_eq!(
            progress.phase(),
            AgentProfileDeletionProgressPhase::Finalize
        );
        let completed = fixture
            .profiles
            .finish_profile_deletion(fixture.intent.request_id(), time(30_024)?)?;
        let completed_progress = fixture.operations.advance_agent_profile_deletion_progress(
            fixture.intent.request_id(),
            progress.revision(),
            time(30_025)?,
            AgentProfileDeletionProgressAdvance::ProfileFinished(completed),
        )?;
        assert_eq!(
            completed_progress.phase(),
            AgentProfileDeletionProgressPhase::Completed
        );
        Ok(())
    }

    #[test]
    fn pre_click_defer_retries_same_target_and_deleted_requires_positive_receipt()
    -> Result<(), Box<dyn Error>> {
        let mut fixture = Fixture::open()?;
        fixture.prepare_targets()?;
        let first = fixture
            .operations
            .next_agent_profile_provider_deletion_dispatch(time(20)?)?
            .ok_or("first Execute missing")?;
        let (browser, tab, document) = browser_metadata();
        assert_pre_click_deferral(
            &mut fixture,
            &first,
            browser.as_deref(),
            tab.as_deref(),
            document.as_deref(),
        )?;
        confirm_retry_and_finalize(&mut fixture, first.operation_id(), browser, tab, document)?;
        fixture.finish()?;
        Ok(())
    }

    #[test]
    fn exact_quiescence_and_runtime_reference_gate_delete_execute() -> Result<(), Box<dyn Error>> {
        let mut fixture = Fixture::open()?;
        fixture.prepare_targets()?;
        let runtime_id = fixture.linked_entry()?.runtime_session_id();
        let operation_id = Uuid::now_v7();
        let provision_request = Uuid::now_v7();
        fixture
            .operations
            .connection
            .pragma_update(None, "foreign_keys", false)?;
        fixture.operations.connection.execute(
            "INSERT INTO ops_agent_provision_operations(
                operation_id,request_id,slot_ordinal,effect_kind,provider_family,
                profile_id,runtime_session_id,state,provider_conversation_id,
                prepared_at_ms,updated_at_ms,revision
             ) VALUES(?1,?2,0,1,1,?3,?4,3,NULL,17,18,2)",
            params![
                operation_id.as_bytes(),
                provision_request.as_bytes(),
                fixture.profile_id.into_uuid().as_bytes(),
                runtime_id.into_uuid().as_bytes()
            ],
        )?;
        fixture
            .operations
            .connection
            .pragma_update(None, "foreign_keys", true)?;
        let counts = fixture
            .operations
            .profile_deletion_quiescence(&fixture.intent)?;
        assert_eq!(counts.unknown_provision_operations(), 1);
        let runtime_summary = fixture
            .operations
            .profile_deletion_runtime_reference_summary(&fixture.intent, runtime_id)?;
        assert_eq!(runtime_summary.provision_reference_count(), 1);
        assert_eq!(
            runtime_summary.suppressed_or_failed_unlinked_provision_count(),
            0
        );
        assert_eq!(runtime_summary.provider_message_reference_count(), 0);
        assert!(runtime_summary.has_other_provision_reference());
        assert!(
            fixture
                .operations
                .next_agent_profile_provider_deletion_dispatch(time(20)?)?
                .is_none()
        );
        let page = fixture
            .operations
            .profile_deletion_runtime_provision_references_page(
                fixture.profile_id,
                runtime_id,
                None,
                4,
            )?;
        assert_eq!(page.references().len(), 1);
        assert_eq!(
            page.references()[0].state(),
            AgentProvisionState::EffectUnknown
        );
        assert_eq!(page.references()[0].runtime_session_id(), Some(runtime_id));
        fixture.finish()?;
        Ok(())
    }
}
