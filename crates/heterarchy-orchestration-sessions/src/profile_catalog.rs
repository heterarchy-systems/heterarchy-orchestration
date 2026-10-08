//! App-global, non-authoritative catalog for reusable Agent Profile identity.
//!
//! This catalog deliberately does **not** own Workspace participation, Team
//! membership, leadership, vote identity, `WorkCredit`, assignments, Forge Agent
//! bindings, Writer Leases, or provider credentials. It is introduced ahead of
//! the later identity-mapping cutover so existing session/member/effect keys stay
//! authoritative until explicit migration proof exists.

use std::fmt;
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use forge_domain::TimestampMillis;
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use thiserror::Error;
use uuid::Uuid;

mod configuration;
mod deletion;
mod resolver;
pub use configuration::{
    ProfileConfiguration, ProfileMutation, ProfileMutationCommand, ProfileMutationKind,
    ProfileMutationReceipt,
};
pub use deletion::{
    AgentProfileDeletionCommand, AgentProfileDeletionIntent, AgentProfileDeletionIntentPage,
    AgentProfileDeletionManifest, AgentProfileDeletionRuntimeEntry,
    AgentProfileDeletionRuntimeEntryPage, AgentProfileDeletionRuntimeTarget,
};
pub use resolver::{
    AgentProfileResolutionClaim, AgentProfileResolutionClaimCommand,
    AgentProfileResolutionClaimKind, AgentProfileResolutionClaimTarget,
    AgentProfileResolverAvailability, AgentProfileResolverCandidate,
    AgentProfileResolverCandidates, MAX_AGENT_PROFILE_RESOLVER_CANDIDATES,
};

/// Application identifier written into `PRAGMA application_id` for the global
/// Agent Profile catalog. It is distinct from the logical Session catalog.
pub const AGENT_PROFILE_APPLICATION_ID: i64 = 0x4846_4150;
/// Current schema version for the non-authoritative Agent Profile catalog.
pub const AGENT_PROFILE_SCHEMA_VERSION: i64 = 6;
const AGENT_PROFILE_V3_SCHEMA_VERSION: i64 = 3;
const AGENT_PROFILE_V4_SCHEMA_VERSION: i64 = 4;
const AGENT_PROFILE_V5_SCHEMA_VERSION: i64 = 5;

const BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const JOURNAL_MODE_RETRY_LIMIT: usize = 8;
const JOURNAL_MODE_RETRY_DELAY: Duration = Duration::from_millis(10);
const MAX_PROFILE_NAME_BYTES: usize = 128;
const MAX_PROVIDER_PREFERENCE_BYTES: usize = 128;
const MAX_PROVIDER_CONVERSATION_ID_BYTES: usize = 512;
const MAX_CAPABILITY_BYTES: usize = 64;
const MAX_CAPABILITIES: usize = 32;
/// Maximum logical Session links returned in one Profile history read.
pub const MAX_AGENT_PROFILE_LOGICAL_SESSION_LINKS: usize = 32;
/// Maximum provider Runtime Session rows returned in one Profile history read.
pub const MAX_AGENT_PROFILE_RUNTIME_SESSIONS: usize = 32;
const UUID_BYTES: usize = 16;

const AGENT_PROFILES_TABLE_SQL: &str = r"
CREATE TABLE agent_profiles (
    profile_id BLOB PRIMARY KEY NOT NULL CHECK(length(profile_id) = 16 AND profile_id != zeroblob(16)),
    request_id BLOB NOT NULL UNIQUE CHECK(length(request_id) = 16 AND request_id != zeroblob(16)),
    stable_name TEXT NOT NULL CHECK(length(CAST(stable_name AS BLOB)) BETWEEN 1 AND 128),
    display_name TEXT NOT NULL CHECK(length(CAST(display_name AS BLOB)) BETWEEN 1 AND 128),
    provider_family INTEGER NOT NULL CHECK(provider_family = 1),
    preferred_model TEXT CHECK(preferred_model IS NULL OR length(CAST(preferred_model AS BLOB)) BETWEEN 1 AND 128),
    preferred_reasoning TEXT CHECK(preferred_reasoning IS NULL OR length(CAST(preferred_reasoning AS BLOB)) BETWEEN 1 AND 128),
    lifecycle_state INTEGER NOT NULL CHECK(lifecycle_state IN (1, 2, 3)),
    creation_origin INTEGER NOT NULL CHECK(creation_origin IN (1, 2, 3, 4)),
    created_at_ms INTEGER NOT NULL CHECK(created_at_ms >= 0),
    updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms >= created_at_ms),
    last_used_at_ms INTEGER CHECK(last_used_at_ms IS NULL OR last_used_at_ms >= created_at_ms),
    revision INTEGER NOT NULL CHECK(revision > 0)
);
";

pub const MAX_PROFILES_PER_LIST: usize = 64;

const AGENT_PROFILE_CAPABILITIES_TABLE_SQL: &str = r"
CREATE TABLE agent_profile_capabilities (
    profile_id BLOB NOT NULL CHECK(length(profile_id) = 16 AND profile_id != zeroblob(16)),
    capability TEXT NOT NULL CHECK(length(CAST(capability AS BLOB)) BETWEEN 1 AND 64),
    PRIMARY KEY(profile_id, capability),
    FOREIGN KEY(profile_id) REFERENCES agent_profiles(profile_id)
);
";

const AGENT_PROFILE_LOGICAL_SESSIONS_TABLE_SQL: &str = r"
CREATE TABLE agent_profile_logical_sessions (
    request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id) = 16 AND request_id != zeroblob(16)),
    profile_id BLOB NOT NULL CHECK(length(profile_id) = 16 AND profile_id != zeroblob(16)),
    profile_revision INTEGER NOT NULL CHECK(profile_revision > 0),
    session_id BLOB NOT NULL UNIQUE CHECK(length(session_id) = 16 AND session_id != zeroblob(16)),
    session_revision INTEGER NOT NULL CHECK(session_revision > 0),
    linked_at_ms INTEGER NOT NULL CHECK(linked_at_ms >= 0),
    FOREIGN KEY(profile_id) REFERENCES agent_profiles(profile_id)
);
";

const AGENT_PROFILE_RUNTIME_SESSIONS_TABLE_SQL: &str = r"
CREATE TABLE agent_profile_runtime_sessions (
    runtime_session_id BLOB PRIMARY KEY NOT NULL CHECK(length(runtime_session_id) = 16 AND runtime_session_id != zeroblob(16)),
    request_id BLOB NOT NULL UNIQUE CHECK(length(request_id) = 16 AND request_id != zeroblob(16)),
    profile_id BLOB NOT NULL CHECK(length(profile_id) = 16 AND profile_id != zeroblob(16)),
    provider_family INTEGER NOT NULL CHECK(provider_family = 1),
    provider_conversation_id TEXT CHECK(provider_conversation_id IS NULL OR length(CAST(provider_conversation_id AS BLOB)) BETWEEN 1 AND 512),
    state INTEGER NOT NULL CHECK(state BETWEEN 1 AND 8),
    opened_at_ms INTEGER NOT NULL CHECK(opened_at_ms >= 0),
    last_seen_at_ms INTEGER NOT NULL CHECK(last_seen_at_ms >= opened_at_ms),
    closed_at_ms INTEGER CHECK(closed_at_ms IS NULL OR closed_at_ms >= opened_at_ms),
    revision INTEGER NOT NULL CHECK(revision > 0),
    FOREIGN KEY(profile_id) REFERENCES agent_profiles(profile_id),
    UNIQUE(provider_family, provider_conversation_id),
    CHECK((state = 8 AND closed_at_ms IS NOT NULL) OR (state != 8 AND closed_at_ms IS NULL)),
    CHECK((state IN (2,3,4,5,6) AND provider_conversation_id IS NOT NULL) OR state IN (1,7,8))
);
";

const AGENT_PROFILE_MUTATION_RECEIPTS_TABLE_SQL: &str = r"
CREATE TABLE agent_profile_mutation_receipts (
    request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id) = 16 AND request_id != zeroblob(16)),
    profile_id BLOB NOT NULL CHECK(length(profile_id) = 16 AND profile_id != zeroblob(16)),
    operation_kind INTEGER NOT NULL CHECK(operation_kind BETWEEN 1 AND 4),
    expected_revision INTEGER CHECK(expected_revision IS NULL OR expected_revision > 0),
    creation_request_id BLOB NOT NULL CHECK(length(creation_request_id) = 16 AND creation_request_id != zeroblob(16)),
    stable_name TEXT NOT NULL CHECK(length(CAST(stable_name AS BLOB)) BETWEEN 1 AND 128),
    display_name TEXT NOT NULL CHECK(length(CAST(display_name AS BLOB)) BETWEEN 1 AND 128),
    provider_family INTEGER NOT NULL CHECK(provider_family = 1),
    preferred_model TEXT CHECK(preferred_model IS NULL OR length(CAST(preferred_model AS BLOB)) BETWEEN 1 AND 128),
    preferred_reasoning TEXT CHECK(preferred_reasoning IS NULL OR length(CAST(preferred_reasoning AS BLOB)) BETWEEN 1 AND 128),
    lifecycle_state INTEGER NOT NULL CHECK(lifecycle_state IN (1, 2, 3)),
    creation_origin INTEGER NOT NULL CHECK(creation_origin IN (1, 2, 3, 4)),
    created_at_ms INTEGER NOT NULL CHECK(created_at_ms >= 0),
    updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms >= created_at_ms),
    last_used_at_ms INTEGER CHECK(last_used_at_ms IS NULL OR last_used_at_ms >= created_at_ms),
    revision INTEGER NOT NULL CHECK(revision > 0),
    FOREIGN KEY(profile_id) REFERENCES agent_profiles(profile_id),
    CHECK((operation_kind = 1 AND expected_revision IS NULL) OR (operation_kind BETWEEN 2 AND 4 AND expected_revision IS NOT NULL))
);
";

const AGENT_PROFILE_MUTATION_CAPABILITIES_TABLE_SQL: &str = r"
CREATE TABLE agent_profile_mutation_capabilities (
    request_id BLOB NOT NULL CHECK(length(request_id) = 16 AND request_id != zeroblob(16)),
    capability TEXT NOT NULL CHECK(length(CAST(capability AS BLOB)) BETWEEN 1 AND 64),
    PRIMARY KEY(request_id, capability),
    FOREIGN KEY(request_id) REFERENCES agent_profile_mutation_receipts(request_id)
);
";

/// Stable global Agent Profile identity. It is intentionally unrelated to
/// Workspace participant IDs, Forge Agent IDs, and provider conversation IDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AgentProfileId(Uuid);

impl AgentProfileId {
    /// Construct a non-nil profile identity.
    ///
    /// # Errors
    /// Rejects the nil UUID.
    pub fn from_uuid(value: Uuid) -> Result<Self, AgentProfileError> {
        if value.is_nil() {
            return Err(AgentProfileError::InvalidIdentity("agent_profile_id"));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn into_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for AgentProfileId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Provider family preference recorded on a global Profile. This is selection
/// metadata only and never effect authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProfileProviderFamily {
    ChatGpt,
}

/// Durable Profile lifecycle. Runtime availability is intentionally not stored
/// here; BUSY/AVAILABLE/OFFLINE/ERROR are later derived projections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProfileLifecycleState {
    Active,
    Disabled,
    Retired,
}

/// Reusable Agent Pool projection. This is scheduling metadata only and never
/// Workspace membership, vote, Writer Lease, or Forge effect authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProfileOperationalState {
    Online,
    Busy,
    Offline,
    Error,
    Retired,
}

/// Provider Runtime Session lifecycle. Runtime Sessions are deliberately
/// separate from Orchestration logical Agent Sessions and Workspace membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProfileRuntimeSessionState {
    Provisioning,
    Ready,
    Active,
    Suspending,
    Offline,
    Waking,
    Failed,
    Closed,
}

/// Stable provider Runtime Session identity under one durable Agent Profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AgentProfileRuntimeSessionId(Uuid);

impl AgentProfileRuntimeSessionId {
    /// Construct a non-nil Runtime Session identity.
    ///
    /// # Errors
    /// Rejects the nil UUID.
    pub fn from_uuid(value: Uuid) -> Result<Self, AgentProfileError> {
        if value.is_nil() {
            return Err(AgentProfileError::InvalidIdentity("runtime_session_id"));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn into_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for AgentProfileRuntimeSessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Exact monotonic provider Runtime Session revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct AgentProfileRuntimeSessionRevision(i64);

impl AgentProfileRuntimeSessionRevision {
    /// Construct a positive revision.
    ///
    /// # Errors
    /// Rejects zero and negative revisions.
    pub fn new(value: i64) -> Result<Self, AgentProfileError> {
        if value <= 0 {
            return Err(AgentProfileError::InvalidRevision(value));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

/// Durable provider Runtime Session record. Provider credentials are never
/// stored here; only bounded non-secret conversation identity is persisted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileRuntimeSession {
    id: AgentProfileRuntimeSessionId,
    request_id: Uuid,
    profile_id: AgentProfileId,
    provider_family: AgentProfileProviderFamily,
    provider_conversation_id: Option<String>,
    state: AgentProfileRuntimeSessionState,
    opened_at: TimestampMillis,
    last_seen_at: TimestampMillis,
    closed_at: Option<TimestampMillis>,
    revision: AgentProfileRuntimeSessionRevision,
}

impl AgentProfileRuntimeSession {
    #[must_use]
    pub const fn id(&self) -> AgentProfileRuntimeSessionId {
        self.id
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
    pub const fn provider_family(&self) -> AgentProfileProviderFamily {
        self.provider_family
    }

    #[must_use]
    pub fn provider_conversation_id(&self) -> Option<&str> {
        self.provider_conversation_id.as_deref()
    }

    #[must_use]
    pub const fn state(&self) -> AgentProfileRuntimeSessionState {
        self.state
    }

    #[must_use]
    pub const fn opened_at(&self) -> TimestampMillis {
        self.opened_at
    }

    #[must_use]
    pub const fn last_seen_at(&self) -> TimestampMillis {
        self.last_seen_at
    }

    #[must_use]
    pub const fn closed_at(&self) -> Option<TimestampMillis> {
        self.closed_at
    }

    #[must_use]
    pub const fn revision(&self) -> AgentProfileRuntimeSessionRevision {
        self.revision
    }
}

/// Bounded newest-first provider Runtime Session history for one Profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileRuntimeSessionHistory {
    sessions: Vec<AgentProfileRuntimeSession>,
    truncated: bool,
}

/// One ascending, stable-identity page of all provider Runtime Sessions for a
/// Profile, including terminal rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileRuntimeSessionPage {
    sessions: Vec<AgentProfileRuntimeSession>,
    next_runtime_id: Option<AgentProfileRuntimeSessionId>,
}

/// Result of idempotently creating one provider Runtime Session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileRuntimeSessionCreateOutcome {
    session: AgentProfileRuntimeSession,
    created: bool,
}

impl AgentProfileRuntimeSessionCreateOutcome {
    #[must_use]
    pub fn session(&self) -> &AgentProfileRuntimeSession {
        &self.session
    }

    #[must_use]
    pub const fn created(&self) -> bool {
        self.created
    }
}

impl AgentProfileRuntimeSessionHistory {
    #[must_use]
    pub fn sessions(&self) -> &[AgentProfileRuntimeSession] {
        &self.sessions
    }

    #[must_use]
    pub const fn truncated(&self) -> bool {
        self.truncated
    }
}

impl AgentProfileRuntimeSessionPage {
    #[must_use]
    pub fn sessions(&self) -> &[AgentProfileRuntimeSession] {
        &self.sessions
    }

    /// Return the last row's ID when more rows exist, for use as the next
    /// `after_runtime_id` cursor.
    #[must_use]
    pub const fn next_runtime_id(&self) -> Option<AgentProfileRuntimeSessionId> {
        self.next_runtime_id
    }
}

/// Idempotent request to begin one provider Runtime Session. Creation starts
/// in `PROVISIONING`; exact provider conversation identity must be confirmed
/// before the Session can become `READY`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreateAgentProfileRuntimeSession {
    proposed_id: AgentProfileRuntimeSessionId,
    request_id: Uuid,
    profile_id: AgentProfileId,
    provider_family: AgentProfileProviderFamily,
    opened_at: TimestampMillis,
}

impl CreateAgentProfileRuntimeSession {
    /// Build a bounded create request.
    ///
    /// # Errors
    /// Rejects a nil request identity.
    pub fn new(
        proposed_id: AgentProfileRuntimeSessionId,
        request_id: Uuid,
        profile_id: AgentProfileId,
        provider_family: AgentProfileProviderFamily,
        opened_at: TimestampMillis,
    ) -> Result<Self, AgentProfileError> {
        if request_id.is_nil() {
            return Err(AgentProfileError::InvalidIdentity("runtime_request_id"));
        }
        Ok(Self {
            proposed_id,
            request_id,
            profile_id,
            provider_family,
            opened_at,
        })
    }
}

/// Audit origin for Profile creation. Origin grants no permission or role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentProfileCreationOrigin {
    UserManual,
    AgentRequest,
    Imported,
    SystemBootstrap,
}

/// Exact monotonic Profile revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct AgentProfileRevision(i64);

impl AgentProfileRevision {
    /// Construct a positive revision.
    ///
    /// # Errors
    /// Rejects zero and negative revisions.
    pub fn new(value: i64) -> Result<Self, AgentProfileError> {
        if value <= 0 {
            return Err(AgentProfileError::InvalidRevision(value));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

/// Validated idempotent request to create one global Profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAgentProfile {
    proposed_id: AgentProfileId,
    request_id: Uuid,
    stable_name: String,
    display_name: String,
    provider_family: AgentProfileProviderFamily,
    preferred_model: Option<String>,
    preferred_reasoning: Option<String>,
    base_capabilities: Vec<String>,
    creation_origin: AgentProfileCreationOrigin,
    created_at: TimestampMillis,
}

impl CreateAgentProfile {
    /// Build a validated create request. Capability order is canonicalized and
    /// duplicate tags are rejected rather than silently merged.
    ///
    /// # Errors
    /// Rejects nil IDs, unbounded/empty text, excessive capabilities, and
    /// duplicate capability tags.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        proposed_id: AgentProfileId,
        request_id: Uuid,
        stable_name: String,
        display_name: String,
        provider_family: AgentProfileProviderFamily,
        preferred_model: Option<String>,
        preferred_reasoning: Option<String>,
        mut base_capabilities: Vec<String>,
        creation_origin: AgentProfileCreationOrigin,
        created_at: TimestampMillis,
    ) -> Result<Self, AgentProfileError> {
        if request_id.is_nil() {
            return Err(AgentProfileError::InvalidIdentity("request_id"));
        }
        validate_text("stable_name", &stable_name, MAX_PROFILE_NAME_BYTES)?;
        validate_text("display_name", &display_name, MAX_PROFILE_NAME_BYTES)?;
        validate_optional_text(
            "preferred_model",
            preferred_model.as_deref(),
            MAX_PROVIDER_PREFERENCE_BYTES,
        )?;
        validate_optional_text(
            "preferred_reasoning",
            preferred_reasoning.as_deref(),
            MAX_PROVIDER_PREFERENCE_BYTES,
        )?;
        if base_capabilities.len() > MAX_CAPABILITIES {
            return Err(AgentProfileError::TooManyCapabilities {
                actual: base_capabilities.len(),
                maximum: MAX_CAPABILITIES,
            });
        }
        for capability in &base_capabilities {
            validate_text("capability", capability, MAX_CAPABILITY_BYTES)?;
        }
        base_capabilities.sort();
        if base_capabilities.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(AgentProfileError::DuplicateCapability);
        }
        Ok(Self {
            proposed_id,
            request_id,
            stable_name,
            display_name,
            provider_family,
            preferred_model,
            preferred_reasoning,
            base_capabilities,
            creation_origin,
            created_at,
        })
    }
}

/// Durable global Profile record. No scoped authority identifiers are present.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfile {
    id: AgentProfileId,
    request_id: Uuid,
    stable_name: String,
    display_name: String,
    provider_family: AgentProfileProviderFamily,
    preferred_model: Option<String>,
    preferred_reasoning: Option<String>,
    base_capabilities: Vec<String>,
    lifecycle_state: AgentProfileLifecycleState,
    creation_origin: AgentProfileCreationOrigin,
    created_at: TimestampMillis,
    updated_at: TimestampMillis,
    last_used_at: Option<TimestampMillis>,
    revision: AgentProfileRevision,
}

impl AgentProfile {
    #[must_use]
    pub const fn id(&self) -> AgentProfileId {
        self.id
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub fn stable_name(&self) -> &str {
        &self.stable_name
    }

    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    #[must_use]
    pub const fn provider_family(&self) -> AgentProfileProviderFamily {
        self.provider_family
    }

    #[must_use]
    pub fn preferred_model(&self) -> Option<&str> {
        self.preferred_model.as_deref()
    }

    #[must_use]
    pub fn preferred_reasoning(&self) -> Option<&str> {
        self.preferred_reasoning.as_deref()
    }

    #[must_use]
    pub fn base_capabilities(&self) -> &[String] {
        &self.base_capabilities
    }

    #[must_use]
    pub const fn lifecycle_state(&self) -> AgentProfileLifecycleState {
        self.lifecycle_state
    }

    #[must_use]
    pub const fn creation_origin(&self) -> AgentProfileCreationOrigin {
        self.creation_origin
    }

    #[must_use]
    pub const fn created_at(&self) -> TimestampMillis {
        self.created_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> TimestampMillis {
        self.updated_at
    }

    #[must_use]
    pub const fn last_used_at(&self) -> Option<TimestampMillis> {
        self.last_used_at
    }

    #[must_use]
    pub const fn revision(&self) -> AgentProfileRevision {
        self.revision
    }

    fn matches_create(&self, command: &CreateAgentProfile) -> bool {
        self.stable_name == command.stable_name
            && self.display_name == command.display_name
            && self.provider_family == command.provider_family
            && self.preferred_model == command.preferred_model
            && self.preferred_reasoning == command.preferred_reasoning
            && self.base_capabilities == command.base_capabilities
            && self.creation_origin == command.creation_origin
    }
}

/// Result of an idempotent Profile create.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileCreateOutcome {
    profile: AgentProfile,
    created: bool,
}

/// Immutable link from one global Profile to one existing logical Agent
/// Session. It carries no Workspace participation or provider-runtime meaning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentProfileLogicalSessionLink {
    request_id: Uuid,
    profile_id: AgentProfileId,
    profile_revision: AgentProfileRevision,
    session_id: forge_domain::AgentSessionId,
    session_revision: forge_domain::AgentSessionRevision,
    linked_at: TimestampMillis,
}

/// Bounded logical Session history for one global Profile.
///
/// `truncated=true` is explicit evidence that additional historical links
/// exist; callers must not interpret the returned prefix as a complete
/// identity history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfileLogicalSessionHistory {
    links: Vec<AgentProfileLogicalSessionLink>,
    truncated: bool,
}

impl AgentProfileLogicalSessionHistory {
    #[must_use]
    pub fn links(&self) -> &[AgentProfileLogicalSessionLink] {
        &self.links
    }

    #[must_use]
    pub const fn truncated(&self) -> bool {
        self.truncated
    }
}

impl AgentProfileLogicalSessionLink {
    #[must_use]
    pub const fn request_id(self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn profile_id(self) -> AgentProfileId {
        self.profile_id
    }

    #[must_use]
    pub const fn profile_revision(self) -> AgentProfileRevision {
        self.profile_revision
    }

    pub const fn session_id(self) -> forge_domain::AgentSessionId {
        self.session_id
    }

    pub const fn session_revision(self) -> forge_domain::AgentSessionRevision {
        self.session_revision
    }

    #[must_use]
    pub const fn linked_at(self) -> TimestampMillis {
        self.linked_at
    }
}

/// Result of idempotently linking an existing logical Session to a Profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentProfileLogicalSessionLinkOutcome {
    link: AgentProfileLogicalSessionLink,
    created: bool,
}

impl AgentProfileLogicalSessionLinkOutcome {
    #[must_use]
    pub const fn link(self) -> AgentProfileLogicalSessionLink {
        self.link
    }

    #[must_use]
    pub const fn created(self) -> bool {
        self.created
    }
}

impl AgentProfileCreateOutcome {
    #[must_use]
    pub fn profile(&self) -> &AgentProfile {
        &self.profile
    }

    #[must_use]
    pub const fn created(&self) -> bool {
        self.created
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
    links: u64,
}

impl FileIdentity {
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            links: metadata.nlink(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DirectoryIdentity {
    device: u64,
    inode: u64,
}

impl DirectoryIdentity {
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    }
}

/// SQLite-backed app-global Profile catalog. This type has no method that can
/// create or mutate Workspace participation or Forge effect authority.
pub struct AgentProfileCatalog {
    path: PathBuf,
    database_parent: PathBuf,
    database_parent_identity: DirectoryIdentity,
    database_identity: FileIdentity,
    connection: Connection,
}

impl fmt::Debug for AgentProfileCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentProfileCatalog")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl AgentProfileCatalog {
    /// Open an existing Profile catalog or create a fresh one at an
    /// application-owned path.
    ///
    /// # Errors
    /// Returns path-integrity, ownership/schema, or SQLite errors.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, AgentProfileError> {
        let path = canonical_catalog_path(path.as_ref())?;
        let database_parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .ok_or_else(|| AgentProfileError::MissingParent(path.clone()))?
            .to_path_buf();
        let database_parent_identity = inspect_directory(&database_parent)?;
        let database_before = inspect_regular_file(&path, true)?;
        let wal_path = sidecar_path(&path, "-wal");
        let shm_path = sidecar_path(&path, "-shm");
        if database_before.is_none()
            && (inspect_regular_file(&wal_path, true)?.is_some()
                || inspect_regular_file(&shm_path, true)?.is_some())
        {
            return Err(AgentProfileError::DatabaseSidecarWithoutMain(path));
        }
        let flags = OpenFlags::default() | OpenFlags::SQLITE_OPEN_NOFOLLOW;
        let connection = Connection::open_with_flags(&path, flags).map_err(|source| {
            AgentProfileError::Open {
                path: path.clone(),
                source,
            }
        })?;
        let database_after = inspect_regular_file(&path, false)?.ok_or_else(|| {
            AgentProfileError::DatabasePathInvariant {
                path: path.clone(),
                detail: "SQLite open returned without a regular database file",
            }
        })?;
        if let Some(expected) = database_before
            && expected != database_after
        {
            return Err(AgentProfileError::DatabasePathChanged(path));
        }
        ensure_directory_identity(&database_parent, database_parent_identity)?;
        let mut catalog = Self {
            path,
            database_parent,
            database_parent_identity,
            database_identity: database_after,
            connection,
        };
        catalog.connection.busy_timeout(BUSY_TIMEOUT)?;
        initialize_or_validate(&mut catalog.connection)?;
        configure(&catalog.connection)?;
        catalog.ensure_live_binding()?;
        Ok(catalog)
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Create one Profile idempotently by request identity. A replay may
    /// propose a different random profile ID; the first committed ID remains
    /// authoritative when semantic Profile content is identical.
    ///
    /// # Errors
    /// Rejects conflicting replay content, reused profile IDs, path-integrity
    /// failures, or SQLite transaction failures.
    pub fn create_profile(
        &mut self,
        command: &CreateAgentProfile,
    ) -> Result<AgentProfileCreateOutcome, AgentProfileError> {
        self.ensure_live_binding()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(receipt) = load_profile_mutation_receipt(&transaction, command.request_id)? {
            if receipt.kind != ProfileMutationKind::OriginalCreate
                || !receipt.profile.matches_create(command)
            {
                return Err(AgentProfileError::ReplayConflict(command.request_id));
            }
            let existing = load_profile(&transaction, receipt.profile.id())?
                .ok_or(AgentProfileError::ProfileMissing(receipt.profile.id()))?;
            commit_profile_transaction(transaction, Some(command.request_id), existing.id())?;
            self.ensure_live_binding()?;
            return Ok(AgentProfileCreateOutcome {
                profile: existing,
                created: false,
            });
        }
        if profile_id_by_request(&transaction, command.request_id)?.is_some() {
            return Err(AgentProfileError::Corrupt(
                "Agent Profile original create receipt missing",
            ));
        }
        if load_profile(&transaction, command.proposed_id)?.is_some() {
            return Err(AgentProfileError::ProfileIdConflict(command.proposed_id));
        }
        let profile = AgentProfile {
            id: command.proposed_id,
            request_id: command.request_id,
            stable_name: command.stable_name.clone(),
            display_name: command.display_name.clone(),
            provider_family: command.provider_family,
            preferred_model: command.preferred_model.clone(),
            preferred_reasoning: command.preferred_reasoning.clone(),
            base_capabilities: command.base_capabilities.clone(),
            lifecycle_state: AgentProfileLifecycleState::Active,
            creation_origin: command.creation_origin,
            created_at: command.created_at,
            updated_at: command.created_at,
            last_used_at: None,
            revision: AgentProfileRevision(1),
        };
        insert_profile(&transaction, &profile)?;
        insert_profile_mutation_receipt(
            &transaction,
            profile.request_id(),
            None,
            ProfileMutationKind::OriginalCreate,
            &profile,
        )?;
        let stored = load_profile_mutation_receipt(&transaction, profile.request_id())?.ok_or(
            AgentProfileError::Corrupt("Profile create receipt readback"),
        )?;
        if stored.kind != ProfileMutationKind::OriginalCreate || stored.profile != profile {
            return Err(AgentProfileError::Corrupt(
                "Profile create receipt readback",
            ));
        }
        commit_profile_transaction(transaction, Some(command.request_id), profile.id())?;
        self.ensure_live_binding()?;
        Ok(AgentProfileCreateOutcome {
            profile,
            created: true,
        })
    }

    /// Administratively retire one local Profile from fresh admission. This
    /// preserves durable history and does not delete provider conversations or
    /// change Forge authority. Idempotent on an already retired Profile.
    ///
    /// # Errors
    /// Rejects unknown Profiles, stale revisions, and live provider Runtime
    /// Sessions that must be closed before retirement.
    pub fn retire_profile(
        &mut self,
        profile_id: AgentProfileId,
        expected_revision: AgentProfileRevision,
        observed_at: forge_domain::TimestampMillis,
    ) -> Result<AgentProfile, AgentProfileError> {
        self.ensure_live_binding()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let profile = load_profile(&transaction, profile_id)?
            .ok_or(AgentProfileError::ProfileMissing(profile_id))?;
        let retired =
            prepare_profile_retirement(&transaction, &profile, expected_revision, observed_at)?;
        if retired.revision() != profile.revision() {
            update_profile(&transaction, &retired, profile.revision())?;
        }
        commit_profile_transaction(transaction, None, profile_id)?;
        self.ensure_live_binding()?;
        Ok(retired)
    }

    /// Apply one validated Profile configuration or lifecycle mutation and
    /// persist its immutable snapshot receipt in the same transaction.
    ///
    /// # Errors
    /// Rejects missing or stale Profiles, inactive metadata updates, unsafe
    /// retirement, conflicting request replay, or an unconfirmed commit.
    pub fn mutate_profile(
        &mut self,
        command: &ProfileMutationCommand,
    ) -> Result<ProfileMutationReceipt, AgentProfileError> {
        self.ensure_live_binding()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(receipt) = load_profile_mutation_receipt(&transaction, command.request_id())? {
            if receipt.matches_command(command) {
                let committed = receipt.into_public(true);
                commit_profile_transaction(
                    transaction,
                    Some(command.request_id()),
                    command.profile_id(),
                )?;
                self.ensure_live_binding()?;
                return Ok(committed);
            }
            return Err(AgentProfileError::ProfileMutationReplayConflict(
                command.request_id(),
            ));
        }
        let current = load_profile(&transaction, command.profile_id())?
            .ok_or(AgentProfileError::ProfileMissing(command.profile_id()))?;
        let updated = apply_profile_mutation(&transaction, &current, command)?;
        update_profile(&transaction, &updated, current.revision())?;
        insert_profile_mutation_receipt(
            &transaction,
            command.request_id(),
            Some(command.expected_revision()),
            command.kind(),
            &updated,
        )?;
        let stored = load_profile_mutation_receipt(&transaction, command.request_id())?.ok_or(
            AgentProfileError::Corrupt("Profile mutation insert readback"),
        )?;
        if !stored.matches_command(command) || stored.profile != updated {
            return Err(AgentProfileError::Corrupt(
                "Profile mutation insert readback",
            ));
        }
        let receipt = ProfileMutationReceipt::committed(
            command.request_id(),
            updated,
            Some(command.expected_revision()),
            command.kind(),
            false,
        );
        commit_profile_transaction(
            transaction,
            Some(command.request_id()),
            command.profile_id(),
        )?;
        self.ensure_live_binding()?;
        Ok(receipt)
    }

    /// Read an immutable Profile mutation receipt without mutating the catalog.
    ///
    /// # Errors
    /// Returns path-integrity, corrupt-row, or SQLite errors.
    pub fn mutation_receipt(
        &self,
        request_id: Uuid,
    ) -> Result<Option<ProfileMutationReceipt>, AgentProfileError> {
        if request_id.is_nil() {
            return Err(AgentProfileError::InvalidIdentity(
                "profile_mutation_request_id",
            ));
        }
        self.ensure_live_binding()?;
        Ok(load_profile_mutation_receipt(&self.connection, request_id)?
            .map(|receipt| receipt.into_public(false)))
    }

    /// Create one provider Runtime Session idempotently by request identity.
    /// New Runtime Sessions always begin in `PROVISIONING` and carry no
    /// provider conversation identity until explicitly confirmed.
    /// An exact stored request replay returns its original Runtime Session
    /// before lifecycle admission checks; fresh creation requires an active
    /// Profile.
    ///
    /// # Errors
    /// Rejects missing/inactive Profiles, conflicting request replay, reused
    /// Runtime Session identities, or persistence failures.
    pub fn create_runtime_session(
        &mut self,
        command: &CreateAgentProfileRuntimeSession,
    ) -> Result<AgentProfileRuntimeSessionCreateOutcome, AgentProfileError> {
        self.ensure_live_binding()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let profile = load_profile(&transaction, command.profile_id)?
            .ok_or(AgentProfileError::ProfileMissing(command.profile_id))?;
        if let Some(existing) = runtime_session_by_request(&transaction, command.request_id)? {
            if existing.profile_id != command.profile_id
                || existing.provider_family != command.provider_family
                || existing.opened_at != command.opened_at
            {
                return Err(AgentProfileError::RuntimeSessionReplayConflict(
                    command.request_id,
                ));
            }
            transaction.commit()?;
            self.ensure_live_binding()?;
            return Ok(AgentProfileRuntimeSessionCreateOutcome {
                session: existing,
                created: false,
            });
        }
        if profile.lifecycle_state() == AgentProfileLifecycleState::Retired {
            return Err(AgentProfileError::RuntimeSessionProfileRetired(
                command.profile_id,
            ));
        }
        if profile.lifecycle_state() != AgentProfileLifecycleState::Active {
            return Err(AgentProfileError::ProfileNotActive {
                profile_id: command.profile_id,
                lifecycle_state: profile.lifecycle_state(),
            });
        }
        if load_runtime_session(&transaction, command.proposed_id)?.is_some() {
            return Err(AgentProfileError::RuntimeSessionIdConflict(
                command.proposed_id,
            ));
        }
        let session = AgentProfileRuntimeSession {
            id: command.proposed_id,
            request_id: command.request_id,
            profile_id: command.profile_id,
            provider_family: command.provider_family,
            provider_conversation_id: None,
            state: AgentProfileRuntimeSessionState::Provisioning,
            opened_at: command.opened_at,
            last_seen_at: command.opened_at,
            closed_at: None,
            revision: AgentProfileRuntimeSessionRevision(1),
        };
        insert_runtime_session(&transaction, &session)?;
        transaction.commit()?;
        self.ensure_live_binding()?;
        Ok(AgentProfileRuntimeSessionCreateOutcome {
            session,
            created: true,
        })
    }

    /// Confirm the exact non-secret provider conversation identity and promote
    /// one `PROVISIONING` Runtime Session to `READY`. Exact identical replay is
    /// idempotent after a lost response.
    ///
    /// # Errors
    /// Rejects invalid/busy conversation identity, stale revisions, invalid
    /// source state, non-monotonic observation time, or persistence failures.
    pub fn confirm_runtime_session_ready(
        &mut self,
        id: AgentProfileRuntimeSessionId,
        expected_revision: AgentProfileRuntimeSessionRevision,
        provider_conversation_id: &str,
        observed_at: TimestampMillis,
    ) -> Result<AgentProfileRuntimeSession, AgentProfileError> {
        validate_text(
            "provider_conversation_id",
            provider_conversation_id,
            MAX_PROVIDER_CONVERSATION_ID_BYTES,
        )?;
        self.ensure_live_binding()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_runtime_session(&transaction, id)?
            .ok_or(AgentProfileError::RuntimeSessionMissing(id))?;
        if current.state == AgentProfileRuntimeSessionState::Ready
            && current.provider_conversation_id.as_deref() == Some(provider_conversation_id)
        {
            transaction.commit()?;
            self.ensure_live_binding()?;
            return Ok(current);
        }
        if deletion::manifest_exists_for_profile(&transaction, current.profile_id)? {
            return Err(AgentProfileError::ProfileDeletionTargetsFrozen(
                current.profile_id,
            ));
        }
        if current.revision != expected_revision {
            return Err(AgentProfileError::RuntimeSessionRevisionConflict {
                expected: expected_revision,
                actual: current.revision,
            });
        }
        if current.state != AgentProfileRuntimeSessionState::Provisioning {
            return Err(AgentProfileError::RuntimeSessionTransition {
                from: current.state,
                to: AgentProfileRuntimeSessionState::Ready,
            });
        }
        ensure_runtime_observation(&current, observed_at)?;
        if let Some(existing_id) = runtime_session_id_by_conversation(
            &transaction,
            current.provider_family,
            provider_conversation_id,
        )? && existing_id != id
        {
            return Err(AgentProfileError::RuntimeSessionConversationConflict);
        }
        let next_revision = next_runtime_revision(current.revision)?;
        transaction.execute(
            "UPDATE agent_profile_runtime_sessions
             SET provider_conversation_id=?1,state=?2,last_seen_at_ms=?3,revision=?4
             WHERE runtime_session_id=?5 AND revision=?6",
            params![
                provider_conversation_id,
                encode_runtime_state(AgentProfileRuntimeSessionState::Ready),
                observed_at.get(),
                next_revision.get(),
                id.into_uuid().as_bytes(),
                expected_revision.get(),
            ],
        )?;
        let updated = load_runtime_session(&transaction, id)?
            .ok_or(AgentProfileError::RuntimeSessionMissing(id))?;
        transaction.commit()?;
        self.ensure_live_binding()?;
        Ok(updated)
    }

    /// Compare-and-set one provider Runtime Session lifecycle transition.
    /// `PROVISIONING -> READY` must use `confirm_runtime_session_ready` so exact
    /// provider conversation identity is never bypassed.
    ///
    /// # Errors
    /// Rejects stale revisions, invalid transitions, missing conversation
    /// identity for a state that requires it, activation/wake of an inactive
    /// Profile, frozen deletion targets, clock regression, or persistence
    /// failures.
    pub fn transition_runtime_session(
        &mut self,
        id: AgentProfileRuntimeSessionId,
        expected_revision: AgentProfileRuntimeSessionRevision,
        target: AgentProfileRuntimeSessionState,
        observed_at: TimestampMillis,
    ) -> Result<AgentProfileRuntimeSession, AgentProfileError> {
        self.ensure_live_binding()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_runtime_session(&transaction, id)?
            .ok_or(AgentProfileError::RuntimeSessionMissing(id))?;
        if deletion::manifest_exists_for_profile(&transaction, current.profile_id)? {
            return Err(AgentProfileError::ProfileDeletionTargetsFrozen(
                current.profile_id,
            ));
        }
        if current.revision != expected_revision {
            return Err(AgentProfileError::RuntimeSessionRevisionConflict {
                expected: expected_revision,
                actual: current.revision,
            });
        }
        if current.state == AgentProfileRuntimeSessionState::Provisioning
            && target == AgentProfileRuntimeSessionState::Ready
        {
            return Err(AgentProfileError::RuntimeSessionIdentityRequired);
        }
        if !runtime_transition_allowed(current.state, target) {
            return Err(AgentProfileError::RuntimeSessionTransition {
                from: current.state,
                to: target,
            });
        }
        if runtime_state_requires_identity(target) && current.provider_conversation_id.is_none() {
            return Err(AgentProfileError::RuntimeSessionIdentityRequired);
        }
        if matches!(
            target,
            AgentProfileRuntimeSessionState::Active | AgentProfileRuntimeSessionState::Waking
        ) {
            let profile = load_profile(&transaction, current.profile_id)?
                .ok_or(AgentProfileError::ProfileMissing(current.profile_id))?;
            if profile.lifecycle_state() != AgentProfileLifecycleState::Active {
                return Err(AgentProfileError::ProfileNotActive {
                    profile_id: current.profile_id,
                    lifecycle_state: profile.lifecycle_state(),
                });
            }
        }
        ensure_runtime_observation(&current, observed_at)?;
        let next_revision = next_runtime_revision(current.revision)?;
        let closed_at =
            (target == AgentProfileRuntimeSessionState::Closed).then_some(observed_at.get());
        transaction.execute(
            "UPDATE agent_profile_runtime_sessions
             SET state=?1,last_seen_at_ms=?2,closed_at_ms=?3,revision=?4
             WHERE runtime_session_id=?5 AND revision=?6",
            params![
                encode_runtime_state(target),
                observed_at.get(),
                closed_at,
                next_revision.get(),
                id.into_uuid().as_bytes(),
                expected_revision.get(),
            ],
        )?;
        let updated = load_runtime_session(&transaction, id)?
            .ok_or(AgentProfileError::RuntimeSessionMissing(id))?;
        transaction.commit()?;
        self.ensure_live_binding()?;
        Ok(updated)
    }

    /// Record the exact provider-observed completion of a wake that was already
    /// admitted elsewhere. This is not a new wake authorization: the caller
    /// must supply the existing Runtime Session identity and its exact
    /// provider conversation identity. It remains usable while the Profile is
    /// Disabled or Retired so prior admitted work can be reconciled.
    ///
    /// # Errors
    /// Rejects a missing Runtime Session, stale revision, changed conversation
    /// identity, frozen deletion targets, invalid source state, clock regression,
    /// or persistence errors.
    pub fn confirm_existing_runtime_wake_observed(
        &mut self,
        id: AgentProfileRuntimeSessionId,
        expected_revision: AgentProfileRuntimeSessionRevision,
        exact_provider_conversation_id: &str,
        observed_at: TimestampMillis,
    ) -> Result<AgentProfileRuntimeSession, AgentProfileError> {
        validate_text(
            "provider_conversation_id",
            exact_provider_conversation_id,
            MAX_PROVIDER_CONVERSATION_ID_BYTES,
        )?;
        self.ensure_live_binding()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_runtime_session(&transaction, id)?
            .ok_or(AgentProfileError::RuntimeSessionMissing(id))?;
        if current.state == AgentProfileRuntimeSessionState::Ready
            && current.provider_conversation_id.as_deref() == Some(exact_provider_conversation_id)
        {
            transaction.commit()?;
            self.ensure_live_binding()?;
            return Ok(current);
        }
        if deletion::manifest_exists_for_profile(&transaction, current.profile_id)? {
            return Err(AgentProfileError::ProfileDeletionTargetsFrozen(
                current.profile_id,
            ));
        }
        if current.revision != expected_revision {
            return Err(AgentProfileError::RuntimeSessionRevisionConflict {
                expected: expected_revision,
                actual: current.revision,
            });
        }
        if current.provider_conversation_id.as_deref() != Some(exact_provider_conversation_id) {
            return Err(AgentProfileError::RuntimeSessionConversationConflict);
        }
        if !matches!(
            current.state,
            AgentProfileRuntimeSessionState::Offline | AgentProfileRuntimeSessionState::Waking
        ) {
            return Err(AgentProfileError::RuntimeSessionTransition {
                from: current.state,
                to: AgentProfileRuntimeSessionState::Ready,
            });
        }
        ensure_runtime_observation(&current, observed_at)?;
        let next_revision = next_runtime_revision(current.revision)?;
        transaction.execute(
            "UPDATE agent_profile_runtime_sessions
             SET state=?1,last_seen_at_ms=?2,closed_at_ms=NULL,revision=?3
             WHERE runtime_session_id=?4 AND revision=?5",
            params![
                encode_runtime_state(AgentProfileRuntimeSessionState::Ready),
                observed_at.get(),
                next_revision.get(),
                id.into_uuid().as_bytes(),
                expected_revision.get(),
            ],
        )?;
        let updated = load_runtime_session(&transaction, id)?
            .ok_or(AgentProfileError::RuntimeSessionMissing(id))?;
        transaction.commit()?;
        self.ensure_live_binding()?;
        Ok(updated)
    }

    /// Resolve one exact provider Runtime Session.
    ///
    /// # Errors
    /// Returns corrupt-row, path-integrity, or SQLite failures.
    pub fn runtime_session(
        &self,
        id: AgentProfileRuntimeSessionId,
    ) -> Result<Option<AgentProfileRuntimeSession>, AgentProfileError> {
        self.ensure_live_binding()?;
        load_runtime_session(&self.connection, id)
    }

    /// Read bounded newest-first provider Runtime Session history.
    ///
    /// # Errors
    /// Returns missing Profile, corrupt-row, path-integrity, or SQLite errors.
    pub fn runtime_session_history(
        &self,
        profile_id: AgentProfileId,
    ) -> Result<AgentProfileRuntimeSessionHistory, AgentProfileError> {
        self.ensure_live_binding()?;
        if load_profile(&self.connection, profile_id)?.is_none() {
            return Err(AgentProfileError::ProfileMissing(profile_id));
        }
        runtime_session_history(&self.connection, profile_id)
    }

    /// Read one bounded ascending Runtime Session inventory page for a Profile.
    /// All lifecycle states are included, including terminal rows with retained
    /// provider conversation identity. Pass `next_runtime_id()` as the cursor
    /// for the following page; IDs are ordered independently of mutable times.
    /// A cursor does not freeze the inventory: deletion callers must first stop
    /// concurrent runtime creation and settle provider-identity changes.
    ///
    /// # Errors
    /// Returns missing Profile, an invalid page bound, corrupt-row,
    /// path-integrity, or SQLite errors.
    pub fn runtime_sessions_page(
        &self,
        profile_id: AgentProfileId,
        after_runtime_id: Option<AgentProfileRuntimeSessionId>,
        limit: u32,
    ) -> Result<AgentProfileRuntimeSessionPage, AgentProfileError> {
        self.ensure_live_binding()?;
        let page_size = match usize::try_from(limit) {
            Ok(page_size) if (1..=MAX_AGENT_PROFILE_RUNTIME_SESSIONS).contains(&page_size) => {
                page_size
            }
            _ => {
                return Err(AgentProfileError::RuntimeSessionPageLimit {
                    actual: limit,
                    maximum: MAX_AGENT_PROFILE_RUNTIME_SESSIONS,
                });
            }
        };
        if load_profile(&self.connection, profile_id)?.is_none() {
            return Err(AgentProfileError::ProfileMissing(profile_id));
        }
        runtime_sessions_page(&self.connection, profile_id, after_runtime_id, page_size)
    }

    /// Derive the reusable Agent Pool operational state from administrative
    /// lifecycle plus provider Runtime Session state. This projection grants no
    /// Workspace/Team/effect authority.
    ///
    /// # Errors
    /// Returns corrupt-row, path-integrity, or SQLite errors.
    pub fn operational_state(
        &self,
        profile_id: AgentProfileId,
    ) -> Result<Option<AgentProfileOperationalState>, AgentProfileError> {
        self.ensure_live_binding()?;
        let Some(profile) = load_profile(&self.connection, profile_id)? else {
            return Ok(None);
        };
        if profile.lifecycle_state() == AgentProfileLifecycleState::Retired {
            return Ok(Some(AgentProfileOperationalState::Retired));
        }
        let (active, online, failed): (i64, i64, i64) = self.connection.query_row(
            "SELECT
                COALESCE(MAX(CASE WHEN state=3 THEN 1 ELSE 0 END),0),
                COALESCE(MAX(CASE WHEN state IN(2,4) THEN 1 ELSE 0 END),0),
                COALESCE(MAX(CASE WHEN state=7 THEN 1 ELSE 0 END),0)
             FROM agent_profile_runtime_sessions WHERE profile_id=?1",
            params![profile_id.into_uuid().as_bytes()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        let state = if active != 0 {
            AgentProfileOperationalState::Busy
        } else if online != 0 {
            AgentProfileOperationalState::Online
        } else if failed != 0 {
            AgentProfileOperationalState::Error
        } else {
            AgentProfileOperationalState::Offline
        };
        Ok(Some(state))
    }

    /// Resolve one Profile by its global stable identity.
    ///
    /// # Errors
    /// Returns path-integrity, corrupt-row, or SQLite errors.
    pub fn resolve(&self, id: AgentProfileId) -> Result<Option<AgentProfile>, AgentProfileError> {
        self.ensure_live_binding()?;
        load_profile(&self.connection, id)
    }

    /// Read a bounded, newest-first page of the app-global Profile catalog.
    ///
    /// The page cap keeps the deliberate per-profile load (one canonical
    /// decode path) bounded; this is an operator observation surface, not a
    /// fan-out hot path.
    ///
    /// # Errors
    /// Returns out-of-bounds limits, invalid stored identities, and storage
    /// failures.
    pub fn list_profiles(&self, limit: usize) -> Result<Vec<AgentProfile>, AgentProfileError> {
        if limit == 0 || limit > MAX_PROFILES_PER_LIST {
            return Err(AgentProfileError::InvalidIdentity("profile list limit"));
        }
        let statement = self.connection.prepare(
            "SELECT profile_id FROM agent_profiles ORDER BY created_at_ms DESC, profile_id DESC LIMIT ?1",
        );
        let mut statement = statement?;
        let ids = statement
            .query_map(
                params![
                    i64::try_from(limit)
                        .map_err(|_| AgentProfileError::InvalidIdentity("profile list limit"))?
                ],
                |row| row.get::<_, Vec<u8>>(0),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        let mut profiles = Vec::with_capacity(ids.len());
        for id in ids {
            let uuid = Uuid::from_slice(&id)
                .map_err(|_| AgentProfileError::Corrupt("profile id length"))?;
            let profile_id = AgentProfileId::from_uuid(uuid)?;
            if let Some(profile) = load_profile(&self.connection, profile_id)? {
                profiles.push(profile);
            }
        }
        Ok(profiles)
    }

    /// Persist an immutable Profile-to-logical-Session identity link after the
    /// caller has resolved the Session from the authoritative Session catalog.
    /// Replays are idempotent; one logical Session can never map to two global
    /// Profiles.
    ///
    /// # Errors
    /// Rejects missing/inactive Profiles for new links, request replay conflicts,
    /// cross-Profile Session reuse, path-integrity failures, or SQLite
    /// transaction failures. Exact stored request/Session replays remain
    /// readable after Profile retirement.
    pub(crate) fn link_verified_logical_session(
        &mut self,
        request_id: Uuid,
        profile_id: AgentProfileId,
        session_id: forge_domain::AgentSessionId,
        session_revision: forge_domain::AgentSessionRevision,
        linked_at: TimestampMillis,
    ) -> Result<AgentProfileLogicalSessionLinkOutcome, AgentProfileError> {
        if request_id.is_nil() {
            return Err(AgentProfileError::InvalidIdentity("link_request_id"));
        }
        self.ensure_live_binding()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let profile = load_profile(&transaction, profile_id)?
            .ok_or(AgentProfileError::ProfileMissing(profile_id))?;
        if let Some(existing) = logical_session_link_by_request(&transaction, request_id)? {
            if existing.profile_id != profile_id || existing.session_id != session_id {
                return Err(AgentProfileError::LogicalSessionReplayConflict(request_id));
            }
            transaction.commit()?;
            self.ensure_live_binding()?;
            return Ok(AgentProfileLogicalSessionLinkOutcome {
                link: existing,
                created: false,
            });
        }
        if let Some(existing) = logical_session_link_by_session(&transaction, session_id)? {
            if existing.profile_id != profile_id {
                return Err(AgentProfileError::LogicalSessionProfileConflict {
                    session_id,
                    existing_profile_id: existing.profile_id,
                    requested_profile_id: profile_id,
                });
            }
            transaction.commit()?;
            self.ensure_live_binding()?;
            return Ok(AgentProfileLogicalSessionLinkOutcome {
                link: existing,
                created: false,
            });
        }
        if profile.lifecycle_state() != AgentProfileLifecycleState::Active {
            return Err(AgentProfileError::ProfileNotActive {
                profile_id,
                lifecycle_state: profile.lifecycle_state(),
            });
        }
        transaction.execute(
            "INSERT INTO agent_profile_logical_sessions(request_id,profile_id,profile_revision,session_id,session_revision,linked_at_ms) VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                request_id.as_bytes(),
                profile_id.into_uuid().as_bytes(),
                profile.revision().get(),
                session_id.into_uuid().as_bytes(),
                session_revision.get(),
                linked_at.get(),
            ],
        )?;
        let link = AgentProfileLogicalSessionLink {
            request_id,
            profile_id,
            profile_revision: profile.revision(),
            session_id,
            session_revision,
            linked_at,
        };
        transaction.commit()?;
        self.ensure_live_binding()?;
        Ok(AgentProfileLogicalSessionLinkOutcome {
            link,
            created: true,
        })
    }

    /// Persist an immutable Profile-to-Session link from a Session record that
    /// the caller already resolved through the trusted Session authority.
    /// This bridge records identity correspondence only; it does not create a
    /// Workspace participant, role, vote, lease, or Forge effect authority.
    ///
    /// # Errors
    /// Rejects missing Profiles, request replay conflicts, cross-Profile
    /// Session reuse, path-integrity failures, or SQLite transaction failures.
    pub fn link_resolved_logical_session(
        &mut self,
        request_id: Uuid,
        profile_id: AgentProfileId,
        session: &forge_domain::RegisteredAgentSession,
        linked_at: TimestampMillis,
    ) -> Result<AgentProfileLogicalSessionLinkOutcome, AgentProfileError> {
        self.link_verified_logical_session(
            request_id,
            profile_id,
            session.id(),
            session.revision(),
            linked_at,
        )
    }

    /// Resolve the global Profile link for one logical Session, if linked.
    /// This read does not infer Workspace participation.
    ///
    /// # Errors
    /// Returns corrupt-row, path-integrity, or SQLite failures.
    pub fn logical_session_link(
        &self,
        session_id: forge_domain::AgentSessionId,
    ) -> Result<Option<AgentProfileLogicalSessionLink>, AgentProfileError> {
        self.ensure_live_binding()?;
        logical_session_link_by_session(&self.connection, session_id)
    }

    /// Read a bounded newest-first logical Session history for one Profile.
    ///
    /// The history contains immutable link receipts only. It does not bypass
    /// principal-bound Session resolution or infer current runtime state.
    ///
    /// # Errors
    /// Returns missing/corrupt Profile state, path-integrity, or SQLite errors.
    pub fn logical_session_history(
        &self,
        profile_id: AgentProfileId,
    ) -> Result<AgentProfileLogicalSessionHistory, AgentProfileError> {
        self.ensure_live_binding()?;
        if load_profile(&self.connection, profile_id)?.is_none() {
            return Err(AgentProfileError::ProfileMissing(profile_id));
        }
        let query_limit = i64::try_from(MAX_AGENT_PROFILE_LOGICAL_SESSION_LINKS + 1)
            .map_err(|_| AgentProfileError::Corrupt("logical Session history limit"))?;
        let mut statement = self.connection.prepare(
            "SELECT request_id,profile_id,profile_revision,session_id,session_revision,linked_at_ms \
             FROM agent_profile_logical_sessions WHERE profile_id=?1 \
             ORDER BY linked_at_ms DESC, request_id DESC LIMIT ?2",
        )?;
        let mut rows = statement.query(params![profile_id.into_uuid().as_bytes(), query_limit])?;
        let mut links = Vec::new();
        while let Some(row) = rows.next()? {
            if links.len() >= MAX_AGENT_PROFILE_LOGICAL_SESSION_LINKS {
                return Ok(AgentProfileLogicalSessionHistory {
                    links,
                    truncated: true,
                });
            }
            let raw = RawLogicalSessionLink {
                request_id: row.get(0)?,
                profile_id: row.get(1)?,
                profile_revision: row.get(2)?,
                session_id: row.get(3)?,
                session_revision: row.get(4)?,
                linked_at: row.get(5)?,
            };
            links.push(decode_logical_session_link(&raw)?);
        }
        Ok(AgentProfileLogicalSessionHistory {
            links,
            truncated: false,
        })
    }

    fn ensure_live_binding(&self) -> Result<(), AgentProfileError> {
        ensure_directory_identity(&self.database_parent, self.database_parent_identity)?;
        let current = inspect_regular_file(&self.path, false)?.ok_or_else(|| {
            AgentProfileError::DatabasePathInvariant {
                path: self.path.clone(),
                detail: "opened catalog database disappeared",
            }
        })?;
        if current != self.database_identity {
            return Err(AgentProfileError::DatabasePathChanged(self.path.clone()));
        }
        Ok(())
    }
}

fn validate_text(
    field: &'static str,
    value: &str,
    maximum: usize,
) -> Result<(), AgentProfileError> {
    let actual = value.len();
    if actual == 0 || actual > maximum {
        return Err(AgentProfileError::InvalidText {
            field,
            actual,
            maximum,
        });
    }
    Ok(())
}

fn validate_optional_text(
    field: &'static str,
    value: Option<&str>,
    maximum: usize,
) -> Result<(), AgentProfileError> {
    if let Some(value) = value {
        validate_text(field, value, maximum)?;
    }
    Ok(())
}

fn canonical_catalog_path(requested_path: &Path) -> Result<PathBuf, AgentProfileError> {
    let parent = requested_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| AgentProfileError::MissingParent(requested_path.to_path_buf()))?;
    inspect_directory(parent)?;
    let canonical_parent =
        fs::canonicalize(parent).map_err(|source| AgentProfileError::PathIo {
            path: parent.to_path_buf(),
            source,
        })?;
    let file_name = requested_path
        .file_name()
        .ok_or_else(|| AgentProfileError::MissingParent(requested_path.to_path_buf()))?;
    Ok(canonical_parent.join(file_name))
}

fn sidecar_path(database: &Path, suffix: &str) -> PathBuf {
    let mut file_name = database
        .file_name()
        .map_or_else(std::ffi::OsString::new, std::ffi::OsStr::to_os_string);
    file_name.push(suffix);
    database.with_file_name(file_name)
}

fn inspect_regular_file(
    path: &Path,
    allow_missing: bool,
) -> Result<Option<FileIdentity>, AgentProfileError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(source) if allow_missing && source.kind() == io::ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(source) => {
            return Err(AgentProfileError::PathIo {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(AgentProfileError::DatabasePathSymlink(path.to_path_buf()));
    }
    if !metadata.is_file() {
        return Err(AgentProfileError::DatabasePathNotRegular(
            path.to_path_buf(),
        ));
    }
    let identity = FileIdentity::from_metadata(&metadata);
    if identity.links != 1 {
        return Err(AgentProfileError::DatabasePathHardlink {
            path: path.to_path_buf(),
            links: identity.links,
        });
    }
    Ok(Some(identity))
}

fn inspect_directory(path: &Path) -> Result<DirectoryIdentity, AgentProfileError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| AgentProfileError::PathIo {
        path: path.to_path_buf(),
        source,
    })?;
    if metadata.file_type().is_symlink() {
        return Err(AgentProfileError::DatabaseParentSymlink(path.to_path_buf()));
    }
    if !metadata.is_dir() {
        return Err(AgentProfileError::DatabaseParentNotDirectory(
            path.to_path_buf(),
        ));
    }
    Ok(DirectoryIdentity::from_metadata(&metadata))
}

fn ensure_directory_identity(
    path: &Path,
    expected: DirectoryIdentity,
) -> Result<(), AgentProfileError> {
    if inspect_directory(path)? != expected {
        return Err(AgentProfileError::DatabaseParentChanged(path.to_path_buf()));
    }
    Ok(())
}

fn configure(connection: &Connection) -> Result<(), AgentProfileError> {
    connection.busy_timeout(BUSY_TIMEOUT)?;
    connection.pragma_update(None, "foreign_keys", true)?;
    for attempt in 0..JOURNAL_MODE_RETRY_LIMIT {
        match connection.pragma_update(None, "journal_mode", "WAL") {
            Ok(()) => break,
            Err(error) if is_busy_error(&error) && attempt + 1 < JOURNAL_MODE_RETRY_LIMIT => {
                std::thread::sleep(JOURNAL_MODE_RETRY_DELAY);
            }
            Err(error) => return Err(error.into()),
        }
    }
    connection.pragma_update(None, "synchronous", "FULL")?;
    Ok(())
}

fn is_busy_error(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(
            error,
            _
        ) if matches!(
            error.code,
            rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
        )
    )
}

fn initialize_or_validate(connection: &mut Connection) -> Result<(), AgentProfileError> {
    let ownership = catalog_ownership(connection)?;
    match ownership {
        (0, 0, 0) => create_schema(connection),
        (AGENT_PROFILE_APPLICATION_ID, 1, 3) => {
            migrate_v1_to_v2(connection)?;
            migrate_v2_to_v3(connection)?;
            migrate_v3_to_v4(connection)?;
            migrate_v4_to_v5(connection)?;
            migrate_v5_to_v6(connection)
        }
        (AGENT_PROFILE_APPLICATION_ID, 2, 4) => {
            migrate_v2_to_v3(connection)?;
            migrate_v3_to_v4(connection)?;
            migrate_v4_to_v5(connection)?;
            migrate_v5_to_v6(connection)
        }
        (AGENT_PROFILE_APPLICATION_ID, AGENT_PROFILE_V3_SCHEMA_VERSION, 6) => {
            migrate_v3_to_v4(connection)?;
            migrate_v4_to_v5(connection)?;
            migrate_v5_to_v6(connection)
        }
        (AGENT_PROFILE_APPLICATION_ID, AGENT_PROFILE_V4_SCHEMA_VERSION, 7) => {
            migrate_v4_to_v5(connection)?;
            migrate_v5_to_v6(connection)
        }
        (AGENT_PROFILE_APPLICATION_ID, AGENT_PROFILE_V5_SCHEMA_VERSION, 9) => {
            migrate_v5_to_v6(connection)
        }
        (AGENT_PROFILE_APPLICATION_ID, AGENT_PROFILE_SCHEMA_VERSION, 11) => {
            validate_schema(connection)
        }
        (application_id, user_version, object_count) => Err(AgentProfileError::Ownership {
            application_id,
            user_version,
            object_count,
        }),
    }
}

fn catalog_ownership(connection: &Connection) -> Result<(i64, i64, i64), AgentProfileError> {
    let application_id: i64 =
        connection.pragma_query_value(None, "application_id", |row| row.get(0))?;
    let user_version: i64 =
        connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    let object_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
        [],
        |row| row.get(0),
    )?;
    Ok((application_id, user_version, object_count))
}

fn create_schema(connection: &mut Connection) -> Result<(), AgentProfileError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let ownership = catalog_ownership(&transaction)?;
    match ownership {
        (0, 0, 0) => {
            transaction.execute_batch(AGENT_PROFILES_TABLE_SQL)?;
            transaction.execute_batch(AGENT_PROFILE_CAPABILITIES_TABLE_SQL)?;
            transaction.execute_batch(AGENT_PROFILE_LOGICAL_SESSIONS_TABLE_SQL)?;
            transaction.execute_batch(AGENT_PROFILE_RUNTIME_SESSIONS_TABLE_SQL)?;
            resolver::create_schema(&transaction)?;
            deletion::create_schema(&transaction)?;
            deletion::create_manifest_schema(&transaction)?;
            transaction.execute_batch(AGENT_PROFILE_MUTATION_RECEIPTS_TABLE_SQL)?;
            transaction.execute_batch(AGENT_PROFILE_MUTATION_CAPABILITIES_TABLE_SQL)?;
            transaction.pragma_update(None, "application_id", AGENT_PROFILE_APPLICATION_ID)?;
            transaction.pragma_update(None, "user_version", AGENT_PROFILE_SCHEMA_VERSION)?;
        }
        (AGENT_PROFILE_APPLICATION_ID, AGENT_PROFILE_SCHEMA_VERSION, 11) => {
            transaction.commit()?;
            return validate_schema(connection);
        }
        (application_id, user_version, object_count) => {
            return Err(AgentProfileError::Ownership {
                application_id,
                user_version,
                object_count,
            });
        }
    }
    transaction.commit()?;
    validate_schema(connection)
}

fn migrate_v1_to_v2(connection: &mut Connection) -> Result<(), AgentProfileError> {
    validate_v1_schema(connection)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if catalog_ownership(&transaction)? != (AGENT_PROFILE_APPLICATION_ID, 1, 3) {
        return Err(AgentProfileError::Corrupt(
            "Agent Profile v1 migration ownership changed",
        ));
    }
    transaction.execute_batch(AGENT_PROFILE_RUNTIME_SESSIONS_TABLE_SQL)?;
    transaction.pragma_update(None, "user_version", 2)?;
    transaction.commit()?;
    validate_v2_schema(connection)
}

fn migrate_v2_to_v3(connection: &mut Connection) -> Result<(), AgentProfileError> {
    validate_v2_schema(connection)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if catalog_ownership(&transaction)? != (AGENT_PROFILE_APPLICATION_ID, 2, 4) {
        return Err(AgentProfileError::Corrupt(
            "Agent Profile v2 migration ownership changed",
        ));
    }
    resolver::create_schema(&transaction)?;
    transaction.pragma_update(None, "user_version", AGENT_PROFILE_V3_SCHEMA_VERSION)?;
    transaction.commit()?;
    validate_v3_schema(connection)
}

fn migrate_v3_to_v4(connection: &mut Connection) -> Result<(), AgentProfileError> {
    validate_v3_schema(connection)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if catalog_ownership(&transaction)?
        != (
            AGENT_PROFILE_APPLICATION_ID,
            AGENT_PROFILE_V3_SCHEMA_VERSION,
            6,
        )
    {
        return Err(AgentProfileError::Corrupt(
            "Agent Profile v3 migration ownership changed",
        ));
    }
    deletion::create_schema(&transaction)?;
    transaction.pragma_update(None, "user_version", AGENT_PROFILE_V4_SCHEMA_VERSION)?;
    transaction.commit()?;
    validate_v4_schema(connection)
}

fn migrate_v4_to_v5(connection: &mut Connection) -> Result<(), AgentProfileError> {
    validate_v4_schema(connection)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if catalog_ownership(&transaction)?
        != (
            AGENT_PROFILE_APPLICATION_ID,
            AGENT_PROFILE_V4_SCHEMA_VERSION,
            7,
        )
    {
        return Err(AgentProfileError::Corrupt(
            "Agent Profile v4 migration ownership changed",
        ));
    }
    deletion::create_manifest_schema(&transaction)?;
    transaction.pragma_update(None, "user_version", AGENT_PROFILE_V5_SCHEMA_VERSION)?;
    transaction.commit()?;
    validate_v5_schema(connection)
}

fn migrate_v5_to_v6(connection: &mut Connection) -> Result<(), AgentProfileError> {
    validate_v5_schema(connection)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if catalog_ownership(&transaction)?
        != (
            AGENT_PROFILE_APPLICATION_ID,
            AGENT_PROFILE_V5_SCHEMA_VERSION,
            9,
        )
    {
        return Err(AgentProfileError::Corrupt(
            "Agent Profile v5 migration ownership changed",
        ));
    }
    transaction.execute_batch(AGENT_PROFILE_MUTATION_RECEIPTS_TABLE_SQL)?;
    transaction.execute_batch(AGENT_PROFILE_MUTATION_CAPABILITIES_TABLE_SQL)?;
    let profile_ids = transaction
        .prepare("SELECT profile_id FROM agent_profiles ORDER BY profile_id")?
        .query_map([], |row| row.get::<_, Vec<u8>>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    for raw_profile_id in profile_ids {
        let profile_id = decode_profile_id(&raw_profile_id, "profile_id")?;
        let profile = load_profile(&transaction, profile_id)?
            .ok_or(AgentProfileError::ProfileMissing(profile_id))?;
        insert_profile_mutation_receipt(
            &transaction,
            profile.request_id(),
            None,
            ProfileMutationKind::OriginalCreate,
            &original_create_snapshot(&profile),
        )?;
    }
    transaction.pragma_update(None, "user_version", AGENT_PROFILE_SCHEMA_VERSION)?;
    transaction.commit()?;
    validate_schema(connection)
}

fn validate_v1_schema(connection: &Connection) -> Result<(), AgentProfileError> {
    let quick: String = connection.query_row("PRAGMA quick_check(1)", [], |row| row.get(0))?;
    if quick != "ok" {
        return Err(AgentProfileError::Integrity);
    }
    let ownership = catalog_ownership(connection)?;
    if ownership != (AGENT_PROFILE_APPLICATION_ID, 1, 3) {
        return Err(AgentProfileError::Ownership {
            application_id: ownership.0,
            user_version: ownership.1,
            object_count: ownership.2,
        });
    }
    validate_schema_sql(connection, "agent_profiles", AGENT_PROFILES_TABLE_SQL)?;
    validate_schema_sql(
        connection,
        "agent_profile_capabilities",
        AGENT_PROFILE_CAPABILITIES_TABLE_SQL,
    )?;
    validate_schema_sql(
        connection,
        "agent_profile_logical_sessions",
        AGENT_PROFILE_LOGICAL_SESSIONS_TABLE_SQL,
    )?;
    validate_profile_authority_boundary(connection)?;
    validate_foreign_keys(connection)
}

fn validate_v2_schema(connection: &Connection) -> Result<(), AgentProfileError> {
    let quick: String = connection.query_row("PRAGMA quick_check(1)", [], |row| row.get(0))?;
    if quick != "ok" {
        return Err(AgentProfileError::Integrity);
    }
    let ownership = catalog_ownership(connection)?;
    if ownership != (AGENT_PROFILE_APPLICATION_ID, 2, 4) {
        return Err(AgentProfileError::Ownership {
            application_id: ownership.0,
            user_version: ownership.1,
            object_count: ownership.2,
        });
    }
    validate_schema_sql(connection, "agent_profiles", AGENT_PROFILES_TABLE_SQL)?;
    validate_schema_sql(
        connection,
        "agent_profile_capabilities",
        AGENT_PROFILE_CAPABILITIES_TABLE_SQL,
    )?;
    validate_schema_sql(
        connection,
        "agent_profile_logical_sessions",
        AGENT_PROFILE_LOGICAL_SESSIONS_TABLE_SQL,
    )?;
    validate_schema_sql(
        connection,
        "agent_profile_runtime_sessions",
        AGENT_PROFILE_RUNTIME_SESSIONS_TABLE_SQL,
    )?;
    validate_profile_authority_boundary(connection)?;
    validate_foreign_keys(connection)
}

fn validate_v3_schema(connection: &Connection) -> Result<(), AgentProfileError> {
    let quick: String = connection.query_row("PRAGMA quick_check(1)", [], |row| row.get(0))?;
    if quick != "ok" {
        return Err(AgentProfileError::Integrity);
    }
    let ownership = catalog_ownership(connection)?;
    if ownership
        != (
            AGENT_PROFILE_APPLICATION_ID,
            AGENT_PROFILE_V3_SCHEMA_VERSION,
            6,
        )
    {
        return Err(AgentProfileError::Ownership {
            application_id: ownership.0,
            user_version: ownership.1,
            object_count: ownership.2,
        });
    }
    validate_schema_sql(connection, "agent_profiles", AGENT_PROFILES_TABLE_SQL)?;
    validate_schema_sql(
        connection,
        "agent_profile_capabilities",
        AGENT_PROFILE_CAPABILITIES_TABLE_SQL,
    )?;
    validate_schema_sql(
        connection,
        "agent_profile_logical_sessions",
        AGENT_PROFILE_LOGICAL_SESSIONS_TABLE_SQL,
    )?;
    validate_schema_sql(
        connection,
        "agent_profile_runtime_sessions",
        AGENT_PROFILE_RUNTIME_SESSIONS_TABLE_SQL,
    )?;
    resolver::validate_schema(connection)?;
    validate_profile_authority_boundary(connection)?;
    validate_foreign_keys(connection)
}

fn validate_v4_schema(connection: &Connection) -> Result<(), AgentProfileError> {
    let quick: String = connection.query_row("PRAGMA quick_check(1)", [], |row| row.get(0))?;
    if quick != "ok" {
        return Err(AgentProfileError::Integrity);
    }
    let ownership = catalog_ownership(connection)?;
    if ownership
        != (
            AGENT_PROFILE_APPLICATION_ID,
            AGENT_PROFILE_V4_SCHEMA_VERSION,
            7,
        )
    {
        return Err(AgentProfileError::Ownership {
            application_id: ownership.0,
            user_version: ownership.1,
            object_count: ownership.2,
        });
    }
    validate_schema_sql(connection, "agent_profiles", AGENT_PROFILES_TABLE_SQL)?;
    validate_schema_sql(
        connection,
        "agent_profile_capabilities",
        AGENT_PROFILE_CAPABILITIES_TABLE_SQL,
    )?;
    validate_schema_sql(
        connection,
        "agent_profile_logical_sessions",
        AGENT_PROFILE_LOGICAL_SESSIONS_TABLE_SQL,
    )?;
    validate_schema_sql(
        connection,
        "agent_profile_runtime_sessions",
        AGENT_PROFILE_RUNTIME_SESSIONS_TABLE_SQL,
    )?;
    resolver::validate_schema(connection)?;
    deletion::validate_v4_schema(connection)?;
    validate_profile_authority_boundary(connection)?;
    validate_foreign_keys(connection)
}

fn validate_v5_schema(connection: &Connection) -> Result<(), AgentProfileError> {
    validate_v5_schema_shape(connection, AGENT_PROFILE_V5_SCHEMA_VERSION, 9)?;
    validate_foreign_keys(connection)
}

fn validate_schema(connection: &Connection) -> Result<(), AgentProfileError> {
    validate_v5_schema_shape(connection, AGENT_PROFILE_SCHEMA_VERSION, 11)?;
    validate_schema_sql(
        connection,
        "agent_profile_mutation_receipts",
        AGENT_PROFILE_MUTATION_RECEIPTS_TABLE_SQL,
    )?;
    validate_schema_sql(
        connection,
        "agent_profile_mutation_capabilities",
        AGENT_PROFILE_MUTATION_CAPABILITIES_TABLE_SQL,
    )?;
    validate_foreign_keys(connection)
}

fn validate_v5_schema_shape(
    connection: &Connection,
    version: i64,
    object_count: i64,
) -> Result<(), AgentProfileError> {
    let quick: String = connection.query_row("PRAGMA quick_check(1)", [], |row| row.get(0))?;
    if quick != "ok" {
        return Err(AgentProfileError::Integrity);
    }
    let ownership = catalog_ownership(connection)?;
    if ownership != (AGENT_PROFILE_APPLICATION_ID, version, object_count) {
        return Err(AgentProfileError::Ownership {
            application_id: ownership.0,
            user_version: ownership.1,
            object_count: ownership.2,
        });
    }
    validate_schema_sql(connection, "agent_profiles", AGENT_PROFILES_TABLE_SQL)?;
    validate_schema_sql(
        connection,
        "agent_profile_capabilities",
        AGENT_PROFILE_CAPABILITIES_TABLE_SQL,
    )?;
    validate_schema_sql(
        connection,
        "agent_profile_logical_sessions",
        AGENT_PROFILE_LOGICAL_SESSIONS_TABLE_SQL,
    )?;
    validate_schema_sql(
        connection,
        "agent_profile_runtime_sessions",
        AGENT_PROFILE_RUNTIME_SESSIONS_TABLE_SQL,
    )?;
    resolver::validate_schema(connection)?;
    deletion::validate_schema(connection)?;
    validate_profile_authority_boundary(connection)
}

fn validate_profile_authority_boundary(connection: &Connection) -> Result<(), AgentProfileError> {
    let forbidden_columns: i64 = connection.query_row(
        "SELECT COUNT(*) FROM pragma_table_info('agent_profiles')
         WHERE name IN ('workspace_id','team_id','participant_id','session_id','agent_id',
                        'assignment_id','writer_lease_id','vote_id','work_credit')",
        [],
        |row| row.get(0),
    )?;
    if forbidden_columns != 0 {
        return Err(AgentProfileError::Schema(
            "agent_profiles contains scoped authority columns",
        ));
    }
    Ok(())
}

fn validate_foreign_keys(connection: &Connection) -> Result<(), AgentProfileError> {
    if connection
        .prepare("PRAGMA foreign_key_check")?
        .query([])?
        .next()?
        .is_some()
    {
        return Err(AgentProfileError::Integrity);
    }
    Ok(())
}

fn validate_schema_sql(
    connection: &Connection,
    name: &'static str,
    expected: &str,
) -> Result<(), AgentProfileError> {
    let sql: Option<String> = connection
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?1",
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

fn normalize_sql(value: &str) -> String {
    value
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .replace("( ", "(")
        .replace(" )", ")")
        .replace(" ,", ",")
}

fn profile_id_by_request(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<AgentProfileId>, AgentProfileError> {
    let raw: Option<Vec<u8>> = connection
        .query_row(
            "SELECT profile_id FROM agent_profiles WHERE request_id=?1",
            params![request_id.as_bytes()],
            |row| row.get(0),
        )
        .optional()?;
    raw.map(|bytes| decode_profile_id(&bytes, "profile_id"))
        .transpose()
}

fn logical_session_link_by_request(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<AgentProfileLogicalSessionLink>, AgentProfileError> {
    load_logical_session_link(
        connection,
        "SELECT request_id,profile_id,profile_revision,session_id,session_revision,linked_at_ms FROM agent_profile_logical_sessions WHERE request_id=?1",
        request_id.as_bytes(),
    )
}

fn logical_session_link_by_session(
    connection: &Connection,
    session_id: forge_domain::AgentSessionId,
) -> Result<Option<AgentProfileLogicalSessionLink>, AgentProfileError> {
    load_logical_session_link(
        connection,
        "SELECT request_id,profile_id,profile_revision,session_id,session_revision,linked_at_ms FROM agent_profile_logical_sessions WHERE session_id=?1",
        session_id.into_uuid().as_bytes(),
    )
}

fn load_logical_session_link(
    connection: &Connection,
    sql: &str,
    key: &[u8],
) -> Result<Option<AgentProfileLogicalSessionLink>, AgentProfileError> {
    let raw: Option<RawLogicalSessionLink> = connection
        .query_row(sql, params![key], |row| {
            Ok(RawLogicalSessionLink {
                request_id: row.get(0)?,
                profile_id: row.get(1)?,
                profile_revision: row.get(2)?,
                session_id: row.get(3)?,
                session_revision: row.get(4)?,
                linked_at: row.get(5)?,
            })
        })
        .optional()?;
    raw.as_ref().map(decode_logical_session_link).transpose()
}

#[derive(Debug)]
struct RawLogicalSessionLink {
    request_id: Vec<u8>,
    profile_id: Vec<u8>,
    profile_revision: i64,
    session_id: Vec<u8>,
    session_revision: i64,
    linked_at: i64,
}

fn decode_logical_session_link(
    raw: &RawLogicalSessionLink,
) -> Result<AgentProfileLogicalSessionLink, AgentProfileError> {
    Ok(AgentProfileLogicalSessionLink {
        request_id: decode_uuid("link_request_id", &raw.request_id)?,
        profile_id: decode_profile_id(&raw.profile_id, "profile_id")?,
        profile_revision: AgentProfileRevision::new(raw.profile_revision)?,
        session_id: forge_domain::AgentSessionId::from_uuid(decode_uuid(
            "session_id",
            &raw.session_id,
        )?),
        session_revision: forge_domain::AgentSessionRevision::new(raw.session_revision)
            .map_err(|_| AgentProfileError::Corrupt("logical Session link revision"))?,
        linked_at: TimestampMillis::new(raw.linked_at)?,
    })
}

fn insert_runtime_session(
    transaction: &rusqlite::Transaction<'_>,
    session: &AgentProfileRuntimeSession,
) -> Result<(), AgentProfileError> {
    transaction.execute(
        "INSERT INTO agent_profile_runtime_sessions(
            runtime_session_id,request_id,profile_id,provider_family,
            provider_conversation_id,state,opened_at_ms,last_seen_at_ms,
            closed_at_ms,revision
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        params![
            session.id.into_uuid().as_bytes(),
            session.request_id.as_bytes(),
            session.profile_id.into_uuid().as_bytes(),
            encode_provider(session.provider_family),
            session.provider_conversation_id,
            encode_runtime_state(session.state),
            session.opened_at.get(),
            session.last_seen_at.get(),
            session.closed_at.map(TimestampMillis::get),
            session.revision.get(),
        ],
    )?;
    Ok(())
}

fn runtime_session_by_request(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<AgentProfileRuntimeSession>, AgentProfileError> {
    let id: Option<Vec<u8>> = connection
        .query_row(
            "SELECT runtime_session_id FROM agent_profile_runtime_sessions WHERE request_id=?1",
            params![request_id.as_bytes()],
            |row| row.get(0),
        )
        .optional()?;
    id.map(|raw| {
        AgentProfileRuntimeSessionId::from_uuid(decode_uuid("runtime_session_id", &raw)?).and_then(
            |id| {
                load_runtime_session(connection, id)?
                    .ok_or(AgentProfileError::RuntimeSessionMissing(id))
            },
        )
    })
    .transpose()
}

fn runtime_session_id_by_conversation(
    connection: &Connection,
    provider: AgentProfileProviderFamily,
    conversation_id: &str,
) -> Result<Option<AgentProfileRuntimeSessionId>, AgentProfileError> {
    let raw: Option<Vec<u8>> = connection
        .query_row(
            "SELECT runtime_session_id FROM agent_profile_runtime_sessions
             WHERE provider_family=?1 AND provider_conversation_id=?2",
            params![encode_provider(provider), conversation_id],
            |row| row.get(0),
        )
        .optional()?;
    raw.map(|value| {
        AgentProfileRuntimeSessionId::from_uuid(decode_uuid("runtime_session_id", &value)?)
    })
    .transpose()
}

#[derive(Debug)]
struct RawRuntimeSession {
    id: Vec<u8>,
    request_id: Vec<u8>,
    profile_id: Vec<u8>,
    provider_family: i64,
    provider_conversation_id: Option<String>,
    state: i64,
    opened_at: i64,
    last_seen_at: i64,
    closed_at: Option<i64>,
    revision: i64,
}

fn load_runtime_session(
    connection: &Connection,
    id: AgentProfileRuntimeSessionId,
) -> Result<Option<AgentProfileRuntimeSession>, AgentProfileError> {
    let raw = connection
        .query_row(
            "SELECT runtime_session_id,request_id,profile_id,provider_family,
                    provider_conversation_id,state,opened_at_ms,last_seen_at_ms,
                    closed_at_ms,revision
             FROM agent_profile_runtime_sessions WHERE runtime_session_id=?1",
            params![id.into_uuid().as_bytes()],
            |row| {
                Ok(RawRuntimeSession {
                    id: row.get(0)?,
                    request_id: row.get(1)?,
                    profile_id: row.get(2)?,
                    provider_family: row.get(3)?,
                    provider_conversation_id: row.get(4)?,
                    state: row.get(5)?,
                    opened_at: row.get(6)?,
                    last_seen_at: row.get(7)?,
                    closed_at: row.get(8)?,
                    revision: row.get(9)?,
                })
            },
        )
        .optional()?;
    raw.as_ref().map(decode_runtime_session).transpose()
}

fn decode_runtime_session(
    raw: &RawRuntimeSession,
) -> Result<AgentProfileRuntimeSession, AgentProfileError> {
    validate_optional_text(
        "provider_conversation_id",
        raw.provider_conversation_id.as_deref(),
        MAX_PROVIDER_CONVERSATION_ID_BYTES,
    )?;
    let state = decode_runtime_state(raw.state)?;
    let opened_at = TimestampMillis::new(raw.opened_at)?;
    let last_seen_at = TimestampMillis::new(raw.last_seen_at)?;
    let closed_at = raw.closed_at.map(TimestampMillis::new).transpose()?;
    if last_seen_at.get() < opened_at.get()
        || (state == AgentProfileRuntimeSessionState::Closed) != closed_at.is_some()
        || (runtime_state_requires_identity(state) && raw.provider_conversation_id.is_none())
    {
        return Err(AgentProfileError::Corrupt("provider Runtime Session state"));
    }
    Ok(AgentProfileRuntimeSession {
        id: AgentProfileRuntimeSessionId::from_uuid(decode_uuid("runtime_session_id", &raw.id)?)?,
        request_id: decode_uuid("runtime_request_id", &raw.request_id)?,
        profile_id: decode_profile_id(&raw.profile_id, "runtime_profile_id")?,
        provider_family: decode_provider(raw.provider_family)?,
        provider_conversation_id: raw.provider_conversation_id.clone(),
        state,
        opened_at,
        last_seen_at,
        closed_at,
        revision: AgentProfileRuntimeSessionRevision::new(raw.revision)?,
    })
}

fn runtime_session_history(
    connection: &Connection,
    profile_id: AgentProfileId,
) -> Result<AgentProfileRuntimeSessionHistory, AgentProfileError> {
    let limit = i64::try_from(MAX_AGENT_PROFILE_RUNTIME_SESSIONS + 1)
        .map_err(|_| AgentProfileError::Corrupt("runtime Session history bound"))?;
    let mut statement = connection.prepare(
        "SELECT runtime_session_id FROM agent_profile_runtime_sessions
         WHERE profile_id=?1 ORDER BY opened_at_ms DESC,runtime_session_id DESC LIMIT ?2",
    )?;
    let ids = statement
        .query_map(params![profile_id.into_uuid().as_bytes(), limit], |row| {
            row.get::<_, Vec<u8>>(0)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let truncated = ids.len() > MAX_AGENT_PROFILE_RUNTIME_SESSIONS;
    let mut sessions = Vec::with_capacity(ids.len().min(MAX_AGENT_PROFILE_RUNTIME_SESSIONS));
    for raw in ids.into_iter().take(MAX_AGENT_PROFILE_RUNTIME_SESSIONS) {
        let id = AgentProfileRuntimeSessionId::from_uuid(decode_uuid("runtime_session_id", &raw)?)?;
        sessions.push(
            load_runtime_session(connection, id)?
                .ok_or(AgentProfileError::RuntimeSessionMissing(id))?,
        );
    }
    Ok(AgentProfileRuntimeSessionHistory {
        sessions,
        truncated,
    })
}

fn runtime_sessions_page(
    connection: &Connection,
    profile_id: AgentProfileId,
    after_runtime_id: Option<AgentProfileRuntimeSessionId>,
    page_size: usize,
) -> Result<AgentProfileRuntimeSessionPage, AgentProfileError> {
    let lookahead_limit = i64::try_from(page_size + 1)
        .map_err(|_| AgentProfileError::Corrupt("runtime Session page bound"))?;
    let after_runtime_id = after_runtime_id.map(|id| id.into_uuid().as_bytes().to_vec());
    let mut statement = connection.prepare(
        "SELECT runtime_session_id,request_id,profile_id,provider_family,
                provider_conversation_id,state,opened_at_ms,last_seen_at_ms,
                closed_at_ms,revision
         FROM agent_profile_runtime_sessions
         WHERE profile_id=?1 AND (?2 IS NULL OR runtime_session_id>?2)
         ORDER BY runtime_session_id ASC LIMIT ?3",
    )?;
    let mut rows = statement
        .query_map(
            params![
                profile_id.into_uuid().as_bytes(),
                after_runtime_id,
                lookahead_limit,
            ],
            |row| {
                Ok(RawRuntimeSession {
                    id: row.get(0)?,
                    request_id: row.get(1)?,
                    profile_id: row.get(2)?,
                    provider_family: row.get(3)?,
                    provider_conversation_id: row.get(4)?,
                    state: row.get(5)?,
                    opened_at: row.get(6)?,
                    last_seen_at: row.get(7)?,
                    closed_at: row.get(8)?,
                    revision: row.get(9)?,
                })
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;
    let has_more = rows.len() > page_size;
    let next_runtime_id = if has_more {
        Some(AgentProfileRuntimeSessionId::from_uuid(decode_uuid(
            "runtime_session_id",
            &rows[page_size - 1].id,
        )?)?)
    } else {
        None
    };
    rows.truncate(page_size);
    let sessions = rows
        .iter()
        .map(|row| {
            let session = decode_runtime_session(row)?;
            if session.profile_id() != profile_id {
                return Err(AgentProfileError::Corrupt(
                    "Runtime Session page crossed Profile scope",
                ));
            }
            Ok(session)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AgentProfileRuntimeSessionPage {
        sessions,
        next_runtime_id,
    })
}

fn ensure_runtime_observation(
    current: &AgentProfileRuntimeSession,
    observed_at: TimestampMillis,
) -> Result<(), AgentProfileError> {
    if observed_at.get() < current.last_seen_at.get() {
        return Err(AgentProfileError::RuntimeSessionClockRegression);
    }
    Ok(())
}

fn next_runtime_revision(
    current: AgentProfileRuntimeSessionRevision,
) -> Result<AgentProfileRuntimeSessionRevision, AgentProfileError> {
    current
        .get()
        .checked_add(1)
        .ok_or(AgentProfileError::RuntimeSessionRevisionOverflow)
        .and_then(AgentProfileRuntimeSessionRevision::new)
}

const fn runtime_state_requires_identity(state: AgentProfileRuntimeSessionState) -> bool {
    matches!(
        state,
        AgentProfileRuntimeSessionState::Ready
            | AgentProfileRuntimeSessionState::Active
            | AgentProfileRuntimeSessionState::Suspending
            | AgentProfileRuntimeSessionState::Offline
            | AgentProfileRuntimeSessionState::Waking
    )
}

const fn runtime_transition_allowed(
    from: AgentProfileRuntimeSessionState,
    to: AgentProfileRuntimeSessionState,
) -> bool {
    matches!(
        (from, to),
        (
            AgentProfileRuntimeSessionState::Provisioning,
            AgentProfileRuntimeSessionState::Ready
                | AgentProfileRuntimeSessionState::Failed
                | AgentProfileRuntimeSessionState::Closed
        ) | (
            AgentProfileRuntimeSessionState::Ready,
            AgentProfileRuntimeSessionState::Active
                | AgentProfileRuntimeSessionState::Suspending
                | AgentProfileRuntimeSessionState::Failed
                | AgentProfileRuntimeSessionState::Closed
        ) | (
            AgentProfileRuntimeSessionState::Active,
            AgentProfileRuntimeSessionState::Ready
                | AgentProfileRuntimeSessionState::Suspending
                | AgentProfileRuntimeSessionState::Failed
                | AgentProfileRuntimeSessionState::Closed
        ) | (
            AgentProfileRuntimeSessionState::Suspending,
            AgentProfileRuntimeSessionState::Offline
                | AgentProfileRuntimeSessionState::Failed
                | AgentProfileRuntimeSessionState::Closed
        ) | (
            AgentProfileRuntimeSessionState::Offline | AgentProfileRuntimeSessionState::Failed,
            AgentProfileRuntimeSessionState::Waking | AgentProfileRuntimeSessionState::Closed
        ) | (
            AgentProfileRuntimeSessionState::Waking,
            AgentProfileRuntimeSessionState::Ready
                | AgentProfileRuntimeSessionState::Offline
                | AgentProfileRuntimeSessionState::Failed
                | AgentProfileRuntimeSessionState::Closed
        )
    )
}

#[derive(Debug)]
struct StoredProfileMutationReceipt {
    request_id: Uuid,
    profile: AgentProfile,
    expected_revision: Option<AgentProfileRevision>,
    kind: ProfileMutationKind,
}

impl StoredProfileMutationReceipt {
    fn matches_command(&self, command: &ProfileMutationCommand) -> bool {
        if self.profile.id() != command.profile_id()
            || self.kind != command.kind()
            || self.expected_revision != Some(command.expected_revision())
        {
            return false;
        }
        match command.operation() {
            ProfileMutation::Update(configuration) => {
                self.profile.display_name() == configuration.display_name()
                    && self.profile.preferred_model() == configuration.preferred_model()
                    && self.profile.preferred_reasoning() == configuration.preferred_reasoning()
                    && self.profile.base_capabilities() == configuration.capabilities()
            }
            ProfileMutation::Disable | ProfileMutation::Retire => true,
        }
    }

    fn into_public(self, replayed: bool) -> ProfileMutationReceipt {
        ProfileMutationReceipt::committed(
            self.request_id,
            self.profile,
            self.expected_revision,
            self.kind,
            replayed,
        )
    }
}

#[derive(Debug)]
struct RawProfileMutationReceipt {
    request_id: Vec<u8>,
    profile_id: Vec<u8>,
    operation_kind: i64,
    expected_revision: Option<i64>,
    creation_request_id: Vec<u8>,
    stable_name: String,
    display_name: String,
    provider_family: i64,
    preferred_model: Option<String>,
    preferred_reasoning: Option<String>,
    lifecycle_state: i64,
    creation_origin: i64,
    created_at: i64,
    updated_at: i64,
    last_used_at: Option<i64>,
    revision: i64,
}

fn load_profile_mutation_receipt(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<StoredProfileMutationReceipt>, AgentProfileError> {
    let Some(raw) = load_raw_profile_mutation_receipt(connection, request_id)? else {
        return Ok(None);
    };
    decode_profile_mutation_receipt(connection, request_id, raw).map(Some)
}

fn load_raw_profile_mutation_receipt(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<RawProfileMutationReceipt>, AgentProfileError> {
    connection
        .query_row(
            "SELECT request_id,profile_id,operation_kind,expected_revision,
                    creation_request_id,stable_name,display_name,provider_family,
                    preferred_model,preferred_reasoning,lifecycle_state,creation_origin,
                    created_at_ms,updated_at_ms,last_used_at_ms,revision
             FROM agent_profile_mutation_receipts WHERE request_id=?1",
            params![request_id.as_bytes()],
            |row| {
                Ok(RawProfileMutationReceipt {
                    request_id: row.get(0)?,
                    profile_id: row.get(1)?,
                    operation_kind: row.get(2)?,
                    expected_revision: row.get(3)?,
                    creation_request_id: row.get(4)?,
                    stable_name: row.get(5)?,
                    display_name: row.get(6)?,
                    provider_family: row.get(7)?,
                    preferred_model: row.get(8)?,
                    preferred_reasoning: row.get(9)?,
                    lifecycle_state: row.get(10)?,
                    creation_origin: row.get(11)?,
                    created_at: row.get(12)?,
                    updated_at: row.get(13)?,
                    last_used_at: row.get(14)?,
                    revision: row.get(15)?,
                })
            },
        )
        .optional()
        .map_err(Into::into)
}

fn decode_profile_mutation_receipt(
    connection: &Connection,
    request_id: Uuid,
    raw: RawProfileMutationReceipt,
) -> Result<StoredProfileMutationReceipt, AgentProfileError> {
    let stored_request_id = decode_uuid("profile_mutation_request_id", &raw.request_id)?;
    if stored_request_id != request_id {
        return Err(AgentProfileError::Corrupt(
            "Profile mutation receipt request identity",
        ));
    }
    let profile_id = decode_profile_id(&raw.profile_id, "profile_mutation_profile_id")?;
    let kind = ProfileMutationKind::decode(raw.operation_kind)?;
    let expected_revision = raw
        .expected_revision
        .map(AgentProfileRevision::new)
        .transpose()?;
    if (kind == ProfileMutationKind::OriginalCreate) != expected_revision.is_none() {
        return Err(AgentProfileError::Corrupt(
            "Profile mutation receipt revision shape",
        ));
    }
    let creation_request_id = decode_uuid(
        "profile_mutation_creation_request_id",
        &raw.creation_request_id,
    )?;
    validate_text("stable_name", &raw.stable_name, MAX_PROFILE_NAME_BYTES)?;
    validate_text("display_name", &raw.display_name, MAX_PROFILE_NAME_BYTES)?;
    validate_optional_text(
        "preferred_model",
        raw.preferred_model.as_deref(),
        MAX_PROVIDER_PREFERENCE_BYTES,
    )?;
    validate_optional_text(
        "preferred_reasoning",
        raw.preferred_reasoning.as_deref(),
        MAX_PROVIDER_PREFERENCE_BYTES,
    )?;
    let mut capabilities = Vec::new();
    let mut statement = connection.prepare(
        "SELECT capability FROM agent_profile_mutation_capabilities
         WHERE request_id=?1 ORDER BY capability",
    )?;
    let mut rows = statement.query(params![request_id.as_bytes()])?;
    while let Some(row) = rows.next()? {
        if capabilities.len() >= MAX_CAPABILITIES {
            return Err(AgentProfileError::Corrupt(
                "Profile mutation capability count exceeded bound",
            ));
        }
        let capability: String = row.get(0)?;
        validate_text("capability", &capability, MAX_CAPABILITY_BYTES)?;
        if capabilities.last() == Some(&capability) {
            return Err(AgentProfileError::Corrupt(
                "duplicate Profile mutation capability",
            ));
        }
        capabilities.push(capability);
    }
    let profile = AgentProfile {
        id: profile_id,
        request_id: creation_request_id,
        stable_name: raw.stable_name,
        display_name: raw.display_name,
        provider_family: decode_provider(raw.provider_family)?,
        preferred_model: raw.preferred_model,
        preferred_reasoning: raw.preferred_reasoning,
        base_capabilities: capabilities,
        lifecycle_state: decode_lifecycle(raw.lifecycle_state)?,
        creation_origin: decode_origin(raw.creation_origin)?,
        created_at: TimestampMillis::new(raw.created_at)?,
        updated_at: TimestampMillis::new(raw.updated_at)?,
        last_used_at: raw.last_used_at.map(TimestampMillis::new).transpose()?,
        revision: AgentProfileRevision::new(raw.revision)?,
    };
    if profile.updated_at().get() < profile.created_at().get()
        || profile
            .last_used_at()
            .is_some_and(|value| value.get() < profile.created_at().get())
    {
        return Err(AgentProfileError::Corrupt(
            "Profile mutation receipt timestamp ordering",
        ));
    }
    Ok(StoredProfileMutationReceipt {
        request_id: stored_request_id,
        profile,
        expected_revision,
        kind,
    })
}

fn insert_profile_mutation_receipt(
    transaction: &rusqlite::Transaction<'_>,
    request_id: Uuid,
    expected_revision: Option<AgentProfileRevision>,
    kind: ProfileMutationKind,
    profile: &AgentProfile,
) -> Result<(), AgentProfileError> {
    if request_id.is_nil() {
        return Err(AgentProfileError::InvalidIdentity(
            "profile_mutation_request_id",
        ));
    }
    if (kind == ProfileMutationKind::OriginalCreate) != expected_revision.is_none() {
        return Err(AgentProfileError::Corrupt(
            "Profile mutation receipt insert revision shape",
        ));
    }
    transaction.execute(
        "INSERT INTO agent_profile_mutation_receipts(
            request_id,profile_id,operation_kind,expected_revision,
            creation_request_id,stable_name,display_name,provider_family,
            preferred_model,preferred_reasoning,lifecycle_state,creation_origin,
            created_at_ms,updated_at_ms,last_used_at_ms,revision
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
        params![
            request_id.as_bytes(),
            profile.id().into_uuid().as_bytes(),
            kind.encode(),
            expected_revision.map(AgentProfileRevision::get),
            profile.request_id().as_bytes(),
            profile.stable_name(),
            profile.display_name(),
            encode_provider(profile.provider_family()),
            profile.preferred_model(),
            profile.preferred_reasoning(),
            encode_lifecycle(profile.lifecycle_state()),
            encode_origin(profile.creation_origin()),
            profile.created_at().get(),
            profile.updated_at().get(),
            profile.last_used_at().map(TimestampMillis::get),
            profile.revision().get(),
        ],
    )?;
    insert_profile_mutation_capabilities(transaction, request_id, profile.base_capabilities())
}

fn insert_profile_mutation_capabilities(
    transaction: &rusqlite::Transaction<'_>,
    request_id: Uuid,
    capabilities: &[String],
) -> Result<(), AgentProfileError> {
    for capability in capabilities {
        transaction.execute(
            "INSERT INTO agent_profile_mutation_capabilities(request_id,capability)
             VALUES(?1,?2)",
            params![request_id.as_bytes(), capability],
        )?;
    }
    Ok(())
}

fn original_create_snapshot(profile: &AgentProfile) -> AgentProfile {
    AgentProfile {
        lifecycle_state: AgentProfileLifecycleState::Active,
        updated_at: profile.created_at,
        last_used_at: None,
        revision: AgentProfileRevision(1),
        ..profile.clone()
    }
}

fn commit_profile_transaction(
    transaction: rusqlite::Transaction<'_>,
    request_id: Option<Uuid>,
    profile_id: AgentProfileId,
) -> Result<(), AgentProfileError> {
    transaction.commit().map_err(|source| match request_id {
        Some(request_id) => AgentProfileError::ProfileMutationCommitUnconfirmed {
            request_id,
            profile_id,
            source,
        },
        None => AgentProfileError::Sql(source),
    })
}

fn apply_profile_mutation(
    transaction: &rusqlite::Transaction<'_>,
    current: &AgentProfile,
    command: &ProfileMutationCommand,
) -> Result<AgentProfile, AgentProfileError> {
    if current.revision() != command.expected_revision() {
        return Err(AgentProfileError::ProfileRevisionConflict {
            profile_id: command.profile_id(),
            expected: command.expected_revision().get(),
            actual: current.revision().get(),
        });
    }
    match command.operation() {
        ProfileMutation::Update(configuration) => {
            if current.lifecycle_state() != AgentProfileLifecycleState::Active {
                return Err(AgentProfileError::ProfileNotActive {
                    profile_id: current.id(),
                    lifecycle_state: current.lifecycle_state(),
                });
            }
            ensure_profile_observation(current, command.observed_at())?;
            Ok(AgentProfile {
                display_name: configuration.display_name().to_owned(),
                preferred_model: configuration.preferred_model().map(str::to_owned),
                preferred_reasoning: configuration.preferred_reasoning().map(str::to_owned),
                base_capabilities: configuration.capabilities().to_vec(),
                updated_at: command.observed_at(),
                revision: next_profile_revision(current.revision())?,
                ..current.clone()
            })
        }
        ProfileMutation::Disable => {
            if current.lifecycle_state() == AgentProfileLifecycleState::Retired {
                return Err(AgentProfileError::ProfileNotActive {
                    profile_id: current.id(),
                    lifecycle_state: current.lifecycle_state(),
                });
            }
            if current.lifecycle_state() == AgentProfileLifecycleState::Disabled {
                return Ok(current.clone());
            }
            ensure_profile_observation(current, command.observed_at())?;
            Ok(AgentProfile {
                lifecycle_state: AgentProfileLifecycleState::Disabled,
                updated_at: command.observed_at(),
                revision: next_profile_revision(current.revision())?,
                ..current.clone()
            })
        }
        ProfileMutation::Retire => prepare_profile_retirement(
            transaction,
            current,
            command.expected_revision(),
            command.observed_at(),
        ),
    }
}

fn ensure_profile_observation(
    current: &AgentProfile,
    observed_at: TimestampMillis,
) -> Result<(), AgentProfileError> {
    if observed_at.get() < current.updated_at().get() {
        return Err(AgentProfileError::ProfileClockRegression {
            profile_id: current.id(),
            expected_at_least: current.updated_at().get(),
            observed: observed_at.get(),
        });
    }
    Ok(())
}

fn next_profile_revision(
    current: AgentProfileRevision,
) -> Result<AgentProfileRevision, AgentProfileError> {
    current
        .get()
        .checked_add(1)
        .ok_or(AgentProfileError::InvalidRevision(i64::MAX))
        .and_then(AgentProfileRevision::new)
}

fn prepare_profile_retirement(
    transaction: &rusqlite::Transaction<'_>,
    profile: &AgentProfile,
    expected_revision: AgentProfileRevision,
    observed_at: TimestampMillis,
) -> Result<AgentProfile, AgentProfileError> {
    if profile.lifecycle_state() == AgentProfileLifecycleState::Retired {
        return Ok(profile.clone());
    }
    if profile.revision() != expected_revision {
        return Err(AgentProfileError::ProfileRevisionConflict {
            profile_id: profile.id(),
            expected: expected_revision.get(),
            actual: profile.revision().get(),
        });
    }
    ensure_profile_observation(profile, observed_at)?;
    let live_runtimes: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM agent_profile_runtime_sessions
         WHERE profile_id=?1 AND state IN (1,2,3,4,6)",
        params![profile.id().into_uuid().as_bytes()],
        |row| row.get(0),
    )?;
    if live_runtimes > 0 {
        return Err(AgentProfileError::LiveRuntimeSessions(profile.id()));
    }
    let retired = AgentProfile {
        lifecycle_state: AgentProfileLifecycleState::Retired,
        updated_at: observed_at,
        revision: next_profile_revision(profile.revision())?,
        ..profile.clone()
    };
    Ok(retired)
}

fn update_profile(
    transaction: &rusqlite::Transaction<'_>,
    profile: &AgentProfile,
    expected_revision: AgentProfileRevision,
) -> Result<(), AgentProfileError> {
    let changed = transaction.execute(
        "UPDATE agent_profiles
         SET display_name=?1,preferred_model=?2,preferred_reasoning=?3,
             lifecycle_state=?4,updated_at_ms=?5,revision=?6
         WHERE profile_id=?7 AND revision=?8",
        params![
            profile.display_name(),
            profile.preferred_model(),
            profile.preferred_reasoning(),
            encode_lifecycle(profile.lifecycle_state()),
            profile.updated_at().get(),
            profile.revision().get(),
            profile.id().into_uuid().as_bytes(),
            expected_revision.get(),
        ],
    )?;
    if changed != 1 {
        return Err(AgentProfileError::Corrupt(
            "Profile mutation update readback",
        ));
    }
    transaction.execute(
        "DELETE FROM agent_profile_capabilities WHERE profile_id=?1",
        params![profile.id().into_uuid().as_bytes()],
    )?;
    for capability in profile.base_capabilities() {
        transaction.execute(
            "INSERT INTO agent_profile_capabilities(profile_id,capability) VALUES(?1,?2)",
            params![profile.id().into_uuid().as_bytes(), capability],
        )?;
    }
    let readback = load_profile(transaction, profile.id())?
        .ok_or(AgentProfileError::ProfileMissing(profile.id()))?;
    if readback != *profile {
        return Err(AgentProfileError::Corrupt(
            "Profile mutation update readback",
        ));
    }
    Ok(())
}

fn insert_profile(
    transaction: &rusqlite::Transaction<'_>,
    profile: &AgentProfile,
) -> Result<(), AgentProfileError> {
    transaction.execute(
        "INSERT INTO agent_profiles(
            profile_id,request_id,stable_name,display_name,provider_family,
            preferred_model,preferred_reasoning,lifecycle_state,creation_origin,
            created_at_ms,updated_at_ms,last_used_at_ms,revision
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
        params![
            profile.id.into_uuid().as_bytes(),
            profile.request_id.as_bytes(),
            profile.stable_name,
            profile.display_name,
            encode_provider(profile.provider_family),
            profile.preferred_model,
            profile.preferred_reasoning,
            encode_lifecycle(profile.lifecycle_state),
            encode_origin(profile.creation_origin),
            profile.created_at.get(),
            profile.updated_at.get(),
            profile.last_used_at.map(TimestampMillis::get),
            profile.revision.get(),
        ],
    )?;
    for capability in &profile.base_capabilities {
        transaction.execute(
            "INSERT INTO agent_profile_capabilities(profile_id,capability) VALUES(?1,?2)",
            params![profile.id.into_uuid().as_bytes(), capability],
        )?;
    }
    Ok(())
}

fn load_profile(
    connection: &Connection,
    id: AgentProfileId,
) -> Result<Option<AgentProfile>, AgentProfileError> {
    #[allow(clippy::type_complexity)]
    let raw: Option<(
        Vec<u8>,
        Vec<u8>,
        String,
        String,
        i64,
        Option<String>,
        Option<String>,
        i64,
        i64,
        i64,
        i64,
        Option<i64>,
        i64,
    )> = connection
        .query_row(
            "SELECT profile_id,request_id,stable_name,display_name,provider_family,
                    preferred_model,preferred_reasoning,lifecycle_state,creation_origin,
                    created_at_ms,updated_at_ms,last_used_at_ms,revision
             FROM agent_profiles WHERE profile_id=?1",
            params![id.into_uuid().as_bytes()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                    row.get(11)?,
                    row.get(12)?,
                ))
            },
        )
        .optional()?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    let mut capabilities = Vec::new();
    let mut statement = connection.prepare(
        "SELECT capability FROM agent_profile_capabilities WHERE profile_id=?1 ORDER BY capability",
    )?;
    let mut rows = statement.query(params![id.into_uuid().as_bytes()])?;
    while let Some(row) = rows.next()? {
        if capabilities.len() >= MAX_CAPABILITIES {
            return Err(AgentProfileError::Corrupt(
                "agent profile capability count exceeded bound",
            ));
        }
        let capability: String = row.get(0)?;
        validate_text("capability", &capability, MAX_CAPABILITY_BYTES)?;
        capabilities.push(capability);
    }
    let profile_id = decode_profile_id(&raw.0, "profile_id")?;
    let request_id = decode_uuid("request_id", &raw.1)?;
    validate_text("stable_name", &raw.2, MAX_PROFILE_NAME_BYTES)?;
    validate_text("display_name", &raw.3, MAX_PROFILE_NAME_BYTES)?;
    validate_optional_text(
        "preferred_model",
        raw.5.as_deref(),
        MAX_PROVIDER_PREFERENCE_BYTES,
    )?;
    validate_optional_text(
        "preferred_reasoning",
        raw.6.as_deref(),
        MAX_PROVIDER_PREFERENCE_BYTES,
    )?;
    Ok(Some(AgentProfile {
        id: profile_id,
        request_id,
        stable_name: raw.2,
        display_name: raw.3,
        provider_family: decode_provider(raw.4)?,
        preferred_model: raw.5,
        preferred_reasoning: raw.6,
        base_capabilities: capabilities,
        lifecycle_state: decode_lifecycle(raw.7)?,
        creation_origin: decode_origin(raw.8)?,
        created_at: TimestampMillis::new(raw.9)?,
        updated_at: TimestampMillis::new(raw.10)?,
        last_used_at: raw.11.map(TimestampMillis::new).transpose()?,
        revision: AgentProfileRevision::new(raw.12)?,
    }))
}

fn decode_profile_id(
    bytes: &[u8],
    field: &'static str,
) -> Result<AgentProfileId, AgentProfileError> {
    AgentProfileId::from_uuid(decode_uuid(field, bytes)?)
}

fn decode_uuid(field: &'static str, bytes: &[u8]) -> Result<Uuid, AgentProfileError> {
    if bytes.len() != UUID_BYTES {
        return Err(AgentProfileError::InvalidUuidLength {
            field,
            actual: bytes.len(),
            expected: UUID_BYTES,
        });
    }
    Uuid::from_slice(bytes).map_err(|source| AgentProfileError::Uuid { field, source })
}

const fn encode_provider(value: AgentProfileProviderFamily) -> i64 {
    match value {
        AgentProfileProviderFamily::ChatGpt => 1,
    }
}

fn decode_provider(value: i64) -> Result<AgentProfileProviderFamily, AgentProfileError> {
    match value {
        1 => Ok(AgentProfileProviderFamily::ChatGpt),
        _ => Err(AgentProfileError::Corrupt("invalid provider_family")),
    }
}

const fn encode_lifecycle(value: AgentProfileLifecycleState) -> i64 {
    match value {
        AgentProfileLifecycleState::Active => 1,
        AgentProfileLifecycleState::Disabled => 2,
        AgentProfileLifecycleState::Retired => 3,
    }
}

fn decode_lifecycle(value: i64) -> Result<AgentProfileLifecycleState, AgentProfileError> {
    match value {
        1 => Ok(AgentProfileLifecycleState::Active),
        2 => Ok(AgentProfileLifecycleState::Disabled),
        3 => Ok(AgentProfileLifecycleState::Retired),
        _ => Err(AgentProfileError::Corrupt("invalid lifecycle_state")),
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

fn decode_runtime_state(value: i64) -> Result<AgentProfileRuntimeSessionState, AgentProfileError> {
    match value {
        1 => Ok(AgentProfileRuntimeSessionState::Provisioning),
        2 => Ok(AgentProfileRuntimeSessionState::Ready),
        3 => Ok(AgentProfileRuntimeSessionState::Active),
        4 => Ok(AgentProfileRuntimeSessionState::Suspending),
        5 => Ok(AgentProfileRuntimeSessionState::Offline),
        6 => Ok(AgentProfileRuntimeSessionState::Waking),
        7 => Ok(AgentProfileRuntimeSessionState::Failed),
        8 => Ok(AgentProfileRuntimeSessionState::Closed),
        _ => Err(AgentProfileError::Corrupt(
            "invalid provider Runtime Session state",
        )),
    }
}

const fn encode_origin(value: AgentProfileCreationOrigin) -> i64 {
    match value {
        AgentProfileCreationOrigin::UserManual => 1,
        AgentProfileCreationOrigin::AgentRequest => 2,
        AgentProfileCreationOrigin::Imported => 3,
        AgentProfileCreationOrigin::SystemBootstrap => 4,
    }
}

fn decode_origin(value: i64) -> Result<AgentProfileCreationOrigin, AgentProfileError> {
    match value {
        1 => Ok(AgentProfileCreationOrigin::UserManual),
        2 => Ok(AgentProfileCreationOrigin::AgentRequest),
        3 => Ok(AgentProfileCreationOrigin::Imported),
        4 => Ok(AgentProfileCreationOrigin::SystemBootstrap),
        _ => Err(AgentProfileError::Corrupt("invalid creation_origin")),
    }
}

/// Fail-closed Agent Profile validation and persistence errors.
#[derive(Debug, Error)]
pub enum AgentProfileError {
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Timestamp(#[from] forge_domain::TimestampMillisError),
    #[error("Agent Profile identity field is invalid: {0}")]
    InvalidIdentity(&'static str),
    #[error("Agent Profile revision must be positive, got {0}")]
    InvalidRevision(i64),
    #[error(
        "Agent Profile revision conflict for {profile_id:?}: expected {expected}, actual {actual}"
    )]
    ProfileRevisionConflict {
        profile_id: AgentProfileId,
        expected: i64,
        actual: i64,
    },
    #[error(
        "Agent Profile deletion timestamp regressed for {profile_id:?}: expected at least {expected_at_least}, observed {observed}"
    )]
    ProfileClockRegression {
        profile_id: AgentProfileId,
        expected_at_least: i64,
        observed: i64,
    },
    #[error(
        "Agent Profile has live provider Runtime Sessions; close them before retirement: {0:?}"
    )]
    LiveRuntimeSessions(AgentProfileId),
    #[error("Agent Profile text field {field} has {actual} bytes; expected 1..={maximum}")]
    InvalidText {
        field: &'static str,
        actual: usize,
        maximum: usize,
    },
    #[error("Agent Profile has {actual} capabilities; maximum is {maximum}")]
    TooManyCapabilities { actual: usize, maximum: usize },
    #[error("Agent Profile capability tags must be unique")]
    DuplicateCapability,
    #[error("Agent Profile Runtime Session identity already exists: {0}")]
    RuntimeSessionIdConflict(AgentProfileRuntimeSessionId),
    #[error("Agent Profile Runtime Session was not found: {0}")]
    RuntimeSessionMissing(AgentProfileRuntimeSessionId),
    #[error("Agent Profile Runtime Session create request {0} conflicts with committed replay")]
    RuntimeSessionReplayConflict(Uuid),
    #[error("retired Agent Profile cannot create a new Runtime Session: {0}")]
    RuntimeSessionProfileRetired(AgentProfileId),
    #[error(
        "Agent Profile {profile_id:?} is not active for fresh admission (state: {lifecycle_state:?})"
    )]
    ProfileNotActive {
        profile_id: AgentProfileId,
        lifecycle_state: AgentProfileLifecycleState,
    },
    #[error("Agent Profile deletion request {0} conflicts with its committed request")]
    ProfileDeletionRequestReplayConflict(Uuid),
    #[error("Agent Profile deletion intent is missing: {0}")]
    ProfileDeletionIntentMissing(Uuid),
    #[error("Agent Profile {profile_id} already has deletion request {request_id}")]
    ProfileDeletionAlreadyRequested {
        profile_id: AgentProfileId,
        request_id: Uuid,
    },
    #[error("Agent Profile deletion manifest is missing: {0}")]
    ProfileDeletionManifestMissing(Uuid),
    #[error("Agent Profile deletion manifest replay conflicts for request {0}")]
    ProfileDeletionManifestReplayConflict(Uuid),
    #[error("Profile deletion targets are frozen for new conversation identities: {0}")]
    ProfileDeletionTargetsFrozen(AgentProfileId),
    #[error(
        "Agent Profile deletion intent page limit must be between 1 and {maximum}, got {actual}"
    )]
    ProfileDeletionIntentPageLimit { actual: u32, maximum: usize },
    #[error(
        "Agent Profile deletion runtime entry page limit must be between 1 and {maximum}, got {actual}"
    )]
    ProfileDeletionEntryPageLimit { actual: u32, maximum: usize },
    #[error(
        "Agent Profile Runtime Session revision conflict: expected {expected:?}, actual {actual:?}"
    )]
    RuntimeSessionRevisionConflict {
        expected: AgentProfileRuntimeSessionRevision,
        actual: AgentProfileRuntimeSessionRevision,
    },
    #[error("Agent Profile Runtime Session transition is invalid: {from:?} -> {to:?}")]
    RuntimeSessionTransition {
        from: AgentProfileRuntimeSessionState,
        to: AgentProfileRuntimeSessionState,
    },
    #[error("Agent Profile Runtime Session provider conversation identity is required")]
    RuntimeSessionIdentityRequired,
    #[error("Agent Profile Runtime Session provider conversation identity is already in use")]
    RuntimeSessionConversationConflict,
    #[error("Agent Profile Runtime Session observation time regressed")]
    RuntimeSessionClockRegression,
    #[error("Agent Profile Runtime Session revision overflow")]
    RuntimeSessionRevisionOverflow,
    #[error(
        "Agent Profile Runtime Session page limit must be between 1 and {maximum}, got {actual}"
    )]
    RuntimeSessionPageLimit { actual: u32, maximum: usize },
    #[error("Agent Profile resolution claim request {0} conflicts with its committed binding")]
    ResolutionClaimReplayConflict(Uuid),
    #[error("Agent Profile resolution claim conflicts with active Profile reservation: {0}")]
    ResolutionClaimConflict(AgentProfileId),
    #[error("Agent Profile resolution claim batch has {0} entries; maximum is 4")]
    ResolutionClaimBatchBound(usize),
    #[error("Agent Profile resolution claim batch contains a duplicate slot or Profile")]
    ResolutionClaimBatchConflict,
    #[error("failed to open Agent Profile catalog at {path}")]
    Open {
        path: PathBuf,
        #[source]
        source: rusqlite::Error,
    },
    #[error("Agent Profile catalog path has no usable parent: {0}")]
    MissingParent(PathBuf),
    #[error("failed to inspect Agent Profile catalog path {path}")]
    PathIo {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("Agent Profile catalog database path is a symbolic link: {0}")]
    DatabasePathSymlink(PathBuf),
    #[error("Agent Profile catalog database path is not a regular file: {0}")]
    DatabasePathNotRegular(PathBuf),
    #[error("Agent Profile catalog database path has {links} hard links: {path}")]
    DatabasePathHardlink { path: PathBuf, links: u64 },
    #[error("Agent Profile catalog database sidecar exists without its main database: {0}")]
    DatabaseSidecarWithoutMain(PathBuf),
    #[error("Agent Profile catalog database path identity changed: {0}")]
    DatabasePathChanged(PathBuf),
    #[error("Agent Profile catalog database invariant failed for {path}: {detail}")]
    DatabasePathInvariant { path: PathBuf, detail: &'static str },
    #[error("Agent Profile catalog parent is a symbolic link: {0}")]
    DatabaseParentSymlink(PathBuf),
    #[error("Agent Profile catalog parent is not a directory: {0}")]
    DatabaseParentNotDirectory(PathBuf),
    #[error("Agent Profile catalog parent identity changed: {0}")]
    DatabaseParentChanged(PathBuf),
    #[error(
        "Agent Profile catalog ownership is invalid: application_id={application_id}, user_version={user_version}, object_count={object_count}"
    )]
    Ownership {
        application_id: i64,
        user_version: i64,
        object_count: i64,
    },
    #[error("Agent Profile catalog is missing or has invalid schema object {0}")]
    Schema(&'static str),
    #[error("Agent Profile catalog integrity check failed")]
    Integrity,
    #[error("Agent Profile identity already exists: {0}")]
    ProfileIdConflict(AgentProfileId),
    #[error("Agent Profile was not found: {0}")]
    ProfileMissing(AgentProfileId),
    #[error("Agent Profile create request {0} conflicts with the committed replay binding")]
    ReplayConflict(Uuid),
    #[error("Agent Profile mutation request {0} conflicts with the committed replay binding")]
    ProfileMutationReplayConflict(Uuid),
    #[error(
        "Agent Profile mutation {request_id} for {profile_id} commit is unconfirmed; read back the same request identity: {source}"
    )]
    ProfileMutationCommitUnconfirmed {
        request_id: Uuid,
        profile_id: AgentProfileId,
        #[source]
        source: rusqlite::Error,
    },
    #[error("Agent Profile logical Session link request {0} conflicts with its committed binding")]
    LogicalSessionReplayConflict(Uuid),
    #[error(
        "logical Session {session_id} is already linked to Profile {existing_profile_id}, not requested Profile {requested_profile_id}"
    )]
    LogicalSessionProfileConflict {
        session_id: forge_domain::AgentSessionId,
        existing_profile_id: AgentProfileId,
        requested_profile_id: AgentProfileId,
    },
    #[error("Agent Profile persisted UUID field {field} has {actual} bytes; expected {expected}")]
    InvalidUuidLength {
        field: &'static str,
        actual: usize,
        expected: usize,
    },
    #[error("Agent Profile persisted UUID field {field} is invalid")]
    Uuid {
        field: &'static str,
        #[source]
        source: uuid::Error,
    },
    #[error("Agent Profile persisted state is corrupt: {0}")]
    Corrupt(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    fn database(name: &str) -> Result<(PathBuf, PathBuf), Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!(
            "orchestration-agent-profile-{name}-{}",
            Uuid::now_v7()
        ));
        fs::create_dir(&root)?;
        Ok((root.join("agent-profiles.sqlite3"), root))
    }

    fn command(
        proposed: u128,
        request: u128,
        stable_name: &str,
    ) -> Result<CreateAgentProfile, Box<dyn Error>> {
        Ok(CreateAgentProfile::new(
            AgentProfileId::from_uuid(Uuid::from_u128(proposed))?,
            Uuid::from_u128(request),
            stable_name.to_owned(),
            format!("{stable_name} display"),
            AgentProfileProviderFamily::ChatGpt,
            Some("gpt-5.6-luna".to_owned()),
            Some("max".to_owned()),
            vec!["backend".to_owned(), "rust".to_owned()],
            AgentProfileCreationOrigin::UserManual,
            TimestampMillis::new(10)?,
        )?)
    }

    fn configuration(display_name: &str) -> Result<ProfileConfiguration, Box<dyn Error>> {
        Ok(ProfileConfiguration::new(
            display_name.to_owned(),
            Some("gpt-5.6-luna".to_owned()),
            Some("max".to_owned()),
            vec!["backend".to_owned(), "rust".to_owned()],
        )?)
    }

    fn mutation_command(
        profile: &AgentProfile,
        request: u128,
        operation: ProfileMutation,
        observed_at: i64,
    ) -> Result<ProfileMutationCommand, Box<dyn Error>> {
        Ok(ProfileMutationCommand::new(
            Uuid::from_u128(request),
            profile.id(),
            profile.revision(),
            operation,
            TimestampMillis::new(observed_at)?,
        )?)
    }

    fn table_rows(
        connection: &Connection,
        table: &str,
    ) -> Result<Vec<Vec<String>>, Box<dyn Error>> {
        let sql = format!("SELECT * FROM {table} ORDER BY rowid");
        let mut statement = connection.prepare(&sql)?;
        let column_count = statement.column_count();
        let rows = statement
            .query_map([], |row| {
                (0..column_count)
                    .map(|index| {
                        let value = row.get_ref(index)?;
                        let encoded = match value {
                            rusqlite::types::ValueRef::Null => "N".to_owned(),
                            rusqlite::types::ValueRef::Integer(value) => format!("I{value}"),
                            rusqlite::types::ValueRef::Real(value) => format!("R{value:?}"),
                            rusqlite::types::ValueRef::Text(value) => {
                                format!("T{}", String::from_utf8_lossy(value))
                            }
                            rusqlite::types::ValueRef::Blob(value) => {
                                let digits = b"0123456789abcdef";
                                let mut hex = String::with_capacity(value.len() * 2);
                                for byte in value {
                                    hex.push(digits[usize::from(byte >> 4)] as char);
                                    hex.push(digits[usize::from(byte & 0x0f)] as char);
                                }
                                format!("B{hex}")
                            }
                        };
                        Ok(encoded)
                    })
                    .collect::<Result<Vec<_>, rusqlite::Error>>()
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    #[test]
    fn v1_catalog_migrates_additively_and_preserves_profile_identity() -> Result<(), Box<dyn Error>>
    {
        let (path, root) = database("v1-migration")?;
        let mut connection = Connection::open(&path)?;
        connection.execute_batch(AGENT_PROFILES_TABLE_SQL)?;
        connection.execute_batch(AGENT_PROFILE_CAPABILITIES_TABLE_SQL)?;
        connection.execute_batch(AGENT_PROFILE_LOGICAL_SESSIONS_TABLE_SQL)?;
        connection.pragma_update(None, "application_id", AGENT_PROFILE_APPLICATION_ID)?;
        connection.pragma_update(None, "user_version", 1)?;
        let profile_id = AgentProfileId::from_uuid(Uuid::from_u128(0x901))?;
        let profile = AgentProfile {
            id: profile_id,
            request_id: Uuid::from_u128(0x902),
            stable_name: "legacy-profile".to_owned(),
            display_name: "Legacy Profile".to_owned(),
            provider_family: AgentProfileProviderFamily::ChatGpt,
            preferred_model: Some("gpt-5.6-luna".to_owned()),
            preferred_reasoning: Some("max".to_owned()),
            base_capabilities: vec!["rust".to_owned()],
            lifecycle_state: AgentProfileLifecycleState::Active,
            creation_origin: AgentProfileCreationOrigin::UserManual,
            created_at: TimestampMillis::new(10)?,
            updated_at: TimestampMillis::new(10)?,
            last_used_at: None,
            revision: AgentProfileRevision::new(1)?,
        };
        let profile_request_id = profile.request_id();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        insert_profile(&transaction, &profile)?;
        transaction.commit()?;
        drop(connection);

        let catalog = AgentProfileCatalog::open(&path)?;
        assert_eq!(catalog.resolve(profile_id)?, Some(profile));
        let history = catalog.runtime_session_history(profile_id)?;
        assert!(history.sessions().is_empty());
        assert!(!history.truncated());
        assert_eq!(
            catalog.profile_deletion_intent_for_profile(profile_id)?,
            None
        );
        let create_receipt = catalog
            .mutation_receipt(profile_request_id)?
            .ok_or("v1 create receipt missing")?;
        assert_eq!(create_receipt.kind(), ProfileMutationKind::OriginalCreate);
        assert_eq!(
            create_receipt.profile().lifecycle_state(),
            AgentProfileLifecycleState::Active
        );
        assert_eq!(create_receipt.profile().revision().get(), 1);
        assert_eq!(create_receipt.profile().base_capabilities(), ["rust"]);
        let reopened = Connection::open(&path)?;
        let version: i64 = reopened.pragma_query_value(None, "user_version", |row| row.get(0))?;
        assert_eq!(version, AGENT_PROFILE_SCHEMA_VERSION);
        let objects: i64 = reopened.query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(objects, 11);
        let receipts: i64 = reopened.query_row(
            "SELECT COUNT(*) FROM agent_profile_mutation_receipts",
            [],
            |row| row.get(0),
        )?;
        let receipt_capabilities: i64 = reopened.query_row(
            "SELECT COUNT(*) FROM agent_profile_mutation_capabilities",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(receipts, 1);
        assert_eq!(receipt_capabilities, 1);
        drop(reopened);
        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    const V5_LEGACY_TABLES: [&str; 8] = [
        "agent_profiles",
        "agent_profile_capabilities",
        "agent_profile_logical_sessions",
        "agent_profile_runtime_sessions",
        "agent_profile_resolution_claims",
        "agent_profile_deletion_requests",
        "agent_profile_deletion_manifests",
        "agent_profile_deletion_runtime_entries",
    ];

    struct V5Fixture {
        profile: AgentProfile,
        deletion_request_id: Uuid,
        before: Vec<Vec<Vec<String>>>,
    }

    fn v5_fixture_profile() -> Result<AgentProfile, Box<dyn Error>> {
        Ok(AgentProfile {
            id: AgentProfileId::from_uuid(Uuid::from_u128(0xC501))?,
            request_id: Uuid::from_u128(0xC502),
            stable_name: "v5-full-profile".to_owned(),
            display_name: "V5 Full Profile".to_owned(),
            provider_family: AgentProfileProviderFamily::ChatGpt,
            preferred_model: Some("gpt-5.6-luna".to_owned()),
            preferred_reasoning: Some("max".to_owned()),
            base_capabilities: vec!["backend".to_owned(), "rust".to_owned()],
            lifecycle_state: AgentProfileLifecycleState::Disabled,
            creation_origin: AgentProfileCreationOrigin::UserManual,
            created_at: TimestampMillis::new(10)?,
            updated_at: TimestampMillis::new(20)?,
            last_used_at: None,
            revision: AgentProfileRevision::new(2)?,
        })
    }

    fn insert_v5_fixture_rows(
        transaction: &rusqlite::Transaction<'_>,
        profile: &AgentProfile,
    ) -> Result<Uuid, Box<dyn Error>> {
        insert_profile(transaction, profile)?;
        let session_id = forge_domain::AgentSessionId::from_uuid(Uuid::from_u128(0xC503));
        transaction.execute(
            "INSERT INTO agent_profile_logical_sessions(
                request_id,profile_id,profile_revision,session_id,session_revision,linked_at_ms
             ) VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                Uuid::from_u128(0xC504).as_bytes(),
                profile.id().into_uuid().as_bytes(),
                profile.revision().get(),
                session_id.into_uuid().as_bytes(),
                1_i64,
                30_i64,
            ],
        )?;
        let runtime = AgentProfileRuntimeSession {
            id: AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xC505))?,
            request_id: Uuid::from_u128(0xC506),
            profile_id: profile.id(),
            provider_family: AgentProfileProviderFamily::ChatGpt,
            provider_conversation_id: Some("v5-conversation".to_owned()),
            state: AgentProfileRuntimeSessionState::Closed,
            opened_at: TimestampMillis::new(12)?,
            last_seen_at: TimestampMillis::new(18)?,
            closed_at: Some(TimestampMillis::new(18)?),
            revision: AgentProfileRuntimeSessionRevision::new(2)?,
        };
        insert_runtime_session(transaction, &runtime)?;
        transaction.execute(
            "INSERT INTO agent_profile_resolution_claims(
                claim_id,agent_request_id,slot_ordinal,claim_kind,profile_id,profile_revision,
                runtime_session_id,runtime_session_revision,claimed_at_ms,released_at_ms,state,revision
             ) VALUES(?1,?2,?3,?4,?5,?6,NULL,NULL,?7,?8,?9,?10)",
            params![
                Uuid::from_u128(0xC507).as_bytes(),
                Uuid::from_u128(0xC508).as_bytes(),
                0_i64,
                1_i64,
                profile.id().into_uuid().as_bytes(),
                profile.revision().get(),
                31_i64,
                32_i64,
                2_i64,
                2_i64,
            ],
        )?;
        let deletion_request_id = Uuid::from_u128(0xC509);
        transaction.execute(
            "INSERT INTO agent_profile_deletion_requests(
                request_id,profile_id,expected_profile_revision,barrier_profile_revision,requested_at_ms
             ) VALUES(?1,?2,?3,?4,?5)",
            params![
                deletion_request_id.as_bytes(),
                profile.id().into_uuid().as_bytes(),
                1_i64,
                profile.revision().get(),
                40_i64,
            ],
        )?;
        transaction.execute(
            "INSERT INTO agent_profile_deletion_manifests(
                request_id,profile_id,profile_revision_at_freeze,frozen_at_ms,completed_at_ms,
                runtime_entry_count,linked_target_count,no_linked_target_count
             ) VALUES(?1,?2,?3,?4,NULL,?5,?6,?7)",
            params![
                deletion_request_id.as_bytes(),
                profile.id().into_uuid().as_bytes(),
                profile.revision().get(),
                50_i64,
                1_i64,
                1_i64,
                0_i64,
            ],
        )?;
        transaction.execute(
            "INSERT INTO agent_profile_deletion_runtime_entries(
                request_id,runtime_session_id,provider_family,provider_conversation_id,
                original_state,original_revision
             ) VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                deletion_request_id.as_bytes(),
                runtime.id().into_uuid().as_bytes(),
                1_i64,
                "v5-conversation",
                8_i64,
                runtime.revision().get(),
            ],
        )?;
        Ok(deletion_request_id)
    }

    fn stage_v5_fixture(path: &Path) -> Result<V5Fixture, Box<dyn Error>> {
        let mut connection = Connection::open(path)?;
        connection.execute_batch(AGENT_PROFILES_TABLE_SQL)?;
        connection.execute_batch(AGENT_PROFILE_CAPABILITIES_TABLE_SQL)?;
        connection.execute_batch(AGENT_PROFILE_LOGICAL_SESSIONS_TABLE_SQL)?;
        connection.execute_batch(AGENT_PROFILE_RUNTIME_SESSIONS_TABLE_SQL)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        resolver::create_schema(&transaction)?;
        deletion::create_schema(&transaction)?;
        deletion::create_manifest_schema(&transaction)?;
        transaction.pragma_update(None, "application_id", AGENT_PROFILE_APPLICATION_ID)?;
        transaction.pragma_update(None, "user_version", AGENT_PROFILE_V5_SCHEMA_VERSION)?;
        let profile = v5_fixture_profile()?;
        let deletion_request_id = insert_v5_fixture_rows(&transaction, &profile)?;
        transaction.commit()?;
        let before = V5_LEGACY_TABLES
            .iter()
            .map(|table| table_rows(&connection, table))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(V5Fixture {
            profile,
            deletion_request_id,
            before,
        })
    }

    #[test]
    fn v5_catalog_migration_preserves_every_legacy_table_and_backfills_create_receipt()
    -> Result<(), Box<dyn Error>> {
        let (path, root) = database("v5-full-migration")?;
        let fixture = stage_v5_fixture(&path)?;
        let catalog = AgentProfileCatalog::open(&path)?;
        let reopened = Connection::open(&path)?;
        let after = V5_LEGACY_TABLES
            .iter()
            .map(|table| table_rows(&reopened, table))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(fixture.before, after);
        let create_receipt = catalog
            .mutation_receipt(fixture.profile.request_id())?
            .ok_or("v5 create receipt missing")?;
        assert_eq!(create_receipt.kind(), ProfileMutationKind::OriginalCreate);
        assert_eq!(
            create_receipt.profile().lifecycle_state(),
            AgentProfileLifecycleState::Active
        );
        assert_eq!(create_receipt.profile().revision().get(), 1);
        assert_eq!(
            create_receipt.profile().base_capabilities(),
            ["backend", "rust"]
        );
        assert!(
            catalog
                .profile_deletion_intent(fixture.deletion_request_id)?
                .is_some()
        );
        assert!(
            catalog
                .profile_deletion_manifest(fixture.deletion_request_id)?
                .is_some()
        );
        drop(reopened);
        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn v3_catalog_migrates_to_deletion_intents_without_rewriting_history()
    -> Result<(), Box<dyn Error>> {
        let (path, root) = database("v3-deletion-migration")?;
        let mut connection = Connection::open(&path)?;
        connection.execute_batch(AGENT_PROFILES_TABLE_SQL)?;
        connection.execute_batch(AGENT_PROFILE_CAPABILITIES_TABLE_SQL)?;
        connection.execute_batch(AGENT_PROFILE_LOGICAL_SESSIONS_TABLE_SQL)?;
        connection.execute_batch(AGENT_PROFILE_RUNTIME_SESSIONS_TABLE_SQL)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        resolver::create_schema(&transaction)?;
        transaction.pragma_update(None, "application_id", AGENT_PROFILE_APPLICATION_ID)?;
        transaction.pragma_update(None, "user_version", AGENT_PROFILE_V3_SCHEMA_VERSION)?;

        let profile_id = AgentProfileId::from_uuid(Uuid::from_u128(0xA901))?;
        let profile = AgentProfile {
            id: profile_id,
            request_id: Uuid::from_u128(0xA902),
            stable_name: "v3-retired-profile".to_owned(),
            display_name: "V3 Retired Profile".to_owned(),
            provider_family: AgentProfileProviderFamily::ChatGpt,
            preferred_model: Some("gpt-5.6-luna".to_owned()),
            preferred_reasoning: Some("max".to_owned()),
            base_capabilities: vec!["rust".to_owned()],
            lifecycle_state: AgentProfileLifecycleState::Retired,
            creation_origin: AgentProfileCreationOrigin::UserManual,
            created_at: TimestampMillis::new(10)?,
            updated_at: TimestampMillis::new(30)?,
            last_used_at: None,
            revision: AgentProfileRevision::new(2)?,
        };
        let profile_request_id = profile.request_id();
        insert_profile(&transaction, &profile)?;
        let runtime = AgentProfileRuntimeSession {
            id: AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xA903))?,
            request_id: Uuid::from_u128(0xA904),
            profile_id,
            provider_family: AgentProfileProviderFamily::ChatGpt,
            provider_conversation_id: Some("v3-retained-conversation".to_owned()),
            state: AgentProfileRuntimeSessionState::Closed,
            opened_at: TimestampMillis::new(20)?,
            last_seen_at: TimestampMillis::new(25)?,
            closed_at: Some(TimestampMillis::new(25)?),
            revision: AgentProfileRuntimeSessionRevision::new(3)?,
        };
        insert_runtime_session(&transaction, &runtime)?;
        transaction.commit()?;
        drop(connection);

        let catalog = AgentProfileCatalog::open(&path)?;
        assert_eq!(catalog.resolve(profile_id)?, Some(profile));
        assert_eq!(
            catalog.runtime_session(runtime.id())?,
            Some(runtime.clone())
        );
        assert_eq!(
            catalog.profile_deletion_intent_for_profile(profile_id)?,
            None
        );
        let create_receipt = catalog
            .mutation_receipt(profile_request_id)?
            .ok_or("v3 create receipt missing")?;
        assert_eq!(
            create_receipt.profile().lifecycle_state(),
            AgentProfileLifecycleState::Active
        );
        assert_eq!(
            create_receipt.profile().updated_at(),
            TimestampMillis::new(10)?
        );
        assert_eq!(create_receipt.profile().revision().get(), 1);
        let page = catalog.runtime_sessions_page(profile_id, None, 32)?;
        assert_eq!(page.sessions(), [runtime]);
        let reopened = Connection::open(&path)?;
        let version: i64 = reopened.pragma_query_value(None, "user_version", |row| row.get(0))?;
        assert_eq!(version, AGENT_PROFILE_SCHEMA_VERSION);
        let objects: i64 = reopened.query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(objects, 11);
        drop(reopened);
        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn v4_catalog_adds_manifest_schema_without_rewriting_intent_or_runtime_rows()
    -> Result<(), Box<dyn Error>> {
        let (path, root) = database("v4-deletion-manifest-migration")?;
        let mut connection = Connection::open(&path)?;
        connection.execute_batch(AGENT_PROFILES_TABLE_SQL)?;
        connection.execute_batch(AGENT_PROFILE_CAPABILITIES_TABLE_SQL)?;
        connection.execute_batch(AGENT_PROFILE_LOGICAL_SESSIONS_TABLE_SQL)?;
        connection.execute_batch(AGENT_PROFILE_RUNTIME_SESSIONS_TABLE_SQL)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        resolver::create_schema(&transaction)?;
        deletion::create_schema(&transaction)?;
        transaction.pragma_update(None, "application_id", AGENT_PROFILE_APPLICATION_ID)?;
        transaction.pragma_update(None, "user_version", AGENT_PROFILE_V4_SCHEMA_VERSION)?;

        let profile_id = AgentProfileId::from_uuid(Uuid::from_u128(0xB901))?;
        let profile = AgentProfile {
            id: profile_id,
            request_id: Uuid::from_u128(0xB902),
            stable_name: "v4-disabled-profile".to_owned(),
            display_name: "V4 Disabled Profile".to_owned(),
            provider_family: AgentProfileProviderFamily::ChatGpt,
            preferred_model: Some("gpt-5.6-luna".to_owned()),
            preferred_reasoning: Some("max".to_owned()),
            base_capabilities: vec!["rust".to_owned()],
            lifecycle_state: AgentProfileLifecycleState::Disabled,
            creation_origin: AgentProfileCreationOrigin::UserManual,
            created_at: TimestampMillis::new(10)?,
            updated_at: TimestampMillis::new(20)?,
            last_used_at: None,
            revision: AgentProfileRevision::new(2)?,
        };
        insert_profile(&transaction, &profile)?;
        let runtime = AgentProfileRuntimeSession {
            id: AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xB903))?,
            request_id: Uuid::from_u128(0xB904),
            profile_id,
            provider_family: AgentProfileProviderFamily::ChatGpt,
            provider_conversation_id: Some("v4-retained-conversation".to_owned()),
            state: AgentProfileRuntimeSessionState::Closed,
            opened_at: TimestampMillis::new(15)?,
            last_seen_at: TimestampMillis::new(19)?,
            closed_at: Some(TimestampMillis::new(19)?),
            revision: AgentProfileRuntimeSessionRevision::new(3)?,
        };
        insert_runtime_session(&transaction, &runtime)?;
        let request_id = Uuid::from_u128(0xB905);
        transaction.execute(
            "INSERT INTO agent_profile_deletion_requests(
                request_id,profile_id,expected_profile_revision,
                barrier_profile_revision,requested_at_ms
             ) VALUES(?1,?2,?3,?4,?5)",
            params![
                request_id.as_bytes(),
                profile_id.into_uuid().as_bytes(),
                1_i64,
                2_i64,
                20_i64,
            ],
        )?;
        transaction.commit()?;
        drop(connection);

        let catalog = AgentProfileCatalog::open(&path)?;
        assert_eq!(catalog.resolve(profile_id)?, Some(profile));
        assert_eq!(
            catalog.runtime_session(runtime.id())?,
            Some(runtime.clone())
        );
        let intent = catalog
            .profile_deletion_intent(request_id)?
            .ok_or("v4 intent missing")?;
        assert_eq!(intent.profile_id(), profile_id);
        assert_eq!(intent.barrier_profile_revision().get(), 2);
        assert_eq!(catalog.profile_deletion_manifest(request_id)?, None);
        assert_eq!(
            catalog
                .runtime_sessions_page(profile_id, None, 32)?
                .sessions(),
            [runtime]
        );
        let reopened = Connection::open(&path)?;
        let version: i64 = reopened.pragma_query_value(None, "user_version", |row| row.get(0))?;
        assert_eq!(version, AGENT_PROFILE_SCHEMA_VERSION);
        let objects: i64 = reopened.query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(objects, 11);
        drop(reopened);
        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    fn prepare_ready_runtime(
        catalog: &mut AgentProfileCatalog,
        profile_id: AgentProfileId,
    ) -> Result<(AgentProfileRuntimeSessionId, AgentProfileRuntimeSession), Box<dyn Error>> {
        let runtime_id = AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xA010))?;
        let first = catalog.create_runtime_session(&CreateAgentProfileRuntimeSession::new(
            runtime_id,
            Uuid::from_u128(0xA011),
            profile_id,
            AgentProfileProviderFamily::ChatGpt,
            TimestampMillis::new(20)?,
        )?)?;
        assert!(first.created());
        assert_eq!(
            first.session().state(),
            AgentProfileRuntimeSessionState::Provisioning
        );
        let replay = catalog.create_runtime_session(&CreateAgentProfileRuntimeSession::new(
            AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xA012))?,
            Uuid::from_u128(0xA011),
            profile_id,
            AgentProfileProviderFamily::ChatGpt,
            TimestampMillis::new(20)?,
        )?)?;
        assert!(!replay.created());
        assert_eq!(replay.session().id(), runtime_id);
        assert!(matches!(
            catalog.transition_runtime_session(
                runtime_id,
                AgentProfileRuntimeSessionRevision::new(1)?,
                AgentProfileRuntimeSessionState::Ready,
                TimestampMillis::new(21)?,
            ),
            Err(AgentProfileError::RuntimeSessionIdentityRequired)
        ));
        let ready = catalog.confirm_runtime_session_ready(
            runtime_id,
            AgentProfileRuntimeSessionRevision::new(1)?,
            "conversation-a",
            TimestampMillis::new(21)?,
        )?;
        let replay_ready = catalog.confirm_runtime_session_ready(
            runtime_id,
            AgentProfileRuntimeSessionRevision::new(1)?,
            "conversation-a",
            TimestampMillis::new(21)?,
        )?;
        assert_eq!(ready.revision().get(), 2);
        assert_eq!(replay_ready.revision(), ready.revision());
        assert_eq!(
            catalog.operational_state(profile_id)?,
            Some(AgentProfileOperationalState::Online)
        );
        Ok((runtime_id, ready))
    }

    fn drive_runtime_to_closed(
        catalog: &mut AgentProfileCatalog,
        profile_id: AgentProfileId,
        runtime_id: AgentProfileRuntimeSessionId,
        ready: &AgentProfileRuntimeSession,
    ) -> Result<AgentProfileRuntimeSession, Box<dyn Error>> {
        let active = catalog.transition_runtime_session(
            runtime_id,
            ready.revision(),
            AgentProfileRuntimeSessionState::Active,
            TimestampMillis::new(22)?,
        )?;
        assert_eq!(
            catalog.operational_state(profile_id)?,
            Some(AgentProfileOperationalState::Busy)
        );
        assert!(matches!(
            catalog.transition_runtime_session(
                runtime_id,
                ready.revision(),
                AgentProfileRuntimeSessionState::Suspending,
                TimestampMillis::new(23)?,
            ),
            Err(AgentProfileError::RuntimeSessionRevisionConflict { .. })
        ));
        let suspending = catalog.transition_runtime_session(
            runtime_id,
            active.revision(),
            AgentProfileRuntimeSessionState::Suspending,
            TimestampMillis::new(23)?,
        )?;
        assert_eq!(
            catalog.operational_state(profile_id)?,
            Some(AgentProfileOperationalState::Online)
        );
        let offline = catalog.transition_runtime_session(
            runtime_id,
            suspending.revision(),
            AgentProfileRuntimeSessionState::Offline,
            TimestampMillis::new(24)?,
        )?;
        assert_eq!(
            catalog.operational_state(profile_id)?,
            Some(AgentProfileOperationalState::Offline)
        );
        let waking = catalog.transition_runtime_session(
            runtime_id,
            offline.revision(),
            AgentProfileRuntimeSessionState::Waking,
            TimestampMillis::new(25)?,
        )?;
        let ready_again = catalog.transition_runtime_session(
            runtime_id,
            waking.revision(),
            AgentProfileRuntimeSessionState::Ready,
            TimestampMillis::new(26)?,
        )?;
        let closed = catalog.transition_runtime_session(
            runtime_id,
            ready_again.revision(),
            AgentProfileRuntimeSessionState::Closed,
            TimestampMillis::new(27)?,
        )?;
        assert_eq!(closed.state(), AgentProfileRuntimeSessionState::Closed);
        assert_eq!(closed.closed_at(), Some(TimestampMillis::new(27)?));
        assert_eq!(
            catalog.operational_state(profile_id)?,
            Some(AgentProfileOperationalState::Offline)
        );
        Ok(closed)
    }

    #[test]
    fn runtime_session_cas_drives_derived_operational_state() -> Result<(), Box<dyn Error>> {
        let (path, root) = database("runtime-state")?;
        let mut catalog = AgentProfileCatalog::open(&path)?;
        let profile = catalog.create_profile(&command(0xA001, 0xA002, "reusable-agent")?)?;
        let profile_id = profile.profile().id();
        assert_eq!(
            catalog.operational_state(profile_id)?,
            Some(AgentProfileOperationalState::Offline)
        );
        let (runtime_id, ready) = prepare_ready_runtime(&mut catalog, profile_id)?;
        let closed = drive_runtime_to_closed(&mut catalog, profile_id, runtime_id, &ready)?;
        let offline_runtime_id = AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xA020))?;
        let offline_created =
            catalog.create_runtime_session(&CreateAgentProfileRuntimeSession::new(
                offline_runtime_id,
                Uuid::from_u128(0xA021),
                profile_id,
                AgentProfileProviderFamily::ChatGpt,
                TimestampMillis::new(30)?,
            )?)?;
        let offline_ready = catalog.confirm_runtime_session_ready(
            offline_runtime_id,
            offline_created.session().revision(),
            "conversation-offline",
            TimestampMillis::new(31)?,
        )?;
        let suspending = catalog.transition_runtime_session(
            offline_runtime_id,
            offline_ready.revision(),
            AgentProfileRuntimeSessionState::Suspending,
            TimestampMillis::new(32)?,
        )?;
        let offline = catalog.transition_runtime_session(
            offline_runtime_id,
            suspending.revision(),
            AgentProfileRuntimeSessionState::Offline,
            TimestampMillis::new(33)?,
        )?;
        let _retired = catalog.retire_profile(
            profile_id,
            profile.profile().revision(),
            TimestampMillis::new(34)?,
        )?;
        assert_eq!(
            catalog.operational_state(profile_id)?,
            Some(AgentProfileOperationalState::Retired)
        );

        let replay = catalog.create_runtime_session(&CreateAgentProfileRuntimeSession::new(
            AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xA022))?,
            Uuid::from_u128(0xA021),
            profile_id,
            AgentProfileProviderFamily::ChatGpt,
            TimestampMillis::new(30)?,
        )?)?;
        assert!(!replay.created());
        assert_eq!(replay.session(), &offline);
        assert!(matches!(
            catalog.create_runtime_session(&CreateAgentProfileRuntimeSession::new(
                AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xA023))?,
                Uuid::from_u128(0xA024),
                profile_id,
                AgentProfileProviderFamily::ChatGpt,
                TimestampMillis::new(35)?,
            )?),
            Err(AgentProfileError::RuntimeSessionProfileRetired(id)) if id == profile_id
        ));
        assert!(matches!(
            catalog.transition_runtime_session(
                offline_runtime_id,
                offline.revision(),
                AgentProfileRuntimeSessionState::Waking,
                TimestampMillis::new(34)?,
            ),
            Err(AgentProfileError::ProfileNotActive {
                profile_id: id,
                lifecycle_state: AgentProfileLifecycleState::Retired,
            }) if id == profile_id
        ));
        assert_eq!(
            catalog.runtime_session(offline_runtime_id)?,
            Some(offline.clone())
        );
        let cleanup = catalog.transition_runtime_session(
            offline_runtime_id,
            offline.revision(),
            AgentProfileRuntimeSessionState::Closed,
            TimestampMillis::new(35)?,
        )?;
        assert_eq!(cleanup.state(), AgentProfileRuntimeSessionState::Closed);
        assert_eq!(
            cleanup.provider_conversation_id(),
            Some("conversation-offline")
        );

        let history = catalog.runtime_session_history(profile_id)?;
        assert_eq!(history.sessions(), [cleanup, closed]);
        assert!(!history.truncated());
        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    fn inventory_ready_runtime(
        catalog: &mut AgentProfileCatalog,
        profile: AgentProfileId,
        runtime: u128,
        request: u128,
        conversation: &str,
        opened_at: i64,
        confirmed_at: i64,
    ) -> Result<AgentProfileRuntimeSession, Box<dyn Error>> {
        let id = AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(runtime))?;
        let created = catalog.create_runtime_session(&CreateAgentProfileRuntimeSession::new(
            id,
            Uuid::from_u128(request),
            profile,
            AgentProfileProviderFamily::ChatGpt,
            TimestampMillis::new(opened_at)?,
        )?)?;
        Ok(catalog.confirm_runtime_session_ready(
            id,
            created.session().revision(),
            conversation,
            TimestampMillis::new(confirmed_at)?,
        )?)
    }

    #[test]
    fn runtime_session_inventory_pages_all_states_by_profile_and_uuid() -> Result<(), Box<dyn Error>>
    {
        let (path, root) = database("runtime-inventory-page")?;
        let mut catalog = AgentProfileCatalog::open(&path)?;
        let profile_id = catalog
            .create_profile(&command(0xE101, 0xE201, "runtime-inventory-primary")?)?
            .profile()
            .id();
        let other_profile_id = catalog
            .create_profile(&command(0xE102, 0xE202, "runtime-inventory-other")?)?
            .profile()
            .id();

        for index in 0..2_u128 {
            let _ = inventory_ready_runtime(
                &mut catalog,
                other_profile_id,
                0xE080 + index,
                0xE280 + index,
                &format!("other-profile-conversation-{index}"),
                10 + i64::try_from(index)?,
                20 + i64::try_from(index)?,
            )?;
        }
        for index in 0..35_u128 {
            let ready = inventory_ready_runtime(
                &mut catalog,
                profile_id,
                0xE100 + index,
                0xE300 + index,
                &format!("primary-profile-conversation-{index}"),
                100 + i64::try_from(index)?,
                200 + i64::try_from(index)?,
            )?;
            let terminal = match index {
                0 => Some(AgentProfileRuntimeSessionState::Closed),
                1 => Some(AgentProfileRuntimeSessionState::Failed),
                _ => None,
            };
            if let Some(state) = terminal {
                let _ = catalog.transition_runtime_session(
                    ready.id(),
                    ready.revision(),
                    state,
                    TimestampMillis::new(300 + i64::try_from(index)?)?,
                )?;
            }
        }

        let first = catalog.runtime_sessions_page(profile_id, None, 32)?;
        assert_eq!(first.sessions().len(), 32);
        assert_eq!(
            first.sessions()[0].id().into_uuid(),
            Uuid::from_u128(0xE100)
        );
        assert!(
            first
                .sessions()
                .windows(2)
                .all(|pair| pair[0].id() < pair[1].id())
        );
        let cursor = first.next_runtime_id().ok_or("missing next-page cursor")?;
        assert_eq!(
            Some(cursor),
            first.sessions().last().map(AgentProfileRuntimeSession::id)
        );

        let second = catalog.runtime_sessions_page(profile_id, Some(cursor), 32)?;
        assert_eq!(second.sessions().len(), 3);
        assert_eq!(second.next_runtime_id(), None);
        assert!(
            second
                .sessions()
                .windows(2)
                .all(|pair| pair[0].id() < pair[1].id())
        );

        let sessions = first
            .sessions()
            .iter()
            .chain(second.sessions())
            .collect::<Vec<_>>();
        assert_eq!(sessions.len(), 35);
        assert!(
            sessions
                .iter()
                .all(|session| session.profile_id() == profile_id)
        );
        assert_eq!(sessions[0].state(), AgentProfileRuntimeSessionState::Closed);
        assert_eq!(
            sessions[0].provider_conversation_id(),
            Some("primary-profile-conversation-0")
        );
        assert_eq!(sessions[1].state(), AgentProfileRuntimeSessionState::Failed);
        assert_eq!(
            sessions[1].provider_conversation_id(),
            Some("primary-profile-conversation-1")
        );
        assert!(matches!(
            catalog.runtime_sessions_page(profile_id, None, 0),
            Err(AgentProfileError::RuntimeSessionPageLimit { .. })
        ));
        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    fn assert_retired_deletion_clock(
        catalog: &mut AgentProfileCatalog,
    ) -> Result<(), Box<dyn Error>> {
        let other = catalog
            .create_profile(&command(0xD560, 0xD561, "retired-deletion")?)?
            .profile()
            .clone();
        let retired =
            catalog.retire_profile(other.id(), other.revision(), TimestampMillis::new(23)?)?;
        let retired_intent = catalog.begin_profile_deletion(&AgentProfileDeletionCommand::new(
            Uuid::from_u128(0xD564),
            retired.id(),
            retired.revision(),
            TimestampMillis::new(100)?,
        )?)?;
        assert_eq!(
            retired_intent.barrier_profile_revision(),
            retired.revision()
        );
        assert!(matches!(
            catalog.freeze_profile_deletion_targets(
                retired_intent.request_id(),
                retired.revision(),
                TimestampMillis::new(50)?,
            ),
            Err(AgentProfileError::ProfileClockRegression {
                profile_id: id,
                expected_at_least: 100,
                observed: 50,
            }) if id == retired.id()
        ));
        let retired_manifest = catalog.freeze_profile_deletion_targets(
            retired_intent.request_id(),
            retired.revision(),
            TimestampMillis::new(120)?,
        )?;
        assert_eq!(retired_manifest.frozen_at(), TimestampMillis::new(120)?);
        assert!(matches!(
            catalog.finish_profile_deletion(
                retired_intent.request_id(),
                TimestampMillis::new(50)?,
            ),
            Err(AgentProfileError::ProfileClockRegression {
                profile_id: id,
                expected_at_least: 120,
                observed: 50,
            }) if id == retired.id()
        ));
        let retired_completed = catalog
            .finish_profile_deletion(retired_intent.request_id(), TimestampMillis::new(121)?)?;
        assert_eq!(
            retired_completed.completed_at(),
            Some(TimestampMillis::new(121)?)
        );
        assert_eq!(
            catalog.profile_deletion_manifest(retired_intent.request_id())?,
            Some(retired_completed)
        );
        assert_eq!(catalog.resolve(retired.id())?, Some(retired));
        Ok(())
    }

    #[test]
    fn deletion_intent_disables_admission_and_replays_after_reopen() -> Result<(), Box<dyn Error>> {
        let (path, root) = database("deletion-intent-replay")?;
        let mut catalog = AgentProfileCatalog::open(&path)?;
        let profile = catalog
            .create_profile(&command(0xD501, 0xD502, "deletion")?)?
            .profile()
            .clone();
        let request_id = Uuid::from_u128(0xD530);
        let deletion = AgentProfileDeletionCommand::new(
            request_id,
            profile.id(),
            profile.revision(),
            TimestampMillis::new(40)?,
        )?;
        assert!(matches!(
            catalog.begin_profile_deletion(&AgentProfileDeletionCommand::new(
                request_id,
                profile.id(),
                profile.revision(),
                TimestampMillis::new(9)?,
            )?),
            Err(AgentProfileError::ProfileClockRegression { .. })
        ));
        assert!(catalog.profile_deletion_intent(request_id)?.is_none());
        let intent = catalog.begin_profile_deletion(&deletion)?;
        let disabled = catalog.resolve(profile.id())?.ok_or("missing Profile")?;
        assert_eq!(
            disabled.lifecycle_state(),
            AgentProfileLifecycleState::Disabled
        );
        assert_eq!(disabled.revision(), intent.barrier_profile_revision());
        assert!(catalog.resolver_candidates("rust")?.candidates().is_empty());
        let target = AgentProfileResolutionClaimTarget::new(
            AgentProfileResolutionClaimKind::WakeNewRuntime,
            profile.id(),
            disabled.revision(),
            None,
        )?;
        let claim = AgentProfileResolutionClaimCommand::new(
            Uuid::from_u128(0xD550),
            Uuid::from_u128(0xD551),
            0,
            target,
            TimestampMillis::new(41)?,
        )?;
        assert!(matches!(catalog.reserve_resolution_claims(&[claim]),
            Err(AgentProfileError::ResolutionClaimConflict(id)) if id == profile.id()));
        assert!(matches!(
            catalog.begin_profile_deletion(&AgentProfileDeletionCommand::new(
                Uuid::from_u128(0xD570),
                profile.id(),
                disabled.revision(),
                TimestampMillis::new(45)?,
            )?),
            Err(AgentProfileError::ProfileDeletionAlreadyRequested { .. })
        ));
        assert!(
            matches!(catalog.begin_profile_deletion(&AgentProfileDeletionCommand::new(
            request_id, profile.id(), disabled.revision(), TimestampMillis::new(45)?,
        )?), Err(AgentProfileError::ProfileDeletionRequestReplayConflict(id)) if id == request_id)
        );
        assert_retired_deletion_clock(&mut catalog)?;
        drop(catalog);
        let mut reopened = AgentProfileCatalog::open(&path)?;
        let replay = reopened.begin_profile_deletion(&AgentProfileDeletionCommand::new(
            request_id,
            profile.id(),
            profile.revision(),
            TimestampMillis::new(99)?,
        )?)?;
        assert_eq!(replay, intent);
        assert_eq!(replay.requested_at(), TimestampMillis::new(40)?);
        assert_eq!(reopened.profile_deletion_intent(request_id)?, Some(intent));
        assert_eq!(
            reopened.profile_deletion_intent_for_profile(profile.id())?,
            Some(intent)
        );
        drop(reopened);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn observed_wake_during_deletion_retains_facts_without_fresh_activation()
    -> Result<(), Box<dyn Error>> {
        let (path, root) = database("deletion-wake-observation")?;
        let mut catalog = AgentProfileCatalog::open(&path)?;
        let profile = catalog
            .create_profile(&command(0xD601, 0xD602, "deletion-wake")?)?
            .profile()
            .clone();
        let ready = inventory_ready_runtime(
            &mut catalog,
            profile.id(),
            0xD610,
            0xD611,
            "deletion-wake-conversation",
            20,
            21,
        )?;
        let suspending = catalog.transition_runtime_session(
            ready.id(),
            ready.revision(),
            AgentProfileRuntimeSessionState::Suspending,
            TimestampMillis::new(22)?,
        )?;
        let offline = catalog.transition_runtime_session(
            ready.id(),
            suspending.revision(),
            AgentProfileRuntimeSessionState::Offline,
            TimestampMillis::new(23)?,
        )?;
        let _ = catalog.begin_profile_deletion(&AgentProfileDeletionCommand::new(
            Uuid::from_u128(0xD630),
            profile.id(),
            profile.revision(),
            TimestampMillis::new(40)?,
        )?)?;
        assert!(matches!(
            catalog.transition_runtime_session(
                offline.id(),
                offline.revision(),
                AgentProfileRuntimeSessionState::Waking,
                TimestampMillis::new(41)?
            ),
            Err(AgentProfileError::ProfileNotActive { .. })
        ));
        assert!(matches!(
            catalog.confirm_existing_runtime_wake_observed(
                offline.id(),
                offline.revision(),
                "different-conversation",
                TimestampMillis::new(42)?
            ),
            Err(AgentProfileError::RuntimeSessionConversationConflict)
        ));
        let observed = catalog.confirm_existing_runtime_wake_observed(
            offline.id(),
            offline.revision(),
            "deletion-wake-conversation",
            TimestampMillis::new(42)?,
        )?;
        assert_eq!(observed.state(), AgentProfileRuntimeSessionState::Ready);
        assert_eq!(
            catalog.confirm_existing_runtime_wake_observed(
                offline.id(),
                offline.revision(),
                "deletion-wake-conversation",
                TimestampMillis::new(43)?
            )?,
            observed
        );
        assert!(matches!(
            catalog.transition_runtime_session(
                observed.id(),
                observed.revision(),
                AgentProfileRuntimeSessionState::Active,
                TimestampMillis::new(43)?
            ),
            Err(AgentProfileError::ProfileNotActive { .. })
        ));
        let closed = catalog.transition_runtime_session(
            observed.id(),
            observed.revision(),
            AgentProfileRuntimeSessionState::Closed,
            TimestampMillis::new(44)?,
        )?;
        assert_eq!(
            closed.provider_conversation_id(),
            Some("deletion-wake-conversation")
        );
        assert_eq!(
            catalog
                .runtime_sessions_page(profile.id(), None, 32)?
                .sessions(),
            [closed]
        );
        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    struct DeletionManifestFixture {
        profile_id: AgentProfileId,
        intent: AgentProfileDeletionIntent,
        manifest: AgentProfileDeletionManifest,
        already_closed: AgentProfileRuntimeSession,
        exact_ready: AgentProfileRuntimeSession,
        offline_before_freeze: AgentProfileRuntimeSession,
        pending_session: AgentProfileRuntimeSession,
        logical_session_id: forge_domain::AgentSessionId,
        original_link: AgentProfileLogicalSessionLink,
    }

    fn stage_deletion_manifest_fixture(
        catalog: &mut AgentProfileCatalog,
    ) -> Result<DeletionManifestFixture, Box<dyn Error>> {
        let profile = catalog
            .create_profile(&command(0xD901, 0xD902, "manifest-finalization")?)?
            .profile()
            .clone();
        let mut already_closed = None;
        let mut exact_ready = None;
        let mut offline_before_freeze = None;
        for index in 0..32_u128 {
            let ready = inventory_ready_runtime(
                catalog,
                profile.id(),
                0xD700 + index,
                0xD800 + index,
                &format!("manifest-conversation-{index}"),
                10 + i64::try_from(index)?,
                20 + i64::try_from(index)?,
            )?;
            if index == 0 {
                already_closed = Some(catalog.transition_runtime_session(
                    ready.id(),
                    ready.revision(),
                    AgentProfileRuntimeSessionState::Closed,
                    TimestampMillis::new(60)?,
                )?);
            } else if index == 1 {
                exact_ready = Some(ready);
            } else if index == 2 {
                let suspending = catalog.transition_runtime_session(
                    ready.id(),
                    ready.revision(),
                    AgentProfileRuntimeSessionState::Suspending,
                    TimestampMillis::new(59)?,
                )?;
                offline_before_freeze = Some(catalog.transition_runtime_session(
                    suspending.id(),
                    suspending.revision(),
                    AgentProfileRuntimeSessionState::Offline,
                    TimestampMillis::new(60)?,
                )?);
            }
        }
        let pending_runtime_id = AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xD720))?;
        let pending_session = catalog
            .create_runtime_session(&CreateAgentProfileRuntimeSession::new(
                pending_runtime_id,
                Uuid::from_u128(0xD820),
                profile.id(),
                AgentProfileProviderFamily::ChatGpt,
                TimestampMillis::new(70)?,
            )?)?
            .session()
            .clone();
        let logical_session_id = forge_domain::AgentSessionId::from_uuid(Uuid::from_u128(0xD840));
        let original_link = catalog
            .link_verified_logical_session(
                Uuid::from_u128(0xD841),
                profile.id(),
                logical_session_id,
                forge_domain::AgentSessionRevision::new(1)?,
                TimestampMillis::new(80)?,
            )?
            .link();
        let intent = catalog.begin_profile_deletion(&AgentProfileDeletionCommand::new(
            Uuid::from_u128(0xD830),
            profile.id(),
            profile.revision(),
            TimestampMillis::new(100)?,
        )?)?;
        let disabled = catalog.resolve(profile.id())?.ok_or("Profile missing")?;
        let manifest = catalog.freeze_profile_deletion_targets(
            intent.request_id(),
            disabled.revision(),
            TimestampMillis::new(120)?,
        )?;
        Ok(DeletionManifestFixture {
            profile_id: profile.id(),
            intent,
            manifest,
            already_closed: already_closed.ok_or("missing Closed Runtime")?,
            exact_ready: exact_ready.ok_or("missing Ready Runtime")?,
            offline_before_freeze: offline_before_freeze.ok_or("missing Offline Runtime")?,
            pending_session,
            logical_session_id,
            original_link,
        })
    }

    fn assert_frozen_manifest_snapshot(
        catalog: &mut AgentProfileCatalog,
        fixture: &DeletionManifestFixture,
    ) -> Result<(), Box<dyn Error>> {
        assert_eq!(fixture.manifest.runtime_entry_count(), 33);
        assert_eq!(fixture.manifest.linked_target_count(), 32);
        assert_eq!(fixture.manifest.no_linked_target_count(), 1);
        assert_eq!(fixture.manifest.completed_at(), None);
        assert_eq!(
            catalog.freeze_profile_deletion_targets(
                fixture.intent.request_id(),
                fixture.manifest.profile_revision_at_freeze(),
                TimestampMillis::new(121)?,
            )?,
            fixture.manifest
        );

        let first =
            catalog.profile_deletion_runtime_entries_page(fixture.intent.request_id(), None, 32)?;
        assert_eq!(first.entries().len(), 32);
        assert_eq!(
            catalog.profile_deletion_runtime_entry(
                fixture.intent.request_id(),
                first.entries()[0].runtime_session_id(),
            )?,
            Some(first.entries()[0].clone())
        );
        assert_eq!(
            catalog.profile_deletion_runtime_entry(
                fixture.intent.request_id(),
                AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xD999))?,
            )?,
            None
        );
        assert!(first.entries().iter().all(|entry| {
            entry.request_id() == fixture.intent.request_id()
                && entry.profile_id() == fixture.profile_id
        }));
        assert_eq!(
            first.entries()[0].target(),
            &AgentProfileDeletionRuntimeTarget::LinkedProviderConversation {
                conversation_id: "manifest-conversation-0".to_owned(),
            }
        );
        let cursor = first
            .next_runtime_id()
            .ok_or("missing frozen-entry cursor")?;
        let last = catalog.profile_deletion_runtime_entries_page(
            fixture.intent.request_id(),
            Some(cursor),
            32,
        )?;
        assert_eq!(last.entries().len(), 1);
        assert_eq!(
            last.entries()[0].runtime_session_id(),
            fixture.pending_session.id()
        );
        assert_eq!(
            last.entries()[0].target(),
            &AgentProfileDeletionRuntimeTarget::NoLinkedTarget
        );
        assert!(matches!(
            catalog.confirm_runtime_session_ready(
                fixture.pending_session.id(),
                fixture.pending_session.revision(),
                "late-unfrozen-conversation",
                TimestampMillis::new(121)?,
            ),
            Err(AgentProfileError::ProfileDeletionTargetsFrozen(id))
                if id == fixture.profile_id
        ));
        assert_eq!(
            catalog.confirm_runtime_session_ready(
                fixture.exact_ready.id(),
                fixture.exact_ready.revision(),
                "manifest-conversation-1",
                TimestampMillis::new(121)?,
            )?,
            fixture.exact_ready
        );
        assert_frozen_runtime_mutation_contract(catalog, fixture)?;
        Ok(())
    }

    fn assert_frozen_runtime_mutation_contract(
        catalog: &mut AgentProfileCatalog,
        fixture: &DeletionManifestFixture,
    ) -> Result<(), Box<dyn Error>> {
        assert_eq!(
            catalog.confirm_existing_runtime_wake_observed(
                fixture.exact_ready.id(),
                fixture.exact_ready.revision(),
                "manifest-conversation-1",
                TimestampMillis::new(122)?,
            )?,
            fixture.exact_ready
        );
        assert!(matches!(
            catalog.confirm_existing_runtime_wake_observed(
                fixture.offline_before_freeze.id(),
                fixture.offline_before_freeze.revision(),
                "manifest-conversation-2",
                TimestampMillis::new(122)?,
            ),
            Err(AgentProfileError::ProfileDeletionTargetsFrozen(id))
                if id == fixture.profile_id
        ));
        assert!(matches!(
            catalog.transition_runtime_session(
                fixture.offline_before_freeze.id(),
                fixture.offline_before_freeze.revision(),
                AgentProfileRuntimeSessionState::Closed,
                TimestampMillis::new(123)?,
            ),
            Err(AgentProfileError::ProfileDeletionTargetsFrozen(id))
                if id == fixture.profile_id
        ));
        assert_eq!(
            catalog.runtime_session(fixture.offline_before_freeze.id())?,
            Some(fixture.offline_before_freeze.clone())
        );
        Ok(())
    }

    #[test]
    fn deletion_manifest_freezes_all_runtime_rows_and_finalizes_atomically()
    -> Result<(), Box<dyn Error>> {
        let (path, root) = database("deletion-manifest-finalization")?;
        let mut catalog = AgentProfileCatalog::open(&path)?;
        let fixture = stage_deletion_manifest_fixture(&mut catalog)?;
        assert_frozen_manifest_snapshot(&mut catalog, &fixture)?;
        drop(catalog);

        let mut reopened = AgentProfileCatalog::open(&path)?;
        assert_eq!(
            reopened.profile_deletion_manifest(fixture.intent.request_id())?,
            Some(fixture.manifest)
        );
        assert_eq!(
            reopened.runtime_session(fixture.already_closed.id())?,
            Some(fixture.already_closed.clone())
        );
        let completed = reopened
            .finish_profile_deletion(fixture.intent.request_id(), TimestampMillis::new(200)?)?;
        assert_eq!(completed.completed_at(), Some(TimestampMillis::new(200)?));
        assert_eq!(
            reopened
                .resolve(fixture.profile_id)?
                .ok_or("Profile tombstone missing")?
                .lifecycle_state(),
            AgentProfileLifecycleState::Retired
        );
        assert_eq!(
            reopened.runtime_session(fixture.already_closed.id())?,
            Some(fixture.already_closed.clone())
        );
        let pending = reopened
            .runtime_session(fixture.pending_session.id())?
            .ok_or("frozen Runtime row missing")?;
        assert_eq!(pending.state(), AgentProfileRuntimeSessionState::Closed);
        assert_eq!(pending.closed_at(), Some(TimestampMillis::new(200)?));
        assert_eq!(pending.provider_conversation_id(), None);
        assert_eq!(
            reopened.logical_session_link(fixture.logical_session_id)?,
            Some(fixture.original_link)
        );
        assert_eq!(
            reopened
                .finish_profile_deletion(fixture.intent.request_id(), TimestampMillis::new(201)?,)?,
            completed
        );
        let first = reopened.runtime_sessions_page(fixture.profile_id, None, 32)?;
        let second =
            reopened.runtime_sessions_page(fixture.profile_id, first.next_runtime_id(), 32)?;
        assert_eq!(
            first
                .sessions()
                .iter()
                .chain(second.sessions())
                .filter(|session| session.state() == AgentProfileRuntimeSessionState::Closed)
                .count(),
            33
        );
        drop(reopened);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn disable_during_deletion_preserves_intent_manifest_and_runtime_rows()
    -> Result<(), Box<dyn Error>> {
        let (path, root) = database("disable-during-deletion")?;
        let mut catalog = AgentProfileCatalog::open(&path)?;
        let fixture = stage_deletion_manifest_fixture(&mut catalog)?;
        let profile = catalog
            .resolve(fixture.profile_id)?
            .ok_or("Profile missing")?;
        let intent_before = catalog
            .profile_deletion_intent(fixture.intent.request_id())?
            .ok_or("deletion intent missing")?;
        let manifest_before = catalog
            .profile_deletion_manifest(fixture.intent.request_id())?
            .ok_or("deletion manifest missing")?;
        let runtime_before = catalog
            .runtime_session(fixture.pending_session.id())?
            .ok_or("pending Runtime Session missing")?;
        let receipt = catalog.mutate_profile(&mutation_command(
            &profile,
            0xD8F0,
            ProfileMutation::Disable,
            130,
        )?)?;
        assert_eq!(
            receipt.profile().lifecycle_state(),
            AgentProfileLifecycleState::Disabled
        );
        assert_eq!(
            catalog.profile_deletion_intent(fixture.intent.request_id())?,
            Some(intent_before)
        );
        assert_eq!(
            catalog.profile_deletion_manifest(fixture.intent.request_id())?,
            Some(manifest_before)
        );
        assert_eq!(
            catalog.runtime_session(fixture.pending_session.id())?,
            Some(runtime_before)
        );
        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn deletion_intent_inventory_pages_all_rows_after_reopen() -> Result<(), Box<dyn Error>> {
        let (path, root) = database("deletion-intent-page")?;
        let mut catalog = AgentProfileCatalog::open(&path)?;
        let mut profile_ids = Vec::new();
        for index in 0..33_u128 {
            let profile = catalog
                .create_profile(&command(
                    0xE100 + index,
                    0xE200 + index,
                    &format!("deletion-page-{index}"),
                )?)?
                .profile()
                .clone();
            profile_ids.push(profile.id());
            let intent = catalog.begin_profile_deletion(&AgentProfileDeletionCommand::new(
                Uuid::from_u128(0xE400 + index),
                profile.id(),
                profile.revision(),
                TimestampMillis::new(20)?,
            )?)?;
            if index == 0 {
                let retired = catalog.retire_profile(
                    profile.id(),
                    intent.barrier_profile_revision(),
                    TimestampMillis::new(30)?,
                )?;
                assert_eq!(
                    retired.lifecycle_state(),
                    AgentProfileLifecycleState::Retired
                );
            }
        }

        let first = catalog.profile_deletion_intents_page(None, 32)?;
        assert_eq!(first.intents().len(), 32);
        assert_eq!(first.intents()[0].request_id(), Uuid::from_u128(0xE400));
        assert_eq!(first.intents()[0].profile_id(), profile_ids[0]);
        assert!(
            first
                .intents()
                .windows(2)
                .all(|pair| pair[0].request_id() < pair[1].request_id())
        );
        let cursor = first
            .next_request_id()
            .ok_or("missing intent page cursor")?;
        assert_eq!(
            Some(cursor),
            first.intents().last().map(|intent| intent.request_id())
        );
        drop(catalog);

        let reopened = AgentProfileCatalog::open(&path)?;
        let second = reopened.profile_deletion_intents_page(Some(cursor), 32)?;
        assert_eq!(second.intents().len(), 1);
        assert_eq!(second.intents()[0].request_id(), Uuid::from_u128(0xE420));
        assert_eq!(second.next_request_id(), None);
        let all = first
            .intents()
            .iter()
            .chain(second.intents())
            .collect::<Vec<_>>();
        assert_eq!(all.len(), 33);
        assert!(
            all.windows(2)
                .all(|pair| pair[0].request_id() < pair[1].request_id())
        );
        assert_eq!(
            reopened
                .resolve(profile_ids[0])?
                .map(|profile| profile.lifecycle_state()),
            Some(AgentProfileLifecycleState::Retired)
        );
        assert!(matches!(
            reopened.profile_deletion_intents_page(None, 0),
            Err(AgentProfileError::ProfileDeletionIntentPageLimit { .. })
        ));
        assert!(matches!(
            reopened.profile_deletion_intents_page(None, 33),
            Err(AgentProfileError::ProfileDeletionIntentPageLimit { .. })
        ));
        assert!(matches!(
            reopened.profile_deletion_intents_page(Some(Uuid::nil()), 1),
            Err(AgentProfileError::InvalidIdentity(
                "profile_deletion_request_cursor"
            ))
        ));
        drop(reopened);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn create_replay_and_reopen_keep_first_global_identity() -> Result<(), Box<dyn Error>> {
        let (path, root) = database("replay")?;
        let first_command = command(0xA101, 0xA201, "luna-rust-01")?;
        let mut catalog = AgentProfileCatalog::open(&path)?;
        let first = catalog.create_profile(&first_command)?;
        assert!(first.created());
        assert_eq!(first.profile().revision().get(), 1);
        assert_eq!(
            first.profile().lifecycle_state(),
            AgentProfileLifecycleState::Active
        );
        assert_eq!(first.profile().base_capabilities(), ["backend", "rust"]);

        let replay = catalog.create_profile(&command(0xA102, 0xA201, "luna-rust-01")?)?;
        assert!(!replay.created());
        assert_eq!(replay.profile().id(), first.profile().id());
        drop(catalog);

        let reopened = AgentProfileCatalog::open(&path)?;
        let readback = reopened
            .resolve(first.profile().id())?
            .ok_or("profile missing after reopen")?;
        assert_eq!(&readback, first.profile());
        drop(reopened);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn original_create_replay_uses_immutable_content_after_configuration_update()
    -> Result<(), Box<dyn Error>> {
        let (path, root) = database("profile-configuration-replay")?;
        let first_command = command(0xF101, 0xF201, "configuration-replay")?;
        let mut catalog = AgentProfileCatalog::open(&path)?;
        let first = catalog.create_profile(&first_command)?;
        let update = mutation_command(
            first.profile(),
            0xF202,
            ProfileMutation::Update(configuration("updated display")?),
            20,
        )?;
        let updated = catalog.mutate_profile(&update)?;
        assert_eq!(updated.profile().revision().get(), 2);
        assert!(!updated.replayed());
        drop(catalog);

        let mut reopened = AgentProfileCatalog::open(&path)?;
        let replay = reopened.create_profile(&command(0xF102, 0xF201, "configuration-replay")?)?;
        assert!(!replay.created());
        assert_eq!(replay.profile().id(), first.profile().id());
        assert_eq!(replay.profile().display_name(), "updated display");
        assert_eq!(replay.profile().revision().get(), 2);
        assert!(matches!(
            reopened.create_profile(&CreateAgentProfile::new(
                AgentProfileId::from_uuid(Uuid::from_u128(0xF103))?,
                Uuid::from_u128(0xF201),
                "configuration-replay".to_owned(),
                "changed create content".to_owned(),
                AgentProfileProviderFamily::ChatGpt,
                Some("gpt-5.6-luna".to_owned()),
                Some("max".to_owned()),
                vec!["backend".to_owned(), "rust".to_owned()],
                AgentProfileCreationOrigin::UserManual,
                TimestampMillis::new(10)?,
            )?),
            Err(AgentProfileError::ReplayConflict(id)) if id == Uuid::from_u128(0xF201)
        ));
        drop(reopened);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn mutation_receipts_replay_exact_snapshots_and_reject_identity_drift()
    -> Result<(), Box<dyn Error>> {
        let (path, root) = database("profile-mutation-receipts")?;
        let mut catalog = AgentProfileCatalog::open(&path)?;
        let created = catalog.create_profile(&command(0xF301, 0xF401, "mutation-receipt")?)?;
        let update = mutation_command(
            created.profile(),
            0xF402,
            ProfileMutation::Update(configuration("receipt update")?),
            20,
        )?;
        let update_receipt = catalog.mutate_profile(&update)?;
        let disabled = mutation_command(
            update_receipt.profile(),
            0xF403,
            ProfileMutation::Disable,
            30,
        )?;
        let disabled_receipt = catalog.mutate_profile(&disabled)?;
        assert_eq!(
            disabled_receipt.profile().lifecycle_state(),
            AgentProfileLifecycleState::Disabled
        );

        let replay = catalog.mutate_profile(&update)?;
        assert!(replay.replayed());
        assert_eq!(replay.profile(), update_receipt.profile());
        assert_eq!(
            catalog
                .mutation_receipt(update.request_id())?
                .map(|value| value.replayed()),
            Some(false)
        );
        assert!(matches!(
            catalog.mutate_profile(&mutation_command(
                created.profile(),
                0xF402,
                ProfileMutation::Update(configuration("different payload")?),
                99,
            )?),
            Err(AgentProfileError::ProfileMutationReplayConflict(id))
                if id == Uuid::from_u128(0xF402)
        ));
        assert!(matches!(
            catalog.mutate_profile(&ProfileMutationCommand::new(
                Uuid::from_u128(0xF402),
                created.profile().id(),
                AgentProfileRevision::new(2)?,
                ProfileMutation::Update(configuration("receipt update")?),
                TimestampMillis::new(99)?,
            )?),
            Err(AgentProfileError::ProfileMutationReplayConflict(id))
                if id == Uuid::from_u128(0xF402)
        ));
        assert!(matches!(
            catalog.mutate_profile(&ProfileMutationCommand::new(
                Uuid::from_u128(0xF402),
                created.profile().id(),
                created.profile().revision(),
                ProfileMutation::Disable,
                TimestampMillis::new(99)?,
            )?),
            Err(AgentProfileError::ProfileMutationReplayConflict(id))
                if id == Uuid::from_u128(0xF402)
        ));
        let other_profile = catalog
            .create_profile(&command(0xF304, 0xF404, "other-mutation-profile")?)?
            .profile()
            .clone();
        assert!(matches!(
            catalog.mutate_profile(&ProfileMutationCommand::new(
                Uuid::from_u128(0xF402),
                other_profile.id(),
                other_profile.revision(),
                ProfileMutation::Update(configuration("receipt update")?),
                TimestampMillis::new(99)?,
            )?),
            Err(AgentProfileError::ProfileMutationReplayConflict(id))
                if id == Uuid::from_u128(0xF402)
        ));
        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn retire_receipt_replay_stays_the_original_snapshot_after_retirement()
    -> Result<(), Box<dyn Error>> {
        let (path, root) = database("profile-retire-receipt-replay")?;
        let mut catalog = AgentProfileCatalog::open(&path)?;
        let created = catalog.create_profile(&command(0xF701, 0xF801, "retire-receipt")?)?;
        let command = mutation_command(created.profile(), 0xF802, ProfileMutation::Retire, 20)?;
        let first = catalog.mutate_profile(&command)?;
        assert!(!first.replayed());
        assert_eq!(
            first.profile().lifecycle_state(),
            AgentProfileLifecycleState::Retired
        );
        let replay = catalog.mutate_profile(&command)?;
        assert!(replay.replayed());
        assert_eq!(replay.profile(), first.profile());
        assert_eq!(
            catalog
                .mutation_receipt(command.request_id())?
                .map(|value| value.profile().clone()),
            Some(first.profile().clone())
        );
        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn profile_mutation_safety_preserves_history_and_blocks_invalid_transitions()
    -> Result<(), Box<dyn Error>> {
        let (path, root) = database("profile-mutation-safety")?;
        let mut catalog = AgentProfileCatalog::open(&path)?;
        let created = catalog.create_profile(&command(0xF501, 0xF601, "mutation-safety")?)?;
        let stale = ProfileMutationCommand::new(
            Uuid::from_u128(0xF602),
            created.profile().id(),
            AgentProfileRevision::new(2)?,
            ProfileMutation::Update(configuration("stale")?),
            TimestampMillis::new(20)?,
        )?;
        assert!(matches!(
            catalog.mutate_profile(&stale),
            Err(AgentProfileError::ProfileRevisionConflict { .. })
        ));
        assert_eq!(catalog.mutation_receipt(stale.request_id())?, None);

        let runtime = catalog.create_runtime_session(&CreateAgentProfileRuntimeSession::new(
            AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xF604))?,
            Uuid::from_u128(0xF605),
            created.profile().id(),
            AgentProfileProviderFamily::ChatGpt,
            TimestampMillis::new(21)?,
        )?)?;
        let disable = mutation_command(created.profile(), 0xF603, ProfileMutation::Disable, 22)?;
        let disabled = catalog.mutate_profile(&disable)?;
        assert_eq!(
            catalog.runtime_session(runtime.session().id())?,
            Some(runtime.session().clone())
        );
        assert!(matches!(
            catalog.create_runtime_session(&CreateAgentProfileRuntimeSession::new(
                AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xF608))?,
                Uuid::from_u128(0xF609),
                created.profile().id(),
                AgentProfileProviderFamily::ChatGpt,
                TimestampMillis::new(23)?,
            )?),
            Err(AgentProfileError::ProfileNotActive { .. })
        ));
        assert!(matches!(
            catalog.mutate_profile(&mutation_command(
                &disabled.profile().clone(),
                0xF606,
                ProfileMutation::Update(configuration("blocked")?),
                30,
            )?),
            Err(AgentProfileError::ProfileNotActive { .. })
        ));
        assert!(matches!(
            catalog.mutate_profile(&mutation_command(
                &disabled.profile().clone(),
                0xF607,
                ProfileMutation::Retire,
                31,
            )?),
            Err(AgentProfileError::LiveRuntimeSessions(id)) if id == created.profile().id()
        ));
        assert_eq!(catalog.mutation_receipt(Uuid::from_u128(0xF607))?, None);
        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn same_request_with_different_profile_content_is_replay_conflict() -> Result<(), Box<dyn Error>>
    {
        let (path, root) = database("replay-conflict")?;
        let mut catalog = AgentProfileCatalog::open(&path)?;
        let _created = catalog.create_profile(&command(0xB101, 0xB201, "luna-rust-01")?)?;
        let conflict = catalog.create_profile(&command(0xB102, 0xB201, "astra-review-01")?);
        assert!(matches!(
            conflict,
            Err(AgentProfileError::ReplayConflict(_))
        ));
        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn profile_catalog_has_no_scoped_authority_columns() -> Result<(), Box<dyn Error>> {
        let (path, root) = database("authority-boundary")?;
        let catalog = AgentProfileCatalog::open(&path)?;
        let connection = Connection::open(catalog.path())?;
        let columns = connection
            .prepare("PRAGMA table_info(agent_profiles)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        for forbidden in [
            "workspace_id",
            "team_id",
            "participant_id",
            "session_id",
            "agent_id",
            "assignment_id",
            "writer_lease_id",
            "vote_id",
            "work_credit",
        ] {
            assert!(!columns.iter().any(|column| column == forbidden));
        }
        drop(connection);
        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn duplicate_capability_and_nil_id_are_rejected_before_persistence()
    -> Result<(), Box<dyn Error>> {
        assert!(matches!(
            AgentProfileId::from_uuid(Uuid::nil()),
            Err(AgentProfileError::InvalidIdentity("agent_profile_id"))
        ));
        let duplicate = CreateAgentProfile::new(
            AgentProfileId::from_uuid(Uuid::from_u128(0xC101))?,
            Uuid::from_u128(0xC201),
            "duplicate-capability".to_owned(),
            "Duplicate Capability".to_owned(),
            AgentProfileProviderFamily::ChatGpt,
            None,
            None,
            vec!["rust".to_owned(), "rust".to_owned()],
            AgentProfileCreationOrigin::AgentRequest,
            TimestampMillis::new(1)?,
        );
        assert!(matches!(
            duplicate,
            Err(AgentProfileError::DuplicateCapability)
        ));
        Ok(())
    }

    #[test]
    fn logical_session_history_is_bounded_newest_first_and_reports_truncation()
    -> Result<(), Box<dyn Error>> {
        let (path, root) = database("session-history-bound")?;
        let mut catalog = AgentProfileCatalog::open(&path)?;
        let created = catalog.create_profile(&command(0xD101, 0xD201, "history-profile")?)?;
        let profile_id = created.profile().id();
        let base_request = 0xD300_u128;
        let base_session = 0xD400_u128;
        for index in 0..=MAX_AGENT_PROFILE_LOGICAL_SESSION_LINKS {
            let offset = u128::try_from(index)?;
            let session_id =
                forge_domain::AgentSessionId::from_uuid(Uuid::from_u128(base_session + offset));
            let _link = catalog.link_verified_logical_session(
                Uuid::from_u128(base_request + offset),
                profile_id,
                session_id,
                forge_domain::AgentSessionRevision::new(1)?,
                TimestampMillis::new(100 + i64::try_from(index)?)?,
            )?;
        }

        let history = catalog.logical_session_history(profile_id)?;
        assert_eq!(
            history.links().len(),
            MAX_AGENT_PROFILE_LOGICAL_SESSION_LINKS
        );
        assert!(history.truncated());
        assert_eq!(
            history.links()[0].session_id(),
            forge_domain::AgentSessionId::from_uuid(Uuid::from_u128(
                base_session + u128::try_from(MAX_AGENT_PROFILE_LOGICAL_SESSION_LINKS)?
            ))
        );
        assert_eq!(
            history.links().last().map(|link| link.session_id()),
            Some(forge_domain::AgentSessionId::from_uuid(Uuid::from_u128(
                base_session + 1
            )))
        );

        let request_id = Uuid::from_u128(base_request + 1);
        let session_id = forge_domain::AgentSessionId::from_uuid(Uuid::from_u128(base_session + 1));
        let before = catalog
            .logical_session_link(session_id)?
            .ok_or("link missing")?;
        let retired = catalog.retire_profile(
            profile_id,
            created.profile().revision(),
            TimestampMillis::new(200)?,
        )?;
        assert_eq!(
            retired.lifecycle_state(),
            AgentProfileLifecycleState::Retired
        );
        let request_replay = catalog.link_verified_logical_session(
            request_id,
            profile_id,
            session_id,
            forge_domain::AgentSessionRevision::new(1)?,
            TimestampMillis::new(101)?,
        )?;
        assert!(!request_replay.created());
        assert_eq!(request_replay.link(), before);
        let session_replay = catalog.link_verified_logical_session(
            Uuid::from_u128(base_request + 100),
            profile_id,
            session_id,
            forge_domain::AgentSessionRevision::new(1)?,
            TimestampMillis::new(101)?,
        )?;
        assert!(!session_replay.created());
        assert_eq!(session_replay.link(), before);
        assert!(matches!(
            catalog.link_verified_logical_session(
                Uuid::from_u128(base_request + 101),
                profile_id,
                forge_domain::AgentSessionId::from_uuid(Uuid::from_u128(base_session + 101)),
                forge_domain::AgentSessionRevision::new(1)?,
                TimestampMillis::new(201)?,
            ),
            Err(AgentProfileError::ProfileNotActive {
                profile_id: id,
                lifecycle_state: AgentProfileLifecycleState::Retired,
            }) if id == profile_id
        ));
        let history_after_retirement = catalog.logical_session_history(profile_id)?;
        assert_eq!(history_after_retirement.links(), history.links());
        drop(catalog);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn foreign_database_is_rejected_without_reconfiguration() -> Result<(), Box<dyn Error>> {
        let (path, root) = database("foreign")?;
        let connection = Connection::open(&path)?;
        connection.execute_batch("CREATE TABLE foreign_table(id INTEGER PRIMARY KEY);")?;
        drop(connection);
        let before = fs::read(&path)?;
        let opened = AgentProfileCatalog::open(&path);
        assert!(matches!(opened, Err(AgentProfileError::Ownership { .. })));
        assert_eq!(fs::read(&path)?, before);
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
