//! Native Swarm coordination overlay over `ops_workspace_messages`,
//! two-phase member watermarks, single-flight coalesced doorbells, and
//! lineage-separated adversarial verifier panels.

mod board;
pub(crate) mod schema;
mod sql;
mod verifier_panel;

#[cfg(test)]
mod tests;

use std::fmt;
use std::str::FromStr;

use forge_domain::{AuthenticatedPrincipal, WorkspaceId};
use heterarchy_orchestration_sessions::AgentProfileId;
use uuid::Uuid;

use crate::sqlite_codecs::principal_is_valid;
use crate::{ExecutionAuthorIdentity, OperationsError};

pub use board::{
    AddSwarmMemberCommand, BreakSwarmLivelockCommand, BreakSwarmLivelockOutcome,
    ClaimSwarmWakeupCommand, CompleteSwarmTurnCommand, CreateSwarmChannelCommand,
    CreateSwarmRunCommand, FailOrSuspendSwarmTurnCommand, KillSwarmMemberCommand,
    PostSwarmMessageCommand, ReadSwarmBoardQuery, RecordSwarmConvergenceCommand,
    RecordSwarmStallNudgeCommand, SettleStandaloneSwarmCommand, SubscribeSwarmChannelCommand,
    SwarmBoardMessage, SwarmBoardPage, SwarmChannelRecord, SwarmDoorbellReason,
    SwarmDoorbellRecord, SwarmMemberRecord, SwarmMemberState, SwarmMessageAuthor,
    SwarmPostMessageOutcome, SwarmRoleKind, SwarmRunMode, SwarmRunRecord, SwarmRunSnapshot,
    SwarmRunState, SwarmStallNudgeOutcome, SwarmTurnFailureDisposition, SwarmTurnOutput,
    SwarmWakeupBatch, SwarmWorkspaceMode, SwitchSwarmMemberProfileCommand,
};
pub use verifier_panel::{
    CreateSwarmVerificationPanelCommand, RecordSwarmVerdictCommand, SwarmConvergenceGateOutcome,
    SwarmPanelSeatKind, SwarmPanelSeatRecord, SwarmVerdictRecord, SwarmVerificationPanelRecord,
};

/// Maximum spawn depth allowed for nested Swarm helpers (`0..=4`).
pub const MAX_SWARM_SPAWN_DEPTH: u8 = 4;
/// Maximum active members permitted in a single Swarm run.
pub const MAX_SWARM_MEMBERS_PER_RUN: usize = 256;
/// Maximum channels permitted in a single Swarm run.
pub const MAX_SWARM_CHANNELS_PER_RUN: usize = 64;
/// Maximum UTF-8 byte length of a Swarm run title.
pub const MAX_SWARM_TITLE_BYTES: usize = 256;
/// Maximum UTF-8 byte length of a Swarm channel name.
pub const MAX_SWARM_CHANNEL_NAME_BYTES: usize = 64;
/// Maximum UTF-8 byte length of a Swarm channel purpose.
pub const MAX_SWARM_CHANNEL_PURPOSE_BYTES: usize = 512;
/// Maximum UTF-8 byte length of a Swarm member `roster_name`.
pub const MAX_SWARM_ROSTER_NAME_BYTES: usize = 128;
/// Maximum messages coalesced into a single wakeup snapshot batch.
pub const MAX_SNAPSHOT_COALESCED_MESSAGES: usize = 32;
/// Maximum cumulative message body bytes coalesced into a single wakeup snapshot batch (`64 KiB`).
pub const MAX_SNAPSHOT_COALESCED_BYTES: usize = 64 * 1024;
/// Maximum messages returned per incremental board read page.
pub const MAX_SWARM_BOARD_PAGE_SIZE: usize = 128;
/// Maximum total turns budget configurable on a Swarm run.
pub const MAX_SWARM_TOTAL_TURNS: u64 = 100_000;
/// Maximum verifier quorum configurable on a panel or run.
pub const MAX_SWARM_VERIFIER_QUORUM: u32 = 16;
/// Default Swarm coordination channel name auto-created for every run.
pub const SWARM_MAIN_CHANNEL_NAME: &str = "main";
/// Maximum rationale bytes for a Swarm verifier verdict.
pub const MAX_SWARM_VERDICT_RATIONALE_BYTES: usize = 8_192;

/// Fail-closed domain and persistence errors returned by Swarm operations.
#[derive(Debug, thiserror::Error)]
pub enum SwarmError {
    #[error("self-review or Sybil lineage violation: {reason}")]
    SybilLineageViolation { reason: &'static str },
    #[error("Swarm Verifier members must use Inherit workspace mode")]
    VerifierMustInherit,
    #[error("workers and helpers may only spawn Helper members, never Verifier or Coordinator")]
    WorkerVerifierSpawnForbidden,
    #[error("Swarm spawn depth {depth} exceeds maximum {max_depth}")]
    SpawnDepthExceeded { depth: u8, max_depth: u8 },
    #[error("Swarm active member count {current} has reached max_members {max_members}")]
    MaxMembersExceeded { current: usize, max_members: usize },
    #[error("Swarm total turn budget exhausted ({used}/{max_turns})")]
    TurnBudgetExceeded { used: u64, max_turns: u64 },
    #[error(
        "DagNodeHybrid Swarm run halts at CandidateReady and cannot transition directly to Settled"
    )]
    DagNodeHybridCannotSettleDirectly,
    #[error("worker @all mention is disabled by Swarm run policy")]
    WorkerAllMentionForbidden,
    #[error("converged_diff_digest does not match accepted Swarm verifier verdict digest")]
    VerdictDiffDigestMismatch,
    #[error("Swarm convergence gate requires quorum of Accepted independent verdicts")]
    ReviewNotAccepted,
    #[error("invalid Swarm state transition: {0}")]
    InvalidStateTransition(&'static str),
    #[error("Swarm entity not found: {0}")]
    NotFound(&'static str),
    #[error(transparent)]
    Operations(#[from] OperationsError),
}

/// Validated lowercase hexadecimal Git commit OID (`40..=64` ASCII hex characters).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SwarmCommitOid(String);

impl SwarmCommitOid {
    /// Validate and canonicalize a Git commit OID hex string (`40..=64` ASCII hex chars).
    ///
    /// # Errors
    /// Rejects non-hex strings or lengths outside `40..=64`.
    pub fn new(raw: impl Into<String>) -> Result<Self, OperationsError> {
        let value = raw.into();
        let trimmed = value.trim();
        if !(40..=64).contains(&trimmed.len())
            || !trimmed.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(OperationsError::Bounds("swarm commit oid"));
        }
        Ok(Self(trimmed.to_ascii_lowercase()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Display for SwarmCommitOid {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for SwarmCommitOid {
    type Err = OperationsError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Self::new(raw)
    }
}

/// Complete three-axis identity of a Swarm member (`AgentProfileId`, `participant_id`, `AuthenticatedPrincipal`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwarmMemberIdentity {
    profile_id: AgentProfileId,
    participant_id: Uuid,
    principal: AuthenticatedPrincipal,
}

impl SwarmMemberIdentity {
    /// Construct a validated three-axis [`SwarmMemberIdentity`].
    ///
    /// # Errors
    /// Rejects nil `profile_id`, nil `participant_id`, or nil token/grant principals.
    pub fn new(
        profile_id: AgentProfileId,
        participant_id: Uuid,
        principal: AuthenticatedPrincipal,
    ) -> Result<Self, OperationsError> {
        let identity = Self {
            profile_id,
            participant_id,
            principal,
        };
        identity.validate()?;
        Ok(identity)
    }

    #[must_use]
    pub const fn profile_id(&self) -> AgentProfileId {
        self.profile_id
    }

    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }

    pub const fn principal(&self) -> AuthenticatedPrincipal {
        self.principal
    }

    fn validate(&self) -> Result<(), OperationsError> {
        if self.profile_id.into_uuid().is_nil()
            || self.participant_id.is_nil()
            || !principal_is_valid(self.principal)
        {
            return Err(OperationsError::Bounds("swarm member identity"));
        }
        Ok(())
    }
}

impl From<ExecutionAuthorIdentity> for SwarmMemberIdentity {
    fn from(value: ExecutionAuthorIdentity) -> Self {
        Self {
            profile_id: value.profile_id(),
            participant_id: value.participant_id(),
            principal: value.principal(),
        }
    }
}

fn validate_non_nil_uuid(value: Uuid, label: &'static str) -> Result<(), OperationsError> {
    if value.is_nil() {
        return Err(OperationsError::Bounds(label));
    }
    Ok(())
}

fn validate_workspace_id(value: WorkspaceId) -> Result<(), OperationsError> {
    validate_non_nil_uuid(value.into_uuid(), "swarm workspace_id")
}

fn validate_non_zero_digest(digest: [u8; 32], label: &'static str) -> Result<(), OperationsError> {
    if digest == [0_u8; 32] {
        return Err(OperationsError::Bounds(label));
    }
    Ok(())
}

fn validate_identifier(
    raw: &str,
    max_bytes: usize,
    label: &'static str,
) -> Result<(), OperationsError> {
    if raw.is_empty() || raw.len() > max_bytes {
        return Err(OperationsError::Bounds(label));
    }
    let mut bytes = raw.bytes();
    let Some(first) = bytes.next() else {
        return Err(OperationsError::Bounds(label));
    };
    if !(first.is_ascii_lowercase() || first.is_ascii_digit()) {
        return Err(OperationsError::Bounds(label));
    }
    if !bytes.all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'_'
    }) {
        return Err(OperationsError::Bounds(label));
    }
    Ok(())
}

fn validate_roster_name(raw: &str) -> Result<(), OperationsError> {
    validate_identifier(raw, MAX_SWARM_ROSTER_NAME_BYTES, "swarm roster_name")?;
    if raw == "all" {
        return Err(OperationsError::Conflict(
            "swarm roster_name 'all' is reserved",
        ));
    }
    Ok(())
}

fn validate_channel_name(raw: &str) -> Result<(), OperationsError> {
    validate_identifier(raw, MAX_SWARM_CHANNEL_NAME_BYTES, "swarm channel_name")
}

fn validate_title(raw: &str) -> Result<(), OperationsError> {
    if raw.trim().is_empty() || raw.len() > MAX_SWARM_TITLE_BYTES || raw.contains('\0') {
        return Err(OperationsError::Bounds("swarm title"));
    }
    Ok(())
}

fn check_timestamp_non_negative(ts: i64, label: &'static str) -> Result<(), OperationsError> {
    if ts < 0 {
        return Err(OperationsError::Bounds(label));
    }
    Ok(())
}

fn map_sqlite_conflict(err: rusqlite::Error, label: &'static str) -> SwarmError {
    if let rusqlite::Error::SqliteFailure(sqlite_err, _) = &err
        && matches!(sqlite_err.code, rusqlite::ErrorCode::ConstraintViolation)
    {
        return SwarmError::Operations(OperationsError::Conflict(label));
    }
    SwarmError::Operations(OperationsError::from(err))
}
