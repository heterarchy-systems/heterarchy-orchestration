//! Typed local MCP bearer-token metadata and lifecycle invariants.
//!
//! Owned by the orchestration token authority; the Forge domain no longer
//! carries these types after the MCP retirement.  Raw bearer bytes and the
//! keyed-hash pepper never enter this model.

use std::fmt;
use std::str::FromStr;

use forge_domain::TimestampMillis;
use thiserror::Error;
use uuid::Uuid;

pub const MCP_TOKEN_DIGEST_BYTES: usize = 32;
pub const MAX_MCP_TOKEN_LABEL_BYTES: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct McpTokenId(Uuid);

impl McpTokenId {
    #[must_use]
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn into_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for McpTokenId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for McpTokenId {
    type Err = McpTokenIdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value)
            .map(Self)
            .map_err(McpTokenIdParseError::InvalidUuid)
    }
}

#[derive(Debug, Error)]
pub enum McpTokenIdParseError {
    #[error("invalid MCP token UUID")]
    InvalidUuid(#[source] uuid::Error),
}

#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpTokenLabel(String);

impl McpTokenLabel {
    /// Validate and wrap one caller-supplied label.
    ///
    /// # Errors
    /// Rejects empty, oversized, or control-character-bearing labels.
    pub fn new(value: String) -> Result<Self, McpTokenLabelError> {
        if value.is_empty() {
            return Err(McpTokenLabelError::Empty);
        }
        if value.len() > MAX_MCP_TOKEN_LABEL_BYTES {
            return Err(McpTokenLabelError::TooLong {
                actual: value.len(),
                maximum: MAX_MCP_TOKEN_LABEL_BYTES,
            });
        }
        if value.chars().any(char::is_control) {
            return Err(McpTokenLabelError::ControlCharacter);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum McpTokenLabelError {
    #[error("MCP token label is empty")]
    Empty,
    #[error("MCP token label is {actual} bytes; maximum is {maximum}")]
    TooLong { actual: usize, maximum: usize },
    #[error("MCP token label contains a control character")]
    ControlCharacter,
}

/// Keyed digest persisted for bearer-token lookup. Debug output is redacted.
#[must_use]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct McpTokenDigest([u8; MCP_TOKEN_DIGEST_BYTES]);

impl McpTokenDigest {
    pub const fn new(bytes: [u8; MCP_TOKEN_DIGEST_BYTES]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn into_bytes(self) -> [u8; MCP_TOKEN_DIGEST_BYTES] {
        self.0
    }
}

impl fmt::Debug for McpTokenDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpTokenDigest(<redacted>)")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct McpTokenRevision(i64);

impl McpTokenRevision {
    pub const INITIAL: Self = Self(1);

    /// Wrap one positive durable revision counter.
    ///
    /// # Errors
    /// Rejects zero and negative counters.
    pub const fn new(value: i64) -> Result<Self, McpTokenRevisionError> {
        if value > 0 {
            Ok(Self(value))
        } else {
            Err(McpTokenRevisionError::Invalid(value))
        }
    }

    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }

    /// Advance one optimistic-concurrency revision.
    ///
    /// # Errors
    /// Fails when the counter would overflow.
    pub const fn checked_next(self) -> Result<Self, McpTokenRevisionError> {
        match self.0.checked_add(1) {
            Some(value) => Ok(Self(value)),
            None => Err(McpTokenRevisionError::Overflow(self.0)),
        }
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum McpTokenRevisionError {
    #[error("MCP token revision must be positive, got {0}")]
    Invalid(i64),
    #[error("MCP token revision overflowed from {0}")]
    Overflow(i64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum McpTokenState {
    Active,
    Revoked,
    Rotated,
}

#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredMcpToken {
    id: McpTokenId,
    label: McpTokenLabel,
    digest: McpTokenDigest,
    state: McpTokenState,
    created_at: TimestampMillis,
    disabled_at: Option<TimestampMillis>,
    replacement_token_id: Option<McpTokenId>,
    revision: McpTokenRevision,
}

/// Checked rehydration input for persistence codecs.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpTokenRecordParts {
    pub id: McpTokenId,
    pub label: McpTokenLabel,
    pub digest: McpTokenDigest,
    pub state: McpTokenState,
    pub created_at: TimestampMillis,
    pub disabled_at: Option<TimestampMillis>,
    pub replacement_token_id: Option<McpTokenId>,
    pub revision: McpTokenRevision,
}

impl RegisteredMcpToken {
    /// Mint one active token record at its initial revision.
    ///
    /// # Errors
    /// Returns the lifecycle validation failure of the assembled record.
    pub fn issue(
        id: McpTokenId,
        label: McpTokenLabel,
        digest: McpTokenDigest,
        created_at: TimestampMillis,
    ) -> Result<Self, McpTokenTransitionError> {
        Self::from_parts(McpTokenRecordParts {
            id,
            label,
            digest,
            state: McpTokenState::Active,
            created_at,
            disabled_at: None,
            replacement_token_id: None,
            revision: McpTokenRevision::INITIAL,
        })
    }

    /// Rehydrate one persisted record through full lifecycle validation.
    ///
    /// # Errors
    /// Returns the lifecycle validation failure of the supplied parts.
    pub fn from_parts(parts: McpTokenRecordParts) -> Result<Self, McpTokenTransitionError> {
        let McpTokenRecordParts {
            id,
            label,
            digest,
            state,
            created_at,
            disabled_at,
            replacement_token_id,
            revision,
        } = parts;
        validate_lifecycle(id, state, created_at, disabled_at, replacement_token_id)?;
        Ok(Self {
            id,
            label,
            digest,
            state,
            created_at,
            disabled_at,
            replacement_token_id,
            revision,
        })
    }

    /// Transition one active record to `Revoked` at the next revision.
    ///
    /// # Errors
    /// Rejects revision conflicts, terminal states, and out-of-order clocks.
    pub fn revoke(
        &self,
        expected_revision: McpTokenRevision,
        disabled_at: TimestampMillis,
    ) -> Result<Self, McpTokenTransitionError> {
        self.ensure_active_revision(expected_revision, disabled_at)?;
        Self::from_parts(McpTokenRecordParts {
            id: self.id,
            label: self.label.clone(),
            digest: self.digest,
            state: McpTokenState::Revoked,
            created_at: self.created_at,
            disabled_at: Some(disabled_at),
            replacement_token_id: None,
            revision: self.revision.checked_next()?,
        })
    }

    /// Transition one active record to `Rotated` at the next revision.
    ///
    /// # Errors
    /// Rejects revision conflicts, terminal states, self-replacement, and
    /// out-of-order clocks.
    pub fn rotate(
        &self,
        replacement_token_id: McpTokenId,
        expected_revision: McpTokenRevision,
        disabled_at: TimestampMillis,
    ) -> Result<Self, McpTokenTransitionError> {
        self.ensure_active_revision(expected_revision, disabled_at)?;
        if replacement_token_id == self.id {
            return Err(McpTokenTransitionError::ReplacementIdentity);
        }
        Self::from_parts(McpTokenRecordParts {
            id: self.id,
            label: self.label.clone(),
            digest: self.digest,
            state: McpTokenState::Rotated,
            created_at: self.created_at,
            disabled_at: Some(disabled_at),
            replacement_token_id: Some(replacement_token_id),
            revision: self.revision.checked_next()?,
        })
    }

    fn ensure_active_revision(
        &self,
        expected_revision: McpTokenRevision,
        disabled_at: TimestampMillis,
    ) -> Result<(), McpTokenTransitionError> {
        if self.revision != expected_revision {
            return Err(McpTokenTransitionError::RevisionConflict);
        }
        if self.state != McpTokenState::Active {
            return Err(McpTokenTransitionError::TerminalState);
        }
        if disabled_at.get() < self.created_at.get() {
            return Err(McpTokenTransitionError::TimestampOrder);
        }
        Ok(())
    }

    #[must_use]
    pub const fn id(&self) -> McpTokenId {
        self.id
    }

    pub const fn label(&self) -> &McpTokenLabel {
        &self.label
    }

    pub const fn digest(&self) -> McpTokenDigest {
        self.digest
    }

    #[must_use]
    pub const fn state(&self) -> McpTokenState {
        self.state
    }

    #[must_use]
    pub const fn created_at(&self) -> TimestampMillis {
        self.created_at
    }

    #[must_use]
    pub const fn disabled_at(&self) -> Option<TimestampMillis> {
        self.disabled_at
    }

    #[must_use]
    pub const fn replacement_token_id(&self) -> Option<McpTokenId> {
        self.replacement_token_id
    }

    #[must_use]
    pub const fn revision(&self) -> McpTokenRevision {
        self.revision
    }
}

fn validate_lifecycle(
    id: McpTokenId,
    state: McpTokenState,
    created_at: TimestampMillis,
    disabled_at: Option<TimestampMillis>,
    replacement_token_id: Option<McpTokenId>,
) -> Result<(), McpTokenTransitionError> {
    if disabled_at.is_some_and(|value| value.get() < created_at.get()) {
        return Err(McpTokenTransitionError::TimestampOrder);
    }
    match state {
        McpTokenState::Active if disabled_at.is_none() && replacement_token_id.is_none() => Ok(()),
        McpTokenState::Revoked if disabled_at.is_some() && replacement_token_id.is_none() => Ok(()),
        McpTokenState::Rotated
            if disabled_at.is_some()
                && replacement_token_id.is_some_and(|replacement| replacement != id) =>
        {
            Ok(())
        }
        McpTokenState::Active | McpTokenState::Revoked | McpTokenState::Rotated => {
            Err(McpTokenTransitionError::LifecycleShape)
        }
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum McpTokenTransitionError {
    #[error("MCP token persisted lifecycle shape is invalid")]
    LifecycleShape,
    #[error("MCP token disabled timestamp precedes creation")]
    TimestampOrder,
    #[error("MCP token revision does not match")]
    RevisionConflict,
    #[error("MCP token is already terminal")]
    TerminalState,
    #[error("MCP token replacement identity must differ")]
    ReplacementIdentity,
    #[error(transparent)]
    Revision(#[from] McpTokenRevisionError),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timestamp(value: i64) -> Result<TimestampMillis, Box<dyn std::error::Error>> {
        Ok(TimestampMillis::new(value)?)
    }

    #[test]
    fn token_issue_redacts_digest() -> Result<(), Box<dyn std::error::Error>> {
        let token = RegisteredMcpToken::issue(
            McpTokenId::from_uuid(Uuid::from_u128(1)),
            McpTokenLabel::new("Local Codex".to_owned())?,
            McpTokenDigest::new([7; MCP_TOKEN_DIGEST_BYTES]),
            timestamp(1)?,
        )?;
        assert_eq!(token.state(), McpTokenState::Active);
        assert_eq!(token.revision(), McpTokenRevision::INITIAL);
        assert_eq!(
            format!("{:?}", token.digest()),
            "McpTokenDigest(<redacted>)"
        );
        Ok(())
    }

    #[test]
    fn token_terminal_transitions_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
        let id = McpTokenId::from_uuid(Uuid::from_u128(1));
        let label = McpTokenLabel::new("Local Codex".to_owned())?;
        let digest = McpTokenDigest::new([7; MCP_TOKEN_DIGEST_BYTES]);
        let token = RegisteredMcpToken::issue(id, label, digest, timestamp(1)?)?;
        let revoked = token.revoke(token.revision(), timestamp(2)?)?;
        assert_eq!(revoked.state(), McpTokenState::Revoked);
        assert!(revoked.revoke(revoked.revision(), timestamp(3)?).is_err());
        assert!(token.rotate(id, token.revision(), timestamp(2)?).is_err());
        Ok(())
    }
}
