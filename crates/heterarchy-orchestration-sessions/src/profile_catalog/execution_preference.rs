//! Durable CLI execution preferences attached to reusable Agent Profiles.
//!
//! Preferences describe reusable operator/agent defaults (`CliEngine`,
//! `preferred_model`, `preferred_reasoning`, execution `capabilities`, and
//! content-addressed `RolePromptDigest`). Scoped execution facts such as
//! `WorkspaceId`, `GoalId`, `NodeId`, worktree paths, leases, PIDs, and MCP
//! grants belong exclusively to `AssignmentExecutionSnapshot` in Operations.

use std::fmt;

use rusqlite::{Connection, OptionalExtension, Transaction, params};
use uuid::Uuid;

use super::{
    AgentProfileError, AgentProfileId, MAX_CAPABILITIES, MAX_CAPABILITY_BYTES,
    MAX_PROVIDER_PREFERENCE_BYTES, normalize_sql, validate_optional_text, validate_text,
};

const ROLE_PROMPT_DIGEST_BYTES: usize = 32;

/// Closed set of supported local CLI execution engines in V1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CliEngine {
    Codex,
    Agy,
}

impl CliEngine {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Agy => "agy",
        }
    }

    /// Parse a canonical lowercase CLI engine identifier.
    ///
    /// # Errors
    /// Rejects unknown or non-canonical engine identifiers.
    pub fn parse(value: &str) -> Result<Self, AgentProfileError> {
        match value {
            "codex" => Ok(Self::Codex),
            "agy" => Ok(Self::Agy),
            _ => Err(AgentProfileError::InvalidIdentity("cli_engine")),
        }
    }

    pub(super) const fn encode(self) -> i64 {
        match self {
            Self::Codex => 1,
            Self::Agy => 2,
        }
    }

    pub(super) fn decode(value: i64) -> Result<Self, AgentProfileError> {
        match value {
            1 => Ok(Self::Codex),
            2 => Ok(Self::Agy),
            _ => Err(AgentProfileError::Corrupt("invalid cli_engine")),
        }
    }
}

impl fmt::Display for CliEngine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Non-zero SHA-256 content digest of an agent's role prompt.
///
/// Durable profile configuration stores content digests rather than host
/// filesystem paths so prompts remain immutable and portable across scopes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RolePromptDigest([u8; ROLE_PROMPT_DIGEST_BYTES]);

impl RolePromptDigest {
    /// Construct a non-zero SHA-256 role prompt digest.
    ///
    /// # Errors
    /// Rejects an all-zero digest.
    pub fn new(bytes: [u8; ROLE_PROMPT_DIGEST_BYTES]) -> Result<Self, AgentProfileError> {
        if bytes == [0_u8; ROLE_PROMPT_DIGEST_BYTES] {
            return Err(AgentProfileError::InvalidIdentity("role_prompt_digest"));
        }
        Ok(Self(bytes))
    }

    /// Construct from a raw 32-byte slice.
    ///
    /// # Errors
    /// Rejects slices of length other than 32 or all-zero bytes.
    pub fn from_slice(bytes: &[u8]) -> Result<Self, AgentProfileError> {
        let array: [u8; ROLE_PROMPT_DIGEST_BYTES] = bytes
            .try_into()
            .map_err(|_| AgentProfileError::InvalidIdentity("role_prompt_digest"))?;
        Self::new(array)
    }

    /// Parse a 64-character lowercase hexadecimal SHA-256 digest.
    ///
    /// # Errors
    /// Rejects invalid lengths, uppercase/non-hex characters, and all-zero digests.
    pub fn from_hex(hex: &str) -> Result<Self, AgentProfileError> {
        let raw_bytes = hex.as_bytes();
        if raw_bytes.len() != ROLE_PROMPT_DIGEST_BYTES * 2 {
            return Err(AgentProfileError::InvalidIdentity("role_prompt_digest"));
        }
        let mut bytes = [0_u8; ROLE_PROMPT_DIGEST_BYTES];
        for (&[high_byte, low_byte], slot) in raw_bytes.as_chunks::<2>().0.iter().zip(&mut bytes) {
            let high = decode_lower_hex_nibble(high_byte)
                .ok_or(AgentProfileError::InvalidIdentity("role_prompt_digest"))?;
            let low = decode_lower_hex_nibble(low_byte)
                .ok_or(AgentProfileError::InvalidIdentity("role_prompt_digest"))?;
            *slot = (high << 4) | low;
        }
        Self::new(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; ROLE_PROMPT_DIGEST_BYTES] {
        &self.0
    }

    #[must_use]
    pub const fn into_bytes(self) -> [u8; ROLE_PROMPT_DIGEST_BYTES] {
        self.0
    }

    #[must_use]
    pub fn to_hex(&self) -> String {
        use fmt::Write as _;
        let mut encoded = String::with_capacity(ROLE_PROMPT_DIGEST_BYTES * 2);
        for byte in self.0 {
            let _ = write!(&mut encoded, "{byte:02x}");
        }
        encoded
    }
}

impl fmt::Display for RolePromptDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

const fn decode_lower_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

/// Reusable CLI execution preference stored on a global `AgentProfile`.
///
/// Profiles with `None` execution preference are ineligible for CLI node
/// execution without implicit engine or model fallback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionPreference {
    cli_engine: CliEngine,
    preferred_model: String,
    preferred_reasoning: Option<String>,
    capabilities: Vec<String>,
    role_prompt_ref: Option<RolePromptDigest>,
}

impl ExecutionPreference {
    /// Construct a validated CLI execution preference with canonical capability
    /// ordering.
    ///
    /// # Errors
    /// Rejects empty/oversized model or reasoning text, excessive capabilities,
    /// and duplicate capability tags.
    pub fn new(
        cli_engine: CliEngine,
        preferred_model: String,
        preferred_reasoning: Option<String>,
        mut capabilities: Vec<String>,
        role_prompt_ref: Option<RolePromptDigest>,
    ) -> Result<Self, AgentProfileError> {
        validate_text(
            "execution_preferred_model",
            &preferred_model,
            MAX_PROVIDER_PREFERENCE_BYTES,
        )?;
        validate_optional_text(
            "execution_preferred_reasoning",
            preferred_reasoning.as_deref(),
            MAX_PROVIDER_PREFERENCE_BYTES,
        )?;
        if capabilities.len() > MAX_CAPABILITIES {
            return Err(AgentProfileError::TooManyCapabilities {
                actual: capabilities.len(),
                maximum: MAX_CAPABILITIES,
            });
        }
        for capability in &capabilities {
            validate_text("execution_capability", capability, MAX_CAPABILITY_BYTES)?;
        }
        capabilities.sort();
        if capabilities.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(AgentProfileError::DuplicateCapability);
        }
        Ok(Self {
            cli_engine,
            preferred_model,
            preferred_reasoning,
            capabilities,
            role_prompt_ref,
        })
    }

    #[must_use]
    pub const fn cli_engine(&self) -> CliEngine {
        self.cli_engine
    }

    #[must_use]
    pub fn preferred_model(&self) -> &str {
        &self.preferred_model
    }

    #[must_use]
    pub fn preferred_reasoning(&self) -> Option<&str> {
        self.preferred_reasoning.as_deref()
    }

    #[must_use]
    pub fn capabilities(&self) -> &[String] {
        &self.capabilities
    }

    #[must_use]
    pub const fn role_prompt_ref(&self) -> Option<RolePromptDigest> {
        self.role_prompt_ref
    }
}

mod sql;

use sql::{
    DELETE_PROFILE_CAPABILITIES_SQL, DELETE_PROFILE_PREFERENCE_SQL, INSERT_MUTATION_CAPABILITY_SQL,
    INSERT_MUTATION_PREFERENCE_SQL, INSERT_PROFILE_CAPABILITY_SQL, INSERT_PROFILE_PREFERENCE_SQL,
    OBJECTS, SELECT_MUTATION_CAPABILITIES_SQL, SELECT_MUTATION_PREFERENCE_SQL,
    SELECT_PROFILE_CAPABILITIES_SQL, SELECT_PROFILE_PREFERENCE_SQL, SELECT_SCHEMA_SQL,
};

pub(in crate::profile_catalog) const OBJECT_COUNT: i64 = 10;

pub(in crate::profile_catalog) fn create_schema(
    transaction: &Transaction<'_>,
) -> Result<(), AgentProfileError> {
    for (_, _, sql) in OBJECTS {
        transaction.execute_batch(sql)?;
    }
    Ok(())
}

pub(in crate::profile_catalog) fn validate_schema(
    connection: &Connection,
) -> Result<(), AgentProfileError> {
    for (kind, name, expected) in OBJECTS {
        let found: Option<String> = connection
            .query_row(SELECT_SCHEMA_SQL, params![kind, name], |row| row.get(0))
            .optional()?;
        let Some(found) = found else {
            return Err(AgentProfileError::Schema(name));
        };
        if found.len() > 16 * 1024 || normalize_sql(&found) != normalize_sql(expected) {
            return Err(AgentProfileError::Schema(name));
        }
    }
    Ok(())
}

pub(in crate::profile_catalog) fn insert_for_profile(
    transaction: &Transaction<'_>,
    profile_id: AgentProfileId,
    preference: &ExecutionPreference,
) -> Result<(), AgentProfileError> {
    transaction.execute(
        INSERT_PROFILE_PREFERENCE_SQL,
        params![
            profile_id.into_uuid().as_bytes(),
            preference.cli_engine().encode(),
            preference.preferred_model(),
            preference.preferred_reasoning(),
            preference
                .role_prompt_ref()
                .as_ref()
                .map(|digest| digest.as_bytes().as_slice()),
        ],
    )?;
    for capability in preference.capabilities() {
        transaction.execute(
            INSERT_PROFILE_CAPABILITY_SQL,
            params![profile_id.into_uuid().as_bytes(), capability],
        )?;
    }
    Ok(())
}

pub(in crate::profile_catalog) fn replace_for_profile(
    transaction: &Transaction<'_>,
    profile_id: AgentProfileId,
    preference: Option<&ExecutionPreference>,
) -> Result<(), AgentProfileError> {
    transaction.execute(
        DELETE_PROFILE_CAPABILITIES_SQL,
        params![profile_id.into_uuid().as_bytes()],
    )?;
    transaction.execute(
        DELETE_PROFILE_PREFERENCE_SQL,
        params![profile_id.into_uuid().as_bytes()],
    )?;
    if let Some(preference) = preference {
        insert_for_profile(transaction, profile_id, preference)?;
    }
    Ok(())
}

pub(in crate::profile_catalog) fn load_for_profile(
    connection: &Connection,
    profile_id: AgentProfileId,
) -> Result<Option<ExecutionPreference>, AgentProfileError> {
    load_preference_rows(
        connection,
        SELECT_PROFILE_PREFERENCE_SQL,
        SELECT_PROFILE_CAPABILITIES_SQL,
        profile_id.into_uuid().as_bytes(),
    )
}

pub(in crate::profile_catalog) fn insert_for_mutation_receipt(
    transaction: &Transaction<'_>,
    request_id: Uuid,
    preference: &ExecutionPreference,
) -> Result<(), AgentProfileError> {
    transaction.execute(
        INSERT_MUTATION_PREFERENCE_SQL,
        params![
            request_id.as_bytes(),
            preference.cli_engine().encode(),
            preference.preferred_model(),
            preference.preferred_reasoning(),
            preference
                .role_prompt_ref()
                .as_ref()
                .map(|digest| digest.as_bytes().as_slice()),
        ],
    )?;
    for capability in preference.capabilities() {
        transaction.execute(
            INSERT_MUTATION_CAPABILITY_SQL,
            params![request_id.as_bytes(), capability],
        )?;
    }
    Ok(())
}

pub(in crate::profile_catalog) fn load_for_mutation_receipt(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<ExecutionPreference>, AgentProfileError> {
    load_preference_rows(
        connection,
        SELECT_MUTATION_PREFERENCE_SQL,
        SELECT_MUTATION_CAPABILITIES_SQL,
        request_id.as_bytes(),
    )
}

struct RawExecutionPreferenceRow {
    cli_engine: i64,
    preferred_model: String,
    preferred_reasoning: Option<String>,
    role_prompt_digest: Option<Vec<u8>>,
}

fn load_preference_rows(
    connection: &Connection,
    preference_sql: &str,
    capability_sql: &str,
    key: &[u8],
) -> Result<Option<ExecutionPreference>, AgentProfileError> {
    let raw: Option<RawExecutionPreferenceRow> = connection
        .query_row(preference_sql, params![key], |row| {
            Ok(RawExecutionPreferenceRow {
                cli_engine: row.get(0)?,
                preferred_model: row.get(1)?,
                preferred_reasoning: row.get(2)?,
                role_prompt_digest: row.get(3)?,
            })
        })
        .optional()?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    let cli_engine = CliEngine::decode(raw.cli_engine)?;
    let role_prompt_ref = raw
        .role_prompt_digest
        .as_deref()
        .map(|bytes| {
            RolePromptDigest::from_slice(bytes)
                .map_err(|_| AgentProfileError::Corrupt("invalid role_prompt_digest"))
        })
        .transpose()?;
    let mut capabilities = Vec::new();
    let mut statement = connection.prepare(capability_sql)?;
    let mut rows = statement.query(params![key])?;
    while let Some(row) = rows.next()? {
        if capabilities.len() >= MAX_CAPABILITIES {
            return Err(AgentProfileError::Corrupt(
                "Profile execution capability count exceeded bound",
            ));
        }
        let capability: String = row.get(0)?;
        validate_text("execution_capability", &capability, MAX_CAPABILITY_BYTES)?;
        if capabilities.last() == Some(&capability) {
            return Err(AgentProfileError::Corrupt(
                "duplicate Profile execution capability",
            ));
        }
        capabilities.push(capability);
    }
    ExecutionPreference::new(
        cli_engine,
        raw.preferred_model,
        raw.preferred_reasoning,
        capabilities,
        role_prompt_ref,
    )
    .map(Some)
}
