use forge_domain::TimestampMillis;
use uuid::Uuid;

use super::{
    AgentProfile, AgentProfileError, AgentProfileId, AgentProfileRevision, MAX_CAPABILITIES,
    MAX_CAPABILITY_BYTES, MAX_PROFILE_NAME_BYTES, MAX_PROVIDER_PREFERENCE_BYTES,
    validate_optional_text, validate_text,
};

/// Validated mutable metadata for one global Agent Profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileConfiguration {
    display_name: String,
    preferred_model: Option<String>,
    preferred_reasoning: Option<String>,
    capabilities: Vec<String>,
}

impl ProfileConfiguration {
    /// Build a configuration with canonical capability ordering.
    ///
    /// # Errors
    /// Rejects empty or oversized text, too many capabilities, and duplicate
    /// capability tags.
    pub fn new(
        display_name: String,
        preferred_model: Option<String>,
        preferred_reasoning: Option<String>,
        mut capabilities: Vec<String>,
    ) -> Result<Self, AgentProfileError> {
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
        if capabilities.len() > MAX_CAPABILITIES {
            return Err(AgentProfileError::TooManyCapabilities {
                actual: capabilities.len(),
                maximum: MAX_CAPABILITIES,
            });
        }
        for capability in &capabilities {
            validate_text("capability", capability, MAX_CAPABILITY_BYTES)?;
        }
        capabilities.sort();
        if capabilities.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(AgentProfileError::DuplicateCapability);
        }
        Ok(Self {
            display_name,
            preferred_model,
            preferred_reasoning,
            capabilities,
        })
    }

    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
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
    pub fn capabilities(&self) -> &[String] {
        &self.capabilities
    }
}

/// One supported Profile lifecycle/configuration mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfileMutation {
    Update(ProfileConfiguration),
    Disable,
    Retire,
}

impl ProfileMutation {
    #[must_use]
    pub const fn kind(&self) -> ProfileMutationKind {
        match self {
            Self::Update(_) => ProfileMutationKind::Update,
            Self::Disable => ProfileMutationKind::Disable,
            Self::Retire => ProfileMutationKind::Retire,
        }
    }
}

/// Immutable operation kind recorded in one Profile mutation receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileMutationKind {
    OriginalCreate,
    Update,
    Disable,
    Retire,
}

impl ProfileMutationKind {
    pub(super) const fn encode(self) -> i64 {
        match self {
            Self::OriginalCreate => 1,
            Self::Update => 2,
            Self::Disable => 3,
            Self::Retire => 4,
        }
    }

    pub(super) fn decode(value: i64) -> Result<Self, AgentProfileError> {
        match value {
            1 => Ok(Self::OriginalCreate),
            2 => Ok(Self::Update),
            3 => Ok(Self::Disable),
            4 => Ok(Self::Retire),
            _ => Err(AgentProfileError::Corrupt("invalid Profile mutation kind")),
        }
    }
}

/// Exact optimistic-concurrency command for one Profile mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileMutationCommand {
    request_id: Uuid,
    profile_id: AgentProfileId,
    expected_revision: AgentProfileRevision,
    operation: ProfileMutation,
    observed_at: TimestampMillis,
}

impl ProfileMutationCommand {
    /// Build one validated mutation command.
    ///
    /// # Errors
    /// Rejects a nil mutation request identity.
    pub fn new(
        request_id: Uuid,
        profile_id: AgentProfileId,
        expected_revision: AgentProfileRevision,
        operation: ProfileMutation,
        observed_at: TimestampMillis,
    ) -> Result<Self, AgentProfileError> {
        if request_id.is_nil() {
            return Err(AgentProfileError::InvalidIdentity(
                "profile_mutation_request_id",
            ));
        }
        Ok(Self {
            request_id,
            profile_id,
            expected_revision,
            operation,
            observed_at,
        })
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
    pub const fn expected_revision(&self) -> AgentProfileRevision {
        self.expected_revision
    }

    #[must_use]
    pub fn operation(&self) -> &ProfileMutation {
        &self.operation
    }

    #[must_use]
    pub const fn kind(&self) -> ProfileMutationKind {
        self.operation.kind()
    }

    #[must_use]
    pub const fn observed_at(&self) -> TimestampMillis {
        self.observed_at
    }
}

/// Immutable snapshot of one committed Profile mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileMutationReceipt {
    request_id: Uuid,
    profile: AgentProfile,
    expected_revision: Option<AgentProfileRevision>,
    kind: ProfileMutationKind,
    replayed: bool,
}

impl ProfileMutationReceipt {
    pub(super) const fn committed(
        request_id: Uuid,
        profile: AgentProfile,
        expected_revision: Option<AgentProfileRevision>,
        kind: ProfileMutationKind,
        replayed: bool,
    ) -> Self {
        Self {
            request_id,
            profile,
            expected_revision,
            kind,
            replayed,
        }
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn profile_id(&self) -> AgentProfileId {
        self.profile.id()
    }

    #[must_use]
    pub fn profile(&self) -> &AgentProfile {
        &self.profile
    }

    #[must_use]
    pub const fn expected_revision(&self) -> Option<AgentProfileRevision> {
        self.expected_revision
    }

    #[must_use]
    pub const fn kind(&self) -> ProfileMutationKind {
        self.kind
    }

    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}
