//! Historical operator receipts for unobservable `ProvisionNew` effects.
//!
//! New uncertainty-retirement writes are disabled; persisted receipts remain
//! available as exact readback beside their original `EffectUnknown` rows.

use forge_domain::{AgentSessionId, AuthenticatedPrincipal, TimestampMillis, WorkspaceId};
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    AgentProvisionEffectKind, AgentProvisionOperation, AgentProvisionState, load_operation,
    next_revision,
};
use crate::{OperationsCatalog, OperationsError, wire};

const RETIREMENT_INTENT_DOMAIN: &[u8] =
    b"heterarchy.orchestration.agent-provision.uncertainty-retirement.v1\0";
const ACTIVE_RECOVERY_VERSION: u8 = 1;

const UNCERTAINTY_RETIREMENTS_SQL: &str = r"
CREATE TABLE ops_agent_provision_uncertainty_retirements (
    operation_id BLOB PRIMARY KEY NOT NULL CHECK(length(operation_id)=16 AND operation_id != zeroblob(16)),
    retirement_id BLOB NOT NULL UNIQUE CHECK(length(retirement_id)=16 AND retirement_id != zeroblob(16)),
    expected_unknown_revision INTEGER NOT NULL CHECK(expected_unknown_revision>=1),
    retired_revision INTEGER NOT NULL CHECK(retired_revision=expected_unknown_revision+1),
    workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16 AND workspace_id != zeroblob(16)),
    requester_session_id BLOB NOT NULL CHECK(length(requester_session_id)=16 AND requester_session_id != zeroblob(16)),
    actor_principal_kind INTEGER NOT NULL CHECK(actor_principal_kind IN(0,1,2)),
    actor_principal_id BLOB CHECK(actor_principal_id IS NULL OR (length(actor_principal_id)=16 AND actor_principal_id != zeroblob(16))),
    recovery_version INTEGER NOT NULL CHECK(recovery_version=1),
    retired_at_ms INTEGER NOT NULL CHECK(retired_at_ms>=0),
    intent_digest BLOB NOT NULL CHECK(length(intent_digest)=32),
    FOREIGN KEY(operation_id) REFERENCES ops_agent_provision_operations(operation_id),
    CHECK(
        (actor_principal_kind=0 AND actor_principal_id IS NULL)
        OR (actor_principal_kind IN(1,2) AND actor_principal_id IS NOT NULL)
    )
);
";

/// Exact authenticated intent to retire recovery for one unknown provider effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentProvisionUncertaintyRetirementTarget {
    operation_id: Uuid,
    expected_unknown_revision: u64,
    workspace_id: WorkspaceId,
    requester_session_id: AgentSessionId,
}

impl AgentProvisionUncertaintyRetirementTarget {
    /// Bind retirement to one exact unknown-operation and request scope.
    ///
    /// # Errors
    /// Rejects nil identities and zero or unrepresentable revisions.
    pub fn new(
        operation_id: Uuid,
        expected_unknown_revision: u64,
        workspace_id: WorkspaceId,
        requester_session_id: AgentSessionId,
    ) -> Result<Self, OperationsError> {
        if operation_id.is_nil()
            || workspace_id.into_uuid().is_nil()
            || requester_session_id.into_uuid().is_nil()
            || expected_unknown_revision == 0
            || i64::try_from(expected_unknown_revision).is_err()
        {
            return Err(OperationsError::Conflict(
                "Agent provision uncertainty retirement target",
            ));
        }
        Ok(Self {
            operation_id,
            expected_unknown_revision,
            workspace_id,
            requester_session_id,
        })
    }

    #[must_use]
    pub const fn operation_id(self) -> Uuid {
        self.operation_id
    }

    #[must_use]
    pub const fn expected_unknown_revision(self) -> u64 {
        self.expected_unknown_revision
    }

    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        self.workspace_id
    }

    pub const fn requester_session_id(self) -> AgentSessionId {
        self.requester_session_id
    }
}

/// Exact authenticated intent to retire recovery for one unknown provider effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentProvisionUncertaintyRetirementCommand {
    retirement_id: Uuid,
    target: AgentProvisionUncertaintyRetirementTarget,
    actor_principal: AuthenticatedPrincipal,
    recovery_version: u8,
    retired_at: TimestampMillis,
}

impl AgentProvisionUncertaintyRetirementCommand {
    /// Construct the frozen v1 recovery-retirement intent.
    ///
    /// # Errors
    /// Rejects nil identities, zero/unrepresentable revisions, and versions
    /// other than the only active recovery protocol.
    pub fn new(
        retirement_id: Uuid,
        target: AgentProvisionUncertaintyRetirementTarget,
        actor_principal: AuthenticatedPrincipal,
        recovery_version: u8,
        retired_at: TimestampMillis,
    ) -> Result<Self, OperationsError> {
        if retirement_id.is_nil() || recovery_version != ACTIVE_RECOVERY_VERSION {
            return Err(OperationsError::Conflict(
                "Agent provision uncertainty retirement command",
            ));
        }
        Ok(Self {
            retirement_id,
            target,
            actor_principal,
            recovery_version,
            retired_at,
        })
    }

    #[must_use]
    pub const fn retirement_id(self) -> Uuid {
        self.retirement_id
    }

    #[must_use]
    pub const fn operation_id(self) -> Uuid {
        self.target.operation_id
    }

    #[must_use]
    pub const fn expected_unknown_revision(self) -> u64 {
        self.target.expected_unknown_revision
    }

    #[must_use]
    pub const fn workspace_id(self) -> WorkspaceId {
        self.target.workspace_id
    }

    pub const fn requester_session_id(self) -> AgentSessionId {
        self.target.requester_session_id
    }

    pub const fn actor_principal(self) -> AuthenticatedPrincipal {
        self.actor_principal
    }

    #[must_use]
    pub const fn recovery_version(self) -> u8 {
        self.recovery_version
    }

    #[must_use]
    pub const fn retired_at(self) -> TimestampMillis {
        self.retired_at
    }
}

/// Durable evidence that recovery was retired while effect truth stayed unknown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProvisionUncertaintyRetirement {
    retirement_id: Uuid,
    operation_id: Uuid,
    expected_unknown_revision: u64,
    retired_revision: u64,
    workspace_id: WorkspaceId,
    requester_session_id: AgentSessionId,
    actor_principal: AuthenticatedPrincipal,
    recovery_version: u8,
    retired_at: TimestampMillis,
    intent_digest: [u8; 32],
}

impl AgentProvisionUncertaintyRetirement {
    #[must_use]
    pub const fn retirement_id(&self) -> Uuid {
        self.retirement_id
    }

    #[must_use]
    pub const fn operation_id(&self) -> Uuid {
        self.operation_id
    }

    #[must_use]
    pub const fn expected_unknown_revision(&self) -> u64 {
        self.expected_unknown_revision
    }

    #[must_use]
    pub const fn retired_revision(&self) -> u64 {
        self.retired_revision
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    pub const fn requester_session_id(&self) -> AgentSessionId {
        self.requester_session_id
    }

    pub const fn actor_principal(&self) -> AuthenticatedPrincipal {
        self.actor_principal
    }

    #[must_use]
    pub const fn recovery_version(&self) -> u8 {
        self.recovery_version
    }

    #[must_use]
    pub const fn retired_at(&self) -> TimestampMillis {
        self.retired_at
    }

    #[must_use]
    pub const fn intent_digest(&self) -> [u8; 32] {
        self.intent_digest
    }
}

/// Idempotent retirement result. `created=false` is an exact intent replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProvisionUncertaintyRetirementOutcome {
    retirement: AgentProvisionUncertaintyRetirement,
    created: bool,
}

impl AgentProvisionUncertaintyRetirementOutcome {
    #[must_use]
    pub const fn retirement(&self) -> &AgentProvisionUncertaintyRetirement {
        &self.retirement
    }

    #[must_use]
    pub const fn created(&self) -> bool {
        self.created
    }
}

impl OperationsCatalog {
    /// Retired provider uncertainty-retirement entrypoint.
    ///
    /// # Errors
    /// Always returns [`OperationsError::ProviderRetired`] without reading or
    /// writing the catalog.
    pub fn retire_agent_provision_uncertainty(
        &mut self,
        command: AgentProvisionUncertaintyRetirementCommand,
    ) -> Result<AgentProvisionUncertaintyRetirementOutcome, OperationsError> {
        let _ = command;
        Err(OperationsError::ProviderRetired)
    }

    /// Read the exact retirement receipt for one operation after a lost response.
    ///
    /// # Errors
    /// Rejects nil identity, corrupt rows, and storage failures.
    pub fn agent_provision_uncertainty_retirement(
        &self,
        operation_id: Uuid,
    ) -> Result<Option<AgentProvisionUncertaintyRetirement>, OperationsError> {
        if operation_id.is_nil() {
            return Err(OperationsError::Conflict(
                "Agent provision uncertainty retirement identity",
            ));
        }
        Ok(load_operation(&self.connection, operation_id)?
            .and_then(|operation| operation.uncertainty_retirement().cloned()))
    }
}

pub(super) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    connection.execute_batch(UNCERTAINTY_RETIREMENTS_SQL)?;
    Ok(())
}

pub(super) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![(
        "table",
        "ops_agent_provision_uncertainty_retirements",
        UNCERTAINTY_RETIREMENTS_SQL,
    )]
}

pub(super) fn load_for_operation(
    connection: &Connection,
    operation_id: Uuid,
) -> Result<Option<AgentProvisionUncertaintyRetirement>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT substr(retirement_id,1,17),expected_unknown_revision,retired_revision,substr(workspace_id,1,17),substr(requester_session_id,1,17),actor_principal_kind,substr(actor_principal_id,1,17),recovery_version,retired_at_ms,substr(intent_digest,1,33) FROM ops_agent_provision_uncertainty_retirements WHERE operation_id=?1",
            params![operation_id.as_bytes()],
            |row| {
                Ok(RawRetirement {
                    retirement_id: row.get(0)?,
                    expected_unknown_revision: row.get(1)?,
                    retired_revision: row.get(2)?,
                    workspace_id: row.get(3)?,
                    requester_session_id: row.get(4)?,
                    actor_principal_kind: row.get(5)?,
                    actor_principal_id: row.get(6)?,
                    recovery_version: row.get(7)?,
                    retired_at: row.get(8)?,
                    intent_digest: row.get(9)?,
                })
            },
        )
        .optional()?;
    raw.map(|raw| decode_retirement(operation_id, raw))
        .transpose()
}
struct RawRetirement {
    retirement_id: Vec<u8>,
    expected_unknown_revision: i64,
    retired_revision: i64,
    workspace_id: Vec<u8>,
    requester_session_id: Vec<u8>,
    actor_principal_kind: i64,
    actor_principal_id: Option<Vec<u8>>,
    recovery_version: i64,
    retired_at: i64,
    intent_digest: Vec<u8>,
}

fn decode_retirement(
    operation_id: Uuid,
    raw: RawRetirement,
) -> Result<AgentProvisionUncertaintyRetirement, OperationsError> {
    let expected_unknown_revision = decode_revision(
        raw.expected_unknown_revision,
        "Agent provision uncertainty expected revision",
    )?;
    let retired_revision = decode_revision(
        raw.retired_revision,
        "Agent provision uncertainty retired revision",
    )?;
    if retired_revision != next_revision(expected_unknown_revision)? {
        return Err(OperationsError::Corrupt(
            "Agent provision uncertainty retirement revision",
        ));
    }
    let recovery_version = u8::try_from(raw.recovery_version)
        .map_err(|_| OperationsError::Corrupt("Agent provision uncertainty recovery version"))?;
    if recovery_version != ACTIVE_RECOVERY_VERSION {
        return Err(OperationsError::Corrupt(
            "Agent provision uncertainty recovery version",
        ));
    }
    let retired_at = TimestampMillis::new(raw.retired_at)
        .map_err(|_| OperationsError::Corrupt("Agent provision uncertainty retirement time"))?;
    let intent_digest: [u8; 32] = raw
        .intent_digest
        .try_into()
        .map_err(|_| OperationsError::Corrupt("Agent provision uncertainty intent digest"))?;
    let retirement = AgentProvisionUncertaintyRetirement {
        retirement_id: Uuid::from_bytes(wire::id(raw.retirement_id)?),
        operation_id,
        expected_unknown_revision,
        retired_revision,
        workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(raw.workspace_id)?)),
        requester_session_id: AgentSessionId::from_uuid(Uuid::from_bytes(wire::id(
            raw.requester_session_id,
        )?)),
        actor_principal: crate::registration::decode_principal_fields(
            raw.actor_principal_kind,
            raw.actor_principal_id.as_deref(),
        )?,
        recovery_version,
        retired_at,
        intent_digest,
    };
    let reconstructed = AgentProvisionUncertaintyRetirementCommand::new(
        retirement.retirement_id,
        AgentProvisionUncertaintyRetirementTarget::new(
            retirement.operation_id,
            retirement.expected_unknown_revision,
            retirement.workspace_id,
            retirement.requester_session_id,
        )?,
        retirement.actor_principal,
        retirement.recovery_version,
        retirement.retired_at,
    )?;
    if retirement_intent_digest(reconstructed) != retirement.intent_digest {
        return Err(OperationsError::Corrupt(
            "Agent provision uncertainty intent digest",
        ));
    }
    Ok(retirement)
}

pub(super) fn validate_operation_projection(
    operation: &AgentProvisionOperation,
) -> Result<(), OperationsError> {
    let Some(retirement) = operation.uncertainty_retirement() else {
        return Ok(());
    };
    if operation.effect_kind() != AgentProvisionEffectKind::ProvisionNew
        || operation.state() != AgentProvisionState::EffectUnknown
        || operation.revision() != retirement.retired_revision
        || operation.updated_at() != retirement.retired_at
    {
        return Err(OperationsError::Corrupt(
            "Agent provision uncertainty operation projection",
        ));
    }
    Ok(())
}

fn decode_revision(value: i64, label: &'static str) -> Result<u64, OperationsError> {
    let revision = u64::try_from(value).map_err(|_| OperationsError::Corrupt(label))?;
    if revision == 0 {
        return Err(OperationsError::Corrupt(label));
    }
    Ok(revision)
}
fn retirement_intent_digest(command: AgentProvisionUncertaintyRetirementCommand) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(RETIREMENT_INTENT_DOMAIN);
    digest.update(command.retirement_id.as_bytes());
    digest.update(command.operation_id().as_bytes());
    digest.update(command.expected_unknown_revision().to_be_bytes());
    digest.update(command.workspace_id().into_uuid().as_bytes());
    digest.update(command.requester_session_id().into_uuid().as_bytes());
    match command.actor_principal {
        AuthenticatedPrincipal::TrustedStdio => digest.update([0]),
        AuthenticatedPrincipal::DirectToken(id) => {
            digest.update([1]);
            digest.update(id.into_uuid().as_bytes());
        }
        AuthenticatedPrincipal::OAuthGrant(id) => {
            digest.update([2]);
            digest.update(id.into_uuid().as_bytes());
        }
    }
    digest.update([command.recovery_version]);
    digest.finalize().into()
}
