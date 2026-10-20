//! Immutable operator acknowledgement for one uncertain provider message.

use forge_domain::{AuthenticatedPrincipal, TimestampMillis, WorkspaceId};
use heterarchy_orchestration_sessions::{AgentProfileId, AgentProfileRuntimeSessionId};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use uuid::Uuid;

use crate::catalog::signed;
use crate::{
    ChatGptProjectId, OperationsCatalog, OperationsError, WorkspaceParticipationId,
    WorkspaceProjectBindingRevision, WorkspaceProjectReference, registration,
};

use super::{
    ProviderMessageState, ProviderMessageTarget, catalog_commit_read, decode_counter,
    decode_provider_family, decode_string, decode_uuid, encode_provider_family, load_outbox,
    require_verified_workspace, to_sqlite_error, validate_message_id,
};

const RECEIPTS_SQL: &str = r"
CREATE TABLE ops_provider_message_unknown_isolations (
    request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16 AND request_id!=zeroblob(16)),
    message_id BLOB NOT NULL UNIQUE REFERENCES ops_provider_message_outbox(message_id) CHECK(length(message_id)=16 AND message_id!=zeroblob(16)),
    expected_revision INTEGER NOT NULL CHECK(expected_revision>=1),
    expected_workspace_id BLOB NOT NULL CHECK(length(expected_workspace_id)=16 AND expected_workspace_id!=zeroblob(16)),
    expected_profile_id BLOB NOT NULL CHECK(length(expected_profile_id)=16 AND expected_profile_id!=zeroblob(16)),
    expected_participation_id BLOB NOT NULL CHECK(length(expected_participation_id)=16 AND expected_participation_id!=zeroblob(16)),
    expected_provider_family INTEGER NOT NULL CHECK(expected_provider_family=1),
    expected_runtime_session_id BLOB NOT NULL CHECK(length(expected_runtime_session_id)=16 AND expected_runtime_session_id!=zeroblob(16)),
    expected_provider_conversation_id TEXT NOT NULL CHECK(length(CAST(expected_provider_conversation_id AS BLOB)) BETWEEN 1 AND 512),
    expected_project_id TEXT CHECK(expected_project_id IS NULL OR (length(CAST(expected_project_id AS BLOB))=36 AND substr(expected_project_id,1,4)='g-p-' AND substr(expected_project_id,5,32) NOT GLOB '*[^0-9a-f]*')),
    expected_project_binding_revision INTEGER CHECK(expected_project_binding_revision IS NULL OR expected_project_binding_revision>=1),
    reason INTEGER NOT NULL CHECK(reason=0),
    operator_principal_kind INTEGER NOT NULL CHECK(operator_principal_kind IN(0,1,2)),
    operator_principal_id BLOB CHECK(operator_principal_id IS NULL OR (length(operator_principal_id)=16 AND operator_principal_id!=zeroblob(16))),
    acknowledged_at_ms INTEGER NOT NULL CHECK(acknowledged_at_ms>=0),
    CHECK((expected_project_id IS NULL AND expected_project_binding_revision IS NULL) OR (expected_project_id IS NOT NULL AND expected_project_binding_revision IS NOT NULL)),
    CHECK((operator_principal_kind=0 AND operator_principal_id IS NULL) OR (operator_principal_kind IN(1,2) AND operator_principal_id IS NOT NULL))
) STRICT;
";

const RECEIPT_NO_UPDATE_TRIGGER: &str = r"
CREATE TRIGGER ops_provider_message_unknown_isolations_no_update
BEFORE UPDATE ON ops_provider_message_unknown_isolations
BEGIN
    SELECT RAISE(ABORT, 'provider UNKNOWN isolation receipts are immutable');
END;
";

const RECEIPT_NO_DELETE_TRIGGER: &str = r"
CREATE TRIGGER ops_provider_message_unknown_isolations_no_delete
BEFORE DELETE ON ops_provider_message_unknown_isolations
BEGIN
    SELECT RAISE(ABORT, 'provider UNKNOWN isolation receipts are immutable');
END;
";

const SELECT_RECEIPT: &str = "SELECT substr(request_id,1,17),substr(message_id,1,17),expected_revision,substr(expected_workspace_id,1,17),substr(expected_profile_id,1,17),substr(expected_participation_id,1,17),expected_provider_family,substr(expected_runtime_session_id,1,17),substr(CAST(expected_provider_conversation_id AS BLOB),1,513),expected_project_id,expected_project_binding_revision,reason,operator_principal_kind,substr(operator_principal_id,1,17),acknowledged_at_ms FROM ops_provider_message_unknown_isolations";

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        (
            "table",
            "ops_provider_message_unknown_isolations",
            RECEIPTS_SQL,
        ),
        (
            "trigger",
            "ops_provider_message_unknown_isolations_no_update",
            RECEIPT_NO_UPDATE_TRIGGER,
        ),
        (
            "trigger",
            "ops_provider_message_unknown_isolations_no_delete",
            RECEIPT_NO_DELETE_TRIGGER,
        ),
    ]
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in schema_objects() {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

/// The only acknowledged reason for isolating a message whose provider result is unknown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderMessageUnknownIsolationReason {
    OperatorAcknowledgedUnknown,
}

/// Exact immutable request to preserve one UNKNOWN message while isolating its runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderMessageUnknownIsolationCommand {
    request_id: Uuid,
    message_id: Uuid,
    expected_revision: u64,
    expected_target: ProviderMessageTarget,
    reason: ProviderMessageUnknownIsolationReason,
    principal: AuthenticatedPrincipal,
    acknowledged_at: TimestampMillis,
}

impl ProviderMessageUnknownIsolationCommand {
    /// Construct one bounded, exact operator acknowledgement.
    ///
    /// # Errors
    /// Rejects nil request/message identities and a zero revision.
    pub fn new(
        request_id: Uuid,
        message_id: Uuid,
        expected_revision: u64,
        expected_target: ProviderMessageTarget,
        reason: ProviderMessageUnknownIsolationReason,
        principal: AuthenticatedPrincipal,
        acknowledged_at: TimestampMillis,
    ) -> Result<Self, OperationsError> {
        if request_id.is_nil() || message_id.is_nil() || expected_revision == 0 {
            return Err(OperationsError::Conflict(
                "provider UNKNOWN isolation identity",
            ));
        }
        Ok(Self {
            request_id,
            message_id,
            expected_revision,
            expected_target,
            reason,
            principal,
            acknowledged_at,
        })
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn message_id(&self) -> Uuid {
        self.message_id
    }

    #[must_use]
    pub const fn expected_revision(&self) -> u64 {
        self.expected_revision
    }

    #[must_use]
    pub fn expected_target(&self) -> &ProviderMessageTarget {
        &self.expected_target
    }

    #[must_use]
    pub const fn reason(&self) -> ProviderMessageUnknownIsolationReason {
        self.reason
    }

    pub const fn principal(&self) -> AuthenticatedPrincipal {
        self.principal
    }
}

/// Durable immutable acknowledgement, including the original request and operator identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderMessageUnknownIsolationReceipt {
    request_id: Uuid,
    message_id: Uuid,
    expected_revision: u64,
    expected_target: ProviderMessageTarget,
    reason: ProviderMessageUnknownIsolationReason,
    principal: AuthenticatedPrincipal,
    acknowledged_at: TimestampMillis,
}

impl ProviderMessageUnknownIsolationReceipt {
    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn message_id(&self) -> Uuid {
        self.message_id
    }

    #[must_use]
    pub const fn expected_revision(&self) -> u64 {
        self.expected_revision
    }

    #[must_use]
    pub fn expected_target(&self) -> &ProviderMessageTarget {
        &self.expected_target
    }

    #[must_use]
    pub const fn reason(&self) -> ProviderMessageUnknownIsolationReason {
        self.reason
    }

    pub const fn principal(&self) -> AuthenticatedPrincipal {
        self.principal
    }

    #[must_use]
    pub const fn acknowledged_at(&self) -> TimestampMillis {
        self.acknowledged_at
    }

    fn has_same_request(&self, command: &ProviderMessageUnknownIsolationCommand) -> bool {
        self.request_id == command.request_id
            && self.message_id == command.message_id
            && self.expected_revision == command.expected_revision
            && self.expected_target == command.expected_target
            && self.reason == command.reason
    }
}

struct RawReceipt {
    request_id: Vec<u8>,
    message_id: Vec<u8>,
    expected_revision: i64,
    expected_workspace_id: Vec<u8>,
    expected_profile_id: Vec<u8>,
    expected_participation_id: Vec<u8>,
    expected_provider_family: i64,
    expected_runtime_session_id: Vec<u8>,
    expected_provider_conversation_id: Vec<u8>,
    expected_project_id: Option<String>,
    expected_project_binding_revision: Option<i64>,
    reason: i64,
    operator_principal_kind: i64,
    operator_principal_id: Option<Vec<u8>>,
    acknowledged_at_ms: i64,
}

impl OperationsCatalog {
    /// Persist one exact operator acknowledgement without changing the UNKNOWN outbox row.
    ///
    /// # Errors
    /// Rejects stale or changed message evidence, duplicate identities, invalid scope or storage failures.
    pub fn isolate_unknown_provider_message(
        &mut self,
        command: &ProviderMessageUnknownIsolationCommand,
    ) -> Result<ProviderMessageUnknownIsolationReceipt, OperationsError> {
        validate_message_id(command.message_id)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;

        if let Some(existing) = load_by_request(&transaction, command.request_id)? {
            if !existing.has_same_request(command) {
                return Err(OperationsError::Conflict(
                    "provider UNKNOWN isolation request identity",
                ));
            }
            require_receipt_target(&transaction, &existing, self.destination)?;
            catalog_commit_read(transaction, self.destination)?;
            return Ok(existing);
        }
        if load_by_message(&transaction, command.message_id)?.is_some() {
            return Err(OperationsError::Conflict(
                "provider UNKNOWN isolation already exists",
            ));
        }

        let existing = load_outbox(&transaction, command.message_id)?.ok_or(
            OperationsError::Conflict("provider UNKNOWN isolation message"),
        )?;
        require_verified_workspace(
            &transaction,
            existing.target().workspace_id(),
            self.destination,
        )?;
        if existing.state() != ProviderMessageState::Unknown
            || existing.revision() != command.expected_revision
            || existing.target() != &command.expected_target
            || existing.receipt().is_some()
            || command.acknowledged_at < existing.updated_at()
        {
            return Err(OperationsError::Conflict(
                "provider UNKNOWN isolation fence",
            ));
        }

        let (principal_kind, principal_id) = registration::encode_principal(command.principal);
        let target = &command.expected_target;
        let (project_id, project_binding_revision) =
            target.project().map_or((None, None), |project| {
                (
                    Some(project.project_id().to_string()),
                    Some(project.binding_revision().as_i64()),
                )
            });
        transaction.execute(
            "INSERT INTO ops_provider_message_unknown_isolations(request_id,message_id,expected_revision,expected_workspace_id,expected_profile_id,expected_participation_id,expected_provider_family,expected_runtime_session_id,expected_provider_conversation_id,expected_project_id,expected_project_binding_revision,reason,operator_principal_kind,operator_principal_id,acknowledged_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,0,?12,?13,?14)",
            params![
                command.request_id.as_bytes(),
                command.message_id.as_bytes(),
                signed(command.expected_revision)?,
                target.workspace_id().into_uuid().as_bytes(),
                target.profile_id().into_uuid().as_bytes(),
                target.participation_id().into_uuid().as_bytes(),
                encode_provider_family(target.provider_family()),
                target.runtime_session_id().into_uuid().as_bytes(),
                target.provider_conversation_id(),
                project_id,
                project_binding_revision,
                principal_kind,
                principal_id.as_deref(),
                command.acknowledged_at.get(),
            ],
        )?;
        let receipt = load_by_request(&transaction, command.request_id)?.ok_or(
            OperationsError::Corrupt("provider UNKNOWN isolation insert"),
        )?;
        let workspace_id = target.workspace_id();
        transaction
            .commit()
            .map_err(|source| OperationsError::OperationCommitUnconfirmed {
                workspace_id,
                request_id: command.request_id,
                source,
            })?;
        Ok(receipt)
    }

    /// Read an isolation receipt by exact request ID or discover it by exact message ID.
    ///
    /// # Errors
    /// Rejects nil identities, mismatched request/message pairs, unverified Workspaces or corrupt rows.
    pub fn provider_message_unknown_isolation(
        &self,
        message_id: Uuid,
        request_id: Option<Uuid>,
    ) -> Result<Option<ProviderMessageUnknownIsolationReceipt>, OperationsError> {
        validate_message_id(message_id)?;
        if request_id.is_some_and(|id| id.is_nil()) {
            return Err(OperationsError::Conflict(
                "provider UNKNOWN isolation request identity",
            ));
        }
        let transaction = self.connection.unchecked_transaction()?;
        let receipt = match request_id {
            Some(request_id) => load_by_request(&transaction, request_id)?,
            None => load_by_message(&transaction, message_id)?,
        };
        if let Some(receipt) = receipt.as_ref() {
            if receipt.message_id != message_id {
                return Err(OperationsError::Conflict(
                    "provider UNKNOWN isolation message identity",
                ));
            }
            require_receipt_target(&transaction, receipt, self.destination)?;
        }
        catalog_commit_read(transaction, self.destination)?;
        Ok(receipt)
    }
}

fn require_receipt_target(
    connection: &Connection,
    receipt: &ProviderMessageUnknownIsolationReceipt,
    destination: Uuid,
) -> Result<(), OperationsError> {
    let outbox = load_outbox(connection, receipt.message_id)?.ok_or(OperationsError::Corrupt(
        "provider UNKNOWN isolation message",
    ))?;
    if outbox.target() != &receipt.expected_target {
        return Err(OperationsError::Corrupt(
            "provider UNKNOWN isolation target",
        ));
    }
    require_verified_workspace(
        connection,
        receipt.expected_target.workspace_id(),
        destination,
    )
}

fn load_by_request(
    connection: &Connection,
    request_id: Uuid,
) -> Result<Option<ProviderMessageUnknownIsolationReceipt>, OperationsError> {
    if request_id.is_nil() {
        return Err(OperationsError::Conflict(
            "provider UNKNOWN isolation request identity",
        ));
    }
    connection
        .query_row(
            &format!("{SELECT_RECEIPT} WHERE request_id=?1"),
            params![request_id.as_bytes()],
            decode_receipt,
        )
        .optional()
        .map_err(OperationsError::from)
}

fn load_by_message(
    connection: &Connection,
    message_id: Uuid,
) -> Result<Option<ProviderMessageUnknownIsolationReceipt>, OperationsError> {
    connection
        .query_row(
            &format!("{SELECT_RECEIPT} WHERE message_id=?1"),
            params![message_id.as_bytes()],
            decode_receipt,
        )
        .optional()
        .map_err(OperationsError::from)
}

fn decode_receipt(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ProviderMessageUnknownIsolationReceipt> {
    let raw = RawReceipt {
        request_id: row.get(0)?,
        message_id: row.get(1)?,
        expected_revision: row.get(2)?,
        expected_workspace_id: row.get(3)?,
        expected_profile_id: row.get(4)?,
        expected_participation_id: row.get(5)?,
        expected_provider_family: row.get(6)?,
        expected_runtime_session_id: row.get(7)?,
        expected_provider_conversation_id: row.get(8)?,
        expected_project_id: row.get(9)?,
        expected_project_binding_revision: row.get(10)?,
        reason: row.get(11)?,
        operator_principal_kind: row.get(12)?,
        operator_principal_id: row.get(13)?,
        acknowledged_at_ms: row.get(14)?,
    };
    let expected_project = match (
        raw.expected_project_id,
        raw.expected_project_binding_revision,
    ) {
        (None, None) => None,
        (Some(project_id), Some(revision)) => Some(WorkspaceProjectReference::new(
            ChatGptProjectId::try_from(project_id.as_str()).map_err(to_sqlite_error)?,
            WorkspaceProjectBindingRevision::new(revision).map_err(to_sqlite_error)?,
        )),
        _ => {
            return Err(to_sqlite_error(OperationsError::Corrupt(
                "provider UNKNOWN isolation Project route",
            )));
        }
    };
    let expected_target = ProviderMessageTarget::new_with_project_route(
        WorkspaceId::from_uuid(decode_uuid(
            raw.expected_workspace_id,
            "provider UNKNOWN isolation Workspace",
        )?),
        AgentProfileId::from_uuid(decode_uuid(
            raw.expected_profile_id,
            "provider UNKNOWN isolation Profile",
        )?)
        .map_err(|_| {
            to_sqlite_error(OperationsError::Corrupt(
                "provider UNKNOWN isolation Profile",
            ))
        })?,
        WorkspaceParticipationId::from_uuid(decode_uuid(
            raw.expected_participation_id,
            "provider UNKNOWN isolation participation",
        )?)
        .map_err(to_sqlite_error)?,
        decode_provider_family(raw.expected_provider_family)?,
        AgentProfileRuntimeSessionId::from_uuid(decode_uuid(
            raw.expected_runtime_session_id,
            "provider UNKNOWN isolation runtime",
        )?)
        .map_err(|_| {
            to_sqlite_error(OperationsError::Corrupt(
                "provider UNKNOWN isolation runtime",
            ))
        })?,
        decode_string(
            raw.expected_provider_conversation_id,
            "provider UNKNOWN isolation conversation",
        )?,
        expected_project,
    )
    .map_err(to_sqlite_error)?;
    let reason = match raw.reason {
        0 => ProviderMessageUnknownIsolationReason::OperatorAcknowledgedUnknown,
        _ => {
            return Err(to_sqlite_error(OperationsError::Corrupt(
                "provider UNKNOWN isolation reason",
            )));
        }
    };
    let principal = registration::decode_principal_fields(
        raw.operator_principal_kind,
        raw.operator_principal_id.as_deref(),
    )
    .map_err(to_sqlite_error)?;
    let acknowledged_at = TimestampMillis::new(raw.acknowledged_at_ms).map_err(|_| {
        to_sqlite_error(OperationsError::Corrupt("provider UNKNOWN isolation time"))
    })?;
    Ok(ProviderMessageUnknownIsolationReceipt {
        request_id: decode_uuid(raw.request_id, "provider UNKNOWN isolation request")?,
        message_id: decode_uuid(raw.message_id, "provider UNKNOWN isolation message")?,
        expected_revision: decode_counter(
            raw.expected_revision,
            "provider UNKNOWN isolation revision",
        )?,
        expected_target,
        reason,
        principal,
        acknowledged_at,
    })
}
