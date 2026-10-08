//! Durable Orchestration operational approval authority.
//!
//! Orchestration owns the operational approval decision: the per-Workspace
//! approval policy with its auto-approve scope rules, the pending approval
//! request queue, and the admission envelope issuance that is the single
//! ingestion path into Forge `execution.begin`. This module moves the
//! in-memory Forge `ApprovalRuntime` half of the old split authority into
//! the operations catalog; Forge retains the durable execution-time
//! revalidation half (`create_pending_approval`/`approve_once`/
//! `consume_grant_and_begin_execution`) and consumes an O-issued admission
//! envelope inside that same transaction. Consumption authority therefore
//! stays in Forge: `ops_admission_records` journals issuance only, and the
//! reserved `consumed` state is set later from Forge-side consumption
//! evidence, never by an O decision.
//!
//! Every mutation happens in one IMMEDIATE transaction. The policy row is
//! fenced by an exact compare-and-set on `policy_revision`; a request is
//! decided exactly once (pending is the only decidable state), and one
//! approved request issues exactly one admission record
//! (`UNIQUE(request_id)`), so replays of the same submission converge on the
//! same durable outcome. Expiry is an explicit reconciliation command — the
//! operations layer owns no clock and runs no background loop.

use crate::{OperationsCatalog, OperationsError, approval_versions, catalog, wire};
use forge_domain::{
    ApprovalDecision, ApprovalRequestId, ApprovalRequestState, AuthenticatedPrincipal,
    AutoApprovalUseLimit, ExecutionActionClass, ExecutionAdmissionError, ExecutionAdmissionGrant,
    ExecutionAdmissionRequest, ExecutionPermissions, ForgeBackendId, OrchestrationIssuerId,
    TimestampMillis, WorkspaceId,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::str::FromStr;
use uuid::Uuid;

/// Lower bound for one approval request TTL (1 second).
pub const MIN_REQUEST_TTL_MS: i64 = 1_000;
/// Upper bound for one approval request TTL (24 hours).
pub const MAX_REQUEST_TTL_MS: i64 = 86_400_000;
/// Lower bound for one issued admission envelope TTL (1 second).
pub const MIN_ADMISSION_TTL_MS: i64 = 1_000;
/// Upper bound for one issued admission envelope TTL (1 hour).
pub const MAX_ADMISSION_TTL_MS: i64 = 3_600_000;
/// Maximum number of auto-approve scope rules in one workspace policy.
pub const MAX_APPROVAL_AUTO_SCOPES: usize = 16;
/// Maximum bounded JSON length for the encoded auto-approve scope rule list.
pub const MAX_APPROVAL_POLICY_JSON_BYTES: usize = 4096;
/// Maximum number of rows returned by one bounded approval request list.
pub const MAX_APPROVAL_LIST_LIMIT: usize = 64;
/// Maximum number of committed approval version changes returned by one
/// reconciliation observation. Mutations continue past this bound and set
/// `requires_resync` on the outcome.
pub const MAX_APPROVAL_OBSERVATION_CHANGES: usize = 64;

const ADMISSION_ID_DOMAIN: &[u8] = b"heterarchy-orchestration/approval-admission-id/v1\0";

const ISSUER_CONFIG_SQL: &str = "CREATE TABLE ops_issuer_config (singleton INTEGER PRIMARY KEY CHECK(singleton=1), issuer_id TEXT NOT NULL CHECK(length(issuer_id)=36), backend_id TEXT NOT NULL CHECK(length(backend_id)=36), admission_ttl_ms INTEGER NOT NULL CHECK(admission_ttl_ms BETWEEN 1000 AND 3600000), updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms>=0))";
const POLICY_SQL: &str = "CREATE TABLE ops_approval_policy (workspace_id BLOB PRIMARY KEY NOT NULL CHECK(length(workspace_id)=16), enabled INTEGER NOT NULL CHECK(enabled IN(0,1)), auto_scopes TEXT NOT NULL CHECK(length(auto_scopes) BETWEEN 2 AND 4096), policy_revision INTEGER NOT NULL CHECK(policy_revision>=1), created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0), updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms>=created_at_ms))";
const REQUESTS_SQL: &str = "CREATE TABLE ops_approval_requests (request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id)=16), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), action_class TEXT NOT NULL CHECK(action_class IN('direct_structured','discovered_project_command','shell_escape','analyzed_shell','generated_project_command')), request_digest BLOB NOT NULL CHECK(length(request_digest)=32), principal_kind INTEGER NOT NULL CHECK(principal_kind IN(0,1,2)), principal_id BLOB CHECK(principal_id IS NULL OR length(principal_id)=16), session_id BLOB NOT NULL CHECK(length(session_id)=16 AND session_id != zeroblob(16)), workspace_read INTEGER NOT NULL CHECK(workspace_read IN(0,1)), workspace_write INTEGER NOT NULL CHECK(workspace_write IN(0,1)), network INTEGER NOT NULL CHECK(network IN(0,1)), policy_revision INTEGER CHECK(policy_revision IS NULL OR policy_revision>=1), auto_scope_id BLOB CHECK(auto_scope_id IS NULL OR (length(auto_scope_id)=16 AND auto_scope_id != zeroblob(16))), state TEXT NOT NULL CHECK(state IN('pending','approved','rejected','expired','cancelled')), requested_at_ms INTEGER NOT NULL CHECK(requested_at_ms>=0), expires_at_ms INTEGER NOT NULL CHECK(expires_at_ms>requested_at_ms), decided_at_ms INTEGER CHECK(decided_at_ms IS NULL OR decided_at_ms>=requested_at_ms), decided_by BLOB CHECK(decided_by IS NULL OR (length(decided_by)=16 AND decided_by != zeroblob(16))), CHECK(state='pending' OR decided_at_ms IS NOT NULL), CHECK(state!='approved' OR decided_by IS NOT NULL), CHECK(auto_scope_id IS NULL OR state='approved'))";
const REQUESTS_WORKSPACE_INDEX: &str = "CREATE INDEX ops_approval_requests_workspace_idx ON ops_approval_requests(workspace_id, requested_at_ms)";
const ADMISSIONS_SQL: &str = "CREATE TABLE ops_admission_records (admission_id BLOB PRIMARY KEY NOT NULL CHECK(length(admission_id)=16), request_id BLOB NOT NULL UNIQUE REFERENCES ops_approval_requests(request_id) CHECK(length(request_id)=16), workspace_id BLOB NOT NULL CHECK(length(workspace_id)=16), issuer_id TEXT NOT NULL CHECK(length(issuer_id)=36), backend_id TEXT NOT NULL CHECK(length(backend_id)=36), issued_at_ms INTEGER NOT NULL CHECK(issued_at_ms>=0), expires_at_ms INTEGER NOT NULL CHECK(expires_at_ms>issued_at_ms), state TEXT NOT NULL CHECK(state IN('issued','consumed','expired')), consumed_at_ms INTEGER CHECK(consumed_at_ms IS NULL OR consumed_at_ms>=issued_at_ms), CHECK(state='consumed' OR consumed_at_ms IS NULL))";

const REQUEST_COLUMNS: &str = "substr(request_id,1,17),substr(workspace_id,1,17),action_class,substr(request_digest,1,33),principal_kind,substr(principal_id,1,17),substr(session_id,1,17),workspace_read,workspace_write,network,policy_revision,substr(auto_scope_id,1,17),state,requested_at_ms,expires_at_ms,decided_at_ms,substr(decided_by,1,17),(SELECT revision FROM ops_approval_request_versions WHERE request_id=ops_approval_requests.request_id)";

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("table", "ops_issuer_config", ISSUER_CONFIG_SQL),
        ("table", "ops_approval_policy", POLICY_SQL),
        ("table", "ops_approval_requests", REQUESTS_SQL),
        (
            "index",
            "ops_approval_requests_workspace_idx",
            REQUESTS_WORKSPACE_INDEX,
        ),
        ("table", "ops_admission_records", ADMISSIONS_SQL),
    ]
}

pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    for (_, _, sql) in schema_objects() {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

/// An action/permission/time-bounded automatic approval rule. Its use-count
/// policy may be finite or explicitly unlimited; neither form removes expiry.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalAutoScope {
    scope_id: Uuid,
    action: ExecutionActionClass,
    permission_ceiling: ExecutionPermissions,
    max_uses: AutoApprovalUseLimit,
    expires_at: TimestampMillis,
}

impl ApprovalAutoScope {
    /// Build a finite rule, retaining the existing 1..=1000 count contract.
    ///
    /// # Errors
    /// Rejects invalid identities, timestamps and finite use limits.
    pub fn new(
        scope_id: Uuid,
        action: ExecutionActionClass,
        permission_ceiling: ExecutionPermissions,
        max_uses: u16,
        expires_at_ms: i64,
    ) -> Result<Self, OperationsError> {
        let limit = AutoApprovalUseLimit::new(max_uses)
            .map_err(|_| OperationsError::Bounds("approval auto scope uses"))?;
        Self::with_use_limit(scope_id, action, permission_ceiling, limit, expires_at_ms)
    }

    /// Build a rule with a validated finite or explicit unlimited-count policy.
    ///
    /// # Errors
    /// Rejects a nil scope identity or an invalid expiry timestamp.
    pub fn with_use_limit(
        scope_id: Uuid,
        action: ExecutionActionClass,
        permission_ceiling: ExecutionPermissions,
        max_uses: AutoApprovalUseLimit,
        expires_at_ms: i64,
    ) -> Result<Self, OperationsError> {
        if scope_id.is_nil() {
            return Err(OperationsError::Bounds("approval auto scope id"));
        }
        let expires_at = TimestampMillis::new(expires_at_ms)
            .map_err(|_| OperationsError::Bounds("approval auto scope expiry"))?;
        Ok(Self {
            scope_id,
            action,
            permission_ceiling,
            max_uses,
            expires_at,
        })
    }

    #[must_use]
    pub const fn scope_id(&self) -> Uuid {
        self.scope_id
    }

    #[must_use]
    pub const fn action(&self) -> ExecutionActionClass {
        self.action
    }

    #[must_use]
    pub const fn permission_ceiling(&self) -> ExecutionPermissions {
        self.permission_ceiling
    }

    #[must_use]
    pub const fn max_uses(&self) -> AutoApprovalUseLimit {
        self.max_uses
    }

    #[must_use]
    pub const fn expires_at_ms(&self) -> i64 {
        self.expires_at.get()
    }
}

/// A required JSON value: a finite integer or explicit null. This is not an
/// optional field, so a missing count cannot silently widen an old policy.
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum AutoScopeUseLimitWire {
    Limited(u16),
    Unlimited(()),
}

/// Bounded JSON wire shape for one stored auto-approve scope rule.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AutoScopeWire {
    scope_id: String,
    action_class: String,
    workspace_read: bool,
    workspace_write: bool,
    network: bool,
    max_uses: AutoScopeUseLimitWire,
    expires_at_ms: i64,
}

impl AutoScopeWire {
    fn encode(scope: &ApprovalAutoScope) -> Self {
        Self {
            scope_id: scope.scope_id().to_string(),
            action_class: action_class_text(scope.action()).to_owned(),
            workspace_read: scope.permission_ceiling().workspace_read(),
            workspace_write: scope.permission_ceiling().workspace_write(),
            network: scope.permission_ceiling().network(),
            max_uses: match scope.max_uses().get() {
                Some(limit) => AutoScopeUseLimitWire::Limited(limit),
                None => AutoScopeUseLimitWire::Unlimited(()),
            },
            expires_at_ms: scope.expires_at_ms(),
        }
    }

    fn decode(value: &Self) -> Result<ApprovalAutoScope, OperationsError> {
        let scope_id = Uuid::parse_str(&value.scope_id)
            .map_err(|_| OperationsError::Corrupt("approval auto scope id"))?;
        if scope_id.to_string() != value.scope_id {
            return Err(OperationsError::Corrupt("approval auto scope id"));
        }
        let action = decode_action_class(&value.action_class)?;
        let max_uses = match value.max_uses {
            AutoScopeUseLimitWire::Limited(limit) => AutoApprovalUseLimit::new(limit)
                .map_err(|_| OperationsError::Corrupt("approval auto scope uses"))?,
            AutoScopeUseLimitWire::Unlimited(()) => AutoApprovalUseLimit::unlimited(),
        };
        ApprovalAutoScope::with_use_limit(
            scope_id,
            action,
            ExecutionPermissions::new(value.workspace_read, value.workspace_write, value.network),
            max_uses,
            value.expires_at_ms,
        )
    }
}

fn encode_auto_scopes(scopes: &[ApprovalAutoScope]) -> Result<String, OperationsError> {
    if scopes.len() > MAX_APPROVAL_AUTO_SCOPES {
        return Err(OperationsError::Bounds("approval auto scopes"));
    }
    let wires: Vec<AutoScopeWire> = scopes.iter().map(AutoScopeWire::encode).collect();
    let encoded = serde_json::to_string(&wires)?;
    if encoded.len() > MAX_APPROVAL_POLICY_JSON_BYTES {
        return Err(OperationsError::Bounds("approval policy json"));
    }
    Ok(encoded)
}

fn decode_auto_scopes(encoded: &str) -> Result<Vec<ApprovalAutoScope>, OperationsError> {
    if encoded.len() > MAX_APPROVAL_POLICY_JSON_BYTES {
        return Err(OperationsError::Corrupt("approval policy json"));
    }
    let wires: Vec<AutoScopeWire> = serde_json::from_str(encoded)?;
    if wires.len() > MAX_APPROVAL_AUTO_SCOPES {
        return Err(OperationsError::Corrupt("approval auto scopes"));
    }
    wires.iter().map(AutoScopeWire::decode).collect()
}

const fn action_class_text(action: ExecutionActionClass) -> &'static str {
    match action {
        ExecutionActionClass::DirectStructured => "direct_structured",
        ExecutionActionClass::DiscoveredProjectCommand => "discovered_project_command",
        ExecutionActionClass::ShellEscape => "shell_escape",
        ExecutionActionClass::AnalyzedShell => "analyzed_shell",
        ExecutionActionClass::GeneratedProjectCommand => "generated_project_command",
    }
}

fn decode_action_class(text: &str) -> Result<ExecutionActionClass, OperationsError> {
    match text {
        "direct_structured" => Ok(ExecutionActionClass::DirectStructured),
        "discovered_project_command" => Ok(ExecutionActionClass::DiscoveredProjectCommand),
        "shell_escape" => Ok(ExecutionActionClass::ShellEscape),
        "analyzed_shell" => Ok(ExecutionActionClass::AnalyzedShell),
        "generated_project_command" => Ok(ExecutionActionClass::GeneratedProjectCommand),
        _ => Err(OperationsError::Corrupt("approval action class")),
    }
}

/// Storage text for one request state. This module constructs only the five
/// durable states; forge-domain states this authority never issues
/// (`AutoApproved`/`Denied` fold into their manual counterparts with the
/// `auto_scope_id` column distinguishing an automatic approval).
const fn state_text(state: ApprovalRequestState) -> Option<&'static str> {
    match state {
        ApprovalRequestState::Pending => Some("pending"),
        ApprovalRequestState::Approved | ApprovalRequestState::AutoApproved => Some("approved"),
        ApprovalRequestState::Rejected | ApprovalRequestState::Denied => Some("rejected"),
        ApprovalRequestState::Expired => Some("expired"),
        ApprovalRequestState::Cancelled => Some("cancelled"),
        ApprovalRequestState::Superseded => None,
    }
}

fn decode_state(text: &str) -> Result<ApprovalRequestState, OperationsError> {
    match text {
        "pending" => Ok(ApprovalRequestState::Pending),
        "approved" => Ok(ApprovalRequestState::Approved),
        "rejected" => Ok(ApprovalRequestState::Rejected),
        "expired" => Ok(ApprovalRequestState::Expired),
        "cancelled" => Ok(ApprovalRequestState::Cancelled),
        _ => Err(OperationsError::Corrupt("approval request state")),
    }
}

const fn principal_parts(principal: AuthenticatedPrincipal) -> (i64, Option<[u8; 16]>) {
    match principal {
        AuthenticatedPrincipal::TrustedStdio => (0, None),
        AuthenticatedPrincipal::DirectToken(id) => (1, Some(id.into_uuid().into_bytes())),
        AuthenticatedPrincipal::OAuthGrant(id) => (2, Some(id.into_uuid().into_bytes())),
    }
}

fn decode_principal(
    kind: i64,
    principal_id: Option<Vec<u8>>,
) -> Result<AuthenticatedPrincipal, OperationsError> {
    match (kind, principal_id) {
        (0, None) => Ok(AuthenticatedPrincipal::TrustedStdio),
        (1, Some(raw)) => Ok(AuthenticatedPrincipal::DirectToken(
            forge_domain::DirectTokenId::from_uuid(Uuid::from_bytes(wire::id(raw)?)),
        )),
        (2, Some(raw)) => Ok(AuthenticatedPrincipal::OAuthGrant(
            forge_domain::OAuthGrantId::from_uuid(Uuid::from_bytes(wire::id(raw)?)),
        )),
        _ => Err(OperationsError::Corrupt("approval principal")),
    }
}

fn decode_optional_id(value: Option<Vec<u8>>) -> Result<Option<Uuid>, OperationsError> {
    value
        .map(|raw| Ok::<_, OperationsError>(Uuid::from_bytes(wire::id(raw)?)))
        .transpose()
}

/// One undecoded request row as read from the catalog.
struct RawRequest {
    request_id: Vec<u8>,
    workspace: Vec<u8>,
    action_class: String,
    digest: Vec<u8>,
    principal_kind: i64,
    principal_id: Option<Vec<u8>>,
    session_id: Vec<u8>,
    workspace_read: bool,
    workspace_write: bool,
    network: bool,
    policy_revision: Option<i64>,
    auto_scope_id: Option<Vec<u8>>,
    state: String,
    requested_at_ms: i64,
    expires_at_ms: i64,
    decided_at_ms: Option<i64>,
    decided_by: Option<Vec<u8>>,
    revision: Option<i64>,
}

impl RawRequest {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            request_id: row.get(0)?,
            workspace: row.get(1)?,
            action_class: row.get(2)?,
            digest: row.get(3)?,
            principal_kind: row.get(4)?,
            principal_id: row.get(5)?,
            session_id: row.get(6)?,
            workspace_read: row.get(7)?,
            workspace_write: row.get(8)?,
            network: row.get(9)?,
            policy_revision: row.get(10)?,
            auto_scope_id: row.get(11)?,
            state: row.get(12)?,
            requested_at_ms: row.get(13)?,
            expires_at_ms: row.get(14)?,
            decided_at_ms: row.get(15)?,
            decided_by: row.get(16)?,
            revision: row.get(17)?,
        })
    }

    fn decode(self) -> Result<RequestRow, OperationsError> {
        let policy_revision = match self.policy_revision {
            None => None,
            Some(value @ 1..) => Some(
                u64::try_from(value)
                    .map_err(|_| OperationsError::Corrupt("approval request policy revision"))?,
            ),
            Some(_) => {
                return Err(OperationsError::Corrupt("approval request policy revision"));
            }
        };
        Ok(RequestRow {
            request_id: ApprovalRequestId::from_uuid(Uuid::from_bytes(wire::id(self.request_id)?)),
            workspace: WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(self.workspace)?)),
            action: decode_action_class(&self.action_class)?,
            digest: self
                .digest
                .try_into()
                .map_err(|_| OperationsError::Corrupt("approval request digest"))?,
            principal: decode_principal(self.principal_kind, self.principal_id)?,
            session_id: Uuid::from_bytes(wire::id(self.session_id)?),
            requested: ExecutionPermissions::new(
                self.workspace_read,
                self.workspace_write,
                self.network,
            ),
            policy_revision,
            auto_scope_id: decode_optional_id(self.auto_scope_id)?,
            state: decode_state(&self.state)?,
            requested_at_ms: self.requested_at_ms,
            expires_at_ms: self.expires_at_ms,
            decided_at_ms: self.decided_at_ms,
            decided_by: decode_optional_id(self.decided_by)?,
            revision: match self.revision {
                Some(value @ 1..) => u64::try_from(value)
                    .map_err(|_| OperationsError::Corrupt("approval request revision"))?,
                _ => return Err(OperationsError::Corrupt("approval request revision")),
            },
        })
    }
}

/// One decoded durable approval request row.
struct RequestRow {
    request_id: ApprovalRequestId,
    workspace: WorkspaceId,
    action: ExecutionActionClass,
    digest: [u8; 32],
    principal: AuthenticatedPrincipal,
    session_id: Uuid,
    requested: ExecutionPermissions,
    policy_revision: Option<u64>,
    auto_scope_id: Option<Uuid>,
    state: ApprovalRequestState,
    requested_at_ms: i64,
    expires_at_ms: i64,
    decided_at_ms: Option<i64>,
    decided_by: Option<Uuid>,
    revision: u64,
}

impl RequestRow {
    fn record(&self) -> ApprovalRequestRecord {
        ApprovalRequestRecord {
            request_id: self.request_id,
            workspace_id: self.workspace,
            action: self.action,
            request_digest: self.digest,
            principal: self.principal,
            session_id: self.session_id,
            requested_permissions: self.requested,
            policy_revision: self.policy_revision,
            auto_scope_id: self.auto_scope_id,
            state: self.state,
            requested_at_ms: self.requested_at_ms,
            expires_at_ms: self.expires_at_ms,
            decided_at_ms: self.decided_at_ms,
            decided_by: self.decided_by,
            revision: self.revision,
        }
    }

    /// The immutable submission content: an exact replay must match every
    /// field. The observation clock is intentionally excluded so a genuine
    /// retry after a lost reply converges on the stored request.
    fn matches_submission(&self, command: &SubmitApprovalCommand) -> bool {
        self.workspace == command.workspace_id
            && self.action == command.action
            && self.digest == command.request_digest
            && self.principal == command.principal
            && self.session_id == command.session_id
            && self.requested == command.requested_permissions
    }
}

fn request_select(where_clause: &str) -> String {
    format!("SELECT {REQUEST_COLUMNS} FROM ops_approval_requests {where_clause}")
}

fn load_request_row(
    connection: &Connection,
    request_id: ApprovalRequestId,
) -> Result<Option<RequestRow>, OperationsError> {
    connection
        .query_row(
            request_select("WHERE request_id=?1").as_str(),
            params![request_id.into_uuid().as_bytes()],
            RawRequest::from_row,
        )
        .optional()
        .map_err(OperationsError::from)
        .and_then(|raw: Option<RawRequest>| raw.map(RawRequest::decode).transpose())
}

fn insert_request(tx: &Transaction<'_>, row: &RequestRow) -> Result<(), OperationsError> {
    let (principal_kind, principal_id) = principal_parts(row.principal);
    let state = state_text(row.state).ok_or(OperationsError::Corrupt("approval request state"))?;
    tx.execute(
        "INSERT INTO ops_approval_requests(request_id,workspace_id,action_class,request_digest,principal_kind,principal_id,session_id,workspace_read,workspace_write,network,policy_revision,auto_scope_id,state,requested_at_ms,expires_at_ms,decided_at_ms,decided_by) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
        params![
            row.request_id.into_uuid().as_bytes(),
            row.workspace.into_uuid().as_bytes(),
            action_class_text(row.action),
            row.digest.as_slice(),
            principal_kind,
            principal_id.map(Vec::from),
            row.session_id.as_bytes(),
            i64::from(row.requested.workspace_read()),
            i64::from(row.requested.workspace_write()),
            i64::from(row.requested.network()),
            row
                .policy_revision
                .map(catalog::signed)
                .transpose()?,
            row.auto_scope_id.map(|id| id.as_bytes().to_vec()),
            state,
            row.requested_at_ms,
            row.expires_at_ms,
            row.decided_at_ms,
            row.decided_by.map(|id| id.as_bytes().to_vec()),
        ],
    )?;
    approval_versions::insert_baseline(tx, row.request_id)?;
    Ok(())
}

/// Deterministic admission identity: one approved request always maps to the
/// same admission record, so replayed decisions cannot duplicate issuance.
fn admission_id_for(request_id: ApprovalRequestId) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(ADMISSION_ID_DOMAIN);
    hasher.update(request_id.into_uuid().as_bytes());
    let digest: [u8; 32] = hasher.finalize().into();
    let mut prefix = [0_u8; 16];
    prefix.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(prefix)
}

struct IssuerRow {
    issuer: OrchestrationIssuerId,
    backend_id: ForgeBackendId,
    admission_ttl_ms: i64,
    updated_at_ms: i64,
}

fn decode_issuer_text(
    issuer: &str,
    backend: &str,
) -> Result<(OrchestrationIssuerId, ForgeBackendId), OperationsError> {
    let issuer_id = OrchestrationIssuerId::from_str(issuer)
        .map_err(|_| OperationsError::Corrupt("admission issuer id"))?;
    if issuer_id.to_string() != issuer {
        return Err(OperationsError::Corrupt("admission issuer id"));
    }
    let backend_id = ForgeBackendId::from_str(backend)
        .map_err(|_| OperationsError::Corrupt("admission backend id"))?;
    if backend_id.to_string() != backend {
        return Err(OperationsError::Corrupt("admission backend id"));
    }
    Ok((issuer_id, backend_id))
}

/// One undecoded issuer configuration row as read from the catalog.
struct RawIssuer {
    issuer_id: String,
    backend_id: String,
    admission_ttl_ms: i64,
    updated_at_ms: i64,
}

fn load_issuer(connection: &Connection) -> Result<Option<IssuerRow>, OperationsError> {
    let raw: Option<RawIssuer> = connection
        .query_row(
            "SELECT issuer_id,backend_id,admission_ttl_ms,updated_at_ms FROM ops_issuer_config WHERE singleton=1",
            [],
            |row| {
                Ok(RawIssuer {
                    issuer_id: row.get(0)?,
                    backend_id: row.get(1)?,
                    admission_ttl_ms: row.get(2)?,
                    updated_at_ms: row.get(3)?,
                })
            },
        )
        .optional()?;
    raw.map(|raw| {
        let (issuer, backend_id) = decode_issuer_text(&raw.issuer_id, &raw.backend_id)?;
        Ok(IssuerRow {
            issuer,
            backend_id,
            admission_ttl_ms: raw.admission_ttl_ms,
            updated_at_ms: raw.updated_at_ms,
        })
    })
    .transpose()
}

fn issue_admission(
    tx: &Transaction<'_>,
    request_id: ApprovalRequestId,
    workspace_id: WorkspaceId,
    issued_at_ms: i64,
    issuer: &IssuerRow,
) -> Result<AdmissionIssuance, OperationsError> {
    let issued_at = TimestampMillis::new(issued_at_ms)
        .map_err(|_| OperationsError::Corrupt("admission issued timestamp"))?;
    let expires_ms = issuer
        .admission_ttl_ms
        .checked_add(issued_at_ms)
        .ok_or(OperationsError::Bounds("admission expiry"))?;
    let expires_at = TimestampMillis::new(expires_ms)
        .map_err(|_| OperationsError::Bounds("admission expiry"))?;
    let admission_id = admission_id_for(request_id);
    tx.execute(
        "INSERT INTO ops_admission_records(admission_id,request_id,workspace_id,issuer_id,backend_id,issued_at_ms,expires_at_ms,state) VALUES(?1,?2,?3,?4,?5,?6,?7,'issued')",
        params![
            admission_id.as_bytes(),
            request_id.into_uuid().as_bytes(),
            workspace_id.into_uuid().as_bytes(),
            issuer.issuer.to_string(),
            issuer.backend_id.to_string(),
            issued_at_ms,
            expires_ms,
        ],
    )?;
    Ok(AdmissionIssuance {
        admission_id,
        request_id,
        workspace_id,
        issuer: issuer.issuer,
        backend_id: issuer.backend_id,
        issued_at,
        expires_at,
    })
}

fn load_admission(
    connection: &Connection,
    request_id: ApprovalRequestId,
) -> Result<Option<AdmissionIssuance>, OperationsError> {
    struct RawAdmission {
        admission: Vec<u8>,
        workspace: Vec<u8>,
        issuer_id: String,
        backend_id: String,
        issued_at_ms: i64,
        expires_at_ms: i64,
    }
    let raw: Option<RawAdmission> = connection
        .query_row(
            "SELECT substr(admission_id,1,17),substr(workspace_id,1,17),issuer_id,backend_id,issued_at_ms,expires_at_ms FROM ops_admission_records WHERE request_id=?1",
            params![request_id.into_uuid().as_bytes()],
            |row| {
                Ok(RawAdmission {
                    admission: row.get(0)?,
                    workspace: row.get(1)?,
                    issuer_id: row.get(2)?,
                    backend_id: row.get(3)?,
                    issued_at_ms: row.get(4)?,
                    expires_at_ms: row.get(5)?,
                })
            },
        )
        .optional()?;
    raw.map(|raw| {
        let (issuer, backend_id) = decode_issuer_text(&raw.issuer_id, &raw.backend_id)?;
        Ok(AdmissionIssuance {
            admission_id: Uuid::from_bytes(wire::id(raw.admission)?),
            request_id,
            workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(raw.workspace)?)),
            issuer,
            backend_id,
            issued_at: TimestampMillis::new(raw.issued_at_ms)
                .map_err(|_| OperationsError::Corrupt("admission issued"))?,
            expires_at: TimestampMillis::new(raw.expires_at_ms)
                .map_err(|_| OperationsError::Corrupt("admission expiry"))?,
        })
    })
    .transpose()
}

struct PolicyRow {
    workspace: WorkspaceId,
    enabled: bool,
    auto_scopes: Vec<ApprovalAutoScope>,
    revision: u64,
    created_at_ms: i64,
    updated_at_ms: i64,
}

fn load_policy(
    connection: &Connection,
    workspace_id: WorkspaceId,
) -> Result<Option<PolicyRow>, OperationsError> {
    let raw: Option<(Vec<u8>, i64, String, i64, i64, i64)> = connection
        .query_row(
            "SELECT substr(workspace_id,1,17),enabled,substr(auto_scopes,1,4097),policy_revision,created_at_ms,updated_at_ms FROM ops_approval_policy WHERE workspace_id=?1",
            params![workspace_id.into_uuid().as_bytes()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            },
        )
        .optional()?;
    raw.map(
        |(workspace, enabled, scopes, revision, created_at_ms, updated_at_ms)| {
            if revision < 1 {
                return Err(OperationsError::Corrupt("approval policy revision"));
            }
            Ok(PolicyRow {
                workspace: WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(workspace)?)),
                enabled: enabled == 1,
                auto_scopes: decode_auto_scopes(&scopes)?,
                revision: u64::try_from(revision)
                    .map_err(|_| OperationsError::Corrupt("approval policy revision"))?,
                created_at_ms,
                updated_at_ms,
            })
        },
    )
    .transpose()
}

/// The first rule of an enabled policy that covers one submission by action
/// class, permission ceiling, validity window and remaining uses.
fn find_matching_auto_scope<'a>(
    tx: &Transaction<'_>,
    policy: &'a PolicyRow,
    command: &SubmitApprovalCommand,
) -> Result<Option<&'a ApprovalAutoScope>, OperationsError> {
    if !policy.enabled {
        return Ok(None);
    }
    for scope in &policy.auto_scopes {
        if scope.action() != command.action
            || !scope
                .permission_ceiling()
                .contains(command.requested_permissions)
            || command.now_ms >= scope.expires_at_ms()
        {
            continue;
        }
        // Unlimited rules have no quota to count. Keep the permission and
        // expiry checks above, but avoid scanning approval history per use.
        if scope.max_uses().get().is_none()
            || scope
                .max_uses()
                .permits(auto_scope_uses(tx, policy.workspace, scope.scope_id())?)
        {
            return Ok(Some(scope));
        }
    }
    Ok(None)
}

fn auto_scope_uses(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    scope_id: Uuid,
) -> Result<u64, OperationsError> {
    let used: i64 = tx.query_row(
        "SELECT COUNT(*) FROM ops_approval_requests WHERE workspace_id=?1 AND auto_scope_id=?2 AND state='approved'",
        params![workspace_id.into_uuid().as_bytes(), scope_id.as_bytes()],
        |row| row.get(0),
    )?;
    u64::try_from(used).map_err(|_| OperationsError::Corrupt("approval auto scope uses"))
}

/// Command replacing one workspace approval policy. `expected_revision`
/// selects the fence: `None` creates the initial policy and conflicts when
/// one already exists, `Some(revision)` replaces exactly that revision.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetApprovalPolicyCommand {
    pub workspace_id: WorkspaceId,
    pub enabled: bool,
    pub auto_scopes: Vec<ApprovalAutoScope>,
    pub expected_revision: Option<u64>,
    pub now_ms: i64,
}

/// A durable approval policy view for one exact Workspace scope.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalPolicyView {
    workspace_id: WorkspaceId,
    enabled: bool,
    auto_scopes: Vec<ApprovalAutoScope>,
    revision: u64,
    created_at_ms: i64,
    updated_at_ms: i64,
}

impl ApprovalPolicyView {
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn auto_scopes(&self) -> &[ApprovalAutoScope] {
        &self.auto_scopes
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub const fn created_at_ms(&self) -> i64 {
        self.created_at_ms
    }

    #[must_use]
    pub const fn updated_at_ms(&self) -> i64 {
        self.updated_at_ms
    }
}

/// Command configuring the Orchestration admission issuer. The issuer and
/// backend identities are opaque forge-domain identities, not credentials.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigureAdmissionIssuerCommand {
    pub issuer: OrchestrationIssuerId,
    pub backend_id: ForgeBackendId,
    pub admission_ttl_ms: i64,
    pub now_ms: i64,
}

impl ConfigureAdmissionIssuerCommand {
    fn validate(self) -> Result<(), OperationsError> {
        if self.issuer.into_uuid().is_nil() || self.backend_id.into_uuid().is_nil() {
            return Err(OperationsError::Bounds("admission issuer identity"));
        }
        if self.now_ms < 0
            || !(MIN_ADMISSION_TTL_MS..=MAX_ADMISSION_TTL_MS).contains(&self.admission_ttl_ms)
        {
            return Err(OperationsError::Bounds("admission ttl"));
        }
        Ok(())
    }
}

/// Durable admission issuer configuration used for every envelope issuance.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionIssuerConfig {
    issuer: OrchestrationIssuerId,
    backend_id: ForgeBackendId,
    admission_ttl_ms: i64,
    updated_at_ms: i64,
}

impl AdmissionIssuerConfig {
    pub const fn issuer(&self) -> OrchestrationIssuerId {
        self.issuer
    }

    pub const fn backend_id(&self) -> ForgeBackendId {
        self.backend_id
    }

    #[must_use]
    pub const fn admission_ttl_ms(&self) -> i64 {
        self.admission_ttl_ms
    }

    #[must_use]
    pub const fn updated_at_ms(&self) -> i64 {
        self.updated_at_ms
    }
}

/// Command submitting one execution approval request from a session-bound
/// principal. The request id is the caller's stable replay identity.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitApprovalCommand {
    pub request_id: ApprovalRequestId,
    pub workspace_id: WorkspaceId,
    pub action: ExecutionActionClass,
    pub request_digest: [u8; 32],
    pub principal: AuthenticatedPrincipal,
    pub session_id: Uuid,
    pub requested_permissions: ExecutionPermissions,
    pub ttl_ms: i64,
    pub now_ms: i64,
}

/// Command deciding one pending approval request. `workspace_id` fences the
/// request scope inside the same transaction. `decided_by` is the
/// authenticated caller's owned open logical Session UUID; transports derive
/// and validate it rather than trusting a body-supplied identity. `Approve`
/// and `Reject` attribute the decision to that session; `Cancel` closes a
/// pending request without an attributing identity.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecideApprovalCommand {
    pub request_id: ApprovalRequestId,
    pub workspace_id: WorkspaceId,
    pub decision: ApprovalDecision,
    pub decided_by: Uuid,
    pub now_ms: i64,
}

/// One durable approval request record.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalRequestRecord {
    request_id: ApprovalRequestId,
    workspace_id: WorkspaceId,
    action: ExecutionActionClass,
    request_digest: [u8; 32],
    principal: AuthenticatedPrincipal,
    session_id: Uuid,
    requested_permissions: ExecutionPermissions,
    policy_revision: Option<u64>,
    auto_scope_id: Option<Uuid>,
    state: ApprovalRequestState,
    requested_at_ms: i64,
    expires_at_ms: i64,
    decided_at_ms: Option<i64>,
    decided_by: Option<Uuid>,
    revision: u64,
}

impl ApprovalRequestRecord {
    #[must_use]
    pub const fn request_id(&self) -> ApprovalRequestId {
        self.request_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn action(&self) -> ExecutionActionClass {
        self.action
    }

    #[must_use]
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }

    pub const fn principal(&self) -> AuthenticatedPrincipal {
        self.principal
    }

    #[must_use]
    pub const fn session_id(&self) -> Uuid {
        self.session_id
    }

    #[must_use]
    pub const fn requested_permissions(&self) -> ExecutionPermissions {
        self.requested_permissions
    }

    #[must_use]
    pub const fn policy_revision(&self) -> Option<u64> {
        self.policy_revision
    }

    #[must_use]
    pub const fn auto_scope_id(&self) -> Option<Uuid> {
        self.auto_scope_id
    }

    #[must_use]
    pub const fn state(&self) -> ApprovalRequestState {
        self.state
    }

    #[must_use]
    pub const fn requested_at_ms(&self) -> i64 {
        self.requested_at_ms
    }

    #[must_use]
    pub const fn expires_at_ms(&self) -> i64 {
        self.expires_at_ms
    }

    #[must_use]
    pub const fn decided_at_ms(&self) -> Option<i64> {
        self.decided_at_ms
    }

    #[must_use]
    pub const fn decided_by(&self) -> Option<Uuid> {
        self.decided_by
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
}

/// Bounded approval request list query.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApprovalRequestQuery {
    pub workspace_id: WorkspaceId,
    pub state: Option<ApprovalRequestState>,
    pub limit: usize,
}

/// Outcome of one submission: the durable request record, the admission
/// envelope when the request is approved, and whether the submission replayed
/// an existing request.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalSubmission {
    request: ApprovalRequestRecord,
    admission: Option<AdmissionIssuance>,
    replay: bool,
}

impl ApprovalSubmission {
    pub const fn request(&self) -> &ApprovalRequestRecord {
        &self.request
    }

    #[must_use]
    pub const fn admission(&self) -> Option<&AdmissionIssuance> {
        self.admission.as_ref()
    }

    #[must_use]
    pub const fn replay(&self) -> bool {
        self.replay
    }
}

/// Outcome of one explicit decision on a pending request.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalDecisionOutcome {
    request: ApprovalRequestRecord,
    admission: Option<AdmissionIssuance>,
}

impl ApprovalDecisionOutcome {
    pub const fn request(&self) -> &ApprovalRequestRecord {
        &self.request
    }

    #[must_use]
    pub const fn admission(&self) -> Option<&AdmissionIssuance> {
        self.admission.as_ref()
    }
}

/// One approval request version committed by a reconciliation observation.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApprovalRequestVersionChange {
    request_id: ApprovalRequestId,
    workspace_id: WorkspaceId,
    revision: u64,
}

impl ApprovalRequestVersionChange {
    #[must_use]
    pub const fn request_id(&self) -> ApprovalRequestId {
        self.request_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
}

/// One approval policy version committed by an automatic-approval disable.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApprovalPolicyVersionChange {
    workspace_id: WorkspaceId,
    revision: u64,
}

impl ApprovalPolicyVersionChange {
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
}

/// Counted result of one explicit expiry reconciliation pass.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalExpiryReconciliation {
    expired_requests: u64,
    expired_admissions: u64,
    requests: Vec<ApprovalRequestVersionChange>,
    requires_resync: bool,
}

impl ApprovalExpiryReconciliation {
    #[must_use]
    pub const fn expired_requests(&self) -> u64 {
        self.expired_requests
    }

    #[must_use]
    pub const fn expired_admissions(&self) -> u64 {
        self.expired_admissions
    }

    pub fn requests(&self) -> &[ApprovalRequestVersionChange] {
        &self.requests
    }

    #[must_use]
    pub const fn requires_resync(&self) -> bool {
        self.requires_resync
    }
}

/// Committed policy changes observed while disabling automatic approval.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalPolicyDisableOutcome {
    changed: usize,
    policies: Vec<ApprovalPolicyVersionChange>,
    requires_resync: bool,
}

impl ApprovalPolicyDisableOutcome {
    #[must_use]
    pub const fn changed(&self) -> usize {
        self.changed
    }

    pub fn policies(&self) -> &[ApprovalPolicyVersionChange] {
        &self.policies
    }

    #[must_use]
    pub const fn requires_resync(&self) -> bool {
        self.requires_resync
    }
}

/// One issued admission envelope journal record. The issuer, backend and
/// half-open validity window are the exact fields Forge's `execution.begin`
/// ingestion joins with its own server-derived request fence.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionIssuance {
    admission_id: Uuid,
    request_id: ApprovalRequestId,
    workspace_id: WorkspaceId,
    issuer: OrchestrationIssuerId,
    backend_id: ForgeBackendId,
    issued_at: TimestampMillis,
    expires_at: TimestampMillis,
}

impl AdmissionIssuance {
    #[must_use]
    pub const fn admission_id(&self) -> Uuid {
        self.admission_id
    }

    #[must_use]
    pub const fn request_id(&self) -> ApprovalRequestId {
        self.request_id
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    pub const fn issuer(&self) -> OrchestrationIssuerId {
        self.issuer
    }

    pub const fn backend_id(&self) -> ForgeBackendId {
        self.backend_id
    }

    #[must_use]
    pub const fn issued_at_ms(&self) -> i64 {
        self.issued_at.get()
    }

    #[must_use]
    pub const fn expires_at_ms(&self) -> i64 {
        self.expires_at.get()
    }

    /// Construct the forge-domain half of the admission envelope for this
    /// issuance. The exact request fence (backend/Workspace scope, Activity,
    /// Goal attribution, Agent actor, execution id and digest) is supplied by
    /// the Forge-side boundary that revalidates the envelope, mirroring the
    /// `ExecutionAdmissionEnvelope { backend_id, issuer, grant }` shape.
    ///
    /// # Errors
    /// Rejects a request whose window disagrees with this issuance.
    pub fn envelope_grant(
        &self,
        request: ExecutionAdmissionRequest,
    ) -> Result<ExecutionAdmissionGrant, ExecutionAdmissionError> {
        ExecutionAdmissionGrant::new(self.issuer, request, self.issued_at, self.expires_at)
    }
}

impl OperationsCatalog {
    fn approval_transaction<T, F>(&mut self, operation: F) -> Result<T, OperationsError>
    where
        F: FnOnce(&Transaction<'_>) -> Result<(WorkspaceId, T), OperationsError>,
    {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (workspace_id, result) = operation(&tx)?;
        tx.commit()
            .map_err(|source| OperationsError::ApprovalCommitUnconfirmed {
                workspace_id: workspace_id.into_uuid(),
                source,
            })?;
        Ok(result)
    }

    /// Install or replace the admission issuer configuration. The values are
    /// exact identities; a later replace overwrites the singleton in one
    /// transaction and every issuance reads the configuration it contains.
    ///
    /// # Errors
    /// Rejects a TTL outside the bounded window and uncertain commits.
    pub fn configure_admission_issuer(
        &mut self,
        command: ConfigureAdmissionIssuerCommand,
    ) -> Result<AdmissionIssuerConfig, OperationsError> {
        command.validate()?;
        let destination = self.destination;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO ops_issuer_config(singleton,issuer_id,backend_id,admission_ttl_ms,updated_at_ms) VALUES(1,?1,?2,?3,?4) ON CONFLICT(singleton) DO UPDATE SET issuer_id=?1,backend_id=?2,admission_ttl_ms=?3,updated_at_ms=?4",
            params![
                command.issuer.to_string(),
                command.backend_id.to_string(),
                command.admission_ttl_ms,
                command.now_ms,
            ],
        )?;
        tx.commit()
            .map_err(|source| OperationsError::CatalogCommitUnconfirmed {
                destination_id: destination,
                source,
            })?;
        Ok(AdmissionIssuerConfig {
            issuer: command.issuer,
            backend_id: command.backend_id,
            admission_ttl_ms: command.admission_ttl_ms,
            updated_at_ms: command.now_ms,
        })
    }

    /// Initialize an explicitly configured host issuer without replacing an
    /// existing binding. Concurrent hosts serialize the check and insert;
    /// exact replays preserve the original configuration timestamp.
    ///
    /// # Errors
    /// Rejects invalid identities/TTL, differing existing configuration, and
    /// uncertain commits. This operation never changes approval policy.
    pub fn ensure_admission_issuer(
        &mut self,
        command: ConfigureAdmissionIssuerCommand,
    ) -> Result<AdmissionIssuerConfig, OperationsError> {
        command.validate()?;
        let destination = self.destination;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let configured = if let Some(existing) = load_issuer(&tx)? {
            if existing.issuer != command.issuer
                || existing.backend_id != command.backend_id
                || existing.admission_ttl_ms != command.admission_ttl_ms
            {
                return Err(OperationsError::Conflict(
                    "admission issuer host configuration",
                ));
            }
            AdmissionIssuerConfig {
                issuer: existing.issuer,
                backend_id: existing.backend_id,
                admission_ttl_ms: existing.admission_ttl_ms,
                updated_at_ms: existing.updated_at_ms,
            }
        } else {
            tx.execute(
                "INSERT INTO ops_issuer_config(singleton,issuer_id,backend_id,admission_ttl_ms,updated_at_ms) VALUES(1,?1,?2,?3,?4)",
                params![
                    command.issuer.to_string(),
                    command.backend_id.to_string(),
                    command.admission_ttl_ms,
                    command.now_ms,
                ],
            )?;
            AdmissionIssuerConfig {
                issuer: command.issuer,
                backend_id: command.backend_id,
                admission_ttl_ms: command.admission_ttl_ms,
                updated_at_ms: command.now_ms,
            }
        };
        tx.commit()
            .map_err(|source| OperationsError::CatalogCommitUnconfirmed {
                destination_id: destination,
                source,
            })?;
        Ok(configured)
    }

    /// Read the current admission issuer configuration, if configured.
    ///
    /// # Errors
    /// Rejects corrupt stored identities and database read failures.
    pub fn admission_issuer_config(
        &self,
    ) -> Result<Option<AdmissionIssuerConfig>, OperationsError> {
        load_issuer(&self.connection).map(|row| {
            row.map(|issuer| AdmissionIssuerConfig {
                issuer: issuer.issuer,
                backend_id: issuer.backend_id,
                admission_ttl_ms: issuer.admission_ttl_ms,
                updated_at_ms: issuer.updated_at_ms,
            })
        })
    }

    /// Read one workspace's approval policy, if one was ever set.
    ///
    /// # Errors
    /// Rejects corrupt stored policy state and database read failures.
    pub fn approval_policy(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<ApprovalPolicyView>, OperationsError> {
        load_policy(&self.connection, workspace_id).map(|row| {
            row.map(|policy| ApprovalPolicyView {
                workspace_id: policy.workspace,
                enabled: policy.enabled,
                auto_scopes: policy.auto_scopes,
                revision: policy.revision,
                created_at_ms: policy.created_at_ms,
                updated_at_ms: policy.updated_at_ms,
            })
        })
    }

    /// Create or compare-and-set replace one workspace approval policy.
    ///
    /// # Errors
    /// Rejects unbounded rule lists, a missing or stale expected revision,
    /// a negative observation time, and uncertain commits.
    pub fn set_approval_policy(
        &mut self,
        command: SetApprovalPolicyCommand,
    ) -> Result<ApprovalPolicyView, OperationsError> {
        let SetApprovalPolicyCommand {
            workspace_id,
            enabled,
            auto_scopes,
            expected_revision,
            now_ms,
        } = command;
        let encoded = encode_auto_scopes(&auto_scopes)?;
        if now_ms < 0 {
            return Err(OperationsError::Bounds("approval policy time"));
        }
        self.approval_transaction(move |tx| {
            let existing = load_policy(tx, workspace_id)?;
            match (existing, expected_revision) {
                (None, None) => {
                    tx.execute(
                        "INSERT INTO ops_approval_policy(workspace_id,enabled,auto_scopes,policy_revision,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,1,?4,?4)",
                        params![
                            workspace_id.into_uuid().as_bytes(),
                            i64::from(enabled),
                            encoded,
                            now_ms,
                        ],
                    )?;
                    Ok((
                        workspace_id,
                        ApprovalPolicyView {
                            workspace_id,
                            enabled,
                            auto_scopes,
                            revision: 1,
                            created_at_ms: now_ms,
                            updated_at_ms: now_ms,
                        },
                    ))
                }
                (Some(row), Some(expected)) => {
                    if row.revision != expected {
                        return Err(OperationsError::Conflict("approval policy revision"));
                    }
                    let next = expected
                        .checked_add(1)
                        .ok_or(OperationsError::Bounds("approval policy revision"))?;
                    tx.execute(
                        "UPDATE ops_approval_policy SET enabled=?1,auto_scopes=?2,policy_revision=?3,updated_at_ms=?4 WHERE workspace_id=?5 AND policy_revision=?6",
                        params![
                            i64::from(enabled),
                            encoded,
                            catalog::signed(next)?,
                            now_ms,
                            workspace_id.into_uuid().as_bytes(),
                            catalog::signed(expected)?,
                        ],
                    )?;
                    Ok((
                        workspace_id,
                        ApprovalPolicyView {
                            workspace_id,
                            enabled,
                            auto_scopes,
                            revision: next,
                            created_at_ms: row.created_at_ms,
                            updated_at_ms: now_ms,
                        },
                    ))
                }
                (None, Some(_)) | (Some(_), None) => {
                    Err(OperationsError::Conflict("approval policy revision"))
                }
            }
        })
    }

    /// Submit one execution approval request from a session-bound principal.
    /// An auto-scope match approves immediately and issues the admission
    /// envelope in the same transaction; every other submission parks as
    /// pending. Replays of the exact same submission converge on the stored
    /// outcome.
    ///
    /// # Errors
    /// Rejects replay conflicts, out-of-window TTLs, a missing admission
    /// issuer configuration for an auto-approval, and uncertain commits.
    pub fn submit_approval(
        &mut self,
        command: SubmitApprovalCommand,
    ) -> Result<ApprovalSubmission, OperationsError> {
        if command.request_id.into_uuid().is_nil()
            || command.session_id.is_nil()
            || command.now_ms < 0
            || !(MIN_REQUEST_TTL_MS..=MAX_REQUEST_TTL_MS).contains(&command.ttl_ms)
        {
            return Err(OperationsError::Bounds("approval request"));
        }
        self.approval_transaction(move |tx| {
            if let Some(existing) = load_request_row(tx, command.request_id)? {
                if !existing.matches_submission(&command) {
                    return Err(OperationsError::Conflict("approval request identity"));
                }
                let admission = load_admission(tx, command.request_id)?;
                return Ok((
                    command.workspace_id,
                    ApprovalSubmission {
                        request: existing.record(),
                        admission,
                        replay: true,
                    },
                ));
            }
            let policy = load_policy(tx, command.workspace_id)?;
            let policy_revision = policy.as_ref().map(|row| row.revision);
            let matched = match policy.as_ref() {
                Some(row) => find_matching_auto_scope(tx, row, &command)?,
                None => None,
            };
            let expires_at_ms = command
                .now_ms
                .checked_add(command.ttl_ms)
                .ok_or(OperationsError::Bounds("approval request expiry"))?;
            let (state, auto_scope_id, decided_at_ms, decided_by) = match matched {
                Some(scope) => (
                    ApprovalRequestState::Approved,
                    Some(scope.scope_id()),
                    Some(command.now_ms),
                    Some(scope.scope_id()),
                ),
                None => (ApprovalRequestState::Pending, None, None, None),
            };
            let row = RequestRow {
                request_id: command.request_id,
                workspace: command.workspace_id,
                action: command.action,
                digest: command.request_digest,
                principal: command.principal,
                session_id: command.session_id,
                requested: command.requested_permissions,
                policy_revision,
                auto_scope_id,
                state,
                requested_at_ms: command.now_ms,
                expires_at_ms,
                decided_at_ms,
                decided_by,
                revision: 1,
            };
            insert_request(tx, &row)?;
            let admission = match matched {
                Some(_) => {
                    let issuer =
                        load_issuer(tx)?.ok_or(OperationsError::Conflict("admission issuer"))?;
                    Some(issue_admission(
                        tx,
                        command.request_id,
                        command.workspace_id,
                        command.now_ms,
                        &issuer,
                    )?)
                }
                None => None,
            };
            Ok((
                command.workspace_id,
                ApprovalSubmission {
                    request: row.record(),
                    admission,
                    replay: false,
                },
            ))
        })
    }

    /// Decide one pending request: approve (issuing the admission envelope in
    /// the same transaction), reject, or cancel. The decision time must not
    /// precede the request, and an already-expired pending request fails
    /// closed.
    ///
    /// # Errors
    /// Rejects unknown requests, non-pending states, unsupported decision
    /// kinds, stale decision times, a missing admission issuer configuration
    /// for an approval, and uncertain commits.
    pub fn decide_approval(
        &mut self,
        command: DecideApprovalCommand,
    ) -> Result<ApprovalDecisionOutcome, OperationsError> {
        if command.now_ms < 0
            || (matches!(
                command.decision,
                ApprovalDecision::Approve | ApprovalDecision::Reject
            ) && command.decided_by.is_nil())
        {
            return Err(OperationsError::Bounds("approval decision"));
        }
        self.approval_transaction(move |tx| {
            let row = load_request_row(tx, command.request_id)?
                .ok_or(OperationsError::Conflict("approval request missing"))?;
            if row.workspace != command.workspace_id {
                return Err(OperationsError::Conflict("approval request workspace"));
            }
            if row.state != ApprovalRequestState::Pending {
                return Err(OperationsError::Conflict("approval request decided"));
            }
            if command.now_ms < row.requested_at_ms {
                return Err(OperationsError::Conflict("approval decision time"));
            }
            let workspace = row.workspace;
            let next_state = match command.decision {
                ApprovalDecision::Cancel => {
                    tx.execute(
                        "UPDATE ops_approval_requests SET state='cancelled',decided_at_ms=?1 WHERE request_id=?2",
                        params![command.now_ms, command.request_id.into_uuid().as_bytes()],
                    )?;
                    approval_versions::increment(tx, command.request_id)?;
                    return Ok((
                        workspace,
                        ApprovalDecisionOutcome {
                            request: load_request_row(tx, command.request_id)?
                                .ok_or(OperationsError::Corrupt("approval request"))?
                                .record(),
                            admission: None,
                        },
                    ));
                }
                ApprovalDecision::Approve => ApprovalRequestState::Approved,
                ApprovalDecision::Reject => ApprovalRequestState::Rejected,
                ApprovalDecision::Expire | ApprovalDecision::Supersede => {
                    return Err(OperationsError::Conflict("approval decision kind"));
                }
            };
            if command.now_ms >= row.expires_at_ms {
                return Err(OperationsError::Conflict("approval request expired"));
            }
            tx.execute(
                "UPDATE ops_approval_requests SET state=?1,decided_at_ms=?2,decided_by=?3 WHERE request_id=?4",
                params![
                    state_text(next_state)
                        .ok_or(OperationsError::Corrupt("approval request state"))?,
                    command.now_ms,
                    command.decided_by.as_bytes(),
                    command.request_id.into_uuid().as_bytes(),
                ],
            )?;
            approval_versions::increment(tx, command.request_id)?;
            let admission = if next_state == ApprovalRequestState::Approved {
                let issuer =
                    load_issuer(tx)?.ok_or(OperationsError::Conflict("admission issuer"))?;
                Some(issue_admission(
                    tx,
                    command.request_id,
                    workspace,
                    command.now_ms,
                    &issuer,
                )?)
            } else {
                None
            };
            Ok((
                workspace,
                ApprovalDecisionOutcome {
                    request: load_request_row(tx, command.request_id)?
                        .ok_or(OperationsError::Corrupt("approval request"))?
                        .record(),
                    admission,
                },
            ))
        })
    }

    /// Read one exact approval request by its durable identity.
    ///
    /// # Errors
    /// Returns storage or corruption failures while decoding the durable row.
    pub fn approval_request(
        &self,
        request_id: Uuid,
    ) -> Result<Option<ApprovalRequestRecord>, OperationsError> {
        load_request_row(&self.connection, ApprovalRequestId::from_uuid(request_id))
            .map(|row| row.map(|row| row.record()))
    }

    /// Read an exact request and its historical issuance in one read snapshot.
    /// Issuance metadata does not assert that an admission remains usable.
    ///
    /// # Errors
    /// Returns storage or corruption failures without mutating either record.
    pub fn approval_request_snapshot(
        &self,
        request_id: Uuid,
    ) -> Result<Option<ApprovalDecisionOutcome>, OperationsError> {
        let request_id = ApprovalRequestId::from_uuid(request_id);
        let snapshot = self.connection.unchecked_transaction()?;
        let outcome = load_request_row(&snapshot, request_id)?
            .map(|row| {
                let admission = load_admission(&snapshot, request_id)?;
                if admission
                    .as_ref()
                    .is_some_and(|value| value.workspace_id() != row.workspace)
                {
                    return Err(OperationsError::Corrupt("approval admission workspace"));
                }
                Ok(ApprovalDecisionOutcome {
                    request: row.record(),
                    admission,
                })
            })
            .transpose()?;
        snapshot.commit()?;
        Ok(outcome)
    }

    /// Explicit expiry reconciliation: expire pending requests whose window
    /// elapsed and expire issued admission envelopes past their validity.
    /// There is no background loop; the caller owns the observation clock.
    ///
    /// # Errors
    /// Rejects a negative observation time and uncertain commits.
    pub fn reconcile_approval_expiry(
        &mut self,
        now_ms: i64,
    ) -> Result<ApprovalExpiryReconciliation, OperationsError> {
        if now_ms < 0 {
            return Err(OperationsError::Bounds("approval reconcile time"));
        }
        let destination = self.destination;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut requests = Vec::with_capacity(MAX_APPROVAL_OBSERVATION_CHANGES);
        let mut request_overflow = false;
        {
            // Stream IDs from the unchanged request table while updating the
            // separate version owner. Expiry retains all-row semantics without
            // allocating a Vec proportional to the outstanding request count.
            let mut statement = tx.prepare(
                "SELECT substr(request_id,1,17),substr(workspace_id,1,17) \
                 FROM ops_approval_requests \
                 WHERE state='pending' AND expires_at_ms<=?1 \
                 ORDER BY requested_at_ms,request_id",
            )?;
            let mut rows = statement.query(params![now_ms])?;
            while let Some(row) = rows.next()? {
                let request_id =
                    ApprovalRequestId::from_uuid(Uuid::from_bytes(wire::id(row.get(0)?)?));
                let workspace_id = WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(row.get(1)?)?));
                let revision = u64::try_from(approval_versions::increment(&tx, request_id)?)
                    .map_err(|_| OperationsError::Corrupt("approval request revision"))?;
                if requests.len() < MAX_APPROVAL_OBSERVATION_CHANGES {
                    requests.push(ApprovalRequestVersionChange {
                        request_id,
                        workspace_id,
                        revision,
                    });
                } else {
                    request_overflow = true;
                }
            }
        }
        let expired_requests = tx.execute(
            "UPDATE ops_approval_requests SET state='expired',decided_at_ms=?1 WHERE state='pending' AND expires_at_ms<=?1",
            params![now_ms],
        )?;
        let expired_admissions = tx.execute(
            "UPDATE ops_admission_records SET state='expired' WHERE state='issued' AND expires_at_ms<=?1",
            params![now_ms],
        )?;
        tx.commit()
            .map_err(|source| OperationsError::CatalogCommitUnconfirmed {
                destination_id: destination,
                source,
            })?;
        let expired_requests = u64::try_from(expired_requests)
            .map_err(|_| OperationsError::Corrupt("approval expiry count"))?;
        let expired_admissions = u64::try_from(expired_admissions)
            .map_err(|_| OperationsError::Corrupt("approval expiry count"))?;
        Ok(ApprovalExpiryReconciliation {
            expired_requests,
            expired_admissions,
            requests,
            requires_resync: request_overflow || expired_admissions > 0,
        })
    }

    /// List one workspace's approval requests in submission order.
    ///
    /// # Errors
    /// Rejects an out-of-bound limit and a workspace with more matching rows
    /// than the bounded limit.
    pub fn approval_requests(
        &self,
        query: ApprovalRequestQuery,
    ) -> Result<Vec<ApprovalRequestRecord>, OperationsError> {
        if query.limit == 0 || query.limit > MAX_APPROVAL_LIST_LIMIT {
            return Err(OperationsError::Bounds("approval list limit"));
        }
        let window = catalog::signed(
            u64::try_from(query.limit)
                .map_err(|_| OperationsError::Bounds("approval list limit"))?
                + 1,
        )?;
        let rows: Vec<RawRequest> = if let Some(state) = query.state {
            let sql = request_select(
                "WHERE workspace_id=?1 AND state=?2 ORDER BY requested_at_ms,request_id LIMIT ?3",
            );
            let mut statement = self.connection.prepare(sql.as_str())?;
            statement
                .query_map(
                    params![
                        query.workspace_id.into_uuid().as_bytes(),
                        state_text(state)
                            .ok_or(OperationsError::Corrupt("approval request state"))?,
                        window,
                    ],
                    RawRequest::from_row,
                )?
                .collect::<Result<Vec<_>, _>>()?
        } else {
            let sql = request_select(
                "WHERE workspace_id=?1 ORDER BY requested_at_ms,request_id LIMIT ?2",
            );
            let mut statement = self.connection.prepare(sql.as_str())?;
            statement
                .query_map(
                    params![query.workspace_id.into_uuid().as_bytes(), window],
                    RawRequest::from_row,
                )?
                .collect::<Result<Vec<_>, _>>()?
        };
        if rows.len() > query.limit {
            return Err(OperationsError::Bounds("approval list window"));
        }
        rows.into_iter()
            .map(|raw| {
                let row = raw.decode()?;
                if row.workspace != query.workspace_id {
                    return Err(OperationsError::Corrupt("approval request workspace"));
                }
                Ok(row.record())
            })
            .collect()
    }
}

/// One policy rule and its remaining quota from the same read transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalScopeRuntimeView {
    pub scope: ApprovalAutoScope,
    pub remaining_uses: Option<u16>,
}

/// Bounded observation of the exact policy used by `submit_approval`.
/// This view carries no execution or mutation authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalPolicyRuntimeView {
    pub workspace_id: WorkspaceId,
    pub enabled: bool,
    pub revision: u64,
    pub scopes: Vec<ApprovalScopeRuntimeView>,
}

impl OperationsCatalog {
    /// Read policy identity and finite usage counts from one SQLite snapshot.
    /// Unlimited scopes deliberately do not scan approval history.
    ///
    /// # Errors
    /// Rejects malformed stored policy, invalid usage counts, and database failures.
    pub fn approval_policy_runtime(
        &mut self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<ApprovalPolicyRuntimeView>, OperationsError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)?;
        let snapshot = match load_policy(&tx, workspace_id)? {
            None => None,
            Some(policy) => {
                let mut scopes = Vec::with_capacity(policy.auto_scopes.len());
                for scope in policy.auto_scopes {
                    let remaining_uses = match scope.max_uses().get() {
                        None => None,
                        Some(limit) => {
                            let used = auto_scope_uses(&tx, workspace_id, scope.scope_id())?;
                            Some(
                                u16::try_from(u64::from(limit).saturating_sub(used)).map_err(
                                    |_| OperationsError::Corrupt("approval remaining uses"),
                                )?,
                            )
                        }
                    };
                    scopes.push(ApprovalScopeRuntimeView {
                        scope,
                        remaining_uses,
                    });
                }
                Some(ApprovalPolicyRuntimeView {
                    workspace_id,
                    enabled: policy.enabled,
                    revision: policy.revision,
                    scopes,
                })
            }
        };
        tx.commit()?;
        Ok(snapshot)
    }

    /// Stop future automatic decisions and return the bounded committed policy
    /// observations. Existing requests/admissions are not rewritten: execution
    /// cancellation remains the execution owner's job. Repeated stop calls do
    /// not consume revisions of already disabled policies.
    ///
    /// # Errors
    /// Rejects invalid clocks, revision overflow, database failures and uncertain commits.
    pub fn disable_automatic_approval_observed(
        &mut self,
        workspace_id: Option<WorkspaceId>,
        now_ms: i64,
    ) -> Result<ApprovalPolicyDisableOutcome, OperationsError> {
        if now_ms < 0 {
            return Err(OperationsError::Bounds("approval stop time"));
        }
        let workspace = workspace_id.map(|id| id.into_uuid().as_bytes().to_vec());
        let destination = self.destination;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let overflow: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM ops_approval_policy
             WHERE enabled=1 AND (?1 IS NULL OR workspace_id=?1) AND policy_revision>=?2)",
            params![workspace, i64::MAX],
            |row| row.get(0),
        )?;
        if overflow {
            return Err(OperationsError::Bounds("approval policy revision"));
        }
        let mut policies = Vec::with_capacity(MAX_APPROVAL_OBSERVATION_CHANGES);
        let mut policy_overflow = false;
        {
            let mut statement = tx.prepare(
                "SELECT substr(workspace_id,1,17),policy_revision \
                 FROM ops_approval_policy \
                 WHERE enabled=1 AND (?1 IS NULL OR workspace_id=?1) \
                 ORDER BY workspace_id",
            )?;
            let mut rows = statement.query(params![workspace])?;
            while let Some(row) = rows.next()? {
                let workspace_id = WorkspaceId::from_uuid(Uuid::from_bytes(wire::id(row.get(0)?)?));
                let current: i64 = row.get(1)?;
                if current < 1 {
                    return Err(OperationsError::Corrupt("approval policy revision"));
                }
                let revision = u64::try_from(current)
                    .map_err(|_| OperationsError::Corrupt("approval policy revision"))?
                    .checked_add(1)
                    .ok_or(OperationsError::Bounds("approval policy revision"))?;
                if policies.len() < MAX_APPROVAL_OBSERVATION_CHANGES {
                    policies.push(ApprovalPolicyVersionChange {
                        workspace_id,
                        revision,
                    });
                } else {
                    policy_overflow = true;
                }
            }
        }
        let changed = tx.execute(
            "UPDATE ops_approval_policy SET enabled=0,policy_revision=policy_revision+1,
             updated_at_ms=MAX(updated_at_ms,?2)
             WHERE enabled=1 AND (?1 IS NULL OR workspace_id=?1)",
            params![workspace, now_ms],
        )?;
        tx.commit()
            .map_err(|source| OperationsError::CatalogCommitUnconfirmed {
                destination_id: destination,
                source,
            })?;
        Ok(ApprovalPolicyDisableOutcome {
            changed,
            policies,
            requires_resync: policy_overflow,
        })
    }

    /// Stop future automatic decisions for one Workspace, or for all Workspaces
    /// on an explicit operator emergency stop. Existing requests/admissions are
    /// not rewritten: execution cancellation remains the execution owner's job.
    /// Repeated stop calls do not consume revisions of already disabled policies.
    ///
    /// # Errors
    /// Rejects invalid clocks, revision overflow, database failures and uncertain commits.
    pub fn disable_automatic_approval(
        &mut self,
        workspace_id: Option<WorkspaceId>,
        now_ms: i64,
    ) -> Result<usize, OperationsError> {
        Ok(self
            .disable_automatic_approval_observed(workspace_id, now_ms)?
            .changed())
    }
}

/// Compile-level shape check: an O-issued issuance constructs the exact
/// forge-domain grant that `forge_control::ExecutionAdmissionEnvelope`
/// carries. The request fence is server-derived on the Forge side; this test
/// supplies a representative fence to prove the type-level contract.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_scope_wire_requires_explicit_unlimited_and_preserves_finite()
    -> Result<(), Box<dyn std::error::Error>> {
        let finite = policy_rule(0xAB51, 1000)?;
        let unlimited = ApprovalAutoScope::with_use_limit(
            Uuid::from_u128(0xAB52),
            finite.action(),
            finite.permission_ceiling(),
            AutoApprovalUseLimit::unlimited(),
            finite.expires_at_ms(),
        )?;
        let scopes = vec![finite, unlimited];
        let encoded = encode_auto_scopes(&scopes)?;
        assert_eq!(decode_auto_scopes(&encoded)?, scopes);
        let mut wire: serde_json::Value = serde_json::from_str(&encoded)?;
        assert_eq!(wire[0]["max_uses"], serde_json::json!(10));
        assert_eq!(wire[1]["max_uses"], serde_json::Value::Null);
        let first = wire[0].as_object_mut().ok_or("scope object missing")?;
        assert!(first.remove("max_uses").is_some());
        assert!(decode_auto_scopes(&serde_json::to_string(&wire)?).is_err());
        for invalid in [0, 1001, -1] {
            wire[0]["max_uses"] = serde_json::json!(invalid);
            assert!(decode_auto_scopes(&serde_json::to_string(&wire)?).is_err());
        }
        Ok(())
    }
    use forge_domain::{
        ActivityId, AgentInstanceId, AgentInstanceRevision, AgentSessionId, DagNodeId,
        DagNodeRevision, DagRevisionNumber, ExecutionId, ExecutionRequestDigest, ForgeBackendId,
        GoalId, GoalNodeAttribution, GoalRevision, OrchestrationIssuerId, WorkspaceRevision,
    };

    struct Fixture {
        root: std::path::PathBuf,
        catalog: OperationsCatalog,
        workspace: WorkspaceId,
    }

    fn approval_fixture(name: &str, seed: u128) -> Result<Fixture, Box<dyn std::error::Error>> {
        let root =
            std::env::temp_dir().join(format!("orchestration-approval-{name}-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let catalog =
            OperationsCatalog::open(&root.join("catalog.sqlite3"), Uuid::from_u128(seed))?;
        let workspace = WorkspaceId::from_uuid(Uuid::from_u128(seed + 3));
        Ok(Fixture {
            root,
            catalog,
            workspace,
        })
    }

    fn policy_rule(
        scope_seed: u128,
        expires_at_ms: i64,
    ) -> Result<ApprovalAutoScope, Box<dyn std::error::Error>> {
        Ok(ApprovalAutoScope::new(
            Uuid::from_u128(scope_seed),
            ExecutionActionClass::DirectStructured,
            ExecutionPermissions::new(true, false, false),
            10,
            expires_at_ms,
        )?)
    }

    fn submit_command(fixture: &Fixture, request_seed: u128, now_ms: i64) -> SubmitApprovalCommand {
        SubmitApprovalCommand {
            request_id: ApprovalRequestId::from_uuid(Uuid::from_u128(request_seed)),
            workspace_id: fixture.workspace,
            action: ExecutionActionClass::DirectStructured,
            request_digest: [0xA5; 32],
            principal: AuthenticatedPrincipal::TrustedStdio,
            session_id: Uuid::from_u128(request_seed + 1),
            requested_permissions: ExecutionPermissions::new(true, false, false),
            ttl_ms: 60_000,
            now_ms,
        }
    }

    fn timestamp_ms(
        value: i64,
    ) -> Result<forge_domain::TimestampMillis, Box<dyn std::error::Error>> {
        Ok(forge_domain::TimestampMillis::new(value)?)
    }

    const ISSUER_SEED: u128 = 0xEE10;
    const BACKEND_SEED: u128 = 0xEE20;

    fn configure_issuer(fixture: &mut Fixture) -> Result<(), Box<dyn std::error::Error>> {
        let _issued =
            fixture
                .catalog
                .configure_admission_issuer(ConfigureAdmissionIssuerCommand {
                    issuer: OrchestrationIssuerId::from_uuid(Uuid::from_u128(ISSUER_SEED)),
                    backend_id: ForgeBackendId::from_uuid(Uuid::from_u128(BACKEND_SEED)),
                    admission_ttl_ms: 300_000,
                    now_ms: 0,
                })?;
        Ok(())
    }

    #[test]
    fn policy_set_get_round_trips_through_cas_revisions() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut fixture = approval_fixture("cas", 0xE100)?;
        assert!(
            fixture
                .catalog
                .approval_policy(fixture.workspace)?
                .is_none()
        );
        let created = fixture
            .catalog
            .set_approval_policy(SetApprovalPolicyCommand {
                workspace_id: fixture.workspace,
                enabled: true,
                auto_scopes: vec![policy_rule(0xE1F0, 10_000)?],
                expected_revision: None,
                now_ms: 100,
            })?;
        assert!(created.enabled());
        assert_eq!(created.revision(), 1);
        assert_eq!(created.auto_scopes().len(), 1);
        assert!(matches!(
            fixture
                .catalog
                .set_approval_policy(SetApprovalPolicyCommand {
                    workspace_id: fixture.workspace,
                    enabled: true,
                    auto_scopes: vec![policy_rule(0xE1F0, 20_000)?],
                    expected_revision: None,
                    now_ms: 200,
                }),
            Err(OperationsError::Conflict("approval policy revision"))
        ));
        assert!(matches!(
            fixture
                .catalog
                .set_approval_policy(SetApprovalPolicyCommand {
                    workspace_id: fixture.workspace,
                    enabled: true,
                    auto_scopes: vec![],
                    expected_revision: Some(7),
                    now_ms: 200,
                }),
            Err(OperationsError::Conflict("approval policy revision"))
        ));
        let replaced = fixture
            .catalog
            .set_approval_policy(SetApprovalPolicyCommand {
                workspace_id: fixture.workspace,
                enabled: true,
                auto_scopes: vec![policy_rule(0xE1F0, 20_000)?],
                expected_revision: Some(1),
                now_ms: 200,
            })?;
        assert_eq!(replaced.revision(), 2);
        assert_eq!(replaced.created_at_ms(), 100);
        assert_eq!(replaced.updated_at_ms(), 200);
        let readback = fixture
            .catalog
            .approval_policy(fixture.workspace)?
            .ok_or("policy readback")?;
        assert_eq!(readback, replaced);
        assert!(matches!(
            fixture
                .catalog
                .set_approval_policy(SetApprovalPolicyCommand {
                    workspace_id: fixture.workspace,
                    enabled: false,
                    auto_scopes: vec![policy_rule(0xE1F0, 20_000)?; 17],
                    expected_revision: Some(2),
                    now_ms: 300,
                }),
            Err(OperationsError::Bounds("approval auto scopes"))
        ));
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn auto_scope_match_approves_and_issues_one_envelope() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut fixture = approval_fixture("auto", 0xE200)?;
        configure_issuer(&mut fixture)?;
        let _policy = fixture
            .catalog
            .set_approval_policy(SetApprovalPolicyCommand {
                workspace_id: fixture.workspace,
                enabled: true,
                auto_scopes: vec![policy_rule(0xE2F0, 10_000)?],
                expected_revision: None,
                now_ms: 0,
            })?;
        let submission = fixture
            .catalog
            .submit_approval(submit_command(&fixture, 0xE210, 100))?;
        assert!(!submission.replay());
        assert_eq!(submission.request().state(), ApprovalRequestState::Approved);
        assert_eq!(
            submission.request().auto_scope_id(),
            Some(Uuid::from_u128(0xE2F0))
        );
        assert_eq!(submission.request().policy_revision(), Some(1));
        assert_eq!(submission.request().revision(), 1);
        let admission = submission.admission().ok_or("auto approval issuance")?;
        assert_eq!(
            admission.issuer(),
            OrchestrationIssuerId::from_uuid(Uuid::from_u128(ISSUER_SEED))
        );
        assert_eq!(
            admission.backend_id(),
            ForgeBackendId::from_uuid(Uuid::from_u128(BACKEND_SEED))
        );
        assert_eq!(admission.issued_at_ms(), 100);
        assert_eq!(admission.expires_at_ms(), 300_100);
        assert_eq!(
            admission.admission_id(),
            admission_id_for(submission.request().request_id())
        );
        let replay = fixture
            .catalog
            .submit_approval(submit_command(&fixture, 0xE210, 100))?;
        assert!(replay.replay());
        assert_eq!(replay.admission(), submission.admission());
        assert_eq!(replay.request().revision(), 1);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn submissions_without_a_matching_scope_stay_pending() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut fixture = approval_fixture("pending", 0xE300)?;
        let _policy = fixture
            .catalog
            .set_approval_policy(SetApprovalPolicyCommand {
                workspace_id: fixture.workspace,
                enabled: true,
                auto_scopes: vec![policy_rule(0xE3F0, 10_000)?],
                expected_revision: None,
                now_ms: 0,
            })?;
        let mut command = submit_command(&fixture, 0xE310, 100);
        command.action = ExecutionActionClass::ShellEscape;
        let submission = fixture.catalog.submit_approval(command)?;
        assert_eq!(submission.request().state(), ApprovalRequestState::Pending);
        assert!(submission.admission().is_none());
        let mut outside = submit_command(&fixture, 0xE311, 100);
        outside.requested_permissions = ExecutionPermissions::new(true, true, false);
        let submission = fixture.catalog.submit_approval(outside)?;
        assert_eq!(submission.request().state(), ApprovalRequestState::Pending);
        let mut late = submit_command(&fixture, 0xE312, 10_000);
        late.now_ms = 10_000;
        let submission = fixture.catalog.submit_approval(late)?;
        assert_eq!(submission.request().state(), ApprovalRequestState::Pending);
        let mut no_policy = approval_fixture("pending-nopolicy", 0xE3A0)?;
        let submission = no_policy
            .catalog
            .submit_approval(submit_command(&no_policy, 0xE3B0, 100))?;
        assert_eq!(submission.request().state(), ApprovalRequestState::Pending);
        assert_eq!(submission.request().policy_revision(), None);
        std::fs::remove_dir_all(fixture.root)?;
        std::fs::remove_dir_all(no_policy.root)?;
        Ok(())
    }

    #[test]
    fn decide_workspace_fence_preserves_pending_request() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut fixture = approval_fixture("decide-workspace-fence", 0xE470)?;
        configure_issuer(&mut fixture)?;
        let submission = fixture
            .catalog
            .submit_approval(submit_command(&fixture, 0xE471, 100))?;
        let request_id = submission.request().request_id();
        assert!(matches!(
            fixture.catalog.decide_approval(DecideApprovalCommand {
                request_id,
                workspace_id: WorkspaceId::from_uuid(Uuid::from_u128(0xE4FE)),
                decision: ApprovalDecision::Approve,
                decided_by: Uuid::from_u128(0xE4D0),
                now_ms: 150,
            }),
            Err(OperationsError::Conflict("approval request workspace"))
        ));
        let readback = fixture
            .catalog
            .approval_request(request_id.into_uuid())?
            .ok_or("workspace-fenced request readback")?;
        assert_eq!(readback.state(), ApprovalRequestState::Pending);
        assert_eq!(readback.revision(), 1);

        let approved = fixture.catalog.decide_approval(DecideApprovalCommand {
            request_id,
            workspace_id: fixture.workspace,
            decision: ApprovalDecision::Approve,
            decided_by: Uuid::from_u128(0xE4D0),
            now_ms: 150,
        })?;
        assert_eq!(approved.request().revision(), 2);
        assert!(approved.admission().is_some());
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn decide_approves_with_issuance_and_rejects_without() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut fixture = approval_fixture("decide", 0xE400)?;
        configure_issuer(&mut fixture)?;
        let submission = fixture
            .catalog
            .submit_approval(submit_command(&fixture, 0xE410, 100))?;
        let request_id = submission.request().request_id();
        let unknown = fixture.catalog.decide_approval(DecideApprovalCommand {
            request_id: ApprovalRequestId::from_uuid(Uuid::from_u128(0xE4FF)),
            workspace_id: fixture.workspace,
            decision: ApprovalDecision::Approve,
            decided_by: Uuid::from_u128(0xE4D0),
            now_ms: 150,
        });
        assert!(matches!(
            unknown,
            Err(OperationsError::Conflict("approval request missing"))
        ));
        let approved = fixture.catalog.decide_approval(DecideApprovalCommand {
            request_id,
            workspace_id: fixture.workspace,
            decision: ApprovalDecision::Approve,
            decided_by: Uuid::from_u128(0xE4D0),
            now_ms: 150,
        })?;
        assert_eq!(approved.request().state(), ApprovalRequestState::Approved);
        assert_eq!(approved.request().revision(), 2);
        assert_eq!(
            approved.request().decided_by(),
            Some(Uuid::from_u128(0xE4D0))
        );
        assert_eq!(approved.request().decided_at_ms(), Some(150));
        let admission = approved.admission().ok_or("approved issuance")?;
        assert_eq!(admission.issued_at_ms(), 150);
        assert_eq!(admission.expires_at_ms(), 300_150);
        assert!(matches!(
            fixture.catalog.decide_approval(DecideApprovalCommand {
                request_id,
                workspace_id: fixture.workspace,
                decision: ApprovalDecision::Reject,
                decided_by: Uuid::from_u128(0xE4D0),
                now_ms: 160,
            }),
            Err(OperationsError::Conflict("approval request decided"))
        ));
        let second = fixture
            .catalog
            .submit_approval(submit_command(&fixture, 0xE411, 200))?;
        let rejected = fixture.catalog.decide_approval(DecideApprovalCommand {
            request_id: second.request().request_id(),
            workspace_id: fixture.workspace,
            decision: ApprovalDecision::Reject,
            decided_by: Uuid::from_u128(0xE4D0),
            now_ms: 250,
        })?;
        assert_eq!(rejected.request().state(), ApprovalRequestState::Rejected);
        assert_eq!(rejected.request().revision(), 2);
        assert!(rejected.admission().is_none());
        let cancelled = fixture
            .catalog
            .submit_approval(submit_command(&fixture, 0xE412, 300))?;
        let cancelled = fixture.catalog.decide_approval(DecideApprovalCommand {
            request_id: cancelled.request().request_id(),
            workspace_id: fixture.workspace,
            decision: ApprovalDecision::Cancel,
            decided_by: Uuid::nil(),
            now_ms: 350,
        })?;
        assert_eq!(cancelled.request().state(), ApprovalRequestState::Cancelled);
        assert_eq!(cancelled.request().revision(), 2);
        assert!(cancelled.request().decided_by().is_none());
        assert!(matches!(
            fixture.catalog.decide_approval(DecideApprovalCommand {
                request_id: ApprovalRequestId::from_uuid(Uuid::from_u128(0xE413)),
                workspace_id: fixture.workspace,
                decision: ApprovalDecision::Expire,
                decided_by: Uuid::from_u128(0xE4D0),
                now_ms: 400,
            }),
            Err(OperationsError::Conflict("approval request missing"))
        ));
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn expiry_reconcile_expires_pending_requests_and_issued_envelopes()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = approval_fixture("expiry", 0xE500)?;
        configure_issuer(&mut fixture)?;
        let _policy = fixture
            .catalog
            .set_approval_policy(SetApprovalPolicyCommand {
                workspace_id: fixture.workspace,
                enabled: true,
                auto_scopes: vec![policy_rule(0xE5F0, 10_000)?],
                expected_revision: None,
                now_ms: 0,
            })?;
        let auto = fixture
            .catalog
            .submit_approval(submit_command(&fixture, 0xE510, 100))?;
        let mut outside_scope = submit_command(&fixture, 0xE511, 100);
        outside_scope.requested_permissions = ExecutionPermissions::new(true, true, true);
        let pending = fixture.catalog.submit_approval(outside_scope)?;
        assert_eq!(pending.request().state(), ApprovalRequestState::Pending);
        let reconciliation = fixture.catalog.reconcile_approval_expiry(100_500)?;
        assert_eq!(reconciliation.expired_requests(), 1);
        assert_eq!(
            reconciliation.expired_admissions(),
            0,
            "the issued envelope outlives the expired pending request"
        );
        assert!(!reconciliation.requires_resync());
        let later = fixture.catalog.reconcile_approval_expiry(300_200)?;
        assert_eq!(later.expired_requests(), 0);
        assert_eq!(later.expired_admissions(), 1);
        let changes_before_read = fixture.catalog.connection.total_changes();
        let historical = fixture
            .catalog
            .approval_request_snapshot(auto.request().request_id().into_uuid())?
            .ok_or("historical approval snapshot")?;
        assert_eq!(historical.request(), auto.request());
        assert_eq!(historical.admission(), auto.admission());
        assert_eq!(
            fixture.catalog.connection.total_changes(),
            changes_before_read
        );
        assert!(later.requests().is_empty());
        assert!(later.requires_resync());
        let expired_request = fixture.catalog.approval_requests(ApprovalRequestQuery {
            workspace_id: fixture.workspace,
            state: Some(ApprovalRequestState::Expired),
            limit: 64,
        })?;
        assert_eq!(expired_request.len(), 1);
        assert_eq!(expired_request[0].revision(), 2);
        assert_eq!(
            expired_request[0].request_id(),
            pending.request().request_id()
        );
        assert_eq!(
            expired_request[0].decided_at_ms(),
            Some(100_500),
            "expiry records the reconciliation observation time"
        );
        assert!(matches!(
            fixture.catalog.decide_approval(DecideApprovalCommand {
                request_id: pending.request().request_id(),
                workspace_id: fixture.workspace,
                decision: ApprovalDecision::Approve,
                decided_by: Uuid::from_u128(0xE5D0),
                now_ms: 100_600,
            }),
            Err(OperationsError::Conflict("approval request decided"))
        ));
        let still_approved = fixture.catalog.approval_requests(ApprovalRequestQuery {
            workspace_id: fixture.workspace,
            state: Some(ApprovalRequestState::Approved),
            limit: 64,
        })?;
        assert_eq!(still_approved.len(), 1);
        assert_eq!(still_approved[0].request_id(), auto.request().request_id());
        assert!(matches!(
            fixture.catalog.reconcile_approval_expiry(-1),
            Err(OperationsError::Bounds("approval reconcile time"))
        ));
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn expiry_observation_bounds_changes_but_expires_every_pending_request()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = approval_fixture("expiry-observation", 0xE550)?;
        for index in 0_u128..65 {
            let _ =
                fixture
                    .catalog
                    .submit_approval(submit_command(&fixture, 0xE600 + index, 100))?;
        }
        let reconciliation = fixture.catalog.reconcile_approval_expiry(100_000)?;
        assert_eq!(reconciliation.expired_requests(), 65);
        assert_eq!(reconciliation.expired_admissions(), 0);
        assert_eq!(
            reconciliation.requests().len(),
            MAX_APPROVAL_OBSERVATION_CHANGES
        );
        assert!(
            reconciliation
                .requests()
                .iter()
                .all(|change| change.workspace_id() == fixture.workspace && change.revision() == 2)
        );
        assert!(reconciliation.requires_resync());
        let expired_count: i64 = fixture.catalog.connection.query_row(
            "SELECT count(*) FROM ops_approval_requests WHERE state='expired'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(expired_count, 65);

        let second = fixture.catalog.reconcile_approval_expiry(100_000)?;
        assert_eq!(second.expired_requests(), 0);
        assert_eq!(second.expired_admissions(), 0);
        assert!(second.requests().is_empty());
        assert!(!second.requires_resync());
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn issuance_fails_closed_without_issuer_configuration() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut fixture = approval_fixture("no-issuer", 0xE600)?;
        let _policy = fixture
            .catalog
            .set_approval_policy(SetApprovalPolicyCommand {
                workspace_id: fixture.workspace,
                enabled: true,
                auto_scopes: vec![policy_rule(0xE6F0, 10_000)?],
                expected_revision: None,
                now_ms: 0,
            })?;
        assert!(matches!(
            fixture
                .catalog
                .submit_approval(submit_command(&fixture, 0xE610, 100)),
            Err(OperationsError::Conflict("admission issuer"))
        ));
        assert!(fixture.catalog.admission_issuer_config()?.is_none());
        assert!(matches!(
            fixture
                .catalog
                .configure_admission_issuer(ConfigureAdmissionIssuerCommand {
                    issuer: OrchestrationIssuerId::from_uuid(Uuid::from_u128(ISSUER_SEED)),
                    backend_id: ForgeBackendId::from_uuid(Uuid::from_u128(BACKEND_SEED)),
                    admission_ttl_ms: 100,
                    now_ms: 0,
                }),
            Err(OperationsError::Bounds("admission ttl"))
        ));
        let configured =
            fixture
                .catalog
                .configure_admission_issuer(ConfigureAdmissionIssuerCommand {
                    issuer: OrchestrationIssuerId::from_uuid(Uuid::from_u128(ISSUER_SEED)),
                    backend_id: ForgeBackendId::from_uuid(Uuid::from_u128(BACKEND_SEED)),
                    admission_ttl_ms: 300_000,
                    now_ms: 50,
                })?;
        assert_eq!(configured.updated_at_ms(), 50);
        let submission = fixture
            .catalog
            .submit_approval(submit_command(&fixture, 0xE610, 100))?;
        assert!(submission.admission().is_some());
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn approval_requests_list_is_bounded_and_state_filtered()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = approval_fixture("list", 0xE700)?;
        configure_issuer(&mut fixture)?;
        for seed in 0..3u128 {
            let _submission = fixture.catalog.submit_approval(submit_command(
                &fixture,
                0xE710 + seed,
                100 + i64::try_from(seed)?,
            ))?;
        }
        let all = fixture.catalog.approval_requests(ApprovalRequestQuery {
            workspace_id: fixture.workspace,
            state: None,
            limit: 64,
        })?;
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].requested_at_ms(), 100);
        assert_eq!(all[2].requested_at_ms(), 102);
        assert!(matches!(
            fixture.catalog.approval_requests(ApprovalRequestQuery {
                workspace_id: fixture.workspace,
                state: None,
                limit: 2,
            }),
            Err(OperationsError::Bounds("approval list window"))
        ));
        let exact = fixture.catalog.approval_requests(ApprovalRequestQuery {
            workspace_id: fixture.workspace,
            state: None,
            limit: 3,
        })?;
        assert_eq!(exact.len(), 3);
        assert!(matches!(
            fixture.catalog.approval_requests(ApprovalRequestQuery {
                workspace_id: fixture.workspace,
                state: None,
                limit: 0,
            }),
            Err(OperationsError::Bounds("approval list limit"))
        ));
        let other = approval_fixture("list-other", 0xE7A0)?;
        assert!(
            other
                .catalog
                .approval_requests(ApprovalRequestQuery {
                    workspace_id: other.workspace,
                    state: None,
                    limit: 64,
                })?
                .is_empty()
        );
        std::fs::remove_dir_all(fixture.root)?;
        std::fs::remove_dir_all(other.root)?;
        Ok(())
    }

    #[test]
    fn issued_envelope_constructs_the_exact_forge_domain_grant()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = approval_fixture("envelope", 0xE800)?;
        configure_issuer(&mut fixture)?;
        let _policy = fixture
            .catalog
            .set_approval_policy(SetApprovalPolicyCommand {
                workspace_id: fixture.workspace,
                enabled: true,
                auto_scopes: vec![policy_rule(0xE8F0, 10_000)?],
                expected_revision: None,
                now_ms: 0,
            })?;
        let submission = fixture
            .catalog
            .submit_approval(submit_command(&fixture, 0xE810, 100))?;
        let admission = submission.admission().ok_or("envelope issuance")?;
        let request = forge_domain::ExecutionAdmissionRequest::new(
            forge_domain::ExecutionAdmissionScope::new(
                admission.backend_id(),
                fixture.workspace,
                WorkspaceRevision::INITIAL,
            ),
            ActivityId::from_uuid(Uuid::from_u128(0xE8A0)),
            GoalNodeAttribution::new(
                GoalId::from_uuid(Uuid::from_u128(0xE8B0)),
                GoalRevision::INITIAL,
                DagRevisionNumber::INITIAL,
                DagNodeId::from_uuid(Uuid::from_u128(0xE8C0)),
                DagNodeRevision::INITIAL,
            ),
            forge_domain::ExecutionAdmissionActor::new(
                AgentInstanceId::from_uuid(Uuid::from_u128(0xE8D0)),
                AgentInstanceRevision::INITIAL,
                AgentSessionId::from_uuid(Uuid::from_u128(0xE8E0)),
            ),
            ExecutionId::from_uuid(Uuid::from_u128(0xE890)),
            ExecutionRequestDigest::from_bytes([0x5A; ExecutionRequestDigest::BYTE_LEN]),
        );
        let grant = admission.envelope_grant(request)?;
        assert_eq!(grant.issuer(), admission.issuer());
        assert_eq!(grant.request(), request);
        assert_eq!(grant.issued_at().get(), admission.issued_at_ms());
        assert_eq!(grant.expires_at().get(), admission.expires_at_ms());
        assert!(
            grant
                .validate_for(admission.issuer(), &request, timestamp_ms(150)?,)
                .is_ok()
        );
        assert_eq!(
            grant.validate_for(
                admission.issuer(),
                &request,
                timestamp_ms(admission.expires_at_ms())?,
            ),
            Err(ExecutionAdmissionError::Expired)
        );
        let substituted = forge_domain::ExecutionAdmissionRequest::new(
            forge_domain::ExecutionAdmissionScope::new(
                ForgeBackendId::from_uuid(Uuid::from_u128(0xE821)),
                fixture.workspace,
                WorkspaceRevision::INITIAL,
            ),
            ActivityId::from_uuid(Uuid::from_u128(0xE8A0)),
            GoalNodeAttribution::new(
                GoalId::from_uuid(Uuid::from_u128(0xE8B0)),
                GoalRevision::INITIAL,
                DagRevisionNumber::INITIAL,
                DagNodeId::from_uuid(Uuid::from_u128(0xE8C0)),
                DagNodeRevision::INITIAL,
            ),
            forge_domain::ExecutionAdmissionActor::new(
                AgentInstanceId::from_uuid(Uuid::from_u128(0xE8D0)),
                AgentInstanceRevision::INITIAL,
                AgentSessionId::from_uuid(Uuid::from_u128(0xE8E0)),
            ),
            ExecutionId::from_uuid(Uuid::from_u128(0xE890)),
            ExecutionRequestDigest::from_bytes([0x5A; ExecutionRequestDigest::BYTE_LEN]),
        );
        assert_eq!(
            grant.validate_for(admission.issuer(), &substituted, timestamp_ms(150)?,),
            Err(ExecutionAdmissionError::RequestMismatch)
        );
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn replay_conflicts_reject_substituted_submission_content()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = approval_fixture("replay", 0xE900)?;
        configure_issuer(&mut fixture)?;
        let command = submit_command(&fixture, 0xE910, 100);
        let _first = fixture.catalog.submit_approval(command.clone())?;
        let mut substituted = command.clone();
        substituted.request_digest = [0x11; 32];
        assert!(matches!(
            fixture.catalog.submit_approval(substituted),
            Err(OperationsError::Conflict("approval request identity"))
        ));
        let mut foreign_session = command;
        foreign_session.session_id = Uuid::from_u128(0xE9F1);
        assert!(matches!(
            fixture.catalog.submit_approval(foreign_session),
            Err(OperationsError::Conflict("approval request identity"))
        ));
        assert!(matches!(
            fixture.catalog.submit_approval(SubmitApprovalCommand {
                ttl_ms: 10,
                ..submit_command(&fixture, 0xE910, 100)
            }),
            Err(OperationsError::Bounds("approval request"))
        ));
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn auto_scope_uses_are_exhausted_by_prior_approvals() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut fixture = approval_fixture("uses", 0xEA00)?;
        configure_issuer(&mut fixture)?;
        let scope = ApprovalAutoScope::new(
            Uuid::from_u128(0xEAF0),
            ExecutionActionClass::DirectStructured,
            ExecutionPermissions::new(true, false, false),
            1,
            100_000,
        )?;
        let _policy = fixture
            .catalog
            .set_approval_policy(SetApprovalPolicyCommand {
                workspace_id: fixture.workspace,
                enabled: true,
                auto_scopes: vec![scope],
                expected_revision: None,
                now_ms: 0,
            })?;
        let first = fixture
            .catalog
            .submit_approval(submit_command(&fixture, 0xEA10, 100))?;
        assert_eq!(first.request().state(), ApprovalRequestState::Approved);
        let second = fixture
            .catalog
            .submit_approval(submit_command(&fixture, 0xEA11, 200))?;
        assert_eq!(second.request().state(), ApprovalRequestState::Pending);
        let reconciled = fixture.catalog.reconcile_approval_expiry(50)?;
        assert_eq!(reconciled.expired_requests(), 0);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn unlimited_scopes_keep_policy_disable_and_expiry_fences()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = approval_fixture("unlimited-fences", 0xFB00)?;
        configure_issuer(&mut fixture)?;
        let scope = ApprovalAutoScope::with_use_limit(
            Uuid::from_u128(0xFBF0),
            ExecutionActionClass::DirectStructured,
            ExecutionPermissions::new(true, false, false),
            AutoApprovalUseLimit::unlimited(),
            100_000,
        )?;
        let policy = fixture
            .catalog
            .set_approval_policy(SetApprovalPolicyCommand {
                workspace_id: fixture.workspace,
                enabled: true,
                auto_scopes: vec![scope.clone()],
                expected_revision: None,
                now_ms: 0,
            })?;
        for index in 0_u32..32 {
            let command =
                submit_command(&fixture, 0xFB10 + u128::from(index), 100 + i64::from(index));
            let result = fixture.catalog.submit_approval(command)?;
            assert_eq!(result.request().state(), ApprovalRequestState::Approved);
        }
        let runtime = fixture
            .catalog
            .approval_policy_runtime(fixture.workspace)?
            .ok_or("policy runtime missing")?;
        assert_eq!(runtime.revision, policy.revision);
        assert!(runtime.enabled);
        assert_eq!(runtime.scopes.len(), 1);
        assert_eq!(runtime.scopes[0].scope, scope);
        assert_eq!(runtime.scopes[0].remaining_uses, None);
        assert_eq!(
            fixture
                .catalog
                .disable_automatic_approval(Some(fixture.workspace), 200)?,
            1,
        );
        let disabled = fixture
            .catalog
            .approval_policy(fixture.workspace)?
            .ok_or("disabled policy missing")?;
        assert!(!disabled.enabled);
        assert_eq!(disabled.revision, policy.revision + 1);
        assert_eq!(disabled.auto_scopes, vec![scope.clone()]);
        assert_eq!(
            fixture
                .catalog
                .disable_automatic_approval(Some(fixture.workspace), 200)?,
            0,
        );
        assert_eq!(
            fixture
                .catalog
                .approval_policy(fixture.workspace)?
                .ok_or("policy missing")?
                .revision,
            disabled.revision,
        );
        let command = submit_command(&fixture, 0xFBC0, 201);
        assert_eq!(
            fixture.catalog.submit_approval(command)?.request().state(),
            ApprovalRequestState::Pending,
        );
        let _enabled = fixture
            .catalog
            .set_approval_policy(SetApprovalPolicyCommand {
                workspace_id: fixture.workspace,
                enabled: true,
                auto_scopes: vec![scope],
                expected_revision: Some(disabled.revision),
                now_ms: 202,
            })?;
        let expired = submit_command(&fixture, 0xFBC1, 100_000);
        assert_eq!(
            fixture.catalog.submit_approval(expired)?.request().state(),
            ApprovalRequestState::Pending,
        );
        drop(fixture.catalog);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn global_policy_disable_bounds_changes_but_updates_every_enabled_policy()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = approval_fixture("policy-disable-observation", 0xFC00)?;
        let workspaces: Vec<WorkspaceId> = (0_u128..65)
            .map(|index| WorkspaceId::from_uuid(Uuid::from_u128(0xFC10 + index)))
            .collect();
        for workspace_id in &workspaces {
            let _ = fixture
                .catalog
                .set_approval_policy(SetApprovalPolicyCommand {
                    workspace_id: *workspace_id,
                    enabled: true,
                    auto_scopes: Vec::new(),
                    expected_revision: None,
                    now_ms: 100,
                })?;
        }
        let observed = fixture
            .catalog
            .disable_automatic_approval_observed(None, 200)?;
        assert_eq!(observed.changed(), 65);
        assert_eq!(observed.policies().len(), MAX_APPROVAL_OBSERVATION_CHANGES);
        assert!(
            observed
                .policies()
                .iter()
                .all(|change| change.revision() == 2)
        );
        assert!(observed.requires_resync());
        for workspace_id in &workspaces {
            let policy = fixture
                .catalog
                .approval_policy(*workspace_id)?
                .ok_or("disabled policy missing")?;
            assert!(!policy.enabled());
            assert_eq!(policy.revision(), 2);
        }

        let second = fixture
            .catalog
            .disable_automatic_approval_observed(None, 300)?;
        assert_eq!(second.changed(), 0);
        assert!(second.policies().is_empty());
        assert!(!second.requires_resync());
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn request_version_overflow_rolls_back_decision_and_bulk_expiry()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = approval_fixture("revision-overflow-rollback", 0xED00)?;
        let ordinary = fixture
            .catalog
            .submit_approval(submit_command(&fixture, 0xED01, 100))?
            .request()
            .request_id();
        let maximal = fixture
            .catalog
            .submit_approval(submit_command(&fixture, 0xED02, 100))?
            .request()
            .request_id();
        fixture.catalog.connection.execute(
            "UPDATE ops_approval_request_versions SET revision=?1 WHERE request_id=?2",
            params![i64::MAX, maximal.into_uuid().as_bytes()],
        )?;
        assert!(matches!(
            fixture.catalog.decide_approval(DecideApprovalCommand {
                request_id: maximal,
                workspace_id: fixture.workspace,
                decision: ApprovalDecision::Reject,
                decided_by: Uuid::from_u128(0xED03),
                now_ms: 200
            }),
            Err(OperationsError::Bounds("approval request revision"))
        ));
        assert!(matches!(
            fixture.catalog.reconcile_approval_expiry(200_000),
            Err(OperationsError::Bounds("approval request revision"))
        ));
        let requests = fixture.catalog.approval_requests(ApprovalRequestQuery {
            workspace_id: fixture.workspace,
            state: None,
            limit: 64,
        })?;
        assert_eq!(requests.len(), 2);
        for request in requests {
            assert_eq!(request.state(), ApprovalRequestState::Pending);
            assert!(request.decided_at_ms().is_none() && request.decided_by().is_none());
            assert_eq!(
                request.revision(),
                if request.request_id() == ordinary {
                    1
                } else {
                    u64::try_from(i64::MAX)?
                }
            );
        }
        drop(fixture.catalog);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }

    #[test]
    fn missing_or_invalid_request_version_never_becomes_a_default_revision()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = approval_fixture("revision-corruption", 0xEE10)?;
        let request = fixture
            .catalog
            .submit_approval(submit_command(&fixture, 0xEE11, 100))?
            .request()
            .request_id();
        fixture.catalog.connection.execute(
            "DELETE FROM ops_approval_request_versions WHERE request_id=?1",
            params![request.into_uuid().as_bytes()],
        )?;
        let query = || ApprovalRequestQuery {
            workspace_id: fixture.workspace,
            state: None,
            limit: 64,
        };
        assert!(matches!(
            fixture.catalog.approval_requests(query()),
            Err(OperationsError::Corrupt("approval request revision"))
        ));
        assert!(matches!(
            fixture.catalog.reconcile_approval_expiry(200_000),
            Err(OperationsError::Corrupt(
                "approval request revision missing"
            ))
        ));
        fixture.catalog.connection.execute(
            "INSERT INTO ops_approval_request_versions(request_id,revision) VALUES(?1,1)",
            params![request.into_uuid().as_bytes()],
        )?;
        fixture
            .catalog
            .connection
            .pragma_update(None, "ignore_check_constraints", true)?;
        fixture.catalog.connection.execute(
            "UPDATE ops_approval_request_versions SET revision=0 WHERE request_id=?1",
            params![request.into_uuid().as_bytes()],
        )?;
        fixture
            .catalog
            .connection
            .pragma_update(None, "ignore_check_constraints", false)?;
        assert!(matches!(
            fixture.catalog.approval_requests(query()),
            Err(OperationsError::Corrupt("approval request revision"))
        ));
        drop(fixture.catalog);
        std::fs::remove_dir_all(fixture.root)?;
        Ok(())
    }
}
