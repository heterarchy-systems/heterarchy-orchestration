//! Append-only records of Reviewer interest expressed through a Profile-bound Session.
//!
//! These records do not establish qualification, appoint a Reviewer, freeze a
//! case pool, grant evidence access, or determine what a withdrawal does to an
//! existing case. Recorded time is audit metadata and has no policy effect.
//! Stable Profile/participation identity groups history; every receipt keeps
//! the original Session used to make that statement.

mod schema;
#[cfg(test)]
mod tests;

use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, TimestampMillis, WorkspaceId};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::{
    OperationsCatalog, OperationsError, WorkspaceParticipationId,
    operational_actor::{OperationalActor, ResolvedOperationalActor, resolve_actor},
};

const MAX_HISTORY_PAGE_SIZE: u32 = 64;

/// Statements expressed through this Profile participation's Agent Session.
/// This enum carries no permission and does not attest to an independent
/// human Agent identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewerParticipationStatement {
    Interested,
    WithdrawInterest {
        withdraws_request_id: Uuid,
        withdraws_revision: u64,
    },
}

/// One immutable, Session-attributed statement receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewerParticipationReceipt {
    workspace_id: WorkspaceId,
    participation_id: WorkspaceParticipationId,
    profile_id: Uuid,
    participant_id: Uuid,
    request_id: Uuid,
    revision: u64,
    expected_revision: u64,
    statement: ReviewerParticipationStatement,
    activity_id: ActivityId,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    display_name: String,
    recorded_at: TimestampMillis,
    replayed: bool,
}

impl ReviewerParticipationReceipt {
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn participation_id(&self) -> WorkspaceParticipationId {
        self.participation_id
    }

    #[must_use]
    pub const fn profile_id(&self) -> Uuid {
        self.profile_id
    }

    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub const fn expected_revision(&self) -> u64 {
        self.expected_revision
    }

    #[must_use]
    pub const fn statement(&self) -> ReviewerParticipationStatement {
        self.statement
    }

    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity_id
    }

    #[must_use]
    pub const fn agent_id(&self) -> AgentInstanceId {
        self.agent_id
    }

    pub const fn session_id(&self) -> AgentSessionId {
        self.session_id
    }

    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    #[must_use]
    pub const fn recorded_at(&self) -> TimestampMillis {
        self.recorded_at
    }

    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// A page of the participation's immutable interest statements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewerParticipationHistoryPage {
    records: Vec<ReviewerParticipationReceipt>,
    next_after_revision: Option<u64>,
}

impl ReviewerParticipationHistoryPage {
    #[must_use]
    pub fn records(&self) -> &[ReviewerParticipationReceipt] {
        &self.records
    }

    #[must_use]
    pub const fn next_after_revision(&self) -> Option<u64> {
        self.next_after_revision
    }
}

/// Compare-and-swap request to record an interest statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewerParticipationCommand {
    workspace_id: WorkspaceId,
    request_id: Uuid,
    expected_revision: u64,
    actor: OperationalActor,
    statement: ReviewerParticipationStatement,
}

impl ReviewerParticipationCommand {
    /// Construct a bounded statement request.
    ///
    /// `WithdrawInterest` must identify the exact current `Interested` receipt
    /// and that receipt's revision. It cannot act on a case or grant/revoke
    /// evidence access.
    ///
    /// # Errors
    /// Rejects nil identity values, invalid withdrawal targets, and native or
    /// operator actors.
    pub fn new(
        workspace_id: WorkspaceId,
        request_id: Uuid,
        expected_revision: u64,
        actor: OperationalActor,
        statement: ReviewerParticipationStatement,
    ) -> Result<Self, OperationsError> {
        validate_identity(workspace_id, request_id)?;
        validate_statement(statement, expected_revision)?;
        if matches!(actor, OperationalActor::LocalOperator) {
            return Err(OperationsError::Conflict(
                "reviewer participation requires its own Profile participant",
            ));
        }
        Ok(Self {
            workspace_id,
            request_id,
            expected_revision,
            actor,
            statement,
        })
    }

    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    #[must_use]
    pub const fn request_id(&self) -> Uuid {
        self.request_id
    }

    #[must_use]
    pub const fn expected_revision(&self) -> u64 {
        self.expected_revision
    }

    #[must_use]
    pub const fn statement(&self) -> ReviewerParticipationStatement {
        self.statement
    }
}

/// Exact schema objects for the catalog owner's additive migration.
pub(crate) const fn schema_objects() -> &'static [(&'static str, &'static str, &'static str)] {
    schema::schema_objects()
}

/// Create the schema inside the catalog owner's existing migration.
pub(crate) fn create_schema(transaction: &Transaction<'_>) -> Result<(), OperationsError> {
    schema::create_schema(transaction)
}

impl OperationsCatalog {
    /// Record an authenticated participant's interest statement using an
    /// IMMEDIATE transaction and a per-participation revision CAS.
    ///
    /// Exact retries from the original Session return the original receipt.
    /// After an unconfirmed commit, resolve the same request ID before retrying.
    ///
    /// # Errors
    /// Rejects unverified Workspaces, operators/native identities, changed
    /// request reuse, stale revisions, invalid withdrawal targets, and storage
    /// or unconfirmed-commit failures.
    pub fn record_reviewer_participation(
        &mut self,
        command: &ReviewerParticipationCommand,
    ) -> Result<ReviewerParticipationReceipt, OperationsError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        crate::teams::ensure_verified_workspace(&transaction, command.workspace_id)?;
        let actor = resolve_profile_actor(&transaction, command.workspace_id, command.actor)?;

        if let Some(receipt) =
            load_receipt_by_request(&transaction, command.workspace_id, command.request_id)?
        {
            if !receipt_matches_command(&receipt, command, &actor) {
                return Err(OperationsError::Conflict(
                    "reviewer participation request replay",
                ));
            }
            transaction.commit()?;
            return Ok(with_replay(receipt));
        }
        if request_id_exists(&transaction, command.request_id)? {
            return Err(OperationsError::Conflict(
                "reviewer participation request ID already used",
            ));
        }

        let head = load_head(&transaction, command.workspace_id, actor.participation_id)?;
        validate_transition(command, head.as_ref())?;
        let next_revision = command
            .expected_revision
            .checked_add(1)
            .ok_or(OperationsError::Bounds("reviewer participation revision"))?;
        let recorded_at = trusted_now()?;
        let receipt = receipt_from_actor(command, &actor, next_revision, recorded_at);
        insert_receipt(&transaction, &receipt)?;
        advance_head(
            &transaction,
            command.workspace_id,
            actor.participation_id,
            command.expected_revision,
            next_revision,
            command.request_id,
        )?;

        let commit_result = transaction.commit();
        finalize_commit(
            commit_result,
            receipt,
            command.workspace_id,
            command.request_id,
            || load_receipt_by_request(&self.connection, command.workspace_id, command.request_id),
            |existing| receipt_matches_command(existing, command, &actor),
        )
    }

    /// Read the exact receipt after normal caller authorization. A new Session
    /// may read history only when it resolves to the same Profile and stable
    /// Workspace participation. The receipt always returns the original
    /// Session and does not decide whether the statement applies to a case.
    ///
    /// # Errors
    /// Rejects invalid identity, unverified Workspace, unready actor, operator
    /// or native actor, and storage errors.
    pub fn reviewer_participation_request(
        &self,
        workspace_id: WorkspaceId,
        request_id: Uuid,
        actor: OperationalActor,
    ) -> Result<Option<ReviewerParticipationReceipt>, OperationsError> {
        validate_identity(workspace_id, request_id)?;
        let transaction = self.connection.unchecked_transaction()?;
        crate::teams::ensure_verified_workspace(&transaction, workspace_id)?;
        let actor = resolve_profile_actor(&transaction, workspace_id, actor)?;
        let record = load_receipt_by_request(&transaction, workspace_id, request_id)?;
        let result = record.filter(|record| {
            record.participation_id == actor.participation_id
                && record.profile_id == actor.profile_id
        });
        transaction.commit()?;
        Ok(result)
    }

    /// Read an ascending, bounded page of this participant's own immutable
    /// interest statements. It returns historical facts only; it does not
    /// decide qualification, assignment, or case access.
    ///
    /// # Errors
    /// Rejects invalid Workspace or cursor, out-of-range page sizes, an
    /// unverified Workspace, an unready actor, and storage errors.
    pub fn reviewer_participation_history(
        &self,
        workspace_id: WorkspaceId,
        actor: OperationalActor,
        after_revision: Option<u64>,
        limit: u32,
    ) -> Result<ReviewerParticipationHistoryPage, OperationsError> {
        validate_workspace_id(workspace_id)?;
        if limit == 0 || limit > MAX_HISTORY_PAGE_SIZE {
            return Err(OperationsError::Bounds(
                "reviewer participation history limit",
            ));
        }
        if after_revision.is_some_and(|revision| revision == 0) {
            return Err(OperationsError::Bounds(
                "reviewer participation history cursor",
            ));
        }
        let transaction = self.connection.unchecked_transaction()?;
        crate::teams::ensure_verified_workspace(&transaction, workspace_id)?;
        let actor = resolve_profile_actor(&transaction, workspace_id, actor)?;
        let records = load_history_page(
            &transaction,
            workspace_id,
            actor.participation_id,
            after_revision,
            limit,
        )?;
        let page_size = usize::try_from(limit)
            .map_err(|_| OperationsError::Bounds("reviewer participation history limit"))?;
        let has_more = records.len() > page_size;
        let mut records = records;
        if has_more {
            records.pop();
        }
        let next_after_revision = if has_more {
            records.last().map(ReviewerParticipationReceipt::revision)
        } else {
            None
        };
        transaction.commit()?;
        Ok(ReviewerParticipationHistoryPage {
            records,
            next_after_revision,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StableParticipant {
    participation_id: WorkspaceParticipationId,
    profile_id: Uuid,
    participant_id: Uuid,
    activity_id: ActivityId,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    display_name: String,
}

fn resolve_profile_actor(
    connection: &Connection,
    workspace_id: WorkspaceId,
    actor: OperationalActor,
) -> Result<StableParticipant, OperationsError> {
    let resolved = resolve_actor(connection, workspace_id, actor)?;
    match resolved {
        ResolvedOperationalActor::ProfileParticipant {
            participation_id,
            profile_id,
            participant_id,
            session_id,
            display_name,
        } => {
            let OperationalActor::Participant {
                activity_id,
                agent_id,
                session_id: input_session,
            } = actor
            else {
                return Err(OperationsError::Conflict(
                    "reviewer participation participant actor",
                ));
            };
            if input_session != session_id {
                return Err(OperationsError::Conflict(
                    "reviewer participation Session changed",
                ));
            }
            Ok(StableParticipant {
                participation_id,
                profile_id,
                participant_id,
                activity_id,
                agent_id,
                session_id,
                display_name,
            })
        }
        ResolvedOperationalActor::Operator { .. }
        | ResolvedOperationalActor::NativeParticipant { .. } => Err(OperationsError::Conflict(
            "reviewer participation requires a Profile participant",
        )),
    }
}

fn validate_transition(
    command: &ReviewerParticipationCommand,
    head: Option<&ReviewerParticipationReceipt>,
) -> Result<(), OperationsError> {
    let actual_revision = head.map_or(0, ReviewerParticipationReceipt::revision);
    if actual_revision != command.expected_revision {
        return Err(OperationsError::Conflict(
            "reviewer participation expected revision",
        ));
    }
    match (command.statement, head) {
        (
            ReviewerParticipationStatement::Interested,
            None
            | Some(ReviewerParticipationReceipt {
                statement: ReviewerParticipationStatement::WithdrawInterest { .. },
                ..
            }),
        ) => Ok(()),
        (
            ReviewerParticipationStatement::WithdrawInterest {
                withdraws_request_id,
                withdraws_revision,
            },
            Some(receipt),
        ) if receipt.request_id == withdraws_request_id
            && receipt.revision == withdraws_revision
            && receipt.statement == ReviewerParticipationStatement::Interested =>
        {
            Ok(())
        }
        (ReviewerParticipationStatement::Interested, Some(_)) => Err(OperationsError::Conflict(
            "reviewer participation is already interested",
        )),
        (ReviewerParticipationStatement::WithdrawInterest { .. }, _) => Err(
            OperationsError::Conflict("reviewer participation withdrawal target is not current"),
        ),
    }
}

fn receipt_from_actor(
    command: &ReviewerParticipationCommand,
    actor: &StableParticipant,
    revision: u64,
    recorded_at: TimestampMillis,
) -> ReviewerParticipationReceipt {
    ReviewerParticipationReceipt {
        workspace_id: command.workspace_id,
        participation_id: actor.participation_id,
        profile_id: actor.profile_id,
        participant_id: actor.participant_id,
        request_id: command.request_id,
        revision,
        expected_revision: command.expected_revision,
        statement: command.statement,
        activity_id: actor.activity_id,
        agent_id: actor.agent_id,
        session_id: actor.session_id,
        display_name: actor.display_name.clone(),
        recorded_at,
        replayed: false,
    }
}

fn validate_identity(workspace_id: WorkspaceId, request_id: Uuid) -> Result<(), OperationsError> {
    validate_workspace_id(workspace_id)?;
    if request_id.is_nil() {
        return Err(OperationsError::Bounds("reviewer participation request"));
    }
    Ok(())
}

fn validate_workspace_id(workspace_id: WorkspaceId) -> Result<(), OperationsError> {
    if workspace_id.into_uuid().is_nil() {
        Err(OperationsError::Bounds("reviewer participation Workspace"))
    } else {
        Ok(())
    }
}

fn validate_statement(
    statement: ReviewerParticipationStatement,
    expected_revision: u64,
) -> Result<(), OperationsError> {
    i64::try_from(expected_revision)
        .map_err(|_| OperationsError::Bounds("reviewer participation revision"))?;
    if let ReviewerParticipationStatement::WithdrawInterest {
        withdraws_request_id,
        withdraws_revision,
    } = statement
        && (withdraws_request_id.is_nil()
            || withdraws_revision == 0
            || withdraws_revision != expected_revision)
    {
        return Err(OperationsError::Bounds(
            "reviewer participation withdrawal target",
        ));
    }
    Ok(())
}

fn trusted_now() -> Result<TimestampMillis, OperationsError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| OperationsError::Conflict("reviewer participation clock before epoch"))?
        .as_millis();
    let millis = i64::try_from(millis)
        .map_err(|_| OperationsError::Bounds("reviewer participation timestamp"))?;
    TimestampMillis::new(millis)
        .map_err(|_| OperationsError::Bounds("reviewer participation timestamp"))
}

fn with_replay(mut receipt: ReviewerParticipationReceipt) -> ReviewerParticipationReceipt {
    receipt.replayed = true;
    receipt
}

fn finalize_commit(
    commit_result: Result<(), rusqlite::Error>,
    fresh_receipt: ReviewerParticipationReceipt,
    workspace_id: WorkspaceId,
    request_id: Uuid,
    readback: impl FnOnce() -> Result<Option<ReviewerParticipationReceipt>, OperationsError>,
    matches_command: impl FnOnce(&ReviewerParticipationReceipt) -> bool,
) -> Result<ReviewerParticipationReceipt, OperationsError> {
    match commit_result {
        Ok(()) => Ok(fresh_receipt),
        Err(source) => match readback() {
            Ok(Some(existing)) if matches_command(&existing) => Ok(with_replay(existing)),
            Ok(Some(_)) => Err(OperationsError::Conflict(
                "reviewer participation request replay after unconfirmed commit",
            )),
            Ok(None) | Err(_) => Err(OperationsError::OperationCommitUnconfirmed {
                workspace_id,
                request_id,
                source,
            }),
        },
    }
}

fn receipt_matches_command(
    receipt: &ReviewerParticipationReceipt,
    command: &ReviewerParticipationCommand,
    actor: &StableParticipant,
) -> bool {
    receipt.workspace_id == command.workspace_id
        && receipt.participation_id == actor.participation_id
        && receipt.profile_id == actor.profile_id
        && receipt.participant_id == actor.participant_id
        && receipt.request_id == command.request_id
        && receipt.expected_revision == command.expected_revision
        && receipt.statement == command.statement
        && receipt.activity_id == actor.activity_id
        && receipt.agent_id == actor.agent_id
        && receipt.session_id == actor.session_id
        && receipt.display_name == actor.display_name
}

fn read_raw_receipt(row: &Row<'_>) -> rusqlite::Result<RawReceipt> {
    Ok(RawReceipt {
        workspace_id: row.get(0)?,
        participation_id: row.get(1)?,
        profile_id: row.get(2)?,
        participant_id: row.get(3)?,
        request_id: row.get(4)?,
        revision: row.get(5)?,
        expected_revision: row.get(6)?,
        statement: row.get(7)?,
        withdraws_request_id: row.get(8)?,
        withdraws_revision: row.get(9)?,
        activity_id: row.get(10)?,
        agent_id: row.get(11)?,
        session_id: row.get(12)?,
        display_name: row.get(13)?,
        recorded_at: row.get(14)?,
    })
}

struct RawReceipt {
    workspace_id: Vec<u8>,
    participation_id: Vec<u8>,
    profile_id: Vec<u8>,
    participant_id: Vec<u8>,
    request_id: Vec<u8>,
    revision: i64,
    expected_revision: i64,
    statement: String,
    withdraws_request_id: Option<Vec<u8>>,
    withdraws_revision: Option<i64>,
    activity_id: Vec<u8>,
    agent_id: Vec<u8>,
    session_id: Vec<u8>,
    display_name: String,
    recorded_at: i64,
}

fn decode_receipt(raw: RawReceipt) -> Result<ReviewerParticipationReceipt, OperationsError> {
    let statement = match (
        raw.statement.as_str(),
        raw.withdraws_request_id,
        raw.withdraws_revision,
    ) {
        ("INTERESTED", None, None) => ReviewerParticipationStatement::Interested,
        ("WITHDRAW_INTEREST", Some(request_id), Some(revision)) => {
            let withdraws_revision = positive_revision(revision, "reviewer withdrawal revision")?;
            ReviewerParticipationStatement::WithdrawInterest {
                withdraws_request_id: uuid_from_bytes(&request_id, "reviewer withdrawal request")?,
                withdraws_revision,
            }
        }
        _ => return Err(OperationsError::Corrupt("reviewer participation statement")),
    };
    let receipt = ReviewerParticipationReceipt {
        workspace_id: WorkspaceId::from_uuid(uuid_from_bytes(
            &raw.workspace_id,
            "reviewer Workspace",
        )?),
        participation_id: WorkspaceParticipationId::from_uuid(uuid_from_bytes(
            &raw.participation_id,
            "reviewer participation",
        )?)?,
        profile_id: uuid_from_bytes(&raw.profile_id, "reviewer Profile")?,
        participant_id: uuid_from_bytes(&raw.participant_id, "reviewer participant")?,
        request_id: uuid_from_bytes(&raw.request_id, "reviewer request")?,
        revision: positive_revision(raw.revision, "reviewer revision")?,
        expected_revision: nonnegative_revision(
            raw.expected_revision,
            "reviewer expected revision",
        )?,
        statement,
        activity_id: ActivityId::from_uuid(uuid_from_bytes(&raw.activity_id, "reviewer Activity")?),
        agent_id: AgentInstanceId::from_uuid(uuid_from_bytes(&raw.agent_id, "reviewer Agent")?),
        session_id: AgentSessionId::from_uuid(uuid_from_bytes(
            &raw.session_id,
            "reviewer Session",
        )?),
        display_name: validate_display_name(raw.display_name)?,
        recorded_at: TimestampMillis::new(raw.recorded_at)
            .map_err(|_| OperationsError::Corrupt("reviewer recorded time"))?,
        replayed: false,
    };
    if receipt.expected_revision.checked_add(1) != Some(receipt.revision) {
        return Err(OperationsError::Corrupt("reviewer participation revision"));
    }
    validate_statement(receipt.statement, receipt.expected_revision)
        .map_err(|_| OperationsError::Corrupt("reviewer participation target"))?;
    Ok(receipt)
}

fn uuid_from_bytes(bytes: &[u8], label: &'static str) -> Result<Uuid, OperationsError> {
    let bytes = <[u8; 16]>::try_from(bytes).map_err(|_| OperationsError::Corrupt(label))?;
    let value = Uuid::from_bytes(bytes);
    if value.is_nil() {
        return Err(OperationsError::Corrupt(label));
    }
    Ok(value)
}

fn positive_revision(value: i64, label: &'static str) -> Result<u64, OperationsError> {
    u64::try_from(value)
        .ok()
        .filter(|revision| *revision > 0)
        .ok_or(OperationsError::Corrupt(label))
}

fn nonnegative_revision(value: i64, label: &'static str) -> Result<u64, OperationsError> {
    u64::try_from(value).map_err(|_| OperationsError::Corrupt(label))
}

fn validate_display_name(value: String) -> Result<String, OperationsError> {
    if value.is_empty()
        || value.trim() != value
        || value.chars().any(char::is_control)
        || value.len() > 128
    {
        return Err(OperationsError::Corrupt("reviewer display name"));
    }
    Ok(value)
}

fn load_receipt_by_request(
    connection: &Connection,
    workspace_id: WorkspaceId,
    request_id: Uuid,
) -> Result<Option<ReviewerParticipationReceipt>, OperationsError> {
    let raw = connection
        .query_row(
            "SELECT workspace_id,participation_id,profile_id,participant_id,request_id,revision,expected_revision,statement,withdraws_request_id,withdraws_revision,activity_id,agent_id,session_id,display_name,recorded_at_ms FROM ops_reviewer_participation_receipts WHERE workspace_id=?1 AND request_id=?2",
            params![workspace_id.into_uuid().as_bytes(), request_id.as_bytes()],
            read_raw_receipt,
        )
        .optional()?;
    raw.map(decode_receipt).transpose()
}

fn request_id_exists(connection: &Connection, request_id: Uuid) -> Result<bool, OperationsError> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM ops_reviewer_participation_receipts WHERE request_id=?1)",
        params![request_id.as_bytes()],
        |row| row.get(0),
    )?)
}

fn load_head(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participation_id: WorkspaceParticipationId,
) -> Result<Option<ReviewerParticipationReceipt>, OperationsError> {
    let raw_head = connection
        .query_row(
            "SELECT current_revision,request_id FROM ops_reviewer_participation_heads WHERE workspace_id=?1 AND participation_id=?2",
            params![workspace_id.into_uuid().as_bytes(), participation_id.into_uuid().as_bytes()],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()?;
    raw_head
        .map(|(revision, bytes)| {
            let revision = positive_revision(revision, "reviewer participation head revision")?;
            let request_id = uuid_from_bytes(&bytes, "reviewer participation head request")?;
            let receipt = load_receipt_by_request(connection, workspace_id, request_id)?.ok_or(
                OperationsError::Corrupt("reviewer participation head receipt"),
            )?;
            if receipt.participation_id != participation_id || receipt.revision != revision {
                return Err(OperationsError::Corrupt(
                    "reviewer participation head target",
                ));
            }
            Ok(receipt)
        })
        .transpose()
}

fn load_history_page(
    connection: &Connection,
    workspace_id: WorkspaceId,
    participation_id: WorkspaceParticipationId,
    after_revision: Option<u64>,
    limit: u32,
) -> Result<Vec<ReviewerParticipationReceipt>, OperationsError> {
    let after = after_revision.map_or(Ok(0_i64), |revision| {
        i64::try_from(revision).map_err(|_| OperationsError::Bounds("reviewer history cursor"))
    })?;
    let query_limit = i64::from(limit) + 1;
    let mut statement = connection.prepare(
        "SELECT workspace_id,participation_id,profile_id,participant_id,request_id,revision,expected_revision,statement,withdraws_request_id,withdraws_revision,activity_id,agent_id,session_id,display_name,recorded_at_ms FROM ops_reviewer_participation_receipts WHERE workspace_id=?1 AND participation_id=?2 AND revision>?3 ORDER BY revision ASC LIMIT ?4",
    )?;
    let rows = statement.query_map(
        params![
            workspace_id.into_uuid().as_bytes(),
            participation_id.into_uuid().as_bytes(),
            after,
            query_limit,
        ],
        read_raw_receipt,
    )?;
    rows.map(|row| decode_receipt(row?))
        .collect::<Result<Vec<_>, _>>()
}

fn insert_receipt(
    transaction: &Transaction<'_>,
    receipt: &ReviewerParticipationReceipt,
) -> Result<(), OperationsError> {
    let (statement, withdraws_request_id, withdraws_revision) = match receipt.statement {
        ReviewerParticipationStatement::Interested => ("INTERESTED", None, None),
        ReviewerParticipationStatement::WithdrawInterest {
            withdraws_request_id,
            withdraws_revision,
        } => (
            "WITHDRAW_INTEREST",
            Some(withdraws_request_id),
            Some(
                i64::try_from(withdraws_revision)
                    .map_err(|_| OperationsError::Bounds("reviewer withdrawal revision"))?,
            ),
        ),
    };
    transaction.execute(
        "INSERT INTO ops_reviewer_participation_receipts(workspace_id,participation_id,profile_id,participant_id,request_id,revision,expected_revision,statement,withdraws_request_id,withdraws_revision,activity_id,agent_id,session_id,display_name,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
        params![
            receipt.workspace_id.into_uuid().as_bytes(),
            receipt.participation_id.into_uuid().as_bytes(),
            receipt.profile_id.as_bytes(),
            receipt.participant_id.as_bytes(),
            receipt.request_id.as_bytes(),
            i64::try_from(receipt.revision).map_err(|_| OperationsError::Bounds("reviewer revision"))?,
            i64::try_from(receipt.expected_revision).map_err(|_| OperationsError::Bounds("reviewer expected revision"))?,
            statement,
            withdraws_request_id
                .as_ref()
                .map(|request_id| request_id.as_bytes().as_slice()),
            withdraws_revision,
            receipt.activity_id.into_uuid().as_bytes(),
            receipt.agent_id.into_uuid().as_bytes(),
            receipt.session_id.into_uuid().as_bytes(),
            receipt.display_name,
            receipt.recorded_at.get(),
        ],
    )?;
    Ok(())
}

fn advance_head(
    transaction: &Transaction<'_>,
    workspace_id: WorkspaceId,
    participation_id: WorkspaceParticipationId,
    expected_revision: u64,
    next_revision: u64,
    request_id: Uuid,
) -> Result<(), OperationsError> {
    let next = i64::try_from(next_revision)
        .map_err(|_| OperationsError::Bounds("reviewer participation revision"))?;
    let expected = i64::try_from(expected_revision)
        .map_err(|_| OperationsError::Bounds("reviewer participation expected revision"))?;
    let changed = if expected_revision == 0 {
        transaction.execute(
            "INSERT INTO ops_reviewer_participation_heads(workspace_id,participation_id,current_revision,request_id) VALUES(?1,?2,?3,?4)",
            params![workspace_id.into_uuid().as_bytes(), participation_id.into_uuid().as_bytes(), next, request_id.as_bytes()],
        )?
    } else {
        transaction.execute(
            "UPDATE ops_reviewer_participation_heads SET current_revision=?3,request_id=?4 WHERE workspace_id=?1 AND participation_id=?2 AND current_revision=?5",
            params![workspace_id.into_uuid().as_bytes(), participation_id.into_uuid().as_bytes(), next, request_id.as_bytes(), expected],
        )?
    };
    if changed == 1 {
        Ok(())
    } else {
        Err(OperationsError::Conflict(
            "reviewer participation head revision",
        ))
    }
}
