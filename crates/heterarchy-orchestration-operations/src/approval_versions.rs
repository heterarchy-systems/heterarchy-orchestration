//! Canonical revision authority for durable approval requests.

use forge_domain::ApprovalRequestId;
use rusqlite::{Connection, OptionalExtension, Transaction, params};

use crate::OperationsError;

const VERSIONS_SQL: &str = "CREATE TABLE ops_approval_request_versions (request_id BLOB PRIMARY KEY NOT NULL REFERENCES ops_approval_requests(request_id), revision INTEGER NOT NULL CHECK(typeof(revision)='integer' AND revision>=1))";

pub(crate) fn schema_objects() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![("table", "ops_approval_request_versions", VERSIONS_SQL)]
}

/// Create the additive version table and seed each existing request at its
/// current baseline. This records no fabricated historical transitions.
pub(crate) fn create_schema(connection: &Connection) -> Result<(), OperationsError> {
    connection.execute_batch(VERSIONS_SQL)?;
    connection.execute(
        "INSERT OR IGNORE INTO ops_approval_request_versions(request_id,revision) SELECT request_id,1 FROM ops_approval_requests",
        [],
    )?;
    Ok(())
}

pub(crate) fn insert_baseline(
    transaction: &Transaction<'_>,
    request_id: ApprovalRequestId,
) -> Result<(), OperationsError> {
    transaction.execute(
        "INSERT INTO ops_approval_request_versions(request_id,revision) VALUES(?1,1)",
        params![request_id.into_uuid().as_bytes()],
    )?;
    Ok(())
}

pub(crate) fn increment(
    transaction: &Transaction<'_>,
    request_id: ApprovalRequestId,
) -> Result<i64, OperationsError> {
    let current: i64 = transaction
        .query_row(
            "SELECT revision FROM ops_approval_request_versions WHERE request_id=?1",
            params![request_id.into_uuid().as_bytes()],
            |row| row.get(0),
        )
        .optional()?
        .ok_or(OperationsError::Corrupt(
            "approval request revision missing",
        ))?;
    if current < 1 {
        return Err(OperationsError::Corrupt("approval request revision"));
    }
    let next = current
        .checked_add(1)
        .ok_or(OperationsError::Bounds("approval request revision"))?;
    let changed = transaction.execute(
        "UPDATE ops_approval_request_versions SET revision=?1 WHERE request_id=?2 AND revision=?3",
        params![next, request_id.into_uuid().as_bytes(), current],
    )?;
    if changed != 1 {
        return Err(OperationsError::Corrupt("approval request revision"));
    }
    Ok(next)
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use uuid::Uuid;

    use super::*;

    #[test]
    fn increment_is_checked_and_rolls_back_i64_overflow() -> Result<(), OperationsError> {
        let mut connection = Connection::open_in_memory()?;
        connection.execute_batch(
            "CREATE TABLE ops_approval_requests(request_id BLOB PRIMARY KEY NOT NULL);",
        )?;
        connection.execute_batch(VERSIONS_SQL)?;
        let request_id = ApprovalRequestId::from_uuid(Uuid::from_u128(0xA001));
        connection.execute(
            "INSERT INTO ops_approval_requests(request_id) VALUES(?1)",
            params![request_id.into_uuid().as_bytes()],
        )?;
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        insert_baseline(&tx, request_id)?;
        assert_eq!(increment(&tx, request_id)?, 2);
        tx.commit()?;
        connection.execute(
            "UPDATE ops_approval_request_versions SET revision=?1 WHERE request_id=?2",
            params![i64::MAX, request_id.into_uuid().as_bytes()],
        )?;
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        assert!(matches!(
            increment(&tx, request_id),
            Err(OperationsError::Bounds("approval request revision"))
        ));
        tx.rollback()?;
        Ok(())
    }
}
