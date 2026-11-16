//! Additive schema for immutable recipient Profile holds.

use rusqlite::{Connection, OptionalExtension};

use super::HomeAcceptanceProfileError;
use super::sql::{OBJECTS, SELECT_SCHEMA_SQL, TABLE_INFO_PINS_SQL};

pub(in crate::profile_catalog) fn create_schema(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), HomeAcceptanceProfileError> {
    for (_, _, sql) in OBJECTS {
        transaction.execute_batch(sql)?;
    }
    Ok(())
}

pub(in crate::profile_catalog) fn validate_schema(
    connection: &Connection,
) -> Result<(), HomeAcceptanceProfileError> {
    for (kind, name, expected) in OBJECTS {
        let found: Option<String> = connection
            .query_row(SELECT_SCHEMA_SQL, [kind, name], |row| row.get(0))
            .optional()?;
        let Some(found) = found else {
            return Err(HomeAcceptanceProfileError::Schema(name));
        };
        if found.len() > 16 * 1024
            || super::super::normalize_sql(&found) != super::super::normalize_sql(expected)
        {
            return Err(HomeAcceptanceProfileError::Schema(name));
        }
    }
    let pin_columns = [
        ("acceptance_id", "BLOB", 1, 1),
        ("issuer_id", "BLOB", 1, 0),
        ("request_digest", "BLOB", 1, 0),
        ("request_snapshot", "BLOB", 1, 0),
        ("state", "INTEGER", 1, 0),
        ("profile_id", "BLOB", 0, 0),
        ("profile_revision", "INTEGER", 0, 0),
        ("logical_session_request_id", "BLOB", 0, 0),
        ("linked_profile_revision", "INTEGER", 0, 0),
        ("linked_session_revision", "INTEGER", 0, 0),
        ("acquired_at_ms", "INTEGER", 0, 0),
        ("source_outcome", "INTEGER", 0, 0),
        ("source_payload_digest", "BLOB", 0, 0),
        ("source_observed_at_ms", "INTEGER", 0, 0),
        ("operations_resolution_id", "BLOB", 0, 0),
        ("operations_resolution_digest", "BLOB", 0, 0),
        ("operations_admission_id", "BLOB", 0, 0),
        ("operations_admission_digest", "BLOB", 0, 0),
        ("operations_profile_pin_present", "INTEGER", 0, 0),
        ("created_at_ms", "INTEGER", 1, 0),
        ("resolved_at_ms", "INTEGER", 0, 0),
    ];
    let mut statement = connection.prepare(TABLE_INFO_PINS_SQL)?;
    let columns = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(5)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    if columns.len() != pin_columns.len()
        || columns.iter().zip(pin_columns).any(|(actual, expected)| {
            actual.0 != expected.0
                || actual.1 != expected.1
                || actual.2 != expected.2
                || actual.3 != expected.3
        })
    {
        return Err(HomeAcceptanceProfileError::Schema(
            "agent_profile_home_acceptance_pins columns",
        ));
    }
    Ok(())
}
