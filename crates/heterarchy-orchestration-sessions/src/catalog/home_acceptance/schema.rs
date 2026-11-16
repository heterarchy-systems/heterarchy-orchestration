//! Canonical additive schema for logical Session acceptance pins.

use rusqlite::{Connection, OptionalExtension};

use super::HomeAcceptanceSessionError;
use crate::catalog::sql::{HOME_ACCEPTANCE_OBJECT_SQL as OBJECT_SQL, SELECT_SCHEMA_SQL};

pub(in crate::catalog) fn create_schema(
    connection: &Connection,
) -> Result<(), HomeAcceptanceSessionError> {
    for (_, _, sql) in OBJECT_SQL {
        connection.execute_batch(sql)?;
    }
    Ok(())
}

pub(in crate::catalog) fn validate_schema(
    connection: &Connection,
) -> Result<(), HomeAcceptanceSessionError> {
    for (object_type, name, expected) in OBJECT_SQL {
        let actual: Option<String> = connection
            .query_row(
                SELECT_SCHEMA_SQL,
                rusqlite::params![object_type, name],
                |row| row.get(0),
            )
            .optional()?;
        let Some(actual) = actual else {
            return Err(HomeAcceptanceSessionError::Schema(name));
        };
        if actual.len() > 16 * 1024 || normalize_sql(&actual) != normalize_sql(expected) {
            return Err(HomeAcceptanceSessionError::Schema(name));
        }
    }
    Ok(())
}

pub(in crate::catalog) fn object_names() -> Vec<(&'static str, &'static str)> {
    OBJECT_SQL
        .iter()
        .map(|(object_type, name, _)| (*object_type, *name))
        .collect()
}

fn normalize_sql(sql: &str) -> String {
    sql.trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}
