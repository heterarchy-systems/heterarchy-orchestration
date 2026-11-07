//! Additive schema for immutable recipient Profile holds.

use rusqlite::{Connection, OptionalExtension};

use super::HomeAcceptanceProfileError;

const PINS: &str = r"
CREATE TABLE agent_profile_home_acceptance_pins (
    acceptance_id BLOB PRIMARY KEY NOT NULL CHECK(length(acceptance_id)=16 AND acceptance_id!=zeroblob(16)),
    issuer_id BLOB NOT NULL CHECK(length(issuer_id)=16 AND issuer_id!=zeroblob(16)),
    request_digest BLOB NOT NULL CHECK(length(request_digest)=32 AND request_digest!=zeroblob(32)),
    request_snapshot BLOB NOT NULL CHECK(length(request_snapshot) BETWEEN 1 AND 320),
    state INTEGER NOT NULL CHECK(state IN (1,2,3)),
    profile_id BLOB CHECK(profile_id IS NULL OR (length(profile_id)=16 AND profile_id!=zeroblob(16))),
    profile_revision INTEGER CHECK(profile_revision IS NULL OR profile_revision>0),
    logical_session_request_id BLOB CHECK(logical_session_request_id IS NULL OR (length(logical_session_request_id)=16 AND logical_session_request_id!=zeroblob(16))),
    linked_profile_revision INTEGER CHECK(linked_profile_revision IS NULL OR linked_profile_revision>0),
    linked_session_revision INTEGER CHECK(linked_session_revision IS NULL OR linked_session_revision>0),
    acquired_at_ms INTEGER CHECK(acquired_at_ms IS NULL OR acquired_at_ms>=0),
    source_outcome INTEGER CHECK(source_outcome IS NULL OR source_outcome IN (1,2)),
    source_payload_digest BLOB CHECK(source_payload_digest IS NULL OR (length(source_payload_digest)=32 AND source_payload_digest!=zeroblob(32))),
    source_observed_at_ms INTEGER CHECK(source_observed_at_ms IS NULL OR source_observed_at_ms>=0),
    operations_resolution_id BLOB CHECK(operations_resolution_id IS NULL OR (length(operations_resolution_id)=16 AND operations_resolution_id!=zeroblob(16))),
    operations_resolution_digest BLOB CHECK(operations_resolution_digest IS NULL OR (length(operations_resolution_digest)=32 AND operations_resolution_digest!=zeroblob(32))),
    operations_admission_id BLOB CHECK(operations_admission_id IS NULL OR (length(operations_admission_id)=16 AND operations_admission_id!=zeroblob(16))),
    operations_admission_digest BLOB CHECK(operations_admission_digest IS NULL OR (length(operations_admission_digest)=32 AND operations_admission_digest!=zeroblob(32))),
    operations_profile_pin_present INTEGER CHECK(operations_profile_pin_present IS NULL OR operations_profile_pin_present IN (0,1)),
    created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0),
    resolved_at_ms INTEGER CHECK(resolved_at_ms IS NULL OR resolved_at_ms>=created_at_ms),
    FOREIGN KEY(profile_id) REFERENCES agent_profiles(profile_id),
    FOREIGN KEY(logical_session_request_id) REFERENCES agent_profile_logical_sessions(request_id),
    CHECK((profile_id IS NULL AND profile_revision IS NULL AND logical_session_request_id IS NULL AND linked_profile_revision IS NULL AND linked_session_revision IS NULL AND acquired_at_ms IS NULL)
       OR (profile_id IS NOT NULL AND profile_revision IS NOT NULL AND logical_session_request_id IS NOT NULL AND linked_profile_revision IS NOT NULL AND linked_session_revision IS NOT NULL AND acquired_at_ms IS NOT NULL)),
    CHECK((operations_resolution_id IS NULL AND operations_resolution_digest IS NULL)
       OR (operations_resolution_id IS NOT NULL AND operations_resolution_digest IS NOT NULL)),
    CHECK((operations_admission_id IS NULL AND operations_admission_digest IS NULL)
       OR (operations_admission_id IS NOT NULL AND operations_admission_digest IS NOT NULL)),
    CHECK(operations_admission_id IS NULL OR operations_profile_pin_present=1),
    CHECK(operations_profile_pin_present!=1 OR profile_id IS NOT NULL),
    CHECK((state=1 AND source_outcome IS NULL AND source_payload_digest IS NULL AND source_observed_at_ms IS NULL AND operations_resolution_id IS NULL AND operations_admission_id IS NULL AND operations_profile_pin_present IS NULL AND resolved_at_ms IS NULL AND profile_id IS NOT NULL)
       OR (state=2 AND source_outcome=1 AND source_payload_digest IS NOT NULL AND source_observed_at_ms IS NOT NULL AND operations_resolution_id IS NOT NULL AND operations_admission_id IS NOT NULL AND operations_profile_pin_present=1 AND profile_id IS NOT NULL AND resolved_at_ms IS NOT NULL)
       OR (state=3 AND source_outcome=2 AND source_payload_digest IS NOT NULL AND source_observed_at_ms IS NOT NULL AND operations_resolution_id IS NOT NULL AND operations_profile_pin_present IN (0,1) AND resolved_at_ms IS NOT NULL))
);
";

const ACTIVE_PROFILE_INDEX: &str = r"
CREATE INDEX agent_profile_home_acceptance_active_profile_idx
ON agent_profile_home_acceptance_pins(profile_id,acceptance_id) WHERE state=1;
";

const PENDING_ISSUER_INDEX: &str = r"
CREATE INDEX agent_profile_home_acceptance_pending_issuer_idx
ON agent_profile_home_acceptance_pins(issuer_id,acceptance_id) WHERE state=1;
";

const PROFILE_MUTATION_GUARD: &str = r"
CREATE TRIGGER agent_profiles_home_acceptance_mutation_guard
BEFORE UPDATE OF stable_name,display_name,provider_family,preferred_model,
    preferred_reasoning,lifecycle_state,creation_origin,revision ON agent_profiles
WHEN EXISTS(SELECT 1 FROM agent_profile_home_acceptance_pins h
             WHERE h.profile_id=OLD.profile_id AND h.state=1)
 AND (NEW.stable_name IS NOT OLD.stable_name
   OR NEW.display_name IS NOT OLD.display_name
   OR NEW.provider_family IS NOT OLD.provider_family
   OR NEW.preferred_model IS NOT OLD.preferred_model
   OR NEW.preferred_reasoning IS NOT OLD.preferred_reasoning
   OR NEW.lifecycle_state IS NOT OLD.lifecycle_state
   OR NEW.creation_origin IS NOT OLD.creation_origin
   OR NEW.revision IS NOT OLD.revision)
BEGIN SELECT RAISE(ABORT,'Profile is pinned by a Home acceptance'); END;
";

const PIN_TRANSITION_GUARD: &str = r"
CREATE TRIGGER agent_profile_home_acceptance_transition_guard
BEFORE UPDATE ON agent_profile_home_acceptance_pins
WHEN OLD.state!=1 OR NEW.state NOT IN (2,3)
 OR NEW.acceptance_id IS NOT OLD.acceptance_id OR NEW.issuer_id IS NOT OLD.issuer_id
 OR NEW.request_digest IS NOT OLD.request_digest OR NEW.request_snapshot IS NOT OLD.request_snapshot
 OR NEW.profile_id IS NOT OLD.profile_id OR NEW.profile_revision IS NOT OLD.profile_revision
 OR NEW.logical_session_request_id IS NOT OLD.logical_session_request_id
 OR NEW.linked_profile_revision IS NOT OLD.linked_profile_revision
 OR NEW.linked_session_revision IS NOT OLD.linked_session_revision
 OR NEW.acquired_at_ms IS NOT OLD.acquired_at_ms OR NEW.created_at_ms IS NOT OLD.created_at_ms
BEGIN SELECT RAISE(ABORT,'Home Profile pin identity is immutable'); END;
";

const PIN_DELETE_GUARD: &str = r"
CREATE TRIGGER agent_profile_home_acceptance_no_delete
BEFORE DELETE ON agent_profile_home_acceptance_pins
BEGIN SELECT RAISE(ABORT,'Home Profile pin history is permanent'); END;
";

const CAPABILITY_INSERT_GUARD: &str = r"
CREATE TRIGGER agent_profile_capabilities_home_acceptance_insert_guard
BEFORE INSERT ON agent_profile_capabilities
WHEN EXISTS(SELECT 1 FROM agent_profile_home_acceptance_pins h
             WHERE h.profile_id=NEW.profile_id AND h.state=1)
BEGIN SELECT RAISE(ABORT,'Profile capabilities are pinned by a Home acceptance'); END;
";

const CAPABILITY_UPDATE_GUARD: &str = r"
CREATE TRIGGER agent_profile_capabilities_home_acceptance_update_guard
BEFORE UPDATE ON agent_profile_capabilities
WHEN (NEW.profile_id IS NOT OLD.profile_id OR NEW.capability IS NOT OLD.capability)
 AND (EXISTS(SELECT 1 FROM agent_profile_home_acceptance_pins h
              WHERE h.profile_id=OLD.profile_id AND h.state=1)
   OR EXISTS(SELECT 1 FROM agent_profile_home_acceptance_pins h
              WHERE h.profile_id=NEW.profile_id AND h.state=1))
BEGIN SELECT RAISE(ABORT,'Profile capabilities are pinned by a Home acceptance'); END;
";

const CAPABILITY_DELETE_GUARD: &str = r"
CREATE TRIGGER agent_profile_capabilities_home_acceptance_delete_guard
BEFORE DELETE ON agent_profile_capabilities
WHEN EXISTS(SELECT 1 FROM agent_profile_home_acceptance_pins h
             WHERE h.profile_id=OLD.profile_id AND h.state=1)
BEGIN SELECT RAISE(ABORT,'Profile capabilities are pinned by a Home acceptance'); END;
";

const OBJECTS: &[(&str, &str, &str)] = &[
    ("table", "agent_profile_home_acceptance_pins", PINS),
    (
        "index",
        "agent_profile_home_acceptance_active_profile_idx",
        ACTIVE_PROFILE_INDEX,
    ),
    (
        "index",
        "agent_profile_home_acceptance_pending_issuer_idx",
        PENDING_ISSUER_INDEX,
    ),
    (
        "trigger",
        "agent_profiles_home_acceptance_mutation_guard",
        PROFILE_MUTATION_GUARD,
    ),
    (
        "trigger",
        "agent_profile_home_acceptance_transition_guard",
        PIN_TRANSITION_GUARD,
    ),
    (
        "trigger",
        "agent_profile_home_acceptance_no_delete",
        PIN_DELETE_GUARD,
    ),
    (
        "trigger",
        "agent_profile_capabilities_home_acceptance_insert_guard",
        CAPABILITY_INSERT_GUARD,
    ),
    (
        "trigger",
        "agent_profile_capabilities_home_acceptance_update_guard",
        CAPABILITY_UPDATE_GUARD,
    ),
    (
        "trigger",
        "agent_profile_capabilities_home_acceptance_delete_guard",
        CAPABILITY_DELETE_GUARD,
    ),
];

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
            .query_row(
                "SELECT sql FROM sqlite_schema WHERE type=?1 AND name=?2",
                [kind, name],
                |row| row.get(0),
            )
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
    let mut statement =
        connection.prepare("PRAGMA table_info(agent_profile_home_acceptance_pins)")?;
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

#[cfg(test)]
pub(super) fn downgrade_empty_to_v6_for_test(
    connection: &Connection,
) -> Result<(), Box<dyn std::error::Error>> {
    if !connection.is_autocommit() {
        return Err("Profile v7 fixture downgrade requires autocommit".into());
    }
    validate_schema(connection)?;
    let populated: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM agent_profile_home_acceptance_pins)",
        [],
        |row| row.get(0),
    )?;
    if populated {
        return Err("refusing to remove populated Home Profile pin history".into());
    }
    let transaction = connection.unchecked_transaction()?;
    for (kind, name, _) in OBJECTS.iter().rev() {
        transaction.execute_batch(&format!("DROP {kind} {name}"))?;
    }
    let violations = transaction
        .prepare("PRAGMA foreign_key_check")?
        .query([])?
        .next()?
        .is_some();
    if violations {
        return Err("Profile v6 fixture downgrade created a foreign key violation".into());
    }
    transaction.pragma_update(
        None,
        "user_version",
        super::super::AGENT_PROFILE_V6_SCHEMA_VERSION,
    )?;
    transaction.commit()?;
    super::super::validate_v6_schema(connection)?;
    Ok(())
}
