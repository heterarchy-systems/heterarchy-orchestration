//! Canonical compile-time validated SQL statements for Profile CLI execution preferences.

use crate::static_sql::StaticSql;

pub(super) const PREFERENCES_TABLE_SQL: &str = StaticSql::ddl(
    "CREATE TABLE agent_profile_execution_preferences (
    profile_id BLOB PRIMARY KEY NOT NULL CHECK(length(profile_id) = 16 AND profile_id != zeroblob(16)),
    cli_engine INTEGER NOT NULL CHECK(cli_engine IN (1, 2)),
    preferred_model TEXT NOT NULL CHECK(length(CAST(preferred_model AS BLOB)) BETWEEN 1 AND 128),
    preferred_reasoning TEXT CHECK(preferred_reasoning IS NULL OR length(CAST(preferred_reasoning AS BLOB)) BETWEEN 1 AND 128),
    role_prompt_digest BLOB CHECK(role_prompt_digest IS NULL OR (length(role_prompt_digest) = 32 AND role_prompt_digest != zeroblob(32))),
    FOREIGN KEY(profile_id) REFERENCES agent_profiles(profile_id)
);",
)
.as_str();

pub(super) const CAPABILITIES_TABLE_SQL: &str = StaticSql::ddl(
    "CREATE TABLE agent_profile_execution_capabilities (
    profile_id BLOB NOT NULL CHECK(length(profile_id) = 16 AND profile_id != zeroblob(16)),
    capability TEXT NOT NULL CHECK(length(CAST(capability AS BLOB)) BETWEEN 1 AND 64),
    PRIMARY KEY(profile_id, capability),
    FOREIGN KEY(profile_id) REFERENCES agent_profile_execution_preferences(profile_id)
);",
)
.as_str();

pub(super) const MUTATION_PREFERENCES_TABLE_SQL: &str = StaticSql::ddl(
    "CREATE TABLE agent_profile_mutation_execution_preferences (
    request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id) = 16 AND request_id != zeroblob(16)),
    cli_engine INTEGER NOT NULL CHECK(cli_engine IN (1, 2)),
    preferred_model TEXT NOT NULL CHECK(length(CAST(preferred_model AS BLOB)) BETWEEN 1 AND 128),
    preferred_reasoning TEXT CHECK(preferred_reasoning IS NULL OR length(CAST(preferred_reasoning AS BLOB)) BETWEEN 1 AND 128),
    role_prompt_digest BLOB CHECK(role_prompt_digest IS NULL OR (length(role_prompt_digest) = 32 AND role_prompt_digest != zeroblob(32))),
    FOREIGN KEY(request_id) REFERENCES agent_profile_mutation_receipts(request_id)
);",
)
.as_str();

pub(super) const MUTATION_CAPABILITIES_TABLE_SQL: &str = StaticSql::ddl(
    "CREATE TABLE agent_profile_mutation_execution_capabilities (
    request_id BLOB NOT NULL CHECK(length(request_id) = 16 AND request_id != zeroblob(16)),
    capability TEXT NOT NULL CHECK(length(CAST(capability AS BLOB)) BETWEEN 1 AND 64),
    PRIMARY KEY(request_id, capability),
    FOREIGN KEY(request_id) REFERENCES agent_profile_mutation_execution_preferences(request_id)
);",
)
.as_str();

pub(super) const PREFERENCES_INSERT_GUARD_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER agent_profile_execution_preferences_home_acceptance_insert_guard
BEFORE INSERT ON agent_profile_execution_preferences
WHEN EXISTS(SELECT 1 FROM agent_profile_home_acceptance_pins h
             WHERE h.profile_id=NEW.profile_id AND h.state=1)
BEGIN SELECT RAISE(ABORT,'Profile execution preference is pinned by a Home acceptance'); END;",
)
.as_str();

pub(super) const PREFERENCES_UPDATE_GUARD_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER agent_profile_execution_preferences_home_acceptance_update_guard
BEFORE UPDATE ON agent_profile_execution_preferences
WHEN (NEW.profile_id IS NOT OLD.profile_id
   OR NEW.cli_engine IS NOT OLD.cli_engine
   OR NEW.preferred_model IS NOT OLD.preferred_model
   OR NEW.preferred_reasoning IS NOT OLD.preferred_reasoning
   OR NEW.role_prompt_digest IS NOT OLD.role_prompt_digest)
 AND (EXISTS(SELECT 1 FROM agent_profile_home_acceptance_pins h
              WHERE h.profile_id=OLD.profile_id AND h.state=1)
   OR EXISTS(SELECT 1 FROM agent_profile_home_acceptance_pins h
              WHERE h.profile_id=NEW.profile_id AND h.state=1))
BEGIN SELECT RAISE(ABORT,'Profile execution preference is pinned by a Home acceptance'); END;",
)
.as_str();

pub(super) const PREFERENCES_DELETE_GUARD_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER agent_profile_execution_preferences_home_acceptance_delete_guard
BEFORE DELETE ON agent_profile_execution_preferences
WHEN EXISTS(SELECT 1 FROM agent_profile_home_acceptance_pins h
             WHERE h.profile_id=OLD.profile_id AND h.state=1)
BEGIN SELECT RAISE(ABORT,'Profile execution preference is pinned by a Home acceptance'); END;",
)
.as_str();

pub(super) const CAPABILITIES_INSERT_GUARD_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER agent_profile_execution_capabilities_home_acceptance_insert_guard
BEFORE INSERT ON agent_profile_execution_capabilities
WHEN EXISTS(SELECT 1 FROM agent_profile_home_acceptance_pins h
             WHERE h.profile_id=NEW.profile_id AND h.state=1)
BEGIN SELECT RAISE(ABORT,'Profile execution capabilities are pinned by a Home acceptance'); END;",
)
.as_str();

pub(super) const CAPABILITIES_UPDATE_GUARD_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER agent_profile_execution_capabilities_home_acceptance_update_guard
BEFORE UPDATE ON agent_profile_execution_capabilities
WHEN (NEW.profile_id IS NOT OLD.profile_id OR NEW.capability IS NOT OLD.capability)
 AND (EXISTS(SELECT 1 FROM agent_profile_home_acceptance_pins h
              WHERE h.profile_id=OLD.profile_id AND h.state=1)
   OR EXISTS(SELECT 1 FROM agent_profile_home_acceptance_pins h
              WHERE h.profile_id=NEW.profile_id AND h.state=1))
BEGIN SELECT RAISE(ABORT,'Profile execution capabilities are pinned by a Home acceptance'); END;",
)
.as_str();

pub(super) const CAPABILITIES_DELETE_GUARD_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER agent_profile_execution_capabilities_home_acceptance_delete_guard
BEFORE DELETE ON agent_profile_execution_capabilities
WHEN EXISTS(SELECT 1 FROM agent_profile_home_acceptance_pins h
             WHERE h.profile_id=OLD.profile_id AND h.state=1)
BEGIN SELECT RAISE(ABORT,'Profile execution capabilities are pinned by a Home acceptance'); END;",
)
.as_str();

pub(super) const SELECT_SCHEMA_SQL: &str =
    StaticSql::dml("SELECT sql FROM sqlite_schema WHERE type=?1 AND name=?2").as_str();

pub(super) const INSERT_PROFILE_PREFERENCE_SQL: &str = StaticSql::dml(
    "INSERT INTO agent_profile_execution_preferences(
    profile_id,cli_engine,preferred_model,preferred_reasoning,role_prompt_digest
) VALUES(?1,?2,?3,?4,?5)",
)
.as_str();

pub(super) const INSERT_PROFILE_CAPABILITY_SQL: &str = StaticSql::dml(
    "INSERT INTO agent_profile_execution_capabilities(profile_id,capability)
VALUES(?1,?2)",
)
.as_str();

pub(super) const DELETE_PROFILE_CAPABILITIES_SQL: &str =
    StaticSql::dml("DELETE FROM agent_profile_execution_capabilities WHERE profile_id=?1").as_str();

pub(super) const DELETE_PROFILE_PREFERENCE_SQL: &str =
    StaticSql::dml("DELETE FROM agent_profile_execution_preferences WHERE profile_id=?1").as_str();

pub(super) const SELECT_PROFILE_PREFERENCE_SQL: &str = StaticSql::dml(
    "SELECT cli_engine,preferred_model,preferred_reasoning,role_prompt_digest
FROM agent_profile_execution_preferences WHERE profile_id=?1",
)
.as_str();

pub(super) const SELECT_PROFILE_CAPABILITIES_SQL: &str = StaticSql::dml(
    "SELECT capability FROM agent_profile_execution_capabilities
WHERE profile_id=?1 ORDER BY capability",
)
.as_str();

pub(super) const INSERT_MUTATION_PREFERENCE_SQL: &str = StaticSql::dml(
    "INSERT INTO agent_profile_mutation_execution_preferences(
    request_id,cli_engine,preferred_model,preferred_reasoning,role_prompt_digest
) VALUES(?1,?2,?3,?4,?5)",
)
.as_str();

pub(super) const INSERT_MUTATION_CAPABILITY_SQL: &str = StaticSql::dml(
    "INSERT INTO agent_profile_mutation_execution_capabilities(request_id,capability)
VALUES(?1,?2)",
)
.as_str();

pub(super) const SELECT_MUTATION_PREFERENCE_SQL: &str = StaticSql::dml(
    "SELECT cli_engine,preferred_model,preferred_reasoning,role_prompt_digest
FROM agent_profile_mutation_execution_preferences WHERE request_id=?1",
)
.as_str();

pub(super) const SELECT_MUTATION_CAPABILITIES_SQL: &str = StaticSql::dml(
    "SELECT capability FROM agent_profile_mutation_execution_capabilities
WHERE request_id=?1 ORDER BY capability",
)
.as_str();

pub(super) const OBJECTS: &[(&str, &str, &str)] = &[
    (
        "table",
        "agent_profile_execution_preferences",
        PREFERENCES_TABLE_SQL,
    ),
    (
        "table",
        "agent_profile_execution_capabilities",
        CAPABILITIES_TABLE_SQL,
    ),
    (
        "table",
        "agent_profile_mutation_execution_preferences",
        MUTATION_PREFERENCES_TABLE_SQL,
    ),
    (
        "table",
        "agent_profile_mutation_execution_capabilities",
        MUTATION_CAPABILITIES_TABLE_SQL,
    ),
    (
        "trigger",
        "agent_profile_execution_preferences_home_acceptance_insert_guard",
        PREFERENCES_INSERT_GUARD_SQL,
    ),
    (
        "trigger",
        "agent_profile_execution_preferences_home_acceptance_update_guard",
        PREFERENCES_UPDATE_GUARD_SQL,
    ),
    (
        "trigger",
        "agent_profile_execution_preferences_home_acceptance_delete_guard",
        PREFERENCES_DELETE_GUARD_SQL,
    ),
    (
        "trigger",
        "agent_profile_execution_capabilities_home_acceptance_insert_guard",
        CAPABILITIES_INSERT_GUARD_SQL,
    ),
    (
        "trigger",
        "agent_profile_execution_capabilities_home_acceptance_update_guard",
        CAPABILITIES_UPDATE_GUARD_SQL,
    ),
    (
        "trigger",
        "agent_profile_execution_capabilities_home_acceptance_delete_guard",
        CAPABILITIES_DELETE_GUARD_SQL,
    ),
];
