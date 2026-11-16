//! Canonical compile-time validated SQL statements for the global Agent Profile catalog.

use crate::static_sql::StaticSql;

pub(super) const AGENT_PROFILES_TABLE_SQL: &str = StaticSql::ddl(
    "CREATE TABLE agent_profiles (
    profile_id BLOB PRIMARY KEY NOT NULL CHECK(length(profile_id) = 16 AND profile_id != zeroblob(16)),
    request_id BLOB NOT NULL UNIQUE CHECK(length(request_id) = 16 AND request_id != zeroblob(16)),
    stable_name TEXT NOT NULL CHECK(length(CAST(stable_name AS BLOB)) BETWEEN 1 AND 128),
    display_name TEXT NOT NULL CHECK(length(CAST(display_name AS BLOB)) BETWEEN 1 AND 128),
    provider_family INTEGER NOT NULL CHECK(provider_family IN (0,1)),
    preferred_model TEXT CHECK(preferred_model IS NULL OR length(CAST(preferred_model AS BLOB)) BETWEEN 1 AND 128),
    preferred_reasoning TEXT CHECK(preferred_reasoning IS NULL OR length(CAST(preferred_reasoning AS BLOB)) BETWEEN 1 AND 128),
    lifecycle_state INTEGER NOT NULL CHECK(lifecycle_state IN (1, 2, 3)),
    creation_origin INTEGER NOT NULL CHECK(creation_origin IN (1, 2, 3, 4)),
    created_at_ms INTEGER NOT NULL CHECK(created_at_ms >= 0),
    updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms >= created_at_ms),
    last_used_at_ms INTEGER CHECK(last_used_at_ms IS NULL OR last_used_at_ms >= created_at_ms),
    revision INTEGER NOT NULL CHECK(revision > 0)
);",
)
.as_str();

pub(super) const AGENT_PROFILE_CAPABILITIES_TABLE_SQL: &str = StaticSql::ddl(
    "CREATE TABLE agent_profile_capabilities (
    profile_id BLOB NOT NULL CHECK(length(profile_id) = 16 AND profile_id != zeroblob(16)),
    capability TEXT NOT NULL CHECK(length(CAST(capability AS BLOB)) BETWEEN 1 AND 64),
    PRIMARY KEY(profile_id, capability),
    FOREIGN KEY(profile_id) REFERENCES agent_profiles(profile_id)
);",
)
.as_str();

pub(super) const AGENT_PROFILE_LOGICAL_SESSIONS_TABLE_SQL: &str = StaticSql::ddl(
    "CREATE TABLE agent_profile_logical_sessions (
    request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id) = 16 AND request_id != zeroblob(16)),
    profile_id BLOB NOT NULL CHECK(length(profile_id) = 16 AND profile_id != zeroblob(16)),
    profile_revision INTEGER NOT NULL CHECK(profile_revision > 0),
    session_id BLOB NOT NULL UNIQUE CHECK(length(session_id) = 16 AND session_id != zeroblob(16)),
    session_revision INTEGER NOT NULL CHECK(session_revision > 0),
    linked_at_ms INTEGER NOT NULL CHECK(linked_at_ms >= 0),
    FOREIGN KEY(profile_id) REFERENCES agent_profiles(profile_id)
);",
)
.as_str();

pub(super) const AGENT_PROFILE_RUNTIME_SESSIONS_TABLE_SQL: &str = StaticSql::ddl(
    "CREATE TABLE agent_profile_runtime_sessions (
    runtime_session_id BLOB PRIMARY KEY NOT NULL CHECK(length(runtime_session_id) = 16 AND runtime_session_id != zeroblob(16)),
    request_id BLOB NOT NULL UNIQUE CHECK(length(request_id) = 16 AND request_id != zeroblob(16)),
    profile_id BLOB NOT NULL CHECK(length(profile_id) = 16 AND profile_id != zeroblob(16)),
    provider_family INTEGER NOT NULL CHECK(provider_family = 1),
    provider_conversation_id TEXT CHECK(provider_conversation_id IS NULL OR length(CAST(provider_conversation_id AS BLOB)) BETWEEN 1 AND 512),
    state INTEGER NOT NULL CHECK(state BETWEEN 1 AND 8),
    opened_at_ms INTEGER NOT NULL CHECK(opened_at_ms >= 0),
    last_seen_at_ms INTEGER NOT NULL CHECK(last_seen_at_ms >= opened_at_ms),
    closed_at_ms INTEGER CHECK(closed_at_ms IS NULL OR closed_at_ms >= opened_at_ms),
    revision INTEGER NOT NULL CHECK(revision > 0),
    FOREIGN KEY(profile_id) REFERENCES agent_profiles(profile_id),
    UNIQUE(provider_family, provider_conversation_id),
    CHECK((state = 8 AND closed_at_ms IS NOT NULL) OR (state != 8 AND closed_at_ms IS NULL)),
    CHECK((state IN (2,3,4,5,6) AND provider_conversation_id IS NOT NULL) OR state IN (1,7,8))
);",
)
.as_str();

pub(super) const AGENT_PROFILE_MUTATION_RECEIPTS_TABLE_SQL: &str = StaticSql::ddl(
    "CREATE TABLE agent_profile_mutation_receipts (
    request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id) = 16 AND request_id != zeroblob(16)),
    profile_id BLOB NOT NULL CHECK(length(profile_id) = 16 AND profile_id != zeroblob(16)),
    operation_kind INTEGER NOT NULL CHECK(operation_kind BETWEEN 1 AND 4),
    expected_revision INTEGER CHECK(expected_revision IS NULL OR expected_revision > 0),
    creation_request_id BLOB NOT NULL CHECK(length(creation_request_id) = 16 AND creation_request_id != zeroblob(16)),
    stable_name TEXT NOT NULL CHECK(length(CAST(stable_name AS BLOB)) BETWEEN 1 AND 128),
    display_name TEXT NOT NULL CHECK(length(CAST(display_name AS BLOB)) BETWEEN 1 AND 128),
    provider_family INTEGER NOT NULL CHECK(provider_family IN (0,1)),
    preferred_model TEXT CHECK(preferred_model IS NULL OR length(CAST(preferred_model AS BLOB)) BETWEEN 1 AND 128),
    preferred_reasoning TEXT CHECK(preferred_reasoning IS NULL OR length(CAST(preferred_reasoning AS BLOB)) BETWEEN 1 AND 128),
    lifecycle_state INTEGER NOT NULL CHECK(lifecycle_state IN (1, 2, 3)),
    creation_origin INTEGER NOT NULL CHECK(creation_origin IN (1, 2, 3, 4)),
    created_at_ms INTEGER NOT NULL CHECK(created_at_ms >= 0),
    updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms >= created_at_ms),
    last_used_at_ms INTEGER CHECK(last_used_at_ms IS NULL OR last_used_at_ms >= created_at_ms),
    revision INTEGER NOT NULL CHECK(revision > 0),
    FOREIGN KEY(profile_id) REFERENCES agent_profiles(profile_id),
    CHECK((operation_kind = 1 AND expected_revision IS NULL) OR (operation_kind BETWEEN 2 AND 4 AND expected_revision IS NOT NULL))
);",
)
.as_str();

pub(super) const AGENT_PROFILE_MUTATION_CAPABILITIES_TABLE_SQL: &str = StaticSql::ddl(
    "CREATE TABLE agent_profile_mutation_capabilities (
    request_id BLOB NOT NULL CHECK(length(request_id) = 16 AND request_id != zeroblob(16)),
    capability TEXT NOT NULL CHECK(length(CAST(capability AS BLOB)) BETWEEN 1 AND 64),
    PRIMARY KEY(request_id, capability),
    FOREIGN KEY(request_id) REFERENCES agent_profile_mutation_receipts(request_id)
);",
)
.as_str();

pub(super) const SELECT_PROFILE_IDS_FOR_LIST_SQL: &str = StaticSql::dml(
    "SELECT profile_id FROM agent_profiles ORDER BY created_at_ms DESC, profile_id DESC LIMIT ?1",
)
.as_str();

pub(super) const INSERT_LOGICAL_SESSION_LINK_SQL: &str = StaticSql::dml(
    "INSERT INTO agent_profile_logical_sessions(request_id,profile_id,profile_revision,session_id,session_revision,linked_at_ms) VALUES(?1,?2,?3,?4,?5,?6)",
)
.as_str();

pub(super) const SELECT_LOGICAL_SESSION_HISTORY_SQL: &str = StaticSql::dml(
    "SELECT request_id,profile_id,profile_revision,session_id,session_revision,linked_at_ms FROM agent_profile_logical_sessions WHERE profile_id=?1 ORDER BY linked_at_ms DESC, request_id DESC LIMIT ?2",
)
.as_str();

pub(super) const COUNT_NON_INTERNAL_SCHEMA_OBJECTS_SQL: &str =
    StaticSql::dml("SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'").as_str();

pub(super) const COUNT_FORBIDDEN_PROFILE_COLUMNS_SQL: &str = StaticSql::dml(
    "SELECT COUNT(*) FROM pragma_table_info('agent_profiles')
WHERE name IN ('workspace_id','team_id','participant_id','session_id','agent_id',
               'assignment_id','writer_lease_id','vote_id','work_credit')",
)
.as_str();

pub(super) const SELECT_TABLE_SCHEMA_SQL: &str =
    StaticSql::dml("SELECT sql FROM sqlite_schema WHERE type='table' AND name=?1").as_str();

pub(super) const SELECT_PROFILE_ID_BY_REQUEST_SQL: &str =
    StaticSql::dml("SELECT profile_id FROM agent_profiles WHERE request_id=?1").as_str();

pub(super) const SELECT_LOGICAL_SESSION_LINK_BY_REQUEST_SQL: &str = StaticSql::dml(
    "SELECT request_id,profile_id,profile_revision,session_id,session_revision,linked_at_ms FROM agent_profile_logical_sessions WHERE request_id=?1",
)
.as_str();

pub(super) const SELECT_LOGICAL_SESSION_LINK_BY_SESSION_SQL: &str = StaticSql::dml(
    "SELECT request_id,profile_id,profile_revision,session_id,session_revision,linked_at_ms FROM agent_profile_logical_sessions WHERE session_id=?1",
)
.as_str();

pub(super) const SELECT_RUNTIME_SESSION_ID_BY_REQUEST_SQL: &str = StaticSql::dml(
    "SELECT runtime_session_id FROM agent_profile_runtime_sessions WHERE request_id=?1",
)
.as_str();

pub(super) const SELECT_RUNTIME_SESSION_BY_ID_SQL: &str = StaticSql::dml(
    "SELECT runtime_session_id,request_id,profile_id,provider_family,
       provider_conversation_id,state,opened_at_ms,last_seen_at_ms,
       closed_at_ms,revision
FROM agent_profile_runtime_sessions WHERE runtime_session_id=?1",
)
.as_str();

pub(super) const SELECT_RUNTIME_SESSION_HISTORY_IDS_SQL: &str = StaticSql::dml(
    "SELECT runtime_session_id FROM agent_profile_runtime_sessions
WHERE profile_id=?1 ORDER BY opened_at_ms DESC,runtime_session_id DESC LIMIT ?2",
)
.as_str();

pub(super) const SELECT_RUNTIME_SESSIONS_PAGE_SQL: &str = StaticSql::dml(
    "SELECT runtime_session_id,request_id,profile_id,provider_family,
       provider_conversation_id,state,opened_at_ms,last_seen_at_ms,
       closed_at_ms,revision
FROM agent_profile_runtime_sessions
WHERE profile_id=?1 AND (?2 IS NULL OR runtime_session_id>?2)
ORDER BY runtime_session_id ASC LIMIT ?3",
)
.as_str();

pub(super) const SELECT_PROFILE_MUTATION_RECEIPT_SQL: &str = StaticSql::dml(
    "SELECT request_id,profile_id,operation_kind,expected_revision,
       creation_request_id,stable_name,display_name,provider_family,
       preferred_model,preferred_reasoning,lifecycle_state,creation_origin,
       created_at_ms,updated_at_ms,last_used_at_ms,revision
FROM agent_profile_mutation_receipts WHERE request_id=?1",
)
.as_str();

pub(super) const SELECT_PROFILE_MUTATION_CAPABILITIES_SQL: &str = StaticSql::dml(
    "SELECT capability FROM agent_profile_mutation_capabilities
WHERE request_id=?1 ORDER BY capability",
)
.as_str();

pub(super) const INSERT_PROFILE_MUTATION_RECEIPT_SQL: &str = StaticSql::dml(
    "INSERT INTO agent_profile_mutation_receipts(
    request_id,profile_id,operation_kind,expected_revision,
    creation_request_id,stable_name,display_name,provider_family,
    preferred_model,preferred_reasoning,lifecycle_state,creation_origin,
    created_at_ms,updated_at_ms,last_used_at_ms,revision
) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
)
.as_str();

pub(super) const INSERT_PROFILE_MUTATION_CAPABILITY_SQL: &str = StaticSql::dml(
    "INSERT INTO agent_profile_mutation_capabilities(request_id,capability) VALUES(?1,?2)",
)
.as_str();

pub(super) const COUNT_LIVE_RUNTIME_SESSIONS_SQL: &str = StaticSql::dml(
    "SELECT COUNT(*) FROM agent_profile_runtime_sessions
WHERE profile_id=?1 AND state IN (1,2,3,4,6)",
)
.as_str();

pub(super) const UPDATE_PROFILE_SQL: &str = StaticSql::dml(
    "UPDATE agent_profiles
SET display_name=?1,preferred_model=?2,preferred_reasoning=?3,
    lifecycle_state=?4,updated_at_ms=?5,revision=?6
WHERE profile_id=?7 AND revision=?8",
)
.as_str();

pub(super) const DELETE_PROFILE_CAPABILITIES_SQL: &str =
    StaticSql::dml("DELETE FROM agent_profile_capabilities WHERE profile_id=?1").as_str();

pub(super) const INSERT_PROFILE_CAPABILITY_SQL: &str =
    StaticSql::dml("INSERT INTO agent_profile_capabilities(profile_id,capability) VALUES(?1,?2)")
        .as_str();

pub(super) const INSERT_PROFILE_SQL: &str = StaticSql::dml(
    "INSERT INTO agent_profiles(
    profile_id,request_id,stable_name,display_name,provider_family,
    preferred_model,preferred_reasoning,lifecycle_state,creation_origin,
    created_at_ms,updated_at_ms,last_used_at_ms,revision
) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
)
.as_str();

pub(super) const SELECT_PROFILE_BY_ID_SQL: &str = StaticSql::dml(
    "SELECT profile_id,request_id,stable_name,display_name,provider_family,
       preferred_model,preferred_reasoning,lifecycle_state,creation_origin,
       created_at_ms,updated_at_ms,last_used_at_ms,revision
FROM agent_profiles WHERE profile_id=?1",
)
.as_str();

pub(super) const SELECT_PROFILE_CAPABILITIES_SQL: &str = StaticSql::dml(
    "SELECT capability FROM agent_profile_capabilities WHERE profile_id=?1 ORDER BY capability",
)
.as_str();

pub(super) const QUICK_CHECK_SQL: &str = StaticSql::dml("PRAGMA quick_check(1)").as_str();

pub(super) const FOREIGN_KEY_CHECK_SQL: &str = StaticSql::dml("PRAGMA foreign_key_check").as_str();
