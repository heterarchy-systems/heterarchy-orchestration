//! Canonical compile-time validated SQL statements for Profile deletion intents and manifests.

use crate::static_sql::StaticSql;

pub(super) const PROFILE_DELETION_REQUESTS_TABLE_SQL: &str = StaticSql::ddl(
    "CREATE TABLE agent_profile_deletion_requests (
    request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id) = 16 AND request_id != zeroblob(16)),
    profile_id BLOB NOT NULL UNIQUE CHECK(length(profile_id) = 16 AND profile_id != zeroblob(16)),
    expected_profile_revision INTEGER NOT NULL CHECK(expected_profile_revision > 0),
    barrier_profile_revision INTEGER NOT NULL CHECK(barrier_profile_revision > 0),
    requested_at_ms INTEGER NOT NULL CHECK(requested_at_ms >= 0),
    FOREIGN KEY(profile_id) REFERENCES agent_profiles(profile_id)
);",
)
.as_str();

pub(super) const PROFILE_DELETION_MANIFESTS_TABLE_SQL: &str = StaticSql::ddl(
    "CREATE TABLE agent_profile_deletion_manifests (
    request_id BLOB PRIMARY KEY NOT NULL CHECK(length(request_id) = 16 AND request_id != zeroblob(16)),
    profile_id BLOB NOT NULL UNIQUE CHECK(length(profile_id) = 16 AND profile_id != zeroblob(16)),
    profile_revision_at_freeze INTEGER NOT NULL CHECK(profile_revision_at_freeze > 0),
    frozen_at_ms INTEGER NOT NULL CHECK(frozen_at_ms >= 0),
    completed_at_ms INTEGER CHECK(completed_at_ms IS NULL OR completed_at_ms >= frozen_at_ms),
    runtime_entry_count INTEGER NOT NULL CHECK(runtime_entry_count >= 0),
    linked_target_count INTEGER NOT NULL CHECK(linked_target_count >= 0),
    no_linked_target_count INTEGER NOT NULL CHECK(no_linked_target_count >= 0),
    FOREIGN KEY(request_id) REFERENCES agent_profile_deletion_requests(request_id),
    FOREIGN KEY(profile_id) REFERENCES agent_profiles(profile_id),
    CHECK(runtime_entry_count = linked_target_count + no_linked_target_count)
);",
)
.as_str();

pub(super) const PROFILE_DELETION_RUNTIME_ENTRIES_TABLE_SQL: &str = StaticSql::ddl(
    "CREATE TABLE agent_profile_deletion_runtime_entries (
    request_id BLOB NOT NULL CHECK(length(request_id) = 16 AND request_id != zeroblob(16)),
    runtime_session_id BLOB NOT NULL CHECK(length(runtime_session_id) = 16 AND runtime_session_id != zeroblob(16)),
    provider_family INTEGER NOT NULL CHECK(provider_family = 1),
    provider_conversation_id TEXT CHECK(provider_conversation_id IS NULL OR length(CAST(provider_conversation_id AS BLOB)) BETWEEN 1 AND 512),
    original_state INTEGER NOT NULL CHECK(original_state BETWEEN 1 AND 8),
    original_revision INTEGER NOT NULL CHECK(original_revision > 0),
    PRIMARY KEY(request_id,runtime_session_id),
    FOREIGN KEY(request_id) REFERENCES agent_profile_deletion_manifests(request_id),
    FOREIGN KEY(runtime_session_id) REFERENCES agent_profile_runtime_sessions(runtime_session_id)
);",
)
.as_str();

pub(super) const UPDATE_PROFILE_LIFECYCLE_CAS_SQL: &str = StaticSql::dml(
    "UPDATE agent_profiles
SET lifecycle_state=?1,updated_at_ms=?2,revision=?3
WHERE profile_id=?4 AND revision=?5 AND lifecycle_state=?6",
)
.as_str();

pub(super) const INSERT_DELETION_REQUEST_SQL: &str = StaticSql::dml(
    "INSERT INTO agent_profile_deletion_requests(
    request_id,profile_id,expected_profile_revision,
    barrier_profile_revision,requested_at_ms
) VALUES(?1,?2,?3,?4,?5)",
)
.as_str();

pub(super) const SELECT_RUNTIME_ENTRY_SQL: &str = StaticSql::dml(
    "SELECT runtime_session_id,provider_family,provider_conversation_id,
       original_state,original_revision
FROM agent_profile_deletion_runtime_entries
WHERE request_id=?1 AND runtime_session_id=?2",
)
.as_str();

pub(super) const VALIDATE_MANIFEST_INVENTORY_SQL: &str = StaticSql::dml(
    "SELECT e.runtime_session_id,e.provider_family,e.provider_conversation_id,
       e.original_state,e.original_revision,r.profile_id,r.provider_family,
       r.provider_conversation_id,r.revision
FROM agent_profile_deletion_runtime_entries e
LEFT JOIN agent_profile_runtime_sessions r ON r.runtime_session_id=e.runtime_session_id
WHERE e.request_id=?1 ORDER BY e.runtime_session_id",
)
.as_str();

pub(super) const HAS_RUNTIME_TARGETS_SQL: &str = StaticSql::dml(
    "SELECT EXISTS(
    SELECT 1 FROM agent_profile_runtime_sessions WHERE profile_id=?1
)",
)
.as_str();

pub(super) const SELECT_INTENT_BY_REQUEST_SQL: &str = StaticSql::dml(
    "SELECT request_id,profile_id,expected_profile_revision,
       barrier_profile_revision,requested_at_ms
FROM agent_profile_deletion_requests WHERE request_id=?1",
)
.as_str();

pub(super) const SELECT_INTENT_BY_PROFILE_SQL: &str = StaticSql::dml(
    "SELECT request_id,profile_id,expected_profile_revision,
       barrier_profile_revision,requested_at_ms
FROM agent_profile_deletion_requests WHERE profile_id=?1",
)
.as_str();

pub(super) const SELECT_INTENTS_PAGE_SQL: &str = StaticSql::dml(
    "SELECT request_id,profile_id,expected_profile_revision,
       barrier_profile_revision,requested_at_ms
FROM agent_profile_deletion_requests
WHERE (?1 IS NULL OR request_id>?1)
ORDER BY request_id ASC LIMIT ?2",
)
.as_str();

pub(super) const SELECT_MANIFEST_BY_REQUEST_SQL: &str = StaticSql::dml(
    "SELECT request_id,profile_id,profile_revision_at_freeze,frozen_at_ms,
       completed_at_ms,runtime_entry_count,linked_target_count,
       no_linked_target_count
FROM agent_profile_deletion_manifests WHERE request_id=?1",
)
.as_str();

pub(super) const RUNTIME_ENTRY_STATS_SQL: &str = StaticSql::dml(
    "SELECT COUNT(*),
       COALESCE(SUM(CASE WHEN provider_conversation_id IS NOT NULL THEN 1 ELSE 0 END),0),
       COALESCE(SUM(CASE WHEN provider_conversation_id IS NULL THEN 1 ELSE 0 END),0),
       COALESCE(MAX(last_seen_at_ms),0)
FROM agent_profile_runtime_sessions WHERE profile_id=?1",
)
.as_str();

pub(super) const INSERT_MANIFEST_SQL: &str = StaticSql::dml(
    "INSERT INTO agent_profile_deletion_manifests(
    request_id,profile_id,profile_revision_at_freeze,frozen_at_ms,
    completed_at_ms,runtime_entry_count,linked_target_count,no_linked_target_count
) VALUES(?1,?2,?3,?4,NULL,?5,?6,?7)",
)
.as_str();

pub(super) const FREEZE_RUNTIME_ENTRIES_SQL: &str = StaticSql::dml(
    "INSERT INTO agent_profile_deletion_runtime_entries(
    request_id,runtime_session_id,provider_family,provider_conversation_id,
    original_state,original_revision
)
SELECT ?1,runtime_session_id,provider_family,provider_conversation_id,state,revision
FROM agent_profile_runtime_sessions WHERE profile_id=?2",
)
.as_str();

pub(super) const SELECT_RUNTIME_ENTRIES_PAGE_SQL: &str = StaticSql::dml(
    "SELECT runtime_session_id,provider_family,provider_conversation_id,
       original_state,original_revision
FROM agent_profile_deletion_runtime_entries
WHERE request_id=?1 AND (?2 IS NULL OR runtime_session_id>?2)
ORDER BY runtime_session_id ASC LIMIT ?3",
)
.as_str();

pub(super) const FROZEN_RUNTIME_STATS_SQL: &str = StaticSql::dml(
    "SELECT COUNT(*),COALESCE(MAX(r.last_seen_at_ms),0),
       COALESCE(SUM(CASE WHEN r.state!=?2 THEN 1 ELSE 0 END),0)
FROM agent_profile_deletion_runtime_entries e
JOIN agent_profile_runtime_sessions r
  ON r.runtime_session_id=e.runtime_session_id
WHERE e.request_id=?1 AND r.profile_id=?3",
)
.as_str();

pub(super) const HAS_MAX_REVISION_RUNTIME_ENTRY_SQL: &str = StaticSql::dml(
    "SELECT EXISTS(
    SELECT 1 FROM agent_profile_deletion_runtime_entries e
    JOIN agent_profile_runtime_sessions r
      ON r.runtime_session_id=e.runtime_session_id
    WHERE e.request_id=?1 AND r.profile_id=?2
      AND r.state!=?3 AND r.revision>=?4
)",
)
.as_str();

pub(super) const CLOSE_FROZEN_RUNTIME_ENTRIES_SQL: &str = StaticSql::dml(
    "UPDATE agent_profile_runtime_sessions
SET state=?1,closed_at_ms=?2,last_seen_at_ms=?2,revision=revision+1
WHERE state!=?1 AND runtime_session_id IN (
    SELECT e.runtime_session_id
    FROM agent_profile_deletion_runtime_entries e
    JOIN agent_profile_deletion_manifests m ON m.request_id=e.request_id
    WHERE e.request_id=?3 AND m.profile_id=?4
)",
)
.as_str();

pub(super) const MARK_MANIFEST_COMPLETED_SQL: &str = StaticSql::dml(
    "UPDATE agent_profile_deletion_manifests SET completed_at_ms=?1
WHERE request_id=?2 AND completed_at_ms IS NULL",
)
.as_str();
