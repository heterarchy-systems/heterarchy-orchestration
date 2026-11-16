//! Canonical compile-time validated SQL statements for the Profile resolver and reservation claims.

use crate::static_sql::StaticSql;

pub(super) const RESOLUTION_CLAIMS_SQL: &str = StaticSql::ddl(
    "CREATE TABLE agent_profile_resolution_claims (
    claim_id BLOB PRIMARY KEY NOT NULL CHECK(length(claim_id) = 16 AND claim_id != zeroblob(16)),
    agent_request_id BLOB NOT NULL CHECK(length(agent_request_id) = 16 AND agent_request_id != zeroblob(16)),
    slot_ordinal INTEGER NOT NULL CHECK(slot_ordinal BETWEEN 0 AND 3),
    claim_kind INTEGER NOT NULL CHECK(claim_kind BETWEEN 1 AND 4),
    profile_id BLOB NOT NULL CHECK(length(profile_id) = 16 AND profile_id != zeroblob(16)),
    profile_revision INTEGER NOT NULL CHECK(profile_revision > 0),
    runtime_session_id BLOB CHECK(runtime_session_id IS NULL OR (length(runtime_session_id) = 16 AND runtime_session_id != zeroblob(16))),
    runtime_session_revision INTEGER CHECK(runtime_session_revision IS NULL OR runtime_session_revision > 0),
    claimed_at_ms INTEGER NOT NULL CHECK(claimed_at_ms >= 0),
    released_at_ms INTEGER CHECK(released_at_ms IS NULL OR released_at_ms >= claimed_at_ms),
    state INTEGER NOT NULL CHECK(state IN (1,2)),
    revision INTEGER NOT NULL CHECK(revision > 0),
    UNIQUE(agent_request_id, slot_ordinal),
    FOREIGN KEY(profile_id) REFERENCES agent_profiles(profile_id),
    FOREIGN KEY(runtime_session_id) REFERENCES agent_profile_runtime_sessions(runtime_session_id),
    CHECK((state = 1 AND released_at_ms IS NULL) OR (state = 2 AND released_at_ms IS NOT NULL)),
    CHECK((claim_kind IN (2,3) AND runtime_session_id IS NOT NULL AND runtime_session_revision IS NOT NULL) OR (claim_kind IN (1,4) AND runtime_session_id IS NULL AND runtime_session_revision IS NULL))
);",
)
.as_str();

pub(super) const ACTIVE_RESOLUTION_CLAIM_INDEX_SQL: &str = StaticSql::ddl(
    "CREATE UNIQUE INDEX agent_profile_resolution_claims_active_profile_idx
ON agent_profile_resolution_claims(profile_id)
WHERE state = 1;",
)
.as_str();

pub(super) const SELECT_INDEX_SCHEMA_SQL: &str =
    StaticSql::dml("SELECT sql FROM sqlite_schema WHERE type='index' AND name=?1").as_str();

pub(super) const RESOLVER_CANDIDATES_SQL: &str = StaticSql::dml(
    "WITH eligible AS (
    SELECT p.profile_id,p.revision,p.last_used_at_ms,p.updated_at_ms
    FROM agent_profiles p
    JOIN agent_profile_capabilities c ON c.profile_id=p.profile_id
    WHERE p.lifecycle_state=1
      AND p.provider_family=1
      AND c.capability=?1
      AND NOT EXISTS (
          SELECT 1 FROM agent_profile_resolution_claims rc
          WHERE rc.profile_id=p.profile_id AND rc.state=1
      )
), ranked_runtime AS (
    SELECT r.runtime_session_id,r.profile_id,r.state,r.revision,r.last_seen_at_ms,
           ROW_NUMBER() OVER (
               PARTITION BY r.profile_id,r.state
               ORDER BY r.last_seen_at_ms DESC,r.runtime_session_id DESC
           ) AS state_rank
    FROM agent_profile_runtime_sessions r
    JOIN eligible e ON e.profile_id=r.profile_id
), status AS (
    SELECT profile_id,
           MAX(CASE WHEN state=1 THEN 1 ELSE 0 END) AS has_provisioning,
           MAX(CASE WHEN state=3 THEN 1 ELSE 0 END) AS has_active,
           MAX(CASE WHEN state=4 THEN 1 ELSE 0 END) AS has_suspending,
           MAX(CASE WHEN state=6 THEN 1 ELSE 0 END) AS has_waking,
           MAX(CASE WHEN state=7 THEN 1 ELSE 0 END) AS has_failed
    FROM ranked_runtime
    GROUP BY profile_id
)
SELECT e.profile_id,e.revision,e.last_used_at_ms,e.updated_at_ms,
       ready.runtime_session_id,ready.revision,
       offline.runtime_session_id,offline.revision,
       COALESCE(s.has_provisioning,0),COALESCE(s.has_active,0),
       COALESCE(s.has_suspending,0),COALESCE(s.has_waking,0),COALESCE(s.has_failed,0)
FROM eligible e
LEFT JOIN ranked_runtime ready
       ON ready.profile_id=e.profile_id AND ready.state=2 AND ready.state_rank=1
LEFT JOIN ranked_runtime offline
       ON offline.profile_id=e.profile_id AND offline.state=5 AND offline.state_rank=1
LEFT JOIN status s ON s.profile_id=e.profile_id
ORDER BY COALESCE(e.last_used_at_ms,-1) DESC,e.updated_at_ms DESC,e.profile_id ASC
LIMIT ?2",
)
.as_str();

pub(super) const CLAIM_BY_REQUEST_SLOT_SQL: &str = StaticSql::dml(
    "SELECT claim_id,agent_request_id,slot_ordinal,claim_kind,profile_id,profile_revision,
       runtime_session_id,runtime_session_revision,claimed_at_ms,revision
FROM agent_profile_resolution_claims
WHERE agent_request_id=?1 AND slot_ordinal=?2 AND state=1",
)
.as_str();

pub(super) const CLAIM_BY_ID_SQL: &str = StaticSql::dml(
    "SELECT claim_id,agent_request_id,slot_ordinal,claim_kind,profile_id,profile_revision,
       runtime_session_id,runtime_session_revision,claimed_at_ms,revision
FROM agent_profile_resolution_claims WHERE claim_id=?1 AND state=1",
)
.as_str();

pub(super) const ACTIVE_CLAIM_FOR_PROFILE_SQL: &str = StaticSql::dml(
    "SELECT claim_id,agent_request_id,slot_ordinal,claim_kind,profile_id,profile_revision,
       runtime_session_id,runtime_session_revision,claimed_at_ms,revision
FROM agent_profile_resolution_claims WHERE profile_id=?1 AND state=1",
)
.as_str();
