//! Canonical compile-time validated SQL statements for Profile Home acceptance pins.

use crate::static_sql::StaticSql;

pub(super) const PINS_TABLE_SQL: &str = StaticSql::ddl(
    "CREATE TABLE agent_profile_home_acceptance_pins (
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
);",
)
.as_str();

pub(super) const ACTIVE_PROFILE_INDEX_SQL: &str = StaticSql::ddl(
    "CREATE INDEX agent_profile_home_acceptance_active_profile_idx
ON agent_profile_home_acceptance_pins(profile_id,acceptance_id) WHERE state=1;",
)
.as_str();

pub(super) const PENDING_ISSUER_INDEX_SQL: &str = StaticSql::ddl(
    "CREATE INDEX agent_profile_home_acceptance_pending_issuer_idx
ON agent_profile_home_acceptance_pins(issuer_id,acceptance_id) WHERE state=1;",
)
.as_str();

pub(super) const PROFILE_MUTATION_GUARD_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER agent_profiles_home_acceptance_mutation_guard
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
BEGIN SELECT RAISE(ABORT,'Profile is pinned by a Home acceptance'); END;",
)
.as_str();

pub(super) const PIN_TRANSITION_GUARD_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER agent_profile_home_acceptance_transition_guard
BEFORE UPDATE ON agent_profile_home_acceptance_pins
WHEN OLD.state!=1 OR NEW.state NOT IN (2,3)
 OR NEW.acceptance_id IS NOT OLD.acceptance_id OR NEW.issuer_id IS NOT OLD.issuer_id
 OR NEW.request_digest IS NOT OLD.request_digest OR NEW.request_snapshot IS NOT OLD.request_snapshot
 OR NEW.profile_id IS NOT OLD.profile_id OR NEW.profile_revision IS NOT OLD.profile_revision
 OR NEW.logical_session_request_id IS NOT OLD.logical_session_request_id
 OR NEW.linked_profile_revision IS NOT OLD.linked_profile_revision
 OR NEW.linked_session_revision IS NOT OLD.linked_session_revision
 OR NEW.acquired_at_ms IS NOT OLD.acquired_at_ms OR NEW.created_at_ms IS NOT OLD.created_at_ms
BEGIN SELECT RAISE(ABORT,'Home Profile pin identity is immutable'); END;",
)
.as_str();

pub(super) const PIN_DELETE_GUARD_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER agent_profile_home_acceptance_no_delete
BEFORE DELETE ON agent_profile_home_acceptance_pins
BEGIN SELECT RAISE(ABORT,'Home Profile pin history is permanent'); END;",
)
.as_str();

pub(super) const CAPABILITY_INSERT_GUARD_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER agent_profile_capabilities_home_acceptance_insert_guard
BEFORE INSERT ON agent_profile_capabilities
WHEN EXISTS(SELECT 1 FROM agent_profile_home_acceptance_pins h
             WHERE h.profile_id=NEW.profile_id AND h.state=1)
BEGIN SELECT RAISE(ABORT,'Profile capabilities are pinned by a Home acceptance'); END;",
)
.as_str();

pub(super) const CAPABILITY_UPDATE_GUARD_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER agent_profile_capabilities_home_acceptance_update_guard
BEFORE UPDATE ON agent_profile_capabilities
WHEN (NEW.profile_id IS NOT OLD.profile_id OR NEW.capability IS NOT OLD.capability)
 AND (EXISTS(SELECT 1 FROM agent_profile_home_acceptance_pins h
              WHERE h.profile_id=OLD.profile_id AND h.state=1)
   OR EXISTS(SELECT 1 FROM agent_profile_home_acceptance_pins h
              WHERE h.profile_id=NEW.profile_id AND h.state=1))
BEGIN SELECT RAISE(ABORT,'Profile capabilities are pinned by a Home acceptance'); END;",
)
.as_str();

pub(super) const CAPABILITY_DELETE_GUARD_SQL: &str = StaticSql::ddl(
    "CREATE TRIGGER agent_profile_capabilities_home_acceptance_delete_guard
BEFORE DELETE ON agent_profile_capabilities
WHEN EXISTS(SELECT 1 FROM agent_profile_home_acceptance_pins h
             WHERE h.profile_id=OLD.profile_id AND h.state=1)
BEGIN SELECT RAISE(ABORT,'Profile capabilities are pinned by a Home acceptance'); END;",
)
.as_str();

pub(super) const SELECT_SCHEMA_SQL: &str =
    StaticSql::dml("SELECT sql FROM sqlite_schema WHERE type=?1 AND name=?2").as_str();

pub(super) const TABLE_INFO_PINS_SQL: &str =
    StaticSql::dml("PRAGMA table_info(agent_profile_home_acceptance_pins)").as_str();

pub(super) const HAS_ACTIVE_PIN_FOR_PROFILE_SQL: &str = StaticSql::dml(
    "SELECT EXISTS(SELECT 1 FROM agent_profile_home_acceptance_pins WHERE profile_id=?1 AND state=1)",
)
.as_str();

pub(super) const SELECT_PENDING_IDS_SQL: &str = StaticSql::dml(
    "SELECT substr(acceptance_id,1,17),length(acceptance_id)
FROM agent_profile_home_acceptance_pins
WHERE issuer_id=?1 AND state=1 AND (?2 IS NULL OR acceptance_id>?2)
ORDER BY acceptance_id LIMIT ?3",
)
.as_str();

pub(super) const INSERT_PIN_SQL: &str = StaticSql::dml(
    "INSERT INTO agent_profile_home_acceptance_pins(
    acceptance_id,issuer_id,request_digest,request_snapshot,state,
    profile_id,profile_revision,logical_session_request_id,
    linked_profile_revision,linked_session_revision,acquired_at_ms,
    source_outcome,source_payload_digest,source_observed_at_ms,
    operations_resolution_id,operations_resolution_digest,
    operations_admission_id,operations_admission_digest,
    operations_profile_pin_present,created_at_ms,resolved_at_ms
) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21)",
)
.as_str();

pub(super) const LOAD_PIN_SQL: &str = StaticSql::dml(
    "SELECT substr(acceptance_id,1,17),length(acceptance_id),
       substr(issuer_id,1,17),length(issuer_id),
       substr(request_digest,1,33),length(request_digest),
       substr(request_snapshot,1,321),length(request_snapshot),state,
       substr(profile_id,1,17),length(profile_id),profile_revision,
       substr(logical_session_request_id,1,17),length(logical_session_request_id),
       linked_profile_revision,linked_session_revision,acquired_at_ms,
       source_outcome,substr(source_payload_digest,1,33),length(source_payload_digest),
       source_observed_at_ms,substr(operations_resolution_id,1,17),
       length(operations_resolution_id),substr(operations_resolution_digest,1,33),
       length(operations_resolution_digest),substr(operations_admission_id,1,17),
       length(operations_admission_id),substr(operations_admission_digest,1,33),
       length(operations_admission_digest),operations_profile_pin_present,
       created_at_ms,resolved_at_ms
FROM agent_profile_home_acceptance_pins WHERE acceptance_id=?1",
)
.as_str();

pub(super) const UPDATE_PIN_SQL: &str = StaticSql::dml(
    "UPDATE agent_profile_home_acceptance_pins
SET state=?1,source_outcome=?2,source_payload_digest=?3,source_observed_at_ms=?4,
    operations_resolution_id=?5,operations_resolution_digest=?6,
    operations_admission_id=?7,operations_admission_digest=?8,
    operations_profile_pin_present=?9,resolved_at_ms=?10
WHERE acceptance_id=?11 AND state=?12",
)
.as_str();

pub(super) const OBJECTS: &[(&str, &str, &str)] = &[
    (
        "table",
        "agent_profile_home_acceptance_pins",
        PINS_TABLE_SQL,
    ),
    (
        "index",
        "agent_profile_home_acceptance_active_profile_idx",
        ACTIVE_PROFILE_INDEX_SQL,
    ),
    (
        "index",
        "agent_profile_home_acceptance_pending_issuer_idx",
        PENDING_ISSUER_INDEX_SQL,
    ),
    (
        "trigger",
        "agent_profiles_home_acceptance_mutation_guard",
        PROFILE_MUTATION_GUARD_SQL,
    ),
    (
        "trigger",
        "agent_profile_home_acceptance_transition_guard",
        PIN_TRANSITION_GUARD_SQL,
    ),
    (
        "trigger",
        "agent_profile_home_acceptance_no_delete",
        PIN_DELETE_GUARD_SQL,
    ),
    (
        "trigger",
        "agent_profile_capabilities_home_acceptance_insert_guard",
        CAPABILITY_INSERT_GUARD_SQL,
    ),
    (
        "trigger",
        "agent_profile_capabilities_home_acceptance_update_guard",
        CAPABILITY_UPDATE_GUARD_SQL,
    ),
    (
        "trigger",
        "agent_profile_capabilities_home_acceptance_delete_guard",
        CAPABILITY_DELETE_GUARD_SQL,
    ),
];
