//! Compile-time validated static SQL statements for the MCP token catalog.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct StaticSql(&'static str);

impl StaticSql {
    pub(super) const fn ddl(sql: &'static str) -> Self {
        assert!(!sql.is_empty(), "DDL SQL must not be empty");
        Self(sql)
    }

    pub(super) const fn dml(sql: &'static str) -> Self {
        let bytes = sql.as_bytes();
        assert!(!bytes.is_empty(), "DML SQL must not be empty");
        let mut idx = 0;
        while idx < bytes.len() {
            let byte = bytes[idx];
            assert!(
                byte != b';',
                "DML SQL must not contain ';' stacked query separator"
            );
            assert!(
                byte != b'{' && byte != b'}',
                "DML SQL must not contain format braces"
            );
            if idx + 1 < bytes.len() {
                let next = bytes[idx + 1];
                assert!(
                    !(byte == b'-' && next == b'-'),
                    "DML SQL must not contain '--' line comment"
                );
                assert!(
                    !(byte == b'/' && next == b'*'),
                    "DML SQL must not contain '/*' block comment"
                );
            }
            idx += 1;
        }
        Self(sql)
    }

    pub(super) const fn as_str(self) -> &'static str {
        self.0
    }
}

pub(super) const TOKENS_TABLE_SQL: &str = StaticSql::ddl(
    "CREATE TABLE mcp_tokens (
    token_id BLOB PRIMARY KEY NOT NULL CHECK(length(token_id) = 16),
    token_digest BLOB NOT NULL UNIQUE CHECK(length(token_digest) = 32),
    state INTEGER NOT NULL CHECK(state IN (1, 2, 3)),
    label TEXT NOT NULL CHECK(length(CAST(label AS BLOB)) BETWEEN 1 AND 128),
    created_at_ms INTEGER NOT NULL,
    disabled_at_ms INTEGER,
    replacement_token_id BLOB REFERENCES mcp_tokens(token_id),
    revision INTEGER NOT NULL CHECK(revision > 0),
    CHECK(
        (state = 1 AND disabled_at_ms IS NULL AND replacement_token_id IS NULL)
        OR (state = 2 AND disabled_at_ms IS NOT NULL AND replacement_token_id IS NULL)
        OR (state = 3 AND disabled_at_ms IS NOT NULL AND replacement_token_id IS NOT NULL)
    ),
    CHECK(disabled_at_ms IS NULL OR disabled_at_ms >= created_at_ms),
    CHECK(replacement_token_id IS NULL OR replacement_token_id != token_id)
);",
)
.as_str();

pub(super) const TOKENS_INDEX_SQL: &str = StaticSql::ddl(
    "CREATE INDEX mcp_tokens_state_created_idx
ON mcp_tokens(state, created_at_ms DESC, token_id);",
)
.as_str();

pub(super) const FIND_ACTIVE_BY_DIGEST_SQL: &str =
    StaticSql::dml("SELECT token_id FROM mcp_tokens WHERE token_digest = ?1 AND state = 1")
        .as_str();

pub(super) const LIST_TOKEN_IDS_SQL: &str = StaticSql::dml(
    "SELECT token_id FROM mcp_tokens ORDER BY created_at_ms DESC, token_id LIMIT ?1",
)
.as_str();

pub(super) const DELETE_TOKEN_SQL: &str =
    StaticSql::dml("DELETE FROM mcp_tokens WHERE token_id = ?1 AND revision = ?2").as_str();

pub(super) const COUNT_SCHEMA_OBJECTS_SQL: &str =
    StaticSql::dml("SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'").as_str();

pub(super) const SELECT_SCHEMA_SQL: &str =
    StaticSql::dml("SELECT sql FROM sqlite_schema WHERE type = ?1 AND name = ?2").as_str();

pub(super) const INSERT_TOKEN_SQL: &str = StaticSql::dml(
    "INSERT INTO mcp_tokens(
    token_id,
    token_digest,
    state,
    label,
    created_at_ms,
    disabled_at_ms,
    replacement_token_id,
    revision
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
)
.as_str();

pub(super) const UPDATE_TERMINAL_TOKEN_SQL: &str = StaticSql::dml(
    "UPDATE mcp_tokens
SET state = ?1,
    disabled_at_ms = ?2,
    replacement_token_id = ?3,
    revision = ?4
WHERE token_id = ?5 AND revision = ?6 AND state = 1",
)
.as_str();

pub(super) const LOAD_TOKEN_SQL: &str = StaticSql::dml(
    "SELECT
    token_digest,
    state,
    label,
    created_at_ms,
    disabled_at_ms,
    replacement_token_id,
    revision
FROM mcp_tokens
WHERE token_id = ?1",
)
.as_str();

pub(super) const PRAGMA_QUICK_CHECK_SQL: &str = StaticSql::dml("PRAGMA quick_check(1)").as_str();

pub(super) const PRAGMA_FOREIGN_KEY_CHECK_SQL: &str =
    StaticSql::dml("PRAGMA foreign_key_check").as_str();
