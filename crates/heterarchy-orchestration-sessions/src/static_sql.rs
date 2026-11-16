//! Compile-time validated static SQL wrapper for Sessions persistence queries.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StaticSql(&'static str);

impl StaticSql {
    #[must_use]
    pub(crate) const fn ddl(sql: &'static str) -> Self {
        assert!(!sql.is_empty(), "DDL SQL must not be empty");
        Self(sql)
    }

    #[must_use]
    pub(crate) const fn dml(sql: &'static str) -> Self {
        let bytes = sql.as_bytes();
        assert!(!bytes.is_empty(), "DML SQL must not be empty");
        let mut index = 0;
        while index < bytes.len() {
            let byte = bytes[index];
            assert!(
                byte != b';',
                "DML SQL must not contain stacked statement semicolons"
            );
            assert!(
                byte != b'{' && byte != b'}',
                "DML SQL must not contain format braces"
            );
            if index + 1 < bytes.len() {
                let next = bytes[index + 1];
                assert!(
                    !(byte == b'-' && next == b'-'),
                    "DML SQL must not contain line comments"
                );
                assert!(
                    !(byte == b'/' && next == b'*'),
                    "DML SQL must not contain block comments"
                );
            }
            index += 1;
        }
        Self(sql)
    }

    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        self.0
    }
}
