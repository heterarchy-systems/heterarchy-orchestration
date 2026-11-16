//! Zero-cost compile-time validated static SQL wrapper.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StaticSql(&'static str);

impl StaticSql {
    #[must_use]
    pub(crate) const fn ddl(sql: &'static str) -> Self {
        assert!(!sql.is_empty(), "static DDL SQL must not be empty");
        Self(sql)
    }

    #[must_use]
    pub(crate) const fn dml(sql: &'static str) -> Self {
        let bytes = sql.as_bytes();
        assert!(!bytes.is_empty(), "static DML SQL must not be empty");
        let mut idx = 0;
        while idx < bytes.len() {
            let byte = bytes[idx];
            assert!(
                byte != b';',
                "static DML SQL must not contain ';' statement terminators"
            );
            assert!(
                byte != b'{' && byte != b'}',
                "static DML SQL must not contain format braces"
            );
            if idx + 1 < bytes.len() {
                assert!(
                    !(byte == b'-' && bytes[idx + 1] == b'-'),
                    "static DML SQL must not contain '--' line comments"
                );
                assert!(
                    !(byte == b'/' && bytes[idx + 1] == b'*'),
                    "static DML SQL must not contain '/*' block comments"
                );
            }
            idx += 1;
        }
        Self(sql)
    }

    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        self.0
    }
}
