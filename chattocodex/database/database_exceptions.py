class DatabaseReadModelError(RuntimeError):
    """Base class for optional PostgreSQL read-model failures."""


class DatabaseDriverUnavailableError(DatabaseReadModelError):
    """Raised when the optional psycopg driver is not installed."""


class DatabaseConnectionError(DatabaseReadModelError):
    """Raised when the configured PostgreSQL database cannot be reached or queried."""
