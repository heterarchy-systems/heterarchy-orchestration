from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from inspect import isawaitable

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlAlchemyResources:
    """Own the application-lifetime async engine and request-scoped session factory."""

    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]


SqlAlchemyResourcesProvider = Callable[
    [], SqlAlchemyResources | Awaitable[SqlAlchemyResources]
]


async def resolve_sqlalchemy_resources(
    provider: SqlAlchemyResourcesProvider,
) -> SqlAlchemyResources:
    """Resolve one Dependency Injector Resource without leaking awaitables to services. Args: provider. Returns: SqlAlchemyResources."""

    provided = provider()
    return await provided if isawaitable(provided) else provided


def normalize_async_postgres_url(database_url: str) -> str:
    """Normalize a PostgreSQL URL to the repository's asyncpg SQLAlchemy dialect. Args: database_url. Returns: str."""

    normalized = database_url.strip()
    if normalized.startswith("postgresql+asyncpg://"):
        return normalized
    if normalized.startswith("postgresql://"):
        return normalized.replace("postgresql://", "postgresql+asyncpg://", 1)
    if normalized.startswith("postgres://"):
        return normalized.replace("postgres://", "postgresql+asyncpg://", 1)
    raise ValueError("database_url must use PostgreSQL with the asyncpg driver")


@asynccontextmanager
async def init_sqlalchemy_resources(
    database_url: str,
    pool_size: int = 10,
    max_overflow: int = 10,
    pool_timeout_seconds: float = 5.0,
) -> AsyncIterator[SqlAlchemyResources]:
    """Create one bounded async engine and dispose it deterministically at shutdown. Args: database_url, pool_size, max_overflow, pool_timeout_seconds. Returns: AsyncIterator[SqlAlchemyResources]."""

    if pool_size <= 0:
        raise ValueError("pool_size must be positive")
    if max_overflow < 0:
        raise ValueError("max_overflow must not be negative")
    if pool_timeout_seconds <= 0:
        raise ValueError("pool_timeout_seconds must be positive")

    engine = create_async_engine(
        normalize_async_postgres_url(database_url),
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout_seconds,
    )
    resources = SqlAlchemyResources(
        engine=engine,
        session_factory=async_sessionmaker(
            bind=engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        ),
    )
    try:
        yield resources
    finally:
        await engine.dispose()
