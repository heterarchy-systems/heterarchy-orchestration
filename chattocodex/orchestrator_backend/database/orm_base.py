from __future__ import annotations

from sqlalchemy.orm import DeclarativeBase


class OrmBase(DeclarativeBase):
    """Canonical SQLAlchemy declarative base for orchestration control-plane tables."""
