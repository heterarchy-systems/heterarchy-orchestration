from __future__ import annotations

import ast
from pathlib import Path

SOURCE_ROOT = Path(__file__).resolve().parents[1]
DATABASE_ROOT = SOURCE_ROOT / "orchestrator_backend" / "database"
MIGRATION_SERVICE = DATABASE_ROOT / "async_postgres_migration_service.py"
ORM_BASE = DATABASE_ROOT / "orm_base.py"


def _call_name(node: ast.expr) -> str | None:
    """Resolve the terminal name of a simple call expression. Args: node. Returns: Call name or None."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _require_orm_schema(errors: list[str]) -> None:
    """Verify ORM metadata owns database bootstrap. Args: errors. Returns: None."""
    migration_tree = ast.parse(MIGRATION_SERVICE.read_text(encoding="utf-8"))
    create_all_seen = any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "create_all"
        for node in ast.walk(migration_tree)
    )
    if not create_all_seen:
        errors.append(
            "orchestrator_backend/database/async_postgres_migration_service.py: "
            "schema reconciliation must be driven by SQLAlchemy metadata.create_all"
        )
    base_tree = ast.parse(ORM_BASE.read_text(encoding="utf-8"))
    declarative_base_seen = any(
        isinstance(node, ast.ClassDef)
        and any(_call_name(base) == "DeclarativeBase" for base in node.bases)
        for node in base_tree.body
    )
    if not declarative_base_seen:
        errors.append(
            "orchestrator_backend/database/orm_base.py: OrmBase must inherit SQLAlchemy DeclarativeBase"
        )


def main() -> int:
    """Validate the ORM-only production database contract. Args: None. Returns: Process exit code."""
    errors: list[str] = []
    sql_files = tuple(
        path
        for path in SOURCE_ROOT.rglob("*.sql")
        if ".agents" not in path.parts and "__pycache__" not in path.parts
    )
    for path in sql_files:
        errors.append(
            f"{path.relative_to(SOURCE_ROOT).as_posix()}: production SQL migration files are forbidden; use typed SQLAlchemy ORM metadata or Python migration operations"
        )
    _require_orm_schema(errors)
    if errors:
        for error in errors:
            print(error)
        return 1
    print("database contract verification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
