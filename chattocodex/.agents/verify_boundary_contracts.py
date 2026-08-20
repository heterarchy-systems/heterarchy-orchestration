from __future__ import annotations

import ast
from pathlib import Path

SOURCE_ROOT = Path(__file__).resolve().parents[1]
BACKEND_ROOT = SOURCE_ROOT / "orchestrator_backend"
MCP_ROOT = SOURCE_ROOT / "orchestrator_mcp"
SHARED_ROOT = SOURCE_ROOT / "shared"

_MCP_IMPLEMENTATION_PREFIXES = (
    "chattocodex.orchestrator_backend.persistence",
    "chattocodex.orchestrator_backend.database",
    "chattocodex.orchestrator_backend.broker_client",
    "chattocodex.orchestrator_backend.control_panel.infrastructure",
)


def _python_files(root: Path) -> tuple[Path, ...]:
    """Return production Python files under one architectural boundary.

    Args:
        root: Boundary root to scan recursively.

    Returns:
        Sorted production Python files excluding bytecode cache directories.
    """
    return tuple(
        path for path in sorted(root.rglob("*.py")) if "__pycache__" not in path.parts
    )


def _relative(path: Path) -> str:
    """Return a stable path relative to the package source root.

    Args:
        path: Absolute or source-root-relative file path.

    Returns:
        POSIX-style path used in harness diagnostics.
    """
    return path.relative_to(SOURCE_ROOT).as_posix()


def _imported_modules(tree: ast.Module) -> tuple[tuple[int, str], ...]:
    """Collect imported module names with source line numbers.

    Args:
        tree: Parsed Python module AST.

    Returns:
        Imported module references as line-number/module-name pairs.
    """
    imports: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module is not None:
            imports.append((node.lineno, node.module))
            continue
        if isinstance(node, ast.Import):
            imports.extend((node.lineno, alias.name) for alias in node.names)
    return tuple(imports)


def _check_forbidden_prefixes(
    *,
    root: Path,
    forbidden_prefixes: tuple[str, ...],
    boundary_name: str,
    errors: list[str],
) -> None:
    """Reject imports that cross a forbidden architectural direction.

    Args:
        root: Boundary root whose files are inspected.
        forbidden_prefixes: Module prefixes forbidden from that boundary.
        boundary_name: Human-readable boundary label for diagnostics.
        errors: Mutable error accumulator owned by the harness entry point.

    Returns:
        None.
    """
    for path in _python_files(root):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for lineno, module in _imported_modules(tree):
            if not any(module.startswith(prefix) for prefix in forbidden_prefixes):
                continue
            errors.append(
                f"{_relative(path)}:{lineno}: {boundary_name} imports forbidden "
                f"implementation boundary {module!r}"
            )


def main() -> int:
    """Verify one-way shared/backend/MCP dependency boundaries.

    Args:
        None.

    Returns:
        Zero when architectural imports are valid, otherwise one.
    """
    errors: list[str] = []
    _check_forbidden_prefixes(
        root=BACKEND_ROOT,
        forbidden_prefixes=("chattocodex.orchestrator_mcp",),
        boundary_name="orchestrator_backend",
        errors=errors,
    )
    _check_forbidden_prefixes(
        root=SHARED_ROOT,
        forbidden_prefixes=(
            "chattocodex.orchestrator_backend",
            "chattocodex.orchestrator_mcp",
        ),
        boundary_name="shared",
        errors=errors,
    )
    _check_forbidden_prefixes(
        root=MCP_ROOT,
        forbidden_prefixes=_MCP_IMPLEMENTATION_PREFIXES,
        boundary_name="orchestrator_mcp",
        errors=errors,
    )
    if errors:
        for error in errors:
            print(error)
        return 1
    print("boundary contract verification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
