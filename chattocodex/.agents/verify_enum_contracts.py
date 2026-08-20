from __future__ import annotations

import ast
from pathlib import Path

SOURCE_ROOT = Path(__file__).resolve().parents[1]
LOCAL_ENUM_ALLOW_MARKER = "enum-contract: local"


def _production_files() -> tuple[Path, ...]:
    return tuple(
        path
        for path in sorted(SOURCE_ROOT.rglob("*.py"))
        if ".agents" not in path.parts and "__pycache__" not in path.parts
    )


def _relative(path: Path) -> str:
    return path.relative_to(SOURCE_ROOT).as_posix()


def _base_name(base: ast.expr) -> str:
    if isinstance(base, ast.Name):
        return base.id
    if isinstance(base, ast.Attribute):
        return base.attr
    return ""


def _has_local_allow_marker(
    source_lines: tuple[str, ...],
    lineno: int,
) -> bool:
    current = source_lines[lineno - 1] if lineno <= len(source_lines) else ""
    previous = source_lines[lineno - 2] if lineno > 1 else ""
    return LOCAL_ENUM_ALLOW_MARKER in current or LOCAL_ENUM_ALLOW_MARKER in previous


def _check_enum_placement(
    path: Path,
    tree: ast.Module,
    source_lines: tuple[str, ...],
    errors: list[str],
) -> None:
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        base_names = {_base_name(base) for base in node.bases}
        if "StrEnum" not in base_names:
            continue
        if path.name.endswith("_enums.py"):
            continue
        if _has_local_allow_marker(source_lines, node.lineno):
            continue
        errors.append(
            f"{_relative(path)}:{node.lineno}: StrEnum {node.name} must be declared in a "
            "feature-owned '*_enums.py' module; use an adjacent "
            f"'{LOCAL_ENUM_ALLOW_MARKER} <reason>' only for an unavoidable local enum"
        )


def _check_legacy_enum_bases(
    path: Path,
    tree: ast.Module,
    errors: list[str],
) -> None:
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        base_names = {_base_name(base) for base in node.bases}
        if "Enum" in base_names and "str" in base_names:
            errors.append(
                f"{_relative(path)}:{node.lineno}: {node.name} uses 'str, Enum'; use StrEnum"
            )


def main() -> int:
    errors: list[str] = []
    enum_count = 0
    for path in _production_files():
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        source_lines = tuple(source.splitlines())
        enum_count += sum(
            1
            for node in ast.walk(tree)
            if isinstance(node, ast.ClassDef)
            and any(_base_name(base) == "StrEnum" for base in node.bases)
        )
        _check_enum_placement(path, tree, source_lines, errors)
        _check_legacy_enum_bases(path, tree, errors)
    if errors:
        for error in errors:
            print(error)
        return 1
    print(f"enum contract verification passed ({enum_count} StrEnum declarations)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
