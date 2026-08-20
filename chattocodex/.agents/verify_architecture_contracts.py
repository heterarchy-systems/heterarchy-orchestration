from __future__ import annotations

import ast
from pathlib import Path

SOURCE_ROOT = Path(__file__).resolve().parents[1]
REVIEW_LINE_LIMIT = 430
DIRECT_MODULE_LIMIT = 10
GENERIC_MODULE_NAMES = frozenset(
    {
        "common.py",
        "helpers.py",
        "manager.py",
        "misc.py",
        "models.py",
        "schema.py",
        "types.py",
        "utils.py",
    }
)

# Existing debt is a shrink-only baseline. A listed module may become smaller,
# but CI rejects growth beyond the reviewed snapshot and rejects any new module
# that crosses the 430-line review trigger.
LEGACY_OVERSIZED_MODULES: dict[str, int] = {}

# Dense packages are also shrink-only debt. New packages must split by a real
# concept/responsibility boundary before they exceed the direct-module limit.
LEGACY_DENSE_PACKAGES: dict[str, int] = {}


def _production_files() -> tuple[Path, ...]:
    return tuple(
        path
        for path in sorted(SOURCE_ROOT.rglob("*.py"))
        if ".agents" not in path.parts and "__pycache__" not in path.parts
    )


def _relative(path: Path) -> str:
    return path.relative_to(SOURCE_ROOT).as_posix()


def _check_package_density(errors: list[str]) -> None:
    directories = (
        SOURCE_ROOT,
        *(
            path
            for path in SOURCE_ROOT.rglob("*")
            if path.is_dir()
            and ".agents" not in path.parts
            and "__pycache__" not in path.parts
        ),
    )
    for directory in sorted(directories):
        direct_module_count = sum(
            1 for path in directory.glob("*.py") if path.name != "__init__.py"
        )
        relative = directory.relative_to(SOURCE_ROOT).as_posix() or "."
        baseline = LEGACY_DENSE_PACKAGES.get(relative)
        if baseline is not None:
            if direct_module_count > baseline:
                errors.append(
                    f"{relative}: dense legacy package grew from reviewed baseline "
                    f"{baseline} to {direct_module_count} direct modules; split by "
                    "concept/responsibility instead"
                )
            continue
        if direct_module_count > DIRECT_MODULE_LIMIT:
            errors.append(
                f"{relative}: package has {direct_module_count} direct modules, exceeding "
                f"the {DIRECT_MODULE_LIMIT}-module architecture review trigger; split by "
                "concept/responsibility"
            )


def _call_name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _contains_model_dump(node: ast.AST) -> bool:
    return any(
        isinstance(candidate, ast.Call) and _call_name(candidate.func) == "model_dump"
        for candidate in ast.walk(node)
    )


def _check_module_name(path: Path, errors: list[str]) -> None:
    if path.name in GENERIC_MODULE_NAMES:
        errors.append(
            f"{_relative(path)}: generic module name is forbidden; use a concept-specific "
            "Clean Architecture role name"
        )


def _check_module_size(path: Path, errors: list[str]) -> None:
    relative = _relative(path)
    line_count = len(path.read_text(encoding="utf-8").splitlines())
    baseline = LEGACY_OVERSIZED_MODULES.get(relative)
    if baseline is not None:
        if line_count > baseline:
            errors.append(
                f"{relative}: oversized legacy module grew from reviewed baseline "
                f"{baseline} to {line_count} lines; split responsibilities instead"
            )
        return
    if line_count > REVIEW_LINE_LIMIT:
        errors.append(
            f"{relative}: new/unreviewed module has {line_count} lines, exceeding the "
            f"{REVIEW_LINE_LIMIT}-line architecture review trigger"
        )


def _check_boundary_roundtrips(path: Path, errors: list[str]) -> None:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if _call_name(node.func) == "model_validate" and any(
            _contains_model_dump(argument) for argument in node.args
        ):
            errors.append(
                f"{_relative(path)}:{node.lineno}: do not serialize a Pydantic model "
                "with model_dump() and re-validate it; use a typed mapper"
            )
        errors.extend(
            (
                f"{_relative(path)}:{node.lineno}: do not spread model_dump() into "
                "another boundary constructor; map typed fields explicitly"
            )
            for keyword in node.keywords
            if keyword.arg is None and _contains_model_dump(keyword.value)
        )


def main() -> int:
    errors: list[str] = []
    _check_package_density(errors)
    for path in _production_files():
        _check_module_name(path, errors)
        _check_module_size(path, errors)
        _check_boundary_roundtrips(path, errors)
    if errors:
        for error in errors:
            print(error)
        return 1
    print("architecture contract verification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
