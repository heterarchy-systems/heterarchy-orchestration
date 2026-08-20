#!/usr/bin/env python3
"""Fail closed when production schema boundaries bypass repository contracts."""

from __future__ import annotations

import ast
import tomllib
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = REPOSITORY_ROOT / "chattocodex"
PYPROJECT_PATH = REPOSITORY_ROOT / "pyproject.toml"
CONTRACT_ROOT = SOURCE_ROOT / "shared" / "contracts"
SCHEMA_BASE_PATH = CONTRACT_ROOT / "schema_base.py"
MANDATORY_SKILLS = (
    SOURCE_ROOT / ".agents" / "fastapi-skill" / "SKILL.md",
    SOURCE_ROOT / ".agents" / "dependency-injector-skill" / "SKILL.md",
)
BARE_COLLECTIONS = {"list", "tuple", "set", "frozenset"}
BOUNDARY_DECORATORS = {
    "tool",
    "resource",
    "prompt",
    "get",
    "post",
    "put",
    "patch",
    "delete",
}
STRICT_SCHEMA_CONFIG = {
    "extra": "forbid",
    "frozen": True,
    "strict": True,
    "use_enum_values": True,
    "validate_default": True,
}
STRICT_ROOT_SCHEMA_CONFIG = {
    "frozen": True,
    "strict": True,
    "use_enum_values": True,
    "validate_default": True,
}


def _call_name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _annotation_origin(node: ast.expr | None) -> str | None:
    if isinstance(node, ast.Subscript):
        return _call_name(node.value)
    return _call_name(node) if node is not None else None


def _annotated_items(node: ast.expr | None) -> tuple[ast.expr, ...]:
    if not isinstance(node, ast.Subscript) or _call_name(node.value) != "Annotated":
        return ()
    if isinstance(node.slice, ast.Tuple):
        return tuple(node.slice.elts)
    return (node.slice,)


def _call_keywords(node: ast.Call) -> dict[str, ast.expr]:
    return {
        keyword.arg: keyword.value
        for keyword in node.keywords
        if keyword.arg is not None
    }


def _literal_value(node: ast.expr | None) -> object:
    if isinstance(node, ast.Constant):
        return node.value
    return None


def _literal_items(node: ast.Subscript) -> tuple[ast.expr, ...]:
    if isinstance(node.slice, ast.Tuple):
        return tuple(node.slice.elts)
    return (node.slice,)


def _class_config_call(tree: ast.Module, *, class_name: str) -> ast.Call | None:
    class_node = next(
        (
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name == class_name
        ),
        None,
    )
    if class_node is None:
        return None
    for statement in class_node.body:
        if not isinstance(statement, ast.Assign):
            continue
        if not any(
            isinstance(target, ast.Name) and target.id == "model_config"
            for target in statement.targets
        ):
            continue
        if (
            isinstance(statement.value, ast.Call)
            and _call_name(statement.value.func) == "ConfigDict"
        ):
            return statement.value
    return None


def _check_config_dict(
    *,
    tree: ast.Module,
    class_name: str,
    expected: dict[str, object],
    errors: list[str],
) -> None:
    config_call = _class_config_call(tree, class_name=class_name)
    if config_call is None:
        errors.append(
            f"canonical schema base must configure {class_name} with ConfigDict"
        )
        return
    keywords = _call_keywords(config_call)
    for key, expected_value in expected.items():
        actual_value = _literal_value(keywords.get(key))
        if actual_value != expected_value:
            errors.append(
                f"{class_name}.model_config must set {key}={expected_value!r}; "
                f"found {actual_value!r}"
            )


def _is_exposed_boundary(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    for decorator in node.decorator_list:
        target = decorator.func if isinstance(decorator, ast.Call) else decorator
        if _call_name(target) in BOUNDARY_DECORATORS:
            return True
    return False


def _relative(path: Path) -> str:
    return path.relative_to(REPOSITORY_ROOT).as_posix()


def _check_skill_files(errors: list[str]) -> None:
    errors.extend(
        f"missing mandatory backend skill: {_relative(path)}"
        for path in MANDATORY_SKILLS
        if not path.is_file()
    )


def _check_schema_base(errors: list[str]) -> None:
    if not SCHEMA_BASE_PATH.is_file():
        errors.append(f"missing canonical schema base: {_relative(SCHEMA_BASE_PATH)}")
        return
    tree = ast.parse(SCHEMA_BASE_PATH.read_text(encoding="utf-8"))
    _check_config_dict(
        tree=tree,
        class_name="StrictSchemaModel",
        expected=STRICT_SCHEMA_CONFIG,
        errors=errors,
    )
    _check_config_dict(
        tree=tree,
        class_name="StrictRootSchemaModel",
        expected=STRICT_ROOT_SCHEMA_CONFIG,
        errors=errors,
    )
    described_field = next(
        (
            node
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == "described_field"
        ),
        None,
    )
    if described_field is None:
        errors.append("canonical schema base must define described_field")
        return
    argument_names = {
        argument.arg
        for argument in (
            *described_field.args.posonlyargs,
            *described_field.args.args,
            *described_field.args.kwonlyargs,
        )
    }
    if "description" not in argument_names:
        errors.append("described_field must require a description parameter")
    if {"default", "default_factory"} & argument_names:
        errors.append("described_field must not accept default/default_factory")


def _check_pydantic_v2_dependencies(errors: list[str]) -> None:
    project = tomllib.loads(PYPROJECT_PATH.read_text(encoding="utf-8"))["project"]
    dependencies = tuple(str(item).replace(" ", "") for item in project["dependencies"])

    def dependency_name(specifier: str) -> str:
        boundary = min(
            (index for index, char in enumerate(specifier) if char in "<>=!~["),
            default=len(specifier),
        )
        return specifier[:boundary]

    for package in ("pydantic", "pydantic-settings"):
        matches = tuple(
            item for item in dependencies if dependency_name(item) == package
        )
        if len(matches) != 1 or ">=2" not in matches[0] or "<3" not in matches[0]:
            errors.append(
                f"pyproject.toml: {package} must be pinned to the Pydantic v2 major range >=2,<3"
            )


def _is_top_level_annotated_alias(node: ast.stmt) -> bool:
    return (
        isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and isinstance(node.value, ast.Subscript)
        and _call_name(node.value.value) == "Annotated"
    )


def _check_schema_layout(errors: list[str]) -> None:
    schema_root = CONTRACT_ROOT
    aliases: dict[str, list[str]] = {}
    for path in sorted(schema_root.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        relative_schema = path.relative_to(schema_root).as_posix()
        top_level_aliases = tuple(
            node for node in tree.body if _is_top_level_annotated_alias(node)
        )
        if "types" not in path.parts and len(top_level_aliases) >= 4:
            errors.append(
                f"shared/contracts/{relative_schema}: {len(top_level_aliases)} top-level Annotated "
                "type aliases make the schema module dense; move reusable constraints to "
                "shared/contracts/types/<concept>_types.py"
            )
        for node in top_level_aliases:
            target = node.targets[0]
            assert isinstance(target, ast.Name)
            aliases.setdefault(target.id, []).append(relative_schema)
        for node in tree.body:
            if not isinstance(node, ast.ClassDef):
                continue
            if not (node.name.endswith("Filter") or "FilterSchema" in node.name):
                continue
            if "filter_schema" not in path.parts:
                errors.append(
                    f"shared/contracts/{relative_schema}:{node.lineno}: filter contract {node.name} "
                    "must live under schemas/filter_schema"
                )
    for alias_name, locations in aliases.items():
        if len(locations) > 1:
            errors.append(
                f"shared/contracts: constrained alias {alias_name} is duplicated in {locations}; "
                "reuse one concept-specific schemas/types contract or rename distinct semantics"
            )


def _check_source_file(path: Path, errors: list[str]) -> None:
    if path == SCHEMA_BASE_PATH or ".agents" in path.parts:
        return
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError as error:
        errors.append(f"cannot parse {_relative(path)}:{error.lineno}: {error.msg}")
        return

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            imported_names = {alias.name for alias in node.names}
            if module.startswith("pydantic.v1"):
                errors.append(
                    f"{_relative(path)}:{node.lineno}: Pydantic v1 compatibility imports are forbidden"
                )
            if (
                module == "pydantic"
                and {"validator", "root_validator"} & imported_names
            ):
                errors.append(
                    f"{_relative(path)}:{node.lineno}: Pydantic v1 validators are forbidden; "
                    "use field_validator/model_validator"
                )
        elif isinstance(node, ast.ClassDef):
            base_names = {_call_name(base) for base in node.bases}
            if "str" in base_names and "Enum" in base_names:
                errors.append(
                    f"{_relative(path)}:{node.lineno}: {node.name} inherits str + Enum; "
                    "use StrEnum"
                )
            for base in node.bases:
                base_name = _call_name(base)
                if base_name in {"BaseModel", "RootModel"}:
                    errors.append(
                        f"{_relative(path)}:{node.lineno}: {node.name} directly inherits "
                        f"{base_name}; use StrictSchemaModel/StrictRootSchemaModel"
                    )
        elif (
            isinstance(node, ast.Subscript)
            and _call_name(node.value) == "Literal"
            and any(
                isinstance(item, ast.Constant) and isinstance(item.value, str)
                for item in _literal_items(node)
            )
        ):
            errors.append(
                f"{_relative(path)}:{node.lineno}: string-valued Literal is forbidden; "
                "use StrEnum"
            )
        elif isinstance(node, ast.Call) and _call_name(node.func) == "Field":
            errors.append(
                f"{_relative(path)}:{node.lineno}: direct Field() is forbidden; "
                "use described_field(description=...)"
            )
        elif (
            isinstance(node, ast.Call) and _call_name(node.func) == "StringConstraints"
        ):
            strict_value = _literal_value(_call_keywords(node).get("strict"))
            if strict_value is not True:
                errors.append(
                    f"{_relative(path)}:{node.lineno}: StringConstraints must set "
                    "strict=True"
                )
        elif isinstance(node, ast.AnnAssign):
            annotated_items = _annotated_items(node.annotation)
            if not annotated_items or _call_name(annotated_items[0]) != "str":
                continue
            for metadata in annotated_items[1:]:
                for metadata_node in ast.walk(metadata):
                    if not isinstance(metadata_node, ast.Call):
                        continue
                    if _call_name(metadata_node.func) != "described_field":
                        continue
                    keywords = _call_keywords(metadata_node)
                    if {"min_length", "max_length", "pattern"} & keywords.keys():
                        errors.append(
                            f"{_relative(path)}:{node.lineno}: constrained string field "
                            "must use StringConstraints(strict=True, ...) rather than "
                            "described_field length/pattern metadata"
                        )
                        break
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if not _is_exposed_boundary(node):
                continue
            origin = _annotation_origin(node.returns)
            if origin in BARE_COLLECTIONS:
                errors.append(
                    f"{_relative(path)}:{node.lineno}: boundary {node.name} returns bare "
                    f"{origin}[...]; use a named StrictRootSchemaModel"
                )


def main() -> int:
    errors: list[str] = []
    _check_skill_files(errors)
    _check_schema_base(errors)
    _check_pydantic_v2_dependencies(errors)
    _check_schema_layout(errors)
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        _check_source_file(path, errors)

    if errors:
        for error in errors:
            print(error)
        print(f"schema contract verification failed with {len(errors)} error(s)")
        return 1
    print("schema contract verification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
