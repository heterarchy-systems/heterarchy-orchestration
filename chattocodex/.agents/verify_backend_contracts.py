from __future__ import annotations

import ast
from pathlib import Path

SOURCE_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = SOURCE_ROOT.parent

MCP_ANY_ALLOWLIST = frozenset(
    {"orchestrator_mcp/transport/connector_qualified_mcp_server.py"}
)
STARLETTE_REQUEST_ALLOWLIST = frozenset(
    {
        "orchestrator_mcp/auth/oauth_compatibility_routes.py",
        "orchestrator_mcp/auth/oauth_routes.py",
    }
)
STARLETTE_ROUTE_ALLOWLIST = frozenset(
    {
        "orchestrator_backend/main_runtime/interface/main_review_control_boundary.py",
    }
)
RAW_REQUEST_BODY_ALLOWLIST = frozenset(
    {
        "orchestrator_backend/control_panel/interface/http/fastapi_json_boundary.py",
    }
)


def _production_files() -> tuple[Path, ...]:
    return tuple(
        path
        for path in sorted(SOURCE_ROOT.rglob("*.py"))
        if ".agents" not in path.parts and "__pycache__" not in path.parts
    )


def _relative(path: Path) -> str:
    return path.relative_to(SOURCE_ROOT).as_posix()


def _attribute_path(node: ast.expr) -> tuple[str, ...] | None:
    if isinstance(node, ast.Name):
        return (node.id,)
    if isinstance(node, ast.Attribute):
        parent = _attribute_path(node.value)
        if parent is None:
            return None
        return (*parent, node.attr)
    return None


def _call_name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _check_settings_base(path: Path, class_name: str, errors: list[str]) -> None:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    target = next(
        (
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name == class_name
        ),
        None,
    )
    if target is None:
        errors.append(
            f"{_relative(path)}: missing required settings class {class_name}"
        )
        return
    base_names = {_call_name(base) for base in target.bases}
    if "BaseSettings" not in base_names:
        errors.append(
            f"{_relative(path)}:{target.lineno}: {class_name} must inherit pydantic-settings BaseSettings"
        )


def _check_fastapi_composition(errors: list[str]) -> None:
    path = (
        SOURCE_ROOT
        / "orchestrator_backend"
        / "control_panel"
        / "interface"
        / "routes"
        / "control_panel_routes.py"
    )
    tree = ast.parse(path.read_text(encoding="utf-8"))
    if not any(
        isinstance(node, ast.Call) and _call_name(node.func) == "FastAPI"
        for node in ast.walk(tree)
    ):
        errors.append(
            f"{_relative(path)}: Control Panel composition root must construct FastAPI"
        )
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or _call_name(node.func) != "FastAPI":
            continue
        for keyword in node.keywords:
            if keyword.arg != "default_response_class":
                continue
            response_name = _call_name(keyword.value)
            if response_name in {"JSONResponse", "ORJSONResponse"}:
                errors.append(
                    f"{_relative(path)}:{node.lineno}: FastAPI must use its native typed Pydantic JSON serialization path; do not configure {response_name}"
                )


def _check_imports_and_calls(path: Path, errors: list[str]) -> None:
    relative = _relative(path)
    tree = ast.parse(path.read_text(encoding="utf-8"))
    any_seen = False
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "json":
                    errors.append(
                        f"{relative}:{node.lineno}: stdlib json is forbidden in production; use typed Pydantic serialization or an explicitly justified non-FastAPI serializer"
                    )
                if alias.name == "starlette.applications":
                    errors.append(
                        f"{relative}:{node.lineno}: direct Starlette application use is forbidden; use FastAPI"
                    )
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            names = {alias.name for alias in node.names}
            if module == "json":
                errors.append(
                    f"{relative}:{node.lineno}: stdlib json is forbidden in production; use typed Pydantic serialization or an explicitly justified non-FastAPI serializer"
                )
            if module == "starlette.applications":
                errors.append(
                    f"{relative}:{node.lineno}: direct Starlette application use is forbidden; use FastAPI"
                )
            if (
                module == "starlette.requests"
                and relative not in STARLETTE_REQUEST_ALLOWLIST
            ):
                errors.append(
                    f"{relative}:{node.lineno}: Starlette Request is restricted to reviewed low-level OAuth adapters"
                )
            if (
                module == "starlette.routing"
                and "Route" in names
                and relative not in STARLETTE_ROUTE_ALLOWLIST
            ):
                errors.append(
                    f"{relative}:{node.lineno}: HTTP routes must use FastAPI APIRouter; direct Starlette Route is restricted to reviewed MCP adapters"
                )
            if module == "typing" and "Any" in names:
                any_seen = True
            if module == "os" and "getenv" in names:
                errors.append(
                    f"{relative}:{node.lineno}: direct environment reads are forbidden; use pydantic-settings"
                )
        elif isinstance(node, ast.Name) and node.id == "Any":
            any_seen = True
        elif isinstance(node, ast.Attribute):
            if _attribute_path(node) == ("typing", "Any"):
                any_seen = True
        elif isinstance(node, ast.Subscript):
            expression = ast.unparse(node)
            if (
                expression in {"dict[str, Any]", "dict[str, object]"}
                and relative not in MCP_ANY_ALLOWLIST
            ):
                errors.append(
                    f"{relative}:{node.lineno}: raw dictionary payload types are forbidden; use a named typed contract"
                )
            if _attribute_path(node.value) == ("os", "environ"):
                errors.append(
                    f"{relative}:{node.lineno}: direct environment reads are forbidden; use pydantic-settings"
                )
        elif isinstance(node, ast.Call):
            function_path = _attribute_path(node.func)
            if function_path == ("asyncio", "to_thread"):
                errors.append(
                    f"{relative}:{node.lineno}: use asyncer.asyncify for blocking sync work from async code"
                )
            if function_path in {("os", "getenv"), ("os", "environ", "get")}:
                errors.append(
                    f"{relative}:{node.lineno}: direct environment reads are forbidden; use pydantic-settings"
                )
            if _call_name(node.func) in {"JSONResponse", "ORJSONResponse"}:
                response_name = _call_name(node.func)
                errors.append(
                    f"{relative}:{node.lineno}: {response_name} is forbidden; use typed FastAPI/Pydantic responses or fastapi.Response with model_dump_json() at an explicit protocol seam"
                )
            if _call_name(node.func) == "Starlette":
                errors.append(
                    f"{relative}:{node.lineno}: direct Starlette application construction is forbidden; use FastAPI"
                )
            if (
                isinstance(node.func, ast.Attribute)
                and node.func.attr == "body"
                and relative not in RAW_REQUEST_BODY_ALLOWLIST
            ):
                errors.append(
                    f"{relative}:{node.lineno}: raw request body parsing is restricted to the shared strict JSON boundary"
                )
    if any_seen and relative not in MCP_ANY_ALLOWLIST:
        errors.append(
            f"{relative}: Any is forbidden in production outside the reviewed upstream MCP override boundary"
        )


def main() -> int:
    errors: list[str] = []
    _check_settings_base(
        SOURCE_ROOT / "shared" / "settings" / "runtime_settings.py",
        "RuntimeSettings",
        errors,
    )
    _check_settings_base(
        SOURCE_ROOT / "orchestrator_backend" / "database" / "database_config.py",
        "DatabaseConfig",
        errors,
    )
    _check_fastapi_composition(errors)
    for path in _production_files():
        _check_imports_and_calls(path, errors)
    if errors:
        for error in errors:
            print(error)
        return 1
    print("backend contract verification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
