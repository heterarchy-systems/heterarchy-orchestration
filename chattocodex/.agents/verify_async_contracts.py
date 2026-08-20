from __future__ import annotations

import ast
from pathlib import Path

SOURCE_ROOT = Path(__file__).resolve().parents[1]


def _production_files() -> tuple[Path, ...]:
    """Collect production Python modules. Args: None. Returns: Production module paths."""
    return tuple(
        path
        for path in sorted(SOURCE_ROOT.rglob("*.py"))
        if ".agents" not in path.parts and "__pycache__" not in path.parts
    )


def _attribute_path(node: ast.expr) -> tuple[str, ...] | None:
    """Resolve a dotted expression into path components. Args: node. Returns: Attribute path or None."""
    if isinstance(node, ast.Name):
        return (node.id,)
    if isinstance(node, ast.Attribute):
        parent = _attribute_path(node.value)
        if parent is not None:
            return (*parent, node.attr)
    return None


def _scan_async_function(
    path: Path,
    function: ast.AsyncFunctionDef,
    errors: list[str],
) -> None:
    """Reject blocking or nested-loop calls from async functions. Args: path, function, errors. Returns: None."""
    relative = path.relative_to(SOURCE_ROOT).as_posix()
    for node in ast.walk(function):
        if not isinstance(node, ast.Call):
            continue
        call_path = _attribute_path(node.func)
        if call_path == ("asyncio", "to_thread"):
            errors.append(
                f"{relative}:{node.lineno}: asyncio.to_thread is forbidden; use asyncer.asyncify for unavoidable blocking boundaries"
            )
        elif call_path == ("asyncio", "run"):
            errors.append(
                f"{relative}:{node.lineno}: async code must not create a nested event loop with asyncio.run"
            )
        elif call_path and call_path[-1] == "run_until_complete":
            errors.append(
                f"{relative}:{node.lineno}: async code must not call run_until_complete"
            )
        elif call_path == ("time", "sleep"):
            errors.append(
                f"{relative}:{node.lineno}: time.sleep blocks the event loop; await an async sleep instead"
            )
        elif call_path in {("subprocess", "run"), ("subprocess", "Popen")}:
            errors.append(
                f"{relative}:{node.lineno}: blocking subprocess APIs are forbidden inside async functions; use an async process boundary or asyncify"
            )


def main() -> int:
    """Validate production async/sync boundaries. Args: None. Returns: Process exit code."""
    errors: list[str] = []
    for path in _production_files():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.AsyncFunctionDef):
                _scan_async_function(path=path, function=node, errors=errors)
    if errors:
        for error in errors:
            print(error)
        return 1
    print("async contract verification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
