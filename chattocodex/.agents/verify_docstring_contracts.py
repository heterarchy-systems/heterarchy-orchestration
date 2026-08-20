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


def _parameter_names(function: ast.FunctionDef | ast.AsyncFunctionDef) -> tuple[str, ...]:
    """Collect documented parameter names. Args: function. Returns: Parameter names excluding self and cls."""
    arguments = function.args
    names = [
        argument.arg
        for argument in (*arguments.posonlyargs, *arguments.args, *arguments.kwonlyargs)
        if argument.arg not in {"self", "cls"}
    ]
    if arguments.vararg is not None:
        names.append(arguments.vararg.arg)
    if arguments.kwarg is not None:
        names.append(arguments.kwarg.arg)
    return tuple(names)


def _check_function(
    path: Path,
    function: ast.FunctionDef | ast.AsyncFunctionDef,
    errors: list[str],
) -> None:
    """Validate one function documentation contract. Args: path, function, errors. Returns: None."""
    relative = path.relative_to(SOURCE_ROOT).as_posix()
    docstring = ast.get_docstring(function, clean=True)
    if docstring is None:
        errors.append(f"{relative}:{function.lineno}: {function.name} is missing a docstring")
        return
    if "Args:" not in docstring:
        errors.append(f"{relative}:{function.lineno}: {function.name} docstring is missing Args:")
    if "Returns:" not in docstring:
        errors.append(f"{relative}:{function.lineno}: {function.name} docstring is missing Returns:")
    for parameter in _parameter_names(function):
        if parameter not in docstring:
            errors.append(
                f"{relative}:{function.lineno}: {function.name} docstring does not name parameter {parameter}"
            )


def main() -> int:
    """Validate production function documentation contracts. Args: None. Returns: Process exit code."""
    errors: list[str] = []
    checked = 0
    for path in _production_files():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                checked += 1
                _check_function(path=path, function=node, errors=errors)
    if errors:
        for error in errors:
            print(error)
        print(f"docstring contract verification failed ({len(errors)} violations across {checked} functions)")
        return 1
    print(f"docstring contract verification passed ({checked} functions)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
