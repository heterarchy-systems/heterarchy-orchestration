from __future__ import annotations

import ast
from pathlib import Path

SOURCE_ROOT = Path(__file__).resolve().parents[1]
ANY_ALLOW_MARKER = "type-contract: allow-any"
KEYWORD_ONLY_ALLOW_MARKER = "type-contract: allow-keyword-only"
_BARE_COLLECTION_NAMES = frozenset({"dict", "list", "set", "tuple"})


def _production_files() -> tuple[Path, ...]:
    return tuple(
        path
        for path in sorted(SOURCE_ROOT.rglob("*.py"))
        if ".agents" not in path.parts and "__pycache__" not in path.parts
    )


def _relative(path: Path) -> str:
    return path.relative_to(SOURCE_ROOT).as_posix()


def _all_arguments(node: ast.FunctionDef | ast.AsyncFunctionDef) -> tuple[ast.arg, ...]:
    arguments = [
        *node.args.posonlyargs,
        *node.args.args,
        *node.args.kwonlyargs,
    ]
    if node.args.vararg is not None:
        arguments.append(node.args.vararg)
    if node.args.kwarg is not None:
        arguments.append(node.args.kwarg)
    return tuple(arguments)


def _allow_any(*, source_lines: tuple[str, ...], lineno: int) -> bool:
    current = source_lines[lineno - 1] if lineno <= len(source_lines) else ""
    previous = source_lines[lineno - 2] if lineno > 1 else ""
    return ANY_ALLOW_MARKER in current or ANY_ALLOW_MARKER in previous


def _allow_keyword_only(*, source_lines: tuple[str, ...], lineno: int) -> bool:
    current = source_lines[lineno - 1] if lineno <= len(source_lines) else ""
    previous = source_lines[lineno - 2] if lineno > 1 else ""
    return KEYWORD_ONLY_ALLOW_MARKER in current or KEYWORD_ONLY_ALLOW_MARKER in previous


def _check_annotation(
    *,
    path: Path,
    annotation: ast.expr,
    label: str,
    errors: list[str],
) -> None:
    if isinstance(annotation, ast.Name) and annotation.id in _BARE_COLLECTION_NAMES:
        errors.append(
            f"{_relative(path)}:{annotation.lineno}: {label} uses bare "
            f"{annotation.id}; parameterize the collection with concrete element types"
        )


def _check_function_annotations(
    *,
    path: Path,
    tree: ast.Module,
    errors: list[str],
) -> None:
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.returns is None:
            errors.append(
                f"{_relative(path)}:{node.lineno}: {node.name} is missing an explicit "
                "return annotation"
            )
        else:
            _check_annotation(
                path=path,
                annotation=node.returns,
                label=f"return annotation for {node.name}",
                errors=errors,
            )
        for argument in _all_arguments(node):
            if argument.arg in {"self", "cls"}:
                continue
            if argument.annotation is None:
                errors.append(
                    f"{_relative(path)}:{node.lineno}: {node.name} parameter "
                    f"{argument.arg!r} is missing an explicit annotation"
                )
                continue
            _check_annotation(
                path=path,
                annotation=argument.annotation,
                label=f"parameter {argument.arg!r} on {node.name}",
                errors=errors,
            )


def _check_keyword_only_usage(
    *,
    path: Path,
    tree: ast.Module,
    source_lines: tuple[str, ...],
    errors: list[str],
) -> None:
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if not node.args.kwonlyargs or node.args.vararg is not None:
            continue
        if _allow_keyword_only(source_lines=source_lines, lineno=node.lineno):
            continue
        names = ", ".join(argument.arg for argument in node.args.kwonlyargs)
        errors.append(
            f"{_relative(path)}:{node.lineno}: {node.name} uses a bare keyword-only "
            f"separator for [{names}]; project-wide bare `*,` is forbidden. Remove it, "
            "or document a truly unavoidable API boundary with "
            f"'{KEYWORD_ONLY_ALLOW_MARKER}'"
        )


def _check_any_usage(
    *,
    path: Path,
    tree: ast.Module,
    source_lines: tuple[str, ...],
    errors: list[str],
) -> None:
    for node in ast.walk(tree):
        if not isinstance(node, ast.Name) or node.id != "Any":
            continue
        if _allow_any(source_lines=source_lines, lineno=node.lineno):
            continue
        errors.append(
            f"{_relative(path)}:{node.lineno}: Any is forbidden by default; use a typed "
            "boundary contract, or document an unavoidable third-party boundary with "
            f"'{ANY_ALLOW_MARKER}'"
        )


def main() -> int:
    errors: list[str] = []
    for path in _production_files():
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        source_lines = tuple(source.splitlines())
        _check_function_annotations(path=path, tree=tree, errors=errors)
        _check_keyword_only_usage(
            path=path,
            tree=tree,
            source_lines=source_lines,
            errors=errors,
        )
        _check_any_usage(
            path=path,
            tree=tree,
            source_lines=source_lines,
            errors=errors,
        )
    if errors:
        for error in errors:
            print(error)
        return 1
    print("type contract verification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
