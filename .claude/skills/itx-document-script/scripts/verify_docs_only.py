#!/usr/bin/env python3
"""
Verifica que los cambios locales (working copy vs HEAD) de uno o mas archivos
.py sean SOLO de documentacion (docstrings o strings sueltas), comparando el
AST de ambas versiones con esos nodos removidos.

Uso:
    python .claude/skills/itx-document-script/scripts/verify_docs_only.py <archivo.py> [archivo2.py ...]

Salida por archivo: DOC-ONLY | LOGIC CHANGED | SYNTAX ERROR | ERROR
Exit code: 0 si todos son DOC-ONLY, 1 en cualquier otro caso.
"""
import ast
import subprocess
import sys


def strip_bare_strings(tree: ast.AST) -> ast.AST:
    """Elimina cualquier Expr que sea solo un string literal (docstring de
    modulo/funcion/clase, en cualquier posicion del body) para poder comparar
    dos versiones de un archivo ignorando su documentacion."""
    for node in ast.walk(tree):
        if hasattr(node, "body") and isinstance(node.body, list):
            new_body = [
                n for n in node.body
                if not (
                    isinstance(n, ast.Expr)
                    and isinstance(n.value, ast.Constant)
                    and isinstance(n.value.value, str)
                )
            ]
            node.body = new_body or [ast.Pass()]
    return tree


def check(path: str) -> str:
    try:
        proc = subprocess.run(
            ["git", "show", f"HEAD:{path}"],
            capture_output=True, check=True,
        )
        old_src = proc.stdout.decode("utf-8", errors="replace")
    except subprocess.CalledProcessError as e:
        return f"ERROR (no se pudo leer HEAD:{path} -> {e.stderr.decode(errors='replace')[:200]})"

    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            new_src = fh.read()
    except OSError as e:
        return f"ERROR (no se pudo leer working copy: {e})"

    try:
        old_tree = strip_bare_strings(ast.parse(old_src))
        new_tree = strip_bare_strings(ast.parse(new_src))
    except SyntaxError as e:
        return f"SYNTAX ERROR ({e})"

    old_dump = ast.dump(old_tree, annotate_fields=False)
    new_dump = ast.dump(new_tree, annotate_fields=False)
    return "DOC-ONLY" if old_dump == new_dump else "LOGIC CHANGED"


def main(argv: list[str]) -> int:
    if not argv:
        print("Uso: verify_docs_only.py <archivo1.py> [archivo2.py ...]")
        return 1
    ok = True
    for path in argv:
        result = check(path)
        print(f"{result:15s} {path}")
        if result != "DOC-ONLY":
            ok = False
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
