#!/usr/bin/env python3
"""
Lista los archivos .py de glue/scripts/ y lambdas/ que todavia NO tienen el
docstring de modulo estandar (marcador "Lambda real:" o "Job real:" en sus
primeros ~4000 caracteres), para saber con que archivo seguir aplicando la
skill itx-document-script.

Uso (desde la raiz del repo o desde cualquier subdirectorio):
    python .claude/skills/itx-document-script/scripts/find_undocumented.py
    python .claude/skills/itx-document-script/scripts/find_undocumented.py <archivo1.py> [archivo2.py ...]

La segunda forma (con argumentos) es el chequeo de idempotencia del paso 0
del flujo de trabajo: en vez de escanear todo el repo, chequea puntualmente
si el/los archivo(s) indicado(s) ya tienen el encabezado estandar, antes de
volver a documentarlos.
"""
import pathlib
import re
import sys

ROOTS = ["glue/scripts", "lambdas"]
ENTRYPOINT_MARKERS = ("Lambda real:", "Job real:")
SKIP_DIR_NAMES = {"__pycache__", "tests", "test", ".venv", "venv"}

# Docstring de modulo: """<primer linea no vacia>
_DOCSTRING_FIRST_LINE = re.compile(r'^\s*"""[^\S\n]*\n?\s*([^\n]*)', re.MULTILINE)


def repo_root() -> pathlib.Path:
    # .../<repo>/.claude/skills/itx-document-script/scripts/find_undocumented.py
    return pathlib.Path(__file__).resolve().parents[4]


def iter_py_files(root: pathlib.Path):
    for root_name in ROOTS:
        base = root / root_name
        if not base.exists():
            continue
        for path in base.rglob("*.py"):
            if any(part in SKIP_DIR_NAMES for part in path.parts):
                continue
            yield path


def is_documented(path: pathlib.Path) -> bool:
    """
    Dos convenciones validas de encabezado (ver SKILL.md):
      - Entry-point (handler.py de Lambda / script principal de Glue):
        titulo con "Lambda real:" o "Job real:".
      - Modulo interno (importado por un entry-point, ej. ardef/calculate.py):
        el docstring de modulo empieza con el propio nombre de archivo
        (ej. \"\"\"calculate.py\n\n...\"\"\").
    """
    try:
        head = path.read_text(encoding="utf-8", errors="replace")[:4000]
    except OSError:
        return False
    if any(marker in head for marker in ENTRYPOINT_MARKERS):
        return True
    match = _DOCSTRING_FIRST_LINE.match(head)
    if match and match.group(1).strip() == path.name:
        return True
    return False


def check_specific_files(args: list[str]) -> None:
    """Modo puntual: chequea solo los archivos pasados por argumento (paso 0
    de idempotencia), sin escanear todo el repo."""
    any_pending = False
    for arg in args:
        path = pathlib.Path(arg)
        if not path.exists():
            print(f"  ??? {arg} (no existe)")
            any_pending = True
            continue
        status = "[x] YA DOCUMENTADO" if is_documented(path) else "[ ] PENDIENTE"
        print(f"  {status:20s} {arg}")
        if "PENDIENTE" in status:
            any_pending = True
    if not any_pending:
        print(
            "\nTodos los archivos ya tienen el encabezado estandar — "
            "confirmar con el usuario antes de volver a documentarlos "
            "(ver SKILL.md, paso 0)."
        )


def main() -> None:
    argv = sys.argv[1:]
    if argv:
        check_specific_files(argv)
        return

    root = repo_root()
    pending, done = [], []
    for path in sorted(iter_py_files(root)):
        rel = path.relative_to(root)
        (done if is_documented(path) else pending).append(rel)

    print(f"Documentados ({len(done)}):")
    for p in done:
        print(f"  [x] {p}")

    print(f"\nPendientes ({len(pending)}):")
    for p in pending:
        print(f"  [ ] {p}")


if __name__ == "__main__":
    main()
