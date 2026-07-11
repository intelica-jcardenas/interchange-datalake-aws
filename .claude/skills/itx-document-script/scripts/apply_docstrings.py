#!/usr/bin/env python3
"""
Aplica un lote de docstrings a un archivo .py a partir de un manifiesto JSON
(qualname -> texto del docstring), en vez de un `Edit` por funcion. Reduce
drasticamente el numero de tool calls por archivo: el modelo redacta todo el
texto en una sola respuesta (el manifiesto), y este script inserta/reemplaza
cada docstring en su posicion exacta via `ast` (sin tocar ninguna otra linea
de codigo).

Modos de uso:
    # 1. Listar los qualnames disponibles en el archivo (para armar el manifiesto)
    python apply_docstrings.py --file <archivo.py> --list

    # 2. Aplicar el manifiesto
    python apply_docstrings.py --file <archivo.py> --manifest <manifiesto.json>

Formato del manifiesto (JSON):
    {
      "MODULE": "texto del docstring de modulo, sin comillas triples...",
      "nombre_funcion_top_level": "texto...",
      "NombreClase": "texto del docstring de la clase...",
      "NombreClase.metodo": "texto del docstring del metodo..."
    }

El texto de cada valor va SIN indentar (flush-left) y SIN las comillas
triples ni backslashes de escape extra -- el script se encarga de indentar
cada linea al nivel correcto y envolverlo en \"\"\"...\"\"\". Si el texto
necesita citar comillas triples literales, evitarlo (usar comillas simples
para ejemplos en su lugar) -- el script aborta si detecta \"\"\" dentro del
valor para no romper la sintaxis generada.

Reglas de reemplazo/insercion:
- Si el primer statement del cuerpo (funcion/clase/modulo, saltando un
  `from __future__ import ...` inicial en el caso de modulo) ya es un string
  suelto (docstring existente) -> se REEMPLAZA por el nuevo texto.
- Si no -> se INSERTA el nuevo docstring justo antes de ese primer statement
  (nunca se toca ninguna otra linea).
- La indentacion se toma automaticamente de la columna del primer statement
  real del cuerpo (existente o no) -- no hace falta indicarla en el
  manifiesto.

Despues de aplicar, correr SIEMPRE `verify_docs_only.py <archivo>` para
confirmar que el resultado sigue siendo DOC-ONLY.
"""
import argparse
import ast
import json
import sys


def build_qualname_map(tree: ast.Module) -> dict:
    """
    Recorre el AST completo y arma un dict qualname -> nodo, para toda
    funcion/metodo/clase (anidados o no) mas la entrada especial "MODULE"
    para el propio modulo. El qualname usa "." para anidamiento, ej.
    "ClaseA.metodo" o "funcion_externa.funcion_interna".
    """
    mapping = {"MODULE": tree}

    def visit(node, path):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                qualname = ".".join(path + [child.name])
                mapping[qualname] = child
                visit(child, path + [child.name])
            else:
                visit(child, path)

    visit(tree, [])
    return mapping


def module_anchor_index(tree: ast.Module) -> int:
    """
    Para el modulo, determina el indice del "primer statement real" a
    efectos de docstring -- normalmente 0, salvo que el archivo empiece con
    `from __future__ import ...`, en cuyo caso el docstring de modulo (si
    existe) va en la posicion 1, no 0 (visto en handlers reales de este
    repo, ej. mc-extract/mc-interpreter).
    """
    body = tree.body
    if (
        body
        and isinstance(body[0], ast.ImportFrom)
        and body[0].module == "__future__"
    ):
        return 1
    return 0


def get_anchor_node(node, tree: ast.Module):
    """
    Retorna el nodo "ancla" de un target (funcion/clase/modulo): si ya tiene
    docstring, es el propio nodo del docstring (para reemplazar); si no, es
    el primer statement real del cuerpo (para insertar antes de el).
    """
    if node is tree:
        idx = module_anchor_index(tree)
        body = tree.body
        return body[idx] if idx < len(body) else None
    if not node.body:
        return None
    return node.body[0]


def is_bare_string_expr(node) -> bool:
    return (
        isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, str)
    )


def format_docstring_block(text: str, indent: str, newline: str) -> list[str]:
    if '"""' in text:
        raise ValueError(
            "El texto del docstring contiene \"\"\" literal -- reformular "
            "sin comillas triples para no romper la sintaxis generada."
        )
    lines = text.split("\n")
    # Quitar una linea en blanco final sobrante si el texto termina en \n
    if lines and lines[-1] == "":
        lines.pop()
    out = [f'{indent}"""{newline}']
    for line in lines:
        if line.strip():
            out.append(f"{indent}{line}{newline}")
        else:
            out.append(newline)
    out.append(f'{indent}"""{newline}')
    return out


def detect_newline(text: str) -> str:
    return "\r\n" if "\r\n" in text else "\n"


def apply_manifest(file_path: str, manifest: dict) -> None:
    with open(file_path, "r", encoding="utf-8", newline="") as fh:
        original_text = fh.read()
    newline = detect_newline(original_text)
    lines = original_text.splitlines(keepends=True)

    tree = ast.parse(original_text)
    qualname_map = build_qualname_map(tree)

    edits = []  # (start_line_1idx, end_line_1idx_or_None, new_lines)
    missing = []
    for qualname, text in manifest.items():
        node = qualname_map.get(qualname)
        if node is None:
            missing.append(qualname)
            continue
        anchor = get_anchor_node(node, tree)
        if anchor is None:
            missing.append(f"{qualname} (cuerpo vacio, no se puede documentar)")
            continue
        indent = " " * anchor.col_offset
        new_block = format_docstring_block(text, indent, newline)
        if is_bare_string_expr(anchor):
            edits.append((anchor.lineno, anchor.end_lineno, new_block))
        else:
            edits.append((anchor.lineno, None, new_block))

    if missing:
        print("ADVERTENCIA -- qualnames no encontrados en el archivo:", file=sys.stderr)
        for m in missing:
            print(f"  - {m}", file=sys.stderr)

    # Aplicar de abajo hacia arriba para no invalidar los numeros de linea
    # de las ediciones que faltan.
    edits.sort(key=lambda e: e[0], reverse=True)
    for start_line, end_line, new_block in edits:
        start_idx = start_line - 1
        if end_line is not None:
            end_idx = end_line  # end_line es inclusive 1-idx -> slice exclusivo correcto
            lines[start_idx:end_idx] = new_block
        else:
            lines[start_idx:start_idx] = new_block

    with open(file_path, "w", encoding="utf-8", newline="") as fh:
        fh.writelines(lines)

    applied = len(manifest) - len(missing)
    print(f"Aplicados {applied}/{len(manifest)} docstrings en {file_path}.")
    if missing:
        print(f"{len(missing)} no se pudieron aplicar (ver advertencias arriba).")


def list_qualnames(file_path: str) -> None:
    with open(file_path, "r", encoding="utf-8") as fh:
        tree = ast.parse(fh.read())
    qualname_map = build_qualname_map(tree)
    for qualname, node in qualname_map.items():
        if qualname == "MODULE":
            anchor = get_anchor_node(node, tree)
            has_doc = anchor is not None and is_bare_string_expr(anchor)
            print(f"MODULE{'  [ya tiene docstring]' if has_doc else ''}")
            continue
        anchor = get_anchor_node(node, tree)
        has_doc = anchor is not None and is_bare_string_expr(anchor)
        kind = "class" if isinstance(node, ast.ClassDef) else "def"
        flag = "  [ya tiene docstring]" if has_doc else "  [sin docstring]"
        print(f"{kind:5s} {qualname}{flag}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", required=True, help="Archivo .py objetivo")
    parser.add_argument("--manifest", help="Ruta a un JSON con el manifiesto qualname->texto")
    parser.add_argument("--list", action="store_true", help="Listar qualnames disponibles")
    args = parser.parse_args()

    if args.list:
        list_qualnames(args.file)
        return 0

    if not args.manifest:
        parser.error("--manifest es requerido salvo que se use --list")

    with open(args.manifest, "r", encoding="utf-8") as fh:
        manifest = json.load(fh)

    apply_manifest(args.file, manifest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
