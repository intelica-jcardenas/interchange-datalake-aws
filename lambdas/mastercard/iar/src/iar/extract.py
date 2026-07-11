"""
extract.py

Capa EXTRACT del pipeline IAR (Mastercard) — módulo interno importado por
`handler.py` (`pipeline_iar`, paso "EXTRACT"). Remueve el bloqueo físico de
1014 bytes (1012 de payload + 2 de separador) que algunos archivos IAR traen,
igual que `unblock_1014()` en el interpreter Mastercard del flujo
transaccional (ver `decisions.md` — "Por qué Mastercard tiene un paso
Interpreter"), con la misma lógica de pushback ante separadores no
estándar.
"""

import io
from pathlib import Path

def unblock_file(fileobj: io.BytesIO) -> bytes:
    """
    Elimina los separadores de bloque de un archivo IAR bloqueado en trozos
    de 1014 bytes: lee 1012 bytes de payload y descarta los 2 bytes
    siguientes solo si son un separador válido (`""`, `\\x00\\x00`,
    `\\x20\\x20` o `\\x40\\x40`); si no lo son, hace pushback (`seek(-2)`)
    para tratarlos como parte del payload del siguiente bloque.

    Args:
        fileobj: Buffer en memoria con el archivo IAR completo, bloqueado.

    Returns:
        bytes del archivo sin los separadores de bloque.

    Ejemplo:
        unblock_file(io.BytesIO(contenido_bloqueado))  # bytes desbloqueados
    """
    fileobj.seek(0)
    chunk_array = bytearray()

    while True:
        chunk = fileobj.read(1012)
        chunk_array.extend(chunk)

        block_separator = fileobj.read(2)

        if block_separator not in [
            b"",
            b"\x00\x00",
            b"\x20\x20",
            b"\x40\x40",
        ]:
            fileobj.seek(fileobj.tell() - 2)

        if len(chunk) < 1012:
            break

    return bytes(chunk_array)


def extract_iar_file(path_to_file: str | Path, blocked: bool = True) -> io.BytesIO:
    """
    Lee un archivo IAR desde disco local y, si viene bloqueado, lo
    desbloquea con `unblock_file`. Utilitario para pruebas/debugging local —
    el flujo real en producción usa `extract_iar_bytes` (recibe bytes desde
    S3, no un path de archivo).

    Args:
        path_to_file: Ruta local al archivo IAR.
        blocked: Si el archivo viene bloqueado en bloques de 1014 bytes.

    Returns:
        Buffer `io.BytesIO` con el contenido (desbloqueado si aplica).

    Ejemplo:
        extract_iar_file("tst_files/iar_sample.txt", blocked=True)
    """
    with open(path_to_file, "rb") as f:
        bufferfile = io.BytesIO(f.read())

    if blocked:
        unblocked = unblock_file(bufferfile)
        return io.BytesIO(unblocked)

    return bufferfile

def extract_iar_bytes(
    file_bytes: bytes,
    blocked: bool,
):
    """
    Punto de entrada real de la etapa EXTRACT: recibe los bytes del archivo
    IAR ya descargados de S3 landing (`handler.py`) y, si el archivo viene
    bloqueado, remueve los separadores de bloque de 1014 bytes.

    Args:
        file_bytes: Contenido completo del archivo IAR, tal como se
            descargó de S3.
        blocked: Si el archivo viene bloqueado (`file_config["file_iar_block"]`
            en `handler.py`).

    Returns:
        Buffer `io.BytesIO` listo para que `raw.py` lo lea registro por
        registro.

    Ejemplo:
        extract_iar_bytes(landing_bytes, blocked=True)
    """
    if blocked:
        file_bytes = unblock_file(
            io.BytesIO(file_bytes)
        )

    return io.BytesIO(file_bytes)