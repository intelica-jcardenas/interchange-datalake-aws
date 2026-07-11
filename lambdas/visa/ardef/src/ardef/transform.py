"""
transform.py

Segunda etapa del pipeline ARDEF. Lee el parquet RAW (líneas de detalle sin
parsear) generado por `interpreter.py` y aplica el schema de posiciones fijas
`ARDEF_SCHEMA` (start/end por campo) para extraer cada campo de negocio a su
propia columna. Todo el contenido queda como texto sin castear — el casteo a
tipos reales (integer/decimal/date) ocurre en `clean.py`.
"""

from datetime import datetime

import pandas as pd
import pyarrow as pa

from ardef.logs.logger import Logger
from ardef.persistence.file import FileStorage
from ardef.schema.ardef_schema import ARDEF_SCHEMA

log = Logger(__name__)
fs = FileStorage()

def _apply_schema(
    line: str,
    schema: dict[str, dict[str, int]],
) -> dict[str, str]:
    """
    Extrae cada campo de una línea de texto ARDEF cortando por posición fija
    según los índices start/end de cada columna del schema. El campo
    `data_type` del schema se ignora acá — solo se usa para el corte
    posicional, el casteo de tipo ocurre en `clean.py`.

    Args:
        line: Línea de texto original del archivo ARDEF (una fila de
            "lines").
        schema: Diccionario columna → {"start": int, "end": int, ...}
            (típicamente ARDEF_SCHEMA).

    Returns:
        Diccionario columna → substring extraído (todo como string, sin
        trim ni casteo).

    Ejemplo:
        _apply_schema("VL...", ARDEF_SCHEMA)  # -> {"table_key": "...", ...}
    """

    return {
        col: line[spec["start"]: spec["end"]]
        for col, spec in schema.items()
    }

def _build_ardef_transform_dataframe(
    raw: pd.DataFrame,
    file_id: str,
    file_processing_date: str,
    schema: dict[str, dict[str, int]],
) -> pd.DataFrame:
    """
    A partir del parquet RAW aplica el schema de posiciones fijas y devuelve un Dataframe
    con una columna por campo. Todo el contenido por defecto es str.

    El campo 'lines' se arrastra como columna meta para servir de llave natural
    de deduplicación en etapas posteriores (vi_calculate).

    Args:
        raw: DataFrame RAW (salida de `interpreter.py`), con columnas de
            metadata + "lines" (línea de texto original).
        file_id: ID único del archivo, solo para logging.
        file_processing_date: Fecha de negocio del archivo, solo para
            logging.
        schema: Diccionario columna → {"start": int, "end": int} usado para
            cortar cada línea (típicamente ARDEF_SCHEMA).

    Returns:
        DataFrame con las columnas de metadata originales + una columna por
        campo del schema, todo como texto. Vacío (mismas columnas, 0 filas)
        si `raw` estaba vacío.

    Ejemplo:
        _build_ardef_transform_dataframe(raw, "ABC123", "2026-04-24", ARDEF_SCHEMA)
    """
    if raw.empty:
        log.logger.warning(
            f"RAW parquet vacio para file_id={file_id}, "
            f"file_processing_date={file_processing_date}"
        )
        meta_cols = [
            "file_id", "file_processing_date", "ardef_version", 
            "ardef_header_date", "line_no", "lines", "row_creation_timestamp", "_eff_ts",
        ]
        return pd.DataFrame([], columns=meta_cols + list(schema.keys()), dtype=str)
    
    log.logger.info(
        f"Aplicando schema ARDEF sobre {len(raw)} lineas | "
        f"file_id={file_id}, file_processing_date={file_processing_date}"
    )

    # Parsear cada linea aplicando los rangos del schema
    parsed = raw["lines"].apply(lambda line: _apply_schema(line=line, schema=schema))
    parsed_df = pd.DataFrame(parsed.tolist(), index=raw.index)

    # Metadatos que se arrastran del parquet RAW
    # ' lines' se mantiene como llave natural de deduplicación
    meta = raw[[
        "file_id", "file_processing_date", "ardef_version", 
        "ardef_header_date", "line_no", "lines", "row_creation_timestamp", "_eff_ts",
    ]].reset_index(drop=True)
    parsed_df = parsed_df.reset_index(drop=True)

    df = pd.concat([meta, parsed_df], axis=1)

    return df.astype(str)

def transform_ardef(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    file_id: str,
    file_processing_date: str,
    origin_subdir: str = "100_ARDEF_RAW",
    target_subdir: str = "200_ARDEF_TRA",
    schema: dict[str, dict[str, int]] | None = None
) -> None:
    """
    Etapa TRANSFORM del pipeline ARDEF. Lee el parquet RAW de ARDEF, aplica la
    plantilla de posiciones fijas y escribe el resultado en un nuevo parquet
    en STAGING.

    Pasos:
        1. Leer STAGING / {brand_id} / {file_type} / {date} / 100_ARDEF_RAW / {file_id}.parquet
        2. Parsear cada línea según ARDEF_SCHEMA (o el schema que se pasa).
           El campo 'lines' se conserva como columna meta para deduplicación posterior.
        3. Escribir STAGING / {brand_id} / {file_type} / {date} / 200_ARDEF_TRA / {file_id}.parquet

    Args:
        origin_layer: Layer de origen (típicamente STAGING).
        target_layer: Layer de destino (típicamente STAGING).
        file_id: ID único del archivo.
        file_processing_date: Fecha de negocio del archivo, "YYYY-MM-DD".
        origin_subdir: Subdirectorio de origen (default "100_ARDEF_RAW").
        target_subdir: Subdirectorio de destino (default "200_ARDEF_TRA").
        schema: Schema de posiciones fijas a aplicar; si es None, usa
            ARDEF_SCHEMA.

    Returns:
        None. Escribe el parquet TRANSFORM en STAGING como efecto
        secundario.

    Ejemplo:
        transform_ardef(FileStorage.Layer.STAGING, FileStorage.Layer.STAGING,
                         "ABC123", "2026-04-24")
    """

    if schema is None:
        schema = ARDEF_SCHEMA

    log.logger.info(
        f"Inicio transform_ardef | " 
        f"file_id={file_id}, file_processing_date={file_processing_date}"
    )

    #1. Leer Raw
    raw = fs.read_parquet(
        layer=origin_layer,
        file_id=file_id,
        file_processing_date=file_processing_date,
        subdir=origin_subdir
    )

    #2. Parsear
    df_transform = _build_ardef_transform_dataframe(
        raw=raw,
        file_id=file_id,
        file_processing_date=file_processing_date,
        schema=schema,
    )

    #3. Escribir
    output_filepath = fs.write_parquet(
        data=df_transform,
        layer=target_layer,
        file_id=file_id,
        file_processing_date=file_processing_date,
        subdir=target_subdir,
        index=False
    )

    log.logger.info(
        f"ARDEF TRANSFORM parquet creado exitosamente: {output_filepath}"
    )