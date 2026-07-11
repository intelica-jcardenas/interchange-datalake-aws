"""
clean.py

Tercera etapa del pipeline ARDEF. Lee el parquet TRANSFORM (campos ya
extraídos por posición fija, todavía como texto sin castear) y aplica
limpieza (ltrim/rtrim) y casteo de tipos según los data_type declarados en
ARDEF_SCHEMA (campos de negocio) y METADATA_SCHEMA (campos de metadata del
pipeline) — deja el DataFrame listo para que `calculate.py` compute
`valid_until` sobre columnas ya tipadas.
"""

from datetime import datetime
from typing import Optional

import pandas as pd
import numpy as np

from ardef.logs.logger import Logger
from ardef.persistence.file import FileStorage
from ardef.schema.ardef_schema import ARDEF_SCHEMA, METADATA_SCHEMA

log = Logger(__name__)
fs = FileStorage()

_DATE_FORMATS = ["%Y-%m-%d", "%Y%m%d", "%d%m%Y"]

def _try_parse_date(value: str) -> Optional[pd.Timestamp]:
    """
    Intenta parsear una cadena de fecha probando, en orden, cada formato de
    `_DATE_FORMATS` ("%Y-%m-%d", "%Y%m%d", "%d%m%Y").

    Args:
        value: Cadena de fecha a parsear, ej. "20260424" o "2026-04-24".

    Returns:
        pd.Timestamp si algún formato aplica, o pd.NaT si ninguno matchea
        (o si `value` no es un string parseable).

    Ejemplo:
        _try_parse_date("20260424")  # -> Timestamp('2026-04-24 00:00:00')
    """
    for fmt in _DATE_FORMATS:
        try:
            return pd.Timestamp(datetime.strptime(value.strip(), fmt).date())
        except (ValueError, AttributeError):
            continue
    return pd.NaT

def _cast_series(series: pd.Series, data_type: str) -> pd.Series:
    """
    Castea una Series de pandas al tipo indicado por `data_type`: "text"
    (strip a string), "integer" (Int64 nullable), "decimal" (Float64
    nullable), "date" (parseo con `_try_parse_date`). Cualquier valor no
    convertible cae a NA/NaT (coerce), nunca lanza excepción. Un data_type
    no reconocido cae al caso "text" con un warning.

    Args:
        series: Serie de pandas a castear (típicamente ya como string).
        data_type: Uno de "text", "integer", "decimal", "date" (viene del
            schema ARDEF_SCHEMA/METADATA_SCHEMA).

    Returns:
        Serie casteada al tipo pandas correspondiente (str, Int64, Float64
        o datetime/NaT).

    Ejemplo:
        _cast_series(pd.Series(["123", "abc"]), "integer")  # -> [123, <NA>]
    """

    match data_type:
        case "text":
            return series.astype(str).str.strip()
        
        case "integer":
            coerced = pd.to_numeric(series.str.strip(), errors="coerce")
            return coerced.astype("Int64")
        
        case "decimal":
            coerced = pd.to_numeric(series.str.strip(), errors="coerce")
            return coerced.astype("Float64")
        
        case "date":
            return series.str.strip().apply(_try_parse_date)
        
        case _:
            log.logger.warning(
                f"data_type '{data_type}' no reconocido - se aplica cast a texto"
            )
            return series.astype(str).str.strip()
        
def _build_ardef_clean_dataframe(
        transformed: pd.DataFrame,
        file_id: str,
        file_processing_date: str,
        ardef_schema: dict[str, dict],
        metadata_schema: dict[str, dict],
) -> pd.DataFrame:
    """
    Construye el DataFrame CLEAN a partir del TRANSFORM: aplica ltrim/rtrim
    y castea cada columna de metadata según `metadata_schema` y cada campo
    de negocio ARDEF según `ardef_schema` (ambos vía `_cast_series`).
    Columnas declaradas en el schema pero ausentes en el parquet de entrada
    se saltean con un warning (no interrumpen el procesamiento).

    Args:
        transformed: DataFrame TRANSFORM (salida de `transform.py`), con
            todos los campos aún como texto.
        file_id: ID único del archivo, para logging.
        file_processing_date: Fecha de negocio del archivo, para logging.
        ardef_schema: Diccionario columna → {"data_type": ...} de los campos
            de negocio ARDEF (típicamente ARDEF_SCHEMA).
        metadata_schema: Diccionario columna → {"data_type": ...} de los
            campos de metadata del pipeline (típicamente METADATA_SCHEMA).

    Returns:
        DataFrame con las mismas columnas de `transformed`, cada una
        casteada a su tipo real. Vacío (mismas columnas, 0 filas) si
        `transformed` estaba vacío.

    Ejemplo:
        _build_ardef_clean_dataframe(transformed, "ABC123", "2026-04-24",
                                      ARDEF_SCHEMA, METADATA_SCHEMA)
    """
    if transformed.empty:
        log.logger.warning(
            f"TRANSFORM parquet vacio para file_id {file_id}, "
            f"file_processing_date={file_processing_date}"
        )
        all_cols = list(metadata_schema.keys()) + list(ardef_schema.keys())
        return pd.DataFrame([], columns=all_cols)
    
    log.logger.info(
        f"Iniciando limpieza y casteo | {len(transformed)} lineas | "
        f"file_id={file_id}, file_processing_date={file_processing_date}"
    )
    
    df = transformed.copy()

    for col, spec in metadata_schema.items():
        if col not in df.columns:
            log.logger.warning(f"Columna de metadato '{col}' no encontrada en el parquet.")
            continue

        df[col] = _cast_series(df[col], spec["data_type"])

    for col, spec in ardef_schema.items():
        if col not in df.columns:
            log.logger.warning(f"Campo ARDEF '{col}' no encontrado en el parquet")
            continue
        df[col] = _cast_series(df[col], spec["data_type"])

    log.logger.info(
        f"Limpieza y casteo completados | file_id = {file_id}, "
        f"file_processing_date={file_processing_date}"
    )

    return df

def clean_ardef(
        origin_layer: FileStorage.Layer,
        target_layer: FileStorage.Layer,
        file_id: str,
        file_processing_date: str,
        origin_subdir: str = "200_ARDEF_TRA",
        target_subdir: str = "300_ARDEF_CLN",
        ardef_schema: dict[str, dict] | None = None,
        metadata_schema: dict[str, dict] | None = None,
) -> None:
    """
    Etapa CLEAN del pipeline ARDEF. Lee el parquet TRANSFORM, aplica limpieza
    y casteo y escribe en STAGING/300_ARDEF_CLN.

    Pasos:
        1. Leer STAGING / {brand_id} / {file_type} / {date} / 200_ARDEF_TRA / {file_id}.parquet
        2. Limpiar (strip) y castear según METADATA_SCHEMA + ARDEF_SCHEMA.
        3. Escribir STAGING / {brand_id} / {file_type} / {date} / 300_ARDEF_CLN / {file_id}.parquet

    Args:
        origin_layer: Layer de origen (típicamente STAGING).
        target_layer: Layer de destino (típicamente STAGING).
        file_id: ID único del archivo.
        file_processing_date: Fecha de negocio del archivo, "YYYY-MM-DD".
        origin_subdir: Subdirectorio de origen (default "200_ARDEF_TRA").
        target_subdir: Subdirectorio de destino (default "300_ARDEF_CLN").
        ardef_schema: Schema de campos de negocio a castear; si es None,
            usa ARDEF_SCHEMA.
        metadata_schema: Schema de campos de metadata a castear; si es
            None, usa METADATA_SCHEMA.

    Returns:
        None. Escribe el parquet CLEAN en STAGING como efecto secundario.

    Ejemplo:
        clean_ardef(FileStorage.Layer.STAGING, FileStorage.Layer.STAGING,
                    "ABC123", "2026-04-24")
    """
    if ardef_schema is None:
        ardef_schema = ARDEF_SCHEMA

    if metadata_schema is None:
        metadata_schema = METADATA_SCHEMA

    log.logger.info(
        f"Inicio clean_ardef | "
        f"file_id={file_id}, file_processing_date={file_processing_date}"
    )

    # 1. Leer TRANSFORM
    transformed = fs.read_parquet(
        layer=origin_layer,
        file_id=file_id,
        file_processing_date=file_processing_date,
        subdir=origin_subdir,
    )

    # 2. Limpiar y castear
    df_clean = _build_ardef_clean_dataframe(
        transformed=transformed,
        file_id=file_id,
        file_processing_date=file_processing_date,
        ardef_schema=ardef_schema,
        metadata_schema=metadata_schema,
    )

    # 3. Escribir
    output_filepath = fs.write_parquet(
        data = df_clean,
        layer=target_layer,
        file_id=file_id,
        file_processing_date=file_processing_date,
        subdir=target_subdir,
        index=False,
    )

    log.logger.info(
        f"ARDEF CLEAN parquet creado exitosamente: {output_filepath}"
    )
