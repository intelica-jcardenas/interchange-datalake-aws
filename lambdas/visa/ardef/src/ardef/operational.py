"""
operational.py

Última etapa del pipeline ARDEF de Visa. Persiste el resultado ya limpio
(capa CLEAN, 400_ARDEF_CAL) directamente en la capa OPERATIONAL, sin aplicar
transformaciones adicionales — ARDEF no tiene un paso de "calculate" propio
como el pipeline transaccional, el CLEAN ya es el dato final a consumir.
"""

import pandas as pd 

from ardef.logs.logger import Logger
from ardef.persistence.file import FileStorage

log = Logger(__name__)
fs = FileStorage()

def _build_ardef_operational_dataframe(
        clean: pd.DataFrame,
        file_id: str, 
        file_processing_date: str,
) -> pd.DataFrame:
    """
    Prepara el DataFrame a persistir en OPERATIONAL a partir del CLEAN ya
    procesado. No aplica ninguna regla de transformación adicional — solo
    valida que no esté vacío y loguea, devolviendo una copia del CLEAN tal
    cual.

    Args:
        clean: DataFrame leído de la capa CLEAN (400_ARDEF_CAL).
        file_id: PK de la tabla file_control.
        file_processing_date: fecha esperada del archivo, "YYYY-MM-DD".

    Returns:
        Copia del DataFrame `clean` (vacío o con datos, según corresponda).

    Ejemplo:
        _build_ardef_operational_dataframe(clean_df, "0A8221C3...", "2026-01-20")
    """

    if clean.empty:
        log.logger.warning(
            f"CLEAN parquet vacío para file_id={file_id}, "
            f"file_processing_date={file_processing_date}"
        )
        return clean.copy()
    
    log.logger.info(
        f"Preparando carga operacional | {len(clean)} registros | "
        f"file_id={file_id}, file_processing_date={file_processing_date}"
    )

    return clean.copy()

def load_operational_ardef(
        origin_layer: FileStorage.Layer,
        target_layer: FileStorage.Layer,
        file_id: str,
        file_processing_date: str,
        origin_subdir: str = "400_ARDEF_CAL",
        target_subdir: str = "500_ARDEF_OPE",
) -> None:
    """
    Orquesta la última etapa del pipeline ARDEF: lee el parquet CLEAN
    (400_ARDEF_CAL) desde staging, lo prepara vía
    `_build_ardef_operational_dataframe` (sin transformaciones adicionales)
    y lo escribe en la capa OPERATIONAL (500_ARDEF_OPE).

    1. Leer STAGING
    2. Preparar Dataframe Operacional
    3. Escribir en OPERATIONAL

    Args:
        origin_layer: capa S3 de origen (típicamente STAGING).
        target_layer: capa S3 de destino (típicamente OPERATIONAL).
        file_id: PK de la tabla file_control.
        file_processing_date: fecha esperada del archivo, "YYYY-MM-DD".
        origin_subdir: subcarpeta de origen (default "400_ARDEF_CAL").
        target_subdir: subcarpeta de destino (default "500_ARDEF_OPE").

    Returns:
        None. Escribe el parquet final en OPERATIONAL como efecto secundario.

    Ejemplo:
        load_operational_ardef(FileStorage.Layer.STAGING, FileStorage.Layer.OPERATIONAL, "0A8221C3...", "2026-01-20")
    """

    log.logger.info(
        f"Inicio load_operational_ardef | "
        f"file_id={file_id}, file_processing_date={file_processing_date}"
    )

    #1. Leer CLEAN
    clean = fs.read_parquet(
        layer=origin_layer,
        file_id=file_id,
        file_processing_date=file_processing_date,
        subdir=origin_subdir
    )

    #2. Preparar
    df_operational = _build_ardef_operational_dataframe(
        clean=clean,
        file_id=file_id,
        file_processing_date=file_processing_date
    )

    #3. Escribir
    output_filepath = fs.write_parquet(
        data=df_operational,
        layer=target_layer,
        file_id=file_id,
        file_processing_date=file_processing_date,
        subdir=target_subdir,
        index=False,
    )

    log.logger.info(
        f"ARDEF OPERATIONAL parquet creado exitosamente: {output_filepath}"
    )