"""
clean.py

Capa CLEAN del pipeline IAR (Mastercard) — módulo interno importado por
`handler.py` (`pipeline_iar`, paso "CLEAN"). Normaliza el DataFrame crudo de
la tabla IP0040T1 (rangos de BIN/reglas) producido por `transform.py`:
completa columnas faltantes del layout, limpia strings, castea numéricos,
parsea fechas y calcula la vigencia inicial (`app_date_valid`) antes de que
`calculate.py` la use para el cálculo SCD2 completo.
"""

from datetime import datetime

import pandas as pd

from schema.schema import IP0040T1_OPERATIONAL_COLUMNS


def clean_ip0040t1(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza y castea el DataFrame de IP0040T1 según el layout operacional
    (`IP0040T1_OPERATIONAL_COLUMNS`): crea con `None` cualquier columna del
    layout ausente en `df`, limpia strings (`strip` + cadena vacía → `NA`),
    castea `low_range`/`high_range` a numérico, parsea `app_processing_date`
    (formato `YYYYMMDD`) y calcula `app_date_valid` a partir de
    `effective_timestamp` (formato Mastercard `yy` + día juliano + `HHMM`,
    completando con `"00"` los minutos faltantes). Agrega metadatos de
    creación (`app_creation_user`/`app_creation_date`) y reordena columnas
    según el layout antes de retornar.

    Args:
        df: DataFrame crudo de IP0040T1 (salida de
            `transform_iar_table_from_raw`).

    Returns:
        Copia de `df` con tipos normalizados y columnas en el orden de
        `IP0040T1_OPERATIONAL_COLUMNS`.

    Ejemplo:
        clean_ip0040t1(df_staging)  # DataFrame listo para la capa CALCULATE
    """
    df = df.copy()

    # Crear columnas faltantes
    for col_name in IP0040T1_OPERATIONAL_COLUMNS:
        if col_name not in df.columns:
            print(
                f"Columna '{col_name}' no encontrada en el DataFrame. "
                "Creando columna con valores nulos."
            )
            df[col_name] = None

    # Limpiar strings
    string_cols = [
        col
        for col in IP0040T1_OPERATIONAL_COLUMNS
        if col in df.columns
    ]

    for col in string_cols:
        df[col] = (
            df[col]
            .astype("string")
            .str.strip()
            .replace("", pd.NA)
        )

    # Numéricos
    df["low_range"] = pd.to_numeric(df["low_range"], errors="coerce")
    df["high_range"] = pd.to_numeric(df["high_range"], errors="coerce")

    # Fecha de procesamiento: YYYYMMDD
    df["app_processing_date"] = pd.to_datetime(
        df["app_processing_date"],
        format="%Y%m%d",
        errors="coerce"
    ).dt.date

    # Campo calculado de vigencia (ver docstring de la función)
    effective_value = (
        df["effective_timestamp"]
        .astype("string")
        .str.strip()
        .fillna("")
        + "00"
    )

    df["app_date_valid"] = pd.to_datetime(
        effective_value,
        format="%y%j%H%M",
        errors="coerce"
    )

    # Campos operacionales
    df["app_date_end"] = pd.NaT
    df["app_creation_user"] = "pipeline_iar"
    df["app_creation_date"] = datetime.now()

    # Reordenar columnas según layout
    df = df[IP0040T1_OPERATIONAL_COLUMNS]

    return df