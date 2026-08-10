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

# Formato de fecha/hora usado en todas las columnas de fecha (String) del
# layout — alineado con la convención de ARDEF/historic_data.parquet: todas
# las columnas se guardan como String, sin excepción. calculate.py se encarga
# de parsear app_date_valid solo internamente para el cálculo SCD2, y vuelve
# a formatear con este mismo patrón antes de devolver el resultado.
DATETIME_STR_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


def _format_datetime_str(series: pd.Series) -> pd.Series:
    """Formatea una Serie datetime a String 'YYYY-MM-DD HH:MM:SS.mmm' (ms, no us)."""
    return series.dt.strftime(DATETIME_STR_FORMAT).str[:-3]


# Columnas que esta misma función calcula siempre más abajo, sin importar lo
# que traiga el DataFrame de entrada — nunca vienen en el archivo IAR crudo,
# así que no son un caso de "columna faltante inesperada" y no deben avisarse
# como tal en el loop de abajo.
_COLUMNS_COMPUTED_LATER = {"app_date_valid", "app_date_end", "app_creation_date"}


def clean_ip0040t1(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza el DataFrame de IP0040T1 según el layout operacional
    (`IP0040T1_OPERATIONAL_COLUMNS`): crea con `None` cualquier columna del
    layout ausente en `df`, limpia strings (`strip` + cadena vacía → `NA`).
    Todas las columnas quedan como String (incluidas low_range/high_range y
    las fechas), alineado con la convención de ARDEF/historic_data.parquet.
    Parsea `app_processing_date` (formato `YYYYMMDD`) y calcula `app_date_valid` a partir de
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

    # Crear columnas faltantes (deberia ser solo del layout crudo — las que
    # esta funcion calcula mas abajo se excluyen, ver _COLUMNS_COMPUTED_LATER)
    for col_name in IP0040T1_OPERATIONAL_COLUMNS:
        if col_name in _COLUMNS_COMPUTED_LATER:
            continue
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

    # low_range/high_range se quedan como String (ya cubiertos arriba por
    # string_cols) — sin cast numérico, alineado con la convención de ARDEF/
    # historic_data.parquet de guardar todo como String.

    # Fecha de procesamiento: YYYYMMDD -> String 'YYYY-MM-DD'
    df["app_processing_date"] = pd.to_datetime(
        df["app_processing_date"],
        format="%Y%m%d",
        errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    # Campo calculado de vigencia (ver docstring de la función)
    effective_value = (
        df["effective_timestamp"]
        .astype("string")
        .str.strip()
        .fillna("")
        + "00"
    )

    app_date_valid_dt = pd.to_datetime(
        effective_value,
        format="%y%j%H%M",
        errors="coerce"
    )
    df["app_date_valid"] = _format_datetime_str(app_date_valid_dt)

    # Campos operacionales — app_date_end se recalcula en calculate.py
    # (apply_scd2_validity); acá queda nulo, como String también.
    df["app_date_end"] = pd.array([pd.NA] * len(df), dtype="string")
    df["app_creation_user"] = "pipeline_iar"
    df["app_creation_date"] = _format_datetime_str(
        pd.Series([datetime.now()] * len(df))
    )

    # Reordenar columnas según layout
    df = df[IP0040T1_OPERATIONAL_COLUMNS]

    return df