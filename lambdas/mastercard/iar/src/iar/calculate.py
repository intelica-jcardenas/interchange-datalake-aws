"""
calculate.py

Capa CALCULATE del pipeline IAR (Mastercard) — módulo interno importado por
`handler.py` (`pipeline_iar`, paso "FOR REFERENCE"). Recibe el histórico
acumulado + los registros nuevos de la tabla IP0040T1 (rangos de BIN/reglas),
elimina duplicados por llave de negocio y calcula la vigencia de cada rango
(`app_date_valid`/`app_date_end`) con una lógica tipo SCD2 (Slowly Changing
Dimension tipo 2), para que `glue-mc-calculate` pueda cruzar cada transacción
contra el rango vigente en la fecha correspondiente.
"""

from pathlib import Path
from datetime import datetime

import pandas as pd

from logs.logger import logger


# Sin app_customer_code a propósito: IP0040T1 es la tabla de rangos de cuenta
# de Mastercard, GLOBAL para toda la red — no es exclusiva de cada cliente.
# Confirmado con 3 fuentes de la plataforma legacy (IAR_logica_antes/):
#   1) comentario citando el manual de Mastercard en iar_update.py:
#      "Any unique combination of issuing account range (low) and GCMS
#       product ID generates a separate record of this type"
#   2) comentarios "# part_of_key" en dataelements.py, solo en low_range y
#      gcms_product (ningún otro campo, cliente incluido, está marcado así)
#   3) las 3 queries SQL de upsert en getquery.py hacen JOIN/match únicamente
#      por low_range + gcms_product (+ effective_timestamp para versionar) —
#      nunca por cliente.
# app_customer_code sigue viajando como columna informativa/lineage, pero NO
# participa en el cálculo de vigencia ni en el dedup.
BUSINESS_KEYS = [
    "low_range",
    "gcms_product",
]

DEDUP_KEYS = [
    "low_range",
    "gcms_product",
    "effective_timestamp",
    "app_full_data",
]

# Mismo formato usado en clean.py (DATETIME_STR_FORMAT) para las columnas de
# fecha guardadas como String. app_date_valid llega ya en este formato desde
# clean.py; acá se parsea SOLO en memoria para poder hacer la aritmética de
# vigencia SCD2, y se vuelve a formatear a String antes de devolver — el
# archivo final (data.parquet) nunca guarda un dtype datetime real.
_DATETIME_STR_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


def apply_scd2_validity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula `app_date_end` de cada registro como el instante justo anterior
    al `app_date_valid` de su sucesor dentro de la misma llave de negocio
    (`BUSINESS_KEYS`), replicando el patrón SCD2: cada fila queda vigente
    desde `app_date_valid` hasta el `app_date_end` calculado (o indefinido,
    `NaT`/vacío, si es la versión más reciente de esa llave).

    `app_date_valid`/`app_date_end` se guardan como String en `df` (alineado
    con la convención de ARDEF/historic_data.parquet) — acá se parsean a
    datetime solo temporalmente, en memoria, para poder hacer la resta de
    vigencia; el resultado se vuelve a formatear a String antes de retornar.

    Args:
        df: DataFrame de IP0040T1 con al menos `BUSINESS_KEYS` +
            `app_date_valid` ya calculado (String).

    Returns:
        Copia de `df` ordenada por `BUSINESS_KEYS` + `app_date_valid`, con la
        columna `app_date_end` poblada como String (o `None` para la versión
        vigente).

    Ejemplo:
        apply_scd2_validity(df_ip0040t1)  # agrega/actualiza app_date_end
    """

    df = df.copy()

    app_date_valid_dt = pd.to_datetime(
        df["app_date_valid"], format=_DATETIME_STR_FORMAT, errors="coerce"
    )
    df = df.assign(_app_date_valid_dt=app_date_valid_dt)

    df = df.sort_values(
        BUSINESS_KEYS + ["_app_date_valid_dt"],
        na_position="last"
    )

    df["_next_app_date_valid_dt"] = (
        df.groupby(BUSINESS_KEYS)["_app_date_valid_dt"]
        .shift(-1)
    )

    app_date_end_dt = df["_next_app_date_valid_dt"] - pd.Timedelta(seconds=1)
    app_date_end_dt = app_date_end_dt.where(df["_next_app_date_valid_dt"].notna())

    df["app_date_end"] = (
        app_date_end_dt.dt.strftime(_DATETIME_STR_FORMAT).str[:-3]
    )
    df["app_date_valid"] = (
        df["_app_date_valid_dt"].dt.strftime(_DATETIME_STR_FORMAT).str[:-3]
    )

    df = df.drop(columns=["_app_date_valid_dt", "_next_app_date_valid_dt"])

    return df


def calculate_ip0040t1_operational(
    df_new: pd.DataFrame, operational_path=None,
) -> pd.DataFrame:
    """
    Capa CALCULATE para la tabla IP0040T1 (rangos de BIN/reglas IAR). Recibe
    el histórico ya acumulado más los registros nuevos de la ejecución actual
    (concatenados por el caller antes de llamar a esta función), deduplica
    por `DEDUP_KEYS` (quedándose con la última versión por llave), marca
    metadatos de creación y calcula la vigencia SCD2 (`apply_scd2_validity`).
    El resultado sobrescribe la tabla maestra en
    `s3-reference/mastercard_iar/data.parquet`.

    Args:
        df_new: DataFrame con histórico + registros nuevos ya concatenados.
        operational_path: No usado actualmente (reservado, siempre `None`
            en la llamada real desde `handler.py`).

    Returns:
        DataFrame final con dedup aplicado y `app_date_valid`/`app_date_end`
        calculados — listo para escribir como la nueva tabla maestra.

    Ejemplo:
        calculate_ip0040t1_operational(df_historico_mas_nuevo)
    """

    df_all = df_new.copy()

    logger.info(f"Registros histórico+nuevo | Registros={len(df_all)}")

    df_all = df_all.drop_duplicates(
        subset=DEDUP_KEYS,
        keep="last"
    )

    logger.info(f"Registros luego dedup | Registros={len(df_all)}")

    df_all["app_creation_user"] = "pipeline_iar"
    df_all["app_creation_date"] = datetime.now().strftime(_DATETIME_STR_FORMAT)[:-3]

    df_final = apply_scd2_validity(df_all)

    logger.info(f"SCD2 calculado | Registros={len(df_final)}")

    return df_final