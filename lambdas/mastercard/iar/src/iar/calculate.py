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


BUSINESS_KEYS = [
    "app_customer_code",
    "low_range",
    "gcms_product",
]

DEDUP_KEYS = [
    "app_customer_code",
    "low_range",
    "gcms_product",
    "effective_timestamp",
    "app_full_data",
]


def apply_scd2_validity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula `app_date_end` de cada registro como el instante justo anterior
    al `app_date_valid` de su sucesor dentro de la misma llave de negocio
    (`BUSINESS_KEYS`), replicando el patrón SCD2: cada fila queda vigente
    desde `app_date_valid` hasta el `app_date_end` calculado (o indefinido,
    `NaT`, si es la versión más reciente de esa llave).

    Args:
        df: DataFrame de IP0040T1 con al menos `BUSINESS_KEYS` +
            `app_date_valid` ya calculado.

    Returns:
        Copia de `df` ordenada por `BUSINESS_KEYS` + `app_date_valid`, con la
        columna `app_date_end` poblada (o `NaT` para la versión vigente).

    Ejemplo:
        apply_scd2_validity(df_ip0040t1)  # agrega/actualiza app_date_end
    """

    df = df.copy()

    df = df.sort_values(
        BUSINESS_KEYS + ["app_date_valid"],
        na_position="last"
    )

    df["_next_app_date_valid"] = (
        df.groupby(BUSINESS_KEYS)["app_date_valid"]
        .shift(-1)
    )

    df["app_date_end"] = df["_next_app_date_valid"] - pd.Timedelta(seconds=1)

    df.loc[df["_next_app_date_valid"].isna(), "app_date_end"] = pd.NaT

    df = df.drop(columns=["_next_app_date_valid"])

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
    df_all["app_creation_date"] = datetime.now()

    df_final = apply_scd2_validity(df_all)

    logger.info(f"SCD2 calculado | Registros={len(df_final)}")

    return df_final