"""
interchange.py — Job real: itl-0004-itx-{env}-intchg-02-glue-vi-interchange
================================================================================
Archivo:     glue/scripts/visa/interchange/interchange.py
S3 Script:   s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/visa/interchange.py

Job de Glue (PySpark, Glue 4.0, Worker G.2X x4) que asigna la tarifa de
interchange a cada transacción Visa (BASEII/SMS) evaluando el maestro de
reglas `visa_rules` con un motor first-match-wins por jurisdicción (soporta
condiciones de lista/rango, comparación numérica y montos con conversión de
moneda). Lee las capas Clean (`s3-staging/.../300_*_cln_*`) y Calculate
(`s3-staging/.../400_*_cal_*`, con los campos derivados de ARDEF ya
calculados) del mismo archivo, las une por `record`, y evalúa las reglas en
paralelo sobre los workers Spark vía `mapInPandas` — el motor de reglas en sí
es pandas puro, igual al prototipo local ya validado. Escribe el resultado en
la capa Interchange (`s3-staging/.../500_*_itx_*`), que `lmbd-vi-store`
consolida junto con CLN y CAL en el Parquet final de operational.

VSS (registros de liquidación) no pasa por este job — no tiene reglas de
interchange propias; se usa solo como contraste de Data Quality en otro punto
del pipeline.

Arquitectura del rule engine:
  - Las reglas (`visa_rules`, filtradas por vigencia a `file_date`) y las
    tasas de cambio se cargan una sola vez y se hacen `broadcast()` a todos
    los workers.
  - `evaluate_interchange_fees()` reparte las transacciones en particiones
    Spark; cada partición corre `_evaluate_rules_pandas()` de forma
    independiente en el worker (sin volver al driver), lo que permite
    escalar a cualquier volumen sin OOM.
  - `calculate_fee_amounts()` aplica la fórmula final (fee_variable ×
    source_amount + fee_fixed convertido a la moneda de la transacción),
    con fee_min/fee_cap como piso/techo, ya en Spark (no en el engine
    pandas).

Job Parameters:
  --client_id        Código de cliente (ej. "EBGR")
  --file_id          ID del archivo procesado
  --file_type        IN | OUT
  --file_date        YYYY-MM-DD (fecha del archivo — filtra vigencia de
                      reglas y tasas de cambio)
  --staging_bucket   Bucket S3 de staging
  --reference_bucket Bucket S3 de referencia (visa_rules, exchange-rates-glue)
  --outputs          JSON: [{"output_type": "BASEII"|"SMS"|"VSS",
                     "s3_key": "staging/..."}, …] — VSS se recibe pero se
                     descarta (sin reglas de interchange propias).
"""

import sys
import json
import uuid
from datetime import datetime, date
import pandas as pd
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, LongType
)
import boto3

s3_client = boto3.client("s3")

# =============================================================================
# CONFIGURACIÓN SPARK
# =============================================================================

spark = SparkSession.builder \
    .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
    .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
    .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS") \
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)
logger = glueContext.get_logger()

def log_info(msg): logger.info(f"GlueLogger: {msg}")
def log_error(msg): logger.error(f"GlueLogger: {msg}")

# =============================================================================
# HELPERS: S3
# =============================================================================

def load_parquet(path: str) -> DataFrame:
    """
    Lee un Parquet completo desde S3 como Spark DataFrame, logueando la ruta
    y el conteo de filas resultante para trazabilidad en CloudWatch.

    Args:
        path: URL S3 completa del Parquet, ej.
            "s3://itl-0004-itx-dev-intchg-02-s3-staging/EBGR/VISA/300_baseii_cln_drafts/...".

    Returns:
        DataFrame de Spark con el contenido leído.

    Ejemplo:
        cln_df = load_parquet(cln_path)
    """
    log_info(f"  Reading: {path}")
    df = spark.read.parquet(path)
    log_info(f"  → {df.count():,} records")
    return df

def save_parquet(df: DataFrame, path: str):
    """
    Guarda el DataFrame como Parquet en S3, con estrategia de particionado de
    escritura según el volumen: `write_single_parquet()` para archivos chicos
    (un solo Parquet, más simple de leer) y `write_parquet_multi()` para
    archivos grandes (4 part-files en paralelo, evitando el error "RPC
    message too large" que puede ocurrir al coalescer demasiadas filas a una
    sola partición). Ninguna de las 2 variantes deja el marcador de
    directorio `_$folder$` que el committer de Spark/Hadoop genera al
    escribir directo con `df.write.parquet(path)` — mismo patrón ya validado
    en `glue/scripts/mastercard/interchange/interchange.py` (single) y
    `glue/scripts/reports/scheme_fee/scheme_fee.py` (multi).

    Args:
        df: DataFrame a escribir.
        path: URL S3 destino, ej.
            "s3://itl-0004-itx-dev-intchg-02-s3-staging/EBGR/VISA/500_baseii_itx_drafts/...".

    Returns:
        None.

    Ejemplo:
        save_parquet(result, itx_path)
    """
    count = df.count()
    if count > 200_000:
        log_info(f"  Large file ({count:,} rows) — using write_parquet_multi (4 partitions)")
        write_parquet_multi(df, path)
    else:
        write_single_parquet(df, path)
    log_info(f"  Saved: {path}")


def _list_all_keys(bucket: str, prefix: str) -> list:
    """
    Lista TODAS las keys bajo un prefijo S3, paginando con
    list_objects_v2 (que devuelve máximo 1000 por llamada) — usado por
    `write_parquet_multi()` tanto para encontrar los part-files recién
    escritos como para limpiar el prefijo final/temporal antes de
    escribir, donde asumir "una sola página" sería un bug silencioso en
    datasets que crezcan más allá de 1000 objetos.

    Args:
        bucket: Nombre del bucket S3.
        prefix: Prefijo a listar.

    Returns:
        Lista de keys (str) bajo ese prefijo, vacía si no hay ninguna.

    Ejemplo:
        _list_all_keys("bucket", "EBGR/VISA/500_baseii_itx_drafts/file_type=IN/date=2026-01-03/")
    """
    keys = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3_client.list_objects_v2(**kwargs)
        keys.extend(obj["Key"] for obj in resp.get("Contents", []))
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return keys


def _delete_all_keys(bucket: str, keys: list) -> None:
    """
    Borra una lista de keys S3 en lotes de 1000 — límite duro de la API
    `delete_objects` (rechaza el request completo si se le pasan más).
    Usado por `write_parquet_multi()` para limpiar el prefijo final antes
    de escribir y el prefijo temporal al terminar.

    Args:
        bucket: Nombre del bucket S3.
        keys: Lista de keys a borrar (puede estar vacía).

    Returns:
        None.

    Ejemplo:
        _delete_all_keys("bucket", ["a.parquet", "b.parquet"])
    """
    for i in range(0, len(keys), 1000):
        chunk = keys[i:i + 1000]
        s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in chunk]},
        )


def write_single_parquet(df: DataFrame, final_s3_path: str) -> None:
    """
    Escribe `df` como un único archivo Parquet en `final_s3_path`, sin el
    marcador de directorio `_$folder$` que el committer de Spark/Hadoop
    genera al escribir directo con `df.write.parquet(path)`. Se escribe a
    un prefijo temporal descartable (nombre único con uuid4) y se copia
    el único part-file al key final exacto vía `copy_object` — sin pasar
    por el committer en el destino final, así que ahí nunca se genera el
    marcador. El prefijo temporal completo se borra siempre al final,
    haya fallado o no la escritura.

    Mismo patrón ya validado en `glue/scripts/mastercard/interchange/interchange.py`
    y `glue/scripts/reports/get_transaction/get_transaction.py`.

    Args:
        df: DataFrame a escribir (se fuerza a 1 solo part-file con
            `coalesce(1)`).
        final_s3_path: URI s3:// completo del archivo final, incluyendo
            el nombre ".parquet".

    Returns:
        None.

    Raises:
        RuntimeError: si no se encuentra ningún part-file tras escribir
            el prefijo temporal.

    Ejemplo:
        write_single_parquet(result_df, "s3://bucket/EBGR/VISA/500_baseii_itx_drafts/.../x.parquet")
    """
    bucket, final_key = final_s3_path[len("s3://"):].split("/", 1)
    tmp_prefix = f"{final_key}_tmp_{uuid.uuid4().hex}/"
    tmp_uri = f"s3://{bucket}/{tmp_prefix}"

    try:
        df.coalesce(1).write.mode("overwrite").parquet(tmp_uri)

        resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=tmp_prefix)
        part_keys = [
            obj["Key"] for obj in resp.get("Contents", [])
            if obj["Key"].endswith(".parquet") and "/part-" in obj["Key"]
        ]
        if not part_keys:
            raise RuntimeError(f"No se encontró part parquet en {tmp_uri}")

        s3_client.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": part_keys[0]},
            Key=final_key,
        )
    finally:
        cleanup = s3_client.list_objects_v2(Bucket=bucket, Prefix=tmp_prefix)
        leftover = [{"Key": obj["Key"]} for obj in cleanup.get("Contents", [])]
        if leftover:
            s3_client.delete_objects(Bucket=bucket, Delete={"Objects": leftover})


def write_parquet_multi(df: DataFrame, final_s3_path: str) -> None:
    """
    Escribe `df` como varios archivos Parquet (4 part-files, vía
    `repartition(4)`) bajo el prefijo de `final_s3_path`, preservando el
    paralelismo de escritura para archivos grandes, sin el marcador de
    directorio `_$folder$` que el committer de Spark/Hadoop genera al
    escribir directo con `df.write.parquet(path)`.

    A diferencia de `write_single_parquet()`, esta variante NO fuerza un
    solo archivo — forzar `coalesce(1)` en archivos de >200,000 filas es
    justamente lo que `save_parquet()` evita (riesgo de "RPC message too
    large"). Mecánica: escribe a un prefijo temporal descartable (nombre
    único con uuid4), copia TODOS los part-files resultantes al prefijo
    final vía `copy_object`, y borra el prefijo temporal completo. Antes
    de copiar, limpia el prefijo final existente — mismo efecto que
    `mode("overwrite")` hubiera tenido escribiendo directo. Mismo patrón
    ya validado en `write_parquet_multi()` de
    `glue/scripts/reports/scheme_fee/scheme_fee.py`.

    Args:
        df: DataFrame a escribir (se fuerza a 4 part-files con
            `repartition(4)`).
        final_s3_path: URI s3:// completo del archivo final "lógico" —
            el prefijo real usado es este path sin el sufijo ".parquet"
            (los 4 part-files quedan bajo ese prefijo, como ya hacía
            `df.repartition(4).write.parquet(path)` antes de este fix).

    Returns:
        None.

    Raises:
        RuntimeError: si no se encuentra ningún part-file tras escribir
            el prefijo temporal.

    Ejemplo:
        write_parquet_multi(result_df, "s3://bucket/EBGR/VISA/500_baseii_itx_drafts/.../x.parquet")
    """
    bucket, final_key = final_s3_path[len("s3://"):].split("/", 1)
    final_prefix = final_key.rstrip("/")
    tmp_prefix = f"{final_prefix}_tmp_{uuid.uuid4().hex}/"
    tmp_uri = f"s3://{bucket}/{tmp_prefix}"

    try:
        df.repartition(4).write.mode("overwrite").parquet(tmp_uri)

        part_keys = [
            key for key in _list_all_keys(bucket, tmp_prefix)
            if key.endswith(".parquet") and "/part-" in key
        ]
        if not part_keys:
            raise RuntimeError(f"No se encontró part parquet en {tmp_uri}")

        existing_keys = _list_all_keys(bucket, f"{final_prefix}/")
        _delete_all_keys(bucket, existing_keys)

        for part_key in part_keys:
            filename = part_key.rsplit("/", 1)[-1]
            s3_client.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": part_key},
                Key=f"{final_prefix}/{filename}",
            )
    finally:
        _delete_all_keys(bucket, _list_all_keys(bucket, tmp_prefix))

# =============================================================================
# CARGA DE TABLAS DE REFERENCIA
# =============================================================================

def load_visa_rules(reference_bucket: str, file_date: date) -> pd.DataFrame:
    """
    Carga el maestro de reglas de interchange Visa (`visa_rules`) y lo filtra
    a las reglas vigentes en `file_date` (entre `valid_from` y `valid_until`
    inclusive; filas sin fecha de vigencia se tratan como vigentes hasta hoy).
    Se lee una sola vez por ejecución del job y se reutiliza (via broadcast)
    para todos los outputs del mismo archivo.

    Args:
        reference_bucket: Bucket S3 de referencia, ej.
            "itl-0004-itx-dev-intchg-02-s3-reference".
        file_date: Fecha de negocio del archivo, usada para filtrar vigencia.

    Returns:
        DataFrame de pandas con las reglas vigentes, ordenado por
        (region_country_code, intelica_id), índice reseteado.

    Ejemplo:
        rules_pd = load_visa_rules("itl-0004-itx-dev-intchg-02-s3-reference", date(2026, 1, 3))
    """
    path = f"s3://{reference_bucket}/visa_rules/data.parquet"
    log_info(f"Loading visa_rules from: {path}")
    df = spark.read.parquet(path).toPandas()

    numeric_cols = ["fee_variable", "fee_fixed", "fee_min", "fee_cap", "intelica_id"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["valid_from", "valid_until"]:
        df[col] = pd.to_datetime(
            df[col].astype(str).str.slice(0, 10),
            format="%Y-%m-%d", errors="coerce"
        ).dt.date
    df[["valid_from", "valid_until"]] = df[["valid_from", "valid_until"]].fillna(date.today())

    df = df[(file_date >= df["valid_from"]) & (file_date <= df["valid_until"])]
    df = df.sort_values(["region_country_code", "intelica_id"])

    log_info(f"visa_rules loaded: {len(df):,} rules for date {file_date}")
    return df.reset_index(drop=True)


def load_exchange_rates(reference_bucket: str, file_date: date, brand: str) -> pd.DataFrame:
    """
    Carga las tasas de cambio del día para la marca indicada desde
    `exchange-rates-glue/brand={brand}/exchange_date={date}/` — la fuente
    oficial y viva de tipo de cambio del pipeline (enriquecida con códigos
    numéricos por el job `glue-exchange-rates`; reemplaza a `exchange_rate/`,
    fuente manual congelada al 2026-04-30). Columnas renombradas a los
    nombres legacy (`currency_from`/`currency_to`/`exchange_value`) para no
    tocar el resto del script, que ya espera ese vocabulario.

    Args:
        reference_bucket: Bucket S3 de referencia.
        file_date: Fecha de negocio del archivo.
        brand: Marca a filtrar, ej. "VISA" (se capitaliza para armar la
            partición `brand=Visa/`).

    Returns:
        DataFrame de pandas con columnas currency_from, currency_to,
        exchange_value. Si la partición exacta no existe, hace fallback
        leyendo toda la tabla y filtrando por brand + exchange_date.

    Ejemplo:
        rates_pd = load_exchange_rates("itl-0004-itx-dev-intchg-02-s3-reference", date(2026, 1, 3), "VISA")
    """
    date_str = file_date.strftime("%Y-%m-%d")
    brand_partition = brand.capitalize()
    path = f"s3://{reference_bucket}/exchange-rates-glue/brand={brand_partition}/exchange_date={date_str}/"
    log_info(f"Loading exchange_rates from: {path}")

    try:
        df_spark = spark.read.parquet(path)
    except Exception as ex:
        log_info(f"  Partition not found ({ex}). Fallback: reading full exchange-rates-glue and filtering.")
        base_path = f"s3://{reference_bucket}/exchange-rates-glue/"
        df_spark = spark.read.parquet(base_path) \
            .filter(F.col("brand") == brand_partition) \
            .filter(F.col("exchange_date") == date_str)

    df_spark = df_spark.select(
        F.col("from_currency").alias("currency_from"),
        F.col("to_currency").alias("currency_to"),
        F.col("fx_rate").alias("exchange_value"),
    )

    df = df_spark.toPandas()

    if not df.empty:
        df["exchange_value"] = pd.to_numeric(df["exchange_value"], errors="coerce")

    log_info(f"exchange_rates loaded: {len(df):,} rates")
    return df

# =============================================================================
# RENOMBRADO DE REGLAS SEGÚN TYPE_RECORD
# =============================================================================

def _rename_rules(rules_pd: pd.DataFrame, type_record: str) -> pd.DataFrame:
    """
    Renombra las columnas de condición de `visa_rules` (nombres genéricos del
    maestro) a los nombres de campo reales que trae el Parquet de
    transacciones, para poder evaluarlas directamente contra el DataFrame de
    transacciones sin una capa de traducción en tiempo de evaluación. El
    mapeo difiere entre BASEII ("draft") y SMS porque cada `type_record`
    tiene su propio vocabulario de campos (ver `visa_fields` en DynamoDB);
    columnas del maestro sin equivalente en el `type_record` dado se
    descartan (`drop_cols`).

    Args:
        rules_pd: Reglas ya filtradas por vigencia (salida de
            `load_visa_rules`).
        type_record: "draft" (BASEII) o "sms". Cualquier otro valor deja
            `rules_pd` sin cambios.

    Returns:
        Copia de `rules_pd` con columnas renombradas/descartadas según
        `type_record`.

    Ejemplo:
        rules_renamed = _rename_rules(rules_pd, "draft")
    """
    rules_pd = rules_pd.copy()
    rules_pd.columns = [c.lower() for c in rules_pd.columns]

    if type_record == "draft":
        rename_map = {
            "account_funding_source": "funding_source",
            "acquirer_bin": "account_reference_number_acquiring_identifier",
            "authorization_code": "authorization_code_valid",
            "cvv2_result_code": "cvv_result_code",
            "dynamic_currency_conversion_indicator": "dcc_indicator",
            "merchant_country_code": "jurisdiction_country",
            "merchant_country_region": "jurisdiction_region",
            "merchant_vat": "merchant_vat_registration_number",
            "moto_eci_indicator": "moto_ec_indicator",
            "national_tax_indicator": "national_tax_included",
            "pos_environment_code": "pos_environment",
            "pos_terminal_capability": "pos_terminal_capacity",
            "special_condition_indicator": "special_condition_indicator_merchant_draft_indicator",
            "summary_commodity": "summary_commodity_code",
            "transaction_amount": "source_amount",
            "transaction_amount_currency": "source_currency_code_alphabetic",
            "transaction_code_qualifier": "draft_code_qualifier_0",
            "transaction_code": "draft_code",
            "type_purchase": "type_of_purchase",
        }
        drop_cols = [
            "acquirer_country", "acquirer_region",
            "processing_code_transaction_type", "point_of_service_condition_code"
        ]
    elif type_record == "sms":
        rename_map = {
            "account_funding_source": "funding_source",
            "acceptance_terminal_indicator": "pos_terminal_type",
            "acquirer_business_id": "acquirer_business_id_sms",
            "authorization_characteristics_indicator": "authorization_characteristics_indicator_sms",
            "authorization_code": "authorization_code_valid",
            "authorization_response_code": "response_code",
            "business_application_id": "business_application_identifier",
            "cardholder_id_method": "customer_identification_method",
            "cvv2_result_code": "cvv_result_code_sms",
            "dynamic_currency_conversion_indicator": "dcc_indicator_sms",
            "fee_program_indicator": "fee_program_indicator_sms",
            "merchant_category_code": "merchant's_type",
            "merchant_country_code": "jurisdiction_country",
            "merchant_country_region": "jurisdiction_region",
            "merchant_verification_value": "mvv_code",
            "message_reason_code": "message_reason_code_sms",
            "moto_eci_indicator": "mail_telephone_or_electronic_commerce_indicator",
            "network_identification_code": "network_id",
            "point_of_service_condition_code": "pos_condition_code",
            "pos_environment_code": "recurring_payment_indicator_flag",
            "pos_entry_mode": "pos_entry_mode_sms",
            "pos_terminal_capability": "pos_terminal_entry_capability",
            "reimbursement_attribute": "reimbursement_attribute_sms",
            "special_condition_indicator": "chargeback_special_condition_merchant_indicator",
            "summary_commodity": "summary_commodity_code",
            "surcharge_amount": "surcharge_amount_sms",
            "transaction_amount": "source_amount",
            "transaction_amount_currency": "source_currency_code_alphabetic",
            "transaction_code": "transaction_code_sms",
            "usage_code": "usage_code_sms",
        }
        drop_cols = [
            "acquirer_country", "acquirer_region", "authorized_amount",
            "business_format_code", "merchant_vat", "national_tax_indicator",
            "prepaid_card_indicator", "summary_commodity",
            "transaction_code_qualifier", "type_purchase",
            "settlement_flag", "token_requestor_id", "cashback",
        ]
    else:
        return rules_pd

    existing_drops = [c for c in drop_cols if c in rules_pd.columns]
    rules_pd = rules_pd.drop(columns=existing_drops, errors="ignore")
    existing_rename = {k: v for k, v in rename_map.items() if k in rules_pd.columns}
    rules_pd = rules_pd.rename(columns=existing_rename)
    return rules_pd

# =============================================================================
# CONVERSIÓN DE MONEDA (Spark)
# =============================================================================

def _add_converted_amount(
    transactions: DataFrame,
    rates_pd: pd.DataFrame,
    rules_pd: pd.DataFrame
) -> DataFrame:
    """
    Pre-calcula, en Spark, una columna `source_amount_{moneda}` por cada
    moneda que las reglas usan como condición de monto (`source_amount`) —
    necesario porque las reglas expresan su umbral en una moneda propia
    (`source_currency_code_alphabetic` de la regla) que puede no coincidir
    con la moneda real de cada transacción, y el motor de reglas en pandas
    (`_apply_amount_currency`) necesita el monto ya convertido a esa moneda
    antes de comparar contra el umbral.

    Args:
        transactions: DataFrame de transacciones (CLN+CAL ya unidos).
        rates_pd: Tasas de cambio del día (columnas currency_from,
            currency_to, exchange_value).
        rules_pd: Reglas ya renombradas (salida de `_rename_rules`).

    Returns:
        `transactions` con una columna `source_amount_{moneda}` agregada por
        cada moneda target encontrada en las reglas; sin cambios si las
        reglas no tienen condición de monto.

    Ejemplo:
        transactions = _add_converted_amount(transactions, rates_pd, rules_renamed)
        # agrega, por ejemplo, source_amount_eur, source_amount_usd
    """
    if "source_amount" not in rules_pd.columns:
        return transactions
    if "source_currency_code_alphabetic" not in rules_pd.columns:
        return transactions

    target_currencies = (
        rules_pd[rules_pd["source_amount"].notna()]
        ["source_currency_code_alphabetic"]
        .dropna()
        .str.strip()
        .str.upper()
        .unique()
        .tolist()
    )
    target_currencies = [c for c in target_currencies if c not in ("", "NAN", "NONE")]

    if not target_currencies:
        return transactions

    log_info(f"  Creating converted amount columns for: {target_currencies}")

    for target_curr in target_currencies:
        col_name = f"source_amount_{target_curr.lower()}"

        target_rates = rates_pd[
            rates_pd["currency_to"].str.upper() == target_curr
        ][["currency_from", "exchange_value"]].copy()

        if target_rates.empty:
            log_info(f"  Warning: no rates found for currency_to={target_curr}")
            continue

        rates_spark = spark.createDataFrame(target_rates)

        transactions = transactions.join(
            F.broadcast(rates_spark),
            transactions["source_currency_code_alphabetic"] == rates_spark["currency_from"],
            how="left"
        ).withColumn(
            col_name,
            F.when(
                F.upper(F.col("source_currency_code_alphabetic")) == target_curr,
                F.col("source_amount")
            ).otherwise(
                F.col("source_amount") * F.col("exchange_value")
            )
        ).drop("currency_from", "exchange_value")

        log_info(f"  Added: {col_name}")

    return transactions

# =============================================================================
# EVALUACIÓN DE REGLAS EN PANDAS (First-Match-Wins)
# =============================================================================

CONDITIONS_TO_SKIP = {
    "region_country_code", "valid_from", "valid_until", "intelica_id",
    "fee_descriptor", "fee_description", "fee_currency", "fee_variable",
    "fee_fixed", "fee_min", "fee_cap", "jurisdiction", "fee_program",
    "guide_date", "fpi", "cod_hierarchy", "program_default",
    "source_currency_code_alphabetic", "message_identifier",
    "validation_code", "v_i_p_full_financial_message_sets", "sender_data",
    "additional_sender_data", "settlement_service", "other_criteria_applies"
}

COLUMN_GROUP_GREATER_LESS = {
    "timeliness", "surcharge_amount", "surcharge_amount_sms"
}

COLUMN_GROUP_AMOUNT_CURRENCY = {"source_amount"}

COLUMN_GROUP_SPACE = {
    "nnss_indicator", "cardholder_id_method", "moto_ec_indicator",
    "moto_eci_indicator", "acceptance_terminal_indicator", "merchant_vat",
    "mail_telephone_or_electronic_commerce_indicator"
}

COLUMN_GROUP_YES_NO = {"cashback"}


def _apply_default(
    condition_name: str,
    condition_value: str,
    batch: pd.DataFrame
) -> pd.DataFrame:
    """
    Evalúa una condición de regla "por lista de valores" (la mayoría de las
    condiciones de `visa_rules` — todo lo que no sea rango numérico o monto
    con moneda, ver `_apply_condition_pandas`). Soporta:
      - Lista separada por comas: "05,06,07" → matchea cualquiera de esos
        valores.
      - Rangos con guion dentro de un elemento de la lista: "1-5" → se
        expande a ["1","2","3","4","5"] antes de comparar.
      - Prefijo "NOT:" → invierte el criterio (excluye los valores listados
        en vez de incluirlos).
      - Literal "SPACE" → se traduce a un espacio literal `" "` (para
        columnas en COLUMN_GROUP_SPACE, donde el espacio es un valor válido
        y no debe normalizarse a "BLANK").
      - Valores vacíos/nulos en la columna del batch → normalizados a
        "BLANK" antes de comparar (salvo columnas de COLUMN_GROUP_SPACE, que
        se comparan tal cual para no perder el espacio literal).

    Args:
        condition_name: Nombre de la columna de transacción a evaluar (ya
            renombrada por `_rename_rules`).
        condition_value: Valor crudo de la condición tal como viene en
            `visa_rules`, ej. "05,06,07" o "NOT:Y".
        batch: Sub-batch de transacciones aún sin matchear para esta regla.

    Returns:
        Subconjunto de `batch` cuyas filas cumplen la condición.

    Ejemplo:
        _apply_default("draft_code", "05,06,07", batch)
    """
    batch = batch.copy()
    condition_value = condition_value.strip().upper()
    condition_value = condition_value.replace("SPACE", " ")

    not_flag = "NOT:" in condition_value
    if not_flag:
        condition_value = condition_value.replace("NOT:", "")

    value_list = condition_value.split(",")
    valid_values = []
    not_valid_values = []

    for value in value_list:
        filled_range = []
        if "-" in value and not value.startswith("-"):
            try:
                range_low, range_high = value.split("-", maxsplit=1)
                filled_range = [str(i) for i in range(int(range_low), int(range_high) + 1)]
            except ValueError:
                pass
        reformatted = filled_range or [value]
        if not_flag:
            not_valid_values.extend(reformatted)
        else:
            valid_values.extend(reformatted)

    if condition_name in COLUMN_GROUP_SPACE:
        batch["_normalized"] = batch[condition_name].astype(str)
    else:
        temp = batch[condition_name].fillna("").astype(str).str.strip()
        temp = temp.mask(temp.str.len() == 0, "BLANK")
        batch["_normalized"] = temp

    if valid_values:
        batch = batch[batch["_normalized"].isin(valid_values)]
    if not_valid_values:
        batch = batch[~batch["_normalized"].isin(not_valid_values)]

    return batch.drop(columns=["_normalized"])


def _apply_greater_less(
    condition_name: str,
    condition_value: str,
    batch: pd.DataFrame
) -> pd.DataFrame:
    """
    Evalúa una condición de comparación numérica sobre columnas de
    COLUMN_GROUP_GREATER_LESS (`timeliness`, `surcharge_amount`,
    `surcharge_amount_sms`). Soporta tres formatos de `condition_value` tal
    como vienen en `visa_rules`:
      - Operador relacional: ">5", "<=10", "=3" → se arma una query de
        pandas (`batch.query(...)`).
      - Rango: "BETWEEN 5 AND 10" → inclusive en ambos extremos.
      - Valor exacto sin operador: "3" → equivalente a "=3".
    Si `condition_value` no matchea ninguno de los tres formatos, retorna
    `batch` sin filtrar (condición no aplicable/mal formada).

    Args:
        condition_name: Columna numérica a evaluar.
        condition_value: Valor de la condición, ej. ">5", "BETWEEN 1 AND 10".
        batch: Sub-batch de transacciones aún sin matchear para esta regla.

    Returns:
        Subconjunto de `batch` que cumple la condición.

    Ejemplo:
        _apply_greater_less("timeliness", "BETWEEN 1 AND 5", batch)
    """
    if any(x in condition_value for x in ["<", ">", "="]):
        query = f"{condition_name} " + condition_value \
            .replace("<=", "<= ").replace(">=", ">= ") \
            .replace(">", "> ").replace("<", "< ")
        return batch.query(query)
    elif "BETWEEN" in condition_value.upper() and "AND" in condition_value.upper():
        lo, hi = map(float, condition_value.upper()
                     .replace("BETWEEN", "").strip().split("AND"))
        return batch[batch[condition_name].astype(float).between(lo, hi, inclusive="both")]
    elif condition_value.replace(".", "", 1).isdigit():
        return batch[batch[condition_name].astype(float) == float(condition_value)]
    return batch


def _apply_yes_no(
    condition_name: str,
    condition_value: str,
    batch: pd.DataFrame
) -> pd.DataFrame:
    """
    Evalúa una condición de COLUMN_GROUP_YES_NO (`cashback`) — columnas que
    en `visa_rules` se expresan como bandera "Yes"/"No" pero cuyo campo
    transaccional real es un monto (ej. `cashback`, monto con 2 decimales).
    Replica `visa_interchange_rule_assign` (`adapters.py`, legacy real):
    "No" → monto == 0; cualquier otro valor ("Yes") → monto > 0.

    Diferencia con legacy: legacy hace `.astype(int)` antes de comparar, lo
    que trunca cualquier monto entre 0.01 y 0.99 a 0 (falso "No" para un
    cashback real pero menor a 1 unidad). Acá se compara el monto numérico
    tal cual (sin truncar) — corrige ese truncamiento en vez de replicarlo.
    Nulos se tratan como "sin cashback" (0.0).

    Args:
        condition_name: Columna de monto a evaluar (siempre `cashback` en
            la práctica, ver COLUMN_GROUP_YES_NO).
        condition_value: Valor de la condición tal como viene en
            `visa_rules`, "Yes" o "No".
        batch: Sub-batch de transacciones aún sin matchear para esta regla.

    Returns:
        Subconjunto de `batch` que cumple la condición.

    Ejemplo:
        _apply_yes_no("cashback", "No", batch)
    """
    amounts = pd.to_numeric(batch[condition_name], errors="coerce").fillna(0.0)
    if condition_value.strip().upper() == "NO":
        return batch[amounts == 0]
    return batch[amounts > 0]


def _apply_amount_currency(
    condition_name: str,
    condition_value: str,
    rule: pd.Series,
    batch: pd.DataFrame,
    rates_pd: pd.DataFrame
) -> pd.DataFrame:
    """
    Evalúa la condición de monto (`source_amount`) de una regla, convirtiendo
    el monto de la transacción a la moneda en que la regla expresa su umbral
    (`rule["source_currency_code_alphabetic"]`) antes de comparar — mismo
    propósito que `_add_converted_amount`, pero aplicado acá fila por fila
    dentro del engine pandas en vez de precalculado en Spark, porque el
    umbral depende de la regla actual, no de una lista fija de monedas.
    Soporta operador relacional (`>`,`<`,`>=`,`<=`,`=`) y `BETWEEN ... AND
    ...`, igual que `_apply_greater_less`.

    Nota de implementación: `pd.merge` resetea el índice del batch, así que
    se preserva el índice original como columna (`reset_index()`) antes del
    merge y se recupera al final (`batch.loc[matched_original_indices]`) —
    sin esto, `batch.loc[filter_df.index]` lanzaría `KeyError` porque los
    índices de `filter_df` (post-merge) no corresponden a los de `batch`.

    Args:
        condition_name: Columna de monto a evaluar (siempre `source_amount`
            en la práctica, ver COLUMN_GROUP_AMOUNT_CURRENCY).
        condition_value: Valor de la condición, ej. ">100", "BETWEEN 0 AND 50".
        rule: Fila de `visa_rules` (renombrada) con la regla actual —
            provee la moneda del umbral.
        batch: Sub-batch de transacciones aún sin matchear para esta regla.
        rates_pd: Tasas de cambio del día.

    Returns:
        Subconjunto de `batch` (con su índice original) que cumple la
        condición de monto ya convertido.

    Ejemplo:
        _apply_amount_currency("source_amount", ">100", rule, batch, rates_pd)
    """
    target_currency = str(rule.get("source_currency_code_alphabetic", "")).strip().upper()
    if not target_currency or target_currency in ("", "NAN", "NONE"):
        return batch

    target_rates = rates_pd[rates_pd["currency_to"].str.upper() == target_currency]

    batch_reset = batch.reset_index()

    filter_df = pd.merge(
        batch_reset,
        target_rates[["currency_from", "exchange_value"]],
        how="left",
        left_on="source_currency_code_alphabetic",
        right_on="currency_from"
    )

    filter_df.loc[
        filter_df["source_currency_code_alphabetic"].str.upper() == target_currency,
        "exchange_value"
    ] = 1.0
    filter_df["comparison_value"] = filter_df[condition_name] * filter_df["exchange_value"]

    if any(x in condition_value for x in ["<", ">", "="]):
        query = "comparison_value " + condition_value \
            .replace("<=", "<= ").replace(">=", ">= ") \
            .replace(">", "> ").replace("<", "< ")
        filter_df = filter_df.query(query)
    elif "BETWEEN" in condition_value.upper():
        lo, hi = map(float, condition_value.upper()
                     .replace("BETWEEN", "").strip().split("AND"))
        filter_df = filter_df[
            filter_df["comparison_value"].between(lo, hi, inclusive="both")
        ]

    matched_original_indices = filter_df["index"].tolist()
    return batch.loc[matched_original_indices]


def _apply_condition_pandas(
    condition_name: str,
    rule: pd.Series,
    batch: pd.DataFrame,
    rates_pd: pd.DataFrame
) -> pd.DataFrame:
    """
    Despacha la evaluación de una condición de regla al evaluador correcto
    según a qué grupo de columnas pertenece `condition_name`:
    COLUMN_GROUP_GREATER_LESS → `_apply_greater_less`,
    COLUMN_GROUP_AMOUNT_CURRENCY → `_apply_amount_currency`,
    COLUMN_GROUP_YES_NO → `_apply_yes_no`, cualquier otra columna →
    `_apply_default` (lista de valores). Condiciones vacías/NaN/"NONE" en la
    regla se consideran "sin restricción" y no filtran nada.

    Args:
        condition_name: Nombre de la condición/columna a evaluar.
        rule: Fila de `visa_rules` (renombrada) con el valor de la condición.
        batch: Sub-batch de transacciones aún sin matchear para esta regla.
        rates_pd: Tasas de cambio del día (solo usado si la condición es de
            monto).

    Returns:
        Subconjunto de `batch` que cumple la condición evaluada.

    Ejemplo:
        next_batch = _apply_condition_pandas("draft_code", rule, next_batch, rates_pd)
    """
    condition_value = str(rule[condition_name]).strip()
    if condition_value.upper() in ("", "NAN", "NONE"):
        return batch

    if condition_name in COLUMN_GROUP_GREATER_LESS:
        return _apply_greater_less(condition_name, condition_value, batch)
    elif condition_name in COLUMN_GROUP_AMOUNT_CURRENCY:
        return _apply_amount_currency(condition_name, condition_value, rule, batch, rates_pd)
    elif condition_name in COLUMN_GROUP_YES_NO:
        return _apply_yes_no(condition_name, condition_value, batch)
    else:
        return _apply_default(condition_name, condition_value, batch)


def _evaluate_rules_pandas(
    transactions_pd: pd.DataFrame,
    rules_pd: pd.DataFrame,
    rates_pd: pd.DataFrame
) -> pd.DataFrame:
    """
    Motor de reglas first-match-wins: para cada transacción, recorre las
    reglas de su misma jurisdicción (`region_country_code == jurisdiction_assigned`)
    en el orden en que vienen ordenadas (ya ordenadas por
    `load_visa_rules`) y le asigna los campos de la primera regla cuyas
    condiciones activas matchean por completo. Transacciones ya matcheadas
    (`interchange_intelica_id != -1`) se excluyen de reglas siguientes vía
    "early exit" — si ninguna transacción de la jurisdicción sigue sin
    matchear, se corta el loop de reglas para esa jurisdicción sin evaluar
    las restantes. Lógica idéntica a la del prototipo local ya validado.

    Llamada desde `mapInPandas` (ver `evaluate_interchange_fees`) — recibe un
    chunk/partición de transacciones, corre enteramente en el worker sin
    volver al driver.

    Args:
        transactions_pd: Chunk de transacciones (columnas CLN+CAL ya unidas,
            incluye `jurisdiction_assigned`).
        rules_pd: Reglas ya renombradas (salida de `_rename_rules`).
        rates_pd: Tasas de cambio del día, usadas por condiciones de monto.

    Returns:
        `transactions_pd` con las columnas `interchange_region_country_code`,
        `interchange_intelica_id` (-1 si ninguna regla matcheó),
        `interchange_fee_descriptor`, `interchange_fee_currency`,
        `interchange_fee_variable`, `interchange_fee_fixed`,
        `interchange_fee_min`, `interchange_fee_cap` agregadas/pobladas.

    Ejemplo:
        result_pdf = _evaluate_rules_pandas(pdf, local_rules, local_rates)
    """
    update_columns = [
        "region_country_code", "intelica_id", "fee_descriptor",
        "fee_currency", "fee_variable", "fee_fixed", "fee_min", "fee_cap"
    ]

    transactions_pd = transactions_pd.copy()
    transactions_pd["interchange_region_country_code"] = ""
    transactions_pd["interchange_intelica_id"] = -1
    transactions_pd["interchange_fee_descriptor"] = ""
    transactions_pd["interchange_fee_currency"] = ""
    transactions_pd["interchange_fee_variable"] = 0.0
    transactions_pd["interchange_fee_fixed"] = 0.0
    transactions_pd["interchange_fee_min"] = 0.0
    transactions_pd["interchange_fee_cap"] = 0.0

    # OPT 1: Pre-compilar condiciones activas por regla
    rules_compiled = []
    for _, rule in rules_pd.iterrows():
        active_conditions = [
            c for c in rule.index
            if c not in CONDITIONS_TO_SKIP
            and not pd.isna(rule[c])
            and str(rule[c]).strip().upper() not in ("", "NAN", "NONE")
        ]
        rules_compiled.append((rule, active_conditions))

    # OPT 2: Procesar por jurisdicción
    jurisdictions = transactions_pd["jurisdiction_assigned"].unique()

    for jurisdiction in jurisdictions:
        jur_mask = transactions_pd["jurisdiction_assigned"] == jurisdiction
        jur_indices = transactions_pd[jur_mask].index

        jur_rules = [
            (rule, conds) for rule, conds in rules_compiled
            if rule["region_country_code"] == jurisdiction
        ]

        if not jur_rules:
            continue

        for rule, active_conditions in jur_rules:

            # OPT 3: Early exit
            unmatched_mask = (
                transactions_pd.loc[jur_indices, "interchange_intelica_id"] == -1
            )
            if not unmatched_mask.any():
                break

            unmatched_indices = jur_indices[unmatched_mask]
            next_batch = transactions_pd.loc[unmatched_indices].copy()

            for condition in active_conditions:
                next_batch = _apply_condition_pandas(
                    condition, rule, next_batch, rates_pd
                )
                if next_batch.empty:
                    break

            if not next_batch.empty:
                for col in update_columns:
                    transactions_pd.loc[
                        next_batch.index, f"interchange_{col}"
                    ] = rule[col]

    return transactions_pd

# =============================================================================
# EVALUACIÓN PRINCIPAL (Spark I/O + mapInPandas distribuido)
# =============================================================================

def evaluate_interchange_fees(
    transactions: DataFrame,
    rules_pd: pd.DataFrame,
    rates_pd: pd.DataFrame,
    type_record: str
) -> DataFrame:
    """
    Punto de entrada del rule engine para un `type_record` (BASEII o SMS):
    prepara reglas y montos convertidos, y distribuye la evaluación real
    (`_evaluate_rules_pandas`) sobre los workers Spark vía `mapInPandas`.

    Arquitectura híbrida distribuida:
      - Spark para I/O y conversión de moneda
      - mapInPandas para ejecutar el rule engine en paralelo en los workers
        sin pasar por el driver → escala a cualquier volumen sin OOM

    Flujo visual:
      Driver: broadcast(rules, rates) → workers
      Worker 1: chunk_1 → _evaluate_rules_pandas → resultado_1  ┐
      Worker 2: chunk_2 → _evaluate_rules_pandas → resultado_2  ├ en paralelo
      Worker N: chunk_N → _evaluate_rules_pandas → resultado_N  ┘
      Spark ensambla los resultados sin pasar por el driver

    Args:
        transactions: DataFrame de transacciones (CLN+CAL ya unidos).
        rules_pd: Reglas ya filtradas por vigencia (salida de
            `load_visa_rules`), aún sin renombrar (se renombran acá adentro).
        rates_pd: Tasas de cambio del día.
        type_record: "draft" (BASEII) o "sms" — determina el renombrado de
            columnas de reglas (`_rename_rules`).

    Returns:
        DataFrame de Spark con las columnas de `OUTPUT_COLS` (identificación
        de la transacción + campos `interchange_*` asignados por el engine),
        listo para pasar a `calculate_fee_amounts`.

    Ejemplo:
        result = evaluate_interchange_fees(merged, rules_pd, rates_pd, "draft")
    """
    log_info(f"Evaluating interchange fees for {type_record} using mapInPandas...")

    # 1. Renombrar columnas de reglas
    rules_renamed = _rename_rules(rules_pd, type_record)

    # 2. Crear columnas de monto convertido en Spark
    if "source_amount" in [c.lower() for c in transactions.columns]:
        transactions = _add_converted_amount(transactions, rates_pd, rules_renamed)

    # 3. Broadcast de referencias a todos los workers
    # Las reglas (~7K filas) y tasas (~28K filas) son pequeñas → broadcast seguro
    log_info("  Broadcasting rules and rates to executors...")
    bc_rules = spark.sparkContext.broadcast(rules_renamed)
    bc_rates = spark.sparkContext.broadcast(rates_pd)

    # Las columnas que necesitamos para calculate_fee_amounts y el select final
    OUTPUT_COLS = [
        "content_hash", "record", "source_currency_code_alphabetic", "source_amount",
        "interchange_region_country_code", "interchange_intelica_id",
        "interchange_fee_descriptor", "interchange_fee_currency",
        "interchange_fee_variable", "interchange_fee_fixed",
        "interchange_fee_min", "interchange_fee_cap",
    ]

    def process_pandas_partitions(iterator):
        """
        Función de partición pasada a `mapInPandas`: por cada chunk pandas
        que Spark entrega en esta partición, corre el rule engine
        (`_evaluate_rules_pandas`) usando las reglas/tasas ya broadcast
        (`bc_rules`/`bc_rates`, sin volver a serializarlas por chunk), aplica
        un "blindaje de tipos" final (nulls/NaN → valores por defecto
        consistentes con `output_schema`) y yieldea solo las columnas de
        `OUTPUT_COLS` — nunca las ~252 columnas completas del input, para no
        inflar innecesariamente la salida serializada de vuelta a Spark.

        Args:
            iterator: Iterador de chunks pandas que Spark entrega para esta
                partición (contrato de `mapInPandas`).

        Returns:
            Generador de DataFrames pandas, cada uno con las columnas de
            `OUTPUT_COLS` y tipos ya coherentes con `output_schema`.
        """
        local_rules = bc_rules.value
        local_rates = bc_rates.value

        for pdf in iterator:
            if pdf.empty:
                continue

            result_pdf = _evaluate_rules_pandas(pdf, local_rules, local_rates)

            # Blindaje de tipos
            result_pdf["interchange_region_country_code"] = (
                result_pdf["interchange_region_country_code"].astype(str).replace("nan", "")
            )
            result_pdf["interchange_intelica_id"] = (
                result_pdf["interchange_intelica_id"].fillna(-1).astype(int)
            )
            result_pdf["interchange_fee_descriptor"] = (
                result_pdf["interchange_fee_descriptor"].astype(str).replace("nan", "")
            )
            result_pdf["interchange_fee_currency"] = (
                result_pdf["interchange_fee_currency"].astype(str).replace("nan", "")
            )
            result_pdf["interchange_fee_variable"] = (
                result_pdf["interchange_fee_variable"].fillna(0.0).astype(float)
            )
            result_pdf["interchange_fee_fixed"] = (
                result_pdf["interchange_fee_fixed"].fillna(0.0).astype(float)
            )
            result_pdf["interchange_fee_min"] = (
                result_pdf["interchange_fee_min"].astype(float)
            )
            result_pdf["interchange_fee_cap"] = (
                result_pdf["interchange_fee_cap"].astype(float)
            )

            yield result_pdf[OUTPUT_COLS]

    # Schema INDEPENDIENTE — solo describe lo que el iterador yields
    # No extender transactions.schema que causa el AnalysisException
    output_schema = StructType([
        StructField("content_hash",                    StringType(), True),
        StructField("record",                          LongType(),   True),
        StructField("source_currency_code_alphabetic", StringType(), True),
        StructField("source_amount",                   DoubleType(), True),
        StructField("interchange_region_country_code", StringType(), True),
        StructField("interchange_intelica_id",         IntegerType(), True),
        StructField("interchange_fee_descriptor",      StringType(), True),
        StructField("interchange_fee_currency",        StringType(), True),
        StructField("interchange_fee_variable",        DoubleType(), True),
        StructField("interchange_fee_fixed",           DoubleType(), True),
        StructField("interchange_fee_min",             DoubleType(), True),
        StructField("interchange_fee_cap",             DoubleType(), True),
    ])

    log_info("  Applying distributed rule engine (mapInPandas)...")
    result = transactions.mapInPandas(process_pandas_partitions, schema=output_schema)

    log_info("  Interchange evaluation complete.")
    return result

# =============================================================================
# CÁLCULO DE FEE AMOUNT
# =============================================================================

def calculate_fee_amounts(df: DataFrame, rates_pd: pd.DataFrame) -> DataFrame:
    """
    Calcula `interchange_fee_amount` en Spark a partir de los campos que
    `_evaluate_rules_pandas` ya asignó a cada transacción
    (interchange_fee_variable/fixed/min/cap, en la moneda de la regla), con
    la fórmula `fee_variable × source_amount + fee_fixed_convertido`, donde
    `fee_min`/`fee_cap` actúan como piso/techo del resultado. El detalle de
    en qué moneda queda expresado el fee y por qué el join de tasas va en
    esa dirección está documentado en el comentario inline justo antes del
    join, más abajo (decisión ya validada, ver `decisions.md` — "dirección
    del exchange_value").

    Args:
        df: Salida de `evaluate_interchange_fees` (con los campos
            `interchange_*` ya asignados por regla).
        rates_pd: Tasas de cambio del día.

    Returns:
        `df` con `interchange_fee_amount` calculado (columnas auxiliares
        `_fee_*`/`exchange_value` usadas para el cálculo se eliminan al
        final).

    Ejemplo:
        result = calculate_fee_amounts(result, rates_pd)
    """
    log_info("Calculating fee amounts...")
    rates_spark = spark.createDataFrame(
        rates_pd[["currency_from", "currency_to", "exchange_value"]]
    )

    # Join direction: (from=fee_ccy, to=source_ccy) → exchange_value = rate(fee_ccy → source_ccy)
    # Converts fee_fixed/fee_min/fee_cap from fee_ccy to source_ccy.
    # fee = fee_variable × source_amount_src_ccy + fee_fixed_fee_ccy × rate(fee_ccy→src_ccy)
    df = df.join(
        F.broadcast(rates_spark),
        (df["interchange_fee_currency"] == rates_spark["currency_from"]) &
        (df["source_currency_code_alphabetic"] == rates_spark["currency_to"]),
        how="left"
    ).withColumn(
        "exchange_value",
        F.when(
            F.col("source_currency_code_alphabetic") == F.col("interchange_fee_currency"),
            F.lit(1.0)
        ).otherwise(F.col("exchange_value"))
    ).drop("currency_from", "currency_to")

    df = df \
        .withColumn("_fee_fixed_src",
            F.coalesce(F.col("interchange_fee_fixed") * F.col("exchange_value"), F.lit(0.0))
        ) \
        .withColumn("_fee_min_src",
            F.coalesce(F.col("interchange_fee_min") * F.col("exchange_value"), F.lit(float("-inf")))
        ) \
        .withColumn("_fee_cap_src",
            F.coalesce(F.col("interchange_fee_cap") * F.col("exchange_value"), F.lit(float("inf")))
        ) \
        .withColumn("_fee_variable",
            F.coalesce(F.col("interchange_fee_variable"), F.lit(0.0))
        )

    df = df.withColumn(
        "interchange_fee_amount",
        F.col("source_amount") * F.col("_fee_variable") + F.col("_fee_fixed_src")
    ).withColumn(
        "interchange_fee_amount",
        F.greatest(F.col("interchange_fee_amount"), F.col("_fee_min_src"))
    ).withColumn(
        "interchange_fee_amount",
        F.least(F.col("interchange_fee_amount"), F.col("_fee_cap_src"))
    )

    df = df.drop(
        "_fee_fixed_src", "_fee_min_src", "_fee_cap_src",
        "_fee_variable", "exchange_value"
    )
    log_info("  Fee amounts calculated.")
    return df

# =============================================================================
# PROCESAR UN OUTPUT (BASEII o SMS)
# =============================================================================

def process_output(
    output_config: dict, staging_bucket: str, type_record: str,
    rules_pd: pd.DataFrame, rates_pd: pd.DataFrame, client_data: dict
) -> dict:
    """
    Procesa un único output (BASEII o SMS) de principio a fin: deriva las
    rutas S3 de CAL e ITX a partir de la ruta CLN recibida (mismo patrón de
    prefijos `NNN_..._XXX` usado en todo el pipeline — `300_..._cln_` →
    `400_..._cal_`/`500_..._itx_`), une CLN+CAL por `record` (las columnas de
    CAL, derivadas de ARDEF, tienen prioridad sobre las de CLN cuando hay
    solapamiento — las de CLN se renombran con sufijo `_cln` para no perder
    el dato original), evalúa las reglas de interchange y calcula el fee, y
    escribe el resultado en la capa ITX.

    `source_amount`/`source_currency_code_alphabetic` se usan durante el
    cálculo pero se excluyen del output final — ya existen en CLN y
    `lmbd-vi-store` los toma de ahí al consolidar (mismo criterio que el
    prototipo local: `stage.drop([...])`).

    Args:
        output_config: Dict con `output_type` ("BASEII"/"SMS") y `s3_key`
            (ruta CLN de este output dentro de `staging_bucket`).
        staging_bucket: Bucket S3 de staging.
        type_record: "draft" (BASEII) o "sms" — pasado a
            `evaluate_interchange_fees`.
        rules_pd: Reglas ya filtradas por vigencia (sin renombrar todavía).
        rates_pd: Tasas de cambio del día.
        client_data: Reservado para datos de cliente (no usado actualmente
            en este job — Visa no necesita BINes propios del cliente para
            interchange, a diferencia de otras etapas).

    Returns:
        Dict con `status`, `output_type`, `s3_key` (ruta ITX escrita) y
        `records` (conteo final).

    Ejemplo:
        result = process_output(output_config, staging_bucket, "draft", rules_pd, rates_pd, {})
    """
    output_type = output_config.get("output_type", "")

    base_s3_key = output_config.get("s3_key", "")
    if not base_s3_key:
        raise ValueError(f"No s3_key in output_config for {output_type}")

    cln_s3_key = base_s3_key
    cal_s3_key = base_s3_key.replace("/300_", "/400_").replace("_cln_", "_cal_")
    itx_s3_key = base_s3_key.replace("/300_", "/500_").replace("_cln_", "_itx_")

    cln_path = f"s3://{staging_bucket}/{cln_s3_key}"
    cal_path = f"s3://{staging_bucket}/{cal_s3_key}"
    itx_path = f"s3://{staging_bucket}/{itx_s3_key}"

    log_info(f"Processing {output_type}")
    log_info(f"  CLN: {cln_path}")
    log_info(f"  CAL: {cal_path}")
    log_info(f"  ITX: {itx_path}")

    cln_df = load_parquet(cln_path)
    cal_df = load_parquet(cal_path)

    log_info("  Joining CLN + CAL...")
    key_cols = {"record", "content_hash"}
    overlap = {c for c in cln_df.columns if c in set(cal_df.columns) - key_cols}
    if overlap:
        for col in overlap:
            cln_df = cln_df.withColumnRenamed(col, col + "_cln")
        log_info(f"  Renamed {len(overlap)} CLN columns with _cln suffix: {sorted(overlap)[:10]}...")
    cal_cols_to_add = [c for c in cal_df.columns if c not in cln_df.columns or c == "record"]
    merged = cln_df.join(cal_df.select(cal_cols_to_add), on="record", how="left")
    log_info(f"  Merged: {merged.count():,} records, {len(merged.columns)} columns")

    result = evaluate_interchange_fees(merged, rules_pd, rates_pd, type_record)

    result = calculate_fee_amounts(result, rates_pd)

    interchange_cols = [
        "content_hash", "record",
        "interchange_intelica_id", "interchange_fee_descriptor", "interchange_fee_currency",
        "interchange_fee_variable", "interchange_fee_fixed", "interchange_fee_min",
        "interchange_fee_cap", "interchange_fee_amount",
    ]
    existing_cols = [c for c in interchange_cols if c in result.columns]
    result = result.select(existing_cols)

    result = result.cache()
    record_count = result.count()
    save_parquet(result, itx_path)
    result.unpersist()

    log_info(f"  ✓ {output_type}: {record_count:,} records → {itx_path}")
    return {
        "status": "SUCCESS",
        "output_type": output_type,
        "s3_key": itx_s3_key,
        "records": record_count
    }

# =============================================================================
# MAIN
# =============================================================================

def main():
    """
    Entry point del job. Resuelve los parámetros del Glue job, carga las
    tablas de referencia una sola vez (`visa_rules`, tasas de cambio del día),
    y procesa cada output declarado en `--outputs`: BASEII/SMS se pasan por
    el rule engine completo (`process_output`); VSS se descarta (sin reglas
    de interchange propias, ver docstring de módulo). Loguea progreso y
    hace `job.commit()` al final para que Glue marque la ejecución como
    completada.

    Returns:
        Dict con `status`, `total_outputs`, `total_records` y el detalle de
        cada output procesado (mismo shape que devuelve `process_output`,
        agregado en una lista) — usado para logging, no consumido por
        Step Functions (el job no tiene salida estructurada hacia afuera,
        solo side-effects en S3).
    """
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME", "client_id", "file_id", "file_type", "file_date",
        "staging_bucket", "reference_bucket", "outputs"
    ])

    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    client_id        = args["client_id"]
    file_id          = args["file_id"]
    file_type        = args["file_type"]
    file_date        = args["file_date"]
    staging_bucket   = args["staging_bucket"]
    reference_bucket = args["reference_bucket"]
    outputs          = json.loads(args["outputs"])

    log_info("=" * 70)
    log_info("ITX-INTERCHANGE (PySpark) - STARTING")
    log_info("=" * 70)
    log_info(f"Client ID:   {client_id}")
    log_info(f"File ID:     {file_id}")
    log_info(f"File Type:   {file_type}")
    log_info(f"File Date:   {file_date}")
    log_info(f"Outputs:     {len(outputs)}")
    log_info("=" * 70)

    try:
        file_date_obj = datetime.strptime(file_date, "%Y-%m-%d").date()
    except ValueError:
        file_date_obj = date.today()

    log_info("Loading reference tables...")
    rules_pd = load_visa_rules(reference_bucket, file_date_obj)
    rates_pd = load_exchange_rates(reference_bucket, file_date_obj, brand="VISA")

    results = []
    total_records = 0

    for output_config in outputs:
        output_type = output_config.get("output_type", "UNKNOWN")
        log_info("")
        log_info("=" * 60)
        log_info(f"Processing: {output_type}")
        log_info("=" * 60)

        if output_type == "BASEII":
            type_record = "draft"
        elif output_type == "SMS":
            type_record = "sms"
        else:
            log_info(f"  Skipping {output_type} — no interchange for VSS")
            continue

        result = process_output(
            output_config=output_config,
            staging_bucket=staging_bucket,
            type_record=type_record,
            rules_pd=rules_pd,
            rates_pd=rates_pd,
            client_data={}
        )
        results.append(result)
        total_records += result.get("records", 0)

    log_info("")
    log_info("=" * 70)
    log_info("INTERCHANGE PROCESS COMPLETED")
    log_info("=" * 70)
    log_info(f"Total outputs:  {len(results)}")
    log_info(f"Total records:  {total_records:,}")

    output_data = {
        "status": "SUCCESS",
        "total_outputs": len(results),
        "total_records": total_records,
        "outputs": results
    }

    log_info(f"Output: {json.dumps(output_data)}")
    job.commit()
    return output_data


if __name__ == "__main__":
    main()