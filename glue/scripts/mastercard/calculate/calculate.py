# =============================================================================
# calculate.py — AWS Glue Job: Mastercard IPM Calculate
# =============================================================================
# Glue Job: itl-0004-itx-dev-intchg-02-glue-mc-calculate
# Glue 4.0 | Spark 3.3 | Python 3 | Worker G.1X x2
#
# Adapta mc_calculate.py para AWS Glue, reemplazando:
#   - SQLite  (Database)       →  DynamoDB  (boto3)
#   - Filesystem (FileStorage) →  S3        (Spark + boto3)
#   - DuckDB enrichers         →  PySpark / Spark SQL
#
# Lógica de negocio preservada al 100%:
#   PASO 2+3+4  →  calculate_pre2()          (range-join IAR con bucket-prefix)
#   PASO 5      →  calculate_ex_rate()        (exchange rates desde S3 Hive)
#   PASO 7      →  calculate_settlement_report()
#   PASO FINAL  →  calculate_final_fields()   (ensamble + jurisdiction_assigned)
#   EXCLUDE     →  build_lookup_691_spark() + apply_exclude_flag()
#
# Job Parameters (siempre presentes):
#   --S3_REFERENCE         s3://itl-0004-itx-dev-intchg-02-s3-reference
#   --S3_STAGING           s3://itl-0004-itx-dev-intchg-02-s3-staging
#
# Job Parameters (pasados por el orquestador en cada ejecución):
#   --client_id            ID del cliente  (ej: "CLIENT01")
#   --file_id              ID del archivo  (ej: "ABC123XYZ...")
#   --file_type            IN | OUT
#   --file_date            YYYY-MM-DD  (fecha del archivo, para IAR y exchange_rate)
#   --outputs              JSON: [{"mti":"1240","s3_key":"staging/…"}, …]
#   --dynamodb_table_client  tabla DynamoDB de clientes
#   --s3_key_1644_cln      path en staging del folder 400_IPM_1644_CLN  (para lookup 691)
#
# Estructura S3 esperada:
#   [S3_REFERENCE]/country/data.parquet
#   [S3_REFERENCE]/region/data.parquet
#   [S3_REFERENCE]/currency/data.parquet
#   [S3_REFERENCE]/mastercard_brand_product/data.parquet
#   [S3_REFERENCE]/mastercard_iar/historic_data.parquet   ← PROVISIONAL
#   [S3_REFERENCE]/exchange_rate/rate_date=YYYY-MM-DD/*.parquet
#
#   [S3_STAGING]/{s3_key_input}/…_1240.parquet            ← CLN input
#   [S3_STAGING]/{s3_key_output}/…_1240.parquet           ← CAL output
# =============================================================================

from __future__ import annotations
 
import sys
import json
import re
from datetime import datetime, date
from typing import Optional
 
import numpy as np
import boto3
import pandas as pd
 
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
 
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StringType, LongType, DoubleType, DecimalType,
    DateType, IntegerType, StructType, StructField,
)
 
# =============================================================================
# SCHEMA CLN — construido dinámicamente desde DynamoDB
# =============================================================================
 
# Campos de metadatos del pipeline (no existen en la tabla DynamoDB de campos).
# Van siempre al principio del schema.
_CLN_META_FIELDS = [
    StructField("file_idn",               StringType(), True),
    StructField("file_dt",                StringType(), True),
    StructField("type_mti",               StringType(), True),
    StructField("ref_id",                 LongType(),   True),
    StructField("function_code",          LongType(),   True),
    StructField("file_id",                StringType(), True),
    StructField("file_processing_date",   StringType(), True),
]
 
_DECIMAL_SCALE_OVERRIDE: dict[str, tuple[int, int]] = {
    "conversion_rate_reconciliation_de_9":      (18, 9),
    "conversion_rate_cardholder_billing_de_10": (18, 9),
    "amounts_transaction_fee_1_pds_146_1":      (18, 2),
    "amounts_transaction_fee_2_pds_146_2":      (18, 2),
    "amounts_transaction_fee_3_pds_146_3":      (18, 2),
}


# Campo especial: TIMESTAMP_NS no soportado en Spark 3.3 → siempre LongType.
_TIMESTAMP_NS_AS_LONG = "date_and_time_local_transaction_de_12"
 
def _dynamo_type_to_spark(col_name: str, data_type: str, float_decimals: str):
    """
    Convierte el data_type de DynamoDB al tipo PySpark correspondiente.
    Casos especiales:
      - 'timestamp' → LongType  (TIMESTAMP_NS no soportado en Spark 3.3)
      - 'decimal'   → DecimalType(18, scale) usando override o float_decimals positivo
      - 'time'      → StringType (se almacena como cadena HH:MM:SS)
      - 'date'      → DateType
      - 'int64'     → LongType
      - 'string'    → StringType
    """
    dt = data_type.strip().lower()
 
    if col_name == _TIMESTAMP_NS_AS_LONG:
        return LongType()  # TIMESTAMP_NS → LongType (hardcodeado)
 
    if dt == "timestamp":
        return LongType()  # cualquier otro timestamp también → LongType por seguridad
 
    if dt == "decimal":
        if col_name in _DECIMAL_SCALE_OVERRIDE:
            p, s = _DECIMAL_SCALE_OVERRIDE[col_name]
            return DecimalType(p, s)
        try:
            scale = int(float_decimals.strip().lstrip("'"))
            if scale >= 0:
                return DecimalType(18, scale)
        except (ValueError, AttributeError):
            pass
        return DecimalType(18, 4)  # fallback genérico
 
    if dt == "int64":    return LongType()
    if dt == "date":     return DateType()
    if dt in ("string", "time"):
        return StringType()
 
    # Tipo desconocido → StringType con warning
    return StringType()

def build_cln_schema_from_dynamodb(dynamo_table_fields: str, mti: str) -> StructType:
    """
    Consulta la tabla DynamoDB de campos Mastercard y construye el StructType
    para leer los parquets CLN del MTI indicado.
 
    - Los campos de metadatos del pipeline van hardcodeados al inicio.
    - 'date_and_time_local_transaction_de_12' siempre se mapea a LongType.
    - El campo 'date' (partición Hive) se añade al final hardcodeado.
 
    Args:
        dynamo_table_fields: nombre de la tabla DynamoDB (ej: itl-0004-...-mastercard_fields-02)
        mti: '1240' o '1442'
    """
    log_info(f"  Building CLN schema from DynamoDB table: {dynamo_table_fields} (MTI={mti})")
 
    dynamodb = boto3.resource("dynamodb")
    table    = dynamodb.Table(dynamo_table_fields)
 
    from boto3.dynamodb.conditions import Attr
    response = table.scan(
        FilterExpression=Attr("type_mti").contains(mti)
    )
    items = response.get("Items", [])
    while "LastEvaluatedKey" in response:
        response = table.scan(
            FilterExpression=Attr("type_mti").contains(mti),
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        items.extend(response.get("Items", []))
 
    log_info(f"  DynamoDB returned {len(items)} field definitions for MTI {mti}")
 
    # Metadata fields primero (no están en DynamoDB)
    fields: list[StructField] = list(_CLN_META_FIELDS)
 
    # Campos del dominio desde DynamoDB — orden no importa, Spark hace match por nombre
    for item in items:
        col   = item.get("column_name", "").strip()
        dtype = item.get("data_type", "string")
        scale = str(item.get("float_decimals", ""))
        if col:
            fields.append(StructField(col, _dynamo_type_to_spark(col, dtype, scale), True))
 
    # Campo 'date' de partición Hive — siempre al final
    fields.append(StructField("date", DateType(), True))
 
    log_info(f"  CLN schema built: {len(fields)} fields")
    return StructType(fields)
 
# =============================================================================
# 1. SPARK / GLUE INITIALIZATION
# =============================================================================
 
spark = (
    SparkSession.builder
    .config("spark.sql.parquet.int96RebaseModeInRead",     "CORRECTED")
    .config("spark.sql.parquet.int96RebaseModeInWrite",    "CORRECTED")
    .config("spark.sql.parquet.datetimeRebaseModeInRead",  "CORRECTED")
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    .config("spark.sql.parquet.outputTimestampType",       "TIMESTAMP_MICROS")
    .config("spark.sql.broadcastTimeout",                  "600")
    .config("spark.sql.adaptive.enabled",                  "true")
    # Arrow-based pandas→Spark conversion: bypasses cloudpickle (incompatible w/ Python 3.11)
    .config("spark.sql.execution.arrow.pyspark.enabled",   "true")
    # CLN parquets contienen TIMESTAMP(NANOS,false) (ej: date_and_time_local_transaction_de_12).
    # PySpark 3.3 no soporta ese subtipo al leer → AnalysisException: Illegal Parquet type.
    # nanosAsLong=true las lee como LongType (ns desde epoch) sin error.
    # Aplica igual en AWS Glue 4.0 (Spark 3.3).
    .config("spark.sql.legacy.parquet.nanosAsLong",              "true")
    .getOrCreate()
)
spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")
 
glueContext  = GlueContext(spark.sparkContext)
_glue_logger = glueContext.get_logger()


# =============================================================================
# 2. LOGGING HELPERS
# =============================================================================
 
def log_info(msg: str)  -> None: _glue_logger.info(f"[MC-CALC] {msg}")
def log_warn(msg: str)  -> None: _glue_logger.warn(f"[MC-CALC] WARNING — {msg}")
def log_error(msg: str) -> None: _glue_logger.error(f"[MC-CALC] ERROR — {msg}")
 
 
# =============================================================================
# 3. S3 PATH HELPERS
# =============================================================================
 
def _parse_s3_url(s3_url: str) -> tuple[str, str]:
    """
    Normaliza una URL S3.
    Acepta: 's3://bucket/optional/prefix'  o  'bucket/optional/prefix'
    Devuelve: (bucket, prefix_sin_slash_inicial_ni_final)
    """
    url = s3_url.strip()
    if url.startswith("s3://"):
        url = url[5:]
    parts = url.split("/", 1)
    bucket = parts[0]
    prefix = parts[1].strip("/") if len(parts) > 1 else ""
    return bucket, prefix
 
 
def _s3_url(base: str, *parts: str) -> str:
    """
    Une partes en una URL S3 completa.
    Ej: _s3_url("s3://bucket", "folder", "file.parquet")
        → "s3://bucket/folder/file.parquet"
    """
    root = base.rstrip("/")
    tail = "/".join(p.strip("/") for p in parts if p)
    return f"{root}/{tail}" if tail else root
 
 
def list_s3_parquets(bucket: str, prefix: str) -> list[str]:
    """Lista todas las S3 keys (.parquet) bajo un prefix."""
    s3        = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    return keys


# =============================================================================
# 4. DYNAMODB HELPERS
# =============================================================================
 
def get_client_data(client_id: str, dynamo_table_name: str) -> dict:
    """
    Lee los datos del cliente desde DynamoDB.
    Replica: db.read_sql("SELECT … FROM client WHERE client_id = ?")
 
    Campos recuperados:
        client_id, local_currency_code, settlement_currency_code,
        acquiring_bins, issuing_bins_6_digits, issuing_bins_8_digits
    """
    dynamodb = boto3.resource("dynamodb")
    table    = dynamodb.Table(dynamo_table_name)
 
    for key_val in [client_id, client_id.upper(), client_id.lower()]:
        response = table.get_item(Key={"client_id": key_val})
        if "Item" in response:
            item = response["Item"]
            log_info(f"  Client found: {item.get('client_name', client_id)}")
            return {
                "client_id":                key_val,
                "client_name":              item.get("client_name", ""),
                "local_currency_code":      item.get("local_currency_code", ""),
                "settlement_currency_code": item.get("settlement_currency_code", ""),
                "acquiring_bins":           item.get("acquiring_bins", ""),
                "issuing_bins_6_digits":    item.get("issuing_bins_6_digits", ""),
                "issuing_bins_8_digits":    item.get("issuing_bins_8_digits", ""),
                "customer_country":         item.get("customer_country", ""),
            }
 
    raise ValueError(f"[get_client_data] Client not found in DynamoDB: {client_id!r}")


# =============================================================================
# 5. REFERENCE DATA LOADERS  (S3 → Spark DataFrame)
# =============================================================================
 
def _load_parquet(path: str) -> DataFrame:
    """Lee un parquet desde S3 y logea el path."""
    log_info(f"  Reading: {path}")
    return spark.read.parquet(path)
 
 
def load_country(s3_reference_url: str) -> DataFrame:
    """
    Maestra de países.
    Columnas usadas: country_code_alternative, country_code, mastercard_region_code.
    """
    return _load_parquet(_s3_url(s3_reference_url, "country", "data.parquet")).select(
        F.col("country_code_alternative").cast(StringType()),
        F.col("country_code").cast(StringType()),
        F.col("mastercard_region_code").cast(LongType()),
    )
 
 
def load_region(s3_reference_url: str) -> DataFrame:
    """
    Maestra de regiones.
    Columnas usadas: region_code.
    """
    return _load_parquet(_s3_url(s3_reference_url, "region", "data.parquet")).select(
        F.col("region_code").cast(LongType()),
    )
 
 
def load_currency(s3_reference_url: str) -> DataFrame:
    """
    Maestra de monedas.
    Columnas usadas: currency_numeric_code, currency_alphabetic_code.
    """
    return _load_parquet(_s3_url(s3_reference_url, "currency", "data.parquet")).select(
        F.col("currency_numeric_code").cast(LongType()),
        F.col("currency_alphabetic_code").cast(StringType()),
    )
 
 
def load_exchange_rate(s3_reference_url: str, rate_date: str) -> DataFrame:
    """
    Carga tasas de cambio desde la partición Hive correspondiente a rate_date.
    Estructura S3: exchange_rate/rate_date=YYYY-MM-DD/<hash>.parquet
    Inyecta la columna 'rate_date' (Date) si no la detecta Spark automáticamente.
 
    Fallback: lee todas las particiones y filtra por fecha.
    """
    partition_path = _s3_url(s3_reference_url, "exchange_rate", f"rate_date={rate_date}")
    log_info(f"  Loading exchange rate partition: {partition_path}")
    try:
        df = spark.read.parquet(partition_path)
        if "rate_date" not in df.columns:
            df = df.withColumn("rate_date", F.lit(rate_date).cast(DateType()))
        else:
            df = df.withColumn("rate_date", F.col("rate_date").cast(DateType()))
        return df
    except Exception as ex:
        log_warn(f"  Partition not found ({ex}). Fallback: reading all exchange_rate partitions.")
        base_path = _s3_url(s3_reference_url, "exchange_rate")
        df = spark.read.parquet(base_path)
        if "rate_date" not in df.columns:
            raise RuntimeError(
                f"[load_exchange_rate] 'rate_date' column missing after fallback read from {base_path}"
            )
        return df.filter(F.col("rate_date").cast(StringType()) == rate_date)
 
 
def _load_iar_raw(s3_reference_url: str) -> DataFrame:
    """
    Carga el parquet IAR crudo.
    PROVISIONAL: usa historic_data.parquet.
    En el futuro, usar data.parquet cuando esté completo.
    """
    try:
        path = _s3_url(s3_reference_url, "mastercard_iar", "historic_data.parquet")
        df   = _load_parquet(path)
        log_info("  IAR: loaded from historic_data.parquet (provisional)")
        return df
    except Exception:
        log_warn("  IAR: historic_data.parquet not found — fallback to data.parquet")
        return _load_parquet(_s3_url(s3_reference_url, "mastercard_iar", "data.parquet"))
 
 
def _load_brand_product_raw(s3_reference_url: str) -> DataFrame:
    """Carga el parquet de Mastercard Brand Product."""
    return _load_parquet(_s3_url(s3_reference_url, "mastercard_brand_product", "data.parquet"))


# =============================================================================
# 6. DATE HELPERS
# =============================================================================
 
def _parse_file_date(file_date_str: str) -> date:
    """
    Parsea file_date desde 'YYYY-MM-DD', 'YYYYMMDD' o 'YYMMDD'.
    Replica _file_dt_to_rate_date() de mc_calculate.py.
    """
    raw = str(file_date_str).strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            pass
    if raw.isdigit() and len(raw) == 6:
        try:
            return datetime.strptime(raw, "%y%m%d").date()
        except ValueError:
            pass
    raise ValueError(f"[_parse_file_date] Cannot parse: {file_date_str!r}")
 
 
def _to_rate_date(file_date_str: str) -> str:
    """Devuelve la fecha en formato 'YYYY-MM-DD' para el lookup de exchange_rate."""
    return _parse_file_date(file_date_str).strftime("%Y-%m-%d")


# =============================================================================
# 7. IAR PREPARATION  (replica calculate_iar_unique de mc_calculate.py)
# =============================================================================
 
def prepare_iar(s3_reference_url: str, file_date_str: str) -> DataFrame:
    """
    Prepara la maestra IAR filtrada y deduplicada para una fecha de archivo.
 
    Replica al 100% calculate_iar_unique():
      - Carga mastercard_iar + mastercard_brand_product
      - Renombra columnas al vocabulario interno
      - Filtros: active_inactive_code='A'  AND  app_date_valid <= file_date
      - LEFT JOIN con brand_product por licensed_product_id
      - Deduplicación: PARTITION BY low_key_for_range
                       ORDER BY app_date_valid DESC, card_program_priority ASC
    
    Se procesa en pandas en el driver (dataset pequeño).
    Retorna Spark DataFrame cacheado para múltiples joins posteriores.
 
    Columnas de salida:
        app_date_valid (str YYYY-MM-DD), low_key_for_range, high_key_for_range,
        iar_country, gcms_product_identifier, funding_source, card_program_identifier
    """
    file_dt_norm = _parse_file_date(file_date_str).strftime("%Y-%m-%d")
    log_info(f"[prepare_iar] date={file_dt_norm}")
 
    # ── 1. IAR crudo — leído directamente con pandas (evita OOM de toPandas) ──
    # _load_iar_raw devuelve un Spark DataFrame diseñado para Glue/S3.
    # En prepare_iar todo el procesamiento es pandas, así que leemos el parquet
    # directamente con pd.read_parquet (funciona tanto en local como en Glue)
    try:
        iar_path = _s3_url(s3_reference_url, "mastercard_iar", "historic_data.parquet")
        df_iar = pd.read_parquet(iar_path)
        log_info("  IAR: loaded via pd.read_parquet from historic_data.parquet (provisional)")
    except Exception:
        log_warn("  IAR: historic_data.parquet not found — fallback to data.parquet")
        iar_path = _s3_url(s3_reference_url, "mastercard_iar", "data.parquet")
        df_iar = pd.read_parquet(iar_path)
 
    df_iar = df_iar.rename(columns={
        "low_range":          "low_key_for_range",
        "high_range":         "high_key_for_range",
        "card_country_alpha": "iar_country",
    })
 
    df_iar["app_date_valid"] = pd.to_datetime(df_iar["app_date_valid"], errors="coerce")
 
    for rng_col in ("low_key_for_range", "high_key_for_range"):
        if rng_col in df_iar.columns:
            df_iar[rng_col] = pd.to_numeric(
                df_iar[rng_col].astype(str).str.strip().str[:18],
                errors="coerce",
            )
 
    if "card_program_priority" in df_iar.columns:
        df_iar["card_program_priority"] = pd.to_numeric(
            df_iar["card_program_priority"], errors="coerce"
        )
 
    # ── 2. Filtros ────────────────────────────────────────────────────────────
    if "active_inactive_code" in df_iar.columns:
        df_iar = df_iar[
            df_iar["active_inactive_code"].astype(str).str.strip() == "A"
        ].copy()
 
    df_iar = df_iar[
        df_iar["app_date_valid"].notna() &
        (df_iar["app_date_valid"].dt.strftime("%Y-%m-%d") <= file_dt_norm)
    ].copy()
 
    # ── 3. Brand Product — leído directamente con pandas ────────────────────
    bp_path = _s3_url(s3_reference_url, "mastercard_brand_product", "data.parquet")
    df_bp = pd.read_parquet(bp_path)
 
    if "active_inactive_code" in df_bp.columns:
        df_bp = df_bp[
            df_bp["active_inactive_code"].astype(str).str.strip() == "A"
        ].copy()
 
    df_bp = df_bp.rename(columns={
        "gcms_product_id":  "gcms_product_identifier",
        "product_category": "funding_source",
    })
    bp_cols = [c for c in ["licensed_product_id", "gcms_product_identifier", "funding_source"]
               if c in df_bp.columns]
    df_bp = df_bp[bp_cols].copy()
 
    # ── 4. LEFT JOIN IAR + BrandProduct ───────────────────────────────────────
    if "licensed_product_id" in df_iar.columns and "licensed_product_id" in df_bp.columns:
        df = df_iar.merge(df_bp, on="licensed_product_id", how="left")
    else:
        log_warn("[prepare_iar] licensed_product_id missing — skipping brand_product join")
        df = df_iar.copy()
 
    # ── 5. Deduplicación ──────────────────────────────────────────────────────
    sort_cols = ["app_date_valid"]
    sort_asc  = [False]
    if "card_program_priority" in df.columns:
        sort_cols.append("card_program_priority")
        sort_asc.append(True)
 
    df = (
        df
        .sort_values(sort_cols, ascending=sort_asc)
        .drop_duplicates(subset=["low_key_for_range"], keep="first")
        .reset_index(drop=True)
    )
 
    # ── 6. Garantizar columnas de salida ──────────────────────────────────────
    required = [
        "app_date_valid", "low_key_for_range", "high_key_for_range",
        "iar_country", "gcms_product_identifier", "funding_source",
        "card_program_identifier",
    ]
    for col in required:
        if col not in df.columns:
            log_warn(f"[prepare_iar] Column '{col}' not found — setting to None")
            df[col] = None
 
    df = df[required].copy()
    df["app_date_valid"] = df["app_date_valid"].dt.strftime("%Y-%m-%d")
 
    log_info(f"  IAR prepared: {len(df):,} ranges for date {file_dt_norm}")
 
    # ── 7. Spark DataFrame + cache ────────────────────────────────────────────
    # Python 3.11 + PySpark 3.3: cloudpickle bundled no soporta el nuevo bytecode
    # de Python 3.11 → IndexError en _walk_global_ops al serializar pandas DataFrames.
    # Solución: habilitar Arrow (spark.sql.execution.arrow.pyspark.enabled=true en
    # SparkSession) + proveer schema explícito → Arrow IPC bypasea cloudpickle por completo.
    #
    # Requisitos para Arrow:
    #   - Columnas numéricas deben ser int64 / Int64 (nullable) — no float64 con NaN
    #   - Columnas string deben ser object con None (no np.nan) para mapear a null en Arrow
    # NOTA: la nueva maestra historic_data.parquet almacena low_range / high_range como
    # VARCHAR, por lo que tras pd.to_numeric los valores quedan en float64 con posibles
    # NaN. El cast directo float64 → Int64 falla con la regla 'safe' de numpy
    # (TypeError: cannot safely cast non-equivalent float64 to int64).
    # Solución: construir el IntegerArray nullable manualmente vía numpy, evitando
    # la ruta _safe_cast de pandas.
    for num_col in ("low_key_for_range", "high_key_for_range"):
        _s    = pd.to_numeric(df[num_col], errors="coerce")
        _mask = _s.isna().to_numpy()
        _ints = np.where(_mask, 0, np.round(_s.to_numpy())).astype(np.int64)
        df[num_col] = pd.arrays.IntegerArray(_ints, mask=_mask)
 
    log_info(f"Cast low_key_for_range and high_key_for_range: Ok")
 
    str_cols = ["app_date_valid", "iar_country", "gcms_product_identifier",
                "funding_source", "card_program_identifier"]
    
    for sc in str_cols:
        df[sc] = df[sc].where(df[sc].notna(), other=None)
 
    _IAR_SCHEMA = StructType([
        StructField("app_date_valid",          StringType(), True),
        StructField("low_key_for_range",       LongType(),   True),
        StructField("high_key_for_range",      LongType(),   True),
        StructField("iar_country",             StringType(), True),
        StructField("gcms_product_identifier", StringType(), True),
        StructField("funding_source",          StringType(), True),
        StructField("card_program_identifier", StringType(), True),
    ])
 
    iar_spark = spark.createDataFrame(df, schema=_IAR_SCHEMA)
    iar_spark = iar_spark.cache()
    log_info(f"  IAR Spark DataFrame cached ({iar_spark.count():,} rows)")
    return iar_spark


# =============================================================================
# 8. CALCULATE PRE2  (PASOS 2 + 3 + 4)
# =============================================================================
 
def calculate_pre2(
    df_cln: DataFrame,
    df_iar: DataFrame,
    country_df: DataFrame,
    region_df: DataFrame,
    client_data: dict,
    file_id: str,
) -> DataFrame:
    """
    Replica calculate_pre2_duckdb() en PySpark.
 
    PASO 2: Extrae campos clave del CLN (settlement_indicator, purchase_date,
            card_purchase_country, acquirer_bin, iss_bin_6/8, num_card_low/high).
    PASO 3: Range-join optimizado con IAR (bucket-prefix) + JOINs con country/region.
            Calcula business_mode y jurisdiction.
    PASO 4: row_number() por (ref_id, file_id, file_idn) → toma n=1.
 
    Range-join optimization (igual que vi_calculate.py para ARDEF):
        - num_card_low  = pan_prefix9 * 10^9
        - num_card_high = pan_prefix9 * 10^9 + 999_999_999
        - join_prefix (transacción) = pan_prefix9 // 10^6   (primeros 3 dígitos)
        - Para IAR: prefix_low  = low_key_for_range // 10^15
                    prefix_high = high_key_for_range // 10^15
        - EXPLODE IAR en [prefix_low, prefix_high] → equi-join + filtro de rango
    """
    log_info(f"[calculate_pre2] file_id={file_id}")
 
    # ── Client BINs ───────────────────────────────────────────────────────────
    def _split_bins(val: str) -> list[str]:
        return [b.strip() for b in str(val).split(",") if b.strip()]
 
    issuing_bins_6 = _split_bins(client_data.get("issuing_bins_6_digits", ""))
    issuing_bins_8 = _split_bins(client_data.get("issuing_bins_8_digits", ""))
    acquiring_bins = _split_bins(client_data.get("acquiring_bins", ""))
 
    # ── PASO 2: campos base del CLN ───────────────────────────────────────────
    df = df_cln.select(
        F.col("ref_id"),
        F.col("file_id"),
        F.col("file_idn"),
        F.col("file_type"),
        F.col("type_mti"),
        F.col("file_dt"),
        F.col("settlement_indicator_1_pds_165_1").alias("settlement_indicator"),
        F.col("pan_de_2").cast(StringType()).alias("pan"),
        F.col("acquirer_reference_data_de_31").cast(StringType()).alias("acq_ref"),
        F.col("date_and_time_local_transaction_de_12").alias("purchase_date"),
        F.col("card_acceptor_country_code_de_43_6").cast(StringType()).alias("card_purchase_country"),
    )
 
    # acquirer_bin: chars 2-7 de acq_ref (1-indexed substr, 6 chars desde posición 2)
    df = df.withColumn("acquirer_bin",   F.substring(F.col("acq_ref"), 2, 6))
 
    # pan_prefix9: primeros 9 dígitos del PAN
    df = df.withColumn(
        "pan_prefix9",
        F.regexp_extract(F.col("pan"), r"^(\d{9})", 1),
    )
    df = df.withColumn("pan_prefix9_long", F.col("pan_prefix9").cast(LongType()))
 
    # iss_bin_6, iss_bin_8
    df = df.withColumn("iss_bin_6", F.substring("pan_prefix9", 1, 6))
    df = df.withColumn("iss_bin_8", F.substring("pan_prefix9", 1, 8))
 
    # num_card_low = pan_prefix9 * 10^9
    # num_card_high = pan_prefix9 * 10^9 + 999_999_999
    _pow9 = F.lit(10 ** 9).cast(LongType())
    df = df.withColumn("num_card_low",
                       (F.col("pan_prefix9_long") * _pow9).cast(LongType()))
    df = df.withColumn("num_card_high",
                       ((F.col("pan_prefix9_long") * _pow9) +
                        F.lit(999_999_999).cast(LongType())).cast(LongType()))
 
    # join_prefix (transacción) = pan_prefix9 // 10^6  (primeros 3 dígitos del prefix de 9)
    _pow6 = F.lit(1_000_000).cast(LongType())
    df = df.withColumn("join_prefix", (F.col("pan_prefix9_long") / _pow6).cast(LongType()))
 
     # ── PASO 3: Preparar IAR con bucket-prefix → range join eficiente ─────────
    # prefix de 18-digit number = floor(value / 10^15)
    _pow15 = F.lit(10 ** 15).cast(LongType())
    df_iar_opt = (
        df_iar
        .withColumn("low_key_long",  F.col("low_key_for_range").cast(LongType()))
        .withColumn("high_key_long", F.col("high_key_for_range").cast(LongType()))
        .withColumn("prefix_low",    (F.col("low_key_long")  / _pow15).cast(LongType()))
        .withColumn("prefix_high",   (F.col("high_key_long") / _pow15).cast(LongType()))
        # Explode: una fila por cada prefijo que abarca el rango IAR
        .withColumn("join_prefix",   F.explode(F.sequence(F.col("prefix_low"), F.col("prefix_high"))))
    )
 
    # ── Range join (equi-join en prefix + filtro de rango) ────────────────────
    # Condición: num_card_low <= high_key AND num_card_high >= low_key
    df_joined = df.join(
        F.broadcast(df_iar_opt),
        on=[
            df["join_prefix"]   == df_iar_opt["join_prefix"],
            df["num_card_low"]  <= df_iar_opt["high_key_long"],
            df["num_card_high"] >= df_iar_opt["low_key_long"],
        ],
        how="left",
    )
 
    # Limpiar columnas temporales del join
    df_joined = df_joined.drop(
        "join_prefix", "prefix_low", "prefix_high",
        "pan_prefix9_long", "low_key_long", "high_key_long", "pan",
    )
 
    # ── JOIN country (card_purchase_country → acquirer country = ac) ──────────
    country_ac = country_df.select(
        F.col("country_code_alternative").alias("_ac_cc_alt"),
        F.col("mastercard_region_code").alias("ac_region_code"),
    )
    df_joined = df_joined.join(
        F.broadcast(country_ac),
        df_joined["card_purchase_country"] == country_ac["_ac_cc_alt"],
        how="left",
    ).drop("_ac_cc_alt")
 
    # JOIN country (iar_country → issuer country = bc)
    country_bc = country_df.select(
        F.col("country_code_alternative").alias("_bc_cc_alt"),
        F.col("country_code").alias("jurisdiction_country"),
        F.col("mastercard_region_code").alias("bc_region_code"),
    )
    df_joined = df_joined.join(
        F.broadcast(country_bc),
        df_joined["iar_country"] == country_bc["_bc_cc_alt"],
        how="left",
    ).drop("_bc_cc_alt")
 
    # JOIN region (por bc_region_code)
    df_joined = df_joined.join(
        F.broadcast(region_df),
        df_joined["bc_region_code"] == region_df["region_code"],
        how="left",
    )
    df_joined = df_joined.withColumn(
        "jurisdiction_region", F.col("region_code").cast(StringType())
    )
 
    # ── business_mode ─────────────────────────────────────────────────────────
    df_joined = df_joined.withColumn(
        "business_mode",
        F.when(F.col("file_type") == "IN",  F.lit("issuing"))
         .when(F.col("file_type") == "OUT", F.lit("acquiring"))
         .otherwise(F.col("file_type").cast(StringType())),
    )
 
    # ── jurisdiction ──────────────────────────────────────────────────────────
    same_country    = F.col("card_purchase_country") == F.col("iar_country")
    collection_flag = (
        F.upper(F.coalesce(F.col("settlement_indicator"), F.lit(""))) == F.lit("C")
    )
    iss_6_match = F.col("iss_bin_6").isin(issuing_bins_6) if issuing_bins_6 else F.lit(False)
    iss_8_match = F.col("iss_bin_8").isin(issuing_bins_8) if issuing_bins_8 else F.lit(False)
    acq_match   = F.col("acquirer_bin").isin(acquiring_bins) if acquiring_bins else F.lit(False)
 
    on_us_in  = same_country & (F.col("file_type") == "IN")  & (collection_flag | acq_match)
    on_us_out = same_country & (F.col("file_type") == "OUT") & (collection_flag | iss_6_match | iss_8_match)
    intra = (~same_country) & (F.col("bc_region_code") == F.col("ac_region_code"))
    inter = (~same_country) & (F.col("bc_region_code") != F.col("ac_region_code"))
 
    df_joined = df_joined.withColumn(
        "jurisdiction",
        F.when(on_us_in | on_us_out, F.lit("on-us"))
         .when(same_country,         F.lit("off-us"))
         .when(intra,                F.lit("intraregional"))
         .when(inter,                F.lit("interregional"))
         .otherwise(F.lit(None).cast(StringType())),
    )
 
    # ── PASO 4: row_number → tomar n=1 ────────────────────────────────────────
    w = (
        Window
        .partitionBy("ref_id", "file_id", "file_idn")
        .orderBy(
            F.col("app_date_valid").desc_nulls_last(),
            F.col("high_key_for_range").desc_nulls_last(),
        )
    )
    df_joined = df_joined.withColumn("n", F.row_number().over(w))
    df_joined = df_joined.filter(F.col("n") == 1)
 
    log_info(f"  [calculate_pre2] rows={df_joined.count():,}")
    return df_joined


# =============================================================================
# 9. CALCULATE EXCHANGE RATE  (PASO 5)
# =============================================================================
 
def calculate_ex_rate(
    df_cln: DataFrame,
    client_data: dict,
    currency_df: DataFrame,
    df_ex: DataFrame,
    file_id: str,
    brand: str = "Mastercard",
) -> DataFrame:
    """
    Replica calculate_ex_rate_duckdb() en PySpark.
 
    PASO 5:
      - Selecciona campos de importe y moneda del CLN.
      - Deriva proc_date desde file_dt (YYMMDD / YYYYMMDD / ISO).
      - JOIN exchange_rate (filtrado por brand y date) × 2:
          ex_set → currency_from → settlement_currency
          ex_loc → currency_from → local_currency
      - exchange_value_settlement: 1 si misma moneda, else lookup.
      - exchange_value_local:      1 si misma moneda, else lookup.
    """
    log_info(f"[calculate_ex_rate] file_id={file_id} brand={brand}")
 
    local_ccy      = str(client_data.get("local_currency_code",      "")).strip().upper()
    settlement_ccy = str(client_data.get("settlement_currency_code", "")).strip().upper()
 
    # ── Campos del CLN ────────────────────────────────────────────────────────
    df = df_cln.select(
        F.col("ref_id"),
        F.col("file_id"),
        F.col("file_idn"),
        F.col("file_type"),
        F.col("type_mti"),
        F.col("file_dt"),
        F.col("amount_reconciliation_de_5").alias("amount_reconciliation"),
        F.col("amount_transaction_de_4").alias("amount_transaction"),
        F.col("currency_code_transaction_de_49").cast(LongType()).alias("currency_code_transaction"),
        F.col("currency_code_reconciliation_de_50").cast(LongType()).alias("currency_code_reconciliation"),
    )
 
    # ── proc_date desde file_dt ───────────────────────────────────────────────
    _fdt_str = F.col("file_dt").cast(StringType())
    df = df.withColumn(
        "proc_date",
        F.when(F.length(_fdt_str) == 6, F.to_date(_fdt_str, "yyMMdd"))
         .when(F.length(_fdt_str) == 8, F.to_date(_fdt_str, "yyyyMMdd"))
         .otherwise(F.to_date(_fdt_str)),
    )
 
    # ── Numeric codes para settlement y local (desde currency lookup) ─────────
    def _resolve_currency_numeric(alpha_code: str) -> Optional[int]:
        if not alpha_code:
            return None
        row = (
            currency_df
            .filter(F.upper(F.col("currency_alphabetic_code")) == alpha_code)
            .select("currency_numeric_code")
            .limit(1)
            .collect()
        )
        if row:
            return int(row[0]["currency_numeric_code"])
        log_warn(f"  Currency numeric code not found for: {alpha_code!r}")
        return None
 
    settlement_numeric = _resolve_currency_numeric(settlement_ccy)
    local_numeric      = _resolve_currency_numeric(local_ccy)
 
    df = df.withColumn("local_currency_code",               F.lit(local_ccy))
    df = df.withColumn("settlement_currency_code",          F.lit(settlement_ccy))
    df = df.withColumn("local_currency_code_numeric",       F.lit(local_numeric).cast(LongType()))
    df = df.withColumn("settlement_currency_code_numeric",  F.lit(settlement_numeric).cast(LongType()))
 
    # ── Preparar exchange_rate: filtrar por brand ─────────────────────────────
    df_ex_brand = df_ex.filter(F.upper(F.col("brand")) == brand.upper())
 
    # Normalizar rate_date → Date para el join con proc_date
    df_ex_brand = df_ex_brand.withColumn(
        "ex_proc_date", F.to_date(F.col("rate_date").cast(StringType()))
    )
 
    # ex_set: tasas hacia settlement_currency
    ex_set = df_ex_brand.filter(
        F.upper(F.col("currency_to")) == settlement_ccy
    ).select(
        F.col("ex_proc_date").alias("_set_date"),
        F.col("currency_from_code").cast(LongType()).alias("_set_from"),
        F.col("exchange_value").alias("_ev_settlement"),
    )
 
    # ex_loc: tasas hacia local_currency
    ex_loc = df_ex_brand.filter(
        F.upper(F.col("currency_to")) == local_ccy
    ).select(
        F.col("ex_proc_date").alias("_loc_date"),
        F.col("currency_from_code").cast(LongType()).alias("_loc_from"),
        F.col("exchange_value").alias("_ev_local"),
    )
 
    # ── JOINs exchange rate ───────────────────────────────────────────────────
    df = df.join(
        F.broadcast(ex_set),
        on=(
            (F.col("proc_date")                 == F.col("_set_date")) &
            (F.col("currency_code_transaction") == F.col("_set_from"))
        ),
        how="left",
    ).drop("_set_date", "_set_from")
 
    df = df.join(
        F.broadcast(ex_loc),
        on=(
            (F.col("proc_date")                 == F.col("_loc_date")) &
            (F.col("currency_code_transaction") == F.col("_loc_from"))
        ),
        how="left",
    ).drop("_loc_date", "_loc_from")
 
    # ── exchange_value_settlement: 1 si misma moneda, else lookup ─────────────
    df = df.withColumn(
        "exchange_value_settlement",
        F.when(
            F.col("currency_code_transaction") == F.col("settlement_currency_code_numeric"),
            F.lit(1.0),
        ).otherwise(F.col("_ev_settlement")),
    ).drop("_ev_settlement")
 
    # ── exchange_value_local: 1 si misma moneda, else lookup ──────────────────
    df = df.withColumn(
        "exchange_value_local",
        F.when(
            F.col("currency_code_transaction") == F.col("local_currency_code_numeric"),
            F.lit(1.0),
        ).otherwise(F.col("_ev_local")),
    ).drop("_ev_local")
 
    log_info(f"  [calculate_ex_rate] rows={df.count():,}")
    return df


# =============================================================================
# 10. CALCULATE SETTLEMENT REPORT  (PASO 7)
# =============================================================================
 
def calculate_settlement_report(
    df_ex_rate: DataFrame,
    df_pre2: DataFrame,
    currency_df: DataFrame,
) -> DataFrame:
    """
    Replica calculate_settlement_report_duckdb() en PySpark.
 
    PASO 7 (sin FI pairing):
      - Filtra df_ex_rate donde type_mti IN ('1240','1442').
      - INNER JOIN con df_pre2 (n=1) por (ref_id, file_id).
      - LEFT JOIN con currency por currency_code_reconciliation (→ _rec_alpha).
      - Calcula settlement_report_currency_code y settlement_report_amount.
 
    Lógica settlement_report_currency_code:
      file_type = 'IN'  → alphabetic_code de la moneda de reconciliación
      file_type ≠ 'IN'  → si jurisdiction ∈ {on-us, off-us}
                           AND currency_code_transaction = local_currency_code_numeric
                           THEN local_currency_code
                           ELSE settlement_currency_code
 
    Lógica settlement_report_amount:
      file_type = 'IN'  → amount_reconciliation
      file_type ≠ 'IN'  → si condición local: round(amount_transaction * ev_local, 4)
                           else: round(amount_transaction * ev_settlement, 4)
    """
    log_info("[calculate_settlement_report]")
 
    # Filtrar ex_rate a MTIs válidos para MC
    df_ex = df_ex_rate.filter(
        F.upper(F.col("type_mti").cast(StringType())).isin("1240", "1442")
    )
 
    # Filtrar pre2 a n=1 (solo las columnas necesarias → evitar ambigüedad en join)
    df_pre2_n1 = df_pre2.filter(F.col("n") == 1).select(
        F.col("ref_id").alias("_p_ref_id"),
        F.col("file_id").alias("_p_file_id"),
        F.col("jurisdiction"),
    )
 
    # currency para reconciliation (cur_rec)
    cur_rec = currency_df.select(
        F.col("currency_numeric_code").cast(LongType()).alias("_rec_numeric"),
        F.col("currency_alphabetic_code").cast(StringType()).alias("_rec_alpha"),
    )
 
    # ── JOINs ─────────────────────────────────────────────────────────────────
    df = df_ex.join(
        df_pre2_n1,
        on=(
            (df_ex["ref_id"]  == df_pre2_n1["_p_ref_id"]) &
            (df_ex["file_id"] == df_pre2_n1["_p_file_id"])
        ),
        how="inner",
    ).drop("_p_ref_id", "_p_file_id")
 
    df = df.join(
        F.broadcast(cur_rec),
        on=(df["currency_code_reconciliation"].cast(LongType()) == cur_rec["_rec_numeric"]),
        how="left",
    ).drop("_rec_numeric")
 
    # ── settlement_report_currency_code ───────────────────────────────────────
    is_local_jur = F.col("jurisdiction").isin("on-us", "off-us")
    same_ccy_num = (
        F.col("currency_code_transaction").cast(LongType()) ==
        F.col("local_currency_code_numeric").cast(LongType())
    )
 
    df = df.withColumn(
        "settlement_report_currency_code",
        F.when(F.col("file_type") == "IN", F.col("_rec_alpha"))
         .otherwise(
             F.when(is_local_jur & same_ccy_num, F.col("local_currency_code"))
              .otherwise(F.col("settlement_currency_code"))
         ),
    ).drop("_rec_alpha")
 
    # ── settlement_report_amount ──────────────────────────────────────────────
    amt_trx = F.col("amount_transaction").cast(DecimalType(38, 4))
    ev_set  = F.col("exchange_value_settlement").cast(DecimalType(38, 10))
    ev_loc  = F.col("exchange_value_local").cast(DecimalType(38, 10))
    amt_rec = F.col("amount_reconciliation").cast(DecimalType(18, 4))
 
    amount_local      = F.round(amt_trx * ev_loc, 4).cast(DecimalType(18, 4))
    amount_settlement = F.round(amt_trx * ev_set, 4).cast(DecimalType(18, 4))
 
    df = df.withColumn(
        "settlement_report_amount",
        F.when(F.col("file_type") == "IN", amt_rec)
         .otherwise(
             F.when(
                 is_local_jur & same_ccy_num & F.col("exchange_value_local").isNotNull(),
                 amount_local,
             ).otherwise(
                 F.when(
                     F.col("exchange_value_settlement").isNotNull(),
                     amount_settlement,
                 ).otherwise(F.lit(None).cast(DecimalType(18, 4)))
             )
         ),
    )
 
    result = df.select(
        "ref_id", "file_id", "file_idn",
        "settlement_report_currency_code",
        "settlement_report_amount",
    )
    log_info(f"  [calculate_settlement_report] rows={result.count():,}")
    return result
 
 
# =============================================================================
# 11. CALCULATE FINAL FIELDS
# =============================================================================
 
def calculate_final_fields(
    df_cln: DataFrame,
    df_pre2: DataFrame,
    df_amount: DataFrame,
    client_id: str,
    file_id: str,
) -> DataFrame:
    """
    Replica calculate_calculated_fields_duckdb() en PySpark.
 
    Ensambla el DataFrame final con:
      - Claves base del CLN (ref_id, file_id, file_idn, file_type, type_mti, file_dt)
      - client_id como literal
      - Campos de pre2 (n=1): business_mode, jurisdiction, jurisdiction_country,
        jurisdiction_region, funding_source, gcms_product_identifier,
        card_program_identifier, iar_country
      - Campos de amount: settlement_report_currency_code, settlement_report_amount
      - jurisdiction_assigned (derivado de jurisdiction):
          intraregional → jurisdiction_region
          interregional → '9'
          else          → jurisdiction_country
 
    Output cast a los tipos del layout CALCULATE_FIELDS_FINAL.
    """
    log_info(f"[calculate_final_fields] file_id={file_id}")
 
    # ── Base desde CLN ────────────────────────────────────────────────────────
    df = df_cln.select(
        "ref_id", "file_id", "file_idn", "type_mti", "file_dt"
    ).withColumn("client_id", F.lit(client_id).cast(StringType()))
 
    # ── pre2 (n=1): solo columnas necesarias, renombradas para evitar ambigüedad ──
    df_pre2_n1 = df_pre2.filter(F.col("n") == 1).select(
        F.col("ref_id").alias("_p_ref_id"),
        F.col("file_id").alias("_p_file_id"),
        F.col("file_idn").alias("_p_file_idn"),
        "business_mode", "jurisdiction",
        "jurisdiction_country", "jurisdiction_region",
        "funding_source", "gcms_product_identifier",
        "card_program_identifier", "iar_country",
    )
 
    df = df.join(
        df_pre2_n1,
        on=(
            (df["ref_id"]   == df_pre2_n1["_p_ref_id"])   &
            (df["file_id"]  == df_pre2_n1["_p_file_id"])  &
            (df["file_idn"] == df_pre2_n1["_p_file_idn"])
        ),
        how="inner",
    ).drop("_p_ref_id", "_p_file_id", "_p_file_idn")
 
    # ── amount: solo columnas necesarias, renombradas ─────────────────────────
    df_amount_sel = df_amount.select(
        F.col("ref_id").alias("_a_ref_id"),
        F.col("file_id").alias("_a_file_id"),
        "settlement_report_currency_code",
        "settlement_report_amount",
    )
 
    df = df.join(
        df_amount_sel,
        on=(
            (df["ref_id"]  == df_amount_sel["_a_ref_id"]) &
            (df["file_id"] == df_amount_sel["_a_file_id"])
        ),
        how="left",
    ).drop("_a_ref_id", "_a_file_id")
 
    # ── jurisdiction_assigned ─────────────────────────────────────────────────
    # intraregional → jurisdiction_region (string del region_code)
    # interregional → '9'
    # on-us / off-us → jurisdiction_country
    df = df.withColumn(
        "jurisdiction_assigned",
        F.when(F.col("jurisdiction") == "intraregional", F.col("jurisdiction_region"))
         .when(F.col("jurisdiction") == "interregional", F.lit("9"))
         .otherwise(F.col("jurisdiction_country")),
    )
 
    # ── Cast final según CALCULATE_FIELDS_FINAL ───────────────────────────────
    result = df.select(
        F.col("file_id").cast(StringType()),
        F.col("ref_id").cast(LongType()),
        F.col("file_idn").cast(StringType()),
        F.col("client_id").cast(StringType()),
        F.col("file_dt").cast(LongType()),
        F.col("type_mti").cast(LongType()),
        F.col("business_mode").cast(StringType()),
        F.col("jurisdiction").cast(StringType()),
        F.col("jurisdiction_country").cast(StringType()),
        F.col("jurisdiction_region").cast(LongType()),
        F.col("funding_source").cast(StringType()),
        F.col("gcms_product_identifier").cast(StringType()),
        F.col("card_program_identifier").cast(StringType()),
        F.col("jurisdiction_assigned").cast(StringType()),
        F.col("settlement_report_currency_code").cast(StringType()),
        F.col("settlement_report_amount").cast(DecimalType(18, 4)),
        F.col("iar_country").cast(StringType()),
    )
 
    log_info(f"  [calculate_final_fields] rows={result.count():,}")
    return result


# =============================================================================
# 12. EXCLUDE FLAG  (replica build_lookup_691 + add_exclude_flag)
# =============================================================================
 
def build_lookup_691_spark(
    staging_s3_url: str,
    file_paths_1644: list,
) -> DataFrame:
    """
    Replica build_lookup_691() para S3/Spark.
 
    Recibe la lista de paths completos de archivos 1644_CLN individuales
    (proveniente del campo outputs donde mti=="1644") y filtra los que
    terminan en '_691.parquet' para construir el lookup de exclusión.
 
    Cada entrada en file_paths_1644 es un s3_key relativo tal como viene
    del outputs array:  "SBSA/MC/400_IPM_1644_CLN/file_type=IN/date=.../file_691.parquet"
    Se construye el path completo anteponiendo staging_s3_url.
 
    La columna 'file_idn' se extrae del nombre del archivo con
    input_file_name() — replica extract_file_identification() del original.
 
    Devuelve DataFrame vacío (con schema correcto) si no hay archivos 691.
    """
    _empty_schema = StructType([
        StructField("_691_file_idn", StringType(), True),
        StructField("source_msg_number", StringType(), True),
    ])
 
    log_info(f"[build_lookup_691_spark] 1644 files received: {len(file_paths_1644)}")
 
    # Filtrar solo los archivos cuyo nombre termina en _691.parquet
    fc_691_keys = [
        k for k in file_paths_1644
        if re.search(r"_691\.parquet$", k, re.IGNORECASE)
    ]
 
    if not fc_691_keys:
        log_info("  No 691 parquets found — returning empty lookup.")
        return spark.createDataFrame([], _empty_schema)
 
    # Construir paths completos s3:// a partir de staging_s3_url + s3_key relativo
    staging_base = staging_s3_url.rstrip("/")
    if staging_base.startswith("s3://") or staging_base.startswith("s3a://"):
        fc_691_paths = [f"{staging_base}/{k.lstrip('/')}" for k in fc_691_keys]
    else:
        fc_691_paths = [f"{staging_base}/{k.lstrip('/')}" for k in fc_691_keys]
    log_info(f"  Found {len(fc_691_paths)} 691 parquet(s)")
 
    df_691 = spark.read.option("mergeSchema", "false").parquet(*fc_691_paths) #TESTING df_691 = spark.read.parquet(*fc_691_paths)
 
    # Extraer file_idn del nombre del archivo:
    # pattern: {md5}_{file_idn}_{mti}_{fc}.parquet → split("_")[1] = file_idn
    df_691 = df_691.withColumn("_src_file", F.input_file_name())
    df_691 = df_691.withColumn(
        "_stem",
        F.regexp_extract(F.col("_src_file"), r"/([^/]+)\.parquet$", 1),
    )
    df_691 = df_691.withColumn(
        "_691_file_idn",
        F.split(F.col("_stem"), "_").getItem(1),
    )
 
    result = df_691.select(
        F.col("_691_file_idn"),
        F.col("source_message_number_id_pds_138").cast(StringType()).alias("source_msg_number"),
    ).dropna(subset=["source_msg_number"]).drop_duplicates()
 
    result = result.cache()
    log_info(f"  Lookup 691 cached ({result.count():,} entries)")
    return result
 
 
def build_exclude_keys(
    df_cln: DataFrame,
    df_lookup_691: DataFrame,
) -> DataFrame:
    """
    Genera el conjunto de claves (ref_id, file_id, file_idn, file_dt, type_mti)
    que deben tener exclude_flag=1.
 
    Replica la lógica de create_df_exclude_flag():
      - Cruza df_cln con df_lookup_691 por
        (file_idn == _691_file_idn) AND (message_number_de_71 == source_msg_number)
      - Solo las filas que hagan match → exclude_flag = 1
    """
    # Columna message_number_de_71 puede no existir → verificar
    if "message_number_de_71" not in df_cln.columns:
        log_warn("[build_exclude_keys] 'message_number_de_71' not in CLN — no exclusions applied")
        return spark.createDataFrame(
            [],
            StructType([
                StructField("ref_id",    LongType(),   True),
                StructField("file_id",   StringType(), True),
                StructField("file_idn",  StringType(), True),
                StructField("file_dt",   LongType(),   True),
                StructField("type_mti",  LongType(),   True),
                StructField("_exclude",  IntegerType(), True),
            ]),
        )
 
    df_cln_min = df_cln.select(
        F.col("ref_id").cast(LongType()),
        F.col("file_id").cast(StringType()),
        F.col("file_idn").cast(StringType()),
        F.col("file_dt").cast(LongType()),
        F.col("type_mti").cast(LongType()),
        F.col("message_number_de_71").cast(StringType()).alias("_msg_num"),
    )
 
    df_exclude_keys = df_cln_min.join(
        F.broadcast(df_lookup_691),
        on=(
            (df_cln_min["file_idn"] == df_lookup_691["_691_file_idn"]) &
            (df_cln_min["_msg_num"] == df_lookup_691["source_msg_number"])
        ),
        how="inner",
    ).select(
        "ref_id", "file_id", "file_idn", "file_dt", "type_mti",
        F.lit(1).cast(IntegerType()).alias("_exclude"),
    )
 
    return df_exclude_keys
 
 
def apply_exclude_flag(
    df_final: DataFrame,
    df_exclude_keys: DataFrame,
) -> DataFrame:
    """
    Aplica exclude_flag a df_final.
    Replica add_exclude_flag():
      - LEFT JOIN por (ref_id, file_id, file_idn, file_dt, type_mti)
      - exclude_flag = 1 donde hay match, 0 en el resto
    """
    ex = df_exclude_keys.select(
        F.col("ref_id").cast(LongType()).alias("_ex_ref_id"),
        F.col("file_id").cast(StringType()).alias("_ex_file_id"),
        F.col("file_idn").cast(StringType()).alias("_ex_file_idn"),
        F.col("file_dt").cast(LongType()).alias("_ex_file_dt"),
        F.col("type_mti").cast(LongType()).alias("_ex_type_mti"),
        "_exclude",
    )
 
    df_out = df_final.join(
        ex,
        on=(
            (df_final["ref_id"].cast(LongType())   == ex["_ex_ref_id"])   &
            (df_final["file_id"].cast(StringType()) == ex["_ex_file_id"])  &
            (df_final["file_idn"].cast(StringType())== ex["_ex_file_idn"]) &
            (df_final["file_dt"].cast(LongType())   == ex["_ex_file_dt"])  &
            (df_final["type_mti"].cast(LongType())  == ex["_ex_type_mti"])
        ),
        how="left",
    ).drop("_ex_ref_id", "_ex_file_id", "_ex_file_idn", "_ex_file_dt", "_ex_type_mti")
 
    df_out = df_out.withColumn(
        "exclude_flag",
        F.when(F.col("_exclude").isNotNull(), F.lit(1)).otherwise(F.lit(0)).cast(LongType()),
    ).drop("_exclude")
 
    return df_out
 
 
# =============================================================================
# 13. I/O HELPERS
# =============================================================================
 
def load_parquet_safe(path: str, schema=None) -> DataFrame:
    """Carga un folder/archivo Parquet desde S3.
    Si se pasa schema, evita la inferencia del footer (necesario para TIMESTAMP_NS).
    """
    log_info(f"  Loading: {path}")
    reader = spark.read.option("mergeSchema", "false")
    if schema is not None:
        reader = reader.schema(schema)
    df    = reader.parquet(path)
    count = df.count()
    log_info(f"  Loaded {count:,} records")
    return df
 
 
def save_parquet(df: DataFrame, path: str) -> None:
    """
    Guarda DataFrame como un único archivo Parquet con el path exacto indicado.
 
    Estrategia: convierte a pandas → escribe con pyarrow directamente al path.
    Esto preserva el nombre de archivo original (ej: 074b0b73..._idn_1240.parquet)
    en vez de generar un part-00000-xxx.parquet.
 
    Aplica para archivos pequeños (un file_idn por vez), por lo que toPandas()
    es seguro — no hay riesgo de OOM.
    """
    import os
    import pyarrow as pa
    import pyarrow.parquet as pq
 
    # Crear carpeta destino si no existe (relevante en local; en S3 no aplica
    # porque S3 no tiene directorios reales, pero no rompe nada)
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
 
    pdf = df.toPandas()
 
    # Normalizar timestamps: Spark puede producir ns, pyarrow espera us
    # Usamos dtype check directo para evitar falsos positivos de Pylance.
    for col in pdf.columns:
        if str(pdf[col].dtype).startswith("datetime64"):
            pdf[col] = pdf[col].astype("datetime64[us]")
 
    table = pa.Table.from_pandas(pdf, preserve_index=False)
    pq.write_table(
        table,
        path,
        compression="snappy",
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )
    log_info(f"  Saved to: {path} ({len(pdf):,} rows)")
 
 
def build_output_file_path(input_file_path: str) -> str:
    """
    Deriva el path completo del archivo CAL a partir del path completo del CLN.
 
    Preserva carpetas intermedias y nombre de archivo. Ejemplos:
      …/400_IPM_1240_CLN/file_type=IN/date=2026-02-18/074b…_101_1240.parquet
      → …/500_IPM_1240_CAL/file_type=IN/date=2026-02-18/074b…_101_1240.parquet
 
      …/400_IPM_1442_CLN/file_type=IN/date=2026-02-18/074b…_151_1442.parquet
      → …/500_IPM_1442_CAL/file_type=IN/date=2026-02-18/074b…_151_1442.parquet
 
      …/400_IPM_1740_CLN/file_type=IN/date=2026-02-18/074b…_171_1740.parquet
      → …/500_IPM_1740_CAL/file_type=IN/date=2026-02-18/074b…_171_1740.parquet
    """
    return (
        input_file_path
        .replace("400_IPM_1240_CLN", "500_IPM_1240_CAL")
        .replace("400_IPM_1442_CLN", "500_IPM_1442_CAL")
        .replace("400_IPM_1740_CLN", "500_IPM_1740_CAL")
    )
 
 
def build_output_path(input_s3_key: str) -> str:
    """
    Deriva el path de la CARPETA CAL a partir de la carpeta CLN.
    Se mantiene para compatibilidad y uso en logs del main().
    Reemplaza:
      400_IPM_1240_CLN  →  500_IPM_1240_CAL
      400_IPM_1442_CLN  →  500_IPM_1442_CAL
    """
    return (
        input_s3_key
        .replace("400_IPM_1240_CLN", "500_IPM_1240_CAL")
        .replace("400_IPM_1442_CLN", "500_IPM_1442_CAL")
    )


# =============================================================================
# 14. PROCESS FILE  (un archivo parquet individual)
# =============================================================================
 
def process_file(
    mti: str,
    input_s3_key: str,
    staging_s3_url: str,
    file_id: str,
    file_type: str,
    client_data: dict,
    df_iar: DataFrame,
    country_df: DataFrame,
    region_df: DataFrame,
    currency_df: DataFrame,
    df_ex: DataFrame,
    df_lookup_691: DataFrame,
    cln_schema=None,
    content_hash: str = "",
) -> dict:
    """
    Procesa un único archivo parquet CLN y produce su correspondiente CAL.
 
    ARQUITECTURA:
      - Recibe el s3_key relativo de un archivo parquet específico
        (ej: "SBSA/MC/400_IPM_1240_CLN/file_type=IN/date=2026-02-18/074b…_1240.parquet")
      - Construye el path completo anteponiendo staging_s3_url.
      - Ejecuta el pipeline completo (pre2 → ex_rate → settlement → final → exclude).
      - Escribe 1 parquet de salida con el mismo nombre en la carpeta CAL.
      - Las maestras (IAR, country, currency, ex_rate) se reciben ya cacheadas.
 
    Cada entrada del array outputs del Step Function produce exactamente
    un archivo CAL de salida — N inputs → N outputs.
    """
    log_info("")
    log_info("=" * 60)
    log_info(f"Processing file — MTI {mti}")
    log_info("=" * 60)
 
    staging_base = staging_s3_url.rstrip("/")
 
    # Construir path completo del archivo de entrada
    input_file_path = f"{staging_base}/{input_s3_key.lstrip('/')}"
 
    # Derivar path del archivo de salida (CLN → CAL)
    output_file_path = build_output_file_path(input_file_path)
 
    filename = input_s3_key.rsplit("/", 1)[-1]
 
    log_info(f"  Input file:  {input_file_path}")
    log_info(f"  Output file: {output_file_path}")
    log_info(f"  Filename:    {filename}")
 
    df_cln   = None
    df_pre2  = None
    df_er    = None
    df_amt   = None
    df_final = None
 
    try:
        # 1. Leer este parquet individual (sin detección Hive)
        df_cln  = load_parquet_safe(input_file_path, schema=cln_schema).withColumn("file_type", F.lit(file_type)).cache()
 
        # 2. Pre2 (PASOS 2+3+4)
        df_pre2 = calculate_pre2(
            df_cln=df_cln,
            df_iar=df_iar,
            country_df=country_df,
            region_df=region_df,
            client_data=client_data,
            file_id=file_id,
        ).cache()
 
        # 3. Exchange Rate (PASO 5)
        df_er = calculate_ex_rate(
            df_cln=df_cln,
            client_data=client_data,
            currency_df=currency_df,
            df_ex=df_ex,
            file_id=file_id,
            brand="Mastercard",
        ).cache()
 
        # 4. Settlement Report (PASO 7)
        df_amt = calculate_settlement_report(
            df_ex_rate=df_er,
            df_pre2=df_pre2,
            currency_df=currency_df,
        ).cache()
 
        # 5. Final Fields
        df_final_raw = calculate_final_fields(
            df_cln=df_cln,
            df_pre2=df_pre2,
            df_amount=df_amt,
            client_id=client_data["client_id"],
            file_id=file_id,
        )
 
        # 6. Exclude Flag
        df_exclude_keys = build_exclude_keys(df_cln, df_lookup_691)
        df_final = apply_exclude_flag(df_final_raw, df_exclude_keys).cache()

        # 7. Columna de trazabilidad (igual que VI)
        df_final = df_final.withColumn("content_hash", F.lit(content_hash))

        # 8. Escribir con el mismo nombre de archivo que el input
        record_count = df_final.count()
        save_parquet(df_final, output_file_path)
 
        log_info(f"    ✓ {filename}: {record_count:,} records → {output_file_path}")
 
        return {
            "status":        "SUCCESS",
            "mti":           mti,
            "input_file":    input_file_path,
            "output_file":   output_file_path,
            "s3_key":        build_output_file_path(input_s3_key),
            "records":       record_count,
        }
 
    finally:
        for _df in [df_cln, df_pre2, df_er, df_amt, df_final]:
            try:
                if _df is not None:
                    _df.unpersist()
            except Exception:
                pass


# =============================================================================
# 15. MAIN
# =============================================================================
 
def main():
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "S3_REFERENCE",
        "S3_STAGING",
        "client_id",
        "file_id",
        "file_type",
        "file_date",
        "outputs",
        "dynamodb_table_client",
        "dynamodb_table_fields",
        "content_hash",
    ])
 
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
 
    client_id           = args["client_id"]
    file_id             = args["file_id"]
    file_type           = args["file_type"]
    file_date           = args["file_date"]           # YYYY-MM-DD
    s3_reference_url    = args["S3_REFERENCE"]
    s3_staging_url      = args["S3_STAGING"]
    dynamo_table_client = args["dynamodb_table_client"]
    dynamo_table_fields = args["dynamodb_table_fields"]
    content_hash        = args["content_hash"]
    outputs             = json.loads(args["outputs"])
 
    log_info("=" * 70)
    log_info("MC-CALCULATE (PySpark/Glue 4.0) — STARTING")
    log_info("=" * 70)
    log_info(f"  JOB_NAME:          {args['JOB_NAME']}")
    log_info(f"  client_id:         {client_id}")
    log_info(f"  file_id:           {file_id}")
    log_info(f"  file_type:         {file_type}")
    log_info(f"  file_date:         {file_date}")
    log_info(f"  content_hash:      {content_hash}")
    log_info(f"  S3_REFERENCE:      {s3_reference_url}")
    log_info(f"  S3_STAGING:        {s3_staging_url}")
    log_info(f"  DynamoDB client:   {dynamo_table_client}")
    log_info(f"  DynamoDB fields:   {dynamo_table_fields}")
    log_info(f"  Total outputs:     {len(outputs)}")
    log_info("=" * 70)
 
    # ── Log outputs recibidos ─────────────────────────────────────────────────
    for i, o in enumerate(outputs):
        log_info(f"  output[{i}]: mti={o.get('mti','?')} s3_key={o.get('s3_key','?')}")
 
    # ── 1. Datos del cliente (DynamoDB) ───────────────────────────────────────
    log_info(f"Loading client data: {client_id}")
    client_data = get_client_data(client_id, dynamo_table_client)
 
    # ── 2. Schemas CLN desde DynamoDB (uno por MTI transaccional) ─────────────
    # Construidos una vez y reutilizados para todos los archivos del mismo MTI.
    log_info("Building CLN schemas from DynamoDB...")
    transactional_mtis = list(dict.fromkeys(
        str(o.get("mti", ""))
        for o in outputs
        if str(o.get("mti", "")) not in ("", "1644", "1740")
    ))
    cln_schemas: dict = {}
    for mti_key in transactional_mtis:
        log_info(f"  Building schema for MTI {mti_key}...")
        cln_schemas[mti_key] = build_cln_schema_from_dynamodb(dynamo_table_fields, mti_key)
    log_info(f"  Schemas built for MTIs: {list(cln_schemas.keys())}")
 
    # ── 3. Tablas de referencia (S3 → Spark) ──────────────────────────────────
    rate_date = _to_rate_date(file_date)
    log_info(f"Loading reference tables (rate_date={rate_date})...")
 
    df_iar      = prepare_iar(s3_reference_url, file_date)
    country_df  = load_country(s3_reference_url).cache()
    region_df   = load_region(s3_reference_url).cache()
    currency_df = load_currency(s3_reference_url).cache()
    df_ex       = load_exchange_rate(s3_reference_url, rate_date).cache()
 
    log_info(f"  country rows:  {country_df.count():,}")
    log_info(f"  region rows:   {region_df.count():,}")
    log_info(f"  currency rows: {currency_df.count():,}")
    log_info(f"  ex_rate rows:  {df_ex.count():,}")
 
    # ── 4. Lookup 691 desde outputs 1644 ─────────────────────────────────────
    # Los archivos 1644_CLN vienen directamente en el outputs array (mti=="1644").
    # Se extrae su s3_key para construir el lookup de exclusión.
    # No se requiere parámetro separado --s3_key_1644_cln.
    keys_1644 = [
        str(o.get("s3_key", ""))
        for o in outputs
        if str(o.get("mti", "")) == "1644" and o.get("s3_key")
    ]
    log_info(f"Building 691 lookup from {len(keys_1644)} 1644 file(s)...")
    df_lookup_691 = build_lookup_691_spark(s3_staging_url, keys_1644)
 
    # ── 5. Procesar cada archivo individual ───────────────────────────────────
    # Cada entrada del outputs array es un parquet específico.
    # MTI 1644 se usa solo para el lookup (ya procesado arriba) — no es transaccional.
    results       = []
    total_records = 0
 
    for output_config in outputs:
        mti          = str(output_config.get("mti", "UNKNOWN"))
        input_s3_key = output_config.get("s3_key", "")
 
        if mti == "1740":
            log_info(f"  Skipping mti=1740 (no business logic): {input_s3_key}")
            results.append({
                "status": "SUCESS",
                "mti": "1740",
                "s3_key": input_s3_key,
                "records": None,
            })
            continue
 
        if mti == "1644":
            log_info(f"  Skipping mti=1644 (lookup only): {input_s3_key}")
            continue
 
        if not input_s3_key:
            raise ValueError(f"Missing s3_key in output_config for mti={mti}")
 
        result = process_file(
            mti=mti,
            input_s3_key=input_s3_key,
            staging_s3_url=s3_staging_url,
            file_id=file_id,
            file_type=file_type,
            client_data=client_data,
            df_iar=df_iar,
            country_df=country_df,
            region_df=region_df,
            currency_df=currency_df,
            df_ex=df_ex,
            df_lookup_691=df_lookup_691,
            cln_schema=cln_schemas.get(mti),
            content_hash=content_hash,
        )
 
        results.append(result)
        total_records += result.get("records", 0)
        log_info(
            f"  ✓ mti={mti} | {result.get('records', 0):,} records"
            f" | {result.get('s3_key', '?')}"
        )
 
    # ── 6. Liberar caches y finalizar ─────────────────────────────────────────
    for _df in [df_iar, country_df, region_df, currency_df, df_ex, df_lookup_691]:
        try:
            _df.unpersist()
        except Exception:
            pass
 
    log_info("")
    log_info("=" * 70)
    log_info("MC-CALCULATE COMPLETED")
    log_info("=" * 70)
    log_info(f"  Total files processed: {len(results)}")
    log_info(f"  Total records:         {total_records:,}")
 
    output_data = {
        "status":        "SUCCESS",
        "total_outputs": len(results),
        "total_records": total_records,
        "outputs":       results,
        "content_hash":  content_hash,
    }
    log_info(f"Output summary: {json.dumps(output_data)}")
 
    job.commit()
    return output_data
 
 
if __name__ == "__main__":
    main()