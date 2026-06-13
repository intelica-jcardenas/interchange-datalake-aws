"""
glue-vi-mc-reporting
====================
Genera el reporte consolidado de transacciones (equivalente al SP
analytics.generate_transaction_tables) a partir de los Parquets
finales del bucket operational.

Fuentes de datos
----------------
  S3 operational  : Visa BASEII, Visa SMS, MC IPM_1240, MC IPM_1442
  S3 reference    : country/data.parquet, currency/data.parquet,
                    exchange_rate/rate_date=*/, visa_bin_products/data.parquet,
                    mastercard_bin_products/data.parquet
  DynamoDB        : tabla client (report_currency_code, duplicate_on_us_flag_*)

Salida
------
  S3 analytics    : {client_id}/reports/report_transactions_{client_id}_{report_suffix}.parquet
  Una sola escritura por ejecución, cubriendo el rango [start_date, end_date].
  report_suffix es un parámetro del job (ej: "202601", "202601_v2").

Schema de salida (31 columnas — idéntico a report_transactions en PostgreSQL)
----------------------------------------------------------------------
  customer_code, file_id, row_id, customer_country_code, business_mode_code,
  brand_code, processing_date, processing_month, transaction_date,
  transaction_month, transaction_group_code, transaction_type_id,
  jurisdiction_code, merchant_country_code, mcc_code, merchant, merchant_id,
  terminal_id, acquirer_bin, issuer_country_code, issuer_bin_6, issuer_bin_8,
  funding_source_code, product_program_id, product_code, card_present_code,
  is_reversal_or_chargeback, interchange_rule, reported_currency_code,
  transaction_amount, interchange_fees_amount, scheme_fees_amount

Parámetros del job
------------------
  --client_code           Código de cliente único, ej: "EBGR"
  --start_date            Inicio del rango en formato YYYY-MM-DD (inclusive)
  --end_date              Fin del rango en formato YYYY-MM-DD (inclusive)
  --report_suffix         Identificador para el nombre de salida, ej: "202601"
                           o "202601_v2" — reemplaza al YYYYMM[_suffix] anterior
  --scheme_fee            "true" / "false"
  --operational_bucket    Nombre del bucket operational (lectura de Parquets)
  --reference_bucket      Nombre del bucket reference (country, exchange-rates)
  --analytics_bucket      Nombre del bucket analytics (escritura del reporte)
  --dynamodb_table_client Nombre de la tabla DynamoDB de clientes

Pendientes (TODO)
-----------------
  - scheme_fees_amount : join con Parquet de scheme fees cuando el flujo esté
                         disponible — actualmente retorna 0.0
  - SMS column names   : verificar nombres exactos contra Parquet operational SMS
  - MC (1240/1442)     : nombres de columna verificados contra Parquet operational
                         (2026-06-12). Pendiente de validar contra el SP legacy:
                         el swap de columna de país por MTI (is_1442) en customer/
                         merchant/issuer_country_code, y la lógica invertida de
                         is_reversal_or_chargeback (message_reversal_indicator
                         NULL = reversal en 1442, NOT NULL = reversal en 1240).
"""

import sys
import boto3
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

import pyspark.sql.functions as F
from pyspark.sql.types import (
    StringType,
    IntegerType,
    BooleanType,
    DoubleType,
    DateType,
    StructType,
    NullType,
)
from pyspark.sql.pandas.types import from_arrow_schema
from pyspark.sql import SparkSession, DataFrame

# =============================================================================
# Spark Configuration
# =============================================================================

spark = (
    SparkSession.builder.config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
    .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    # operational MC (IPM_1240/1442) contiene date_and_time_local_transaction_de_12
    # como TIMESTAMP(NANOS,false) — PySpark 3.3 no soporta ese subtipo al leer →
    # AnalysisException: Illegal Parquet type. nanosAsLong=true la lee como LongType
    # (ns desde epoch) sin error. Mismo patrón ya usado en glue-mc-calculate/interchange.
    .config("spark.sql.legacy.parquet.nanosAsLong", "true")
    .getOrCreate()
)
spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")

glueContext = GlueContext(spark.sparkContext)

# =============================================================================
# Logs definition
# =============================================================================
logger = glueContext.get_logger()


def log_info(message: str):
    logger.info(f"GlueLogger: {message}")


def log_error(message: str):
    logger.error(f"GlueLogger: {message}")


# =============================================================================
# PARÁMETROS DEL JOB
# =============================================================================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "client_code",
        "start_date",
        "end_date",
        "report_suffix",
        "scheme_fee",
        "operational_bucket",
        "reference_bucket",
        "analytics_bucket",
        "dynamodb_table_client",
    ],
)

CLIENT_CODE = args["client_code"].strip().upper()
START_DATE = args["start_date"]
END_DATE = args["end_date"]
REPORT_SUFFIX = args["report_suffix"].strip()
SCHEME_FEE = args["scheme_fee"].lower() == "true"
OPERATIONAL_BUCKET = args["operational_bucket"]
BUCKET_REF = args["reference_bucket"]
ANALYTICS_BUCKET = args["analytics_bucket"]
DDB_CLIENT_TABLE = args["dynamodb_table_client"]

# =============================================================================
# HELPERS
# =============================================================================


def get_client_config(client_code: str) -> dict:
    """Lee report_currency_code y flags duplicate_on_us desde DynamoDB."""
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(DDB_CLIENT_TABLE)
    resp = table.get_item(Key={"client_id": client_code})
    item = resp.get("Item", {})
    return {
        "report_currency": item.get("report_currency_code", "USD"),
        "dup_on_us_visa": bool(item.get("duplicate_on_us_flag_visa", False)),
        "dup_on_us_mc": bool(item.get("duplicate_on_us_flag_mastercard", False)),
    }


# Columnas operational con TIMESTAMP(NANOS,false) — Glue 4.0 (Spark 3.3) no puede
# inferir ese subtipo (AnalysisException: Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))),
# sin importar el valor de nanosAsLong (esa config solo afecta la conversión de datos
# cuando el schema ya es explícito, no la inferencia). Único campo conocido: MTI 1240/1442.
_NANOS_AS_LONG_COLS = {"date_and_time_local_transaction_de_12"}


def _widest_arrow_type(t1: pa.DataType, t2: pa.DataType) -> pa.DataType:
    """
    Devuelve el tipo Arrow más "ancho" entre t1 y t2, suficiente para
    representar sin pérdida los valores de ambos. Usado para unificar columnas
    cuyo tipo físico Parquet varía entre archivos del mismo directorio
    operational MC (decimal con distinta precisión, int64 vs double, string
    vs null) — análogo al gotcha _cal_dtype_map de lmbd-vi-store, pero entre
    archivos en vez de entre etapas del pipeline.
    """
    if t1.equals(t2):
        return t1
    if pa.types.is_null(t1):
        return t2
    if pa.types.is_null(t2):
        return t1
    if pa.types.is_decimal(t1) and pa.types.is_decimal(t2):
        scale = max(t1.scale, t2.scale)
        int_digits = max(t1.precision - t1.scale, t2.precision - t2.scale)
        return pa.decimal128(min(int_digits + scale, 38), scale)
    if pa.types.is_floating(t1) or pa.types.is_floating(t2):
        return pa.float64()
    if pa.types.is_integer(t1) and pa.types.is_integer(t2):
        return t1 if t1.bit_width >= t2.bit_width else t2
    # Sin regla específica (ej. string vs número): mantener el primero visto.
    return t1


def _align_table_to_schema(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """
    Castea/rellena las columnas de table al schema unificado.

    - Columna NANOS (_NANOS_AS_LONG_COLS): si llega como timestamp[ns], se
      reinterpreta con .view(int64) — timestamp[ns] -> int64 no es un cast
      válido en pyarrow porque son tipos lógicos distintos sobre el mismo
      layout binario (ns desde epoch), .view() preserva los bytes.
    - Columnas ausentes en table: se rellenan con null del tipo destino.
    - Resto: .cast(field.type) (decimal con precisión menor -> mayor,
      int64 -> double, null -> tipo real, todos castings seguros).
    """
    arrays = []
    for field in schema:
        if field.name in table.column_names:
            col = table.column(field.name)
            if field.name in _NANOS_AS_LONG_COLS and pa.types.is_timestamp(col.type):
                col = pa.chunked_array(
                    [chunk.view(pa.int64()) for chunk in col.chunks], type=pa.int64()
                )
            elif not col.type.equals(field.type):
                col = col.cast(field.type)
        else:
            col = pa.nulls(table.num_rows, type=field.type)
        arrays.append(col)
    return pa.Table.from_arrays(arrays, schema=schema)


def _read_operational_via_pyarrow(base_path: str, client_id: str) -> DataFrame | None:
    """
    Lee todos los Parquet bajo base_path con PyArrow y construye el DataFrame
    de Spark en el driver — usado cuando spark.read.parquet() falla por
    TIMESTAMP(NANOS) (error de inferencia de schema) o por
    SchemaColumnConvertNotSupportedException (tipos físicos inconsistentes
    entre archivos: decimal con distinta precisión, int64 vs double, string
    vs null — ver gotcha tipos operational MC IPM_1240/1442). PyArrow lee
    ambos casos sin error (confirmado por scan_mc_operational_schema_variance.py).

    Por archivo:
      - lee la tabla completa con PyArrow
      - agrega columnas derivadas del path: particiones Hive (file_type, date),
        file_id (= content_hash, nombre del archivo) y customer_code

    Luego calcula un schema unificado (tipo más ancho por columna, ver
    _widest_arrow_type), fuerza _NANOS_AS_LONG_COLS a int64 (equivalente a
    spark.sql.legacy.parquet.nanosAsLong=true) y descarta columnas que quedan
    100% null en todos los archivos. Castea cada tabla a ese schema
    (_align_table_to_schema), concatena, convierte a pandas y construye el
    DataFrame de Spark con ese schema explícito.
    """
    parsed = urlparse(base_path)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]
    if not keys:
        return None

    fs = pafs.S3FileSystem()
    tables = []
    for key in keys:
        with fs.open_input_file(f"{bucket}/{key}") as f:
            table = pq.ParquetFile(f).read()

        n = table.num_rows
        partitions = dict(part.split("=", 1) for part in key.split("/") if "=" in part)
        for col_name, value in partitions.items():
            table = table.append_column(col_name, pa.array([value] * n, type=pa.string()))

        file_id = key.rsplit("/", 1)[-1].rsplit(".parquet", 1)[0]
        table = table.append_column("file_id", pa.array([file_id] * n, type=pa.string()))
        table = table.append_column("customer_code", pa.array([client_id] * n, type=pa.string()))

        tables.append(table)

    # Schema unificado: tipo más ancho por columna entre todos los archivos.
    unified_fields = {}
    for table in tables:
        for field in table.schema:
            if field.name not in unified_fields:
                unified_fields[field.name] = field.type
            else:
                unified_fields[field.name] = _widest_arrow_type(
                    unified_fields[field.name], field.type
                )

    for col_name in _NANOS_AS_LONG_COLS:
        if col_name in unified_fields:
            unified_fields[col_name] = pa.int64()

    # Columnas 100% null en TODOS los archivos: sin tipo real, se descartan.
    unified_fields = {
        name: dtype
        for name, dtype in unified_fields.items()
        if not pa.types.is_null(dtype)
    }
    unified_schema = pa.schema(
        [pa.field(name, dtype) for name, dtype in unified_fields.items()]
    )

    aligned = [_align_table_to_schema(table, unified_schema) for table in tables]
    unified = pa.concat_tables(aligned)

    pdf = unified.to_pandas()
    return spark.createDataFrame(pdf, schema=from_arrow_schema(unified_schema))


def read_operational(
    client_id: str, brand: str, data_type: str, start_date: str, end_date: str
) -> DataFrame | None:
    """
    Lee todos los Parquets del bucket operational para un cliente, marca y
    tipo de dato, filtrando por el rango de fechas [start_date, end_date].

    Añade columnas de metadata derivadas del path:
      - customer_code : literal del client_id
      - file_id       : nombre del archivo Parquet (= content_hash)

    Las particiones Hive (file_type, date) son añadidas automáticamente
    por Spark como columnas del DataFrame (o derivadas del path por PyArrow
    en el fallback, ver _read_operational_via_pyarrow).
    """
    base_path = f"s3://{OPERATIONAL_BUCKET}/{client_id}/{brand}/{data_type}/"

    try:
        df = spark.read.parquet(base_path)

        # Columnas 100% null en algunos archivos se infieren como NullType y
        # rompen SchemaColumnConvertNotSupportedException al leer el directorio
        # junto a archivos donde tienen tipo real. No se usan en el reporte:
        # se excluyen del schema antes de leer.
        null_type_cols = [
            f.name for f in df.schema.fields if isinstance(f.dataType, NullType)
        ]
        if null_type_cols:
            log_info(f"[read_operational] Excluding null-typed columns: {null_type_cols}")
            reduced_schema = StructType(
                [f for f in df.schema.fields if f.name not in null_type_cols]
            )
            df = spark.read.schema(reduced_schema).parquet(base_path)

        df = df.filter(F.col("date").between(start_date, end_date))

        if df.rdd.isEmpty():
            log_info(
                f"[read_operational] No rows for {client_id}/{brand}/{data_type} "
                f"in [{start_date}, {end_date}]"
            )
            return None

        df = (
            df.withColumn("_path", F.input_file_name())
            .withColumn("file_id", F.regexp_extract("_path", r"/([^/]+)\.parquet$", 1))
            .withColumn("customer_code", F.lit(client_id))
            .drop("_path")
        )
    except Exception as e:
        msg = str(e)
        is_nanos_error = "Illegal Parquet type" in msg and "TIMESTAMP(NANOS" in msg
        is_type_mismatch = "SchemaColumnConvertNotSupportedException" in msg
        if not (is_nanos_error or is_type_mismatch):
            log_info(f"[read_operational] No data at {base_path}: {e}")
            return None

        log_info(
            f"[read_operational] {base_path}: tipos Parquet inconsistentes "
            f"entre archivos (TIMESTAMP(NANOS) o decimal/int/null), "
            f"leyendo via PyArrow"
        )
        try:
            df = _read_operational_via_pyarrow(base_path, client_id)
        except Exception as e2:
            log_info(f"[read_operational] No data at {base_path}: {e2}")
            return None
        if df is None:
            log_info(f"[read_operational] No data at {base_path}")
            return None

        df = df.filter(F.col("date").between(start_date, end_date))

        if df.rdd.isEmpty():
            log_info(
                f"[read_operational] No rows for {client_id}/{brand}/{data_type} "
                f"in [{start_date}, {end_date}]"
            )
            return None

    log_info(
        f"[read_operational] Loaded {client_id}/{brand}/{data_type}: {df.count()} rows"
    )
    return df


# =============================================================================
# TABLAS DE REFERENCIA
# =============================================================================


def load_country() -> DataFrame:
    """
    Carga la tabla de países desde S3 reference.
    Columnas usadas:
      country_code             (2-letter ISO, e.g. 'GR')
      country_code_alternative (3-letter ISO, e.g. 'GRC')
    """
    path = f"s3://{BUCKET_REF}/country/data.parquet"
    return spark.read.parquet(path).select("country_code", "country_code_alternative")


def load_visa_bin_products() -> DataFrame:
    """
    Carga la tabla de productos Visa por rango de BIN (m_visa_bin_products
    en legacy) desde S3 reference.

    Mapea product_id (calculado en glue-vi-calculate via cruce ARDEF) a
    product_program_id. Columnas usadas: bin_product_id, range_program_id.
    """
    path = f"s3://{BUCKET_REF}/visa_bin_products/data.parquet"
    return spark.read.parquet(path).select("bin_product_id", "range_program_id")


def load_currency() -> DataFrame:
    """
    Carga la tabla de monedas (ISO 4217 numérico → alfabético) desde S3
    reference. Usada para mapear currency_code_transaction_de_49 /
    currency_code_reconciliation_de_50 (códigos numéricos en el operational
    de Mastercard) a código alfabético para el join con exchange_rate
    (cuyas columnas from_currency/to_currency son alfabéticas).
    """
    path = f"s3://{BUCKET_REF}/currency/data.parquet"
    return spark.read.parquet(path).select(
        "currency_numeric_code", "currency_alphabetic_code"
    )


def load_mastercard_bin_products() -> DataFrame:
    """
    Carga la tabla de productos Mastercard por rango de BIN
    (m_mastercard_bin_products en legacy) desde S3 reference.

    Mapea gcms_product_identifier (DE_48 PDS_0002, ya presente en el
    operational) a product_program_id. Columnas usadas: bin_product_id,
    range_program_id.
    """
    path = f"s3://{BUCKET_REF}/mastercard_bin_products/data.parquet"
    return spark.read.parquet(path).select("bin_product_id", "range_program_id")


def load_exchange_rates(start_date: str, end_date: str, brand_path: str) -> DataFrame:
    """
    Carga los tipos de cambio desde S3 reference para el rango de fechas
    [start_date, end_date]. Path: exchange_rate/rate_date=YYYY-MM-DD/

    La tabla cubre ambas marcas en una sola ubicación, distinguidas por la
    columna `brand` ('VISA' / 'MasterCard'). La partición rate_date es
    añadida automáticamente por Spark.
    Columnas resultantes: exchange_date, from_currency, to_currency, fx_rate
    """
    path = f"s3://{BUCKET_REF}/exchange_rate/"
    try:
        df = spark.read.parquet(path)
    except Exception as e:
        log_error(f"[load_exchange_rates] No exchange rates at {path}: {e}")
        return spark.createDataFrame(
            [],
            schema="exchange_date string, from_currency string, to_currency string, fx_rate double",
        )

    df = df.filter(F.upper(F.col("brand")) == brand_path.upper()).select(
        F.col("rate_date").alias("exchange_date"),
        F.col("currency_from").alias("from_currency"),
        F.col("currency_to").alias("to_currency"),
        F.col("exchange_value").alias("fx_rate"),
    )

    return df.filter(F.col("exchange_date").between(start_date, end_date))


# =============================================================================
# COLUMNAS FINALES (32) — orden idéntico al SP
# =============================================================================

FINAL_COLS = [
    "customer_code",
    "file_id",
    "row_id",
    "customer_country_code",
    "business_mode_code",
    "brand_code",
    "processing_date",
    "processing_month",
    "transaction_date",
    "transaction_month",
    "transaction_group_code",
    "transaction_type_id",
    "jurisdiction_code",
    "merchant_country_code",
    "mcc_code",
    "merchant",
    "merchant_id",
    "terminal_id",
    "acquirer_bin",
    "issuer_country_code",
    "issuer_bin_6",
    "issuer_bin_8",
    "funding_source_code",
    "product_program_id",
    "product_code",
    "card_present_code",
    "is_reversal_or_chargeback",
    "interchange_rule",
    "reported_currency_code",
    "transaction_amount",
    "interchange_fees_amount",
    "scheme_fees_amount",
]


def _apply_reversal_negation(df: DataFrame) -> DataFrame:
    """Negar transaction_amount e interchange_fees_amount en reversales."""
    return df.withColumn(
        "transaction_amount",
        F.when(
            F.col("is_reversal_or_chargeback"), F.col("transaction_amount") * -1
        ).otherwise(F.col("transaction_amount")),
    ).withColumn(
        "interchange_fees_amount",
        F.when(
            F.col("is_reversal_or_chargeback"), F.col("interchange_fees_amount") * -1
        ).otherwise(F.col("interchange_fees_amount")),
    )


def _apply_duplicate_on_us(df: DataFrame, brand: str) -> DataFrame:
    """
    Duplica las filas on-us con business_mode='A' (Acquirer) añadiendo
    una copia con business_mode_code='I' (Issuer).
    Equivalente al bloque duplicate_on_us_flag en el SP.
    """
    dup_suffix = f" ON-US DUP ({brand.upper()} ACQ TO ISS)"
    dup_df = df.filter(
        (F.col("jurisdiction_code") == "on-us") & (F.col("business_mode_code") == "A")
    ).withColumn("business_mode_code", F.lit("I"))
    return df.union(dup_df)


def _join_exchange_rates(
    df: DataFrame,
    xrate_df: DataFrame,
    src_currency_col: str,
    fee_currency_col: str,
    report_currency: str,
) -> DataFrame:
    """
    Añade columnas xr1_rate y xr2_rate al DataFrame.
      xr1_rate : tipo de cambio de la moneda de la transacción al report_currency
      xr2_rate : tipo de cambio de la moneda del fee al report_currency
    Join: date (operational) == exchange_date (reference)
    """
    xr = xrate_df.filter(F.col("to_currency") == report_currency)

    # X1: moneda transacción → report_currency
    x1 = xr.select(
        F.col("exchange_date").alias("_xr1_date"),
        F.col("from_currency").alias("_xr1_from"),
        F.col("fx_rate").alias("xr1_rate"),
    )
    df = df.join(
        x1,
        (df["date"] == x1["_xr1_date"]) & (df[src_currency_col] == x1["_xr1_from"]),
        how="left",
    ).drop("_xr1_date", "_xr1_from")

    # X2: moneda del fee → report_currency
    x2 = xr.select(
        F.col("exchange_date").alias("_xr2_date"),
        F.col("from_currency").alias("_xr2_from"),
        F.col("fx_rate").alias("xr2_rate"),
    )
    df = df.join(
        x2,
        (df["date"] == x2["_xr2_date"]) & (df[fee_currency_col] == x2["_xr2_from"]),
        how="left",
    ).drop("_xr2_date", "_xr2_from")

    return df


# =============================================================================
# TRANSFORM — VISA BASE II
# =============================================================================


def transform_visa_baseii(
    df: DataFrame,
    country_df: DataFrame,
    xrate_df: DataFrame,
    vi_bin_products_df: DataFrame,
    report_currency: str,
    dup_on_us: bool,
) -> DataFrame:
    """
    Equivalente a analytics.get_visa_baseii_transactions().

    Joins contra tablas de referencia:
      merchant_country_ref (country_df): merchant_country_code (2-letter)
                                          → country_code_alternative (3-letter)
      issuer_country_ref   (country_df): ardef_country (2-letter)
                                          → country_code_alternative (3-letter)
      product_ref (vi_bin_products_df): product_id → bin_product_id
                                         → range_program_id (product_program_id)
    """
    merchant_country_ref = country_df.select(
        F.col("country_code").alias("_country_code_merchant"),
        F.col("country_code_alternative").alias("merchant_country_alt"),
    )
    issuer_country_ref = country_df.select(
        F.col("country_code").alias("_country_code_issuer"),
        F.col("country_code_alternative").alias("issuer_country_alt"),
    )
    df = df.join(
        merchant_country_ref,
        df["merchant_country_code"] == merchant_country_ref["_country_code_merchant"],
        how="left",
    ).drop("_country_code_merchant")
    df = df.join(
        issuer_country_ref,
        df["ardef_country"] == issuer_country_ref["_country_code_issuer"],
        how="left",
    ).drop("_country_code_issuer")

    product_ref = vi_bin_products_df.select(
        F.col("bin_product_id").alias("_bin_product_id"),
        F.col("range_program_id").alias("product_program_id_raw"),
    )
    df = df.join(
        product_ref, df["product_id"] == product_ref["_bin_product_id"], how="left"
    ).drop("_bin_product_id")

    # -- Joins de tipo de cambio --
    df = _join_exchange_rates(
        df,
        xrate_df,
        src_currency_col="source_currency_code_alphabetic",
        fee_currency_col="interchange_fee_currency",
        report_currency=report_currency,
    )

    # -- Transformaciones de columnas --
    df = df.select(
        F.col("customer_code").cast(StringType()),
        F.col("file_id").cast(StringType()),
        F.col("record").cast(IntegerType()).alias("row_id"),
        # customer_country_code: Issuer → issuer_country_ref (ardef); Acquirer → merchant_country_ref
        F.when(F.col("business_mode") == "ISSUING", F.col("issuer_country_alt"))
        .when(F.col("business_mode") == "ACQUIRING", F.col("merchant_country_alt"))
        .cast(StringType())
        .alias("customer_country_code"),
        F.when(F.col("business_mode") == "ISSUING", F.lit("I"))
        .when(F.col("business_mode") == "ACQUIRING", F.lit("A"))
        .cast(StringType())
        .alias("business_mode_code"),
        F.lit("VI").alias("brand_code"),
        F.col("date").cast(DateType()).alias("processing_date"),
        F.date_format(F.col("date").cast(DateType()), "yyyy-MM").alias(
            "processing_month"
        ),
        F.col("purchase_date").cast(DateType()).alias("transaction_date"),
        F.date_format(F.col("purchase_date").cast(DateType()), "yyyy-MM").alias(
            "transaction_month"
        ),
        F.when(F.col("draft_code").isin("05", "25"), F.lit("PUR"))
        .when(F.col("draft_code").isin("06", "26"), F.lit("CRD"))
        .when(F.col("draft_code").isin("07", "27"), F.lit("CSH"))
        .otherwise(F.lit("OTH"))
        .alias("transaction_group_code"),
        F.col("business_transaction_type")
        .cast(StringType())
        .alias("transaction_type_id"),
        F.col("jurisdiction").cast(StringType()).alias("jurisdiction_code"),
        F.col("merchant_country_alt").cast(StringType()).alias("merchant_country_code"),
        F.col("merchant_category_code").cast(IntegerType()).alias("mcc_code"),
        F.trim(F.upper(F.col("merchant_name"))).cast(StringType()).alias("merchant"),
        F.trim(F.upper(F.col("card_acceptor_id")))
        .cast(StringType())
        .alias("merchant_id"),
        F.trim(F.upper(F.col("terminal_id"))).cast(StringType()).alias("terminal_id"),
        F.col("account_reference_number_acquiring_identifier")
        .cast(IntegerType())
        .alias("acquirer_bin"),
        F.col("issuer_country_alt").cast(StringType()).alias("issuer_country_code"),
        F.col("account_number").substr(1, 6).cast(IntegerType()).alias("issuer_bin_6"),
        F.col("issuer_bin_8").cast(IntegerType()).alias("issuer_bin_8"),
        F.col("funding_source").cast(StringType()).alias("funding_source_code"),
        F.col("product_program_id_raw").cast(IntegerType()).alias("product_program_id"),
        F.col("product_id").cast(StringType()).alias("product_code"),
        F.when(F.col("moto_ec_indicator") == " ", F.lit("CPR"))
        .when(F.col("moto_ec_indicator").rlike("^[1-9]$"), F.lit("CNP"))
        .otherwise(F.lit("UNK"))
        .alias("card_present_code"),
        F.col("reversal_indicator")
        .cast(BooleanType())
        .alias("is_reversal_or_chargeback"),
        F.col("interchange_fee_descriptor")
        .cast(StringType())
        .alias("interchange_rule"),
        F.lit(report_currency).alias("reported_currency_code"),
        # transaction_amount = source_amount * X1 (fallback 1.0)
        (F.coalesce(F.col("xr1_rate"), F.lit(1.0)) * F.col("source_amount"))
        .cast(DoubleType())
        .alias("transaction_amount"),
        # interchange_fees_amount = interchange_fee_amount * X2 (fallback X1, luego 1.0)
        (
            F.coalesce(F.col("xr2_rate"), F.col("xr1_rate"), F.lit(1.0))
            * F.col("interchange_fee_amount")
        )
        .cast(DoubleType())
        .alias("interchange_fees_amount"),
        F.lit(0.0).cast(DoubleType()).alias("scheme_fees_amount"),
    )

    df = _apply_reversal_negation(df)
    if dup_on_us:
        df = _apply_duplicate_on_us(df, brand="VISA")
    return df


# =============================================================================
# TRANSFORM — VISA SMS
# =============================================================================


def transform_visa_sms(
    df: DataFrame,
    country_df: DataFrame,
    xrate_df: DataFrame,
    vi_bin_products_df: DataFrame,
    report_currency: str,
    dup_on_us: bool,
) -> DataFrame:
    """
    Equivalente a analytics.get_visa_sms_transactions().

    NOTA: nombres de columna pendientes de verificar contra Parquet operational SMS.
    Los nombres marcados con # VERIFY son supuestos basados en el SP y el
    pipeline de clean; confirmar cuando haya un archivo SMS de muestra disponible.
    """
    merchant_country_ref = country_df.select(
        F.col("country_code").alias("_country_code_merchant"),
        F.col("country_code_alternative").alias("merchant_country_alt"),
    )
    issuer_country_ref = country_df.select(
        F.col("country_code").alias("_country_code_issuer"),
        F.col("country_code_alternative").alias("issuer_country_alt"),
    )
    # VERIFY: en SMS el país del merchant es card_acceptor_country
    df = df.join(
        merchant_country_ref,
        df["card_acceptor_country"] == merchant_country_ref["_country_code_merchant"],  # VERIFY col name
        how="left",
    ).drop("_country_code_merchant")
    df = df.join(
        issuer_country_ref,
        df["ardef_country"] == issuer_country_ref["_country_code_issuer"],
        how="left",
    ).drop("_country_code_issuer")

    product_ref = vi_bin_products_df.select(
        F.col("bin_product_id").alias("_bin_product_id"),
        F.col("range_program_id").alias("product_program_id_raw"),
    )
    df = df.join(
        product_ref, df["product_id"] == product_ref["_bin_product_id"], how="left"
    ).drop("_bin_product_id")

    # VERIFY: moneda de transacción en SMS es transaction_currency_code (numérico)
    # Como el exchange rate usa códigos alfabéticos, necesitamos el campo alphabético
    # Verificar si el Parquet SMS tiene source_currency_code_alphabetic o similar
    df = _join_exchange_rates(
        df,
        xrate_df,
        src_currency_col="source_currency_code_alphabetic",  # VERIFY
        fee_currency_col="interchange_fee_currency",  # VERIFY
        report_currency=report_currency,
    )

    df = df.select(
        F.col("customer_code").cast(StringType()),
        F.col("file_id").cast(StringType()),
        F.col("record").cast(IntegerType()).alias("row_id"),
        # En SMS: issuer_acquirer_indicator ya es I/A directamente (no derivado de file_type)
        # customer_country_code: I → issuer_country_ref; A → merchant_country_ref
        F.when(F.col("business_mode") == "ISSUING", F.col("issuer_country_alt"))
        .when(F.col("business_mode") == "ACQUIRING", F.col("merchant_country_alt"))
        .cast(StringType())
        .alias("customer_country_code"),
        F.when(F.col("business_mode") == "ISSUING", F.lit("I"))
        .when(F.col("business_mode") == "ACQUIRING", F.lit("A"))
        .cast(StringType())
        .alias("business_mode_code"),
        F.lit("VI").alias("brand_code"),
        F.col("date").cast(DateType()).alias("processing_date"),
        F.date_format(F.col("date").cast(DateType()), "yyyy-MM").alias(
            "processing_month"
        ),
        F.col("local_transaction_date")
        .cast(DateType())
        .alias("transaction_date"),  # VERIFY col name
        F.date_format(
            F.col("local_transaction_date").cast(DateType()), "yyyy-MM"
        ).alias("transaction_month"),  # VERIFY
        F.when(
            F.col("transaction_code_sms").isin("05", "25"), F.lit("PUR")
        )  # VERIFY col name
        .when(F.col("transaction_code_sms").isin("06", "26"), F.lit("CRD"))
        .when(F.col("transaction_code_sms").isin("07", "27"), F.lit("CSH"))
        .otherwise(F.lit("OTH"))
        .alias("transaction_group_code"),
        F.col("business_transaction_type")
        .cast(StringType())
        .alias("transaction_type_id"),
        F.col("jurisdiction").cast(StringType()).alias("jurisdiction_code"),
        F.col("merchant_country_alt").cast(StringType()).alias("merchant_country_code"),
        F.col("merchants_type")
        .cast(IntegerType())
        .alias("mcc_code"),  # VERIFY col name
        F.trim(F.upper(F.col("card_acceptor_name")))
        .cast(StringType())
        .alias("merchant"),  # VERIFY
        F.trim(F.upper(F.col("card_acceptor_id_sms")))
        .cast(StringType())
        .alias("merchant_id"),  # VERIFY
        F.trim(F.upper(F.col("card_acceptor_terminal_id")))
        .cast(StringType())
        .alias("terminal_id"),  # VERIFY
        F.col("acquiring_institution_id_1")
        .cast(IntegerType())
        .alias("acquirer_bin"),  # VERIFY
        F.col("issuer_country_alt").cast(StringType()).alias("issuer_country_code"),
        F.col("card_number")
        .substr(1, 6)
        .cast(IntegerType())
        .alias("issuer_bin_6"),  # VERIFY
        F.col("issuer_bin_8").cast(IntegerType()).alias("issuer_bin_8"),
        F.col("funding_source").cast(StringType()).alias("funding_source_code"),
        F.col("product_program_id_raw").cast(IntegerType()).alias("product_program_id"),
        F.col("product_id").cast(StringType()).alias("product_code"),
        F.when(
            F.col("mailtelephone_or_electronic_commerce_indicator") == " ", F.lit("CPR")
        )  # VERIFY col name
        .when(
            F.col("mailtelephone_or_electronic_commerce_indicator").rlike("^[1-9]$"),
            F.lit("CNP"),
        )
        .otherwise(F.lit("UNK"))
        .alias("card_present_code"),
        F.col("reversal_indicator")
        .cast(BooleanType())
        .alias("is_reversal_or_chargeback"),
        F.col("interchange_fee_descriptor")
        .cast(StringType())
        .alias("interchange_rule"),
        F.lit(report_currency).alias("reported_currency_code"),
        # SMS tiene lógica especial: si transaction_amount=0 usa cryptogram_amount + surcharge_amount_sms
        F.when(
            F.col("transaction_amount") == 0,
            F.coalesce(F.col("xr1_rate"), F.lit(1.0))
            * (
                F.col("cryptogram_amount")
                + F.col("surcharge_amount_sms")  # VERIFY col names
            ),
        )
        .otherwise(
            F.coalesce(F.col("xr1_rate"), F.lit(1.0))
            * F.col("transaction_amount")  # VERIFY
        )
        .cast(DoubleType())
        .alias("transaction_amount"),
        (
            F.coalesce(F.col("xr2_rate"), F.col("xr1_rate"), F.lit(1.0))
            * F.col("interchange_fee_amount")
        )
        .cast(DoubleType())
        .alias("interchange_fees_amount"),  # VERIFY
        F.lit(0.0).cast(DoubleType()).alias("scheme_fees_amount"),
    )

    df = _apply_reversal_negation(df)
    if dup_on_us:
        df = _apply_duplicate_on_us(df, brand="VISA")
    return df


# =============================================================================
# TRANSFORM — MASTERCARD (MTI 1240 y 1442)
# =============================================================================


def transform_mastercard(
    df: DataFrame,
    currency_df: DataFrame,
    xrate_df: DataFrame,
    mc_bin_products_df: DataFrame,
    report_currency: str,
    dup_on_us: bool,
) -> DataFrame:
    """
    Equivalente a analytics.get_mastercard_transactions() para MTI 1240 y 1442.

    A diferencia de Visa, el operational de MC ya trae los países en alpha-3
    (card_acceptor_country_code_de_43_6 = merchant, iar_country = issuer) —
    no requiere join contra country_df.

    Las monedas (currency_code_transaction_de_49 / currency_code_
    reconciliation_de_50) son ISO 4217 numéricos — se mapean a alfabético
    via currency_alpha_ref (currency_df) para el join de exchange_rate.
    rate_currency (fee IAR) ya viene alfabético.

    product_ref (mc_bin_products_df): gcms_product_identifier → bin_product_id
    → range_program_id (product_program_id), igual patrón que Visa con
    visa_bin_products.

    Swap de columna de país según MTI (is_1442) e is_reversal_or_chargeback
    invertido: ver Pendientes (TODO) en el docstring del módulo.
    """
    product_ref = mc_bin_products_df.select(
        F.col("bin_product_id").alias("_bin_product_id"),
        F.col("range_program_id").alias("product_program_id_raw"),
    )
    df = df.join(
        product_ref,
        df["gcms_product_identifier"] == product_ref["_bin_product_id"],
        how="left",
    ).drop("_bin_product_id")

    # currency_alpha_ref: moneda numérica (ISO 4217) → alfabética, misma condición IN/OUT que el monto
    currency_alpha_ref = currency_df.select(
        F.col("currency_numeric_code").alias("_currency_numeric"),
        F.col("currency_alphabetic_code").alias("src_currency_alpha"),
    )
    df = df.withColumn(
        "_src_currency_numeric",
        F.lpad(
            F.when(
                F.col("file_type") == "IN", F.col("currency_code_reconciliation_de_50")
            )
            .otherwise(F.col("currency_code_transaction_de_49"))
            .cast(StringType()),
            3,
            "0",
        ),
    )
    df = df.join(
        currency_alpha_ref,
        df["_src_currency_numeric"] == currency_alpha_ref["_currency_numeric"],
        how="left",
    ).drop("_currency_numeric", "_src_currency_numeric")

    # -- Join de tipo de cambio --
    df = _join_exchange_rates(
        df,
        xrate_df,
        src_currency_col="src_currency_alpha",
        fee_currency_col="rate_currency",
        report_currency=report_currency,
    )

    # Lógica MTI 1442: customer_country invierte la fuente de país respecto a 1240
    is_1442 = F.col("type_mti") == "1442"

    df = df.select(
        F.col("customer_code").cast(StringType()),
        F.col("file_id").cast(StringType()),
        F.col("ref_id").cast(IntegerType()).alias("row_id"),
        F.when((F.col("file_type") == "IN") & ~is_1442, F.col("iar_country"))
        .when(
            (F.col("file_type") == "IN") & is_1442,
            F.col("card_acceptor_country_code_de_43_6"),
        )
        .when(
            (F.col("file_type") == "OUT") & ~is_1442,
            F.col("card_acceptor_country_code_de_43_6"),
        )
        .when((F.col("file_type") == "OUT") & is_1442, F.col("iar_country"))
        .cast(StringType())
        .alias("customer_country_code"),
        F.when(F.col("file_type") == "IN", F.lit("I"))
        .when(F.col("file_type") == "OUT", F.lit("A"))
        .cast(StringType())
        .alias("business_mode_code"),
        F.lit("MC").alias("brand_code"),
        F.col("date").cast(DateType()).alias("processing_date"),
        F.date_format(F.col("date").cast(DateType()), "yyyy-MM").alias(
            "processing_month"
        ),
        F.col("date_de_12_1").cast(DateType()).alias("transaction_date"),
        F.date_format(F.col("date_de_12_1").cast(DateType()), "yyyy-MM").alias(
            "transaction_month"
        ),
        F.when(
            F.col("processing_code_de_3").substr(1, 2).isin("00", "09", "18"),
            F.lit("PUR"),
        )
        .when(F.col("processing_code_de_3").substr(1, 2) == "20", F.lit("CRD"))
        .when(
            F.col("processing_code_de_3").substr(1, 2).isin("01", "12", "17", "28", "50"),
            F.lit("CSH"),
        )
        .otherwise(F.lit("OTH"))
        .alias("transaction_group_code"),
        F.col("processing_code_de_3")
        .substr(1, 2)
        .cast(StringType())
        .alias("transaction_type_id"),
        F.col("jurisdiction").cast(StringType()).alias("jurisdiction_code"),
        # merchant_country: 1240 → card_acceptor_country; 1442 → iar_country
        F.when(~is_1442, F.col("card_acceptor_country_code_de_43_6"))
        .when(is_1442, F.col("iar_country"))
        .cast(StringType())
        .alias("merchant_country_code"),
        F.col("card_acceptor_business_code_[mcc]_de_26")
        .cast(IntegerType())
        .alias("mcc_code"),
        F.trim(F.upper(F.col("card_acceptor_name_de_43_1")))
        .cast(StringType())
        .alias("merchant"),
        F.trim(F.upper(F.col("card_acceptor_id_code_de_42")))
        .cast(StringType())
        .alias("merchant_id"),
        F.trim(F.upper(F.col("card_acceptor_terminal_id_de_41")))
        .cast(StringType())
        .alias("terminal_id"),
        F.when(
            F.trim(F.col("acquirer_reference_data_de_31")) == "",
            F.lit(None).cast(IntegerType()),
        )
        .otherwise(
            F.col("acquirer_reference_data_de_31").substr(2, 6).cast(IntegerType())
        )
        .alias("acquirer_bin"),
        # issuer_country: 1240 → iar_country; 1442 → card_acceptor_country
        F.when(~is_1442, F.col("iar_country"))
        .when(is_1442, F.col("card_acceptor_country_code_de_43_6"))
        .cast(StringType())
        .alias("issuer_country_code"),
        F.col("pan_de_2").substr(1, 6).cast(IntegerType()).alias("issuer_bin_6"),
        F.col("pan_de_2").substr(1, 8).cast(IntegerType()).alias("issuer_bin_8"),
        F.col("funding_source").cast(StringType()).alias("funding_source_code"),
        F.col("product_program_id_raw").cast(IntegerType()).alias("product_program_id"),
        F.col("gcms_product_identifier").cast(StringType()).alias("product_code"),
        F.when(F.col("pos_entry_mode_de_22").substr(6, 1) == "1", F.lit("CPR"))
        .when(F.col("pos_entry_mode_de_22").substr(6, 1) == "0", F.lit("CNP"))
        .otherwise(F.lit("UNK"))
        .alias("card_present_code"),
        # is_reversal_or_chargeback: lógica diferente según MTI
        F.when(
            (~is_1442)
            & F.col("message_reversal_indicator_pds_25").isNotNull()
            & (F.col("message_reversal_indicator_pds_25") != ""),
            F.lit(True),
        )
        .when(
            is_1442
            & (
                F.col("message_reversal_indicator_pds_25").isNull()
                | (F.col("message_reversal_indicator_pds_25") == "")
            ),
            F.lit(True),
        )
        .otherwise(F.lit(False))
        .cast(BooleanType())
        .alias("is_reversal_or_chargeback"),
        F.concat(F.col("jurisdiction_assigned"), F.lit("-"), F.col("ird"))
        .cast(StringType())
        .alias("interchange_rule"),
        F.lit(report_currency).alias("reported_currency_code"),
        # transaction_amount: IN usa amount_reconciliation; OUT usa amount_transaction
        (
            F.coalesce(F.col("xr1_rate"), F.lit(1.0))
            * F.when(
                F.col("file_type") == "IN", F.col("amount_reconciliation_de_5")
            ).otherwise(F.col("amount_transaction_de_4"))
        )
        .cast(DoubleType())
        .alias("transaction_amount"),
        # interchange_fees_amount: IN usa amounts_transaction_fee_7; OUT usa calculated_value (ITX)
        (
            F.coalesce(F.col("xr2_rate"), F.col("xr1_rate"), F.lit(1.0))
            * F.when(
                F.col("file_type") == "IN", F.col("amounts_transaction_fee_7_pds_146_7")
            ).otherwise(F.col("calculated_value"))
        )
        .cast(DoubleType())
        .alias("interchange_fees_amount"),
        F.lit(0.0).cast(DoubleType()).alias("scheme_fees_amount"),
    )

    df = _apply_reversal_negation(df)
    if dup_on_us:
        df = _apply_duplicate_on_us(df, brand="MC")
    return df


# =============================================================================
# PROCESAMIENTO POR CLIENTE Y RANGO DE FECHAS
# =============================================================================


def process_client_range(
    client_id: str,
    start_date: str,
    end_date: str,
    country_df: DataFrame,
    xrate_vi_df: DataFrame,
    vi_bin_products_df: DataFrame,
    currency_df: DataFrame,
    xrate_mc_df: DataFrame,
    mc_bin_products_df: DataFrame,
    client_cfg: dict,
) -> DataFrame | None:
    """
    Genera el DataFrame de report_transactions para un cliente en el rango
    [start_date, end_date]. Une BASEII + SMS + MC_1240 + MC_1442.
    """
    rep_cur = client_cfg["report_currency"]
    dup_vi = client_cfg["dup_on_us_visa"]
    dup_mc = client_cfg["dup_on_us_mc"]
    frames = []

    # -- Visa BASE II --
    df_baseii = read_operational(
        client_id, "VISA", "baseii_drafts", start_date, end_date
    )
    if df_baseii is not None:
        try:
            frames.append(
                transform_visa_baseii(
                    df_baseii, country_df, xrate_vi_df, vi_bin_products_df, rep_cur, dup_vi
                )
            )
        except Exception as e:
            log_error(f"[BASEII] {client_id}/{start_date}_{end_date}: {e}")

    # -- Visa SMS --
    df_sms = read_operational(client_id, "VISA", "sms_messages", start_date, end_date)
    if df_sms is not None:
        try:
            frames.append(
                transform_visa_sms(
                    df_sms, country_df, xrate_vi_df, vi_bin_products_df, rep_cur, dup_vi
                )
            )
        except Exception as e:
            log_error(f"[SMS] {client_id}/{start_date}_{end_date}: {e}")

    # -- MC MTI 1240 --
    df_mc1240 = read_operational(client_id, "MC", "IPM_1240", start_date, end_date)
    if df_mc1240 is not None:
        try:
            frames.append(
                transform_mastercard(
                    df_mc1240, currency_df, xrate_mc_df, mc_bin_products_df, rep_cur, dup_mc
                )
            )
        except Exception as e:
            log_error(f"[MC 1240] {client_id}/{start_date}_{end_date}: {e}")

    # -- MC MTI 1442 --
    df_mc1442 = read_operational(client_id, "MC", "IPM_1442", start_date, end_date)
    if df_mc1442 is not None:
        try:
            frames.append(
                transform_mastercard(
                    df_mc1442, currency_df, xrate_mc_df, mc_bin_products_df, rep_cur, dup_mc
                )
            )
        except Exception as e:
            log_error(f"[MC 1442] {client_id}/{start_date}_{end_date}: {e}")

    if not frames:
        log_info(
            f"[process_client_range] No data for {client_id}/{start_date}_{end_date}"
        )
        return None

    result = frames[0]
    for f in frames[1:]:
        result = result.union(f)

    return result.select(FINAL_COLS)


def write_result(df: DataFrame, client_id: str) -> str:
    """
    Escribe el DataFrame resultado como Parquet en S3 analytics.
    Nombre: report_transactions_{client_id}_{report_suffix}.parquet
    Path:   {bucket}/{client_id}/reports/
    """
    filename = f"report_transactions_{client_id}_{REPORT_SUFFIX}.parquet"

    s3_key = f"{client_id}/reports/{filename}"
    s3_path = f"s3://{ANALYTICS_BUCKET}/{s3_key}"

    log_info(f"[write_result] Writing {df.count()} rows → {s3_path}")

    # Escribir como un solo archivo (repartition a 1)
    df.repartition(1).write.mode("overwrite").parquet(s3_path)

    log_info(f"[write_result] Done → {s3_path}")
    return s3_path


# =============================================================================
# MAIN
# =============================================================================


def main():
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    log_info("=" * 70)
    log_info("glue-vi-mc-reporting Start")
    log_info(f"  client        : {CLIENT_CODE}")
    log_info(f"  range         : {START_DATE} -> {END_DATE}")
    log_info(f"  report_suffix : {REPORT_SUFFIX}")
    log_info(f"  scheme_fee    : {SCHEME_FEE}")
    log_info("=" * 70)

    # Tablas de referencia (cache, se reutilizan en cada transform)
    # -- Visa --
    country_df = load_country().cache()
    xrate_vi_df = load_exchange_rates(START_DATE, END_DATE, brand_path="Visa").cache()
    vi_bin_products_df = load_visa_bin_products().cache()
    # -- Mastercard --
    currency_df = load_currency().cache()
    xrate_mc_df = load_exchange_rates(START_DATE, END_DATE, brand_path="MasterCard").cache()
    mc_bin_products_df = load_mastercard_bin_products().cache()

    client_cfg = get_client_config(CLIENT_CODE)
    log_info(f"  report_currency : {client_cfg['report_currency']}")
    log_info(f"  dup_on_us_visa  : {client_cfg['dup_on_us_visa']}")
    log_info(f"  dup_on_us_mc    : {client_cfg['dup_on_us_mc']}")

    result_df = process_client_range(
        client_id=CLIENT_CODE,
        start_date=START_DATE,
        end_date=END_DATE,
        country_df=country_df,
        xrate_vi_df=xrate_vi_df,
        vi_bin_products_df=vi_bin_products_df,
        currency_df=currency_df,
        xrate_mc_df=xrate_mc_df,
        mc_bin_products_df=mc_bin_products_df,
        client_cfg=client_cfg,
    )

    if result_df is not None:
        s3_path = write_result(result_df, CLIENT_CODE)
        log_info(f"Written: {s3_path}")
    else:
        log_info(f"No data for {CLIENT_CODE} in [{START_DATE}, {END_DATE}], skipping")

    log_info("=" * 70)
    log_info("glue-vi-mc-reporting DONE")
    log_info("=" * 70)

    job.commit()


if __name__ == "__main__":
    main()
