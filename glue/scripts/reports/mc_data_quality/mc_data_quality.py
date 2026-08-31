
"""
mc_data_quality.py — Job real: itl-0004-itx-{env}-intchg-02-glue-mc-data-quality
================================================================================
Archivo:     glue/scripts/reports/mc_data_quality/mc_data_quality.py
S3 Script:   s3://itl-0004-itx-dev-intchg-02-s3-reference/glue/scripts/report/mc_data_quality.py

Replica en PySpark la lógica de Data Quality Mastercard del SP legacy en
PostgreSQL: compara, para un rango de fechas y un cliente, los datos
transaccionales (operational IPM_1240) contra los datos de liquidación
reportados por la marca (operational IPM_1644, function_code=685) —
agrupados por las mismas dimensiones de negocio (fecha, tipo de mensaje,
business_mode, jurisdiction, moneda, trx_type, ird) para poder comparar
conteos y montos lado a lado en un reporte único. Aún no integrado a
ningún Step Function (ver CLAUDE.md → tabla de Glue Jobs) — se ejecuta
manualmente, nunca corrido en producción (ver decisions.md/gotchas.md).

Cada fuente (settlement, transactional) tiene su propia función que arma
un DataFrame normalizado y agrega con una consulta SQL vía Spark SQL
(temp views), replicando columna por columna las expresiones CASE/JOIN del
SQL original. El combinado (unionByName de ambas fuentes) se calcula día
por día en un rango [start_date, end_date] — un fallo en un día individual
no aborta el rango completo, se loguea y se omite (ver
get_mastercard_validation_results_range).

Particularidad SBSA: aplica un filtro adicional de hash de archivo
(hash_file_filter) contra DynamoDB (tabla file_control, equivalente Standard
2.0 de la tabla legacy control.t_control_file) — ver get_control_hash_codes().
Mismo patrón/tabla que usa vi_data_quality.py::get_valid_content_hashes().
Para el resto de los clientes este filtro no aplica.

Flujo:
1. get_mastercard_validation_results_range(): itera día por día el rango
2. Por día: get_mastercard_validation_results_combined() = settlement UNION transactional
3. get_mastercard_validation_results_settlement(): lee IPM_1644 (function_code=685),
   normaliza y agrega vía SQL contra business_transaction_type/currency/exchange-rates-glue
4. get_mastercard_validation_results_transactional(): lee IPM_1240,
   normaliza y agrega vía SQL contra las mismas 3 referencias
5. Escribe el resultado combinado del rango completo a s3-analytics

Job Parameters:
  --CUSTOMER_CODE               Código de cliente único, ej: "EBGR"
  --START_DATE                  Inicio del rango en formato YYYY-MM-DD (inclusive)
  --END_DATE                    Fin del rango en formato YYYY-MM-DD (inclusive)
  --ISSUER_ACQUIRER_INDICATOR   "I" (Issuer/IN) o "A" (Acquirer/OUT) — determina file_type
  --BRAND_LOCAL_INDICATOR       "brand" o "default" — filtro de hash_file_filter para SBSA

Variables de entorno (no son Job Parameters, se leen con os.getenv):
  S3_REFERENCE             : bucket de referencia (default: s3a://itl-0004-itx-dev-intchg-02-s3-reference)
  S3_OPERATIONAL           : bucket operational (default: s3a://itl-0004-itx-dev-intchg-02-s3-operational)
  S3_OUTPUT                : bucket de salida del reporte (default: s3a://itl-0004-itx-dev-intchg-02-s3-analytics)
  DYNAMO_TABLE_CLIENT      : tabla DynamoDB de clientes (default: itl-0004-itx-dev-dynamo-client-02)
  AWS_REGION               : región AWS (default: eu-south-2)
  DYNAMO_TABLE_FILE_CONTROL: tabla DynamoDB de control de archivos, usada por
                             hash_file_filter (solo SBSA, default: itl-0004-itx-dev-dynamo-file_control-02)
  SPARK_SHUFFLE_PARTITIONS : particiones de shuffle Spark (default: 32)

Salida:
  S3 analytics: {S3_OUTPUT}/{customer_code}/reports/quality/MC/{YYYYMM}/{archivo}.parquet
  (carpeta MC agregada 2026-08-12 para diferenciar por marca, igual convención
  que ya usa el bucket operational — {cliente}/MC/... y {cliente}/VISA/...)
  Nombre de archivo (ver build_output_names()): si el rango cubre el mes
  calendario completo, "{customer_code}_data_quality_{YYYYMM}.parquet";
  si es un rango parcial, "{customer_code}_data_quality_{YYYYMMDD}_{YYYYMMDD}.parquet"
  (agregado 2026-08-12 junto con write_single_named_parquet(), que reemplaza
  el nombre autogenerado por Spark, part-00000-<uuid>-c000.snappy.parquet,
  por este nombre legible).
"""
import os
import time
from calendar import monthrange
from decimal import Decimal
from typing import Any, List
from functools import reduce

from datetime import datetime, timedelta

import boto3
from boto3.dynamodb.conditions import Attr
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, DecimalType

from awsglue.utils import getResolvedOptions
import sys

# =============================================================================
# CONFIG TÉCNICA
# =============================================================================
args = getResolvedOptions(
    sys.argv,
    [
        'CUSTOMER_CODE',
        'START_DATE',
        'END_DATE',
        'ISSUER_ACQUIRER_INDICATOR',
        'BRAND_LOCAL_INDICATOR'
    ]
)

customer_code = args['CUSTOMER_CODE']
start_date = args['START_DATE']
end_date = args['END_DATE']
issuer_acquirer_indicator = args['ISSUER_ACQUIRER_INDICATOR']
brand_local_indicator = args['BRAND_LOCAL_INDICATOR']

DEBUG = False

S3_REFERENCE = os.getenv(
    "S3_REFERENCE",
    "s3a://itl-0004-itx-dev-intchg-02-s3-reference",
)

S3_OPERATIONAL = os.getenv(
    "S3_OPERATIONAL",
    "s3a://itl-0004-itx-dev-intchg-02-s3-operational",
)

S3_OUTPUT = os.getenv(
    "S3_OUTPUT",
    "s3a://itl-0004-itx-dev-intchg-02-s3-analytics",
)

DYNAMO_TABLE_CLIENT = os.getenv(
    "DYNAMO_TABLE_CLIENT",
    "itl-0004-itx-dev-dynamo-client-02",
)

AWS_REGION = os.getenv("AWS_REGION", "eu-south-2")

# Solo necesario para SBSA, usado por hash_file_filter (ver get_control_hash_codes).
# Equivalente Standard 2.0 de la tabla legacy control.t_control_file.
DYNAMO_TABLE_FILE_CONTROL = os.getenv(
    "DYNAMO_TABLE_FILE_CONTROL",
    "itl-0004-itx-dev-dynamo-file_control-02",
)


# =============================================================================
# COLUMNAS 1644 - SETTLEMENT
# =============================================================================

COL_STL_TXN_FUNC = "reconciled_transaction_function_1_pds_372_1"
COL_STL_MEMBER_ACTIVITY = "reconciled_member_activity_pds_302"
COL_STL_PROCESSING_CODE = "reconciled_processing_code_pds_374"
COL_STL_SETTLEMENT_INDICATOR = "settlement_indicator_1_pds_165_1"
COL_STL_BUSINESS_ACTIVITY_2 = "reconciled_business_activity_2_pds_358_2"
COL_STL_BUSINESS_ACTIVITY_4 = "reconciled_business_activity_4_pds_358_4"
COL_STL_CURRENCY_RECON = "currency_code_reconciliation_de_50"
COL_STL_TOTAL_TRX_NUMBER = "total_transaction_number_pds_402"
COL_STL_AMT_NET_TRX_RECON = "amount_net_transaction_in_reconciliation_currency_2_pds_394_2"
COL_STL_AMT_NET_FEE_RECON = "amount_net_fee_in_reconciliation_currency_2_pds_395_2"
COL_STL_MSG_REASON = "message_reason_code_de_25"
COL_STL_RECONCILED_FILE = "reconciled_file_pds_300"
COL_STL_ECI = "electronic_commerce_indicator_3_pds_52_3"


# =============================================================================
# COLUMNAS 1240 - TRANSACTIONAL
# =============================================================================

COL_TRX_FILE_DATE = "file_dt"
COL_TRX_HASH = "content_hash"
COL_TRX_APP_ID = "ref_id"
COL_TRX_MTI = "type_mti"
COL_TRX_PROCESSING_CODE = "processing_code_de_3"
COL_TRX_SETTLEMENT_INDICATOR = "settlement_indicator_1_pds_165_1"
COL_TRX_BUSINESS_ACTIVITY_4 = "business_activity_4_pds_158_4"
COL_TRX_MSG_REASON = "message_reason_code_de_25"
COL_TRX_ECI = "electronic_commerce_indicator_3_pds_52_3"
COL_TRX_AMOUNT_RECON = "amount_reconciliation_de_5"
COL_TRX_AMOUNT_TRANSACTION = "amount_transaction_de_4"
COL_TRX_FEE_7 = "amounts_transaction_fee_7_pds_146_7"
COL_TRX_CURRENCY_RECON = "currency_code_reconciliation_de_50"
COL_TRX_CURRENCY_TRANSACTION = "currency_code_transaction_de_49"

# Campos ya enriquecidos en operational/IPM_1240
COL_TRX_JURISDICTION = "jurisdiction"
COL_TRX_SETTLEMENT_REPORT_CURRENCY = "settlement_report_currency_code"
COL_TRX_INTELICA_ID = "intelica_id"
COL_TRX_CALCULATED_VALUE = "calculated_value"


# =============================================================================
# HELPERS
# =============================================================================

def read_ipm_1644_operational(spark, path: str) -> DataFrame:
    """
    Lee el Parquet operational de MTI 1644 (settlement) con un schema Arrow/
    Spark explícito, para evitar que Spark infiera tipos inconsistentes entre
    archivos (ver decisions.md → "Por qué lmbd-mc-store restaura el schema
    Arrow del CLN..." — mismo problema que motivó el schema explícito en la
    capa operational).

    IMPORTANTE (bug encontrado 2026-08-11): los campos amount_net_fee_* y
    amount_net_transaction_* son decimal(18,2) en el parquet real, pero
    estaban declarados acá como DecimalType(6,2)/(9,2) — mucho más angostos.
    Con spark.sql.parquet.enableVectorizedReader=false (como corre el job
    real, ver __main__), Spark trunca/corrompe en silencio, sin error ni
    warning, cualquier valor que no entre en la precisión declarada — no
    lanza excepción (eso solo pasa con el lector vectorizado activado).
    Confirmado empíricamente: para EBGR 2026-01-05, esto perdía 40,913.04 de
    la suma de fees de un solo archivo (2 filas de 3 dígitos enteros nomás,
    ej. 11,231.49) porque no entraban en el límite de 4 dígitos enteros de
    DecimalType(6,2). Esto explicaba gran parte de lo que se había atribuido
    a "discrepancia real de negocio" en el reporte de calidad — no lo era.
    Ahora ambos campos usan DecimalType(18,2), igual al tipo nativo real.

    Args:
        spark: Sesión de Spark activa.
        path: Path S3 al directorio operational de IPM_1644 a leer
            ({S3_OPERATIONAL}/{customer_code}/MC/IPM_1644/file_type=IN/date={query_date}/).

    Returns:
        DataFrame con el schema fijo declarado (18 columnas — llaves,
        columnas reconciled_*, content_hash, file_id, file_processing_date).

    Ejemplo:
        read_ipm_1644_operational(spark, "s3a://.../EBGR/MC/IPM_1644/file_type=IN/date=2026-01-03/")
    """
    schema = StructType([
        StructField("file_idn", StringType(), True),
        StructField("file_dt", StringType(), True),
        StructField("type_mti", StringType(), True),
        StructField("ref_id", StringType(), True),
        StructField("function_code", LongType(), True),

        StructField("currency_code_reconciliation_de_50", LongType(), True),
        StructField("message_reason_code_de_25", LongType(), True),

        StructField(
            "amount_net_fee_in_reconciliation_currency_2_pds_395_2",
            DecimalType(18, 2),
            True,
        ),
        StructField(
            "amount_net_transaction_in_reconciliation_currency_2_pds_394_2",
            DecimalType(18, 2),
            True,
        ),

        StructField("reconciled_business_activity_2_pds_358_2", StringType(), True),
        StructField("reconciled_business_activity_4_pds_358_4", StringType(), True),
        StructField("reconciled_file_pds_300", StringType(), True),
        StructField("reconciled_member_activity_pds_302", StringType(), True),
        StructField("reconciled_processing_code_pds_374", StringType(), True),
        StructField("reconciled_transaction_function_1_pds_372_1", LongType(), True),
        StructField("settlement_indicator_1_pds_165_1", StringType(), True),
        StructField("total_transaction_number_pds_402", LongType(), True),

        StructField("content_hash", StringType(), True),
        StructField("file_id", StringType(), True),
        StructField("file_processing_date", StringType(), True),
    ])

    print(f"[READ] IPM_1644 with explicit schema: {path}", flush=True)

    return (
        spark.read
        .schema(schema)
        .option("mergeSchema", "false")
        .parquet(path)
    )
    
def read_ipm_1240_operational(spark, path: str) -> DataFrame:
    """
    Lee el Parquet operational de MTI 1240 (transaccional) con un schema
    Arrow/Spark explícito, incluyendo las columnas ya enriquecidas por el
    pipeline (jurisdiction, settlement_report_currency_code, intelica_id,
    calculated_value) — mismo motivo que read_ipm_1644_operational().

    IMPORTANTE (mismo bug que read_ipm_1644_operational, ver su docstring):
    amount_reconciliation_de_5, amount_transaction_de_4 y
    amounts_transaction_fee_7_pds_146_7 son decimal(18,4) en el parquet real,
    pero estaban declarados como DecimalType(8,4)/(10,4)/(6,4) — el de fee
    (6,4) apenas soportaba hasta 99.9999, cualquier comisión de más de 2
    dígitos enteros se corrompía en silencio. Confirmado empíricamente para
    EBGR 2026-01-05: perdía 10,215.16 en amount_reconciliation (1 fila) y
    225.88 en fee (2 filas) de un solo día. Ahora los 3 usan DecimalType(18,4).

    Args:
        spark: Sesión de Spark activa.
        path: Path S3 al directorio operational de IPM_1240 a leer
            ({S3_OPERATIONAL}/{customer_code}/MC/IPM_1240/file_type={file_type}/date={query_date}/).

    Returns:
        DataFrame con el schema fijo declarado (20 columnas).

    Ejemplo:
        read_ipm_1240_operational(spark, "s3a://.../EBGR/MC/IPM_1240/file_type=IN/date=2026-01-03/")
    """
    schema = StructType([
        StructField("file_idn", StringType(), True),
        StructField("file_dt", StringType(), True),
        StructField("type_mti", StringType(), True),
        StructField("ref_id",  StringType(), True),
        StructField("function_code", LongType(), True),
        StructField("content_hash", StringType(), True),

        StructField("processing_code_de_3", StringType(), True),
        StructField("settlement_indicator_1_pds_165_1", StringType(), True),
        StructField("business_activity_4_pds_158_4", StringType(), True),
        StructField("message_reason_code_de_25", LongType(), True),
        StructField("electronic_commerce_indicator_3_pds_52_3", StringType(), True),

        StructField("amount_reconciliation_de_5", DecimalType(18, 4), True),
        StructField("amount_transaction_de_4", DecimalType(18, 4), True),
        StructField("amounts_transaction_fee_7_pds_146_7", DecimalType(18, 4), True),

        StructField("currency_code_reconciliation_de_50", LongType(), True),
        StructField("currency_code_transaction_de_49", LongType(), True),

        StructField("jurisdiction", StringType(), True),
        StructField("settlement_report_currency_code", StringType(), True),
        StructField("intelica_id", StringType(), True),
        StructField("calculated_value", DoubleType(), True),
    ])
    
    print(f"[READ] IPM_1240 with explicit schema: {path}", flush=True)

    return (
        spark.read
        .schema(schema)
        .option("mergeSchema", "false")
        .parquet(path)
    )
    
def clean_decimal(value: Any) -> Any:
    """
    Convierte un valor Decimal de DynamoDB a int (si es un entero exacto) o
    float, para que sea serializable/comparable sin el tipo Decimal de
    Python. Cualquier otro tipo se devuelve sin cambios.

    Args:
        value: Valor a convertir, típicamente un atributo de un item de
            DynamoDB.

    Returns:
        int, float, o el valor original sin cambios si no es Decimal.

    Ejemplo:
        clean_decimal(Decimal('4.0'))  # 4
        clean_decimal(Decimal('4.5'))  # 4.5
    """
    if isinstance(value, Decimal):
        return int(value) if value % 1 == 0 else float(value)
    return value


def get_client_from_dynamo(customer_code: str) -> dict:
    """
    Consulta en DynamoDB (tabla client) la configuración de un cliente por
    su client_id, convirtiendo todos los valores Decimal a int/float con
    clean_decimal().

    Args:
        customer_code: Código de cliente, ej. "EBGR".

    Returns:
        Dict con los atributos del cliente (incluye report_currency_code,
        usado en ambas funciones de validación).

    Raises:
        ValueError: si no existe un item para customer_code en la tabla.

    Ejemplo:
        get_client_from_dynamo("EBGR")  # {'client_id': 'EBGR', 'report_currency_code': 'EUR', ...}
    """
    ddb = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = ddb.Table(DYNAMO_TABLE_CLIENT)

    resp = table.get_item(Key={"client_id": customer_code})
    item = resp.get("Item")

    if not item:
        raise ValueError(f"No existe cliente en DynamoDB: {customer_code}")

    return {k: clean_decimal(v) for k, v in item.items()}


def read_parquet(spark, path: str, name: str) -> DataFrame:
    """
    Lee un parquet desde S3 sin schema explícito, dejando que Spark infiera
    el schema desde los datos (a diferencia de
    read_ipm_1644_operational/read_ipm_1240_operational, que sí declaran
    schema fijo). Usado para tablas de referencia pequeñas y consistentes
    (business_transaction_type, currency, validation_conditions) — se les
    pasa el archivo exacto (`.../data.parquet`), no el prefijo/carpeta, para
    no depender de que Spark ignore algún archivo no-parquet que conviva ahí
    (ej. `currency/` también tiene `m_currency.csv/`, usado por
    glue-exchange-rates vía catálogo).

    Args:
        spark: Sesión de Spark activa.
        path: Path S3 al archivo o directorio Parquet a leer.
        name: Nombre descriptivo de la fuente, solo para logging.

    Returns:
        DataFrame con el contenido leído.

    Ejemplo:
        read_parquet(spark, f"{S3_REFERENCE}/currency/data.parquet", "currency")
    """
    print(f"[READ] {name}: {path}", flush=True)
    return spark.read.option("mergeSchema", "false").parquet(path)


def read_exchange_rates_glue(spark, s3_reference: str, query_date: str) -> DataFrame:
    """
    Fuente: exchange-rates-glue/brand=Mastercard/exchange_date={query_date}/
    (enriquecido con codigos numericos por glue-exchange-rates, cobertura viva
    y actualizada) — reemplaza a exchange_rate/, fuente manual congelada al
    2026-04-30. Renombra columnas a los nombres legacy (brand/currency_from/
    currency_to/currency_from_code/exchange_value) que ya espera el resto
    del script.
    """
    path = f"{s3_reference}/exchange-rates-glue/brand=Mastercard/exchange_date={query_date}/"
    print(f"[READ] exchange-rates-glue: {path}", flush=True)
    df = spark.read.option("mergeSchema", "false").parquet(path)
    return df.select(
        F.lit("MasterCard").alias("brand"),
        F.col("from_currency").alias("currency_from"),
        F.col("to_currency").alias("currency_to"),
        F.col("from_currency_numeric_code").alias("currency_from_code"),
        F.col("fx_rate").alias("exchange_value"),
    )


def safe_col(df: DataFrame, col_name: str, default=""):
    """
    Devuelve la columna col_name si existe en df, o un literal default si
    no — evita AnalysisException: cannot resolve column cuando una columna
    opcional (ej. message_reason_code, ausente en algunos bloques) falta en
    el schema leído.

    Args:
        df: DataFrame donde buscar la columna.
        col_name: Nombre de columna a buscar.
        default: Valor literal a usar si la columna no existe (default: "").

    Returns:
        F.col(col_name) si existe, o F.lit(default) si no.

    Ejemplo:
        safe_col(df, "message_reason_code", "")  # F.col(...) o F.lit("")
    """
    if col_name in df.columns:
        return F.col(col_name)
    return F.lit(default)


def yymmdd_to_date(col_expr):
    """
    Convierte una columna con año de 2 dígitos + mes + día (formato interno
    file_dt/file_date del pipeline MC, ej. "260103") a DATE, asumiendo siglo
    2000 (prefijo "20" fijo).

    Args:
        col_expr: Expresión de columna con el valor YYMMDD.

    Returns:
        Columna DATE.

    Ejemplo:
        yymmdd_to_date(F.col("file_dt"))  # DATE 2026-01-03 para "260103"
    """
    return F.to_date(
        F.concat(F.lit("20"), col_expr.cast("string")),
        "yyyyMMdd",
    )


def get_validation_condition(
    df_validation: DataFrame,
    customer_code: str,
    condition_type: str,
    data_source: str | None = None,
) -> str:
    """
    Busca en la tabla de referencia validation_conditions la condición SQL
    configurada para un cliente, marca ("MC" fijo) y tipo de condición (ej.
    "where", "hash_file_filter"), opcionalmente filtrando también por
    data_source ("stl"/"trx"). Solo se admiten filas con vc_report ==
    "validation".

    Args:
        df_validation: DataFrame de la tabla validation_conditions ya
            cargada.
        customer_code: Código de cliente a filtrar.
        condition_type: Tipo de condición a buscar, ej. "where" o
            "hash_file_filter".
        data_source: Filtro adicional opcional ("stl" o "trx"); si es None,
            no se aplica.

    Returns:
        El string de la condición (vc_query_code) de la primera fila que
        matchea, o "" si no hay ninguna.

    Ejemplo:
        get_validation_condition(df_validation, "SBSA", "hash_file_filter")
        # "app_hash_file IN (...)" o ""
    """
    df = df_validation.filter(
        (F.col("vc_report") == "validation")
        & (F.col("vc_brand") == "MC")
        & (F.col("vc_customer_code") == customer_code)
        & (F.col("vc_condition_type") == condition_type)
    )

    if data_source:
        df = df.filter(F.col("vc_data_source") == data_source)

    rows = df.select("vc_query_code").limit(1).collect()

    if not rows:
        return ""

    return rows[0]["vc_query_code"] or ""


# =============================================================================
# HASH FILTER SOLO SBSA
# =============================================================================

def _scan_with_backoff(table, scan_kwargs: dict, max_retries: int = 6):
    """
    Wrapper de table.scan() con backoff exponencial ante
    ProvisionedThroughputExceededException.

    file_control-02 tiene solo 1 RCU provisionada — un scan completo de la
    tabla (cientos de items) la agota fácil, y este job la escanea DOS veces
    seguidas por día (una vez para settlement, otra para transactional) vía
    get_control_hash_codes(). Confirmado empíricamente: el segundo scan
    revienta con ProvisionedThroughputExceededException incluso después de
    que boto3 ya reintentó internamente 9 veces con su backoff default. Esto
    no es solo un problema de pruebas locales — el job real correría en el
    mismo riesgo si se ejecuta para SBSA (más aún si vi_data_quality.py, que
    también escanea esta tabla, corre en paralelo).

    Args:
        table: recurso Table de boto3 (dynamodb.Table(...)).
        scan_kwargs: kwargs a pasar a table.scan() (FilterExpression,
            ExclusiveStartKey, etc.).
        max_retries: intentos máximos antes de propagar la excepción.

    Returns:
        La respuesta de table.scan(...).

    Ejemplo:
        _scan_with_backoff(table, {"FilterExpression": filter_expr})
    """
    from botocore.exceptions import ClientError

    for attempt in range(max_retries):
        try:
            return table.scan(**scan_kwargs)
        except ClientError as e:
            if e.response["Error"]["Code"] != "ProvisionedThroughputExceededException":
                raise
            if attempt == max_retries - 1:
                raise
            wait = 2 ** attempt
            print(
                f"[WARN] file_control scan throttled (intento {attempt + 1}/{max_retries}), "
                f"reintentando en {wait}s",
                flush=True,
            )
            time.sleep(wait)


def get_control_hash_codes_by_date(
    customer_code: str,
    start_date: str,
    end_date: str,
    brand_local_indicator: str,
    include_cps_collections: bool = False,
) -> dict:
    """
    Trae los content_hash válidos de DynamoDB (file_control) para TODO el
    rango [start_date, end_date] en un solo scan, agrupados por fecha —
    mismo patrón que vi_data_quality.py::get_valid_content_hashes(), que
    recibe start_date/end_date y usa Attr("file_processing_date").between()
    en vez de pedir un día a la vez.

    Por qué (encontrado 2026-08-17): get_mastercard_validation_results_range()
    procesa el rango día por día, y antes de este cambio cada día llamaba a
    get_control_hash_codes() (scan de un solo día) por separado — 62 scans
    idénticos para un mes de SBSA (31 días × 2 fuentes). Un Scan+FilterExpression
    de DynamoDB consume RCU por el TOTAL de items leídos de la tabla, no por
    los que matchean el filtro (file_control-02 solo tiene 'file_id' como
    partition key, sin sort key ni GSI — no admite Query por fecha), así que
    pedir "un día" cuesta lo mismo que pedir "todo el rango": el costo real
    depende de CUÁNTAS VECES se llama, no de qué tan angosto sea el filtro.
    Confirmado en un job real: SBSA/enero completo tardó >27 min (cancelado
    manualmente) vs. ~1 min de un día suelto.

    Llamando esta función UNA vez por rango (como hace VI) en vez de una vez
    por día, un mes de SBSA pasa de 62 scans a 2 (uno para settlement, uno
    para transactional — difieren en include_cps_collections). No resuelve
    el problema de fondo (la tabla debería tener un GSI para permitir Query
    real — ver pendiente #2 en la memoria del proyecto), pero evita la
    lentitud práctica sin cambiar la tabla.

    Args:
        customer_code: Código de cliente (se espera "SBSA").
        start_date, end_date: Rango a traer, formato "YYYY-MM-DD" (inclusive,
            vía Attr.between — igual semántica que VI).
        brand_local_indicator: "brand" o "default" — igual que en
            get_control_hash_codes().
        include_cps_collections: igual que en get_control_hash_codes().

    Returns:
        Dict {query_date: [content_hash, ...]} — un día sin ningún hash
        válido simplemente no aparece como key (el caller debe tratar esa
        ausencia igual que el sentinel ["xyz"] de get_control_hash_codes()).

    Ejemplo:
        get_control_hash_codes_by_date("SBSA", "2026-01-01", "2026-01-31", "brand", True)
        # {"2026-01-01": ["hash1", "hash2"], "2026-01-03": ["hash3"], ...}
    """
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamodb.Table(DYNAMO_TABLE_FILE_CONTROL)

    filter_expr = (
        Attr("client_id").eq(customer_code)
        & Attr("brand_id").eq("MC")
        & Attr("file_processing_date").between(start_date, end_date)
    )

    by_date: dict = {}
    scan_kwargs = {"FilterExpression": filter_expr}

    while True:
        response = _scan_with_backoff(table, scan_kwargs)

        for item in response.get("Items", []):
            file_date = item.get("file_processing_date", "")
            landing_file_name = item.get("landing_file_name", "")
            content_hash = item.get("content_hash", "")

            if not content_hash:
                continue

            if brand_local_indicator.lower() == "brand":
                matches = (
                    "MasterCard_Inward" in landing_file_name
                    or "MasterCard_Outward" in landing_file_name
                )
                if include_cps_collections:
                    matches = matches or "MasterCard_CPS_Collections_File" in landing_file_name
            else:
                matches = "Local_MasterCard" in landing_file_name

            if matches:
                by_date.setdefault(file_date, []).append(content_hash)

        if "LastEvaluatedKey" in response:
            scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
        else:
            break

    total_hashes = sum(len(v) for v in by_date.values())
    print(
        f"[VALIDATION] file_control scan por rango ({start_date}..{end_date}) "
        f"para {customer_code}: {total_hashes} hashes en {len(by_date)} fechas",
        flush=True,
    )
    return by_date


def get_control_hash_codes(
    customer_code: str,
    query_date: str,
    brand_local_indicator: str,
    include_cps_collections: bool = False,
) -> List[str]:
    """
    Replica la lógica de filtrado por hash de archivo, exclusiva de SBSA,
    leyendo de DynamoDB (tabla file_control) — mismo patrón y misma tabla
    real que usa vi_data_quality.py::get_valid_content_hashes(). Reemplaza
    el enfoque original vía Parquet (CONTROL_FILE_PATH / control.t_control_file),
    que no tiene ninguna fuente de datos disponible en Standard 2.0 — el
    control de archivos se migró a DynamoDB, no a un Parquet, y esta función
    quedó desactualizada apuntando a un path que nunca existió, dejando el
    hash_file_filter de SBSA inerte (siempre caía al sentinel ['xyz']).

    Filtra por client_id=customer_code, brand_id='MC' y file_processing_date
    == query_date; luego matchea landing_file_name en Python (DynamoDB no
    soporta LIKE) según el patrón de nombre de archivo correspondiente al
    indicador brand/local:

      Settlement:
        brand: MasterCard_Inward / MasterCard_Outward
        default: Local_MasterCard

      Transactional:
        brand: MasterCard_Inward / MasterCard_Outward / MasterCard_CPS_Collections_File
        default: Local_MasterCard

    NOTA: esta versión escanea un solo día por llamada — usada como fallback
    cuando no hay un mapa {fecha: hashes} precalculado (ver
    get_control_hash_codes_by_date(), que trae el rango completo en un solo
    scan y es lo que usa get_mastercard_validation_results_range() en el
    camino normal). Queda disponible para no romper llamadas puntuales
    fuera del loop de rango.

    Args:
        customer_code: Código de cliente (se espera "SBSA", único caso donde
            se llama a esta función).
        query_date: Fecha a filtrar, formato "YYYY-MM-DD".
        brand_local_indicator: "brand" o "default" — determina qué patrones
            de landing_file_name se incluyen.
        include_cps_collections: Si es True, agrega el patrón
            MasterCard_CPS_Collections_File al filtro "brand" (usado solo
            para el data_source "trx").

    Returns:
        Lista de content_hash que matchean los filtros, o ["xyz"] si no hay
        ninguno (sentinel que no matchea ningún hash real).

    Ejemplo:
        get_control_hash_codes("SBSA", "2026-01-24", "brand", include_cps_collections=True)
    """
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamodb.Table(DYNAMO_TABLE_FILE_CONTROL)

    filter_expr = (
        Attr("client_id").eq(customer_code)
        & Attr("brand_id").eq("MC")
        & Attr("file_processing_date").eq(query_date)
    )

    codes: List[str] = []
    scan_kwargs = {"FilterExpression": filter_expr}

    while True:
        response = _scan_with_backoff(table, scan_kwargs)

        for item in response.get("Items", []):
            landing_file_name = item.get("landing_file_name", "")
            content_hash = item.get("content_hash", "")

            if not content_hash:
                continue

            if brand_local_indicator.lower() == "brand":
                matches = (
                    "MasterCard_Inward" in landing_file_name
                    or "MasterCard_Outward" in landing_file_name
                )
                if include_cps_collections:
                    matches = matches or "MasterCard_CPS_Collections_File" in landing_file_name
            else:
                matches = "Local_MasterCard" in landing_file_name

            if matches:
                codes.append(content_hash)

        if "LastEvaluatedKey" in response:
            scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
        else:
            break

    return codes if codes else ["xyz"]


# =============================================================================
# VALIDATION CONDITIONS
# =============================================================================

def apply_validation_conditions(
    df: DataFrame,
    df_validation: DataFrame,
    query_date: str,
    customer_code: str,
    issuer_acquirer_indicator: str,
    brand_local_indicator: str,
    data_source: str,
    member_activity_col: str | None = None,
    hash_col: str = "content_hash",
    hash_codes_by_date: dict | None = None,
) -> DataFrame:
    """
    Aplica condiciones dinámicas de la tabla validation_conditions sobre un
    DataFrame ya normalizado, replicando dos patrones conocidos del SQL
    legacy:
      - Filtro por reconciled_member_activity (o el campo equivalente pasado
        en member_activity_col) igual a issuer_acquirer_indicator, si la
        condición "where" configurada lo menciona.
      - hash_file_filter, exclusivo de SBSA (ver get_control_hash_codes()),
        si la condición configurada lo menciona y la columna de hash está
        presente en df.

    Args:
        df: DataFrame ya normalizado (settlement o transactional) a filtrar.
        df_validation: DataFrame de la tabla validation_conditions.
        query_date: Fecha en formato "YYYY-MM-DD".
        customer_code: Código de cliente.
        issuer_acquirer_indicator: "I" o "A", usado para el filtro de
            member_activity.
        brand_local_indicator: "brand" o "default", pasado a
            get_control_hash_codes() para SBSA.
        data_source: "stl" o "trx" — determina qué condición "where" se busca
            y si se incluye MasterCard_CPS_Collections_File en el hash filter.
        member_activity_col: Nombre de la columna de member_activity a filtrar
            (solo aplica a settlement; None para transactional).
        hash_col: Nombre de columna de hash preferido (default:
            "content_hash"); si no está en df, se usa "app_hash_file" como
            fallback.
        hash_codes_by_date: mapa {fecha: [hashes]} precalculado para todo el
            rango por get_control_hash_codes_by_date() (un solo scan de
            DynamoDB por rango, en vez de uno por día — ver esa función).
            Si se pasa, se usa `hash_codes_by_date.get(query_date, ["xyz"])`
            en vez de llamar a get_control_hash_codes() (que sigue siendo el
            fallback si no se pasa nada, ej. para llamadas puntuales fuera
            del loop de rango).

    Returns:
        El DataFrame filtrado (puede ser el mismo df sin cambios si ninguna
        condición aplicó).

    Ejemplo:
        apply_validation_conditions(df, df_validation, "2026-01-03",
                                     "SBSA", "I", "brand", "trx",
                                     member_activity_col=None, hash_col="content_hash")
    """

    where_sql = get_validation_condition(
        df_validation=df_validation,
        customer_code=customer_code,
        condition_type="where",
        data_source=data_source,
    )

    if (
        where_sql
        and "reconciled_member_activity" in where_sql
        and member_activity_col
        and member_activity_col in df.columns
    ):
        print(
            f"[VALIDATION][{data_source}] Aplicando member_activity={issuer_acquirer_indicator}",
            flush=True,
        )
        df = df.filter(
            F.col(member_activity_col).cast("string") == issuer_acquirer_indicator
        )

    if customer_code.upper() == "SBSA":
        hash_sql = get_validation_condition(
            df_validation=df_validation,
            customer_code=customer_code,
            condition_type="hash_file_filter",
        )

        actual_hash_col = hash_col if hash_col in df.columns else "app_hash_file"

        if hash_sql and "app_hash_file" in hash_sql and actual_hash_col in df.columns:
            if hash_codes_by_date is not None:
                hash_codes = hash_codes_by_date.get(query_date, ["xyz"])
            else:
                hash_codes = get_control_hash_codes(
                    customer_code=customer_code,
                    query_date=query_date,
                    brand_local_indicator=brand_local_indicator,
                    include_cps_collections=(data_source == "trx"),
                )

            print(f"[VALIDATION][{data_source}] SBSA hash_file_filter codes={len(hash_codes)}")
            df = df.filter(F.col(actual_hash_col).isin(hash_codes))

    return df


def compute_extra_fields_stl(t1: DataFrame, df_validation: DataFrame, customer_code: str):
    """
    Replica el override iqa_extra_fields de validation_conditions (settlement).

    Legacy: sin fila configurada para (MC, stl, customer), message_reason_code,
    electronic_commerce_indicator y reconciled_file_flg van en blanco (''). Con
    fila configurada (hoy solo SBSA), message_reason_code y
    electronic_commerce_indicator vienen del dato real, y reconciled_file_flg
    se deriva 'Y'/'N' según si reconciled_file contiene '00004' — texto exacto
    de la única fila iqa_extra_fields/stl vista hasta hoy (customer=SBSA).

    Si en el futuro otro cliente configura iqa_extra_fields/stl con un texto
    distinto al de SBSA, esta función seguirá tratándolo como "override
    presente" para message_reason_code/electronic_commerce_indicator (que sí
    coincide con el único patrón visto), pero NO recalculará el flag Y/N de
    reconciled_file_flg salvo que sea SBSA — revisar manualmente ese caso.

    Returns:
        Tupla de 3 columnas ya aliasadas: (message_reason_code,
        electronic_commerce_indicator_3, reconciled_file).
    """
    has_override = get_validation_condition(
        df_validation=df_validation,
        customer_code=customer_code,
        condition_type="iqa_extra_fields",
        data_source="stl",
    ) != ""

    if not has_override:
        blank = F.lit("").cast("string")
        return (
            blank.alias("message_reason_code"),
            blank.alias("electronic_commerce_indicator_3"),
            blank.alias("reconciled_file"),
        )

    if customer_code.upper() == "SBSA":
        reconciled_file_col = F.when(
            safe_col(t1, COL_STL_RECONCILED_FILE, "").cast("string").like("%00004"),
            F.lit("Y"),
        ).otherwise(F.lit("N"))
    else:
        reconciled_file_col = safe_col(t1, COL_STL_RECONCILED_FILE, "").cast("string")

    return (
        safe_col(t1, COL_STL_MSG_REASON, "").cast("string").alias("message_reason_code"),
        safe_col(t1, COL_STL_ECI, "").cast("string").alias("electronic_commerce_indicator_3"),
        reconciled_file_col.alias("reconciled_file"),
    )


def compute_extra_fields_trx(t1: DataFrame, df_validation: DataFrame, customer_code: str):
    """
    Replica el override iqa_extra_fields de validation_conditions (transactional).

    Legacy: sin fila configurada para (MC, trx, customer), message_reason_code
    y electronic_commerce_indicator van en blanco (''). Con fila configurada
    (hoy solo SBSA), ambos vienen del dato real — reconciled_file_flg no
    aplica en transactional, siempre es '' en ambos sistemas.

    Returns:
        Tupla de 2 columnas ya aliasadas: (message_reason_code,
        electronic_commerce_indicator_3).
    """
    has_override = get_validation_condition(
        df_validation=df_validation,
        customer_code=customer_code,
        condition_type="iqa_extra_fields",
        data_source="trx",
    ) != ""

    if not has_override:
        blank = F.lit("").cast("string")
        return blank.alias("message_reason_code"), blank.alias("electronic_commerce_indicator_3")

    return (
        safe_col(t1, COL_TRX_MSG_REASON, "").cast("string").alias("message_reason_code"),
        safe_col(t1, COL_TRX_ECI, "").cast("string").alias("electronic_commerce_indicator_3"),
    )


def compute_processing_code(df_validation: DataFrame, customer_code: str, raw_col):
    """
    Replica el override iqa_processing_code de validation_conditions
    (transactional, hoy solo SBSA). Sin fila configurada: default legacy
    LEFT(processing_code, 2).

    Con fila configurada (SBSA), el legacy remapea con este CASE (texto
    exacto de validation_conditions):
        LEFT(processing_code,2) = '**' → '18'
        LEFT(processing_code,2) = '*0' → '20'
        LEFT(processing_code,2) = '0*' AND business_activity_4 IN ('CC','CD') → '01'
        LEFT(processing_code,2) = '0*' → '09'
        ELSE LEFT(processing_code,2)

    ADVERTENCIA (sin resolver): en datos reales de SBSA (IPM_1240 Standard
    2.0) el carácter de máscara observado es '?' (ej. '??','?0','0?'), no '*'
    como está escrito en validation_conditions — con el patrón tal cual
    escrito, este remapeo NO va a matchear ningún registro real hoy. Se
    replica literalmente el texto configurado (no se adivina '?' en su
    lugar) hasta confirmar con el owner de validation_conditions si la fila
    debe actualizarse al nuevo carácter de máscara.

    Returns:
        Columna sin alias (processing_code remapeado o LEFT(2) por defecto).
    """
    prefix = F.substring(raw_col.cast("string"), 1, 2)

    has_override = get_validation_condition(
        df_validation=df_validation,
        customer_code=customer_code,
        condition_type="iqa_processing_code",
        data_source="trx",
    ) != ""

    if not (customer_code.upper() == "SBSA" and has_override):
        return prefix

    return (
        F.when(prefix == "**", F.lit("18"))
         .when(prefix == "*0", F.lit("20"))
         .when(
             (prefix == "0*") & (F.col(COL_TRX_BUSINESS_ACTIVITY_4).isin("CC", "CD")),
             F.lit("01"),
         )
         .when(prefix == "0*", F.lit("09"))
         .otherwise(prefix)
    )


# =============================================================================
# FUNCIÓN SETTLEMENT
# =============================================================================

def get_mastercard_validation_results_settlement(
    spark,
    query_date: str,
    customer_code: str,
    issuer_acquirer_indicator: str,
    brand_local_indicator: str = "default",
    hash_codes_by_date: dict | None = None,
) -> DataFrame:
    """
    Arma el resultado de validación de un día desde la fuente de liquidación
    (operational IPM_1644, function_code=685, reconciled_transaction_function_1=
    "1240"). Lee, filtra por MTI/function_code, aplica las condiciones
    dinámicas de validation_conditions (member_activity + hash_file_filter
    SBSA), normaliza a un schema intermedio (t1_norm) y agrega vía Spark SQL
    contra 3 referencias (business_transaction_type, exchange-rates-glue,
    currency), calculando jurisdiction desde
    reconciled_business_activity_2/settlement_indicator_1 y convirtiendo
    los montos netos a report_currency_code con el tipo de cambio de la
    fecha.

    Args:
        spark: Sesión de Spark activa.
        query_date: Fecha a procesar, formato "YYYY-MM-DD".
        customer_code: Código de cliente, ej. "EBGR".
        issuer_acquirer_indicator: "I" o "A" — determina file_type (IN/OUT,
            aunque el settlement 1644 siempre se lee de IN, ver comentario
            inline) y se usa para el filtro de member_activity.
        brand_local_indicator: "brand" o "default" (default: "default").
        hash_codes_by_date: mapa {fecha: [hashes]} precalculado por
            get_control_hash_codes_by_date() para todo el rango — se pasa
            tal cual a apply_validation_conditions(). None (default) hace
            que apply_validation_conditions() escanee DynamoDB por su cuenta
            para este único día (fallback, ver esa función).

    Returns:
        DataFrame agregado con columnas app_processing_date, data_source,
        file_source, app_customer_code, app_message_type, business_mode,
        jurisdiction, report_currency_code, settlement_currency, trx_type,
        ird, intelica_id, message_reason_code, electronic_commerce_indicator,
        reconciled_file_flg, trx_count, trx_amt, itx_amt — una fila por
        combinación única de dimensiones.

    Ejemplo:
        get_mastercard_validation_results_settlement(spark, "2026-01-03", "EBGR", "I")
    """
    customer_code = customer_code.upper()
    brand_local_indicator = brand_local_indicator or "default"

    file_type = "OUT" if issuer_acquirer_indicator == "A" else "IN"

    client = get_client_from_dynamo(customer_code)
    report_currency_code = client["report_currency_code"]

    print(f"[PARAM][STL] query_date={query_date}")
    print(f"[PARAM][STL] customer_code={customer_code}")
    print(f"[PARAM][STL] issuer_acquirer_indicator={issuer_acquirer_indicator}")
    print(f"[PARAM][STL] file_type={file_type}")
    print(f"[PARAM][STL] report_currency_code={report_currency_code}")

    # Settlement 1644 normalmente llega por IN.
    # Se lee la partición completa y se filtra por MTI/function_code,
    # evitando wildcard tipo *_1644_685.parquet que puede fallar en Glue/Spark.
    data_path = (
        f"{S3_OPERATIONAL}/{customer_code}/MC/IPM_1644/"
        f"file_type=IN/date={query_date}/"
    )

    btt_path = f"{S3_REFERENCE}/mastercard_business_transaction_type/data.parquet"
    currency_path = f"{S3_REFERENCE}/currency/data.parquet"
    validation_path = f"{S3_REFERENCE}/validation_conditions/data.parquet"

    df_raw = read_ipm_1644_operational(spark, data_path)

    df_exchange = read_exchange_rates_glue(spark, S3_REFERENCE, query_date)
    df_btt = read_parquet(spark, btt_path, "mastercard_business_transaction_type")
    df_currency = read_parquet(spark, currency_path, "currency")
    df_validation = read_parquet(spark, validation_path, "validation_conditions")

    
    t1 = (
        df_raw
        .filter(F.col("type_mti").cast("string") == "1644")
        .filter(F.col("function_code").cast("string") == "685")
        .filter(F.col(COL_STL_TXN_FUNC).cast("string").isin("1240", "1442"))
    )


    t1 = apply_validation_conditions(
        df=t1,
        df_validation=df_validation,
        query_date=query_date,
        customer_code=customer_code,
        issuer_acquirer_indicator=issuer_acquirer_indicator,
        brand_local_indicator=brand_local_indicator,
        data_source="stl",
        member_activity_col=COL_STL_MEMBER_ACTIVITY,
        hash_col="content_hash",
        hash_codes_by_date=hash_codes_by_date,
    )
    if DEBUG:
        print(f"[COUNT][STL] filtered_transactions={t1.count()}")


    extra_msg_reason, extra_eci, extra_reconciled_file = compute_extra_fields_stl(
        t1, df_validation, customer_code
    )

    t1_norm = t1.select(
        yymmdd_to_date(F.col("file_dt")).alias("app_processing_date"),
        F.col("file_id").cast("string"),
        F.col("ref_id").cast("long").alias("app_id"),
        F.lit(customer_code).cast("string").alias("app_customer_code"),
        F.col(COL_STL_MEMBER_ACTIVITY).cast("string").alias("reconciled_member_activity"),
        F.col(COL_STL_BUSINESS_ACTIVITY_2).cast("string").alias("reconciled_business_activity_2"),
        F.when(
            F.col(COL_STL_BUSINESS_ACTIVITY_2).cast("string").rlike("^[0-9]+$"),
            F.col(COL_STL_BUSINESS_ACTIVITY_2).cast("int"),
        ).otherwise(F.lit(None).cast("int")).alias("reconciled_business_activity_2_int"),
        F.col(COL_STL_BUSINESS_ACTIVITY_4).cast("string").alias("reconciled_business_activity_4"),
        F.col(COL_STL_TOTAL_TRX_NUMBER).cast("double").alias("total_transaction_number"),
        F.col(COL_STL_AMT_NET_TRX_RECON).cast("double").alias("amount_net_transaction_in_reconciliation_currency_2"),
        F.col(COL_STL_AMT_NET_FEE_RECON).cast("double").alias("amount_net_fee_in_reconciliation_currency_2"),
        extra_msg_reason,
        extra_eci,
        F.col(COL_STL_PROCESSING_CODE).cast("string").alias("reconciled_processing_code"),
        F.col(COL_STL_CURRENCY_RECON).cast("string").alias("currency_code_reconciliation"),
        F.col("function_code").cast("string").alias("function_code"),
        F.col(COL_STL_TXN_FUNC).cast("string").alias("reconciled_transaction_function_1"),
        F.col(COL_STL_SETTLEMENT_INDICATOR).cast("string").alias("settlement_indicator_1"),
        F.col("type_mti").cast("string").alias("app_message_type"),
        extra_reconciled_file,
    )
    if DEBUG:
        print(f"[COUNT SLT] {t1_norm.count()}")

    if DEBUG:
        (
            t1_norm
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(
                "/home/ameza/IntelicaProyectos/glue-interchangemc/output_local/ipm_1644_raw"
            )
        )
    
    m1 = df_btt.select(
        F.col("business_transaction_type_id").cast("string").alias("business_transaction_type_id"),
        F.col("transaction_type_id").cast("string").alias("transaction_type_id"),
    )

    x1 = (
        df_exchange
        .filter(F.col("brand") == "MasterCard")
        .select(
            F.col("currency_from_code").cast("string").alias("currency_from_code"),
            F.col("currency_to").cast("string").alias("currency_to"),
            F.col("exchange_value").cast("double").alias("exchange_value"),
        )
    )

    mc = df_currency.select(
        F.col("currency_numeric_code").cast("string").alias("currency_numeric_code"),
        F.col("currency_alphabetic_code").cast("string").alias("currency_alphabetic_code"),
    )

    t1_norm.createOrReplaceTempView("stl_t1")
    F.broadcast(m1).createOrReplaceTempView("stl_m1")
    F.broadcast(x1).createOrReplaceTempView("stl_x1")
    F.broadcast(mc).createOrReplaceTempView("stl_mc")

    final_df = spark.sql(f"""
        SELECT
            T1.app_processing_date                                            AS app_processing_date,
            CAST('Settlement Data' AS STRING)                                 AS data_source,
            CAST('{brand_local_indicator.lower()}' AS STRING)                 AS file_source,
            T1.app_customer_code                                              AS app_customer_code,
            T1.reconciled_transaction_function_1                              AS app_message_type,

            CASE
                WHEN T1.reconciled_member_activity = 'A' THEN 'Acquirer'
                WHEN T1.reconciled_member_activity = 'I' THEN 'Issuer'
                ELSE 'Unknown'
            END                                                               AS business_mode,

            CASE
                WHEN T1.settlement_indicator_1 = 'C' THEN 'on-us'
                WHEN CAST(T1.reconciled_business_activity_2 AS INT) IN (4) THEN 'off-us'
                WHEN CAST(T1.reconciled_business_activity_2 AS INT) IN (2, 3) THEN 'intraregional'
                WHEN CAST(T1.reconciled_business_activity_2 AS INT) IN (1,8) THEN 'interregional'
                ELSE NULL
            END                                                               AS jurisdiction,

            CAST('{report_currency_code}' AS STRING)                          AS report_currency_code,

            CASE
                WHEN MC.currency_alphabetic_code = CAST('{report_currency_code}' AS STRING)
                    THEN 'Local'
                ELSE 'Foreign'
            END                                                               AS settlement_currency,

            M1.transaction_type_id                                             AS trx_type,
            T1.reconciled_business_activity_4                                 AS ird,
            CAST('' AS STRING)                                                 AS intelica_id,
            T1.message_reason_code                                             AS message_reason_code,
            T1.electronic_commerce_indicator_3                                AS electronic_commerce_indicator,
            T1.reconciled_file                                                 AS reconciled_file_flg,

            CAST(SUM(T1.total_transaction_number) AS DECIMAL(38,6))            AS trx_count,

            CAST(
                SUM(
                    COALESCE(X1.exchange_value, 1)
                    * T1.amount_net_transaction_in_reconciliation_currency_2
                )
                AS DECIMAL(38,6)
            )                                                                 AS trx_amt,

            CAST(
                SUM(
                    COALESCE(X1.exchange_value, 1)
                    * T1.amount_net_fee_in_reconciliation_currency_2
                )
                AS DECIMAL(38,6)
            )                                                                 AS itx_amt

        FROM stl_t1 T1

        LEFT JOIN stl_m1 M1
            ON T1.reconciled_processing_code = M1.business_transaction_type_id

        LEFT JOIN stl_x1 X1
            ON X1.currency_from_code = T1.currency_code_reconciliation
           AND X1.currency_to = CAST('{report_currency_code}' AS STRING)

        LEFT JOIN stl_mc MC
            ON MC.currency_numeric_code = T1.currency_code_reconciliation

        GROUP BY
            T1.app_processing_date,
            T1.app_customer_code,
            T1.reconciled_transaction_function_1,
            T1.reconciled_member_activity,
            T1.settlement_indicator_1,
            T1.reconciled_business_activity_2,
            T1.reconciled_business_activity_2_int,
            MC.currency_alphabetic_code,
            M1.transaction_type_id,
            T1.reconciled_business_activity_4,
            T1.message_reason_code,
            T1.electronic_commerce_indicator_3,
            T1.reconciled_file
    """)

    return final_df


# =============================================================================
# FUNCIÓN TRANSACTIONAL
# =============================================================================

def get_mastercard_validation_results_transactional(
    spark,
    query_date: str,
    customer_code: str,
    issuer_acquirer_indicator: str,
    brand_local_indicator: str = "default",
    hash_codes_by_date: dict | None = None,
) -> DataFrame:
    """
    Arma el resultado de validación de un día desde la fuente transaccional
    (operational IPM_1240), usando file_type derivado de
    issuer_acquirer_indicator ("A"→OUT, "I"→IN). Lee, filtra por MTI=1240,
    aplica las condiciones dinámicas de validation_conditions (solo
    hash_file_filter SBSA — no hay member_activity_col para esta fuente,
    app_type_file ya distingue Issuer/Acquirer), normaliza a un schema
    intermedio (t1_norm) y agrega vía Spark SQL contra 3 referencias
    (business_transaction_type, exchange-rates-glue, currency). Las
    expresiones de monto/moneda difieren según file_type: IN usa
    amount_reconciliation/amounts_transaction_fee_7 con tipo de cambio
    por currency_code_reconciliation; OUT usa amount_transaction/
    calculated_value con tipo de cambio por currency_code_transaction
    — calculated_value ya está en la moneda de la transacción
    (trx_ccy/DE_49), no en rate_currency, desde el rewrite de
    calculate_mastercard_fee_pyspark (ver gotchas.md). Una única tasa
    (X1) alcanza para monto y fee en ambos file_type — antes había un
    segundo join (X2) redundante, eliminado (ver gotchas.md).

    Args:
        spark: Sesión de Spark activa.
        query_date: Fecha a procesar, formato "YYYY-MM-DD".
        customer_code: Código de cliente, ej. "EBGR".
        issuer_acquirer_indicator: "I" o "A" — determina file_type (IN/OUT)
            y por lo tanto qué columnas/joins de moneda se usan.
        brand_local_indicator: "brand" o "default" (default: "default").
        hash_codes_by_date: mapa {fecha: [hashes]} precalculado por
            get_control_hash_codes_by_date() para todo el rango — ver mismo
            parámetro en get_mastercard_validation_results_settlement().

    Returns:
        DataFrame agregado con las mismas columnas que
        get_mastercard_validation_results_settlement() (mismo schema, para
        poder unir ambas fuentes con unionByName).

    Ejemplo:
        get_mastercard_validation_results_transactional(spark, "2026-01-03", "EBGR", "I")
    """
    customer_code = customer_code.upper()
    brand_local_indicator = brand_local_indicator or "default"

    file_type = "OUT" if issuer_acquirer_indicator == "A" else "IN"

    client = get_client_from_dynamo(customer_code)
    report_currency_code = client["report_currency_code"]

    print(f"[PARAM][TRX] query_date={query_date}")
    print(f"[PARAM][TRX] customer_code={customer_code}")
    print(f"[PARAM][TRX] issuer_acquirer_indicator={issuer_acquirer_indicator}")
    print(f"[PARAM][TRX] file_type={file_type}")
    print(f"[PARAM][TRX] report_currency_code={report_currency_code}")

    data_path = (
        f"{S3_OPERATIONAL}/{customer_code}/MC/IPM_1240/"
        f"file_type={file_type}/date={query_date}/"
    )

    btt_path = f"{S3_REFERENCE}/mastercard_business_transaction_type/data.parquet"
    currency_path = f"{S3_REFERENCE}/currency/data.parquet"
    validation_path = f"{S3_REFERENCE}/validation_conditions/data.parquet"

    #df_raw = read_parquet(spark, data_path, "IPM_1240")
    df_raw = read_ipm_1240_operational(spark, data_path)

    if DEBUG:
        print(f"[COUNT RAW] {df_raw.count()}")

        (
            df_raw
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(
                "/home/ameza/IntelicaProyectos/glue-interchangemc/output_local/ipm_1240_raw"
            )
        )

    df_exchange = read_exchange_rates_glue(spark, S3_REFERENCE, query_date)
    df_btt = read_parquet(spark, btt_path, "mastercard_business_transaction_type")
    df_currency = read_parquet(spark, currency_path, "currency")
    df_validation = read_parquet(spark, validation_path, "validation_conditions")

    t1 = df_raw.filter(F.col(COL_TRX_MTI).cast("string").isin("1240", "1442"))

    t1 = apply_validation_conditions(
        df=t1,
        df_validation=df_validation,
        query_date=query_date,
        customer_code=customer_code,
        issuer_acquirer_indicator=issuer_acquirer_indicator,
        brand_local_indicator=brand_local_indicator,
        data_source="trx",
        member_activity_col=None,
        hash_col=COL_TRX_HASH,
        hash_codes_by_date=hash_codes_by_date,
    )
    if DEBUG:
        print(f"[COUNT][TRX] filtered_transactions={t1.count()}")

    processing_code_col = compute_processing_code(df_validation, customer_code, F.col(COL_TRX_PROCESSING_CODE))
    extra_msg_reason, extra_eci = compute_extra_fields_trx(t1, df_validation, customer_code)

    t1_norm = t1.select(
        yymmdd_to_date(F.col(COL_TRX_FILE_DATE)).alias("app_processing_date"),
        F.col(COL_TRX_HASH).cast("string").alias("app_hash_file"),
        F.col(COL_TRX_APP_ID).cast("long").alias("app_id"),
        F.lit(customer_code).cast("string").alias("app_customer_code"),
        F.col(COL_TRX_MTI).cast("string").alias("app_message_type"),
        F.lit(file_type).cast("string").alias("app_type_file"),

        F.col(COL_TRX_SETTLEMENT_INDICATOR).cast("string").alias("settlement_indicator_1"),
        processing_code_col.alias("processing_code"),
        F.col(COL_TRX_BUSINESS_ACTIVITY_4).cast("string").alias("business_activity_4"),

        extra_msg_reason,
        extra_eci,

        F.col(COL_TRX_AMOUNT_RECON).cast("double").alias("amount_reconciliation"),
        F.col(COL_TRX_AMOUNT_TRANSACTION).cast("double").alias("amount_transaction"),
        F.col(COL_TRX_FEE_7).cast("double").alias("amounts_transaction_fee_7"),

        F.col(COL_TRX_CURRENCY_RECON).cast("string").alias("currency_code_reconciliation"),
        F.col(COL_TRX_CURRENCY_TRANSACTION).cast("string").alias("currency_code_transaction"),

        F.col(COL_TRX_JURISDICTION).cast("string").alias("jurisdiction"),
        F.col(COL_TRX_SETTLEMENT_REPORT_CURRENCY).cast("string").alias("settlement_report_currency_code"),

        F.col(COL_TRX_INTELICA_ID).cast("string").alias("intelica_id"),
        F.col(COL_TRX_CALCULATED_VALUE).cast("double").alias("calculated_value"),
    )

    m1 = df_btt.select(
        F.col("business_transaction_type_id").cast("string").alias("business_transaction_type_id"),
        F.col("transaction_type_id").cast("string").alias("transaction_type_id"),
    )

    x = (
        df_exchange
        .filter(F.col("brand") == "MasterCard")
        .select(
            F.col("currency_from_code").cast("string").alias("currency_from_code"),
            F.col("currency_from").cast("string").alias("currency_from"),
            F.col("currency_to").cast("string").alias("currency_to"),
            F.col("exchange_value").cast("double").alias("exchange_value"),
        )
    )

    mc = df_currency.select(
        F.col("currency_alphabetic_code").cast("string").alias("currency_alphabetic_code"),
    )

    t1_norm.createOrReplaceTempView("trx_t1")
    F.broadcast(m1).createOrReplaceTempView("trx_m1")
    F.broadcast(x).createOrReplaceTempView("trx_x")
    F.broadcast(mc).createOrReplaceTempView("trx_mc")

    if file_type == "IN":
        trx_amt_expr = "T1.amount_reconciliation"
        itx_amt_expr = "T1.amounts_transaction_fee_7"
        x1_currency_join = "X1.currency_from_code = T1.currency_code_reconciliation"
    else:
        trx_amt_expr = "T1.amount_transaction"
        itx_amt_expr = "T1.calculated_value"
        x1_currency_join = "X1.currency_from_code = T1.currency_code_transaction"

    final_df = spark.sql(f"""
        SELECT
            T1.app_processing_date                                      AS app_processing_date,
            CAST('Transactional Data' AS STRING)                        AS data_source,
            CAST('{brand_local_indicator.lower()}' AS STRING)           AS file_source,
            T1.app_customer_code                                        AS app_customer_code,
            T1.app_message_type                                         AS app_message_type,

            CASE
                WHEN T1.app_type_file = 'OUT' THEN 'Acquirer'
                WHEN T1.app_type_file = 'IN' THEN 'Issuer'
                ELSE 'Unknown'
            END                                                         AS business_mode,

            CASE
                WHEN T1.settlement_indicator_1 = 'C' THEN 'on-us'
                ELSE T1.jurisdiction
            END                                                         AS jurisdiction,

            CAST('{report_currency_code}' AS STRING)                    AS report_currency_code,

            CASE
                WHEN MC.currency_alphabetic_code = CAST('{report_currency_code}' AS STRING)
                    THEN 'Local'
                ELSE 'Foreign'
            END                                                         AS settlement_currency,

            M1.transaction_type_id                                      AS trx_type,
            T1.business_activity_4                                      AS ird,
            T1.intelica_id                                              AS intelica_id,
            T1.message_reason_code                                      AS message_reason_code,
            T1.electronic_commerce_indicator_3                          AS electronic_commerce_indicator,
            CAST('' AS STRING)                                          AS reconciled_file_flg,

            CAST(COUNT(1) AS DECIMAL(38,6))                             AS trx_count,

            CAST(
                SUM(COALESCE(X1.exchange_value, 1) * {trx_amt_expr})
                AS DECIMAL(38,6)
            )                                                           AS trx_amt,

            CAST(
                SUM(COALESCE(X1.exchange_value, 1) * {itx_amt_expr})
                AS DECIMAL(38,6)
            )                                                           AS itx_amt

        FROM trx_t1 T1

        LEFT JOIN trx_m1 M1
            ON T1.processing_code = M1.business_transaction_type_id

        LEFT JOIN trx_x X1
            ON {x1_currency_join}
           AND X1.currency_to = CAST('{report_currency_code}' AS STRING)

        LEFT JOIN trx_mc MC
            ON MC.currency_alphabetic_code = T1.settlement_report_currency_code

        WHERE T1.app_message_type IN ('1240', '1442')

        GROUP BY
            T1.app_processing_date,
            T1.app_customer_code,
            T1.app_message_type,
            T1.app_type_file,
            T1.settlement_indicator_1,
            T1.jurisdiction,
            MC.currency_alphabetic_code,
            M1.transaction_type_id,
            T1.business_activity_4,
            T1.intelica_id,
            T1.message_reason_code,
            T1.electronic_commerce_indicator_3
    """)

    return final_df


# =============================================================================
# COMBINADO
# =============================================================================

def get_mastercard_validation_results_combined(
    spark,
    query_date: str,
    customer_code: str,
    issuer_acquirer_indicator: str,
    brand_local_indicator: str = "default",
    hash_codes_by_date_stl: dict | None = None,
    hash_codes_by_date_trx: dict | None = None,
) -> DataFrame:

    """
    Combina los resultados de settlement y transactional de un mismo día en
    un único DataFrame (unionByName, mismo schema en ambas fuentes).

    Args:
        spark: Sesión de Spark activa.
        query_date: Fecha a procesar, formato "YYYY-MM-DD".
        customer_code: Código de cliente.
        issuer_acquirer_indicator: "I" o "A".
        brand_local_indicator: "brand" o "default" (default: "default").
        hash_codes_by_date_stl: mapa {fecha: [hashes]} de
            get_control_hash_codes_by_date() para la rama settlement (ver
            get_mastercard_validation_results_range(), que lo calcula una
            vez para todo el rango, no por día).
        hash_codes_by_date_trx: igual que el anterior, para la rama
            transactional (difiere de stl en include_cps_collections).

    Returns:
        DataFrame con la unión de settlement + transactional para ese día.

    Ejemplo:
        get_mastercard_validation_results_combined(spark, "2026-01-03", "EBGR", "I")
    """
    df_settlement = get_mastercard_validation_results_settlement(
        spark=spark,
        query_date=query_date,
        customer_code=customer_code,
        issuer_acquirer_indicator=issuer_acquirer_indicator,
        brand_local_indicator=brand_local_indicator,
        hash_codes_by_date=hash_codes_by_date_stl,
    )

    df_transactional = get_mastercard_validation_results_transactional(
        spark=spark,
        query_date=query_date,
        customer_code=customer_code,
        issuer_acquirer_indicator=issuer_acquirer_indicator,
        brand_local_indicator=brand_local_indicator,
        hash_codes_by_date=hash_codes_by_date_trx,
    )

    return df_settlement.unionByName(df_transactional)


from pyspark.sql import DataFrame

# =============================================================================
# EJECUTAR POR RANGO
# =============================================================================

def get_mastercard_validation_results_range(
    spark,
    start_date: str,
    end_date: str,
    customer_code: str,
    issuer_acquirer_indicator: str,
    brand_local_indicator: str = "default",
) -> DataFrame:

    """
    Itera día por día un rango [start_date, end_date] y combina el resultado
    de get_mastercard_validation_results_combined() de cada día en un único
    DataFrame. Un fallo en un día individual (ej. archivo faltante para esa
    fecha) no aborta el rango completo — se loguea como warning y se omite,
    acumulando failed_dates.

    Para SBSA (único cliente con hash_file_filter), trae los content_hash
    válidos de DynamoDB UNA SOLA VEZ para todo el rango, antes de empezar el
    loop de días — usando get_control_hash_codes_by_date(), mismo patrón que
    vi_data_quality.py::get_valid_content_hashes() (recibe start_date/end_date
    y hace un solo scan, en vez de que cada día del rango dispare su propio
    scan). Antes de este cambio, un mes de SBSA hacía 62 scans idénticos
    (31 días × 2 fuentes) contra una tabla de 1 RCU — confirmado en un job
    real que eso tardaba >27 min. Con el fetch único quedan solo 2 scans por
    corrida (uno para settlement, otro para transactional, ya que difieren
    en include_cps_collections). Para el resto de los clientes esto no
    aplica — ni se llama a DynamoDB.

    Args:
        spark: Sesión de Spark activa.
        start_date: Inicio del rango, formato "YYYY-MM-DD" (inclusive).
        end_date: Fin del rango, formato "YYYY-MM-DD" (inclusive).
        customer_code: Código de cliente.
        issuer_acquirer_indicator: "I" o "A".
        brand_local_indicator: "brand" o "default" (default: "default").

    Returns:
        DataFrame con la unión (unionByName acumulado) de todos los días
        procesados exitosamente.

    Raises:
        RuntimeError: si ningún día del rango produjo datos válidos.

    Ejemplo:
        get_mastercard_validation_results_range(spark, "2026-01-01", "2026-01-05", "EBGR", "I")
    """
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")

    hash_codes_by_date_stl: dict | None = None
    hash_codes_by_date_trx: dict | None = None

    if customer_code.upper() == "SBSA":
        print(f"[RANGE] SBSA — trayendo hash_file_filter para todo el rango de una vez", flush=True)
        hash_codes_by_date_stl = get_control_hash_codes_by_date(
            customer_code=customer_code,
            start_date=start_date,
            end_date=end_date,
            brand_local_indicator=brand_local_indicator,
            include_cps_collections=False,
        )
        hash_codes_by_date_trx = get_control_hash_codes_by_date(
            customer_code=customer_code,
            start_date=start_date,
            end_date=end_date,
            brand_local_indicator=brand_local_indicator,
            include_cps_collections=True,
        )

    dfs: List[DataFrame] = []
    failed_dates: List[str] = []

    while current_date <= end_date_obj:
        query_date = current_date.strftime("%Y-%m-%d")
        print(f"[RANGE] Procesando {query_date}", flush=True)

        try:
            df_day = get_mastercard_validation_results_combined(
                spark=spark,
                query_date=query_date,
                customer_code=customer_code,
                issuer_acquirer_indicator=issuer_acquirer_indicator,
                brand_local_indicator=brand_local_indicator,
                hash_codes_by_date_stl=hash_codes_by_date_stl,
                hash_codes_by_date_trx=hash_codes_by_date_trx,
            )
            dfs.append(df_day)

        except Exception as e:
            failed_dates.append(query_date)
            print(f"[WARN] Fecha omitida {query_date}: {str(e)}", flush=True)

        current_date += timedelta(days=1)

    if not dfs:
        raise RuntimeError(
            f"No se encontraron datos válidos entre {start_date} y {end_date}. "
            f"Fechas fallidas: {failed_dates}"
        )

    if failed_dates:
        print(f"[WARN] Fechas omitidas: {failed_dates}", flush=True)

    return reduce(lambda a, b: a.unionByName(b), dfs)


def build_output_names(customer_code: str, start_date: str, end_date: str) -> tuple[str, str]:
    """
    Arma el nombre de carpeta (mes, formato YYYYMM) y de archivo final para el
    output combinado, agregado 2026-08-12 para reemplazar el nombre
    autogenerado por Spark (part-00000-<uuid>-c000.snappy.parquet) por uno
    legible.

    Si el rango [start_date, end_date] cubre el mes calendario completo del
    mes de start_date (día 1 al último día del mes, mismo año/mes en ambos
    extremos), el archivo se nombra solo con el mes (YYYYMM). Si es un rango
    parcial, se nombra con ambas fechas (YYYYMMDD_YYYYMMDD).

    No contempla rangos que crucen más de un mes calendario (ej.
    2026-01-15 a 2026-02-15) — en ese caso usa el mes de start_date para la
    carpeta y el rango completo para el nombre del archivo, igual que
    cualquier rango parcial.

    Args:
        customer_code: Código de cliente, ej. "EBGR".
        start_date: Inicio del rango, formato "YYYY-MM-DD".
        end_date: Fin del rango, formato "YYYY-MM-DD".

    Returns:
        Tupla (year_month, file_name) — ej. ("202601", "EBGR_data_quality_202601.parquet")
        o ("202601", "EBGR_data_quality_20260105_20260107.parquet").

    Ejemplo:
        build_output_names("EBGR", "2026-01-01", "2026-01-31")
        # ("202601", "EBGR_data_quality_202601.parquet")
        build_output_names("EBGR", "2026-01-05", "2026-01-07")
        # ("202601", "EBGR_data_quality_20260105_20260107.parquet")
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    year_month = start_dt.strftime("%Y%m")

    last_day_of_month = monthrange(start_dt.year, start_dt.month)[1]
    is_full_month = (
        start_dt.day == 1
        and end_dt.year == start_dt.year
        and end_dt.month == start_dt.month
        and end_dt.day == last_day_of_month
    )

    if is_full_month:
        file_name = f"{customer_code}_data_quality_{year_month}.parquet"
    else:
        file_name = (
            f"{customer_code}_data_quality_"
            f"{start_dt.strftime('%Y%m%d')}_{end_dt.strftime('%Y%m%d')}.parquet"
        )

    return year_month, file_name


def write_single_named_parquet(spark, df: DataFrame, staging_path: str, final_file_path: str) -> None:
    """
    Escribe df como un único archivo parquet con nombre exacto (final_file_path),
    en vez del nombre autogenerado por Spark (part-00000-<uuid>-c000.snappy.parquet).
    Agregado 2026-08-12 junto con build_output_names().

    Escribe primero a staging_path (coalesce(1) + mode overwrite), después usa
    la API de Hadoop FileSystem (vía el JVM que ya usa Spark internamente) para
    mover el único part-*.parquet resultante al nombre final exacto, y borra
    la carpeta de staging. Funciona igual en local (file://) y en S3 (s3a://)
    porque ambos casos pasan por la misma API de Hadoop — no depende de boto3
    ni de si S3_OUTPUT es un bucket real o una ruta local de disco.

    Args:
        spark: Sesión de Spark activa.
        df: DataFrame a escribir (se le aplica coalesce(1) acá adentro).
        staging_path: Carpeta temporal donde Spark escribe su output normal
            antes del renombre (se borra al final).
        final_file_path: Ruta completa del archivo final, incluyendo nombre
            (ej. ".../MC/202601/EBGR_data_quality_202601.parquet").

    Ejemplo:
        write_single_named_parquet(
            spark, df_result,
            staging_path=f"{output_folder}_staging_tmp/",
            final_file_path=f"{output_folder}{file_name}",
        )
    """
    (
        df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(staging_path)
    )

    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()

    staging_hadoop_path = jvm.org.apache.hadoop.fs.Path(staging_path)
    final_hadoop_path = jvm.org.apache.hadoop.fs.Path(final_file_path)

    # FileSystem.get(conf) sin URI devuelve el filesystem default (file:/// en
    # Glue), no el del path real (s3a://) — falla en Glue real con
    # "Wrong FS: s3a://... expected: file:///" aunque funcione en local (ahí
    # default y real son el mismo file://). path.getFileSystem(conf) resuelve
    # el filesystem correcto según el scheme del propio path.
    fs = staging_hadoop_path.getFileSystem(hadoop_conf)

    part_file = None
    for status in fs.listStatus(staging_hadoop_path):
        name = status.getPath().getName()
        if name.startswith("part-") and name.endswith(".parquet"):
            part_file = status.getPath()
            break

    if part_file is None:
        raise RuntimeError(
            f"No se encontró ningún part-*.parquet en {staging_path} "
            f"para renombrar a {final_file_path}"
        )

    if fs.exists(final_hadoop_path):
        fs.delete(final_hadoop_path, False)

    fs.rename(part_file, final_hadoop_path)
    fs.delete(staging_hadoop_path, True)


# =============================================================================
# MAIN LOCAL TEST
# =============================================================================

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    spark.sparkContext.setLogLevel("ERROR")

    spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.broadcastTimeout", "600")
    spark.conf.set("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "32"))

    print(f"[MAIN] CUSTOMER_CODE={customer_code}", flush=True)
    print(f"[MAIN] START_DATE={start_date}", flush=True)
    print(f"[MAIN] END_DATE={end_date}", flush=True)
    print(f"[MAIN] ISSUER_ACQUIRER_INDICATOR={issuer_acquirer_indicator}", flush=True)
    print(f"[MAIN] BRAND_LOCAL_INDICATOR={brand_local_indicator}", flush=True)
    #customer_code = os.getenv("CUSTOMER_CODE", "EBGR")
    #brand_local_indicator = os.getenv("BRAND_LOCAL_INDICATOR", "default")
    #issuer_acquirer_indicator = os.getenv("ISSUER_ACQUIRER_INDICATOR", "I") #I: Incoming - Issuer, A: Outgoing - Acquirer
    #start_date = os.getenv("START_DATE", "2026-01-01")
    #end_date = os.getenv("END_DATE", "2026-01-01")

    df_result = get_mastercard_validation_results_range(
        spark=spark,
        start_date=start_date,
        end_date=end_date,
        customer_code=customer_code,
        issuer_acquirer_indicator=issuer_acquirer_indicator, 
        brand_local_indicator=brand_local_indicator,
    )
    if DEBUG:
        print(f"[COUNT] combined_result={df_result.count()}")

    if df_result is None:
        raise Exception(
            f"No se generó información para {customer_code} entre {start_date} y {end_date}"
    )
    
    if DEBUG:
        output_path = (
            "/home/ameza/IntelicaProyectos/glue-interchangemc/"
            f"output_local/validation_combined/{customer_code}/"
            f"start_date={start_date}_end_date={end_date}/"
        )
        (
            df_result
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(output_path)
        )
        print(f"[OK] Output combinado generado en: {output_path}", flush=True)
    else:
        year_month, file_name = build_output_names(customer_code, start_date, end_date)
        output_folder = f"{S3_OUTPUT}/{customer_code}/reports/quality/MC/{year_month}/"
        final_output_path = f"{output_folder}{file_name}"
        staging_path = f"{output_folder}_staging_tmp/"

        write_single_named_parquet(
            spark=spark,
            df=df_result,
            staging_path=staging_path,
            final_file_path=final_output_path,
        )
        print(f"[OK] Output combinado generado en: {final_output_path}", flush=True)

    spark.stop()
